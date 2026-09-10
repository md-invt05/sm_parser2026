#!/usr/bin/env python3
"""
DeFi liquidity scanner: индексирует создания смарт-контрактов с заданного блока
до latest, кладёт их в SQLite и параллельно считает on-chain балансы (натив +
ERC-20/BEP-20) в USD на момент проверки.

Запуск:
    python scan_defi.py
    python scan_defi.py --chains ethereum,bsc --min-usd 100000
    python scan_defi.py --chains ethereum --from-block 21000000 --export-only
    python scan_defi.py --balances-only
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import yaml
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from tenacity import retry, stop_after_attempt, wait_exponential

load_dotenv()

ROOT = Path(__file__).resolve().parent
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
BALANCE_OF_SEL = "0x70a08231"
DECIMALS_SEL = "0x313ce567"
SYMBOL_SEL = "0x95d89b41"
ZERO = "0x0000000000000000000000000000000000000000"
LLAMA_PRICES = "https://coins.llama.fi/prices/current/"

log = logging.getLogger("scanner")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


@dataclass
class ChainCfg:
    key: str
    enabled: bool
    chain_id: int
    display_name: str
    llama_chain: str
    native_symbol: str
    native_coingecko: str
    start_block: int
    logs_max_range: int
    explorer: str
    rpc: list[str]


@dataclass
class AppCfg:
    min_usd: float
    db_path: Path
    export_dir: Path
    recheck_interval_sec: int
    rpc_concurrency: int
    balance_concurrency: int
    block_batch_size: int
    receipt_batch_size: int
    price_batch_size: int
    http_timeout_sec: int
    max_retries: int
    run_indexer: bool
    run_balance_checker: bool
    discover_tokens_from_transfers: bool
    chains: dict[str, ChainCfg]


def _env_rpcs(chain_key: str) -> list[str]:
    raw = os.getenv(f"{chain_key.upper()}_RPC", "").strip()
    if not raw:
        return []
    return [u.strip() for u in raw.split(",") if u.strip()]


def load_config(path: Path) -> AppCfg:
    raw = yaml.safe_load(path.read_text())
    chains: dict[str, ChainCfg] = {}
    for key, c in raw["chains"].items():
        rpcs = _env_rpcs(key) or list(c.get("rpc") or [])
        chains[key] = ChainCfg(
            key=key,
            enabled=bool(c.get("enabled", True)),
            chain_id=int(c["chain_id"]),
            display_name=c.get("display_name", key),
            llama_chain=c["llama_chain"],
            native_symbol=c["native_symbol"],
            native_coingecko=c["native_coingecko"],
            start_block=int(c.get("start_block") or 1),
            logs_max_range=int(c.get("logs_max_range") or 500),
            explorer=c.get("explorer", ""),
            rpc=rpcs,
        )
    return AppCfg(
        min_usd=float(raw["min_usd"]),
        db_path=(ROOT / raw.get("db_path", "data/contracts.db")).resolve(),
        export_dir=(ROOT / raw.get("export_dir", "reports")).resolve(),
        recheck_interval_sec=int(raw.get("recheck_interval_sec", 86400)),
        rpc_concurrency=int(raw.get("rpc_concurrency", 8)),
        balance_concurrency=int(raw.get("balance_concurrency", 12)),
        block_batch_size=int(raw.get("block_batch_size", 15)),
        receipt_batch_size=int(raw.get("receipt_batch_size", 40)),
        price_batch_size=int(raw.get("price_batch_size", 40)),
        http_timeout_sec=int(raw.get("http_timeout_sec", 40)),
        max_retries=int(raw.get("max_retries", 5)),
        run_indexer=bool(raw.get("run_indexer", True)),
        run_balance_checker=bool(raw.get("run_balance_checker", True)),
        discover_tokens_from_transfers=bool(raw.get("discover_tokens_from_transfers", True)),
        chains=chains,
    )


def load_token_seed(path: Path) -> dict[str, list[dict[str, Any]]]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text()) or {}


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS chain_state (
    chain            TEXT PRIMARY KEY,
    start_block      INTEGER NOT NULL,
    last_indexed     INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS contracts (
    chain            TEXT NOT NULL,
    address          TEXT NOT NULL,
    created_block    INTEGER,
    created_tx       TEXT,
    creator          TEXT,
    first_seen_at    TEXT NOT NULL,
    last_checked_at  TEXT,
    last_total_usd   REAL,
    last_status      TEXT,
    PRIMARY KEY (chain, address)
);

CREATE TABLE IF NOT EXISTS tokens (
    chain            TEXT NOT NULL,
    address          TEXT NOT NULL,
    symbol           TEXT,
    decimals         INTEGER,
    source           TEXT,
    PRIMARY KEY (chain, address)
);

CREATE TABLE IF NOT EXISTS scans (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    chain            TEXT NOT NULL,
    address          TEXT NOT NULL,
    scanned_at       TEXT NOT NULL,
    native_amount    REAL,
    native_usd       REAL,
    tokens_usd       REAL,
    total_usd        REAL,
    meets_threshold  INTEGER,
    note             TEXT
);

CREATE TABLE IF NOT EXISTS scan_tokens (
    scan_id          INTEGER NOT NULL REFERENCES scans(id) ON DELETE CASCADE,
    token            TEXT NOT NULL,
    symbol           TEXT,
    amount           REAL,
    price_usd        REAL,
    usd_value        REAL
);

CREATE INDEX IF NOT EXISTS idx_contracts_check ON contracts(chain, last_checked_at);
CREATE INDEX IF NOT EXISTS idx_scans_addr ON scans(chain, address, scanned_at);
"""


class DB:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._lock = threading.Lock()
        self.conn = sqlite3.connect(path, check_same_thread=False, timeout=60)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def init_chain(self, chain: str, start_block: int) -> int:
        with self._lock:
            row = self.conn.execute(
                "SELECT last_indexed, start_block FROM chain_state WHERE chain=?", (chain,)
            ).fetchone()
            if row is None:
                self.conn.execute(
                    "INSERT INTO chain_state(chain, start_block, last_indexed) VALUES(?,?,?)",
                    (chain, start_block, max(start_block - 1, 0)),
                )
                self.conn.commit()
                return max(start_block - 1, 0)
            return int(row["last_indexed"])

    def set_last_indexed(self, chain: str, block: int) -> None:
        with self._lock:
            self.conn.execute(
                "UPDATE chain_state SET last_indexed=? WHERE chain=?", (block, chain)
            )
            self.conn.commit()

    def upsert_contracts(self, rows: list[tuple]) -> int:
        if not rows:
            return 0
        with self._lock:
            self.conn.executemany(
                """
                INSERT OR IGNORE INTO contracts
                    (chain, address, created_block, created_tx, creator, first_seen_at)
                VALUES (?,?,?,?,?,?)
                """,
                rows,
            )
            self.conn.commit()
            return self.conn.execute("SELECT changes()").fetchone()[0]

    def upsert_tokens(self, rows: list[tuple]) -> None:
        if not rows:
            return
        with self._lock:
            self.conn.executemany(
                """
                INSERT INTO tokens(chain, address, symbol, decimals, source)
                VALUES (?,?,?,?,?)
                ON CONFLICT(chain, address) DO UPDATE SET
                    symbol=COALESCE(excluded.symbol, tokens.symbol),
                    decimals=COALESCE(excluded.decimals, tokens.decimals)
                """,
                rows,
            )
            self.conn.commit()

    def tokens_for(self, chain: str) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self.conn.execute("SELECT * FROM tokens WHERE chain=?", (chain,))
            )

    def contract_addresses(self, chain: str) -> set[str]:
        with self._lock:
            return {
                r["address"].lower()
                for r in self.conn.execute(
                    "SELECT address FROM contracts WHERE chain=?", (chain,)
                )
            }

    def pending_contracts(self, chain: str, recheck_sec: int, limit: int = 200) -> list[sqlite3.Row]:
        cutoff = datetime.now(timezone.utc).isoformat()
        with self._lock:
            return list(
                self.conn.execute(
                    """
                    SELECT * FROM contracts
                    WHERE chain=?
                      AND (
                        last_checked_at IS NULL
                        OR strftime('%s', last_checked_at) < strftime('%s', ?) - ?
                      )
                    ORDER BY last_checked_at IS NULL DESC, created_block DESC
                    LIMIT ?
                    """,
                    (chain, cutoff, recheck_sec, limit),
                )
            )

    def save_scan(
        self,
        chain: str,
        address: str,
        native_amount: float,
        native_usd: float,
        token_rows: list[dict[str, Any]],
        total_usd: float,
        min_usd: float,
    ) -> None:
        tokens_usd = sum(t["usd_value"] for t in token_rows)
        meets = 1 if total_usd >= min_usd else 0
        if total_usd <= 0:
            status = "zero"
            note = "нулевой баланс"
        elif meets:
            status = "qualifying"
            note = f"{total_usd:,.2f}$ >= {min_usd:,.0f}$"
        else:
            status = "below"
            note = f"{total_usd:,.2f}$ (меньше условия {min_usd:,.0f}$)"
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            cur = self.conn.execute(
                """
                INSERT INTO scans(chain, address, scanned_at, native_amount, native_usd,
                                  tokens_usd, total_usd, meets_threshold, note)
                VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (chain, address, now, native_amount, native_usd, tokens_usd, total_usd, meets, note),
            )
            scan_id = cur.lastrowid
            self.conn.executemany(
                """
                INSERT INTO scan_tokens(scan_id, token, symbol, amount, price_usd, usd_value)
                VALUES (?,?,?,?,?,?)
                """,
                [
                    (scan_id, t["token"], t["symbol"], t["amount"], t["price_usd"], t["usd_value"])
                    for t in token_rows
                ],
            )
            self.conn.execute(
                """
                UPDATE contracts
                SET last_checked_at=?, last_total_usd=?, last_status=?
                WHERE chain=? AND address=?
                """,
                (now, total_usd, status, chain, address),
            )
            self.conn.commit()

    def latest_nonzero_scans(self) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self.conn.execute(
                    """
                    SELECT c.chain, c.address, c.created_block, c.created_tx, c.creator,
                           c.last_status, s.scanned_at, s.native_amount, s.native_usd,
                           s.tokens_usd, s.total_usd, s.note, s.id AS scan_id
                    FROM contracts c
                    JOIN scans s ON s.id = (
                        SELECT id FROM scans
                        WHERE chain=c.chain AND address=c.address
                        ORDER BY scanned_at DESC LIMIT 1
                    )
                    WHERE s.total_usd > 0
                    ORDER BY s.total_usd DESC
                    """
                )
            )

    def tokens_of_scan(self, scan_id: int) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self.conn.execute(
                    "SELECT * FROM scan_tokens WHERE scan_id=? AND usd_value > 0 ORDER BY usd_value DESC",
                    (scan_id,),
                )
            )


# ---------------------------------------------------------------------------
# RPC
# ---------------------------------------------------------------------------


class RpcPool:
    def __init__(self, urls: list[str], timeout: int, retries: int, concurrency: int):
        if not urls:
            raise ValueError("нет RPC URL")
        self.urls = urls
        self.timeout = timeout
        self.retries = retries
        self.sem = asyncio.Semaphore(concurrency)
        self._i = 0
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "RpcPool":
        self._client = httpx.AsyncClient(timeout=self.timeout)
        return self

    async def __aexit__(self, *a: Any) -> None:
        if self._client:
            await self._client.aclose()

    def _next_url(self) -> str:
        url = self.urls[self._i % len(self.urls)]
        self._i += 1
        return url

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(min=0.4, max=8))
    async def call(self, method: str, params: list[Any]) -> Any:
        assert self._client
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        async with self.sem:
            url = self._next_url()
            r = await self._client.post(url, json=payload)
            r.raise_for_status()
            data = r.json()
        if "error" in data and data["error"]:
            raise RuntimeError(f"{method} {data['error']}")
        return data.get("result")

    async def batch(self, calls: list[tuple[str, list[Any]]]) -> list[Any]:
        assert self._client
        if not calls:
            return []
        payload = [
            {"jsonrpc": "2.0", "id": i, "method": m, "params": p}
            for i, (m, p) in enumerate(calls)
        ]
        last_err: Exception | None = None
        for attempt in range(self.retries):
            url = self._next_url()
            try:
                async with self.sem:
                    r = await self._client.post(url, json=payload)
                    r.raise_for_status()
                    data = r.json()
                if isinstance(data, dict) and data.get("error"):
                    raise RuntimeError(str(data["error"]))
                if not isinstance(data, list):
                    raise RuntimeError(f"ожидали batch-массив, получили {type(data)}")
                by_id = {item.get("id"): item for item in data}
                out = []
                for i in range(len(calls)):
                    item = by_id.get(i, {})
                    if item.get("error"):
                        out.append(None)
                    else:
                        out.append(item.get("result"))
                return out
            except Exception as e:
                last_err = e
                await asyncio.sleep(0.4 * (attempt + 1))
        raise RuntimeError(f"RPC batch failed: {last_err}")


def hex_int(v: Any) -> int:
    if v is None:
        return 0
    if isinstance(v, int):
        return v
    return int(v, 16)


def pad_addr(addr: str) -> str:
    return addr.lower().replace("0x", "").rjust(64, "0")


def topic_to_addr(topic: str) -> str:
    return "0x" + topic[-40:].lower()


def decode_uint(data: str | None) -> int:
    if not data or data == "0x":
        return 0
    return int(data, 16)


def decode_string(data: str | None) -> str | None:
    if not data or data == "0x" or len(data) < 2 + 64:
        return None
    raw = bytes.fromhex(data[2:])
    if len(raw) >= 64 and int.from_bytes(raw[:32], "big") == 32:
        n = int.from_bytes(raw[32:64], "big")
        return raw[64 : 64 + n].decode("utf-8", "replace").strip("\x00")
    # bytes32 symbol
    try:
        return raw[:32].split(b"\x00", 1)[0].decode("utf-8", "replace") or None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Prices
# ---------------------------------------------------------------------------


class PriceBook:
    def __init__(self, timeout: int):
        self.timeout = timeout
        self.cache: dict[str, float] = {}

    async def fetch(self, keys: list[str]) -> dict[str, float]:
        missing = [k for k in keys if k not in self.cache]
        if not missing:
            return {k: self.cache[k] for k in keys if k in self.cache}
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            for i in range(0, len(missing), 40):
                chunk = missing[i : i + 40]
                url = LLAMA_PRICES + ",".join(chunk)
                try:
                    r = await client.get(url)
                    r.raise_for_status()
                    coins = r.json().get("coins") or {}
                    for k, v in coins.items():
                        px = v.get("price")
                        if px is not None:
                            self.cache[k] = float(px)
                except Exception as e:
                    log.warning("DefiLlama prices error: %s", e)
        return {k: self.cache[k] for k in keys if k in self.cache}


def llama_key(chain: ChainCfg, token: str | None = None) -> str:
    if token is None:
        return f"coingecko:{chain.native_coingecko}"
    return f"{chain.llama_chain}:{token.lower()}"


# ---------------------------------------------------------------------------
# Indexer
# ---------------------------------------------------------------------------


async def latest_block(rpc: RpcPool) -> int:
    return hex_int(await rpc.call("eth_blockNumber", []))


async def index_chain(
    db: DB,
    chain: ChainCfg,
    rpc: RpcPool,
    cfg: AppCfg,
    stop: asyncio.Event,
    from_block_override: int | None,
) -> None:
    last = db.init_chain(chain.key, from_block_override or chain.start_block)
    if from_block_override and last < from_block_override - 1:
        last = from_block_override - 1
        db.set_last_indexed(chain.key, last)

    log.info("[%s] indexer start from block %s", chain.key, last + 1)

    while not stop.is_set():
        try:
            head = await latest_block(rpc)
        except Exception as e:
            log.warning("[%s] eth_blockNumber: %s", chain.key, e)
            await asyncio.sleep(3)
            continue

        if last >= head:
            await asyncio.sleep(4)
            continue

        batch_end = min(head, last + cfg.block_batch_size)
        blocks_to_get = list(range(last + 1, batch_end + 1))
        try:
            results = await rpc.batch(
                [("eth_getBlockByNumber", [hex(b), True]) for b in blocks_to_get]
            )
        except Exception as e:
            log.warning("[%s] getBlock batch %s-%s: %s", chain.key, last + 1, batch_end, e)
            await asyncio.sleep(2)
            continue

        create_txs: list[dict[str, Any]] = []
        for block in results:
            if not block:
                continue
            for tx in block.get("transactions") or []:
                if tx.get("to") in (None, "", "0x"):
                    create_txs.append(tx)

        new_rows: list[tuple] = []
        if create_txs:
            try:
                receipts = await rpc.batch(
                    [
                        ("eth_getTransactionReceipt", [tx["hash"]])
                        for tx in create_txs[: cfg.receipt_batch_size * 4]
                    ]
                )
            except Exception as e:
                log.warning("[%s] receipts: %s", chain.key, e)
                receipts = []
            now = datetime.now(timezone.utc).isoformat()
            for tx, rcpt in zip(create_txs, receipts):
                if not rcpt:
                    continue
                addr = rcpt.get("contractAddress")
                if not addr:
                    continue
                new_rows.append(
                    (
                        chain.key,
                        addr.lower(),
                        hex_int(tx.get("blockNumber")),
                        tx.get("hash"),
                        (tx.get("from") or "").lower(),
                        now,
                    )
                )

        inserted = db.upsert_contracts(new_rows)

        if cfg.discover_tokens_from_transfers:
            await harvest_transfers(db, chain, rpc, last + 1, batch_end)

        db.set_last_indexed(chain.key, batch_end)
        last = batch_end
        if inserted:
            log.info(
                "[%s] blocks %s..%s head=%s new_contracts=%s",
                chain.key,
                blocks_to_get[0],
                batch_end,
                head,
                inserted,
            )


async def harvest_transfers(
    db: DB, chain: ChainCfg, rpc: RpcPool, start: int, end: int
) -> None:
    """Собирает ERC-20, которые слали на уже известные контракты в этом диапазоне."""
    known = db.contract_addresses(chain.key)
    if not known:
        return

    step = max(1, min(chain.logs_max_range, end - start + 1))
    found: list[tuple] = []
    s = start
    while s <= end:
        e = min(end, s + step - 1)
        try:
            logs = await rpc.call(
                "eth_getLogs",
                [{"fromBlock": hex(s), "toBlock": hex(e), "topics": [TRANSFER_TOPIC]}],
            )
        except Exception:
            # слишком широкий диапазон / rate limit — дробим
            if step > 1:
                step = max(1, step // 2)
                continue
            s = e + 1
            continue
        for lg in logs or []:
            topics = lg.get("topics") or []
            if len(topics) < 3:
                continue
            to_addr = topic_to_addr(topics[2])
            if to_addr not in known:
                continue
            token = (lg.get("address") or "").lower()
            if token and token != ZERO:
                found.append((chain.key, token, None, None, "transfer_log"))
        s = e + 1
    if found:
        # unique
        uniq = {(a, b): (a, b, c, d, e) for a, b, c, d, e in found}
        db.upsert_tokens(list(uniq.values()))


# ---------------------------------------------------------------------------
# Balances
# ---------------------------------------------------------------------------


async def eth_call(rpc: RpcPool, to: str, data: str) -> str | None:
    try:
        return await rpc.call("eth_call", [{"to": to, "data": data}, "latest"])
    except Exception:
        return None


async def fill_token_meta(db: DB, chain: ChainCfg, rpc: RpcPool) -> None:
    rows = [
        r
        for r in db.tokens_for(chain.key)
        if r["decimals"] is None or r["symbol"] is None
    ]
    for r in rows:
        addr = r["address"]
        dec = r["decimals"]
        sym = r["symbol"]
        if dec is None:
            raw = await eth_call(rpc, addr, DECIMALS_SEL)
            try:
                dec = decode_uint(raw)
                if dec > 36:
                    dec = None
            except Exception:
                dec = None
        if sym is None:
            raw = await eth_call(rpc, addr, SYMBOL_SEL)
            sym = decode_string(raw)
        db.upsert_tokens([(chain.key, addr, sym, dec, r["source"] or "meta")])


async def read_balances(
    rpc: RpcPool,
    holder: str,
    tokens: list[sqlite3.Row],
) -> tuple[int, list[tuple[str, int]]]:
    calls: list[tuple[str, list[Any]]] = [
        ("eth_getBalance", [holder, "latest"]),
    ]
    data = BALANCE_OF_SEL + pad_addr(holder)
    for t in tokens:
        calls.append(("eth_call", [{"to": t["address"], "data": data}, "latest"]))
    results = await rpc.batch(calls)
    native = decode_uint(results[0]) if results else 0
    tok_bals: list[tuple[str, int]] = []
    for t, raw in zip(tokens, results[1:]):
        try:
            bal = decode_uint(raw)
        except Exception:
            bal = 0
        if bal:
            tok_bals.append((t["address"], bal))
    return native, tok_bals


async def check_chain_balances(
    db: DB,
    chain: ChainCfg,
    rpc: RpcPool,
    prices: PriceBook,
    cfg: AppCfg,
    min_usd: float,
    stop: asyncio.Event,
) -> None:
    log.info("[%s] balance checker online", chain.key)
    sem = asyncio.Semaphore(cfg.balance_concurrency)

    while not stop.is_set():
        await fill_token_meta(db, chain, rpc)
        pending = db.pending_contracts(chain.key, cfg.recheck_interval_sec, limit=150)
        if not pending:
            await asyncio.sleep(5)
            continue

        tokens = db.tokens_for(chain.key)
        price_keys = [llama_key(chain)] + [llama_key(chain, t["address"]) for t in tokens]
        await prices.fetch(price_keys)

        async def one(row: sqlite3.Row) -> None:
            async with sem:
                try:
                    native_wei, tok_bals = await read_balances(rpc, row["address"], tokens)
                except Exception as e:
                    log.debug("[%s] balance %s: %s", chain.key, row["address"], e)
                    return
            native_amt = native_wei / 1e18
            native_px = prices.cache.get(llama_key(chain), 0.0)
            native_usd = native_amt * native_px
            tok_rows = []
            tokens_by_addr = {t["address"].lower(): t for t in tokens}
            for addr, raw in tok_bals:
                meta = tokens_by_addr.get(addr.lower())
                dec = int(meta["decimals"] if meta and meta["decimals"] is not None else 18)
                amt = raw / (10 ** dec)
                px = prices.cache.get(llama_key(chain, addr), 0.0)
                usd = amt * px
                if usd <= 0 and amt <= 0:
                    continue
                tok_rows.append(
                    {
                        "token": addr,
                        "symbol": (meta["symbol"] if meta else None) or addr[:10],
                        "amount": amt,
                        "price_usd": px,
                        "usd_value": usd,
                    }
                )
            total = native_usd + sum(t["usd_value"] for t in tok_rows)
            db.save_scan(
                chain.key,
                row["address"],
                native_amt,
                native_usd,
                [t for t in tok_rows if t["usd_value"] > 0],
                total,
                min_usd,
            )
            if total > 0:
                log.info(
                    "[%s] %s  $%s  %s",
                    chain.key,
                    row["address"],
                    f"{total:,.0f}",
                    "OK" if total >= min_usd else "below",
                )

        await asyncio.gather(*(one(r) for r in pending), return_exceptions=True)


# ---------------------------------------------------------------------------
# Excel
# ---------------------------------------------------------------------------


HEADER_FILL = PatternFill("solid", fgColor="1F4E79")
HEADER_FONT = Font(color="FFFFFF", bold=True, name="Calibri", size=11)
OK_FILL = PatternFill("solid", fgColor="C6EFCE")
BELOW_FILL = PatternFill("solid", fgColor="FFF2CC")
THIN = Border(
    left=Side(style="thin", color="BFBFBF"),
    right=Side(style="thin", color="BFBFBF"),
    top=Side(style="thin", color="BFBFBF"),
    bottom=Side(style="thin", color="BFBFBF"),
)
MONEY = '#,##0.00"$"'


def _style_header(ws) -> None:
    ws.auto_filter.ref = ws.dimensions
    ws.freeze_panes = "A2"
    ws.row_dimensions[1].height = 22
    for cell in ws[1]:
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)


def _autosize(ws, widths: dict[int, int]) -> None:
    for i, w in widths.items():
        ws.column_dimensions[get_column_letter(i)].width = w


def export_xlsx(db: DB, cfg: AppCfg, chains: dict[str, ChainCfg], min_usd: float) -> tuple[Path, Path]:
    cfg.export_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    qual_path = cfg.export_dir / f"qualifying_{stamp}.xlsx"
    below_path = cfg.export_dir / f"below_threshold_{stamp}.xlsx"

    headers = [
        "Сеть",
        "Адрес контракта",
        "Explorer",
        "Создан в блоке",
        "Tx создания",
        "Creator",
        "Проверено (UTC)",
        f"Натив ({'/'.join(sorted({c.native_symbol for c in chains.values()}))})",
        "Натив USD",
        "Токены USD",
        "Итого USD",
        "Маркировка",
        "Состав (токен=USD)",
    ]

    def build(path: Path, rows: list[sqlite3.Row], title: str, fill) -> None:
        wb = Workbook()
        ws = wb.active
        ws.title = title[:31]
        ws.append(headers)
        for r in rows:
            parts = []
            for t in db.tokens_of_scan(r["scan_id"]):
                parts.append(f"{t['symbol']}={t['usd_value']:.2f}$")
            chain = chains.get(r["chain"])
            expl = (chain.explorer if chain else "") + r["address"]
            native_sym = chain.native_symbol if chain else ""
            ws.append(
                [
                    chain.display_name if chain else r["chain"],
                    r["address"],
                    expl,
                    r["created_block"],
                    r["created_tx"],
                    r["creator"],
                    r["scanned_at"],
                    float(r["native_amount"] or 0),
                    float(r["native_usd"] or 0),
                    float(r["tokens_usd"] or 0),
                    float(r["total_usd"] or 0),
                    r["note"],
                    "; ".join(parts),
                ]
            )
            for cell in ws[ws.max_row]:
                cell.border = THIN
                cell.alignment = Alignment(vertical="center")
            for col in (9, 10, 11):
                ws.cell(ws.max_row, col).number_format = MONEY
            ws.cell(ws.max_row, 8).number_format = "#,##0.0000"
            for cell in ws[ws.max_row]:
                cell.fill = fill

        # info sheet
        info = wb.create_sheet("Параметры")
        info.append(["Параметр", "Значение"])
        info.append(["Порог USD", min_usd])
        info.append(["Выгрузка UTC", datetime.now(timezone.utc).isoformat()])
        info.append(["База", str(db.path)])
        info.append(["Строк", len(rows)])
        info.append(["Правило", title])
        _style_header(ws)
        _style_header(info)
        _autosize(
            ws,
            {
                1: 18, 2: 46, 3: 42, 4: 16, 5: 20, 6: 20, 7: 22,
                8: 16, 9: 14, 10: 14, 11: 14, 12: 40, 13: 60,
            },
        )
        _autosize(info, {1: 28, 2: 60})
        wb.save(path)

    all_rows = db.latest_nonzero_scans()
    qualifying = [r for r in all_rows if (r["total_usd"] or 0) >= min_usd]
    below = [r for r in all_rows if 0 < (r["total_usd"] or 0) < min_usd]

    build(qual_path, qualifying, f"Баланс ≥ {min_usd:,.0f}$", OK_FILL)
    build(below_path, below, f"Баланс < {min_usd:,.0f}$ (не ноль)", BELOW_FILL)
    log.info("xlsx: %s (%s rows)", qual_path.name, len(qualifying))
    log.info("xlsx: %s (%s rows)", below_path.name, len(below))
    return qual_path, below_path


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def seed_tokens(db: DB, seed: dict[str, list[dict[str, Any]]], enabled: list[str]) -> None:
    rows = []
    for chain in enabled:
        for t in seed.get(chain, []):
            rows.append(
                (
                    chain,
                    t["address"].lower(),
                    t.get("symbol"),
                    t.get("decimals"),
                    "seed",
                )
            )
    db.upsert_tokens(rows)


async def run(args: argparse.Namespace) -> None:
    cfg = load_config(ROOT / "config.yaml")
    if args.min_usd is not None:
        cfg.min_usd = args.min_usd
    wanted = [c.strip() for c in args.chains.split(",")] if args.chains else [
        k for k, v in cfg.chains.items() if v.enabled
    ]
    selected = []
    for k in wanted:
        if k not in cfg.chains:
            raise SystemExit(f"неизвестная сеть: {k}. Доступны: {', '.join(cfg.chains)}")
        selected.append(cfg.chains[k])

    db = DB(cfg.db_path)
    seed_tokens(db, load_token_seed(ROOT / "tokens.yaml"), [c.key for c in selected])

    if args.export_only:
        export_xlsx(db, cfg, {c.key: c for c in selected}, cfg.min_usd)
        db.close()
        return

    stop = asyncio.Event()
    prices = PriceBook(cfg.http_timeout_sec)
    tasks = []
    pools: list[RpcPool] = []

    try:
        for chain in selected:
            if not chain.rpc:
                log.error("[%s] нет RPC — пропуск", chain.key)
                continue
            pool = RpcPool(chain.rpc, cfg.http_timeout_sec, cfg.max_retries, cfg.rpc_concurrency)
            await pool.__aenter__()
            pools.append(pool)

            do_index = cfg.run_indexer and not args.balances_only
            do_bal = cfg.run_balance_checker and not args.index_only
            if do_index:
                tasks.append(
                    asyncio.create_task(
                        index_chain(db, chain, pool, cfg, stop, args.from_block),
                        name=f"idx-{chain.key}",
                    )
                )
            if do_bal:
                tasks.append(
                    asyncio.create_task(
                        check_chain_balances(db, chain, pool, prices, cfg, cfg.min_usd, stop),
                        name=f"bal-{chain.key}",
                    )
                )

        if not tasks:
            log.error("нечего запускать")
            return

        log.info(
            "запущено задач=%s сети=%s порог=$%s db=%s",
            len(tasks),
            ",".join(c.key for c in selected),
            f"{cfg.min_usd:,.0f}",
            db.path,
        )
        exporter = asyncio.create_task(export_loop(db, cfg, selected, stop), name="xlsx")
        tasks.append(exporter)
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        stop.set()
        try:
            export_xlsx(db, cfg, {c.key: c for c in selected}, cfg.min_usd)
        except Exception as e:
            log.warning("финальный export: %s", e)
        for p in pools:
            await p.__aexit__(None, None, None)
        db.close()


async def export_loop(db: DB, cfg: AppCfg, selected: list[ChainCfg], stop: asyncio.Event) -> None:
    mapping = {c.key: c for c in selected}
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=180)
        except asyncio.TimeoutError:
            try:
                export_xlsx(db, cfg, mapping, cfg.min_usd)
            except Exception as e:
                log.warning("periodic export: %s", e)


def main() -> None:
    parser = argparse.ArgumentParser(description="DeFi contract liquidity scanner")
    parser.add_argument("--chains", help="ethereum,bsc,polygon,zk,zksync,robinhood,hyperliquid")
    parser.add_argument("--from-block", type=int, dest="from_block", help="стартовый блок (для всех выбранных сетей)")
    parser.add_argument("--min-usd", type=float, dest="min_usd", help="порог в USD, по умолчанию из config.yaml")
    parser.add_argument("--index-only", action="store_true", help="только парсить создания контрактов")
    parser.add_argument("--balances-only", action="store_true", help="только проверять балансы из БД")
    parser.add_argument("--export-only", action="store_true", help="только выгрузить xlsx из БД")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        log.info("остановлено")


if __name__ == "__main__":
    main()
