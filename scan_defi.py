#!/usr/bin/env python3
"""
DeFi liquidity scanner — автономный режим.

Индексирует прямые создания контрактов, считает мультичейн-балансы в USD,
пишет SQLite и три xlsx-отчёта.
При 429 / CU limit / перегрузе RPC сам уходит в паузу и продолжает.
Логи — в logs/, не в консоль (консоль только старт и редкий heartbeat).

    python scan_defi.py
    python scan_defi.py --rpc-check
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import random
import signal
import sqlite3
import threading
import time
import traceback
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
import yaml
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from monitoring import LOAD_PROFILES, MonitorStore, percentile

load_dotenv()

ROOT = Path(__file__).resolve().parent
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
BALANCE_OF_SEL = "0x70a08231"
DECIMALS_SEL = "0x313ce567"
SYMBOL_SEL = "0x95d89b41"
ZERO = "0x0000000000000000000000000000000000000000"
LLAMA_PRICES = "https://coins.llama.fi/prices/current/"
# Task-local policy: balance probes must not inherit the indexer's long retries.
BALANCE_RPC = ContextVar("balance_rpc", default=False)

RATE_HINTS = (
    "rate limit",
    "too many requests",
    "too many request",
    "429",
    "compute units",
    "cu limit",
    "exceeded",
    "over rate",
    "capacity",
    "throttl",
    "slow down",
    "temporarily unavailable",
    "try again",
    "call rate",
    "credits",
    "quota",
    "limit reached",
    "projected cu",
)
RANGE_HINTS = (
    "block range",
    "query returned more",
    "response size",
    "log response size",
    "more than",
    "range limit",
    "too many results",
    "timeout",
    "timed out",
    "-32602",
)
AUTH_HINTS = (
    "unauthorized",
    "forbidden",
    "invalid api key",
    "missing api key",
    "api key required",
    "access token",
    "authentication required",
    "project id",
)
RETRYABLE_HTTP = {408, 425, 429, 500, 502, 503, 504, 520, 522, 524}

log = logging.getLogger("scanner")
console = logging.getLogger("scanner.console")


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
    native_decimals: int
    start_block: int
    confirmations: int
    logs_max_range: int
    explorer: str
    rpc: list[str]


@dataclass
class AppCfg:
    min_usd: float
    db_path: Path
    export_dir: Path
    log_dir: Path
    monitoring_db_path: Path
    recheck_interval_sec: int
    global_rpc_concurrency: int
    rpc_concurrency: int
    balance_concurrency: int
    balance_chain_concurrency: int
    balance_chain_timeout_sec: float
    balance_address_timeout_sec: float
    balance_retry_sec: int
    rabby_fallback: bool
    rabby_request_interval_sec: float
    rabby_timeout_sec: float
    rabby_uncertainty: float
    block_batch_size: int
    receipt_batch_size: int
    price_batch_size: int
    http_timeout_sec: int
    max_retries: int
    cooldown_min_sec: float
    cooldown_max_sec: float
    all_down_sleep_sec: float
    adaptive_batch: bool
    heartbeat_sec: int
    export_every_sec: int
    daily_snapshot: bool
    task_restart_sec: int
    run_indexer: bool
    run_balance_checker: bool
    discover_tokens_from_transfers: bool
    discover_tx_to_contracts: bool
    code_cache_ttl_sec: int
    default_chains: list[str]
    chains: dict[str, ChainCfg]


def _env_rpcs(chain_key: str) -> list[str]:
    raw = os.getenv(f"{chain_key.upper()}_RPC", "").strip()
    if not raw:
        return []
    return [u.strip() for u in raw.split(",") if u.strip()]


DRPC_FALLBACK_SLUGS = {
    "ethereum": "ethereum", "bsc": "bsc", "polygon": "polygon",
    "arbitrum": "arbitrum", "optimism": "optimism", "base": "base",
    "zk": "polygon-zkevm", "zksync": "zksync", "robinhood": "robinhood",
    "linea": "linea", "scroll": "scroll", "mantle": "mantle", "blast": "blast",
    "celo": "celo", "gnosis": "gnosis", "cronos": "cronos", "kava": "kava",
    "metis": "metis", "harmony": "harmony-0",
}


def _drpc_fallback_rpcs(chain_key: str) -> list[str]:
    """Build secondary keyed dRPC URLs without duplicating every network URL in .env."""
    slug = DRPC_FALLBACK_SLUGS.get(chain_key)
    raw = os.getenv("DRPC_FALLBACK_KEYS", "").strip()
    if not slug or not raw:
        return []
    return [
        f"https://lb.drpc.live/{slug}/{token.strip()}"
        for token in raw.split(",")
        if token.strip()
    ]


def load_config(path: Path) -> AppCfg:
    raw = yaml.safe_load(path.read_text())
    uncertainty = float(raw.get("rabby_uncertainty", 0.10))
    if not 0 < uncertainty < 1:
        raise ValueError("rabby_uncertainty must be between 0 and 1")
    chains: dict[str, ChainCfg] = {}
    for key, c in raw["chains"].items():
        # Keyed/private endpoints have priority, public endpoints stay as fallback.
        rpcs = list(dict.fromkeys(
            _env_rpcs(key) + _drpc_fallback_rpcs(key) + list(c.get("rpc") or [])
        ))
        chains[key] = ChainCfg(
            key=key,
            enabled=bool(c.get("enabled", True)),
            chain_id=int(c["chain_id"]),
            display_name=c.get("display_name", key),
            llama_chain=c["llama_chain"],
            native_symbol=c["native_symbol"],
            native_coingecko=c["native_coingecko"],
            native_decimals=int(c.get("native_decimals", 18)),
            start_block=int(c.get("start_block") or 1),
            confirmations=int(c.get("confirmations", 20)),
            logs_max_range=int(c.get("logs_max_range") or 500),
            explorer=c.get("explorer", ""),
            rpc=rpcs,
        )
    return AppCfg(
        min_usd=float(raw["min_usd"]),
        db_path=(ROOT / raw.get("db_path", "data/contracts.db")).resolve(),
        export_dir=(ROOT / raw.get("export_dir", "reports")).resolve(),
        log_dir=(ROOT / raw.get("log_dir", "logs")).resolve(),
        monitoring_db_path=(ROOT / raw.get("monitoring_db_path", "data/monitoring.db")).resolve(),
        recheck_interval_sec=int(raw.get("recheck_interval_sec", 86400)),
        global_rpc_concurrency=int(raw.get("global_rpc_concurrency", 12)),
        rpc_concurrency=int(raw.get("rpc_concurrency", 2)),
        balance_concurrency=max(1, int(raw.get("balance_concurrency", 8))),
        balance_chain_concurrency=max(1, int(raw.get("balance_chain_concurrency", 12))),
        balance_chain_timeout_sec=max(0.01, float(raw.get("balance_chain_timeout_sec", 20))),
        balance_address_timeout_sec=max(0.01, float(raw.get("balance_address_timeout_sec", 60))),
        balance_retry_sec=max(1, int(raw.get("balance_retry_sec", 300))),
        rabby_fallback=bool(raw.get("rabby_fallback", True)),
        rabby_request_interval_sec=max(0.0, float(raw.get("rabby_request_interval_sec", 3))),
        rabby_timeout_sec=max(0.01, float(raw.get("rabby_timeout_sec", 12))),
        rabby_uncertainty=uncertainty,
        block_batch_size=int(raw.get("block_batch_size", 8)),
        receipt_batch_size=int(raw.get("receipt_batch_size", 20)),
        price_batch_size=int(raw.get("price_batch_size", 40)),
        http_timeout_sec=int(raw.get("http_timeout_sec", 40)),
        max_retries=int(raw.get("max_retries", 8)),
        cooldown_min_sec=float(raw.get("cooldown_min_sec", 5)),
        cooldown_max_sec=float(raw.get("cooldown_max_sec", 900)),
        all_down_sleep_sec=float(raw.get("all_down_sleep_sec", 30)),
        adaptive_batch=bool(raw.get("adaptive_batch", True)),
        heartbeat_sec=max(10, int(raw.get("heartbeat_sec", 60))),
        export_every_sec=int(raw.get("export_every_sec", 300)),
        daily_snapshot=bool(raw.get("daily_snapshot", True)),
        task_restart_sec=int(raw.get("task_restart_sec", 20)),
        run_indexer=bool(raw.get("run_indexer", True)),
        run_balance_checker=bool(raw.get("run_balance_checker", True)),
        discover_tokens_from_transfers=bool(raw.get("discover_tokens_from_transfers", True)),
        discover_tx_to_contracts=bool(raw.get("discover_tx_to_contracts", True)),
        code_cache_ttl_sec=max(1, int(raw.get("code_cache_ttl_sec", 86400))),
        default_chains=list(raw.get("default_chains") or [
            key for key, chain in chains.items() if chain.enabled
        ]),
        chains=chains,
    )


def apply_load_profile(cfg: AppCfg, profile: str) -> None:
    if profile not in LOAD_PROFILES:
        profile = "normal"
    for name, value in LOAD_PROFILES[profile].items():
        setattr(cfg, name, value)


def load_token_seed(path: Path) -> dict[str, list[dict[str, Any]]]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text()) or {}


def setup_logging(log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", "%Y-%m-%d %H:%M:%S")

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)

    file_h = RotatingFileHandler(
        log_dir / "scanner.log",
        maxBytes=20 * 1024 * 1024,
        backupCount=14,
        encoding="utf-8",
    )
    file_h.setLevel(logging.INFO)
    file_h.setFormatter(fmt)
    root.addHandler(file_h)

    err_h = RotatingFileHandler(
        log_dir / "errors.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=7,
        encoding="utf-8",
    )
    err_h.setLevel(logging.WARNING)
    err_h.setFormatter(fmt)
    root.addHandler(err_h)
    # httpx INFO records include complete URLs and may leak API keys from .env.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    # Консоль: только явные сообщения scanner.console (старт / heartbeat).
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(message)s"))
    console.setLevel(logging.INFO)
    console.propagate = False
    console.addHandler(ch)


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
    last_indexed     INTEGER NOT NULL DEFAULT 0,
    discovery_revision INTEGER NOT NULL DEFAULT 2
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

CREATE TABLE IF NOT EXISTS schema_meta (
    version          INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS contract_tokens (
    chain            TEXT NOT NULL,
    contract         TEXT NOT NULL,
    token            TEXT NOT NULL,
    source           TEXT NOT NULL DEFAULT 'transfer',
    first_seen_block INTEGER,
    last_seen_block  INTEGER,
    PRIMARY KEY (chain, contract, token)
);

CREATE TABLE IF NOT EXISTS address_scans (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    address          TEXT NOT NULL,
    scanned_at       TEXT NOT NULL,
    status           TEXT NOT NULL,
    total_usd        REAL NOT NULL,
    coverage         INTEGER NOT NULL,
    total_networks   INTEGER NOT NULL,
    note             TEXT
);

CREATE TABLE IF NOT EXISTS address_chain_scans (
    scan_id          INTEGER NOT NULL REFERENCES address_scans(id) ON DELETE CASCADE,
    chain            TEXT NOT NULL,
    has_code         INTEGER,
    status           TEXT NOT NULL,
    native_amount    REAL,
    native_usd       REAL,
    tokens_usd       REAL,
    total_usd        REAL,
    note             TEXT,
    PRIMARY KEY (scan_id, chain)
);

CREATE TABLE IF NOT EXISTS address_token_scans (
    scan_id          INTEGER NOT NULL REFERENCES address_scans(id) ON DELETE CASCADE,
    chain            TEXT NOT NULL,
    token            TEXT NOT NULL,
    symbol           TEXT,
    raw_amount       TEXT,
    amount           REAL,
    price_usd        REAL,
    usd_value        REAL,
    priced           INTEGER NOT NULL,
    PRIMARY KEY (scan_id, chain, token)
);

CREATE INDEX IF NOT EXISTS idx_contract_tokens_holder
    ON contract_tokens(chain, contract);
CREATE INDEX IF NOT EXISTS idx_address_scans_latest
    ON address_scans(address, scanned_at);

CREATE TABLE IF NOT EXISTS rabby_estimates (
    address          TEXT PRIMARY KEY,
    checked_at       TEXT NOT NULL,
    status           TEXT NOT NULL,
    estimated_usd    REAL,
    lower_usd        REAL,
    upper_usd        REAL,
    coverage         INTEGER NOT NULL,
    total_networks   INTEGER NOT NULL,
    uncertainty      REAL NOT NULL,
    chain_parts      TEXT NOT NULL,
    token_parts      TEXT NOT NULL,
    note             TEXT,
    rpc_scan_id      INTEGER NOT NULL REFERENCES address_scans(id)
);

CREATE TABLE IF NOT EXISTS contract_discoveries (
    chain            TEXT NOT NULL,
    address          TEXT NOT NULL,
    source           TEXT NOT NULL,
    observed_block   INTEGER,
    observed_tx      TEXT,
    actor            TEXT,
    first_seen_at    TEXT NOT NULL,
    PRIMARY KEY (chain, address, source)
);

CREATE TABLE IF NOT EXISTS contract_code_cache (
    chain            TEXT NOT NULL,
    address          TEXT NOT NULL,
    has_code         INTEGER NOT NULL,
    checked_at       TEXT NOT NULL,
    checked_block    INTEGER,
    PRIMARY KEY (chain, address)
);

CREATE INDEX IF NOT EXISTS idx_contract_discoveries_address
    ON contract_discoveries(address, chain);
CREATE INDEX IF NOT EXISTS idx_contract_code_cache_checked
    ON contract_code_cache(chain, checked_at);
"""


class DB:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._lock = threading.Lock()
        self.conn = sqlite3.connect(path, check_same_thread=False, timeout=60)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        chain_state_columns = {
            row["name"] for row in self.conn.execute("PRAGMA table_info(chain_state)")
        }
        if "discovery_revision" not in chain_state_columns:
            # Existing cursors are revision 1 (deployment-only). They are rewound
            # lazily by init_chain only when tx.to discovery is actually enabled.
            self.conn.execute(
                "ALTER TABLE chain_state ADD COLUMN discovery_revision INTEGER NOT NULL DEFAULT 1"
            )
        rabby_columns = {
            row["name"] for row in self.conn.execute("PRAGMA table_info(rabby_estimates)")
        }
        if "rpc_scan_id" not in rabby_columns:
            self.conn.execute("ALTER TABLE rabby_estimates ADD COLUMN rpc_scan_id INTEGER")
        self.conn.execute(
            """
            INSERT OR IGNORE INTO contract_discoveries(
                chain, address, source, observed_block, observed_tx, actor, first_seen_at
            )
            SELECT chain, lower(address), 'direct_deploy', created_block, created_tx,
                   creator, first_seen_at
            FROM contracts
            WHERE created_block IS NOT NULL OR created_tx IS NOT NULL
            """
        )
        self.conn.execute(
            "INSERT INTO schema_meta(version) SELECT 4 WHERE NOT EXISTS (SELECT 1 FROM schema_meta)"
        )
        self.conn.execute("UPDATE schema_meta SET version=4 WHERE version < 4")
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def init_chain(
        self, chain: str, start_block: int, discover_tx_to_contracts: bool = False
    ) -> int:
        with self._lock:
            row = self.conn.execute(
                """
                SELECT last_indexed, start_block, discovery_revision
                FROM chain_state WHERE chain=?
                """,
                (chain,),
            ).fetchone()
            if row is None:
                revision = 2 if discover_tx_to_contracts else 1
                self.conn.execute(
                    """
                    INSERT INTO chain_state(
                        chain, start_block, last_indexed, discovery_revision
                    ) VALUES(?,?,?,?)
                    """,
                    (chain, start_block, max(start_block - 1, 0), revision),
                )
                self.conn.commit()
                return max(start_block - 1, 0)
            if discover_tx_to_contracts and int(row["discovery_revision"]) < 2:
                rewound = min(int(row["last_indexed"]), max(start_block - 1, 0))
                self.conn.execute(
                    """
                    UPDATE chain_state
                    SET start_block=?, last_indexed=?, discovery_revision=2
                    WHERE chain=?
                    """,
                    (start_block, rewound, chain),
                )
                self.conn.commit()
                log.info(
                    "[%s] tx.to discovery enabled; cursor rewound %s -> %s",
                    chain,
                    row["last_indexed"],
                    rewound,
                )
                return rewound
            return int(row["last_indexed"])

    def last_indexed(self, chain: str) -> int:
        with self._lock:
            row = self.conn.execute(
                "SELECT last_indexed FROM chain_state WHERE chain=?", (chain,)
            ).fetchone()
            return int(row["last_indexed"]) if row else 0

    def discovery_revision(self, chain: str) -> int | None:
        with self._lock:
            row = self.conn.execute(
                "SELECT discovery_revision FROM chain_state WHERE chain=?", (chain,)
            ).fetchone()
            return int(row["discovery_revision"]) if row else None

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
            before = self.conn.total_changes
            self.conn.executemany(
                """
                INSERT OR IGNORE INTO contracts
                    (chain, address, created_block, created_tx, creator, first_seen_at)
                VALUES (?,?,?,?,?,?)
                """,
                rows,
            )
            inserted = self.conn.total_changes - before
            self.conn.executemany(
                """
                UPDATE contracts SET
                    created_block=COALESCE(created_block, ?),
                    created_tx=COALESCE(created_tx, ?),
                    creator=COALESCE(creator, ?)
                WHERE chain=? AND address=? AND ? IS NOT NULL
                """,
                [
                    (row[2], row[3], row[4], row[0], row[1], row[2])
                    for row in rows
                ],
            )
            self.conn.commit()
            return inserted

    def upsert_contract_discoveries(self, rows: list[tuple]) -> int:
        if not rows:
            return 0
        with self._lock:
            before = self.conn.total_changes
            self.conn.executemany(
                """
                INSERT OR IGNORE INTO contract_discoveries(
                    chain, address, source, observed_block, observed_tx, actor, first_seen_at
                ) VALUES (?,?,?,?,?,?,?)
                """,
                rows,
            )
            inserted = self.conn.total_changes - before
            self.conn.commit()
            return inserted

    def discovery_addresses(self, chain: str, source: str) -> set[str]:
        with self._lock:
            return {
                row["address"].lower()
                for row in self.conn.execute(
                    "SELECT address FROM contract_discoveries WHERE chain=? AND source=?",
                    (chain, source),
                )
            }

    def cached_code_statuses(
        self, chain: str, addresses: list[str], max_age_sec: int
    ) -> dict[str, bool]:
        if not addresses:
            return {}
        cutoff = datetime.fromtimestamp(
            time.time() - max_age_sec, timezone.utc
        ).isoformat()
        found: dict[str, bool] = {}
        with self._lock:
            for offset in range(0, len(addresses), 500):
                chunk = addresses[offset : offset + 500]
                placeholders = ",".join("?" for _ in chunk)
                rows = self.conn.execute(
                    f"""
                    SELECT address, has_code FROM contract_code_cache
                    WHERE chain=? AND checked_at>=? AND address IN ({placeholders})
                    """,
                    (chain, cutoff, *chunk),
                ).fetchall()
                found.update(
                    (row["address"].lower(), bool(row["has_code"])) for row in rows
                )
        return found

    def upsert_code_cache(self, rows: list[tuple]) -> None:
        if not rows:
            return
        with self._lock:
            self.conn.executemany(
                """
                INSERT INTO contract_code_cache(
                    chain, address, has_code, checked_at, checked_block
                ) VALUES (?,?,?,?,?)
                ON CONFLICT(chain, address) DO UPDATE SET
                    has_code=excluded.has_code,
                    checked_at=excluded.checked_at,
                    checked_block=excluded.checked_block
                """,
                rows,
            )
            self.conn.commit()

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
            return list(self.conn.execute("SELECT * FROM tokens WHERE chain=?", (chain,)))

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

    def chain_counts(self, chain: str) -> dict[str, int]:
        with self._lock:
            total = self.conn.execute(
                "SELECT COUNT(*) FROM contracts WHERE chain=?", (chain,)
            ).fetchone()[0]
            unchecked = self.conn.execute(
                "SELECT COUNT(*) FROM contracts WHERE chain=? AND last_checked_at IS NULL",
                (chain,),
            ).fetchone()[0]
            qual = self.conn.execute(
                "SELECT COUNT(*) FROM contracts WHERE chain=? AND last_status='qualifying'",
                (chain,),
            ).fetchone()[0]
            below = self.conn.execute(
                "SELECT COUNT(*) FROM contracts WHERE chain=? AND last_status='below'",
                (chain,),
            ).fetchone()[0]
        return {
            "contracts": int(total),
            "unchecked": int(unchecked),
            "qualifying": int(qual),
            "below": int(below),
        }

    def upsert_contract_tokens(self, rows: list[tuple[str, str, str, str, int]]) -> None:
        if not rows:
            return
        with self._lock:
            self.conn.executemany(
                """
                INSERT INTO contract_tokens(
                    chain, contract, token, source, first_seen_block, last_seen_block
                ) VALUES (?,?,?,?,?,?)
                ON CONFLICT(chain, contract, token) DO UPDATE SET
                    last_seen_block=CASE
                        WHEN excluded.last_seen_block > contract_tokens.last_seen_block
                        THEN excluded.last_seen_block ELSE contract_tokens.last_seen_block END
                """,
                [(*row, row[4]) for row in rows],
            )
            self.conn.commit()

    def tokens_for_contract(self, chain: str, contract: str) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self.conn.execute(
                    """
                    SELECT DISTINCT t.*
                    FROM tokens t
                    WHERE t.chain=?
                      AND (
                        t.source='seed'
                        OR EXISTS (
                            SELECT 1 FROM contract_tokens ct
                            WHERE ct.chain=t.chain
                              AND ct.token=t.address
                              AND ct.contract=?
                        )
                      )
                    ORDER BY t.address
                    """,
                    (chain, contract.lower()),
                )
            )

    def pending_addresses(
        self, recheck_sec: int, limit: int = 200, retry_sec: int | None = None,
        as_of: float | None = None,
    ) -> list[str]:
        now = time.time() if as_of is None else as_of
        cutoff = datetime.fromtimestamp(now - recheck_sec, timezone.utc).isoformat()
        retry_cutoff = datetime.fromtimestamp(
            now - (retry_sec if retry_sec is not None else recheck_sec), timezone.utc
        ).isoformat()
        with self._lock:
            rows = self.conn.execute(
                """
                SELECT lower(c.address) AS address
                FROM contracts c
                LEFT JOIN address_scans a
                  ON a.id=(
                    SELECT a2.id FROM address_scans a2
                    WHERE a2.address=lower(c.address)
                    ORDER BY a2.scanned_at DESC, a2.id DESC LIMIT 1
                  )
                WHERE a.scanned_at IS NULL OR a.scanned_at < ?
                   OR ((a.status='incomplete' OR a.coverage < a.total_networks)
                       AND a.scanned_at < ?)
                GROUP BY lower(c.address)
                ORDER BY COALESCE(a.scanned_at, '') ASC, lower(c.address)
                LIMIT ?
                """,
                (cutoff, retry_cutoff, limit),
            ).fetchall()
            return [row["address"] for row in rows]

    def save_rabby_estimate(self, address: str, estimate: dict[str, Any], scan_id: int) -> None:
        with self._lock:
            self.conn.execute(
                """INSERT INTO rabby_estimates(
                     address, checked_at, status, estimated_usd, lower_usd, upper_usd,
                     coverage, total_networks, uncertainty, chain_parts, token_parts, note, rpc_scan_id
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(address) DO UPDATE SET
                     checked_at=excluded.checked_at, status=excluded.status,
                     estimated_usd=excluded.estimated_usd, lower_usd=excluded.lower_usd,
                     upper_usd=excluded.upper_usd, coverage=excluded.coverage,
                     total_networks=excluded.total_networks, uncertainty=excluded.uncertainty,
                     chain_parts=excluded.chain_parts, token_parts=excluded.token_parts,
                     note=excluded.note, rpc_scan_id=excluded.rpc_scan_id""",
                (address.lower(), datetime.now(timezone.utc).isoformat(), estimate["status"],
                 estimate["estimated_usd"], estimate["lower_usd"], estimate["upper_usd"],
                 estimate["coverage"], estimate["total_networks"], estimate["uncertainty"],
                 json.dumps(estimate["chains"], ensure_ascii=False),
                 json.dumps(estimate["tokens"], ensure_ascii=False), estimate.get("note"), scan_id),
            )
            self.conn.commit()

    def pending_rabby_scan(self, retry_sec: int, as_of: float | None = None) -> sqlite3.Row | None:
        cutoff = datetime.fromtimestamp(
            (time.time() if as_of is None else as_of) - retry_sec, timezone.utc
        ).isoformat()
        with self._lock:
            return self.conn.execute(
                """SELECT a.* FROM address_scans a
                   LEFT JOIN rabby_estimates r ON r.address=a.address
                   WHERE a.id=(SELECT MAX(a2.id) FROM address_scans a2 WHERE a2.address=a.address)
                     AND EXISTS (SELECT 1 FROM address_chain_scans c WHERE c.scan_id=a.id
                                 AND c.status NOT IN ('complete','absent'))
                     AND (r.address IS NULL OR r.checked_at < ?)
                   ORDER BY COALESCE(r.checked_at, '') ASC, a.id LIMIT 1""", (cutoff,)
            ).fetchone()

    def rabby_estimate(self, address: str) -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM rabby_estimates WHERE address=?", (address.lower(),)
            ).fetchone()

    def save_address_scan(
        self,
        address: str,
        status: str,
        total_usd: float,
        coverage: int,
        total_networks: int,
        chain_rows: list[dict[str, Any]],
        token_rows: list[dict[str, Any]],
        note: str | None = None,
    ) -> int:
        now = datetime.now(timezone.utc).isoformat()
        address = address.lower()
        with self._lock:
            cur = self.conn.execute(
                """
                INSERT INTO address_scans(
                    address, scanned_at, status, total_usd, coverage, total_networks, note
                ) VALUES (?,?,?,?,?,?,?)
                """,
                (address, now, status, total_usd, coverage, total_networks, note),
            )
            scan_id = int(cur.lastrowid)
            self.conn.executemany(
                """
                INSERT INTO address_chain_scans(
                    scan_id, chain, has_code, status, native_amount, native_usd,
                    tokens_usd, total_usd, note
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                [
                    (
                        scan_id,
                        row["chain"],
                        row.get("has_code"),
                        row["status"],
                        row.get("native_amount"),
                        row.get("native_usd"),
                        row.get("tokens_usd"),
                        row.get("total_usd"),
                        row.get("note"),
                    )
                    for row in chain_rows
                ],
            )
            self.conn.executemany(
                """
                INSERT INTO address_token_scans(
                    scan_id, chain, token, symbol, raw_amount, amount,
                    price_usd, usd_value, priced
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                [
                    (
                        scan_id,
                        row["chain"],
                        row["token"],
                        row.get("symbol"),
                        str(row.get("raw_amount", "0")),
                        row.get("amount"),
                        row.get("price_usd"),
                        row.get("usd_value"),
                        1 if row.get("priced") else 0,
                    )
                    for row in token_rows
                ],
            )
            self.conn.execute(
                """
                UPDATE contracts
                SET last_checked_at=?, last_total_usd=?, last_status=?
                WHERE lower(address)=?
                """,
                (now, total_usd, status, address),
            )
            self.conn.commit()
            return scan_id

    def latest_address_scans(self) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self.conn.execute(
                    """
                    SELECT a.*
                    FROM address_scans a
                    WHERE a.id=(
                        SELECT a2.id FROM address_scans a2
                        WHERE a2.address=a.address
                        ORDER BY a2.scanned_at DESC, a2.id DESC LIMIT 1
                    )
                    ORDER BY a.total_usd DESC, a.address
                    """
                )
            )

    def address_scan_chains(self, scan_id: int) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self.conn.execute(
                    "SELECT * FROM address_chain_scans WHERE scan_id=? ORDER BY chain",
                    (scan_id,),
                )
            )

    def address_scan_tokens(self, scan_id: int) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self.conn.execute(
                    """
                    SELECT * FROM address_token_scans
                    WHERE scan_id=? AND CAST(raw_amount AS INTEGER) > 0
                    ORDER BY chain, COALESCE(usd_value, -1) DESC, token
                    """,
                    (scan_id,),
                )
            )

    def address_sources(self, address: str) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self.conn.execute(
                    """
                    SELECT chain, source, observed_block, observed_tx, actor, first_seen_at
                    FROM contract_discoveries WHERE lower(address)=?
                    ORDER BY first_seen_at, chain, source
                    """,
                    (address.lower(),),
                )
            )

    def aggregate_counts(self) -> dict[str, int]:
        with self._lock:
            rows = self.conn.execute(
                """
                SELECT status, COUNT(*) AS n FROM address_scans a
                WHERE a.id=(
                    SELECT a2.id FROM address_scans a2
                    WHERE a2.address=a.address
                    ORDER BY a2.scanned_at DESC, a2.id DESC LIMIT 1
                ) GROUP BY status
                """
            ).fetchall()
        return {row["status"]: int(row["n"]) for row in rows}

    def monitoring_snapshot(self) -> dict[str, Any]:
        """Return one cheap aggregate snapshot for monitoring.db."""
        with self._lock:
            unique_addresses = int(self.conn.execute(
                "SELECT COUNT(DISTINCT lower(address)) FROM contracts"
            ).fetchone()[0])
            contract_instances = int(self.conn.execute(
                "SELECT COUNT(*) FROM contracts"
            ).fetchone()[0])
            sources = {
                str(row["source"]): int(row["n"])
                for row in self.conn.execute(
                    "SELECT source,COUNT(*) AS n FROM contract_discoveries GROUP BY source"
                )
            }
            pending = int(self.conn.execute(
                "SELECT COUNT(DISTINCT lower(address)) FROM contracts WHERE last_checked_at IS NULL"
            ).fetchone()[0])
            oldest_age = self.conn.execute(
                "SELECT MAX(strftime('%s','now')-strftime('%s',first_seen_at)) "
                "FROM contracts WHERE last_checked_at IS NULL"
            ).fetchone()[0]
            completed = int(self.conn.execute(
                "SELECT COUNT(DISTINCT address) FROM address_scans"
            ).fetchone()[0])
            statuses = {
                str(row["status"]): int(row["n"])
                for row in self.conn.execute(
                    """
                    SELECT status,COUNT(*) AS n FROM address_scans a
                    WHERE a.id=(SELECT a2.id FROM address_scans a2 WHERE a2.address=a.address
                                ORDER BY a2.scanned_at DESC,a2.id DESC LIMIT 1)
                    GROUP BY status
                    """
                )
            }
            coverage = {
                f"{int(row['coverage'])}/{int(row['total_networks'])}": int(row["n"])
                for row in self.conn.execute(
                    """
                    SELECT coverage,total_networks,COUNT(*) AS n FROM address_scans a
                    WHERE a.id=(SELECT a2.id FROM address_scans a2 WHERE a2.address=a.address
                                ORDER BY a2.scanned_at DESC,a2.id DESC LIMIT 1)
                    GROUP BY coverage,total_networks
                    """
                )
            }
        return {
            "unique_addresses": unique_addresses,
            "contract_instances": contract_instances,
            "direct_deploy": sources.get("direct_deploy", 0),
            "active_call": sources.get("active_call", 0),
            "balance_pending": pending,
            "balance_oldest_age_sec": float(oldest_age) if oldest_age is not None else None,
            "balance_completed": completed,
            "qualifying": statuses.get("qualifying", 0),
            "below_count": statuses.get("below", 0),
            "incomplete": statuses.get("incomplete", 0),
            "coverage_json": coverage,
        }

    def discovery_count(self, chain: str, source: str) -> int:
        with self._lock:
            return int(self.conn.execute(
                "SELECT COUNT(*) FROM contract_discoveries WHERE chain=? AND source=?",
                (chain, source),
            ).fetchone()[0])


# ---------------------------------------------------------------------------
# RPC with cooldowns
# ---------------------------------------------------------------------------


class RpcError(Exception):
    def __init__(
        self,
        kind: str,
        message: str,
        retry_after: float | None = None,
        permanent: bool = False,
    ):
        super().__init__(message)
        self.kind = kind
        self.retry_after = retry_after
        self.permanent = permanent


def _short_url(url: str) -> str:
    p = urlparse(url)
    return p.netloc or url[:40]


def _parse_retry_after(headers: httpx.Headers) -> float | None:
    raw = headers.get("retry-after")
    if not raw:
        return None
    try:
        return max(1.0, float(raw))
    except ValueError:
        return None


def classify_rpc_problem(status: int | None, body: str, headers: httpx.Headers | None = None) -> RpcError | None:
    text = (body or "").lower()
    retry_after = _parse_retry_after(headers) if headers is not None else None
    if status in (401, 403) or any(h in text for h in AUTH_HINTS):
        return RpcError("auth", "authentication rejected", permanent=True)
    if status in (429,) or any(h in text for h in RATE_HINTS):
        return RpcError("rate_limit", "RPC rate limit", retry_after)
    if any(h in text for h in RANGE_HINTS):
        return RpcError("range", "RPC range limit", None)
    if status in RETRYABLE_HTTP:
        return RpcError("http", f"HTTP {status}", retry_after or 10)
    if status and status >= 400:
        return RpcError("http", f"HTTP {status}", retry_after)
    return None


@dataclass
class Endpoint:
    url: str
    cooldown_until: float = 0.0
    fail_streak: int = 0
    ok_streak: int = 0
    permanent_error: str | None = None
    batch_supported: bool = True
    last_error: str | None = None
    chain_verified: bool = False

    def available(self) -> bool:
        return self.permanent_error is None and time.time() >= self.cooldown_until

    def cool(self, seconds: float, cap: float) -> float:
        wait = min(cap, max(1.0, seconds))
        self.fail_streak += 1
        self.ok_streak = 0
        # экспонента от серии фейлов
        wait = min(cap, wait * (2 ** min(self.fail_streak - 1, 6)))
        wait *= random.uniform(0.85, 1.15)
        self.cooldown_until = time.time() + wait
        return wait

    def ok(self) -> None:
        self.fail_streak = 0
        self.ok_streak += 1
        self.cooldown_until = 0.0
        self.last_error = None

    def disable(self, reason: str) -> None:
        self.permanent_error = reason
        self.last_error = reason


class RpcPool:
    def __init__(
        self,
        chain: str,
        urls: list[str],
        cfg: AppCfg,
        expected_chain_id: int | None = None,
        global_sem: asyncio.Semaphore | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        if not urls:
            raise ValueError(f"{chain}: нет RPC URL")
        self.chain = chain
        self.cfg = cfg
        self.expected_chain_id = expected_chain_id
        self.endpoints = [Endpoint(u) for u in urls]
        self.timeout = cfg.http_timeout_sec
        self.retries = cfg.max_retries
        self.sem = asyncio.Semaphore(cfg.rpc_concurrency)
        self.global_sem = global_sem or asyncio.Semaphore(cfg.global_rpc_concurrency)
        self.transport = transport
        self.preflight_complete = False
        self._i = 0
        self._client: httpx.AsyncClient | None = None
        self.block_batch = cfg.block_batch_size
        self.receipt_batch = cfg.receipt_batch_size
        self.call_batch = min(cfg.receipt_batch_size, 20)
        self.paused_until = 0.0
        self.metric_requests = 0
        self.metric_successes = 0
        self.metric_errors: dict[str, int] = {}
        self.metric_latencies_ms: list[float] = []
        self.last_head: int | None = None

    async def __aenter__(self) -> "RpcPool":
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout, connect=15.0),
            transport=self.transport,
        )
        return self

    async def __aexit__(self, *a: Any) -> None:
        if self._client:
            await self._client.aclose()

    def shrink_batch(self, kind: str = "block") -> None:
        if not self.cfg.adaptive_batch:
            return
        attr = {"block": "block_batch", "receipt": "receipt_batch", "call": "call_batch"}.get(
            kind, "block_batch"
        )
        old = getattr(self, attr)
        new = max(1, old // 2)
        setattr(self, attr, new)
        if new != old:
            log.warning("[%s] %s batch %s -> %s", self.chain, kind, old, new)

    def grow_batch(self, kind: str = "block") -> None:
        if not self.cfg.adaptive_batch:
            return
        attr, limit = {
            "block": ("block_batch", self.cfg.block_batch_size),
            "receipt": ("receipt_batch", self.cfg.receipt_batch_size),
            "call": ("call_batch", min(self.cfg.receipt_batch_size, 20)),
        }.get(kind, ("block_batch", self.cfg.block_batch_size))
        if getattr(self, attr) < limit:
            setattr(self, attr, min(limit, getattr(self, attr) + 1))

    def _pick(self) -> Endpoint | None:
        n = len(self.endpoints)
        for i in range(n):
            ep = self.endpoints[(self._i + i) % n]
            if ep.available() and (not self.preflight_complete or ep.chain_verified):
                self._i = (self._i + i + 1) % n
                return ep
        return None

    def _soonest_wait(self) -> float:
        now = time.time()
        waits = [
            max(0.0, ep.cooldown_until - now)
            for ep in self.endpoints
            if ep.permanent_error is None
            and (not self.preflight_complete or ep.chain_verified)
        ]
        return min(waits) if waits else self.cfg.all_down_sleep_sec

    async def _wait_healthy_endpoint(self) -> Endpoint:
        while True:
            ep = self._pick()
            if ep is not None:
                return ep
            if BALANCE_RPC.get():
                raise RpcError("cooldown", "no ready endpoint for balance probe")
            usable = [
                item for item in self.endpoints
                if item.permanent_error is None
                and (not self.preflight_complete or item.chain_verified)
            ]
            if not usable:
                reasons = ", ".join(
                    f"{_short_url(item.url)}={item.permanent_error}" for item in self.endpoints
                )
                raise RpcError(
                    "permanent",
                    f"{self.chain}: all RPC endpoints disabled ({reasons})",
                    permanent=True,
                )
            wait = min(
                self.cfg.cooldown_max_sec,
                max(self.cfg.all_down_sleep_sec, self._soonest_wait()),
            )
            self.paused_until = time.time() + wait
            log.warning("[%s] all RPC endpoints cooling down for %.0fs", self.chain, wait)
            await asyncio.sleep(wait)

    async def _request_endpoint(self, ep: Endpoint, payload: Any) -> Any:
        assert self._client
        started = time.monotonic()
        self.metric_requests += 1
        try:
            try:
                # Queued work for one chain must not reserve global slots.
                async with self.sem:
                    async with self.global_sem:
                        if BALANCE_RPC.get() and not ep.available():
                            raise RpcError("cooldown", "endpoint became unavailable while queued")
                        response = await self._client.post(ep.url, json=payload)
            except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
                raise RpcError("network", type(exc).__name__) from exc

            body = response.text[:1200]
            if response.status_code >= 400:
                raise classify_rpc_problem(response.status_code, body, response.headers) or RpcError(
                    "http", f"HTTP {response.status_code}"
                )
            try:
                data = response.json()
            except (ValueError, json.JSONDecodeError) as exc:
                raise RpcError("malformed", "invalid JSON response") from exc
            if isinstance(data, dict) and data.get("error") is not None:
                error_text = json.dumps(data["error"], ensure_ascii=False)
                raise classify_rpc_problem(None, error_text) or RpcError("rpc", "JSON-RPC error")
            if not isinstance(data, (dict, list)):
                raise RpcError("malformed", f"unexpected JSON type: {type(data).__name__}")
            ep.ok()
            self.metric_successes += 1
            return data
        except Exception as exc:
            kind = exc.kind if isinstance(exc, RpcError) else type(exc).__name__
            self.metric_errors[kind] = self.metric_errors.get(kind, 0) + 1
            raise
        finally:
            self.metric_latencies_ms.append((time.monotonic() - started) * 1000)
            if len(self.metric_latencies_ms) > 5000:
                del self.metric_latencies_ms[:-2500]

    async def _post(self, payload: Any) -> tuple[Endpoint, Any]:
        last: Exception | None = None
        attempts = min(2, self.retries) if BALANCE_RPC.get() else self.retries
        for _attempt in range(max(1, attempts)):
            ep = await self._wait_healthy_endpoint()
            try:
                return ep, await self._request_endpoint(ep, payload)
            except RpcError as exc:
                last = exc
                if exc.kind == "cooldown":
                    continue
                ep.last_error = exc.kind
                if exc.permanent or exc.kind == "auth":
                    ep.disable(exc.kind)
                    log.warning(
                        "[%s] %s disabled for this run (%s)",
                        self.chain,
                        _short_url(ep.url),
                        exc.kind,
                    )
                    continue
                base = exc.retry_after or self.cfg.cooldown_min_sec
                if exc.kind == "network":
                    base *= 2
                slept = ep.cool(base, self.cfg.cooldown_max_sec)
                log.warning(
                    "[%s] %s %s, cooldown %.0fs",
                    self.chain,
                    _short_url(ep.url),
                    exc.kind,
                    slept,
                )
                if not BALANCE_RPC.get():
                    await asyncio.sleep(min(2.0, slept))
            except Exception as exc:
                last = exc
                ep.last_error = type(exc).__name__
                slept = ep.cool(self.cfg.cooldown_min_sec, self.cfg.cooldown_max_sec)
                log.warning(
                    "[%s] %s unexpected %s, cooldown %.0fs",
                    self.chain,
                    _short_url(ep.url),
                    type(exc).__name__,
                    slept,
                )
                if not BALANCE_RPC.get():
                    await asyncio.sleep(min(1.0, slept))
        if isinstance(last, RpcError):
            raise last
        raise RpcError("network", "RPC retries exhausted")

    async def call(self, method: str, params: list[Any]) -> Any:
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        _ep, data = await self._post(payload)
        if not isinstance(data, dict) or "result" not in data:
            raise RpcError("malformed", f"{method}: missing result")
        result = data["result"]
        if method == "eth_blockNumber" and result is not None:
            self.last_head = hex_int(result)
        return result

    async def batch_partial(self, calls: list[tuple[str, list[Any]]]) -> list[Any | Exception]:
        if not calls:
            return []
        if self.preflight_complete and not any(
            endpoint.chain_verified
            and endpoint.permanent_error is None
            and endpoint.batch_supported
            for endpoint in self.endpoints
        ):
            return list(
                await asyncio.gather(
                    *(self.call(method, params) for method, params in calls),
                    return_exceptions=True,
                )
            )
        payload = [
            {"jsonrpc": "2.0", "id": i, "method": method, "params": params}
            for i, (method, params) in enumerate(calls)
        ]
        endpoint: Endpoint | None = None
        try:
            endpoint, data = await self._post(payload)
        except RpcError:
            data = None
        if not isinstance(data, list):
            if endpoint is not None:
                endpoint.batch_supported = False
            return list(
                await asyncio.gather(
                    *(self.call(method, params) for method, params in calls),
                    return_exceptions=True,
                )
            )

        by_id = {item.get("id"): item for item in data if isinstance(item, dict)}
        out: list[Any | Exception] = [RpcError("rpc", "missing batch item") for _ in calls]
        retry_indexes: list[int] = []
        for i in range(len(calls)):
            item = by_id.get(i)
            if (
                not item
                or item.get("error") is not None
                or "result" not in item
                or item.get("result") is None
            ):
                retry_indexes.append(i)
            else:
                out[i] = item["result"]
        if retry_indexes:
            async def retry_one(index: int) -> Any:
                last_result: Any = None
                for _ in range(max(1, min(len(self.endpoints), self.retries))):
                    last_result = await self.call(*calls[index])
                    if last_result is not None:
                        return last_result
                return last_result

            retried = await asyncio.gather(
                *(retry_one(i) for i in retry_indexes),
                return_exceptions=True,
            )
            for i, result in zip(retry_indexes, retried):
                out[i] = result
        return out

    async def batch(self, calls: list[tuple[str, list[Any]]]) -> list[Any]:
        out = await self.batch_partial(calls)
        for item in out:
            if isinstance(item, Exception):
                raise item
        return out

    async def preflight(self) -> list[dict[str, Any]]:
        """Validate endpoint chain IDs and basic calls without exposing full URLs."""
        assert self._client
        report: list[dict[str, Any]] = []
        for ep in self.endpoints:
            row: dict[str, Any] = {"endpoint": _short_url(ep.url), "ok": False}
            try:
                chain_data = await self._request_endpoint(
                    ep,
                    {"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []},
                )
                if not isinstance(chain_data, dict) or "result" not in chain_data:
                    raise RpcError("malformed", "eth_chainId missing result")
                actual = hex_int(chain_data["result"])
                row["chain_id"] = actual
                if self.expected_chain_id is not None and actual != self.expected_chain_id:
                    reason = f"wrong chain id {actual}, expected {self.expected_chain_id}"
                    ep.disable(reason)
                    raise RpcError("wrong_chain", reason, permanent=True)
                ep.chain_verified = True

                head_data = await self._request_endpoint(
                    ep,
                    {"jsonrpc": "2.0", "id": 2, "method": "eth_blockNumber", "params": []},
                )
                if not isinstance(head_data, dict) or "result" not in head_data:
                    raise RpcError("malformed", "eth_blockNumber missing result")
                row["head"] = hex_int(head_data["result"])
                self.last_head = max(self.last_head or 0, int(row["head"]))
                try:
                    batch_data = await self._request_endpoint(
                        ep,
                        [
                            {"jsonrpc": "2.0", "id": 11, "method": "eth_chainId", "params": []},
                            {"jsonrpc": "2.0", "id": 12, "method": "eth_blockNumber", "params": []},
                        ],
                    )
                    ep.batch_supported = (
                        isinstance(batch_data, list)
                        and len(batch_data) == 2
                        and all(
                            isinstance(item, dict)
                            and item.get("error") is None
                            and "result" in item
                            for item in batch_data
                        )
                    )
                except RpcError:
                    ep.batch_supported = False
                row["batch"] = ep.batch_supported
                row["ok"] = True
            except RpcError as exc:
                row["error"] = exc.kind
                if exc.permanent or exc.kind in ("auth", "wrong_chain"):
                    ep.disable(exc.kind)
                else:
                    ep.last_error = exc.kind
            report.append(row)
        self.preflight_complete = True
        return report

    def active_endpoint(self) -> str:
        ep = next(
            (
                candidate for candidate in self.endpoints
                if candidate.available()
                and (not self.preflight_complete or candidate.chain_verified)
            ),
            None,
        )
        return _short_url(ep.url) if ep else "none"

    def usable(self) -> bool:
        return any(
            endpoint.permanent_error is None
            and (not self.preflight_complete or endpoint.chain_verified)
            for endpoint in self.endpoints
        )

    def take_metrics(self) -> dict[str, Any]:
        latencies = self.metric_latencies_ms
        result = {
            "requests": self.metric_requests,
            "successes": self.metric_successes,
            "errors": sum(self.metric_errors.values()),
            "errors_by_type": dict(self.metric_errors),
            "latency_p50_ms": percentile(latencies, 0.50),
            "latency_p95_ms": percentile(latencies, 0.95),
        }
        self.metric_requests = 0
        self.metric_successes = 0
        self.metric_errors = {}
        self.metric_latencies_ms = []
        return result


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
    try:
        return raw[:32].split(b"\x00", 1)[0].decode("utf-8", "replace") or None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Prices
# ---------------------------------------------------------------------------


class PriceBook:
    def __init__(self, timeout: int, batch_size: int = 40):
        self.timeout = timeout
        self.batch_size = max(1, batch_size)
        self.cache: dict[str, tuple[float, float]] = {}  # key -> (price, ts)
        self._lock = asyncio.Lock()
        self._missing_until: dict[str, float] = {}
        self._cooldown_until = 0.0

    async def fetch(self, keys: list[str], ttl: float = 120.0) -> dict[str, float]:
        keys = list(dict.fromkeys(keys))
        # Coalesce concurrent misses: addresses usually need the same seed prices.
        async with self._lock:
            now = time.time()
            missing = [k for k in keys if
                       (k not in self.cache or now - self.cache[k][1] > ttl)
                       and self._missing_until.get(k, 0) <= now]
            if missing and now >= self._cooldown_until:
                async with httpx.AsyncClient(timeout=min(self.timeout, 8)) as client:
                    for i in range(0, len(missing), self.batch_size):
                        chunk = missing[i:i + self.batch_size]
                        try:
                            r = await client.get(LLAMA_PRICES + ",".join(chunk))
                            if r.status_code == 429:
                                self._cooldown_until = time.time() + (_parse_retry_after(r.headers) or 30)
                                log.warning("DefiLlama rate limit; prices temporarily unavailable")
                                break
                            r.raise_for_status()
                            coins = r.json().get("coins") or {}
                            ts = time.time()
                            for k in chunk:
                                px = coins.get(k, {}).get("price")
                                if px is not None and math.isfinite(float(px)) and float(px) > 0:
                                    self.cache[k] = (float(px), ts)
                                else:
                                    self._missing_until[k] = ts + 30
                        except Exception as exc:
                            self._cooldown_until = time.time() + 10
                            log.warning("DefiLlama prices error: %s", type(exc).__name__)
                            break
        now = time.time()
        return {k: self.cache[k][0] for k in keys
                if k in self.cache and now - self.cache[k][1] <= ttl}


def llama_key(chain: ChainCfg, token: str | None = None) -> str:
    if token is None:
        return f"coingecko:{chain.native_coingecko}"
    return f"{chain.llama_chain}:{token.lower()}"


# ---------------------------------------------------------------------------
# Indexer
# ---------------------------------------------------------------------------


def normalize_evm_address(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    address = value.lower()
    if len(address) != 42 or not address.startswith("0x"):
        return None
    try:
        bytes.fromhex(address[2:])
    except ValueError:
        return None
    return address


def rpc_code_has_bytecode(value: Any) -> bool:
    if not isinstance(value, str) or not value.startswith("0x") or len(value) % 2:
        raise RpcError("malformed", "eth_getCode returned invalid bytecode")
    try:
        bytes.fromhex(value[2:])
    except ValueError as exc:
        raise RpcError("malformed", "eth_getCode returned invalid bytecode") from exc
    return bool(value[2:].lstrip("0"))


async def discover_tx_to_contracts(
    db: DB,
    chain: ChainCfg,
    rpc: RpcPool,
    cfg: AppCfg,
    candidates: dict[str, tuple[dict[str, Any], int]],
    known_contracts: set[str],
    known_active_calls: set[str],
    checked_block: int,
    seen_at: str,
) -> tuple[int, int, int]:
    """Persist unique top-level tx.to contracts; all RPC results are mandatory."""
    if not candidates:
        return 0, 0, 0

    cached = db.cached_code_statuses(
        chain.key,
        [address for address in candidates if address not in known_contracts],
        cfg.code_cache_ttl_sec,
    )
    contract_addresses = {
        address for address in candidates
        if address in known_contracts or cached.get(address) is True
    }
    unknown = [
        address for address in candidates
        if address not in known_contracts and address not in cached
    ]
    cache_rows: list[tuple[Any, ...]] = []
    offset = 0
    while offset < len(unknown):
        chunk_size = max(1, rpc.call_batch)
        chunk = unknown[offset : offset + chunk_size]
        try:
            results = await rpc.batch(
                [("eth_getCode", [address, "latest"]) for address in chunk]
            )
            if len(results) != len(chunk):
                raise RpcError("partial", "one or more eth_getCode results are missing")
            parsed = [rpc_code_has_bytecode(result) for result in results]
        except Exception:
            rpc.shrink_batch("call")
            raise
        for address, has_code in zip(chunk, parsed):
            cache_rows.append(
                (chain.key, address, int(has_code), seen_at, checked_block)
            )
            if has_code:
                contract_addresses.add(address)
        rpc.grow_batch("call")
        offset += len(chunk)

    # Delay cache writes until every code batch succeeded. A failed code lookup
    # therefore cannot make a partially checked range appear complete.
    db.upsert_code_cache(cache_rows)
    new_contract_rows = []
    discovery_rows = []
    for address in sorted(contract_addresses):
        tx, block_number = candidates[address]
        if address not in known_contracts:
            new_contract_rows.append(
                (chain.key, address, None, None, None, seen_at)
            )
        if address not in known_active_calls:
            discovery_rows.append(
                (
                    chain.key,
                    address,
                    "active_call",
                    block_number,
                    tx.get("hash"),
                    normalize_evm_address(tx.get("from")),
                    seen_at,
                )
            )

    inserted = db.upsert_contracts(new_contract_rows)
    sources_inserted = db.upsert_contract_discoveries(discovery_rows)
    known_contracts.update(contract_addresses)
    known_active_calls.update(row[1] for row in discovery_rows)
    return inserted, sources_inserted, len(unknown)


async def latest_block(rpc: RpcPool) -> int:
    return hex_int(await rpc.call("eth_blockNumber", []))


async def index_chain(
    db: DB,
    chain: ChainCfg,
    rpc: RpcPool,
    cfg: AppCfg,
    stop: asyncio.Event,
    from_block_override: int | None,
    to_block_override: int | None = None,
    once: bool = False,
    monitor: MonitorStore | None = None,
) -> None:
    previous_revision = db.discovery_revision(chain.key)
    activation_rewind = (
        cfg.discover_tx_to_contracts
        and previous_revision is not None
        and previous_revision < 2
    )
    last = db.init_chain(
        chain.key, chain.start_block, cfg.discover_tx_to_contracts
    )
    if (
        from_block_override is not None
        and last < from_block_override - 1
        and not activation_rewind
    ):
        last = from_block_override - 1
        db.set_last_indexed(chain.key, last)
    elif activation_rewind and from_block_override is not None:
        log.info(
            "[%s] --from-block ignored for the one-time tx.to rescan from %s",
            chain.key,
            last + 1,
        )

    log.info("[%s] indexer start from block %s", chain.key, last + 1)
    known_contracts = db.contract_addresses(chain.key)
    known_active_calls = (
        db.discovery_addresses(chain.key, "active_call")
        if cfg.discover_tx_to_contracts else set()
    )
    fixed_target: int | None = None
    range_failures = 0
    while not stop.is_set():
        await wait_if_paused(monitor, stop)
        if stop.is_set():
            break
        if fixed_target is not None and last >= fixed_target:
            log.info("[%s] finite index pass complete at %s", chain.key, last)
            return
        if not once and to_block_override is not None and last >= to_block_override:
            log.info("[%s] configured end block reached at %s", chain.key, last)
            return
        try:
            latest = await latest_block(rpc)
            target = max(0, latest - chain.confirmations)
            if to_block_override is not None:
                target = min(target, to_block_override)
            if once:
                if fixed_target is None:
                    fixed_target = target
                target = fixed_target
        except Exception as exc:
            log.warning("[%s] eth_blockNumber: %s", chain.key, exc)
            if once:
                raise
            await asyncio.sleep(cfg.all_down_sleep_sec)
            continue

        if last >= target:
            if once or (to_block_override is not None and last >= to_block_override):
                log.info("[%s] finite index pass complete at %s", chain.key, last)
                return
            await asyncio.sleep(6)
            continue

        batch_end = min(target, last + max(1, rpc.block_batch))
        numbers = list(range(last + 1, batch_end + 1))
        try:
            blocks = await rpc.batch(
                [("eth_getBlockByNumber", [hex(number), True]) for number in numbers]
            )
            if len(blocks) != len(numbers) or any(not isinstance(block, dict) for block in blocks):
                raise RpcError("partial", f"missing block in {numbers[0]}-{batch_end}")
            for expected, block in zip(numbers, blocks):
                if hex_int(block.get("number")) != expected:
                    raise RpcError("partial", f"wrong block returned for {expected}")

            deployments: list[dict[str, Any]] = []
            tx_to_candidates: dict[str, tuple[dict[str, Any], int]] = {}
            for block_number, block in zip(numbers, blocks):
                for tx in block.get("transactions") or []:
                    if tx.get("to") in (None, "", "0x"):
                        deployments.append(tx)
                    elif cfg.discover_tx_to_contracts:
                        address = normalize_evm_address(tx.get("to"))
                        if address is not None and address not in known_active_calls:
                            tx_to_candidates.setdefault(address, (tx, block_number))

            receipts: list[Any] = []
            for offset in range(0, len(deployments), max(1, rpc.receipt_batch)):
                chunk = deployments[offset : offset + max(1, rpc.receipt_batch)]
                part = await rpc.batch(
                    [("eth_getTransactionReceipt", [tx["hash"]]) for tx in chunk]
                )
                if len(part) != len(chunk) or any(not isinstance(receipt, dict) for receipt in part):
                    rpc.shrink_batch("receipt")
                    raise RpcError("partial", "one or more deployment receipts are missing")
                receipts.extend(part)
                rpc.grow_batch("receipt")

            now = datetime.now(timezone.utc).isoformat()
            new_rows: list[tuple[Any, ...]] = []
            direct_source_rows: list[tuple[Any, ...]] = []
            for tx, receipt in zip(deployments, receipts):
                address = normalize_evm_address(receipt.get("contractAddress"))
                if address is None:
                    continue
                block_number = hex_int(tx.get("blockNumber"))
                creator = normalize_evm_address(tx.get("from"))
                new_rows.append(
                    (
                        chain.key,
                        address,
                        block_number,
                        tx.get("hash"),
                        creator,
                        now,
                    )
                )
                direct_source_rows.append(
                    (
                        chain.key,
                        address,
                        "direct_deploy",
                        block_number,
                        tx.get("hash"),
                        creator,
                        now,
                    )
                )
            inserted = db.upsert_contracts(new_rows)
            db.upsert_contract_discoveries(direct_source_rows)
            known_contracts.update(row[1] for row in new_rows)

            active_inserted = 0
            active_sources = 0
            code_checked = 0
            if cfg.discover_tx_to_contracts:
                active_inserted, active_sources, code_checked = await discover_tx_to_contracts(
                    db,
                    chain,
                    rpc,
                    cfg,
                    tx_to_candidates,
                    known_contracts,
                    known_active_calls,
                    batch_end,
                    now,
                )

            if cfg.discover_tokens_from_transfers:
                await harvest_transfers(db, chain, rpc, numbers[0], batch_end)

            # The cursor moves only after every mandatory block, receipt and log range succeeded.
            db.set_last_indexed(chain.key, batch_end)
            last = batch_end
            range_failures = 0
            rpc.grow_batch("block")
            log.info(
                "[%s] idx %s..%s safe_head=%s lag=%s new=%s active_sources=%s "
                "code_checked=%s batch=%s",
                chain.key,
                numbers[0],
                batch_end,
                target,
                max(0, target - last),
                inserted + active_inserted,
                active_sources,
                code_checked,
                len(numbers),
            )
        except Exception as exc:
            range_failures += 1
            rpc.shrink_batch("block")
            log.warning(
                "[%s] range %s-%s not committed: %s",
                chain.key,
                numbers[0],
                batch_end,
                exc,
            )
            if once and range_failures >= max(1, cfg.max_retries):
                raise
            await asyncio.sleep(cfg.cooldown_min_sec)


async def harvest_transfers(
    db: DB, chain: ChainCfg, rpc: RpcPool, start: int, end: int
) -> None:
    known = db.contract_addresses(chain.key)
    if not known:
        return

    step = max(1, min(chain.logs_max_range, end - start + 1))
    token_rows: dict[str, tuple[str, str, None, None, str]] = {}
    relation_rows: dict[
        tuple[str, str], tuple[str, str, str, str, int]
    ] = {}
    known_list = sorted(known)
    holder_chunk_size = 50
    cursor = start
    while cursor <= end:
        range_end = min(end, cursor + step - 1)
        try:
            logs: list[dict[str, Any]] = []
            for offset in range(0, len(known_list), holder_chunk_size):
                holders = known_list[offset : offset + holder_chunk_size]
                holder_topics = ["0x" + pad_addr(holder) for holder in holders]
                recipient_filter: str | list[str] = (
                    holder_topics[0] if len(holder_topics) == 1 else holder_topics
                )
                part = await rpc.call(
                    "eth_getLogs",
                    [
                        {
                            "fromBlock": hex(cursor),
                            "toBlock": hex(range_end),
                            "topics": [TRANSFER_TOPIC, None, recipient_filter],
                        }
                    ],
                )
                if not isinstance(part, list):
                    raise RpcError("malformed", "eth_getLogs did not return a list")
                logs.extend(part)
        except RpcError as exc:
            if exc.kind in ("range", "rate_limit") and step > 1:
                step = max(1, step // 2)
                log.info("[%s] eth_getLogs %s, range -> %s", chain.key, exc.kind, step)
                continue
            raise

        for item in logs:
            topics = item.get("topics") or []
            if len(topics) < 3:
                continue
            contract = topic_to_addr(topics[2])
            if contract not in known:
                continue
            token = (item.get("address") or "").lower()
            if not token or token == ZERO:
                continue
            block_number = hex_int(item.get("blockNumber"))
            token_rows[token] = (chain.key, token, None, None, "transfer_log")
            relation_rows[(contract, token)] = (
                chain.key,
                contract,
                token,
                "transfer_log",
                block_number,
            )
        cursor = range_end + 1
        if step < chain.logs_max_range:
            step = min(chain.logs_max_range, step * 2)

    db.upsert_tokens(list(token_rows.values()))
    db.upsert_contract_tokens(list(relation_rows.values()))


# ---------------------------------------------------------------------------
# Balances
# ---------------------------------------------------------------------------


def classify_address_scan(
    total_usd: float,
    chain_rows: list[dict[str, Any]],
    min_usd: float,
) -> tuple[str, int]:
    coverage = sum(1 for row in chain_rows if row.get("has_code") is not None)
    if total_usd >= min_usd:
        return "qualifying", coverage
    if chain_rows and all(row["status"] in ("complete", "absent") for row in chain_rows):
        return "below", coverage
    return "incomplete", coverage


async def scan_address_chain(
    db: DB,
    chain: ChainCfg,
    rpc: RpcPool | None,
    prices: PriceBook,
    address: str,
    balance_sem: asyncio.Semaphore,
    timeout_sec: float = 20,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    # Waiting for our local scheduler is not counted as a slow RPC.
    async with balance_sem:
        policy = BALANCE_RPC.set(True)
        try:
            return await _scan_address_chain(db, chain, rpc, prices, address, timeout_sec)
        finally:
            BALANCE_RPC.reset(policy)


async def _scan_address_chain(
    db: DB, chain: ChainCfg, rpc: RpcPool | None, prices: PriceBook,
    address: str, timeout_sec: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    base_row: dict[str, Any] = {
        "chain": chain.key,
        "has_code": None,
        "status": "rpc_error",
        "native_amount": None,
        "native_usd": None,
        "tokens_usd": None,
        "total_usd": None,
        "note": None,
    }
    if rpc is None:
        base_row["note"] = "no usable RPC"
        return base_row, []

    token_rows: list[dict[str, Any]] = []
    try:
        async with asyncio.timeout(timeout_sec):
            code = await rpc.call("eth_getCode", [address, "latest"])
            if not isinstance(code, str) or not code.startswith("0x") or len(code) % 2:
                raise RpcError("malformed", "eth_getCode missing result")
            bytes.fromhex(code[2:])
            if not code[2:].lstrip("0"):
                return {
                    **base_row,
                    "has_code": 0,
                    "status": "absent",
                    "native_amount": 0.0,
                    "native_usd": 0.0,
                    "tokens_usd": 0.0,
                    "total_usd": 0.0,
                }, []

            base_row["has_code"] = 1
            tokens = db.tokens_for_contract(chain.key, address)
            for token in tokens:
                if token["decimals"] is not None and token["symbol"] is not None:
                    continue
                decimals = token["decimals"]
                symbol = token["symbol"]
                try:
                    if decimals is None:
                        raw_decimals = await rpc.call(
                            "eth_call",
                            [{"to": token["address"], "data": DECIMALS_SEL}, "latest"],
                        )
                        value = decode_uint(raw_decimals)
                        decimals = value if 0 <= value <= 36 else None
                    if symbol is None:
                        raw_symbol = await rpc.call(
                            "eth_call",
                            [{"to": token["address"], "data": SYMBOL_SEL}, "latest"],
                        )
                        symbol = decode_string(raw_symbol)
                except Exception as exc:
                    log.debug("[%s] token metadata %s: %s", chain.key, token["address"], exc)
                db.upsert_tokens(
                    [
                        (
                            chain.key,
                            token["address"],
                            symbol,
                            decimals,
                            token["source"] or "meta",
                        )
                    ]
                )
            tokens = db.tokens_for_contract(chain.key, address)

            price_keys = [llama_key(chain)] + [
                llama_key(chain, token["address"]) for token in tokens
            ]
            book = await prices.fetch(price_keys)
            raw_native = await rpc.call("eth_getBalance", [address, "latest"])
            if raw_native is None:
                raise RpcError("partial", "native balance missing")
            native_raw = decode_uint(raw_native)
            native_amount = native_raw / (10 ** chain.native_decimals)
            native_price = book.get(llama_key(chain))
            native_usd = native_amount * native_price if native_price is not None else None
            base_row.update(native_amount=native_amount, native_usd=native_usd,
                            tokens_usd=0.0, total_usd=native_usd or 0.0)
            token_rows.append({
                "chain": chain.key, "token": "native", "symbol": chain.native_symbol,
                "raw_amount": native_raw, "amount": native_amount,
                "price_usd": native_price, "usd_value": native_usd,
                "priced": native_price is not None,
            })

            holder_data = BALANCE_OF_SEL + pad_addr(address)
            token_rpc_error = False
            unpriced_positive = native_raw > 0 and native_price is None
            tokens_usd = 0.0
            for offset in range(0, len(tokens), max(1, rpc.call_batch)):
                chunk = tokens[offset : offset + max(1, rpc.call_batch)]
                part = await rpc.batch_partial(
                    [
                        (
                            "eth_call",
                            [{"to": token["address"], "data": holder_data}, "latest"],
                        )
                        for token in chunk
                    ]
                )
                token_rpc_error |= len(part) != len(chunk)


                for token, result in zip(chunk, part):
                    if isinstance(result, Exception) or result is None:
                        token_rpc_error = True
                        continue
                    try:
                        raw_amount = decode_uint(result)
                    except Exception:
                        token_rpc_error = True
                        continue
                    if raw_amount <= 0:
                        continue
                    decimals = token["decimals"]
                    amount = raw_amount / (10 ** int(decimals)) if decimals is not None else None
                    price = book.get(llama_key(chain, token["address"]))
                    priced = amount is not None and price is not None
                    usd_value = amount * price if priced else None
                    if usd_value is not None:
                        tokens_usd += usd_value
                    else:
                        unpriced_positive = True
                    token_rows.append(
                        {
                            "chain": chain.key,
                            "token": token["address"],
                            "symbol": token["symbol"] or token["address"][:10],
                            "raw_amount": raw_amount,
                            "amount": amount,
                            "price_usd": price,
                            "usd_value": usd_value,
                            "priced": priced,
                        }
                    )
                base_row.update(tokens_usd=tokens_usd, total_usd=(native_usd or 0.0) + tokens_usd)

            known_total = (native_usd or 0.0) + tokens_usd
            if token_rpc_error:
                chain_status = "partial"
                note = "one or more token balance calls failed"
            elif unpriced_positive:
                chain_status = "price_missing"
                note = "positive balance without metadata or price"
            else:
                chain_status = "complete"
                note = None
            return {
                **base_row,
                "has_code": 1,
                "status": chain_status,
                "native_amount": native_amount,
                "native_usd": native_usd,
                "tokens_usd": tokens_usd,
                "total_usd": known_total,
                "note": note,
            }, token_rows
    except Exception as exc:
        base_row["note"] = "chain_timeout" if isinstance(exc, TimeoutError) else type(exc).__name__
        if isinstance(exc, RpcError):
            base_row["note"] = exc.kind
        if base_row["total_usd"] is not None:
            base_row["status"] = "partial"
        return base_row, token_rows


class RabbyClient:
    """Optional public GETs only; auth failures disable this client for the run."""

    def __init__(self, cfg: AppCfg, transport: httpx.AsyncBaseTransport | None = None):
        self.interval = cfg.rabby_request_interval_sec
        self.client = httpx.AsyncClient(
            base_url="https://api.rabby.io", timeout=cfg.rabby_timeout_sec,
            headers={"Accept": "application/json"}, transport=transport,
        )
        self.lock = asyncio.Lock()
        self.next_request = 0.0
        self.cooldown_until = 0.0
        self.disabled = False

    async def close(self) -> None:
        await self.client.aclose()

    async def get(self, path: str, address: str) -> Any:
        async with self.lock:
            if self.disabled:
                raise RpcError("auth", "Rabby disabled for this run", permanent=True)
            if time.monotonic() < self.cooldown_until:
                raise RpcError("cooldown", "Rabby cooling down")
            await asyncio.sleep(max(0, self.next_request - time.monotonic()))
            self.next_request = time.monotonic() + self.interval
            try:
                response = await self.client.get(path, params={"id": address})
                if response.status_code in (401, 403):
                    self.disabled = True
                    raise RpcError("auth", "Rabby authentication required", permanent=True)
                if response.status_code == 429:
                    self.cooldown_until = time.monotonic() + (_parse_retry_after(response.headers) or 60)
                    raise RpcError("rate_limit", "Rabby rate limit")
                response.raise_for_status()
                data = response.json()
                if isinstance(data, dict) and (
                    data.get("error_code") not in (None, 0) or data.get("error")
                ):
                    raise RpcError("api_error", "Rabby error envelope")
                return data
            except (httpx.HTTPError, ValueError) as exc:
                self.cooldown_until = time.monotonic() + 30
                raise RpcError("api_error", type(exc).__name__) from exc

    async def snapshot(self, address: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        total = await self.get("/v1/user/total_balance", address)
        tokens = await self.get("/v1/user/cache_token_list", address)
        if not isinstance(total, dict) or not isinstance(total.get("chain_list"), list):
            raise RpcError("malformed", "Rabby chain_list missing")
        if not isinstance(tokens, list) or any(not isinstance(t, dict) for t in tokens):
            raise RpcError("malformed", "Rabby token list missing")
        return total, tokens


def _nonnegative_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) and result >= 0 else None
    except (TypeError, ValueError):
        return None


def build_rabby_estimate(
    total: dict[str, Any], tokens: list[dict[str, Any]],
    chains: dict[str, ChainCfg], code_rows: list[dict[str, Any]],
    threshold: float, uncertainty: float,
) -> dict[str, Any]:
    # Map by numeric chain ID. Rabby aliases (eth, arb, xdai...) are not our keys.
    by_id: dict[int, dict[str, Any]] = {}
    aliases: dict[str, str] = {}
    supported = {chain.chain_id: key for key, chain in chains.items()}
    for item in total.get("chain_list", []):
        if not isinstance(item, dict):
            continue
        try:
            chain_id = int(item["community_id"])
        except (KeyError, ValueError, TypeError):
            continue
        if chain_id in by_id:
            raise RpcError("malformed", "duplicate Rabby chain")
        by_id[chain_id] = item
        if chain_id in supported:
            aliases[str(item.get("id"))] = supported[chain_id]
    codes = {row["chain"]: row.get("has_code") for row in code_rows}
    parts, token_parts = [], []
    known = 0.0
    covered = 0
    missing = False
    for key, chain in chains.items():
        code = codes.get(key)
        item = by_id.get(chain.chain_id)
        amount = _nonnegative_number(item.get("usd_value")) if item else None
        if code == 0:
            amount = 0.0  # Never count EOA holdings.
        elif code is None:
            amount = None
        if amount is None:
            missing = True
        else:
            known += amount
            covered += 1
        parts.append({"chain": key, "has_code": code, "estimated_usd": amount})
    for token in tokens:
        key = aliases.get(str(token.get("chain")))
        if key is None or codes.get(key) != 1 or token.get("is_scam") is True:
            continue
        amount = _nonnegative_number(token.get("amount"))
        price = _nonnegative_number(token.get("price"))
        if amount is None:
            missing = True
            continue
        if amount <= 0:
            continue
        priced = price is not None and price > 0
        missing |= not priced
        token_parts.append({
            "chain": key, "token": str(token.get("id", "")),
            "symbol": str(token.get("symbol") or token.get("id", "")),
            "raw_amount": str(token.get("raw_amount_str", token.get("raw_amount", ""))),
            "amount": amount, "price_usd": price if priced else None,
            "usd_value": amount * price if priced else None, "priced": priced,
        })
    # If estimate = real * (1 +/- uncertainty), these are the real-value bounds.
    value = known if covered else None
    delta = Decimal(str(uncertainty))
    lower = float(Decimal(str(value)) / (1 + delta)) if value is not None else None
    upper = float(Decimal(str(value)) / (1 - delta)) if value is not None and not missing else None
    if missing:
        status = "incomplete"
    elif lower >= threshold:
        status = "estimated_above"
    elif upper >= threshold:
        status = "near_threshold"
    else:
        status = "estimated_below"
    return {
        "status": status, "estimated_usd": value, "lower_usd": lower, "upper_usd": upper,
        "coverage": covered, "total_networks": len(chains), "uncertainty": uncertainty,
        "chains": parts, "tokens": token_parts,
        "note": "Rabby estimate only; uncertainty is an assumption, not a guarantee"
                + ("; missing code, network balance or token price; upper bound unknown" if missing else ""),
    }


async def rabby_fallback_loop(
    db: DB, chains: dict[str, ChainCfg], cfg: AppCfg, stop: asyncio.Event,
    rpc_finished: asyncio.Event, once: bool,
) -> None:
    client = RabbyClient(cfg)
    snapshot_at = time.time() if once else None
    try:
        while not stop.is_set():
            if client.disabled:
                return
            row = db.pending_rabby_scan(cfg.balance_retry_sec, snapshot_at)
            if row is None or time.monotonic() < client.cooldown_until:
                if once and rpc_finished.is_set() and row is None:
                    return
                if once and time.monotonic() < client.cooldown_until:
                    # A finite pass must not wait through service cooldowns.
                    return
                try:
                    await asyncio.wait_for(stop.wait(), timeout=1)
                except TimeoutError:
                    continue
                return
            try:
                total, tokens = await client.snapshot(row["address"])
                estimate = build_rabby_estimate(
                    total, tokens, chains,
                    [dict(part) for part in db.address_scan_chains(row["id"])],
                    cfg.min_usd, cfg.rabby_uncertainty,
                )
            except Exception as exc:
                estimate = {
                    "status": "incomplete", "estimated_usd": None, "lower_usd": None,
                    "upper_usd": None, "coverage": 0, "total_networks": len(chains),
                    "uncertainty": cfg.rabby_uncertainty, "chains": [], "tokens": [],
                    "note": "Rabby unavailable: " + (exc.kind if isinstance(exc, RpcError) else type(exc).__name__),
                }
            db.save_rabby_estimate(row["address"], estimate, row["id"])
            log.info("[rabby] %s estimate=%s status=%s coverage=%s/%s",
                     row["address"], estimate["estimated_usd"], estimate["status"],
                     estimate["coverage"], len(chains))
    finally:
        await client.close()


async def check_multichain_balances(
    db: DB,
    chains: dict[str, ChainCfg],
    pools: dict[str, RpcPool],
    prices: PriceBook,
    cfg: AppCfg,
    min_usd: float,
    stop: asyncio.Event,
    once: bool = False,
    monitor: MonitorStore | None = None,
) -> None:
    log.info("multichain balance checker online (%s networks, %s addresses, %s chain slots)",
             len(chains), cfg.balance_concurrency, cfg.balance_chain_concurrency)
    chain_sem = asyncio.Semaphore(cfg.balance_chain_concurrency)
    rpc_finished = asyncio.Event()
    fallback = asyncio.create_task(
        rabby_fallback_loop(db, chains, cfg, stop, rpc_finished, once), name="rabby-fallback"
    ) if cfg.rabby_fallback else None

    async def scan_one(address: str) -> None:
        started = time.monotonic()
        jobs = {
            asyncio.create_task(scan_address_chain(
                db, chain, pools.get(chain.key), prices, address, chain_sem,
                cfg.balance_chain_timeout_sec,
            )): chain.key for chain in chains.values()
        }
        try:
            done, unfinished = await asyncio.wait(jobs, timeout=cfg.balance_address_timeout_sec)
            results = []
            for task, key in jobs.items():
                if task in done:
                    results.append(task.result())
                else:
                    results.append(({
                        "chain": key, "has_code": None, "status": "rpc_error",
                        "total_usd": None, "note": "address_timeout",
                    }, []))
            for task in unfinished:
                task.cancel()
            await asyncio.gather(*unfinished, return_exceptions=True)
        finally:
            for task in jobs:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*jobs, return_exceptions=True)
        chain_rows = [item[0] for item in results]
        token_rows = [token for item in results for token in item[1]]
        total_usd = sum(float(row.get("total_usd") or 0.0) for row in chain_rows)
        status, coverage = classify_address_scan(total_usd, chain_rows, min_usd)
        notes = sorted({row["status"] for row in chain_rows
                        if row["status"] not in ("complete", "absent")})
        db.save_address_scan(
            address, status, total_usd, coverage, len(chains),
            chain_rows, token_rows, ", ".join(notes) or None,
        )
        log.info("[balances] %s $%.2f %s coverage=%s/%s elapsed=%.2fs",
                 address, total_usd, status, coverage, len(chains), time.monotonic() - started)

    active: dict[asyncio.Task[Any], str] = {}
    stop_job = asyncio.create_task(stop.wait())
    snapshot_at = time.time() if once else None
    try:
        while not stop.is_set():
            await wait_if_paused(monitor, stop)
            if stop.is_set():
                break
            pending = db.pending_addresses(
                cfg.recheck_interval_sec, limit=max(80, cfg.balance_concurrency * 2),
                retry_sec=cfg.balance_retry_sec, as_of=snapshot_at,
            )
            in_flight = set(active.values())
            for address in pending:
                if len(active) >= cfg.balance_concurrency:
                    break
                if address not in in_flight:
                    active[asyncio.create_task(scan_one(address), name=f"balance-{address}")] = address
                    in_flight.add(address)
            if not active:
                if once:
                    break
                try:
                    await asyncio.wait_for(stop.wait(), timeout=3)
                except TimeoutError:
                    pass
                continue
            done, _ = await asyncio.wait([*active, stop_job], return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task is not stop_job:
                    del active[task]
                    task.result()
            if fallback is not None and fallback.done():
                fallback.result()
        rpc_finished.set()
        if once and fallback is not None and not stop.is_set():
            await fallback
    finally:
        rpc_finished.set()
        jobs = [*active, stop_job] + ([fallback] if fallback is not None else [])
        for task in jobs:
            if not task.done():
                task.cancel()
        await asyncio.gather(*jobs, return_exceptions=True)


# ---------------------------------------------------------------------------
# Excel
# ---------------------------------------------------------------------------


HEADER_FILL = PatternFill("solid", fgColor="1F4E79")
HEADER_FONT = Font(color="FFFFFF", bold=True, name="Calibri", size=11)
OK_FILL = PatternFill("solid", fgColor="C6EFCE")
BELOW_FILL = PatternFill("solid", fgColor="FFF2CC")
INCOMPLETE_FILL = PatternFill("solid", fgColor="F4CCCC")
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


def export_xlsx(
    db: DB,
    cfg: AppCfg,
    chains: dict[str, ChainCfg],
    min_usd: float,
    snapshot: bool = False,
) -> tuple[Path, Path, Path]:
    cfg.export_dir.mkdir(parents=True, exist_ok=True)
    if snapshot:
        day = datetime.now(timezone.utc).strftime("%Y%m%d")
        folder = cfg.export_dir / "archive"
        folder.mkdir(parents=True, exist_ok=True)
        paths = (
            folder / f"qualifying_{day}.xlsx",
            folder / f"below_threshold_{day}.xlsx",
            folder / f"incomplete_{day}.xlsx",
        )
    else:
        paths = (
            cfg.export_dir / "qualifying.xlsx",
            cfg.export_dir / "below_threshold.xlsx",
            cfg.export_dir / "incomplete.xlsx",
        )

    headers = [
        "Address",
        "Total USD",
        "Coverage",
        "Status",
        "Networks with bytecode",
        "Network balances",
        "Tokens",
        "Discovery sources",
        "First seen (UTC)",
        "Last seen (UTC)",
        "Checked (UTC)",
        "Notes",
        "Rabby estimate USD",
        "Rabby lower USD (assumed tolerance)",
        "Rabby upper USD (assumed tolerance)",
        "Rabby status",
        "Rabby coverage",
        "Rabby checked (UTC)",
        "Rabby networks / tokens",
        "Rabby notes",
    ]

    def prepared(row: sqlite3.Row) -> list[Any]:
        chain_parts = db.address_scan_chains(row["id"])
        token_parts = db.address_scan_tokens(row["id"])
        sources = db.address_sources(row["address"])
        with_code = [
            chains[item["chain"]].display_name if item["chain"] in chains else item["chain"]
            for item in chain_parts
            if item["has_code"] == 1
        ]
        networks = []
        for item in chain_parts:
            if item["has_code"] != 1:
                continue
            amount = float(item["total_usd"] or 0.0)
            networks.append(f"{item['chain']}=${amount:,.2f} ({item['status']})")
        token_values = []
        for item in token_parts:
            label = item["symbol"] or item["token"][:10]
            if item["priced"]:
                token_values.append(
                    f"{item['chain']}:{label}=${float(item['usd_value'] or 0):,.2f}"
                )
            else:
                amount = item["amount"] if item["amount"] is not None else item["raw_amount"]
                token_values.append(f"{item['chain']}:{label}={amount} (unpriced)")
        source_values = [
            f"{item['chain']}:{item['source']}:{item['observed_block']}:"
            f"{item['observed_tx'] or ''}"
            for item in sources
        ]
        seen = [item["first_seen_at"] for item in sources if item["first_seen_at"]]
        estimate = db.rabby_estimate(row["address"])
        estimate_values: list[Any] = [None] * 8
        if estimate is not None:
            summary = "; ".join(
                f"{part['chain']}=${part['estimated_usd']}"
                for part in json.loads(estimate["chain_parts"]) if part.get("has_code") == 1
            )
            tokens_text = "; ".join(
                f"{part['chain']}:{part['symbol']}={part['amount']}"
                + (f" (${part['usd_value']:.2f})" if part["priced"] else " (unpriced)")
                for part in json.loads(estimate["token_parts"])
            )
            estimate_values = [
                estimate["estimated_usd"], estimate["lower_usd"], estimate["upper_usd"],
                estimate["status"], f"{estimate['coverage']}/{estimate['total_networks']}",
                estimate["checked_at"], (summary + " | " + tokens_text)[:32000],
                (estimate["note"] or "") + f"; assumed deviation={estimate['uncertainty']:.0%}"
                + ("; relates to an older RPC scan" if estimate["rpc_scan_id"] != row["id"] else ""),
            ]
        return [
            row["address"],
            float(row["total_usd"] or 0.0),
            f"{row['coverage']}/{row['total_networks']}",
            row["status"],
            "; ".join(with_code),
            "; ".join(networks),
            "; ".join(token_values),
            "; ".join(source_values),
            min(seen) if seen else None,
            max(seen) if seen else None,
            row["scanned_at"],
            row["note"],
        ] + estimate_values

    def build(path: Path, rows: list[sqlite3.Row], title: str, fill: PatternFill) -> None:
        tmp = path.with_suffix(".tmp.xlsx")
        wb = Workbook()
        ws = wb.active
        ws.title = title[:31]
        ws.append(headers)
        for row in rows:
            ws.append(prepared(row))
            for cell in ws[ws.max_row]:
                cell.border = THIN
                cell.alignment = Alignment(vertical="top", wrap_text=True)
                cell.fill = fill
            ws.cell(ws.max_row, 2).number_format = MONEY
        info = wb.create_sheet("Parameters")
        info.append(["Parameter", "Value"])
        info.append(["USD threshold", min_usd])
        info.append(["Exported UTC", datetime.now(timezone.utc).isoformat()])
        info.append(["Database", str(db.path)])
        info.append(["Rows", len(rows)])
        info.append(["Rabby", "Supplementary estimate; never changes RPC status or total"])
        _style_header(ws)
        _style_header(info)
        _autosize(
            ws,
            {1: 46, 2: 16, 3: 12, 4: 14, 5: 34, 6: 60, 7: 70, 8: 70,
             9: 24, 10: 24, 11: 24, 12: 35, 13: 20, 14: 24, 15: 24,
             16: 22, 17: 16, 18: 24, 19: 70, 20: 60},
        )
        _autosize(info, {1: 24, 2: 70})
        wb.save(tmp)
        tmp.replace(path)

    latest = db.latest_address_scans()
    # A database can contain scans made with a different --min-usd value.
    # Classify exports from the actual latest total, not the historic status label.
    qualifying = [
        row for row in latest
        if float(row["total_usd"] or 0.0) >= min_usd
    ]
    below = [
        row for row in latest
        if row["status"] == "below"
        and 0.0 < float(row["total_usd"] or 0.0) < min_usd
    ]
    incomplete = [
        row for row in latest
        if row["status"] == "incomplete" and float(row["total_usd"] or 0.0) < min_usd
    ]
    build(paths[0], qualifying, "Qualifying", OK_FILL)
    build(paths[1], below, "Below threshold", BELOW_FILL)
    build(paths[2], incomplete, "Incomplete", INCOMPLETE_FILL)
    log.info(
        "xlsx qualifying=%s below=%s incomplete=%s",
        len(qualifying),
        len(below),
        len(incomplete),
    )
    return paths


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def seed_tokens(db: DB, seed: dict[str, list[dict[str, Any]]], enabled: list[str]) -> None:
    rows = []
    for chain in enabled:
        for t in seed.get(chain, []):
            rows.append((chain, t["address"].lower(), t.get("symbol"), t.get("decimals"), "seed"))
    db.upsert_tokens(rows)


async def safe_head(rpc: RpcPool) -> int | None:
    try:
        return await latest_block(rpc)
    except Exception as e:
        log.warning("[%s] не удалось снять head: %s", rpc.chain, e)
        return None


def write_status(path: Path, lines: list[str]) -> None:
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


async def wait_if_paused(monitor: MonitorStore | None, stop: asyncio.Event) -> None:
    while monitor is not None and monitor.setting("scanner_paused", "0") == "1":
        if stop.is_set():
            return
        try:
            await asyncio.wait_for(stop.wait(), timeout=1)
        except asyncio.TimeoutError:
            pass


def backup_database(db: DB, backup_dir: Path, keep: int = 7) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    target = backup_dir / f"contracts_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')}.db"
    destination = sqlite3.connect(target)
    try:
        with db._lock:
            db.conn.backup(destination)
    finally:
        destination.close()
    backups = sorted(backup_dir.glob("contracts_*.db"), key=lambda item: item.stat().st_mtime)
    for old in backups[:-max(1, keep)]:
        old.unlink(missing_ok=True)
    return target


async def print_startup_status(
    selected: list[ChainCfg], pools: dict[str, RpcPool], db: DB
) -> None:
    header = [
        "",
        "=== DeFi scanner start ===",
        f"UTC        {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}",
        f"DB         {db.path}",
        "",
        f"{'сеть':<14} {'start':>12} {'last_indexed':>14} {'head':>12} {'осталось':>12} {'контрактов':>12}",
        "-" * 80,
    ]
    for line in header:
        console.info(line)
        log.info(line if line else " ")

    rows = []
    for c in selected:
        last = db.last_indexed(c.key)
        if last == 0:
            last = max(c.start_block - 1, 0)
        head = await safe_head(pools[c.key])
        if head is not None:
            head = max(0, head - c.confirmations)
        remain = (head - last) if head is not None else None
        counts = db.chain_counts(c.key)
        line = (
            f"{c.key:<14} {c.start_block:>12,} {last:>14,} "
            f"{(f'{head:,}' if head is not None else 'n/a'):>12} "
            f"{(f'{remain:,}' if remain is not None else 'n/a'):>12} "
            f"{counts['contracts']:>12,}"
        )
        console.info(line)
        log.info(line)
        rows.append((c.key, last, head, remain, counts))

    console.info("-" * 80)
    console.info("Дальше работа в фоне. Логи: logs/scanner.log   статус: logs/status.txt")
    console.info("Excel: reports/qualifying.xlsx, below_threshold.xlsx, incomplete.xlsx")
    console.info("Остановка: Ctrl+C  (или systemctl stop / kill)")
    console.info("")


async def heartbeat_loop(
    db: DB,
    cfg: AppCfg,
    selected: list[ChainCfg],
    pools: dict[str, RpcPool],
    stop: asyncio.Event,
    monitor: MonitorStore,
    run_id: str,
) -> None:
    status_path = cfg.log_dir / "status.txt"
    previous: dict[str, tuple[float, int]] = {}
    discovery_keys = {chain.key for chain in selected}
    last_prune_day = ""
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=cfg.heartbeat_sec)
            break
        except asyncio.TimeoutError:
            pass
        sampled_at = datetime.now(timezone.utc)
        paused = monitor.setting("scanner_paused", "0") == "1"
        monitor.heartbeat(run_id, "paused" if paused else "running")
        lines = [
            f"updated_utc {sampled_at.isoformat()}",
            f"{'chain':<14} {'last':>12} {'safe_head':>12} {'lag':>10} {'contracts':>10} {'rpc':<30} {'cooldown':>9} {'error'}",
        ]
        for key, pool in pools.items():
            c = cfg.chains[key]
            role = "discovery" if key in discovery_keys else "balance"
            last = db.last_indexed(c.key) if role == "discovery" else None
            head = max(0, pool.last_head - c.confirmations) if pool.last_head is not None else None
            lag = (head - last) if head is not None and last is not None else None
            cnt = db.chain_counts(c.key)
            cooldown = max(
                [max(0.0, endpoint.cooldown_until - time.time()) for endpoint in pool.endpoints]
                or [0.0]
            )
            errors = sorted({endpoint.last_error for endpoint in pool.endpoints if endpoint.last_error})
            old = previous.get(c.key)
            blocks_per_hour = None
            if old and last is not None and sampled_at.timestamp() > old[0]:
                blocks_per_hour = max(
                    0.0, (last - old[1]) * 3600 / (sampled_at.timestamp() - old[0])
                )
            if last is not None:
                previous[c.key] = (sampled_at.timestamp(), last)
            metrics = pool.take_metrics()
            monitor.add_chain_sample(
                run_id=run_id, chain=c.key, role=role, cursor=last,
                safe_head=head, lag=lag, blocks_per_hour=blocks_per_hour,
                contracts=cnt["contracts"],
                direct_deploy=db.discovery_count(c.key, "direct_deploy"),
                active_call=db.discovery_count(c.key, "active_call"),
                active_rpc=pool.active_endpoint(), cooldown_sec=cooldown,
                rpc_requests=metrics["requests"], rpc_successes=metrics["successes"],
                rpc_errors=metrics["errors"], latency_p50_ms=metrics["latency_p50_ms"],
                latency_p95_ms=metrics["latency_p95_ms"],
                errors_json=metrics["errors_by_type"],
            )
            line = (
                f"{c.key:<14} {(f'{last:,}' if last is not None else 'balance'):>12} "
                f"{(f'{head:,}' if head is not None else 'n/a'):>12} "
                f"{(f'{lag:,}' if lag is not None else 'n/a'):>10} "
                f"{cnt['contracts']:>10,} {pool.active_endpoint():<30} "
                f"{cooldown:>8.0f}s {','.join(errors) or '-'}"
            )
            lines.append(line)
            log.info("heartbeat %s", line)
        aggregate = db.aggregate_counts()
        lines.append(
            "address_scans "
            + " ".join(f"{status}={count}" for status, count in sorted(aggregate.items()))
        )
        write_status(status_path, lines)
        snapshot = db.monitoring_snapshot()
        reports: dict[str, dict[str, Any]] = {}
        export_times: list[str] = []
        for name in ("qualifying.xlsx", "below_threshold.xlsx", "incomplete.xlsx"):
            path = cfg.export_dir / name
            if path.exists():
                stat = path.stat()
                mtime = datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat()
                reports[name] = {"bytes": stat.st_size, "updated_at": mtime}
                export_times.append(mtime)
        wal = Path(str(db.path) + "-wal")
        monitor.add_aggregate_sample(
            run_id=run_id, **snapshot,
            db_bytes=db.path.stat().st_size if db.path.exists() else 0,
            wal_bytes=wal.stat().st_size if wal.exists() else 0,
            reports_json=reports,
            last_export_at=max(export_times) if export_times else None,
        )
        prune_day = sampled_at.strftime("%Y%m%d")
        if prune_day != last_prune_day:
            monitor.prune(30)
            last_prune_day = prune_day
        # короткий пинг в консоль, чтобы по ssh было видно что жив
        console.info("heartbeat  " + " | ".join(
            f"{c.key}:{db.last_indexed(c.key)}" for c in selected
        ))


async def export_loop(
    db: DB,
    cfg: AppCfg,
    selected: list[ChainCfg],
    stop: asyncio.Event,
    monitor: MonitorStore | None = None,
) -> None:
    mapping = {c.key: c for c in selected}
    last_snap_day = ""
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=cfg.export_every_sec)
            break
        except asyncio.TimeoutError:
            pass
        try:
            export_xlsx(db, cfg, mapping, cfg.min_usd, snapshot=False)
            day = datetime.now(timezone.utc).strftime("%Y%m%d")
            if cfg.daily_snapshot and day != last_snap_day:
                export_xlsx(db, cfg, mapping, cfg.min_usd, snapshot=True)
                last_snap_day = day
            if monitor:
                monitor.set_setting("consecutive_export_failures", "0")
                monitor.resolve_incident("export:failed")
        except Exception as e:
            log.warning("periodic export: %s", e)
            if monitor:
                failures = int(monitor.setting("consecutive_export_failures", "0") or 0) + 1
                monitor.set_setting("consecutive_export_failures", failures)
                if failures >= 2:
                    monitor.open_incident(
                        "export:failed", "critical", "export_failure",
                        f"XLSX export failed {failures} times: {type(e).__name__}",
                    )


async def control_loop(
    db: DB,
    cfg: AppCfg,
    chains: dict[str, ChainCfg],
    monitor: MonitorStore,
    stop: asyncio.Event,
) -> None:
    while not stop.is_set():
        for request in monitor.pending_controls():
            try:
                if request.action == "pause":
                    monitor.set_runtime_state("pausing", "finishing current block/address work")
                    monitor.set_setting("scanner_paused", "1")
                    result = "pause requested"
                elif request.action == "resume":
                    monitor.set_setting("scanner_paused", "0")
                    result = "scanner resumed"
                elif request.action == "export":
                    paths = await asyncio.to_thread(export_xlsx, db, cfg, chains, cfg.min_usd)
                    result = "exported: " + ", ".join(path.name for path in paths)
                elif request.action == "backup":
                    path = await asyncio.to_thread(backup_database, db, ROOT / "backups", 7)
                    result = f"backup: {path.name}"
                else:
                    raise ValueError("unsupported scanner control action")
                monitor.resolve_incident(f"control:{request.action}")
                monitor.finish_control(request.id, "complete", result)
            except Exception as exc:
                monitor.finish_control(request.id, "failed", f"{type(exc).__name__}: {exc}")
                monitor.open_incident(
                    f"control:{request.action}", "critical", "control_failure",
                    f"{request.action} failed: {type(exc).__name__}",
                )
        try:
            await asyncio.wait_for(stop.wait(), timeout=1)
        except asyncio.TimeoutError:
            pass


async def daily_backup_loop(
    db: DB, monitor: MonitorStore, stop: asyncio.Event,
) -> None:
    while not stop.is_set():
        last = float(monitor.setting("last_backup_at", "0") or 0)
        if time.time() - last >= 86400:
            try:
                path = await asyncio.to_thread(backup_database, db, ROOT / "backups", 7)
                monitor.set_setting("last_backup_at", time.time())
                monitor.set_setting("last_backup_file", path.name)
                monitor.resolve_incident("backup:failed")
            except Exception as exc:
                monitor.open_incident(
                    "backup:failed", "critical", "backup_failure",
                    f"Daily SQLite backup failed: {type(exc).__name__}",
                )
        try:
            await asyncio.wait_for(stop.wait(), timeout=3600)
        except asyncio.TimeoutError:
            pass


async def supervised(
    name: str, factory, stop: asyncio.Event, restart_sec: int,
    monitor: MonitorStore | None = None,
) -> None:
    while not stop.is_set():
        worker: asyncio.Task[Any] | None = None
        try:
            worker = asyncio.create_task(factory(), name=f"worker-{name}")
            done, _ = await asyncio.wait({worker}, timeout=10)
            if worker not in done and monitor:
                monitor.resolve_incident(f"worker:{name}")
            await worker
            if monitor:
                monitor.resolve_incident(f"worker:{name}")
            return
        except asyncio.CancelledError:
            if worker and not worker.done():
                worker.cancel()
                await asyncio.gather(worker, return_exceptions=True)
            raise
        except Exception:
            log.error("[%s] воркер упал, рестарт через %sс\n%s", name, restart_sec, traceback.format_exc())
            if monitor:
                monitor.open_incident(
                    f"worker:{name}", "critical", "worker_crash",
                    f"Worker {name} crashed and will restart",
                )
            try:
                await asyncio.wait_for(stop.wait(), timeout=restart_sec)
            except asyncio.TimeoutError:
                continue


async def run(args: argparse.Namespace) -> None:
    cfg = load_config(ROOT / "config.yaml")
    setup_logging(cfg.log_dir)
    monitor = MonitorStore(cfg.monitoring_db_path)
    profile = os.getenv("SCANNER_LOAD_PROFILE", "").strip().lower()
    if not profile:
        profile = (monitor.setting("load_profile", "normal") or "normal").lower()
    if profile not in LOAD_PROFILES:
        log.warning("unknown load profile %s; using normal", profile)
        profile = "normal"
    apply_load_profile(cfg, profile)
    if args.min_usd is not None:
        cfg.min_usd = args.min_usd
    if getattr(args, "rabby_fallback", None) is not None:
        cfg.rabby_fallback = args.rabby_fallback
    if getattr(args, "tx_to_contracts", None) is not None:
        cfg.discover_tx_to_contracts = args.tx_to_contracts

    wanted = (
        [item.strip() for item in args.chains.split(",") if item.strip()]
        if args.chains
        else cfg.default_chains
    )
    unknown = [key for key in wanted if key not in cfg.chains]
    if unknown:
        raise SystemExit(
            f"unknown network(s): {', '.join(unknown)}. Available: {', '.join(cfg.chains)}"
        )
    selected = [cfg.chains[key] for key in wanted]
    balance_chains = {key: chain for key, chain in cfg.chains.items() if chain.enabled}

    db = DB(cfg.db_path)
    seed_tokens(db, load_token_seed(ROOT / "tokens.yaml"), list(balance_chains))
    cfg.export_dir.mkdir(parents=True, exist_ok=True)

    if args.export_only:
        export_xlsx(db, cfg, balance_chains, cfg.min_usd)
        db.close()
        monitor.close()
        return

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            try:
                signal.signal(sig, lambda *_args: loop.call_soon_threadsafe(stop.set))
            except (ValueError, OSError):
                pass
    mode = "index-only" if args.index_only else "balances-only" if args.balances_only else "full"
    run_id = None if args.rpc_check else monitor.begin_run(
        mode, cfg.min_usd, profile, vars(args)
    )
    global_rpc_sem = asyncio.Semaphore(cfg.global_rpc_concurrency)
    prices = PriceBook(cfg.http_timeout_sec, cfg.price_batch_size)
    pools: dict[str, RpcPool] = {}
    should_export = not args.rpc_check
    try:
        needed = (
            balance_chains
            if not args.index_only or args.rpc_check
            else {chain.key: chain for chain in selected}
        )
        for chain in needed.values():
            if not chain.rpc:
                log.warning("[%s] no RPC configured", chain.key)
                continue
            pool = RpcPool(
                chain.key,
                chain.rpc,
                cfg,
                expected_chain_id=chain.chain_id,
                global_sem=global_rpc_sem,
            )
            await pool.__aenter__()
            pools[chain.key] = pool

        reports = await asyncio.gather(
            *(pool.preflight() for pool in pools.values()),
            return_exceptions=True,
        )
        for (key, _pool), report in zip(pools.items(), reports):
            if isinstance(report, Exception):
                log.warning("[%s] RPC preflight failed: %s", key, report)
                if args.rpc_check:
                    console.info(f"{key}: ERROR {type(report).__name__}")
                continue
            ok_count = sum(1 for row in report if row.get("ok"))
            log.info("[%s] RPC preflight %s/%s usable", key, ok_count, len(report))
            if args.rpc_check:
                heads = [row["head"] for row in report if row.get("ok") and row.get("head")]
                details = ", ".join(
                    f"{row['endpoint']}={'ok' if row.get('ok') else row.get('error', 'error')}"
                    for row in report
                )
                head_text = f" head={max(heads):,}" if heads else ""
                console.info(f"{key}: {ok_count}/{len(report)}{head_text}  {details}")

        if args.rpc_check:
            return

        discovery_ready = [
            chain for chain in selected
            if chain.key in pools and pools[chain.key].usable()
        ]
        if not discovery_ready and not args.balances_only:
            raise RuntimeError("none of the selected discovery networks has an RPC")
        await print_startup_status(discovery_ready, pools, db)

        if args.once:
            if cfg.run_indexer and not args.balances_only:
                await asyncio.gather(
                    *(
                        index_chain(
                            db,
                            chain,
                            pools[chain.key],
                            cfg,
                            stop,
                            args.from_block,
                            args.to_block,
                            once=True,
                            monitor=monitor,
                        )
                        for chain in discovery_ready
                    )
                )
            if cfg.run_balance_checker and not args.index_only:
                await check_multichain_balances(
                    db,
                    balance_chains,
                    pools,
                    prices,
                    cfg,
                    cfg.min_usd,
                    stop,
                    once=True,
                    monitor=monitor,
                )
            export_xlsx(db, cfg, balance_chains, cfg.min_usd)
            should_export = False
            return

        tasks: list[asyncio.Task[Any]] = []
        if cfg.run_indexer and not args.balances_only:
            for chain in discovery_ready:
                pool = pools[chain.key]
                tasks.append(
                    asyncio.create_task(
                        supervised(
                            f"idx-{chain.key}",
                            lambda c=chain, p=pool: index_chain(
                                db,
                                c,
                                p,
                                cfg,
                                stop,
                                args.from_block,
                                args.to_block,
                                once=False,
                                monitor=monitor,
                            ),
                            stop,
                            cfg.task_restart_sec,
                            monitor,
                        ),
                        name=f"idx-{chain.key}",
                    )
                )
        if cfg.run_balance_checker and not args.index_only:
            tasks.append(
                asyncio.create_task(
                    supervised(
                        "balances",
                        lambda: check_multichain_balances(
                            db,
                            balance_chains,
                            pools,
                            prices,
                            cfg,
                            cfg.min_usd,
                            stop,
                            monitor=monitor,
                        ),
                        stop,
                        cfg.task_restart_sec,
                        monitor,
                    ),
                    name="balances",
                )
            )
        tasks.append(
            asyncio.create_task(
                export_loop(db, cfg, list(balance_chains.values()), stop, monitor),
                name="xlsx",
            )
        )
        tasks.append(
            asyncio.create_task(
                heartbeat_loop(db, cfg, discovery_ready, pools, stop, monitor, run_id),
                name="heartbeat",
            )
        )
        tasks.append(
            asyncio.create_task(
                control_loop(db, cfg, balance_chains, monitor, stop),
                name="control",
            )
        )
        tasks.append(
            asyncio.create_task(daily_backup_loop(db, monitor, stop), name="daily-backup")
        )
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        if run_id is not None:
            monitor.heartbeat(run_id, "stopping")
        stop.set()
        if should_export:
            try:
                export_xlsx(db, cfg, balance_chains, cfg.min_usd)
            except Exception as exc:
                log.warning("final export: %s", exc)
        for pool in pools.values():
            await pool.__aexit__(None, None, None)
        db.close()
        if run_id is not None:
            monitor.finish_run(run_id)
        monitor.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="DeFi contract liquidity scanner")
    parser.add_argument("--chains", help="discovery networks, comma separated")
    parser.add_argument("--from-block", type=int, dest="from_block")
    parser.add_argument("--to-block", type=int, dest="to_block")
    parser.add_argument("--min-usd", type=float, dest="min_usd")
    parser.add_argument("--index-only", action="store_true")
    parser.add_argument("--balances-only", action="store_true")
    parser.add_argument("--export-only", action="store_true")
    parser.add_argument("--rpc-check", action="store_true", help="validate all configured RPCs and exit")
    parser.add_argument("--once", action="store_true", help="finish the current finite pass and exit")
    parser.add_argument(
        "--rabby-fallback", action=argparse.BooleanOptionalAction, default=None,
        help="supplement failed/slow RPC scans with a separately labelled Rabby estimate",
    )
    parser.add_argument(
        "--tx-to-contracts", action=argparse.BooleanOptionalAction, default=None,
        help="discover unique top-level tx.to recipients that currently have bytecode",
    )
    args = parser.parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
