"""Regression cases prepared for the post-model-switch test phase."""

import asyncio
import sqlite3
from pathlib import Path

import scan_defi as scanner
import yaml
from sui_support import SuiConfig, SuiStore


ROOT = Path(__file__).resolve().parents[1]
ADDRESS = "0x" + "ab" * 20
TOKEN = "0x" + "cd" * 20


def test_config_rejects_testnet_chain_id(tmp_path):
    raw = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
    raw["chains"]["bsc"]["chain_id"] = 97
    config = tmp_path / "testnet.yaml"
    config.write_text(yaml.safe_dump(raw), encoding="utf-8")
    try:
        scanner.load_config(config)
    except ValueError as exc:
        assert "non-mainnet" in str(exc)
    else:
        raise AssertionError("testnet chain ID was accepted")


class HugeTokenRpc:
    call_batch = 4

    async def call(self, method, params):
        if method == "eth_getCode":
            return "0x6000"
        if method == "eth_getBalance":
            return "0x0"
        if method == "eth_call":
            selector = params[0]["data"]
            if selector == scanner.DECIMALS_SEL:
                return "0x" + format(18, "064x")
            if selector == scanner.SYMBOL_SEL:
                return "0x" + b"vUSDT".ljust(32, b"\0").hex()
            return "0x" + format(10**30, "064x")
        raise AssertionError(method)

    async def batch_partial(self, calls):
        return [await self.call(method, params) for method, params in calls]


class Prices:
    async def fetch(self, keys, ttl=120):
        return {key: 10**140 for key in keys if not key.startswith("coingecko:")}


def test_huge_bsc_erc20_is_quarantined_without_discarding_raw(tmp_path):
    async def run():
        db = scanner.DB(tmp_path / "balances.db")
        db.upsert_tokens([("bsc", TOKEN, "vUSDT", 18, "seed")])
        cfg = scanner.load_config(ROOT / "config.yaml")
        for address in (
            "0x9400f8ad57e9e0f352345935d6d3175975eb1d9f",
            "0x87ec973455d72fbc0e6af970bec3b1631628474d",
        ):
            row, tokens = await scanner.scan_address_chain(
                db, cfg.chains["bsc"], HugeTokenRpc(), Prices(), cfg,
                address, asyncio.Semaphore(1), 2,
            )
            assert row["status"] == "anomalous_balance"
            assert row["total_usd"] == 0
            assert row["excluded_usd"] >= 10**100
            assert tokens[1]["raw_amount"] == 10**30
            assert tokens[1]["valuation_status"] == "anomalous_balance"
            assert scanner.classify_address_scan(0, [row], 500_000)[0] == "incomplete"
        assert db.conn.execute("SELECT COUNT(*) FROM anomalous_balances").fetchone()[0] == 2
        db.close()

    asyncio.run(run())


def test_malformed_erc20_uint256_is_not_accepted_as_balance():
    assert scanner.decode_abi_uint256("0x" + format(7, "064x")) == 7
    for malformed in ("0x7", "0x" + "f" * 128, "0x" + "g" * 64):
        try:
            scanner.decode_abi_uint256(malformed)
        except ValueError:
            pass
        else:
            raise AssertionError("malformed ERC-20 result was accepted")


def test_reviewed_token_policy_can_explicitly_include_large_asset(tmp_path):
    async def run():
        db = scanner.DB(tmp_path / "reviewed.db")
        db.upsert_tokens([("bsc", TOKEN, "vUSDT", 18, "seed")])
        db.seed_valuation_policies([{
            "chain": "bsc", "address": ADDRESS, "asset": TOKEN,
            "policy": "include_verified", "reason": "independently verified asset",
        }])
        cfg = scanner.load_config(ROOT / "config.yaml")
        row, tokens = await scanner.scan_address_chain(
            db, cfg.chains["bsc"], HugeTokenRpc(), Prices(), cfg,
            ADDRESS, asyncio.Semaphore(1), 2,
        )
        assert row["status"] == "complete"
        assert row["total_usd"] >= 10**100
        assert tokens[1]["valuation_status"] == "included"
        db.close()

    asyncio.run(run())


def test_historical_token_revaluation_is_idempotent(tmp_path):
    db = scanner.DB(tmp_path / "old.db")
    huge = 10**150
    with db.conn:
        db.conn.execute(
            "INSERT INTO contracts(chain,address,first_seen_at) VALUES(?,?,?)",
            ("bsc", ADDRESS, "2026-09-01T00:00:00+00:00"),
        )
    scan_id = db.save_address_scan(
        ADDRESS, "qualifying", huge, 1, 20,
        [{"chain": "bsc", "has_code": 1, "status": "complete",
          "native_amount": 0, "native_usd": 0, "included_native_usd": 0,
          "tokens_usd": huge, "total_usd": huge}],
        [{"chain": "bsc", "token": TOKEN, "symbol": "vUSDT",
          "raw_amount": 10**30, "amount": 10**12, "price_usd": 10**138,
          "usd_value": huge, "priced": True}],
    )
    assert db.revalue_token_balances(100_000_000, 500_000) == 1
    assert db.revalue_token_balances(100_000_000, 500_000) == 0
    row = db.conn.execute("SELECT * FROM address_scans WHERE id=?", (scan_id,)).fetchone()
    token = db.conn.execute(
        "SELECT * FROM address_token_scans WHERE scan_id=?", (scan_id,),
    ).fetchone()
    assert row["status"] == "incomplete" and row["total_usd"] == 0
    assert token["raw_amount"] == str(10**30)
    assert token["valuation_status"] == "anomalous_balance"
    contract = db.conn.execute(
        "SELECT last_status,last_total_usd FROM contracts WHERE chain='bsc' AND address=?",
        (ADDRESS,),
    ).fetchone()
    assert contract["last_status"] == "incomplete" and contract["last_total_usd"] == 0
    db.close()


def test_balance_oldest_includes_due_rechecks(tmp_path):
    db = scanner.DB(tmp_path / "queue.db")
    with db.conn:
        db.conn.execute(
            "INSERT INTO contracts(chain,address,first_seen_at) VALUES(?,?,?)",
            ("bsc", ADDRESS, "2026-09-01T00:00:00+00:00"),
        )
        db.conn.execute(
            """INSERT INTO address_chain_state(
                 address,chain,checked_at,status,next_retry_at)
               VALUES(?,?,?,?,?)""",
            (ADDRESS, "bsc", "2026-09-20T00:00:00+00:00", "rpc_error",
             "2026-09-21T00:00:00+00:00"),
        )
    snapshot = db.monitoring_snapshot(500_000)
    assert snapshot["balance_pending"] == 1
    assert snapshot["balance_oldest_age_sec"] > 3600
    db.close()


def test_governor_balance_only_and_zero_slots():
    async def run():
        governor = scanner.LoadGovernor(True, 4, 1)
        assert governor.evaluate(25_000, 20, 0, now=0, sui_pending=100) == (0, 0)
        slots = scanner.DiscoverySlots(1, 1)
        await slots.set_capacity(0, 0)
        entered = asyncio.Event()

        async def worker():
            async with slots.slot("live", "bsc"):
                entered.set()

        task = asyncio.create_task(worker())
        await asyncio.sleep(0)
        assert not entered.is_set()
        await slots.set_capacity(1, 0)
        await asyncio.wait_for(task, 1)
        assert entered.is_set()
        assert governor.evaluate(1_000, 100, 0, now=1) == (0, 0)
        assert governor.evaluate(1_000, 100, 0, now=602) == (3, 0)

    asyncio.run(run())


def test_sui_outage_preserves_snapshot_and_huge_tvl_is_incomplete(tmp_path):
    store = SuiStore(tmp_path / "sui.db")
    rows = [
        {"id": "small", "projectName": "Small", "currTvl": 1_000_000,
         "packages": [{"packageId": "0x2"}]},
        {"id": "huge", "projectName": "Huge", "currTvl": 900_000_000_000_000,
         "packages": [{"packageId": "0x3"}]},
    ]
    assert store.sync_defi_projects(rows, 500_000, 10_000_000_000) == 2
    before = {row["project_key"]: dict(row) for row in store.latest_projects()}
    store.mark_defi_sync_failed("HTTP 503 at /dex")
    after = {row["project_key"]: dict(row) for row in store.latest_projects()}
    assert before == after
    assert after["small"]["status"] == "qualifying"
    assert after["huge"]["status"] == "incomplete"
    assert after["huge"]["raw_tvl"] == "900000000000000"
    assert store.revalue_suspicious_projects(10_000_000_000) == 0
    store.close()


def test_sui_enrichment_selects_oldest_due_first(tmp_path):
    store = SuiStore(tmp_path / "sui.db")
    store.add_seen("new", 200, None)
    store.add_seen("old", 100, None)
    assert store.pending_enrichment(1)[0]["digest"] == "old"
    store.defer_enrichment("old", "temporary")
    assert store.pending_enrichment(1)[0]["digest"] == "new"
    store.close()


def test_sui_schema_migration_preserves_old_rows(tmp_path):
    path = tmp_path / "legacy.db"
    with sqlite3.connect(path) as conn:
        conn.executescript("""
            CREATE TABLE sui_seen_transactions(
              digest TEXT PRIMARY KEY,checkpoint INTEGER NOT NULL,timestamp_ms INTEGER,
              enriched INTEGER NOT NULL DEFAULT 0,seen_at TEXT NOT NULL);
            CREATE TABLE sui_defi_projects(
              project_key TEXT PRIMARY KEY,project_name TEXT NOT NULL,indexed_tvl REAL,
              status TEXT NOT NULL,packages_json TEXT NOT NULL,synced_at TEXT NOT NULL,
              provider_complete INTEGER NOT NULL DEFAULT 1,note TEXT);
            INSERT INTO sui_seen_transactions VALUES('old',12,NULL,0,'2026-09-01');
            INSERT INTO sui_defi_projects VALUES(
              'legacy','Legacy',900000000000000,'qualifying','["0x2"]',
              '2026-09-01',1,NULL);
        """)
    first = SuiStore(path)
    assert first.pending_enrichment(1)[0]["digest"] == "old"
    assert first.revalue_suspicious_projects(10_000_000_000) == 1
    first.close()
    second = SuiStore(path)
    assert second.revalue_suspicious_projects(10_000_000_000) == 0
    row = second.conn.execute(
        "SELECT * FROM sui_defi_projects WHERE project_key='legacy'"
    ).fetchone()
    assert row["status"] == "incomplete"
    assert row["indexed_tvl"] == 900000000000000
    assert row["raw_tvl"] is not None
    second.close()


def test_log_topic_batch_shrinks_without_advancing_range():
    class LogsRpc:
        log_range_limit = 1
        log_holder_batch = 2
        log_success_streak = 0
        log_failure_streak = 0

        def __init__(self):
            self.ranges = []

        async def call(self, method, params):
            assert method == "eth_getLogs"
            query = params[0]
            self.ranges.append((query["fromBlock"], query["toBlock"]))
            if isinstance(query["topics"][2], list):
                raise scanner.RpcError("range", "too many topics")
            return []

        def force_failover(self, group, seconds):
            raise AssertionError("shrinking topics should suffice")

    async def run():
        rpc = LogsRpc()
        chain = scanner.load_config(ROOT / "config.yaml").chains["bsc"]
        rows = await scanner.collect_transfers(
            chain, rpc, 123, 123, {ADDRESS, "0x" + "ef" * 20},
        )
        assert rows == ([], [])
        assert rpc.log_holder_batch == 1
        assert rpc.ranges == [("0x7b", "0x7b")] * 3

    asyncio.run(run())
