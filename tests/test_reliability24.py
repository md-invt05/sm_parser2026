import asyncio
from dataclasses import replace
from pathlib import Path

import scan_defi as scanner


ROOT = Path(__file__).resolve().parents[1]
ADDRESS = "0x" + "ab" * 20


class BalanceRpc:
    call_batch = 10

    def __init__(self, native_raw: int):
        self.native_raw = native_raw
        self.confirmations = 0

    async def call(self, method, params):
        if method == "eth_getCode":
            return "0x6000"
        if method == "eth_getBalance" and params[1] == "latest":
            return hex(self.native_raw)
        if method == "eth_getBalance" and params[1] == "0x0":
            return hex(self.native_raw)
        raise AssertionError((method, params))

    async def confirmed_call(self, method, params, expected):
        self.confirmations += 1
        return True, expected

    async def batch_partial(self, calls):
        return []


class Prices:
    def __init__(self, value):
        self.value = value

    async def fetch(self, keys, ttl=120):
        return {keys[0]: self.value}


async def _test_large_native_balance_is_quarantined_but_raw_value_is_kept(tmp_path):
    db = scanner.DB(tmp_path / "db.sqlite")
    cfg = replace(scanner.load_config(ROOT / "config.yaml"), native_anomaly_usd=100_000_000)
    chain = cfg.chains["ethereum"]
    rpc = BalanceRpc(200_000_000 * 10**18)
    row, tokens = await scanner.scan_address_chain(
        db, chain, rpc, Prices(2_500), cfg, ADDRESS, asyncio.Semaphore(1), 2,
    )
    assert row["status"] == "anomalous_balance"
    assert row["total_usd"] == 0
    assert row["observed_native_usd"] == 500_000_000_000
    assert row["excluded_usd"] == 500_000_000_000
    assert tokens[0]["raw_amount"] == 200_000_000 * 10**18
    assert tokens[0]["valuation_status"] == "anomalous_balance"
    assert rpc.confirmations == 1
    assert db.conn.execute("SELECT COUNT(*) FROM anomalous_balances").fetchone()[0] == 1
    db.close()


async def _test_known_zkevm_system_reserve_is_excluded_without_becoming_incomplete(tmp_path):
    db = scanner.DB(tmp_path / "db.sqlite")
    cfg = scanner.load_config(ROOT / "config.yaml")
    db.seed_valuation_policies(cfg.valuation_policies)
    address = "0x2a3dd3eb832af982ec71669e178424b10dca2ede"
    rpc = BalanceRpc(200_000_000 * 10**18)
    row, _tokens = await scanner.scan_address_chain(
        db, cfg.chains["zk"], rpc, Prices(2_500), cfg,
        address, asyncio.Semaphore(1), 2,
    )
    assert row["status"] == "complete"
    assert row["total_usd"] == 0
    assert row["valuation_status"] == "exclude_from_total"
    assert row["excluded_usd"] == 500_000_000_000
    assert rpc.confirmations == 0
    db.close()


def test_large_native_balance_is_quarantined_but_raw_value_is_kept(tmp_path):
    asyncio.run(_test_large_native_balance_is_quarantined_but_raw_value_is_kept(tmp_path))


def test_known_zkevm_system_reserve_is_excluded_without_becoming_incomplete(tmp_path):
    asyncio.run(_test_known_zkevm_system_reserve_is_excluded_without_becoming_incomplete(tmp_path))


def test_only_failed_address_chain_becomes_due(tmp_path):
    db = scanner.DB(tmp_path / "db.sqlite")
    db.upsert_contracts([("ethereum", ADDRESS, 1, "0xtx", None, "2026-01-01")])
    db.save_address_chain_state(
        ADDRESS,
        {"chain": "ethereum", "status": "complete", "has_code": 1,
         "total_usd": 300_000, "tokens_usd": 300_000},
        [], 500_000,
    )
    db.save_address_chain_state(
        ADDRESS,
        {"chain": "optimism", "status": "rpc_error", "has_code": None,
         "total_usd": None, "note": "timeout"},
        [], 500_000,
    )
    future = __import__("time").time() + 601
    assert db.due_address_chains(ADDRESS, ["ethereum", "optimism"], future) == ["optimism"]
    parts, _ = db.current_address_parts(ADDRESS, ["ethereum", "optimism"])
    assert sum(float(row.get("total_usd") or 0) for row in parts) == 300_000
    db.close()


def test_dual_cursor_commit_is_atomic_and_idempotent(tmp_path):
    db = scanner.DB(tmp_path / "db.sqlite")
    db.init_chain("ethereum", 100, True)
    db.init_chain_cursors("ethereum", 100, 120)
    contract = ("ethereum", ADDRESS, None, None, None, "2026-01-01")
    source = ("ethereum", ADDRESS, "active_call", 100, "0xtx", None, "2026-01-01")
    assert db.commit_index_range("ethereum", "backfill", 103, [contract], [source], [], [], []) == (1, 1)
    assert db.cursor("ethereum", "backfill")["next_block"] == 104
    assert db.commit_index_range("ethereum", "live", 120, [contract], [source], [], [], []) == (0, 0)
    assert db.conn.execute("SELECT COUNT(*) FROM contracts").fetchone()[0] == 1
    assert db.conn.execute("SELECT COUNT(*) FROM contract_discoveries").fetchone()[0] == 1
    db.close()


def test_method_cooldown_does_not_disable_other_rpc_methods():
    endpoint = scanner.Endpoint("https://example.invalid")
    endpoint.health("logs").last_error = "range"
    endpoint.cool(60, 900, "logs")
    assert not endpoint.available("logs")
    assert endpoint.available("code")


def test_historical_system_balance_is_revalued_once(tmp_path):
    db = scanner.DB(tmp_path / "db.sqlite")
    cfg = scanner.load_config(ROOT / "config.yaml")
    address = "0x2a3dd3eb832af982ec71669e178424b10dca2ede"
    db.upsert_contracts([("zk", address, None, None, None, "2026-01-01")])
    scan_id = db.save_address_scan(
        address, "qualifying", 479_000_000_010, 1, 1,
        [{"chain": "zk", "has_code": 1, "status": "complete",
          "native_amount": 200_000_000, "native_usd": 479_000_000_000,
          "tokens_usd": 10, "total_usd": 479_000_000_010}],
        [{"chain": "zk", "token": "native", "symbol": "ETH",
          "raw_amount": str(200_000_000 * 10**18), "amount": 200_000_000,
          "price_usd": 2395, "usd_value": 479_000_000_000, "priced": True}],
    )
    db.seed_valuation_policies(cfg.valuation_policies)
    assert db.revalue_system_balances(500_000) == 1
    assert db.revalue_system_balances(500_000) == 0
    row = db.conn.execute("SELECT * FROM address_scans WHERE id=?", (scan_id,)).fetchone()
    assert row["total_usd"] == 10
    part = db.conn.execute(
        "SELECT * FROM address_chain_scans WHERE scan_id=?", (scan_id,)
    ).fetchone()
    assert part["excluded_usd"] == 479_000_000_000
    assert part["observed_native_usd"] == 479_000_000_000
    db.close()
