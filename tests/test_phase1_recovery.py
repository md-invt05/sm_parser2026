"""Focused safety checks for tip gaps, RPC pressure and reserved capacity."""

import asyncio
from dataclasses import replace
from pathlib import Path

import httpx
import pytest

import scan_defi as scanner
import sui_support


ADDRESS = "0x" + "ab" * 20
TOKEN_A = "0x" + "cd" * 20
TOKEN_B = "0x" + "ef" * 20
ROOT = Path(__file__).resolve().parents[1]


def test_historical_token_debt_does_not_throttle_normal_live():
    governor = scanner.LoadGovernor(True, 6, 1)
    assert governor.evaluate(0, 0, 0, now=0, token_pending=66_000) == (6, 1)
    assert governor.evaluate(0, 0, 0, now=30, rpc_attempts=100,
                             rpc_errors=40) == (6, 1)
    assert governor.evaluate(0, 0, 0, now=91, rpc_attempts=100,
                             rpc_errors=40) == (3, 0)
    assert governor.evaluate(0, 0, 0, now=100, rpc_attempts=100,
                             rpc_errors=0, rpc_p95_ms=100) == (3, 0)
    assert governor.evaluate(0, 0, 0, now=699, rpc_attempts=100,
                             rpc_errors=0, rpc_p95_ms=100) == (3, 0)
    # RPC pressure clears at 700, but the drain state has its own hysteresis.
    assert governor.evaluate(0, 0, 0, now=700, rpc_attempts=100,
                             rpc_errors=0, rpc_p95_ms=100) == (3, 1)
    assert governor.evaluate(0, 0, 0, now=1301, rpc_attempts=100,
                             rpc_errors=0, rpc_p95_ms=100) == (6, 1)


def test_method_breaker_allows_one_half_open_probe_and_two_successes():
    async def scenario():
        entered = asyncio.Event()
        release = asyncio.Event()
        requests = 0

        async def handler(_request):
            nonlocal requests
            requests += 1
            entered.set()
            await release.wait()
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1,
                                             "result": "0x1"})

        cfg = scanner.load_config(ROOT / "config.yaml")
        pool = scanner.RpcPool("ethereum", ["https://example.invalid"], cfg,
                               transport=httpx.MockTransport(handler))
        async with pool:
            endpoint = pool.endpoints[0]
            health = endpoint.health("balance")
            health.fail_streak = 3
            health.circuit_until = 0
            health.cooldown_until = 0
            balance_context = scanner.BALANCE_RPC.set(True)
            try:
                first = asyncio.create_task(pool._request_endpoint(
                    endpoint, {"jsonrpc": "2.0", "id": 1,
                               "method": "eth_getBalance", "params": []}, "balance"))
                await asyncio.wait_for(entered.wait(), 2)
                with pytest.raises(scanner.RpcError, match="probe already in flight"):
                    await pool._request_endpoint(endpoint, {"id": 2}, "balance")
                assert requests == 1
                release.set()
                await first
                assert health.fail_streak == 3
                assert health.ok_streak == 1
                await pool._request_endpoint(endpoint, {"id": 3}, "balance")
                assert health.fail_streak == 0
                assert health.ok_streak == 2
                assert requests == 2
            finally:
                scanner.BALANCE_RPC.reset(balance_context)

    asyncio.run(scenario())


def test_auth_disable_persists_before_heartbeat(tmp_path):
    async def scenario():
        db = scanner.DB(tmp_path / "contracts.db")
        cfg = replace(scanner.load_config(ROOT / "config.yaml"), max_retries=1)
        transport = httpx.MockTransport(
            lambda _request: httpx.Response(401, text="unauthorized"))
        pool = scanner.RpcPool("ethereum", ["https://private.invalid/key"],
                               cfg, transport=transport, health_store=db)
        async with pool:
            with pytest.raises(scanner.RpcError):
                await pool.call("eth_blockNumber", [])
        saved = db.load_rpc_health("ethereum")
        assert any(row["permanent_error"] == "auth" for row in saved)
        reopened = scanner.RpcPool("ethereum", ["https://private.invalid/key"],
                                   cfg, transport=transport, health_store=db)
        assert reopened.endpoints[0].permanent_error == "auth"
        db.close()
    asyncio.run(scenario())


def test_cas_rejects_stale_discovery_without_writing_data(tmp_path):
    db = scanner.DB(tmp_path / "contracts.db")
    db.init_chain("ethereum", 100, True)
    db.init_chain_cursors("ethereum", 100, 120, lookback=1)
    contract = ("ethereum", ADDRESS, None, None, None, "2026-01-01")
    with pytest.raises(RuntimeError, match="cursor changed"):
        db.commit_index_range(
            "ethereum", "live", 120, [contract], [], [], [], [],
            expected_start=119,
        )
    assert db.conn.execute("SELECT COUNT(*) FROM contracts").fetchone()[0] == 0
    assert db.cursor("ethereum", "live")["next_block"] == 120
    db.close()


def test_catchup_updates_earliest_observation_and_token_start(tmp_path):
    db = scanner.DB(tmp_path / "contracts.db")
    db.init_chain("ethereum", 100, True)
    db.init_chain_cursors("ethereum", 100, 200, lookback=1)
    late = ("ethereum", ADDRESS, "active_call", 200, "0xlate", None, "2026-01-01")
    db.commit_index_range(
        "ethereum", "live", 200,
        [("ethereum", ADDRESS, None, None, None, "2026-01-01")],
        [late], [], [], [], enqueue_token_logs=True, expected_start=200,
    )
    early = ("ethereum", ADDRESS, "active_call", 100, "0xearly", None, "2026-01-01")
    db.commit_index_range(
        "ethereum", "backfill", 100, [], [early], [], [], [],
        enqueue_token_logs=True, expected_start=100,
    )
    assert db.conn.execute(
        "SELECT observed_block FROM contract_discoveries WHERE chain='ethereum' AND address=?",
        (ADDRESS,),
    ).fetchone()[0] == 100
    assert db.token_log_task("ethereum", ADDRESS)["first_block"] == 100
    db.close()


def test_reorg_alert_halts_all_chain_cursors_across_restart(tmp_path):
    db = scanner.DB(tmp_path / "contracts.db")
    db.init_chain("ethereum", 100, True)
    db.init_chain_cursors("ethereum", 100, 120, lookback=1)
    db.commit_index_range(
        "ethereum", "live", 120, [], [], [], [], [],
        expected_start=120, block_hashes=[(120, "0xabc")],
    )
    gap_id = db.reanchor_tip("ethereum", 200, 1000)
    assert gap_id is not None
    db.halt_discovery_on_reorg("ethereum", "live", None, 120)
    assert db.cursor("ethereum", "live")["status"] == "reorg_alert"
    assert db.cursor("ethereum", "backfill")["status"] == "reorg_alert"
    assert db.discovery_gap(gap_id)["status"] == "reorg_alert"
    assert db.discovery_gap_snapshot()["ethereum"]["reorg_alerts"] == 1
    assert db.reanchor_tip("ethereum", 220, 1000) is None
    db.init_chain_cursors("ethereum", 100, 220)
    assert db.cursor("ethereum", "live")["status"] == "reorg_alert"
    with pytest.raises(RuntimeError, match="missing"):
        db.commit_index_range("ethereum", "live", 200, [], [], [], [], [],
                              expected_start=199)
    db.close()


def test_rpc_limiter_borrows_idle_sui_capacity_but_keeps_hard_cap():
    async def scenario():
        limiter = scanner.RoleRpcLimiter(2, 2, 2)
        release = asyncio.Event()
        started = asyncio.Event()
        peak = 0

        async def discovery():
            nonlocal peak
            async with limiter.slot("discovery"):
                peak = max(peak, sum(limiter.active.values()))
                if sum(limiter.active.values()) == 6:
                    started.set()
                await release.wait()

        tasks = [asyncio.create_task(discovery()) for _ in range(6)]
        await asyncio.wait_for(started.wait(), 1)
        assert peak == limiter.total_limit == 6
        sui_started = asyncio.Event()

        async def sui():
            async with limiter.slot("sui"):
                sui_started.set()

        sui_task = asyncio.create_task(sui())
        await asyncio.sleep(.01)
        assert not sui_started.is_set()
        release.set()
        await asyncio.gather(*tasks, sui_task)
        assert sui_started.is_set()

    asyncio.run(scenario())


def test_catchup_history_slot_gives_original_backfill_one_in_five():
    async def scenario():
        slots = scanner.DiscoverySlots(live_slots=1, backfill_slots=1)
        release = asyncio.Event()
        entered = asyncio.Event()
        order = []

        async def holder():
            async with slots.slot("backfill", "holder"):
                entered.set()
                await release.wait()

        async def worker(role, name):
            async with slots.slot(role, name):
                order.append(name)

        held = asyncio.create_task(holder())
        await entered.wait()
        tasks = [asyncio.create_task(worker("catchup", f"gap-{i}")) for i in range(8)]
        tasks.append(asyncio.create_task(worker("backfill", "original")))
        for _ in range(100):
            if len(slots.queues["backfill"]) == 9:
                break
            await asyncio.sleep(.001)
        assert len(slots.queues["backfill"]) == 9
        release.set()
        await asyncio.gather(held, *tasks)
        assert order.index("original") < 5

    asyncio.run(scenario())


def test_sui_page_error_uses_in_worker_backoff(monkeypatch):
    async def scenario():
        stop = asyncio.Event()
        calls = 0

        async def broken(*_args, **_kwargs):
            nonlocal calls
            calls += 1
            stop.set()
            raise sui_support.BlockberryError("unavailable", "temporary", endpoint="/transactions")

        monkeypatch.setattr(sui_support, "discover_sui_once", broken)
        await sui_support.sui_discovery_loop(
            None, None, sui_support.SuiConfig(), stop,
        )
        assert calls == 1

    asyncio.run(scenario())


def test_token_retry_preserves_successes_and_schedules_reconcile(tmp_path):
    db = scanner.DB(tmp_path / "contracts.db")
    db.save_address_chain_state(ADDRESS, {
        "chain": "ethereum", "status": "partial", "has_code": 1,
        "native_raw": "0", "native_amount": 0, "included_native_usd": 0,
        "observed_native_usd": 0, "tokens_usd": 200, "total_usd": 200,
        "checked_tokens": ["native", TOKEN_A], "failed_tokens": [TOKEN_B],
    }, [
        {"chain": "ethereum", "token": "native", "raw_amount": 0,
         "usd_value": 0, "priced": True},
        {"chain": "ethereum", "token": TOKEN_A, "raw_amount": 2,
         "amount": 2, "price_usd": 100, "usd_value": 200, "priced": True},
    ], 500_000)
    due = db.due_token_balance_work(ADDRESS, "ethereum", as_of=10**10)
    assert [row["asset"] for row in due] == [TOKEN_B]
    assert db.conn.execute(
        "SELECT usd_value FROM address_token_state WHERE address=? AND token=?",
        (ADDRESS, TOKEN_A),
    ).fetchone()[0] == 200
    result = db.apply_token_balance_work(ADDRESS, "ethereum", [{
        "token": TOKEN_B, "raw_amount": 3, "amount": 3,
        "symbol": "TEST", "price_usd": 100, "usd_value": 300,
        "priced": True, "valuation_status": "included",
    }], [])
    assert result["status"] == "partial"  # Different snapshot blocks cannot become below.
    assert result["total_usd"] == 500
    assert db.conn.execute(
        "SELECT COUNT(*) FROM balance_work_items WHERE kind='token_balance'",
    ).fetchone()[0] == 0
    assert db.conn.execute(
        "SELECT COUNT(*) FROM balance_work_items WHERE kind='chain_reconcile'",
    ).fetchone()[0] == 1
    db.close()


def test_failed_token_does_not_keep_stale_usd_in_confirmed_sum(tmp_path):
    db = scanner.DB(tmp_path / "contracts.db")
    base = {
        "chain": "ethereum", "has_code": 1, "native_raw": "0",
        "native_amount": 0, "included_native_usd": 0, "observed_native_usd": 0,
    }
    old_token = {"token": TOKEN_B, "raw_amount": 4, "amount": 4,
                 "price_usd": 100, "usd_value": 400, "priced": True}
    db.save_address_chain_state(ADDRESS, {
        **base, "status": "complete", "tokens_usd": 400, "total_usd": 400,
    }, [old_token], 500_000)
    db.save_address_chain_state(ADDRESS, {
        **base, "status": "partial", "tokens_usd": 0, "total_usd": 0,
        "checked_tokens": ["native"], "failed_tokens": [TOKEN_B],
    }, [], 500_000)
    state = db.conn.execute(
        "SELECT total_usd FROM address_chain_state WHERE address=? AND chain='ethereum'",
        (ADDRESS,),
    ).fetchone()
    token = db.conn.execute(
        "SELECT raw_amount,usd_value,valuation_status FROM address_token_state WHERE address=? AND token=?",
        (ADDRESS, TOKEN_B),
    ).fetchone()
    assert state["total_usd"] == 0
    assert (token["raw_amount"], token["usd_value"], token["valuation_status"]) == (
        "4", 400, "stale",
    )
    db.close()


def test_token_retry_calls_only_failed_asset_not_code_or_native(tmp_path):
    class Rpc:
        call_batch = 10

        def __init__(self):
            self.methods = []
            self.assets = []

        async def call(self, method, _params):
            self.methods.append(method)
            assert method == "eth_blockNumber"
            return hex(1000)

        async def batch_partial(self, calls):
            self.assets.extend(item[1][0]["to"] for item in calls)
            return ["0x" + format(3, "064x") for _ in calls]

    class Prices:
        async def fetch(self, keys):
            return {key: 100 for key in keys}

    cfg = scanner.load_config(ROOT / "config.yaml")
    chain = cfg.chains["ethereum"]
    db = scanner.DB(tmp_path / "contracts.db")
    db.conn.execute(
        "INSERT INTO tokens(chain,address,symbol,decimals,source) VALUES(?,?,?,?,?)",
        ("ethereum", TOKEN_B, "TEST", 0, "test"),
    )
    db.conn.execute(
        "INSERT INTO contract_tokens(chain,contract,token,source,first_seen_block,last_seen_block) "
        "VALUES(?,?,?,?,?,?)",
        ("ethereum", ADDRESS, TOKEN_B, "test", 1, 1),
    )
    db.save_address_chain_state(ADDRESS, {
        "chain": "ethereum", "status": "partial", "has_code": 1,
        "native_raw": "0", "native_amount": 0, "included_native_usd": 0,
        "observed_native_usd": 0, "tokens_usd": 0, "total_usd": 0,
        "checked_tokens": ["native"], "failed_tokens": [TOKEN_B],
    }, [], cfg.min_usd)
    tasks = db.due_token_balance_work(ADDRESS, "ethereum", as_of=10**10)
    rpc = Rpc()
    result, _ = asyncio.run(scanner.retry_failed_token_balances(
        db, chain, rpc, Prices(), cfg, ADDRESS, tasks, asyncio.Semaphore(1),
    ))
    assert rpc.methods == ["eth_blockNumber"]
    assert rpc.assets == [TOKEN_B]
    assert result["total_usd"] == 300
    assert result["status"] == "partial"
    db.close()


def test_code_and_native_retry_do_not_repeat_completed_operations(tmp_path):
    class Rpc:
        def __init__(self):
            self.methods = []

        async def call(self, method, _params):
            self.methods.append(method)
            return "0x6000" if method == "eth_getCode" else hex(10**18)

    class Prices:
        async def fetch(self, keys):
            return {key: 2000 for key in keys}

    cfg = scanner.load_config(ROOT / "config.yaml")
    chain = cfg.chains["ethereum"]
    db = scanner.DB(tmp_path / "contracts.db")
    db.save_address_chain_state(ADDRESS, {
        "chain": "ethereum", "status": "rpc_error", "has_code": None,
        "total_usd": None, "note": "timeout",
    }, [], cfg.min_usd)
    rpc = Rpc()
    db.conn.execute("UPDATE balance_work_items SET due_at='2000-01-01T00:00:00+00:00'")
    code = db.due_balance_work(ADDRESS, "ethereum")[0]
    asyncio.run(scanner.retry_simple_balance_work(
        db, chain, rpc, Prices(), cfg, ADDRESS, code, asyncio.Semaphore(1),
    ))
    native = db.due_balance_work(ADDRESS, "ethereum")[0]
    assert native["kind"] == "native_balance"
    asyncio.run(scanner.retry_simple_balance_work(
        db, chain, rpc, Prices(), cfg, ADDRESS, native, asyncio.Semaphore(1),
    ))
    state = db.conn.execute(
        "SELECT status,total_usd FROM address_chain_state WHERE address=? AND chain='ethereum'",
        (ADDRESS,),
    ).fetchone()
    assert rpc.methods == ["eth_getCode", "eth_getBalance"]
    assert state["status"] == "complete" and state["total_usd"] == 2000
    assert db.conn.execute("SELECT COUNT(*) FROM balance_work_items").fetchone()[0] == 0
    db.close()


def test_balance_operation_lease_survives_restart_and_expires(tmp_path):
    import time

    path = tmp_path / "contracts.db"
    db = scanner.DB(path)
    db.save_address_chain_state(ADDRESS, {
        "chain": "ethereum", "status": "rpc_error", "has_code": None,
        "total_usd": None,
    }, [], 500_000)
    db.conn.execute("UPDATE balance_work_items SET due_at='2000-01-01T00:00:00+00:00'")
    assert db.lease_balance_work(ADDRESS, "ethereum", "code", seconds=2)
    assert db.due_balance_work(ADDRESS, "ethereum") == []
    db.close()
    reopened = scanner.DB(path)
    assert reopened.due_balance_work(ADDRESS, "ethereum") == []
    assert reopened.due_balance_work(ADDRESS, "ethereum", as_of=time.time() + 3)[0]["kind"] == "code"
    reopened.fail_balance_work(ADDRESS, "ethereum", "code", "", "local_slot_wait",
                               local_wait=True)
    assert reopened.conn.execute(
        "SELECT failure_streak FROM balance_work_items WHERE kind='code'"
    ).fetchone()[0] == 0
    reopened.close()


def test_metadata_retry_uses_saved_raw_balance_not_balanceof(tmp_path):
    class Rpc:
        def __init__(self):
            self.methods = []

        async def call(self, method, _params):
            self.methods.append(method)
            assert method == "eth_call"
            return "0x" + format(6, "064x")

    class Prices:
        async def fetch(self, keys):
            return {key: 1 for key in keys}

    cfg = scanner.load_config(ROOT / "config.yaml")
    chain = cfg.chains["ethereum"]
    db = scanner.DB(tmp_path / "contracts.db")
    db.conn.execute(
        "INSERT INTO tokens(chain,address,symbol,decimals,source) VALUES(?,?,?,?,?)",
        ("ethereum", TOKEN_B, "USDT", None, "test"),
    )
    db.conn.execute(
        "INSERT INTO contract_tokens(chain,contract,token,source,first_seen_block,last_seen_block) "
        "VALUES(?,?,?,?,?,?)", ("ethereum", ADDRESS, TOKEN_B, "test", 1, 1),
    )
    db.save_address_chain_state(ADDRESS, {
        "chain": "ethereum", "status": "price_missing", "has_code": 1,
        "native_raw": "0", "native_amount": 0, "included_native_usd": 0,
        "observed_native_usd": 0, "tokens_usd": 0, "total_usd": 0,
    }, [{"token": TOKEN_B, "raw_amount": 2_000_000,
         "amount": None, "price_usd": None, "usd_value": None, "priced": False}],
        cfg.min_usd)
    db.conn.execute("UPDATE balance_work_items SET due_at='2000-01-01T00:00:00+00:00'")
    task = db.due_balance_work(ADDRESS, "ethereum")[0]
    assert task["kind"] == "token_metadata"
    rpc = Rpc()
    asyncio.run(scanner.retry_simple_balance_work(
        db, chain, rpc, Prices(), cfg, ADDRESS, task, asyncio.Semaphore(1),
    ))
    token = db.conn.execute(
        "SELECT amount,usd_value FROM address_token_state WHERE address=? AND token=?",
        (ADDRESS, TOKEN_B),
    ).fetchone()
    assert rpc.methods == ["eth_call"]
    assert token["amount"] == 2 and token["usd_value"] == 2
    assert db.conn.execute(
        "SELECT COUNT(*) FROM balance_work_items WHERE kind='token_metadata'"
    ).fetchone()[0] == 0
    db.close()


def test_large_token_holder_is_sliced_without_rpc_failure_streak(tmp_path):
    class Rpc:
        call_batch = 20

        async def call(self, method, _params):
            if method == "eth_getCode":
                return "0x6000"
            if method == "eth_getBalance":
                return "0x0"
            raise AssertionError(method)

        async def batch_partial(self, calls):
            assert len(calls) <= 20
            return ["0x" + format(1, "064x") for _ in calls]

    class Prices:
        async def fetch(self, keys):
            return {key: 1 for key in keys}

    cfg = scanner.load_config(ROOT / "config.yaml")
    chain = cfg.chains["ethereum"]
    db = scanner.DB(tmp_path / "contracts.db")
    for number in range(1, 26):
        asset = f"0x{number:040x}"
        db.conn.execute(
            "INSERT INTO tokens(chain,address,symbol,decimals,source) VALUES(?,?,?,?,?)",
            ("ethereum", asset, "T", 0, "test"),
        )
        db.conn.execute(
            "INSERT INTO contract_tokens(chain,contract,token,source,first_seen_block,last_seen_block) "
            "VALUES(?,?,?,?,?,?)",
            ("ethereum", ADDRESS, asset, "test", 1, 1),
        )
    result, tokens = asyncio.run(scanner._scan_address_chain(
        db, chain, Rpc(), Prices(), cfg, ADDRESS, 120,
    ))
    assert result["status"] == "partial"
    assert len(result["checked_tokens"]) == 21  # native + first 20 assets
    assert len(result["deferred_tokens"]) == 5
    db.save_address_chain_state(ADDRESS, result, tokens, cfg.min_usd)
    state = db.conn.execute(
        "SELECT failure_streak,total_usd FROM address_chain_state WHERE address=? AND chain='ethereum'",
        (ADDRESS,),
    ).fetchone()
    assert (state["failure_streak"], state["total_usd"]) == (0, 20)
    assert len(db.due_token_balance_work(ADDRESS, "ethereum", as_of=10**10)) == 5
    db.close()


def test_legacy_token_only_partial_lazily_migrates_once(tmp_path):
    db = scanner.DB(tmp_path / "contracts.db")
    for asset in (TOKEN_A, TOKEN_B):
        db.conn.execute(
            "INSERT INTO tokens(chain,address,symbol,decimals,source) VALUES(?,?,?,?,?)",
            ("ethereum", asset, "T", 0, "test"),
        )
        db.conn.execute(
            "INSERT INTO contract_tokens(chain,contract,token,source,first_seen_block,last_seen_block) "
            "VALUES(?,?,?,?,?,?)",
            ("ethereum", ADDRESS, asset, "test", 1, 1),
        )
    db.save_address_chain_state(ADDRESS, {
        "chain": "ethereum", "status": "partial", "has_code": 1,
        "native_raw": "0", "native_amount": 0, "included_native_usd": 0,
        "observed_native_usd": 0, "tokens_usd": 100, "total_usd": 100,
        "checked_tokens": ["native", TOKEN_A],
    }, [{"token": TOKEN_A, "raw_amount": 1, "amount": 1,
         "price_usd": 100, "usd_value": 100, "priced": True}], 500_000)
    first = db.due_token_balance_work(ADDRESS, "ethereum")
    second = db.due_token_balance_work(ADDRESS, "ethereum")
    assert [row["asset"] for row in first] == [TOKEN_B]
    assert [row["asset"] for row in second] == [TOKEN_B]
    assert db.conn.execute("SELECT COUNT(*) FROM balance_work_items").fetchone()[0] == 1
    db.close()
