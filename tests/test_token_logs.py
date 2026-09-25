import asyncio
from dataclasses import replace
from pathlib import Path

import httpx
import pytest

import scan_defi as scanner


ROOT = Path(__file__).resolve().parents[1]
ADDRESS = "0x" + "12" * 20
TOKEN = "0x" + "34" * 20


def config():
    return scanner.load_config(ROOT / "config.yaml")


def queue_task(db, address=ADDRESS, first=100, limited=0):
    with db.conn:
        db.conn.execute(
            """INSERT INTO token_log_tasks(
                 chain,address,first_block,next_block,history_limited,priority,due_at,updated_at)
               VALUES('ethereum',?,?,?,?,100,'2020-01-01T00:00:00+00:00',
                      '2020-01-01T00:00:00+00:00')""",
            (address, first, first, limited),
        )


def test_discovery_commit_enqueues_log_task_atomically(tmp_path):
    db = scanner.DB(tmp_path / "contracts.db")
    db.init_chain("ethereum", 100, True)
    db.init_chain_cursors("ethereum", 100, 99, lookback=0)
    contract = ("ethereum", ADDRESS, 100, "0xtx", ADDRESS, "2026-01-01T00:00:00+00:00")
    source = ("ethereum", ADDRESS, "direct_deploy", 100, "0xtx", ADDRESS,
              "2026-01-01T00:00:00+00:00")
    db.conn.execute(
        """CREATE TRIGGER reject_task BEFORE INSERT ON token_log_tasks
           BEGIN SELECT RAISE(ABORT,'queue unavailable'); END"""
    )
    with pytest.raises(Exception, match="queue unavailable"):
        db.commit_index_range(
            "ethereum", "live", 100, [contract], [source], [], [], [],
            enqueue_token_logs=True,
        )
    assert db.conn.execute("SELECT COUNT(*) FROM contracts").fetchone()[0] == 0
    assert db.cursor("ethereum", "live")["last_committed"] == 99
    db.conn.execute("DROP TRIGGER reject_task")
    assert db.commit_index_range(
        "ethereum", "live", 100, [contract], [source], [], [], [],
        enqueue_token_logs=True,
    ) == (1, 1)
    task = db.token_log_task("ethereum", ADDRESS)
    assert (task["first_block"], task["next_block"], task["history_limited"]) == (100, 100, 0)
    assert db.cursor("ethereum", "live")["last_committed"] == 100
    assert db.commit_index_range(
        "ethereum", "live", 100, [contract], [source], [], [], [],
        enqueue_token_logs=True,
    ) == (0, 0)
    assert db.conn.execute("SELECT COUNT(*) FROM token_log_tasks").fetchone()[0] == 1
    db.close()


def test_priority_migration_is_idempotent_and_skips_low_value(tmp_path):
    db = scanner.DB(tmp_path / "contracts.db")
    db.init_chain("ethereum", 100, True)
    db.upsert_contracts([
        ("ethereum", ADDRESS, 101, "0xtx", ADDRESS, "2026-01-01T00:00:00+00:00"),
        ("ethereum", TOKEN, None, None, None, "2026-01-01T00:00:00+00:00"),
    ])
    db.conn.execute("UPDATE contracts SET last_total_usd=60000 WHERE address=?", (ADDRESS,))
    db.conn.execute("UPDATE contracts SET last_total_usd=1 WHERE address=?", (TOKEN,))
    assert db.seed_priority_token_log_tasks(500000) == 1
    assert db.seed_priority_token_log_tasks(500000) == 0
    assert db.token_log_task("ethereum", ADDRESS)["next_block"] == 101
    assert db.token_log_task("ethereum", TOKEN) is None
    db.close()


def test_active_call_starts_at_observed_block_with_history_flag(tmp_path):
    db = scanner.DB(tmp_path / "contracts.db")
    db.init_chain("ethereum", 100, True)
    db.init_chain_cursors("ethereum", 100, 99, lookback=0)
    contract = ("ethereum", ADDRESS, None, None, None, "2026-01-01T00:00:00+00:00")
    source = ("ethereum", ADDRESS, "active_call", 105, "0xtx", ADDRESS,
              "2026-01-01T00:00:00+00:00")
    db.commit_index_range(
        "ethereum", "live", 105, [contract], [source], [], [], [],
        enqueue_token_logs=True,
    )
    task = db.token_log_task("ethereum", ADDRESS)
    assert (task["first_block"], task["history_limited"]) == (105, 1)
    db.close()


class LogsRpc:
    def __init__(self, result=None, error=None):
        self.result = result if result is not None else []
        self.error = error
        self.queries = []

    async def call(self, method, params):
        assert method == "eth_getLogs"
        self.queries.append(params[0])
        if self.error:
            raise self.error
        return self.result


def test_log_failure_preserves_cursor_then_new_token_wakes_only_its_chain(tmp_path):
    asyncio.run(_log_failure_preserves_cursor_then_new_token_wakes_only_its_chain(tmp_path))


async def _log_failure_preserves_cursor_then_new_token_wakes_only_its_chain(tmp_path):
    db = scanner.DB(tmp_path / "contracts.db")
    queue_task(db)
    chain = replace(config().chains["ethereum"], logs_max_range=128)
    task = db.token_log_task("ethereum", ADDRESS)
    rpc = LogsRpc(error=scanner.RpcError("range", "too many logs"))
    with pytest.raises(scanner.RpcError):
        await scanner.process_token_log_task(db, chain, rpc, task, 100)
    assert db.token_log_task("ethereum", ADDRESS)["next_block"] == 100
    assert db.fail_token_log_task("ethereum", ADDRESS, "range") == 1
    assert db.token_log_task("ethereum", ADDRESS)["window_size"] == 8
    db.conn.execute(
        """INSERT INTO address_chain_state(address,chain,checked_at,status,next_retry_at)
           VALUES(?,?,?,'partial',?)""",
        (ADDRESS, "ethereum", "2026-01-01T00:00:00+00:00",
         "2099-01-01T00:00:00+00:00"),
    )
    rpc = LogsRpc([{
        "topics": [scanner.TRANSFER_TOPIC, "0x" + "00" * 32,
                   "0x" + scanner.pad_addr(ADDRESS)],
        "address": TOKEN, "blockNumber": hex(100),
    }])
    assert await scanner.process_token_log_task(
        db, chain, rpc, db.token_log_task("ethereum", ADDRESS), 100,
    ) == 1
    assert db.token_log_task("ethereum", ADDRESS)["next_block"] == 101
    assert db.conn.execute(
        "SELECT COUNT(*) FROM contract_tokens WHERE chain='ethereum' AND contract=?",
        (ADDRESS,),
    ).fetchone()[0] == 1
    due = db.conn.execute(
        "SELECT next_retry_at FROM address_chain_state WHERE address=? AND chain='ethereum'",
        (ADDRESS,),
    ).fetchone()[0]
    assert due < "2099-01-01"
    assert rpc.queries[0]["topics"][2] == "0x" + scanner.pad_addr(ADDRESS)
    db.close()


def test_once_worker_drains_one_window_and_keeps_queue(tmp_path):
    async def run():
        db = scanner.DB(tmp_path / "contracts.db")
        queue_task(db)

        class Pool:
            token_log_budget = None
            last_head = None

            async def call(self, method, params):
                if method == "eth_blockNumber":
                    return hex(100)
                assert method == "eth_getLogs"
                return []

            def active_endpoint(self, group="head"):
                return "mock.invalid"

        chain = replace(config().chains["ethereum"], confirmations=0)
        await scanner.token_log_loop(
            db, {"ethereum": chain}, {"ethereum": Pool()}, config(),
            asyncio.Event(), once=True,
        )
        assert db.token_log_task("ethereum", ADDRESS)["next_block"] == 101
        assert db.token_log_task("ethereum", ADDRESS)["completed_at"] is not None
        db.close()

    asyncio.run(run())


def test_live_discovery_commits_without_logs_then_worker_resumes(tmp_path):
    async def run():
        db = scanner.DB(tmp_path / "smoke-contracts.db")
        cfg = replace(config(), discover_tx_to_contracts=False)
        chain = replace(cfg.chains["ethereum"], confirmations=0)
        db.init_chain("ethereum", 100, False)
        db.init_chain_cursors("ethereum", 100, 99, lookback=0)
        stop = asyncio.Event()

        class Pool:
            block_batch = 1
            receipt_batch = 1
            token_log_budget = None
            last_head = 100

            async def call(self, method, params):
                if method == "eth_blockNumber":
                    return hex(100)
                if method == "eth_getLogs":
                    raise scanner.RpcError("timeout", "logs unavailable")
                raise AssertionError(method)

            async def batch(self, calls):
                method = calls[0][0]
                if method == "eth_getBlockByNumber":
                    stop.set()
                    return [{"number": hex(100), "transactions": [{
                        "to": None, "hash": "0xtx", "from": TOKEN,
                        "blockNumber": hex(100),
                    }]}]
                if method == "eth_getTransactionReceipt":
                    return [{"contractAddress": ADDRESS}]
                raise AssertionError(method)

            def grow_batch(self, kind):
                pass

            def shrink_batch(self, kind):
                pass

            def active_endpoint(self, group="head"):
                return "mock.invalid"

        pool = Pool()
        await scanner.index_chain_cursor(
            db, chain, pool, cfg, stop, "live", scanner.DiscoverySlots(1, 1),
        )
        assert db.cursor("ethereum", "live")["last_committed"] == 100
        assert db.token_log_task("ethereum", ADDRESS)["next_block"] == 100
        await scanner.token_log_loop(
            db, {"ethereum": chain}, {"ethereum": pool}, cfg,
            asyncio.Event(), once=True,
        )
        task = db.token_log_task("ethereum", ADDRESS)
        assert task["next_block"] == 100
        assert task["failures"] == 1
        assert db.cursor("ethereum", "live")["last_committed"] == 100
        db.close()

    asyncio.run(run())


def test_token_coverage_partial_preserves_known_lower_bound(tmp_path):
    async def run():
        db = scanner.DB(tmp_path / "contracts.db")
        queue_task(db)

        class BalanceRpc:
            call_batch = 3
            last_head = 100

            async def call(self, method, params):
                if method == "eth_getCode":
                    return "0x6000"
                if method == "eth_getBalance":
                    return hex(100 * 10**18)
                raise AssertionError(method)

            async def batch_partial(self, calls):
                return []

        class Prices:
            async def fetch(self, keys, ttl=120):
                return {scanner.llama_key(config().chains["ethereum"]): 1.0}

        cfg = config()
        row, _ = await scanner.scan_address_chain(
            db, cfg.chains["ethereum"], BalanceRpc(), Prices(), cfg,
            ADDRESS, asyncio.Semaphore(1),
        )
        assert row["total_usd"] == 100
        assert row["status"] == "partial"
        assert scanner.classify_address_scan(100, [row], 500)[0] == "incomplete"
        assert scanner.classify_address_scan(100, [row], 50)[0] == "qualifying"
        db.save_address_chain_state(ADDRESS, row, [], 500)
        state = db.conn.execute(
            "SELECT failure_streak,next_retry_at FROM address_chain_state"
        ).fetchone()
        assert state["failure_streak"] == 0
        assert state["next_retry_at"] > "2026-09-25"
        db.conn.execute(
            """UPDATE token_log_tasks SET completed_at='2026-09-25T00:00:00+00:00',
                      next_block=101,due_at='2099-01-01T00:00:00+00:00'"""
        )
        row, _ = await scanner.scan_address_chain(
            db, cfg.chains["ethereum"], BalanceRpc(), Prices(), cfg,
            ADDRESS, asyncio.Semaphore(1),
        )
        assert row["status"] == "complete"
        db.close()

    asyncio.run(run())


def test_budget_counts_attempts_globally_and_per_chain():
    asyncio.run(_budget_counts_attempts_globally_and_per_chain())


async def _budget_counts_attempts_globally_and_per_chain():
    budget = scanner.TokenLogBudget(5, 4)
    for _ in range(4):
        await budget.acquire("ethereum")
    assert await budget.available_chains(["ethereum", "base"]) == ["base"]
    await budget.acquire("base")
    assert await budget.available_chains(["base"]) == []


def test_method_auth_does_not_disable_working_head_and_403_quota_is_rate_limit():
    asyncio.run(_method_auth_does_not_disable_working_head_and_403_quota_is_rate_limit())


async def _method_auth_does_not_disable_working_head_and_403_quota_is_rate_limit():
    assert scanner.classify_rpc_problem(403, "quota exceeded").kind == "rate_limit"

    def handler(request):
        if request.url.host == "first.invalid":
            return httpx.Response(403, text="forbidden")
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": []})

    cfg = replace(config(), max_retries=2, cooldown_min_sec=0)
    pool = scanner.RpcPool(
        "optimism", ["https://first.invalid", "https://second.invalid"], cfg,
        transport=httpx.MockTransport(handler),
    )
    async with pool:
        for ep in pool.endpoints:
            ep.chain_verified = True
        pool.preflight_complete = True
        assert await pool.call("eth_getLogs", [{}]) == []
        assert pool.endpoints[0].health("logs").permanent_error == "auth"
        assert pool.endpoints[0].available("head")
        assert pool.active_endpoint("logs") == "second.invalid"


def test_price_timeout_backoff_never_makes_up_zero(tmp_path, monkeypatch):
    asyncio.run(_price_timeout_backoff_never_makes_up_zero(monkeypatch))


async def _price_timeout_backoff_never_makes_up_zero(monkeypatch):
    class TimeoutClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def get(self, url):
            raise httpx.ConnectTimeout("unavailable")

    monkeypatch.setattr(scanner.httpx, "AsyncClient", TimeoutClient)
    prices = scanner.PriceBook(8)
    assert await prices.fetch(["ethereum:token"]) == {}
    first = prices._cooldown_until
    prices._cooldown_until = 0
    assert await prices.fetch(["ethereum:token"]) == {}
    assert prices._cooldown_until > first + 5
