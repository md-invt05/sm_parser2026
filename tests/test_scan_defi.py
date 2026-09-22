import asyncio
import os
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

import httpx
import yaml

import scan_defi


ROOT = Path(__file__).resolve().parents[1]


class ConfigAndDatabaseTests(unittest.TestCase):
    def test_config_has_exact_supported_networks_and_all_network_default(self):
        cfg = scan_defi.load_config(ROOT / "config.yaml")
        self.assertEqual(20, len(cfg.chains))
        self.assertEqual([*cfg.chains, "sui"], cfg.default_chains)
        excluded = {
            "aurora", "moonbeam", "moonriver", "evmos", "coredao", "songbird", "flare", "pulse"
        }
        self.assertTrue(excluded.isdisjoint(cfg.chains))
        raw = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
        # The YAML snapshot is capped at three public endpoints; keyed/private
        # endpoints from .env are intentionally prepended in addition to them.
        self.assertTrue(all(len(chain.get("rpc") or []) <= 3 for chain in raw["chains"].values()))
        self.assertTrue(cfg.discover_tx_to_contracts)
        self.assertEqual(86400, cfg.code_cache_ttl_sec)
        self.assertEqual(2, cfg.balance_concurrency)
        self.assertEqual(4, cfg.balance_chain_concurrency)
        self.assertEqual(6, cfg.global_rpc_concurrency)
        self.assertEqual("legacy_read_only", cfg.chains["zk"].lifecycle)

    def test_env_rpc_is_prepended_and_public_fallbacks_remain(self):
        old = os.environ.get("ETHEREUM_RPC")
        os.environ["ETHEREUM_RPC"] = "https://private.invalid/key,https://second.invalid/key"
        try:
            cfg = scan_defi.load_config(ROOT / "config.yaml")
        finally:
            if old is None:
                os.environ.pop("ETHEREUM_RPC", None)
            else:
                os.environ["ETHEREUM_RPC"] = old
        self.assertEqual("https://private.invalid/key", cfg.chains["ethereum"].rpc[0])
        self.assertIn("https://ethereum-rpc.publicnode.com", cfg.chains["ethereum"].rpc)

    def test_additive_migration_preserves_legacy_data_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "old.db"
            conn = sqlite3.connect(path)
            conn.executescript(
                """
                CREATE TABLE chain_state(chain TEXT PRIMARY KEY, start_block INTEGER NOT NULL,
                                         last_indexed INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE contracts(
                    chain TEXT NOT NULL, address TEXT NOT NULL, created_block INTEGER,
                    created_tx TEXT, creator TEXT, first_seen_at TEXT NOT NULL,
                    last_checked_at TEXT, last_total_usd REAL, last_status TEXT,
                    PRIMARY KEY(chain,address));
                CREATE TABLE tokens(chain TEXT NOT NULL, address TEXT NOT NULL, symbol TEXT,
                                    decimals INTEGER, source TEXT, PRIMARY KEY(chain,address));
                CREATE TABLE scans(id INTEGER PRIMARY KEY AUTOINCREMENT, chain TEXT NOT NULL,
                    address TEXT NOT NULL, scanned_at TEXT NOT NULL, native_amount REAL,
                    native_usd REAL, tokens_usd REAL, total_usd REAL,
                    meets_threshold INTEGER, note TEXT);
                CREATE TABLE scan_tokens(scan_id INTEGER NOT NULL, token TEXT NOT NULL,
                    symbol TEXT, amount REAL, price_usd REAL, usd_value REAL);
                INSERT INTO contracts(chain,address,created_block,first_seen_at)
                    VALUES('ethereum','0xabc',1,'2026-01-01T00:00:00+00:00');
                """
            )
            conn.commit()
            conn.close()
            first = scan_defi.DB(path)
            first.close()
            second = scan_defi.DB(path)
            self.assertEqual(1, second.conn.execute("SELECT COUNT(*) FROM contracts").fetchone()[0])
            self.assertEqual(1, second.conn.execute("SELECT COUNT(*) FROM schema_meta").fetchone()[0])
            for table in (
                "contract_tokens", "address_scans", "address_chain_scans",
                "address_token_scans", "contract_discoveries", "contract_code_cache",
                "chain_cursors", "address_chain_state", "address_token_state",
                "asset_valuation_policies", "anomalous_balances", "rpc_method_health",
            ):
                row = second.conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
                ).fetchone()
                self.assertIsNotNone(row)
            source = second.conn.execute(
                "SELECT source FROM contract_discoveries WHERE chain='ethereum' AND address='0xabc'"
            ).fetchone()
            self.assertEqual("direct_deploy", source["source"])
            self.assertEqual(7, second.conn.execute("SELECT version FROM schema_meta").fetchone()[0])
            second.close()

    def test_upsert_contract_count_is_exact_for_executemany(self):
        with tempfile.TemporaryDirectory() as folder:
            db = scan_defi.DB(Path(folder) / "db.sqlite")
            rows = [
                ("ethereum", f"0x{i:040x}", i, f"0x{i:064x}", "0xcreator", "now")
                for i in range(101)
            ]
            self.assertEqual(101, db.upsert_contracts(rows))
            self.assertEqual(0, db.upsert_contracts(rows))
            db.close()

    def test_price_book_uses_configured_batch_size(self):
        self.assertEqual(7, scan_defi.PriceBook(timeout=10, batch_size=7).batch_size)

    def test_tx_to_revision_rewinds_once(self):
        with tempfile.TemporaryDirectory() as folder:
            db = scan_defi.DB(Path(folder) / "db.sqlite")
            self.assertEqual(99, db.init_chain("ethereum", 100, False))
            db.set_last_indexed("ethereum", 150)
            self.assertEqual(99, db.init_chain("ethereum", 100, True))
            db.set_last_indexed("ethereum", 120)
            self.assertEqual(120, db.init_chain("ethereum", 100, True))
            db.close()


class ClassificationTests(unittest.TestCase):
    def test_rpc_problem_classification(self):
        self.assertEqual("auth", scan_defi.classify_rpc_problem(401, "denied").kind)
        self.assertTrue(scan_defi.classify_rpc_problem(403, "denied").permanent)
        self.assertEqual("rate_limit", scan_defi.classify_rpc_problem(429, "busy").kind)
        self.assertEqual("http", scan_defi.classify_rpc_problem(503, "down").kind)
        self.assertEqual("auth", scan_defi.classify_rpc_problem(200, "API key required").kind)

    def test_multichain_threshold_and_incomplete_rules(self):
        complete = [
            {"has_code": 1, "status": "complete", "total_usd": 60_000},
            {"has_code": 1, "status": "complete", "total_usd": 50_000},
        ]
        self.assertEqual(("qualifying", 2), scan_defi.classify_address_scan(110_000, complete, 100_000))
        eoa_elsewhere = complete[:1] + [{"has_code": 0, "status": "absent", "total_usd": 0}]
        self.assertEqual(("below", 2), scan_defi.classify_address_scan(60_000, eoa_elsewhere, 100_000))
        unavailable = complete[:1] + [{"has_code": None, "status": "rpc_error", "total_usd": None}]
        self.assertEqual(("incomplete", 1), scan_defi.classify_address_scan(60_000, unavailable, 100_000))
        unpriced = complete[:1] + [{"has_code": 1, "status": "price_missing", "total_usd": 0}]
        self.assertEqual(("incomplete", 2), scan_defi.classify_address_scan(60_000, unpriced, 100_000))

    def test_erc20_decoding(self):
        self.assertEqual(255, scan_defi.decode_uint("0xff"))
        encoded = (
            "0x" + (32).to_bytes(32, "big").hex() + (4).to_bytes(32, "big").hex()
            + b"USDC".ljust(32, b"\0").hex()
        )
        self.assertEqual("USDC", scan_defi.decode_string(encoded))


class RpcTests(unittest.IsolatedAsyncioTestCase):
    async def test_partial_batch_retries_only_missing_items(self):
        requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            payload = __import__("json").loads(request.content)
            requests.append(payload)
            if isinstance(payload, list):
                return httpx.Response(200, json=[{"jsonrpc": "2.0", "id": 0, "result": "0x1"}])
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": "0x2"})

        cfg = replace(scan_defi.load_config(ROOT / "config.yaml"), max_retries=1)
        pool = scan_defi.RpcPool(
            "test", ["https://rpc.invalid"], cfg, transport=httpx.MockTransport(handler)
        )
        async with pool:
            result = await pool.batch([("a", []), ("b", [])])
        self.assertEqual(["0x1", "0x2"], result)
        self.assertEqual(2, len(requests))
        self.assertEqual("b", requests[1]["method"])

    async def test_http_200_rpc_auth_error_switches_endpoint(self):
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.host == "bad.invalid":
                return httpx.Response(
                    200,
                    json={"jsonrpc": "2.0", "id": 1,
                          "error": {"code": -32000, "message": "API key required"}},
                )
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": "0x10"})

        cfg = replace(scan_defi.load_config(ROOT / "config.yaml"), max_retries=2)
        pool = scan_defi.RpcPool(
            "test",
            ["https://bad.invalid", "https://good.invalid"],
            cfg,
            transport=httpx.MockTransport(handler),
        )
        async with pool:
            result = await pool.call("eth_blockNumber", [])
        self.assertEqual("0x10", result)
        self.assertEqual("auth", pool.endpoints[0].permanent_error)

    async def test_preflight_disables_wrong_chain(self):
        def handler(request: httpx.Request) -> httpx.Response:
            payload = __import__("json").loads(request.content)
            method = payload["method"]
            value = "0x2" if method == "eth_chainId" else "0x10"
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": payload["id"], "result": value})

        cfg = replace(scan_defi.load_config(ROOT / "config.yaml"), max_retries=1)
        pool = scan_defi.RpcPool(
            "test", ["https://wrong.invalid"], cfg, expected_chain_id=1,
            transport=httpx.MockTransport(handler)
        )
        async with pool:
            report = await pool.preflight()
        self.assertEqual("wrong_chain", report[0]["error"])
        self.assertIsNotNone(pool.endpoints[0].permanent_error)

    async def test_malformed_json_switches_endpoint(self):
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.host == "broken.invalid":
                return httpx.Response(200, text="not-json")
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": "0x20"})

        cfg = replace(
            scan_defi.load_config(ROOT / "config.yaml"),
            max_retries=2,
            cooldown_min_sec=0,
        )
        pool = scan_defi.RpcPool(
            "test",
            ["https://broken.invalid", "https://good.invalid"],
            cfg,
            transport=httpx.MockTransport(handler),
        )
        async with pool:
            result = await pool.call("eth_blockNumber", [])
        self.assertEqual("0x20", result)
        self.assertEqual("malformed", pool.endpoints[0].last_error)


class FakeIndexRpc:
    def __init__(self, blocks, receipts=None, log_error=False, codes=None):
        self.blocks = blocks
        self.receipts = receipts or {}
        self.log_error = log_error
        self.codes = codes or {}
        self.block_batch = 8
        self.receipt_batch = 20
        self.call_batch = 20
        self.receipt_batches = []
        self.code_batches = []

    async def call(self, method, params):
        if method == "eth_blockNumber":
            return hex(max(self.blocks))
        if method == "eth_getLogs":
            if self.log_error:
                raise scan_defi.RpcError("rpc", "logs failed")
            return []
        raise AssertionError(method)

    async def batch(self, calls):
        method = calls[0][0]
        if method == "eth_getBlockByNumber":
            return [self.blocks[int(params[0], 16)] for _, params in calls]
        if method == "eth_getTransactionReceipt":
            self.receipt_batches.append(len(calls))
            return [self.receipts.get(params[0]) for _, params in calls]
        if method == "eth_getCode":
            self.code_batches.append([params[0].lower() for _, params in calls])
            return [self.codes.get(params[0].lower(), "0x") for _, params in calls]
        raise AssertionError(method)

    def shrink_batch(self, kind="block"):
        if kind == "block":
            self.block_batch = max(1, self.block_batch // 2)
        elif kind == "receipt":
            self.receipt_batch = max(1, self.receipt_batch // 2)
        elif kind == "call":
            self.call_batch = max(1, self.call_batch // 2)

    def grow_batch(self, kind="block"):
        return None


class FakePrices:
    def __init__(self, values):
        self.values = values

    async def fetch(self, keys, ttl=120.0):
        return {key: self.values[key] for key in keys if key in self.values}


class FakeBalanceRpc:
    def __init__(self, code="0x6000", native=0, token_balances=None):
        self.code = code
        self.native = native
        self.token_balances = token_balances or {}
        self.call_batch = 20
        self.balance_calls = 0

    async def call(self, method, params):
        if method == "eth_getCode":
            return self.code
        if method == "eth_getBalance":
            self.balance_calls += 1
            return hex(self.native)
        if method == "eth_call":
            target = params[0]["to"].lower()
            selector = params[0]["data"]
            if selector == scan_defi.DECIMALS_SEL:
                return hex(18)
            if selector == scan_defi.SYMBOL_SEL:
                return "0x" + b"TOK".ljust(32, b"\0").hex()
            return hex(self.token_balances.get(target, 0))
        raise AssertionError(method)

    async def batch_partial(self, calls):
        return [await self.call(method, params) for method, params in calls]


class CursorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.cfg = replace(
            scan_defi.load_config(ROOT / "config.yaml"),
            discover_tokens_from_transfers=False,
            max_retries=2,
            cooldown_min_sec=0,
        )
        self.chain = replace(self.cfg.chains["ethereum"], start_block=100, confirmations=0)

    async def test_missing_receipt_does_not_advance_cursor(self):
        tx = {"hash": "0xdeploy", "to": None, "from": "0xcreator", "blockNumber": hex(100)}
        block = {"number": hex(100), "transactions": [tx]}
        rpc = FakeIndexRpc({100: block}, receipts={"0xdeploy": None})
        with tempfile.TemporaryDirectory() as folder:
            db = scan_defi.DB(Path(folder) / "db.sqlite")
            with self.assertRaises(scan_defi.RpcError):
                await scan_defi.index_chain(
                    db, self.chain, rpc, self.cfg, asyncio.Event(), None, 100, once=True
                )
            self.assertEqual(99, db.last_indexed("ethereum"))
            db.close()

    async def test_missing_block_does_not_advance_cursor(self):
        rpc = FakeIndexRpc({100: None})
        with tempfile.TemporaryDirectory() as folder:
            db = scan_defi.DB(Path(folder) / "db.sqlite")
            with self.assertRaises(scan_defi.RpcError):
                await scan_defi.index_chain(
                    db, self.chain, rpc, self.cfg, asyncio.Event(), None, 100, once=True
                )
            self.assertEqual(99, db.last_indexed("ethereum"))
            db.close()

    async def test_repeated_tx_to_is_checked_once_and_saved_once(self):
        contract = "0x" + "11" * 20
        eoa = "0x" + "22" * 20
        sender = "0x" + "33" * 20
        txs = [
            {"hash": "0x01", "to": contract, "from": sender},
            {"hash": "0x02", "to": contract.upper().replace("0X", "0x"), "from": sender},
            {"hash": "0x03", "to": eoa, "from": sender},
        ]
        block = {"number": hex(100), "transactions": txs}
        rpc = FakeIndexRpc(
            {100: block}, codes={contract: "0x6000", eoa: "0x"}
        )
        with tempfile.TemporaryDirectory() as folder:
            db = scan_defi.DB(Path(folder) / "db.sqlite")
            await scan_defi.index_chain(
                db, self.chain, rpc, self.cfg, asyncio.Event(), None, 100, once=True
            )
            contracts = db.conn.execute(
                "SELECT address, created_block FROM contracts"
            ).fetchall()
            self.assertEqual([(contract, None)], [tuple(row) for row in contracts])
            sources = db.conn.execute(
                "SELECT source FROM contract_discoveries WHERE address=?", (contract,)
            ).fetchall()
            self.assertEqual(["active_call"], [row["source"] for row in sources])
            self.assertEqual(2, sum(len(batch) for batch in rpc.code_batches))
            self.assertEqual(100, db.last_indexed("ethereum"))
            db.close()

    async def test_direct_deploy_and_active_call_share_one_contract(self):
        contract = "0x" + "44" * 20
        sender = "0x" + "55" * 20
        deploy = {
            "hash": "0xdeploy", "to": None, "from": sender,
            "blockNumber": hex(100),
        }
        call = {"hash": "0xcall", "to": contract, "from": sender}
        block = {"number": hex(100), "transactions": [deploy, call]}
        rpc = FakeIndexRpc(
            {100: block}, receipts={"0xdeploy": {"contractAddress": contract}}
        )
        with tempfile.TemporaryDirectory() as folder:
            db = scan_defi.DB(Path(folder) / "db.sqlite")
            await scan_defi.index_chain(
                db, self.chain, rpc, self.cfg, asyncio.Event(), None, 100, once=True
            )
            self.assertEqual(1, db.conn.execute("SELECT COUNT(*) FROM contracts").fetchone()[0])
            self.assertEqual(
                ["active_call", "direct_deploy"],
                [
                    row["source"] for row in db.conn.execute(
                        "SELECT source FROM contract_discoveries ORDER BY source"
                    )
                ],
            )
            self.assertEqual([], rpc.code_batches)
            db.close()

    async def test_eoa_code_cache_avoids_repeat_lookup(self):
        eoa = "0x" + "66" * 20
        sender = "0x" + "77" * 20
        block = {
            "number": hex(100),
            "transactions": [{"hash": "0xeoa", "to": eoa, "from": sender}],
        }
        rpc = FakeIndexRpc({100: block}, codes={eoa: "0x"})
        with tempfile.TemporaryDirectory() as folder:
            db = scan_defi.DB(Path(folder) / "db.sqlite")
            await scan_defi.index_chain(
                db, self.chain, rpc, self.cfg, asyncio.Event(), None, 100, once=True
            )
            db.set_last_indexed("ethereum", 99)
            await scan_defi.index_chain(
                db, self.chain, rpc, self.cfg, asyncio.Event(), None, 100, once=True
            )
            self.assertEqual(1, len(rpc.code_batches))
            self.assertEqual(0, db.conn.execute("SELECT COUNT(*) FROM contracts").fetchone()[0])
            db.close()

    async def test_invalid_code_result_does_not_advance_cursor(self):
        target = "0x" + "88" * 20
        sender = "0x" + "99" * 20
        block = {
            "number": hex(100),
            "transactions": [{"hash": "0xbad", "to": target, "from": sender}],
        }
        rpc = FakeIndexRpc({100: block}, codes={target: None})
        cfg = replace(self.cfg, max_retries=1)
        with tempfile.TemporaryDirectory() as folder:
            db = scan_defi.DB(Path(folder) / "db.sqlite")
            with self.assertRaises(scan_defi.RpcError):
                await scan_defi.index_chain(
                    db, self.chain, rpc, cfg, asyncio.Event(), None, 100, once=True
                )
            self.assertEqual(99, db.last_indexed("ethereum"))
            self.assertEqual(
                0, db.conn.execute("SELECT COUNT(*) FROM contract_code_cache").fetchone()[0]
            )
            db.close()


class MultichainBalanceTests(unittest.IsolatedAsyncioTestCase):
    async def test_same_address_qualifies_across_two_chains_and_eoa_is_ignored(self):
        cfg = replace(scan_defi.load_config(ROOT / "config.yaml"), recheck_interval_sec=86400,
                      rabby_fallback=False)
        address = "0x" + "ab" * 20
        chains = cfg.chains
        pools = {
            key: FakeBalanceRpc(code="0x")
            for key in chains
        }
        pools["ethereum"] = FakeBalanceRpc(native=60_000 * 10**18)
        pools["bsc"] = FakeBalanceRpc(native=50_000 * 10**18)
        # This EOA has a large balance, but eth_getCode is empty and getBalance must not run.
        pools["polygon"] = FakeBalanceRpc(code="0x", native=999_999 * 10**18)
        prices = FakePrices(
            {
                scan_defi.llama_key(chains["ethereum"]): 1.0,
                scan_defi.llama_key(chains["bsc"]): 1.0,
            }
        )
        with tempfile.TemporaryDirectory() as folder:
            db = scan_defi.DB(Path(folder) / "db.sqlite")
            db.upsert_contracts(
                [("ethereum", address, 1, "0xtx", "0xcreator", "2026-01-01T00:00:00+00:00")]
            )
            await scan_defi.check_multichain_balances(
                db, chains, pools, prices, cfg, 100_000, asyncio.Event(), once=True
            )
            row = db.latest_address_scans()[0]
            self.assertEqual("qualifying", row["status"])
            self.assertEqual(110_000, row["total_usd"])
            self.assertEqual(20, row["coverage"])
            self.assertEqual(0, pools["polygon"].balance_calls)
            db.close()

    async def test_positive_unpriced_token_is_saved_and_result_is_incomplete(self):
        cfg = replace(scan_defi.load_config(ROOT / "config.yaml"), recheck_interval_sec=86400,
                      rabby_fallback=False)
        address = "0x" + "cd" * 20
        token = "0x" + "11" * 20
        pools = {key: FakeBalanceRpc(code="0x") for key in cfg.chains}
        pools["ethereum"] = FakeBalanceRpc(
            native=0, token_balances={token.lower(): 7 * 10**18}
        )
        prices = FakePrices({scan_defi.llama_key(cfg.chains["ethereum"]): 1.0})
        with tempfile.TemporaryDirectory() as folder:
            db = scan_defi.DB(Path(folder) / "db.sqlite")
            db.upsert_contracts(
                [("ethereum", address, 1, "0xtx", "0xcreator", "2026-01-01T00:00:00+00:00")]
            )
            db.upsert_tokens([("ethereum", token.lower(), "TOK", 18, "seed")])
            await scan_defi.check_multichain_balances(
                db, cfg.chains, pools, prices, cfg, 100_000, asyncio.Event(), once=True
            )
            row = db.latest_address_scans()[0]
            self.assertEqual("incomplete", row["status"])
            tokens = db.address_scan_tokens(row["id"])
            self.assertEqual(1, len(tokens))
            self.assertEqual("7000000000000000000", tokens[0]["raw_amount"])
            self.assertEqual(0, tokens[0]["priced"])
            db.close()


class CursorBulkTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.cfg = replace(
            scan_defi.load_config(ROOT / "config.yaml"),
            discover_tokens_from_transfers=False,
            max_retries=2,
            cooldown_min_sec=0,
        )
        self.chain = replace(self.cfg.chains["ethereum"], start_block=100, confirmations=0)

    async def test_more_than_eighty_deployments_are_all_processed(self):
        txs = [
            {"hash": f"0x{i:064x}", "to": None, "from": "0xcreator", "blockNumber": hex(100)}
            for i in range(101)
        ]
        receipts = {
            tx["hash"]: {"contractAddress": f"0x{i + 1:040x}"}
            for i, tx in enumerate(txs)
        }
        rpc = FakeIndexRpc({100: {"number": hex(100), "transactions": txs}}, receipts)
        with tempfile.TemporaryDirectory() as folder:
            db = scan_defi.DB(Path(folder) / "db.sqlite")
            await scan_defi.index_chain(
                db, self.chain, rpc, self.cfg, asyncio.Event(), None, 100, once=True
            )
            count = db.conn.execute("SELECT COUNT(*) FROM contracts").fetchone()[0]
            self.assertEqual(101, count)
            self.assertEqual([20, 20, 20, 20, 20, 1], rpc.receipt_batches)
            self.assertEqual(100, db.last_indexed("ethereum"))
            db.close()

    async def test_log_failure_does_not_advance_cursor(self):
        cfg = replace(self.cfg, discover_tokens_from_transfers=True)
        rpc = FakeIndexRpc({100: {"number": hex(100), "transactions": []}}, log_error=True)
        with tempfile.TemporaryDirectory() as folder:
            db = scan_defi.DB(Path(folder) / "db.sqlite")
            db.upsert_contracts(
                [("ethereum", "0x" + "12" * 20, 99, "0xtx", "0xcreator", "now")]
            )
            with self.assertRaises(scan_defi.RpcError):
                await scan_defi.index_chain(
                    db, self.chain, rpc, cfg, asyncio.Event(), None, 100, once=True
                )
            self.assertEqual(99, db.last_indexed("ethereum"))
            db.close()


if __name__ == "__main__":
    unittest.main()
