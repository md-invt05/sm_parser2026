import asyncio
import json
import tempfile
import time
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
from openpyxl import load_workbook

import scan_defi as s


ROOT = Path(__file__).resolve().parents[1]
ADDRESS = "0x" + "ab" * 20


def config(**changes):
    defaults = dict(rabby_fallback=False, balance_chain_timeout_sec=0.2,
                    balance_address_timeout_sec=1, rabby_request_interval_sec=0)
    defaults.update(changes)
    return replace(s.load_config(ROOT / "config.yaml"), **defaults)


def add_address(db, address=ADDRESS):
    db.upsert_contracts([("ethereum", address, 1, "0xtx", "0xcreator", "2026-01-01")])


def missing_row(chain="ethereum", has_code=1):
    return dict(chain=chain, has_code=has_code, status="rpc_error", total_usd=None)


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.db = s.DB(Path(":memory:"))
        self.cfg = config()
        self.chains = {"ethereum": self.cfg.chains["ethereum"]}

    async def asyncTearDown(self):
        self.db.close()

    async def test_eight_addresses_overlap_and_refill_without_waiting_for_slow_address(self):
        addresses = [f"0x{i:040x}" for i in range(1, 11)]
        for address in addresses:
            add_address(self.db, address)
        eight_entered = asyncio.Event()
        slow_release = asyncio.Event()
        fast_release = asyncio.Event()
        last_entered = asyncio.Event()
        calls = []

        async def call(method, params):
            address = params[0]
            calls.append(address)
            if len(calls) == 8:
                eight_entered.set()
            if address == addresses[-1]:
                last_entered.set()
            await (slow_release if address == addresses[0] else fast_release).wait()
            return "0x"

        pool = AsyncMock()
        pool.call.side_effect = call
        cfg = replace(
            self.cfg, balance_concurrency=8,
            balance_chain_concurrency=8,
            balance_chain_timeout_sec=3, balance_address_timeout_sec=4,
        )
        job = asyncio.create_task(s.check_multichain_balances(
            self.db, self.chains, {"ethereum": pool}, AsyncMock(), cfg, 100000,
            asyncio.Event(), once=True,
        ))
        try:
            await asyncio.wait_for(eight_entered.wait(), 2)
            self.assertEqual(8, len(calls))
            fast_release.set()
            await asyncio.wait_for(last_entered.wait(), 2)
            self.assertFalse(job.done())
            slow_release.set()
            await asyncio.wait_for(job, 2)
            self.assertEqual(10, len(set(calls)))
            self.assertEqual(10, self.db.conn.execute("SELECT COUNT(*) FROM address_scans").fetchone()[0])
        finally:
            job.cancel()
            await asyncio.gather(job, return_exceptions=True)

    async def test_slow_network_is_incomplete_but_healthy_network_value_is_saved(self):
        add_address(self.db)
        chains = {key: self.cfg.chains[key] for key in ("ethereum", "base")}

        async def scan(db, chain, *args):
            if chain.key == "base":
                await asyncio.Event().wait()
            return dict(chain="ethereum", has_code=1, status="complete", total_usd=60000), []

        with patch.object(s, "scan_address_chain", side_effect=scan):
            await s.check_multichain_balances(
                self.db, chains, {}, AsyncMock(), replace(self.cfg, balance_address_timeout_sec=0.03),
                100000, asyncio.Event(), once=True,
            )
        row = self.db.latest_address_scans()[0]
        self.assertEqual(("incomplete", 60000, 1), (row["status"], row["total_usd"], row["coverage"]))
        parts = {p["chain"]: p for p in self.db.address_scan_chains(row["id"])}
        self.assertIsNone(parts["base"]["total_usd"])
        self.assertEqual("address_timeout", parts["base"]["note"])

    async def test_stop_cancels_children_without_recording_false_zero(self):
        add_address(self.db)
        entered = asyncio.Event()
        cancelled = asyncio.Event()

        async def call(*args):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        pool = AsyncMock()
        pool.call.side_effect = call
        stop = asyncio.Event()
        job = asyncio.create_task(s.check_multichain_balances(
            self.db, self.chains, {"ethereum": pool}, AsyncMock(), self.cfg, 100000, stop,
        ))
        await asyncio.wait_for(entered.wait(), 1)
        stop.set()
        await asyncio.wait_for(job, 1)
        self.assertTrue(cancelled.is_set())
        self.assertEqual([], self.db.latest_address_scans())

    async def test_network_timeout_keeps_code_and_native_balance(self):
        pool = AsyncMock()
        pool.call_batch = 1
        pool.call.side_effect = ["0x6000", hex(3 * 10**18)]
        async def stalled(*args):
            await asyncio.Event().wait()
        pool.batch_partial.side_effect = stalled
        token = "0x" + "12" * 20
        self.db.upsert_tokens([("ethereum", token, "TOK", 18, "seed")])
        prices = AsyncMock()
        prices.fetch.return_value = {s.llama_key(self.chains["ethereum"]): 2}
        row, tokens = await s.scan_address_chain(
            self.db, self.chains["ethereum"], pool, prices, self.cfg,
            ADDRESS, asyncio.Semaphore(1), 0.03,
        )
        self.assertEqual((1, "partial", 6), (row["has_code"], row["status"], row["total_usd"]))
        self.assertEqual("3000000000000000000", str(tokens[0]["raw_amount"]))
        self.assertFalse(s.BALANCE_RPC.get())

    async def test_cooldown_returns_immediately_and_does_not_change_indexer_policy(self):
        requests = []
        def handle(request):
            requests.append(request)
            return httpx.Response(200, json={"result": "0x", "id": 1})
        pool = s.RpcPool("ethereum", ["https://fake.invalid"], self.cfg,
                         transport=httpx.MockTransport(handle))
        async with pool:
            pool.endpoints[0].cooldown_until = time.time() + 900
            row, _ = await s.scan_address_chain(
                self.db, self.chains["ethereum"], pool, AsyncMock(), self.cfg,
                ADDRESS, asyncio.Semaphore(1), 0.1,
            )
            self.assertEqual("cooldown", row["note"])
            self.assertEqual([], requests)
            self.assertFalse(s.BALANCE_RPC.get())
            async def finish_cooldown(_):
                pool.endpoints[0].cooldown_until = 0
            with patch.object(s.asyncio, "sleep", side_effect=finish_cooldown):
                self.assertEqual("0x", await pool.call("eth_getCode", [ADDRESS, "latest"]))

    async def test_queue_for_one_chain_does_not_reserve_global_rpc_slot(self):
        shared = asyncio.Semaphore(1)
        def handle(request):
            return httpx.Response(200, json={"result": "0x", "id": 1})
        a = s.RpcPool("a", ["https://a.invalid"], self.cfg, global_sem=shared,
                      transport=httpx.MockTransport(handle))
        b = s.RpcPool("b", ["https://b.invalid"], self.cfg, global_sem=shared,
                      transport=httpx.MockTransport(handle))
        a.sem = asyncio.Semaphore(0)
        async with a, b:
            blocked = asyncio.create_task(a.call("x", []))
            try:
                await asyncio.sleep(0)
                self.assertEqual("0x", await asyncio.wait_for(b.call("x", []), 0.2))
            finally:
                blocked.cancel()
                await asyncio.gather(blocked, return_exceptions=True)

    async def test_incomplete_retries_after_five_minutes_not_24_hours(self):
        add_address(self.db)
        self.db.save_address_chain_state(ADDRESS, missing_row(), [], 100000)
        self.assertEqual([], self.db.pending_addresses(86400, retry_sec=600))
        self.assertEqual(
            [ADDRESS],
            self.db.pending_addresses(86400, retry_sec=600, as_of=time.time() + 601),
        )


class RabbyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.cfg = config()
        self.chains = {key: self.cfg.chains[key] for key in ("ethereum", "base")}

    def estimate(self, value=100000, code=0, tokens=None):
        return s.build_rabby_estimate(
            {"total_usd_value": 1e12, "chain_list": [
                {"community_id": 1, "id": "eth", "usd_value": value},
                {"community_id": 8453, "id": "base", "usd_value": 900000},
                {"community_id": 999999, "id": "unsupported", "usd_value": 900000},
            ]}, tokens or [], self.chains,
            [missing_row("ethereum"), missing_row("base", code)], 100000, 0.1,
        )

    async def test_threshold_band_eoa_and_unsupported_networks(self):
        for value, expected in [(89999, "estimated_below"), (90000, "near_threshold"),
                                (100000, "near_threshold"), (110000, "estimated_above")]:
            e = self.estimate(value)
            self.assertEqual(value, e["estimated_usd"])
            self.assertEqual(expected, e["status"])
            self.assertEqual(2, e["coverage"])
        self.assertEqual(100000, self.estimate(110000)["lower_usd"])
        unknown = self.estimate(100000, code=None)
        self.assertEqual("incomplete", unknown["status"])
        self.assertEqual(100000, unknown["estimated_usd"])
        self.assertIsNone(unknown["upper_usd"])

    async def test_positive_unpriced_tokens_are_retained(self):
        e = self.estimate(tokens=[{"chain": "eth", "id": "0xtoken", "symbol": "TOK",
                                  "amount": 7, "raw_amount_str": "7000000000000000000", "price": 0}])
        self.assertEqual("incomplete", e["status"])
        self.assertFalse(e["tokens"][0]["priced"])
        self.assertEqual("7000000000000000000", e["tokens"][0]["raw_amount"])
        self.assertIsNone(e["upper_usd"])

    async def test_429_and_auth_disable_requests_without_fake_auth_headers(self):
        for status in (429, 401, 403):
            requests = []
            def handle(request):
                requests.append(request)
                return httpx.Response(status, headers={"Retry-After": "120"}, json={})
            client = s.RabbyClient(self.cfg, transport=httpx.MockTransport(handle))
            try:
                for _ in range(2):
                    with self.assertRaises(s.RpcError):
                        await client.snapshot(ADDRESS)
                self.assertEqual(1, len(requests))
                self.assertNotIn("Authorization", requests[0].headers)
                self.assertEqual(status != 429, client.disabled)
            finally:
                await client.close()

    async def test_malformed_response_is_not_a_zero_estimate(self):
        for body in ({"error_code": 1}, {"chain_list": None}, None):
            client = s.RabbyClient(self.cfg, transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json=body)))
            try:
                with self.assertRaises(s.RpcError):
                    await client.snapshot(ADDRESS)
            finally:
                await client.close()

    async def test_fallback_only_runs_on_incomplete_and_never_promotes_rpc_status(self):
        db = s.DB(Path(":memory:"))
        add_address(db)
        db.save_address_scan(ADDRESS, "below", 0, 1, 1,
                             [dict(chain="ethereum", has_code=0, status="absent", total_usd=0)], [])
        self.assertIsNone(db.pending_rabby_scan(300))
        scan_id = db.save_address_scan(ADDRESS, "incomplete", 0, 1, 1, [missing_row()], [])
        fake = AsyncMock()
        fake.disabled = False
        fake.cooldown_until = 0
        fake.snapshot.return_value = ({"chain_list": [
            {"community_id": 1, "id": "eth", "usd_value": 600000}]}, [])
        done = asyncio.Event()
        done.set()
        try:
            with patch.object(s, "RabbyClient", return_value=fake):
                await s.rabby_fallback_loop(db, {"ethereum": self.chains["ethereum"]},
                                           replace(self.cfg, rabby_token_discovery_after_sec=0),
                                           asyncio.Event(), done, True)
            self.assertEqual(1, fake.snapshot.await_count)
            estimate = db.rabby_estimate(ADDRESS)
            self.assertEqual("estimated_above", estimate["status"])
            self.assertEqual(scan_id, estimate["rpc_scan_id"])
            self.assertEqual("incomplete", db.latest_address_scans()[0]["status"])
            with tempfile.TemporaryDirectory() as folder:
                cfg = replace(self.cfg, export_dir=Path(folder))
                paths = s.export_xlsx(db, cfg, self.chains, 100000)
                wb = load_workbook(paths[0])
                self.assertEqual(1, wb.active.max_row)
                wb.close()
                wb = load_workbook(paths[2])
                header = [c.value for c in wb.active[1]]
                result = dict(zip(header, [c.value for c in wb.active[2]]))
                self.assertEqual(0, result["Total USD"])
                self.assertEqual(600000, result["Rabby estimate USD"])
                wb.close()
        finally:
            db.close()

    async def test_existing_intermediate_rabby_table_migrates_without_data_loss(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "migration.db"
            db = s.DB(path)
            db.conn.execute("DROP TABLE rabby_estimates")
            db.conn.execute("""CREATE TABLE rabby_estimates (
                address TEXT PRIMARY KEY, checked_at TEXT NOT NULL, status TEXT NOT NULL,
                estimated_usd REAL, lower_usd REAL, upper_usd REAL, coverage INTEGER NOT NULL,
                total_networks INTEGER NOT NULL, uncertainty REAL NOT NULL, chain_parts TEXT NOT NULL,
                token_parts TEXT NOT NULL, note TEXT)""")
            db.conn.execute("INSERT INTO rabby_estimates VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (ADDRESS, "now", "incomplete", None, None, None, 0, 20, .1, "[]", "[]", None))
            db.conn.commit()
            db.close()
            migrated = s.DB(path)
            try:
                self.assertIn("rpc_scan_id", {r["name"] for r in migrated.conn.execute(
                    "PRAGMA table_info(rabby_estimates)")})
                self.assertEqual(1, migrated.conn.execute("SELECT COUNT(*) FROM rabby_estimates").fetchone()[0])
            finally:
                migrated.close()


if __name__ == "__main__":
    unittest.main()
