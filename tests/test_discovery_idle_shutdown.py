import asyncio
import unittest
from pathlib import Path
from types import SimpleNamespace

import httpx
import scan_defi as scanner
from sui_support import BlockberryClient, SuiConfig


class IdleDiscoveryAndShutdownTests(unittest.IsolatedAsyncioTestCase):
    async def test_zero_capacity_indexer_never_probes_head(self):
        class CursorDB:
            def contract_addresses(self, _chain):
                return set()

            def discovery_addresses(self, _chain, _source):
                return set()

            def cursor(self, _chain, _role):
                return {"status": "active", "next_block": 100,
                        "anchor_block": 200, "note": ""}

        class Rpc:
            last_head = None
            calls = 0

            async def call(self, _method, _params):
                self.calls += 1
                return "0xc8"

        stop = asyncio.Event()
        slots = scanner.DiscoverySlots(0, 1)
        await slots.set_capacity(0, 0)
        rpc = Rpc()
        task = asyncio.create_task(scanner.index_chain_cursor(
            CursorDB(), SimpleNamespace(key="ethereum", confirmations=0), rpc,
            SimpleNamespace(all_down_sleep_sec=1), stop, "live", slots,
        ))
        await asyncio.sleep(0.03)
        self.assertEqual(0, rpc.calls)
        self.assertEqual(1, (await slots.snapshot())["live_waiting"])
        await scanner.stop_background_workers(stop, slots, [task], timeout_sec=0.1)
        self.assertEqual(0, rpc.calls)
        self.assertEqual(0, (await slots.snapshot())["live_active"])
        self.assertEqual(0, (await slots.snapshot())["live_waiting"])

    async def test_zero_capacity_reorg_probe_is_queued(self):
        class GapDB:
            def next_discovery_gap(self, _chain):
                return None

            def pending_reorg_conflict(self, _chain):
                return 100

        class Rpc:
            calls = 0

            async def call(self, _method, _params):
                self.calls += 1
                return {}

        stop = asyncio.Event()
        slots = scanner.DiscoverySlots(0, 1)
        await slots.set_capacity(0, 0)
        rpc = Rpc()
        task = asyncio.create_task(scanner.index_chain_gaps(
            GapDB(), SimpleNamespace(key="ethereum"), rpc,
            SimpleNamespace(), stop, slots,
        ))
        await asyncio.sleep(0.03)
        self.assertEqual(0, rpc.calls)
        await scanner.stop_background_workers(stop, slots, [task], timeout_sec=0.1)
        self.assertEqual(0, (await slots.snapshot())["backfill_active"])

    async def test_shutdown_waits_for_active_worker_before_client_close(self):
        events = []
        stop = asyncio.Event()
        slots = scanner.DiscoverySlots(1, 1)
        active_ready = asyncio.Event()

        async def active():
            async with slots.slot("live", "ethereum"):
                active_ready.set()
                await stop.wait()
                await asyncio.sleep(0.02)
                events.append("worker_finished")

        async def queued():
            async with slots.slot("live", "base"):
                events.append("queued_started")

        first = asyncio.create_task(active())
        await active_ready.wait()
        second = asyncio.create_task(queued())
        await asyncio.sleep(0.01)
        await scanner.stop_background_workers(stop, slots, [first, second], timeout_sec=0.2)
        events.append("client_closed")
        self.assertEqual(["worker_finished", "client_closed"], events)
        self.assertEqual(0, (await slots.snapshot())["live_active"])

    async def test_token_head_attempt_has_token_workload(self):
        cfg = scanner.load_config(Path(scanner.__file__).parent / "config.yaml")
        pool = scanner.RpcPool(
            "ethereum", ["https://rpc.invalid"], cfg,
            transport=httpx.MockTransport(lambda _request: httpx.Response(
                200, json={"jsonrpc": "2.0", "id": 1, "result": "0x1"},
            )),
        )
        async with pool:
            workload = scanner.RPC_WORKLOAD.set("token_logs")
            try:
                await pool._request_endpoint(
                    pool.endpoints[0],
                    {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []},
                    "head",
                )
            finally:
                scanner.RPC_WORKLOAD.reset(workload)
        self.assertEqual("token_logs", pool.pending_metric_attempts[-1][1])

    async def test_sui_raw_timing_aggregates_without_secret_or_digest(self):
        client = BlockberryClient("private-test-key", SuiConfig(max_retries=1))
        client.client = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _request: httpx.Response(
                200, json={"result": {"transaction": {}}},
            )), base_url="https://blockberry.invalid",
        )
        try:
            await client.raw_transaction("digest-only-for-test")
            client.record_enrichment_timing(1.0, 2.0, 3.0, True)
            summary = client.take_enrichment_timings()
        finally:
            await client.client.aclose()
        self.assertEqual(1, summary["raw_attempted"])
        self.assertEqual(1, summary["completed"])
        self.assertEqual((2, 2), summary["db_ms"])
        self.assertGreaterEqual(summary["http_ms"][1], 0)
        self.assertNotIn("private-test-key", str(summary))
        self.assertNotIn("digest-only-for-test", str(summary))


if __name__ == "__main__":
    unittest.main()
