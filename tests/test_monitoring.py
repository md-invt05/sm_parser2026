import asyncio
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import yaml
import httpx

from monitoring import LOAD_PROFILES, MonitorStore, parse_period, percentile
from telegram_bot import BotService, DockerController, ReportBuilder, parse_prometheus
from docker_guard import CONTAINER_ROUTE
import scan_defi


ROOT = Path(__file__).resolve().parents[1]


class MonitoringStoreTests(unittest.TestCase):
    def test_schema_is_idempotent_and_settings_persist(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "monitoring.db"
            first = MonitorStore(path)
            first.set_setting("load_profile", "high")
            run_id = first.begin_run("full", 500000, "high", {"once": False})
            first.heartbeat(run_id)
            first.close()
            second = MonitorStore(path)
            self.assertEqual("high", second.setting("load_profile"))
            self.assertEqual(run_id, second.runtime()["run_id"])
            self.assertEqual(1, second.rows("SELECT COUNT(*) n FROM schema_meta")[0]["n"])
            second.close()

    def test_incident_deduplication_and_single_recovery(self):
        with tempfile.TemporaryDirectory() as folder:
            store = MonitorStore(Path(folder) / "monitoring.db")
            incident_id, created = store.open_incident("rpc:bsc", "critical", "rpc", "down")
            self.assertTrue(created)
            repeated_id, created = store.open_incident("rpc:bsc", "critical", "rpc", "still down")
            self.assertFalse(created)
            self.assertEqual(incident_id, repeated_id)
            pending = store.pending_incident_notifications()
            self.assertEqual(1, len(pending))
            self.assertEqual(2, pending[0]["repeats"])
            store.mark_incident_notified(incident_id, recovery=False)
            self.assertEqual([], store.pending_incident_notifications())
            store.resolve_incident("rpc:bsc")
            self.assertEqual(1, len(store.pending_incident_notifications()))
            store.mark_incident_notified(incident_id, recovery=True)
            self.assertEqual([], store.pending_incident_notifications())
            store.close()

    def test_control_queue_survives_restart(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "monitoring.db"
            first = MonitorStore(path)
            request_id = first.request_control("backup", "710271818")
            first.close()
            second = MonitorStore(path)
            pending = second.pending_controls()
            self.assertEqual(request_id, pending[0].id)
            self.assertEqual("backup", pending[0].action)
            second.finish_control(request_id, "complete", "ok")
            self.assertEqual([], second.pending_controls())
            second.close()

    def test_online_backup_keeps_seven_newest_files(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            db = scan_defi.DB(root / "contracts.db")
            for _ in range(9):
                scan_defi.backup_database(db, root / "backups", keep=7)
            self.assertEqual(7, len(list((root / "backups").glob("contracts_*.db"))))
            db.close()

    def test_concurrent_writes(self):
        with tempfile.TemporaryDirectory() as folder:
            store = MonitorStore(Path(folder) / "monitoring.db")
            errors = []
            def writer(index):
                try:
                    for item in range(25):
                        store.set_setting(f"worker_{index}", item)
                except Exception as exc:  # pragma: no cover - assertion aid
                    errors.append(exc)
            jobs = [threading.Thread(target=writer, args=(index,)) for index in range(4)]
            for job in jobs:
                job.start()
            for job in jobs:
                job.join()
            self.assertEqual([], errors)
            self.assertEqual("24", store.setting("worker_3"))
            store.close()


class MonitoringHelpersTests(unittest.TestCase):
    def test_profiles_match_fixed_safe_values(self):
        self.assertEqual(6, LOAD_PROFILES["low"]["global_rpc_concurrency"])
        self.assertEqual(8, LOAD_PROFILES["normal"]["balance_concurrency"])
        self.assertEqual(20, LOAD_PROFILES["high"]["balance_chain_concurrency"])
        self.assertEqual({"low", "normal", "high"}, set(LOAD_PROFILES))

    def test_period_and_percentile(self):
        self.assertEqual(21600, parse_period("6h"))
        self.assertEqual(95, percentile([0, 100], .95))
        with self.assertRaises(ValueError):
            parse_period("13h")

    def test_prometheus_parser(self):
        metrics = parse_prometheus(
            '# HELP sample x\nnode_load1 2.5\nnode_cpu_seconds_total{cpu="0",mode="idle"} 10\n'
        )
        self.assertEqual(2.5, metrics["node_load1"][0][1])
        self.assertEqual("idle", metrics["node_cpu_seconds_total"][0][0]["mode"])

    def test_report_period_delta(self):
        with tempfile.TemporaryDirectory() as folder:
            store = MonitorStore(Path(folder) / "monitoring.db")
            run_id = store.begin_run("full", 500000, "normal", {})
            common = dict(
                run_id=run_id, balance_pending=5, qualifying=1, below_count=2,
                incomplete=3, coverage_json={}, db_bytes=100, wal_bytes=0,
                reports_json={}, last_export_at=None,
            )
            store.add_aggregate_sample(
                unique_addresses=10, contract_instances=12, direct_deploy=2,
                active_call=10, balance_completed=4, **common,
            )
            store.add_aggregate_sample(
                unique_addresses=15, contract_instances=20, direct_deploy=3,
                active_call=17, balance_completed=9, **common,
            )
            report = ReportBuilder(store, Path(folder) / "contracts.db", timezone.utc).report(3600)
            self.assertIn("Новых уникальных адресов: 5", report)
            self.assertIn("Новых instances: 8", report)
            store.close()

    def test_whitelist_is_fail_closed(self):
        service = BotService.__new__(BotService)
        service.allowed_chats = {710271818}
        allowed = SimpleNamespace(effective_chat=SimpleNamespace(id=710271818))
        denied = SimpleNamespace(effective_chat=SimpleNamespace(id=1))
        missing = SimpleNamespace(effective_chat=None)
        self.assertTrue(service.allowed(allowed))
        self.assertFalse(service.allowed(denied))
        self.assertFalse(service.allowed(missing))

    def test_confirmation_is_bound_to_chat_and_expires(self):
        service = BotService.__new__(BotService)
        service.confirmations = {
            "good": (710271818, time.time() + 60, "restart", {}),
            "expired": (710271818, time.time() - 1, "stop", {}),
            "other": (710271818, time.time() + 60, "start", {}),
        }
        self.assertEqual(("restart", {}), service.consume_confirmation("good", 710271818))
        self.assertIsNone(service.consume_confirmation("expired", 710271818))
        self.assertIsNone(service.consume_confirmation("other", 1))

    def test_compose_does_not_mount_socket_into_bot(self):
        compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
        bot_volumes = compose["services"]["telegram-bot"].get("volumes", [])
        self.assertTrue(all("docker.sock" not in volume for volume in bot_volumes))
        proxy_volumes = compose["services"]["docker-proxy"]["volumes"]
        self.assertTrue(any("docker.sock" in volume for volume in proxy_volumes))

    def test_docker_guard_has_only_fixed_lifecycle_and_stats_routes(self):
        allowed = {"json", "stats", "start", "stop", "restart"}
        for action in allowed:
            match = CONTAINER_ROUTE.fullmatch(f"/containers/evm-scanner/{action}")
            self.assertIsNotNone(match)
            self.assertEqual(action, match.group(2))
        self.assertIsNone(CONTAINER_ROUTE.fullmatch("/containers/json"))
        self.assertIsNone(CONTAINER_ROUTE.fullmatch("/images/json"))
        self.assertIsNone(CONTAINER_ROUTE.fullmatch("/containers/evm-scanner/exec"))


class MonitoringAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_docker_controller_checks_label_before_lifecycle(self):
        calls = []
        async def handler(request: httpx.Request) -> httpx.Response:
            calls.append((request.method, request.url.path))
            if request.method == "GET":
                return httpx.Response(200, json={"Config": {"Labels": {"evm-parser.controlled": "true"}}})
            return httpx.Response(204)
        controller = DockerController(
            "http://proxy", "evm-scanner", transport=httpx.MockTransport(handler)
        )
        try:
            await controller.lifecycle("restart")
        finally:
            await controller.close()
        self.assertEqual(
            [("GET", "/containers/evm-scanner/json"), ("POST", "/containers/evm-scanner/restart")],
            calls,
        )

    async def test_sustained_critical_cpu_opens_incident(self):
        with tempfile.TemporaryDirectory() as folder:
            store = MonitorStore(Path(folder) / "monitoring.db")
            now = datetime.now(timezone.utc)
            for offset in range(300, -1, -15):
                store.add_resource_sample({
                    "ts": (now - timedelta(seconds=offset)).isoformat(),
                    "cpu_percent": 97,
                    "cpu_cores": 4,
                    "load1": 1,
                })
            service = BotService.__new__(BotService)
            service.monitor = store
            await service._evaluate_resource_thresholds(now)
            fingerprints = {row["fingerprint"] for row in store.rows(
                "SELECT fingerprint FROM incidents WHERE resolved_at IS NULL"
            )}
            self.assertIn("system:cpu:critical", fingerprints)
            store.close()

    async def test_pause_waits_until_resumed(self):
        with tempfile.TemporaryDirectory() as folder:
            store = MonitorStore(Path(folder) / "monitoring.db")
            store.set_setting("scanner_paused", "1")
            stop = asyncio.Event()
            waiter = asyncio.create_task(scan_defi.wait_if_paused(store, stop))
            await asyncio.sleep(0.02)
            self.assertFalse(waiter.done())
            store.set_setting("scanner_paused", "0")
            await asyncio.wait_for(waiter, 2)
            store.close()


if __name__ == "__main__":
    unittest.main()
