from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import tempfile
import time
import uuid
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable
from zoneinfo import ZoneInfo

import httpx
from dotenv import load_dotenv

from monitoring import LOAD_PROFILES, MonitorStore, parse_period, percentile, utc_now

try:
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
    from telegram.ext import (
        Application,
        CallbackQueryHandler,
        CommandHandler,
        ContextTypes,
    )
except ImportError:  # Keeps pure helper/unit tests importable without the optional bot package.
    Application = None  # type: ignore[assignment]


ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")
log = logging.getLogger("telegram-monitor")
UTC = timezone.utc
TELEGRAM_SAFE_FILE_BYTES = 45 * 1024 * 1024
FILE_FRESH_SEC = 6 * 3600


def human_bytes(value: float | int | None) -> str:
    if value is None:
        return "n/a"
    amount = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(amount) < 1024 or unit == "TiB":
            return f"{amount:.1f} {unit}"
        amount /= 1024
    return "n/a"


def human_duration(seconds: float | int | None) -> str:
    if seconds is None:
        return "n/a"
    seconds = max(0, int(seconds))
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if not parts:
        parts.append(f"{seconds}s")
    return " ".join(parts)


def iso_to_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
    except ValueError:
        return None


def timestamp_span(first: str | None, last: str | None) -> float:
    first_dt, last_dt = iso_to_dt(first), iso_to_dt(last)
    return max(0.0, (last_dt - first_dt).total_seconds()) if first_dt and last_dt else 0.0


def balance_scans_stalled(sample: Any) -> bool:
    return bool(
        int(sample["pending"] or 0) > 0
        and sample["min_done"] is not None
        and sample["min_done"] == sample["max_done"]
        and timestamp_span(sample["first_ts"], sample["last_ts"]) >= 540
    )


def parse_prometheus(text: str) -> dict[str, list[tuple[dict[str, str], float]]]:
    out: dict[str, list[tuple[dict[str, str], float]]] = defaultdict(list)
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or " " not in line:
            continue
        metric, raw_value = line.rsplit(None, 1)
        try:
            value = float(raw_value)
        except ValueError:
            continue
        labels: dict[str, str] = {}
        if "{" in metric and metric.endswith("}"):
            name, label_text = metric[:-1].split("{", 1)
            metric = name
            for pair in label_text.split(","):
                if "=" in pair:
                    key, val = pair.split("=", 1)
                    labels[key] = val.strip('"')
        out[metric].append((labels, value))
    return out


def _metric_sum(metrics: dict[str, list[tuple[dict[str, str], float]]], name: str,
                predicate: Callable[[dict[str, str]], bool] | None = None) -> float:
    return sum(value for labels, value in metrics.get(name, []) if predicate is None or predicate(labels))


class DockerController:
    def __init__(self, base_url: str, container_name: str, transport: httpx.AsyncBaseTransport | None = None):
        self.base_url = base_url.rstrip("/")
        self.container_name = container_name
        self.client = httpx.AsyncClient(timeout=15, transport=transport)

    async def close(self) -> None:
        await self.client.aclose()

    async def inspect(self) -> dict[str, Any]:
        response = await self.client.get(f"{self.base_url}/containers/{self.container_name}/json")
        response.raise_for_status()
        return response.json()

    async def stats(self) -> dict[str, Any]:
        response = await self.client.get(
            f"{self.base_url}/containers/{self.container_name}/stats", params={"stream": "false"}
        )
        response.raise_for_status()
        return response.json()

    async def lifecycle(self, action: str) -> None:
        if action not in {"start", "stop", "restart"}:
            raise ValueError("unsupported lifecycle action")
        inspected = await self.inspect()
        labels = inspected.get("Config", {}).get("Labels", {}) or {}
        if labels.get("evm-parser.controlled") != "true":
            raise PermissionError("target container lacks the required control label")
        params = {"t": "30"} if action in {"stop", "restart"} else None
        response = await self.client.post(
            f"{self.base_url}/containers/{self.container_name}/{action}", params=params
        )
        if response.status_code not in (204, 304):
            response.raise_for_status()


class ResourceCollector:
    def __init__(self, node_url: str, docker: DockerController):
        self.node_url = node_url.rstrip("/")
        self.docker = docker
        self.client = httpx.AsyncClient(timeout=15)
        self.previous_node: tuple[float, float, float] | None = None

    async def close(self) -> None:
        await self.client.aclose()

    async def collect(self) -> dict[str, Any]:
        response = await self.client.get(f"{self.node_url}/metrics")
        response.raise_for_status()
        metrics = parse_prometheus(response.text)
        now = time.time()
        cpu_total = _metric_sum(metrics, "node_cpu_seconds_total")
        cpu_idle = _metric_sum(metrics, "node_cpu_seconds_total", lambda labels: labels.get("mode") == "idle")
        cpu_iowait = _metric_sum(metrics, "node_cpu_seconds_total", lambda labels: labels.get("mode") == "iowait")
        cpu_percent = io_wait = None
        if self.previous_node:
            elapsed_total = cpu_total - self.previous_node[0]
            if elapsed_total > 0:
                cpu_percent = max(0.0, min(100.0, 100 * (1 - (cpu_idle - self.previous_node[1]) / elapsed_total)))
                io_wait = max(0.0, 100 * (cpu_iowait - self.previous_node[2]) / elapsed_total)
        self.previous_node = (cpu_total, cpu_idle, cpu_iowait)

        mem_total = _metric_sum(metrics, "node_memory_MemTotal_bytes")
        mem_available = _metric_sum(metrics, "node_memory_MemAvailable_bytes")
        swap_total = _metric_sum(metrics, "node_memory_SwapTotal_bytes")
        swap_free = _metric_sum(metrics, "node_memory_SwapFree_bytes")
        filesystems = [
            (labels, value) for labels, value in metrics.get("node_filesystem_avail_bytes", [])
            if labels.get("fstype") not in {"tmpfs", "overlay", "squashfs"}
        ]
        disk_free = None
        for labels, available in filesystems:
            size = next((value for size_labels, value in metrics.get("node_filesystem_size_bytes", [])
                         if size_labels.get("mountpoint") == labels.get("mountpoint") and
                         size_labels.get("device") == labels.get("device")), 0)
            if size > 0:
                current = 100 * available / size
                disk_free = current if disk_free is None else min(disk_free, current)
        cpu_cores = max(1, len({labels.get("cpu") for labels, _ in metrics.get("node_cpu_seconds_total", []) if labels.get("cpu")}))
        data: dict[str, Any] = {
            "ts": datetime.fromtimestamp(now, UTC).isoformat(),
            "cpu_percent": cpu_percent,
            "load1": _metric_sum(metrics, "node_load1") or None,
            "cpu_cores": cpu_cores,
            "ram_percent": (100 * (mem_total - mem_available) / mem_total) if mem_total else None,
            "swap_percent": (100 * (swap_total - swap_free) / swap_total) if swap_total else 0.0,
            "disk_free_percent": disk_free,
            "io_wait_percent": io_wait,
            "net_rx_bytes": int(_metric_sum(metrics, "node_network_receive_bytes_total", lambda labels: labels.get("device") != "lo")),
            "net_tx_bytes": int(_metric_sum(metrics, "node_network_transmit_bytes_total", lambda labels: labels.get("device") != "lo")),
        }
        try:
            inspect, stats = await asyncio.gather(self.docker.inspect(), self.docker.stats())
            cpu_delta = stats.get("cpu_stats", {}).get("cpu_usage", {}).get("total_usage", 0) - stats.get("precpu_stats", {}).get("cpu_usage", {}).get("total_usage", 0)
            system_delta = stats.get("cpu_stats", {}).get("system_cpu_usage", 0) - stats.get("precpu_stats", {}).get("system_cpu_usage", 0)
            online = stats.get("cpu_stats", {}).get("online_cpus") or cpu_cores
            container_cpu = (cpu_delta / system_delta * online * 100) if system_delta > 0 else None
            networks = stats.get("networks", {}).values()
            blk = stats.get("blkio_stats", {}).get("io_service_bytes_recursive") or []
            data.update({
                "container_cpu_percent": container_cpu,
                "container_mem_bytes": stats.get("memory_stats", {}).get("usage"),
                "container_mem_limit": stats.get("memory_stats", {}).get("limit"),
                "container_net_rx_bytes": sum(int(row.get("rx_bytes", 0)) for row in networks),
                "container_net_tx_bytes": sum(int(row.get("tx_bytes", 0)) for row in networks),
                "container_block_read_bytes": sum(int(row.get("value", 0)) for row in blk if str(row.get("op", "")).lower() == "read"),
                "container_block_write_bytes": sum(int(row.get("value", 0)) for row in blk if str(row.get("op", "")).lower() == "write"),
                "container_pids": stats.get("pids_stats", {}).get("current"),
                "restart_count": inspect.get("RestartCount", 0),
                "details_json": {"container_running": bool(inspect.get("State", {}).get("Running"))},
            })
        except Exception as exc:
            data["details_json"] = {"docker_error": type(exc).__name__}
        return data


class ReportBuilder:
    def __init__(self, monitor: MonitorStore, contracts_db: Path, tz: ZoneInfo):
        self.monitor = monitor
        self.contracts_db = contracts_db
        self.tz = tz

    def _fmt_time(self, value: str | None) -> str:
        parsed = iso_to_dt(value)
        return parsed.astimezone(self.tz).strftime("%d.%m %H:%M:%S") if parsed else "n/a"

    def _sui_summary(self, cutoff: str | None = None) -> dict[str, Any] | None:
        if not self.contracts_db.exists():
            return None
        try:
            runtime = self.monitor.runtime()
            min_usd = float(runtime["min_usd"] or 500000) if runtime else 500000.0
            with sqlite3.connect(self.contracts_db, timeout=5) as conn:
                package_total = int(conn.execute("SELECT COUNT(*) FROM sui_packages").fetchone()[0])
                new_packages = int(conn.execute(
                    "SELECT COUNT(*) FROM sui_packages WHERE first_seen_at>=?", (cutoff,),
                ).fetchone()[0]) if cutoff else 0
                objects = int(conn.execute(
                    "SELECT COUNT(*) FROM sui_package_objects WHERE active=1"
                ).fetchone()[0])
                pending = int(conn.execute(
                    "SELECT COUNT(*) FROM sui_seen_transactions WHERE enriched=0"
                ).fetchone()[0])
                sources = dict(conn.execute(
                    "SELECT source,COUNT(*) FROM sui_discoveries GROUP BY source"
                ).fetchall())
                statuses = dict(conn.execute(
                    """SELECT CASE
                         WHEN status='incomplete' OR provider_complete=0 OR packages_json='[]'
                           OR strftime('%s','now')-strftime('%s',synced_at)>1800 THEN 'incomplete'
                         WHEN COALESCE(indexed_tvl,0)>=? THEN 'qualifying'
                         ELSE 'below' END AS current_status,COUNT(*)
                       FROM sui_defi_projects GROUP BY current_status""",
                    (min_usd,),
                ).fetchall())
                sync_row = conn.execute(
                    "SELECT value FROM sui_sync_state WHERE key='last_defi_sync'"
                ).fetchone()
                last_sync = float(sync_row[0]) if sync_row else 0.0
                error_row = conn.execute(
                    "SELECT value FROM sui_sync_state WHERE key='last_defi_error'"
                ).fetchone()
            return {
                "packages": package_total, "new_packages": new_packages,
                "objects": objects, "pending": pending,
                "publish": int(sources.get("publish", 0)),
                "active_call": int(sources.get("active_call", 0)),
                "qualifying": int(statuses.get("qualifying", 0)),
                "below": int(statuses.get("below", 0)),
                "incomplete": int(statuses.get("incomplete", 0)),
                "snapshot_age_sec": max(0.0, time.time() - last_sync) if last_sync else None,
                "last_error": str(error_row[0]) if error_row and error_row[0] else None,
            }
        except sqlite3.Error:
            return None

    def status(self) -> str:
        runtime = self.monitor.runtime()
        latest = self.monitor.rows("SELECT * FROM aggregate_samples ORDER BY id DESC LIMIT 1")
        if runtime is None:
            return "Парсер ещё не записал runtime-состояние."
        heartbeat = iso_to_dt(runtime["heartbeat_at"])
        age = (datetime.now(UTC) - heartbeat).total_seconds() if heartbeat else None
        aggregate = latest[0] if latest else None
        uptime = None
        started = iso_to_dt(runtime["started_at"])
        if started:
            uptime = (datetime.now(UTC) - started).total_seconds()
        lines = [
            f"Парсер: {runtime['state']} | heartbeat {human_duration(age)} назад",
            f"Uptime: {human_duration(uptime)} | профиль: {runtime['load_profile']} | порог: ${float(runtime['min_usd'] or 0):,.0f}",
        ]
        lines.append(f"Exporter: {self.monitor.setting('exporter_state', 'idle')}")
        runtime_note: dict[str, Any] = {}
        try:
            runtime_note = json.loads(runtime["note"] or "{}")
            scheduler = runtime_note.get("discovery_scheduler") or {}
        except (TypeError, ValueError, json.JSONDecodeError):
            runtime_note = {}
            scheduler = {}
        if scheduler:
            lines.append(
                "Discovery: "
                f"live {scheduler.get('live_active', 0)}/{scheduler.get('live_slots', 0)}, "
                f"wait {scheduler.get('live_waiting', 0)}, "
                f"max {float(scheduler.get('live_max_wait_sec', 0)):.0f}s; "
                f"backfill {scheduler.get('backfill_active', 0)}/{scheduler.get('backfill_slots', 0)}, "
                f"wait {scheduler.get('backfill_waiting', 0)}, "
                f"max {float(scheduler.get('backfill_max_wait_sec', 0)):.0f}s"
            )
        governor = runtime_note.get("load_governor") or {} if isinstance(runtime_note, dict) else {}
        budgets = runtime_note.get("rpc_budgets") or {} if isinstance(runtime_note, dict) else {}
        if governor:
            lines.append(
                f"Governor: {governor.get('state', 'unknown')} | {governor.get('reason', '')}"
            )
        if budgets:
            lines.append(
                f"RPC budget: discovery {budgets.get('discovery_limit', 0)}, "
                f"balances {budgets.get('balance_limit', 0)}, total {budgets.get('total_limit', 0)}"
            )
        token_logs = runtime_note.get("token_logs") or {} if isinstance(runtime_note, dict) else {}
        if token_logs:
            lines.append(
                "Token logs: "
                f"due {sum(int(item.get('due', 0)) for item in token_logs.values()):,}, "
                f"partial {sum(int(item.get('partial', 0)) for item in token_logs.values()):,}, "
                f"failed {sum(int(item.get('failed', 0)) for item in token_logs.values()):,}; "
                f"oldest {human_duration(max(float(item.get('oldest_age_sec', 0)) for item in token_logs.values()))}"
            )
        if aggregate:
            lines.append(
                f"Адреса: {aggregate['unique_addresses']:,} | очередь: {aggregate['balance_pending']:,}"
            )
            lines.append(
                f"Balance queue: new {aggregate['balance_new_pending']:,}, "
                f"retry {aggregate['balance_retry_pending']:,}; "
                f"completed scans {aggregate['balance_scans_total']:,}"
            )
            lines.append(
                f"≥ порога: {aggregate['qualifying']:,} | below: {aggregate['below_count']:,} | incomplete: {aggregate['incomplete']:,}"
            )
        sui = self._sui_summary()
        if sui:
            lines.append(
                f"Sui: packages {sui['packages']:,} | >= threshold {sui['qualifying']:,} | "
                f"incomplete {sui['incomplete']:,} | enrichment queue {sui['pending']:,} | "
                f"TVL snapshot {human_duration(sui['snapshot_age_sec'])} old"
                f"{' (stale)' if sui['snapshot_age_sec'] is None or sui['snapshot_age_sec'] > 1800 else ''}"
                f"{'; provider error: ' + sui['last_error'] if sui['last_error'] else ''}"
            )
        return "\n".join(lines)

    def report(self, seconds: int) -> str:
        cutoff = (datetime.now(UTC) - timedelta(seconds=seconds)).isoformat()
        aggregates = self.monitor.rows(
            "SELECT * FROM aggregate_samples WHERE ts>=? ORDER BY ts", (cutoff,)
        )
        chains = self.monitor.rows(
            """
            SELECT c.* FROM chain_samples c JOIN(
              SELECT chain,role,MAX(id) id FROM chain_samples WHERE ts>=? GROUP BY chain,role
            ) x ON x.id=c.id ORDER BY c.chain
            """, (cutoff,),
        )
        if not aggregates:
            return self.status() + "\n\nЗа выбранный период минутных срезов пока нет."
        first, last = aggregates[0], aggregates[-1]
        scan_delta = (
            f"+{max(0, last['balance_scans_total'] - first['balance_scans_total']):,}"
            if first["balance_scans_total"] and first["run_id"] == last["run_id"]
            else "n/a after restart or metrics migration"
        )
        lines = [
            f"Отчёт за {human_duration(seconds)} (МСК)", self.status(), "",
            f"Новых уникальных адресов: {max(0, last['unique_addresses'] - first['unique_addresses']):,}",
            f"Новых instances: {max(0, last['contract_instances'] - first['contract_instances']):,}",
            f"Источники: direct +{max(0, last['direct_deploy'] - first['direct_deploy']):,}, active_call +{max(0, last['active_call'] - first['active_call']):,}",
            f"Balance scans: {scan_delta}; "
            f"first-time addresses: +{max(0, last['balance_completed'] - first['balance_completed']):,}",
            f"≥ порога: +{max(0, last['qualifying'] - first['qualifying']):,}, сейчас {last['qualifying']:,}",
            f"below/incomplete: {last['below_count']:,}/{last['incomplete']:,}; очередь {last['balance_pending']:,}, oldest {human_duration(last['balance_oldest_age_sec'])}",
            "Coverage: " + ", ".join(
                f"{key}={value}" for key, value in sorted(json.loads(last['coverage_json'] or '{}').items())
            ),
            "",
            "Сети:",
        ]
        sui = self._sui_summary(cutoff)
        if sui:
            lines.extend([
                f"Sui packages: +{sui['new_packages']:,}, now {sui['packages']:,}; "
                f"publish/active {sui['publish']:,}/{sui['active_call']:,}",
                f"Sui lower-bound >= threshold/below/incomplete: "
                f"{sui['qualifying']:,}/{sui['below']:,}/{sui['incomplete']:,}; "
                f"state objects {sui['objects']:,}, enrichment queue {sui['pending']:,}",
                "",
            ])
        for row in chains:
            requests = int(row["rpc_requests"] or 0)
            errors = int(row["rpc_errors"] or 0)
            error_rate = errors / requests * 100 if requests else 0
            lines.append(
                f"• {row['chain']}: lag {row['lag'] if row['lag'] is not None else 'n/a'}, "
                f"{float(row['blocks_per_hour'] or 0):.0f} blk/h, RPC {row['active_rpc'] or 'none'}, "
                f"p95 {float(row['latency_p95_ms'] or 0):.0f} ms, err {error_rate:.1f}%"
            )
        resources = self.resources(seconds)
        if resources:
            lines.extend(["", resources])
        lines.append(f"Последний экспорт: {self._fmt_time(last['last_export_at'])}")
        backup_at = self.monitor.setting("last_backup_at", "0")
        backup_dt = datetime.fromtimestamp(float(backup_at), UTC).isoformat() if float(backup_at or 0) else None
        lines.append(f"Последний backup: {self._fmt_time(backup_dt)}")
        return "\n".join(lines)[:4000]

    def networks(self) -> str:
        runtime = self.monitor.runtime()
        try:
            runtime_note = json.loads(runtime["note"] or "{}") if runtime else {}
        except (TypeError, ValueError, json.JSONDecodeError):
            runtime_note = {}
        governor = runtime_note.get("load_governor") or {} if isinstance(runtime_note, dict) else {}
        token_logs = runtime_note.get("token_logs") or {} if isinstance(runtime_note, dict) else {}
        paused_by_governor = governor.get("state") == "balance_only"
        rows = self.monitor.rows(
            """
            SELECT c.* FROM chain_samples c JOIN(
              SELECT chain,role,MAX(id) id FROM chain_samples GROUP BY chain,role
            ) x ON x.id=c.id ORDER BY c.chain
            """
        )
        if not rows:
            return "Сетевых срезов пока нет."
        lines = ["Состояние сетей (discovery/balance):"]
        for row in rows:
            worker = self.monitor.discovery_worker(row["chain"], row["role"])
            worker_text = ""
            if worker is not None and row["role"] in {"live", "backfill"}:
                started = iso_to_dt(worker["started_at"])
                age = (datetime.now(UTC) - started).total_seconds() if started else None
                if paused_by_governor and worker["stage"] == "queued":
                    worker_text = "; worker paused_by_balance_only (range preserved)"
                else:
                    worker_text = (
                        f"; worker {worker['stage']} {worker['range_start']}-{worker['range_end']} "
                        f"age {human_duration(age)}, failures {worker['failures']}"
                        f", endpoint {worker['endpoint'] or 'none'}"
                    )
            log_state = token_logs.get(row["chain"], {}) if row["role"] == "live" else {}
            if log_state:
                worker_text += (
                    f"; token logs due {log_state.get('due', 0)}, "
                    f"partial {log_state.get('partial', 0)}, "
                    f"failed {log_state.get('failed', 0)}, "
                    f"next {log_state.get('next_block', 'n/a')}, "
                    f"oldest {human_duration(log_state.get('oldest_age_sec'))}, "
                    f"RPC {log_state.get('endpoint', 'none')}, "
                    f"errors {log_state.get('errors') or '-'}"
                )
            lines.append(
                f"• {row['chain']} [{row['role']}]: cursor {row['cursor']}, head {row['safe_head']}, lag {row['lag']}; "
                f"RPC {row['active_rpc']}; cooldown {float(row['cooldown_sec'] or 0):.0f}s; "
                f"p50/p95 {float(row['latency_p50_ms'] or 0):.0f}/{float(row['latency_p95_ms'] or 0):.0f}ms"
                f"{worker_text}"
            )
        try:
            with sqlite3.connect(self.contracts_db, timeout=5) as conn:
                conn.row_factory = sqlite3.Row
                method_rows = conn.execute(
                    """SELECT chain,hostname,method_group,cooldown_until,circuit_until,
                              permanent_error,batch_limit
                       FROM rpc_method_health
                       WHERE cooldown_until>? OR circuit_until>? OR permanent_error IS NOT NULL
                       ORDER BY chain,hostname,method_group""",
                    (time.time(), time.time()),
                ).fetchall()
            if method_rows:
                lines.append("Method cooldowns / circuits:")
                for item in method_rows:
                    remaining = max(
                        0.0, float(item["cooldown_until"] or 0),
                        float(item["circuit_until"] or 0),
                    ) - time.time()
                    lines.append(
                        f"  {item['chain']} {item['hostname']}:{item['method_group']} "
                        f"{max(0, remaining):.0f}s batch={item['batch_limit'] or '-'} "
                        f"{item['permanent_error'] or ''}"
                    )
        except sqlite3.Error:
            pass
        return "\n".join(lines)[:4000]

    def resources(self, seconds: int) -> str:
        cutoff = (datetime.now(UTC) - timedelta(seconds=seconds)).isoformat()
        rows = self.monitor.rows("SELECT * FROM resource_samples WHERE ts>=? ORDER BY ts", (cutoff,))
        if not rows:
            return "Ресурсы: данных node-exporter пока нет."
        def stats(name: str) -> tuple[float | None, float | None, float | None, float | None]:
            values = [float(row[name]) for row in rows if row[name] is not None]
            return (
                values[-1] if values else None,
                sum(values) / len(values) if values else None,
                max(values) if values else None,
                percentile(values, .95),
            )
        cpu = stats("cpu_percent")
        ram = stats("ram_percent")
        load = stats("load1")
        io = stats("io_wait_percent")
        disk = stats("disk_free_percent")
        cmem = stats("container_mem_bytes")
        latest = rows[-1]
        previous_cutoff = (datetime.now(UTC) - timedelta(seconds=seconds * 2)).isoformat()
        previous = self.monitor.rows(
            "SELECT * FROM resource_samples WHERE ts>=? AND ts<? ORDER BY ts",
            (previous_cutoff, cutoff),
        )
        def average(source: list[sqlite3.Row], name: str) -> float | None:
            values = [float(row[name]) for row in source if row[name] is not None]
            return sum(values) / len(values) if values else None
        cpu_change = metric_change(average(rows, "cpu_percent"), average(previous, "cpu_percent"))
        ram_change = metric_change(average(rows, "ram_percent"), average(previous, "ram_percent"))
        load_change = metric_change(average(rows, "load1"), average(previous, "load1"))
        return (
            "Ресурсы current/avg/peak/p95:\n"
            f"CPU: {format_stats(cpu, '%')}\n"
            f"RAM: {format_stats(ram, '%')}\n"
            f"Load1: {format_stats(load, '')}\n"
            f"I/O wait: {format_stats(io, '%')}\n"
            f"Disk free: {format_stats(disk, '%')}\n"
            f"Container RAM: {human_bytes(cmem[0])}/{human_bytes(latest['container_mem_limit'])}; "
            f"net RX/TX {human_bytes(latest['container_net_rx_bytes'])}/{human_bytes(latest['container_net_tx_bytes'])}\n"
            f"Изменение среднего к прошлому периоду: CPU {cpu_change}, RAM {ram_change}, Load {load_change}"
        )

    def errors(self, seconds: int) -> str:
        cutoff = (datetime.now(UTC) - timedelta(seconds=seconds)).isoformat()
        incidents = self.monitor.rows(
            "SELECT * FROM incidents WHERE last_seen_at>=? ORDER BY id DESC LIMIT 30", (cutoff,)
        )
        samples = self.monitor.rows("SELECT chain,active_rpc,errors_json FROM chain_samples WHERE ts>=?", (cutoff,))
        counters: dict[str, int] = defaultdict(int)
        by_network: dict[tuple[str, str, str], int] = defaultdict(int)
        for row in samples:
            try:
                for key, value in json.loads(row["errors_json"]).items():
                    counters[key] += int(value)
                    by_network[(row["chain"], row["active_rpc"] or "none", key)] += int(value)
            except (ValueError, TypeError):
                pass
        lines = [f"Ошибки за {human_duration(seconds)}:"]
        lines.append("RPC: " + (", ".join(f"{key}={value}" for key, value in sorted(counters.items())) or "нет"))
        for (chain, provider, kind), value in sorted(by_network.items(), key=lambda item: item[1], reverse=True)[:15]:
            lines.append(f"• {chain}/{provider}/{kind}: {value}")
        if incidents:
            lines.append("Инциденты:")
            for row in incidents:
                state = "recovered" if row["resolved_at"] else "OPEN"
                lines.append(f"• [{row['severity']}] {row['message']} ({state}, x{row['repeats']})")
        return "\n".join(lines)[:4000]


def format_stats(values: tuple[float | None, float | None, float | None, float | None], suffix: str) -> str:
    return "/".join("n/a" if value is None else f"{value:.1f}{suffix}" for value in values)


def metric_change(current: float | None, previous: float | None) -> str:
    if current is None or previous is None:
        return "n/a"
    return f"{current - previous:+.1f}"


class BotService:
    def __init__(self):
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        if not token:
            raise RuntimeError("TELEGRAM_BOT_TOKEN is not configured")
        self.token = token
        self.allowed_chats = {
            int(value.strip()) for value in os.getenv("TELEGRAM_CHAT_IDS", "").split(",")
            if value.strip().lstrip("-").isdigit()
        }
        self.monitor = MonitorStore(ROOT / "data" / "monitoring.db")
        self.tz = ZoneInfo(os.getenv("TELEGRAM_TIMEZONE", "Europe/Moscow"))
        self.reporter = ReportBuilder(self.monitor, ROOT / "data" / "contracts.db", self.tz)
        self.docker = DockerController(
            os.getenv("DOCKER_PROXY_URL", "http://docker-proxy:2375"),
            os.getenv("SCANNER_CONTAINER_NAME", "evm-scanner"),
        )
        self.resources = ResourceCollector(
            os.getenv("NODE_EXPORTER_URL", "http://node-exporter:9100"), self.docker
        )
        self.confirmations: dict[str, tuple[int, float, str, dict[str, Any]]] = {}
        self.background: list[asyncio.Task[Any]] = []

    def allowed(self, update: Any) -> bool:
        chat = update.effective_chat
        return bool(chat and chat.id in self.allowed_chats)

    async def guard(self, update: Any, command: str) -> bool:
        allowed = self.allowed(update)
        chat_id = str(update.effective_chat.id) if update.effective_chat else "unknown"
        user_id = str(update.effective_user.id) if update.effective_user else None
        self.monitor.audit(chat_id, user_id, command, allowed, "accepted" if allowed else "denied")
        if not allowed and update.effective_message:
            await update.effective_message.reply_text("Доступ запрещён.")
        return allowed

    async def cmd_status(self, update: Any, context: Any) -> None:
        if await self.guard(update, "/status"):
            await update.message.reply_text(self.reporter.status())

    async def cmd_report(self, update: Any, context: Any) -> None:
        if not await self.guard(update, "/report"):
            return
        try:
            seconds = parse_period(context.args[0] if context.args else "6h", ("1h", "6h", "24h", "7d"))
        except ValueError as exc:
            await update.message.reply_text(str(exc))
            return
        await update.message.reply_text(self.reporter.report(seconds))

    async def cmd_networks(self, update: Any, context: Any) -> None:
        if await self.guard(update, "/networks"):
            await update.message.reply_text(self.reporter.networks())

    async def cmd_resources(self, update: Any, context: Any) -> None:
        if not await self.guard(update, "/resources"):
            return
        try:
            seconds = parse_period(context.args[0] if context.args else "1h", ("1h", "6h", "24h"))
        except ValueError as exc:
            await update.message.reply_text(str(exc))
            return
        await update.message.reply_text(self.reporter.resources(seconds))

    async def cmd_errors(self, update: Any, context: Any) -> None:
        if not await self.guard(update, "/errors"):
            return
        try:
            seconds = parse_period(context.args[0] if context.args else "6h", ("1h", "6h", "24h"))
        except ValueError as exc:
            await update.message.reply_text(str(exc))
            return
        await update.message.reply_text(self.reporter.errors(seconds))

    async def cmd_files(self, update: Any, context: Any) -> None:
        if not await self.guard(update, "/files"):
            return
        lines = ["Отчёты:"]
        for name in (
            "qualifying.xlsx", "below_threshold.xlsx", "incomplete.xlsx",
            "sui_qualifying.xlsx", "sui_below_threshold.xlsx", "sui_incomplete.xlsx",
            "sui_packages.xlsx",
        ):
            path = ROOT / "reports" / name
            if path.exists():
                stamp = datetime.fromtimestamp(path.stat().st_mtime, self.tz).strftime("%d.%m %H:%M")
                lines.append(f"• {name}: {human_bytes(path.stat().st_size)}, {stamp}")
            else:
                lines.append(f"• {name}: отсутствует")
        await update.message.reply_text("\n".join(lines))

    async def cmd_file(self, update: Any, context: Any) -> None:
        if not await self.guard(update, "/file"):
            return
        mapping = {
            "qualifying": "qualifying.xlsx", "below": "below_threshold.xlsx",
            "incomplete": "incomplete.xlsx", "sui_qualifying": "sui_qualifying.xlsx",
            "sui_below": "sui_below_threshold.xlsx", "sui_incomplete": "sui_incomplete.xlsx",
            "sui_packages": "sui_packages.xlsx",
        }
        key = context.args[0].lower() if context.args else ""
        if key not in mapping:
            await update.message.reply_text(
                "Использование: /file qualifying|below|incomplete|"
                "sui_qualifying|sui_below|sui_incomplete|sui_packages"
            )
            return
        path = ROOT / "reports" / mapping[key]
        age = time.time() - path.stat().st_mtime if path.exists() and path.stat().st_size else None
        if age is None or age > FILE_FRESH_SEC:
            await update.message.reply_text(
                "Отчёт старше 6 часов или отсутствует; обновляю только запрошенный файл…"
            )
            request_id = self.monitor.request_export_once(str(update.effective_chat.id), key)
            completed = await self.wait_control(request_id, 300)
            if not completed:
                if not path.exists():
                    await update.message.reply_text("Экспорт ещё выполняется; готового файла пока нет.")
                    return
                await update.message.reply_text(
                    f"Экспорт ещё выполняется; отправляю предыдущий файл "
                    f"(возраст {human_duration(time.time() - path.stat().st_mtime)})."
                )
        await self.send_file(update.effective_chat.id, path, context.bot)

    async def wait_control(self, request_id: int, timeout_sec: float) -> bool:
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            row = self.monitor.control_request(request_id)
            if row is not None and row["status"] != "pending":
                return row["status"] == "complete"
            await asyncio.sleep(1)
        return False

    async def send_file(self, chat_id: int, path: Path, bot: Any) -> None:
        if not path.exists():
            await bot.send_message(chat_id, f"Файл {path.name} отсутствует.")
            return
        send_path = path
        temporary: Path | None = None
        if path.stat().st_size > TELEGRAM_SAFE_FILE_BYTES:
            handle = tempfile.NamedTemporaryFile(prefix=path.stem + "_", suffix=".zip", dir=path.parent, delete=False)
            handle.close()
            temporary = Path(handle.name)
            with zipfile.ZipFile(temporary, "w", zipfile.ZIP_DEFLATED) as archive:
                archive.write(path, path.name)
            send_path = temporary
        try:
            if send_path.stat().st_size > TELEGRAM_SAFE_FILE_BYTES:
                await bot.send_message(chat_id, f"{path.name}: {human_bytes(path.stat().st_size)}; архив превышает безопасный лимит Telegram.")
                return
            with send_path.open("rb") as stream:
                await bot.send_document(chat_id, stream, filename=send_path.name)
        finally:
            if temporary:
                temporary.unlink(missing_ok=True)

    async def cmd_schedule(self, update: Any, context: Any) -> None:
        if not await self.guard(update, "/schedule"):
            return
        if not context.args:
            seconds = int(self.monitor.setting("report_interval_sec", "21600") or 21600)
            await update.message.reply_text(f"Интервал: {human_duration(seconds)}")
            return
        try:
            seconds = parse_period(context.args[0], ("30m", "1h", "6h", "12h", "24h"))
        except ValueError as exc:
            await update.message.reply_text(str(exc))
            return
        self.monitor.set_setting("report_interval_sec", seconds)
        self.monitor.set_setting("last_scheduled_report_at", time.time())
        await update.message.reply_text(f"Новый интервал: {human_duration(seconds)}")

    async def cmd_load(self, update: Any, context: Any) -> None:
        if not await self.guard(update, "/load"):
            return
        if not context.args:
            profile = self.monitor.setting("load_profile", "conservative")
            await update.message.reply_text(f"Профиль: {profile}\n{json.dumps(LOAD_PROFILES.get(profile or 'conservative', {}), ensure_ascii=False, indent=2)}")
            return
        profile = context.args[0].lower()
        if profile not in LOAD_PROFILES:
            await update.message.reply_text("Разрешены только conservative, low, steady, normal, high.")
            return
        await self.ask_confirmation(update, "load", {"profile": profile}, f"Переключить профиль на {profile} и перезапустить scanner?")

    async def cmd_lifecycle(self, update: Any, context: Any) -> None:
        action = update.message.text.split()[0].lstrip("/").lower()
        if not await self.guard(update, f"/{action}"):
            return
        await self.ask_confirmation(update, action, {}, f"Подтвердить {action} scanner?")

    async def cmd_control(self, update: Any, context: Any) -> None:
        action = update.message.text.split()[0].lstrip("/").lower()
        if not await self.guard(update, f"/{action}"):
            return
        await self.ask_confirmation(update, action, {}, f"Подтвердить {action}?")

    async def ask_confirmation(self, update: Any, action: str, payload: dict[str, Any], text: str) -> None:
        token = uuid.uuid4().hex[:16]
        self.confirmations[token] = (update.effective_chat.id, time.time() + 60, action, payload)
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("Подтвердить", callback_data=f"confirm:{token}:yes"),
            InlineKeyboardButton("Отмена", callback_data=f"confirm:{token}:no"),
        ]])
        await update.message.reply_text(text + "\nКнопка действует 60 секунд.", reply_markup=keyboard)

    async def on_confirmation(self, update: Any, context: Any) -> None:
        query = update.callback_query
        await query.answer()
        if not self.allowed(update):
            await query.edit_message_text("Доступ запрещён.")
            return
        _, token, answer = query.data.split(":", 2)
        item = self.consume_confirmation(token, update.effective_chat.id)
        if not item:
            await query.edit_message_text("Подтверждение истекло.")
            return
        if answer != "yes":
            await query.edit_message_text("Отменено.")
            return
        action, payload = item
        try:
            if action in {"start", "stop", "restart"}:
                await self.docker.lifecycle(action)
                result = f"Команда {action} отправлена контейнеру."
            elif action == "load":
                self.monitor.set_setting("load_profile", payload["profile"])
                await self.docker.lifecycle("restart")
                result = f"Профиль {payload['profile']} сохранён; scanner перезапускается."
            elif action in {"pause", "resume", "export", "backup"}:
                request_id = self.monitor.request_control(action, str(update.effective_chat.id))
                result = f"Запрос {action} поставлен в очередь (#{request_id})."
            else:
                raise ValueError("unsupported action")
            await query.edit_message_text(result)
        except Exception as exc:
            await query.edit_message_text(f"Не удалось выполнить {action}: {type(exc).__name__}")

    def consume_confirmation(self, token: str, chat_id: int) -> tuple[str, dict[str, Any]] | None:
        item = self.confirmations.pop(token, None)
        if not item or item[0] != chat_id or item[1] < time.time():
            return None
        return item[2], item[3]

    async def cmd_history(self, update: Any, context: Any) -> None:
        if not await self.guard(update, "/history"):
            return
        rows = self.monitor.rows("SELECT * FROM command_audit ORDER BY id DESC LIMIT 20")
        text = "Последние команды:\n" + "\n".join(
            f"• {row['ts'][:19]} {row['chat_id']} {row['command']} {'ok' if row['allowed'] else 'denied'}"
            for row in rows
        )
        await update.message.reply_text(text[:4000])

    async def cmd_help(self, update: Any, context: Any) -> None:
        if not await self.guard(update, "/help"):
            return
        await update.message.reply_text(
            "/status\n/report 1h|6h|24h|7d\n/networks\n/resources 1h|6h|24h\n"
            "/errors 1h|6h|24h\n/files\n"
            "/file qualifying|below|incomplete|sui_qualifying|sui_below|sui_incomplete|sui_packages\n"
            "/schedule 30m|1h|6h|12h|24h\n/load [conservative|low|steady|normal|high]\n"
            "/start /stop /restart\n/pause /resume\n/export /backup\n/history"
        )

    async def scheduled_reports_loop(self, application: Any) -> None:
        while True:
            interval = int(self.monitor.setting("report_interval_sec", "21600") or 21600)
            last = float(self.monitor.setting("last_scheduled_report_at", "0") or 0)
            if time.time() - last >= interval:
                export_id = self.monitor.request_control(
                    "export_qualifying", "scheduled-report"
                )
                fresh = await self.wait_control(export_id, 60)
                for chat_id in self.allowed_chats:
                    try:
                        await application.bot.send_message(chat_id, self.reporter.report(interval))
                        if not fresh:
                            path = ROOT / "reports" / "qualifying.xlsx"
                            age = time.time() - path.stat().st_mtime if path.exists() else None
                            await application.bot.send_message(
                                chat_id,
                                "Qualifying-export не завершился за 60 секунд; "
                                f"отправляю предыдущий файл (возраст {human_duration(age)}).",
                            )
                        await self.send_file(chat_id, ROOT / "reports" / "qualifying.xlsx", application.bot)
                        sui_report = ROOT / "reports" / "sui_qualifying.xlsx"
                        if sui_report.exists():
                            await self.send_file(chat_id, sui_report, application.bot)
                        self.monitor.record_delivery(str(chat_id), "scheduled_report", "sent")
                    except Exception as exc:
                        self.monitor.record_delivery(str(chat_id), "scheduled_report", "failed", error=type(exc).__name__)
                self.monitor.set_setting("last_scheduled_report_at", time.time())
            await asyncio.sleep(15)

    async def resource_loop(self) -> None:
        while True:
            try:
                self.monitor.add_resource_sample(await self.resources.collect())
                self.monitor.resolve_incident("monitor:resources_unavailable")
            except Exception as exc:
                self.monitor.open_incident(
                    "monitor:resources_unavailable", "warning", "monitoring_failure",
                    f"System metrics unavailable: {type(exc).__name__}",
                )
            await asyncio.sleep(15)

    def _condition(self, active: bool, fingerprint: str, severity: str, kind: str, message: str) -> None:
        if active:
            self.monitor.open_incident(fingerprint, severity, kind, message)
        else:
            self.monitor.resolve_incident(fingerprint)

    def _evaluate_chain_incidents(self, now: datetime) -> None:
        fifteen = (now - timedelta(minutes=15)).isoformat()
        runtime = self.monitor.runtime()
        try:
            runtime_note = json.loads(runtime["note"] or "{}") if runtime is not None else {}
        except (TypeError, ValueError, json.JSONDecodeError):
            runtime_note = {}
        if not isinstance(runtime_note, dict):
            runtime_note = {}
        governor_note = runtime_note.get("load_governor")
        discovery_held = isinstance(governor_note, dict) and governor_note.get("state") == "balance_only"
        latest_chains = self.monitor.rows(
            "SELECT chain,run_id,active_rpc,cooldown_sec,cursor,safe_head,ts FROM chain_samples WHERE id IN "
            "(SELECT MAX(id) FROM chain_samples WHERE role IN ('live','balance','discovery+balance') GROUP BY chain)"
        )
        for row in latest_chains:
            down_samples = self.monitor.rows(
                "SELECT COUNT(*) n,MIN(ts) first_ts,MAX(ts) last_ts FROM chain_samples "
                "WHERE chain=? AND run_id IS ? AND ts>=? AND active_rpc='none'",
                (row["chain"], row["run_id"], (now - timedelta(minutes=2)).isoformat()),
            )[0]
            down_span = timestamp_span(down_samples["first_ts"], down_samples["last_ts"])
            self._condition(
                down_samples["n"] >= 2 and down_span >= 100,
                f"rpc:down:{row['chain']}", "critical", "rpc_outage",
                f"All RPC endpoints for {row['chain']} are unavailable",
            )

            fingerprint = f"cursor:stalled:{row['chain']}"
            opened = self.monitor.active_incident(fingerprint)
            latest_cursor = int(row["cursor"]) if row["cursor"] is not None else None
            if opened is not None:
                try:
                    details = json.loads(opened["details_json"] or "{}")
                except (TypeError, ValueError, json.JSONDecodeError):
                    details = {}
                if not isinstance(details, dict):
                    details = {}
                opened_cursor = details.get("opened_cursor")
                if opened_cursor is None and latest_cursor is not None:
                    opened_cursor = latest_cursor
                    details["opened_cursor"] = opened_cursor
                if (
                    latest_cursor is not None
                    and opened_cursor is not None
                    and latest_cursor > int(opened_cursor)
                ):
                    self.monitor.resolve_incident(fingerprint)
                else:
                    # Losing the RPC or a head that stopped advancing is not a
                    # recovery.  Keep the incident open until cursor advances.
                    self.monitor.open_incident(
                        fingerprint, "critical", "cursor_stall",
                        f"Cursor for {row['chain']} did not move for 15 minutes while head advanced",
                        details=details,
                    )
                continue

            # Compare against the last sample at least fifteen minutes old.
            # A monotonic cursor equal to that baseline has not moved during
            # the full interval; this also prevents startup false positives.
            baseline_rows = self.monitor.rows(
                "SELECT cursor,safe_head,ts FROM chain_samples "
                "WHERE chain=? AND run_id IS ? "
                "AND role IN ('live','discovery+balance') AND ts<=? "
                "ORDER BY id DESC LIMIT 1",
                (row["chain"], row["run_id"], fifteen),
            )
            baseline = baseline_rows[0] if baseline_rows else None
            stalled = bool(
                row["active_rpc"] not in (None, "", "none")
                and baseline is not None
                and latest_cursor is not None
                and baseline["cursor"] is not None
                and latest_cursor == int(baseline["cursor"])
                and int(row["safe_head"] or 0) > int(baseline["safe_head"] or 0)
                and timestamp_span(baseline["ts"], row["ts"]) >= 900
            )
            # In steady critical-drain mode a network can be intentionally
            # waiting for a fair live slot.  That is backlog pressure, not a
            # broken RPC/indexer; opening a critical cursor incident here only
            # creates noise.  A worker that owns a range remains eligible for
            # the watchdog/stall alert.
            worker = self.monitor.discovery_worker(row["chain"], "live")
            waiting_for_slot = worker is not None and worker["stage"] == "queued"
            stalled = stalled and not waiting_for_slot and not discovery_held
            if stalled:
                worker_details = dict(worker) if worker is not None else {}
                self.monitor.open_incident(
                    fingerprint, "critical", "cursor_stall",
                    f"Cursor for {row['chain']} did not move for 15 minutes while head advanced",
                    details={"opened_cursor": latest_cursor, "worker": worker_details},
                )

    async def evaluate_incidents(self) -> None:
        runtime = self.monitor.runtime()
        stale = True
        if runtime:
            heartbeat = iso_to_dt(runtime["heartbeat_at"])
            stale = heartbeat is None or (datetime.now(UTC) - heartbeat).total_seconds() > 120
        self._condition(stale, "scanner:heartbeat", "critical", "stale_heartbeat", "Scanner heartbeat is older than 2 minutes")
        try:
            inspected = await self.docker.inspect()
            container_running = bool(inspected.get("State", {}).get("Running"))
            self._condition(not container_running, "scanner:container_down", "critical", "container_down", "Scanner container is stopped")
        except Exception as exc:
            self._condition(True, "monitor:docker_unavailable", "warning", "monitoring_failure", f"Docker proxy unavailable: {type(exc).__name__}")
        else:
            self.monitor.resolve_incident("monitor:docker_unavailable")

        now = datetime.now(UTC)
        migrating = bool(runtime and runtime["state"] == "migrating")
        five = (now - timedelta(minutes=5)).isoformat()
        ten = (now - timedelta(minutes=10)).isoformat()
        rpc_rows = self.monitor.rows(
            "SELECT SUM(rpc_requests) req,SUM(rpc_errors) err FROM chain_samples WHERE ts>=?", (five,)
        )
        req = int(rpc_rows[0]["req"] or 0) if rpc_rows else 0
        err = int(rpc_rows[0]["err"] or 0) if rpc_rows else 0
        self._condition(req >= 50 and err / req > .20, "rpc:error_rate", "critical", "rpc_error_rate",
                        f"RPC error rate is {err / req:.1%} ({err}/{req})" if req else "RPC error rate high")

        if not migrating:
            self._evaluate_chain_incidents(now)

        balances = self.monitor.rows(
            "SELECT MIN(balance_scans_total) min_done,MAX(balance_scans_total) max_done,MAX(balance_pending) pending,"
            "MIN(ts) first_ts,MAX(ts) last_ts "
            "FROM aggregate_samples WHERE ts>=?", (ten,),
        )[0]
        balance_stalled = balance_scans_stalled(balances)
        if not migrating:
            self._condition(balance_stalled, "balance:stalled", "critical", "balance_stall",
                            "Balance queue is non-empty and no balance scan completed for 10 minutes")

        restart_rows = self.monitor.rows(
            "SELECT MIN(restart_count) first_count,MAX(restart_count) last_count "
            "FROM resource_samples WHERE ts>=?", (ten,),
        )[0]
        restart_delta = int(restart_rows["last_count"] or 0) - int(restart_rows["first_count"] or 0)
        self._condition(restart_delta >= 3, "scanner:restart_loop", "critical", "restart_loop",
                        f"Scanner container restarted {restart_delta} times in 10 minutes")

        await self._evaluate_resource_thresholds(now)
        aggregates = self.monitor.rows(
            "SELECT wal_bytes,db_bytes FROM aggregate_samples WHERE ts>=? ORDER BY ts",
            ((now - timedelta(minutes=10)).isoformat(),),
        )
        wal_runaway = False
        if len(aggregates) >= 2:
            first_wal = int(aggregates[0]["wal_bytes"] or 0)
            last_wal = int(aggregates[-1]["wal_bytes"] or 0)
            db_size = int(aggregates[-1]["db_bytes"] or 0)
            wal_runaway = last_wal > 1024 ** 3 and last_wal > max(first_wal * 2, db_size)
        self._condition(wal_runaway, "database:wal_growth", "critical", "database_growth", "SQLite WAL grew sharply and exceeds 1 GiB")

    async def _evaluate_resource_thresholds(self, now: datetime) -> None:
        def cpu_state(
            fingerprint: str, threshold: float, minutes: int,
            recovery_threshold: float, severity: str, message: str,
        ) -> bool:
            opened = self.monitor.active_incident(fingerprint)
            if opened is None:
                cutoff = (now - timedelta(minutes=minutes)).isoformat()
                samples = self.monitor.rows(
                    "SELECT cpu_percent value,ts FROM resource_samples "
                    "WHERE ts>=? AND cpu_percent IS NOT NULL ORDER BY ts", (cutoff,),
                )
                span = timestamp_span(samples[0]["ts"], samples[-1]["ts"]) if samples else 0
                if span >= minutes * 60 - 20 and all(
                    float(sample["value"]) > threshold for sample in samples
                ):
                    self.monitor.open_incident(
                        fingerprint, severity, "system_load", message,
                    )
                    return True
                return False
            recovery_cutoff = (now - timedelta(minutes=2)).isoformat()
            recovery = self.monitor.rows(
                "SELECT cpu_percent value,ts FROM resource_samples "
                "WHERE ts>=? AND cpu_percent IS NOT NULL ORDER BY ts", (recovery_cutoff,),
            )
            span = timestamp_span(recovery[0]["ts"], recovery[-1]["ts"]) if recovery else 0
            if span >= 100 and all(
                float(sample["value"]) < recovery_threshold for sample in recovery
            ):
                self.monitor.resolve_incident(fingerprint)
                return False
            return True

        critical_cpu = cpu_state(
            "system:cpu:critical", 95, 5, 85, "critical",
            "CPU above 95% for 5 minutes",
        )
        if not critical_cpu:
            cpu_state(
                "system:cpu:warning", 80, 10, 70, "warning",
                "CPU above 80% for 10 minutes",
            )
        rules = [
            ("ram_percent", 80, 5, "warning", "RAM above 80% for 5 minutes", "system:ram:warning", False),
            ("ram_percent", 92, 2, "critical", "RAM above 92% for 2 minutes", "system:ram:critical", False),
            ("io_wait_percent", 20, 5, "warning", "I/O wait above 20% for 5 minutes", "system:iowait", False),
            ("disk_free_percent", 15, 0, "warning", "Disk free below 15%", "system:disk:warning", True),
            ("disk_free_percent", 5, 0, "critical", "Disk free below 5%", "system:disk:critical", True),
        ]
        for column, threshold, minutes, severity, message, fingerprint, lower in rules:
            cutoff = (now - timedelta(minutes=minutes)).isoformat()
            if minutes == 0:
                rows = self.monitor.rows(
                    f"SELECT {column} value,ts FROM resource_samples WHERE {column} IS NOT NULL ORDER BY id DESC LIMIT 1"
                )
            else:
                rows = self.monitor.rows(
                    f"SELECT {column} value,ts FROM resource_samples WHERE ts>=? AND {column} IS NOT NULL ORDER BY ts",
                    (cutoff,),
                )
            values = [float(row["value"]) for row in rows]
            span = timestamp_span(rows[0]["ts"], rows[-1]["ts"]) if rows else 0
            active = bool(values) and (minutes == 0 or span >= minutes * 60 - 20) and all(
                value < threshold if lower else value > threshold for value in values
            )
            self._condition(active, fingerprint, severity, "system_load", message)
        rows = self.monitor.rows("SELECT * FROM resource_samples WHERE ts>=? ORDER BY ts", ((now - timedelta(minutes=10)).isoformat(),))
        if rows:
            span = timestamp_span(rows[0]["ts"], rows[-1]["ts"])
            load_active = span >= 580 and all(float(row["load1"] or 0) > 1.5 * float(row["cpu_cores"] or 1) for row in rows)
            self._condition(load_active, "system:load:warning", "warning", "system_load", "Load average above 1.5 × CPU cores for 10 minutes")
            recent = [row for row in rows if iso_to_dt(row["ts"]) and iso_to_dt(row["ts"]) >= now - timedelta(minutes=5)]
            recent_span = timestamp_span(recent[0]["ts"], recent[-1]["ts"]) if recent else 0
            load_critical = recent_span >= 280 and all(float(row["load1"] or 0) > 2 * float(row["cpu_cores"] or 1) for row in recent)
            self._condition(load_critical, "system:load:critical", "critical", "system_load", "Load average above 2 × CPU cores for 5 minutes")
            cmem = [100 * float(row["container_mem_bytes"] or 0) / float(row["container_mem_limit"] or 1) for row in recent]
            self._condition(recent_span >= 280 and all(value > 85 for value in cmem), "container:memory", "warning", "container_load", "Scanner container memory above 85% for 5 minutes")

    async def incident_loop(self, application: Any) -> None:
        while True:
            try:
                await self.evaluate_incidents()
                for row in self.monitor.pending_incident_notifications():
                    recovery = row["resolved_at"] is not None
                    opened = iso_to_dt(row["opened_at"])
                    resolved = iso_to_dt(row["resolved_at"])
                    duration = (resolved - opened).total_seconds() if opened and resolved else None
                    prefix = "✅ RECOVERY" if recovery else ("🚨 CRITICAL" if row["severity"] == "critical" else "⚠️ WARNING")
                    text = f"{prefix}\n{row['message']}\nПовторов: {row['repeats']}"
                    if recovery:
                        text += f"\nДлительность: {human_duration(duration)}"
                    if not recovery and row["kind"] in {"system_load", "container_load"}:
                        text += "\nРекомендация: /load conservative (потребуется подтверждение)."
                    delivered = False
                    for chat_id in self.allowed_chats:
                        try:
                            markup = None
                            if not recovery and row["kind"] in {"system_load", "container_load"}:
                                markup = InlineKeyboardMarkup([[
                                    InlineKeyboardButton("Переключить на CONSERVATIVE", callback_data="suggest:conservative")
                                ]])
                            await application.bot.send_message(chat_id, text, reply_markup=markup)
                            self.monitor.record_delivery(str(chat_id), "recovery" if recovery else "incident", "sent", str(row["id"]))
                            delivered = True
                        except Exception as exc:
                            self.monitor.record_delivery(str(chat_id), "incident", "failed", str(row["id"]), type(exc).__name__)
                    if delivered:
                        self.monitor.mark_incident_notified(int(row["id"]), recovery)
            except Exception:
                log.exception("incident evaluation failed")
            await asyncio.sleep(10)

    async def post_init(self, application: Any) -> None:
        self.background = [
            asyncio.create_task(self.scheduled_reports_loop(application), name="scheduled-reports"),
            asyncio.create_task(self.resource_loop(), name="resource-collector"),
            asyncio.create_task(self.incident_loop(application), name="incident-monitor"),
        ]

    async def on_suggest_low(self, update: Any, context: Any) -> None:
        query = update.callback_query
        await query.answer()
        if not self.allowed(update):
            return
        token = uuid.uuid4().hex[:16]
        self.confirmations[token] = (update.effective_chat.id, time.time() + 60, "load", {"profile": "conservative"})
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("Подтвердить LOW", callback_data=f"confirm:{token}:yes"),
            InlineKeyboardButton("Отмена", callback_data=f"confirm:{token}:no"),
        ]])
        await query.message.reply_text("Подтвердить профиль LOW и graceful restart?", reply_markup=keyboard)

    async def post_shutdown(self, application: Any) -> None:
        for task in self.background:
            task.cancel()
        await asyncio.gather(*self.background, return_exceptions=True)
        await self.resources.close()
        await self.docker.close()
        self.monitor.close()

    def build(self) -> Any:
        if Application is None:
            raise RuntimeError("Install python-telegram-bot from requirements.txt")
        app = Application.builder().token(self.token).post_init(self.post_init).post_shutdown(self.post_shutdown).build()
        app.add_handler(CommandHandler("status", self.cmd_status))
        app.add_handler(CommandHandler("report", self.cmd_report))
        app.add_handler(CommandHandler("networks", self.cmd_networks))
        app.add_handler(CommandHandler("resources", self.cmd_resources))
        app.add_handler(CommandHandler("errors", self.cmd_errors))
        app.add_handler(CommandHandler("files", self.cmd_files))
        app.add_handler(CommandHandler("file", self.cmd_file))
        app.add_handler(CommandHandler("schedule", self.cmd_schedule))
        app.add_handler(CommandHandler("load", self.cmd_load))
        for command in ("start", "stop", "restart"):
            app.add_handler(CommandHandler(command, self.cmd_lifecycle))
        for command in ("pause", "resume", "export", "backup"):
            app.add_handler(CommandHandler(command, self.cmd_control))
        app.add_handler(CommandHandler("history", self.cmd_history))
        app.add_handler(CommandHandler("help", self.cmd_help))
        app.add_handler(CallbackQueryHandler(self.on_confirmation, pattern=r"^confirm:"))
        app.add_handler(CallbackQueryHandler(self.on_suggest_low, pattern=r"^suggest:(low|conservative)$"))
        return app


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    service = BotService()
    if not service.allowed_chats:
        raise SystemExit("TELEGRAM_CHAT_IDS is empty; refusing to start without a whitelist")
    service.build().run_polling(allowed_updates=Update.ALL_TYPES if "Update" in globals() else None)


if __name__ == "__main__":
    main()
