from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


UTC = timezone.utc


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


LOAD_PROFILES: dict[str, dict[str, int]] = {
    "conservative": {
        "global_rpc_concurrency": 6,
        "rpc_concurrency": 1,
        "balance_concurrency": 2,
        "balance_chain_concurrency": 4,
        "block_batch_size": 4,
        "receipt_batch_size": 10,
        "price_batch_size": 20,
        "discovery_live_slots": 4,
        "discovery_backfill_slots": 1,
    },
    "low": {
        "global_rpc_concurrency": 6,
        "rpc_concurrency": 1,
        "balance_concurrency": 2,
        "balance_chain_concurrency": 4,
        "block_batch_size": 4,
        "receipt_batch_size": 10,
        "price_batch_size": 20,
        "discovery_live_slots": 4,
        "discovery_backfill_slots": 1,
    },
    "steady": {
        "global_rpc_concurrency": 10,
        "discovery_rpc_concurrency": 4,
        "balance_rpc_concurrency": 6,
        "rpc_concurrency": 2,
        "balance_concurrency": 6,
        "balance_chain_concurrency": 10,
        "block_batch_size": 6,
        "receipt_batch_size": 15,
        "price_batch_size": 30,
        "discovery_live_slots": 4,
        "discovery_backfill_slots": 1,
    },
    "normal": {
        "global_rpc_concurrency": 12,
        "rpc_concurrency": 2,
        "balance_concurrency": 8,
        "balance_chain_concurrency": 12,
        "block_batch_size": 8,
        "receipt_batch_size": 20,
        "price_batch_size": 40,
        "discovery_live_slots": 6,
        "discovery_backfill_slots": 1,
    },
    "high": {
        "global_rpc_concurrency": 20,
        "rpc_concurrency": 3,
        "balance_concurrency": 12,
        "balance_chain_concurrency": 20,
        "block_batch_size": 12,
        "receipt_batch_size": 30,
        "price_batch_size": 60,
        "discovery_live_slots": 10,
        "discovery_backfill_slots": 2,
    },
}

for _profile in LOAD_PROFILES.values():
    _total = _profile["global_rpc_concurrency"]
    _profile.setdefault("discovery_rpc_concurrency", max(1, _total // 2))
    _profile.setdefault(
        "balance_rpc_concurrency", _total - _profile["discovery_rpc_concurrency"]
    )


MONITORING_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS schema_meta(version INTEGER NOT NULL);

CREATE TABLE IF NOT EXISTS runtime_state(
    id INTEGER PRIMARY KEY CHECK(id=1),
    run_id TEXT,
    state TEXT NOT NULL DEFAULT 'stopped',
    started_at TEXT,
    heartbeat_at TEXT,
    stopped_at TEXT,
    mode TEXT,
    min_usd REAL,
    load_profile TEXT NOT NULL DEFAULT 'conservative',
    pid INTEGER,
    args_json TEXT NOT NULL DEFAULT '{}',
    note TEXT
);

CREATE TABLE IF NOT EXISTS chain_samples(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    run_id TEXT,
    chain TEXT NOT NULL,
    role TEXT NOT NULL,
    cursor INTEGER,
    safe_head INTEGER,
    lag INTEGER,
    blocks_per_hour REAL,
    contracts INTEGER NOT NULL DEFAULT 0,
    direct_deploy INTEGER NOT NULL DEFAULT 0,
    active_call INTEGER NOT NULL DEFAULT 0,
    active_rpc TEXT,
    cooldown_sec REAL NOT NULL DEFAULT 0,
    rpc_requests INTEGER NOT NULL DEFAULT 0,
    rpc_successes INTEGER NOT NULL DEFAULT 0,
    rpc_errors INTEGER NOT NULL DEFAULT 0,
    latency_p50_ms REAL,
    latency_p95_ms REAL,
    errors_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_chain_samples_time ON chain_samples(chain, ts);

CREATE TABLE IF NOT EXISTS aggregate_samples(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    run_id TEXT,
    unique_addresses INTEGER NOT NULL DEFAULT 0,
    contract_instances INTEGER NOT NULL DEFAULT 0,
    direct_deploy INTEGER NOT NULL DEFAULT 0,
    active_call INTEGER NOT NULL DEFAULT 0,
    balance_pending INTEGER NOT NULL DEFAULT 0,
    balance_new_pending INTEGER NOT NULL DEFAULT 0,
    balance_retry_pending INTEGER NOT NULL DEFAULT 0,
    balance_oldest_age_sec REAL,
    balance_completed INTEGER NOT NULL DEFAULT 0,
    balance_scans_total INTEGER NOT NULL DEFAULT 0,
    qualifying INTEGER NOT NULL DEFAULT 0,
    below_count INTEGER NOT NULL DEFAULT 0,
    incomplete INTEGER NOT NULL DEFAULT 0,
    coverage_json TEXT NOT NULL DEFAULT '{}',
    db_bytes INTEGER NOT NULL DEFAULT 0,
    wal_bytes INTEGER NOT NULL DEFAULT 0,
    reports_json TEXT NOT NULL DEFAULT '{}',
    last_export_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_aggregate_samples_time ON aggregate_samples(ts);

CREATE TABLE IF NOT EXISTS resource_samples(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    cpu_percent REAL,
    load1 REAL,
    cpu_cores REAL,
    ram_percent REAL,
    swap_percent REAL,
    disk_free_percent REAL,
    io_wait_percent REAL,
    net_rx_bytes INTEGER,
    net_tx_bytes INTEGER,
    container_cpu_percent REAL,
    container_mem_bytes INTEGER,
    container_mem_limit INTEGER,
    container_net_rx_bytes INTEGER,
    container_net_tx_bytes INTEGER,
    container_block_read_bytes INTEGER,
    container_block_write_bytes INTEGER,
    container_pids INTEGER,
    restart_count INTEGER,
    details_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_resource_samples_time ON resource_samples(ts);

CREATE TABLE IF NOT EXISTS incidents(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint TEXT NOT NULL,
    severity TEXT NOT NULL,
    kind TEXT NOT NULL,
    scope TEXT,
    message TEXT NOT NULL,
    opened_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    repeats INTEGER NOT NULL DEFAULT 1,
    resolved_at TEXT,
    open_notified_at TEXT,
    recovery_notified_at TEXT,
    details_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_incidents_open ON incidents(fingerprint, resolved_at);

CREATE TABLE IF NOT EXISTS settings(
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS control_requests(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    requested_by TEXT,
    requested_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    handled_at TEXT,
    result TEXT
);
CREATE INDEX IF NOT EXISTS idx_control_pending ON control_requests(status, id);

CREATE TABLE IF NOT EXISTS command_audit(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    user_id TEXT,
    command TEXT NOT NULL,
    allowed INTEGER NOT NULL,
    result TEXT
);

CREATE TABLE IF NOT EXISTS deliveries(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    reference_id TEXT,
    status TEXT NOT NULL,
    error TEXT
);

CREATE TABLE IF NOT EXISTS discovery_workers(
    worker_key TEXT PRIMARY KEY,
    chain TEXT NOT NULL,
    role TEXT NOT NULL,
    stage TEXT NOT NULL,
    range_start INTEGER,
    range_end INTEGER,
    started_at TEXT,
    updated_at TEXT NOT NULL,
    failures INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    endpoint TEXT
);
"""


@dataclass(frozen=True)
class ControlRequest:
    id: int
    action: str
    payload: dict[str, Any]
    requested_by: str | None


class MonitorStore:
    """Small, independent SQLite store shared by scanner and monitoring bot."""

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(MONITORING_SCHEMA)
        aggregate_columns = {
            row["name"] for row in self.conn.execute("PRAGMA table_info(aggregate_samples)")
        }
        if "balance_oldest_age_sec" not in aggregate_columns:
            self.conn.execute("ALTER TABLE aggregate_samples ADD COLUMN balance_oldest_age_sec REAL")
        for name in ("balance_new_pending", "balance_retry_pending", "balance_scans_total"):
            if name not in aggregate_columns:
                self.conn.execute(
                    f"ALTER TABLE aggregate_samples ADD COLUMN {name} INTEGER NOT NULL DEFAULT 0"
                )
        worker_columns = {
            row["name"] for row in self.conn.execute("PRAGMA table_info(discovery_workers)")
        }
        if "endpoint" not in worker_columns:
            self.conn.execute("ALTER TABLE discovery_workers ADD COLUMN endpoint TEXT")
        with self.conn:
            previous_version_row = self.conn.execute(
                "SELECT version FROM schema_meta LIMIT 1"
            ).fetchone()
            previous_version = int(previous_version_row[0]) if previous_version_row else 0
            self.conn.execute(
                "INSERT INTO schema_meta(version) SELECT 2 WHERE NOT EXISTS "
                "(SELECT 1 FROM schema_meta)"
            )
            self.conn.execute(
                "INSERT OR IGNORE INTO settings(key,value,updated_at) VALUES(?,?,?)",
                ("report_interval_sec", "21600", utc_now()),
            )
            self.conn.execute(
                "INSERT OR IGNORE INTO settings(key,value,updated_at) VALUES(?,?,?)",
                ("load_profile", "conservative", utc_now()),
            )
            if previous_version < 2:
                self.conn.execute(
                    "UPDATE settings SET value='conservative',updated_at=? "
                    "WHERE key='load_profile' AND value IN ('low','normal')",
                    (utc_now(),),
                )
                self.conn.execute("UPDATE schema_meta SET version=2 WHERE version<2")
            self.conn.execute("UPDATE schema_meta SET version=3 WHERE version<3")
            self.conn.execute(
                "INSERT OR IGNORE INTO settings(key,value,updated_at) VALUES(?,?,?)",
                ("scanner_paused", "0", utc_now()),
            )
            self.conn.execute(
                "INSERT OR IGNORE INTO settings(key,value,updated_at) VALUES(?,?,?)",
                ("last_scheduled_report_at", str(time.time()), utc_now()),
            )

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    def setting(self, key: str, default: str | None = None) -> str | None:
        with self._lock:
            row = self.conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return str(row["value"]) if row else default

    def set_setting(self, key: str, value: Any) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                "INSERT INTO settings(key,value,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
                (key, str(value), utc_now()),
            )

    def begin_run(self, mode: str, min_usd: float, profile: str, args: dict[str, Any]) -> str:
        run_id = str(uuid.uuid4())
        now = utc_now()
        with self._lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO runtime_state(id,run_id,state,started_at,heartbeat_at,stopped_at,
                                          mode,min_usd,load_profile,pid,args_json,note)
                VALUES(1,?,?,?,?,?,?,?,?,?,?,NULL)
                ON CONFLICT(id) DO UPDATE SET
                    run_id=excluded.run_id,state=excluded.state,started_at=excluded.started_at,
                    heartbeat_at=excluded.heartbeat_at,stopped_at=NULL,mode=excluded.mode,
                    min_usd=excluded.min_usd,load_profile=excluded.load_profile,pid=excluded.pid,
                    args_json=excluded.args_json,note=NULL
                """,
                (run_id, "running", now, now, None, mode, min_usd, profile,
                 __import__("os").getpid(), json.dumps(args, ensure_ascii=False, default=str)),
            )
        return run_id

    def heartbeat(self, run_id: str, state: str = "running", note: str | None = None) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE runtime_state SET state=?,heartbeat_at=?,note=? WHERE id=1 AND run_id=?",
                (state, utc_now(), note, run_id),
            )

    def set_runtime_state(self, state: str, note: str | None = None) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE runtime_state SET state=?,heartbeat_at=?,note=? WHERE id=1",
                (state, utc_now(), note),
            )

    def finish_run(self, run_id: str, state: str = "stopped", note: str | None = None) -> None:
        now = utc_now()
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE runtime_state SET state=?,heartbeat_at=?,stopped_at=?,note=? "
                "WHERE id=1 AND run_id=?",
                (state, now, now, note, run_id),
            )

    def runtime(self) -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute("SELECT * FROM runtime_state WHERE id=1").fetchone()

    def add_chain_sample(self, **values: Any) -> None:
        columns = [
            "ts", "run_id", "chain", "role", "cursor", "safe_head", "lag",
            "blocks_per_hour", "contracts", "direct_deploy", "active_call",
            "active_rpc", "cooldown_sec", "rpc_requests", "rpc_successes",
            "rpc_errors", "latency_p50_ms", "latency_p95_ms", "errors_json",
        ]
        values.setdefault("ts", utc_now())
        values["errors_json"] = json.dumps(values.get("errors_json", {}), ensure_ascii=False)
        with self._lock, self.conn:
            self.conn.execute(
                f"INSERT INTO chain_samples({','.join(columns)}) VALUES({','.join('?' for _ in columns)})",
                tuple(values.get(column) for column in columns),
            )

    def add_aggregate_sample(self, **values: Any) -> None:
        columns = [
            "ts", "run_id", "unique_addresses", "contract_instances", "direct_deploy",
            "active_call", "balance_pending", "balance_completed", "qualifying",
            "balance_new_pending", "balance_retry_pending", "balance_scans_total",
            "balance_oldest_age_sec",
            "below_count", "incomplete", "coverage_json", "db_bytes", "wal_bytes",
            "reports_json", "last_export_at",
        ]
        values.setdefault("ts", utc_now())
        for name in ("balance_new_pending", "balance_retry_pending", "balance_scans_total"):
            values.setdefault(name, 0)
        for key in ("coverage_json", "reports_json"):
            values[key] = json.dumps(values.get(key, {}), ensure_ascii=False)
        with self._lock, self.conn:
            self.conn.execute(
                f"INSERT INTO aggregate_samples({','.join(columns)}) VALUES({','.join('?' for _ in columns)})",
                tuple(values.get(column) for column in columns),
            )

    def add_resource_sample(self, values: dict[str, Any]) -> None:
        allowed = {
            "ts", "cpu_percent", "load1", "cpu_cores", "ram_percent", "swap_percent",
            "disk_free_percent", "io_wait_percent", "net_rx_bytes", "net_tx_bytes",
            "container_cpu_percent", "container_mem_bytes", "container_mem_limit",
            "container_net_rx_bytes", "container_net_tx_bytes", "container_block_read_bytes",
            "container_block_write_bytes", "container_pids", "restart_count", "details_json",
        }
        clean = {key: value for key, value in values.items() if key in allowed}
        clean.setdefault("ts", utc_now())
        clean["details_json"] = json.dumps(clean.get("details_json", {}), ensure_ascii=False)
        columns = list(clean)
        with self._lock, self.conn:
            self.conn.execute(
                f"INSERT INTO resource_samples({','.join(columns)}) VALUES({','.join('?' for _ in columns)})",
                tuple(clean[column] for column in columns),
            )

    def open_incident(
        self, fingerprint: str, severity: str, kind: str, message: str,
        scope: str | None = None, details: dict[str, Any] | None = None,
    ) -> tuple[int, bool]:
        now = utc_now()
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT id FROM incidents WHERE fingerprint=? AND resolved_at IS NULL "
                "ORDER BY id DESC LIMIT 1", (fingerprint,),
            ).fetchone()
            if row:
                self.conn.execute(
                    "UPDATE incidents SET last_seen_at=?,repeats=repeats+1,message=?,details_json=? WHERE id=?",
                    (now, message, json.dumps(details or {}, ensure_ascii=False), row["id"]),
                )
                return int(row["id"]), False
            cur = self.conn.execute(
                "INSERT INTO incidents(fingerprint,severity,kind,scope,message,opened_at,last_seen_at,details_json) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (fingerprint, severity, kind, scope, message, now, now,
                 json.dumps(details or {}, ensure_ascii=False)),
            )
            return int(cur.lastrowid), True

    def resolve_incident(self, fingerprint: str) -> int | None:
        now = utc_now()
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT id FROM incidents WHERE fingerprint=? AND resolved_at IS NULL "
                "ORDER BY id DESC LIMIT 1", (fingerprint,),
            ).fetchone()
            if not row:
                return None
            self.conn.execute("UPDATE incidents SET resolved_at=? WHERE id=?", (now, row["id"]))
            return int(row["id"])

    def active_incident(self, fingerprint: str) -> sqlite3.Row | None:
        """Return the current incident so recovery can use its opening state."""
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM incidents WHERE fingerprint=? AND resolved_at IS NULL "
                "ORDER BY id DESC LIMIT 1",
                (fingerprint,),
            ).fetchone()

    def pending_incident_notifications(self) -> list[sqlite3.Row]:
        with self._lock:
            return list(self.conn.execute(
                "SELECT * FROM incidents WHERE "
                "(resolved_at IS NULL AND open_notified_at IS NULL) OR "
                "(resolved_at IS NOT NULL AND open_notified_at IS NOT NULL AND recovery_notified_at IS NULL) "
                "ORDER BY id"
            ))

    def mark_incident_notified(self, incident_id: int, recovery: bool) -> None:
        column = "recovery_notified_at" if recovery else "open_notified_at"
        with self._lock, self.conn:
            self.conn.execute(f"UPDATE incidents SET {column}=? WHERE id=?", (utc_now(), incident_id))

    def request_control(self, action: str, requested_by: str | None, payload: dict[str, Any] | None = None) -> int:
        with self._lock, self.conn:
            cur = self.conn.execute(
                "INSERT INTO control_requests(action,payload_json,requested_by,requested_at) VALUES(?,?,?,?)",
                (action, json.dumps(payload or {}, ensure_ascii=False), requested_by, utc_now()),
            )
            return int(cur.lastrowid)

    def request_export_once(
        self, requested_by: str, file_key: str,
    ) -> int:
        """Join a pending full/target export instead of queuing a duplicate."""
        with self._lock, self.conn:
            rows = self.conn.execute(
                "SELECT id,action,payload_json FROM control_requests "
                "WHERE status='pending' AND action IN ('export','export_file','export_qualifying') "
                "ORDER BY id"
            ).fetchall()
            for row in rows:
                if row["action"] == "export":
                    return int(row["id"])
                if row["action"] == "export_qualifying" and file_key in {
                    "qualifying", "sui_qualifying"
                }:
                    return int(row["id"])
                try:
                    payload = json.loads(row["payload_json"] or "{}")
                except (TypeError, ValueError):
                    payload = {}
                if payload.get("file_key") == file_key:
                    return int(row["id"])
            cur = self.conn.execute(
                "INSERT INTO control_requests(action,payload_json,requested_by,requested_at) "
                "VALUES('export_file',?,?,?)",
                (json.dumps({"file_key": file_key}), requested_by, utc_now()),
            )
            return int(cur.lastrowid)

    def pending_controls(self) -> list[ControlRequest]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM control_requests WHERE status='pending' ORDER BY id"
            ).fetchall()
        return [ControlRequest(int(row["id"]), row["action"], json.loads(row["payload_json"]), row["requested_by"]) for row in rows]

    def finish_control(self, request_id: int, status: str, result: str) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE control_requests SET status=?,handled_at=?,result=? WHERE id=?",
                (status, utc_now(), result[:2000], request_id),
            )

    def control_request(self, request_id: int) -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM control_requests WHERE id=?", (request_id,)
            ).fetchone()

    def set_discovery_worker(
        self, chain: str, role: str, stage: str,
        range_start: int | None = None, range_end: int | None = None,
        failures: int = 0, last_error: str | None = None,
        endpoint: str | None = None,
    ) -> None:
        now = utc_now()
        key = f"{chain}:{role}"
        with self._lock, self.conn:
            self.conn.execute(
                """INSERT INTO discovery_workers(
                       worker_key,chain,role,stage,range_start,range_end,started_at,
                       updated_at,failures,last_error,endpoint)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(worker_key) DO UPDATE SET
                       stage=excluded.stage,range_start=excluded.range_start,
                       range_end=excluded.range_end,
                       started_at=CASE
                           WHEN discovery_workers.range_start IS NOT excluded.range_start
                             OR discovery_workers.range_end IS NOT excluded.range_end
                           THEN excluded.started_at ELSE discovery_workers.started_at END,
                       updated_at=excluded.updated_at,failures=excluded.failures,
                       last_error=excluded.last_error,endpoint=excluded.endpoint""",
                (key, chain, role, stage, range_start, range_end, now, now,
                 failures, last_error, endpoint),
            )

    def discovery_worker(self, chain: str, role: str = "live") -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM discovery_workers WHERE worker_key=?",
                (f"{chain}:{role}",),
            ).fetchone()

    def audit(self, chat_id: str, user_id: str | None, command: str, allowed: bool, result: str = "") -> None:
        with self._lock, self.conn:
            self.conn.execute(
                "INSERT INTO command_audit(ts,chat_id,user_id,command,allowed,result) VALUES(?,?,?,?,?,?)",
                (utc_now(), chat_id, user_id, command[:500], int(allowed), result[:1000]),
            )

    def record_delivery(self, chat_id: str, kind: str, status: str, reference_id: str | None = None, error: str | None = None) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                "INSERT INTO deliveries(ts,chat_id,kind,reference_id,status,error) VALUES(?,?,?,?,?,?)",
                (utc_now(), chat_id, kind, reference_id, status, error[:1000] if error else None),
            )

    def rows(self, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        with self._lock:
            return list(self.conn.execute(sql, tuple(params)))

    def prune(self, days: int = 30) -> None:
        cutoff = datetime.fromtimestamp(time.time() - days * 86400, UTC).isoformat()
        with self._lock, self.conn:
            for table in ("chain_samples", "aggregate_samples", "resource_samples"):
                self.conn.execute(f"DELETE FROM {table} WHERE ts<?", (cutoff,))


def parse_period(value: str, allowed: tuple[str, ...] = ("30m", "1h", "6h", "12h", "24h", "7d")) -> int:
    value = value.strip().lower()
    if value not in allowed:
        raise ValueError(f"allowed: {', '.join(allowed)}")
    unit = value[-1]
    amount = int(value[:-1])
    return amount * {"m": 60, "h": 3600, "d": 86400}[unit]


def percentile(values: Iterable[float | int | None], quantile: float) -> float | None:
    clean = sorted(float(value) for value in values if value is not None)
    if not clean:
        return None
    index = (len(clean) - 1) * quantile
    lower = int(index)
    upper = min(len(clean) - 1, lower + 1)
    fraction = index - lower
    return clean[lower] * (1 - fraction) + clean[upper] * fraction
