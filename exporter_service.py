"""Isolated, resource-limited XLSX worker for monitoring control requests."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from monitoring import MonitorStore
from scan_defi import (
    DB, ROOT, copy_daily_report_snapshot, load_config, run_export_process, setup_logging,
)


EXPORT_ACTIONS = {"export", "export_qualifying", "export_file"}
log = logging.getLogger("exporter")


async def serve(min_usd: float | None = None) -> None:
    cfg = load_config(ROOT / "config.yaml")
    if min_usd is not None:
        cfg.min_usd = min_usd
    setup_logging(cfg.log_dir)
    monitor = MonitorStore(cfg.monitoring_db_path)
    db = DB(cfg.db_path, read_only=True)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass
    lock = asyncio.Lock()
    monitor.set_setting("exporter_state", "idle")
    try:
        while not stop.is_set():
            requests = [r for r in monitor.pending_controls() if r.action in EXPORT_ACTIONS]
            if not requests:
                qualifying = cfg.export_dir / "qualifying.xlsx"
                last = qualifying.stat().st_mtime if qualifying.exists() else 0
                if time.time() - last >= cfg.export_every_sec:
                    requests = [type("PeriodicRequest", (), {
                        "id": None, "action": "export_qualifying", "payload": {},
                    })()]
            for request in requests:
                mode = ("full" if request.action == "export" else
                        "qualifying" if request.action == "export_qualifying" else "file")
                file_key = request.payload.get("file_key") if mode == "file" else None
                try:
                    monitor.set_setting("exporter_state", f"running:{file_key or mode}")
                    paths = await run_export_process(
                        db, cfg, cfg.chains, lock, mode=mode, file_key=file_key,
                    )
                    monitor.set_setting("exporter_state", "idle")
                    monitor.set_setting(f"last_{mode}_export_at", time.time())
                    if mode in {"full", "qualifying"}:
                        monitor.set_setting("last_export_revision", db.data_revision())
                    if cfg.daily_snapshot:
                        local = datetime.now(timezone.utc).astimezone(ZoneInfo("Europe/Moscow"))
                        day = local.strftime("%Y%m%d")
                        if local.hour >= 3 and monitor.setting("last_snapshot_day", "") != day:
                            try:
                                await asyncio.to_thread(copy_daily_report_snapshot, cfg.export_dir)
                            except Exception:
                                log.exception("Daily XLSX archive failed")
                            else:
                                monitor.set_setting("last_snapshot_day", day)
                    monitor.set_setting("consecutive_export_failures", 0)
                    monitor.resolve_incident("export:failed")
                    if request.id is not None:
                        monitor.finish_control(request.id, "complete",
                                               "exported: " + ", ".join(p.name for p in paths))
                except Exception as exc:
                    log.exception("XLSX export failed")
                    monitor.set_setting("exporter_state", "failed")
                    failures = int(monitor.setting("consecutive_export_failures", "0") or 0) + 1
                    monitor.set_setting("consecutive_export_failures", failures)
                    if request.id is not None:
                        monitor.finish_control(request.id, "failed", f"{type(exc).__name__}: {exc}")
                    if failures >= 2:
                        monitor.open_incident("export:failed", "critical", "export_failure",
                                              f"XLSX export failed {failures} times: {type(exc).__name__}")
                    if request.id is None:
                        await asyncio.sleep(60)
            try:
                await asyncio.wait_for(stop.wait(), timeout=2)
            except asyncio.TimeoutError:
                pass
    finally:
        db.close()
        monitor.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-usd", type=float)
    asyncio.run(serve(parser.parse_args().min_usd))
