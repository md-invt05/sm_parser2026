import asyncio
import io
import logging
from datetime import datetime, timedelta, timezone

from monitoring import MonitorStore
from scan_defi import DB
from telegram_bot import BotService, TokenRedactingFormatter


ADDRESS = "0x" + "ab" * 20
OTHER = "0x" + "cd" * 20


def test_new_address_queue_matches_first_scan_and_repairs_on_restart(tmp_path):
    path = tmp_path / "contracts.db"
    db = DB(path)
    assert db.upsert_contracts([
        ("ethereum", ADDRESS, 10, "0x1", None, "2026-01-02"),
        ("base", ADDRESS, 11, "0x2", None, "2026-01-01"),
        ("ethereum", OTHER, 12, "0x3", None, "2026-01-03"),
    ]) == 3
    assert db.pending_addresses(0) == [ADDRESS, OTHER]
    assert db.balance_queue_snapshot()["balance_new_pending"] == 2
    assert db.conn.execute(
        "SELECT first_seen_at FROM new_balance_addresses WHERE address=?", (ADDRESS,)
    ).fetchone()[0] == "2026-01-01"
    db.save_address_chain_state(ADDRESS, {
        "chain": "ethereum", "status": "rpc_error", "has_code": None,
        "total_usd": None,
    }, [], 500_000)
    assert db.pending_addresses(0)[0] == OTHER
    assert db.balance_queue_snapshot()["balance_new_pending"] == 1
    # A stopped old writer or interrupted maintenance can leave the cache stale.
    with db.conn:
        db.conn.execute("DELETE FROM new_balance_addresses")
    db.close()
    reopened = DB(path)
    assert reopened.pending_addresses(0)[0] == OTHER
    assert reopened.balance_queue_snapshot()["balance_new_pending"] == 1
    reopened.close()


def test_new_queue_respects_canonical_and_cap_and_transaction_rollback(tmp_path):
    db = DB(tmp_path / "contracts.db")
    db.upsert_contracts([("ethereum", ADDRESS, 10, "0x1", None, "2026-01-01")])
    with db.conn:
        db.conn.execute("UPDATE contracts SET canonical=0 WHERE address=?", (ADDRESS,))
    assert db.pending_addresses(0) == []
    db.reconcile_new_balance_addresses()
    assert db.balance_queue_snapshot()["balance_new_pending"] == 0
    with db.conn:
        db.conn.execute("UPDATE contracts SET canonical=1 WHERE address=?", (ADDRESS,))
    db.reconcile_new_balance_addresses()
    assert db.pending_addresses(0) == [ADDRESS]
    with db.conn:
        db.conn.execute(
            "INSERT INTO address_recheck_caps(address,capped_at,scan_count) VALUES(?,?,?)",
            (ADDRESS, "2026-01-01", 3),
        )
    assert db.pending_addresses(0) == []
    db.reconcile_new_balance_addresses()
    assert db.balance_queue_snapshot()["balance_new_pending"] == 0
    db.close()


def test_token_governor_count_equals_detailed_snapshot(tmp_path):
    db = DB(tmp_path / "contracts.db")
    now = datetime.now(timezone.utc)
    due = (now - timedelta(minutes=1)).isoformat()
    later = (now + timedelta(days=1)).isoformat()
    with db.conn:
        for index, (recent, history, completed) in enumerate([
            (due, later, None), (later, due, None), (later, due, due), (later, later, None),
        ]):
            db.conn.execute(
                """INSERT INTO token_log_tasks(chain,address,first_block,next_block,
                   due_at,updated_at,recent_due_at,history_due_at,completed_at)
                   VALUES('ethereum',?,?,?,?,?,?,?,?)""",
                (f"0x{index:040x}", 1, 1, due, due, recent, history, completed),
            )
    assert db.token_log_due_count() == sum(
        row["due"] for row in db.token_log_queue_snapshot().values()
    )
    db.close()


def test_latest_chain_pointer_seeds_and_prunes_with_history(tmp_path):
    path = tmp_path / "monitoring.db"
    store = MonitorStore(path)
    old = (datetime.now(timezone.utc) - timedelta(days=40)).isoformat()
    current = datetime.now(timezone.utc).isoformat()
    for role, stamp in (("live", old), ("backfill", current), ("balance", current)):
        store.add_chain_sample(chain="base", role=role, ts=stamp, active_rpc=role,
                               contracts=0, direct_deploy=0, active_call=0,
                               cooldown_sec=0, rpc_requests=0, rpc_successes=0, rpc_errors=0)
    expected = store.rows(
        """SELECT id FROM chain_samples WHERE id IN
           (SELECT MAX(id) FROM chain_samples
            WHERE role IN ('live','balance','discovery+balance') GROUP BY chain)"""
    )[0]["id"]
    assert store.latest_incident_chain_samples()[0]["active_rpc"] == "balance"
    with store.conn:
        store.conn.execute("DELETE FROM latest_chain_sample_ids")
        store.conn.execute("UPDATE schema_meta SET version=4")
    store.close()
    store = MonitorStore(path)
    assert store.rows("SELECT sample_id FROM latest_chain_sample_ids WHERE role='balance'")[0][0] == expected
    store.prune(30)
    assert store.rows("SELECT 1 FROM latest_chain_sample_ids WHERE role='live'") == []
    assert store.latest_incident_chain_samples()[0]["active_rpc"] == "balance"
    store.close()


def test_load_alert_requires_two_minutes_to_recover_and_suppresses_warning(tmp_path):
    store = MonitorStore(tmp_path / "monitoring.db")
    bot = BotService.__new__(BotService)
    bot.monitor = store
    now = datetime.now(timezone.utc)
    for seconds in range(600, -1, -15):
        store.add_resource_sample({
            "ts": (now - timedelta(seconds=seconds)).isoformat(),
            "load1": 4.2, "cpu_cores": 2,
        })
    asyncio.run(bot._evaluate_resource_thresholds(now))
    assert store.active_incident("system:load:critical") is not None
    assert store.active_incident("system:load:warning") is None
    store.add_resource_sample({"ts": (now + timedelta(seconds=15)).isoformat(),
                               "load1": 3.0, "cpu_cores": 2})
    asyncio.run(bot._evaluate_resource_thresholds(now + timedelta(seconds=15)))
    assert store.active_incident("system:load:critical") is not None
    for seconds in range(30, 151, 15):
        store.add_resource_sample({"ts": (now + timedelta(seconds=seconds)).isoformat(),
                                   "load1": 3.0, "cpu_cores": 2})
    asyncio.run(bot._evaluate_resource_thresholds(now + timedelta(seconds=150)))
    assert store.active_incident("system:load:critical") is None
    store.close()


def test_existing_load_warning_does_not_recover_on_one_quiet_sample(tmp_path):
    store = MonitorStore(tmp_path / "monitoring.db")
    bot = BotService.__new__(BotService)
    bot.monitor = store
    now = datetime.now(timezone.utc)
    store.open_incident("system:load:warning", "warning", "system_load", "high load")
    for seconds in range(600, -1, -15):
        store.add_resource_sample({"ts": (now - timedelta(seconds=seconds)).isoformat(),
                                   "load1": 4.2, "cpu_cores": 2})
    asyncio.run(bot._evaluate_resource_thresholds(now))
    assert store.active_incident("system:load:critical") is not None
    assert store.active_incident("system:load:warning") is not None
    store.add_resource_sample({"ts": (now + timedelta(seconds=15)).isoformat(),
                               "load1": 2.0, "cpu_cores": 2})
    asyncio.run(bot._evaluate_resource_thresholds(now + timedelta(seconds=15)))
    assert store.active_incident("system:load:warning") is not None
    for seconds in range(30, 151, 15):
        store.add_resource_sample({"ts": (now + timedelta(seconds=seconds)).isoformat(),
                                   "load1": 2.0, "cpu_cores": 2})
    asyncio.run(bot._evaluate_resource_thresholds(now + timedelta(seconds=150)))
    assert store.active_incident("system:load:warning") is None
    store.close()


def test_telegram_token_is_redacted_even_in_exception_text():
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    secret = "123456:fakeSecretForTest"
    handler.setFormatter(TokenRedactingFormatter("%(message)s", secret))
    logger = logging.getLogger("test-token-redaction")
    logger.addHandler(handler)
    try:
        logger.warning("https://api.telegram.org/bot%s/getUpdates", secret)
        assert secret not in stream.getvalue()
        assert "[redacted]" in stream.getvalue()
    finally:
        logger.removeHandler(handler)
