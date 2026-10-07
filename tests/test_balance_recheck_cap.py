from datetime import datetime, timedelta, timezone

import scan_defi as scanner


ADDRESS = "0x" + "ab" * 20
TOKEN = "0x" + "cd" * 20
CHAIN_ROW = {"chain": "ethereum", "status": "rpc_error", "has_code": None,
             "total_usd": None}


def _scan(db, status="incomplete"):
    return db.save_address_scan(ADDRESS, status, 0, 0, 1, [CHAIN_ROW], [])


def test_initial_scan_and_two_rechecks_then_no_more_even_after_rpc_failure(tmp_path):
    db = scanner.DB(tmp_path / "contracts.db")
    db.upsert_contracts([("ethereum", ADDRESS, 1, "0x1", None, "2026-01-01")])
    db.save_address_chain_state(ADDRESS, CHAIN_ROW, [], 500_000)
    later = (datetime.now(timezone.utc) + timedelta(days=10)).timestamp()
    for count in (1, 2):
        _scan(db)
        assert db.due_address_chains(ADDRESS, ["ethereum"], later) == ["ethereum"]
        assert db.pending_addresses(86400, as_of=later) == [ADDRESS]
        assert db.conn.execute("SELECT COUNT(*) FROM address_recheck_caps").fetchone()[0] == 0

    _scan(db)
    assert db.conn.execute("SELECT scan_count FROM address_recheck_caps").fetchone()[0] == 3
    assert db.due_address_chains(ADDRESS, ["ethereum"], later) == []
    assert db.pending_addresses(86400, as_of=later) == []
    assert db.due_balance_work(ADDRESS, "ethereum", later) == []
    assert db.pending_rabby_scan(86400, as_of=later, minimum_age_sec=0) is None
    assert db.balance_queue_snapshot()["balance_pending"] == 0
    assert db.conn.execute(
        "SELECT COUNT(*) FROM balance_work_items WHERE status='pending'"
    ).fetchone()[0] == 0
    db.close()


def test_new_token_and_schedule_cannot_requeue_capped_address(tmp_path):
    db = scanner.DB(tmp_path / "contracts.db")
    db.upsert_contracts([("ethereum", ADDRESS, 1, "0x1", None, "2026-01-01")])
    db.save_address_chain_state(ADDRESS, CHAIN_ROW, [], 500_000)
    for _ in range(3):
        _scan(db)
    db.upsert_contract_tokens([("ethereum", ADDRESS, TOKEN, "test", 10)])
    db.apply_address_schedule(ADDRESS, "qualifying", 600_000, 500_000)
    db.conn.execute(
        "UPDATE address_chain_state SET next_retry_at='2020-01-01T00:00:00+00:00',"
        "price_retry_at='2020-01-01T00:00:00+00:00' WHERE address=?", (ADDRESS,),
    )
    assert db.pending_addresses(86400) == []
    assert db.due_address_chains(ADDRESS, ["ethereum"]) == []
    assert db.price_refresh_candidates() == []
    assert db.balance_queue_snapshot()["balance_pending"] == 0
    db.close()


def test_existing_rechecks_are_capped_once_without_changing_scans_or_cursors(tmp_path):
    db = scanner.DB(tmp_path / "contracts.db")
    db.upsert_contracts([("ethereum", ADDRESS, 1, "0x1", None, "2026-01-01")])
    db.save_address_chain_state(ADDRESS, CHAIN_ROW, [], 500_000)
    for _ in range(3):
        _scan(db)
    db.conn.execute("DELETE FROM address_recheck_caps")
    db.conn.execute(
        "UPDATE address_chain_state SET next_retry_at='2020-01-01T00:00:00+00:00'"
    )
    db.conn.execute(
        "INSERT OR REPLACE INTO balance_work_items"
        "(chain,address,kind,asset,status,priority,due_at,updated_at) "
        "VALUES('ethereum',?,'code','','pending',10,'2020-01-01','2020-01-01')",
        (ADDRESS,),
    )
    before = db.conn.execute(
        "SELECT COUNT(*),SUM(total_usd) FROM address_scans"
    ).fetchone()
    assert db.cap_existing_address_rechecks() == 1
    assert db.cap_existing_address_rechecks() == 0
    after = db.conn.execute(
        "SELECT COUNT(*),SUM(total_usd) FROM address_scans"
    ).fetchone()
    assert tuple(before) == tuple(after)
    assert db.conn.execute(
        "SELECT status FROM balance_work_items"
    ).fetchone()[0] == "capped"
    assert db.pending_addresses(86400) == []
    db.close()
