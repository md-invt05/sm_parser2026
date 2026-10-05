import sqlite3
from pathlib import Path

import pytest

import scan_defi as scanner
from sui_support import SuiStore
from scripts.rollback_bridge import bridge


ADDRESS = "0x" + "ab" * 20


def test_bridge_replays_open_gap_and_restores_legacy_due_without_loss(tmp_path: Path):
    path = tmp_path / "contracts.db"
    backup = tmp_path / "backup.db"
    db = scanner.DB(path)
    db.init_chain("ethereum", 100, True)
    db.init_chain_cursors("ethereum", 100, 120, lookback=1)
    gap_id = db.reanchor_tip("ethereum", 200, 1000)
    assert gap_id is not None
    db.save_address_chain_state(ADDRESS, {
        "chain": "ethereum", "status": "partial", "has_code": 1,
        "native_raw": "0", "native_amount": 0,
        "observed_native_usd": 0, "included_native_usd": 0,
        "tokens_usd": 0, "total_usd": 0,
    }, [], 500_000)
    db.conn.execute(
        """INSERT INTO balance_work_items(chain,address,kind,asset,status,priority,due_at,updated_at)
           VALUES('ethereum',?,'token_balance','0xtoken','pending',50,
                  '2000-01-01T00:00:00+00:00','2000-01-01T00:00:00+00:00')""",
        (ADDRESS,),
    )
    db.conn.execute(
        """INSERT INTO token_log_tasks(chain,address,first_block,next_block,due_at,
             updated_at,recent_due_at,history_due_at)
           VALUES('ethereum',?,100,100,'2099-01-01T00:00:00+00:00',
                  '2000-01-01T00:00:00+00:00',
                  '2001-01-01T00:00:00+00:00','2000-01-01T00:00:00+00:00')""",
        (ADDRESS,),
    )
    db.conn.commit()
    tip_before = int(db.cursor("ethereum", "live")["next_block"])
    financial_before = db.conn.execute(
        "SELECT status,total_usd FROM address_chain_state WHERE address=?", (ADDRESS,)
    ).fetchone()
    with sqlite3.connect(backup) as saved:
        db.conn.backup(saved)
    db.close()
    dry = bridge(path)
    assert not dry["applied"] and dry["rewind_live_to"]["ethereum"] < tip_before
    with sqlite3.connect(path) as before:
        assert before.execute(
            "SELECT next_block FROM chain_cursors WHERE chain='ethereum' AND role='live'"
        ).fetchone()[0] == tip_before
    with pytest.raises(ValueError, match="offline"):
        bridge(path, apply=True, backup=backup)
    applied = bridge(path, apply=True, backup=backup, offline_ack=True)
    assert applied["applied"]
    assert applied["before"]["financial_sha256"] == applied["after"]["financial_sha256"]
    assert applied["before"]["contracts"] == applied["after"]["contracts"]
    again = bridge(path, apply=True, backup=backup, offline_ack=True)
    assert again["rewind_live_to"] == {}
    with sqlite3.connect(path) as check:
        assert check.execute(
            "SELECT next_block FROM chain_cursors WHERE chain='ethereum' AND role='live'"
        ).fetchone()[0] == dry["rewind_live_to"]["ethereum"]
        assert check.execute(
            "SELECT next_retry_at FROM address_chain_state WHERE address=?", (ADDRESS,)
        ).fetchone()[0] == "2000-01-01T00:00:00+00:00"
        assert check.execute(
            "SELECT due_at FROM token_log_tasks WHERE address=?", (ADDRESS,)
        ).fetchone()[0] == "2000-01-01T00:00:00+00:00"
        assert check.execute(
            "SELECT status,total_usd FROM address_chain_state WHERE address=?", (ADDRESS,)
        ).fetchone() == tuple(financial_before)


def test_bridge_refuses_unresolved_reorg_and_marks_sui_gap_unverified(tmp_path: Path):
    path = tmp_path / "contracts.db"
    backup = tmp_path / "backup.db"
    db = scanner.DB(path)
    db.init_chain("ethereum", 100, True)
    db.init_chain_cursors("ethereum", 100, 120, lookback=1)
    db.conn.execute(
        "UPDATE chain_cursors SET status='reorg_alert' WHERE role='live'"
    )
    db.conn.commit()
    with pytest.raises(ValueError, match="reorg"):
        bridge(path)
    db.conn.execute(
        "UPDATE chain_cursors SET status='active' WHERE role='live'"
    )
    db.conn.commit()
    sui = SuiStore(path)
    sui.conn.execute(
        """UPDATE sui_state SET verified_next_checkpoint=11,
             verified_start_checkpoint=10,source_mode='grpc_partial' WHERE id=1"""
    )
    sui.conn.execute(
        """INSERT INTO sui_checkpoint_gaps(start_checkpoint,end_checkpoint,
             next_checkpoint,status,reason,created_at,updated_at)
           VALUES(11,12,11,'active','test','2026-01-01','2026-01-01')"""
    )
    sui.conn.commit()
    sui.close()
    with sqlite3.connect(backup) as saved:
        db.conn.backup(saved)
    db.close()
    assert bridge(path, apply=True, backup=backup, offline_ack=True)[
        "sui_coverage_unverified"
    ]
    with sqlite3.connect(path) as check:
        mode, cursor = check.execute(
            "SELECT source_mode,verified_next_checkpoint FROM sui_state WHERE id=1"
        ).fetchone()
        assert (mode, cursor) == ("coverage_unverified", 11)
