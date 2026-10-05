"""Prepare a compatible old scanner without discarding new discoveries.

Default invocation is read-only. --apply requires an offline scanner and an
existing, integrity-checked online backup. New tables and observations remain.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from contextlib import closing
from pathlib import Path


def _table(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def plan(conn: sqlite3.Connection) -> dict:
    required = {"chain_cursors", "discovery_gaps", "balance_work_items",
                "address_chain_state", "token_log_tasks"}
    missing = sorted(name for name in required if not _table(conn, name))
    if missing:
        raise ValueError(f"bridge schema incomplete: {missing}")
    hazards = conn.execute(
        """SELECT COUNT(*) FROM chain_cursors WHERE status IN ('reorg_alert','reorg_replay')"""
    ).fetchone()[0] + conn.execute(
        """SELECT COUNT(*) FROM discovery_gaps WHERE status IN ('reorg_alert','reorg_replay')"""
    ).fetchone()[0]
    if hazards:
        raise ValueError("unresolved reorg; rollback bridge cannot choose a safe cursor")
    gaps = conn.execute(
        """SELECT chain,MIN(next_block) FROM discovery_gaps
           WHERE status='active' AND next_block<=end_block GROUP BY chain"""
    ).fetchall()
    rewind = {}
    for chain, next_block in gaps:
        row = conn.execute(
            "SELECT next_block FROM chain_cursors WHERE chain=? AND role='live'", (chain,)
        ).fetchone()
        if row is None:
            raise ValueError(f"missing live cursor for gap: {chain}")
        target = min(int(row[0]), int(next_block))
        if target < 0:
            raise ValueError(f"invalid rollback cursor: {chain}")
        if target < int(row[0]):
            rewind[str(chain)] = target
    work = conn.execute(
        """SELECT chain,address,MIN(due_at) FROM balance_work_items
           WHERE status='pending' GROUP BY chain,address"""
    ).fetchall()
    unlinked = [(chain, address) for chain, address, _ in work if conn.execute(
        "SELECT 1 FROM address_chain_state WHERE chain=? AND address=?", (chain, address)
    ).fetchone() is None]
    if unlinked:
        raise ValueError(f"pending balance work lacks chain state: {len(unlinked)}")
    token = conn.execute(
        """SELECT COUNT(*) FROM token_log_tasks WHERE recent_due_at IS NOT NULL
           OR history_due_at IS NOT NULL"""
    ).fetchone()[0]
    sui_unverified = False
    if _table(conn, "sui_state") and _table(conn, "sui_checkpoint_gaps"):
        sui_unverified = bool(conn.execute(
            "SELECT 1 FROM sui_checkpoint_gaps WHERE status='active' LIMIT 1"
        ).fetchone())
    return {"rewind_live_to": rewind, "balance_chains": len(work),
            "token_tasks": int(token), "sui_coverage_unverified": sui_unverified}


def snapshot(conn: sqlite3.Connection) -> dict:
    finance = hashlib.sha256()
    for table, columns, order in (
        ("address_chain_state", "address,chain,status,native_raw,total_usd,tokens_usd",
         "address,chain"),
        ("address_token_state", "address,chain,token,raw_amount,amount,usd_value",
         "address,chain,token"),
    ):
        for row in conn.execute(f"SELECT {columns} FROM {table} ORDER BY {order}"):
            finance.update(repr(tuple(row)).encode("utf-8"))
            finance.update(b"\n")
    return {
        "contracts": conn.execute("SELECT COUNT(*) FROM contracts").fetchone()[0],
        "discoveries": conn.execute("SELECT COUNT(*) FROM contract_discoveries").fetchone()[0],
        "financial_sha256": finance.hexdigest(),
        "live_next": dict(conn.execute(
            "SELECT chain,next_block FROM chain_cursors WHERE role='live' ORDER BY chain"
        ).fetchall()),
    }


def bridge(path: Path, *, apply: bool = False, backup: Path | None = None,
           offline_ack: bool = False) -> dict:
    if not path.is_file():
        raise ValueError("database path does not exist")
    if apply:
        if not offline_ack or backup is None or not backup.is_file():
            raise ValueError("--apply requires --offline-ack and an existing --backup")
        if path.resolve() == backup.resolve():
            raise ValueError("backup must be a separate file")
        with closing(sqlite3.connect(
                f"file:{backup.resolve().as_posix()}?mode=ro", uri=True)) as saved:
            if saved.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise ValueError("backup integrity check failed")
        conn = sqlite3.connect(path)
    else:
        conn = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    try:
        result = plan(conn)
        before = snapshot(conn)
        if not apply:
            return {**result, "before": before, "after": None, "applied": False}
        conn.execute("BEGIN IMMEDIATE")
        try:
            for chain, target in result["rewind_live_to"].items():
                conn.execute(
                    """UPDATE chain_cursors SET next_block=?,last_committed=?,
                         segment_start_block=?,status='active',
                         note='rollback bridge: replay earliest open gap'
                       WHERE chain=? AND role='live' AND next_block>?""",
                    (target, target - 1, target, chain, target),
                )
            conn.execute(
                """UPDATE address_chain_state SET next_retry_at=(
                     SELECT MIN(w.due_at) FROM balance_work_items w
                     WHERE w.chain=address_chain_state.chain
                       AND w.address=address_chain_state.address
                       AND w.status='pending')
                   WHERE EXISTS(SELECT 1 FROM balance_work_items w
                     WHERE w.chain=address_chain_state.chain
                       AND w.address=address_chain_state.address
                       AND w.status='pending' AND w.due_at<address_chain_state.next_retry_at)"""
            )
            conn.execute(
                """UPDATE token_log_tasks SET due_at=MIN(
                     COALESCE(recent_due_at,history_due_at),
                     COALESCE(history_due_at,recent_due_at))
                   WHERE recent_due_at IS NOT NULL OR history_due_at IS NOT NULL"""
            )
            if result["sui_coverage_unverified"]:
                conn.execute(
                    """UPDATE sui_state SET source_mode='coverage_unverified'
                       WHERE id=1"""
                )
            after = snapshot(conn)
            if (before["contracts"] != after["contracts"] or
                    before["discoveries"] != after["discoveries"] or
                    before["financial_sha256"] != after["financial_sha256"]):
                raise ValueError("rollback bridge changed financial/discovery observations")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        return {**result, "before": before, "after": after, "applied": True}
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--backup", type=Path)
    parser.add_argument("--offline-ack", action="store_true")
    args = parser.parse_args()
    print(json.dumps(bridge(args.database, apply=args.apply,
                            backup=args.backup, offline_ack=args.offline_ack),
                     ensure_ascii=False, indent=2))
