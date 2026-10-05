"""Rehearse additive migration and legacy rollback on a disposable DB copy.

The source SQLite snapshot is opened read-only. No scanner or network is used.
"""

from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from contextlib import closing
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scan_defi import DB  # noqa: E402
from sui_support import SuiStore  # noqa: E402
from scripts.rollback_bridge import bridge  # noqa: E402
from scripts.validate_migration_copy import snapshot  # noqa: E402


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main(source: Path) -> dict:
    check(source.is_file(), "source snapshot missing")
    with tempfile.TemporaryDirectory(prefix="rollback-rehearsal-", dir=ROOT / "backups") as temp:
        work = Path(temp) / "migrated.db"
        saved = Path(temp) / "pre_bridge.db"
        with closing(sqlite3.connect(f"file:{source.resolve().as_posix()}?mode=ro", uri=True)) as src:
            with closing(sqlite3.connect(work)) as target:
                src.backup(target, pages=1024)
        db = DB(work)
        sui = SuiStore(work)
        sui.close()
        db.close()
        with closing(sqlite3.connect(work)) as conn:
            baseline = snapshot(conn)
            chain, live_next = conn.execute(
                "SELECT chain,next_block FROM chain_cursors WHERE role='live' "
                "AND chain='ethereum'"
            ).fetchone()
            address, prior_due = conn.execute(
                "SELECT address,next_retry_at FROM address_chain_state "
                "WHERE chain=? AND next_retry_at>'2001-01-01' LIMIT 1", (chain,)
            ).fetchone()
            token_chain, token_address, prior_token_due = conn.execute(
                "SELECT chain,address,due_at FROM token_log_tasks "
                "WHERE chain=? LIMIT 1", (chain,)
            ).fetchone()
            sui_old = conn.execute(
                "SELECT last_checkpoint FROM sui_state WHERE id=1"
            ).fetchone()[0]
            gap_start = int(live_next) - 10
            check(gap_start > 0, "live cursor cannot be rewound safely")
            now = "2026-10-05T00:00:00+00:00"
            retry_due = "2000-01-01T00:00:00+00:00"
            conn.execute(
                "INSERT INTO discovery_gaps(chain,start_block,end_block,next_block,"
                "last_committed,status,reason,created_at,updated_at) "
                "VALUES(?,?,?,?,?,'active','rehearsal',?,?)",
                (chain, gap_start, gap_start + 4, gap_start, gap_start - 1, now, now),
            )
            conn.execute(
                "INSERT INTO balance_work_items(chain,address,kind,asset,status,"
                "priority,due_at,updated_at) VALUES(?,?,?,?,'pending',50,?,?)",
                (chain, address, "token_balance", "rehearsal-token", retry_due, now),
            )
            conn.execute(
                "UPDATE token_log_tasks SET due_at='2099-01-01T00:00:00+00:00',"
                "recent_due_at='2001-01-01T00:00:00+00:00',"
                "history_due_at='2000-01-01T00:00:00+00:00' "
                "WHERE chain=? AND address=?", (token_chain, token_address),
            )
            conn.execute(
                "UPDATE sui_state SET verified_start_checkpoint=?,"
                "verified_next_checkpoint=?,source_mode='grpc_partial' WHERE id=1",
                (sui_old - 10, sui_old - 5),
            )
            conn.execute(
                "INSERT INTO sui_checkpoint_gaps(start_checkpoint,end_checkpoint,"
                "next_checkpoint,status,reason,created_at,updated_at) "
                "VALUES(?,?,?,'active','rehearsal',?,?)",
                (sui_old - 5, sui_old - 3, sui_old - 5, now, now),
            )
            conn.commit()
            with closing(sqlite3.connect(saved)) as backup:
                conn.backup(backup, pages=1024)
        dry = bridge(work)
        check(not dry["applied"], "dry-run unexpectedly wrote data")
        check(dry["rewind_live_to"].get(chain) == gap_start,
              "dry-run did not identify earliest gap")
        check(dry["sui_coverage_unverified"], "Sui gap was not identified")
        applied = bridge(work, apply=True, backup=saved, offline_ack=True)
        again = bridge(work, apply=True, backup=saved, offline_ack=True)
        with closing(sqlite3.connect(work)) as conn:
            after = snapshot(conn)
            live_after = conn.execute(
                "SELECT next_block FROM chain_cursors WHERE chain=? AND role='live'",
                (chain,),
            ).fetchone()[0]
            due_after = conn.execute(
                "SELECT next_retry_at FROM address_chain_state WHERE chain=? AND address=?",
                (chain, address),
            ).fetchone()[0]
            token_after = conn.execute(
                "SELECT due_at FROM token_log_tasks WHERE chain=? AND address=?",
                (token_chain, token_address),
            ).fetchone()[0]
            sui_mode, sui_last = conn.execute(
                "SELECT source_mode,last_checkpoint FROM sui_state WHERE id=1"
            ).fetchone()
        for key in ("contracts", "contract_discoveries", "financial_sha256",
                    "chain_financial_sha256", "token_financial_sha256",
                    "status_counts"):
            check(baseline[key] == after[key], f"rollback changed {key}")
        check(live_after == gap_start and again["rewind_live_to"] == {},
              "legacy cursor rewind is not idempotent")
        check(due_after == retry_due and prior_due > retry_due,
              "granular retry not represented in legacy schedule")
        check(token_after == "2000-01-01T00:00:00+00:00" and
              prior_token_due != token_after, "token due not bridged")
        check(sui_mode == "coverage_unverified" and sui_last == sui_old,
              "Sui coverage was incorrectly promoted")
        check(applied["before"]["financial_sha256"] ==
              applied["after"]["financial_sha256"],
              "bridge changed financial observations")
        return {
            "dry_run": {"rewind_live_to": dry["rewind_live_to"],
                        "balance_chains": dry["balance_chains"],
                        "token_tasks": dry["token_tasks"],
                        "sui_coverage_unverified": dry["sui_coverage_unverified"]},
            "applied": applied["applied"],
            "second_apply_rewind": again["rewind_live_to"],
            "legacy_live_cursor": live_after,
            "legacy_balance_due": due_after,
            "legacy_token_due": token_after,
            "sui_mode": sui_mode,
            "contracts": after["contracts"],
            "discoveries": after["contract_discoveries"],
            "financial_sha256": after["financial_sha256"],
            "source_was_read_only": True,
        }


if __name__ == "__main__":
    print(json.dumps(main(Path(sys.argv[1])), ensure_ascii=False, indent=2))
