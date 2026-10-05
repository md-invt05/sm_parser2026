"""Read-only source -> SQLite online backup -> additive migration on disposable copy.

Usage: python scripts/validate_migration_copy.py data/contracts.db
The source is never opened for writing. No scanner or RPC is started.
"""

from __future__ import annotations

import hashlib
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


def has_table(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def digest_rows(conn: sqlite3.Connection, table: str, columns: str, order: str) -> str | None:
    if not has_table(conn, table):
        return None
    digest = hashlib.sha256()
    for row in conn.execute(f"SELECT {columns} FROM {table} ORDER BY {order}"):
        digest.update(json.dumps(tuple(row), ensure_ascii=False, default=str,
                                 separators=(",", ":")).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def snapshot(conn: sqlite3.Connection) -> dict:
    result = {}
    result["db_size_bytes"] = int(conn.execute("PRAGMA page_count").fetchone()[0]) * \
        int(conn.execute("PRAGMA page_size").fetchone()[0])
    for table in (
        "contracts", "contract_discoveries", "chain_cursors", "discovery_gaps",
        "discovery_block_hashes", "address_chain_state", "address_token_state",
        "address_scans", "token_log_tasks", "balance_work_items", "sui_seen_transactions",
        "sui_packages", "sui_defi_projects", "sui_checkpoint_gaps",
    ):
        result[table] = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) \
            if has_table(conn, table) else None
    for meta in ("schema_meta", "sui_schema_meta"):
        result[meta] = int(conn.execute(f"SELECT MAX(version) FROM {meta}").fetchone()[0] or 0) \
            if has_table(conn, meta) else None
    result["cursors_sha256"] = digest_rows(
        conn, "chain_cursors", "chain,role,next_block,anchor_block,last_committed",
        "chain,role",
    )
    result["legacy_cursor_sha256"] = digest_rows(
        conn, "chain_state", "chain,start_block,last_indexed", "chain",
    )
    result["financial_sha256"] = digest_rows(
        conn, "address_scans", "address,status,total_usd,coverage,scanned_at",
        "address,id",
    )
    result["chain_financial_sha256"] = digest_rows(
        conn, "address_chain_state", "address,chain,status,native_raw,total_usd,tokens_usd",
        "address,chain",
    )
    result["token_financial_sha256"] = digest_rows(
        conn, "address_token_state", "address,chain,token,raw_amount,usd_value,priced",
        "address,chain,token",
    )
    if has_table(conn, "address_scans"):
        result["status_counts"] = dict(conn.execute(
            "SELECT status,COUNT(*) FROM address_scans GROUP BY status"
        ).fetchall())
        count, total = conn.execute(
            "SELECT COUNT(*),COALESCE(SUM(total_usd),0) FROM address_scans "
            "WHERE id IN (SELECT MAX(id) FROM address_scans GROUP BY address)"
        ).fetchone()
        result["current_address_count"] = int(count)
        result["confirmed_usd_aggregate"] = float(total)
    if has_table(conn, "sui_state"):
        columns = {row[1] for row in conn.execute("PRAGMA table_info(sui_state)")}
        select = "last_checkpoint,verified_next_checkpoint,verified_start_checkpoint" \
            if "verified_next_checkpoint" in columns else "last_checkpoint"
        result["sui_state"] = tuple(conn.execute(
            f"SELECT {select} FROM sui_state WHERE id=1"
        ).fetchone())
        if "grpc_tip_next_checkpoint" in columns:
            result["sui_tip_state"] = tuple(conn.execute(
                "SELECT grpc_tip_start_checkpoint,grpc_tip_next_checkpoint "
                "FROM sui_state WHERE id=1"
            ).fetchone())
    return result


def main(source: Path) -> dict:
    if not source.is_file():
        raise SystemExit(f"source DB not found: {source}")
    source_uri = f"file:{source.resolve().as_posix()}?mode=ro"
    with closing(sqlite3.connect(source_uri, uri=True)) as original:
        original.execute("PRAGMA query_only=ON")
        before = snapshot(original)
        with tempfile.TemporaryDirectory(prefix=".migration-check-", dir=ROOT) as temp_dir:
            target = Path(temp_dir) / "contracts.db"
            with closing(sqlite3.connect(target)) as copy:
                original.backup(copy, pages=1024)
            db = DB(target)
            sui = SuiStore(target)
            # Startup's additive task seed is safe on the copy. It may add
            # technical tasks but must not alter existing observations.
            seeded = db.seed_priority_token_log_tasks(500_000)
            sui.close()
            db.close()
            with closing(sqlite3.connect(target)) as migrated:
                after = snapshot(migrated)
            second_db = DB(target)
            second_sui = SuiStore(target)
            second_seeded = second_db.seed_priority_token_log_tasks(500_000)
            second_sui.close()
            second_db.close()
            with closing(sqlite3.connect(target)) as again:
                second = snapshot(again)
    invariants = ("contracts", "contract_discoveries", "chain_cursors",
                  "address_chain_state", "address_token_state",
                  "address_scans", "sui_seen_transactions", "sui_packages",
                  "sui_defi_projects", "cursors_sha256", "legacy_cursor_sha256",
                  "financial_sha256", "chain_financial_sha256", "token_financial_sha256",
                  "status_counts", "current_address_count", "confirmed_usd_aggregate")
    changed = [key for key in invariants if before.get(key) != after.get(key)]
    if changed:
        raise AssertionError(f"financial/cursor invariants changed: {changed}")
    if after != second or second_seeded:
        raise AssertionError("migration/seed is not idempotent")
    if before.get("sui_state") and before["sui_state"][0] != after["sui_state"][0]:
        raise AssertionError("Blockberry last_checkpoint changed")
    if before.get("sui_state") and len(before["sui_state"]) == 1 and \
            after["sui_state"][1:] != (None, None):
        raise AssertionError("legacy Blockberry maximum was promoted to verified gRPC cursor")
    if "sui_tip_state" not in before and after.get("sui_tip_state") != (None, None):
        raise AssertionError("migration invented a verified Sui tip from Blockberry state")
    if before.get("sui_checkpoint_gaps") is None and after.get("sui_checkpoint_gaps") != 0:
        raise AssertionError("migration invented a Sui checkpoint gap without preflight")
    return {"before": before, "after": after, "seeded_token_tasks": seeded,
            "second_seeded": second_seeded, "source_was_read_only": True}


if __name__ == "__main__":
    source = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "data" / "contracts.db"
    print(json.dumps(main(source), ensure_ascii=False, indent=2))
