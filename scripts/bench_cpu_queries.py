"""Compare CPU-read queries on disposable SQLite copies, never on live DB files.

Usage: python scripts/bench_cpu_queries.py COPY_OF_CONTRACTS_DB COPY_OF_MONITORING_DB
The script migrates both arguments in place. Keep the verified online backups separate.
"""

from __future__ import annotations

import argparse
import math
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from monitoring import MonitorStore
from scan_defi import DB


OLD_PENDING = """SELECT lower(c.address) address,MIN(c.first_seen_at) due_at
FROM contracts c WHERE c.canonical=1 AND NOT EXISTS(
  SELECT 1 FROM address_chain_state s WHERE s.address=lower(c.address))
AND NOT EXISTS(SELECT 1 FROM address_recheck_caps x WHERE x.address=lower(c.address))
GROUP BY lower(c.address) ORDER BY due_at,address LIMIT 200"""

NEW_PENDING = """SELECT q.address,q.first_seen_at due_at FROM new_balance_addresses q
WHERE EXISTS(SELECT 1 FROM contracts c WHERE lower(c.address)=q.address AND c.canonical=1)
  AND NOT EXISTS(SELECT 1 FROM address_chain_state s WHERE s.address=q.address)
  AND NOT EXISTS(SELECT 1 FROM address_recheck_caps x WHERE x.address=q.address)
ORDER BY q.first_seen_at,q.address LIMIT 200"""

OLD_NEW_COUNT = """WITH known AS MATERIALIZED (
  SELECT DISTINCT address FROM address_chain_state
), unique_contracts AS (
  SELECT address,MIN(first_seen_at) first_seen_at FROM contracts GROUP BY address
)
SELECT COUNT(*) n,MIN(c.first_seen_at) oldest FROM unique_contracts c
LEFT JOIN known k ON k.address=c.address WHERE k.address IS NULL AND NOT EXISTS(
  SELECT 1 FROM address_recheck_caps x WHERE x.address=c.address)"""

NEW_NEW_COUNT = "SELECT COUNT(*) n,MIN(first_seen_at) oldest FROM new_balance_addresses"

OLD_LATEST = """SELECT chain,run_id,active_rpc,cooldown_sec,cursor,safe_head,ts
FROM chain_samples WHERE id IN (SELECT MAX(id) FROM chain_samples
WHERE role IN ('live','balance','discovery+balance') GROUP BY chain)"""

NEW_LATEST = """SELECT s.chain,s.run_id,s.active_rpc,s.cooldown_sec,s.cursor,s.safe_head,s.ts
FROM chain_samples s JOIN (SELECT chain,MAX(sample_id) sample_id
FROM latest_chain_sample_ids WHERE role IN ('live','balance','discovery+balance')
GROUP BY chain) latest ON latest.sample_id=s.id"""


def benchmark(label: str, connection, old_sql: str, new_sql: str, repeats: int) -> None:
    def sample(sql: str) -> tuple[list[tuple], list[float]]:
        times = []
        values = []
        for _ in range(repeats):
            started = time.perf_counter()
            values = [tuple(row) for row in connection.execute(sql)]
            times.append((time.perf_counter() - started) * 1000)
        return values, times

    old, old_ms = sample(old_sql)
    new, new_ms = sample(new_sql)
    if label == "latest_chain":
        old, new = sorted(old), sorted(new)
    if label != "new_count" and old != new:
        raise RuntimeError(f"{label}: result mismatch ({len(old)} old, {len(new)} new)")
    if label == "new_count" and old != new:
        print(f"{label}: count differs (legacy query included noncanonical rows): {old} vs {new}")
    p95 = lambda samples: sorted(samples)[math.ceil(len(samples) * 0.95) - 1]
    print(f"{label}: old p95={p95(old_ms):.1f}ms, new p95={p95(new_ms):.1f}ms; rows={len(new)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("contracts_copy", type=Path)
    parser.add_argument("monitoring_copy", type=Path)
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()
    if args.contracts_copy.resolve() == Path("data/contracts.db").resolve():
        parser.error("refusing to migrate the default live contracts DB")
    if args.monitoring_copy.resolve() == Path("data/monitoring.db").resolve():
        parser.error("refusing to migrate the default live monitoring DB")
    if args.repeats < 1:
        parser.error("--repeats must be positive")
    started = time.perf_counter()
    db = DB(args.contracts_copy)
    print(f"contracts copy migration/reconciliation: {time.perf_counter() - started:.1f}s")
    try:
        benchmark("pending", db.conn, OLD_PENDING, NEW_PENDING, args.repeats)
        benchmark("new_count", db.conn, OLD_NEW_COUNT, NEW_NEW_COUNT, args.repeats)
    finally:
        db.close()
    started = time.perf_counter()
    monitor = MonitorStore(args.monitoring_copy)
    print(f"monitoring copy migration: {time.perf_counter() - started:.1f}s")
    try:
        benchmark("latest_chain", monitor.conn, OLD_LATEST, NEW_LATEST, args.repeats)
    finally:
        monitor.close()


if __name__ == "__main__":
    main()
