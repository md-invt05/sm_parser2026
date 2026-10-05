import asyncio
from datetime import datetime, timedelta, timezone

import scan_defi as scanner


PAST = "2020-01-01T00:00:00+00:00"
FUTURE = "2099-01-01T00:00:00+00:00"


def address(number):
    return f"0x{number:040x}"


def add_tasks(db, count, *, start=1, recent=False, priority=10, retry=False):
    rows = []
    contracts = []
    for number in range(start, start + count):
        addr = address(number)
        contracts.append(("ethereum", addr, 100, "0xtx", addr, PAST))
        rows.append((
            "ethereum", addr, 100, 100, 0, priority, PAST, PAST,
            PAST if recent else FUTURE,
            PAST if not recent else FUTURE,
            PAST, PAST, 0 if recent else 1,
            1 if retry and recent else 0,
            1 if retry and not recent else 0,
        ))
    with db.conn:
        db.conn.executemany(
            "INSERT OR IGNORE INTO contracts(chain,address,created_block,created_tx,creator,first_seen_at) "
            "VALUES(?,?,?,?,?,?)", contracts,
        )
        db.conn.executemany(
            """INSERT INTO token_log_tasks(chain,address,first_block,next_block,history_limited,
               priority,due_at,updated_at,recent_due_at,history_due_at,recent_updated_at,
               history_updated_at,recent_complete,recent_failures,history_failures)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows,
        )


def test_50k_history_does_not_block_recent_and_history_still_runs(tmp_path):
    db = scanner.DB(tmp_path / "contracts.db")
    add_tasks(db, 50_000)
    add_tasks(db, 10, start=60_000, recent=True)
    assert db.next_token_log_task(["ethereum"])["_class"] == "recent"
    assert db.next_token_log_task(["ethereum"], prefer_history=True)["_class"] == "historical"
    assert [db.next_token_log_task(["ethereum"], prefer_history=i % 5 == 4)["_class"]
            for i in range(10)].count("historical") == 2
    db.close()


def test_high_value_precedes_retry_and_retry_storm_never_blocks_recent(tmp_path):
    db = scanner.DB(tmp_path / "contracts.db")
    add_tasks(db, 100, retry=True)
    add_tasks(db, 1, start=1000, priority=50)
    assert db.next_token_log_task(["ethereum"])["_class"] == "high_value"
    add_tasks(db, 1, start=2000, recent=True)
    assert db.next_token_log_task(["ethereum"])["_class"] == "recent"
    db.close()


def test_restart_preserves_lanes_and_historical_due_migration(tmp_path):
    path = tmp_path / "contracts.db"
    db = scanner.DB(path)
    add_tasks(db, 1, priority=50)
    db.close()
    reopened = scanner.DB(path)
    task = reopened.next_token_log_task(["ethereum"])
    assert task["_lane"] == "history"
    assert reopened.token_log_task("ethereum", address(1))["history_due_at"] == PAST
    reopened.close()


def test_log_budget_is_unchanged_and_history_can_borrow_idle_capacity():
    async def run():
        budget = scanner.TokenLogBudget(30, 4)
        assert budget.limits() == (30, 4)
        assert await budget.history_available()
        budget.balance_only = True
        assert budget.limits() == (10, 2)
        assert await budget.history_available()
    asyncio.run(run())


def test_raw_history_due_does_not_reduce_normal_live_slots():
    governor = scanner.LoadGovernor(True, 6, 1, allow_balance_only=False)
    assert governor.evaluate(0, 0, 0, now=10, token_pending=50_000) == (6, 1)
    assert governor.state == "normal"
