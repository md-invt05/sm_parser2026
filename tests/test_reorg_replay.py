import asyncio

import pytest

import scan_defi as scanner


CHAIN = "ethereum"
ORPHAN = "0x" + "aa" * 20
SURVIVOR = "0x" + "bb" * 20
NOW = "2026-01-01T00:00:00+00:00"


def seeded_db(path, end=104):
    db = scanner.DB(path)
    db.init_chain(CHAIN, 100, True)
    db.init_chain_cursors(CHAIN, 100, 99, lookback=0)
    contract = (CHAIN, ORPHAN, 103, "0xold", SURVIVOR, NOW)
    discovery = (CHAIN, ORPHAN, "direct_deploy", 103, "0xold", SURVIVOR, NOW)
    db.commit_index_range(
        CHAIN, "live", end, [contract], [discovery], [], [], [],
        expected_start=100, enqueue_token_logs=True,
        block_hashes=[(number, f"0x{number:064x}") for number in range(100, end + 1)],
    )
    return db


@pytest.mark.parametrize("ancestor,conflict", [(103, 104), (100, 101)])
def test_one_and_multiblock_reorg_quarantine_and_replay(tmp_path, ancestor, conflict):
    db = seeded_db(tmp_path / "contracts.db")
    old_cursor = int(db.cursor(CHAIN, "live")["next_block"])
    db.halt_discovery_on_reorg(CHAIN, "live", None, conflict)
    assert db.cursor(CHAIN, "live")["next_block"] == old_cursor
    replay_id = db.begin_reorg_replay(CHAIN, ancestor, conflict)
    gap = db.next_discovery_gap(CHAIN)
    assert gap["id"] == replay_id
    assert gap["next_block"] == ancestor + 1
    assert db.cursor(CHAIN, "live")["status"] == "reorg_replay"
    assert db.conn.execute(
        "SELECT canonical FROM contracts WHERE address=?", (ORPHAN,)
    ).fetchone()[0] == (1 if ancestor >= 103 else 0)
    assert (ORPHAN in db.pending_addresses(0)) == (ancestor >= 103)
    assert db.conn.execute(
        "SELECT COUNT(*) FROM token_log_tasks WHERE address=?", (ORPHAN,)
    ).fetchone()[0] == 1  # Audit task retained.

    # A failed required stage cannot advance the replay gap.
    assert db.discovery_gap(replay_id)["next_block"] == ancestor + 1
    rows = [] if ancestor >= 103 else [(CHAIN, SURVIVOR, 104, "0xnew", SURVIVOR, NOW)]
    sources = [] if ancestor >= 103 else [
        (CHAIN, SURVIVOR, "direct_deploy", 104, "0xnew", SURVIVOR, NOW)
    ]
    db.commit_index_range(
        CHAIN, "catchup", 104, rows, sources, [], [], [],
        expected_start=ancestor + 1, gap_id=replay_id, enqueue_token_logs=True,
        block_hashes=[(number, f"0x{number + 1000:064x}")
                      for number in range(ancestor + 1, 105)],
    )
    assert db.discovery_gap(replay_id)["status"] == "complete"
    assert db.cursor(CHAIN, "live")["status"] == "active"
    assert db.cursor(CHAIN, "live")["next_block"] == old_cursor
    if ancestor < 103:
        assert db.conn.execute(
            "SELECT canonical FROM contracts WHERE address=?", (ORPHAN,)
        ).fetchone()[0] == 0
        assert ORPHAN not in db.contract_addresses(CHAIN)
        assert SURVIVOR in db.contract_addresses(CHAIN)
        assert db.pending_addresses(0) == [SURVIVOR]
    db.close()


def test_replay_same_contract_restores_canonical_without_duplicates(tmp_path):
    db = seeded_db(tmp_path / "contracts.db")
    db.halt_discovery_on_reorg(CHAIN, "live", None, 102)
    replay_id = db.begin_reorg_replay(CHAIN, 101, 102)
    contract = (CHAIN, ORPHAN, 104, "0xreplacement", SURVIVOR, NOW)
    discovery = (CHAIN, ORPHAN, "direct_deploy", 104, "0xreplacement", SURVIVOR, NOW)
    db.commit_index_range(
        CHAIN, "catchup", 104, [contract], [discovery], [], [], [],
        expected_start=102, gap_id=replay_id, enqueue_token_logs=True,
        block_hashes=[(n, f"0x{n+1000:064x}") for n in range(102, 105)],
    )
    assert db.conn.execute("SELECT COUNT(*) FROM contracts WHERE chain=?", (CHAIN,)).fetchone()[0] == 1
    assert db.conn.execute("SELECT COUNT(*) FROM contract_discoveries WHERE chain=?", (CHAIN,)).fetchone()[0] == 1
    assert db.conn.execute("SELECT COUNT(*) FROM token_log_tasks WHERE chain=?", (CHAIN,)).fetchone()[0] == 1
    assert db.conn.execute(
        "SELECT canonical,created_block,created_tx FROM contracts WHERE address=?", (ORPHAN,)
    ).fetchone()[:] == (1, 104, "0xreplacement")
    assert db.conn.execute(
        "SELECT canonical,observed_block FROM contract_discoveries WHERE address=?", (ORPHAN,)
    ).fetchone()[:] == (1, 104)
    db.close()


def test_orphan_token_link_and_code_cache_are_not_trusted_during_replay(tmp_path):
    db = seeded_db(tmp_path / "contracts.db")
    token = "0x" + "cc" * 20
    with db.conn:
        db.conn.execute(
            "INSERT INTO tokens(chain,address,source) VALUES(?,?,?)", (CHAIN, token, "transfer_log"),
        )
        db.conn.execute(
            "INSERT INTO contract_tokens(chain,contract,token,source,first_seen_block,last_seen_block) "
            "VALUES(?,?,?,?,?,?)", (CHAIN, ORPHAN, token, "transfer_log", 103, 103),
        )
        db.conn.execute(
            "INSERT INTO contract_code_cache(chain,address,has_code,checked_at,checked_block) "
            "VALUES(?,?,?,?,?)", (CHAIN, ORPHAN, 1, NOW, 103),
        )
    assert [row["address"] for row in db.tokens_for_contract(CHAIN, ORPHAN)] == [token]
    db.halt_discovery_on_reorg(CHAIN, "live", None, 102)
    replay_id = db.begin_reorg_replay(CHAIN, 101, 102)
    assert db.tokens_for_contract(CHAIN, ORPHAN) == []
    assert db.conn.execute(
        "SELECT COUNT(*) FROM contract_code_cache WHERE chain=? AND address=?",
        (CHAIN, ORPHAN),
    ).fetchone()[0] == 0
    assert db.token_log_task(CHAIN, ORPHAN)["next_block"] == 102
    db.commit_index_range(CHAIN, "catchup", 104, [], [], [], [], [],
                          expected_start=102, gap_id=replay_id)
    assert db.tokens_for_contract(CHAIN, ORPHAN) == []
    db.close()


def test_restart_during_replay_preserves_gap_and_resume(tmp_path):
    path = tmp_path / "contracts.db"
    db = seeded_db(path)
    db.halt_discovery_on_reorg(CHAIN, "live", None, 102)
    replay_id = db.begin_reorg_replay(CHAIN, 101, 102)
    db.commit_index_range(
        CHAIN, "catchup", 102, [], [], [], [], [],
        expected_start=102, gap_id=replay_id, block_hashes=[(102, "0xreplaced")],
    )
    db.close()
    reopened = scanner.DB(path)
    assert reopened.next_discovery_gap(CHAIN)["next_block"] == 103
    reopened.commit_index_range(
        CHAIN, "catchup", 104, [], [], [], [], [],
        expected_start=103, gap_id=replay_id,
        block_hashes=[(103, "0xnew103"), (104, "0xnew104")],
    )
    assert reopened.cursor(CHAIN, "live")["status"] == "active"
    reopened.close()


def test_reorg_replay_has_priority_over_existing_catchup_and_resumes_it(tmp_path):
    db = seeded_db(tmp_path / "contracts.db")
    with db.conn:
        db.conn.execute(
            "UPDATE chain_cursors SET status='active',anchor_block=200 "
            "WHERE chain=? AND role='backfill'", (CHAIN,),
        )
    ordinary_gap = db.reanchor_tip(CHAIN, 200, 1000, lookback=0)
    assert ordinary_gap is not None
    assert db.next_discovery_gap(CHAIN)["id"] == ordinary_gap
    db.halt_discovery_on_reorg(CHAIN, "live", ordinary_gap, 102)
    replay_id = db.begin_reorg_replay(CHAIN, 101, 102)
    assert db.next_discovery_gap(CHAIN)["id"] == replay_id
    assert db.discovery_gap(ordinary_gap)["status"] == "reorg_replay"
    db.commit_index_range(
        CHAIN, "catchup", 104, [], [], [], [], [], expected_start=102,
        gap_id=replay_id,
        block_hashes=[(n, f"0x{n+1000:064x}") for n in range(102, 105)],
    )
    assert db.next_discovery_gap(CHAIN)["id"] == ordinary_gap
    assert db.discovery_gap(ordinary_gap)["next_block"] == 105
    assert db.cursor(CHAIN, "backfill")["status"] == "active"
    assert db.cursor(CHAIN, "live")["status"] == "active"
    db.close()


def test_ancestor_lookup_fail_closed_for_deep_reorg_and_rpc_error(tmp_path):
    async def run():
        db = seeded_db(tmp_path / "contracts.db")

        class Rpc:
            def __init__(self, fail=False):
                self.fail = fail

            async def call(self, method, params):
                if self.fail:
                    raise scanner.RpcError("network", "offline")
                number = int(params[0], 16)
                return {"number": hex(number), "hash": "0x" + "ff" * 32}

        assert await scanner.find_reorg_common_ancestor(db, CHAIN, Rpc(), 104) is None
        with pytest.raises(scanner.RpcError):
            await scanner.find_reorg_common_ancestor(db, CHAIN, Rpc(fail=True), 104)
        assert db.cursor(CHAIN, "live")["next_block"] == 105
        db.halt_discovery_on_reorg(CHAIN, "live", None, 104)
        assert db.pending_reorg_conflict(CHAIN) == 104
        db.mark_reorg_manual(CHAIN, 104)
        assert db.pending_reorg_conflict(CHAIN) is None
        assert db.cursor(CHAIN, "live")["status"] == "reorg_alert"
        db.close()
    asyncio.run(run())


def test_no_reorg_finds_nearest_common_ancestor(tmp_path):
    async def run():
        db = seeded_db(tmp_path / "contracts.db")

        class Rpc:
            async def call(self, method, params):
                number = int(params[0], 16)
                return {"number": hex(number),
                        "hash": f"0x{number:064x}" if number <= 101 else "0xchanged"}

        assert await scanner.find_reorg_common_ancestor(db, CHAIN, Rpc(), 103) == 101
        db.close()
    asyncio.run(run())
