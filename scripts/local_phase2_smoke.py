"""Bounded, offline discovery/Sui/token integration smoke on a disposable SQLite DB."""

from __future__ import annotations

import asyncio
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scan_defi import DB  # noqa: E402
from sui_grpc_wire_pb2 import ListTransactionsResponse, QUERY_END_REASON_CHECKPOINT_BOUND  # noqa: E402
from sui_support import SuiConfig, SuiStore, discover_sui_grpc_once  # noqa: E402


async def run(path: Path) -> None:
    db = DB(path)
    db.init_chain("ethereum", 100, True)
    db.init_chain_cursors("ethereum", 100, 99, lookback=0)
    address = "0x" + "aa" * 20
    observed = "2026-01-01T00:00:00+00:00"
    contract = ("ethereum", address, 100, "0xold", address, observed)
    source = ("ethereum", address, "direct_deploy", 100, "0xold", address, observed)
    db.commit_index_range("ethereum", "live", 100, [contract], [source], [], [], [],
                          expected_start=100, enqueue_token_logs=True,
                          block_hashes=[(100, "0xoldhash")])
    assert db.next_token_log_task(["ethereum"])["address"] == address
    assert db.cursor("ethereum", "live")["next_block"] == 101

    sui = SuiStore(path)

    class FakeGrpc:
        async def preflight(self):
            return {"head": 8, "lowest": 8, "chain": "mainnet"}

        async def list_checkpoint(self, checkpoint, after=None):
            frame = ListTransactionsResponse(watermark={"cursor": b"done"})
            frame.transaction.digest = "smoke-digest"
            frame.transaction.checkpoint = checkpoint
            yield frame
            terminal = ListTransactionsResponse(watermark={"cursor": b"end"})
            terminal.end.reason = QUERY_END_REASON_CHECKPOINT_BOUND
            yield terminal

    result = await discover_sui_grpc_once(sui, FakeGrpc(),
                                          SuiConfig(grpc_checkpoint_chunk=1))
    assert result["verified_checkpoints"] == 1
    assert sui.state()["verified_next_checkpoint"] == 9
    assert sui.conn.execute("SELECT COUNT(*) FROM sui_seen_transactions").fetchone()[0] == 1
    sui.close()
    db.close()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix=".phase2-smoke-", dir=ROOT) as directory:
        asyncio.run(run(Path(directory) / "smoke.db"))
    print("offline local smoke: discovery cursor, token task and Sui verified checkpoint OK")
