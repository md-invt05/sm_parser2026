import asyncio

import pytest

from sui_grpc_wire_pb2 import (
    GetServiceInfoResponse, ListTransactionsResponse, QUERY_END_REASON_CHECKPOINT_BOUND,
    QUERY_END_REASON_ITEM_LIMIT, QUERY_END_REASON_SCAN_LIMIT,
)
from sui_support import (
    SuiConfig, SuiGrpcClient, SuiGrpcError, SuiStore, discover_sui_grpc_once,
    sui_discovery_loop, sui_grpc_retry_delay,
)


def test_onfinality_host_uses_api_key_metadata_and_safe_pacing():
    async def run():
        client = SuiGrpcClient("not-a-real-key", SuiConfig(),
                               endpoint="sui.api.onfinality.io:443")
        try:
            assert client.metadata == (("api-key", "not-a-real-key"),)
            start = asyncio.get_running_loop().time()
            await client._pace_request()
            await client._pace_request()
            assert asyncio.get_running_loop().time() - start >= 0.045
        finally:
            await client.close()
        with pytest.raises(SuiGrpcError, match="host:port"):
            SuiGrpcClient("key", SuiConfig(), endpoint="https://example.com/key")
    asyncio.run(run())


def test_onfinality_quota_backoff_is_not_a_probe_storm(monkeypatch):
    monkeypatch.setattr("sui_support.random.uniform", lambda _a, _b: 1.0)
    assert [sui_grpc_retry_delay("UNAVAILABLE", n) for n in (1, 2, 3)] == [30, 60, 120]
    assert [sui_grpc_retry_delay("RESOURCE_EXHAUSTED", n)
            for n in (1, 2, 3, 4, 5)] == [900, 1800, 3600, 7200, 7200]


def test_bounded_range_requires_terminal_marker_before_all_cursors_advance(tmp_path):
    class FakeRanges:
        info = {"head": 20, "lowest": 18, "history_start": 18, "chain": "mainnet"}

        def __init__(self, terminal=True):
            self.terminal = terminal
            self.calls = []

        async def preflight(self):
            return self.info

        async def list_range(self, start, end, after=None):
            self.calls.append((start, end, after))
            yield frame(b"a", "tx-18", 18)
            yield frame(b"b", "tx-20", 20)
            if self.terminal:
                yield frame(b"done", end=QUERY_END_REASON_CHECKPOINT_BOUND)

    async def run():
        store = SuiStore(tmp_path / "sui.db")
        broken = FakeRanges(False)
        with pytest.raises(SuiGrpcError, match="QueryEnd"):
            await discover_sui_grpc_once(
                store, broken, SuiConfig(grpc_checkpoint_chunk=10),
            )
        assert store.state()["verified_next_checkpoint"] == 18
        assert store.conn.execute("SELECT COUNT(*) FROM sui_seen_transactions").fetchone()[0] == 2
        good = FakeRanges()
        result = await discover_sui_grpc_once(
            store, good, SuiConfig(grpc_checkpoint_chunk=10),
        )
        assert good.calls == [(18, 21, None)]
        assert result["verified_checkpoints"] == 3
        assert result["new_transactions"] == 0
        assert store.state()["verified_next_checkpoint"] == 21
        store.close()
    asyncio.run(run())


def test_preflight_finds_thirty_day_boundary_without_starting_at_retention_floor():
    async def run():
        import time

        client = SuiGrpcClient("dummy", SuiConfig(discovery_days=30),
                               endpoint="sui.api.onfinality.io:443")
        cutoff = time.time() - 30 * 86400

        async def info(_request, **_kwargs):
            return GetServiceInfoResponse(
                chain="mainnet", checkpoint_height=20,
                lowest_available_checkpoint=10,
            )

        async def transactions(_request, **_kwargs):
            yield frame(b"end", end=QUERY_END_REASON_CHECKPOINT_BOUND)

        async def timestamp(checkpoint):
            return cutoff + checkpoint - 15 + 0.5

        client._info = info
        client._transactions = transactions
        client.checkpoint_timestamp = timestamp
        try:
            result = await client.preflight()
            assert result["lowest"] == 10
            assert result["history_start"] == 15
        finally:
            await client.close()
    asyncio.run(run())


def test_preflight_rejects_provider_that_only_accepts_one_item_probe():
    async def run():
        client = SuiGrpcClient("dummy", SuiConfig(grpc_stream_limit=500),
                               endpoint="sui.api.onfinality.io:443")
        calls = []

        async def info(_request, **_kwargs):
            return GetServiceInfoResponse(
                chain="mainnet", checkpoint_height=20,
                lowest_available_checkpoint=10,
            )

        async def transactions(request, **_kwargs):
            calls.append(request.options.limit)
            if request.options.limit > 1:
                raise SuiGrpcError("response limit")
            yield frame(b"end", end=QUERY_END_REASON_CHECKPOINT_BOUND)

        client._info = info
        client._transactions = transactions
        try:
            with pytest.raises(SuiGrpcError, match="response limit"):
                await client.preflight()
            assert calls == [1, 500]
            assert client._preflight_cache is None
        finally:
            await client.close()
    asyncio.run(run())


def test_preflight_requires_checkpoint_bound_not_just_item_limit():
    async def run():
        client = SuiGrpcClient("dummy", SuiConfig(grpc_stream_limit=10),
                               endpoint="sui.api.onfinality.io:443")

        async def info(_request, **_kwargs):
            return GetServiceInfoResponse(
                chain="mainnet", checkpoint_height=20,
                lowest_available_checkpoint=10,
            )

        async def transactions(request, **_kwargs):
            if request.start_checkpoint == 10:
                yield frame(b"floor", end=QUERY_END_REASON_CHECKPOINT_BOUND)
            else:
                yield frame(b"same", end=QUERY_END_REASON_ITEM_LIMIT)

        client._info = info
        client._transactions = transactions
        try:
            with pytest.raises(SuiGrpcError, match="did not advance"):
                await client.preflight()
            assert client._preflight_cache is None
        finally:
            await client.close()
    asyncio.run(run())


def frame(cursor=b"x", digest=None, checkpoint=None, end=None):
    result = ListTransactionsResponse(watermark={"cursor": cursor})
    if digest is not None:
        result.transaction.digest = digest
        result.transaction.checkpoint = checkpoint
    if end is not None:
        result.end.reason = end
    return result


class FakeGrpc:
    def __init__(self, pages, head=10, lowest=10):
        self.pages = pages
        self.info = {"head": head, "lowest": lowest, "chain": "mainnet"}
        self.calls = []

    async def preflight(self):
        return self.info

    async def list_checkpoint(self, checkpoint, after=None):
        self.calls.append((checkpoint, after))
        for item in self.pages[(checkpoint, after)]:
            if isinstance(item, BaseException):
                raise item
            yield item


def test_terminal_checkpoint_and_legacy_cursor_never_promoted(tmp_path):
    async def run():
        store = SuiStore(tmp_path / "sui.db")
        store.update_state(1000, 0, True, "old Blockberry maximum")
        client = FakeGrpc({(10, None): [frame(b"a", "tx-1", 10),
                                         frame(b"b", end=QUERY_END_REASON_CHECKPOINT_BOUND)]})
        result = await discover_sui_grpc_once(store, client, SuiConfig(grpc_checkpoint_chunk=1))
        assert result["verified_checkpoints"] == 1
        assert store.state()["verified_start_checkpoint"] == 10
        assert store.state()["verified_next_checkpoint"] == 11
        assert store.state()["last_checkpoint"] == 1000
        assert store.state()["source_mode"] == "grpc_verified_from_start"
        store.close()
    asyncio.run(run())


@pytest.mark.parametrize("failure", [SuiGrpcError("broken stream"), TimeoutError("timed out")])
def test_partial_stream_never_advances_and_restart_replays_without_duplicate(tmp_path, failure):
    async def run():
        path = tmp_path / "sui.db"
        store = SuiStore(path)
        cfg = SuiConfig(grpc_checkpoint_chunk=1)
        bad = FakeGrpc({(10, None): [frame(b"a", "tx-1", 10), failure]})
        with pytest.raises(type(failure)):
            await discover_sui_grpc_once(store, bad, cfg)
        assert store.state()["verified_next_checkpoint"] == 10
        assert store.conn.execute("SELECT COUNT(*) FROM sui_seen_transactions").fetchone()[0] == 1
        store.close()
        reopened = SuiStore(path)
        good = FakeGrpc({(10, None): [frame(b"a", "tx-1", 10),
                                        frame(b"b", end=QUERY_END_REASON_CHECKPOINT_BOUND)]})
        result = await discover_sui_grpc_once(reopened, good, cfg)
        assert result["new_transactions"] == 0
        assert reopened.state()["verified_next_checkpoint"] == 11
        assert reopened.conn.execute("SELECT COUNT(*) FROM sui_seen_transactions").fetchone()[0] == 1
        reopened.close()
    asyncio.run(run())


def test_empty_checkpoint_and_sequential_checkpoints_have_no_gap(tmp_path):
    async def run():
        store = SuiStore(tmp_path / "sui.db")
        client = FakeGrpc({
            (10, None): [frame(b"empty", end=QUERY_END_REASON_CHECKPOINT_BOUND)],
            (11, None): [frame(b"tx", "tx-11", 11),
                         frame(b"end", end=QUERY_END_REASON_CHECKPOINT_BOUND)],
        }, head=11)
        result = await discover_sui_grpc_once(store, client, SuiConfig(grpc_checkpoint_chunk=2))
        assert result["verified_checkpoints"] == 2
        assert client.calls == [(10, None), (11, None)]
        assert store.state()["verified_next_checkpoint"] == 12
        store.close()
    asyncio.run(run())


def test_item_limit_resumes_from_terminal_watermark_only(tmp_path):
    async def run():
        store = SuiStore(tmp_path / "sui.db")
        client = FakeGrpc({
            (10, None): [frame(b"first", "tx-1", 10),
                         frame(b"page", "tx-2", 10, QUERY_END_REASON_ITEM_LIMIT)],
            (10, b"page"): [frame(b"done", end=QUERY_END_REASON_CHECKPOINT_BOUND)],
        })
        await discover_sui_grpc_once(store, client, SuiConfig(grpc_checkpoint_chunk=1))
        assert client.calls == [(10, None), (10, b"page")]
        assert store.state()["verified_next_checkpoint"] == 11
        assert store.state()["grpc_resume_watermark"] is None
        store.close()
    asyncio.run(run())


def test_repeated_watermark_and_premature_eof_never_commit(tmp_path):
    async def run():
        store = SuiStore(tmp_path / "sui.db")
        repeated = FakeGrpc({
            (10, None): [frame(b"page", end=QUERY_END_REASON_SCAN_LIMIT)],
            (10, b"page"): [frame(b"page", end=QUERY_END_REASON_SCAN_LIMIT)],
        })
        with pytest.raises(SuiGrpcError, match="watermark"):
            await discover_sui_grpc_once(store, repeated, SuiConfig(grpc_checkpoint_chunk=1))
        assert store.state()["verified_next_checkpoint"] == 10
        assert store.state()["grpc_resume_watermark"] == b"page"
        premature = FakeGrpc({(10, None): [frame(b"x", "tx-x", 10)]})
        with pytest.raises(SuiGrpcError, match="QueryEnd"):
            await discover_sui_grpc_once(store, premature, SuiConfig(grpc_checkpoint_chunk=1))
        assert store.state()["verified_next_checkpoint"] == 10
        store.close()
    asyncio.run(run())


def test_failed_preflight_keeps_blockberry_best_effort_unverified(tmp_path):
    class FailingGrpc:
        async def preflight(self):
            raise SuiGrpcError("provider unavailable")

    class Berry:
        async def transactions(self, page, size):
            return {"content": []}

        async def packages(self, page, size):
            return {"content": []}

        def take_metrics(self):
            return {"requests": 0, "successes": 0, "errors": 0,
                    "latency_p50_ms": None, "latency_p95_ms": None, "errors_by_type": {}}

    async def run():
        store = SuiStore(tmp_path / "sui.db")
        await sui_discovery_loop(store, Berry(), SuiConfig(), asyncio.Event(),
                                 once=True, grpc_client=FailingGrpc())
        assert store.state()["verified_next_checkpoint"] is None
        assert store.state()["source_mode"] == "coverage_unverified"
        store.close()
    asyncio.run(run())


def test_tip_checkpoint_and_persisted_catchup_gap_join_without_holes(tmp_path):
    async def run():
        path = tmp_path / "sui.db"
        store = SuiStore(path)
        pages = {(height, None): [
            frame(str(height).encode(), f"tx-{height}", height),
            frame(b"end", end=QUERY_END_REASON_CHECKPOINT_BOUND),
        ] for height in range(10, 21)}
        client = FakeGrpc(pages, head=20, lowest=10)
        cfg = SuiConfig(grpc_checkpoint_chunk=3)
        tip = await discover_sui_grpc_once(store, client, cfg)
        assert tip["verified_checkpoints"] == 3
        assert client.calls == [(18, None), (19, None), (20, None)]
        state = store.state()
        assert state["verified_next_checkpoint"] == 10
        assert state["grpc_tip_next_checkpoint"] == 21
        assert state["source_mode"] == "grpc_partial"
        assert tuple(store.conn.execute(
            "SELECT start_checkpoint,end_checkpoint,next_checkpoint,status "
            "FROM sui_checkpoint_gaps"
        ).fetchone()) == (10, 17, 10, "active")
        store.close()

        # Restart cannot recreate the same gap or promote the old Blockberry max.
        reopened = SuiStore(path)
        reopened.update_state(9999, 0, True, "old Blockberry maximum")
        for _ in range(4):
            await discover_sui_grpc_once(reopened, client, cfg)
        state = reopened.state()
        assert state["verified_next_checkpoint"] == 21
        assert state["grpc_tip_next_checkpoint"] == 21
        assert state["source_mode"] == "grpc_verified_from_start"
        assert state["last_checkpoint"] == 9999
        assert reopened.conn.execute(
            "SELECT COUNT(*) FROM sui_checkpoint_gaps WHERE status='active'"
        ).fetchone()[0] == 0
        assert reopened.conn.execute(
            "SELECT COUNT(*) FROM sui_seen_transactions"
        ).fetchone()[0] == 11
        reopened.close()
    asyncio.run(run())


def test_tip_stream_error_keeps_gap_and_both_verified_boundaries(tmp_path):
    async def run():
        store = SuiStore(tmp_path / "sui.db")
        client = FakeGrpc({(18, None): [frame(b"tx", "tx-18", 18),
                                        SuiGrpcError("stream broken")]},
                          head=20, lowest=10)
        with pytest.raises(SuiGrpcError, match="stream broken"):
            await discover_sui_grpc_once(store, client, SuiConfig(grpc_checkpoint_chunk=3))
        state = store.state()
        assert state["verified_next_checkpoint"] == 10
        assert state["grpc_tip_next_checkpoint"] == 18
        assert store.conn.execute(
            "SELECT next_checkpoint FROM sui_checkpoint_gaps WHERE status='active'"
        ).fetchone()[0] == 10
        assert store.conn.execute(
            "SELECT COUNT(*) FROM sui_seen_transactions"
        ).fetchone()[0] == 1
        store.close()
    asyncio.run(run())


def test_provider_retention_cannot_silently_skip_unfinished_gap(tmp_path):
    store = SuiStore(tmp_path / "sui.db")
    assert store.grpc_schedule(10, 20, 3) == ("tip", 18, 21)
    with pytest.raises(SuiGrpcError, match="retention"):
        store.grpc_schedule(11, 21, 3)
    assert store.state()["verified_next_checkpoint"] == 10
    assert store.conn.execute(
        "SELECT next_checkpoint FROM sui_checkpoint_gaps WHERE status='active'"
    ).fetchone()[0] == 10
    store.close()
