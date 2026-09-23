import asyncio
import json
import sqlite3
from pathlib import Path

import httpx

from sui_support import (
    BlockberryClient,
    BlockberryError,
    SuiConfig,
    SuiStore,
    balance_sui_once,
    normalize_coin_type,
    normalize_sui_id,
    parse_raw_transaction,
    scan_sui_package,
    discover_sui_once,
    export_sui_xlsx,
    sui_balance_loop,
)


def test_normalize_sui_identifiers():
    assert normalize_sui_id("0x2") == "0x" + "0" * 63 + "2"
    assert normalize_coin_type("0x2::sui::SUI") == "0x" + "0" * 63 + "2::sui::SUI"


def test_raw_transaction_extracts_calls_publish_and_only_state_objects():
    package = "0x" + "a" * 64
    raw = {
        "result": {
            "transaction": {"data": {"sender": "0x5", "transaction": {
                "transactions": [
                    {"MoveCall": {"package": package, "module": "pool", "function": "swap"}},
                    {"Publish": {"modules": []}},
                ]
            }}},
            "objectChanges": [
                {"type": "published", "packageId": package},
                {"type": "created", "objectId": "0x10", "objectType": package + "::pool::Pool",
                 "owner": {"Shared": {"initial_shared_version": "1"}}},
                {"type": "mutated", "objectId": "0x11", "objectType": package + "::vault::Vault",
                 "owner": {"ObjectOwner": "0x10"}},
                {"type": "created", "objectId": "0x12", "objectType": package + "::user::Position",
                 "owner": {"AddressOwner": "0x9"}},
            ],
        }
    }
    parsed = parse_raw_transaction(raw)
    assert parsed["calls"] == {package}
    assert parsed["published"] == {package}
    assert {row["object_id"] for row in parsed["objects"]} == {
        normalize_sui_id("0x10"), normalize_sui_id("0x11")
    }
    assert {row["owner_kind"] for row in parsed["objects"]} == {"shared", "object"}


def test_sui_migration_and_deduplication(tmp_path: Path):
    db_path = tmp_path / "contracts.db"
    sqlite3.connect(db_path).execute("CREATE TABLE legacy(id INTEGER)").connection.close()
    store = SuiStore(db_path)
    package = "0x" + "a" * 64
    store.upsert_package(package, "active_call", 10, "tx1")
    store.upsert_package(package, "active_call", 11, "tx2")
    store.add_objects([{
        "package_id": package, "object_id": normalize_sui_id("0x10"),
        "object_type": package + "::pool::Pool", "owner_kind": "shared",
        "parent_object": None,
    }], 10)
    store.add_objects([{
        "package_id": package, "object_id": normalize_sui_id("0x10"),
        "object_type": package + "::pool::Pool", "owner_kind": "shared",
        "parent_object": None,
    }], 11)
    assert store.summary()["packages"] == 1
    assert store.summary()["objects"] == 1
    assert store.conn.execute("SELECT COUNT(*) FROM legacy").fetchone()[0] == 0
    assert store.conn.execute("SELECT last_checkpoint FROM sui_packages").fetchone()[0] == 11
    store.close()
    # Idempotent second migration/open.
    SuiStore(db_path).close()


class FakeBalanceClient:
    def __init__(self, rows_by_owner):
        self.rows_by_owner = rows_by_owner

    async def account_balance(self, owner):
        return self.rows_by_owner[owner]


def test_package_scan_is_legacy_and_never_calls_account_balance(tmp_path: Path):
    store = SuiStore(tmp_path / "db.sqlite")
    package = "0x" + "a" * 64
    store.upsert_package(package, "active_call", 1, "tx")
    client = FakeBalanceClient({})
    result = asyncio.run(scan_sui_package(store, client, package, 500000))
    assert result["status"] == "incomplete"
    assert result["total_usd"] == 0
    assert "package/shared-object" in store.conn.execute(
        "SELECT note FROM sui_package_scans"
    ).fetchone()[0]
    store.close()


def test_project_tvl_is_deduplicated_across_package_versions(tmp_path: Path):
    store = SuiStore(tmp_path / "db.sqlite")
    package_a = "0x" + "a" * 64
    package_b = "0x" + "b" * 64
    store.sync_defi_projects([{
        "projectName": "Cetus", "currTvl": 1_250_000,
        "packages": [{"packageAddress": package_a}, {"packageAddress": package_b}],
    }], 500_000)
    projects = store.latest_projects()
    assert len(projects) == 1
    assert projects[0]["indexed_tvl"] == 1_250_000
    assert len(__import__("json").loads(projects[0]["packages_json"])) == 2
    store.close()


def test_blockberry_balance_accepts_array_response():
    async def run():
        client = BlockberryClient("secret", SuiConfig(max_retries=1))
        client.client = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _request: httpx.Response(200, json=[{
                "coinType": "0x2::sui::SUI", "balance": "1",
            }])),
            base_url="https://example.invalid",
        )
        try:
            rows = await client.account_balance("0x5")
            assert len(rows) == 1
        finally:
            await client.client.aclose()
    asyncio.run(run())


def test_discovery_paginates_past_tenth_page_until_saved_digest(tmp_path: Path):
    store = SuiStore(tmp_path / "db.sqlite")
    store.add_seen("known", 1, None)
    calls = []

    class Client:
        async def transactions(self, page, size):
            calls.append(page)
            digest = "known" if page == 12 else f"tx-{page}"
            return {"content": [{
                "txStatus": "SUCCESS", "checkpoint": 100 - page,
                "timestamp": 4_102_444_800_000, "txHash": digest,
                "packagesMetadata": [],
            }]}

        async def packages(self, page, size):
            return {"content": []}

    result = asyncio.run(discover_sui_once(
        store, Client(), SuiConfig(max_pages=20, raw_enrich_per_pass=0),
    ))
    assert calls[-1] == 12
    assert result["window_limited"] is False
    store.close()


def test_blockberry_429_retries_without_leaking_key():
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, headers={"retry-after": "0"})
        return httpx.Response(200, json={"content": [{"checkpoint": 123}]})

    async def run():
        cfg = SuiConfig(max_retries=2)
        client = BlockberryClient("secret-key", cfg)
        client.client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler), base_url="https://example.invalid",
            headers={"x-api-key": "secret-key"},
        )
        try:
            report = await client.preflight()
            assert report["head"] == 123
        finally:
            await client.client.aclose()

    asyncio.run(run())
    # One rate-limited transaction request, its retry, then the DEX capability probe.
    assert calls == 3


def test_blockberry_preflight_skips_non_object_transaction_rows():
    async def run():
        client = BlockberryClient("secret", SuiConfig(max_retries=1))

        async def transactions(_page, _size):
            return {"content": [None, "temporary-index-item", {"checkpoint": "123"}]}

        async def dex_page(_page, _size):
            return {"content": []}

        client.transactions = transactions
        client._dex_page = dex_page
        report = await client.preflight()
        assert report["ok"] is True
        assert report["head"] == 123
        assert report["discovery_ok"] is True

    asyncio.run(run())


def test_blockberry_dex_contract_and_pagination():
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        query = dict(request.url.params)
        body = json.loads(request.content)
        assert request.method == "POST"
        assert request.url.path == "/dex"
        assert query["size"] == "100"
        assert query["orderBy"] == "DESC"
        assert query["period"] == "DAY"
        assert query["sortBy"] == "CURRENT_TVL"
        assert body == {"withTvlOnly": False}
        page = int(query["page"])
        return httpx.Response(200, json={
            "content": [{"projectName": f"DEX {page}", "currTvl": page + 1,
                         "packages": [{"packageId": "0x2"}]}],
            "totalPages": 2, "last": page == 1,
        })

    async def run():
        client = BlockberryClient("secret", SuiConfig(max_pages=5, page_size=100))
        client.client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler), base_url="https://example.invalid",
        )
        try:
            rows = await client.defi_projects()
            assert [row["projectName"] for row in rows] == ["DEX 0", "DEX 1"]
        finally:
            await client.client.aclose()

    asyncio.run(run())
    assert len(requests) == 2


def test_blockberry_dex_pools_contract():
    def handler(request: httpx.Request) -> httpx.Response:
        query = dict(request.url.params)
        assert request.method == "POST"
        assert request.url.path == "/dex/pools"
        assert query == {
            "page": "3", "size": "25", "orderBy": "DESC",
            "period": "DAY", "sortBy": "LIQUIDITY_IN_USD",
        }
        assert json.loads(request.content) == {
            "poolFactoryId": [], "poolFactoryEmpty": True,
        }
        return httpx.Response(200, json={"content": [{"poolId": "0x10"}]})

    async def run():
        client = BlockberryClient("secret", SuiConfig())
        client.client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler), base_url="https://example.invalid",
        )
        try:
            assert len(await client.dex_pools(3, 25)) == 1
        finally:
            await client.client.aclose()

    asyncio.run(run())


def test_blockberry_empty_pool_page_is_a_valid_end_of_pagination():
    async def run():
        client = BlockberryClient("secret", SuiConfig())
        client.client = httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(200, json={"content": [], "last": True})
            ),
            base_url="https://example.invalid",
        )
        try:
            assert await client.dex_pools(1, 100) == []
        finally:
            await client.client.aclose()
    asyncio.run(run())


def test_blockberry_client_errors_keep_status_and_endpoint():
    for status, kind in ((400, "bad_request"), (404, "not_found"), (405, "method_not_allowed")):
        async def run():
            client = BlockberryClient("secret", SuiConfig(max_retries=1))
            client.client = httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda _request: httpx.Response(status, json={"error": "schema"})
                ),
                base_url="https://example.invalid",
            )
            try:
                try:
                    await client._dex_page(0, 1)
                except BlockberryError as exc:
                    assert exc.kind == kind
                    assert exc.status_code == status
                    assert exc.endpoint == "/dex"
                else:
                    raise AssertionError("client error was accepted")
            finally:
                await client.client.aclose()
        asyncio.run(run())


def test_blockberry_auth_malformed_and_server_retry():
    async def auth_run():
        client = BlockberryClient("secret", SuiConfig(max_retries=3))
        calls = 0
        def handler(_request):
            nonlocal calls
            calls += 1
            return httpx.Response(403, json={"error": "denied"})
        client.client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler), base_url="https://example.invalid",
        )
        try:
            try:
                await client._dex_page(0, 1)
            except BlockberryError as exc:
                assert exc.kind == "auth"
                assert exc.permanent
            else:
                raise AssertionError("auth error was accepted")
            assert calls == 1
        finally:
            await client.client.aclose()

    async def malformed_run():
        client = BlockberryClient("secret", SuiConfig(max_retries=1))
        client.client = httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(200, content=b"{")
            ),
            base_url="https://example.invalid",
        )
        try:
            try:
                await client._dex_page(0, 1)
            except BlockberryError as exc:
                assert exc.kind == "malformed_json"
            else:
                raise AssertionError("malformed response was accepted")
        finally:
            await client.client.aclose()

    async def retry_run():
        calls = 0
        def handler(_request):
            nonlocal calls
            calls += 1
            if calls == 1:
                return httpx.Response(503, json={"error": "temporary"})
            return httpx.Response(200, json={
                "content": [{"projectName": "Cetus"}], "last": True,
            })
        client = BlockberryClient("secret", SuiConfig(max_retries=2))
        client.client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler), base_url="https://example.invalid",
        )
        try:
            assert len(await client.defi_projects()) == 1
            assert calls == 2
        finally:
            await client.client.aclose()

    asyncio.run(auth_run())
    asyncio.run(malformed_run())
    asyncio.run(retry_run())


def test_incomplete_dex_pagination_is_rejected():
    async def run():
        client = BlockberryClient("secret", SuiConfig(max_pages=1, page_size=1))
        client.client = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _request: httpx.Response(200, json={
                "content": [{"projectName": "Cetus"}],
                "totalPages": 2, "last": False,
            })),
            base_url="https://example.invalid",
        )
        try:
            try:
                await client.defi_projects()
            except BlockberryError as exc:
                assert exc.kind == "partial"
            else:
                raise AssertionError("partial pagination was accepted")
        finally:
            await client.client.aclose()
    asyncio.run(run())


def test_partial_defi_sync_preserves_tvl_and_marks_snapshot_incomplete(tmp_path: Path):
    store = SuiStore(tmp_path / "db.sqlite")
    package = "0x" + "a" * 64
    store.sync_defi_projects([{
        "projectName": "Cetus", "currTvl": 1_250_000,
        "packages": [{"packageAddress": package}],
    }], 500_000)

    class Client:
        async def defi_projects(self):
            raise BlockberryError("partial", "page 2 missing", endpoint="/dex")

    async def run():
        try:
            await sui_balance_loop(
                store, Client(), SuiConfig(defi_sync_sec=60), 500_000,
                asyncio.Event(), once=True,
            )
        except BlockberryError:
            pass
        else:
            raise AssertionError("partial snapshot was accepted")

    asyncio.run(run())
    row = store.latest_projects()[0]
    assert row["indexed_tvl"] == 1_250_000
    assert row["provider_complete"] == 0
    assert "failed" in row["note"].lower()
    store.close()


def test_project_tvl_is_not_increased_by_verified_pool_liquidity(tmp_path: Path):
    store = SuiStore(tmp_path / "db.sqlite")
    package = "0x" + "a" * 64
    pool = "0x" + "b" * 64
    store.sync_defi_projects([{
        "projectName": "Cetus", "currTvl": 1_250_000,
        "packages": [{"packageAddress": package}],
    }], 500_000)
    store.save_pools([{
        "poolId": pool, "projectName": "Cetus", "liquidityInUsd": 500_000,
    }])
    store.save_pool_verification(pool, {"data": {"type": "pool", "owner": {"Shared": {}}}})
    row = store.latest_projects()[0]
    assert row["indexed_tvl"] == 1_250_000
    assert row["verified_pool_tvl"] == 500_000
    store.close()


def test_qualifying_export_does_not_build_full_sui_bundle(tmp_path: Path):
    store = SuiStore(tmp_path / "db.sqlite")
    package = "0x" + "a" * 64
    store.sync_defi_projects([{
        "projectName": "Cetus", "currTvl": 1_250_000,
        "packages": [{"packageAddress": package}],
    }], 500_000)
    paths = export_sui_xlsx(
        store, tmp_path / "reports", 500_000, mode="qualifying"
    )
    assert paths[0].exists()
    assert not paths[1].exists()
    assert not paths[2].exists()
    assert not paths[3].exists()
    store.close()


def test_alchemy_balance_fallback_stays_unpriced_and_gets_no_blockberry_key():
    fallback_headers = []

    def primary(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"error": "down"})

    def fallback(request: httpx.Request) -> httpx.Response:
        fallback_headers.append(dict(request.headers))
        return httpx.Response(200, json={
            "jsonrpc": "2.0", "id": 1,
            "result": [{"coinType": "0x2::sui::SUI", "totalBalance": "42"}],
        })

    async def run():
        client = BlockberryClient("blockberry-secret", SuiConfig(max_retries=1),
                                  fallback_urls=["https://alchemy.invalid"])
        client.client = httpx.AsyncClient(
            transport=httpx.MockTransport(primary), base_url="https://blockberry.invalid",
            headers={"x-api-key": "blockberry-secret"},
        )
        client.fallback_client = httpx.AsyncClient(transport=httpx.MockTransport(fallback))
        try:
            rows = await client.account_balance("0x2")
            assert rows[0]["balance"] == "42"
            assert rows[0]["balanceUsd"] is None
        finally:
            await client.client.aclose()
            await client.fallback_client.aclose()

    asyncio.run(run())
    assert "x-api-key" not in fallback_headers[0]
