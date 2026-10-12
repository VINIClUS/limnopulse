from datetime import UTC, datetime

import pytest
from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError
from fastapi import FastAPI
from fastapi.requests import Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from limnopulse_api.adapters.dynamodb_v2 import canonical_request_hash
from limnopulse_api.api.v2.routers import assets as assets_router
from limnopulse_api.api.v2.routers import sites as sites_router
from limnopulse_api.api.v2.schemas.assets import AssetCreate
from limnopulse_api.api.v2.schemas.sites import SiteCreate
from limnopulse_api.auth.dev import DevAuthProvider
from limnopulse_api.core.config import Settings
from limnopulse_api.core.errors import ConflictError
from limnopulse_api.domain.assets import (
    Asset,
    AssetStatus,
    AssetType,
    AssetWithPondProfile,
    PondProfile,
    Site,
    SiteStatus,
)
from limnopulse_api.domain.entities import Membership
from limnopulse_api.domain.ids import default_site_id
from limnopulse_api.domain.roles import TenantRole
from limnopulse_api.domain.v2_common import IdempotencyReceipt
from limnopulse_api.main import create_app

NOW = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)
TENANT = "tnt_a"
SITE_ID = "site_" + "1" * 32
ARCHIVED_SITE_ID = "site_" + "4" * 32
ASSET_ID = "ast_" + "2" * 32
OTHER_ASSET_ID = "ast_" + "3" * 32
HEADERS = {"X-Dev-User-Sub": "sub_1"}


class FakeMembershipService:
    def __init__(self, role: TenantRole) -> None:
        self.membership = Membership(
            tenant_id=TENANT,
            cognito_sub="sub_1",
            role=role,
            created_at=NOW,
            updated_at=NOW,
            version=1,
        )

    async def get_active_membership(self, cognito_sub: str, tenant_id: str) -> Membership | None:
        if (cognito_sub, tenant_id) != (self.membership.cognito_sub, self.membership.tenant_id):
            return None
        return self.membership


def make_site(site_id: str = SITE_ID, status: SiteStatus = SiteStatus.ACTIVE) -> Site:
    return Site(
        tenant_id=TENANT,
        site_id=site_id,
        name="Farm",
        status=status,
        created_at=NOW,
        updated_at=NOW,
        version=1,
    )


def make_asset(tenant_id: str = TENANT, asset_id: str = ASSET_ID) -> AssetWithPondProfile:
    return AssetWithPondProfile(
        asset=Asset(
            tenant_id=tenant_id,
            asset_id=asset_id,
            site_id=SITE_ID,
            asset_type=AssetType.POND,
            name="North",
            status=AssetStatus.ACTIVE,
            created_at=NOW,
            updated_at=NOW,
            version=1,
        ),
        pond_profile=PondProfile(
            tenant_id=tenant_id,
            asset_id=asset_id,
            description="open",
            legacy_pond_id="pond_1",
            created_at=NOW,
            updated_at=NOW,
            version=1,
        ),
    )


class FakeSiteAssetRepository:
    """In-memory repository keyed by tenant; replays receipts like the DynamoDB adapter."""

    def __init__(self) -> None:
        self.sites: dict[tuple[str, str], Site] = {
            (TENANT, SITE_ID): make_site(),
            (TENANT, ARCHIVED_SITE_ID): make_site(ARCHIVED_SITE_ID, SiteStatus.ARCHIVED),
            (TENANT, default_site_id(TENANT)): make_site(default_site_id(TENANT)),
        }
        self.assets: dict[tuple[str, str], AssetWithPondProfile] = {
            (TENANT, ASSET_ID): make_asset(),
            ("tnt_other", OTHER_ASSET_ID): make_asset("tnt_other", OTHER_ASSET_ID),
        }
        self.receipts: dict[tuple[str, str, str], IdempotencyReceipt] = {}
        self.asset_create_count = 0
        self.site_create_count = 0
        self.asset_updates: list[dict] = []

    async def get_idempotency_replay(self, tenant_id, scope, key_hash, request_hash):
        receipt = self.receipts.get((tenant_id, scope, key_hash))
        if receipt is None:
            return None
        if receipt.request_hash != request_hash:
            raise ConflictError("idempotency key reused with a different request")
        return receipt.response_snapshot_json

    async def get_site(self, tenant_id: str, site_id: str) -> Site | None:
        return self.sites.get((tenant_id, site_id))

    async def list_sites(self, tenant_id: str) -> list[Site]:
        return [site for (tenant, _), site in self.sites.items() if tenant == tenant_id]

    async def create_site(self, site: Site, receipt: IdempotencyReceipt) -> Site:
        self.site_create_count += 1
        self.sites[(site.tenant_id, site.site_id)] = site
        self.receipts[(receipt.tenant_id, receipt.scope, receipt.key_hash)] = receipt
        return site

    async def update_site(self, tenant_id, site_id, expected_version, *, name, status):
        current = self.sites[(tenant_id, site_id)]
        updated = current.model_copy(
            update={
                "name": name or current.name,
                "status": status or current.status,
                "version": expected_version + 1,
            }
        )
        self.sites[(tenant_id, site_id)] = updated
        return updated

    async def get_asset(self, tenant_id: str, asset_id: str) -> AssetWithPondProfile | None:
        return self.assets.get((tenant_id, asset_id))

    async def list_assets(self, tenant_id: str) -> list[AssetWithPondProfile]:
        return [asset for (tenant, _), asset in self.assets.items() if tenant == tenant_id]

    async def create_asset(
        self, asset: Asset, profile: PondProfile, receipt: IdempotencyReceipt
    ) -> AssetWithPondProfile:
        self.asset_create_count += 1
        created = AssetWithPondProfile(asset=asset, pond_profile=profile)
        self.assets[(asset.tenant_id, asset.asset_id)] = created
        self.receipts[(receipt.tenant_id, receipt.scope, receipt.key_hash)] = receipt
        return created

    async def update_asset(self, tenant_id, asset_id, expected_version, **changes):
        self.asset_updates.append({"expected_version": expected_version, **changes})
        current = self.assets[(tenant_id, asset_id)]
        asset = current.asset.model_copy(
            update={
                "name": changes["name"] or current.asset.name,
                "status": changes["status"] or current.asset.status,
                "version": expected_version + 1,
            }
        )
        profile = current.pond_profile
        if changes["pond_description"] is not None:
            profile = profile.model_copy(
                update={
                    "description": changes["pond_description"],
                    "version": changes["pond_profile_expected_version"] + 1,
                }
            )
        updated = AssetWithPondProfile(asset=asset, pond_profile=profile)
        self.assets[(tenant_id, asset_id)] = updated
        return updated


class FailingRepository(FakeSiteAssetRepository):
    def __init__(self, error: Exception) -> None:
        super().__init__()
        self.error = error

    async def list_sites(self, tenant_id: str) -> list[Site]:
        raise self.error

    async def get_asset(self, tenant_id: str, asset_id: str) -> AssetWithPondProfile | None:
        raise self.error


async def _handle_infrastructure_error(request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=503, content={"detail": "service unavailable"})


def build_app(role: TenantRole, repository: object | None) -> FastAPI:
    app = FastAPI()
    app.include_router(sites_router.router)
    app.include_router(assets_router.router)
    app.add_exception_handler(ClientError, _handle_infrastructure_error)
    app.add_exception_handler(BotoCoreError, _handle_infrastructure_error)
    app.state.auth_provider = DevAuthProvider()
    app.state.membership_service = FakeMembershipService(role)
    app.state.site_asset_repository = repository
    return app


def app_with_site_asset_repository(
    role: str = "admin",
) -> tuple[FastAPI, FakeSiteAssetRepository]:
    repository = FakeSiteAssetRepository()
    return build_app(TenantRole(role), repository), repository


def client_for(role: str = "admin") -> tuple[TestClient, FakeSiteAssetRepository]:
    app, repository = app_with_site_asset_repository(role)
    return TestClient(app, headers=HEADERS), repository


# --- creates and idempotency ------------------------------------------------------------------


def test_admin_can_create_pond_asset_and_replay_idempotently() -> None:
    app, repository = app_with_site_asset_repository(role="admin")
    payload = {"site_id": "site_" + "1" * 32, "type": "pond", "name": "North"}
    first = TestClient(app).post(
        "/tenants/tnt_a/assets",
        json=payload,
        headers={"Idempotency-Key": "asset-create-01", **HEADERS},
    )
    second = TestClient(app).post(
        "/tenants/tnt_a/assets",
        json=payload,
        headers={"Idempotency-Key": "asset-create-01", **HEADERS},
    )
    assert first.status_code == 201
    assert second.status_code == 201
    assert second.json() == first.json()
    assert repository.asset_create_count == 1
    body = first.json()
    assert body["type"] == "pond"
    assert "asset_type" not in body
    assert body["site_id"] == SITE_ID
    assert body["status"] == "active"
    assert body["schema_version"] == 2
    assert body["version"] == 1
    assert body["created_at"] == body["updated_at"]
    assert len(body["created_at"]) == 27 and body["created_at"].endswith("Z")
    assert body["pond_profile"]["version"] == 1
    assert body["pond_profile"]["legacy_pond_id"] is None


def test_asset_create_request_hash_covers_scope_path_and_body() -> None:
    client, repository = client_for("owner")
    payload = {"site_id": SITE_ID, "type": "pond", "name": "North", "pond_description": "x"}
    response = client.post(
        "/tenants/tnt_a/assets", json=payload, headers={"Idempotency-Key": "asset-create-02"}
    )
    assert response.status_code == 201
    receipt = next(iter(repository.receipts.values()))
    assert receipt.request_hash == canonical_request_hash(
        {
            "scope": "asset-create",
            "path": {"tenant_id": TENANT},
            "body": AssetCreate.model_validate(payload).model_dump(mode="json"),
        }
    )


def test_site_create_replays_and_rejects_key_reuse_with_other_body() -> None:
    client, repository = client_for("owner")
    headers = {"Idempotency-Key": "site-create-01"}
    first = client.post("/tenants/tnt_a/sites", json={"name": "Farm"}, headers=headers)
    second = client.post("/tenants/tnt_a/sites", json={"name": "Farm"}, headers=headers)
    other = client.post("/tenants/tnt_a/sites", json={"name": "Other"}, headers=headers)

    assert first.status_code == 201
    assert second.status_code == 201 and second.json() == first.json()
    assert other.status_code == 409
    assert repository.site_create_count == 1
    receipt = next(iter(repository.receipts.values()))
    assert receipt.request_hash == canonical_request_hash(
        {
            "scope": "site-create",
            "path": {"tenant_id": TENANT},
            "body": SiteCreate(name="Farm").model_dump(mode="json"),
        }
    )
    body = first.json()
    assert body["site_id"].startswith("site_") and len(body["site_id"]) == 37
    assert set(body) == {
        "tenant_id",
        "site_id",
        "name",
        "status",
        "schema_version",
        "created_at",
        "updated_at",
        "version",
    }


def test_asset_key_reuse_with_other_body_conflicts() -> None:
    client, repository = client_for("admin")
    headers = {"Idempotency-Key": "asset-create-03"}
    payload = {"site_id": SITE_ID, "type": "pond", "name": "North"}
    assert client.post("/tenants/tnt_a/assets", json=payload, headers=headers).status_code == 201
    response = client.post(
        "/tenants/tnt_a/assets", json={**payload, "name": "South"}, headers=headers
    )
    assert response.status_code == 409
    assert repository.asset_create_count == 1


@pytest.mark.parametrize("key", [None, "short", "k" * 129])
def test_create_requires_valid_idempotency_key(key: str | None) -> None:
    client, repository = client_for("admin")
    headers = {} if key is None else {"Idempotency-Key": key}
    site = client.post("/tenants/tnt_a/sites", json={"name": "Farm"}, headers=headers)
    asset = client.post(
        "/tenants/tnt_a/assets",
        json={"site_id": SITE_ID, "type": "pond", "name": "North"},
        headers=headers,
    )
    assert (site.status_code, asset.status_code) == (422, 422)
    assert (repository.site_create_count, repository.asset_create_count) == (0, 0)


def test_asset_create_on_unknown_site_is_404_and_archived_site_is_409() -> None:
    client, _ = client_for("admin")
    unknown = client.post(
        "/tenants/tnt_a/assets",
        json={"site_id": "site_" + "9" * 32, "type": "pond", "name": "North"},
        headers={"Idempotency-Key": "asset-create-04"},
    )
    archived = client.post(
        "/tenants/tnt_a/assets",
        json={"site_id": ARCHIVED_SITE_ID, "type": "pond", "name": "North"},
        headers={"Idempotency-Key": "asset-create-05"},
    )
    assert unknown.status_code == 404
    assert archived.status_code == 409


@pytest.mark.parametrize(
    "payload",
    [
        {"site_id": SITE_ID, "type": "tank", "name": "North"},
        {"site_id": SITE_ID, "name": "North"},
        {"site_id": SITE_ID, "asset_type": "pond", "name": "North"},
        {"site_id": SITE_ID, "type": "pond", "name": ""},
        {"site_id": SITE_ID, "type": "pond", "name": "n" * 121},
        {"site_id": "site_1", "type": "pond", "name": "North"},
        {"site_id": SITE_ID, "type": "pond", "name": "North", "tenant_id": "tnt_b"},
        {"site_id": SITE_ID, "type": "pond", "name": "North", "status": "archived"},
    ],
)
def test_asset_create_body_is_strict(payload: dict) -> None:
    client, repository = client_for("admin")
    response = client.post(
        "/tenants/tnt_a/assets", json=payload, headers={"Idempotency-Key": "asset-create-06"}
    )
    assert response.status_code == 422
    assert repository.asset_create_count == 0


@pytest.mark.parametrize("payload", [{"name": ""}, {"name": "Farm", "site_id": SITE_ID}, {}])
def test_site_create_body_is_strict(payload: dict) -> None:
    client, _ = client_for("admin")
    response = client.post(
        "/tenants/tnt_a/sites", json=payload, headers={"Idempotency-Key": "site-create-02"}
    )
    assert response.status_code == 422


# --- reads ------------------------------------------------------------------------------------


@pytest.mark.parametrize("role", ["owner", "admin", "member", "viewer"])
def test_every_read_role_can_list_and_get(role: str) -> None:
    client, _ = client_for(role)
    sites = client.get("/tenants/tnt_a/sites")
    assets = client.get("/tenants/tnt_a/assets")
    assert sites.status_code == 200
    assert {item["site_id"] for item in sites.json()["items"]} == {
        SITE_ID,
        ARCHIVED_SITE_ID,
        default_site_id(TENANT),
    }
    assert assets.status_code == 200
    assert [item["asset_id"] for item in assets.json()["items"]] == [ASSET_ID]
    assert client.get(f"/tenants/tnt_a/sites/{SITE_ID}").status_code == 200
    asset = client.get(f"/tenants/tnt_a/assets/{ASSET_ID}")
    assert asset.status_code == 200
    assert asset.json()["type"] == "pond"
    assert asset.json()["pond_profile"]["description"] == "open"
    assert asset.json()["pond_profile"]["legacy_pond_id"] == "pond_1"


def test_missing_or_cross_tenant_resources_are_404() -> None:
    client, _ = client_for("viewer")
    assert client.get(f"/tenants/tnt_a/assets/{OTHER_ASSET_ID}").status_code == 404
    assert client.get("/tenants/tnt_a/sites/site_" + "9" * 32).status_code == 404
    patch = TestClient(build_app(TenantRole.OWNER, FakeSiteAssetRepository()), headers=HEADERS)
    response = patch.patch(
        f"/tenants/tnt_a/assets/{OTHER_ASSET_ID}", json={"expected_version": 1, "name": "X"}
    )
    assert response.status_code == 404


def test_non_member_is_forbidden() -> None:
    client, _ = client_for("owner")
    assert client.get("/tenants/tnt_b/sites").status_code == 403
    assert client.get("/tenants/tnt_a/sites", headers={"X-Dev-User-Sub": ""}).status_code == 401


@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        ("get", "/tenants/tnt_a/sites/site_1", None),
        ("get", "/tenants/tnt_a/assets/ast_1", None),
        ("get", "/tenants/tnt_a/assets/" + "ast_" + "A" * 32, None),
        ("patch", "/tenants/tnt_a/sites/site_1", {"expected_version": 1, "name": "N"}),
        ("patch", "/tenants/tnt_a/assets/" + "x" * 36, {"expected_version": 1, "name": "N"}),
    ],
)
def test_malformed_path_ids_are_422(method: str, path: str, body: dict | None) -> None:
    client, _ = client_for("owner")
    response = getattr(client, method)(path, **({} if body is None else {"json": body}))
    assert response.status_code == 422


# --- roles ------------------------------------------------------------------------------------


def test_viewer_cannot_patch_site() -> None:
    app, _ = app_with_site_asset_repository(role="viewer")
    response = TestClient(app).patch(
        "/tenants/tnt_a/sites/site_1",
        json={"expected_version": 1, "name": "New"},
        headers=HEADERS,
    )
    assert response.status_code == 403


@pytest.mark.parametrize("role", ["member", "viewer"])
def test_non_writers_cannot_mutate(role: str) -> None:
    client, repository = client_for(role)
    key = {"Idempotency-Key": "write-attempt-01"}
    responses = [
        client.post("/tenants/tnt_a/sites", json={"name": "Farm"}, headers=key),
        client.post(
            "/tenants/tnt_a/assets",
            json={"site_id": SITE_ID, "type": "pond", "name": "North"},
            headers=key,
        ),
        client.patch(f"/tenants/tnt_a/sites/{SITE_ID}", json={"expected_version": 1}),
        client.patch(f"/tenants/tnt_a/assets/{ASSET_ID}", json={"expected_version": 1}),
    ]
    assert [response.status_code for response in responses] == [403, 403, 403, 403]
    assert repository.asset_updates == []


# --- patches ----------------------------------------------------------------------------------


def test_owner_patches_asset_with_both_versions() -> None:
    client, repository = client_for("owner")
    response = client.patch(
        f"/tenants/tnt_a/assets/{ASSET_ID}",
        json={
            "expected_version": 1,
            "name": "East",
            "status": "archived",
            "pond_description": "covered",
            "pond_profile_expected_version": 1,
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert (body["name"], body["status"], body["version"]) == ("East", "archived", 2)
    assert body["pond_profile"]["description"] == "covered"
    assert body["pond_profile"]["version"] == 2
    assert repository.asset_updates == [
        {
            "expected_version": 1,
            "name": "East",
            "status": AssetStatus.ARCHIVED,
            "pond_description": "covered",
            "pond_profile_expected_version": 1,
        }
    ]


@pytest.mark.parametrize(
    "payload",
    [
        {"expected_version": 1, "site_id": SITE_ID},
        {"expected_version": 1, "type": "pond"},
        {"expected_version": 1, "pond_description": "covered"},
        {"expected_version": 1, "pond_profile_expected_version": 1},
        {"expected_version": 0, "name": "East"},
        {"name": "East"},
        {"expected_version": 1, "name": ""},
        {"expected_version": 1, "status": "deleted"},
    ],
)
def test_asset_patch_body_is_strict(payload: dict) -> None:
    client, repository = client_for("admin")
    response = client.patch(f"/tenants/tnt_a/assets/{ASSET_ID}", json=payload)
    assert response.status_code == 422
    assert repository.asset_updates == []


def test_asset_patch_conflicts_are_409() -> None:
    client, repository = client_for("admin")
    stale = client.patch(
        f"/tenants/tnt_a/assets/{ASSET_ID}", json={"expected_version": 5, "name": "East"}
    )
    archive = client.patch(
        f"/tenants/tnt_a/assets/{ASSET_ID}", json={"expected_version": 1, "status": "archived"}
    )
    revive = client.patch(
        f"/tenants/tnt_a/assets/{ASSET_ID}", json={"expected_version": 2, "status": "active"}
    )
    assert (stale.status_code, archive.status_code, revive.status_code) == (409, 200, 409)
    assert len(repository.asset_updates) == 1


def test_site_patch_lifecycle_and_default_site_rules() -> None:
    client, _ = client_for("admin")
    default = client.patch(
        f"/tenants/tnt_a/sites/{default_site_id(TENANT)}",
        json={"expected_version": 1, "status": "archived"},
    )
    revive = client.patch(
        f"/tenants/tnt_a/sites/{ARCHIVED_SITE_ID}",
        json={"expected_version": 1, "status": "active"},
    )
    archive = client.patch(
        f"/tenants/tnt_a/sites/{SITE_ID}",
        json={"expected_version": 1, "name": "Farm 2", "status": "archived"},
    )
    stale = client.patch(f"/tenants/tnt_a/sites/{SITE_ID}", json={"expected_version": 1})
    missing = client.patch("/tenants/tnt_a/sites/site_" + "9" * 32, json={"expected_version": 1})
    strict = client.patch(
        f"/tenants/tnt_a/sites/{SITE_ID}", json={"expected_version": 2, "tenant_id": "tnt_b"}
    )

    assert default.status_code == 409
    assert revive.status_code == 409
    assert archive.status_code == 200
    assert (archive.json()["status"], archive.json()["version"]) == ("archived", 2)
    assert stale.status_code == 409
    assert missing.status_code == 404
    assert strict.status_code == 422


# --- infrastructure ---------------------------------------------------------------------------


def test_missing_repository_state_is_503() -> None:
    client = TestClient(build_app(TenantRole.OWNER, None), headers=HEADERS)
    assert client.get("/tenants/tnt_a/sites").status_code == 503
    assert client.get(f"/tenants/tnt_a/assets/{ASSET_ID}").status_code == 503


@pytest.mark.parametrize(
    "error",
    [
        ClientError(
            {"Error": {"Code": "InternalServerError", "Message": "botocore secret"}}, "Query"
        ),
        EndpointConnectionError(endpoint_url="http://dynamodb.invalid"),
    ],
)
def test_infrastructure_errors_are_503_without_botocore_text(error: Exception) -> None:
    client = TestClient(build_app(TenantRole.OWNER, FailingRepository(error)), headers=HEADERS)
    for path in ("/tenants/tnt_a/sites", f"/tenants/tnt_a/assets/{ASSET_ID}"):
        response = client.get(path)
        assert response.status_code == 503
        assert response.json() == {"detail": "service unavailable"}


def test_v2_routers_are_not_mounted_in_the_application() -> None:
    app = create_app(Settings(app_env="test", auth_mode="dev"))
    paths = {getattr(route, "path", "") for route in app.routes}
    assert not [path for path in paths if path.endswith(("/sites", "/assets"))]
    assert not [path for path in paths if "/sites/" in path or "/assets/" in path]
