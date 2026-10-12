from datetime import UTC, datetime, timedelta

import pytest
from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from limnopulse_api.adapters.dynamodb_deployments import DynamoDeploymentRepository
from limnopulse_api.api.v2.routers import deployments
from limnopulse_api.auth.dev import DevAuthProvider
from limnopulse_api.core.errors import ConflictError
from limnopulse_api.domain.assets import (
    Asset,
    AssetStatus,
    AssetType,
    AssetWithPondProfile,
    PondProfile,
    Site,
)
from limnopulse_api.domain.deployments import Deployment, DeploymentStatus
from limnopulse_api.domain.devices_v2 import ComponentKind, DeviceComponent, ProbeProfile
from limnopulse_api.domain.entities import Membership
from limnopulse_api.domain.ids import default_component_id_for_legacy_device
from limnopulse_api.domain.roles import TenantRole

TENANT = "tnt_a"
OTHER_TENANT = "tnt_b"
DEVICE_ID = "dev_" + "0" * 32
COMPONENT_ID = "cmp_" + "1" * 32
DEPLOYMENT_ID = "dep_" + "2" * 32
SITE_ID = "site_" + "4" * 32
WEST_ASSET_ID = "ast_" + "5" * 32
EAST_ASSET_ID = "ast_" + "6" * 32
ARCHIVED_ASSET_ID = "ast_" + "7" * 32
LEGACY_COMPONENT_ID = default_component_id_for_legacy_device(TENANT, DEVICE_ID)
CREATED = datetime(2026, 8, 1, tzinfo=UTC)
STARTED = "2026-08-25T10:00:00Z"
PATH = f"/tenants/{TENANT}/deployments"
AUTH = {"X-Dev-User-Sub": "sub_1"}


class FakeMembershipService:
    def __init__(self, role: TenantRole) -> None:
        self.role = role

    async def get_active_membership(self, cognito_sub: str, tenant_id: str) -> Membership | None:
        if cognito_sub != "sub_1" or tenant_id != TENANT:
            return None
        return Membership(
            tenant_id=tenant_id,
            cognito_sub=cognito_sub,
            role=self.role,
            created_at=CREATED,
            updated_at=CREATED,
            version=1,
        )


def component(component_id: str = COMPONENT_ID) -> DeviceComponent:
    return DeviceComponent(
        tenant_id=TENANT,
        component_id=component_id,
        device_id=DEVICE_ID,
        kind=ComponentKind.PROBE,
        name="Probe",
        probe_profile=ProbeProfile(probe_profile_id="prb_" + "8" * 32),
        created_at=CREATED,
        updated_at=CREATED,
        version=1,
    )


class FakeComponentReader:
    def __init__(self, *components: DeviceComponent) -> None:
        self.components = {(c.tenant_id, c.component_id): c for c in components}

    async def get_component(self, tenant_id, component_id):
        return self.components.get((tenant_id, component_id))


class FakeSiteAssetReader:
    def __init__(self) -> None:
        stamps = {"created_at": CREATED, "updated_at": CREATED, "version": 1}
        self.site = Site(tenant_id=TENANT, site_id=SITE_ID, name="Farm", **stamps)
        self.assets = {}
        for asset_id, status in (
            (WEST_ASSET_ID, AssetStatus.ACTIVE),
            (EAST_ASSET_ID, AssetStatus.ACTIVE),
            (ARCHIVED_ASSET_ID, AssetStatus.ARCHIVED),
        ):
            self.assets[asset_id] = AssetWithPondProfile(
                asset=Asset(
                    tenant_id=TENANT,
                    asset_id=asset_id,
                    site_id=SITE_ID,
                    asset_type=AssetType.POND,
                    name=asset_id,
                    status=status,
                    **stamps,
                ),
                pond_profile=PondProfile(tenant_id=TENANT, asset_id=asset_id, **stamps),
            )

    async def get_site(self, tenant_id, site_id):
        return self.site if (tenant_id, site_id) == (TENANT, SITE_ID) else None

    async def get_asset(self, tenant_id, asset_id):
        return self.assets.get(asset_id) if tenant_id == TENANT else None


class FakeDeploymentRepository:
    def __init__(self, *, reject_second_start: bool = False) -> None:
        self.rows: dict[tuple[str, str], Deployment] = {}
        self.receipts: dict = {}
        self.reject_second_start = reject_second_start

    async def get_idempotency_replay(self, tenant_id, scope, key_hash, request_hash):
        receipt = self.receipts.get((tenant_id, scope, key_hash))
        if receipt is None:
            return None
        if receipt.request_hash != request_hash:
            raise ConflictError("idempotency key reused with a different request")
        return receipt.response_snapshot_json

    async def get_deployment(self, tenant_id, deployment_id):
        return self.rows.get((tenant_id, deployment_id))

    async def list_component_deployments(self, tenant_id, component_id):
        return sorted(
            (
                d
                for d in self.rows.values()
                if d.tenant_id == tenant_id and d.component_id == component_id
            ),
            key=lambda d: d.started_at,
        )

    async def get_current_deployment(self, tenant_id, component_id):
        if self.reject_second_start:
            # Both concurrent requests observe no pointer; the transaction picks the winner.
            return None
        for d in await self.list_component_deployments(tenant_id, component_id):
            if d.status is DeploymentStatus.ACTIVE:
                return d
        return None

    async def get_deployment_at(self, tenant_id, component_id, observed_at):
        return None

    async def start_deployment(self, deployment, receipt):
        history = await self.list_component_deployments(
            deployment.tenant_id, deployment.component_id
        )
        if any(d.status is DeploymentStatus.ACTIVE for d in history):
            raise ConflictError("component already has an active deployment")
        if history and deployment.started_at < history[-1].ended_at:
            raise ConflictError("deployment overlaps the previous interval")
        self.receipts[(receipt.tenant_id, receipt.scope, receipt.key_hash)] = receipt
        self.rows[(deployment.tenant_id, deployment.deployment_id)] = deployment
        return deployment

    async def end_deployment(self, tenant_id, deployment_id, *, expected_version, ended_at):
        current = self.rows[(tenant_id, deployment_id)]
        ended = Deployment.model_validate(
            {
                **current.model_dump(),
                "ended_at": ended_at,
                "status": DeploymentStatus.ENDED,
                "updated_at": max(datetime.now(UTC), current.updated_at),
                "version": expected_version + 1,
            }
        )
        self.rows[(tenant_id, deployment_id)] = ended
        return ended


class FailingDynamoClient:
    def __init__(self, error: Exception) -> None:
        self.error = error

    def get_item(self, **kwargs):
        raise self.error

    def query(self, **kwargs):
        raise self.error

    def transact_write_items(self, **kwargs):
        raise self.error


async def _handle_infrastructure_error(request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=503, content={"detail": "service unavailable"})


def app_with_deployment_repository(
    role: str = "owner",
    *,
    repository: object | None = None,
    reject_second_start: bool = False,
    skip_state: tuple[str, ...] = (),
) -> FastAPI:
    app = FastAPI()
    app.include_router(deployments.router)
    app.add_exception_handler(BotoCoreError, _handle_infrastructure_error)
    app.add_exception_handler(ClientError, _handle_infrastructure_error)
    app.state.auth_provider = DevAuthProvider()
    app.state.membership_service = FakeMembershipService(TenantRole(role))
    state = {
        "deployment_repository": repository
        if repository is not None
        else FakeDeploymentRepository(reject_second_start=reject_second_start),
        "site_asset_repository": FakeSiteAssetReader(),
        "device_component_repository": FakeComponentReader(
            component(), component(LEGACY_COMPONENT_ID)
        ),
    }
    for name, value in state.items():
        if name not in skip_state:
            setattr(app.state, name, value)
    return app


def client_for(app: FastAPI) -> TestClient:
    return TestClient(app, headers=AUTH)


def start_deployment_request(app_or_client, *, key: str = "deployment-create-01", **overrides):
    client = app_or_client if isinstance(app_or_client, TestClient) else client_for(app_or_client)
    body = {"component_id": COMPONENT_ID, "asset_id": WEST_ASSET_ID, "started_at": STARTED}
    body.update(overrides)
    return client.post(PATH, json=body, headers={"Idempotency-Key": key})


def end_request(client: TestClient, deployment_id: str, **overrides):
    body = {"expected_version": 1, "ended_at": "2026-08-25T11:00:00Z"}
    body.update(overrides)
    return client.post(f"{PATH}/{deployment_id}/end", json=body)


# Router shape ----------------------------------------------------------------------------------


def test_router_is_unmounted_and_unprefixed() -> None:
    from limnopulse_api.main import create_app

    paths = {route.path for route in deployments.router.routes}
    assert paths == {
        "/tenants/{tenant_id}/deployments",
        "/tenants/{tenant_id}/deployments/{deployment_id}/end",
    }
    mounted = {getattr(route, "path", "") for route in create_app().routes}
    assert not any("deployments" in path for path in mounted)


def test_schema_names_do_not_collide_with_v1_components() -> None:
    app = app_with_deployment_repository()
    names = set(app.openapi()["components"]["schemas"])
    assert {"V2DeploymentResponse", "V2DeploymentStartRequest", "V2DeploymentEndRequest"} <= names


# List ------------------------------------------------------------------------------------------


def test_list_requires_component_id() -> None:
    app = app_with_deployment_repository(role="viewer")
    assert client_for(app).get(PATH).status_code == 422


def test_list_rejects_malformed_and_unknown_components() -> None:
    client = client_for(app_with_deployment_repository(role="viewer"))
    assert client.get(PATH, params={"component_id": "cmp_1"}).status_code == 422
    unknown = client.get(PATH, params={"component_id": "cmp_" + "f" * 32})
    assert unknown.status_code == 404
    assert unknown.json() == {"detail": "not found"}


@pytest.mark.parametrize("role", ["owner", "admin", "member", "viewer"])
def test_every_member_role_can_list(role: str) -> None:
    client = client_for(app_with_deployment_repository(role=role))
    response = client.get(PATH, params={"component_id": COMPONENT_ID})
    assert response.status_code == 200
    assert response.json() == {"items": []}


def test_non_member_tenant_is_forbidden() -> None:
    client = client_for(app_with_deployment_repository())
    response = client.get(
        f"/tenants/{OTHER_TENANT}/deployments", params={"component_id": COMPONENT_ID}
    )
    assert response.status_code == 403


# Start -----------------------------------------------------------------------------------------


def test_start_returns_the_created_deployment_with_derived_site() -> None:
    client = client_for(app_with_deployment_repository(role="admin"))
    response = start_deployment_request(client)

    assert response.status_code == 201
    body = response.json()
    assert body["deployment_id"].startswith("dep_")
    assert body["site_id"] == SITE_ID
    assert body["asset_id"] == WEST_ASSET_ID
    assert body["component_id"] == COMPONENT_ID
    assert body["started_at"] == "2026-08-25T10:00:00.000000Z"
    assert body["ended_at"] is None
    assert body["status"] == "active"
    assert body["version"] == 1
    assert body["schema_version"] == 2

    listed = client.get(PATH, params={"component_id": COMPONENT_ID}).json()["items"]
    assert [item["deployment_id"] for item in listed] == [body["deployment_id"]]


@pytest.mark.parametrize("role", ["viewer", "member"])
def test_non_writer_cannot_start_or_end(role: str) -> None:
    client = client_for(app_with_deployment_repository(role=role))
    assert start_deployment_request(client).status_code == 403
    assert end_request(client, DEPLOYMENT_ID).status_code == 403


def test_start_requires_an_idempotency_key() -> None:
    client = client_for(app_with_deployment_repository())
    body = {"component_id": COMPONENT_ID, "asset_id": WEST_ASSET_ID, "started_at": STARTED}
    assert client.post(PATH, json=body).status_code == 422
    assert start_deployment_request(client, key="short").status_code == 422


@pytest.mark.parametrize(
    "overrides",
    [
        {"site_id": SITE_ID},
        {"tenant_id": TENANT},
        {"started_at": "2026-08-25T10:00:00"},
        {"started_at": "not-a-date"},
        {"component_id": "cmp_1"},
        {"asset_id": "pond_1"},
        {"started_at": "2999-01-01T00:00:00Z"},
    ],
)
def test_start_rejects_invalid_bodies(overrides) -> None:
    client = client_for(app_with_deployment_repository())
    assert start_deployment_request(client, **overrides).status_code == 422


def test_invalid_json_number_in_body_is_422_not_500() -> None:
    client = client_for(app_with_deployment_repository())
    response = client.post(
        PATH,
        content=(
            f'{{"component_id": "{COMPONENT_ID}", "asset_id": "{WEST_ASSET_ID}", '
            '"started_at": NaN}'
        ),
        headers={"Idempotency-Key": "deployment-create-01", "Content-Type": "application/json"},
    )
    assert response.status_code == 422
    end = client.post(
        f"{PATH}/{DEPLOYMENT_ID}/end",
        content='{"expected_version": Infinity, "ended_at": "2026-08-25T11:00:00Z"}',
        headers={"Content-Type": "application/json"},
    )
    assert end.status_code == 422


@pytest.mark.parametrize(
    ("overrides", "status_code", "detail"),
    [
        ({"component_id": "cmp_" + "f" * 32}, 404, "not found"),
        ({"asset_id": "ast_" + "f" * 32}, 404, "not found"),
        ({"asset_id": ARCHIVED_ASSET_ID}, 409, "asset is archived"),
        (
            {"component_id": LEGACY_COMPONENT_ID},
            409,
            "legacy default component deployments are managed through /v1",
        ),
    ],
)
def test_start_maps_parent_errors(overrides, status_code, detail) -> None:
    response = start_deployment_request(client_for(app_with_deployment_repository()), **overrides)
    assert response.status_code == status_code
    assert response.json() == {"detail": detail}


def test_same_key_same_body_replays_and_different_body_conflicts() -> None:
    client = client_for(app_with_deployment_repository())
    first = start_deployment_request(client)
    replay = start_deployment_request(client)
    assert (first.status_code, replay.status_code) == (201, 201)
    assert replay.content == first.content

    conflict = start_deployment_request(client, asset_id=EAST_ASSET_ID)
    assert conflict.status_code == 409
    assert conflict.json() == {"detail": "idempotency key reused with a different request"}


def test_second_start_while_active_conflicts() -> None:
    client = client_for(app_with_deployment_repository())
    assert start_deployment_request(client).status_code == 201
    second = start_deployment_request(client, key="deployment-create-02", asset_id=EAST_ASSET_ID)
    assert second.status_code == 409
    assert second.json() == {"detail": "component already has an active deployment"}


def test_concurrent_start_has_one_winner() -> None:
    app = app_with_deployment_repository(role="owner", reject_second_start=True)
    first = start_deployment_request(app, key="deployment-create-01")
    second = start_deployment_request(app, key="deployment-create-02")
    assert sorted([first.status_code, second.status_code]) == [201, 409]


# End and relocation ----------------------------------------------------------------------------


def test_move_west_to_east_ends_then_starts_at_the_exact_boundary() -> None:
    client = client_for(app_with_deployment_repository())
    west = start_deployment_request(client).json()

    ended = end_request(client, west["deployment_id"])
    assert ended.status_code == 200
    assert ended.json()["status"] == "ended"
    assert ended.json()["ended_at"] == "2026-08-25T11:00:00.000000Z"
    assert ended.json()["version"] == 2
    assert ended.json()["asset_id"] == WEST_ASSET_ID

    east = start_deployment_request(
        client,
        key="deployment-create-02",
        asset_id=EAST_ASSET_ID,
        started_at="2026-08-25T11:00:00Z",
    )
    assert east.status_code == 201

    overlap = start_deployment_request(
        client,
        key="deployment-create-03",
        asset_id=WEST_ASSET_ID,
        started_at="2026-08-25T10:30:00Z",
    )
    assert overlap.status_code == 409

    listed = client.get(PATH, params={"component_id": COMPONENT_ID}).json()["items"]
    assert [(item["asset_id"], item["status"]) for item in listed] == [
        (WEST_ASSET_ID, "ended"),
        (EAST_ASSET_ID, "active"),
    ]


def test_end_errors() -> None:
    client = client_for(app_with_deployment_repository())
    deployment_id = start_deployment_request(client).json()["deployment_id"]

    assert end_request(client, "dep_1").status_code == 422
    assert end_request(client, "dep_" + "f" * 32).status_code == 404
    assert end_request(client, deployment_id, ended_at="2026-08-25T11:00:00").status_code == 422
    assert end_request(client, deployment_id, ended_at=STARTED).status_code == 422
    assert end_request(client, deployment_id, ended_at="2999-01-01T00:00:00Z").status_code == 422
    assert end_request(client, deployment_id, expected_version=0).status_code == 422
    stale = end_request(client, deployment_id, expected_version=7)
    assert stale.status_code == 409
    assert stale.json() == {"detail": "deployment version conflict"}

    assert end_request(client, deployment_id).status_code == 200
    again = end_request(client, deployment_id, expected_version=2)
    assert again.status_code == 409
    assert again.json() == {"detail": "deployment is not active"}


# Infrastructure --------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "missing",
    ["deployment_repository", "site_asset_repository", "device_component_repository"],
)
def test_missing_repository_is_503(missing: str) -> None:
    client = client_for(app_with_deployment_repository(skip_state=(missing,)))
    response = client.get(PATH, params={"component_id": COMPONENT_ID})
    assert response.status_code == 503
    assert response.json() == {"detail": "service unavailable"}


@pytest.mark.parametrize(
    "error",
    [
        ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "secret"}},
            "TransactWriteItems",
        ),
        EndpointConnectionError(endpoint_url="http://dynamodb.internal"),
    ],
)
def test_dynamodb_failures_are_503_without_botocore_text(error: Exception) -> None:
    repository = DynamoDeploymentRepository("LimnopulseDomain", FailingDynamoClient(error))
    client = TestClient(app_with_deployment_repository(repository=repository), headers=AUTH)

    responses = [
        start_deployment_request(client),
        end_request(client, DEPLOYMENT_ID),
        client.get(PATH, params={"component_id": COMPONENT_ID}),
    ]

    for response in responses:
        assert response.status_code == 503
        assert response.json() == {"detail": "service unavailable"}
        assert "secret" not in response.text
        assert "dynamodb.internal" not in response.text


def test_end_far_in_the_past_is_still_validated_against_start() -> None:
    client = client_for(app_with_deployment_repository())
    deployment_id = start_deployment_request(client).json()["deployment_id"]
    before = (datetime(2026, 8, 25, 10, tzinfo=UTC) - timedelta(days=1)).isoformat()
    assert end_request(client, deployment_id, ended_at=before).status_code == 422
