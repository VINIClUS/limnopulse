"""M1 release gate (Epic #16, Task M1-08): one file proving every M1 acceptance invariant.

A failure here is corrected in the owning M1 task (M1-05, M1-06 or M1-07); never weaken the
gate or regenerate the `/v1` golden to hide drift.
"""

import ast
import asyncio
import importlib.util
import inspect
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from limnopulse_api.adapters.dynamodb_integrations import integration_account_to_item
from limnopulse_api.adapters.dynamodb_v1_pond_device_projection import (
    DynamoV1PondDeviceProjection,
)
from limnopulse_api.api.dependencies import require_tenant_access
from limnopulse_api.api.openapi_contract import build_v1_openapi_contract, iter_effective_api_routes
from limnopulse_api.api.v2.schemas.integrations import (
    DeviceIntegrationCreate,
    IntegrationAccountCreate,
)
from limnopulse_api.core.config import Settings
from limnopulse_api.core.errors import ConflictError
from limnopulse_api.domain.deployments import DeploymentStatus
from limnopulse_api.domain.devices_v2 import ComponentKind, DeviceV2
from limnopulse_api.domain.entities import Device, Membership
from limnopulse_api.domain.ids import (
    asset_id_for_legacy_pond,
    default_component_id_for_legacy_device,
    default_site_id,
    deployment_id_for_legacy_device,
)
from limnopulse_api.domain.integrations import DeviceIntegration, IntegrationAccountStatus
from limnopulse_api.domain.roles import TenantRole
from limnopulse_api.domain.v2_common import QuotaDimension, new_idempotency_receipt
from limnopulse_api.main import create_app
from limnopulse_api.migrations.default_site_asset import DefaultSiteAssetMigrator
from limnopulse_api.repositories.v2_contracts import (
    DeploymentRepository,
    DeviceComponentRepository,
    IntegrationRepository,
    SiteAssetRepository,
)
from limnopulse_api.services.devices_v2 import DeviceV2CreateCommand, DeviceV2Service
from limnopulse_api.services.integrations import IntegrationService
from tests.contracts.m1_fakes import (
    TABLE,
    ConditionalDynamoClient,
    DynamoM1Repositories,
    InMemoryM1Repositories,
    M1AcceptanceScenario,
)

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src/limnopulse_api"
GOLDEN = ROOT / "tests/contracts/openapi/v1.json"
NO_SCAN_GUARD = ROOT / "tests/unit/test_no_scan_guard.py"
TENANT = "tnt_1"
WEST_POND = "pond_west"
EAST_POND = "pond_east"
LEGACY_DEVICE = "dev_" + "1" * 32
LEGACY_CREATED = datetime(2026, 3, 4, 5, 6, 7, tzinfo=UTC)
NOW = datetime(2026, 8, 25, 12, tzinfo=UTC)
HEADERS = {"X-Dev-User-Sub": "sub_1"}
M1_STATE = (
    "site_asset_repository",
    "device_component_repository",
    "integration_repository",
    "deployment_repository",
)
M1_V2_OPERATIONS = {
    ("/v2/tenants/{tenant_id}/sites", "get"),
    ("/v2/tenants/{tenant_id}/sites", "post"),
    ("/v2/tenants/{tenant_id}/sites/{site_id}", "get"),
    ("/v2/tenants/{tenant_id}/sites/{site_id}", "patch"),
    ("/v2/tenants/{tenant_id}/assets", "get"),
    ("/v2/tenants/{tenant_id}/assets", "post"),
    ("/v2/tenants/{tenant_id}/assets/{asset_id}", "get"),
    ("/v2/tenants/{tenant_id}/assets/{asset_id}", "patch"),
    ("/v2/tenants/{tenant_id}/devices", "get"),
    ("/v2/tenants/{tenant_id}/devices", "post"),
    ("/v2/tenants/{tenant_id}/devices/{device_id}", "get"),
    ("/v2/tenants/{tenant_id}/devices/{device_id}", "patch"),
    ("/v2/tenants/{tenant_id}/devices/{device_id}/components", "get"),
    ("/v2/tenants/{tenant_id}/devices/{device_id}/components", "post"),
    ("/v2/tenants/{tenant_id}/devices/{device_id}/integrations", "get"),
    ("/v2/tenants/{tenant_id}/devices/{device_id}/integrations", "post"),
    ("/v2/tenants/{tenant_id}/components/{component_id}", "get"),
    ("/v2/tenants/{tenant_id}/components/{component_id}", "patch"),
    ("/v2/tenants/{tenant_id}/components/{component_id}/capabilities", "get"),
    ("/v2/tenants/{tenant_id}/components/{component_id}/capabilities", "post"),
    ("/v2/tenants/{tenant_id}/integration-accounts", "get"),
    ("/v2/tenants/{tenant_id}/integration-accounts", "post"),
    ("/v2/tenants/{tenant_id}/deployments", "get"),
    ("/v2/tenants/{tenant_id}/deployments", "post"),
    ("/v2/tenants/{tenant_id}/deployments/{deployment_id}/end", "post"),
}
V1_TENANT_ROUTE_COUNT = 28
FORBIDDEN_CORE_WORDS = ("redis", "aws_iot", "thing_name", "certificate_arn", "shadow_name")
M1_MODULES = (
    "repositories/v2_contracts.py",
    "repositories/v1_pond_device_projection.py",
    "adapters/dynamodb_v2.py",
    "adapters/dynamodb_sites_assets.py",
    "adapters/dynamodb_devices_v2.py",
    "adapters/dynamodb_integrations.py",
    "adapters/dynamodb_deployments.py",
    "adapters/dynamodb_v1_pond_device_projection.py",
    "adapters/dynamodb_default_site_asset_migration.py",
    "services/sites.py",
    "services/assets.py",
    "services/devices_v2.py",
    "services/components.py",
    "services/capabilities.py",
    "services/integrations.py",
    "services/deployments.py",
    "services/v1_pond_device_projection.py",
    "migrations/default_site_asset.py",
)


# Local helpers -----------------------------------------------------------------------------------


class FakeViewerMembershipService:
    async def get_active_membership(self, cognito_sub: str, tenant_id: str) -> Membership | None:
        if (cognito_sub, tenant_id) != ("sub_1", TENANT):
            return None
        return Membership(
            tenant_id=tenant_id,
            cognito_sub=cognito_sub,
            role=TenantRole.VIEWER,
            created_at=NOW,
            updated_at=NOW,
            version=1,
        )


class FakeUnreachableLegacyRepository:
    """With the projection flag on, `/v1` Pond/Device routes must never fall back here."""

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"legacy repository reached with the projection flag on: {name}")


@pytest.fixture(autouse=True)
def _isolate_flag_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("API_V2_ENABLED", raising=False)
    monkeypatch.delenv("V1_POND_DEVICE_PROJECTION_ENABLED", raising=False)


class Clock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


def dt(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(UTC)


def settings(**overrides: object) -> Settings:
    return Settings(_env_file=None, app_env="test", auth_mode="dev", **overrides)


def legacy_tenant_item(tenant_id: str = TENANT) -> dict[str, Any]:
    return {
        "PK": f"TENANT#{tenant_id}",
        "SK": "META",
        "entity_type": "tenant",
        "tenant_id": tenant_id,
        "name": "Fazenda Azul",
        "settings": {},
        "status": "active",
        "created_at": LEGACY_CREATED.isoformat(),
        "updated_at": LEGACY_CREATED.isoformat(),
        "version": 1,
        "schema_version": 1,
    }


def legacy_pond_item(pond_id: str, *, tenant_id: str = TENANT) -> dict[str, Any]:
    return {
        "PK": f"TENANT#{tenant_id}",
        "SK": f"POND#{pond_id}",
        "entity_type": "pond",
        "tenant_id": tenant_id,
        "pond_id": pond_id,
        "name": f"Viveiro {pond_id}",
        "description": None,
        "status": "active",
        "created_at": LEGACY_CREATED.isoformat(),
        "updated_at": LEGACY_CREATED.isoformat(),
        "version": 1,
        "schema_version": 1,
    }


def legacy_device_items(device_id: str, pond_id: str, *, tenant_id: str = TENANT) -> list[dict]:
    body = {
        "tenant_id": tenant_id,
        "pond_id": pond_id,
        "device_id": device_id,
        "name": "Gateway",
        "auth_type": "mtls",
        "firmware_version": "1.0.0",
        "status": "active",
        "created_at": LEGACY_CREATED.isoformat(),
        "updated_at": LEGACY_CREATED.isoformat(),
        "version": 1,
        "schema_version": 1,
    }
    return [
        {"PK": f"TENANT#{tenant_id}", "SK": f"DEVICE#{device_id}", "entity_type": "device", **body},
        {"PK": f"DEVICE#{device_id}", "SK": "META", "entity_type": "device_lookup", **body},
    ]


def legacy_store() -> ConditionalDynamoClient:
    """One tenant with two legacy Ponds and one legacy Device placed in the West Pond."""
    client = ConditionalDynamoClient()
    client.seed(legacy_tenant_item())
    client.seed(legacy_pond_item(WEST_POND))
    client.seed(legacy_pond_item(EAST_POND))
    for item in legacy_device_items(LEGACY_DEVICE, WEST_POND):
        client.seed(item)
    return client


def migrate(client: ConditionalDynamoClient, *, apply: bool, clock: datetime = NOW):
    return DefaultSiteAssetMigrator(
        client, table_name=TABLE, clock=lambda: clock, sleep=lambda _delay: None
    ).run([TENANT], apply=apply, page_size=25, deadline_seconds=300)


def migrated_store() -> ConditionalDynamoClient:
    client = legacy_store()
    assert migrate(client, apply=True).result == "success"
    return client


async def _no_sleep(attempt: int) -> None:
    return None


def v1_projection(client: ConditionalDynamoClient, clock: Clock) -> DynamoV1PondDeviceProjection:
    return DynamoV1PondDeviceProjection(TABLE, client, clock=clock, retry_sleep=_no_sleep)


def scenario(backend: str, clock: Clock) -> M1AcceptanceScenario:
    if backend == "in_memory":
        return M1AcceptanceScenario.in_memory(clock=clock)
    return M1AcceptanceScenario.over_dynamo(ConditionalDynamoClient(), clock=clock)


def dependency_calls(dependant):
    yield dependant.call
    for dependency in dependant.dependencies:
        yield from dependency_calls(dependency)


def tenant_routes(app) -> list:
    return [route for route in iter_effective_api_routes(app) if "{tenant_id}" in route.path_format]


def projection_app(client: ConditionalDynamoClient, *, missing: str | None = None):
    app = create_app(settings(api_v2_enabled=True, v1_pond_device_projection_enabled=True))
    app.state.membership_service = FakeViewerMembershipService()
    app.state.domain_repository = FakeUnreachableLegacyRepository()
    for name, repository in (
        DynamoM1Repositories.over(client, clock=lambda: NOW).app_state().items()
    ):
        if name != missing:
            setattr(app.state, name, repository)
    return app


# 1) A Gateway exposes two distinct Probe Components ------------------------------------------------


@pytest.mark.parametrize("backend", ["in_memory", "dynamo"])
def test_gateway_can_expose_multiple_probes(backend: str) -> None:
    store = scenario(backend, Clock(NOW))
    gateway = store.create_device(TENANT, "Gateway")
    first = store.create_probe(TENANT, gateway.device_id, "Probe A")
    second = store.create_probe(TENANT, gateway.device_id, "Probe B")

    assert first.component_id != second.component_id
    assert first.kind is second.kind is ComponentKind.PROBE
    assert {first.device_id, second.device_id} == {gateway.device_id}
    assert {item.component_id for item in store.list_components(gateway.device_id)} == {
        first.component_id,
        second.component_id,
    }


def test_in_memory_repositories_implement_the_four_frozen_protocols() -> None:
    repositories = InMemoryM1Repositories(clock=Clock(NOW))
    for protocol in (
        SiteAssetRepository,
        DeviceComponentRepository,
        IntegrationRepository,
        DeploymentRepository,
    ):
        assert isinstance(repositories, protocol)


def integration_fixture() -> tuple[ConditionalDynamoClient, DynamoM1Repositories, Any, str, str]:
    """A native v2 Device plus a pending IntegrationAccount, created through the real services
    over the real Dynamo adapters."""
    client = ConditionalDynamoClient()
    repositories = DynamoM1Repositories.over(client, clock=lambda: NOW)
    device = asyncio.run(
        DeviceV2Service(repositories.device_component_repository, clock=lambda: NOW).create(
            TENANT, DeviceV2CreateCommand(name="Gateway"), "m1-gate-device-0001"
        )
    ).entity
    service = IntegrationService(
        repositories.integration_repository,
        repositories.device_component_repository,
        clock=lambda: NOW,
    )
    created = asyncio.run(
        service.create_integration_account(
            TENANT,
            IntegrationAccountCreate(provider_key="vendor.cloud", display_name="Vendor"),
            "m1-gate-account-0001",
        )
    )
    account_id = json.loads(created.snapshot_json)["integration_account_id"]
    return client, repositories, service, device.device_id, account_id


def test_device_integration_create_commits_through_the_in_condition_fence() -> None:
    client, repositories, service, device_id, account_id = integration_fixture()

    result = asyncio.run(
        service.create_device_integration(
            TENANT,
            device_id,
            DeviceIntegrationCreate(integration_account_id=account_id, external_identity="sn-1"),
            "m1-gate-integration-0001",
        )
    )

    assert result.replayed is False
    integrations = asyncio.run(
        repositories.integration_repository.list_device_integrations(TENANT, device_id)
    )
    assert [item.integration_account_id for item in integrations] == [account_id]
    assert len([key for key in client.items if key[0].startswith("EXTERNAL_ID#")]) == 1
    assert client.scan_calls == 0


def test_device_integration_for_a_revoked_account_is_a_conflict() -> None:
    client, repositories, service, device_id, account_id = integration_fixture()
    account = asyncio.run(
        repositories.integration_repository.get_integration_account(TENANT, account_id)
    )
    revoked = account.model_copy(update={"status": IntegrationAccountStatus.REVOKED})
    client.seed(integration_account_to_item(revoked, repositories.integration_repository.keys))
    before = client.snapshot()
    integration = DeviceIntegration(
        tenant_id=TENANT,
        device_integration_id="di_" + "7" * 32,
        device_id=device_id,
        integration_account_id=account_id,
        provider_key="vendor.cloud",
        external_identity_hash="e" * 64,
        created_at=NOW,
        updated_at=NOW,
        version=1,
    )
    receipt = new_idempotency_receipt(
        tenant_id=TENANT,
        scope="device-integration-create",
        key_hash="a" * 64,
        request_hash="b" * 64,
        response_snapshot_json="{}",
        created_at=NOW,
    )

    # The commit-time `#status IN (:pending, :active)` fence rejects the revoked parent.
    with pytest.raises(ConflictError, match="not provisionable"):
        asyncio.run(
            repositories.integration_repository.create_device_integration(integration, receipt)
        )
    with pytest.raises(ConflictError, match="revoked"):
        asyncio.run(
            service.create_device_integration(
                TENANT,
                device_id,
                DeviceIntegrationCreate(
                    integration_account_id=account_id, external_identity="sn-1"
                ),
                "m1-gate-integration-0002",
            )
        )
    assert client.snapshot() == before


# 2) West -> East relocation keeps the old event-time assignment ------------------------------------


@pytest.mark.parametrize("backend", ["in_memory", "dynamo"])
def test_relocation_preserves_old_event_time_assignment(backend: str) -> None:
    store = scenario(backend, Clock(NOW))
    site = store.create_site(TENANT, "Fazenda")
    west = store.create_asset(TENANT, site.site_id, "West").asset
    east = store.create_asset(TENANT, site.site_id, "East").asset
    gateway = store.create_device(TENANT, "Gateway")
    probe = store.create_probe(TENANT, gateway.device_id, "Probe")

    first = store.start(probe.component_id, west.asset_id, dt("2026-08-25T10:00:00Z"))
    second = store.move(probe.component_id, east.asset_id, dt("2026-08-25T11:00:00Z"))

    assert store.resolve(probe.component_id, dt("2026-08-25T09:59:59Z")) is None
    assert store.resolve(probe.component_id, dt("2026-08-25T10:30:00Z")).asset_id == west.asset_id
    # Half-open: the transition instant belongs to the new interval only.
    assert store.resolve(probe.component_id, dt("2026-08-25T11:00:00Z")).asset_id == east.asset_id
    assert store.resolve(probe.component_id, dt("2026-08-25T11:30:00Z")).asset_id == east.asset_id
    history = store.history(probe.component_id)
    assert [(d.deployment_id, d.asset_id, d.status) for d in history] == [
        (first.deployment_id, west.asset_id, DeploymentStatus.ENDED),
        (second.deployment_id, east.asset_id, DeploymentStatus.ACTIVE),
    ]
    assert history[0].started_at == first.started_at
    assert history[0].ended_at == second.started_at == dt("2026-08-25T11:00:00Z")


@pytest.mark.parametrize("backend", ["in_memory", "dynamo"])
def test_second_current_interval_and_backdated_overlap_are_rejected(backend: str) -> None:
    store = scenario(backend, Clock(NOW))
    site = store.create_site(TENANT, "Fazenda")
    west = store.create_asset(TENANT, site.site_id, "West").asset
    east = store.create_asset(TENANT, site.site_id, "East").asset
    probe = store.create_probe(TENANT, store.create_device(TENANT, "Gateway").device_id, "Probe")
    store.start(probe.component_id, west.asset_id, dt("2026-08-25T10:00:00Z"))

    with pytest.raises(ConflictError):
        store.start(probe.component_id, east.asset_id, dt("2026-08-25T10:30:00Z"))
    store.move(probe.component_id, east.asset_id, dt("2026-08-25T11:00:00Z"))
    store.end(probe.component_id, dt("2026-08-25T11:30:00Z"))
    # Half-open end with no successor: the end instant itself belongs to no interval.
    assert store.resolve(probe.component_id, dt("2026-08-25T11:30:00Z")) is None
    last_instant = dt("2026-08-25T11:29:59.999999Z")
    assert store.resolve(probe.component_id, last_instant).asset_id == east.asset_id
    with pytest.raises(ConflictError):
        store.start(probe.component_id, west.asset_id, dt("2026-08-25T11:15:00Z"))
    store.start(probe.component_id, west.asset_id, dt("2026-08-25T11:30:00Z"))

    assert [d.asset_id for d in store.history(probe.component_id)] == [
        west.asset_id,
        east.asset_id,
        west.asset_id,
    ]
    assert store.resolve(probe.component_id, dt("2026-08-25T10:30:00Z")).asset_id == west.asset_id
    assert store.resolve(probe.component_id, dt("2026-08-25T11:15:00Z")).asset_id == east.asset_id


def test_v1_pond_move_keeps_the_old_assignment_on_the_real_adapters() -> None:
    """Flag-on `/v1` move (M1-05) over the migrated rows (M1-06) on the M1-04 history."""
    client = migrated_store()
    clock = Clock(dt("2026-08-25T11:00:00Z"))
    projection = v1_projection(client, clock)
    before = asyncio.run(projection.get_device(TENANT, LEGACY_DEVICE))
    assert before is not None and before.pond_id == WEST_POND

    moved = asyncio.run(
        projection.update_device(
            TENANT,
            LEGACY_DEVICE,
            before.version,
            name=None,
            pond_id=EAST_POND,
            firmware_version=None,
        )
    )

    assert moved.pond_id == EAST_POND
    repositories = DynamoM1Repositories.over(client, clock=clock)
    component_id = default_component_id_for_legacy_device(TENANT, LEGACY_DEVICE)

    def asset_at(value: str) -> str:
        deployment = asyncio.run(
            repositories.deployment_repository.get_deployment_at(TENANT, component_id, dt(value))
        )
        assert deployment is not None
        return deployment.asset_id

    assert asset_at("2026-08-25T10:30:00Z") == asset_id_for_legacy_pond(TENANT, WEST_POND)
    assert asset_at("2026-08-25T11:30:00Z") == asset_id_for_legacy_pond(TENANT, EAST_POND)
    history = asyncio.run(
        repositories.deployment_repository.list_component_deployments(TENANT, component_id)
    )
    assert [d.status for d in history] == [DeploymentStatus.ENDED, DeploymentStatus.ACTIVE]
    assert history[0].deployment_id == deployment_id_for_legacy_device(TENANT, LEGACY_DEVICE)
    assert history[0].ended_at == history[1].started_at
    assert client.scan_calls == 0


# 3) Device v2 has no canonical Pond; the /v1 projection keeps it -----------------------------------


def test_device_v2_has_no_pond_but_the_v1_projection_retains_it() -> None:
    assert "pond_id" not in DeviceV2.model_fields
    assert "pond_id" in Device.model_fields
    client = migrated_store()
    repositories = DynamoM1Repositories.over(client, clock=lambda: NOW)

    native = asyncio.run(repositories.device_component_repository.get_device(TENANT, LEGACY_DEVICE))
    projected = asyncio.run(v1_projection(client, Clock(NOW)).get_device(TENANT, LEGACY_DEVICE))

    assert native is not None and "pond_id" not in native.model_dump()
    assert projected is not None and projected.pond_id == WEST_POND


# 4) Future quota dimensions are representable ------------------------------------------------------


def test_future_quota_dimensions_are_stable() -> None:
    assert {
        "sites",
        "assets",
        "devices",
        "components",
        "notification_destinations",
    } <= {dimension.value for dimension in QuotaDimension}


# 5) The pure core has no AWS IoT or Redis authority ------------------------------------------------


def core_source_files() -> list[Path]:
    files = sorted((SRC / "domain").glob("*.py"))
    files += [SRC / module for module in M1_MODULES]
    files += sorted((SRC / "api/v2").rglob("*.py"))
    files += sorted((ROOT / "scripts/migrations").rglob("*.py"))
    return files


def test_pure_core_has_no_aws_thing_certificate_shadow_or_redis_authority() -> None:
    files = core_source_files()
    assert all(path.is_file() for path in files), [str(p) for p in files if not p.is_file()]
    assert SRC / "repositories/v2_contracts.py" in files
    assert SRC / "domain/devices_v2.py" in files
    offenders = [
        f"{path.relative_to(ROOT)}: {word}"
        for path in files
        for word in FORBIDDEN_CORE_WORDS
        if word in path.read_text(encoding="utf-8").lower()
    ]
    assert offenders == []


# 6) Applying the migration twice duplicates nothing ------------------------------------------------


def test_migration_dry_run_then_apply_twice_duplicates_nothing() -> None:
    client = legacy_store()
    legacy = client.snapshot()

    dry_run = migrate(client, apply=False)
    assert client.snapshot() == legacy
    assert client.write_calls == 0
    first = migrate(client, apply=True)
    after_first = client.snapshot()
    second = migrate(client, apply=True, clock=dt("2026-09-30T18:00:00Z"))

    assert (dry_run.apply, dry_run.result) == (False, "success")
    assert (dry_run.would_create_default_sites, dry_run.would_create_assets) == (1, 2)
    assert (dry_run.would_create_components, dry_run.would_create_deployments) == (1, 1)
    assert first.result == second.result == "success"
    assert (first.default_sites_created, first.assets_created) == (1, 2)
    assert (first.components_created, first.deployments_created) == (1, 1)
    created = {name: value for name, value in second.model_dump().items() if "_created" in name}
    assert created and set(created.values()) == {0}
    assert (second.devices_projected, second.legacy_links_updated) == (0, 0)
    assert second.already_migrated > 0
    assert second.schema_conflicts == 0

    receipt = ("TENANT#" + TENANT, "MIGRATION#M1#DEFAULT_SITE_ASSET_DEVICE")
    changed = {key for key, item in client.snapshot().items() if after_first.get(key) != item}
    assert changed == {receipt}
    assert client.item(*receipt)["run_count"] == 2
    assert client.keys_with_sk_prefix("SITE#") == {
        (f"TENANT#{TENANT}", f"SITE#{default_site_id(TENANT)}")
    }
    assert {sk for _, sk in client.keys_with_sk_prefix("ASSET#")} == {
        f"ASSET#{asset_id_for_legacy_pond(TENANT, WEST_POND)}",
        f"ASSET#{asset_id_for_legacy_pond(TENANT, EAST_POND)}",
    }
    component_id = default_component_id_for_legacy_device(TENANT, LEGACY_DEVICE)
    assert {pk for pk, sk in client.keys_with_sk_prefix("COMPONENT#")} == {f"TENANT#{TENANT}"}
    assert client.item(f"COMPONENT#{component_id}", "META") is not None
    deployment_rows = {
        key
        for key in client.keys_with_sk_prefix("DEPLOYMENT#")
        if key[0] == f"COMPONENT#{component_id}" and key[1] != "DEPLOYMENT#CURRENT"
    }
    assert len([key for key in deployment_rows if "MARKER" not in key[1]]) == 1
    assert client.scan_calls == 0


# 7) Projection flag with incomplete repository state returns 503 -----------------------------------


def test_projection_flag_with_complete_state_serves_v1_from_the_projection() -> None:
    client = migrated_store()
    http = TestClient(projection_app(client))

    ponds = http.get(f"/v1/tenants/{TENANT}/ponds", headers=HEADERS)
    devices = http.get(f"/v1/tenants/{TENANT}/devices", headers=HEADERS)

    assert ponds.status_code == devices.status_code == 200
    assert {item["pond_id"] for item in ponds.json()["items"]} == {WEST_POND, EAST_POND}
    assert [(i["device_id"], i["pond_id"]) for i in devices.json()["items"]] == [
        (LEGACY_DEVICE, WEST_POND)
    ]


@pytest.mark.parametrize("missing", M1_STATE)
def test_projection_flag_with_incomplete_state_returns_503(missing: str) -> None:
    client = migrated_store()
    before = client.snapshot()
    http = TestClient(projection_app(client, missing=missing), raise_server_exceptions=False)

    for path in (f"/v1/tenants/{TENANT}/ponds", f"/v1/tenants/{TENANT}/devices"):
        response = http.get(path, headers=HEADERS)
        assert (response.status_code, response.json()) == (503, {"detail": "service unavailable"})
    assert client.snapshot() == before


# 8) v2 off has no /v2 path; v2 on mounts exactly the M1 groups --------------------------------------


def test_v2_flag_off_mounts_no_v2_path() -> None:
    for app_settings in (settings(), settings(v1_pond_device_projection_enabled=True)):
        paths = create_app(app_settings).openapi()["paths"]
        assert not any(path == "/v2" or path.startswith("/v2/") for path in paths)


def test_v2_flag_on_mounts_exactly_the_m1_groups() -> None:
    paths = create_app(settings(api_v2_enabled=True)).openapi()["paths"]
    v2_paths = {path: methods for path, methods in paths.items() if path.startswith("/v2/")}
    operations = {(path, method) for path, methods in v2_paths.items() for method in methods}

    assert len(v2_paths) == 13
    assert len(operations) == 25
    assert operations == M1_V2_OPERATIONS


# 9) Contract and guard coverage --------------------------------------------------------------------


@pytest.mark.parametrize(
    "flags",
    [
        {},
        {"api_v2_enabled": True},
        {"v1_pond_device_projection_enabled": True},
        {"api_v2_enabled": True, "v1_pond_device_projection_enabled": True},
    ],
)
def test_v1_contract_matches_the_p0_golden(flags: dict[str, bool]) -> None:
    golden = json.loads(GOLDEN.read_text(encoding="utf-8"))
    assert build_v1_openapi_contract(create_app(settings(**flags))) == golden


@pytest.mark.parametrize(
    ("flags", "expected"),
    [
        ({}, V1_TENANT_ROUTE_COUNT),
        ({"api_v2_enabled": True}, V1_TENANT_ROUTE_COUNT + len(M1_V2_OPERATIONS)),
        ({"v1_pond_device_projection_enabled": True}, V1_TENANT_ROUTE_COUNT),
        (
            {"api_v2_enabled": True, "v1_pond_device_projection_enabled": True},
            V1_TENANT_ROUTE_COUNT + len(M1_V2_OPERATIONS),
        ),
    ],
)
def test_every_tenant_route_requires_membership(flags: dict[str, bool], expected: int) -> None:
    routes = tenant_routes(create_app(settings(**flags)))
    assert len(routes) == expected
    for route in routes:
        assert require_tenant_access in set(dependency_calls(route.dependant))


def _load_no_scan_guard():
    spec = importlib.util.spec_from_file_location("m1_gate_no_scan_guard", NO_SCAN_GUARD)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_no_scan_guard_walks_the_m1_migration_modules(tmp_path: Path) -> None:
    guard = _load_no_scan_guard()
    walked = ast.unparse(
        ast.parse(inspect.getsource(guard.test_no_dynamodb_scan_in_application_code))
    )
    assert "python_offenders(ROOT / 'src')" in walked
    assert "python_offenders(ROOT / 'scripts')" in walked
    assert guard.ROOT == ROOT

    migration_dirs = (SRC / "migrations", ROOT / "scripts/migrations")
    for directory in migration_dirs:
        modules = sorted(directory.rglob("*.py"))
        assert modules, directory
        root = ROOT / ("src" if directory.is_relative_to(ROOT / "src") else "scripts")
        assert set(modules) <= set(root.rglob("*.py"))
        assert guard.python_offenders(directory) == []

    # The guard really inspects these modules: a planted Scan in a copy is reported.
    planted = tmp_path / "migrations"
    planted.mkdir()
    source = (SRC / "migrations/default_site_asset.py").read_text(encoding="utf-8")
    (planted / "default_site_asset.py").write_text(
        source + "\n\ndef _planted(client):\n    return client.scan(TableName='x')\n",
        encoding="utf-8",
    )
    monkeypatched_root = guard.ROOT
    guard.ROOT = tmp_path
    try:
        assert guard.python_offenders(planted) == ["migrations/default_site_asset.py"]
    finally:
        guard.ROOT = monkeypatched_root


def test_no_dynamodb_scan_in_m1_and_application_code() -> None:
    guard = _load_no_scan_guard()
    offenders = (
        guard.python_offenders(ROOT / "src")
        + guard.python_offenders(ROOT / "scripts")
        + guard.go_offenders(ROOT / "cmd")
        + guard.go_offenders(ROOT / "internal")
    )
    assert offenders == []
