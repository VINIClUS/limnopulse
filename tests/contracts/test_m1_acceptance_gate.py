"""M1 release gate (Epic #16, Task M1-08): one file proving every M1 acceptance invariant.

A failure here is corrected in the owning M1 task (M1-05, M1-06 or M1-07); never weaken the
gate or regenerate the `/v1` golden to hide drift.
"""

import ast
import asyncio
import importlib.util
import inspect
import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError
from fastapi.testclient import TestClient
from pydantic import BaseModel

from limnopulse_api.adapters.dynamodb import DynamoDomainRepository
from limnopulse_api.adapters.dynamodb_deployments import deployment_marker_key
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
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.assets import (
    Asset,
    AssetStatus,
    AssetType,
    PondProfile,
    Site,
    SiteStatus,
)
from limnopulse_api.domain.deployments import Deployment, DeploymentStatus
from limnopulse_api.domain.devices_v2 import (
    CapabilityAvailability,
    CapabilityDeclaration,
    CapabilityOperation,
    CapabilityProvenance,
    CapabilitySupport,
    ComponentKind,
    ComponentStatus,
    DeviceComponent,
    DeviceStatus,
    DeviceV2,
    ProbeProfile,
)
from limnopulse_api.domain.entities import Device, Membership
from limnopulse_api.domain.ids import (
    asset_id_for_legacy_pond,
    default_component_id_for_legacy_device,
    default_site_id,
    deployment_id_for_legacy_device,
)
from limnopulse_api.domain.integrations import (
    DeviceIntegration,
    IntegrationAccount,
    IntegrationAccountStatus,
)
from limnopulse_api.domain.roles import TenantRole
from limnopulse_api.domain.v2_common import (
    IDEMPOTENCY_RECEIPT_TTL,
    QuotaDimension,
    new_idempotency_receipt,
)
from limnopulse_api.main import create_app
from limnopulse_api.migrations.default_site_asset import DefaultSiteAssetMigrator
from limnopulse_api.repositories.v2_contracts import (
    DeploymentRepository,
    DeviceComponentRepository,
    IntegrationRepository,
    SiteAssetRepository,
)
from limnopulse_api.services.assets import AssetService
from limnopulse_api.services.devices_v2 import DeviceV2CreateCommand, DeviceV2Service
from limnopulse_api.services.integrations import IntegrationService
from limnopulse_api.services.sites import SiteService
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


def _identity_claim_backends() -> list[tuple[str, Any, Any]]:
    """(name, IntegrationRepository, DeviceComponentRepository) for both implementations."""
    in_memory = InMemoryM1Repositories(clock=Clock(NOW))
    dynamo = DynamoM1Repositories.over(ConditionalDynamoClient(), clock=lambda: NOW)
    return [
        ("in_memory", in_memory, in_memory),
        ("dynamo", dynamo.integration_repository, dynamo.device_component_repository),
    ]


def _receipt(tenant_id: str, scope: str, index: int):
    return new_idempotency_receipt(
        tenant_id=tenant_id,
        scope=scope,
        key_hash=f"{index:064x}",
        request_hash="b" * 64,
        response_snapshot_json="{}",
        created_at=NOW,
    )


def _claim(integrations: Any, tenant_id: str, index: int, account_id: str, device_id: str):
    integration = DeviceIntegration(
        tenant_id=tenant_id,
        device_integration_id=f"di_{index:032x}",
        device_id=device_id,
        integration_account_id=account_id,
        provider_key="vendor.cloud",
        external_identity_hash="c" * 64,
        created_at=NOW,
        updated_at=NOW,
        version=1,
    )
    return asyncio.run(
        integrations.create_device_integration(
            integration, _receipt(tenant_id, "device-integration-create", index)
        )
    )


def _seed_tenant(integrations: Any, devices: Any, tenant_id: str, index: int) -> tuple[str, str]:
    account = IntegrationAccount(
        tenant_id=tenant_id,
        integration_account_id=f"ia_{index:032x}",
        provider_key="vendor.cloud",
        display_name="Vendor",
        created_at=NOW,
        updated_at=NOW,
        version=1,
    )
    device = DeviceV2(
        tenant_id=tenant_id,
        device_id=f"dev_{index:032x}",
        name="Gateway",
        created_at=NOW,
        updated_at=NOW,
        version=1,
    )
    asyncio.run(
        integrations.create_integration_account(
            account, _receipt(tenant_id, "integration-account-create", index)
        )
    )
    asyncio.run(devices.create_device(device, _receipt(tenant_id, "device-create", index)))
    return account.integration_account_id, device.device_id


def test_external_identity_claims_are_global_on_both_backends() -> None:
    outcomes = {}
    for name, integrations, devices in _identity_claim_backends():
        account_a, device_a = _seed_tenant(integrations, devices, "tnt_a", 1)
        account_b, device_b = _seed_tenant(integrations, devices, "tnt_b", 2)
        _claim(integrations, "tnt_a", 10, account_a, device_a)
        messages = []
        # Same tenant, a different integration id, the same provider identity.
        with pytest.raises(ConflictError) as same_tenant:
            _claim(integrations, "tnt_a", 11, account_a, device_a)
        messages.append(str(same_tenant.value))
        # Another tenant claiming the same provider identity.
        with pytest.raises(ConflictError) as cross_tenant:
            _claim(integrations, "tnt_b", 12, account_b, device_b)
        messages.append(str(cross_tenant.value))
        listed = asyncio.run(integrations.list_device_integrations("tnt_b", device_b))
        assert listed == [], name
        outcomes[name] = messages

    assert outcomes["in_memory"] == outcomes["dynamo"] == ["external identity already claimed"] * 2


def _capability(component_id: str, index: int, **schemas: Any) -> CapabilityDeclaration:
    return CapabilityDeclaration(
        tenant_id=TENANT,
        component_id=component_id,
        capability_id=f"cap_{index:032x}",
        capability_key="water.temperature",
        operation=CapabilityOperation.READ,
        support=CapabilitySupport.SUPPORTED,
        availability=CapabilityAvailability.AVAILABLE,
        provenance=CapabilityProvenance.MODEL_PROFILE,
        created_at=NOW,
        updated_at=NOW,
        version=1,
        **schemas,
    )


def _component_backends() -> list[tuple[str, Any, Callable[[], Any]]]:
    in_memory = InMemoryM1Repositories(clock=Clock(NOW))
    client = ConditionalDynamoClient()
    dynamo = DynamoM1Repositories.over(client, clock=lambda: NOW)

    def in_memory_state() -> Any:
        return (dict(in_memory.capabilities), dict(in_memory.receipts))

    return [
        ("in_memory", in_memory, in_memory_state),
        ("dynamo", dynamo.device_component_repository, client.snapshot),
    ]


def _seed_component(repository: Any, index: int) -> DeviceComponent:
    device = DeviceV2(
        tenant_id=TENANT,
        device_id=f"dev_{index:032x}",
        name="Gateway",
        created_at=NOW,
        updated_at=NOW,
        version=1,
    )
    component = DeviceComponent(
        tenant_id=TENANT,
        component_id=f"cmp_{index:032x}",
        device_id=device.device_id,
        kind=ComponentKind.PROBE,
        name="Probe",
        probe_profile=ProbeProfile(probe_profile_id=f"prb_{index:032x}"),
        created_at=NOW,
        updated_at=NOW,
        version=1,
    )
    asyncio.run(repository.create_device(device, _receipt(TENANT, "device-create", index)))
    asyncio.run(repository.create_component(component, _receipt(TENANT, "component-create", index)))
    return component


@pytest.mark.parametrize(
    "terminal", [None, ComponentStatus.REPLACED, ComponentStatus.DECOMMISSIONED]
)
def test_capability_parent_checks_agree_on_both_backends(
    terminal: ComponentStatus | None,
) -> None:
    outcomes = {}
    for name, repository, state in _component_backends():
        if terminal is None:
            component_id = "cmp_" + "f" * 32
        else:
            component = _seed_component(repository, 1)
            asyncio.run(
                repository.update_component(
                    TENANT, component.component_id, 1, name=None, status=terminal
                )
            )
            component_id = component.component_id
        before = state()
        with pytest.raises((NotFoundError, ConflictError)) as raised:
            asyncio.run(
                repository.create_capability(
                    _capability(component_id, 9), _receipt(TENANT, "capability-create", 9)
                )
            )
        assert state() == before, name
        outcomes[name] = (type(raised.value), str(raised.value))

    expected = (
        (NotFoundError, "component not found")
        if terminal is None
        else (ConflictError, f"component is {terminal}")
    )
    assert outcomes["in_memory"] == outcomes["dynamo"] == expected


def test_non_integral_numbers_round_trip_through_the_dynamo_fake() -> None:
    client = ConditionalDynamoClient()
    repositories = DynamoM1Repositories.over(client, clock=lambda: NOW)
    repository = repositories.device_component_repository
    component = _seed_component(repository, 2)
    schemas = {
        "parameter_schema": {"type": "number", "minimum": 0.5, "nested": {"steps": [0.25, 1.5]}},
        "result_schema": {"type": "number", "multipleOf": 0.1},
    }

    # Production stores capability schemas as canonical JSON strings.
    asyncio.run(
        repository.create_capability(
            _capability(component.component_id, 3, **schemas),
            _receipt(TENANT, "capability-create", 3),
        )
    )
    (stored,) = asyncio.run(repository.list_capabilities(TENANT, component.component_id))
    assert (stored.parameter_schema, stored.result_schema) == (
        schemas["parameter_schema"],
        schemas["result_schema"],
    )
    # Generic numeric attributes take the adapter base's float -> Decimal path.
    key = {"PK": "TEST#numbers", "SK": "META"}
    item = {**key, "ratio": 0.5, "nested": {"values": [0.25, 2, 1.5]}}
    client.put_item(**repository.put_request(item))
    assert asyncio.run(repository.get_item(key, consistent=True)) == item
    assert client.item("TEST#numbers", "META")["ratio"] == Decimal("0.5")


# Parity sweep: InMemoryM1Repositories vs the four real Dynamo adapters ---------------------------


class ParityBundle:
    """The four protocol implementations of one backend, fresh for each scenario."""

    def __init__(self, backend: str) -> None:
        self.clock = Clock(NOW)
        self.backend = backend
        if backend == "in_memory":
            self.memory = InMemoryM1Repositories(clock=self.clock)
            self.sites = self.devices = self.integrations = self.deployments = self.memory
        else:
            self.client = ConditionalDynamoClient()
            dynamo = DynamoM1Repositories.over(self.client, clock=self.clock)
            self.sites = dynamo.site_asset_repository
            self.devices = dynamo.device_component_repository
            self.integrations = dynamo.integration_repository
            self.deployments = dynamo.deployment_repository

    def seed_legacy_pond(self, pond_id: str, name: str) -> None:
        if self.backend == "in_memory":
            self.memory.seed_legacy_pond(TENANT_A, pond_id, name=name)
        else:
            self.client.seed({**legacy_pond_item(pond_id, tenant_id=TENANT_A), "name": name})

    async def legacy_pond(self, pond_id: str) -> dict[str, Any]:
        """The legacy Pond fields an Asset update mirrors, plus its own version."""
        if self.backend == "in_memory":
            row = self.memory.legacy_ponds[(TENANT_A, pond_id)]
        else:
            row = self.client.item(f"TENANT#{TENANT_A}", f"POND#{pond_id}")
        return {field: row[field] for field in ("name", "description", "status", "version")}


TENANT_A, TENANT_B = "tnt_a", "tnt_b"
P_SITE, P_ASSET, P_DEVICE = "site_" + "1" * 32, "ast_" + "2" * 32, "dev_" + "3" * 32
P_COMPONENT, P_ACCOUNT, P_DEPLOYMENT = "cmp_" + "4" * 32, "ia_" + "5" * 32, "dep_" + "6" * 32
T10, T11, T1130 = dt("2026-08-25T10:00:00Z"), dt("2026-08-25T11:00:00Z"), dt("2026-08-25T11:30:00Z")


def p_receipt(
    tenant_id: str,
    scope: str,
    index: int,
    *,
    request: str = "b",
    snapshot="{}",
    created_at: datetime = NOW,
):
    return new_idempotency_receipt(
        tenant_id=tenant_id,
        scope=scope,
        key_hash=f"{index:064x}",
        request_hash=request * 64,
        response_snapshot_json=snapshot,
        created_at=created_at,
    )


def p_site(tenant_id: str = TENANT_A, site_id: str = P_SITE, name: str = "Fazenda") -> Site:
    return Site(
        tenant_id=tenant_id, site_id=site_id, name=name, created_at=NOW, updated_at=NOW, version=1
    )


def p_asset(
    tenant_id: str = TENANT_A,
    asset_id: str = P_ASSET,
    site_id: str = P_SITE,
    legacy_pond_id: str | None = None,
):
    asset = Asset(
        tenant_id=tenant_id,
        asset_id=asset_id,
        site_id=site_id,
        asset_type=AssetType.POND,
        name="West",
        created_at=NOW,
        updated_at=NOW,
        version=1,
    )
    profile = PondProfile(
        tenant_id=tenant_id,
        asset_id=asset_id,
        legacy_pond_id=legacy_pond_id,
        created_at=NOW,
        updated_at=NOW,
        version=1,
    )
    return asset, profile


def p_device(tenant_id: str = TENANT_A, device_id: str = P_DEVICE) -> DeviceV2:
    return DeviceV2(
        tenant_id=tenant_id,
        device_id=device_id,
        name="Gateway",
        created_at=NOW,
        updated_at=NOW,
        version=1,
    )


def p_component(
    tenant_id: str = TENANT_A, component_id: str = P_COMPONENT, device_id: str = P_DEVICE
) -> DeviceComponent:
    return DeviceComponent(
        tenant_id=tenant_id,
        component_id=component_id,
        device_id=device_id,
        kind=ComponentKind.PROBE,
        name="Probe",
        probe_profile=ProbeProfile(probe_profile_id="prb_" + "7" * 32),
        created_at=NOW,
        updated_at=NOW,
        version=1,
    )


def p_account(status: IntegrationAccountStatus = IntegrationAccountStatus.PENDING):
    return IntegrationAccount(
        tenant_id=TENANT_A,
        integration_account_id=P_ACCOUNT,
        provider_key="vendor.cloud",
        display_name="Vendor",
        status=status,
        created_at=NOW,
        updated_at=NOW,
        version=1,
    )


def p_integration(device_id: str = P_DEVICE, index: int = 1) -> DeviceIntegration:
    return DeviceIntegration(
        tenant_id=TENANT_A,
        device_integration_id=f"di_{index:032x}",
        device_id=device_id,
        integration_account_id=P_ACCOUNT,
        provider_key="vendor.cloud",
        external_identity_hash=f"{index:064x}",
        created_at=NOW,
        updated_at=NOW,
        version=1,
    )


def p_deployment(
    deployment_id: str = P_DEPLOYMENT, started_at: datetime = T10, **overrides: Any
) -> Deployment:
    values = {
        "tenant_id": TENANT_A,
        "deployment_id": deployment_id,
        "component_id": P_COMPONENT,
        "site_id": P_SITE,
        "asset_id": P_ASSET,
        "started_at": started_at,
        "created_at": NOW,
        "updated_at": NOW,
        "version": 1,
        **overrides,
    }
    return Deployment(**values)


def p_capability() -> CapabilityDeclaration:
    return _capability(P_COMPONENT, 1).model_copy(update={"tenant_id": TENANT_A})


async def p_world(r: ParityBundle) -> None:
    """tnt_a: an active Site, Asset, Device, probe Component and pending IntegrationAccount."""
    await r.sites.create_site(p_site(), p_receipt(TENANT_A, "site-create", 1))
    await r.sites.create_asset(*p_asset(), p_receipt(TENANT_A, "asset-create", 1))
    await r.devices.create_device(p_device(), p_receipt(TENANT_A, "device-create", 1))
    await r.devices.create_component(p_component(), p_receipt(TENANT_A, "component-create", 1))
    await r.integrations.create_integration_account(
        p_account(), p_receipt(TENANT_A, "integration-account-create", 1)
    )


async def p_start(r: ParityBundle, deployment: Deployment, index: int) -> Deployment:
    return await r.deployments.start_deployment(
        deployment, p_receipt(TENANT_A, "deployment-create", index)
    )


def _scenario(steps):
    async def run(r: ParityBundle) -> Any:
        await p_world(r)
        return await steps(r)

    return run


MIGRATED_ASSET = "ast_" + "a" * 32
FRACTIONAL_CREATED_AT = NOW + timedelta(microseconds=500_000)
FRACTIONAL_EXPIRY = FRACTIONAL_CREATED_AT + IDEMPOTENCY_RECEIPT_TTL  # stored as ceil -> +1s


async def p_migrated_asset(r: ParityBundle, *, seed_legacy: bool = True) -> None:
    """An Asset linked to a legacy Pond, as the migration and the `/v1` projection create it."""
    if seed_legacy:
        r.seed_legacy_pond("pond_west", "West")
    await r.sites.create_asset(
        *p_asset(asset_id=MIGRATED_ASSET, legacy_pond_id="pond_west"),
        p_receipt(TENANT_A, "asset-create", 9),
    )


def _migrated_asset_update(**changes: Any):
    async def steps(r: ParityBundle) -> Any:
        await p_migrated_asset(r)
        values = {
            "name": None,
            "status": None,
            "pond_description": None,
            "pond_profile_expected_version": None,
            **changes,
        }
        updated = await r.sites.update_asset(TENANT_A, MIGRATED_ASSET, 1, **values)
        return [updated, await r.legacy_pond("pond_west")]

    return steps


async def _receipt_reuse_at(r: ParityBundle, observed_at: datetime) -> Any:
    await r.sites.create_site(
        p_site(site_id="site_" + "a" * 32),
        p_receipt(TENANT_A, "site-create", 5, created_at=FRACTIONAL_CREATED_AT),
    )
    r.clock.value = observed_at
    return await r.sites.create_site(
        p_site(site_id="site_" + "b" * 32),
        p_receipt(TENANT_A, "site-create", 5, request="c", created_at=observed_at),
    )


PARITY_SCENARIOS: dict[str, Any] = {
    # Migrated Asset mirrored onto its legacy Pond (D2, D6) ---------------------------------
    "asset.legacy_pond.rename": _scenario(_migrated_asset_update(name="Renamed")),
    "asset.legacy_pond.description": _scenario(
        _migrated_asset_update(pond_description="deep", pond_profile_expected_version=1)
    ),
    "asset.legacy_pond.archive": _scenario(_migrated_asset_update(status=AssetStatus.ARCHIVED)),
    "asset.legacy_pond.missing": _scenario(
        lambda r: _then(
            p_migrated_asset(r, seed_legacy=False),
            lambda: r.sites.update_asset(
                TENANT_A,
                MIGRATED_ASSET,
                1,
                name="X",
                status=None,
                pond_description=None,
                pond_profile_expected_version=None,
            ),
        )
    ),
    # Receipt expiry: stored as ceil(created_at + 24h), compared with the whole-second clock --
    "receipt.at_fractional_expiry_still_taken": _scenario(
        lambda r: _receipt_reuse_at(r, FRACTIONAL_EXPIRY)
    ),
    "receipt.after_expiry_ceiling_accepted": _scenario(
        lambda r: _receipt_reuse_at(r, FRACTIONAL_EXPIRY + timedelta(microseconds=500_000))
    ),
    # SiteAssetRepository ------------------------------------------------------------------
    "site.create.duplicate_same_tenant": _scenario(
        lambda r: r.sites.create_site(p_site(), p_receipt(TENANT_A, "site-create", 2))
    ),
    "site.create.same_id_other_tenant": _scenario(
        lambda r: r.sites.create_site(p_site(TENANT_B), p_receipt(TENANT_B, "site-create", 1))
    ),
    "site.create.replay_same_request": _scenario(
        lambda r: r.sites.create_site(
            p_site(site_id="site_" + "9" * 32, name="Other"),
            p_receipt(TENANT_A, "site-create", 7, snapshot=p_site().model_dump_json()),
        )
    ),
    "site.create.key_reused_for_different_request": _scenario(
        lambda r: r.sites.create_site(
            p_site(site_id="site_" + "9" * 32), p_receipt(TENANT_A, "site-create", 1, request="c")
        )
    ),
    "site.update.ok": _scenario(
        lambda r: r.sites.update_site(TENANT_A, P_SITE, 1, name="Renamed", status=None)
    ),
    "site.update.stale_version": _scenario(
        lambda r: r.sites.update_site(TENANT_A, P_SITE, 7, name="Renamed", status=None)
    ),
    "site.update.missing": _scenario(
        lambda r: r.sites.update_site(TENANT_B, P_SITE, 1, name="Renamed", status=None)
    ),
    "site.get.other_tenant": _scenario(lambda r: r.sites.get_site(TENANT_B, P_SITE)),
    "site.list": _scenario(lambda r: r.sites.list_sites(TENANT_A)),
    "asset.create.on_archived_site": _scenario(
        lambda r: _then(
            r.sites.update_site(TENANT_A, P_SITE, 1, name=None, status=SiteStatus.ARCHIVED),
            lambda: r.sites.create_asset(
                *p_asset(asset_id="ast_" + "9" * 32), p_receipt(TENANT_A, "asset-create", 2)
            ),
        )
    ),
    "asset.create.missing_site": _scenario(
        lambda r: r.sites.create_asset(
            *p_asset(asset_id="ast_" + "9" * 32, site_id="site_" + "9" * 32),
            p_receipt(TENANT_A, "asset-create", 2),
        )
    ),
    "asset.create.same_id_other_tenant": _scenario(
        lambda r: _then(
            r.sites.create_site(p_site(TENANT_B), p_receipt(TENANT_B, "site-create", 1)),
            lambda: r.sites.create_asset(
                *p_asset(TENANT_B), p_receipt(TENANT_B, "asset-create", 1)
            ),
        )
    ),
    "asset.update.missing": _scenario(
        lambda r: r.sites.update_asset(
            TENANT_B,
            P_ASSET,
            1,
            name="X",
            status=None,
            pond_description=None,
            pond_profile_expected_version=None,
        )
    ),
    "asset.update.stale_version": _scenario(
        lambda r: r.sites.update_asset(
            TENANT_A,
            P_ASSET,
            5,
            name="X",
            status=None,
            pond_description=None,
            pond_profile_expected_version=None,
        )
    ),
    "asset.update.stale_profile_version": _scenario(
        lambda r: r.sites.update_asset(
            TENANT_A,
            P_ASSET,
            1,
            name=None,
            status=None,
            pond_description="d",
            pond_profile_expected_version=5,
        )
    ),
    "asset.update.description_without_profile_version": _scenario(
        lambda r: r.sites.update_asset(
            TENANT_A,
            P_ASSET,
            1,
            name=None,
            status=None,
            pond_description="d",
            pond_profile_expected_version=None,
        )
    ),
    "asset.update.archive_with_description": _scenario(
        lambda r: r.sites.update_asset(
            TENANT_A,
            P_ASSET,
            1,
            name="East",
            status=AssetStatus.ARCHIVED,
            pond_description="d",
            pond_profile_expected_version=1,
        )
    ),
    "site.service.reactivate_archived": _scenario(
        lambda r: _then(
            r.sites.update_site(TENANT_A, P_SITE, 1, name=None, status=SiteStatus.ARCHIVED),
            lambda: SiteService(r.sites, clock=lambda: NOW).update(
                tenant_id=TENANT_A,
                site_id=P_SITE,
                expected_version=2,
                name=None,
                status="active",
            ),
        )
    ),
    "asset.service.reactivate_archived": _scenario(
        lambda r: _then(
            r.sites.update_asset(
                TENANT_A,
                P_ASSET,
                1,
                name=None,
                status=AssetStatus.ARCHIVED,
                pond_description=None,
                pond_profile_expected_version=None,
            ),
            lambda: AssetService(r.sites, clock=lambda: NOW).update(
                tenant_id=TENANT_A,
                asset_id=P_ASSET,
                expected_version=2,
                name=None,
                status="active",
                pond_description=None,
                pond_profile_expected_version=None,
            ),
        )
    ),
    # DeviceComponentRepository ------------------------------------------------------------
    "device.create.same_id_other_tenant": _scenario(
        lambda r: r.devices.create_device(
            p_device(TENANT_B), p_receipt(TENANT_B, "device-create", 1)
        )
    ),
    "device.create.receipt_already_used": _scenario(
        lambda r: r.devices.create_device(
            p_device(device_id="dev_" + "9" * 32), p_receipt(TENANT_A, "device-create", 1)
        )
    ),
    "device.update.missing": _scenario(
        lambda r: r.devices.update_device(
            TENANT_B, P_DEVICE, 1, name="X", firmware_version=None, status=None
        )
    ),
    "device.update.stale_version": _scenario(
        lambda r: r.devices.update_device(
            TENANT_A, P_DEVICE, 3, name="X", firmware_version=None, status=None
        )
    ),
    "device.update.decommissioned_to_active": _scenario(
        lambda r: _then(
            r.devices.update_device(
                TENANT_A,
                P_DEVICE,
                1,
                name=None,
                firmware_version=None,
                status=DeviceStatus.DECOMMISSIONED,
            ),
            lambda: r.devices.update_device(
                TENANT_A,
                P_DEVICE,
                2,
                name=None,
                firmware_version=None,
                status=DeviceStatus.ACTIVE,
            ),
        )
    ),
    "device.update.suspend": _scenario(
        lambda r: r.devices.update_device(
            TENANT_A,
            P_DEVICE,
            1,
            name="X",
            firmware_version="2.0",
            status=DeviceStatus.SUSPENDED,
        )
    ),
    "device.get.other_tenant": _scenario(lambda r: r.devices.get_device(TENANT_B, P_DEVICE)),
    "component.create.missing_device": _scenario(
        lambda r: r.devices.create_component(
            p_component(component_id="cmp_" + "9" * 32, device_id="dev_" + "9" * 32),
            p_receipt(TENANT_A, "component-create", 2),
        )
    ),
    "component.create.decommissioned_device": _scenario(
        lambda r: _then(
            r.devices.update_device(
                TENANT_A,
                P_DEVICE,
                1,
                name=None,
                firmware_version=None,
                status=DeviceStatus.DECOMMISSIONED,
            ),
            lambda: r.devices.create_component(
                p_component(component_id="cmp_" + "9" * 32),
                p_receipt(TENANT_A, "component-create", 2),
            ),
        )
    ),
    "component.create.same_id_other_tenant": _scenario(
        lambda r: _then(
            r.devices.create_device(
                p_device(TENANT_B, "dev_" + "8" * 32), p_receipt(TENANT_B, "device-create", 1)
            ),
            lambda: r.devices.create_component(
                p_component(TENANT_B, device_id="dev_" + "8" * 32),
                p_receipt(TENANT_B, "component-create", 1),
            ),
        )
    ),
    "component.create.receipt_already_used": _scenario(
        lambda r: r.devices.create_component(
            p_component(component_id="cmp_" + "9" * 32),
            p_receipt(TENANT_A, "component-create", 1),
        )
    ),
    "component.update.other_tenant": _scenario(
        lambda r: r.devices.update_component(TENANT_B, P_COMPONENT, 1, name="X", status=None)
    ),
    "component.update.stale_version": _scenario(
        lambda r: r.devices.update_component(TENANT_A, P_COMPONENT, 4, name="X", status=None)
    ),
    "component.update.replaced_to_active": _scenario(
        lambda r: _then(
            r.devices.update_component(
                TENANT_A, P_COMPONENT, 1, name=None, status=ComponentStatus.REPLACED
            ),
            lambda: r.devices.update_component(
                TENANT_A, P_COMPONENT, 2, name=None, status=ComponentStatus.ACTIVE
            ),
        )
    ),
    "component.update.rename": _scenario(
        lambda r: r.devices.update_component(TENANT_A, P_COMPONENT, 1, name="X", status=None)
    ),
    "component.get.other_tenant": _scenario(
        lambda r: r.devices.get_component(TENANT_B, P_COMPONENT)
    ),
    "component.list": _scenario(lambda r: r.devices.list_components(TENANT_A, P_DEVICE)),
    "capability.create.duplicate": _scenario(
        lambda r: _then(
            r.devices.create_capability(
                p_capability(), p_receipt(TENANT_A, "capability-create", 1)
            ),
            lambda: r.devices.create_capability(
                p_capability(), p_receipt(TENANT_A, "capability-create", 2)
            ),
        )
    ),
    "capability.list.missing_component": _scenario(
        lambda r: r.devices.list_capabilities(TENANT_A, "cmp_" + "9" * 32)
    ),
    "capability.create_then_list": _scenario(
        lambda r: _then(
            r.devices.create_capability(
                p_capability(), p_receipt(TENANT_A, "capability-create", 1)
            ),
            lambda: r.devices.list_capabilities(TENANT_A, P_COMPONENT),
        )
    ),
    # IntegrationRepository ----------------------------------------------------------------
    "account.create.duplicate": _scenario(
        lambda r: r.integrations.create_integration_account(
            p_account(), p_receipt(TENANT_A, "integration-account-create", 2)
        )
    ),
    "account.create.receipt_already_used": _scenario(
        lambda r: r.integrations.create_integration_account(
            p_account().model_copy(update={"integration_account_id": "ia_" + "9" * 32}),
            p_receipt(TENANT_A, "integration-account-create", 1),
        )
    ),
    "integration.create.missing_device": _scenario(
        lambda r: r.integrations.create_device_integration(
            p_integration(device_id="dev_" + "9" * 32),
            p_receipt(TENANT_A, "device-integration-create", 1),
        )
    ),
    "integration.create.decommissioned_device": _scenario(
        lambda r: _then(
            r.devices.update_device(
                TENANT_A,
                P_DEVICE,
                1,
                name=None,
                firmware_version=None,
                status=DeviceStatus.DECOMMISSIONED,
            ),
            lambda: r.integrations.create_device_integration(
                p_integration(), p_receipt(TENANT_A, "device-integration-create", 1)
            ),
        )
    ),
    "integration.create.receipt_already_used": _scenario(
        lambda r: _then(
            r.integrations.create_device_integration(
                p_integration(), p_receipt(TENANT_A, "device-integration-create", 1)
            ),
            lambda: r.integrations.create_device_integration(
                p_integration(index=2), p_receipt(TENANT_A, "device-integration-create", 1)
            ),
        )
    ),
    "integration.create_then_list": _scenario(
        lambda r: _then(
            r.integrations.create_device_integration(
                p_integration(), p_receipt(TENANT_A, "device-integration-create", 1)
            ),
            lambda: r.integrations.list_device_integrations(TENANT_A, P_DEVICE),
        )
    ),
    # DeploymentRepository -----------------------------------------------------------------
    "deployment.start_then_current": _scenario(
        lambda r: _then(
            p_start(r, p_deployment(), 1),
            lambda: r.deployments.get_current_deployment(TENANT_A, P_COMPONENT),
        )
    ),
    "deployment.start.second_active": _scenario(
        lambda r: _then(
            p_start(r, p_deployment(), 1),
            lambda: p_start(r, p_deployment("dep_" + "9" * 32, T11), 2),
        )
    ),
    "deployment.start.overlaps_previous": _scenario(
        lambda r: _then(
            p_start(r, p_deployment(), 1),
            lambda: r.deployments.end_deployment(
                TENANT_A, P_DEPLOYMENT, expected_version=1, ended_at=T11
            ),
            lambda: p_start(r, p_deployment("dep_" + "9" * 32, T10), 2),
        )
    ),
    "deployment.start.reused_deployment_id": _scenario(
        lambda r: _then(
            p_start(r, p_deployment(), 1),
            lambda: r.deployments.end_deployment(
                TENANT_A, P_DEPLOYMENT, expected_version=1, ended_at=T11
            ),
            lambda: p_start(r, p_deployment(started_at=T1130), 2),
        )
    ),
    "deployment.start.component_decommissioned": _scenario(
        lambda r: _then(
            r.devices.update_component(
                TENANT_A, P_COMPONENT, 1, name=None, status=ComponentStatus.DECOMMISSIONED
            ),
            lambda: p_start(r, p_deployment(), 1),
        )
    ),
    "deployment.start.asset_archived": _scenario(
        lambda r: _then(
            r.sites.update_asset(
                TENANT_A,
                P_ASSET,
                1,
                name=None,
                status=AssetStatus.ARCHIVED,
                pond_description=None,
                pond_profile_expected_version=None,
            ),
            lambda: p_start(r, p_deployment(), 1),
        )
    ),
    "deployment.start.site_archived": _scenario(
        lambda r: _then(
            r.sites.update_site(TENANT_A, P_SITE, 1, name=None, status=SiteStatus.ARCHIVED),
            lambda: p_start(r, p_deployment(), 1),
        )
    ),
    "deployment.start.receipt_already_used": _scenario(
        lambda r: _then(
            p_start(r, p_deployment(), 1),
            lambda: r.deployments.end_deployment(
                TENANT_A, P_DEPLOYMENT, expected_version=1, ended_at=T11
            ),
            lambda: p_start(r, p_deployment("dep_" + "9" * 32, T11), 1),
        )
    ),
    "deployment.start.not_active": _scenario(
        lambda r: p_start(r, p_deployment(ended_at=T11, status=DeploymentStatus.ENDED), 1)
    ),
    "deployment.end.missing": _scenario(
        lambda r: r.deployments.end_deployment(
            TENANT_A, P_DEPLOYMENT, expected_version=1, ended_at=T11
        )
    ),
    "deployment.end.stale_version": _scenario(
        lambda r: _then(
            p_start(r, p_deployment(), 1),
            lambda: r.deployments.end_deployment(
                TENANT_A, P_DEPLOYMENT, expected_version=3, ended_at=T11
            ),
        )
    ),
    "deployment.end.twice": _scenario(
        lambda r: _then(
            p_start(r, p_deployment(), 1),
            lambda: r.deployments.end_deployment(
                TENANT_A, P_DEPLOYMENT, expected_version=1, ended_at=T11
            ),
            lambda: r.deployments.end_deployment(
                TENANT_A, P_DEPLOYMENT, expected_version=2, ended_at=T1130
            ),
        )
    ),
    "deployment.end.before_start": _scenario(
        lambda r: _then(
            p_start(r, p_deployment(started_at=T11), 1),
            lambda: r.deployments.end_deployment(
                TENANT_A, P_DEPLOYMENT, expected_version=1, ended_at=T10
            ),
        )
    ),
    "deployment.end_then_history_and_lookup": _scenario(
        lambda r: _then(
            p_start(r, p_deployment(), 1),
            lambda: r.deployments.end_deployment(
                TENANT_A, P_DEPLOYMENT, expected_version=1, ended_at=T11
            ),
            lambda: _gather(
                r.deployments.list_component_deployments(TENANT_A, P_COMPONENT),
                r.deployments.get_deployment_at(TENANT_A, P_COMPONENT, T10),
                r.deployments.get_deployment_at(TENANT_A, P_COMPONENT, T11),
                r.deployments.get_deployment(TENANT_B, P_DEPLOYMENT),
            ),
        )
    ),
}


async def _then(first, *rest):
    """Await `first`, then each zero-argument factory in turn; return the last result."""
    result = await first
    for step in rest:
        result = await step()
    return result


async def _gather(*awaitables):
    return [await awaitable for awaitable in awaitables]


def _observable(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, list):
        return [_observable(item) for item in value]
    return value


def parity_outcome(backend: str, scenario: Any) -> tuple:
    try:
        value = asyncio.run(scenario(ParityBundle(backend)))
    except (NotFoundError, ConflictError, ValueError) as exc:
        kind = next(k for k in (NotFoundError, ConflictError, ValueError) if isinstance(exc, k))
        return ("raises", kind.__name__, str(exc).splitlines()[0])
    return ("returns", _observable(value))


@pytest.mark.parametrize("name", sorted(PARITY_SCENARIOS))
def test_in_memory_repositories_match_the_dynamo_adapters(name: str) -> None:
    in_memory = parity_outcome("in_memory", PARITY_SCENARIOS[name])
    dynamo = parity_outcome("dynamo", PARITY_SCENARIOS[name])
    assert in_memory == dynamo


def test_parity_scenarios_exercise_both_outcomes() -> None:
    kinds = {parity_outcome("dynamo", scenario)[0] for scenario in PARITY_SCENARIOS.values()}
    assert kinds == {"raises", "returns"}


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


MIGRATION_RECEIPT_KEY = (f"TENANT#{TENANT}", "MIGRATION#M1#DEFAULT_SITE_ASSET_DEVICE")


def assert_second_apply_only_bumped_the_receipt(
    after_first: dict[tuple[str, str], dict],
    after_second: dict[tuple[str, str], dict],
    *,
    first_run_count: int = 1,
) -> None:
    """Idempotency over the full key union: nothing added, removed or changed except the
    receipt, which exists after both applies and records exactly one more run."""
    assert MIGRATION_RECEIPT_KEY in after_first and MIGRATION_RECEIPT_KEY in after_second
    assert after_first[MIGRATION_RECEIPT_KEY]["run_count"] == first_run_count
    assert after_second[MIGRATION_RECEIPT_KEY]["run_count"] == first_run_count + 1
    assert set(after_first) == set(after_second)
    without_receipt = [
        {key: item for key, item in snapshot.items() if key != MIGRATION_RECEIPT_KEY}
        for snapshot in (after_first, after_second)
    ]
    assert without_receipt[0] == without_receipt[1]


def _deterministic_row_keys() -> dict[str, tuple[str, str]]:
    component_id = default_component_id_for_legacy_device(TENANT, LEGACY_DEVICE)
    marker = deployment_marker_key(component_id)
    return {
        "pond_profile": (f"ASSET#{asset_id_for_legacy_pond(TENANT, WEST_POND)}", "PROFILE#POND"),
        "current": (f"COMPONENT#{component_id}", "DEPLOYMENT#CURRENT"),
        "marker": (marker["PK"], marker["SK"]),
    }


@pytest.mark.parametrize("row", ["pond_profile", "current", "marker"])
def test_idempotency_check_detects_a_row_lost_between_applies(row: str) -> None:
    client = migrated_store()
    after_first = client.snapshot()
    key = _deterministic_row_keys()[row]
    assert key in after_first
    after_second = {k: dict(v) for k, v in after_first.items() if k != key}
    after_second[MIGRATION_RECEIPT_KEY]["run_count"] = 2

    with pytest.raises(AssertionError):
        assert_second_apply_only_bumped_the_receipt(after_first, after_second)


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

    assert_second_apply_only_bumped_the_receipt(after_first, client.snapshot())
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


NEW_LEGACY_DEVICE = "dev_" + "4" * 32


@pytest.mark.parametrize("window", ["enable", "rollback"])
def test_apply_after_a_mixed_flag_window_converges(window: str) -> None:
    """A rolling restart leaves flag-off instances accepting legacy-only `/v1` writes after the
    final pre-cutover apply (enable), or flag-on and flag-off writes interleaved (rollback). One
    more dry-run + `--apply` repairs moves and backfills creates; the next apply is a no-op."""
    client = migrated_store()
    legacy = DynamoDomainRepository(TABLE, client)
    if window == "rollback":
        # An old flag-on instance moves the Device East, then a flag-off one moves it back West.
        projection = v1_projection(client, Clock(dt("2026-08-25T11:00:00Z")))
        device = asyncio.run(projection.get_device(TENANT, LEGACY_DEVICE))
        asyncio.run(
            projection.update_device(
                TENANT,
                LEGACY_DEVICE,
                device.version,
                name=None,
                pond_id=EAST_POND,
                firmware_version=None,
            )
        )
        target_pond = WEST_POND
    else:
        target_pond = EAST_POND
    device = asyncio.run(legacy.get_device(TENANT, LEGACY_DEVICE))
    asyncio.run(
        legacy.update_device(
            TENANT,
            LEGACY_DEVICE,
            device.version,
            name=None,
            pond_id=target_pond,
            firmware_version=None,
        )
    )
    asyncio.run(legacy.create_device(TENANT, EAST_POND, NEW_LEGACY_DEVICE, "Late", None))
    repositories = DynamoM1Repositories.over(client, clock=lambda: NOW)

    def current_asset(device_id: str) -> str | None:
        component_id = default_component_id_for_legacy_device(TENANT, device_id)
        current = asyncio.run(
            repositories.deployment_repository.get_current_deployment(TENANT, component_id)
        )
        return None if current is None else current.asset_id

    # Drift left by the window, before the repairing apply.
    assert current_asset(LEGACY_DEVICE) != asset_id_for_legacy_pond(TENANT, target_pond)
    assert current_asset(NEW_LEGACY_DEVICE) is None

    dry_run = migrate(client, apply=False)
    repair = migrate(client, apply=True)
    after_repair = client.snapshot()
    third = migrate(client, apply=True)

    assert dry_run.result == repair.result == third.result == "success"
    assert (repair.components_created, repair.deployments_created) == (1, 2)
    assert current_asset(LEGACY_DEVICE) == asset_id_for_legacy_pond(TENANT, target_pond)
    assert current_asset(NEW_LEGACY_DEVICE) == asset_id_for_legacy_pond(TENANT, EAST_POND)
    new_component = default_component_id_for_legacy_device(TENANT, NEW_LEGACY_DEVICE)
    assert client.item(f"COMPONENT#{new_component}", "META") is not None
    assert {f: v for f, v in third.model_dump().items() if "_created" in f} == {
        f: 0 for f in third.model_dump() if "_created" in f
    }
    assert_second_apply_only_bumped_the_receipt(after_repair, client.snapshot(), first_run_count=2)
    assert client.scan_calls == 0


RUNBOOK = ROOT / "docs/m1-acceptance-runbook.md"


def runbook_json(heading: str, **placeholders: str) -> list[dict]:
    """The first JSON block under a runbook heading, with its `<placeholders>` filled in."""
    text = RUNBOOK.read_text(encoding="utf-8")
    section = text[text.index(heading) :]
    opening = section.index("```json") + len("```json")
    block = section[opening : section.index("```", opening)]
    for name, value in {"DYNAMODB_DOMAIN_TABLE": TABLE, **placeholders}.items():
        block = block.replace(f"<{name}>", value)
    assert "<" not in block, block
    return json.loads(block)


def runbook_fenced_delete(tenant_id: str, device_id: str, version: int) -> list[dict]:
    """The runbook's `delete-legacy-device.json`, with its placeholders filled in."""
    return runbook_json(
        "### Removing a legacy Device",
        tenant=tenant_id,
        device=device_id,
        **{"observed-version": str(version)},
    )


def runbook_fenced_pond_delete(tenant_id: str, pond_id: str, version: int) -> list[dict]:
    """The runbook's `delete-legacy-pond.json`, with its placeholders filled in."""
    return runbook_json(
        "### Retiring an unsafe legacy Pond",
        tenant=tenant_id,
        pond=pond_id,
        **{"observed-version": str(version)},
    )


def test_empty_tenant_migrates_with_nothing_to_count() -> None:
    client = ConditionalDynamoClient()
    client.seed(legacy_tenant_item())

    dry_run, first, second = (migrate(client, apply=apply) for apply in (False, True, True))

    assert dry_run.would_create_default_sites == 1
    assert (first.default_sites_created, second.default_sites_created) == (1, 0)
    for summary in (dry_run, first, second):
        counts = summary.model_dump()
        assert summary.result == "success" and summary.schema_conflicts == 0
        assert summary.already_migrated == 0 and summary.legacy_links_updated == 0
        assert all(
            counts[name] == 0
            for name in counts
            if name.endswith("_created") and name != "default_sites_created"
        )


UNSAFE_POND = "pond west"


def test_unsafe_pond_retirement_sequence() -> None:
    client = ConditionalDynamoClient()
    client.seed(legacy_tenant_item())
    client.seed(legacy_pond_item(UNSAFE_POND))
    for item in legacy_device_items(LEGACY_DEVICE, UNSAFE_POND):
        client.seed(item)
    legacy = DynamoDomainRepository(TABLE, client)
    projection = v1_projection(client, Clock(NOW))

    def conflicts() -> list[tuple[str, str]]:
        return [(c.kind, c.key) for c in migrate(client, apply=False).conflicts]

    assert conflicts() == [("pond_invalid", f"TENANT#{TENANT}/POND#{UNSAFE_POND}")]
    # Step 2: the replacement Pond (flag off here; flag on would also work).
    asyncio.run(legacy.create_pond(TENANT, WEST_POND, "West", None))
    # Step 3: the flag-on move reads the unsafe source Pond and is refused; flag off works.
    with pytest.raises(ConflictError, match="pond is not compatible"):
        asyncio.run(
            projection.update_device(
                TENANT, LEGACY_DEVICE, 1, name=None, pond_id=WEST_POND, firmware_version=None
            )
        )
    asyncio.run(
        legacy.update_device(
            TENANT, LEGACY_DEVICE, 1, name=None, pond_id=WEST_POND, firmware_version=None
        )
    )
    assert conflicts() == [("pond_invalid", f"TENANT#{TENANT}/POND#{UNSAFE_POND}")]
    # Step 4: a stale observed version cancels the fenced delete; the observed one succeeds.
    with pytest.raises(ClientError):
        client.transact_write_items(
            TransactItems=runbook_fenced_pond_delete(TENANT, UNSAFE_POND, 7)
        )
    client.transact_write_items(TransactItems=runbook_fenced_pond_delete(TENANT, UNSAFE_POND, 1))
    assert client.item(f"TENANT#{TENANT}", f"POND#{UNSAFE_POND}") is None
    # Step 5: the tenant converges.
    assert conflicts() == []
    applied = migrate(client, apply=True)
    assert applied.result == "success"
    assert (applied.assets_created, applied.devices_projected) == (1, 1)


def test_runbook_fenced_pond_delete_refuses_a_linked_pond() -> None:
    client = migrated_store()
    before = client.snapshot()
    with pytest.raises(ClientError):
        client.transact_write_items(TransactItems=runbook_fenced_pond_delete(TENANT, WEST_POND, 1))
    assert client.snapshot() == before


def _legacy_device_rows(client: ConditionalDynamoClient, device_id: str) -> list:
    return [
        client.item(f"TENANT#{TENANT}", f"DEVICE#{device_id}"),
        client.item(f"DEVICE#{device_id}", "META"),
    ]


@pytest.mark.parametrize("state", ["never_projected", "projected", "changed"])
def test_runbook_fenced_device_delete_refuses_projected_devices(state: str) -> None:
    client = legacy_store()
    observed_version = 1
    if state == "projected":
        assert migrate(client, apply=True).result == "success"
    elif state == "changed":
        # A concurrent `/v1` write bumps the version after the operator's preflight read.
        legacy = DynamoDomainRepository(TABLE, client)
        asyncio.run(
            legacy.update_device(
                TENANT, LEGACY_DEVICE, 1, name="Renamed", pond_id=None, firmware_version=None
            )
        )
    request = runbook_fenced_delete(TENANT, LEGACY_DEVICE, observed_version)
    before = client.snapshot()

    if state == "never_projected":
        client.transact_write_items(TransactItems=request)
        assert _legacy_device_rows(client, LEGACY_DEVICE) == [None, None]
        component_id = default_component_id_for_legacy_device(TENANT, LEGACY_DEVICE)
        assert client.item(f"COMPONENT#{component_id}", "META") is None
        return
    with pytest.raises(ClientError) as cancelled:
        client.transact_write_items(TransactItems=request)
    assert cancelled.value.response["Error"]["Code"] == "TransactionCanceledException"
    assert client.snapshot() == before


def test_missing_pond_and_unsafe_device_id_remedies() -> None:
    """The runbook's remedy order for a Device with a dangling pond and an unsafe id."""
    client = ConditionalDynamoClient()
    client.seed(legacy_tenant_item())
    client.seed(legacy_pond_item(WEST_POND))
    for item in legacy_device_items("dev_bad", "pond_gone"):
        client.seed(item)

    def conflicts() -> list[tuple[str, str]]:
        summary = migrate(client, apply=False)
        return [(conflict.kind, conflict.key) for conflict in summary.conflicts]

    # The pond is checked first: the unsafe id is not reported yet.
    assert conflicts() == [("device_missing_pond", f"TENANT#{TENANT}/DEVICE#dev_bad")]
    # Flag on: the PATCH to an existing pond never reaches the pond.
    with pytest.raises(ConflictError, match="not compatible with the v2 projection"):
        asyncio.run(
            v1_projection(client, Clock(NOW)).update_device(
                TENANT, "dev_bad", 1, name=None, pond_id=WEST_POND, firmware_version=None
            )
        )
    # Flag off: the same PATCH succeeds and the dry-run now reports the unsafe id.
    asyncio.run(
        DynamoDomainRepository(TABLE, client).update_device(
            TENANT, "dev_bad", 1, name=None, pond_id=WEST_POND, firmware_version=None
        )
    )
    assert conflicts() == [("device_invalid", f"TENANT#{TENANT}/DEVICE#dev_bad")]
    # It was never projected, so the runbook's fenced delete retires it and the tenant converges.
    client.transact_write_items(TransactItems=runbook_fenced_delete(TENANT, "dev_bad", 2))
    assert conflicts() == []
    assert migrate(client, apply=True).result == "success"


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
