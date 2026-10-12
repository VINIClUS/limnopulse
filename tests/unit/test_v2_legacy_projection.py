from datetime import UTC, datetime, timedelta, timezone

import pytest

from limnopulse_api.domain.assets import AssetStatus, AssetType, SiteStatus
from limnopulse_api.domain.deployments import DeploymentStatus
from limnopulse_api.domain.devices_v2 import (
    CompatibilityLevel,
    ComponentKind,
    ComponentStatus,
    DeviceStatus,
)
from limnopulse_api.domain.entities import Device, Pond, Tenant
from limnopulse_api.domain.ids import (
    asset_id_for_legacy_pond,
    default_component_id_for_legacy_device,
    default_probe_profile_id_for_legacy_device,
    default_site_id,
    deployment_id_for_legacy_device,
)
from limnopulse_api.domain.legacy_projection import (
    asset_and_profile_for_legacy_pond,
    asset_status_from_legacy,
    component_for_legacy_device,
    default_site_for_tenant,
    deployment_for_legacy_device,
    device_status_from_legacy,
    device_v2_from_legacy,
)

CREATED = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
UPDATED = datetime(2026, 2, 3, 4, 5, 6, tzinfo=UTC)


def legacy_tenant(**overrides) -> Tenant:
    values = {
        "tenant_id": "tnt_local_001",
        "name": "Local Farm",
        "created_at": CREATED,
        "updated_at": UPDATED,
        "version": 4,
    }
    values.update(overrides)
    return Tenant(**values)


def legacy_pond(**overrides) -> Pond:
    values = {
        "tenant_id": "tnt_local_001",
        "pond_id": "pond_local_001",
        "name": "North pond",
        "description": "Tilapia",
        "created_at": CREATED,
        "updated_at": UPDATED,
        "version": 3,
    }
    values.update(overrides)
    return Pond(**values)


def legacy_device(**overrides) -> Device:
    values = {
        "tenant_id": "tnt_local_001",
        "pond_id": "pond_local_001",
        "device_id": "local-device-001",
        "name": "Probe gateway",
        "firmware_version": "1.2.3",
        "created_at": CREATED,
        "updated_at": UPDATED,
        "version": 2,
    }
    values.update(overrides)
    return Device(**values)


def test_default_site_uses_tenant_identity_name_and_window() -> None:
    site = default_site_for_tenant(legacy_tenant())

    assert site.site_id == default_site_id("tnt_local_001")
    assert site.tenant_id == "tnt_local_001"
    assert site.name == "Local Farm"
    assert site.status is SiteStatus.ACTIVE
    assert (site.created_at, site.updated_at, site.version) == (CREATED, UPDATED, 4)


def test_default_site_is_deterministic() -> None:
    first = default_site_for_tenant(legacy_tenant())
    second = default_site_for_tenant(legacy_tenant())
    assert first == second


def test_asset_and_profile_copy_legacy_pond() -> None:
    bundle = asset_and_profile_for_legacy_pond(legacy_pond(), default_site_id("tnt_local_001"))

    asset_id = asset_id_for_legacy_pond("tnt_local_001", "pond_local_001")
    assert bundle.asset.asset_id == asset_id
    assert bundle.asset.site_id == default_site_id("tnt_local_001")
    assert bundle.asset.asset_type is AssetType.POND
    assert bundle.asset.name == "North pond"
    assert bundle.asset.status is AssetStatus.ACTIVE
    assert (bundle.asset.created_at, bundle.asset.updated_at, bundle.asset.version) == (
        CREATED,
        UPDATED,
        3,
    )
    assert bundle.pond_profile.asset_id == asset_id
    assert bundle.pond_profile.description == "Tilapia"
    assert bundle.pond_profile.legacy_pond_id == "pond_local_001"
    assert bundle.pond_profile.version == 3


def test_legacy_second_precision_timestamps_are_preserved_exactly() -> None:
    raw = "2026-01-02T03:04:05+00:00"
    pond = Pond.model_validate(
        {
            "tenant_id": "tnt_a",
            "pond_id": "p1",
            "name": "P",
            "created_at": raw,
            "updated_at": raw,
            "version": 1,
        }
    )
    bundle = asset_and_profile_for_legacy_pond(pond, "site_1")

    assert bundle.asset.created_at == datetime.fromisoformat(raw)
    assert bundle.asset.created_at.isoformat() == raw


def test_naive_and_offset_legacy_timestamps_are_normalized_to_utc() -> None:
    naive = legacy_tenant(created_at=datetime(2026, 1, 2), updated_at=datetime(2026, 1, 3))
    site = default_site_for_tenant(naive)
    assert site.created_at == datetime(2026, 1, 2, tzinfo=UTC)

    offset = timezone(timedelta(hours=-3))
    pond = legacy_pond(
        created_at=datetime(2026, 1, 2, 0, 0, tzinfo=offset),
        updated_at=datetime(2026, 1, 2, 0, 0, tzinfo=offset),
    )
    bundle = asset_and_profile_for_legacy_pond(pond, "site_1")
    assert bundle.asset.created_at == datetime(2026, 1, 2, 3, 0, tzinfo=UTC)
    assert bundle.asset.created_at.utcoffset() == timedelta(0)


@pytest.mark.parametrize(
    ("legacy", "expected"),
    [
        ("active", AssetStatus.ACTIVE),
        ("archived", AssetStatus.ARCHIVED),
        ("ARCHIVED", AssetStatus.ARCHIVED),
        (" Active ", AssetStatus.ACTIVE),
        ("deleted", AssetStatus.ACTIVE),
        ("", AssetStatus.ACTIVE),
    ],
)
def test_asset_status_mapping_never_invents_terminal_state(legacy, expected) -> None:
    assert asset_status_from_legacy(legacy) is expected
    pond = legacy_pond(status=legacy)
    assert asset_and_profile_for_legacy_pond(pond, "site_1").asset.status is expected


@pytest.mark.parametrize(
    ("legacy", "expected"),
    [
        ("active", DeviceStatus.ACTIVE),
        ("provisioning", DeviceStatus.PROVISIONING),
        ("suspended", DeviceStatus.SUSPENDED),
        ("decommissioned", DeviceStatus.DECOMMISSIONED),
        ("Suspended", DeviceStatus.SUSPENDED),
        ("archived", DeviceStatus.SUSPENDED),
        ("offline", DeviceStatus.SUSPENDED),
        ("", DeviceStatus.SUSPENDED),
    ],
)
def test_device_status_mapping_sends_unknown_values_to_reversible_suspended(
    legacy, expected
) -> None:
    assert device_status_from_legacy(legacy) is expected
    assert device_v2_from_legacy(legacy_device(status=legacy)).status is expected


def test_device_v2_from_legacy_drops_pond_and_auth_bindings() -> None:
    device = device_v2_from_legacy(legacy_device())

    assert device.device_id == "local-device-001"
    assert device.tenant_id == "tnt_local_001"
    assert device.name == "Probe gateway"
    assert device.firmware_version == "1.2.3"
    assert device.compatibility_level is CompatibilityLevel.CUSTOM
    assert device.model_profile_key is None
    assert (device.created_at, device.updated_at, device.version) == (CREATED, UPDATED, 2)
    dumped = device.model_dump()
    assert "pond_id" not in dumped
    assert "auth_type" not in dumped


def test_component_for_legacy_device_is_deterministic_probe() -> None:
    component = component_for_legacy_device(legacy_device())

    assert component.component_id == default_component_id_for_legacy_device(
        "tnt_local_001", "local-device-001"
    )
    assert component.device_id == "local-device-001"
    assert component.kind is ComponentKind.PROBE
    assert component.status is ComponentStatus.ACTIVE
    assert component.actuator_profile is None
    assert component.probe_profile is not None
    assert component.probe_profile.probe_profile_id == (
        default_probe_profile_id_for_legacy_device("tnt_local_001", "local-device-001")
    )
    assert (component.created_at, component.updated_at, component.version) == (
        CREATED,
        UPDATED,
        1,
    )
    assert component == component_for_legacy_device(legacy_device())


def test_component_for_decommissioned_legacy_device_is_decommissioned() -> None:
    component = component_for_legacy_device(legacy_device(status="decommissioned"))
    assert component.status is ComponentStatus.DECOMMISSIONED


def test_deployment_for_legacy_device_starts_at_device_creation() -> None:
    asset_id = asset_id_for_legacy_pond("tnt_local_001", "pond_local_001")
    site_id = default_site_id("tnt_local_001")
    deployment = deployment_for_legacy_device(legacy_device(), asset_id, site_id)

    assert deployment.deployment_id == deployment_id_for_legacy_device(
        "tnt_local_001", "local-device-001"
    )
    assert deployment.component_id == default_component_id_for_legacy_device(
        "tnt_local_001", "local-device-001"
    )
    assert deployment.asset_id == asset_id
    assert deployment.site_id == site_id
    assert deployment.started_at == CREATED
    assert deployment.ended_at is None
    assert deployment.status is DeploymentStatus.ACTIVE
    assert (deployment.created_at, deployment.updated_at, deployment.version) == (
        CREATED,
        UPDATED,
        1,
    )
    assert deployment == deployment_for_legacy_device(legacy_device(), asset_id, site_id)


def test_projection_ids_are_tenant_scoped() -> None:
    first = component_for_legacy_device(legacy_device())
    second = component_for_legacy_device(legacy_device(tenant_id="tnt_other"))
    assert first.component_id != second.component_id
