"""Pure, deterministic v2 projections of legacy `/v1` Tenant, Pond and Device rows.

The builders perform no I/O. Identifiers come from the frozen compatibility ID functions and
timestamps/versions come from the legacy rows, so repeated projection of the same legacy row
always produces the same v2 entity.

Legacy `status` is a free string (`/v1` writes only "active"). Unknown values never map to a
terminal v2 state, because terminal states cannot be left:

- Pond -> Asset: "archived" -> ARCHIVED; anything else -> ACTIVE.
- Device -> DeviceV2: exact v2 values map 1:1; anything else -> SUSPENDED, which is
  reversible through SUSPENDED -> ACTIVE.
- Device -> default probe Component: DECOMMISSIONED device -> DECOMMISSIONED; else ACTIVE.

Matching is case-insensitive and ignores surrounding whitespace.
"""

from datetime import UTC, datetime

from limnopulse_api.domain.assets import (
    Asset,
    AssetStatus,
    AssetType,
    AssetWithPondProfile,
    PondProfile,
    Site,
    SiteStatus,
)
from limnopulse_api.domain.deployments import Deployment, DeploymentStatus
from limnopulse_api.domain.devices_v2 import (
    ComponentKind,
    ComponentStatus,
    DeviceComponent,
    DeviceStatus,
    DeviceV2,
    ProbeProfile,
)
from limnopulse_api.domain.entities import Device, Pond, Tenant
from limnopulse_api.domain.ids import (
    asset_id_for_legacy_pond,
    default_component_id_for_legacy_device,
    default_probe_profile_id_for_legacy_device,
    default_site_id,
    deployment_id_for_legacy_device,
)


def legacy_timestamp_to_utc(value: datetime) -> datetime:
    """Legacy rows store UTC; naive values are read as UTC and offsets converted to UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def asset_status_from_legacy(status: str) -> AssetStatus:
    if status.strip().lower() == AssetStatus.ARCHIVED.value:
        return AssetStatus.ARCHIVED
    return AssetStatus.ACTIVE


def device_status_from_legacy(status: str) -> DeviceStatus:
    try:
        return DeviceStatus(status.strip().lower())
    except ValueError:
        return DeviceStatus.SUSPENDED


def default_site_for_tenant(tenant: Tenant) -> Site:
    return Site(
        tenant_id=tenant.tenant_id,
        site_id=default_site_id(tenant.tenant_id),
        name=tenant.name,
        status=SiteStatus.ACTIVE,
        created_at=legacy_timestamp_to_utc(tenant.created_at),
        updated_at=legacy_timestamp_to_utc(tenant.updated_at),
        version=tenant.version,
    )


def asset_and_profile_for_legacy_pond(pond: Pond, site_id: str) -> AssetWithPondProfile:
    asset_id = asset_id_for_legacy_pond(pond.tenant_id, pond.pond_id)
    created_at = legacy_timestamp_to_utc(pond.created_at)
    updated_at = legacy_timestamp_to_utc(pond.updated_at)
    return AssetWithPondProfile(
        asset=Asset(
            tenant_id=pond.tenant_id,
            asset_id=asset_id,
            site_id=site_id,
            asset_type=AssetType.POND,
            name=pond.name,
            status=asset_status_from_legacy(pond.status),
            created_at=created_at,
            updated_at=updated_at,
            version=pond.version,
        ),
        pond_profile=PondProfile(
            tenant_id=pond.tenant_id,
            asset_id=asset_id,
            description=pond.description,
            legacy_pond_id=pond.pond_id,
            created_at=created_at,
            updated_at=updated_at,
            version=pond.version,
        ),
    )


def device_v2_from_legacy(device: Device) -> DeviceV2:
    return DeviceV2(
        tenant_id=device.tenant_id,
        device_id=device.device_id,
        name=device.name,
        firmware_version=device.firmware_version,
        status=device_status_from_legacy(device.status),
        created_at=legacy_timestamp_to_utc(device.created_at),
        updated_at=legacy_timestamp_to_utc(device.updated_at),
        version=device.version,
    )


def component_for_legacy_device(device: Device) -> DeviceComponent:
    decommissioned = device_status_from_legacy(device.status) is DeviceStatus.DECOMMISSIONED
    return DeviceComponent(
        tenant_id=device.tenant_id,
        component_id=default_component_id_for_legacy_device(device.tenant_id, device.device_id),
        device_id=device.device_id,
        kind=ComponentKind.PROBE,
        name=device.name,
        probe_profile=ProbeProfile(
            probe_profile_id=default_probe_profile_id_for_legacy_device(
                device.tenant_id, device.device_id
            )
        ),
        status=ComponentStatus.DECOMMISSIONED if decommissioned else ComponentStatus.ACTIVE,
        created_at=legacy_timestamp_to_utc(device.created_at),
        updated_at=legacy_timestamp_to_utc(device.updated_at),
        version=1,
    )


def deployment_for_legacy_device(device: Device, asset_id: str, site_id: str) -> Deployment:
    """Open Deployment of the default Component starting when the legacy Device was created."""
    created_at = legacy_timestamp_to_utc(device.created_at)
    return Deployment(
        tenant_id=device.tenant_id,
        deployment_id=deployment_id_for_legacy_device(device.tenant_id, device.device_id),
        component_id=default_component_id_for_legacy_device(device.tenant_id, device.device_id),
        site_id=site_id,
        asset_id=asset_id,
        started_at=created_at,
        status=DeploymentStatus.ACTIVE,
        created_at=created_at,
        updated_at=legacy_timestamp_to_utc(device.updated_at),
        version=1,
    )
