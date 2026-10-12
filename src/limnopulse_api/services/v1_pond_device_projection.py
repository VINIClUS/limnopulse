"""Pure v2 <-> `/v1` Pond/Device projection rules used by the compatibility adapter.

`/v1` reads are served from the legacy `POND#`/`DEVICE#` rows; these mappers exist to build the
v2 writes and to assert that the v2 projection never diverges from the legacy rows on the
fields both models share (name, description, status, Pond identity and current placement).
No function here performs I/O.
"""

from datetime import UTC, datetime, timedelta

from limnopulse_api.domain.assets import Asset, PondProfile
from limnopulse_api.domain.deployments import Deployment, DeploymentStatus
from limnopulse_api.domain.devices_v2 import DeviceV2
from limnopulse_api.domain.entities import Device, Pond
from limnopulse_api.domain.ids import default_component_id_for_legacy_device

V2_PROJECTION_VERSION = 1
LEGACY_AUTH_TYPE = "mtls"
# Attributes the projection adds to legacy rows; legacy mappers ignore them.
POND_SITE_LINK_ATTRIBUTE = "v2_site_id"
POND_ASSET_LINK_ATTRIBUTE = "v2_asset_id"
DEVICE_COMPONENT_LINK_ATTRIBUTE = "default_component_id"
PROJECTION_VERSION_ATTRIBUTE = "v2_projection_version"

_MIN_INTERVAL = timedelta(microseconds=1)


def legacy_now(now: datetime) -> datetime:
    """Legacy `/v1` rows store UTC with second precision."""
    return now.astimezone(UTC).replace(microsecond=0)


def pond_from_asset(asset: Asset, pond_profile: PondProfile) -> Pond:
    """The `/v1` Pond an Asset projects to; the profile must carry the legacy alias."""
    if pond_profile.legacy_pond_id is None:
        raise ValueError("asset has no legacy pond alias")
    if (pond_profile.tenant_id, pond_profile.asset_id) != (asset.tenant_id, asset.asset_id):
        raise ValueError("pond_profile must belong to the asset")
    return Pond(
        tenant_id=asset.tenant_id,
        pond_id=pond_profile.legacy_pond_id,
        name=asset.name,
        description=pond_profile.description,
        status=asset.status.value,
        created_at=asset.created_at,
        updated_at=asset.updated_at,
        version=asset.version,
    )


def device_v1_projection(
    device: DeviceV2,
    current_deployment: Deployment | None,
    pond_profile: PondProfile | None,
) -> Device | None:
    """The `/v1` Device, or `None` unless the default Component is deployed to a legacy Pond."""
    if current_deployment is None or pond_profile is None:
        return None
    if pond_profile.legacy_pond_id is None:
        return None
    expected_component = default_component_id_for_legacy_device(device.tenant_id, device.device_id)
    if (
        current_deployment.tenant_id != device.tenant_id
        or current_deployment.component_id != expected_component
        or current_deployment.status is not DeploymentStatus.ACTIVE
        or (pond_profile.tenant_id, pond_profile.asset_id)
        != (device.tenant_id, current_deployment.asset_id)
    ):
        return None
    return Device(
        tenant_id=device.tenant_id,
        pond_id=pond_profile.legacy_pond_id,
        device_id=device.device_id,
        name=device.name,
        auth_type=LEGACY_AUTH_TYPE,
        firmware_version=device.firmware_version,
        status=device.status.value,
        created_at=device.created_at,
        updated_at=device.updated_at,
        version=device.version,
    )


def transition_instant(now: datetime, current_started_at: datetime) -> datetime:
    """UTC instant of a Pond move: never at or before the start of the interval it ends."""
    return max(now.astimezone(UTC), current_started_at + _MIN_INTERVAL)


def ended_deployment(current: Deployment, ended_at: datetime, now: datetime) -> Deployment:
    """`current` closed at `ended_at`; target and start never change (no reassignment)."""
    return Deployment.model_validate(
        {
            **current.model_dump(),
            "ended_at": ended_at,
            "status": DeploymentStatus.ENDED,
            "updated_at": max(now.astimezone(UTC), current.updated_at),
            "version": current.version + 1,
        }
    )


def started_deployment(
    *,
    tenant_id: str,
    deployment_id: str,
    component_id: str,
    site_id: str,
    asset_id: str,
    started_at: datetime,
) -> Deployment:
    return Deployment(
        tenant_id=tenant_id,
        deployment_id=deployment_id,
        component_id=component_id,
        site_id=site_id,
        asset_id=asset_id,
        started_at=started_at,
        created_at=started_at,
        updated_at=started_at,
        version=1,
    )
