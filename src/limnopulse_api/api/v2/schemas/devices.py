from typing import Annotated

from fastapi import Path
from pydantic import AfterValidator, BaseModel

from limnopulse_api.api.v2.schemas.common import V2VersionedResponse
from limnopulse_api.domain.devices_v2 import CompatibilityLevel, DeviceStatus, DeviceV2
from limnopulse_api.domain.ids import require_device_id
from limnopulse_api.services.devices_v2 import DeviceV2CreateCommand, DeviceV2UpdateCommand

# Malformed IDs are 422; historical IDs such as `local-device-001` stay valid.
V2DevicePathId = Annotated[str, Path(), AfterValidator(require_device_id)]


class V2DeviceCreateRequest(DeviceV2CreateCommand):
    """Rejects `tenant_id`, `pond_id`, `auth_type` and any provider identifier (extra=forbid)."""


class V2DeviceUpdateRequest(DeviceV2UpdateCommand):
    """Optimistic PATCH; `None` leaves a field unchanged."""


class V2DeviceResponse(V2VersionedResponse):
    device_id: str
    name: str
    model_profile_key: str | None = None
    firmware_version: str | None = None
    compatibility_level: CompatibilityLevel
    status: DeviceStatus

    @classmethod
    def from_domain(cls, device: DeviceV2) -> "V2DeviceResponse":
        return cls.model_validate(device.model_dump())


class V2DeviceListResponse(BaseModel):
    items: list[V2DeviceResponse]
