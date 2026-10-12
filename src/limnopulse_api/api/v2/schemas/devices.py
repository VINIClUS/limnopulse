from pydantic import BaseModel

from limnopulse_api.api.v2.schemas.common import V2VersionedResponse
from limnopulse_api.domain.devices_v2 import CompatibilityLevel, DeviceStatus, DeviceV2
from limnopulse_api.services.devices_v2 import DeviceV2CreateCommand, DeviceV2UpdateCommand


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
