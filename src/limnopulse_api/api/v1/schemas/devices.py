from pydantic import BaseModel, Field

from limnopulse_api.api.v1.schemas.common import VersionedResponse


class DeviceCreate(BaseModel):
    pond_id: str
    name: str = Field(min_length=1, max_length=120)
    firmware_version: str | None = None


class DeviceUpdate(BaseModel):
    expected_version: int
    pond_id: str | None = None
    name: str | None = Field(default=None, min_length=1, max_length=120)
    firmware_version: str | None = None


class DeviceResponse(VersionedResponse):
    tenant_id: str
    pond_id: str
    device_id: str
    name: str
    auth_type: str
    firmware_version: str | None = None


class DeviceListResponse(BaseModel):
    items: list[DeviceResponse]
