from pydantic import BaseModel, Field

from limnopulse_api.api.v1.schemas.common import VersionedResponse


class TenantCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)


class TenantUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=120)
    expected_version: int


class TenantResponse(VersionedResponse):
    tenant_id: str
    name: str


class TenantListResponse(BaseModel):
    items: list[TenantResponse]
