from pydantic import BaseModel, Field

from limnopulse_api.api.v1.schemas.common import VersionedResponse


class TenantCreate(BaseModel):
    city: str | None = Field(default=None, min_length=1, max_length=120)
    name: str = Field(min_length=1, max_length=120)


class TenantUpdate(BaseModel):
    city: str | None = Field(default=None, min_length=1, max_length=120)
    name: str | None = Field(default=None, min_length=1, max_length=120)
    expected_version: int


class TenantResponse(VersionedResponse):
    city: str | None = None
    tenant_id: str
    name: str


class TenantListResponse(BaseModel):
    items: list[TenantResponse]
