from pydantic import BaseModel, Field

from limnopulse_api.api.v1.schemas.common import VersionedResponse


class PondCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    description: str | None = None


class PondUpdate(BaseModel):
    expected_version: int
    name: str | None = Field(default=None, min_length=1, max_length=120)
    description: str | None = None


class PondResponse(VersionedResponse):
    tenant_id: str
    pond_id: str
    name: str
    description: str | None = None


class PondListResponse(BaseModel):
    items: list[PondResponse]
