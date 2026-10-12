from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from limnopulse_api.api.v2.schemas.common import (
    UtcTimestamp,
    V2ExpectedVersionRequest,
    V2VersionedResponse,
)
from limnopulse_api.api.v2.schemas.sites import SiteIdField
from limnopulse_api.domain.assets import AssetStatus, AssetType, AssetWithPondProfile, PondProfile

ASSET_ID_PATTERN = r"^ast_[0-9a-f]{32}$"

AssetName = Annotated[str, Field(min_length=1, max_length=120)]


class AssetCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    site_id: SiteIdField
    asset_type: AssetType = Field(alias="type")
    name: AssetName
    pond_description: str | None = None


class AssetPatch(V2ExpectedVersionRequest):
    """`site_id` and `type` are immutable; `None` leaves a field unchanged."""

    name: AssetName | None = None
    status: AssetStatus | None = None
    pond_description: str | None = None
    pond_profile_expected_version: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def validate_profile_fence(self) -> "AssetPatch":
        if (self.pond_description is None) != (self.pond_profile_expected_version is None):
            raise ValueError(
                "pond_description and pond_profile_expected_version must be sent together"
            )
        return self


class PondProfileResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    description: str | None = None
    legacy_pond_id: str | None = None
    schema_version: Literal[2] = 2
    created_at: UtcTimestamp
    updated_at: UtcTimestamp
    version: int

    @classmethod
    def from_domain(cls, profile: PondProfile) -> "PondProfileResponse":
        return cls(
            description=profile.description,
            legacy_pond_id=profile.legacy_pond_id,
            schema_version=profile.schema_version,
            created_at=profile.created_at,
            updated_at=profile.updated_at,
            version=profile.version,
        )


class AssetResponse(V2VersionedResponse):
    model_config = ConfigDict(extra="forbid", validate_by_name=True, validate_by_alias=True)

    asset_id: str
    site_id: str
    asset_type: AssetType = Field(alias="type")
    name: str
    status: AssetStatus
    pond_profile: PondProfileResponse

    @classmethod
    def from_domain(cls, value: AssetWithPondProfile) -> "AssetResponse":
        asset = value.asset
        return cls(
            tenant_id=asset.tenant_id,
            asset_id=asset.asset_id,
            site_id=asset.site_id,
            asset_type=asset.asset_type,
            name=asset.name,
            status=asset.status,
            schema_version=asset.schema_version,
            created_at=asset.created_at,
            updated_at=asset.updated_at,
            version=asset.version,
            pond_profile=PondProfileResponse.from_domain(value.pond_profile),
        )


class AssetListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[AssetResponse]
