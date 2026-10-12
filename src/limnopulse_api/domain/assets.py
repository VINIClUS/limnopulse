from enum import StrEnum

from pydantic import BaseModel, ConfigDict, model_validator

from limnopulse_api.domain.v2_common import SafeId, V2VersionedEntity, ensure_status_transition


class SiteStatus(StrEnum):
    ACTIVE = "active"
    ARCHIVED = "archived"


class AssetType(StrEnum):
    POND = "pond"


class AssetStatus(StrEnum):
    ACTIVE = "active"
    ARCHIVED = "archived"


_SITE_TRANSITIONS = {SiteStatus.ACTIVE: frozenset({SiteStatus.ARCHIVED})}
_ASSET_TRANSITIONS = {AssetStatus.ACTIVE: frozenset({AssetStatus.ARCHIVED})}


class Site(V2VersionedEntity):
    tenant_id: SafeId
    site_id: SafeId
    name: str
    status: SiteStatus = SiteStatus.ACTIVE


class Asset(V2VersionedEntity):
    tenant_id: SafeId
    asset_id: SafeId
    site_id: SafeId
    asset_type: AssetType
    name: str
    status: AssetStatus = AssetStatus.ACTIVE


class PondProfile(V2VersionedEntity):
    tenant_id: SafeId
    asset_id: SafeId
    description: str | None = None
    legacy_pond_id: SafeId | None = None


class AssetWithPondProfile(BaseModel):
    model_config = ConfigDict(frozen=True)

    asset: Asset
    pond_profile: PondProfile

    @model_validator(mode="after")
    def validate_profile_owner(self) -> "AssetWithPondProfile":
        if (self.asset.tenant_id, self.asset.asset_id) != (
            self.pond_profile.tenant_id,
            self.pond_profile.asset_id,
        ):
            raise ValueError("pond_profile must belong to the asset")
        return self


def ensure_site_transition(current: SiteStatus, target: SiteStatus) -> None:
    ensure_status_transition(current, target, _SITE_TRANSITIONS, entity="site")


def ensure_asset_transition(current: AssetStatus, target: AssetStatus) -> None:
    ensure_status_transition(current, target, _ASSET_TRANSITIONS, entity="asset")
