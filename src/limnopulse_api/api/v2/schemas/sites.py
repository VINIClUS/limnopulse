from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from limnopulse_api.api.v2.schemas.common import V2ExpectedVersionRequest, V2VersionedResponse
from limnopulse_api.domain.assets import Site, SiteStatus

SITE_ID_PATTERN = r"^site_[0-9a-f]{32}$"

SiteName = Annotated[str, Field(min_length=1, max_length=120)]
SiteIdField = Annotated[str, Field(pattern=SITE_ID_PATTERN)]


class SiteCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: SiteName


class SitePatch(V2ExpectedVersionRequest):
    """`None` leaves a field unchanged; clearing fields is not supported in M1."""

    name: SiteName | None = None
    status: SiteStatus | None = None


class SiteResponse(V2VersionedResponse):
    model_config = ConfigDict(extra="forbid")

    site_id: str
    name: str
    status: SiteStatus

    @classmethod
    def from_domain(cls, site: Site) -> "SiteResponse":
        return cls(
            tenant_id=site.tenant_id,
            site_id=site.site_id,
            name=site.name,
            status=site.status,
            schema_version=site.schema_version,
            created_at=site.created_at,
            updated_at=site.updated_at,
            version=site.version,
        )


class SiteListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[SiteResponse]
