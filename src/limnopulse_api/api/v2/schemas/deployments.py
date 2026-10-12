from typing import Annotated

from fastapi import Path
from pydantic import AfterValidator, BaseModel

from limnopulse_api.api.v2.schemas.common import UtcTimestamp, V2VersionedResponse
from limnopulse_api.domain.deployments import Deployment, DeploymentStatus
from limnopulse_api.domain.ids import require_canonical_id
from limnopulse_api.services.deployments import DeploymentEndCommand, DeploymentStartCommand


def _require_deployment_id(value: str) -> str:
    return require_canonical_id(value, "dep")


# Malformed IDs are 422; unknown well-formed IDs are 404.
V2DeploymentPathId = Annotated[str, Path(), AfterValidator(_require_deployment_id)]


class V2DeploymentStartRequest(DeploymentStartCommand):
    """`component_id`, `asset_id` and an offset-aware `started_at`; `site_id` is derived."""


class V2DeploymentEndRequest(DeploymentEndCommand):
    """Ends the active interval at an offset-aware `ended_at`, fenced by `expected_version`."""


class V2DeploymentResponse(V2VersionedResponse):
    deployment_id: str
    component_id: str
    site_id: str
    asset_id: str
    started_at: UtcTimestamp
    ended_at: UtcTimestamp | None = None
    status: DeploymentStatus

    @classmethod
    def from_domain(cls, deployment: Deployment) -> "V2DeploymentResponse":
        return cls.model_validate(deployment.model_dump())


class V2DeploymentListResponse(BaseModel):
    items: list[V2DeploymentResponse]
