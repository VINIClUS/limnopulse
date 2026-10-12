from datetime import datetime
from enum import StrEnum

from pydantic import model_validator

from limnopulse_api.domain.v2_common import (
    SafeId,
    V2VersionedEntity,
    ensure_status_transition,
    require_utc,
)


class DeploymentStatus(StrEnum):
    ACTIVE = "active"
    ENDED = "ended"


_DEPLOYMENT_TRANSITIONS = {DeploymentStatus.ACTIVE: frozenset({DeploymentStatus.ENDED})}


class Deployment(V2VersionedEntity):
    """Half-open interval `[started_at, ended_at)` placing one Component on one Asset."""

    tenant_id: SafeId
    deployment_id: SafeId
    component_id: SafeId
    site_id: SafeId
    asset_id: SafeId
    started_at: datetime
    ended_at: datetime | None = None
    status: DeploymentStatus = DeploymentStatus.ACTIVE

    @model_validator(mode="after")
    def validate_interval(self) -> "Deployment":
        require_utc(self.started_at, "started_at")
        if self.ended_at is not None:
            require_utc(self.ended_at, "ended_at")
            if self.ended_at <= self.started_at:
                raise ValueError("ended_at must be after started_at")
        if (self.status is DeploymentStatus.ENDED) != (self.ended_at is not None):
            raise ValueError("ended_at must be set exactly when status is ended")
        return self


def ensure_deployment_transition(current: DeploymentStatus, target: DeploymentStatus) -> None:
    ensure_status_transition(current, target, _DEPLOYMENT_TRANSITIONS, entity="deployment")
