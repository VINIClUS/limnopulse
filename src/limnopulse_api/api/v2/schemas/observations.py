"""Query and response models of the `/v2` observation routes.

Domain enums are exposed as plain strings so every OpenAPI component these routes add keeps
the `V2` prefix and never collides with a `/v1` component name.
"""

from datetime import UTC, datetime
from typing import Annotated

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

from limnopulse_api.api.v2.schemas.common import UtcTimestamp
from limnopulse_api.domain.metric_catalog import metric_definition
from limnopulse_api.domain.telemetry_v2 import (
    MAX_TELEMETRY_TIME,
    MIN_TELEMETRY_TIME,
    POINT_QUALITY_STATES,
    QualityState,
    StoredObservation,
)
from limnopulse_api.repositories.telemetry_v2 import ObservationSelector

DEFAULT_OBSERVATION_LIMIT = 500
MAX_OBSERVATION_LIMIT = 1000
_POINT_STATE_VALUES = frozenset(str(state) for state in POINT_QUALITY_STATES)


def _require_query_bound(value: datetime) -> datetime:
    """Closed `[MIN_TELEMETRY_TIME, MAX_TELEMETRY_TIME]`: the query window itself is half-open."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must be timezone-aware")
    try:
        utc = value.astimezone(UTC)
    except OverflowError:
        raise ValueError("timestamp outside [1970-01-01, 2262-01-01] UTC") from None
    if not MIN_TELEMETRY_TIME <= utc <= MAX_TELEMETRY_TIME:
        raise ValueError("timestamp outside [1970-01-01, 2262-01-01] UTC")
    return utc


def _require_catalog_metric(value: str) -> str:
    if metric_definition(value) is None:
        raise ValueError("metric_key is not in the metric catalog")
    return value


def _require_point_state(value: str) -> str:
    if value not in _POINT_STATE_VALUES:
        raise ValueError(f"quality_state must be one of {sorted(_POINT_STATE_VALUES)}")
    return value


QueryBound = Annotated[datetime, AfterValidator(_require_query_bound)]
CatalogMetricKey = Annotated[str, AfterValidator(_require_catalog_metric)]
PointQualityState = Annotated[str, AfterValidator(_require_point_state)]


class _V2SelectorQuery(BaseModel):
    """Exactly one of `asset_id`, `component_id` or `device_id`, validated like the reader."""

    model_config = ConfigDict(extra="forbid")

    asset_id: str | None = None
    component_id: str | None = None
    device_id: str | None = None
    metric_key: list[CatalogMetricKey] | None = None

    @model_validator(mode="after")
    def validate_selector(self) -> "_V2SelectorQuery":
        self.selector()
        return self

    def selector(self) -> ObservationSelector:
        chosen = [
            (kind, entity_id)
            for kind, entity_id in (
                ("asset", self.asset_id),
                ("component", self.component_id),
                ("device", self.device_id),
            )
            if entity_id is not None
        ]
        if len(chosen) != 1:
            raise ValueError("exactly one of asset_id, component_id or device_id is required")
        kind, entity_id = chosen[0]
        return ObservationSelector(kind, entity_id)  # type: ignore[arg-type]

    def metric_keys(self) -> frozenset[str] | None:
        return None if self.metric_key is None else frozenset(self.metric_key)


class V2ObservationQuery(_V2SelectorQuery):
    quality_state: list[PointQualityState] | None = None
    start: QueryBound | None = None
    stop: QueryBound | None = None
    limit: int = Field(default=DEFAULT_OBSERVATION_LIMIT, ge=1, le=MAX_OBSERVATION_LIMIT)

    def quality_states(self) -> frozenset[QualityState] | None:
        if self.quality_state is None:
            return None
        return frozenset(QualityState(state) for state in self.quality_state)


class V2LatestObservationQuery(_V2SelectorQuery):
    pass


class V2ObservationQualityResponse(BaseModel):
    state: str
    flags: list[str]
    provenance: list[str]


class V2ObservationResponse(BaseModel):
    """`integration_id`, `source_event_id` and source metadata are never exposed."""

    event_id: str
    observed_at: UtcTimestamp
    received_at: UtcTimestamp
    ingested_at: UtcTimestamp
    metric_key: str
    value: float
    unit: str
    site_id: str
    asset_id: str
    device_id: str
    component_id: str
    probe_id: str | None
    deployment_id: str
    quality: V2ObservationQualityResponse
    sequence: int | None
    source_type: str

    @classmethod
    def from_domain(cls, observation: StoredObservation) -> "V2ObservationResponse":
        return cls(
            event_id=observation.event_id,
            observed_at=observation.observed_at,
            received_at=observation.received_at,
            ingested_at=observation.ingested_at,
            metric_key=observation.metric_key,
            value=observation.value,
            unit=observation.canonical_unit,
            site_id=observation.site_id,
            asset_id=observation.asset_id,
            device_id=observation.device_id,
            component_id=observation.component_id,
            probe_id=observation.probe_id,
            deployment_id=observation.deployment_id,
            quality=V2ObservationQualityResponse(
                state=str(observation.quality_state),
                flags=[str(flag) for flag in observation.quality_flags],
                provenance=[str(entry) for entry in observation.quality_provenance],
            ),
            sequence=observation.sequence,
            source_type=str(observation.source_type),
        )


class V2ObservationListResponse(BaseModel):
    items: list[V2ObservationResponse]
