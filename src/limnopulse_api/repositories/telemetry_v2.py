from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal, Protocol, runtime_checkable

from limnopulse_api.domain.ids import require_canonical_id, require_device_id
from limnopulse_api.domain.telemetry_v2 import (
    CanonicalObservation,
    QualityState,
    StoredObservation,
)
from limnopulse_api.domain.telemetry_v2_schema import LegacyPoint


@runtime_checkable
class ObservationWriter(Protocol):
    async def write_observations(self, observations: Sequence[CanonicalObservation]) -> None: ...


@runtime_checkable
class LegacyObservationWriter(Protocol):
    async def write_legacy_points(self, points: Sequence[LegacyPoint]) -> None: ...


@dataclass(frozen=True)
class ObservationSelector:
    kind: Literal["asset", "component", "device"]
    entity_id: str

    def __post_init__(self) -> None:
        if self.kind == "asset":
            require_canonical_id(self.entity_id, "ast")
        elif self.kind == "component":
            require_canonical_id(self.entity_id, "cmp")
        elif self.kind == "device":
            require_device_id(self.entity_id)
        else:
            raise ValueError("selector kind must be asset, component or device")


@runtime_checkable
class ObservationReader(Protocol):
    """Windows are half-open by `observed_at`: `[start, stop)` and `[now - lookback, now)`."""

    async def query_observations(
        self,
        *,
        tenant_id: str,
        selector: ObservationSelector,
        metric_keys: frozenset[str] | None,
        quality_states: frozenset[QualityState] | None,
        start: datetime,
        stop: datetime,
        limit: int,
    ) -> list[StoredObservation]: ...

    async def query_latest_observations(
        self,
        *,
        tenant_id: str,
        selector: ObservationSelector,
        metric_keys: frozenset[str] | None,
        lookback: timedelta,
        now: datetime,
    ) -> list[StoredObservation]: ...
