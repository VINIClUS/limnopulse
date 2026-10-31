"""Ack-safe telemetry pipeline (P2-06): normalize, then dual-write the accepted observations.

Ack contract (D9): a caller acknowledges a message only after `ingest*` returns. A
`TelemetryWriteError` propagates unchanged, so the message is not acknowledged and is
redelivered; idempotent identity makes redelivery safe. `ValueError` (an unknown mapping version
or an over-limit batch) is caller misuse and is raised before anything is written. Quarantined
records are returned, and the caller must persist them before acknowledging (P3).
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from limnopulse_api.domain.legacy_telemetry_mapping import map_legacy_reading
from limnopulse_api.domain.telemetry_v2 import (
    CanonicalObservation,
    NormalizationResult,
    QuarantinedObservation,
    SourceObservation,
    TrustedSourceContext,
)
from limnopulse_api.services.telemetry_normalizer import TelemetryNormalizer
from limnopulse_api.services.telemetry_v2_writes import (
    ObservationWriteService,
    TelemetryWriteOutcome,
)


@dataclass(frozen=True)
class PipelineOutcome:
    written: TelemetryWriteOutcome
    quarantined: tuple[QuarantinedObservation, ...]
    counts: Mapping[str, int]  # read-only; closed keys of `NormalizationResult.counts()`
    # The observations actually written; P3 derives ingest marks and sequence events from them.
    accepted: tuple[CanonicalObservation, ...] = ()


class TelemetryPipeline:
    def __init__(
        self, *, normalizer: TelemetryNormalizer, write_service: ObservationWriteService
    ) -> None:
        self.normalizer = normalizer
        self.write_service = write_service

    async def ingest(
        self, context: TrustedSourceContext, observations: Sequence[SourceObservation]
    ) -> PipelineOutcome:
        return await self._ingest(context, observations, ())

    async def ingest_legacy_reading(
        self, context: TrustedSourceContext, payload: Mapping[str, Any]
    ) -> PipelineOutcome:
        mapped = map_legacy_reading(context, payload)
        return await self._ingest(context, mapped.observations, mapped.quarantined)

    async def _ingest(
        self,
        context: TrustedSourceContext,
        observations: Sequence[SourceObservation],
        mapper_quarantined: tuple[QuarantinedObservation, ...],
    ) -> PipelineOutcome:
        normalized = await self.normalizer.normalize(context, observations)
        written = await self.write_service.write(normalized.accepted)
        result = NormalizationResult(
            accepted=normalized.accepted,
            quarantined=mapper_quarantined + normalized.quarantined,
        )
        return PipelineOutcome(
            written=written,
            quarantined=result.quarantined,
            counts=MappingProxyType(dict(result.counts())),
            accepted=result.accepted,
        )
