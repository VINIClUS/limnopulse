"""Per-message ingest state: monotone marks plus best-effort sequence events (D11).

`IngestStateService` wraps the P3-00 `build_ingest_marks` and `classify_component_sequence`:

- marks are applied through `IngestStateRepository.apply_ingest_marks` (monotone, idempotent);
  a replay or an unverified fence yields no mark, and the repository is not called at all;
- sequence events are classified against the state read **once** per message, before this
  message's marks are applied, and only when the message is not a replay and
  `receive_count == 1`. They feed metrics only; a redriven message (receive count reset to 1)
  is classified again, so events may double-count (best-effort).
"""

from collections import defaultdict
from collections.abc import Callable
from datetime import datetime

from limnopulse_api.adapters.dynamodb_v2 import utc_now
from limnopulse_api.domain.ingestion import (
    IngestMarks,
    IngressEnvelope,
    IngressFence,
    SequenceEvent,
    build_ingest_marks,
    classify_component_sequence,
)
from limnopulse_api.repositories.ingestion import IngestStateRepository
from limnopulse_api.services.telemetry_pipeline import PipelineOutcome


class IngestStateService:
    def __init__(
        self, repository: IngestStateRepository, *, clock: Callable[[], datetime] = utc_now
    ) -> None:
        self.repository = repository
        self.clock = clock

    async def record_outcome(
        self, envelope: IngressEnvelope, outcome: PipelineOutcome, *, receive_count: int
    ) -> tuple[SequenceEvent, ...]:
        """Apply the message's marks; return its sequence events (empty unless classified)."""
        if envelope.replay:
            return ()
        events: tuple[SequenceEvent, ...] = ()
        if receive_count == 1:
            events = await self._classify(envelope, outcome)
        await self._apply(build_ingest_marks(envelope, outcome, ingested_at=self.clock()))
        return events

    async def record_fence(self, envelope: IngressEnvelope, fence: IngressFence) -> None:
        """A verified terminal fence marks connector health only (D8, A40)."""
        await self._apply(build_ingest_marks(envelope, None, fence=fence, ingested_at=self.clock()))

    async def _classify(
        self, envelope: IngressEnvelope, outcome: PipelineOutcome
    ) -> tuple[SequenceEvent, ...]:
        pairs: dict[str, list[tuple[datetime, int | None]]] = defaultdict(list)
        for observation in outcome.accepted:
            sequence = observation.observation.sequence
            if sequence is not None:
                pairs[observation.resolved.component_id].append(
                    (observation.observation.observed_at, sequence)
                )
        if not pairs:
            return ()
        states = await self.repository.get_component_states(envelope.tenant_id, sorted(pairs))
        events: list[SequenceEvent] = []
        for component_id in sorted(pairs):
            events.extend(
                classify_component_sequence(states.get(component_id), pairs[component_id])
            )
        return tuple(events)

    async def _apply(self, marks: IngestMarks) -> None:
        connector = marks.connector_mark
        if (
            marks.component_marks
            or marks.device_mark is not None
            or any(value is not None for value in connector.model_dump().values())
        ):
            await self.repository.apply_ingest_marks(marks)
