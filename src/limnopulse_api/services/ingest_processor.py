"""Telemetry ingress message processing (P3-08, D8): one queued envelope -> one decision.

The D8 order is fixed: parse the envelope, re-resolve and fence it (ADR-001), run the P2
`TelemetryPipeline`, persist the quarantine rows, apply the monotone ingest-state marks, emit the
metrics, and only then answer `delete`. Every failure follows the frozen worker failure table
(`INGRESS_FAILURE_TABLE`), so a message is never deleted before its durable writes landed:

- a terminal fence on a verified account persists one replayable `fenced_<reason>` row per
  observation plus the summary row, then marks connector health, then deletes;
- a temporary fence (`IngressDefer`) and every retryable failure change the visibility by the
  full-jitter backoff; an unverified (forged) fence writes nothing and uses the permanent
  visibility, so `maxReceiveCount` moves it to the DLQ;
- the processed and fenced metrics follow the durable writes, so a write failure never counts
  them twice; the deferred, unverified-fence and write-failure counters count once per receive.
  A failing metrics call is logged by metric name and never changes the decision.

The processor never deletes or changes visibility itself; `IngestWorkerLoop` applies the
decision. It never emits `ingress_messages_total` either (A46): the loop alone knows the final
category, because the processing deadline can override the decision. Failures are logged by
category only, never with a traceback or an exception message, which can carry identifiers.
"""

import logging
import random as _random
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Protocol

from botocore.exceptions import BotoCoreError, ClientError

from limnopulse_api.core.errors import TelemetryWriteError
from limnopulse_api.domain.ingestion import (
    INGRESS_FAILURE_TABLE,
    PERMANENT_VISIBILITY_SECONDS,
    IngressDefer,
    IngressEnvelope,
    IngressFailureCategory,
    IngressFence,
    IngressMessageAction,
    IngressQueueMessage,
    SequenceEvent,
    TrustedIngress,
    build_quarantine_entries,
    ingress_backoff_seconds,
)
from limnopulse_api.domain.telemetry_v2 import SourceObservation, TrustedSourceContext
from limnopulse_api.repositories.ingestion import (
    QuarantineRepository,
    TelemetryMetrics,
    TrustedMapping,
)
from limnopulse_api.services.telemetry_pipeline import PipelineOutcome

logger = logging.getLogger("limnopulse.telemetry.ingest")

_Category = IngressFailureCategory
_STORE_ERRORS = (BotoCoreError, ClientError)


class ProcessAction(StrEnum):
    DELETE = "delete"
    VISIBILITY = "visibility"


@dataclass(frozen=True)
class ProcessDecision:
    """The final SQS action for one message; `seconds` is set only for `visibility`."""

    action: ProcessAction
    seconds: int | None
    category: IngressFailureCategory


@dataclass(frozen=True)
class RetryPolicy:
    """`backoff(n) = clamp(ceil(random() * min(cap, base * 2**(n-1))), 1, 43200)`, full jitter."""

    base: int = 30
    cap: int = 900
    permanent_visibility: int = PERMANENT_VISIBILITY_SECONDS
    random: Callable[[], float] = _random.random

    def backoff(self, receive_count: int) -> int:
        return ingress_backoff_seconds(
            receive_count, base=self.base, cap=self.cap, random=self.random
        )

    def decision(self, category: IngressFailureCategory, receive_count: int) -> ProcessDecision:
        """The failure-table action for `category`, with its visibility seconds."""
        action = INGRESS_FAILURE_TABLE[category][1]
        if action is IngressMessageAction.DELETE:
            return ProcessDecision(ProcessAction.DELETE, None, category)
        if action is IngressMessageAction.PERMANENT:
            return ProcessDecision(ProcessAction.VISIBILITY, self.permanent_visibility, category)
        return ProcessDecision(ProcessAction.VISIBILITY, self.backoff(receive_count), category)


class IngestPipeline(Protocol):
    """The `TelemetryPipeline.ingest` surface the processor uses."""

    async def ingest(
        self, context: TrustedSourceContext, observations: tuple[SourceObservation, ...]
    ) -> PipelineOutcome: ...


class IngestState(Protocol):
    """The `IngestStateService` surface the processor uses."""

    async def record_outcome(
        self, envelope: IngressEnvelope, outcome: PipelineOutcome, *, receive_count: int
    ) -> tuple[SequenceEvent, ...]: ...

    async def record_fence(self, envelope: IngressEnvelope, fence: IngressFence) -> None: ...


class _ContractViolation(Exception):
    """A `ValueError` raised by the pipeline itself (unknown mapping version, over-limit batch)."""


class IngestMessageProcessor:
    def __init__(
        self,
        *,
        mapping: TrustedMapping,
        pipeline: IngestPipeline,
        quarantine: QuarantineRepository,
        state: IngestState,
        metrics: TelemetryMetrics,
        clock: Callable[[], datetime],
        retry: RetryPolicy | None = None,
    ) -> None:
        self.mapping = mapping
        self.pipeline = pipeline
        self.quarantine = quarantine
        self.state = state
        self.metrics = metrics
        self.clock = clock
        self.retry = retry or RetryPolicy()

    async def process(self, message: IngressQueueMessage) -> ProcessDecision:
        """Process one message; a cancellation (the loop's deadline) propagates unchanged."""
        category = await self._categorize(message)
        if category not in (_Category.PROCESSED, _Category.FENCED):
            logger.warning("ingress message not processed: category=%s", category.value)
        return self.retry.decision(category, message.receive_count)

    async def _categorize(self, message: IngressQueueMessage) -> IngressFailureCategory:
        try:
            envelope = IngressEnvelope.model_validate_json(message.body)
        except ValueError:
            return _Category.ENVELOPE_INVALID
        try:
            return await self._process(envelope, message.receive_count)
        except TelemetryWriteError as error:
            self._increment(
                "ingress_write_failures_total", retryable="true" if error.retryable else "false"
            )
            return _Category.WRITE_RETRYABLE if error.retryable else _Category.WRITE_PERMANENT
        except _STORE_ERRORS:
            return _Category.STORE_UNAVAILABLE
        except _ContractViolation:
            return _Category.CONTRACT_VIOLATION
        except Exception:  # noqa: BLE001 - failure table: any other exception is unexpected.
            return _Category.UNEXPECTED

    async def _process(
        self, envelope: IngressEnvelope, receive_count: int
    ) -> IngressFailureCategory:
        result = await self.mapping.fence(envelope)
        if isinstance(result, IngressDefer):
            self._increment("ingress_deferred_total", reason=result.reason.value)
            return _Category.DEFERRED
        if isinstance(result, IngressFence):
            return await self._fence(envelope, result)
        return await self._ingest(envelope, result, receive_count)

    async def _fence(
        self, envelope: IngressEnvelope, fence: IngressFence
    ) -> IngressFailureCategory:
        if not fence.account_verified:
            # A forged envelope: nothing is written, and the DLQ keeps the raw body.
            self._increment("ingress_fenced_total", reason=fence.reason.value)
            return _Category.FENCED_UNVERIFIED
        rows = build_quarantine_entries(envelope, fence=fence, now=self.clock())
        await self.quarantine.put_quarantine_entries(rows)
        await self.state.record_fence(envelope, fence)
        self._increment("ingress_fenced_total", reason=fence.reason.value)
        return _Category.FENCED

    async def _ingest(
        self, envelope: IngressEnvelope, trusted: TrustedIngress, receive_count: int
    ) -> IngressFailureCategory:
        started = self.clock()
        try:
            outcome = await self.pipeline.ingest(trusted.context, trusted.observations)
        except ValueError as error:
            raise _ContractViolation from error
        duration = max((self.clock() - started).total_seconds(), 0.0)
        rows = build_quarantine_entries(envelope, outcome=outcome, now=self.clock())
        if rows:
            await self.quarantine.put_quarantine_entries(rows)
        events = await self.state.record_outcome(envelope, outcome, receive_count=receive_count)
        self._emit_processed(outcome, events, duration)
        return _Category.PROCESSED

    def _emit_processed(
        self, outcome: PipelineOutcome, events: tuple[SequenceEvent, ...], duration: float
    ) -> None:
        if outcome.accepted:
            self._increment("ingress_observations_total", len(outcome.accepted), result="accepted")
        if outcome.quarantined:
            self._increment(
                "ingress_observations_total", len(outcome.quarantined), result="quarantined"
            )
        reasons = Counter(item.reason.value for item in outcome.quarantined)
        for reason, count in sorted(reasons.items()):
            self._increment("ingress_quarantined_total", count, reason=reason)
        flags = Counter(
            flag.value for observation in outcome.accepted for flag in observation.quality.flags
        )
        for flag, count in sorted(flags.items()):
            self._increment("ingress_quality_flags_total", count, flag=flag)
        for event, count in Counter(event.value for event in events).items():
            self._increment("ingress_sequence_events_total", count, event=event)
        try:
            self.metrics.observe("ingress_normalization_duration_seconds", duration)
        except Exception as error:  # noqa: BLE001 - metrics never decide.
            _metric_failed("ingress_normalization_duration_seconds", error)

    def _increment(self, name: str, value: int = 1, **labels: str) -> None:
        try:
            self.metrics.increment(name, value, **labels)
        except Exception as error:  # noqa: BLE001 - metrics never decide.
            _metric_failed(name, error)


def _metric_failed(name: str, error: Exception) -> None:
    logger.warning("ingress metric failed: metric=%s (%s)", name, type(error).__name__)
