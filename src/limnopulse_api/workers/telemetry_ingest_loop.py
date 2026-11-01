"""The telemetry ingress worker loop (P3-08, D7): long polling, bounded concurrency, heartbeat,
processing deadline and SIGTERM drain. Composition and the entrypoint belong to P3-10.

- **Receive sizing.** Each receive asks for `min(max_messages, free permits)` and is skipped
  while no permit is free, so no received message waits unprocessed while its visibility runs out.
- **Heartbeat.** It starts as soon as a message is received, before `process`, and extends the
  visibility to `visibility_seconds` every `visibility_seconds / 2` through the P3-05
  `heartbeat` helper (A46). It is cancelled **and awaited** before the final `delete` or
  `change_visibility`, so a late heartbeat never overwrites the backoff.
- **Deadline.** `process` runs under `asyncio.wait_for(processing_deadline_seconds)`. A timeout,
  or a processor that raises, becomes `unexpected` with `processor.retry.backoff(n)` (A46).
- **Final actions.** A failed delete or visibility change arrives as
  `IngressQueueError(retryable=False)` (A45). It is logged by operation and category, counted in
  `final_action_failures`, and never raised: redelivery is idempotent.
- **Metrics.** `ingress_queue_age_seconds` from `SentTimestamp` at receive, and one
  `ingress_messages_total{outcome, category}` per message after its final action.
- **Shutdown.** Once `stop` is set, no new receive starts. A long poll already in flight is
  allowed to finish (at most `wait_seconds` plus a short grace) and whatever it returns is
  released at once with `change_visibility(0)`, so a stop never burns a receive count. In-flight
  messages get `drain_seconds` to finish; any left are cancelled without a final action and
  reappear when their visibility lapses.
- **Metrics never decide.** A failing metrics call is logged by metric name and never changes a
  final action, and no exception from a message task goes unobserved.
- **Liveness (ADR-020).** `liveness()` is called at the top of every turn and after every receive.
  While every permit is busy the loop wakes every `liveness_tick_seconds` to mark, so a busy loop
  never reads as a stalled one, unless its oldest message has held a permit longer than
  `overdue_after_seconds` (the composition passes the worst legitimate message lifetime): every
  permit wedged is no progress, so the marks stop. A failing mark is logged and never decides.

Log lines carry closed categories only, never `exc_info` or an exception message: a queue
error's `__cause__` can carry a receipt handle.
"""

import asyncio
import contextlib
import logging
import math
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from limnopulse_api.adapters.dynamodb_v2 import utc_now
from limnopulse_api.domain.ingestion import (
    INGRESS_FAILURE_TABLE,
    MAX_VISIBILITY_SECONDS,
    IngressFailureCategory,
    IngressQueueMessage,
)
from limnopulse_api.repositories.ingestion import IngressQueueConsumer, TelemetryMetrics
from limnopulse_api.services.ingest_processor import ProcessAction, ProcessDecision, RetryPolicy

logger = logging.getLogger("limnopulse.telemetry.ingest")

_RECEIVE_ERROR_PAUSE_SECONDS = 0.5
# How long past `wait_seconds` a stop waits for an in-flight long poll before abandoning it.
_POLL_GRACE_SECONDS = 5.0
# How often the loop wakes to mark liveness while every permit is busy.
LIVENESS_TICK_SECONDS = 5.0


def _no_liveness() -> None:
    return None


class HeartbeatingConsumer(IngressQueueConsumer, Protocol):
    """The consumer port plus the P3-05 `heartbeat` helper (A46)."""

    async def heartbeat(self, message: IngressQueueMessage, seconds: int) -> None: ...


class MessageProcessor(Protocol):
    retry: RetryPolicy

    async def process(self, message: IngressQueueMessage) -> ProcessDecision: ...


@dataclass(frozen=True)
class IngestWorkerConfig:
    max_messages: int = 10
    wait_seconds: int = 20
    visibility_seconds: int = 60
    concurrency: int = 4
    drain_seconds: float = 30
    processing_deadline_seconds: float = 45

    def __post_init__(self) -> None:
        # The SQS limits, so a bad value fails at composition instead of on every receive.
        if not 1 <= self.max_messages <= 10:
            raise ValueError("max_messages must be within [1, 10]")
        if not 0 <= self.wait_seconds <= 20:
            raise ValueError("wait_seconds must be within [0, 20]")
        if not 1 <= self.visibility_seconds <= MAX_VISIBILITY_SECONDS:
            raise ValueError("visibility_seconds must be within [1, 43200]")
        if self.concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        if self.drain_seconds <= 0 or self.processing_deadline_seconds <= 0:
            raise ValueError("drain_seconds and processing_deadline_seconds must be positive")


class IngestWorkerLoop:
    def __init__(
        self,
        *,
        consumer: HeartbeatingConsumer,
        processor: MessageProcessor,
        config: IngestWorkerConfig,
        metrics: TelemetryMetrics,
        clock: Callable[[], datetime] = utc_now,
        liveness: Callable[[], None] = _no_liveness,
        liveness_tick_seconds: float = LIVENESS_TICK_SECONDS,
        overdue_after_seconds: float = math.inf,
    ) -> None:
        self.consumer = consumer
        self.processor = processor
        self.config = config
        self.metrics = metrics
        self.clock = clock
        self.liveness = liveness
        self.liveness_tick_seconds = liveness_tick_seconds
        self.overdue_after_seconds = overdue_after_seconds
        self.final_action_failures = 0

    async def run(self, stop: asyncio.Event) -> None:
        # Each in-flight task with the monotonic time it started.
        in_flight: dict[asyncio.Task[None], float] = {}
        stopped = asyncio.ensure_future(stop.wait())
        try:
            while not stop.is_set():
                free = self.config.concurrency - len(in_flight)
                if free <= 0:
                    if not self._overdue(in_flight):
                        self._mark_alive()
                    await asyncio.wait(
                        {*in_flight, stopped},
                        timeout=self.liveness_tick_seconds,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    continue
                self._mark_alive()
                messages = await self._receive(min(self.config.max_messages, free), stopped)
                self._mark_alive()
                if stop.is_set():
                    await self._release(messages)
                    break
                for message in messages:
                    self._start(message, in_flight)
        finally:
            stopped.cancel()
            await self._drain(in_flight)

    async def _receive(
        self, max_messages: int, stopped: asyncio.Future[bool]
    ) -> list[IngressQueueMessage]:
        """One long poll. A stop lets an in-flight poll finish (bounded) so its messages can be
        released; a cancelled `run` cancels the poll and waits for it, leaving no orphan task."""
        receive = asyncio.ensure_future(
            self.consumer.receive(
                max_messages=max_messages,
                wait_seconds=self.config.wait_seconds,
                visibility_seconds=self.config.visibility_seconds,
            )
        )
        try:
            await asyncio.wait({receive, stopped}, return_when=asyncio.FIRST_COMPLETED)
            if not receive.done():
                await asyncio.wait(
                    {receive}, timeout=self.config.wait_seconds + _POLL_GRACE_SECONDS
                )
        finally:
            if not receive.done():
                receive.cancel()
                await asyncio.wait({receive})
        if receive.cancelled():
            return []
        try:
            return receive.result()
        except Exception as error:  # noqa: BLE001 - a failed poll must not stop the worker.
            logger.warning("ingress receive failed (%s)", type(error).__name__)
            if not stopped.done():
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(asyncio.shield(stopped), _RECEIVE_ERROR_PAUSE_SECONDS)
            return []

    async def _release(self, messages: list[IngressQueueMessage]) -> None:
        """Hand messages received during a stop straight back to the queue."""
        for message in messages:
            try:
                await self.consumer.change_visibility(message, 0)
            except Exception as error:  # noqa: BLE001 - the message reappears anyway.
                self.final_action_failures += 1
                logger.warning("ingress release on stop failed (%s)", type(error).__name__)

    def _mark_alive(self) -> None:
        try:
            self.liveness()
        except Exception as error:  # noqa: BLE001 - liveness never decides.
            logger.warning("ingress liveness mark failed (%s)", type(error).__name__)

    def _overdue(self, in_flight: dict[asyncio.Task[None], float]) -> bool:
        oldest = min(in_flight.values(), default=time.monotonic())
        return time.monotonic() - oldest > self.overdue_after_seconds

    def _start(
        self, message: IngressQueueMessage, in_flight: dict[asyncio.Task[None], float]
    ) -> None:
        task = asyncio.create_task(self._handle(message))
        in_flight[task] = time.monotonic()

        def done(finished: asyncio.Task[None]) -> None:
            in_flight.pop(finished, None)
            if not finished.cancelled() and (error := finished.exception()) is not None:
                # Observed here, so asyncio never logs the traceback or the message.
                logger.error("ingress message handler failed (%s)", type(error).__name__)

        task.add_done_callback(done)

    async def _drain(self, in_flight: dict[asyncio.Task[None], float]) -> None:
        if not in_flight:
            return
        _, pending = await asyncio.wait(set(in_flight), timeout=self.config.drain_seconds)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    async def _handle(self, message: IngressQueueMessage) -> None:
        # Started first: the heartbeat covers the message from the moment it was received.
        heartbeat = asyncio.create_task(self._heartbeat(message))
        try:
            self._observe_queue_age(message)
            decision = await self._decide(message)
        finally:
            await _stop_heartbeat(heartbeat)
        await self._finish(message, decision)

    def _observe_queue_age(self, message: IngressQueueMessage) -> None:
        try:
            age = (self.clock() - message.sent_at).total_seconds()
            self.metrics.observe("ingress_queue_age_seconds", max(age, 0.0))
        except Exception as error:  # noqa: BLE001 - metrics never decide.
            _metric_failed("ingress_queue_age_seconds", error)

    async def _decide(self, message: IngressQueueMessage) -> ProcessDecision:
        try:
            return await asyncio.wait_for(
                self.processor.process(message), self.config.processing_deadline_seconds
            )
        except Exception as error:  # noqa: BLE001 - the deadline (TimeoutError) or a processor bug.
            category = IngressFailureCategory.UNEXPECTED
            logger.warning(
                "ingress message not processed: category=%s (%s)",
                category.value,
                type(error).__name__,
            )
            return ProcessDecision(
                ProcessAction.VISIBILITY,
                self.processor.retry.backoff(message.receive_count),
                category,
            )

    async def _heartbeat(self, message: IngressQueueMessage) -> None:
        seconds = self.config.visibility_seconds
        while True:
            await asyncio.sleep(seconds / 2)
            try:
                await self.consumer.heartbeat(message, seconds)
            except Exception as error:  # noqa: BLE001 - the next beat retries.
                logger.warning("ingress heartbeat failed (%s)", type(error).__name__)

    async def _finish(self, message: IngressQueueMessage, decision: ProcessDecision) -> None:
        action = ProcessAction(decision.action)
        try:
            if action is ProcessAction.DELETE:
                await self.consumer.delete(message)
            else:
                seconds = decision.seconds
                if seconds is None:  # never an immediate redelivery loop
                    seconds = self.processor.retry.backoff(message.receive_count)
                await self.consumer.change_visibility(message, seconds)
        except Exception as error:  # noqa: BLE001 - A45: counted, never raised.
            self.final_action_failures += 1
            logger.warning(
                "ingress final action failed: action=%s category=%s (%s)",
                action.value,
                decision.category.value,
                type(error).__name__,
            )
        try:
            outcome = INGRESS_FAILURE_TABLE[decision.category][0]
            self.metrics.increment(
                "ingress_messages_total", outcome=outcome.value, category=decision.category.value
            )
        except Exception as error:  # noqa: BLE001 - metrics never decide.
            _metric_failed("ingress_messages_total", error)


async def _stop_heartbeat(heartbeat: asyncio.Task[None]) -> None:
    """Cancel the heartbeat and wait until it has really finished.

    A cancellation of the caller (the drain) while waiting is held back until the heartbeat is
    done and then re-raised, so a heartbeat call still in flight can never land after the final
    action, and a cancelled message gets no final action at all.
    """
    heartbeat.cancel()
    try:
        # `asyncio.wait` never raises for the heartbeat's own cancellation, only for ours.
        await asyncio.wait({heartbeat})
    except asyncio.CancelledError:
        await _until_done(heartbeat)
        raise


async def _until_done(task: asyncio.Task[None]) -> None:
    """Wait for `task` to finish, even through further cancellations of the caller."""
    while not task.done():
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait({task})


def _metric_failed(name: str, error: Exception) -> None:
    logger.warning("ingress metric failed: metric=%s (%s)", name, type(error).__name__)
