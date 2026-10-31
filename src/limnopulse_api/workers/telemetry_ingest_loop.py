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
- **Shutdown.** Once `stop` is set, no new receive starts (a pending long poll is abandoned and
  its messages simply reappear). In-flight messages get `drain_seconds` to finish; any left are
  cancelled and reappear when their visibility lapses.

Log lines carry closed categories only, never `exc_info` or an exception message: a queue
error's `__cause__` can carry a receipt handle.
"""

import asyncio
import contextlib
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from limnopulse_api.adapters.dynamodb_v2 import utc_now
from limnopulse_api.domain.ingestion import (
    INGRESS_FAILURE_TABLE,
    IngressFailureCategory,
    IngressQueueMessage,
)
from limnopulse_api.repositories.ingestion import IngressQueueConsumer, TelemetryMetrics
from limnopulse_api.services.ingest_processor import ProcessAction, ProcessDecision, RetryPolicy

logger = logging.getLogger("limnopulse.telemetry.ingest")

_RECEIVE_ERROR_PAUSE_SECONDS = 0.5


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


class IngestWorkerLoop:
    def __init__(
        self,
        *,
        consumer: HeartbeatingConsumer,
        processor: MessageProcessor,
        config: IngestWorkerConfig,
        metrics: TelemetryMetrics,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.consumer = consumer
        self.processor = processor
        self.config = config
        self.metrics = metrics
        self.clock = clock
        self.final_action_failures = 0

    async def run(self, stop: asyncio.Event) -> None:
        in_flight: set[asyncio.Task[None]] = set()
        stopped = asyncio.ensure_future(stop.wait())
        try:
            while not stop.is_set():
                free = self.config.concurrency - len(in_flight)
                if free <= 0:
                    await asyncio.wait({*in_flight, stopped}, return_when=asyncio.FIRST_COMPLETED)
                    continue
                messages = await self._receive(min(self.config.max_messages, free), stopped)
                if messages is None:
                    break
                for message in messages:
                    task = asyncio.create_task(self._handle(message))
                    in_flight.add(task)
                    task.add_done_callback(in_flight.discard)
        finally:
            stopped.cancel()
            await self._drain(in_flight)

    async def _receive(
        self, max_messages: int, stopped: asyncio.Future[bool]
    ) -> list[IngressQueueMessage] | None:
        """One long poll; `None` when `stop` was set first (the poll is abandoned)."""
        receive = asyncio.ensure_future(
            self.consumer.receive(
                max_messages=max_messages,
                wait_seconds=self.config.wait_seconds,
                visibility_seconds=self.config.visibility_seconds,
            )
        )
        await asyncio.wait({receive, stopped}, return_when=asyncio.FIRST_COMPLETED)
        if not receive.done():
            receive.cancel()
            with contextlib.suppress(BaseException):
                await receive
            return None
        try:
            return receive.result()
        except Exception as error:  # noqa: BLE001 - a failed poll must not stop the worker.
            logger.warning("ingress receive failed (%s)", type(error).__name__)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(asyncio.shield(stopped), _RECEIVE_ERROR_PAUSE_SECONDS)
            return []

    async def _drain(self, in_flight: set[asyncio.Task[None]]) -> None:
        if not in_flight:
            return
        _, pending = await asyncio.wait(set(in_flight), timeout=self.config.drain_seconds)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    async def _handle(self, message: IngressQueueMessage) -> None:
        heartbeat = asyncio.create_task(self._heartbeat(message))
        age = (self.clock() - message.sent_at).total_seconds()
        self.metrics.observe("ingress_queue_age_seconds", max(age, 0.0))
        try:
            decision = await self._decide(message)
        finally:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat
        await self._finish(message, decision)

    async def _decide(self, message: IngressQueueMessage) -> ProcessDecision:
        try:
            return await asyncio.wait_for(
                self.processor.process(message), self.config.processing_deadline_seconds
            )
        except Exception:  # noqa: BLE001 - the deadline (TimeoutError) or a processor bug.
            return ProcessDecision(
                ProcessAction.VISIBILITY,
                self.processor.retry.backoff(message.receive_count),
                IngressFailureCategory.UNEXPECTED,
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
                await self.consumer.change_visibility(message, int(decision.seconds or 0))
        except Exception as error:  # noqa: BLE001 - A45: counted, never raised.
            self.final_action_failures += 1
            logger.warning(
                "ingress final action failed: action=%s category=%s (%s)",
                action.value,
                decision.category.value,
                type(error).__name__,
            )
        outcome = INGRESS_FAILURE_TABLE[decision.category][0]
        self.metrics.increment(
            "ingress_messages_total", outcome=outcome.value, category=decision.category.value
        )
