"""P3-08 diff-review regressions (new after the approved test sha abc85ff; disclosed in the PR).

Each test pins one reviewer finding against the worker loop or the processor:
- a failing queue-age metric must not leave a heartbeat running (R1);
- an exception escaping a message task is observed, never left to asyncio's handler (R1);
- a drain cancel while a heartbeat call is in flight issues no final action before, or at all (R2);
- a stop during a long poll waits for it and releases what it returns (R3);
- a cancelled `run` leaves no orphan receive task (R4);
- a failing metrics call never changes the processor's decision (R5);
- the config rejects values SQS would reject (R7);
- a visibility decision without seconds falls back to the backoff, and a deadline is logged (R8).
"""

from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Iterator
from typing import Any

import pytest

from limnopulse_api.domain.ingestion import IngressFailureCategory
from limnopulse_api.services.ingest_processor import ProcessDecision
from limnopulse_api.workers.telemetry_ingest_loop import IngestWorkerConfig
from tests.support.ingestion_contract_kits import envelope
from tests.support.ingestion_fakes import RecordingTelemetryMetrics
from tests.unit.test_ingest_processor import make_world, message_for, reading
from tests.unit.test_telemetry_ingest_loop import (
    GUARD_SECONDS,
    ScriptedConsumer,
    StubProcessor,
    make_loop,
    make_message,
    run_until,
    running,
    wait_for,
)

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _hard_deadline() -> Iterator[None]:
    def expire(signum: int, frame: Any) -> None:
        raise TimeoutError("test exceeded its hard deadline (busy loop?)")

    previous = signal.signal(signal.SIGALRM, expire)
    signal.setitimer(signal.ITIMER_REAL, 3 * GUARD_SECONDS)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


class RaisingMetrics(RecordingTelemetryMetrics):
    """Records like the P3-00 fake, but raises for the named metrics."""

    def __init__(self, *failing: str) -> None:
        super().__init__()
        self.failing = set(failing)

    def increment(self, name: str, value: int = 1, **labels: str) -> None:
        if name in self.failing:
            raise RuntimeError("exporter down")
        super().increment(name, value, **labels)

    def observe(self, name: str, value: float, **labels: str) -> None:
        if name in self.failing:
            raise RuntimeError("exporter down")
        super().observe(name, value, **labels)


@pytest.fixture
def handler_calls() -> list[dict[str, Any]]:
    """Contexts passed to the event loop's exception handler (each test has its own loop)."""
    return []


def _spy_exception_handler(calls: list[dict[str, Any]]) -> None:
    asyncio.get_running_loop().set_exception_handler(lambda _loop, context: calls.append(context))


def _heartbeat_tasks() -> list[asyncio.Task[Any]]:
    return [
        task
        for task in asyncio.all_tasks()
        if getattr(task.get_coro(), "__qualname__", "") == "IngestWorkerLoop._heartbeat"
        and not task.done()
    ]


# R1 -------------------------------------------------------------------------------------------


async def test_a_failing_queue_age_metric_leaves_no_heartbeat_running() -> None:
    consumer = ScriptedConsumer([make_message(1)])
    processor = StubProcessor(consumer.events, delay=0.05)
    loop, _ = make_loop(
        consumer,
        processor,
        metrics=RaisingMetrics("ingress_queue_age_seconds"),
        visibility_seconds=1,
    )

    await run_until(loop, lambda: bool(consumer.named("delete")))
    await asyncio.sleep(1.2)

    assert consumer.named("heartbeat_start") == []
    assert _heartbeat_tasks() == []


async def test_a_failing_outcome_metric_is_observed_and_never_reaches_asyncio(
    handler_calls: list[dict[str, Any]], caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    _spy_exception_handler(handler_calls)
    consumer = ScriptedConsumer([make_message(1), make_message(2)])
    loop, _ = make_loop(
        consumer,
        StubProcessor(consumer.events),
        metrics=RaisingMetrics("ingress_messages_total"),
        concurrency=1,
    )

    await run_until(loop, lambda: len(consumer.named("delete")) == 2)

    assert handler_calls == []
    assert any("ingress_messages_total" in r.getMessage() for r in caplog.records)
    assert all(r.exc_info is None for r in caplog.records)


async def test_an_exception_escaping_a_message_task_is_observed(
    handler_calls: list[dict[str, Any]],
) -> None:
    _spy_exception_handler(handler_calls)
    consumer = ScriptedConsumer([make_message(1), make_message(2)])
    processor = StubProcessor(
        consumer.events,
        decide=lambda m: ProcessDecision(action="bogus", seconds=None, category="processed"),
    )
    loop, _ = make_loop(consumer, processor, concurrency=1)

    await run_until(loop, lambda: len(processor.finished) == 2)
    await asyncio.sleep(0.05)

    assert handler_calls == []


# R2 -------------------------------------------------------------------------------------------


async def test_a_drain_cancel_during_an_in_flight_heartbeat_issues_no_final_action() -> None:
    consumer = ScriptedConsumer([make_message(1)])
    consumer.heartbeat_gate = asyncio.Event()
    processor = StubProcessor(consumer.events)
    processor.gate()
    loop, _ = make_loop(consumer, processor, visibility_seconds=1, drain_seconds=0.1)

    stop = asyncio.Event()
    task = asyncio.create_task(loop.run(stop))
    await wait_for(lambda: bool(consumer.named("heartbeat_start")))
    processor.release("m1")  # processing ends while the heartbeat call is in flight
    await wait_for(lambda: processor.active == 0)
    stop.set()
    await asyncio.sleep(0.3)  # the drain deadline has passed and cancelled the message

    assert not task.done()  # still waiting for the in-flight heartbeat call
    assert consumer.named("delete") == [] and consumer.named("visibility") == []
    consumer.heartbeat_gate.set()
    await asyncio.wait_for(task, GUARD_SECONDS)

    kinds = [event[0] for event in consumer.events if event[0] != "receive"]
    assert kinds[-1] == "heartbeat_end"
    assert "delete" not in kinds and "visibility" not in kinds


# R3 / R4 --------------------------------------------------------------------------------------


class SlowPollConsumer(ScriptedConsumer):
    """A long poll that, like boto3 in a thread, completes even when its caller is cancelled."""

    def __init__(self, messages: list[Any], *, poll_seconds: float) -> None:
        super().__init__()
        self.batch = messages
        self.poll_seconds = poll_seconds
        self.polls = 0
        self.inner: list[asyncio.Future[Any]] = []

    async def receive(
        self, *, max_messages: int, wait_seconds: int, visibility_seconds: int
    ) -> list[Any]:
        self.polls += 1
        self.events.append(("receive", max_messages))
        inner = asyncio.ensure_future(self._poll())
        self.inner.append(inner)
        return await asyncio.shield(inner)

    async def _poll(self) -> list[Any]:
        await asyncio.sleep(self.poll_seconds)
        batch, self.batch = self.batch, []
        return batch


async def test_a_stop_during_a_long_poll_releases_what_it_returns() -> None:
    consumer = SlowPollConsumer([make_message(1), make_message(2)], poll_seconds=0.3)
    processor = StubProcessor(consumer.events)
    loop, _ = make_loop(consumer, processor, wait_seconds=1)

    stop = asyncio.Event()
    task = asyncio.create_task(loop.run(stop))
    await wait_for(lambda: consumer.polls == 1)
    stop.set()
    await asyncio.wait_for(task, GUARD_SECONDS)

    assert consumer.named("visibility") == [("visibility", "m1", 0), ("visibility", "m2", 0)]
    assert processor.finished == []
    assert consumer.polls == 1


async def test_a_cancelled_run_leaves_no_orphan_receive() -> None:
    consumer = SlowPollConsumer([], poll_seconds=5)
    loop, _ = make_loop(consumer, StubProcessor(consumer.events))

    stop = asyncio.Event()
    task = asyncio.create_task(loop.run(stop))
    await wait_for(lambda: consumer.polls == 1)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, GUARD_SECONDS)
    others = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    assert [t for t in others if not t.done() and t not in consumer.inner] == []


# R5 -------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "failing",
    ["ingress_normalization_duration_seconds", "ingress_observations_total"],
)
async def test_a_failing_metric_never_changes_the_processed_decision(failing: str) -> None:
    world = await make_world()
    world.processor.metrics = RaisingMetrics(failing)

    decision = await world.processor.process(message_for(envelope(observations=[reading()])))

    assert decision.action == "delete"
    assert decision.category == IngressFailureCategory.PROCESSED


async def test_a_failing_write_failure_metric_keeps_the_permanent_decision() -> None:
    from limnopulse_api.core.errors import TelemetryWriteError

    world = await make_world()
    world.processor.metrics = RaisingMetrics("ingress_write_failures_total")
    world.pipeline.fail_with = TelemetryWriteError("bad point", retryable=False)

    decision = await world.processor.process(message_for(envelope(observations=[reading()])))

    assert decision.category == IngressFailureCategory.WRITE_PERMANENT


# R7 / R8 --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "overrides",
    [
        {"max_messages": 0},
        {"max_messages": 11},
        {"wait_seconds": -1},
        {"wait_seconds": 21},
        {"visibility_seconds": 0},
        {"visibility_seconds": 43_201},
        {"concurrency": 0},
        {"drain_seconds": 0},
        {"processing_deadline_seconds": 0},
    ],
)
async def test_the_config_rejects_values_sqs_would_reject(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        IngestWorkerConfig(**overrides)


async def test_a_visibility_decision_without_seconds_falls_back_to_the_backoff() -> None:
    consumer = ScriptedConsumer([make_message(1, receive_count=4)])
    processor = StubProcessor(
        consumer.events,
        decide=lambda m: ProcessDecision(
            action="visibility", seconds=None, category=IngressFailureCategory.DEFERRED
        ),
    )
    loop, _ = make_loop(consumer, processor)

    await run_until(loop, lambda: bool(consumer.named("visibility")))

    assert consumer.named("visibility") == [("visibility", "m1", processor.retry.backoff(4))]


async def test_a_deadline_overrun_is_logged_by_category(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    consumer = ScriptedConsumer([make_message(1)])
    loop, _ = make_loop(
        consumer, StubProcessor(consumer.events, hang=True), processing_deadline_seconds=0.05
    )

    async with running(loop):
        await wait_for(lambda: bool(consumer.named("visibility")))

    assert any("category=unexpected" in r.getMessage() for r in caplog.records)
    assert all(r.exc_info is None for r in caplog.records)
