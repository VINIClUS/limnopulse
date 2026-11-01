"""P3-08: `IngestWorkerLoop` — receive sizing, concurrency, heartbeat, deadline, drain, metrics.

The consumer is scripted: every `receive` suspends briefly, like a long poll, so the loop never
spins, and every call is recorded in one shared event log. Each test bounds the loop with
`asyncio.wait_for` and stops it through the `stop` event.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import logging
import signal
from collections import deque
from collections.abc import Callable, Iterable, Iterator
from datetime import datetime, timedelta
from typing import Any

import pytest

from limnopulse_api.core.errors import IngressQueueError
from limnopulse_api.domain.ingestion import (
    INGRESS_FAILURE_TABLE,
    IngressFailureCategory,
    IngressQueueMessage,
)
from limnopulse_api.services.ingest_processor import ProcessDecision, RetryPolicy
from limnopulse_api.workers.telemetry_ingest_loop import IngestWorkerConfig, IngestWorkerLoop
from tests.support.ingestion_contract_kits import RECEIVED, TENANT
from tests.support.ingestion_fakes import RecordingTelemetryMetrics

pytestmark = pytest.mark.asyncio

NOW = RECEIVED + timedelta(minutes=5)
GUARD_SECONDS = 10
RECEIPT_SECRET = "rh-SECRET-handle"


@pytest.fixture(autouse=True)
def _hard_deadline() -> Iterator[None]:
    """A SIGALRM bound that fires even when a busy loop never yields to the event loop."""

    def expire(signum: int, frame: Any) -> None:
        raise TimeoutError("test exceeded its hard deadline (busy loop?)")

    previous = signal.signal(signal.SIGALRM, expire)
    signal.setitimer(signal.ITIMER_REAL, 3 * GUARD_SECONDS)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def make_message(index: int, *, receive_count: int = 1, sent_at: datetime = RECEIVED) -> Any:
    return IngressQueueMessage(
        message_id=f"m{index}",
        receipt_handle=f"{RECEIPT_SECRET}-{index}",
        body=f'{{"index":{index}}}',
        receive_count=receive_count,
        sent_at=sent_at,
    )


class ScriptedConsumer:
    """Hands out queued messages; records receives, heartbeats and final actions in order.

    A heartbeat is shielded like the P3-05 adapter: once started it completes even when its
    caller is cancelled, so an un-awaited cancellation shows up as a late `heartbeat_end`.
    """

    def __init__(
        self,
        messages: Iterable[Any] = (),
        *,
        events: list[tuple[Any, ...]] | None = None,
        heartbeat_seconds: float = 0.0,
    ) -> None:
        self.pending: deque[Any] = deque(messages)
        self.events: list[tuple[Any, ...]] = [] if events is None else events
        self.receives: list[dict[str, int]] = []
        self.heartbeat_seconds = heartbeat_seconds
        self.receive_errors: deque[BaseException] = deque()
        self.delete_errors: deque[BaseException] = deque()
        self.visibility_errors: deque[BaseException] = deque()
        self.heartbeat_errors: deque[BaseException] = deque()
        self.heartbeat_gate: asyncio.Event | None = None  # holds an in-flight heartbeat

    async def receive(
        self, *, max_messages: int, wait_seconds: int, visibility_seconds: int
    ) -> list[Any]:
        self.receives.append(
            {
                "max_messages": max_messages,
                "wait_seconds": wait_seconds,
                "visibility_seconds": visibility_seconds,
            }
        )
        self.events.append(("receive", max_messages))
        await asyncio.sleep(0.005)  # a long poll always suspends
        if self.receive_errors:
            raise self.receive_errors.popleft()
        if not 1 <= max_messages <= 10:
            raise IngressQueueError("max_messages out of range", retryable=False)
        batch = []
        while self.pending and len(batch) < max_messages:
            batch.append(self.pending.popleft())
        return batch

    async def delete(self, message: Any) -> None:
        self.events.append(("delete", message.message_id))
        if self.delete_errors:
            raise self.delete_errors.popleft()

    async def change_visibility(self, message: Any, seconds: int) -> None:
        self.events.append(("visibility", message.message_id, seconds))
        if self.visibility_errors:
            raise self.visibility_errors.popleft()

    async def heartbeat(self, message: Any, seconds: int) -> None:
        self.events.append(("heartbeat_start", message.message_id, seconds))
        if self.heartbeat_errors:
            self.events.append(("heartbeat_end", message.message_id))
            raise self.heartbeat_errors.popleft()
        gate = self.heartbeat_gate
        call = asyncio.ensure_future(
            gate.wait() if gate is not None else asyncio.sleep(self.heartbeat_seconds)
        )
        try:
            await asyncio.shield(call)
        except asyncio.CancelledError:
            await call
            self.events.append(("heartbeat_end", message.message_id))
            raise
        self.events.append(("heartbeat_end", message.message_id))

    def named(self, kind: str) -> list[tuple[Any, ...]]:
        return [event for event in self.events if event[0] == kind]


class StubProcessor:
    """Answers each message with `decide(message)`; `gates` hold a message until released."""

    def __init__(
        self,
        events: list[tuple[Any, ...]],
        *,
        decide: Callable[[Any], ProcessDecision] | None = None,
        delay: float = 0.0,
        delays: dict[str, float] | None = None,
        hang: bool = False,
        raises: BaseException | None = None,
    ) -> None:
        self.events = events
        self.decide = decide or (
            lambda message: ProcessDecision(
                action="delete", seconds=None, category=IngressFailureCategory.PROCESSED
            )
        )
        self.delay = delay
        self.delays = delays or {}
        self.hang = hang
        self.raises = raises
        self.gates: dict[str, asyncio.Event] = {}
        self.gated = False
        self.active = 0
        self.max_active = 0
        self.cancelled: list[str] = []
        self.finished: list[str] = []
        self.retry = RetryPolicy(base=30, cap=900, permanent_visibility=60, random=lambda: 1.0)

    def gate(self) -> None:
        self.gated = True

    def release(self, message_id: str) -> None:
        self.gates.setdefault(message_id, asyncio.Event()).set()

    async def process(self, message: Any) -> ProcessDecision:
        self.events.append(("process_start", message.message_id))
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if self.hang:
                await asyncio.Event().wait()
            if self.gated:
                await self.gates.setdefault(message.message_id, asyncio.Event()).wait()
            delay = self.delays.get(message.message_id, self.delay)
            if delay:
                await asyncio.sleep(delay)
            if self.raises is not None:
                raise self.raises
        except asyncio.CancelledError:
            self.cancelled.append(message.message_id)
            raise
        finally:
            self.active -= 1
        self.finished.append(message.message_id)
        self.events.append(("process_end", message.message_id))
        return self.decide(message)


def config(**overrides: Any) -> IngestWorkerConfig:
    values: dict[str, Any] = {
        "max_messages": 10,
        "wait_seconds": 20,
        "visibility_seconds": 60,
        "concurrency": 4,
        "drain_seconds": 5,
        "processing_deadline_seconds": 30,
    }
    values.update(overrides)
    return IngestWorkerConfig(**values)


def make_loop(
    consumer: ScriptedConsumer,
    processor: Any,
    *,
    metrics: RecordingTelemetryMetrics | None = None,
    clock: Callable[[], datetime] = lambda: NOW,
    **overrides: Any,
) -> tuple[IngestWorkerLoop, RecordingTelemetryMetrics]:
    recorder = metrics or RecordingTelemetryMetrics()
    loop = IngestWorkerLoop(
        consumer=consumer,
        processor=processor,
        config=config(**overrides),
        metrics=recorder,
        clock=clock,
    )
    return loop, recorder


_LOOP_TASK: contextvars.ContextVar[asyncio.Task[None] | None] = contextvars.ContextVar(
    "loop_task", default=None
)


async def wait_for(predicate: Callable[[], bool], *, timeout: float = GUARD_SECONDS) -> None:
    """Poll `predicate`; fail at once if the running loop task already ended."""
    deadline = asyncio.get_running_loop().time() + timeout
    task = _LOOP_TASK.get()
    while not predicate():
        if task is not None and task.done():
            task.result()  # re-raise the loop's own failure
            raise AssertionError("the worker loop returned before the condition held")
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.005)


@contextlib.asynccontextmanager
async def running(loop: IngestWorkerLoop) -> Any:
    stop = asyncio.Event()
    task = asyncio.create_task(loop.run(stop))
    token = _LOOP_TASK.set(task)
    try:
        yield stop, task
    finally:
        _LOOP_TASK.reset(token)
        stop.set()
        await asyncio.wait_for(task, GUARD_SECONDS)


async def run_until(loop: IngestWorkerLoop, predicate: Callable[[], bool]) -> None:
    async with running(loop):
        await wait_for(predicate)


def messages_total(metrics: RecordingTelemetryMetrics) -> list[dict[str, str]]:
    return [r["labels"] for r in metrics.records if r["metric"] == "ingress_messages_total"]


# Final actions and the message outcome metric -----------------------------------------------


DECISIONS = [
    (IngressFailureCategory.PROCESSED, "delete", None),
    (IngressFailureCategory.FENCED, "delete", None),
    (IngressFailureCategory.ENVELOPE_INVALID, "visibility", 60),
    (IngressFailureCategory.FENCED_UNVERIFIED, "visibility", 60),
    (IngressFailureCategory.DEFERRED, "visibility", 120),
    (IngressFailureCategory.STORE_UNAVAILABLE, "visibility", 240),
    (IngressFailureCategory.WRITE_RETRYABLE, "visibility", 31),
    (IngressFailureCategory.WRITE_PERMANENT, "visibility", 60),
    (IngressFailureCategory.CONTRACT_VIOLATION, "visibility", 60),
    (IngressFailureCategory.UNEXPECTED, "visibility", 900),
]


@pytest.mark.parametrize(("category", "action", "seconds"), DECISIONS, ids=lambda v: str(v))
async def test_the_decision_is_applied_and_counted_once(
    category: IngressFailureCategory, action: str, seconds: int | None
) -> None:
    consumer = ScriptedConsumer([make_message(1)])
    processor = StubProcessor(
        consumer.events,
        decide=lambda m: ProcessDecision(action=action, seconds=seconds, category=category),
    )
    loop, metrics = make_loop(consumer, processor)

    await run_until(loop, lambda: bool(messages_total(metrics)))

    expected = ("delete", "m1") if action == "delete" else ("visibility", "m1", seconds)
    finals = consumer.named("delete") + consumer.named("visibility")
    assert finals == [expected]
    outcome = INGRESS_FAILURE_TABLE[category][0].value
    assert messages_total(metrics) == [{"outcome": outcome, "category": category.value}]


async def test_receive_uses_the_configured_wait_and_visibility() -> None:
    consumer = ScriptedConsumer([make_message(1)])
    loop, _ = make_loop(
        consumer, StubProcessor(consumer.events), wait_seconds=7, visibility_seconds=45
    )

    await run_until(loop, lambda: bool(consumer.named("delete")))

    assert consumer.receives[0] == {
        "max_messages": 4,
        "wait_seconds": 7,
        "visibility_seconds": 45,
    }


# Receive sizing and concurrency ----------------------------------------------------------------


async def test_each_receive_asks_for_min_of_max_messages_and_free_permits() -> None:
    consumer = ScriptedConsumer([make_message(i) for i in range(1, 4)])
    processor = StubProcessor(consumer.events)
    processor.gate()
    loop, _ = make_loop(consumer, processor, max_messages=2, concurrency=3)

    async with running(loop):
        await wait_for(lambda: processor.active == 3)
        await asyncio.sleep(0.1)  # no permit is free: no receive may happen meanwhile
        assert [r["max_messages"] for r in consumer.receives] == [2, 1]
        processor.release("m1")
        await wait_for(lambda: len(consumer.receives) == 3)
        assert consumer.receives[2]["max_messages"] == 1
        for message_id in ("m2", "m3"):
            processor.release(message_id)
        await wait_for(lambda: len(consumer.named("delete")) == 3)

    assert all(1 <= r["max_messages"] <= 2 for r in consumer.receives)


async def test_empty_receives_release_their_permits() -> None:
    consumer = ScriptedConsumer()
    loop, _ = make_loop(consumer, StubProcessor(consumer.events), concurrency=3, max_messages=10)

    async with running(loop):
        await wait_for(lambda: len(consumer.receives) >= 3)  # several empty long polls
        consumer.pending.extend(make_message(i) for i in range(1, 4))
        await wait_for(lambda: len(consumer.named("delete")) == 3)
        await wait_for(lambda: consumer.receives[-1]["max_messages"] == 3)

    assert all(r["max_messages"] == 3 for r in consumer.receives[:3])


async def test_no_receive_asks_for_zero_messages() -> None:
    consumer = ScriptedConsumer([make_message(i) for i in range(1, 6)])
    processor = StubProcessor(consumer.events, delay=0.02)
    loop, _ = make_loop(consumer, processor, max_messages=10, concurrency=1)

    await run_until(loop, lambda: len(consumer.named("delete")) == 5)

    assert consumer.receives
    assert all(r["max_messages"] == 1 for r in consumer.receives)


async def test_the_concurrency_bound_is_respected_and_reached() -> None:
    consumer = ScriptedConsumer([make_message(i) for i in range(1, 13)])
    processor = StubProcessor(consumer.events, delay=0.03)
    loop, _ = make_loop(consumer, processor, max_messages=10, concurrency=3)

    await run_until(loop, lambda: len(consumer.named("delete")) == 12)

    assert processor.max_active == 3
    assert sorted(processor.finished) == sorted(f"m{i}" for i in range(1, 13))


# Heartbeat --------------------------------------------------------------------------------------


async def test_the_heartbeat_extends_visibility_on_a_slow_message() -> None:
    consumer = ScriptedConsumer([make_message(1)])
    processor = StubProcessor(consumer.events, delay=2.6)
    loop, _ = make_loop(consumer, processor, visibility_seconds=2)

    await run_until(loop, lambda: bool(consumer.named("delete")))

    beats = consumer.named("heartbeat_start")
    assert len(beats) >= 2  # every visibility_seconds / 2
    # Each beat extends the visibility by the full visibility_seconds.
    assert all(beat == ("heartbeat_start", "m1", 2) for beat in beats)


async def test_a_fast_message_gets_no_heartbeat() -> None:
    consumer = ScriptedConsumer([make_message(1)])
    loop, _ = make_loop(consumer, StubProcessor(consumer.events), visibility_seconds=2)

    await run_until(loop, lambda: bool(consumer.named("delete")))
    await asyncio.sleep(1.2)

    assert consumer.named("heartbeat_start") == []


HEARTBEAT_SCENARIOS = ["delete", "visibility", "processor-raises", "deadline"]


@pytest.mark.parametrize("scenario", HEARTBEAT_SCENARIOS)
async def test_an_in_flight_heartbeat_is_awaited_before_the_final_action(scenario: str) -> None:
    """Deterministic ordering: the heartbeat is held in flight on a gate while processing ends
    (or the deadline fires); the final action must wait for it, and nothing may follow."""
    consumer = ScriptedConsumer([make_message(1, receive_count=2)])
    consumer.heartbeat_gate = asyncio.Event()
    decide = None
    if scenario == "visibility":
        decide = lambda m: ProcessDecision(
            action="visibility", seconds=77, category=IngressFailureCategory.DEFERRED
        )
    processor = StubProcessor(
        consumer.events,
        decide=decide,
        hang=scenario == "deadline",
        raises=RuntimeError("bug") if scenario == "processor-raises" else None,
    )
    processor.gate()
    overrides: dict[str, Any] = {"visibility_seconds": 1}
    if scenario == "deadline":
        overrides["processing_deadline_seconds"] = 0.7
    loop, metrics = make_loop(consumer, processor, **overrides)
    final_kind = "delete" if scenario == "delete" else "visibility"

    async with running(loop):
        await wait_for(lambda: bool(consumer.named("heartbeat_start")))
        assert processor.active == 1  # the heartbeat runs while the message is processed
        if scenario == "deadline":
            await wait_for(lambda: processor.cancelled == ["m1"])
        else:
            processor.release("m1")
            await wait_for(lambda: processor.active == 0)
        await asyncio.sleep(0.05)
        assert consumer.named(final_kind) == []  # held until the heartbeat call returns
        consumer.heartbeat_gate.set()
        await wait_for(lambda: bool(consumer.named(final_kind)))
        await asyncio.sleep(0.8)  # a heartbeat that kept running would show up now

    kinds = [event[0] for event in consumer.events if event[0] != "receive"]
    final = kinds.index(final_kind)
    assert kinds[final - 1] == "heartbeat_end"
    assert kinds[final + 1 :] == []
    if scenario == "visibility":
        assert consumer.named("visibility") == [("visibility", "m1", 77)]
    if scenario in {"processor-raises", "deadline"}:
        assert consumer.named("visibility") == [("visibility", "m1", processor.retry.backoff(2))]
        assert messages_total(metrics) == [{"outcome": "retry", "category": "unexpected"}]


async def test_each_message_has_its_own_heartbeat() -> None:
    consumer = ScriptedConsumer([make_message(1), make_message(2)])
    processor = StubProcessor(consumer.events, delays={"m1": 0.05, "m2": 1.8})
    loop, _ = make_loop(consumer, processor, visibility_seconds=1, max_messages=2)

    await run_until(loop, lambda: len(consumer.named("delete")) == 2)

    assert consumer.receives[0]["max_messages"] == 2  # both arrived in one batch
    m1_deleted = consumer.events.index(("delete", "m1"))
    assert not [e for e in consumer.events[m1_deleted:] if e[:2] == ("heartbeat_start", "m1")]
    assert len([e for e in consumer.events if e[:2] == ("heartbeat_start", "m2")]) >= 2


async def test_a_heartbeat_failure_neither_stops_the_message_nor_the_loop(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    consumer = ScriptedConsumer([make_message(1), make_message(2)])
    consumer.heartbeat_errors.append(_queue_error("change_visibility", 1))
    processor = StubProcessor(consumer.events, delays={"m1": 1.8, "m2": 0.01})
    loop, _ = make_loop(consumer, processor, visibility_seconds=1, concurrency=1)

    await run_until(loop, lambda: len(consumer.named("delete")) == 2)

    assert processor.finished == ["m1", "m2"]
    m1_beats = [e for e in consumer.events if e[:2] == ("heartbeat_start", "m1")]
    assert len(m1_beats) >= 2  # the heartbeat keeps beating after a failed call
    _assert_redacted_logs(caplog)


# Processing deadline ---------------------------------------------------------------------------


async def test_the_processing_deadline_maps_to_unexpected_with_backoff() -> None:
    consumer = ScriptedConsumer([make_message(1, receive_count=3)])
    processor = StubProcessor(consumer.events, hang=True)
    loop, metrics = make_loop(consumer, processor, processing_deadline_seconds=0.05)

    await run_until(loop, lambda: bool(consumer.named("visibility")))

    assert consumer.named("visibility") == [("visibility", "m1", processor.retry.backoff(3))]
    assert processor.cancelled == ["m1"]
    assert consumer.named("delete") == []
    assert messages_total(metrics) == [{"outcome": "retry", "category": "unexpected"}]


async def test_a_hung_message_does_not_block_the_others() -> None:
    consumer = ScriptedConsumer([make_message(1), make_message(2)])
    processor = StubProcessor(consumer.events)
    hung = {"m1"}
    original = processor.process

    async def process(message: Any) -> ProcessDecision:
        if message.message_id in hung:
            await asyncio.sleep(GUARD_SECONDS * 2)
        return await original(message)

    processor.process = process  # type: ignore[method-assign]
    loop, metrics = make_loop(consumer, processor, processing_deadline_seconds=0.1)

    await run_until(loop, lambda: len(messages_total(metrics)) == 2)

    assert consumer.named("delete") == [("delete", "m2")]
    assert [event[1] for event in consumer.named("visibility")] == ["m1"]


# Final-action failures --------------------------------------------------------------------------


def _queue_error(operation: str, index: int) -> IngressQueueError:
    error = IngressQueueError(f"SQS {operation} failed (ReceiptHandleIsInvalid)", retryable=False)
    error.__cause__ = RuntimeError(f"handle {RECEIPT_SECRET}-{index} for {TENANT}")
    return error


async def test_a_delete_failure_is_counted_and_not_raised(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    consumer = ScriptedConsumer([make_message(1), make_message(2)])
    consumer.delete_errors.append(_queue_error("delete", 1))
    processor = StubProcessor(consumer.events, delay=0.01)
    loop, metrics = make_loop(consumer, processor, concurrency=1)

    await run_until(loop, lambda: len(consumer.named("delete")) == 2)

    assert loop.final_action_failures == 1
    assert len(messages_total(metrics)) == 2
    _assert_redacted_logs(caplog)
    _assert_logged_category(caplog, "processed")


async def test_a_visibility_failure_is_counted_and_not_raised(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    consumer = ScriptedConsumer([make_message(1), make_message(2)])
    consumer.visibility_errors.append(_queue_error("change_visibility", 1))
    processor = StubProcessor(
        consumer.events,
        decide=lambda m: ProcessDecision(
            action="visibility", seconds=60, category=IngressFailureCategory.ENVELOPE_INVALID
        ),
    )
    loop, _ = make_loop(consumer, processor, concurrency=1)

    await run_until(loop, lambda: len(consumer.named("visibility")) == 2)

    assert loop.final_action_failures == 1
    _assert_redacted_logs(caplog)
    _assert_logged_category(caplog, "envelope_invalid")


async def test_a_receive_failure_does_not_stop_the_loop(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    consumer = ScriptedConsumer([make_message(1)])
    # A45: receive failures surface as retryable queue errors.
    error = IngressQueueError("SQS receive failed (RequestThrottled)", retryable=True)
    error.__cause__ = RuntimeError(f"handle {RECEIPT_SECRET}-9 for {TENANT}")
    consumer.receive_errors.append(error)
    loop, _ = make_loop(consumer, StubProcessor(consumer.events))

    await run_until(loop, lambda: bool(consumer.named("delete")))

    assert len(consumer.receives) >= 2
    _assert_redacted_logs(caplog)


def _assert_logged_category(caplog: pytest.LogCaptureFixture, category: str) -> None:
    """The failure line names its category (a closed value), so operators can triage it."""
    assert any(category in record.getMessage() for record in caplog.records)


def _assert_redacted_logs(caplog: pytest.LogCaptureFixture) -> None:
    assert caplog.records, "the failure must be logged"
    for record in caplog.records:
        assert record.exc_info is None
        text = record.getMessage()
        assert RECEIPT_SECRET not in text
        assert TENANT not in text


# Shutdown drain ---------------------------------------------------------------------------------


async def test_stop_drains_in_flight_messages_without_new_receives() -> None:
    consumer = ScriptedConsumer([make_message(1), make_message(2), make_message(3)])
    processor = StubProcessor(consumer.events)
    processor.gate()
    loop, _ = make_loop(consumer, processor, concurrency=2, max_messages=2)

    stop = asyncio.Event()
    task = asyncio.create_task(loop.run(stop))
    _LOOP_TASK.set(task)
    await wait_for(lambda: processor.active == 2)
    receives_at_stop = len(consumer.receives)
    stop.set()
    await asyncio.sleep(0.05)
    assert not task.done()  # still draining
    processor.release("m1")
    processor.release("m2")
    await asyncio.wait_for(task, GUARD_SECONDS)

    assert len(consumer.receives) == receives_at_stop
    assert sorted(consumer.named("delete")) == [("delete", "m1"), ("delete", "m2")]
    assert list(consumer.pending) == [make_message(3)]


async def test_drain_gives_up_after_drain_seconds() -> None:
    consumer = ScriptedConsumer([make_message(1)])
    processor = StubProcessor(consumer.events, hang=True)
    loop, _ = make_loop(consumer, processor, drain_seconds=0.2, visibility_seconds=1)

    stop = asyncio.Event()
    task = asyncio.create_task(loop.run(stop))
    _LOOP_TASK.set(task)
    await wait_for(lambda: processor.active == 1)
    started = asyncio.get_running_loop().time()
    stop.set()
    await asyncio.wait_for(task, GUARD_SECONDS)
    elapsed = asyncio.get_running_loop().time() - started
    await asyncio.sleep(0.8)  # no heartbeat may outlive the loop

    assert elapsed < 1.5
    assert processor.cancelled == ["m1"]  # nothing is left running after the drain
    assert consumer.named("delete") == [] and consumer.named("visibility") == []
    beats = len(consumer.named("heartbeat_start"))
    await asyncio.sleep(0.6)
    assert len(consumer.named("heartbeat_start")) == beats


async def test_an_already_set_stop_never_receives() -> None:
    consumer = ScriptedConsumer([make_message(1)])
    loop, _ = make_loop(consumer, StubProcessor(consumer.events))
    stop = asyncio.Event()
    stop.set()

    await asyncio.wait_for(loop.run(stop), GUARD_SECONDS)

    assert consumer.receives == []


# Queue age -------------------------------------------------------------------------------------


async def test_queue_age_is_observed_from_sent_timestamp() -> None:
    sent = NOW - timedelta(seconds=7, milliseconds=500)
    consumer = ScriptedConsumer([make_message(1, sent_at=sent), make_message(2, sent_at=NOW)])
    loop, metrics = make_loop(consumer, StubProcessor(consumer.events))

    await run_until(loop, lambda: len(consumer.named("delete")) == 2)

    ages = [r for r in metrics.records if r["metric"] == "ingress_queue_age_seconds"]
    assert sorted(r["value"] for r in ages) == [0.0, 7.5]
    assert all(r["labels"] == {} for r in ages)


async def test_queue_age_is_never_negative() -> None:
    consumer = ScriptedConsumer([make_message(1, sent_at=NOW + timedelta(seconds=3))])
    loop, metrics = make_loop(consumer, StubProcessor(consumer.events))

    await run_until(loop, lambda: bool(consumer.named("delete")))

    (age,) = [r for r in metrics.records if r["metric"] == "ingress_queue_age_seconds"]
    assert age["value"] == 0.0


async def test_the_loop_emits_only_closed_metric_names() -> None:
    consumer = ScriptedConsumer([make_message(i) for i in range(1, 4)])
    loop, metrics = make_loop(consumer, StubProcessor(consumer.events))

    await run_until(loop, lambda: len(consumer.named("delete")) == 3)

    assert {r["metric"] for r in metrics.records} == {
        "ingress_messages_total",
        "ingress_queue_age_seconds",
    }


# The real processor behind the loop: quarantine is persisted before the delete ---------------


async def test_rows_and_marks_are_written_before_the_delete() -> None:
    from tests.support.ingestion_contract_kits import envelope
    from tests.unit.test_ingest_processor import FOREIGN_COMPONENT, make_world, reading

    world = await make_world()
    item = envelope(observations=[reading(sequence=1), reading(component_id=FOREIGN_COMPONENT)])
    shared: list[Any] = []
    consumer = ScriptedConsumer(
        [
            IngressQueueMessage(
                message_id="m1",
                receipt_handle="rh-1",
                body=item.to_json(),
                receive_count=1,
                sent_at=RECEIVED,
            )
        ],
        events=shared,
    )
    loop, _ = make_loop(consumer, world.processor)

    original_delete = consumer.delete

    async def delete(message: Any) -> None:
        world.events.append("queue.delete")
        await original_delete(message)

    consumer.delete = delete  # type: ignore[method-assign]

    await run_until(loop, lambda: bool(consumer.named("delete")))

    sequence = [e for e in world.events if e != "state.get_component_states"]
    assert sequence == [
        "mapping.fence",
        "pipeline.ingest",
        "quarantine.put_quarantine_entries",
        "state.apply_ingest_marks",
        "queue.delete",
    ]
    assert len(world.quarantine_rows) == 1


async def test_a_quarantine_failure_behind_the_loop_never_deletes() -> None:
    from tests.support.ingestion_contract_kits import envelope, store_errors
    from tests.unit.test_ingest_processor import FOREIGN_COMPONENT, make_world, reading

    world = await make_world()
    world.fail_next("put_quarantine_entries", store_errors()[0])
    item = envelope(observations=[reading(component_id=FOREIGN_COMPONENT)])
    consumer = ScriptedConsumer(
        [
            IngressQueueMessage(
                message_id="m1",
                receipt_handle="rh-1",
                body=item.to_json(),
                receive_count=1,
                sent_at=RECEIVED,
            )
        ]
    )
    loop, metrics = make_loop(consumer, world.processor)

    await run_until(loop, lambda: bool(consumer.named("visibility")))

    assert consumer.named("delete") == []
    assert messages_total(metrics) == [{"outcome": "retry", "category": "store_unavailable"}]


# ADR-020 S5: liveness marks ------------------------------------------------------------------


class LivenessMarks:
    """Records every liveness mark into the consumer's shared event log."""

    def __init__(self, events: list[tuple[Any, ...]]) -> None:
        self.events = events
        self.count = 0

    def __call__(self) -> None:
        self.count += 1
        self.events.append(("liveness",))


def liveness_loop(
    consumer: ScriptedConsumer,
    processor: Any,
    marks: Callable[[], None],
    *,
    tick: float = 0.01,
    overdue_after: float = float("inf"),
    **overrides: Any,
) -> IngestWorkerLoop:
    return IngestWorkerLoop(
        consumer=consumer,
        processor=processor,
        config=config(**overrides),
        metrics=RecordingTelemetryMetrics(),
        clock=lambda: NOW,
        liveness=marks,
        liveness_tick_seconds=tick,
        overdue_after_seconds=overdue_after,
    )


async def test_the_loop_marks_liveness_at_the_top_of_every_turn_and_after_every_receive() -> None:
    consumer = ScriptedConsumer()
    marks = LivenessMarks(consumer.events)
    loop = liveness_loop(consumer, StubProcessor(consumer.events), marks)

    await run_until(loop, lambda: len(consumer.receives) >= 4)

    turns = [event[0] for event in consumer.events if event[0] in {"liveness", "receive"}]
    assert turns[:9] == ["liveness", "receive", "liveness"] * 3


async def test_a_loop_without_liveness_runs_as_before() -> None:
    consumer = ScriptedConsumer([make_message(1)])
    loop, _ = make_loop(consumer, StubProcessor(consumer.events))

    await run_until(loop, lambda: bool(consumer.named("delete")))

    assert consumer.named("liveness") == []


async def test_the_loop_keeps_marking_while_every_permit_is_busy() -> None:
    """The permits-full wait ticks, so a busy loop never reads as a stalled one."""
    consumer = ScriptedConsumer([make_message(1)])
    processor = StubProcessor(consumer.events)
    processor.gate()
    marks = LivenessMarks(consumer.events)
    loop = liveness_loop(consumer, processor, marks, concurrency=1)

    async with running(loop):
        await wait_for(lambda: processor.active == 1)
        receives, before = len(consumer.receives), marks.count
        await wait_for(lambda: marks.count >= before + 5)
        assert len(consumer.receives) == receives  # all of it from the busy branch
        processor.release("m1")
        await wait_for(lambda: bool(consumer.named("delete")))


async def test_a_permit_held_past_the_message_bound_stops_the_marks() -> None:
    """The oldest message past the bound while every permit is busy is no progress: the marks
    stop, so the watchdog can see it, and they resume once the permit frees up."""
    consumer = ScriptedConsumer([make_message(1)])
    processor = StubProcessor(consumer.events)
    processor.gate()
    marks = LivenessMarks(consumer.events)
    loop = liveness_loop(consumer, processor, marks, concurrency=1, overdue_after=0.1)

    async with running(loop):
        await wait_for(lambda: processor.active == 1)
        await asyncio.sleep(0.15)  # past the bound
        stopped_at = marks.count
        await asyncio.sleep(0.1)
        assert marks.count == stopped_at
        processor.release("m1")
        await wait_for(lambda: marks.count >= stopped_at + 3)


async def test_a_failing_liveness_mark_never_stops_the_loop(
    caplog: pytest.LogCaptureFixture,
) -> None:
    consumer = ScriptedConsumer([make_message(1), make_message(2)])

    def broken() -> None:
        raise RuntimeError("sentinel-liveness-detail")

    loop = liveness_loop(consumer, StubProcessor(consumer.events), broken)
    caplog.set_level(logging.WARNING)

    await run_until(loop, lambda: len(consumer.named("delete")) == 2)

    assert "ingress liveness mark failed (RuntimeError)" in caplog.text
    assert "sentinel-liveness-detail" not in caplog.text
