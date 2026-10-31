"""P3-05 unit tests for `redrive_dlq` (D10) over the P3-00 in-memory queue fake.

The DLQ and the source queue are `FakeIngressQueue`s wrapped in a recorder, so the tests pin the
order of operations (send before delete; no delete after a failed send), that dry-run is the
default and only releases messages, and that exactly `max_messages` are handled.
"""

from typing import Any

import pytest

from limnopulse_api.core.errors import IngressMessageTooLarge, IngressQueueError
from limnopulse_api.domain.ingestion import IngressQueueMessage
from limnopulse_api.services.ingress_redrive import RedriveReport, redrive_dlq
from tests.support.ingestion_fakes import FakeIngressQueue, Faults

BODIES = [
    '{"schema":"telemetry-ingress@1","ingress_id":"ing_' + "0" * 32 + '"}',
    "not json, redriven as-is \U0001f600",
    '{ "spacing" : "kept" ,\n "schema":"unknown@9" }',
]


class RecordingQueue:
    """Wraps a fake queue; logs every port call into a shared event list."""

    def __init__(self, name: str, inner: FakeIngressQueue, events: list[tuple[Any, ...]]) -> None:
        self.name = name
        self.inner = inner
        self.events = events
        self.fail_bodies: dict[str, BaseException] = {}

    async def send(self, envelope: Any) -> str:
        self.events.append((self.name, "send"))
        return await self.inner.send(envelope)

    async def send_raw(self, body: str) -> str:
        self.events.append((self.name, "send_raw", body))
        if body in self.fail_bodies:
            raise self.fail_bodies[body]
        return await self.inner.send_raw(body)

    async def receive(
        self, *, max_messages: int, wait_seconds: int, visibility_seconds: int
    ) -> list[IngressQueueMessage]:
        received = await self.inner.receive(
            max_messages=max_messages,
            wait_seconds=wait_seconds,
            visibility_seconds=visibility_seconds,
        )
        self.events.append((self.name, "receive", max_messages, len(received)))
        return received

    async def delete(self, message: IngressQueueMessage) -> None:
        self.events.append((self.name, "delete", message.body))
        await self.inner.delete(message)

    async def change_visibility(self, message: IngressQueueMessage, seconds: int) -> None:
        self.events.append((self.name, "change_visibility", message.body, seconds))
        await self.inner.change_visibility(message, seconds)

    def ops(self, op: str) -> list[tuple[Any, ...]]:
        return [event for event in self.events if event[0] == self.name and event[1] == op]

    def bodies(self) -> list[str]:
        return [stored["body"] for stored in self.inner.messages.values()]


@pytest.fixture
def events() -> list[tuple[Any, ...]]:
    return []


@pytest.fixture
def dlq(events: list[tuple[Any, ...]]) -> RecordingQueue:
    return RecordingQueue("dlq", FakeIngressQueue(Faults()), events)


@pytest.fixture
def source(events: list[tuple[Any, ...]]) -> RecordingQueue:
    return RecordingQueue("source", FakeIngressQueue(Faults()), events)


async def _seed(dlq: RecordingQueue, bodies: list[str]) -> None:
    for body in bodies:
        await dlq.inner.send_raw(body)


async def _visible(queue: RecordingQueue) -> list[str]:
    seen: list[str] = []
    while batch := await queue.inner.receive(
        max_messages=10, wait_seconds=0, visibility_seconds=300
    ):
        seen.extend(message.body for message in batch)
    return seen


def test_redrive_dlq_is_keyword_only() -> None:
    queue = FakeIngressQueue(Faults())
    with pytest.raises(TypeError):
        redrive_dlq(queue, queue, 1)  # type: ignore[misc]


# --- dry-run ----------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dry_run_is_the_default_and_only_counts_and_releases(
    dlq: RecordingQueue, source: RecordingQueue
) -> None:
    await _seed(dlq, BODIES)

    report = await redrive_dlq(dlq=dlq, source=source, max_messages=10)

    assert report == RedriveReport(seen=3, redriven=0, failed=0)
    assert source.ops("send_raw") == [] and source.ops("send") == []
    assert dlq.ops("delete") == []
    # Each message is released exactly once with visibility 0 ...
    released = sorted((event[2], event[3]) for event in dlq.ops("change_visibility"))
    assert released == sorted((body, 0) for body in BODIES)
    # ... so every one is still on the DLQ and visible again right away.
    assert sorted(await _visible(dlq)) == sorted(BODIES)
    assert source.bodies() == []


@pytest.mark.asyncio
async def test_dry_run_apply_false_matches_the_default(
    dlq: RecordingQueue, source: RecordingQueue
) -> None:
    await _seed(dlq, BODIES)

    report = await redrive_dlq(dlq=dlq, source=source, max_messages=10, apply=False)

    assert report == RedriveReport(seen=3, redriven=0, failed=0)
    assert source.bodies() == []
    assert sorted(dlq.bodies()) == sorted(BODIES)


@pytest.mark.asyncio
async def test_dry_run_stops_at_max_messages(dlq: RecordingQueue, source: RecordingQueue) -> None:
    bodies = [f"body-{index}" for index in range(5)]
    await _seed(dlq, bodies)

    report = await redrive_dlq(dlq=dlq, source=source, max_messages=2)

    assert report == RedriveReport(seen=2, redriven=0, failed=0)
    assert len(dlq.ops("change_visibility")) == 2
    assert sorted(await _visible(dlq)) == sorted(bodies)


# --- apply ------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_sends_the_raw_body_unchanged_then_deletes(
    dlq: RecordingQueue, source: RecordingQueue
) -> None:
    await _seed(dlq, BODIES)

    report = await redrive_dlq(dlq=dlq, source=source, max_messages=10, apply=True)

    assert report == RedriveReport(seen=3, redriven=3, failed=0)
    assert sorted(source.bodies()) == sorted(BODIES)  # byte-identical, never re-serialized
    assert source.ops("send") == []  # the raw path, never the envelope path
    assert dlq.bodies() == []
    for body in BODIES:
        sent = dlq.events.index(("source", "send_raw", body))
        deleted = dlq.events.index(("dlq", "delete", body))
        assert sent < deleted


@pytest.mark.asyncio
async def test_apply_moves_exactly_max_messages_and_leaves_the_rest(
    dlq: RecordingQueue, source: RecordingQueue
) -> None:
    bodies = [f"body-{index}" for index in range(5)]
    await _seed(dlq, bodies)

    report = await redrive_dlq(dlq=dlq, source=source, max_messages=3, apply=True)

    assert report == RedriveReport(seen=3, redriven=3, failed=0)
    assert len(source.bodies()) == 3
    assert len(dlq.bodies()) == 2
    assert sorted(source.bodies() + dlq.bodies()) == sorted(bodies)


@pytest.mark.asyncio
async def test_apply_pages_through_more_than_one_receive_batch(
    dlq: RecordingQueue, source: RecordingQueue
) -> None:
    bodies = [f"body-{index:02d}" for index in range(25)]
    await _seed(dlq, bodies)

    report = await redrive_dlq(dlq=dlq, source=source, max_messages=25, apply=True)

    assert report == RedriveReport(seen=25, redriven=25, failed=0)
    assert sorted(source.bodies()) == bodies
    assert dlq.bodies() == []
    assert all(1 <= event[2] <= 10 for event in dlq.ops("receive"))


@pytest.mark.asyncio
async def test_redrive_stops_when_the_dlq_is_empty(
    dlq: RecordingQueue, source: RecordingQueue
) -> None:
    await _seed(dlq, BODIES[:2])

    report = await redrive_dlq(dlq=dlq, source=source, max_messages=50, apply=True)

    assert report == RedriveReport(seen=2, redriven=2, failed=0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        IngressQueueError("source unavailable", retryable=True),
        IngressQueueError("source refused", retryable=False),
        IngressMessageTooLarge("body exceeds the guard"),
    ],
    ids=["retryable", "permanent", "too_large"],
)
async def test_a_failed_send_never_deletes_the_message(
    dlq: RecordingQueue, source: RecordingQueue, error: BaseException
) -> None:
    await _seed(dlq, BODIES)
    source.fail_bodies[BODIES[1]] = error

    report = await redrive_dlq(dlq=dlq, source=source, max_messages=10, apply=True)

    assert report == RedriveReport(seen=3, redriven=2, failed=1)
    assert ("dlq", "delete", BODIES[1]) not in dlq.events
    assert dlq.bodies() == [BODIES[1]]
    assert sorted(source.bodies()) == sorted([BODIES[0], BODIES[2]])


@pytest.mark.asyncio
async def test_a_failing_source_leaves_the_dlq_intact_and_sees_each_message_once(
    dlq: RecordingQueue, source: RecordingQueue
) -> None:
    await _seed(dlq, BODIES)
    for body in BODIES:
        source.fail_bodies[body] = IngressQueueError("source unavailable", retryable=True)

    report = await redrive_dlq(dlq=dlq, source=source, max_messages=10, apply=True)

    assert report == RedriveReport(seen=3, redriven=0, failed=3)
    assert dlq.ops("delete") == []
    assert sorted(dlq.bodies()) == sorted(BODIES)
    assert len(source.ops("send_raw")) == 3  # one attempt per message, no re-receive loop
