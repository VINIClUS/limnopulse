"""P3-05 GREEN review regressions (the two diff reviews), kept apart from the frozen RED tests.

- A cancelled delete or visibility change waits for its in-flight call, so a late heartbeat can
  never land after the worker's final action (P3-08 heartbeat rule).
- A malformed receive response becomes a retryable queue error, not a bare `KeyError`.
- The redrive releases what it holds even when the scan fails, best-effort per message, and with
  `apply` releases the messages it could not move (failed send, or failed delete after a send).
"""

import asyncio
import threading
from datetime import UTC, datetime
from typing import Any

import pytest

from limnopulse_api.adapters.sqs_ingestion import SqsIngressQueue
from limnopulse_api.core.errors import IngressQueueError
from limnopulse_api.domain.ingestion import IngressQueueMessage
from limnopulse_api.services.ingress_redrive import RedriveReport, redrive_dlq
from tests.support.ingestion_fakes import FakeIngressQueue, Faults

MESSAGE = IngressQueueMessage(
    message_id="m-1",
    receipt_handle="handle-1",
    body="{}",
    receive_count=1,
    sent_at=datetime(2026, 10, 30, tzinfo=UTC),
)


class BlockingClient:
    """Each call signals `started`, then blocks until `gate` opens, then records completion."""

    def __init__(self) -> None:
        self.started = threading.Event()
        self.gate = threading.Event()
        self.finished: list[str] = []

    def _block(self, name: str) -> dict[str, Any]:
        self.started.set()
        assert self.gate.wait(5)
        self.finished.append(name)
        return {}

    def delete_message(self, **_: Any) -> dict[str, Any]:
        return self._block("delete_message")

    def change_message_visibility(self, **_: Any) -> dict[str, Any]:
        return self._block("change_message_visibility")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "method"),
    [
        ("heartbeat", "change_message_visibility"),
        ("change_visibility", "change_message_visibility"),
        ("delete", "delete_message"),
    ],
)
async def test_a_cancelled_final_action_waits_for_its_in_flight_call(
    operation: str, method: str
) -> None:
    client = BlockingClient()
    queue = SqsIngressQueue(client=client, queue_url="http://sqs.invalid/q")
    invoke = {
        "heartbeat": lambda: queue.heartbeat(MESSAGE, 60),
        "change_visibility": lambda: queue.change_visibility(MESSAGE, 60),
        "delete": lambda: queue.delete(MESSAGE),
    }[operation]
    task = asyncio.ensure_future(invoke())
    assert await asyncio.to_thread(client.started.wait, 5)

    task.cancel()
    threading.Timer(0.05, client.gate.set).start()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert client.finished == [method]  # the call settled before the cancellation returned


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "item",
    [
        {"MessageId": "m", "ReceiptHandle": "h", "Body": "{}"},
        {"MessageId": "m", "ReceiptHandle": "h", "Body": "{}", "Attributes": {}},
        {
            "MessageId": "m",
            "ReceiptHandle": "h",
            "Body": "{}",
            "Attributes": {"ApproximateReceiveCount": "x", "SentTimestamp": "1"},
        },
    ],
    ids=["no_attributes", "empty_attributes", "non_numeric_count"],
)
async def test_a_malformed_receive_response_is_a_retryable_queue_error(item: dict) -> None:
    class Client:
        def receive_message(self, **_: Any) -> dict[str, Any]:
            return {"Messages": [item]}

    queue = SqsIngressQueue(client=Client(), queue_url="http://sqs.invalid/q")

    with pytest.raises(IngressQueueError) as exc_info:
        await queue.receive(max_messages=1, wait_seconds=0, visibility_seconds=30)

    assert exc_info.value.retryable is True


# --- redrive ----------------------------------------------------------------------------------


class SpyQueue:
    """A fake queue whose receive, delete or release can be made to fail."""

    def __init__(self) -> None:
        self.inner = FakeIngressQueue(Faults())
        self.receive_calls = 0
        self.fail_receive_on: int | None = None
        self.fail_delete = False
        self.fail_release_of: set[str] = set()
        self.sent: list[str] = []
        self.fail_send = False

    async def send(self, envelope: Any) -> str:
        return await self.inner.send(envelope)

    async def send_raw(self, body: str) -> str:
        self.sent.append(body)
        if self.fail_send:
            raise IngressQueueError("source unavailable", retryable=True)
        return await self.inner.send_raw(body)

    async def receive(self, **kwargs: Any) -> list[IngressQueueMessage]:
        self.receive_calls += 1
        if self.receive_calls == self.fail_receive_on:
            raise IngressQueueError("dlq unavailable", retryable=True)
        return await self.inner.receive(**kwargs)

    async def delete(self, message: IngressQueueMessage) -> None:
        if self.fail_delete:
            raise IngressQueueError("delete refused", retryable=False)
        await self.inner.delete(message)

    async def change_visibility(self, message: IngressQueueMessage, seconds: int) -> None:
        if message.body in self.fail_release_of:
            raise IngressQueueError("release refused", retryable=False)
        await self.inner.change_visibility(message, seconds)

    async def visible(self) -> list[str]:
        batch = await self.inner.receive(max_messages=10, wait_seconds=0, visibility_seconds=300)
        return sorted(message.body for message in batch)


async def _seed(queue: SpyQueue, count: int) -> list[str]:
    bodies = [f"body-{index:02d}" for index in range(count)]
    for body in bodies:
        await queue.inner.send_raw(body)
    return bodies


async def _redrive(**kwargs: Any) -> RedriveReport:
    return await asyncio.wait_for(redrive_dlq(**kwargs), 10)


@pytest.mark.asyncio
async def test_a_dry_run_that_fails_mid_scan_still_releases_what_it_saw() -> None:
    dlq, source = SpyQueue(), SpyQueue()
    bodies = await _seed(dlq, 12)
    dlq.fail_receive_on = 2  # the first batch of 10 is held, then the scan fails

    with pytest.raises(IngressQueueError):
        await _redrive(dlq=dlq, source=source, max_messages=20)

    assert await dlq.visible() == bodies[:10]


@pytest.mark.asyncio
async def test_a_failed_release_does_not_stop_the_others_or_lose_the_report() -> None:
    dlq, source = SpyQueue(), SpyQueue()
    bodies = await _seed(dlq, 3)
    dlq.fail_release_of = {bodies[0]}

    report = await _redrive(dlq=dlq, source=source, max_messages=10)

    assert report == RedriveReport(seen=3, redriven=0, failed=0)
    assert await dlq.visible() == bodies[1:]


@pytest.mark.asyncio
async def test_apply_releases_the_messages_it_could_not_send() -> None:
    dlq, source = SpyQueue(), SpyQueue()
    bodies = await _seed(dlq, 3)
    source.fail_send = True

    report = await _redrive(dlq=dlq, source=source, max_messages=10, apply=True)

    assert report == RedriveReport(seen=3, redriven=0, failed=3)
    assert await dlq.visible() == bodies  # a rerun sees them at once


@pytest.mark.asyncio
async def test_a_failed_delete_after_a_send_counts_as_failed_and_keeps_the_message() -> None:
    dlq, source = SpyQueue(), SpyQueue()
    bodies = await _seed(dlq, 1)
    dlq.fail_delete = True

    report = await _redrive(dlq=dlq, source=source, max_messages=10, apply=True)

    assert report == RedriveReport(seen=1, redriven=0, failed=1)
    assert source.sent == bodies  # sent once; the duplicate on a rerun is safe (D10)
    assert await dlq.visible() == bodies
