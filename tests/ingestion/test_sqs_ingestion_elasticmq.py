"""P3-05 integration tests: `SqsIngressQueue` and `redrive_dlq` on real ElasticMQ 1.6.16.

They run the P3-00 `INGRESS_QUEUE_CONTRACT` kit against the real adapter, so the adapter and the
in-memory fake cannot drift apart, and prove the byte-identical body, the receive count, the
redrive policy and the DLQ redrive core. Faults are injected at the boto3 client boundary as
`IngressQueueError`, which the adapter must pass through unchanged (A44). Skips unless
``RUN_INGESTION_INTEGRATION=1`` (``make verify-ingestion``).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from limnopulse_api.adapters.sqs_ingestion import SqsIngressQueue
from limnopulse_api.core.errors import IngressMessageTooLarge
from limnopulse_api.domain.ingestion import INGRESS_ENVELOPE_SCHEMA, IngressQueueMessage
from limnopulse_api.services.ingress_redrive import RedriveReport, redrive_dlq
from tests.support.ingestion_contract_kits import (
    INGRESS_QUEUE_CONTRACT,
    envelope,
    kit_ids,
    multibyte_envelope,
    oversized_envelope,
)
from tests.support.ingestion_harness import IngestionQueuePair, sqs_client

QueueFactory = Callable[..., IngestionQueuePair]

# Kit operation (port method) -> the boto3 client method the adapter calls for it.
_CLIENT_METHODS = {
    "send": "send_message",
    "send_raw": "send_message",
    "receive": "receive_message",
    "delete": "delete_message",
    "change_visibility": "change_message_visibility",
}


class FaultyClient:
    """A real boto3 SQS client whose next call of a method can raise an injected error once."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self._pending: dict[str, list[BaseException]] = {}

    def fail_next(self, method: str, exc: BaseException) -> None:
        self._pending.setdefault(method, []).append(exc)

    def __getattr__(self, name: str) -> Any:
        target = getattr(self._inner, name)
        if not callable(target):
            return target

        def call(*args: Any, **kwargs: Any) -> Any:
            pending = self._pending.get(name)
            if pending:
                raise pending.pop(0)
            return target(*args, **kwargs)

        return call


class SqsQueueKitSubject:
    """The `QueueKitSubject` for the real adapter: one queue serves as producer and consumer."""

    def __init__(self, queue_url: str) -> None:
        self.client = FaultyClient(sqs_client())
        self.producer = SqsIngressQueue(client=self.client, queue_url=queue_url)
        self.consumer = self.producer

    def fail_next(self, operation: str, exc: BaseException) -> None:
        self.client.fail_next(_CLIENT_METHODS[operation], exc)


def _queue(url: str) -> SqsIngressQueue:
    return SqsIngressQueue(client=sqs_client(), queue_url=url)


def _counts(url: str) -> tuple[int, int]:
    attributes = sqs_client().get_queue_attributes(
        QueueUrl=url,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]
    return (
        int(attributes["ApproximateNumberOfMessages"]),
        int(attributes["ApproximateNumberOfMessagesNotVisible"]),
    )


async def _drain(queue: SqsIngressQueue) -> list[IngressQueueMessage]:
    drained: list[IngressQueueMessage] = []
    while batch := await queue.receive(max_messages=10, wait_seconds=0, visibility_seconds=60):
        drained.extend(batch)
    return drained


# --- the P3-00 queue kit on the real adapter --------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", INGRESS_QUEUE_CONTRACT, ids=kit_ids(INGRESS_QUEUE_CONTRACT))
async def test_queue_contract_kit_on_elasticmq(
    ingestion_queue_pair: QueueFactory, scenario: Callable[[Any], Any]
) -> None:
    pair = ingestion_queue_pair(max_receive_count=5, visibility_seconds=30)

    await scenario(SqsQueueKitSubject(pair.source_url))


# --- adapter semantics on ElasticMQ -----------------------------------------------------------


@pytest.mark.asyncio
async def test_send_then_receive_keeps_the_body_byte_identical(
    ingestion_queue_pair: QueueFactory,
) -> None:
    pair = ingestion_queue_pair()
    queue = _queue(pair.source_url)
    sample = multibyte_envelope()
    message = sample.model_copy(update={"observations": sample.observations[:5]})

    message_id = await queue.send(message)
    (received,) = await queue.receive(max_messages=10, wait_seconds=0, visibility_seconds=30)

    assert received.message_id == message_id
    assert received.body.encode("utf-8") == message.to_json().encode("utf-8")
    assert received.receive_count == 1
    assert received.sent_at.utcoffset() is not None
    assert received.sent_at.utcoffset().total_seconds() == 0  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_send_sets_the_schema_message_attribute(ingestion_queue_pair: QueueFactory) -> None:
    pair = ingestion_queue_pair()
    await _queue(pair.source_url).send(envelope())
    await _queue(pair.source_url).send_raw('{"raw":true}')

    response = sqs_client().receive_message(
        QueueUrl=pair.source_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=0,
        MessageAttributeNames=["All"],
    )

    attributes = [message["MessageAttributes"] for message in response["Messages"]]
    assert (
        attributes
        == [{"schema": {"DataType": "String", "StringValue": INGRESS_ENVELOPE_SCHEMA}}] * 2
    )


@pytest.mark.asyncio
async def test_receive_count_increments_on_every_receive(
    ingestion_queue_pair: QueueFactory,
) -> None:
    pair = ingestion_queue_pair(max_receive_count=5)
    queue = _queue(pair.source_url)
    await queue.send(envelope())

    counts = []
    for _ in range(3):
        (received,) = await queue.receive(max_messages=1, wait_seconds=0, visibility_seconds=30)
        counts.append(received.receive_count)
        await queue.change_visibility(received, 0)

    assert counts == [1, 2, 3]


@pytest.mark.asyncio
@pytest.mark.parametrize("build", [oversized_envelope, multibyte_envelope])
async def test_an_oversized_envelope_never_reaches_the_queue(
    ingestion_queue_pair: QueueFactory, build: Callable[[], Any]
) -> None:
    pair = ingestion_queue_pair()

    with pytest.raises(IngressMessageTooLarge):
        await _queue(pair.source_url).send(build())

    assert _counts(pair.source_url) == (0, 0)


# --- redrive policy and the redrive core -----------------------------------------------------


async def _receive_eventually(queue: SqsIngressQueue, attempts: int = 20) -> IngressQueueMessage:
    for _ in range(attempts):
        if received := await queue.receive(max_messages=1, wait_seconds=1, visibility_seconds=30):
            (message,) = received
            return message
    raise AssertionError("no message arrived")


@pytest.mark.asyncio
async def test_dlq_redrive_after_max_receive_count_preserves_the_body(
    ingestion_queue_pair: QueueFactory,
) -> None:
    pair = ingestion_queue_pair(max_receive_count=2)
    source, dlq = _queue(pair.source_url), _queue(pair.dlq_url)
    message = multibyte_envelope().model_copy(
        update={"observations": multibyte_envelope().observations[:2]}
    )
    await source.send(message)

    for expected in (1, 2):
        received = await _receive_eventually(source)
        assert received.receive_count == expected
        await source.change_visibility(received, 0)

    # The receive after maxReceiveCount moves the message to the DLQ instead of returning it.
    assert await source.receive(max_messages=10, wait_seconds=1, visibility_seconds=30) == []
    dead = await _receive_eventually(dlq)
    assert dead.body == message.to_json()
    await dlq.change_visibility(dead, 0)

    report = await redrive_dlq(dlq=dlq, source=source, max_messages=10, apply=True)

    assert report == RedriveReport(seen=1, redriven=1, failed=0)
    back = await _receive_eventually(source)
    assert back.body.encode("utf-8") == message.to_json().encode("utf-8")
    assert _counts(pair.dlq_url) == (0, 0)


@pytest.mark.asyncio
async def test_redrive_dlq_moves_exactly_n_messages_and_leaves_the_rest(
    ingestion_queue_pair: QueueFactory,
) -> None:
    pair = ingestion_queue_pair()
    source, dlq = _queue(pair.source_url), _queue(pair.dlq_url)
    bodies = [f'{{"n":{index},"e":"\U0001f600"}}' for index in range(5)]
    for body in bodies:
        await dlq.send_raw(body)

    report = await redrive_dlq(dlq=dlq, source=source, max_messages=3, apply=True)

    assert report == RedriveReport(seen=3, redriven=3, failed=0)
    moved = [message.body for message in await _drain(source)]
    left = [message.body for message in await _drain(dlq)]
    assert len(moved) == 3
    assert len(left) == 2
    assert sorted(moved + left) == sorted(bodies)


@pytest.mark.asyncio
async def test_redrive_dlq_dry_run_releases_every_message(
    ingestion_queue_pair: QueueFactory,
) -> None:
    pair = ingestion_queue_pair()
    source, dlq = _queue(pair.source_url), _queue(pair.dlq_url)
    bodies = [f'{{"n":{index}}}' for index in range(3)]
    for body in bodies:
        await dlq.send_raw(body)

    report = await redrive_dlq(dlq=dlq, source=source, max_messages=10)

    assert report == RedriveReport(seen=3, redriven=0, failed=0)
    assert _counts(pair.dlq_url) == (3, 0)  # released: all visible again at once
    assert _counts(pair.source_url) == (0, 0)
    assert sorted(message.body for message in await _drain(dlq)) == sorted(bodies)
