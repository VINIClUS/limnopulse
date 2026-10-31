"""P3-05 unit tests for `SqsIngressQueue` against a stubbed boto3 SQS client.

The stub records every client call (with the thread it ran on) and returns canned responses, so
these tests pin the exact request parameters, the D6 size guard, the receive mapping, visibility
clamping and the botocore error mapping without a service. The real-queue semantics are proven by
the P3-00 queue kit on ElasticMQ in `tests/ingestion/test_sqs_ingestion_elasticmq.py`.
"""

import threading
from datetime import UTC, datetime
from typing import Any

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError, ReadTimeoutError

from limnopulse_api.adapters.sqs_ingestion import SqsIngressQueue
from limnopulse_api.core.errors import IngressMessageTooLarge, IngressQueueError
from limnopulse_api.domain.ingestion import (
    INGRESS_ENVELOPE_SCHEMA,
    INGRESS_SCHEMA_ATTRIBUTE_BYTES,
    MAX_INGRESS_MESSAGE_BYTES,
    IngressQueueMessage,
    ingress_message_bytes,
)
from limnopulse_api.repositories.ingestion import IngressQueueConsumer, IngressQueueProducer
from tests.support.ingestion_contract_kits import (
    envelope,
    envelope_of_message_bytes,
    multibyte_envelope,
    oversized_envelope,
)

QUEUE_URL = "http://127.0.0.1:9324/000000000000/limnopulse-telemetry-ingress"
SCHEMA_ATTRIBUTES = {
    "schema": {"DataType": "String", "StringValue": INGRESS_ENVELOPE_SCHEMA},
}
SENT_MS = 1_793_361_600_123  # 2026-10-30T12:00:00.123Z
SENT_AT = datetime(2026, 10, 30, 12, 0, 0, 123_000, tzinfo=UTC)
MESSAGE = IngressQueueMessage(
    message_id="m-1",
    receipt_handle="handle-1",
    body="{}",
    receive_count=1,
    sent_at=SENT_AT,
)


class StubSqsClient:
    """Records `(method, kwargs, thread)`; a queued response or exception answers each call."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any], threading.Thread]] = []
        self.responses: dict[str, list[Any]] = {}

    def answer(self, method: str, response: Any) -> None:
        self.responses.setdefault(method, []).append(response)

    def _call(self, method: str, kwargs: dict[str, Any]) -> Any:
        self.calls.append((method, kwargs, threading.current_thread()))
        pending = self.responses.get(method)
        response = pending.pop(0) if pending else _DEFAULTS[method]
        if isinstance(response, BaseException):
            raise response
        return response

    def send_message(self, **kwargs: Any) -> Any:
        return self._call("send_message", kwargs)

    def receive_message(self, **kwargs: Any) -> Any:
        return self._call("receive_message", kwargs)

    def delete_message(self, **kwargs: Any) -> Any:
        return self._call("delete_message", kwargs)

    def change_message_visibility(self, **kwargs: Any) -> Any:
        return self._call("change_message_visibility", kwargs)

    def methods(self) -> list[str]:
        return [method for method, _, _ in self.calls]


_DEFAULTS: dict[str, Any] = {
    "send_message": {"MessageId": "m-1", "MD5OfMessageBody": "x"},
    "receive_message": {},
    "delete_message": {},
    "change_message_visibility": {},
}


def _client_error(code: str, operation: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "stub"}}, operation)


def botocore_errors(operation: str) -> list[BaseException]:
    return [
        _client_error("ServiceUnavailable", operation),
        _client_error("QueueDoesNotExist", operation),
        EndpointConnectionError(endpoint_url="http://sqs.invalid"),
        ReadTimeoutError(endpoint_url="http://sqs.invalid"),
    ]


def error_id(error: BaseException) -> str:
    if isinstance(error, ClientError):
        return error.response["Error"]["Code"]
    return type(error).__name__


async def on_worker_thread(client: StubSqsClient) -> bool:
    """True when every recorded client call ran off the event-loop thread (asyncio.to_thread)."""
    loop_thread = threading.current_thread()
    return bool(client.calls) and all(thread is not loop_thread for _, _, thread in client.calls)


@pytest.fixture
def client() -> StubSqsClient:
    return StubSqsClient()


@pytest.fixture
def queue(client: StubSqsClient) -> SqsIngressQueue:
    return SqsIngressQueue(client=client, queue_url=QUEUE_URL)


def test_constructor_is_keyword_only_and_implements_both_ports(client: StubSqsClient) -> None:
    with pytest.raises(TypeError):
        SqsIngressQueue(client, QUEUE_URL)  # type: ignore[misc]

    queue = SqsIngressQueue(client=client, queue_url=QUEUE_URL)

    assert isinstance(queue, IngressQueueProducer)
    assert isinstance(queue, IngressQueueConsumer)


# --- send -------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_sends_the_compact_envelope_with_the_schema_attribute(
    client: StubSqsClient, queue: SqsIngressQueue
) -> None:
    message = envelope()
    client.answer("send_message", {"MessageId": "sqs-id-7"})

    message_id = await queue.send(message)

    assert message_id == "sqs-id-7"
    ((method, kwargs, _),) = client.calls
    assert method == "send_message"
    assert kwargs == {
        "QueueUrl": QUEUE_URL,
        "MessageBody": message.to_json(),
        "MessageAttributes": SCHEMA_ATTRIBUTES,
    }
    assert await on_worker_thread(client)  # asyncio.to_thread, never the event loop


@pytest.mark.asyncio
async def test_send_preserves_non_ascii_bytes(
    client: StubSqsClient, queue: SqsIngressQueue
) -> None:
    message = multibyte_envelope().model_copy(
        update={"observations": multibyte_envelope().observations[:3]}
    )

    await queue.send(message)

    body = client.calls[0][1]["MessageBody"]
    assert body == message.to_json()
    assert "\U0001f600" in body  # ensure_ascii=False: never \\ud83d escapes


@pytest.mark.asyncio
async def test_send_accepts_an_envelope_exactly_at_the_guard(
    client: StubSqsClient, queue: SqsIngressQueue
) -> None:
    message = envelope_of_message_bytes(MAX_INGRESS_MESSAGE_BYTES)

    await queue.send(message)

    assert client.methods() == ["send_message"]
    sent = client.calls[0][1]["MessageBody"]
    assert len(sent.encode("utf-8")) + INGRESS_SCHEMA_ATTRIBUTE_BYTES == MAX_INGRESS_MESSAGE_BYTES


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "build",
    [
        lambda: envelope_of_message_bytes(MAX_INGRESS_MESSAGE_BYTES + 1),
        multibyte_envelope,  # fewer characters than the guard, more UTF-8 bytes
        oversized_envelope,
    ],
    ids=["one_byte_over", "multibyte_over", "largest_oversized"],
)
async def test_send_over_the_guard_raises_and_never_calls_sqs(
    client: StubSqsClient, queue: SqsIngressQueue, build: Any
) -> None:
    message = build()
    assert ingress_message_bytes(message) > MAX_INGRESS_MESSAGE_BYTES

    with pytest.raises(IngressMessageTooLarge):
        await queue.send(message)

    assert client.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("response", [{}, {"MessageId": ""}, {"MessageId": None}])
async def test_send_without_a_message_id_is_retryable(
    client: StubSqsClient, queue: SqsIngressQueue, response: dict[str, Any]
) -> None:
    client.answer("send_message", response)

    with pytest.raises(IngressQueueError) as exc_info:
        await queue.send(envelope())

    assert exc_info.value.retryable is True


@pytest.mark.asyncio
@pytest.mark.parametrize("error", botocore_errors("SendMessage"), ids=error_id)
async def test_send_maps_botocore_errors_to_retryable(
    client: StubSqsClient, queue: SqsIngressQueue, error: BaseException
) -> None:
    client.answer("send_message", error)

    with pytest.raises(IngressQueueError) as exc_info:
        await queue.send(envelope())

    assert exc_info.value.retryable is True
    assert client.methods() == ["send_message"]  # no internal retry; boto3 already retries


# --- send_raw ---------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_raw_sends_the_body_unchanged_with_the_schema_attribute(
    client: StubSqsClient, queue: SqsIngressQueue
) -> None:
    body = '{"schema":"unknown@9", "spacing" : "kept",\n"emoji":"\U0001f600"}'
    client.answer("send_message", {"MessageId": "raw-1"})

    assert await queue.send_raw(body) == "raw-1"

    ((method, kwargs, _),) = client.calls
    assert method == "send_message"
    assert kwargs == {
        "QueueUrl": QUEUE_URL,
        "MessageBody": body,
        "MessageAttributes": SCHEMA_ATTRIBUTES,
    }
    assert await on_worker_thread(client)


@pytest.mark.asyncio
async def test_send_raw_applies_the_same_size_guard_in_utf8_bytes(
    client: StubSqsClient, queue: SqsIngressQueue
) -> None:
    budget = MAX_INGRESS_MESSAGE_BYTES - INGRESS_SCHEMA_ATTRIBUTE_BYTES
    at_limit = "a" * budget
    one_over = "a" * (budget + 1)
    multibyte = "é" * (budget // 2 + 1)  # fewer characters than the budget, more bytes
    assert len(multibyte) <= budget < len(multibyte.encode("utf-8"))

    await queue.send_raw(at_limit)
    for body in (one_over, multibyte):
        with pytest.raises(IngressMessageTooLarge):
            await queue.send_raw(body)

    assert [kwargs["MessageBody"] for _, kwargs, _ in client.calls] == [at_limit]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response", [{"MD5OfMessageBody": "x"}, {"MessageId": ""}, {"MessageId": None}]
)
async def test_send_raw_without_a_message_id_is_retryable(
    client: StubSqsClient, queue: SqsIngressQueue, response: dict[str, Any]
) -> None:
    client.answer("send_message", response)

    with pytest.raises(IngressQueueError) as exc_info:
        await queue.send_raw("{}")

    assert exc_info.value.retryable is True


@pytest.mark.asyncio
@pytest.mark.parametrize("error", botocore_errors("SendMessage"), ids=error_id)
async def test_send_raw_maps_botocore_errors_to_retryable(
    client: StubSqsClient, queue: SqsIngressQueue, error: BaseException
) -> None:
    client.answer("send_message", error)

    with pytest.raises(IngressQueueError) as exc_info:
        await queue.send_raw("{}")

    assert exc_info.value.retryable is True


# --- receive ----------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_receive_sends_the_exact_request(
    client: StubSqsClient, queue: SqsIngressQueue
) -> None:
    await queue.receive(max_messages=7, wait_seconds=20, visibility_seconds=60)

    ((method, kwargs, _),) = client.calls
    assert method == "receive_message"
    assert kwargs == {
        "QueueUrl": QUEUE_URL,
        "MaxNumberOfMessages": 7,
        "WaitTimeSeconds": 20,
        "VisibilityTimeout": 60,
        "MessageSystemAttributeNames": ["ApproximateReceiveCount", "SentTimestamp"],
    }
    assert "AttributeNames" not in kwargs  # deprecated
    assert await on_worker_thread(client)


@pytest.mark.asyncio
async def test_receive_maps_each_message(client: StubSqsClient, queue: SqsIngressQueue) -> None:
    client.answer(
        "receive_message",
        {
            "Messages": [
                {
                    "MessageId": "m-1",
                    "ReceiptHandle": "handle-1",
                    "MD5OfBody": "ignored",
                    "Body": '{"a":"\U0001f600"}',
                    "Attributes": {"ApproximateReceiveCount": "1", "SentTimestamp": str(SENT_MS)},
                },
                {
                    "MessageId": "m-2",
                    "ReceiptHandle": "handle-2",
                    "Body": "not json at all",
                    "Attributes": {"ApproximateReceiveCount": "9", "SentTimestamp": "0"},
                },
                {
                    # 1.5 s after the epoch: milliseconds, never read as seconds.
                    "MessageId": "m-3",
                    "ReceiptHandle": "handle-3",
                    "Body": "",
                    "Attributes": {"ApproximateReceiveCount": "2", "SentTimestamp": "1500"},
                },
            ]
        },
    )

    received = await queue.receive(max_messages=10, wait_seconds=0, visibility_seconds=30)

    assert received == [
        IngressQueueMessage(
            message_id="m-1",
            receipt_handle="handle-1",
            body='{"a":"\U0001f600"}',
            receive_count=1,
            sent_at=SENT_AT,
        ),
        IngressQueueMessage(
            message_id="m-2",
            receipt_handle="handle-2",
            body="not json at all",
            receive_count=9,
            sent_at=datetime(1970, 1, 1, tzinfo=UTC),
        ),
        IngressQueueMessage(
            message_id="m-3",
            receipt_handle="handle-3",
            body="",
            receive_count=2,
            sent_at=datetime(1970, 1, 1, 0, 0, 1, 500_000, tzinfo=UTC),
        ),
    ]
    assert received[0].sent_at.utcoffset() is not None
    assert received[0].sent_at.utcoffset().total_seconds() == 0  # type: ignore[union-attr]


@pytest.mark.asyncio
@pytest.mark.parametrize("response", [{}, {"Messages": []}])
async def test_receive_of_an_empty_queue_is_an_empty_list(
    client: StubSqsClient, queue: SqsIngressQueue, response: dict[str, Any]
) -> None:
    client.answer("receive_message", response)

    assert await queue.receive(max_messages=1, wait_seconds=0, visibility_seconds=0) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("error", botocore_errors("ReceiveMessage"), ids=error_id)
async def test_receive_maps_botocore_errors_to_retryable(
    client: StubSqsClient, queue: SqsIngressQueue, error: BaseException
) -> None:
    client.answer("receive_message", error)

    with pytest.raises(IngressQueueError) as exc_info:
        await queue.receive(max_messages=1, wait_seconds=0, visibility_seconds=30)

    assert exc_info.value.retryable is True


# --- delete and visibility --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_sends_the_receipt_handle(
    client: StubSqsClient, queue: SqsIngressQueue
) -> None:
    await queue.delete(MESSAGE)

    ((method, kwargs, _),) = client.calls
    assert (method, kwargs) == (
        "delete_message",
        {"QueueUrl": QUEUE_URL, "ReceiptHandle": "handle-1"},
    )
    assert await on_worker_thread(client)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("seconds", "expected"),
    [(0, 0), (1, 1), (45, 45), (43_200, 43_200), (43_201, 43_200), (10**9, 43_200), (-1, 0)],
)
async def test_change_visibility_clamps_to_the_sqs_range(
    client: StubSqsClient, queue: SqsIngressQueue, seconds: int, expected: int
) -> None:
    await queue.change_visibility(MESSAGE, seconds)

    ((method, kwargs, _),) = client.calls
    assert (method, kwargs) == (
        "change_message_visibility",
        {"QueueUrl": QUEUE_URL, "ReceiptHandle": "handle-1", "VisibilityTimeout": expected},
    )
    assert await on_worker_thread(client)


@pytest.mark.asyncio
@pytest.mark.parametrize(("seconds", "expected"), [(60, 60), (99_999, 43_200), (-5, 0)])
async def test_heartbeat_extends_visibility_through_change_visibility(
    client: StubSqsClient, queue: SqsIngressQueue, seconds: int, expected: int
) -> None:
    await queue.heartbeat(MESSAGE, seconds)

    assert [(method, kwargs) for method, kwargs, _ in client.calls] == [
        (
            "change_message_visibility",
            {"QueueUrl": QUEUE_URL, "ReceiptHandle": "handle-1", "VisibilityTimeout": expected},
        )
    ]


def _final_action_errors(operation: str) -> list[BaseException]:
    return [
        _client_error("ReceiptHandleIsInvalid", operation),
        _client_error("AWS.SimpleQueueService.MessageNotInflight", operation),
        *botocore_errors(operation),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("error", _final_action_errors("DeleteMessage"), ids=error_id)
async def test_delete_failures_are_permanent_queue_errors(
    client: StubSqsClient, queue: SqsIngressQueue, error: BaseException
) -> None:
    client.answer("delete_message", error)

    with pytest.raises(IngressQueueError) as exc_info:
        await queue.delete(MESSAGE)

    assert exc_info.value.retryable is False
    assert client.methods() == ["delete_message"]


@pytest.mark.asyncio
@pytest.mark.parametrize("error", _final_action_errors("ChangeMessageVisibility"), ids=error_id)
async def test_change_visibility_failures_are_permanent_queue_errors(
    client: StubSqsClient, queue: SqsIngressQueue, error: BaseException
) -> None:
    client.answer("change_message_visibility", error)

    with pytest.raises(IngressQueueError) as exc_info:
        await queue.change_visibility(MESSAGE, 30)

    assert exc_info.value.retryable is False


@pytest.mark.asyncio
async def test_heartbeat_failures_are_permanent_queue_errors(
    client: StubSqsClient, queue: SqsIngressQueue
) -> None:
    client.answer(
        "change_message_visibility", _client_error("ReceiptHandleIsInvalid", "ChangeVisibility")
    )

    with pytest.raises(IngressQueueError) as exc_info:
        await queue.heartbeat(MESSAGE, 60)

    assert exc_info.value.retryable is False


# --- injected faults pass through (A44) -------------------------------------------------------

OPERATIONS = {
    "send": ("send_message", lambda queue: queue.send(envelope())),
    "send_raw": ("send_message", lambda queue: queue.send_raw("{}")),
    "receive": (
        "receive_message",
        lambda queue: queue.receive(max_messages=1, wait_seconds=0, visibility_seconds=30),
    ),
    "delete": ("delete_message", lambda queue: queue.delete(MESSAGE)),
    "change_visibility": (
        "change_message_visibility",
        lambda queue: queue.change_visibility(MESSAGE, 30),
    ),
    "heartbeat": ("change_message_visibility", lambda queue: queue.heartbeat(MESSAGE, 30)),
}


FINAL_ACTIONS = ("delete", "change_visibility", "heartbeat")
ERROR_BUILDERS = {
    "queue_error_retryable": lambda: IngressQueueError("injected at the client", retryable=True),
    "queue_error_permanent": lambda: IngressQueueError("injected at the client", retryable=False),
    "non_botocore": lambda: RuntimeError("a bug, not a queue failure"),
}
# An injected IngressQueueError passes through every operation (A44). Any other non-botocore
# error passes through send, send_raw and receive, which map botocore errors only; on the final
# actions it is a permanent queue error instead (see the next test).
PASS_THROUGH = [
    pytest.param(operation, build, id=f"{operation}-{kind}")
    for operation in OPERATIONS
    for kind, build in ERROR_BUILDERS.items()
    if not (kind == "non_botocore" and operation in FINAL_ACTIONS)
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("operation", "build"), PASS_THROUGH)
async def test_non_botocore_errors_pass_through_unchanged(
    client: StubSqsClient, queue: SqsIngressQueue, operation: str, build: Any
) -> None:
    method, invoke = OPERATIONS[operation]
    injected = build()
    client.answer(method, injected)

    with pytest.raises(type(injected)) as exc_info:
        await invoke(queue)

    assert exc_info.value is injected  # never re-wrapped
    assert client.methods() == [method]  # never retried internally


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", FINAL_ACTIONS)
async def test_final_action_non_queue_errors_are_permanent(
    client: StubSqsClient, queue: SqsIngressQueue, operation: str
) -> None:
    method, invoke = OPERATIONS[operation]
    injected = RuntimeError("an unexpected failure")
    client.answer(method, injected)

    with pytest.raises(IngressQueueError) as exc_info:
        await invoke(queue)

    assert exc_info.value.retryable is False
    assert exc_info.value.__cause__ is injected
    assert client.methods() == [method]
