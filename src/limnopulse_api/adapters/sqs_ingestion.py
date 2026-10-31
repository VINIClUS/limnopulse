"""SQS adapter for the telemetry ingress queue and its DLQ (P3-05, D6).

`SqsIngressQueue` is both the `IngressQueueProducer` and the `IngressQueueConsumer` of one queue.
Every boto3 call runs in `asyncio.to_thread`. The D6 size guard counts UTF-8 body bytes plus the
`schema` attribute bytes and refuses before any call. botocore failures of send and receive become
a retryable `IngressQueueError`; any failure of delete or a visibility change becomes a permanent
one (the worker loop logs and counts it). An `IngressQueueError` raised below the client boundary
passes through as-is, and nothing is retried here because boto3 already retries. Error messages
never carry the message body. A cancelled delete or visibility change still waits for its
in-flight call, so a late heartbeat can never land after the worker's final action.
"""

import asyncio
import contextlib
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from limnopulse_api.core.errors import IngressMessageTooLarge, IngressQueueError
from limnopulse_api.domain.ingestion import (
    INGRESS_ENVELOPE_SCHEMA,
    INGRESS_SCHEMA_ATTRIBUTE_BYTES,
    MAX_INGRESS_MESSAGE_BYTES,
    MAX_VISIBILITY_SECONDS,
    IngressEnvelope,
    IngressQueueMessage,
)

_SCHEMA_ATTRIBUTES = {"schema": {"DataType": "String", "StringValue": INGRESS_ENVELOPE_SCHEMA}}
_SYSTEM_ATTRIBUTES = ["ApproximateReceiveCount", "SentTimestamp"]
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class SqsIngressQueue:
    """Implements `IngressQueueProducer` and `IngressQueueConsumer` over one SQS queue."""

    def __init__(self, *, client: Any, queue_url: str) -> None:
        self._client = client
        self._queue_url = queue_url

    async def send(self, envelope: IngressEnvelope) -> str:
        return await self._send_body(envelope.to_json())

    async def send_raw(self, body: str) -> str:
        """Redrive only: the body is sent unchanged, under the same guard and attribute."""
        return await self._send_body(body)

    async def receive(
        self, *, max_messages: int, wait_seconds: int, visibility_seconds: int
    ) -> list[IngressQueueMessage]:
        try:
            response = await asyncio.to_thread(
                self._client.receive_message,
                QueueUrl=self._queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_seconds,
                VisibilityTimeout=visibility_seconds,
                MessageSystemAttributeNames=_SYSTEM_ATTRIBUTES,
            )
        except (BotoCoreError, ClientError) as error:
            raise IngressQueueError(_describe("receive", error), retryable=True) from error
        try:
            return [_message(item) for item in response.get("Messages") or []]
        except (KeyError, TypeError, ValueError) as error:
            raise IngressQueueError(
                "SQS receive returned a malformed message", retryable=True
            ) from error

    async def delete(self, message: IngressQueueMessage) -> None:
        try:
            await _run_to_completion(
                self._client.delete_message,
                QueueUrl=self._queue_url,
                ReceiptHandle=message.receipt_handle,
            )
        except IngressQueueError:
            raise
        except Exception as error:
            raise IngressQueueError(_describe("delete", error), retryable=False) from error

    async def change_visibility(self, message: IngressQueueMessage, seconds: int) -> None:
        timeout = min(max(seconds, 0), MAX_VISIBILITY_SECONDS)
        try:
            await _run_to_completion(
                self._client.change_message_visibility,
                QueueUrl=self._queue_url,
                ReceiptHandle=message.receipt_handle,
                VisibilityTimeout=timeout,
            )
        except IngressQueueError:
            raise
        except Exception as error:
            raise IngressQueueError(
                _describe("change_visibility", error), retryable=False
            ) from error

    async def heartbeat(self, message: IngressQueueMessage, seconds: int) -> None:
        """Extend the visibility of an in-flight message (the worker heartbeat)."""
        await self.change_visibility(message, seconds)

    async def _send_body(self, body: str) -> str:
        if len(body.encode("utf-8")) + INGRESS_SCHEMA_ATTRIBUTE_BYTES > MAX_INGRESS_MESSAGE_BYTES:
            raise IngressMessageTooLarge("ingress message exceeds the size guard")
        try:
            response = await asyncio.to_thread(
                self._client.send_message,
                QueueUrl=self._queue_url,
                MessageBody=body,
                MessageAttributes=_SCHEMA_ATTRIBUTES,
            )
        except (BotoCoreError, ClientError) as error:
            raise IngressQueueError(_describe("send", error), retryable=True) from error
        message_id = response.get("MessageId")
        if not message_id:
            raise IngressQueueError("send returned no message id", retryable=True)
        return str(message_id)


async def _run_to_completion(call: Callable[..., Any], /, **kwargs: Any) -> Any:
    """`asyncio.to_thread`, except that a cancelled caller first waits for the in-flight call.

    The thread cannot be stopped, so without this a cancelled heartbeat could still change the
    visibility after the caller's final `delete` or `change_visibility` (P3-08 heartbeat rule).
    """
    task = asyncio.ensure_future(asyncio.to_thread(call, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        with contextlib.suppress(Exception):
            await task
        raise


def _message(item: dict[str, Any]) -> IngressQueueMessage:
    attributes = item.get("Attributes") or {}
    sent_ms = int(attributes["SentTimestamp"])
    return IngressQueueMessage(
        message_id=item["MessageId"],
        receipt_handle=item["ReceiptHandle"],
        body=item["Body"],
        receive_count=int(attributes["ApproximateReceiveCount"]),
        sent_at=_EPOCH + timedelta(milliseconds=sent_ms),
    )


def _describe(operation: str, error: Exception) -> str:
    if isinstance(error, ClientError):
        code = error.response.get("Error", {}).get("Code", "unknown")
        return f"SQS {operation} failed ({code})"
    return f"SQS {operation} failed ({type(error).__name__})"
