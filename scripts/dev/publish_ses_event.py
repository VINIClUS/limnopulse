from __future__ import annotations

import argparse
import json
import os
import uuid
from datetime import UTC, datetime
from typing import Any, Protocol

import boto3


SUPPORTED_EVENT_TYPES = (
    "Send",
    "DeliveryDelay",
    "Delivery",
    "Bounce",
    "Complaint",
    "Reject",
)


class SQSClient(Protocol):
    def send_message(self, **request: str) -> dict[str, str]: ...


def _require_identity(name: str, value: str) -> str:
    normalized = value.strip()
    if not normalized or "\x00" in normalized:
        raise ValueError(f"{name} is invalid")
    return normalized


def build_ses_event(
    *,
    event_type: str,
    delivery_id: str,
    attempt_id: str,
    provider_message_id: str,
    event_id: str | None = None,
    occurred_at: datetime | None = None,
    bounce_type: str = "Permanent",
    reject_reason: str = "Bad content",
) -> dict[str, Any]:
    if event_type not in SUPPORTED_EVENT_TYPES:
        raise ValueError("event_type is unsupported")
    delivery_id = _require_identity("delivery_id", delivery_id)
    attempt_id = _require_identity("attempt_id", attempt_id)
    provider_message_id = _require_identity("provider_message_id", provider_message_id)
    event_id = _require_identity("event_id", event_id or f"evt_{uuid.uuid4()}")
    occurred_at = occurred_at or datetime.now(UTC)
    if occurred_at.tzinfo is None:
        raise ValueError("occurred_at must be timezone-aware")
    timestamp = occurred_at.astimezone(UTC).isoformat().replace("+00:00", "Z")

    detail: dict[str, Any] = {
        "eventType": event_type,
        "mail": {
            "timestamp": timestamp,
            "messageId": provider_message_id,
            "tags": {
                "delivery_id": [delivery_id],
                "attempt_id": [attempt_id],
            },
        },
    }
    if event_type == "DeliveryDelay":
        detail["deliveryDelay"] = {
            "delayType": "TransientCommunicationFailure",
            "delayedRecipients": [],
        }
    elif event_type == "Delivery":
        detail["delivery"] = {"processingTimeMillis": 1, "recipients": []}
    elif event_type == "Bounce":
        if bounce_type not in {"Permanent", "Transient"}:
            raise ValueError("bounce_type must be Permanent or Transient")
        detail["bounce"] = {
            "bounceType": bounce_type,
            "bounceSubType": "General",
            "bouncedRecipients": [],
        }
    elif event_type == "Complaint":
        detail["complaint"] = {"complainedRecipients": []}
    elif event_type == "Reject":
        detail["reject"] = {"reason": _require_identity("reject_reason", reject_reason)}

    return {
        "version": "0",
        "id": event_id,
        "detail-type": f"Email {event_type}",
        "source": "aws.ses",
        "account": "000000000000",
        "time": timestamp,
        "region": "us-east-1",
        "resources": [],
        "detail": detail,
    }


def publish_event(*, client: SQSClient, queue_url: str, event: dict[str, Any]) -> str:
    queue_url = _require_identity("queue_url", queue_url)
    response = client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(event, separators=(",", ":"), sort_keys=True),
    )
    message_id = response.get("MessageId", "").strip()
    if not message_id:
        raise RuntimeError("SQS did not confirm the synthetic SES event")
    return message_id


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish a local synthetic EventBridge SES event to ElasticMQ."
    )
    parser.add_argument("event_type", choices=SUPPORTED_EVENT_TYPES)
    parser.add_argument("--delivery-id", required=True)
    parser.add_argument("--attempt-id", required=True)
    parser.add_argument("--provider-message-id", required=True)
    parser.add_argument("--event-id")
    parser.add_argument("--bounce-type", choices=("Permanent", "Transient"), default="Permanent")
    parser.add_argument("--reject-reason", default="Bad content")
    parser.add_argument(
        "--endpoint-url", default=os.getenv("SQS_ENDPOINT_URL", "http://127.0.0.1:9324")
    )
    parser.add_argument(
        "--queue-url",
        default=os.getenv(
            "SQS_SES_EVENTS_URL",
            "http://127.0.0.1:9324/000000000000/limnopulse-ses-events",
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    client = boto3.client(
        "sqs",
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        endpoint_url=args.endpoint_url,
        aws_access_key_id="local",
        aws_secret_access_key="local",
    )
    event = build_ses_event(
        event_type=args.event_type,
        delivery_id=args.delivery_id,
        attempt_id=args.attempt_id,
        provider_message_id=args.provider_message_id,
        event_id=args.event_id,
        bounce_type=args.bounce_type,
        reject_reason=args.reject_reason,
    )
    message_id = publish_event(client=client, queue_url=args.queue_url, event=event)
    print(json.dumps({"result": "published", "sqs_message_id": message_id}, sort_keys=True))


if __name__ == "__main__":
    main()
