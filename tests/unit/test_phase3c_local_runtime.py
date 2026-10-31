from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path

import yaml

from scripts.dev.publish_ses_event import build_ses_event, publish_event

ROOT = Path(__file__).resolve().parents[2]


ELASTICMQ_CONF = ROOT / "infra" / "elasticmq" / "elasticmq.conf"
QUEUE_DEFAULTS = (
    "defaultVisibilityTimeout = 60 seconds",
    "delay = 0 seconds",
)
REDRIVEN_QUEUES = {
    "limnopulse-notification-jobs": "limnopulse-notification-jobs-dlq",
    "limnopulse-telegram-notification-jobs": "limnopulse-telegram-notification-jobs-dlq",
    "limnopulse-ses-events": "limnopulse-ses-events-dlq",
    "limnopulse-telemetry-ingress": "limnopulse-telemetry-ingress-dlq",
}


def _block_body(text: str, header: re.Match[str]) -> str:
    depth, index = 0, header.end() - 1
    while True:
        if text[index] == "{":
            depth += 1
        elif text[index] == "}":
            depth -= 1
            if depth == 0:
                return text[header.end() : index]
        index += 1


def _elasticmq_queues_block(config: str) -> str:
    headers = list(re.finditer(r"^queues\s*\{", config, re.MULTILINE))
    assert len(headers) == 1
    return _block_body(config, headers[0])


def _elasticmq_queue_header(config: str, queue: str) -> re.Match[str]:
    """Locate one queue declaration inside the top-level ``queues {}`` block."""
    queues = _elasticmq_queues_block(config)
    headers = list(re.finditer(rf'^\s*"{re.escape(queue)}"\s*\{{', queues, re.MULTILINE))
    assert len(headers) == 1, queue
    return headers[0]


def _elasticmq_queue_lines(config: str, queue: str) -> list[str]:
    """Return the normalized body lines of one ElasticMQ queue block."""
    queues = _elasticmq_queues_block(config)
    body = _block_body(queues, _elasticmq_queue_header(config, queue))
    return [line.strip() for line in body.splitlines() if line.strip()]


class FakeSQS:
    def __init__(self) -> None:
        self.requests: list[dict[str, str]] = []

    def send_message(self, **request: str) -> dict[str, str]:
        self.requests.append(request)
        return {"MessageId": "local-sqs-message-1"}


def test_elasticmq_defines_notification_queues_and_redrive_deterministically() -> None:
    compose = yaml.safe_load((ROOT / "compose.yaml").read_text(encoding="utf-8"))
    service = compose["services"]["elasticmq"]
    config = (ROOT / "infra" / "elasticmq" / "elasticmq.conf").read_text(encoding="utf-8")

    assert service["image"].startswith("softwaremill/elasticmq-native:")
    assert not service["image"].endswith(":latest")
    assert service["ports"] == ["127.0.0.1:9324:9324"]
    assert "./infra/elasticmq/elasticmq.conf:/opt/elasticmq.conf:ro" in service["volumes"]
    assert "healthcheck" in service
    assert "Action=ListQueues" in " ".join(service["healthcheck"]["test"])
    for queue in (
        "limnopulse-notification-jobs",
        "limnopulse-notification-jobs-dlq",
        "limnopulse-telegram-notification-jobs",
        "limnopulse-telegram-notification-jobs-dlq",
        "limnopulse-ses-events",
        "limnopulse-ses-events-dlq",
        "limnopulse-ses-events-routing-dlq",
    ):
        assert f'"{queue}"' in config
    for queue, dlq in REDRIVEN_QUEUES.items():
        lines = _elasticmq_queue_lines(config, queue)
        assert lines[-4:] == [
            "deadLettersQueue {",
            f'name = "{dlq}"',
            "maxReceiveCount = 8",
            "}",
        ], queue
        assert sum("deadLettersQueue" in line for line in lines) == 1, queue
        assert "deadLettersQueue" not in " ".join(_elasticmq_queue_lines(config, dlq)), dlq
    assert config.count("maxReceiveCount = 8") == len(REDRIVEN_QUEUES)


def test_elasticmq_defines_telemetry_ingress_queue_and_dlq_exactly() -> None:
    config = ELASTICMQ_CONF.read_text(encoding="utf-8")

    assert _elasticmq_queue_lines(config, "limnopulse-telemetry-ingress-dlq") == [
        *QUEUE_DEFAULTS,
        "receiveMessageWait = 0 seconds",
    ]
    assert _elasticmq_queue_lines(config, "limnopulse-telemetry-ingress") == [
        *QUEUE_DEFAULTS,
        "receiveMessageWait = 20 seconds",
        "deadLettersQueue {",
        'name = "limnopulse-telemetry-ingress-dlq"',
        "maxReceiveCount = 8",
        "}",
    ]
    assert "RedriveAllowPolicy" not in config
    assert "redrive-allow" not in config.lower()
    assert (
        _elasticmq_queue_header(config, "limnopulse-telemetry-ingress-dlq").start()
        < _elasticmq_queue_header(config, "limnopulse-telemetry-ingress").start()
    )


def test_notification_compose_processes_use_real_sdk_endpoints_and_safe_fake_sender() -> None:
    compose = yaml.safe_load((ROOT / "compose.yaml").read_text(encoding="utf-8"))
    relay = compose["services"]["notification-relay"]
    worker = compose["services"]["notification-worker"]

    assert relay["profiles"] == ["notifications"]
    assert relay["command"] == ["relay"]
    assert relay["restart"] == "no"
    assert worker["profiles"] == ["notifications"]
    assert worker["command"] == ["worker"]
    assert worker["init"] is True
    assert worker["stop_grace_period"] == "45s"
    assert worker["environment"]["NOTIFICATION_EMAIL_SENDER_MODE"] == "success"
    assert "NOTIFICATION_FAKE_MESSAGE_ID" not in worker["environment"]
    assert worker["environment"]["SES_CONFIGURATION_SET_NAME"] == "limnopulse-notifications"
    assert worker["environment"]["SQS_ENDPOINT_URL"] == "http://elasticmq:9324"
    assert worker["environment"]["SQS_NOTIFICATION_JOBS_URL"].endswith(
        "/limnopulse-notification-jobs"
    )
    assert worker["environment"]["SQS_SES_EVENTS_URL"].endswith("/limnopulse-ses-events")
    assert "SES_ENDPOINT_URL" not in worker["environment"]


def test_notifications_container_is_distroless_nonroot_and_keeps_commands_external() -> None:
    dockerfile = (ROOT / "Dockerfile.notifications").read_text(encoding="utf-8")

    assert "CGO_ENABLED=0" in dockerfile
    assert "gcr.io/distroless/static-debian12:nonroot" in dockerfile
    assert "USER nonroot:nonroot" in dockerfile
    assert 'ENTRYPOINT ["/notifications"]' in dockerfile
    assert "CMD" not in dockerfile
    assert "HEALTHCHECK NONE" in dockerfile


def test_synthetic_ses_helper_builds_every_supported_event_without_recipient_pii() -> None:
    now = datetime(2026, 7, 17, 12, 0, tzinfo=UTC)
    for event_type in (
        "Send",
        "DeliveryDelay",
        "Delivery",
        "Bounce",
        "Complaint",
        "Reject",
    ):
        event = build_ses_event(
            event_type=event_type,
            delivery_id="del_real_1",
            attempt_id="att_real_1",
            provider_message_id="provider_real_1",
            event_id=f"evt_{event_type}",
            occurred_at=now,
        )
        assert event["version"] == "0"
        assert event["source"] == "aws.ses"
        assert event["detail"]["eventType"] == event_type
        assert event["detail"]["mail"]["tags"] == {
            "delivery_id": ["del_real_1"],
            "attempt_id": ["att_real_1"],
        }
        encoded = json.dumps(event)
        assert "destination" not in encoded.lower()
        assert "@" not in encoded


def test_synthetic_ses_helper_publishes_exact_event_to_selected_queue() -> None:
    client = FakeSQS()
    event = build_ses_event(
        event_type="Delivery",
        delivery_id="del_1",
        attempt_id="att_1",
        provider_message_id="provider_1",
        event_id="evt_1",
    )

    message_id = publish_event(
        client=client,
        queue_url="http://elasticmq:9324/000000000000/limnopulse-ses-events",
        event=event,
    )

    assert message_id == "local-sqs-message-1"
    assert client.requests == [
        {
            "QueueUrl": "http://elasticmq:9324/000000000000/limnopulse-ses-events",
            "MessageBody": json.dumps(event, separators=(",", ":"), sort_keys=True),
        }
    ]


def test_environment_example_exposes_notification_local_contract_without_real_email() -> None:
    environment = (ROOT / ".env.example").read_text(encoding="utf-8")

    for key in (
        "SQS_ENDPOINT_URL",
        "SQS_NOTIFICATION_JOBS_URL",
        "SQS_SES_EVENTS_URL",
        "SES_FROM_EMAIL",
        "SES_CONFIGURATION_SET_NAME",
        "NOTIFICATION_EMAIL_SENDER_MODE",
        "NOTIFICATION_FAKE_MESSAGE_ID",
    ):
        assert f"{key}=" in environment
    assert "NOTIFICATION_EMAIL_SENDER_MODE=success" in environment
    assert "@example.test" in environment
