from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import yaml

from scripts.dev.publish_ses_event import build_ses_event, publish_event

ROOT = Path(__file__).resolve().parents[2]


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
        "limnopulse-ses-events",
        "limnopulse-ses-events-dlq",
        "limnopulse-ses-events-routing-dlq",
    ):
        assert f'"{queue}"' in config
    assert config.count("maxReceiveCount = 8") == 2


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
    assert worker["environment"]["NOTIFICATION_FAKE_MESSAGE_ID"] == "provider_message_local_compose"
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
