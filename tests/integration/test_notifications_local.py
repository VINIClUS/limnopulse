from __future__ import annotations

import base64
import hashlib
import json
import os
import signal
import struct
import subprocess
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError

from scripts.dev.init_dynamodb import ensure_table
from scripts.dev.publish_ses_event import build_ses_event, publish_event

ROOT = Path(__file__).resolve().parents[2]
RUN_INTEGRATION = os.getenv("RUN_NOTIFICATION_INTEGRATION") == "1"
SQS_ENDPOINT = "http://127.0.0.1:9324"


pytestmark = pytest.mark.skipif(
    not RUN_INTEGRATION,
    reason="set RUN_NOTIFICATION_INTEGRATION=1 after starting DynamoDB Local and ElasticMQ",
)


@dataclass(frozen=True)
class Runtime:
    binary: Path
    dynamodb: Any
    table: Any
    sqs: Any
    jobs_url: str
    jobs_dlq_url: str
    events_url: str
    events_dlq_url: str
    routing_dlq_url: str


def _aws_client(service: str, endpoint: str) -> Any:
    return boto3.client(
        service,
        region_name="us-east-1",
        endpoint_url=endpoint,
        aws_access_key_id="local",
        aws_secret_access_key="local",
        config=Config(connect_timeout=2, read_timeout=5, retries={"max_attempts": 1}),
    )


def _create_queue_set(sqs: Any, prefix: str) -> dict[str, str]:
    jobs_dlq_url = sqs.create_queue(QueueName=f"{prefix}-jobs-dlq")["QueueUrl"]
    events_dlq_url = sqs.create_queue(QueueName=f"{prefix}-events-dlq")["QueueUrl"]
    routing_dlq_url = sqs.create_queue(QueueName=f"{prefix}-routing-dlq")["QueueUrl"]
    jobs_dlq_arn = sqs.get_queue_attributes(QueueUrl=jobs_dlq_url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    events_dlq_arn = sqs.get_queue_attributes(QueueUrl=events_dlq_url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    # Ephemeral test queues disable their queue-level wait as well as the
    # worker flag. ElasticMQ otherwise applies the 20-second queue default to
    # an explicit zero and can retain a cancelled receive between processes.
    common = {"VisibilityTimeout": "60", "ReceiveMessageWaitTimeSeconds": "0"}
    jobs_url = sqs.create_queue(
        QueueName=f"{prefix}-jobs",
        Attributes={
            **common,
            "RedrivePolicy": json.dumps(
                {"deadLetterTargetArn": jobs_dlq_arn, "maxReceiveCount": "8"}
            ),
        },
    )["QueueUrl"]
    events_url = sqs.create_queue(
        QueueName=f"{prefix}-events",
        Attributes={
            **common,
            "RedrivePolicy": json.dumps(
                {"deadLetterTargetArn": events_dlq_arn, "maxReceiveCount": "8"}
            ),
        },
    )["QueueUrl"]
    return {
        "jobs_url": jobs_url,
        "jobs_dlq_url": jobs_dlq_url,
        "events_url": events_url,
        "events_dlq_url": events_dlq_url,
        "routing_dlq_url": routing_dlq_url,
    }


@pytest.fixture(scope="session")
def notification_binary(tmp_path_factory: pytest.TempPathFactory) -> Path:
    binary = tmp_path_factory.mktemp("notification-bin") / "notifications"
    subprocess.run(
        ["go", "build", "-trimpath", "-o", str(binary), "./cmd/notifications"],
        cwd=ROOT,
        check=True,
        timeout=120,
    )
    return binary


@pytest.fixture
def runtime(notification_binary: Path) -> Iterator[Runtime]:
    dynamodb = _aws_client("dynamodb", "http://127.0.0.1:8001")
    sqs = _aws_client("sqs", SQS_ENDPOINT)
    suffix = uuid.uuid4().hex[:12]
    table_name = f"LimnopulseNotificationsIntegration-{suffix}"
    try:
        ensure_table(dynamodb, table_name, include_alert_indexes=True)
        queue_names = {url.rsplit("/", 1)[-1] for url in sqs.list_queues().get("QueueUrls", [])}
    except EndpointConnectionError as error:
        pytest.fail(
            "local notification integration requires `docker compose up -d dynamodb-local elasticmq`: "
            f"{error}"
        )
    required_queue_names = {
        "limnopulse-notification-jobs",
        "limnopulse-notification-jobs-dlq",
        "limnopulse-ses-events",
        "limnopulse-ses-events-dlq",
        "limnopulse-ses-events-routing-dlq",
    }
    assert required_queue_names <= queue_names
    queue_urls = _create_queue_set(sqs, f"limnopulse-it-{suffix}")

    resource = boto3.resource(
        "dynamodb",
        region_name="us-east-1",
        endpoint_url="http://127.0.0.1:8001",
        aws_access_key_id="local",
        aws_secret_access_key="local",
        config=Config(connect_timeout=2, read_timeout=5, retries={"max_attempts": 1}),
    )
    current = Runtime(
        binary=notification_binary,
        dynamodb=dynamodb,
        table=resource.Table(table_name),
        sqs=sqs,
        **queue_urls,
    )
    yield current
    try:
        dynamodb.delete_table(TableName=table_name)
    except ClientError:
        pass
    for queue_url in queue_urls.values():
        try:
            sqs.delete_queue(QueueUrl=queue_url)
        except ClientError:
            pass


def _fixed(value: datetime) -> str:
    utc = value.astimezone(UTC)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond:06d}000Z"


def _relay_index(kind: str, tenant_id: str, item_id: str, at: datetime) -> tuple[str, str]:
    canonical = f"{kind}\0{tenant_id}\0{item_id}".encode()
    bucket = int.from_bytes(hashlib.sha256(canonical).digest()[:8], "big") % 64
    tenant = base64.urlsafe_b64encode(tenant_id.encode()).decode().rstrip("=")
    item = base64.urlsafe_b64encode(item_id.encode()).decode().rstrip("=")
    return (
        f"NOTIFICATION_RELAY#V1#BUCKET#{bucket:02d}",
        f"{_fixed(at)}#{kind}#{tenant}#{item}",
    )


def _delivery_id(event_id: str, kind: str, recipient_id: str) -> str:
    canonical = f"limnopulse:delivery:v1\0{event_id}\0{kind}\0email\0{recipient_id}"
    return "del_" + hashlib.sha256(canonical.encode()).hexdigest()


def _content_hash(
    template_id: str,
    subject: str,
    text: str,
    html: str,
) -> str:
    digest = hashlib.sha256()
    for field in (
        "limnopulse:rendered-email:v1",
        template_id,
        "1",
        "pt-BR",
        subject,
        text,
        html,
    ):
        encoded = field.encode()
        digest.update(struct.pack(">Q", len(encoded)))
        digest.update(encoded)
    return digest.hexdigest()


def _base_environment(runtime: Runtime, mode: str) -> dict[str, str]:
    environment = os.environ.copy()
    environment.update(
        {
            "APP_ENV": "local",
            "AWS_ACCESS_KEY_ID": "local",
            "AWS_SECRET_ACCESS_KEY": "local",
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_REGION": "us-east-1",
            "DYNAMODB_DOMAIN_TABLE": runtime.table.name,
            "DYNAMODB_ENDPOINT_URL": "http://127.0.0.1:8001",
            "SQS_ENDPOINT_URL": SQS_ENDPOINT,
            "SQS_NOTIFICATION_JOBS_URL": runtime.jobs_url,
            "SQS_SES_EVENTS_URL": runtime.events_url,
            "SES_FROM_EMAIL": "alerts@example.test",
            "SES_CONFIGURATION_SET_NAME": "limnopulse-notifications",
            "NOTIFICATION_EMAIL_SENDER_MODE": mode,
            "NOTIFICATION_MAX_SEND_RATE": "100",
        }
    )
    environment.pop("NOTIFICATION_FAKE_MESSAGE_ID", None)
    return environment


def _start_worker(
    runtime: Runtime,
    mode: str = "success",
    *,
    invalid_visibility: str | None = None,
) -> subprocess.Popen[str]:
    command = [
        str(runtime.binary),
        "worker",
        "--send-concurrency=1",
        "--feedback-concurrency=1",
        "--max-send-rate=100",
        "--send-burst=1",
        "--receive-wait=0s",
    ]
    if invalid_visibility is not None:
        command.append(f"--invalid-visibility={invalid_visibility}")
    return subprocess.Popen(
        command,
        cwd=ROOT,
        env=_base_environment(runtime, mode),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _stop_worker(process: subprocess.Popen[str]) -> dict[str, Any]:
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
    try:
        stdout, stderr = process.communicate(timeout=40)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate(timeout=10)
        pytest.fail("notification worker did not drain within 40s")
    assert process.returncode == 0, (
        f"notification worker exited with code {process.returncode}; "
        f"stdout_bytes={len(stdout)} stderr_bytes={len(stderr)}"
    )
    return json.loads(stdout)


def _run_relay(runtime: Runtime, relay_time: datetime) -> dict[str, Any]:
    completed = subprocess.run(
        [
            str(runtime.binary),
            "relay",
            f"--relay-time={relay_time.astimezone(UTC).isoformat().replace('+00:00', 'Z')}",
            "--query-parallelism=16",
            "--work-parallelism=8",
            "--max-work=200",
        ],
        cwd=ROOT,
        env=_base_environment(runtime, "success"),
        capture_output=True,
        check=False,
        text=True,
        timeout=60,
    )
    assert completed.returncode == 0, (
        f"relay exited with code {completed.returncode}; "
        f"stdout_bytes={len(completed.stdout)} stderr_bytes={len(completed.stderr)}"
    )
    return json.loads(completed.stdout)


def _wait_for(
    read: Callable[[], Any],
    predicate: Callable[[Any], bool],
    *,
    timeout: float = 20,
) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = read()
        if predicate(value):
            return value
        time.sleep(0.1)
    pytest.fail(f"condition not met within {timeout}s")


def _read_item(runtime: Runtime, pk: str, sk: str) -> dict[str, Any]:
    return runtime.table.get_item(Key={"PK": pk, "SK": sk}, ConsistentRead=True).get("Item", {})


def _wait_delivery(
    runtime: Runtime,
    outbox_id: str,
    delivery_id: str,
    state: str,
    *,
    timeout: float = 20,
) -> dict[str, Any]:
    return _wait_for(
        lambda: _read_item(runtime, f"NOTIFICATION_OUTBOX#{outbox_id}", f"DELIVERY#{delivery_id}"),
        lambda item: item.get("state") == state,
        timeout=timeout,
    )


def _put_membership(
    runtime: Runtime, tenant_id: str, recipient_id: str, created_at: datetime
) -> None:
    runtime.table.put_item(
        Item={
            "PK": f"TENANT#{tenant_id}",
            "SK": f"MEMBER#{recipient_id}",
            "entity_type": "membership",
            "tenant_id": tenant_id,
            "cognito_sub": recipient_id,
            "role": "owner",
            "status": "active",
            "version": 1,
            "created_at": _fixed(created_at),
        }
    )


def _put_direct_delivery(
    runtime: Runtime,
    *,
    tenant_id: str,
    outbox_id: str,
    delivery_id: str,
    event_id: str,
    recipient_id: str,
    email: str,
    now: datetime,
    state: str = "queued",
    attempt_count: int = 0,
    possibly_accepted: bool = False,
    ambiguous_exhausted: bool = False,
) -> None:
    assert delivery_id == _delivery_id(event_id, "opening", recipient_id)
    subject, text, html = "Local alert", "Local alert body", "<p>Local alert body</p>"
    item: dict[str, Any] = {
        "PK": f"NOTIFICATION_OUTBOX#{outbox_id}",
        "SK": f"DELIVERY#{delivery_id}",
        "entity_type": "notification_delivery",
        "relay_schema_version": 1,
        "tenant_id": tenant_id,
        "outbox_id": outbox_id,
        "delivery_id": delivery_id,
        "event_id": event_id,
        "rule_id": "rule_local",
        "kind": "opening",
        "channel": "email",
        "recipient_id": recipient_id,
        "normalized_email": email,
        "membership_snapshot": {"role": "owner", "status": "active", "version": 1},
        "state": state,
        "content": {
            "template_id": "alert-opening/v1",
            "template_version": 1,
            "locale": "pt-BR",
            "subject": subject,
            "text": text,
            "html": html,
            "content_hash": _content_hash("alert-opening/v1", subject, text, html),
        },
        "created_at": _fixed(now),
        "updated_at": _fixed(now),
        "delivery_revision": 1,
        "attempt_count": attempt_count,
    }
    if possibly_accepted:
        item["possibly_accepted"] = True
    if ambiguous_exhausted:
        item["ambiguous_exhausted"] = True
        item["next_attempt_at"] = _fixed(now - timedelta(seconds=1))
    runtime.table.put_item(Item=item)


def _job(
    tenant_id: str,
    outbox_id: str,
    delivery_id: str,
    event_id: str,
    kind: str = "opening",
) -> str:
    return json.dumps(
        {
            "schema_version": 1,
            "message_type": "notification.delivery",
            "tenant_id": tenant_id,
            "outbox_id": outbox_id,
            "delivery_id": delivery_id,
            "event_id": event_id,
            "kind": kind,
            "channel": "email",
        },
        separators=(",", ":"),
    )


def _send_job(
    runtime: Runtime,
    tenant_id: str,
    outbox_id: str,
    delivery_id: str,
    event_id: str,
    kind: str = "opening",
) -> None:
    runtime.sqs.send_message(
        QueueUrl=runtime.jobs_url,
        MessageBody=_job(tenant_id, outbox_id, delivery_id, event_id, kind),
    )


def _seed_opening_and_recovery(runtime: Runtime, now: datetime) -> dict[str, str]:
    tenant_id = "tnt_integration_flow"
    recipient_id = "user_integration_flow"
    event_id = "evt_integration_flow"
    rule_id = "rule_integration_flow"
    opening_outbox = "outbox_integration_opening"
    recovery_outbox = "outbox_integration_recovery"
    email = "integration-recipient@example.test"
    _put_membership(runtime, tenant_id, recipient_id, now - timedelta(hours=1))
    runtime.table.put_item(
        Item={
            "PK": f"TENANT#{tenant_id}",
            "SK": f"NOTIFICATION_PREFERENCE#USER#{recipient_id}",
            "entity_type": "notification_preference",
            "tenant_id": tenant_id,
            "cognito_sub": recipient_id,
            "email_enabled": True,
            "email_address": email,
            "email_verified": True,
            "minimum_severity": "warning",
        }
    )
    runtime.table.put_item(
        Item={
            "PK": f"TENANT#{tenant_id}",
            "SK": f"ALERT_EVENT#{event_id}",
            "entity_type": "alert_event",
            "tenant_id": tenant_id,
            "event_id": event_id,
            "rule_id": rule_id,
            "rule_name": "Local temperature alert",
            "severity": "critical",
            "pond_id": "pond_integration",
            "device_id": "device_integration",
            "metric": "temp_c",
            "operator": ">",
            "threshold": Decimal(30),
            "window_start": _fixed(now - timedelta(minutes=5)),
            "window_end": _fixed(now),
            "last_evaluated_at": _fixed(now),
            "last_evaluation_value": Decimal(31),
        }
    )
    for outbox_id, kind, status, work_kind, dependency in (
        (opening_outbox, "opening", "pending", "INTENT", ""),
        (recovery_outbox, "recovery", "blocked", "DEPENDENCY", opening_outbox),
    ):
        relay_pk, relay_sk = _relay_index(work_kind, tenant_id, outbox_id, now)
        runtime.table.put_item(
            Item={
                "PK": f"TENANT#{tenant_id}",
                "SK": f"NOTIFICATION_OUTBOX#{outbox_id}",
                "entity_type": "notification_outbox",
                "outbox_id": outbox_id,
                "event_id": event_id,
                "tenant_id": tenant_id,
                "rule_id": rule_id,
                "channel": "email",
                "kind": kind,
                "status": status,
                "depends_on_outbox_id": dependency,
                "created_at": _fixed(now),
                "relay_schema_version": 1,
                "expansion_status": "pending",
                "available_at": _fixed(now),
                "relay_work_kind": work_kind,
                "relay_gsi_pk": relay_pk,
                "relay_gsi_sk": relay_sk,
            }
        )
    return {
        "tenant_id": tenant_id,
        "recipient_id": recipient_id,
        "event_id": event_id,
        "opening_outbox": opening_outbox,
        "recovery_outbox": recovery_outbox,
        "opening_delivery": _delivery_id(event_id, "opening", recipient_id),
        "recovery_delivery": _delivery_id(event_id, "recovery", recipient_id),
        "email": email,
    }


def test_multiprocess_happy_path_recovery_dependency_feedback_and_suppression(
    runtime: Runtime,
) -> None:
    # Keep replay time in the past so durable created_at never exceeds the
    # worker's real clock while still advancing the dependency by one minute.
    now = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=5)
    identity = _seed_opening_and_recovery(runtime, now)
    _run_relay(runtime, now + timedelta(seconds=10))
    _run_relay(runtime, now + timedelta(seconds=11))
    worker = _start_worker(runtime)
    try:
        opening = _wait_delivery(
            runtime,
            identity["opening_outbox"],
            identity["opening_delivery"],
            "succeeded",
        )
        assert opening["attempt_count"] == 1

        _run_relay(runtime, now + timedelta(minutes=2))
        _run_relay(runtime, now + timedelta(minutes=2, seconds=1))
        recovery = _wait_delivery(
            runtime,
            identity["recovery_outbox"],
            identity["recovery_delivery"],
            "succeeded",
            timeout=75,
        )
        assert recovery["depends_on_delivery_id"] == identity["opening_delivery"]
        assert recovery["attempt_count"] == 1

        for event_type in ("Bounce", "Complaint"):
            publish_event(
                client=runtime.sqs,
                queue_url=runtime.events_url,
                event=build_ses_event(
                    event_type=event_type,
                    delivery_id=identity["opening_delivery"],
                    attempt_id=opening["last_attempt_id"],
                    provider_message_id=opening["provider_message_id"],
                    event_id=f"evt_feedback_{event_type.lower()}",
                ),
            )
            wanted = "hard_bounced" if event_type == "Bounce" else "complained"
            opening = _wait_for(
                lambda: _read_item(
                    runtime,
                    f"NOTIFICATION_OUTBOX#{identity['opening_outbox']}",
                    f"DELIVERY#{identity['opening_delivery']}",
                ),
                lambda item, wanted=wanted: item.get("provider_outcome") == wanted,
            )

        suppression_key = "EMAIL_IDENTITY#" + hashlib.sha256(identity["email"].encode()).hexdigest()
        suppression = _read_item(runtime, suppression_key, "DELIVERABILITY")
        assert suppression["deliverability"] == "suppressed"
        assert suppression["suppression_reason"] == "complaint"
        assert "normalized_email" not in suppression
    finally:
        if worker.poll() is None:
            _stop_worker(worker)


def test_crash_republication_and_duplicate_job_create_one_attempt(runtime: Runtime) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    tenant, recipient = "tnt_republication", "user_republication"
    outbox, event = "outbox_republication", "evt_republication"
    delivery = _delivery_id(event, "opening", recipient)
    _put_membership(runtime, tenant, recipient, now - timedelta(hours=1))
    _put_direct_delivery(
        runtime,
        tenant_id=tenant,
        outbox_id=outbox,
        delivery_id=delivery,
        event_id=event,
        recipient_id=recipient,
        email="republish@example.test",
        now=now,
        state="pending",
    )
    relay_pk, relay_sk = _relay_index("DELIVERY", tenant, delivery, now)
    runtime.table.update_item(
        Key={"PK": f"NOTIFICATION_OUTBOX#{outbox}", "SK": f"DELIVERY#{delivery}"},
        UpdateExpression=(
            "SET available_at = :available_at, relay_work_kind = :work_kind, "
            "relay_gsi_pk = :relay_pk, relay_gsi_sk = :relay_sk, "
            "relay_lease_owner = :owner, relay_lease_epoch = :epoch, "
            "relay_lease_expires_at = :expired"
        ),
        ExpressionAttributeValues={
            ":available_at": _fixed(now),
            ":work_kind": "DELIVERY",
            ":relay_pk": relay_pk,
            ":relay_sk": relay_sk,
            ":owner": "relay_crashed_after_sqs",
            ":epoch": 1,
            ":expired": _fixed(now - timedelta(seconds=1)),
        },
    )
    # This first message is the confirmed SQS publication made by the crashed
    # relay. DynamoDB intentionally remains pending and indexed.
    _send_job(runtime, tenant, outbox, delivery, event)
    replay = _run_relay(runtime, now + timedelta(seconds=1))
    assert replay["published_jobs"] == 1
    repaired = _read_item(runtime, f"NOTIFICATION_OUTBOX#{outbox}", f"DELIVERY#{delivery}")
    assert repaired["state"] == "queued"
    assert "relay_gsi_pk" not in repaired
    _send_job(runtime, tenant, outbox, delivery, event)
    queued_before_worker = _wait_for(
        lambda: runtime.sqs.get_queue_attributes(
            QueueUrl=runtime.jobs_url,
            AttributeNames=["ApproximateNumberOfMessages"],
        )["Attributes"],
        lambda attrs: int(attrs["ApproximateNumberOfMessages"]) >= 3,
    )
    confirmed_copies = int(queued_before_worker["ApproximateNumberOfMessages"])

    worker = _start_worker(runtime)
    try:
        succeeded = _wait_delivery(runtime, outbox, delivery, "succeeded")
        assert succeeded["attempt_count"] == 1
        _wait_for(
            lambda: runtime.sqs.get_queue_attributes(
                QueueUrl=runtime.jobs_url,
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                ],
            )["Attributes"],
            lambda attrs: (
                attrs["ApproximateNumberOfMessages"] == "0"
                and attrs["ApproximateNumberOfMessagesNotVisible"] == "0"
            ),
        )
        summary = _stop_worker(worker)
        assert summary["messages_received"] >= confirmed_copies
        assert summary["messages_deleted"] >= confirmed_copies
        assert summary["visibility_changed"] == 0
        duplicate = _read_item(runtime, f"NOTIFICATION_OUTBOX#{outbox}", f"DELIVERY#{delivery}")
        assert duplicate["attempt_count"] == 1
    finally:
        if worker.poll() is None:
            _stop_worker(worker)


def test_retry_ambiguous_confirmation_and_no_feedback_unknown(runtime: Runtime) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    tenant, recipient = "tnt_failure_modes", "user_failure_modes"
    _put_membership(runtime, tenant, recipient, now - timedelta(hours=1))

    retry_event = "evt_retry"
    retry = ("outbox_retry", _delivery_id(retry_event, "opening", recipient), retry_event)
    _put_direct_delivery(
        runtime,
        tenant_id=tenant,
        outbox_id=retry[0],
        delivery_id=retry[1],
        event_id=retry[2],
        recipient_id=recipient,
        email="retry@example.test",
        now=now,
    )
    _send_job(runtime, tenant, *retry)
    retry_worker = _start_worker(runtime, "retryable")
    try:
        retried = _wait_delivery(runtime, retry[0], retry[1], "retryable_failed")
        assert retried["attempt_count"] == 1
        assert retried["last_error_category"] == "retryable_service_unavailable"
    finally:
        _stop_worker(retry_worker)

    first_attempt_id = retried["last_attempt_id"]
    runtime.table.update_item(
        Key={"PK": f"NOTIFICATION_OUTBOX#{retry[0]}", "SK": f"DELIVERY#{retry[1]}"},
        UpdateExpression="SET next_attempt_at = :due",
        ConditionExpression="#state = :retryable AND attempt_count = :one",
        ExpressionAttributeNames={"#state": "state"},
        ExpressionAttributeValues={
            ":due": _fixed(datetime.now(UTC) - timedelta(seconds=1)),
            ":retryable": "retryable_failed",
            ":one": 1,
        },
    )
    _send_job(runtime, tenant, *retry)
    retry_success_worker = _start_worker(runtime, "success")
    try:
        succeeded_after_retry = _wait_delivery(runtime, retry[0], retry[1], "succeeded")
        assert succeeded_after_retry["attempt_count"] == 2
        assert succeeded_after_retry["last_attempt_id"] != first_attempt_id
        assert succeeded_after_retry["provider_attempt_id"] == succeeded_after_retry["last_attempt_id"]
        attempts = runtime.table.query(
            KeyConditionExpression="PK = :pk AND begins_with(SK, :attempt)",
            ExpressionAttributeValues={
                ":pk": f"NOTIFICATION_DELIVERY#{retry[1]}",
                ":attempt": "ATTEMPT#",
            },
            ConsistentRead=True,
        )["Items"]
        assert {attempt["attempt_id"] for attempt in attempts} == {
            first_attempt_id,
            succeeded_after_retry["last_attempt_id"],
        }
        assert {attempt["outcome"] for attempt in attempts} == {"retryable", "succeeded"}

        # A terminal duplicate is deleted without starting a third Attempt.
        _send_job(runtime, tenant, *retry)
        _wait_for(
            lambda: runtime.sqs.get_queue_attributes(
                QueueUrl=runtime.jobs_url,
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                ],
            )["Attributes"],
            lambda attrs: (
                attrs["ApproximateNumberOfMessages"] == "0"
                and attrs["ApproximateNumberOfMessagesNotVisible"] == "0"
            ),
            timeout=40,
        )
        retry_summary = _stop_worker(retry_success_worker)
        assert retry_summary["messages_deleted"] >= 2
        assert retry_summary["visibility_changed"] == 0
        assert _read_item(
            runtime,
            f"NOTIFICATION_OUTBOX#{retry[0]}",
            f"DELIVERY#{retry[1]}",
        )["attempt_count"] == 2
    finally:
        if retry_success_worker.poll() is None:
            _stop_worker(retry_success_worker)

    ambiguous_event = "evt_ambiguous"
    ambiguous = (
        "outbox_ambiguous",
        _delivery_id(ambiguous_event, "opening", recipient),
        ambiguous_event,
    )
    _put_direct_delivery(
        runtime,
        tenant_id=tenant,
        outbox_id=ambiguous[0],
        delivery_id=ambiguous[1],
        event_id=ambiguous[2],
        recipient_id=recipient,
        email="ambiguous@example.test",
        now=now,
    )
    _send_job(runtime, tenant, *ambiguous)
    ambiguous_worker = _start_worker(runtime, "ambiguous_timeout")
    try:
        uncertain = _wait_delivery(runtime, ambiguous[0], ambiguous[1], "retryable_failed")
        assert uncertain["possibly_accepted"] is True
        assert uncertain["last_error_category"] == "ambiguous_timeout"
    finally:
        _stop_worker(ambiguous_worker)

    delayed_event = "evt_delayed"
    delayed = (
        "outbox_delayed",
        _delivery_id(delayed_event, "opening", recipient),
        delayed_event,
    )
    _put_direct_delivery(
        runtime,
        tenant_id=tenant,
        outbox_id=delayed[0],
        delivery_id=delayed[1],
        event_id=delayed[2],
        recipient_id=recipient,
        email="delayed@example.test",
        now=now,
    )
    _send_job(runtime, tenant, *delayed)
    delayed_worker = _start_worker(runtime, "ambiguous_timeout")
    try:
        delayed_uncertain = _wait_delivery(
            runtime,
            delayed[0],
            delayed[1],
            "retryable_failed",
        )
        assert delayed_uncertain["possibly_accepted"] is True
        assert delayed_uncertain["attempt_count"] == 1
    finally:
        _stop_worker(delayed_worker)

    unknown_event = "evt_unknown"
    unknown = (
        "outbox_unknown",
        _delivery_id(unknown_event, "opening", recipient),
        unknown_event,
    )
    _put_direct_delivery(
        runtime,
        tenant_id=tenant,
        outbox_id=unknown[0],
        delivery_id=unknown[1],
        event_id=unknown[2],
        recipient_id=recipient,
        email="unknown@example.test",
        now=now,
        state="retryable_failed",
        attempt_count=5,
        possibly_accepted=True,
        ambiguous_exhausted=True,
    )
    _send_job(runtime, tenant, *unknown)
    confirmation_worker = _start_worker(runtime)
    try:
        publish_event(
            client=runtime.sqs,
            queue_url=runtime.events_url,
            event=build_ses_event(
                event_type="Send",
                delivery_id=ambiguous[1],
                attempt_id=uncertain["last_attempt_id"],
                provider_message_id="provider_ambiguous_confirmation",
                event_id="evt_ambiguous_confirmation",
            ),
        )
        publish_event(
            client=runtime.sqs,
            queue_url=runtime.events_url,
            event=build_ses_event(
                event_type="DeliveryDelay",
                delivery_id=delayed[1],
                attempt_id=delayed_uncertain["last_attempt_id"],
                provider_message_id="provider_delayed_confirmation",
                event_id="evt_delayed_confirmation",
            ),
        )
        confirmed = _wait_delivery(runtime, ambiguous[0], ambiguous[1], "succeeded")
        assert confirmed["provider_outcome"] == "accepted"
        delayed_confirmed = _wait_for(
            lambda: _read_item(
                runtime,
                f"NOTIFICATION_OUTBOX#{delayed[0]}",
                f"DELIVERY#{delayed[1]}",
            ),
            lambda item: item.get("provider_outcome") == "delayed",
        )
        assert delayed_confirmed["state"] == "retryable_failed"
        assert delayed_confirmed["attempt_count"] == 1
        runtime.table.update_item(
            Key={
                "PK": f"NOTIFICATION_OUTBOX#{delayed[0]}",
                "SK": f"DELIVERY#{delayed[1]}",
            },
            UpdateExpression="SET next_attempt_at = :due",
            ConditionExpression=(
                "#state = :retryable AND provider_outcome = :delayed "
                "AND attempt_count = :one"
            ),
            ExpressionAttributeNames={"#state": "state"},
            ExpressionAttributeValues={
                ":due": _fixed(datetime.now(UTC) - timedelta(seconds=1)),
                ":retryable": "retryable_failed",
                ":delayed": "delayed",
                ":one": 1,
            },
        )
        # The original SQS copy remains under its ambiguous-send visibility
        # delay. This duplicate makes the already-due durable work visible and
        # proves at-least-once delivery cannot trigger another provider call.
        _send_job(runtime, tenant, *delayed)
        delayed_unknown = _wait_delivery(runtime, delayed[0], delayed[1], "unknown")
        assert delayed_unknown["attempt_count"] == 1
        assert delayed_unknown["provider_outcome"] == "delayed"
        no_feedback = _wait_delivery(runtime, unknown[0], unknown[1], "unknown")
        assert no_feedback["possibly_accepted"] is True
        assert "ambiguous_exhausted" not in no_feedback
    finally:
        _stop_worker(confirmation_worker)


def test_elasticmq_redrives_after_eight_receives_without_automatic_dlq_consumption(
    runtime: Runtime,
) -> None:
    runtime.sqs.send_message(QueueUrl=runtime.events_url, MessageBody="not-json")
    worker = _start_worker(runtime, invalid_visibility="1s")
    try:
        _wait_for(
            lambda: runtime.sqs.get_queue_attributes(
                QueueUrl=runtime.events_dlq_url,
                AttributeNames=["ApproximateNumberOfMessages"],
            )["Attributes"],
            lambda attrs: attrs["ApproximateNumberOfMessages"] == "1",
            timeout=20,
        )
        summary = _stop_worker(worker)
    finally:
        if worker.poll() is None:
            _stop_worker(worker)

    feedback_summary = summary["consumers"]["ses_feedback"]
    assert feedback_summary["messages_received"] == 8
    assert feedback_summary["messages_deleted"] == 0
    assert feedback_summary["visibility_changed"] == 8
    assert feedback_summary["error_categories"]["invalid_feedback"] == 8
    assert summary["feedback_metrics"]["malformed"] == 8
    source = runtime.sqs.get_queue_attributes(
        QueueUrl=runtime.events_url,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]
    assert source == {
        "ApproximateNumberOfMessages": "0",
        "ApproximateNumberOfMessagesNotVisible": "0",
    }

    # Inspect only after the worker exits; its feedback consumer is wired to
    # the source queue and never drains the DLQ.
    dlq = runtime.sqs.receive_message(
        QueueUrl=runtime.events_dlq_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=0,
        VisibilityTimeout=30,
    ).get("Messages", [])
    assert len(dlq) == 1
    assert dlq[0]["Body"] == "not-json"
