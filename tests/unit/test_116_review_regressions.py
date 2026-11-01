"""#116 regressions: the non-blocking hardening items from the P3 PR audits (new file).

Each test names the #116 item it pins. Edits to frozen P3 tests that #116 asks for directly (the
P3-11 hardening, the P3-10 caplog tests and the lifecycle port pin) live in their own files and
are listed in the PR body.
"""

import asyncio
import copy
import io
import json
import logging
import re
import threading
from collections.abc import Coroutine
from datetime import timedelta
from pathlib import Path
from typing import Any, ClassVar

import boto3
import pytest
from botocore.exceptions import ClientError
from fastapi.testclient import TestClient

from limnopulse_api.adapters.dynamodb_ingest_credentials import DynamoIngestCredentialRepository
from limnopulse_api.adapters.sqs_ingestion import SqsIngressQueue
from limnopulse_api.adapters.telemetry_metrics_log import (
    METRICS_LOGGER,
    StructuredLogTelemetryMetrics,
)
from limnopulse_api.core.config import Settings, get_settings
from limnopulse_api.core.errors import (
    ConflictError,
    IngressMessageTooLarge,
    IngressQueueError,
    NotFoundError,
)
from limnopulse_api.domain.ingestion import (
    INGRESS_SCHEMA_ATTRIBUTE_BYTES,
    MAX_INGRESS_MESSAGE_BYTES,
    MAX_VISIBILITY_SECONDS,
    IngressReceiptState,
    QuarantineEntryStatus,
)
from limnopulse_api.domain.integrations import DeviceIntegrationStatus, IntegrationAccountStatus
from limnopulse_api.main import create_app
from limnopulse_api.services.ingress_redrive import redrive_dlq
from limnopulse_api.services.integration_lifecycle import IntegrationLifecycleService
from limnopulse_api.workers import telemetry_ingest
from scripts.admin import telemetry_dlq, telemetry_quarantine
from tests.support.ingestion_contract_kits import ACCOUNT as ACCOUNT_ID
from tests.support.ingestion_contract_kits import (
    AUDIT,
    DEVICE,
    INTEGRATION,
    OTHER_CREDENTIAL,
    OTHER_DEVICE,
    RECEIVED,
    TENANT,
    credential,
    device_integration,
    fenced_entries,
    integration_account,
    store_errors,
)
from tests.support.ingestion_dynamo_fake import IngestionDynamoClient
from tests.support.ingestion_fakes import (
    FakeIngressQueue,
    Faults,
    RecordingTelemetryMetrics,
    lifecycle_subject,
)
from tests.unit.test_dynamodb_ingest_credentials import (
    AUDIT_TABLE as CREDENTIAL_AUDIT_TABLE,
)
from tests.unit.test_dynamodb_ingest_credentials import (
    TABLE as CREDENTIAL_TABLE,
)
from tests.unit.test_dynamodb_ingest_credentials import (
    TransactionConflictOnce,
)
from tests.unit.test_dynamodb_ingest_credentials import (
    fake_subject as credential_subject,
)
from tests.unit.test_dynamodb_integration_resolver import fake_subject as dynamo_lifecycle_subject
from tests.unit.test_dynamodb_quarantine import DynamoQuarantineSubject
from tests.unit.test_ingress_service import (
    IDEMPOTENCY_KEY,
    OTHER_KEY,
    SECRET,
    TOKEN,
    UNAVAILABLE,
    IngestRejected,
    assert_rejected,
    make_batch,
    racing,
    stored_receipt,
)
from tests.unit.test_telemetry_dlq_cli import Harness as DlqHarness
from tests.unit.test_telemetry_quarantine_cli import Harness as QuarantineHarness
from tests.unit.test_telemetry_quarantine_cli import _fenced, _keys

LATER = RECEIVED + timedelta(minutes=5)
ROOT = Path(__file__).resolve().parents[2]
SENTINEL = "SENTINEL-116-must-not-leak"
DLQ_URL = "http://127.0.0.1:9/000000000000/limnopulse-telemetry-ingress-dlq"
QUEUE_URL = "http://127.0.0.1:9/000000000000/limnopulse-telemetry-ingress"


# P3-03: the issue path drops the exception chain ------------------------------------------------


@pytest.mark.asyncio
async def test_issue_failures_carry_no_exception_chain() -> None:
    """#116 P3-03: a cancelled issue transaction holds the META Put (the digest) in its frames,
    so neither a 404, a 409 nor a TransactionConflict 409 may chain or carry the cause."""
    subject = credential_subject()
    with pytest.raises(NotFoundError) as missing:  # the account row does not exist
        await subject.repository.create_ingest_credential(credential(), AUDIT)
    await subject.seed_account(integration_account())
    await subject.repository.create_ingest_credential(credential(), AUDIT)
    with pytest.raises(ConflictError) as duplicate:  # the slot, listing and META Puts collide
        await subject.repository.create_ingest_credential(credential(), AUDIT)
    racing_repository = DynamoIngestCredentialRepository(
        CREDENTIAL_TABLE,
        TransactionConflictOnce(subject.raw),
        clock=lambda: RECEIVED,
        audit_table_name=CREDENTIAL_AUDIT_TABLE,
    )
    with pytest.raises(ConflictError) as raced:
        await racing_repository.create_ingest_credential(
            credential(ingest_credential_id=OTHER_CREDENTIAL, slot=2), AUDIT
        )

    for failure in (missing.value, duplicate.value, raced.value):
        assert failure.__cause__ is None, failure
        assert failure.__context__ is None, failure


# P3-04: lifecycle same-status no-op and ownership on the pre-read -------------------------------

_LIFECYCLE_SUBJECTS = {"fake": lifecycle_subject, "dynamo": dynamo_lifecycle_subject}


@pytest.fixture(params=sorted(_LIFECYCLE_SUBJECTS))
def lifecycle(request: pytest.FixtureRequest) -> Any:
    return _LIFECYCLE_SUBJECTS[request.param]()


async def _account_to(subject: Any, target: IntegrationAccountStatus, version: int) -> Any:
    return await subject.repository.transition_integration_account(
        TENANT, ACCOUNT_ID, target, expected_version=version, updated_at=LATER, audit=AUDIT
    )


async def _integration_to(
    subject: Any, target: DeviceIntegrationStatus, version: int, **kwargs: Any
) -> Any:
    return await subject.repository.transition_device_integration(
        TENANT,
        INTEGRATION,
        target,
        expected_version=version,
        updated_at=LATER,
        audit=AUDIT,
        **kwargs,
    )


@pytest.mark.asyncio
async def test_a_same_status_account_transition_is_a_no_op(lifecycle: Any) -> None:
    """#116 P3-04: active→active writes nothing, bumps no version and audits nothing."""
    await lifecycle.seed_account(integration_account())

    unchanged = await _account_to(lifecycle, IntegrationAccountStatus.ACTIVE, 1)

    assert unchanged == integration_account()
    assert lifecycle.audit_items() == []
    moved = await _account_to(lifecycle, IntegrationAccountStatus.ERROR, 1)  # still version 1
    assert (moved.status, moved.version) == (IntegrationAccountStatus.ERROR, 2)
    assert len(lifecycle.audit_items()) == 1


@pytest.mark.asyncio
async def test_a_same_status_device_integration_transition_is_a_no_op(lifecycle: Any) -> None:
    """#116 P3-04: neither the DeviceIntegration nor its EXTERNAL_ID row moves; no audit item."""
    await lifecycle.seed_device_integration(device_integration())

    unchanged = await _integration_to(lifecycle, DeviceIntegrationStatus.ACTIVE, 1)

    assert unchanged == device_integration()
    assert lifecycle.audit_items() == []
    # The lookup row is fenced on version 1 too, so this proves neither row was bumped.
    moved = await _integration_to(lifecycle, DeviceIntegrationStatus.DECOMMISSIONED, 1)
    assert (moved.status, moved.version) == (DeviceIntegrationStatus.DECOMMISSIONED, 2)
    assert len(lifecycle.audit_items()) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status", list(IntegrationAccountStatus))
async def test_every_same_status_account_transition_is_a_no_op(
    lifecycle: Any, status: IntegrationAccountStatus
) -> None:
    """#116 P3-04: terminal states too (`revoked` → `revoked` used to bump and audit)."""
    seeded = integration_account(status=status)
    await lifecycle.seed_account(seeded)

    first = await _account_to(lifecycle, status, 1)
    again = await _account_to(lifecycle, status, 1)  # still version 1: nothing was bumped

    assert first == again == seeded
    assert lifecycle.audit_items() == []


@pytest.mark.asyncio
@pytest.mark.parametrize("status", list(DeviceIntegrationStatus))
async def test_every_same_status_device_integration_transition_is_a_no_op(
    lifecycle: Any, status: DeviceIntegrationStatus
) -> None:
    seeded = device_integration(status=status)
    await lifecycle.seed_device_integration(seeded)

    first = await _integration_to(lifecycle, status, 1, device_id=DEVICE)
    again = await _integration_to(lifecycle, status, 1)

    assert first == again == seeded
    assert lifecycle.audit_items() == []


@pytest.mark.asyncio
async def test_a_stale_same_status_transition_is_still_a_version_conflict(lifecycle: Any) -> None:
    """#116 P3-04: the no-op never bypasses the version fence."""
    await lifecycle.seed_account(integration_account())
    await lifecycle.seed_device_integration(device_integration())

    with pytest.raises(ConflictError):
        await _account_to(lifecycle, IntegrationAccountStatus.ACTIVE, 2)
    with pytest.raises(ConflictError):
        await _integration_to(lifecycle, DeviceIntegrationStatus.ACTIVE, 2)

    assert lifecycle.audit_items() == []


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [1, 9], ids=["current", "stale"])
@pytest.mark.parametrize(
    "target", [DeviceIntegrationStatus.ERROR, DeviceIntegrationStatus.ACTIVE], ids=["move", "same"]
)
async def test_the_repository_404s_another_devices_integration_before_the_version(
    lifecycle: Any, version: int, target: DeviceIntegrationStatus
) -> None:
    """#116 P3-04: ownership is checked on the consistent pre-read, before the version fence
    and before the same-status no-op, so a wrong Device is 404 even with a stale version and
    never gets another Device's integration back."""
    await lifecycle.seed_device_integration(device_integration())

    with pytest.raises(NotFoundError):
        await _integration_to(lifecycle, target, version, device_id=OTHER_DEVICE)

    assert lifecycle.audit_items() == []
    owned = await _integration_to(lifecycle, DeviceIntegrationStatus.ERROR, 1, device_id=DEVICE)
    assert (owned.status, owned.version) == (DeviceIntegrationStatus.ERROR, 2)


@pytest.mark.asyncio
async def test_the_service_checks_ownership_without_a_tenant_wide_query() -> None:
    """#116 P3-04: the service no longer lists the tenant's DeviceIntegrations."""
    subject = dynamo_lifecycle_subject()
    await subject.seed_device_integration(device_integration())
    service = IntegrationLifecycleService(subject.repository, clock=lambda: LATER)

    with pytest.raises(NotFoundError):
        await service.transition_device_integration(
            TENANT,
            OTHER_DEVICE,
            INTEGRATION,
            DeviceIntegrationStatus.DECOMMISSIONED,
            expected_version=1,
            audit=AUDIT,
        )
    moved = await service.transition_device_integration(
        TENANT, DEVICE, INTEGRATION, DeviceIntegrationStatus.ERROR, expected_version=1, audit=AUDIT
    )

    assert (moved.status, moved.version, moved.updated_at) == ("error", 2, LATER)
    assert "query" not in [call.operation for call in subject.client.calls]
    reads = subject.store.reads
    assert reads and all(kind == "get_item" and consistent for kind, _, consistent in reads)


# P3-05: redrive -------------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("queued", [0, 1], ids=["empty", "with_a_message"])
async def test_the_fake_queue_receive_yields_to_the_event_loop(queued: int) -> None:
    """#116 P3-05: a fake that never suspends lets a busy scan loop defeat `wait_for`."""
    queue = FakeIngressQueue(Faults())
    for _ in range(queued):
        await queue.send_raw("{}")
    ticks = 0

    async def ticker() -> None:
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0)

    task = asyncio.create_task(ticker())
    await asyncio.sleep(0)
    before = ticks
    try:
        await queue.receive(max_messages=1, wait_seconds=0, visibility_seconds=1)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert ticks > before


def _sqs_message(index: int) -> dict[str, Any]:
    return {
        "MessageId": f"m-{index}",
        "ReceiptHandle": f"rh-{index}",
        "Body": f"body-{index}",
        "Attributes": {"SentTimestamp": "1700000000000", "ApproximateReceiveCount": "1"},
    }


class BlockingSqsClient:
    """A boto3 SQS stub whose receive blocks in its worker thread, like a poll still in flight
    when the operator presses Ctrl-C (the thread cannot be stopped)."""

    def __init__(self, count: int) -> None:
        self.entered = threading.Event()
        self.proceed = threading.Event()
        self.pending = [_sqs_message(index) for index in range(count)]
        self.released: list[tuple[str, int]] = []

    def receive_message(self, **kwargs: Any) -> dict[str, Any]:
        self.entered.set()
        assert self.proceed.wait(10), "the test never let the receive finish"
        size = kwargs["MaxNumberOfMessages"]
        batch, self.pending = self.pending[:size], self.pending[size:]
        return {"Messages": batch}

    def change_message_visibility(self, **kwargs: Any) -> dict[str, Any]:
        self.released.append((kwargs["ReceiptHandle"], kwargs["VisibilityTimeout"]))
        return {}

    def send_message(self, **kwargs: Any) -> dict[str, Any]:
        raise AssertionError("nothing may be sent after the cancellation")

    def delete_message(self, **kwargs: Any) -> dict[str, Any]:
        raise AssertionError("nothing may be deleted after the cancellation")


async def _cancel_mid_receive(run: Coroutine[Any, Any, Any], client: BlockingSqsClient) -> None:
    task = asyncio.ensure_future(run)
    assert await asyncio.to_thread(client.entered.wait, 10)
    task.cancel()
    for _ in range(3):  # the cancellation lands while the boto3 call still blocks
        await asyncio.sleep(0)
    client.proceed.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 10)


@pytest.mark.asyncio
async def test_a_cancelled_redrive_releases_the_receive_that_was_in_flight() -> None:
    """#116 P3-05: Ctrl-C mid-receive must not leave the batch hidden for the 300 s hold."""
    client = BlockingSqsClient(3)
    dlq = SqsIngressQueue(client=client, queue_url=DLQ_URL)
    source = FakeIngressQueue(Faults())

    await _cancel_mid_receive(
        redrive_dlq(dlq=dlq, source=source, max_messages=10, apply=True), client
    )

    assert sorted(client.released) == [("rh-0", 0), ("rh-1", 0), ("rh-2", 0)]
    assert source.messages == {}


@pytest.mark.asyncio
async def test_a_cancelled_dlq_sample_releases_the_receive_that_was_in_flight() -> None:
    client = BlockingSqsClient(2)
    dlq = SqsIngressQueue(client=client, queue_url=DLQ_URL)

    await _cancel_mid_receive(
        telemetry_dlq.run(
            ["sample", "--limit", "10"],
            dlq=dlq,
            source=None,
            metrics=RecordingTelemetryMetrics(),
            out=io.StringIO(),
        ),
        client,
    )

    assert sorted(client.released) == [("rh-0", 0), ("rh-1", 0)]


def _fail_second_receive(harness: DlqHarness, error: BaseException) -> None:
    inner_receive = harness.dlq.inner.receive
    calls = 0

    async def flaky(**kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise error
        return await inner_receive(**kwargs)

    harness.dlq.inner.receive = flaky  # type: ignore[method-assign]


@pytest.mark.asyncio
async def test_an_applied_redrive_that_stops_midway_exits_3_with_the_counts_so_far() -> None:
    """#116 P3-05: what already moved is reported (and counted), not lost behind exit 1."""
    harness = DlqHarness()
    await harness.fill([f"body-{index:02d}" for index in range(12)])
    _fail_second_receive(harness, IngressQueueError(SENTINEL, retryable=True))

    code = await harness.run("redrive", "--max", "20", "--apply")

    assert code == telemetry_dlq.EXIT_PARTIAL == 3
    assert harness.summary() == {
        "command": "redrive",
        "apply": True,
        "seen": 10,
        "redriven": 10,
        "failed": 0,
        "error": "queue_unavailable",
    }
    assert harness.metric_total("redriven") == 10
    assert harness.metric_total("failed") == 0
    assert len(harness.source.bodies()) == 10
    assert SENTINEL not in harness.out.getvalue()


@pytest.mark.asyncio
async def test_a_dry_run_that_stops_midway_exits_3_with_the_count_so_far() -> None:
    harness = DlqHarness()
    # Exactly one receive's worth, so `all_visible` (one receive of 10) proves all were released.
    await harness.fill([f"body-{index:02d}" for index in range(10)])
    _fail_second_receive(harness, RuntimeError(SENTINEL))

    code = await harness.run("redrive", "--max", "20")

    assert code == 3
    assert harness.summary() == {
        "command": "redrive",
        "apply": False,
        "seen": 10,
        "redriven": 0,
        "failed": 0,
        "error": "unexpected",
    }
    assert harness.metrics.records == []
    assert harness.source.bodies() == []
    assert len(await harness.all_visible()) == 10  # everything it held was released
    assert SENTINEL not in harness.out.getvalue()


@pytest.mark.asyncio
async def test_a_failure_inside_a_batch_counts_per_message_and_releases_the_rest() -> None:
    """#116 P3-05: a failure on the third message of a batch reports 3 seen and 2 moved (not
    the batch), and the messages it never reached are released, not hidden for the hold."""
    harness = DlqHarness()
    await harness.fill([f"body-{index:02d}" for index in range(10)])
    inner_send = harness.source.send_raw
    sends = 0

    async def failing_third_send(body: str) -> str:
        nonlocal sends
        sends += 1
        if sends == 3:
            raise RuntimeError(SENTINEL)
        return await inner_send(body)

    harness.source.send_raw = failing_third_send  # type: ignore[method-assign]

    code = await harness.run("redrive", "--max", "20", "--apply")

    assert code == 3
    assert harness.summary() == {
        "command": "redrive",
        "apply": True,
        "seen": 3,
        "redriven": 2,
        "failed": 0,
        "error": "unexpected",
    }
    assert harness.metric_total("redriven") == 2
    assert sorted(harness.source.bodies()) == ["body-00", "body-01"]
    assert await harness.all_visible() == [f"body-{index:02d}" for index in range(2, 10)]
    assert SENTINEL not in harness.out.getvalue()


@pytest.mark.asyncio
async def test_a_redrive_cancelled_during_a_send_releases_the_whole_batch() -> None:
    harness = DlqHarness()
    await harness.fill([f"body-{index:02d}" for index in range(5)])
    sending = asyncio.Event()

    async def blocked_send(body: str) -> str:
        sending.set()
        await asyncio.Event().wait()  # the send never returns: Ctrl-C lands here
        raise AssertionError("unreachable")

    harness.source.send_raw = blocked_send  # type: ignore[method-assign]
    task = asyncio.ensure_future(
        redrive_dlq(dlq=harness.dlq, source=harness.source, max_messages=10, apply=True)
    )
    await asyncio.wait_for(sending.wait(), 10)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 10)

    assert await harness.all_visible() == [f"body-{index:02d}" for index in range(5)]


@pytest.mark.asyncio
async def test_a_ctrl_c_after_applied_moves_still_reports_and_counts_them() -> None:
    """#116 P3-05: Ctrl-C after two moves: the summary so far and the metric for the two are
    written before the interrupt propagates, and the rest of the batch is released."""
    harness = DlqHarness()
    await harness.fill([f"body-{index:02d}" for index in range(5)])
    inner_send = harness.source.send_raw
    third = asyncio.Event()
    sends = 0

    async def send_blocking_on_the_third(body: str) -> str:
        nonlocal sends
        sends += 1
        if sends == 3:
            third.set()
            await asyncio.Event().wait()  # Ctrl-C lands here
        return await inner_send(body)

    harness.source.send_raw = send_blocking_on_the_third  # type: ignore[method-assign]
    out = io.StringIO()
    task = asyncio.ensure_future(
        telemetry_dlq.run(
            ["redrive", "--apply"],
            dlq=harness.dlq,
            source=harness.source,
            metrics=harness.metrics,
            out=out,
        )
    )
    await asyncio.wait_for(third.wait(), 10)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 10)

    (summary,) = [json.loads(line) for line in out.getvalue().splitlines()]
    assert summary == {
        "command": "redrive",
        "apply": True,
        "seen": 3,
        "redriven": 2,
        "failed": 0,
        "error": "interrupted",
    }
    assert harness.metric_total("redriven") == 2
    assert await harness.all_visible() == ["body-02", "body-03", "body-04"]


@pytest.mark.asyncio
async def test_a_clean_redrive_summary_has_no_error_key() -> None:
    harness = DlqHarness()
    await harness.fill(["body-00", "body-01"])

    assert await harness.run("redrive", "--apply") == 0
    assert "error" not in harness.summary()


# P3-06: no empty round trip after an exact page ---------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(("rows", "more"), [(3, False), (4, True)], ids=["exact", "one_more"])
async def test_a_page_of_exactly_limit_rows_costs_no_extra_round_trip(
    rows: int, more: bool
) -> None:
    """#116 P3-06: one Query decides whether another page exists."""
    subject = DynamoQuarantineSubject()
    entries = fenced_entries(rows - 1)  # rows - 1 observation rows plus the fenced_message row
    assert len(entries) == rows
    await subject.repository.put_quarantine_entries(entries)

    page = await subject.repository.list_quarantine_entries(TENANT, after_sk=None, limit=3)

    assert len(page.entries) == 3
    assert [kind for kind, _, _ in subject.client.reads] == ["query"]
    assert (page.next_after_sk is not None) is more
    if more:
        rest = await subject.repository.list_quarantine_entries(
            TENANT, after_sk=page.next_after_sk, limit=3
        )
        assert len(rest.entries) == 1 and rest.next_after_sk is None


@pytest.mark.asyncio
async def test_a_filtered_page_whose_remaining_rows_are_filtered_out_ends_the_listing() -> None:
    """#116 P3-06, with the CLI's default `--status open`: rows 0-5 are open, open, dismissed,
    dismissed, open, dismissed. The second Query reads [open, dismissed] and ends the partition,
    so the page of 3 is the last one; no cursor to an empty page."""
    subject = DynamoQuarantineSubject()
    await subject.repository.put_quarantine_entries(fenced_entries(5))
    ordered = (await subject.repository.list_quarantine_entries(TENANT, after_sk=None, limit=10))
    for index in (2, 3, 5):
        await subject.repository.transition_quarantine_entry(
            TENANT,
            ordered.entries[index].entry_key,
            QuarantineEntryStatus.DISMISSED,
            expected_version=1,
            at=RECEIVED,
        )
    before = len(subject.client.reads)

    page = await subject.repository.list_quarantine_entries(
        TENANT, status=QuarantineEntryStatus.OPEN, after_sk=None, limit=3
    )

    assert [entry.entry_key for entry in page.entries] == [
        ordered.entries[index].entry_key for index in (0, 1, 4)
    ]
    assert page.next_after_sk is None
    assert [kind for kind, _, _ in subject.client.reads[before:]] == ["query", "query"]


# P3-07: the "claim lost twice" warning is redacted ----------------------------------------------


@pytest.mark.asyncio
async def test_the_claim_lost_twice_warning_never_carries_the_raw_key_or_a_secret(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """#116 P3-07: the second-claim path (reachable only when the adapter and the service
    disagree on liveness) logs a warning; a mutant that adds the Idempotency-Key must fail."""
    caplog.set_level(logging.DEBUG)
    batch = make_batch()
    stale = stored_receipt(
        batch, state=IngressReceiptState.PENDING, created_at=RECEIVED.replace(year=2025)
    )
    ingress = racing(stale, stale, stale)
    await ingress.seed()

    with pytest.raises(IngestRejected) as exc_info:
        await ingress.admit(batch)

    assert_rejected(exc_info, UNAVAILABLE)
    warnings = [
        record
        for record in caplog.records
        if record.levelno == logging.WARNING
        and record.name == "limnopulse_api.services.ingestion"
        and "claim lost twice" in record.getMessage()
    ]
    (warning,) = warnings
    # The static text only: no key, hash, account, tenant or credential id can ride along.
    assert (warning.getMessage(), warning.args, warning.exc_info) == (
        "ingest receipt claim lost twice",
        (),
        None,
    )
    for secret in (IDEMPOTENCY_KEY, OTHER_KEY, TOKEN, SECRET, stale.key_sha256):
        assert secret not in caplog.text


# P3-09: operator CLIs --------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("error", store_errors(), ids=["client_error", "endpoint_error"])
async def test_dismiss_maps_a_store_failure_to_failed_store_unavailable(error: Exception) -> None:
    """#116 P3-09: like replay, a botocore error on one row is `failed/store_unavailable` and
    the run goes on (exit 3), instead of aborting with exit 1."""
    harness = QuarantineHarness()
    entries, _ = _fenced(2)
    await harness.seed(entries)
    first, second = _keys(entries)
    harness.faults.fail_next("transition_quarantine_entry", error)

    code = await harness.run("dismiss", "--tenant", TENANT, "--entry", first, second, "--apply")

    assert code == telemetry_quarantine.EXIT_PARTIAL == 3
    assert harness.entry_lines() == [
        {"entry_key": first, "result": "failed", "reason": "store_unavailable"},
        {"entry_key": second, "result": "dismissed"},
    ]
    assert harness.summary() == {
        "command": "dismiss",
        "apply": True,
        "dismissed": 1,
        "would_dismiss": 0,
        "skipped": 0,
        "failed": 1,
    }
    statuses = [harness.row(entry).status for entry in entries]
    assert statuses == [QuarantineEntryStatus.OPEN, QuarantineEntryStatus.DISMISSED]


@pytest.mark.asyncio
async def test_dismiss_maps_only_store_errors_an_unexpected_error_still_aborts() -> None:
    harness = QuarantineHarness()
    entries, _ = _fenced(2)
    await harness.seed(entries)
    first, second = _keys(entries)
    harness.faults.fail_next("transition_quarantine_entry", RuntimeError(SENTINEL))

    code = await harness.run("dismiss", "--tenant", TENANT, "--entry", first, second, "--apply")

    assert code == telemetry_quarantine.EXIT_ERROR == 1
    assert harness.lines()[-1] == {"error": "unexpected"}
    assert SENTINEL not in harness.out.getvalue()
    statuses = [harness.row(entry).status for entry in entries]
    assert statuses == [QuarantineEntryStatus.OPEN, QuarantineEntryStatus.OPEN]


@pytest.mark.asyncio
async def test_list_defaults_to_a_page_of_50() -> None:
    """#116 P3-09 test gap: the `--limit` default."""
    harness = QuarantineHarness()
    await harness.seed(fenced_entries(50))  # 51 open rows

    code = await harness.run("list", "--tenant", TENANT)

    assert code == 0
    assert len(harness.entry_lines()) == 50
    summary = harness.summary()
    assert summary["count"] == 50 and summary["next_after"] is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("argv", [("sample",), ("redrive", "--apply")])
async def test_the_dlq_cli_reports_an_unexpected_error_as_a_category_only(argv: tuple) -> None:
    """#116 P3-09 test gap: the DLQ CLI's catch-all branch."""
    harness = DlqHarness()
    await harness.fill([f"body-{index}" for index in range(3)])

    async def broken(**kwargs: Any) -> Any:
        raise RuntimeError(SENTINEL)

    harness.dlq.inner.receive = broken  # type: ignore[method-assign]

    code = await harness.run(*argv)

    assert code == telemetry_dlq.EXIT_ERROR == 1
    assert harness.lines() == [{"error": "unexpected"}]
    assert SENTINEL not in harness.out.getvalue()


# P3-00: fake fidelity ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_fake_send_raw_applies_the_d6_size_guard() -> None:
    """#116 P3-00: `send_raw` counts UTF-8 bytes plus the schema attribute, like the adapter."""
    queue = FakeIngressQueue(Faults())
    limit = MAX_INGRESS_MESSAGE_BYTES - INGRESS_SCHEMA_ATTRIBUTE_BYTES

    await queue.send_raw("x" * limit)
    with pytest.raises(IngressMessageTooLarge):
        await queue.send_raw("x" * (limit + 1))
    with pytest.raises(IngressMessageTooLarge):
        await queue.send_raw("é" * (limit // 2 + 1))  # fewer characters than bytes

    assert len(queue.messages) == 1


@pytest.mark.asyncio
async def test_the_fake_queue_rejects_what_elasticmq_lets_through() -> None:
    """#116 P3-00: ElasticMQ accepts a visibility change on a message that is not in flight and
    a visibility of 43201 s; SQS refuses both, and so does the unit fake."""
    now = [0.0]
    queue = FakeIngressQueue(Faults(), clock=lambda: now[0])
    await queue.send_raw("{}")
    (message,) = await queue.receive(max_messages=1, wait_seconds=0, visibility_seconds=30)

    with pytest.raises(IngressQueueError):
        await queue.change_visibility(message, MAX_VISIBILITY_SECONDS + 1)
    now[0] += 31  # the visibility lapsed: the message is no longer in flight
    with pytest.raises(IngressQueueError):
        await queue.change_visibility(message, 0)


_TABLE = "LimnopulseDomain"
_KEY = {"PK": {"S": "P#1"}, "SK": {"S": "S#1"}}
_X = {":v": {"S": "x"}}
# Each case names an unaliased reserved word somewhere DynamoDB parses an expression. The
# DynamoDB Local twin (`tests/ingestion/test_116_review_regressions_dynamodb.py`) replays them.
_RESERVED_BODIES: dict[str, tuple[str, dict[str, Any]]] = {
    "update_set": ("update_item", {"Key": _KEY, "UpdateExpression": "SET state = :v"}),
    "update_remove": (
        "update_item",
        {
            "Key": _KEY,
            "UpdateExpression": "SET #s = :v REMOVE data",
            "ExpressionAttributeNames": {"#s": "status"},
        },
    ),
    "condition": (
        "put_item",
        {
            "Item": {**_KEY, "status": {"S": "x"}},
            "ConditionExpression": "attribute_not_exists(PK) OR status = :v",
        },
    ),
    "mixed_case": ("update_item", {"Key": _KEY, "UpdateExpression": "SET Name = :v"}),
    "function_argument": (
        "put_item",
        {"Item": {**_KEY}, "ConditionExpression": "attribute_exists(status) OR PK = :v"},
    ),
    "path_element": (
        "update_item",
        {
            "Key": _KEY,
            "UpdateExpression": "SET #d.state = :v",
            "ExpressionAttributeNames": {"#d": "details"},
        },
    ),
    "delete_condition": (
        "delete_item",
        {"Key": _KEY, "ConditionExpression": "attribute_not_exists(PK) OR type = :v"},
    ),
    **{
        f"word_{word}": ("update_item", {"Key": _KEY, "UpdateExpression": f"SET {word} = :v"})
        for word in ["abort", "count", "comment", "date", "hash", "owner", "region", "role", "timestamp", "token", "url", "user", "value", "zone"]
    },
}
_TRANSACT_BODIES = {
    "transact_put": (
        "Put",
        {"Item": {**_KEY}, "ConditionExpression": "attribute_not_exists(PK) OR role = :v"},
    ),
    "transact_update": ("Update", {"Key": _KEY, "UpdateExpression": "SET owner = :v"}),
    "transact_delete": ("Delete", {"Key": _KEY, "ConditionExpression": "region = :v"}),
    "transact_condition_check": (
        "ConditionCheck",
        {"Key": _KEY, "ConditionExpression": "token = :v"},
    ),
}
RESERVED_CASES = sorted([*_RESERVED_BODIES, "query_filter", *_TRANSACT_BODIES])


def reserved_request(case: str, table: str) -> tuple[str, dict[str, Any]]:
    """`(operation, kwargs)` for one case against `table`, complete and otherwise valid."""
    if case == "query_filter":
        return "query", {
            "TableName": table,
            "KeyConditionExpression": "PK = :pk AND begins_with(SK, :sk_prefix)",
            "FilterExpression": "source = :v",
            "ExpressionAttributeValues": {":pk": {"S": "P#1"}, ":sk_prefix": {"S": "S#"}, **_X},
        }
    if case in _TRANSACT_BODIES:
        action, body = _TRANSACT_BODIES[case]
        item = {"TableName": table, "ExpressionAttributeValues": _X, **body}
        return "transact_write_items", {"TransactItems": [{action: item}]}
    operation, body = _RESERVED_BODIES[case]
    return operation, {"TableName": table, "ExpressionAttributeValues": _X, **body}


@pytest.mark.parametrize("case", RESERVED_CASES)
def test_the_dynamo_fake_rejects_an_unaliased_reserved_word(case: str) -> None:
    """#116 P3-00: like DynamoDB, a reserved word (`state`, `status`, ...) needs a `#` alias, in
    every expression of every write and of a Query, transaction bodies included."""
    operation, request = reserved_request(case, _TABLE)
    client = IngestionDynamoClient()

    with pytest.raises(ClientError) as exc_info:
        getattr(client, operation)(**request)

    assert exc_info.value.response["Error"]["Code"] == "ValidationException"
    assert "reserved keyword" in exc_info.value.response["Error"]["Message"]
    assert client.items == {}


def test_the_dynamo_fake_accepts_aliases_functions_and_syntax_keywords() -> None:
    client = IngestionDynamoClient()
    client.put_item(
        TableName=_TABLE,
        Item={**_KEY, "status": {"S": "open"}, "state": {"S": "pending"}},
        ConditionExpression="attribute_not_exists(PK) AND attribute_not_exists(#state)",
        ExpressionAttributeNames={"#state": "state"},
    )
    client.update_item(
        TableName=_TABLE,
        Key=_KEY,
        UpdateExpression="SET #state = :next, replayed_ingress_id = :id REMOVE #status",
        ConditionExpression="attribute_exists(PK) AND #state IN (:pending, :next)",
        ExpressionAttributeNames={"#state": "state", "#status": "status"},
        ExpressionAttributeValues={
            ":next": {"S": "enqueued"},
            ":pending": {"S": "pending"},
            ":id": {"S": "ing_1"},
        },
    )
    # `:status`/`:state` placeholders, and names that merely contain a reserved word.
    client.update_item(
        TableName=_TABLE,
        Key=_KEY,
        UpdateExpression="SET #status = :status, source_type = :state, data_hash = :state",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={":status": {"S": "open"}, ":state": {"S": "x"}},
    )
    response = client.query(
        TableName=_TABLE,
        KeyConditionExpression="PK = :pk AND begins_with(SK, :sk_prefix)",
        ExpressionAttributeValues={":pk": {"S": "P#1"}, ":sk_prefix": {"S": "S#"}},
    )

    assert len(response["Items"]) == 1
    assert client.items[("P#1", "S#1")]["state"] == "enqueued"
    assert client.items[("P#1", "S#1")]["source_type"] == "x"


def _report(
    *, outcome: str, wasxfail: str | None = None, longrepr: Any = ("tests/ingestion/x.py", 1, "r")
) -> pytest.TestReport:
    report = pytest.TestReport(
        nodeid="tests/ingestion/test_x.py::test_x",
        location=("tests/ingestion/test_x.py", 1, "test_x"),
        keywords={},
        outcome=outcome,  # type: ignore[arg-type]
        longrepr=None if outcome == "passed" else longrepr,
        when="call",
    )
    if wasxfail is not None:
        report.wasxfail = wasxfail
    return report


def _guarded(report: pytest.TestReport) -> pytest.TestReport:
    from tests.ingestion import conftest as ingestion_conftest

    item = type("Item", (), {"path": ingestion_conftest._HERE / "test_x.py"})()
    hook = ingestion_conftest.pytest_runtest_makereport(item, None)
    next(hook)
    with pytest.raises(StopIteration) as stop:
        hook.send(report)
    return stop.value.value


@pytest.mark.parametrize(
    ("wasxfail", "longrepr"),
    [
        (None, ("tests/ingestion/x.py", 1, "r")),
        ("known flake", ("tests/ingestion/x.py", 1, "r")),
        ("known flake", "AssertionError: an exception repr, as a real xfail carries"),
    ],
    ids=["skip", "xfail", "xfail_exception_repr"],
)
def test_the_ingestion_no_skip_guard_fails_an_xfail_too(
    monkeypatch: pytest.MonkeyPatch, wasxfail: str | None, longrepr: Any
) -> None:
    """#116 P3-00: under the gate an xfail would hide a failing test as silently as a skip."""
    monkeypatch.setenv("RUN_INGESTION_INTEGRATION", "1")

    guarded = _guarded(_report(outcome="skipped", wasxfail=wasxfail, longrepr=longrepr))

    assert guarded.outcome == "failed"
    assert not hasattr(guarded, "wasxfail")  # a plain failure, never reported as "xfailed"
    assert "must not skip" in str(guarded.longrepr)
    if wasxfail is not None:
        assert wasxfail in str(guarded.longrepr)


def test_the_ingestion_no_skip_guard_leaves_reports_alone_without_the_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("RUN_INGESTION_INTEGRATION", raising=False)

    assert _guarded(_report(outcome="skipped", wasxfail="known flake")).outcome == "skipped"
    assert _guarded(_report(outcome="skipped")).outcome == "skipped"


def _ingestion_gate_section() -> str:
    text = (ROOT / "docs/verification.md").read_text(encoding="utf-8")
    return " ".join(text.split("## Ingestion integration gate", 1)[1].split("\n## ", 1)[0].split())


def test_verification_documents_the_elasticmq_gaps_the_unit_fake_covers() -> None:
    """#116 P3-00: the emulator is more lenient than SQS on these two, so the fake carries them."""
    section = _ingestion_gate_section()
    assert re.search(r"not in flight", section)
    assert "43201" in section and "43200" in section
    assert re.search(r"`tests/support/ingestion_fakes\.py`", section)


def test_the_dynamodb_local_bind_address_is_documented() -> None:
    """#116 P3-00: DynamoDB Local has no bind flag, so the runner listens on every interface."""
    section = _ingestion_gate_section()
    assert re.search(r"DynamoDB Local.{0,300}?\b0\.0\.0\.0\b.{0,300}?bind", section)
    runner = (ROOT / "scripts/dev/run_local_dynamodb.sh").read_text(encoding="utf-8")
    comments = " ".join(line for line in runner.splitlines() if line.lstrip().startswith("#"))
    assert re.search(r"\b0\.0\.0\.0\b", comments)
    assert re.search(r"no bind", comments)


def test_verification_says_the_ingestion_guard_fails_an_xfail_too() -> None:
    section = _ingestion_gate_section()
    assert re.search(r"skip or `xfail`.{0,80}fail", section)


# P3-10: metrics logging, API SQS budget, shared kwargs, Influx close -----------------------------


@pytest.fixture
def metrics_logger(monkeypatch: pytest.MonkeyPatch) -> logging.Logger:
    """The process-global metrics logger, restored after the test."""
    logger = logging.getLogger(METRICS_LOGGER)
    monkeypatch.setattr(logger, "level", logging.NOTSET)
    monkeypatch.setattr(logger, "handlers", [])
    monkeypatch.setattr(logger, "propagate", True)
    return logger


class _Redis:
    async def aclose(self, close_connection_pool: Any = None) -> None:
        return None


class _ApiInflux:
    def __init__(self, **kwargs: Any) -> None:
        pass

    def query_api(self) -> object:
        return object()

    def write_api(self, *args: Any, **kwargs: Any) -> object:
        return object()

    def close(self) -> None:
        pass


def _ingest_app(monkeypatch: pytest.MonkeyPatch, clients: dict[str, dict[str, Any]]) -> Any:
    def client(service_name: str, **kwargs: Any) -> object:
        clients[service_name] = kwargs
        return object()

    for name in ("API_V2_ENABLED", "INGEST_V2_ENABLED", "TELEMETRY_V2_WRITE_ENABLED"):
        monkeypatch.delenv(name, raising=False)
    get_settings.cache_clear()
    monkeypatch.setattr("limnopulse_api.main.boto3.client", client)
    monkeypatch.setattr("limnopulse_api.main.redis.from_url", lambda url: _Redis())
    monkeypatch.setattr("limnopulse_api.main.InfluxDBClient", _ApiInflux)
    return create_app(
        Settings(
            _env_file=None,
            app_env="test",
            auth_mode="dev",
            api_v2_enabled=True,
            telemetry_v2_write_enabled=True,
            ingest_v2_enabled=True,
            sqs_telemetry_ingress_queue_url=QUEUE_URL,
        )
    )


class _Counting(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        if record.name == METRICS_LOGGER:
            self.lines.append(record.getMessage())


def test_api_metric_lines_are_emitted_once_even_with_a_root_handler(
    monkeypatch: pytest.MonkeyPatch, metrics_logger: logging.Logger
) -> None:
    """#116 P3-10: a root handler (custom uvicorn log config, gunicorn, one process for API and
    worker) must not print every metric line a second time."""
    root = _Counting()
    logging.getLogger().addHandler(root)
    try:
        with TestClient(_ingest_app(monkeypatch, {})) as client:
            own = _Counting()
            metrics_logger.addHandler(own)
            client.app.state.telemetry_metrics.increment(
                "ingest_requests_total", outcome="accepted"
            )
    finally:
        logging.getLogger().removeHandler(root)

    assert metrics_logger.propagate is False
    assert len(own.lines) == 1
    assert root.lines == []


def test_the_worker_metric_lines_still_reach_the_root_handler(
    metrics_logger: logging.Logger,
) -> None:
    """#116 P3-10: only the API's `_enable_metrics_logging` stops propagation; the worker logs
    through `basicConfig`'s root handler, so the adapter itself must leave it on."""
    root = _Counting()
    logging.getLogger().addHandler(root)
    metrics_logger.setLevel(logging.INFO)
    try:
        StructuredLogTelemetryMetrics().increment("ingest_requests_total", outcome="accepted")
    finally:
        logging.getLogger().removeHandler(root)

    assert metrics_logger.propagate is True
    assert len(root.lines) == 1


def test_api_sqs_admission_worst_case_is_two_attempts_within_30_seconds(
    monkeypatch: pytest.MonkeyPatch, metrics_logger: logging.Logger
) -> None:
    """#116 P3-10: botocore's `max_attempts` counts retries, so the old 3 meant 4 × 15 s = 60 s;
    the budget is read from a real client, never from that convention."""
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    clients: dict[str, dict[str, Any]] = {}
    with TestClient(_ingest_app(monkeypatch, clients)):
        pass
    config = clients["sqs"]["config"]
    # `boto3.client` is patched above, so a fresh session builds the real client; from a copy,
    # because botocore rewrites the `retries` dict of the shared module-level config in place.
    real = boto3.session.Session().client(
        "sqs", region_name="us-east-2", config=copy.deepcopy(config)
    )

    attempts = real.meta.config.retries["total_max_attempts"]

    assert attempts == 2
    assert attempts * (config.connect_timeout + config.read_timeout) <= 30


def test_the_api_and_the_worker_share_one_client_kwargs_helper() -> None:
    """#116 P3-10: `main` and the worker compose their boto3 clients through one helper."""
    from limnopulse_api import main
    from limnopulse_api.adapters import aws_clients

    assert not hasattr(main, "_dynamodb_client_kwargs")
    for helper in (aws_clients.dynamodb_client_kwargs, aws_clients.sqs_client_kwargs):
        assert helper.__module__ == "limnopulse_api.adapters.aws_clients"  # defined, not re-exported
    assert main.dynamodb_client_kwargs is aws_clients.dynamodb_client_kwargs
    assert telemetry_ingest.dynamodb_client_kwargs is aws_clients.dynamodb_client_kwargs
    assert telemetry_ingest.sqs_client_kwargs is aws_clients.sqs_client_kwargs
    assert main.sqs_client_kwargs is aws_clients.sqs_client_kwargs


@pytest.mark.parametrize(
    ("app_env", "endpoint", "expected"),
    [
        ("local", "http://127.0.0.1:8001", {"endpoint_url": "http://127.0.0.1:8001", "local": True}),
        ("test", "http://127.0.0.1:8001", {"endpoint_url": "http://127.0.0.1:8001", "local": True}),
        ("production", "http://127.0.0.1:8001", {"endpoint_url": "http://127.0.0.1:8001"}),
        ("local", None, {}),
        ("production", None, {}),
    ],
    ids=["local_endpoint", "test_endpoint", "deployed_endpoint", "local_aws", "deployed_aws"],
)
@pytest.mark.parametrize("service", ["dynamodb", "sqs"])
def test_the_shared_helper_adds_dummy_credentials_only_for_a_local_endpoint(
    service: str, app_env: str, endpoint: str | None, expected: dict[str, Any]
) -> None:
    from limnopulse_api.adapters import aws_clients

    settings = Settings.model_construct(
        aws_region="us-east-2",
        app_env=app_env,
        dynamodb_endpoint_url=endpoint if service == "dynamodb" else "http://other.invalid",
        sqs_endpoint_url=endpoint if service == "sqs" else "http://other.invalid",
    )
    helper = getattr(aws_clients, f"{service}_client_kwargs")
    wanted: dict[str, str] = {"region_name": "us-east-2"}
    if "endpoint_url" in expected:
        wanted["endpoint_url"] = expected["endpoint_url"]
    if expected.get("local"):
        wanted |= {"aws_access_key_id": "local", "aws_secret_access_key": "local"}

    assert helper(settings) == wanted


class _WorkerInflux:
    instances: ClassVar[list["_WorkerInflux"]] = []

    def __init__(self, **kwargs: Any) -> None:
        self.closed = 0
        _WorkerInflux.instances.append(self)

    def write_api(self, *args: Any, **kwargs: Any) -> object:
        return object()

    def close(self) -> None:
        self.closed += 1


def _still_open() -> None:
    assert [client.closed for client in _WorkerInflux.instances] == [0], "closed before the run"


class _StoppedLoop:
    async def run(self, stop: asyncio.Event) -> None:
        _still_open()
        stop.set()  # stands in for SIGTERM: only a stop exits 0 (ADR-020)


class _CrashedLoop:
    async def run(self, stop: asyncio.Event) -> None:
        _still_open()
        raise RuntimeError("crash")


class _CancelledLoop:
    async def run(self, stop: asyncio.Event) -> None:
        _still_open()
        raise asyncio.CancelledError


@pytest.mark.parametrize("ending", ["stop", "crash", "cancelled", "composition_failure"])
def test_the_worker_closes_its_influx_client_on_every_exit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, ending: str
) -> None:
    """#116 P3-10: the worker never closed `InfluxDBClient`. The seam: `main()` hands
    `build_worker` an `influx_client_factory` that creates the module's `InfluxDBClient` and
    closes each client it created when the run ends, however it ends."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("TELEMETRY_V2_WRITE_ENABLED", "true")
    monkeypatch.setenv("SQS_TELEMETRY_INGRESS_QUEUE_URL", QUEUE_URL)
    monkeypatch.setattr(telemetry_ingest, "InfluxDBClient", _WorkerInflux)
    _WorkerInflux.instances = []

    def build(settings: Settings, *, influx_client_factory: Any, **kwargs: Any) -> Any:
        influx_client_factory(url="http://influx.invalid", token="t", org="o")
        if ending == "composition_failure":
            raise RuntimeError("composition")
        return {"stop": _StoppedLoop, "crash": _CrashedLoop, "cancelled": _CancelledLoop}[ending]()

    monkeypatch.setattr(telemetry_ingest, "build_worker", build)

    if ending == "cancelled":
        with pytest.raises(asyncio.CancelledError):
            telemetry_ingest.main()
    else:
        assert telemetry_ingest.main() == (0 if ending == "stop" else 1)
    assert [client.closed for client in _WorkerInflux.instances] == [1]


def test_the_stop_grace_comment_names_the_current_grace() -> None:
    """#116 P3-10: the P3-10 regression comment said 75 s; the grace is 90 s."""
    source = (ROOT / "tests/unit/test_p3_10_review_regressions.py").read_text(encoding="utf-8")

    assert "75 s" not in source
    assert "90 s stop grace" in source
