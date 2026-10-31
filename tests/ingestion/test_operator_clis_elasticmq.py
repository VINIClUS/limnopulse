"""P3-09 on ElasticMQ and DynamoDB Local: the operator CLIs end to end through `main`.

- quarantine → `replay --apply` → the message on the ingress queue equals the expected replay
  envelope (D10), and the rows are `replayed` with that id;
- `telemetry_dlq.py redrive --apply` moves N messages and preserves their bodies byte for byte; a
  dry run and `sample` leave every message on the DLQ.

Skips unless ``RUN_INGESTION_INTEGRATION=1`` (``make verify-ingestion``).
"""

from __future__ import annotations

import asyncio
import io
import json
import threading
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from limnopulse_api.adapters.dynamodb_quarantine import DynamoQuarantineRepository
from limnopulse_api.core.config import Settings
from limnopulse_api.domain.ids import replay_ingress_id
from limnopulse_api.domain.ingestion import (
    INGRESS_ENVELOPE_SCHEMA,
    IngressEnvelope,
    IngressFence,
    IngressFenceReason,
    NormalizationVersions,
    QuarantineEntry,
    QuarantineEntryKind,
    QuarantineEntryStatus,
    build_quarantine_entries,
)
from limnopulse_api.domain.telemetry_v2 import SourceObservation
from scripts.admin import telemetry_dlq, telemetry_quarantine
from tests.support.ingestion_contract_kits import (
    INGRESS,
    OBSERVED,
    RECEIVED,
    TENANT,
    envelope,
    source_observation,
)
from tests.support.ingestion_fakes import RecordingTelemetryMetrics
from tests.support.ingestion_harness import (
    IngestionQueuePair,
    IngestionTables,
    dynamodb_client,
    sqs_client,
)

QueueFactory = Callable[..., IngestionQueuePair]
CLI_TIMEOUT_SECONDS = 120


def _bounded(main: Callable[..., int], argv: list[str], **kwargs: Any) -> int:
    """Runs a CLI `main` in a daemon thread, so a runaway scan fails the gate instead of hanging."""
    result: list[int] = []
    errors: list[BaseException] = []

    def target() -> None:
        try:
            result.append(main(argv, **kwargs))
        except BaseException as error:  # noqa: BLE001 - re-raised in the test thread
            errors.append(error)

    worker = threading.Thread(target=target, daemon=True)
    worker.start()
    worker.join(CLI_TIMEOUT_SECONDS)
    if worker.is_alive():
        raise AssertionError(f"the CLI did not finish within {CLI_TIMEOUT_SECONDS}s")
    if errors:
        raise errors[0]
    return result[0]


PINNED_VERSIONS = NormalizationVersions(
    catalog_version="catalog@pinned",
    unit_mapping_version="units@pinned",
    quality_policy_version="quality@pinned",
)
DLQ_BODIES = [
    envelope().to_json(),
    "not json, redriven as-is \U0001f600",
    '{ "spacing" : "kept" ,\n "schema":"unknown@9" }',
    envelope(ingress_id="ing_" + "9" * 32, connector_version="fw-2").to_json(),
]


def _entries() -> list[QuarantineEntry]:
    message = envelope(
        connector_version="fw-1.2",
        mapping_versions=PINNED_VERSIONS,
        observations=[
            source_observation(observed_at=OBSERVED + timedelta(seconds=index), sequence=index)
            for index in range(3)
        ],
    )
    rows = build_quarantine_entries(
        message,
        fence=IngressFence(
            reason=IngressFenceReason.INTEGRATION_DECOMMISSIONED, account_verified=True
        ),
        now=datetime.now(UTC),
    )
    return [row for row in rows if row.kind == QuarantineEntryKind.FENCED_OBSERVATION]


def _expected_envelope(entries: Sequence[QuarantineEntry]) -> IngressEnvelope:
    first = entries[0]
    ordered = sorted(entries, key=lambda entry: entry.entry_key)
    return IngressEnvelope.model_validate(
        {
            "schema": INGRESS_ENVELOPE_SCHEMA,
            "ingress_id": replay_ingress_id([entry.entry_key for entry in entries]),
            "replay_of": first.ingress_id,
            "tenant_id": first.tenant_id,
            "integration_account_id": first.integration_account_id,
            "provider_key": first.provider_key,
            "external_identity_hash": first.external_identity_hash,
            "device_integration_id": first.device_integration_id,
            "device_id": first.device_id,
            "source_type": first.source_type,
            "connector_version": first.connector_version,
            "received_at": first.received_at,
            "replay": True,
            "mapping_versions": first.mapping_versions,
            "observations": [
                SourceObservation.model_validate_json(entry.source_observation_json or "")
                for entry in ordered
            ],
        }
    )


def _drain(sqs: Any, url: str, *, expected: int, attempts: int = 20) -> list[dict[str, Any]]:
    """Receive until `expected` messages arrived; an `expected=0` check polls `attempts` times."""
    messages: list[dict[str, Any]] = []
    for _ in range(attempts):
        if expected and len(messages) >= expected:
            break
        response = sqs.receive_message(
            QueueUrl=url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
            VisibilityTimeout=30,
            MessageAttributeNames=["All"],
        )
        messages.extend(response.get("Messages") or [])
    return messages


def _settings(tables: IngestionTables | None, pair: IngestionQueuePair) -> Settings:
    values: dict[str, Any] = {
        "sqs_telemetry_ingress_queue_url": pair.source_url,
        "sqs_telemetry_ingress_dlq_url": pair.dlq_url,
    }
    if tables is not None:
        values["dynamodb_domain_table"] = tables.domain
    return Settings(_env_file=None, **values)


def test_quarantine_replay_puts_the_expected_envelope_on_the_ingress_queue(
    ingestion_tables: IngestionTables, ingestion_queue_pair: QueueFactory
) -> None:
    pair = ingestion_queue_pair(visibility_seconds=30)
    dynamodb, sqs = dynamodb_client(), sqs_client()
    repository = DynamoQuarantineRepository(ingestion_tables.domain, dynamodb)
    entries = _entries()
    asyncio.run(repository.put_quarantine_entries(entries))
    keys = [entry.entry_key for entry in entries]
    metrics = RecordingTelemetryMetrics()
    out = io.StringIO()

    dry_code = _bounded(
        telemetry_quarantine.main,
        ["replay", "--tenant", TENANT, "--entry", *keys],
        settings=_settings(ingestion_tables, pair),
        dynamodb_client=dynamodb,
        sqs_client=sqs,
        metrics=metrics,
        out=io.StringIO(),
        err=io.StringIO(),
    )
    assert dry_code == 0
    assert _drain(sqs, pair.source_url, expected=0, attempts=2) == []

    code = _bounded(
        telemetry_quarantine.main,
        ["replay", "--tenant", TENANT, "--entry", *keys, "--apply"],
        settings=_settings(ingestion_tables, pair),
        dynamodb_client=dynamodb,
        sqs_client=sqs,
        metrics=metrics,
        out=out,
        err=io.StringIO(),
    )

    assert code == 0, out.getvalue()
    (message,) = _drain(sqs, pair.source_url, expected=1)
    sent = IngressEnvelope.model_validate_json(message["Body"])
    assert sent == _expected_envelope(entries)
    assert sent.ingress_id != INGRESS and sent.replay_of == INGRESS and sent.replay is True
    assert sent.received_at == RECEIVED
    assert json.loads(message["Body"])["mapping_versions"] == PINNED_VERSIONS.model_dump()
    assert message["MessageAttributes"]["schema"]["StringValue"] == INGRESS_ENVELOPE_SCHEMA
    for entry in entries:
        row = asyncio.run(repository.get_quarantine_entry(TENANT, entry.entry_key))
        assert row is not None
        assert row.status == QuarantineEntryStatus.REPLAYED
        assert row.replayed_ingress_id == sent.ingress_id
    replayed = sum(
        record["value"] for record in metrics.records if record["labels"] == {"result": "replayed"}
    )
    assert replayed == 3

    # A rerun skips the now-replayed rows and sends nothing more.
    rerun = _bounded(
        telemetry_quarantine.main,
        ["replay", "--tenant", TENANT, "--entry", *keys, "--apply"],
        settings=_settings(ingestion_tables, pair),
        dynamodb_client=dynamodb,
        sqs_client=sqs,
        metrics=metrics,
        out=io.StringIO(),
        err=io.StringIO(),
    )
    assert rerun == 3
    assert _drain(sqs, pair.source_url, expected=0, attempts=2) == []


def test_dlq_redrive_moves_n_messages_and_preserves_their_bodies(
    ingestion_queue_pair: QueueFactory,
) -> None:
    pair = ingestion_queue_pair(visibility_seconds=30)
    sqs = sqs_client()
    for body in DLQ_BODIES:
        sqs.send_message(QueueUrl=pair.dlq_url, MessageBody=body)
    metrics = RecordingTelemetryMetrics()

    sample_out = io.StringIO()
    assert (
        _bounded(
            telemetry_dlq.main,
            ["sample"],
            settings=_settings(None, pair),
            sqs_client=sqs,
            metrics=metrics,
            out=sample_out,
            err=io.StringIO(),
        )
        == 0
    )
    sampled = [json.loads(line) for line in sample_out.getvalue().splitlines()]
    assert len([line for line in sampled if "message_id" in line]) == len(DLQ_BODIES)
    assert "not json" not in sample_out.getvalue()

    assert (
        _bounded(
            telemetry_dlq.main,
            ["redrive"],
            settings=_settings(None, pair),
            sqs_client=sqs,
            metrics=metrics,
            out=io.StringIO(),
            err=io.StringIO(),
        )
        == 0
    )

    out = io.StringIO()
    code = _bounded(
        telemetry_dlq.main,
        ["redrive", "--apply", "--max", str(len(DLQ_BODIES))],
        settings=_settings(None, pair),
        sqs_client=sqs,
        metrics=metrics,
        out=out,
        err=io.StringIO(),
    )

    assert code == 0, out.getvalue()
    summary = json.loads(out.getvalue().splitlines()[-1])
    assert (summary["seen"], summary["redriven"], summary["failed"]) == (len(DLQ_BODIES), 4, 0)
    moved = _drain(sqs, pair.source_url, expected=len(DLQ_BODIES))
    assert sorted(message["Body"] for message in moved) == sorted(DLQ_BODIES)
    assert _drain(sqs, pair.dlq_url, expected=0, attempts=2) == []
    redriven = sum(
        record["value"] for record in metrics.records if record["labels"] == {"result": "redriven"}
    )
    assert redriven == len(DLQ_BODIES)
