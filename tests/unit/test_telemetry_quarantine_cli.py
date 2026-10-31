"""P3-09 unit tests for the operator quarantine CLI `scripts/admin/telemetry_quarantine.py` (D10).

The CLI is driven through two seams:

- `await run(argv, *, quarantine, ingest_state, producer, metrics, out, clock) -> int` over the
  P3-00 in-memory port fakes (`FakeQuarantineRepository`, `FakeIngestStateRepository`,
  `FakeIngressQueue`, `RecordingTelemetryMetrics`);
- `main(argv, *, settings, dynamodb_client, sqs_client, metrics, out, err, clock) -> int`, the
  composition: it reads only existing and P3-00 Settings fields, builds its own boto3 clients
  unless they are injected, and uses a JSON-lines stderr metrics sink unless `metrics` is given.

Output is JSON lines on `out`: one object per entry and a final summary object carrying
`"command"`. Exit codes: 0 ok (a dry run included), 1 error (usage, store, queue or settings),
3 partial (an applied replay or dismiss with any skipped or failed entry). Dry run is the default;
only `--apply` sends or writes. Errors are reported as a fixed category, never an exception text.

A replay groups the requested open, replayable entries by `(ingress_id, device_integration_id)`
and sends one NEW `telemetry-ingress@1` envelope per group: `ingress_id = replay_ingress_id(group
entry keys)`, `replay_of` the root original ingress id (`entry.replay_of or entry.ingress_id`),
`replay=true`, the first `received_at`, the stored mapping versions and provider fields, and the
stored source observations in entry-key order. Each entry is marked `replayed` (with that id) only
after its group's send succeeded. Repeated `--entry` keys count once. A post-send conflict on
one row fails that row (`reason=conflict`); any other store error aborts the run with exit 1.
`list --limit` accepts 1..1000 (default 50) and `--status` defaults to `open`. Replay metrics are
one `ingress_quarantine_replayed_total` increment per applied entry; a dry run and `dismiss` emit
none.
"""

import io
import json
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any

import pytest

from limnopulse_api.adapters.dynamodb_quarantine import DynamoQuarantineRepository
from limnopulse_api.core.config import Settings
from limnopulse_api.core.errors import ConflictError, IngressMessageTooLarge, IngressQueueError
from limnopulse_api.domain.ids import replay_ingress_id
from limnopulse_api.domain.ingestion import (
    IngressEnvelope,
    IngressFence,
    IngressFenceReason,
    NormalizationVersions,
    QuarantineEntry,
    QuarantineEntryKind,
    QuarantineEntryStatus,
    build_quarantine_entries,
)
from limnopulse_api.domain.telemetry_v2 import (
    QuarantinedObservation,
    QuarantineReason,
    SourceObservation,
)
from limnopulse_api.services.telemetry_pipeline import PipelineOutcome
from limnopulse_api.services.telemetry_v2_writes import TelemetryWriteOutcome
from scripts.admin import telemetry_quarantine
from tests.support.ingestion_contract_kits import (
    ACCOUNT,
    COMPONENT,
    DEVICE,
    INGRESS,
    INTEGRATION,
    OBSERVED,
    OTHER_ACCOUNT,
    OTHER_INTEGRATION,
    OTHER_TENANT,
    RECEIVED,
    TENANT,
    envelope,
    marks,
    source_observation,
)
from tests.support.ingestion_dynamo_fake import IngestionDynamoClient
from tests.support.ingestion_fakes import (
    FakeIngestStateRepository,
    FakeIngressQueue,
    FakeQuarantineRepository,
    Faults,
    IngestionStore,
    RecordingTelemetryMetrics,
)

NOW = RECEIVED + timedelta(hours=1)
OTHER_INGRESS = "ing_" + "9" * 32
ROOT_INGRESS = "ing_" + "e" * 32
SENTINEL = "SENTINEL-p3-09-must-not-leak"
FENCED_REASON = "fenced_integration_decommissioned"
PINNED_VERSIONS = NormalizationVersions(
    catalog_version="catalog@pinned",
    unit_mapping_version="units@pinned",
    quality_policy_version="quality@pinned",
)
REPLAYED_METRIC = "ingress_quarantine_replayed_total"


class Harness:
    def __init__(self) -> None:
        self.faults = Faults()
        self.store = IngestionStore(self.faults)
        self.store.now = NOW
        self.quarantine = FakeQuarantineRepository(self.store)
        self.ingest_state = FakeIngestStateRepository(self.store)
        self.queue = FakeIngressQueue(self.faults)
        self.metrics = RecordingTelemetryMetrics()
        self.out = io.StringIO()

    async def seed(self, entries: Sequence[QuarantineEntry]) -> list[QuarantineEntry]:
        await self.quarantine.put_quarantine_entries(entries)
        return list(entries)

    async def run(self, *argv: str) -> int:
        self.out = io.StringIO()
        return await telemetry_quarantine.run(
            list(argv),
            quarantine=self.quarantine,
            ingest_state=self.ingest_state,
            producer=self.queue,
            metrics=self.metrics,
            out=self.out,
            clock=lambda: NOW,
        )

    def lines(self) -> list[dict[str, Any]]:
        return [json.loads(line) for line in self.out.getvalue().splitlines() if line.strip()]

    def entry_lines(self) -> list[dict[str, Any]]:
        return [line for line in self.lines() if "command" not in line]

    def summary(self) -> dict[str, Any]:
        summaries = [line for line in self.lines() if "command" in line]
        assert len(summaries) == 1, self.out.getvalue()
        return summaries[0]

    def sent(self) -> list[IngressEnvelope]:
        return [
            IngressEnvelope.model_validate_json(message["body"])
            for message in self.queue.messages.values()
        ]

    def row(self, entry: QuarantineEntry) -> QuarantineEntry:
        return self.store.quarantine[(entry.tenant_id, entry.entry_key)]

    def metric_total(self, result: str) -> int:
        return sum(
            record["value"]
            for record in self.metrics.records
            if record["metric"] == REPLAYED_METRIC and record["labels"] == {"result": result}
        )


@pytest.fixture
def harness() -> Harness:
    return Harness()


def _fenced(
    count: int = 2,
    *,
    ingress_id: str = INGRESS,
    integration_id: str = INTEGRATION,
    tenant_id: str = TENANT,
    replay_of: str | None = None,
    metadata: dict[str, str] | None = None,
) -> tuple[list[QuarantineEntry], QuarantineEntry]:
    """Replayable `fenced_observation` rows of one message, and its `fenced_message` row."""
    observations = [
        source_observation(
            observed_at=OBSERVED + timedelta(seconds=index),
            sequence=index + 1,
            source_metadata=metadata or {},
        )
        for index in range(count)
    ]
    message = envelope(
        ingress_id=ingress_id,
        tenant_id=tenant_id,
        device_integration_id=integration_id,
        connector_version="fw-1.2",
        mapping_versions=PINNED_VERSIONS,
        replay_of=replay_of,
        replay=replay_of is not None,
        observations=observations,
    )
    rows = build_quarantine_entries(
        message,
        fence=IngressFence(
            reason=IngressFenceReason.INTEGRATION_DECOMMISSIONED, account_verified=True
        ),
        now=RECEIVED,
    )
    replayable = [row for row in rows if row.kind == QuarantineEntryKind.FENCED_OBSERVATION]
    summary = next(row for row in rows if row.kind == QuarantineEntryKind.FENCED_MESSAGE)
    assert len(replayable) == count and all(row.replayable for row in replayable)
    return replayable, summary


def _normalizer_rows(*, truncated: bool) -> list[QuarantineEntry]:
    """`kind=observation` rows (normalizer quarantine); a truncated excerpt is not replayable."""
    observation = source_observation(value=7.25, sequence=4)
    excerpt = observation.model_dump_json(exclude_none=True, exclude_defaults=True)
    if truncated:
        excerpt = excerpt[:-5]
    quarantined = QuarantinedObservation(
        reason=QuarantineReason.UNIT_UNSUPPORTED,
        tenant_id=TENANT,
        device_id=DEVICE,
        component_id=COMPONENT,
        metric_key="temperature",
        observed_at=OBSERVED,
        received_at=RECEIVED,
        source_event_id=None,
        detail="quarantined",
        source_excerpt_json=excerpt,
        truncated=truncated,
    )
    outcome = PipelineOutcome(
        written=TelemetryWriteOutcome(v2_points=0, legacy_points=0, legacy_excluded=0),
        quarantined=(quarantined,),
        counts={},
    )
    message = envelope(
        ingress_id=OTHER_INGRESS if truncated else INGRESS,
        mapping_versions=PINNED_VERSIONS,
        observations=[observation],
    )
    rows = list(build_quarantine_entries(message, outcome=outcome, now=RECEIVED))
    assert [row.kind for row in rows] == [QuarantineEntryKind.OBSERVATION]
    assert rows[0].replayable is (not truncated)
    return rows


def _keys(entries: Sequence[QuarantineEntry]) -> list[str]:
    return [entry.entry_key for entry in entries]


def _expected_envelope(entries: Sequence[QuarantineEntry]) -> IngressEnvelope:
    first = entries[0]
    ordered = sorted(entries, key=lambda entry: entry.entry_key)
    return IngressEnvelope.model_validate(
        {
            "schema": "telemetry-ingress@1",
            "ingress_id": replay_ingress_id(_keys(entries)),
            "replay_of": first.replay_of or first.ingress_id,
            "tenant_id": first.tenant_id,
            "integration_account_id": first.integration_account_id,
            "provider_key": first.provider_key,
            "external_identity_hash": first.external_identity_hash,
            "device_integration_id": first.device_integration_id,
            "device_id": first.device_id,
            "source_type": first.source_type,
            "connector_version": first.connector_version,
            "received_at": min(entry.received_at for entry in entries),
            "replay": True,
            "mapping_versions": first.mapping_versions,
            "observations": [
                SourceObservation.model_validate_json(entry.source_observation_json or "")
                for entry in ordered
            ],
        }
    )


# Replay ----------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replay_apply_sends_one_new_envelope_with_the_stored_fields(harness) -> None:
    entries, _ = _fenced(3)
    # One row was touched before (version 3): the transition must use the version it read.
    entries[1] = entries[1].model_copy(update={"version": 3})
    await harness.seed(entries)

    code = await harness.run("replay", "--tenant", TENANT, "--entry", *_keys(entries), "--apply")

    assert code == telemetry_quarantine.EXIT_OK == 0
    sent = harness.sent()
    assert sent == [_expected_envelope(entries)]
    replay = sent[0]
    assert replay.ingress_id != INGRESS
    assert replay.ingress_id == replay_ingress_id(_keys(entries))
    assert replay.replay_of == INGRESS and replay.replay is True
    assert replay.received_at == RECEIVED
    assert replay.mapping_versions == PINNED_VERSIONS
    assert replay.connector_version == "fw-1.2"
    body = json.loads(next(iter(harness.queue.messages.values()))["body"])
    assert body["replay"] is True and body["replay_of"] == INGRESS
    assert body["mapping_versions"] == PINNED_VERSIONS.model_dump(mode="json")
    for entry in entries:
        row = harness.row(entry)
        assert row.status == QuarantineEntryStatus.REPLAYED
        assert row.replayed_ingress_id == replay.ingress_id
        assert row.version == entry.version + 1
        assert row.updated_at == NOW
    results = {line["entry_key"]: line for line in harness.entry_lines()}
    assert set(results) == set(_keys(entries))
    assert {line["result"] for line in results.values()} == {"replayed"}
    assert {line["replay_ingress_id"] for line in results.values()} == {replay.ingress_id}
    summary = harness.summary()
    assert summary["command"] == "replay" and summary["apply"] is True
    assert (summary["replayed"], summary["skipped"], summary["failed"]) == (3, 0, 0)
    assert harness.metric_total("replayed") == 3
    assert harness.metric_total("failed") == harness.metric_total("skipped") == 0


@pytest.mark.asyncio
async def test_replay_groups_by_ingress_id_and_device_integration(harness) -> None:
    first, _ = _fenced(2)
    other_ingress, _ = _fenced(1, ingress_id=OTHER_INGRESS)
    # Same ingress id, different integration (a forged pair): distinct excerpts keep ids apart.
    other_integration, _ = _fenced(
        2, integration_id=OTHER_INTEGRATION, metadata={"firmware_version": "other"}
    )
    assert not set(_keys(first)) & set(_keys(other_integration))
    await harness.seed([*first, *other_ingress, *other_integration])

    keys = _keys([*other_integration, *first, *other_ingress])
    code = await harness.run("replay", "--tenant", TENANT, "--entry", *keys, "--apply")

    assert code == 0
    sent = {item.ingress_id: item for item in harness.sent()}
    expected = [_expected_envelope(group) for group in (first, other_ingress, other_integration)]
    assert len(sent) == 3
    assert sent == {item.ingress_id: item for item in expected}
    assert sent[replay_ingress_id(_keys(other_ingress))].replay_of == OTHER_INGRESS
    assert (
        sent[replay_ingress_id(_keys(other_integration))].device_integration_id == OTHER_INTEGRATION
    )
    for group in (first, other_ingress, other_integration):
        for entry in group:
            assert harness.row(entry).replayed_ingress_id == replay_ingress_id(_keys(group))


@pytest.mark.asyncio
async def test_replay_id_is_deterministic_and_reused_by_a_rerun(harness) -> None:
    entries, _ = _fenced(2)
    await harness.seed(entries)
    keys = _keys(entries)
    # Crash after the send and before any row is marked: the store dies on the first write.
    harness.faults.fail_next("transition_quarantine_entry", RuntimeError(SENTINEL))

    first_code = await harness.run("replay", "--tenant", TENANT, "--entry", *keys, "--apply")
    assert first_code == telemetry_quarantine.EXIT_ERROR == 1
    assert SENTINEL not in harness.out.getvalue()
    assert {harness.row(entry).status for entry in entries} == {QuarantineEntryStatus.OPEN}
    rerun_code = await harness.run(
        "replay", "--tenant", TENANT, "--entry", *reversed(keys), "--apply"
    )

    assert rerun_code == 0
    sent = harness.sent()
    assert len(sent) == 2
    assert sent[0] == sent[1] == _expected_envelope(entries)
    assert sent[0].ingress_id == replay_ingress_id(keys)
    assert {harness.row(entry).status for entry in entries} == {QuarantineEntryStatus.REPLAYED}
    assert {harness.row(entry).replayed_ingress_id for entry in entries} == {
        replay_ingress_id(keys)
    }


@pytest.mark.asyncio
async def test_replay_reports_a_row_that_changed_after_the_send_as_failed(harness) -> None:
    entries, _ = _fenced(2)
    await harness.seed(entries)
    harness.faults.fail_next("transition_quarantine_entry", ConflictError(SENTINEL))

    code = await harness.run("replay", "--tenant", TENANT, "--entry", *_keys(entries), "--apply")

    assert code == telemetry_quarantine.EXIT_PARTIAL == 3
    assert SENTINEL not in harness.out.getvalue()
    assert len(harness.sent()) == 1
    results = sorted(line["result"] for line in harness.entry_lines())
    assert results == ["failed", "replayed"]
    failed = next(line for line in harness.entry_lines() if line["result"] == "failed")
    assert failed["reason"] == "conflict"
    assert harness.metric_total("failed") == 1 and harness.metric_total("replayed") == 1


@pytest.mark.asyncio
async def test_replay_of_a_replayed_message_points_at_the_root_original(harness) -> None:
    entries, _ = _fenced(1, ingress_id=OTHER_INGRESS, replay_of=ROOT_INGRESS)
    await harness.seed(entries)

    assert (
        await harness.run("replay", "--tenant", TENANT, "--entry", *_keys(entries), "--apply") == 0
    )

    (sent,) = harness.sent()
    assert sent.replay_of == ROOT_INGRESS and sent.replay is True
    assert sent.ingress_id == replay_ingress_id(_keys(entries))


@pytest.mark.asyncio
async def test_replay_sends_a_normalizer_quarantined_row_and_skips_a_truncated_one(harness) -> None:
    (row,) = await harness.seed(_normalizer_rows(truncated=False))
    (truncated,) = await harness.seed(_normalizer_rows(truncated=True))

    code = await harness.run(
        "replay", "--tenant", TENANT, "--entry", row.entry_key, truncated.entry_key, "--apply"
    )

    assert code == 3
    assert harness.sent() == [_expected_envelope([row])]
    assert harness.sent()[0].observations[0].value == 7.25
    results = {line["entry_key"]: line for line in harness.entry_lines()}
    assert results[row.entry_key]["result"] == "replayed"
    assert results[truncated.entry_key] == {
        **results[truncated.entry_key],
        "result": "skipped",
        "reason": "not_replayable",
    }
    assert harness.row(truncated).status == QuarantineEntryStatus.OPEN


@pytest.mark.asyncio
async def test_replay_counts_a_repeated_entry_key_once(harness) -> None:
    entries, _ = _fenced(2)
    await harness.seed(entries)
    keys = _keys(entries)

    code = await harness.run(
        "replay", "--tenant", TENANT, "--entry", *keys, keys[0], keys[1], "--apply"
    )

    assert code == 0
    assert harness.sent() == [_expected_envelope(entries)]
    assert len(harness.sent()[0].observations) == 2
    assert sorted(line["entry_key"] for line in harness.entry_lines()) == sorted(keys)
    assert harness.metric_total("replayed") == 2


@pytest.mark.parametrize(
    "error", [IngressQueueError(SENTINEL, retryable=True), IngressMessageTooLarge(SENTINEL)]
)
@pytest.mark.asyncio
async def test_replay_marks_nothing_when_the_send_fails(harness, error) -> None:
    failing, _ = _fenced(2)
    succeeding, _ = _fenced(1, ingress_id=OTHER_INGRESS)
    await harness.seed([*failing, *succeeding])
    harness.faults.fail_next("send", error)

    keys = _keys([*failing, *succeeding])
    code = await harness.run("replay", "--tenant", TENANT, "--entry", *keys, "--apply")

    assert code == 3
    assert SENTINEL not in harness.out.getvalue()
    sent = harness.sent()
    assert len(sent) == 1
    failed_group = (
        failing if sent[0].ingress_id != replay_ingress_id(_keys(failing)) else succeeding
    )
    replayed_group = succeeding if failed_group is failing else failing
    for entry in failed_group:
        row = harness.row(entry)
        assert row.status == QuarantineEntryStatus.OPEN
        assert row.replayed_ingress_id is None and row.version == entry.version
    for entry in replayed_group:
        assert harness.row(entry).status == QuarantineEntryStatus.REPLAYED
    results = {line["entry_key"]: line["result"] for line in harness.entry_lines()}
    assert {results[key] for key in _keys(failed_group)} == {"failed"}
    assert {results[key] for key in _keys(replayed_group)} == {"replayed"}
    summary = harness.summary()
    assert summary["failed"] == len(failed_group)
    assert summary["replayed"] == len(replayed_group)
    assert harness.metric_total("failed") == len(failed_group)
    assert harness.metric_total("replayed") == len(replayed_group)


@pytest.mark.asyncio
async def test_replay_skips_and_reports_non_replayable_and_non_open_entries(harness) -> None:
    replayable, summary_row = _fenced(3)
    dismissed, already_replayed, open_entry = replayable
    await harness.seed([*replayable, summary_row])
    await harness.quarantine.transition_quarantine_entry(
        TENANT, dismissed.entry_key, QuarantineEntryStatus.DISMISSED, expected_version=1, at=NOW
    )
    await harness.quarantine.transition_quarantine_entry(
        TENANT,
        already_replayed.entry_key,
        QuarantineEntryStatus.REPLAYED,
        expected_version=1,
        at=NOW,
        replayed_ingress_id=OTHER_INGRESS,
    )
    missing_key = RECEIVED.strftime("%Y-%m-%dT%H:%M:%S.%fZ") + "#qtn_" + "0" * 32
    keys = [summary_row.entry_key, dismissed.entry_key, already_replayed.entry_key, missing_key]

    code = await harness.run(
        "replay", "--tenant", TENANT, "--entry", *keys, open_entry.entry_key, "--apply"
    )

    assert code == 3
    (sent,) = harness.sent()
    assert sent == _expected_envelope([open_entry])
    assert [item.model_dump() for item in sent.observations] == [
        SourceObservation.model_validate_json(open_entry.source_observation_json or "").model_dump()
    ]
    results = {line["entry_key"]: line for line in harness.entry_lines()}
    assert {key: results[key]["result"] for key in keys} == dict.fromkeys(keys, "skipped")
    assert results[summary_row.entry_key]["reason"] == "not_replayable"
    assert results[dismissed.entry_key]["reason"] == "not_open"
    assert results[already_replayed.entry_key]["reason"] == "not_open"
    assert results[missing_key]["reason"] == "not_found"
    assert results[open_entry.entry_key]["result"] == "replayed"
    assert harness.row(summary_row).status == QuarantineEntryStatus.OPEN
    assert harness.row(dismissed).status == QuarantineEntryStatus.DISMISSED
    assert harness.row(already_replayed).replayed_ingress_id == OTHER_INGRESS
    summary = harness.summary()
    assert (summary["replayed"], summary["skipped"], summary["failed"]) == (1, 4, 0)
    assert harness.metric_total("skipped") == 4
    assert harness.metric_total("replayed") == 1


@pytest.mark.asyncio
async def test_replay_never_reads_another_tenants_rows(harness) -> None:
    foreign, _ = _fenced(1, tenant_id=OTHER_TENANT)
    await harness.seed(foreign)

    code = await harness.run("replay", "--tenant", TENANT, "--entry", *_keys(foreign), "--apply")

    assert code == 3
    assert harness.sent() == []
    assert harness.entry_lines()[0]["reason"] == "not_found"
    assert harness.row(foreign[0]).status == QuarantineEntryStatus.OPEN


@pytest.mark.asyncio
async def test_replay_dry_run_is_the_default_and_sends_and_writes_nothing(harness) -> None:
    entries, summary_row = _fenced(2)
    await harness.seed([*entries, summary_row])
    before = dict(harness.store.quarantine)

    code = await harness.run(
        "replay", "--tenant", TENANT, "--entry", *_keys([*entries, summary_row])
    )

    assert code == 0
    assert harness.queue.messages == {}
    assert harness.store.quarantine == before
    assert harness.metrics.records == []
    results = {line["entry_key"]: line for line in harness.entry_lines()}
    for entry in entries:
        assert results[entry.entry_key]["result"] == "would_replay"
        assert results[entry.entry_key]["replay_ingress_id"] == replay_ingress_id(_keys(entries))
    assert results[summary_row.entry_key]["result"] == "skipped"
    summary = harness.summary()
    assert summary["apply"] is False and summary["replayed"] == 0
    assert summary["would_replay"] == 2 and summary["skipped"] == 1


@pytest.mark.asyncio
async def test_replay_store_failure_exits_1_without_sending_or_leaking(harness) -> None:
    entries, _ = _fenced(2)
    await harness.seed(entries)
    harness.faults.fail_next("get_quarantine_entry", RuntimeError(SENTINEL))

    code = await harness.run("replay", "--tenant", TENANT, "--entry", *_keys(entries), "--apply")

    assert code == telemetry_quarantine.EXIT_ERROR == 1
    assert harness.queue.messages == {}
    assert {harness.row(entry).status for entry in entries} == {QuarantineEntryStatus.OPEN}
    assert SENTINEL not in harness.out.getvalue()
    assert harness.lines() and "error" in harness.lines()[-1]


# Dismiss ---------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dismiss_apply_transitions_with_the_read_version(harness) -> None:
    entries, _ = _fenced(2)
    entries[0] = entries[0].model_copy(update={"version": 3})
    await harness.seed(entries)

    code = await harness.run("dismiss", "--tenant", TENANT, "--entry", *_keys(entries), "--apply")

    assert code == 0
    assert harness.metrics.records == []  # dismiss emits no replay metric
    for entry in entries:
        row = harness.row(entry)
        assert row.status == QuarantineEntryStatus.DISMISSED
        assert row.version == entry.version + 1 and row.updated_at == NOW
        assert row.replayed_ingress_id is None
    assert {line["result"] for line in harness.entry_lines()} == {"dismissed"}
    assert harness.summary()["command"] == "dismiss"
    assert harness.summary()["dismissed"] == 2
    assert harness.queue.messages == {}


class StaleReadQuarantine:
    """Serves the version it read first, as if another operator changed the row in between."""

    def __init__(self, inner: FakeQuarantineRepository, stale: dict[str, QuarantineEntry]) -> None:
        self.inner = inner
        self.stale = stale
        self.transitions: list[dict[str, Any]] = []

    async def get_quarantine_entry(self, tenant_id: str, entry_key: str) -> QuarantineEntry | None:
        if entry_key in self.stale:
            return self.stale[entry_key]
        return await self.inner.get_quarantine_entry(tenant_id, entry_key)

    async def transition_quarantine_entry(self, tenant_id: str, entry_key: str, target, **kwargs):
        self.transitions.append({"entry_key": entry_key, "target": target, **kwargs})
        return await self.inner.transition_quarantine_entry(tenant_id, entry_key, target, **kwargs)

    async def list_quarantine_entries(self, *args: Any, **kwargs: Any):
        return await self.inner.list_quarantine_entries(*args, **kwargs)

    async def put_quarantine_entries(self, entries: Sequence[QuarantineEntry]) -> None:
        await self.inner.put_quarantine_entries(entries)


@pytest.mark.asyncio
async def test_dismiss_is_versioned_and_a_concurrent_change_fails(harness) -> None:
    raced, fresh = _fenced(2)[0]
    fresh = fresh.model_copy(update={"version": 3})
    await harness.seed([raced, fresh])
    # Another operator bumps `raced` to version 2 by changing it after this CLI read version 1:
    # the fake row is rewritten at version 2 but still open, so only the version can fence it.
    harness.store.quarantine[(TENANT, raced.entry_key)] = raced.model_copy(update={"version": 2})
    harness.quarantine = StaleReadQuarantine(harness.quarantine, {raced.entry_key: raced})

    code = await harness.run(
        "dismiss", "--tenant", TENANT, "--entry", raced.entry_key, fresh.entry_key, "--apply"
    )

    assert code == 3
    by_key = {item["entry_key"]: item for item in harness.quarantine.transitions}
    assert by_key[raced.entry_key]["expected_version"] == 1
    assert by_key[fresh.entry_key]["expected_version"] == fresh.version == 3
    assert harness.row(fresh).version == 4
    assert by_key[fresh.entry_key]["target"] == QuarantineEntryStatus.DISMISSED
    assert harness.row(raced).status == QuarantineEntryStatus.OPEN
    assert harness.row(fresh).status == QuarantineEntryStatus.DISMISSED
    results = {line["entry_key"]: line for line in harness.entry_lines()}
    assert results[raced.entry_key]["result"] == "failed"
    assert results[raced.entry_key]["reason"] == "conflict"
    assert results[fresh.entry_key]["result"] == "dismissed"
    assert harness.summary()["failed"] == 1


@pytest.mark.asyncio
async def test_dismiss_dry_run_writes_nothing_and_skips_terminal_rows(harness) -> None:
    entries, summary_row = _fenced(2)
    await harness.seed([*entries, summary_row])
    await harness.quarantine.transition_quarantine_entry(
        TENANT, entries[0].entry_key, QuarantineEntryStatus.DISMISSED, expected_version=1, at=NOW
    )
    before = dict(harness.store.quarantine)

    code = await harness.run(
        "dismiss", "--tenant", TENANT, "--entry", *_keys([*entries, summary_row])
    )

    assert code == 0
    assert harness.store.quarantine == before
    results = {line["entry_key"]: line for line in harness.entry_lines()}
    assert results[entries[0].entry_key]["result"] == "skipped"
    assert results[entries[0].entry_key]["reason"] == "not_open"
    # A non-replayable open row may still be dismissed.
    assert results[entries[1].entry_key]["result"] == "would_dismiss"
    assert results[summary_row.entry_key]["result"] == "would_dismiss"
    assert harness.summary()["apply"] is False


@pytest.mark.asyncio
async def test_dismiss_apply_skips_terminal_rows_as_partial(harness) -> None:
    entries, _ = _fenced(2)
    await harness.seed(entries)
    await harness.quarantine.transition_quarantine_entry(
        TENANT, entries[0].entry_key, QuarantineEntryStatus.DISMISSED, expected_version=1, at=NOW
    )

    (foreign,) = await harness.seed(_fenced(1, ingress_id=ROOT_INGRESS, tenant_id=OTHER_TENANT)[0])
    missing_key = RECEIVED.strftime("%Y-%m-%dT%H:%M:%S.%fZ") + "#qtn_" + "0" * 32
    keys = [*_keys(entries), foreign.entry_key, missing_key]

    code = await harness.run("dismiss", "--tenant", TENANT, "--entry", *keys, "--apply")

    assert code == 3
    assert harness.row(entries[0]).version == 2  # not written again
    assert harness.row(entries[1]).status == QuarantineEntryStatus.DISMISSED
    assert harness.row(foreign).status == QuarantineEntryStatus.OPEN
    results = {line["entry_key"]: line for line in harness.entry_lines()}
    assert results[foreign.entry_key]["reason"] == results[missing_key]["reason"] == "not_found"
    summary = harness.summary()
    assert (summary["dismissed"], summary["skipped"], summary["failed"]) == (1, 3, 0)


# List ------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_pages_by_sk_lower_bound_and_defaults_to_open(harness) -> None:
    first, first_summary = _fenced(3)
    second, second_summary = _fenced(2, ingress_id=OTHER_INGRESS)
    rows = await harness.seed([*first, first_summary, *second, second_summary])
    await harness.quarantine.transition_quarantine_entry(
        TENANT, first[0].entry_key, QuarantineEntryStatus.DISMISSED, expected_version=1, at=NOW
    )
    open_keys = sorted(key for key in _keys(rows) if key != first[0].entry_key)

    seen: list[str] = []
    after: list[str] = []
    finished = False
    for _ in range(10):
        assert await harness.run("list", "--tenant", TENANT, "--limit", "2", *after) == 0
        page = [line["entry_key"] for line in harness.entry_lines()]
        assert len(page) <= 2
        seen.extend(page)
        summary = harness.summary()
        assert summary["command"] == "list" and summary["count"] == len(page)
        if summary["next_after"] is None:
            finished = True
            break
        assert page and summary["next_after"] == page[-1]
        after = ["--after", summary["next_after"]]
    assert seen == open_keys
    assert finished and len(open_keys) == 6
    assert {line["status"] for line in harness.entry_lines()} <= {"open"}


@pytest.mark.asyncio
async def test_list_filters_by_status_and_reason(harness) -> None:
    entries, summary_row = _fenced(2)
    await harness.seed([*entries, summary_row])
    await harness.quarantine.transition_quarantine_entry(
        TENANT, entries[0].entry_key, QuarantineEntryStatus.DISMISSED, expected_version=1, at=NOW
    )

    assert await harness.run("list", "--tenant", TENANT, "--status", "dismissed") == 0
    dismissed = harness.entry_lines()
    assert [line["entry_key"] for line in dismissed] == [entries[0].entry_key]
    assert dismissed[0]["status"] == "dismissed"
    assert dismissed[0]["reason"] == FENCED_REASON

    assert await harness.run("list", "--tenant", TENANT, "--reason", "fenced_account_revoked") == 0
    assert harness.entry_lines() == []
    assert await harness.run("list", "--tenant", TENANT, "--reason", FENCED_REASON) == 0
    assert sorted(line["entry_key"] for line in harness.entry_lines()) == sorted(
        _keys([entries[1], summary_row])
    )
    line = next(
        item for item in harness.entry_lines() if item["entry_key"] == summary_row.entry_key
    )
    assert line["kind"] == "fenced_message" and line["replayable"] is False


@pytest.mark.asyncio
async def test_list_is_tenant_scoped(harness) -> None:
    await harness.seed(_fenced(1, tenant_id=OTHER_TENANT)[0])

    assert await harness.run("list", "--tenant", TENANT) == 0

    assert harness.entry_lines() == []
    assert harness.summary()["next_after"] is None


@pytest.mark.parametrize("limit", ["0", "-1", "1001", "many"])
@pytest.mark.asyncio
async def test_list_rejects_an_out_of_range_limit(harness, limit) -> None:
    assert await harness.run("list", "--tenant", TENANT, "--limit", limit) == 1


@pytest.mark.asyncio
async def test_list_shows_excerpts_only_on_request(harness) -> None:
    entries, summary_row = _fenced(1, metadata={"firmware_version": SENTINEL})
    await harness.seed([*entries, summary_row])

    assert await harness.run("list", "--tenant", TENANT) == 0
    assert SENTINEL not in harness.out.getvalue()

    assert await harness.run("list", "--tenant", TENANT, "--show-excerpt") == 0
    lines = {line["entry_key"]: line for line in harness.entry_lines()}
    assert (
        lines[entries[0].entry_key]["source_observation_json"] == entries[0].source_observation_json
    )
    assert SENTINEL in lines[summary_row.entry_key]["envelope_excerpt_json"]


# Health ----------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_health_prints_the_connector_health_row_read_only(harness) -> None:
    quarantined_at = RECEIVED + timedelta(minutes=5)
    await harness.ingest_state.apply_ingest_marks(
        marks(quarantined_at=quarantined_at, quarantine_reason="unit_unknown")
    )
    before = {name: dict(row) for name, row in harness.store.connector_health.items()}

    assert await harness.run("health", "--tenant", TENANT, "--account", ACCOUNT) == 0

    assert harness.store.connector_health == before
    assert harness.store.quarantine == {} and harness.queue.messages == {}
    (line,) = harness.lines()
    assert line["command"] == "health" and line["found"] is True
    health = line["health"]
    assert health["integration_account_id"] == ACCOUNT
    assert health["last_quarantine_reason"] == "unit_unknown"
    assert datetime.fromisoformat(health["last_received_at"]) == RECEIVED
    assert datetime.fromisoformat(health["last_quarantined_at"]) == quarantined_at
    assert health["last_fenced_at"] is None


@pytest.mark.asyncio
async def test_health_of_an_unknown_or_foreign_account_is_not_found(harness) -> None:
    await harness.ingest_state.apply_ingest_marks(marks(tenant_id=OTHER_TENANT))

    assert await harness.run("health", "--tenant", TENANT, "--account", ACCOUNT) == 0
    (line,) = harness.lines()
    assert line["command"] == "health" and line["found"] is False and line["health"] is None
    assert await harness.run("health", "--tenant", TENANT, "--account", OTHER_ACCOUNT) == 0
    assert harness.lines()[0]["found"] is False


# Redaction and argv ----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_output_never_carries_raw_identifiers_or_excerpts(harness) -> None:
    entries, summary_row = _fenced(2, metadata={"firmware_version": SENTINEL})
    await harness.seed([*entries, summary_row])
    await harness.ingest_state.apply_ingest_marks(marks())
    outputs: list[str] = []
    for argv in (
        ("list", "--tenant", TENANT),
        ("replay", "--tenant", TENANT, "--entry", *_keys(entries)),
        ("dismiss", "--tenant", TENANT, "--entry", summary_row.entry_key),
        ("replay", "--tenant", TENANT, "--entry", *_keys(entries), "--apply"),
        ("dismiss", "--tenant", TENANT, "--entry", summary_row.entry_key, "--apply"),
        ("health", "--tenant", TENANT, "--account", ACCOUNT),
    ):
        await harness.run(*argv)
        outputs.append(harness.out.getvalue())
    text = "\n".join(outputs)
    assert SENTINEL not in text
    assert entries[0].external_identity_hash not in text
    assert "source_observation_json" not in text and "envelope_excerpt_json" not in text
    assert all(json.loads(line) for line in text.splitlines() if line.strip())


@pytest.mark.parametrize(
    "argv",
    [
        ("list", "--tenant", TENANT, "--bogus", SENTINEL),
        ("list", "--tenant", TENANT, "--status", SENTINEL),
        ("list", "--tenant", TENANT, "--limit", SENTINEL),
        (SENTINEL,),
        ("replay", "--tenant", TENANT),
        ("health", "--tenant", TENANT),
        (),
    ],
)
def test_usage_errors_exit_1_without_echoing_argv(argv, capsys) -> None:
    out, err = io.StringIO(), io.StringIO()

    dynamodb, sqs = IngestionDynamoClient(), RecordingSqsClient()

    code = telemetry_quarantine.main(
        list(argv),
        settings=_settings(),
        dynamodb_client=dynamodb,
        sqs_client=sqs,
        metrics=RecordingTelemetryMetrics(),
        out=out,
        err=err,
    )

    assert code == 1
    assert sqs.sent == [] and dynamodb.items == {}
    captured = capsys.readouterr()
    assert SENTINEL not in out.getvalue() + err.getvalue() + captured.out + captured.err


# Composition -----------------------------------------------------------------------------------


class RecordingClient:
    """Proxies a boto3-style client and records the operation names the CLI calls."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.operations: list[str] = []

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self._inner, name)
        if not callable(attribute):
            return attribute

        def call(*args: Any, **kwargs: Any) -> Any:
            self.operations.append(name)
            return attribute(*args, **kwargs)

        return call


class RecordingSqsClient:
    """A boto3 SQS client stand-in that records `send_message` calls."""

    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []

    def send_message(self, **kwargs: Any) -> dict[str, Any]:
        self.sent.append(kwargs)
        return {"MessageId": f"msg-{len(self.sent)}"}


TABLE = "OperatorDomain"
QUEUE_URL = "http://sqs.local/000000000000/limnopulse-telemetry-ingress"


def _settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "dynamodb_domain_table": TABLE,
        "sqs_telemetry_ingress_queue_url": QUEUE_URL,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


async def _seed_dynamo(client: IngestionDynamoClient) -> list[QuarantineEntry]:
    entries, _ = _fenced(2)
    repository = DynamoQuarantineRepository(TABLE, client, clock=lambda: NOW)
    await repository.put_quarantine_entries(entries)
    return entries


@pytest.mark.asyncio
async def test_main_composes_dynamodb_and_sqs_from_settings() -> None:
    raw = IngestionDynamoClient()
    sqs = RecordingSqsClient()
    entries = await _seed_dynamo(raw)
    dynamodb = RecordingClient(raw)
    metrics = RecordingTelemetryMetrics()
    out = io.StringIO()

    code = await _main_in_thread(
        ["replay", "--tenant", TENANT, "--entry", *_keys(entries), "--apply"],
        settings=_settings(),
        dynamodb_client=dynamodb,
        sqs_client=sqs,
        metrics=metrics,
        out=out,
        err=io.StringIO(),
        clock=lambda: NOW,
    )

    assert code == 0
    (call,) = sqs.sent
    assert call["QueueUrl"] == QUEUE_URL
    # The operator policy (P3-02 IAM) allows only Query, GetItem and UpdateItem on the domain table.
    assert dynamodb.operations and set(dynamodb.operations) <= {"query", "get_item", "update_item"}
    assert IngressEnvelope.model_validate_json(call["MessageBody"]) == _expected_envelope(entries)
    repository = DynamoQuarantineRepository(TABLE, raw, clock=lambda: NOW)
    for entry in entries:
        row = await repository.get_quarantine_entry(TENANT, entry.entry_key)
        assert row is not None and row.status == QuarantineEntryStatus.REPLAYED
    assert sum(r["value"] for r in metrics.records if r["metric"] == REPLAYED_METRIC) == 2


@pytest.mark.asyncio
async def test_main_without_a_queue_url_refuses_to_replay() -> None:
    dynamodb = IngestionDynamoClient()
    sqs = RecordingSqsClient()
    entries = await _seed_dynamo(dynamodb)

    code = await _main_in_thread(
        ["replay", "--tenant", TENANT, "--entry", *_keys(entries), "--apply"],
        settings=_settings(sqs_telemetry_ingress_queue_url=""),
        dynamodb_client=dynamodb,
        sqs_client=sqs,
        metrics=RecordingTelemetryMetrics(),
        out=io.StringIO(),
        err=io.StringIO(),
        clock=lambda: NOW,
    )

    assert code == 1
    assert sqs.sent == []
    repository = DynamoQuarantineRepository(TABLE, dynamodb, clock=lambda: NOW)
    row = await repository.get_quarantine_entry(TENANT, entries[0].entry_key)
    assert row is not None and row.status == QuarantineEntryStatus.OPEN


@pytest.mark.asyncio
async def test_main_lists_through_dynamodb_and_emits_metrics_as_json_lines_on_stderr() -> None:
    dynamodb = IngestionDynamoClient()
    entries = await _seed_dynamo(dynamodb)
    out, err = io.StringIO(), io.StringIO()

    assert (
        await _main_in_thread(
            ["list", "--tenant", TENANT],
            settings=_settings(),
            dynamodb_client=dynamodb,
            sqs_client=RecordingSqsClient(),
            out=out,
            err=err,
            clock=lambda: NOW,
        )
        == 0
    )
    listed = [json.loads(line) for line in out.getvalue().splitlines()]
    assert [line["entry_key"] for line in listed if "command" not in line] == sorted(_keys(entries))

    out, err = io.StringIO(), io.StringIO()
    code = await _main_in_thread(
        ["replay", "--tenant", TENANT, "--entry", *_keys(entries), "--apply"],
        settings=_settings(),
        dynamodb_client=dynamodb,
        sqs_client=RecordingSqsClient(),
        out=out,
        err=err,
        clock=lambda: NOW,
    )
    assert code == 0
    metrics = [json.loads(line) for line in err.getvalue().splitlines() if line.strip()]
    assert metrics, "the default sink writes one JSON object per increment to stderr"
    for record in metrics:
        assert set(record) == {"metric", "value", "labels"}
        assert record["metric"] == REPLAYED_METRIC
        assert record["labels"] == {"result": "replayed"}
    assert sum(record["value"] for record in metrics) == 2
    assert all(
        "command" in json.loads(line) or "entry_key" in json.loads(line)
        for line in out.getvalue().splitlines()
    )


async def _main_in_thread(argv: list[str], **kwargs: Any) -> int:
    """`main` runs its own event loop, so the async tests call it from a worker thread."""
    import asyncio

    return await asyncio.to_thread(telemetry_quarantine.main, argv, **kwargs)


def test_main_builds_its_own_clients_from_settings(monkeypatch) -> None:
    built: list[dict[str, Any]] = []
    dynamodb, sqs = IngestionDynamoClient(), RecordingSqsClient()

    def client(service: str, **kwargs: Any) -> Any:
        built.append({"service": service, **kwargs})
        return dynamodb if service == "dynamodb" else sqs

    monkeypatch.setattr(telemetry_quarantine.boto3, "client", client)
    settings = _settings(
        aws_region="sa-east-1",
        dynamodb_endpoint_url="http://dynamodb.local:8001",
        sqs_endpoint_url="http://sqs.local:9324",
    )

    code = telemetry_quarantine.main(
        ["list", "--tenant", TENANT],
        settings=settings,
        metrics=RecordingTelemetryMetrics(),
        out=io.StringIO(),
        err=io.StringIO(),
        clock=lambda: NOW,
    )

    assert code == 0
    by_service = {item["service"]: item for item in built}
    assert by_service["dynamodb"]["region_name"] == "sa-east-1"
    assert by_service["dynamodb"]["endpoint_url"] == "http://dynamodb.local:8001"
    if "sqs" in by_service:
        assert by_service["sqs"]["region_name"] == "sa-east-1"
        assert by_service["sqs"]["endpoint_url"] == "http://sqs.local:9324"
