"""P3-09 diff-review regressions (GREEN, disclosed): one vector per correctness finding.

1. A deeply nested DLQ body is diagnosed `not_json`, and every message `sample` saw is released.
2. A broken client endpoint (it may hold a credential) fails with `settings`, never a traceback.
3. A store error while marking a sent group fails that row (`store_unavailable`) and the run goes
   on to a summary and exit 3.
4. A group that no longer forms a valid envelope is skipped as `invalid_envelope`, never sent.
5. Output is ASCII-escaped JSON, so a non-UTF-8 stdout cannot fail a listing halfway.
6. `sample` needs only the DLQ URL; `redrive` refuses to run without the source queue.
"""

import io
import json
from datetime import timedelta
from typing import Any

import pytest
from botocore.exceptions import ClientError

from limnopulse_api.core.config import Settings
from limnopulse_api.domain.ingestion import (
    IngressFence,
    IngressFenceReason,
    QuarantineEntry,
    QuarantineEntryKind,
    QuarantineEntryStatus,
    build_quarantine_entries,
)
from scripts.admin import telemetry_dlq, telemetry_quarantine
from tests.support.ingestion_contract_kits import (
    OBSERVED,
    RECEIVED,
    TENANT,
    envelope,
    source_observation,
)
from tests.support.ingestion_fakes import (
    FakeIngestStateRepository,
    FakeIngressQueue,
    FakeQuarantineRepository,
    Faults,
    IngestionStore,
    RecordingTelemetryMetrics,
)

NOW = RECEIVED + timedelta(hours=1)
SENTINEL = "SENTINEL-p3-09-review"
SOURCE_URL = "http://sqs.local/000000000000/limnopulse-telemetry-ingress"
DLQ_URL = "http://sqs.local/000000000000/limnopulse-telemetry-ingress-dlq"


class QuarantineHarness:
    def __init__(self) -> None:
        self.faults = Faults()
        self.store = IngestionStore(self.faults)
        self.store.now = NOW
        self.quarantine = FakeQuarantineRepository(self.store)
        self.queue = FakeIngressQueue(self.faults)
        self.metrics = RecordingTelemetryMetrics()
        self.out = io.StringIO()

    async def run(self, *argv: str) -> int:
        self.out = io.StringIO()
        return await telemetry_quarantine.run(
            list(argv),
            quarantine=self.quarantine,
            ingest_state=FakeIngestStateRepository(self.store),
            producer=self.queue,
            metrics=self.metrics,
            out=self.out,
            clock=lambda: NOW,
        )

    def lines(self) -> list[dict[str, Any]]:
        return [json.loads(line) for line in self.out.getvalue().splitlines()]


def _rows(count: int = 2) -> list[QuarantineEntry]:
    rows = build_quarantine_entries(
        envelope(
            observations=[
                source_observation(observed_at=OBSERVED + timedelta(seconds=index))
                for index in range(count)
            ]
        ),
        fence=IngressFence(
            reason=IngressFenceReason.INTEGRATION_DECOMMISSIONED, account_verified=True
        ),
        now=RECEIVED,
    )
    return [row for row in rows if row.kind == QuarantineEntryKind.FENCED_OBSERVATION]


# 1 ---------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sample_survives_a_deeply_nested_body_and_releases_everything() -> None:
    dlq = FakeIngressQueue(Faults())
    for body in ("[" * 100_000, '{"schema":"x"}', "plain"):
        await dlq.send_raw(body)
    out = io.StringIO()

    code = await telemetry_dlq.run(
        ["sample"], dlq=dlq, source=None, metrics=RecordingTelemetryMetrics(), out=out
    )

    assert code == 0
    lines = [json.loads(line) for line in out.getvalue().splitlines()]
    assert sorted(line["diagnosis"] for line in lines if "diagnosis" in line) == [
        "invalid_envelope",
        "not_json",
        "not_json",
    ]
    visible = await dlq.receive(max_messages=10, wait_seconds=0, visibility_seconds=0)
    assert len(visible) == 3


# 2 ---------------------------------------------------------------------------------------------


def test_quarantine_main_reports_a_broken_endpoint_without_echoing_it(capsys) -> None:
    out, err = io.StringIO(), io.StringIO()

    code = telemetry_quarantine.main(
        ["list", "--tenant", TENANT],
        settings=Settings(_env_file=None, dynamodb_endpoint_url=f"http://user:{SENTINEL}@bad host"),
        metrics=RecordingTelemetryMetrics(),
        out=out,
        err=err,
    )

    assert code == 1
    assert json.loads(out.getvalue()) == {"error": "settings"}
    captured = capsys.readouterr()
    assert SENTINEL not in out.getvalue() + err.getvalue() + captured.out + captured.err


def test_dlq_main_reports_a_broken_endpoint_without_echoing_it(capsys) -> None:
    out, err = io.StringIO(), io.StringIO()

    code = telemetry_dlq.main(
        ["sample"],
        settings=Settings(
            _env_file=None,
            sqs_telemetry_ingress_dlq_url=DLQ_URL,
            sqs_endpoint_url=f"http://user:{SENTINEL}@bad host",
        ),
        metrics=RecordingTelemetryMetrics(),
        out=out,
        err=err,
    )

    assert code == 1
    assert json.loads(out.getvalue()) == {"error": "settings"}
    captured = capsys.readouterr()
    assert SENTINEL not in out.getvalue() + err.getvalue() + captured.out + captured.err


# 3 ---------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_store_error_while_marking_fails_that_row_and_the_run_completes() -> None:
    harness = QuarantineHarness()
    rows = _rows(2)
    await harness.quarantine.put_quarantine_entries(rows)
    harness.faults.fail_next(
        "transition_quarantine_entry",
        ClientError({"Error": {"Code": "ThrottlingException", "Message": SENTINEL}}, "UpdateItem"),
    )

    code = await harness.run(
        "replay", "--tenant", TENANT, "--entry", *(row.entry_key for row in rows), "--apply"
    )

    assert code == 3
    assert len(harness.queue.messages) == 1
    assert SENTINEL not in harness.out.getvalue()
    results = sorted(
        (line["result"], line.get("reason")) for line in harness.lines() if "entry_key" in line
    )
    assert results == [("failed", "store_unavailable"), ("replayed", None)]
    summary = harness.lines()[-1]
    assert summary["command"] == "replay" and summary["failed"] == 1


# 4 ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("apply", [False, True])
@pytest.mark.asyncio
async def test_a_group_that_cannot_form_an_envelope_is_skipped_and_never_sent(apply) -> None:
    harness = QuarantineHarness()
    (row,) = _rows(1)
    # 1001 rows of one message: more observations than an envelope may carry.
    rows = [row.model_copy(update={"quarantine_id": f"qtn_{index:032x}"}) for index in range(1001)]
    await harness.quarantine.put_quarantine_entries(rows)

    argv = ["replay", "--tenant", TENANT, "--entry", *(item.entry_key for item in rows)]
    code = await harness.run(*argv, *(["--apply"] if apply else []))

    assert code == (3 if apply else 0)
    assert harness.queue.messages == {}
    reasons = {line.get("reason") for line in harness.lines() if "entry_key" in line}
    assert reasons == {"invalid_envelope"}
    assert all(
        harness.store.quarantine[(TENANT, item.entry_key)].status == QuarantineEntryStatus.OPEN
        for item in rows
    )


# 5 ---------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_output_is_ascii_escaped_json() -> None:
    harness = QuarantineHarness()
    (row,) = _rows(1)
    await harness.quarantine.put_quarantine_entries([row.model_copy(update={"reason": "café"})])

    assert await harness.run("list", "--tenant", TENANT, "--show-excerpt") == 0

    harness.out.getvalue().encode("ascii")  # never raises
    assert harness.lines()[0]["reason"] == "café"


# 6 ---------------------------------------------------------------------------------------------


class _Sqs:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def receive_message(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs["QueueUrl"])
        return {"Messages": []}


def test_sample_needs_only_the_dlq_url_and_redrive_needs_the_source() -> None:
    settings = Settings(_env_file=None, sqs_telemetry_ingress_dlq_url=DLQ_URL)
    sqs = _Sqs()
    out = io.StringIO()

    assert (
        telemetry_dlq.main(
            ["sample"],
            settings=settings,
            sqs_client=sqs,
            metrics=RecordingTelemetryMetrics(),
            out=out,
            err=io.StringIO(),
        )
        == 0
    )
    assert sqs.calls == [DLQ_URL]

    sqs.calls.clear()
    out = io.StringIO()
    code = telemetry_dlq.main(
        ["redrive", "--apply"],
        settings=settings,
        sqs_client=sqs,
        metrics=RecordingTelemetryMetrics(),
        out=out,
        err=io.StringIO(),
    )
    assert code == 1
    assert json.loads(out.getvalue()) == {"error": "queue_not_configured"}
    assert sqs.calls == []
    assert SOURCE_URL not in out.getvalue()
