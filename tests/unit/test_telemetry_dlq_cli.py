"""P3-09 unit tests for the operator DLQ CLI `scripts/admin/telemetry_dlq.py` (D10, §23.3).

Seams:

- `await run(argv, *, dlq, source, metrics, out) -> int` over the P3-00 `FakeIngressQueue`;
- `main(argv, *, settings, sqs_client, metrics, out, err) -> int`, the composition: one boto3 SQS
  client (injectable) and `SqsIngressQueue` over `SQS_TELEMETRY_INGRESS_DLQ_URL` and
  `SQS_TELEMETRY_INGRESS_QUEUE_URL`; a JSON-lines stderr metrics sink unless `metrics` is given.

`sample [--limit 10]` prints one metadata line per DLQ message (`message_id`, `receive_count`,
`sent_at`, `body_bytes`, `diagnosis` in {`valid_envelope`, `not_json`, `invalid_envelope`}, and for
a valid envelope its `ingress_id`, `replay` and `observations` count), never the body or the
receipt handle. It counts each message once and releases every message it saw (visibility 0).
`redrive [--max 100] [--apply]` delegates to the P3-05 `redrive_dlq` core; dry run is the default.
Exit codes: 0 ok (a dry run included), 1 error, 3 partial (an applied redrive with failures).
Nothing in the CLI consumes the DLQ unless an operator runs a subcommand.
Bounds: `sample --limit` 1..1000, `redrive --max` 1..10000. Applied redrive metrics carry the
report's `redriven` and `failed` counts; a dry run and `sample` emit none.
"""

import asyncio
import io
import json
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from limnopulse_api.core.config import Settings
from limnopulse_api.core.errors import IngressQueueError
from limnopulse_api.domain.ingestion import IngressQueueMessage
from limnopulse_api.services.ingress_redrive import RedriveReport
from scripts.admin import telemetry_dlq
from tests.support.ingestion_contract_kits import INGRESS, envelope, source_observation
from tests.support.ingestion_fakes import FakeIngressQueue, Faults, RecordingTelemetryMetrics

SENTINEL = "SENTINEL-p3-09-dlq-must-not-leak"
REDRIVEN_METRIC = "ingress_dlq_redriven_total"
VALID_BODY = envelope(
    observations=[source_observation(source_metadata={"firmware_version": SENTINEL})]
).to_json()
NOT_JSON_BODY = f"not json {SENTINEL} \U0001f600"
INVALID_BODY = json.dumps({"schema": "unknown@9", "note": SENTINEL})
BODIES = [VALID_BODY, NOT_JSON_BODY, INVALID_BODY]
RECEIVE_BUDGET = 50


class RecordingQueue:
    """Wraps a fake queue, records every call, and fails a runaway scan instead of hanging."""

    def __init__(self, inner: FakeIngressQueue) -> None:
        self.inner = inner
        self.calls: list[tuple[str, Any]] = []

    async def send(self, envelope_: Any) -> str:
        self.calls.append(("send", envelope_))
        return await self.inner.send(envelope_)

    async def send_raw(self, body: str) -> str:
        self.calls.append(("send_raw", body))
        return await self.inner.send_raw(body)

    async def receive(
        self, *, max_messages: int, wait_seconds: int, visibility_seconds: int
    ) -> list[IngressQueueMessage]:
        self.calls.append(("receive", max_messages, visibility_seconds))
        if self.count("receive") > RECEIVE_BUDGET:
            raise AssertionError("the CLI kept receiving: a scan must stop")
        return await self.inner.receive(
            max_messages=max_messages,
            wait_seconds=wait_seconds,
            visibility_seconds=visibility_seconds,
        )

    async def delete(self, message: IngressQueueMessage) -> None:
        self.calls.append(("delete", message.message_id))
        await self.inner.delete(message)

    async def change_visibility(self, message: IngressQueueMessage, seconds: int) -> None:
        self.calls.append(("change_visibility", message.message_id, seconds))
        await self.inner.change_visibility(message, seconds)

    def count(self, name: str) -> int:
        return sum(1 for call in self.calls if call[0] == name)

    def bodies(self) -> list[str]:
        return [message["body"] for message in self.inner.messages.values()]


class Harness:
    def __init__(self) -> None:
        self.faults = Faults()
        self.dlq = RecordingQueue(FakeIngressQueue(self.faults))
        self.source = RecordingQueue(FakeIngressQueue(Faults()))
        self.metrics = RecordingTelemetryMetrics()
        self.out = io.StringIO()

    async def fill(self, bodies: list[str]) -> None:
        for body in bodies:
            await self.dlq.inner.send_raw(body)

    async def run(self, *argv: str) -> int:
        self.out = io.StringIO()
        return await telemetry_dlq.run(
            list(argv), dlq=self.dlq, source=self.source, metrics=self.metrics, out=self.out
        )

    def lines(self) -> list[dict[str, Any]]:
        return [json.loads(line) for line in self.out.getvalue().splitlines() if line.strip()]

    def summary(self) -> dict[str, Any]:
        summaries = [line for line in self.lines() if "command" in line]
        assert len(summaries) == 1, self.out.getvalue()
        return summaries[0]

    def metric_total(self, result: str) -> int:
        return sum(
            record["value"]
            for record in self.metrics.records
            if record["metric"] == REDRIVEN_METRIC and record["labels"] == {"result": result}
        )

    async def all_visible(self) -> list[str]:
        """Every DLQ message is receivable again right now (released, not held)."""
        received = await self.dlq.inner.receive(
            max_messages=10, wait_seconds=0, visibility_seconds=0
        )
        return sorted(message.body for message in received)


@pytest.fixture
def harness() -> Harness:
    return Harness()


# Redrive ---------------------------------------------------------------------------------------


class RedriveSpy:
    def __init__(self, report: RedriveReport) -> None:
        self.report = report
        self.calls: list[dict[str, Any]] = []

    async def __call__(self, **kwargs: Any) -> RedriveReport:
        self.calls.append(kwargs)
        return self.report


@pytest.mark.asyncio
async def test_redrive_delegates_to_the_core_with_dry_run_by_default(harness, monkeypatch) -> None:
    spy = RedriveSpy(RedriveReport(seen=5, redriven=0, failed=0))
    monkeypatch.setattr(telemetry_dlq, "redrive_dlq", spy)

    code = await harness.run("redrive")

    assert code == telemetry_dlq.EXIT_OK == 0
    (call,) = spy.calls
    assert call["dlq"] is harness.dlq and call["source"] is harness.source
    assert call["max_messages"] == 100 and call["apply"] is False
    summary = harness.summary()
    assert summary["command"] == "redrive" and summary["apply"] is False
    assert (summary["seen"], summary["redriven"], summary["failed"]) == (5, 0, 0)
    assert harness.metrics.records == []


@pytest.mark.asyncio
async def test_redrive_apply_passes_max_and_reports_partial(harness, monkeypatch) -> None:
    spy = RedriveSpy(RedriveReport(seen=7, redriven=5, failed=2))
    monkeypatch.setattr(telemetry_dlq, "redrive_dlq", spy)

    code = await harness.run("redrive", "--max", "7", "--apply")

    assert code == telemetry_dlq.EXIT_PARTIAL == 3
    (call,) = spy.calls
    assert call["dlq"] is harness.dlq and call["source"] is harness.source
    assert call["max_messages"] == 7 and call["apply"] is True
    summary = harness.summary()
    assert summary["apply"] is True
    assert (summary["seen"], summary["redriven"], summary["failed"]) == (7, 5, 2)
    assert harness.metric_total("redriven") == 5
    assert harness.metric_total("failed") == 2


@pytest.mark.asyncio
async def test_redrive_apply_moves_every_body_unchanged(harness) -> None:
    await harness.fill(BODIES)

    code = await harness.run("redrive", "--apply")

    assert code == 0
    assert harness.source.bodies() == BODIES
    assert harness.dlq.bodies() == []
    assert harness.summary()["redriven"] == 3
    assert harness.metric_total("redriven") == 3
    assert harness.metric_total("failed") == 0


@pytest.mark.asyncio
async def test_redrive_dry_run_counts_and_releases_without_moving(harness) -> None:
    await harness.fill(BODIES)

    code = await harness.run("redrive")

    assert code == 0
    assert harness.summary()["seen"] == 3 and harness.summary()["redriven"] == 0
    assert harness.source.calls == []
    assert harness.dlq.count("delete") == 0
    assert await harness.all_visible() == sorted(BODIES)


@pytest.mark.asyncio
async def test_redrive_failed_send_keeps_the_message_and_is_partial(harness) -> None:
    await harness.fill(BODIES)
    harness.source.inner.faults.fail_next("send_raw", IngressQueueError("down", retryable=True))

    code = await harness.run("redrive", "--apply")

    assert code == 3
    assert len(harness.dlq.bodies()) == 1
    assert sorted(harness.source.bodies() + harness.dlq.bodies()) == sorted(BODIES)
    assert harness.metric_total("redriven") == 2
    assert harness.metric_total("failed") == 1


@pytest.mark.asyncio
async def test_redrive_queue_error_exits_1_without_leaking(harness) -> None:
    await harness.fill(BODIES)
    harness.faults.fail_next("receive", IngressQueueError(SENTINEL, retryable=True))

    code = await harness.run("redrive", "--apply")

    assert code == telemetry_dlq.EXIT_ERROR == 1
    assert SENTINEL not in harness.out.getvalue()
    assert harness.lines() and "error" in harness.lines()[-1]
    assert harness.source.bodies() == []
    assert len(harness.dlq.bodies()) == 3


@pytest.mark.parametrize("value", ["0", "-3", "10001", "lots"])
@pytest.mark.asyncio
async def test_redrive_rejects_an_out_of_range_max(harness, value) -> None:
    await harness.fill(BODIES)

    assert await harness.run("redrive", "--max", value, "--apply") == 1
    assert harness.dlq.calls == [] and harness.source.calls == []


# Sample ----------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sample_prints_metadata_only_and_releases_every_message(harness) -> None:
    await harness.fill(BODIES)

    code = await harness.run("sample")

    assert code == 0
    lines = [line for line in harness.lines() if "command" not in line]
    assert len(lines) == 3
    assert len({line["message_id"] for line in lines}) == 3
    by_diagnosis = {line["diagnosis"]: line for line in lines}
    assert set(by_diagnosis) == {"valid_envelope", "not_json", "invalid_envelope"}
    valid = by_diagnosis["valid_envelope"]
    assert valid["ingress_id"] == INGRESS
    assert valid["replay"] is False and valid["observations"] == 1
    assert valid["body_bytes"] == len(VALID_BODY.encode("utf-8"))
    assert by_diagnosis["not_json"]["body_bytes"] == len(NOT_JSON_BODY.encode("utf-8"))
    for line in lines:
        assert line["receive_count"] == 1
        assert datetime.fromisoformat(line["sent_at"]).tzinfo is not None
    text = harness.out.getvalue()
    assert SENTINEL not in text
    for message in harness.dlq.inner.messages.values():
        assert message["receipt_handle"] not in text
    summary = harness.summary()
    assert summary["command"] == "sample" and summary["sampled"] == 3
    # Released, never consumed: still on the DLQ and visible again at once.
    assert harness.dlq.count("delete") == 0 and harness.source.calls == []
    assert await harness.all_visible() == sorted(BODIES)
    assert harness.metrics.records == []


@pytest.mark.asyncio
async def test_sample_respects_the_limit_and_counts_each_message_once(harness) -> None:
    await harness.fill([*BODIES, *BODIES])

    assert await harness.run("sample", "--limit", "4") == 0
    lines = [line for line in harness.lines() if "command" not in line]
    assert len(lines) == 4 and len({line["message_id"] for line in lines}) == 4
    assert all(call[1] <= 10 for call in harness.dlq.calls if call[0] == "receive")
    assert all(call[2] > 0 for call in harness.dlq.calls if call[0] == "receive")

    assert await harness.run("sample", "--limit", "10") == 0
    lines = [line for line in harness.lines() if "command" not in line]
    assert len(lines) == 6 and len({line["message_id"] for line in lines}) == 6
    assert harness.dlq.count("delete") == 0
    released = [call[1] for call in harness.dlq.calls if call[0] == "change_visibility"]
    assert all(call[2] == 0 for call in harness.dlq.calls if call[0] == "change_visibility")
    assert len(released) == 4 + 6


@pytest.mark.asyncio
async def test_sample_defaults_to_ten_messages(harness) -> None:
    await harness.fill([*BODIES, *BODIES, *BODIES, *BODIES])  # 12

    assert await harness.run("sample") == 0

    assert len([line for line in harness.lines() if "command" not in line]) == 10
    assert harness.summary()["sampled"] == 10
    assert len(harness.dlq.bodies()) == 12


@pytest.mark.asyncio
async def test_sample_of_an_empty_dlq_is_ok(harness) -> None:
    assert await harness.run("sample") == 0
    summary = harness.summary()
    assert summary["command"] == "sample" and summary["sampled"] == 0


@pytest.mark.asyncio
async def test_sample_releases_what_it_saw_when_a_receive_fails(harness) -> None:
    await harness.fill([*BODIES, *BODIES, *BODIES, *BODIES])  # 12: two receives

    calls = 0
    inner_receive = harness.dlq.inner.receive

    async def flaky_receive(**kwargs: Any) -> list[IngressQueueMessage]:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise IngressQueueError(SENTINEL, retryable=True)
        return await inner_receive(**kwargs)

    harness.dlq.inner.receive = flaky_receive  # type: ignore[method-assign]

    code = await harness.run("sample", "--limit", "12")

    assert code == 1
    assert SENTINEL not in harness.out.getvalue()
    harness.dlq.inner.receive = inner_receive  # type: ignore[method-assign]
    visible = await harness.dlq.inner.receive(max_messages=10, wait_seconds=0, visibility_seconds=0)
    assert len(visible) == 10  # the first batch was released despite the failure


@pytest.mark.parametrize("value", ["0", "-1", "1001", "few"])
@pytest.mark.asyncio
async def test_sample_rejects_an_out_of_range_limit(harness, value) -> None:
    assert await harness.run("sample", "--limit", value) == 1
    assert harness.dlq.calls == []


@pytest.mark.parametrize("argv", [(), ("consume",), ("--apply",)])
@pytest.mark.asyncio
async def test_nothing_touches_the_dlq_without_an_explicit_subcommand(harness, argv) -> None:
    await harness.fill(BODIES)

    assert await harness.run(*argv) == 1

    assert harness.dlq.calls == [] and harness.source.calls == []
    assert len(harness.dlq.bodies()) == 3


# Composition and argv --------------------------------------------------------------------------

SOURCE_URL = "http://sqs.local/000000000000/limnopulse-telemetry-ingress"
DLQ_URL = "http://sqs.local/000000000000/limnopulse-telemetry-ingress-dlq"


class InMemorySqsClient:
    """A boto3 SQS client stand-in: per-URL queues with visibility and receipt handles."""

    def __init__(self) -> None:
        self.queues: dict[str, list[dict[str, Any]]] = {SOURCE_URL: [], DLQ_URL: []}
        self.calls: list[tuple[str, str]] = []

    def put(self, url: str, body: str) -> None:
        self.queues[url].append(
            {"id": uuid4().hex, "body": body, "visible": True, "handle": "", "count": 0}
        )

    def send_message(self, *, QueueUrl: str, MessageBody: str, **_: Any) -> dict[str, Any]:
        self.calls.append(("send_message", QueueUrl))
        self.put(QueueUrl, MessageBody)
        return {"MessageId": self.queues[QueueUrl][-1]["id"]}

    def receive_message(
        self, *, QueueUrl: str, MaxNumberOfMessages: int, VisibilityTimeout: int, **_: Any
    ) -> dict[str, Any]:
        self.calls.append(("receive_message", QueueUrl))
        # A scan must hold what it received; a runaway loop fails instead of hanging the suite.
        assert VisibilityTimeout > 0, "a DLQ scan must hold each message while it runs"
        if sum(1 for name, _ in self.calls if name == "receive_message") > RECEIVE_BUDGET:
            raise AssertionError("the CLI kept receiving: a scan must stop")
        out = []
        for message in self.queues[QueueUrl]:
            if len(out) >= MaxNumberOfMessages:
                break
            if not message["visible"]:
                continue
            message["count"] += 1
            message["handle"] = uuid4().hex
            message["visible"] = VisibilityTimeout == 0
            out.append(
                {
                    "MessageId": message["id"],
                    "ReceiptHandle": message["handle"],
                    "Body": message["body"],
                    "Attributes": {
                        "ApproximateReceiveCount": str(message["count"]),
                        "SentTimestamp": str(
                            int(datetime(2026, 10, 30, tzinfo=UTC).timestamp() * 1000)
                        ),
                    },
                }
            )
        return {"Messages": out}

    def delete_message(self, *, QueueUrl: str, ReceiptHandle: str) -> dict[str, Any]:
        self.calls.append(("delete_message", QueueUrl))
        self.queues[QueueUrl] = [m for m in self.queues[QueueUrl] if m["handle"] != ReceiptHandle]
        return {}

    def change_message_visibility(
        self, *, QueueUrl: str, ReceiptHandle: str, VisibilityTimeout: int
    ) -> dict[str, Any]:
        self.calls.append(("change_message_visibility", QueueUrl))
        for message in self.queues[QueueUrl]:
            if message["handle"] == ReceiptHandle:
                message["visible"] = VisibilityTimeout == 0
        return {}


def _settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "sqs_telemetry_ingress_queue_url": SOURCE_URL,
        "sqs_telemetry_ingress_dlq_url": DLQ_URL,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


async def _main(argv: list[str], **kwargs: Any) -> int:
    """`main` runs its own event loop, so the async tests call it from a worker thread."""
    return await asyncio.to_thread(telemetry_dlq.main, argv, **kwargs)


@pytest.mark.asyncio
async def test_main_redrives_from_the_dlq_url_to_the_source_url() -> None:
    client = InMemorySqsClient()
    for body in BODIES:
        client.put(DLQ_URL, body)
    out, err = io.StringIO(), io.StringIO()

    code = await _main(
        ["redrive", "--apply"], settings=_settings(), sqs_client=client, out=out, err=err
    )

    assert code == 0
    assert [m["body"] for m in client.queues[SOURCE_URL]] == BODIES
    assert client.queues[DLQ_URL] == []
    assert {url for name, url in client.calls if name == "send_message"} == {SOURCE_URL}
    assert {url for name, url in client.calls if name == "receive_message"} == {DLQ_URL}
    # The operator policy (P3-02 IAM): Receive/Delete/ChangeVisibility/GetQueueAttributes on the
    # DLQ and SendMessage on the ingress queue, nothing else.
    dlq_operations = {"receive_message", "delete_message", "change_message_visibility"}
    dlq_operations.add("get_queue_attributes")
    assert {name for name, url in client.calls if url == DLQ_URL} <= dlq_operations
    assert {name for name, url in client.calls if url == SOURCE_URL} <= {"send_message"}
    metrics = [json.loads(line) for line in err.getvalue().splitlines() if line.strip()]
    assert metrics, "the default sink writes one JSON object per increment to stderr"
    for record in metrics:
        assert set(record) == {"metric", "value", "labels"}
        assert record["metric"] == REDRIVEN_METRIC
    assert sum(r["value"] for r in metrics if r["labels"] == {"result": "redriven"}) == 3
    assert SENTINEL not in out.getvalue() + err.getvalue()


@pytest.mark.asyncio
async def test_main_samples_through_sqs_and_releases() -> None:
    client = InMemorySqsClient()
    for body in BODIES:
        client.put(DLQ_URL, body)
    out = io.StringIO()

    code = await _main(
        ["sample"],
        settings=_settings(),
        sqs_client=client,
        metrics=RecordingTelemetryMetrics(),
        out=out,
        err=io.StringIO(),
    )

    assert code == 0
    assert len([line for line in out.getvalue().splitlines() if "message_id" in line]) == 3
    assert all(message["visible"] for message in client.queues[DLQ_URL])
    assert not any(name in {"send_message", "delete_message"} for name, _ in client.calls)
    assert SENTINEL not in out.getvalue()


@pytest.mark.parametrize(
    "overrides",
    [{"sqs_telemetry_ingress_dlq_url": ""}, {"sqs_telemetry_ingress_queue_url": ""}],
)
@pytest.mark.asyncio
async def test_main_needs_both_queue_urls(overrides) -> None:
    client = InMemorySqsClient()
    client.put(DLQ_URL, VALID_BODY)

    code = await _main(
        ["redrive", "--apply"],
        settings=_settings(**overrides),
        sqs_client=client,
        metrics=RecordingTelemetryMetrics(),
        out=io.StringIO(),
        err=io.StringIO(),
    )

    assert code == 1
    assert client.calls == []


@pytest.mark.parametrize(
    "argv",
    [
        ("sample", "--bogus", SENTINEL),
        ("sample", "--limit", SENTINEL),
        ("redrive", "--max", SENTINEL),
        (SENTINEL,),
    ],
)
def test_usage_errors_exit_1_without_echoing_argv(argv, capsys) -> None:
    out, err = io.StringIO(), io.StringIO()
    client = InMemorySqsClient()

    code = telemetry_dlq.main(
        list(argv),
        settings=_settings(),
        sqs_client=client,
        metrics=RecordingTelemetryMetrics(),
        out=out,
        err=err,
    )

    assert code == 1
    assert client.calls == []
    captured = capsys.readouterr()
    assert SENTINEL not in out.getvalue() + err.getvalue() + captured.out + captured.err


def test_main_builds_its_own_sqs_client_from_settings(monkeypatch) -> None:
    built: list[dict[str, Any]] = []
    client = InMemorySqsClient()

    def factory(service: str, **kwargs: Any) -> Any:
        built.append({"service": service, **kwargs})
        return client

    monkeypatch.setattr(telemetry_dlq.boto3, "client", factory)

    code = telemetry_dlq.main(
        ["sample"],
        settings=_settings(aws_region="sa-east-1", sqs_endpoint_url="http://sqs.local:9324"),
        metrics=RecordingTelemetryMetrics(),
        out=io.StringIO(),
        err=io.StringIO(),
    )

    assert code == 0
    assert [item["service"] for item in built] == ["sqs"]
    assert built[0]["region_name"] == "sa-east-1"
    assert built[0]["endpoint_url"] == "http://sqs.local:9324"
