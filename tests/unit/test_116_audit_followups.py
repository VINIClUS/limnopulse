"""#116 audit follow-ups: the non-blocking findings of the AUDIT APPROVED comment on #128 (new
file, so no frozen test changes). Each test pins a mutant the audit found surviving.

- Finding 1: the quarantine lookahead keeps the cursor when a short last read still shows a row.
- Finding 2: an InfluxDB `close()` that raises never changes the worker's exit code.
- Finding 3: an exception's text never reaches the ingress `_unavailable` warning or the worker's
  crash and composition logs; only its category does.
- Finding 4: the EXTERNAL_ID lookup row is fenced on the exact version, and a second ingest-on
  `create_app` adds no second metrics handler.
- Docs: the P2 rollback recreates instead of restarting, and P3 DLQ step 5 covers a dry run that
  exits 3 with an `error` category.
"""

from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path
from typing import Any, ClassVar

import pytest
from botocore.exceptions import ClientError
from fastapi.testclient import TestClient

from limnopulse_api.adapters.dynamodb_integrations import external_identity_lookup_to_item
from limnopulse_api.adapters.telemetry_metrics_log import METRICS_LOGGER
from limnopulse_api.core.errors import ConflictError, IngressQueueError
from limnopulse_api.domain.ingestion import QuarantineEntryStatus
from limnopulse_api.domain.integrations import DeviceIntegrationStatus
from limnopulse_api.workers import telemetry_ingest
from tests.contracts.test_p2_acceptance_gate import markdown_section
from tests.support.ingestion_contract_kits import (
    AUDIT,
    RECEIVED,
    TENANT,
    device_integration,
    fenced_entries,
)
from tests.unit.test_116_review_regressions import QUEUE_URL, _ingest_app
from tests.unit.test_dynamodb_integration_resolver import DynamoIngestionSubject, fake_subject
from tests.unit.test_dynamodb_quarantine import DynamoQuarantineSubject
from tests.unit.test_ingress_service import BEARER, IngestRejected, Ingress

ROOT = Path(__file__).resolve().parents[2]
SENTINEL = "SENTINEL-116-audit-must-not-leak"


def _sentinel_store_error(operation: str = "GetItem") -> ClientError:
    return ClientError({"Error": {"Code": "InternalServerError", "Message": SENTINEL}}, operation)


# Finding 1: the quarantine lookahead -------------------------------------------------------------


async def two_page_listing(repository: Any) -> None:
    """Rows 0-4 with rows 0 and 1 dismissed, listed `status=open, limit=2`. The second Query
    reads rows 3 and 4 and ends the partition, but row 4 still shows, so page 1 is [2, 3] with a
    cursor and page 2 is [4] without one. Without the lookahead, row 4 is never listed."""
    await repository.put_quarantine_entries(fenced_entries(4))
    ordered = (
        await repository.list_quarantine_entries(TENANT, after_sk=None, limit=10)
    ).entries
    assert len(ordered) == 5
    for index in (0, 1):
        await repository.transition_quarantine_entry(
            TENANT,
            ordered[index].entry_key,
            QuarantineEntryStatus.DISMISSED,
            expected_version=1,
            at=RECEIVED,
        )

    first = await repository.list_quarantine_entries(
        TENANT, status=QuarantineEntryStatus.OPEN, after_sk=None, limit=2
    )
    assert [entry.entry_key for entry in first.entries] == [
        ordered[2].entry_key,
        ordered[3].entry_key,
    ]
    assert first.next_after_sk == ordered[3].entry_key
    rest = await repository.list_quarantine_entries(
        TENANT, status=QuarantineEntryStatus.OPEN, after_sk=first.next_after_sk, limit=2
    )
    assert [entry.entry_key for entry in rest.entries] == [ordered[4].entry_key]
    assert rest.next_after_sk is None


@pytest.mark.asyncio
async def test_a_short_last_read_that_still_shows_a_row_keeps_the_cursor() -> None:
    await two_page_listing(DynamoQuarantineSubject().repository)


# Finding 2 and finding 3 (worker): exit codes and log categories ---------------------------------


class _Influx:
    instances: ClassVar[list[_Influx]] = []
    close_error: ClassVar[BaseException | None] = None

    def __init__(self, **kwargs: Any) -> None:
        self.close_calls = 0
        _Influx.instances.append(self)

    def write_api(self, *args: Any, **kwargs: Any) -> object:
        return object()

    def close(self) -> None:
        self.close_calls += 1
        if _Influx.close_error is not None:
            raise _Influx.close_error


class _Loop:
    def __init__(self, error: BaseException | None) -> None:
        self.error = error

    async def run(self, stop: asyncio.Event) -> None:
        if self.error is not None:
            raise self.error


def _run_worker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    crash: BaseException | None = None,
    composition: BaseException | None = None,
    close_error: BaseException | None = None,
) -> int:
    monkeypatch.chdir(tmp_path)  # no developer `.env`
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("TELEMETRY_V2_WRITE_ENABLED", "true")
    monkeypatch.setenv("SQS_TELEMETRY_INGRESS_QUEUE_URL", QUEUE_URL)
    monkeypatch.setattr(telemetry_ingest, "InfluxDBClient", _Influx)
    monkeypatch.setattr(_Influx, "instances", [])
    monkeypatch.setattr(_Influx, "close_error", close_error)

    def build(settings: Any, *, influx_client_factory: Any, **kwargs: Any) -> Any:
        influx_client_factory(url="http://influx.invalid", token="t", org="o")
        if composition is not None:
            raise composition
        return _Loop(crash)

    monkeypatch.setattr(telemetry_ingest, "build_worker", build)
    return telemetry_ingest.main()


def _worker_lines(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == "limnopulse.telemetry.ingest" and record.levelno >= logging.WARNING
    ]


@pytest.mark.parametrize(
    ("crash", "code"),
    [(None, 0), (RuntimeError("crash"), 1)],
    ids=["stop", "crash"],
)
def test_a_close_that_raises_never_changes_the_worker_exit_code(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    crash: BaseException | None,
    code: int,
) -> None:
    """#116 audit finding 2: a stop stays exit 0 (no `restart: on-failure` restart of a worker
    stopped on purpose), and the close failure is logged by category only."""
    caplog.set_level(logging.INFO)

    assert _run_worker(monkeypatch, tmp_path, crash=crash, close_error=OSError(SENTINEL)) == code

    assert [client.close_calls for client in _Influx.instances] == [1]
    assert "influx client close failed (OSError)" in _worker_lines(caplog)
    assert SENTINEL not in caplog.text


@pytest.mark.parametrize("where", ["crash", "composition"])
def test_the_worker_logs_a_crash_or_composition_error_by_category_only(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    where: str,
) -> None:
    """#116 audit finding 3: the error's text (which can quote an endpoint, a key or a body)
    never reaches the worker's log; its type does."""
    caplog.set_level(logging.INFO)
    error = RuntimeError(SENTINEL)

    code = _run_worker(monkeypatch, tmp_path, **{where: error})

    assert code == 1
    expected = {
        "crash": "telemetry ingest worker crashed (RuntimeError)",
        "composition": "telemetry ingest worker could not be composed (RuntimeError)",
    }[where]
    assert expected in _worker_lines(caplog)
    assert SENTINEL not in caplog.text


# Finding 3 (ingress): `_unavailable` logs the category only ------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "step", "error"),
    [
        ("get_ingress_receipt", "receipt lookup", _sentinel_store_error()),
        ("admit", "admission", _sentinel_store_error()),
        ("allows", "entitlement check", RuntimeError(SENTINEL)),
        ("claim_ingress_receipt", "receipt claim", _sentinel_store_error("PutItem")),
        ("send", "queue send", IngressQueueError(SENTINEL, retryable=True)),
    ],
    ids=["receipt_lookup", "admission", "entitlement", "claim", "send"],
)
async def test_the_unavailable_warning_carries_the_category_never_the_text(
    caplog: pytest.LogCaptureFixture, operation: str, step: str, error: BaseException
) -> None:
    caplog.set_level(logging.DEBUG)
    ingress = Ingress()
    await ingress.seed()
    ingress.subject.fail_next(operation, error)

    with pytest.raises(IngestRejected):
        await ingress.admit()

    assert f"ingest {step} failed: {type(error).__name__}" in caplog.text
    assert SENTINEL not in caplog.text


@pytest.mark.asyncio
async def test_an_authentication_store_error_is_logged_by_category_only(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    ingress = Ingress()
    await ingress.seed()
    ingress.subject.fail_next("authenticate", _sentinel_store_error())

    with pytest.raises(IngestRejected):
        await ingress.service.authenticate(BEARER)

    assert "ingest authentication failed: ClientError" in caplog.text
    assert SENTINEL not in caplog.text


# Finding 4: the lookup-row fence and the metrics-handler dedupe ----------------------------------


@pytest.fixture
def dynamo_lifecycle() -> DynamoIngestionSubject:
    """The real adapter on the unit Dynamo fake; the DynamoDB Local twin overrides it."""
    return fake_subject()


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [-1, 1], ids=["behind", "ahead"])
async def test_a_drifted_lookup_row_version_fails_the_transition(
    dynamo_lifecycle: DynamoIngestionSubject, drift: int
) -> None:
    """#116 audit finding 4: the EXTERNAL_ID lookup row is fenced on the exact version (`=`, not
    `>=` or `<=`), so a lookup row that drifted from its DeviceIntegration stops the move."""
    integration = device_integration()
    await dynamo_lifecycle.seed_device_integration(integration)
    keys = dynamo_lifecycle.integrations.keys
    dynamo_lifecycle.put(
        {**external_identity_lookup_to_item(integration, keys), "version": 1 + drift}
    )

    with pytest.raises(ConflictError):
        await dynamo_lifecycle.repository.transition_device_integration(
            TENANT,
            integration.device_integration_id,
            DeviceIntegrationStatus.ERROR,
            expected_version=1,
            updated_at=RECEIVED,
            audit=AUDIT,
        )

    row = dynamo_lifecycle.item(keys.device_integration(TENANT, integration.device_integration_id))
    assert row is not None and (row["status"], row["version"]) == ("active", 1)
    assert dynamo_lifecycle.audit_items() == []


def test_a_second_ingest_on_app_adds_no_second_metrics_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#116 audit finding 4: `_enable_metrics_logging` runs on every ingest-on app; its guard
    keeps one handler, or every metric line would print once per app."""
    logger = logging.getLogger(METRICS_LOGGER)
    monkeypatch.setattr(logger, "handlers", [])

    for _ in range(2):
        with TestClient(_ingest_app(monkeypatch, {})):
            pass

    owned = [h for h in logger.handlers if getattr(h, "_limnopulse_metrics", False)]
    assert len(owned) == 1


# Docs ------------------------------------------------------------------------------------------


def _section(path: str, name: str) -> str:
    return " ".join(markdown_section((ROOT / path).read_text(encoding="utf-8"), name).split())


def test_the_p2_rollback_recreates_instead_of_restarting() -> None:
    """`docker compose restart` keeps a container's old environment, so a switch rollback that
    only restarts leaves the switches on (the README P2 rollback already says recreate)."""
    text = _section("docs/p2-telemetry-rollout-runbook.md", "Rollback")
    assert "restart (or redeploy)" not in text
    assert re.search(r"recreate", text) and re.search(r"`docker compose up -d", text)
    assert re.search(r"`docker compose restart` keeps the old environment", text)


def test_the_dlq_drill_says_a_dry_run_can_exit_3_with_an_error_category() -> None:
    """#116 audit finding 5: since #128 a redrive that stops midway exits 3 in both modes."""
    text = _section("docs/p3-ingestion-rollout-runbook.md", "DLQ drill")
    assert re.search(r"dry run.{0,160}exits? 3|exit code 3.{0,240}dry run", text, re.IGNORECASE)
    assert re.search(r"`error`", text)
    for category in ("queue_unavailable", "unexpected", "interrupted"):
        assert f"`{category}`" in text, category
