"""P3-08: `StructuredLogTelemetryMetrics` — one JSON object per emission on the metrics logger,
names and labels from the closed table only (the P3-00 `TELEMETRY_METRICS_CONTRACT`)."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from limnopulse_api.adapters.telemetry_metrics_log import StructuredLogTelemetryMetrics
from limnopulse_api.domain.ingestion import INGEST_METRICS
from limnopulse_api.repositories.ingestion import TelemetryMetrics
from tests.support.ingestion_contract_kits import (
    ACCOUNT,
    DEVICE,
    TELEMETRY_METRICS_CONTRACT,
    TENANT,
    kit_ids,
)

LOGGER = "limnopulse.telemetry.metrics"


class LogMetricsSubject:
    """Kit subject: `recorded()` parses the JSON lines the adapter wrote on its logger."""

    def __init__(self, caplog: pytest.LogCaptureFixture) -> None:
        self.caplog = caplog
        self.metrics = StructuredLogTelemetryMetrics()

    def records(self) -> list[logging.LogRecord]:
        return [record for record in self.caplog.records if record.name == LOGGER]

    def recorded(self) -> list[dict[str, Any]]:
        return [json.loads(record.getMessage()) for record in self.records()]


@pytest.fixture
def subject(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> Iterator[LogMetricsSubject]:
    caplog.set_level(logging.DEBUG)
    # #116: the API sets `propagate=False` on this logger, so after any ingest-on app in the same
    # process caplog's root handler sees nothing; capture on the logger itself, exactly once.
    logger = logging.getLogger(LOGGER)
    monkeypatch.setattr(logger, "propagate", False)
    logger.addHandler(caplog.handler)
    try:
        yield LogMetricsSubject(caplog)
    finally:
        logger.removeHandler(caplog.handler)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario", TELEMETRY_METRICS_CONTRACT, ids=kit_ids(TELEMETRY_METRICS_CONTRACT)
)
async def test_metrics_kit_on_the_structured_log_adapter(
    subject: LogMetricsSubject, scenario: Callable[[Any], Any]
) -> None:
    await scenario(subject)


def test_the_adapter_implements_the_port() -> None:
    assert isinstance(StructuredLogTelemetryMetrics(), TelemetryMetrics)


def test_each_emission_is_one_info_line_with_exactly_three_keys(
    subject: LogMetricsSubject,
) -> None:
    subject.metrics.increment("ingress_fenced_total", reason="account_revoked")
    subject.metrics.observe("ingress_normalization_duration_seconds", 0.25)

    records = subject.records()
    assert len(records) == 2
    assert {record.levelno for record in records} == {logging.INFO}
    for record in records:
        payload = json.loads(record.getMessage())
        assert set(payload) == {"metric", "value", "labels"}
        assert record.exc_info is None
    assert subject.recorded() == [
        {"metric": "ingress_fenced_total", "value": 1, "labels": {"reason": "account_revoked"}},
        {"metric": "ingress_normalization_duration_seconds", "value": 0.25, "labels": {}},
    ]


def test_the_line_is_a_single_compact_json_object(subject: LogMetricsSubject) -> None:
    subject.metrics.increment(
        "ingress_messages_total", 2, outcome="retry", category="store_unavailable"
    )

    (record,) = subject.records()
    text = record.getMessage()
    assert "\n" not in text
    assert json.loads(text) == {
        "metric": "ingress_messages_total",
        "value": 2,
        "labels": {"outcome": "retry", "category": "store_unavailable"},
    }


@pytest.mark.parametrize("name", sorted(INGEST_METRICS))
def test_every_closed_name_is_accepted_with_its_closed_labels(
    subject: LogMetricsSubject, name: str
) -> None:
    labels = {label: min(values) for label, values in INGEST_METRICS[name].items()}
    emit = subject.metrics.observe if name.endswith("_seconds") else subject.metrics.increment

    emit(name, 1, **labels)

    assert subject.recorded() == [{"metric": name, "value": 1, "labels": labels}]


@pytest.mark.parametrize(
    "call",
    [
        lambda m: m.increment("ingress_fenced_total", reason="account_revoked", tenant_id=TENANT),
        lambda m: m.increment("ingress_fenced_total", reason=DEVICE),
        lambda m: m.increment("ingress_fenced_total"),
        lambda m: m.observe("ingress_queue_age_seconds", 1.0, integration_account_id=ACCOUNT),
        lambda m: m.increment("ingress_unknown_total"),
        lambda m: m.observe("ingress_messages_seconds", 1.0),
    ],
    ids=[
        "extra-label",
        "open-value",
        "missing-label",
        "identifier-label",
        "unknown",
        "unknown-obs",
    ],
)
def test_open_names_and_labels_raise_and_log_nothing(
    subject: LogMetricsSubject, call: Callable[[Any], Any]
) -> None:
    with pytest.raises(ValueError):
        call(subject.metrics)

    for record in subject.caplog.records:
        text = record.getMessage()
        assert TENANT not in text and DEVICE not in text and ACCOUNT not in text
    assert subject.records() == []


def test_the_adapter_writes_on_its_own_logger_only(subject: LogMetricsSubject) -> None:
    subject.metrics.increment("ingress_observations_total", 4, result="accepted")

    assert [record.name for record in subject.caplog.records] == [LOGGER]
