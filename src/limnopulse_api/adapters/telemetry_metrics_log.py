"""Structured-log `TelemetryMetrics` adapter (P3-08, D14).

Each `increment`/`observe` writes one compact JSON object, `{"metric", "value", "labels"}`, at
INFO on the logger `limnopulse.telemetry.metrics`, shaped so an OTLP adapter can follow without
renaming. Names and labels are checked against the closed table before anything is written, so
an open label (a tenant, device or identity) raises `ValueError` and never reaches a log line.
"""

import json
import logging

from limnopulse_api.domain.ingestion import require_metric_labels

METRICS_LOGGER = "limnopulse.telemetry.metrics"


class StructuredLogTelemetryMetrics:
    """Implements `TelemetryMetrics`."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger(METRICS_LOGGER)

    def increment(self, name: str, value: int = 1, **labels: str) -> None:
        self._emit(name, value, labels)

    def observe(self, name: str, value: float, **labels: str) -> None:
        self._emit(name, value, labels)

    def _emit(self, name: str, value: float, labels: dict[str, str]) -> None:
        require_metric_labels(name, labels)
        line = json.dumps(
            {"metric": name, "value": value, "labels": labels},
            separators=(",", ":"),
            allow_nan=False,
        )
        self._logger.info("%s", line)
