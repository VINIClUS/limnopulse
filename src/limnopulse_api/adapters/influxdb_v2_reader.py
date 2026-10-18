"""InfluxDB reader for canonical `observation_numeric` points (P2-04).

Every literal built from a runtime value goes through `flux_string_literal`, and every ID is
validated before a query is built. Windows are half-open by `observed_at`.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from anyio import to_thread

from limnopulse_api.core.errors import TelemetryQueryError
from limnopulse_api.domain.ids import require_safe_id
from limnopulse_api.domain.metric_catalog import metric_definition
from limnopulse_api.domain.telemetry_v2 import (
    ABSENT_SEQUENCE,
    ABSENT_STRING,
    QualityState,
    StoredObservation,
)
from limnopulse_api.domain.telemetry_v2_schema import OBSERVATION_MEASUREMENT, flux_string_literal
from limnopulse_api.domain.v2_common import format_utc_timestamp
from limnopulse_api.repositories.telemetry_v2 import ObservationSelector

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_NANOSECONDS_PER_MICROSECOND = 1000
_SELECTOR_TAGS = {"asset": "asset_id", "component": "component_id", "device": "device_id"}


class InfluxObservationReader:
    def __init__(self, *, query_api: Any, org: str, bucket: str) -> None:
        self.query_api = query_api
        self.org = org
        self.bucket = bucket

    async def query_observations(
        self,
        *,
        tenant_id: str,
        selector: ObservationSelector,
        metric_keys: frozenset[str] | None,
        quality_states: frozenset[QualityState] | None,
        start: datetime,
        stop: datetime,
        limit: int,
    ) -> list[StoredObservation]:
        lines = self._pivoted_points(tenant_id, selector, metric_keys, start=start, stop=stop)
        if quality_states is not None:
            # quality_state is a field (D5), so it is compared on the pivoted row.
            states = sorted(str(QualityState(state)) for state in quality_states)
            lines.append(_contains_filter("quality_state", states))
        lines += [
            "  |> group()",
            '  |> sort(columns: ["_time", "component_id", "metric_key"])',
            f"  |> limit(n: {int(limit)})",
        ]
        return await self._run("\n".join(lines))

    async def query_latest_observations(
        self,
        *,
        tenant_id: str,
        selector: ObservationSelector,
        metric_keys: frozenset[str] | None,
        lookback: timedelta,
        now: datetime,
    ) -> list[StoredObservation]:
        lines = self._pivoted_points(
            tenant_id, selector, metric_keys, start=now - lookback, stop=now
        )
        # Latest by observed_at only; receipt and ingest times never decide (D11).
        lines += [
            '  |> group(columns: ["component_id", "metric_key"])',
            '  |> sort(columns: ["_time"])',
            '  |> last(column: "_time")',
            "  |> group()",
            '  |> sort(columns: ["component_id", "metric_key"])',
        ]
        return await self._run("\n".join(lines))

    def _pivoted_points(
        self,
        tenant_id: str,
        selector: ObservationSelector,
        metric_keys: frozenset[str] | None,
        *,
        start: datetime,
        stop: datetime,
    ) -> list[str]:
        require_safe_id(tenant_id)
        lines = [
            f"from(bucket: {flux_string_literal(self.bucket)})",
            f"  |> range(start: {_flux_time(start)}, stop: {_flux_time(stop)})",
            _equals_filter("_measurement", OBSERVATION_MEASUREMENT),
            _equals_filter("tenant_id", tenant_id),
            _equals_filter(_SELECTOR_TAGS[selector.kind], selector.entity_id),
        ]
        if metric_keys is not None:
            for metric_key in metric_keys:
                if metric_definition(metric_key) is None:
                    raise ValueError(f"metric_key {metric_key!r} is not in the metric catalog")
            lines.append(_contains_filter("metric_key", sorted(metric_keys)))
        # Each table is one series, so `_time` alone keys one row per point.
        lines.append('  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")')
        return lines

    async def _run(self, query: str) -> list[StoredObservation]:
        try:
            tables = await to_thread.run_sync(self.query_api.query, query, self.org)
        except Exception as exc:
            raise TelemetryQueryError("observation query failed") from exc
        try:
            return [_stored_observation(record.values) for record in _records(tables)]
        except (KeyError, TypeError, ValueError) as exc:
            raise TelemetryQueryError("stored observation could not be decoded") from exc


def _records(tables: Iterable[Any]) -> Iterable[Any]:
    for table in tables:
        yield from table.records


def _flux_time(value: datetime) -> str:
    return f"time(v: {flux_string_literal(format_utc_timestamp(value))})"


def _equals_filter(column: str, value: str) -> str:
    return f'  |> filter(fn: (r) => r["{column}"] == {flux_string_literal(value)})'


def _contains_filter(column: str, values: Iterable[str]) -> str:
    literals = ", ".join(flux_string_literal(value) for value in values)
    return f'  |> filter(fn: (r) => contains(value: r["{column}"], set: [{literals}]))'


def _datetime_from_ns(value: int) -> datetime:
    # Integer division only: a float conversion loses microseconds near MAX_TELEMETRY_TIME.
    return _EPOCH + timedelta(microseconds=int(value) // _NANOSECONDS_PER_MICROSECOND)


def _optional_string(value: object) -> str | None:
    if value is None or value == ABSENT_STRING:
        return None
    return str(value)


def _sequence(value: object) -> int | None:
    if value is None or value == ABSENT_SEQUENCE:
        return None
    return int(value)  # type: ignore[call-overload]


def _stored_observation(values: Mapping[str, Any]) -> StoredObservation:
    metric_key = values["metric_key"]
    # The unit is resolved under the catalog version the point was normalized with (ADR-005).
    catalog_version = values["catalog_version"]
    definition = metric_definition(metric_key, version=catalog_version)
    if definition is None:
        raise ValueError(f"stored metric_key {metric_key!r} is not in {catalog_version}")
    return StoredObservation(
        event_id=values["event_id"],
        observed_at=values["_time"],
        received_at=_datetime_from_ns(values["received_at_ns"]),
        ingested_at=_datetime_from_ns(values["ingested_at_ns"]),
        metric_key=metric_key,
        value=float(values["value"]),
        canonical_unit=definition.canonical_unit,
        site_id=values["site_id"],
        asset_id=values["asset_id"],
        device_id=values["device_id"],
        component_id=values["component_id"],
        probe_id=_optional_string(values.get("probe_id")),
        deployment_id=values["deployment_id"],
        quality_state=values["quality_state"],
        quality_flags=tuple(json.loads(values["quality_flags_json"])),
        quality_provenance=tuple(json.loads(values["quality_provenance_json"])),
        sequence=_sequence(values.get("sequence")),
        source_type=values["source_type"],
    )
