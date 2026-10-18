"""`/v1` telemetry read projection over canonical `observation_numeric` points (P2-07).

`V2ProjectedTelemetryRepository` implements the existing `TelemetryRepository` so that, for data
dual-written from legacy default Components, the `/v1` readings and latest shapes equal the
legacy adapter's, and summaries match it with float means within `rel_tol=1e-12` (D10, r7.2).

Flux only reads (InfluxDB 2.7.11 constraints, verified by the equivalence suite):
- filters are `==`/`or` chains, which storage pushes down (`contains()` is not pushed down);
- there is no per-row `map`: a vectorized `map` emits columns in a varying order and makes a
  later `pivot` panic, and a row-based one is slow;
- the `seq` winner of a point is ranked per series with `sort(desc) |> unique(column: "_time")`,
  never with one table per timestamp.
Python renames `metric_key` to the legacy field and the Asset to its pond, and reproduces the
legacy per-series `sort`/`limit`/`last()` semantics. A legacy series is one (tenant, pond,
device): `source` and `topic` are constant per device (D8).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from anyio import to_thread

from limnopulse_api.core.errors import TelemetryQueryError
from limnopulse_api.domain.ids import (
    asset_id_for_legacy_pond,
    default_component_id_for_legacy_device,
    require_safe_id,
)
from limnopulse_api.domain.metric_catalog import metric_catalog
from limnopulse_api.domain.telemetry import (
    LatestMetrics,
    MetricsSummary,
    MetricStatistics,
    SummaryPoint,
    TelemetryReading,
    validate_flux_time_bound,
)
from limnopulse_api.domain.telemetry_v2 import ABSENT_SEQUENCE
from limnopulse_api.domain.telemetry_v2_schema import OBSERVATION_MEASUREMENT, flux_string_literal
from limnopulse_api.domain.v2_common import format_utc_timestamp

_LEGACY_FIELD_BY_METRIC: Mapping[str, str] = {
    d.metric_key: d.legacy_field for d in metric_catalog() if d.legacy_field is not None
}
_SUMMARY_FIELDS = ("do_mg_l", "ph", "temp_c")
_SUMMARY_METRICS = tuple(
    metric for metric, legacy in _LEGACY_FIELD_BY_METRIC.items() if legacy in _SUMMARY_FIELDS
)
_SUMMARY_INTERVALS = {"24h": "5m", "7d": "1h", "30d": "1h"}
_INTEGER_FIELDS = frozenset({"rssi"})
_RANK_FIELDS = ("sequence", "ingested_at_ns", "event_id")
# A legacy series in `observation_numeric` terms: the pond is a function of the Asset.
_SERIES_KEY = ("tenant_id", "asset_id", "device_id")


@dataclass
class _Series:
    """One legacy series, a (pond, device) of the tenant, with its fields per `_time`."""

    pond_id: str
    device_id: str
    rows: dict[datetime, dict[str, float | int]] = field(default_factory=dict)


class V2ProjectedTelemetryRepository:
    def __init__(self, *, query_api: Any, org: str, bucket: str, domain_repository: Any) -> None:
        self.query_api = query_api
        self.org = org
        self.bucket = bucket
        self.domain_repository = domain_repository

    async def query_summary(self, *, tenant_id: str, pond_id: str, period: str) -> MetricsSummary:
        if period not in _SUMMARY_INTERVALS:
            raise ValueError("invalid period")
        interval = _SUMMARY_INTERVALS[period]
        ponds = self._pond_scope(tenant_id, pond_id)
        components = await self._components(tenant_id)
        tables: Iterable[Any] = []
        if components:
            base = self._source(
                tenant_id, ponds, components, f"start: -{period}", metrics=_SUMMARY_METRICS
            )
            # The legacy summary shape over the v2 rows: filters and group are pushed down
            # (`ReadGroup`), the aggregates run in the Flux engine, no raw row reaches the client.
            query = "\n".join(
                [
                    f"data = {base.lstrip()}",
                    '  |> filter(fn: (r) => r["_field"] == "value")',
                    '  |> group(columns: ["metric_key"])',
                    (
                        f"data |> aggregateWindow(every: {interval}, fn: mean, createEmpty: false)"
                        ' |> yield(name: "series")'
                    ),
                    'data |> mean() |> yield(name: "mean")',
                    'data |> min() |> yield(name: "min")',
                    'data |> max() |> yield(name: "max")',
                    'data |> count() |> yield(name: "count")',
                ]
            )
            try:
                tables = await self._query(query)
            except Exception as exc:
                raise TelemetryQueryError("summary query failed") from exc
        return _summary_from_tables(
            tables, tenant_id=tenant_id, pond_id=pond_id, period=period, interval=interval
        )

    async def query_readings(
        self,
        *,
        tenant_id: str,
        pond_id: str,
        start: str,
        stop: str | None,
        limit: int,
    ) -> list[TelemetryReading]:
        range_args = _range_args(start=start, stop=stop)
        ponds = self._pond_scope(tenant_id, pond_id)
        components = await self._components(tenant_id)
        if not components:
            return []
        limit = int(limit)
        query = "\n".join(
            [
                self._source(tenant_id, ponds, components, range_args),
                # One row per (series, _time) with the metrics as columns; legacy `limit` is per
                # series table.
                '  |> filter(fn: (r) => r["_field"] == "value")',
                f"  |> group(columns: {_flux_columns(_SERIES_KEY)})",
                '  |> pivot(rowKey: ["_time"], columnKey: ["metric_key"], valueColumn: "_value")',
                '  |> sort(columns: ["_time"])',
                f"  |> limit(n: {limit})",
            ]
        )
        series = _series_from_rows(await self._query(query), ponds)
        times = [measured_at for item in series for measured_at in item.rows]
        if times:
            # The `seq` winners only over the returned rows' span, so the cost follows `limit`.
            await self._merge_seq(tenant_id, ponds, components, series, min(times), max(times))
        readings = []
        for item in _ordered(series):
            for measured_at in sorted(item.rows):
                readings.append(
                    TelemetryReading(
                        measured_at=measured_at,
                        tenant_id=tenant_id,
                        pond_id=item.pond_id,
                        device_id=item.device_id,
                        **item.rows[measured_at],
                    )
                )
        return readings

    async def query_latest_metrics(self, *, tenant_id: str, pond_id: str) -> LatestMetrics:
        latest = await self._latest_by_pond(tenant_id, self._pond_scope(tenant_id, pond_id))
        return latest.get(pond_id, LatestMetrics(tenant_id=tenant_id, pond_id=pond_id))

    async def query_latest_metrics_for_tenant(self, *, tenant_id: str) -> list[LatestMetrics]:
        require_safe_id(tenant_id)
        ponds = {
            asset_id_for_legacy_pond(tenant_id, pond.pond_id): pond.pond_id
            for pond in await self.domain_repository.list_ponds(tenant_id)
            # A pond ID that is not a safe ID is never dual-written (D7), so it has no data.
            if _is_safe_id(pond.pond_id)
        }
        latest = await self._latest_by_pond(tenant_id, ponds)
        return [latest[pond_id] for pond_id in sorted(latest)]

    async def _latest_by_pond(
        self, tenant_id: str, ponds: Mapping[str, str]
    ) -> dict[str, LatestMetrics]:
        components = await self._components(tenant_id)
        if not components or not ponds:
            return {}
        # Legacy `last()` runs per field table: here per (series, metric), pushed down.
        query = "\n".join(
            [
                self._source(tenant_id, ponds, components, "start: -24h"),
                '  |> filter(fn: (r) => r["_field"] == "value")',
                "  |> last()",
            ]
        )
        series = _series_from_last(await self._query(query), ponds)
        for item in series:
            # The legacy pivot keeps only the fields whose last point is at the newest time.
            newest = max(item.rows)
            item.rows = {newest: item.rows[newest]}
        times = [measured_at for item in series for measured_at in item.rows]
        if times:
            await self._merge_seq(tenant_id, ponds, components, series, min(times), max(times))
        latest: dict[str, LatestMetrics] = {}
        for item in _ordered(series):
            (measured_at,) = item.rows
            current = latest.get(item.pond_id)
            # Strict `>`: on a tie the first series in legacy table order wins.
            if current is None or measured_at > current.measured_at:
                latest[item.pond_id] = LatestMetrics(
                    measured_at=measured_at,
                    tenant_id=tenant_id,
                    pond_id=item.pond_id,
                    **item.rows[measured_at],
                )
        return latest

    def _pond_scope(self, tenant_id: str, pond_id: str) -> dict[str, str]:
        require_safe_id(tenant_id)
        require_safe_id(pond_id)
        return {asset_id_for_legacy_pond(tenant_id, pond_id): pond_id}

    async def _components(self, tenant_id: str) -> list[str]:
        """Only legacy default Components (D7 mirror); native v2 Components never reach /v1."""
        require_safe_id(tenant_id)
        devices = await self.domain_repository.list_devices(tenant_id)
        return sorted(
            {default_component_id_for_legacy_device(tenant_id, d.device_id) for d in devices}
        )

    def _source(
        self,
        tenant_id: str,
        ponds: Mapping[str, str],
        components: Iterable[str],
        range_args: str,
        metrics: Iterable[str] = tuple(_LEGACY_FIELD_BY_METRIC),
    ) -> str:
        """Legacy-visible observations; one source per query, so every filter is pushed down."""
        return "\n".join(
            [
                f"from(bucket: {flux_string_literal(self.bucket)})",
                f"  |> range({range_args})",
                _any_filter("_measurement", [OBSERVATION_MEASUREMENT]),
                _any_filter("tenant_id", [tenant_id]),
                _any_filter("asset_id", sorted(ponds)),
                _any_filter("component_id", components),
                _any_filter("metric_key", list(metrics)),
            ]
        )

    async def _merge_seq(
        self,
        tenant_id: str,
        ponds: Mapping[str, str],
        components: Iterable[str],
        series: Iterable[_Series],
        first: datetime,
        last: datetime,
    ) -> None:
        """Add `seq` to each returned row, the winner of its legacy (series, _time).

        Absent sequences are dropped first, so they rank lowest, as in `build_legacy_points`;
        each series is ranked as one table, never one table per timestamp.
        """
        range_args = (
            f"start: {_flux_time(first)}, stop: {_flux_time(last + timedelta(microseconds=1))}"
        )
        query = "\n".join(
            [
                self._source(tenant_id, ponds, components, range_args),
                f"  |> filter(fn: (r) => {_or_clauses('_field', _RANK_FIELDS)})",
                '  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")',
                f'  |> filter(fn: (r) => r["sequence"] != {ABSENT_SEQUENCE})',
                f"  |> group(columns: {_flux_columns(_SERIES_KEY)})",
                '  |> sort(columns: ["_time", "ingested_at_ns", "event_id"], desc: true)',
                '  |> unique(column: "_time")',
            ]
        )
        rows = {(item.pond_id, item.device_id): item.rows for item in series}
        for record in _records(await self._query(query)):
            key = _series_key(record.values, ponds)
            fields = rows.get(key, {}).get(record.values["_time"]) if key else None
            if fields is not None:
                fields["seq"] = _integer(record.values["sequence"])

    async def _query(self, query: str) -> Any:
        return await to_thread.run_sync(self.query_api.query, query, self.org)


def _is_safe_id(value: Any) -> bool:
    try:
        require_safe_id(value)
    except ValueError:
        return False
    return True


def _series_key(values: Mapping[str, Any], ponds: Mapping[str, str]) -> tuple[str, str] | None:
    pond_id = ponds.get(str(values.get("asset_id")))
    if pond_id is None:
        return None
    return pond_id, str(values["device_id"])


def _series_from_rows(tables: Iterable[Any], ponds: Mapping[str, str]) -> list[_Series]:
    """Readings: one pivoted row per (series, _time), renamed to the legacy fields."""
    series: dict[tuple[str, str], _Series] = {}
    for record in _records(tables):
        values = record.values
        key = _series_key(values, ponds)
        if key is None:
            continue
        fields = {}
        for metric, legacy_field in _LEGACY_FIELD_BY_METRIC.items():
            value = (
                None if values.get(metric) is None else _legacy_value(legacy_field, values[metric])
            )
            if value is not None:
                fields[legacy_field] = value
        if fields:
            series.setdefault(key, _Series(*key)).rows[values["_time"]] = fields
    return list(series.values())


def _series_from_last(tables: Iterable[Any], ponds: Mapping[str, str]) -> list[_Series]:
    """Latest: each field's last point, filed under that point's time like the legacy pivot."""
    series: dict[tuple[str, str], _Series] = {}
    for record in _records(tables):
        values = record.values
        key = _series_key(values, ponds)
        if key is None:
            continue
        legacy_field = _LEGACY_FIELD_BY_METRIC[values["metric_key"]]
        value = _legacy_value(legacy_field, values["_value"])
        if value is not None:
            rows = series.setdefault(key, _Series(*key)).rows
            rows.setdefault(values["_time"], {})[legacy_field] = value
    return list(series.values())


def _ordered(series: Iterable[_Series]) -> list[_Series]:
    # Legacy tables come in series-key order; within one pond that is `device_id` order.
    return sorted(series, key=lambda item: item.device_id)


def _legacy_value(legacy_field: str, value: Any) -> float | int | None:
    number = float(value)
    if legacy_field in _INTEGER_FIELDS:
        # `rssi` is stored as a float (D5) and dual-written only when integral, so legacy never
        # holds a non-integral one.
        return int(number) if number.is_integer() else None
    return number


def _integer(value: Any) -> int:
    # The Flux `sequence` column is an integer: no float round trip, no lax coercion.
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("sequence must be an integer")
    return value


def _range_args(*, start: str, stop: str | None) -> str:
    args = [f"start: {_flux_time_bound(start)}"]
    if stop is not None:
        args.append(f"stop: {_flux_time_bound(stop)}")
    return ", ".join(args)


def _flux_time_bound(value: str) -> str:
    validated = validate_flux_time_bound(value)
    if validated.startswith("-"):
        return validated
    return f"time(v: {flux_string_literal(validated)})"


def _flux_time(value: datetime) -> str:
    return f"time(v: {flux_string_literal(format_utc_timestamp(value))})"


def _flux_columns(columns: Iterable[str]) -> str:
    return "[" + ", ".join(flux_string_literal(column) for column in columns) + "]"


def _or_clauses(column: str, values: Iterable[str]) -> str:
    return " or ".join(f'r["{column}"] == {flux_string_literal(value)}' for value in values)


def _any_filter(column: str, values: Iterable[str]) -> str:
    """`or`-joined equality, which storage pushes down (`contains()` is not)."""
    return f"  |> filter(fn: (r) => {_or_clauses(column, values)})"


def _records(tables: Iterable[Any]) -> Iterable[Any]:
    for table in tables:
        yield from table.records


def _summary_from_tables(
    tables: Iterable[Any], *, tenant_id: str, pond_id: str, period: str, interval: str
) -> MetricsSummary:
    stats: dict[str, dict[str, Any]] = {legacy: {} for legacy in _SUMMARY_FIELDS}
    points: dict[Any, dict[str, Any]] = {}
    for record in _records(tables):
        values = record.values
        legacy_field = _LEGACY_FIELD_BY_METRIC.get(str(values.get("metric_key")))
        value = values.get("_value")
        if legacy_field not in stats or value is None:
            continue
        result = values.get("result")
        if result == "series":
            points.setdefault(values["_time"], {})[legacy_field] = value
        elif result in {"mean", "min", "max", "count"}:
            stats[legacy_field][result] = value
    return MetricsSummary(
        tenant_id=tenant_id,
        pond_id=pond_id,
        period=period,
        interval=interval,
        series=[SummaryPoint(measured_at=t, **v) for t, v in sorted(points.items())],
        statistics={k: MetricStatistics(**v) for k, v in stats.items()},
    )
