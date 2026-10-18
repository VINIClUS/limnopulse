"""`/v1` telemetry read projection over canonical `observation_numeric` points (P2-07).

`V2ProjectedTelemetryRepository` implements the existing `TelemetryRepository` so that, for data
dual-written from legacy default Components, every `/v1` shape equals the legacy adapter's
(D10). Each point is pivoted to one row, mapped to the constant legacy identity (D8) and grouped
into legacy series tables (series key plus `_field`) before the unchanged legacy tail runs.

Flux constraints verified on InfluxDB 2.7.11:
- `rssi` and `seq` are integer tables, so the float metrics, `rssi` and `seq` are separate
  branches; each branch is grouped into legacy series tables before `union()`, and every branch
  emits the same column layout.
- Latest keeps `_field` through `sort(_time) |> last()` and pivots straight to the legacy row
  key; regrouping without `_field` would mix integer and float `_value` columns.
- Summary sums in legacy series order (`device_id`, then `_time`) with `window() |> mean()`;
  `aggregateWindow(fn: mean)` re-orders rows and changes the float sum.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
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
from limnopulse_api.domain.telemetry_v2 import ABSENT_SEQUENCE, LEGACY_MQTT_TOPIC_TEMPLATE
from limnopulse_api.domain.telemetry_v2_schema import (
    LEGACY_MEASUREMENT,
    LEGACY_SCHEMA_VERSION,
    LEGACY_SOURCE,
    OBSERVATION_MEASUREMENT,
    flux_string_literal,
)

_LEGACY_FIELDS = (
    "temp_c",
    "ph",
    "do_mg_l",
    "turbidity_ntu",
    "salinity_ppt",
    "battery_v",
    "rssi",
    "seq",
)
_SUMMARY_FIELDS = ("do_mg_l", "ph", "temp_c")
_SUMMARY_INTERVALS = {"24h": "5m", "7d": "1h", "30d": "1h"}
_RSSI_FIELD = "rssi"
_POINT_FIELDS = ("value", "sequence", "ingested_at_ns", "event_id")
_LEGACY_SERIES_KEY = (
    "_start",
    "_stop",
    "_measurement",
    "tenant_id",
    "pond_id",
    "device_id",
    "source",
    "schema_version",
    "topic",
)
# The `seq` winner of a legacy (series, _time) group, as in `build_legacy_points`.
_SEQ_GROUP = ("tenant_id", "asset_id", "device_id", "_time")
# Legacy InfluxDB aggregates `mean` in storage: each series is summed in time order, the series
# sums are added in series (device) order, and the total is divided by the count. A plain
# `mean()` sums all rows in one pass, which rounds differently, so it is rebuilt with `reduce`
# over rows sorted by `device_id`, then `_time`.
_LEGACY_MEAN = """legacyMean = (tables=<-) => tables
  |> reduce(
    identity: {device: "", total: 0.0, partial: 0.0, n: 0},
    fn: (r, accumulator) => if r.device_id != accumulator.device then {
        device: r.device_id,
        total: accumulator.total + accumulator.partial,
        partial: 0.0 + r._value,
        n: accumulator.n + 1,
      } else {
        device: accumulator.device,
        total: accumulator.total,
        partial: accumulator.partial + r._value,
        n: accumulator.n + 1,
      },
  )
  |> map(fn: (r) => ({r with _value: (r.total + r.partial) / float(v: r.n)}))"""
_TOPIC_PREFIX, _TOPIC_SUFFIX = LEGACY_MQTT_TOPIC_TEMPLATE.split("{device_id}")


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
        query = await self._projected_points(
            tenant_id=tenant_id,
            ponds=self._pond_scope(tenant_id, pond_id),
            range_args=f"start: -{period}",
            legacy_fields=_SUMMARY_FIELDS,
        )
        tables: Iterable[Any] = []
        if query is not None:
            query = "\n".join(
                [
                    query,
                    '  |> group(columns: ["_field"])',
                    # Legacy series order: device by device, each in time order.
                    '  |> sort(columns: ["device_id", "_time"])',
                    _LEGACY_MEAN,
                    (
                        f"data |> window(every: {interval}, createEmpty: false) |> legacyMean()"
                        ' |> duplicate(column: "_stop", as: "_time") |> yield(name: "series")'
                    ),
                    'data |> legacyMean() |> yield(name: "mean")',
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
        query = await self._projected_points(
            tenant_id=tenant_id,
            ponds=self._pond_scope(tenant_id, pond_id),
            range_args=range_args,
            legacy_fields=_LEGACY_FIELDS,
        )
        if query is None:
            return []
        query = "\n".join(
            [
                query,
                "data",
                '  |> pivot(rowKey:["_time", "tenant_id", "pond_id", "device_id"], columnKey: ["_field"], valueColumn: "_value")',
                '  |> sort(columns: ["_time"])',
                f"  |> limit(n: {int(limit)})",
            ]
        )
        tables = await self._query(query)
        return [_reading_from_values(record.values) for record in _records(tables)]

    async def query_latest_metrics(self, *, tenant_id: str, pond_id: str) -> LatestMetrics:
        query = await self._projected_points(
            tenant_id=tenant_id,
            ponds=self._pond_scope(tenant_id, pond_id),
            range_args="start: -24h",
            legacy_fields=_LEGACY_FIELDS,
        )
        if query is None:
            return LatestMetrics(tenant_id=tenant_id, pond_id=pond_id)
        tables = await self._query(_latest_tail(query, limit_one=True))
        latest_values: dict[str, Any] | None = None
        for record in _records(tables):
            if latest_values is None or record.values.get("_time") > latest_values.get("_time"):
                latest_values = record.values
        if latest_values is not None:
            return _latest_from_values(latest_values, tenant_id=tenant_id, pond_id=pond_id)
        return LatestMetrics(tenant_id=tenant_id, pond_id=pond_id)

    async def query_latest_metrics_for_tenant(self, *, tenant_id: str) -> list[LatestMetrics]:
        require_safe_id(tenant_id)
        ponds = {}
        for pond in await self.domain_repository.list_ponds(tenant_id):
            ponds[asset_id_for_legacy_pond(tenant_id, pond.pond_id)] = require_safe_id(pond.pond_id)
        query = await self._projected_points(
            tenant_id=tenant_id,
            ponds=ponds,
            range_args="start: -24h",
            legacy_fields=_LEGACY_FIELDS,
        )
        if query is None:
            return []
        tables = await self._query(_latest_tail(query, limit_one=False))
        latest_by_pond: dict[str, dict[str, Any]] = {}
        for record in _records(tables):
            values = record.values
            pond_id = str(values.get("pond_id", ""))
            if not pond_id:
                continue
            current = latest_by_pond.get(pond_id)
            if current is None or values.get("_time") > current.get("_time"):
                latest_by_pond[pond_id] = values
        return [
            _latest_from_values(values, tenant_id=tenant_id, pond_id=pond_id)
            for pond_id, values in sorted(latest_by_pond.items())
        ]

    def _pond_scope(self, tenant_id: str, pond_id: str) -> dict[str, str]:
        require_safe_id(tenant_id)
        require_safe_id(pond_id)
        return {asset_id_for_legacy_pond(tenant_id, pond_id): pond_id}

    async def _projected_points(
        self,
        *,
        tenant_id: str,
        ponds: Mapping[str, str],
        range_args: str,
        legacy_fields: Iterable[str],
    ) -> str | None:
        """Build the `data` stream of legacy series tables, or None when nothing can match."""
        require_safe_id(tenant_id)
        devices = await self.domain_repository.list_devices(tenant_id)
        # Only legacy default Components (D7 mirror); native v2 Components never reach /v1.
        components = sorted(
            {default_component_id_for_legacy_device(tenant_id, d.device_id) for d in devices}
        )
        if not components or not ponds:
            return None
        wanted = set(legacy_fields)
        definitions = [
            d for d in metric_catalog() if d.legacy_field is not None and d.legacy_field in wanted
        ]
        float_metrics = {
            d.metric_key: d.legacy_field for d in definitions if d.legacy_field != _RSSI_FIELD
        }
        rssi_metrics = [d.metric_key for d in definitions if d.legacy_field == _RSSI_FIELD]
        lines = [
            'import "dict"',
            f"ponds = {_flux_dict(sorted(ponds.items()))}",
            f"legacyFields = {_flux_dict(float_metrics.items())}",
            f"points = from(bucket: {flux_string_literal(self.bucket)})",
            f"  |> range({range_args})",
            _equals_filter("_measurement", OBSERVATION_MEASUREMENT),
            _equals_filter("tenant_id", tenant_id),
            _contains_filter("asset_id", sorted(ponds)),
            _contains_filter("component_id", components),
            _contains_filter("metric_key", [d.metric_key for d in definitions]),
            _contains_filter("_field", _POINT_FIELDS),
            # Each table is one series, so `_time` alone keys one row per point.
            '  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")',
            "floats = points",
            _contains_filter("metric_key", sorted(float_metrics)),
            _legacy_row('dict.get(dict: legacyFields, key: r.metric_key, default: "")', "r.value"),
        ]
        branches = ["floats"]
        if rssi_metrics:
            lines += [
                "rssi = points",
                _contains_filter("metric_key", rssi_metrics),
                _legacy_row(flux_string_literal(_RSSI_FIELD), "int(v: r.value)"),
            ]
            branches.append("rssi")
        if "seq" in wanted:
            lines += [
                "seq = points",
                # Absent sequences rank lowest, as in `build_legacy_points`.
                f"  |> filter(fn: (r) => r.sequence != {ABSENT_SEQUENCE})",
                f"  |> group(columns: {_flux_columns(_SEQ_GROUP)})",
                '  |> sort(columns: ["ingested_at_ns", "event_id"])',
                '  |> last(column: "sequence")',
                # Integer end to end: sequences up to MAX_SEQUENCE stay exact.
                _legacy_row(flux_string_literal("seq"), "r.sequence"),
            ]
            branches.append("seq")
        source = branches[0] if len(branches) == 1 else f"union(tables: [{', '.join(branches)}])"
        lines += [
            f"data = {source}",
            f"  |> group(columns: {_flux_columns((*_LEGACY_SERIES_KEY, '_field'))})",
        ]
        return "\n".join(lines)

    async def _query(self, query: str) -> Any:
        return await to_thread.run_sync(self.query_api.query, query, self.org)


def _legacy_row(field: str, value: str) -> str:
    """Map a pivoted point to the legacy row shape (D8), grouped into legacy series tables."""
    topic = (
        f"{flux_string_literal(_TOPIC_PREFIX)} + r.device_id + {flux_string_literal(_TOPIC_SUFFIX)}"
    )
    record = ", ".join(
        [
            "_start: r._start",
            "_stop: r._stop",
            "_time: r._time",
            f"_measurement: {flux_string_literal(LEGACY_MEASUREMENT)}",
            "tenant_id: r.tenant_id",
            'pond_id: dict.get(dict: ponds, key: r.asset_id, default: "")',
            "device_id: r.device_id",
            f"source: {flux_string_literal(LEGACY_SOURCE)}",
            f"schema_version: {flux_string_literal(LEGACY_SCHEMA_VERSION)}",
            f"topic: {topic}",
            f"_field: {field}",
            f"_value: {value}",
        ]
    )
    return "\n".join(
        [
            f"  |> map(fn: (r) => ({{{record}}}))",
            f"  |> group(columns: {_flux_columns((*_LEGACY_SERIES_KEY, '_field'))})",
        ]
    )


def _latest_tail(query: str, *, limit_one: bool) -> str:
    lines = [
        query,
        "data",
        # `_field` stays in the group key: legacy `last()` runs per field table.
        '  |> sort(columns: ["_time"])',
        "  |> last()",
        '  |> pivot(rowKey:["_time", "tenant_id", "pond_id"], columnKey: ["_field"], valueColumn: "_value")',
        '  |> sort(columns: ["_time"], desc: true)',
    ]
    if limit_one:
        lines.append("  |> limit(n: 1)")
    return "\n".join(lines)


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


def _flux_columns(columns: Iterable[str]) -> str:
    return "[" + ", ".join(flux_string_literal(column) for column in columns) + "]"


def _flux_dict(items: Iterable[tuple[str, str]]) -> str:
    entries = ", ".join(f"{flux_string_literal(k)}: {flux_string_literal(v)}" for k, v in items)
    return f"[{entries}]"


def _equals_filter(column: str, value: str) -> str:
    return f'  |> filter(fn: (r) => r["{column}"] == {flux_string_literal(value)})'


def _contains_filter(column: str, values: Iterable[str]) -> str:
    literals = ", ".join(flux_string_literal(value) for value in values)
    return f'  |> filter(fn: (r) => contains(value: r["{column}"], set: [{literals}]))'


def _records(tables: Iterable[Any]) -> Iterable[Any]:
    for table in tables:
        yield from table.records


def _water_quality_values(values: Mapping[str, Any]) -> dict[str, Any]:
    return {
        field: values[field]
        for field in _LEGACY_FIELDS
        if field in values and values[field] is not None
    }


def _reading_from_values(values: Mapping[str, Any]) -> TelemetryReading:
    return TelemetryReading(
        measured_at=values["_time"],
        tenant_id=str(values["tenant_id"]),
        pond_id=str(values["pond_id"]),
        device_id=str(values["device_id"]),
        **_water_quality_values(values),
    )


def _latest_from_values(
    values: Mapping[str, Any], *, tenant_id: str, pond_id: str
) -> LatestMetrics:
    return LatestMetrics(
        measured_at=values.get("_time"),
        tenant_id=str(values.get("tenant_id", tenant_id)),
        pond_id=str(values.get("pond_id", pond_id)),
        **_water_quality_values(values),
    )


def _summary_from_tables(
    tables: Iterable[Any], *, tenant_id: str, pond_id: str, period: str, interval: str
) -> MetricsSummary:
    stats: dict[str, dict[str, Any]] = {field: {} for field in _SUMMARY_FIELDS}
    points: dict[Any, dict[str, Any]] = {}
    for record in _records(tables):
        values = record.values
        field, value = values.get("_field"), values.get("_value")
        if field not in stats or value is None:
            continue
        result = values.get("result")
        if result == "series":
            points.setdefault(values["_time"], {})[field] = value
        elif result in {"mean", "min", "max", "count"}:
            stats[field][result] = value
    return MetricsSummary(
        tenant_id=tenant_id,
        pond_id=pond_id,
        period=period,
        interval=interval,
        series=[SummaryPoint(measured_at=t, **v) for t, v in sorted(points.items())],
        statistics={k: MetricStatistics(**v) for k, v in stats.items()},
    )
