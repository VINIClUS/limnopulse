"""Unit tests for the `/v1` projection over `observation_numeric` (P2-07).

The fake `query_api` records each Flux string and returns legacy-shaped tables, so the Flux
shape is checked here and its runtime equivalence in `tests/influx/test_v1_projection_equivalence.py`.
"""

from __future__ import annotations

import inspect
import json
import re
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from limnopulse_api.adapters.influxdb_v1_projection import V2ProjectedTelemetryRepository

from limnopulse_api.adapters.influxdb import InfluxTelemetryRepository
from limnopulse_api.core.errors import TelemetryQueryError
from limnopulse_api.domain.ids import (
    asset_id_for_legacy_pond,
    default_component_id_for_legacy_device,
)
from limnopulse_api.domain.metric_catalog import metric_catalog
from limnopulse_api.domain.telemetry_v2_schema import flux_string_literal
from limnopulse_api.repositories.telemetry import TelemetryRepository

TENANT = "tnt_1"
POND = "pond_1"
LEGACY_METRIC_KEYS = {d.metric_key for d in metric_catalog() if d.legacy_field is not None}
SUMMARY_METRIC_KEYS = {"water.dissolved_oxygen", "water.ph", "water.temperature"}
POINT_FIELDS = {"value", "sequence", "ingested_at_ns", "event_id"}
LEGACY_SERIES_KEY = {
    "_start",
    "_stop",
    "_measurement",
    "tenant_id",
    "pond_id",
    "device_id",
    "source",
    "schema_version",
    "topic",
}
T0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
T1 = datetime(2026, 1, 1, 12, 5, tzinfo=UTC)


# --- fakes ------------------------------------------------------------------------------------


class FakeRecord:
    def __init__(self, values: dict[str, Any]) -> None:
        self.values = values


class FakeTable:
    def __init__(self, records: list[FakeRecord]) -> None:
        self.records = records


class FakeQueryApi:
    def __init__(self, tables: list[FakeTable] | None = None, error: Exception | None = None):
        self.tables = tables or []
        self.error = error
        self.calls: list[dict[str, str]] = []

    def query(self, query: str, org: str):
        self.calls.append({"query": query, "org": org})
        if self.error is not None:
            raise self.error
        return self.tables


class FakeDomainRepository:
    def __init__(
        self,
        devices: dict[str, str] | None = None,
        ponds: tuple[str, ...] = ("pond_1", "pond_2"),
    ) -> None:
        self.devices = {"dev_a": "pond_1", "dev_b": "pond_2"} if devices is None else devices
        self.ponds = ponds
        self.calls: list[tuple[str, str]] = []

    async def list_devices(self, tenant_id: str):
        self.calls.append(("list_devices", tenant_id))
        return [
            SimpleNamespace(tenant_id=tenant_id, device_id=device_id, pond_id=pond_id)
            for device_id, pond_id in self.devices.items()
        ]

    async def list_ponds(self, tenant_id: str):
        self.calls.append(("list_ponds", tenant_id))
        return [SimpleNamespace(tenant_id=tenant_id, pond_id=pond_id) for pond_id in self.ponds]

    async def get_pond(self, tenant_id: str, pond_id: str):  # pragma: no cover - unused
        raise AssertionError("the projection must not look up single ponds")


def projection(
    query_api: FakeQueryApi | None = None,
    domain: FakeDomainRepository | None = None,
    bucket: str = "limnopulse_raw",
) -> V2ProjectedTelemetryRepository:
    return V2ProjectedTelemetryRepository(
        query_api=query_api or FakeQueryApi(),
        org="limnopulse",
        bucket=bucket,
        domain_repository=domain or FakeDomainRepository(),
    )


def legacy(query_api: FakeQueryApi) -> InfluxTelemetryRepository:
    return InfluxTelemetryRepository(query_api=query_api, org="limnopulse", bucket="limnopulse_raw")


async def call(repository: Any, method: str, **overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "query_readings": {
            "tenant_id": TENANT,
            "pond_id": POND,
            "start": "-30m",
            "stop": None,
            "limit": 7,
        },
        "query_latest_metrics": {"tenant_id": TENANT, "pond_id": POND},
        "query_latest_metrics_for_tenant": {"tenant_id": TENANT},
        "query_summary": {"tenant_id": TENANT, "pond_id": POND, "period": "24h"},
    }[method]
    kwargs.update(overrides)
    return await getattr(repository, method)(**kwargs)


async def flux_for(method: str, domain: FakeDomainRepository | None = None, **overrides) -> str:
    query_api = FakeQueryApi()
    await call(projection(query_api, domain), method, **overrides)
    assert len(query_api.calls) == 1
    assert query_api.calls[0]["org"] == "limnopulse"
    return query_api.calls[0]["query"]


def compact(query: str) -> str:
    return re.sub(r"\s+", "", query)


def _string_list(raw: str) -> list[str]:
    return json.loads(f"[{raw}]")


def contains_set(query: str, column: str) -> set[str]:
    """The literal set of the `contains(value: r[column], ...)` filter (or `==` filter)."""
    flat = compact(query)
    sets = re.findall(rf'contains\(value:r(?:\["{column}"\]|\.{column}),set:\[([^\]]*)\]\)', flat)
    equals = re.findall(rf'r(?:\["{column}"\]|\.{column})==("(?:[^"\\]|\\.)*")', flat)
    assert sets or equals, f"no filter on {column}"
    values: set[str] = set()
    for raw in sets:
        values |= set(_string_list(raw))
    for raw in equals:
        values.add(json.loads(raw))
    return values


def first_filter_set(query: str, column: str) -> set[str]:
    """The literal set of the first filter on `column` (the point filter before the pivot)."""
    flat = compact(query)
    pattern = re.compile(
        rf'contains\(value:r(?:\["{column}"\]|\.{column}),set:\[([^\]]*)\]\)'
        rf'|r(?:\["{column}"\]|\.{column})==("(?:[^"\\]|\\.)*")'
    )
    match = pattern.search(flat)
    assert match is not None, f"no filter on {column}"
    if match.group(1) is not None:
        return set(_string_list(match.group(1)))
    return {json.loads(match.group(2))}


def group_calls(query: str) -> list[tuple[int, set[str]]]:
    flat = compact(query)
    return [
        (match.start(), set(_string_list(match.group(1))))
        for match in re.finditer(r"group\(columns:\[([^\]]*)\]\)", flat)
    ]


def legacy_group_position(query: str, columns: set[str]) -> int:
    """The position of the last `group(columns: ...)` with exactly these columns."""
    positions = [position for position, group in group_calls(query) if group == columns]
    assert positions, f"no group(columns: {sorted(columns)})"
    return positions[-1]


def ordered(flat: str, start: int, needles: list[str]) -> None:
    """Each needle occurs after `start`, in order."""
    position = start
    for needle in needles:
        found = flat.find(needle, position)
        assert found != -1, f"{needle!r} missing after offset {position}"
        position = found + len(needle)


METHODS = (
    "query_readings",
    "query_latest_metrics",
    "query_latest_metrics_for_tenant",
    "query_summary",
)
POND_METHODS = ("query_readings", "query_latest_metrics", "query_summary")


# --- protocol ---------------------------------------------------------------------------------


def test_implements_the_telemetry_repository_protocol_exactly() -> None:
    for name in METHODS:
        expected = inspect.signature(getattr(TelemetryRepository, name))
        actual = inspect.signature(getattr(V2ProjectedTelemetryRepository, name))
        assert list(actual.parameters) == list(expected.parameters), name
        for parameter_name, parameter in expected.parameters.items():
            assert actual.parameters[parameter_name].kind == parameter.kind, (name, parameter_name)
        assert inspect.iscoroutinefunction(getattr(V2ProjectedTelemetryRepository, name))

    init = inspect.signature(V2ProjectedTelemetryRepository.__init__)
    keyword_only = {
        name
        for name, parameter in init.parameters.items()
        if parameter.kind is inspect.Parameter.KEYWORD_ONLY
    }
    assert keyword_only == {"query_api", "org", "bucket", "domain_repository"}


# --- Flux filters -----------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("method", METHODS)
async def test_reads_only_observation_numeric_points_of_the_tenant(method: str) -> None:
    query = await flux_for(method)
    flat = compact(query)

    assert 'from(bucket:"limnopulse_raw")' in flat
    assert first_filter_set(query, "_measurement") == {"observation_numeric"}
    assert first_filter_set(query, "tenant_id") == {TENANT}
    assert first_filter_set(query, "_field") == POINT_FIELDS
    # One row per point: the pivot comes after the point-field filter.
    field_filter = re.search(r'contains\(value:r(?:\["_field"\]|\._field),set:', flat)
    assert field_filter is not None
    assert flat.find("pivot(", field_filter.end()) != -1
    assert "pivot(" not in flat[: field_filter.start()]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method", ("query_readings", "query_latest_metrics", "query_latest_metrics_for_tenant")
)
async def test_reads_exactly_the_seven_legacy_metrics(method: str) -> None:
    query = await flux_for(method)

    assert contains_set(query, "metric_key") == LEGACY_METRIC_KEYS
    assert len(LEGACY_METRIC_KEYS) == 7


@pytest.mark.asyncio
async def test_summary_reads_its_three_legacy_metrics() -> None:
    query = await flux_for("query_summary")

    metric_keys = contains_set(query, "metric_key")
    assert SUMMARY_METRIC_KEYS <= metric_keys <= LEGACY_METRIC_KEYS


@pytest.mark.asyncio
@pytest.mark.parametrize("method", METHODS)
async def test_component_set_is_the_legacy_default_component_of_every_listed_device(
    method: str,
) -> None:
    domain = FakeDomainRepository(devices={"dev_a": "pond_1", "dev_b": "pond_2"})
    query = await flux_for(method, domain)

    assert contains_set(query, "component_id") == {
        default_component_id_for_legacy_device(TENANT, "dev_a"),
        default_component_id_for_legacy_device(TENANT, "dev_b"),
    }
    assert ("list_devices", TENANT) in domain.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("method", POND_METHODS)
async def test_pond_scoped_queries_read_only_the_legacy_pond_asset(method: str) -> None:
    query = await flux_for(method)

    assert contains_set(query, "asset_id") == {asset_id_for_legacy_pond(TENANT, POND)}
    assert flux_string_literal(POND) in query
    assert flux_string_literal("pond_2") not in query


@pytest.mark.asyncio
async def test_tenant_latest_reads_the_assets_of_listed_ponds_and_names_their_ponds() -> None:
    domain = FakeDomainRepository(ponds=("pond_1", "pond_2", "pond_3"))
    query = await flux_for("query_latest_metrics_for_tenant", domain)

    assets = {pond: asset_id_for_legacy_pond(TENANT, pond) for pond in domain.ponds}
    assert contains_set(query, "asset_id") == set(assets.values())
    assert ("list_ponds", TENANT) in domain.calls
    flat = compact(query)
    for pond, asset in assets.items():
        assert flux_string_literal(pond) in flat
        assert flux_string_literal(asset) in flat


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method", ("query_readings", "query_latest_metrics", "query_latest_metrics_for_tenant")
)
async def test_rows_carry_the_constant_legacy_shape(method: str) -> None:
    flat = compact(await flux_for(method))

    assert re.search(r'_measurement:"water_quality"|key:"_measurement",value:"water_quality"', flat)
    assert re.search(r'source:"mqtt"|key:"source",value:"mqtt"', flat)
    assert re.search(r'schema_version:"1"|key:"schema_version",value:"1"', flat)
    device = r'r(?:\.device_id|\["device_id"\])'
    assert re.search(
        rf'"limnopulse/v1/devices/"\+{device}\+"/readings"'
        rf'|"limnopulse/v1/devices/\$\{{{device}\}}/readings"',
        flat,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize(
    ("bucket", "literal"),
    [("raw${x}", '"raw\\${x}"'), ("raw$archive", '"raw$archive"')],
)
async def test_bucket_literal_is_escaped(method: str, bucket: str, literal: str) -> None:
    query_api = FakeQueryApi()
    await call(projection(query_api, bucket=bucket), method)

    flat = compact(query_api.calls[0]["query"])
    assert f"from(bucket:{literal})" in flat
    assert "${x}" not in flat.replace("\\${x}", "")


@pytest.mark.asyncio
@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("tenant_id", ['tnt"1', "tnt${x}", "tnt 1", ""])
async def test_invalid_tenant_id_is_rejected_before_any_query(method: str, tenant_id: str) -> None:
    query_api = FakeQueryApi()

    with pytest.raises(ValueError):
        await call(projection(query_api), method, tenant_id=tenant_id)

    assert query_api.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("method", POND_METHODS)
@pytest.mark.parametrize("pond_id", ['pond"1', "pond${x}", "pond 1"])
async def test_invalid_pond_id_is_rejected_before_any_query(method: str, pond_id: str) -> None:
    query_api = FakeQueryApi()

    with pytest.raises(ValueError):
        await call(projection(query_api), method, pond_id=pond_id)

    assert query_api.calls == []


@pytest.mark.asyncio
async def test_listed_pond_ids_never_reach_flux_unescaped() -> None:
    query_api = FakeQueryApi()
    domain = FakeDomainRepository(ponds=("pond_1", "pond${x}"))

    try:
        await call(projection(query_api, domain), "query_latest_metrics_for_tenant")
    except ValueError:
        assert query_api.calls == []
        return
    flat = compact(query_api.calls[0]["query"])
    assert "${x}" not in flat.replace("\\${x}", "")


# --- range bounds -----------------------------------------------------------------------------


def _range(query: str) -> str:
    """The balanced `range(...)` call of the query."""
    flat = compact(query)
    start = flat.find("range(")
    assert start != -1
    depth = 0
    for index in range(start + len("range"), len(flat)):
        depth += {"(": 1, ")": -1}.get(flat[index], 0)
        if depth == 0:
            return flat[start : index + 1]
    raise AssertionError("unbalanced range(...)")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("start", "stop"),
    [
        ("-30m", None),
        ("-2h", "-5m"),
        ("2026-01-01T12:00:00Z", None),
        ("2026-01-01T12:00:00.750Z", "2026-01-01T13:00:00+00:00"),
    ],
)
async def test_readings_range_bounds_are_the_legacy_bounds(start: str, stop: str | None) -> None:
    query_api = FakeQueryApi()
    legacy_api = FakeQueryApi()
    await call(projection(query_api), "query_readings", start=start, stop=stop)
    await call(legacy(legacy_api), "query_readings", start=start, stop=stop)

    assert _range(query_api.calls[0]["query"]) == _range(legacy_api.calls[0]["query"])


@pytest.mark.asyncio
@pytest.mark.parametrize(("start", "stop"), [("yesterday", None), ("-1h", "now()")])
async def test_readings_reject_invalid_time_bounds_like_legacy(start: str, stop) -> None:
    query_api = FakeQueryApi()
    with pytest.raises(ValueError):
        await call(legacy(FakeQueryApi()), "query_readings", start=start, stop=stop)

    with pytest.raises(ValueError):
        await call(projection(query_api), "query_readings", start=start, stop=stop)
    assert query_api.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ("query_latest_metrics", "query_latest_metrics_for_tenant"))
async def test_latest_reads_the_legacy_24h_window(method: str) -> None:
    legacy_api = FakeQueryApi()
    await call(legacy(legacy_api), method)

    assert _range(await flux_for(method)) == _range(legacy_api.calls[0]["query"])


@pytest.mark.asyncio
@pytest.mark.parametrize("period", ("24h", "7d", "30d"))
async def test_summary_reads_the_legacy_period_window(period: str) -> None:
    legacy_api = FakeQueryApi()
    await call(legacy(legacy_api), "query_summary", period=period)

    assert _range(await flux_for("query_summary", period=period)) == _range(
        legacy_api.calls[0]["query"]
    )


# --- legacy series tables and tails -----------------------------------------------------------


@pytest.mark.asyncio
async def test_readings_group_by_series_and_field_before_pivot_sort_and_limit() -> None:
    query = await flux_for("query_readings", limit=7)
    flat = compact(query)
    start = legacy_group_position(query, LEGACY_SERIES_KEY | {"_field"})

    ordered(
        flat,
        start,
        [
            (
                'pivot(rowKey:["_time","tenant_id","pond_id","device_id"],columnKey:["_field"],'
                'valueColumn:"_value")'
            ),
            'sort(columns:["_time"])',
            "limit(n:7)",
        ],
    )
    assert "limit(" not in flat[:start]
    assert 'sort(columns:["_time"])' not in flat[:start]
    # Legacy sort/limit run per series table: nothing regroups the tail.
    assert "group()" not in flat[start:]
    assert [position for position, _ in group_calls(query) if position > start] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ("query_latest_metrics", "query_latest_metrics_for_tenant"))
async def test_latest_keeps_field_tables_through_last_then_pivots_to_the_legacy_row(
    method: str,
) -> None:
    query = await flux_for(method)
    flat = compact(query)
    start = legacy_group_position(query, LEGACY_SERIES_KEY | {"_field"})

    needles = [
        'sort(columns:["_time"])',
        "last()",
        'pivot(rowKey:["_time","tenant_id","pond_id"],columnKey:["_field"],valueColumn:"_value")',
        'sort(columns:["_time"],desc:true)',
    ]
    if method == "query_latest_metrics":
        needles.append("limit(n:1)")
    ordered(flat, start, needles)
    # last() never runs on tables that have lost `_field` (it would collapse the metrics).
    last_position = flat.find("last()", start)
    for position, columns in group_calls(query):
        if start < position < last_position:
            assert "_field" in columns
    if method == "query_latest_metrics_for_tenant":
        assert "limit(" not in flat[start:]


@pytest.mark.asyncio
@pytest.mark.parametrize(("period", "interval"), [("24h", "5m"), ("7d", "1h"), ("30d", "1h")])
async def test_summary_sorts_by_device_then_time_after_field_group(
    period: str, interval: str
) -> None:
    query = await flux_for("query_summary", period=period)
    flat = compact(query)
    start = legacy_group_position(query, {"_field"})

    ordered(flat, start, ['sort(columns:["device_id","_time"])', f"every:{interval}"])
    for result in ("series", "mean", "min", "max", "count"):
        assert f'yield(name:"{result}")' in flat[start:]
    for function in ("mean()", "min()", "max()", "count()"):
        assert function in flat[start:]
    assert "limit(" not in flat


# --- integer fields ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method", ("query_readings", "query_latest_metrics", "query_latest_metrics_for_tenant")
)
async def test_rssi_is_rebuilt_with_int_and_seq_keeps_its_integer_type(method: str) -> None:
    flat = compact(await flux_for(method))

    assert re.search(r'int\(v:r(?:\.value|\["value"\])\)', flat)
    assert re.search(r'_value:r(?:\.sequence|\["sequence"\])[,}]', flat)
    assert not re.search(r'(?:float|int|uint)\(v:r(?:\.sequence|\["sequence"\])\)', flat)
    assert '"seq"' in flat
    assert '"rssi"' in flat


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method", ("query_readings", "query_latest_metrics", "query_latest_metrics_for_tenant")
)
async def test_seq_branch_drops_absent_sequences_before_ranking_points(method: str) -> None:
    query = await flux_for(method)
    flat = compact(query)

    sequence = r'r(?:\.sequence|\["sequence"\])'
    absent = re.search(rf"{sequence}(?:!=-1|>=0|>-1)", flat)
    assert absent is not None
    rank = flat.find('sort(columns:["ingested_at_ns","event_id"])')
    assert rank > absent.start()
    assert flat.find("last(", rank) != -1
    # The ranking groups are legacy (series, _time) groups, formed after the per-point pivot.
    pivot = flat.find("pivot(")
    seq_groups = [columns for position, columns in group_calls(query) if pivot < position < rank]
    assert seq_groups
    assert {"_time", "tenant_id", "device_id"} <= seq_groups[-1]
    assert seq_groups[-1] & {"asset_id", "pond_id"}
    assert not seq_groups[-1] & {"metric_key", "_field"}


# --- decoding (same Python semantics as the legacy adapter) -----------------------------------


def _row(time: datetime, pond_id: str = POND, device_id: str = "dev_a", **fields) -> FakeRecord:
    return FakeRecord(
        {"_time": time, "tenant_id": TENANT, "pond_id": pond_id, "device_id": device_id, **fields}
    )


@pytest.mark.asyncio
async def test_readings_decode_like_legacy() -> None:
    tables = [
        FakeTable([_row(T0, temp_c=25.1, ph=7.2, rssi=-67, seq=2**63 - 1, battery_v=None)]),
        FakeTable([_row(T1, device_id="dev_b", do_mg_l=6.4, turbidity_ntu=3.1)]),
    ]
    expected = await call(legacy(FakeQueryApi(tables)), "query_readings")
    actual = await call(projection(FakeQueryApi(tables)), "query_readings")

    assert actual == expected
    assert [reading.device_id for reading in actual] == ["dev_a", "dev_b"]
    assert actual[0].seq == 2**63 - 1
    assert actual[0].battery_v is None


@pytest.mark.asyncio
async def test_latest_decodes_the_newest_row_like_legacy() -> None:
    tables = [
        FakeTable([_row(T0, device_id="dev_a", temp_c=25.0)]),
        FakeTable([_row(T1, device_id="dev_b", temp_c=26.0, seq=3)]),
        FakeTable([_row(T1, device_id="dev_c", temp_c=27.0)]),
    ]
    expected = await call(legacy(FakeQueryApi(tables)), "query_latest_metrics")
    actual = await call(projection(FakeQueryApi(tables)), "query_latest_metrics")

    assert actual == expected
    assert actual.temp_c == 26.0


@pytest.mark.asyncio
async def test_tenant_latest_decodes_the_newest_row_per_pond_like_legacy() -> None:
    tables = [
        FakeTable([_row(T0, pond_id="pond_2", temp_c=24.0)]),
        FakeTable([_row(T1, pond_id="pond_1", temp_c=25.0)]),
        FakeTable([_row(T1, pond_id="pond_2", temp_c=26.0)]),
        FakeTable([FakeRecord({"_time": T1, "tenant_id": TENANT, "temp_c": 1.0})]),
    ]
    expected = await call(legacy(FakeQueryApi(tables)), "query_latest_metrics_for_tenant")
    actual = await call(projection(FakeQueryApi(tables)), "query_latest_metrics_for_tenant")

    assert actual == expected
    assert [(item.pond_id, item.temp_c) for item in actual] == [("pond_1", 25.0), ("pond_2", 26.0)]


@pytest.mark.asyncio
async def test_summary_decodes_like_legacy() -> None:
    def record(result: str, field: str, value: Any, time: datetime = T1) -> FakeRecord:
        return FakeRecord({"result": result, "_field": field, "_value": value, "_time": time})

    tables = [
        FakeTable([record("series", "temp_c", 25.5, T0), record("series", "ph", 7.1, T1)]),
        FakeTable([record("series", "do_mg_l", None, T1), record("series", "seq", 3, T1)]),
        FakeTable([record("mean", "temp_c", 25.5), record("count", "temp_c", 2)]),
        FakeTable([record("min", "ph", 7.0), record("max", "ph", 7.2)]),
    ]
    expected = await call(legacy(FakeQueryApi(tables)), "query_summary", period="7d")
    actual = await call(projection(FakeQueryApi(tables)), "query_summary", period="7d")

    assert actual == expected
    assert actual.interval == "1h"
    assert [point.measured_at for point in actual.series] == [T0, T1]


@pytest.mark.asyncio
@pytest.mark.parametrize("method", METHODS)
async def test_a_tenant_without_legacy_devices_reads_empty_like_legacy(method: str) -> None:
    domain = FakeDomainRepository(devices={})
    expected = await call(legacy(FakeQueryApi()), method)
    actual = await call(projection(FakeQueryApi(), domain), method)

    assert actual == expected


# --- errors -----------------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("period", ("1h", "24", "7D", ""))
async def test_summary_rejects_an_invalid_period_before_any_query(period: str) -> None:
    query_api = FakeQueryApi()

    with pytest.raises(ValueError, match="invalid period"):
        await call(projection(query_api), "query_summary", period=period)

    assert query_api.calls == []


@pytest.mark.asyncio
async def test_summary_wraps_query_failures_in_telemetry_query_error() -> None:
    error = RuntimeError("influx down")

    with pytest.raises(TelemetryQueryError) as raised:
        await call(projection(FakeQueryApi(error=error)), "query_summary")

    assert raised.value.__cause__ is error


@pytest.mark.asyncio
@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("error", [RuntimeError("influx down"), OSError("reset"), KeyError("x")])
async def test_query_failures_surface_exactly_like_legacy(method: str, error: Exception) -> None:
    async def outcome(repository: Any) -> tuple[type, str, Exception | None, bool]:
        with pytest.raises(Exception) as raised:
            await call(repository, method)
        exc = raised.value
        return type(exc), str(exc), exc.__cause__, exc is error

    expected = await outcome(legacy(FakeQueryApi(error=error)))
    assert await outcome(projection(FakeQueryApi(error=error))) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("period", ("1h", "7D"))
async def test_invalid_period_raises_exactly_like_legacy(period: str) -> None:
    with pytest.raises(ValueError) as expected:
        await call(legacy(FakeQueryApi()), "query_summary", period=period)
    with pytest.raises(ValueError) as actual:
        await call(projection(FakeQueryApi()), "query_summary", period=period)

    assert type(actual.value) is type(expected.value)
    assert str(actual.value) == str(expected.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method", ("query_readings", "query_latest_metrics", "query_latest_metrics_for_tenant")
)
async def test_other_methods_propagate_query_failures_unwrapped(method: str) -> None:
    error = RuntimeError("influx down")

    with pytest.raises(RuntimeError) as raised:
        await call(projection(FakeQueryApi(error=error)), method)

    assert raised.value is error
    assert not isinstance(raised.value, TelemetryQueryError)
