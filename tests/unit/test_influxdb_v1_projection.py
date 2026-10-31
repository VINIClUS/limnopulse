"""Unit tests for the `/v1` projection over `observation_numeric` (P2-07, plan r7.2).

Flux only reads, with pushdown-friendly filters and no per-row `map`; Python shapes the legacy
rows. The fake `query_api` records each Flux string and returns canonical-shaped tables, one
response per call. Runtime equivalence lives in `tests/influx/test_v1_projection_equivalence.py`.
"""

from __future__ import annotations

import inspect
import json
import re
import sys
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import ConfigDict

from limnopulse_api.adapters.influxdb import InfluxTelemetryRepository
from limnopulse_api.adapters.influxdb_v1_projection import V2ProjectedTelemetryRepository
from limnopulse_api.core.errors import TelemetryQueryError
from limnopulse_api.domain.ids import (
    asset_id_for_legacy_pond,
    default_component_id_for_legacy_device,
)
from limnopulse_api.domain.metric_catalog import metric_catalog
from limnopulse_api.domain.telemetry import LatestMetrics, MetricStatistics, TelemetryReading
from limnopulse_api.domain.v2_common import format_utc_timestamp
from limnopulse_api.repositories.telemetry import TelemetryRepository

TENANT = "tnt_1"
POND = "pond_1"
ASSET = asset_id_for_legacy_pond(TENANT, POND)
UNKNOWN_ASSET = "ast_00000000000000000000000000000000"
LEGACY_METRIC_KEYS = {d.metric_key for d in metric_catalog() if d.legacy_field is not None}
SUMMARY_METRIC_KEYS = {"water.dissolved_oxygen", "water.ph", "water.temperature"}
SERIES_KEY = {"tenant_id", "asset_id", "device_id"}
T0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
T1 = datetime(2026, 1, 1, 12, 5, tzinfo=UTC)
T2 = datetime(2026, 1, 1, 12, 10, 0, 750000, tzinfo=UTC)
METHODS = (
    "query_readings",
    "query_latest_metrics",
    "query_latest_metrics_for_tenant",
    "query_summary",
)
POND_METHODS = ("query_readings", "query_latest_metrics", "query_summary")
ROW_METHODS = ("query_readings", "query_latest_metrics", "query_latest_metrics_for_tenant")


# --- fakes ------------------------------------------------------------------------------------


class FakeRecord:
    def __init__(self, values: dict[str, Any]) -> None:
        self.values = values


class FakeTable:
    def __init__(self, records: list[FakeRecord]) -> None:
        self.records = records


class FakeQueryApi:
    """Returns `responses[i]` for the i-th call (then empty), or raises `error`."""

    def __init__(self, *responses: list[FakeTable], error: Exception | None = None) -> None:
        self.responses = list(responses)
        self.error = error
        self.calls: list[dict[str, str]] = []

    def query(self, query: str, org: str):
        self.calls.append({"query": query, "org": org})
        if self.error is not None:
            raise self.error
        index = len(self.calls) - 1
        return self.responses[index] if index < len(self.responses) else []


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


def value_row(time: datetime, device: str = "dev_a", asset: str = ASSET, **metrics) -> FakeRecord:
    """A pivoted readings row: one column per metric key."""
    return FakeRecord(
        {"_time": time, "tenant_id": TENANT, "asset_id": asset, "device_id": device, **metrics}
    )


def last_row(
    time: datetime, metric: str, value: float, device: str = "dev_a", asset: str = ASSET
) -> FakeRecord:
    """A per-(series, metric) `last()` row."""
    return FakeRecord(
        {
            "_time": time,
            "tenant_id": TENANT,
            "asset_id": asset,
            "device_id": device,
            "metric_key": metric,
            "_field": "value",
            "_value": value,
        }
    )


def seq_row(time: datetime, sequence: Any, device: str = "dev_a", asset: str = ASSET) -> FakeRecord:
    """A `seq` winner row."""
    return FakeRecord(
        {
            "_time": time,
            "tenant_id": TENANT,
            "asset_id": asset,
            "device_id": device,
            "sequence": sequence,
            "ingested_at_ns": 1,
            "event_id": "obs_x",
        }
    )


def one_row(method: str) -> tuple[list[FakeTable], ...]:
    """A first response with one row, so a row method also runs its `seq` query."""
    if method == "query_readings":
        return ([FakeTable([value_row(T0, **{"water.ph": 7.0})])],)
    if method in ROW_METHODS:
        return ([FakeTable([last_row(T0, "water.ph", 7.0)])],)
    return ()


async def queries_for(
    method: str,
    domain: FakeDomainRepository | None = None,
    responses: tuple[list[FakeTable], ...] | None = None,
    **overrides: Any,
) -> list[str]:
    query_api = FakeQueryApi(*(one_row(method) if responses is None else responses))
    await call(projection(query_api, domain), method, **overrides)
    assert query_api.calls
    assert {item["org"] for item in query_api.calls} == {"limnopulse"}
    return [item["query"] for item in query_api.calls]


def compact(query: str) -> str:
    return re.sub(r"\s+", "", query)


def or_filter_values(query: str, column: str) -> set[str]:
    """Values of the first filter on `column`: one `==` clause per value, joined with `or`."""
    flat = compact(query)
    clause = rf'r\["{column}"\]=="(?:[^"\\]|\\.)*"'
    match = re.search(rf"filter\(fn:\(r\)=>({clause}(?:or{clause})*)\)", flat)
    assert match is not None, f"no or-filter on {column}"
    return {json.loads(raw) for raw in re.findall(r'==("(?:[^"\\]|\\.)*")', match.group(1))}


def group_columns(query: str) -> list[set[str]]:
    flat = compact(query)
    return [
        set(json.loads(f"[{raw}]")) for raw in re.findall(r"group\(columns:\[([^\]]*)\]\)", flat)
    ]


def ordered(flat: str, needles: list[str]) -> None:
    position = 0
    for needle in needles:
        found = flat.find(needle, position)
        assert found != -1, f"{needle!r} missing after offset {position}"
        position = found + len(needle)


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


def _flux_time(value: datetime) -> str:
    return f'time(v:"{format_utc_timestamp(value)}")'


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


# --- pushdown-friendly reads ------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("method", METHODS)
async def test_flux_only_reads_with_pushdown_filters_and_no_map(method: str) -> None:
    queries = await queries_for(method)

    assert len(queries) == (2 if method in ROW_METHODS else 1)
    for query in queries:
        flat = compact(query)
        assert flat.startswith(
            ('from(bucket:"limnopulse_raw")|>range(', 'data=from(bucket:"limnopulse_raw")|>range(')
        )
        assert flat.count("from(") == 1
        for forbidden in ("contains(", "map(", "reduce(", "dict."):
            assert forbidden not in flat
        assert or_filter_values(query, "_measurement") == {"observation_numeric"}
        assert or_filter_values(query, "tenant_id") == {TENANT}


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ROW_METHODS)
async def test_rows_read_exactly_the_seven_legacy_metrics(method: str) -> None:
    for query in await queries_for(method):
        assert or_filter_values(query, "metric_key") == LEGACY_METRIC_KEYS
    assert len(LEGACY_METRIC_KEYS) == 7


@pytest.mark.asyncio
async def test_summary_reads_its_three_legacy_metrics() -> None:
    (query,) = await queries_for("query_summary")

    assert or_filter_values(query, "metric_key") == SUMMARY_METRIC_KEYS


@pytest.mark.asyncio
@pytest.mark.parametrize("method", METHODS)
async def test_component_set_is_the_legacy_default_component_of_every_listed_device(
    method: str,
) -> None:
    domain = FakeDomainRepository(devices={"dev_a": "pond_1", "dev_b": "pond_2"})

    for query in await queries_for(method, domain):
        assert or_filter_values(query, "component_id") == {
            default_component_id_for_legacy_device(TENANT, "dev_a"),
            default_component_id_for_legacy_device(TENANT, "dev_b"),
        }
    assert ("list_devices", TENANT) in domain.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("method", POND_METHODS)
async def test_pond_scoped_queries_read_only_the_legacy_pond_asset(method: str) -> None:
    for query in await queries_for(method):
        assert or_filter_values(query, "asset_id") == {ASSET}


@pytest.mark.asyncio
async def test_tenant_latest_reads_the_assets_of_listed_ponds() -> None:
    domain = FakeDomainRepository(ponds=("pond_1", "pond_2", "pond_3"))

    for query in await queries_for("query_latest_metrics_for_tenant", domain):
        assert or_filter_values(query, "asset_id") == {
            asset_id_for_legacy_pond(TENANT, pond) for pond in domain.ponds
        }
    assert ("list_ponds", TENANT) in domain.calls


@pytest.mark.asyncio
async def test_tenant_latest_skips_listed_ponds_that_are_not_safe_ids() -> None:
    domain = FakeDomainRepository(ponds=("pond_1", "pond 2", 'pond"3', "pond${x}", "x" * 129))
    query_api = FakeQueryApi(*one_row("query_latest_metrics_for_tenant"))

    latest = await call(projection(query_api, domain), "query_latest_metrics_for_tenant")

    assert [item.pond_id for item in latest] == ["pond_1"]
    assert len(query_api.calls) == 2
    for item in query_api.calls:
        assert or_filter_values(item["query"], "asset_id") == {ASSET}


@pytest.mark.asyncio
async def test_tenant_latest_with_no_safe_pond_queries_nothing() -> None:
    query_api = FakeQueryApi()

    latest = await call(
        projection(query_api, FakeDomainRepository(ponds=("pond 2",))),
        "query_latest_metrics_for_tenant",
    )

    assert latest == []
    assert query_api.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize(
    ("bucket", "literal"),
    [("raw${x}", '"raw\\x24{x}"'), ("raw$archive", '"raw\\x24archive"')],
)
async def test_bucket_literal_is_escaped(method: str, bucket: str, literal: str) -> None:
    query_api = FakeQueryApi(*one_row(method))
    await call(projection(query_api, bucket=bucket), method)

    assert query_api.calls
    for item in query_api.calls:
        flat = compact(item["query"])
        assert f"from(bucket:{literal})|>range(" in flat
        assert "${x}" not in flat.replace("\\${x}", "")


@pytest.mark.asyncio
@pytest.mark.parametrize("method", METHODS)
async def test_bucket_literal_is_lossless(method: str) -> None:
    # P2 plan D19 (#84): non-ASCII and HTML characters stay raw; every `$` becomes `\x24`.
    query_api = FakeQueryApi(*one_row(method))
    await call(projection(query_api, bucket="é<&>\u2028${x}"), method)

    assert query_api.calls
    for item in query_api.calls:
        assert 'from(bucket: "é<&>\u2028\\x24{x}")' in item["query"]


@pytest.mark.asyncio
@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("bucket", ["raw\x00", "raw\x1b[0m", "raw\ud800"], ids=ascii)
async def test_bucket_that_cannot_round_trip_is_rejected_before_any_query(
    method: str, bucket: str
) -> None:
    query_api = FakeQueryApi(*one_row(method))
    with pytest.raises(ValueError):
        await call(projection(query_api, bucket=bucket), method)

    assert query_api.calls == []


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


# --- range bounds -----------------------------------------------------------------------------


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
    legacy_api = FakeQueryApi()
    await call(legacy(legacy_api), "query_readings", start=start, stop=stop)

    queries = await queries_for("query_readings", start=start, stop=stop)

    assert _range(queries[0]) == _range(legacy_api.calls[0]["query"])


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

    queries = await queries_for(method)

    assert _range(queries[0]) == _range(legacy_api.calls[0]["query"])


@pytest.mark.asyncio
@pytest.mark.parametrize("period", ("24h", "7d", "30d"))
async def test_summary_reads_the_legacy_period_window(period: str) -> None:
    legacy_api = FakeQueryApi()
    await call(legacy(legacy_api), "query_summary", period=period)

    (query,) = await queries_for("query_summary", period=period)

    assert _range(query) == _range(legacy_api.calls[0]["query"])


# --- query shapes -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_readings_pivot_each_legacy_series_then_sort_and_limit_it() -> None:
    queries = await queries_for("query_readings", limit=7)

    ordered(
        compact(queries[0]),
        [
            '|>filter(fn:(r)=>r["_field"]=="value")',
            "|>group(columns:[",
            '|>pivot(rowKey:["_time"],columnKey:["metric_key"],valueColumn:"_value")',
            '|>sort(columns:["_time"])',
            "|>limit(n:7)",
        ],
    )
    assert group_columns(queries[0]) == [SERIES_KEY]


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ("query_latest_metrics", "query_latest_metrics_for_tenant"))
async def test_latest_takes_storage_last_per_series_and_metric(method: str) -> None:
    queries = await queries_for(method)
    flat = compact(queries[0])

    assert flat.endswith('|>filter(fn:(r)=>r["_field"]=="value")|>last()')
    assert group_columns(queries[0]) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ROW_METHODS)
async def test_seq_winner_drops_absent_sequences_then_ranks_per_series(method: str) -> None:
    queries = await queries_for(method)
    flat = compact(queries[1])

    assert or_filter_values(queries[1], "_field") == {"sequence", "ingested_at_ns", "event_id"}
    ordered(
        flat,
        [
            '|>pivot(rowKey:["_time"],columnKey:["_field"],valueColumn:"_value")',
            '|>filter(fn:(r)=>r["sequence"]!=-1)',
            "|>group(columns:[",
            '|>sort(columns:["_time","ingested_at_ns","event_id"],desc:true)',
            '|>unique(column:"_time")',
        ],
    )
    # One table per legacy series, never one per timestamp.
    assert group_columns(queries[1]) == [SERIES_KEY]
    assert "float(" not in flat and "int(" not in flat


@pytest.mark.asyncio
async def test_seq_query_spans_only_the_returned_rows() -> None:
    rows = [
        value_row(T1, **{"water.ph": 7.1}),
        value_row(T2, device="dev_b", **{"water.ph": 7.2}),
        value_row(T0, **{"water.ph": 7.0}),
    ]
    queries = await queries_for("query_readings", responses=([FakeTable(rows)],))

    stop = T2 + timedelta(microseconds=1)
    assert _range(queries[1]) == f"range(start:{_flux_time(T0)},stop:{_flux_time(stop)})"


@pytest.mark.asyncio
async def test_latest_seq_query_spans_only_the_newest_points() -> None:
    lasts = [
        last_row(T0, "water.temperature", 25.0),
        last_row(T1, "water.ph", 7.1),
        last_row(T2, "water.ph", 7.2, device="dev_b"),
    ]
    queries = await queries_for("query_latest_metrics", responses=([FakeTable(lasts)],))

    stop = T2 + timedelta(microseconds=1)
    assert _range(queries[1]) == f"range(start:{_flux_time(T1)},stop:{_flux_time(stop)})"


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ROW_METHODS)
async def test_no_rows_means_no_seq_query(method: str) -> None:
    query_api = FakeQueryApi()

    await call(projection(query_api), method)

    assert len(query_api.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(("period", "interval"), [("24h", "5m"), ("7d", "1h"), ("30d", "1h")])
async def test_summary_aggregates_in_storage_by_metric(period: str, interval: str) -> None:
    (query,) = await queries_for("query_summary", period=period)
    flat = compact(query)

    ordered(flat, ['|>filter(fn:(r)=>r["_field"]=="value")', '|>group(columns:["metric_key"])'])
    assert (
        f'data|>aggregateWindow(every:{interval},fn:mean,createEmpty:false)|>yield(name:"series")'
        in flat
    )
    for result in ("mean", "min", "max", "count"):
        assert f'data|>{result}()|>yield(name:"{result}")' in flat
    # Aggregated server-side: raw rows are never transferred.
    assert "pivot(" not in flat
    assert "limit(" not in flat


# --- Python shaping ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_readings_rename_metrics_and_merge_seq_winners() -> None:
    values = [
        value_row(
            T0,
            **{
                "water.temperature": 25.1,
                "water.ph": 7.2,
                "water.dissolved_oxygen": 6.4,
                "water.turbidity": 3.1,
                "water.salinity": 11.8,
                "device.battery_voltage": 3.82,
                "device.rssi": -67.0,
            },
        ),
        value_row(T1, **{"water.ph": 7.3, "device.rssi": None}),
        value_row(T2, **{"water.ph": 7.4}),
    ]
    # A winner outside the returned rows is ignored.
    seqs = [seq_row(T0, 2**63 - 1), seq_row(T2, 3), seq_row(T1 + timedelta(seconds=1), 9)]
    query_api = FakeQueryApi([FakeTable(values)], [FakeTable(seqs)])

    readings = await call(projection(query_api), "query_readings")

    identity = {"tenant_id": TENANT, "pond_id": POND, "device_id": "dev_a"}
    assert [r.model_dump(exclude_none=True) for r in readings] == [
        {
            "measured_at": T0,
            **identity,
            "temp_c": 25.1,
            "ph": 7.2,
            "do_mg_l": 6.4,
            "turbidity_ntu": 3.1,
            "salinity_ppt": 11.8,
            "battery_v": 3.82,
            "rssi": -67,
            "seq": 2**63 - 1,
        },
        {"measured_at": T1, **identity, "ph": 7.3},
        {"measured_at": T2, **identity, "ph": 7.4, "seq": 3},
    ]
    assert type(readings[0].rssi) is int
    assert type(readings[0].seq) is int


@pytest.mark.asyncio
async def test_readings_come_in_legacy_series_order_then_time() -> None:
    values = [
        value_row(T1, device="dev_b", **{"water.ph": 7.0}),
        value_row(T0, device="dev_b", **{"water.ph": 7.1}),
        value_row(T2, device="dev_a", **{"water.ph": 7.2}),
        value_row(T0, device="dev_c", asset=UNKNOWN_ASSET, **{"water.ph": 1.0}),
    ]

    readings = await call(projection(FakeQueryApi([FakeTable(values)])), "query_readings")

    assert [(r.device_id, r.measured_at) for r in readings] == [
        ("dev_a", T2),
        ("dev_b", T0),
        ("dev_b", T1),
    ]


@pytest.mark.asyncio
async def test_integer_fields_are_built_as_int_without_lax_coercion(monkeypatch) -> None:
    module = sys.modules[V2ProjectedTelemetryRepository.__module__]

    class StrictReading(TelemetryReading):
        model_config = ConfigDict(frozen=True, strict=True)

    class StrictLatest(LatestMetrics):
        model_config = ConfigDict(frozen=True, strict=True)

    monkeypatch.setattr(module, "TelemetryReading", StrictReading)
    monkeypatch.setattr(module, "LatestMetrics", StrictLatest)
    values = [value_row(T0, **{"water.ph": 7.0, "device.rssi": -67.0})]
    lasts = [last_row(T0, "device.rssi", -70.0)]

    (reading,) = await call(
        projection(FakeQueryApi([FakeTable(values)], [FakeTable([seq_row(T0, 5)])])),
        "query_readings",
    )
    latest = await call(projection(FakeQueryApi([FakeTable(lasts)])), "query_latest_metrics")

    assert (reading.rssi, reading.seq, latest.rssi) == (-67, 5, -70)


@pytest.mark.asyncio
async def test_a_non_integral_rssi_is_not_visible() -> None:
    values = [value_row(T0, **{"water.ph": 7.0, "device.rssi": -67.5})]

    (reading,) = await call(projection(FakeQueryApi([FakeTable(values)])), "query_readings")

    assert reading.rssi is None
    assert reading.ph == 7.0


@pytest.mark.asyncio
async def test_a_non_integer_sequence_is_never_coerced() -> None:
    values = [value_row(T0, **{"water.ph": 7.0})]
    query_api = FakeQueryApi([FakeTable(values)], [FakeTable([seq_row(T0, 3.0)])])

    with pytest.raises(TypeError):
        await call(projection(query_api), "query_readings")


@pytest.mark.asyncio
async def test_latest_keeps_only_the_fields_of_the_newest_point() -> None:
    lasts = [
        last_row(T0, "water.temperature", 25.0),
        last_row(T1, "water.ph", 7.1),
        last_row(T1, "device.rssi", -70.0),
    ]
    query_api = FakeQueryApi([FakeTable(lasts)], [FakeTable([seq_row(T1, 4)])])

    latest = await call(projection(query_api), "query_latest_metrics")

    assert latest == LatestMetrics(
        measured_at=T1, tenant_id=TENANT, pond_id=POND, ph=7.1, rssi=-70, seq=4
    )
    assert type(latest.rssi) is int


@pytest.mark.asyncio
async def test_latest_drops_a_seq_that_is_not_at_the_newest_point() -> None:
    query_api = FakeQueryApi(
        [FakeTable([last_row(T1, "water.ph", 7.1)])], [FakeTable([seq_row(T0, 4)])]
    )

    latest = await call(projection(query_api), "query_latest_metrics")

    assert latest == LatestMetrics(measured_at=T1, tenant_id=TENANT, pond_id=POND, ph=7.1)


@pytest.mark.asyncio
async def test_latest_picks_the_newest_series_and_the_first_on_a_tie() -> None:
    lasts = [
        last_row(T1, "water.ph", 7.2, device="dev_c"),
        last_row(T1, "water.ph", 7.1, device="dev_b"),
        last_row(T0, "water.ph", 7.0, device="dev_a"),
    ]

    latest = await call(projection(FakeQueryApi([FakeTable(lasts)])), "query_latest_metrics")

    # dev_b and dev_c tie at T1; legacy keeps the first series table (device order).
    assert latest == LatestMetrics(measured_at=T1, tenant_id=TENANT, pond_id=POND, ph=7.1)


@pytest.mark.asyncio
async def test_latest_without_data_is_the_empty_legacy_shape() -> None:
    latest = await call(projection(FakeQueryApi()), "query_latest_metrics")

    assert latest == LatestMetrics(tenant_id=TENANT, pond_id=POND)


@pytest.mark.asyncio
async def test_tenant_latest_is_the_newest_point_per_pond_sorted_by_pond() -> None:
    asset_2 = asset_id_for_legacy_pond(TENANT, "pond_2")
    lasts = [
        last_row(T0, "water.ph", 7.0, device="dev_b", asset=asset_2),
        last_row(T1, "water.ph", 7.1, device="dev_c", asset=asset_2),
        last_row(T0, "water.ph", 6.9, device="dev_a"),
        last_row(T2, "water.ph", 1.0, asset=UNKNOWN_ASSET),
    ]

    latest = await call(
        projection(FakeQueryApi([FakeTable(lasts)])), "query_latest_metrics_for_tenant"
    )

    assert latest == [
        LatestMetrics(measured_at=T0, tenant_id=TENANT, pond_id="pond_1", ph=6.9),
        LatestMetrics(measured_at=T1, tenant_id=TENANT, pond_id="pond_2", ph=7.1),
    ]


@pytest.mark.asyncio
async def test_summary_maps_metric_keys_to_the_legacy_fields() -> None:
    def record(result: str, metric: str, value: Any, time: datetime = T1) -> FakeRecord:
        return FakeRecord({"result": result, "metric_key": metric, "_value": value, "_time": time})

    tables = [
        FakeTable(
            [
                record("series", "water.temperature", 25.5, T0),
                record("series", "water.ph", 7.1, T1),
                record("series", "water.dissolved_oxygen", None, T1),
            ]
        ),
        FakeTable([record("mean", "water.temperature", 25.5), record("count", "water.ph", 2)]),
        FakeTable([record("min", "water.ph", 7.0), record("max", "water.ph", 7.2)]),
        FakeTable([record("max", "device.rssi", -60.0), record("mean", "water.turbidity", 1.0)]),
    ]

    summary = await call(projection(FakeQueryApi(tables)), "query_summary", period="7d")

    assert summary.interval == "1h"
    assert [(p.measured_at, p.temp_c, p.ph, p.do_mg_l) for p in summary.series] == [
        (T0, 25.5, None, None),
        (T1, None, 7.1, None),
    ]
    assert summary.statistics == {
        "do_mg_l": MetricStatistics(),
        "ph": MetricStatistics(min=7.0, max=7.2, count=2),
        "temp_c": MetricStatistics(mean=25.5),
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("method", METHODS)
async def test_a_tenant_without_legacy_devices_reads_empty_like_legacy(method: str) -> None:
    query_api = FakeQueryApi()
    expected = await call(legacy(FakeQueryApi()), method)
    actual = await call(projection(query_api, FakeDomainRepository(devices={})), method)

    assert actual == expected
    assert query_api.calls == []


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
@pytest.mark.parametrize("method", ROW_METHODS)
async def test_other_methods_propagate_query_failures_unwrapped(method: str) -> None:
    error = RuntimeError("influx down")

    with pytest.raises(RuntimeError) as raised:
        await call(projection(FakeQueryApi(error=error)), method)

    assert raised.value is error
    assert not isinstance(raised.value, TelemetryQueryError)


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ROW_METHODS)
async def test_a_failing_seq_query_propagates_unwrapped(method: str) -> None:
    error = RuntimeError("influx down")

    class FailsSecond(FakeQueryApi):
        def query(self, query: str, org: str):
            if self.calls:
                self.calls.append({"query": query, "org": org})
                raise error
            return super().query(query, org)

    with pytest.raises(RuntimeError) as raised:
        await call(projection(FailsSecond(*one_row(method))), method)

    assert raised.value is error
