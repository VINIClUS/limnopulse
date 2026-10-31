"""P2-04 reader unit tests: Flux shape, escaping, ID validation and row parsing (fake query_api)."""

import json
import re
from datetime import UTC, datetime, timedelta

import pytest
from influxdb_client.client.flux_table import FluxRecord, FluxTable, TableList

from limnopulse_api.adapters import influxdb_v2_reader
from limnopulse_api.adapters.influxdb_v2_reader import InfluxObservationReader
from limnopulse_api.core.errors import TelemetryQueryError
from limnopulse_api.domain.metric_catalog import metric_catalog
from limnopulse_api.domain.telemetry_v2 import (
    ABSENT_SEQUENCE,
    ABSENT_STRING,
    CanonicalObservation,
    QualityFlag,
    QualityProvenance,
    QualityState,
    SourceType,
    StoredObservation,
)
from limnopulse_api.domain.telemetry_v2_schema import flux_string_literal
from limnopulse_api.repositories.telemetry_v2 import ObservationReader, ObservationSelector
from tests.support.telemetry_v2_vectors import scenario

ORG = "org-under-test"
BUCKET = "raw"
TENANT = "tnt_local_001"
ASSET_ID = "ast_751707aef3455ab2ba60733d238b594f"
COMPONENT_ID = "cmp_32c73f5ac8dc596e9446956875639204"
DEVICE_ID = "local-device-001"
START = datetime(2026, 7, 9, 11, 0, 0, 123456, tzinfo=UTC)
STOP = datetime(2026, 7, 9, 13, 0, tzinfo=UTC)
NOW = datetime(2026, 7, 9, 12, 30, 0, 654321, tzinfo=UTC)
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class FakeQueryApi:
    """Records every Flux query; returns canned tables or raises."""

    def __init__(self, tables: list[FluxTable] | None = None, error: Exception | None = None):
        self.tables = tables or []
        self.error = error
        self.calls: list[tuple[str, str | None]] = []

    def query(self, query: str, org: str | None = None, **kwargs: object) -> TableList:
        self.calls.append((query, org))
        if self.error is not None:
            raise self.error
        result = TableList()
        result.extend(self.tables)
        return result


def _ns(value: datetime) -> int:
    return ((value - _EPOCH) // timedelta(microseconds=1)) * 1000


def row_for(o: CanonicalObservation) -> dict[str, object]:
    """A pivoted `observation_numeric` row as InfluxDB returns it for the encoded point."""
    resolved = o.resolved
    values: dict[str, object] = {
        "result": "_result",
        "table": 0,
        "_start": START,
        "_stop": STOP,
        "_time": o.observation.observed_at,
        "_measurement": "observation_numeric",
        "asset_id": resolved.asset_id,
        "component_id": resolved.component_id,
        "deployment_id": resolved.deployment_id,
        "device_id": resolved.device_id,
        "metric_key": o.observation.metric_key,
        "schema_version": "2",
        "site_id": resolved.site_id,
        "tenant_id": resolved.tenant_id,
        "catalog_version": o.mapping.catalog_version,
        "delay_ms": (_ns(o.source.received_at) - _ns(o.observation.observed_at)) // 1_000_000,
        "event_id": o.event_id,
        "ingested_at_ns": _ns(o.delivery.ingested_at),
        "integration_id": o.source.integration_id or ABSENT_STRING,
        "quality_flags_json": json.dumps([str(f) for f in o.quality.flags], separators=(",", ":")),
        "quality_policy_version": o.mapping.quality_policy_version,
        "quality_provenance_json": json.dumps(
            [str(p) for p in o.quality.provenance], separators=(",", ":")
        ),
        "quality_state": str(o.quality.state),
        "received_at_ns": _ns(o.source.received_at),
        "sequence": ABSENT_SEQUENCE if o.observation.sequence is None else o.observation.sequence,
        "source_event_id": o.source_event_id or ABSENT_STRING,
        "source_type": str(o.source.source_type),
        "source_unit": o.observation.source_unit,
        "unit_mapping_version": o.mapping.unit_mapping_version,
        "value": o.observation.value,
    }
    if resolved.probe_id is not None:
        values["probe_id"] = resolved.probe_id
    return values


def stored_for(o: CanonicalObservation) -> StoredObservation:
    resolved = o.resolved
    return StoredObservation(
        event_id=o.event_id,
        observed_at=o.observation.observed_at,
        received_at=o.source.received_at,
        ingested_at=o.delivery.ingested_at,
        metric_key=o.observation.metric_key,
        value=o.observation.value,
        canonical_unit=o.observation.canonical_unit,
        site_id=resolved.site_id,
        asset_id=resolved.asset_id,
        device_id=resolved.device_id,
        component_id=resolved.component_id,
        probe_id=resolved.probe_id,
        deployment_id=resolved.deployment_id,
        quality_state=o.quality.state,
        quality_flags=o.quality.flags,
        quality_provenance=o.quality.provenance,
        sequence=o.observation.sequence,
        source_type=o.source.source_type,
    )


def table_of(*rows: dict[str, object]) -> FluxTable:
    table = FluxTable()
    table.records = [FluxRecord(table=0, values=dict(row)) for row in rows]
    return table


def reader_for(api: FakeQueryApi, bucket: str = BUCKET) -> InfluxObservationReader:
    return InfluxObservationReader(query_api=api, org=ORG, bucket=bucket)


async def run_query(
    api: FakeQueryApi,
    *,
    selector: ObservationSelector | None = None,
    metric_keys: frozenset[str] | None = None,
    quality_states: frozenset[QualityState] | None = None,
    limit: int = 500,
    tenant_id: str = TENANT,
    bucket: str = BUCKET,
) -> list[StoredObservation]:
    return await reader_for(api, bucket).query_observations(
        tenant_id=tenant_id,
        selector=selector or ObservationSelector("asset", ASSET_ID),
        metric_keys=metric_keys,
        quality_states=quality_states,
        start=START,
        stop=STOP,
        limit=limit,
    )


async def run_latest(
    api: FakeQueryApi,
    *,
    selector: ObservationSelector | None = None,
    metric_keys: frozenset[str] | None = None,
    tenant_id: str = TENANT,
    bucket: str = BUCKET,
    lookback: timedelta = timedelta(hours=24),
) -> list[StoredObservation]:
    return await reader_for(api, bucket).query_latest_observations(
        tenant_id=tenant_id,
        selector=selector or ObservationSelector("asset", ASSET_ID),
        metric_keys=metric_keys,
        lookback=lookback,
        now=NOW,
    )


def only_query(api: FakeQueryApi) -> str:
    assert len(api.calls) == 1
    query, org = api.calls[0]
    assert org == ORG
    return query


def tag_equals(tag: str, value: str) -> re.Pattern[str]:
    """`r["tag"] == <literal>` or `r.tag == <literal>`, the literal escaped by the helper."""
    return re.compile(
        rf'r(?:\["{re.escape(tag)}"\]|\.{re.escape(tag)})\s*==\s*{re.escape(flux_string_literal(value))}'
    )


def _parse_flux_time(argument: str) -> datetime:
    argument = argument.strip()
    match = re.fullmatch(r'time\(\s*v\s*:\s*"([^"]+)"\s*\)', argument)
    text = match.group(1) if match else argument
    head, _, fraction = text.rstrip("Z").partition(".")
    fraction = fraction[:6].ljust(6, "0") if fraction else "000000"
    assert text.endswith("Z"), f"range bound must be an RFC3339 UTC time: {argument!r}"
    return datetime.fromisoformat(f"{head}.{fraction}+00:00")


def range_bounds(query: str) -> tuple[datetime, datetime]:
    match = re.search(
        r"range\(\s*start\s*:\s*(.+?)\s*,\s*stop\s*:\s*(.+?)\s*\)\s*(?:\n|\|>)", query
    )
    assert match is not None, query
    return _parse_flux_time(match.group(1)), _parse_flux_time(match.group(2))


def position(query: str, pattern: str) -> int:
    match = re.search(pattern, query)
    assert match is not None, f"{pattern!r} not found in:\n{query}"
    return match.start()


def assert_no_unescaped_interpolation(query: str) -> None:
    # P2 plan D19: every `$` is written as `\x24`, so no query keeps a raw `$`.
    assert "$" not in query, query


# --- protocol -----------------------------------------------------------------------------


def test_reader_implements_the_observation_reader_protocol() -> None:
    assert isinstance(reader_for(FakeQueryApi()), ObservationReader)


def test_constructor_is_keyword_only() -> None:
    with pytest.raises(TypeError):
        InfluxObservationReader(FakeQueryApi(), ORG, BUCKET)  # type: ignore[misc]


# --- query_observations: Flux shape ---------------------------------------------------------


@pytest.mark.asyncio
async def test_observations_query_filters_measurement_tenant_and_window() -> None:
    api = FakeQueryApi()
    await run_query(api)
    query = only_query(api)

    assert f"from(bucket: {flux_string_literal(BUCKET)})" in query
    assert range_bounds(query) == (START, STOP)
    assert tag_equals("_measurement", "observation_numeric").search(query)
    assert tag_equals("tenant_id", TENANT).search(query)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "tag", "entity_id"),
    [
        ("asset", "asset_id", ASSET_ID),
        ("component", "component_id", COMPONENT_ID),
        ("device", "device_id", DEVICE_ID),
    ],
)
async def test_selector_filters_exactly_its_own_tag(kind: str, tag: str, entity_id: str) -> None:
    api = FakeQueryApi()
    await run_query(api, selector=ObservationSelector(kind, entity_id))  # type: ignore[arg-type]
    query = only_query(api)

    assert tag_equals(tag, entity_id).search(query)
    for other in {"asset_id", "component_id", "device_id"} - {tag}:
        assert re.search(rf'r(?:\["{other}"\]|\.{other})\s*==', query) is None, query


@pytest.mark.asyncio
async def test_metric_keys_filter_each_escaped_key_and_is_absent_without_keys() -> None:
    api = FakeQueryApi()
    keys = frozenset({"water.ph", "device.rssi"})
    await run_query(api, metric_keys=keys)
    filtered = only_query(api)
    for key in keys:
        assert flux_string_literal(key) in filtered
    assert flux_string_literal("water.temperature") not in filtered

    unfiltered_api = FakeQueryApi()
    await run_query(unfiltered_api)
    unfiltered = only_query(unfiltered_api)
    for definition in metric_catalog():
        assert flux_string_literal(definition.metric_key) not in unfiltered


@pytest.mark.asyncio
async def test_quality_states_filter_after_pivot_and_is_absent_without_states() -> None:
    api = FakeQueryApi()
    states = frozenset({QualityState.VALID, QualityState.OUT_OF_RANGE})
    await run_query(api, quality_states=states)
    query = only_query(api)
    pivot = position(query, r"\|>\s*pivot\(")
    for state in states:
        literal = flux_string_literal(str(state))
        assert literal in query
        # quality_state is a field (D5), so it can only be compared on the pivoted row.
        assert query.index(literal, pivot) > pivot
    assert flux_string_literal("suspect") not in query

    unfiltered_api = FakeQueryApi()
    await run_query(unfiltered_api)
    unfiltered = only_query(unfiltered_api)
    for state in QualityState:
        assert flux_string_literal(str(state)) not in unfiltered


@pytest.mark.asyncio
async def test_observations_pivot_then_ungroup_then_global_sort_then_limit() -> None:
    api = FakeQueryApi()
    await run_query(api, limit=37)
    query = only_query(api)

    pivot = position(query, r"\|>\s*pivot\(")
    group = position(query, r"\|>\s*group\(\s*\)")
    sort = position(
        query,
        r'\|>\s*sort\(\s*columns\s*:\s*\[\s*"_time"\s*,\s*"component_id"\s*,\s*"metric_key"\s*\]\s*\)',
    )
    limit = position(query, r"\|>\s*limit\(\s*n\s*:\s*37\s*\)")
    assert pivot < group < sort < limit
    # never group() before pivot: the first group of any kind follows the pivot
    assert position(query, r"\|>\s*group\(") > pivot
    assert re.search(r"desc\s*:\s*true", query) is None


# --- query_latest_observations: Flux shape --------------------------------------------------


@pytest.mark.asyncio
async def test_latest_window_is_now_minus_lookback_to_now() -> None:
    api = FakeQueryApi()
    await run_latest(api)
    query = only_query(api)

    assert range_bounds(query) == (NOW - timedelta(hours=24), NOW)
    assert tag_equals("_measurement", "observation_numeric").search(query)
    assert tag_equals("tenant_id", TENANT).search(query)
    assert tag_equals("asset_id", ASSET_ID).search(query)


@pytest.mark.asyncio
async def test_latest_chain_is_pivot_group_sort_last_ungroup_sort() -> None:
    api = FakeQueryApi()
    await run_latest(api, metric_keys=frozenset({"water.ph"}))
    query = only_query(api)

    pivot = position(query, r"\|>\s*pivot\(")
    group_pair = position(
        query, r'\|>\s*group\(\s*columns\s*:\s*\[\s*"component_id"\s*,\s*"metric_key"\s*\]\s*\)'
    )
    sort_time = position(query, r'\|>\s*sort\(\s*columns\s*:\s*\[\s*"_time"\s*\]\s*\)')
    last = position(query, r'\|>\s*last\(\s*column\s*:\s*"_time"\s*\)')
    ungroup = position(query, r"\|>\s*group\(\s*\)")
    final_sort = position(
        query, r'\|>\s*sort\(\s*columns\s*:\s*\[\s*"component_id"\s*,\s*"metric_key"\s*\]\s*\)'
    )
    assert pivot < group_pair < sort_time < last < ungroup < final_sort
    assert position(query, r"\|>\s*group\(") > pivot
    assert flux_string_literal("water.ph") in query
    assert re.search(r"desc\s*:\s*true", query) is None


@pytest.mark.asyncio
async def test_latest_orders_by_observed_time_never_by_receipt_or_ingest() -> None:
    api = FakeQueryApi()
    await run_latest(api)
    query = only_query(api)
    for column in ("received_at_ns", "ingested_at_ns"):
        assert re.search(rf'sort\([^)]*"{column}"', query) is None
        assert re.search(rf'last\([^)]*"{column}"', query) is None


@pytest.mark.asyncio
async def test_latest_uses_the_given_lookback() -> None:
    api = FakeQueryApi()
    await run_latest(api, lookback=timedelta(hours=2, minutes=30))
    assert range_bounds(only_query(api)) == (NOW - timedelta(hours=2, minutes=30), NOW)


@pytest.mark.asyncio
async def test_latest_without_metric_keys_has_no_metric_or_quality_filter() -> None:
    api = FakeQueryApi()
    await run_latest(api)
    query = only_query(api)
    for definition in metric_catalog():
        assert flux_string_literal(definition.metric_key) not in query
    for state in QualityState:
        assert flux_string_literal(str(state)) not in query


# --- escaping and validation ---------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("bucket", ['raw"${evil}', "raw$archive", 'raw\\") |> drop(columns: ["x"]'])
async def test_bucket_is_always_a_helper_escaped_literal(bucket: str) -> None:
    for run in (run_query, run_latest):
        api = FakeQueryApi()
        await run(api, bucket=bucket)
        query = only_query(api)
        assert f"from(bucket: {flux_string_literal(bucket)})" in query
        assert_no_unescaped_interpolation(query)
    if bucket == "raw$archive":
        assert '"raw\\x24archive"' in query


@pytest.mark.asyncio
@pytest.mark.parametrize("bucket", ["raw\x00", "raw\x1b[0m", "raw\ud800"], ids=ascii)
async def test_bucket_that_cannot_round_trip_is_rejected_before_any_query(bucket: str) -> None:
    # P2 plan D19 (#84): the helper raises, so nothing reaches InfluxDB.
    for run in (run_query, run_latest):
        api = FakeQueryApi()
        with pytest.raises(ValueError):
            await run(api, bucket=bucket)
        assert api.calls == []


@pytest.mark.asyncio
async def test_every_runtime_literal_goes_through_the_escaping_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[str] = []

    def spy(value: str) -> str:
        seen.append(value)
        return flux_string_literal(value)

    monkeypatch.setattr(influxdb_v2_reader, "flux_string_literal", spy, raising=True)
    keys = frozenset({"water.ph", "device.rssi"})
    states = frozenset({QualityState.VALID, QualityState.SUSPECT})

    await run_query(
        FakeQueryApi(),
        selector=ObservationSelector("device", DEVICE_ID),
        metric_keys=keys,
        quality_states=states,
    )
    expected = {BUCKET, TENANT, DEVICE_ID, *keys, *(str(state) for state in states)}
    assert expected <= {str(value) for value in seen}

    seen.clear()
    await run_latest(
        FakeQueryApi(), selector=ObservationSelector("component", COMPONENT_ID), metric_keys=keys
    )
    assert {BUCKET, TENANT, COMPONENT_ID, *keys} <= {str(value) for value in seen}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tenant_id",
    [
        'tnt") |> drop(columns: ["tenant_id"]) //',
        "tnt_${secret}",
        "",
        "tnt a",
        "tnt\u2028x",
        "-tnt",
    ],
)
async def test_invalid_tenant_ids_are_rejected_before_any_query(tenant_id: str) -> None:
    for run in (run_query, run_latest):
        api = FakeQueryApi()
        with pytest.raises(ValueError):
            await run(api, tenant_id=tenant_id)
        assert api.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("metric_key", ['water.ph" or true or "', "water.${x}", "water.oxygen", ""])
async def test_metric_keys_outside_the_catalog_are_rejected_before_any_query(
    metric_key: str,
) -> None:
    for run in (run_query, run_latest):
        api = FakeQueryApi()
        with pytest.raises(ValueError):
            await run(api, metric_keys=frozenset({"water.ph", metric_key}))
        assert api.calls == []


@pytest.mark.asyncio
async def test_non_enum_quality_state_is_rejected_before_any_query() -> None:
    api = FakeQueryApi()
    with pytest.raises(ValueError):
        await run_query(api, quality_states=frozenset({'valid" or true or "'}))  # type: ignore[arg-type]
    assert api.calls == []


@pytest.mark.asyncio
async def test_query_failure_is_wrapped_in_telemetry_query_error() -> None:
    for run in (run_query, run_latest):
        api = FakeQueryApi(error=RuntimeError("influx down"))
        with pytest.raises(TelemetryQueryError):
            await run(api)


# --- row parsing ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rows_parse_every_field_into_stored_observations_in_returned_order() -> None:
    observations = scenario("legacy_sample").observations
    ordered = sorted(observations, key=lambda o: o.observation.metric_key)
    api = FakeQueryApi([table_of(*(row_for(o) for o in ordered))])

    result = await run_query(api)

    assert result == [stored_for(o) for o in ordered]
    assert all(isinstance(item, StoredObservation) for item in result)
    rssi = next(item for item in result if item.metric_key == "device.rssi")
    assert rssi.value == -67.0 and rssi.canonical_unit == "dBm"
    assert rssi.sequence == 1 and rssi.source_type is SourceType.LEGACY_MQTT


@pytest.mark.asyncio
async def test_rows_from_several_tables_are_all_returned() -> None:
    first, second = scenario("two_probes_same_metric").observations
    api = FakeQueryApi([table_of(row_for(first)), table_of(row_for(second))])

    result = await run_query(api)

    assert result == [stored_for(first), stored_for(second)]
    assert {item.probe_id for item in result} == {
        first.resolved.probe_id,
        second.resolved.probe_id,
    }


@pytest.mark.asyncio
async def test_sentinels_decode_to_none() -> None:
    observation = scenario("two_probes_same_metric").observations[0]
    assert observation.observation.sequence is None
    row = row_for(observation)
    assert row["sequence"] == ABSENT_SEQUENCE
    without_probe = {key: value for key, value in row.items() if key != "probe_id"}
    empty_probe = {**row, "probe_id": ABSENT_STRING}
    null_probe = {**row, "probe_id": None}
    api = FakeQueryApi([table_of(without_probe, empty_probe, null_probe)])

    result = await run_query(api)

    assert len(result) == 3
    for item in result:
        assert item.sequence is None
        assert item.probe_id is None


@pytest.mark.asyncio
async def test_zero_sequence_is_not_a_sentinel() -> None:
    observation = scenario("legacy_sample").observations[0]
    api = FakeQueryApi([table_of({**row_for(observation), "sequence": 0})])

    (item,) = await run_query(api)

    assert item.sequence == 0


@pytest.mark.asyncio
async def test_nanosecond_fields_convert_exactly_to_aware_microsecond_datetimes() -> None:
    observation = scenario("legacy_sample").observations[0]
    # Near the upper bound a float conversion loses microseconds (float spacing ~1 µs there).
    received = datetime(2261, 12, 31, 23, 59, 59, 999_999, tzinfo=UTC)
    ingested = datetime(2261, 12, 31, 23, 59, 58, 123_457, tzinfo=UTC)
    row = {**row_for(observation), "received_at_ns": _ns(received), "ingested_at_ns": _ns(ingested)}
    api = FakeQueryApi([table_of(row)])

    (item,) = await run_query(api)

    assert item.received_at == received and item.ingested_at == ingested
    assert item.received_at.utcoffset() == timedelta(0)
    assert item.ingested_at.utcoffset() == timedelta(0)


@pytest.mark.asyncio
async def test_quality_json_fields_decode_to_ordered_enum_tuples() -> None:
    observation = scenario("delayed_replay").observations[0]
    api = FakeQueryApi([table_of(row_for(observation))])

    (item,) = await run_query(api)

    assert item.quality_flags == (QualityFlag.DELAYED, QualityFlag.REPLAY)
    assert item.quality_provenance == observation.quality.provenance
    assert QualityProvenance.REPLAY in item.quality_provenance
    assert item.quality_state is QualityState.VALID


@pytest.mark.asyncio
async def test_latest_rows_parse_one_item_per_component_and_metric() -> None:
    first, second = scenario("two_probes_same_metric").observations
    api = FakeQueryApi([table_of(row_for(first), row_for(second))])

    result = await run_latest(api)

    assert result == [stored_for(first), stored_for(second)]
    assert len({(item.component_id, item.metric_key) for item in result}) == 2


@pytest.mark.asyncio
async def test_unit_is_resolved_under_the_stored_catalog_version() -> None:
    observation = scenario("legacy_sample").observations[0]
    unknown_version = {**row_for(observation), "catalog_version": "metric-catalog@999"}
    api = FakeQueryApi([table_of(unknown_version)])

    with pytest.raises(TelemetryQueryError):
        await run_query(api)


@pytest.mark.asyncio
async def test_unit_comes_from_the_stored_catalog_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observation = scenario("legacy_sample").observations[0]
    seen: list[tuple[str, str]] = []
    real = influxdb_v2_reader.metric_definition

    def recording(metric_key: str, **kwargs: str):
        seen.append((metric_key, kwargs.get("version", "")))
        return real(metric_key, **kwargs)

    monkeypatch.setattr(influxdb_v2_reader, "metric_definition", recording, raising=True)
    api = FakeQueryApi([table_of(row_for(observation))])

    (item,) = await run_query(api)

    assert item.canonical_unit == observation.observation.canonical_unit
    assert (observation.observation.metric_key, observation.mapping.catalog_version) in seen


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [0, -1, True])
async def test_non_positive_limit_is_rejected_before_any_query(limit: int) -> None:
    api = FakeQueryApi()
    with pytest.raises(ValueError):
        await run_query(api, limit=limit)
    assert api.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("stop", [START, START - timedelta(microseconds=1)])
async def test_empty_or_reversed_window_is_rejected_before_any_query(stop: datetime) -> None:
    api = FakeQueryApi()
    with pytest.raises(ValueError):
        await reader_for(api).query_observations(
            tenant_id=TENANT,
            selector=ObservationSelector("asset", ASSET_ID),
            metric_keys=None,
            quality_states=None,
            start=START,
            stop=stop,
            limit=10,
        )
    assert api.calls == []
