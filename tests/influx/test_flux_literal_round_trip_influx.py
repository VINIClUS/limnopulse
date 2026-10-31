"""#84 (P2 plan D19): every `flux_string_literal` round-trips through InfluxDB 2.7.11.

The client reads results as annotated CSV, which turns `\\n` into `\\r\\n` and drops `\\r`. So each
value is compared on the server against a reference literal made of `\\x` escapes of its UTF-8
bytes, and only a boolean and a rune count come back.
"""

from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from influxdb_client import InfluxDBClient

from limnopulse_api.adapters.influxdb_v2_reader import InfluxObservationReader
from limnopulse_api.domain.telemetry_v2_schema import encode_observation_line, flux_string_literal
from limnopulse_api.repositories.telemetry_v2 import ObservationSelector
from tests.support.influx_harness import influx_clients, influx_settings, write_lines
from tests.support.telemetry_v2_vectors import scenario

# The #84 list, the characters Go `encoding/json` escapes even without HTML escaping, and
# neighbours of the escapes the rule emits.
ROUND_TRIP_VALUES = (
    "$${a}",
    "${a}",
    "$",
    "t$\n",
    "\\${z}",
    "$$$${{a}}",
    "a$\\${b}",
    "é",
    "<&>",
    'a"b\\c',
    "\u2028",
    "\u2029",
    '$"',
    "raw$archive",
    "\r\t",
    "a\x7fb\x85",
    "😀",
    "\\u2028",
    "\\x24",
    "\\",
    "}{$",
    "",
)

# Line protocol cannot carry an empty tag, a raw CR/LF/tab or a trailing backslash; spaces,
# commas and equals signs would need line-protocol escapes, so they are left out too.
TAG_VALUES = tuple(
    value
    for value in ROUND_TRIP_VALUES
    if value and not value.endswith("\\") and not set(value) & set("\n\r\t ,=")
)

TENANT = "tnt_local_001"
POND_ASSET = "ast_751707aef3455ab2ba60733d238b594f"
WINDOW = (datetime(2026, 7, 9, 11, 0, tzinfo=UTC), datetime(2026, 7, 9, 14, 0, tzinfo=UTC))


def hex_reference(value: str) -> str:
    return '"' + "".join(f"\\x{byte:02x}" for byte in value.encode("utf-8")) + '"'


def records(bucket: str, flux: str) -> list[dict[str, object]]:
    with influx_clients(bucket) as (query_api, _write_api):
        tables = query_api.query(flux, org=influx_settings().org)
    return [
        {key: value for key, value in record.values.items() if key not in ("result", "table")}
        for table in tables
        for record in table.records
    ]


@pytest.mark.parametrize("value", ROUND_TRIP_VALUES, ids=ascii)
def test_literal_round_trips_on_influxdb(influx_bucket: str, value: str) -> None:
    flux = (
        'import "array"\nimport "strings"\n'
        f"array.from(rows: [{{v: {flux_string_literal(value)}}}])\n"
        f"  |> map(fn: (r) => ({{equal: r.v == {hex_reference(value)}, "
        "length: strings.strlen(v: r.v)}))"
    )
    assert records(influx_bucket, flux) == [{"equal": True, "length": len(value)}]


def test_tag_filter_finds_exactly_the_written_value(influx_bucket: str) -> None:
    assert len(TAG_VALUES) >= 10
    lines = [
        f"flux_literal,probe={value} index={index}i 1767268800000000000"
        for index, value in enumerate(TAG_VALUES)
    ]
    write_lines(influx_bucket, lines)
    for index, value in enumerate(TAG_VALUES):
        flux = (
            f"from(bucket: {flux_string_literal(influx_bucket)})\n"
            "  |> range(start: 2026-01-01T00:00:00Z, stop: 2026-01-02T00:00:00Z)\n"
            '  |> filter(fn: (r) => r._measurement == "flux_literal")\n'
            f"  |> filter(fn: (r) => r.probe == {flux_string_literal(value)})\n"
            '  |> keep(columns: ["_value"])'
        )
        assert records(influx_bucket, flux) == [{"_value": index}], ascii(value)


@pytest.fixture
def hostile_bucket(influx_bucket: str) -> Iterator[str]:
    """A second bucket whose name needs every D19 step; `influx_bucket` supplies the skip."""
    name = f"{influx_bucket}_$${{a}}é<&>\u2028\\${{z}}"
    settings = influx_settings()
    client = InfluxDBClient(url=settings.url, token=settings.token, org=settings.org)
    try:
        bucket = client.buckets_api().create_bucket(bucket_name=name)
        try:
            yield name
        finally:
            client.buckets_api().delete_bucket(bucket)
    finally:
        client.close()


@pytest.mark.asyncio
async def test_v2_reader_reads_a_bucket_whose_name_needs_lossless_literals(
    hostile_bucket: str,
) -> None:
    observations = scenario("legacy_sample").observations
    write_lines(hostile_bucket, [encode_observation_line(o) for o in observations])
    with influx_clients(hostile_bucket) as (query_api, _write_api):
        reader = InfluxObservationReader(
            query_api=query_api, org=influx_settings().org, bucket=hostile_bucket
        )
        result = await reader.query_observations(
            tenant_id=TENANT,
            selector=ObservationSelector("asset", POND_ASSET),
            metric_keys=None,
            quality_states=None,
            start=WINDOW[0],
            stop=WINDOW[1],
            limit=1000,
        )
    assert result
    assert sorted(item.event_id for item in result) == sorted(o.event_id for o in observations)


@pytest.mark.parametrize(
    "value", ["\x00", "a\x01b", "\x08", "\x0c", "\x1b[0m", "$${\x1f}"], ids=ascii
)
def test_c0_controls_are_rejected_before_any_query(value: str) -> None:
    with pytest.raises(ValueError, match="control character"):
        flux_string_literal(value)
