from __future__ import annotations

import json
import os
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
from influxdb_client import InfluxDBClient

from tests.support.influx_harness import influx_clients, write_lines

pytest_plugins = ["pytester"]

ROOT = Path(__file__).resolve().parents[2]
MEASUREMENT = "harness_smoke"
LINE = f"{MEASUREMENT},probe=p1 value=21.5 1767268800000000000"
POINT_TIME = datetime(2026, 1, 1, 12, tzinfo=UTC)
FLUX = (
    'from(bucket: "{bucket}")'
    " |> range(start: 2026-01-01T00:00:00Z, stop: 2026-01-02T00:00:00Z)"
    f' |> filter(fn: (r) => r._measurement == "{MEASUREMENT}")'
)


def _rows(query_api, bucket: str) -> list[tuple[object, ...]]:
    return [
        (
            record.get_measurement(),
            record.get_field(),
            record.values.get("probe"),
            record.get_value(),
            record.get_time(),
        )
        for table in query_api.query(FLUX.format(bucket=bucket))
        for record in table.records
    ]


EXPECTED = [(MEASUREMENT, "value", "p1", 21.5, POINT_TIME)]


@pytest.fixture
def server() -> Iterator[InfluxDBClient]:
    """An independent client built from the contract defaults, not from the harness."""
    client = InfluxDBClient(
        url=os.getenv("INFLUX_INTEGRATION_URL") or "http://127.0.0.1:8086",
        token=os.getenv("INFLUX_INTEGRATION_TOKEN") or "local-dev-token",
        org=os.getenv("INFLUX_INTEGRATION_ORG") or "limnopulse",
    )
    try:
        yield client
    finally:
        client.close()


def test_write_lines_then_flux_reads_back_the_point(influx_bucket: str) -> None:
    write_lines(influx_bucket, [LINE])

    with influx_clients(influx_bucket) as (query_api, _write_api):
        rows = _rows(query_api, influx_bucket)

    assert rows, "the Flux read returned nothing for the written point"
    assert rows == EXPECTED


def test_yielded_write_api_writes_synchronously(influx_bucket: str) -> None:
    with influx_clients(influx_bucket) as (query_api, write_api):
        write_api.write(bucket=influx_bucket, record=LINE)
        rows = _rows(query_api, influx_bucket)

    assert rows, "the Flux read returned nothing for the written point"
    assert rows == EXPECTED


def test_bucket_exists_on_the_server_without_a_retention_rule(
    influx_bucket: str, server: InfluxDBClient
) -> None:
    bucket = server.buckets_api().find_bucket_by_name(influx_bucket)

    assert bucket is not None
    assert all(rule.every_seconds == 0 for rule in bucket.retention_rules or [])


INNER_TESTS = f'''
import json
import os

from tests.support.influx_harness import influx_bucket, influx_clients, write_lines  # noqa: F401

FLUX = {FLUX!r}
LINE = {LINE!r}


def _record(kind, bucket):
    with open(os.environ["HARNESS_SMOKE_LOG"], "a", encoding="utf-8") as log:
        log.write(json.dumps({{"kind": kind, "bucket": bucket}}) + "\\n")


def _count(bucket):
    with influx_clients(bucket) as (query_api, _write_api):
        return sum(len(table.records) for table in query_api.query(FLUX.format(bucket=bucket)))


def test_a_writes_one_point(influx_bucket):
    _record("a", influx_bucket)
    write_lines(influx_bucket, [LINE])
    assert _count(influx_bucket) == 1


def test_b_does_not_see_the_other_bucket(influx_bucket):
    _record("b", influx_bucket)
    assert _count(influx_bucket) == 0


def test_c_fails_after_writing(influx_bucket):
    _record("c", influx_bucket)
    write_lines(influx_bucket, [LINE])
    assert False, "deliberate failure: teardown must still delete the bucket"
'''


def test_buckets_are_isolated_and_deleted_at_teardown(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch, server: InfluxDBClient
) -> None:
    log = pytester.path / "buckets.jsonl"
    monkeypatch.setenv("HARNESS_SMOKE_LOG", str(log))
    monkeypatch.setenv("PYTHONPATH", str(ROOT))
    pytester.makepyfile(test_inner=INNER_TESTS)

    result = pytester.runpytest_subprocess("-p", "no:cacheprovider")

    result.assert_outcomes(passed=2, failed=1)
    records = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    assert [record["kind"] for record in records] == ["a", "b", "c"]
    names = [record["bucket"] for record in records]
    assert len(set(names)) == 3, names
    for name in names:
        assert server.buckets_api().find_bucket_by_name(name) is None, name
