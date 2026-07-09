from datetime import UTC, datetime

import pytest

from limnopulse_api.adapters.influxdb import InfluxTelemetryRepository


class FakeRecord:
    def __init__(self, values: dict[str, object]) -> None:
        self.values = values


class FakeTable:
    def __init__(self, records: list[FakeRecord]) -> None:
        self.records = records


class FakeQueryApi:
    def __init__(self, tables: list[FakeTable]) -> None:
        self.tables = tables
        self.calls: list[dict[str, str]] = []

    def query(self, query: str, org: str):
        self.calls.append({"query": query, "org": org})
        return self.tables


@pytest.mark.asyncio
async def test_query_readings_builds_flux_with_server_side_filters() -> None:
    measured_at = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    query_api = FakeQueryApi(
        [
            FakeTable(
                [
                    FakeRecord(
                        {
                            "_time": measured_at,
                            "tenant_id": "tnt_1",
                            "pond_id": "pond_1",
                            "device_id": "dev_1",
                            "temp_c": 25.1,
                            "ph": 7.2,
                            "do_mg_l": 6.4,
                            "seq": 42,
                        }
                    )
                ]
            )
        ]
    )
    repository = InfluxTelemetryRepository(
        query_api=query_api,
        org="limnopulse",
        bucket="limnopulse_raw",
    )

    readings = await repository.query_readings(
        tenant_id="tnt_1",
        pond_id="pond_1",
        start="-30m",
        stop=None,
        limit=100,
    )

    assert len(readings) == 1
    assert readings[0].tenant_id == "tnt_1"
    assert readings[0].pond_id == "pond_1"
    assert readings[0].device_id == "dev_1"
    assert readings[0].temp_c == 25.1
    assert readings[0].seq == 42
    assert query_api.calls[0]["org"] == "limnopulse"
    query = query_api.calls[0]["query"]
    assert 'from(bucket: "limnopulse_raw")' in query
    assert '|> range(start: -30m)' in query
    assert 'r["_measurement"] == "water_quality"' in query
    assert 'r["tenant_id"] == "tnt_1"' in query
    assert 'r["pond_id"] == "pond_1"' in query
    assert "|> limit(n: 100)" in query


@pytest.mark.asyncio
async def test_query_latest_metrics_uses_latest_and_pivot() -> None:
    measured_at = datetime(2026, 1, 1, 12, 5, tzinfo=UTC)
    query_api = FakeQueryApi(
        [
            FakeTable(
                [
                    FakeRecord(
                        {
                            "_time": measured_at,
                            "tenant_id": "tnt_1",
                            "pond_id": "pond_1",
                            "temp_c": 25.1,
                            "ph": 7.2,
                        }
                    )
                ]
            )
        ]
    )
    repository = InfluxTelemetryRepository(
        query_api=query_api,
        org="limnopulse",
        bucket="limnopulse_raw",
    )

    metrics = await repository.query_latest_metrics(tenant_id="tnt_1", pond_id="pond_1")

    assert metrics.tenant_id == "tnt_1"
    assert metrics.pond_id == "pond_1"
    assert metrics.measured_at == measured_at
    assert metrics.temp_c == 25.1
    query = query_api.calls[0]["query"]
    assert "|> last()" in query
    assert "|> pivot(" in query
    assert 'r["tenant_id"] == "tnt_1"' in query
    assert 'r["pond_id"] == "pond_1"' in query


@pytest.mark.asyncio
async def test_query_latest_metrics_returns_newest_record_when_influx_returns_multiple_rows() -> None:
    older_at = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    newer_at = datetime(2026, 1, 1, 12, 10, tzinfo=UTC)
    query_api = FakeQueryApi(
        [
            FakeTable(
                [
                    FakeRecord(
                        {
                            "_time": older_at,
                            "tenant_id": "tnt_1",
                            "pond_id": "pond_1",
                            "temp_c": 24.0,
                        }
                    ),
                    FakeRecord(
                        {
                            "_time": newer_at,
                            "tenant_id": "tnt_1",
                            "pond_id": "pond_1",
                            "temp_c": 25.5,
                        }
                    ),
                ]
            )
        ]
    )
    repository = InfluxTelemetryRepository(
        query_api=query_api,
        org="limnopulse",
        bucket="limnopulse_raw",
    )

    metrics = await repository.query_latest_metrics(tenant_id="tnt_1", pond_id="pond_1")

    assert metrics.measured_at == newer_at
    assert metrics.temp_c == 25.5
    query = query_api.calls[0]["query"]
    assert '|> sort(columns: ["_time"], desc: true)' in query
    assert "|> limit(n: 1)" in query
