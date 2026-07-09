from datetime import UTC, datetime

import pytest

from limnopulse_api.adapters.influxdb import InfluxTelemetryRepository


class FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


class RecordingClient:
    def __init__(self, response_text: str) -> None:
        self.response_text = response_text
        self.calls: list[dict[str, object]] = []

    async def post(self, url: str, **kwargs) -> FakeResponse:
        self.calls.append({"url": url, **kwargs})
        return FakeResponse(self.response_text)


@pytest.mark.asyncio
async def test_list_readings_queries_influx_with_tenant_pond_time_and_fields() -> None:
    csv_text = """#group,false,false,true,true,false,false,true,true,true,false,false
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,
#datatype,string,string,string,double,double
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,tenant_id,pond_id,device_id,temp_c,ph
,,0,2026-07-08T14:00:00Z,2026-07-08T15:00:00Z,2026-07-08T14:35:00Z,tnt_1,pond_1,dev_1,25.5,7.1
"""
    client = RecordingClient(csv_text)
    repository = InfluxTelemetryRepository(
        base_url="http://influxdb:8086",
        org="limnopulse",
        bucket="aquafarm_raw",
        token="local-token",
        timeout_seconds=5.0,
        client=client,  # type: ignore[arg-type]
    )

    readings = await repository.list_readings(
        tenant_id="tnt_1",
        pond_id="pond_1",
        start=datetime(2026, 7, 8, 14, 0, tzinfo=UTC),
        stop=datetime(2026, 7, 8, 15, 0, tzinfo=UTC),
        limit=50,
        fields=("temp_c", "ph"),
    )

    assert len(readings) == 1
    assert readings[0].timestamp == datetime(2026, 7, 8, 14, 35, tzinfo=UTC)
    assert readings[0].tenant_id == "tnt_1"
    assert readings[0].pond_id == "pond_1"
    assert readings[0].device_id == "dev_1"
    assert dict(readings[0].metrics) == {"temp_c": 25.5, "ph": 7.1}
    assert client.calls[0]["url"] == "/api/v2/query"
    assert client.calls[0]["params"] == {"org": "limnopulse"}
    assert client.calls[0]["headers"] == {
        "Accept": "application/csv",
        "Content-Type": "application/vnd.flux",
        "Authorization": "Token local-token",
    }
    assert client.calls[0]["timeout"] == 5.0
    query = client.calls[0]["content"]
    assert 'from(bucket: "aquafarm_raw")' in query
    assert 'r["tenant_id"] == "tnt_1"' in query
    assert 'r["pond_id"] == "pond_1"' in query
    assert 'range(start: time(v: "2026-07-08T14:00:00Z")' in query
    assert 'stop: time(v: "2026-07-08T15:00:00Z"))' in query
    assert 'set: ["temp_c", "ph"]' in query
    assert "limit(n: 50)" in query


@pytest.mark.asyncio
async def test_latest_metrics_returns_none_when_csv_has_no_rows() -> None:
    client = RecordingClient("#datatype,string,long\n,result,table\n")
    repository = InfluxTelemetryRepository(
        base_url="http://influxdb:8086",
        org="limnopulse",
        bucket="aquafarm_raw",
        token="",
        timeout_seconds=5.0,
        client=client,  # type: ignore[arg-type]
    )

    reading = await repository.latest_metrics(
        tenant_id="tnt_1",
        pond_id="pond_1",
        start=datetime(2026, 7, 8, 14, 0, tzinfo=UTC),
        stop=datetime(2026, 7, 8, 15, 0, tzinfo=UTC),
        fields=("temp_c",),
    )

    assert reading is None
    assert "Authorization" not in client.calls[0]["headers"]
    assert "limit(n: 1)" in client.calls[0]["content"]
