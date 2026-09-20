"""Real local API + DynamoDB + Redis + InfluxDB. No HTTP stubs.
Run only against the disposable local stack documented in frontend/README.md.
"""

import os
from datetime import UTC, datetime, timedelta
from io import StringIO
from uuid import uuid4

import boto3
import httpx
import pytest
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from limnopulse_api.adapters.leads import DynamoLeadRepository
from scripts.admin.export_leads import export_csv

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_FRONTEND_LOCAL_TESTS") != "1",
    reason="requires local API/DynamoDB/Redis/InfluxDB",
)


def test_real_summary_uses_all_raw_samples_and_enforces_tenant_access():
    headers = {"X-Dev-User-Sub": f"integration-{uuid4()}"}
    with httpx.Client(base_url="http://127.0.0.1:8000", headers=headers, timeout=30) as api:
        tenant = api.post(
            "/v1/tenants", json={"name": "Telemetry integration", "city": "Panorama - SP"}
        ).json()
        tid = tenant["tenant_id"]
        pond = api.post(f"/v1/tenants/{tid}/ponds", json={"name": "Summary test"}).json()
        pid = pond["pond_id"]
        path = f"/v1/tenants/{tid}/ponds/{pid}/metrics/summary"
        empty = api.get(path)
        assert empty.status_code == 200
        assert empty.json()["series"] == []
        now = datetime.now(UTC) - timedelta(seconds=10)
        points = [
            Point("water_quality")
            .tag("tenant_id", tid)
            .tag("pond_id", pid)
            .tag("device_id", f"dev_{i % 2}")
            .field("do_mg_l", 4.0 if i < 600 else 8.0)
            .field("ph", 7.3)
            .field("temp_c", 28.1)
            .time(now - timedelta(seconds=i))
            for i in range(1800)
        ]
        with InfluxDBClient(
            url="http://127.0.0.1:8086", token="local-dev-token", org="limnopulse"
        ) as influx, influx.write_api(write_options=SYNCHRONOUS) as writer:
            writer.write(bucket="limnopulse_raw", record=points)
        for period, interval in [("24h", "5m"), ("7d", "1h"), ("30d", "1h")]:
            response = api.get(path, params={"period": period})
            assert response.status_code == 200, response.text
            result = response.json()
            assert result["interval"] == interval
            stats = result["statistics"]["do_mg_l"]
            assert stats["count"] == 1800
            assert stats["mean"] == pytest.approx(20 / 3)
            assert stats["min"] == 4.0 and stats["max"] == 8.0
            assert 0 < len(result["series"]) < 1800
        assert api.get(path, headers={"X-Dev-User-Sub": "other-user"}).status_code == 403
        assert api.get(path.replace(pid, "missing")).status_code == 404


def test_real_lead_survives_repository_recreation_and_exports():
    marker = f"local-{uuid4()}@example.com"
    start = datetime.now(UTC) - timedelta(seconds=1)
    response = httpx.post(
        "http://127.0.0.1:8000/v1/leads",
        json={
            "name": "Local Persistence",
            "email": marker,
            "source": "integration",
            "consent": True,
        },
        timeout=10,
    )
    assert response.status_code == 201, response.text
    db = boto3.client(
        "dynamodb",
        region_name="us-east-1",
        endpoint_url="http://127.0.0.1:8001",
        aws_access_key_id="local",
        aws_secret_access_key="local",
    )
    repository = DynamoLeadRepository("LimnopulseDomain", db)
    output = StringIO()
    export_csv(repository, start, datetime.now(UTC) + timedelta(seconds=1), output)
    assert marker in output.getvalue()
    assert response.json()["lead_id"] in output.getvalue()
