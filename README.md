# Limnopulse

Phase 1 FastAPI foundation plus Phase 2A authorized telemetry read path for Limnopulse.

## Local Setup

```bash
cp .env.example .env
python -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
docker compose up -d redis dynamodb-local influxdb
# creates LimnopulseDomain and LimnopulseAudit locally
python scripts/dev/init_dynamodb.py
python scripts/dev/seed_local.py
python -m uvicorn limnopulse_api.main:app --reload --host 0.0.0.0 --port 8000
```

## Local Auth

With `APP_ENV=local` and `AUTH_MODE=dev`, use:

```text
X-Dev-User-Sub: local-user-001
X-Dev-User-Email: local@example.test
```

Dev headers authenticate identity only. Tenant access still requires an active membership in `LimnopulseDomain`.

## Telemetry Reads

Phase 2A exposes authorized FastAPI read endpoints backed by InfluxDB:

```text
GET /v1/tenants/{tenant_id}/ponds/{pond_id}/readings
GET /v1/tenants/{tenant_id}/ponds/{pond_id}/metrics/latest
```

The API validates user membership against DynamoDB, confirms the pond belongs to the requested tenant,
and queries InfluxDB only with `tenant_id`, `pond_id`, and a time range filter. MQTT, Telegraf,
broker ACLs, and ingestion are intentionally left for Phase 2B.

Local InfluxDB defaults:

```text
INFLUXDB_URL=http://localhost:8086
INFLUXDB_ORG=limnopulse
INFLUXDB_RAW_BUCKET=aquafarm_raw
INFLUXDB_TOKEN=limnopulse-local-token
```

## Tests

```bash
python -m pytest -q
```
