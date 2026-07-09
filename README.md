# Limnopulse

FastAPI foundation for Limnopulse with Phase 2A authorized telemetry read endpoints.

## Local Setup

Docker Compose is the development runtime for local dependencies. Cloud infrastructure is managed separately with OpenTofu under `infra/opentofu`.

```bash
cp .env.example .env
python -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
docker compose up -d redis dynamodb-local influxdb mqtt-broker telegraf
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

Phase 2A exposes authorized read endpoints through FastAPI:

```text
GET /v1/tenants/{tenant_id}/ponds/{pond_id}/readings?start=-1h&limit=500
GET /v1/tenants/{tenant_id}/ponds/{pond_id}/metrics/latest
```

The API checks active tenant membership and verifies the pond in DynamoDB before querying InfluxDB. Clients never access InfluxDB directly. Go workers, alerts, and notification channels are not implemented yet.

## Local Telemetry Ingestion

Phase 2B adds a local MQTT-to-InfluxDB scaffold:

```bash
docker compose up -d influxdb mqtt-broker telegraf
python scripts/dev/seed_local.py
python scripts/dev/publish_sample_reading.py
```

The sample publisher sends `examples/telemetry/reading.local.json` to:

```text
limnopulse/v1/devices/local-device-001/readings
```

Telegraf subscribes to `limnopulse/v1/devices/+/readings` and writes `water_quality` metrics to `limnopulse_raw` with `device_id`, `source=mqtt`, and `schema_version=1` tags. A local-only Starlark registry enriches known development devices with tenant and pond tags before the InfluxDB write:

```text
local-device-001 -> tenant_id=tnt_local_001, pond_id=pond_local_001
```

Unknown local devices are dropped by Telegraf so tenantless points are not written to the API-read bucket. The sample sensor payload intentionally omits tenant and pond identifiers; production enrichment and authorization still belong outside the device payload.

After the API is running and the local seed has created `tnt_local_001`, `pond_local_001`, and `local-device-001`, query the sample through FastAPI:

```bash
curl -H "X-Dev-User-Sub: local-user-001" \
  -H "X-Dev-User-Email: local@example.test" \
  "http://127.0.0.1:8000/v1/tenants/tnt_local_001/ponds/pond_local_001/readings?start=2026-07-09T00:00:00Z&stop=2026-07-10T00:00:00Z&limit=10"
```

The local Mosquitto service uses anonymous access bound to `127.0.0.1:1883` for development only. Production MQTT still needs TLS/mTLS, per-device credentials, and broker ACL hardening before exposure.

## Cloud Infrastructure

OpenTofu is the cloud infrastructure path for Limnopulse:

```bash
cd infra/opentofu
tofu init -backend=false
tofu fmt -check
tofu validate
```

The scaffold covers DynamoDB on-demand tables, Cognito User Pool/client, SQS with DLQ, and optional SES identity. `backend.example.hcl` is a placeholder for a future real remote-state setup; do not use it for local validation. Redis cloud, InfluxDB managed provisioning, production MQTT hardening, Go workers, and notification channels remain future slices. Do not commit real backend config, `.tfvars`, state files, plans, account ids, domains, or secrets.

## Tests

```bash
python -m pytest -q
```
