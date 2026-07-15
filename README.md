# Limnopulse

FastAPI foundation for Limnopulse with authorized telemetry reads and Phase 3A Alert Rule configuration.

## Local Setup

Docker Compose is the development runtime for local dependencies. Cloud infrastructure is managed separately with OpenTofu under `infra/opentofu`.

```bash
cp .env.example .env
python -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
docker compose up -d redis dynamodb-local influxdb mqtt-broker telegraf
# creates LimnopulseDomain and LimnopulseAudit and enables expires_at TTL
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

The API checks active tenant membership and verifies the pond in DynamoDB before querying InfluxDB. Clients never access InfluxDB directly. Go alert evaluation and notification delivery are not implemented yet.

## Alert Rules

Phase 3A exposes tenant-scoped rule configuration:

```text
GET   /v1/tenants/{tenant_id}/alert-rules
POST  /v1/tenants/{tenant_id}/alert-rules
PATCH /v1/tenants/{tenant_id}/alert-rules/{rule_id}
POST  /v1/tenants/{tenant_id}/alert-rules/{rule_id}/replace
```

All active tenant roles may list rules. Only owner and admin roles may create, patch, or replace them. Creation validates that the pond exists and that an optional device belongs to the same tenant and pond.

Example creation body:

```json
{
  "pond_id": "pond_001",
  "device_id": "dev_001",
  "metric": "do_mg_l",
  "name": "Low dissolved oxygen",
  "operator": "<",
  "threshold": 5.0,
  "aggregation": "min",
  "window": "5m",
  "duration": "3m",
  "severity": "critical",
  "channels": ["email", "telegram"],
  "cooldown_seconds": 1800,
  "enabled": true
}
```

PATCH accepts `expected_version` plus at least one mutable field. Tenant, pond, optional device, and metric form the semantic identity and cannot be patched. To change identity, call `/replace` with a complete replacement body, `expected_version`, and an `Idempotency-Key` header between 8 and 128 characters. The same key and payload replays the result for 24 hours; reusing the key with another payload returns `409`.

This phase stores channel declarations but does not evaluate telemetry or send notifications. See `docs/architecture.md` for the Phase 3B evaluator and Phase 3C dispatcher boundaries.

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
