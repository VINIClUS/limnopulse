# Limnopulse

FastAPI and one-shot Go evaluation runtime for Limnopulse telemetry and alerts.

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

The API checks active tenant membership and verifies the pond in DynamoDB before querying InfluxDB. Clients never access InfluxDB directly.

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

Phase 3B evaluates rules but does not send notifications. It stores opening and recovery outboxes durably for the Phase 3C dispatcher.

## Alert Events and Evaluation

Phase 3B exposes durable incident reads and administrative transitions:

```text
GET  /v1/tenants/{tenant_id}/alert-events
GET  /v1/tenants/{tenant_id}/alert-events/{event_id}
POST /v1/tenants/{tenant_id}/alert-events/{event_id}/acknowledge
POST /v1/tenants/{tenant_id}/alert-events/{event_id}/resolve
```

All tenant roles may read. Members, admins and owners may acknowledge; only
admins and owners may manually resolve. Mutations require `expected_version`.

Run one local evaluation after initializing DynamoDB and telemetry:

```bash
docker compose --profile manual run --rm alert-evaluator run
```

The process exits after the owned work is complete. Scheduling remains external.
See [Phase 3B evaluator operations](docs/alert-evaluator-phase-3b.md) for replay,
sharding, schedule backfill, scheduler examples, exit codes and metrics.

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

The scaffold covers DynamoDB on-demand tables and alert indexes, Cognito User Pool/client, SQS with DLQ, and optional SES identity. `backend.example.hcl` is a placeholder for a future real remote-state setup; do not use it for local validation. Redis cloud, InfluxDB managed provisioning, production MQTT hardening, scheduled evaluator deployment, and notification delivery remain future slices. Do not commit real backend config, `.tfvars`, state files, plans, account ids, domains, or secrets.

## Tests

```bash
python -m pytest -q
go test -race ./...
```
