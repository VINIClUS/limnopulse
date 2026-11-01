# Limnopulse

FastAPI and one-shot Go evaluation runtime for Limnopulse telemetry and alerts.

## Local Setup

Docker Compose is the development runtime for local dependencies. Cloud infrastructure is managed separately with OpenTofu under `infra/opentofu`.

```bash
cp .env.example .env
python -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
docker compose up -d redis dynamodb-local influxdb mqtt-broker telegraf elasticmq
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

Phase 3B evaluates rules and transactionally stores opening and recovery
outboxes. Phase 3C-A relays eligible email outboxes to SQS and delivers them
through SES. Phase 3C-B adds verified Telegram binding and a separate Telegram
queue/worker without changing the email delivery contract.

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
Before switching consumers to the active-alert index, follow the tenant-by-tenant
rollout in [active-alert index operations](docs/alert-active-index-rollout.md).

## Email Notifications

Phase 3C-A adds a one-shot notification relay plus one continuous email/SES
feedback worker. For a deterministic local run, initialize DynamoDB, start the
fake worker, and invoke the relay:

```bash
docker compose up -d dynamodb-local elasticmq
python scripts/dev/init_dynamodb.py
docker compose --profile notifications up -d notification-worker
docker compose --profile notifications run --rm notification-relay
```

The local worker uses a fake successful sender; it never contacts SES. The relay
has no loop and is intended for an external 60-second scheduler. DynamoDB holds
the authoritative delivery and attempt state, while SQS remains an at-least-once
transport. Phase 3C-A adds no public Delivery or Attempt API.

See [Phase 3C-A notification operations](docs/notifications-phase-3c-a.md) for
cloud rollout order, relay backfill, SES prerequisites, replay, metrics, DLQ
recovery, rollback and PII limits.

## Telegram Notifications

Phase 3C-B keeps Telegram enrollment in FastAPI and delivery in its own Go
worker. The public `POST /webhooks/telegram` route authenticates Telegram with
`X-Telegram-Bot-Api-Secret-Token`; it does not use Cognito and does not enable a
preference. `/start <token>` only verifies the separate binding. Issuing the
one-time token and changing `telegram_enabled` remain tenant-scoped,
Cognito-authenticated operations with active membership checks.
Hosted FastAPI requires an environment-specific `TELEGRAM_BOT_USERNAME`; the
`limnopulse_local_bot` default is accepted only in local and test environments.

```text
GET    /v1/tenants/{tenant_id}/me/telegram-binding
POST   /v1/tenants/{tenant_id}/me/telegram-binding-token
DELETE /v1/tenants/{tenant_id}/me/telegram-binding
POST   /webhooks/telegram
```

For local delivery, start the dedicated queue consumer and WireMock Bot API:

```bash
docker compose --profile notifications up -d \
  redis dynamodb-local elasticmq telegram-bot-api-fake \
  telegram-notification-worker
docker compose --profile notifications run --rm notification-relay
```

DynamoDB owns binding, destination, preferences, deliveries, attempts and
idempotency. Redis is only a fail-closed Telegram rate limiter. Telegram jobs
contain no chat ID or rendered message, and Telegram jobs never enter the email
queue. See [Phase 3C-B Telegram operations](docs/notifications-phase-3c-b.md)
for secrets, backfill, rollout, rollback, retry semantics and DLQ handling.

## M1 v2 Domain API (disabled by default)

Two independent rollout flags, both `false` by default (see `.env.example`):

- `API_V2_ENABLED` mounts the M1 `/v2` domain API.
- `V1_POND_DEVICE_PROJECTION_ENABLED` makes `/v1` Pond/Device writes dual-write to v2 records and lazily provision them; `/v1` reads stay on the legacy `POND#`/`DEVICE#` rows. With it on, a `/v1` device create or patch without a `pond_id` returns `409`.

With `API_V2_ENABLED=true` only the M1 route groups are mounted:

```text
GET/POST  /v2/tenants/{tenant_id}/sites
GET/PATCH /v2/tenants/{tenant_id}/sites/{site_id}
GET/POST  /v2/tenants/{tenant_id}/assets
GET/PATCH /v2/tenants/{tenant_id}/assets/{asset_id}
GET/POST  /v2/tenants/{tenant_id}/devices
GET/PATCH /v2/tenants/{tenant_id}/devices/{device_id}
GET/POST  /v2/tenants/{tenant_id}/devices/{device_id}/components
GET/POST  /v2/tenants/{tenant_id}/devices/{device_id}/integrations
GET/PATCH /v2/tenants/{tenant_id}/components/{component_id}
GET/POST  /v2/tenants/{tenant_id}/components/{component_id}/capabilities
GET/POST  /v2/tenants/{tenant_id}/integration-accounts
GET/POST  /v2/tenants/{tenant_id}/deployments
POST      /v2/tenants/{tenant_id}/deployments/{deployment_id}/end
```

`API_V2_ENABLED` alone mounts no `/v2` telemetry or observation route; the three P2 telemetry routes are mounted only with `API_V2_TELEMETRY_ENABLED=true` as well (see [P2 Canonical Telemetry](#p2-canonical-telemetry-ships-dark)). There are no health, calibration, billing, AWS IoT, notification-destination or command routes. `API_V2_ENABLED` does not change any `/v1` path, dependency or response. Rollout follows the M1-08 runbook (to be added). A local `.env` with `API_V2_ENABLED=true` changes the default route count that the tenant-mapping conformance tests expect, so keep it `false` when running the test suite.

## P2 Canonical Telemetry (ships dark)

P2 adds canonical metric observations in the InfluxDB `observation_numeric` measurement (in the existing `INFLUXDB_BUCKET_RAW` bucket), an ack-safe normalizer pipeline and a `/v1` read projection. Every switch is off by default (see `.env.example`); with all of them off the API behaves exactly as before P2:

| Switch | Default | Effect |
|---|---|---|
| `TELEMETRY_V2_WRITE_ENABLED` | `false` | Write `observation_numeric` points (only the dev CLI writes in P2). |
| `TELEMETRY_LEGACY_DUAL_WRITE_ENABLED` | `false` | Also write eligible legacy `water_quality` points from the normalizer path. |
| `API_V2_TELEMETRY_ENABLED` | `false` | Mount `GET /v2/tenants/{tenant_id}/metric-definitions`, `.../observations` and `.../observations/latest`; requires `API_V2_ENABLED=true` (the API refuses to start otherwise). |
| `V1_TELEMETRY_READ_SOURCE` | `legacy` | `legacy` reads `/v1` telemetry from `water_quality`; `v2` swaps in the projection from `observation_numeric`. |
| `ALERT_EVALUATOR_TELEMETRY_SOURCE` | `legacy` | Go alert evaluator window reader (`legacy` or `v2`; also `--telemetry-source`). |

The two write switches are independent; the write path writes v2 first, then legacy, and any failure raises without reporting anything as written (D9).

**D16 — P2 ships dark.** No deployed service calls the pipeline in P2, and Telegraf keeps writing only `water_quality`. `V1_TELEMETRY_READ_SOURCE=v2` and `ALERT_EVALUATOR_TELEMETRY_SOURCE=v2` may be enabled only where a producer writes every legacy device to `observation_numeric` and the keyed divergence check reports zero missing or different `(pond, device, metric, _time, value)` tuples over max(longest rule window, legacy read window). In production the owner flips them only through the P3 runbook's [D16 owner gate](docs/p3-ingestion-rollout-runbook.md#d16-owner-gate): legacy devices are HTTPS only (#104).

The only P2 producer is the dev CLI `scripts/dev/ingest_reading_v2.py`. It builds the trusted source context (`legacy_mqtt`) from its arguments only; a tenant, pond or device in the payload is never authority. `--observed-at now` rewrites the payload `ts` so that a Deployment created at seed time covers it:

```bash
TELEMETRY_V2_WRITE_ENABLED=true TELEMETRY_LEGACY_DUAL_WRITE_ENABLED=true \
  python scripts/dev/ingest_reading_v2.py --tenant tnt_local_001 --device local-device-001 \
  --observed-at now
```

It uses the DynamoDB-backed M1 repositories and the configured InfluxDB, and prints one JSON object of counts (no secrets). With both write switches off it is a dry run: the reading is normalized and zero points are written. Exit codes: `0` nothing quarantined, `2` anything quarantined, `1` any error (nothing reported as written; ingest again).

**Rollback:** before cutover (while Telegraf still writes `water_quality`), set the switches back to their defaults (`false`, and `legacy` for the read sources) and recreate the services that read them (`docker compose up -d <service>`; `docker compose restart` keeps the old environment). No data deletion is needed; `water_quality` and the legacy reads are untouched while legacy dual write is off. After cutover, follow the post-cutover rollback in the runbook below: restore a `water_quality` producer before switching any read source back to `legacy`.

The rollout sequence, release gate, keyed divergence checks and known limitations are in the [P2 telemetry rollout runbook](docs/p2-telemetry-rollout-runbook.md).

## V4 P3 Generic HTTPS Ingestion (ships dark)

P3 adds a generic HTTPS ingress, `POST /ingest/v2/observations`, which authenticates a connector with a per-IntegrationAccount ingest credential, resolves the device through the trusted DeviceIntegration mapping and answers `202` only after a durable write to the telemetry SQS queue. A separate telemetry ingest worker drains that queue into `observation_numeric`. P3 ships dark: every switch defaults off (see `.env.example`), and with all of them off the API behaves exactly as before P3.

| Switch | Default | Effect |
|---|---|---|
| `INGEST_V2_ENABLED` | `false` | Mount `POST /ingest/v2/observations`, the three ingest-credential routes and the two integration-lifecycle status routes (owner/admin). Requires `API_V2_ENABLED=true`, `TELEMETRY_V2_WRITE_ENABLED=true` and a non-empty `SQS_TELEMETRY_INGRESS_QUEUE_URL`; the API refuses to start otherwise. |
| `SQS_TELEMETRY_INGRESS_QUEUE_URL` | empty | The telemetry ingress queue (`limnopulse-telemetry-ingress`); `SQS_ENDPOINT_URL` selects ElasticMQ locally. |

With `API_V2_ENABLED=true` alone the tenant routes stay the 53 M1 routes; `INGEST_V2_ENABLED=true` adds exactly 5 tenant routes plus the non-tenant ingest route. The ingest route has no Cognito or dev user authentication: the ingest credential is its only authority.

**Worker.** The `telemetry-ingest-worker` compose service (profile `ingestion`) runs the API image with the entrypoint `python -m limnopulse_api.workers.telemetry_ingest`. It has its own composition (no optional cache client) and exits 78 (a configuration error) unless `TELEMETRY_V2_WRITE_ENABLED=true` and `SQS_TELEMETRY_INGRESS_QUEUE_URL` is set, so it can never delete a message it did not write. SIGTERM drains in-flight messages; it exits 0 only after a stop signal and 1 on a crash or a stalled loop (ADR-020 exit codes). In production it runs as the dedicated `telemetry_ingest` IAM user: `TELEMETRY_INGEST_AWS_ACCESS_KEY_ID` / `TELEMETRY_INGEST_AWS_SECRET_ACCESS_KEY` and `TELEMETRY_INGEST_INFLUXDB_TOKEN` are interpolated into its own environment, never the API keys.

Local rehearsal (the no-Docker runners `scripts/dev/run_local_elasticmq.sh`, `run_local_dynamodb.sh` and `run_local_influxd.sh` work too):

```bash
docker compose up -d elasticmq dynamodb-local influxdb
python scripts/dev/init_dynamodb.py && python scripts/dev/seed_local.py
python -m scripts.migrations.m1_backfill_default_site_assets --tenant tnt_local_001 --apply
python scripts/dev/seed_ingest_local.py   # prints an ingest token once
API_V2_ENABLED=true TELEMETRY_V2_WRITE_ENABLED=true INGEST_V2_ENABLED=true \
  SQS_TELEMETRY_INGRESS_QUEUE_URL=http://localhost:9324/000000000000/limnopulse-telemetry-ingress \
  uvicorn limnopulse_api.main:app --reload
docker compose --profile ingestion up telemetry-ingest-worker
```

`scripts/dev/seed_ingest_local.py` creates an active `https_direct` IntegrationAccount and an active DeviceIntegration for `local-device-001`, issues an ingest credential and prints its token to stdout once; nothing is stored in the repository. Keep `INGEST_V2_ENABLED=false` in a local `.env` when running the test suite.

Since ADR-020 the production worker restarts `unless-stopped`, including after a Docker daemon restart or host reboot, and every deploy passes `--profile ingestion`, so the API (producer) and the worker (consumer) always run the same image. The loop marks a liveness stamp every turn; a watchdog exits 1 when the loop stalls (111 s without a mark at the defaults), and the compose healthcheck runs `python -m limnopulse_api.workers.telemetry_ingest healthcheck` against the stamp. On a configuration error (exit 78) a shell guard in the entrypoint parks the container instead of exiting, so Docker never restarts it on that code and its healthcheck reports it unhealthy, which fails the deploy health gate. The queue-age alarm still covers a worker that is not consuming; the [runbook](docs/p3-ingestion-rollout-runbook.md#production-environment) has the details.

**Rollback:** set `INGEST_V2_ENABLED=false` and recreate the API (`docker compose --env-file .env --env-file .env.image -f compose.production.yaml up -d api`; `docker compose restart` keeps the old environment), then stop the `ingestion` profile, in the [rollback order](docs/p3-ingestion-rollout-runbook.md#rollback) of the runbook. Queued messages stay in the queue and the DLQ until the worker runs again. The rollout sequence, owner flips and drills are in the [P3 ingestion rollout runbook](docs/p3-ingestion-rollout-runbook.md).

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

The local Mosquitto service uses anonymous access and is published only on host loopback at `127.0.0.1:1883` for development. It remains reachable by the local Compose services. Production MQTT still needs TLS/mTLS, per-device credentials, and broker ACL hardening before exposure.

## Cloud Infrastructure

OpenTofu is the cloud infrastructure path for Limnopulse:

```bash
cd infra/opentofu
tofu init -backend=false
tofu fmt -check
tofu validate
```

The scaffold covers DynamoDB on-demand tables and alert/notification indexes,
Cognito User Pool/client, encrypted email, Telegram and SES-feedback queues with
their DLQs, an SESv2 configuration set, EventBridge feedback routing, Telegram
secret containers and least-privilege runtime policies.
`backend.example.hcl` is a placeholder for a future real remote-state setup; do
not use it for local validation. SES identities and production sending access
are account operations outside this generic stack. Redis cloud, InfluxDB managed
provisioning, production MQTT hardening and scheduler deployment remain
environment-specific slices. Do not commit real backend config, `.tfvars`, state
files, plans, account IDs, domains or secrets.

## Tests

```bash
python -m pytest -q
go test -race ./...
```

The multiprocess notification suite is opt-in because it requires healthy local
DynamoDB, ElasticMQ, Redis and WireMock services:

```bash
RUN_NOTIFICATION_INTEGRATION=1 \
  python -m pytest -q tests/integration/test_notifications_local.py
```

## Frontend React

As sete telas do LimnoPulse ficam em [`frontend/`](frontend/README.md), com Vite na porta 3000 e proxy `/v1` para a API. Consulte o README do frontend para Cognito, modo local, onboarding, leads/CSV, métricas agregadas, testes e publicação na mesma origem. Relatório visual em [`artifacts/visual/index.html`](artifacts/visual/index.html). Nenhum deploy ou cobrança faz parte desta entrega.
