# Limnopulse Phase 2A Telemetry Reads Design

**Date:** 2026-07-09
**Status:** Approved by continuation
**Scope:** Authorized telemetry read endpoints through FastAPI

## Context

Phase 1 delivered the FastAPI foundation with Cognito/dev auth, DynamoDB tenant membership authorization, tenant/pond/device CRUD, Redis cache-aside, and local development scripts. The broader architecture defines Phase 2 as telemetry: MQTT Broker TLS, Telegraf MQTT consumer, InfluxDB buckets/schema, and authorized readings endpoints.

This slice implements only the API-side read foundation and local InfluxDB configuration. It does not implement live MQTT ingestion.

## Goals

- Add InfluxDB configuration to the application settings.
- Add a telemetry repository interface and InfluxDB adapter for Flux queries.
- Add domain models for telemetry readings and latest metrics.
- Add FastAPI endpoints:
  - `GET /v1/tenants/{tenant_id}/ponds/{pond_id}/readings`
  - `GET /v1/tenants/{tenant_id}/ponds/{pond_id}/metrics/latest`
- Preserve tenant authorization through active DynamoDB membership.
- Verify that the requested pond exists in the authorized tenant before querying InfluxDB.
- Keep all InfluxDB filters server-controlled: `tenant_id`, `pond_id`, and time range.
- Add tests for authz, pond existence checks, query delegation, and Flux query construction.
- Extend local development docs and Compose with InfluxDB.

## Non-Goals

- No MQTT Broker implementation.
- No Telegraf configuration or real ingestion pipeline.
- No Go workers or Lambdas.
- No alert evaluator, alert rules, SQS, SES, Telegram, WhatsApp, or SMS.
- No direct client access to InfluxDB.
- No relational database or mobile BaaS datastore alternatives.

## Architecture

The existing FastAPI layering remains:

```text
api -> services -> repositories/adapters
```

Telemetry reads use:

```text
router -> PondTelemetryService -> DomainRepository + TelemetryRepository
```

The service first checks `DomainRepository.get_pond(tenant_id, pond_id)`. If the pond is absent, it returns `404` and never calls InfluxDB. If present, it calls the telemetry repository with the server-side `tenant_id` and `pond_id` from the URL, not from user-provided query/body input.

## API Design

### Readings

```text
GET /v1/tenants/{tenant_id}/ponds/{pond_id}/readings?start=-1h&stop=<optional>&limit=500
```

Rules:

- Requires active tenant membership with any read role.
- `start` accepts a relative Flux duration such as `-1h` or an absolute RFC3339 timestamp.
- `stop` is optional and may be an RFC3339 timestamp.
- `limit` is bounded from `1` to `1000`.
- Returns readings sorted by timestamp as returned by InfluxDB after the adapter query.

### Latest Metrics

```text
GET /v1/tenants/{tenant_id}/ponds/{pond_id}/metrics/latest
```

Rules:

- Requires active tenant membership with any read role.
- Verifies pond ownership before querying telemetry.
- Returns the latest known values for water-quality fields.

## InfluxDB Query Design

Measurement:

```text
water_quality
```

Required tags:

```text
tenant_id
pond_id
device_id
source=mqtt
schema_version
```

Readings use Flux filters for:

```text
_measurement == "water_quality"
tenant_id == <tenant_id>
pond_id == <pond_id>
```

The adapter pivots field rows into one record per timestamp/device where practical. The API model keeps telemetry fields optional because not every sensor sends every field.

## Configuration

Add `.env.example` variables:

```dotenv
INFLUXDB_URL=http://localhost:8086
INFLUXDB_TOKEN=local-dev-token
INFLUXDB_ORG=limnopulse
INFLUXDB_BUCKET_RAW=limnopulse_raw
TELEMETRY_DEFAULT_RANGE=-1h
TELEMETRY_MAX_LIMIT=1000
```

The token in `.env.example` is a local placeholder only. Real tokens must come from environment or secret storage.

## Testing

Required tests:

- Missing membership still returns `403`.
- Existing membership plus missing pond returns `404` and does not call telemetry.
- Existing pond delegates readings query with exact `tenant_id`, `pond_id`, `start`, `stop`, and `limit`.
- Existing pond delegates latest metrics query with exact `tenant_id` and `pond_id`.
- InfluxDB adapter builds Flux queries containing server-side tenant and pond filters.
- Settings expose InfluxDB defaults.
- Guardrail search remains free of PostgreSQL/Firebase/Firestore.

## Acceptance Criteria

- Phase 1 tests continue passing.
- New telemetry tests pass.
- FastAPI exposes both Phase 2A read endpoints.
- InfluxDB is only accessed through an adapter/repository boundary.
- Client requests cannot supply or override `tenant_id` or `pond_id` for InfluxDB filters.
- README explains how to start local InfluxDB as part of local development.
- Compose includes InfluxDB only; no MQTT/Telegraf/alerts are added in this slice.
