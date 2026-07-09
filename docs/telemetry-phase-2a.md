# Phase 2A — Telemetry Read Path

## Scope

Phase 2A adds the authorized read path for telemetry without enabling real device ingestion. It introduces
local InfluxDB configuration, an isolated telemetry repository interface, an HTTP/Flux InfluxDB adapter,
and FastAPI endpoints that proxy telemetry to authenticated users.

## Endpoints

```text
GET /v1/tenants/{tenant_id}/ponds/{pond_id}/readings
GET /v1/tenants/{tenant_id}/ponds/{pond_id}/metrics/latest
```

Both endpoints require a valid principal and an active DynamoDB membership for the requested tenant.
Before querying InfluxDB, the API verifies that the requested pond exists under that tenant.

## Query constraints

InfluxDB is never exposed directly to clients. API queries always include:

```text
tenant_id
pond_id
time range
water_quality measurement
allowed water-quality fields only
```

The `readings` endpoint requires `start` and accepts optional `stop`, repeated `fields`, and `limit`.
The `metrics/latest` endpoint uses a bounded lookback window so even latest reads are time-filtered.

## Deferred to Phase 2B

Phase 2A intentionally does not add MQTT, Telegraf, broker TLS/mTLS, device ACLs, or ingestion.
Those remain the Phase 2B scope so ingestion and read authorization can be tested independently.
