# Limnopulse Phase 2C Local Device Registry Enrichment Design

**Date:** 2026-07-09
**Status:** Approved by continuation
**Scope:** Local-only MQTT ingestion enrichment for development

## Context

Phase 2A reads InfluxDB points through FastAPI after validating tenant membership and pond existence. Phase 2B writes local MQTT readings to InfluxDB with `device_id`, `source`, and `schema_version` tags, but intentionally keeps `tenant_id` and `pond_id` out of sensor payloads. That preserves the security model, but local sample points are not returned by the tenant/pond-filtered API.

## Goals

- Add a local-only device registry enrichment step in Telegraf.
- Map `local-device-001` to `tenant_id=tnt_local_001` and `pond_id=pond_local_001`.
- Drop unknown devices during local ingestion instead of writing tenantless points.
- Seed the matching local pond and device records in DynamoDB.
- Keep sensor payloads free of tenant and pond identifiers.
- Keep the design explicit that this is a development bridge, not production authorization.

## Non-Goals

- No production MQTT broker, TLS, mTLS, broker ACLs, or certificate issuance.
- No DynamoDB lookup from Telegraf.
- No Go ingestion worker.
- No SQS, DLQ, alert evaluator, SES, Telegram, WhatsApp, or SMS.
- No client access to InfluxDB.

## Architecture

Telegraf uses `processors.starlark` with a mounted script file at `/etc/telegraf/device_registry.star`. The script reads the `device_id` tag produced from the MQTT topic, sets `tenant_id` and `pond_id` for known local devices, and returns `None` for unknown devices so they are not written to InfluxDB.

The local seed script creates:

- Tenant: `tnt_local_001`
- Pond: `pond_local_001`
- Device: `local-device-001`

This makes the manual sample path API-readable after Docker services and seed data are running.

## Data Flow

```text
sample publisher
  -> mqtt broker topic limnopulse/v1/devices/local-device-001/readings
  -> telegraf mqtt_consumer extracts device_id
  -> telegraf processors.starlark maps device_id to tenant_id/pond_id
  -> influxdb output bucket limnopulse_raw
  -> FastAPI authorized telemetry read endpoints
```

## Security Posture

The enrichment map is static and local-only. It does not grant user access to tenants; FastAPI still requires active DynamoDB membership before returning any telemetry. Unknown devices are dropped locally to avoid creating unscoped telemetry.

## Acceptance Criteria

- Telegraf config includes `processors.starlark` using `/etc/telegraf/device_registry.star`.
- Compose mounts both Telegraf config and registry script read-only.
- Registry script maps only the seeded local device to seeded local tenant and pond IDs.
- Registry script drops unknown devices.
- Seed script creates the matching pond and device when the tenant already exists or is newly created.
- Sample payload still omits `tenant_id` and `pond_id`.
- Static tests validate the local registry contract without Docker.
- Existing FastAPI tests keep passing.
