# Limnopulse Phase 2B Local Ingestion Design

**Date:** 2026-07-09
**Status:** Approved by continuation
**Scope:** Local MQTT-to-InfluxDB telemetry ingestion scaffold

## Context

Phase 2A added authorized FastAPI reads through InfluxDB. Phase 2B starts the ingestion side of the telemetry roadmap without implementing alerting or production device credential management.

## Goals

- Add a local MQTT broker service to Docker Compose.
- Add Telegraf local configuration using `inputs.mqtt_consumer` and `outputs.influxdb_v2`.
- Subscribe only to Limnopulse telemetry topics:
  - `limnopulse/v1/devices/+/readings`
  - `limnopulse/v1/devices/+/health`
- Parse the device id from the topic.
- Parse JSON payload fields into the `water_quality` measurement.
- Write to the existing local InfluxDB bucket `limnopulse_raw`.
- Add static Influx tags `source=mqtt` and `schema_version=1`.
- Provide a sample local payload and publisher script for manual testing.
- Add static tests for the Compose and Telegraf configuration.

## Non-Goals

- No production TLS/mTLS certificate issuance.
- No real device credential rotation.
- No alert evaluator, Go worker, SQS, SES, Telegram, WhatsApp, or SMS.
- No direct client access to InfluxDB.
- No tenant or pond identity in sensor payloads.

## Local Security Posture

This slice is a local development scaffold. It exposes MQTT on `1883` and keeps the broker local-only. Production hardening must add TLS/mTLS, per-device credentials, and broker ACLs before internet exposure.

## Data Flow

```text
sample publisher
  -> mqtt broker topic limnopulse/v1/devices/local-device-001/readings
  -> telegraf mqtt_consumer
  -> influxdb output bucket limnopulse_raw
  -> FastAPI Phase 2A read endpoints
```

## Payload Contract

The readings payload contains sensor values only:

```json
{
  "ts": "2026-07-09T12:00:00Z",
  "seq": 1,
  "temp_c": 25.1,
  "ph": 7.2,
  "do_mg_l": 6.4,
  "turbidity_ntu": 3.1,
  "salinity_ppt": 11.8,
  "battery_v": 3.82,
  "rssi": -67
}
```

The payload must not include `tenant_id` or `pond_id`. The current local scaffold tags `device_id`, `source`, and `schema_version`; server-side enrichment from DynamoDB lookup is a later production ingestion step. Until enrichment adds `tenant_id` and `pond_id`, sample points are not returned by the Phase 2A tenant/pond-filtered API endpoints.

## Acceptance Criteria

- Compose includes `mqtt-broker` and `telegraf` services wired to InfluxDB.
- Telegraf config uses `inputs.mqtt_consumer` and `outputs.influxdb_v2`.
- Telegraf subscribes only to `limnopulse/v1/devices/+/readings` and `limnopulse/v1/devices/+/health`.
- Telegraf writes to `limnopulse_raw` and emits `water_quality`.
- Telegraf emits `device_id`, `source=mqtt`, and `schema_version=1` tags.
- Sample readings payload excludes tenant and pond identifiers.
- Tests can validate the configuration without Docker.
- Existing FastAPI tests keep passing.
