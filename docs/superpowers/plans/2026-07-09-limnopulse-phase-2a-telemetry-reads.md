# Limnopulse Phase 2A Telemetry Reads Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add authorized FastAPI telemetry read endpoints backed by an InfluxDB adapter without implementing live MQTT ingestion.

**Architecture:** Keep the existing `api -> services -> repositories/adapters` layering. The telemetry service verifies pond ownership in DynamoDB before any InfluxDB query and passes server-controlled `tenant_id` and `pond_id` filters to the telemetry repository.

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, influxdb-client, pytest, Docker Compose, InfluxDB 2.x.

## Global Constraints

- New implementation artifacts use canonical Limnopulse naming.
- Relational database and mobile BaaS datastore alternatives remain out of the architecture.
- Redis remains cache-aside only and is not used for telemetry truth.
- Client applications never access InfluxDB directly.
- JWT identity still does not grant tenant access; active DynamoDB membership is required.
- Every telemetry query must include server-side `tenant_id`, `pond_id`, and time-range filters.
- Phase 2A must not implement MQTT Broker, Telegraf real ingestion, Go workers, SQS, SES, Telegram, WhatsApp, SMS, alerts, or real device credential rotation.
- Shell commands should be prefixed with `rtk`.

---

### Task 1: Telemetry Configuration And Models

**Files:**
- Modify: `pyproject.toml`
- Modify: `.env.example`
- Modify: `src/limnopulse_api/core/config.py`
- Create: `src/limnopulse_api/domain/telemetry.py`
- Test: `tests/unit/test_settings.py`

**Interfaces:**
- Produces: `TelemetryReading`
- Produces: `LatestMetrics`
- Produces settings fields `influxdb_url`, `influxdb_token`, `influxdb_org`, `influxdb_bucket_raw`, `telemetry_default_range`, `telemetry_max_limit`

- [ ] Write failing settings/model tests for InfluxDB defaults.
- [ ] Add `influxdb-client` dependency.
- [ ] Add Pydantic settings fields and telemetry domain models.
- [ ] Run `rtk powershell -NoProfile -Command ".\\.venv\\Scripts\\python.exe -m pytest tests/unit/test_settings.py -q"`.

### Task 2: Telemetry Repository And InfluxDB Adapter

**Files:**
- Create: `src/limnopulse_api/repositories/telemetry.py`
- Create: `src/limnopulse_api/adapters/influxdb.py`
- Test: `tests/unit/test_influxdb_adapter.py`

**Interfaces:**
- Produces: `TelemetryRepository` protocol.
- Produces: `InfluxTelemetryRepository.query_readings(...)`.
- Produces: `InfluxTelemetryRepository.query_latest_metrics(...)`.

- [ ] Write failing adapter tests that inspect generated Flux query strings through a fake query API.
- [ ] Implement Flux query construction with server-side `tenant_id`, `pond_id`, measurement, range, pivot, and limit filters.
- [ ] Convert Flux records into `TelemetryReading` and `LatestMetrics`.
- [ ] Run `rtk powershell -NoProfile -Command ".\\.venv\\Scripts\\python.exe -m pytest tests/unit/test_influxdb_adapter.py -q"`.

### Task 3: Telemetry Service And API Routes

**Files:**
- Create: `src/limnopulse_api/services/telemetry.py`
- Create: `src/limnopulse_api/api/v1/schemas/telemetry.py`
- Create: `src/limnopulse_api/api/v1/routers/telemetry.py`
- Modify: `src/limnopulse_api/api/dependencies.py`
- Modify: `src/limnopulse_api/api/router.py`
- Modify: `src/limnopulse_api/main.py`
- Test: `tests/api/test_telemetry.py`

**Interfaces:**
- Produces: `PondTelemetryService`.
- Produces endpoints under `/v1/tenants/{tenant_id}/ponds/{pond_id}`.

- [ ] Write failing API tests for `403`, `404`, readings success delegation, and latest metrics success delegation.
- [ ] Add dependency accessor for `telemetry_repository`.
- [ ] Wire an InfluxDB repository into app lifespan.
- [ ] Implement telemetry service and routers with read-role authorization.
- [ ] Run `rtk powershell -NoProfile -Command ".\\.venv\\Scripts\\python.exe -m pytest tests/api/test_telemetry.py -q"`.

### Task 4: Local Dev Docs And Verification

**Files:**
- Modify: `compose.yaml`
- Modify: `README.md`
- Modify: `docs/architecture.md` only if needed to clarify Phase 2A local execution without changing the target architecture.

**Interfaces:**
- Produces local InfluxDB service configuration and documented run commands.

- [ ] Add InfluxDB 2.x service to Compose with local-only credentials.
- [ ] Document local InfluxDB setup and clarify that MQTT/Telegraf are not implemented in Phase 2A.
- [ ] Run full tests: `rtk powershell -NoProfile -Command ".\\.venv\\Scripts\\python.exe -m pytest -q"`.
- [ ] Run compile: `rtk powershell -NoProfile -Command ".\\.venv\\Scripts\\python.exe -m compileall src scripts tests"`.
- [ ] Run the architecture anti-requirement guardrail search in execution artifacts.

## Self-Review

- Spec coverage: configuration, adapter, service, routers, local docs, tests, and guardrails are covered.
- Placeholder scan: no TBD/TODO placeholders are present.
- Scope check: MQTT, Telegraf, Go workers, alerts, queues, and notification channels remain outside this slice.
