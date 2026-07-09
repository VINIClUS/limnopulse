# Limnopulse Phase 2B Local Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a local MQTT-to-InfluxDB ingestion scaffold for development and testing.

**Architecture:** Use Docker Compose for Mosquitto, Telegraf, and InfluxDB. Telegraf consumes JSON MQTT payloads, parses the device id from the topic, and writes water-quality fields to the local InfluxDB raw bucket.

**Tech Stack:** Docker Compose, Eclipse Mosquitto, Telegraf, InfluxDB 2.x, Python pytest static config tests.

## Global Constraints

- Use Limnopulse naming in new artifacts.
- Keep Redis cache-only.
- Do not add alerting, Go workers, SQS, SES, Telegram, WhatsApp, or SMS.
- Do not add production TLS/mTLS secrets or real device credentials.
- Do not put tenant_id or pond_id in sensor payloads.
- Shell commands should be prefixed with `rtk`.

---

### Task 1: Static Config Tests

**Files:**
- Create: `tests/unit/test_local_ingestion_config.py`

**Interfaces:**
- Produces static checks for Compose, Telegraf config, Mosquitto config, sample payload, and publisher script.

- [ ] Write failing static tests for services, topics, bucket, measurement, and payload contract.
- [ ] Run the new test and confirm it fails because files/services are missing.

### Task 2: Local Ingestion Config

**Files:**
- Modify: `compose.yaml`
- Create: `infra/mqtt/mosquitto.conf`
- Create: `infra/telegraf/telegraf.conf`
- Create: `examples/telemetry/reading.local.json`
- Create: `scripts/dev/publish_sample_reading.py`

**Interfaces:**
- Produces local MQTT broker and Telegraf ingestion path.

- [ ] Add Mosquitto and Telegraf services to Compose.
- [ ] Add local-only Mosquitto config.
- [ ] Add Telegraf MQTT consumer and InfluxDB v2 output config.
- [ ] Add sample JSON payload without tenant or pond identifiers.
- [ ] Add publisher script using Python stdlib sockets only.
- [ ] Run static tests until they pass.

### Task 3: Docs And Verification

**Files:**
- Modify: `README.md`

**Interfaces:**
- Produces local run instructions for ingestion scaffold.

- [ ] Document local startup and sample publishing.
- [ ] Run `rtk powershell -NoProfile -Command ".\\.venv\\Scripts\\python.exe -m pytest -q"`.
- [ ] Run `rtk powershell -NoProfile -Command ".\\.venv\\Scripts\\python.exe -m compileall src scripts tests"`.
- [ ] Run anti-requirement guardrail search over execution artifacts.

## Self-Review

- Scope remains local ingestion only.
- The plan does not add production credentials or alerting systems.
- Tests prove the scaffold shape without requiring Docker.
