# Limnopulse Phase 2C Local Device Registry Enrichment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make local MQTT sample telemetry API-readable by adding development-only device registry enrichment before InfluxDB writes.

**Architecture:** Telegraf keeps parsing `device_id` from MQTT topics, then a Starlark processor maps known local devices to seeded `tenant_id` and `pond_id` tags. Unknown local devices are dropped, and FastAPI remains the only tenant-authorized read path.

**Tech Stack:** Telegraf `processors.starlark`, Docker Compose, DynamoDB Local seed script, pytest static configuration tests.

## Global Constraints

- Keep `tenant_id` and `pond_id` out of sensor payloads.
- Keep this enrichment local-only and development-scoped.
- Do not add production MQTT TLS/mTLS, broker ACLs, Go workers, SQS, DLQ, SES, Telegram, WhatsApp, or SMS.
- Do not add PostgreSQL, Firestore, or Firebase.
- Redis remains cache-only.
- Shell commands should be prefixed with `rtk`.

---

### Task 1: Registry Enrichment Tests

**Files:**
- Modify: `tests/unit/test_local_ingestion_config.py`

**Interfaces:**
- Produces static tests for the Telegraf Starlark processor, Compose mount, seed IDs, and sample contract.

- [x] Add tests that assert Telegraf config references `/etc/telegraf/device_registry.star`.
- [x] Add tests that assert Compose mounts `./infra/telegraf/device_registry.star` read-only.
- [x] Add tests that assert the registry maps `local-device-001` to `tnt_local_001` and `pond_local_001`.
- [x] Add tests that assert unknown devices are dropped by returning `None`.
- [x] Add tests that assert seed script creates `pond_local_001` and `local-device-001`.
- [x] Run the targeted test file and confirm failures point to missing enrichment artifacts.

### Task 2: Enrichment Implementation

**Files:**
- Create: `infra/telegraf/device_registry.star`
- Modify: `infra/telegraf/telegraf.conf`
- Modify: `compose.yaml`
- Modify: `scripts/dev/seed_local.py`

**Interfaces:**
- Produces local enrichment tags consumed by existing Phase 2A InfluxDB queries.

- [x] Add the Starlark `apply(metric)` function with a static local mapping.
- [x] Configure Telegraf `[[processors.starlark]]` with `script = "/etc/telegraf/device_registry.star"`.
- [x] Mount the registry script in Compose read-only.
- [x] Update local seed to create the matching pond and device records using repository methods.
- [x] Run the targeted test file until it passes.

### Task 3: Documentation And Verification

**Files:**
- Modify: `README.md`

**Interfaces:**
- Produces local run instructions showing how the sample can be queried through FastAPI.

- [x] Document that Telegraf enriches known local devices with tenant and pond tags.
- [x] Document that unknown devices are dropped in the local scaffold.
- [x] Document a sample authorized API query for `tnt_local_001` and `pond_local_001`.
- [x] Run the full pytest suite.
- [x] Run compileall.
- [x] Run Docker Compose config validation.
- [x] Run anti-requirement search over new execution artifacts.

## Self-Review

- The plan preserves the rule that devices do not assert tenant authorization.
- The local static map is explicit development scaffolding and not production identity.
- Unknown devices are not written as tenantless points.
