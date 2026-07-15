# Limnopulse Phase 3B Alert Evaluator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans
> to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for
> tracking.

**Goal:** Build the one-shot Go alert evaluator, durable incident/outbox model,
tenant alert API and schedule backfill without crossing into notification
delivery.

**Architecture:** A deep Go runner owns discovery and orchestration behind a
small interface. A pure state machine decides transitions; DynamoDB, InfluxDB,
Redis and OTLP are adapters. Python continues to own public HTTP contracts and
atomic administrative rule changes.

**Tech Stack:** Go 1.26, AWS SDK Go v2, InfluxDB Go client v2, go-redis v9,
OpenTelemetry Go, FastAPI/Pydantic, DynamoDB, Redis and InfluxDB.

## Global constraints

- One run captures a stable evaluation time and terminates within 45 seconds.
- Runtime discovery and migrations use Query/Get/Update only, never Scan.
- DynamoDB is authoritative; Redis and OTLP failures cannot corrupt state.
- Event, transition and per-channel outboxes share one TransactWriteItems call.
- Phase 3B makes no SQS, SES or Telegram calls.

---

### Task 1: Shared schedule schema and infrastructure

- [ ] Add failing Python tests for canonical buckets, semantic revisions,
  sparse index attributes and device-only metrics.
- [ ] Implement the shared schedule contract in the domain and alert-rule
  repository.
- [ ] Add both DynamoDB GSIs to OpenTofu and local initialization.
- [ ] Run focused Python and infrastructure tests and commit.

### Task 2: Go domain and state machine

- [ ] Create the Go module and failing tests for slot calculation, bucket
  ownership, event/outbox identities and all incident transitions.
- [ ] Implement immutable domain values, pure decisions and configuration
  validation.
- [ ] Run Go unit tests and commit.

### Task 3: InfluxDB quality adapter

- [ ] Add failing tests for safe Flux literals, bounded windows, canonical
  buckets, pond aggregation, stale/no-data and empty latest slots.
- [ ] Implement the compact query adapter and result validation.
- [ ] Run focused and integration tests and commit.

### Task 4: DynamoDB coordination and one-shot runner

- [ ] Add failing tests for paginated GSI discovery, fair bucket ownership,
  conditional claims, fencing, coalescing and atomic decisions.
- [ ] Implement DynamoDB, optional Redis hints, deadline handling and exits.
- [ ] Add the `run` and `backfill-schedule` commands and JSON summaries.
- [ ] Run Go tests including the race detector and commit.

### Task 5: AlertEvent API and administrative transitions

- [ ] Add failing API/repository tests for list/detail, roles, acknowledgement,
  manual resolution and semantic rule changes.
- [ ] Implement event schemas, repository, service, router and atomic audit
  writes.
- [ ] Run the focused and full Python suites and commit.

### Task 6: Packaging, observability and handoff

- [ ] Add OTLP tests, Docker build, one-shot Compose profile and operational
  documentation.
- [ ] Add guardrails for Scan and Phase 3C integrations.
- [ ] Run all Go, Python, OpenTofu, Compose and smoke checks.
- [ ] Review the diff, commit, push and open the Phase 3B pull request.

