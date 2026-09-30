# Phase 3C-B Telegram Implementation Plan

**Goal:** add verified Telegram binding and isolated Telegram delivery while
preserving all Phase 3C-A email contracts.

## 1. Durable channel-neutral foundation

- [x] Characterize email identities, queues, retries, feedback and templates.
- [x] Add channel-specific relay schema validation without changing email v1.
- [x] Add the Telegram immutable snapshot variant while keeping jobs compact.
- [x] Route email and Telegram publishers through an explicit channel router.

## 2. Binding, webhook and preferences

- [x] Add `TelegramBinding`, request, destination and effective-state models.
- [x] Add transactional/idempotent DynamoDB issue, consume, stop and revoke
  repository operations.
- [x] Add extractable `TelegramBindingService` using the existing membership
  service rather than a second authorization implementation.
- [x] Add authenticated binding endpoints and optimistic preference support.
- [x] Add `POST /webhooks/telegram`, authenticate the secret before bounded body
  parsing, validate private `/start` and `/stop`, and avoid sends/preferences.
- [x] Add current/previous Secrets Manager verification with a bounded cache.

## 3. Relay, rendering and migration

- [x] Add deterministic plain-text opening/recovery rendering and content hash.
- [x] Resolve binding/destination with known-key consistent reads and snapshot
  them only on the durable Delivery.
- [x] Preserve opening destination for recovery and fence current lifecycle.
- [x] Add explicit-tenant `notifications backfill-telegram` Query pagination,
  dry-run/apply, conflict reporting and no-Scan regression coverage.

## 4. Dedicated worker and infrastructure

- [x] Add Bot API sender and typed Telegram error taxonomy.
- [x] Add global/per-destination Redis Lua rate limiting with Redis `TIME`.
- [x] Add consistent gates and transactional BeginAttempt fences.
- [x] Persist permanent destination suppression in the completion transaction.
- [x] Make ambiguous Telegram outcomes terminal `unknown` without auto-resend.
- [x] Add dedicated SQS/DLQ, secret containers, IAM, Compose worker and WireMock.

## 5. Regression and release verification

- [x] Add unit/API regression tests for all auth, idempotency, race, template,
  queue, retry and migration boundaries.
- [x] Add multiprocess Regression coverage using DynamoDB Local, ElasticMQ,
  Redis and WireMock.
- [ ] Run full Python, Go race/vet, Ruff, Compose and OpenTofu gates.
- [ ] Complete Standards and Spec reviews, resolve findings, and record final
  evidence before integration.

