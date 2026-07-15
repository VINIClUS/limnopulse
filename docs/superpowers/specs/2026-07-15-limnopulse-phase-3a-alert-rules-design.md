# Limnopulse Phase 3A Alert Rules Design

**Date:** 2026-07-15
**Status:** Approved for implementation

## Scope

Phase 3A adds tenant-scoped alert-rule configuration to the FastAPI/DynamoDB domain. It does not evaluate telemetry, create alert events, apply Redis cooldowns, dispatch notifications, or integrate notification providers. Those responsibilities remain in Phase 3B and Phase 3C.

## API contract

The API exposes:

- `GET /v1/tenants/{tenant_id}/alert-rules` for owner, admin, member, and viewer roles.
- `POST /v1/tenants/{tenant_id}/alert-rules` for owner and admin roles, returning `201`.
- `PATCH /v1/tenants/{tenant_id}/alert-rules/{rule_id}` for owner and admin roles.
- `POST /v1/tenants/{tenant_id}/alert-rules/{rule_id}/replace` for owner and admin roles, returning `201` and requiring an `Idempotency-Key` header of 8 to 128 characters.

There is no individual GET, DELETE, AlertEvent endpoint, or channel integration in Phase 3A.

All request models reject unknown fields. PATCH requires `expected_version` and at least one mutable field. Identity fields supplied to PATCH are rejected with `422`.

## AlertRule model

Semantic identity is immutable after creation:

- `tenant_id`
- `pond_id`
- optional `device_id`
- `metric`

Mutable fields are:

- `name`
- `operator`
- `threshold`
- `aggregation`
- `window`
- `duration`
- `severity`
- `channels`
- `cooldown_seconds`
- `enabled`

Rules use server-generated IDs in the form `rule_<uuid hex>`. The supported values are:

- metrics: `temp_c`, `ph`, `do_mg_l`, `turbidity_ntu`, `salinity_ppt`, `battery_v`, `rssi`
- operators: `<`, `<=`, `>`, `>=`
- aggregations: `min`, `max`, `mean`, `last`
- severities: `warning`, `critical`
- channels: `email`, `telegram`

`window` and `duration` are compact duration strings such as `60s`, `5m`, or `24h`, each bounded to 60 seconds through 24 hours. `cooldown_seconds` is bounded to 60 through 86,400. At least one unique notification channel is required.

Every target pond must exist in the tenant. When `device_id` is present, that device must exist in the same tenant and pond. Missing and mismatched targets return `404` without revealing cross-tenant resource existence.

## Update and replacement semantics

PATCH uses optimistic concurrency. A matching `expected_version` increments `version`; a stale version returns `409`.

Changing semantic identity requires replacement. Replacement atomically:

1. disables the old rule;
2. sets its status to `replaced`;
3. increments its version;
4. sets `replaced_by_rule_id`;
5. creates a version-1 replacement with `replaces_rule_id`;
6. writes an audit record;
7. persists the idempotency result.

The response is `{ "replaced": <old rule>, "replacement": <new rule> }`.

The same idempotency key and request payload replays the stored result for 24 hours. Reusing the same key with a different request payload returns `409`. Expired records may remain physically present because DynamoDB TTL deletion is eventual; application conditions therefore treat `expires_at <= now` as reusable.

## Persistence

Alert rules live in `LimnopulseDomain`:

```text
PK = TENANT#<tenant_id>
SK = ALERT_RULE#<rule_id>
```

List operations use DynamoDB `Query` with the `ALERT_RULE#` sort-key prefix and never use `Scan`.

Replacement idempotency records also live in `LimnopulseDomain` under a SHA-256-derived sort key. They contain only the request hash, response snapshots, timestamps, and numeric `expires_at`; raw idempotency keys and authentication material are not persisted.

Audit records live in `LimnopulseAudit`:

```text
PK = TENANT#<tenant_id>#MONTH#YYYY-MM
SK = <timestamp>#<event_id>
```

Each mutation transaction spans the domain and audit tables. Audit records contain actor/action/resource metadata, SHA-256 hashes of before and after state, request IP, user agent, creation time, and numeric `expires_at`. They do not contain JWTs, credentials, or request payloads. Audit TTL is 90 days.

TTL on `expires_at` is enabled for both DynamoDB tables in OpenTofu and local initialization.

## Module boundaries

Alert-rule persistence is exposed through a new `AlertRuleRepository` protocol and implemented by `DynamoAlertRuleRepository`. `DomainRepository` remains focused on tenants, memberships, ponds, and devices. `AlertRuleService` owns target validation and ID generation; the adapter owns atomic storage, version conditions, audit serialization, and idempotent replay.

## Phase handoff

- Phase 3B: Go evaluator, InfluxDB windows, Redis cooldown/deduplication, and AlertEvent creation.
- Phase 3C: SQS dispatcher, SES, Telegram, retries, and delivery records.
