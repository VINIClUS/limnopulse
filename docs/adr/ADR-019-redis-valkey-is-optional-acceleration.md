# ADR-019 — Redis/Valkey is optional acceleration; DynamoDB/SQS and bounded workers preserve correctness.

**Status:** Accepted

## Context

Membership and JWKS caches already have durable fallbacks, but current Telegram rate limiting still depends on Redis. Calling Redis optional before every correctness path fails conservatively would be inaccurate.

## Decision

Redis/Valkey may accelerate cache, deduplication, and distributed rate coordination, but it is never authoritative for identity, tenant mapping, idempotency, deployment, quota, compatibility, notification policy, or delivery state.

## Consequences

The platform can operate correctly without a mandatory cache deployment. No-cache behavior may reduce throughput and must use conservative durable limits rather than fail open.

## V4 traceability

V4 §§3, 21.1, 24 Phase 7A, 27, and 31 identify current Redis coupling and require DynamoDB/SQS plus bounded workers to preserve correctness.

## Implementation gate

Phase 7A must pass no-Redis integration tests for quotas, anti-storm behavior, destination gates, and worker bounds before production documentation calls Redis optional.

Phase 7A must prove that raw Push tokens, phone numbers, Telegram chat IDs, and secrets are never used as Redis/Valkey keys or metric labels; cache-enabled and no-cache tests must preserve this privacy boundary. Anti-storm windows must be enforced independently per tenant and event family; one tenant or event family's burst must not consume or suppress another's allowance, and Redis/no-Redis tests must prove isolation.

Phase 7A must test Redis/Valkey becoming unavailable during active worker processing; workers must fall back to conservative durable limits without losing quota, anti-storm, destination, or worker-bound protections and must never fail open.

Any cache-enabled authorization path must treat membership, destination, and policy entries as bounded-TTL/versioned hints only; at `BeginAttempt`, a newer durable revision or revocation must win over a stale cache entry, and tests must prove stale cached authorization cannot dispatch a notification.

## Non-goals

This record does not ban Redis, promise equal no-cache performance, move durable queues into Redis, or allow fail-open limits during cache failure.
