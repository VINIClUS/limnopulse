# ADR-002 — Introduce Site and Asset while preserving Pond `/v1`.

**Status:** Accepted

## Context

The v1 Pond is simultaneously a customer resource, location, and telemetry scope. V4 needs broader Sites and Assets without breaking existing clients or rewriting historical Pond identity.

## Decision

Add Site and Asset/PondProfile in `/v2`. Preserve Pond request and response contracts through a deterministic compatibility projection onto the generalized model.

## Consequences

New domains can model non-pond assets while current integrations continue to work. Additive rows, migration receipts, and projection rules create dual-representation work that must be idempotent and observable.

## V4 traceability

V4 §§3, 9, 19, 24 Phase 1, 27, and 32 define Site/Asset introduction and the retained Pond compatibility boundary.

## Implementation gate

Phase 1 owns the additive model. The gate requires tenant isolation, idempotent default Site/Asset migration, no Scan, and unchanged v1 Pond behavior with rollback by disabling v2/projection paths.

## Non-goals

This record does not remove Pond `/v1`, rewrite historical IDs, or make a migration contingent on destructive replacement of legacy rows.
