# ADR-010 — Stripe is the billing adapter; internal PlanVersion/EntitlementSnapshot is canonical.

**Status:** Accepted

## Context

Provider subscription state is asynchronous, mutable, and unsuitable as a hot-path authorization source. Product limits and safety-relevant behavior need immutable, auditable internal meaning.

## Decision

Stripe supplies checkout, portal, subscription events, and reconciliation through an adapter. Immutable PlanVersion and durable EntitlementSnapshot records are canonical for application enforcement.

## Consequences

Hot paths remain available during Stripe outages and historical limits remain explainable. Webhook ordering, reconciliation, version migration, and stale-snapshot operations require explicit workers and controls.

## V4 traceability

V4 §§17, 24 Phase 4, and 27 define the adapter boundary, immutable plan catalog, internal trial, quotas, and audit-only rollback.

## Implementation gate

Phase 4 must prove verified and idempotent webhook convergence, test/live isolation, immutable PlanVersion values, downgrade preflight, explicit subscriber migration, and no synchronous Stripe dependency.

## Non-goals

This record does not make Stripe authoritative for tenant identity, command safety, monitoring truth, or direct deletion of over-limit resources.
