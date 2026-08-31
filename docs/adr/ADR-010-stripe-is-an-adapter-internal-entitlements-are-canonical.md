# ADR-010 — Stripe is the billing adapter; internal PlanVersion/EntitlementSnapshot is canonical.

**Status:** Accepted

## Context

Provider subscription state is asynchronous, mutable, and unsuitable as a hot-path authorization source. Product limits and safety-relevant behavior need immutable, auditable internal meaning.

## Decision

Stripe supplies checkout, portal, subscription events, and reconciliation through an adapter. Immutable PlanVersion and durable EntitlementSnapshot records are canonical for application enforcement.

## Consequences

Hot paths remain available during Stripe outages and historical limits remain explainable. Webhook ordering, reconciliation, version migration, and stale-snapshot operations require explicit workers and controls.

## V4 traceability

V4 §§18, 24 Phase 4, and 27 define the adapter boundary, immutable plan catalog, internal trial, quotas, and audit-only rollback.

## Implementation gate

Phase 4 must prove verified and idempotent webhook convergence, test/live isolation, immutable PlanVersion values, downgrade preflight, explicit subscriber migration, and no synchronous Stripe dependency. Phase 4 must freeze and test this exact serialized launch SMS mapping; USD fields use integer minor units:

```json
{
  "Trial": {"provider_calls": 0, "budget_usd_minor": 0, "max_price_usd_minor": 0},
  "Starter": {"provider_calls": 0, "budget_usd_minor": 0, "max_price_usd_minor": 0},
  "Farm": {"provider_calls": 10, "budget_usd_minor": 50, "max_price_usd_minor": 5},
  "Pro": {"provider_calls": 50, "budget_usd_minor": 250, "max_price_usd_minor": 5},
  "Business": {"provider_calls": 250, "budget_usd_minor": 1250, "max_price_usd_minor": 5}
}
```

Phase 4 must prove a BRL Stripe subscription retains the USD-denominated SMS budget and that neither entitlement evaluation nor notification dispatch performs a synchronous FX call. Phase 4 webhook ingress must return `2xx` only after durable queue acceptance and must return `5xx` on transient enqueue failure so Stripe retries; signature-verified, idempotent processing must remain asynchronous. EntitlementSnapshot cache entries must be snapshot-versioned and short-lived; a stale active cache entry must never override a newer durable restricted or suspended state. Phase 4 tests must prove stale-cache and mid-request suspension block both SMS spend and command dispatch. It must also prove the published billing-degradation behavior: grace keeps ingestion and critical alerts enabled while limiting new resources and automatic policies; restricted preserves critical notifications and only bounded existing ingestion with read-only history; suspended stops new paid processing, disables commands, and emits explicit suspension warnings. No grace, restricted, or suspended path may report monitoring as active after ingestion or monitoring coverage has stopped.

## Non-goals

This record does not make Stripe authoritative for tenant identity, command safety, monitoring truth, or direct deletion of over-limit resources.
