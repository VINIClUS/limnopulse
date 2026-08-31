# ADR-018 — AWS End User Messaging Push and SMS are the initial MVP delivery providers, not notification-domain authority.

**Status:** Accepted

## Context

The MVP needs mobile Push and critical SMS while retaining LimnoPulse-owned destinations, policy, delivery identity, budgets, and incident semantics. Platform and country readiness differ and must not leak into the canonical domain.

## Decision

Use AWS End User Messaging as the initial Push and SMS implementation behind the provider-neutral delivery port. Canonical platforms are `android` and `ios`; provider channel names and credentials remain adapter details.

## Consequences

One durable model supports platform rollout and provider replacement while isolated lanes bound failures. Commercial launch must wait for explicit platform, country, origination, privacy, and deliverability evidence.

## V4 traceability

V4 §§4, 17, 24 Phases 7B/7C, 27, and 31 require Android/FCM first, iOS/APNs second, and both before broad Brazil+United States launch. Production SMS is BR/US-only: Brazil requires shared/international-route and carrier readiness with no initial short code; the US requires registered toll-free readiness. Direct FCM/APNs adapters, Web Push, and alternative providers remain deferred.

## Implementation gate

Phase 7B independently proves Android then iOS registration, rotation, invalidation, environment separation, secure previews, and dual-platform launch readiness. Phase 7C applies the common SMS gates to both Brazil and the United States: verified and consented destinations, critical-only acknowledgement-aware escalation, exact `PlanVersion` count and USD budget/max-price enforcement, single-part GSM-7/UCS-2 preflight with multipart rejection, durable reservation and provider-call semantics, idempotent delayed/duplicate/out-of-order delivery-receipt reconciliation, spend/price/storm controls, and an independent SMS kill switch. Country readiness remains separate: every non-BR/US country is blocked, Brazil remains blocked until shared/international-route validation and carrier tests pass, and the United States remains blocked until registered toll-free approval plus STOP/HELP and opt-in/privacy/terms controls pass.

## Non-goals

This record does not make AWS EUM canonical, equate acceptance with receipt or acknowledgement, provision a Brazilian short code, add 10DLC, support marketing traffic, or implement direct FCM/APNs or Web Push now.
