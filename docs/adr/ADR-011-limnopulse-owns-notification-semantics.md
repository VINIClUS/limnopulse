# ADR-011 — LimnoPulse owns notification semantics, destinations and durable delivery state; providers are replaceable.

**Status:** Accepted

## Context

The existing SES and Telegram paths already depend on durable membership gates, immutable delivery identity, attempts, feedback, and incident relationships. Provider-owned orchestration would fragment those semantics and weaken tenant, privacy, acknowledgement, and rollback controls.

## Decision

LimnoPulse owns notification policy revisions, typed destinations, localized content revisions, escalation, budgets, Delivery/Attempt state, and provider-result interpretation. Providers implement a narrow delivery port and never become the notification ledger or policy authority.

## Consequences

Email, Telegram, Push, and SMS share durable semantics while retaining isolated lanes and provider-specific feedback. LimnoPulse must operate the policy and ledger machinery and preserve existing SES/Telegram identities during additive migration.

## V4 traceability

V4 §§17, 24 Phase 7A, 27, and 31 require versioned `pt-BR`/`en-US` content; generic lock-screen previews by default; bounded, revisioned, audited owner/admin asset-context opt-in; one canonical Android/iOS destination and Delivery model; and SMS country/readiness plus immutable PlanVersion controls.

## Implementation gate

Phase 7A must keep SES/Telegram suites green, fence destination and policy revisions at attempt start, require fresh authorization for detail, keep PII out of jobs, default unknown preview policy to generic, and remain correct without Redis. Phases 7B/7C consume this contract only after those gates pass.

## Non-goals

This record does not make provider acceptance human acknowledgement, expose precise telemetry on a lock screen, authorize asset context for members/viewers, or create a marketing notification platform.
