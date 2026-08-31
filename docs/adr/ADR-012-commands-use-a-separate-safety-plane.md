# ADR-012 — Commands use a separate safety and physical-verification plane.

**Status:** Accepted

## Context

Sending telemetry or notifications is not equivalent to changing physical equipment. A transport success can coexist with stale preconditions, unsafe state, or failure to achieve the intended physical result.

## Decision

Commands use typed definitions, risk classes, capability and entitlement gates, approvals where required, isolated dispatch, durable audit, and explicit physical postcondition verification.

## Consequences

Manual and assisted actions are explainable and kill-switchable, and ambiguous outcomes remain visible. Command delivery requires more state and cannot reuse telemetry ingestion or notification success semantics.

## V4 traceability

V4 §§12, 24 Phase 8, 27, and 31 separate the command safety plane and identify physical verification as a highest-risk decision.

## Implementation gate

Phase 8 must reject plan-only authorization and unsafe preconditions, distinguish transport acceptance from physical success, test timeout/ambiguity, and provide global, provider, and tenant kill switches.

## Non-goals

This record does not authorize autonomous actuation, treat cloud connectivity as a critical interlock, or infer safety from commercial tier.
