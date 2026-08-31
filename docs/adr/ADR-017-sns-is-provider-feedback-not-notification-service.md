# ADR-017 — SNS is a narrow provider-event primitive, not the LimnoPulse notification service.

**Status:** Accepted

## Context

AWS End User Messaging SMS emits provider events through SNS, but SNS does not own tenant policy, destination verification, escalation, budgets, delivery identity, or acknowledgement semantics.

## Decision

Use SNS only for supported provider-event fanout, principally SMS configuration-set feedback into SQS. LimnoPulse remains the source of notification policy and durable Delivery/Attempt reconciliation.

## Consequences

Delayed, duplicate, and out-of-order provider events can be reconciled idempotently without coupling normal dispatch to SNS. The feedback adapter must authenticate and normalize provider-specific states.

## V4 traceability

V4 §§17, 21, 24 Phase 7C, and 27 define configuration set to SNS to SQS feedback and reject SNS as notification authority.

## Implementation gate

Phase 7C must prove idempotent DLR reconciliation, preserve conservative reservations after accepted or ambiguous sends, and keep provider delivery distinct from incident acknowledgement or resolution.

## Non-goals

This record does not use SNS for policy, ledger, normal send orchestration, durable worker backpressure, or human acknowledgement.
