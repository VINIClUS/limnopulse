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

Phase 7C must prove idempotent DLR reconciliation, preserve conservative reservations after accepted or ambiguous sends, and keep provider delivery distinct from incident acknowledgement or resolution. For a definite no-acceptance/no-charge SMS result, feedback settlement must release the monetary reservation while retaining the consumed call count; final provider cost must settle actual cost and release only proven excess, while missing final feedback retains the conservative reservation. Phase 7C must also prove least-privilege AWS End User Messaging publish permission, an SQS queue policy restricted by `aws:SourceArn` to the SNS topic, a subscription delivery-failure DLQ where appropriate, and fixture-tested selection of SNS envelope or raw delivery.

## Non-goals

This record does not use SNS for policy, ledger, normal send orchestration, durable worker backpressure, or human acknowledgement.
