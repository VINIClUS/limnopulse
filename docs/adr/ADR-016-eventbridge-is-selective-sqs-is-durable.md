# ADR-016 — EventBridge is selective integration routing/scheduling; SQS remains the durable work boundary.

**Status:** Accepted

## Context

EventBridge is useful for SES feedback and scheduling, but a custom domain bus would add schemas, routing rules, replay policy, IAM, and cost without removing the need for durable consumer backpressure.

## Decision

Use EventBridge selectively where managed routing or scheduling fits. Put asynchronous work behind SQS/DLQ or an equivalent durable consumer boundary, with domain state correct even if event publication is unavailable.

## Consequences

Workers retain retry isolation and bounded backlog behavior. Producers need fenced publication/outbox semantics, while a broader EventBridge bus remains subject to consumer and cost evidence.

## V4 traceability

V4 §§7, 17, 21, 24, 27, and the future EventBridge decision gate preserve SES feedback and Scheduler while deferring a custom domain bus.

## Implementation gate

Current SES feedback remains intact. When EventBridge Scheduler is selected for evaluator, relay, reconciliation, or backfill work, the selected IAM role and target invocation, idempotent duplicate delivery, retry behavior, and Scheduler DLQ operation where appropriate must be proven. Any future bus requires multiple justified consumers, versioned schemas, PII review, transactional publication fencing, durable target queues, failure/replay tests, IAM review, cost comparison, and reversible publication.

## Non-goals

This record does not make EventBridge a ledger, queue, ordering guarantee, or mandatory transport between all internal components.
