# Phase 3C-B: Telegram Delivery Backlog

Phase 3C-A intentionally delivers email only. The evaluator may still create a
Telegram NotificationOutbox because the Alert Rule contract already accepts the
channel. New Telegram rows are not placed on the email relay index; the Phase
3C-A backfill marks legacy rows it visits as `deferred_unsupported_channel` and
removes any conflicting relay attributes. Telegram must not be sent through the
email jobs queue.

Phase 3C-B should define and implement:

- tenant-scoped Telegram destination enrollment and verification;
- secret storage and rotation for bot credentials outside DynamoDB domain rows;
- immutable recipient snapshots without leaking chat IDs into jobs, logs or
  metrics;
- deterministic delivery identity and durable attempts compatible with the
  existing at-least-once worker model;
- Telegram-specific rate limiting, retry/permanent error taxonomy and abuse
  handling;
- suppression/unsubscribe semantics and authorization for destination changes;
- replay or migration of deferred Telegram outboxes without DynamoDB Scan;
- local fake transport, integration tests, cloud IAM/networking and operational
  runbooks;
- an explicit decision on shared versus channel-specific queues and workers.

Acceptance must prove that enabling Telegram cannot change email delivery
identity, ordering, retries, SES feedback or the one-shot relay scheduler
contract.
