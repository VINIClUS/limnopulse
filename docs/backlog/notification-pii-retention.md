# Notification PII Retention and Redaction Backlog

Phase 3C-A persists a normalized email address and rendered email once on each
Delivery snapshot. This is necessary for immutable, replay-safe attempts but it
creates a data-lifecycle obligation. Delivery and Attempt rows currently have no
TTL, and DynamoDB backups can outlive active rows.

A future milestone must establish, with product/legal/security owners:

- retention periods by delivery state, attempt outcome and tenant plan;
- whether content and destination are redacted in place or rows are archived
  and deleted, while preserving minimum idempotency evidence;
- backup/PITR expiry and deletion propagation expectations;
- tenant offboarding behavior and a verifiable deletion report;
- data-subject access, correction and deletion workflows;
- authorization and audit controls for operations, DLQ inspection and support;
- encryption-key and regional-residency requirements;
- metrics that report lifecycle progress without address, content or tenant
  cardinality;
- treatment of suppression hashes, including the legal/product basis for
  retaining a do-not-contact signal after delivery data is removed.

No retention worker should be introduced until its interaction with delivery
idempotency, late SES feedback, recovery dependencies, DLQ replay and audit
requirements is specified and tested.
