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

Phase 7A must keep SES/Telegram suites green, fence destination and policy revisions at attempt start, keep PII out of jobs, default unknown preview policy to generic, and remain correct without Redis. At `BeginAttempt`, Phase 7A must recheck final incident and acknowledgement state; if a queued escalation was acknowledged before the provider call, dispatch must deterministically perform no send and incur no charge. Generic preview must use the exact localized `pt-BR` and `en-US` templates. Its visible-payload allowlist must exclude tenant, site/asset, location, precise telemetry, personal/phone, command, actuator, credential, token, and other sensitive fields; its data payload is limited to an opaque incident/notification ID, authenticated deep link, version, and minimal routing metadata. Phase 7A must restrict `asset_context` policy writes to owners and admins; each write must be revisioned and audited, and member or viewer updates must be rejected. Each `asset_context` preview may add at most one approved site/asset label, must record explicit exposure acknowledgement, and must never include sensitive telemetry, location, personal data, or free-form operational content. Detailed incident fetch must require fresh membership authorization. Phases 7B/7C consume this contract only after those gates pass.

## Non-goals

This record does not make provider acceptance human acknowledgement, expose precise telemetry on a lock screen, authorize asset context for members/viewers, or create a marketing notification platform.
