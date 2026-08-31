import glob
import json
import re
import shutil
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
EXPECTED = {
    "FastAPI control plane": "implemented",
    "Tenant membership authorization": "implemented",
    "Pond/Device v1 and Influx reads": "implemented",
    "Alert evaluator and durable notifications": "implemented",
    "MQTT/Telegraf/Starlark registry": "local",
    "OpenTofu cloud foundation": "scaffold",
    "Site/Asset/Component/Deployment": "planned",
    "Billing/AWS IoT/Push/SMS/commands": "planned",
    "Device permanently bound to a pond": "obsolete",
}
EXPECTED_INVENTORY_METADATA = {
    "FastAPI control plane": (
        "implemented",
        "Preserve the existing API and composition boundary.",
        "Phase 0 baseline",
    ),
    "Tenant membership authorization": (
        "implemented",
        "Preserve as a core invariant; authenticated identity is not tenant authority.",
        "Phase 0 baseline",
    ),
    "Pond/Device v1 and Influx reads": (
        "implemented",
        "Preserve `/v1`; add the generalized model behind `/v2` and retain legacy reads during telemetry migration.",
        "Phases 1–2",
    ),
    "Alert evaluator and durable notifications": (
        "implemented",
        "Preserve the evaluator and ledger; generalize metric, destination, policy, and provider boundaries additively.",
        "Phases 6–7A",
    ),
    "MQTT/Telegraf/Starlark registry": (
        "local",
        "Keep for local lab and compatibility; production moves to a trusted queue and normalizer path.",
        "Phase 3",
    ),
    "OpenTofu cloud foundation": (
        "scaffold",
        "Extend incrementally with phase-owned resources; scaffold is not deployed capability.",
        "Phase 3 onward",
    ),
    "Site/Asset/Component/Deployment": (
        "planned",
        "Add behind `/v2` with additive storage and `/v1` compatibility projection.",
        "Phase 1",
    ),
    "Billing/AWS IoT/Push/SMS/commands": (
        "planned",
        "Add through canonical internal contracts and replaceable provider or safety adapters.",
        "Phases 4, 5, 7B, 7C, 8",
    ),
    "Device permanently bound to a pond": (
        "obsolete",
        "Replace canonical v2 `pond_id` with temporal Deployment while projecting legacy behavior.",
        "Phase 1",
    ),
}
EXPECTED_ADR_FILES = (
    "ADR-001-aws-iot-is-an-integration-adapter.md",
    "ADR-002-site-and-asset-preserve-v1-pond.md",
    "ADR-003-device-component-and-temporal-deployment.md",
    "ADR-004-effective-capability-is-derived.md",
    "ADR-005-canonical-telemetry-is-metric-based.md",
    "ADR-006-telemetry-has-three-timestamps.md",
    "ADR-007-influx-v2-dual-write-migration.md",
    "ADR-008-hardware-accuracy-remains-customer-vendor-owned.md",
    "ADR-009-edge-is-optional-and-customer-hosted.md",
    "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
    "ADR-011-limnopulse-owns-notification-semantics.md",
    "ADR-012-commands-use-a-separate-safety-plane.md",
    "ADR-013-v1-remains-compatible-v2-is-generalized.md",
    "ADR-014-commercial-tier-does-not-imply-safety.md",
    "ADR-015-automatic-cloud-control-is-deferred.md",
    "ADR-016-eventbridge-is-selective-sqs-is-durable.md",
    "ADR-017-sns-is-provider-feedback-not-notification-service.md",
    "ADR-018-eum-push-and-sms-are-provider-adapters.md",
    "ADR-019-redis-valkey-is-optional-acceleration.md",
)
EXPECTED_ADR_TITLES = {
    "ADR-001-aws-iot-is-an-integration-adapter.md": "# ADR-001 — AWS IoT is an integration adapter, not the Device domain.",
    "ADR-002-site-and-asset-preserve-v1-pond.md": "# ADR-002 — Introduce Site and Asset while preserving Pond `/v1`.",
    "ADR-003-device-component-and-temporal-deployment.md": "# ADR-003 — Separate Device, Component, Probe, Actuator and temporal Deployment.",
    "ADR-004-effective-capability-is-derived.md": "# ADR-004 — Effective capability is provider/model/instance/runtime-derived.",
    "ADR-005-canonical-telemetry-is-metric-based.md": "# ADR-005 — Canonical telemetry is metric-based and transport-independent.",
    "ADR-006-telemetry-has-three-timestamps.md": "# ADR-006 — Event time, receive time and ingest time are distinct.",
    "ADR-007-influx-v2-dual-write-migration.md": "# ADR-007 — InfluxDB v2 uses generic numeric observations and dual-write migration.",
    "ADR-008-hardware-accuracy-remains-customer-vendor-owned.md": "# ADR-008 — Customer/vendor owns physical hardware accuracy and local installation.",
    "ADR-009-edge-is-optional-and-customer-hosted.md": "# ADR-009 — Edge is optional, software-only and customer-hosted.",
    "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md": "# ADR-010 — Stripe is the billing adapter; internal PlanVersion/EntitlementSnapshot is canonical.",
    "ADR-011-limnopulse-owns-notification-semantics.md": "# ADR-011 — LimnoPulse owns notification semantics, destinations and durable delivery state; providers are replaceable.",
    "ADR-012-commands-use-a-separate-safety-plane.md": "# ADR-012 — Commands use a separate safety and physical-verification plane.",
    "ADR-013-v1-remains-compatible-v2-is-generalized.md": "# ADR-013 — `/v1` remains compatible; generalized device domain is `/v2`.",
    "ADR-014-commercial-tier-does-not-imply-safety.md": "# ADR-014 — Commercial tier does not imply hardware compatibility or command safety.",
    "ADR-015-automatic-cloud-control-is-deferred.md": "# ADR-015 — Automatic cloud control is deferred; critical interlocks remain local.",
    "ADR-016-eventbridge-is-selective-sqs-is-durable.md": "# ADR-016 — EventBridge is selective integration routing/scheduling; SQS remains the durable work boundary.",
    "ADR-017-sns-is-provider-feedback-not-notification-service.md": "# ADR-017 — SNS is a narrow provider-event primitive, not the LimnoPulse notification service.",
    "ADR-018-eum-push-and-sms-are-provider-adapters.md": "# ADR-018 — AWS End User Messaging Push and SMS are the initial MVP delivery providers, not notification-domain authority.",
    "ADR-019-redis-valkey-is-optional-acceleration.md": "# ADR-019 — Redis/Valkey is optional acceleration; DynamoDB/SQS and bounded workers preserve correctness.",
}
EXPECTED_ADR_ENTRY_PHASES = {
    "ADR-001-aws-iot-is-an-integration-adapter.md": "Phase 1 contract; Phase 5 adapter",
    "ADR-002-site-and-asset-preserve-v1-pond.md": "Phase 1",
    "ADR-003-device-component-and-temporal-deployment.md": "Phase 1",
    "ADR-004-effective-capability-is-derived.md": "Phase 1 contract",
    "ADR-005-canonical-telemetry-is-metric-based.md": "Phase 2",
    "ADR-006-telemetry-has-three-timestamps.md": "Phase 2",
    "ADR-007-influx-v2-dual-write-migration.md": "Phase 2",
    "ADR-008-hardware-accuracy-remains-customer-vendor-owned.md": "Phase 6",
    "ADR-009-edge-is-optional-and-customer-hosted.md": "Phase 9",
    "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md": "Phase 4",
    "ADR-011-limnopulse-owns-notification-semantics.md": "Phase 7A",
    "ADR-012-commands-use-a-separate-safety-plane.md": "Phase 8",
    "ADR-013-v1-remains-compatible-v2-is-generalized.md": "Phase 1",
    "ADR-014-commercial-tier-does-not-imply-safety.md": "Phase 4 contract; Phase 8 safety gate",
    "ADR-015-automatic-cloud-control-is-deferred.md": "Phase 10 decision gate",
    "ADR-016-eventbridge-is-selective-sqs-is-durable.md": "Existing feedback; future bus gate",
    "ADR-017-sns-is-provider-feedback-not-notification-service.md": "Phase 7C",
    "ADR-018-eum-push-and-sms-are-provider-adapters.md": "Phases 7B–7C",
    "ADR-019-redis-valkey-is-optional-acceleration.md": "Phase 7A",
}
EXPECTED_ADR_SECTION_HEADINGS = (
    "## Context",
    "## Decision",
    "## Consequences",
    "## V4 traceability",
    "## Implementation gate",
    "## Non-goals",
)
EXPECTED_PLAN_SMS_LIMITS = {
    "Trial": {
        "provider_calls": 0,
        "budget_usd_minor": 0,
        "max_price_usd_minor": 0,
    },
    "Starter": {
        "provider_calls": 0,
        "budget_usd_minor": 0,
        "max_price_usd_minor": 0,
    },
    "Farm": {
        "provider_calls": 10,
        "budget_usd_minor": 50,
        "max_price_usd_minor": 5,
    },
    "Pro": {
        "provider_calls": 50,
        "budget_usd_minor": 250,
        "max_price_usd_minor": 5,
    },
    "Business": {
        "provider_calls": 250,
        "budget_usd_minor": 1250,
        "max_price_usd_minor": 5,
    },
}
REQUIRED_ADR_GATE_PATTERNS = {
    "ADR-001-aws-iot-is-an-integration-adapter.md": (
        r"\bBefore any queued consumer acts, it must recheck the DeviceIntegration "
        r"lifecycle state;\s+after decommission, both queued command dispatch and "
        r"queued ingest must be fenced\b",
        r"\bDuring Phase 5 enrollment, if LimnoPulse generates an AWS IoT private "
        r"key, it must return that key only at creation and must never persist it or "
        r"return it later;\s+only the certificate ID or fingerprint may be stored\b",
        r"\bPhase 5 ingest must resolve tenant ownership exclusively from the "
        r"authenticated source mapping;\s+any tenant asserted in the payload, "
        r"including another tenant's ID, must be ignored or rejected and must never "
        r"override that mapping\b",
        r"\bPhase 5 certificate rotation must verify the replacement AWS IoT identity "
        r"can connect before disabling the old certificate;\s+a failed replacement "
        r"must keep the old certificate enabled\b",
    ),
    "ADR-003-device-component-and-temporal-deployment.md": (
        r"\bPhase 1 must reject overlapping Deployment intervals for the same "
        r"Component while permitting adjacent half-open intervals where one ends "
        r"exactly when the next starts\b",
        r"\bPhase 1 concurrent relocation must use optimistic version checks so one "
        r"racing writer receives a version conflict while non-overlap, the current "
        r"Deployment pointer, and immutable ended history remain preserved\b",
        r"\bPhase 1 probe replacement must retire the old Component and ProbeProfile "
        r"identities and create new identities;\s+it must preserve prior Deployment, "
        r"telemetry, and calibration attribution and must never mutate or rewrite "
        r"that history in place\b",
    ),
    "ADR-004-effective-capability-is-derived.md": (
        r"\bPhase 6 must prove identical, reordered, and replayed health evidence "
        r"produces deterministic Device and Component health transitions, keeping "
        r"health-derived effective-capability inputs stable\b",
    ),
    "ADR-005-canonical-telemetry-is-metric-based.md": (
        r"\bBefore any canonical write, Phase 2 must deterministically classify a "
        r"schema-valid but physically implausible value against the metric/model "
        r"plausible range as `out_of_range`, never `valid`, and persist the quality "
        r"provenance\b",
    ),
    "ADR-006-telemetry-has-three-timestamps.md": (
        r"\bPhase 2 must detect negative or extreme clock skew and emit a quality "
        r"flag while preserving original event-time semantics for delayed, replayed, "
        r"and out-of-order observations\b",
        r"\bWhen no valid Deployment covers event time, Phase 2 must preserve the "
        r"source event in bounded quarantine/DLQ metadata, mark connector health, "
        r"and must not fall back to the current Deployment or location\b",
        r"\bCurrent-health views must use `received_at` or `ingested_at` so replaying "
        r"an old `observed_at` cannot refresh current health or mark a device online\b",
        r"\bEvent-time alert windows must apply a completeness delay for supported "
        r"lateness and produce order-independent outcomes for the same observations\b",
    ),
    "ADR-007-influx-v2-dual-write-migration.md": (
        r"\bBefore dual write is enabled, Phase 2 must prove an Influx outage after "
        r"durable queue acceptance leaves telemetry retryable or moves it to a "
        r"recoverable DLQ, never acknowledges or loses it after a failed write, and "
        r"recovers it without data loss\b",
    ),
    "ADR-008-hardware-accuracy-remains-customer-vendor-owned.md": (
        r"\bFor water-condition rules, Phase 6 no-data, stale, and query-error "
        r"outcomes must neither open nor resolve an incident;\s+an active incident "
        r"must remain active unless the rule is explicitly a stale/offline rule\b",
    ),
    "ADR-009-edge-is-optional-and-customer-hosted.md": (
        r"\bSeparately from edge acceptance, Phase 9 may ship the first vendor "
        r"connector only after tests prove connector upgrades preserve stable "
        r"canonical metric identity, rate-limit/retry/cursor recovery, and clearly "
        r"expose the compatibility level\b",
        r"\bIf the selected Phase 9 vendor connector has a webhook path, it must "
        r"verify signatures and deduplicate events so provider retries remain "
        r"idempotent\b",
    ),
    "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md": (
        r"\bgrace keeps ingestion and critical alerts enabled\b",
        r"\brestricted preserves critical notifications and only bounded existing ingestion\b",
        r"\bsuspended stops new paid processing,\s+disables commands\b",
        r"\bno grace, restricted, or suspended path may report monitoring as active "
        r"after ingestion or monitoring coverage has stopped\b",
        r"\bPhase 4 webhook ingress must return `2xx` only after durable queue "
        r"acceptance and must return `5xx` on transient enqueue failure so Stripe "
        r"retries;\s+signature-verified, idempotent processing must remain "
        r"asynchronous\b",
        r"\bEntitlementSnapshot cache entries must be snapshot-versioned and "
        r"short-lived;\s+a stale active cache entry must never override a newer "
        r"durable restricted or suspended state\.\s+Phase 4 tests must prove "
        r"stale-cache and mid-request suspension block both SMS spend and command "
        r"dispatch\b",
        r"\bPhase 4 must prove a BRL Stripe subscription retains the USD-denominated "
        r"SMS budget and that neither entitlement evaluation nor notification "
        r"dispatch performs a synchronous FX call\b",
        r"\bEvery Enterprise PlanVersion must explicitly set the "
        r"`notifications\.sms\.critical` boolean, SMS provider-call count, budget "
        r"amount, budget currency, maximum price, and overage behavior;\s+the "
        r"contract values may vary, but no missing field may inherit an implicit or "
        r"unlimited default\b",
        r"\bOn an absent entitlement cache entry, Phase 4 must fetch the durable "
        r"EntitlementSnapshot;\s+if the authoritative store is unavailable, paid SMS "
        r"and command actions must be conservatively denied, never treated as active "
        r"or default, so cache availability cannot change authorization, budget, or "
        r"logical outcome\b",
    ),
    "ADR-011-limnopulse-owns-notification-semantics.md": (
        r"\bPhase 7A must restrict `asset_context` policy writes to owners and admins;\s+"
        r"each write must be revisioned and audited, and member or viewer updates "
        r"must be rejected\b",
        r"\bEach `asset_context` preview may add at most one approved site/asset "
        r"label, must record explicit exposure acknowledgement, and must never "
        r"include sensitive telemetry, location, personal data, or free-form "
        r"operational content\b",
        r"\bDetailed incident fetch must require fresh membership authorization\b",
        r"\bGeneric preview must use the exact localized `pt-BR` and `en-US` "
        r"templates\.\s+Its visible-payload allowlist must exclude tenant, site/asset, "
        r"location, precise telemetry, personal/phone, command, actuator, credential, "
        r"token, and other sensitive fields;\s+its data payload is limited to an "
        r"opaque incident/notification ID, authenticated deep link, version, and "
        r"minimal routing metadata\b",
        r"\bAt `BeginAttempt`, Phase 7A must recheck final incident and "
        r"acknowledgement state;\s+if a queued escalation was acknowledged before "
        r"the provider call, dispatch must deterministically perform no send and "
        r"incur no charge\b",
        r"\bBefore queue dispatch, the same deterministic Delivery ID must create at "
        r"most one logical Delivery even when outbox or fanout expansion retries\b",
        r"\bAt `BeginAttempt`, Phase 7A must recheck recipient preference independently "
        r"of destination and policy state;\s+if the channel was disabled after queueing, "
        r"the Attempt must make no provider call and incur no charge\b",
        r"\bThe destination snapshot selected at fanout must remain immutable after "
        r"Delivery creation;\s+later token, phone, or provider-binding changes must not "
        r"rewrite queued or historical Delivery evidence\b",
    ),
    "ADR-012-commands-use-a-separate-safety-plane.md": (
        r"\bPhase 8 dispatch must require idempotency validity AND a non-expired TTL "
        r"in the same execution-gate conjunction;\s+invalid or replayed idempotency, "
        r"an expired TTL, or a command outside its time-bounded window must not "
        r"dispatch, and Phase 8 tests must verify both gates\b",
        r"\bActor permission is a mandatory Phase 8 dispatch conjunct separate from "
        r"entitlement, effective capability, and safety preconditions;\s+the safety "
        r"matrix must prove a permitted actor still needs every other gate and a "
        r"denied actor never dispatches even when those other gates pass\b",
        r"\bPhase 8 must explicitly reject stopping the last running aerator while "
        r"dissolved oxygen is low\b",
        r"\bImmediately before dispatch, Phase 8 must recheck the current approval and "
        r"governing policy revisions;\s+stale revisions, version conflicts, and "
        r"approval races must fence dispatch\b",
        r"\bPhase 8 must derive risk class from the command definition and current "
        r"context:\s+R2 requires human approval, R3 is prohibited or requires a "
        r"stricter role and site policy, and R4 is never exposed;\s+the command "
        r"risk-class matrix must prove each outcome\b",
        r"\bWhen transport succeeds but a required physical postcondition is unmet, "
        r"Phase 8 must record `physical_verification=not_confirmed`, a failed command "
        r"result, and an operational incident\b",
        r"\bPhase 8 must append an immutable audit event for every command request, "
        r"approval, dispatch, and result transition;\s+mutable command state must not "
        r"substitute for any lifecycle audit event\b",
    ),
    "ADR-015-automatic-cloud-control-is-deferred.md": (
        r"\bPhase 10 automatic execution requires dry-run history accumulated over "
        r"time and may not rely on a one-off policy simulation\b",
    ),
    "ADR-016-eventbridge-is-selective-sqs-is-durable.md": (
        r"\bselected IAM role and target invocation,\s+"
        r"idempotent duplicate delivery,\s+retry behavior,\s+and "
        r"Scheduler DLQ operation where appropriate must be proven\b",
        r"\bBecause Scheduler is at-least-once, every selected target must remain "
        r"leased and fenced;\s+Scheduler verification must prove retry overlap with a "
        r"slow invocation cannot let two workers act on the same work unit\b",
        r"\bPhase 3 HTTPS ingress must return accepted only after a durable SQS "
        r"write;\s+a failed write must not return accepted\.\s+Phase 3 must prove "
        r"at-least-once replay is safe and test DLQ redrive\b",
        r"\bPhase 3 must deny ingress credentials or mapping data when an "
        r"IntegrationAccount attempts to claim a tenant it does not own\b",
        r"\bPhase 3 must authenticate the provider source before resolving ownership "
        r"mapping and must reject missing, invalid, or mismatched credentials\b",
    ),
    "ADR-017-sns-is-provider-feedback-not-notification-service.md": (
        r"\bPhase 7C must also prove least-privilege AWS End User Messaging publish "
        r"permission, an SQS queue policy restricted by `aws:SourceArn` to the SNS "
        r"topic, a subscription delivery-failure DLQ where appropriate, and "
        r"fixture-tested selection of SNS envelope or raw delivery\b",
        r"\bFor a definite no-acceptance/no-charge SMS result, feedback settlement "
        r"must release the monetary reservation while retaining the consumed call "
        r"count;\s+final provider cost must settle actual cost and release only proven "
        r"excess, while missing final feedback retains the conservative reservation\b",
        r"\bMalformed or unrelated SNS feedback already accepted by SQS must have "
        r"bounded consumer retry/redrive, a consumer poison-message DLQ, and queue-age/"
        r"DLQ observability;\s+this consumer path is distinct from and cannot be "
        r"replaced by the SNS subscription delivery-failure DLQ\b",
        r"\bPhase 7C feedback must correlate an Attempt and reservation exclusively by "
        r"provider message ID, never by destination or phone number\b",
        r"\bAn independent feedback-consumer kill switch must pause consumption while "
        r"preserving queued events and evidence;\s+the SMS send-lane kill switch must "
        r"not substitute for it\b",
    ),
    "ADR-018-eum-push-and-sms-are-provider-adapters.md": (
        r"\bOn both Android and iOS, registration must reject cross-user and "
        r"cross-tenant claims for a token already owned by another user or tenant and "
        r"must not overwrite the existing owner\b",
        r"\bA `SendTextMessage` timeout after potential provider acceptance must leave "
        r"the Attempt unknown or awaiting provider feedback;\s+it must not be "
        r"automatically retried or resent, preventing duplicate messages and duplicate "
        r"cost\b",
        r"\bBefore Phase 7B launch, every raw Push token must be encrypted and never "
        r"returned after write\b",
        r"\bAcross Push and SMS, raw token, phone number, and message body values must "
        r"be absent from queue jobs, logs, metrics, and ordinary audit records\b",
        r"\bA provider per-address permanent Push failure must conditionally "
        r"invalidate only the destination version observed by the Attempt;\s+a late "
        r"failure for version N must preserve rotated version N\+1\b",
        r"\bA provider-returned updated FCM/GCM token must be conditionally persisted "
        r"as a fenced rotation against the destination version observed by the "
        r"Attempt;\s+it must not be ignored, and a late provider response must not "
        r"overwrite a newer client rotation\b",
        r"\bAn independent Push kill switch must stop Push while preserving durable "
        r"state and leaving email, Telegram, and SMS intact\b",
        r"\bA Push transport timeout after potential provider acceptance must become "
        r"ambiguous or unknown and must not be automatically retried or resent\b",
        r"\bEach SMS verification challenge must use a separate Attempt with digest, "
        r"TTL, attempt and rate limits, its own anti-abuse controls, and a separate "
        r"platform budget;\s+Phase 7C tests must prove it cannot share or bypass the "
        r"critical-escalation Attempt or budget\b",
        r"\bEvery verified E\.164 SMS destination must store its number using "
        r"application-level envelope encryption at rest throughout its lifecycle;\s+"
        r"plaintext phone numbers must remain excluded from ordinary reads, queue "
        r"jobs, logs, metrics, and ordinary audit records\b",
        r"\bDuplicate standard SQS Push or SMS jobs, including concurrent "
        r"multiprocess delivery, must be idempotent:\s+the same logical job yields "
        r"one durable Attempt, one provider attempt, and one cost commitment\b",
        r"\bEach Push destination must be keyed by tenant, recipient, client app, "
        r"and client app instance;\s+multiple devices for one user must coexist and "
        r"fan out independently, and registration or rotation of one instance must "
        r"not overwrite another\b",
        r"\bEvery validation or other failure before `SendTextMessage` starts must "
        r"release both the reserved count slot and the reserved USD amount\b",
        r"\bPhase 7C BR and US tests must prove SMS origination references, pools, "
        r"and routes are environment-bound and cannot cross development, staging, "
        r"or production\b",
        r"\bThe final pre-provider transaction must recheck active recipient "
        r"membership and authorization;\s+a recipient revoked after queueing must not "
        r"receive the SMS\b",
        r"\bPhase 7C must use the exact approved critical SMS templates: `pt-BR` "
        r"`LimnoPulse: incidente crítico\. Abra o app e confirme\.` and `en-US` "
        r"`LimnoPulse: critical incident\. Open the app and acknowledge\.`;\s+content "
        r"must exclude tenant, site/asset, location, precise telemetry, personal/phone, "
        r"command, actuator, credential, token, and other sensitive fields\b",
        r"\bA definite throttled or temporary Push per-address result, including 429 "
        r"or 5xx, must retry with bounds rather than being dropped or retried without "
        r"limit;\s+ambiguous and permanent outcomes retain their separate rules\b",
        r"\bPhase 7C request fixtures must prove `SendTextMessage` uses "
        r"`MessageType=TRANSACTIONAL`, the approved origination reference, protection "
        r"configuration, SMS configuration set, bounded TTL, and provider `MaxPrice`",
        r"\bAlongside route and carrier readiness, the Brazil gate must prove in-app "
        r"destination management and opt-out, disclose that the displayed origin is "
        r"unstable, and make no promise of replies or a fixed originating number\b",
        r"\bDistinct logical SMS jobs racing for the same tenant's last count slot or "
        r"USD budget must contend atomically on one conditional budget-period version "
        r"without Redis;\s+at most one winner may reserve and contact the provider, "
        r"and every loser must defer or fail before the call so the tenant cannot "
        r"overspend\b",
        r"\bIn the pre-dispatch transaction, a current-country price above the "
        r"PlanVersion cap must prevent `SendTextMessage` and consume no call count;\s+"
        r"provider `MaxPrice` remains defense in depth and must not replace this "
        r"guard\b",
        r"\bAndroid/FCM and iOS/APNs must have separate kill switches;\s+each switch "
        r"must stop only its own platform while preserving durable state, the other "
        r"platform, and every other channel\b",
        r"\bSuccessful provider Push acceptance must never become user incident "
        r"acknowledgement and must leave incident acknowledgement and escalation "
        r"state unchanged\b",
        r"\bThe public destination contract must accept only canonical `android` and "
        r"`ios` and reject provider channel names `GCM` and `APNS`;\s+adapter fixtures "
        r"must prove `android` maps to AWS `GCM` and `ios` maps to `APNS` or "
        r"`APNS_SANDBOX`\.",
        r"\bThe Phase 7C pre-dispatch transaction must require present and fresh "
        r"country configuration, route validation, origination reference, current "
        r"price, and registration state;\s+any absent or stale readiness evidence must "
        r"fail closed before the provider call\b",
        r"\bBefore production, every readiness evidence category must define its "
        r"authoritative source and an objective expiration rule using `expires_at` or "
        r"maximum age;\s+subjective freshness or evidence with no expiry must fail "
        r"closed\b",
        r"\bThe provider `ProtectConfiguration` country policy itself must allow only "
        r"BR and US and block every other country;\s+the application country allowlist "
        r"is independent and must not substitute for this provider control\b",
        r"\bFCM service-account JSON and APNs private-key payloads must never enter "
        r"OpenTofu state;\s+when infrastructure provisioning cannot avoid state "
        r"exposure, credentials must use secure post-provisioning or secret deployment\b",
        r"\bEvery Push registration or refresh must authenticate the current principal "
        r"and verify current ACTIVE tenant membership;\s+a missing, invalid, or "
        r"mismatched principal must be rejected even when the token is unclaimed, and "
        r"inactive membership must be rejected independently\b",
    ),
}
FORBIDDEN_ADR_GATE_PATTERNS = {
    "ADR-001-aws-iot-is-an-integration-adapter.md": (
        r"\bqueued consumer may act without rechecking integration lifecycle state\b",
        r"\bdecommission may leave queued commands or ingest unfenced\b",
        r"\bLimnoPulse may persist an AWS IoT generated private key in an integration "
        r"record\b",
        r"\ban AWS IoT generated private key may be returned after creation\b",
        r"\bAWS IoT ingest may resolve tenant ownership from a tenant asserted in "
        r"the payload\b",
        r"\ba payload tenant from another tenant may override the authenticated "
        r"source mapping\b",
        r"\bcertificate rotation may disable the old AWS IoT identity before "
        r"verifying the replacement can connect\b",
        r"\ba failed replacement AWS IoT identity may leave the old certificate "
        r"disabled\b",
    ),
    "ADR-003-device-component-and-temporal-deployment.md": (
        r"\bPhase 1 may accept overlapping Deployment intervals for the same "
        r"Component\b",
        r"\bPhase 1 must reject adjacent half-open Deployment intervals\b",
        r"\bconcurrent relocations may both commit without an optimistic version "
        r"conflict\b",
        r"\ba relocation race may violate non-overlap, current pointer, or immutable "
        r"history\b",
        r"\bprobe replacement may mutate the old Component or ProbeProfile identity "
        r"in place\b",
        r"\bprobe replacement may rewrite prior Deployment, telemetry, or calibration "
        r"attribution to the new probe\b",
    ),
    "ADR-004-effective-capability-is-derived.md": (
        r"\bidentical, reordered, or replayed health evidence may produce different "
        r"Device or Component health transitions\b",
    ),
    "ADR-005-canonical-telemetry-is-metric-based.md": (
        r"\ba schema-valid but implausible metric value may be classified valid "
        r"instead of out_of_range\b",
        r"\bplausible-range quality classification and provenance may occur after "
        r"the canonical write\b",
    ),
    "ADR-006-telemetry-has-three-timestamps.md": (
        r"\bnegative or extreme clock skew may pass without a quality flag\b",
        r"\bdelayed, replayed, or out-of-order observations may lose their "
        r"event-time semantics\b",
        r"\bwhen no event-time Deployment exists, telemetry may fall back to the "
        r"current Deployment or location\b",
        r"\bmissing event-time Deployment may be discarded without bounded "
        r"quarantine or DLQ metadata and without a connector-health signal\b",
        r"\breplayed old telemetry may refresh current health and mark a device online\b",
        r"\bcurrent health may ignore receive and ingest time and use old observed_at "
        r"as a fresh heartbeat\b",
        r"\bevent-time alert windows may finalize without a completeness delay\b",
        r"\bsamples within supported lateness may produce different alert outcomes by "
        r"arrival order\b",
    ),
    "ADR-007-influx-v2-dual-write-migration.md": (
        r"\bafter durable queue acceptance, an Influx write failure may acknowledge "
        r"and lose telemetry\b",
        r"\btelemetry from a failed Influx write may be neither retryable nor "
        r"recoverable from DLQ\b",
        r"\bdual write may begin before recovery from an Influx outage is proven\b",
    ),
    "ADR-008-hardware-accuracy-remains-customer-vendor-owned.md": (
        r"\bno-data, stale, or query-error evaluation may open a water-condition "
        r"incident without an explicit stale/offline rule\b",
        r"\bno-data, stale, or query-error evaluation may resolve an active "
        r"water-condition incident without an explicit stale/offline rule\b",
    ),
    "ADR-009-edge-is-optional-and-customer-hosted.md": (
        r"\ba Phase 9 connector upgrade may change canonical metric identity\b",
        r"\bthe first vendor connector may ship without rate-limit, retry, or cursor "
        r"recovery tests\b",
        r"\bthe first vendor connector may hide its compatibility level\b",
        r"\bpassing edge acceptance may substitute for vendor-connector acceptance\b",
        r"\ba selected vendor webhook path may accept events without signature "
        r"verification\b",
        r"\bvendor webhook retries may bypass event dedupe or idempotence\b",
    ),
    "ADR-011-limnopulse-owns-notification-semantics.md": (
        r"\bmember or viewer may update the asset_context preview policy\b",
        r"\basset_context policy write may proceed without revision or audit\b",
        r"\bdetailed incident fetch may proceed without fresh membership authorization\b",
        r"\bgeneric preview may use non-exact localized templates\b",
        r"\bgeneric visible payload may include tenant, asset, location, precise "
        r"telemetry, command, or sensitive fields\b",
        r"\bgeneric data payload may include operational detail beyond opaque "
        r"identifiers and minimal routing metadata\b",
        r"\bBeginAttempt may send and charge after the queued escalation was "
        r"acknowledged before the provider call\b",
        r"\ban asset_context preview may include more than one site or asset label\b",
        r"\ban asset_context label may be unapproved or exposed without "
        r"acknowledgement\b",
        r"\basset_context may include sensitive telemetry, location, personal data, "
        r"or free-form operational content\b",
        r"\bthe same delivery ID may create duplicate logical Deliveries before queue "
        r"dispatch\b",
        r"\bBeginAttempt may call the provider after the recipient disabled the "
        r"channel preference while queued\b",
        r"\bBeginAttempt may infer preferences from destination or policy state "
        r"instead of rechecking them independently\b",
        r"\btoken, phone, or provider-binding changes may rewrite a Delivery "
        r"destination snapshot after fanout\b",
        r"\ba Delivery destination snapshot may remain mutable after creation\b",
    ),
    "ADR-012-commands-use-a-separate-safety-plane.md": (
        r"\bmay proceed with invalid idempotency\b",
        r"\bmay proceed with an expired TTL or a time-unbounded command\b",
        r"\bactor permission may be inferred from entitlement, capability, or "
        r"preconditions instead of checked at dispatch\b",
        r"\bdenied actor may dispatch when entitlement, capability, and preconditions "
        r"pass\b",
        r"\bPhase 8 may dispatch a stop command for the last running aerator while "
        r"dissolved oxygen is low\b",
        r"\bdispatch may rely on queued approval and policy revisions without "
        r"rechecking their current versions\b",
        r"\bstale approval, version conflict, or approval race may proceed to "
        r"dispatch\b",
        r"\ban R2 command may dispatch without human approval\b",
        r"\ban R3 command may use a lower-risk path instead of prohibition or "
        r"stricter policy\b",
        r"\ban R4 command may be exposed\b",
        r"\bcommand risk class may ignore the current context and use only its "
        r"definition\b",
        r"\ban unmet physical postcondition may leave the command result successful\b",
        r"\ban unmet physical postcondition may omit physical_verification=not_confirmed "
        r"or the operational incident\b",
        r"\bmutable command state may substitute for immutable command lifecycle "
        r"audit events\b",
        r"\bcommand request, approval, dispatch, or result may omit its immutable "
        r"audit event\b",
    ),
    "ADR-015-automatic-cloud-control-is-deferred.md": (
        r"\bPhase 10 may approve automatic execution from a one-off policy simulation "
        r"without dry-run history over time\b",
    ),
    "ADR-016-eventbridge-is-selective-sqs-is-durable.md": (
        r"\bwithout leases\b",
        r"\bwithout fencing\b",
        r"\bPhase 3 HTTPS ingress may return accepted before a durable SQS write or "
        r"after the write fails\b",
        r"\bPhase 3 at-least-once replay may be unsafe and its DLQ redrive need not "
        r"be tested\b",
        r"\bPhase 3 may allow an IntegrationAccount to claim a tenant it does not own\b",
        r"\bingress credentials or mapping data may override IntegrationAccount "
        r"tenant ownership\b",
        r"\bPhase 3 may resolve ingress ownership before authenticating the provider "
        r"source\b",
        r"\bPhase 3 may accept missing, invalid, or mismatched provider credentials\b",
    ),
    "ADR-017-sns-is-provider-feedback-not-notification-service.md": (
        r"\bwithout least privilege\b",
        r"\bmay omit the aws:SourceArn restriction\b",
        r"\bmay omit (?:its|the) delivery-failure DLQ\b",
        r"\bneed not be fixture-tested\b",
        r"\ba definite no-charge SMS result may release the consumed call count\b",
        r"\ba definite no-charge SMS result may retain the monetary reservation\b",
        r"\bfinal provider cost may skip actual-cost settlement or release of proven "
        r"excess\b",
        r"\bmissing final SMS feedback may release the conservative monetary "
        r"reservation\b",
        r"\bmalformed or unrelated SNS feedback already in SQS may retry without "
        r"bounds and bypass the consumer DLQ\b",
        r"\bmalformed or unrelated SNS feedback may fail without consumer "
        r"observability\b",
        r"\bthe SNS subscription delivery-failure DLQ may substitute for consumer "
        r"redrive and its poison-message DLQ\b",
        r"\bSMS feedback may correlate an Attempt by destination phone number instead "
        r"of provider message ID\b",
        r"\bprovider message ID need not be the exclusive SMS feedback correlation key\b",
        r"\bthe feedback-consumer kill switch may discard queued events or evidence\b",
        r"\bthe SMS send-lane kill switch may substitute for an independent "
        r"feedback-consumer kill switch\b",
    ),
    "ADR-018-eum-push-and-sms-are-provider-adapters.md": (
        r"\bmay accept cross-user and cross-tenant claims\b.*\boverwrite\b",
        r"\btimeout after potential SendTextMessage acceptance may be automatically "
        r"retried or resent\b",
        r"\bambiguous SMS send may be resent even when that can duplicate the message "
        r"and cost\b",
        r"\braw Push tokens may be stored unencrypted or returned after write\b",
        r"\bPush token, phone number, or message body may appear in queue jobs, logs, "
        r"metrics, or ordinary audit\b",
        r"\blate permanent Push failure for destination version N may invalidate "
        r"rotated version N\+1\b",
        r"\bPush kill switch may discard durable state or disable email, Telegram, "
        r"or SMS\b",
        r"\bPush timeout after potential provider acceptance may be automatically "
        r"retried or resent\b",
        r"\bSMS verification challenge may share the critical-escalation Attempt and "
        r"platform budget\b",
        r"\bSMS verification may omit its digest, TTL, attempt limits, rate limits, "
        r"or anti-abuse tests\b",
        r"\ba verified E\.164 SMS number may be stored unencrypted at rest\b",
        r"\bduplicate Push or SMS SQS jobs may create multiple durable Attempts, "
        r"provider attempts, or cost commitments\b",
        r"\bregistration for a second client app instance may overwrite the user's "
        r"existing Push destination\b",
        r"\bmultiple devices for one user may be collapsed into one Push destination "
        r"instead of independent fanout\b",
        r"\ba validation failure before SendTextMessage may retain the reserved "
        r"count slot\b",
        r"\ba failure before SendTextMessage may retain the reserved USD amount\b",
        r"\bdevelopment or staging may use production SMS origination references, "
        r"pools, or routes\b",
        r"\bPhase 7C may omit BR and US SMS origination environment-isolation tests\b",
        r"\bSMS dispatch may skip the final active membership and recipient-authorization "
        r"recheck\b",
        r"\ba recipient revoked after queueing may still receive the tenant SMS\b",
        r"\bPhase 7C may use non-approved localized critical SMS templates\b",
        r"\bcritical SMS may include tenant, asset, precise telemetry, phone, or "
        r"command detail\b",
        r"\ba definite throttled or temporary Push 429/5xx result may be dropped "
        r"without retry\b",
        r"\ba definite throttled or temporary Push result may retry without bounds\b",
        r"\bSendTextMessage may omit transactional type, origination reference, "
        r"protection configuration, or SMS configuration set\b",
        r"\bSendTextMessage may omit a bounded TTL or provider MaxPrice\b",
        r"\ba provider-returned updated FCM/GCM token may be ignored instead of "
        r"conditionally persisted\b",
        r"\ba late provider-returned updated token may overwrite a newer client "
        r"rotation\b",
        r"\bBrazil may launch without in-app destination management or opt-out\b",
        r"\bBrazil may omit disclosure that the displayed origin is unstable\b",
        r"\bBrazil may promise replies or a fixed originating number\b",
        r"\bdistinct logical SMS jobs may reserve the last count slot and USD budget "
        r"using separate budget-period versions\b",
        r"\ba same-tenant SMS budget race may overspend or let more than one loser "
        r"contact the provider\b",
        r"\bcorrect SMS reservation concurrency may depend on Redis\b",
        r"\ba current-country price above the PlanVersion cap may start "
        r"SendTextMessage and consume the call count\b",
        r"\bthe provider MaxPrice check may replace the pre-dispatch country-price "
        r"guard\b",
        r"\bthe Android/FCM kill switch may also stop iOS/APNs\b",
        r"\bthe iOS/APNs kill switch may also stop Android/FCM or discard durable "
        r"state\b",
        r"\bsuccessful provider Push acceptance may acknowledge the user incident\b",
        r"\bsuccessful provider Push acceptance may change or cancel escalation state\b",
        r"\bthe public canonical Push platform may accept AWS GCM or APNS channel "
        r"names\b",
        r"\bthe adapter may map canonical android to APNS instead of AWS GCM\b",
        r"\bthe adapter may map canonical ios to GCM instead of APNS or APNS_SANDBOX\b",
        r"\bSMS pre-dispatch may proceed when country configuration, route validation, "
        r"origination reference, current price, or registration state is absent\b",
        r"\bSMS pre-dispatch may rely on stale country readiness evidence\b",
        r"\bProtectConfiguration may allow countries beyond BR and US\b",
        r"\bthe application country allowlist may substitute for a restrictive "
        r"ProtectConfiguration policy\b",
        r"\bFCM service-account JSON or an APNs private key may be stored in OpenTofu "
        r"state\b",
        r"\bOpenTofu may deploy raw Push credentials directly instead of secure "
        r"post-provisioning or secret deployment\b",
        r"\ban unclaimed Push token may be registered without an authenticated tenant "
        r"member\b",
        r"\bPush registration may accept a missing, invalid, or mismatched principal\b",
        r"(?=[^.\n]*\b(?:SMS|production) readiness(?: freshness)?\b)"
        r"(?=[^.\n]*\bsubjective\b)(?=[^.\n]*\b(?:may|need not|without|omit|no)\b)"
        r"(?=[^.\n]*objective (?:expiry|expiration))"
        r"(?=[^.\n]*authoritative (?:evidence )?source)[^.\n]*\.",
        r"(?=[^.\n]*\bPush registration(?: or refresh)?\b)"
        r"(?=[^.\n]*\baccept\b)(?=[^.\n]*\binactive tenant membership\b)"
        r"[^.\n]*\.",
    ),
    "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md": (
        r"\bStripe webhook ingress may return 2xx before durable queue acceptance\b",
        r"\btransient Stripe enqueue failure may return 2xx instead of 5xx\b",
        r"\bstale active entitlement cache entry may override a newer durable "
        r"restricted or suspended state\b",
        r"\bmid-request suspension may still allow SMS spend or command dispatch\b",
        r"\ba BRL Stripe subscription may convert the SMS budget away from USD\b",
        r"\bentitlement evaluation or notification dispatch may perform synchronous "
        r"FX calls\b",
        r"\ban Enterprise PlanVersion may omit SMS count, budget, currency, max price, "
        r"or overage fields\b",
        r"\bmissing Enterprise SMS limits may inherit implicit or unlimited defaults\b",
        r"(?=(?:[^.\n]|\.(?=[A-Za-z]))*\bEnterprise PlanVersion\b)"
        r"(?=(?:[^.\n]|\.(?=[A-Za-z]))*\bomit(?:s|ted)?\b)"
        r"(?=(?:[^.\n]|\.(?=[A-Za-z]))*notifications\.sms\.critical)"
        r"(?=(?:[^.\n]|\.(?=[A-Za-z]))*\binherit(?:s|ed)?\b)"
        r"(?:[^.\n]|\.(?=[A-Za-z]))*\.",
        r"\ban absent entitlement cache entry may be treated as active or default "
        r"entitlement\b",
        r"\bwhen durable entitlement lookup is unavailable, a cache miss may allow "
        r"paid SMS or command action instead of conservative denial\b",
    ),
}


def inventory_rows(
    path: Path = ROOT / "docs/current-state.md",
) -> dict[str, tuple[str, tuple[str, ...], str, str]]:
    rows: dict[str, tuple[str, tuple[str, ...], str, str]] = {}
    text = path.read_text(encoding="utf-8")
    for line in text.splitlines():
        cells = [cell.strip() for cell in line.strip().split("|")]
        if len(cells) == 7 and cells[1] not in {"", "Surface", "---"}:
            surface = cells[1]
            assert surface not in rows, f"duplicate inventory surface: {surface}"
            evidence_cell = cells[3]
            evidence_paths = tuple(re.findall(r"`([^`]+)`", evidence_cell))
            assert evidence_paths and evidence_cell == ", ".join(
                f"`{evidence}`" for evidence in evidence_paths
            ), f"invalid evidence path list: {surface}"
            for evidence in evidence_paths:
                relative_path = Path(evidence)
                assert not relative_path.is_absolute() and ".." not in relative_path.parts, (
                    f"evidence path must be repository-relative: {evidence}"
                )
                resolves = (
                    any(ROOT.glob(evidence))
                    if glob.has_magic(evidence)
                    else (ROOT / relative_path).exists()
                )
                assert resolves, f"evidence path does not resolve: {evidence}"
            rows[surface] = (
                cells[2].strip("`"),
                evidence_paths,
                cells[4],
                cells[5],
            )
    return rows


def test_inventory_uses_the_approved_statuses() -> None:
    assert {
        surface: row[0] for surface, row in inventory_rows().items()
    } == EXPECTED


def inventory_metadata(
    path: Path = ROOT / "docs/current-state.md",
) -> dict[str, tuple[str, ...]]:
    return {
        surface: (row[0], *row[2:])
        for surface, row in inventory_rows(path).items()
    }


def assert_inventory_metadata(path: Path = ROOT / "docs/current-state.md") -> None:
    assert inventory_metadata(path) == EXPECTED_INVENTORY_METADATA, (
        "exact V4 treatment and owning phase mapping required"
    )


def test_inventory_uses_exact_v4_treatment_and_owning_phase() -> None:
    assert_inventory_metadata()


@pytest.mark.parametrize(
    ("old", "new"),
    (
        ("Preserve the existing API and composition boundary.", ""),
        ("Phase 0 baseline", "Phase 9"),
    ),
)
def test_inventory_parser_retains_semantic_columns(
    tmp_path: Path,
    old: str,
    new: str,
) -> None:
    inventory_path = tmp_path / "current-state.md"
    inventory = (ROOT / "docs/current-state.md").read_text(encoding="utf-8")
    mutated = inventory.replace(old, new, 1)
    assert mutated != inventory
    inventory_path.write_text(mutated, encoding="utf-8")

    with pytest.raises(AssertionError, match="exact V4 treatment"):
        assert_inventory_metadata(inventory_path)


def test_inventory_rejects_duplicate_surface(tmp_path: Path) -> None:
    inventory_path = tmp_path / "current-state.md"
    inventory_path.write_text(
        (ROOT / "docs/current-state.md").read_text(encoding="utf-8")
        + "\n| FastAPI control plane | `implemented` | `duplicate.py` | Preserve. | Phase 0 |\n",
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="duplicate inventory surface"):
        inventory_rows(inventory_path)


def test_inventory_rejects_missing_evidence_path(tmp_path: Path) -> None:
    inventory_path = tmp_path / "current-state.md"
    inventory = (ROOT / "docs/current-state.md").read_text(encoding="utf-8")
    inventory_path.write_text(
        inventory.replace(
            "`src/limnopulse_api/main.py`",
            "`src/limnopulse_api/missing-main.py`",
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="evidence path"):
        inventory_rows(inventory_path)


def assert_adr_inventory(adr_root: Path) -> None:
    index_path = adr_root / "README.md"
    assert index_path.is_file(), "missing docs/adr/README.md"
    index = index_path.read_text(encoding="utf-8")
    link_targets = set(re.findall(r"\[[^\]]+\]\(([^)]+)\)", index))
    index_rows = tuple(
        re.findall(
            r"^\| \[([^\]]+)\]\(([^)]+)\) \| ([^|]+) \|$",
            index,
            flags=re.MULTILINE,
        )
    )
    entry_phases = {target: phase for _, target, phase in index_rows}

    expected = set(EXPECTED_ADR_FILES)
    discovered = {path.name for path in adr_root.glob("ADR-*.md")}
    assert discovered == expected, (
        f"unexpected ADR files: {sorted(discovered - expected)}; "
        f"missing ADR files: {sorted(expected - discovered)}"
    )

    for filename in EXPECTED_ADR_FILES:
        path = adr_root / filename
        assert path.is_file(), f"missing docs/adr/{filename}"
        assert filename in link_targets, (
            f"docs/adr/README.md does not link exact Markdown target {filename}"
        )
        record = path.read_text(encoding="utf-8")
        headings = tuple(re.findall(r"^#{1,6} .+$", record, flags=re.MULTILINE))
        expected_headings = (
            EXPECTED_ADR_TITLES[filename],
            *EXPECTED_ADR_SECTION_HEADINGS,
        )
        assert headings == expected_headings, f"exact ADR headings required: {filename}"
        status_lines = tuple(
            re.findall(
                r"^\*\*Status:\*\* ([^\n]+)$",
                record,
                flags=re.MULTILINE,
            )
        )
        assert status_lines == ("Accepted",), (
            f"dedicated ADR status must be exactly Accepted: {filename}"
        )
        implementation_gate = re.search(
            r"(?ms)^## Implementation gate\n\n(.*?)(?=^## Non-goals$)",
            record,
        )
        gate_body = implementation_gate.group(1) if implementation_gate else ""
        assert gate_body.strip(), (
            f"normative implementation gate must be non-empty: {filename}"
        )
        required_gate_patterns = REQUIRED_ADR_GATE_PATTERNS.get(filename, ())
        if required_gate_patterns:
            missing_patterns = tuple(
                pattern
                for pattern in required_gate_patterns
                if not re.search(
                    pattern,
                    gate_body,
                    flags=re.IGNORECASE | re.DOTALL,
                )
            )
            assert not missing_patterns, (
                f"normative implementation gate markers missing in {filename}: "
                f"{missing_patterns}"
            )
            forbidden_patterns = tuple(
                pattern
                for pattern in FORBIDDEN_ADR_GATE_PATTERNS.get(filename, ())
                if re.search(
                    pattern,
                    gate_body,
                    flags=re.IGNORECASE | re.DOTALL,
                )
            )
            assert not forbidden_patterns, (
                f"normative implementation gate inversions in {filename}: "
                f"{forbidden_patterns}"
            )
            if filename == (
                "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md"
            ):
                serialized_mapping = re.search(
                    r"(?ms)Phase 4 must freeze and test this exact serialized launch "
                    r"SMS mapping; USD fields use integer minor units:\n\n"
                    r"```json\n(.*?)\n```",
                    gate_body,
                )
                assert serialized_mapping, "exact launch SMS mapping is missing"
                try:
                    plan_sms_limits = json.loads(serialized_mapping.group(1))
                except json.JSONDecodeError as error:
                    raise AssertionError(
                        "exact launch SMS mapping must be valid JSON"
                    ) from error
                non_integer_fields = (
                    (("<root>", "<mapping>"),)
                    if not isinstance(plan_sms_limits, dict)
                    else tuple(
                        (tier, field)
                        for tier, expected_fields in EXPECTED_PLAN_SMS_LIMITS.items()
                        for field in expected_fields
                        if not isinstance(plan_sms_limits.get(tier), dict)
                        or type(plan_sms_limits[tier].get(field)) is not int
                    )
                )
                assert not non_integer_fields, (
                    "exact launch SMS mapping integer fields must use JSON integers: "
                    f"{non_integer_fields}"
                )
                assert plan_sms_limits == EXPECTED_PLAN_SMS_LIMITS, (
                    "exact launch SMS mapping must retain every tier and value"
                )

    assert entry_phases == EXPECTED_ADR_ENTRY_PHASES, (
        "docs/adr/README.md must retain the exact ADR entry phase mapping"
    )
    expected_index_rows = tuple(
        (filename[:7], filename, EXPECTED_ADR_ENTRY_PHASES[filename])
        for filename in EXPECTED_ADR_FILES
    )
    assert index_rows == expected_index_rows, (
        "docs/adr/README.md must retain the exact ADR index triples"
    )


def test_adr_index_is_complete_and_every_record_is_accepted() -> None:
    assert_adr_inventory(ROOT / "docs/adr")


def test_adr_record_requires_dedicated_accepted_status(tmp_path: Path) -> None:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    adr_path = adr_root / EXPECTED_ADR_FILES[0]
    record = adr_path.read_text(encoding="utf-8")
    proposed_with_history = record.replace(
        "**Status:** Accepted",
        "**Status:** Proposed",
        1,
    ).replace(
        "\n## Non-goals",
        "\nHistorical note: the earlier draft contained **Status:** Accepted.\n\n"
        "## Non-goals",
        1,
    )
    assert proposed_with_history != record
    adr_path.write_text(proposed_with_history, encoding="utf-8")

    with pytest.raises(AssertionError, match="dedicated ADR status"):
        assert_adr_inventory(adr_root)


def test_adr_inventory_rejects_unexpected_record(tmp_path: Path) -> None:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    (adr_root / "ADR-999-stale.md").write_text(
        "# ADR-999 — Stale record\n\n**Status:** Accepted\n",
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="unexpected ADR files"):
        assert_adr_inventory(adr_root)


def test_adr_index_requires_exact_markdown_link_target(tmp_path: Path) -> None:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    filename = EXPECTED_ADR_FILES[0]
    index_path = adr_root / "README.md"
    index = index_path.read_text(encoding="utf-8")
    index_path.write_text(
        index.replace(f"]({filename})", "](ADR-999-wrong-target.md)", 1)
        + f"\nPlain-text stale filename: {filename}\n",
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="exact Markdown target"):
        assert_adr_inventory(adr_root)


def test_adr_record_requires_exact_headings(tmp_path: Path) -> None:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    adr_path = adr_root / EXPECTED_ADR_FILES[0]
    record = adr_path.read_text(encoding="utf-8")
    adr_path.write_text(
        record.replace("## Decision", "## Resolution", 1),
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="exact ADR headings"):
        assert_adr_inventory(adr_root)


def test_adr_index_requires_exact_entry_phase_mapping(tmp_path: Path) -> None:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    filename = "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md"
    index_path = adr_root / "README.md"
    index = index_path.read_text(encoding="utf-8")
    index_path.write_text(
        index.replace(f"]({filename}) | Phase 4 |", f"]({filename}) | Phase 5 |", 1),
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="exact ADR entry phase mapping"):
        assert_adr_inventory(adr_root)


def test_adr_index_requires_exact_visible_label(tmp_path: Path) -> None:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    index_path = adr_root / "README.md"
    index = index_path.read_text(encoding="utf-8")
    index_path.write_text(
        index.replace("[ADR-001](", "[ADR-999](", 1),
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="exact ADR index triples"):
        assert_adr_inventory(adr_root)


@pytest.mark.parametrize(
    "filename",
    EXPECTED_ADR_FILES,
)
def test_normative_implementation_gate_cannot_be_removed(
    tmp_path: Path,
    filename: str,
) -> None:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    adr_path = adr_root / filename
    record = adr_path.read_text(encoding="utf-8")
    without_gate_body = re.sub(
        r"(?ms)(^## Implementation gate\n\n).*?(?=^## Non-goals$)",
        r"\1",
        record,
    )
    assert without_gate_body != record
    adr_path.write_text(without_gate_body, encoding="utf-8")

    with pytest.raises(AssertionError, match="normative implementation gate"):
        assert_adr_inventory(adr_root)


def test_billing_gate_forbids_false_active_monitoring(tmp_path: Path) -> None:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    adr_path = (
        adr_root
        / "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md"
    )
    record = adr_path.read_text(encoding="utf-8")
    weakened_gate = record.replace(
        "No grace, restricted, or suspended path may report monitoring as active",
        "A grace, restricted, or suspended path may report monitoring as active",
        1,
    )
    assert weakened_gate != record
    adr_path.write_text(weakened_gate, encoding="utf-8")

    with pytest.raises(AssertionError, match="normative implementation gate"):
        assert_adr_inventory(adr_root)


@pytest.mark.parametrize(
    ("required_clause", "inverted_clause"),
    (
        (
            "grace keeps ingestion and critical alerts enabled",
            "grace stops ingestion and disables critical alerts",
        ),
        (
            "restricted preserves critical notifications and only bounded existing ingestion",
            "restricted disables critical notifications and permits unlimited existing ingestion",
        ),
        (
            "suspended stops new paid processing, disables commands",
            "suspended continues new paid processing, enables commands",
        ),
    ),
)
def test_billing_gate_rejects_inverted_state_behavior(
    tmp_path: Path,
    required_clause: str,
    inverted_clause: str,
) -> None:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    adr_path = (
        adr_root
        / "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md"
    )
    record = adr_path.read_text(encoding="utf-8")
    inverted_gate = record.replace(required_clause, inverted_clause, 1)
    assert inverted_gate != record
    adr_path.write_text(inverted_gate, encoding="utf-8")

    with pytest.raises(AssertionError, match="normative implementation gate"):
        assert_adr_inventory(adr_root)


@pytest.mark.parametrize(
    ("required_clause", "inverted_clause"),
    (
        (
            "idempotent duplicate delivery",
            "non-idempotent duplicate delivery",
        ),
        ("retry behavior", "no retry behavior"),
        (
            "Scheduler DLQ operation",
            "no Scheduler DLQ operation",
        ),
    ),
)
def test_scheduler_gate_rejects_negative_reliability_behavior(
    tmp_path: Path,
    required_clause: str,
    inverted_clause: str,
) -> None:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    adr_path = adr_root / "ADR-016-eventbridge-is-selective-sqs-is-durable.md"
    record = adr_path.read_text(encoding="utf-8")
    inverted_gate = record.replace(required_clause, inverted_clause, 1)
    assert inverted_gate != record
    adr_path.write_text(inverted_gate, encoding="utf-8")

    with pytest.raises(AssertionError, match="normative implementation gate"):
        assert_adr_inventory(adr_root)


@pytest.mark.parametrize(
    ("filename", "inverted_clause"),
    (
        (
            "ADR-016-eventbridge-is-selective-sqs-is-durable.md",
            "At-least-once Scheduler targets may run without leases during retry overlap with a slow invocation.",
        ),
        (
            "ADR-016-eventbridge-is-selective-sqs-is-durable.md",
            "At-least-once Scheduler targets may run without fencing during retry overlap with a slow invocation.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Android and iOS may accept cross-user and cross-tenant claims for an already owned token and overwrite its owner.",
        ),
        (
            "ADR-017-sns-is-provider-feedback-not-notification-service.md",
            "AWS End User Messaging may publish to the SNS topic without least privilege.",
        ),
        (
            "ADR-017-sns-is-provider-feedback-not-notification-service.md",
            "The SQS queue policy may omit the aws:SourceArn restriction.",
        ),
        (
            "ADR-017-sns-is-provider-feedback-not-notification-service.md",
            "The subscription may omit its delivery-failure DLQ.",
        ),
        (
            "ADR-017-sns-is-provider-feedback-not-notification-service.md",
            "SNS envelope and raw delivery need not be fixture-tested.",
        ),
        (
            "ADR-012-commands-use-a-separate-safety-plane.md",
            "Phase 8 dispatch may proceed with invalid idempotency.",
        ),
        (
            "ADR-012-commands-use-a-separate-safety-plane.md",
            "Phase 8 dispatch may proceed with an expired TTL or a time-unbounded command.",
        ),
    ),
)
def test_adr_gate_rejects_round_four_semantic_inversion(
    tmp_path: Path,
    filename: str,
    inverted_clause: str,
) -> None:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    adr_path = adr_root / filename
    record = adr_path.read_text(encoding="utf-8")
    inverted_gate = record.replace(
        "\n## Non-goals",
        f"\n{inverted_clause}\n\n## Non-goals",
        1,
    )
    assert inverted_gate != record
    adr_path.write_text(inverted_gate, encoding="utf-8")

    with pytest.raises(AssertionError, match="normative implementation gate"):
        assert_adr_inventory(adr_root)


@pytest.mark.parametrize(
    ("filename", "inverted_clause"),
    (
        (
            "ADR-003-device-component-and-temporal-deployment.md",
            "Phase 1 may accept overlapping Deployment intervals for the same Component.",
        ),
        (
            "ADR-003-device-component-and-temporal-deployment.md",
            "Phase 1 must reject adjacent half-open Deployment intervals.",
        ),
        (
            "ADR-015-automatic-cloud-control-is-deferred.md",
            "Phase 10 may approve automatic execution from a one-off policy simulation without dry-run history over time.",
        ),
        (
            "ADR-001-aws-iot-is-an-integration-adapter.md",
            "A queued consumer may act without rechecking integration lifecycle state.",
        ),
        (
            "ADR-001-aws-iot-is-an-integration-adapter.md",
            "Decommission may leave queued commands or ingest unfenced.",
        ),
        (
            "ADR-011-limnopulse-owns-notification-semantics.md",
            "A member or viewer may update the asset_context preview policy.",
        ),
        (
            "ADR-011-limnopulse-owns-notification-semantics.md",
            "An asset_context policy write may proceed without revision or audit.",
        ),
        (
            "ADR-011-limnopulse-owns-notification-semantics.md",
            "Detailed incident fetch may proceed without fresh membership authorization.",
        ),
        (
            "ADR-006-telemetry-has-three-timestamps.md",
            "Negative or extreme clock skew may pass without a quality flag.",
        ),
        (
            "ADR-006-telemetry-has-three-timestamps.md",
            "Delayed, replayed, or out-of-order observations may lose their event-time semantics.",
        ),
    ),
)
def test_adr_gate_rejects_round_five_semantic_inversion(
    tmp_path: Path,
    filename: str,
    inverted_clause: str,
) -> None:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    adr_path = adr_root / filename
    record = adr_path.read_text(encoding="utf-8")
    inverted_gate = record.replace(
        "\n## Non-goals",
        f"\n{inverted_clause}\n\n## Non-goals",
        1,
    )
    assert inverted_gate != record
    adr_path.write_text(inverted_gate, encoding="utf-8")

    with pytest.raises(AssertionError, match="normative implementation gate"):
        assert_adr_inventory(adr_root)


@pytest.mark.parametrize(
    ("filename", "inverted_clause"),
    (
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "A timeout after potential SendTextMessage acceptance may be automatically retried or resent.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "An ambiguous SMS send may be resent even when that can duplicate the message and cost.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Raw Push tokens may be stored unencrypted or returned after write.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "A Push token, phone number, or message body may appear in queue jobs, logs, metrics, or ordinary audit.",
        ),
        (
            "ADR-012-commands-use-a-separate-safety-plane.md",
            "Actor permission may be inferred from entitlement, capability, or preconditions instead of checked at dispatch.",
        ),
        (
            "ADR-012-commands-use-a-separate-safety-plane.md",
            "A denied actor may dispatch when entitlement, capability, and preconditions pass.",
        ),
        (
            "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
            "Stripe webhook ingress may return 2xx before durable queue acceptance.",
        ),
        (
            "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
            "A transient Stripe enqueue failure may return 2xx instead of 5xx.",
        ),
    ),
)
def test_adr_gate_rejects_round_six_semantic_inversion(
    tmp_path: Path,
    filename: str,
    inverted_clause: str,
) -> None:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    adr_path = adr_root / filename
    record = adr_path.read_text(encoding="utf-8")
    inverted_gate = record.replace(
        "\n## Non-goals",
        f"\n{inverted_clause}\n\n## Non-goals",
        1,
    )
    assert inverted_gate != record
    adr_path.write_text(inverted_gate, encoding="utf-8")

    with pytest.raises(AssertionError, match="normative implementation gate"):
        assert_adr_inventory(adr_root)


@pytest.mark.parametrize(
    ("filename", "inverted_clause"),
    (
        (
            "ADR-012-commands-use-a-separate-safety-plane.md",
            "Phase 8 may dispatch a stop command for the last running aerator while dissolved oxygen is low.",
        ),
        (
            "ADR-012-commands-use-a-separate-safety-plane.md",
            "Dispatch may rely on queued approval and policy revisions without rechecking their current versions.",
        ),
        (
            "ADR-012-commands-use-a-separate-safety-plane.md",
            "A stale approval, version conflict, or approval race may proceed to dispatch.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "A late permanent Push failure for destination version N may invalidate rotated version N+1.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "The Push kill switch may discard durable state or disable email, Telegram, or SMS.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "A Push timeout after potential provider acceptance may be automatically retried or resent.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "An SMS verification challenge may share the critical-escalation Attempt and platform budget.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "SMS verification may omit its digest, TTL, attempt limits, rate limits, or anti-abuse tests.",
        ),
        (
            "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
            "A stale active entitlement cache entry may override a newer durable restricted or suspended state.",
        ),
        (
            "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
            "A mid-request suspension may still allow SMS spend or command dispatch.",
        ),
        (
            "ADR-011-limnopulse-owns-notification-semantics.md",
            "Generic preview may use non-exact localized templates.",
        ),
        (
            "ADR-011-limnopulse-owns-notification-semantics.md",
            "Generic visible payload may include tenant, asset, location, precise telemetry, command, or sensitive fields.",
        ),
        (
            "ADR-011-limnopulse-owns-notification-semantics.md",
            "Generic data payload may include operational detail beyond opaque identifiers and minimal routing metadata.",
        ),
        (
            "ADR-004-effective-capability-is-derived.md",
            "Identical, reordered, or replayed health evidence may produce different Device or Component health transitions.",
        ),
    ),
)
def test_adr_gate_rejects_round_seven_semantic_inversion(
    tmp_path: Path,
    filename: str,
    inverted_clause: str,
) -> None:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    adr_path = adr_root / filename
    record = adr_path.read_text(encoding="utf-8")
    inverted_gate = record.replace(
        "\n## Non-goals",
        f"\n{inverted_clause}\n\n## Non-goals",
        1,
    )
    assert inverted_gate != record
    adr_path.write_text(inverted_gate, encoding="utf-8")

    with pytest.raises(AssertionError, match="normative implementation gate"):
        assert_adr_inventory(adr_root)


@pytest.mark.parametrize(
    ("filename", "inverted_clause"),
    (
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "A verified E.164 SMS number may be stored unencrypted at rest.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Duplicate Push or SMS SQS jobs may create multiple durable Attempts, provider attempts, or cost commitments.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Registration for a second client app instance may overwrite the user's existing Push destination.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Multiple devices for one user may be collapsed into one Push destination instead of independent fanout.",
        ),
        (
            "ADR-011-limnopulse-owns-notification-semantics.md",
            "BeginAttempt may send and charge after the queued escalation was acknowledged before the provider call.",
        ),
        (
            "ADR-016-eventbridge-is-selective-sqs-is-durable.md",
            "Phase 3 HTTPS ingress may return accepted before a durable SQS write or after the write fails.",
        ),
        (
            "ADR-016-eventbridge-is-selective-sqs-is-durable.md",
            "Phase 3 at-least-once replay may be unsafe and its DLQ redrive need not be tested.",
        ),
        (
            "ADR-006-telemetry-has-three-timestamps.md",
            "When no event-time Deployment exists, telemetry may fall back to the current Deployment or location.",
        ),
        (
            "ADR-006-telemetry-has-three-timestamps.md",
            "Missing event-time Deployment may be discarded without bounded quarantine or DLQ metadata and without a connector-health signal.",
        ),
    ),
)
def test_adr_gate_rejects_round_eight_semantic_inversion(
    tmp_path: Path,
    filename: str,
    inverted_clause: str,
) -> None:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    adr_path = adr_root / filename
    record = adr_path.read_text(encoding="utf-8")
    inverted_gate = record.replace(
        "\n## Non-goals",
        f"\n{inverted_clause}\n\n## Non-goals",
        1,
    )
    assert inverted_gate != record
    adr_path.write_text(inverted_gate, encoding="utf-8")

    with pytest.raises(AssertionError, match="normative implementation gate"):
        assert_adr_inventory(adr_root)


@pytest.mark.parametrize(
    ("tier", "field", "expected_value", "wrong_value"),
    (
        ("Trial", "provider_calls", 0, 1),
        ("Trial", "budget_usd_minor", 0, 1),
        ("Trial", "max_price_usd_minor", 0, 1),
        ("Starter", "provider_calls", 0, 1),
        ("Starter", "budget_usd_minor", 0, 1),
        ("Starter", "max_price_usd_minor", 0, 1),
        ("Farm", "provider_calls", 10, 11),
        ("Farm", "budget_usd_minor", 50, 51),
        ("Farm", "max_price_usd_minor", 5, 6),
        ("Pro", "provider_calls", 50, 51),
        ("Pro", "budget_usd_minor", 250, 251),
        ("Pro", "max_price_usd_minor", 5, 6),
        ("Business", "provider_calls", 250, 251),
        ("Business", "budget_usd_minor", 1250, 1251),
        ("Business", "max_price_usd_minor", 5, 6),
    ),
)
def test_planversion_gate_rejects_wrong_exact_launch_sms_value(
    tmp_path: Path,
    tier: str,
    field: str,
    expected_value: int,
    wrong_value: int,
) -> None:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    adr_path = (
        adr_root
        / "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md"
    )
    record = adr_path.read_text(encoding="utf-8")
    lines = record.splitlines()
    row_index = next(
        index for index, line in enumerate(lines) if line.startswith(f'  "{tier}":')
    )
    expected_fragment = f'"{field}": {expected_value}'
    assert expected_fragment in lines[row_index]
    lines[row_index] = lines[row_index].replace(
        expected_fragment,
        f'"{field}": {wrong_value}',
        1,
    )
    adr_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    with pytest.raises(AssertionError, match="exact launch SMS mapping"):
        assert_adr_inventory(adr_root)


@pytest.mark.parametrize(
    ("tier", "field", "integer_token", "wrong_type_token"),
    (
        ("Trial", "provider_calls", "0", "false"),
        ("Trial", "budget_usd_minor", "0", "false"),
        ("Trial", "max_price_usd_minor", "0", "false"),
        ("Starter", "provider_calls", "0", "false"),
        ("Starter", "budget_usd_minor", "0", "false"),
        ("Starter", "max_price_usd_minor", "0", "false"),
        ("Farm", "provider_calls", "10", "10.0"),
        ("Farm", "budget_usd_minor", "50", "50.0"),
        ("Farm", "max_price_usd_minor", "5", "5.0"),
        ("Pro", "provider_calls", "50", "50.0"),
        ("Pro", "budget_usd_minor", "250", "250.0"),
        ("Pro", "max_price_usd_minor", "5", "5.0"),
        ("Business", "provider_calls", "250", "250.0"),
        ("Business", "budget_usd_minor", "1250", "1250.0"),
        ("Business", "max_price_usd_minor", "5", "5.0"),
    ),
)
def test_planversion_gate_rejects_non_integer_exact_launch_sms_type(
    tmp_path: Path,
    tier: str,
    field: str,
    integer_token: str,
    wrong_type_token: str,
) -> None:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    adr_path = (
        adr_root
        / "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md"
    )
    record = adr_path.read_text(encoding="utf-8")
    lines = record.splitlines()
    row_index = next(
        index for index, line in enumerate(lines) if line.startswith(f'  "{tier}":')
    )
    integer_fragment = f'"{field}": {integer_token}'
    assert integer_fragment in lines[row_index]
    lines[row_index] = lines[row_index].replace(
        integer_fragment,
        f'"{field}": {wrong_type_token}',
        1,
    )
    adr_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    with pytest.raises(AssertionError, match="integer fields"):
        assert_adr_inventory(adr_root)


@pytest.mark.parametrize(
    "inverted_clause",
    (
        "A Phase 9 connector upgrade may change canonical metric identity.",
        "The first vendor connector may ship without rate-limit, retry, or cursor recovery tests.",
        "The first vendor connector may hide its compatibility level.",
        "Passing edge acceptance may substitute for vendor-connector acceptance.",
    ),
)
def test_vendor_connector_gate_rejects_round_nine_semantic_inversion(
    tmp_path: Path,
    inverted_clause: str,
) -> None:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    adr_path = adr_root / "ADR-009-edge-is-optional-and-customer-hosted.md"
    record = adr_path.read_text(encoding="utf-8")
    inverted_gate = record.replace(
        "\n## Non-goals",
        f"\n{inverted_clause}\n\n## Non-goals",
        1,
    )
    assert inverted_gate != record
    adr_path.write_text(inverted_gate, encoding="utf-8")

    with pytest.raises(AssertionError, match="normative implementation gate"):
        assert_adr_inventory(adr_root)


@pytest.mark.parametrize(
    ("filename", "inverted_clause"),
    (
        (
            "ADR-016-eventbridge-is-selective-sqs-is-durable.md",
            "Phase 3 may allow an IntegrationAccount to claim a tenant it does not own.",
        ),
        (
            "ADR-016-eventbridge-is-selective-sqs-is-durable.md",
            "Ingress credentials or mapping data may override IntegrationAccount tenant ownership.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "A validation failure before SendTextMessage may retain the reserved count slot.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "A failure before SendTextMessage may retain the reserved USD amount.",
        ),
        (
            "ADR-008-hardware-accuracy-remains-customer-vendor-owned.md",
            "No-data, stale, or query-error evaluation may open a water-condition incident without an explicit stale/offline rule.",
        ),
        (
            "ADR-008-hardware-accuracy-remains-customer-vendor-owned.md",
            "No-data, stale, or query-error evaluation may resolve an active water-condition incident without an explicit stale/offline rule.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Development or staging may use production SMS origination references, pools, or routes.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Phase 7C may omit BR and US SMS origination environment-isolation tests.",
        ),
        (
            "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
            "A BRL Stripe subscription may convert the SMS budget away from USD.",
        ),
        (
            "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
            "Entitlement evaluation or notification dispatch may perform synchronous FX calls.",
        ),
        (
            "ADR-007-influx-v2-dual-write-migration.md",
            "After durable queue acceptance, an Influx write failure may acknowledge and lose telemetry.",
        ),
        (
            "ADR-007-influx-v2-dual-write-migration.md",
            "Telemetry from a failed Influx write may be neither retryable nor recoverable from DLQ.",
        ),
        (
            "ADR-007-influx-v2-dual-write-migration.md",
            "Dual write may begin before recovery from an Influx outage is proven.",
        ),
        (
            "ADR-001-aws-iot-is-an-integration-adapter.md",
            "LimnoPulse may persist an AWS IoT generated private key in an integration record.",
        ),
        (
            "ADR-001-aws-iot-is-an-integration-adapter.md",
            "An AWS IoT generated private key may be returned after creation.",
        ),
        (
            "ADR-009-edge-is-optional-and-customer-hosted.md",
            "A selected vendor webhook path may accept events without signature verification.",
        ),
        (
            "ADR-009-edge-is-optional-and-customer-hosted.md",
            "Vendor webhook retries may bypass event dedupe or idempotence.",
        ),
    ),
)
def test_adr_gate_rejects_round_ten_semantic_inversion(
    tmp_path: Path,
    filename: str,
    inverted_clause: str,
) -> None:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    adr_path = adr_root / filename
    record = adr_path.read_text(encoding="utf-8")
    inverted_gate = record.replace(
        "\n## Non-goals",
        f"\n{inverted_clause}\n\n## Non-goals",
        1,
    )
    assert inverted_gate != record
    adr_path.write_text(inverted_gate, encoding="utf-8")

    with pytest.raises(AssertionError, match="normative implementation gate"):
        assert_adr_inventory(adr_root)


@pytest.mark.parametrize(
    ("filename", "inverted_clause"),
    (
        (
            "ADR-006-telemetry-has-three-timestamps.md",
            "Replayed old telemetry may refresh current health and mark a device online.",
        ),
        (
            "ADR-006-telemetry-has-three-timestamps.md",
            "Current health may ignore receive and ingest time and use old observed_at as a fresh heartbeat.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "SMS dispatch may skip the final active membership and recipient-authorization recheck.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "A recipient revoked after queueing may still receive the tenant SMS.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Phase 7C may use non-approved localized critical SMS templates.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Critical SMS may include tenant, asset, precise telemetry, phone, or command detail.",
        ),
        (
            "ADR-017-sns-is-provider-feedback-not-notification-service.md",
            "A definite no-charge SMS result may release the consumed call count.",
        ),
        (
            "ADR-017-sns-is-provider-feedback-not-notification-service.md",
            "A definite no-charge SMS result may retain the monetary reservation.",
        ),
        (
            "ADR-017-sns-is-provider-feedback-not-notification-service.md",
            "Final provider cost may skip actual-cost settlement or release of proven excess.",
        ),
        (
            "ADR-017-sns-is-provider-feedback-not-notification-service.md",
            "Missing final SMS feedback may release the conservative monetary reservation.",
        ),
        (
            "ADR-006-telemetry-has-three-timestamps.md",
            "Event-time alert windows may finalize without a completeness delay.",
        ),
        (
            "ADR-006-telemetry-has-three-timestamps.md",
            "Samples within supported lateness may produce different alert outcomes by arrival order.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "A definite throttled or temporary Push 429/5xx result may be dropped without retry.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "A definite throttled or temporary Push result may retry without bounds.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "SendTextMessage may omit transactional type, origination reference, protection configuration, or SMS configuration set.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "SendTextMessage may omit a bounded TTL or provider MaxPrice.",
        ),
        (
            "ADR-012-commands-use-a-separate-safety-plane.md",
            "An R2 command may dispatch without human approval.",
        ),
        (
            "ADR-012-commands-use-a-separate-safety-plane.md",
            "An R3 command may use a lower-risk path instead of prohibition or stricter policy.",
        ),
        (
            "ADR-012-commands-use-a-separate-safety-plane.md",
            "An R4 command may be exposed.",
        ),
        (
            "ADR-012-commands-use-a-separate-safety-plane.md",
            "Command risk class may ignore the current context and use only its definition.",
        ),
        (
            "ADR-016-eventbridge-is-selective-sqs-is-durable.md",
            "Phase 3 may resolve ingress ownership before authenticating the provider source.",
        ),
        (
            "ADR-016-eventbridge-is-selective-sqs-is-durable.md",
            "Phase 3 may accept missing, invalid, or mismatched provider credentials.",
        ),
        (
            "ADR-012-commands-use-a-separate-safety-plane.md",
            "An unmet physical postcondition may leave the command result successful.",
        ),
        (
            "ADR-012-commands-use-a-separate-safety-plane.md",
            "An unmet physical postcondition may omit physical_verification=not_confirmed or the operational incident.",
        ),
        (
            "ADR-003-device-component-and-temporal-deployment.md",
            "Concurrent relocations may both commit without an optimistic version conflict.",
        ),
        (
            "ADR-003-device-component-and-temporal-deployment.md",
            "A relocation race may violate non-overlap, current pointer, or immutable history.",
        ),
    ),
)
def test_adr_gate_rejects_round_eleven_semantic_inversion(
    tmp_path: Path,
    filename: str,
    inverted_clause: str,
) -> None:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    adr_path = adr_root / filename
    record = adr_path.read_text(encoding="utf-8")
    inverted_gate = record.replace(
        "\n## Non-goals",
        f"\n{inverted_clause}\n\n## Non-goals",
        1,
    )
    assert inverted_gate != record
    adr_path.write_text(inverted_gate, encoding="utf-8")

    with pytest.raises(AssertionError, match="normative implementation gate"):
        assert_adr_inventory(adr_root)


@pytest.mark.parametrize(
    ("filename", "inverted_clause"),
    (
        (
            "ADR-011-limnopulse-owns-notification-semantics.md",
            "An asset_context preview may include more than one site or asset label.",
        ),
        (
            "ADR-011-limnopulse-owns-notification-semantics.md",
            "An asset_context label may be unapproved or exposed without acknowledgement.",
        ),
        (
            "ADR-011-limnopulse-owns-notification-semantics.md",
            "Asset_context may include sensitive telemetry, location, personal data, or free-form operational content.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "A provider-returned updated FCM/GCM token may be ignored instead of conditionally persisted.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "A late provider-returned updated token may overwrite a newer client rotation.",
        ),
        (
            "ADR-017-sns-is-provider-feedback-not-notification-service.md",
            "Malformed or unrelated SNS feedback already in SQS may retry without bounds and bypass the consumer DLQ.",
        ),
        (
            "ADR-017-sns-is-provider-feedback-not-notification-service.md",
            "Malformed or unrelated SNS feedback may fail without consumer observability.",
        ),
        (
            "ADR-017-sns-is-provider-feedback-not-notification-service.md",
            "The SNS subscription delivery-failure DLQ may substitute for consumer redrive and its poison-message DLQ.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Brazil may launch without in-app destination management or opt-out.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Brazil may omit disclosure that the displayed origin is unstable.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Brazil may promise replies or a fixed originating number.",
        ),
    ),
)
def test_adr_gate_rejects_round_twelve_semantic_inversion(
    tmp_path: Path,
    filename: str,
    inverted_clause: str,
) -> None:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    adr_path = adr_root / filename
    record = adr_path.read_text(encoding="utf-8")
    inverted_gate = record.replace(
        "\n## Non-goals",
        f"\n{inverted_clause}\n\n## Non-goals",
        1,
    )
    assert inverted_gate != record
    adr_path.write_text(inverted_gate, encoding="utf-8")

    with pytest.raises(AssertionError, match="normative implementation gate"):
        assert_adr_inventory(adr_root)


@pytest.mark.parametrize(
    ("filename", "inverted_clause"),
    (
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Distinct logical SMS jobs may reserve the last count slot and USD budget using separate budget-period versions.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "A same-tenant SMS budget race may overspend or let more than one loser contact the provider.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Correct SMS reservation concurrency may depend on Redis.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "A current-country price above the PlanVersion cap may start SendTextMessage and consume the call count.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "The provider MaxPrice check may replace the pre-dispatch country-price guard.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "The Android/FCM kill switch may also stop iOS/APNs.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "The iOS/APNs kill switch may also stop Android/FCM or discard durable state.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Successful provider Push acceptance may acknowledge the user incident.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Successful provider Push acceptance may change or cancel escalation state.",
        ),
        (
            "ADR-003-device-component-and-temporal-deployment.md",
            "Probe replacement may mutate the old Component or ProbeProfile identity in place.",
        ),
        (
            "ADR-003-device-component-and-temporal-deployment.md",
            "Probe replacement may rewrite prior Deployment, telemetry, or calibration attribution to the new probe.",
        ),
        (
            "ADR-001-aws-iot-is-an-integration-adapter.md",
            "AWS IoT ingest may resolve tenant ownership from a tenant asserted in the payload.",
        ),
        (
            "ADR-001-aws-iot-is-an-integration-adapter.md",
            "A payload tenant from another tenant may override the authenticated source mapping.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "The public canonical Push platform may accept AWS GCM or APNS channel names.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "The adapter may map canonical android to APNS instead of AWS GCM.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "The adapter may map canonical ios to GCM instead of APNS or APNS_SANDBOX.",
        ),
    ),
)
def test_adr_gate_rejects_round_thirteen_semantic_inversion(
    tmp_path: Path,
    filename: str,
    inverted_clause: str,
) -> None:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    adr_path = adr_root / filename
    record = adr_path.read_text(encoding="utf-8")
    inverted_gate = record.replace(
        "\n## Non-goals",
        f"\n{inverted_clause}\n\n## Non-goals",
        1,
    )
    assert inverted_gate != record
    adr_path.write_text(inverted_gate, encoding="utf-8")

    with pytest.raises(AssertionError, match="normative implementation gate"):
        assert_adr_inventory(adr_root)


@pytest.mark.parametrize(
    ("filename", "inverted_clause"),
    (
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "SMS pre-dispatch may proceed when country configuration, route validation, origination reference, current price, or registration state is absent.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "SMS pre-dispatch may rely on stale country readiness evidence.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "ProtectConfiguration may allow countries beyond BR and US.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "The application country allowlist may substitute for a restrictive ProtectConfiguration policy.",
        ),
        (
            "ADR-017-sns-is-provider-feedback-not-notification-service.md",
            "SMS feedback may correlate an Attempt by destination phone number instead of provider message ID.",
        ),
        (
            "ADR-017-sns-is-provider-feedback-not-notification-service.md",
            "Provider message ID need not be the exclusive SMS feedback correlation key.",
        ),
        (
            "ADR-017-sns-is-provider-feedback-not-notification-service.md",
            "The feedback-consumer kill switch may discard queued events or evidence.",
        ),
        (
            "ADR-017-sns-is-provider-feedback-not-notification-service.md",
            "The SMS send-lane kill switch may substitute for an independent feedback-consumer kill switch.",
        ),
        (
            "ADR-011-limnopulse-owns-notification-semantics.md",
            "The same delivery ID may create duplicate logical Deliveries before queue dispatch.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "FCM service-account JSON or an APNs private key may be stored in OpenTofu state.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "OpenTofu may deploy raw Push credentials directly instead of secure post-provisioning or secret deployment.",
        ),
        (
            "ADR-005-canonical-telemetry-is-metric-based.md",
            "A schema-valid but implausible metric value may be classified valid instead of out_of_range.",
        ),
        (
            "ADR-005-canonical-telemetry-is-metric-based.md",
            "Plausible-range quality classification and provenance may occur after the canonical write.",
        ),
        (
            "ADR-001-aws-iot-is-an-integration-adapter.md",
            "Certificate rotation may disable the old AWS IoT identity before verifying the replacement can connect.",
        ),
        (
            "ADR-001-aws-iot-is-an-integration-adapter.md",
            "A failed replacement AWS IoT identity may leave the old certificate disabled.",
        ),
        (
            "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
            "An Enterprise PlanVersion may omit SMS count, budget, currency, max price, or overage fields.",
        ),
        (
            "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
            "Missing Enterprise SMS limits may inherit implicit or unlimited defaults.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "An unclaimed Push token may be registered without an authenticated tenant member.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Push registration may accept a missing, invalid, or mismatched principal.",
        ),
        (
            "ADR-011-limnopulse-owns-notification-semantics.md",
            "BeginAttempt may call the provider after the recipient disabled the channel preference while queued.",
        ),
        (
            "ADR-011-limnopulse-owns-notification-semantics.md",
            "BeginAttempt may infer preferences from destination or policy state instead of rechecking them independently.",
        ),
        (
            "ADR-011-limnopulse-owns-notification-semantics.md",
            "Token, phone, or provider-binding changes may rewrite a Delivery destination snapshot after fanout.",
        ),
        (
            "ADR-011-limnopulse-owns-notification-semantics.md",
            "A Delivery destination snapshot may remain mutable after creation.",
        ),
        (
            "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
            "An absent entitlement cache entry may be treated as active or default entitlement.",
        ),
        (
            "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
            "When durable entitlement lookup is unavailable, a cache miss may allow paid SMS or command action instead of conservative denial.",
        ),
        (
            "ADR-012-commands-use-a-separate-safety-plane.md",
            "Mutable command state may substitute for immutable command lifecycle audit events.",
        ),
        (
            "ADR-012-commands-use-a-separate-safety-plane.md",
            "Command request, approval, dispatch, or result may omit its immutable audit event.",
        ),
    ),
)
def test_adr_gate_rejects_round_fourteen_semantic_inversion(
    tmp_path: Path,
    filename: str,
    inverted_clause: str,
) -> None:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    adr_path = adr_root / filename
    record = adr_path.read_text(encoding="utf-8")
    inverted_gate = record.replace(
        "\n## Non-goals",
        f"\n{inverted_clause}\n\n## Non-goals",
        1,
    )
    assert inverted_gate != record
    adr_path.write_text(inverted_gate, encoding="utf-8")

    with pytest.raises(AssertionError, match="normative implementation gate"):
        assert_adr_inventory(adr_root)


@pytest.mark.parametrize(
    ("filename", "inverted_clause"),
    (
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "SMS readiness freshness may be subjective and need not define an objective expiry or authoritative evidence source.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Production readiness may use subjective freshness without an authoritative evidence source or objective expiry rule.",
        ),
        (
            "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
            "An Enterprise PlanVersion may omit notifications.sms.critical and inherit its default.",
        ),
        (
            "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
            "An Enterprise PlanVersion may omit notifications.sms.critical and inherit its value.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "A Push registration may accept an inactive tenant membership.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Push registration or refresh may accept an authenticated principal with inactive tenant membership.",
        ),
    ),
)
def test_adr_gate_rejects_round_fourteen_b_semantic_inversion(
    tmp_path: Path,
    filename: str,
    inverted_clause: str,
) -> None:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    adr_path = adr_root / filename
    record = adr_path.read_text(encoding="utf-8")
    inverted_gate = record.replace(
        "\n## Non-goals",
        f"\n{inverted_clause}\n\n## Non-goals",
        1,
    )
    assert inverted_gate != record
    adr_path.write_text(inverted_gate, encoding="utf-8")

    with pytest.raises(AssertionError, match="normative implementation gate"):
        assert_adr_inventory(adr_root)


def test_scheduler_gate_requires_lease_and_fencing_clause(tmp_path: Path) -> None:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    adr_path = adr_root / "ADR-016-eventbridge-is-selective-sqs-is-durable.md"
    record = adr_path.read_text(encoding="utf-8")
    required_clause = (
        "Because Scheduler is at-least-once, every selected target must remain "
        "leased and fenced; Scheduler verification must prove retry overlap with a "
        "slow invocation cannot let two workers act on the same work unit."
    )
    record_with_clause = record.replace(
        "\n## Non-goals",
        f"\n{required_clause}\n\n## Non-goals",
        1,
    )
    without_clause = record_with_clause.replace(required_clause, "")
    assert without_clause != record_with_clause
    adr_path.write_text(without_clause, encoding="utf-8")

    with pytest.raises(AssertionError, match="normative implementation gate"):
        assert_adr_inventory(adr_root)
