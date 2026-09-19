import glob
import json
import re
import shutil
from collections.abc import Callable
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
EXPECTED_EXECUTION_BASELINE = (
    "141e108a479c983ed3a5efcbe729a30a43ab0ecb"
)
EXPECTED_RUNTIME_BASELINE = (
    "4953601fbbc2f95c79e34439cce855307b7db2c8"
)
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
    "ADR-016-eventbridge-is-selective-sqs-is-durable.md": (
        "Phase 3; existing feedback; future EventBridge decision gate"
    ),
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
REQUIRED_ADR_DECISION_PATTERNS = {
    "ADR-001-aws-iot-is-an-integration-adapter.md": (
        r"\bThe core Device and Integration models are provider-neutral\.\s+"
        r"AWS IoT implements provisioning, authentication, trusted mapping, and "
        r"ingress behind an adapter;\s+its identifiers and credentials stay in "
        r"integration records\b"
    ),
}
EXPECTED_PLAN_SMS_LIMITS = {
    "Trial": {
        "critical": False,
        "monthly_messages_max": 0,
        "monthly_budget_minor": 0,
        "monthly_budget_currency": "USD",
        "max_price_per_message_minor": 0,
        "overage": False,
    },
    "Starter": {
        "critical": False,
        "monthly_messages_max": 0,
        "monthly_budget_minor": 0,
        "monthly_budget_currency": "USD",
        "max_price_per_message_minor": 0,
        "overage": False,
    },
    "Farm": {
        "critical": True,
        "monthly_messages_max": 10,
        "monthly_budget_minor": 50,
        "monthly_budget_currency": "USD",
        "max_price_per_message_minor": 5,
        "overage": False,
    },
    "Pro": {
        "critical": True,
        "monthly_messages_max": 50,
        "monthly_budget_minor": 250,
        "monthly_budget_currency": "USD",
        "max_price_per_message_minor": 5,
        "overage": False,
    },
    "Business": {
        "critical": True,
        "monthly_messages_max": 250,
        "monthly_budget_minor": 1250,
        "monthly_budget_currency": "USD",
        "max_price_per_message_minor": 5,
        "overage": False,
    },
}
PLAN_SMS_INTEGER_FIELDS = (
    "monthly_messages_max",
    "monthly_budget_minor",
    "max_price_per_message_minor",
)
PLAN_SMS_CURRENCY_FIELDS = ("monthly_budget_currency",)
PLAN_SMS_BOOLEAN_FIELDS = ("critical", "overage")


def reject_duplicate_json_keys(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    values: dict[str, object] = {}
    for key, value in pairs:
        if key in values:
            raise ValueError(f"duplicate JSON key: {key}")
        values[key] = value
    return values


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
    "ADR-019-redis-valkey-is-optional-acceleration.md": (),
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
    ),
    "ADR-019-redis-valkey-is-optional-acceleration.md": (),
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
        r"\ban absent entitlement cache entry may be treated as active or default "
        r"entitlement\b",
        r"\bwhen durable entitlement lookup is unavailable, a cache miss may allow "
        r"paid SMS or command action instead of conservative denial\b",
    ),
}

CODEX_REVIEW_GATE_CASES = (
    {
        "name": "BeginAttempt membership fence",
        "filename": "ADR-011-limnopulse-owns-notification-semantics.md",
        "required_pattern": (
            r"\bAt `BeginAttempt`, Phase 7A must recheck current active tenant "
            r"membership for every channel;\s+if a queued recipient lost membership "
            r"before the provider call, dispatch must deterministically perform no "
            r"send and incur no charge\b"
        ),
        "required_clause": (
            "At `BeginAttempt`, Phase 7A must recheck current active tenant "
            "membership for every channel; if a queued recipient lost membership "
            "before the provider call, dispatch must deterministically perform no "
            "send and incur no charge."
        ),
        "inverted_clause": (
            "At `BeginAttempt`, Phase 7A may skip current active tenant membership "
            "for a queued recipient."
        ),
        "forbidden_pattern": (
            r"\bAt `BeginAttempt`, Phase 7A may skip current active tenant membership "
            r"for a queued recipient\b"
        ),
    },
    {
        "name": "immutable content snapshot",
        "filename": "ADR-011-limnopulse-owns-notification-semantics.md",
        "required_pattern": (
            r"\bThe content snapshot selected at fanout must remain immutable "
            r"alongside the destination snapshot after Delivery creation;\s+later "
            r"template or content-revision changes must not rewrite queued or "
            r"historical Delivery evidence\b"
        ),
        "required_clause": (
            "The content snapshot selected at fanout must remain immutable alongside "
            "the destination snapshot after Delivery creation; later template or "
            "content-revision changes must not rewrite queued or historical Delivery "
            "evidence."
        ),
        "inverted_clause": (
            "The content snapshot may be resolved or overwritten at attempt time "
            "after fanout."
        ),
        "forbidden_pattern": (
            r"\bthe content snapshot may be resolved or overwritten at attempt time "
            r"after fanout\b"
        ),
    },
    {
        "name": "SMS destination lifecycle authorization",
        "filename": "ADR-018-eum-push-and-sms-are-provider-adapters.md",
        "required_pattern": (
            r"\bEvery SMS destination create, verify, and delete request must "
            r"authenticate the current principal and verify current ACTIVE tenant "
            r"membership;\s+missing, invalid, mismatched, inactive, cross-user, or "
            r"cross-tenant requests must be rejected, even when the destination is "
            r"pending or unclaimed\b"
        ),
        "required_clause": (
            "Every SMS destination create, verify, and delete request must authenticate "
            "the current principal and verify current ACTIVE tenant membership; missing, "
            "invalid, mismatched, inactive, cross-user, or cross-tenant requests must "
            "be rejected, even when the destination is pending or unclaimed."
        ),
        "inverted_clause": (
            "SMS destination create, verify, and delete requests may skip current "
            "principal authentication or active tenant membership."
        ),
        "forbidden_pattern": (
            r"\bSMS destination create, verify, and delete requests may skip current "
            r"principal authentication or active tenant membership\b"
        ),
    },
    {
        "name": "transactional non-SMS quota counters",
        "filename": "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
        "required_pattern": (
            r"\bPhase 4 must enforce sites, devices, components, integrations, active "
            r"rules, and Push destinations with transactionally maintained counters;\s+"
            r"reserve and create must be atomic where possible, archive/delete decrements "
            r"idempotent, and boundary plus concurrent-create tests must prove no quota "
            r"oversubscription\b"
        ),
        "required_clause": (
            "Phase 4 must enforce sites, devices, components, integrations, active rules, "
            "and Push destinations with transactionally maintained counters; reserve and "
            "create must be atomic where possible, archive/delete decrements idempotent, "
            "and boundary plus concurrent-create tests must prove no quota oversubscription."
        ),
        "inverted_clause": (
            "Phase 4 may enforce non-SMS resource quotas with non-transactional "
            "check-then-create logic that permits concurrent oversubscription."
        ),
        "forbidden_pattern": (
            r"\bPhase 4 may enforce non-SMS resource quotas with non-transactional "
            r"check-then-create logic that permits concurrent oversubscription\b"
        ),
    },
    {
        "name": "provider SMS spend boundary",
        "filename": "ADR-018-eum-push-and-sms-are-provider-adapters.md",
        "required_pattern": (
            r"\bPhase 7C launch readiness must enable and exercise AWS account/enforced "
            r"provider spend limits and billing alarms as an outer boundary in addition "
            r"to tenant budgets, per-message caps, and storm controls\b"
        ),
        "required_clause": (
            "Phase 7C launch readiness must enable and exercise AWS account/enforced "
            "provider spend limits and billing alarms as an outer boundary in addition "
            "to tenant budgets, per-message caps, and storm controls."
        ),
        "inverted_clause": (
            "Phase 7C launch readiness may omit AWS account spend limits and billing "
            "alarms when tenant budgets pass."
        ),
        "forbidden_pattern": (
            r"\bPhase 7C launch readiness may omit AWS account spend limits and billing "
            r"alarms when tenant budgets pass\b"
        ),
    },
    {
        "name": "stable HTTP batch identity",
        "filename": "ADR-016-eventbridge-is-selective-sqs-is-durable.md",
        "required_pattern": (
            r"\bPhase 3 HTTP batch ingress must require an `Idempotency-Key` or stable "
            r"`source_event_id`;\s+if the durable SQS write succeeds but the HTTP "
            r"response is lost, retrying with that identity must not create a second "
            r"canonical observation\b"
        ),
        "required_clause": (
            "Phase 3 HTTP batch ingress must require an `Idempotency-Key` or stable "
            "`source_event_id`; if the durable SQS write succeeds but the HTTP response "
            "is lost, retrying with that identity must not create a second canonical "
            "observation."
        ),
        "inverted_clause": (
            "Phase 3 HTTP batch ingress may assign a new identity when a durable SQS "
            "write succeeds but the response is lost."
        ),
        "forbidden_pattern": (
            r"\bPhase 3 HTTP batch ingress may assign a new identity when a durable "
            r"SQS write succeeds but the response is lost\b"
        ),
    },
    {
        "name": "provider credential rotation rollback",
        "filename": "ADR-018-eum-push-and-sms-are-provider-adapters.md",
        "required_pattern": (
            r"\bPhase 7B must version, rotate, and roll back FCM service-account and "
            r"APNs credentials;\s+invalid or expired replacements must fail closed "
            r"without leaving a compromised credential active, and the credential "
            r"rotation/rollback runbooks and tests must be proven before launch\b"
        ),
        "required_clause": (
            "Phase 7B must version, rotate, and roll back FCM service-account and APNs "
            "credentials; invalid or expired replacements must fail closed without leaving "
            "a compromised credential active, and the credential rotation/rollback runbooks "
            "and tests must be proven before launch."
        ),
        "inverted_clause": (
            "Phase 7B may replace Push provider credentials without versioned rollback "
            "or invalid-credential tests."
        ),
        "forbidden_pattern": (
            r"\bPhase 7B may replace Push provider credentials without versioned rollback "
            r"or invalid-credential tests\b"
        ),
    },
    {
        "name": "Push destination lifecycle authorization",
        "filename": "ADR-018-eum-push-and-sms-are-provider-adapters.md",
        "required_pattern": (
            r"\bEvery Push destination revoke and delete request must authenticate "
            r"the current principal and verify current ACTIVE tenant membership;\s+"
            r"missing, invalid, mismatched, inactive, cross-user, or cross-tenant "
            r"requests must be rejected\b"
        ),
        "required_clause": (
            "Every Push destination revoke and delete request must authenticate "
            "the current principal and verify current ACTIVE tenant membership; "
            "missing, invalid, mismatched, inactive, cross-user, or cross-tenant "
            "requests must be rejected."
        ),
        "inverted_clause": (
            "Push destination revoke and delete requests may skip current principal "
            "authentication or active tenant membership."
        ),
        "forbidden_pattern": (
            r"\bPush destination revoke and delete requests may skip current principal "
            r"authentication or active tenant membership\b"
        ),
    },
    {
        "name": "durable command intent before dispatch",
        "filename": "ADR-012-commands-use-a-separate-safety-plane.md",
        "required_pattern": (
            r"\bPhase 8 must durably persist and claim command intent before provider "
            r"dispatch;\s+no provider call may begin until that durable claim succeeds\b"
        ),
        "required_clause": (
            "Phase 8 must durably persist and claim command intent before provider "
            "dispatch; no provider call may begin until that durable claim succeeds."
        ),
        "inverted_clause": (
            "Phase 8 may call the provider before persisting or durably claiming command "
            "intent."
        ),
        "forbidden_pattern": (
            r"\bPhase 8 may call the provider before persisting or durably claiming "
            r"command intent\b"
        ),
    },
    {
        "name": "unsupported notification locale handling",
        "filename": "ADR-011-limnopulse-owns-notification-semantics.md",
        "required_pattern": (
            r"\bUnsupported destination locales must be rejected or use only an explicit "
            r"versioned template fallback;\s+no implicit locale fallback is allowed\b"
        ),
        "required_clause": (
            "Unsupported destination locales must be rejected or use only an explicit "
            "versioned template fallback; no implicit locale fallback is allowed."
        ),
        "inverted_clause": (
            "Unsupported destination locales may use an implicit locale fallback."
        ),
        "forbidden_pattern": (
            r"\bUnsupported destination locales may use an implicit locale fallback\b"
        ),
    },
    {
        "name": "nested Push response failure parsing",
        "filename": "ADR-018-eum-push-and-sms-are-provider-adapters.md",
        "required_pattern": (
            r"\bPhase 7B adapter fixtures must prove that an overall provider `200` "
            r"containing a per-address permanent failure is parsed as a permanent "
            r"failure for that address and conditionally invalidates only the observed "
            r"destination version\b"
        ),
        "required_clause": (
            "Phase 7B adapter fixtures must prove that an overall provider `200` "
            "containing a per-address permanent failure is parsed as a permanent "
            "failure for that address and conditionally invalidates only the observed "
            "destination version."
        ),
        "inverted_clause": (
            "Phase 7B adapter fixtures may treat an overall provider `200` with a "
            "per-address permanent failure as success without invalidating the "
            "destination."
        ),
        "forbidden_pattern": (
            r"\bPhase 7B adapter fixtures may treat an overall provider `200` with a "
            r"per-address permanent failure as success without invalidating the "
            r"destination\b"
        ),
    },
    {
        "name": "Redis destination privacy",
        "filename": "ADR-019-redis-valkey-is-optional-acceleration.md",
        "required_pattern": (
            r"\bPhase 7A must prove that raw Push tokens, phone numbers, Telegram chat "
            r"IDs, and secrets are never used as Redis/Valkey keys or metric labels;\s+"
            r"cache-enabled and no-cache tests must preserve this privacy boundary\b"
        ),
        "required_clause": (
            "Phase 7A must prove that raw Push tokens, phone numbers, Telegram chat "
            "IDs, and secrets are never used as Redis/Valkey keys or metric labels; "
            "cache-enabled and no-cache tests must preserve this privacy boundary."
        ),
        "inverted_clause": (
            "Redis/Valkey cache keys may contain raw Push tokens, phone numbers, "
            "Telegram chat IDs, or secrets."
        ),
        "forbidden_pattern": (
            r"\bRedis/Valkey cache keys may contain raw Push tokens, phone numbers, "
            r"Telegram chat IDs, or secrets\b"
        ),
    },
    {
        "name": "billing owner/admin authorization",
        "filename": "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
        "required_pattern": (
            r"\bPhase 4 must require a tenant owner/admin role for Checkout, Customer "
            r"Portal, plan-change, and every billing mutation or session endpoint;\s+"
            r"ordinary members must be rejected and each decision audited\b"
        ),
        "required_clause": (
            "Phase 4 must require a tenant owner/admin role for Checkout, Customer "
            "Portal, plan-change, and every billing mutation or session endpoint; "
            "ordinary members must be rejected and each decision audited."
        ),
        "inverted_clause": (
            "Phase 4 may allow ordinary tenant members to call Checkout, Customer "
            "Portal, plan-change, or billing mutation/session endpoints."
        ),
        "forbidden_pattern": (
            r"\bPhase 4 may allow ordinary tenant members to call Checkout, Customer "
            r"Portal, plan-change, or billing mutation/session endpoints\b"
        ),
    },
    {
        "name": "tenant and event-family anti-storm isolation",
        "filename": "ADR-019-redis-valkey-is-optional-acceleration.md",
        "required_pattern": (
            r"\bAnti-storm windows must be enforced independently per tenant and event "
            r"family;\s+one tenant or event family's burst must not consume or suppress "
            r"another's allowance, and Redis/no-Redis tests must prove isolation\b"
        ),
        "required_clause": (
            "Anti-storm windows must be enforced independently per tenant and event "
            "family; one tenant or event family's burst must not consume or suppress "
            "another's allowance, and Redis/no-Redis tests must prove isolation."
        ),
        "inverted_clause": (
            "Anti-storm windows may use one global allowance shared across tenants and "
            "event families."
        ),
        "forbidden_pattern": (
            r"\bAnti-storm windows may use one global allowance shared across tenants "
            r"and event families\b"
        ),
    },
    {
        "name": "trusted telemetry ownership chain",
        "filename": "ADR-001-aws-iot-is-an-integration-adapter.md",
        "required_pattern": (
            r"\bPhase 5 ingest must resolve the complete source-to-tenant-to-site-to-"
            r"asset-to-deployment-to-component ownership chain from trusted authenticated "
            r"mapping;\s+every payload-supplied site, asset, deployment, or component ID "
            r"must be ignored or rejected and must never override that chain\b"
        ),
        "required_clause": (
            "Phase 5 ingest must resolve the complete source-to-tenant-to-site-to-asset-"
            "to-deployment-to-component ownership chain from trusted authenticated "
            "mapping; every payload-supplied site, asset, deployment, or component ID "
            "must be ignored or rejected and must never override that chain."
        ),
        "inverted_clause": (
            "Phase 5 ingest may accept payload-supplied site, asset, deployment, or "
            "component IDs instead of the trusted ownership chain."
        ),
        "forbidden_pattern": (
            r"\bPhase 5 ingest may accept payload-supplied site, asset, deployment, or "
            r"component IDs instead of the trusted ownership chain\b"
        ),
    },
    {
        "name": "Push token envelope encryption",
        "filename": "ADR-018-eum-push-and-sms-are-provider-adapters.md",
        "required_pattern": (
            r"\bBefore Phase 7B launch, every raw Push token must use application-level "
            r"envelope encryption through KMS or the Database Encryption SDK and must "
            r"never be returned after write;\s+server-side table encryption alone is "
            r"insufficient\b"
        ),
        "required_clause": (
            "Before Phase 7B launch, every raw Push token must use application-level "
            "envelope encryption through KMS or the Database Encryption SDK and must "
            "never be returned after write; server-side table encryption alone is "
            "insufficient."
        ),
        "inverted_clause": (
            "Push tokens may rely only on DynamoDB or other server-side table encryption "
            "after write."
        ),
        "forbidden_pattern": (
            r"\bPush tokens may rely only on DynamoDB or other server-side table "
            r"encryption after write\b"
        ),
    },
    {
        "name": "bounded HTTP ingress protections",
        "filename": "ADR-016-eventbridge-is-selective-sqs-is-durable.md",
        "required_pattern": (
            r"\bPhase 3 HTTPS ingress must enforce bounded request payload size, "
            r"per-IntegrationAccount rate limits, and entitlement limits before durable "
            r"enqueue;\s+oversized or over-rate requests must be rejected without "
            r"unbounded buffering or queue admission\b"
        ),
        "required_clause": (
            "Phase 3 HTTPS ingress must enforce bounded request payload size, "
            "per-IntegrationAccount rate limits, and entitlement limits before durable "
            "enqueue; oversized or over-rate requests must be rejected without "
            "unbounded buffering or queue admission."
        ),
        "inverted_clause": (
            "Phase 3 HTTPS ingress may accept oversized or over-rate requests into the "
            "durable queue without payload or entitlement bounds."
        ),
        "forbidden_pattern": (
            r"\bPhase 3 HTTPS ingress may accept oversized or over-rate requests into "
            r"the durable queue without payload or entitlement bounds\b"
        ),
    },
    {
        "name": "PlanVersion Stripe price resolution",
        "filename": "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
        "required_pattern": (
            r"\bPhase 4 Checkout must accept plan, interval, and currency plus allowlisted "
            r"success_path and cancel_path;\s+the "
            r"server must resolve an environment-specific Stripe Price ID from the "
            r"approved immutable PlanVersion catalog and reject client-supplied Price IDs\b"
        ),
        "required_clause": (
            "Phase 4 Checkout must accept plan, interval, and currency plus allowlisted "
            "success_path and cancel_path; the server must resolve an environment-specific "
            "Stripe Price ID from the approved immutable PlanVersion catalog and reject "
            "client-supplied Price IDs."
        ),
        "inverted_clause": (
            "Phase 4 Checkout may accept a client-supplied Stripe Price ID instead of "
            "resolving it from PlanVersion."
        ),
        "forbidden_pattern": (
            r"\bPhase 4 Checkout may accept a client-supplied Stripe Price ID instead "
            r"of resolving it from PlanVersion\b"
        ),
    },
    {
        "name": "APNs environment mismatch rejection",
        "filename": "ADR-018-eum-push-and-sms-are-provider-adapters.md",
        "required_pattern": (
            r"\bPhase 7B must fail closed when an iOS destination, token, credential, "
            r"or channel reference is tagged for the wrong APNs sandbox/production "
            r"environment;\s+APNS and APNS_SANDBOX references are non-interchangeable "
            r"and mismatch tests are required\b"
        ),
        "required_clause": (
            "Phase 7B must fail closed when an iOS destination, token, credential, or "
            "channel reference is tagged for the wrong APNs sandbox/production "
            "environment; APNS and APNS_SANDBOX references are non-interchangeable and "
            "mismatch tests are required."
        ),
        "inverted_clause": (
            "Phase 7B may send an iOS destination through APNS or APNS_SANDBOX despite "
            "a sandbox/production environment mismatch."
        ),
        "forbidden_pattern": (
            r"\bPhase 7B may send an iOS destination through APNS or APNS_SANDBOX "
            r"despite a sandbox/production environment mismatch\b"
        ),
    },
    {
        "name": "versioned destination lifecycle mutations",
        "filename": "ADR-018-eum-push-and-sms-are-provider-adapters.md",
        "required_pattern": (
            r"\bEvery Push and SMS destination lifecycle mutation—create, verify, "
            r"registration, refresh, "
            r"revoke, delete, invalidation, and opt-out—must use optimistic versioning/"
            r"conditional writes;\s+stale races must not resurrect or overwrite newer "
            r"state\b"
        ),
        "required_clause": (
            "Every Push and SMS destination lifecycle mutation—create, verify, "
            "registration, refresh, "
            "revoke, delete, invalidation, and opt-out—must use optimistic versioning/"
            "conditional writes; stale races must not resurrect or overwrite newer "
            "state."
        ),
        "inverted_clause": (
            "Destination lifecycle mutations may overwrite state without optimistic "
            "version checks or conditional writes."
        ),
        "forbidden_pattern": (
            r"\bDestination lifecycle mutations may overwrite state without optimistic "
            r"version checks or conditional writes\b"
        ),
    },
    {
        "name": "SMS verification entitlement",
        "filename": "ADR-018-eum-push-and-sms-are-provider-adapters.md",
        "required_pattern": (
            r"\bBefore any SMS verification challenge sends, Phase 7C must verify "
            r"tenant SMS eligibility and entitlement, including Trial/Starter/suspended "
            r"denial;\s+only an explicitly audited administrative import of already-verified "
            r"destinations may bypass that check\b"
        ),
        "required_clause": (
            "Before any SMS verification challenge sends, Phase 7C must verify tenant "
            "SMS eligibility and entitlement, including Trial/Starter/suspended denial; "
            "only an explicitly audited administrative import of already-verified "
            "destinations may bypass that check."
        ),
        "inverted_clause": (
            "SMS verification challenges may send for Trial, Starter, or suspended tenants "
            "without an entitlement check."
        ),
        "forbidden_pattern": (
            r"\bSMS verification challenges may send for Trial, Starter, or suspended "
            r"tenants without an entitlement check\b"
        ),
    },
    {
        "name": "exact GSM-7 extension counting",
        "filename": "ADR-018-eum-push-and-sms-are-provider-adapters.md",
        "required_pattern": (
            r"\bPhase 7C message preflight must count GSM-7 extension escapes and "
            r"concatenation thresholds exactly, enforce one-part GSM-7/UCS-2 limits, "
            r"reject multipart messages, and regression-test template changes\b"
        ),
        "required_clause": (
            "Phase 7C message preflight must count GSM-7 extension escapes and "
            "concatenation thresholds exactly, enforce one-part GSM-7/UCS-2 limits, "
            "reject multipart messages, and regression-test template changes."
        ),
        "inverted_clause": (
            "Phase 7C message preflight may count GSM-7 extension escapes as ordinary "
            "characters and allow multipart messages."
        ),
        "forbidden_pattern": (
            r"\bPhase 7C message preflight may count GSM-7 extension escapes as ordinary "
            r"characters and allow multipart messages\b"
        ),
    },
    {
        "name": "Stripe missed-webhook reconciliation",
        "filename": "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
        "required_pattern": (
            r"\bPhase 4 must periodically reconcile current Stripe subscription state "
            r"against internal BillingAccount and EntitlementSnapshot;\s+permanently "
            r"missed webhook/retry/DLQ events must be detected and stale entitlements "
            r"corrected\b"
        ),
        "required_clause": (
            "Phase 4 must periodically reconcile current Stripe subscription state "
            "against internal BillingAccount and EntitlementSnapshot; permanently "
            "missed webhook/retry/DLQ events must be detected and stale entitlements "
            "corrected."
        ),
        "inverted_clause": (
            "Phase 4 may rely on webhook retries and DLQs without periodic Stripe "
            "reconciliation."
        ),
        "forbidden_pattern": (
            r"\bPhase 4 may rely on webhook retries and DLQs without periodic Stripe "
            r"reconciliation\b"
        ),
    },
    {
        "name": "ambiguous Push no cross-channel fallback",
        "filename": "ADR-018-eum-push-and-sms-are-provider-adapters.md",
        "required_pattern": (
            r"\bAfter possible Push provider acceptance becomes ambiguous or unknown, "
            r"the orchestrator must not automatically fall back to SMS or another "
            r"channel unless a versioned policy explicitly accepts duplicate risk\b"
        ),
        "required_clause": (
            "After possible Push provider acceptance becomes ambiguous or unknown, the "
            "orchestrator must not automatically fall back to SMS or another channel "
            "unless a versioned policy explicitly accepts duplicate risk."
        ),
        "inverted_clause": (
            "After ambiguous Push acceptance, the orchestrator may automatically fall "
            "back to SMS without a versioned duplicate-risk policy."
        ),
        "forbidden_pattern": (
            r"\bAfter ambiguous Push acceptance, the orchestrator may automatically fall "
            r"back to SMS without a versioned duplicate-risk policy\b"
        ),
    },
    {
        "name": "Push and SMS dispatch queue DLQs",
        "filename": "ADR-018-eum-push-and-sms-are-provider-adapters.md",
        "required_pattern": (
            r"\bPhase 7B/7C launch readiness must provision separate Push and SMS "
            r"dispatch queues and DLQs with bounded poison-message redrive, queue-age/"
            r"DLQ alarms, and recovery tests;\s+feedback-consumer DLQs do not substitute\b"
        ),
        "required_clause": (
            "Phase 7B/7C launch readiness must provision separate Push and SMS dispatch "
            "queues and DLQs with bounded poison-message redrive, queue-age/DLQ alarms, "
            "and recovery tests; feedback-consumer DLQs do not substitute."
        ),
        "inverted_clause": (
            "Push and SMS dispatch queues may retry poison messages indefinitely without "
            "separate DLQs or recovery alarms."
        ),
        "forbidden_pattern": (
            r"\bPush and SMS dispatch queues may retry poison messages indefinitely "
            r"without separate DLQs or recovery alarms\b"
        ),
    },
    {
        "name": "bounded Stripe webhook body",
        "filename": "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
        "required_pattern": (
            r"\bPhase 4 Stripe webhook ingress must enforce a strict raw-body size "
            r"bound before signature verification and enqueue;\s+oversized bodies must "
            r"be rejected without unbounded buffering\b"
        ),
        "required_clause": (
            "Phase 4 Stripe webhook ingress must enforce a strict raw-body size bound "
            "before signature verification and enqueue; oversized bodies must be "
            "rejected without unbounded buffering."
        ),
        "inverted_clause": (
            "Phase 4 Stripe webhook ingress may buffer an unbounded raw body before "
            "signature verification."
        ),
        "forbidden_pattern": (
            r"\bPhase 4 Stripe webhook ingress may buffer an unbounded raw body before "
            r"signature verification\b"
        ),
    },
    {
        "name": "Stripe secret state exclusion and rollback",
        "filename": "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
        "required_pattern": (
            r"\bPhase 4 must ensure OpenTofu never writes Stripe secret or webhook secret "
            r"values/versions into state; configure them only through secure post-"
            r"provisioning or environment secret deployment, and prove independent Stripe "
            r"credential rotation and rollback\b"
        ),
        "required_clause": (
            "Phase 4 must ensure OpenTofu never writes Stripe secret or webhook secret "
            "values/versions into state; configure them only through secure post-"
            "provisioning or environment secret deployment, and prove independent Stripe "
            "credential rotation and rollback."
        ),
        "inverted_clause": (
            "Phase 4 may write Stripe secret or webhook secret values into OpenTofu state."
        ),
        "forbidden_pattern": (
            r"\bPhase 4 may write Stripe secret or webhook secret values into OpenTofu "
            r"state\b"
        ),
    },
    {
        "name": "Checkout request idempotency",
        "filename": "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
        "required_pattern": (
            r"\bPhase 4 Checkout must derive a request idempotency key from tenant and "
            r"request identity and prove lost-response retries resolve to one Checkout "
            r"Session/operation rather than creating a duplicate\b"
        ),
        "required_clause": (
            "Phase 4 Checkout must derive a request idempotency key from tenant and "
            "request identity and prove lost-response retries resolve to one Checkout "
            "Session/operation rather than creating a duplicate."
        ),
        "inverted_clause": (
            "Phase 4 Checkout may create duplicate sessions when a response is lost and "
            "the owner retries the request."
        ),
        "forbidden_pattern": (
            r"\bPhase 4 Checkout may create duplicate sessions when a response is lost "
            r"and the owner retries the request\b"
        ),
    },
    {
        "name": "destination lifecycle audit trail",
        "filename": "ADR-018-eum-push-and-sms-are-provider-adapters.md",
        "required_pattern": (
            r"\bEvery Push and SMS destination lifecycle mutation—create, verify, rotate, "
            r"invalidate, revoke, delete, and opt-out—and every cross-user token-claim "
            r"rejection must emit an immutable audit event\b"
        ),
        "required_clause": (
            "Every Push and SMS destination lifecycle mutation—create, verify, rotate, "
            "invalidate, revoke, delete, and opt-out—and every cross-user token-claim "
            "rejection must emit an immutable audit event."
        ),
        "inverted_clause": (
            "Destination lifecycle mutations may omit immutable audit events and cross-user "
            "token-claim rejection audit records."
        ),
        "forbidden_pattern": (
            r"\bDestination lifecycle mutations may omit immutable audit events and "
            r"cross-user token-claim rejection audit records\b"
        ),
    },
    {
        "name": "destination PII erasure",
        "filename": "ADR-018-eum-push-and-sms-are-provider-adapters.md",
        "required_pattern": (
            r"\bAfter account lifecycle deletion, raw Push tokens and phone numbers must "
            r"be erased; only non-reversible hashes and delivery evidence may remain when "
            r"policy/legal retention permits, and erasure tests must prove tombstoned "
            r"destinations retain no decryptable PII\b"
        ),
        "required_clause": (
            "After account lifecycle deletion, raw Push tokens and phone numbers must be "
            "erased; only non-reversible hashes and delivery evidence may remain when "
            "policy/legal retention permits, and erasure tests must prove tombstoned "
            "destinations retain no decryptable PII."
        ),
        "inverted_clause": (
            "Account deletion may retain decryptable raw Push tokens or phone numbers "
            "indefinitely."
        ),
        "forbidden_pattern": (
            r"\bAccount deletion may retain decryptable raw Push tokens or phone numbers "
            r"indefinitely\b"
        ),
    },
    {
        "name": "restricted Customer Portal plan changes",
        "filename": "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
        "required_pattern": (
            r"\bPhase 4 Customer Portal must permit only payment methods, invoice "
            r"viewing/download, cancel at period end, and resume where supported; "
            r"direct plan upgrades/downgrades must be disabled until versioned transition "
            r"controls are enabled through LimnoPulse APIs and tested\b"
        ),
        "required_clause": (
            "Phase 4 Customer Portal must permit only payment methods, invoice "
            "viewing/download, cancel at period end, and resume where supported; "
            "direct plan upgrades/downgrades must be disabled until versioned transition "
            "controls are enabled through LimnoPulse APIs and tested."
        ),
        "inverted_clause": (
            "Phase 4 Customer Portal may permit direct unrestricted plan upgrades or "
            "downgrades without LimnoPulse PlanVersion validation and downgrade preflight."
        ),
        "forbidden_pattern": (
            r"\bPhase 4 Customer Portal may permit direct unrestricted plan upgrades or "
            r"downgrades without LimnoPulse PlanVersion validation and downgrade preflight\b"
        ),
    },
    {
        "name": "per-recipient SMS storm window",
        "filename": "ADR-018-eum-push-and-sms-are-provider-adapters.md",
        "required_pattern": (
            r"\bPhase 7C must enforce durable SMS storm/rate windows independently per "
            r"recipient, tenant, and event family before provider dispatch; recipient-"
            r"scoped limits must prevent one recipient from receiving every family allowance\b"
        ),
        "required_clause": (
            "Phase 7C must enforce durable SMS storm/rate windows independently per "
            "recipient, tenant, and event family before provider dispatch; recipient-"
            "scoped limits must prevent one recipient from receiving every family allowance."
        ),
        "inverted_clause": (
            "Phase 7C may use only tenant and event-family storm windows and allow one "
            "recipient to receive every family allowance."
        ),
        "forbidden_pattern": (
            r"\bPhase 7C may use only tenant and event-family storm windows and allow one "
            r"recipient to receive every family allowance\b"
        ),
    },
)
for _case in CODEX_REVIEW_GATE_CASES:
    _filename = _case["filename"]
    REQUIRED_ADR_GATE_PATTERNS[_filename] += (_case["required_pattern"],)
    FORBIDDEN_ADR_GATE_PATTERNS[_filename] += (_case["forbidden_pattern"],)


def semantic_gate_inversions(filename: str, gate_body: str) -> tuple[str, ...]:
    compact_gate = re.sub(r"\s+", " ", gate_body)
    sentences = tuple(
        sentence.strip()
        for sentence in re.split(r"(?<=[.!?])\s+", compact_gate)
        if sentence.strip()
    )
    segments = tuple(
        segment.strip()
        for sentence in sentences
        for segment in re.split(r"\s*(?:;|\bwhile\b)\s*", sentence, flags=re.I)
        if segment.strip()
    )

    def contains(sentence: str, pattern: str) -> bool:
        return re.search(pattern, sentence, flags=re.IGNORECASE) is not None

    def modal_permits(sentence: str, predicate: str) -> bool:
        permission = (
            rf"\b(?:may|can)\s+"
            rf"(?!(?:\w+\s+){{0,3}}(?:not|never)\b)"
            rf"(?!under\s+no\s+circumstances\b)"
            rf"(?:\w+\s+){{0,3}}{predicate}\b"
        )
        scoped_no = rf"\bno\b[^;.!?]{{0,120}}{permission}"
        explicitly_allowed = (
            rf"\b(?:is|are)\s+(?:allowed|permitted)\s+to\s+"
            rf"(?:\w+\s+){{0,3}}{predicate}\b"
            rf"|{predicate}\b[^;.!?]{{0,80}}\b(?:is|are)\s+"
            rf"(?:allowed|permitted)\b"
        )
        return (
            contains(sentence, permission) and not contains(sentence, scoped_no)
        ) or contains(sentence, explicitly_allowed)

    inversions: list[str] = []
    for sentence in segments:
        if filename.endswith("ADR-018-eum-push-and-sms-are-provider-adapters.md"):
            if all(
                contains(sentence, pattern)
                for pattern in (
                    r"\b(?:SMS|production) readiness(?: freshness)?\b",
                    r"\bsubjective\b",
                    r"\bobjective (?:expiry|expiration)\b",
                    r"\bauthoritative (?:evidence )?source\b",
                )
            ) and (
                modal_permits(
                    sentence,
                    r"(?:be\s+subjective|use\s+subjective|accept(?:s|ed|ing)?|"
                    r"allow(?:s|ed|ing)?|rely(?:s|ied|ing)?)",
                )
                or contains(
                    sentence,
                    r"\bneed not\b[^;.!?]{0,80}\b(?:define|require|include)\b",
                )
                or contains(sentence, r"\b(?:is|are)\s+(?:allowed|permitted)\b")
            ):
                inversions.append("permissive subjective SMS readiness")
            if all(
                contains(sentence, pattern)
                for pattern in (
                    r"\bPush registration(?: or refresh)?\b",
                    r"\binactive tenant membership\b",
                    r"\b(?:accept(?:s|ed|ing)?|allow(?:s|ed|ing)?|"
                    r"permit(?:s|ted|ting)?)\b",
                )
            ) and (
                modal_permits(
                    sentence,
                    r"(?:accept(?:s|ed|ing)?|allow(?:s|ed|ing)?|"
                    r"permit(?:s|ted|ting)?)",
                )
                or contains(
                    sentence,
                    r"\bneed not\b[^;.!?]{0,80}\b(?:reject|deny|block|refuse)\b",
                )
                or contains(sentence, r"\b(?:is|are)\s+(?:allowed|permitted)\b")
                or contains(sentence, r"\b(?:permits|allows)\b")
            ):
                inversions.append("permissive inactive Push membership")
        enterprise_sms_flag = filename.endswith(
            "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md"
        ) and all(
            contains(sentence, pattern)
            for pattern in (
                r"\bEnterprise PlanVersion\b",
                r"\bomit(?:s|ted)?\b",
                r"notifications\.sms\.critical",
                r"\binherit(?:s|ed)?\b",
            )
        )
        if enterprise_sms_flag and (
            modal_permits(
                sentence,
                r"(?:omit(?:s|ted|ting)?|inherit(?:s|ed|ing)?|use(?:s|d|ing)?)",
            )
            or contains(sentence, r"\b(?:is|are)\s+(?:allowed|permitted)\b")
        ):
            inversions.append("permissive inherited Enterprise SMS critical flag")
    return tuple(inversions)


ENTERPRISE_PLAN_VERSION_REQUIREMENTS: tuple[tuple[str, str], ...] = (
    (
        "critical flag",
        r"(?m)^\s*-\s+`notifications\.sms\.critical`:\s+boolean flag\b",
    ),
    (
        "monthly message count",
        r"(?m)^\s*-\s+`notifications\.sms\.monthly_messages_max`:\s+"
        r"monthly SMS provider-call count\b",
    ),
    (
        "monthly budget amount",
        r"(?m)^\s*-\s+`notifications\.sms\.monthly_budget_minor`:\s+"
        r"monthly budget amount in integer minor units\b",
    ),
    (
        "monthly budget currency",
        r"(?m)^\s*-\s+`notifications\.sms\.monthly_budget_currency`:\s+"
        r"monthly budget currency\b",
    ),
    (
        "maximum price per message",
        r"(?m)^\s*-\s+`notifications\.sms\.max_price_per_message_minor`:\s+"
        r"maximum price per message in integer minor units\b",
    ),
    (
        "overage behavior",
        r"(?m)^\s*-\s+`notifications\.sms\.overage`:\s+overage behavior\b",
    ),
    (
        "no implicit or unlimited default",
        r"\bno missing field may inherit an implicit or unlimited default\b",
    ),
)


def copy_adr_fixture(tmp_path: Path) -> Path:
    adr_root = tmp_path / "adr"
    shutil.copytree(ROOT / "docs/adr", adr_root)
    return adr_root


def mutate_adr_record(
    adr_root: Path,
    filename: str,
    mutation: Callable[[str], str],
) -> None:
    adr_path = adr_root / filename
    record = adr_path.read_text(encoding="utf-8")
    mutated = mutation(record)
    assert mutated != record
    adr_path.write_text(mutated, encoding="utf-8")


def insert_gate_clause(record: str, clause: str) -> str:
    return record.replace(
        "\n## Non-goals",
        f"\n{clause}\n\n## Non-goals",
        1,
    )


def mutate_adr_gate(adr_root: Path, filename: str, clause: str) -> None:
    mutate_adr_record(
        adr_root,
        filename,
        lambda record: insert_gate_clause(record, clause),
    )


def remove_adr_fragment(adr_root: Path, filename: str, fragment: str) -> None:
    mutate_adr_record(
        adr_root,
        filename,
        lambda record: record.replace(fragment, "", 1),
    )


def replace_adr_fragment(
    adr_root: Path,
    filename: str,
    old: str,
    new: str,
) -> None:
    mutate_adr_record(
        adr_root,
        filename,
        lambda record: record.replace(old, new, 1),
    )


def mutate_planversion_row(
    adr_root: Path,
    tier: str,
    old_fragment: str,
    new_fragment: str,
) -> None:
    adr_path = (
        adr_root
        / "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md"
    )
    lines = adr_path.read_text(encoding="utf-8").splitlines()
    row_index = next(
        index for index, line in enumerate(lines) if line.startswith(f'  "{tier}":')
    )
    assert old_fragment in lines[row_index]
    lines[row_index] = lines[row_index].replace(old_fragment, new_fragment, 1)
    adr_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def assert_adr_rejected(
    adr_root: Path,
    match: str = "normative implementation gate",
) -> None:
    with pytest.raises(AssertionError, match=match):
        assert_adr_inventory(adr_root)


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


def assert_architecture_baseline_metadata(
    current_state_path: Path = ROOT / "docs/current-state.md",
    architecture_path: Path = ROOT / "docs/architecture.md",
) -> None:
    current_state = current_state_path.read_text(encoding="utf-8")
    assert f"**Execution baseline:** `main@{EXPECTED_EXECUTION_BASELINE}`" in (
        current_state
    )
    assert f"**Runtime baseline:** `{EXPECTED_RUNTIME_BASELINE}`" in current_state
    assert "`4953601` hardens the alert-evaluator container runtime and updates Go dependencies" in current_state
    assert "documentation and tests after that baseline do not change runtime behavior" in current_state

    architecture = architecture_path.read_text(encoding="utf-8")
    assert "**Version:** 1.4" in architecture
    assert "**Updated:** 2026-08-25" in architecture
    for link in (
        "[Current-state inventory](current-state.md)",
        "[Platform redesign technical specification V4](superpowers/specs/"
        "2026-08-16-limnopulse-platform-redesign-tech-spec-v4.md)",
        "[V4 parallel execution design](superpowers/specs/"
        "2026-08-25-limnopulse-v4-parallel-execution-design.md)",
    ):
        assert link in architecture


def test_architecture_baseline_metadata_is_pinned() -> None:
    assert_architecture_baseline_metadata()


@pytest.mark.parametrize(
    ("relative_path", "old", "new"),
    (
        (
            "docs/current-state.md",
            f"**Runtime baseline:** `{EXPECTED_RUNTIME_BASELINE}`",
            "**Runtime baseline:** `ce46b47fd646de762098a632b12e02d482c66485`",
        ),
        ("docs/architecture.md", "**Version:** 1.4", "**Version:** 1.3"),
        (
            "docs/architecture.md",
            "[Current-state inventory](current-state.md)",
            "Current-state inventory",
        ),
    ),
)
def test_architecture_baseline_metadata_rejects_mutation(
    tmp_path: Path,
    relative_path: str,
    old: str,
    new: str,
) -> None:
    current_state_path = tmp_path / "current-state.md"
    architecture_path = tmp_path / "architecture.md"
    shutil.copy(ROOT / "docs/current-state.md", current_state_path)
    shutil.copy(ROOT / "docs/architecture.md", architecture_path)
    target_path = (
        current_state_path
        if relative_path.endswith("current-state.md")
        else architecture_path
    )
    record = target_path.read_text(encoding="utf-8")
    mutated = record.replace(old, new, 1)
    assert mutated != record
    target_path.write_text(mutated, encoding="utf-8")

    with pytest.raises(AssertionError):
        assert_architecture_baseline_metadata(current_state_path, architecture_path)


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
        decision = re.search(
            r"(?ms)^## Decision\n\n(.*?)(?=^## Consequences$)",
            record,
        )
        decision_body = decision.group(1) if decision else ""
        required_decision_pattern = REQUIRED_ADR_DECISION_PATTERNS.get(filename)
        if required_decision_pattern:
            assert re.search(
                required_decision_pattern,
                decision_body,
                flags=re.IGNORECASE | re.DOTALL,
            ), f"ADR decision semantics missing in {filename}"
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
            if filename == (
                "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md"
            ):
                missing_enterprise_requirements = tuple(
                    label
                    for label, pattern in ENTERPRISE_PLAN_VERSION_REQUIREMENTS
                    if not re.search(
                        pattern,
                        gate_body,
                        flags=re.IGNORECASE | re.DOTALL,
                    )
                )
                assert not missing_enterprise_requirements, (
                    "Enterprise PlanVersion requirement missing: "
                    f"{', '.join(missing_enterprise_requirements)}"
                )
            forbidden_patterns = tuple(
                pattern
                for pattern in FORBIDDEN_ADR_GATE_PATTERNS.get(filename, ())
                if re.search(
                    pattern,
                    gate_body,
                    flags=re.IGNORECASE | re.DOTALL,
                )
            ) + semantic_gate_inversions(filename, gate_body)
            assert not forbidden_patterns, (
                f"normative implementation gate inversions in {filename}: "
                f"{forbidden_patterns}"
            )
            if filename == (
                "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md"
            ):
                serialized_mapping_matches = tuple(
                    re.finditer(
                        r"(?ms)Phase 4 must freeze and test this exact serialized launch "
                        r"SMS mapping; USD fields use integer minor units:\n\n"
                        r"```json\n(.*?)\n```",
                        gate_body,
                    )
                )
                assert serialized_mapping_matches, (
                    "exact launch SMS mapping is missing"
                )
                assert len(serialized_mapping_matches) == 1, (
                    "exact launch SMS mapping must appear exactly once"
                )
                serialized_mapping = serialized_mapping_matches[0]
                try:
                    plan_sms_limits = json.loads(
                        serialized_mapping.group(1),
                        object_pairs_hook=reject_duplicate_json_keys,
                    )
                except json.JSONDecodeError as error:
                    raise AssertionError(
                        "exact launch SMS mapping must be valid JSON"
                    ) from error
                except ValueError as error:
                    raise AssertionError(
                        "exact launch SMS mapping must not contain duplicate keys"
                    ) from error
                non_integer_fields = (
                    (("<root>", "<mapping>"),)
                    if not isinstance(plan_sms_limits, dict)
                    else tuple(
                        (tier, field)
                        for tier, expected_fields in EXPECTED_PLAN_SMS_LIMITS.items()
                        for field in PLAN_SMS_INTEGER_FIELDS
                        if not isinstance(plan_sms_limits.get(tier), dict)
                        or type(plan_sms_limits[tier].get(field)) is not int
                    )
                )
                assert not non_integer_fields, (
                    "exact launch SMS mapping integer fields must use JSON integers: "
                    f"{non_integer_fields}"
                )
                non_boolean_fields = tuple(
                    (tier, field)
                    for tier, expected_fields in EXPECTED_PLAN_SMS_LIMITS.items()
                    for field in PLAN_SMS_BOOLEAN_FIELDS
                    if not isinstance(plan_sms_limits.get(tier), dict)
                    or type(plan_sms_limits[tier].get(field)) is not bool
                )
                assert not non_boolean_fields, (
                    "exact launch SMS mapping boolean fields must use JSON booleans: "
                    f"{non_boolean_fields}"
                )
                non_string_fields = tuple(
                    (tier, field)
                    for tier, expected_fields in EXPECTED_PLAN_SMS_LIMITS.items()
                    for field in PLAN_SMS_CURRENCY_FIELDS
                    if not isinstance(plan_sms_limits.get(tier), dict)
                    or type(plan_sms_limits[tier].get(field)) is not str
                )
                assert not non_string_fields, (
                    "exact launch SMS mapping currency fields must use JSON strings: "
                    f"{non_string_fields}"
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


def test_adr_decision_rejects_provider_identity_inversion(tmp_path: Path) -> None:
    adr_root = copy_adr_fixture(tmp_path)
    replace_adr_fragment(
        adr_root,
        "ADR-001-aws-iot-is-an-integration-adapter.md",
        "The core Device and Integration models are provider-neutral. AWS IoT "
        "implements provisioning, authentication, trusted mapping, and ingress "
        "behind an adapter; its identifiers and credentials stay in integration "
        "records.",
        "AWS IoT Thing identity is the canonical Device domain identity.",
    )

    assert_adr_rejected(adr_root, match="ADR decision semantics")


def test_adr_record_requires_dedicated_accepted_status(tmp_path: Path) -> None:
    adr_root = copy_adr_fixture(tmp_path)
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
    adr_root = copy_adr_fixture(tmp_path)
    (adr_root / "ADR-999-stale.md").write_text(
        "# ADR-999 — Stale record\n\n**Status:** Accepted\n",
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="unexpected ADR files"):
        assert_adr_inventory(adr_root)


def test_adr_index_requires_exact_markdown_link_target(tmp_path: Path) -> None:
    adr_root = copy_adr_fixture(tmp_path)
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
    adr_root = copy_adr_fixture(tmp_path)
    adr_path = adr_root / EXPECTED_ADR_FILES[0]
    record = adr_path.read_text(encoding="utf-8")
    adr_path.write_text(
        record.replace("## Decision", "## Resolution", 1),
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="exact ADR headings"):
        assert_adr_inventory(adr_root)


def test_adr_index_requires_exact_entry_phase_mapping(tmp_path: Path) -> None:
    adr_root = copy_adr_fixture(tmp_path)
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
    adr_root = copy_adr_fixture(tmp_path)
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
    adr_root = copy_adr_fixture(tmp_path)
    adr_path = adr_root / filename
    record = adr_path.read_text(encoding="utf-8")
    without_gate_body = re.sub(
        r"(?ms)(^## Implementation gate\n\n).*?(?=^## Non-goals$)",
        r"\1",
        record,
    )
    assert without_gate_body != record
    adr_path.write_text(without_gate_body, encoding="utf-8")

    assert_adr_rejected(adr_root)


def test_billing_gate_forbids_false_active_monitoring(tmp_path: Path) -> None:
    adr_root = copy_adr_fixture(tmp_path)
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

    assert_adr_rejected(adr_root)


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
    adr_root = copy_adr_fixture(tmp_path)
    replace_adr_fragment(
        adr_root,
        "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
        required_clause,
        inverted_clause,
    )

    assert_adr_rejected(adr_root)


@pytest.mark.parametrize(
    ("requirement", "marker"),
    (
        ("critical flag", "`notifications.sms.critical`"),
        ("monthly message count", "`notifications.sms.monthly_messages_max`"),
        ("monthly budget amount", "`notifications.sms.monthly_budget_minor`"),
        ("monthly budget currency", "`notifications.sms.monthly_budget_currency`"),
        (
            "maximum price per message",
            "`notifications.sms.max_price_per_message_minor`",
        ),
        ("overage behavior", "overage behavior"),
        (
            "no implicit or unlimited default",
            "no missing field may inherit an implicit or unlimited default",
        ),
    ),
)
def test_enterprise_gate_rejects_each_missing_explicit_requirement(
    tmp_path: Path,
    requirement: str,
    marker: str,
) -> None:
    adr_root = copy_adr_fixture(tmp_path)
    filename = "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md"
    remove_adr_fragment(adr_root, filename, marker)
    assert_adr_rejected(
        adr_root,
        match=rf"Enterprise PlanVersion requirement missing: {re.escape(requirement)}",
    )


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
    adr_root = copy_adr_fixture(tmp_path)
    replace_adr_fragment(
        adr_root,
        "ADR-016-eventbridge-is-selective-sqs-is-durable.md",
        required_clause,
        inverted_clause,
    )

    assert_adr_rejected(adr_root)


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
    adr_root = copy_adr_fixture(tmp_path)
    mutate_adr_gate(adr_root, filename, inverted_clause)

    assert_adr_rejected(adr_root)


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
    adr_root = copy_adr_fixture(tmp_path)
    mutate_adr_gate(adr_root, filename, inverted_clause)

    assert_adr_rejected(adr_root)


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
    adr_root = copy_adr_fixture(tmp_path)
    mutate_adr_gate(adr_root, filename, inverted_clause)

    assert_adr_rejected(adr_root)


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
    adr_root = copy_adr_fixture(tmp_path)
    mutate_adr_gate(adr_root, filename, inverted_clause)

    assert_adr_rejected(adr_root)


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
    adr_root = copy_adr_fixture(tmp_path)
    mutate_adr_gate(adr_root, filename, inverted_clause)

    assert_adr_rejected(adr_root)


@pytest.mark.parametrize(
    ("tier", "field", "expected_value", "wrong_value"),
    (
        ("Trial", "monthly_messages_max", 0, 1),
        ("Trial", "monthly_budget_minor", 0, 1),
        ("Trial", "max_price_per_message_minor", 0, 1),
        ("Starter", "monthly_messages_max", 0, 1),
        ("Starter", "monthly_budget_minor", 0, 1),
        ("Starter", "max_price_per_message_minor", 0, 1),
        ("Farm", "monthly_messages_max", 10, 11),
        ("Farm", "monthly_budget_minor", 50, 51),
        ("Farm", "max_price_per_message_minor", 5, 6),
        ("Pro", "monthly_messages_max", 50, 51),
        ("Pro", "monthly_budget_minor", 250, 251),
        ("Pro", "max_price_per_message_minor", 5, 6),
        ("Business", "monthly_messages_max", 250, 251),
        ("Business", "monthly_budget_minor", 1250, 1251),
        ("Business", "max_price_per_message_minor", 5, 6),
    ),
)
def test_planversion_gate_rejects_wrong_exact_launch_sms_value(
    tmp_path: Path,
    tier: str,
    field: str,
    expected_value: int,
    wrong_value: int,
) -> None:
    adr_root = copy_adr_fixture(tmp_path)
    expected_fragment = f'"{field}": {expected_value}'
    mutate_planversion_row(
        adr_root,
        tier,
        expected_fragment,
        f'"{field}": {wrong_value}',
    )

    with pytest.raises(AssertionError, match="exact launch SMS mapping"):
        assert_adr_inventory(adr_root)


@pytest.mark.parametrize(
    ("tier", "field", "integer_token", "wrong_type_token"),
    (
        ("Trial", "monthly_messages_max", "0", "false"),
        ("Trial", "monthly_budget_minor", "0", "false"),
        ("Trial", "max_price_per_message_minor", "0", "false"),
        ("Starter", "monthly_messages_max", "0", "false"),
        ("Starter", "monthly_budget_minor", "0", "false"),
        ("Starter", "max_price_per_message_minor", "0", "false"),
        ("Farm", "monthly_messages_max", "10", "10.0"),
        ("Farm", "monthly_budget_minor", "50", "50.0"),
        ("Farm", "max_price_per_message_minor", "5", "5.0"),
        ("Pro", "monthly_messages_max", "50", "50.0"),
        ("Pro", "monthly_budget_minor", "250", "250.0"),
        ("Pro", "max_price_per_message_minor", "5", "5.0"),
        ("Business", "monthly_messages_max", "250", "250.0"),
        ("Business", "monthly_budget_minor", "1250", "1250.0"),
        ("Business", "max_price_per_message_minor", "5", "5.0"),
    ),
)
def test_planversion_gate_rejects_non_integer_exact_launch_sms_type(
    tmp_path: Path,
    tier: str,
    field: str,
    integer_token: str,
    wrong_type_token: str,
) -> None:
    adr_root = copy_adr_fixture(tmp_path)
    integer_fragment = f'"{field}": {integer_token}'
    mutate_planversion_row(
        adr_root,
        tier,
        integer_fragment,
        f'"{field}": {wrong_type_token}',
    )

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
    adr_root = copy_adr_fixture(tmp_path)
    mutate_adr_gate(
        adr_root,
        "ADR-009-edge-is-optional-and-customer-hosted.md",
        inverted_clause,
    )

    assert_adr_rejected(adr_root)


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
    adr_root = copy_adr_fixture(tmp_path)
    mutate_adr_gate(adr_root, filename, inverted_clause)

    assert_adr_rejected(adr_root)


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
    adr_root = copy_adr_fixture(tmp_path)
    mutate_adr_gate(adr_root, filename, inverted_clause)

    assert_adr_rejected(adr_root)


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
    adr_root = copy_adr_fixture(tmp_path)
    mutate_adr_gate(adr_root, filename, inverted_clause)

    assert_adr_rejected(adr_root)


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
    adr_root = copy_adr_fixture(tmp_path)
    mutate_adr_gate(adr_root, filename, inverted_clause)

    assert_adr_rejected(adr_root)


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
    adr_root = copy_adr_fixture(tmp_path)
    mutate_adr_gate(adr_root, filename, inverted_clause)

    assert_adr_rejected(adr_root)


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
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Inactive tenant membership may be accepted during Push registration.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Push registration can allow inactive tenant membership.",
        ),
    ),
)
def test_adr_gate_rejects_round_fourteen_b_semantic_inversion(
    tmp_path: Path,
    filename: str,
    inverted_clause: str,
) -> None:
    adr_root = copy_adr_fixture(tmp_path)
    mutate_adr_gate(adr_root, filename, inverted_clause)

    assert_adr_rejected(adr_root)


@pytest.mark.parametrize(
    ("filename", "safe_clause"),
    (
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "SMS readiness must reject subjective evidence without an authoritative evidence source or objective expiry.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Production readiness must not accept subjective evidence without an authoritative source or objective expiration.",
        ),
        (
            "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
            "An Enterprise PlanVersion must not omit notifications.sms.critical or use inherited values.",
        ),
        (
            "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
            "No Enterprise PlanVersion may omit notifications.sms.critical or inherit a default.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Push registration must not accept inactive tenant membership.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Push registration must never accept inactive tenant membership.",
        ),
    ),
)
def test_adr_gate_allows_round_fourteen_c_normative_prohibition(
    tmp_path: Path,
    filename: str,
    safe_clause: str,
) -> None:
    adr_root = copy_adr_fixture(tmp_path)
    mutate_adr_gate(adr_root, filename, safe_clause)

    assert_adr_inventory(adr_root)


@pytest.mark.parametrize(
    ("filename", "inverted_clause"),
    (
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "SMS readiness may use subjective evidence without an authoritative evidence source or objective expiry; logs must not expose tokens.",
        ),
        (
            "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
            "An Enterprise PlanVersion may omit notifications.sms.critical and inherit its default; operators must not bypass audit.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Push registration may accept inactive tenant membership; logs must not expose tokens.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Push registration may permit inactive tenant membership.",
        ),
    ),
)
def test_adr_gate_rejects_round_fourteen_e_predicate_local_permission(
    tmp_path: Path,
    filename: str,
    inverted_clause: str,
) -> None:
    adr_root = copy_adr_fixture(tmp_path)
    mutate_adr_gate(adr_root, filename, inverted_clause)

    assert_adr_rejected(adr_root)


@pytest.mark.parametrize(
    ("filename", "safe_clause"),
    (
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "SMS readiness may not accept subjective evidence without an authoritative evidence source or objective expiry.",
        ),
        (
            "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
            "An Enterprise PlanVersion may never omit notifications.sms.critical or inherit default values.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Push registration may not accept inactive tenant membership.",
        ),
    ),
)
def test_adr_gate_allows_round_fourteen_e_predicate_local_prohibition(
    tmp_path: Path,
    filename: str,
    safe_clause: str,
) -> None:
    adr_root = copy_adr_fixture(tmp_path)
    mutate_adr_gate(adr_root, filename, safe_clause)

    assert_adr_inventory(adr_root)


@pytest.mark.parametrize(
    ("filename", "inverted_clause"),
    (
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "SMS readiness is permitted to rely on subjective evidence without an authoritative evidence source or objective expiry.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Subjective SMS readiness without an authoritative evidence source or objective expiry is allowed.",
        ),
        (
            "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
            "An Enterprise PlanVersion is permitted to omit notifications.sms.critical and inherit its default.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Push registration permits inactive tenant membership.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Inactive tenant membership is allowed during Push registration.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "No audit may be skipped while SMS readiness may rely on subjective evidence without an authoritative evidence source or objective expiry.",
        ),
        (
            "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
            "No operator may bypass audit while an Enterprise PlanVersion may omit notifications.sms.critical and inherit its default.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "No log may expose tokens while Push registration may accept inactive tenant membership.",
        ),
    ),
)
def test_adr_gate_rejects_round_fourteen_f_common_permission_forms(
    tmp_path: Path,
    filename: str,
    inverted_clause: str,
) -> None:
    adr_root = copy_adr_fixture(tmp_path)
    mutate_adr_gate(adr_root, filename, inverted_clause)

    assert_adr_rejected(adr_root)


@pytest.mark.parametrize(
    ("filename", "safe_clause"),
    (
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "SMS readiness can under no circumstances rely on subjective evidence without an authoritative evidence source or objective expiry.",
        ),
        (
            "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
            "An Enterprise PlanVersion may under no circumstances omit notifications.sms.critical or inherit defaults.",
        ),
        (
            "ADR-018-eum-push-and-sms-are-provider-adapters.md",
            "Push registration may absolutely not accept inactive tenant membership.",
        ),
    ),
)
def test_adr_gate_allows_round_fourteen_f_emphatic_local_prohibition(
    tmp_path: Path,
    filename: str,
    safe_clause: str,
) -> None:
    adr_root = copy_adr_fixture(tmp_path)
    mutate_adr_gate(adr_root, filename, safe_clause)

    assert_adr_inventory(adr_root)


@pytest.mark.parametrize(
    "case",
    CODEX_REVIEW_GATE_CASES,
    ids=lambda case: case["name"],
)
def test_codex_review_gate_rejects_missing_requirement(
    tmp_path: Path,
    case: dict[str, str],
) -> None:
    adr_root = copy_adr_fixture(tmp_path)
    remove_adr_fragment(adr_root, case["filename"], case["required_clause"])

    assert_adr_rejected(adr_root, match="normative implementation gate markers missing")


@pytest.mark.parametrize(
    "case",
    CODEX_REVIEW_GATE_CASES,
    ids=lambda case: case["name"],
)
def test_codex_review_gate_rejects_inverted_requirement(
    tmp_path: Path,
    case: dict[str, str],
) -> None:
    adr_root = copy_adr_fixture(tmp_path)
    mutate_adr_gate(adr_root, case["filename"], case["inverted_clause"])

    assert_adr_rejected(adr_root, match="normative implementation gate inversions")


@pytest.mark.parametrize(
    ("tier", "old", "new"),
    tuple(
        (tier, '"overage": false', '"overage": true')
        for tier in EXPECTED_PLAN_SMS_LIMITS
    )
    + tuple(
        (tier, ', "overage": false', "")
        for tier in EXPECTED_PLAN_SMS_LIMITS
    ),
)
def test_planversion_gate_rejects_missing_or_enabled_sms_overage(
    tmp_path: Path,
    tier: str,
    old: str,
    new: str,
) -> None:
    adr_root = copy_adr_fixture(tmp_path)
    mutate_planversion_row(adr_root, tier, old, new)

    assert_adr_rejected(adr_root, match="exact launch SMS mapping")


@pytest.mark.parametrize(
    ("tier", "expected_value", "wrong_value"),
    (
        ("Trial", False, True),
        ("Starter", False, True),
        ("Farm", True, False),
        ("Pro", True, False),
        ("Business", True, False),
    ),
)
def test_planversion_gate_rejects_wrong_exact_launch_sms_critical_value(
    tmp_path: Path,
    tier: str,
    expected_value: bool,
    wrong_value: bool,
) -> None:
    adr_root = copy_adr_fixture(tmp_path)
    expected_fragment = f'"critical": {str(expected_value).lower()}'
    wrong_fragment = f'"critical": {str(wrong_value).lower()}'
    mutate_planversion_row(adr_root, tier, expected_fragment, wrong_fragment)

    with pytest.raises(AssertionError, match="exact launch SMS mapping"):
        assert_adr_inventory(adr_root)


@pytest.mark.parametrize(
    ("tier", "expected_token", "wrong_token"),
    tuple(
        (tier, str(expected).lower(), "0")
        for tier, expected in (
            ("Trial", False),
            ("Starter", False),
            ("Farm", True),
            ("Pro", True),
            ("Business", True),
        )
    ),
)
def test_planversion_gate_rejects_non_boolean_exact_launch_sms_critical_type(
    tmp_path: Path,
    tier: str,
    expected_token: str,
    wrong_token: str,
) -> None:
    adr_root = copy_adr_fixture(tmp_path)
    expected_fragment = f'"critical": {expected_token}'
    mutate_planversion_row(
        adr_root,
        tier,
        expected_fragment,
        f'"critical": {wrong_token}',
    )

    with pytest.raises(AssertionError, match="boolean fields"):
        assert_adr_inventory(adr_root)


def test_planversion_gate_rejects_duplicate_serialized_launch_sms_mapping(
    tmp_path: Path,
) -> None:
    adr_root = copy_adr_fixture(tmp_path)
    adr_path = (
        adr_root
        / "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md"
    )
    record = adr_path.read_text(encoding="utf-8")
    mapping_match = re.search(
        r"(?ms)Phase 4 must freeze and test this exact serialized launch SMS mapping; "
        r"USD fields use integer minor units:\n\n```json\n.*?\n```",
        record,
    )
    assert mapping_match
    duplicate_mapping = f"{mapping_match.group(0)}\n\n{mapping_match.group(0)}"
    duplicated_record = record.replace(mapping_match.group(0), duplicate_mapping, 1)
    assert duplicated_record != record
    adr_path.write_text(duplicated_record, encoding="utf-8")

    assert_adr_rejected(adr_root, match="exact launch SMS mapping must appear exactly once")


def test_planversion_gate_rejects_duplicate_serialized_launch_sms_key(
    tmp_path: Path,
) -> None:
    adr_root = copy_adr_fixture(tmp_path)
    adr_path = (
        adr_root
        / "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md"
    )
    record = adr_path.read_text(encoding="utf-8")
    trial_row = '  "Trial": {"critical": false, '
    duplicated_trial_row = '  "Trial": {"critical": false, "critical": false, '
    duplicated_record = record.replace(trial_row, duplicated_trial_row, 1)
    assert duplicated_record != record
    adr_path.write_text(duplicated_record, encoding="utf-8")

    assert_adr_rejected(adr_root, match="exact launch SMS mapping must not contain duplicate keys")


def test_scheduler_gate_requires_lease_and_fencing_clause(tmp_path: Path) -> None:
    adr_root = copy_adr_fixture(tmp_path)
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

    assert_adr_rejected(adr_root)
