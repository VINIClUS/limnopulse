import glob
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
REQUIRED_ADR_GATE_PATTERNS = {
    "ADR-001-aws-iot-is-an-integration-adapter.md": (
        r"\bBefore any queued consumer acts, it must recheck the DeviceIntegration "
        r"lifecycle state;\s+after decommission, both queued command dispatch and "
        r"queued ingest must be fenced\b",
    ),
    "ADR-003-device-component-and-temporal-deployment.md": (
        r"\bPhase 1 must reject overlapping Deployment intervals for the same "
        r"Component while permitting adjacent half-open intervals where one ends "
        r"exactly when the next starts\b",
    ),
    "ADR-004-effective-capability-is-derived.md": (
        r"\bPhase 6 must prove identical, reordered, and replayed health evidence "
        r"produces deterministic Device and Component health transitions, keeping "
        r"health-derived effective-capability inputs stable\b",
    ),
    "ADR-006-telemetry-has-three-timestamps.md": (
        r"\bPhase 2 must detect negative or extreme clock skew and emit a quality "
        r"flag while preserving original event-time semantics for delayed, replayed, "
        r"and out-of-order observations\b",
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
    ),
    "ADR-011-limnopulse-owns-notification-semantics.md": (
        r"\bPhase 7A must restrict `asset_context` policy writes to owners and admins;\s+"
        r"each write must be revisioned and audited, and member or viewer updates "
        r"must be rejected\b",
        r"\bDetailed incident fetch must require fresh membership authorization\b",
        r"\bGeneric preview must use the exact localized `pt-BR` and `en-US` "
        r"templates\.\s+Its visible-payload allowlist must exclude tenant, site/asset, "
        r"location, precise telemetry, personal/phone, command, actuator, credential, "
        r"token, and other sensitive fields;\s+its data payload is limited to an "
        r"opaque incident/notification ID, authenticated deep link, version, and "
        r"minimal routing metadata\b",
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
    ),
    "ADR-017-sns-is-provider-feedback-not-notification-service.md": (
        r"\bPhase 7C must also prove least-privilege AWS End User Messaging publish "
        r"permission, an SQS queue policy restricted by `aws:SourceArn` to the SNS "
        r"topic, a subscription delivery-failure DLQ where appropriate, and "
        r"fixture-tested selection of SNS envelope or raw delivery\b",
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
        r"\bAn independent Push kill switch must stop Push while preserving durable "
        r"state and leaving email, Telegram, and SMS intact\b",
        r"\bA Push transport timeout after potential provider acceptance must become "
        r"ambiguous or unknown and must not be automatically retried or resent\b",
        r"\bEach SMS verification challenge must use a separate Attempt with digest, "
        r"TTL, attempt and rate limits, its own anti-abuse controls, and a separate "
        r"platform budget;\s+Phase 7C tests must prove it cannot share or bypass the "
        r"critical-escalation Attempt or budget\b",
    ),
}
FORBIDDEN_ADR_GATE_PATTERNS = {
    "ADR-001-aws-iot-is-an-integration-adapter.md": (
        r"\bqueued consumer may act without rechecking integration lifecycle state\b",
        r"\bdecommission may leave queued commands or ingest unfenced\b",
    ),
    "ADR-003-device-component-and-temporal-deployment.md": (
        r"\bPhase 1 may accept overlapping Deployment intervals for the same "
        r"Component\b",
        r"\bPhase 1 must reject adjacent half-open Deployment intervals\b",
    ),
    "ADR-004-effective-capability-is-derived.md": (
        r"\bidentical, reordered, or replayed health evidence may produce different "
        r"Device or Component health transitions\b",
    ),
    "ADR-006-telemetry-has-three-timestamps.md": (
        r"\bnegative or extreme clock skew may pass without a quality flag\b",
        r"\bdelayed, replayed, or out-of-order observations may lose their "
        r"event-time semantics\b",
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
    ),
    "ADR-015-automatic-cloud-control-is-deferred.md": (
        r"\bPhase 10 may approve automatic execution from a one-off policy simulation "
        r"without dry-run history over time\b",
    ),
    "ADR-016-eventbridge-is-selective-sqs-is-durable.md": (
        r"\bwithout leases\b",
        r"\bwithout fencing\b",
    ),
    "ADR-017-sns-is-provider-feedback-not-notification-service.md": (
        r"\bwithout least privilege\b",
        r"\bmay omit the aws:SourceArn restriction\b",
        r"\bmay omit (?:its|the) delivery-failure DLQ\b",
        r"\bneed not be fixture-tested\b",
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
    ),
    "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md": (
        r"\bStripe webhook ingress may return 2xx before durable queue acceptance\b",
        r"\btransient Stripe enqueue failure may return 2xx instead of 5xx\b",
        r"\bstale active entitlement cache entry may override a newer durable "
        r"restricted or suspended state\b",
        r"\bmid-request suspension may still allow SMS spend or command dispatch\b",
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
        assert "**Status:** Accepted" in record, filename
        required_gate_patterns = REQUIRED_ADR_GATE_PATTERNS.get(filename, ())
        if required_gate_patterns:
            implementation_gate = re.search(
                r"(?ms)^## Implementation gate\n\n(.*?)(?=^## Non-goals$)",
                record,
            )
            gate_body = implementation_gate.group(1) if implementation_gate else ""
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
    (
        "ADR-001-aws-iot-is-an-integration-adapter.md",
        "ADR-003-device-component-and-temporal-deployment.md",
        "ADR-004-effective-capability-is-derived.md",
        "ADR-006-telemetry-has-three-timestamps.md",
        "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
        "ADR-011-limnopulse-owns-notification-semantics.md",
        "ADR-012-commands-use-a-separate-safety-plane.md",
        "ADR-015-automatic-cloud-control-is-deferred.md",
        "ADR-016-eventbridge-is-selective-sqs-is-durable.md",
        "ADR-017-sns-is-provider-feedback-not-notification-service.md",
        "ADR-018-eum-push-and-sms-are-provider-adapters.md",
    ),
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
