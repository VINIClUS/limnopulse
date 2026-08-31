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
    "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md": (
        r"\bgrace\b",
        r"\brestricted\b",
        r"\bsuspended\b",
        r"\bno\s+(?:grace|restricted|suspended).*report monitoring\s+as\s+active",
        r"ingestion.*coverage.*stopped",
    ),
    "ADR-016-eventbridge-is-selective-sqs-is-durable.md": (
        r"\bScheduler\b",
        r"selected.*\brole\b.*target invocation",
        r"idempotent duplicate",
        r"\bretry\b",
        r"\bDLQ\b",
    ),
}


def inventory_rows(
    path: Path = ROOT / "docs/current-state.md",
) -> dict[str, tuple[str, tuple[str, ...]]]:
    rows: dict[str, tuple[str, tuple[str, ...]]] = {}
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
            rows[surface] = (cells[2].strip("`"), evidence_paths)
    return rows


def test_inventory_uses_the_approved_statuses() -> None:
    assert {
        surface: status for surface, (status, _) in inventory_rows().items()
    } == EXPECTED


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
        "ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md",
        "ADR-016-eventbridge-is-selective-sqs-is-durable.md",
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
