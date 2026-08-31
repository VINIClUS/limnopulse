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


def inventory_rows(path: Path = ROOT / "docs/current-state.md") -> dict[str, str]:
    rows: dict[str, str] = {}
    text = path.read_text(encoding="utf-8")
    for line in text.splitlines():
        cells = [cell.strip() for cell in line.strip().split("|")]
        if len(cells) == 7 and cells[1] not in {"", "Surface", "---"}:
            surface = cells[1]
            assert surface not in rows, f"duplicate inventory surface: {surface}"
            rows[surface] = cells[2].strip("`")
    return rows


def test_inventory_uses_the_approved_statuses() -> None:
    assert inventory_rows() == EXPECTED


def test_inventory_rejects_duplicate_surface(tmp_path: Path) -> None:
    inventory_path = tmp_path / "current-state.md"
    inventory_path.write_text(
        (ROOT / "docs/current-state.md").read_text(encoding="utf-8")
        + "\n| FastAPI control plane | `implemented` | `duplicate.py` | Preserve. | Phase 0 |\n",
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="duplicate inventory surface"):
        inventory_rows(inventory_path)


def assert_adr_inventory(adr_root: Path) -> None:
    index_path = adr_root / "README.md"
    assert index_path.is_file(), "missing docs/adr/README.md"
    index = index_path.read_text(encoding="utf-8")
    link_targets = set(re.findall(r"\[[^\]]+\]\(([^)]+)\)", index))

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
        assert "**Status:** Accepted" in path.read_text(encoding="utf-8"), filename


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
