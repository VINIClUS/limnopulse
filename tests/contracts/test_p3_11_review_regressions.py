"""P3-11 GREEN review regressions (new file, disclosed in the PR; the approved gate tests are
frozen). Each test pins one finding of the two diff reviews of the runbook and ADR paragraphs."""

import re
from pathlib import Path

from tests.contracts.test_p2_acceptance_gate import markdown_section

ROOT = Path(__file__).resolve().parents[2]
RUNBOOK = ROOT / "docs/p3-ingestion-rollout-runbook.md"
ADR_006 = ROOT / "docs/adr/ADR-006-telemetry-has-three-timestamps.md"


def _p3_paragraph(adr: Path) -> str:
    section = markdown_section(adr.read_text(encoding="utf-8"), "Implementation gate")
    (paragraph,) = [
        p for p in section.split("\n\n") if p.strip().startswith("**Phase split (P3).**")
    ]
    return " ".join(paragraph.split())


def _section(name: str) -> str:
    return " ".join(markdown_section(RUNBOOK.read_text(encoding="utf-8"), name).split())


def test_adr_006_does_not_claim_the_observed_mark_ignores_the_device_clock() -> None:
    """The observed marks come from `observed_at`; only a skewed clock is kept from pinning them."""
    text = _p3_paragraph(ADR_006)
    assert "never by a replay or a device clock" not in text
    assert re.search(r"skewed device clock never pins the observed mark", text)
    assert re.search(r"`received_at`", text)


def test_runbook_stop_grace_rule_is_the_a47_rule() -> None:
    """A47: the grace must exceed drain + wait + 5 s; the call budget is advisory headroom."""
    text = _section("Production environment")
    assert re.search(r"must exceed drain \+ wait \+ 5 s \(A47\)", text)
    assert "plus the in-flight call budget" not in text


def test_runbook_release_gate_lists_verify_influx() -> None:
    text = _section("Preconditions")
    for target in ("verify-python", "verify-go", "verify-influx", "verify-ingestion"):
        assert f"`make {target}`" in text, target


def test_runbook_suspended_device_is_its_own_bullet() -> None:
    raw = markdown_section(RUNBOOK.read_text(encoding="utf-8"), "Decommissioning")
    bullets = [" ".join(b.split()) for b in re.split(r"^- ", raw, flags=re.MULTILINE)[1:]]
    (account,) = [b for b in bullets if b.startswith("**IntegrationAccount.**")]
    assert "suspended" not in account
    (device,) = [b for b in bullets if b.startswith("**Suspended Device.**")]
    assert re.search(r"temporary fence", device) and re.search(r"DLQ", device)
