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


def _runbook() -> str:
    return " ".join(RUNBOOK.read_text(encoding="utf-8").split())


def test_runbook_least_privilege_matches_the_worker_grants() -> None:
    """The worker's DynamoDB grant is table-wide, and it may read queue attributes."""
    tofu = (ROOT / "infra/opentofu/ingestion_queues.tf").read_text(encoding="utf-8")
    assert '"sqs:GetQueueAttributes"' in tofu
    text = _section("Preconditions")
    assert "its DynamoDB rows" not in text
    assert re.search(r"read the attributes of the source queue", text)
    assert re.search(r"without a leading-key condition", text)
    assert "`<project>-<env>-telemetry-ingress-operator`" in text


def test_runbook_production_env_is_one_shared_file() -> None:
    text = _section("Production environment")
    assert "in its own `.env`" not in text
    assert re.search(r"One shared `\.env`", text)
    for name in ("API_V2_ENABLED=true", "TELEMETRY_V2_WRITE_ENABLED=true"):
        assert f"`{name}`" in text, name
    assert "on the worker and restart it" not in _section("Rollback")


def test_runbook_dlq_drill_states_what_sample_and_redrive_really_do() -> None:
    text = _section("DLQ drill")
    assert "Both subcommands are a dry run" not in text
    assert re.search(r"`sample` only reads and releases", text)
    assert re.search(r"`sent_at`", text) and "receive count, age," not in text
    assert "leave them in the DLQ" not in text
    assert re.search(r"`redrive` has no filter", text)


def test_runbook_partial_mark_and_rerun_exit_codes() -> None:
    text = _section("Quarantine reconciliation drill")
    assert "a conflict stopped the rest" not in text
    assert re.search(r"each row is marked on its own", text)
    assert re.search(r"`not_open`.{0,40}exits 3", text)


def test_runbook_deadline_says_one_place_and_the_four_day_source_limit() -> None:
    text = _section("Alarms and deadlines")
    assert "kept in two places" not in text
    assert re.search(r"one of two places", text)
    assert re.search(r"4 days in the source queue", text)
    assert "telemetry_dlq.py" in _runbook()


# --- Codex review on PR #122 ---------------------------------------------------------------------


def test_runbook_recreates_containers_instead_of_restarting_them() -> None:
    """Compose keeps a container's environment across `restart`; a switch change needs `up -d`."""
    text = _runbook()
    assert re.search(r"recreate, never just restart", text)
    assert re.search(r"compose\.production\.yaml up -d api", text)
    assert re.search(r"--profile ingestion up -d telemetry-ingest-worker", text)
    for name in ("Enablement order", "Rollback"):
        assert not re.search(r"\brestart (the|it)\b", _section(name)), name
    assert "restart both services" not in _section("Production environment")


def test_runbook_does_not_promise_replay_for_terminal_fences() -> None:
    """Decommissioned and revoked states are terminal, and a replay keeps the original ids."""
    integrations = (ROOT / "src/limnopulse_api/domain/integrations.py").read_text(encoding="utf-8")
    for terminal in (
        "DeviceIntegrationStatus.DECOMMISSIONED: frozenset",
        "IntegrationAccountStatus.REVOKED: frozenset",
    ):
        assert terminal not in integrations, terminal
    drill = _section("Quarantine reconciliation drill")
    assert "restore the DeviceIntegration, account or Device" not in drill
    assert re.search(r"Dismiss fenced rows", drill)
    assert re.search(r"fenced again", drill)
    assert "persisted as replayable" not in _section("Decommissioning")
