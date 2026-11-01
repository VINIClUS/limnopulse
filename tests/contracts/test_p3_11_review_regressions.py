"""P3-11 GREEN review regressions (new file, disclosed in the PR; the approved gate tests are
frozen). Each test pins one finding of the two diff reviews of the runbook and ADR paragraphs."""

import re
from pathlib import Path

import yaml

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


# --- #116 follow-ups from the P3-11 audit ---------------------------------------------------------

README = ROOT / "README.md"


def _readme_paragraphs(start: str) -> list[str]:
    text = README.read_text(encoding="utf-8")
    return [" ".join(p.split()) for p in text.split("\n\n") if p.strip().startswith(start)]


def test_readme_rollbacks_recreate_instead_of_restarting() -> None:
    """`docker compose restart` keeps a container's old environment, so a rollback that only
    restarts the API leaves the switch on."""
    text = " ".join(README.read_text(encoding="utf-8").split())
    assert "unset `INGEST_V2_ENABLED`" not in text
    assert "restart the API" not in text
    p2, p3 = _readme_paragraphs("**Rollback:**")
    assert not re.search(r"\band restart\b", p2)
    assert re.search(r"recreate", p2) and re.search(r"up -d", p2)
    assert re.search(r"set `INGEST_V2_ENABLED=false` and recreate the API", p3)
    assert re.search(r"`docker compose [^`]*up -d api`", p3)
    assert "](docs/p3-ingestion-rollout-runbook.md#rollback)" in p3
    # The architecture overview repeats both rollbacks; it must not keep the old instruction.
    architecture = " ".join((ROOT / "docs/architecture.md").read_text(encoding="utf-8").split())
    assert "unset `INGEST_V2_ENABLED`" not in architecture
    assert "restart the API" not in architecture
    assert "set `INGEST_V2_ENABLED=false` and recreate the API" in architecture
    assert "defaults and restart." not in architecture
    # GitHub's `#rollback` anchor needs a heading whose text is exactly "Rollback".
    assert re.search(r"^#+ Rollback\s*$", RUNBOOK.read_text(encoding="utf-8"), re.MULTILINE)


def test_runbook_grace_bullet_says_drain_and_wait_are_not_interpolated() -> None:
    raw = markdown_section(RUNBOOK.read_text(encoding="utf-8"), "Production environment")
    bullets = [" ".join(b.split()) for b in re.split(r"(?m)^- ", raw)]
    (bullet,) = [b for b in bullets if b.startswith("**`stop_grace_period`.**")]
    assert re.search(r"must exceed drain \+ wait \+ 5 s \(A47\)", bullet)
    assert re.search(r"not interpolated", bullet)
    assert re.search(r"no effect", bullet)
    for compose in ("compose.yaml", "compose.production.yaml"):
        services = yaml.safe_load((ROOT / compose).read_text(encoding="utf-8"))["services"]
        worker = services["telemetry-ingest-worker"]
        assert "env_file" not in worker, compose  # `.env` reaches the worker only by name
        environment = worker.get("environment") or {}
        if isinstance(environment, list):  # the `- NAME=value` form
            environment = dict(entry.partition("=")[::2] for entry in environment)
        for name in ("TELEMETRY_INGEST_DRAIN_SECONDS", "TELEMETRY_INGEST_WORKER_WAIT_SECONDS"):
            assert name in bullet, name
            assert name not in environment, (compose, name)  # the claim holds in both files


def test_runbook_says_a_rerun_after_a_partial_mark_gets_a_new_replay_id() -> None:
    """#116 P3-09: already on main since P3-11; pinned so it cannot regress."""
    text = _section("Quarantine reconciliation drill")
    assert re.search(r"After a partial mark.{0,300}new replay id", text)


# --- ADR-020 (S5): the ingest worker's restart policy, liveness and park-on-78 ---------------------

PLAN = ROOT / "docs/superpowers/plans/2026-10-30-limnopulse-v4-p3-ingress-integration.md"


def test_runbook_documents_the_adr_020_restart_liveness_and_park_rules() -> None:
    text = _section("Production environment")
    for pattern in (
        r"restarts `unless-stopped`",
        r"Every deploy passes `--profile ingestion`",
        r"`python -m limnopulse_api\.workers\.telemetry_ingest healthcheck`",
        r"45 \+ 10 \+ 28 \+ 28 = 111 s",
        r"SIGTERM disarms the watchdog",
        r"\*\*Park on 78\.\*\*",
        r"exits 78",
        r"`sleep infinity`",
        r"parks again after a reboot",
        r"`shutdown_call_budget_seconds\(settings\)`",
    ):
        assert re.search(pattern, text), pattern
    runbook = _runbook()
    for stale in ("SHUTDOWN_CALL_BUDGET_SECONDS", "on-failure:5", "(exit 2)", "with exit 2"):
        assert stale not in runbook, stale
    # A rollback that turns writes off leaves a parked worker, which a later deploy waits on.
    assert re.search(r"fails its health gate", _section("Rollback"))


def test_plan_marks_d7_and_d21_superseded_by_adr_020_and_keeps_their_text() -> None:
    text = PLAN.read_text(encoding="utf-8")
    (d7,) = [line for line in text.splitlines() if line.startswith("| D7 |")]
    (d21,) = [line for line in text.splitlines() if line.startswith("| D21 |")]
    assert "a bounded `on-failure` restart in production" in d7
    assert "The default deploy does not start it." in d21
    raw = markdown_section(text, "Audit amendments (r2)")
    (a49,) = [line for line in raw.splitlines() if line.startswith("| A49 |")]
    for pattern in (
        r"Superseded by ADR-020",
        r"D7's bounded `on-failure` restart",
        r"D21's rule that the default deploy does not start the worker",
        r"`unless-stopped`",
        r"exits 78",
        r"`127\.0\.0\.1:8086`",
        r"A47 shutdown budget .{0,40}unchanged",
    ):
        assert re.search(pattern, a49), pattern
