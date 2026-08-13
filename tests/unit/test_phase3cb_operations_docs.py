from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_phase3cb_runbook_records_safe_rollout_backfill_and_rollback() -> None:
    operations = (ROOT / "docs" / "notifications-phase-3c-b.md").read_text(encoding="utf-8")
    normalized = " ".join(operations.split())

    ordered_rollout = (
        "1. Provision the dedicated Telegram queue",
        "2. Populate both secret containers",
        "3. Deploy the FastAPI binding and webhook code",
        "4. Register the Telegram webhook",
        "5. Deploy the evaluator, relay and worker binaries",
        "6. Run `notifications backfill-telegram`",
        "7. Start the Telegram worker",
        "8. Enable Telegram on the relay and evaluator",
        "9. Monitor the queue, DLQ, limiter and durable states",
    )
    positions = [normalized.index(step) for step in ordered_rollout]
    assert positions == sorted(positions)
    for phrase in (
        "never uses DynamoDB Scan",
        "TELEGRAM_DELIVERY_ENABLED=false",
        "Pause the evaluator scheduler before the final backfill",
        "turn off Telegram on the relay and evaluator first",
        "Never run an automatic Telegram DLQ consumer",
        "DynamoDB remains authoritative",
    ):
        assert phrase in normalized


def test_phase3cb_runbook_records_auth_pii_and_failure_boundaries() -> None:
    operations = (ROOT / "docs" / "notifications-phase-3c-b.md").read_text(encoding="utf-8")
    normalized = " ".join(operations.split())

    for phrase in (
        "X-Telegram-Bot-Api-Secret-Token",
        "one-time binding token",
        "Cognito access token",
        "does not change `telegram_enabled`",
        "chat ID never appears in the SQS job",
        "one provider call",
        "unknown",
        "link previews are disabled",
    ):
        assert phrase in normalized


def test_phase3cb_design_and_implementation_plan_are_persisted() -> None:
    design = (
        ROOT
        / "docs"
        / "superpowers"
        / "specs"
        / ("2026-08-13-limnopulse-phase-3c-b-telegram-design.md")
    )
    plan = (
        ROOT / "docs" / "superpowers" / "plans" / ("2026-08-13-limnopulse-phase-3c-b-telegram.md")
    )
    assert design.is_file()
    assert plan.is_file()
    assert "POST /webhooks/telegram" in design.read_text(encoding="utf-8")
    assert "Regression" in plan.read_text(encoding="utf-8")
