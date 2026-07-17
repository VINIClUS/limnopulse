from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_phase3c_operations_document_rollout_order_and_rollback_are_explicit() -> None:
    operations = (ROOT / "docs" / "notifications-phase-3c-a.md").read_text(encoding="utf-8")
    normalized = " ".join(operations.split())

    ordered_rollout = (
        "1. Create the DynamoDB GSI",
        "2. Deploy the evaluator writer",
        "3. Stop legacy evaluators",
        "4. Keep relay and worker off",
        "5. Backfill relay attributes",
        "6. Validate SES",
        "7. Start one worker",
        "8. Schedule the relay every 60 seconds",
        "9. Monitor backlog, unknown deliveries and all three DLQs",
    )
    positions = [normalized.index(step) for step in ordered_rollout]
    assert positions == sorted(positions)
    assert "`notifications backfill-relay`" in normalized
    assert "`notifications relay`" in normalized
    assert "`notifications worker`" in normalized
    assert "Never run an automatic DLQ consumer" in normalized
    assert "turn off the relay first" in normalized
    assert "preserve DynamoDB delivery and attempt rows" in normalized


def test_phase3c_operations_document_records_pii_and_scope_boundaries() -> None:
    operations = (ROOT / "docs" / "notifications-phase-3c-a.md").read_text(encoding="utf-8")
    normalized = " ".join(operations.split())
    telegram = (ROOT / "docs" / "backlog" / "phase-3c-b-telegram.md").read_text(encoding="utf-8")
    retention = (ROOT / "docs" / "backlog" / "notification-pii-retention.md").read_text(
        encoding="utf-8"
    )

    for phrase in (
        "DynamoDB is authoritative",
        "normalized email address is durable PII",
        "SES destination arrays are discarded",
        "No TTL is applied to delivery or attempt rows",
        "No public Delivery or Attempt API",
        "Telegram is not delivered in Phase 3C-A",
    ):
        assert phrase in normalized
    assert "Phase 3C-B" in telegram
    assert "retention" in retention.lower()
    assert "tenant offboarding" in retention.lower()
