from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _production_sources(directory: Path) -> str:
    return "\n".join(
        path.read_text(encoding="utf-8")
        for path in directory.rglob("*.go")
        if not path.name.endswith("_test.go")
    )


def test_feedback_uses_exact_keys_without_scan_or_ses_address_payloads() -> None:
    feedback = _production_sources(ROOT / "internal" / "notifications" / "feedback")
    event = (ROOT / "internal" / "notifications" / "feedback" / "event.go").read_text(
        encoding="utf-8"
    )
    transaction = (
        ROOT / "internal" / "notifications" / "feedback" / "dynamo" / "transaction.go"
    ).read_text(encoding="utf-8")

    assert ".Scan(" not in feedback
    assert "ScanInput" not in feedback
    assert '`json:"destination"`' not in event
    assert '`json:"emailAddress"`' not in event
    for forbidden_log_call in ("log.Print", "slog.", "fmt.Print", "zap."):
        assert forbidden_log_call not in feedback

    suppression_put = transaction.split("func (store Store) suppressionPut", 1)[1]
    suppression_item = suppression_put.split("if err != nil", 1)[0]
    assert "DeliverabilityStorageKey(delivery.NormalizedEmail)" in suppression_put
    assert '"normalized_email"' not in suppression_item
    assert '"destination"' not in suppression_item
    assert '"email_address"' not in suppression_item


def test_feedback_metrics_are_bounded_and_worker_does_not_consume_dlqs() -> None:
    metrics = (ROOT / "internal" / "notifications" / "feedback" / "metrics.go").read_text(
        encoding="utf-8"
    )
    command = (ROOT / "cmd" / "notifications" / "worker_command.go").read_text(encoding="utf-8")
    config = (ROOT / "internal" / "notifications" / "worker" / "config" / "config.go").read_text(
        encoding="utf-8"
    )

    for forbidden in (
        "destination",
        "emailaddress",
        "normalizedemail",
        "messagebody",
        "receipthandle",
    ):
        assert forbidden not in metrics.lower()

    assert command.count("workersqs.Queue{") == 2
    assert "QueueURL: config.SQSQueueURL" in command
    assert "QueueURL: config.SQSFeedbackURL" in command
    assert "DLQ" not in command
    assert "DLQ" not in config
