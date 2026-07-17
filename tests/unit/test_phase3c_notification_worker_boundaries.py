from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _production_sources(directory: Path) -> str:
    return "\n".join(
        path.read_text(encoding="utf-8")
        for path in directory.rglob("*.go")
        if not path.name.endswith("_test.go")
    )


def test_worker_uses_sqs_visibility_for_retries_without_scan_or_sleep() -> None:
    sources = _production_sources(ROOT / "internal" / "notifications" / "worker")

    assert ".Scan(" not in sources
    assert "ScanInput" not in sources
    assert "time.Sleep" not in sources
    assert "ReceiveMessage" in sources
    assert "ChangeMessageVisibility" in sources
    assert "WaitTimeSeconds" in sources
    assert "MaxNumberOfMessages" in sources
    assert "RetryMaxAttempts = 1" in sources


def test_attempt_rows_and_worker_summary_do_not_duplicate_email_content_or_pii() -> None:
    attempts = (
        ROOT / "internal" / "notifications" / "worker" / "dynamo" / "attempts.go"
    ).read_text(encoding="utf-8")
    attempt_put = attempts.split('"entity_type": "notification_attempt"', 1)[1].split(
        "if err != nil", 1
    )[0]
    for forbidden in (
        "normalized_email",
        "subject",
        "html",
        "text",
        "bearer",
        "access_token",
    ):
        assert forbidden not in attempt_put.lower()

    runner = (
        ROOT / "internal" / "notifications" / "worker" / "runner.go"
    ).read_text(encoding="utf-8")
    summary = runner.split("type RunSummary struct", 1)[1].split(
        "type Runner struct", 1
    )[0]
    for forbidden in (
        "email",
        "recipient",
        "subject",
        "html",
        "body",
        "receipt",
        "queue_url",
    ):
        assert forbidden not in summary.lower()
