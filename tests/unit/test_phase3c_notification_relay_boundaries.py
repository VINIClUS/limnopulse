from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _production_go_sources(directory: Path) -> str:
    return "\n".join(
        path.read_text(encoding="utf-8")
        for path in directory.rglob("*.go")
        if not path.name.endswith("_test.go")
    )


def test_notification_relay_is_query_only_bounded_and_one_shot() -> None:
    relay_sources = _production_go_sources(ROOT / "internal" / "notifications" / "relay")
    command_source = (ROOT / "cmd" / "notifications" / "main.go").read_text(
        encoding="utf-8"
    )
    sources = relay_sources + "\n" + command_source

    assert ".Scan(" not in sources
    assert "ScanInput" not in sources
    assert "time.Sleep" not in sources
    assert "for {" not in sources
    assert "ReceiveMessage" not in sources
    assert "service/ses" not in sources
    assert "TransactItems" in sources
    assert "SendMessage" in sources


def test_notification_job_and_run_summary_fields_are_no_pii() -> None:
    job_source = (ROOT / "internal" / "notifications" / "job.go").read_text(
        encoding="utf-8"
    )
    envelope = job_source.split("type JobEnvelope struct", 1)[1].split("}\n", 1)[0]

    runner_source = (
        ROOT / "internal" / "notifications" / "relay" / "runner.go"
    ).read_text(encoding="utf-8")
    summary = runner_source.split("type RunSummary struct", 1)[1].split(
        "type Runner struct", 1
    )[0]

    for forbidden in (
        "email_address",
        "normalized_email",
        "recipient_id",
        "subject",
        "html",
        "message_body",
        "content_hash",
    ):
        assert forbidden not in envelope.lower()
        assert forbidden not in summary.lower()
