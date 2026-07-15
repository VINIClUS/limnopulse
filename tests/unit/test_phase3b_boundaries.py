from pathlib import Path

import yaml


ROOT = Path(__file__).parents[2]


def test_evaluator_has_no_dynamodb_scan_or_phase3c_clients() -> None:
    go_sources = "\n".join(
        path.read_text()
        for directory in (ROOT / "cmd", ROOT / "internal" / "alertevaluator")
        for path in directory.rglob("*.go")
    )

    assert ".Scan(" not in go_sources
    assert "dynamodb.ScanInput" not in go_sources
    assert "service/sqs" not in go_sources
    assert "service/ses" not in go_sources
    assert "api.telegram.org" not in go_sources.lower()
    assert "telegram-bot-api" not in go_sources.lower()


def test_compose_evaluator_is_manual_one_shot() -> None:
    compose = yaml.safe_load((ROOT / "compose.yaml").read_text())
    evaluator = compose["services"]["alert-evaluator"]

    assert evaluator["profiles"] == ["manual"]
    assert evaluator["command"] == ["run"]
    assert evaluator["restart"] == "no"
