from pathlib import Path

import yaml

from tests.support.local_workers import local_env

ROOT = Path(__file__).parents[2]


def test_evaluator_has_no_dynamodb_scan_or_phase3c_clients() -> None:
    go_sources = "\n".join(
        path.read_text()
        for directory in (
            ROOT / "cmd" / "alert-evaluator",
            ROOT / "internal" / "alertevaluator",
        )
        for path in directory.rglob("*.go")
    )

    assert ".Scan(" not in go_sources
    assert "dynamodb.ScanInput" not in go_sources
    assert "service/sqs" not in go_sources
    assert "service/ses" not in go_sources
    assert "api.telegram.org" not in go_sources.lower()
    assert "telegram-bot-api" not in go_sources.lower()


def test_local_evaluator_is_a_manual_one_shot_outside_compose() -> None:
    # ADR-020: a developer runs one evaluation by hand with scripts/dev/run_worker.sh;
    # production schedules it with a systemd timer. Compose never runs it.
    compose = yaml.safe_load((ROOT / "compose.yaml").read_text())
    assert "alert-evaluator" not in compose["services"]
    environment = local_env("alert-evaluator")
    assert environment["APP_ENV"] == "local"
    assert environment["DYNAMODB_ENDPOINT_URL"] == "http://127.0.0.1:8001"
    assert environment["INFLUXDB_URL"] == "http://127.0.0.1:8086"
    readme = (ROOT / "README.md").read_text()
    assert "scripts/dev/run_worker.sh alert-evaluator run" in readme
