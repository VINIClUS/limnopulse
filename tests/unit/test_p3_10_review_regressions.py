"""P3-10 GREEN diff-review regressions (new file; the approved tests at f2e682e are unchanged).

- A failure while composing the worker exits non-zero and logs a category only.
- Bounded botocore retries on the worker clients and fail-fast timeouts on the API's SQS
  client, so neither the drain (A47) nor an admission request can hang on SQS.
- Outside local/test the worker refuses (exit 2) when its own AWS keys or InfluxDB write token
  are empty, instead of polling with no credentials while the API keeps answering 202.
- The production worker passes its tuning knobs through (except drain and wait, which A47 binds
  to the stop grace) and disables the instance-metadata credential lookup.
"""

import asyncio
from pathlib import Path
from typing import Any

import pytest
import yaml
from botocore.config import Config
from fastapi.testclient import TestClient

from limnopulse_api.core.config import Settings, get_settings
from limnopulse_api.main import create_app
from limnopulse_api.workers import telemetry_ingest
from limnopulse_api.workers.telemetry_ingest import build_worker, main

ROOT = Path(__file__).resolve().parents[2]
QUEUE_URL = "http://localhost:9324/000000000000/limnopulse-telemetry-ingress"
PROD_QUEUE_URL = "https://sqs.us-east-2.amazonaws.com/000000000000/limnopulse-telemetry-ingress"
ENVIRONMENT = (
    "APP_ENV",
    "AUTH_MODE",
    "TELEGRAM_WEBHOOK_ENABLED",
    "API_V2_ENABLED",
    "INGEST_V2_ENABLED",
    "TELEMETRY_V2_WRITE_ENABLED",
    "SQS_ENDPOINT_URL",
    "SQS_TELEMETRY_INGRESS_QUEUE_URL",
    "DYNAMODB_ENDPOINT_URL",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "INFLUXDB_TOKEN",
)


@pytest.fixture(autouse=True)
def _isolate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)
    for name in ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


class _Clients:
    def __init__(self) -> None:
        self.calls: dict[str, dict[str, Any]] = {}

    def client(self, service_name: str, **kwargs: Any) -> object:
        self.calls[service_name] = kwargs
        return object()

    def influx(self, **kwargs: Any) -> Any:
        class _Influx:
            def write_api(self, *args: Any, **kwargs: Any) -> object:
                return object()

        return _Influx()


@pytest.mark.parametrize("service_name", ["dynamodb", "sqs"])
def test_worker_clients_use_bounded_standard_retries(service_name: str) -> None:
    clients = _Clients()
    settings = Settings(
        _env_file=None,
        app_env="test",
        auth_mode="dev",
        telemetry_v2_write_enabled=True,
        sqs_telemetry_ingress_queue_url=QUEUE_URL,
    )

    build_worker(settings, client_factory=clients.client, influx_client_factory=clients.influx)

    config = clients.calls[service_name]["config"]
    assert config.retries["mode"] == "standard"
    assert 1 <= config.retries["max_attempts"] <= 3


def test_api_sqs_client_fails_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, dict[str, Any]] = {}

    class _Redis:
        async def aclose(self, close_connection_pool=None) -> None:
            return None

    class _Influx:
        def __init__(self, **kwargs: Any) -> None:
            pass

        def query_api(self) -> object:
            return object()

        def write_api(self, *args: Any, **kwargs: Any) -> object:
            return object()

        def close(self) -> None:
            pass

    def client(service_name: str, **kwargs: Any) -> object:
        calls[service_name] = kwargs
        return object()

    monkeypatch.setattr("limnopulse_api.main.boto3.client", client)
    monkeypatch.setattr("limnopulse_api.main.redis.from_url", lambda url: _Redis())
    monkeypatch.setattr("limnopulse_api.main.InfluxDBClient", _Influx)
    app = create_app(
        Settings(
            _env_file=None,
            app_env="test",
            auth_mode="dev",
            api_v2_enabled=True,
            telemetry_v2_write_enabled=True,
            ingest_v2_enabled=True,
            sqs_telemetry_ingress_queue_url=QUEUE_URL,
        )
    )

    with TestClient(app):
        pass

    config = calls["sqs"]["config"]
    assert isinstance(config, Config)
    assert 0 < config.connect_timeout <= 10
    assert 0 < config.read_timeout <= 15
    assert config.retries["mode"] == "standard"
    assert 1 <= config.retries["max_attempts"] <= 3


def test_composition_failure_exits_non_zero_with_a_category_only(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("TELEMETRY_V2_WRITE_ENABLED", "true")
    monkeypatch.setenv("SQS_TELEMETRY_INGRESS_QUEUE_URL", QUEUE_URL)

    def fail(settings: Settings, **kwargs: object) -> object:
        raise RuntimeError("sentinel-composition-detail")

    monkeypatch.setattr(telemetry_ingest, "build_worker", fail)

    with caplog.at_level("DEBUG"):
        code = main()

    assert code not in (0, None)
    assert "sentinel-composition-detail" not in caplog.text
    assert "RuntimeError" in caplog.text


def _prod_environment(monkeypatch: pytest.MonkeyPatch, **overrides: str) -> None:
    values = {
        "APP_ENV": "prod",
        "AUTH_MODE": "cognito",
        "TELEGRAM_WEBHOOK_ENABLED": "false",
        "TELEMETRY_V2_WRITE_ENABLED": "true",
        "SQS_TELEMETRY_INGRESS_QUEUE_URL": PROD_QUEUE_URL,
        "AWS_ACCESS_KEY_ID": "worker-key-id",
        "AWS_SECRET_ACCESS_KEY": "worker-secret",
        "INFLUXDB_TOKEN": "worker-write-token",
    }
    values.update(overrides)
    for name, value in values.items():
        monkeypatch.setenv(name, value)


class _Done:
    async def run(self, stop: asyncio.Event) -> None:
        return None


@pytest.mark.parametrize("empty", ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "INFLUXDB_TOKEN"])
def test_production_worker_refuses_empty_credentials(
    monkeypatch: pytest.MonkeyPatch, empty: str
) -> None:
    _prod_environment(monkeypatch, **{empty: ""})
    built: list[Settings] = []
    monkeypatch.setattr(
        telemetry_ingest, "build_worker", lambda settings, **kwargs: built.append(settings)
    )

    assert main() == 2
    assert built == []


def test_production_worker_with_its_credentials_starts(monkeypatch: pytest.MonkeyPatch) -> None:
    _prod_environment(monkeypatch)
    monkeypatch.setattr(telemetry_ingest, "build_worker", lambda settings, **kwargs: _Done())

    assert main() == 0


def test_production_worker_passes_tuning_knobs_except_drain_and_wait() -> None:
    compose = yaml.safe_load((ROOT / "compose.production.yaml").read_text(encoding="utf-8"))
    environment = compose["services"]["telemetry-ingest-worker"]["environment"]
    defaults = Settings(_env_file=None, app_env="test", auth_mode="dev")

    assert environment["AWS_EC2_METADATA_DISABLED"] == "true"
    for field in (
        "telemetry_ingest_worker_concurrency",
        "telemetry_ingest_worker_max_messages",
        "telemetry_ingest_worker_visibility_seconds",
        "telemetry_ingest_processing_deadline_seconds",
        "telemetry_ingest_retry_base_seconds",
        "telemetry_ingest_retry_cap_seconds",
    ):
        name = field.upper()
        assert environment[name] == f"${{{name}:-{getattr(defaults, field)}}}", name
    # A47 binds these two to the 75 s stop grace; they stay at their defaults.
    assert "TELEMETRY_INGEST_DRAIN_SECONDS" not in environment
    assert "TELEMETRY_INGEST_WORKER_WAIT_SECONDS" not in environment
