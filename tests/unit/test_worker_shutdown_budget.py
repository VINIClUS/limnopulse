"""The A47 shutdown budget against botocore's real retry semantics.

botocore's `retries={"max_attempts": N}` counts retries: a built client makes N + 1 attempts in
all (its resolved `total_max_attempts`). These tests build real clients from the worker's own
configs and read the attempts and timeouts back from botocore, so a misread of that convention
cannot pass the way the fake-factory checks in `test_p3_10_review_regressions.py` could.
"""

import copy
import re
from pathlib import Path
from typing import Any

import boto3
import pytest
import yaml

from limnopulse_api.core.config import Settings, get_settings
from limnopulse_api.workers import telemetry_ingest
from limnopulse_api.workers.telemetry_ingest import build_worker
from limnopulse_api.workers.telemetry_ingest_loop import _POLL_GRACE_SECONDS

ROOT = Path(__file__).resolve().parents[2]
QUEUE_URL = "http://localhost:9324/000000000000/limnopulse-telemetry-ingress"


@pytest.fixture(autouse=True)
def _isolate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)
    for name in ("APP_ENV", "AWS_REGION", "SQS_ENDPOINT_URL", "DYNAMODB_ENDPOINT_URL"):
        monkeypatch.delenv(name, raising=False)
    for name in ("TELEMETRY_INGEST_DRAIN_SECONDS", "TELEMETRY_INGEST_WORKER_WAIT_SECONDS"):
        monkeypatch.delenv(name, raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _backoff_seconds(attempts: int) -> int:
    """botocore's standard mode sleeps at most min(2 ** (i - 1), 20) s before retry i."""
    return sum(min(2 ** (retry - 1), 20) for retry in range(1, attempts))


class _RealClients:
    """Builds each client with real botocore, from a copy of the worker's config (botocore rewrites
    a `max_attempts` retries dict in place), and records what botocore resolved."""

    def __init__(self) -> None:
        self.attempts: dict[str, int] = {}
        self.worst: dict[str, float] = {}

    def client(self, service_name: str, **kwargs: Any) -> object:
        kwargs["config"] = copy.deepcopy(kwargs["config"])
        kwargs.setdefault("aws_access_key_id", "test")
        kwargs.setdefault("aws_secret_access_key", "test")
        client = boto3.session.Session().client(service_name, **kwargs)
        config = client.meta.config
        attempts = config.retries["total_max_attempts"]
        self.attempts[service_name] = attempts
        self.worst[service_name] = attempts * (
            config.connect_timeout + config.read_timeout
        ) + _backoff_seconds(attempts)
        return client

    def influx(self, **kwargs: Any) -> Any:
        class _Influx:
            def write_api(self, *args: Any, **kwargs: Any) -> object:
                return object()

        return _Influx()


def _defaults() -> Settings:
    return Settings(
        _env_file=None,
        app_env="test",
        auth_mode="dev",
        telemetry_v2_write_enabled=True,
        sqs_telemetry_ingress_queue_url=QUEUE_URL,
    )


def _built() -> _RealClients:
    clients = _RealClients()
    build_worker(_defaults(), client_factory=clients.client, influx_client_factory=clients.influx)
    return clients


def _grace_seconds(path: Path) -> int:
    compose = yaml.safe_load(path.read_text(encoding="utf-8"))
    value = compose["services"]["telemetry-ingest-worker"]["stop_grace_period"]
    match = re.fullmatch(r"(?:(?P<m>[0-9]+)m)?(?:(?P<s>[0-9]+)s)?", value)
    assert match is not None and value
    return int(match.group("m") or 0) * 60 + int(match.group("s") or 0)


def test_worker_sqs_client_makes_a_single_attempt() -> None:
    """No botocore retry: the loop retries a failed poll, and a failed heartbeat, delete or
    visibility change only means a later redelivery."""
    assert _built().attempts["sqs"] == 1


def test_worker_dynamodb_client_makes_one_bounded_retry() -> None:
    assert _built().attempts["dynamodb"] == 2


@pytest.mark.parametrize("service_name", ["dynamodb", "sqs"])
def test_shutdown_call_budget_covers_the_real_worst_call(service_name: str) -> None:
    worst = _built().worst[service_name]

    assert worst <= telemetry_ingest.shutdown_call_budget_seconds(_defaults())


@pytest.mark.parametrize("name", ["compose.yaml", "compose.production.yaml"])
def test_stop_grace_covers_the_real_worst_in_flight_call(name: str) -> None:
    """A47, from the built clients rather than the budget function: the in-flight long poll, then
    the drain, then the longest boto3 call still in flight must all end before SIGKILL."""
    settings = _defaults()
    stop = (
        settings.telemetry_ingest_worker_wait_seconds
        + _POLL_GRACE_SECONDS
        + settings.telemetry_ingest_drain_seconds
        + max(_built().worst.values())
    )

    assert stop < _grace_seconds(ROOT / name)
