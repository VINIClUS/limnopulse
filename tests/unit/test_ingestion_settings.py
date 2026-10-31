"""P3-00 Settings: the ingestion fields, their bounds, the validator and redacted errors."""

from typing import Any

import pytest
from pydantic import ValidationError

from limnopulse_api.core.config import Settings

# field -> (default, lower bound, upper bound); bounds are None for non-integer fields.
INGESTION_FIELDS: dict[str, tuple[Any, int | None, int | None]] = {
    "ingest_v2_enabled": (False, None, None),
    "sqs_endpoint_url": (None, None, None),
    "sqs_telemetry_ingress_queue_url": ("", None, None),
    "sqs_telemetry_ingress_dlq_url": ("", None, None),
    "ingest_rate_limit_per_minute": (120, 1, 6_000),
    "ingest_rate_limit_burst": (20, 1, 1_000),
    "ingest_rate_limiter_max_keys": (10_000, 100, 1_000_000),
    "ingest_preauth_rate_limit_per_minute": (60, 1, 6_000),
    "ingest_preauth_global_per_minute": (600, 1, 60_000),
    "telemetry_ingest_processing_deadline_seconds": (45, 5, 900),
    "telemetry_ingest_worker_concurrency": (4, 1, 10),
    "telemetry_ingest_worker_max_messages": (10, 1, 10),
    "telemetry_ingest_worker_wait_seconds": (20, 0, 20),
    "telemetry_ingest_worker_visibility_seconds": (60, 10, 43_200),
    "telemetry_ingest_retry_base_seconds": (30, 1, 900),
    "telemetry_ingest_retry_cap_seconds": (900, 1, 43_200),
    "telemetry_ingest_drain_seconds": (30, 1, 300),
}
BOUNDED = [
    (name, low, high) for name, (_, low, high) in INGESTION_FIELDS.items() if low is not None
]
ENABLED = {
    "ingest_v2_enabled": True,
    "api_v2_enabled": True,
    "telemetry_v2_write_enabled": True,
    "sqs_telemetry_ingress_queue_url": "http://localhost:9324/000000000000/limnopulse-telemetry-ingress",
}


@pytest.fixture(autouse=True)
def _isolate_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in {
        *INGESTION_FIELDS,
        "api_v2_enabled",
        "telemetry_v2_write_enabled",
        "influxdb_token",
    }:
        monkeypatch.delenv(name.upper(), raising=False)


def _settings(**overrides: Any) -> Settings:
    return Settings(_env_file=None, app_env="test", auth_mode="dev", **overrides)


def test_ingestion_defaults_are_off() -> None:
    settings = _settings()

    for name, (default, _, _) in INGESTION_FIELDS.items():
        assert getattr(settings, name) == default, name
    assert settings.ingest_v2_enabled is False


@pytest.mark.parametrize(("name", "low", "high"), BOUNDED, ids=[item[0] for item in BOUNDED])
def test_integer_bounds_accept_the_edges_and_reject_beyond(name: str, low: int, high: int) -> None:
    extra: dict[str, Any] = {}
    if name == "telemetry_ingest_retry_cap_seconds":
        extra["telemetry_ingest_retry_base_seconds"] = 1
    if name == "telemetry_ingest_retry_base_seconds":
        extra["telemetry_ingest_retry_cap_seconds"] = 43_200

    assert getattr(_settings(**{name: low}, **extra), name) == low
    assert getattr(_settings(**{name: high}, **extra), name) == high
    for value in (low - 1, high + 1):
        with pytest.raises(ValidationError):
            _settings(**{name: value}, **extra)


@pytest.mark.parametrize(
    ("env_name", "raw", "field", "expected"),
    [
        (
            "SQS_TELEMETRY_INGRESS_QUEUE_URL",
            "http://q/ingress",
            "sqs_telemetry_ingress_queue_url",
            "http://q/ingress",
        ),
        ("SQS_ENDPOINT_URL", "http://localhost:9324", "sqs_endpoint_url", "http://localhost:9324"),
        (
            "SQS_TELEMETRY_INGRESS_DLQ_URL",
            "http://q/dlq",
            "sqs_telemetry_ingress_dlq_url",
            "http://q/dlq",
        ),
        ("INGEST_RATE_LIMIT_PER_MINUTE", "300", "ingest_rate_limit_per_minute", 300),
        ("TELEMETRY_INGEST_WORKER_CONCURRENCY", "2", "telemetry_ingest_worker_concurrency", 2),
    ],
)
def test_fields_read_their_exact_env_names(
    monkeypatch: pytest.MonkeyPatch, env_name: str, raw: str, field: str, expected: Any
) -> None:
    monkeypatch.setenv(env_name, raw)

    assert getattr(_settings(), field) == expected


def test_ingest_switch_reads_its_env_name(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INGEST_V2_ENABLED", "true")
    monkeypatch.setenv("API_V2_ENABLED", "true")
    monkeypatch.setenv("TELEMETRY_V2_WRITE_ENABLED", "true")
    monkeypatch.setenv("SQS_TELEMETRY_INGRESS_QUEUE_URL", "http://q/ingress")

    assert _settings().ingest_v2_enabled is True


def test_ingest_switch_with_every_requirement_is_accepted() -> None:
    settings = _settings(**ENABLED)

    assert settings.ingest_v2_enabled is True


@pytest.mark.parametrize(
    "missing",
    [
        {"api_v2_enabled": False},
        {"telemetry_v2_write_enabled": False},
        {"sqs_telemetry_ingress_queue_url": ""},
    ],
    ids=["api_v2", "telemetry_write", "queue_url"],
)
def test_ingest_switch_requires_its_dependencies(missing: dict[str, Any]) -> None:
    with pytest.raises(ValidationError) as exc_info:
        _settings(**{**ENABLED, **missing})

    assert "INGEST_V2_ENABLED" in str(exc_info.value)


def test_retry_base_must_not_exceed_the_cap() -> None:
    assert (
        _settings(
            telemetry_ingest_retry_base_seconds=900, telemetry_ingest_retry_cap_seconds=900
        ).telemetry_ingest_retry_cap_seconds
        == 900
    )

    with pytest.raises(ValidationError) as exc_info:
        _settings(telemetry_ingest_retry_base_seconds=600, telemetry_ingest_retry_cap_seconds=300)

    assert "TELEMETRY_INGEST_RETRY_BASE_SECONDS" in str(exc_info.value)


def test_ingest_settings_errors_never_echo_values(monkeypatch: pytest.MonkeyPatch) -> None:
    secret = "zq9x-queue-secret-must-not-leak"
    for field in Settings.model_fields:
        if field == "influxdb_token":
            break
        monkeypatch.delenv(field.upper(), raising=False)
    monkeypatch.setenv("INFLUXDB_TOKEN", secret)
    monkeypatch.setenv("INGEST_V2_ENABLED", "true")
    monkeypatch.setenv("SQS_TELEMETRY_INGRESS_DLQ_URL", f"https://sqs/{secret}")

    with pytest.raises(ValidationError) as exc_info:
        Settings(_env_file=None)

    assert "zq9x" not in str(exc_info.value)
    assert "INGEST_V2_ENABLED" in str(exc_info.value)
