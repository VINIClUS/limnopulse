"""P3-10 worker entrypoint (D7): `build_worker(settings)` composes the P3-08 loop from injected
client factories without `create_app` or the optional cache, and `main()` refuses to start
(exit 2) unless v2 writes are on and the queue URL is set, then stops on SIGTERM/SIGINT."""

import asyncio
import math
import os
import signal
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from botocore.config import Config
from influxdb_client.client.write_api import SYNCHRONOUS, WriteOptions, WriteType
from limnopulse_api.workers.telemetry_ingest import build_worker, main

from limnopulse_api.adapters.dynamodb_deployments import DynamoDeploymentRepository
from limnopulse_api.adapters.dynamodb_devices_v2 import DynamoDeviceComponentRepository
from limnopulse_api.adapters.dynamodb_ingest_state import DynamoIngestStateRepository
from limnopulse_api.adapters.dynamodb_integrations import DynamoIntegrationRepository
from limnopulse_api.adapters.dynamodb_quarantine import DynamoQuarantineRepository
from limnopulse_api.adapters.dynamodb_sites_assets import DynamoSiteAssetRepository
from limnopulse_api.adapters.influxdb_v2_writer import InfluxObservationWriter
from limnopulse_api.adapters.sqs_ingestion import SqsIngressQueue
from limnopulse_api.adapters.telemetry_metrics_log import StructuredLogTelemetryMetrics
from limnopulse_api.core.config import Settings, get_settings
from limnopulse_api.services.ingest_processor import IngestMessageProcessor
from limnopulse_api.services.ingest_state import IngestStateService
from limnopulse_api.services.telemetry_normalizer import TelemetryNormalizer
from limnopulse_api.services.telemetry_pipeline import TelemetryPipeline
from limnopulse_api.services.telemetry_v2_writes import ObservationWriteService
from limnopulse_api.services.trusted_mapping import TrustedMappingService
from limnopulse_api.workers import telemetry_ingest
from limnopulse_api.workers.telemetry_ingest_loop import IngestWorkerConfig, IngestWorkerLoop

QUEUE_URL = "http://localhost:9324/000000000000/limnopulse-telemetry-ingress"
SQS_ENDPOINT = "http://localhost:9324"
DYNAMO_ENDPOINT = "http://localhost:8001"
DOMAIN_TABLE = "DomainUnderTest"
INFLUX = {"url": "http://influx.test:8086", "token": "worker-write-token", "org": "org_test"}
BUCKET = "raw_under_test"
WORKER_ENVIRONMENT = (
    "APP_ENV",
    "AUTH_MODE",
    "API_V2_ENABLED",
    "INGEST_V2_ENABLED",
    "TELEMETRY_V2_WRITE_ENABLED",
    "TELEMETRY_LEGACY_DUAL_WRITE_ENABLED",
    "SQS_ENDPOINT_URL",
    "SQS_TELEMETRY_INGRESS_QUEUE_URL",
    "SQS_TELEMETRY_INGRESS_DLQ_URL",
    "DYNAMODB_ENDPOINT_URL",
)
# A hard bound for every test that drives `main()`: the fakes never hang, but a missing signal
# handler or stop would otherwise wait forever.
HARD_BOUND_SECONDS = 10


@pytest.fixture(autouse=True)
def _isolate_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)  # no stray `.env`
    for name in WORKER_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def hard_bound():
    def expired(signum: int, frame: object) -> None:
        raise TimeoutError("worker entrypoint test exceeded its hard bound")

    previous = signal.signal(signal.SIGALRM, expired)
    signal.alarm(HARD_BOUND_SECONDS)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "app_env": "test",
        "auth_mode": "dev",
        "aws_region": "us-east-2",
        "dynamodb_domain_table": DOMAIN_TABLE,
        "dynamodb_endpoint_url": DYNAMO_ENDPOINT,
        "sqs_endpoint_url": SQS_ENDPOINT,
        "sqs_telemetry_ingress_queue_url": QUEUE_URL,
        "telemetry_v2_write_enabled": True,
        "influxdb_url": INFLUX["url"],
        "influxdb_token": INFLUX["token"],
        "influxdb_org": INFLUX["org"],
        "influxdb_bucket_raw": BUCKET,
        "telemetry_ingest_worker_max_messages": 7,
        "telemetry_ingest_worker_wait_seconds": 11,
        "telemetry_ingest_worker_visibility_seconds": 90,
        "telemetry_ingest_worker_concurrency": 3,
        "telemetry_ingest_drain_seconds": 12,
        "telemetry_ingest_processing_deadline_seconds": 33,
        "telemetry_ingest_retry_base_seconds": 17,
        "telemetry_ingest_retry_cap_seconds": 400,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


class FakeSqsClient:
    def __init__(self) -> None:
        self.receive_calls: list[dict[str, Any]] = []

    def receive_message(self, **kwargs: Any) -> dict[str, Any]:
        self.receive_calls.append(kwargs)
        return {"Messages": []}

    def __getattr__(self, name: str) -> object:
        raise AssertionError(f"unexpected SQS call: {name}")


class FakeWriteApi:
    def __init__(self, options: WriteOptions) -> None:
        self.options = options


class FakeInfluxClient:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.write_api_calls: list[WriteOptions] = []
        self.write_apis: list[FakeWriteApi] = []

    def write_api(self, *args: Any, **kwargs: Any) -> FakeWriteApi:
        options = kwargs.get("write_options", args[0] if args else None)
        options = options if options is not None else WriteOptions()  # the batching default
        self.write_api_calls.append(options)
        api = FakeWriteApi(options)
        self.write_apis.append(api)
        return api

    def query_api(self, *args: Any, **kwargs: Any) -> object:
        raise AssertionError("the worker never queries InfluxDB")


class Factories:
    def __init__(self) -> None:
        self.boto_calls: list[tuple[str, dict[str, Any]]] = []
        self.dynamo = object()
        self.sqs = FakeSqsClient()
        self.influx: list[FakeInfluxClient] = []

    def client(self, service_name: str, **kwargs: Any) -> object:
        self.boto_calls.append((service_name, kwargs))
        if service_name == "dynamodb":
            return self.dynamo
        if service_name == "sqs":
            return self.sqs
        raise AssertionError(f"the worker must not create a {service_name} client")

    def influx_client(self, **kwargs: Any) -> FakeInfluxClient:
        client = FakeInfluxClient(**kwargs)
        self.influx.append(client)
        return client

    def kwargs(self, service_name: str) -> dict[str, Any]:
        (kwargs,) = [kwargs for name, kwargs in self.boto_calls if name == service_name]
        return kwargs


def _build(settings: Settings) -> tuple[IngestWorkerLoop, Factories]:
    factories = Factories()
    loop = build_worker(
        settings,
        client_factory=factories.client,
        influx_client_factory=factories.influx_client,
    )
    return loop, factories


# --- build_worker ------------------------------------------------------------------------------


def test_build_worker_returns_a_loop_configured_from_the_int_settings() -> None:
    loop, _ = _build(_settings())

    assert isinstance(loop, IngestWorkerLoop)
    assert loop.config == IngestWorkerConfig(
        max_messages=7,
        wait_seconds=11,
        visibility_seconds=90,
        concurrency=3,
        drain_seconds=12,
        processing_deadline_seconds=33,
    )
    assert type(loop.config.wait_seconds) is int
    assert type(loop.config.visibility_seconds) is int
    assert type(loop.config.max_messages) is int


def test_build_worker_uses_the_default_settings_values() -> None:
    settings = Settings(
        _env_file=None,
        app_env="test",
        auth_mode="dev",
        telemetry_v2_write_enabled=True,
        sqs_telemetry_ingress_queue_url=QUEUE_URL,
    )

    loop, _ = _build(settings)

    assert loop.config == IngestWorkerConfig(
        max_messages=settings.telemetry_ingest_worker_max_messages,
        wait_seconds=settings.telemetry_ingest_worker_wait_seconds,
        visibility_seconds=settings.telemetry_ingest_worker_visibility_seconds,
        concurrency=settings.telemetry_ingest_worker_concurrency,
        drain_seconds=settings.telemetry_ingest_drain_seconds,
        processing_deadline_seconds=settings.telemetry_ingest_processing_deadline_seconds,
    )


def test_build_worker_creates_only_a_dynamodb_and_an_sqs_client() -> None:
    _, factories = _build(_settings())

    assert sorted(name for name, _ in factories.boto_calls) == ["dynamodb", "sqs"]
    dynamo = factories.kwargs("dynamodb")
    assert dynamo["region_name"] == "us-east-2"
    assert dynamo["endpoint_url"] == DYNAMO_ENDPOINT
    sqs = factories.kwargs("sqs")
    assert sqs["region_name"] == "us-east-2"
    assert sqs["endpoint_url"] == SQS_ENDPOINT


def test_build_worker_sqs_client_matches_the_shared_sqs_client_kwargs() -> None:
    settings = _settings()

    _, factories = _build(settings)

    expected = telemetry_ingest.sqs_client_kwargs(settings)
    sqs = factories.kwargs("sqs")
    assert {key: sqs.get(key) for key in expected} == expected


def test_build_worker_never_injects_dummy_credentials_outside_local_and_test() -> None:
    settings = Settings(
        _env_file=None,
        app_env="prod",
        auth_mode="cognito",
        telegram_webhook_enabled=False,
        telemetry_v2_write_enabled=True,
        sqs_telemetry_ingress_queue_url=QUEUE_URL,
    )

    _, factories = _build(settings)

    for _, kwargs in factories.boto_calls:
        assert "aws_access_key_id" not in kwargs
        assert "aws_secret_access_key" not in kwargs
        assert "endpoint_url" not in kwargs


@pytest.mark.parametrize("service_name", ["sqs", "dynamodb"])
def test_build_worker_keeps_botocore_timeouts_finite(service_name: str) -> None:
    settings = _settings(telemetry_ingest_worker_wait_seconds=20)

    _, factories = _build(settings)

    configured = factories.kwargs(service_name).get("config")
    effective = Config() if configured is None else Config().merge(configured)
    for timeout in (effective.connect_timeout, effective.read_timeout):
        assert isinstance(timeout, (int, float)) and not isinstance(timeout, bool)
        assert 0 < timeout and math.isfinite(timeout)
    if service_name == "sqs":
        # A long poll must finish inside the read timeout, or every idle receive would fail.
        assert effective.read_timeout > settings.telemetry_ingest_worker_wait_seconds


def test_build_worker_consumes_the_configured_queue_through_the_sqs_client() -> None:
    loop, factories = _build(_settings())

    assert isinstance(loop.consumer, SqsIngressQueue)
    messages = asyncio.run(
        loop.consumer.receive(max_messages=1, wait_seconds=0, visibility_seconds=10)
    )

    assert messages == []
    assert [call["QueueUrl"] for call in factories.sqs.receive_calls] == [QUEUE_URL]
    assert hasattr(loop.consumer, "heartbeat")


def test_build_worker_composes_the_processor_from_the_dynamo_client() -> None:
    loop, factories = _build(_settings())
    processor = loop.processor

    assert isinstance(processor, IngestMessageProcessor)
    assert processor.retry.base == 17
    assert processor.retry.cap == 400
    assert isinstance(processor.metrics, StructuredLogTelemetryMetrics)
    assert processor.metrics is loop.metrics

    mapping = processor.mapping
    assert isinstance(mapping, TrustedMappingService)
    assert isinstance(mapping.resolver, DynamoIntegrationRepository)
    assert isinstance(mapping.integrations, DynamoIntegrationRepository)
    assert isinstance(mapping.devices, DynamoDeviceComponentRepository)
    assert isinstance(mapping.components, DynamoDeviceComponentRepository)

    assert isinstance(processor.quarantine, DynamoQuarantineRepository)
    assert isinstance(processor.state, IngestStateService)
    assert isinstance(processor.state.repository, DynamoIngestStateRepository)

    stores = (
        mapping.resolver,
        mapping.integrations,
        mapping.devices,
        mapping.components,
        processor.quarantine,
        processor.state.repository,
    )
    for store in stores:
        assert store.client is factories.dynamo
        assert store.table_name == DOMAIN_TABLE


@pytest.mark.parametrize("legacy_dual_write", [False, True])
def test_build_worker_writes_through_one_synchronous_influx_writer(
    legacy_dual_write: bool,
) -> None:
    loop, factories = _build(_settings(telemetry_legacy_dual_write_enabled=legacy_dual_write))
    pipeline = loop.processor.pipeline

    (influx,) = factories.influx
    assert influx.kwargs == INFLUX
    assert len(influx.write_api_calls) == 1
    options = influx.write_api_calls[0]
    assert options is SYNCHRONOUS or options.write_type is WriteType.synchronous

    assert isinstance(pipeline, TelemetryPipeline)
    assert isinstance(pipeline.normalizer, TelemetryNormalizer)
    assert isinstance(pipeline.normalizer.components, DynamoDeviceComponentRepository)
    assert isinstance(pipeline.normalizer.deployments, DynamoDeploymentRepository)
    assert pipeline.normalizer.deployments.client is factories.dynamo

    service = pipeline.write_service
    assert isinstance(service, ObservationWriteService)
    assert service.v2_write_enabled is True
    assert isinstance(service.writer, InfluxObservationWriter)
    assert service.writer.write_api is influx.write_apis[0]
    assert service.writer.bucket == BUCKET
    assert service.writer.org == INFLUX["org"]
    assert service.legacy_dual_write_enabled is legacy_dual_write
    if legacy_dual_write:
        assert service.legacy_writer is service.writer
    else:
        assert service.legacy_writer is None
    assert isinstance(service.site_assets, DynamoSiteAssetRepository)
    assert service.site_assets.client is factories.dynamo


def test_worker_module_never_imports_the_api_app_or_the_optional_cache() -> None:
    code = (
        "import sys\n"
        "import limnopulse_api.workers.telemetry_ingest\n"
        "print(sorted(name for name in ('redis', 'limnopulse_api.main') if name in sys.modules))\n"
    )

    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True, timeout=60
    )

    assert result.stdout.strip() == "[]"


# --- main(): start guard -----------------------------------------------------------------------


def _forbid_build(monkeypatch: pytest.MonkeyPatch) -> list[Settings]:
    calls: list[Settings] = []

    def forbidden(settings: Settings, **kwargs: object) -> IngestWorkerLoop:
        calls.append(settings)
        raise AssertionError("a guarded worker must not compose anything")

    monkeypatch.setattr(telemetry_ingest, "build_worker", forbidden)
    return calls


@pytest.mark.parametrize(
    "environment",
    [
        {"SQS_TELEMETRY_INGRESS_QUEUE_URL": QUEUE_URL},
        {"SQS_TELEMETRY_INGRESS_QUEUE_URL": QUEUE_URL, "TELEMETRY_V2_WRITE_ENABLED": "false"},
        {"TELEMETRY_V2_WRITE_ENABLED": "true"},
        {"TELEMETRY_V2_WRITE_ENABLED": "true", "SQS_TELEMETRY_INGRESS_QUEUE_URL": ""},
        {},
    ],
    ids=repr,
)
def test_main_refuses_to_start_with_exit_2_unless_writes_and_queue_are_configured(
    monkeypatch: pytest.MonkeyPatch, environment: dict[str, str]
) -> None:
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    calls = _forbid_build(monkeypatch)

    assert main() == 2
    assert calls == []


def test_module_execution_exits_2_when_v2_writes_are_off(tmp_path: Path) -> None:
    environment = {
        key: value
        for key, value in os.environ.items()
        if key not in WORKER_ENVIRONMENT and not key.startswith("TELEMETRY_")
    }
    environment["SQS_TELEMETRY_INGRESS_QUEUE_URL"] = QUEUE_URL

    result = subprocess.run(
        [sys.executable, "-m", "limnopulse_api.workers.telemetry_ingest"],
        capture_output=True,
        text=True,
        check=False,
        cwd=tmp_path,
        env=environment,
        timeout=60,
    )

    assert result.returncode == 2
    assert QUEUE_URL not in result.stdout + result.stderr


# --- main(): run and signal handling ---------------------------------------------------------


class FakeLoop:
    def __init__(self, signum: int | None) -> None:
        self.signum = signum
        self.stop_seen: asyncio.Event | None = None
        self.stopped = False

    async def run(self, stop: asyncio.Event) -> None:
        self.stop_seen = stop
        if self.signum is not None:
            os.kill(os.getpid(), self.signum)
        await asyncio.wait_for(stop.wait(), timeout=5)
        self.stopped = stop.is_set()


def _configure_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("TELEMETRY_V2_WRITE_ENABLED", "true")
    monkeypatch.setenv("SQS_TELEMETRY_INGRESS_QUEUE_URL", QUEUE_URL)


@pytest.fixture
def fallback_signal_handlers():
    """If `main()` installed no handler, a signal lands here instead of killing pytest."""
    received: list[int] = []
    names = (signal.SIGTERM, signal.SIGINT)
    previous = {signum: signal.getsignal(signum) for signum in names}
    for signum in names:
        signal.signal(signum, lambda number, frame: received.append(number))
    try:
        yield received
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT], ids=["SIGTERM", "SIGINT"])
def test_main_runs_the_built_loop_and_a_signal_sets_stop(
    monkeypatch: pytest.MonkeyPatch, hard_bound, fallback_signal_handlers, signum: int
) -> None:
    _configure_environment(monkeypatch)
    fake = FakeLoop(signum)
    built: list[Settings] = []

    def build(settings: Settings, **kwargs: object) -> FakeLoop:
        built.append(settings)
        return fake

    monkeypatch.setattr(telemetry_ingest, "build_worker", build)

    assert main() == 0
    assert fake.stopped is True
    assert fallback_signal_handlers == []
    (settings,) = built
    assert settings.telemetry_v2_write_enabled is True
    assert settings.sqs_telemetry_ingress_queue_url == QUEUE_URL
