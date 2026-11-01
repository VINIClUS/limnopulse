"""P3-10 worker entrypoint (D7): `build_worker(settings)` composes the P3-08 loop from injected
client factories without `create_app` or the optional cache, and `main()` refuses to start
unless v2 writes are on and the queue URL is set, then stops on SIGTERM/SIGINT.

ADR-020 (S5): a refusal is a configuration error and exits 78, a crash or a stall exits 1, and only
a stop signal exits 0. `main(["healthcheck"])` answers from the liveness stamp alone."""

import asyncio
import math
import os
import re
import signal
import subprocess
import sys
import tempfile
import textwrap
import time
from pathlib import Path
from typing import Any

import pytest
from botocore.config import Config
from influxdb_client.client.write_api import SYNCHRONOUS, WriteOptions, WriteType

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
from limnopulse_api.workers.telemetry_ingest import build_worker, main
from limnopulse_api.workers.telemetry_ingest_liveness import (
    LivenessStamp,
    LivenessWatchdog,
    liveness_path,
)
from limnopulse_api.workers.telemetry_ingest_loop import IngestWorkerConfig, IngestWorkerLoop

QUEUE_URL = "http://localhost:9324/000000000000/limnopulse-telemetry-ingress"
DLQ_URL = "http://localhost:9324/000000000000/limnopulse-telemetry-ingress-dlq"
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
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))  # the liveness stamp lives here
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
def test_build_worker_sets_explicit_finite_botocore_timeouts(service_name: str) -> None:
    """A46: the drain waits on in-flight heartbeat and final-action calls, so the bound is
    pinned explicitly instead of relying on botocore's defaults."""
    settings = _settings(telemetry_ingest_worker_wait_seconds=20)

    _, factories = _build(settings)

    configured = factories.kwargs(service_name).get("config")
    assert isinstance(configured, Config)
    # botocore keeps the explicitly passed options in a private mapping; fall back to "set at
    # all" if a botocore upgrade renames it.
    provided = getattr(configured, "_user_provided_options", None)
    if provided is not None:
        assert {"connect_timeout", "read_timeout"} <= set(provided)
    for timeout in (configured.connect_timeout, configured.read_timeout):
        assert isinstance(timeout, (int, float)) and not isinstance(timeout, bool)
        # The 120 s ceiling is a test choice: well above the 20 s long poll, yet short enough
        # that a hung call cannot hold the drain for long. The plan pins only "finite" (A46).
        assert 0 < timeout <= 120 and math.isfinite(timeout)
    if service_name == "sqs":
        # A long poll must finish inside the read timeout, or every idle receive would fail.
        assert configured.read_timeout > settings.telemetry_ingest_worker_wait_seconds


def test_build_worker_consumes_the_configured_queue_through_the_sqs_client() -> None:
    # The DLQ URL is set too: the worker must consume the ingress queue, never the DLQ (D10).
    loop, factories = _build(_settings(sqs_telemetry_ingress_dlq_url=DLQ_URL))

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
    assert {key: influx.kwargs.get(key) for key in INFLUX} == INFLUX
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


@pytest.mark.parametrize(
    "overrides",
    [{"telemetry_v2_write_enabled": False}, {"sqs_telemetry_ingress_queue_url": ""}],
    ids=repr,
)
def test_build_worker_itself_refuses_a_worker_that_could_delete_unwritten_data(
    overrides: dict[str, object],
) -> None:
    """D7 defence in depth: any caller, not only `main()`, gets no loop and no client."""
    factories = Factories()

    with pytest.raises(ValueError):
        build_worker(
            _settings(**overrides),
            client_factory=factories.client,
            influx_client_factory=factories.influx_client,
        )

    assert factories.boto_calls == []
    assert factories.influx == []


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
def test_main_refuses_to_start_with_exit_78_unless_writes_and_queue_are_configured(
    monkeypatch: pytest.MonkeyPatch, environment: dict[str, str]
) -> None:
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    calls = _forbid_build(monkeypatch)

    assert main() == 78
    assert calls == []


def test_module_execution_exits_78_when_v2_writes_are_off(tmp_path: Path) -> None:
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

    assert result.returncode == 78
    assert QUEUE_URL not in result.stdout + result.stderr


# --- main(): run and signal handling ---------------------------------------------------------


class FakeLoop:
    """Stands in for `IngestWorkerLoop.run`: after `stop` it still drains (async work), the way
    the P3-08 loop releases its poll and waits for in-flight messages."""

    def __init__(self, signum: int | None) -> None:
        self.signum = signum
        self.stop_seen: asyncio.Event | None = None
        self.stopped = False
        self.drained = False
        self.cancelled = False

    async def run(self, stop: asyncio.Event) -> None:
        self.stop_seen = stop
        try:
            if self.signum is not None:
                os.kill(os.getpid(), self.signum)
            await asyncio.wait_for(stop.wait(), timeout=5)
            self.stopped = stop.is_set()
            for _ in range(3):
                await asyncio.sleep(0)
            await asyncio.sleep(0.05)
            self.drained = True
        except asyncio.CancelledError:
            self.cancelled = True
            raise


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
    # A signal only sets `stop`: `run()` is allowed to finish its drain, never cancelled.
    assert fake.drained is True
    assert fake.cancelled is False
    assert fallback_signal_handlers == []
    (settings,) = built
    assert settings.telemetry_v2_write_enabled is True
    assert settings.sqs_telemetry_ingress_queue_url == QUEUE_URL


class CrashingLoop:
    async def run(self, stop: asyncio.Event) -> None:
        raise RuntimeError("sentinel-crash-detail")


def test_main_exits_non_zero_when_the_loop_crashes_without_logging_the_error_text(
    monkeypatch: pytest.MonkeyPatch, hard_bound, caplog: pytest.LogCaptureFixture
) -> None:
    """A crash must not look like a clean stop: it exits 1, so `unless-stopped` restarts it."""
    _configure_environment(monkeypatch)
    monkeypatch.setattr(telemetry_ingest, "build_worker", lambda settings, **kwargs: CrashingLoop())

    with caplog.at_level("DEBUG"):
        code = main()

    assert code == 1
    assert "sentinel-crash-detail" not in caplog.text


# --- ADR-020 (S5): exit codes, argv and the healthcheck ----------------------------------------


def test_the_worker_uses_the_shared_adr_020_exit_codes() -> None:
    assert (
        telemetry_ingest.EXIT_STOPPED,
        telemetry_ingest.EXIT_FAILURE,
        telemetry_ingest.EXIT_CONFIG,
    ) == (0, 1, 78)
    assert not hasattr(telemetry_ingest, "EXIT_REFUSED")  # 2 now means a partial one-shot run


def test_invalid_settings_are_a_configuration_error(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _configure_environment(monkeypatch)
    monkeypatch.setenv("TELEMETRY_INGEST_WORKER_CONCURRENCY", "0")
    calls = _forbid_build(monkeypatch)

    with caplog.at_level("INFO"):
        assert main() == 78

    assert calls == []
    assert "telemetry ingest worker refused to start: 1 invalid settings" in caplog.text


@pytest.mark.parametrize("argv", [["bogus"], ["healthcheck", "extra"], ["--help"]], ids=repr)
def test_an_unknown_argument_is_a_configuration_error(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, argv: list[str]
) -> None:
    """Never argparse: its usage error exits 2, which ADR-020 keeps for a partial one-shot run."""
    _configure_environment(monkeypatch)
    calls = _forbid_build(monkeypatch)

    with caplog.at_level("INFO"):
        assert main(argv) == 78

    assert calls == []
    assert "telemetry ingest worker refused to start: unknown arguments" in caplog.text
    assert "bogus" not in caplog.text


def test_the_healthcheck_dispatches_before_settings_and_the_start_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Settings() would fail and the start guard would refuse: neither runs for a healthcheck.
    monkeypatch.setenv("TELEMETRY_INGEST_WORKER_CONCURRENCY", "0")
    calls = _forbid_build(monkeypatch)

    assert main(["healthcheck"]) == 1  # no stamp yet
    LivenessStamp(liveness_path(), stall_after_seconds=100).mark()
    assert main(["healthcheck"]) == 0
    assert calls == []
    assert liveness_path().exists()  # a healthcheck never removes the stamp


def test_a_starting_worker_removes_an_old_stamp_first(monkeypatch: pytest.MonkeyPatch) -> None:
    """A refused (parked) worker must never look alive through a stamp it did not write."""
    LivenessStamp(liveness_path(), stall_after_seconds=100).mark()
    monkeypatch.setenv("SQS_TELEMETRY_INGRESS_QUEUE_URL", QUEUE_URL)  # writes stay off

    assert main() == 78
    assert not liveness_path().exists()


def _worker_subprocess_environment(tmp_path: Path, **extra: str) -> dict[str, str]:
    environment = {
        key: value
        for key, value in os.environ.items()
        if key not in WORKER_ENVIRONMENT and not key.startswith("TELEMETRY_")
    }
    environment.update({"TMPDIR": str(tmp_path), **extra})
    return environment


def _imported_modules(importtime: str) -> set[str]:
    return set(re.findall(r"^import time:[^|]*\|[^|]*\|\s*(\S+)\s*$", importtime, re.MULTILINE))


def test_module_healthcheck_reads_only_the_stamp_without_the_worker_imports(
    tmp_path: Path,
) -> None:
    """Docker runs the probe inside the worker's 0.5-CPU limit: it must not pay the ~1.6 s of
    boto3, InfluxDB and pydantic imports the worker needs, nor load any setting."""
    command = [
        sys.executable,
        "-X",
        "importtime",
        "-m",
        "limnopulse_api.workers.telemetry_ingest",
        "healthcheck",
    ]
    environment = _worker_subprocess_environment(
        tmp_path, TELEMETRY_INGEST_WORKER_CONCURRENCY="0"
    )

    def probe() -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            cwd=tmp_path,
            env=environment,
            timeout=60,
        )

    missing = probe()
    LivenessStamp(tmp_path / "limnopulse-telemetry-ingest.liveness", stall_after_seconds=100).mark()
    fresh = probe()

    assert (missing.returncode, fresh.returncode) == (1, 0), (missing.stdout, fresh.stdout)
    assert "no liveness stamp" in missing.stdout
    imported = _imported_modules(fresh.stderr)
    assert "limnopulse_api.workers.telemetry_ingest_liveness" in imported
    for heavy in (
        "boto3",
        "botocore",
        "influxdb_client",
        "pydantic",
        "redis",
        "limnopulse_api.main",
        "limnopulse_api.core.config",
    ):
        assert heavy not in imported, heavy


class ReturningLoop:
    async def run(self, stop: asyncio.Event) -> None:
        return None


def test_a_loop_that_returns_without_a_stop_exits_1(
    monkeypatch: pytest.MonkeyPatch, hard_bound, caplog: pytest.LogCaptureFixture
) -> None:
    """Exit 0 means a stop signal and nothing else."""
    _configure_environment(monkeypatch)
    monkeypatch.setattr(telemetry_ingest, "build_worker", lambda settings, **kwargs: ReturningLoop())

    with caplog.at_level("INFO"):
        assert main([]) == 1

    assert "telemetry ingest worker loop ended without a stop" in caplog.text


def test_main_without_an_argument_still_starts_the_worker(
    monkeypatch: pytest.MonkeyPatch, hard_bound, fallback_signal_handlers
) -> None:
    _configure_environment(monkeypatch)
    fake = FakeLoop(signal.SIGTERM)
    monkeypatch.setattr(telemetry_ingest, "build_worker", lambda settings, **kwargs: fake)

    assert main([]) == 0
    assert fake.drained is True


# --- ADR-020 (S5): stall detection -------------------------------------------------------------


class SilentLoop:
    """Never marks liveness. `blocking_seconds` holds the event loop thread itself first, the way
    a synchronous call made on the loop would; afterwards it only awaits, forever."""

    def __init__(self, *, blocking_seconds: float = 0.0, signum: int | None = None) -> None:
        self.blocking_seconds = blocking_seconds
        self.signum = signum
        self.cancelled = False

    async def run(self, stop: asyncio.Event) -> None:
        try:
            if self.blocking_seconds:
                time.sleep(self.blocking_seconds)
            if self.signum is not None:
                os.kill(os.getpid(), self.signum)
                await stop.wait()
                await asyncio.sleep(0.6)  # a drain longer than the stall threshold
                return
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise


class FastWatchdog:
    def __init__(self, monkeypatch: pytest.MonkeyPatch, *, grace: float) -> None:
        self.exits: list[int] = []
        self.watchdogs: list[LivenessWatchdog] = []
        self.stamps: list[LivenessStamp] = []
        watchdogs, stamps = self.watchdogs, self.stamps

        class Recorded(LivenessWatchdog):
            def __init__(self, stamp: LivenessStamp, **kwargs: object) -> None:
                super().__init__(stamp, **kwargs)  # type: ignore[arg-type]
                watchdogs.append(self)
                stamps.append(stamp)

        monkeypatch.setattr(telemetry_ingest, "LivenessWatchdog", Recorded)
        monkeypatch.setattr(telemetry_ingest, "stall_after_seconds", lambda settings: 0.2)
        monkeypatch.setattr(telemetry_ingest, "_stall_exit_grace_seconds", lambda settings: grace)
        monkeypatch.setattr(telemetry_ingest, "_WATCHDOG_CHECK_SECONDS", 0.02)
        monkeypatch.setattr(telemetry_ingest, "_force_exit", self.exits.append)

    def join(self) -> None:
        for watchdog in self.watchdogs:
            watchdog.join(HARD_BOUND_SECONDS)
            assert not watchdog.alive


def test_a_stalled_loop_is_cancelled_and_exits_1(
    monkeypatch: pytest.MonkeyPatch,
    hard_bound,
    caplog: pytest.LogCaptureFixture,
    capfd: pytest.CaptureFixture[str],
) -> None:
    """The clean path: the watchdog cancels the loop through the event loop, `main` returns 1,
    and the forced exit stays armed in case the interpreter's shutdown hangs."""
    _configure_environment(monkeypatch)
    watch = FastWatchdog(monkeypatch, grace=2.0)  # wide, so a slow runner cannot flake it
    fake = SilentLoop()
    monkeypatch.setattr(telemetry_ingest, "build_worker", lambda settings, **kwargs: fake)

    with caplog.at_level("INFO"):
        assert main() == 1

    assert fake.cancelled is True
    assert watch.exits == []  # the clean path came first
    assert "telemetry ingest worker loop cancelled after a stall" in caplog.text
    assert "telemetry ingest worker stalled" in capfd.readouterr().err
    watch.join()
    assert watch.exits == [1]


def test_a_blocked_event_loop_gets_the_forced_exit(
    monkeypatch: pytest.MonkeyPatch, hard_bound
) -> None:
    """A blocked event loop never runs the cancel: only the thread's forced exit ends it."""
    _configure_environment(monkeypatch)
    watch = FastWatchdog(monkeypatch, grace=0.1)
    fake = SilentLoop(blocking_seconds=1.5)
    monkeypatch.setattr(telemetry_ingest, "build_worker", lambda settings, **kwargs: fake)

    started = time.monotonic()
    code = main()

    assert watch.exits == [1]  # fired while the loop was still blocked
    assert time.monotonic() - started >= 1.5
    assert code == 1
    watch.join()


def test_a_stop_signal_disarms_the_watchdog_for_the_drain(
    monkeypatch: pytest.MonkeyPatch, hard_bound, fallback_signal_handlers
) -> None:
    _configure_environment(monkeypatch)
    watch = FastWatchdog(monkeypatch, grace=0.1)
    fake = SilentLoop(signum=signal.SIGTERM)
    monkeypatch.setattr(telemetry_ingest, "build_worker", lambda settings, **kwargs: fake)

    assert main() == 0

    assert (watch.exits, fake.cancelled) == ([], False)
    (watchdog,) = watch.watchdogs
    assert watchdog.stalled is False and not watchdog.alive
    assert fallback_signal_handlers == []


def test_the_watchdog_watches_the_stamp_the_loop_marks(
    monkeypatch: pytest.MonkeyPatch, hard_bound
) -> None:
    _configure_environment(monkeypatch)
    watch = FastWatchdog(monkeypatch, grace=0.1)
    built: list[dict[str, object]] = []

    def build(settings: Settings, **kwargs: object) -> ReturningLoop:
        built.append(kwargs)
        return ReturningLoop()

    monkeypatch.setattr(telemetry_ingest, "build_worker", build)

    main()

    ((stamp,), (kwargs,)) = watch.stamps, built
    assert kwargs["liveness"] == stamp.mark
    assert stamp.path == liveness_path()
    assert stamp.stall_after_seconds == 0.2


def test_a_blocked_event_loop_exits_1_in_a_real_process(tmp_path: Path) -> None:
    """End to end with the real `os._exit`: nothing in the process could end it otherwise."""
    code = textwrap.dedent(
        """
        import sys
        import time

        from limnopulse_api.workers import telemetry_ingest as worker


        class Blocked:
            async def run(self, stop):
                time.sleep(3600)  # holds the event loop thread for good


        worker.build_worker = lambda settings, **kwargs: Blocked()
        worker.stall_after_seconds = lambda settings: 0.3
        worker._stall_exit_grace_seconds = lambda settings: 0.3
        worker._WATCHDOG_CHECK_SECONDS = 0.05
        sys.exit(worker.main([]))
        """
    )
    environment = _worker_subprocess_environment(
        tmp_path,
        APP_ENV="test",
        AUTH_MODE="dev",
        TELEMETRY_V2_WRITE_ENABLED="true",
        SQS_TELEMETRY_INGRESS_QUEUE_URL=QUEUE_URL,
    )

    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=False,
        cwd=tmp_path,
        env=environment,
        timeout=60,
    )

    assert result.returncode == 1, result.stderr
    assert "telemetry ingest worker stalled" in result.stderr
    assert "forcing exit 1" in result.stderr


# --- ADR-020 (S5): composition and the stall threshold -----------------------------------------


def test_build_worker_wires_liveness_and_the_message_bound_into_the_loop() -> None:
    settings = _settings()
    marks: list[None] = []

    loop = build_worker(
        settings,
        client_factory=Factories().client,
        influx_client_factory=Factories().influx_client,
        liveness=lambda: marks.append(None),
    )

    loop.liveness()
    assert marks == [None]
    assert loop.overdue_after_seconds == telemetry_ingest.stall_after_seconds(settings)


def test_build_worker_pins_a_finite_influx_timeout() -> None:
    """The stall threshold counts one InfluxDB write: anyio waits for its thread on a deadline."""
    _, factories = _build(_settings())

    (influx,) = factories.influx
    assert influx.kwargs["timeout"] == telemetry_ingest.INFLUX_TIMEOUT_SECONDS * 1000 == 10_000


def test_the_stall_threshold_is_the_worst_message_lifetime_at_the_defaults() -> None:
    """45 s deadline + one 10 s InfluxDB write + an awaited heartbeat (28 s) + the final action
    (28 s): 111 s at the defaults, the "about 100 s" ADR-020 S5 asked for."""
    defaults = Settings(_env_file=None, app_env="test", auth_mode="dev")

    budget = telemetry_ingest.shutdown_call_budget_seconds(defaults)
    assert budget == 28
    assert telemetry_ingest.stall_after_seconds(defaults) == 45 + 10 + 2 * budget == 111


@pytest.mark.parametrize("wait_seconds", [0, 1, 10, 20])
@pytest.mark.parametrize("deadline", [5, 45, 900])
def test_the_stall_threshold_outlasts_every_legitimate_loop_turn(
    wait_seconds: int, deadline: int
) -> None:
    """A turn is one receive (the SQS worst call, plus the 0.5 s error pause) or one busy tick.
    The receive may first wait for a pool thread behind a call already in flight, so the
    threshold must exceed twice that, for every allowed setting."""
    settings = Settings(
        _env_file=None,
        app_env="test",
        auth_mode="dev",
        telemetry_ingest_worker_wait_seconds=wait_seconds,
        telemetry_ingest_processing_deadline_seconds=deadline,
    )
    receive = 3 + wait_seconds + 5 + 0.5  # connect + long poll + read margin + error pause
    turn = max(receive, 5.0)  # the busy tick is LIVENESS_TICK_SECONDS

    assert telemetry_ingest.stall_after_seconds(settings) > 2 * turn
    assert telemetry_ingest.stall_after_seconds(settings) > deadline


def test_the_stall_exit_grace_is_the_a47_stop_budget() -> None:
    """A stalled loop gets the same time to drain as a stopped one."""
    defaults = Settings(_env_file=None, app_env="test", auth_mode="dev")

    assert telemetry_ingest._stall_exit_grace_seconds(defaults) == (
        defaults.telemetry_ingest_worker_wait_seconds
        + 5
        + defaults.telemetry_ingest_drain_seconds
        + telemetry_ingest.shutdown_call_budget_seconds(defaults)
    )
