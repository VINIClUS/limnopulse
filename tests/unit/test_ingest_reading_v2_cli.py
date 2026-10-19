"""P2-08 dev CLI `scripts/dev/ingest_reading_v2.py`, driven through `main(argv) -> int` with fakes.

Test seams: the module-level `boto3`, `InfluxDBClient` and `TelemetryPipeline` names. Settings
come from the environment (the test runs in an empty working directory, so no `.env` applies).
Settings may be built per run or through `get_settings()` (its cache is cleared around each
test). Exit codes: 0 when nothing is quarantined (including the both-switches-off dry run, which
normalizes and writes zero points), 2 when anything is quarantined, 1 on any error, usage errors
included (2 is reserved for quarantine). Stdout is
one JSON object of counts: `written` (the `TelemetryWriteOutcome` fields), `quarantined` (the
number of quarantined records) and `counts` (the pipeline's closed-key counters).
"""

import json
import re
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from typing import Any, ClassVar, Self

import pytest
from botocore.exceptions import EndpointConnectionError
from influxdb_client.client.write_api import SYNCHRONOUS, WriteOptions, WriteType
from pydantic import TypeAdapter

from limnopulse_api.adapters.dynamodb_deployments import DynamoDeploymentRepository
from limnopulse_api.adapters.dynamodb_devices_v2 import DynamoDeviceComponentRepository
from limnopulse_api.adapters.dynamodb_sites_assets import DynamoSiteAssetRepository
from limnopulse_api.adapters.influxdb_v2_writer import InfluxObservationWriter
from limnopulse_api.core.config import get_settings
from limnopulse_api.core.errors import TelemetryWriteError
from limnopulse_api.domain.legacy_telemetry_mapping import map_legacy_reading
from limnopulse_api.domain.telemetry_v2 import (
    SourceType,
    TelemetryDatetime,
    TrustedSourceContext,
)
from limnopulse_api.services.telemetry_normalizer import TelemetryNormalizer
from limnopulse_api.services.telemetry_pipeline import PipelineOutcome
from limnopulse_api.services.telemetry_v2_writes import (
    ObservationWriteService,
    TelemetryWriteOutcome,
)
from scripts.dev import ingest_reading_v2

ROOT = Path(__file__).resolve().parents[2]
EXAMPLE_READING = ROOT / "examples/telemetry/reading.local.json"
TENANT = "tnt_cli"
DEVICE = "local-device-001"
TOKEN = "p2-08-influx-token-must-not-leak"
FIXED_RECEIVED_AT = "2026-10-18T12:00:05Z"
TELEGRAF_TIMESTAMP = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.(?P<fraction>\d{1,9}))?(?:Z|[+-]\d{2}:\d{2})$"
)
_TIMESTAMP = TypeAdapter(TelemetryDatetime)
FLAG_ENVIRONMENT = (
    "TELEMETRY_V2_WRITE_ENABLED",
    "TELEMETRY_LEGACY_DUAL_WRITE_ENABLED",
    "API_V2_TELEMETRY_ENABLED",
    "API_V2_ENABLED",
    "V1_TELEMETRY_READ_SOURCE",
    "V1_POND_DEVICE_PROJECTION_ENABLED",
)
COUNTS = {"accepted": 9, "quarantined.unit_unknown": 0, "state.valid": 9}


def _outcome(*, v2: int = 9, legacy: int = 9, quarantined: tuple = ()) -> PipelineOutcome:
    return PipelineOutcome(
        written=TelemetryWriteOutcome(v2_points=v2, legacy_points=legacy, legacy_excluded=0),
        quarantined=quarantined,
        counts=MappingProxyType(dict(COUNTS)),
    )


def _quarantined_record():
    context = TrustedSourceContext(
        tenant_id=TENANT,
        device_id=DEVICE,
        integration_id=None,
        integration_type="legacy_mqtt",
        source_type=SourceType.LEGACY_MQTT,
        connector_version=None,
        received_at=datetime(2026, 10, 18, 12, 0, tzinfo=UTC),
    )
    records = map_legacy_reading(context, {"ts": "not-a-time"}).quarantined
    assert len(records) == 1
    return records[0]


class FakePipeline:
    instances: ClassVar[list["FakePipeline"]] = []
    result: ClassVar[PipelineOutcome | Exception] = _outcome()

    def __init__(self, *, normalizer: Any, write_service: Any) -> None:
        self.normalizer = normalizer
        self.write_service = write_service
        self.calls: list[tuple[TrustedSourceContext, dict[str, Any]]] = []
        FakePipeline.instances.append(self)

    async def ingest_legacy_reading(self, context, payload) -> PipelineOutcome:
        self.calls.append((context, dict(payload)))
        if isinstance(FakePipeline.result, Exception):
            raise FakePipeline.result
        return FakePipeline.result


class FakeWriteApi:
    def __init__(self, write_options: WriteOptions) -> None:
        self._write_options = write_options

    def write(self, *args: object, **kwargs: object) -> None:
        raise AssertionError("the fake pipeline never writes")


class FakeInfluxClient:
    instances: ClassVar[list["FakeInfluxClient"]] = []

    def __init__(self, **kwargs: object) -> None:
        self.init_kwargs = kwargs
        self.write_api_calls: list[WriteOptions] = []
        self.write_api_instances: list[FakeWriteApi] = []
        self.close_calls = 0
        FakeInfluxClient.instances.append(self)

    def query_api(self, *args: object, **kwargs: object) -> object:
        return object()

    def write_api(self, *args: object, **kwargs: object) -> FakeWriteApi:
        options = kwargs.get("write_options", args[0] if args else None)
        options = options if options is not None else WriteOptions()
        self.write_api_calls.append(options)
        write_api = FakeWriteApi(options)
        self.write_api_instances.append(write_api)
        return write_api

    def close(self) -> None:
        self.close_calls += 1

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


@pytest.fixture
def cli(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    FakePipeline.instances = []
    FakePipeline.result = _outcome()
    FakeInfluxClient.instances = []
    boto_calls: list[tuple[str, dict[str, object]]] = []
    fake_dynamo = object()

    def fake_boto3_client(service_name: str, *args: object, **kwargs: object) -> object:
        boto_calls.append((service_name, kwargs))
        return fake_dynamo

    # Every flag is set explicitly, so a developer `.env` can never decide a test's switches.
    for name in FLAG_ENVIRONMENT:
        monkeypatch.setenv(name, "legacy" if name == "V1_TELEMETRY_READ_SOURCE" else "false")
    monkeypatch.chdir(tmp_path)
    get_settings.cache_clear()
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("AUTH_MODE", "dev")
    monkeypatch.setenv("INFLUXDB_URL", "http://influx.test:8086")
    monkeypatch.setenv("INFLUXDB_TOKEN", TOKEN)
    monkeypatch.setenv("INFLUXDB_ORG", "org_cli")
    monkeypatch.setenv("INFLUXDB_BUCKET_RAW", "raw_cli")
    monkeypatch.setenv("DYNAMODB_DOMAIN_TABLE", "DomainCli")
    monkeypatch.setenv("DYNAMODB_ENDPOINT_URL", "http://dynamodb.test:8001")
    monkeypatch.setenv("AWS_REGION", "sa-east-1")
    monkeypatch.setenv("TELEMETRY_V2_WRITE_ENABLED", "true")
    monkeypatch.setenv("TELEMETRY_LEGACY_DUAL_WRITE_ENABLED", "true")
    monkeypatch.setattr(ingest_reading_v2.boto3, "client", fake_boto3_client)
    monkeypatch.setattr(ingest_reading_v2, "InfluxDBClient", FakeInfluxClient)
    monkeypatch.setattr(ingest_reading_v2, "TelemetryPipeline", FakePipeline)

    reading = tmp_path / "reading.json"
    reading.write_text(EXAMPLE_READING.read_text(encoding="utf-8"), encoding="utf-8")
    yield SimpleNamespace(
        dynamo=fake_dynamo, boto_calls=boto_calls, reading=reading, tmp_path=tmp_path
    )
    get_settings.cache_clear()


def _run(*extra: str, file: Path | None, tenant: str = TENANT) -> int:
    argv = ["--tenant", tenant, "--device", DEVICE, *extra]
    if file is not None:
        argv += ["--file", str(file)]
    return ingest_reading_v2.main(argv)


def _only_call() -> tuple[TrustedSourceContext, dict[str, Any]]:
    assert len(FakePipeline.instances) == 1
    calls = FakePipeline.instances[0].calls
    assert len(calls) == 1
    return calls[0]


def _example_payload() -> dict[str, Any]:
    return json.loads(EXAMPLE_READING.read_text(encoding="utf-8"))


# --- Exit codes and summary -------------------------------------------------------------------


def test_everything_written_exits_0_and_prints_a_json_count_summary(cli, capsys) -> None:
    code = _run("--received-at", FIXED_RECEIVED_AT, file=cli.reading)

    out, err = capsys.readouterr()
    assert code == 0
    summary = json.loads(out)
    assert summary["written"] == {"v2_points": 9, "legacy_points": 9, "legacy_excluded": 0}
    assert summary["quarantined"] == 0
    assert summary["counts"] == COUNTS
    assert TOKEN not in out and TOKEN not in err


@pytest.mark.parametrize(("v2", "legacy"), [(9, 9), (0, 0)])
def test_any_quarantined_record_exits_2(cli, capsys, v2: int, legacy: int) -> None:
    FakePipeline.result = _outcome(v2=v2, legacy=legacy, quarantined=(_quarantined_record(),))

    code = _run(file=cli.reading)

    out, err = capsys.readouterr()
    assert code == 2
    summary = json.loads(out)
    assert summary["quarantined"] == 1
    assert summary["written"]["v2_points"] == v2
    assert summary["written"]["legacy_points"] == legacy
    assert summary["counts"] == COUNTS
    assert TOKEN not in out and TOKEN not in err


@pytest.mark.parametrize(
    "error",
    [
        TelemetryWriteError("observation write failed", retryable=True),
        TelemetryWriteError("observation write rejected", retryable=False),
        ValueError("telemetry batch exceeds 1000 observations"),
        EndpointConnectionError(endpoint_url="http://dynamodb.test:8001"),
        RuntimeError("unexpected failure"),
    ],
    ids=["write-retryable", "write-permanent", "caller-misuse", "dynamodb", "unexpected"],
)
def test_pipeline_errors_exit_1_and_report_nothing_written(cli, capsys, error: Exception) -> None:
    FakePipeline.result = error

    code = _run(file=cli.reading)

    out, err = capsys.readouterr()
    assert code == 1
    assert "v2_points" not in out
    assert TOKEN not in out and TOKEN not in err
    assert FakeInfluxClient.instances and all(
        client.close_calls >= 1 for client in FakeInfluxClient.instances
    )


def test_both_switches_off_is_a_dry_run_that_exits_0_and_creates_no_write_api(
    cli, capsys, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TELEMETRY_V2_WRITE_ENABLED", "false")
    monkeypatch.setenv("TELEMETRY_LEGACY_DUAL_WRITE_ENABLED", "false")
    FakePipeline.result = _outcome(v2=0, legacy=0)

    code = _run(file=cli.reading)

    out, _ = capsys.readouterr()
    assert code == 0
    assert json.loads(out)["written"] == {"v2_points": 0, "legacy_points": 0, "legacy_excluded": 0}
    assert all(client.write_api_calls == [] for client in FakeInfluxClient.instances)
    service = FakePipeline.instances[0].write_service
    assert service.v2_write_enabled is False
    assert service.legacy_dual_write_enabled is False


@pytest.mark.parametrize(
    "content",
    ["[1, 2, 3]", '"a string"', "42", "null", "{not json"],
    ids=["array", "string", "number", "null", "malformed"],
)
def test_a_payload_that_is_not_a_json_object_exits_1_without_ingesting(
    cli, capsys, content: str
) -> None:
    cli.reading.write_text(content, encoding="utf-8")

    code = _run(file=cli.reading)

    out, _ = capsys.readouterr()
    assert code == 1
    assert all(pipeline.calls == [] for pipeline in FakePipeline.instances)
    assert "v2_points" not in out


class RawPayloadPipeline(FakePipeline):
    """Records the payload object as received, so a non-object can never be masked."""

    async def ingest_legacy_reading(self, context, payload) -> PipelineOutcome:
        self.calls.append((context, payload))
        return _outcome()


@pytest.mark.parametrize("content", ["[1, 2, 3]", '"a string"', "42", "null"])
def test_a_non_object_payload_never_reaches_the_pipeline(
    cli, monkeypatch: pytest.MonkeyPatch, content: str
) -> None:
    monkeypatch.setattr(ingest_reading_v2, "TelemetryPipeline", RawPayloadPipeline)
    cli.reading.write_text(content, encoding="utf-8")

    assert _run(file=cli.reading) == 1
    assert all(pipeline.calls == [] for pipeline in FakePipeline.instances)


def test_a_missing_file_exits_1(cli, capsys) -> None:
    assert _run(file=cli.tmp_path / "missing.json") == 1
    assert all(pipeline.calls == [] for pipeline in FakePipeline.instances)
    out, err = capsys.readouterr()
    assert TOKEN not in out and TOKEN not in err


def _exit_code(argv: list[str]) -> int:
    try:
        return ingest_reading_v2.main(argv)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 1


@pytest.mark.parametrize(
    "argv",
    [
        ["--device", DEVICE],
        ["--tenant", TENANT],
        ["--tenant", TENANT, "--device", DEVICE, "--unknown-flag"],
    ],
    ids=["no-tenant", "no-device", "unknown-flag"],
)
def test_a_usage_error_exits_1_never_the_quarantine_code(cli, argv: list[str]) -> None:
    assert _exit_code([*argv, "--file", str(cli.reading)]) == 1
    assert all(pipeline.calls == [] for pipeline in FakePipeline.instances)


def test_the_script_entry_point_returns_main_exit_code_to_the_shell(cli) -> None:
    script = ROOT / "scripts/dev/ingest_reading_v2.py"
    result = subprocess.run(
        [sys.executable, str(script), "--tenant", TENANT, "--device", DEVICE, "--file", "missing"],
        cwd=cli.tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )

    assert result.returncode == 1, result.stderr
    assert "Traceback" not in result.stderr
    assert TOKEN not in result.stdout and TOKEN not in result.stderr


def test_help_documents_the_both_switches_off_dry_run(cli, capsys) -> None:
    assert _exit_code(["--help"]) == 0

    out, _ = capsys.readouterr()
    assert re.search(r"dry[- ]run", " ".join(out.split()).lower()), out


@pytest.mark.parametrize(
    "extra",
    [("--observed-at", "yesterday"), ("--received-at", "not-a-time"), ("--received-at", "12:00")],
)
def test_an_invalid_time_argument_exits_1_not_2(cli, extra: tuple[str, str]) -> None:
    assert _run(*extra, file=cli.reading) == 1
    assert all(pipeline.calls == [] for pipeline in FakePipeline.instances)


def test_an_invalid_switch_combination_exits_1_naming_the_switch(
    cli, capsys, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("API_V2_TELEMETRY_ENABLED", "true")
    monkeypatch.setenv("API_V2_ENABLED", "false")

    assert _run(file=cli.reading) == 1

    out, err = capsys.readouterr()
    assert "API_V2_TELEMETRY_ENABLED" in err
    assert TOKEN not in out and TOKEN not in err
    assert all(pipeline.calls == [] for pipeline in FakePipeline.instances)


def test_unrecognized_arguments_are_not_echoed(cli, capsys) -> None:
    secret = "pasted-secret-value-123"

    assert _exit_code(["--tenant", TENANT, "--device", DEVICE, "--token", secret]) == 1

    out, err = capsys.readouterr()
    assert secret not in out and secret not in err


def test_json_nested_too_deeply_exits_1_without_raising(cli) -> None:
    cli.reading.write_text("[" * 100_000 + "]" * 100_000, encoding="utf-8")

    assert _run(file=cli.reading) == 1
    assert all(pipeline.calls == [] for pipeline in FakePipeline.instances)


def test_an_invalid_tenant_argument_exits_1(cli) -> None:
    assert _run(file=cli.reading, tenant="Not A Tenant!") == 1
    assert all(pipeline.calls == [] for pipeline in FakePipeline.instances)


# --- Trusted context and payload --------------------------------------------------------------


def test_the_context_comes_only_from_cli_arguments(cli) -> None:
    payload = _example_payload() | {
        "tenant_id": "tnt_payload",
        "device_id": "payload-device",
        "pond_id": "pond_payload",
        "integration_type": "vendor_x",
        "source_type": "https",
    }
    cli.reading.write_text(json.dumps(payload), encoding="utf-8")

    assert _run("--received-at", FIXED_RECEIVED_AT, file=cli.reading) == 0

    context, sent = _only_call()
    assert context.tenant_id == TENANT
    assert context.device_id == DEVICE
    assert context.source_type is SourceType.LEGACY_MQTT
    assert context.integration_type == "legacy_mqtt"
    assert context.integration_id is None
    assert context.replay is False
    assert context.mapping_versions is None
    assert context.connector_version is None
    assert context.received_at == datetime(2026, 10, 18, 12, 0, 5, tzinfo=UTC)
    # The payload is handed over as read: the CLI never writes its own tenant into it.
    assert {key: sent[key] for key in _example_payload()} == _example_payload()
    assert sent.get("tenant_id") != TENANT


def test_default_received_at_is_the_current_time(cli) -> None:
    before = datetime.now(UTC)
    assert _run(file=cli.reading) == 0
    after = datetime.now(UTC)

    context, _ = _only_call()
    assert before.replace(microsecond=0) <= context.received_at <= after


def test_without_observed_at_the_file_ts_is_kept(cli) -> None:
    assert _run(file=cli.reading) == 0

    _, sent = _only_call()
    assert sent == _example_payload()


def test_observed_at_iso_replaces_ts_with_that_instant(cli) -> None:
    assert _run("--observed-at", "2026-10-18T11:59:00.250+00:00", file=cli.reading) == 0

    _, sent = _only_call()
    assert {k: v for k, v in sent.items() if k != "ts"} == {
        k: v for k, v in _example_payload().items() if k != "ts"
    }
    assert TELEGRAF_TIMESTAMP.fullmatch(sent["ts"]) is not None, sent["ts"]
    assert _TIMESTAMP.validate_python(sent["ts"]) == datetime(
        2026, 10, 18, 11, 59, 0, 250000, tzinfo=UTC
    )


def test_observed_at_now_rewrites_ts_to_a_current_telegraf_timestamp(cli) -> None:
    before = datetime.now(UTC)
    assert _run("--observed-at", "now", file=cli.reading) == 0
    after = datetime.now(UTC)

    _, sent = _only_call()
    assert {key: value for key, value in sent.items() if key != "ts"} == {
        key: value for key, value in _example_payload().items() if key != "ts"
    }
    match = TELEGRAF_TIMESTAMP.fullmatch(sent["ts"])
    assert match is not None, sent["ts"]
    assert not (match.group("fraction") or "")[6:].strip("0")
    observed_at = _TIMESTAMP.validate_python(sent["ts"])
    assert before.replace(microsecond=0) <= observed_at <= after


def test_the_default_file_is_the_example_reading(cli, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(ROOT)

    assert _run(file=None) == 0

    _, sent = _only_call()
    assert sent == _example_payload()


# --- Composition ------------------------------------------------------------------------------


@pytest.mark.parametrize(("v2_write", "legacy_write"), [(True, True), (True, False), (False, True)])
def test_write_switches_build_one_synchronous_raw_bucket_writer(
    cli, monkeypatch: pytest.MonkeyPatch, v2_write: bool, legacy_write: bool
) -> None:
    monkeypatch.setenv("TELEMETRY_V2_WRITE_ENABLED", str(v2_write).lower())
    monkeypatch.setenv("TELEMETRY_LEGACY_DUAL_WRITE_ENABLED", str(legacy_write).lower())

    assert _run(file=cli.reading) == 0

    assert len(FakeInfluxClient.instances) == 1
    client = FakeInfluxClient.instances[0]
    assert client.init_kwargs["url"] == "http://influx.test:8086"
    assert client.init_kwargs["token"] == TOKEN
    assert client.init_kwargs["org"] == "org_cli"
    assert len(client.write_api_calls) == 1
    options = client.write_api_calls[0]
    assert options is SYNCHRONOUS or options.write_type is WriteType.synchronous
    assert client.close_calls >= 1

    pipeline = FakePipeline.instances[0]
    service = pipeline.write_service
    assert isinstance(service, ObservationWriteService)
    assert service.v2_write_enabled is v2_write
    assert service.legacy_dual_write_enabled is legacy_write
    writer = service.writer if v2_write else service.legacy_writer
    assert isinstance(writer, InfluxObservationWriter)
    assert writer.write_api is client.write_api_instances[0]
    assert writer.bucket == "raw_cli"
    assert writer.org == "org_cli"
    if v2_write and legacy_write:
        assert service.legacy_writer is service.writer
    assert isinstance(service.site_assets, DynamoSiteAssetRepository)
    assert service.site_assets.client is cli.dynamo
    assert service.site_assets.table_name == "DomainCli"


def test_the_normalizer_uses_the_dynamodb_m1_repositories_and_a_utc_clock(cli) -> None:
    assert _run(file=cli.reading) == 0

    assert cli.boto_calls and {name for name, _ in cli.boto_calls} == {"dynamodb"}
    for _, kwargs in cli.boto_calls:
        assert kwargs.get("endpoint_url") == "http://dynamodb.test:8001"
        assert kwargs.get("region_name") == "sa-east-1"
    normalizer = FakePipeline.instances[0].normalizer
    assert isinstance(normalizer, TelemetryNormalizer)
    assert isinstance(normalizer.components, DynamoDeviceComponentRepository)
    assert isinstance(normalizer.deployments, DynamoDeploymentRepository)
    for repository in (normalizer.components, normalizer.deployments):
        assert repository.client is cli.dynamo
        assert repository.table_name == "DomainCli"
    now = normalizer.clock()
    assert now.tzinfo is not None and now.utcoffset().total_seconds() == 0
    assert abs((datetime.now(UTC) - now).total_seconds()) < 60
