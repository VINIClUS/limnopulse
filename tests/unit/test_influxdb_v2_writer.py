"""Unit tests for the InfluxDB v2 observation writer (P2-03), against a fake `write_api`."""

import inspect
import logging
import threading
from datetime import UTC, datetime, timedelta

import pytest
from influxdb_client import WritePrecision
from influxdb_client.client.write_api import WriteApi, WriteOptions, WriteType
from influxdb_client.rest import ApiException
from urllib3.exceptions import (
    ConnectTimeoutError,
    HTTPError,
    MaxRetryError,
    NewConnectionError,
    ProtocolError,
    ReadTimeoutError,
)

from limnopulse_api.adapters.influxdb_v2_writer import InfluxObservationWriter
from limnopulse_api.core.errors import TelemetryWriteError
from limnopulse_api.domain.telemetry_v2_schema import LegacyPoint, build_legacy_points
from limnopulse_api.repositories.telemetry_v2 import LegacyObservationWriter, ObservationWriter
from tests.support.telemetry_v2_vectors import load_vectors, scenario

BUCKET = "raw$bucket"
ORG = "limnopulse-org"
SCENARIOS = tuple(load_vectors()["scenarios"])
LEGACY_SCENARIOS = tuple(name for name in SCENARIOS if scenario(name).legacy_lines)
PAYLOAD_MARKER = "PAYLOAD-MARKER-7f3a9c"
_WRITE_SIGNATURE = inspect.signature(WriteApi.write)


class FakeWriteApi:
    """Records each `write` call as normalized kwargs and the thread it ran on."""

    def __init__(self, failures: dict[int, BaseException] | None = None) -> None:
        self.calls: list[dict[str, object]] = []
        self.threads: list[int] = []
        self.failures = failures or {}

    def write(self, *args: object, **kwargs: object) -> None:
        bound = _WRITE_SIGNATURE.bind(self, *args, **kwargs)
        bound.apply_defaults()
        call = dict(bound.arguments)
        call.pop("self")
        call["lines"] = _record_lines(call["record"])
        self.calls.append(call)
        self.threads.append(threading.get_ident())
        failure = self.failures.get(len(self.calls))
        if failure is not None:
            raise failure

    @property
    def lines(self) -> list[str]:
        return [line for call in self.calls for line in call["lines"]]


def _record_lines(record: object) -> list[str]:
    if isinstance(record, bytes):
        record = record.decode("utf-8")
    if isinstance(record, str):
        return record.split("\n")
    assert isinstance(record, (list, tuple)), f"unexpected record type {type(record)!r}"
    lines: list[str] = []
    for item in record:
        if isinstance(item, bytes):
            item = item.decode("utf-8")
        assert isinstance(item, str), f"records must be line protocol, got {type(item)!r}"
        lines.extend(item.split("\n"))
    return lines


def make_writer(write_api: FakeWriteApi, **kwargs: object) -> InfluxObservationWriter:
    return InfluxObservationWriter(write_api=write_api, bucket=BUCKET, org=ORG, **kwargs)


def legacy_points(name: str) -> tuple[LegacyPoint, ...]:
    item = scenario(name)
    return build_legacy_points(item.observations, item.legacy_pond_by_asset)


def many_legacy_points(count: int) -> list[LegacyPoint]:
    start = datetime(2026, 7, 9, tzinfo=UTC)
    return [
        LegacyPoint(
            tenant_id="tnt_local_001",
            pond_id="pond_local_001",
            device_id="local-device-001",
            observed_at=start + timedelta(seconds=index),
            fields={"temp_c": 25.0, "seq": index},
        )
        for index in range(count)
    ]


# --- construction -----------------------------------------------------------------------------


def test_constructor_is_keyword_only_with_default_chunk_size() -> None:
    signature = inspect.signature(InfluxObservationWriter)
    params = signature.parameters
    assert list(params) == ["write_api", "bucket", "org", "max_lines_per_request"]
    assert all(p.kind is inspect.Parameter.KEYWORD_ONLY for p in params.values())
    assert params["max_lines_per_request"].default == 5000
    with pytest.raises(TypeError):
        InfluxObservationWriter(FakeWriteApi(), BUCKET, ORG)  # type: ignore[misc]


def test_writer_implements_both_writer_protocols() -> None:
    writer = make_writer(FakeWriteApi())
    assert isinstance(writer, ObservationWriter)
    assert isinstance(writer, LegacyObservationWriter)


# --- golden lines -----------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("name", SCENARIOS)
async def test_observation_lines_are_byte_exact_golden_vectors(name: str) -> None:
    item = scenario(name)
    write_api = FakeWriteApi()

    await make_writer(write_api).write_observations(item.observations)

    assert write_api.lines == list(item.v2_lines)
    for call in write_api.calls:
        assert call["bucket"] == BUCKET
        assert call["org"] == ORG
        assert call["write_precision"] == WritePrecision.NS


@pytest.mark.asyncio
@pytest.mark.parametrize("name", LEGACY_SCENARIOS)
async def test_legacy_lines_are_byte_exact_golden_vectors(name: str) -> None:
    write_api = FakeWriteApi()

    await make_writer(write_api).write_legacy_points(legacy_points(name))

    assert write_api.lines == list(scenario(name).legacy_lines)
    for call in write_api.calls:
        assert call["bucket"] == BUCKET
        assert call["org"] == ORG
        assert call["write_precision"] == WritePrecision.NS


@pytest.mark.asyncio
async def test_blocking_write_runs_off_the_event_loop_thread() -> None:
    write_api = FakeWriteApi()
    writer = make_writer(write_api)

    await writer.write_observations(scenario("legacy_sample").observations)
    await writer.write_legacy_points(legacy_points("legacy_sample"))

    loop_thread = threading.get_ident()
    assert len(write_api.threads) == 2
    assert all(thread != loop_thread for thread in write_api.threads)


# --- chunking ---------------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("max_lines", "sizes"),
    [(50, [50, 50, 20]), (120, [120]), (119, [119, 1]), (1000, [120])],
)
async def test_observation_requests_are_chunked_in_order(max_lines: int, sizes: list[int]) -> None:
    item = scenario("multi_point_series")
    assert len(item.v2_lines) == 120
    write_api = FakeWriteApi()

    await make_writer(write_api, max_lines_per_request=max_lines).write_observations(
        item.observations
    )

    assert [len(call["lines"]) for call in write_api.calls] == sizes
    assert write_api.lines == list(item.v2_lines)


@pytest.mark.asyncio
async def test_legacy_requests_are_chunked_in_order() -> None:
    item = scenario("multi_point_series")
    assert len(item.legacy_lines) == 60
    write_api = FakeWriteApi()

    await make_writer(write_api, max_lines_per_request=25).write_legacy_points(
        legacy_points("multi_point_series")
    )

    assert [len(call["lines"]) for call in write_api.calls] == [25, 25, 10]
    assert write_api.lines == list(item.legacy_lines)


@pytest.mark.asyncio
async def test_default_chunk_size_is_5000_lines() -> None:
    write_api = FakeWriteApi()

    await make_writer(write_api).write_legacy_points(many_legacy_points(5001))

    assert [len(call["lines"]) for call in write_api.calls] == [5000, 1]


# --- error classification ---------------------------------------------------------------------


def _api_exception(status: int) -> ApiException:
    error = ApiException(status=status, reason="error")
    error.body = f'{{"code":"error","message":"partial write: value={PAYLOAD_MARKER}"}}'
    return error


async def _write(writer: InfluxObservationWriter, method: str) -> None:
    if method == "observations":
        await writer.write_observations(scenario("legacy_sample").observations)
    else:
        await writer.write_legacy_points(legacy_points("legacy_sample"))


WRITE_METHODS = ("observations", "legacy")


@pytest.mark.asyncio
@pytest.mark.parametrize("method", WRITE_METHODS)
@pytest.mark.parametrize("status", [429, 500, 502, 503, 504, 599])
async def test_throttling_and_server_errors_are_retryable(method: str, status: int) -> None:
    writer = make_writer(FakeWriteApi({1: _api_exception(status)}))

    with pytest.raises(TelemetryWriteError) as raised:
        await _write(writer, method)

    assert raised.value.retryable is True


@pytest.mark.asyncio
@pytest.mark.parametrize("method", WRITE_METHODS)
@pytest.mark.parametrize("status", [400, 401, 403, 404, 413, 422])
async def test_client_errors_including_partial_write_are_permanent(
    method: str, status: int
) -> None:
    writer = make_writer(FakeWriteApi({1: _api_exception(status)}))

    with pytest.raises(TelemetryWriteError) as raised:
        await _write(writer, method)

    assert raised.value.retryable is False


@pytest.mark.asyncio
@pytest.mark.parametrize("method", WRITE_METHODS)
@pytest.mark.parametrize("status", [400, 401, 403, 404, 413, 422])
async def test_permanent_errors_log_a_schema_conflict_without_payload_values(
    caplog: pytest.LogCaptureFixture, method: str, status: int
) -> None:
    caplog.set_level(logging.DEBUG)
    writer = make_writer(FakeWriteApi({1: _api_exception(status)}))

    with pytest.raises(TelemetryWriteError) as raised:
        await _write(writer, method)

    entries = [
        record
        for record in caplog.records
        if record.levelno >= logging.WARNING and "schema" in record.getMessage().lower()
    ]
    assert entries, "a 4xx write must emit a schema-conflict log entry"
    for forbidden in (PAYLOAD_MARKER, "25.1", "3.82", "-67"):
        assert forbidden not in caplog.text
    assert PAYLOAD_MARKER not in str(raised.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [429, 503])
async def test_retryable_errors_leak_no_payload_values(
    caplog: pytest.LogCaptureFixture, status: int
) -> None:
    caplog.set_level(logging.DEBUG)
    writer = make_writer(FakeWriteApi({1: _api_exception(status)}))

    with pytest.raises(TelemetryWriteError) as raised:
        await _write(writer, "observations")

    assert PAYLOAD_MARKER not in caplog.text
    assert PAYLOAD_MARKER not in str(raised.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [ValueError("bad value"), TypeError("bug")], ids=repr)
async def test_unmapped_exceptions_are_never_classified_retryable(error: BaseException) -> None:
    """Only ApiException, urllib3 HTTPError and OSError are classified; a bug must not loop."""
    writer = make_writer(FakeWriteApi({1: error}))

    with pytest.raises(Exception) as raised:
        await _write(writer, "observations")

    assert not (isinstance(raised.value, TelemetryWriteError) and raised.value.retryable)


def _transport_errors() -> list[BaseException]:
    return [
        NewConnectionError(None, "connection refused"),  # type: ignore[arg-type]
        ConnectTimeoutError("connect timed out"),
        ReadTimeoutError(None, "/api/v2/write", "read timed out"),  # type: ignore[arg-type]
        MaxRetryError(None, "/api/v2/write", NewConnectionError(None, "refused")),  # type: ignore[arg-type]
        ProtocolError("connection aborted"),
        HTTPError("generic urllib3 failure"),
        ConnectionRefusedError(111, "Connection refused"),
        TimeoutError("timed out"),
        OSError("network unreachable"),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("method", WRITE_METHODS)
@pytest.mark.parametrize("error", _transport_errors(), ids=lambda error: type(error).__name__)
async def test_transport_errors_are_retryable(method: str, error: BaseException) -> None:
    writer = make_writer(FakeWriteApi({1: error}))

    with pytest.raises(TelemetryWriteError) as raised:
        await _write(writer, method)

    assert raised.value.retryable is True


@pytest.mark.asyncio
@pytest.mark.parametrize(("status", "retryable"), [(503, True), (422, False)])
async def test_a_failing_later_chunk_is_never_swallowed(status: int, retryable: bool) -> None:
    write_api = FakeWriteApi({2: _api_exception(status)})
    writer = make_writer(write_api, max_lines_per_request=3)

    with pytest.raises(TelemetryWriteError) as raised:
        await writer.write_observations(scenario("legacy_sample").observations)

    assert raised.value.retryable is retryable
    assert len(write_api.calls) >= 2


# --- added after approval (needs re-approval): review finding on non-synchronous write APIs --


class _OptionsWriteApi(FakeWriteApi):
    def __init__(self, write_type: WriteType) -> None:
        super().__init__()
        self._write_options = WriteOptions(write_type=write_type)


@pytest.mark.parametrize("write_type", [WriteType.batching, WriteType.asynchronous])
def test_non_synchronous_write_api_is_rejected(write_type: WriteType) -> None:
    """A batching/asynchronous WriteApi reports failures only to a callback: never swallow."""
    with pytest.raises(ValueError):
        make_writer(_OptionsWriteApi(write_type))


@pytest.mark.asyncio
async def test_synchronous_write_api_is_accepted() -> None:
    write_api = _OptionsWriteApi(WriteType.synchronous)

    await make_writer(write_api).write_legacy_points(legacy_points("legacy_sample"))

    assert write_api.lines == list(scenario("legacy_sample").legacy_lines)


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [_api_exception(422), _api_exception(503), OSError("reset")])
async def test_raised_error_carries_no_response_context(error: BaseException) -> None:
    writer = make_writer(FakeWriteApi({1: error}))

    with pytest.raises(TelemetryWriteError) as raised:
        await _write(writer, "observations")

    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


# --- added after approval (needs re-approval): Codex finding on status-zero ApiException -----


@pytest.mark.asyncio
@pytest.mark.parametrize("method", WRITE_METHODS)
async def test_status_zero_api_exception_is_a_retryable_transport_failure(
    caplog: pytest.LogCaptureFixture, method: str
) -> None:
    """influxdb-client 1.50 rethrows a urllib3 SSLError as ApiException(status=0)."""
    caplog.set_level(logging.DEBUG)
    error = ApiException(status=0, reason="SSLError\ncertificate verify failed")
    writer = make_writer(FakeWriteApi({1: error}))

    with pytest.raises(TelemetryWriteError) as raised:
        await _write(writer, method)

    assert raised.value.retryable is True
    assert not any("schema" in record.getMessage().lower() for record in caplog.records)


# --- added after approval (pre-approved by the orchestrator PR audit) -------------------------


class _AsyncWriteApi:
    async def write(self, *args: object, **kwargs: object) -> None:  # pragma: no cover
        raise AssertionError("never awaited by a synchronous writer")


def test_coroutine_write_api_is_rejected() -> None:
    """WriteApiAsync.write returns a coroutine the writer would never await: nothing written."""
    with pytest.raises(ValueError):
        InfluxObservationWriter(write_api=_AsyncWriteApi(), bucket=BUCKET, org=ORG)


@pytest.mark.asyncio
async def test_status_zero_log_is_not_a_schema_conflict(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    writer = make_writer(FakeWriteApi({1: ApiException(status=0, reason="SSLError\nEOF")}))

    with pytest.raises(TelemetryWriteError):
        await _write(writer, "observations")

    assert caplog.records
    assert all("schema" not in record.getMessage().lower() for record in caplog.records)
