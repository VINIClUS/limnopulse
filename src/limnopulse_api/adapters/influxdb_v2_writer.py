"""InfluxDB writer for canonical `observation_numeric` points and legacy `water_quality` points.

Lines come from the P2-00 encoders and are written with nanosecond precision, in chunks, off the
event loop. Every failure becomes `TelemetryWriteError`: throttling, server and transport errors
are retryable; any other 4xx (including a 422 partial write) is permanent. Logs and error
messages never carry payload values or the server's response body.
"""

import logging
from collections.abc import Sequence
from typing import Any

from anyio import to_thread
from influxdb_client import WritePrecision
from influxdb_client.client.write_api import WriteType
from influxdb_client.rest import ApiException
from urllib3.exceptions import HTTPError

from limnopulse_api.core.errors import TelemetryWriteError
from limnopulse_api.domain.telemetry_v2 import CanonicalObservation
from limnopulse_api.domain.telemetry_v2_schema import (
    LegacyPoint,
    encode_legacy_line,
    encode_observation_line,
)

_LOGGER = logging.getLogger(__name__)
_TOO_MANY_REQUESTS = 429
_SERVER_ERROR = 500


class InfluxObservationWriter:
    """Implements `ObservationWriter` and `LegacyObservationWriter`."""

    def __init__(
        self, *, write_api: Any, bucket: str, org: str, max_lines_per_request: int = 5000
    ) -> None:
        if max_lines_per_request < 1:
            raise ValueError("max_lines_per_request must be positive")
        # A batching or asynchronous WriteApi returns before the server answers and reports
        # errors only to a callback, so a failed write would be acknowledged as written.
        write_options = getattr(write_api, "_write_options", None)
        if write_options is not None and write_options.write_type is not WriteType.synchronous:
            raise ValueError("write_api must use synchronous write options")
        self.write_api = write_api
        self.bucket = bucket
        self.org = org
        self.max_lines_per_request = max_lines_per_request

    async def write_observations(self, observations: Sequence[CanonicalObservation]) -> None:
        await self._write([encode_observation_line(o) for o in observations])

    async def write_legacy_points(self, points: Sequence[LegacyPoint]) -> None:
        await self._write([encode_legacy_line(p) for p in points])

    async def _write(self, lines: list[str]) -> None:
        for start in range(0, len(lines), self.max_lines_per_request):
            chunk = lines[start : start + self.max_lines_per_request]
            error: TelemetryWriteError | None = None
            try:
                await to_thread.run_sync(self._write_chunk, chunk)
            except ApiException as exc:
                error = _classify_api_error(exc, len(chunk))
            except (HTTPError, OSError) as exc:
                error = TelemetryWriteError(
                    f"InfluxDB write failed in transport: {type(exc).__name__}", retryable=True
                )
            # Raised outside the handler so no exception context (which echoes the response
            # body and therefore payload values) is attached.
            if error is not None:
                raise error

    def _write_chunk(self, chunk: list[str]) -> None:
        self.write_api.write(
            bucket=self.bucket, org=self.org, record=chunk, write_precision=WritePrecision.NS
        )


def _classify_api_error(exc: ApiException, line_count: int) -> TelemetryWriteError:
    status = exc.status or 0
    if status == _TOO_MANY_REQUESTS or status >= _SERVER_ERROR:
        return TelemetryWriteError(f"InfluxDB write failed with HTTP {status}", retryable=True)
    # A 422 is a partial write (e.g. a field-type conflict): other lines may be stored, and a
    # retry cannot succeed. Log the status and size only; the body echoes payload values.
    _LOGGER.warning(
        "InfluxDB rejected a telemetry write (schema conflict or client error): status=%s lines=%s",
        status,
        line_count,
    )
    return TelemetryWriteError(f"InfluxDB rejected the write with HTTP {status}", retryable=False)
