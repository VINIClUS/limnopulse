from typing import Protocol

from limnopulse_api.domain.telemetry import LatestMetrics, TelemetryReading


class TelemetryRepository(Protocol):
    async def query_readings(
        self,
        *,
        tenant_id: str,
        pond_id: str,
        start: str,
        stop: str | None,
        limit: int,
    ) -> list[TelemetryReading]:
        raise NotImplementedError

    async def query_latest_metrics(self, *, tenant_id: str, pond_id: str) -> LatestMetrics:
        raise NotImplementedError
