from datetime import datetime
from typing import Protocol

from limnopulse_api.domain.telemetry import TelemetryReading


class TelemetryRepository(Protocol):
    async def list_readings(
        self,
        tenant_id: str,
        pond_id: str,
        start: datetime,
        stop: datetime,
        limit: int,
        fields: tuple[str, ...],
    ) -> list[TelemetryReading]:
        raise NotImplementedError

    async def latest_metrics(
        self,
        tenant_id: str,
        pond_id: str,
        start: datetime,
        stop: datetime,
        fields: tuple[str, ...],
    ) -> TelemetryReading | None:
        raise NotImplementedError
