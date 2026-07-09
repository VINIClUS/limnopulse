from datetime import datetime

from limnopulse_api.core.errors import NotFoundError
from limnopulse_api.domain.telemetry import TelemetryReading
from limnopulse_api.repositories.domain import DomainRepository
from limnopulse_api.repositories.telemetry import TelemetryRepository


class TelemetryService:
    def __init__(
        self,
        *,
        domain_repository: DomainRepository,
        telemetry_repository: TelemetryRepository,
    ) -> None:
        self.domain_repository = domain_repository
        self.telemetry_repository = telemetry_repository

    async def list_readings(
        self,
        tenant_id: str,
        pond_id: str,
        start: datetime,
        stop: datetime,
        limit: int,
        fields: tuple[str, ...],
    ) -> list[TelemetryReading]:
        await self._ensure_pond_exists(tenant_id, pond_id)
        return await self.telemetry_repository.list_readings(
            tenant_id=tenant_id,
            pond_id=pond_id,
            start=start,
            stop=stop,
            limit=limit,
            fields=fields,
        )

    async def latest_metrics(
        self,
        tenant_id: str,
        pond_id: str,
        start: datetime,
        stop: datetime,
        fields: tuple[str, ...],
    ) -> TelemetryReading | None:
        await self._ensure_pond_exists(tenant_id, pond_id)
        return await self.telemetry_repository.latest_metrics(
            tenant_id=tenant_id,
            pond_id=pond_id,
            start=start,
            stop=stop,
            fields=fields,
        )

    async def _ensure_pond_exists(self, tenant_id: str, pond_id: str) -> None:
        pond = await self.domain_repository.get_pond(tenant_id, pond_id)
        if pond is None:
            raise NotFoundError(f"Pond {pond_id} not found")
