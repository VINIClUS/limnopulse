from limnopulse_api.core.errors import NotFoundError
from limnopulse_api.domain.telemetry import LatestMetrics, TelemetryReading
from limnopulse_api.repositories.domain import DomainRepository
from limnopulse_api.repositories.telemetry import TelemetryRepository


class PondTelemetryService:
    def __init__(
        self,
        *,
        domain_repository: DomainRepository,
        telemetry_repository: TelemetryRepository,
    ) -> None:
        self.domain_repository = domain_repository
        self.telemetry_repository = telemetry_repository

    async def query_readings(
        self,
        *,
        tenant_id: str,
        pond_id: str,
        start: str,
        stop: str | None,
        limit: int,
    ) -> list[TelemetryReading]:
        await self._require_pond(tenant_id=tenant_id, pond_id=pond_id)
        return await self.telemetry_repository.query_readings(
            tenant_id=tenant_id,
            pond_id=pond_id,
            start=start,
            stop=stop,
            limit=limit,
        )

    async def query_latest_metrics(self, *, tenant_id: str, pond_id: str) -> LatestMetrics:
        await self._require_pond(tenant_id=tenant_id, pond_id=pond_id)
        return await self.telemetry_repository.query_latest_metrics(
            tenant_id=tenant_id,
            pond_id=pond_id,
        )

    async def _require_pond(self, *, tenant_id: str, pond_id: str) -> None:
        pond = await self.domain_repository.get_pond(tenant_id, pond_id)
        if pond is None:
            raise NotFoundError(f"Pond {pond_id} not found")
