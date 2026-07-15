from limnopulse_api.core.errors import NotFoundError
from limnopulse_api.domain.alert_events import AlertEvent
from limnopulse_api.domain.alerts import AuditContext
from limnopulse_api.repositories.alert_events import AlertEventRepository


class AlertEventService:
    def __init__(self, repository: AlertEventRepository) -> None:
        self.repository = repository

    async def list(self, tenant_id: str) -> list[AlertEvent]:
        return await self.repository.list_events(tenant_id)

    async def get(self, tenant_id: str, event_id: str) -> AlertEvent:
        event = await self.repository.get_event(tenant_id, event_id)
        if event is None:
            raise NotFoundError(f"Alert event {event_id} not found")
        return event

    async def acknowledge(
        self,
        tenant_id: str,
        event_id: str,
        expected_version: int,
        audit: AuditContext,
    ) -> AlertEvent:
        return await self.repository.acknowledge_event(
            tenant_id, event_id, expected_version, audit
        )

    async def resolve(
        self,
        tenant_id: str,
        event_id: str,
        expected_version: int,
        audit: AuditContext,
    ) -> AlertEvent:
        return await self.repository.resolve_event(tenant_id, event_id, expected_version, audit)
