from typing import Protocol

from limnopulse_api.domain.alert_events import AlertEvent
from limnopulse_api.domain.alerts import AuditContext


class AlertEventRepository(Protocol):
    async def list_events(self, tenant_id: str) -> list[AlertEvent]:
        raise NotImplementedError

    async def get_event(self, tenant_id: str, event_id: str) -> AlertEvent | None:
        raise NotImplementedError

    async def acknowledge_event(
        self,
        tenant_id: str,
        event_id: str,
        expected_version: int,
        audit: AuditContext,
    ) -> AlertEvent:
        raise NotImplementedError

    async def resolve_event(
        self,
        tenant_id: str,
        event_id: str,
        expected_version: int,
        audit: AuditContext,
    ) -> AlertEvent:
        raise NotImplementedError
