from typing import Protocol

from limnopulse_api.domain.alerts import AuditContext
from limnopulse_api.domain.notification_preferences import (
    EmailDeliverabilityRecord,
    NotificationPreference,
)


class NotificationPreferenceRepository(Protocol):
    async def get(
        self,
        tenant_id: str,
        cognito_sub: str,
    ) -> NotificationPreference | None:
        raise NotImplementedError

    async def get_email_deliverability(
        self,
        address: str,
    ) -> EmailDeliverabilityRecord | None:
        raise NotImplementedError

    async def save(
        self,
        preference: NotificationPreference,
        expected_version: int | None,
        audit: AuditContext,
        *,
        previous: NotificationPreference | None,
    ) -> NotificationPreference:
        raise NotImplementedError
