"""IntegrationAccount and DeviceIntegration status changes (P3-04, ADR-001, D23).

The repository applies the domain transition rules and the optimistic version, and writes one
D23 audit item in the same transaction. The service stamps `updated_at` and, for a
DeviceIntegration, requires the path Device to own it: a DeviceIntegration of another Device is
404, never a transition.
"""

from collections.abc import Callable
from datetime import datetime
from typing import Protocol

from limnopulse_api.adapters.dynamodb_v2 import utc_now
from limnopulse_api.core.errors import NotFoundError
from limnopulse_api.domain.alerts import AuditContext
from limnopulse_api.domain.integrations import (
    DeviceIntegration,
    DeviceIntegrationStatus,
    IntegrationAccount,
    IntegrationAccountStatus,
)
from limnopulse_api.repositories.ingestion import IntegrationLifecycleRepository


class DeviceIntegrationLister(Protocol):
    async def list_device_integrations(
        self, tenant_id: str, device_id: str
    ) -> list[DeviceIntegration]: ...


class IntegrationLifecycleService:
    def __init__(
        self,
        repository: IntegrationLifecycleRepository,
        integrations: DeviceIntegrationLister,
        *,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.repository = repository
        self.integrations = integrations
        self.clock = clock

    async def transition_integration_account(
        self,
        tenant_id: str,
        integration_account_id: str,
        target: IntegrationAccountStatus,
        *,
        expected_version: int,
        audit: AuditContext,
    ) -> IntegrationAccount:
        return await self.repository.transition_integration_account(
            tenant_id,
            integration_account_id,
            target,
            expected_version=expected_version,
            updated_at=self.clock(),
            audit=audit,
        )

    async def transition_device_integration(
        self,
        tenant_id: str,
        device_id: str,
        device_integration_id: str,
        target: DeviceIntegrationStatus,
        *,
        expected_version: int,
        audit: AuditContext,
    ) -> DeviceIntegration:
        owned = await self.integrations.list_device_integrations(tenant_id, device_id)
        if not any(item.device_integration_id == device_integration_id for item in owned):
            raise NotFoundError("device integration not found")
        return await self.repository.transition_device_integration(
            tenant_id,
            device_integration_id,
            target,
            expected_version=expected_version,
            updated_at=self.clock(),
            audit=audit,
        )
