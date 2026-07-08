from typing import Protocol

from limnopulse_api.domain.entities import Device, Membership, Pond, Tenant


class DomainRepository(Protocol):
    async def get_membership(self, cognito_sub: str, tenant_id: str) -> Membership | None:
        raise NotImplementedError

    async def list_memberships_for_user(self, cognito_sub: str) -> list[Membership]:
        raise NotImplementedError

    async def create_tenant_with_owner(self, tenant_id: str, name: str, owner_sub: str) -> Tenant:
        raise NotImplementedError

    async def list_tenants_for_memberships(self, memberships: list[Membership]) -> list[Tenant]:
        raise NotImplementedError

    async def get_tenant(self, tenant_id: str) -> Tenant | None:
        raise NotImplementedError

    async def update_tenant(
        self,
        tenant_id: str,
        expected_version: int,
        name: str | None,
    ) -> Tenant:
        raise NotImplementedError

    async def list_ponds(self, tenant_id: str) -> list[Pond]:
        raise NotImplementedError

    async def get_pond(self, tenant_id: str, pond_id: str) -> Pond | None:
        raise NotImplementedError

    async def create_pond(
        self,
        tenant_id: str,
        pond_id: str,
        name: str,
        description: str | None,
    ) -> Pond:
        raise NotImplementedError

    async def update_pond(
        self,
        tenant_id: str,
        pond_id: str,
        expected_version: int,
        name: str | None,
        description: str | None,
    ) -> Pond:
        raise NotImplementedError

    async def list_devices(self, tenant_id: str) -> list[Device]:
        raise NotImplementedError

    async def get_device(self, tenant_id: str, device_id: str) -> Device | None:
        raise NotImplementedError

    async def create_device(
        self,
        tenant_id: str,
        pond_id: str,
        device_id: str,
        name: str,
        firmware_version: str | None,
    ) -> Device:
        raise NotImplementedError

    async def update_device(
        self,
        tenant_id: str,
        device_id: str,
        expected_version: int,
        name: str | None,
        pond_id: str | None,
        firmware_version: str | None,
    ) -> Device:
        raise NotImplementedError
