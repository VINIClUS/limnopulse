from limnopulse_api.domain.entities import Tenant
from limnopulse_api.domain.ids import new_tenant_id
from limnopulse_api.repositories.domain import DomainRepository


class TenantService:
    def __init__(self, repository: DomainRepository) -> None:
        self.repository = repository

    async def list_for_user(self, cognito_sub: str) -> list[Tenant]:
        memberships = await self.repository.list_memberships_for_user(cognito_sub)
        active_memberships = [membership for membership in memberships if membership.status == "active"]
        return await self.repository.list_tenants_for_memberships(active_memberships)

    async def create(self, name: str, owner_sub: str) -> Tenant:
        return await self.repository.create_tenant_with_owner(new_tenant_id(), name, owner_sub)

    async def get(self, tenant_id: str) -> Tenant | None:
        return await self.repository.get_tenant(tenant_id)

    async def update(self, tenant_id: str, expected_version: int, name: str | None) -> Tenant:
        return await self.repository.update_tenant(tenant_id, expected_version, name)
