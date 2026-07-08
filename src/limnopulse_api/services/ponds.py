from limnopulse_api.domain.entities import Pond
from limnopulse_api.domain.ids import new_pond_id
from limnopulse_api.repositories.domain import DomainRepository


class PondService:
    def __init__(self, repository: DomainRepository) -> None:
        self.repository = repository

    async def list(self, tenant_id: str) -> list[Pond]:
        return await self.repository.list_ponds(tenant_id)

    async def get(self, tenant_id: str, pond_id: str) -> Pond | None:
        return await self.repository.get_pond(tenant_id, pond_id)

    async def create(self, tenant_id: str, name: str, description: str | None) -> Pond:
        return await self.repository.create_pond(tenant_id, new_pond_id(), name, description)

    async def update(
        self,
        tenant_id: str,
        pond_id: str,
        expected_version: int,
        name: str | None,
        description: str | None,
    ) -> Pond:
        return await self.repository.update_pond(
            tenant_id=tenant_id,
            pond_id=pond_id,
            expected_version=expected_version,
            name=name,
            description=description,
        )
