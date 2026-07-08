from limnopulse_api.domain.entities import Device
from limnopulse_api.domain.ids import new_device_id
from limnopulse_api.repositories.domain import DomainRepository


class DeviceService:
    def __init__(self, repository: DomainRepository) -> None:
        self.repository = repository

    async def list(self, tenant_id: str) -> list[Device]:
        return await self.repository.list_devices(tenant_id)

    async def get(self, tenant_id: str, device_id: str) -> Device | None:
        return await self.repository.get_device(tenant_id, device_id)

    async def create(
        self,
        tenant_id: str,
        pond_id: str,
        name: str,
        firmware_version: str | None,
    ) -> Device:
        return await self.repository.create_device(
            tenant_id=tenant_id,
            pond_id=pond_id,
            device_id=new_device_id(),
            name=name,
            firmware_version=firmware_version,
        )

    async def update(
        self,
        tenant_id: str,
        device_id: str,
        expected_version: int,
        name: str | None,
        pond_id: str | None,
        firmware_version: str | None,
    ) -> Device:
        return await self.repository.update_device(
            tenant_id=tenant_id,
            device_id=device_id,
            expected_version=expected_version,
            name=name,
            pond_id=pond_id,
            firmware_version=firmware_version,
        )
