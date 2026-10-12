"""The `/v1` Pond/Device surface, served by the legacy repository or the v2 projection."""

from typing import Protocol, runtime_checkable

from limnopulse_api.domain.entities import Device, Pond


@runtime_checkable
class V1PondDeviceProjection(Protocol):
    """Exactly the Pond/Device subset of `DomainRepository`."""

    async def list_ponds(self, tenant_id: str) -> list[Pond]: ...

    async def get_pond(self, tenant_id: str, pond_id: str) -> Pond | None: ...

    async def create_pond(
        self,
        tenant_id: str,
        pond_id: str,
        name: str,
        description: str | None,
    ) -> Pond: ...

    async def update_pond(
        self,
        tenant_id: str,
        pond_id: str,
        expected_version: int,
        name: str | None,
        description: str | None,
    ) -> Pond: ...

    async def list_devices(self, tenant_id: str) -> list[Device]: ...

    async def get_device(self, tenant_id: str, device_id: str) -> Device | None: ...

    async def create_device(
        self,
        tenant_id: str,
        pond_id: str,
        device_id: str,
        name: str,
        firmware_version: str | None,
    ) -> Device: ...

    async def update_device(
        self,
        tenant_id: str,
        device_id: str,
        expected_version: int,
        name: str | None,
        pond_id: str | None,
        firmware_version: str | None,
    ) -> Device: ...
