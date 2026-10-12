"""Frozen M1 `/v2` repository protocols shared by every bounded-context adapter and service.

Write repositories expose `get_idempotency_replay`, which returns the stored response snapshot
for a live receipt with the same request hash, `None` when no live receipt exists, and raises
`ConflictError` when the same key was used for a different request.
"""

from datetime import datetime
from typing import Protocol, runtime_checkable

from limnopulse_api.domain.assets import (
    Asset,
    AssetStatus,
    AssetWithPondProfile,
    PondProfile,
    Site,
    SiteStatus,
)
from limnopulse_api.domain.deployments import Deployment
from limnopulse_api.domain.devices_v2 import (
    CapabilityDeclaration,
    ComponentStatus,
    DeviceComponent,
    DeviceStatus,
    DeviceV2,
)
from limnopulse_api.domain.integrations import DeviceIntegration, IntegrationAccount
from limnopulse_api.domain.v2_common import IdempotencyReceipt


@runtime_checkable
class SiteAssetReader(Protocol):
    async def get_site(self, tenant_id: str, site_id: str) -> Site | None: ...

    async def get_asset(self, tenant_id: str, asset_id: str) -> AssetWithPondProfile | None: ...


@runtime_checkable
class SiteAssetRepository(SiteAssetReader, Protocol):
    async def get_idempotency_replay(
        self, tenant_id: str, scope: str, key_hash: str, request_hash: str
    ) -> str | None: ...

    async def list_sites(self, tenant_id: str) -> list[Site]: ...

    async def create_site(self, site: Site, receipt: IdempotencyReceipt) -> Site: ...

    async def update_site(
        self,
        tenant_id: str,
        site_id: str,
        expected_version: int,
        *,
        name: str | None,
        status: SiteStatus | None,
    ) -> Site: ...

    async def list_assets(self, tenant_id: str) -> list[AssetWithPondProfile]: ...

    async def create_asset(
        self, asset: Asset, profile: PondProfile, receipt: IdempotencyReceipt
    ) -> AssetWithPondProfile: ...

    async def update_asset(
        self,
        tenant_id: str,
        asset_id: str,
        expected_version: int,
        *,
        name: str | None,
        status: AssetStatus | None,
        pond_description: str | None,
        pond_profile_expected_version: int | None,
    ) -> AssetWithPondProfile: ...


@runtime_checkable
class DeviceReader(Protocol):
    async def get_device(self, tenant_id: str, device_id: str) -> DeviceV2 | None: ...


@runtime_checkable
class ComponentReader(Protocol):
    async def get_component(self, tenant_id: str, component_id: str) -> DeviceComponent | None: ...


@runtime_checkable
class DeviceComponentRepository(DeviceReader, ComponentReader, Protocol):
    async def get_idempotency_replay(
        self, tenant_id: str, scope: str, key_hash: str, request_hash: str
    ) -> str | None: ...

    async def list_devices(self, tenant_id: str) -> list[DeviceV2]: ...

    async def create_device(self, device: DeviceV2, receipt: IdempotencyReceipt) -> DeviceV2: ...

    async def update_device(
        self,
        tenant_id: str,
        device_id: str,
        expected_version: int,
        *,
        name: str | None,
        firmware_version: str | None,
        status: DeviceStatus | None,
    ) -> DeviceV2: ...

    async def list_components(self, tenant_id: str, device_id: str) -> list[DeviceComponent]: ...

    async def create_component(
        self, component: DeviceComponent, receipt: IdempotencyReceipt
    ) -> DeviceComponent: ...

    async def update_component(
        self,
        tenant_id: str,
        component_id: str,
        expected_version: int,
        *,
        name: str | None,
        status: ComponentStatus | None,
    ) -> DeviceComponent: ...

    async def list_capabilities(
        self, tenant_id: str, component_id: str
    ) -> list[CapabilityDeclaration]: ...

    async def create_capability(
        self, declaration: CapabilityDeclaration, receipt: IdempotencyReceipt
    ) -> CapabilityDeclaration: ...


@runtime_checkable
class IntegrationRepository(Protocol):
    async def get_idempotency_replay(
        self, tenant_id: str, scope: str, key_hash: str, request_hash: str
    ) -> str | None: ...

    async def get_integration_account(
        self, tenant_id: str, integration_account_id: str
    ) -> IntegrationAccount | None: ...

    async def list_integration_accounts(self, tenant_id: str) -> list[IntegrationAccount]: ...

    async def create_integration_account(
        self, account: IntegrationAccount, receipt: IdempotencyReceipt
    ) -> IntegrationAccount: ...

    async def list_device_integrations(
        self, tenant_id: str, device_id: str
    ) -> list[DeviceIntegration]: ...

    async def create_device_integration(
        self, integration: DeviceIntegration, receipt: IdempotencyReceipt
    ) -> DeviceIntegration: ...


@runtime_checkable
class DeploymentRepository(Protocol):
    async def get_idempotency_replay(
        self, tenant_id: str, scope: str, key_hash: str, request_hash: str
    ) -> str | None: ...

    async def get_deployment(self, tenant_id: str, deployment_id: str) -> Deployment | None: ...

    async def list_component_deployments(
        self, tenant_id: str, component_id: str
    ) -> list[Deployment]: ...

    async def get_current_deployment(
        self, tenant_id: str, component_id: str
    ) -> Deployment | None: ...

    async def get_deployment_at(
        self, tenant_id: str, component_id: str, observed_at: datetime
    ) -> Deployment | None: ...

    async def start_deployment(
        self, deployment: Deployment, receipt: IdempotencyReceipt
    ) -> Deployment: ...

    async def end_deployment(
        self,
        tenant_id: str,
        deployment_id: str,
        *,
        expected_version: int,
        ended_at: datetime,
    ) -> Deployment: ...
