"""Temporal Component-to-Asset Deployments.

A Deployment targets a `DeviceComponent` (never `Device.pond`) and places it on one tenant-owned
Asset for the half-open UTC interval `[started_at, ended_at)`. The Site is derived from the
Asset; clients never supply it. A new interval may start exactly at the previous end. Ended
intervals are immutable. Default Components of legacy `/v1` Devices are managed through `/v1`.

Parent status is checked twice: the service reads Component, Asset and Site for precise 404/409
answers, and the repository fences all three at commit time in the same transaction.
"""

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Annotated

from pydantic import AfterValidator, BaseModel, BeforeValidator, ConfigDict, Field

from limnopulse_api.adapters.dynamodb_v2 import (
    canonical_request_hash,
    idempotency_key_hash,
    utc_now,
)
from limnopulse_api.core.errors import ConflictError, LimnopulseError, NotFoundError
from limnopulse_api.domain.assets import AssetStatus, SiteStatus
from limnopulse_api.domain.deployments import Deployment, DeploymentStatus
from limnopulse_api.domain.devices_v2 import ComponentStatus, DeviceComponent
from limnopulse_api.domain.ids import (
    default_component_id_for_legacy_device,
    is_canonical_id,
    new_deployment_id,
    require_canonical_id,
)
from limnopulse_api.domain.v2_common import new_idempotency_receipt
from limnopulse_api.repositories.v2_contracts import (
    ComponentReader,
    DeploymentRepository,
    SiteAssetReader,
)
from limnopulse_api.services.devices_v2 import CreateOutcome, entity_snapshot_json

DEPLOYMENT_CREATE_SCOPE = "deployment-create"
LEGACY_COMPONENT_CONFLICT = "legacy default component deployments are managed through /v1"


class DeploymentInputError(LimnopulseError):
    """A well-formed request whose instants are invalid for the interval (HTTP 422)."""


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must include a UTC offset")
    try:
        return value.astimezone(UTC)
    except OverflowError as exc:
        raise ValueError("timestamp out of range") from exc


def _reject_epoch_numbers(value: object) -> object:
    if not isinstance(value, (str, datetime)):
        # Pydantic maps only ValueError to a 422; a TypeError would surface as a 500.
        raise ValueError("timestamp must be an ISO 8601 string")  # noqa: TRY004
    return value


AwareUtcDatetime = Annotated[
    datetime, BeforeValidator(_reject_epoch_numbers), AfterValidator(_aware_utc)
]


def _require_component_id(value: str) -> str:
    return require_canonical_id(value, "cmp")


def _require_asset_id(value: str) -> str:
    return require_canonical_id(value, "ast")


class DeploymentStartCommand(BaseModel):
    """`site_id` is derived from the Asset and therefore rejected (extra=forbid)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    component_id: Annotated[str, AfterValidator(_require_component_id)]
    asset_id: Annotated[str, AfterValidator(_require_asset_id)]
    started_at: AwareUtcDatetime


class DeploymentEndCommand(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    expected_version: int = Field(ge=1)
    ended_at: AwareUtcDatetime


def is_valid_deployment_id(deployment_id: str) -> bool:
    return is_canonical_id(deployment_id, "dep")


def is_legacy_default_component(component: DeviceComponent) -> bool:
    return component.component_id == default_component_id_for_legacy_device(
        component.tenant_id, component.device_id
    )


class DeploymentService:
    def __init__(
        self,
        deployments: DeploymentRepository,
        components: ComponentReader,
        sites_assets: SiteAssetReader,
        *,
        clock: Callable[[], datetime] = utc_now,
        id_factory: Callable[[], str] = new_deployment_id,
    ) -> None:
        self.deployments = deployments
        self.components = components
        self.sites_assets = sites_assets
        self.clock = clock
        self.id_factory = id_factory

    async def list(self, tenant_id: str, component_id: str) -> list[Deployment]:
        """History of one Component; requires the Component partition (no tenant-wide search)."""
        await self._require_component(tenant_id, component_id)
        return await self.deployments.list_component_deployments(tenant_id, component_id)

    async def get_current(self, tenant_id: str, component_id: str) -> Deployment | None:
        return await self.deployments.get_current_deployment(tenant_id, component_id)

    async def get_at(
        self, tenant_id: str, component_id: str, observed_at: datetime
    ) -> Deployment | None:
        return await self.deployments.get_deployment_at(
            tenant_id, component_id, _aware_utc(observed_at)
        )

    async def start(
        self, tenant_id: str, command: DeploymentStartCommand, idempotency_key: str
    ) -> CreateOutcome[Deployment]:
        self._require_not_future(command.started_at, "started_at")

        async def build() -> Deployment:
            component = await self._require_component(tenant_id, command.component_id)
            if component.status is not ComponentStatus.ACTIVE:
                raise ConflictError(f"component is {component.status}")
            if is_legacy_default_component(component):
                raise ConflictError(LEGACY_COMPONENT_CONFLICT)
            asset_with_profile = await self.sites_assets.get_asset(tenant_id, command.asset_id)
            if asset_with_profile is None:
                raise NotFoundError("asset not found")
            asset = asset_with_profile.asset
            if asset.tenant_id != tenant_id:
                raise NotFoundError("asset not found")
            if asset.status is not AssetStatus.ACTIVE:
                raise ConflictError(f"asset is {asset.status}")
            site = await self.sites_assets.get_site(tenant_id, asset.site_id)
            if site is None or site.tenant_id != tenant_id or site.status is not SiteStatus.ACTIVE:
                raise ConflictError("site is not active")
            if await self.deployments.get_current_deployment(tenant_id, component.component_id):
                raise ConflictError("component already has an active deployment")
            now = self.clock()
            return Deployment(
                tenant_id=tenant_id,
                deployment_id=self.id_factory(),
                component_id=component.component_id,
                site_id=site.site_id,
                asset_id=asset.asset_id,
                started_at=command.started_at,
                ended_at=None,
                status=DeploymentStatus.ACTIVE,
                created_at=now,
                updated_at=now,
                version=1,
            )

        key_hash = idempotency_key_hash(idempotency_key)
        request_hash = canonical_request_hash(
            {
                "scope": DEPLOYMENT_CREATE_SCOPE,
                "tenant_id": tenant_id,
                "body": command.model_dump(mode="json"),
            }
        )
        # A same-key/same-body receipt is resolved before parents and the current pointer, so a
        # valid replay stays a replay after the state advances.
        replay = await self._replay(tenant_id, key_hash, request_hash)
        if replay is not None:
            return replay
        try:
            deployment = await build()
            snapshot_json = entity_snapshot_json(deployment)
            receipt = new_idempotency_receipt(
                tenant_id=tenant_id,
                scope=DEPLOYMENT_CREATE_SCOPE,
                key_hash=key_hash,
                request_hash=request_hash,
                response_snapshot_json=snapshot_json,
                created_at=self.clock(),
            )
            created = await self.deployments.start_deployment(deployment, receipt)
        except ConflictError:
            # A concurrent same-key request may have won; its pointer or receipt then conflicts.
            replay = await self._replay(tenant_id, key_hash, request_hash)
            if replay is None:
                raise
            return replay
        return CreateOutcome(entity=created, snapshot_json=snapshot_json, replayed=False)

    async def end(
        self, tenant_id: str, deployment_id: str, command: DeploymentEndCommand
    ) -> Deployment:
        self._require_not_future(command.ended_at, "ended_at")
        current = None
        if is_valid_deployment_id(deployment_id):
            current = await self.deployments.get_deployment(tenant_id, deployment_id)
        if current is None:
            raise NotFoundError("deployment not found")
        component = await self.components.get_component(tenant_id, current.component_id)
        if component is not None and is_legacy_default_component(component):
            raise ConflictError(LEGACY_COMPONENT_CONFLICT)
        if current.status is not DeploymentStatus.ACTIVE:
            raise ConflictError("deployment is not active")
        if current.version != command.expected_version:
            raise ConflictError("deployment version conflict")
        if command.ended_at <= current.started_at:
            raise DeploymentInputError("ended_at must be after started_at")
        return await self.deployments.end_deployment(
            tenant_id,
            deployment_id,
            expected_version=command.expected_version,
            ended_at=command.ended_at,
        )

    async def _replay(
        self, tenant_id: str, key_hash: str, request_hash: str
    ) -> CreateOutcome[Deployment] | None:
        snapshot = await self.deployments.get_idempotency_replay(
            tenant_id, DEPLOYMENT_CREATE_SCOPE, key_hash, request_hash
        )
        if snapshot is None:
            return None
        return CreateOutcome(
            entity=Deployment.model_validate_json(snapshot),
            snapshot_json=snapshot,
            replayed=True,
        )

    def _require_not_future(self, value: datetime, field_name: str) -> None:
        if value > self.clock():
            raise DeploymentInputError(f"{field_name} must not be in the future")

    async def _require_component(self, tenant_id: str, component_id: str) -> DeviceComponent:
        component = None
        if is_canonical_id(component_id, "cmp"):
            component = await self.components.get_component(tenant_id, component_id)
        if component is None or component.tenant_id != tenant_id:
            raise NotFoundError("component not found")
        return component
