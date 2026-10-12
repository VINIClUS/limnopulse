from collections.abc import Callable
from datetime import datetime

from limnopulse_api.adapters.dynamodb_v2 import (
    canonical_request_hash,
    idempotency_key_hash,
    utc_now,
)
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.assets import (
    Asset,
    AssetStatus,
    AssetType,
    AssetWithPondProfile,
    PondProfile,
    SiteStatus,
    ensure_asset_transition,
)
from limnopulse_api.domain.ids import new_asset_id
from limnopulse_api.domain.v2_common import new_idempotency_receipt
from limnopulse_api.repositories.v2_contracts import SiteAssetRepository

ASSET_CREATE_SCOPE = "asset-create"


def asset_create_request_hash(tenant_id: str, body: dict[str, object]) -> str:
    """Hash of the client request only: scope, path parameters and the JSON body."""
    return canonical_request_hash(
        {"scope": ASSET_CREATE_SCOPE, "path": {"tenant_id": tenant_id}, "body": body}
    )


class AssetService:
    def __init__(
        self,
        repository: SiteAssetRepository,
        *,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.repository = repository
        self.clock = clock

    async def list(self, tenant_id: str) -> list[AssetWithPondProfile]:
        return await self.repository.list_assets(tenant_id)

    async def get(self, tenant_id: str, asset_id: str) -> AssetWithPondProfile:
        asset = await self.repository.get_asset(tenant_id, asset_id)
        if asset is None:
            raise NotFoundError("asset not found")
        return asset

    async def create(
        self,
        *,
        tenant_id: str,
        site_id: str,
        asset_type: AssetType | str,
        name: str,
        pond_description: str | None,
        idempotency_key: str,
    ) -> AssetWithPondProfile:
        resolved_type = AssetType(asset_type)
        key_hash = idempotency_key_hash(idempotency_key)
        request_hash = asset_create_request_hash(
            tenant_id,
            {
                "site_id": site_id,
                "asset_type": resolved_type.value,
                "name": name,
                "pond_description": pond_description,
            },
        )
        snapshot = await self.repository.get_idempotency_replay(
            tenant_id, ASSET_CREATE_SCOPE, key_hash, request_hash
        )
        if snapshot is not None:
            return AssetWithPondProfile.model_validate_json(snapshot)

        site = await self.repository.get_site(tenant_id, site_id)
        if site is None:
            raise NotFoundError("site not found")
        if site.status is not SiteStatus.ACTIVE:
            raise ConflictError("site is not active")

        now = self.clock()
        asset_id = new_asset_id()
        created = AssetWithPondProfile(
            asset=Asset(
                tenant_id=tenant_id,
                asset_id=asset_id,
                site_id=site_id,
                asset_type=resolved_type,
                name=name,
                status=AssetStatus.ACTIVE,
                created_at=now,
                updated_at=now,
                version=1,
            ),
            pond_profile=PondProfile(
                tenant_id=tenant_id,
                asset_id=asset_id,
                description=pond_description,
                created_at=now,
                updated_at=now,
                version=1,
            ),
        )
        receipt = new_idempotency_receipt(
            tenant_id=tenant_id,
            scope=ASSET_CREATE_SCOPE,
            key_hash=key_hash,
            request_hash=request_hash,
            response_snapshot_json=created.model_dump_json(),
            created_at=now,
        )
        return await self.repository.create_asset(created.asset, created.pond_profile, receipt)

    async def update(
        self,
        *,
        tenant_id: str,
        asset_id: str,
        expected_version: int,
        name: str | None,
        status: AssetStatus | str | None,
        pond_description: str | None,
        pond_profile_expected_version: int | None,
    ) -> AssetWithPondProfile:
        if (pond_description is None) != (pond_profile_expected_version is None):
            raise ValueError(
                "pond_description and pond_profile_expected_version must be sent together"
            )
        target = AssetStatus(status) if status is not None else None
        current = await self.get(tenant_id, asset_id)
        if current.asset.version != expected_version:
            raise ConflictError("asset version conflict")
        if (
            pond_profile_expected_version is not None
            and current.pond_profile.version != pond_profile_expected_version
        ):
            raise ConflictError("pond profile version conflict")
        if target is not None:
            ensure_asset_transition(current.asset.status, target)
        return await self.repository.update_asset(
            tenant_id,
            asset_id,
            expected_version,
            name=name,
            status=target,
            pond_description=pond_description,
            pond_profile_expected_version=pond_profile_expected_version,
        )
