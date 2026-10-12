from datetime import UTC, datetime

import pytest

from limnopulse_api.adapters.dynamodb_v2 import canonical_request_hash, idempotency_key_hash
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.assets import (
    Asset,
    AssetStatus,
    AssetType,
    AssetWithPondProfile,
    PondProfile,
    Site,
    SiteStatus,
)
from limnopulse_api.domain.ids import default_site_id, is_canonical_id
from limnopulse_api.domain.v2_common import IdempotencyReceipt
from limnopulse_api.services.assets import ASSET_CREATE_SCOPE, AssetService
from limnopulse_api.services.sites import SITE_CREATE_SCOPE, SiteService

NOW = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)
SITE_ID = "site_" + "1" * 32


def fixed_clock() -> datetime:
    return NOW


def active_site(tenant_id: str, site_id: str, status: SiteStatus = SiteStatus.ACTIVE) -> Site:
    return Site(
        tenant_id=tenant_id,
        site_id=site_id,
        name="Farm",
        status=status,
        created_at=NOW,
        updated_at=NOW,
        version=1,
    )


def asset_with_profile(
    tenant_id: str,
    asset_id: str,
    *,
    status: AssetStatus = AssetStatus.ACTIVE,
    version: int = 1,
    profile_version: int = 1,
) -> AssetWithPondProfile:
    return AssetWithPondProfile(
        asset=Asset(
            tenant_id=tenant_id,
            asset_id=asset_id,
            site_id=SITE_ID,
            asset_type=AssetType.POND,
            name="North",
            status=status,
            created_at=NOW,
            updated_at=NOW,
            version=version,
        ),
        pond_profile=PondProfile(
            tenant_id=tenant_id,
            asset_id=asset_id,
            created_at=NOW,
            updated_at=NOW,
            version=profile_version,
        ),
    )


class FakeSiteAssetRepository:
    def __init__(self) -> None:
        self.sites: dict[tuple[str, str], Site] = {}
        self.assets: dict[tuple[str, str], AssetWithPondProfile] = {}
        self.receipts: dict[tuple[str, str, str], IdempotencyReceipt] = {}
        self.site_updates: list[dict] = []
        self.asset_updates: list[dict] = []

    @classmethod
    def with_archived_asset(cls, tenant_id: str, asset_id: str) -> "FakeSiteAssetRepository":
        repository = cls()
        repository.assets[(tenant_id, asset_id)] = asset_with_profile(
            tenant_id, asset_id, status=AssetStatus.ARCHIVED, version=2
        )
        return repository

    async def get_idempotency_replay(
        self, tenant_id: str, scope: str, key_hash: str, request_hash: str
    ) -> str | None:
        receipt = self.receipts.get((tenant_id, scope, key_hash))
        if receipt is None:
            return None
        if receipt.request_hash != request_hash:
            raise ConflictError("idempotency key reused with a different request")
        return receipt.response_snapshot_json

    async def get_site(self, tenant_id: str, site_id: str) -> Site | None:
        return self.sites.get((tenant_id, site_id))

    async def list_sites(self, tenant_id: str) -> list[Site]:
        return [site for (tenant, _), site in self.sites.items() if tenant == tenant_id]

    async def create_site(self, site: Site, receipt: IdempotencyReceipt) -> Site:
        self.sites[(site.tenant_id, site.site_id)] = site
        self.receipts[(receipt.tenant_id, receipt.scope, receipt.key_hash)] = receipt
        return site

    async def update_site(self, tenant_id, site_id, expected_version, *, name, status) -> Site:
        self.site_updates.append(
            {"expected_version": expected_version, "name": name, "status": status}
        )
        current = self.sites[(tenant_id, site_id)]
        return current.model_copy(
            update={
                "name": name or current.name,
                "status": status or current.status,
                "version": expected_version + 1,
            }
        )

    async def get_asset(self, tenant_id: str, asset_id: str) -> AssetWithPondProfile | None:
        return self.assets.get((tenant_id, asset_id))

    async def list_assets(self, tenant_id: str) -> list[AssetWithPondProfile]:
        return [asset for (tenant, _), asset in self.assets.items() if tenant == tenant_id]

    async def create_asset(
        self, asset: Asset, profile: PondProfile, receipt: IdempotencyReceipt
    ) -> AssetWithPondProfile:
        created = AssetWithPondProfile(asset=asset, pond_profile=profile)
        self.assets[(asset.tenant_id, asset.asset_id)] = created
        self.receipts[(receipt.tenant_id, receipt.scope, receipt.key_hash)] = receipt
        return created

    async def update_asset(self, tenant_id, asset_id, expected_version, **changes):
        self.asset_updates.append({"expected_version": expected_version, **changes})
        return self.assets[(tenant_id, asset_id)]


# --- assets -----------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_pond_asset_requires_active_site_in_same_tenant() -> None:
    repository = FakeSiteAssetRepository()
    repository.sites[("tnt_other", "site_1")] = active_site("tnt_other", "site_1")
    service = AssetService(repository, clock=fixed_clock)
    with pytest.raises(NotFoundError):
        await service.create(
            tenant_id="tnt_a",
            site_id="site_1",
            asset_type="pond",
            name="North pond",
            pond_description=None,
            idempotency_key="asset-create-01",
        )
    assert repository.assets == {}


@pytest.mark.asyncio
async def test_create_pond_asset_on_archived_site_conflicts() -> None:
    repository = FakeSiteAssetRepository()
    repository.sites[("tnt_a", SITE_ID)] = active_site("tnt_a", SITE_ID, SiteStatus.ARCHIVED)
    with pytest.raises(ConflictError):
        await AssetService(repository, clock=fixed_clock).create(
            tenant_id="tnt_a",
            site_id=SITE_ID,
            asset_type="pond",
            name="North pond",
            pond_description=None,
            idempotency_key="asset-create-01",
        )


@pytest.mark.asyncio
async def test_create_pond_asset_builds_canonical_asset_profile_and_receipt() -> None:
    repository = FakeSiteAssetRepository()
    repository.sites[("tnt_a", SITE_ID)] = active_site("tnt_a", SITE_ID)

    created = await AssetService(repository, clock=fixed_clock).create(
        tenant_id="tnt_a",
        site_id=SITE_ID,
        asset_type="pond",
        name="North pond",
        pond_description="covered",
        idempotency_key="asset-create-01",
    )

    assert is_canonical_id(created.asset.asset_id, "ast")
    assert created.pond_profile.asset_id == created.asset.asset_id
    assert created.pond_profile.description == "covered"
    assert created.pond_profile.legacy_pond_id is None
    assert created.asset.created_at == created.asset.updated_at == NOW
    assert (created.asset.version, created.pond_profile.version) == (1, 1)
    receipt = next(iter(repository.receipts.values()))
    assert receipt.scope == ASSET_CREATE_SCOPE == "asset-create"
    assert receipt.key_hash == idempotency_key_hash("asset-create-01")
    assert receipt.request_hash == canonical_request_hash(
        {
            "scope": "asset-create",
            "path": {"tenant_id": "tnt_a"},
            "body": {
                "site_id": SITE_ID,
                "asset_type": "pond",
                "name": "North pond",
                "pond_description": "covered",
            },
        }
    )
    assert AssetWithPondProfile.model_validate_json(receipt.response_snapshot_json) == created


@pytest.mark.asyncio
async def test_create_asset_replays_before_checking_site() -> None:
    repository = FakeSiteAssetRepository()
    repository.sites[("tnt_a", SITE_ID)] = active_site("tnt_a", SITE_ID)
    service = AssetService(repository, clock=fixed_clock)
    request = {
        "tenant_id": "tnt_a",
        "site_id": SITE_ID,
        "asset_type": "pond",
        "name": "North pond",
        "pond_description": None,
        "idempotency_key": "asset-create-01",
    }
    first = await service.create(**request)
    repository.sites[("tnt_a", SITE_ID)] = active_site("tnt_a", SITE_ID, SiteStatus.ARCHIVED)

    assert await service.create(**request) == first
    assert len(repository.assets) == 1
    with pytest.raises(ConflictError):
        await service.create(**{**request, "name": "Other"})


@pytest.mark.asyncio
async def test_archived_asset_cannot_return_to_active() -> None:
    repository = FakeSiteAssetRepository.with_archived_asset("tnt_a", "ast_1")
    with pytest.raises(ConflictError):
        await AssetService(repository, clock=fixed_clock).update(
            tenant_id="tnt_a",
            asset_id="ast_1",
            expected_version=2,
            name=None,
            status="active",
            pond_description=None,
            pond_profile_expected_version=None,
        )
    assert repository.asset_updates == []


@pytest.mark.asyncio
async def test_update_asset_missing_or_cross_tenant_is_not_found() -> None:
    repository = FakeSiteAssetRepository()
    repository.assets[("tnt_other", "ast_1")] = asset_with_profile("tnt_other", "ast_1")
    with pytest.raises(NotFoundError):
        await AssetService(repository, clock=fixed_clock).update(
            tenant_id="tnt_a",
            asset_id="ast_1",
            expected_version=1,
            name="East",
            status=None,
            pond_description=None,
            pond_profile_expected_version=None,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("expected_version", "profile_version"),
    [(3, None), (4, 1)],
)
async def test_update_asset_stale_versions_conflict(
    expected_version: int, profile_version: int | None
) -> None:
    repository = FakeSiteAssetRepository()
    repository.assets[("tnt_a", "ast_1")] = asset_with_profile(
        "tnt_a", "ast_1", version=4, profile_version=2
    )
    with pytest.raises(ConflictError):
        await AssetService(repository, clock=fixed_clock).update(
            tenant_id="tnt_a",
            asset_id="ast_1",
            expected_version=expected_version,
            name=None,
            status=None,
            pond_description=None if profile_version is None else "covered",
            pond_profile_expected_version=profile_version,
        )
    assert repository.asset_updates == []


@pytest.mark.asyncio
async def test_update_asset_rejects_unpaired_profile_fields() -> None:
    repository = FakeSiteAssetRepository()
    repository.assets[("tnt_a", "ast_1")] = asset_with_profile("tnt_a", "ast_1")
    with pytest.raises(ValueError):
        await AssetService(repository, clock=fixed_clock).update(
            tenant_id="tnt_a",
            asset_id="ast_1",
            expected_version=1,
            name=None,
            status=None,
            pond_description="covered",
            pond_profile_expected_version=None,
        )


@pytest.mark.asyncio
async def test_update_asset_archives_with_both_versions() -> None:
    repository = FakeSiteAssetRepository()
    repository.assets[("tnt_a", "ast_1")] = asset_with_profile(
        "tnt_a", "ast_1", version=4, profile_version=2
    )
    await AssetService(repository, clock=fixed_clock).update(
        tenant_id="tnt_a",
        asset_id="ast_1",
        expected_version=4,
        name="East",
        status="archived",
        pond_description="covered",
        pond_profile_expected_version=2,
    )
    assert repository.asset_updates == [
        {
            "expected_version": 4,
            "name": "East",
            "status": AssetStatus.ARCHIVED,
            "pond_description": "covered",
            "pond_profile_expected_version": 2,
        }
    ]


# --- sites ------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_site_generates_canonical_id_and_receipt_and_replays() -> None:
    repository = FakeSiteAssetRepository()
    service = SiteService(repository, clock=fixed_clock)

    created = await service.create(
        tenant_id="tnt_a", name="Farm", idempotency_key="site-create-01"
    )

    assert is_canonical_id(created.site_id, "site")
    assert created.status is SiteStatus.ACTIVE
    receipt = next(iter(repository.receipts.values()))
    assert receipt.scope == SITE_CREATE_SCOPE == "site-create"
    assert receipt.request_hash == canonical_request_hash(
        {"scope": "site-create", "path": {"tenant_id": "tnt_a"}, "body": {"name": "Farm"}}
    )
    assert await service.create(
        tenant_id="tnt_a", name="Farm", idempotency_key="site-create-01"
    ) == created
    assert len(repository.sites) == 1
    with pytest.raises(ConflictError):
        await service.create(tenant_id="tnt_a", name="Other", idempotency_key="site-create-01")


@pytest.mark.asyncio
async def test_archived_site_cannot_return_to_active() -> None:
    repository = FakeSiteAssetRepository()
    repository.sites[("tnt_a", SITE_ID)] = active_site("tnt_a", SITE_ID, SiteStatus.ARCHIVED)
    with pytest.raises(ConflictError):
        await SiteService(repository, clock=fixed_clock).update(
            tenant_id="tnt_a", site_id=SITE_ID, expected_version=1, name=None, status="active"
        )
    assert repository.site_updates == []


@pytest.mark.asyncio
async def test_default_site_cannot_be_archived() -> None:
    site_id = default_site_id("tnt_a")
    repository = FakeSiteAssetRepository()
    repository.sites[("tnt_a", site_id)] = active_site("tnt_a", site_id)
    with pytest.raises(ConflictError):
        await SiteService(repository, clock=fixed_clock).update(
            tenant_id="tnt_a", site_id=site_id, expected_version=1, name=None, status="archived"
        )
    assert repository.site_updates == []


@pytest.mark.asyncio
async def test_site_update_is_tenant_scoped_and_version_checked() -> None:
    repository = FakeSiteAssetRepository()
    repository.sites[("tnt_other", SITE_ID)] = active_site("tnt_other", SITE_ID)
    service = SiteService(repository, clock=fixed_clock)
    with pytest.raises(NotFoundError):
        await service.update(
            tenant_id="tnt_a", site_id=SITE_ID, expected_version=1, name="New", status=None
        )
    with pytest.raises(ConflictError):
        await service.update(
            tenant_id="tnt_other", site_id=SITE_ID, expected_version=2, name="New", status=None
        )

    updated = await service.update(
        tenant_id="tnt_other", site_id=SITE_ID, expected_version=1, name="New", status="archived"
    )
    assert updated.status is SiteStatus.ARCHIVED
    assert repository.site_updates == [
        {"expected_version": 1, "name": "New", "status": SiteStatus.ARCHIVED}
    ]
