"""DynamoDB persistence for the `/v2` Site and Asset/PondProfile bounded context.

Sites and Assets live in the tenant partition (`TENANT#<tenant>` / `SITE#`, `ASSET#`); the
PondProfile lives under `ASSET#<asset>/PROFILE#POND` and is read only after the tenant-scoped
Asset row proved ownership. An Asset migrated from a legacy `/v1` Pond keeps that Pond row in
sync: every Asset PATCH mirrors onto `TENANT#<tenant>/POND#<pond>` in the same transaction.
"""

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from limnopulse_api.adapters.dynamodb_v2 import (
    DynamoConditionalConflict,
    DynamoV2KeyBuilder,
    DynamoV2RepositoryBase,
    canonical_timestamp_key,
    parse_canonical_timestamp,
)
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
from limnopulse_api.domain.v2_common import IdempotencyReceipt

SITE_ENTITY_TYPE = "site"
ASSET_ENTITY_TYPE = "asset"
POND_PROFILE_ENTITY_TYPE = "pond_profile"

# Transaction action indexes used to classify a cancelled create.
_SITE_RECEIPT_INDEX = 1
_ASSET_RECEIPT_INDEX = 2
_ASSET_SITE_CHECK_INDEX = 3

_VERSION_FENCE = "attribute_exists(PK) AND #version = :expected_version"
_KEYS = DynamoV2KeyBuilder()


def _optional(item: dict[str, Any], name: str, value: Any) -> None:
    if value is not None:
        item[name] = value


def site_to_item(site: Site) -> dict[str, Any]:
    return {
        **_KEYS.site(site.tenant_id, site.site_id),
        "entity_type": SITE_ENTITY_TYPE,
        "tenant_id": site.tenant_id,
        "site_id": site.site_id,
        "name": site.name,
        "status": site.status.value,
        "schema_version": site.schema_version,
        "created_at": canonical_timestamp_key(site.created_at),
        "updated_at": canonical_timestamp_key(site.updated_at),
        "version": site.version,
    }


def site_from_item(item: Mapping[str, Any]) -> Site:
    return Site(
        tenant_id=item["tenant_id"],
        site_id=item["site_id"],
        name=item["name"],
        status=SiteStatus(item["status"]),
        created_at=parse_canonical_timestamp(item["created_at"]),
        updated_at=parse_canonical_timestamp(item["updated_at"]),
        version=int(item["version"]),
    )


def asset_to_item(asset: Asset) -> dict[str, Any]:
    return {
        **_KEYS.asset(asset.tenant_id, asset.asset_id),
        "entity_type": ASSET_ENTITY_TYPE,
        "tenant_id": asset.tenant_id,
        "asset_id": asset.asset_id,
        "site_id": asset.site_id,
        "asset_type": asset.asset_type.value,
        "name": asset.name,
        "status": asset.status.value,
        "schema_version": asset.schema_version,
        "created_at": canonical_timestamp_key(asset.created_at),
        "updated_at": canonical_timestamp_key(asset.updated_at),
        "version": asset.version,
    }


def asset_from_item(item: Mapping[str, Any]) -> Asset:
    return Asset(
        tenant_id=item["tenant_id"],
        asset_id=item["asset_id"],
        site_id=item["site_id"],
        asset_type=AssetType(item["asset_type"]),
        name=item["name"],
        status=AssetStatus(item["status"]),
        created_at=parse_canonical_timestamp(item["created_at"]),
        updated_at=parse_canonical_timestamp(item["updated_at"]),
        version=int(item["version"]),
    )


def pond_profile_to_item(profile: PondProfile) -> dict[str, Any]:
    item: dict[str, Any] = {
        **_KEYS.pond_profile(profile.asset_id),
        "entity_type": POND_PROFILE_ENTITY_TYPE,
        "tenant_id": profile.tenant_id,
        "asset_id": profile.asset_id,
        "schema_version": profile.schema_version,
        "created_at": canonical_timestamp_key(profile.created_at),
        "updated_at": canonical_timestamp_key(profile.updated_at),
        "version": profile.version,
    }
    _optional(item, "description", profile.description)
    _optional(item, "legacy_pond_id", profile.legacy_pond_id)
    return item


def pond_profile_from_item(item: Mapping[str, Any]) -> PondProfile:
    return PondProfile(
        tenant_id=item["tenant_id"],
        asset_id=item["asset_id"],
        description=item.get("description"),
        legacy_pond_id=item.get("legacy_pond_id"),
        created_at=parse_canonical_timestamp(item["created_at"]),
        updated_at=parse_canonical_timestamp(item["updated_at"]),
        version=int(item["version"]),
    )


class DynamoSiteAssetRepository(DynamoV2RepositoryBase):
    # Reads ---------------------------------------------------------------------------------

    async def get_site(self, tenant_id: str, site_id: str) -> Site | None:
        item = await self.get_item(self.keys.site(tenant_id, site_id), consistent=True)
        if item is None or item.get("tenant_id") != tenant_id:
            return None
        return site_from_item(item)

    async def list_sites(self, tenant_id: str) -> list[Site]:
        items = await self.query_prefix(
            self.keys.tenant_partition(tenant_id), self.keys.SITE_PREFIX
        )
        return [site_from_item(item) for item in items if item.get("tenant_id") == tenant_id]

    async def get_asset(self, tenant_id: str, asset_id: str) -> AssetWithPondProfile | None:
        item = await self.get_item(self.keys.asset(tenant_id, asset_id), consistent=True)
        if item is None or item.get("tenant_id") != tenant_id:
            return None
        return await self._with_profile(tenant_id, asset_from_item(item), consistent=True)

    async def list_assets(self, tenant_id: str) -> list[AssetWithPondProfile]:
        items = await self.query_prefix(
            self.keys.tenant_partition(tenant_id), self.keys.ASSET_PREFIX
        )
        assets: list[AssetWithPondProfile] = []
        for item in items:
            if item.get("tenant_id") != tenant_id:
                continue
            combined = await self._with_profile(tenant_id, asset_from_item(item))
            if combined is not None:
                assets.append(combined)
        return assets

    # Writes --------------------------------------------------------------------------------

    async def create_site(self, site: Site, receipt: IdempotencyReceipt) -> Site:
        actions = [
            {"Put": self.put_new_request(site_to_item(site))},
            {"Put": self.receipt_put_request(receipt)},
        ]
        try:
            await self.transact_write(actions)
        except DynamoConditionalConflict as exc:
            if _SITE_RECEIPT_INDEX in exc.failed_indexes:
                return Site.model_validate_json(await self._replay_after_conflict(exc, receipt))
            raise ConflictError("site already exists") from exc
        return site

    async def update_site(
        self,
        tenant_id: str,
        site_id: str,
        expected_version: int,
        *,
        name: str | None,
        status: SiteStatus | None,
    ) -> Site:
        assignments, names, values = self._versioned_set(expected_version)
        if name is not None:
            self._assign(assignments, names, values, "name", name)
        if status is not None:
            self._assign(assignments, names, values, "status", status.value)
        item = await self.update_item(
            self.update_request(
                self.keys.site(tenant_id, site_id),
                update_expression="SET " + ", ".join(assignments),
                condition=_VERSION_FENCE,
                names=names,
                values=values,
                return_values="ALL_NEW",
            )
        )
        return site_from_item(item)

    async def create_asset(
        self, asset: Asset, profile: PondProfile, receipt: IdempotencyReceipt
    ) -> AssetWithPondProfile:
        created = AssetWithPondProfile(asset=asset, pond_profile=profile)
        actions = [
            {"Put": self.put_new_request(asset_to_item(asset))},
            {"Put": self.put_new_request(pond_profile_to_item(profile))},
            {"Put": self.receipt_put_request(receipt)},
            {
                # The Site may have been archived after the service checked it.
                "ConditionCheck": self.condition_check_request(
                    self.keys.site(asset.tenant_id, asset.site_id),
                    condition="attribute_exists(PK) AND #status = :active",
                    names={"#status": "status"},
                    values={":active": SiteStatus.ACTIVE.value},
                )
            },
        ]
        try:
            await self.transact_write(actions)
        except DynamoConditionalConflict as exc:
            if _ASSET_RECEIPT_INDEX in exc.failed_indexes:
                snapshot = await self._replay_after_conflict(exc, receipt)
                return AssetWithPondProfile.model_validate_json(snapshot)
            if _ASSET_SITE_CHECK_INDEX in exc.failed_indexes:
                raise ConflictError("site is not active") from exc
            raise ConflictError("asset already exists") from exc
        return created

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
    ) -> AssetWithPondProfile:
        if (pond_description is None) != (pond_profile_expected_version is None):
            raise ValueError(
                "pond_description and pond_profile_expected_version must be sent together"
            )
        current = await self.get_asset(tenant_id, asset_id)
        if current is None:
            raise NotFoundError("asset not found")
        asset, profile = current.asset, current.pond_profile
        now = self.clock()

        assignments, names, values = self._versioned_set(expected_version, now=now)
        if name is not None:
            self._assign(assignments, names, values, "name", name)
        if status is not None:
            self._assign(assignments, names, values, "status", status.value)
        actions: list[dict[str, Any]] = [
            {
                "Update": self.update_request(
                    self.keys.asset(tenant_id, asset_id),
                    update_expression="SET " + ", ".join(assignments),
                    condition=_VERSION_FENCE,
                    names=names,
                    values=values,
                )
            }
        ]
        updated_asset = Asset.model_validate(
            {
                **asset.model_dump(),
                "name": name if name is not None else asset.name,
                "status": status if status is not None else asset.status,
                "updated_at": now,
                "version": expected_version + 1,
            }
        )

        updated_profile = profile
        if pond_description is not None and pond_profile_expected_version is not None:
            assignments, names, values = self._versioned_set(pond_profile_expected_version, now=now)
            self._assign(assignments, names, values, "description", pond_description)
            actions.append(
                {
                    "Update": self.update_request(
                        self.keys.pond_profile(asset_id),
                        update_expression="SET " + ", ".join(assignments),
                        condition=f"{_VERSION_FENCE} AND #tenant_id = :tenant_id",
                        names={**names, "#tenant_id": "tenant_id"},
                        values={**values, ":tenant_id": tenant_id},
                    )
                }
            )
            updated_profile = PondProfile.model_validate(
                {
                    **profile.model_dump(),
                    "description": pond_description,
                    "updated_at": now,
                    "version": pond_profile_expected_version + 1,
                }
            )

        if profile.legacy_pond_id is not None:
            actions.append(
                await self._legacy_pond_mirror(
                    tenant_id,
                    profile.legacy_pond_id,
                    name=name,
                    description=pond_description,
                    archived=status is AssetStatus.ARCHIVED,
                )
            )

        await self.transact_write(actions)
        return AssetWithPondProfile(asset=updated_asset, pond_profile=updated_profile)

    # Internals -----------------------------------------------------------------------------

    async def _with_profile(
        self, tenant_id: str, asset: Asset, *, consistent: bool = False
    ) -> AssetWithPondProfile | None:
        item = await self.get_item(self.keys.pond_profile(asset.asset_id), consistent=consistent)
        if item is None or item.get("tenant_id") != tenant_id:
            return None
        profile = pond_profile_from_item(item)
        if profile.asset_id != asset.asset_id:
            return None
        return AssetWithPondProfile(asset=asset, pond_profile=profile)

    async def _replay_after_conflict(
        self, exc: DynamoConditionalConflict, receipt: IdempotencyReceipt
    ) -> str:
        """Snapshot of the live receipt that won the race for the same request, else a conflict."""
        snapshot = await self.get_idempotency_replay(
            receipt.tenant_id, receipt.scope, receipt.key_hash, receipt.request_hash
        )
        if snapshot is None:
            raise ConflictError("idempotency key is being used by a concurrent request") from exc
        return snapshot

    async def _legacy_pond_mirror(
        self,
        tenant_id: str,
        pond_id: str,
        *,
        name: str | None,
        description: str | None,
        archived: bool,
    ) -> dict[str, Any]:
        """SET-only mirror onto the linked legacy Pond row, fenced on its observed version."""
        key = self.keys.legacy_pond(tenant_id, pond_id)
        legacy = await self.get_item(key, consistent=True)
        if legacy is None:
            raise ConflictError("linked legacy pond is missing")
        observed_version = int(legacy["version"])
        legacy_updated_at = self.clock().astimezone(UTC).replace(microsecond=0).isoformat()
        assignments, names, values = self._versioned_set(
            observed_version, updated_at=legacy_updated_at
        )
        if name is not None:
            self._assign(assignments, names, values, "name", name)
        if description is not None:
            self._assign(assignments, names, values, "description", description)
        if archived:
            self._assign(assignments, names, values, "status", AssetStatus.ARCHIVED.value)
        return {
            "Update": self.update_request(
                key,
                update_expression="SET " + ", ".join(assignments),
                condition=_VERSION_FENCE,
                names=names,
                values=values,
            )
        }

    def _versioned_set(
        self,
        expected_version: int,
        *,
        now: datetime | None = None,
        updated_at: str | None = None,
    ) -> tuple[list[str], dict[str, str], dict[str, Any]]:
        if updated_at is None:
            updated_at = canonical_timestamp_key(now if now is not None else self.clock())
        return (
            ["#updated_at = :updated_at", "#version = :next_version"],
            {"#updated_at": "updated_at", "#version": "version"},
            {
                ":updated_at": updated_at,
                ":next_version": expected_version + 1,
                ":expected_version": expected_version,
            },
        )

    @staticmethod
    def _assign(
        assignments: list[str],
        names: dict[str, str],
        values: dict[str, Any],
        attribute: str,
        value: Any,
    ) -> None:
        assignments.append(f"#{attribute} = :{attribute}")
        names[f"#{attribute}"] = attribute
        values[f":{attribute}"] = value
