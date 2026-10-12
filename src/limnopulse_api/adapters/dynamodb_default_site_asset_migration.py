"""DynamoDB access for the M1 default Site/Asset and legacy Device backfill.

The migration reads only explicitly named tenants: a consistent `GetItem` of
`TENANT#<t>/META` and consistent, `Limit`-paginated `Query` calls over the `POND#` and
`DEVICE#` prefixes of that tenant partition, plus `GetItem` of known deterministic v2 keys.
There is no Scan and no tenant discovery.

Legacy rows are parsed here with their own allowlist (the `/v1` adapter parsers are private).
Native v2 Device rows that share the `DEVICE#` prefix (no `pond_id`, or `schema_version=2`) are
not legacy and are skipped by the caller.

Compatibility links on legacy rows are SET-only and never bump `version`/`updated_at`:
`v2_site_id`, `v2_asset_id`, `v2_projection_version` on `POND#`; `default_component_id`,
`v2_projection_version` on the Device tenant row and its `DEVICE#<d>/META` lookup. Each link
update is fenced on the observed `version` (and `pond_id` for Devices) so a concurrent `/v1`
edit makes the migration re-plan that record instead of overwriting it.
"""

from collections.abc import Iterator, Mapping
from datetime import datetime
from decimal import Decimal
from typing import Any

from limnopulse_api.adapters.dynamodb_deployments import deployment_to_item
from limnopulse_api.adapters.dynamodb_v2 import DynamoV2RepositoryBase, canonical_timestamp_key
from limnopulse_api.domain.deployments import Deployment, DeploymentStatus
from limnopulse_api.domain.entities import Device, Pond, Tenant

V2_SITE_ID_ATTRIBUTE = "v2_site_id"
V2_ASSET_ID_ATTRIBUTE = "v2_asset_id"
DEFAULT_COMPONENT_ID_ATTRIBUTE = "default_component_id"
V2_PROJECTION_VERSION_ATTRIBUTE = "v2_projection_version"
V2_PROJECTION_VERSION = 1

MIGRATION_RECEIPT_ENTITY_TYPE = "migration_receipt"
MIGRATION_NAME = "M1_DEFAULT_SITE_ASSET_DEVICE"

LEGACY_TENANT_SK = "META"
LEGACY_POND_PREFIX = "POND#"
LEGACY_DEVICE_PREFIX = "DEVICE#"


def key_label(key: Mapping[str, str]) -> str:
    """`PK/SK` label of a key; keys hold identifiers only, never names."""
    return f"{key['PK']}/{key['SK']}"


def is_legacy_device_item(item: Mapping[str, Any]) -> bool:
    """Only native v2 rows (`schema_version == 2`) are skipped; every other `DEVICE#` row is
    legacy and must parse, so a malformed schema-v1 row is reported instead of ignored."""
    return item.get("schema_version") != 2


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise TypeError("legacy timestamp must be a string")
    return datetime.fromisoformat(value)


def stored_int(value: Any) -> int:
    """A stored integer (int or integral Decimal); never a bool, string or fraction.

    Every stored version, seq or schema_version the migration reads goes through this.
    """
    if isinstance(value, bool):
        raise TypeError("stored integer must not be a boolean")
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal) and value == value.to_integral_value():
        return int(value)
    raise TypeError("stored integer has an invalid type")


def legacy_tenant_from_item(item: Mapping[str, Any]) -> Tenant:
    return Tenant(
        tenant_id=item["tenant_id"],
        name=item["name"],
        settings=item.get("settings") or {},
        status=item.get("status", "active"),
        created_at=_timestamp(item["created_at"]),
        updated_at=_timestamp(item["updated_at"]),
        version=stored_int(item["version"]),
        schema_version=stored_int(item.get("schema_version", 1)),
    )


def legacy_pond_from_item(item: Mapping[str, Any]) -> Pond:
    return Pond(
        tenant_id=item["tenant_id"],
        pond_id=item["pond_id"],
        name=item["name"],
        description=item.get("description"),
        status=item.get("status", "active"),
        created_at=_timestamp(item["created_at"]),
        updated_at=_timestamp(item["updated_at"]),
        version=stored_int(item["version"]),
        schema_version=stored_int(item.get("schema_version", 1)),
    )


def legacy_device_from_item(item: Mapping[str, Any]) -> Device:
    return Device(
        tenant_id=item["tenant_id"],
        pond_id=item["pond_id"],
        device_id=item["device_id"],
        name=item["name"],
        auth_type=item.get("auth_type", "mtls"),
        firmware_version=item.get("firmware_version"),
        status=item.get("status", "active"),
        created_at=_timestamp(item["created_at"]),
        updated_at=_timestamp(item["updated_at"]),
        version=stored_int(item["version"]),
        schema_version=stored_int(item.get("schema_version", 1)),
    )


class DynamoDefaultSiteAssetMigrationStore(DynamoV2RepositoryBase):
    """Synchronous reads and conditional write builders used by `DefaultSiteAssetMigrator`."""

    # Reads ---------------------------------------------------------------------------------

    def get_tenant_item(self, tenant_id: str) -> dict[str, Any] | None:
        return self.get_item_sync(self.tenant_meta_key(tenant_id), consistent=True)

    def get_known_item(self, key: Mapping[str, str]) -> dict[str, Any] | None:
        return self.get_item_sync(key, consistent=True)

    def query_tenant_prefix_pages(
        self, tenant_id: str, sk_prefix: str, *, page_size: int
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield one deserialized page per `Query` call so callers can check deadlines."""
        values = self.serialize_values(
            {":pk": self.keys.tenant_partition(tenant_id), ":sk_prefix": sk_prefix}
        )
        last_evaluated_key: dict[str, Any] | None = None
        while True:
            request: dict[str, Any] = {
                "TableName": self.table_name,
                "KeyConditionExpression": "PK = :pk AND begins_with(SK, :sk_prefix)",
                "ExpressionAttributeValues": values,
                "ConsistentRead": True,
                "Limit": page_size,
            }
            if last_evaluated_key is not None:
                request["ExclusiveStartKey"] = last_evaluated_key
            response = self.client.query(**request)
            yield [self.deserialize_item(item) for item in response.get("Items", [])]
            last_evaluated_key = response.get("LastEvaluatedKey")
            if last_evaluated_key is None:
                return

    # Keys ----------------------------------------------------------------------------------

    def tenant_meta_key(self, tenant_id: str) -> dict[str, str]:
        return {"PK": self.keys.tenant_partition(tenant_id), "SK": LEGACY_TENANT_SK}

    # Request builders ----------------------------------------------------------------------

    def pond_link_request(
        self, tenant_id: str, pond_id: str, *, site_id: str, asset_id: str, observed_version: int
    ) -> dict[str, Any]:
        return self.update_request(
            self.keys.legacy_pond(tenant_id, pond_id),
            update_expression=(
                "SET #v2_site_id = :v2_site_id, #v2_asset_id = :v2_asset_id, "
                "#v2_projection_version = :v2_projection_version"
            ),
            condition=(
                "attribute_exists(PK) AND #version = :observed_version AND #tenant_id = :tenant_id"
            ),
            names={
                "#v2_site_id": V2_SITE_ID_ATTRIBUTE,
                "#v2_asset_id": V2_ASSET_ID_ATTRIBUTE,
                "#v2_projection_version": V2_PROJECTION_VERSION_ATTRIBUTE,
                "#version": "version",
                "#tenant_id": "tenant_id",
            },
            values={
                ":v2_site_id": site_id,
                ":v2_asset_id": asset_id,
                ":v2_projection_version": V2_PROJECTION_VERSION,
                ":observed_version": observed_version,
                ":tenant_id": tenant_id,
            },
        )

    def device_link_request(
        self,
        key: Mapping[str, str],
        *,
        tenant_id: str,
        pond_id: str,
        component_id: str,
        observed_version: int,
    ) -> dict[str, Any]:
        """Link update for the Device tenant row or its lookup, fenced on version and Pond."""
        return self.update_request(
            key,
            update_expression=(
                "SET #default_component_id = :default_component_id, "
                "#v2_projection_version = :v2_projection_version"
            ),
            condition=(
                "attribute_exists(PK) AND #version = :observed_version "
                "AND #pond_id = :pond_id AND #tenant_id = :tenant_id"
            ),
            names={
                "#default_component_id": DEFAULT_COMPONENT_ID_ATTRIBUTE,
                "#v2_projection_version": V2_PROJECTION_VERSION_ATTRIBUTE,
                "#version": "version",
                "#pond_id": "pond_id",
                "#tenant_id": "tenant_id",
            },
            values={
                ":default_component_id": component_id,
                ":v2_projection_version": V2_PROJECTION_VERSION,
                ":observed_version": observed_version,
                ":pond_id": pond_id,
                ":tenant_id": tenant_id,
            },
        )

    def fence_check_request(
        self,
        key: Mapping[str, str],
        *,
        tenant_id: str,
        observed_version: int | None = None,
        require_active: bool = False,
    ) -> dict[str, Any]:
        """Commit-time check that a row still exists for the tenant (at the observed version,
        and ACTIVE when required)."""
        condition = "attribute_exists(PK) AND #tenant_id = :tenant_id"
        names = {"#tenant_id": "tenant_id"}
        values: dict[str, Any] = {":tenant_id": tenant_id}
        if observed_version is not None:
            condition += " AND #version = :observed_version"
            names["#version"] = "version"
            values[":observed_version"] = observed_version
        if require_active:
            condition += " AND #status = :active"
            names["#status"] = "status"
            values[":active"] = "active"
        return self.condition_check_request(key, condition=condition, names=names, values=values)

    def device_fence_check_request(
        self, key: Mapping[str, str], *, tenant_id: str, pond_id: str, observed_version: int
    ) -> dict[str, Any]:
        """Fence a Device tenant row or lookup on the version and Pond the plan observed."""
        return self.condition_check_request(
            key,
            condition=(
                "attribute_exists(PK) AND #version = :observed_version "
                "AND #pond_id = :pond_id AND #tenant_id = :tenant_id"
            ),
            names={"#version": "version", "#pond_id": "pond_id", "#tenant_id": "tenant_id"},
            values={
                ":observed_version": observed_version,
                ":pond_id": pond_id,
                ":tenant_id": tenant_id,
            },
        )

    def asset_exists_check_request(self, tenant_id: str, asset_id: str) -> dict[str, Any]:
        return self.condition_check_request(
            self.keys.asset(tenant_id, asset_id),
            condition="attribute_exists(PK) AND #tenant_id = :tenant_id",
            names={"#tenant_id": "tenant_id"},
            values={":tenant_id": tenant_id},
        )

    def site_active_check_request(self, tenant_id: str, site_id: str) -> dict[str, Any]:
        return self.condition_check_request(
            self.keys.site(tenant_id, site_id),
            condition="attribute_exists(PK) AND #status = :active AND #tenant_id = :tenant_id",
            names={"#status": "status", "#tenant_id": "tenant_id"},
            values={":active": "active", ":tenant_id": tenant_id},
        )

    def asset_active_check_request(self, tenant_id: str, asset_id: str) -> dict[str, Any]:
        return self.condition_check_request(
            self.keys.asset(tenant_id, asset_id),
            condition="attribute_exists(PK) AND #status = :active AND #tenant_id = :tenant_id",
            names={"#status": "status", "#tenant_id": "tenant_id"},
            values={":active": "active", ":tenant_id": tenant_id},
        )

    def end_deployment_row_request(
        self, key: Mapping[str, str], current: Deployment, ended: Deployment
    ) -> dict[str, Any]:
        """End one active row of `current` (mirror or history), fenced on its observed version."""
        return self.update_request(
            key,
            update_expression=(
                "SET #status = :status, #ended_at = :ended_at, "
                "#updated_at = :updated_at, #version = :version"
            ),
            condition=(
                "attribute_exists(PK) AND #version = :expected_version AND #status = :active"
                " AND #tenant_id = :tenant_id AND #deployment_id = :deployment_id"
            ),
            names={
                "#status": "status",
                "#ended_at": "ended_at",
                "#updated_at": "updated_at",
                "#version": "version",
                "#tenant_id": "tenant_id",
                "#deployment_id": "deployment_id",
            },
            values={
                ":status": ended.status.value,
                ":ended_at": canonical_timestamp_key(ended.ended_at),
                ":updated_at": canonical_timestamp_key(ended.updated_at),
                ":version": ended.version,
                ":expected_version": current.version,
                ":active": DeploymentStatus.ACTIVE.value,
                ":tenant_id": current.tenant_id,
                ":deployment_id": current.deployment_id,
            },
        )

    def replace_current_request(
        self, current: Deployment, replacement: Deployment
    ) -> dict[str, Any]:
        """Overwrite `DEPLOYMENT#CURRENT` only while it still names the observed Deployment."""
        return self.put_request(
            deployment_to_item(replacement, form="current"),
            condition=(
                "attribute_exists(PK) AND #deployment_id = :deployment_id"
                " AND #version = :expected_version AND #tenant_id = :tenant_id"
            ),
            names={
                "#deployment_id": "deployment_id",
                "#version": "version",
                "#tenant_id": "tenant_id",
            },
            values={
                ":deployment_id": current.deployment_id,
                ":expected_version": current.version,
                ":tenant_id": current.tenant_id,
            },
        )

    # Migration receipt ---------------------------------------------------------------------

    def record_migration_receipt(
        self, tenant_id: str, *, completed_at: datetime, counts: Mapping[str, int]
    ) -> None:
        """Upsert the per-tenant marker; it only records runs and is never used to skip work.

        Conditioned on the key being free or already holding this tenant's M1 receipt, so a
        foreign row at the key is never overwritten (`DynamoConditionalConflict`).
        """
        self.update_item_sync(
            self.update_request(
                self.keys.migration_receipt(tenant_id),
                condition=(
                    "attribute_not_exists(PK) OR (#entity_type = :entity_type "
                    "AND #tenant_id = :tenant_id AND #migration = :migration)"
                ),
                update_expression=(
                    "SET #entity_type = :entity_type, #tenant_id = :tenant_id, "
                    "#migration = :migration, "
                    "#first_completed_at = if_not_exists(#first_completed_at, :completed_at), "
                    "#last_completed_at = :completed_at, "
                    "#run_count = if_not_exists(#run_count, :zero) + :one, "
                    "#last_run = :last_run"
                ),
                names={
                    "#entity_type": "entity_type",
                    "#tenant_id": "tenant_id",
                    "#migration": "migration",
                    "#first_completed_at": "first_completed_at",
                    "#last_completed_at": "last_completed_at",
                    "#run_count": "run_count",
                    "#last_run": "last_run",
                },
                values={
                    ":entity_type": MIGRATION_RECEIPT_ENTITY_TYPE,
                    ":tenant_id": tenant_id,
                    ":migration": MIGRATION_NAME,
                    ":completed_at": canonical_timestamp_key(completed_at),
                    ":zero": 0,
                    ":one": 1,
                    ":last_run": dict(counts),
                },
            )
        )
