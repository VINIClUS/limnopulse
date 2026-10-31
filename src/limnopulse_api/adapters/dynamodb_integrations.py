"""DynamoDB persistence for tenant IntegrationAccounts and DeviceIntegrations.

Raw external identities never reach this adapter: a DeviceIntegration carries only the
provider-scoped SHA-256 digest, which also keys the global `EXTERNAL_ID#<provider>#<sha256>`
lookup that makes one provider identity claimable by exactly one DeviceIntegration.

P3-04 adds the `ExternalIdentityResolver` read (two consistent GetItems that must agree) and the
`IntegrationLifecycleRepository` transitions: one transaction of version-conditioned partial
`Update`s plus the D23 audit `Put` into the audit table. A DeviceIntegration transition also moves
the lookup row's `status`/`version`, so the identity stays claimed (D22) and both rows agree.
"""

from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any

from limnopulse_api.adapters.dynamodb_v2 import (
    DynamoConditionalConflict,
    DynamoV2RepositoryBase,
    canonical_timestamp_key,
    parse_canonical_timestamp,
    utc_now,
)
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.alerts import AuditContext
from limnopulse_api.domain.devices_v2 import DeviceStatus
from limnopulse_api.domain.ingestion import ingest_audit_item
from limnopulse_api.domain.integrations import (
    DeviceIntegration,
    DeviceIntegrationStatus,
    IntegrationAccount,
    IntegrationAccountStatus,
    ensure_device_integration_transition,
    ensure_integration_account_transition,
)
from limnopulse_api.domain.v2_common import IdempotencyReceipt

INTEGRATION_ACCOUNT_ENTITY_TYPE = "integration_account"
DEVICE_INTEGRATION_ENTITY_TYPE = "device_integration"

_ACCOUNT_ROW_INDEX = 0
_ACCOUNT_RECEIPT_INDEX = 1
_INTEGRATION_ROW_INDEX = 0
_LOOKUP_INDEX = 1
_INTEGRATION_RECEIPT_INDEX = 2
_ACCOUNT_CHECK_INDEX = 3
_DEVICE_CHECK_INDEX = 4

_IDEMPOTENCY_CONFLICT = "idempotency key already used"


def _stamps(entity: IntegrationAccount | DeviceIntegration) -> dict[str, Any]:
    return {
        "created_at": canonical_timestamp_key(entity.created_at),
        "updated_at": canonical_timestamp_key(entity.updated_at),
        "version": entity.version,
        "schema_version": entity.schema_version,
    }


def _parsed_stamps(item: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "created_at": parse_canonical_timestamp(item["created_at"]),
        "updated_at": parse_canonical_timestamp(item["updated_at"]),
        "version": item["version"],
        "schema_version": item.get("schema_version", 2),
    }


def integration_account_to_item(account: IntegrationAccount, keys: Any) -> dict[str, Any]:
    item: dict[str, Any] = {
        **keys.integration_account(account.tenant_id, account.integration_account_id),
        "entity_type": INTEGRATION_ACCOUNT_ENTITY_TYPE,
        "tenant_id": account.tenant_id,
        "integration_account_id": account.integration_account_id,
        "provider_key": account.provider_key,
        "display_name": account.display_name,
        "status": account.status.value,
        **_stamps(account),
    }
    if account.credential_reference is not None:
        item["credential_reference"] = account.credential_reference
    if account.public_identifier is not None:
        item["public_identifier"] = account.public_identifier
    return item


def integration_account_from_item(item: Mapping[str, Any]) -> IntegrationAccount:
    return IntegrationAccount(
        tenant_id=item["tenant_id"],
        integration_account_id=item["integration_account_id"],
        provider_key=item["provider_key"],
        display_name=item["display_name"],
        credential_reference=item.get("credential_reference"),
        public_identifier=item.get("public_identifier"),
        status=item["status"],
        **_parsed_stamps(item),
    )


def device_integration_to_item(integration: DeviceIntegration, keys: Any) -> dict[str, Any]:
    return {
        **keys.device_integration(integration.tenant_id, integration.device_integration_id),
        "entity_type": DEVICE_INTEGRATION_ENTITY_TYPE,
        "tenant_id": integration.tenant_id,
        "device_integration_id": integration.device_integration_id,
        "device_id": integration.device_id,
        "integration_account_id": integration.integration_account_id,
        "provider_key": integration.provider_key,
        "external_identity_hash": integration.external_identity_hash,
        "status": integration.status.value,
        **_stamps(integration),
    }


def device_integration_from_item(item: Mapping[str, Any]) -> DeviceIntegration:
    return DeviceIntegration(
        tenant_id=item["tenant_id"],
        device_integration_id=item["device_integration_id"],
        device_id=item["device_id"],
        integration_account_id=item["integration_account_id"],
        provider_key=item["provider_key"],
        external_identity_hash=item["external_identity_hash"],
        status=item["status"],
        **_parsed_stamps(item),
    )


def external_identity_lookup_to_item(integration: DeviceIntegration, keys: Any) -> dict[str, Any]:
    """Global uniqueness row; holds only routing metadata and the digest, never raw identity."""
    return {
        **keys.external_identity(integration.provider_key, integration.external_identity_hash),
        "tenant_id": integration.tenant_id,
        "device_id": integration.device_id,
        "device_integration_id": integration.device_integration_id,
        "provider_key": integration.provider_key,
        "external_identity_hash": integration.external_identity_hash,
        "status": integration.status.value,
        "version": integration.version,
    }


_STATUS_UPDATE = "SET #status = :status, #updated_at = :updated_at, #version = :next_version"
_STATUS_UPDATE_NAMES = {"#status": "status", "#updated_at": "updated_at", "#version": "version"}
_STATUS_CONDITION = "attribute_exists(PK) AND #version = :version AND #status = :current"


class DynamoIntegrationRepository(DynamoV2RepositoryBase):
    """`IntegrationRepository`, `ExternalIdentityResolver` and `IntegrationLifecycleRepository`
    over the shared single table; no Scan and no GSI. Lifecycle transitions also write one D23
    audit item into `audit_table_name`, which they therefore require."""

    def __init__(
        self,
        table_name: str,
        client: Any,
        *,
        audit_table_name: str | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        super().__init__(table_name, client, clock=clock)
        self.audit_table_name = audit_table_name

    async def get_integration_account(
        self, tenant_id: str, integration_account_id: str
    ) -> IntegrationAccount | None:
        item = await self.get_item(
            self.keys.integration_account(tenant_id, integration_account_id), consistent=True
        )
        if item is None or item.get("entity_type") != INTEGRATION_ACCOUNT_ENTITY_TYPE:
            return None
        account = integration_account_from_item(item)
        return account if account.tenant_id == tenant_id else None

    async def list_integration_accounts(self, tenant_id: str) -> list[IntegrationAccount]:
        items = await self.query_prefix(
            self.keys.tenant_partition(tenant_id),
            self.keys.INTEGRATION_ACCOUNT_PREFIX,
            consistent=True,
        )
        return [
            integration_account_from_item(item)
            for item in items
            if item.get("entity_type") == INTEGRATION_ACCOUNT_ENTITY_TYPE
        ]

    async def create_integration_account(
        self, account: IntegrationAccount, receipt: IdempotencyReceipt
    ) -> IntegrationAccount:
        actions = [
            {"Put": self.put_new_request(integration_account_to_item(account, self.keys))},
            {"Put": self.receipt_put_request(receipt)},
        ]
        try:
            await self.transact_write(actions)
        except DynamoConditionalConflict as exc:
            if _ACCOUNT_RECEIPT_INDEX in exc.failed_indexes:
                raise ConflictError(_IDEMPOTENCY_CONFLICT) from exc
            raise ConflictError("integration account already exists") from exc
        return account

    async def list_device_integrations(
        self, tenant_id: str, device_id: str
    ) -> list[DeviceIntegration]:
        items = await self.query_prefix(
            self.keys.tenant_partition(tenant_id),
            self.keys.DEVICE_INTEGRATION_PREFIX,
            consistent=True,
        )
        return [
            device_integration_from_item(item)
            for item in items
            if item.get("entity_type") == DEVICE_INTEGRATION_ENTITY_TYPE
            and item.get("device_id") == device_id
        ]

    async def create_device_integration(
        self, integration: DeviceIntegration, receipt: IdempotencyReceipt
    ) -> DeviceIntegration:
        actions = [
            {"Put": self.put_new_request(device_integration_to_item(integration, self.keys))},
            {"Put": self.put_new_request(external_identity_lookup_to_item(integration, self.keys))},
            {"Put": self.receipt_put_request(receipt)},
            # Parent status is fenced at commit time, not only by the service's earlier reads.
            {
                "ConditionCheck": self.condition_check_request(
                    self.keys.integration_account(
                        integration.tenant_id, integration.integration_account_id
                    ),
                    condition="attribute_exists(PK) AND #status IN (:pending, :active)",
                    names={"#status": "status"},
                    values={
                        ":pending": IntegrationAccountStatus.PENDING.value,
                        ":active": IntegrationAccountStatus.ACTIVE.value,
                    },
                )
            },
            {
                # Legacy Device rows share this key and carry status "active".
                "ConditionCheck": self.condition_check_request(
                    self.keys.device(integration.tenant_id, integration.device_id),
                    condition="attribute_exists(PK) AND #status <> :decommissioned",
                    names={"#status": "status"},
                    values={":decommissioned": DeviceStatus.DECOMMISSIONED.value},
                )
            },
        ]
        try:
            await self.transact_write(actions)
        except DynamoConditionalConflict as exc:
            # A same-key retry racing its original fails on both the lookup and the receipt;
            # the receipt wins so the service can reread it and replay the stored snapshot.
            if _INTEGRATION_RECEIPT_INDEX in exc.failed_indexes:
                raise ConflictError(_IDEMPOTENCY_CONFLICT) from exc
            if _ACCOUNT_CHECK_INDEX in exc.failed_indexes:
                raise ConflictError("integration account is not provisionable") from exc
            if _DEVICE_CHECK_INDEX in exc.failed_indexes:
                raise ConflictError("device is decommissioned") from exc
            if _LOOKUP_INDEX in exc.failed_indexes:
                raise ConflictError("external identity already claimed") from exc
            raise ConflictError("device integration already exists") from exc
        return integration

    # External identity resolution (P3-04) ---------------------------------------------------

    async def resolve_external_identity(
        self, provider_key: str, external_identity_hash: str
    ) -> DeviceIntegration | None:
        """Both rows read strongly consistent; `None` when either is missing or they disagree."""
        lookup = await self.get_item(
            self.keys.external_identity(provider_key, external_identity_hash), consistent=True
        )
        if lookup is None:
            return None
        tenant_id = lookup.get("tenant_id")
        integration_id = lookup.get("device_integration_id")
        if not isinstance(tenant_id, str) or not isinstance(integration_id, str):
            return None
        item = await self.get_item(
            self.keys.device_integration(tenant_id, integration_id), consistent=True
        )
        if item is None or item.get("entity_type") != DEVICE_INTEGRATION_ENTITY_TYPE:
            return None
        integration = device_integration_from_item(item)
        claimed = (
            tenant_id,
            lookup.get("device_id"),
            integration_id,
            provider_key,
            external_identity_hash,
        )
        actual = (
            integration.tenant_id,
            integration.device_id,
            integration.device_integration_id,
            integration.provider_key,
            integration.external_identity_hash,
        )
        return integration if actual == claimed else None

    # Lifecycle (P3-04, D23) -----------------------------------------------------------------

    async def transition_integration_account(
        self,
        tenant_id: str,
        integration_account_id: str,
        target: IntegrationAccountStatus,
        *,
        expected_version: int,
        updated_at: datetime,
        audit: AuditContext,
    ) -> IntegrationAccount:
        audit_table = self._require_audit_table()
        key = self.keys.integration_account(tenant_id, integration_account_id)
        item = await self.get_item(key, consistent=True)
        if (
            item is None
            or item.get("entity_type") != INTEGRATION_ACCOUNT_ENTITY_TYPE
            or item.get("tenant_id") != tenant_id
        ):
            raise NotFoundError("integration account not found")
        current = integration_account_from_item(item)
        if current.version != expected_version:
            raise ConflictError("integration account version conflict")
        ensure_integration_account_transition(current.status, target)
        updated = IntegrationAccount.model_validate(
            {
                **current.model_dump(),
                "status": target,
                "updated_at": updated_at,
                "version": current.version + 1,
            }
        )
        actions = [
            {"Update": self._status_update(key, current.status.value, updated)},
            {
                "Put": self._audit_put(
                    audit_table,
                    ingest_audit_item(
                        action="integration_account.status_changed",
                        tenant_id=tenant_id,
                        resource_id=integration_account_id,
                        before_status=current.status.value,
                        after_status=updated.status.value,
                        context=audit,
                        now=updated_at,
                    ),
                )
            },
        ]
        try:
            await self.transact_write(actions)
        except DynamoConditionalConflict as exc:
            raise ConflictError("integration account version conflict") from exc
        return updated

    async def transition_device_integration(
        self,
        tenant_id: str,
        device_integration_id: str,
        target: DeviceIntegrationStatus,
        *,
        expected_version: int,
        updated_at: datetime,
        audit: AuditContext,
    ) -> DeviceIntegration:
        audit_table = self._require_audit_table()
        key = self.keys.device_integration(tenant_id, device_integration_id)
        item = await self.get_item(key, consistent=True)
        if (
            item is None
            or item.get("entity_type") != DEVICE_INTEGRATION_ENTITY_TYPE
            or item.get("tenant_id") != tenant_id
        ):
            raise NotFoundError("device integration not found")
        current = device_integration_from_item(item)
        if current.version != expected_version:
            raise ConflictError("device integration version conflict")
        ensure_device_integration_transition(current.status, target)
        updated = DeviceIntegration.model_validate(
            {
                **current.model_dump(),
                "status": target,
                "updated_at": updated_at,
                "version": current.version + 1,
            }
        )
        lookup_key = self.keys.external_identity(
            current.provider_key, current.external_identity_hash
        )
        actions = [
            {"Update": self._status_update(key, current.status.value, updated)},
            {
                # The identity stays claimed by this DeviceIntegration; only its state moves.
                "Update": self.update_request(
                    lookup_key,
                    update_expression="SET #status = :status, #version = :next_version",
                    condition=(
                        "attribute_exists(PK) AND #version = :version"
                        " AND device_integration_id = :device_integration_id"
                    ),
                    names={"#status": "status", "#version": "version"},
                    values={
                        ":status": updated.status.value,
                        ":next_version": updated.version,
                        ":version": current.version,
                        ":device_integration_id": device_integration_id,
                    },
                )
            },
            {
                "Put": self._audit_put(
                    audit_table,
                    ingest_audit_item(
                        action="device_integration.status_changed",
                        tenant_id=tenant_id,
                        resource_id=device_integration_id,
                        before_status=current.status.value,
                        after_status=updated.status.value,
                        context=audit,
                        now=updated_at,
                    ),
                )
            },
        ]
        try:
            await self.transact_write(actions)
        except DynamoConditionalConflict as exc:
            raise ConflictError("device integration version conflict") from exc
        return updated

    def _require_audit_table(self) -> str:
        if not self.audit_table_name:
            raise RuntimeError("lifecycle transitions need an audit table")
        return self.audit_table_name

    def _status_update(
        self,
        key: Mapping[str, str],
        current_status: str,
        updated: IntegrationAccount | DeviceIntegration,
    ) -> dict[str, Any]:
        """Only the changed fields: never a full `Put`, so unknown attributes survive."""
        return self.update_request(
            key,
            update_expression=_STATUS_UPDATE,
            condition=_STATUS_CONDITION,
            names=_STATUS_UPDATE_NAMES,
            values={
                ":status": updated.status.value,
                ":updated_at": canonical_timestamp_key(updated.updated_at),
                ":next_version": updated.version,
                ":version": updated.version - 1,
                ":current": current_status,
            },
        )

    def _audit_put(self, audit_table: str, item: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "TableName": audit_table,
            "Item": self.serialize_item(item),
            "ConditionExpression": "attribute_not_exists(PK)",
        }
