"""`IngestCredentialRepository` over the shared single table (D2, D23).

The authoritative row `INGEST_CREDENTIAL#<ic>/META` is the only item that holds `secret_sha256`.
The tenant listing row and the two fixed slot rows carry metadata only. Issue and revoke are one
`TransactWriteItems` each, and each writes exactly one conditioned audit item into the audit
table, so an audit failure aborts the change. 404 vs 409 is decided from the cancelled item
(`ReturnValuesOnConditionCheckFailure=ALL_OLD`), never from a second read.
"""

from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any

from botocore.exceptions import ClientError

from limnopulse_api.adapters.dynamodb_v2 import (
    DynamoConditionalConflict,
    DynamoV2RepositoryBase,
    canonical_timestamp_key,
    ingest_credential_from_item,
    ingest_credential_to_item,
    utc_now,
)
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.alerts import AuditContext
from limnopulse_api.domain.ingestion import (
    HTTPS_PROVIDER_KEYS,
    IngestCredential,
    IngestCredentialStatus,
    ingest_audit_item,
)
from limnopulse_api.domain.integrations import IntegrationAccountStatus

CREDENTIAL_ENTITY_TYPE = "ingest_credential"
LISTING_ENTITY_TYPE = "ingest_credential_listing"
SLOT_ENTITY_TYPE = "ingest_credential_slot"
_ALL_OLD = "ALL_OLD"
_ISSUANCE_CHECK = (
    "attribute_exists(PK) AND #status IN (:pending, :active) AND provider_key = :https"
)
_REVOKE_CONDITION = (
    "tenant_id = :t AND integration_account_id = :ia AND #status = :active AND #version = :v"
)
# Transaction action indexes.
_ACCOUNT_CHECK = 0
_AUTHORITATIVE_UPDATE = 0


def _listing_item(credential: IngestCredential, keys: Any) -> dict[str, Any]:
    """Metadata only: never the digest."""
    item: dict[str, Any] = {
        **keys.ingest_credential_listing(
            credential.tenant_id,
            credential.integration_account_id,
            credential.ingest_credential_id,
        ),
        "entity_type": LISTING_ENTITY_TYPE,
        "tenant_id": credential.tenant_id,
        "integration_account_id": credential.integration_account_id,
        "ingest_credential_id": credential.ingest_credential_id,
        "slot": credential.slot,
        "fingerprint": credential.fingerprint,
        "status": credential.status.value,
        "created_at": canonical_timestamp_key(credential.created_at),
        "updated_at": canonical_timestamp_key(credential.updated_at),
        "version": credential.version,
    }
    return item


class DynamoIngestCredentialRepository(DynamoV2RepositoryBase):
    def __init__(
        self,
        table_name: str,
        client: Any,
        *,
        clock: Callable[[], datetime] = utc_now,
        audit_table_name: str,
    ) -> None:
        super().__init__(table_name, client, clock=clock)
        self.audit_table_name = audit_table_name

    async def get_ingest_credential(self, ingest_credential_id: str) -> IngestCredential | None:
        item = await self.get_item(
            self.keys.ingest_credential(ingest_credential_id), consistent=True
        )
        if item is None or item.get("entity_type") != CREDENTIAL_ENTITY_TYPE:
            return None
        return ingest_credential_from_item(item)

    async def list_ingest_credentials(
        self, tenant_id: str, integration_account_id: str
    ) -> list[IngestCredential]:
        listings = await self.query_prefix(
            self.keys.tenant_partition(tenant_id),
            self.keys.ingest_credential_listing_prefix(integration_account_id),
            consistent=True,
        )
        credentials: list[IngestCredential] = []
        for listing in listings:
            if listing.get("entity_type") != LISTING_ENTITY_TYPE:
                continue
            # The listing row has no digest; the authoritative row is the source of truth.
            credential = await self.get_ingest_credential(listing["ingest_credential_id"])
            if credential is not None and (
                credential.tenant_id,
                credential.integration_account_id,
            ) == (tenant_id, integration_account_id):
                credentials.append(credential)
        return credentials

    async def create_ingest_credential(
        self, credential: IngestCredential, audit: AuditContext
    ) -> IngestCredential:
        if credential.status != IngestCredentialStatus.ACTIVE or credential.revoked_at is not None:
            raise ValueError("a new ingest credential must be active")
        tenant_id = credential.tenant_id
        account_id = credential.integration_account_id
        (provider_key,) = HTTPS_PROVIDER_KEYS
        slot_item = {
            **self.keys.ingest_credential_slot(tenant_id, account_id, credential.slot),
            "entity_type": SLOT_ENTITY_TYPE,
            "tenant_id": tenant_id,
            "integration_account_id": account_id,
            "ingest_credential_id": credential.ingest_credential_id,
            "slot": credential.slot,
        }
        audit_item = ingest_audit_item(
            action="ingest_credential.issued",
            tenant_id=tenant_id,
            resource_id=credential.ingest_credential_id,
            before_status=None,
            after_status=credential.status.value,
            context=audit,
            now=credential.created_at,
            fingerprint=credential.fingerprint,
        )
        actions = [
            {
                "ConditionCheck": {
                    **self.condition_check_request(
                        self.keys.integration_account(tenant_id, account_id),
                        condition=_ISSUANCE_CHECK,
                        names={"#status": "status"},
                        values={
                            ":pending": IntegrationAccountStatus.PENDING.value,
                            ":active": IntegrationAccountStatus.ACTIVE.value,
                            ":https": provider_key,
                        },
                    ),
                    "ReturnValuesOnConditionCheckFailure": _ALL_OLD,
                }
            },
            {"Put": self.put_new_request(slot_item)},
            {"Put": self.put_new_request(_listing_item(credential, self.keys))},
            {"Put": self.put_new_request(ingest_credential_to_item(credential))},
            {"Put": self._audit_put(audit_item)},
        ]
        try:
            await self._transact(actions)
        except DynamoConditionalConflict as exc:
            not_found = (
                _ACCOUNT_CHECK in exc.failed_indexes and _old_item(exc, _ACCOUNT_CHECK) is None
            )
        else:
            return credential
        # Raised outside the handler, as on revoke: the cancelled transaction's frames hold the
        # META Put with `secret_sha256`, so no cause or context may keep them reachable.
        if not_found:
            raise NotFoundError("integration account not found")
        raise ConflictError("ingest credential could not be issued")

    async def revoke_ingest_credential(
        self,
        tenant_id: str,
        integration_account_id: str,
        ingest_credential_id: str,
        *,
        expected_version: int,
        revoked_at: datetime,
        audit: AuditContext,
    ) -> IngestCredential:
        # One read, only to find the slot to free; the transaction conditions decide the outcome.
        current = await self.get_ingest_credential(ingest_credential_id)
        if current is None or (current.tenant_id, current.integration_account_id) != (
            tenant_id,
            integration_account_id,
        ):
            raise NotFoundError("ingest credential not found")
        revoked = IngestCredential.model_validate(
            {
                **current.model_dump(),
                "status": IngestCredentialStatus.REVOKED,
                "revoked_at": revoked_at,
                "updated_at": revoked_at,
                "version": expected_version + 1,
            }
        )
        stamp = canonical_timestamp_key(revoked_at)
        update_values = {
            ":revoked": IngestCredentialStatus.REVOKED.value,
            ":at": stamp,
            ":next": revoked.version,
        }
        set_revoked = "SET #status = :revoked, revoked_at = :at, updated_at = :at, #version = :next"
        audit_item = ingest_audit_item(
            action="ingest_credential.revoked",
            tenant_id=tenant_id,
            resource_id=ingest_credential_id,
            before_status=IngestCredentialStatus.ACTIVE.value,
            after_status=IngestCredentialStatus.REVOKED.value,
            context=audit,
            now=revoked_at,
            fingerprint=current.fingerprint,
        )
        actions = [
            {
                "Update": {
                    **self.update_request(
                        self.keys.ingest_credential(ingest_credential_id),
                        update_expression=set_revoked,
                        condition=_REVOKE_CONDITION,
                        names={"#status": "status", "#version": "version"},
                        values={
                            **update_values,
                            ":t": tenant_id,
                            ":ia": integration_account_id,
                            ":active": IngestCredentialStatus.ACTIVE.value,
                            ":v": expected_version,
                        },
                    ),
                    "ReturnValuesOnConditionCheckFailure": _ALL_OLD,
                }
            },
            {
                "Update": self.update_request(
                    self.keys.ingest_credential_listing(
                        tenant_id, integration_account_id, ingest_credential_id
                    ),
                    update_expression=set_revoked,
                    names={"#status": "status", "#version": "version"},
                    values=update_values,
                )
            },
            {
                "Delete": self._delete_request(
                    self.keys.ingest_credential_slot(
                        tenant_id, integration_account_id, current.slot
                    ),
                    condition="ingest_credential_id = :ic",
                    values={":ic": ingest_credential_id},
                )
            },
            {"Put": self._audit_put(audit_item)},
        ]
        try:
            await self._transact(actions)
        except DynamoConditionalConflict as exc:
            not_found = _AUTHORITATIVE_UPDATE in exc.failed_indexes and not self._is_active_row(
                _old_item(exc, _AUTHORITATIVE_UPDATE), tenant_id, integration_account_id
            )
        else:
            return revoked
        # Raised outside the handler: the cancelled `ALL_OLD` item holds `secret_sha256`, so it
        # must not stay reachable through the exception chain (error trackers, frame locals).
        if not_found:
            raise NotFoundError("ingest credential not found")
        raise ConflictError("ingest credential version conflict")

    async def _transact(self, actions: list[dict[str, Any]]) -> None:
        """A cancellation caused only by a concurrent transaction on the same items is a
        conflict (409), as in the other transactional adapters; store failures propagate."""
        try:
            await self.transact_write(actions)
        except ClientError as exc:
            if not _is_transaction_conflict(exc):
                raise
        else:
            return
        # Outside the handler, so the conflict carries no context (see `create_ingest_credential`).
        raise ConflictError("concurrent ingest credential change")

    def _is_active_row(
        self, item: Mapping[str, Any] | None, tenant_id: str, integration_account_id: str
    ) -> bool:
        row = None if item is None else self.deserialize_item(item)
        return (
            row is not None
            and row.get("tenant_id") == tenant_id
            and row.get("integration_account_id") == integration_account_id
            and row.get("status") == IngestCredentialStatus.ACTIVE.value
        )

    # Request builders ------------------------------------------------------------------------

    def _audit_put(self, item: Mapping[str, Any]) -> dict[str, Any]:
        """A conditioned audit Put: a colliding audit key aborts the whole transaction."""
        return {**self.put_new_request(item), "TableName": self.audit_table_name}

    def _delete_request(
        self, key: Mapping[str, str], *, condition: str, values: Mapping[str, Any]
    ) -> dict[str, Any]:
        """The slot `Delete` builder lives here, not on the pinned repository base."""
        request: dict[str, Any] = {"TableName": self.table_name, "Key": self.serialize_item(key)}
        return self._with_condition(request, condition, None, values)


def _is_transaction_conflict(exc: ClientError) -> bool:
    response = exc.response
    if response.get("Error", {}).get("Code") != "TransactionCanceledException":
        return False
    reasons = response.get("CancellationReasons")
    return isinstance(reasons, list) and any(
        isinstance(reason, Mapping) and reason.get("Code") == "TransactionConflict"
        for reason in reasons
    )


def _old_item(exc: DynamoConditionalConflict, index: int) -> Mapping[str, Any] | None:
    """The `ALL_OLD` item DynamoDB returned for a cancelled transaction action, if any."""
    response = getattr(exc.__cause__, "response", None)
    reasons = response.get("CancellationReasons") if isinstance(response, Mapping) else None
    if not isinstance(reasons, list) or index >= len(reasons):
        return None
    reason = reasons[index]
    return reason.get("Item") if isinstance(reason, Mapping) else None
