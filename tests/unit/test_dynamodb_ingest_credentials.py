"""P3-03 DynamoDB ingest-credential adapter: the P3-00 kits on the in-memory Dynamo fake, plus the
D2 transaction shapes, the D23 audit item and the no-plaintext storage rule.

`DynamoCredentialSubject` is reused by `tests/ingestion/test_ingest_credentials_dynamodb.py`,
which runs the same kits against DynamoDB Local.
"""

import asyncio
import json
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError

from limnopulse_api.adapters.dynamodb_ingest_credentials import DynamoIngestCredentialRepository
from limnopulse_api.adapters.dynamodb_integrations import (
    DynamoIntegrationRepository,
    integration_account_to_item,
)
from limnopulse_api.adapters.dynamodb_v2 import DynamoV2KeyBuilder, ingest_credential_to_item
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain import ingestion as ingestion_domain
from limnopulse_api.domain.ingestion import IngestCredential, IngestCredentialStatus
from limnopulse_api.domain.integrations import IntegrationAccount, IntegrationAccountStatus
from limnopulse_api.repositories.ingestion import ConnectorAuthenticator, IngestCredentialRepository
from limnopulse_api.services.connector_auth import IngestCredentialAuthenticator
from tests.support.ingestion_contract_kits import (
    ACCOUNT,
    AUDIT,
    CONNECTOR_AUTHENTICATOR_CONTRACT,
    CREATED,
    CREDENTIAL,
    CREDENTIAL_REPOSITORY_CONTRACT,
    OTHER_CREDENTIAL,
    RECEIVED,
    SECRET,
    TENANT,
    credential,
    integration_account,
    kit_ids,
)
from tests.support.ingestion_dynamo_fake import IngestionDynamoClient

TABLE = "LimnopulseDomain"
AUDIT_TABLE = "LimnopulseAudit"
KEYS = DynamoV2KeyBuilder()
_SERIALIZER = TypeSerializer()
_DESERIALIZER = TypeDeserializer()

# Port operation (or `STORE_READS` name) -> (boto3 operation, partition-key prefix).
_FAULT_POINTS: dict[str, tuple[str, str | None]] = {
    "get_ingest_credential": ("get_item", "INGEST_CREDENTIAL#"),
    "authenticate": ("get_item", "INGEST_CREDENTIAL#"),
    "get_integration_account": ("get_item", "TENANT#"),
    "list_ingest_credentials": ("query", None),
    "create_ingest_credential": ("transact_write_items", None),
    "revoke_ingest_credential": ("transact_write_items", None),
}


def _encode(item: dict[str, Any]) -> dict[str, Any]:
    return {key: _SERIALIZER.serialize(value) for key, value in item.items()}


def _decode(item: dict[str, Any]) -> dict[str, Any]:
    return {key: _DESERIALIZER.deserialize(value) for key, value in item.items()}


class FaultingClient:
    """Wraps any boto3-shaped DynamoDB client; `fail_next` raises the given object once from the
    next matching call, before it reaches the store (fake or DynamoDB Local)."""

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self._faults: dict[str, list[tuple[str | None, BaseException]]] = defaultdict(list)

    def fail_next(self, operation: str, exc: BaseException, *, pk_prefix: str | None) -> None:
        self._faults[operation].append((pk_prefix, exc))

    def _check(self, operation: str, pks: list[str]) -> None:
        pending = self._faults.get(operation, [])
        for index, (prefix, exc) in enumerate(pending):
            if prefix is None or any(pk.startswith(prefix) for pk in pks):
                del pending[index]
                raise exc

    @staticmethod
    def _pk(raw: dict[str, Any] | None) -> list[str]:
        return [] if not raw or "PK" not in raw else [_decode({"PK": raw["PK"]})["PK"]]

    def get_item(self, **kwargs: Any) -> Any:
        self._check("get_item", self._pk(kwargs.get("Key")))
        return self.inner.get_item(**kwargs)

    def query(self, **kwargs: Any) -> Any:
        self._check("query", [])
        return self.inner.query(**kwargs)

    def transact_write_items(self, **kwargs: Any) -> Any:
        self._check("transact_write_items", [])
        return self.inner.transact_write_items(**kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)


class DynamoCredentialSubject:
    """`CredentialKitSubject` and `AuthenticatorKitSubject` over the real P3-03 adapter."""

    def __init__(
        self,
        client: Any,
        *,
        table: str = TABLE,
        audit_table: str = AUDIT_TABLE,
        clock: Callable[[], datetime] = lambda: RECEIVED,
    ) -> None:
        self.raw = client
        self.table = table
        self.audit_table = audit_table
        self.client = FaultingClient(client)
        self.repository = DynamoIngestCredentialRepository(
            table, self.client, clock=clock, audit_table_name=audit_table
        )
        self.integrations = DynamoIntegrationRepository(table, self.client, clock=clock)
        self.authenticator = IngestCredentialAuthenticator(
            credentials=self.repository, integrations=self.integrations
        )

    def fail_next(self, operation: str, exc: BaseException) -> None:
        boto_operation, prefix = _FAULT_POINTS[operation]
        self.client.fail_next(boto_operation, exc, pk_prefix=prefix)

    async def seed_account(self, account: IntegrationAccount) -> None:
        item = integration_account_to_item(account, KEYS)
        self.raw.put_item(TableName=self.table, Item=_encode(item))

    async def seed_credential(self, credential: IngestCredential) -> None:
        item = ingest_credential_to_item(credential)
        self.raw.put_item(TableName=self.table, Item=_encode(item))

    def audit_items(self) -> list[dict[str, Any]]:
        items = self._audit_rows()
        return sorted(items, key=lambda item: (item["created_at"], item["action"]))

    def _audit_rows(self) -> list[dict[str, Any]]:
        if isinstance(self.raw, IngestionDynamoClient):
            return [
                dict(item)
                for item in self.raw.items.values()
                if item.get("entity_type") == "audit_event"
            ]
        rows: list[dict[str, Any]] = []
        request: dict[str, Any] = {"TableName": self.audit_table, "ConsistentRead": True}
        while True:  # test-only Scan of a throwaway audit table
            response = self.raw.scan(**request)
            rows += [_decode(item) for item in response.get("Items", [])]
            if "LastEvaluatedKey" not in response:
                return rows
            request["ExclusiveStartKey"] = response["LastEvaluatedKey"]


def fake_subject() -> DynamoCredentialSubject:
    return DynamoCredentialSubject(IngestionDynamoClient())


# Contract kits -------------------------------------------------------------------------------


def test_adapters_implement_their_ports() -> None:
    subject = fake_subject()

    assert isinstance(subject.repository, IngestCredentialRepository)
    assert isinstance(subject.authenticator, ConnectorAuthenticator)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario", CREDENTIAL_REPOSITORY_CONTRACT, ids=kit_ids(CREDENTIAL_REPOSITORY_CONTRACT)
)
async def test_repository_passes_the_credential_kit_on_the_dynamo_fake(scenario) -> None:
    await scenario(fake_subject())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario", CONNECTOR_AUTHENTICATOR_CONTRACT, ids=kit_ids(CONNECTOR_AUTHENTICATOR_CONTRACT)
)
async def test_authenticator_passes_the_authenticator_kit_on_the_dynamo_fake(scenario) -> None:
    await scenario(fake_subject())


# Transaction shapes (D2, D23) ------------------------------------------------------------------


def _action_kinds(transaction: list[dict[str, Any]]) -> list[tuple[str, str, str, str]]:
    """(kind, table, PK, SK) per transaction action."""
    rows = []
    for action in transaction:
        ((kind, body),) = action.items()
        key = _decode(body.get("Item") or body["Key"])
        rows.append((kind, body["TableName"], key["PK"], key["SK"]))
    return rows


def _body(transaction: list[dict[str, Any]], kind: str, pk: str, sk: str) -> dict[str, Any]:
    for action in transaction:
        ((action_kind, body),) = action.items()
        key = _decode(body.get("Item") or body["Key"])
        if (action_kind, key["PK"], key["SK"]) == (kind, pk, sk):
            return body
    raise AssertionError(f"no {kind} on {pk}/{sk}")


@pytest.mark.asyncio
async def test_issue_is_one_transaction_with_the_d2_conditions_and_one_audit_put() -> None:
    subject = fake_subject()
    await subject.seed_account(integration_account())

    await subject.repository.create_ingest_credential(credential(), AUDIT)

    (transaction,) = subject.raw.transactions
    kinds = _action_kinds(transaction)
    account_key = KEYS.integration_account(TENANT, ACCOUNT)
    slot_key = KEYS.ingest_credential_slot(TENANT, ACCOUNT, 1)
    listing_key = KEYS.ingest_credential_listing(TENANT, ACCOUNT, CREDENTIAL)
    credential_key = KEYS.ingest_credential(CREDENTIAL)
    assert sorted(row for row in kinds if row[1] == TABLE) == sorted(
        [
            ("ConditionCheck", TABLE, account_key["PK"], account_key["SK"]),
            ("Put", TABLE, slot_key["PK"], slot_key["SK"]),
            ("Put", TABLE, listing_key["PK"], listing_key["SK"]),
            ("Put", TABLE, credential_key["PK"], credential_key["SK"]),
        ]
    )
    audit_puts = [row for row in kinds if row[1] == AUDIT_TABLE]
    assert len(audit_puts) == 1 and audit_puts[0][0] == "Put"
    # The IntegrationAccount item is checked, never written.
    assert not [row for row in kinds if row[0] != "ConditionCheck" and row[3] == account_key["SK"]]

    check = _body(transaction, "ConditionCheck", account_key["PK"], account_key["SK"])
    assert check["ConditionExpression"] in {
        "#status IN (:pending, :active) AND provider_key = :https",  # D2 verbatim
        "attribute_exists(PK) AND #status IN (:pending, :active) AND provider_key = :https",
    }
    assert _decode(check["ExpressionAttributeValues"]) == {
        ":pending": "pending",
        ":active": "active",
        ":https": "https_direct",
    }
    assert check["ReturnValuesOnConditionCheckFailure"] == "ALL_OLD"
    for key in (slot_key, listing_key, credential_key):
        put = _body(transaction, "Put", key["PK"], key["SK"])
        assert put["ConditionExpression"] == "attribute_not_exists(PK)"
    slot = _decode(_body(transaction, "Put", slot_key["PK"], slot_key["SK"])["Item"])
    assert slot["entity_type"] == "ingest_credential_slot"
    assert slot["ingest_credential_id"] == CREDENTIAL
    listing = _decode(_body(transaction, "Put", listing_key["PK"], listing_key["SK"])["Item"])
    assert listing["entity_type"] == "ingest_credential_listing"


@pytest.mark.asyncio
async def test_revoke_is_one_transaction_bound_to_tenant_account_status_and_version() -> None:
    subject = fake_subject()
    await subject.seed_account(integration_account())
    await subject.repository.create_ingest_credential(credential(), AUDIT)

    await subject.repository.revoke_ingest_credential(
        TENANT, ACCOUNT, CREDENTIAL, expected_version=1, revoked_at=RECEIVED, audit=AUDIT
    )

    transaction = subject.raw.transactions[-1]
    kinds = _action_kinds(transaction)
    slot_key = KEYS.ingest_credential_slot(TENANT, ACCOUNT, 1)
    listing_key = KEYS.ingest_credential_listing(TENANT, ACCOUNT, CREDENTIAL)
    credential_key = KEYS.ingest_credential(CREDENTIAL)
    domain_actions = sorted(row for row in kinds if row[1] == TABLE)
    assert domain_actions == sorted(
        [
            ("Update", TABLE, credential_key["PK"], credential_key["SK"]),
            ("Update", TABLE, listing_key["PK"], listing_key["SK"]),
            ("Delete", TABLE, slot_key["PK"], slot_key["SK"]),
        ]
    )
    assert [row[0] for row in kinds if row[1] == AUDIT_TABLE] == ["Put"]

    authoritative = _body(transaction, "Update", credential_key["PK"], credential_key["SK"])
    condition = authoritative["ConditionExpression"]
    for fragment in ("tenant_id = :t", "integration_account_id = :ia", "= :active", "= :v"):
        assert fragment in condition
    values = _decode(authoritative["ExpressionAttributeValues"])
    assert (values[":t"], values[":ia"], values[":active"], values[":v"]) == (
        TENANT,
        ACCOUNT,
        "active",
        1,
    )
    assert authoritative["ReturnValuesOnConditionCheckFailure"] == "ALL_OLD"
    slot = _body(transaction, "Delete", slot_key["PK"], slot_key["SK"])
    assert slot["ConditionExpression"] == "ingest_credential_id = :ic"
    assert _decode(slot["ExpressionAttributeValues"]) == {":ic": CREDENTIAL}


class ChangedAfterRead(FaultingClient):
    """Changes the credential row behind the adapter's back, right after the adapter read it:
    only the cancelled item (`ALL_OLD`) shows the truth."""

    def __init__(self, inner: Any, **changes: Any) -> None:
        super().__init__(inner)
        self.changes = changes

    def get_item(self, **kwargs: Any) -> Any:
        response = super().get_item(**kwargs)
        key = _decode(kwargs["Key"])
        if key["PK"].startswith("INGEST_CREDENTIAL#") and "Item" in response:
            self.inner.items[(key["PK"], key["SK"])].update(self.changes)
        return response


@pytest.mark.asyncio
async def test_revoke_classifies_404_or_409_from_the_cancelled_item() -> None:
    """404 vs 409 comes from the cancelled authoritative item (`ALL_OLD`), not a re-read."""
    subject = fake_subject()
    await subject.seed_account(integration_account())
    await subject.repository.create_ingest_credential(credential(), AUDIT)
    reads_before = len(subject.raw.reads)

    with pytest.raises(ConflictError) as stale:
        await subject.repository.revoke_ingest_credential(
            TENANT, ACCOUNT, CREDENTIAL, expected_version=9, revoked_at=RECEIVED, audit=AUDIT
        )
    assert not isinstance(stale.value, NotFoundError)
    for tenant_id, credential_id in (("tenant-b", CREDENTIAL), (TENANT, OTHER_CREDENTIAL)):
        with pytest.raises(NotFoundError):
            await subject.repository.revoke_ingest_credential(
                tenant_id,
                ACCOUNT,
                credential_id,
                expected_version=1,
                revoked_at=RECEIVED,
                audit=AUDIT,
            )
    # Each attempt reads at most the authoritative row once (to find its slot).
    reads = subject.raw.reads[reads_before:]
    assert all(pk.startswith("INGEST_CREDENTIAL#") for _, pk, _ in reads)
    assert len(reads) <= 3
    assert all(consistent for _, _, consistent in reads)

    # The row was active at version 1 when read, revoked before the commit: the cancelled
    # item says "not active", so the answer is 404 (kit semantics), never a version 409.
    def racing(**changes: Any) -> DynamoIngestCredentialRepository:
        client = ChangedAfterRead(subject.raw, **changes)
        return DynamoIngestCredentialRepository(
            TABLE, client, clock=lambda: RECEIVED, audit_table_name=AUDIT_TABLE
        )

    with pytest.raises(NotFoundError):
        await racing(
            status="revoked", revoked_at="2026-01-01T00:00:00.000000Z"
        ).revoke_ingest_credential(
            TENANT, ACCOUNT, CREDENTIAL, expected_version=1, revoked_at=RECEIVED, audit=AUDIT
        )
    # Active again but at a newer version than the one read and expected: a version 409.
    with pytest.raises(ConflictError) as bumped:
        await racing(status="active", version=2).revoke_ingest_credential(
            TENANT, ACCOUNT, CREDENTIAL, expected_version=1, revoked_at=RECEIVED, audit=AUDIT
        )
    assert not isinstance(bumped.value, NotFoundError)


@pytest.mark.asyncio
async def test_concurrent_issues_for_one_free_slot_have_exactly_one_winner() -> None:
    subject = fake_subject()
    await subject.seed_account(integration_account())
    contenders = [
        credential(),
        credential(ingest_credential_id=OTHER_CREDENTIAL),
    ]

    results = await asyncio.gather(
        *(subject.repository.create_ingest_credential(item, AUDIT) for item in contenders),
        return_exceptions=True,
    )

    winners = [result for result in results if isinstance(result, IngestCredential)]
    assert len(winners) == 1
    assert sum(isinstance(result, ConflictError) for result in results) == 1
    assert await subject.repository.list_ingest_credentials(TENANT, ACCOUNT) == winners
    assert len(subject.audit_items()) == 1


@pytest.mark.asyncio
async def test_issue_against_a_concurrently_changed_account_is_a_conflict() -> None:
    """The account row exists but is no longer eligible at commit time: 409, never 404."""
    subject = fake_subject()
    for account in (
        integration_account(status=IntegrationAccountStatus.REVOKED),
        integration_account(provider_key="vendor_cloud"),
    ):
        await subject.seed_account(account)
        with pytest.raises(ConflictError) as exc_info:
            await subject.repository.create_ingest_credential(credential(), AUDIT)
        assert not isinstance(exc_info.value, NotFoundError)


@pytest.mark.asyncio
async def test_issue_for_a_missing_account_is_not_found() -> None:
    subject = fake_subject()

    with pytest.raises(NotFoundError):
        await subject.repository.create_ingest_credential(credential(), AUDIT)


# Storage: no plaintext, digest only on the authoritative row -----------------------------------


@pytest.mark.asyncio
async def test_only_the_authoritative_row_holds_the_digest() -> None:
    subject = fake_subject()
    await subject.seed_account(integration_account())
    issued = await subject.repository.create_ingest_credential(credential(), AUDIT)
    await subject.repository.revoke_ingest_credential(
        TENANT, ACCOUNT, CREDENTIAL, expected_version=1, revoked_at=RECEIVED, audit=AUDIT
    )
    token = ingestion_domain.ingest_token(CREDENTIAL, SECRET)
    authoritative = tuple(KEYS.ingest_credential(CREDENTIAL).values())

    written = [
        _decode(body.get("Item") or {})
        for transaction in subject.raw.transactions
        for action in transaction
        for body in action.values()
    ] + [dict(item) for item in subject.raw.items.values()]
    for item in written:
        rendered = json.dumps(item, default=str)
        assert SECRET not in rendered
        assert token not in rendered
        for name, value in item.items():
            if issued.secret_sha256 in str(value):
                assert (item["PK"], item["SK"], name) == (*authoritative, "secret_sha256"), item
    for transaction in subject.raw.transactions:
        rendered = json.dumps(transaction, default=str)
        assert SECRET not in rendered and token not in rendered
    # Every audit item carries the fingerprint, never the digest.
    audit = subject.audit_items()
    assert len(audit) == 2
    assert {item["fingerprint"] for item in audit} == {issued.fingerprint}
    assert all(issued.secret_sha256 not in json.dumps(item, default=str) for item in audit)


@pytest.mark.asyncio
async def test_listing_reads_back_the_authoritative_credential() -> None:
    subject = fake_subject()
    await subject.seed_account(integration_account())
    first = await subject.repository.create_ingest_credential(credential(), AUDIT)
    second = await subject.repository.create_ingest_credential(
        credential(ingest_credential_id=OTHER_CREDENTIAL, slot=2), AUDIT
    )
    revoked = await subject.repository.revoke_ingest_credential(
        TENANT, ACCOUNT, CREDENTIAL, expected_version=1, revoked_at=RECEIVED, audit=AUDIT
    )

    listed = await subject.repository.list_ingest_credentials(TENANT, ACCOUNT)

    assert sorted(listed, key=lambda item: item.ingest_credential_id) == [revoked, second]
    assert revoked.version == first.version + 1
    assert revoked.status == IngestCredentialStatus.REVOKED


@pytest.mark.asyncio
async def test_credential_reads_are_strongly_consistent() -> None:
    """No caching and no stale read: a revoke is visible to the very next authenticate."""
    subject = fake_subject()
    await subject.seed_account(integration_account())
    await subject.repository.create_ingest_credential(credential(), AUDIT)
    token = ingestion_domain.ingest_token(CREDENTIAL, SECRET)

    assert await subject.authenticator.authenticate(token) is not None
    await subject.repository.revoke_ingest_credential(
        TENANT, ACCOUNT, CREDENTIAL, expected_version=1, revoked_at=RECEIVED, audit=AUDIT
    )
    assert await subject.authenticator.authenticate(token) is None

    await subject.repository.list_ingest_credentials(TENANT, ACCOUNT)
    assert subject.raw.reads
    assert all(consistent for _, _, consistent in subject.raw.reads)


class AuditCollision(FaultingClient):
    """Before a transaction runs, stores a copy of its audit item, so the audit Put (conditioned
    on `attribute_not_exists(PK)`) fails inside that transaction."""

    def __init__(self, inner: Any, audit_table: str) -> None:
        super().__init__(inner)
        self.audit_table = audit_table

    def transact_write_items(self, **kwargs: Any) -> Any:
        for action in kwargs["TransactItems"]:
            put = action.get("Put")
            if put is not None and put["TableName"] == self.audit_table:
                self.inner.put_item(TableName=self.audit_table, Item=put["Item"])
        return super().transact_write_items(**kwargs)


@pytest.mark.asyncio
async def test_an_audit_write_failure_aborts_the_change() -> None:
    """The audit Put shares the transaction: when it cannot be written, nothing is."""
    subject = fake_subject()
    await subject.seed_account(integration_account())
    colliding = DynamoIngestCredentialRepository(
        TABLE,
        AuditCollision(subject.raw, AUDIT_TABLE),
        clock=lambda: RECEIVED,
        audit_table_name=AUDIT_TABLE,
    )

    with pytest.raises((ConflictError, ClientError)):
        await colliding.create_ingest_credential(credential(), AUDIT)

    assert await subject.repository.get_ingest_credential(CREDENTIAL) is None
    assert await subject.repository.list_ingest_credentials(TENANT, ACCOUNT) == []
    slot = KEYS.ingest_credential_slot(TENANT, ACCOUNT, 1)
    assert (slot["PK"], slot["SK"]) not in subject.raw.items

    await subject.repository.create_ingest_credential(credential(), AUDIT)
    with pytest.raises((ConflictError, ClientError)):
        await colliding.revoke_ingest_credential(
            TENANT, ACCOUNT, CREDENTIAL, expected_version=1, revoked_at=RECEIVED, audit=AUDIT
        )

    assert await subject.repository.get_ingest_credential(CREDENTIAL) == credential()
    assert (slot["PK"], slot["SK"]) in subject.raw.items


@pytest.mark.asyncio
async def test_audit_item_records_actor_tenant_resource_and_statuses() -> None:
    subject = fake_subject()
    await subject.seed_account(integration_account())
    await subject.repository.create_ingest_credential(credential(), AUDIT)
    revoked_at = CREATED + timedelta(days=3)
    await subject.repository.revoke_ingest_credential(
        TENANT, ACCOUNT, CREDENTIAL, expected_version=1, revoked_at=revoked_at, audit=AUDIT
    )

    issued, revoked = subject.audit_items()

    expected_issue = ingestion_domain.ingest_audit_item(
        action="ingest_credential.issued",
        tenant_id=TENANT,
        resource_id=CREDENTIAL,
        before_status=None,
        after_status="active",
        context=AUDIT,
        now=CREATED,
        fingerprint=credential().fingerprint,
    )
    expected_revoke = ingestion_domain.ingest_audit_item(
        action="ingest_credential.revoked",
        tenant_id=TENANT,
        resource_id=CREDENTIAL,
        before_status="active",
        after_status="revoked",
        context=AUDIT,
        now=revoked_at.astimezone(UTC),
        fingerprint=credential().fingerprint,
    )
    # Random per item, or derived from the adapter's choice of audit instant.
    ignored = {"PK", "SK", "event_id", "created_at", "expires_at"}
    for actual, expected in ((issued, expected_issue), (revoked, expected_revoke)):
        assert {k: v for k, v in actual.items() if k not in ignored} == {
            k: v for k, v in expected.items() if k not in ignored
        }
