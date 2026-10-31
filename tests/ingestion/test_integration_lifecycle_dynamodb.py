"""P3-04 against DynamoDB Local: the resolver, lifecycle, consistent Device read and trusted
mapping kits (including the `STORE_READS` fault scenarios), plus the atomic DI/EXTERNAL_ID
transition, the partial account update and the D23 audit items in the real audit table.

Skipped unless `RUN_INGESTION_INTEGRATION=1` (`make verify-ingestion`).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import pytest

from limnopulse_api.adapters.dynamodb_v2 import account_scoped_identity_hash
from limnopulse_api.api.v2.schemas.integrations import DeviceIntegrationCreate
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.ingestion import IngressAdmission
from limnopulse_api.domain.integrations import DeviceIntegrationStatus, IntegrationAccountStatus
from limnopulse_api.services.integrations import IntegrationService
from tests.support.ingestion_contract_kits import (
    ACCOUNT,
    AUDIT,
    CONSISTENT_DEVICE_READER_CONTRACT,
    DEVICE,
    EXTERNAL_IDENTITY_RESOLVER_CONTRACT,
    IDENTITY_HASH,
    INTEGRATION,
    INTEGRATION_LIFECYCLE_CONTRACT,
    OTHER_ACCOUNT,
    OTHER_DEVICE,
    OTHER_TENANT,
    PROVIDER,
    RECEIVED,
    SOURCE,
    TENANT,
    TRUSTED_MAPPING_CONTRACT,
    batch,
    component,
    device,
    device_integration,
    integration_account,
    kit_ids,
    store_errors,
)
from tests.support.ingestion_harness import IngestionTables, dynamodb_client
from tests.unit.test_dynamodb_integration_resolver import (
    AUDIT_ENTITY_TYPE,
    DynamoIngestionSubject,
)
from tests.unit.test_trusted_mapping import with_mapping

pytestmark = pytest.mark.asyncio
_AUDIT_TENANTS = (TENANT, OTHER_TENANT)


def _audit_reader(client: Any, table: str, subject: DynamoIngestionSubject) -> Callable[[], Any]:
    def read() -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for tenant_id in _AUDIT_TENANTS:
            response = client.query(
                TableName=table,
                KeyConditionExpression="PK = :pk",
                ExpressionAttributeValues={
                    ":pk": {"S": f"TENANT#{tenant_id}#MONTH#{RECEIVED:%Y-%m}"}
                },
                ConsistentRead=True,
            )
            items += [subject.integrations.deserialize_item(item) for item in response["Items"]]
        return [item for item in items if item.get("entity_type") == AUDIT_ENTITY_TYPE]

    return read


@pytest.fixture
def subject(ingestion_tables: IngestionTables) -> DynamoIngestionSubject:
    client = dynamodb_client()
    made = DynamoIngestionSubject(
        client, domain_table=ingestion_tables.domain, audit_table=ingestion_tables.audit
    )
    made._audit_reader = _audit_reader(client, ingestion_tables.audit, made)
    return with_mapping(made)


@pytest.mark.parametrize(
    "scenario",
    EXTERNAL_IDENTITY_RESOLVER_CONTRACT,
    ids=kit_ids(EXTERNAL_IDENTITY_RESOLVER_CONTRACT),
)
async def test_resolver_kit_on_dynamodb_local(
    subject: DynamoIngestionSubject, scenario: Callable[[Any], Any]
) -> None:
    await scenario(subject)


@pytest.mark.parametrize(
    "scenario", INTEGRATION_LIFECYCLE_CONTRACT, ids=kit_ids(INTEGRATION_LIFECYCLE_CONTRACT)
)
async def test_lifecycle_kit_on_dynamodb_local(
    subject: DynamoIngestionSubject, scenario: Callable[[Any], Any]
) -> None:
    await scenario(subject)


@pytest.mark.parametrize(
    "scenario",
    CONSISTENT_DEVICE_READER_CONTRACT,
    ids=kit_ids(CONSISTENT_DEVICE_READER_CONTRACT),
)
async def test_consistent_device_reader_kit_on_dynamodb_local(
    subject: DynamoIngestionSubject, scenario: Callable[[Any], Any]
) -> None:
    await scenario(subject)


@pytest.mark.parametrize(
    "scenario", TRUSTED_MAPPING_CONTRACT, ids=kit_ids(TRUSTED_MAPPING_CONTRACT)
)
async def test_trusted_mapping_kit_on_dynamodb_local(
    subject: DynamoIngestionSubject, scenario: Callable[[Any], Any]
) -> None:
    await scenario(subject)


async def test_device_integration_transition_is_atomic_with_the_lookup_row(
    subject: DynamoIngestionSubject,
) -> None:
    await subject.seed_device_integration(device_integration())
    keys = subject.integrations.keys

    await subject.repository.transition_device_integration(
        TENANT,
        INTEGRATION,
        DeviceIntegrationStatus.DECOMMISSIONED,
        expected_version=1,
        updated_at=RECEIVED,
        audit=AUDIT,
    )
    with pytest.raises(ConflictError):
        await subject.repository.transition_device_integration(
            TENANT,
            INTEGRATION,
            DeviceIntegrationStatus.ERROR,
            expected_version=1,
            updated_at=RECEIVED,
            audit=AUDIT,
        )

    row = subject.item(keys.device_integration(TENANT, INTEGRATION))
    lookup = subject.item(keys.external_identity(PROVIDER, IDENTITY_HASH))
    assert (row["status"], row["version"]) == ("decommissioned", 2)
    assert (lookup["status"], lookup["version"]) == ("decommissioned", 2)
    assert lookup["device_integration_id"] == INTEGRATION
    (audit,) = subject.audit_items()
    assert (audit["before_status"], audit["after_status"]) == ("active", "decommissioned")


async def test_account_transition_keeps_unknown_attributes(
    subject: DynamoIngestionSubject,
) -> None:
    await subject.seed_account(
        integration_account(status=IntegrationAccountStatus.PENDING), future_attribute="kept"
    )

    await subject.repository.transition_integration_account(
        TENANT,
        ACCOUNT,
        IntegrationAccountStatus.ACTIVE,
        expected_version=1,
        updated_at=RECEIVED,
        audit=AUDIT,
    )

    item = subject.item(subject.integrations.keys.integration_account(TENANT, ACCOUNT))
    assert (item["status"], item["version"], item["future_attribute"]) == ("active", 2, "kept")
    (audit,) = subject.audit_items()
    assert audit["actor_id"] == AUDIT.actor_id
    assert audit["resource_id"] == ACCOUNT


async def test_https_namespace_on_dynamodb_local(subject: DynamoIngestionSubject) -> None:
    for tenant_id, account_id, device_id in (
        (TENANT, ACCOUNT, DEVICE),
        (OTHER_TENANT, OTHER_ACCOUNT, OTHER_DEVICE),
    ):
        await subject.seed_account(
            integration_account(tenant_id=tenant_id, integration_account_id=account_id)
        )
        await subject.seed_device(device(tenant_id=tenant_id, device_id=device_id))
    ids = iter(["di_" + "a" * 32, "di_" + "b" * 32, "di_" + "c" * 32])
    service = IntegrationService(
        subject.integrations,
        subject.devices,
        clock=lambda: RECEIVED,
        device_integration_id_factory=lambda: next(ids),
    )

    async def create(tenant_id: str, device_id: str, account_id: str, key: str) -> dict:
        result = await service.create_device_integration(
            tenant_id,
            device_id,
            DeviceIntegrationCreate(integration_account_id=account_id, external_identity="pond-1"),
            key,
        )
        return json.loads(result.snapshot_json)

    mine = await create(TENANT, DEVICE, ACCOUNT, "namespace-key-01")
    theirs = await create(OTHER_TENANT, OTHER_DEVICE, OTHER_ACCOUNT, "namespace-key-02")
    with pytest.raises(ConflictError, match="external identity already claimed"):
        await create(TENANT, DEVICE, ACCOUNT, "namespace-key-03")

    assert mine["external_identity_hash"] == account_scoped_identity_hash(
        PROVIDER, ACCOUNT, "pond-1"
    )
    assert theirs["external_identity_hash"] == account_scoped_identity_hash(
        PROVIDER, OTHER_ACCOUNT, "pond-1"
    )


@pytest.mark.parametrize("error", store_errors(), ids=lambda error: type(error).__name__)
async def test_admit_propagates_an_account_read_failure_on_dynamodb_local(
    subject: DynamoIngestionSubject, error: BaseException
) -> None:
    await subject.seed_account(integration_account())
    await subject.seed_device_integration(device_integration())
    await subject.seed_device(device())
    await subject.seed_component(component())
    subject.fail_next("get_integration_account", error)

    with pytest.raises(type(error)) as caught:
        await subject.mapping.admit(SOURCE, batch())

    assert caught.value is error
    assert isinstance(await subject.mapping.admit(SOURCE, batch()), IngressAdmission)


@pytest.mark.parametrize("kind", ["account", "integration"])
async def test_a_concurrent_write_is_a_conflict_on_dynamodb_local(
    subject: DynamoIngestionSubject, kind: str
) -> None:
    # The version condition must hold at commit on the real store; the audit item rolls back.
    keys = subject.integrations.keys
    if kind == "account":
        await subject.seed_account(integration_account())
        key = keys.integration_account(TENANT, ACCOUNT)
    else:
        await subject.seed_device_integration(device_integration())
        key = keys.device_integration(TENANT, INTEGRATION)
    subject.client.before_next(
        "transact_write_items", lambda: subject.put({**subject.item(key), "version": 2})
    )

    with pytest.raises(ConflictError) as caught:
        if kind == "account":
            await subject.repository.transition_integration_account(
                TENANT,
                ACCOUNT,
                IntegrationAccountStatus.ERROR,
                expected_version=1,
                updated_at=RECEIVED,
                audit=AUDIT,
            )
        else:
            await subject.repository.transition_device_integration(
                TENANT,
                INTEGRATION,
                DeviceIntegrationStatus.ERROR,
                expected_version=1,
                updated_at=RECEIVED,
                audit=AUDIT,
            )

    assert not isinstance(caught.value, NotFoundError)
    assert subject.item(key)["status"] == "active"
    assert subject.audit_items() == []
