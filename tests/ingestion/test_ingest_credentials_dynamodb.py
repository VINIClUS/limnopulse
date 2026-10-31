"""P3-03 ingest credentials against DynamoDB Local 3.3.2: the P3-00 credential and authenticator
kits on the real adapter, the slot race, and the D2/D23 transactions as DynamoDB evaluates them.

Skips unless ``RUN_INGESTION_INTEGRATION=1`` (``make verify-ingestion``).
"""

from __future__ import annotations

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor

import pytest

from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.ingestion import IngestCredential
from limnopulse_api.domain.integrations import IntegrationAccountStatus
from limnopulse_api.services.ingest_credentials import IngestCredentialService
from tests.support.ingestion_contract_kits import (
    ACCOUNT,
    AUDIT,
    CONNECTOR_AUTHENTICATOR_CONTRACT,
    CREDENTIAL,
    CREDENTIAL_REPOSITORY_CONTRACT,
    OTHER_CREDENTIAL,
    OTHER_TENANT,
    RECEIVED,
    TENANT,
    credential,
    integration_account,
    kit_ids,
)
from tests.support.ingestion_harness import IngestionTables, dynamodb_client
from tests.unit.test_dynamodb_ingest_credentials import DynamoCredentialSubject


def _subject(tables: IngestionTables) -> DynamoCredentialSubject:
    return DynamoCredentialSubject(dynamodb_client(), table=tables.domain, audit_table=tables.audit)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario", CREDENTIAL_REPOSITORY_CONTRACT, ids=kit_ids(CREDENTIAL_REPOSITORY_CONTRACT)
)
async def test_repository_passes_the_credential_kit_on_dynamodb_local(
    scenario, ingestion_tables: IngestionTables
) -> None:
    await scenario(_subject(ingestion_tables))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario", CONNECTOR_AUTHENTICATOR_CONTRACT, ids=kit_ids(CONNECTOR_AUTHENTICATOR_CONTRACT)
)
async def test_authenticator_passes_the_kit_on_dynamodb_local(
    scenario, ingestion_tables: IngestionTables
) -> None:
    await scenario(_subject(ingestion_tables))


@pytest.mark.asyncio
async def test_concurrent_issues_for_one_free_slot_have_exactly_one_winner(
    ingestion_tables: IngestionTables,
) -> None:
    subject = _subject(ingestion_tables)
    await subject.seed_account(integration_account())
    contenders = [credential(), credential(ingest_credential_id=OTHER_CREDENTIAL)]

    def create(item: IngestCredential):
        try:
            return asyncio.run(subject.repository.create_ingest_credential(item, AUDIT))
        except ConflictError as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(create, contenders))

    winners = [result for result in results if isinstance(result, IngestCredential)]
    assert len(winners) == 1
    assert sum(isinstance(result, ConflictError) for result in results) == 1
    assert await subject.repository.list_ingest_credentials(TENANT, ACCOUNT) == winners
    assert len(subject.audit_items()) == 1


@pytest.mark.asyncio
async def test_service_rotation_and_immediate_revocation(ingestion_tables: IngestionTables) -> None:
    subject = _subject(ingestion_tables)
    await subject.seed_account(integration_account())
    service = IngestCredentialService(
        credentials=subject.repository,
        integrations=subject.integrations,
        clock=lambda: RECEIVED,
    )

    old = await service.issue(TENANT, ACCOUNT, audit=AUDIT)
    new = await service.issue(TENANT, ACCOUNT, audit=AUDIT)
    with pytest.raises(ConflictError):
        await service.issue(TENANT, ACCOUNT, audit=AUDIT)
    assert await subject.authenticator.authenticate(old.token) is not None

    await service.revoke(
        TENANT, ACCOUNT, old.credential.ingest_credential_id, expected_version=1, audit=AUDIT
    )

    assert await subject.authenticator.authenticate(old.token) is None
    assert await subject.authenticator.authenticate(new.token) is not None
    replacement = await service.issue(TENANT, ACCOUNT, audit=AUDIT)
    assert replacement.credential.slot == old.credential.slot
    rendered = json.dumps(subject.audit_items(), default=str)
    for issued in (old, new, replacement):
        assert issued.token not in rendered
        assert issued.credential.secret_sha256 not in rendered


@pytest.mark.asyncio
async def test_issue_condition_check_rejects_a_changed_account_with_409(
    ingestion_tables: IngestionTables,
) -> None:
    subject = _subject(ingestion_tables)
    for account in (
        integration_account(status=IntegrationAccountStatus.REVOKED),
        integration_account(provider_key="vendor_cloud"),
    ):
        await subject.seed_account(account)
        with pytest.raises(ConflictError) as exc_info:
            await subject.repository.create_ingest_credential(credential(), AUDIT)
        assert not isinstance(exc_info.value, NotFoundError)
    assert subject.audit_items() == []


@pytest.mark.asyncio
async def test_cross_tenant_revoke_is_not_found_and_leaves_the_row(
    ingestion_tables: IngestionTables,
) -> None:
    subject = _subject(ingestion_tables)
    await subject.seed_account(integration_account(tenant_id=OTHER_TENANT))
    foreign = credential(tenant_id=OTHER_TENANT)
    await subject.repository.create_ingest_credential(foreign, AUDIT)

    with pytest.raises(NotFoundError):
        await subject.repository.revoke_ingest_credential(
            TENANT, ACCOUNT, CREDENTIAL, expected_version=1, revoked_at=RECEIVED, audit=AUDIT
        )

    assert await subject.repository.get_ingest_credential(CREDENTIAL) == foreign
    assert len(subject.audit_items()) == 1
