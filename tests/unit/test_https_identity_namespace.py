"""P3-04 (D4, D22): HTTPS DeviceIntegration identities are scoped to their IntegrationAccount.

`IntegrationService` hashes an HTTPS account's identity with `account_scoped_identity_hash`, so two
tenants (or two accounts) can each claim the same `external_device_id`, while one account still
claims it once. Other providers keep the M1 global hash unchanged.
"""

import json
from datetime import UTC, datetime
from typing import Any

import pytest

from limnopulse_api.adapters.dynamodb_v2 import (
    account_scoped_identity_hash,
    external_identity_hash,
)
from limnopulse_api.api.v2.schemas.integrations import DeviceIntegrationCreate
from limnopulse_api.core.errors import ConflictError
from limnopulse_api.domain.ingestion import (
    HTTPS_PROVIDER_KEYS,
    AuthenticatedSource,
    IngestObservationBatch,
    IngressAdmission,
    IngressAdmissionDenied,
    IngressDenial,
)
from limnopulse_api.domain.integrations import DeviceIntegrationStatus, IntegrationAccountStatus
from limnopulse_api.services.integrations import IntegrationService
from tests.support.ingestion_contract_kits import (
    ACCOUNT,
    AUDIT,
    COMPONENT,
    DEVICE,
    OTHER_ACCOUNT,
    OTHER_DEVICE,
    OTHER_TENANT,
    TENANT,
    component,
    device,
    device_integration,
    integration_account,
)
from tests.unit.test_dynamodb_integration_resolver import DynamoIngestionSubject, fake_subject
from tests.unit.test_trusted_mapping import with_mapping

NOW = datetime(2026, 10, 31, 9, 0, tzinfo=UTC)
POND = "pond-1"
HTTPS = "https_direct"
VENDOR = "vendor.cloud"
THIRD_ACCOUNT = "ia_" + "9" * 32
_ids = iter(f"di_{index:032x}" for index in range(1, 1_000))


def service(subject: DynamoIngestionSubject) -> IntegrationService:
    return IntegrationService(
        subject.integrations,
        subject.devices,
        clock=lambda: NOW,
        device_integration_id_factory=lambda: next(_ids),
    )


async def seed_world(subject: DynamoIngestionSubject, *, provider_key: str = HTTPS) -> None:
    for tenant_id, account_id, device_id in (
        (TENANT, ACCOUNT, DEVICE),
        (TENANT, THIRD_ACCOUNT, DEVICE),
        (OTHER_TENANT, OTHER_ACCOUNT, OTHER_DEVICE),
    ):
        await subject.seed_account(
            integration_account(
                tenant_id=tenant_id, integration_account_id=account_id, provider_key=provider_key
            )
        )
        await subject.seed_device(device(tenant_id=tenant_id, device_id=device_id))


async def create(
    subject: DynamoIngestionSubject,
    tenant_id: str,
    device_id: str,
    account_id: str,
    identity: str = POND,
    key: str = "namespace-key-01",
) -> dict[str, Any]:
    result = await service(subject).create_device_integration(
        tenant_id,
        device_id,
        DeviceIntegrationCreate(integration_account_id=account_id, external_identity=identity),
        key,
    )
    return json.loads(result.snapshot_json)


@pytest.mark.asyncio
async def test_two_tenants_https_accounts_each_claim_the_same_external_id() -> None:
    subject = fake_subject()
    await seed_world(subject)

    mine = await create(subject, TENANT, DEVICE, ACCOUNT)
    theirs = await create(subject, OTHER_TENANT, OTHER_DEVICE, OTHER_ACCOUNT)

    assert mine["external_identity_hash"] == account_scoped_identity_hash(HTTPS, ACCOUNT, POND)
    assert theirs["external_identity_hash"] == account_scoped_identity_hash(
        HTTPS, OTHER_ACCOUNT, POND
    )
    assert mine["external_identity_hash"] != theirs["external_identity_hash"]
    assert mine["external_identity_hash"] != external_identity_hash(HTTPS, POND)


@pytest.mark.asyncio
async def test_two_https_accounts_of_one_tenant_each_claim_the_same_external_id() -> None:
    subject = fake_subject()
    await seed_world(subject)

    first = await create(subject, TENANT, DEVICE, ACCOUNT)
    second = await create(subject, TENANT, DEVICE, THIRD_ACCOUNT, key="namespace-key-02")

    assert first["external_identity_hash"] != second["external_identity_hash"]


@pytest.mark.asyncio
async def test_one_https_account_still_claims_an_identity_once() -> None:
    subject = fake_subject()
    await seed_world(subject)
    await create(subject, TENANT, DEVICE, ACCOUNT)

    with pytest.raises(ConflictError, match="external identity already claimed") as caught:
        await create(subject, TENANT, DEVICE, ACCOUNT, key="namespace-key-02")

    assert POND not in str(caught.value)


@pytest.mark.asyncio
async def test_non_https_providers_keep_the_global_hash() -> None:
    subject = fake_subject()
    await seed_world(subject, provider_key=VENDOR)

    created = await create(subject, TENANT, DEVICE, ACCOUNT)

    assert created["external_identity_hash"] == external_identity_hash(VENDOR, POND)
    with pytest.raises(ConflictError, match="external identity already claimed"):
        await create(subject, OTHER_TENANT, OTHER_DEVICE, OTHER_ACCOUNT, key="namespace-key-02")


@pytest.mark.asyncio
async def test_only_providers_in_the_https_set_are_account_scoped() -> None:
    # Membership in HTTPS_PROVIDER_KEYS decides, never a name that merely looks like HTTPS.
    lookalike = "https_legacy"
    assert lookalike not in HTTPS_PROVIDER_KEYS
    subject = fake_subject()
    await seed_world(subject, provider_key=lookalike)

    created = await create(subject, TENANT, DEVICE, ACCOUNT)

    assert created["external_identity_hash"] == external_identity_hash(lookalike, POND)


@pytest.mark.asyncio
async def test_raw_external_id_is_never_stored() -> None:
    subject = fake_subject()
    await seed_world(subject)
    secretish = "sensor-raw-SN-777"

    await create(subject, TENANT, DEVICE, ACCOUNT, identity=secretish)

    assert all(
        secretish not in json.dumps(item, default=str) for item in subject.store.items.values()
    )


def _source(tenant_id: str, account_id: str) -> AuthenticatedSource:
    return AuthenticatedSource(
        tenant_id=tenant_id,
        integration_account_id=account_id,
        provider_key=HTTPS,
        ingest_credential_id="ic_" + "a" * 32,
    )


def _batch(external_device_id: str = POND) -> IngestObservationBatch:
    return IngestObservationBatch.model_validate(
        {
            "external_device_id": external_device_id,
            "observations": [
                {
                    "component_id": COMPONENT,
                    "metric_key": "water.temperature",
                    "value": 21.0,
                    "source_unit": "degC",
                    "observed_at": "2026-10-31T08:59:00Z",
                }
            ],
        }
    )


@pytest.mark.asyncio
async def test_an_identity_created_by_the_service_resolves_at_admission() -> None:
    subject = with_mapping(fake_subject())
    await seed_world(subject)
    await subject.seed_component(component())
    created = await create(subject, TENANT, DEVICE, ACCOUNT)
    await subject.repository.transition_device_integration(
        TENANT,
        created["device_integration_id"],
        DeviceIntegrationStatus.ACTIVE,
        expected_version=1,
        updated_at=NOW,
        audit=AUDIT,
    )

    admission = await subject.mapping.admit(_source(TENANT, ACCOUNT), _batch())

    assert isinstance(admission, IngressAdmission)
    assert admission.device_integration.device_integration_id == created["device_integration_id"]
    # Another account presenting the same external id resolves nothing.
    assert await subject.mapping.admit(_source(TENANT, THIRD_ACCOUNT), _batch()) == IngressDenial(
        reason=IngressAdmissionDenied.IDENTITY_UNRESOLVED, observation_index=None
    )


@pytest.mark.asyncio
async def test_https_integrations_from_before_p3_04_never_resolve() -> None:
    # D22: a DeviceIntegration created with the global hash under `https_direct` fails closed.
    subject = with_mapping(fake_subject())
    await subject.seed_account(integration_account(status=IntegrationAccountStatus.ACTIVE))
    await subject.seed_device(device())
    await subject.seed_component(component())
    await subject.seed_device_integration(
        device_integration(external_identity_hash=external_identity_hash(HTTPS, POND))
    )

    assert await subject.mapping.admit(_source(TENANT, ACCOUNT), _batch()) == IngressDenial(
        reason=IngressAdmissionDenied.IDENTITY_UNRESOLVED, observation_index=None
    )
