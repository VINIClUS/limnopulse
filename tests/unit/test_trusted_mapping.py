"""P3-04: `TrustedMappingService` admission (D4) and fence (D8 step 2) over the real adapters.

The P3-00 trusted-mapping kit (the frozen condition -> enum table, the component rule and the
`STORE_READS` fault scenarios) runs against the service composed over `DynamoIntegrationRepository`
and `DynamoDeviceComponentRepository` on the in-memory Dynamo fake; the same composition runs on
DynamoDB Local in `tests/ingestion/test_integration_lifecycle_dynamodb.py`.
"""

from collections.abc import Callable
from typing import Any

import pytest
from limnopulse_api.services.trusted_mapping import TrustedMappingService

from limnopulse_api.adapters.dynamodb_v2 import account_scoped_identity_hash
from limnopulse_api.domain.devices_v2 import ComponentStatus
from limnopulse_api.domain.ingestion import (
    AuthenticatedSource,
    IngestObservationBatch,
    IngressAdmission,
    IngressAdmissionDenied,
    IngressDenial,
    IngressFence,
    IngressFenceReason,
    TrustedIngress,
)
from limnopulse_api.domain.telemetry_v2 import SourceObservation
from limnopulse_api.repositories import ingestion as ports
from tests.support.ingestion_contract_kits import (
    COMPONENT,
    CREDENTIAL,
    DEFAULT_COMPONENT,
    DEVICE,
    EXTERNAL_DEVICE,
    OBSERVED,
    OTHER_ACCOUNT,
    OTHER_COMPONENT,
    OTHER_DEVICE,
    OTHER_TENANT,
    PROVIDER,
    SOURCE,
    TENANT,
    TRUSTED_MAPPING_CONTRACT,
    batch,
    component,
    device,
    device_integration,
    envelope,
    integration_account,
    kit_ids,
)
from tests.unit.test_dynamodb_integration_resolver import DynamoIngestionSubject, fake_subject

OTHER_INTEGRATION_ID = "di_" + "5" * 32


def with_mapping(subject: DynamoIngestionSubject) -> DynamoIngestionSubject:
    subject.mapping = TrustedMappingService(
        resolver=subject.integrations,
        integrations=subject.integrations,
        devices=subject.devices,
        components=subject.devices,
    )
    return subject


def mapping_subject() -> DynamoIngestionSubject:
    return with_mapping(fake_subject())


async def seed_full(subject: DynamoIngestionSubject, *components: Any) -> None:
    await subject.seed_account(integration_account())
    await subject.seed_device_integration(device_integration())
    await subject.seed_device(device())
    for item in components or (component(),):
        await subject.seed_component(item)


def explicit_batch(*observations: dict[str, Any]) -> IngestObservationBatch:
    return IngestObservationBatch.model_validate(
        {"external_device_id": EXTERNAL_DEVICE, "observations": list(observations)}
    )


def reading(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "metric_key": "water.temperature",
        "value": 21.5,
        "source_unit": "degC",
        "observed_at": OBSERVED.isoformat(),
    }
    payload.update(overrides)
    return payload


# Contract kit ------------------------------------------------------------------------------


def test_service_implements_the_trusted_mapping_port() -> None:
    assert isinstance(mapping_subject().mapping, ports.TrustedMapping)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario", TRUSTED_MAPPING_CONTRACT, ids=kit_ids(TRUSTED_MAPPING_CONTRACT)
)
async def test_trusted_mapping_passes_the_p3_00_kit(scenario: Callable[[Any], Any]) -> None:
    await scenario(mapping_subject())


# Admission: ADR-016 ownership ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_two_tenants_resolve_their_own_mapping_for_the_same_external_id() -> None:
    subject = mapping_subject()
    other_hash = account_scoped_identity_hash(PROVIDER, OTHER_ACCOUNT, EXTERNAL_DEVICE)
    await seed_full(subject)
    await subject.seed_account(
        integration_account(tenant_id=OTHER_TENANT, integration_account_id=OTHER_ACCOUNT)
    )
    await subject.seed_device_integration(
        device_integration(
            tenant_id=OTHER_TENANT,
            integration_account_id=OTHER_ACCOUNT,
            device_integration_id=OTHER_INTEGRATION_ID,
            device_id=OTHER_DEVICE,
            external_identity_hash=other_hash,
        )
    )
    await subject.seed_device(device(tenant_id=OTHER_TENANT, device_id=OTHER_DEVICE))
    await subject.seed_component(
        component(OTHER_COMPONENT, tenant_id=OTHER_TENANT, device_id=OTHER_DEVICE)
    )
    other_source = AuthenticatedSource(
        tenant_id=OTHER_TENANT,
        integration_account_id=OTHER_ACCOUNT,
        provider_key=PROVIDER,
        ingest_credential_id="ic_" + "e" * 32,
    )

    mine = await subject.mapping.admit(SOURCE, batch(COMPONENT))
    theirs = await subject.mapping.admit(other_source, batch(OTHER_COMPONENT))

    assert isinstance(mine, IngressAdmission) and isinstance(theirs, IngressAdmission)
    assert (mine.device_integration.tenant_id, mine.device.device_id) == (TENANT, DEVICE)
    assert (theirs.device_integration.tenant_id, theirs.device.device_id) == (
        OTHER_TENANT,
        OTHER_DEVICE,
    )
    # One tenant's selector never reaches the other tenant's Components.
    assert await subject.mapping.admit(SOURCE, batch(OTHER_COMPONENT)) == IngressDenial(
        reason=IngressAdmissionDenied.COMPONENT_INVALID, observation_index=0
    )
    assert await subject.mapping.admit(other_source, batch(COMPONENT)) == IngressDenial(
        reason=IngressAdmissionDenied.COMPONENT_INVALID, observation_index=0
    )


@pytest.mark.asyncio
async def test_admission_hashes_with_the_authenticated_account() -> None:
    # The same external id under another account of the same tenant is another identity.
    subject = mapping_subject()
    await seed_full(subject)
    source = AuthenticatedSource(
        tenant_id=TENANT,
        integration_account_id=OTHER_ACCOUNT,
        provider_key=PROVIDER,
        ingest_credential_id=CREDENTIAL,
    )
    await subject.seed_account(integration_account(integration_account_id=OTHER_ACCOUNT))

    assert await subject.mapping.admit(source, batch(COMPONENT)) == IngressDenial(
        reason=IngressAdmissionDenied.IDENTITY_UNRESOLVED, observation_index=None
    )


@pytest.mark.asyncio
async def test_admission_reads_are_strongly_consistent() -> None:
    subject = mapping_subject()
    await seed_full(subject)

    assert isinstance(await subject.mapping.admit(SOURCE, batch(COMPONENT)), IngressAdmission)

    for pk_prefix, sk_prefix in (
        ("EXTERNAL_ID#", None),
        ("TENANT#", "DEVICE_INTEGRATION#"),
        ("TENANT#", "INTEGRATION_ACCOUNT#"),
        ("TENANT#", "DEVICE#"),
    ):
        reads = subject.client.reads(pk_prefix=pk_prefix, sk_prefix=sk_prefix)
        assert reads, (pk_prefix, sk_prefix)
        assert all(read.consistent for read in reads), (pk_prefix, sk_prefix)


@pytest.mark.asyncio
async def test_admission_reads_each_distinct_component_once_and_never_lists() -> None:
    subject = mapping_subject()
    await seed_full(subject, component(), component(DEFAULT_COMPONENT))
    request = batch(COMPONENT, COMPONENT, None, COMPONENT, None)

    admission = await subject.mapping.admit(SOURCE, request)

    assert isinstance(admission, IngressAdmission)
    component_reads = sorted(
        read.keys[0][0] for read in subject.client.reads(pk_prefix="COMPONENT#")
    )
    assert component_reads == sorted({f"COMPONENT#{COMPONENT}", f"COMPONENT#{DEFAULT_COMPONENT}"})
    assert not [call for call in subject.client.calls if call.operation == "query"]
    assert [item.component_id for item in admission.observations] == [
        COMPONENT,
        COMPONENT,
        DEFAULT_COMPONENT,
        COMPONENT,
        DEFAULT_COMPONENT,
    ]


@pytest.mark.asyncio
async def test_admission_never_writes() -> None:
    subject = mapping_subject()
    await seed_full(subject)

    await subject.mapping.admit(SOURCE, batch(COMPONENT))
    await subject.mapping.fence(envelope())

    assert {call.operation for call in subject.client.calls} <= {"get_item"}


# Admission: component rule ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_explicit_active_component_passes_through_unchanged() -> None:
    subject = mapping_subject()
    await seed_full(subject)
    request = explicit_batch(
        reading(
            component_id=COMPONENT,
            sequence=41,
            source_event_id="evt-0041",
            source_metadata={"firmware_version": "1.2.3"},
        ),
        reading(component_id=COMPONENT, metric_key="water.ph", value=7.2, source_unit="pH"),
    )

    admission = await subject.mapping.admit(SOURCE, request)

    assert isinstance(admission, IngressAdmission)
    expected = tuple(
        SourceObservation.model_validate(item.model_dump(mode="json"))
        for item in request.observations
    )
    assert admission.observations == expected
    assert admission.source == SOURCE


@pytest.mark.asyncio
async def test_the_legacy_default_is_filled_only_where_omitted() -> None:
    subject = mapping_subject()
    await seed_full(subject, component(), component(DEFAULT_COMPONENT))
    request = explicit_batch(reading(), reading(component_id=COMPONENT, sequence=7))

    admission = await subject.mapping.admit(SOURCE, request)

    assert isinstance(admission, IngressAdmission)
    assert [item.component_id for item in admission.observations] == [DEFAULT_COMPONENT, COMPONENT]
    assert admission.observations[1].sequence == 7
    assert admission.observations[0].value == 21.5


@pytest.mark.asyncio
async def test_v2_native_device_requires_an_explicit_component() -> None:
    subject = mapping_subject()
    await seed_full(subject)

    assert await subject.mapping.admit(SOURCE, batch(None)) == IngressDenial(
        reason=IngressAdmissionDenied.COMPONENT_REQUIRED, observation_index=0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stored",
    [
        pytest.param(None, id="unknown"),
        pytest.param(component(OTHER_COMPONENT, device_id=OTHER_DEVICE), id="another device"),
        pytest.param(component(OTHER_COMPONENT, tenant_id=OTHER_TENANT), id="another tenant"),
        pytest.param(component(OTHER_COMPONENT, status=ComponentStatus.REPLACED), id="replaced"),
        pytest.param(
            component(OTHER_COMPONENT, status=ComponentStatus.DECOMMISSIONED), id="decommissioned"
        ),
    ],
)
async def test_explicit_foreign_unknown_or_inactive_component_is_invalid(stored: Any) -> None:
    subject = mapping_subject()
    await seed_full(subject)
    if stored is not None:
        await subject.seed_component(stored)

    assert await subject.mapping.admit(SOURCE, batch(COMPONENT, COMPONENT, OTHER_COMPONENT)) == (
        IngressDenial(reason=IngressAdmissionDenied.COMPONENT_INVALID, observation_index=2)
    )


@pytest.mark.asyncio
async def test_the_first_failing_observation_names_the_index() -> None:
    subject = mapping_subject()
    await seed_full(subject)

    # Index 1 is omitted (no legacy default), index 2 is foreign: the first failure wins.
    assert await subject.mapping.admit(SOURCE, batch(COMPONENT, None, OTHER_COMPONENT)) == (
        IngressDenial(reason=IngressAdmissionDenied.COMPONENT_REQUIRED, observation_index=1)
    )


@pytest.mark.asyncio
async def test_an_inactive_mapping_is_denied_before_any_component_read() -> None:
    subject = mapping_subject()
    await seed_full(subject)
    await subject.seed_device(device(status="suspended"))

    assert await subject.mapping.admit(SOURCE, batch(OTHER_COMPONENT)) == IngressDenial(
        reason=IngressAdmissionDenied.DEVICE_INACTIVE, observation_index=None
    )
    assert subject.client.reads(pk_prefix="COMPONENT#") == []


# Fence ---------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fence_reads_are_strongly_consistent() -> None:
    subject = mapping_subject()
    await seed_full(subject)

    assert isinstance(await subject.mapping.fence(envelope()), TrustedIngress)

    for pk_prefix, sk_prefix in (
        ("EXTERNAL_ID#", None),
        ("TENANT#", "DEVICE_INTEGRATION#"),
        ("TENANT#", "INTEGRATION_ACCOUNT#"),
        ("TENANT#", "DEVICE#"),
    ):
        reads = subject.client.reads(pk_prefix=pk_prefix, sk_prefix=sk_prefix)
        assert reads, (pk_prefix, sk_prefix)
        assert all(read.consistent for read in reads), (pk_prefix, sk_prefix)


@pytest.mark.asyncio
async def test_fence_rejects_a_mapping_row_with_an_inconsistent_hash() -> None:
    # The lookup still points at the row, but the row no longer carries the envelope's hash.
    subject = mapping_subject()
    await seed_full(subject)
    keys = subject.integrations.keys
    row = subject.item(keys.device_integration(TENANT, device_integration().device_integration_id))
    subject.put({**row, "external_identity_hash": "f" * 64})

    assert await subject.mapping.fence(envelope()) == IngressFence(
        reason=IngressFenceReason.IDENTITY_UNRESOLVED, account_verified=True
    )


@pytest.mark.asyncio
async def test_fence_verifies_the_account_by_the_envelope_coordinates() -> None:
    # D8 forged-envelope rule: `account_verified` means an IntegrationAccount row exists for
    # (envelope.tenant_id, envelope.integration_account_id), as the frozen `classify_fence` and
    # P3-00 `FakeTrustedMapping` define it. Here the forged tenant really has that account id.
    subject = mapping_subject()
    await seed_full(subject)
    await subject.seed_account(integration_account(tenant_id=OTHER_TENANT))

    assert await subject.mapping.fence(envelope(tenant_id=OTHER_TENANT)) == IngressFence(
        reason=IngressFenceReason.INTEGRATION_MISMATCH, account_verified=True
    )
    assert await subject.mapping.fence(envelope(integration_account_id=OTHER_ACCOUNT)) == (
        IngressFence(reason=IngressFenceReason.INTEGRATION_MISMATCH, account_verified=False)
    )
