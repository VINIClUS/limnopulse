"""P3-00 contract kits: one scenario tuple per P3 port, plus the shared DynamoDB vectors.

A kit scenario is an `async def scenario(subject) -> None`. P3-00 runs every scenario against
the in-memory fakes (`tests/support/ingestion_fakes.py`); the task that implements a real
adapter runs the same scenarios against it (in-memory Dynamo fake in unit tests, real services in
`tests/ingestion/`), so a fake and its adapter cannot drift apart.

Each subject is a small object that exposes the port under test plus seeding helpers; its shape
is documented by the `*KitSubject` protocols below. The kits never assert the error returned for
the same item appearing twice in one transaction (DynamoDB Local and AWS differ there).
"""

import json
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Protocol

import pytest
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError, EndpointConnectionError

from limnopulse_api.adapters.dynamodb_v2 import (
    account_scoped_identity_hash,
    external_identity_hash,
)
from limnopulse_api.core.errors import (
    ConflictError,
    IngressMessageTooLarge,
    IngressQueueError,
    NotFoundError,
)
from limnopulse_api.domain.alerts import AuditContext
from limnopulse_api.domain.devices_v2 import (
    ComponentKind,
    ComponentStatus,
    DeviceComponent,
    DeviceStatus,
    DeviceV2,
    ProbeProfile,
)
from limnopulse_api.domain.ids import default_component_id_for_legacy_device
from limnopulse_api.domain.ingestion import (
    MAX_INGRESS_MESSAGE_BYTES,
    QUARANTINE_TTL,
    AuthenticatedSource,
    ComponentIngestState,
    ComponentMark,
    ConnectorHealth,
    ConnectorMark,
    DeviceIngestState,
    DeviceMark,
    IngestCredential,
    IngestCredentialStatus,
    IngestMarks,
    IngestObservationBatch,
    IngressAdmission,
    IngressAdmissionDenied,
    IngressDefer,
    IngressDeferReason,
    IngressDenial,
    IngressEnvelope,
    IngressFence,
    IngressFenceReason,
    IngressReceipt,
    IngressReceiptState,
    QuarantineEntry,
    QuarantineEntryStatus,
    TrustedIngress,
    build_quarantine_entries,
    build_trusted_context,
    ingest_credential_fingerprint,
    ingest_secret_sha256,
    ingest_token,
    ingress_message_bytes,
    new_ingress_receipt,
)
from limnopulse_api.domain.integrations import (
    DeviceIntegration,
    DeviceIntegrationStatus,
    IntegrationAccount,
    IntegrationAccountStatus,
)
from limnopulse_api.domain.telemetry_v2 import (
    CanonicalObservation,
    MetricValueType,
    NormalizationVersions,
    ObservationDelivery,
    ObservationMeasurement,
    ObservationQuality,
    ObservationResolution,
    ObservationSource,
    QualityFlag,
    QualityProvenance,
    QualityState,
    SourceObservation,
    SourceType,
    observation_event_id,
)

# Shared fixture vocabulary ----------------------------------------------------------------------

TENANT = "tenant-a"
OTHER_TENANT = "tenant-b"
ACCOUNT = "ia_" + "1" * 32
OTHER_ACCOUNT = "ia_" + "6" * 32
CREDENTIAL = "ic_" + "a" * 32
OTHER_CREDENTIAL = "ic_" + "b" * 32
INTEGRATION = "di_" + "2" * 32
OTHER_INTEGRATION = "di_" + "7" * 32
DEVICE = "dev-1"
OTHER_DEVICE = "dev-2"
COMPONENT = "cmp_" + "3" * 32
OTHER_COMPONENT = "cmp_" + "8" * 32
DEFAULT_COMPONENT = default_component_id_for_legacy_device(TENANT, DEVICE)
INGRESS = "ing_" + "4" * 32
PROVIDER = "https_direct"
EXTERNAL_DEVICE = "sensor-01"
IDENTITY_HASH = account_scoped_identity_hash(PROVIDER, ACCOUNT, EXTERNAL_DEVICE)
SECRET = "A" * 21 + "_-" + "b" * 20  # 43 base64url characters, including "_" and "-"
CREATED = datetime(2026, 1, 1, tzinfo=UTC)
RECEIVED = datetime(2026, 10, 30, 12, 0, tzinfo=UTC)
OBSERVED = datetime(2026, 10, 30, 11, 59, tzinfo=UTC)
AUDIT = AuditContext(actor_id="user-1", ip="203.0.113.7", user_agent="kit")
KEY_SHA = "c" * 64
REQUEST_SHA = "d" * 64
EMOJI = "\U0001f600"


def source_observation(**overrides: Any) -> SourceObservation:
    payload: dict[str, Any] = {
        "component_id": COMPONENT,
        "metric_key": "temperature",
        "value": 27.5,
        "source_unit": "C",
        "observed_at": OBSERVED,
    }
    payload.update(overrides)
    return SourceObservation.model_validate(payload)


def envelope(**overrides: Any) -> IngressEnvelope:
    payload: dict[str, Any] = {
        "schema": "telemetry-ingress@1",
        "ingress_id": INGRESS,
        "tenant_id": TENANT,
        "integration_account_id": ACCOUNT,
        "provider_key": PROVIDER,
        "external_identity_hash": IDENTITY_HASH,
        "device_integration_id": INTEGRATION,
        "device_id": DEVICE,
        "source_type": "https",
        "received_at": RECEIVED,
        "mapping_versions": NormalizationVersions(),
        "observations": [source_observation()],
    }
    payload.update(overrides)
    return IngressEnvelope.model_validate(payload)


def integration_account(**overrides: Any) -> IntegrationAccount:
    payload: dict[str, Any] = {
        "tenant_id": TENANT,
        "integration_account_id": ACCOUNT,
        "provider_key": PROVIDER,
        "display_name": "Direct HTTPS",
        "status": IntegrationAccountStatus.ACTIVE,
        "created_at": CREATED,
        "updated_at": CREATED,
        "version": 1,
    }
    payload.update(overrides)
    return IntegrationAccount.model_validate(payload)


def device_integration(**overrides: Any) -> DeviceIntegration:
    payload: dict[str, Any] = {
        "tenant_id": TENANT,
        "device_integration_id": INTEGRATION,
        "device_id": DEVICE,
        "integration_account_id": ACCOUNT,
        "provider_key": PROVIDER,
        "external_identity_hash": IDENTITY_HASH,
        "status": DeviceIntegrationStatus.ACTIVE,
        "created_at": CREATED,
        "updated_at": CREATED,
        "version": 1,
    }
    payload.update(overrides)
    return DeviceIntegration.model_validate(payload)


def device(**overrides: Any) -> DeviceV2:
    payload: dict[str, Any] = {
        "tenant_id": TENANT,
        "device_id": DEVICE,
        "name": "Probe station",
        "status": DeviceStatus.ACTIVE,
        "created_at": CREATED,
        "updated_at": CREATED,
        "version": 1,
    }
    payload.update(overrides)
    return DeviceV2.model_validate(payload)


def component(
    component_id: str = COMPONENT,
    *,
    tenant_id: str = TENANT,
    device_id: str = DEVICE,
    status: ComponentStatus = ComponentStatus.ACTIVE,
) -> DeviceComponent:
    return DeviceComponent(
        tenant_id=tenant_id,
        component_id=component_id,
        device_id=device_id,
        kind=ComponentKind.PROBE,
        name="probe",
        probe_profile=ProbeProfile(probe_profile_id="prb_" + "9" * 32),
        status=status,
        created_at=CREATED,
        updated_at=CREATED,
        version=1,
    )


def credential(**overrides: Any) -> IngestCredential:
    digest = ingest_secret_sha256(SECRET)
    payload: dict[str, Any] = {
        "tenant_id": TENANT,
        "ingest_credential_id": CREDENTIAL,
        "integration_account_id": ACCOUNT,
        "slot": 1,
        "secret_sha256": digest,
        "fingerprint": ingest_credential_fingerprint(digest),
        "status": IngestCredentialStatus.ACTIVE,
        "revoked_at": None,
        "created_at": CREATED,
        "updated_at": CREATED,
        "version": 1,
    }
    payload.update(overrides)
    return IngestCredential.model_validate(payload)


def receipt(
    *,
    ingress_id: str = INGRESS,
    integration_account_id: str = ACCOUNT,
    key_sha256: str = KEY_SHA,
    created_at: datetime = RECEIVED,
) -> IngressReceipt:
    return new_ingress_receipt(
        integration_account_id=integration_account_id,
        tenant_id=TENANT,
        key_sha256=key_sha256,
        request_sha256=REQUEST_SHA,
        ingress_id=ingress_id,
        received_at=created_at,
        mapping_versions=NormalizationVersions(),
        created_at=created_at,
    )


def fenced_entries(
    count: int = 3, *, tenant_id: str = TENANT, now: datetime = RECEIVED
) -> tuple[QuarantineEntry, ...]:
    observations = [
        source_observation(observed_at=OBSERVED + timedelta(seconds=index))
        for index in range(count)
    ]
    return build_quarantine_entries(
        envelope(tenant_id=tenant_id, observations=observations),
        fence=IngressFence(
            reason=IngressFenceReason.INTEGRATION_DECOMMISSIONED, account_verified=True
        ),
        now=now,
    )


def marks(
    *,
    tenant_id: str = TENANT,
    component_id: str = COMPONENT,
    integration_account_id: str = ACCOUNT,
    received_at: datetime = RECEIVED,
    ingested_at: datetime | None = None,
    observed_at: datetime | None = OBSERVED,
    sequence: int | None = 10,
    quarantined_at: datetime | None = None,
    quarantine_reason: str | None = None,
) -> IngestMarks:
    ingested = ingested_at or received_at + timedelta(seconds=1)
    return IngestMarks(
        tenant_id=tenant_id,
        device_id=DEVICE,
        integration_account_id=integration_account_id,
        component_marks=(
            ComponentMark(
                component_id=component_id,
                received_at=received_at,
                ingested_at=ingested,
                observed_at=observed_at,
                sequence=sequence,
            ),
        ),
        device_mark=DeviceMark(received_at=received_at, ingested_at=ingested),
        connector_mark=ConnectorMark(
            received_at=received_at,
            accepted_at=received_at,
            quarantined_at=quarantined_at,
            quarantine_reason=quarantine_reason,
        ),
    )


def canonical_observation(
    *,
    component_id: str = COMPONENT,
    metric_key: str = "temperature",
    observed_at: datetime = OBSERVED,
    sequence: int | None = None,
    flags: Sequence[QualityFlag] = (),
    tenant_id: str = TENANT,
) -> CanonicalObservation:
    ordered = tuple(sorted(set(flags)))
    return CanonicalObservation(
        event_id=observation_event_id(tenant_id, component_id, metric_key, observed_at),
        source_event_id=None,
        source=ObservationSource(
            integration_id=INTEGRATION,
            integration_type=PROVIDER,
            source_type=SourceType.HTTPS,
            connector_version=None,
            received_at=RECEIVED,
        ),
        resolved=ObservationResolution(
            tenant_id=tenant_id,
            site_id="site_" + "a" * 32,
            asset_id="ast_" + "b" * 32,
            deployment_id="dep_" + "c" * 32,
            device_id=DEVICE,
            component_id=component_id,
            probe_id=None,
        ),
        observation=ObservationMeasurement(
            metric_key=metric_key,
            value=1.0,
            value_type=MetricValueType.FLOAT,
            source_unit="C",
            canonical_unit="C",
            observed_at=observed_at,
            sequence=sequence,
        ),
        delivery=ObservationDelivery(
            ingested_at=RECEIVED + timedelta(seconds=1),
            delayed=QualityFlag.DELAYED in ordered,
            replay=QualityFlag.REPLAY in ordered,
        ),
        quality=ObservationQuality(
            state=QualityState.VALID,
            flags=ordered,
            provenance=(QualityProvenance.SCHEMA_VALIDATION,),
        ),
    )


def kit_ids(scenarios: Sequence[Callable[..., Any]]) -> list[str]:
    return [scenario.__name__ for scenario in scenarios]


# Ingest credentials --------------------------------------------------------------------------


# Named store reads that a multi-read port performs below its boundary. The authenticator and
# trusted-mapping subjects accept these as `fail_next` operations, so a fault can hit a later
# read (the account after the credential; the device or a component after the identity).
STORE_READS = (
    "get_ingest_credential",
    "get_integration_account",
    "resolve_external_identity",
    "get_device",
    "get_component",
)


class FaultInjectable(Protocol):
    """Every kit subject can make the next call of `operation` raise `exc` exactly once, at or
    below the port boundary. `operation` is a port method name, or for the authenticator and
    trusted-mapping subjects one of `STORE_READS`. Later waves use it to test their 503 paths.

    The scenarios inject one fault and assert that the very same object propagates, so an
    adapter must not retry internally (boto3 already retries) nor re-wrap the error. The queue
    scenarios assume ElasticMQ-style deterministic short polling (`wait_seconds=0`)."""

    def fail_next(self, operation: str, exc: BaseException) -> None: ...


def store_errors() -> tuple[BaseException, ...]:
    return (
        ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "busy"}},
            "GetItem",
        ),
        EndpointConnectionError(endpoint_url="http://dynamodb.invalid"),
    )


def _fault_scenario(
    name: str,
    operation: str,
    invoke: Callable[[Any], Awaitable[Any]],
    *,
    prepare: Callable[[Any], Awaitable[None]] | None = None,
    errors: Callable[[], tuple[BaseException, ...]] = store_errors,
) -> Callable[[Any], Awaitable[None]]:
    async def scenario(subject: Any) -> None:
        if prepare is not None:
            await prepare(subject)
        for error in errors():
            subject.fail_next(operation, error)
            with pytest.raises(type(error)) as exc_info:
                await invoke(subject)
            assert exc_info.value is error  # never swallowed, mapped to None, a denial or a fence
        await invoke(subject)  # one-shot: the next call reaches the store again

    scenario.__name__ = f"{name}_propagates_store_failures"
    return scenario


class CredentialKitSubject(FaultInjectable, Protocol):
    repository: Any  # IngestCredentialRepository

    async def seed_account(self, account: IntegrationAccount) -> None: ...

    def audit_items(self) -> list[dict[str, Any]]: ...


async def credential_create_get_list(subject: CredentialKitSubject) -> None:
    await subject.seed_account(integration_account())
    first = credential()

    assert await subject.repository.create_ingest_credential(first, AUDIT) == first
    assert await subject.repository.get_ingest_credential(CREDENTIAL) == first
    assert await subject.repository.get_ingest_credential(OTHER_CREDENTIAL) is None
    assert await subject.repository.list_ingest_credentials(TENANT, ACCOUNT) == [first]
    assert await subject.repository.list_ingest_credentials(OTHER_TENANT, ACCOUNT) == []
    assert await subject.repository.list_ingest_credentials(TENANT, OTHER_ACCOUNT) == []


async def credential_slot_taken_is_conflict(subject: CredentialKitSubject) -> None:
    await subject.seed_account(integration_account())
    await subject.repository.create_ingest_credential(credential(), AUDIT)

    with pytest.raises(ConflictError):
        await subject.repository.create_ingest_credential(
            credential(ingest_credential_id=OTHER_CREDENTIAL), AUDIT
        )
    assert await subject.repository.get_ingest_credential(OTHER_CREDENTIAL) is None
    assert [item["action"] for item in subject.audit_items()] == ["ingest_credential.issued"]

    second = credential(ingest_credential_id=OTHER_CREDENTIAL, slot=2)
    assert await subject.repository.create_ingest_credential(second, AUDIT) == second
    listed = await subject.repository.list_ingest_credentials(TENANT, ACCOUNT)
    assert sorted(item.ingest_credential_id for item in listed) == [CREDENTIAL, OTHER_CREDENTIAL]


async def credential_requires_an_eligible_account(subject: CredentialKitSubject) -> None:
    # No IntegrationAccount row at all.
    with pytest.raises((ConflictError, NotFoundError)):
        await subject.repository.create_ingest_credential(credential(), AUDIT)
    for status in (IntegrationAccountStatus.REVOKED, IntegrationAccountStatus.ERROR):
        await subject.seed_account(integration_account(status=status))
        with pytest.raises((ConflictError, NotFoundError)):
            await subject.repository.create_ingest_credential(credential(), AUDIT)
    await subject.seed_account(
        integration_account(integration_account_id=OTHER_ACCOUNT, provider_key="vendor_cloud")
    )
    with pytest.raises((ConflictError, NotFoundError)):
        await subject.repository.create_ingest_credential(
            credential(integration_account_id=OTHER_ACCOUNT), AUDIT
        )
    assert await subject.repository.get_ingest_credential(CREDENTIAL) is None
    assert subject.audit_items() == []


async def credential_cannot_target_another_tenants_account(
    subject: CredentialKitSubject,
) -> None:
    await subject.seed_account(integration_account())

    with pytest.raises((ConflictError, NotFoundError)):
        await subject.repository.create_ingest_credential(credential(tenant_id=OTHER_TENANT), AUDIT)

    assert await subject.repository.get_ingest_credential(CREDENTIAL) is None
    assert await subject.repository.list_ingest_credentials(OTHER_TENANT, ACCOUNT) == []
    assert await subject.repository.list_ingest_credentials(TENANT, ACCOUNT) == []
    assert subject.audit_items() == []
    # The slot stays free for the account's own tenant.
    assert await subject.repository.create_ingest_credential(credential(), AUDIT) == credential()


async def credential_pending_account_may_issue(subject: CredentialKitSubject) -> None:
    await subject.seed_account(integration_account(status=IntegrationAccountStatus.PENDING))

    assert await subject.repository.create_ingest_credential(credential(), AUDIT) == credential()


async def credential_revoke_frees_the_slot(subject: CredentialKitSubject) -> None:
    await subject.seed_account(integration_account())
    await subject.repository.create_ingest_credential(credential(), AUDIT)
    revoked_at = CREATED + timedelta(days=1)

    revoked = await subject.repository.revoke_ingest_credential(
        TENANT, ACCOUNT, CREDENTIAL, expected_version=1, revoked_at=revoked_at, audit=AUDIT
    )

    expected = credential(
        status=IngestCredentialStatus.REVOKED,
        revoked_at=revoked_at,
        updated_at=revoked_at,
        version=2,
    )
    assert revoked == expected
    assert await subject.repository.get_ingest_credential(CREDENTIAL) == expected
    replacement = credential(ingest_credential_id=OTHER_CREDENTIAL, slot=1)
    assert await subject.repository.create_ingest_credential(replacement, AUDIT) == replacement


async def credential_revoke_stale_version_is_conflict(subject: CredentialKitSubject) -> None:
    await subject.seed_account(integration_account())
    await subject.repository.create_ingest_credential(credential(), AUDIT)

    before = subject.audit_items()
    with pytest.raises(ConflictError) as exc_info:
        await subject.repository.revoke_ingest_credential(
            TENANT, ACCOUNT, CREDENTIAL, expected_version=7, revoked_at=RECEIVED, audit=AUDIT
        )

    assert not isinstance(exc_info.value, NotFoundError)
    assert await subject.repository.get_ingest_credential(CREDENTIAL) == credential()
    assert subject.audit_items() == before


async def credential_revoke_never_reveals_foreign_rows(subject: CredentialKitSubject) -> None:
    await subject.seed_account(integration_account())
    await subject.repository.create_ingest_credential(credential(), AUDIT)
    attempts = [
        (OTHER_TENANT, ACCOUNT, CREDENTIAL),
        (TENANT, OTHER_ACCOUNT, CREDENTIAL),
        (TENANT, ACCOUNT, OTHER_CREDENTIAL),
    ]

    before = subject.audit_items()
    for tenant_id, account_id, credential_id in attempts:
        with pytest.raises(NotFoundError):
            await subject.repository.revoke_ingest_credential(
                tenant_id,
                account_id,
                credential_id,
                expected_version=1,
                revoked_at=RECEIVED,
                audit=AUDIT,
            )
    assert await subject.repository.get_ingest_credential(CREDENTIAL) == credential()
    assert subject.audit_items() == before

    await subject.repository.revoke_ingest_credential(
        TENANT, ACCOUNT, CREDENTIAL, expected_version=1, revoked_at=RECEIVED, audit=AUDIT
    )
    after_revoke = subject.audit_items()
    with pytest.raises(NotFoundError):  # already revoked: the `#status = :active` condition
        await subject.repository.revoke_ingest_credential(
            TENANT, ACCOUNT, CREDENTIAL, expected_version=2, revoked_at=RECEIVED, audit=AUDIT
        )
    assert subject.audit_items() == after_revoke


async def credential_writes_one_audit_item_per_change(subject: CredentialKitSubject) -> None:
    await subject.seed_account(integration_account())
    issued = await subject.repository.create_ingest_credential(credential(), AUDIT)
    await subject.repository.revoke_ingest_credential(
        TENANT, ACCOUNT, CREDENTIAL, expected_version=1, revoked_at=RECEIVED, audit=AUDIT
    )

    items = subject.audit_items()

    assert [item["action"] for item in items] == [
        "ingest_credential.issued",
        "ingest_credential.revoked",
    ]
    assert [(item["before_status"], item["after_status"]) for item in items] == [
        (None, "active"),
        ("active", "revoked"),
    ]
    assert {item["fingerprint"] for item in items} == {issued.fingerprint}
    assert {item["actor_id"] for item in items} == {AUDIT.actor_id}
    assert {item["resource_id"] for item in items} == {CREDENTIAL}
    rendered = json.dumps(items, default=str)
    assert issued.secret_sha256 not in rendered
    assert SECRET not in rendered


CREDENTIAL_REPOSITORY_CONTRACT = (
    credential_create_get_list,
    credential_slot_taken_is_conflict,
    credential_requires_an_eligible_account,
    credential_cannot_target_another_tenants_account,
    credential_pending_account_may_issue,
    credential_revoke_frees_the_slot,
    credential_revoke_stale_version_is_conflict,
    credential_revoke_never_reveals_foreign_rows,
    credential_writes_one_audit_item_per_change,
)


# Connector authentication -------------------------------------------------------------------


class AuthenticatorKitSubject(FaultInjectable, Protocol):
    authenticator: Any  # ConnectorAuthenticator

    async def seed_account(self, account: IntegrationAccount) -> None: ...

    async def seed_credential(self, credential: IngestCredential) -> None: ...


async def _seed_authenticable(subject: AuthenticatorKitSubject, **account: Any) -> None:
    await subject.seed_account(integration_account(**account))
    await subject.seed_credential(credential())


async def authenticate_valid_token(subject: AuthenticatorKitSubject) -> None:
    await _seed_authenticable(subject)

    source = await subject.authenticator.authenticate(ingest_token(CREDENTIAL, SECRET))

    assert source == AuthenticatedSource(
        tenant_id=TENANT,
        integration_account_id=ACCOUNT,
        provider_key=PROVIDER,
        ingest_credential_id=CREDENTIAL,
    )


async def authenticate_rejects_wrong_or_malformed_tokens(subject: AuthenticatorKitSubject) -> None:
    await _seed_authenticable(subject)
    rejected = [
        ingest_token(CREDENTIAL, "B" * 43),
        ingest_token(OTHER_CREDENTIAL, SECRET),
        "lpi_" + CREDENTIAL + "_" + SECRET[:-1],
        "Bearer " + ingest_token(CREDENTIAL, SECRET),
        "",
    ]

    for token in rejected:
        assert await subject.authenticator.authenticate(token) is None, token


async def authenticate_rejects_revoked_credentials(subject: AuthenticatorKitSubject) -> None:
    await subject.seed_account(integration_account())
    await subject.seed_credential(
        credential(
            status=IngestCredentialStatus.REVOKED,
            revoked_at=RECEIVED,
            updated_at=RECEIVED,
            version=2,
        )
    )

    assert await subject.authenticator.authenticate(ingest_token(CREDENTIAL, SECRET)) is None


async def authenticate_requires_an_active_https_account(subject: AuthenticatorKitSubject) -> None:
    for account in (
        {"status": IntegrationAccountStatus.PENDING},
        {"status": IntegrationAccountStatus.ERROR},
        {"status": IntegrationAccountStatus.REVOKED},
        {"provider_key": "vendor_cloud"},
    ):
        await _seed_authenticable(subject, **account)
        assert await subject.authenticator.authenticate(ingest_token(CREDENTIAL, SECRET)) is None


async def authenticate_requires_the_account_row(subject: AuthenticatorKitSubject) -> None:
    await subject.seed_credential(credential())

    assert await subject.authenticator.authenticate(ingest_token(CREDENTIAL, SECRET)) is None


CONNECTOR_AUTHENTICATOR_CONTRACT = (
    authenticate_valid_token,
    authenticate_rejects_wrong_or_malformed_tokens,
    authenticate_rejects_revoked_credentials,
    authenticate_requires_an_active_https_account,
    authenticate_requires_the_account_row,
)


# External identity resolution ----------------------------------------------------------------


class ResolverKitSubject(FaultInjectable, Protocol):
    resolver: Any  # ExternalIdentityResolver

    async def seed_device_integration(self, integration: DeviceIntegration) -> None: ...


async def resolve_known_identity(subject: ResolverKitSubject) -> None:
    integration = device_integration()
    await subject.seed_device_integration(integration)

    assert await subject.resolver.resolve_external_identity(PROVIDER, IDENTITY_HASH) == integration
    assert await subject.resolver.resolve_external_identity(PROVIDER, "e" * 64) is None
    assert await subject.resolver.resolve_external_identity("vendor_cloud", IDENTITY_HASH) is None


async def resolve_is_account_scoped(subject: ResolverKitSubject) -> None:
    foreign_hash = account_scoped_identity_hash(PROVIDER, OTHER_ACCOUNT, EXTERNAL_DEVICE)
    foreign = device_integration(
        tenant_id=OTHER_TENANT,
        integration_account_id=OTHER_ACCOUNT,
        external_identity_hash=foreign_hash,
    )
    await subject.seed_device_integration(foreign)

    assert await subject.resolver.resolve_external_identity(PROVIDER, foreign_hash) == foreign
    assert await subject.resolver.resolve_external_identity(PROVIDER, IDENTITY_HASH) is None
    global_hash = external_identity_hash(PROVIDER, EXTERNAL_DEVICE)
    assert await subject.resolver.resolve_external_identity(PROVIDER, global_hash) is None


async def resolve_returns_decommissioned_integrations(subject: ResolverKitSubject) -> None:
    # The identity stays claimed (D22); the fence needs the row to say why it refuses.
    retired = device_integration(status=DeviceIntegrationStatus.DECOMMISSIONED)
    await subject.seed_device_integration(retired)

    assert await subject.resolver.resolve_external_identity(PROVIDER, IDENTITY_HASH) == retired


EXTERNAL_IDENTITY_RESOLVER_CONTRACT = (
    resolve_known_identity,
    resolve_is_account_scoped,
    resolve_returns_decommissioned_integrations,
)


# Integration lifecycle -----------------------------------------------------------------------


class LifecycleKitSubject(FaultInjectable, Protocol):
    repository: Any  # IntegrationLifecycleRepository

    async def seed_account(self, account: IntegrationAccount) -> None: ...

    async def seed_device_integration(self, integration: DeviceIntegration) -> None: ...

    def audit_items(self) -> list[dict[str, Any]]: ...


async def lifecycle_account_transition(subject: LifecycleKitSubject) -> None:
    await subject.seed_account(integration_account(status=IntegrationAccountStatus.PENDING))

    updated = await subject.repository.transition_integration_account(
        TENANT,
        ACCOUNT,
        IntegrationAccountStatus.ACTIVE,
        expected_version=1,
        updated_at=RECEIVED,
        audit=AUDIT,
    )

    assert updated == integration_account(version=2, updated_at=RECEIVED)
    (item,) = subject.audit_items()
    assert item["action"] == "integration_account.status_changed"
    assert (item["before_status"], item["after_status"]) == ("pending", "active")
    assert item["resource_id"] == ACCOUNT


async def lifecycle_account_errors(subject: LifecycleKitSubject) -> None:
    await subject.seed_account(integration_account(status=IntegrationAccountStatus.REVOKED))

    with pytest.raises(ConflictError):  # revoked is terminal
        await subject.repository.transition_integration_account(
            TENANT,
            ACCOUNT,
            IntegrationAccountStatus.ACTIVE,
            expected_version=1,
            updated_at=RECEIVED,
            audit=AUDIT,
        )
    await subject.seed_account(
        integration_account(
            integration_account_id=OTHER_ACCOUNT, status=IntegrationAccountStatus.ACTIVE
        )
    )
    with pytest.raises(ConflictError) as stale:  # stale version on a legal transition
        await subject.repository.transition_integration_account(
            TENANT,
            OTHER_ACCOUNT,
            IntegrationAccountStatus.ERROR,
            expected_version=9,
            updated_at=RECEIVED,
            audit=AUDIT,
        )
    assert not isinstance(stale.value, NotFoundError)
    for tenant_id, account_id in ((OTHER_TENANT, ACCOUNT), (TENANT, "ia_" + "0" * 32)):
        with pytest.raises(NotFoundError):
            await subject.repository.transition_integration_account(
                tenant_id,
                account_id,
                IntegrationAccountStatus.ACTIVE,
                expected_version=1,
                updated_at=RECEIVED,
                audit=AUDIT,
            )
    assert subject.audit_items() == []


async def lifecycle_device_integration_transition(subject: LifecycleKitSubject) -> None:
    await subject.seed_device_integration(device_integration())

    updated = await subject.repository.transition_device_integration(
        TENANT,
        INTEGRATION,
        DeviceIntegrationStatus.DECOMMISSIONED,
        expected_version=1,
        updated_at=RECEIVED,
        audit=AUDIT,
    )

    assert updated == device_integration(
        status=DeviceIntegrationStatus.DECOMMISSIONED, version=2, updated_at=RECEIVED
    )
    (item,) = subject.audit_items()
    assert item["action"] == "device_integration.status_changed"
    assert (item["before_status"], item["after_status"]) == ("active", "decommissioned")
    with pytest.raises(ConflictError):  # decommissioned is terminal
        await subject.repository.transition_device_integration(
            TENANT,
            INTEGRATION,
            DeviceIntegrationStatus.ACTIVE,
            expected_version=2,
            updated_at=RECEIVED,
            audit=AUDIT,
        )
    with pytest.raises(NotFoundError):
        await subject.repository.transition_device_integration(
            OTHER_TENANT,
            INTEGRATION,
            DeviceIntegrationStatus.ERROR,
            expected_version=2,
            updated_at=RECEIVED,
            audit=AUDIT,
        )


INTEGRATION_LIFECYCLE_CONTRACT = (
    lifecycle_account_transition,
    lifecycle_account_errors,
    lifecycle_device_integration_transition,
)


# Consistent device reads ---------------------------------------------------------------------


class DeviceReaderKitSubject(FaultInjectable, Protocol):
    reader: Any  # ConsistentDeviceReader

    async def seed_device(self, device: DeviceV2) -> None: ...


async def consistent_device_read(subject: DeviceReaderKitSubject) -> None:
    await subject.seed_device(device())

    assert await subject.reader.get_device(TENANT, DEVICE, consistent=True) == device()
    assert await subject.reader.get_device(TENANT, DEVICE, consistent=False) == device()
    assert await subject.reader.get_device(OTHER_TENANT, DEVICE, consistent=True) is None
    assert await subject.reader.get_device(TENANT, OTHER_DEVICE, consistent=True) is None


CONSISTENT_DEVICE_READER_CONTRACT = (consistent_device_read,)


# Trusted mapping -----------------------------------------------------------------------------


class TrustedMappingKitSubject(FaultInjectable, Protocol):
    mapping: Any  # TrustedMapping

    async def seed_account(self, account: IntegrationAccount) -> None: ...

    async def seed_device_integration(self, integration: DeviceIntegration) -> None: ...

    async def seed_device(self, device: DeviceV2) -> None: ...

    async def seed_component(self, component: DeviceComponent) -> None: ...


SOURCE = AuthenticatedSource(
    tenant_id=TENANT,
    integration_account_id=ACCOUNT,
    provider_key=PROVIDER,
    ingest_credential_id=CREDENTIAL,
)


def batch(*component_ids: str | None) -> IngestObservationBatch:
    observations = []
    for index, component_id in enumerate(component_ids or (COMPONENT,)):
        item: dict[str, Any] = {
            "metric_key": "temperature",
            "value": 27.5 + index,
            "source_unit": "C",
            "observed_at": OBSERVED.isoformat(),
        }
        if component_id is not None:
            item["component_id"] = component_id
        observations.append(item)
    return IngestObservationBatch.model_validate(
        {"external_device_id": EXTERNAL_DEVICE, "observations": observations}
    )


async def _seed_mapping(
    subject: TrustedMappingKitSubject,
    *,
    account: Mapping[str, Any] | None = None,
    integration: Mapping[str, Any] | None = None,
    device_state: Mapping[str, Any] | None = None,
    with_device: bool = True,
    with_account: bool = True,
    components: Sequence[DeviceComponent] = (),
) -> None:
    if with_account:
        await subject.seed_account(integration_account(**(account or {})))
    await subject.seed_device_integration(device_integration(**(integration or {})))
    if with_device:
        await subject.seed_device(device(**(device_state or {})))
    for item in components:
        await subject.seed_component(item)


async def admit_fills_the_default_component(subject: TrustedMappingKitSubject) -> None:
    await _seed_mapping(subject, components=(component(), component(DEFAULT_COMPONENT)))
    request = batch(COMPONENT, None)

    admission = await subject.mapping.admit(SOURCE, request)

    assert isinstance(admission, IngressAdmission)
    assert admission.source == SOURCE
    assert admission.device_integration == device_integration()
    assert admission.device == device()
    assert [item.component_id for item in admission.observations] == [COMPONENT, DEFAULT_COMPONENT]
    assert [item.value for item in admission.observations] == [27.5, 28.5]
    assert all(isinstance(item, SourceObservation) for item in admission.observations)


ADMIT_DENIAL_CASES: tuple[tuple[str, dict[str, Any], IngressDenial], ...] = (
    (
        "unknown identity",
        {"integration": {"external_identity_hash": "e" * 64}},
        IngressDenial(reason=IngressAdmissionDenied.IDENTITY_UNRESOLVED, observation_index=None),
    ),
    (
        "foreign account identity",
        {
            "integration": {
                "integration_account_id": OTHER_ACCOUNT,
                "external_identity_hash": account_scoped_identity_hash(
                    PROVIDER, OTHER_ACCOUNT, EXTERNAL_DEVICE
                ),
            }
        },
        IngressDenial(reason=IngressAdmissionDenied.IDENTITY_UNRESOLVED, observation_index=None),
    ),
    (
        "integration decommissioned",
        {"integration": {"status": DeviceIntegrationStatus.DECOMMISSIONED}},
        IngressDenial(reason=IngressAdmissionDenied.INTEGRATION_INACTIVE, observation_index=None),
    ),
    (
        "integration provisioning",
        {"integration": {"status": DeviceIntegrationStatus.PROVISIONING}},
        IngressDenial(reason=IngressAdmissionDenied.INTEGRATION_INACTIVE, observation_index=None),
    ),
    (
        "account revoked",
        {"account": {"status": IntegrationAccountStatus.REVOKED}},
        IngressDenial(reason=IngressAdmissionDenied.ACCOUNT_INACTIVE, observation_index=None),
    ),
    (
        "device suspended",
        {"device_state": {"status": DeviceStatus.SUSPENDED}},
        IngressDenial(reason=IngressAdmissionDenied.DEVICE_INACTIVE, observation_index=None),
    ),
    (
        "device missing",
        {"with_device": False},
        IngressDenial(reason=IngressAdmissionDenied.DEVICE_INACTIVE, observation_index=None),
    ),
    (
        "integration error",
        {"integration": {"status": DeviceIntegrationStatus.ERROR}},
        IngressDenial(reason=IngressAdmissionDenied.INTEGRATION_INACTIVE, observation_index=None),
    ),
    (
        "account pending",
        {"account": {"status": IntegrationAccountStatus.PENDING}},
        IngressDenial(reason=IngressAdmissionDenied.ACCOUNT_INACTIVE, observation_index=None),
    ),
    (
        "account error",
        {"account": {"status": IntegrationAccountStatus.ERROR}},
        IngressDenial(reason=IngressAdmissionDenied.ACCOUNT_INACTIVE, observation_index=None),
    ),
    (
        "account not https",
        {"account": {"provider_key": "vendor_cloud"}},
        IngressDenial(reason=IngressAdmissionDenied.ACCOUNT_INACTIVE, observation_index=None),
    ),
    (
        "device provisioning",
        {"device_state": {"status": DeviceStatus.PROVISIONING}},
        IngressDenial(reason=IngressAdmissionDenied.DEVICE_INACTIVE, observation_index=None),
    ),
    (
        "device decommissioned",
        {"device_state": {"status": DeviceStatus.DECOMMISSIONED}},
        IngressDenial(reason=IngressAdmissionDenied.DEVICE_INACTIVE, observation_index=None),
    ),
    (
        "device of another tenant",
        {"device_state": {"tenant_id": OTHER_TENANT}},
        IngressDenial(reason=IngressAdmissionDenied.DEVICE_INACTIVE, observation_index=None),
    ),
    (
        "integration of another tenant",
        {"integration": {"tenant_id": OTHER_TENANT}},
        IngressDenial(reason=IngressAdmissionDenied.INTEGRATION_MISMATCH, observation_index=None),
    ),
    (
        "integration of another account",
        {"integration": {"integration_account_id": OTHER_ACCOUNT}},
        IngressDenial(reason=IngressAdmissionDenied.INTEGRATION_MISMATCH, observation_index=None),
    ),
    (
        "first failure wins",
        {
            "integration": {"status": DeviceIntegrationStatus.DECOMMISSIONED},
            "device_state": {"status": DeviceStatus.SUSPENDED},
        },
        IngressDenial(reason=IngressAdmissionDenied.INTEGRATION_INACTIVE, observation_index=None),
    ),
)


def _admit_denial_scenario(
    name: str, seed: dict[str, Any], expected: IngressDenial
) -> Callable[[TrustedMappingKitSubject], Awaitable[None]]:
    async def scenario(subject: TrustedMappingKitSubject) -> None:
        await _seed_mapping(subject, components=(component(),), **seed)
        assert await subject.mapping.admit(SOURCE, batch(COMPONENT)) == expected

    scenario.__name__ = "admit_denied_" + name.replace(" ", "_")
    return scenario


async def admit_rejects_foreign_or_inactive_components(
    subject: TrustedMappingKitSubject,
) -> None:
    await _seed_mapping(
        subject,
        components=(
            component(),
            component(OTHER_COMPONENT, device_id=OTHER_DEVICE),
            component(DEFAULT_COMPONENT, status=ComponentStatus.DECOMMISSIONED),
        ),
    )

    assert await subject.mapping.admit(SOURCE, batch(COMPONENT, OTHER_COMPONENT)) == IngressDenial(
        reason=IngressAdmissionDenied.COMPONENT_INVALID, observation_index=1
    )
    assert await subject.mapping.admit(SOURCE, batch(COMPONENT, DEFAULT_COMPONENT)) == (
        IngressDenial(reason=IngressAdmissionDenied.COMPONENT_INVALID, observation_index=1)
    )
    # Omitted, and the legacy default is not an active Component of the Device.
    assert await subject.mapping.admit(SOURCE, batch(COMPONENT, COMPONENT, None)) == (
        IngressDenial(reason=IngressAdmissionDenied.COMPONENT_REQUIRED, observation_index=2)
    )


async def fence_accepts_a_trusted_envelope(subject: TrustedMappingKitSubject) -> None:
    await _seed_mapping(subject, components=(component(),))
    message = envelope()

    trusted = await subject.mapping.fence(message)

    assert isinstance(trusted, TrustedIngress)
    assert trusted.context == build_trusted_context(message, device_integration())
    assert tuple(trusted.observations) == tuple(message.observations)


FENCE_CASES: tuple[tuple[str, dict[str, Any], dict[str, Any], object], ...] = (
    (
        "unresolved identity",
        {},
        {"external_identity_hash": "e" * 64},
        IngressFence(reason=IngressFenceReason.IDENTITY_UNRESOLVED, account_verified=True),
    ),
    (
        "forged integration id",
        {},
        {"device_integration_id": OTHER_INTEGRATION},
        IngressFence(reason=IngressFenceReason.INTEGRATION_MISMATCH, account_verified=True),
    ),
    (
        "forged device id",
        {},
        {"device_id": OTHER_DEVICE},
        IngressFence(reason=IngressFenceReason.INTEGRATION_MISMATCH, account_verified=True),
    ),
    (
        "integration decommissioned",
        {"integration": {"status": DeviceIntegrationStatus.DECOMMISSIONED}},
        {},
        IngressFence(reason=IngressFenceReason.INTEGRATION_DECOMMISSIONED, account_verified=True),
    ),
    (
        "integration error",
        {"integration": {"status": DeviceIntegrationStatus.ERROR}},
        {},
        IngressDefer(reason=IngressDeferReason.INTEGRATION_NOT_ACTIVE),
    ),
    (
        "account revoked",
        {"account": {"status": IntegrationAccountStatus.REVOKED}},
        {},
        IngressFence(reason=IngressFenceReason.ACCOUNT_REVOKED, account_verified=True),
    ),
    (
        "account pending",
        {"account": {"status": IntegrationAccountStatus.PENDING}},
        {},
        IngressDefer(reason=IngressDeferReason.ACCOUNT_NOT_ACTIVE),
    ),
    (
        "device decommissioned",
        {"device_state": {"status": DeviceStatus.DECOMMISSIONED}},
        {},
        IngressFence(reason=IngressFenceReason.DEVICE_DECOMMISSIONED, account_verified=True),
    ),
    (
        "device provisioning",
        {"device_state": {"status": DeviceStatus.PROVISIONING}},
        {},
        IngressDefer(reason=IngressDeferReason.DEVICE_NOT_ACTIVE),
    ),
    (
        "forged tenant without an account row",
        {},
        {"tenant_id": OTHER_TENANT},
        IngressFence(reason=IngressFenceReason.INTEGRATION_MISMATCH, account_verified=False),
    ),
    (
        "forged account id",
        {},
        {"integration_account_id": OTHER_ACCOUNT},
        IngressFence(reason=IngressFenceReason.INTEGRATION_MISMATCH, account_verified=False),
    ),
    (
        "forged provider key",
        {},
        {"provider_key": "https_other"},
        IngressFence(reason=IngressFenceReason.IDENTITY_UNRESOLVED, account_verified=True),
    ),
    (
        "integration decommissioned without an account row",
        {"with_account": False, "integration": {"status": DeviceIntegrationStatus.DECOMMISSIONED}},
        {},
        IngressFence(reason=IngressFenceReason.INTEGRATION_DECOMMISSIONED, account_verified=False),
    ),
    (
        "account missing",
        {"with_account": False},
        {},
        IngressFence(reason=IngressFenceReason.IDENTITY_UNRESOLVED, account_verified=False),
    ),
    (
        "account not https",
        {"account": {"provider_key": "vendor_cloud"}},
        {},
        IngressFence(reason=IngressFenceReason.ACCOUNT_REVOKED, account_verified=True),
    ),
    (
        "account error",
        {"account": {"status": IntegrationAccountStatus.ERROR}},
        {},
        IngressDefer(reason=IngressDeferReason.ACCOUNT_NOT_ACTIVE),
    ),
    (
        "integration provisioning",
        {"integration": {"status": DeviceIntegrationStatus.PROVISIONING}},
        {},
        IngressDefer(reason=IngressDeferReason.INTEGRATION_NOT_ACTIVE),
    ),
    (
        "device missing",
        {"with_device": False},
        {},
        IngressFence(reason=IngressFenceReason.DEVICE_DECOMMISSIONED, account_verified=True),
    ),
    (
        "device suspended",
        {"device_state": {"status": DeviceStatus.SUSPENDED}},
        {},
        IngressDefer(reason=IngressDeferReason.DEVICE_NOT_ACTIVE),
    ),
)


def _fence_scenario(
    name: str, seed: dict[str, Any], forged: dict[str, Any], expected: object
) -> Callable[[TrustedMappingKitSubject], Awaitable[None]]:
    async def scenario(subject: TrustedMappingKitSubject) -> None:
        await _seed_mapping(subject, components=(component(),), **seed)
        assert await subject.mapping.fence(envelope(**forged)) == expected

    scenario.__name__ = "fence_" + name.replace(" ", "_")
    return scenario


TRUSTED_MAPPING_CONTRACT = (
    admit_fills_the_default_component,
    *(_admit_denial_scenario(*case) for case in ADMIT_DENIAL_CASES),
    admit_rejects_foreign_or_inactive_components,
    fence_accepts_a_trusted_envelope,
    *(_fence_scenario(*case) for case in FENCE_CASES),
)


# Ingress receipts ----------------------------------------------------------------------------


class ReceiptKitSubject(FaultInjectable, Protocol):
    repository: Any  # IngressReceiptRepository


async def receipt_claim_fresh(subject: ReceiptKitSubject) -> None:
    first = receipt()

    assert await subject.repository.get_ingress_receipt(ACCOUNT, KEY_SHA) is None
    assert await subject.repository.claim_ingress_receipt(first, now=RECEIVED) == first
    assert await subject.repository.get_ingress_receipt(ACCOUNT, KEY_SHA) == first
    assert await subject.repository.get_ingress_receipt(OTHER_ACCOUNT, KEY_SHA) is None


async def receipt_claim_race_returns_the_winner(subject: ReceiptKitSubject) -> None:
    first = receipt()
    loser = receipt(ingress_id="ing_" + "5" * 32, created_at=RECEIVED + timedelta(seconds=1))
    await subject.repository.claim_ingress_receipt(first, now=RECEIVED)

    winner = await subject.repository.claim_ingress_receipt(loser, now=loser.created_at)

    assert winner == first
    assert await subject.repository.get_ingress_receipt(ACCOUNT, KEY_SHA) == first


async def receipt_claim_after_logical_expiry(subject: ReceiptKitSubject) -> None:
    first = receipt()
    await subject.repository.claim_ingress_receipt(first, now=RECEIVED)
    almost = RECEIVED + timedelta(hours=24) - timedelta(microseconds=1)
    later = receipt(ingress_id="ing_" + "5" * 32, created_at=almost)

    assert await subject.repository.claim_ingress_receipt(later, now=almost) == first

    expired_at = RECEIVED + timedelta(hours=24)
    replacement = receipt(ingress_id="ing_" + "6" * 32, created_at=expired_at)
    assert await subject.repository.claim_ingress_receipt(replacement, now=expired_at) == (
        replacement
    )
    assert await subject.repository.get_ingress_receipt(ACCOUNT, KEY_SHA) == replacement


async def receipt_claim_cutoff_comes_from_now(subject: ReceiptKitSubject) -> None:
    first = receipt()
    await subject.repository.claim_ingress_receipt(first, now=RECEIVED)
    # A receipt built long after the first, but claimed while the first is still live.
    early_now = RECEIVED + timedelta(hours=1)
    future = receipt(ingress_id="ing_" + "5" * 32, created_at=RECEIVED + timedelta(hours=25))

    assert await subject.repository.claim_ingress_receipt(future, now=early_now) == first

    # A receipt built early, but claimed once the first has expired by `now`.
    late_now = RECEIVED + timedelta(hours=24)
    stale = receipt(ingress_id="ing_" + "6" * 32, created_at=RECEIVED + timedelta(seconds=1))
    assert await subject.repository.claim_ingress_receipt(stale, now=late_now) == stale
    assert await subject.repository.get_ingress_receipt(ACCOUNT, KEY_SHA) == stale


async def receipt_mark_enqueued_is_conditioned(subject: ReceiptKitSubject) -> None:
    first = receipt()
    await subject.repository.claim_ingress_receipt(first, now=RECEIVED)

    await subject.repository.mark_ingress_receipt_enqueued(
        ACCOUNT, KEY_SHA, "ing_" + "5" * 32, at=RECEIVED
    )
    assert await subject.repository.get_ingress_receipt(ACCOUNT, KEY_SHA) == first

    await subject.repository.mark_ingress_receipt_enqueued(ACCOUNT, KEY_SHA, INGRESS, at=RECEIVED)
    enqueued = await subject.repository.get_ingress_receipt(ACCOUNT, KEY_SHA)
    assert enqueued == first.model_copy(update={"state": IngressReceiptState.ENQUEUED})

    await subject.repository.mark_ingress_receipt_enqueued(ACCOUNT, KEY_SHA, INGRESS, at=RECEIVED)
    await subject.repository.mark_ingress_receipt_enqueued(ACCOUNT, "f" * 64, INGRESS, at=RECEIVED)
    assert await subject.repository.get_ingress_receipt(ACCOUNT, KEY_SHA) == enqueued
    assert await subject.repository.get_ingress_receipt(ACCOUNT, "f" * 64) is None


INGRESS_RECEIPT_CONTRACT = (
    receipt_claim_fresh,
    receipt_claim_race_returns_the_winner,
    receipt_claim_after_logical_expiry,
    receipt_claim_cutoff_comes_from_now,
    receipt_mark_enqueued_is_conditioned,
    _fault_scenario(
        "receipt_get",
        "get_ingress_receipt",
        lambda s: s.repository.get_ingress_receipt(ACCOUNT, KEY_SHA),
    ),
    _fault_scenario(
        "receipt_claim",
        "claim_ingress_receipt",
        lambda s: s.repository.claim_ingress_receipt(receipt(), now=RECEIVED),
    ),
    _fault_scenario(
        "receipt_mark",
        "mark_ingress_receipt_enqueued",
        lambda s: s.repository.mark_ingress_receipt_enqueued(
            ACCOUNT, KEY_SHA, INGRESS, at=RECEIVED
        ),
    ),
)


# Ingress queue -------------------------------------------------------------------------------


class QueueKitSubject(FaultInjectable, Protocol):
    producer: Any  # IngressQueueProducer
    consumer: Any  # IngressQueueConsumer


async def _receive(subject: QueueKitSubject) -> list[Any]:
    return list(
        await subject.consumer.receive(max_messages=10, wait_seconds=0, visibility_seconds=30)
    )


def oversized_envelope() -> IngressEnvelope:
    observations = [
        source_observation(
            metric_key="m" * 64,
            source_unit="u" * 16,
            source_event_id="e" * 128,
            sequence=index,
            source_metadata={"firmware_version": EMOJI * 64},
        )
        for index in range(1_000)
    ]
    return envelope(observations=observations)


async def queue_send_and_receive(subject: QueueKitSubject) -> None:
    message = envelope()

    message_id = await subject.producer.send(message)
    (received,) = await _receive(subject)

    assert received.message_id == message_id
    assert received.body == message.to_json()
    assert IngressEnvelope.model_validate_json(received.body) == message
    assert received.receive_count == 1
    assert received.sent_at.tzinfo is not None
    assert received.receipt_handle


async def queue_visibility_and_delete(subject: QueueKitSubject) -> None:
    await subject.producer.send(envelope())
    (first,) = await _receive(subject)

    assert await _receive(subject) == []
    await subject.consumer.change_visibility(first, 0)
    (second,) = await _receive(subject)
    assert second.message_id == first.message_id
    assert second.receive_count == 2
    await subject.consumer.delete(second)
    assert await _receive(subject) == []


def envelope_of_message_bytes(target: int) -> IngressEnvelope:
    """An envelope whose UTF-8 body bytes + the 31 attribute bytes equal `target` exactly."""

    def build(event_lengths: Sequence[int]) -> IngressEnvelope:
        return envelope(
            observations=[
                source_observation(
                    metric_key="m" * 64, source_unit="u" * 16, source_event_id="e" * length
                )
                for length in event_lengths
            ]
        )

    # Observations start with a 40-character source_event_id and may grow it to 128, so the
    # target is reachable well within MAX_INGEST_OBSERVATIONS.
    one = ingress_message_bytes(build([40]))
    per_observation = ingress_message_bytes(build([40, 40])) - one
    count = (target - one) // per_observation + 1
    assert count <= 1_000
    lengths = [40] * count
    missing = target - ingress_message_bytes(build(lengths))
    for index in range(count):
        grow = min(missing, 88)
        lengths[index] += grow
        missing -= grow
    assert missing == 0
    exact = build(lengths)
    assert ingress_message_bytes(exact) == target
    return exact


def multibyte_envelope() -> IngressEnvelope:
    """Fewer than 262144 characters but more than 262144 UTF-8 bytes."""
    message = envelope(
        observations=[
            source_observation(sequence=index, source_metadata={"firmware_version": EMOJI * 64})
            for index in range(800)
        ]
    )
    text = message.to_json()
    assert len(text) + 31 <= MAX_INGRESS_MESSAGE_BYTES < len(text.encode("utf-8")) + 31
    return message


async def queue_rejects_oversized_envelopes(subject: QueueKitSubject) -> None:
    for message in (
        oversized_envelope(),
        multibyte_envelope(),
        envelope_of_message_bytes(MAX_INGRESS_MESSAGE_BYTES + 1),
    ):
        with pytest.raises(IngressMessageTooLarge):
            await subject.producer.send(message)

    assert await _receive(subject) == []


async def queue_accepts_an_envelope_at_the_exact_limit(subject: QueueKitSubject) -> None:
    message = envelope_of_message_bytes(MAX_INGRESS_MESSAGE_BYTES)

    await subject.producer.send(message)
    (received,) = await _receive(subject)

    assert received.body == message.to_json()


async def queue_receive_honours_max_messages(subject: QueueKitSubject) -> None:
    for index in range(3):
        await subject.producer.send(envelope(ingress_id=f"ing_{index:032x}"))

    first = await subject.consumer.receive(max_messages=2, wait_seconds=0, visibility_seconds=30)
    rest = await _receive(subject)

    assert len(first) == 2
    assert len(rest) == 1
    assert len({item.message_id for item in [*first, *rest]}) == 3


async def queue_send_raw_keeps_the_body(subject: QueueKitSubject) -> None:
    body = '{"schema":"unknown@9","note":"redriven as-is"}'

    message_id = await subject.producer.send_raw(body)
    (received,) = await _receive(subject)

    assert (received.message_id, received.body) == (message_id, body)


INGRESS_QUEUE_CONTRACT = (
    queue_send_and_receive,
    queue_visibility_and_delete,
    queue_rejects_oversized_envelopes,
    queue_accepts_an_envelope_at_the_exact_limit,
    queue_receive_honours_max_messages,
    queue_send_raw_keeps_the_body,
)


# Quarantine ----------------------------------------------------------------------------------


class QuarantineKitSubject(FaultInjectable, Protocol):
    repository: Any  # QuarantineRepository

    def set_now(self, now: datetime) -> None: ...


async def _all_pages(
    subject: QuarantineKitSubject, tenant_id: str, limit: int, **filters: Any
) -> list[QuarantineEntry]:
    collected: list[QuarantineEntry] = []
    after: str | None = None
    for _ in range(100):
        page = await subject.repository.list_quarantine_entries(
            tenant_id, after_sk=after, limit=limit, **filters
        )
        assert len(page.entries) <= limit
        collected.extend(page.entries)
        if page.next_after_sk is None:
            return collected
        # A filtered page may be empty yet carry a cursor (Query with Limit plus a filter).
        # The cursor may be the last EVALUATED sort key (Query Limit before the filter).
        if page.entries:
            assert page.next_after_sk >= page.entries[-1].entry_key
        assert after is None or page.next_after_sk > after
        after = page.next_after_sk
    raise AssertionError("pagination did not terminate")


async def quarantine_put_get_list(subject: QuarantineKitSubject) -> None:
    subject.set_now(RECEIVED)
    entries = fenced_entries()

    await subject.repository.put_quarantine_entries(entries)

    expected = sorted(entries, key=lambda item: item.entry_key)
    page = await subject.repository.list_quarantine_entries(TENANT, after_sk=None, limit=10)
    assert list(page.entries) == expected
    assert page.next_after_sk is None
    for entry in entries:
        assert await subject.repository.get_quarantine_entry(TENANT, entry.entry_key) == entry
        assert await subject.repository.get_quarantine_entry(OTHER_TENANT, entry.entry_key) is None
    assert await _all_pages(subject, OTHER_TENANT, 10) == []


async def quarantine_paging_by_sk_lower_bound(subject: QuarantineKitSubject) -> None:
    subject.set_now(RECEIVED)
    entries = fenced_entries(5)
    await subject.repository.put_quarantine_entries(entries)

    assert await _all_pages(subject, TENANT, 2) == sorted(entries, key=lambda item: item.entry_key)


async def quarantine_filters(subject: QuarantineKitSubject) -> None:
    subject.set_now(RECEIVED)
    entries = fenced_entries()
    await subject.repository.put_quarantine_entries(entries)
    first = sorted(entries, key=lambda item: item.entry_key)[0]
    await subject.repository.transition_quarantine_entry(
        TENANT, first.entry_key, QuarantineEntryStatus.DISMISSED, expected_version=1, at=RECEIVED
    )

    open_entries = await _all_pages(subject, TENANT, 2, status=QuarantineEntryStatus.OPEN)
    dismissed = await _all_pages(subject, TENANT, 2, status=QuarantineEntryStatus.DISMISSED)
    fenced = await _all_pages(subject, TENANT, 2, reason="fenced_integration_decommissioned")

    assert {item.entry_key for item in open_entries} == {item.entry_key for item in entries} - {
        first.entry_key
    }
    assert [item.entry_key for item in dismissed] == [first.entry_key]
    assert len(fenced) == len(entries)
    assert await _all_pages(subject, TENANT, 2, reason="metric_unknown") == []


async def quarantine_put_is_an_idempotent_create(subject: QuarantineKitSubject) -> None:
    subject.set_now(RECEIVED)
    entries = fenced_entries()
    await subject.repository.put_quarantine_entries(entries)
    target = entries[0]
    replayed_at = RECEIVED + timedelta(minutes=5)

    replayed = await subject.repository.transition_quarantine_entry(
        TENANT,
        target.entry_key,
        QuarantineEntryStatus.REPLAYED,
        expected_version=1,
        at=replayed_at,
        replayed_ingress_id="ing_" + "5" * 32,
    )
    await subject.repository.put_quarantine_entries(entries)

    assert replayed == target.model_copy(
        update={
            "status": QuarantineEntryStatus.REPLAYED,
            "replayed_ingress_id": "ing_" + "5" * 32,
            "updated_at": replayed_at,
            "version": 2,
        }
    )
    assert await subject.repository.get_quarantine_entry(TENANT, target.entry_key) == replayed


async def quarantine_transition_errors(subject: QuarantineKitSubject) -> None:
    subject.set_now(RECEIVED)
    entries = fenced_entries()
    await subject.repository.put_quarantine_entries(entries)
    key = entries[0].entry_key

    with pytest.raises(ConflictError):
        await subject.repository.transition_quarantine_entry(
            TENANT, key, QuarantineEntryStatus.DISMISSED, expected_version=4, at=RECEIVED
        )
    with pytest.raises(NotFoundError):
        await subject.repository.transition_quarantine_entry(
            OTHER_TENANT, key, QuarantineEntryStatus.DISMISSED, expected_version=1, at=RECEIVED
        )
    with pytest.raises(NotFoundError):
        await subject.repository.transition_quarantine_entry(
            TENANT,
            key[:-1] + ("0" if key[-1] != "0" else "1"),
            QuarantineEntryStatus.DISMISSED,
            expected_version=1,
            at=RECEIVED,
        )
    dismissed = await subject.repository.transition_quarantine_entry(
        TENANT, key, QuarantineEntryStatus.DISMISSED, expected_version=1, at=RECEIVED
    )
    assert (dismissed.status, dismissed.version, dismissed.replayed_ingress_id) == (
        QuarantineEntryStatus.DISMISSED,
        2,
        None,
    )
    with pytest.raises(ConflictError):  # terminal
        await subject.repository.transition_quarantine_entry(
            TENANT,
            key,
            QuarantineEntryStatus.REPLAYED,
            expected_version=2,
            at=RECEIVED,
            replayed_ingress_id="ing_" + "5" * 32,
        )


async def quarantine_logical_expiry(subject: QuarantineKitSubject) -> None:
    entries = fenced_entries()
    subject.set_now(RECEIVED)
    await subject.repository.put_quarantine_entries(entries)
    key = entries[0].entry_key

    subject.set_now(RECEIVED + QUARANTINE_TTL - timedelta(microseconds=1))
    assert await subject.repository.get_quarantine_entry(TENANT, key) == entries[0]
    assert len(await _all_pages(subject, TENANT, 10)) == len(entries)

    subject.set_now(RECEIVED + QUARANTINE_TTL)
    assert await subject.repository.get_quarantine_entry(TENANT, key) is None
    assert await _all_pages(subject, TENANT, 10) == []


QUARANTINE_REPOSITORY_CONTRACT = (
    quarantine_put_get_list,
    quarantine_paging_by_sk_lower_bound,
    quarantine_filters,
    quarantine_put_is_an_idempotent_create,
    quarantine_transition_errors,
    quarantine_logical_expiry,
)


# Ingest state --------------------------------------------------------------------------------


class IngestStateKitSubject(FaultInjectable, Protocol):
    repository: Any  # IngestStateRepository

    # The port has no device-state read (P6 owns the views); the subject exposes the raw row.
    async def device_state(self, device_id: str) -> DeviceIngestState | None: ...


async def state_apply_then_read(subject: IngestStateKitSubject) -> None:
    applied = marks(quarantined_at=RECEIVED, quarantine_reason="metric_unknown")

    await subject.repository.apply_ingest_marks(applied)

    states = await subject.repository.get_component_states(TENANT, [COMPONENT, OTHER_COMPONENT])
    assert states == {
        COMPONENT: ComponentIngestState(
            tenant_id=TENANT,
            component_id=COMPONENT,
            device_id=DEVICE,
            last_received_at=RECEIVED,
            last_ingested_at=RECEIVED + timedelta(seconds=1),
            last_observed_at=OBSERVED,
            last_observed_sequence=10,
        )
    }
    assert await subject.repository.get_connector_health(TENANT, ACCOUNT) == ConnectorHealth(
        tenant_id=TENANT,
        integration_account_id=ACCOUNT,
        last_received_at=RECEIVED,
        last_accepted_at=RECEIVED,
        last_quarantined_at=RECEIVED,
        last_quarantine_reason="metric_unknown",
        last_fenced_at=None,
        last_fence_reason=None,
    )
    assert await subject.device_state(DEVICE) == DeviceIngestState(
        tenant_id=TENANT,
        device_id=DEVICE,
        last_received_at=RECEIVED,
        last_ingested_at=RECEIVED + timedelta(seconds=1),
    )


async def state_rows_are_tenant_checked(subject: IngestStateKitSubject) -> None:
    await subject.repository.apply_ingest_marks(marks())

    assert await subject.repository.get_component_states(OTHER_TENANT, [COMPONENT]) == {}
    assert await subject.repository.get_connector_health(OTHER_TENANT, ACCOUNT) is None
    assert await subject.repository.get_connector_health(TENANT, OTHER_ACCOUNT) is None


async def state_marks_are_monotone(subject: IngestStateKitSubject) -> None:
    newer = marks(
        received_at=RECEIVED + timedelta(minutes=1),
        observed_at=OBSERVED + timedelta(minutes=1),
        sequence=11,
    )
    older = marks(observed_at=OBSERVED, sequence=99)

    await subject.repository.apply_ingest_marks(newer)
    await subject.repository.apply_ingest_marks(older)  # older after newer: must not regress

    state = (await subject.repository.get_component_states(TENANT, [COMPONENT]))[COMPONENT]
    device_state = await subject.device_state(DEVICE)
    assert device_state is not None
    assert device_state.last_received_at == RECEIVED + timedelta(minutes=1)
    assert device_state.last_ingested_at == RECEIVED + timedelta(minutes=1, seconds=1)
    assert state.last_received_at == RECEIVED + timedelta(minutes=1)
    assert state.last_ingested_at == RECEIVED + timedelta(minutes=1, seconds=1)
    assert (state.last_observed_at, state.last_observed_sequence) == (
        OBSERVED + timedelta(minutes=1),
        11,
    )
    health = await subject.repository.get_connector_health(TENANT, ACCOUNT)
    assert health is not None
    assert health.last_received_at == RECEIVED + timedelta(minutes=1)


async def state_connector_marks_never_regress(subject: IngestStateKitSubject) -> None:
    later = RECEIVED + timedelta(minutes=5)
    newer = IngestMarks(
        tenant_id=TENANT,
        device_id=None,
        integration_account_id=ACCOUNT,
        component_marks=(),
        device_mark=None,
        connector_mark=ConnectorMark(
            received_at=later,
            accepted_at=later,
            quarantined_at=later,
            quarantine_reason="metric_unknown",
            fenced_at=later,
            fence_reason="account_revoked",
        ),
    )
    older = newer.model_copy(
        update={
            "connector_mark": ConnectorMark(
                received_at=RECEIVED,
                accepted_at=RECEIVED,
                quarantined_at=RECEIVED,
                quarantine_reason="value_invalid",
                fenced_at=RECEIVED,
                fence_reason="integration_mismatch",
            )
        }
    )

    await subject.repository.apply_ingest_marks(newer)
    await subject.repository.apply_ingest_marks(older)

    assert await subject.repository.get_connector_health(TENANT, ACCOUNT) == ConnectorHealth(
        tenant_id=TENANT,
        integration_account_id=ACCOUNT,
        last_received_at=later,
        last_accepted_at=later,
        last_quarantined_at=later,
        last_quarantine_reason="metric_unknown",
        last_fenced_at=later,
        last_fence_reason="account_revoked",
    )


async def state_ties_are_order_independent(subject: IngestStateKitSubject) -> None:
    first_account, second_account = ACCOUNT, OTHER_ACCOUNT
    low = {"observed_at": OBSERVED, "sequence": 5, "quarantine_reason": "metric_unknown"}
    high = {"observed_at": OBSERVED, "sequence": 9, "quarantine_reason": "value_invalid"}
    for component_id, account_id, order in (
        (COMPONENT, first_account, (low, high)),
        (OTHER_COMPONENT, second_account, (high, low)),
    ):
        for values in order:
            await subject.repository.apply_ingest_marks(
                marks(
                    component_id=component_id,
                    integration_account_id=account_id,
                    quarantined_at=RECEIVED,
                    **values,
                )
            )

    states = await subject.repository.get_component_states(TENANT, [COMPONENT, OTHER_COMPONENT])
    assert {state.last_observed_sequence for state in states.values()} == {9}
    for account_id in (first_account, second_account):
        health = await subject.repository.get_connector_health(TENANT, account_id)
        assert health is not None
        assert health.last_quarantine_reason == "value_invalid"


async def state_fence_marks_touch_only_the_connector(subject: IngestStateKitSubject) -> None:
    fenced = IngestMarks(
        tenant_id=TENANT,
        device_id=None,
        integration_account_id=ACCOUNT,
        component_marks=(),
        device_mark=None,
        connector_mark=ConnectorMark(fenced_at=RECEIVED, fence_reason="device_decommissioned"),
    )

    await subject.repository.apply_ingest_marks(fenced)

    assert await subject.repository.get_component_states(TENANT, [COMPONENT]) == {}
    assert await subject.device_state(DEVICE) is None
    health = await subject.repository.get_connector_health(TENANT, ACCOUNT)
    assert health == ConnectorHealth(
        tenant_id=TENANT,
        integration_account_id=ACCOUNT,
        last_received_at=None,
        last_accepted_at=None,
        last_quarantined_at=None,
        last_quarantine_reason=None,
        last_fenced_at=RECEIVED,
        last_fence_reason="device_decommissioned",
    )


async def state_fence_reason_ties_are_order_independent(subject: IngestStateKitSubject) -> None:
    def fenced(account_id: str, reason: str) -> IngestMarks:
        return IngestMarks(
            tenant_id=TENANT,
            device_id=None,
            integration_account_id=account_id,
            component_marks=(),
            device_mark=None,
            connector_mark=ConnectorMark(fenced_at=RECEIVED, fence_reason=reason),
        )

    for account_id, order in (
        (ACCOUNT, ("account_revoked", "integration_mismatch")),
        (OTHER_ACCOUNT, ("integration_mismatch", "account_revoked")),
    ):
        for reason in order:
            await subject.repository.apply_ingest_marks(fenced(account_id, reason))

    for account_id in (ACCOUNT, OTHER_ACCOUNT):
        health = await subject.repository.get_connector_health(TENANT, account_id)
        assert health is not None
        assert (health.last_fenced_at, health.last_fence_reason) == (
            RECEIVED,
            "integration_mismatch",
        )


async def state_empty_marks_write_nothing(subject: IngestStateKitSubject) -> None:
    empty = IngestMarks(
        tenant_id=TENANT,
        device_id=None,
        integration_account_id=ACCOUNT,
        component_marks=(),
        device_mark=None,
        connector_mark=ConnectorMark(),
    )

    await subject.repository.apply_ingest_marks(empty)

    assert await subject.repository.get_connector_health(TENANT, ACCOUNT) is None
    assert await subject.device_state(DEVICE) is None


INGEST_STATE_CONTRACT = (
    state_connector_marks_never_regress,
    _fault_scenario(
        "state_apply", "apply_ingest_marks", lambda s: s.repository.apply_ingest_marks(marks())
    ),
    _fault_scenario(
        "state_components",
        "get_component_states",
        lambda s: s.repository.get_component_states(TENANT, [COMPONENT]),
    ),
    _fault_scenario(
        "state_health",
        "get_connector_health",
        lambda s: s.repository.get_connector_health(TENANT, ACCOUNT),
    ),
    state_empty_marks_write_nothing,
    state_fence_reason_ties_are_order_independent,
    state_apply_then_read,
    state_rows_are_tenant_checked,
    state_marks_are_monotone,
    state_ties_are_order_independent,
    state_fence_marks_touch_only_the_connector,
)


# Rate limiter, entitlement and metrics ------------------------------------------------------

RateLimiterFactory = Callable[..., Any]  # (capacity=, refill_per_second=, max_keys=) -> limiter


def limiter_capacity_then_refill(make: RateLimiterFactory) -> None:
    limiter = make(capacity=3, refill_per_second=1.0, max_keys=100)

    assert [limiter.try_acquire("a", now=0.0) for _ in range(3)] == [None, None, None]
    retry_after = limiter.try_acquire("a", now=0.0)
    assert retry_after is not None and 0.0 < retry_after <= 1.0
    assert limiter.try_acquire("b", now=0.0) is None
    assert limiter.try_acquire("a", now=1.0) is None
    assert limiter.try_acquire("a", now=1.0) is not None


def limiter_evicts_the_least_recently_used_key(make: RateLimiterFactory) -> None:
    limiter = make(capacity=1, refill_per_second=0.001, max_keys=2)

    assert limiter.try_acquire("a", now=0.0) is None
    assert limiter.try_acquire("b", now=0.0) is None
    assert limiter.try_acquire("a", now=0.0) is not None  # "a" becomes most recently used
    assert limiter.try_acquire("c", now=0.0) is None  # evicts "b"
    assert limiter.try_acquire("a", now=0.0) is not None  # "a" survived the eviction
    assert limiter.try_acquire("b", now=0.0) is None  # "b" starts with a fresh bucket


RATE_LIMITER_CONTRACT = (
    limiter_capacity_then_refill,
    limiter_evicts_the_least_recently_used_key,
)


class EntitlementKitSubject(FaultInjectable, Protocol):
    check: Any  # IngressEntitlementCheck


async def entitlement_default_allows(subject: EntitlementKitSubject) -> None:
    assert await subject.check.allows(TENANT, ACCOUNT, 1) is True
    assert await subject.check.allows(TENANT, ACCOUNT, 1_000) is True


ENTITLEMENT_CHECK_CONTRACT = (entitlement_default_allows,)


class MetricsKitSubject(Protocol):
    metrics: Any  # TelemetryMetrics

    def recorded(self) -> list[dict[str, Any]]: ...


async def metrics_records_one_object_per_call(subject: MetricsKitSubject) -> None:
    subject.metrics.increment("ingest_requests_total", outcome="accepted")
    subject.metrics.increment("ingress_observations_total", 3, result="quarantined")
    subject.metrics.observe("ingress_queue_age_seconds", 1.5)

    assert subject.recorded() == [
        {"metric": "ingest_requests_total", "value": 1, "labels": {"outcome": "accepted"}},
        {"metric": "ingress_observations_total", "value": 3, "labels": {"result": "quarantined"}},
        {"metric": "ingress_queue_age_seconds", "value": 1.5, "labels": {}},
    ]


async def metrics_refuse_open_names_and_labels(subject: MetricsKitSubject) -> None:
    for call in (
        lambda: subject.metrics.increment(
            "ingest_requests_total", outcome="accepted", tenant_id=TENANT
        ),
        lambda: subject.metrics.increment("ingest_requests_total", outcome="bogus"),
        lambda: subject.metrics.increment("ingest_unknown_total"),
        lambda: subject.metrics.observe("ingress_queue_age_seconds", 1.0, device_id=DEVICE),
    ):
        with pytest.raises(ValueError):
            call()

    assert subject.recorded() == []


TELEMETRY_METRICS_CONTRACT = (
    metrics_records_one_object_per_call,
    metrics_refuse_open_names_and_labels,
)


# Store-failure propagation, one scenario per port operation ---------------------------------
#
# A store failure is never swallowed: an authenticator must not answer None (401 instead of 503),
# a mapping must not deny or fence, and a repository must not report "missing".


async def _seed_credential_account(subject: Any) -> None:
    await subject.seed_account(integration_account())


async def _seed_issued_credential(subject: Any) -> None:
    await subject.seed_account(integration_account())
    await subject.repository.create_ingest_credential(credential(), AUDIT)


async def _seed_full_mapping(subject: Any) -> None:
    await _seed_mapping(subject, components=(component(),))


async def _seed_quarantine(subject: Any) -> None:
    subject.set_now(RECEIVED)
    await subject.repository.put_quarantine_entries(fenced_entries())


async def _seed_received_message(subject: Any) -> None:
    await subject.producer.send(envelope())
    (message,) = await _receive(subject)
    subject.kit_received_message = message


def queue_errors() -> tuple[BaseException, ...]:
    return (
        IngressQueueError("transient send failure", retryable=True),
        IngressQueueError("permanent send failure", retryable=False),
    )


CREDENTIAL_REPOSITORY_CONTRACT += (
    _fault_scenario(
        "credential_get",
        "get_ingest_credential",
        lambda s: s.repository.get_ingest_credential(CREDENTIAL),
    ),
    _fault_scenario(
        "credential_list",
        "list_ingest_credentials",
        lambda s: s.repository.list_ingest_credentials(TENANT, ACCOUNT),
    ),
    _fault_scenario(
        "credential_create",
        "create_ingest_credential",
        lambda s: s.repository.create_ingest_credential(credential(), AUDIT),
        prepare=_seed_credential_account,
    ),
    _fault_scenario(
        "credential_revoke",
        "revoke_ingest_credential",
        lambda s: s.repository.revoke_ingest_credential(
            TENANT, ACCOUNT, CREDENTIAL, expected_version=1, revoked_at=RECEIVED, audit=AUDIT
        ),
        prepare=_seed_issued_credential,
    ),
)
CONNECTOR_AUTHENTICATOR_CONTRACT += (
    _fault_scenario(
        "authenticate",
        "authenticate",
        lambda s: s.authenticator.authenticate(ingest_token(CREDENTIAL, SECRET)),
        prepare=_seed_authenticable,
    ),
    _fault_scenario(
        "authenticate_account_read",
        "get_integration_account",
        lambda s: s.authenticator.authenticate(ingest_token(CREDENTIAL, SECRET)),
        prepare=_seed_authenticable,
    ),
)
EXTERNAL_IDENTITY_RESOLVER_CONTRACT += (
    _fault_scenario(
        "resolve",
        "resolve_external_identity",
        lambda s: s.resolver.resolve_external_identity(PROVIDER, IDENTITY_HASH),
    ),
)
INTEGRATION_LIFECYCLE_CONTRACT += (
    _fault_scenario(
        "lifecycle_account",
        "transition_integration_account",
        lambda s: s.repository.transition_integration_account(
            TENANT,
            ACCOUNT,
            IntegrationAccountStatus.ERROR,
            expected_version=1,
            updated_at=RECEIVED,
            audit=AUDIT,
        ),
        prepare=_seed_credential_account,
    ),
    _fault_scenario(
        "lifecycle_device_integration",
        "transition_device_integration",
        lambda s: s.repository.transition_device_integration(
            TENANT,
            INTEGRATION,
            DeviceIntegrationStatus.ERROR,
            expected_version=1,
            updated_at=RECEIVED,
            audit=AUDIT,
        ),
        prepare=lambda s: s.seed_device_integration(device_integration()),
    ),
)
CONSISTENT_DEVICE_READER_CONTRACT += (
    _fault_scenario(
        "device_read",
        "get_device",
        lambda s: s.reader.get_device(TENANT, DEVICE, consistent=True),
        prepare=lambda s: s.seed_device(device()),
    ),
)
TRUSTED_MAPPING_CONTRACT += (
    _fault_scenario(
        "admit",
        "admit",
        lambda s: s.mapping.admit(SOURCE, batch(COMPONENT)),
        prepare=_seed_full_mapping,
    ),
    _fault_scenario(
        "fence", "fence", lambda s: s.mapping.fence(envelope()), prepare=_seed_full_mapping
    ),
    # The last reads: a component-read failure must be a 503, never COMPONENT_INVALID.
    _fault_scenario(
        "admit_device_read",
        "get_device",
        lambda s: s.mapping.admit(SOURCE, batch(COMPONENT)),
        prepare=_seed_full_mapping,
    ),
    _fault_scenario(
        "admit_component_read",
        "get_component",
        lambda s: s.mapping.admit(SOURCE, batch(COMPONENT)),
        prepare=_seed_full_mapping,
    ),
    _fault_scenario(
        "fence_account_read",
        "get_integration_account",
        lambda s: s.mapping.fence(envelope()),
        prepare=_seed_full_mapping,
    ),
    _fault_scenario(
        "fence_device_read",
        "get_device",
        lambda s: s.mapping.fence(envelope()),
        prepare=_seed_full_mapping,
    ),
)
INGRESS_QUEUE_CONTRACT += (
    _fault_scenario(
        "queue_send", "send", lambda s: s.producer.send(envelope()), errors=queue_errors
    ),
    _fault_scenario(
        "queue_send_raw", "send_raw", lambda s: s.producer.send_raw("{}"), errors=queue_errors
    ),
    _fault_scenario("queue_receive", "receive", _receive, errors=queue_errors),
    _fault_scenario(
        "queue_change_visibility",
        "change_visibility",
        lambda s: s.consumer.change_visibility(s.kit_received_message, 30),
        prepare=_seed_received_message,
        errors=queue_errors,
    ),
    _fault_scenario(
        "queue_delete",
        "delete",
        lambda s: s.consumer.delete(s.kit_received_message),
        prepare=_seed_received_message,
        errors=queue_errors,
    ),
)
QUARANTINE_REPOSITORY_CONTRACT += (
    _fault_scenario(
        "quarantine_put",
        "put_quarantine_entries",
        lambda s: s.repository.put_quarantine_entries(fenced_entries()),
        prepare=lambda s: _set_now(s, RECEIVED),
    ),
    _fault_scenario(
        "quarantine_list",
        "list_quarantine_entries",
        lambda s: s.repository.list_quarantine_entries(TENANT, after_sk=None, limit=10),
        prepare=_seed_quarantine,
    ),
    _fault_scenario(
        "quarantine_get",
        "get_quarantine_entry",
        lambda s: s.repository.get_quarantine_entry(TENANT, fenced_entries()[0].entry_key),
        prepare=_seed_quarantine,
    ),
    _fault_scenario(
        "quarantine_transition",
        "transition_quarantine_entry",
        lambda s: s.repository.transition_quarantine_entry(
            TENANT,
            fenced_entries()[0].entry_key,
            QuarantineEntryStatus.DISMISSED,
            expected_version=1,
            at=RECEIVED,
        ),
        prepare=_seed_quarantine,
    ),
)
ENTITLEMENT_CHECK_CONTRACT += (
    _fault_scenario("entitlement", "allows", lambda s: s.check.allows(TENANT, ACCOUNT, 1)),
)


async def _set_now(subject: Any, now: datetime) -> None:
    subject.set_now(now)


# DynamoDB expression vectors ------------------------------------------------------------------
#
# Every expression the P3 adapters emit, plus the M1 expressions, as (seed, request, expectation).
# `run_dynamo_vector(client, table_name, vector)` runs one against a boto3-shaped client: the
# Dynamo fake in unit tests, DynamoDB Local in `tests/ingestion/` (P3-06).

PK, SK = "VECTOR#1", "ROW"


@dataclass(frozen=True)
class DynamoVector:
    name: str
    operation: str  # put_item | update_item | delete_item | transact_write_items | query
    request: Mapping[str, Any]  # plain Python values; the runner serializes and adds TableName
    seed: tuple[Mapping[str, Any], ...] = ()
    succeeds: bool = True
    # Expected rows after the call (key -> item, or None for "absent").
    after: Mapping[tuple[str, str], Mapping[str, Any] | None] = field(default_factory=dict)
    old_item: Mapping[str, Any] | None = None  # ALL_OLD item on a single-item conditional failure
    cancellation_codes: tuple[str, ...] = ()
    cancellation_items: Mapping[int, Mapping[str, Any]] = field(default_factory=dict)
    query_sks: tuple[str, ...] = ()
    query_has_more: bool | None = None  # LastEvaluatedKey expected (True) or absent (False)


def _row(**attributes: Any) -> dict[str, Any]:
    return {"PK": PK, "SK": SK, **attributes}


def _put(condition: str, *, names: Mapping[str, str] | None = None, **values: Any) -> dict:
    request: dict[str, Any] = {"Item": _row(written=1), "ConditionExpression": condition}
    if names:
        request["ExpressionAttributeNames"] = dict(names)
    if values:
        request["ExpressionAttributeValues"] = {f":{key}": value for key, value in values.items()}
    return request


def _update(
    expression: str,
    condition: str | None,
    *,
    names: Mapping[str, str] | None = None,
    values: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    request: dict[str, Any] = {"Key": {"PK": PK, "SK": SK}, "UpdateExpression": expression}
    if condition is not None:
        request["ConditionExpression"] = condition
    if names:
        request["ExpressionAttributeNames"] = dict(names)
    if values:
        request["ExpressionAttributeValues"] = dict(values)
    return request


def _vectors_put_new() -> list[DynamoVector]:
    condition = "attribute_not_exists(PK)"
    return [
        DynamoVector(
            "put_new_absent", "put_item", _put(condition), after={(PK, SK): _row(written=1)}
        ),
        DynamoVector(
            "put_new_present",
            "put_item",
            {**_put(condition), "ReturnValuesOnConditionCheckFailure": "ALL_OLD"},
            seed=(_row(old=1),),
            succeeds=False,
            after={(PK, SK): _row(old=1)},
            old_item=_row(old=1),
        ),
    ]


def _vectors_receipt_claim() -> list[DynamoVector]:
    condition = "attribute_not_exists(PK) OR #created_at_us <= :cutoff_us"
    names = {"#created_at_us": "created_at_us"}
    cases = [("expired_equal", 100, True), ("expired_older", 99, True), ("live", 101, False)]
    vectors = [
        DynamoVector(
            "receipt_claim_absent",
            "put_item",
            _put(condition, names=names, cutoff_us=100),
            after={(PK, SK): _row(written=1)},
        )
    ]
    for name, created_at_us, succeeds in cases:
        seeded = _row(created_at_us=created_at_us)
        vectors.append(
            DynamoVector(
                f"receipt_claim_{name}",
                "put_item",
                _put(condition, names=names, cutoff_us=100),
                seed=(seeded,),
                succeeds=succeeds,
                after={(PK, SK): _row(written=1) if succeeds else seeded},
            )
        )
    return vectors


def _vectors_issuance_check() -> list[DynamoVector]:
    condition = "attribute_exists(PK) AND #status IN (:pending, :active) AND provider_key = :https"
    check = {
        "ConditionCheck": {
            "Key": {"PK": PK, "SK": SK},
            "ConditionExpression": condition,
            "ExpressionAttributeNames": {"#status": "status"},
            "ExpressionAttributeValues": {
                ":pending": "pending",
                ":active": "active",
                ":https": "https_direct",
            },
            "ReturnValuesOnConditionCheckFailure": "ALL_OLD",
        }
    }
    slot = {
        "Put": {
            "Item": {"PK": PK, "SK": "SLOT#1"},
            "ConditionExpression": "attribute_not_exists(PK)",
        }
    }
    cases = [
        ("active", _row(status="active", provider_key="https_direct"), True),
        ("pending", _row(status="pending", provider_key="https_direct"), True),
        ("revoked", _row(status="revoked", provider_key="https_direct"), False),
        ("vendor", _row(status="active", provider_key="vendor_cloud"), False),
        ("missing", None, False),
    ]
    vectors = []
    for name, account, succeeds in cases:
        vectors.append(
            DynamoVector(
                f"issuance_check_{name}",
                "transact_write_items",
                {"TransactItems": [check, slot]},
                seed=() if account is None else (account,),
                succeeds=succeeds,
                after={(PK, "SLOT#1"): {"PK": PK, "SK": "SLOT#1"} if succeeds else None},
                cancellation_codes=() if succeeds else ("ConditionalCheckFailed", "None"),
                cancellation_items={} if succeeds or account is None else {0: account},
            )
        )
    return vectors


def _vectors_versions_and_conjunctions() -> list[DynamoVector]:
    vectors = []
    plain_version = ("#version = :v", {"#version": "version"})
    m1_version = ("attribute_exists(PK) AND #version = :v", {"#version": "version"})
    conjunction = (
        "tenant_id = :t AND integration_account_id = :ia AND #status = :active AND #version = :v",
        {"#status": "status", "#version": "version"},
    )
    seeded = _row(version=1, status="active", tenant_id="t1", integration_account_id="ia1")
    for label, (condition, names) in {
        "version": plain_version,
        "m1_version": m1_version,
        "conjunction": conjunction,
    }.items():
        for version, succeeds in ((1, True), (2, False)):
            values = {":v": version, ":t": "t1", ":ia": "ia1", ":active": "active"}
            used = {key: value for key, value in values.items() if key in condition}
            used[":new"] = 2
            vectors.append(
                DynamoVector(
                    f"{label}_{'match' if succeeds else 'stale'}",
                    "update_item",
                    {
                        **_update("SET #version = :new", condition, names=names, values=used),
                        "ReturnValuesOnConditionCheckFailure": "ALL_OLD",
                    },
                    seed=(seeded,),
                    succeeds=succeeds,
                    after={(PK, SK): {**seeded, "version": 2} if succeeds else seeded},
                    old_item=None if succeeds else seeded,
                )
            )
    not_decommissioned = "attribute_exists(PK) AND #status <> :decommissioned"
    for status, succeeds in (("active", True), ("decommissioned", False)):
        row = _row(status=status)
        vectors.append(
            DynamoVector(
                f"m1_not_decommissioned_{status}",
                "update_item",
                _update(
                    "SET touched = :one",
                    not_decommissioned,
                    names={"#status": "status"},
                    values={":decommissioned": "decommissioned", ":one": 1},
                ),
                seed=(row,),
                succeeds=succeeds,
                after={(PK, SK): {**row, "touched": 1} if succeeds else row},
            )
        )
    return vectors


def _vectors_marks() -> list[DynamoVector]:
    simple = "attribute_not_exists(#m) OR #m < :t"
    expression = "SET #m = :t, tenant_id = if_not_exists(tenant_id, :tenant)"
    names = {"#m": "last_received_at"}
    vectors = []
    for name, existing, succeeds in (
        ("absent", None, True),
        ("older", "2026-10-30T11:59:59.999999Z", True),
        ("equal", "2026-10-30T12:00:00.000000Z", False),
        ("newer", "2026-10-30T12:00:00.000001Z", False),
    ):
        seeded = (
            _row(tenant_id="t1")
            if existing is None
            else _row(tenant_id="t1", last_received_at=existing)
        )
        vectors.append(
            DynamoVector(
                f"simple_mark_{name}",
                "update_item",
                _update(
                    expression,
                    simple,
                    names=names,
                    values={":t": "2026-10-30T12:00:00.000000Z", ":tenant": "t2"},
                ),
                seed=(seeded,),
                succeeds=succeeds,
                after={
                    (PK, SK): {**seeded, "last_received_at": "2026-10-30T12:00:00.000000Z"}
                    if succeeds
                    else seeded
                },
            )
        )
    vectors.append(
        DynamoVector(
            "simple_mark_sets_identity_on_create",
            "update_item",
            _update(
                expression,
                simple,
                names=names,
                values={":t": "2026-10-30T12:00:00.000000Z", ":tenant": "t2"},
            ),
            after={(PK, SK): _row(tenant_id="t2", last_received_at="2026-10-30T12:00:00.000000Z")},
        )
    )
    paired = "attribute_not_exists(#m) OR #m < :t OR (#m = :t AND #tb < :tb)"
    paired_names = {"#m": "last_observed_at", "#tb": "last_observed_sequence"}
    for name, existing, succeeds in (
        ("absent", None, True),
        ("older", ("2026-10-30T11:00:00.000000Z", 99), True),
        ("tie_lower_breaker", ("2026-10-30T12:00:00.000000Z", 4), True),
        ("tie_equal_breaker", ("2026-10-30T12:00:00.000000Z", 5), False),
        ("tie_higher_breaker", ("2026-10-30T12:00:00.000000Z", 6), False),
        ("newer", ("2026-10-30T13:00:00.000000Z", 1), False),
    ):
        seeded = (
            _row()
            if existing is None
            else _row(last_observed_at=existing[0], last_observed_sequence=existing[1])
        )
        written = {
            **seeded,
            "last_observed_at": "2026-10-30T12:00:00.000000Z",
            "last_observed_sequence": 5,
        }
        vectors.append(
            DynamoVector(
                f"paired_mark_{name}",
                "update_item",
                _update(
                    "SET #m = :t, #tb = :tb",
                    paired,
                    names=paired_names,
                    values={":t": "2026-10-30T12:00:00.000000Z", ":tb": 5},
                ),
                seed=(seeded,),
                succeeds=succeeds,
                after={(PK, SK): written if succeeds else seeded},
            )
        )
    return vectors


def _vectors_remove_delete_transactions() -> list[DynamoVector]:
    leased = _row(status="open", lease_owner="worker-1", version=1)
    slot = {"PK": PK, "SK": "SLOT#1", "ingest_credential_id": "ic1"}
    vectors = [
        DynamoVector(
            "update_set_and_remove",
            "update_item",
            _update(
                "SET #status = :s, #version = :v2 REMOVE lease_owner",
                "#version = :v1",
                names={"#status": "status", "#version": "version"},
                values={":s": "replayed", ":v1": 1, ":v2": 2},
            ),
            seed=(leased,),
            after={(PK, SK): _row(status="replayed", version=2)},
        ),
        DynamoVector(
            "delete_conditioned_match",
            "delete_item",
            {
                "Key": {"PK": PK, "SK": "SLOT#1"},
                "ConditionExpression": "ingest_credential_id = :ic",
                "ExpressionAttributeValues": {":ic": "ic1"},
            },
            seed=(slot,),
            after={(PK, "SLOT#1"): None},
        ),
        DynamoVector(
            "delete_conditioned_other",
            "delete_item",
            {
                "Key": {"PK": PK, "SK": "SLOT#1"},
                "ConditionExpression": "ingest_credential_id = :ic",
                "ExpressionAttributeValues": {":ic": "ic2"},
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD",
            },
            seed=(slot,),
            succeeds=False,
            after={(PK, "SLOT#1"): slot},
            old_item=slot,
        ),
    ]
    credential_row = _row(status="active", version=1)
    actions = [
        {
            "Put": {
                "Item": {"PK": PK, "SK": "AUDIT#1"},
                "ConditionExpression": "attribute_not_exists(PK)",
            }
        },
        {
            "Update": {
                "Key": {"PK": PK, "SK": SK},
                "UpdateExpression": "SET #status = :revoked, #version = :v2",
                "ConditionExpression": "#status = :active AND #version = :v1",
                "ExpressionAttributeNames": {"#status": "status", "#version": "version"},
                "ExpressionAttributeValues": {
                    ":revoked": "revoked",
                    ":active": "active",
                    ":v1": 1,
                    ":v2": 2,
                },
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD",
            }
        },
        {
            "Delete": {
                "Key": {"PK": PK, "SK": "SLOT#1"},
                "ConditionExpression": "ingest_credential_id = :ic",
                "ExpressionAttributeValues": {":ic": "ic1"},
            }
        },
    ]
    vectors.append(
        DynamoVector(
            "transaction_commits_every_action",
            "transact_write_items",
            {"TransactItems": actions},
            seed=(credential_row, slot),
            after={
                (PK, "AUDIT#1"): {"PK": PK, "SK": "AUDIT#1"},
                (PK, SK): _row(status="revoked", version=2),
                (PK, "SLOT#1"): None,
            },
        )
    )
    stale = _row(status="active", version=3)
    vectors.append(
        DynamoVector(
            "transaction_reports_reasons_by_index",
            "transact_write_items",
            {"TransactItems": actions},
            seed=(stale, slot),
            succeeds=False,
            after={(PK, "AUDIT#1"): None, (PK, SK): stale, (PK, "SLOT#1"): slot},
            cancellation_codes=("None", "ConditionalCheckFailed", "None"),
            cancellation_items={1: stale},
        )
    )
    return vectors


def _vectors_queries() -> list[DynamoVector]:
    rows = tuple(
        {"PK": "Q#1", "SK": sk}
        for sk in (
            "2026-10-30T12:00:00.000000Z#qtn_a",
            "2026-10-30T12:00:01.000000Z#qtn_b",
            "2026-10-30T12:00:02.000000Z#qtn_c",
            "INGEST_CREDENTIAL#ia1#ic1",
            "INGEST_CREDENTIAL#ia1#ic2",
            "INGEST_CREDENTIAL#ia2#ic3",
        )
    )
    return [
        DynamoVector(
            "query_begins_with",
            "query",
            {
                "KeyConditionExpression": "PK = :pk AND begins_with(SK, :sk_prefix)",
                "ExpressionAttributeValues": {":pk": "Q#1", ":sk_prefix": "INGEST_CREDENTIAL#ia1#"},
            },
            seed=rows,
            query_sks=("INGEST_CREDENTIAL#ia1#ic1", "INGEST_CREDENTIAL#ia1#ic2"),
            query_has_more=False,
        ),
        DynamoVector(
            "query_between",
            "query",
            {
                "KeyConditionExpression": "PK = :pk AND SK BETWEEN :sk_lower AND :sk_upper",
                "ExpressionAttributeValues": {
                    ":pk": "Q#1",
                    ":sk_lower": "2026-10-30T12:00:01",
                    ":sk_upper": "2026-10-30T12:00:02.000000Z#~",
                },
            },
            seed=rows,
            query_sks=(rows[1]["SK"], rows[2]["SK"]),
        ),
        DynamoVector(
            "query_greater_than_with_limit",
            "query",
            {
                "KeyConditionExpression": "PK = :pk AND SK > :sk_lower",
                "ExpressionAttributeValues": {":pk": "Q#1", ":sk_lower": rows[0]["SK"]},
                "Limit": 2,
            },
            seed=rows,
            query_sks=(rows[1]["SK"], rows[2]["SK"]),
            query_has_more=True,
        ),
        DynamoVector(
            "query_descending",
            "query",
            {
                "KeyConditionExpression": "PK = :pk AND SK BETWEEN :sk_lower AND :sk_upper",
                "ExpressionAttributeValues": {
                    ":pk": "Q#1",
                    ":sk_lower": "2026",
                    ":sk_upper": "2026~",
                },
                "ScanIndexForward": False,
                "Limit": 2,
            },
            seed=rows,
            query_sks=(rows[2]["SK"], rows[1]["SK"]),
        ),
    ]


def _vectors_m1() -> list[DynamoVector]:
    """Expressions the existing M1 adapters emit (`adapters/dynamodb_*.py`)."""
    vectors: list[DynamoVector] = []
    receipt_condition = "attribute_not_exists(PK) OR #expires_at <= :now"
    for name, seeded, succeeds in (
        ("absent", None, True),
        ("expired", _row(expires_at=100), True),
        ("live", _row(expires_at=101), False),
    ):
        vectors.append(
            DynamoVector(
                f"m1_receipt_{name}",
                "put_item",
                _put(receipt_condition, names={"#expires_at": "expires_at"}, now=100),
                seed=() if seeded is None else (seeded,),
                succeeds=succeeds,
                after={(PK, SK): _row(written=1) if succeeds else seeded},
            )
        )
    guarded = _row(
        status="active", tenant_id="t1", version=3, last_ended_at="2026-01-02", deployment_id="d1"
    )
    names = {
        "#status": "status",
        "#tenant_id": "tenant_id",
        "#version": "version",
        "#last_ended_at": "last_ended_at",
        "#deployment_id": "deployment_id",
    }
    cases = [
        ("status_tenant", "attribute_exists(PK) AND #status = :active AND #tenant_id = :tenant_id"),
        ("tenant", "attribute_exists(PK) AND #tenant_id = :tenant_id"),
        ("status_in", "attribute_exists(PK) AND #status IN (:pending, :active)"),
        ("deployment", "attribute_exists(PK) AND #deployment_id = :deployment_id"),
        (
            "version_tenant",
            "attribute_exists(PK) AND #version = :expected_version AND #tenant_id = :tenant_id",
        ),
        (
            "ended_before",
            "attribute_exists(PK) AND (attribute_not_exists(#last_ended_at) "
            "OR #last_ended_at <= :started_at)",
        ),
    ]
    matching = {
        ":active": "active",
        ":pending": "pending",
        ":tenant_id": "t1",
        ":expected_version": 3,
        ":started_at": "2026-01-03",
        ":deployment_id": "d1",
    }
    failing = {
        ":active": "active",
        ":pending": "pending",
        ":tenant_id": "t2",
        ":expected_version": 2,
        ":started_at": "2026-01-01",
        ":deployment_id": "d2",
    }
    for label, condition in cases:
        used_names = {key: value for key, value in names.items() if key in condition}
        for outcome, values, row in (
            ("match", matching, guarded),
            ("mismatch", failing, {**guarded, "status": "revoked"}),
            ("absent", matching, None),
        ):
            used = {key: value for key, value in values.items() if key in condition}
            used[":one"] = 1
            succeeds = outcome == "match"
            if label == "ended_before" and outcome == "mismatch":
                row = {**guarded, "last_ended_at": "2026-01-02"}
            if label == "tenant" and outcome == "mismatch":
                row = guarded
            if label == "status_in" and outcome == "mismatch":
                row = {**guarded, "status": "revoked"}
            vectors.append(
                DynamoVector(
                    f"m1_{label}_{outcome}",
                    "update_item",
                    _update("SET touched = :one", condition, names=used_names, values=used),
                    seed=() if row is None else (row,),
                    succeeds=succeeds,
                    after={(PK, SK): {**row, "touched": 1} if succeeds else row},
                )
            )
    migration = (
        "attribute_not_exists(PK) OR (#entity_type = :entity_type "
        "AND #tenant_id = :tenant_id AND #migration = :migration)"
    )
    migration_names = {
        "#entity_type": "entity_type",
        "#tenant_id": "tenant_id",
        "#migration": "migration",
    }
    for name, seeded, succeeds in (
        ("absent", None, True),
        ("same", _row(entity_type="migration", tenant_id="t1", migration="m1"), True),
        ("other", _row(entity_type="migration", tenant_id="t2", migration="m1"), False),
    ):
        vectors.append(
            DynamoVector(
                f"m1_migration_receipt_{name}",
                "put_item",
                _put(
                    migration,
                    names=migration_names,
                    entity_type="migration",
                    tenant_id="t1",
                    migration="m1",
                ),
                seed=() if seeded is None else (seeded,),
                succeeds=succeeds,
                after={(PK, SK): _row(written=1) if succeeds else seeded},
            )
        )
    counter = "SET #run_count = if_not_exists(#run_count, :zero) + :one"
    for name, seeded, expected in (("absent", None, 1), ("existing", _row(run_count=4), 5)):
        vectors.append(
            DynamoVector(
                f"m1_counter_{name}",
                "update_item",
                _update(
                    counter, None, names={"#run_count": "run_count"}, values={":zero": 0, ":one": 1}
                ),
                seed=() if seeded is None else (seeded,),
                after={(PK, SK): _row(run_count=expected)},
            )
        )
    vectors.append(
        DynamoVector(
            "remove_only",
            "update_item",
            _update("REMOVE lease_owner, lease_expires_at", "attribute_exists(PK)"),
            seed=(_row(status="open", lease_owner="w1", lease_expires_at=5),),
            after={(PK, SK): _row(status="open")},
        )
    )
    rows = tuple({"PK": "Q#2", "SK": sk} for sk in ("A", "B", "C", "D"))
    for name, condition, values, expected in (
        ("upper", "PK = :pk AND SK <= :sk_upper", {":sk_upper": "B"}, ("A", "B")),
        ("lower", "PK = :pk AND SK >= :sk_lower", {":sk_lower": "C"}, ("C", "D")),
    ):
        vectors.append(
            DynamoVector(
                f"query_m1_{name}",
                "query",
                {
                    "KeyConditionExpression": condition,
                    "ExpressionAttributeValues": {":pk": "Q#2", **values},
                },
                seed=rows,
                query_sks=expected,
                query_has_more=False,
            )
        )
    return vectors


DYNAMO_EXPRESSION_VECTORS: tuple[DynamoVector, ...] = (
    *_vectors_m1(),
    *_vectors_put_new(),
    *_vectors_receipt_claim(),
    *_vectors_issuance_check(),
    *_vectors_versions_and_conjunctions(),
    *_vectors_marks(),
    *_vectors_remove_delete_transactions(),
    *_vectors_queries(),
)

_SERIALIZER = TypeSerializer()
_DESERIALIZER = TypeDeserializer()


def _encode(values: Mapping[str, Any]) -> dict[str, Any]:
    return {key: _SERIALIZER.serialize(value) for key, value in values.items()}


def _decode(values: Mapping[str, Any]) -> dict[str, Any]:
    def plain(value: Any) -> Any:
        if isinstance(value, Decimal):
            return int(value) if value == value.to_integral_value() else value
        return value

    return {key: plain(_DESERIALIZER.deserialize(value)) for key, value in values.items()}


def _wire(request: Mapping[str, Any], table_name: str) -> dict[str, Any]:
    wired: dict[str, Any] = {"TableName": table_name}
    for key, value in request.items():
        if key in {"Item", "Key", "ExpressionAttributeValues"}:
            wired[key] = _encode(value)
        else:
            wired[key] = value
    return wired


def _touched_keys(vector: DynamoVector) -> set[tuple[str, str]]:
    keys = {(item["PK"], item["SK"]) for item in vector.seed}
    keys.update(vector.after)
    bodies = [
        body for action in vector.request.get("TransactItems", []) for body in action.values()
    ] or [vector.request]
    for body in bodies:
        raw = body.get("Item") or body.get("Key")
        if raw is not None:
            keys.add((raw["PK"], raw["SK"]))
    return keys


def run_dynamo_vector(client: Any, table_name: str, vector: DynamoVector) -> None:
    """Runs one vector; it first deletes every row the vector touches, so vectors can share one
    table (DynamoDB Local) in any order."""
    for pk, sk in _touched_keys(vector):
        client.delete_item(TableName=table_name, Key=_encode({"PK": pk, "SK": sk}))
    for item in vector.seed:
        client.put_item(TableName=table_name, Item=_encode(item))
    if vector.operation == "query":
        response = client.query(**_wire(vector.request, table_name))
        assert tuple(_decode(item)["SK"] for item in response["Items"]) == vector.query_sks
        if vector.query_has_more is not None:
            assert ("LastEvaluatedKey" in response) is vector.query_has_more
        return
    if vector.operation == "transact_write_items":
        request = {
            "TransactItems": [
                {kind: _wire(body, table_name) for kind, body in action.items()}
                for action in vector.request["TransactItems"]
            ]
        }
    else:
        request = _wire(vector.request, table_name)
    operation = getattr(client, vector.operation)
    if vector.succeeds:
        operation(**request)
    else:
        with pytest.raises(ClientError) as exc_info:
            operation(**request)
        response = exc_info.value.response
        if vector.operation == "transact_write_items":
            assert response["Error"]["Code"] == "TransactionCanceledException"
            reasons = response["CancellationReasons"]
            assert tuple(reason["Code"] for reason in reasons) == vector.cancellation_codes
            for index, expected in vector.cancellation_items.items():
                assert _decode(reasons[index]["Item"]) == dict(expected)
        else:
            assert response["Error"]["Code"] == "ConditionalCheckFailedException"
            if vector.old_item is not None:
                assert _decode(response["Item"]) == dict(vector.old_item)
    for (pk, sk), expected in vector.after.items():
        found = client.get_item(
            TableName=table_name, Key=_encode({"PK": pk, "SK": sk}), ConsistentRead=True
        ).get("Item")
        if expected is None:
            assert found is None, (pk, sk)
        else:
            assert found is not None, (pk, sk)
            assert _decode(found) == dict(expected), (pk, sk)
