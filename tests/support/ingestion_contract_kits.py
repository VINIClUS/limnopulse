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
from botocore.exceptions import ClientError

from limnopulse_api.adapters.dynamodb_v2 import account_scoped_identity_hash
from limnopulse_api.core.errors import ConflictError, IngressMessageTooLarge, NotFoundError
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
    QUARANTINE_TTL,
    AuthenticatedSource,
    ComponentIngestState,
    ComponentMark,
    ConnectorHealth,
    ConnectorMark,
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


class CredentialKitSubject(Protocol):
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

    second = credential(ingest_credential_id=OTHER_CREDENTIAL, slot=2)
    assert await subject.repository.create_ingest_credential(second, AUDIT) == second
    listed = await subject.repository.list_ingest_credentials(TENANT, ACCOUNT)
    assert sorted(item.ingest_credential_id for item in listed) == [CREDENTIAL, OTHER_CREDENTIAL]


async def credential_requires_an_eligible_account(subject: CredentialKitSubject) -> None:
    # No IntegrationAccount row at all.
    with pytest.raises((ConflictError, NotFoundError)):
        await subject.repository.create_ingest_credential(credential(), AUDIT)
    await subject.seed_account(integration_account(status=IntegrationAccountStatus.REVOKED))
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

    with pytest.raises(ConflictError) as exc_info:
        await subject.repository.revoke_ingest_credential(
            TENANT, ACCOUNT, CREDENTIAL, expected_version=7, revoked_at=RECEIVED, audit=AUDIT
        )

    assert not isinstance(exc_info.value, NotFoundError)
    assert await subject.repository.get_ingest_credential(CREDENTIAL) == credential()


async def credential_revoke_never_reveals_foreign_rows(subject: CredentialKitSubject) -> None:
    await subject.seed_account(integration_account())
    await subject.repository.create_ingest_credential(credential(), AUDIT)
    attempts = [
        (OTHER_TENANT, ACCOUNT, CREDENTIAL),
        (TENANT, OTHER_ACCOUNT, CREDENTIAL),
        (TENANT, ACCOUNT, OTHER_CREDENTIAL),
    ]

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

    await subject.repository.revoke_ingest_credential(
        TENANT, ACCOUNT, CREDENTIAL, expected_version=1, revoked_at=RECEIVED, audit=AUDIT
    )
    with pytest.raises(NotFoundError):  # already revoked: the `#status = :active` condition
        await subject.repository.revoke_ingest_credential(
            TENANT, ACCOUNT, CREDENTIAL, expected_version=2, revoked_at=RECEIVED, audit=AUDIT
        )


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
    credential_pending_account_may_issue,
    credential_revoke_frees_the_slot,
    credential_revoke_stale_version_is_conflict,
    credential_revoke_never_reveals_foreign_rows,
    credential_writes_one_audit_item_per_change,
)


# Connector authentication -------------------------------------------------------------------


class AuthenticatorKitSubject(Protocol):
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


class ResolverKitSubject(Protocol):
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
    await subject.seed_device_integration(
        device_integration(
            tenant_id=OTHER_TENANT,
            integration_account_id=OTHER_ACCOUNT,
            external_identity_hash=foreign_hash,
        )
    )

    assert await subject.resolver.resolve_external_identity(PROVIDER, IDENTITY_HASH) is None


EXTERNAL_IDENTITY_RESOLVER_CONTRACT = (resolve_known_identity, resolve_is_account_scoped)


# Integration lifecycle -----------------------------------------------------------------------


class LifecycleKitSubject(Protocol):
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
    with pytest.raises(ConflictError):  # stale version
        await subject.repository.transition_integration_account(
            TENANT,
            ACCOUNT,
            IntegrationAccountStatus.REVOKED,
            expected_version=9,
            updated_at=RECEIVED,
            audit=AUDIT,
        )
    for tenant_id, account_id in ((OTHER_TENANT, ACCOUNT), (TENANT, OTHER_ACCOUNT)):
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


class DeviceReaderKitSubject(Protocol):
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


class TrustedMappingKitSubject(Protocol):
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
    components: Sequence[DeviceComponent] = (),
) -> None:
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


class ReceiptKitSubject(Protocol):
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
    receipt_mark_enqueued_is_conditioned,
)


# Ingress queue -------------------------------------------------------------------------------


class QueueKitSubject(Protocol):
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
            source_metadata={"firmware_version": "\U0001f600" * 64},
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


async def queue_rejects_oversized_envelopes(subject: QueueKitSubject) -> None:
    with pytest.raises(IngressMessageTooLarge):
        await subject.producer.send(oversized_envelope())

    assert await _receive(subject) == []


async def queue_send_raw_keeps_the_body(subject: QueueKitSubject) -> None:
    body = '{"schema":"unknown@9","note":"redriven as-is"}'

    message_id = await subject.producer.send_raw(body)
    (received,) = await _receive(subject)

    assert (received.message_id, received.body) == (message_id, body)


INGRESS_QUEUE_CONTRACT = (
    queue_send_and_receive,
    queue_visibility_and_delete,
    queue_rejects_oversized_envelopes,
    queue_send_raw_keeps_the_body,
)


# Quarantine ----------------------------------------------------------------------------------


class QuarantineKitSubject(Protocol):
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
        assert page.next_after_sk == page.entries[-1].entry_key
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
            key[:-1] + "0",
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


class IngestStateKitSubject(Protocol):
    repository: Any  # IngestStateRepository


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
    await subject.repository.apply_ingest_marks(older)
    await subject.repository.apply_ingest_marks(newer)

    state = (await subject.repository.get_component_states(TENANT, [COMPONENT]))[COMPONENT]
    assert state.last_received_at == RECEIVED + timedelta(minutes=1)
    assert state.last_ingested_at == RECEIVED + timedelta(minutes=1, seconds=1)
    assert (state.last_observed_at, state.last_observed_sequence) == (
        OBSERVED + timedelta(minutes=1),
        11,
    )
    health = await subject.repository.get_connector_health(TENANT, ACCOUNT)
    assert health is not None
    assert health.last_received_at == RECEIVED + timedelta(minutes=1)


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


INGEST_STATE_CONTRACT = (
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


class EntitlementKitSubject(Protocol):
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


TELEMETRY_METRICS_CONTRACT = (metrics_records_one_object_per_call,)


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


DYNAMO_EXPRESSION_VECTORS: tuple[DynamoVector, ...] = (
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


def run_dynamo_vector(client: Any, table_name: str, vector: DynamoVector) -> None:
    for item in vector.seed:
        client.put_item(TableName=table_name, Item=_encode(item))
    if vector.operation == "query":
        response = client.query(**_wire(vector.request, table_name))
        assert tuple(_decode(item)["SK"] for item in response["Items"]) == vector.query_sks
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
