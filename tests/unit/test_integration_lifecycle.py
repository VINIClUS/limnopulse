"""P3-04: IntegrationAccount / DeviceIntegration lifecycle transitions with D23 audit items.

The lifecycle kit runs against `DynamoIntegrationRepository` on the in-memory Dynamo fake; the
same kit runs on DynamoDB Local in `tests/ingestion/test_integration_lifecycle_dynamodb.py`.
"""

from collections.abc import Callable
from datetime import timedelta
from typing import Any

import pytest

from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.integrations import (
    DeviceIntegrationStatus,
    IntegrationAccountStatus,
    ensure_device_integration_transition,
    ensure_integration_account_transition,
)
from limnopulse_api.repositories import ingestion as ports
from limnopulse_api.services.integration_lifecycle import IntegrationLifecycleService
from tests.support.ingestion_contract_kits import (
    ACCOUNT,
    AUDIT,
    DEVICE,
    IDENTITY_HASH,
    INTEGRATION,
    INTEGRATION_LIFECYCLE_CONTRACT,
    OTHER_DEVICE,
    OTHER_TENANT,
    PROVIDER,
    RECEIVED,
    SECRET,
    TENANT,
    device_integration,
    integration_account,
    kit_ids,
)
from tests.unit.test_dynamodb_integration_resolver import (
    AUDIT_TABLE,
    DOMAIN_TABLE,
    WRITE_OPERATIONS,
    DynamoIngestionSubject,
    fake_subject,
)

LATER = RECEIVED + timedelta(minutes=5)


def _allowed(check: Callable[[Any, Any], None], current: Any, target: Any) -> bool:
    try:
        check(current, target)
    except ConflictError:
        return False
    return True


ACCOUNT_MOVES = [
    (current, target, _allowed(ensure_integration_account_transition, current, target))
    for current in IntegrationAccountStatus
    for target in IntegrationAccountStatus
    if current != target
]
INTEGRATION_MOVES = [
    (current, target, _allowed(ensure_device_integration_transition, current, target))
    for current in DeviceIntegrationStatus
    for target in DeviceIntegrationStatus
    if current != target
]


def _account_key(subject: DynamoIngestionSubject, tenant_id: str = TENANT) -> dict[str, str]:
    return subject.integrations.keys.integration_account(tenant_id, ACCOUNT)


def _integration_key(subject: DynamoIngestionSubject) -> dict[str, str]:
    return subject.integrations.keys.device_integration(TENANT, INTEGRATION)


def _lookup_key(subject: DynamoIngestionSubject) -> dict[str, str]:
    return subject.integrations.keys.external_identity(PROVIDER, IDENTITY_HASH)


async def _transition_account(
    subject: DynamoIngestionSubject, target: IntegrationAccountStatus, *, version: int = 1
) -> Any:
    return await subject.repository.transition_integration_account(
        TENANT, ACCOUNT, target, expected_version=version, updated_at=LATER, audit=AUDIT
    )


async def _transition_integration(
    subject: DynamoIngestionSubject, target: DeviceIntegrationStatus, *, version: int = 1
) -> Any:
    return await subject.repository.transition_device_integration(
        TENANT, INTEGRATION, target, expected_version=version, updated_at=LATER, audit=AUDIT
    )


# Contract kit ------------------------------------------------------------------------------


def test_adapter_implements_the_lifecycle_port() -> None:
    assert isinstance(fake_subject().repository, ports.IntegrationLifecycleRepository)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario", INTEGRATION_LIFECYCLE_CONTRACT, ids=kit_ids(INTEGRATION_LIFECYCLE_CONTRACT)
)
async def test_lifecycle_adapter_passes_the_p3_00_kit(scenario: Callable[[Any], Any]) -> None:
    await scenario(fake_subject())


# Transition matrix -------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("current", "target", "allowed"),
    ACCOUNT_MOVES,
    ids=[f"{c.value}-to-{t.value}" for c, t, _ in ACCOUNT_MOVES],
)
async def test_account_transition_matrix(
    current: IntegrationAccountStatus, target: IntegrationAccountStatus, allowed: bool
) -> None:
    subject = fake_subject()
    await subject.seed_account(integration_account(status=current))

    if allowed:
        updated = await _transition_account(subject, target)
        assert updated == integration_account(status=target, version=2, updated_at=LATER)
        assert subject.item(_account_key(subject))["status"] == target.value
        assert len(subject.audit_items()) == 1
    else:
        with pytest.raises(ConflictError) as caught:
            await _transition_account(subject, target)
        assert not isinstance(caught.value, NotFoundError)
        assert subject.item(_account_key(subject))["status"] == current.value
        assert subject.item(_account_key(subject))["version"] == 1
        assert subject.audit_items() == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("current", "target", "allowed"),
    INTEGRATION_MOVES,
    ids=[f"{c.value}-to-{t.value}" for c, t, _ in INTEGRATION_MOVES],
)
async def test_device_integration_transition_matrix(
    current: DeviceIntegrationStatus, target: DeviceIntegrationStatus, allowed: bool
) -> None:
    subject = fake_subject()
    await subject.seed_device_integration(device_integration(status=current))

    if allowed:
        updated = await _transition_integration(subject, target)
        assert updated == device_integration(status=target, version=2, updated_at=LATER)
        assert subject.item(_integration_key(subject))["status"] == target.value
        assert len(subject.audit_items()) == 1
    else:
        with pytest.raises(ConflictError) as caught:
            await _transition_integration(subject, target)
        assert not isinstance(caught.value, NotFoundError)
        assert subject.item(_integration_key(subject))["status"] == current.value
        assert subject.item(_lookup_key(subject))["status"] == current.value
        assert subject.audit_items() == []


def test_the_matrix_covers_legal_and_illegal_moves() -> None:
    for moves in (ACCOUNT_MOVES, INTEGRATION_MOVES):
        assert {allowed for _, _, allowed in moves} == {True, False}


# Versions ----------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stale_account_version_is_a_conflict_and_changes_nothing() -> None:
    subject = fake_subject()
    await subject.seed_account(integration_account(version=3))
    before = subject.item(_account_key(subject))

    for stale in (2, 4):
        with pytest.raises(ConflictError) as caught:
            await _transition_account(subject, IntegrationAccountStatus.ERROR, version=stale)
        assert not isinstance(caught.value, NotFoundError)

    assert subject.item(_account_key(subject)) == before
    assert subject.audit_items() == []


@pytest.mark.asyncio
async def test_stale_device_integration_version_is_a_conflict_and_changes_nothing() -> None:
    subject = fake_subject()
    await subject.seed_device_integration(device_integration(version=3))
    before = (subject.item(_integration_key(subject)), subject.item(_lookup_key(subject)))

    for stale in (2, 4):
        with pytest.raises(ConflictError) as caught:
            await _transition_integration(subject, DeviceIntegrationStatus.ERROR, version=stale)
        assert not isinstance(caught.value, NotFoundError)

    assert (subject.item(_integration_key(subject)), subject.item(_lookup_key(subject))) == before
    assert subject.audit_items() == []


@pytest.mark.asyncio
async def test_consecutive_transitions_bump_the_version() -> None:
    subject = fake_subject()
    await subject.seed_account(integration_account(status=IntegrationAccountStatus.PENDING))

    await _transition_account(subject, IntegrationAccountStatus.ACTIVE, version=1)
    updated = await _transition_account(subject, IntegrationAccountStatus.ERROR, version=2)

    assert updated.version == 3
    assert subject.item(_account_key(subject))["version"] == 3
    assert sorted(item["after_status"] for item in subject.audit_items()) == ["active", "error"]


@pytest.mark.asyncio
async def test_missing_rows_are_not_found() -> None:
    subject = fake_subject()

    with pytest.raises(NotFoundError):
        await _transition_account(subject, IntegrationAccountStatus.ERROR)
    with pytest.raises(NotFoundError):
        await _transition_integration(subject, DeviceIntegrationStatus.ERROR)
    assert subject.audit_items() == []


@pytest.mark.asyncio
async def test_another_tenants_device_integration_is_not_found() -> None:
    subject = fake_subject()
    await subject.seed_device_integration(device_integration(tenant_id=OTHER_TENANT))

    with pytest.raises(NotFoundError):
        await _transition_integration(subject, DeviceIntegrationStatus.ERROR)


# Atomicity and partial updates -------------------------------------------------------------


@pytest.mark.asyncio
async def test_device_integration_transition_updates_the_lookup_row_in_one_transaction() -> None:
    subject = fake_subject()
    await subject.seed_device_integration(device_integration())

    await _transition_integration(subject, DeviceIntegrationStatus.DECOMMISSIONED)

    (transaction,) = [call for call in subject.client.calls if call.operation in WRITE_OPERATIONS]
    assert transaction.operation == "transact_write_items"
    domain_keys = {
        key
        for key, (action,) in zip(
            transaction.keys,
            (tuple(action.values()) for action in transaction.request["TransactItems"]),
            strict=True,
        )
        if action["TableName"] == DOMAIN_TABLE
    }
    assert domain_keys == {
        (_integration_key(subject)["PK"], _integration_key(subject)["SK"]),
        (_lookup_key(subject)["PK"], _lookup_key(subject)["SK"]),
    }
    lookup = subject.item(_lookup_key(subject))
    assert (lookup["status"], lookup["version"]) == ("decommissioned", 2)
    # The identity stays claimed by the same DeviceIntegration (D22).
    assert (lookup["tenant_id"], lookup["device_integration_id"], lookup["device_id"]) == (
        TENANT,
        INTEGRATION,
        DEVICE,
    )
    row = subject.item(_integration_key(subject))
    assert (row["status"], row["version"]) == ("decommissioned", 2)


@pytest.mark.asyncio
async def test_account_transition_is_one_update_that_keeps_other_attributes() -> None:
    subject = fake_subject()
    account = integration_account(
        status=IntegrationAccountStatus.PENDING,
        credential_reference="secretsmanager:vendor/tenant-a",
        public_identifier="org-42",
    )
    await subject.seed_account(account, future_attribute="kept", nested={"a": 1})

    await _transition_account(subject, IntegrationAccountStatus.ACTIVE)

    item = subject.item(_account_key(subject))
    assert item["future_attribute"] == "kept"
    assert item["nested"] == {"a": 1}
    assert item["credential_reference"] == "secretsmanager:vendor/tenant-a"
    assert item["public_identifier"] == "org-42"
    assert item["display_name"] == account.display_name
    assert item["created_at"] == "2026-01-01T00:00:00.000000Z"
    (transaction,) = [call for call in subject.client.calls if call.operation in WRITE_OPERATIONS]
    domain_actions = [
        kind
        for action in transaction.request["TransactItems"]
        for kind, body in action.items()
        if body["TableName"] == DOMAIN_TABLE
    ]
    assert domain_actions == ["Update"]


@pytest.mark.asyncio
async def test_device_integration_transition_keeps_other_attributes() -> None:
    subject = fake_subject()
    await subject.seed_device_integration(device_integration(), future_attribute="kept")

    await _transition_integration(subject, DeviceIntegrationStatus.ERROR)

    item = subject.item(_integration_key(subject))
    assert item["future_attribute"] == "kept"
    assert item["external_identity_hash"] == IDENTITY_HASH
    assert item["integration_account_id"] == ACCOUNT


# D23 audit items ---------------------------------------------------------------------------


def _audit_puts(subject: DynamoIngestionSubject) -> list[dict[str, Any]]:
    (transaction,) = [call for call in subject.client.calls if call.operation in WRITE_OPERATIONS]
    return [
        body
        for action in transaction.request["TransactItems"]
        for kind, body in action.items()
        if kind == "Put" and body["TableName"] == AUDIT_TABLE
    ]


@pytest.mark.asyncio
async def test_account_transition_writes_one_audit_item_in_the_same_transaction() -> None:
    subject = fake_subject()
    await subject.seed_account(integration_account())

    await _transition_account(subject, IntegrationAccountStatus.REVOKED)

    (put,) = _audit_puts(subject)
    item = subject.integrations.deserialize_item(put["Item"])
    assert put["ConditionExpression"] == "attribute_not_exists(PK)"
    assert item["action"] == "integration_account.status_changed"
    assert item["resource_type"] == "integration_account"
    assert (item["tenant_id"], item["resource_id"]) == (TENANT, ACCOUNT)
    assert (item["before_status"], item["after_status"]) == ("active", "revoked")
    assert (item["actor_id"], item["ip"], item["user_agent"]) == (
        AUDIT.actor_id,
        AUDIT.ip,
        AUDIT.user_agent,
    )
    assert item["fingerprint"] is None
    assert item["PK"] == f"TENANT#{TENANT}#MONTH#{LATER:%Y-%m}"
    assert subject.audit_items() == [item]


@pytest.mark.asyncio
async def test_device_integration_transition_writes_one_audit_item_in_the_same_transaction() -> (
    None
):
    subject = fake_subject()
    await subject.seed_device_integration(device_integration(status=DeviceIntegrationStatus.ERROR))

    await _transition_integration(subject, DeviceIntegrationStatus.ACTIVE)

    (put,) = _audit_puts(subject)
    item = subject.integrations.deserialize_item(put["Item"])
    assert item["action"] == "device_integration.status_changed"
    assert item["resource_type"] == "device_integration"
    assert (item["tenant_id"], item["resource_id"]) == (TENANT, INTEGRATION)
    assert (item["before_status"], item["after_status"]) == ("error", "active")
    assert item["actor_id"] == AUDIT.actor_id
    serialized = str(put)
    assert IDENTITY_HASH not in serialized
    assert SECRET not in serialized


@pytest.mark.asyncio
async def test_audit_items_need_the_audit_table() -> None:
    subject = fake_subject()
    await subject.seed_account(integration_account())
    unaudited = type(subject.integrations)(DOMAIN_TABLE, subject.client)

    with pytest.raises((RuntimeError, ValueError)):
        await unaudited.transition_integration_account(
            TENANT,
            ACCOUNT,
            IntegrationAccountStatus.ERROR,
            expected_version=1,
            updated_at=LATER,
            audit=AUDIT,
        )

    assert subject.item(_account_key(subject))["status"] == "active"


# Service -----------------------------------------------------------------------------------


class RecordingIntegrations:
    """`list_device_integrations` over the subject's adapter, recording each call."""

    def __init__(self, subject: DynamoIngestionSubject) -> None:
        self.subject = subject
        self.calls: list[tuple[str, str]] = []

    async def list_device_integrations(self, tenant_id: str, device_id: str) -> list[Any]:
        self.calls.append((tenant_id, device_id))
        return await self.subject.integrations.list_device_integrations(tenant_id, device_id)


def _service(subject: DynamoIngestionSubject) -> IntegrationLifecycleService:
    return IntegrationLifecycleService(
        subject.repository, subject.integrations, clock=lambda: LATER
    )


@pytest.mark.asyncio
async def test_service_stamps_updated_at_from_its_clock() -> None:
    subject = fake_subject()
    await subject.seed_account(integration_account())
    await subject.seed_device_integration(device_integration())

    account = await _service(subject).transition_integration_account(
        TENANT, ACCOUNT, IntegrationAccountStatus.ERROR, expected_version=1, audit=AUDIT
    )
    integration = await _service(subject).transition_device_integration(
        TENANT,
        DEVICE,
        INTEGRATION,
        DeviceIntegrationStatus.ERROR,
        expected_version=1,
        audit=AUDIT,
    )

    assert (account.status, account.updated_at, account.version) == ("error", LATER, 2)
    assert (integration.status, integration.updated_at, integration.version) == ("error", LATER, 2)


@pytest.mark.asyncio
async def test_service_requires_the_path_device_to_own_the_integration() -> None:
    subject = fake_subject()
    await subject.seed_device_integration(device_integration())

    with pytest.raises(NotFoundError):
        await _service(subject).transition_device_integration(
            TENANT,
            OTHER_DEVICE,
            INTEGRATION,
            DeviceIntegrationStatus.DECOMMISSIONED,
            expected_version=1,
            audit=AUDIT,
        )

    assert subject.item(_integration_key(subject))["status"] == "active"
    assert subject.audit_items() == []
    assert not [call for call in subject.client.calls if call.operation == "transact_write_items"]


@pytest.mark.asyncio
async def test_service_checks_the_device_through_the_tenant_listing() -> None:
    subject = fake_subject()
    await subject.seed_device_integration(device_integration())
    integrations = RecordingIntegrations(subject)
    service = IntegrationLifecycleService(subject.repository, integrations, clock=lambda: LATER)

    await service.transition_device_integration(
        TENANT, DEVICE, INTEGRATION, DeviceIntegrationStatus.ERROR, expected_version=1, audit=AUDIT
    )

    assert integrations.calls == [(TENANT, DEVICE)]


@pytest.mark.asyncio
async def test_service_surfaces_repository_conflicts() -> None:
    subject = fake_subject()
    await subject.seed_account(integration_account(status=IntegrationAccountStatus.REVOKED))

    with pytest.raises(ConflictError):
        await _service(subject).transition_integration_account(
            TENANT, ACCOUNT, IntegrationAccountStatus.ACTIVE, expected_version=1, audit=AUDIT
        )
