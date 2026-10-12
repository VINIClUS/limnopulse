import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from limnopulse_api.adapters.dynamodb_v2 import (
    canonical_request_hash,
    external_identity_hash,
    idempotency_key_hash,
)
from limnopulse_api.api.v2.schemas.integrations import (
    DeviceIntegrationCreate,
    IntegrationAccountCreate,
)
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.devices_v2 import DeviceStatus, DeviceV2
from limnopulse_api.domain.integrations import (
    DeviceIntegration,
    IntegrationAccount,
    IntegrationAccountStatus,
)
from limnopulse_api.domain.v2_common import IdempotencyReceipt
from limnopulse_api.services.integrations import (
    DEVICE_INTEGRATION_CREATE_SCOPE,
    INTEGRATION_ACCOUNT_CREATE_SCOPE,
    IntegrationInputError,
    IntegrationService,
)

NOW = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)
TENANT = "tnt_a"
OTHER_TENANT = "tnt_b"
ACCOUNT_ID = "ia_" + "1" * 32
INTEGRATION_ID = "di_" + "2" * 32
NEW_ACCOUNT_ID = "ia_" + "6" * 32
DEVICE_ID = "dev_" + "3" * 32
RAW_IDENTITY = "serial-raw-SN-0042"
IDEMPOTENCY_KEY = "device-integration-01"


class FakeDeviceReader:
    def __init__(self, *devices: DeviceV2) -> None:
        self.devices = {(device.tenant_id, device.device_id): device for device in devices}
        self.calls: list[tuple[str, str]] = []

    async def get_device(self, tenant_id: str, device_id: str) -> DeviceV2 | None:
        self.calls.append((tenant_id, device_id))
        return self.devices.get((tenant_id, device_id))


class FakeIntegrationRepository:
    def __init__(self, *accounts: IntegrationAccount) -> None:
        self.accounts = {(item.tenant_id, item.integration_account_id): item for item in accounts}
        self.integrations: list[DeviceIntegration] = []
        self.receipts: dict[tuple[str, str, str], IdempotencyReceipt] = {}
        self.claimed: set[tuple[str, str]] = set()
        self.created: list[tuple[object, IdempotencyReceipt]] = []
        self.replay_calls = 0
        self.commit_receipt_before_conflict: IdempotencyReceipt | None = None

    async def get_idempotency_replay(self, tenant_id, scope, key_hash, request_hash):
        self.replay_calls += 1
        receipt = self.receipts.get((tenant_id, scope, key_hash))
        if receipt is None:
            return None
        if receipt.request_hash != request_hash:
            raise ConflictError("idempotency key reused with a different request")
        return receipt.response_snapshot_json

    async def get_integration_account(self, tenant_id, integration_account_id):
        return self.accounts.get((tenant_id, integration_account_id))

    async def list_integration_accounts(self, tenant_id):
        return [item for (tenant, _), item in self.accounts.items() if tenant == tenant_id]

    def _store_receipt(self, receipt: IdempotencyReceipt) -> None:
        key = (receipt.tenant_id, receipt.scope, receipt.key_hash)
        if key in self.receipts:
            raise ConflictError("idempotency key already used")
        self.receipts[key] = receipt

    async def create_integration_account(self, account, receipt):
        self._store_receipt(receipt)
        self.accounts[(account.tenant_id, account.integration_account_id)] = account
        self.created.append((account, receipt))
        return account

    async def list_device_integrations(self, tenant_id, device_id):
        return [
            item
            for item in self.integrations
            if item.tenant_id == tenant_id and item.device_id == device_id
        ]

    async def create_device_integration(self, integration, receipt):
        if self.commit_receipt_before_conflict is not None:
            self._store_receipt(self.commit_receipt_before_conflict)
            self.commit_receipt_before_conflict = None
        lookup = (integration.provider_key, integration.external_identity_hash)
        if lookup in self.claimed:
            raise ConflictError("external identity already claimed")
        self._store_receipt(receipt)
        self.claimed.add(lookup)
        self.integrations.append(integration)
        self.created.append((integration, receipt))
        return integration


def device(**overrides) -> DeviceV2:
    values = {
        "tenant_id": TENANT,
        "device_id": DEVICE_ID,
        "name": "Probe hub",
        "created_at": NOW,
        "updated_at": NOW,
        "version": 1,
    }
    values.update(overrides)
    return DeviceV2(**values)


def account(**overrides) -> IntegrationAccount:
    values = {
        "tenant_id": TENANT,
        "integration_account_id": ACCOUNT_ID,
        "provider_key": "vendor.cloud",
        "display_name": "Vendor",
        "created_at": NOW,
        "updated_at": NOW,
        "version": 1,
    }
    values.update(overrides)
    return IntegrationAccount(**values)


def service(repository, reader=None, *, ids=None) -> IntegrationService:
    di_ids = iter(ids or [INTEGRATION_ID, "di_" + "7" * 32, "di_" + "8" * 32])
    return IntegrationService(
        repository,
        reader if reader is not None else FakeDeviceReader(device()),
        clock=lambda: NOW,
        integration_account_id_factory=lambda: NEW_ACCOUNT_ID,
        device_integration_id_factory=lambda: next(di_ids),
    )


def integration_payload(**overrides) -> DeviceIntegrationCreate:
    values = {"integration_account_id": ACCOUNT_ID, "external_identity": RAW_IDENTITY}
    values.update(overrides)
    return DeviceIntegrationCreate(**values)


# Schema boundaries ----------------------------------------------------------------------------


def test_provider_key_is_namespaced_and_not_an_aws_enum() -> None:
    assert (
        IntegrationAccountCreate(provider_key="vendor.cloud", display_name="Vendor").provider_key
        == "vendor.cloud"
    )
    assert IntegrationAccountCreate(provider_key="acme", display_name="A").provider_key == "acme"
    assert (
        IntegrationAccountCreate(provider_key="aws.iot_core", display_name="A").provider_key
        == "aws.iot_core"
    )
    for invalid in ("AWS IoT", "AWS_IOT", "vendor..cloud", ".vendor", "1vendor", "vendor#x", ""):
        with pytest.raises(ValidationError):
            IntegrationAccountCreate(provider_key=invalid, display_name="Vendor")


@pytest.mark.parametrize("field", ["api_key", "secret", "password", "token", "provider_secret"])
def test_api_rejects_secret_material(field: str) -> None:
    with pytest.raises(ValidationError):
        IntegrationAccountCreate(
            provider_key="vendor.cloud", display_name="Vendor", **{field: "plaintext"}
        )


def test_account_display_name_and_reference_bounds() -> None:
    with pytest.raises(ValidationError):
        IntegrationAccountCreate(provider_key="vendor.cloud", display_name="")
    with pytest.raises(ValidationError):
        IntegrationAccountCreate(provider_key="vendor.cloud", display_name="x" * 121)
    with pytest.raises(ValidationError):
        IntegrationAccountCreate(
            provider_key="vendor.cloud", display_name="V", credential_reference="x" * 257
        )
    created = IntegrationAccountCreate(
        provider_key="vendor.cloud",
        display_name="x" * 120,
        credential_reference="secretsmanager:vendor/tnt_a",
        public_identifier="org-42",
    )
    assert created.credential_reference == "secretsmanager:vendor/tnt_a"


def test_device_integration_body_cannot_choose_provider_or_status() -> None:
    for field in ("provider_key", "status", "tenant_id", "external_identity_hash"):
        with pytest.raises(ValidationError):
            integration_payload(**{field: "vendor.cloud"})


@pytest.mark.parametrize("value", ["ia_1", "ia_" + "G" * 32, "di_" + "1" * 32, "local-account"])
def test_device_integration_requires_canonical_account_id(value: str) -> None:
    with pytest.raises(ValidationError):
        integration_payload(integration_account_id=value)


def test_device_integration_requires_non_empty_identity() -> None:
    with pytest.raises(ValidationError):
        integration_payload(external_identity="")


@pytest.mark.parametrize(
    "identity",
    ["X ", " X", "X\n", "\tX", "X\r", "a\x00b", "a\x1fb", "a\x7fb", " ", "\u00a0X"],
)
def test_external_identity_rejects_padding_and_control_characters(identity: str) -> None:
    with pytest.raises(ValidationError):
        integration_payload(external_identity=identity)


@pytest.mark.parametrize("identity", ["X", "serial number 42", "vendor/SN:42-é", "x" * 512])
def test_external_identity_accepts_inner_spaces_and_unicode(identity: str) -> None:
    assert integration_payload(external_identity=identity).external_identity == identity


@pytest.mark.asyncio
async def test_identity_hashing_failure_is_input_error_without_raw_value() -> None:
    repository = FakeIntegrationRepository(account())
    payload = DeviceIntegrationCreate.model_construct(
        integration_account_id=ACCOUNT_ID, external_identity=""
    )
    with pytest.raises(IntegrationInputError, match="invalid external identity"):
        await service(repository).create_device_integration(
            TENANT, DEVICE_ID, payload, IDEMPOTENCY_KEY
        )
    assert repository.created == []


@pytest.mark.asyncio
async def test_corrupt_parent_data_is_not_an_input_error() -> None:
    class CorruptRepository(FakeIntegrationRepository):
        async def get_integration_account(self, tenant_id, integration_account_id):
            return IntegrationAccount(**{**account().model_dump(), "status": "corrupted"})

    with pytest.raises(ValidationError) as caught:
        await service(CorruptRepository()).create_device_integration(
            TENANT, DEVICE_ID, integration_payload(), IDEMPOTENCY_KEY
        )
    assert not isinstance(caught.value, IntegrationInputError)


# IntegrationAccount ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_integration_account_starts_pending_with_receipt() -> None:
    repository = FakeIntegrationRepository()
    payload = IntegrationAccountCreate(provider_key="vendor.cloud", display_name="Vendor")

    result = await service(repository).create_integration_account(
        TENANT, payload, "integration-account-01"
    )

    created, receipt = repository.created[0]
    assert created.status is IntegrationAccountStatus.PENDING
    assert created.integration_account_id == NEW_ACCOUNT_ID
    assert created.tenant_id == TENANT
    assert receipt.scope == INTEGRATION_ACCOUNT_CREATE_SCOPE
    assert receipt.key_hash == idempotency_key_hash("integration-account-01")
    assert receipt.request_hash == canonical_request_hash(
        {
            "scope": INTEGRATION_ACCOUNT_CREATE_SCOPE,
            "path": {"tenant_id": TENANT},
            "body": payload.model_dump(mode="json"),
        }
    )
    assert receipt.response_snapshot_json == result.snapshot_json
    assert result.replayed is False
    body = json.loads(result.snapshot_json)
    assert body["status"] == "pending"
    assert body["integration_account_id"] == NEW_ACCOUNT_ID
    assert body["created_at"] == "2026-08-25T12:00:00.000000Z"


@pytest.mark.asyncio
async def test_create_integration_account_replays_and_rejects_changed_body() -> None:
    repository = FakeIntegrationRepository()
    payload = IntegrationAccountCreate(provider_key="vendor.cloud", display_name="Vendor")
    first = await service(repository).create_integration_account(TENANT, payload, "key-00001")

    replay = await service(repository).create_integration_account(TENANT, payload, "key-00001")
    assert replay.snapshot_json == first.snapshot_json and replay.replayed is True
    assert len(repository.created) == 1

    changed = IntegrationAccountCreate(provider_key="vendor.cloud", display_name="Other")
    with pytest.raises(ConflictError):
        await service(repository).create_integration_account(TENANT, changed, "key-00001")


# DeviceIntegration ----------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status", [IntegrationAccountStatus.PENDING, IntegrationAccountStatus.ACTIVE]
)
async def test_pending_or_active_account_permits_provisioning(status) -> None:
    repository = FakeIntegrationRepository(account(status=status))

    result = await service(repository).create_device_integration(
        TENANT, DEVICE_ID, integration_payload(), IDEMPOTENCY_KEY
    )

    created, receipt = repository.created[0]
    assert created.status.value == "provisioning"
    assert created.device_integration_id == INTEGRATION_ID
    assert created.provider_key == "vendor.cloud"
    assert created.external_identity_hash == external_identity_hash("vendor.cloud", RAW_IDENTITY)
    assert receipt.scope == DEVICE_INTEGRATION_CREATE_SCOPE
    assert json.loads(result.snapshot_json)["external_identity_hash"] == (
        created.external_identity_hash
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status", [IntegrationAccountStatus.ERROR, IntegrationAccountStatus.REVOKED]
)
async def test_error_or_revoked_account_rejects_new_integration(status) -> None:
    repository = FakeIntegrationRepository(account(status=status))
    with pytest.raises(ConflictError, match=status.value):
        await service(repository).create_device_integration(
            TENANT, DEVICE_ID, integration_payload(), IDEMPOTENCY_KEY
        )
    assert repository.created == []


@pytest.mark.asyncio
async def test_provider_key_is_derived_from_account() -> None:
    repository = FakeIntegrationRepository(account(provider_key="acme.mqtt"))
    await service(repository).create_device_integration(
        TENANT, DEVICE_ID, integration_payload(), IDEMPOTENCY_KEY
    )
    created, _ = repository.created[0]
    assert created.provider_key == "acme.mqtt"
    assert created.external_identity_hash == external_identity_hash("acme.mqtt", RAW_IDENTITY)


@pytest.mark.asyncio
async def test_missing_or_cross_tenant_device_is_not_found() -> None:
    repository = FakeIntegrationRepository(account())
    with pytest.raises(NotFoundError):
        await service(repository, FakeDeviceReader()).create_device_integration(
            TENANT, DEVICE_ID, integration_payload(), IDEMPOTENCY_KEY
        )
    other_tenant_reader = FakeDeviceReader(device(tenant_id=OTHER_TENANT))
    with pytest.raises(NotFoundError):
        await service(repository, other_tenant_reader).create_device_integration(
            TENANT, DEVICE_ID, integration_payload(), IDEMPOTENCY_KEY
        )
    with pytest.raises(NotFoundError):
        await service(repository, other_tenant_reader).list_device_integrations(TENANT, DEVICE_ID)
    assert repository.created == []


@pytest.mark.asyncio
async def test_device_reader_returning_foreign_tenant_device_is_not_found() -> None:
    class LeakyReader:
        async def get_device(self, tenant_id, device_id):
            return device(tenant_id=OTHER_TENANT)

    repository = FakeIntegrationRepository(account())
    with pytest.raises(NotFoundError):
        await service(repository, LeakyReader()).create_device_integration(
            TENANT, DEVICE_ID, integration_payload(), IDEMPOTENCY_KEY
        )


@pytest.mark.asyncio
async def test_decommissioned_device_rejects_new_integration() -> None:
    repository = FakeIntegrationRepository(account())
    reader = FakeDeviceReader(device(status=DeviceStatus.DECOMMISSIONED))
    with pytest.raises(ConflictError):
        await service(repository, reader).create_device_integration(
            TENANT, DEVICE_ID, integration_payload(), IDEMPOTENCY_KEY
        )


@pytest.mark.asyncio
async def test_missing_or_cross_tenant_account_is_not_found() -> None:
    repository = FakeIntegrationRepository(account(tenant_id=OTHER_TENANT))
    with pytest.raises(NotFoundError):
        await service(repository).create_device_integration(
            TENANT, DEVICE_ID, integration_payload(), IDEMPOTENCY_KEY
        )


@pytest.mark.asyncio
async def test_historical_device_id_is_accepted() -> None:
    repository = FakeIntegrationRepository(account())
    reader = FakeDeviceReader(device(device_id="local-device-001"))
    await service(repository, reader).create_device_integration(
        TENANT, "local-device-001", integration_payload(), IDEMPOTENCY_KEY
    )
    assert repository.created[0][0].device_id == "local-device-001"


@pytest.mark.asyncio
async def test_raw_identity_never_reaches_persistence_or_snapshot() -> None:
    repository = FakeIntegrationRepository(account())
    result = await service(repository).create_device_integration(
        TENANT, DEVICE_ID, integration_payload(), IDEMPOTENCY_KEY
    )
    integration, receipt = repository.created[0]
    assert RAW_IDENTITY not in integration.model_dump_json()
    assert RAW_IDENTITY not in receipt.model_dump_json()
    assert RAW_IDENTITY not in result.snapshot_json
    assert "external_identity" not in json.loads(result.snapshot_json)


@pytest.mark.asyncio
async def test_same_identity_cannot_be_claimed_twice_even_across_tenants() -> None:
    repository = FakeIntegrationRepository(
        account(), account(tenant_id=OTHER_TENANT, integration_account_id="ia_" + "9" * 32)
    )
    reader = FakeDeviceReader(device(), device(tenant_id=OTHER_TENANT))
    await service(repository, reader).create_device_integration(
        TENANT, DEVICE_ID, integration_payload(), IDEMPOTENCY_KEY
    )

    with pytest.raises(ConflictError, match="external identity already claimed") as caught:
        await service(repository, reader).create_device_integration(
            OTHER_TENANT,
            DEVICE_ID,
            integration_payload(integration_account_id="ia_" + "9" * 32),
            "other-tenant-key",
        )
    assert RAW_IDENTITY not in str(caught.value)
    with pytest.raises(ConflictError, match="external identity already claimed"):
        await service(repository, reader).create_device_integration(
            TENANT, DEVICE_ID, integration_payload(), "second-key-0001"
        )


@pytest.mark.asyncio
async def test_device_integration_replay_happens_before_parent_reads() -> None:
    repository = FakeIntegrationRepository(account())
    reader = FakeDeviceReader(device())
    first = await service(repository, reader).create_device_integration(
        TENANT, DEVICE_ID, integration_payload(), IDEMPOTENCY_KEY
    )
    reader.devices.clear()  # A replay must not depend on the parents still validating.

    replay = await service(repository, reader).create_device_integration(
        TENANT, DEVICE_ID, integration_payload(), IDEMPOTENCY_KEY
    )

    assert replay.snapshot_json == first.snapshot_json
    assert replay.replayed is True
    assert len(reader.calls) == 1


@pytest.mark.asyncio
async def test_device_integration_same_key_different_identity_conflicts() -> None:
    repository = FakeIntegrationRepository(account())
    await service(repository).create_device_integration(
        TENANT, DEVICE_ID, integration_payload(), IDEMPOTENCY_KEY
    )
    with pytest.raises(ConflictError, match="idempotency key reused"):
        await service(repository).create_device_integration(
            TENANT,
            DEVICE_ID,
            integration_payload(external_identity="other-serial"),
            IDEMPOTENCY_KEY,
        )


@pytest.mark.asyncio
async def test_concurrent_same_key_race_replays_winning_snapshot() -> None:
    repository = FakeIntegrationRepository(account())
    winner = await service(repository).create_device_integration(
        TENANT, DEVICE_ID, integration_payload(), IDEMPOTENCY_KEY
    )
    winning_receipt = repository.created[0][1]
    # Simulate the loser having passed its replay check before the winner committed.
    racing = FakeIntegrationRepository(account())
    racing.claimed = set(repository.claimed)
    racing.commit_receipt_before_conflict = winning_receipt

    result = await service(racing, ids=["di_" + "a" * 32]).create_device_integration(
        TENANT, DEVICE_ID, integration_payload(), IDEMPOTENCY_KEY
    )

    assert result.snapshot_json == winner.snapshot_json
    assert result.replayed is True
    assert racing.replay_calls == 2


@pytest.mark.asyncio
async def test_list_device_integrations_requires_tenant_device() -> None:
    repository = FakeIntegrationRepository(account())
    await service(repository).create_device_integration(
        TENANT, DEVICE_ID, integration_payload(), IDEMPOTENCY_KEY
    )
    listed = await service(repository).list_device_integrations(TENANT, DEVICE_ID)
    assert [item.device_integration_id for item in listed] == [INTEGRATION_ID]
