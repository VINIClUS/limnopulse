"""P3-04: the DynamoDB external-identity resolver and the adapter-only consistent Device read.

`FaultingClient` and `DynamoIngestionSubject` are the P3-04 kit subjects over the real adapters.
They run on the in-memory Dynamo fake here and on DynamoDB Local in
`tests/ingestion/test_integration_lifecycle_dynamodb.py`; the other P3-04 test modules import
them from this module (`tests/support/` is P3-00 owned).
"""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pytest

from limnopulse_api.adapters.dynamodb_devices_v2 import (
    DynamoDeviceComponentRepository,
    component_to_item,
    device_v2_to_item,
)
from limnopulse_api.adapters.dynamodb_integrations import (
    DynamoIntegrationRepository,
    device_integration_to_item,
    external_identity_lookup_to_item,
    integration_account_to_item,
)
from limnopulse_api.adapters.dynamodb_v2 import external_identity_hash
from limnopulse_api.domain.devices_v2 import DeviceComponent, DeviceV2
from limnopulse_api.domain.integrations import (
    DeviceIntegration,
    DeviceIntegrationStatus,
    IntegrationAccount,
)
from limnopulse_api.repositories import ingestion as ports
from tests.support.ingestion_contract_kits import (
    CONSISTENT_DEVICE_READER_CONTRACT,
    DEVICE,
    EXTERNAL_DEVICE,
    EXTERNAL_IDENTITY_RESOLVER_CONTRACT,
    IDENTITY_HASH,
    INTEGRATION,
    OTHER_DEVICE,
    OTHER_INTEGRATION,
    OTHER_TENANT,
    PROVIDER,
    TENANT,
    device,
    device_integration,
    kit_ids,
    store_errors,
)
from tests.support.ingestion_dynamo_fake import IngestionDynamoClient

DOMAIN_TABLE = "LimnopulseDomain"
AUDIT_TABLE = "LimnopulseAudit"
AUDIT_ENTITY_TYPE = "audit_event"

# Fault point (port method or `STORE_READS` name) -> (boto3 operation, PK prefix, SK prefix).
# A port-level fault ("admit", "fence", a transition) hits the first store call of that kind.
FAULT_POINTS: dict[str, tuple[str, str | None, str | None]] = {
    "resolve_external_identity": ("get_item", "EXTERNAL_ID#", None),
    "get_integration_account": ("get_item", "TENANT#", "INTEGRATION_ACCOUNT#"),
    "get_device": ("get_item", "TENANT#", "DEVICE#"),
    "get_component": ("get_item", "COMPONENT#", None),
    "admit": ("get_item", None, None),
    "fence": ("get_item", None, None),
    "transition_integration_account": ("transact_write_items", None, None),
    "transition_device_integration": ("transact_write_items", None, None),
}

WRITE_OPERATIONS = frozenset({"put_item", "update_item", "delete_item", "transact_write_items"})
_OPERATIONS = frozenset(
    {"get_item", "query", "put_item", "update_item", "delete_item", "transact_write_items"}
)


@dataclass(frozen=True)
class StoreCall:
    operation: str
    keys: tuple[tuple[str | None, str | None], ...]
    consistent: bool
    request: Mapping[str, Any]


def _plain_key(raw: Mapping[str, Any] | None) -> tuple[str | None, str | None]:
    if raw is None:
        return (None, None)
    pk, sk = raw.get("PK"), raw.get("SK")
    return (None if pk is None else pk["S"], None if sk is None else sk["S"])


def _request_keys(operation: str, request: Mapping[str, Any]) -> tuple[tuple[str | None, ...], ...]:
    if operation == "transact_write_items":
        return tuple(
            _plain_key(body.get("Key") or body.get("Item"))
            for action in request["TransactItems"]
            for body in action.values()
        )
    if operation == "query":
        return ((request["ExpressionAttributeValues"][":pk"]["S"], None),)
    return (_plain_key(request.get("Key") or request.get("Item")),)


class FaultingClient:
    """A boto3-shaped DynamoDB client wrapper: records every call and raises an armed fault once
    from the next matching call, before the call reaches the store."""

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.calls: list[StoreCall] = []
        self._faults: list[tuple[str, str | None, str | None, BaseException]] = []
        self._before: dict[str, list[Callable[[], None]]] = {}

    def fail_next(
        self,
        operation: str,
        exc: BaseException,
        *,
        pk_prefix: str | None = None,
        sk_prefix: str | None = None,
    ) -> None:
        self._faults.append((operation, pk_prefix, sk_prefix, exc))

    def before_next(self, operation: str, action: Callable[[], None]) -> None:
        """Run `action` once just before the next `operation` reaches the store (for example a
        concurrent writer bumping a version between an adapter's read and its write)."""
        self._before.setdefault(operation, []).append(action)

    def _raise_armed(self, operation: str, keys: Sequence[tuple[str | None, str | None]]) -> None:
        for index, (wanted, pk_prefix, sk_prefix, exc) in enumerate(self._faults):
            if wanted != operation:
                continue
            for pk, sk in keys:
                if (pk_prefix is None or (pk or "").startswith(pk_prefix)) and (
                    sk_prefix is None or (sk or "").startswith(sk_prefix)
                ):
                    del self._faults[index]
                    raise exc

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self.inner, name)
        if name not in _OPERATIONS:
            return attribute

        def call(**request: Any) -> Any:
            keys = _request_keys(name, request)
            self.calls.append(
                StoreCall(name, keys, bool(request.get("ConsistentRead", False)), request)
            )
            self._raise_armed(name, keys)
            pending = self._before.get(name)
            if pending:
                pending.pop(0)()
            return attribute(**request)

        return call

    def reads(self, *, pk_prefix: str, sk_prefix: str | None = None) -> list[StoreCall]:
        return [
            call
            for call in self.calls
            if call.operation == "get_item"
            and (call.keys[0][0] or "").startswith(pk_prefix)
            and (sk_prefix is None or (call.keys[0][1] or "").startswith(sk_prefix))
        ]


class DynamoIngestionSubject:
    """Kit subject over the real P3-04 adapters (resolver, lifecycle, consistent Device read)."""

    def __init__(
        self,
        store: Any,
        *,
        domain_table: str = DOMAIN_TABLE,
        audit_table: str = AUDIT_TABLE,
        audit_reader: Callable[[], list[dict[str, Any]]] | None = None,
    ) -> None:
        self.store = store
        self.domain_table = domain_table
        self.audit_table = audit_table
        self.client = FaultingClient(store)
        self.integrations = DynamoIntegrationRepository(
            domain_table, self.client, audit_table_name=audit_table
        )
        self.devices = DynamoDeviceComponentRepository(domain_table, self.client)
        self.resolver = self.integrations
        self.repository = self.integrations
        self.reader = self.devices
        self._audit_reader = audit_reader or self._fake_audit_items

    # Seeding (straight into the store, never through the fault proxy) ---------------------

    def put(self, item: Mapping[str, Any]) -> None:
        self.store.put_item(
            TableName=self.domain_table, Item=self.integrations.serialize_item(item)
        )

    def item(self, key: Mapping[str, str]) -> dict[str, Any] | None:
        response = self.store.get_item(
            TableName=self.domain_table,
            Key=self.integrations.serialize_item(key),
            ConsistentRead=True,
        )
        raw = response.get("Item")
        return None if raw is None else self.integrations.deserialize_item(raw)

    async def seed_account(self, account: IntegrationAccount, **extra: Any) -> None:
        self.put({**integration_account_to_item(account, self.integrations.keys), **extra})

    async def seed_device_integration(self, integration: DeviceIntegration, **extra: Any) -> None:
        self.put({**device_integration_to_item(integration, self.integrations.keys), **extra})
        self.put(external_identity_lookup_to_item(integration, self.integrations.keys))

    async def seed_device(self, device: DeviceV2) -> None:
        self.put(device_v2_to_item(device))

    async def seed_component(self, component: DeviceComponent) -> None:
        self.put(component_to_item(component))

    # Kit hooks -------------------------------------------------------------------------------

    def fail_next(self, operation: str, exc: BaseException) -> None:
        boto_operation, pk_prefix, sk_prefix = FAULT_POINTS[operation]
        self.client.fail_next(boto_operation, exc, pk_prefix=pk_prefix, sk_prefix=sk_prefix)

    def audit_items(self) -> list[dict[str, Any]]:
        return self._audit_reader()

    def _fake_audit_items(self) -> list[dict[str, Any]]:
        return [
            dict(item)
            for item in self.store.items.values()
            if item.get("entity_type") == AUDIT_ENTITY_TYPE
        ]


def fake_subject() -> DynamoIngestionSubject:
    return DynamoIngestionSubject(IngestionDynamoClient())


# Contract kits -----------------------------------------------------------------------------


def test_adapters_implement_the_p3_ports() -> None:
    subject = fake_subject()

    assert isinstance(subject.resolver, ports.ExternalIdentityResolver)
    assert isinstance(subject.reader, ports.ConsistentDeviceReader)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario",
    EXTERNAL_IDENTITY_RESOLVER_CONTRACT,
    ids=kit_ids(EXTERNAL_IDENTITY_RESOLVER_CONTRACT),
)
async def test_resolver_passes_the_p3_00_kit(scenario: Callable[[Any], Any]) -> None:
    await scenario(fake_subject())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario",
    CONSISTENT_DEVICE_READER_CONTRACT,
    ids=kit_ids(CONSISTENT_DEVICE_READER_CONTRACT),
)
async def test_consistent_device_reader_passes_the_p3_00_kit(
    scenario: Callable[[Any], Any],
) -> None:
    await scenario(fake_subject())


# Resolver --------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_is_two_strongly_consistent_get_items() -> None:
    subject = fake_subject()
    await subject.seed_device_integration(device_integration())

    resolved = await subject.resolver.resolve_external_identity(PROVIDER, IDENTITY_HASH)

    assert resolved == device_integration()
    assert [call.operation for call in subject.client.calls] == ["get_item", "get_item"]
    assert [call.keys[0] for call in subject.client.calls] == [
        (f"EXTERNAL_ID#{PROVIDER}#{IDENTITY_HASH}", "META"),
        (f"TENANT#{TENANT}", f"DEVICE_INTEGRATION#{INTEGRATION}"),
    ]
    assert all(call.consistent for call in subject.client.calls)


@pytest.mark.asyncio
async def test_resolve_unknown_identity_reads_only_the_lookup_row() -> None:
    subject = fake_subject()

    assert await subject.resolver.resolve_external_identity(PROVIDER, IDENTITY_HASH) is None
    assert len(subject.client.calls) == 1


def _lookup(integration: DeviceIntegration, **overrides: Any) -> dict[str, Any]:
    keys = DynamoIntegrationRepository(DOMAIN_TABLE, None).keys
    return {**external_identity_lookup_to_item(integration, keys), **overrides}


INCONSISTENT_ROWS: tuple[tuple[str, dict[str, Any], dict[str, Any]], ...] = (
    # (case, lookup-row overrides, DI-row overrides)
    ("lookup names another tenant", {"tenant_id": OTHER_TENANT}, {}),  # row exists there too
    ("lookup names another device", {"device_id": OTHER_DEVICE}, {}),
    ("lookup names another integration", {"device_integration_id": OTHER_INTEGRATION}, {}),
    ("integration row carries another hash", {}, {"external_identity_hash": "e" * 64}),
    ("integration row carries another provider", {}, {"provider_key": "https_other"}),
    ("integration row names another device", {}, {"device_id": OTHER_DEVICE}),
    ("integration row carries another tenant", {}, {"tenant_id": OTHER_TENANT}),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("lookup_overrides", "row_overrides"),
    [case[1:] for case in INCONSISTENT_ROWS],
    ids=[case[0] for case in INCONSISTENT_ROWS],
)
async def test_resolve_rejects_inconsistent_rows(
    lookup_overrides: dict[str, Any], row_overrides: dict[str, Any]
) -> None:
    subject = fake_subject()
    integration = device_integration()
    keys = subject.integrations.keys
    subject.put({**device_integration_to_item(integration, keys), **row_overrides})
    if "tenant_id" in lookup_overrides:
        # A row exists at the other tenant's key, but it still belongs to this tenant.
        other = device_integration(tenant_id=OTHER_TENANT)
        subject.put({**device_integration_to_item(other, keys), "tenant_id": TENANT})
    if "device_integration_id" in lookup_overrides:
        # The lookup points at a second row whose stored id disagrees with its key.
        other = device_integration(device_integration_id=OTHER_INTEGRATION)
        subject.put(
            {**device_integration_to_item(other, keys), "device_integration_id": INTEGRATION}
        )
    subject.put(_lookup(integration, **lookup_overrides))

    assert await subject.resolver.resolve_external_identity(PROVIDER, IDENTITY_HASH) is None


@pytest.mark.asyncio
async def test_resolve_rejects_a_lookup_whose_integration_row_is_missing() -> None:
    subject = fake_subject()
    subject.put(_lookup(device_integration()))

    assert await subject.resolver.resolve_external_identity(PROVIDER, IDENTITY_HASH) is None


@pytest.mark.asyncio
async def test_resolve_ignores_a_row_of_another_entity_type_at_the_integration_key() -> None:
    subject = fake_subject()
    integration = device_integration()
    keys = subject.integrations.keys
    subject.put({**device_integration_to_item(integration, keys), "entity_type": "device"})
    subject.put(_lookup(integration))

    assert await subject.resolver.resolve_external_identity(PROVIDER, IDENTITY_HASH) is None


@pytest.mark.asyncio
async def test_a_pre_p3_04_global_hash_row_is_not_the_account_scoped_identity() -> None:
    # D22: a DeviceIntegration stored under the M1 global hash never answers the scoped hash.
    subject = fake_subject()
    await subject.seed_device_integration(
        device_integration(external_identity_hash=external_identity_hash(PROVIDER, EXTERNAL_DEVICE))
    )

    assert await subject.resolver.resolve_external_identity(PROVIDER, IDENTITY_HASH) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("status", list(DeviceIntegrationStatus))
async def test_resolve_returns_the_row_in_every_status(status: DeviceIntegrationStatus) -> None:
    subject = fake_subject()
    integration = device_integration(status=status)
    await subject.seed_device_integration(integration)

    assert await subject.resolver.resolve_external_identity(PROVIDER, IDENTITY_HASH) == integration


@pytest.mark.asyncio
@pytest.mark.parametrize("error", store_errors(), ids=lambda error: type(error).__name__)
async def test_resolve_propagates_a_failure_of_the_integration_row_read(
    error: BaseException,
) -> None:
    subject = fake_subject()
    await subject.seed_device_integration(device_integration())
    subject.client.fail_next(
        "get_item", error, pk_prefix="TENANT#", sk_prefix="DEVICE_INTEGRATION#"
    )

    with pytest.raises(type(error)) as caught:
        await subject.resolver.resolve_external_identity(PROVIDER, IDENTITY_HASH)

    assert caught.value is error


# Consistent Device read --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_consistent_device_read_sets_consistent_read() -> None:
    subject = fake_subject()
    await subject.seed_device(device())

    assert await subject.devices.get_device(TENANT, DEVICE, consistent=True) == device()

    (call,) = subject.client.calls
    assert call.keys[0] == (f"TENANT#{TENANT}", f"DEVICE#{DEVICE}")
    assert call.request["ConsistentRead"] is True


@pytest.mark.asyncio
async def test_m1_device_read_stays_eventually_consistent() -> None:
    subject = fake_subject()
    await subject.seed_device(device())

    assert await subject.devices.get_device(TENANT, DEVICE) == device()
    assert await subject.devices.get_device(TENANT, DEVICE, consistent=False) == device()

    assert [call.consistent for call in subject.client.calls] == [False, False]


@pytest.mark.asyncio
async def test_consistent_device_read_is_tenant_checked() -> None:
    subject = fake_subject()
    subject.put({**device_v2_to_item(device()), "tenant_id": OTHER_TENANT})

    assert await subject.devices.get_device(TENANT, DEVICE, consistent=True) is None
