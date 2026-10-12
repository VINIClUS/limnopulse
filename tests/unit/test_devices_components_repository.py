import json
import re
from datetime import UTC, datetime

import pytest
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError

from limnopulse_api.adapters.dynamodb_devices_v2 import (
    DynamoDeviceComponentRepository,
    capability_from_item,
    capability_to_item,
    component_from_item,
    component_to_item,
    device_lookup_to_item,
    device_v2_from_item,
    device_v2_to_item,
)
from limnopulse_api.adapters.dynamodb_v2 import (
    DynamoConditionalConflict,
    DynamoV2RepositoryBase,
    idempotency_key_hash,
)
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.devices_v2 import (
    ActuatorProfile,
    CapabilityAvailability,
    CapabilityDeclaration,
    CapabilityOperation,
    CapabilityProvenance,
    CapabilitySupport,
    CommandRiskClass,
    ComponentKind,
    ComponentStatus,
    DeviceComponent,
    DeviceStatus,
    DeviceV2,
    ProbeProfile,
)
from limnopulse_api.domain.v2_common import new_idempotency_receipt
from limnopulse_api.repositories.v2_contracts import (
    ComponentReader,
    DeviceComponentRepository,
    DeviceReader,
)

NOW = datetime(2026, 8, 25, 12, 0, 0, 500000, tzinfo=UTC)
LATER = datetime(2026, 8, 25, 13, 30, 15, 250000, tzinfo=UTC)
TENANT_A = "tnt_a"
TENANT_B = "tnt_b"
DEVICE_1 = "dev_" + "1" * 32
DEVICE_2 = "dev_" + "2" * 32
COMPONENT_1 = "cmp_" + "1" * 32
COMPONENT_2 = "cmp_" + "2" * 32
CAPABILITY_1 = "cap_" + "1" * 32
PROBE_1 = "prb_" + "1" * 32
ACTUATOR_1 = "act_" + "1" * 32
HEX_A = "a" * 64


class FakeDynamoClient:
    """In-memory table evaluating only the condition shapes this adapter emits."""

    def __init__(self, page_size: int = 2) -> None:
        self.serializer = TypeSerializer()
        self.deserializer = TypeDeserializer()
        self.items: dict[tuple[str, str], dict] = {}
        self.calls: list[tuple[str, dict]] = []
        self.page_size = page_size
        self.fail_with: Exception | None = None

    def seed(self, item: dict) -> None:
        self.items[(item["PK"], item["SK"])] = {
            key: self.serializer.serialize(value) for key, value in item.items()
        }

    def plain(self, pk: str, sk: str) -> dict | None:
        item = self.items.get((pk, sk))
        if item is None:
            return None
        return {key: self.deserializer.deserialize(value) for key, value in item.items()}

    def get_item(self, **kwargs):
        self.calls.append(("get_item", kwargs))
        key = self._key(kwargs["Key"])
        item = self.items.get(key)
        return {} if item is None else {"Item": item}

    def query(self, **kwargs):
        self.calls.append(("query", kwargs))
        values = {
            name: self.deserializer.deserialize(value)
            for name, value in kwargs["ExpressionAttributeValues"].items()
        }
        assert kwargs["KeyConditionExpression"] == "PK = :pk AND begins_with(SK, :sk_prefix)"
        matches = sorted(
            (key, item)
            for key, item in self.items.items()
            if key[0] == values[":pk"] and key[1].startswith(values[":sk_prefix"])
        )
        start = kwargs.get("ExclusiveStartKey")
        if start is not None:
            start_key = self._key(start)
            matches = [match for match in matches if match[0] > start_key]
        page = matches[: self.page_size]
        response: dict = {"Items": [item for _, item in page]}
        if len(matches) > self.page_size:
            last = page[-1][0]
            response["LastEvaluatedKey"] = {"PK": {"S": last[0]}, "SK": {"S": last[1]}}
        return response

    def transact_write_items(self, **kwargs):
        self.calls.append(("transact_write_items", kwargs))
        if self.fail_with is not None:
            error, self.fail_with = self.fail_with, None
            raise error
        actions = kwargs["TransactItems"]
        reasons = []
        for action in actions:
            ((kind, request),) = action.items()
            key = self._key(request["Item"] if kind == "Put" else request["Key"])
            ok = self._condition(request, self.items.get(key))
            reasons.append({"Code": "None" if ok else "ConditionalCheckFailed"})
        if any(reason["Code"] != "None" for reason in reasons):
            raise ClientError(
                {
                    "Error": {"Code": "TransactionCanceledException", "Message": "cancelled"},
                    "CancellationReasons": reasons,
                },
                "TransactWriteItems",
            )
        for action in actions:
            ((kind, request),) = action.items()
            if kind == "Put":
                self.items[self._key(request["Item"])] = dict(request["Item"])
            elif kind == "Update":
                self._apply_update(request)
        return {}

    def __getattr__(self, name: str):
        raise AssertionError(f"unexpected client call: {name}")

    def _key(self, item: dict) -> tuple[str, str]:
        return (
            self.deserializer.deserialize(item["PK"]),
            self.deserializer.deserialize(item["SK"]),
        )

    def _condition(self, request: dict, existing: dict | None) -> bool:
        condition = request.get("ConditionExpression")
        if condition is None:
            return True
        names = request.get("ExpressionAttributeNames", {})
        values = {
            name: self.deserializer.deserialize(value)
            for name, value in request.get("ExpressionAttributeValues", {}).items()
        }
        current = (
            {}
            if existing is None
            else {key: self.deserializer.deserialize(value) for key, value in existing.items()}
        )

        def clause(text: str) -> bool:
            text = text.strip()
            if match := re.fullmatch(r"attribute_exists\((\S+)\)", text):
                return names.get(match[1], match[1]) in current
            if match := re.fullmatch(r"attribute_not_exists\((\S+)\)", text):
                return names.get(match[1], match[1]) not in current
            if match := re.fullmatch(r"(\S+) (=|<=|<>) (\S+)", text):
                attribute = names.get(match[1], match[1])
                if attribute not in current:
                    return False
                left, right = current[attribute], values[match[3]]
                return {"=": left == right, "<=": left <= right, "<>": left != right}[match[2]]
            raise AssertionError(f"unsupported condition clause: {text}")

        if " OR " in condition:
            return any(clause(part) for part in condition.split(" OR "))
        return all(clause(part) for part in condition.split(" AND "))

    def _apply_update(self, request: dict) -> None:
        key = self._key(request["Key"])
        expression = request["UpdateExpression"]
        assert expression.startswith("SET ")
        names = request["ExpressionAttributeNames"]
        values = request["ExpressionAttributeValues"]
        item = self.items[key]
        for assignment in expression[4:].split(", "):
            name, value = assignment.split(" = ")
            item[names[name]] = values[value]


def repository(client: FakeDynamoClient | None = None, now: datetime = NOW):
    return DynamoDeviceComponentRepository(
        "LimnopulseDomain", client or FakeDynamoClient(), clock=lambda: now
    )


def device_v2(**overrides) -> DeviceV2:
    values = {
        "tenant_id": TENANT_A,
        "device_id": DEVICE_1,
        "name": "Gateway",
        "firmware_version": "1.2.3",
        "created_at": NOW,
        "updated_at": NOW,
        "version": 1,
    }
    values.update(overrides)
    return DeviceV2(**values)


def probe(**overrides) -> DeviceComponent:
    values = {
        "tenant_id": TENANT_A,
        "component_id": COMPONENT_1,
        "device_id": DEVICE_1,
        "kind": ComponentKind.PROBE,
        "name": "Probe A",
        "probe_profile": ProbeProfile(
            probe_profile_id=PROBE_1, serial_number="SN-1", measurement_metric_keys=("ph",)
        ),
        "created_at": NOW,
        "updated_at": NOW,
        "version": 1,
    }
    values.update(overrides)
    return DeviceComponent(**values)


def capability(**overrides) -> CapabilityDeclaration:
    values = {
        "tenant_id": TENANT_A,
        "component_id": COMPONENT_1,
        "capability_id": CAPABILITY_1,
        "capability_key": "aerator.set_power",
        "operation": CapabilityOperation.COMMAND,
        "support": CapabilitySupport.SUPPORTED,
        "availability": CapabilityAvailability.AVAILABLE,
        "provenance": CapabilityProvenance.MODEL_PROFILE,
        "parameter_schema": {"type": "object", "properties": {"power": {"maximum": 1.0}}},
        "result_schema": {"type": "object", "default": 2.5},
        "provider_implementation_key": "relay.channel_1",
        "risk_class": CommandRiskClass.R2,
        "verified_at": NOW,
        "created_at": NOW,
        "updated_at": NOW,
        "version": 1,
    }
    values.update(overrides)
    return CapabilityDeclaration(**values)


def receipt(scope: str):
    return new_idempotency_receipt(
        tenant_id=TENANT_A,
        scope=scope,
        key_hash=idempotency_key_hash(f"{scope}-key"),
        request_hash=HEX_A,
        response_snapshot_json="{}",
        created_at=NOW,
    )


def legacy_device_items(tenant_id: str = TENANT_A, device_id: str = "local-device-001"):
    common = {
        "tenant_id": tenant_id,
        "pond_id": "pond_1",
        "device_id": device_id,
        "name": "Legacy Probe",
        "auth_type": "mtls",
        "firmware_version": None,
        "status": "active",
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-01T00:00:00+00:00",
        "version": 3,
        "schema_version": 1,
    }
    row = {"PK": f"TENANT#{tenant_id}", "SK": f"DEVICE#{device_id}", "entity_type": "device"}
    lookup = {"PK": f"DEVICE#{device_id}", "SK": "META", "entity_type": "device_lookup"}
    return {**row, **common}, {**lookup, **common}


def transaction(client: FakeDynamoClient) -> list[dict]:
    return next(kwargs for name, kwargs in client.calls if name == "transact_write_items")[
        "TransactItems"
    ]


def transaction_item_keys(client: FakeDynamoClient) -> list[tuple[str, str]]:
    keys = []
    for action in transaction(client):
        ((kind, request),) = action.items()
        key = request["Item"] if kind == "Put" else request["Key"]
        keys.append((key["PK"]["S"], key["SK"]["S"]))
    return keys


def test_repository_implements_frozen_protocols_on_the_shared_base() -> None:
    repo = repository()
    assert isinstance(repo, DynamoV2RepositoryBase)
    assert isinstance(repo, DeviceComponentRepository)
    assert isinstance(repo, DeviceReader)
    assert isinstance(repo, ComponentReader)


# Devices ---------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_device_writes_tenant_record_lookup_and_receipt() -> None:
    client = FakeDynamoClient()
    await repository(client).create_device(device_v2(), receipt("device-create"))
    assert transaction_item_keys(client) == [
        ("TENANT#tnt_a", f"DEVICE#{DEVICE_1}"),
        (f"DEVICE#{DEVICE_1}", "META"),
        ("TENANT#tnt_a", f"IDEMPOTENCY#device-create#{idempotency_key_hash('device-create-key')}"),
    ]
    actions = transaction(client)
    assert actions[0]["Put"]["ConditionExpression"] == "attribute_not_exists(PK)"
    assert actions[1]["Put"]["ConditionExpression"] == "attribute_not_exists(PK)"
    row = client.plain("TENANT#tnt_a", f"DEVICE#{DEVICE_1}")
    lookup = client.plain(f"DEVICE#{DEVICE_1}", "META")
    assert row["schema_version"] == 2
    assert row["created_at"] == "2026-08-25T12:00:00.500000Z"
    assert not {"pond_id", "auth_type", "expires_at"} & set(row)
    assert set(lookup) == {"PK", "SK", "entity_type", "tenant_id", "device_id", "status", "version"}
    assert lookup["version"] == row["version"] == 1


@pytest.mark.asyncio
async def test_create_device_with_existing_id_is_a_conflict() -> None:
    client = FakeDynamoClient()
    legacy_row, legacy_lookup = legacy_device_items(device_id=DEVICE_1)
    client.seed(legacy_row)
    client.seed(legacy_lookup)
    with pytest.raises(DynamoConditionalConflict) as caught:
        await repository(client).create_device(device_v2(), receipt("device-create"))
    assert caught.value.failed_indexes == (0, 1)
    assert client.plain("TENANT#tnt_a", f"DEVICE#{DEVICE_1}")["pond_id"] == "pond_1"


@pytest.mark.asyncio
async def test_device_codecs_round_trip_native_rows() -> None:
    device = device_v2(model_profile_key="acme.gw1", compatibility_level="certified")
    assert device_v2_from_item(device_v2_to_item(device)) == device
    assert device_lookup_to_item(device)["status"] == "active"


@pytest.mark.asyncio
async def test_get_and_list_map_legacy_rows_through_allowlist() -> None:
    client = FakeDynamoClient()
    row, lookup = legacy_device_items()
    client.seed({**row, "default_component_id": COMPONENT_1, "v2_projection_version": 1})
    client.seed(lookup)
    client.seed(device_v2_to_item(device_v2()))
    repo = repository(client)

    legacy = await repo.get_device(TENANT_A, "local-device-001")
    assert legacy == DeviceV2(
        tenant_id=TENANT_A,
        device_id="local-device-001",
        name="Legacy Probe",
        status=DeviceStatus.ACTIVE,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
        version=3,
    )
    devices = await repo.list_devices(TENANT_A)
    assert sorted(device.device_id for device in devices) == [DEVICE_1, "local-device-001"]


def test_legacy_unknown_status_and_naive_timestamps_are_normalized() -> None:
    row, _ = legacy_device_items()
    row.update(
        status="maintenance",
        created_at="2026-01-01T00:00:00",
        updated_at="2026-01-02T00:00:00",
    )
    device = device_v2_from_item(row)
    assert device.status is DeviceStatus.SUSPENDED
    assert device.updated_at == datetime(2026, 1, 2, tzinfo=UTC)


@pytest.mark.asyncio
async def test_list_devices_paginates_and_ignores_other_prefixes() -> None:
    client = FakeDynamoClient(page_size=1)
    for index in range(3):
        device_id = "dev_" + str(index) * 32
        client.seed(device_v2_to_item(device_v2(device_id=device_id)))
    client.seed({"PK": "TENANT#tnt_a", "SK": "DEVICE_INTEGRATION#di_1", "tenant_id": TENANT_A})
    devices = await repository(client).list_devices(TENANT_A)
    assert len(devices) == 3
    queries = [kwargs for name, kwargs in client.calls if name == "query"]
    assert len(queries) == 3
    assert "ExclusiveStartKey" in queries[1]


@pytest.mark.asyncio
async def test_device_reads_are_tenant_isolated() -> None:
    client = FakeDynamoClient()
    client.seed(device_v2_to_item(device_v2(tenant_id=TENANT_B)))
    repo = repository(client)
    assert await repo.get_device(TENANT_A, DEVICE_1) is None
    assert await repo.list_devices(TENANT_A) == []
    with pytest.raises(NotFoundError):
        await repo.update_device(
            TENANT_A, DEVICE_1, 1, name="x", firmware_version=None, status=None
        )


@pytest.mark.asyncio
async def test_update_native_device_sets_row_and_minimal_lookup_with_same_version() -> None:
    client = FakeDynamoClient()
    device = device_v2()
    client.seed(device_v2_to_item(device))
    client.seed(device_lookup_to_item(device))
    updated = await repository(client, now=LATER).update_device(
        TENANT_A, DEVICE_1, 1, name="Renamed", firmware_version=None, status=DeviceStatus.SUSPENDED
    )
    assert updated.version == 2
    assert updated.status is DeviceStatus.SUSPENDED
    assert updated.updated_at == LATER
    row = client.plain("TENANT#tnt_a", f"DEVICE#{DEVICE_1}")
    lookup = client.plain(f"DEVICE#{DEVICE_1}", "META")
    assert row["name"] == "Renamed"
    assert row["firmware_version"] == "1.2.3"
    assert row["updated_at"] == "2026-08-25T13:30:15.250000Z"
    assert lookup == {
        "PK": f"DEVICE#{DEVICE_1}",
        "SK": "META",
        "entity_type": "device_lookup",
        "tenant_id": TENANT_A,
        "device_id": DEVICE_1,
        "status": "suspended",
        "version": 2,
    }
    actions = transaction(client)
    assert [next(iter(action)) for action in actions] == ["Update", "Update"]
    for action in actions:
        assert "#version = :expected_version" in action["Update"]["ConditionExpression"]
        assert "#tenant_id = :tenant_id" in action["Update"]["ConditionExpression"]


@pytest.mark.asyncio
async def test_update_legacy_device_preserves_pond_and_auth_type() -> None:
    client = FakeDynamoClient()
    row, lookup = legacy_device_items()
    client.seed(row)
    client.seed(lookup)
    updated = await repository(client, now=LATER).update_device(
        TENANT_A, "local-device-001", 3, name="Renamed", firmware_version="2.0", status=None
    )
    assert updated.version == 4
    assert updated.updated_at == datetime(2026, 8, 25, 13, 30, 15, tzinfo=UTC)
    legacy_keys = (("TENANT#tnt_a", "DEVICE#local-device-001"), ("DEVICE#local-device-001", "META"))
    for pk, sk in legacy_keys:
        stored = client.plain(pk, sk)
        assert stored["pond_id"] == "pond_1"
        assert stored["auth_type"] == "mtls"
        assert stored["schema_version"] == 1
        assert stored["name"] == "Renamed"
        assert stored["firmware_version"] == "2.0"
        assert stored["status"] == "active"
        assert stored["version"] == 4
        assert stored["updated_at"] == "2026-08-25T13:30:15+00:00"
        assert stored["created_at"] == "2026-01-01T00:00:00+00:00"
    assert all("Put" not in action for action in transaction(client))


@pytest.mark.asyncio
async def test_update_device_rejects_stale_version_and_terminal_transition() -> None:
    client = FakeDynamoClient()
    device = device_v2(status=DeviceStatus.DECOMMISSIONED)
    client.seed(device_v2_to_item(device))
    client.seed(device_lookup_to_item(device))
    repo = repository(client)
    with pytest.raises(ConflictError):
        await repo.update_device(
            TENANT_A, DEVICE_1, 2, name="x", firmware_version=None, status=None
        )
    with pytest.raises(ConflictError):
        await repo.update_device(
            TENANT_A, DEVICE_1, 1, name=None, firmware_version=None, status=DeviceStatus.ACTIVE
        )


@pytest.mark.asyncio
async def test_update_device_fences_lookup_version_transactionally() -> None:
    client = FakeDynamoClient()
    client.seed(device_v2_to_item(device_v2()))
    client.seed(device_lookup_to_item(device_v2(version=5)))
    with pytest.raises(DynamoConditionalConflict) as caught:
        await repository(client).update_device(
            TENANT_A, DEVICE_1, 1, name="x", firmware_version=None, status=None
        )
    assert caught.value.failed_indexes == (1,)
    assert client.plain("TENANT#tnt_a", f"DEVICE#{DEVICE_1}")["name"] == "Gateway"


@pytest.mark.asyncio
async def test_infrastructure_errors_are_not_swallowed() -> None:
    client = FakeDynamoClient()
    client.fail_with = ClientError(
        {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "slow"}},
        "TransactWriteItems",
    )
    with pytest.raises(ClientError):
        await repository(client).create_device(device_v2(), receipt("device-create"))


# Components ------------------------------------------------------------------------------------


def seed_device(client: FakeDynamoClient, **overrides) -> DeviceV2:
    device = device_v2(**overrides)
    client.seed(device_v2_to_item(device))
    client.seed(device_lookup_to_item(device))
    return device


def seed_component(client: FakeDynamoClient, component: DeviceComponent) -> None:
    client.seed(component_to_item(component, canonical=False))
    client.seed(component_to_item(component, canonical=True))


@pytest.mark.asyncio
async def test_create_component_writes_mirrors_fences_device_and_receipt() -> None:
    client = FakeDynamoClient()
    seed_device(client)
    await repository(client).create_component(probe(), receipt("component-create"))
    keys = transaction_item_keys(client)
    assert keys[:3] == [
        ("TENANT#tnt_a", f"COMPONENT#{COMPONENT_1}"),
        (f"COMPONENT#{COMPONENT_1}", "META"),
        ("TENANT#tnt_a", f"DEVICE#{DEVICE_1}"),
    ]
    assert keys[3][1].startswith("IDEMPOTENCY#component-create#")
    assert "ConditionCheck" in transaction(client)[2]
    mirror = client.plain("TENANT#tnt_a", f"COMPONENT#{COMPONENT_1}")
    canonical = client.plain(f"COMPONENT#{COMPONENT_1}", "META")
    assert {k: v for k, v in mirror.items() if k not in {"PK", "SK", "entity_type"}} == {
        k: v for k, v in canonical.items() if k not in {"PK", "SK", "entity_type"}
    }
    assert canonical["version"] == mirror["version"] == 1


@pytest.mark.asyncio
async def test_create_component_requires_same_tenant_active_device() -> None:
    client = FakeDynamoClient()
    seed_device(client, tenant_id=TENANT_B)
    with pytest.raises(NotFoundError):
        await repository(client).create_component(probe(), receipt("component-create"))

    client = FakeDynamoClient()
    seed_device(client, status=DeviceStatus.DECOMMISSIONED)
    with pytest.raises(ConflictError):
        await repository(client).create_component(probe(), receipt("component-create"))


@pytest.mark.asyncio
async def test_one_gateway_owns_multiple_probes_and_list_filters_by_device() -> None:
    client = FakeDynamoClient(page_size=1)
    seed_device(client)
    seed_device(client, device_id=DEVICE_2)
    seed_component(client, probe())
    seed_component(client, probe(component_id=COMPONENT_2, name="Probe B"))
    seed_component(client, probe(component_id="cmp_" + "3" * 32, device_id=DEVICE_2))
    components = await repository(client).list_components(TENANT_A, DEVICE_1)
    assert [component.component_id for component in components] == [COMPONENT_1, COMPONENT_2]


@pytest.mark.asyncio
async def test_component_lookup_rejects_tenant_mismatch() -> None:
    client = FakeDynamoClient()
    seed_component(client, probe(tenant_id=TENANT_B))
    repo = repository(client)
    assert await repo.get_component(TENANT_A, COMPONENT_1) is None
    assert await repo.list_components(TENANT_A, DEVICE_1) == []
    assert await repo.list_capabilities(TENANT_A, COMPONENT_1) == []
    with pytest.raises(NotFoundError):
        await repo.update_component(TENANT_A, COMPONENT_1, 1, name="x", status=None)


@pytest.mark.asyncio
async def test_component_codecs_round_trip_all_kinds() -> None:
    hybrid = probe(
        kind=ComponentKind.HYBRID,
        model_profile_key="acme.probe",
        actuator_profile=ActuatorProfile(actuator_profile_id=ACTUATOR_1, actuator_type="relay"),
    )
    actuator = probe(
        kind=ComponentKind.ACTUATOR,
        probe_profile=None,
        actuator_profile=ActuatorProfile(actuator_profile_id=ACTUATOR_1, actuator_type="relay"),
    )
    for component in (probe(), hybrid, actuator):
        for canonical in (True, False):
            assert component_from_item(component_to_item(component, canonical=canonical)) == (
                component
            )


@pytest.mark.asyncio
async def test_update_component_updates_both_mirrors_with_same_version() -> None:
    client = FakeDynamoClient()
    seed_component(client, probe())
    updated = await repository(client, now=LATER).update_component(
        TENANT_A, COMPONENT_1, 1, name="Probe Z", status=ComponentStatus.REPLACED
    )
    assert updated.version == 2
    assert updated.status is ComponentStatus.REPLACED
    assert updated.kind is ComponentKind.PROBE
    assert updated.probe_profile == probe().probe_profile
    mirror_keys = (
        ("TENANT#tnt_a", f"COMPONENT#{COMPONENT_1}"),
        (f"COMPONENT#{COMPONENT_1}", "META"),
    )
    for pk, sk in mirror_keys:
        stored = client.plain(pk, sk)
        assert stored["version"] == 2
        assert stored["status"] == "replaced"
        assert stored["name"] == "Probe Z"
        assert stored["updated_at"] == "2026-08-25T13:30:15.250000Z"
    actions = transaction(client)
    assert len(actions) == 2
    for action in actions:
        assert "#version = :expected_version" in action["Update"]["ConditionExpression"]


@pytest.mark.asyncio
async def test_update_component_rejects_terminal_transition_and_mirror_drift() -> None:
    client = FakeDynamoClient()
    seed_component(client, probe(status=ComponentStatus.REPLACED))
    with pytest.raises(ConflictError):
        await repository(client).update_component(
            TENANT_A, COMPONENT_1, 1, name=None, status=ComponentStatus.ACTIVE
        )

    client = FakeDynamoClient()
    client.seed(component_to_item(probe(version=4), canonical=False))
    client.seed(component_to_item(probe(), canonical=True))
    with pytest.raises(DynamoConditionalConflict) as caught:
        await repository(client).update_component(TENANT_A, COMPONENT_1, 1, name="x", status=None)
    assert caught.value.failed_indexes == (0,)


# Capabilities ----------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_capability_round_trips_float_schemas_as_canonical_json() -> None:
    client = FakeDynamoClient()
    seed_component(client, probe())
    await repository(client).create_capability(capability(), receipt("capability-create"))
    stored = client.plain(f"COMPONENT#{COMPONENT_1}", f"CAPABILITY#{CAPABILITY_1}")
    assert isinstance(stored["parameter_schema_json"], str)
    assert "1.0" in stored["parameter_schema_json"]
    assert "parameter_schema" not in stored
    listed = await repository(client).list_capabilities(TENANT_A, COMPONENT_1)
    assert listed == [capability()]
    maximum = listed[0].parameter_schema["properties"]["power"]["maximum"]
    assert isinstance(maximum, float)
    assert json.dumps(listed[0].result_schema, sort_keys=True) == (
        '{"default": 2.5, "type": "object"}'
    )


def test_capability_codec_round_trips_read_declaration_without_optionals() -> None:
    declaration = capability(
        operation=CapabilityOperation.READ,
        capability_key="water.ph",
        risk_class=None,
        parameter_schema=None,
        result_schema=None,
        provider_implementation_key=None,
        verified_at=None,
    )
    item = capability_to_item(declaration)
    assert not {"risk_class", "verified_at", "parameter_schema_json"} & set(item)
    assert capability_from_item(item) == declaration


@pytest.mark.asyncio
async def test_create_capability_fences_component_and_writes_receipt() -> None:
    client = FakeDynamoClient()
    seed_component(client, probe())
    await repository(client).create_capability(capability(), receipt("capability-create"))
    keys = transaction_item_keys(client)
    assert keys[:2] == [
        (f"COMPONENT#{COMPONENT_1}", "META"),
        (f"COMPONENT#{COMPONENT_1}", f"CAPABILITY#{CAPABILITY_1}"),
    ]
    assert keys[2][1].startswith("IDEMPOTENCY#capability-create#")
    assert "ConditionCheck" in transaction(client)[0]


@pytest.mark.asyncio
async def test_create_capability_rejects_foreign_or_terminal_component() -> None:
    client = FakeDynamoClient()
    seed_component(client, probe(tenant_id=TENANT_B))
    with pytest.raises(NotFoundError):
        await repository(client).create_capability(capability(), receipt("capability-create"))

    client = FakeDynamoClient()
    seed_component(client, probe(status=ComponentStatus.DECOMMISSIONED))
    with pytest.raises(ConflictError):
        await repository(client).create_capability(capability(), receipt("capability-create"))


@pytest.mark.asyncio
async def test_list_capabilities_queries_known_component_partition_and_filters_tenant() -> None:
    client = FakeDynamoClient(page_size=1)
    seed_component(client, probe())
    client.seed(capability_to_item(capability()))
    client.seed(capability_to_item(capability(capability_id="cap_" + "2" * 32, tenant_id="x")))
    listed = await repository(client).list_capabilities(TENANT_A, COMPONENT_1)
    assert [item.capability_id for item in listed] == [CAPABILITY_1]
    query = next(kwargs for name, kwargs in client.calls if name == "query")
    values = query["ExpressionAttributeValues"]
    assert values[":pk"] == {"S": f"COMPONENT#{COMPONENT_1}"}
    assert values[":sk_prefix"] == {"S": "CAPABILITY#"}


def test_repository_exposes_no_capability_mutation_or_delete() -> None:
    names = {name for name in dir(DynamoDeviceComponentRepository) if not name.startswith("_")}
    assert not {name for name in names if "delete" in name}
    assert not {"update_capability", "replace_capability"} & names


@pytest.mark.asyncio
async def test_updates_never_move_updated_at_backwards() -> None:
    client = FakeDynamoClient()
    device = device_v2(created_at=LATER, updated_at=LATER)
    client.seed(device_v2_to_item(device))
    client.seed(device_lookup_to_item(device))
    seed_component(client, probe(created_at=LATER, updated_at=LATER))
    repo = repository(client, now=NOW)
    updated_device = await repo.update_device(
        TENANT_A, DEVICE_1, 1, name="x", firmware_version=None, status=None
    )
    updated_component = await repo.update_component(TENANT_A, COMPONENT_1, 1, name="x", status=None)
    assert updated_device.updated_at == LATER
    assert updated_component.updated_at == LATER
