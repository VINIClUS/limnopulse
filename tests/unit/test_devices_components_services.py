import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from limnopulse_api.adapters.dynamodb_v2 import canonical_request_hash
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.devices_v2 import (
    ActuatorProfile,
    CapabilityDeclaration,
    CapabilityOperation,
    ComponentKind,
    ComponentStatus,
    DeviceComponent,
    DeviceStatus,
    DeviceV2,
    ProbeProfile,
)
from limnopulse_api.domain.ids import is_canonical_id
from limnopulse_api.services.capabilities import CapabilityCreateCommand, CapabilityService
from limnopulse_api.services.components import (
    ComponentCreateCommand,
    ComponentService,
    ComponentUpdateCommand,
)
from limnopulse_api.services.devices_v2 import (
    DeviceV2CreateCommand,
    DeviceV2Service,
    DeviceV2UpdateCommand,
    entity_snapshot_json,
)

NOW = datetime(2026, 8, 25, 12, 0, 0, 500000, tzinfo=UTC)
TENANT_A = "tnt_a"
TENANT_B = "tnt_b"
GATEWAY = "dev_" + "a" * 32
COMPONENT_1 = "cmp_" + "1" * 32


def fixed_clock() -> datetime:
    return NOW


class FakeDeviceComponentRepository:
    """Implements only the calls these service tests assert on."""

    def __init__(self) -> None:
        self.devices: dict[tuple[str, str], DeviceV2] = {}
        self.components: dict[str, DeviceComponent] = {}
        self.capabilities: dict[str, CapabilityDeclaration] = {}
        self.receipts: dict[tuple[str, str, str], tuple[str, str]] = {}
        self.lose_receipt_race = False
        self.updates: list[tuple] = []

    @classmethod
    def with_device(cls, tenant_id: str, device_id: str, **overrides):
        repository = cls()
        values = {
            "tenant_id": tenant_id,
            "device_id": device_id,
            "name": "Gateway",
            "created_at": NOW,
            "updated_at": NOW,
            "version": 1,
        }
        values.update(overrides)
        repository.devices[(tenant_id, device_id)] = DeviceV2(**values)
        return repository

    async def get_idempotency_replay(self, tenant_id, scope, key_hash, request_hash):
        stored = self.receipts.get((tenant_id, scope, key_hash))
        if stored is None:
            return None
        if stored[0] != request_hash:
            raise ConflictError("idempotency key reused with a different request")
        return stored[1]

    def _store_receipt(self, receipt) -> None:
        key = (receipt.tenant_id, receipt.scope, receipt.key_hash)
        if self.lose_receipt_race:
            # Simulate a concurrent winner that stored a receipt for the same request.
            self.receipts[key] = (receipt.request_hash, receipt.response_snapshot_json)
            raise ConflictError("conditional write conflict")
        if key in self.receipts:
            raise ConflictError("conditional write conflict")
        self.receipts[key] = (receipt.request_hash, receipt.response_snapshot_json)

    async def get_device(self, tenant_id, device_id):
        return self.devices.get((tenant_id, device_id))

    async def list_devices(self, tenant_id):
        return [device for (tenant, _), device in self.devices.items() if tenant == tenant_id]

    async def create_device(self, device, receipt):
        self._store_receipt(receipt)
        self.devices[(device.tenant_id, device.device_id)] = device
        return device

    async def update_device(self, tenant_id, device_id, expected_version, **changes):
        self.updates.append(("device", tenant_id, device_id, expected_version, changes))
        current = self.devices[(tenant_id, device_id)]
        values = {key: value for key, value in changes.items() if value is not None}
        updated = current.model_copy(update={**values, "version": expected_version + 1})
        self.devices[(tenant_id, device_id)] = updated
        return updated

    async def get_component(self, tenant_id, component_id):
        component = self.components.get(component_id)
        if component is None or component.tenant_id != tenant_id:
            return None
        return component

    async def list_components(self, tenant_id, device_id):
        return [
            component
            for component in self.components.values()
            if component.tenant_id == tenant_id and component.device_id == device_id
        ]

    async def create_component(self, component, receipt):
        self._store_receipt(receipt)
        self.components[component.component_id] = component
        return component

    async def update_component(self, tenant_id, component_id, expected_version, **changes):
        self.updates.append(("component", tenant_id, component_id, expected_version, changes))
        current = self.components[component_id]
        values = {key: value for key, value in changes.items() if value is not None}
        updated = current.model_copy(update={**values, "version": expected_version + 1})
        self.components[component_id] = updated
        return updated

    async def list_capabilities(self, tenant_id, component_id):
        return [
            declaration
            for declaration in self.capabilities.values()
            if declaration.tenant_id == tenant_id and declaration.component_id == component_id
        ]

    async def create_capability(self, declaration, receipt):
        self._store_receipt(receipt)
        self.capabilities[declaration.capability_id] = declaration
        return declaration


def probe_request(name: str) -> ComponentCreateCommand:
    return ComponentCreateCommand(
        kind=ComponentKind.PROBE,
        name=name,
        probe_profile={"serial_number": f"SN-{name}", "measurement_metric_keys": ["ph"]},
    )


def component_payload(**overrides) -> dict:
    values = {
        "tenant_id": TENANT_A,
        "component_id": COMPONENT_1,
        "device_id": GATEWAY,
        "name": "Component",
        "created_at": NOW,
        "updated_at": NOW,
        "version": 1,
    }
    values.update(overrides)
    return values


def probe_profile() -> ProbeProfile:
    return ProbeProfile(probe_profile_id="prb_" + "1" * 32)


def actuator_profile() -> ActuatorProfile:
    return ActuatorProfile(actuator_profile_id="act_" + "1" * 32, actuator_type="relay")


def command_capability(**overrides) -> CapabilityCreateCommand:
    values = {
        "capability_key": "aerator.set_power",
        "operation": "command",
        "support": "supported",
        "availability": "available",
        "provenance": "model_profile",
        "parameter_schema": {"type": "object", "properties": {"power": {"maximum": 1.0}}},
        "provider_implementation_key": "relay.channel_1",
        "risk_class": "r2",
        "verified_at": NOW,
    }
    values.update(overrides)
    return CapabilityCreateCommand(**values)


async def component_service_with_probe(**device_overrides):
    repository = FakeDeviceComponentRepository.with_device(TENANT_A, GATEWAY, **device_overrides)
    service = ComponentService(repository, clock=fixed_clock)
    return repository, service


# Profile kind invariants -----------------------------------------------------------------------


def test_probe_requires_only_a_probe_profile() -> None:
    component = component_payload(kind="probe", probe_profile=probe_profile())
    assert DeviceComponent(**component).actuator_profile is None
    with pytest.raises(ValidationError):
        DeviceComponent(**component_payload(kind="probe", actuator_profile=actuator_profile()))


@pytest.mark.parametrize(
    "payload",
    [
        {"kind": "probe", "name": "P"},
        {
            "kind": "probe",
            "name": "P",
            "probe_profile": {},
            "actuator_profile": {"actuator_type": "relay"},
        },
        {"kind": "actuator", "name": "A", "probe_profile": {}},
        {"kind": "hybrid", "name": "H", "probe_profile": {}},
    ],
)
def test_component_create_command_enforces_kind_profiles(payload) -> None:
    with pytest.raises(ValidationError):
        ComponentCreateCommand(**payload)


@pytest.mark.parametrize("field", ["pond_id", "auth_type", "aws_thing_name", "certificate_arn"])
def test_device_create_command_rejects_legacy_and_provider_fields(field: str) -> None:
    with pytest.raises(ValidationError):
        DeviceV2CreateCommand(name="Gateway", **{field: "x"})


@pytest.mark.parametrize(
    "field", ["device_id", "kind", "probe_profile", "model_profile_key", "tenant_id"]
)
def test_component_update_command_rejects_immutable_fields(field: str) -> None:
    with pytest.raises(ValidationError):
        ComponentUpdateCommand(expected_version=1, **{field: "x"})


# Devices ---------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_device_create_starts_active_with_canonical_id_and_receipt() -> None:
    repository = FakeDeviceComponentRepository()
    service = DeviceV2Service(repository, clock=fixed_clock)
    outcome = await service.create(TENANT_A, DeviceV2CreateCommand(name="Gateway"), "key-a-000")
    device = outcome.entity
    assert is_canonical_id(device.device_id, "dev")
    assert device.status is DeviceStatus.ACTIVE
    assert device.version == 1
    assert outcome.replayed is False
    assert json.loads(outcome.snapshot_json)["created_at"] == "2026-08-25T12:00:00.500000Z"
    ((tenant, scope, _),) = repository.receipts
    assert (tenant, scope) == (TENANT_A, "device-create")


@pytest.mark.asyncio
async def test_device_create_replays_same_request_and_rejects_different_body() -> None:
    repository = FakeDeviceComponentRepository()
    service = DeviceV2Service(repository, clock=fixed_clock)
    first = await service.create(TENANT_A, DeviceV2CreateCommand(name="Gateway"), "key-a-000")
    again = await service.create(TENANT_A, DeviceV2CreateCommand(name="Gateway"), "key-a-000")
    assert again.replayed is True
    assert again.snapshot_json == first.snapshot_json
    assert again.entity == first.entity
    assert len(repository.devices) == 1
    with pytest.raises(ConflictError):
        await service.create(TENANT_A, DeviceV2CreateCommand(name="Other"), "key-a-000")


@pytest.mark.asyncio
async def test_device_create_request_hash_covers_scope_tenant_and_body_only() -> None:
    repository = FakeDeviceComponentRepository()
    service = DeviceV2Service(repository, clock=fixed_clock)
    await service.create(TENANT_A, DeviceV2CreateCommand(name="Gateway"), "key-a-000")
    ((stored_hash, _),) = repository.receipts.values()
    assert stored_hash == canonical_request_hash(
        {
            "scope": "device-create",
            "tenant_id": TENANT_A,
            "body": DeviceV2CreateCommand(name="Gateway").model_dump(mode="json"),
        }
    )


@pytest.mark.asyncio
async def test_create_replays_when_a_concurrent_request_wins_the_receipt() -> None:
    repository = FakeDeviceComponentRepository()
    repository.lose_receipt_race = True
    service = DeviceV2Service(repository, clock=fixed_clock)
    outcome = await service.create(TENANT_A, DeviceV2CreateCommand(name="Gateway"), "key-a-000")
    assert outcome.replayed is True


@pytest.mark.asyncio
async def test_device_update_checks_lifecycle_and_tenant() -> None:
    repository = FakeDeviceComponentRepository.with_device(
        TENANT_A, GATEWAY, status=DeviceStatus.DECOMMISSIONED
    )
    service = DeviceV2Service(repository, clock=fixed_clock)
    with pytest.raises(ConflictError):
        await service.update(
            TENANT_A, GATEWAY, DeviceV2UpdateCommand(expected_version=1, status="active")
        )
    with pytest.raises(NotFoundError):
        await service.update(TENANT_B, GATEWAY, DeviceV2UpdateCommand(expected_version=1))
    assert repository.updates == []


@pytest.mark.asyncio
async def test_device_get_accepts_historical_ids_and_rejects_unsafe_ids() -> None:
    repository = FakeDeviceComponentRepository.with_device(TENANT_A, "local-device-001")
    service = DeviceV2Service(repository, clock=fixed_clock)
    assert (await service.get(TENANT_A, "local-device-001")).device_id == "local-device-001"
    assert await service.get(TENANT_A, "dev_not-canonical") is None
    assert await service.get(TENANT_A, "bad#id") is None


# Components ------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_one_gateway_can_have_two_distinct_probes() -> None:
    _, service = await component_service_with_probe()
    first = await service.create(TENANT_A, GATEWAY, probe_request("Probe A"), "key-a-000")
    second = await service.create(TENANT_A, GATEWAY, probe_request("Probe B"), "key-b-000")
    assert first.entity.component_id != second.entity.component_id
    assert first.entity.probe_profile.probe_profile_id != (
        second.entity.probe_profile.probe_profile_id
    )
    assert is_canonical_id(first.entity.component_id, "cmp")
    assert is_canonical_id(first.entity.probe_profile.probe_profile_id, "prb")
    assert first.entity.status is ComponentStatus.ACTIVE
    assert len(await service.list(TENANT_A, GATEWAY)) == 2


@pytest.mark.asyncio
async def test_component_create_generates_actuator_profile_id() -> None:
    _, service = await component_service_with_probe()
    outcome = await service.create(
        TENANT_A,
        GATEWAY,
        ComponentCreateCommand(
            kind="hybrid",
            name="Combo",
            probe_profile={},
            actuator_profile={"actuator_type": "relay"},
        ),
        "key-h-000",
    )
    assert is_canonical_id(outcome.entity.actuator_profile.actuator_profile_id, "act")


@pytest.mark.asyncio
async def test_component_create_requires_same_tenant_live_device() -> None:
    repository = FakeDeviceComponentRepository.with_device(TENANT_B, GATEWAY)
    service = ComponentService(repository, clock=fixed_clock)
    with pytest.raises(NotFoundError):
        await service.create(TENANT_A, GATEWAY, probe_request("Probe A"), "key-a-000")
    with pytest.raises(NotFoundError):
        await service.list(TENANT_A, GATEWAY)

    _, service = await component_service_with_probe(status=DeviceStatus.DECOMMISSIONED)
    with pytest.raises(ConflictError):
        await service.create(TENANT_A, GATEWAY, probe_request("Probe A"), "key-a-000")


@pytest.mark.asyncio
async def test_component_idempotency_is_scoped_to_device_path() -> None:
    repository, service = await component_service_with_probe()
    repository.devices[(TENANT_A, "dev_" + "b" * 32)] = repository.devices[
        (TENANT_A, GATEWAY)
    ].model_copy(update={"device_id": "dev_" + "b" * 32})
    await service.create(TENANT_A, GATEWAY, probe_request("Probe A"), "key-a-000")
    with pytest.raises(ConflictError):
        await service.create(TENANT_A, "dev_" + "b" * 32, probe_request("Probe A"), "key-a-000")


@pytest.mark.asyncio
async def test_component_terminal_lifecycle_is_rejected() -> None:
    _, service = await component_service_with_probe()
    created = await service.create(TENANT_A, GATEWAY, probe_request("Probe A"), "key-a-000")
    component_id = created.entity.component_id
    replaced = await service.update(
        TENANT_A,
        component_id,
        ComponentUpdateCommand(expected_version=1, status="replaced"),
    )
    assert replaced.status is ComponentStatus.REPLACED
    with pytest.raises(ConflictError):
        await service.update(
            TENANT_A, component_id, ComponentUpdateCommand(expected_version=2, status="active")
        )
    with pytest.raises(NotFoundError):
        await service.update(TENANT_B, component_id, ComponentUpdateCommand(expected_version=2))


# Capabilities ----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "overrides",
    [
        {"risk_class": None},
        {"operation": "read", "risk_class": "r0"},
        {"capability_key": "Bad Key"},
        {"capability_key": "a..b"},
        {"verified_at": datetime(2026, 8, 25, 12)},
        {"provider_implementation_key": "arn:aws:iot:us-east-1:123:thing/x"},
        {"tenant_id": TENANT_A},
        {"aws_thing_name": "thing"},
        {"parameter_schema": {"x": float("nan")}},
        {"result_schema": {"x": float("inf")}},
        {"parameter_schema": {"x": {1, 2}}},
        {"parameter_schema": {"d": "x" * (32 * 1024)}},
    ],
)
def test_capability_command_validation(overrides) -> None:
    with pytest.raises(ValidationError):
        command_capability(**overrides)


def test_capability_command_allows_write_and_calibration_without_risk() -> None:
    assert command_capability(operation="write", risk_class=None).risk_class is None
    assert command_capability(operation="read", risk_class=None).operation is (
        CapabilityOperation.READ
    )


@pytest.mark.asyncio
async def test_capability_is_appended_to_live_component_in_tenant() -> None:
    repository, components = await component_service_with_probe()
    created = await components.create(TENANT_A, GATEWAY, probe_request("Probe A"), "key-a-000")
    service = CapabilityService(repository, clock=fixed_clock)
    component_id = created.entity.component_id
    outcome = await service.create(TENANT_A, component_id, command_capability(), "cap-key-000")
    declaration = outcome.entity
    assert is_canonical_id(declaration.capability_id, "cap")
    assert declaration.provider_implementation_key == "relay.channel_1"
    assert declaration.parameter_schema["properties"]["power"]["maximum"] == 1.0
    assert '"maximum": 1.0' in outcome.snapshot_json
    replay = await service.create(TENANT_A, component_id, command_capability(), "cap-key-000")
    assert replay.replayed is True
    assert replay.snapshot_json == outcome.snapshot_json
    assert await service.list(TENANT_A, component_id) == [declaration]
    with pytest.raises(NotFoundError):
        await service.list(TENANT_B, component_id)
    with pytest.raises(NotFoundError):
        await service.create(TENANT_B, component_id, command_capability(), "cap-key-001")


@pytest.mark.asyncio
async def test_capability_rejected_for_terminal_component() -> None:
    repository, components = await component_service_with_probe()
    created = await components.create(TENANT_A, GATEWAY, probe_request("Probe A"), "key-a-000")
    component_id = created.entity.component_id
    await components.update(
        TENANT_A, component_id, ComponentUpdateCommand(expected_version=1, status="decommissioned")
    )
    service = CapabilityService(repository, clock=fixed_clock)
    with pytest.raises(ConflictError):
        await service.create(TENANT_A, component_id, command_capability(), "cap-key-000")


def test_capability_service_is_append_only() -> None:
    public = {name for name in dir(CapabilityService) if not name.startswith("_")}
    assert public == {"create", "list"}


def test_entity_snapshot_uses_fixed_width_utc_and_round_trips() -> None:
    device = DeviceV2(
        tenant_id=TENANT_A,
        device_id=GATEWAY,
        name="Gateway",
        created_at=datetime(2026, 8, 25, tzinfo=UTC),
        updated_at=datetime(2026, 8, 25, tzinfo=UTC),
        version=1,
    )
    snapshot = json.loads(entity_snapshot_json(device))
    assert snapshot["updated_at"] == "2026-08-25T00:00:00.000000Z"
    assert snapshot["status"] == "active"
    assert DeviceV2.model_validate(snapshot) == device


def test_capability_schema_at_size_limit_is_accepted() -> None:
    padding = 32 * 1024 - len('{"d":""}')
    assert command_capability(parameter_schema={"d": "x" * padding}).parameter_schema


@pytest.mark.parametrize(
    "profile",
    [
        {"measurement_metric_keys": ["k"] * 65},
        {"measurement_metric_keys": ["k" * 129]},
        {"measurement_metric_keys": [""]},
    ],
)
def test_probe_profile_metric_keys_are_bounded(profile: dict) -> None:
    with pytest.raises(ValidationError):
        ComponentCreateCommand(kind="probe", name="P", probe_profile=profile)
    assert ComponentCreateCommand(
        kind="probe", name="P", probe_profile={"measurement_metric_keys": ["k" * 128] * 64}
    )
