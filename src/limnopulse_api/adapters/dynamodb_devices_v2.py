"""DynamoDB persistence for v2 Devices, Components and append-only Capability declarations.

Device v2 shares the `TENANT#<t>/DEVICE#<d>` row and `DEVICE#<d>/META` lookup with legacy `/v1`
Devices. Legacy rows keep `pond_id`/`auth_type` and a full-copy lookup; reads map every row
through an explicit allowlist and updates only SET mutable attributes, so `/v1` keeps working.
"""

import json
import logging
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from limnopulse_api.adapters.dynamodb_v2 import (
    DynamoV2KeyBuilder,
    DynamoV2RepositoryBase,
    parse_canonical_timestamp,
)
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.devices_v2 import (
    ActuatorProfile,
    CapabilityDeclaration,
    ComponentStatus,
    DeviceComponent,
    DeviceStatus,
    DeviceV2,
    ProbeProfile,
    ensure_component_transition,
    ensure_device_transition,
)
from limnopulse_api.domain.legacy_projection import (
    device_status_from_legacy,
    legacy_timestamp_to_utc,
)
from limnopulse_api.domain.v2_common import IdempotencyReceipt, format_utc_timestamp

DEVICE_ENTITY_TYPE = "device"
DEVICE_LOOKUP_ENTITY_TYPE = "device_lookup"
COMPONENT_ENTITY_TYPE = "component"
COMPONENT_MIRROR_ENTITY_TYPE = "component_mirror"
CAPABILITY_ENTITY_TYPE = "capability_declaration"
_TERMINAL_COMPONENT_STATUSES = frozenset({ComponentStatus.REPLACED, ComponentStatus.DECOMMISSIONED})
_KEYS = DynamoV2KeyBuilder()
_LOGGER = logging.getLogger(__name__)


def _without_none(item: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in item.items() if value is not None}


def _parse_timestamp(value: str) -> datetime:
    """Accept fixed-width v2 timestamps and legacy `isoformat()` values (naive read as UTC)."""
    return legacy_timestamp_to_utc(datetime.fromisoformat(value))


def _is_legacy_device_item(item: Mapping[str, Any]) -> bool:
    return "pond_id" in item


# Device codecs ---------------------------------------------------------------------------------


def device_v2_to_item(device: DeviceV2) -> dict[str, Any]:
    """Native v2 tenant row; never carries `pond_id`, `auth_type` or provider identity."""
    return _without_none(
        {
            **_KEYS.device(device.tenant_id, device.device_id),
            "entity_type": DEVICE_ENTITY_TYPE,
            "tenant_id": device.tenant_id,
            "device_id": device.device_id,
            "name": device.name,
            "model_profile_key": device.model_profile_key,
            "firmware_version": device.firmware_version,
            "compatibility_level": device.compatibility_level.value,
            "status": device.status.value,
            "created_at": format_utc_timestamp(device.created_at),
            "updated_at": format_utc_timestamp(device.updated_at),
            "version": device.version,
            "schema_version": 2,
        }
    )


def device_lookup_to_item(device: DeviceV2) -> dict[str, Any]:
    """Minimal global lookup: only tenant ownership, identity, status and version."""
    return {
        **_KEYS.device_lookup(device.device_id),
        "entity_type": DEVICE_LOOKUP_ENTITY_TYPE,
        "tenant_id": device.tenant_id,
        "device_id": device.device_id,
        "status": device.status.value,
        "version": device.version,
    }


def device_v2_from_item(item: Mapping[str, Any]) -> DeviceV2:
    """Map a native v2 or legacy `/v1` Device row through an explicit allowlist."""
    values: dict[str, Any] = {
        "tenant_id": item["tenant_id"],
        "device_id": item["device_id"],
        "name": item["name"],
        "firmware_version": item.get("firmware_version"),
        "status": device_status_from_legacy(str(item.get("status", "active"))),
        "created_at": _parse_timestamp(item["created_at"]),
        "updated_at": _parse_timestamp(item["updated_at"]),
        "version": int(item["version"]),
    }
    if item.get("model_profile_key") is not None:
        values["model_profile_key"] = item["model_profile_key"]
    if item.get("compatibility_level") is not None:
        values["compatibility_level"] = item["compatibility_level"]
    return DeviceV2(**values)


# Component codecs ------------------------------------------------------------------------------


def _probe_profile_to_item(profile: ProbeProfile | None) -> dict[str, Any] | None:
    if profile is None:
        return None
    return _without_none(
        {
            "probe_profile_id": profile.probe_profile_id,
            "serial_number": profile.serial_number,
            "measurement_metric_keys": list(profile.measurement_metric_keys),
        }
    )


def _actuator_profile_to_item(profile: ActuatorProfile | None) -> dict[str, Any] | None:
    if profile is None:
        return None
    return {
        "actuator_profile_id": profile.actuator_profile_id,
        "actuator_type": profile.actuator_type,
    }


def component_to_item(component: DeviceComponent, *, canonical: bool = True) -> dict[str, Any]:
    """Canonical `COMPONENT#<c>/META` row, or the `TENANT#<t>/COMPONENT#<c>` mirror."""
    if canonical:
        key = _KEYS.component(component.component_id)
        entity_type = COMPONENT_ENTITY_TYPE
    else:
        key = _KEYS.component_mirror(component.tenant_id, component.component_id)
        entity_type = COMPONENT_MIRROR_ENTITY_TYPE
    return _without_none(
        {
            **key,
            "entity_type": entity_type,
            "tenant_id": component.tenant_id,
            "component_id": component.component_id,
            "device_id": component.device_id,
            "kind": component.kind.value,
            "name": component.name,
            "model_profile_key": component.model_profile_key,
            "probe_profile": _probe_profile_to_item(component.probe_profile),
            "actuator_profile": _actuator_profile_to_item(component.actuator_profile),
            "status": component.status.value,
            "created_at": format_utc_timestamp(component.created_at),
            "updated_at": format_utc_timestamp(component.updated_at),
            "version": component.version,
            "schema_version": 2,
        }
    )


def component_from_item(item: Mapping[str, Any]) -> DeviceComponent:
    probe = item.get("probe_profile")
    actuator = item.get("actuator_profile")
    return DeviceComponent(
        tenant_id=item["tenant_id"],
        component_id=item["component_id"],
        device_id=item["device_id"],
        kind=item["kind"],
        name=item["name"],
        model_profile_key=item.get("model_profile_key"),
        probe_profile=(
            None
            if probe is None
            else ProbeProfile(
                probe_profile_id=probe["probe_profile_id"],
                serial_number=probe.get("serial_number"),
                measurement_metric_keys=tuple(probe.get("measurement_metric_keys") or ()),
            )
        ),
        actuator_profile=(
            None
            if actuator is None
            else ActuatorProfile(
                actuator_profile_id=actuator["actuator_profile_id"],
                actuator_type=actuator["actuator_type"],
            )
        ),
        status=item["status"],
        created_at=parse_canonical_timestamp(item["created_at"]),
        updated_at=parse_canonical_timestamp(item["updated_at"]),
        version=int(item["version"]),
    )


# Capability codecs -----------------------------------------------------------------------------


def _schema_to_json(schema: Mapping[str, object] | None) -> str | None:
    # Stored as canonical JSON text: DynamoDB numbers would collapse 1.0 into 1.
    if schema is None:
        return None
    return json.dumps(schema, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _schema_from_json(value: str | None) -> dict[str, object] | None:
    return None if value is None else json.loads(value)


def capability_to_item(declaration: CapabilityDeclaration) -> dict[str, Any]:
    return _without_none(
        {
            **_KEYS.capability(declaration.component_id, declaration.capability_id),
            "entity_type": CAPABILITY_ENTITY_TYPE,
            "tenant_id": declaration.tenant_id,
            "component_id": declaration.component_id,
            "capability_id": declaration.capability_id,
            "capability_key": declaration.capability_key,
            "operation": declaration.operation.value,
            "support": declaration.support.value,
            "availability": declaration.availability.value,
            "provenance": declaration.provenance.value,
            "parameter_schema_json": _schema_to_json(declaration.parameter_schema),
            "result_schema_json": _schema_to_json(declaration.result_schema),
            "provider_implementation_key": declaration.provider_implementation_key,
            "risk_class": None if declaration.risk_class is None else declaration.risk_class.value,
            "verified_at": (
                None
                if declaration.verified_at is None
                else format_utc_timestamp(declaration.verified_at)
            ),
            "created_at": format_utc_timestamp(declaration.created_at),
            "updated_at": format_utc_timestamp(declaration.updated_at),
            "version": declaration.version,
            "schema_version": 2,
        }
    )


def capability_from_item(item: Mapping[str, Any]) -> CapabilityDeclaration:
    verified_at = item.get("verified_at")
    return CapabilityDeclaration(
        tenant_id=item["tenant_id"],
        component_id=item["component_id"],
        capability_id=item["capability_id"],
        capability_key=item["capability_key"],
        operation=item["operation"],
        support=item["support"],
        availability=item["availability"],
        provenance=item["provenance"],
        parameter_schema=_schema_from_json(item.get("parameter_schema_json")),
        result_schema=_schema_from_json(item.get("result_schema_json")),
        provider_implementation_key=item.get("provider_implementation_key"),
        risk_class=item.get("risk_class"),
        verified_at=None if verified_at is None else parse_canonical_timestamp(verified_at),
        created_at=parse_canonical_timestamp(item["created_at"]),
        updated_at=parse_canonical_timestamp(item["updated_at"]),
        version=int(item["version"]),
    )


# Repository ------------------------------------------------------------------------------------


class DynamoDeviceComponentRepository(DynamoV2RepositoryBase):
    """Implements `DeviceComponentRepository`, `DeviceReader` and `ComponentReader`."""

    # Devices ---------------------------------------------------------------------------------

    async def get_device(
        self, tenant_id: str, device_id: str, *, consistent: bool = False
    ) -> DeviceV2 | None:
        """M1 reads stay eventually consistent; `consistent=True` is the adapter-only P3 read
        (`ConsistentDeviceReader`) used by admission and the fence."""
        item = await self.get_item(self.keys.device(tenant_id, device_id), consistent=consistent)
        if item is None or item.get("tenant_id") != tenant_id:
            return None
        return device_v2_from_item(item)

    async def list_devices(self, tenant_id: str) -> list[DeviceV2]:
        items = await self.query_prefix(
            self.keys.tenant_partition(tenant_id), DynamoV2KeyBuilder.DEVICE_PREFIX
        )
        devices = []
        for item in items:
            if item.get("tenant_id") != tenant_id:
                continue
            try:
                devices.append(device_v2_from_item(item))
            except (KeyError, TypeError, ValueError):
                # One malformed historical row must not hide the rest; log only the key.
                _LOGGER.warning(
                    "skipping unmappable device row PK=%s SK=%s", item.get("PK"), item.get("SK")
                )
        return devices

    async def create_device(self, device: DeviceV2, receipt: IdempotencyReceipt) -> DeviceV2:
        await self.transact_write(
            [
                {"Put": self.put_new_request(device_v2_to_item(device))},
                {"Put": self.put_new_request(device_lookup_to_item(device))},
                {"Put": self.receipt_put_request(receipt)},
            ]
        )
        return device

    async def update_device(
        self,
        tenant_id: str,
        device_id: str,
        expected_version: int,
        *,
        name: str | None,
        firmware_version: str | None,
        status: DeviceStatus | None,
    ) -> DeviceV2:
        item = await self.get_item(self.keys.device(tenant_id, device_id), consistent=True)
        if item is None or item.get("tenant_id") != tenant_id:
            raise NotFoundError("device not found")
        if int(item["version"]) != expected_version:
            raise ConflictError("device version conflict")
        current = device_v2_from_item(item)
        if status is not None:
            ensure_device_transition(current.status, status)

        legacy = _is_legacy_device_item(item)
        # Never move `updated_at` backwards (clock skew), which would make the row invalid.
        now = max(self.clock(), current.updated_at)
        # Legacy `/v1` rows keep their second-precision `isoformat()` timestamps.
        updated_at = now.replace(microsecond=0).isoformat() if legacy else format_utc_timestamp(now)
        changes: dict[str, Any] = _without_none(
            {
                "name": name,
                "firmware_version": firmware_version,
                "status": None if status is None else status.value,
            }
        )
        row_changes = {**changes, "updated_at": updated_at, "version": expected_version + 1}
        # A legacy lookup is a full copy; a native lookup holds only status and version.
        lookup_changes = (
            row_changes
            if legacy
            else _without_none(
                {
                    "status": changes.get("status"),
                    "version": expected_version + 1,
                }
            )
        )
        updated = device_v2_from_item({**item, **row_changes})
        await self.transact_write(
            [
                {
                    "Update": self._fenced_set_request(
                        self.keys.device(tenant_id, device_id),
                        row_changes,
                        expected_version=expected_version,
                        tenant_id=tenant_id,
                    )
                },
                {
                    "Update": self._fenced_set_request(
                        self.keys.device_lookup(device_id),
                        lookup_changes,
                        expected_version=expected_version,
                        tenant_id=tenant_id,
                    )
                },
            ]
        )
        return updated

    # Components ------------------------------------------------------------------------------

    async def get_component(self, tenant_id: str, component_id: str) -> DeviceComponent | None:
        item = await self.get_item(self.keys.component(component_id))
        if item is None or item.get("tenant_id") != tenant_id:
            return None
        return component_from_item(item)

    async def list_components(self, tenant_id: str, device_id: str) -> list[DeviceComponent]:
        items = await self.query_prefix(
            self.keys.tenant_partition(tenant_id), DynamoV2KeyBuilder.COMPONENT_PREFIX
        )
        return [
            component_from_item(item)
            for item in items
            if item.get("tenant_id") == tenant_id and item.get("device_id") == device_id
        ]

    async def create_component(
        self, component: DeviceComponent, receipt: IdempotencyReceipt
    ) -> DeviceComponent:
        device_key = self.keys.device(component.tenant_id, component.device_id)
        device_item = await self.get_item(device_key, consistent=True)
        if device_item is None or device_item.get("tenant_id") != component.tenant_id:
            raise NotFoundError("device not found")
        device = device_v2_from_item(device_item)
        if device.status is DeviceStatus.DECOMMISSIONED:
            raise ConflictError("device is decommissioned")
        await self.transact_write(
            [
                {"Put": self.put_new_request(component_to_item(component, canonical=False))},
                {"Put": self.put_new_request(component_to_item(component, canonical=True))},
                # Fence the parent Device on the observed version (decommission race).
                {"ConditionCheck": self.version_check_request(device_key, device.version)},
                {"Put": self.receipt_put_request(receipt)},
            ]
        )
        return component

    async def update_component(
        self,
        tenant_id: str,
        component_id: str,
        expected_version: int,
        *,
        name: str | None,
        status: ComponentStatus | None,
    ) -> DeviceComponent:
        item = await self.get_item(self.keys.component(component_id), consistent=True)
        if item is None or item.get("tenant_id") != tenant_id:
            raise NotFoundError("component not found")
        if int(item["version"]) != expected_version:
            raise ConflictError("component version conflict")
        current = component_from_item(item)
        if status is not None:
            ensure_component_transition(current.status, status)
        updated = DeviceComponent(
            **{
                **current.model_dump(),
                **_without_none({"name": name, "status": status}),
                "updated_at": max(self.clock(), current.updated_at),
                "version": expected_version + 1,
            }
        )
        changes = _without_none(
            {
                "name": name,
                "status": None if status is None else status.value,
                "updated_at": format_utc_timestamp(updated.updated_at),
                "version": updated.version,
            }
        )
        await self.transact_write(
            [
                {
                    "Update": self._fenced_set_request(
                        self.keys.component_mirror(tenant_id, component_id),
                        changes,
                        expected_version=expected_version,
                        tenant_id=tenant_id,
                    )
                },
                {
                    "Update": self._fenced_set_request(
                        self.keys.component(component_id),
                        changes,
                        expected_version=expected_version,
                        tenant_id=tenant_id,
                    )
                },
            ]
        )
        return updated

    # Capabilities ----------------------------------------------------------------------------

    async def list_capabilities(
        self, tenant_id: str, component_id: str
    ) -> list[CapabilityDeclaration]:
        if await self.get_component(tenant_id, component_id) is None:
            return []
        items = await self.query_prefix(
            self.keys.component(component_id)["PK"], DynamoV2KeyBuilder.CAPABILITY_PREFIX
        )
        return [capability_from_item(item) for item in items if item.get("tenant_id") == tenant_id]

    async def create_capability(
        self, declaration: CapabilityDeclaration, receipt: IdempotencyReceipt
    ) -> CapabilityDeclaration:
        component_key = self.keys.component(declaration.component_id)
        item = await self.get_item(component_key, consistent=True)
        if item is None or item.get("tenant_id") != declaration.tenant_id:
            raise NotFoundError("component not found")
        component = component_from_item(item)
        if component.status in _TERMINAL_COMPONENT_STATUSES:
            raise ConflictError(f"component is {component.status}")
        await self.transact_write(
            [
                {"ConditionCheck": self.version_check_request(component_key, component.version)},
                {"Put": self.put_new_request(capability_to_item(declaration))},
                {"Put": self.receipt_put_request(receipt)},
            ]
        )
        return declaration

    # Internals -------------------------------------------------------------------------------

    def _fenced_set_request(
        self,
        key: Mapping[str, str],
        changes: Mapping[str, Any],
        *,
        expected_version: int,
        tenant_id: str,
    ) -> dict[str, Any]:
        names = {"#tenant_id": "tenant_id", "#version": "version"}
        values: dict[str, Any] = {":expected_version": expected_version, ":tenant_id": tenant_id}
        assignments = []
        for index, (attribute, value) in enumerate(changes.items()):
            names[f"#f{index}"] = attribute
            values[f":v{index}"] = value
            assignments.append(f"#f{index} = :v{index}")
        return self.update_request(
            key,
            update_expression="SET " + ", ".join(assignments),
            condition=(
                "attribute_exists(PK) AND #version = :expected_version AND #tenant_id = :tenant_id"
            ),
            names=names,
            values=values,
        )
