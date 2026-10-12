import json
from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from limnopulse_api.domain.v2_common import (
    CAPABILITY_KEY_PATTERN,
    DeviceId,
    SafeId,
    V2VersionedEntity,
    ensure_status_transition,
    require_utc,
)


class DeviceStatus(StrEnum):
    PROVISIONING = "provisioning"
    ACTIVE = "active"
    SUSPENDED = "suspended"
    DECOMMISSIONED = "decommissioned"


class ComponentKind(StrEnum):
    PROBE = "probe"
    ACTUATOR = "actuator"
    HYBRID = "hybrid"


class ComponentStatus(StrEnum):
    ACTIVE = "active"
    REPLACED = "replaced"
    DECOMMISSIONED = "decommissioned"


class CapabilityOperation(StrEnum):
    READ = "read"
    WRITE = "write"
    COMMAND = "command"
    CALIBRATION = "calibration"


class CapabilitySupport(StrEnum):
    SUPPORTED = "supported"
    UNSUPPORTED = "unsupported"
    UNKNOWN = "unknown"


class CapabilityAvailability(StrEnum):
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    UNKNOWN = "unknown"


class CapabilityProvenance(StrEnum):
    MODEL_PROFILE = "model_profile"
    INTEGRATION = "integration"
    DISCOVERED = "discovered"
    INSTANCE_OVERRIDE = "instance_override"


class CommandRiskClass(StrEnum):
    R0 = "r0"
    R1 = "r1"
    R2 = "r2"
    R3 = "r3"
    R4 = "r4"


class CompatibilityLevel(StrEnum):
    CERTIFIED = "certified"
    COMPATIBLE = "compatible"
    CUSTOM = "custom"


_DEVICE_TRANSITIONS = {
    DeviceStatus.PROVISIONING: frozenset(
        {DeviceStatus.ACTIVE, DeviceStatus.SUSPENDED, DeviceStatus.DECOMMISSIONED}
    ),
    DeviceStatus.ACTIVE: frozenset({DeviceStatus.SUSPENDED, DeviceStatus.DECOMMISSIONED}),
    DeviceStatus.SUSPENDED: frozenset({DeviceStatus.ACTIVE, DeviceStatus.DECOMMISSIONED}),
}
# REPLACED and DECOMMISSIONED are both terminal for a Component.
_COMPONENT_TRANSITIONS = {
    ComponentStatus.ACTIVE: frozenset({ComponentStatus.REPLACED, ComponentStatus.DECOMMISSIONED}),
}


class DeviceV2(V2VersionedEntity):
    tenant_id: SafeId
    device_id: DeviceId
    name: str
    model_profile_key: str | None = None
    firmware_version: str | None = None
    compatibility_level: CompatibilityLevel = CompatibilityLevel.CUSTOM
    status: DeviceStatus = DeviceStatus.ACTIVE


class ProbeProfile(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    probe_profile_id: SafeId
    serial_number: str | None = None
    measurement_metric_keys: tuple[str, ...] = ()


class ActuatorProfile(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    actuator_profile_id: SafeId
    actuator_type: str


class DeviceComponent(V2VersionedEntity):
    tenant_id: SafeId
    component_id: SafeId
    device_id: DeviceId
    kind: ComponentKind
    name: str
    model_profile_key: str | None = None
    probe_profile: ProbeProfile | None = None
    actuator_profile: ActuatorProfile | None = None
    status: ComponentStatus = ComponentStatus.ACTIVE

    @model_validator(mode="after")
    def validate_kind_profiles(self) -> "DeviceComponent":
        needs_probe = self.kind in {ComponentKind.PROBE, ComponentKind.HYBRID}
        needs_actuator = self.kind in {ComponentKind.ACTUATOR, ComponentKind.HYBRID}
        if needs_probe != (self.probe_profile is not None):
            raise ValueError(f"{self.kind} component probe_profile mismatch")
        if needs_actuator != (self.actuator_profile is not None):
            raise ValueError(f"{self.kind} component actuator_profile mismatch")
        return self


class CapabilityDeclaration(V2VersionedEntity):
    tenant_id: SafeId
    component_id: SafeId
    capability_id: SafeId
    capability_key: str = Field(pattern=CAPABILITY_KEY_PATTERN)
    operation: CapabilityOperation
    support: CapabilitySupport
    availability: CapabilityAvailability
    provenance: CapabilityProvenance
    parameter_schema: dict[str, object] | None = None
    result_schema: dict[str, object] | None = None
    provider_implementation_key: str | None = None
    risk_class: CommandRiskClass | None = None
    verified_at: datetime | None = None

    @field_validator("parameter_schema", "result_schema", mode="after")
    @classmethod
    def copy_json_schema(cls, value: dict[str, object] | None) -> dict[str, object] | None:
        if value is None:
            return None
        try:
            return json.loads(json.dumps(value, allow_nan=False))
        except (TypeError, ValueError) as exc:
            raise ValueError("schema must be a JSON object") from exc

    @model_validator(mode="after")
    def validate_risk_and_verification(self) -> "CapabilityDeclaration":
        if self.operation is CapabilityOperation.COMMAND and self.risk_class is None:
            raise ValueError("risk_class is required for command capabilities")
        if self.operation is CapabilityOperation.READ and self.risk_class is not None:
            raise ValueError("risk_class is not allowed for read capabilities")
        if self.verified_at is not None:
            require_utc(self.verified_at, "verified_at")
        return self


def ensure_device_transition(current: DeviceStatus, target: DeviceStatus) -> None:
    ensure_status_transition(current, target, _DEVICE_TRANSITIONS, entity="device")


def ensure_component_transition(current: ComponentStatus, target: ComponentStatus) -> None:
    ensure_status_transition(current, target, _COMPONENT_TRANSITIONS, entity="component")
