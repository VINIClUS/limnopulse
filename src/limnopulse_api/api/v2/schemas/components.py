from pydantic import BaseModel

from limnopulse_api.api.v2.schemas.common import V2VersionedResponse
from limnopulse_api.domain.devices_v2 import ComponentKind, ComponentStatus, DeviceComponent
from limnopulse_api.services.components import ComponentCreateCommand, ComponentUpdateCommand


class V2ComponentCreateRequest(ComponentCreateCommand):
    """`kind`, profiles and model profile are fixed at creation; profile IDs are generated."""


class V2ComponentUpdateRequest(ComponentUpdateCommand):
    """Only `name` and `status` are mutable; immutable fields are rejected (extra=forbid)."""


class V2ProbeProfileResponse(BaseModel):
    probe_profile_id: str
    serial_number: str | None = None
    measurement_metric_keys: list[str]


class V2ActuatorProfileResponse(BaseModel):
    actuator_profile_id: str
    actuator_type: str


class V2ComponentResponse(V2VersionedResponse):
    component_id: str
    device_id: str
    kind: ComponentKind
    name: str
    model_profile_key: str | None = None
    probe_profile: V2ProbeProfileResponse | None = None
    actuator_profile: V2ActuatorProfileResponse | None = None
    status: ComponentStatus

    @classmethod
    def from_domain(cls, component: DeviceComponent) -> "V2ComponentResponse":
        return cls.model_validate(component.model_dump())


class V2ComponentListResponse(BaseModel):
    items: list[V2ComponentResponse]
