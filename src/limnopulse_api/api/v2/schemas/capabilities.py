from typing import Literal

from pydantic import BaseModel

from limnopulse_api.api.v2.schemas.common import UtcTimestamp
from limnopulse_api.domain.devices_v2 import (
    CapabilityAvailability,
    CapabilityDeclaration,
    CapabilityOperation,
    CapabilityProvenance,
    CapabilitySupport,
    CommandRiskClass,
)
from limnopulse_api.services.capabilities import CapabilityCreateCommand


class V2CapabilityCreateRequest(CapabilityCreateCommand):
    """Append-only declaration; there is no PATCH or DELETE for capabilities in M1."""


class V2CapabilityResponse(BaseModel):
    tenant_id: str
    component_id: str
    capability_id: str
    capability_key: str
    operation: CapabilityOperation
    support: CapabilitySupport
    availability: CapabilityAvailability
    provenance: CapabilityProvenance
    parameter_schema: dict[str, object] | None = None
    result_schema: dict[str, object] | None = None
    provider_implementation_key: str | None = None
    risk_class: CommandRiskClass | None = None
    verified_at: UtcTimestamp | None = None
    schema_version: Literal[2] = 2
    created_at: UtcTimestamp
    updated_at: UtcTimestamp
    version: int

    @classmethod
    def from_domain(cls, declaration: CapabilityDeclaration) -> "V2CapabilityResponse":
        return cls.model_validate(declaration.model_dump())


class V2CapabilityListResponse(BaseModel):
    items: list[V2CapabilityResponse]
