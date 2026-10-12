import json
from collections.abc import Callable
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from limnopulse_api.adapters.dynamodb_v2 import utc_now
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.devices_v2 import (
    CapabilityAvailability,
    CapabilityDeclaration,
    CapabilityOperation,
    CapabilityProvenance,
    CapabilitySupport,
    CommandRiskClass,
    ComponentStatus,
    DeviceComponent,
)
from limnopulse_api.domain.ids import new_capability_id
from limnopulse_api.domain.v2_common import CAPABILITY_KEY_PATTERN, require_utc
from limnopulse_api.repositories.v2_contracts import DeviceComponentRepository
from limnopulse_api.services.components import is_valid_component_id
from limnopulse_api.services.devices_v2 import (
    TEXT_MAX_LENGTH,
    CreateOutcome,
    create_idempotently,
)

CAPABILITY_CREATE_SCOPE = "capability-create"
SCHEMA_MAX_BYTES = 32 * 1024
_TERMINAL_COMPONENT_STATUSES = frozenset({ComponentStatus.REPLACED, ComponentStatus.DECOMMISSIONED})


class CapabilityCreateCommand(BaseModel):
    """Append-only declaration; `provider_implementation_key` is opaque provider-neutral text.

    The idempotency request hash is computed over the JSON form of the body, so schemas that
    differ only in number spelling (`1` vs `1.0`) are different requests.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    capability_key: str = Field(pattern=CAPABILITY_KEY_PATTERN, max_length=TEXT_MAX_LENGTH)
    operation: CapabilityOperation
    support: CapabilitySupport
    availability: CapabilityAvailability
    provenance: CapabilityProvenance
    parameter_schema: dict[str, object] | None = None
    result_schema: dict[str, object] | None = None
    provider_implementation_key: str | None = Field(
        default=None, min_length=1, max_length=TEXT_MAX_LENGTH
    )
    risk_class: CommandRiskClass | None = None
    verified_at: datetime | None = None

    @field_validator("parameter_schema", "result_schema", mode="after")
    @classmethod
    def require_bounded_json_object(cls, value: dict[str, object] | None) -> dict | None:
        if value is None:
            return None
        try:
            encoded = json.dumps(value, allow_nan=False, separators=(",", ":"))
        except (TypeError, ValueError) as exc:
            raise ValueError("schema must be a finite JSON object") from exc
        if len(encoded.encode()) > SCHEMA_MAX_BYTES:
            raise ValueError(f"schema must not exceed {SCHEMA_MAX_BYTES} bytes as JSON")
        return value

    @field_validator("provider_implementation_key", mode="after")
    @classmethod
    def reject_provider_resource_names(cls, value: str | None) -> str | None:
        if value is not None and value.strip().lower().startswith("arn:"):
            raise ValueError("provider_implementation_key must not be a provider resource name")
        return value

    @model_validator(mode="after")
    def validate_risk_and_verification(self) -> "CapabilityCreateCommand":
        if self.operation is CapabilityOperation.COMMAND and self.risk_class is None:
            raise ValueError("risk_class is required for command capabilities")
        if self.operation is CapabilityOperation.READ and self.risk_class is not None:
            raise ValueError("risk_class is not allowed for read capabilities")
        if self.verified_at is not None:
            require_utc(self.verified_at, "verified_at")
        return self


class CapabilityService:
    def __init__(
        self,
        repository: DeviceComponentRepository,
        *,
        clock: Callable[[], datetime] = utc_now,
        id_factory: Callable[[], str] = new_capability_id,
    ) -> None:
        self.repository = repository
        self.clock = clock
        self.id_factory = id_factory

    async def list(self, tenant_id: str, component_id: str) -> list[CapabilityDeclaration]:
        await self._require_component(tenant_id, component_id)
        return await self.repository.list_capabilities(tenant_id, component_id)

    async def create(
        self,
        tenant_id: str,
        component_id: str,
        command: CapabilityCreateCommand,
        idempotency_key: str,
    ) -> CreateOutcome[CapabilityDeclaration]:
        async def build() -> CapabilityDeclaration:
            component = await self._require_component(tenant_id, component_id)
            if component.status in _TERMINAL_COMPONENT_STATUSES:
                raise ConflictError(f"component is {component.status}")
            now = self.clock()
            return CapabilityDeclaration(
                tenant_id=tenant_id,
                component_id=component_id,
                capability_id=self.id_factory(),
                **command.model_dump(),
                created_at=now,
                updated_at=now,
                version=1,
            )

        return await create_idempotently(
            self.repository,
            tenant_id=tenant_id,
            scope=CAPABILITY_CREATE_SCOPE,
            idempotency_key=idempotency_key,
            request_payload={
                "tenant_id": tenant_id,
                "component_id": component_id,
                "body": command.model_dump(mode="json"),
            },
            entity_type=CapabilityDeclaration,
            build=build,
            persist=self.repository.create_capability,
            clock=self.clock,
        )

    async def _require_component(self, tenant_id: str, component_id: str) -> DeviceComponent:
        component = None
        if is_valid_component_id(component_id):
            component = await self.repository.get_component(tenant_id, component_id)
        if component is None:
            raise NotFoundError("component not found")
        return component
