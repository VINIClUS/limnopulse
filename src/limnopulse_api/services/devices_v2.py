"""Provider-neutral Device v2 service plus the create/idempotency helpers shared by
`services.components` and `services.capabilities`."""

import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from limnopulse_api.adapters.dynamodb_v2 import (
    canonical_request_hash,
    idempotency_key_hash,
    utc_now,
)
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.devices_v2 import (
    CompatibilityLevel,
    DeviceStatus,
    DeviceV2,
    ensure_device_transition,
)
from limnopulse_api.domain.ids import new_device_id, require_device_id
from limnopulse_api.domain.v2_common import (
    IdempotencyReceipt,
    format_utc_timestamp,
    new_idempotency_receipt,
)
from limnopulse_api.repositories.v2_contracts import DeviceComponentRepository

DEVICE_CREATE_SCOPE = "device-create"
NAME_MAX_LENGTH = 120
TEXT_MAX_LENGTH = 256


@dataclass(frozen=True)
class CreateOutcome[EntityT: BaseModel]:
    """A created (or replayed) entity together with the exact response snapshot to return."""

    entity: EntityT
    snapshot_json: str
    replayed: bool


def _snapshot_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return format_utc_timestamp(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {key: _snapshot_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_snapshot_value(item) for item in value]
    return value


def entity_snapshot_json(entity: BaseModel) -> str:
    """JSON body of a `/v2` response: the entity's fields with fixed-width UTC timestamps."""
    return json.dumps(_snapshot_value(entity.model_dump()), allow_nan=False)


def is_valid_device_id(device_id: str) -> bool:
    try:
        require_device_id(device_id)
    except ValueError:
        return False
    return True


async def create_idempotently[EntityT: BaseModel](
    repository: Any,
    *,
    tenant_id: str,
    scope: str,
    idempotency_key: str,
    request_payload: Mapping[str, Any],
    entity_type: type[EntityT],
    build: Callable[[], Awaitable[EntityT]],
    persist: Callable[[EntityT, IdempotencyReceipt], Awaitable[EntityT]],
    clock: Callable[[], datetime],
) -> CreateOutcome[EntityT]:
    """Replay a live receipt first; otherwise write the entity and its receipt atomically."""
    key_hash = idempotency_key_hash(idempotency_key)
    request_hash = canonical_request_hash({"scope": scope, **request_payload})
    snapshot = await repository.get_idempotency_replay(tenant_id, scope, key_hash, request_hash)
    if snapshot is not None:
        return _replayed(entity_type, snapshot)
    entity = await build()
    snapshot_json = entity_snapshot_json(entity)
    receipt = new_idempotency_receipt(
        tenant_id=tenant_id,
        scope=scope,
        key_hash=key_hash,
        request_hash=request_hash,
        response_snapshot_json=snapshot_json,
        created_at=clock(),
    )
    try:
        created = await persist(entity, receipt)
    except ConflictError:
        # A concurrent request with the same key may have won the receipt race.
        snapshot = await repository.get_idempotency_replay(
            tenant_id, scope, key_hash, request_hash
        )
        if snapshot is None:
            raise
        return _replayed(entity_type, snapshot)
    return CreateOutcome(entity=created, snapshot_json=snapshot_json, replayed=False)


def _replayed[EntityT: BaseModel](
    entity_type: type[EntityT], snapshot_json: str
) -> CreateOutcome[EntityT]:
    return CreateOutcome(
        entity=entity_type.model_validate_json(snapshot_json),
        snapshot_json=snapshot_json,
        replayed=True,
    )


class DeviceV2CreateCommand(BaseModel):
    """Client-supplied Device fields; identity, tenant and lifecycle are server-owned."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1, max_length=NAME_MAX_LENGTH)
    model_profile_key: str | None = Field(default=None, min_length=1, max_length=TEXT_MAX_LENGTH)
    firmware_version: str | None = Field(default=None, min_length=1, max_length=TEXT_MAX_LENGTH)
    compatibility_level: CompatibilityLevel = CompatibilityLevel.CUSTOM


class DeviceV2UpdateCommand(BaseModel):
    """`None` leaves a field unchanged; clearing optional fields is unsupported in M1."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    expected_version: int = Field(ge=1)
    name: str | None = Field(default=None, min_length=1, max_length=NAME_MAX_LENGTH)
    firmware_version: str | None = Field(default=None, min_length=1, max_length=TEXT_MAX_LENGTH)
    status: DeviceStatus | None = None


class DeviceV2Service:
    def __init__(
        self,
        repository: DeviceComponentRepository,
        *,
        clock: Callable[[], datetime] = utc_now,
        id_factory: Callable[[], str] = new_device_id,
    ) -> None:
        self.repository = repository
        self.clock = clock
        self.id_factory = id_factory

    async def list(self, tenant_id: str) -> list[DeviceV2]:
        return await self.repository.list_devices(tenant_id)

    async def get(self, tenant_id: str, device_id: str) -> DeviceV2 | None:
        if not is_valid_device_id(device_id):
            return None
        return await self.repository.get_device(tenant_id, device_id)

    async def create(
        self, tenant_id: str, command: DeviceV2CreateCommand, idempotency_key: str
    ) -> CreateOutcome[DeviceV2]:
        async def build() -> DeviceV2:
            now = self.clock()
            return DeviceV2(
                tenant_id=tenant_id,
                device_id=self.id_factory(),
                name=command.name,
                model_profile_key=command.model_profile_key,
                firmware_version=command.firmware_version,
                compatibility_level=command.compatibility_level,
                status=DeviceStatus.ACTIVE,
                created_at=now,
                updated_at=now,
                version=1,
            )

        return await create_idempotently(
            self.repository,
            tenant_id=tenant_id,
            scope=DEVICE_CREATE_SCOPE,
            idempotency_key=idempotency_key,
            request_payload={"tenant_id": tenant_id, "body": command.model_dump(mode="json")},
            entity_type=DeviceV2,
            build=build,
            persist=self.repository.create_device,
            clock=self.clock,
        )

    async def update(
        self, tenant_id: str, device_id: str, command: DeviceV2UpdateCommand
    ) -> DeviceV2:
        current = await self.get(tenant_id, device_id)
        if current is None:
            raise NotFoundError("device not found")
        if command.status is not None:
            ensure_device_transition(current.status, command.status)
        return await self.repository.update_device(
            tenant_id,
            device_id,
            command.expected_version,
            name=command.name,
            firmware_version=command.firmware_version,
            status=command.status,
        )
