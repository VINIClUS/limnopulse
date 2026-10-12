import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

from limnopulse_api.core.errors import ConflictError
from limnopulse_api.domain.ids import require_device_id, require_safe_id

IDEMPOTENCY_RECEIPT_TTL = timedelta(hours=24)
SHA256_HEX_PATTERN = r"^[0-9a-f]{64}$"
CAPABILITY_KEY_PATTERN = r"^[a-z][a-z0-9]*(?:[._][a-z0-9]+)*$"
PROVIDER_KEY_PATTERN = r"^[a-z][a-z0-9_]*(?:\.[a-z0-9_]+)*$"
IDEMPOTENCY_SCOPE_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$"

SafeId = Annotated[str, AfterValidator(require_safe_id)]
DeviceId = Annotated[str, AfterValidator(require_device_id)]
Sha256Hex = Annotated[str, Field(pattern=SHA256_HEX_PATTERN)]
ProviderKey = Annotated[str, Field(pattern=PROVIDER_KEY_PATTERN)]


class QuotaDimension(StrEnum):
    SITES = "sites"
    ASSETS = "assets"
    DEVICES = "devices"
    COMPONENTS = "components"
    NOTIFICATION_DESTINATIONS = "notification_destinations"


def require_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{field_name} must be UTC")
    return value


def format_utc_timestamp(value: datetime) -> str:
    """Serialize an aware datetime as fixed-width UTC `YYYY-MM-DDTHH:MM:SS.ffffffZ`."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    utc = value.astimezone(UTC)
    return (
        f"{utc.year:04d}-{utc.month:02d}-{utc.day:02d}T"
        f"{utc.hour:02d}:{utc.minute:02d}:{utc.second:02d}.{utc.microsecond:06d}Z"
    )


def ensure_status_transition[StatusT: StrEnum](
    current: StatusT,
    target: StatusT,
    allowed: Mapping[StatusT, frozenset[StatusT]],
    *,
    entity: str,
) -> None:
    if current == target:
        return
    if target not in allowed.get(current, frozenset()):
        raise ConflictError(f"{entity} cannot transition from {current} to {target}")


class V2VersionedEntity(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    created_at: datetime
    updated_at: datetime
    version: int = Field(ge=1)
    schema_version: Literal[2] = 2

    @model_validator(mode="after")
    def validate_utc_version_window(self) -> "V2VersionedEntity":
        if self.created_at.tzinfo is None or self.updated_at.tzinfo is None:
            raise ValueError("timestamps must be timezone-aware")
        utc = timedelta(0)
        if self.created_at.utcoffset() != utc or self.updated_at.utcoffset() != utc:
            raise ValueError("timestamps must be UTC")
        if self.created_at > self.updated_at:
            raise ValueError("created_at must not be after updated_at")
        return self


class IdempotencyReceipt(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    tenant_id: SafeId
    scope: str = Field(pattern=IDEMPOTENCY_SCOPE_PATTERN)
    key_hash: Sha256Hex
    request_hash: Sha256Hex
    response_snapshot_json: str
    created_at: datetime
    expires_at: datetime

    @model_validator(mode="after")
    def validate_receipt(self) -> "IdempotencyReceipt":
        require_utc(self.created_at, "created_at")
        require_utc(self.expires_at, "expires_at")
        if self.expires_at != self.created_at + IDEMPOTENCY_RECEIPT_TTL:
            raise ValueError("expires_at must be exactly 24 hours after created_at")
        try:
            json.loads(self.response_snapshot_json)
        except ValueError as exc:
            raise ValueError("response_snapshot_json must be valid JSON") from exc
        return self


def new_idempotency_receipt(
    *,
    tenant_id: str,
    scope: str,
    key_hash: str,
    request_hash: str,
    response_snapshot_json: str,
    created_at: datetime,
) -> IdempotencyReceipt:
    return IdempotencyReceipt(
        tenant_id=tenant_id,
        scope=scope,
        key_hash=key_hash,
        request_hash=request_hash,
        response_snapshot_json=response_snapshot_json,
        created_at=created_at,
        expires_at=created_at + IDEMPOTENCY_RECEIPT_TTL,
    )
