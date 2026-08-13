from datetime import datetime
from enum import StrEnum
from hashlib import sha256
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TelegramBindingStatus(StrEnum):
    PENDING = "pending"
    VERIFIED = "verified"
    REVOKED = "revoked"
    INVALIDATED = "invalidated"
    CONSUMED = "consumed"
    EXPIRED = "expired"


class TelegramDestinationStatus(StrEnum):
    ACTIVE = "active"
    SUPPRESSED = "suppressed"


class TelegramBinding(BaseModel):
    model_config = ConfigDict(frozen=True)

    tenant_id: str
    recipient_id: str
    destination_id: str
    status: Literal[TelegramBindingStatus.VERIFIED, TelegramBindingStatus.REVOKED]
    verified_at: datetime
    revoked_at: datetime | None
    version: int = Field(ge=1)
    created_at: datetime
    updated_at: datetime


class TelegramBindingRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    request_id: str
    tenant_id: str
    recipient_id: str
    token_hash: str
    status: Literal[
        TelegramBindingStatus.PENDING,
        TelegramBindingStatus.INVALIDATED,
        TelegramBindingStatus.CONSUMED,
        TelegramBindingStatus.EXPIRED,
    ]
    expires_at: datetime
    consumed_at: datetime | None = None
    created_at: datetime

    @field_validator("token_hash")
    @classmethod
    def validate_token_hash(cls, value: str) -> str:
        if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
            raise ValueError("token hash must be a lowercase SHA-256 digest")
        return value


class TelegramDestination(BaseModel):
    model_config = ConfigDict(frozen=True)

    destination_id: str
    recipient_id: str
    chat_id: int = Field(gt=0)
    status: TelegramDestinationStatus
    suppression_reason: str | None = None
    stopped_at: datetime | None = None
    last_update_id: int = Field(default=0, ge=0)
    version: int = Field(ge=1)
    created_at: datetime
    updated_at: datetime

    @staticmethod
    def id_for_chat(chat_id: int) -> str:
        if chat_id <= 0:
            raise ValueError("private Telegram chat ID must be positive")
        return sha256(str(chat_id).encode("ascii")).hexdigest()


class TelegramEligibilityFence(BaseModel):
    model_config = ConfigDict(frozen=True)

    tenant_id: str
    recipient_id: str
    destination_id: str
    chat_id: int = Field(gt=0)
    binding_version: int = Field(ge=1)
    destination_version: int = Field(ge=1)


class TelegramBindingView(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: Literal["absent", "pending", "verified", "suppressed", "revoked"]
    version: int | None = None
    verified_at: datetime | None = None
    pending_request_id: str | None = None
    pending_expires_at: datetime | None = None
    effective_enabled: bool = False


def telegram_is_effectively_enabled(
    enabled: bool,
    binding: TelegramBinding | None,
    destination: TelegramDestination | None,
) -> bool:
    return bool(
        enabled
        and binding is not None
        and binding.status is TelegramBindingStatus.VERIFIED
        and destination is not None
        and destination.status is TelegramDestinationStatus.ACTIVE
    )


def telegram_binding_view(
    *,
    enabled: bool,
    binding: TelegramBinding | None,
    pending: TelegramBindingRequest | None,
    destination: TelegramDestination | None,
    now: datetime,
) -> TelegramBindingView:
    if pending is not None and (
        pending.status is not TelegramBindingStatus.PENDING or pending.expires_at <= now
    ):
        pending = None
    if binding is None:
        status = "pending" if pending is not None else "absent"
    elif binding.status is TelegramBindingStatus.REVOKED:
        status = "revoked"
    elif destination is not None and destination.status is TelegramDestinationStatus.SUPPRESSED:
        status = "suppressed"
    else:
        status = "verified"
    return TelegramBindingView(
        status=status,
        version=binding.version if binding is not None else None,
        verified_at=binding.verified_at if binding is not None else None,
        pending_request_id=pending.request_id if pending is not None else None,
        pending_expires_at=pending.expires_at if pending is not None else None,
        effective_enabled=telegram_is_effectively_enabled(
            enabled,
            binding,
            destination,
        ),
    )
