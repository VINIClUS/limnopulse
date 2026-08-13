from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

from limnopulse_api.domain.alerts import AlertSeverity
from limnopulse_api.domain.telegram import TelegramBindingView


class EmailDeliverability(StrEnum):
    UNKNOWN = "unknown"
    DELIVERABLE = "deliverable"
    SUPPRESSED = "suppressed"


_SEVERITY_RANK = {
    AlertSeverity.WARNING: 0,
    AlertSeverity.CRITICAL: 1,
}


def _require_ascii_email_address(value: str) -> str:
    if not value.isascii():
        raise ValueError("email address must be ASCII")
    return value


AsciiEmailAddress = Annotated[str, AfterValidator(_require_ascii_email_address)]


class NotificationPreference(BaseModel):
    model_config = ConfigDict(frozen=True)

    tenant_id: str
    cognito_sub: str
    version: int = Field(ge=1)
    email_enabled: bool
    email_address: AsciiEmailAddress | None = None
    email_verified: bool = False
    checked_at: datetime | None = None
    identity_source: Literal["cognito_get_user", "development_header"] | None = None
    telegram_enabled: bool = False
    minimum_severity: AlertSeverity
    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def validate_email_snapshot(self) -> "NotificationPreference":
        snapshot = (self.email_address, self.checked_at, self.identity_source)
        if self.email_enabled and (
            self.email_address is None
            or not self.email_verified
            or self.checked_at is None
            or self.identity_source is None
        ):
            raise ValueError("enabled email requires a verified identity snapshot")
        if any(value is not None for value in snapshot) and any(
            value is None for value in snapshot
        ):
            raise ValueError("email identity snapshot must be complete")
        if self.email_address is None and self.email_verified:
            raise ValueError("email verification requires an identity snapshot")
        return self


class EmailDeliverabilityRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    deliverability: EmailDeliverability
    suppression_reason: str | None = None


class NotificationPreferenceEmailView(BaseModel):
    model_config = ConfigDict(frozen=True)

    enabled: bool
    address: str | None
    verified: bool
    deliverability: EmailDeliverability
    suppression_reason: str | None
    effective_enabled: bool


class NotificationPreferenceView(BaseModel):
    model_config = ConfigDict(frozen=True)

    configured: bool
    version: int | None
    email: NotificationPreferenceEmailView
    telegram: "NotificationPreferenceTelegramView"
    minimum_severity: AlertSeverity


class NotificationPreferenceTelegramView(BaseModel):
    model_config = ConfigDict(frozen=True)

    enabled: bool
    status: Literal["absent", "pending", "verified", "suppressed", "revoked"]
    version: int | None
    verified_at: datetime | None
    pending_request_id: str | None
    pending_expires_at: datetime | None
    effective_enabled: bool

    @classmethod
    def from_binding_view(
        cls,
        enabled: bool,
        view: TelegramBindingView,
    ) -> "NotificationPreferenceTelegramView":
        return cls(enabled=enabled, **view.model_dump())


def severity_rank(severity: AlertSeverity) -> int:
    return _SEVERITY_RANK[severity]


def severity_meets_minimum(
    severity: AlertSeverity,
    minimum_severity: AlertSeverity,
) -> bool:
    return severity_rank(severity) >= severity_rank(minimum_severity)


def mask_email_address(address: str) -> str:
    local_part, domain = address.rsplit("@", 1)
    if len(local_part) == 1:
        masked_local = f"{local_part}***"
    else:
        masked_local = f"{local_part[0]}***{local_part[-1]}"
    return f"{masked_local}@{domain}"


def email_is_effectively_enabled(
    preference: NotificationPreference,
    deliverability: EmailDeliverability,
) -> bool:
    return (
        preference.email_enabled
        and preference.email_verified
        and deliverability is not EmailDeliverability.SUPPRESSED
    )
