from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field

from limnopulse_api.domain.alerts import AlertSeverity


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
    email_address: AsciiEmailAddress
    email_verified: bool
    checked_at: datetime
    identity_source: Literal["cognito_get_user"]
    minimum_severity: AlertSeverity
    created_at: datetime
    updated_at: datetime


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
    minimum_severity: AlertSeverity


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
