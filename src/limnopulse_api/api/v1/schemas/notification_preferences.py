from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt

from limnopulse_api.domain.alerts import AlertSeverity
from limnopulse_api.domain.notification_preferences import EmailDeliverability


class NotificationPreferenceUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_version: StrictInt | None = Field(ge=1)
    email_enabled: StrictBool
    telegram_enabled: StrictBool | None = None
    minimum_severity: AlertSeverity = AlertSeverity.CRITICAL


class EmailNotificationPreferenceResponse(BaseModel):
    enabled: bool
    address: str | None
    verified: bool
    deliverability: EmailDeliverability
    suppression_reason: str | None
    effective_enabled: bool


class NotificationPreferenceResponse(BaseModel):
    configured: bool
    version: int | None
    email: EmailNotificationPreferenceResponse
    telegram: "TelegramNotificationPreferenceResponse"
    minimum_severity: AlertSeverity


class TelegramNotificationPreferenceResponse(BaseModel):
    enabled: bool
    status: str
    version: int | None
    verified_at: datetime | None
    pending_request_id: str | None
    pending_expires_at: datetime | None
    effective_enabled: bool
