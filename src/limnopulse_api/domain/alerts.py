from collections.abc import Mapping
from enum import StrEnum
import re
from typing import Annotated, Any, Self

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, field_validator, model_validator

from limnopulse_api.domain.entities import VersionedEntity


class AlertMetric(StrEnum):
    TEMP_C = "temp_c"
    PH = "ph"
    DO_MG_L = "do_mg_l"
    TURBIDITY_NTU = "turbidity_ntu"
    SALINITY_PPT = "salinity_ppt"
    BATTERY_V = "battery_v"
    RSSI = "rssi"


class AlertOperator(StrEnum):
    LESS_THAN = "<"
    LESS_THAN_OR_EQUAL = "<="
    GREATER_THAN = ">"
    GREATER_THAN_OR_EQUAL = ">="


class AlertAggregation(StrEnum):
    MIN = "min"
    MAX = "max"
    MEAN = "mean"
    LAST = "last"


class AlertSeverity(StrEnum):
    WARNING = "warning"
    CRITICAL = "critical"


class AlertChannel(StrEnum):
    EMAIL = "email"
    TELEGRAM = "telegram"


_DURATION_PATTERN = re.compile(r"^(?P<amount>[1-9][0-9]*)(?P<unit>[smh])$")
_DURATION_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3_600}
_MIN_DURATION_SECONDS = 60
_MAX_DURATION_SECONDS = 24 * 60 * 60


def _validate_alert_duration(value: str) -> str:
    match = _DURATION_PATTERN.fullmatch(value)
    if match is None:
        raise ValueError("duration must use a positive integer followed by s, m, or h")
    seconds = int(match.group("amount")) * _DURATION_UNIT_SECONDS[match.group("unit")]
    if not _MIN_DURATION_SECONDS <= seconds <= _MAX_DURATION_SECONDS:
        raise ValueError("duration must be between 60 seconds and 24 hours")
    return value


AlertDuration = Annotated[str, AfterValidator(_validate_alert_duration)]


class AlertRule(VersionedEntity):
    tenant_id: str
    rule_id: str
    pond_id: str
    device_id: str | None = None
    metric: AlertMetric
    name: str = Field(min_length=1, max_length=120)
    operator: AlertOperator
    threshold: float = Field(allow_inf_nan=False)
    aggregation: AlertAggregation
    window: AlertDuration
    duration: AlertDuration
    severity: AlertSeverity
    channels: tuple[AlertChannel, ...] = Field(min_length=1)
    cooldown_seconds: int = Field(ge=60, le=86_400)
    enabled: bool
    replaces_rule_id: str | None = None
    replaced_by_rule_id: str | None = None
    evaluation_revision: int = Field(default=1, ge=1)

    @field_validator("channels")
    @classmethod
    def channels_must_be_unique(
        cls,
        value: tuple[AlertChannel, ...],
    ) -> tuple[AlertChannel, ...]:
        if len(set(value)) != len(value):
            raise ValueError("channels must be unique")
        return value

    @model_validator(mode="after")
    def device_metrics_require_device(self) -> Self:
        if self.metric in {AlertMetric.BATTERY_V, AlertMetric.RSSI} and self.device_id is None:
            raise ValueError("device_id is required for battery_v and rssi alert rules")
        return self


class AuditContext(BaseModel):
    model_config = ConfigDict(frozen=True)

    actor_id: str
    ip: str | None = None
    user_agent: str | None = None


class AlertRuleReplacement(BaseModel):
    model_config = ConfigDict(frozen=True)

    replaced: AlertRule
    replacement: AlertRule


AlertRuleUpdates = Mapping[str, Any]
