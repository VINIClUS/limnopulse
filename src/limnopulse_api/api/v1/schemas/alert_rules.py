from typing import Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from limnopulse_api.api.v1.schemas.common import VersionedResponse
from limnopulse_api.domain.alerts import (
    AlertAggregation,
    AlertChannel,
    AlertDuration,
    AlertMetric,
    AlertOperator,
    AlertSeverity,
)


class AlertRuleDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

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


class AlertRuleCreate(AlertRuleDefinition):
    pass


class AlertRuleUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_version: int = Field(ge=1)
    name: str | None = Field(default=None, min_length=1, max_length=120)
    operator: AlertOperator | None = None
    threshold: float | None = Field(default=None, allow_inf_nan=False)
    aggregation: AlertAggregation | None = None
    window: AlertDuration | None = None
    duration: AlertDuration | None = None
    severity: AlertSeverity | None = None
    channels: tuple[AlertChannel, ...] | None = Field(default=None, min_length=1)
    cooldown_seconds: int | None = Field(default=None, ge=60, le=86_400)
    enabled: bool | None = None

    @field_validator("channels")
    @classmethod
    def channels_must_be_unique(
        cls,
        value: tuple[AlertChannel, ...] | None,
    ) -> tuple[AlertChannel, ...] | None:
        if value is not None and len(set(value)) != len(value):
            raise ValueError("channels must be unique")
        return value

    @model_validator(mode="after")
    def require_non_null_change(self) -> Self:
        changed_fields = self.model_fields_set - {"expected_version"}
        if not changed_fields:
            raise ValueError("at least one mutable field is required")
        if any(getattr(self, field_name) is None for field_name in changed_fields):
            raise ValueError("mutable fields cannot be null")
        return self

    def updates(self) -> dict[str, object]:
        return self.model_dump(
            mode="python",
            exclude={"expected_version"},
            exclude_unset=True,
        )


class AlertRuleReplace(AlertRuleDefinition):
    expected_version: int = Field(ge=1)


class AlertRuleResponse(VersionedResponse):
    tenant_id: str
    rule_id: str
    pond_id: str
    device_id: str | None = None
    metric: AlertMetric
    name: str
    operator: AlertOperator
    threshold: float
    aggregation: AlertAggregation
    window: str
    duration: str
    severity: AlertSeverity
    channels: tuple[AlertChannel, ...]
    cooldown_seconds: int
    enabled: bool
    replaces_rule_id: str | None = None
    replaced_by_rule_id: str | None = None


class AlertRuleListResponse(BaseModel):
    items: list[AlertRuleResponse]


class AlertRuleReplacementResponse(BaseModel):
    replaced: AlertRuleResponse
    replacement: AlertRuleResponse
