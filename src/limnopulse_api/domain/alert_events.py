from datetime import datetime
from enum import StrEnum

from pydantic import Field

from limnopulse_api.domain.alerts import (
    AlertAggregation,
    AlertMetric,
    AlertOperator,
    AlertSeverity,
)
from limnopulse_api.domain.entities import VersionedEntity


class AlertEventStatus(StrEnum):
    OPEN = "open"
    ACKNOWLEDGED = "acknowledged"
    SUPPRESSED = "suppressed"
    RESOLVED = "resolved"


class AlertEvent(VersionedEntity):
    tenant_id: str
    event_id: str
    rule_id: str
    rule_version: int = Field(ge=1)
    evaluation_revision: int = Field(ge=1)
    rule_name: str
    pond_id: str
    device_id: str | None = None
    metric: AlertMetric
    operator: AlertOperator
    threshold: float = Field(allow_inf_nan=False)
    aggregation: AlertAggregation
    severity: AlertSeverity
    status: AlertEventStatus
    opened_at: datetime
    confirmed_open_window_end: datetime
    window_start: datetime
    window_end: datetime
    last_evaluated_at: datetime
    last_evaluation_quality: str
    last_evaluation_value: float | None = None
    acknowledged_at: datetime | None = None
    acknowledged_by: str | None = None
    resolved_at: datetime | None = None
    resolved_by: str | None = None
    suppression_source_event_id: str | None = None
