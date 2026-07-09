from datetime import datetime

from pydantic import BaseModel, Field

from limnopulse_api.domain.telemetry import MetricValue


class TelemetryReadingResponse(BaseModel):
    ts: datetime
    tenant_id: str
    pond_id: str
    device_id: str | None = None
    metrics: dict[str, MetricValue] = Field(default_factory=dict)


class TelemetryReadingsResponse(BaseModel):
    items: list[TelemetryReadingResponse]


class LatestMetricsResponse(BaseModel):
    tenant_id: str
    pond_id: str
    ts: datetime | None = None
    device_id: str | None = None
    metrics: dict[str, MetricValue] = Field(default_factory=dict)
