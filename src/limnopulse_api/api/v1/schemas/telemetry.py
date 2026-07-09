from pydantic import BaseModel, Field


class WaterQualityFieldsResponse(BaseModel):
    temp_c: float | None = None
    ph: float | None = None
    do_mg_l: float | None = None
    turbidity_ntu: float | None = None
    salinity_ppt: float | None = None
    battery_v: float | None = None
    rssi: int | None = None
    seq: int | None = None


class TelemetryReadingResponse(WaterQualityFieldsResponse):
    measured_at: str
    tenant_id: str
    pond_id: str
    device_id: str


class TelemetryReadingListResponse(BaseModel):
    items: list[TelemetryReadingResponse]


class LatestMetricsResponse(WaterQualityFieldsResponse):
    measured_at: str | None = None
    tenant_id: str
    pond_id: str


class TelemetryReadQuery(BaseModel):
    start: str = Field(default="-1h")
    stop: str | None = None
    limit: int = Field(default=500, ge=1, le=1000)
