import re
from datetime import datetime

from pydantic import BaseModel, ConfigDict


_RELATIVE_DURATION_PATTERN = re.compile(r"^-\d+(ns|us|ms|s|m|h|d|w)$")


def validate_flux_time_bound(value: str) -> str:
    if _RELATIVE_DURATION_PATTERN.match(value):
        return value
    datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value


class WaterQualityFields(BaseModel):
    model_config = ConfigDict(frozen=True)

    temp_c: float | None = None
    ph: float | None = None
    do_mg_l: float | None = None
    turbidity_ntu: float | None = None
    salinity_ppt: float | None = None
    battery_v: float | None = None
    rssi: int | None = None
    seq: int | None = None


class TelemetryReading(WaterQualityFields):
    measured_at: datetime
    tenant_id: str
    pond_id: str
    device_id: str


class LatestMetrics(WaterQualityFields):
    measured_at: datetime | None = None
    tenant_id: str
    pond_id: str
