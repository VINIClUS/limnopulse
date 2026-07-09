from collections.abc import Mapping
from datetime import datetime
from types import MappingProxyType
from typing import Any, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, field_serializer

MetricValue: TypeAlias = int | float

WATER_QUALITY_FIELDS: tuple[str, ...] = (
    "temp_c",
    "ph",
    "do_mg_l",
    "turbidity_ntu",
    "salinity_ppt",
    "battery_v",
    "rssi",
    "seq",
)


class TelemetryReading(BaseModel):
    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    tenant_id: str
    pond_id: str
    device_id: str | None = None
    metrics: Mapping[str, MetricValue] = Field(default_factory=dict)

    def model_post_init(self, __context: Any) -> None:
        object.__setattr__(self, "metrics", MappingProxyType(dict(self.metrics)))

    @field_serializer("metrics")
    def serialize_metrics(self, value: Mapping[str, MetricValue]) -> dict[str, MetricValue]:
        return dict(value)
