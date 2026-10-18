from typing import Literal

from pydantic import BaseModel

from limnopulse_api.domain.telemetry_v2 import MetricDefinition


class V2MetricDefinitionResponse(BaseModel):
    metric_key: str
    display_name: str
    canonical_unit: str
    # A plain literal, not the domain enum: an enum would add an unprefixed OpenAPI component.
    value_type: Literal["float", "integer"]
    plausible_min: float
    plausible_max: float
    legacy_field: str | None
    catalog_version: str

    @classmethod
    def from_domain(
        cls, definition: MetricDefinition, catalog_version: str
    ) -> "V2MetricDefinitionResponse":
        return cls.model_validate(
            {**definition.model_dump(mode="json"), "catalog_version": catalog_version}
        )


class V2MetricDefinitionListResponse(BaseModel):
    items: list[V2MetricDefinitionResponse]
