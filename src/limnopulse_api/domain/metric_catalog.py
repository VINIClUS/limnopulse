"""Global, versioned metric catalog (D1): exactly the seven legacy metrics, no tenant keys."""

from collections.abc import Mapping
from types import MappingProxyType

from limnopulse_api.domain.telemetry_v2 import (
    METRIC_CATALOG_VERSION,
    MetricDefinition,
    MetricValueType,
)


def _definition(
    metric_key: str,
    display_name: str,
    canonical_unit: str,
    value_type: MetricValueType,
    plausible_min: float,
    plausible_max: float,
    legacy_field: str,
) -> MetricDefinition:
    return MetricDefinition(
        metric_key=metric_key,
        display_name=display_name,
        canonical_unit=canonical_unit,
        value_type=value_type,
        plausible_min=plausible_min,
        plausible_max=plausible_max,
        legacy_field=legacy_field,
    )


_FLOAT = MetricValueType.FLOAT
_INTEGER = MetricValueType.INTEGER

# Plausible ranges are inclusive; order is the locked table order.
METRIC_CATALOGS: Mapping[str, tuple[MetricDefinition, ...]] = MappingProxyType(
    {
        "metric-catalog@1": (
            _definition("water.temperature", "Water temperature", "degC", _FLOAT, -5, 60, "temp_c"),
            _definition("water.ph", "pH", "pH", _FLOAT, 0, 14, "ph"),
            _definition(
                "water.dissolved_oxygen", "Dissolved oxygen", "mg/L", _FLOAT, 0, 50, "do_mg_l"
            ),
            _definition("water.turbidity", "Turbidity", "NTU", _FLOAT, 0, 4000, "turbidity_ntu"),
            _definition("water.salinity", "Salinity", "ppt", _FLOAT, 0, 70, "salinity_ppt"),
            _definition(
                "device.battery_voltage", "Battery voltage", "V", _FLOAT, 0, 60, "battery_v"
            ),
            _definition("device.rssi", "Signal strength (RSSI)", "dBm", _INTEGER, -150, 0, "rssi"),
        ),
    }
)


def metric_catalog(
    version: str = METRIC_CATALOG_VERSION,
    *,
    catalogs: Mapping[str, tuple[MetricDefinition, ...]] = METRIC_CATALOGS,
) -> tuple[MetricDefinition, ...]:
    try:
        return catalogs[version]
    except KeyError:
        raise ValueError(f"unknown metric catalog version: {version}") from None


def metric_definition(
    metric_key: str,
    *,
    version: str = METRIC_CATALOG_VERSION,
    catalogs: Mapping[str, tuple[MetricDefinition, ...]] = METRIC_CATALOGS,
) -> MetricDefinition | None:
    for definition in metric_catalog(version, catalogs=catalogs):
        if definition.metric_key == metric_key:
            return definition
    return None


def metric_for_legacy_field(
    field: str,
    *,
    version: str = METRIC_CATALOG_VERSION,
    catalogs: Mapping[str, tuple[MetricDefinition, ...]] = METRIC_CATALOGS,
) -> MetricDefinition | None:
    for definition in metric_catalog(version, catalogs=catalogs):
        if definition.legacy_field == field:
            return definition
    return None
