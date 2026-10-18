import re
from pathlib import Path

import pytest
from limnopulse_api.domain.metric_catalog import (
    METRIC_CATALOGS,
    metric_catalog,
    metric_definition,
    metric_for_legacy_field,
)
from limnopulse_api.domain.telemetry_v2 import (
    METRIC_CATALOG_VERSION,
    MetricDefinition,
    MetricValueType,
)
from pydantic import ValidationError

from limnopulse_api.domain.alerts import AlertMetric
from limnopulse_api.domain.v2_common import CAPABILITY_KEY_PATTERN

ROOT = Path(__file__).resolve().parents[2]
GO_QUERY = ROOT / "internal/alertevaluator/influx/query.go"

EXPECTED_ROWS = [
    ("water.temperature", "Water temperature", "degC", "float", -5, 60, "temp_c"),
    ("water.ph", "pH", "pH", "float", 0, 14, "ph"),
    ("water.dissolved_oxygen", "Dissolved oxygen", "mg/L", "float", 0, 50, "do_mg_l"),
    ("water.turbidity", "Turbidity", "NTU", "float", 0, 4000, "turbidity_ntu"),
    ("water.salinity", "Salinity", "ppt", "float", 0, 70, "salinity_ppt"),
    ("device.battery_voltage", "Battery voltage", "V", "float", 0, 60, "battery_v"),
    ("device.rssi", "Signal strength (RSSI)", "dBm", "integer", -150, 0, "rssi"),
]


def _row(definition: MetricDefinition) -> tuple[object, ...]:
    return (
        definition.metric_key,
        definition.display_name,
        definition.canonical_unit,
        definition.value_type.value,
        definition.plausible_min,
        definition.plausible_max,
        definition.legacy_field,
    )


def _test_catalog_v2() -> tuple[MetricDefinition, ...]:
    return (
        MetricDefinition(
            metric_key="water.chlorophyll",
            display_name="Chlorophyll",
            canonical_unit="ug/L",
            value_type=MetricValueType.FLOAT,
            plausible_min=0,
            plausible_max=500,
            legacy_field=None,
        ),
    )


def test_registry_contains_only_catalog_v1() -> None:
    assert set(METRIC_CATALOGS) == {"metric-catalog@1"}
    assert METRIC_CATALOGS[METRIC_CATALOG_VERSION] == metric_catalog()


def test_catalog_v1_equals_locked_table_in_order() -> None:
    catalog = metric_catalog()
    assert isinstance(catalog, tuple)
    assert [_row(definition) for definition in catalog] == EXPECTED_ROWS
    assert metric_catalog(METRIC_CATALOG_VERSION) == catalog
    assert metric_catalog("metric-catalog@1") == catalog


def test_catalog_types_are_exact() -> None:
    for definition in metric_catalog():
        assert isinstance(definition, MetricDefinition)
        assert isinstance(definition.value_type, MetricValueType)
        assert isinstance(definition.plausible_min, float)
        assert isinstance(definition.plausible_max, float)
        assert definition.plausible_min < definition.plausible_max


def test_catalog_keys_match_capability_key_pattern_and_are_unique() -> None:
    keys = [definition.metric_key for definition in metric_catalog()]
    assert len(keys) == len(set(keys)) == 7
    for key in keys:
        assert re.fullmatch(CAPABILITY_KEY_PATTERN, key), key


def test_legacy_fields_equal_alert_metrics() -> None:
    legacy_fields = {definition.legacy_field for definition in metric_catalog()}
    assert legacy_fields == {metric.value for metric in AlertMetric}


def test_legacy_fields_equal_go_allowed_metrics() -> None:
    source = GO_QUERY.read_text(encoding="utf-8")
    block = re.search(r"var allowedMetrics = map\[string\]struct\{\}\{(.*?)\n\}", source, re.DOTALL)
    assert block is not None
    go_metrics = set(re.findall(r'"([a-z_]+)"\s*:', block.group(1)))
    assert go_metrics == {definition.legacy_field for definition in metric_catalog()}


@pytest.mark.parametrize("row", EXPECTED_ROWS, ids=[row[0] for row in EXPECTED_ROWS])
def test_metric_lookup_by_key_and_legacy_field(row) -> None:
    key, legacy_field = row[0], row[6]
    definition = metric_definition(key)
    assert definition is not None
    assert _row(definition) == row
    assert metric_for_legacy_field(legacy_field) == definition
    assert metric_definition(key, version=METRIC_CATALOG_VERSION) == definition
    assert metric_for_legacy_field(legacy_field, version=METRIC_CATALOG_VERSION) == definition


@pytest.mark.parametrize("key", ["water.chlorophyll", "temp_c", "Water.Temperature", ""])
def test_unknown_metric_key_returns_none(key) -> None:
    assert metric_definition(key) is None


@pytest.mark.parametrize("field", ["seq", "water.temperature", "TEMP_C", ""])
def test_unknown_legacy_field_returns_none(field) -> None:
    assert metric_for_legacy_field(field) is None


def test_unknown_catalog_version_raises() -> None:
    with pytest.raises(ValueError):
        metric_catalog("metric-catalog@2")
    with pytest.raises(ValueError):
        metric_definition("water.temperature", version="metric-catalog@2")
    with pytest.raises(ValueError):
        metric_for_legacy_field("temp_c", version="metric-catalog@2")


def test_injected_catalog_registry_is_honored() -> None:
    catalogs = {**METRIC_CATALOGS, "metric-catalog@2": _test_catalog_v2()}

    assert metric_catalog("metric-catalog@2", catalogs=catalogs) == _test_catalog_v2()
    chlorophyll = metric_definition(
        "water.chlorophyll", version="metric-catalog@2", catalogs=catalogs
    )
    assert chlorophyll is not None
    assert chlorophyll.display_name == "Chlorophyll"
    assert (
        metric_definition("water.temperature", version="metric-catalog@2", catalogs=catalogs)
        is None
    )
    assert metric_for_legacy_field("temp_c", version="metric-catalog@2", catalogs=catalogs) is None
    assert metric_definition("water.chlorophyll") is None
    with pytest.raises(ValueError):
        metric_catalog(catalogs={"metric-catalog@2": _test_catalog_v2()})


def test_catalog_api_is_keyword_only_for_registry_parameters() -> None:
    with pytest.raises(TypeError):
        metric_definition("water.temperature", METRIC_CATALOG_VERSION)  # type: ignore[misc]
    with pytest.raises(TypeError):
        metric_for_legacy_field("temp_c", METRIC_CATALOG_VERSION)  # type: ignore[misc]
    with pytest.raises(TypeError):
        metric_catalog(METRIC_CATALOG_VERSION, METRIC_CATALOGS)  # type: ignore[misc]


@pytest.mark.parametrize("value_type", ["decimal", "", None])
def test_metric_definition_rejects_unknown_value_type(value_type) -> None:
    with pytest.raises(ValidationError):
        MetricDefinition(
            metric_key="water.chlorophyll",
            display_name="Chlorophyll",
            canonical_unit="ug/L",
            value_type=value_type,
            plausible_min=0,
            plausible_max=500,
            legacy_field=None,
        )


def test_metric_definition_is_frozen() -> None:
    definition = metric_catalog()[0]
    with pytest.raises(ValidationError):
        definition.display_name = "Changed"  # type: ignore[misc]
