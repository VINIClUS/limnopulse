"""P2-02: versioned, injectable unit conversion (`unit-mapping@1`, D2)."""

import ast
import inspect
import math
import struct
from collections.abc import Mapping

import pytest
from limnopulse_api.domain.telemetry_units import (
    UNIT_MAPPINGS,
    UnitConversionError,
    convert_to_canonical,
)

from limnopulse_api.domain import telemetry_units
from limnopulse_api.domain.metric_catalog import metric_catalog, metric_definition
from limnopulse_api.domain.telemetry_v2 import (
    MAX_EXACT_INTEGER,
    UNIT_MAPPING_VERSION,
    MetricDefinition,
    MetricValueType,
    QuarantineReason,
)

TEMPERATURE = "water.temperature"
PH = "water.ph"
DISSOLVED_OXYGEN = "water.dissolved_oxygen"
TURBIDITY = "water.turbidity"
SALINITY = "water.salinity"
BATTERY = "device.battery_voltage"
RSSI = "device.rssi"

# The locked `unit-mapping@1` table: metric_key -> accepted source units.
LOCKED_UNITS: dict[str, frozenset[str]] = {
    TEMPERATURE: frozenset({"degC", "degF", "K"}),
    PH: frozenset({"pH"}),
    DISSOLVED_OXYGEN: frozenset({"mg/L", "ppm", "ug/L"}),
    TURBIDITY: frozenset({"NTU"}),
    SALINITY: frozenset({"ppt", "g/kg"}),
    BATTERY: frozenset({"V", "mV"}),
    RSSI: frozenset({"dBm"}),
}

IDENTITY_UNITS = (
    (TEMPERATURE, "degC"),
    (PH, "pH"),
    (DISSOLVED_OXYGEN, "mg/L"),
    (DISSOLVED_OXYGEN, "ppm"),
    (TURBIDITY, "NTU"),
    (SALINITY, "ppt"),
    (SALINITY, "g/kg"),
    (BATTERY, "V"),
)

# Every unit code named anywhere in the table, plus case variants, near misses and the
# intentionally unsupported percent-saturation dissolved oxygen.
CANDIDATE_UNITS = sorted(
    set().union(*LOCKED_UNITS.values())
    | {
        "degc",
        "DEGC",
        "degf",
        "k",
        "C",
        "F",
        "°C",
        "ph",
        "PH",
        "mg/l",
        "MG/L",
        "PPM",
        "ug/l",
        "µg/L",
        "ntu",
        "PPT",
        "G/KG",
        "g/L",
        "psu",
        "v",
        "mv",
        "MV",
        "dbm",
        "DBM",
        "%",
        "%sat",
        "% sat",
        "percent_saturation",
        " degC",
        "degC ",
        "",
    }
)


def metric(metric_key: str) -> MetricDefinition:
    definition = metric_definition(metric_key)
    assert definition is not None
    return definition


def bits(value: float) -> bytes:
    return struct.pack("<d", value)


def assert_close(actual: float, expected: float) -> None:
    assert type(actual) is float
    assert math.isclose(actual, expected, rel_tol=1e-12, abs_tol=1e-12), (actual, expected)


def assert_rejected(
    reason: QuarantineReason, metric_key: str, value: object, source_unit: str, **kwargs: object
) -> UnitConversionError:
    with pytest.raises(UnitConversionError) as caught:
        convert_to_canonical(metric(metric_key), value, source_unit, **kwargs)  # type: ignore[arg-type]
    error = caught.value
    assert error.reason is reason
    assert isinstance(error.detail, str)
    assert error.detail
    return error


# --- registry -------------------------------------------------------------------------------


def test_registry_is_keyed_by_version_and_only_contains_unit_mapping_v1() -> None:
    assert isinstance(UNIT_MAPPINGS, Mapping)
    assert UNIT_MAPPING_VERSION == "unit-mapping@1"
    assert set(UNIT_MAPPINGS) == {"unit-mapping@1"}


def test_default_version_parameter_is_the_frozen_unit_mapping_version() -> None:
    parameters = inspect.signature(convert_to_canonical).parameters
    assert parameters["unit_mapping_version"].default == UNIT_MAPPING_VERSION
    assert parameters["unit_mapping_version"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["unit_mappings"].default is UNIT_MAPPINGS
    assert parameters["unit_mappings"].kind is inspect.Parameter.KEYWORD_ONLY


def test_unit_conversion_error_carries_reason_and_detail() -> None:
    error = UnitConversionError(QuarantineReason.UNIT_UNSUPPORTED, "unit 'x' not accepted")
    assert isinstance(error, Exception)
    assert error.reason is QuarantineReason.UNIT_UNSUPPORTED
    assert error.detail == "unit 'x' not accepted"


# --- every row of the locked table ----------------------------------------------------------


@pytest.mark.parametrize(
    ("metric_key", "source_unit", "value", "expected"),
    [
        (TEMPERATURE, "degF", 77, 25.0),
        (TEMPERATURE, "degF", 32.0, 0.0),
        (TEMPERATURE, "degF", 212.0, 100.0),
        (TEMPERATURE, "degF", -40.0, -40.0),
        (TEMPERATURE, "degF", 78.8, (78.8 - 32) * 5 / 9),
        (TEMPERATURE, "K", 273.15, 0.0),
        (TEMPERATURE, "K", 300, 300 - 273.15),
        (TEMPERATURE, "K", 298.65, 25.5),
        (DISSOLVED_OXYGEN, "ug/L", 6400, 6.4),
        (DISSOLVED_OXYGEN, "ug/L", 6400.0, 6.4),
        (DISSOLVED_OXYGEN, "ug/L", 1, 0.001),
        (BATTERY, "mV", 3700, 3.7),
        (BATTERY, "mV", 12345.6, 12.3456),
        (BATTERY, "mV", 0, 0.0),
    ],
)
def test_non_identity_conversions_follow_the_locked_formula(
    metric_key: str, source_unit: str, value: float, expected: float
) -> None:
    assert_close(convert_to_canonical(metric(metric_key), value, source_unit), expected)


@pytest.mark.parametrize(("metric_key", "source_unit"), IDENTITY_UNITS)
@pytest.mark.parametrize("value", [-3, 0, 7, 25.123456789012345, 0.1, -0.0, 1e-300])
def test_identity_conversions_return_a_bit_exact_float(
    metric_key: str, source_unit: str, value: float
) -> None:
    result = convert_to_canonical(metric(metric_key), value, source_unit)
    assert type(result) is float
    assert bits(result) == bits(float(value))


@pytest.mark.parametrize(("metric_key", "source_unit"), [*IDENTITY_UNITS, (RSSI, "dBm")])
def test_identity_conversions_accept_the_largest_exact_integer(
    metric_key: str, source_unit: str
) -> None:
    for value in (MAX_EXACT_INTEGER - 1, -(MAX_EXACT_INTEGER - 1)):
        result = convert_to_canonical(metric(metric_key), value, source_unit)
        assert type(result) is float
        assert result == float(value)
        assert int(result) == value


def test_every_catalog_metric_accepts_exactly_its_locked_units() -> None:
    assert {item.metric_key for item in metric_catalog()} == set(LOCKED_UNITS)
    for definition in metric_catalog():
        accepted = set()
        for source_unit in CANDIDATE_UNITS:
            try:
                convert_to_canonical(definition, 1, source_unit)
            except UnitConversionError as error:
                assert error.reason is QuarantineReason.UNIT_UNSUPPORTED, (
                    definition.metric_key,
                    source_unit,
                )
            else:
                accepted.add(source_unit)
        assert accepted == LOCKED_UNITS[definition.metric_key], definition.metric_key


@pytest.mark.parametrize(
    ("metric_key", "source_unit"),
    [
        (TEMPERATURE, "degc"),
        (TEMPERATURE, "k"),
        (TEMPERATURE, "DEGF"),
        (DISSOLVED_OXYGEN, "mg/l"),
        (DISSOLVED_OXYGEN, "UG/L"),
        (DISSOLVED_OXYGEN, "%"),
        (DISSOLVED_OXYGEN, "%sat"),
        (BATTERY, "mv"),
        (BATTERY, "v"),
        (RSSI, "dbm"),
        (PH, "PH"),
        (SALINITY, "PPT"),
        (TURBIDITY, "ntu"),
        (PH, "degC"),
        (SALINITY, "mg/L"),
        (BATTERY, "dBm"),
        (RSSI, "V"),
        (TEMPERATURE, "furlongs"),
    ],
)
def test_unknown_or_wrong_case_unit_is_unit_unsupported(metric_key: str, source_unit: str) -> None:
    error = assert_rejected(QuarantineReason.UNIT_UNSUPPORTED, metric_key, 1.0, source_unit)
    assert source_unit in error.detail


# --- value_invalid ---------------------------------------------------------------------------


@pytest.mark.parametrize("metric_key", sorted(LOCKED_UNITS))
@pytest.mark.parametrize("value", [True, False])
def test_booleans_are_value_invalid(metric_key: str, value: bool) -> None:
    source_unit = min(LOCKED_UNITS[metric_key])
    assert_rejected(QuarantineReason.VALUE_INVALID, metric_key, value, source_unit)


@pytest.mark.parametrize("metric_key", sorted(LOCKED_UNITS))
@pytest.mark.parametrize("value", ["25.0", "7", "", b"7"])
def test_strings_are_value_invalid(metric_key: str, value: object) -> None:
    source_unit = min(LOCKED_UNITS[metric_key])
    assert_rejected(QuarantineReason.VALUE_INVALID, metric_key, value, source_unit)


@pytest.mark.parametrize(
    ("metric_key", "source_unit"),
    [(key, unit) for key, units in sorted(LOCKED_UNITS.items()) for unit in sorted(units)],
)
@pytest.mark.parametrize("value", [math.nan, -math.nan, math.inf, -math.inf])
def test_nan_and_infinity_are_value_invalid(
    metric_key: str, source_unit: str, value: float
) -> None:
    assert_rejected(QuarantineReason.VALUE_INVALID, metric_key, value, source_unit)


@pytest.mark.parametrize(
    ("metric_key", "source_unit"),
    [(key, unit) for key, units in sorted(LOCKED_UNITS.items()) for unit in sorted(units)],
)
@pytest.mark.parametrize(
    "value", [MAX_EXACT_INTEGER, -MAX_EXACT_INTEGER, MAX_EXACT_INTEGER + 1, 10**30, -(10**30)]
)
def test_integers_at_or_beyond_max_exact_integer_are_value_invalid(
    metric_key: str, source_unit: str, value: int
) -> None:
    assert_rejected(QuarantineReason.VALUE_INVALID, metric_key, value, source_unit)


def test_non_identity_conversion_accepts_the_largest_exact_integer() -> None:
    result = convert_to_canonical(metric(BATTERY), MAX_EXACT_INTEGER - 1, "mV")
    assert_close(result, (MAX_EXACT_INTEGER - 1) / 1000)


@pytest.mark.parametrize(
    ("metric_key", "source_unit", "value"),
    [
        (TEMPERATURE, "degC", 1e20),
        (TEMPERATURE, "degC", -1e300),
        (TURBIDITY, "NTU", float(MAX_EXACT_INTEGER) * 4),
        (BATTERY, "V", 2.0**60),
    ],
)
def test_large_finite_floats_of_float_metrics_are_converted_not_rejected(
    metric_key: str, source_unit: str, value: float
) -> None:
    result = convert_to_canonical(metric(metric_key), value, source_unit)
    assert bits(result) == bits(value)


def test_non_finite_conversion_result_is_value_invalid() -> None:
    # (x - 32) * 5 overflows to infinity for a finite x near the float maximum.
    assert_rejected(QuarantineReason.VALUE_INVALID, TEMPERATURE, 1.7e308, "degF")


# --- device.rssi (integer metric) ------------------------------------------------------------


@pytest.mark.parametrize("value", [-60, -60.0, 0, 0.0, -150, -151.0, 5, -0.0])
def test_integral_rssi_is_accepted_as_float(value: float) -> None:
    result = convert_to_canonical(metric(RSSI), value, "dBm")
    assert type(result) is float
    assert result == float(value)
    assert result.is_integer()


@pytest.mark.parametrize("value", [-60.5, -0.1, 0.5, -149.999, 1e-9, -72.00000000001])
def test_non_integral_rssi_is_value_invalid(value: float) -> None:
    assert_rejected(QuarantineReason.VALUE_INVALID, RSSI, value, "dBm")


@pytest.mark.parametrize(
    "value",
    [
        float(MAX_EXACT_INTEGER),
        -float(MAX_EXACT_INTEGER),
        2.0**60,
        -(2.0**60),
        1e19,
        -1e19,
        1e300,
    ],
)
def test_rssi_floats_at_or_beyond_max_exact_integer_are_value_invalid(value: float) -> None:
    assert value.is_integer()
    assert_rejected(QuarantineReason.VALUE_INVALID, RSSI, value, "dBm")


def test_rssi_float_just_below_max_exact_integer_is_accepted() -> None:
    value = float(MAX_EXACT_INTEGER - 1)
    assert convert_to_canonical(metric(RSSI), value, "dBm") == value


def test_integrality_is_decided_by_value_type_after_conversion() -> None:
    # A test-only integer metric with a non-identity unit: integrality is checked on the
    # canonical value, so 1500 mV -> 1.5 is rejected while 2000 mV -> 2.0 is accepted.
    integer_battery = metric(BATTERY).model_copy(update={"value_type": MetricValueType.INTEGER})
    assert convert_to_canonical(integer_battery, 2000, "mV") == 2.0
    with pytest.raises(UnitConversionError) as caught:
        convert_to_canonical(integer_battery, 1500, "mV")
    assert caught.value.reason is QuarantineReason.VALUE_INVALID
    float_rssi = metric(RSSI).model_copy(update={"value_type": MetricValueType.FLOAT})
    assert convert_to_canonical(float_rssi, -60.5, "dBm") == -60.5


# --- versioning and injection ----------------------------------------------------------------


def _with_test_only_v2() -> dict[str, object]:
    v1 = UNIT_MAPPINGS["unit-mapping@1"]
    v2 = {key: dict(units) for key, units in v1.items()}
    v2[TEMPERATURE]["degC"] = lambda value: value + 1.0
    v2[TEMPERATURE]["mdegC"] = lambda value: value / 1000
    return {**UNIT_MAPPINGS, "unit-mapping@2": v2}


def test_injected_test_only_v2_changes_the_result_only_when_selected() -> None:
    temperature = metric(TEMPERATURE)
    mappings = _with_test_only_v2()

    assert convert_to_canonical(temperature, 20.0, "degC") == 20.0
    assert convert_to_canonical(temperature, 20.0, "degC", unit_mappings=mappings) == 20.0
    assert (
        convert_to_canonical(
            temperature,
            20.0,
            "degC",
            unit_mapping_version="unit-mapping@1",
            unit_mappings=mappings,
        )
        == 20.0
    )
    assert (
        convert_to_canonical(
            temperature,
            20.0,
            "degC",
            unit_mapping_version="unit-mapping@2",
            unit_mappings=mappings,
        )
        == 21.0
    )
    assert_close(
        convert_to_canonical(
            temperature,
            20500,
            "mdegC",
            unit_mapping_version="unit-mapping@2",
            unit_mappings=mappings,
        ),
        20.5,
    )
    with pytest.raises(UnitConversionError) as caught:
        convert_to_canonical(temperature, 20500, "mdegC", unit_mappings=mappings)
    assert caught.value.reason is QuarantineReason.UNIT_UNSUPPORTED
    assert "unit-mapping@2" not in UNIT_MAPPINGS


def test_value_rules_still_apply_under_an_injected_version() -> None:
    mappings = _with_test_only_v2()
    for value in (True, math.nan, MAX_EXACT_INTEGER, "20"):
        with pytest.raises(UnitConversionError) as caught:
            convert_to_canonical(
                metric(TEMPERATURE),
                value,  # type: ignore[arg-type]
                "degC",
                unit_mapping_version="unit-mapping@2",
                unit_mappings=mappings,
            )
        assert caught.value.reason is QuarantineReason.VALUE_INVALID


@pytest.mark.parametrize("version", ["unit-mapping@2", "unit-mapping@0", "", "UNIT-MAPPING@1"])
def test_unknown_version_raises_value_error_not_a_quarantine(version: str) -> None:
    with pytest.raises(ValueError) as caught:
        convert_to_canonical(metric(TEMPERATURE), 20.0, "degC", unit_mapping_version=version)
    assert not isinstance(caught.value, UnitConversionError)


def test_unknown_version_is_checked_against_the_injected_registry() -> None:
    with pytest.raises(ValueError) as caught:
        convert_to_canonical(
            metric(TEMPERATURE),
            20.0,
            "degC",
            unit_mapping_version="unit-mapping@1",
            unit_mappings={},
        )
    assert not isinstance(caught.value, UnitConversionError)


# --- purity ----------------------------------------------------------------------------------

FORBIDDEN_IMPORTS = frozenset(
    {"os", "io", "pathlib", "socket", "subprocess", "time", "random", "requests", "httpx"}
)
FORBIDDEN_NAMES = frozenset({"open", "print", "input", "now", "utcnow", "today", "monotonic"})


def test_unit_module_is_pure() -> None:
    tree = ast.parse(inspect.getsource(telemetry_units))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert alias.name.split(".")[0] not in FORBIDDEN_IMPORTS, alias.name
        elif isinstance(node, ast.ImportFrom):
            assert (node.module or "").split(".")[0] not in FORBIDDEN_IMPORTS, node.module
        elif isinstance(node, ast.Name):
            assert node.id not in FORBIDDEN_NAMES, node.id
        elif isinstance(node, ast.Attribute):
            assert node.attr not in FORBIDDEN_NAMES, node.attr
