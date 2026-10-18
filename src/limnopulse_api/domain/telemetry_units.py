"""Versioned, injectable unit conversion (`unit-mapping@1`, D2).

Source units are explicit per metric and case-sensitive; an unknown unit is quarantined as
`unit_unsupported`, never guessed. Percent-saturation dissolved oxygen is intentionally
unsupported because it needs temperature and salinity.
"""

import math
from collections.abc import Callable, Mapping
from types import MappingProxyType

from limnopulse_api.domain.telemetry_v2 import (
    MAX_EXACT_INTEGER,
    UNIT_MAPPING_VERSION,
    MetricDefinition,
    MetricValueType,
    QuarantineReason,
)

UnitConverter = Callable[[int | float], float]
UnitTable = Mapping[str, Mapping[str, UnitConverter]]  # metric_key -> source_unit -> converter

# Identity rows are `float` itself, so the canonical value is a bit-exact `float(value)`.
_IDENTITY: UnitConverter = float


def _fahrenheit_to_celsius(value: float) -> float:
    return (value - 32) * 5 / 9


def _kelvin_to_celsius(value: float) -> float:
    return value - 273.15


def _milli_to_unit(value: float) -> float:
    return value / 1000


UNIT_MAPPINGS: Mapping[str, UnitTable] = MappingProxyType(
    {
        "unit-mapping@1": MappingProxyType(
            {
                "water.temperature": MappingProxyType(
                    {"degC": _IDENTITY, "degF": _fahrenheit_to_celsius, "K": _kelvin_to_celsius}
                ),
                "water.ph": MappingProxyType({"pH": _IDENTITY}),
                "water.dissolved_oxygen": MappingProxyType(
                    {"mg/L": _IDENTITY, "ppm": _IDENTITY, "ug/L": _milli_to_unit}
                ),
                "water.turbidity": MappingProxyType({"NTU": _IDENTITY}),
                "water.salinity": MappingProxyType({"ppt": _IDENTITY, "g/kg": _IDENTITY}),
                "device.battery_voltage": MappingProxyType({"V": _IDENTITY, "mV": _milli_to_unit}),
                "device.rssi": MappingProxyType({"dBm": _IDENTITY}),
            }
        ),
    }
)


class UnitConversionError(Exception):
    """A source value that cannot become a canonical value; `reason` drives quarantine.

    Not a `ValueError`, so a quarantine handler can never swallow the unknown-version
    `ValueError`, which signals a programming error rather than bad data.
    """

    def __init__(self, reason: QuarantineReason, detail: str) -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


def _invalid(detail: str) -> UnitConversionError:
    return UnitConversionError(QuarantineReason.VALUE_INVALID, detail)


def convert_to_canonical(
    metric: MetricDefinition,
    value: int | float,  # noqa: PYI041 - the locked contract signature
    source_unit: str,
    *,
    unit_mapping_version: str = UNIT_MAPPING_VERSION,
    unit_mappings: Mapping[str, UnitTable] = UNIT_MAPPINGS,
) -> float:
    try:
        table = unit_mappings[unit_mapping_version]
    except KeyError:
        raise ValueError(f"unknown unit mapping version: {unit_mapping_version}") from None
    # The unit is resolved first: an observation with both defects is `unit_unsupported`.
    converter = table.get(metric.metric_key, {}).get(source_unit)
    if converter is None:
        raise UnitConversionError(
            QuarantineReason.UNIT_UNSUPPORTED,
            f"unit {source_unit[:16]!r} is not accepted for {metric.metric_key[:64]}",
        )
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise _invalid("value must be an integer or a float")
    if isinstance(value, float) and not math.isfinite(value):
        raise _invalid("value must be finite")
    if isinstance(value, int) and abs(value) >= MAX_EXACT_INTEGER:
        raise _invalid("integer magnitude must be below 2**53")
    canonical = float(converter(value))
    if not math.isfinite(canonical):
        raise _invalid("canonical value must be finite")
    if metric.value_type is MetricValueType.INTEGER and (
        not canonical.is_integer() or abs(canonical) >= MAX_EXACT_INTEGER
    ):
        raise _invalid("integer metric value must be integral with magnitude below 2**53")
    return canonical
