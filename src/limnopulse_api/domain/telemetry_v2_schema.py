"""InfluxDB schema and canonical line protocol for `observation_numeric` and legacy dual-write.

The series key holds identity and resolved-ownership tags only (D5); everything else is a field,
and every point carries the complete field set so a rewrite replaces every field.
"""

import json
import math
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from limnopulse_api.domain.ids import (
    default_component_id_for_legacy_device,
    require_canonical_id,
    require_device_id,
    require_safe_id,
)
from limnopulse_api.domain.metric_catalog import metric_definition
from limnopulse_api.domain.telemetry_v2 import (
    ABSENT_SEQUENCE,
    ABSENT_STRING,
    LEGACY_MQTT_TOPIC_TEMPLATE,
    MAX_QUALITY_JSON_FIELD_BYTES,
    TELEMETRY_SCHEMA_VERSION,
    CanonicalObservation,
    require_telemetry_time,
)

OBSERVATION_MEASUREMENT = "observation_numeric"
LEGACY_MEASUREMENT = "water_quality"
OBSERVATION_TAG_KEYS = frozenset(
    {
        "asset_id",
        "component_id",
        "deployment_id",
        "device_id",
        "metric_key",
        "probe_id",
        "schema_version",
        "site_id",
        "tenant_id",
    }
)
OBSERVATION_REQUIRED_TAG_KEYS = OBSERVATION_TAG_KEYS - {"probe_id"}
OBSERVATION_FIELD_KEYS = frozenset(
    {
        "catalog_version",
        "delay_ms",
        "event_id",
        "ingested_at_ns",
        "integration_id",
        "quality_flags_json",
        "quality_policy_version",
        "quality_provenance_json",
        "quality_state",
        "received_at_ns",
        "sequence",
        "source_event_id",
        "source_type",
        "source_unit",
        "unit_mapping_version",
        "value",
    }
)
LEGACY_TAG_KEYS = frozenset(
    {"device_id", "pond_id", "schema_version", "source", "tenant_id", "topic"}
)
LEGACY_INTEGER_FIELDS = frozenset({"rssi", "seq"})

# D8: the stored Telegraf identity, whatever the canonical source type.
LEGACY_SOURCE = "mqtt"
LEGACY_SCHEMA_VERSION = "1"
OBSERVATION_SCHEMA_VERSION = str(TELEMETRY_SCHEMA_VERSION)

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MICROSECOND = timedelta(microseconds=1)
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


def _canonical_validator(prefix: str) -> Callable[[str], str]:
    return lambda value: require_canonical_id(value, prefix)


_TAG_ID_VALIDATORS: Mapping[str, Callable[[str], str]] = {
    "asset_id": _canonical_validator("ast"),
    "component_id": _canonical_validator("cmp"),
    "deployment_id": _canonical_validator("dep"),
    "device_id": require_device_id,
    "probe_id": _canonical_validator("prb"),
    "site_id": _canonical_validator("site"),
    "tenant_id": require_safe_id,
}


@dataclass(frozen=True)
class LegacyPoint:
    """One `water_quality` point; `source` and `topic` are derived constants (D8)."""

    tenant_id: str
    pond_id: str
    device_id: str
    observed_at: datetime
    fields: Mapping[str, float | int]


def observation_tags(o: CanonicalObservation) -> dict[str, str]:
    resolved = o.resolved
    tags = {
        "asset_id": resolved.asset_id,
        "component_id": resolved.component_id,
        "deployment_id": resolved.deployment_id,
        "device_id": resolved.device_id,
        "metric_key": o.observation.metric_key,
        "schema_version": OBSERVATION_SCHEMA_VERSION,
        "site_id": resolved.site_id,
        "tenant_id": resolved.tenant_id,
    }
    if resolved.probe_id is not None:
        tags["probe_id"] = resolved.probe_id
    return dict(sorted(tags.items()))


def validate_observation_tags(tags: Mapping[str, str]) -> None:
    """Cardinality guard: a closed allowlist of validated identity and ownership tags."""
    unknown = set(tags) - OBSERVATION_TAG_KEYS
    if unknown:
        raise ValueError(f"tags not allowed on {OBSERVATION_MEASUREMENT}: {sorted(unknown)}")
    missing = OBSERVATION_REQUIRED_TAG_KEYS - set(tags)
    if missing:
        raise ValueError(f"required tags missing: {sorted(missing)}")
    for key, value in tags.items():
        if not isinstance(value, str):
            raise ValueError(f"tag {key} must be a string")
        if key == "schema_version":
            if value != OBSERVATION_SCHEMA_VERSION:
                raise ValueError(f"schema_version tag must be {OBSERVATION_SCHEMA_VERSION!r}")
        elif key == "metric_key":
            if metric_definition(value) is None:
                raise ValueError(f"metric_key {value!r} is not in the metric catalog")
        else:
            _TAG_ID_VALIDATORS[key](value)


def _epoch_microseconds(value: datetime) -> int:
    return (require_telemetry_time(value) - _EPOCH) // _MICROSECOND


def _escape_key(value: str) -> str:
    return value.replace(",", "\\,").replace("=", "\\=").replace(" ", "\\ ")


def _format_string(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _format_float(value: float | int) -> str:
    if isinstance(value, bool):
        raise ValueError("boolean is not a numeric field value")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("float field values must be finite")
    return repr(number)


def _format_integer(value: int) -> str:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("integer field values must be int")
    if not _INT64_MIN <= value <= _INT64_MAX:
        raise ValueError("integer field value outside the signed 64-bit range")
    return f"{value}i"


def _quality_json(items: Iterable[str]) -> str:
    encoded = json.dumps([str(item) for item in items], separators=(",", ":"))
    if len(encoded.encode("utf-8")) > MAX_QUALITY_JSON_FIELD_BYTES:
        raise ValueError(f"quality JSON field exceeds {MAX_QUALITY_JSON_FIELD_BYTES} bytes")
    return encoded


def _line(measurement: str, tags: Mapping[str, str], fields: Mapping[str, str], ns: int) -> str:
    tag_set = ",".join(f"{_escape_key(k)}={_escape_key(v)}" for k, v in sorted(tags.items()))
    field_set = ",".join(f"{_escape_key(k)}={v}" for k, v in sorted(fields.items()))
    return f"{_escape_key(measurement)},{tag_set} {field_set} {ns}"


def encode_observation_line(o: CanonicalObservation) -> str:
    tags = observation_tags(o)
    validate_observation_tags(tags)
    measurement = o.observation
    observed_us = _epoch_microseconds(measurement.observed_at)
    received_us = _epoch_microseconds(o.source.received_at)
    ingested_us = _epoch_microseconds(o.delivery.ingested_at)
    integration_id = o.source.integration_id
    sequence = measurement.sequence
    fields = {
        "catalog_version": _format_string(o.mapping.catalog_version),
        "delay_ms": _format_integer((received_us - observed_us) // 1000),
        "event_id": _format_string(o.event_id),
        "ingested_at_ns": _format_integer(ingested_us * 1000),
        "integration_id": _format_string(
            ABSENT_STRING if integration_id is None else integration_id
        ),
        "quality_flags_json": _format_string(_quality_json(o.quality.flags)),
        "quality_policy_version": _format_string(o.mapping.quality_policy_version),
        "quality_provenance_json": _format_string(_quality_json(o.quality.provenance)),
        "quality_state": _format_string(str(o.quality.state)),
        "received_at_ns": _format_integer(received_us * 1000),
        "sequence": _format_integer(ABSENT_SEQUENCE if sequence is None else sequence),
        "source_event_id": _format_string(
            ABSENT_STRING if o.source_event_id is None else o.source_event_id
        ),
        "source_type": _format_string(str(o.source.source_type)),
        "source_unit": _format_string(measurement.source_unit),
        "unit_mapping_version": _format_string(o.mapping.unit_mapping_version),
        "value": _format_float(measurement.value),
    }
    return _line(OBSERVATION_MEASUREMENT, tags, fields, observed_us * 1000)


def _encodable_integer(value: float) -> bool:
    return float(value).is_integer() and _INT64_MIN <= int(value) <= _INT64_MAX


def _legacy_integer(value: float | int) -> int:
    if isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            raise ValueError("legacy integer fields must be integral")
        return int(value)
    return value


def encode_legacy_line(p: LegacyPoint) -> str:
    if not p.fields:
        raise ValueError("a legacy point needs at least one field")
    tags = {
        "device_id": p.device_id,
        "pond_id": p.pond_id,
        "schema_version": LEGACY_SCHEMA_VERSION,
        "source": LEGACY_SOURCE,
        "tenant_id": p.tenant_id,
        "topic": LEGACY_MQTT_TOPIC_TEMPLATE.format(device_id=p.device_id),
    }
    fields = {
        key: (
            _format_integer(_legacy_integer(value))
            if key in LEGACY_INTEGER_FIELDS
            else _format_float(value)
        )
        for key, value in p.fields.items()
    }
    return _line(LEGACY_MEASUREMENT, tags, fields, _epoch_microseconds(p.observed_at) * 1000)


def _rank(o: CanonicalObservation) -> tuple[datetime, str]:
    return (o.delivery.ingested_at, o.event_id)


def _legacy_field(o: CanonicalObservation, legacy_pond_by_asset: Mapping[str, str]) -> str | None:
    resolved = o.resolved
    if resolved.component_id != default_component_id_for_legacy_device(
        resolved.tenant_id, resolved.device_id
    ):
        return None
    if resolved.asset_id not in legacy_pond_by_asset:
        return None
    definition = metric_definition(o.observation.metric_key)
    if definition is None or definition.legacy_field is None:
        return None
    if definition.legacy_field in LEGACY_INTEGER_FIELDS and not _encodable_integer(
        o.observation.value
    ):
        return None
    return definition.legacy_field


def is_legacy_eligible(o: CanonicalObservation, legacy_pond_by_asset: Mapping[str, str]) -> bool:
    """D7 eligibility plus legacy integer encodability; callers count the exclusions."""
    return _legacy_field(o, legacy_pond_by_asset) is not None


def build_legacy_points(
    observations: Iterable[CanonicalObservation], legacy_pond_by_asset: Mapping[str, str]
) -> tuple[LegacyPoint, ...]:
    """Group D7-eligible observations into Telegraf-identical points (D8).

    Ineligible observations are skipped, never raised: the caller counts them. A non-integral or
    out-of-int64 value for an integer legacy field (unreachable after `unit-mapping@1`) is
    ineligible too.
    """
    groups: dict[tuple[str, str, str, datetime], list[tuple[str, CanonicalObservation]]] = {}
    for o in observations:
        legacy_field = _legacy_field(o, legacy_pond_by_asset)
        if legacy_field is None:
            continue
        resolved = o.resolved
        pond_id = legacy_pond_by_asset[resolved.asset_id]
        key = (resolved.tenant_id, pond_id, resolved.device_id, o.observation.observed_at)
        groups.setdefault(key, []).append((legacy_field, o))

    points = []
    for (tenant_id, pond_id, device_id, observed_at), members in groups.items():
        winners: dict[str, CanonicalObservation] = {}
        for legacy_field, o in members:
            current = winners.get(legacy_field)
            # `>=`: on a full tie the later entry wins, as InfluxDB keeps the last write.
            if current is None or _rank(o) >= _rank(current):
                winners[legacy_field] = o
        fields: dict[str, float | int] = {}
        for legacy_field, o in winners.items():
            value = o.observation.value
            fields[legacy_field] = (
                _legacy_integer(value) if legacy_field in LEGACY_INTEGER_FIELDS else float(value)
            )
        sequenced = [o for _, o in members if o.observation.sequence is not None]
        if sequenced:
            fields["seq"] = max(reversed(sequenced), key=_rank).observation.sequence
        points.append(
            LegacyPoint(
                tenant_id=tenant_id,
                pond_id=pond_id,
                device_id=device_id,
                observed_at=observed_at,
                fields=dict(sorted(fields.items())),
            )
        )
    return tuple(sorted(points, key=lambda p: (p.tenant_id, p.pond_id, p.device_id, p.observed_at)))


# C0 controls other than \n, \r and \t: JSON writes them as `\b`, `\f` or `\u00XX`, and
# Flux has none of those escapes.
_FLUX_REJECTED_CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def flux_string_literal(value: str) -> str:
    """Build a lossless Flux string literal (P2 plan D19): unescaped JSON, every `$` as `\\x24`.

    The literal decodes to exactly `value` and never interpolates. A C0 control other than
    `\\n`, `\\r` and `\\t`, or a lone surrogate, cannot round-trip and raises `ValueError`.
    """
    control = _FLUX_REJECTED_CONTROL.search(value)
    if control is not None:
        raise ValueError(f"Flux string literal holds control character U+{ord(control[0]):04X}")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ValueError("Flux string literal is not valid Unicode text") from error
    return json.dumps(value, ensure_ascii=False).replace("$", "\\x24")
