import dataclasses
import json
import math
import re
from datetime import UTC, datetime, timedelta

import pytest
from limnopulse_api.domain.telemetry_v2 import (
    MAX_TELEMETRY_TIME,
    MIN_TELEMETRY_TIME,
    CanonicalObservation,
    MetricValueType,
    NormalizationVersions,
    ObservationDelivery,
    ObservationMeasurement,
    ObservationQuality,
    ObservationResolution,
    ObservationSource,
    QualityFlag,
    QualityProvenance,
    QualityState,
    SourceType,
    observation_event_id,
)
from limnopulse_api.domain.telemetry_v2_schema import (
    LEGACY_INTEGER_FIELDS,
    LEGACY_MEASUREMENT,
    LEGACY_TAG_KEYS,
    OBSERVATION_FIELD_KEYS,
    OBSERVATION_MEASUREMENT,
    OBSERVATION_REQUIRED_TAG_KEYS,
    OBSERVATION_TAG_KEYS,
    LegacyPoint,
    build_legacy_points,
    encode_legacy_line,
    encode_observation_line,
    flux_string_literal,
    is_legacy_eligible,
    observation_tags,
    validate_observation_tags,
)

from limnopulse_api.domain.ids import (
    asset_id_for_legacy_pond,
    default_component_id_for_legacy_device,
    default_probe_profile_id_for_legacy_device,
    default_site_id,
    deployment_id_for_legacy_device,
)

TENANT = "tnt_local_001"
DEVICE = "local-device-001"
POND = "pond_local_001"
SITE = default_site_id(TENANT)
ASSET = asset_id_for_legacy_pond(TENANT, POND)
COMPONENT = default_component_id_for_legacy_device(TENANT, DEVICE)
DEPLOYMENT = deployment_id_for_legacy_device(TENANT, DEVICE)
PROBE = default_probe_profile_id_for_legacy_device(TENANT, DEVICE)
NATIVE_COMPONENT = "cmp_" + "c" * 32
INTEGRATION = "di_" + "a" * 32
OBSERVED = datetime(2026, 7, 9, 12, 0, tzinfo=UTC)
OBSERVED_NS = 1_783_598_400_000_000_000
BASE_PROVENANCE = (QualityProvenance.SCHEMA_VALIDATION, QualityProvenance.UNIT_MAPPING)
LEGACY_POND_BY_ASSET = {ASSET: POND}

UNITS = {
    "water.temperature": ("degC", MetricValueType.FLOAT),
    "water.ph": ("pH", MetricValueType.FLOAT),
    "water.dissolved_oxygen": ("mg/L", MetricValueType.FLOAT),
    "water.turbidity": ("NTU", MetricValueType.FLOAT),
    "water.salinity": ("ppt", MetricValueType.FLOAT),
    "device.battery_voltage": ("V", MetricValueType.FLOAT),
    "device.rssi": ("dBm", MetricValueType.INTEGER),
}
LEGACY_SAMPLE = {
    "water.temperature": 25.1,
    "water.ph": 7.2,
    "water.dissolved_oxygen": 6.4,
    "water.turbidity": 3.1,
    "water.salinity": 11.8,
    "device.battery_voltage": 3.82,
    "device.rssi": -67.0,
}


def observation(
    metric_key: str = "water.temperature",
    value: float = 25.1,
    *,
    tenant_id: str = TENANT,
    device_id: str = DEVICE,
    component_id: str | None = None,
    asset_id: str = ASSET,
    probe_id: str | None = PROBE,
    observed_at: datetime = OBSERVED,
    received_at: datetime | None = None,
    ingested_at: datetime | None = None,
    sequence: int | None = 1,
    source_unit: str | None = None,
    source_type: SourceType = SourceType.LEGACY_MQTT,
    integration_id: str | None = None,
    source_event_id: str | None = None,
    state: QualityState = QualityState.VALID,
    flags: tuple[QualityFlag, ...] = (),
    provenance: tuple[QualityProvenance, ...] = BASE_PROVENANCE,
) -> CanonicalObservation:
    component_id = component_id or default_component_id_for_legacy_device(tenant_id, device_id)
    canonical_unit, value_type = UNITS[metric_key]
    received_at = received_at or observed_at + timedelta(seconds=2, milliseconds=500)
    ingested_at = ingested_at or observed_at + timedelta(seconds=3)
    return CanonicalObservation(
        schema_version=2,
        event_id=observation_event_id(tenant_id, component_id, metric_key, observed_at),
        source_event_id=source_event_id,
        source=ObservationSource(
            integration_id=integration_id,
            integration_type="local.mosquitto",
            source_type=source_type,
            connector_version=None,
            received_at=received_at,
        ),
        resolved=ObservationResolution(
            tenant_id=tenant_id,
            site_id=default_site_id(tenant_id),
            asset_id=asset_id,
            deployment_id=deployment_id_for_legacy_device(tenant_id, device_id),
            device_id=device_id,
            component_id=component_id,
            probe_id=probe_id,
        ),
        observation=ObservationMeasurement(
            metric_key=metric_key,
            value=value,
            value_type=value_type,
            source_unit=source_unit or canonical_unit,
            canonical_unit=canonical_unit,
            observed_at=observed_at,
            sequence=sequence,
        ),
        delivery=ObservationDelivery(
            ingested_at=ingested_at,
            delayed=QualityFlag.DELAYED in flags,
            replay=QualityFlag.REPLAY in flags,
        ),
        quality=ObservationQuality(
            state=state, flags=flags, confidence=None, provenance=provenance
        ),
        mapping=NormalizationVersions(),
        source_metadata={},
    )


def legacy_sample(*, sequence: int | None = 1, **kwargs: object) -> list[CanonicalObservation]:
    return [
        observation(metric_key, value, sequence=sequence, **kwargs)
        for metric_key, value in LEGACY_SAMPLE.items()
    ]


def replace_measurement(item: CanonicalObservation, **update: object) -> CanonicalObservation:
    return item.model_copy(update={"observation": item.observation.model_copy(update=update)})


def legacy_lines(points: tuple[LegacyPoint, ...]) -> list[str]:
    return sorted(encode_legacy_line(point) for point in points)


def tags_of(line: str) -> dict[str, str]:
    series = line.split(" ", 1)[0]
    return dict(pair.split("=", 1) for pair in series.split(",")[1:])


# --- names ----------------------------------------------------------------------------------


def test_schema_names_are_frozen() -> None:
    assert OBSERVATION_MEASUREMENT == "observation_numeric"
    assert LEGACY_MEASUREMENT == "water_quality"
    assert set(OBSERVATION_TAG_KEYS) == {
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
    assert set(OBSERVATION_REQUIRED_TAG_KEYS) == set(OBSERVATION_TAG_KEYS) - {"probe_id"}
    assert set(OBSERVATION_FIELD_KEYS) == {
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
    assert set(LEGACY_TAG_KEYS) == {
        "device_id",
        "pond_id",
        "schema_version",
        "source",
        "tenant_id",
        "topic",
    }
    assert LEGACY_INTEGER_FIELDS == frozenset({"rssi", "seq"})
    assert isinstance(LEGACY_INTEGER_FIELDS, frozenset)


def test_identity_free_fields_are_never_tags() -> None:
    never_tags = {
        "event_id",
        "source_event_id",
        "integration_id",
        "firmware_version",
        "quality_state",
        "quality_flags_json",
        "quality_provenance_json",
        "source_type",
        "connector_version",
    }
    assert not never_tags & set(OBSERVATION_TAG_KEYS)


# --- observation_numeric line protocol ------------------------------------------------------


def test_encode_observation_line_matches_hand_written_golden() -> None:
    event_id = observation_event_id(TENANT, COMPONENT, "water.temperature", OBSERVED)
    expected = (
        "observation_numeric,"
        f"asset_id={ASSET},component_id={COMPONENT},deployment_id={DEPLOYMENT},"
        f"device_id={DEVICE},metric_key=water.temperature,probe_id={PROBE},"
        f"schema_version=2,site_id={SITE},tenant_id={TENANT} "
        'catalog_version="metric-catalog@1",delay_ms=2500i,'
        f'event_id="{event_id}",ingested_at_ns=1783598403000000000i,integration_id="",'
        'quality_flags_json="[]",quality_policy_version="quality-policy@1",'
        'quality_provenance_json="[\\"schema_validation\\",\\"unit_mapping\\"]",'
        'quality_state="valid",received_at_ns=1783598402500000000i,sequence=1i,'
        'source_event_id="",source_type="legacy_mqtt",source_unit="degC",'
        'unit_mapping_version="unit-mapping@1",value=25.1 '
        f"{OBSERVED_NS}"
    )

    line = encode_observation_line(observation())

    assert line == expected
    assert not line.endswith("\n")
    assert "\n" not in line


def test_encode_observation_line_writes_present_optionals() -> None:
    line = encode_observation_line(
        observation(
            sequence=0,
            integration_id=INTEGRATION,
            source_event_id="evt-7",
            source_type=SourceType.HTTPS,
        )
    )
    assert ",sequence=0i," in line
    assert f',integration_id="{INTEGRATION}",' in line
    assert ',source_event_id="evt-7",' in line
    assert ',source_type="https",' in line


def test_encode_observation_line_writes_sentinels_for_absent_optionals() -> None:
    line = encode_observation_line(
        observation(probe_id=None, sequence=None, integration_id=None, source_event_id=None)
    )
    series, fields, timestamp = line.split(" ")

    assert "probe_id" not in series
    assert set(tags_of(line)) == set(OBSERVATION_REQUIRED_TAG_KEYS)
    assert ",sequence=-1i," in f",{fields},"
    assert ',integration_id="",' in f",{fields},"
    assert ',source_event_id="",' in f",{fields},"
    unquoted = re.sub(r'"(?:[^"\\]|\\.)*"', '""', fields)
    assert [pair.split("=", 1)[0] for pair in unquoted.split(",")] == sorted(OBSERVATION_FIELD_KEYS)
    assert timestamp == str(OBSERVED_NS)


@pytest.mark.parametrize(
    ("metric_key", "value", "encoded"),
    [
        ("device.rssi", -67.0, "value=-67.0"),
        ("water.temperature", 25.0, "value=25.0"),
        ("device.battery_voltage", 4.82, "value=4.82"),
        ("water.dissolved_oxygen", 1e-05, "value=1e-05"),
        ("water.turbidity", 0.0, "value=0.0"),
    ],
)
def test_value_is_always_a_repr_float(metric_key, value, encoded) -> None:
    line = encode_observation_line(observation(metric_key, value))
    assert line.split(" ")[1].endswith(f",{encoded}")


def test_time_is_integer_microseconds_times_one_thousand() -> None:
    last = MAX_TELEMETRY_TIME - timedelta(microseconds=1)
    item = observation(
        observed_at=last,
        received_at=last,
        ingested_at=last,
    )
    expected_ns = ((last - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)) * 1000

    line = encode_observation_line(item)

    assert expected_ns == 9_214_646_399_999_999_000
    assert line.endswith(f" {expected_ns}")
    assert f",received_at_ns={expected_ns}i," in line
    assert f",ingested_at_ns={expected_ns}i," in line
    assert ",delay_ms=0i," in line


def test_sub_second_time_is_exact() -> None:
    observed = OBSERVED + timedelta(microseconds=123_456)
    line = encode_observation_line(observation(observed_at=observed))
    assert line.endswith(" 1783598400123456000")


def test_minimum_time_encodes_as_zero() -> None:
    line = encode_observation_line(
        observation(
            observed_at=MIN_TELEMETRY_TIME,
            received_at=MIN_TELEMETRY_TIME,
            ingested_at=MIN_TELEMETRY_TIME,
        )
    )
    assert line.endswith(" 0")
    assert ",received_at_ns=0i," in line


def test_delay_ms_uses_floor_division_of_microseconds() -> None:
    late = observation(received_at=OBSERVED + timedelta(microseconds=2_999))
    early = observation(received_at=OBSERVED - timedelta(microseconds=1_500))
    assert ",delay_ms=2i," in encode_observation_line(late)
    assert ",delay_ms=-2i," in encode_observation_line(early)


def test_quality_json_fields_are_compact_arrays() -> None:
    line = encode_observation_line(
        observation(
            state=QualityState.OUT_OF_RANGE,
            flags=(QualityFlag.CLOCK_SKEW_FUTURE, QualityFlag.DELAYED, QualityFlag.REPLAY),
            provenance=(
                QualityProvenance.SCHEMA_VALIDATION,
                QualityProvenance.UNIT_MAPPING,
                QualityProvenance.PLAUSIBLE_RANGE,
                QualityProvenance.CLOCK_SKEW,
                QualityProvenance.TIMELINESS,
                QualityProvenance.REPLAY,
            ),
        )
    )
    assert (',quality_flags_json="[\\"clock_skew_future\\",\\"delayed\\",\\"replay\\"]",') in line
    assert (
        ',quality_provenance_json="[\\"schema_validation\\",\\"unit_mapping\\",'
        '\\"plausible_range\\",\\"clock_skew\\",\\"timeliness\\",\\"replay\\"]",'
    ) in line
    assert ',quality_state="out_of_range",' in line


def test_string_fields_escape_backslash_and_quote() -> None:
    item = observation(source_event_id='ev"t\\1')
    item = replace_measurement(item, source_unit='a"b\\c')

    line = encode_observation_line(item)

    assert ',source_event_id="ev\\"t\\\\1",' in line
    assert ',source_unit="a\\"b\\\\c",' in line


def test_integers_outside_signed_64_bit_range_are_rejected() -> None:
    maximum = replace_measurement(observation(), sequence=2**63 - 1)
    assert ",sequence=9223372036854775807i," in encode_observation_line(maximum)

    for sequence in (2**63, -(2**63) - 1):
        with pytest.raises(ValueError):
            encode_observation_line(replace_measurement(observation(), sequence=sequence))


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_non_finite_values_are_rejected(value) -> None:
    with pytest.raises(ValueError):
        encode_observation_line(replace_measurement(observation(), value=value))


def _with_quality(item: CanonicalObservation, **update: object) -> CanonicalObservation:
    return item.model_copy(update={"quality": item.quality.model_copy(update=update)})


def test_quality_json_over_512_bytes_is_rejected() -> None:
    oversized_flags = tuple(sorted(QualityFlag)) * 8
    oversized_provenance = tuple(QualityProvenance) * 8
    assert len(json.dumps([flag.value for flag in oversized_flags], separators=(",", ":"))) > 512
    assert (
        len(json.dumps([item.value for item in oversized_provenance], separators=(",", ":"))) > 512
    )

    # The encoder serializes the tuples exactly as given (defense in depth); the byte bound is the
    # rejection reason, not ordering or uniqueness.
    with pytest.raises(ValueError, match="512"):
        encode_observation_line(_with_quality(observation(), flags=oversized_flags))
    with pytest.raises(ValueError, match="512"):
        encode_observation_line(_with_quality(observation(), provenance=oversized_provenance))


@pytest.mark.parametrize(
    "moment",
    [MIN_TELEMETRY_TIME - timedelta(microseconds=1), MAX_TELEMETRY_TIME],
)
@pytest.mark.parametrize("target", ["observed_at", "received_at", "ingested_at"])
def test_datetimes_outside_telemetry_range_are_rejected(target, moment) -> None:
    item = observation()
    if target == "observed_at":
        item = replace_measurement(item, observed_at=moment)
    elif target == "received_at":
        item = item.model_copy(
            update={"source": item.source.model_copy(update={"received_at": moment})}
        )
    else:
        item = item.model_copy(
            update={"delivery": item.delivery.model_copy(update={"ingested_at": moment})}
        )

    with pytest.raises(ValueError):
        encode_observation_line(item)


# --- cardinality guard ----------------------------------------------------------------------


def test_observation_tags_are_identity_and_ownership_only() -> None:
    assert observation_tags(observation()) == {
        "asset_id": ASSET,
        "component_id": COMPONENT,
        "deployment_id": DEPLOYMENT,
        "device_id": DEVICE,
        "metric_key": "water.temperature",
        "probe_id": PROBE,
        "schema_version": "2",
        "site_id": SITE,
        "tenant_id": TENANT,
    }
    assert "probe_id" not in observation_tags(observation(probe_id=None))


@pytest.mark.parametrize(
    "resolved_update",
    [
        {"component_id": "prb_" + "1" * 32},
        {"tenant_id": "bad tenant"},
        {"asset_id": "site_" + "1" * 32},
    ],
)
def test_encoder_applies_the_cardinality_guard(resolved_update) -> None:
    item = observation()
    item = item.model_copy(update={"resolved": item.resolved.model_copy(update=resolved_update)})
    with pytest.raises(ValueError):
        encode_observation_line(item)


def test_encoder_rejects_non_catalog_metric() -> None:
    with pytest.raises(ValueError):
        encode_observation_line(replace_measurement(observation(), metric_key="water.chlorophyll"))


def test_native_probe_tags_pass_the_guard() -> None:
    tags = observation_tags(observation(component_id=NATIVE_COMPONENT, probe_id="prb_" + "f" * 32))
    validate_observation_tags(tags)
    assert tags["component_id"] == NATIVE_COMPONENT


def test_valid_tags_pass_the_guard() -> None:
    validate_observation_tags(observation_tags(observation()))
    validate_observation_tags(observation_tags(observation(probe_id=None)))
    validate_observation_tags({**observation_tags(observation()), "device_id": "dev_" + "d" * 32})


@pytest.mark.parametrize(
    "extra",
    [
        "quality_state",
        "source_type",
        "event_id",
        "source_event_id",
        "integration_id",
        "firmware_version",
        "connector_version",
        "pond_id",
        "unknown",
    ],
)
def test_guard_rejects_unknown_tags(extra) -> None:
    with pytest.raises(ValueError):
        validate_observation_tags({**observation_tags(observation()), extra: "x"})


@pytest.mark.parametrize(
    "missing",
    [
        "asset_id",
        "component_id",
        "deployment_id",
        "device_id",
        "metric_key",
        "schema_version",
        "site_id",
        "tenant_id",
    ],
)
def test_guard_rejects_missing_required_tags(missing) -> None:
    tags = observation_tags(observation())
    del tags[missing]
    with pytest.raises(ValueError):
        validate_observation_tags(tags)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("tenant_id", "bad tenant"),
        ("tenant_id", "tnt#1"),
        ("site_id", "ast_" + "1" * 32),
        ("asset_id", "site_" + "1" * 32),
        ("asset_id", "ast_" + "1" * 31),
        ("deployment_id", "dep_" + "G" * 32),
        ("device_id", "dev_short"),
        ("device_id", "bad device"),
        ("component_id", "prb_" + "1" * 32),
        ("probe_id", "cmp_" + "1" * 32),
        ("metric_key", "water.chlorophyll"),
        ("metric_key", "temp_c"),
        ("schema_version", "1"),
        ("schema_version", "02"),
        ("schema_version", ""),
    ],
)
def test_guard_rejects_invalid_tag_values(key, value) -> None:
    with pytest.raises(ValueError):
        validate_observation_tags({**observation_tags(observation()), key: value})


# --- legacy dual-write point ----------------------------------------------------------------


def test_legacy_point_is_a_frozen_dataclass() -> None:
    assert dataclasses.is_dataclass(LegacyPoint)
    assert [field.name for field in dataclasses.fields(LegacyPoint)] == [
        "tenant_id",
        "pond_id",
        "device_id",
        "observed_at",
        "fields",
    ]
    point = LegacyPoint(
        tenant_id=TENANT, pond_id=POND, device_id=DEVICE, observed_at=OBSERVED, fields={"ph": 7.2}
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        point.pond_id = "other"  # type: ignore[misc]


def test_encode_legacy_line_matches_telegraf_shape() -> None:
    point = LegacyPoint(
        tenant_id=TENANT,
        pond_id=POND,
        device_id=DEVICE,
        observed_at=OBSERVED + timedelta(milliseconds=750),
        fields={"temp_c": 25.0, "rssi": -67, "seq": 1, "do_mg_l": 1e-05},
    )
    assert encode_legacy_line(point) == (
        "water_quality,device_id=local-device-001,pond_id=pond_local_001,schema_version=1,"
        "source=mqtt,tenant_id=tnt_local_001,"
        "topic=limnopulse/v1/devices/local-device-001/readings "
        "do_mg_l=1e-05,rssi=-67i,seq=1i,temp_c=25.0 1783598400750000000"
    )


@pytest.mark.parametrize(
    ("fields", "observed_at"),
    [
        ({"ph": math.nan}, OBSERVED),
        ({"ph": math.inf}, OBSERVED),
        ({"seq": 2**63}, OBSERVED),
        ({"rssi": -(2**63) - 1}, OBSERVED),
        ({"ph": 7.2}, MAX_TELEMETRY_TIME),
        ({"ph": 7.2}, MIN_TELEMETRY_TIME - timedelta(microseconds=1)),
    ],
)
def test_encode_legacy_line_rejects_unencodable_points(fields, observed_at) -> None:
    point = LegacyPoint(
        tenant_id=TENANT, pond_id=POND, device_id=DEVICE, observed_at=observed_at, fields=fields
    )
    with pytest.raises(ValueError):
        encode_legacy_line(point)


def test_encode_legacy_line_coerces_field_types_by_legacy_schema() -> None:
    point = LegacyPoint(
        tenant_id=TENANT,
        pond_id=POND,
        device_id=DEVICE,
        observed_at=OBSERVED,
        fields={"temp_c": 25, "rssi": -67.0, "seq": 3},
    )
    assert encode_legacy_line(point).endswith(" rssi=-67i,seq=3i,temp_c=25.0 1783598400000000000")


def test_encode_legacy_line_escapes_tag_values() -> None:
    point = LegacyPoint(
        tenant_id=TENANT,
        pond_id="pond a,b=c",
        device_id=DEVICE,
        observed_at=OBSERVED,
        fields={"ph": 7.2},
    )
    assert ",pond_id=pond\\ a\\,b\\=c," in encode_legacy_line(point)


def test_build_legacy_points_reproduces_legacy_sample_golden() -> None:
    points = build_legacy_points(legacy_sample(), LEGACY_POND_BY_ASSET)

    assert isinstance(points, tuple)
    assert legacy_lines(points) == [
        (
            "water_quality,device_id=local-device-001,pond_id=pond_local_001,schema_version=1,"
            "source=mqtt,tenant_id=tnt_local_001,"
            "topic=limnopulse/v1/devices/local-device-001/readings "
            "battery_v=3.82,do_mg_l=6.4,ph=7.2,rssi=-67i,salinity_ppt=11.8,seq=1i,"
            "temp_c=25.1,turbidity_ntu=3.1 1783598400000000000"
        )
    ]
    (point,) = points
    assert type(point.fields["rssi"]) is int
    assert type(point.fields["seq"]) is int
    assert type(point.fields["temp_c"]) is float
    assert point.observed_at == OBSERVED


@pytest.mark.parametrize("source_type", list(SourceType))
def test_legacy_identity_is_constant_for_every_source_type(source_type) -> None:
    points = build_legacy_points(legacy_sample(source_type=source_type), LEGACY_POND_BY_ASSET)
    (line,) = legacy_lines(points)
    assert tags_of(line) == {
        "device_id": DEVICE,
        "pond_id": POND,
        "schema_version": "1",
        "source": "mqtt",
        "tenant_id": TENANT,
        "topic": "limnopulse/v1/devices/local-device-001/readings",
    }


def test_build_legacy_points_keeps_exact_sub_second_time() -> None:
    observed = OBSERVED + timedelta(microseconds=750_001)
    (line,) = legacy_lines(
        build_legacy_points(legacy_sample(observed_at=observed), LEGACY_POND_BY_ASSET)
    )
    assert line.endswith(" 1783598400750001000")


def test_d7_excludes_native_components_without_raising() -> None:
    native = [
        observation("water.dissolved_oxygen", 6.4, component_id=NATIVE_COMPONENT),
        observation("water.ph", 7.2, component_id=NATIVE_COMPONENT),
    ]
    assert build_legacy_points(native, LEGACY_POND_BY_ASSET) == ()

    mixed = native + [observation("water.temperature", 25.1)]
    (line,) = legacy_lines(build_legacy_points(mixed, LEGACY_POND_BY_ASSET))
    assert " seq=1i,temp_c=25.1 " in line
    assert "do_mg_l" not in line


def test_d7_excludes_default_component_of_another_device() -> None:
    foreign = observation(
        component_id=default_component_id_for_legacy_device(TENANT, "local-device-002")
    )
    assert build_legacy_points([foreign], LEGACY_POND_BY_ASSET) == ()


def test_d7_excludes_metrics_without_legacy_field() -> None:
    no_legacy_field = replace_measurement(observation(), metric_key="water.chlorophyll")
    assert build_legacy_points([no_legacy_field], LEGACY_POND_BY_ASSET) == ()


def test_d7_excludes_assets_without_legacy_pond() -> None:
    other_asset = "ast_" + "e" * 32
    items = legacy_sample(asset_id=other_asset)
    assert build_legacy_points(items, LEGACY_POND_BY_ASSET) == ()
    assert build_legacy_points(legacy_sample(), {}) == ()


def test_empty_input_builds_no_points() -> None:
    assert build_legacy_points([], LEGACY_POND_BY_ASSET) == ()


def test_build_legacy_points_groups_by_tenant_pond_device_and_exact_time() -> None:
    other_device = "local-device-002"
    other_pond_asset = asset_id_for_legacy_pond(TENANT, "pond_other")
    other_tenant = "tnt_other_001"
    other_tenant_asset = asset_id_for_legacy_pond(other_tenant, POND)
    items = [
        observation("water.ph", 7.2),
        observation("water.temperature", 25.1),
        observation("water.ph", 7.3, device_id=other_device),
        observation("water.ph", 7.4, observed_at=OBSERVED + timedelta(microseconds=1)),
        observation("water.ph", 7.5, asset_id=other_pond_asset),
        observation("water.ph", 7.6, tenant_id=other_tenant, asset_id=other_tenant_asset),
    ]
    mapping = {ASSET: POND, other_pond_asset: "pond_other", other_tenant_asset: POND}

    points = build_legacy_points(items, mapping)

    assert len(points) == 5
    keyed = {
        (point.tenant_id, point.pond_id, point.device_id, point.observed_at): dict(point.fields)
        for point in points
    }
    assert keyed[(TENANT, POND, DEVICE, OBSERVED)] == {"ph": 7.2, "temp_c": 25.1, "seq": 1}
    assert keyed[(TENANT, POND, other_device, OBSERVED)] == {"ph": 7.3, "seq": 1}
    assert keyed[(TENANT, POND, DEVICE, OBSERVED + timedelta(microseconds=1))] == {
        "ph": 7.4,
        "seq": 1,
    }
    assert keyed[(TENANT, "pond_other", DEVICE, OBSERVED)] == {"ph": 7.5, "seq": 1}
    assert keyed[(other_tenant, POND, DEVICE, OBSERVED)] == {"ph": 7.6, "seq": 1}


@pytest.mark.parametrize("reverse", [False, True])
def test_conflicting_field_resolves_to_latest_ingested_at(reverse) -> None:
    older = observation(
        "water.temperature", 20.0, sequence=4, ingested_at=OBSERVED + timedelta(seconds=5)
    )
    newer = observation(
        "water.temperature", 21.0, sequence=5, ingested_at=OBSERVED + timedelta(seconds=9)
    )
    items = [newer, older] if reverse else [older, newer]

    (point,) = build_legacy_points(items, LEGACY_POND_BY_ASSET)

    assert dict(point.fields) == {"temp_c": 21.0, "seq": 5}


@pytest.mark.parametrize("reverse", [False, True])
def test_seq_comes_from_latest_observation_with_a_sequence(reverse) -> None:
    items = [
        observation(
            "water.temperature", 25.1, sequence=None, ingested_at=OBSERVED + timedelta(seconds=9)
        ),
        observation("water.ph", 7.2, sequence=5, ingested_at=OBSERVED + timedelta(seconds=8)),
        observation(
            "water.dissolved_oxygen", 6.4, sequence=9, ingested_at=OBSERVED + timedelta(seconds=7)
        ),
    ]
    if reverse:
        items.reverse()

    (point,) = build_legacy_points(items, LEGACY_POND_BY_ASSET)

    assert dict(point.fields) == {"temp_c": 25.1, "ph": 7.2, "do_mg_l": 6.4, "seq": 5}


@pytest.mark.parametrize("reverse", [False, True])
def test_seq_tie_on_ingested_at_breaks_by_greater_event_id(reverse) -> None:
    ingested = OBSERVED + timedelta(seconds=8)
    first = observation("water.ph", 7.2, sequence=11, ingested_at=ingested)
    second = observation("water.salinity", 11.8, sequence=22, ingested_at=ingested)
    winner = max((first, second), key=lambda item: item.event_id)
    items = [second, first] if reverse else [first, second]

    (point,) = build_legacy_points(items, LEGACY_POND_BY_ASSET)

    assert point.fields["seq"] == winner.observation.sequence


def test_seq_is_omitted_when_every_sequence_is_absent() -> None:
    (point,) = build_legacy_points(legacy_sample(sequence=None), LEGACY_POND_BY_ASSET)
    assert "seq" not in point.fields
    assert " seq=" not in encode_legacy_line(point)
    assert ",seq=" not in encode_legacy_line(point)


def test_legacy_rssi_is_an_integer_field() -> None:
    (point,) = build_legacy_points(
        [observation("device.rssi", -67.0, sequence=None)], LEGACY_POND_BY_ASSET
    )
    assert dict(point.fields) == {"rssi": -67}
    assert type(point.fields["rssi"]) is int
    assert encode_legacy_line(point).endswith(" rssi=-67i 1783598400000000000")


# --- flux literal ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("plain", '"plain"'),
        ('say "hi"', '"say \\"hi\\""'),
        ("back\\slash", '"back\\\\slash"'),
        ("line\nbreak", '"line\\nbreak"'),
        ("${x}", '"\\${x}"'),
        ("a${b}c${d}", '"a\\${b}c\\${d}"'),
        ("raw$archive", '"raw$archive"'),
        ("$", '"$"'),
        ("{x}", '"{x}"'),
    ],
)
def test_flux_string_literal_escapes_json_and_interpolation(value, expected) -> None:
    assert flux_string_literal(value) == expected


@pytest.mark.parametrize("value", ["tnt_local_001", 'q"\\\n\t', "x${y}$z", "é"])
def test_flux_string_literal_is_json_dumps_with_interpolation_escape(value) -> None:
    assert flux_string_literal(value) == json.dumps(value).replace("${", "\\${")


# --- review regressions (added after approval of 029c55c) -----------------------------------


def test_non_integral_rssi_is_ineligible_not_raised() -> None:
    items = [
        observation("device.rssi", -67.5, sequence=None),
        observation("water.ph", 7.2, sequence=None),
    ]
    (point,) = build_legacy_points(items, LEGACY_POND_BY_ASSET)
    assert dict(point.fields) == {"ph": 7.2}
    assert build_legacy_points(items[:1], LEGACY_POND_BY_ASSET) == ()


@pytest.mark.parametrize("value", [float(2**63), -float(2**64)])
def test_out_of_int64_rssi_is_ineligible_not_raised(value) -> None:
    items = [
        observation("device.rssi", value, sequence=None),
        observation("water.ph", 7.2, sequence=None),
    ]
    (point,) = build_legacy_points(items, LEGACY_POND_BY_ASSET)
    assert dict(point.fields) == {"ph": 7.2}
    encode_legacy_line(point)


# --- orchestrator PR audit regressions (pre-authorized) -------------------------------------


def test_full_tie_keeps_the_last_observation_like_influxdb() -> None:
    ingested = OBSERVED + timedelta(seconds=5)
    first = observation("water.ph", 7.0, sequence=1, ingested_at=ingested)
    second = observation("water.ph", 8.0, sequence=2, ingested_at=ingested)
    assert first.event_id == second.event_id

    (point,) = build_legacy_points([first, second], LEGACY_POND_BY_ASSET)
    assert dict(point.fields) == {"ph": 8.0, "seq": 2}
    (point,) = build_legacy_points([second, first], LEGACY_POND_BY_ASSET)
    assert dict(point.fields) == {"ph": 7.0, "seq": 1}


@pytest.mark.parametrize(
    ("item", "eligible"),
    [
        (observation("water.ph", 7.2), True),
        (observation("device.rssi", -67.0), True),
        (observation("water.ph", 7.2, component_id=NATIVE_COMPONENT), False),
        (observation("water.ph", 7.2, asset_id="ast_" + "e" * 32), False),
        (replace_measurement(observation(), metric_key="water.chlorophyll"), False),
        (observation("device.rssi", -67.5), False),
        (observation("device.rssi", float(2**63)), False),
    ],
)
def test_is_legacy_eligible_mirrors_build_legacy_points(item, eligible) -> None:
    assert is_legacy_eligible(item, LEGACY_POND_BY_ASSET) is eligible
    assert bool(build_legacy_points([item], LEGACY_POND_BY_ASSET)) is eligible


def test_legacy_exclusions_are_countable_from_eligibility() -> None:
    items = legacy_sample() + [
        observation("water.ph", 7.2, component_id=NATIVE_COMPONENT),
        observation("device.rssi", -67.5, observed_at=OBSERVED + timedelta(minutes=1)),
    ]
    excluded = len(items) - sum(is_legacy_eligible(item, LEGACY_POND_BY_ASSET) for item in items)
    assert excluded == 2
    assert len(build_legacy_points(items, LEGACY_POND_BY_ASSET)) == 1
