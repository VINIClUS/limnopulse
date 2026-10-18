import dataclasses
import math
from datetime import UTC, datetime, timedelta, timezone
from uuid import NAMESPACE_URL, uuid5

import pytest
from limnopulse_api.domain.telemetry_v2 import (
    ABSENT_SEQUENCE,
    ABSENT_STRING,
    LEGACY_MQTT_TOPIC_TEMPLATE,
    MAX_EXACT_INTEGER,
    MAX_OBSERVATIONS_PER_BATCH,
    MAX_QUALITY_JSON_FIELD_BYTES,
    MAX_QUARANTINE_EXCERPT_BYTES,
    MAX_SEQUENCE,
    MAX_TELEMETRY_TIME,
    METRIC_CATALOG_VERSION,
    MIN_TELEMETRY_TIME,
    P2_WRITTEN_QUALITY_STATES,
    POINT_QUALITY_STATES,
    QUALITY_POLICY_VERSION,
    SOURCE_METADATA_ALLOWLIST,
    TELEMETRY_SCHEMA_VERSION,
    UNIT_MAPPING_VERSION,
    CanonicalObservation,
    MetricDefinition,
    MetricValueType,
    NormalizationResult,
    NormalizationVersions,
    ObservationDelivery,
    ObservationMeasurement,
    ObservationQuality,
    ObservationResolution,
    ObservationSource,
    QualityFlag,
    QualityProvenance,
    QualityState,
    QuarantinedObservation,
    QuarantineReason,
    SourceObservation,
    SourceType,
    StoredObservation,
    TrustedSourceContext,
    observation_event_id,
)
from pydantic import BaseModel, ValidationError

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
COMPONENT = default_component_id_for_legacy_device(TENANT, DEVICE)
OTHER_COMPONENT = "cmp_" + "1" * 32
SITE = default_site_id(TENANT)
ASSET = asset_id_for_legacy_pond(TENANT, POND)
DEPLOYMENT = deployment_id_for_legacy_device(TENANT, DEVICE)
PROBE = default_probe_profile_id_for_legacy_device(TENANT, DEVICE)
INTEGRATION = "di_" + "a" * 32
OBSERVED = datetime(2026, 7, 9, 12, 0, tzinfo=UTC)
RECEIVED = OBSERVED + timedelta(seconds=2)
INGESTED = OBSERVED + timedelta(seconds=3)
BASE_PROVENANCE = (QualityProvenance.SCHEMA_VALIDATION, QualityProvenance.UNIT_MAPPING)


def source_observation(**overrides: object) -> SourceObservation:
    values: dict[str, object] = {
        "component_id": COMPONENT,
        "metric_key": "water.temperature",
        "value": 25.1,
        "source_unit": "degC",
        "observed_at": OBSERVED,
        "sequence": 1,
        "source_event_id": None,
        "source_metadata": {},
    }
    values.update(overrides)
    return SourceObservation(**values)


def trusted_context(**overrides: object) -> TrustedSourceContext:
    values: dict[str, object] = {
        "tenant_id": TENANT,
        "device_id": DEVICE,
        "integration_id": INTEGRATION,
        "integration_type": "local.mosquitto",
        "source_type": SourceType.LEGACY_MQTT,
        "connector_version": "1.0.0",
        "received_at": RECEIVED,
    }
    values.update(overrides)
    return TrustedSourceContext(**values)


def resolution(**overrides: object) -> ObservationResolution:
    values: dict[str, object] = {
        "tenant_id": TENANT,
        "site_id": SITE,
        "asset_id": ASSET,
        "deployment_id": DEPLOYMENT,
        "device_id": DEVICE,
        "component_id": COMPONENT,
        "probe_id": PROBE,
    }
    values.update(overrides)
    return ObservationResolution(**values)


def measurement(**overrides: object) -> ObservationMeasurement:
    values: dict[str, object] = {
        "metric_key": "water.temperature",
        "value": 25.1,
        "value_type": MetricValueType.FLOAT,
        "source_unit": "degC",
        "canonical_unit": "degC",
        "observed_at": OBSERVED,
        "sequence": 1,
    }
    values.update(overrides)
    return ObservationMeasurement(**values)


def quality(**overrides: object) -> ObservationQuality:
    values: dict[str, object] = {
        "state": QualityState.VALID,
        "flags": (),
        "confidence": None,
        "provenance": BASE_PROVENANCE,
    }
    values.update(overrides)
    return ObservationQuality(**values)


def canonical(
    *,
    resolved: ObservationResolution | None = None,
    observation: ObservationMeasurement | None = None,
    delivery: ObservationDelivery | None = None,
    point_quality: ObservationQuality | None = None,
    **overrides: object,
) -> CanonicalObservation:
    resolved = resolved or resolution()
    observation = observation or measurement()
    values: dict[str, object] = {
        "schema_version": 2,
        "event_id": observation_event_id(
            resolved.tenant_id,
            resolved.component_id,
            observation.metric_key,
            observation.observed_at,
        ),
        "source_event_id": None,
        "source": ObservationSource(
            integration_id=INTEGRATION,
            integration_type="local.mosquitto",
            source_type=SourceType.LEGACY_MQTT,
            connector_version="1.0.0",
            received_at=RECEIVED,
        ),
        "resolved": resolved,
        "observation": observation,
        "delivery": delivery
        or ObservationDelivery(ingested_at=INGESTED, delayed=False, replay=False),
        "quality": point_quality or quality(),
        "mapping": NormalizationVersions(),
        "source_metadata": {},
    }
    values.update(overrides)
    return CanonicalObservation(**values)


def quarantined(**overrides: object) -> QuarantinedObservation:
    values: dict[str, object] = {
        "reason": QuarantineReason.UNIT_UNSUPPORTED,
        "tenant_id": TENANT,
        "device_id": DEVICE,
        "component_id": COMPONENT,
        "metric_key": "water.temperature",
        "observed_at": OBSERVED,
        "received_at": RECEIVED,
        "source_event_id": None,
        "detail": "unit not accepted",
        "source_excerpt_json": '{"unit":"bogus"}',
        "truncated": False,
    }
    values.update(overrides)
    return QuarantinedObservation(**values)


def stored(**overrides: object) -> StoredObservation:
    values: dict[str, object] = {
        "event_id": observation_event_id(TENANT, COMPONENT, "water.temperature", OBSERVED),
        "observed_at": OBSERVED,
        "received_at": RECEIVED,
        "ingested_at": INGESTED,
        "metric_key": "water.temperature",
        "value": 25.1,
        "canonical_unit": "degC",
        "site_id": SITE,
        "asset_id": ASSET,
        "device_id": DEVICE,
        "component_id": COMPONENT,
        "probe_id": PROBE,
        "deployment_id": DEPLOYMENT,
        "quality_state": QualityState.VALID,
        "quality_flags": (),
        "quality_provenance": BASE_PROVENANCE,
        "sequence": 1,
        "source_type": SourceType.LEGACY_MQTT,
    }
    values.update(overrides)
    return StoredObservation(**values)


# --- versions and limits --------------------------------------------------------------------


def test_versions_and_limits_are_frozen() -> None:
    assert TELEMETRY_SCHEMA_VERSION == 2
    assert METRIC_CATALOG_VERSION == "metric-catalog@1"
    assert UNIT_MAPPING_VERSION == "unit-mapping@1"
    assert QUALITY_POLICY_VERSION == "quality-policy@1"
    assert MAX_OBSERVATIONS_PER_BATCH == 1000
    assert MAX_QUALITY_JSON_FIELD_BYTES == 512
    assert MAX_QUARANTINE_EXCERPT_BYTES == 2048
    assert MAX_SEQUENCE == 2**63 - 1
    assert MAX_EXACT_INTEGER == 2**53
    assert MIN_TELEMETRY_TIME == datetime(1970, 1, 1, tzinfo=UTC)
    assert MAX_TELEMETRY_TIME == datetime(2262, 1, 1, tzinfo=UTC)
    assert MIN_TELEMETRY_TIME.utcoffset() == timedelta(0)
    assert MAX_TELEMETRY_TIME.utcoffset() == timedelta(0)
    assert ABSENT_SEQUENCE == -1
    assert ABSENT_STRING == ""
    assert SOURCE_METADATA_ALLOWLIST == frozenset({"firmware_version"})
    assert isinstance(SOURCE_METADATA_ALLOWLIST, frozenset)
    assert LEGACY_MQTT_TOPIC_TEMPLATE == "limnopulse/v1/devices/{device_id}/readings"


# --- enumerations ---------------------------------------------------------------------------


def test_frozen_telemetry_enum_values() -> None:
    assert [item.value for item in MetricValueType] == ["float", "integer"]
    assert [item.value for item in SourceType] == [
        "legacy_mqtt",
        "https",
        "mqtt",
        "vendor_webhook",
        "vendor_poll",
        "edge",
    ]
    assert [item.value for item in QualityState] == [
        "valid",
        "suspect",
        "stale",
        "out_of_range",
        "uncalibrated",
        "calibration_unknown",
        "duplicate",
        "delayed",
        "impossible_jump",
        "missing",
    ]
    assert [item.value for item in QualityFlag] == [
        "delayed",
        "clock_skew_future",
        "clock_skew_past_extreme",
        "replay",
        "duplicate",
        "sequence_gap",
        "sequence_regression",
    ]
    assert [item.value for item in QualityProvenance] == [
        "schema_validation",
        "unit_mapping",
        "plausible_range",
        "clock_skew",
        "timeliness",
        "replay",
    ]
    assert [item.value for item in QuarantineReason] == [
        "metric_unknown",
        "unit_unsupported",
        "value_invalid",
        "observed_at_invalid",
        "component_not_owned",
        "component_not_measuring",
        "deployment_unresolved",
    ]


@pytest.mark.parametrize(
    "enum_type",
    [MetricValueType, SourceType, QualityState, QualityFlag, QualityProvenance, QuarantineReason],
)
def test_enum_member_names_are_upper_cased_values(enum_type) -> None:
    for item in enum_type:
        assert item.name == item.value.upper()
        assert isinstance(item, str)


def test_quality_state_subsets_are_frozen() -> None:
    assert POINT_QUALITY_STATES == frozenset(
        {
            QualityState.VALID,
            QualityState.SUSPECT,
            QualityState.OUT_OF_RANGE,
            QualityState.IMPOSSIBLE_JUMP,
            QualityState.UNCALIBRATED,
            QualityState.CALIBRATION_UNKNOWN,
        }
    )
    assert P2_WRITTEN_QUALITY_STATES == frozenset({QualityState.VALID, QualityState.OUT_OF_RANGE})
    assert isinstance(POINT_QUALITY_STATES, frozenset)
    assert isinstance(P2_WRITTEN_QUALITY_STATES, frozenset)


# --- model configuration --------------------------------------------------------------------

MODELS = [
    MetricDefinition,
    TrustedSourceContext,
    SourceObservation,
    ObservationSource,
    ObservationResolution,
    ObservationMeasurement,
    ObservationDelivery,
    ObservationQuality,
    NormalizationVersions,
    CanonicalObservation,
    QuarantinedObservation,
    StoredObservation,
]


@pytest.mark.parametrize("model", MODELS)
def test_models_are_frozen_pydantic_and_forbid_extra(model) -> None:
    assert issubclass(model, BaseModel)
    assert model.model_config["extra"] == "forbid"
    assert model.model_config["frozen"] is True


def test_model_field_sets_are_frozen() -> None:
    assert list(MetricDefinition.model_fields) == [
        "metric_key",
        "display_name",
        "canonical_unit",
        "value_type",
        "plausible_min",
        "plausible_max",
        "legacy_field",
    ]
    assert set(TrustedSourceContext.model_fields) == {
        "tenant_id",
        "device_id",
        "integration_id",
        "integration_type",
        "source_type",
        "connector_version",
        "received_at",
        "replay",
        "mapping_versions",
    }
    assert set(ObservationSource.model_fields) == {
        "integration_id",
        "integration_type",
        "source_type",
        "connector_version",
        "received_at",
    }
    assert set(ObservationResolution.model_fields) == {
        "tenant_id",
        "site_id",
        "asset_id",
        "deployment_id",
        "device_id",
        "component_id",
        "probe_id",
    }
    assert set(ObservationMeasurement.model_fields) == {
        "metric_key",
        "value",
        "value_type",
        "source_unit",
        "canonical_unit",
        "observed_at",
        "sequence",
    }
    assert set(ObservationDelivery.model_fields) == {"ingested_at", "delayed", "replay"}
    assert set(ObservationQuality.model_fields) == {"state", "flags", "confidence", "provenance"}
    assert set(NormalizationVersions.model_fields) == {
        "catalog_version",
        "unit_mapping_version",
        "quality_policy_version",
    }
    assert set(CanonicalObservation.model_fields) == {
        "schema_version",
        "event_id",
        "source_event_id",
        "source",
        "resolved",
        "observation",
        "delivery",
        "quality",
        "mapping",
        "source_metadata",
    }
    assert set(QuarantinedObservation.model_fields) == {
        "reason",
        "tenant_id",
        "device_id",
        "component_id",
        "metric_key",
        "observed_at",
        "received_at",
        "source_event_id",
        "detail",
        "source_excerpt_json",
        "truncated",
    }
    assert set(StoredObservation.model_fields) == {
        "event_id",
        "observed_at",
        "received_at",
        "ingested_at",
        "metric_key",
        "value",
        "canonical_unit",
        "site_id",
        "asset_id",
        "device_id",
        "component_id",
        "probe_id",
        "deployment_id",
        "quality_state",
        "quality_flags",
        "quality_provenance",
        "sequence",
        "source_type",
    }


# --- trusted source context -----------------------------------------------------------------


def test_trusted_source_context_defaults_and_valid_shape() -> None:
    context = trusted_context()
    assert context.replay is False
    assert context.mapping_versions is None
    assert trusted_context(integration_id=None, connector_version=None).integration_id is None
    pinned = NormalizationVersions(catalog_version="metric-catalog@0")
    assert trusted_context(replay=True, mapping_versions=pinned).mapping_versions == pinned


@pytest.mark.parametrize(
    "overrides",
    [
        {"tenant_id": "bad#tenant"},
        {"tenant_id": ""},
        {"device_id": "dev_not-hex"},
        {"device_id": "bad device"},
        {"integration_id": "ia_" + "a" * 32},
        {"integration_id": "di_" + "A" * 32},
        {"integration_type": "AWS IoT"},
        {"integration_type": ""},
        {"source_type": "carrier_pigeon"},
        {"connector_version": "v" * 65},
        {"received_at": datetime(2026, 7, 9, 12, 0)},
        {"received_at": datetime(1969, 12, 31, 23, 59, 59, tzinfo=UTC)},
        {"tenant": TENANT},
    ],
)
def test_trusted_source_context_rejects_invalid_values(overrides) -> None:
    with pytest.raises(ValidationError):
        trusted_context(**overrides)


def test_trusted_source_context_accepts_connector_version_at_limit() -> None:
    assert trusted_context(connector_version="v" * 64).connector_version == "v" * 64


# --- source observation ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "field", ["tenant_id", "site_id", "asset_id", "deployment_id", "device_id"]
)
def test_source_observation_carries_no_ownership_field(field) -> None:
    assert field not in SourceObservation.model_fields
    with pytest.raises(ValidationError):
        source_observation(**{field: "anything"})


def test_source_observation_field_set_is_frozen() -> None:
    assert set(SourceObservation.model_fields) == {
        "component_id",
        "metric_key",
        "value",
        "source_unit",
        "observed_at",
        "sequence",
        "source_event_id",
        "source_metadata",
    }


def test_source_observation_accepts_strict_numbers_and_preserves_type() -> None:
    assert type(source_observation(value=7).value) is int
    assert type(source_observation(value=7.5).value) is float
    assert source_observation(sequence=None).sequence is None
    assert source_observation(sequence=0).sequence == 0
    assert source_observation(sequence=MAX_SEQUENCE).sequence == MAX_SEQUENCE
    assert source_observation(metric_key="m" * 64).metric_key == "m" * 64
    assert source_observation(source_unit="u").source_unit == "u"
    assert source_observation(source_unit="u" * 16).source_unit == "u" * 16
    assert source_observation(source_event_id="e" * 128).source_event_id == "e" * 128
    metadata = {"firmware_version": "f" * 64}
    assert source_observation(source_metadata=metadata).source_metadata == metadata


@pytest.mark.parametrize(
    "overrides",
    [
        {"value": True},
        {"value": False},
        {"value": "25.1"},
        {"value": None},
        {"value": math.nan},
        {"value": math.inf},
        {"value": -math.inf},
        {"sequence": MAX_SEQUENCE + 1},
        {"sequence": -1},
        {"metric_key": "m" * 65},
        {"source_unit": ""},
        {"source_unit": "u" * 17},
        {"source_event_id": ""},
        {"source_event_id": "e" * 129},
        {"source_event_id": "evt\n1"},
        {"source_event_id": "evt\x001"},
        {"source_metadata": {"location": "north"}},
        {"source_metadata": {"firmware_version": "f" * 65}},
        {"component_id": "ast_" + "1" * 32},
        {"component_id": "cmp_" + "Z" * 32},
        {"component_id": "cmp_123"},
        {"observed_at": datetime(2026, 7, 9, 12, 0)},
        {"observed_at": MIN_TELEMETRY_TIME - timedelta(microseconds=1)},
        {"observed_at": MAX_TELEMETRY_TIME},
    ],
)
def test_source_observation_rejects_invalid_values(overrides) -> None:
    with pytest.raises(ValidationError):
        source_observation(**overrides)


def test_telemetry_time_bounds_are_half_open() -> None:
    assert source_observation(observed_at=MIN_TELEMETRY_TIME).observed_at == MIN_TELEMETRY_TIME
    last = MAX_TELEMETRY_TIME - timedelta(microseconds=1)
    assert source_observation(observed_at=last).observed_at == last


def test_aware_datetimes_are_normalized_to_utc() -> None:
    offset = timezone(timedelta(hours=2))
    local = datetime(2026, 7, 9, 14, 0, tzinfo=offset)

    observation = source_observation(observed_at=local)
    context = trusted_context(received_at=local)

    assert observation.observed_at == OBSERVED
    assert observation.observed_at.utcoffset() == timedelta(0)
    assert context.received_at.utcoffset() == timedelta(0)
    assert observation.observed_at.hour == 12


# --- resolution / measurement / quality -----------------------------------------------------


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("tenant_id", "bad tenant"),
        ("site_id", "ast_" + "1" * 32),
        ("asset_id", "site_" + "1" * 32),
        ("deployment_id", "cmp_" + "1" * 32),
        ("device_id", "dev_short"),
        ("component_id", "prb_" + "1" * 32),
        ("probe_id", "cmp_" + "1" * 32),
    ],
)
def test_resolution_rejects_bad_prefixes(field, value) -> None:
    with pytest.raises(ValidationError):
        resolution(**{field: value})


def test_resolution_probe_is_optional() -> None:
    assert resolution(probe_id=None).probe_id is None


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_measurement_rejects_non_finite_values(value) -> None:
    with pytest.raises(ValidationError):
        measurement(value=value)


def test_measurement_value_is_float() -> None:
    assert type(measurement(value=25.1).value) is float
    assert type(measurement(value=7).value) is float


@pytest.mark.parametrize(
    "overrides",
    [
        {"value": True},
        {"value": False},
        {"value": "25.1"},
        {"value_type": "decimal"},
        {"sequence": MAX_SEQUENCE + 1},
        {"sequence": -1},
        {"observed_at": datetime(2026, 7, 9, 12, 0)},
        {"observed_at": MAX_TELEMETRY_TIME},
        {"observed_at": MIN_TELEMETRY_TIME - timedelta(microseconds=1)},
    ],
)
def test_measurement_rejects_invalid_values(overrides) -> None:
    with pytest.raises(ValidationError):
        measurement(**overrides)


def _observation_source(**overrides: object) -> ObservationSource:
    values: dict[str, object] = {
        "integration_id": INTEGRATION,
        "integration_type": "local.mosquitto",
        "source_type": SourceType.LEGACY_MQTT,
        "connector_version": "1.0.0",
        "received_at": RECEIVED,
    }
    values.update(overrides)
    return ObservationSource(**values)


def test_observation_source_accepts_absent_optionals() -> None:
    source = _observation_source(integration_id=None, connector_version=None)
    assert source.integration_id is None
    assert source.connector_version is None


@pytest.mark.parametrize(
    "overrides",
    [
        {"integration_id": "ia_" + "a" * 32},
        {"integration_type": "AWS IoT"},
        {"source_type": "carrier_pigeon"},
        {"connector_version": "v" * 65},
        {"received_at": datetime(2026, 7, 9, 12, 0, 2)},
        {"received_at": MAX_TELEMETRY_TIME},
    ],
)
def test_observation_source_rejects_invalid_values(overrides) -> None:
    with pytest.raises(ValidationError):
        _observation_source(**overrides)


@pytest.mark.parametrize(
    "state",
    [
        QualityState.STALE,
        QualityState.MISSING,
        QualityState.DUPLICATE,
        QualityState.DELAYED,
    ],
)
def test_quality_rejects_non_point_states(state) -> None:
    with pytest.raises(ValidationError):
        quality(state=state)


@pytest.mark.parametrize("state", sorted(POINT_QUALITY_STATES))
def test_quality_accepts_every_point_state(state) -> None:
    assert quality(state=state).state == state


@pytest.mark.parametrize(
    "overrides",
    [
        {"flags": (QualityFlag.REPLAY, QualityFlag.DELAYED)},
        {"flags": (QualityFlag.DELAYED, QualityFlag.DELAYED)},
        {"provenance": ()},
        {"provenance": (QualityProvenance.UNIT_MAPPING, QualityProvenance.SCHEMA_VALIDATION)},
        {
            "provenance": (
                QualityProvenance.SCHEMA_VALIDATION,
                QualityProvenance.SCHEMA_VALIDATION,
            )
        },
        {
            "provenance": (
                QualityProvenance.SCHEMA_VALIDATION,
                QualityProvenance.UNIT_MAPPING,
                QualityProvenance.REPLAY,
                QualityProvenance.TIMELINESS,
            )
        },
        {"confidence": 0.5},
    ],
)
def test_quality_rejects_unsorted_duplicate_or_misordered_entries(overrides) -> None:
    with pytest.raises(ValidationError):
        quality(**overrides)


def test_quality_accepts_sorted_flags_and_table_ordered_provenance() -> None:
    value = quality(
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
    assert value.confidence is None
    assert isinstance(value.flags, tuple)
    assert isinstance(value.provenance, tuple)


def test_normalization_versions_default_to_pinned_constants() -> None:
    versions = NormalizationVersions()
    assert versions.catalog_version == METRIC_CATALOG_VERSION
    assert versions.unit_mapping_version == UNIT_MAPPING_VERSION
    assert versions.quality_policy_version == QUALITY_POLICY_VERSION


# --- canonical observation ------------------------------------------------------------------


def test_canonical_observation_accepts_consistent_envelope() -> None:
    observation = canonical(
        source_event_id="evt-1",
        source_metadata={"firmware_version": "1.2.3"},
    )
    assert observation.schema_version == 2
    assert observation.event_id.startswith("obs_")
    assert observation.mapping == NormalizationVersions()


@pytest.mark.parametrize(
    ("tenant_id", "component_id", "metric_key", "observed_at"),
    [
        (TENANT, OTHER_COMPONENT, "water.temperature", OBSERVED),
        ("tnt_other", COMPONENT, "water.temperature", OBSERVED),
        (TENANT, COMPONENT, "water.ph", OBSERVED),
        (TENANT, COMPONENT, "water.temperature", OBSERVED + timedelta(microseconds=1)),
    ],
)
def test_canonical_observation_rejects_mismatched_event_id(
    tenant_id, component_id, metric_key, observed_at
) -> None:
    other = observation_event_id(tenant_id, component_id, metric_key, observed_at)
    with pytest.raises(ValidationError, match="event_id"):
        canonical(event_id=other)


@pytest.mark.parametrize(
    "overrides",
    [
        {"schema_version": 1},
        {"event_id": "evt_" + "0" * 32},
        {"source_metadata": {"location": "north"}},
        {"source_event_id": ""},
        {"tenant_id": TENANT},
    ],
)
def test_canonical_observation_rejects_invalid_envelope(overrides) -> None:
    with pytest.raises(ValidationError):
        canonical(**overrides)


@pytest.mark.parametrize(
    ("delayed", "replay", "flags"),
    [
        (True, False, ()),
        (False, False, (QualityFlag.DELAYED,)),
        (False, True, ()),
        (False, False, (QualityFlag.REPLAY,)),
        (True, False, (QualityFlag.REPLAY,)),
    ],
)
def test_canonical_observation_rejects_inconsistent_delivery_mirrors(
    delayed, replay, flags
) -> None:
    with pytest.raises(ValidationError):
        canonical(
            delivery=ObservationDelivery(ingested_at=INGESTED, delayed=delayed, replay=replay),
            point_quality=quality(
                flags=flags,
                provenance=BASE_PROVENANCE
                + ((QualityProvenance.TIMELINESS,) if QualityFlag.DELAYED in flags else ())
                + ((QualityProvenance.REPLAY,) if QualityFlag.REPLAY in flags else ()),
            ),
        )


def test_canonical_observation_accepts_mirrored_delivery_flags() -> None:
    observation = canonical(
        delivery=ObservationDelivery(ingested_at=INGESTED, delayed=True, replay=True),
        point_quality=quality(
            flags=(QualityFlag.CLOCK_SKEW_PAST_EXTREME, QualityFlag.DELAYED, QualityFlag.REPLAY),
            provenance=(
                QualityProvenance.SCHEMA_VALIDATION,
                QualityProvenance.UNIT_MAPPING,
                QualityProvenance.CLOCK_SKEW,
                QualityProvenance.TIMELINESS,
                QualityProvenance.REPLAY,
            ),
        ),
    )
    assert observation.delivery.delayed is True
    assert observation.delivery.replay is True


@pytest.mark.parametrize(
    "ingested_at",
    [datetime(2026, 7, 9, 12, 0, 3), MAX_TELEMETRY_TIME, datetime(1969, 1, 1, tzinfo=UTC)],
)
def test_delivery_rejects_invalid_ingested_at(ingested_at) -> None:
    with pytest.raises(ValidationError):
        ObservationDelivery(ingested_at=ingested_at, delayed=False, replay=False)


# --- identity -------------------------------------------------------------------------------


def test_observation_event_id_matches_locked_formula_and_literal() -> None:
    component = "cmp_" + "0" * 31 + "1"
    expected = (
        "obs_"
        + uuid5(
            NAMESPACE_URL,
            f"limnopulse/v4/observation/{TENANT}/{component}/water.temperature/"
            "2026-07-09T12:00:00.000000Z",
        ).hex
    )

    assert observation_event_id(TENANT, component, "water.temperature", OBSERVED) == expected
    assert expected == "obs_45ea82fa38745a029aa5cd66a3b807d1"


def test_observation_event_id_is_stable_and_discriminating() -> None:
    base = observation_event_id(TENANT, COMPONENT, "water.ph", OBSERVED)
    offset = OBSERVED.astimezone(timezone(timedelta(hours=-3)))

    assert observation_event_id(TENANT, COMPONENT, "water.ph", OBSERVED) == base
    assert observation_event_id(TENANT, COMPONENT, "water.ph", offset) == base
    assert (
        len(
            {
                base,
                observation_event_id("tnt_other", COMPONENT, "water.ph", OBSERVED),
                observation_event_id(TENANT, OTHER_COMPONENT, "water.ph", OBSERVED),
                observation_event_id(TENANT, COMPONENT, "water.temperature", OBSERVED),
                observation_event_id(
                    TENANT, COMPONENT, "water.ph", OBSERVED + timedelta(microseconds=1)
                ),
            }
        )
        == 5
    )


def test_observation_event_id_ignores_versions_quality_and_delivery_times() -> None:
    first = canonical()
    second = canonical(
        source_event_id="redelivered-42",
        source=ObservationSource(
            integration_id=None,
            integration_type="local.mosquitto",
            source_type=SourceType.LEGACY_MQTT,
            connector_version=None,
            received_at=RECEIVED + timedelta(days=40),
        ),
        delivery=ObservationDelivery(
            ingested_at=INGESTED + timedelta(days=41), delayed=True, replay=True
        ),
        point_quality=quality(
            state=QualityState.OUT_OF_RANGE,
            flags=(QualityFlag.CLOCK_SKEW_PAST_EXTREME, QualityFlag.DELAYED, QualityFlag.REPLAY),
            provenance=(
                QualityProvenance.SCHEMA_VALIDATION,
                QualityProvenance.UNIT_MAPPING,
                QualityProvenance.PLAUSIBLE_RANGE,
                QualityProvenance.CLOCK_SKEW,
                QualityProvenance.TIMELINESS,
                QualityProvenance.REPLAY,
            ),
        ),
        mapping=NormalizationVersions(
            catalog_version="metric-catalog@2",
            unit_mapping_version="unit-mapping@2",
            quality_policy_version="quality-policy@2",
        ),
    )

    assert first.event_id == second.event_id


# --- quarantine / stored --------------------------------------------------------------------


def test_quarantined_observation_accepts_bounded_record() -> None:
    record = quarantined(
        component_id=None,
        metric_key=None,
        observed_at=None,
        detail="d" * 256,
        source_excerpt_json="é" * 1024,
        truncated=True,
    )
    assert record.reason == QuarantineReason.UNIT_UNSUPPORTED
    assert record.truncated is True


@pytest.mark.parametrize(
    "overrides",
    [
        {"detail": "d" * 257},
        {"source_excerpt_json": "x" * 2049},
        {"source_excerpt_json": "é" * 1025},
        {"reason": "bogus"},
        {"tenant_id": "bad tenant"},
        {"device_id": "dev_short"},
        {"component_id": "ast_" + "1" * 32},
        {"received_at": datetime(2026, 7, 9, 12, 0, 2)},
        {"received_at": MAX_TELEMETRY_TIME},
        {"observed_at": datetime(2026, 7, 9, 12, 0)},
        {"observed_at": MIN_TELEMETRY_TIME - timedelta(microseconds=1)},
        {"truncated": "maybe"},
        {"tenant": TENANT},
    ],
)
def test_quarantined_observation_rejects_invalid_values(overrides) -> None:
    with pytest.raises(ValidationError):
        quarantined(**overrides)


def test_stored_observation_shape() -> None:
    record = stored(sequence=None, probe_id=None)
    assert record.sequence is None
    assert record.probe_id is None
    assert isinstance(record.quality_flags, tuple)
    assert isinstance(record.quality_provenance, tuple)

    typed = stored(
        quality_state="out_of_range",
        quality_flags=["delayed", "replay"],
        quality_provenance=["schema_validation", "unit_mapping", "timeliness", "replay"],
        source_type="https",
        value=7,
    )
    assert typed.quality_state is QualityState.OUT_OF_RANGE
    assert typed.quality_flags == (QualityFlag.DELAYED, QualityFlag.REPLAY)
    assert all(isinstance(flag, QualityFlag) for flag in typed.quality_flags)
    assert all(isinstance(item, QualityProvenance) for item in typed.quality_provenance)
    assert typed.source_type is SourceType.HTTPS
    assert type(typed.value) is float


@pytest.mark.parametrize(
    "overrides",
    [
        {"observed_at": datetime(2026, 7, 9, 12, 0)},
        {"ingested_at": datetime(2026, 7, 9, 12, 0, 3)},
        {"event_id": "evt_" + "0" * 32},
        {"asset_id": "site_" + "1" * 32},
        {"quality_state": "bogus"},
        {"quality_flags": ("bogus",)},
        {"quality_provenance": ("bogus",)},
        {"source_type": "carrier_pigeon"},
        {"received_at": datetime(2026, 7, 9, 12, 0, 2)},
        {"observed_at": MAX_TELEMETRY_TIME},
        {"component_id": "ast_" + "1" * 32},
        {"deployment_id": "cmp_" + "1" * 32},
        {"probe_id": "cmp_" + "1" * 32},
        {"value": True},
        {"value": math.nan},
        {"sequence": -2},
        {"extra": 1},
    ],
)
def test_stored_observation_rejects_invalid_values(overrides) -> None:
    with pytest.raises(ValidationError):
        stored(**overrides)


# --- normalization result -------------------------------------------------------------------

CLOSED_COUNT_KEYS = (
    {"accepted"}
    | {f"quarantined.{reason.value}" for reason in QuarantineReason}
    | {f"flag.{flag.value}" for flag in QualityFlag}
    | {f"state.{state.value}" for state in QualityState}
)


def test_normalization_result_counts_use_closed_keys() -> None:
    ph = measurement(metric_key="water.ph", value=7.2, source_unit="pH", canonical_unit="pH")
    out_of_range = measurement(
        metric_key="water.dissolved_oxygen",
        value=75.0,
        source_unit="mg/L",
        canonical_unit="mg/L",
    )
    delayed = canonical(
        observation=ph,
        delivery=ObservationDelivery(ingested_at=INGESTED, delayed=True, replay=False),
        point_quality=quality(
            flags=(QualityFlag.DELAYED,),
            provenance=BASE_PROVENANCE + (QualityProvenance.TIMELINESS,),
        ),
    )
    result = NormalizationResult(
        accepted=(
            canonical(),
            delayed,
            canonical(
                observation=out_of_range,
                point_quality=quality(
                    state=QualityState.OUT_OF_RANGE,
                    provenance=BASE_PROVENANCE + (QualityProvenance.PLAUSIBLE_RANGE,),
                ),
            ),
        ),
        quarantined=(
            quarantined(),
            quarantined(),
            quarantined(reason=QuarantineReason.METRIC_UNKNOWN),
        ),
    )

    counts = result.counts()

    assert set(counts) <= CLOSED_COUNT_KEYS
    assert all(isinstance(value, int) for value in counts.values())
    assert {key: value for key, value in counts.items() if value} == {
        "accepted": 3,
        "quarantined.unit_unsupported": 2,
        "quarantined.metric_unknown": 1,
        "flag.delayed": 1,
        "state.valid": 2,
        "state.out_of_range": 1,
    }
    assert not any(TENANT in key or DEVICE in key for key in counts)


def test_normalization_result_counts_for_empty_result() -> None:
    result = NormalizationResult(accepted=(), quarantined=())
    with pytest.raises((ValidationError, dataclasses.FrozenInstanceError, AttributeError)):
        result.accepted = (canonical(),)  # type: ignore[misc]
    counts = result.counts()
    assert set(counts) <= CLOSED_COUNT_KEYS
    assert {key: value for key, value in counts.items() if value} == {}
    assert isinstance(result.accepted, tuple)
    assert isinstance(result.quarantined, tuple)
