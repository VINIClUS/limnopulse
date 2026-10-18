"""Canonical telemetry contract (P2): versions, enums, envelope models and identity.

Device payloads never carry tenant, site, asset or deployment authority: `SourceObservation`
has no ownership field, and ownership is resolved by trusted code into `ObservationResolution`.
"""

import math
import re
from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Literal
from uuid import NAMESPACE_URL, uuid5

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PlainSerializer,
    StrictBool,
    StrictFloat,
    StrictInt,
    model_validator,
)

from limnopulse_api.domain.ids import require_canonical_id
from limnopulse_api.domain.v2_common import (
    DeviceId,
    ProviderKey,
    SafeId,
    format_utc_timestamp,
)

TELEMETRY_SCHEMA_VERSION = 2
METRIC_CATALOG_VERSION = "metric-catalog@1"
UNIT_MAPPING_VERSION = "unit-mapping@1"
QUALITY_POLICY_VERSION = "quality-policy@1"
MAX_OBSERVATIONS_PER_BATCH = 1000
MAX_QUALITY_JSON_FIELD_BYTES = 512
MAX_QUARANTINE_EXCERPT_BYTES = 2048
MAX_SEQUENCE = 2**63 - 1  # signed 64-bit line-protocol integer
MAX_EXACT_INTEGER = 2**53  # |int| >= this is value_invalid (exact float)
MIN_TELEMETRY_TIME = datetime(1970, 1, 1, tzinfo=UTC)  # inclusive
MAX_TELEMETRY_TIME = datetime(2262, 1, 1, tzinfo=UTC)  # exclusive; int64-ns line-protocol safe
ABSENT_SEQUENCE = -1  # field sentinel: optional fields are always written (D5)
ABSENT_STRING = ""  # field sentinel for source_event_id / integration_id
SOURCE_METADATA_ALLOWLIST = frozenset({"firmware_version"})  # values <= 64 chars, never tagged
LEGACY_MQTT_TOPIC_TEMPLATE = "limnopulse/v1/devices/{device_id}/readings"

MAX_SOURCE_METADATA_VALUE_LENGTH = 64


class MetricValueType(StrEnum):
    FLOAT = "float"
    INTEGER = "integer"


class SourceType(StrEnum):
    """Transport class; provider identity stays an opaque `ProviderKey`."""

    LEGACY_MQTT = "legacy_mqtt"
    HTTPS = "https"
    MQTT = "mqtt"
    VENDOR_WEBHOOK = "vendor_webhook"
    VENDOR_POLL = "vendor_poll"
    EDGE = "edge"


class QualityState(StrEnum):
    VALID = "valid"
    SUSPECT = "suspect"
    STALE = "stale"
    OUT_OF_RANGE = "out_of_range"
    UNCALIBRATED = "uncalibrated"
    CALIBRATION_UNKNOWN = "calibration_unknown"
    DUPLICATE = "duplicate"
    DELAYED = "delayed"
    IMPOSSIBLE_JUMP = "impossible_jump"
    MISSING = "missing"


POINT_QUALITY_STATES = frozenset(
    {
        QualityState.VALID,
        QualityState.SUSPECT,
        QualityState.OUT_OF_RANGE,
        QualityState.IMPOSSIBLE_JUMP,
        QualityState.UNCALIBRATED,
        QualityState.CALIBRATION_UNKNOWN,
    }
)
P2_WRITTEN_QUALITY_STATES = frozenset({QualityState.VALID, QualityState.OUT_OF_RANGE})


class QualityFlag(StrEnum):
    DELAYED = "delayed"
    CLOCK_SKEW_FUTURE = "clock_skew_future"
    CLOCK_SKEW_PAST_EXTREME = "clock_skew_past_extreme"
    REPLAY = "replay"
    # Reserved for P3/P6 per-Component state.
    DUPLICATE = "duplicate"
    SEQUENCE_GAP = "sequence_gap"
    SEQUENCE_REGRESSION = "sequence_regression"


class QualityProvenance(StrEnum):
    SCHEMA_VALIDATION = "schema_validation"
    UNIT_MAPPING = "unit_mapping"
    PLAUSIBLE_RANGE = "plausible_range"
    CLOCK_SKEW = "clock_skew"
    TIMELINESS = "timeliness"
    REPLAY = "replay"


class QuarantineReason(StrEnum):
    METRIC_UNKNOWN = "metric_unknown"
    UNIT_UNSUPPORTED = "unit_unsupported"
    VALUE_INVALID = "value_invalid"
    OBSERVED_AT_INVALID = "observed_at_invalid"
    COMPONENT_NOT_OWNED = "component_not_owned"
    COMPONENT_NOT_MEASURING = "component_not_measuring"
    DEPLOYMENT_UNRESOLVED = "deployment_unresolved"


_PROVENANCE_ORDER = {item: index for index, item in enumerate(QualityProvenance)}
_OUT_OF_RANGE = "telemetry timestamp outside [1970-01-01, 2262-01-01) UTC"
# Telegraf parity: Go layout `2006-01-02T15:04:05Z07:00` (uppercase T, optional `.`/`,` fraction,
# `Z` or `+HH:MM`), so v2 never accepts a reading the legacy path rejects.
_ISO_TIMESTAMP = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:[.,](?P<fraction>\d+))?(?:Z|[+-]\d{2}:\d{2})"
)


def require_telemetry_time(value: datetime) -> datetime:
    """Normalize an aware datetime to UTC inside `[MIN_TELEMETRY_TIME, MAX_TELEMETRY_TIME)`."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("telemetry timestamps must be timezone-aware")
    try:
        utc = value.astimezone(UTC)
    except OverflowError:
        raise ValueError(_OUT_OF_RANGE) from None
    if not MIN_TELEMETRY_TIME <= utc < MAX_TELEMETRY_TIME:
        raise ValueError(_OUT_OF_RANGE)
    return utc


def _require_exact_timestamp_input(value: object) -> object:
    """Accept datetimes or ISO-8601 text; never unix numbers or sub-microsecond truncation."""
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        raise ValueError("telemetry timestamps must be datetimes or ISO-8601 strings")
    match = _ISO_TIMESTAMP.fullmatch(value)
    if match is None:
        raise ValueError("telemetry timestamps must be datetimes or ISO-8601 strings")
    fraction = match.group("fraction") or ""
    if fraction[6:].strip("0"):
        raise ValueError("sub-microsecond precision unsupported")
    return value


def _canonical(prefix: str):
    return Annotated[str, AfterValidator(lambda value: require_canonical_id(value, prefix))]


def _require_number(value: object) -> object:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError("value must be a number")
    return value


def _require_finite(value: int | float) -> int | float:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("value must be finite")
    return value


def _require_printable(value: str) -> str:
    if not value.isprintable():
        raise ValueError("value must be printable")
    return value


class ReadOnlyDict(dict[str, str]):
    """A `dict` that refuses mutation yet survives `copy.deepcopy` and `pickle`."""

    def _refuse(self, *args: object, **kwargs: object) -> None:
        raise TypeError("source_metadata is read-only")

    __setitem__ = __delitem__ = __ior__ = _refuse
    clear = pop = popitem = setdefault = update = _refuse

    def __reduce__(self) -> tuple[type["ReadOnlyDict"], tuple[dict[str, str]]]:
        return (type(self), (dict(self),))

    def __copy__(self) -> "ReadOnlyDict":
        return self

    def __deepcopy__(self, memo: dict[int, object]) -> "ReadOnlyDict":
        return self


def _require_metadata(value: dict[str, str]) -> Mapping[str, str]:
    unknown = set(value) - SOURCE_METADATA_ALLOWLIST
    if unknown:
        raise ValueError(f"source_metadata keys not allowed: {sorted(unknown)}")
    if any(len(item) > MAX_SOURCE_METADATA_VALUE_LENGTH for item in value.values()):
        raise ValueError("source_metadata values must be at most 64 characters")
    return ReadOnlyDict(value)


def _require_point_state(value: QualityState) -> QualityState:
    if value not in POINT_QUALITY_STATES:
        raise ValueError(f"{value} is not a point quality state")
    return value


TelemetryDatetime = Annotated[
    datetime,
    BeforeValidator(_require_exact_timestamp_input),
    AfterValidator(require_telemetry_time),
]
SiteId = _canonical("site")
AssetId = _canonical("ast")
ComponentId = _canonical("cmp")
ProbeId = _canonical("prb")
DeploymentId = _canonical("dep")
IntegrationId = _canonical("di")
ObservationEventId = _canonical("obs")
FiniteFloat = Annotated[float, BeforeValidator(_require_number), AfterValidator(_require_finite)]
SourceValue = Annotated[StrictInt | StrictFloat, AfterValidator(_require_finite)]
SequenceNumber = Annotated[StrictInt, Field(ge=0, le=MAX_SEQUENCE)]
MetricKey = Annotated[str, Field(min_length=1, max_length=64)]
UnitCode = Annotated[str, Field(min_length=1, max_length=16), AfterValidator(_require_printable)]
SourceEventId = Annotated[
    str, Field(min_length=1, max_length=128), AfterValidator(_require_printable)
]
VersionTag = Annotated[str, Field(min_length=1), AfterValidator(_require_printable)]
ConnectorVersion = Annotated[str, Field(max_length=64)]
# Read-only after validation (frozen models must not be mutated); serialized as a plain dict.
SourceMetadata = Annotated[
    dict[str, str], AfterValidator(_require_metadata), PlainSerializer(dict, return_type=dict)
]


class _TelemetryModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class MetricDefinition(_TelemetryModel):
    metric_key: str
    display_name: str
    canonical_unit: str
    value_type: MetricValueType
    plausible_min: float
    plausible_max: float
    legacy_field: str | None


class NormalizationVersions(_TelemetryModel):
    catalog_version: VersionTag = METRIC_CATALOG_VERSION
    unit_mapping_version: VersionTag = UNIT_MAPPING_VERSION
    quality_policy_version: VersionTag = QUALITY_POLICY_VERSION


class TrustedSourceContext(_TelemetryModel):
    """Built by trusted code only; a replay carries the first-receipt context (D5)."""

    tenant_id: SafeId
    device_id: DeviceId
    integration_id: IntegrationId | None
    integration_type: ProviderKey
    source_type: SourceType
    connector_version: ConnectorVersion | None
    received_at: TelemetryDatetime
    replay: StrictBool = False
    mapping_versions: NormalizationVersions | None = None


class SourceObservation(_TelemetryModel):
    component_id: ComponentId
    metric_key: MetricKey
    value: SourceValue
    source_unit: UnitCode
    observed_at: TelemetryDatetime
    sequence: SequenceNumber | None = None
    source_event_id: SourceEventId | None = None
    source_metadata: SourceMetadata = Field(default_factory=dict, validate_default=True)


class ObservationSource(_TelemetryModel):
    integration_id: IntegrationId | None
    integration_type: ProviderKey
    source_type: SourceType
    connector_version: ConnectorVersion | None
    received_at: TelemetryDatetime


class ObservationResolution(_TelemetryModel):
    tenant_id: SafeId
    site_id: SiteId
    asset_id: AssetId
    deployment_id: DeploymentId
    device_id: DeviceId
    component_id: ComponentId
    probe_id: ProbeId | None


class ObservationMeasurement(_TelemetryModel):
    metric_key: MetricKey
    value: FiniteFloat
    value_type: MetricValueType
    source_unit: UnitCode
    canonical_unit: UnitCode
    observed_at: TelemetryDatetime
    sequence: SequenceNumber | None


class ObservationDelivery(_TelemetryModel):
    ingested_at: TelemetryDatetime
    delayed: StrictBool
    replay: StrictBool


class ObservationQuality(_TelemetryModel):
    state: Annotated[QualityState, AfterValidator(_require_point_state)]
    flags: tuple[QualityFlag, ...]
    confidence: None = None
    provenance: tuple[QualityProvenance, ...]

    @model_validator(mode="after")
    def validate_ordering(self) -> "ObservationQuality":
        if list(self.flags) != sorted(set(self.flags)):
            raise ValueError("quality flags must be unique and sorted")
        if not self.provenance:
            raise ValueError("quality provenance must not be empty")
        order = [_PROVENANCE_ORDER[item] for item in self.provenance]
        if order != sorted(set(order)):
            raise ValueError("quality provenance must be unique and in table order")
        return self


def observation_event_id(
    tenant_id: str, component_id: str, metric_key: str, observed_at: datetime
) -> str:
    discriminator = (
        f"limnopulse/v4/observation/{tenant_id}/{component_id}/{metric_key}/"
        f"{format_utc_timestamp(observed_at)}"
    )
    return f"obs_{uuid5(NAMESPACE_URL, discriminator).hex}"


class CanonicalObservation(_TelemetryModel):
    schema_version: Literal[2] = 2
    event_id: ObservationEventId
    source_event_id: SourceEventId | None
    source: ObservationSource
    resolved: ObservationResolution
    observation: ObservationMeasurement
    delivery: ObservationDelivery
    quality: ObservationQuality
    mapping: NormalizationVersions = Field(default_factory=NormalizationVersions)
    source_metadata: SourceMetadata = Field(default_factory=dict, validate_default=True)

    @model_validator(mode="after")
    def validate_identity_and_mirrors(self) -> "CanonicalObservation":
        expected = observation_event_id(
            self.resolved.tenant_id,
            self.resolved.component_id,
            self.observation.metric_key,
            self.observation.observed_at,
        )
        if self.event_id != expected:
            raise ValueError("event_id does not match observation_event_id(...)")
        if self.delivery.delayed != (QualityFlag.DELAYED in self.quality.flags):
            raise ValueError("delivery.delayed must mirror the delayed quality flag")
        if self.delivery.replay != (QualityFlag.REPLAY in self.quality.flags):
            raise ValueError("delivery.replay must mirror the replay quality flag")
        return self


def _require_excerpt_bytes(value: str) -> str:
    if len(value.encode("utf-8")) > MAX_QUARANTINE_EXCERPT_BYTES:
        raise ValueError("source_excerpt_json must be at most 2048 UTF-8 bytes")
    return value


class QuarantinedObservation(_TelemetryModel):
    reason: QuarantineReason
    tenant_id: SafeId
    device_id: DeviceId
    component_id: ComponentId | None
    metric_key: str | None
    observed_at: TelemetryDatetime | None
    received_at: TelemetryDatetime
    source_event_id: Annotated[str, Field(max_length=128)] | None
    detail: Annotated[str, Field(max_length=256)]
    source_excerpt_json: Annotated[str, AfterValidator(_require_excerpt_bytes)]
    truncated: StrictBool


class StoredObservation(_TelemetryModel):
    event_id: ObservationEventId
    observed_at: TelemetryDatetime
    received_at: TelemetryDatetime
    ingested_at: TelemetryDatetime
    metric_key: MetricKey
    value: FiniteFloat
    canonical_unit: UnitCode
    site_id: SiteId
    asset_id: AssetId
    device_id: DeviceId
    component_id: ComponentId
    probe_id: ProbeId | None
    deployment_id: DeploymentId
    quality_state: QualityState
    quality_flags: tuple[QualityFlag, ...]
    quality_provenance: tuple[QualityProvenance, ...]
    sequence: SequenceNumber | None
    source_type: SourceType


class NormalizationResult(_TelemetryModel):
    accepted: tuple[CanonicalObservation, ...]
    quarantined: tuple[QuarantinedObservation, ...]

    def counts(self) -> Mapping[str, int]:
        """Closed-key counters; no tenant or device labels (§23.2 emission is P3)."""
        counts: Counter[str] = Counter({"accepted": len(self.accepted)})
        counts.update({f"quarantined.{reason.value}": 0 for reason in QuarantineReason})
        counts.update({f"flag.{flag.value}": 0 for flag in QualityFlag})
        counts.update({f"state.{state.value}": 0 for state in QualityState})
        for item in self.quarantined:
            counts[f"quarantined.{item.reason.value}"] += 1
        for observation in self.accepted:
            counts[f"state.{observation.quality.state.value}"] += 1
            for flag in observation.quality.flags:
                counts[f"flag.{flag.value}"] += 1
        return dict(counts)
