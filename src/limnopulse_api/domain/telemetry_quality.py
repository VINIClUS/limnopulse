"""Deterministic quality classification (`quality-policy@1`, D3, D4, D15).

The state is `valid` or `out_of_range` only; clock skew, delay and replay are flags. Every
result is a pure function of its arguments and the pinned policy: no clock is ever read.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from types import MappingProxyType

from limnopulse_api.domain.telemetry_v2 import (
    QUALITY_POLICY_VERSION,
    MetricDefinition,
    ObservationQuality,
    QualityFlag,
    QualityProvenance,
    QualityState,
)


@dataclass(frozen=True)
class QualityPolicy:
    delay_threshold: timedelta
    future_skew_threshold: timedelta
    past_extreme_skew_threshold: timedelta


QUALITY_POLICIES: Mapping[str, QualityPolicy] = MappingProxyType(
    {
        "quality-policy@1": QualityPolicy(
            delay_threshold=timedelta(seconds=300),
            future_skew_threshold=timedelta(seconds=300),
            past_extreme_skew_threshold=timedelta(days=30),
        ),
    }
)

_MICROSECONDS_PER_DAY = 86_400 * 1_000_000


def _microseconds(delta: timedelta) -> int:
    return delta.days * _MICROSECONDS_PER_DAY + delta.seconds * 1_000_000 + delta.microseconds


def delay_ms(observed_at: datetime, received_at: datetime) -> int:
    """`(received_us - observed_us) // 1000`: exact integer floor, negative values included."""
    return _microseconds(received_at - observed_at) // 1000


def classify_quality(
    metric: MetricDefinition,
    canonical_value: float,
    *,
    observed_at: datetime,
    received_at: datetime,
    replay: bool,
    quality_policy_version: str = QUALITY_POLICY_VERSION,
    policies: Mapping[str, QualityPolicy] = QUALITY_POLICIES,
) -> ObservationQuality:
    try:
        policy = policies[quality_policy_version]
    except KeyError:
        raise ValueError(f"unknown quality policy version: {quality_policy_version}") from None

    state = QualityState.VALID
    flags: set[QualityFlag] = set()
    provenance = [QualityProvenance.SCHEMA_VALIDATION, QualityProvenance.UNIT_MAPPING]

    if not metric.plausible_min <= canonical_value <= metric.plausible_max:
        state = QualityState.OUT_OF_RANGE
        provenance.append(QualityProvenance.PLAUSIBLE_RANGE)

    delay = received_at - observed_at
    if -delay > policy.future_skew_threshold:
        flags.add(QualityFlag.CLOCK_SKEW_FUTURE)
    if delay > policy.past_extreme_skew_threshold:
        flags.add(QualityFlag.CLOCK_SKEW_PAST_EXTREME)
    if flags:
        provenance.append(QualityProvenance.CLOCK_SKEW)
    if delay > policy.delay_threshold:
        flags.add(QualityFlag.DELAYED)
        provenance.append(QualityProvenance.TIMELINESS)
    if replay:
        flags.add(QualityFlag.REPLAY)
        provenance.append(QualityProvenance.REPLAY)

    return ObservationQuality(
        state=state, flags=tuple(sorted(flags)), confidence=None, provenance=tuple(provenance)
    )
