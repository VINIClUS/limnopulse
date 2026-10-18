"""P2-02: deterministic quality classification (`quality-policy@1`, D3, D4, D15)."""

import ast
import inspect
import math
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta, timezone

import pytest
from limnopulse_api.domain.telemetry_quality import (
    QUALITY_POLICIES,
    QualityPolicy,
    classify_quality,
    delay_ms,
)

from limnopulse_api.domain import telemetry_quality
from limnopulse_api.domain.metric_catalog import metric_catalog, metric_definition
from limnopulse_api.domain.telemetry_v2 import (
    MAX_TELEMETRY_TIME,
    MIN_TELEMETRY_TIME,
    P2_WRITTEN_QUALITY_STATES,
    QUALITY_POLICY_VERSION,
    MetricDefinition,
    ObservationQuality,
    QualityFlag,
    QualityProvenance,
    QualityState,
)
from tests.support.telemetry_v2_vectors import load_vectors, scenario

OBSERVED = datetime(2026, 7, 9, 12, 0, 0, tzinfo=UTC)
MICRO = timedelta(microseconds=1)
FIVE_MINUTES = timedelta(seconds=300)
THIRTY_DAYS = timedelta(days=30)

BASE = (QualityProvenance.SCHEMA_VALIDATION, QualityProvenance.UNIT_MAPPING)
RANGE = QualityProvenance.PLAUSIBLE_RANGE
SKEW = QualityProvenance.CLOCK_SKEW
TIMELINESS = QualityProvenance.TIMELINESS
REPLAY_PROVENANCE = QualityProvenance.REPLAY

DELAYED = QualityFlag.DELAYED
FUTURE = QualityFlag.CLOCK_SKEW_FUTURE
PAST_EXTREME = QualityFlag.CLOCK_SKEW_PAST_EXTREME
REPLAY = QualityFlag.REPLAY

VALID = QualityState.VALID
OUT_OF_RANGE = QualityState.OUT_OF_RANGE


def metric(metric_key: str = "water.temperature") -> MetricDefinition:
    definition = metric_definition(metric_key)
    assert definition is not None
    return definition


def classify(
    value: float = 25.0,
    *,
    delay: timedelta = timedelta(seconds=2),
    replay: bool = False,
    definition: MetricDefinition | None = None,
    **kwargs: object,
) -> ObservationQuality:
    return classify_quality(
        definition or metric(),
        value,
        observed_at=OBSERVED,
        received_at=OBSERVED + delay,
        replay=replay,
        **kwargs,  # type: ignore[arg-type]
    )


def expected(
    state: QualityState = VALID,
    flags: tuple[QualityFlag, ...] = (),
    provenance: tuple[QualityProvenance, ...] = (),
) -> ObservationQuality:
    return ObservationQuality(
        state=state, flags=flags, confidence=None, provenance=(*BASE, *provenance)
    )


# --- registry -------------------------------------------------------------------------------


def test_registry_is_keyed_by_version_and_pins_the_locked_thresholds() -> None:
    assert isinstance(QUALITY_POLICIES, Mapping)
    assert QUALITY_POLICY_VERSION == "quality-policy@1"
    assert set(QUALITY_POLICIES) == {"quality-policy@1"}
    with pytest.raises(TypeError):
        QUALITY_POLICIES["quality-policy@2"] = QUALITY_POLICIES["quality-policy@1"]  # type: ignore[index]
    policy = QUALITY_POLICIES["quality-policy@1"]
    assert isinstance(policy, QualityPolicy)
    assert policy == QualityPolicy(
        delay_threshold=timedelta(seconds=300),
        future_skew_threshold=timedelta(seconds=300),
        past_extreme_skew_threshold=timedelta(days=30),
    )


def test_default_version_parameter_is_the_frozen_quality_policy_version() -> None:
    parameters = inspect.signature(classify_quality).parameters
    for name in ("observed_at", "received_at", "replay", "quality_policy_version", "policies"):
        assert parameters[name].kind is inspect.Parameter.KEYWORD_ONLY, name
    assert parameters["quality_policy_version"].default == QUALITY_POLICY_VERSION
    assert parameters["policies"].default is QUALITY_POLICIES


# --- plausible range (state) -----------------------------------------------------------------


@pytest.mark.parametrize("definition", metric_catalog(), ids=lambda item: item.metric_key)
def test_inclusive_range_boundaries_are_valid(definition: MetricDefinition) -> None:
    for value in (definition.plausible_min, definition.plausible_max):
        assert classify(float(value), definition=definition) == expected()


@pytest.mark.parametrize("definition", metric_catalog(), ids=lambda item: item.metric_key)
def test_values_just_outside_the_range_are_out_of_range(definition: MetricDefinition) -> None:
    below = math.nextafter(definition.plausible_min, -math.inf)
    above = math.nextafter(definition.plausible_max, math.inf)
    for value in (below, above, definition.plausible_min - 1, definition.plausible_max + 1):
        assert classify(value, definition=definition) == expected(OUT_OF_RANGE, (), (RANGE,))


@pytest.mark.parametrize("definition", metric_catalog(), ids=lambda item: item.metric_key)
def test_values_inside_the_range_are_valid(definition: MetricDefinition) -> None:
    low, high = definition.plausible_min, definition.plausible_max
    interior = [low + (high - low) * step / 64 for step in range(1, 64)]
    for value in (math.nextafter(low, math.inf), *interior, math.nextafter(high, -math.inf)):
        assert classify(value, definition=definition) == expected(), value


@pytest.mark.parametrize("value", [-1e300, 1e300, -6.0, 61.0, 4001.0])
def test_far_out_of_range_values_are_never_valid(value: float) -> None:
    quality = classify(value)
    assert quality.state is OUT_OF_RANGE
    assert RANGE in quality.provenance


def test_range_comes_from_the_metric_definition() -> None:
    narrow = metric().model_copy(update={"plausible_min": 10.0, "plausible_max": 20.0})
    assert classify(10.0, definition=narrow) == expected()
    assert classify(20.0, definition=narrow) == expected()
    assert classify(9.5, definition=narrow).state is OUT_OF_RANGE
    assert classify(25.0, definition=narrow).state is OUT_OF_RANGE
    assert classify(25.0).state is VALID


def test_rssi_range_boundaries() -> None:
    rssi = metric("device.rssi")
    assert classify(-150.0, definition=rssi).state is VALID
    assert classify(0.0, definition=rssi).state is VALID
    assert classify(-151.0, definition=rssi).state is OUT_OF_RANGE
    assert classify(1.0, definition=rssi).state is OUT_OF_RANGE


# --- delay / timeliness ---------------------------------------------------------------------


def test_delay_of_exactly_300_seconds_is_not_delayed() -> None:
    assert classify(delay=FIVE_MINUTES) == expected()


def test_delay_of_300_seconds_plus_one_microsecond_is_delayed() -> None:
    assert classify(delay=FIVE_MINUTES + MICRO) == expected(VALID, (DELAYED,), (TIMELINESS,))


def test_delay_of_exactly_30_days_is_delayed_but_not_past_extreme() -> None:
    assert classify(delay=THIRTY_DAYS) == expected(VALID, (DELAYED,), (TIMELINESS,))


def test_delay_of_30_days_plus_one_microsecond_is_past_extreme_and_delayed() -> None:
    assert classify(delay=THIRTY_DAYS + MICRO) == expected(
        VALID, (PAST_EXTREME, DELAYED), (SKEW, TIMELINESS)
    )


def test_zero_and_small_delays_carry_no_flag() -> None:
    for delay in (timedelta(0), MICRO, timedelta(seconds=299, microseconds=999_999)):
        assert classify(delay=delay) == expected()


# --- future clock skew ----------------------------------------------------------------------


def test_future_skew_of_exactly_300_seconds_is_not_flagged() -> None:
    assert classify(delay=-FIVE_MINUTES) == expected()


def test_future_skew_of_300_seconds_plus_one_microsecond_is_flagged() -> None:
    assert classify(delay=-(FIVE_MINUTES + MICRO)) == expected(VALID, (FUTURE,), (SKEW,))


def test_small_future_skew_is_not_flagged_or_delayed() -> None:
    for delay in (-MICRO, -timedelta(seconds=299, microseconds=999_999)):
        assert classify(delay=delay) == expected()


def test_extreme_future_skew_is_only_future_skew() -> None:
    assert classify(delay=-(THIRTY_DAYS * 10)) == expected(VALID, (FUTURE,), (SKEW,))


# --- replay ---------------------------------------------------------------------------------


def test_replay_marker_adds_replay_flag_and_provenance() -> None:
    assert classify(replay=True) == expected(VALID, (REPLAY,), (REPLAY_PROVENANCE,))
    assert classify(replay=False) == expected()


# --- combinations: sorted unique flags, exact provenance order ------------------------------


@pytest.mark.parametrize(
    ("value", "delay", "replay", "state", "flags", "provenance"),
    [
        (
            25.0,
            FIVE_MINUTES + MICRO,
            True,
            VALID,
            (DELAYED, REPLAY),
            (TIMELINESS, REPLAY_PROVENANCE),
        ),
        (
            25.0,
            THIRTY_DAYS + MICRO,
            True,
            VALID,
            (PAST_EXTREME, DELAYED, REPLAY),
            (SKEW, TIMELINESS, REPLAY_PROVENANCE),
        ),
        (
            99.0,
            THIRTY_DAYS + MICRO,
            True,
            OUT_OF_RANGE,
            (PAST_EXTREME, DELAYED, REPLAY),
            (RANGE, SKEW, TIMELINESS, REPLAY_PROVENANCE),
        ),
        (
            -99.0,
            -(FIVE_MINUTES + MICRO),
            True,
            OUT_OF_RANGE,
            (FUTURE, REPLAY),
            (RANGE, SKEW, REPLAY_PROVENANCE),
        ),
        (99.0, FIVE_MINUTES + MICRO, False, OUT_OF_RANGE, (DELAYED,), (RANGE, TIMELINESS)),
        (99.0, timedelta(0), True, OUT_OF_RANGE, (REPLAY,), (RANGE, REPLAY_PROVENANCE)),
        (25.0, -(FIVE_MINUTES + MICRO), False, VALID, (FUTURE,), (SKEW,)),
    ],
)
def test_combined_conditions_produce_sorted_flags_and_ordered_provenance(
    value: float,
    delay: timedelta,
    replay: bool,
    state: QualityState,
    flags: tuple[QualityFlag, ...],
    provenance: tuple[QualityProvenance, ...],
) -> None:
    quality = classify(value, delay=delay, replay=replay)
    assert quality == expected(state, flags, provenance)
    assert list(quality.flags) == sorted(set(quality.flags))
    assert quality.provenance[:2] == BASE
    assert len(set(quality.provenance)) == len(quality.provenance)


@pytest.mark.parametrize(
    "delay",
    [
        timedelta(0),
        FIVE_MINUTES + MICRO,
        THIRTY_DAYS + MICRO,
        -(FIVE_MINUTES + MICRO),
        timedelta(days=3650),
    ],
)
@pytest.mark.parametrize("replay", [False, True])
def test_skew_delay_and_replay_never_change_the_state(delay: timedelta, replay: bool) -> None:
    assert classify(25.0, delay=delay, replay=replay).state is VALID
    assert classify(-60.0, delay=delay, replay=replay).state is OUT_OF_RANGE
    assert classify(60.0, delay=delay, replay=replay).state is VALID


def test_only_p2_written_states_are_produced_and_confidence_is_always_none() -> None:
    delays = (
        timedelta(0),
        FIVE_MINUTES,
        FIVE_MINUTES + MICRO,
        THIRTY_DAYS,
        THIRTY_DAYS + MICRO,
        -FIVE_MINUTES,
        -(FIVE_MINUTES + MICRO),
    )
    for definition in metric_catalog():
        values = (
            definition.plausible_min - 1,
            definition.plausible_min,
            (definition.plausible_min + definition.plausible_max) / 2,
            definition.plausible_max,
            definition.plausible_max + 1,
        )
        for value in values:
            for delay in delays:
                for replay in (False, True):
                    quality = classify(value, delay=delay, replay=replay, definition=definition)
                    assert isinstance(quality, ObservationQuality)
                    assert quality.state in P2_WRITTEN_QUALITY_STATES
                    assert quality.confidence is None
                    assert quality.provenance[:2] == BASE
                    assert (quality.state is OUT_OF_RANGE) == (RANGE in quality.provenance)


def test_timestamps_with_non_utc_offsets_are_compared_as_instants() -> None:
    plus_three = timezone(timedelta(hours=3))
    observed = datetime(2026, 7, 9, 15, 0, 0, tzinfo=plus_three)  # 12:00Z
    received = datetime(2026, 7, 9, 12, 5, 0, 1, tzinfo=UTC)  # 300 s + 1 µs later
    quality = classify_quality(
        metric(), 25.0, observed_at=observed, received_at=received, replay=False
    )
    assert quality == expected(VALID, (DELAYED,), (TIMELINESS,))


def test_classification_is_deterministic() -> None:
    first = classify(99.0, delay=THIRTY_DAYS + MICRO, replay=True)
    for _ in range(3):
        assert classify(99.0, delay=THIRTY_DAYS + MICRO, replay=True) == first


# --- versioning and injection ----------------------------------------------------------------


def _with_test_only_v2() -> dict[str, QualityPolicy]:
    v2 = QualityPolicy(
        delay_threshold=timedelta(seconds=60),
        future_skew_threshold=timedelta(seconds=10),
        past_extreme_skew_threshold=timedelta(days=1),
    )
    return {**QUALITY_POLICIES, "quality-policy@2": v2}


def test_injected_test_only_v2_is_honored_only_when_selected() -> None:
    policies = _with_test_only_v2()
    two_minutes = timedelta(seconds=120)

    assert classify(delay=two_minutes) == expected()
    assert classify(delay=two_minutes, policies=policies) == expected()
    assert (
        classify(delay=two_minutes, quality_policy_version="quality-policy@1", policies=policies)
        == expected()
    )
    assert classify(
        delay=two_minutes, quality_policy_version="quality-policy@2", policies=policies
    ) == expected(VALID, (DELAYED,), (TIMELINESS,))
    assert classify(
        delay=-timedelta(seconds=11), quality_policy_version="quality-policy@2", policies=policies
    ) == expected(VALID, (FUTURE,), (SKEW,))
    assert classify(
        delay=timedelta(days=2), quality_policy_version="quality-policy@2", policies=policies
    ) == expected(VALID, (PAST_EXTREME, DELAYED), (SKEW, TIMELINESS))
    assert classify(delay=timedelta(days=2), policies=policies) == expected(
        VALID, (DELAYED,), (TIMELINESS,)
    )
    assert "quality-policy@2" not in QUALITY_POLICIES


@pytest.mark.parametrize(
    "version", ["quality-policy@2", "quality-policy@0", "", "QUALITY-POLICY@1"]
)
def test_unknown_version_raises_value_error(version: str) -> None:
    with pytest.raises(ValueError):
        classify(quality_policy_version=version)


def test_unknown_version_is_checked_against_the_injected_registry() -> None:
    with pytest.raises(ValueError):
        classify(quality_policy_version="quality-policy@1", policies={})


# --- delay_ms -------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("delta", "expected_ms"),
    [
        (timedelta(0), 0),
        (MICRO, 0),
        (timedelta(microseconds=999), 0),
        (timedelta(microseconds=1000), 1),
        (timedelta(microseconds=1999), 1),
        (timedelta(seconds=2), 2000),
        (FIVE_MINUTES + MICRO, 300_000),
        (THIRTY_DAYS, 2_592_000_000),
        (-MICRO, -1),
        (-timedelta(microseconds=999), -1),
        (-timedelta(microseconds=1000), -1),
        (-timedelta(microseconds=1001), -2),
        (-timedelta(microseconds=1500), -2),
        (-FIVE_MINUTES, -300_000),
        (-(FIVE_MINUTES + MICRO), -300_001),
    ],
)
def test_delay_ms_is_floor_of_microsecond_difference(delta: timedelta, expected_ms: int) -> None:
    result = delay_ms(OBSERVED, OBSERVED + delta)
    assert type(result) is int
    assert result == expected_ms


@pytest.mark.parametrize(
    ("observed", "delta", "expected_ms"),
    [
        # Large deltas where float `total_seconds() * 1000` rounds to the wrong millisecond;
        # every instant stays inside [MIN_TELEMETRY_TIME, MAX_TELEMETRY_TIME).
        (MIN_TELEMETRY_TIME, timedelta(microseconds=8_626_555_501_451_999), 8_626_555_501_451),
        (MIN_TELEMETRY_TIME, timedelta(microseconds=8_701_726_501_526_000), 8_701_726_501_526),
        (
            datetime(2261, 12, 31, tzinfo=UTC),
            -timedelta(microseconds=8_701_726_501_526_000),
            -8_701_726_501_526,
        ),
    ],
)
def test_delay_ms_is_exact_for_large_deltas(
    observed: datetime, delta: timedelta, expected_ms: int
) -> None:
    received = observed + delta
    assert MIN_TELEMETRY_TIME <= min(observed, received)
    assert max(observed, received) < MAX_TELEMETRY_TIME
    assert delay_ms(observed, received) == expected_ms


def test_delay_ms_is_exact_over_the_whole_telemetry_time_range() -> None:
    observed = datetime(1970, 1, 1, tzinfo=UTC)
    received = datetime(2261, 12, 31, 23, 59, 59, 999_999, tzinfo=UTC)
    delta = received - observed
    micros = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    assert delay_ms(observed, received) == micros // 1000
    assert delay_ms(received, observed) == (-micros) // 1000


def test_delay_ms_compares_instants_across_offsets() -> None:
    observed = datetime(2026, 7, 9, 9, 0, 0, tzinfo=timezone(timedelta(hours=-3)))  # 12:00Z
    received = datetime(2026, 7, 9, 12, 0, 1, 500_000, tzinfo=UTC)
    assert delay_ms(observed, received) == 1500


# --- contract vectors ------------------------------------------------------------------------


def _vector_observations():
    for name in load_vectors()["scenarios"]:
        yield from scenario(name).observations


def test_classification_reproduces_every_contract_vector_quality() -> None:
    observations = list(_vector_observations())
    assert len(observations) >= 10
    seen_states: set[QualityState] = set()
    seen_flags: set[QualityFlag] = set()
    for item in observations:
        definition = metric_definition(
            item.observation.metric_key, version=item.mapping.catalog_version
        )
        assert definition is not None
        quality = classify_quality(
            definition,
            item.observation.value,
            observed_at=item.observation.observed_at,
            received_at=item.source.received_at,
            replay=item.delivery.replay,
            quality_policy_version=item.mapping.quality_policy_version,
        )
        assert quality == item.quality, item.event_id
        assert (DELAYED in quality.flags) is item.delivery.delayed
        assert (REPLAY in quality.flags) is item.delivery.replay
        seen_states.add(quality.state)
        seen_flags.update(quality.flags)
    assert seen_states == {VALID, OUT_OF_RANGE}
    assert {DELAYED, REPLAY} <= seen_flags


# --- determinism (D4) -----------------------------------------------------------------------

FORBIDDEN_CLOCK_NAMES = frozenset({"now", "utcnow", "today", "monotonic"})
ALLOWED_STDLIB_IMPORTS = frozenset(
    {
        "__future__",
        "collections",
        "dataclasses",
        "datetime",
        "enum",
        "functools",
        "math",
        "numbers",
        "types",
        "typing",
        "pydantic",
    }
)


def _import_allowed(module: str) -> bool:
    return module.split(".")[0] in ALLOWED_STDLIB_IMPORTS or module.startswith(
        "limnopulse_api.domain."
    )


def test_quality_module_never_reads_the_clock_or_does_io() -> None:
    """D4: no clock reads and no I/O.

    Every import comes from a small pure allowlist or `limnopulse_api.domain`, so a clock
    helper cannot be borrowed from an adapter or service. Deliberately strict on top: any
    identifier named like a clock accessor and any `time.<attr>` reference fail.
    """
    tree = ast.parse(inspect.getsource(telemetry_quality))
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            assert node.id not in FORBIDDEN_CLOCK_NAMES, node.id
            assert node.id not in {"open", "print", "input"}, node.id
        elif isinstance(node, ast.Attribute):
            assert node.attr not in FORBIDDEN_CLOCK_NAMES, node.attr
            if isinstance(node.value, ast.Name) and node.value.id == "time":
                pytest.fail(f"time.{node.attr} referenced")
        elif isinstance(node, ast.Import):
            for alias in node.names:
                assert _import_allowed(alias.name), alias.name
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                assert node.level == 1, "relative import escapes limnopulse_api.domain"
            else:
                assert _import_allowed(node.module or ""), node.module
            for alias in node.names:
                assert alias.name not in FORBIDDEN_CLOCK_NAMES | {"time"}, alias.name


def test_far_future_observation_is_classified_without_reading_the_clock() -> None:
    # A clamp-to-now (e.g. `min(received_at, <clock>)`) would invent delay or skew flags here.
    observed = datetime(2200, 1, 1, tzinfo=UTC)
    quality = classify_quality(
        metric(),
        25.0,
        observed_at=observed,
        received_at=observed + timedelta(seconds=2),
        replay=False,
    )
    assert quality == expected()
    assert delay_ms(observed, observed + timedelta(seconds=2)) == 2000
    early = datetime(1970, 1, 1, tzinfo=UTC)
    assert (
        classify_quality(
            metric(), 25.0, observed_at=early, received_at=early + FIVE_MINUTES, replay=False
        )
        == expected()
    )
