"""Unit tests for the legacy MQTT reading mapper (P2-06): trusted tenancy, ts and field rules."""

import json
import math
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from limnopulse_api.domain.ids import default_component_id_for_legacy_device
from limnopulse_api.domain.legacy_telemetry_mapping import LegacyMappingResult, map_legacy_reading
from limnopulse_api.domain.metric_catalog import metric_catalog
from limnopulse_api.domain.telemetry_v2 import (
    MAX_QUARANTINE_EXCERPT_BYTES,
    MAX_SEQUENCE,
    QuarantinedObservation,
    QuarantineReason,
    SourceObservation,
    SourceType,
    TrustedSourceContext,
)

EXAMPLE_PATH = Path(__file__).resolve().parents[2] / "examples/telemetry/reading.local.json"
TENANT = "tenant-a"
DEVICE = "dev-legacy-1"
RECEIVED = datetime(2026, 7, 9, 12, 0, 5, tzinfo=UTC)
COMPONENT = default_component_id_for_legacy_device(TENANT, DEVICE)
EXPECTED = {
    "water.temperature": ("temp_c", "degC", 25.1),
    "water.ph": ("ph", "pH", 7.2),
    "water.dissolved_oxygen": ("do_mg_l", "mg/L", 6.4),
    "water.turbidity": ("turbidity_ntu", "NTU", 3.1),
    "water.salinity": ("salinity_ppt", "ppt", 11.8),
    "device.battery_voltage": ("battery_v", "V", 3.82),
    "device.rssi": ("rssi", "dBm", -67),
}
OWNERSHIP_CLAIMS = {
    "tenant_id": "tenant-evil",
    "pond_id": "pond-evil",
    "asset_id": "ast_00000000000000000000000000000000",
    "site_id": "site_00000000000000000000000000000000",
    "deployment_id": "dep_00000000000000000000000000000000",
    "device_id": "dev-evil",
    "component_id": "cmp_00000000000000000000000000000000",
    "unknown_metric": 1.0,
}


def context(**changes: Any) -> TrustedSourceContext:
    values: dict[str, Any] = {
        "tenant_id": TENANT,
        "device_id": DEVICE,
        "integration_id": None,
        "integration_type": "legacy_mqtt",
        "source_type": SourceType.LEGACY_MQTT,
        "connector_version": None,
        "received_at": RECEIVED,
    }
    values.update(changes)
    return TrustedSourceContext(**values)


def example() -> dict[str, Any]:
    return json.loads(EXAMPLE_PATH.read_text())


def by_metric(result: LegacyMappingResult) -> dict[str, SourceObservation]:
    observations = tuple(result.observations)
    keyed = {o.metric_key: o for o in observations}
    assert len(keyed) == len(observations), "one observation per metric"
    return keyed


def only_quarantine(result: LegacyMappingResult) -> QuarantinedObservation:
    quarantined = tuple(result.quarantined)
    assert len(quarantined) == 1, quarantined
    return quarantined[0]


def assert_reading_quarantined(result: LegacyMappingResult) -> QuarantinedObservation:
    assert tuple(result.observations) == ()
    record = only_quarantine(result)
    assert record.reason is QuarantineReason.OBSERVED_AT_INVALID
    assert record.tenant_id == TENANT
    assert record.device_id == DEVICE
    assert record.observed_at is None
    assert record.received_at == RECEIVED
    assert record.metric_key is None
    assert record.component_id in (None, COMPONENT)
    assert len(record.source_excerpt_json.encode("utf-8")) <= MAX_QUARANTINE_EXCERPT_BYTES
    return record


def mentions(record: QuarantinedObservation, *needles: str) -> bool:
    text = f"{record.metric_key or ''} {record.detail}"
    return any(needle in text for needle in needles)


# The example reading ---------------------------------------------------------------------------


def test_example_reading_yields_seven_catalog_observations_with_sequence_one() -> None:
    result = map_legacy_reading(context(), example())

    assert tuple(result.quarantined) == ()
    observations = by_metric(result)
    assert set(observations) == set(EXPECTED)
    assert set(observations) == {d.metric_key for d in metric_catalog()}
    for metric_key, (_, unit, value) in EXPECTED.items():
        o = observations[metric_key]
        assert o.component_id == COMPONENT
        assert o.source_unit == unit
        assert o.value == value
        assert type(o.value) is type(value)
        assert o.observed_at == datetime(2026, 7, 9, 12, 0, tzinfo=UTC)
        assert o.sequence == 1
        assert o.source_event_id is None
        assert dict(o.source_metadata) == {}


def test_component_comes_from_the_trusted_context_not_the_payload() -> None:
    other = map_legacy_reading(context(tenant_id="tenant-b", device_id="dev-legacy-2"), example())

    expected = default_component_id_for_legacy_device("tenant-b", "dev-legacy-2")
    assert expected != COMPONENT
    assert {o.component_id for o in other.observations} == {expected}


def test_offset_and_comma_fraction_timestamps_are_normalized_to_utc() -> None:
    payload = example() | {"ts": "2026-07-09T09:00:00,250000-03:00"}

    result = map_legacy_reading(context(), payload)

    assert tuple(result.quarantined) == ()
    instants = {o.observed_at for o in result.observations}
    assert instants == {datetime(2026, 7, 9, 12, 0, 0, 250000, tzinfo=UTC)}
    assert all(o.observed_at.utcoffset() == timedelta(0) for o in result.observations)


def test_nine_digit_fraction_with_trailing_zeros_is_accepted() -> None:
    payload = example() | {"ts": "2026-07-09T12:00:00.123456000Z"}

    result = map_legacy_reading(context(), payload)

    assert tuple(result.quarantined) == ()
    assert len(tuple(result.observations)) == 7
    assert {o.observed_at for o in result.observations} == {
        datetime(2026, 7, 9, 12, 0, 0, 123456, tzinfo=UTC)
    }


@pytest.mark.parametrize(
    "ts",
    ["1970-01-01T00:00:00Z", "2261-12-31T23:59:59.999999Z"],
    ids=["min-inclusive", "just-below-max"],
)
def test_telemetry_window_bounds_are_accepted(ts: str) -> None:
    result = map_legacy_reading(context(), example() | {"ts": ts})

    assert tuple(result.quarantined) == ()
    assert len(tuple(result.observations)) == 7


# Reading-level `ts` quarantine -----------------------------------------------------------------


def test_missing_ts_quarantines_the_whole_reading() -> None:
    payload = example()
    del payload["ts"]

    assert_reading_quarantined(map_legacy_reading(context(), payload))


@pytest.mark.parametrize(
    "ts",
    [
        "2026-07-09T12:00:00",  # naive
        "2026-07-09 12:00:00Z",  # space separator: not Telegraf RFC3339
        "2026-07-09t12:00:00z",  # lowercase
        "not-a-timestamp",
        "",
        "2026-02-30T12:00:00Z",
        1783598400,  # unix seconds are not accepted
        1783598400.0,
        True,
        None,
        datetime(2026, 7, 9, 12, 0),  # noqa: DTZ001 - naive datetime object
    ],
    ids=[
        "naive",
        "space",
        "lowercase",
        "garbage",
        "empty",
        "impossible-date",
        "unix-int",
        "unix-float",
        "bool",
        "null",
        "naive-datetime",
    ],
)
def test_naive_or_unparseable_ts_quarantines_the_whole_reading(ts: object) -> None:
    assert_reading_quarantined(map_legacy_reading(context(), example() | {"ts": ts}))


@pytest.mark.parametrize(
    "ts",
    ["1969-12-31T23:59:59.999999Z", "2262-01-01T00:00:00Z", "2262-01-01T01:00:00+01:00"],
    ids=["before-min", "max-exclusive", "max-exclusive-offset"],
)
def test_out_of_window_ts_quarantines_the_whole_reading(ts: str) -> None:
    assert_reading_quarantined(map_legacy_reading(context(), example() | {"ts": ts}))


def test_sub_microsecond_ts_quarantines_the_reading_and_is_never_truncated() -> None:
    payload = example() | {"ts": "2026-07-09T12:00:00.123456789Z"}

    record = assert_reading_quarantined(map_legacy_reading(context(), payload))

    assert "sub-microsecond precision unsupported" in record.detail


def test_reading_quarantine_ignores_other_defects() -> None:
    payload = example() | {"ts": "garbage", "seq": True, "ph": "7.2"}

    assert_reading_quarantined(map_legacy_reading(context(), payload))


def test_reading_quarantine_excerpt_is_bounded_and_truncated_with_a_flag() -> None:
    payload = example() | {"ts": "BAD" + "é" * 3000}

    record = assert_reading_quarantined(map_legacy_reading(context(), payload))

    assert record.truncated is True
    assert "BAD" in record.source_excerpt_json
    assert len(record.source_excerpt_json.encode("utf-8")) <= MAX_QUARANTINE_EXCERPT_BYTES


def test_small_reading_quarantine_excerpt_is_complete_json() -> None:
    payload = example() | {"ts": "garbage"}

    record = assert_reading_quarantined(map_legacy_reading(context(), payload))

    assert record.truncated is False
    assert "garbage" in record.source_excerpt_json
    json.loads(record.source_excerpt_json)


# `seq` -----------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "seq",
    [True, False, -1, MAX_SEQUENCE + 1, 2**64, "1", 1.0, [1]],
    ids=["true", "false", "negative", "above-max", "uint64", "string", "float", "list"],
)
def test_bad_seq_quarantines_only_seq_and_keeps_the_observations(seq: object) -> None:
    result = map_legacy_reading(context(), example() | {"seq": seq})

    record = only_quarantine(result)
    assert record.reason is QuarantineReason.VALUE_INVALID
    assert record.tenant_id == TENANT
    assert record.device_id == DEVICE
    assert record.received_at == RECEIVED
    assert mentions(record, "seq")
    observations = by_metric(result)
    assert set(observations) == set(EXPECTED)
    assert {o.sequence for o in observations.values()} == {None}


@pytest.mark.parametrize("seq", [0, MAX_SEQUENCE], ids=["zero", "max"])
def test_seq_bounds_are_accepted(seq: int) -> None:
    result = map_legacy_reading(context(), example() | {"seq": seq})

    assert tuple(result.quarantined) == ()
    assert {o.sequence for o in result.observations} == {seq}


def test_absent_seq_maps_to_no_sequence_without_quarantine() -> None:
    payload = example()
    del payload["seq"]

    result = map_legacy_reading(context(), payload)

    assert tuple(result.quarantined) == ()
    assert len(tuple(result.observations)) == 7
    assert {o.sequence for o in result.observations} == {None}


# Per-field values ------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [True, False, "7.2", math.nan, math.inf, -math.inf, [7.2], {"v": 7.2}],
    ids=["true", "false", "string", "nan", "inf", "-inf", "list", "object"],
)
def test_bad_field_value_quarantines_only_that_field(value: object) -> None:
    result = map_legacy_reading(context(), example() | {"ph": value})

    record = only_quarantine(result)
    assert record.reason is QuarantineReason.VALUE_INVALID
    assert record.metric_key in ("water.ph", "ph")
    assert record.tenant_id == TENANT
    assert record.device_id == DEVICE
    assert record.received_at == RECEIVED
    assert len(record.source_excerpt_json.encode("utf-8")) <= MAX_QUARANTINE_EXCERPT_BYTES
    observations = by_metric(result)
    assert set(observations) == set(EXPECTED) - {"water.ph"}
    assert {o.sequence for o in observations.values()} == {1}


def test_each_bad_field_gets_its_own_quarantine() -> None:
    payload = example() | {"ph": "x", "rssi": True, "temp_c": math.nan}

    result = map_legacy_reading(context(), payload)

    quarantined = tuple(result.quarantined)
    assert len(quarantined) == 3
    assert {q.reason for q in quarantined} == {QuarantineReason.VALUE_INVALID}
    keys = {q.metric_key for q in quarantined}
    assert keys in (
        {"water.ph", "device.rssi", "water.temperature"},
        {"ph", "rssi", "temp_c"},
    )
    remaining = set(EXPECTED) - {"water.ph", "device.rssi", "water.temperature"}
    assert set(by_metric(result)) == remaining


def test_null_fields_are_skipped_without_quarantine() -> None:
    payload = example() | {"ph": None, "rssi": None}

    result = map_legacy_reading(context(), payload)

    assert tuple(result.quarantined) == ()
    assert set(by_metric(result)) == set(EXPECTED) - {"water.ph", "device.rssi"}


def test_reading_with_only_ts_yields_nothing() -> None:
    result = map_legacy_reading(context(), {"ts": "2026-07-09T12:00:00Z"})

    assert tuple(result.observations) == ()
    assert tuple(result.quarantined) == ()


def test_integer_values_keep_their_numeric_type_for_the_normalizer() -> None:
    result = map_legacy_reading(context(), example() | {"ph": 7, "rssi": -67.0})

    observations = by_metric(result)
    assert observations["water.ph"].value == 7
    assert type(observations["water.ph"].value) is int
    assert observations["device.rssi"].value == -67.0
    assert type(observations["device.rssi"].value) is float


def test_out_of_range_values_are_emitted_for_the_normalizer_to_classify() -> None:
    result = map_legacy_reading(context(), example() | {"ph": 20.0, "temp_c": -40})

    assert tuple(result.quarantined) == ()
    observations = by_metric(result)
    assert observations["water.ph"].value == 20.0
    assert observations["water.temperature"].value == -40


# Tenancy: payload claims never influence anything -----------------------------------------------


def test_payload_ownership_claims_are_ignored() -> None:
    plain = map_legacy_reading(context(), example())
    claimed = map_legacy_reading(context(), example() | OWNERSHIP_CLAIMS)

    assert tuple(claimed.observations) == tuple(plain.observations)
    assert tuple(claimed.quarantined) == tuple(plain.quarantined) == ()
    assert {o.component_id for o in claimed.observations} == {COMPONENT}


def test_payload_ownership_claims_are_ignored_on_quarantine() -> None:
    payload = example() | {"ph": "bad"}
    plain = map_legacy_reading(context(), payload)
    claimed = map_legacy_reading(context(), payload | OWNERSHIP_CLAIMS)

    assert tuple(claimed.observations) == tuple(plain.observations)
    excluded = {"source_excerpt_json", "truncated"}
    assert [q.model_dump(exclude=excluded) for q in claimed.quarantined] == [
        q.model_dump(exclude=excluded) for q in plain.quarantined
    ]
    assert {q.tenant_id for q in claimed.quarantined} == {TENANT}
    assert {q.device_id for q in claimed.quarantined} == {DEVICE}


def test_mapping_is_deterministic_and_does_not_mutate_the_payload() -> None:
    payload = example()
    before = json.dumps(payload, sort_keys=True)

    first = map_legacy_reading(context(), payload)
    second = map_legacy_reading(context(), payload)

    assert tuple(first.observations) == tuple(second.observations)
    assert json.dumps(payload, sort_keys=True) == before


# Regression (post-approval review): the excerpt never raises and is strict JSON ----------------


def _strict_json(text: str) -> object:
    def reject(token: str) -> object:
        raise ValueError(f"non-standard JSON token {token}")

    return json.loads(text, parse_constant=reject)


@pytest.mark.parametrize(
    "payload",
    [
        {"ts": "\ud800"},
        {"ts": "bad", "note": "\ud800"},
        {"ts": 10**5000},
        {"ts": "bad", 1: "x", (1, 2): {"a": [1, {2: 3}]}},
    ],
    ids=["surrogate-ts", "surrogate-ignored-key", "huge-int-ts", "non-string-keys"],
)
def test_reading_quarantine_survives_hostile_payload_values(payload: dict[Any, Any]) -> None:
    record = assert_reading_quarantined(map_legacy_reading(context(), payload))

    assert record.truncated is False
    _strict_json(record.source_excerpt_json)


@pytest.mark.parametrize(
    ("changes", "metric_keys"),
    [
        ({"ph": "\ud800"}, ("water.ph", "ph")),
        ({"ph": math.nan}, ("water.ph", "ph")),
        ({"ph": math.inf}, ("water.ph", "ph")),
        ({"seq": 10**5000}, None),
    ],
    ids=["surrogate-value", "nan-value", "inf-value", "huge-int-seq"],
)
def test_field_quarantine_excerpt_is_strict_json_for_hostile_values(
    changes: dict[str, Any], metric_keys: tuple[str, ...] | None
) -> None:
    record = only_quarantine(map_legacy_reading(context(), example() | changes))

    assert record.reason is QuarantineReason.VALUE_INVALID
    if metric_keys is not None:
        assert record.metric_key in metric_keys
    _strict_json(record.source_excerpt_json)
    assert len(record.source_excerpt_json.encode("utf-8")) <= MAX_QUARANTINE_EXCERPT_BYTES
