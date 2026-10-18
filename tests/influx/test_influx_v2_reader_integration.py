"""P2-04 reader against a real InfluxDB 2.7.11: every Flux string runs, semantics hold."""

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta

import pytest

from limnopulse_api.adapters.influxdb_v2_reader import InfluxObservationReader
from limnopulse_api.core.errors import TelemetryQueryError
from limnopulse_api.domain.metric_catalog import metric_definition
from limnopulse_api.domain.telemetry_v2 import (
    CanonicalObservation,
    QualityFlag,
    QualityProvenance,
    QualityState,
    StoredObservation,
    observation_event_id,
)
from limnopulse_api.domain.telemetry_v2_schema import encode_observation_line
from limnopulse_api.repositories.telemetry_v2 import ObservationSelector
from tests.support.influx_harness import influx_clients, influx_settings, write_lines
from tests.support.telemetry_v2_vectors import scenario

TENANT = "tnt_local_001"
POND_ASSET = "ast_751707aef3455ab2ba60733d238b594f"
NATIVE_DEVICE = "dev_22222222222222222222222222222222"
PROBE_LESS_COMPONENT = "cmp_a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3"
NOON = datetime(2026, 7, 9, 12, 0, tzinfo=UTC)
WINDOW = (datetime(2026, 7, 9, 11, 0, tzinfo=UTC), datetime(2026, 7, 9, 14, 0, tzinfo=UTC))


def stored_for(o: CanonicalObservation) -> StoredObservation:
    resolved = o.resolved
    return StoredObservation(
        event_id=o.event_id,
        observed_at=o.observation.observed_at,
        received_at=o.source.received_at,
        ingested_at=o.delivery.ingested_at,
        metric_key=o.observation.metric_key,
        value=o.observation.value,
        canonical_unit=o.observation.canonical_unit,
        site_id=resolved.site_id,
        asset_id=resolved.asset_id,
        device_id=resolved.device_id,
        component_id=resolved.component_id,
        probe_id=resolved.probe_id,
        deployment_id=resolved.deployment_id,
        quality_state=o.quality.state,
        quality_flags=o.quality.flags,
        quality_provenance=o.quality.provenance,
        sequence=o.observation.sequence,
        source_type=o.source.source_type,
    )


def variant(
    o: CanonicalObservation,
    *,
    observed_at: datetime | None = None,
    received_at: datetime | None = None,
    ingested_at: datetime | None = None,
    metric_key: str | None = None,
    component_id: str | None = None,
    probe_id: object = ...,
    replay: bool = False,
) -> CanonicalObservation:
    """A valid copy with new times/identity; event_id recomputed, quality mirrors kept."""
    data = o.model_dump()
    if observed_at is not None:
        data["observation"]["observed_at"] = observed_at
    if received_at is not None:
        data["source"]["received_at"] = received_at
    if ingested_at is not None:
        data["delivery"]["ingested_at"] = ingested_at
    if metric_key is not None:
        definition = metric_definition(metric_key)
        assert definition is not None
        data["observation"]["metric_key"] = metric_key
        data["observation"]["canonical_unit"] = definition.canonical_unit
        data["observation"]["source_unit"] = definition.canonical_unit
        data["observation"]["value_type"] = definition.value_type
    if component_id is not None:
        data["resolved"]["component_id"] = component_id
    if probe_id is not ...:
        data["resolved"]["probe_id"] = probe_id
    if replay:
        data["quality"]["flags"] = (QualityFlag.DELAYED, QualityFlag.REPLAY)
        data["quality"]["provenance"] = (
            QualityProvenance.SCHEMA_VALIDATION,
            QualityProvenance.UNIT_MAPPING,
            QualityProvenance.TIMELINESS,
            QualityProvenance.REPLAY,
        )
        data["delivery"]["delayed"] = True
        data["delivery"]["replay"] = True
    data["event_id"] = observation_event_id(
        data["resolved"]["tenant_id"],
        data["resolved"]["component_id"],
        data["observation"]["metric_key"],
        data["observation"]["observed_at"],
    )
    return CanonicalObservation.model_validate(data)


def seed(bucket: str, observations: Iterable[CanonicalObservation]) -> None:
    write_lines(bucket, [encode_observation_line(o) for o in observations])


def ordered(observations: Iterable[CanonicalObservation]) -> list[StoredObservation]:
    items = [stored_for(o) for o in observations]
    return sorted(items, key=lambda s: (s.observed_at, s.component_id, s.metric_key))


async def query(
    bucket: str,
    selector: ObservationSelector,
    *,
    start: datetime = WINDOW[0],
    stop: datetime = WINDOW[1],
    metric_keys: frozenset[str] | None = None,
    quality_states: frozenset[QualityState] | None = None,
    limit: int = 1000,
) -> list[StoredObservation]:
    with influx_clients(bucket) as (query_api, _write_api):
        reader = InfluxObservationReader(
            query_api=query_api, org=influx_settings().org, bucket=bucket
        )
        return await reader.query_observations(
            tenant_id=TENANT,
            selector=selector,
            metric_keys=metric_keys,
            quality_states=quality_states,
            start=start,
            stop=stop,
            limit=limit,
        )


async def latest(
    bucket: str,
    selector: ObservationSelector,
    now: datetime,
    *,
    metric_keys: frozenset[str] | None = None,
) -> list[StoredObservation]:
    with influx_clients(bucket) as (query_api, _write_api):
        reader = InfluxObservationReader(
            query_api=query_api, org=influx_settings().org, bucket=bucket
        )
        return await reader.query_latest_observations(
            tenant_id=TENANT,
            selector=selector,
            metric_keys=metric_keys,
            lookback=timedelta(hours=24),
            now=now,
        )


def noon_observations() -> list[CanonicalObservation]:
    """legacy_sample (7 metrics) + two native probes + one probe-less native component at noon."""
    legacy = list(scenario("legacy_sample").observations)
    probes = list(scenario("two_probes_same_metric").observations)
    probe_less = variant(probes[0], component_id=PROBE_LESS_COMPONENT, probe_id=None)
    return legacy + probes + [probe_less]


# --- absolute windows ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_two_probes_on_one_device_are_distinguishable_series(influx_bucket: str) -> None:
    probes = scenario("two_probes_same_metric").observations
    seed(influx_bucket, probes)

    result = await query(influx_bucket, ObservationSelector("device", NATIVE_DEVICE))

    assert result, "seeded data must be readable"
    assert result == ordered(probes)
    assert len({(item.component_id, item.probe_id) for item in result}) == 2
    assert {item.value for item in result} == {6.4, 6.6}
    assert all(item.sequence is None for item in result)  # ABSENT_SEQUENCE decoded


@pytest.mark.asyncio
async def test_asset_window_returns_every_point_in_global_order(influx_bucket: str) -> None:
    observations = noon_observations()
    seed(influx_bucket, observations)

    result = await query(influx_bucket, ObservationSelector("asset", POND_ASSET))

    assert len(result) == 10
    assert result == ordered(observations)
    probe_less = [item for item in result if item.component_id == PROBE_LESS_COMPONENT]
    assert len(probe_less) == 1 and probe_less[0].probe_id is None


@pytest.mark.asyncio
async def test_limit_applies_after_the_global_sort(influx_bucket: str) -> None:
    probes = scenario("two_probes_same_metric").observations
    legacy = scenario("legacy_sample").observations[0]
    # Distinct times whose component order is the reverse of their time order.
    earlier = variant(probes[1], observed_at=NOON - timedelta(minutes=30), metric_key="water.ph")
    later = variant(legacy, observed_at=NOON + timedelta(minutes=30), metric_key="water.ph")
    observations = [*noon_observations(), earlier, later]
    seed(influx_bucket, observations)
    expected = ordered(observations)
    assert expected[0] == stored_for(earlier) and expected[-1] == stored_for(later)

    for limit in (1, 3, 8, 11):
        result = await query(influx_bucket, ObservationSelector("asset", POND_ASSET), limit=limit)
        assert result, "seeded data must be readable"
        assert result == expected[:limit]


@pytest.mark.asyncio
async def test_component_selector_and_metric_filter(influx_bucket: str) -> None:
    observations = noon_observations()
    seed(influx_bucket, observations)
    legacy_component = scenario("legacy_sample").observations[0].resolved.component_id

    by_component = await query(influx_bucket, ObservationSelector("component", legacy_component))
    assert by_component == ordered(scenario("legacy_sample").observations)

    keys = frozenset({"water.ph", "device.rssi"})
    filtered = await query(
        influx_bucket, ObservationSelector("asset", POND_ASSET), metric_keys=keys
    )
    assert filtered, "seeded data must be readable"
    assert filtered == [item for item in ordered(observations) if item.metric_key in keys]
    rssi = next(item for item in filtered if item.metric_key == "device.rssi")
    assert rssi.value == -67.0 and rssi.sequence == 1


@pytest.mark.asyncio
async def test_quality_state_filter(influx_bucket: str) -> None:
    out_of_range = scenario("out_of_range").observations
    observations = noon_observations() + list(out_of_range)
    seed(influx_bucket, observations)
    selector = ObservationSelector("asset", POND_ASSET)

    only_bad = await query(
        influx_bucket, selector, quality_states=frozenset({QualityState.OUT_OF_RANGE})
    )
    assert only_bad == ordered(out_of_range)
    assert only_bad[0].quality_state is QualityState.OUT_OF_RANGE

    only_valid = await query(
        influx_bucket, selector, quality_states=frozenset({QualityState.VALID})
    )
    assert only_valid == ordered(noon_observations())

    both = await query(
        influx_bucket,
        selector,
        quality_states=frozenset({QualityState.VALID, QualityState.OUT_OF_RANGE}),
        metric_keys=frozenset({"water.dissolved_oxygen"}),
    )
    assert both, "seeded data must be readable"
    assert both == [
        item for item in ordered(observations) if item.metric_key == "water.dissolved_oxygen"
    ]


@pytest.mark.asyncio
async def test_window_is_half_open_by_observed_at(influx_bucket: str) -> None:
    relocation = scenario("relocation").observations
    seed(influx_bucket, relocation)
    before, after = sorted(relocation, key=lambda o: o.observation.observed_at)
    t0, t1 = before.observation.observed_at, after.observation.observed_at
    device = ObservationSelector("device", before.resolved.device_id)
    one_us = timedelta(microseconds=1)

    assert await query(influx_bucket, device, start=t0, stop=t1) == [stored_for(before)]
    assert await query(influx_bucket, device, start=t0, stop=t1 + one_us) == ordered(relocation)
    assert await query(influx_bucket, device, start=t0 + one_us, stop=t1 + one_us) == [
        stored_for(after)
    ]
    assert await query(influx_bucket, device, start=t0 - timedelta(hours=1), stop=t0) == []


@pytest.mark.asyncio
async def test_relocation_follows_the_asset_at_event_time(influx_bucket: str) -> None:
    relocation = scenario("relocation").observations
    seed(influx_bucket, relocation)
    before, after = sorted(relocation, key=lambda o: o.observation.observed_at)
    start, stop = datetime(2026, 7, 9, 9, 0, tzinfo=UTC), datetime(2026, 7, 9, 15, 0, tzinfo=UTC)

    west = await query(
        influx_bucket,
        ObservationSelector("asset", before.resolved.asset_id),
        start=start,
        stop=stop,
    )
    east = await query(
        influx_bucket, ObservationSelector("asset", after.resolved.asset_id), start=start, stop=stop
    )

    assert west == [stored_for(before)]
    assert east == [stored_for(after)]
    assert west[0].deployment_id != east[0].deployment_id


@pytest.mark.asyncio
async def test_other_tenants_points_are_never_returned(influx_bucket: str) -> None:
    probes = scenario("two_probes_same_metric").observations
    foreign = []
    for o in probes:
        data = o.model_dump()
        data["resolved"]["tenant_id"] = "tnt_foreign_002"
        data["event_id"] = observation_event_id(
            "tnt_foreign_002",
            data["resolved"]["component_id"],
            data["observation"]["metric_key"],
            data["observation"]["observed_at"],
        )
        foreign.append(CanonicalObservation.model_validate(data))
    seed(influx_bucket, list(probes) + foreign)

    result = await query(influx_bucket, ObservationSelector("device", NATIVE_DEVICE))

    assert result == ordered(probes)


@pytest.mark.asyncio
async def test_query_against_a_missing_bucket_raises_telemetry_query_error(
    influx_bucket: str,
) -> None:
    with pytest.raises(TelemetryQueryError):
        await query(f"{influx_bucket}_missing", ObservationSelector("asset", POND_ASSET))
    with pytest.raises(TelemetryQueryError):
        await latest(
            f"{influx_bucket}_missing", ObservationSelector("asset", POND_ASSET), datetime.now(UTC)
        )


# --- latest (relative window, seeded against a captured now) -------------------------------


def expected_latest(observations: Iterable[CanonicalObservation]) -> list[StoredObservation]:
    winners: dict[tuple[str, str], CanonicalObservation] = {}
    for o in observations:
        key = (o.resolved.component_id, o.observation.metric_key)
        if key not in winners or o.observation.observed_at > winners[key].observation.observed_at:
            winners[key] = o
    return [stored_for(winners[key]) for key in sorted(winners)]


@pytest.mark.asyncio
async def test_latest_is_one_item_per_component_and_metric(influx_bucket: str) -> None:
    now = datetime.now(UTC)
    series = scenario("multi_point_series").rebased(now)
    seed(influx_bucket, series.observations)
    assets = sorted({o.resolved.asset_id for o in series.observations})
    devices = sorted({o.resolved.device_id for o in series.observations})

    for asset_id in assets:
        result = await latest(influx_bucket, ObservationSelector("asset", asset_id), now)
        members = [o for o in series.observations if o.resolved.asset_id == asset_id]
        assert result, "seeded data must be readable"
        assert result == expected_latest(members)
        assert len(result) == len({(i.component_id, i.metric_key) for i in result}) >= 2

    for device_id in devices:
        result = await latest(influx_bucket, ObservationSelector("device", device_id), now)
        members = [o for o in series.observations if o.resolved.device_id == device_id]
        assert result == expected_latest(members)

    component_id = series.observations[0].resolved.component_id
    one_metric = await latest(
        influx_bucket,
        ObservationSelector("component", component_id),
        now,
        metric_keys=frozenset({"water.temperature"}),
    )
    assert one_metric == expected_latest(
        o
        for o in series.observations
        if o.resolved.component_id == component_id
        and o.observation.metric_key == "water.temperature"
    )
    assert len(one_metric) == 1


@pytest.mark.asyncio
async def test_latest_across_components_is_ordered_by_component_then_metric(
    influx_bucket: str,
) -> None:
    now = datetime.now(UTC)
    base = scenario("two_probes_same_metric").observations
    observations = []
    for index, o in enumerate(base):
        for metric_key in ("water.temperature", "water.dissolved_oxygen"):
            observations.append(
                variant(
                    o,
                    observed_at=now - timedelta(minutes=10 + index),
                    received_at=now - timedelta(minutes=9),
                    ingested_at=now - timedelta(minutes=9),
                    metric_key=metric_key,
                )
            )
    seed(influx_bucket, observations)

    result = await latest(influx_bucket, ObservationSelector("device", NATIVE_DEVICE), now)

    assert len(result) == 4
    assert [(i.component_id, i.metric_key) for i in result] == sorted(
        (i.component_id, i.metric_key) for i in result
    )
    assert result == expected_latest(observations)


@pytest.mark.asyncio
async def test_a_replay_with_newer_receipt_never_becomes_latest(influx_bucket: str) -> None:
    now = datetime.now(UTC)
    series = scenario("multi_point_series").rebased(now)
    target = max(
        (o for o in series.observations if o.observation.metric_key == "water.temperature"),
        key=lambda o: o.observation.observed_at,
    )
    newest_receipt = max(o.source.received_at for o in series.observations)
    newest_ingest = max(o.delivery.ingested_at for o in series.observations)
    replay = variant(
        target,
        observed_at=target.observation.observed_at - timedelta(minutes=9, seconds=30),
        received_at=newest_receipt + timedelta(seconds=1),
        ingested_at=newest_ingest + timedelta(seconds=1),
        replay=True,
    )
    assert all(replay.source.received_at > o.source.received_at for o in series.observations)
    assert all(replay.delivery.ingested_at > o.delivery.ingested_at for o in series.observations)
    seed(influx_bucket, list(series.observations) + [replay])
    selector = ObservationSelector("component", target.resolved.component_id)

    result = await latest(
        influx_bucket, selector, now, metric_keys=frozenset({"water.temperature"})
    )

    assert result == [stored_for(target)]
    # the replay is stored and readable; it just is not the latest by observed_at
    window = await query(
        influx_bucket,
        selector,
        start=replay.observation.observed_at,
        stop=replay.observation.observed_at + timedelta(microseconds=1),
        metric_keys=frozenset({"water.temperature"}),
    )
    assert window == [stored_for(replay)]


@pytest.mark.asyncio
async def test_a_future_skewed_point_is_never_latest(influx_bucket: str) -> None:
    now = datetime.now(UTC)
    series = scenario("multi_point_series").rebased(now)
    target = max(
        (o for o in series.observations if o.observation.metric_key == "water.dissolved_oxygen"),
        key=lambda o: o.observation.observed_at,
    )
    future = variant(
        target,
        observed_at=now + timedelta(minutes=5),
        received_at=now - timedelta(seconds=2),
        ingested_at=now - timedelta(seconds=1),
    )
    exactly_now = variant(target, observed_at=now, metric_key="water.ph")
    seed(influx_bucket, list(series.observations) + [future, exactly_now])
    selector = ObservationSelector("component", target.resolved.component_id)

    result = await latest(influx_bucket, selector, now)

    assert result, "seeded data must be readable"
    assert stored_for(target) in result
    assert all(item.observed_at < now for item in result)
    assert all(item.metric_key != "water.ph" for item in result)  # stop = now is excluded


@pytest.mark.asyncio
async def test_latest_lookback_lower_bound_is_inclusive(influx_bucket: str) -> None:
    now = datetime.now(UTC)
    base = scenario("two_probes_same_metric").observations[0]
    inside = variant(base, observed_at=now - timedelta(hours=24), metric_key="water.ph")
    outside = variant(
        base,
        observed_at=now - timedelta(hours=24, microseconds=1),
        metric_key="water.temperature",
    )
    seed(influx_bucket, [inside, outside])

    result = await latest(influx_bucket, ObservationSelector("device", NATIVE_DEVICE), now)

    assert result == [stored_for(inside)]
