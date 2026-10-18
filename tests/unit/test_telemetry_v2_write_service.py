"""Unit tests for the dual-write service (P2-03): switches, D7 exclusions and failure order."""

import inspect
from collections.abc import Sequence
from datetime import UTC, datetime

import pytest
from limnopulse_api.core.errors import TelemetryWriteError
from limnopulse_api.domain.assets import Asset, AssetType, AssetWithPondProfile, PondProfile
from limnopulse_api.domain.telemetry_v2 import (
    MAX_OBSERVATIONS_PER_BATCH,
    CanonicalObservation,
    SourceType,
)
from limnopulse_api.domain.telemetry_v2_schema import (
    LegacyPoint,
    build_legacy_points,
    encode_legacy_line,
)
from limnopulse_api.repositories.v2_contracts import SiteAssetReader
from limnopulse_api.services.telemetry_v2_writes import (
    ObservationWriteService,
    TelemetryWriteOutcome,
)
from tests.support.telemetry_v2_vectors import TelemetryScenario, scenario

CREATED = datetime(2026, 1, 1, tzinfo=UTC)
_GET_ASSET = inspect.signature(SiteAssetReader.get_asset)


class FakeObservationWriter:
    def __init__(self, events: list[str], error: BaseException | None = None) -> None:
        self.events = events
        self.error = error
        self.batches: list[list[CanonicalObservation]] = []

    async def write_observations(self, observations: Sequence[CanonicalObservation]) -> None:
        self.events.append("v2")
        self.batches.append(list(observations))
        if self.error is not None:
            raise self.error

    @property
    def written(self) -> list[CanonicalObservation]:
        return [item for batch in self.batches for item in batch]


class FakeLegacyWriter:
    def __init__(self, events: list[str], error: BaseException | None = None) -> None:
        self.events = events
        self.error = error
        self.batches: list[list[LegacyPoint]] = []

    async def write_legacy_points(self, points: Sequence[LegacyPoint]) -> None:
        self.events.append("legacy")
        self.batches.append(list(points))
        if self.error is not None:
            raise self.error

    @property
    def written(self) -> list[LegacyPoint]:
        return [item for batch in self.batches for item in batch]


class FakeSiteAssets:
    """`SiteAssetReader` fake keyed by `(tenant_id, asset_id)`; records every lookup."""

    def __init__(
        self,
        assets: dict[tuple[str, str], AssetWithPondProfile] | None = None,
        error: BaseException | None = None,
    ) -> None:
        self.assets = assets or {}
        self.error = error
        self.lookups: list[tuple[str, str]] = []

    async def get_site(self, tenant_id: str, site_id: str) -> None:
        raise AssertionError("the write service must not read Sites")

    async def get_asset(self, *args: object, **kwargs: object) -> AssetWithPondProfile | None:
        bound = _GET_ASSET.bind(self, *args, **kwargs)
        key = (bound.arguments["tenant_id"], bound.arguments["asset_id"])
        self.lookups.append(key)
        if self.error is not None:
            raise self.error
        return self.assets.get(key)


def pond_asset(
    tenant_id: str, site_id: str, asset_id: str, legacy_pond_id: str | None
) -> AssetWithPondProfile:
    return AssetWithPondProfile(
        asset=Asset(
            tenant_id=tenant_id,
            asset_id=asset_id,
            site_id=site_id,
            asset_type=AssetType.POND,
            name=f"Pond {asset_id[-4:]}",
            created_at=CREATED,
            updated_at=CREATED,
            version=1,
        ),
        pond_profile=PondProfile(
            tenant_id=tenant_id,
            asset_id=asset_id,
            legacy_pond_id=legacy_pond_id,
            created_at=CREATED,
            updated_at=CREATED,
            version=1,
        ),
    )


def assets_for(
    item: TelemetryScenario, pond_override: dict[str, str | None] | None = None
) -> FakeSiteAssets:
    """Assets for every resolved asset of the scenario, with the scenario's legacy ponds."""
    pond_override = pond_override or {}
    assets: dict[tuple[str, str], AssetWithPondProfile] = {}
    for o in item.observations:
        resolved = o.resolved
        pond = pond_override.get(
            resolved.asset_id, item.legacy_pond_by_asset.get(resolved.asset_id)
        )
        assets[(resolved.tenant_id, resolved.asset_id)] = pond_asset(
            resolved.tenant_id, resolved.site_id, resolved.asset_id, pond
        )
    return FakeSiteAssets(assets)


class Harness:
    def __init__(
        self,
        *,
        site_assets: FakeSiteAssets,
        v2: bool = True,
        legacy: bool = True,
        v2_error: BaseException | None = None,
        legacy_error: BaseException | None = None,
    ) -> None:
        self.events: list[str] = []
        self.writer = FakeObservationWriter(self.events, v2_error)
        self.legacy_writer = FakeLegacyWriter(self.events, legacy_error)
        self.site_assets = site_assets
        self.service = ObservationWriteService(
            writer=self.writer,
            legacy_writer=self.legacy_writer,
            site_assets=site_assets,
            v2_write_enabled=v2,
            legacy_dual_write_enabled=legacy,
        )


def outcome_tuple(outcome: TelemetryWriteOutcome) -> tuple[int, int, int]:
    return (outcome.v2_points, outcome.legacy_points, outcome.legacy_excluded)


def with_metric(o: CanonicalObservation, metric_key: str) -> CanonicalObservation:
    return o.model_copy(
        update={"observation": o.observation.model_copy(update={"metric_key": metric_key})}
    )


def with_value(o: CanonicalObservation, value: float) -> CanonicalObservation:
    return o.model_copy(update={"observation": o.observation.model_copy(update={"value": value})})


# --- construction -----------------------------------------------------------------------------


def test_outcome_has_the_three_counters() -> None:
    outcome = TelemetryWriteOutcome(v2_points=3, legacy_points=2, legacy_excluded=1)
    assert outcome_tuple(outcome) == (3, 2, 1)


def test_constructor_is_keyword_only() -> None:
    params = inspect.signature(ObservationWriteService).parameters
    assert set(params) == {
        "writer",
        "legacy_writer",
        "site_assets",
        "v2_write_enabled",
        "legacy_dual_write_enabled",
    }
    assert all(p.kind is inspect.Parameter.KEYWORD_ONLY for p in params.values())


@pytest.mark.parametrize(
    ("has_writer", "has_legacy_writer", "v2", "legacy"),
    [
        (False, True, True, False),
        (False, True, True, True),
        (True, False, False, True),
        (True, False, True, True),
        (False, False, True, True),
        (False, False, True, False),
        (False, False, False, True),
    ],
)
def test_missing_writer_for_an_enabled_switch_is_rejected(
    has_writer: bool, has_legacy_writer: bool, v2: bool, legacy: bool
) -> None:
    events: list[str] = []
    with pytest.raises(ValueError):
        ObservationWriteService(
            writer=FakeObservationWriter(events) if has_writer else None,
            legacy_writer=FakeLegacyWriter(events) if has_legacy_writer else None,
            site_assets=FakeSiteAssets(),
            v2_write_enabled=v2,
            legacy_dual_write_enabled=legacy,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("has_writer", "has_legacy_writer", "v2", "legacy"),
    [
        (False, False, False, False),
        (True, False, True, False),
        (False, True, False, True),
        (True, True, False, False),
    ],
)
async def test_writer_may_be_none_only_when_its_switch_is_off(
    has_writer: bool, has_legacy_writer: bool, v2: bool, legacy: bool
) -> None:
    item = scenario("legacy_sample")
    events: list[str] = []
    service = ObservationWriteService(
        writer=FakeObservationWriter(events) if has_writer else None,
        legacy_writer=FakeLegacyWriter(events) if has_legacy_writer else None,
        site_assets=assets_for(item),
        v2_write_enabled=v2,
        legacy_dual_write_enabled=legacy,
    )

    outcome = await service.write(item.observations)

    assert outcome_tuple(outcome) == (7 if v2 else 0, 1 if legacy else 0, 0)


# --- switches ---------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_both_switches_off_writes_nothing() -> None:
    item = scenario("legacy_sample")
    harness = Harness(site_assets=assets_for(item), v2=False, legacy=False)

    outcome = await harness.service.write(item.observations)

    assert outcome_tuple(outcome) == (0, 0, 0)
    assert harness.writer.written == []
    assert harness.legacy_writer.written == []
    assert harness.site_assets.lookups == []


@pytest.mark.asyncio
async def test_v2_only_writes_every_observation_and_no_legacy_point() -> None:
    item = scenario("legacy_sample")
    harness = Harness(site_assets=assets_for(item), v2=True, legacy=False)

    outcome = await harness.service.write(item.observations)

    assert outcome_tuple(outcome) == (7, 0, 0)
    assert harness.writer.written == list(item.observations)
    assert harness.legacy_writer.written == []
    assert harness.site_assets.lookups == []


@pytest.mark.asyncio
async def test_legacy_only_writes_the_legacy_point_and_no_v2_observation() -> None:
    item = scenario("legacy_sample")
    harness = Harness(site_assets=assets_for(item), v2=False, legacy=True)

    outcome = await harness.service.write(item.observations)

    assert outcome_tuple(outcome) == (0, 1, 0)
    assert harness.writer.written == []
    assert [encode_legacy_line(p) for p in harness.legacy_writer.written] == list(item.legacy_lines)


@pytest.mark.asyncio
async def test_both_switches_write_v2_first_then_legacy() -> None:
    item = scenario("legacy_sample")
    harness = Harness(site_assets=assets_for(item))

    outcome = await harness.service.write(item.observations)

    assert outcome_tuple(outcome) == (7, 1, 0)
    assert harness.events == ["v2", "legacy"]
    assert harness.writer.written == list(item.observations)
    assert harness.legacy_writer.written == list(
        build_legacy_points(item.observations, item.legacy_pond_by_asset)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name",
    [
        "legacy_sample",
        "sub_second",
        "out_of_range",
        "delayed_replay",
        "relocation",
        "unit_converted",
        "multi_point_series",
    ],
)
async def test_legacy_points_match_golden_lines_per_scenario(name: str) -> None:
    item = scenario(name)
    harness = Harness(site_assets=assets_for(item))

    outcome = await harness.service.write(item.observations)

    lines = [encode_legacy_line(p) for p in harness.legacy_writer.written]
    assert lines == list(item.legacy_lines)
    assert outcome_tuple(outcome) == (len(item.v2_lines), len(item.legacy_lines), 0)


@pytest.mark.asyncio
async def test_legacy_pond_comes_from_the_asset_pond_profile() -> None:
    item = scenario("legacy_sample")
    (asset_id,) = item.legacy_pond_by_asset
    harness = Harness(site_assets=assets_for(item, {asset_id: "pond_from_profile"}))

    await harness.service.write(item.observations)

    (point,) = harness.legacy_writer.written
    assert point.pond_id == "pond_from_profile"
    assert point.tenant_id == "tnt_local_001"
    assert point.device_id == "local-device-001"


# --- D7 exclusions ----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_native_probes_are_excluded_and_counted() -> None:
    item = scenario("two_probes_same_metric")
    harness = Harness(site_assets=assets_for(item))

    outcome = await harness.service.write(item.observations)

    assert outcome_tuple(outcome) == (2, 0, 2)
    assert harness.writer.written == list(item.observations)
    assert harness.legacy_writer.written == []


@pytest.mark.asyncio
async def test_asset_without_legacy_pond_is_excluded_and_counted() -> None:
    item = scenario("legacy_sample")
    (asset_id,) = item.legacy_pond_by_asset
    harness = Harness(site_assets=assets_for(item, {asset_id: None}))

    outcome = await harness.service.write(item.observations)

    assert outcome_tuple(outcome) == (7, 0, 7)
    assert harness.legacy_writer.written == []


@pytest.mark.asyncio
async def test_missing_asset_is_excluded_and_counted_not_raised() -> None:
    item = scenario("legacy_sample")
    harness = Harness(site_assets=FakeSiteAssets())

    outcome = await harness.service.write(item.observations)

    assert outcome_tuple(outcome) == (7, 0, 7)
    assert harness.legacy_writer.written == []
    assert harness.site_assets.lookups


@pytest.mark.asyncio
async def test_uncatalogued_metric_without_legacy_field_is_excluded_and_counted() -> None:
    """Every `metric-catalog@1` metric has a legacy field, so this D7 clause is reachable only
    with a synthetic, uncatalogued key (built with `model_copy`, which skips validation). v2 is
    off because the v2 encoder rightly rejects an uncatalogued metric."""
    item = scenario("legacy_sample")
    no_legacy_field = with_metric(item.observations[0], "water.chlorophyll")
    observations = [*item.observations, no_legacy_field]
    harness = Harness(site_assets=assets_for(item), v2=False)

    outcome = await harness.service.write(observations)

    assert outcome_tuple(outcome) == (0, 1, 1)
    assert [encode_legacy_line(p) for p in harness.legacy_writer.written] == list(item.legacy_lines)


def with_source_type(o: CanonicalObservation, source_type: SourceType) -> CanonicalObservation:
    return o.model_copy(update={"source": o.source.model_copy(update={"source_type": source_type})})


@pytest.mark.asyncio
@pytest.mark.parametrize("source_type", [SourceType.HTTPS, SourceType.MQTT, SourceType.EDGE])
async def test_legacy_identity_is_telegraf_whatever_the_source_type(
    source_type: SourceType,
) -> None:
    """D8: a legacy default Component dual-writes the stored Telegraf identity on any transport."""
    item = scenario("legacy_sample")
    observations = [with_source_type(o, source_type) for o in item.observations]
    harness = Harness(site_assets=assets_for(item))

    outcome = await harness.service.write(observations)

    assert outcome_tuple(outcome) == (7, 1, 0)
    assert [encode_legacy_line(p) for p in harness.legacy_writer.written] == list(item.legacy_lines)


@pytest.mark.asyncio
async def test_native_probe_is_excluded_even_over_the_legacy_transport() -> None:
    """D7 eligibility depends on the Component, never on `source_type`."""
    item = scenario("two_probes_same_metric")
    observations = [with_source_type(o, SourceType.LEGACY_MQTT) for o in item.observations]
    harness = Harness(site_assets=assets_for(item))

    outcome = await harness.service.write(observations)

    assert outcome_tuple(outcome) == (2, 0, 2)
    assert harness.legacy_writer.written == []


@pytest.mark.asyncio
async def test_non_integral_integer_metric_is_excluded_and_counted() -> None:
    item = scenario("legacy_sample")
    rssi = next(o for o in item.observations if o.observation.metric_key == "device.rssi")
    observations = [o for o in item.observations if o is not rssi] + [with_value(rssi, -67.5)]
    harness = Harness(site_assets=assets_for(item))

    outcome = await harness.service.write(observations)

    assert outcome_tuple(outcome) == (7, 1, 1)
    (point,) = harness.legacy_writer.written
    assert "rssi" not in point.fields


@pytest.mark.asyncio
async def test_mixed_batch_counts_each_exclusion_once() -> None:
    sample = scenario("legacy_sample")
    probes = scenario("two_probes_same_metric")
    relocation = scenario("relocation")
    west_asset = next(a for a, p in relocation.legacy_pond_by_asset.items() if p == "pond_west")
    site_assets = assets_for(sample)
    site_assets.assets.update(assets_for(probes).assets)
    east_only = assets_for(relocation)
    del east_only.assets[(relocation.tenant_id, west_asset)]
    site_assets.assets.update(east_only.assets)
    observations = [*sample.observations, *probes.observations, *relocation.observations]
    harness = Harness(site_assets=site_assets)

    outcome = await harness.service.write(observations)

    assert outcome_tuple(outcome) == (11, 2, 3)
    assert sorted(p.pond_id for p in harness.legacy_writer.written) == [
        "pond_east",
        "pond_local_001",
    ]


# --- Asset lookups ----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_asset_lookups_use_resolved_ownership_and_are_cached_per_call() -> None:
    item = scenario("multi_point_series")
    harness = Harness(site_assets=assets_for(item))
    expected = sorted({(o.resolved.tenant_id, o.resolved.asset_id) for o in item.observations})
    assert len(expected) == 2

    await harness.service.write(item.observations)
    assert sorted(harness.site_assets.lookups) == expected

    await harness.service.write(item.observations)
    assert sorted(harness.site_assets.lookups) == sorted(expected * 2)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [RuntimeError("lookup failed"), OSError("unreachable"), TimeoutError("slow")],
    ids=lambda error: type(error).__name__,
)
async def test_asset_lookup_exception_is_retryable_and_writes_no_legacy(
    error: BaseException,
) -> None:
    item = scenario("legacy_sample")
    harness = Harness(site_assets=FakeSiteAssets(error=error))

    with pytest.raises(TelemetryWriteError) as raised:
        await harness.service.write(item.observations)

    assert raised.value.retryable is True
    assert harness.legacy_writer.written == []


# --- failures ---------------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("retryable", [True, False])
async def test_v2_failure_attempts_no_legacy_write(retryable: bool) -> None:
    item = scenario("legacy_sample")
    error = TelemetryWriteError("v2 write failed", retryable=retryable)
    harness = Harness(site_assets=assets_for(item), v2_error=error)

    with pytest.raises(TelemetryWriteError) as raised:
        await harness.service.write(item.observations)

    assert raised.value.retryable is retryable
    assert harness.events == ["v2"]
    assert harness.legacy_writer.batches == []


@pytest.mark.asyncio
@pytest.mark.parametrize("retryable", [True, False])
async def test_legacy_failure_after_v2_success_raises(retryable: bool) -> None:
    item = scenario("legacy_sample")
    error = TelemetryWriteError("legacy write failed", retryable=retryable)
    harness = Harness(site_assets=assets_for(item), legacy_error=error)

    with pytest.raises(TelemetryWriteError) as raised:
        await harness.service.write(item.observations)

    assert raised.value.retryable is retryable
    assert harness.events == ["v2", "legacy"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failing", ["v2", "legacy"])
async def test_any_writer_failure_surfaces_as_telemetry_write_error(failing: str) -> None:
    """D9: any failure raises TelemetryWriteError, even one a writer did not classify."""
    item = scenario("legacy_sample")
    error = OSError("socket closed")
    harness = Harness(
        site_assets=assets_for(item),
        v2_error=error if failing == "v2" else None,
        legacy_error=error if failing == "legacy" else None,
    )

    with pytest.raises(TelemetryWriteError):
        await harness.service.write(item.observations)

    assert harness.events == (["v2"] if failing == "v2" else ["v2", "legacy"])


# --- batch limit ------------------------------------------------------------------------------


def _batch(size: int) -> list[CanonicalObservation]:
    observations = scenario("multi_point_series").observations
    return [observations[index % len(observations)] for index in range(size)]


@pytest.mark.asyncio
async def test_batch_at_the_limit_is_accepted() -> None:
    item = scenario("multi_point_series")
    harness = Harness(site_assets=assets_for(item))

    outcome = await harness.service.write(_batch(MAX_OBSERVATIONS_PER_BATCH))

    assert MAX_OBSERVATIONS_PER_BATCH == 1000
    assert outcome.v2_points == MAX_OBSERVATIONS_PER_BATCH
    assert outcome.legacy_points == len(item.legacy_lines)
    assert outcome.legacy_excluded == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(("v2", "legacy"), [(True, True), (True, False), (False, True)])
async def test_batch_over_the_limit_is_rejected_before_any_write(v2: bool, legacy: bool) -> None:
    item = scenario("multi_point_series")
    harness = Harness(site_assets=assets_for(item), v2=v2, legacy=legacy)

    with pytest.raises((ValueError, TelemetryWriteError)) as raised:
        await harness.service.write(_batch(MAX_OBSERVATIONS_PER_BATCH + 1))

    if isinstance(raised.value, TelemetryWriteError):
        assert raised.value.retryable is False
    assert harness.events == []
    assert harness.site_assets.lookups == []
