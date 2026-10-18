"""Real-InfluxDB equivalence of the `/v1` projection over `observation_numeric` (P2-07).

Every scenario is dual-written through the P2-03 `ObservationWriteService` (both switches on)
into one bucket, so the legacy `InfluxTelemetryRepository` reads `water_quality` and
`V2ProjectedTelemetryRepository` reads `observation_numeric` from the same writes. Data is seeded
relative to a `now` captured once per test; every compared result is asserted non-empty first.

Readings and latest are byte-identical. Summaries are identical except their float means, which
match within `rel_tol=1e-12` (plan r7.2): storage sums partials per (series, shard group) in
series-key order, and the v2 series keys differ from the legacy ones by design.
"""

from __future__ import annotations

import math
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient

from limnopulse_api.adapters.influxdb import InfluxTelemetryRepository
from limnopulse_api.adapters.influxdb_v1_projection import V2ProjectedTelemetryRepository
from limnopulse_api.adapters.influxdb_v2_writer import InfluxObservationWriter
from limnopulse_api.core.config import Settings
from limnopulse_api.domain.assets import Asset, AssetType, AssetWithPondProfile, PondProfile
from limnopulse_api.domain.entities import Device, Membership, Pond
from limnopulse_api.domain.ids import (
    asset_id_for_legacy_pond,
    default_component_id_for_legacy_device,
    default_probe_profile_id_for_legacy_device,
    default_site_id,
    deployment_id_for_legacy_device,
)
from limnopulse_api.domain.metric_catalog import metric_definition
from limnopulse_api.domain.roles import TenantRole
from limnopulse_api.domain.telemetry import LatestMetrics, MetricsSummary
from limnopulse_api.domain.telemetry_v2 import CanonicalObservation, observation_event_id
from limnopulse_api.main import create_app
from limnopulse_api.services.telemetry_v2_writes import ObservationWriteService
from tests.support.influx_harness import influx_clients, influx_settings
from tests.support.telemetry_v2_vectors import scenario

TENANT = "tnt_local_001"
OTHER_TENANT = "tnt_other_001"
EMPTY_TENANT = "tnt_empty_001"
CREATED = datetime(2026, 1, 1, tzinfo=UTC)
PERIODS = ("24h", "7d", "30d")
PONDS = ("pond_local_001", "pond_north", "pond_south", "pond_sum", "pond_tie")
FLOAT_FIELDS = ("temp_c", "ph", "do_mg_l", "turbidity_ntu", "salinity_ppt", "battery_v")
INTEGER_FIELDS = ("rssi", "seq")
SUM_DEVICES = ("sum-device-1", "sum-device-2", "sum-device-3")
SHARD_POND = "pond_shard"
SHARD_DEVICES = ("shard-device-1", "shard-device-2")
# Hours before `now` for the shard pond: 9 days apart, so the points lie in different 7-day
# shard groups (the default for a bucket without retention); none is inside the last 24 h.
SHARD_HOURS = (26.5, 50.5, 9 * 24 + 0.5, 9 * 24 + 1.5)
SHARD_GROUP = timedelta(days=7)
TIE_DEVICE = "tie-device-001"
NATIVE_COMPONENT = "cmp_0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f"
SEQ_2_53_PLUS_1 = 2**53 + 1
SEQ_MAX = 2**63 - 1
# Two samples per device in one 5-minute window of a pond with three devices.
SUM_VALUES = {
    "sum-device-1": (26.5, 25.3),
    "sum-device-2": (28.0, 26.8),
    "sum-device-3": (26.9, 61.3),
}
TEMPLATE = scenario("legacy_sample").observations[0]
OUT_OF_RANGE_QUALITY = scenario("out_of_range").observations[0].quality


# --- fakes ------------------------------------------------------------------------------------


class InMemorySiteAssets:
    def __init__(self) -> None:
        self.assets: dict[tuple[str, str], AssetWithPondProfile] = {}

    def add(self, tenant_id: str, asset_id: str, site_id: str, legacy_pond_id: str) -> None:
        self.assets[(tenant_id, asset_id)] = AssetWithPondProfile(
            asset=Asset(
                tenant_id=tenant_id,
                asset_id=asset_id,
                site_id=site_id,
                asset_type=AssetType.POND,
                name="Pond",
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

    async def get_site(self, tenant_id: str, site_id: str) -> None:
        raise AssertionError("the write service must not read Sites")

    async def get_asset(self, tenant_id: str, asset_id: str) -> AssetWithPondProfile | None:
        return self.assets.get((tenant_id, asset_id))


class FakeDomainRepository:
    """The M1 `/v1` domain repository surface the router and the projection use."""

    def __init__(
        self, devices: dict[str, dict[str, str]], ponds: dict[str, tuple[str, ...]]
    ) -> None:
        # tenant -> device_id -> pond_id, plus ponds that have no device.
        self.devices = devices
        self.ponds = ponds

    async def list_devices(self, tenant_id: str) -> list[Device]:
        return [
            Device(
                tenant_id=tenant_id,
                pond_id=pond_id,
                device_id=device_id,
                name=device_id,
                created_at=CREATED,
                updated_at=CREATED,
                version=1,
            )
            for device_id, pond_id in sorted(self.devices.get(tenant_id, {}).items())
        ]

    async def list_ponds(self, tenant_id: str) -> list[Pond]:
        return [self._pond(tenant_id, pond_id) for pond_id in self._pond_ids(tenant_id)]

    async def get_pond(self, tenant_id: str, pond_id: str) -> Pond | None:
        if pond_id in self._pond_ids(tenant_id):
            return self._pond(tenant_id, pond_id)
        return None

    def _pond_ids(self, tenant_id: str) -> list[str]:
        ponds = set(self.devices.get(tenant_id, {}).values()) | set(self.ponds.get(tenant_id, ()))
        return sorted(ponds)

    def _pond(self, tenant_id: str, pond_id: str) -> Pond:
        return Pond(
            tenant_id=tenant_id,
            pond_id=pond_id,
            name=pond_id,
            created_at=CREATED,
            updated_at=CREATED,
            version=1,
        )


class FakeMembershipService:
    async def get_active_membership(self, cognito_sub: str, tenant_id: str) -> Membership | None:
        if cognito_sub != "sub_1":
            return None
        return Membership(
            tenant_id=tenant_id,
            cognito_sub=cognito_sub,
            role=TenantRole.VIEWER,
            created_at=CREATED,
            updated_at=CREATED,
            version=1,
        )


class RecordingQueryApi:
    """Delegates to the real `query_api` and keeps every returned record's values."""

    def __init__(self, query_api: Any) -> None:
        self.query_api = query_api
        self.records: list[dict[str, Any]] = []

    def query(self, query: str, org: str):
        tables = self.query_api.query(query, org)
        for table in tables:
            self.records.extend(record.values for record in table.records)
        return tables


# --- seeding ----------------------------------------------------------------------------------


def make_observation(
    *,
    tenant_id: str,
    pond_id: str,
    device_id: str,
    metric_key: str,
    value: float,
    observed_at: datetime,
    sequence: int | None,
    ingested_at: datetime | None = None,
    component_id: str | None = None,
    out_of_range: bool = False,
) -> CanonicalObservation:
    """A test-local observation of a legacy device; not part of the frozen vectors."""
    definition = metric_definition(metric_key)
    assert definition is not None
    component = component_id or default_component_id_for_legacy_device(tenant_id, device_id)
    data = TEMPLATE.model_dump()
    data["resolved"] = {
        "tenant_id": tenant_id,
        "site_id": default_site_id(tenant_id),
        "asset_id": asset_id_for_legacy_pond(tenant_id, pond_id),
        "deployment_id": deployment_id_for_legacy_device(tenant_id, device_id),
        "device_id": device_id,
        "component_id": component,
        "probe_id": default_probe_profile_id_for_legacy_device(tenant_id, device_id),
    }
    data["observation"].update(
        metric_key=metric_key,
        value=float(value),
        value_type=definition.value_type,
        source_unit=definition.canonical_unit,
        canonical_unit=definition.canonical_unit,
        observed_at=observed_at,
        sequence=sequence,
    )
    data["source"]["received_at"] = observed_at + timedelta(seconds=1)
    data["delivery"]["ingested_at"] = ingested_at or observed_at + timedelta(seconds=2)
    if out_of_range:
        data["quality"] = OUT_OF_RANGE_QUALITY.model_dump()
    data["event_id"] = observation_event_id(tenant_id, component, metric_key, observed_at)
    return CanonicalObservation.model_validate(data)


@dataclass(frozen=True)
class Seeded:
    now: datetime
    devices: dict[str, dict[str, str]]
    out_of_range_at: datetime
    sub_second_at: datetime
    expected_tie_seq: dict[datetime, int | None]


def _sum_observations(base: datetime) -> list[CanonicalObservation]:
    observations = []
    for device_id, values in SUM_VALUES.items():
        for index, value in enumerate(values):
            observations.append(
                make_observation(
                    tenant_id=TENANT,
                    pond_id="pond_sum",
                    device_id=device_id,
                    metric_key="water.temperature",
                    value=value,
                    observed_at=base + timedelta(seconds=30 + 60 * index),
                    sequence=index,
                    # Mixed quality_state: /v1 has no quality, so every state stays visible.
                    out_of_range=device_id == "sum-device-3" and index == 1,
                )
            )
    return observations


def _shard_observations(now: datetime) -> list[CanonicalObservation]:
    observations = []
    for index, device_id in enumerate(SHARD_DEVICES):
        for position, hours in enumerate(SHARD_HOURS):
            for metric_key, base in (("water.temperature", 24.1), ("water.ph", 7.01)):
                observations.append(
                    make_observation(
                        tenant_id=TENANT,
                        pond_id=SHARD_POND,
                        device_id=device_id,
                        metric_key=metric_key,
                        value=base + 0.1 * position + 0.07 * index,
                        observed_at=now - timedelta(hours=hours),
                        sequence=position,
                    )
                )
    return observations


def _tie_observations(base: datetime) -> tuple[list[CanonicalObservation], dict]:
    def tie(metric_key: str, value: float, at: datetime, sequence, ingested: datetime):
        return make_observation(
            tenant_id=TENANT,
            pond_id="pond_tie",
            device_id=TIE_DEVICE,
            metric_key=metric_key,
            value=value,
            observed_at=at,
            sequence=sequence,
            ingested_at=ingested,
        )

    observations: list[CanonicalObservation] = []
    expected: dict[datetime, int | None] = {}

    # A: the later `ingested_at` wins even though its `event_id` is the smaller one.
    at = base
    first, second = sorted(
        ("water.temperature", "water.ph"),
        key=lambda key: observation_event_id(
            TENANT, default_component_id_for_legacy_device(TENANT, TIE_DEVICE), key, at
        ),
    )
    observations += [
        tie(second, 7.1, at, 20, at + timedelta(seconds=3)),
        tie(first, 7.2, at, 10, at + timedelta(seconds=5)),
    ]
    expected[at] = 10

    # B: equal `ingested_at`, so the greater `event_id` wins. The time is chosen so that the
    # winner is `water.ph`, which sorts before `water.temperature`: ranking by `ingested_at_ns`
    # alone would keep the row that arrives last instead.
    at = next(
        candidate
        for candidate in (base + timedelta(minutes=1, seconds=s) for s in range(60))
        if _tie_event_id("water.ph", candidate) > _tie_event_id("water.temperature", candidate)
    )
    pair = [
        tie("water.temperature", 24.1, at, 30, at + timedelta(seconds=3)),
        tie("water.ph", 7.3, at, 40, at + timedelta(seconds=3)),
    ]
    winner = max(pair, key=lambda o: o.event_id)
    assert winner.observation.metric_key == "water.ph"
    observations += sorted(pair, key=lambda o: o.event_id, reverse=True)
    expected[at] = winner.observation.sequence

    # C: the latest-ingested point has no sequence; absent sequences rank lowest.
    at = base + timedelta(minutes=2)
    observations += [
        tie("water.ph", 7.4, at, 50, at + timedelta(seconds=3)),
        tie("water.temperature", 24.2, at, None, at + timedelta(seconds=5)),
    ]
    expected[at] = 50

    # D: no sequence at all, so `seq` is omitted.
    at = base + timedelta(minutes=3)
    observations += [
        tie("water.ph", 7.5, at, None, at + timedelta(seconds=3)),
        tie("water.temperature", 24.3, at, None, at + timedelta(seconds=5)),
    ]
    expected[at] = None

    # Integer exactness beyond float precision; the last one is also the pond's latest.
    at = base + timedelta(minutes=4)
    observations.append(tie("water.temperature", 24.4, at, SEQ_2_53_PLUS_1, at))
    expected[at] = SEQ_2_53_PLUS_1
    at = base + timedelta(minutes=5)
    # The latest point also has conflicting sequences, so the latest path ranks them too.
    observations += [
        tie("device.rssi", -71, at, 5, at),
        tie("water.temperature", 24.5, at, SEQ_MAX, at + timedelta(seconds=1)),
    ]
    expected[at] = SEQ_MAX
    return observations, expected


def _tie_event_id(metric_key: str, at: datetime) -> str:
    component = default_component_id_for_legacy_device(TENANT, TIE_DEVICE)
    return observation_event_id(TENANT, component, metric_key, at)


def _site_assets(devices: dict[str, dict[str, str]]) -> InMemorySiteAssets:
    assets = InMemorySiteAssets()
    for tenant_id, device_ponds in devices.items():
        for pond_id in set(device_ponds.values()):
            assets.add(
                tenant_id,
                asset_id_for_legacy_pond(tenant_id, pond_id),
                default_site_id(tenant_id),
                pond_id,
            )
    return assets


def seed(bucket: str, now: datetime) -> Seeded:
    legacy_sample = scenario("legacy_sample").rebased(now - timedelta(minutes=20))
    # `now` has whole seconds, so the rebased point keeps a `.750` fraction.
    sub_second = scenario("sub_second").rebased(now - timedelta(minutes=19, microseconds=250000))
    out_of_range = scenario("out_of_range").rebased(now - timedelta(minutes=18))
    series = scenario("multi_point_series").rebased(now - timedelta(minutes=2))
    # Native v2 probes on the legacy pond Asset, at the most recent time.
    native = scenario("two_probes_same_metric").rebased(now)
    native_on_legacy_device = make_observation(
        tenant_id=TENANT,
        pond_id="pond_local_001",
        device_id="local-device-001",
        metric_key="water.dissolved_oxygen",
        value=9.99,
        observed_at=now - timedelta(milliseconds=500),
        sequence=999,
        component_id=NATIVE_COMPONENT,
    )
    other_tenant = make_observation(
        tenant_id=OTHER_TENANT,
        pond_id="pond_local_001",
        device_id="local-device-001",
        metric_key="water.temperature",
        value=33.3,
        observed_at=now - timedelta(milliseconds=500),
        sequence=7,
    )
    # Another tenant's point carrying this tenant's asset and component IDs: only the
    # tenant filter keeps it out (v2 only; its Asset is unknown, so it is not dual-written).
    other_tenant_same_ids = make_observation(
        tenant_id=OTHER_TENANT,
        pond_id="pond_local_001",
        device_id="local-device-001",
        metric_key="water.dissolved_oxygen",
        value=44.4,
        observed_at=now - timedelta(milliseconds=400),
        sequence=8,
        component_id=default_component_id_for_legacy_device(TENANT, "local-device-001"),
    )
    other_data = other_tenant_same_ids.model_dump()
    other_data["resolved"]["asset_id"] = asset_id_for_legacy_pond(TENANT, "pond_local_001")
    other_tenant_same_ids = CanonicalObservation.model_validate(other_data)
    sum_base = _five_minute_floor(now - timedelta(minutes=15))
    sum_observations = _sum_observations(sum_base)
    tie_observations, expected_tie_seq = _tie_observations(now - timedelta(minutes=12))

    shard_observations = _shard_observations(now)
    devices = {
        TENANT: {
            "local-device-001": "pond_local_001",
            "series-device-a": "pond_north",
            "series-device-b": "pond_south",
            **{device_id: "pond_sum" for device_id in SUM_DEVICES},
            TIE_DEVICE: "pond_tie",
            **{device_id: SHARD_POND for device_id in SHARD_DEVICES},
        },
        OTHER_TENANT: {"local-device-001": "pond_local_001"},
    }
    batches = [
        legacy_sample.observations + sub_second.observations + out_of_range.observations,
        series.observations,
        native.observations + (native_on_legacy_device, other_tenant, other_tenant_same_ids),
        tuple(sum_observations) + tuple(tie_observations),
    ]
    # Every point lies in the hour before `now` (a completed hour), within every read window.
    for batch in batches:
        for o in batch:
            assert now - timedelta(hours=1) <= o.observation.observed_at < now
    assert len(series.observations) >= 2 * 30
    # The shard pond spans two shard groups, in completed hours outside the last 24 h.
    shard_groups = {_shard_group(o.observation.observed_at) for o in shard_observations}
    assert len(shard_groups) >= 2
    assert all(o.observation.observed_at < now - timedelta(hours=25) for o in shard_observations)
    batches.append(tuple(shard_observations))
    site_assets = _site_assets(devices)
    settings = influx_settings()
    with influx_clients(bucket) as (_query_api, write_api):
        writer = InfluxObservationWriter(write_api=write_api, bucket=bucket, org=settings.org)
        service = ObservationWriteService(
            writer=writer,
            legacy_writer=writer,
            site_assets=site_assets,
            v2_write_enabled=True,
            legacy_dual_write_enabled=True,
        )
        excluded = 0
        for batch in batches:
            outcome = _run(service.write(batch))
            assert outcome.v2_points == len(batch)
            excluded += outcome.legacy_excluded
    # The three native observations and the unknown-Asset point stay out of `water_quality`.
    assert excluded == 4
    return Seeded(
        now=now,
        devices=devices,
        out_of_range_at=out_of_range.observations[0].observation.observed_at,
        sub_second_at=sub_second.observations[0].observation.observed_at,
        expected_tie_seq=expected_tie_seq,
    )


def _five_minute_floor(value: datetime) -> datetime:
    return value.replace(minute=value.minute - value.minute % 5, second=0, microsecond=0)


def _shard_group(value: datetime) -> int:
    return (value - datetime(1970, 1, 1, tzinfo=UTC)) // SHARD_GROUP


def _means_match(actual: float | None, expected: float | None) -> bool:
    if actual is None or expected is None:
        return actual is expected
    return math.isclose(actual, expected, rel_tol=1e-12, abs_tol=0.0)


def assert_summaries_match(actual: Any, expected: Any, context: Any = None) -> None:
    """Exact on everything but the float means; the means within `rel_tol=1e-12` (r7.2)."""
    if isinstance(actual, MetricsSummary):
        actual, expected = actual.model_dump(), expected.model_dump()
    assert actual.keys() == expected.keys(), context
    for key in ("tenant_id", "pond_id", "period", "interval"):
        assert actual[key] == expected[key], context
    assert [p["measured_at"] for p in actual["series"]] == [
        p["measured_at"] for p in expected["series"]
    ], context
    for got, want in zip(actual["series"], expected["series"], strict=True):
        assert got.keys() == want.keys(), context
        for metric in ("do_mg_l", "ph", "temp_c"):
            assert _means_match(got[metric], want[metric]), (context, got, want)
    assert actual["statistics"].keys() == expected["statistics"].keys(), context
    for metric, want in expected["statistics"].items():
        got = actual["statistics"][metric]
        assert (got["min"], got["max"], got["count"]) == (
            want["min"],
            want["max"],
            want["count"],
        ), (context, metric)
        assert _means_match(got["mean"], want["mean"]), (context, metric, got, want)


def _run(awaitable):
    import anyio

    async def main():
        return await awaitable

    return anyio.run(main)


@contextmanager
def repositories(bucket: str, devices) -> Iterator[tuple[Any, Any, RecordingQueryApi, Any]]:
    settings = influx_settings()
    domain = FakeDomainRepository(devices, {EMPTY_TENANT: ("pond_empty",)})
    with influx_clients(bucket) as (query_api, _write_api):
        recording = RecordingQueryApi(query_api)
        legacy = InfluxTelemetryRepository(query_api=query_api, org=settings.org, bucket=bucket)
        projected = V2ProjectedTelemetryRepository(
            query_api=recording, org=settings.org, bucket=bucket, domain_repository=domain
        )
        yield legacy, projected, recording, domain


@pytest.fixture
def seeded(influx_bucket: str) -> Seeded:
    # The summary's last window is stamped with the query's `_stop`, so every point lies in a
    # completed hour (and 5-minute window): both repositories then see identical windows.
    captured = datetime.now(UTC)
    return seed(influx_bucket, captured.replace(minute=0, second=0, microsecond=0))


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _reading_calls(seeded: Seeded) -> list[dict[str, Any]]:
    now = seeded.now
    calls = [{"pond_id": pond_id, "start": "-2h", "stop": None, "limit": 1000} for pond_id in PONDS]
    calls += [
        # `limit` smaller than the rows: legacy applies it per series table (per device).
        {"pond_id": "pond_north", "start": "-2h", "stop": None, "limit": 7},
        {"pond_id": "pond_sum", "start": "-2h", "stop": None, "limit": 1},
        {"pond_id": "pond_tie", "start": "-2h", "stop": None, "limit": 3},
        # A 30-day range across shard groups.
        {"pond_id": SHARD_POND, "start": "-30d", "stop": None, "limit": 1000},
        {"pond_id": SHARD_POND, "start": "-30d", "stop": None, "limit": 2},
        # A relative start with an absolute stop.
        {
            "pond_id": "pond_north",
            "start": "-2h",
            "stop": _iso(now - timedelta(minutes=5)),
            "limit": 1000,
        },
        # Absolute bounds, sub-second data included.
        {
            "pond_id": "pond_local_001",
            "start": _iso(now - timedelta(hours=1)),
            "stop": _iso(now),
            "limit": 1000,
        },
        {
            "pond_id": "pond_north",
            "start": _iso(now - timedelta(minutes=25)),
            "stop": _iso(now - timedelta(minutes=5)),
            "limit": 1000,
        },
    ]
    return calls


def _assert_nonempty(result: Any) -> None:
    if isinstance(result, list):
        assert result
    elif isinstance(result, LatestMetrics):
        assert result.measured_at is not None
    elif isinstance(result, MetricsSummary):
        assert result.series
        assert any(stats.count > 0 for stats in result.statistics.values())
    else:  # pragma: no cover - defensive
        raise TypeError(f"unexpected result {result!r}")


# --- domain-level equality --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_readings_are_equal_to_legacy(influx_bucket: str, seeded: Seeded) -> None:
    with repositories(influx_bucket, seeded.devices) as (legacy, projected, _recording, _domain):
        for call in _reading_calls(seeded):
            expected = await legacy.query_readings(tenant_id=TENANT, **call)
            actual = await projected.query_readings(tenant_id=TENANT, **call)
            _assert_nonempty(expected)
            assert actual == expected, call


@pytest.mark.asyncio
async def test_readings_limit_applies_per_device_like_legacy(
    influx_bucket: str, seeded: Seeded
) -> None:
    with repositories(influx_bucket, seeded.devices) as (_legacy, projected, _recording, _domain):
        readings = await projected.query_readings(
            tenant_id=TENANT, pond_id="pond_sum", start="-2h", stop=None, limit=1
        )
    assert [reading.device_id for reading in readings] == list(SUM_DEVICES)
    with repositories(influx_bucket, seeded.devices) as (_legacy, projected, _recording, _domain):
        north = await projected.query_readings(
            tenant_id=TENANT, pond_id="pond_north", start="-2h", stop=None, limit=1000
        )
        limited = await projected.query_readings(
            tenant_id=TENANT, pond_id="pond_north", start="-2h", stop=None, limit=7
        )
    assert len(north) >= 30
    assert limited == north[:7]


@pytest.mark.asyncio
async def test_latest_metrics_are_equal_to_legacy(influx_bucket: str, seeded: Seeded) -> None:
    with repositories(influx_bucket, seeded.devices) as (legacy, projected, _recording, _domain):
        for pond_id in PONDS:
            expected = await legacy.query_latest_metrics(tenant_id=TENANT, pond_id=pond_id)
            actual = await projected.query_latest_metrics(tenant_id=TENANT, pond_id=pond_id)
            _assert_nonempty(expected)
            assert actual == expected, pond_id


@pytest.mark.asyncio
async def test_latest_metrics_for_tenant_are_equal_to_legacy(
    influx_bucket: str, seeded: Seeded
) -> None:
    with repositories(influx_bucket, seeded.devices) as (legacy, projected, _recording, _domain):
        for tenant_id in (TENANT, OTHER_TENANT):
            expected = await legacy.query_latest_metrics_for_tenant(tenant_id=tenant_id)
            actual = await projected.query_latest_metrics_for_tenant(tenant_id=tenant_id)
            _assert_nonempty(expected)
            assert actual == expected, tenant_id
    assert [item.pond_id for item in actual] == ["pond_local_001"]


@pytest.mark.asyncio
async def test_summaries_are_equal_to_legacy_for_every_period(
    influx_bucket: str, seeded: Seeded
) -> None:
    with repositories(influx_bucket, seeded.devices) as (legacy, projected, _recording, _domain):
        for pond_id in PONDS:
            for period in PERIODS:
                expected = await legacy.query_summary(
                    tenant_id=TENANT, pond_id=pond_id, period=period
                )
                actual = await projected.query_summary(
                    tenant_id=TENANT, pond_id=pond_id, period=period
                )
                _assert_nonempty(expected)
                assert_summaries_match(actual, expected, (pond_id, period))


@pytest.mark.asyncio
async def test_summary_across_shard_groups_matches_legacy(
    influx_bucket: str, seeded: Seeded
) -> None:
    with repositories(influx_bucket, seeded.devices) as (legacy, projected, _recording, _domain):
        results = {}
        for period in ("7d", "30d"):
            expected = await legacy.query_summary(
                tenant_id=TENANT, pond_id=SHARD_POND, period=period
            )
            actual = await projected.query_summary(
                tenant_id=TENANT, pond_id=SHARD_POND, period=period
            )
            _assert_nonempty(expected)
            assert_summaries_match(actual, expected, period)
            results[period] = actual
    points_per_metric = len(SHARD_DEVICES) * len(SHARD_HOURS)
    # 30d reads both shard groups; 7d only the two recent hours of each device.
    assert results["30d"].statistics["temp_c"].count == points_per_metric
    assert results["7d"].statistics["temp_c"].count == len(SHARD_DEVICES) * 2
    assert len(results["30d"].series) == len(SHARD_HOURS)


# --- exclusions, tie-break and integers -------------------------------------------------------


@pytest.mark.asyncio
async def test_native_probes_and_other_tenants_are_not_visible(
    influx_bucket: str, seeded: Seeded
) -> None:
    with repositories(influx_bucket, seeded.devices) as (_legacy, projected, _recording, _domain):
        readings = await projected.query_readings(
            tenant_id=TENANT, pond_id="pond_local_001", start="-2h", stop=None, limit=1000
        )
        latest = await projected.query_latest_metrics(tenant_id=TENANT, pond_id="pond_local_001")
        tenant_latest = await projected.query_latest_metrics_for_tenant(tenant_id=TENANT)
        summary = await projected.query_summary(
            tenant_id=TENANT, pond_id="pond_local_001", period="24h"
        )

    assert readings
    assert {reading.device_id for reading in readings} == {"local-device-001"}
    assert all(reading.measured_at <= seeded.out_of_range_at for reading in readings)
    assert all(reading.do_mg_l != 9.99 and reading.temp_c != 33.3 for reading in readings)
    # The latest legacy-visible point is the out-of-range DO (D15), not a native probe.
    assert latest.measured_at == seeded.out_of_range_at
    assert latest.do_mg_l == 75.0
    local = next(item for item in tenant_latest if item.pond_id == "pond_local_001")
    assert local.measured_at == seeded.out_of_range_at
    assert {item.pond_id for item in tenant_latest} == set(PONDS)
    assert summary.statistics["do_mg_l"].max == 75.0
    assert summary.statistics["do_mg_l"].count == 3


@pytest.mark.asyncio
async def test_sub_second_time_is_exact(influx_bucket: str, seeded: Seeded) -> None:
    assert seeded.sub_second_at.microsecond == 750000
    with repositories(influx_bucket, seeded.devices) as (legacy, projected, _recording, _domain):
        call = {
            "pond_id": "pond_local_001",
            "start": _iso(seeded.now - timedelta(hours=1)),
            "stop": _iso(seeded.now),
            "limit": 1000,
        }
        expected = await legacy.query_readings(tenant_id=TENANT, **call)
        actual = await projected.query_readings(tenant_id=TENANT, **call)
    assert seeded.sub_second_at in [reading.measured_at for reading in actual]
    assert actual == expected


@pytest.mark.asyncio
async def test_seq_tie_break_and_integer_boundaries(influx_bucket: str, seeded: Seeded) -> None:
    with repositories(influx_bucket, seeded.devices) as (legacy, projected, recording, _domain):
        expected = await legacy.query_readings(
            tenant_id=TENANT, pond_id="pond_tie", start="-2h", stop=None, limit=1000
        )
        actual = await projected.query_readings(
            tenant_id=TENANT, pond_id="pond_tie", start="-2h", stop=None, limit=1000
        )
        before_latest = len(recording.records)
        latest = await projected.query_latest_metrics(tenant_id=TENANT, pond_id="pond_tie")
        legacy_latest = await legacy.query_latest_metrics(tenant_id=TENANT, pond_id="pond_tie")
        raw_latest_seqs = [
            values["sequence"]
            for values in recording.records[before_latest:]
            if "sequence" in values
        ]

    assert {reading.measured_at: reading.seq for reading in actual} == seeded.expected_tie_seq
    assert {reading.measured_at: reading.seq for reading in expected} == seeded.expected_tie_seq
    assert actual == expected
    for reading in actual:
        assert reading.seq is None or type(reading.seq) is int
    assert latest == legacy_latest
    assert latest.seq == SEQ_MAX and type(latest.seq) is int
    assert latest.rssi == -71 and type(latest.rssi) is int
    # The Flux result itself carries integers: no reliance on Pydantic lax coercion.
    raw_seqs = [values["sequence"] for values in recording.records if "sequence" in values]
    assert SEQ_2_53_PLUS_1 in raw_seqs and SEQ_MAX in raw_seqs
    assert all(type(value) is int for value in raw_seqs)
    assert SEQ_MAX in raw_latest_seqs


@pytest.mark.asyncio
async def test_projected_values_keep_legacy_field_types(influx_bucket: str, seeded: Seeded) -> None:
    with repositories(influx_bucket, seeded.devices) as (_legacy, projected, recording, _domain):
        results: list[Any] = []
        for pond_id in PONDS:
            results += await projected.query_readings(
                tenant_id=TENANT, pond_id=pond_id, start="-2h", stop=None, limit=1000
            )
            results.append(await projected.query_latest_metrics(tenant_id=TENANT, pond_id=pond_id))
        results += await projected.query_latest_metrics_for_tenant(tenant_id=TENANT)

    seen = {field: 0 for field in FLOAT_FIELDS + INTEGER_FIELDS}
    for result in results:
        for field in seen:
            value = getattr(result, field)
            if value is None:
                continue
            seen[field] += 1
            expected_type = int if field in INTEGER_FIELDS else float
            assert type(value) is expected_type, (field, value)
    assert all(seen.values()), seen
    # Raw Flux: canonical values are floats and sequences integers; no `water_quality` rows.
    assert recording.records
    for values in recording.records:
        assert values.get("_measurement") in (None, "observation_numeric")
        if "sequence" in values:
            assert type(values["sequence"]) is int


@pytest.mark.asyncio
async def test_a_tenant_without_legacy_devices_reads_empty_like_legacy(
    influx_bucket: str, seeded: Seeded
) -> None:
    with repositories(influx_bucket, seeded.devices) as (legacy, projected, _recording, _domain):
        pond = {"tenant_id": EMPTY_TENANT, "pond_id": "pond_empty"}
        readings = {"start": "-2h", "stop": None, "limit": 10}
        assert await projected.query_readings(**pond, **readings) == await legacy.query_readings(
            **pond, **readings
        )
        assert await projected.query_latest_metrics(**pond) == await legacy.query_latest_metrics(
            **pond
        )
        assert await projected.query_latest_metrics_for_tenant(
            tenant_id=EMPTY_TENANT
        ) == await legacy.query_latest_metrics_for_tenant(tenant_id=EMPTY_TENANT)
        assert await projected.query_summary(**pond, period="24h") == await legacy.query_summary(
            **pond, period="24h"
        )


# --- HTTP-level equality ----------------------------------------------------------------------


def _app(domain: FakeDomainRepository, telemetry_repository: Any):
    app = create_app(Settings(app_env="test", auth_mode="dev"))
    app.state.domain_repository = domain
    app.state.membership_service = FakeMembershipService()
    app.state.telemetry_repository = telemetry_repository
    return app


def _http_paths(seeded: Seeded) -> list[tuple[str, dict[str, Any]]]:
    paths: list[tuple[str, dict[str, Any]]] = [(f"/v1/tenants/{TENANT}/metrics/latest", {})]
    for pond_id in PONDS:
        base = f"/v1/tenants/{TENANT}/ponds/{pond_id}"
        paths.append((f"{base}/metrics/latest", {}))
        for period in PERIODS:
            paths.append((f"{base}/metrics/summary", {"period": period}))
    for period in ("7d", "30d"):
        paths.append(
            (f"/v1/tenants/{TENANT}/ponds/{SHARD_POND}/metrics/summary", {"period": period})
        )
    for call in _reading_calls(seeded):
        params = {"start": call["start"], "limit": call["limit"]}
        if call["stop"] is not None:
            params["stop"] = call["stop"]
        paths.append((f"/v1/tenants/{TENANT}/ponds/{call['pond_id']}/readings", params))
    return paths


def test_v1_http_responses_match_legacy(influx_bucket: str, seeded: Seeded) -> None:
    with repositories(influx_bucket, seeded.devices) as (legacy, projected, _recording, domain):
        legacy_app = _app(domain, legacy)
        projected_app = _app(domain, projected)
        headers = {"X-Dev-User-Sub": "sub_1"}
        with TestClient(legacy_app) as legacy_client, TestClient(projected_app) as client:
            # The startup hook must not replace the injected repositories.
            assert legacy_client.app.state.telemetry_repository is legacy
            assert client.app.state.telemetry_repository is projected
            assert client.app.state.domain_repository is domain
            for path, params in _http_paths(seeded):
                expected = legacy_client.get(path, params=params, headers=headers)
                actual = client.get(path, params=params, headers=headers)
                assert expected.status_code == 200, (path, expected.text)
                body = expected.json()
                assert body.get("items", body.get("series", [body])) != [], path
                if "measured_at" in body:
                    assert body["measured_at"] is not None, path
                assert actual.status_code == expected.status_code, path
                if path.endswith("/metrics/summary"):
                    assert_summaries_match(actual.json(), body, path)
                else:
                    assert actual.content == expected.content, path
