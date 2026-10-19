"""P2 acceptance gate on a real InfluxDB 2.7.11 (P2-09): gates 1, 2, 4, 6, 7, 8 and 9, and the
runbook's keyed divergence checks.

Everything is written through the merged production path: the legacy mapper, the normalizer and
the write service composed by the app lifespan with the P2 switches (or the same classes built
directly where a test needs fault injection). Data is seeded relative to a `now` captured once
per test and floored to the hour, so every summary window compared is a completed one. Every
comparison asserts its result is non-empty first.
"""

import json
import math
import random
from collections import deque
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient
from influxdb_client import InfluxDBClient
from influxdb_client.rest import ApiException
from urllib3.exceptions import ProtocolError

from limnopulse_api.adapters.influxdb import InfluxTelemetryRepository
from limnopulse_api.adapters.influxdb_v1_projection import V2ProjectedTelemetryRepository
from limnopulse_api.adapters.influxdb_v2_reader import InfluxObservationReader
from limnopulse_api.adapters.influxdb_v2_writer import InfluxObservationWriter
from limnopulse_api.api.v2.telemetry_dependencies import get_clock
from limnopulse_api.core.config import Settings
from limnopulse_api.core.errors import TelemetryWriteError
from limnopulse_api.domain.entities import Device, Membership, Pond
from limnopulse_api.domain.ids import (
    asset_id_for_legacy_pond,
    default_component_id_for_legacy_device,
)
from limnopulse_api.domain.legacy_telemetry_mapping import map_legacy_reading
from limnopulse_api.domain.roles import TenantRole
from limnopulse_api.domain.telemetry_v2 import (
    METRIC_CATALOG_VERSION,
    QUALITY_POLICY_VERSION,
    UNIT_MAPPING_VERSION,
    CanonicalObservation,
    NormalizationVersions,
    QualityFlag,
    QualityState,
    SourceObservation,
    SourceType,
    observation_event_id,
)
from limnopulse_api.domain.telemetry_v2_schema import (
    LEGACY_TAG_KEYS,
    OBSERVATION_FIELD_KEYS,
    OBSERVATION_TAG_KEYS,
)
from limnopulse_api.main import create_app
from limnopulse_api.services.telemetry_normalizer import TelemetryNormalizer
from limnopulse_api.services.telemetry_pipeline import TelemetryPipeline
from limnopulse_api.services.telemetry_v2_writes import ObservationWriteService
from tests.contracts.m1_fakes import InMemoryM1Repositories
from tests.contracts.test_p2_acceptance_gate import (
    divergence_checks,
    example_payload,
    fill_divergence_check,
    legacy_context,
    seed_legacy_device,
)
from tests.support.influx_harness import influx_clients, influx_settings, write_lines
from tests.support.telemetry_v2_vectors import scenario

TENANT = "tnt_local_001"
POND = "pond_local_001"
DEVICE = "local-device-001"
SECOND_DEVICE = "local-device-002"
COMPONENT = default_component_id_for_legacy_device(TENANT, DEVICE)
SECOND_COMPONENT = default_component_id_for_legacy_device(TENANT, SECOND_DEVICE)
ASSET = asset_id_for_legacy_pond(TENANT, POND)
NATIVE_COMPONENT = "cmp_0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f"
CREATED = datetime(2026, 1, 1, tzinfo=UTC)
HEADERS = {"X-Dev-User-Sub": "sub_1"}
ALLOWED_TAG_KEYS = frozenset(
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
EXPECTED_FIELD_KEYS = frozenset(
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
TIME_MIN = "1970-01-01T00:00:00Z"
TIME_MAX = "2262-01-01T00:00:00Z"


# --- fakes and composition ----------------------------------------------------------------------


class FakeDomainRepository:
    """The M1 `/v1` domain surface the routers and the projection read."""

    def __init__(self, devices: dict[str, str]) -> None:
        self.devices = devices  # device_id -> pond_id, for TENANT

    async def list_devices(self, tenant_id: str) -> list[Device]:
        if tenant_id != TENANT:
            return []
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
            for device_id, pond_id in sorted(self.devices.items())
        ]

    async def list_ponds(self, tenant_id: str) -> list[Pond]:
        if tenant_id != TENANT:
            return []
        return [self._pond(tenant_id, p) for p in sorted(set(self.devices.values()))]

    async def get_pond(self, tenant_id: str, pond_id: str) -> Pond | None:
        if tenant_id == TENANT and pond_id in self.devices.values():
            return self._pond(tenant_id, pond_id)
        return None

    @staticmethod
    def _pond(tenant_id: str, pond_id: str) -> Pond:
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
        if cognito_sub != "sub_1" or tenant_id != TENANT:
            return None
        return Membership(
            tenant_id=tenant_id,
            cognito_sub=cognito_sub,
            role=TenantRole.VIEWER,
            created_at=CREATED,
            updated_at=CREATED,
            version=1,
        )


class FakeRedisClient:
    async def aclose(self, close_connection_pool: object = None) -> None:
        return None


@dataclass
class World:
    """One tenant's M1 state (in memory) plus the InfluxDB bucket under test."""

    bucket: str
    repos: InMemoryM1Repositories
    domain: FakeDomainRepository
    monkeypatch: pytest.MonkeyPatch

    @contextmanager
    def app(self, **switches: object) -> Iterator[TestClient]:
        """The real app and lifespan, with the P2 switches, on this bucket; DynamoDB is faked."""
        settings = influx_settings()
        self.monkeypatch.setattr(
            "limnopulse_api.main.redis.from_url", lambda url: FakeRedisClient()
        )
        self.monkeypatch.setattr(
            "limnopulse_api.main.DynamoDomainRepository", lambda *a, **k: self.domain
        )
        for name in (
            "DynamoSiteAssetRepository",
            "DynamoDeviceComponentRepository",
            "DynamoDeploymentRepository",
        ):
            self.monkeypatch.setattr(f"limnopulse_api.main.{name}", lambda *a, **k: self.repos)
        app = create_app(
            Settings(
                _env_file=None,
                app_env="test",
                auth_mode="dev",
                influxdb_url=settings.url,
                influxdb_token=settings.token,
                influxdb_org=settings.org,
                influxdb_bucket_raw=self.bucket,
                **switches,
            )
        )
        with TestClient(app) as client:
            app.state.membership_service = FakeMembershipService()
            yield client

    def pipeline(self, client: TestClient, clock: Any) -> TelemetryPipeline:
        return TelemetryPipeline(
            normalizer=TelemetryNormalizer(
                components=self.repos, deployments=self.repos, clock=clock
            ),
            write_service=client.app.state.observation_write_service,
        )


@pytest.fixture
def world(influx_bucket: str, monkeypatch: pytest.MonkeyPatch) -> World:
    for name in (
        "API_V2_ENABLED",
        "API_V2_TELEMETRY_ENABLED",
        "TELEMETRY_V2_WRITE_ENABLED",
        "TELEMETRY_LEGACY_DUAL_WRITE_ENABLED",
        "V1_TELEMETRY_READ_SOURCE",
        "V1_POND_DEVICE_PROJECTION_ENABLED",
    ):
        monkeypatch.delenv(name, raising=False)
    return World(
        bucket=influx_bucket,
        repos=InMemoryM1Repositories(clock=lambda: datetime.now(UTC)),
        domain=FakeDomainRepository({DEVICE: POND, SECOND_DEVICE: POND}),
        monkeypatch=monkeypatch,
    )


@pytest.fixture
def control_bucket(influx_bucket: str) -> Iterator[str]:
    """A second fresh bucket (the harness fixture yields one per test)."""
    settings = influx_settings()
    client = InfluxDBClient(url=settings.url, token=settings.token, org=settings.org)
    try:
        bucket = client.buckets_api().create_bucket(bucket_name=f"{influx_bucket}_control")
        try:
            yield bucket.name
        finally:
            client.buckets_api().delete_bucket(bucket)
    finally:
        client.close()


def captured_hour() -> datetime:
    return datetime.now(UTC).replace(minute=0, second=0, microsecond=0)


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


async def seed_tenant(world: World) -> None:
    await seed_legacy_device(world.repos, tenant_id=TENANT, device_id=DEVICE, pond_id=POND)
    await seed_legacy_device(world.repos, tenant_id=TENANT, device_id=SECOND_DEVICE, pond_id=POND)


async def ingest_rehearsal(pipeline: TelemetryPipeline, now: datetime) -> dict[str, datetime]:
    """Legacy readings for two devices in the hour before `now`: the example sample, a
    sub-second sample, an out-of-range value and a 2-minute series with changing values."""
    times: dict[str, datetime] = {}
    readings: list[tuple[str, datetime, dict[str, Any]]] = []
    times["sample"] = now - timedelta(minutes=20)
    readings.append((DEVICE, times["sample"], {}))
    times["sub_second"] = now - timedelta(minutes=19) + timedelta(milliseconds=750)
    readings.append((DEVICE, times["sub_second"], {"seq": 2, "temp_c": 25.3}))
    times["out_of_range"] = now - timedelta(minutes=18)
    readings.append((DEVICE, times["out_of_range"], {"seq": 3, "do_mg_l": 75.0}))
    for index in range(12):
        at = now - timedelta(minutes=50) + timedelta(minutes=2 * index)
        for device, offset in ((DEVICE, 0.0), (SECOND_DEVICE, 0.37)):
            readings.append(
                (
                    device,
                    at + timedelta(seconds=7 if device == SECOND_DEVICE else 0),
                    {
                        "seq": 100 + index,
                        "temp_c": 24.0 + 0.1 * index + offset,
                        "ph": 7.0 + 0.01 * index,
                        "do_mg_l": 6.0 + 0.05 * index + offset,
                        "rssi": -60 - index,
                    },
                )
            )
    for device, observed_at, overrides in readings:
        context = legacy_context(TENANT, device, received_at=observed_at + timedelta(seconds=2))
        payload = example_payload(observed_at, **overrides)
        if observed_at.microsecond:
            payload["ts"] = observed_at.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        outcome = await pipeline.ingest_legacy_reading(context, payload)
        assert outcome.quarantined == ()
        assert outcome.written.v2_points == 7
        assert outcome.written.legacy_points == 1
    return times


def query_records(bucket: str, flux: str) -> list[dict[str, Any]]:
    settings = influx_settings()
    with influx_clients(bucket) as (query_api, _write_api):
        tables = query_api.query(flux, org=settings.org)
        return [dict(record.values) for table in tables for record in table.records]


def query_tables(bucket: str, flux: str) -> list[list[dict[str, Any]]]:
    settings = influx_settings()
    with influx_clients(bucket) as (query_api, _write_api):
        tables = query_api.query(flux, org=settings.org)
        return [[dict(record.values) for record in table.records] for table in tables]


def snapshot(bucket: str) -> list[tuple[Any, ...]]:
    """Every stored point field: (measurement, sorted tags, field, time, value)."""
    records = query_records(
        bucket,
        f'from(bucket: "{bucket}") |> range(start: {TIME_MIN}, stop: {TIME_MAX})',
    )
    rows = []
    for values in records:
        tags = tuple(
            sorted(
                (k, v)
                for k, v in values.items()
                if not k.startswith("_") and k not in ("result", "table")
            )
        )
        rows.append(
            (values["_measurement"], tags, values["_field"], values["_time"], values["_value"])
        )
    return sorted(rows, key=repr)


def stored_v2_fields(bucket: str, start: datetime, stop: datetime) -> dict[str, dict[str, Any]]:
    """event_id -> {field: value} of every stored `observation_numeric` point in the window."""
    records = query_records(
        bucket,
        f'from(bucket: "{bucket}") |> range(start: {_iso(start)}, stop: {_iso(stop)}) '
        '|> filter(fn: (r) => r._measurement == "observation_numeric") '
        '|> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")',
    )
    return {values["event_id"]: values for values in records}


# --- Gate 1: /v1 parity in both read modes ------------------------------------------------------


def _means_match(actual: float | None, expected: float | None) -> bool:
    if actual is None or expected is None:
        return actual is expected
    return math.isclose(actual, expected, rel_tol=1e-12, abs_tol=0.0)


def assert_summary_parity(actual: dict[str, Any], expected: dict[str, Any], now: datetime) -> int:
    """Exact on every field except the float means (rel 1e-12, r7.2), on completed windows only:
    the window holding the query's `now()` is stamped with that instant. Returns the number of
    compared windows."""
    assert actual.keys() == expected.keys()
    for key in ("tenant_id", "pond_id", "period", "interval"):
        assert actual[key] == expected[key], key

    def completed(series: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [p for p in series if datetime.fromisoformat(p["measured_at"]) <= now]

    got, want = completed(actual["series"]), completed(expected["series"])
    assert [p["measured_at"] for p in got] == [p["measured_at"] for p in want]
    for got_point, want_point in zip(got, want, strict=True):
        assert got_point.keys() == want_point.keys()
        for metric in set(want_point) - {"measured_at"}:
            assert _means_match(got_point[metric], want_point[metric]), (metric, got_point)
    assert actual["statistics"].keys() == expected["statistics"].keys()
    for metric, want_stats in expected["statistics"].items():
        got_stats = actual["statistics"][metric]
        assert got_stats.keys() == want_stats.keys()
        for key in set(want_stats) - {"mean"}:
            assert got_stats[key] == want_stats[key], (metric, key)
        assert _means_match(got_stats["mean"], want_stats["mean"]), (metric, got_stats)
    return len(want)


@pytest.mark.asyncio
async def test_gate1_v1_responses_are_identical_in_both_read_modes(world: World) -> None:
    now = captured_hour()
    await seed_tenant(world)
    write_switches = {
        "telemetry_v2_write_enabled": True,
        "telemetry_legacy_dual_write_enabled": True,
    }
    with world.app(**write_switches, v1_telemetry_read_source="legacy") as legacy:
        service = legacy.app.state.observation_write_service
        assert isinstance(service, ObservationWriteService)
        assert service.v2_write_enabled and service.legacy_dual_write_enabled
        assert isinstance(service.writer, InfluxObservationWriter)
        assert type(legacy.app.state.telemetry_repository) is InfluxTelemetryRepository
        times = await ingest_rehearsal(world.pipeline(legacy, lambda: now), now)
        await write_native(legacy, now, times)
        with world.app(v1_telemetry_read_source="v2") as projected:
            assert isinstance(
                projected.app.state.telemetry_repository, V2ProjectedTelemetryRepository
            )
            base = f"/v1/tenants/{TENANT}"
            exact = [
                (f"{base}/ponds/{POND}/readings", {"start": "-2h", "limit": 1000}),
                (f"{base}/ponds/{POND}/readings", {"start": "-2h", "limit": 5}),
                (
                    f"{base}/ponds/{POND}/readings",
                    {"start": _iso(now - timedelta(hours=1)), "stop": _iso(now), "limit": 1000},
                ),
                (f"{base}/ponds/{POND}/metrics/latest", {}),
                (f"{base}/metrics/latest", {}),
            ]
            for path, params in exact:
                expected = legacy.get(path, params=params, headers=HEADERS)
                actual = projected.get(path, params=params, headers=HEADERS)
                assert expected.status_code == 200, (path, expected.text)
                body = expected.json()
                if path.endswith("/readings"):
                    assert body["items"], path
                elif path == f"{base}/metrics/latest":
                    pond = [item for item in body["items"] if item["pond_id"] == POND]
                    assert len(pond) == 1 and pond[0]["measured_at"] is not None, body
                else:
                    assert body["measured_at"] is not None, body
                assert actual.status_code == 200, (path, actual.text)
                assert actual.content == expected.content, (path, params)

            readings = legacy.get(
                f"{base}/ponds/{POND}/readings",
                params={"start": "-2h", "limit": 1000},
                headers=HEADERS,
            ).json()["items"]
            measured = {datetime.fromisoformat(item["measured_at"]) for item in readings}
            assert times["sub_second"] in measured
            assert times["sub_second"].microsecond == 750000
            assert {item["device_id"] for item in readings} == {DEVICE, SECOND_DEVICE}
            assert 75.0 in {item["do_mg_l"] for item in readings}

            for period in ("24h", "7d"):
                params = {"period": period}
                path = f"{base}/ponds/{POND}/metrics/summary"
                expected = legacy.get(path, params=params, headers=HEADERS)
                actual = projected.get(path, params=params, headers=HEADERS)
                assert expected.status_code == actual.status_code == 200
                body = expected.json()
                assert body["statistics"]["temp_c"]["count"] > 0
                assert assert_summary_parity(actual.json(), body, now) > 0


# --- Gate 2: two probes of one metric -----------------------------------------------------------


@pytest.mark.asyncio
async def test_gate2_two_probes_are_distinct_v2_series_and_absent_from_v1(world: World) -> None:
    now = captured_hour()
    await seed_tenant(world)
    probes = scenario("two_probes_same_metric").rebased(now - timedelta(minutes=5))
    probe_time = probes.observations[0].observation.observed_at
    switches = {
        "api_v2_enabled": True,
        "api_v2_telemetry_enabled": True,
        "telemetry_v2_write_enabled": True,
        "telemetry_legacy_dual_write_enabled": True,
    }
    with world.app(**switches) as legacy:
        assert isinstance(legacy.app.state.observation_reader, InfluxObservationReader)
        await ingest_rehearsal(world.pipeline(legacy, lambda: now), now)
        outcome = await legacy.app.state.observation_write_service.write(probes.observations)
        assert (outcome.v2_points, outcome.legacy_points, outcome.legacy_excluded) == (2, 0, 2)
        legacy.app.dependency_overrides[get_clock] = lambda: lambda: now
        base = f"/v2/tenants/{TENANT}"
        listed = legacy.get(
            f"{base}/observations",
            params={
                "asset_id": ASSET,
                "metric_key": "water.dissolved_oxygen",
                "start": _iso(probe_time),
                "stop": _iso(probe_time + timedelta(microseconds=1)),
            },
            headers=HEADERS,
        )
        latest = legacy.get(
            f"{base}/observations/latest",
            params={"asset_id": ASSET, "metric_key": "water.dissolved_oxygen"},
            headers=HEADERS,
        )
        with world.app(v1_telemetry_read_source="v2") as projected:
            v1 = [
                (
                    client.get(
                        f"/v1/tenants/{TENANT}/ponds/{POND}/readings", params=p, headers=HEADERS
                    )
                )
                for client in (legacy, projected)
                for p in ({"start": "-2h", "limit": 1000},)
            ]

    assert listed.status_code == 200, listed.text
    items = listed.json()["items"]
    assert len(items) == 2
    assert {item["component_id"] for item in items} == set(probes.components)
    assert len({item["probe_id"] for item in items}) == 2
    assert {item["metric_key"] for item in items} == {"water.dissolved_oxygen"}
    assert {datetime.fromisoformat(item["observed_at"]) for item in items} == {probe_time}
    assert sorted(item["value"] for item in items) == [6.4, 6.6]
    assert latest.status_code == 200, latest.text
    latest_probes = [i for i in latest.json()["items"] if i["component_id"] in probes.components]
    assert len(latest_probes) == 2
    # /v1 in both read modes: non-empty, identical, and without the native probes.
    legacy_v1, projected_v1 = v1
    assert legacy_v1.status_code == projected_v1.status_code == 200
    assert projected_v1.content == legacy_v1.content
    rows = legacy_v1.json()["items"]
    assert rows
    assert not {probes.devices[0]} & {row["device_id"] for row in rows}
    assert probe_time not in {datetime.fromisoformat(row["measured_at"]) for row in rows}


# --- Gate 4: idempotent writes ------------------------------------------------------------------


def _reclassified(observation: CanonicalObservation, ingested_at: datetime) -> CanonicalObservation:
    data = observation.model_dump()
    data["quality"] = {
        "state": QualityState.OUT_OF_RANGE,
        "flags": [],
        "confidence": None,
        "provenance": ["schema_validation", "unit_mapping", "plausible_range"],
    }
    data["delivery"] = {"ingested_at": ingested_at, "delayed": False, "replay": False}
    return CanonicalObservation.model_validate(data)


@pytest.mark.asyncio
async def test_gate4_duplicate_replay_reclassified_and_shuffled_writes_are_idempotent(
    world: World,
) -> None:
    now = captured_hour()
    await seed_tenant(world)
    normalizer = TelemetryNormalizer(
        components=world.repos, deployments=world.repos, clock=lambda: now
    )
    observed = [now - timedelta(minutes=30) + timedelta(minutes=5 * i) for i in range(3)]
    original: list[CanonicalObservation] = []
    replayed: list[CanonicalObservation] = []
    for index, observed_at in enumerate(observed):
        received_at = observed_at + timedelta(seconds=2)
        mapped = map_legacy_reading(
            legacy_context(TENANT, DEVICE, received_at=received_at),
            example_payload(observed_at, seq=index),
        )
        first = await normalizer.normalize(
            legacy_context(TENANT, DEVICE, received_at=received_at), mapped.observations
        )
        replay_context = legacy_context(TENANT, DEVICE, received_at=received_at, replay=True)
        replay = await TelemetryNormalizer(
            components=world.repos,
            deployments=world.repos,
            clock=lambda: now + timedelta(minutes=1),
        ).normalize(
            replay_context.model_copy(update={"mapping_versions": NormalizationVersions()}),
            mapped.observations,
        )
        original += first.accepted
        replayed += replay.accepted
    assert len(original) == 21
    assert [o.event_id for o in replayed] == [o.event_id for o in original]
    assert all(QualityFlag.REPLAY in o.quality.flags for o in replayed)
    shuffled = list(original)
    random.Random(2209).shuffle(shuffled)
    reclassified = [_reclassified(o, now + timedelta(minutes=2)) for o in original[:7]]

    with world.app(
        telemetry_v2_write_enabled=True, telemetry_legacy_dual_write_enabled=True
    ) as app:
        service = app.app.state.observation_write_service
        for batch in (
            original,
            original,  # duplicate
            replayed,  # replay after the fact, first-receipt context
            shuffled[:11],
            shuffled[11:],
            list(reversed(original)),
            reclassified,  # last writer wins per field
        ):
            await service.write(batch)

    start, stop = now - timedelta(hours=1), now
    value_tables = query_tables(
        world.bucket,
        f'from(bucket: "{world.bucket}") |> range(start: {_iso(start)}, stop: {_iso(stop)}) '
        '|> filter(fn: (r) => r._measurement == "observation_numeric" and r._field == "value")',
    )
    # One series per metric of the one Component: nothing forked a series.
    assert len(value_tables) == 7
    assert sum(len(table) for table in value_tables) == 21
    stored = stored_v2_fields(world.bucket, start, stop)
    assert set(stored) == {o.event_id for o in original}
    for observation in original:
        point = stored[observation.event_id]
        reclassified_now = observation.event_id in {o.event_id for o in reclassified}
        assert point["quality_state"] == ("out_of_range" if reclassified_now else "valid")
        assert point["quality_flags_json"] == "[]"
        assert point["source_type"] == SourceType.LEGACY_MQTT
    legacy_rows = query_records(
        world.bucket,
        f'from(bucket: "{world.bucket}") |> range(start: {_iso(start)}, stop: {_iso(stop)}) '
        '|> filter(fn: (r) => r._measurement == "water_quality" and r._field == "temp_c")',
    )
    assert sorted(row["_time"] for row in legacy_rows) == observed


# --- Gates 6 and 8: conversion, versions, out-of-range, stored ----------------------------------


@pytest.mark.asyncio
async def test_gate6_and_gate8_stored_points_keep_conversion_versions_and_quality(
    world: World,
) -> None:
    now = captured_hour()
    await seed_tenant(world)
    observed_at = now - timedelta(minutes=10)
    sources = [
        SourceObservation(
            component_id=COMPONENT,
            metric_key=metric_key,
            value=value,
            source_unit=unit,
            observed_at=observed_at,
            sequence=9,
        )
        for metric_key, value, unit in (
            ("water.temperature", 77.0, "degF"),
            ("water.dissolved_oxygen", 75.0, "mg/L"),
            ("device.battery_voltage", 3820, "mV"),
        )
    ]
    with world.app(
        telemetry_v2_write_enabled=True, telemetry_legacy_dual_write_enabled=True
    ) as app:
        outcome = await world.pipeline(app, lambda: now).ingest(
            legacy_context(TENANT, DEVICE, received_at=observed_at + timedelta(seconds=1)), sources
        )
    assert outcome.quarantined == ()

    stored = stored_v2_fields(world.bucket, now - timedelta(hours=1), now)
    by_metric = {
        values["metric_key"]: values
        for values in query_records(
            world.bucket,
            f'from(bucket: "{world.bucket}") '
            f"|> range(start: {_iso(now - timedelta(hours=1))}, stop: {_iso(now)}) "
            '|> filter(fn: (r) => r._measurement == "observation_numeric") '
            '|> pivot(rowKey: ["_time", "metric_key"], columnKey: ["_field"], valueColumn: "_value")',
        )
    }
    assert len(stored) == 3 and set(by_metric) == {
        "water.temperature",
        "water.dissolved_oxygen",
        "device.battery_voltage",
    }
    expected = {
        "water.temperature": (25.0, "degF", "valid"),
        "water.dissolved_oxygen": (75.0, "mg/L", "out_of_range"),
        "device.battery_voltage": (3.82, "mV", "valid"),
    }
    for metric_key, (value, unit, state) in expected.items():
        point = by_metric[metric_key]
        assert math.isclose(point["value"], value, rel_tol=1e-12, abs_tol=1e-12), metric_key
        assert point["source_unit"] == unit
        assert point["catalog_version"] == METRIC_CATALOG_VERSION
        assert point["unit_mapping_version"] == UNIT_MAPPING_VERSION
        assert point["quality_policy_version"] == QUALITY_POLICY_VERSION
        assert point["quality_state"] == state
        provenance = json.loads(point["quality_provenance_json"])
        assert ("plausible_range" in provenance) is (state == "out_of_range"), metric_key
    legacy = query_records(
        world.bucket,
        f'from(bucket: "{world.bucket}") '
        f"|> range(start: {_iso(now - timedelta(hours=1))}, stop: {_iso(now)}) "
        '|> filter(fn: (r) => r._measurement == "water_quality")',
    )
    fields = {row["_field"]: row["_value"] for row in legacy}
    assert fields["do_mg_l"] == 75.0  # D15: legacy reads still show it
    assert math.isclose(fields["temp_c"], 25.0, rel_tol=1e-12, abs_tol=1e-12)


# --- Gate 7: cardinality ------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gate7_tag_keys_are_the_allowlist_and_no_identity_or_quality_is_a_tag(
    world: World,
) -> None:
    now = captured_hour()
    await seed_tenant(world)
    oldest = now - timedelta(minutes=40)
    sources = [
        SourceObservation(
            component_id=COMPONENT,
            metric_key="water.dissolved_oxygen",
            value=value,
            source_unit="mg/L",
            observed_at=oldest + timedelta(minutes=index),
            sequence=index,
            source_event_id=f"evt-gate-{index:04d}",
            source_metadata={"firmware_version": f"fw-gate-{index}"},
        )
        for index, value in enumerate((6.4, 75.0, 6.6))
    ]
    integration_id = "di_" + "e" * 32
    probes = scenario("two_probes_same_metric").rebased(now - timedelta(minutes=5))
    with world.app(
        telemetry_v2_write_enabled=True, telemetry_legacy_dual_write_enabled=True
    ) as app:
        pipeline = world.pipeline(app, lambda: now)
        outcome = await pipeline.ingest(
            legacy_context(
                TENANT,
                DEVICE,
                received_at=now - timedelta(minutes=1),
                source_type=SourceType.HTTPS,
                integration_id=integration_id,
                replay=True,
            ),
            sources,
        )
        assert outcome.quarantined == ()
        await app.app.state.observation_write_service.write(probes.observations)

    start = _iso(oldest - timedelta(hours=1))
    predicate = '(r) => r._measurement == "observation_numeric"'
    schema = 'import "influxdata/influxdb/schema"\n'
    tag_keys = {
        row["_value"]
        for row in query_records(
            world.bucket,
            schema
            + f'schema.tagKeys(bucket: "{world.bucket}", predicate: {predicate}, start: {start})',
        )
    }
    field_keys = {
        row["_value"]
        for row in query_records(
            world.bucket,
            schema
            + f'schema.fieldKeys(bucket: "{world.bucket}", predicate: {predicate}, start: {start})',
        )
    }
    legacy_tag_keys = {
        row["_value"]
        for row in query_records(
            world.bucket,
            schema + f'schema.tagKeys(bucket: "{world.bucket}", '
            f'predicate: (r) => r._measurement == "water_quality", start: {start})',
        )
    }
    # Literal sets (plan "InfluxDB observation_numeric schema"), and the frozen constants.
    assert {key for key in tag_keys if not key.startswith("_")} == ALLOWED_TAG_KEYS
    assert ALLOWED_TAG_KEYS == OBSERVATION_TAG_KEYS
    assert field_keys == EXPECTED_FIELD_KEYS == OBSERVATION_FIELD_KEYS
    assert not field_keys & tag_keys
    assert {key for key in legacy_tag_keys if not key.startswith("_")} == LEGACY_TAG_KEYS

    stored = stored_v2_fields(world.bucket, oldest - timedelta(hours=1), now)
    assert len(stored) == 5
    forbidden = (
        set(stored)  # event IDs
        | {f"evt-gate-{i:04d}" for i in range(3)}
        | {f"fw-gate-{i}" for i in range(3)}
        | {integration_id, "di_" + "d1" * 16}
        | {state.value for state in QualityState}
        | {flag.value for flag in QualityFlag}
        | {source_type.value for source_type in SourceType}
        | {METRIC_CATALOG_VERSION, UNIT_MAPPING_VERSION, QUALITY_POLICY_VERSION}
    )
    assert {values["source_event_id"] for values in stored.values()} >= {"evt-gate-0000"}
    assert {values["integration_id"] for values in stored.values()} >= {integration_id}
    for key in OBSERVATION_TAG_KEYS:
        values = {
            row["_value"]
            for row in query_records(
                world.bucket,
                schema + f'schema.tagValues(bucket: "{world.bucket}", tag: "{key}", '
                f"predicate: {predicate}, start: {start})",
            )
        }
        assert values, key
        assert not values & forbidden, (key, values & forbidden)


# --- Gate 9: InfluxDB outage after acceptance ---------------------------------------------------


class FlakyWriteApi:
    """Wraps the real synchronous `WriteApi`; the listed call numbers (1-based) fail, with a
    transport error, or with HTTP 422 when `permanent`."""

    def __init__(self, write_api: Any, failing_calls: set[int], *, permanent: bool = False) -> None:
        self.write_api = write_api
        self.failing_calls = failing_calls
        self.permanent = permanent
        self.calls = 0

    def write(self, **kwargs: Any) -> None:
        self.calls += 1
        if self.calls in self.failing_calls:
            if self.permanent:
                raise ApiException(status=422, reason="Unprocessable Entity")
            if self.calls % 2:
                raise ApiException(status=503, reason="Service Unavailable")
            raise ProtocolError("connection reset by peer")
        self.write_api.write(**kwargs)


@dataclass
class Message:
    message_id: str
    device_id: str
    payload: dict[str, Any]
    received_at: datetime


class QueueFake:
    """At-least-once delivery: the head message is redelivered until it is acknowledged. A
    non-retryable failure stops the consumer with the message still pending (never acked)."""

    def __init__(self, messages: Sequence[Message]) -> None:
        self.pending = deque(messages)
        self.acked: list[str] = []
        self.deliveries: list[str] = []
        self.stopped_on: str | None = None

    async def drain(self, pipeline: TelemetryPipeline, *, max_deliveries: int = 20) -> list[str]:
        failures = []
        while self.pending and len(self.deliveries) < max_deliveries:
            message = self.pending[0]
            self.deliveries.append(message.message_id)
            # A redelivery carries the first-receipt context unchanged (D5).
            context = legacy_context(TENANT, message.device_id, received_at=message.received_at)
            try:
                outcome = await pipeline.ingest_legacy_reading(context, message.payload)
            except TelemetryWriteError as exc:
                assert message.message_id not in self.acked
                failures.append(message.message_id)
                if not exc.retryable:
                    self.stopped_on = message.message_id
                    break
                continue
            assert outcome.quarantined == ()
            self.acked.append(message.message_id)
            self.pending.popleft()
        return failures


def _messages(now: datetime) -> list[Message]:
    messages = []
    for index, device_id in enumerate((DEVICE, SECOND_DEVICE, DEVICE)):
        observed_at = now - timedelta(minutes=30 - index)
        messages.append(
            Message(
                message_id=f"m{index}",
                device_id=device_id,
                payload=example_payload(observed_at, seq=index, temp_c=24.0 + index),
                received_at=observed_at + timedelta(seconds=2),
            )
        )
    return messages


async def _drive(
    world: World,
    bucket: str,
    now: datetime,
    failing: set[int],
    *,
    permanent: bool = False,
    queue: QueueFake | None = None,
) -> tuple[QueueFake, list[str]]:
    settings = influx_settings()
    queue = queue or QueueFake(_messages(now))
    with influx_clients(bucket) as (_query_api, write_api):
        flaky = FlakyWriteApi(write_api, failing, permanent=permanent)
        writer = InfluxObservationWriter(write_api=flaky, bucket=bucket, org=settings.org)
        pipeline = TelemetryPipeline(
            normalizer=TelemetryNormalizer(
                components=world.repos, deployments=world.repos, clock=lambda: now
            ),
            write_service=ObservationWriteService(
                writer=writer,
                legacy_writer=writer,
                site_assets=world.repos,
                v2_write_enabled=True,
                legacy_dual_write_enabled=True,
            ),
        )
        failures = await queue.drain(pipeline)
    return queue, failures


@pytest.mark.parametrize(
    "failing",
    [
        {1, 2, 3},  # the first N calls: v2 writes fail before anything is stored
        {2, 5},  # after a stored v2 write, the legacy write fails (partial), then a v2 write
    ],
    ids=["first-n-calls", "partial-write"],
)
@pytest.mark.asyncio
async def test_gate9_outage_never_acknowledges_and_redelivery_recovers_the_exact_points(
    world: World, control_bucket: str, failing: set[int]
) -> None:
    now = captured_hour()
    await seed_tenant(world)

    control, control_failures = await _drive(world, control_bucket, now, set())
    queue, failures = await _drive(world, world.bucket, now, failing)

    assert control_failures == []
    assert control.acked == ["m0", "m1", "m2"]
    assert queue.stopped_on is None
    assert len(failures) == len(failing)
    assert queue.acked == ["m0", "m1", "m2"]
    assert len(queue.deliveries) == 3 + len(failing)
    expected = snapshot(control_bucket)
    assert expected
    assert {row[0] for row in expected} == {"observation_numeric", "water_quality"}
    assert sum(row[0] == "observation_numeric" for row in expected) == 3 * 7 * len(
        OBSERVATION_FIELD_KEYS
    )
    assert snapshot(world.bucket) == expected


@pytest.mark.asyncio
async def test_gate9_permanent_write_error_is_never_acknowledged(
    world: World, control_bucket: str
) -> None:
    now = captured_hour()
    await seed_tenant(world)

    control, _ = await _drive(world, control_bucket, now, set())
    # HTTP 422 on the second call: m0's v2 write is stored, its legacy write is rejected.
    queue, failures = await _drive(world, world.bucket, now, {2}, permanent=True)

    assert failures == ["m0"]
    assert queue.stopped_on == "m0"
    assert queue.acked == []
    assert [m.message_id for m in queue.pending] == ["m0", "m1", "m2"]
    stored = snapshot(world.bucket)
    assert stored and {row[0] for row in stored} == {"observation_numeric"}
    # Once the cause is fixed, redriving the unacknowledged messages recovers the exact set.
    queue, failures = await _drive(world, world.bucket, now, set(), queue=queue)
    assert failures == [] and queue.acked == ["m0", "m1", "m2"]
    expected = snapshot(control_bucket)
    assert expected and control.acked == ["m0", "m1", "m2"]
    assert snapshot(world.bucket) == expected


# --- Runbook keyed divergence checks ------------------------------------------------------------


@dataclass(frozen=True)
class Rehearsal:
    now: datetime
    times: dict[str, datetime]


async def write_native(client: TestClient, now: datetime, times: dict[str, datetime]) -> None:
    """Native probes on the pond Asset, and a native Component on a legacy device at the time of
    a legacy reading: v2 only, never visible through `/v1`."""
    probes = scenario("two_probes_same_metric").rebased(now - timedelta(minutes=3))
    template = probes.observations[0].model_dump()
    template["resolved"].update(device_id=DEVICE, component_id=NATIVE_COMPONENT)
    template["event_id"] = observation_event_id(
        TENANT, NATIVE_COMPONENT, "water.dissolved_oxygen", times["sample"]
    )
    template["observation"]["observed_at"] = times["sample"]
    native = CanonicalObservation.model_validate(template)
    outcome = await client.app.state.observation_write_service.write((*probes.observations, native))
    assert (outcome.v2_points, outcome.legacy_points) == (3, 0)


async def _rehearse(world: World) -> Rehearsal:
    now = captured_hour()
    await seed_tenant(world)
    with world.app(
        telemetry_v2_write_enabled=True, telemetry_legacy_dual_write_enabled=True
    ) as app:
        times = await ingest_rehearsal(world.pipeline(app, lambda: now), now)
        await write_native(app, now, times)
    return Rehearsal(now=now, times=times)


def run_divergence_check(world: World, name: str, now: datetime) -> list[dict[str, Any]]:
    query = fill_divergence_check(
        divergence_checks()[name],
        bucket=world.bucket,
        tenant_id=TENANT,
        pond_id=POND,
        asset_id=ASSET,
        component_ids=(COMPONENT, SECOND_COMPONENT),
        start=now - timedelta(hours=2),
        stop=now,
    )
    return query_records(world.bucket, query)


def _legacy_line(device_id: str, fields: str, at: datetime) -> str:
    ns = (at - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1) * 1000
    return (
        f"water_quality,device_id={device_id},pond_id={POND},schema_version=1,source=mqtt,"
        f"tenant_id={TENANT},topic=limnopulse/v1/devices/{device_id}/readings {fields} {ns}"
    )


@pytest.mark.asyncio
async def test_runbook_divergence_checks_report_nothing_after_dual_write(world: World) -> None:
    rehearsal = await _rehearse(world)
    legacy_rows = query_records(
        world.bucket,
        f'from(bucket: "{world.bucket}") '
        f"|> range(start: {_iso(rehearsal.now - timedelta(hours=2))}, stop: {_iso(rehearsal.now)}) "
        '|> filter(fn: (r) => r._measurement == "water_quality")',
    )
    assert len(legacy_rows) > 50

    for name in ("points", "latest", "summary"):
        assert run_divergence_check(world, name, rehearsal.now) == [], name


@pytest.mark.asyncio
async def test_runbook_divergence_checks_detect_a_changed_legacy_value(world: World) -> None:
    rehearsal = await _rehearse(world)
    # DEVICE's newest reading is the out-of-range one (it carries every example field).
    # Overwrite its `temp_c`, as a divergent producer would.
    newest = rehearsal.times["out_of_range"]
    write_lines(world.bucket, [_legacy_line(DEVICE, "temp_c=99.5", newest)])

    points = run_divergence_check(world, "points", rehearsal.now)
    latest = run_divergence_check(world, "latest", rehearsal.now)
    summary = run_divergence_check(world, "summary", rehearsal.now)

    assert [(row["_time"], row["device_id"], row["field"]) for row in points] == [
        (newest, DEVICE, "temp_c")
    ]
    assert [(row["device_id"], row["field"]) for row in latest] == [(DEVICE, "temp_c")]
    assert [row["field"] for row in summary] == ["temp_c"]


@pytest.mark.asyncio
async def test_runbook_divergence_checks_detect_a_legacy_point_missing_from_v2(
    world: World,
) -> None:
    rehearsal = await _rehearse(world)
    telegraf_only = rehearsal.now - timedelta(minutes=1)
    # A reading only Telegraf wrote: v2 has no tuple for it.
    write_lines(world.bucket, [_legacy_line(SECOND_DEVICE, "ph=7.4,seq=900i", telegraf_only)])

    points = run_divergence_check(world, "points", rehearsal.now)
    latest = run_divergence_check(world, "latest", rehearsal.now)
    summary = run_divergence_check(world, "summary", rehearsal.now)

    assert {(row["_time"], row["device_id"], row["field"]) for row in points} == {
        (telegraf_only, SECOND_DEVICE, "ph"),
        (telegraf_only, SECOND_DEVICE, "seq"),
    }
    assert [(row["device_id"], row["field"]) for row in latest] == [(SECOND_DEVICE, "ph")]
    assert [row["field"] for row in summary] == ["ph"]


@pytest.mark.asyncio
async def test_runbook_points_check_detects_a_changed_seq(world: World) -> None:
    rehearsal = await _rehearse(world)
    sample = rehearsal.times["sample"]
    write_lines(world.bucket, [_legacy_line(DEVICE, "seq=4242i", sample)])

    points = run_divergence_check(world, "points", rehearsal.now)

    assert points
    assert {(row["_time"], row["device_id"], row["field"]) for row in points} == {
        (sample, DEVICE, "seq")
    }


async def _write_v2_only(world: World, observed_at: datetime, now: datetime) -> None:
    """One default-Component reading written to `observation_numeric` only (legacy dual-write
    off), as a producer that drops the legacy write would."""
    context = legacy_context(TENANT, DEVICE, received_at=observed_at + timedelta(seconds=2))
    mapped = map_legacy_reading(context, example_payload(observed_at, seq=777))
    normalized = await TelemetryNormalizer(
        components=world.repos, deployments=world.repos, clock=lambda: now
    ).normalize(context, mapped.observations)
    assert len(normalized.accepted) == 7
    settings = influx_settings()
    with influx_clients(world.bucket) as (_query_api, write_api):
        writer = InfluxObservationWriter(write_api=write_api, bucket=world.bucket, org=settings.org)
        service = ObservationWriteService(
            writer=writer,
            legacy_writer=None,
            site_assets=world.repos,
            v2_write_enabled=True,
            legacy_dual_write_enabled=False,
        )
        outcome = await service.write(normalized.accepted)
    assert (outcome.v2_points, outcome.legacy_points) == (7, 0)


@pytest.mark.asyncio
async def test_runbook_divergence_checks_detect_a_v2_point_missing_from_legacy(
    world: World,
) -> None:
    rehearsal = await _rehearse(world)
    v2_only = rehearsal.now - timedelta(minutes=1)
    await _write_v2_only(world, v2_only, rehearsal.now)

    points = run_divergence_check(world, "points", rehearsal.now)
    latest = run_divergence_check(world, "latest", rehearsal.now)
    summary = run_divergence_check(world, "summary", rehearsal.now)

    fields = {"temp_c", "ph", "do_mg_l", "turbidity_ntu", "salinity_ppt", "battery_v", "rssi"}
    assert {(row["_time"], row["device_id"], row["field"]) for row in points} == {
        (v2_only, DEVICE, field) for field in fields | {"seq"}
    }
    assert {(row["device_id"], row["field"]) for row in latest} == {
        (DEVICE, field) for field in fields
    }
    assert sorted(row["field"] for row in summary) == ["do_mg_l", "ph", "temp_c"]


@pytest.mark.asyncio
async def test_runbook_summary_check_uses_the_relative_mean_tolerance(world: World) -> None:
    rehearsal = await _rehearse(world)
    # A series point of DEVICE that is neither the newest nor a temp_c min or max.
    at = rehearsal.now - timedelta(minutes=50) + timedelta(minutes=10)
    write_lines(world.bucket, [_legacy_line(DEVICE, f"temp_c={24.5 * (1 + 1e-13)!r}", at)])

    points = run_divergence_check(world, "points", rehearsal.now)
    latest = run_divergence_check(world, "latest", rehearsal.now)
    summary = run_divergence_check(world, "summary", rehearsal.now)

    # The keyed point check is exact; the summary means compare within rel 1e-12 (r7.2).
    assert [(row["_time"], row["device_id"], row["field"]) for row in points] == [
        (at, DEVICE, "temp_c")
    ]
    assert latest == []
    assert summary == []


@pytest.mark.asyncio
async def test_runbook_summary_check_compares_counts_not_only_means(world: World) -> None:
    rehearsal = await _rehearse(world)
    start = rehearsal.now - timedelta(hours=2)
    values = [
        row["_value"]
        for row in query_records(
            world.bucket,
            f'from(bucket: "{world.bucket}") '
            f"|> range(start: {_iso(start)}, stop: {_iso(rehearsal.now)}) "
            '|> filter(fn: (r) => r._measurement == "water_quality" and r._field == "temp_c")',
        )
    ]
    assert len(values) > 20
    mean = sum(values) / len(values)
    # A legacy-only point at the mean: the count changes, the mean, min and max do not.
    write_lines(
        world.bucket,
        [_legacy_line(SECOND_DEVICE, f"temp_c={mean!r}", rehearsal.now - timedelta(minutes=1))],
    )

    summary = run_divergence_check(world, "summary", rehearsal.now)

    assert [row["field"] for row in summary] == ["temp_c"]


@pytest.mark.asyncio
async def test_runbook_summary_check_reports_a_mean_only_divergence(world: World) -> None:
    rehearsal = await _rehearse(world)
    # A series point of DEVICE that is neither the newest nor a temp_c min or max: shifting it
    # by +0.01 changes only the mean (count, min and max stay equal).
    at = rehearsal.now - timedelta(minutes=50) + timedelta(minutes=10)
    write_lines(world.bucket, [_legacy_line(DEVICE, "temp_c=24.51", at)])

    points = run_divergence_check(world, "points", rehearsal.now)
    latest = run_divergence_check(world, "latest", rehearsal.now)
    summary = run_divergence_check(world, "summary", rehearsal.now)

    assert [(row["_time"], row["device_id"], row["field"]) for row in points] == [
        (at, DEVICE, "temp_c")
    ]
    assert latest == []
    assert [row["field"] for row in summary] == ["temp_c"]
