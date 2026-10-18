"""Real-InfluxDB integration tests for the v2 writer and dual-write service (P2-03)."""

import ast
import json
import random
import socket
import tomllib
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

from limnopulse_api.adapters.influxdb import InfluxTelemetryRepository
from limnopulse_api.adapters.influxdb_v2_writer import InfluxObservationWriter
from limnopulse_api.core.errors import TelemetryWriteError
from limnopulse_api.domain.assets import Asset, AssetType, AssetWithPondProfile, PondProfile
from limnopulse_api.domain.telemetry_v2 import (
    CanonicalObservation,
    QualityProvenance,
    QualityState,
)
from limnopulse_api.domain.telemetry_v2_schema import (
    LEGACY_MEASUREMENT,
    OBSERVATION_FIELD_KEYS,
    OBSERVATION_MEASUREMENT,
    build_legacy_points,
    flux_string_literal,
    observation_tags,
)
from limnopulse_api.services.telemetry_v2_writes import ObservationWriteService
from tests.support.influx_harness import influx_clients, influx_settings, write_lines
from tests.support.telemetry_v2_vectors import TelemetryScenario, load_vectors, scenario

ROOT = Path(__file__).resolve().parents[2]
TELEGRAF_CONF = ROOT / "infra/telegraf/telegraf.conf"
DEVICE_REGISTRY = ROOT / "infra/telegraf/device_registry.star"
READING_EXAMPLE = ROOT / "examples/telemetry/reading.local.json"
READINGS_TOPIC = "limnopulse/v1/devices/+/readings"
RANGE_START = "2026-07-09T00:00:00Z"
RANGE_STOP = "2026-07-10T00:00:00Z"
CREATED = datetime(2026, 1, 1, tzinfo=UTC)
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MICROSECOND = timedelta(microseconds=1)

PointKey = tuple[frozenset[tuple[str, str]], datetime]


# --- fakes and helpers ------------------------------------------------------------------------


class InMemorySiteAssets:
    def __init__(self, assets: dict[tuple[str, str], AssetWithPondProfile]) -> None:
        self.assets = assets

    async def get_site(self, tenant_id: str, site_id: str) -> None:
        raise AssertionError("the write service must not read Sites")

    async def get_asset(self, tenant_id: str, asset_id: str) -> AssetWithPondProfile | None:
        return self.assets.get((tenant_id, asset_id))


def site_assets_for(scenarios: Iterable[TelemetryScenario]) -> InMemorySiteAssets:
    assets: dict[tuple[str, str], AssetWithPondProfile] = {}
    for item in scenarios:
        for o in item.observations:
            r = o.resolved
            assets[(r.tenant_id, r.asset_id)] = AssetWithPondProfile(
                asset=Asset(
                    tenant_id=r.tenant_id,
                    asset_id=r.asset_id,
                    site_id=r.site_id,
                    asset_type=AssetType.POND,
                    name="Pond",
                    created_at=CREATED,
                    updated_at=CREATED,
                    version=1,
                ),
                pond_profile=PondProfile(
                    tenant_id=r.tenant_id,
                    asset_id=r.asset_id,
                    legacy_pond_id=item.legacy_pond_by_asset.get(r.asset_id),
                    created_at=CREATED,
                    updated_at=CREATED,
                    version=1,
                ),
            )
    return InMemorySiteAssets(assets)


def dual_write_service(
    writer: InfluxObservationWriter, site_assets: InMemorySiteAssets
) -> ObservationWriteService:
    return ObservationWriteService(
        writer=writer,
        legacy_writer=writer,
        site_assets=site_assets,
        v2_write_enabled=True,
        legacy_dual_write_enabled=True,
    )


def stored_points(query_api, bucket: str, measurement: str) -> dict[PointKey, dict[str, object]]:
    flux = (
        f"from(bucket: {flux_string_literal(bucket)})"
        f" |> range(start: {RANGE_START}, stop: {RANGE_STOP})"
        f" |> filter(fn: (r) => r._measurement == {flux_string_literal(measurement)})"
    )
    points: dict[PointKey, dict[str, object]] = {}
    for table in query_api.query(flux, org=influx_settings().org):
        for record in table.records:
            tags = frozenset(
                (key, str(value))
                for key, value in record.values.items()
                if not key.startswith("_") and key not in {"result", "table"}
            )
            fields = points.setdefault((tags, record.get_time()), {})
            assert record.get_field() not in fields
            value = record.get_value()
            # The client's CSV decoder returns an empty string field (`ABSENT_STRING`) as None.
            fields[record.get_field()] = "" if value is None else value
    return points


def snapshot(query_api, bucket: str) -> dict[str, dict[PointKey, dict[str, object]]]:
    return {
        measurement: stored_points(query_api, bucket, measurement)
        for measurement in (OBSERVATION_MEASUREMENT, LEGACY_MEASUREMENT)
    }


def _micros(value: datetime) -> int:
    return (value - _EPOCH) // _MICROSECOND


def expected_fields(o: CanonicalObservation) -> dict[str, object]:
    """The complete `observation_numeric` field set, derived from the plan's field table."""
    observed_us = _micros(o.observation.observed_at)
    received_us = _micros(o.source.received_at)
    sequence = o.observation.sequence
    return {
        "catalog_version": o.mapping.catalog_version,
        "delay_ms": (received_us - observed_us) // 1000,
        "event_id": o.event_id,
        "ingested_at_ns": _micros(o.delivery.ingested_at) * 1000,
        "integration_id": o.source.integration_id or "",
        "quality_flags_json": json.dumps([str(f) for f in o.quality.flags], separators=(",", ":")),
        "quality_policy_version": o.mapping.quality_policy_version,
        "quality_provenance_json": json.dumps(
            [str(p) for p in o.quality.provenance], separators=(",", ":")
        ),
        "quality_state": str(o.quality.state),
        "received_at_ns": received_us * 1000,
        "sequence": -1 if sequence is None else sequence,
        "source_event_id": o.source_event_id or "",
        "source_type": str(o.source.source_type),
        "source_unit": o.observation.source_unit,
        "unit_mapping_version": o.mapping.unit_mapping_version,
        "value": float(o.observation.value),
    }


def revise(o: CanonicalObservation, mutate) -> CanonicalObservation:
    data = o.model_dump()
    mutate(data)
    return CanonicalObservation.model_validate(data)


# --- idempotency ------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_same_batch_twice_and_shuffled_yields_an_identical_point_set(
    influx_bucket: str,
) -> None:
    scenarios = [scenario(name) for name in load_vectors()["scenarios"]]
    observations = [o for item in scenarios for o in item.observations]
    v2_count = sum(len(item.v2_lines) for item in scenarios)
    legacy_count = sum(len(item.legacy_lines) for item in scenarios)
    site_assets = site_assets_for(scenarios)

    with influx_clients(influx_bucket) as (query_api, write_api):
        writer = InfluxObservationWriter(
            write_api=write_api, bucket=influx_bucket, org=influx_settings().org
        )
        service = dual_write_service(writer, site_assets)

        outcome = await service.write(observations)
        first = snapshot(query_api, influx_bucket)

        assert (outcome.v2_points, outcome.legacy_points) == (v2_count, legacy_count)
        assert len(first[OBSERVATION_MEASUREMENT]) == v2_count > 0
        assert len(first[LEGACY_MEASUREMENT]) == legacy_count > 0
        for fields in first[OBSERVATION_MEASUREMENT].values():
            assert set(fields) == OBSERVATION_FIELD_KEYS
        assert {f["event_id"] for f in first[OBSERVATION_MEASUREMENT].values()} == {
            o.event_id for o in observations
        }

        await service.write(observations)
        assert snapshot(query_api, influx_bucket) == first

        shuffled = list(observations)
        random.Random(20261018).shuffle(shuffled)
        assert shuffled != observations
        await service.write(shuffled)
        assert snapshot(query_api, influx_bucket) == first


@pytest.mark.asyncio
async def test_reclassified_redelivery_overwrites_every_field_of_the_same_point(
    influx_bucket: str,
) -> None:
    sample = scenario("legacy_sample")

    def first_receipt(data: dict) -> None:
        data["observation"]["sequence"] = 5
        data["source_event_id"] = "msg-0005"

    def corrected(data: dict) -> None:
        data["observation"]["sequence"] = None
        data["source_event_id"] = None
        data["source"]["received_at"] += timedelta(seconds=60)
        data["delivery"]["ingested_at"] += timedelta(seconds=90)
        if data["observation"]["metric_key"] == "water.dissolved_oxygen":
            data["observation"]["value"] = 75.0
            data["quality"]["state"] = QualityState.OUT_OF_RANGE
            data["quality"]["provenance"] = (
                QualityProvenance.SCHEMA_VALIDATION,
                QualityProvenance.UNIT_MAPPING,
                QualityProvenance.PLAUSIBLE_RANGE,
            )

    first = [revise(o, first_receipt) for o in sample.observations]
    second = [revise(o, corrected) for o in first]
    assert [o.event_id for o in first] == [o.event_id for o in second]
    by_event = {o.event_id: o for o in second}

    with influx_clients(influx_bucket) as (query_api, write_api):
        writer = InfluxObservationWriter(
            write_api=write_api, bucket=influx_bucket, org=influx_settings().org
        )
        service = dual_write_service(writer, site_assets_for([sample]))

        await service.write(first)
        before = snapshot(query_api, influx_bucket)
        await service.write(second)
        after = snapshot(query_api, influx_bucket)

    stale = list(before[OBSERVATION_MEASUREMENT].values())
    assert len(stale) == 7
    assert {(f["sequence"], f["source_event_id"]) for f in stale} == {(5, "msg-0005")}

    points = after[OBSERVATION_MEASUREMENT]
    assert len(points) == 7
    for (tags, time), fields in points.items():
        expected = by_event[fields["event_id"]]
        assert fields == expected_fields(expected)
        assert dict(tags) == observation_tags(expected)
        assert time == expected.observation.observed_at
    oxygen = next(f for f in points.values() if f["value"] == 75.0)
    assert oxygen["quality_state"] == "out_of_range"
    assert {f["sequence"] for f in points.values()} == {-1}
    assert {f["source_event_id"] for f in points.values()} == {""}

    (legacy,) = after[LEGACY_MEASUREMENT].values()
    assert legacy["do_mg_l"] == 75.0
    assert legacy["temp_c"] == 25.1
    assert legacy["rssi"] == -67
    # D10 known limitation: the legacy line omits `seq`, so the stale value survives.
    assert legacy["seq"] == 5


# --- Telegraf coexistence (D8) ----------------------------------------------------------------


def _readings_consumer() -> dict:
    config = tomllib.loads(TELEGRAF_CONF.read_text(encoding="utf-8"))
    (consumer,) = [c for c in config["inputs"]["mqtt_consumer"] if READINGS_TOPIC in c["topics"]]
    return consumer


def _device_registry() -> dict[str, dict[str, str]]:
    module = ast.parse(DEVICE_REGISTRY.read_text(encoding="utf-8"))
    for node in module.body:
        if isinstance(node, ast.Assign) and node.targets[0].id == "DEVICE_REGISTRY":
            return ast.literal_eval(node.value)
    raise AssertionError("DEVICE_REGISTRY not found")


def telegraf_line(device_id: str, payload: dict[str, object]) -> str:
    """The line Telegraf stores for `payload`, built from the parsed config and its defaults."""
    consumer = _readings_consumer()
    (json_v2,) = consumer["json_v2"]
    (topic_parsing,) = consumer["topic_parsing"]
    topic = READINGS_TOPIC.replace("+", device_id)
    tags = dict(consumer["tags"])
    for name, part in zip(topic_parsing["tags"].split("/"), topic.split("/"), strict=True):
        if name != "_":
            tags[name] = part
    tags[consumer.get("topic_tag", "topic")] = topic
    tags.update(_device_registry()[tags["device_id"]])
    fields = []
    for field in json_v2["field"]:
        value = payload.get(field["path"])
        if value is None:
            continue
        rendered = f"{int(value)}i" if field["type"] == "int" else repr(float(value))
        fields.append(f"{field['path']}={rendered}")
    observed = datetime.fromisoformat(str(payload[json_v2["timestamp_path"]]))
    tag_set = ",".join(f"{key}={value}" for key, value in sorted(tags.items()))
    return f"{json_v2['measurement_name']},{tag_set} {','.join(fields)} {_micros(observed) * 1000}"


@pytest.mark.asyncio
@pytest.mark.parametrize("telegraf_first", [True, False], ids=["telegraf_first", "dual_first"])
@pytest.mark.parametrize(
    ("name", "ts"),
    [("legacy_sample", "2026-07-09T12:00:00Z"), ("sub_second", "2026-07-09T12:00:00.750Z")],
)
async def test_dual_written_point_coexists_with_telegraf_as_one_point(
    influx_bucket: str, name: str, ts: str, telegraf_first: bool
) -> None:
    item = scenario(name)
    payload = {**json.loads(READING_EXAMPLE.read_text(encoding="utf-8")), "ts": ts}
    device_id = "local-device-001"
    line = telegraf_line(device_id, payload)
    observed = datetime.fromisoformat(ts)
    assert {o.observation.observed_at for o in item.observations} == {observed}

    with influx_clients(influx_bucket) as (query_api, write_api):
        writer = InfluxObservationWriter(
            write_api=write_api, bucket=influx_bucket, org=influx_settings().org
        )
        service = ObservationWriteService(
            writer=None,
            legacy_writer=writer,
            site_assets=site_assets_for([item]),
            v2_write_enabled=False,
            legacy_dual_write_enabled=True,
        )
        if telegraf_first:
            write_lines(influx_bucket, [line])
        outcome = await service.write(item.observations)
        if not telegraf_first:
            write_lines(influx_bucket, [line])
        assert outcome.legacy_points == 1

        points = stored_points(query_api, influx_bucket, LEGACY_MEASUREMENT)
        repository = InfluxTelemetryRepository(
            query_api=query_api, org=influx_settings().org, bucket=influx_bucket
        )
        readings = await repository.query_readings(
            tenant_id="tnt_local_001",
            pond_id="pond_local_001",
            start=RANGE_START,
            stop=RANGE_STOP,
            limit=100,
        )

    assert len(points) == 1
    ((tags, time),) = points
    assert time == observed
    assert dict(tags) == {
        "device_id": device_id,
        "pond_id": "pond_local_001",
        "schema_version": "1",
        "source": "mqtt",
        "tenant_id": "tnt_local_001",
        "topic": f"limnopulse/v1/devices/{device_id}/readings",
    }
    assert len(readings) == 1
    (reading,) = readings
    assert reading.measured_at == observed
    assert reading.device_id == device_id
    for field in ("temp_c", "ph", "do_mg_l", "turbidity_ntu", "salinity_ppt", "battery_v"):
        assert getattr(reading, field) == payload[field]
    assert (reading.rssi, reading.seq) == (payload["rssi"], payload["seq"])


# --- failure classification against a real server ---------------------------------------------


def _closed_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["observations", "legacy"])
async def test_closed_loopback_port_is_a_retryable_write_error(method: str) -> None:
    settings = influx_settings()
    sample = scenario("legacy_sample")
    client = InfluxDBClient(
        url=f"http://127.0.0.1:{_closed_loopback_port()}",
        token=settings.token,
        org=settings.org,
        timeout=5_000,
    )
    try:
        with client.write_api(write_options=SYNCHRONOUS) as write_api:
            writer = InfluxObservationWriter(
                write_api=write_api, bucket="unreachable", org=settings.org
            )
            with pytest.raises(TelemetryWriteError) as raised:
                if method == "observations":
                    await writer.write_observations(sample.observations)
                else:
                    await writer.write_legacy_points(
                        build_legacy_points(sample.observations, sample.legacy_pond_by_asset)
                    )
    finally:
        client.close()

    assert raised.value.retryable is True


@pytest.mark.asyncio
async def test_field_type_conflict_is_a_permanent_write_error(influx_bucket: str) -> None:
    sample = scenario("legacy_sample")
    observed_ns = _micros(sample.observations[0].observation.observed_at) * 1000
    write_lines(
        influx_bucket,
        [f'{OBSERVATION_MEASUREMENT},tenant_id=tnt_conflict value="text" {observed_ns}'],
    )

    with influx_clients(influx_bucket) as (_query_api, write_api):
        writer = InfluxObservationWriter(
            write_api=write_api, bucket=influx_bucket, org=influx_settings().org
        )
        with pytest.raises(TelemetryWriteError) as raised:
            await writer.write_observations(sample.observations)

    assert raised.value.retryable is False
