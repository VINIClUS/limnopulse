import json
import shutil
import sys
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from pathlib import Path

import pytest
from limnopulse_api.domain.metric_catalog import metric_catalog, metric_definition
from limnopulse_api.domain.telemetry_v2 import (
    METRIC_CATALOG_VERSION,
    QUALITY_POLICY_VERSION,
    TELEMETRY_SCHEMA_VERSION,
    UNIT_MAPPING_VERSION,
    CanonicalObservation,
    QualityFlag,
    QualityProvenance,
    QualityState,
    SourceType,
    observation_event_id,
)
from limnopulse_api.domain.telemetry_v2_schema import (
    LEGACY_INTEGER_FIELDS,
    LEGACY_MEASUREMENT,
    LEGACY_TAG_KEYS,
    OBSERVATION_FIELD_KEYS,
    OBSERVATION_MEASUREMENT,
    OBSERVATION_REQUIRED_TAG_KEYS,
    OBSERVATION_TAG_KEYS,
    build_legacy_points,
    encode_legacy_line,
    encode_observation_line,
)

from limnopulse_api.domain.ids import (
    asset_id_for_legacy_pond,
    default_component_id_for_legacy_device,
)
from scripts.dev import export_telemetry_v2_vectors as exporter
from tests.support.telemetry_v2_vectors import (
    VECTORS_PATH,
    TelemetryScenario,
    load_vectors,
    scenario,
)

ROOT = Path(__file__).resolve().parents[2]
COMMITTED = ROOT / "testdata/telemetry_v2/contract_vectors.json"
READING = ROOT / "examples/telemetry/reading.local.json"
LOCAL_TENANT = "tnt_local_001"
LOCAL_POND = "pond_local_001"
LOCAL_DEVICE = "local-device-001"
SCENARIOS = (
    "legacy_sample",
    "sub_second",
    "two_probes_same_metric",
    "out_of_range",
    "delayed_replay",
    "relocation",
    "unit_converted",
    "multi_point_series",
)
LEGACY_SAMPLE_LINE = (
    "water_quality,device_id=local-device-001,pond_id=pond_local_001,schema_version=1,"
    "source=mqtt,tenant_id=tnt_local_001,topic=limnopulse/v1/devices/local-device-001/readings "
    "battery_v=3.82,do_mg_l=6.4,ph=7.2,rssi=-67i,salinity_ppt=11.8,seq=1i,temp_c=25.1,"
    "turbidity_ntu=3.1 1783598400000000000"
)


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _run(monkeypatch: pytest.MonkeyPatch, *args: str) -> int:
    monkeypatch.setattr(sys, "argv", ["export_telemetry_v2_vectors.py", *args])
    return exporter.main()


# --- export script --------------------------------------------------------------------------


def test_loader_reads_the_committed_file() -> None:
    assert VECTORS_PATH == COMMITTED


def test_check_passes_on_committed_file(monkeypatch) -> None:
    assert _run(monkeypatch, "--check") == 0


def test_check_fails_on_mutated_copy(monkeypatch, tmp_path) -> None:
    copy = tmp_path / "contract_vectors.json"
    shutil.copyfile(COMMITTED, copy)
    monkeypatch.setattr(exporter, "OUTPUT_PATH", copy)
    assert _run(monkeypatch, "--check") == 0

    text = copy.read_text(encoding="utf-8")
    assert "temp_c=25.1," in text
    copy.write_text(text.replace("temp_c=25.1,", "temp_c=25.2,", 1), encoding="utf-8")
    assert _run(monkeypatch, "--check") == 1

    copy.write_text(text.rstrip("\n"), encoding="utf-8")
    assert _run(monkeypatch, "--check") == 1


def test_check_fails_when_file_is_missing(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(exporter, "OUTPUT_PATH", tmp_path / "missing.json")
    assert _run(monkeypatch, "--check") == 1
    assert not (tmp_path / "missing.json").exists()


def test_export_regenerates_the_committed_bytes(monkeypatch, tmp_path) -> None:
    target = tmp_path / "nested" / "contract_vectors.json"
    monkeypatch.setattr(exporter, "OUTPUT_PATH", target)

    assert _run(monkeypatch) == 0
    first = target.read_bytes()
    assert _run(monkeypatch) == 0

    assert first == target.read_bytes() == COMMITTED.read_bytes()


def test_export_script_has_no_output_flag(monkeypatch, tmp_path) -> None:
    with pytest.raises(SystemExit) as raised:
        _run(monkeypatch, "--output", str(tmp_path / "x.json"))
    assert raised.value.code == 2


# --- static content -------------------------------------------------------------------------


def test_vector_file_pins_versions_and_names() -> None:
    vectors = load_vectors()

    assert vectors["versions"] == {
        "telemetry_schema_version": TELEMETRY_SCHEMA_VERSION,
        "metric_catalog_version": METRIC_CATALOG_VERSION,
        "unit_mapping_version": UNIT_MAPPING_VERSION,
        "quality_policy_version": QUALITY_POLICY_VERSION,
    }
    assert vectors["measurements"] == {
        "observation": OBSERVATION_MEASUREMENT,
        "legacy": LEGACY_MEASUREMENT,
    }
    assert vectors["observation_tag_keys"] == sorted(OBSERVATION_TAG_KEYS)
    assert vectors["observation_required_tag_keys"] == sorted(OBSERVATION_REQUIRED_TAG_KEYS)
    assert vectors["observation_field_keys"] == sorted(OBSERVATION_FIELD_KEYS)
    assert vectors["legacy_tag_keys"] == sorted(LEGACY_TAG_KEYS)
    assert vectors["legacy_integer_fields"] == sorted(LEGACY_INTEGER_FIELDS)


def test_vector_file_catalog_rows_match_the_catalog() -> None:
    rows = load_vectors()["metric_catalog"]
    assert rows == [definition.model_dump(mode="json") for definition in metric_catalog()]
    assert [row["display_name"] for row in rows] == [
        "Water temperature",
        "pH",
        "Dissolved oxygen",
        "Turbidity",
        "Salinity",
        "Battery voltage",
        "Signal strength (RSSI)",
    ]


def test_asset_id_vectors_match_ids_module() -> None:
    vectors = load_vectors()["id_vectors"]["asset_id_for_legacy_pond"]
    assert len(vectors) >= 5
    assert len({(item["tenant_id"], item["pond_id"]) for item in vectors}) == len(vectors)
    assert {"tenant_id": LOCAL_TENANT, "pond_id": LOCAL_POND} in [
        {"tenant_id": item["tenant_id"], "pond_id": item["pond_id"]} for item in vectors
    ]
    for item in vectors:
        assert asset_id_for_legacy_pond(item["tenant_id"], item["pond_id"]) == item["asset_id"]


def test_component_id_vectors_match_ids_module() -> None:
    vectors = load_vectors()["id_vectors"]["default_component_id_for_legacy_device"]
    assert len(vectors) >= 5
    assert len({(item["tenant_id"], item["device_id"]) for item in vectors}) == len(vectors)
    assert (LOCAL_TENANT, LOCAL_DEVICE) in {
        (item["tenant_id"], item["device_id"]) for item in vectors
    }
    for item in vectors:
        assert (
            default_component_id_for_legacy_device(item["tenant_id"], item["device_id"])
            == item["component_id"]
        )


def test_event_id_vectors_match_identity_function() -> None:
    vectors = load_vectors()["id_vectors"]["observation_event_id"]
    assert len(vectors) >= 5
    assert len({item["event_id"] for item in vectors}) == len(vectors)
    local_component = default_component_id_for_legacy_device(LOCAL_TENANT, LOCAL_DEVICE)
    assert any(
        item["tenant_id"] == LOCAL_TENANT and item["component_id"] == local_component
        for item in vectors
    )
    for item in vectors:
        observed_at = _parse(item["observed_at"])
        assert item["observed_at"].endswith("Z")
        assert (
            observation_event_id(
                item["tenant_id"], item["component_id"], item["metric_key"], observed_at
            )
            == item["event_id"]
        )


def test_vector_file_holds_exactly_the_locked_scenarios() -> None:
    assert set(load_vectors()["scenarios"]) == set(SCENARIOS)


# --- loader round trip ----------------------------------------------------------------------


@pytest.mark.parametrize("name", SCENARIOS)
def test_loader_round_trips_every_scenario(name) -> None:
    raw = load_vectors()["scenarios"][name]
    loaded = scenario(name)

    assert isinstance(loaded, TelemetryScenario)
    assert loaded.name == name
    assert loaded.observations
    assert all(isinstance(item, CanonicalObservation) for item in loaded.observations)
    assert [item.model_dump(mode="json") for item in loaded.observations] == raw["observations"]
    assert loaded.v2_lines == tuple(raw["v2_lines"])
    assert loaded.legacy_lines == tuple(raw["legacy_lines"])
    assert loaded.legacy_count == len(raw["legacy_lines"])
    assert loaded.tenant_id == raw["tenant_id"]
    assert loaded.source_type == SourceType(raw["source_type"])
    assert dict(loaded.legacy_pond_by_asset) == raw["legacy_pond_by_asset"]


@pytest.mark.parametrize("name", SCENARIOS)
def test_scenario_lines_are_reproduced_by_the_production_encoders(name) -> None:
    loaded = scenario(name)

    assert loaded.v2_lines == tuple(encode_observation_line(item) for item in loaded.observations)
    assert len(loaded.v2_lines) == len(loaded.observations)
    rebuilt = sorted(
        encode_legacy_line(point)
        for point in build_legacy_points(loaded.observations, loaded.legacy_pond_by_asset)
    )
    assert rebuilt == sorted(loaded.legacy_lines)


@pytest.mark.parametrize("name", SCENARIOS)
def test_scenario_metadata_matches_its_observations(name) -> None:
    loaded = scenario(name)

    assert {item.resolved.tenant_id for item in loaded.observations} == {loaded.tenant_id}
    assert {item.source.source_type for item in loaded.observations} == {loaded.source_type}
    assert set(loaded.devices) == {item.resolved.device_id for item in loaded.observations}
    assert set(loaded.components) == {item.resolved.component_id for item in loaded.observations}
    for asset_id, pond_id in loaded.legacy_pond_by_asset.items():
        assert asset_id == asset_id_for_legacy_pond(loaded.tenant_id, pond_id)


# --- scenario semantics ---------------------------------------------------------------------


def _legacy_sample_assertions(loaded: TelemetryScenario, observed_at: datetime) -> None:
    reading = json.loads(READING.read_text(encoding="utf-8"))
    assert loaded.tenant_id == LOCAL_TENANT
    assert loaded.source_type is SourceType.LEGACY_MQTT
    assert loaded.legacy_pond_by_asset == {
        asset_id_for_legacy_pond(LOCAL_TENANT, LOCAL_POND): LOCAL_POND
    }
    assert len(loaded.v2_lines) == 7
    assert loaded.legacy_count == 1
    default_component = default_component_id_for_legacy_device(LOCAL_TENANT, LOCAL_DEVICE)
    values = {}
    for item in loaded.observations:
        assert item.resolved.component_id == default_component
        assert item.resolved.device_id == LOCAL_DEVICE
        assert item.observation.observed_at == observed_at
        assert item.observation.sequence == reading["seq"]
        assert item.quality.state is QualityState.VALID
        definition = metric_definition(item.observation.metric_key)
        assert definition is not None
        values[definition.legacy_field] = item.observation.value
    assert values == {key: value for key, value in reading.items() if key not in {"ts", "seq"}}


def test_legacy_sample_scenario_reproduces_the_example_reading() -> None:
    loaded = scenario("legacy_sample")
    reading = json.loads(READING.read_text(encoding="utf-8"))

    _legacy_sample_assertions(loaded, _parse(reading["ts"]))
    assert loaded.legacy_lines == (LEGACY_SAMPLE_LINE,)


def test_sub_second_scenario_keeps_exact_time() -> None:
    loaded = scenario("sub_second")
    observed_at = datetime(2026, 7, 9, 12, 0, 0, 750_000, tzinfo=UTC)

    _legacy_sample_assertions(loaded, observed_at)
    (line,) = loaded.legacy_lines
    assert line.endswith(" 1783598400750000000")
    assert line == LEGACY_SAMPLE_LINE.replace(" 1783598400000000000", " 1783598400750000000")
    assert all(item.endswith(" 1783598400750000000") for item in loaded.v2_lines)


def test_two_probes_scenario_never_reaches_legacy() -> None:
    loaded = scenario("two_probes_same_metric")

    assert len(loaded.v2_lines) == 2
    assert loaded.legacy_count == 0
    first, second = loaded.observations
    assert (
        first.observation.metric_key == second.observation.metric_key == ("water.dissolved_oxygen")
    )
    assert first.observation.observed_at == second.observation.observed_at
    assert first.resolved.device_id == second.resolved.device_id
    assert first.resolved.device_id.startswith("dev_")
    assert first.resolved.component_id != second.resolved.component_id
    assert first.event_id != second.event_id
    assert first.resolved.probe_id is not None and second.resolved.probe_id is not None
    assert first.resolved.probe_id != second.resolved.probe_id
    assert first.resolved.asset_id == second.resolved.asset_id
    assert first.resolved.asset_id in loaded.legacy_pond_by_asset
    for item in loaded.observations:
        assert item.resolved.component_id != default_component_id_for_legacy_device(
            item.resolved.tenant_id, item.resolved.device_id
        )
    assert all(",probe_id=prb_" in line for line in loaded.v2_lines)


def test_out_of_range_scenario_is_written_with_state_and_provenance() -> None:
    loaded = scenario("out_of_range")

    (item,) = loaded.observations
    assert item.observation.metric_key == "water.dissolved_oxygen"
    assert item.observation.value == 75.0
    assert item.quality.state is QualityState.OUT_OF_RANGE
    assert item.quality.flags == ()
    assert item.quality.provenance == (
        QualityProvenance.SCHEMA_VALIDATION,
        QualityProvenance.UNIT_MAPPING,
        QualityProvenance.PLAUSIBLE_RANGE,
    )
    assert item.resolved.component_id == default_component_id_for_legacy_device(
        item.resolved.tenant_id, item.resolved.device_id
    )
    (v2_line,) = loaded.v2_lines
    assert ',quality_state="out_of_range",' in v2_line
    (legacy_line,) = loaded.legacy_lines
    assert " do_mg_l=75.0" in legacy_line


def test_delayed_replay_scenario_carries_both_flags() -> None:
    loaded = scenario("delayed_replay")

    assert len(loaded.v2_lines) >= 1
    assert loaded.legacy_count == 1
    for item in loaded.observations:
        assert item.quality.flags == (QualityFlag.DELAYED, QualityFlag.REPLAY)
        assert item.quality.provenance == (
            QualityProvenance.SCHEMA_VALIDATION,
            QualityProvenance.UNIT_MAPPING,
            QualityProvenance.TIMELINESS,
            QualityProvenance.REPLAY,
        )
        assert item.quality.state is QualityState.VALID
        assert item.source.received_at - item.observation.observed_at <= timedelta(days=30)
        assert item.delivery.delayed is True
        assert item.delivery.replay is True
        assert item.source.received_at - item.observation.observed_at > timedelta(seconds=300)
    assert all('\\"delayed\\"' in line and '\\"replay\\"' in line for line in loaded.v2_lines)


def test_relocation_scenario_splits_one_component_across_two_ponds() -> None:
    loaded = scenario("relocation")

    assert len(loaded.v2_lines) >= 2
    assert loaded.legacy_count == 2
    assert {item.resolved.component_id for item in loaded.observations} == {
        default_component_id_for_legacy_device(loaded.tenant_id, device)
        for device in loaded.devices
    }
    assert len(loaded.devices) == 1
    assert set(loaded.legacy_pond_by_asset.values()) == {"pond_west", "pond_east"}
    by_pond: dict[str, list[CanonicalObservation]] = {}
    for item in loaded.observations:
        by_pond.setdefault(loaded.legacy_pond_by_asset[item.resolved.asset_id], []).append(item)
    assert set(by_pond) == {"pond_west", "pond_east"}
    west_deployments = {item.resolved.deployment_id for item in by_pond["pond_west"]}
    east_deployments = {item.resolved.deployment_id for item in by_pond["pond_east"]}
    assert len(west_deployments) == len(east_deployments) == 1
    assert west_deployments != east_deployments
    assert max(item.observation.observed_at for item in by_pond["pond_west"]) < min(
        item.observation.observed_at for item in by_pond["pond_east"]
    )
    assert {line.split(",")[2] for line in loaded.legacy_lines} == {
        "pond_id=pond_west",
        "pond_id=pond_east",
    }


def test_unit_converted_scenario_stores_canonical_unit() -> None:
    loaded = scenario("unit_converted")

    (item,) = loaded.observations
    assert item.observation.metric_key == "water.temperature"
    assert item.observation.source_unit == "degF"
    assert item.observation.canonical_unit == "degC"
    assert item.observation.value == 25.0
    (v2_line,) = loaded.v2_lines
    assert ',source_unit="degF",' in v2_line
    assert v2_line.split(" ")[1].endswith(",value=25.0")
    (legacy_line,) = loaded.legacy_lines
    assert " temp_c=25.0 " in legacy_line or ",temp_c=25.0 " in legacy_line


def test_multi_point_series_scenario_shape() -> None:
    loaded = scenario("multi_point_series")

    assert len(loaded.devices) == 2
    assert len(set(loaded.legacy_pond_by_asset.values())) == 2
    ponds_by_device: dict[str, set[str]] = {}
    times_by_device: dict[str, set[datetime]] = {}
    for item in loaded.observations:
        device = item.resolved.device_id
        ponds_by_device.setdefault(device, set()).add(
            loaded.legacy_pond_by_asset[item.resolved.asset_id]
        )
        times_by_device.setdefault(device, set()).add(item.observation.observed_at)
    assert len({frozenset(ponds) for ponds in ponds_by_device.values()}) == 2
    for times in times_by_device.values():
        ordered = sorted(times)
        assert len(ordered) >= 2
        assert all(b - a == timedelta(minutes=1) for a, b in pairwise(ordered))
    assert len(loaded.observations) >= 30
    assert loaded.legacy_count >= 30
    assert loaded.legacy_count == sum(len(times) for times in times_by_device.values())


# --- rebased --------------------------------------------------------------------------------


@pytest.mark.parametrize("name", ["multi_point_series", "relocation", "delayed_replay"])
def test_rebased_keeps_spacing_and_recomputes_identity(name) -> None:
    original = scenario(name)
    now = datetime(2031, 3, 4, 5, 6, 7, 891_011, tzinfo=UTC)

    rebased = original.rebased(now)

    assert isinstance(rebased, TelemetryScenario)
    assert len(rebased.observations) == len(original.observations)
    latest = max(item.observation.observed_at for item in rebased.observations)
    assert latest == now - timedelta(seconds=1)
    assert all(item.observation.observed_at < now for item in rebased.observations)
    deltas = set()
    for before, after in zip(original.observations, rebased.observations, strict=True):
        delta = after.observation.observed_at - before.observation.observed_at
        deltas.add(delta)
        assert after.source.received_at - before.source.received_at == delta
        assert after.delivery.ingested_at - before.delivery.ingested_at == delta
        assert after.observation.value == before.observation.value
        assert after.quality == before.quality
        assert after.event_id != before.event_id
        assert after.event_id == observation_event_id(
            after.resolved.tenant_id,
            after.resolved.component_id,
            after.observation.metric_key,
            after.observation.observed_at,
        )
    assert len(deltas) == 1
    assert rebased.v2_lines == tuple(encode_observation_line(item) for item in rebased.observations)
    assert rebased.legacy_count == original.legacy_count
    assert sorted(rebased.legacy_lines) == sorted(
        encode_legacy_line(point)
        for point in build_legacy_points(rebased.observations, rebased.legacy_pond_by_asset)
    )
    assert scenario(name) == original


def test_rebased_rejects_naive_now() -> None:
    with pytest.raises(ValueError):
        scenario("legacy_sample").rebased(datetime(2031, 1, 1))
