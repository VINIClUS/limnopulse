import json
import re
import tomllib
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]


def test_compose_includes_local_mqtt_broker_and_telegraf_services() -> None:
    compose = yaml.safe_load((ROOT / "compose.yaml").read_text())
    telegraf_config = tomllib.loads((ROOT / "infra/telegraf/telegraf.conf").read_text())

    services = compose["services"]
    assert services["mqtt-broker"]["image"] == "eclipse-mosquitto:2"
    assert services["mqtt-broker"]["ports"] == ["127.0.0.1:1883:1883"]
    assert "./infra/mqtt/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro" in services[
        "mqtt-broker"
    ]["volumes"]

    telegraf = services["telegraf"]
    assert telegraf["image"] == "telegraf:1.32-alpine"
    assert set(telegraf["depends_on"]) == {"mqtt-broker", "influxdb"}
    assert "./infra/telegraf/telegraf.conf:/etc/telegraf/telegraf.conf:ro" in telegraf[
        "volumes"
    ]
    assert (
        "./infra/telegraf/device_registry.star:/etc/telegraf/device_registry.star:ro"
        in telegraf["volumes"]
    )
    mounted_container_paths = {volume.split(":")[1] for volume in telegraf["volumes"]}
    assert telegraf_config["processors"]["starlark"][0]["script"] in mounted_container_paths


def test_mosquitto_config_is_local_only_and_non_persistent() -> None:
    config = (ROOT / "infra/mqtt/mosquitto.conf").read_text()

    assert "listener 1883 127.0.0.1" in config
    assert "allow_anonymous true" in config
    assert "persistence false" in config
    assert "password_file" not in config


def test_telegraf_reads_expected_mqtt_topics_and_writes_raw_bucket() -> None:
    config = tomllib.loads((ROOT / "infra/telegraf/telegraf.conf").read_text())

    outputs = config["outputs"]["influxdb_v2"]
    assert outputs[0]["urls"] == ["${INFLUXDB_URL}"]
    assert outputs[0]["token"] == "${INFLUXDB_TOKEN}"
    assert outputs[0]["organization"] == "${INFLUXDB_ORG}"
    assert outputs[0]["bucket"] == "${INFLUXDB_BUCKET_RAW}"

    mqtt_inputs = config["inputs"]["mqtt_consumer"]
    assert mqtt_inputs[0]["topics"] == ["limnopulse/v1/devices/+/readings"]
    assert mqtt_inputs[0]["servers"] == ["tcp://mqtt-broker:1883"]
    assert mqtt_inputs[0]["data_format"] == "json_v2"
    assert mqtt_inputs[0]["tags"] == {"source": "mqtt", "schema_version": "1"}
    assert mqtt_inputs[0]["json_v2"][0]["measurement_name"] == "water_quality"
    assert mqtt_inputs[0]["topic_parsing"][0]["tags"] == "_/_/_/device_id/_"

    assert mqtt_inputs[1]["topics"] == ["limnopulse/v1/devices/+/health"]
    assert mqtt_inputs[1]["tags"] == {"source": "mqtt", "schema_version": "1"}
    assert mqtt_inputs[1]["json_v2"][0]["measurement_name"] == "device_health"


def test_telegraf_enriches_local_devices_before_influx_write() -> None:
    config = tomllib.loads((ROOT / "infra/telegraf/telegraf.conf").read_text())

    processors = config["processors"]["starlark"]
    assert processors == [{"script": "/etc/telegraf/device_registry.star"}]


def test_local_device_registry_maps_seeded_device_and_drops_unknown_devices() -> None:
    registry = (ROOT / "infra/telegraf/device_registry.star").read_text()

    assert "def apply(metric):" in registry
    assert '"local-device-001"' in registry
    assert '"tenant_id": "tnt_local_001"' in registry
    assert '"pond_id": "pond_local_001"' in registry
    assert 'metric.tags["tenant_id"] = entry["tenant_id"]' in registry
    assert 'metric.tags["pond_id"] = entry["pond_id"]' in registry
    assert "return None" in registry


def test_sample_publisher_registry_and_seed_use_same_local_device_id() -> None:
    publisher = (ROOT / "scripts/dev/publish_sample_reading.py").read_text()
    registry = (ROOT / "infra/telegraf/device_registry.star").read_text()
    seed = (ROOT / "scripts/dev/seed_local.py").read_text()

    topic_match = re.search(
        r'DEFAULT_TOPIC = "limnopulse/v1/devices/([^/]+)/readings"',
        publisher,
    )
    assert topic_match is not None
    local_device_id = topic_match.group(1)

    assert f'"{local_device_id}":' in registry
    assert f'"{local_device_id}"' in seed


def test_local_seed_creates_matching_pond_and_device_registry_records() -> None:
    seed = (ROOT / "scripts/dev/seed_local.py").read_text()

    assert '"pond_local_001"' in seed
    assert '"local-device-001"' in seed
    assert "await repository.create_pond(" in seed
    assert "await repository.create_device(" in seed
    assert seed.count("except ConflictError:") >= 3


def test_sample_reading_payload_does_not_include_domain_authorization_tags() -> None:
    payload = json.loads((ROOT / "examples/telemetry/reading.local.json").read_text())

    assert "tenant_id" not in payload
    assert "pond_id" not in payload
    assert payload["ts"].endswith("Z")
    assert {"seq", "temp_c", "ph", "do_mg_l", "battery_v", "rssi"}.issubset(payload)


def test_sample_publisher_targets_only_the_local_device_topic() -> None:
    script = (ROOT / "scripts/dev/publish_sample_reading.py").read_text()

    assert 'DEFAULT_TOPIC = "limnopulse/v1/devices/local-device-001/readings"' in script
    assert "tenant_id" not in script
    assert "pond_id" not in script


def test_local_ingestion_artifacts_do_not_contain_real_secret_patterns() -> None:
    paths = [
        ROOT / "README.md",
        ROOT / ".env.example",
        ROOT / "compose.yaml",
        *sorted((ROOT / "infra").rglob("*")),
        *sorted((ROOT / "scripts/dev").rglob("*")),
        *sorted((ROOT / "examples").rglob("*")),
    ]
    secret_patterns = [
        re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
        re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
        re.compile(r"\bghp_[A-Za-z0-9_]{20,}\b"),
        re.compile(r"\bsk-[A-Za-z0-9]{20,}\b"),
        re.compile(r"\beyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{10,}\b"),
    ]

    offenders: list[str] = []
    for path in paths:
        if path.is_dir() or "__pycache__" in path.parts:
            continue
        if path.suffix not in {
            ".md",
            ".example",
            ".yaml",
            ".yml",
            ".conf",
            ".py",
            ".json",
            ".star",
        }:
            continue
        text = path.read_text(encoding="utf-8")
        for pattern in secret_patterns:
            if pattern.search(text):
                offenders.append(str(path.relative_to(ROOT)))
                break

    assert offenders == []


def test_execution_artifacts_do_not_introduce_disallowed_storage_dependencies() -> None:
    paths = [
        ROOT / "pyproject.toml",
        ROOT / "compose.yaml",
        *sorted((ROOT / "src").rglob("*")),
        *sorted((ROOT / "infra").rglob("*")),
        *sorted((ROOT / "scripts").rglob("*")),
    ]
    forbidden_terms = [
        "post" + "gres",
        "post" + "gresql",
        "psy" + "copg",
        "sql" + "alchemy",
        "fire" + "store",
        "fire" + "base",
    ]
    forbidden = re.compile("|".join(forbidden_terms), re.IGNORECASE)

    offenders: list[str] = []
    for path in paths:
        if path.is_dir() or "__pycache__" in path.parts:
            continue
        if path.suffix not in {".py", ".toml", ".yaml", ".yml", ".conf", ".star"}:
            continue
        if forbidden.search(path.read_text(encoding="utf-8")):
            offenders.append(str(path.relative_to(ROOT)))

    assert offenders == []
