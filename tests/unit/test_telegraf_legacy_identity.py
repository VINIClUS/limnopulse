import tomllib
from datetime import UTC, datetime
from pathlib import Path

from limnopulse_api.domain.metric_catalog import metric_catalog
from limnopulse_api.domain.telemetry_v2 import LEGACY_MQTT_TOPIC_TEMPLATE, MetricValueType
from limnopulse_api.domain.telemetry_v2_schema import (
    LEGACY_INTEGER_FIELDS,
    LEGACY_MEASUREMENT,
    LEGACY_TAG_KEYS,
    LegacyPoint,
    encode_legacy_line,
)

ROOT = Path(__file__).resolve().parents[2]
TELEGRAF_CONF = ROOT / "infra/telegraf/telegraf.conf"
DEVICE_REGISTRY = ROOT / "infra/telegraf/device_registry.star"
READINGS_TOPIC = "limnopulse/v1/devices/+/readings"


def _readings_consumer() -> dict:
    config = tomllib.loads(TELEGRAF_CONF.read_text(encoding="utf-8"))
    consumers = [
        consumer
        for consumer in config["inputs"]["mqtt_consumer"]
        if READINGS_TOPIC in consumer["topics"]
    ]
    assert len(consumers) == 1
    return consumers[0]


def _parse_line(line: str) -> tuple[str, dict[str, str], str, str]:
    series, fields, timestamp = line.split(" ")
    measurement, *tag_pairs = series.split(",")
    return measurement, dict(pair.split("=", 1) for pair in tag_pairs), fields, timestamp


def test_telegraf_readings_consumer_identity_is_frozen() -> None:
    consumer = _readings_consumer()

    assert consumer["topics"] == [READINGS_TOPIC]
    assert consumer["tags"] == {"source": "mqtt", "schema_version": "1"}
    assert "topic_tag" not in consumer
    (topic_parsing,) = consumer["topic_parsing"]
    assert topic_parsing == {"topic": READINGS_TOPIC, "tags": "_/_/_/device_id/_"}
    (json_v2,) = consumer["json_v2"]
    assert json_v2["measurement_name"] == "water_quality"
    assert "timestamp_path" in json_v2


def test_telegraf_registry_adds_tenant_and_pond_tags() -> None:
    source = DEVICE_REGISTRY.read_text(encoding="utf-8")
    assert 'metric.tags["tenant_id"]' in source
    assert 'metric.tags["pond_id"]' in source


def test_telegraf_field_types_match_catalog_and_integer_fields() -> None:
    (json_v2,) = _readings_consumer()["json_v2"]
    telegraf_types = {field["path"]: field["type"] for field in json_v2["field"]}

    catalog_types = {
        definition.legacy_field: (
            "int" if definition.value_type is MetricValueType.INTEGER else "float"
        )
        for definition in metric_catalog()
    }
    assert telegraf_types == {**catalog_types, "seq": "int"}
    assert {path for path, kind in telegraf_types.items() if kind == "int"} == LEGACY_INTEGER_FIELDS


def test_topic_template_matches_telegraf_subscription() -> None:
    assert LEGACY_MQTT_TOPIC_TEMPLATE.format(device_id="+") == READINGS_TOPIC


def test_d8_encoder_reproduces_the_telegraf_series_identity() -> None:
    consumer = _readings_consumer()
    device_id = "local-device-001"
    point = LegacyPoint(
        tenant_id="tnt_local_001",
        pond_id="pond_local_001",
        device_id=device_id,
        observed_at=datetime(2026, 7, 9, 12, 0, 0, 750_000, tzinfo=UTC),
        fields={"temp_c": 25.1, "seq": 1},
    )

    measurement, tags, fields, timestamp = _parse_line(encode_legacy_line(point))

    (json_v2,) = consumer["json_v2"]
    assert measurement == json_v2["measurement_name"] == LEGACY_MEASUREMENT
    assert tags == {
        **consumer["tags"],
        "device_id": device_id,
        "tenant_id": "tnt_local_001",
        "pond_id": "pond_local_001",
        "topic": READINGS_TOPIC.replace("+", device_id),
    }
    assert set(tags) == set(LEGACY_TAG_KEYS)
    assert list(tags) == sorted(tags)
    assert fields == "seq=1i,temp_c=25.1"
    assert timestamp == "1783598400750000000"
