"""Generate the P2 cross-language telemetry vectors from the Python contract.

Usage: export_telemetry_v2_vectors.py [--check]   (--check exits 1 on drift, writing nothing)
"""

import argparse
import json
from collections.abc import Callable, Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from limnopulse_api.domain.ids import (
    asset_id_for_legacy_pond,
    default_component_id_for_legacy_device,
    default_probe_profile_id_for_legacy_device,
    default_site_id,
    deployment_id_for_legacy_device,
)
from limnopulse_api.domain.metric_catalog import metric_catalog, metric_for_legacy_field
from limnopulse_api.domain.telemetry_v2 import (
    METRIC_CATALOG_VERSION,
    QUALITY_POLICY_VERSION,
    TELEMETRY_SCHEMA_VERSION,
    UNIT_MAPPING_VERSION,
    CanonicalObservation,
    NormalizationVersions,
    ObservationDelivery,
    ObservationMeasurement,
    ObservationQuality,
    ObservationResolution,
    ObservationSource,
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
from limnopulse_api.domain.v2_common import format_utc_timestamp

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_PATH = REPOSITORY_ROOT / "testdata/telemetry_v2/contract_vectors.json"
READING_EXAMPLE = REPOSITORY_ROOT / "examples/telemetry/reading.local.json"

LOCAL_TENANT = "tnt_local_001"
LOCAL_POND = "pond_local_001"
LOCAL_DEVICE = "local-device-001"
LEGACY_INTEGRATION_TYPE = "local.mosquitto"
NATIVE_INTEGRATION_TYPE = "vendor.example"
BASE_PROVENANCE = (QualityProvenance.SCHEMA_VALIDATION, QualityProvenance.UNIT_MAPPING)
CANONICAL_UNITS = {definition.metric_key: definition for definition in metric_catalog()}


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(UTC)


def _observation(
    *,
    tenant_id: str,
    device_id: str,
    metric_key: str,
    value: float,
    observed_at: datetime,
    asset_id: str,
    component_id: str | None = None,
    probe_id: str | None = None,
    deployment_id: str | None = None,
    sequence: int | None = None,
    source_unit: str | None = None,
    source_type: SourceType = SourceType.LEGACY_MQTT,
    integration_id: str | None = None,
    received_after: timedelta = timedelta(seconds=2),
    ingested_after: timedelta = timedelta(seconds=3),
    state: QualityState = QualityState.VALID,
    flags: tuple[QualityFlag, ...] = (),
    provenance: tuple[QualityProvenance, ...] = BASE_PROVENANCE,
) -> CanonicalObservation:
    """Legacy default-Component identity unless native IDs are given."""
    definition = CANONICAL_UNITS[metric_key]
    component_id = component_id or default_component_id_for_legacy_device(tenant_id, device_id)
    legacy = component_id == default_component_id_for_legacy_device(tenant_id, device_id)
    if legacy:
        probe_id = default_probe_profile_id_for_legacy_device(tenant_id, device_id)
        deployment_id = deployment_id or deployment_id_for_legacy_device(tenant_id, device_id)
    integration_type = (
        LEGACY_INTEGRATION_TYPE
        if source_type is SourceType.LEGACY_MQTT
        else NATIVE_INTEGRATION_TYPE
    )
    return CanonicalObservation(
        schema_version=2,
        event_id=observation_event_id(tenant_id, component_id, metric_key, observed_at),
        source_event_id=None,
        source=ObservationSource(
            integration_id=integration_id,
            integration_type=integration_type,
            source_type=source_type,
            connector_version=None,
            received_at=observed_at + received_after,
        ),
        resolved=ObservationResolution(
            tenant_id=tenant_id,
            site_id=default_site_id(tenant_id),
            asset_id=asset_id,
            deployment_id=deployment_id,
            device_id=device_id,
            component_id=component_id,
            probe_id=probe_id,
        ),
        observation=ObservationMeasurement(
            metric_key=metric_key,
            value=value,
            value_type=definition.value_type,
            source_unit=source_unit or definition.canonical_unit,
            canonical_unit=definition.canonical_unit,
            observed_at=observed_at,
            sequence=sequence,
        ),
        delivery=ObservationDelivery(
            ingested_at=observed_at + ingested_after,
            delayed=QualityFlag.DELAYED in flags,
            replay=QualityFlag.REPLAY in flags,
        ),
        quality=ObservationQuality(
            state=state, flags=flags, confidence=None, provenance=provenance
        ),
        mapping=NormalizationVersions(),
        source_metadata={},
    )


def _reading_observations(observed_at: datetime) -> list[CanonicalObservation]:
    reading = json.loads(READING_EXAMPLE.read_text(encoding="utf-8"))
    asset_id = asset_id_for_legacy_pond(LOCAL_TENANT, LOCAL_POND)
    observations = []
    for legacy_field, value in reading.items():
        definition = metric_for_legacy_field(legacy_field)
        if definition is None:
            continue
        observations.append(
            _observation(
                tenant_id=LOCAL_TENANT,
                device_id=LOCAL_DEVICE,
                metric_key=definition.metric_key,
                value=float(value),
                observed_at=observed_at,
                asset_id=asset_id,
                sequence=reading["seq"],
            )
        )
    return observations


def _legacy_sample() -> tuple[str, list[CanonicalObservation]]:
    reading = json.loads(READING_EXAMPLE.read_text(encoding="utf-8"))
    return (
        "examples/telemetry/reading.local.json as normalized by the legacy MQTT mapper",
        _reading_observations(_utc(reading["ts"])),
    )


def _sub_second() -> tuple[str, list[CanonicalObservation]]:
    return (
        "legacy_sample at a sub-second observed_at; legacy time stays exact",
        _reading_observations(_utc("2026-07-09T12:00:00.750Z")),
    )


def _two_probes_same_metric() -> tuple[str, list[CanonicalObservation]]:
    device_id = "dev_" + "2" * 32
    observed_at = _utc("2026-07-09T12:00:00Z")
    asset_id = asset_id_for_legacy_pond(LOCAL_TENANT, LOCAL_POND)
    probes = (
        ("cmp_" + "a1" * 16, "prb_" + "b1" * 16, "dep_" + "c1" * 16, 6.4),
        ("cmp_" + "a2" * 16, "prb_" + "b2" * 16, "dep_" + "c2" * 16, 6.6),
    )
    return (
        "two probe Components of one native Device measure the same metric on a legacy pond",
        [
            _observation(
                tenant_id=LOCAL_TENANT,
                device_id=device_id,
                metric_key="water.dissolved_oxygen",
                value=value,
                observed_at=observed_at,
                asset_id=asset_id,
                component_id=component_id,
                probe_id=probe_id,
                deployment_id=deployment_id,
                source_type=SourceType.HTTPS,
                integration_id="di_" + "d1" * 16,
            )
            for component_id, probe_id, deployment_id, value in probes
        ],
    )


def _out_of_range() -> tuple[str, list[CanonicalObservation]]:
    return (
        "legacy default Component reports dissolved oxygen above its plausible range",
        [
            _observation(
                tenant_id=LOCAL_TENANT,
                device_id=LOCAL_DEVICE,
                metric_key="water.dissolved_oxygen",
                value=75.0,
                observed_at=_utc("2026-07-09T13:00:00Z"),
                asset_id=asset_id_for_legacy_pond(LOCAL_TENANT, LOCAL_POND),
                sequence=2,
                state=QualityState.OUT_OF_RANGE,
                provenance=BASE_PROVENANCE + (QualityProvenance.PLAUSIBLE_RANGE,),
            )
        ],
    )


def _delayed_replay() -> tuple[str, list[CanonicalObservation]]:
    observed_at = _utc("2026-07-09T14:00:00Z")
    return (
        "a replayed reading received ten minutes late: delayed and replay flags",
        [
            _observation(
                tenant_id=LOCAL_TENANT,
                device_id=LOCAL_DEVICE,
                metric_key=metric_key,
                value=value,
                observed_at=observed_at,
                asset_id=asset_id_for_legacy_pond(LOCAL_TENANT, LOCAL_POND),
                sequence=3,
                received_after=timedelta(minutes=10),
                ingested_after=timedelta(minutes=10, seconds=1),
                flags=(QualityFlag.DELAYED, QualityFlag.REPLAY),
                provenance=BASE_PROVENANCE
                + (QualityProvenance.TIMELINESS, QualityProvenance.REPLAY),
            )
            for metric_key, value in (("water.temperature", 24.8), ("water.ph", 7.1))
        ],
    )


def _relocation() -> tuple[str, list[CanonicalObservation]]:
    device_id = "relocated-device-001"
    moves = (
        ("pond_west", "dep_" + "e1" * 16, "2026-07-09T10:00:00Z", 6.1),
        ("pond_east", "dep_" + "e2" * 16, "2026-07-09T14:00:00Z", 5.9),
    )
    return (
        "one legacy default Component moved from pond_west to pond_east",
        [
            _observation(
                tenant_id=LOCAL_TENANT,
                device_id=device_id,
                metric_key="water.dissolved_oxygen",
                value=value,
                observed_at=_utc(observed_at),
                asset_id=asset_id_for_legacy_pond(LOCAL_TENANT, pond_id),
                deployment_id=deployment_id,
                sequence=index,
            )
            for index, (pond_id, deployment_id, observed_at, value) in enumerate(moves, start=1)
        ],
    )


def _unit_converted() -> tuple[str, list[CanonicalObservation]]:
    return (
        "77 degF water temperature stored as 25.0 degC",
        [
            _observation(
                tenant_id=LOCAL_TENANT,
                device_id=LOCAL_DEVICE,
                metric_key="water.temperature",
                value=25.0,
                observed_at=_utc("2026-07-09T15:00:00Z"),
                asset_id=asset_id_for_legacy_pond(LOCAL_TENANT, LOCAL_POND),
                source_unit="degF",
                sequence=4,
            )
        ],
    )


def _multi_point_series() -> tuple[str, list[CanonicalObservation]]:
    start = _utc("2026-07-09T10:00:00Z")
    devices = (("series-device-a", "pond_north", 24.0), ("series-device-b", "pond_south", 26.0))
    observations = []
    for device_id, pond_id, base_temperature in devices:
        asset_id = asset_id_for_legacy_pond(LOCAL_TENANT, pond_id)
        for minute in range(30):
            observed_at = start + timedelta(minutes=minute)
            for metric_key, value in (
                ("water.temperature", round(base_temperature + minute / 10, 1)),
                ("water.dissolved_oxygen", round(6.0 + (minute % 7) / 10, 1)),
            ):
                observations.append(
                    _observation(
                        tenant_id=LOCAL_TENANT,
                        device_id=device_id,
                        metric_key=metric_key,
                        value=value,
                        observed_at=observed_at,
                        asset_id=asset_id,
                        sequence=minute,
                    )
                )
    return "30 one-minute readings for two legacy devices in two legacy ponds", observations


SCENARIOS: dict[str, Callable[[], tuple[str, list[CanonicalObservation]]]] = {
    "legacy_sample": _legacy_sample,
    "sub_second": _sub_second,
    "two_probes_same_metric": _two_probes_same_metric,
    "out_of_range": _out_of_range,
    "delayed_replay": _delayed_replay,
    "relocation": _relocation,
    "unit_converted": _unit_converted,
    "multi_point_series": _multi_point_series,
}

LEGACY_PONDS = {
    "legacy_sample": (LOCAL_POND,),
    "sub_second": (LOCAL_POND,),
    "two_probes_same_metric": (LOCAL_POND,),
    "out_of_range": (LOCAL_POND,),
    "delayed_replay": (LOCAL_POND,),
    "relocation": ("pond_west", "pond_east"),
    "unit_converted": (LOCAL_POND,),
    "multi_point_series": ("pond_north", "pond_south"),
}


def _unique(values: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(values))


def _scenario(name: str) -> dict[str, Any]:
    description, observations = SCENARIOS[name]()
    legacy_pond_by_asset = {
        asset_id_for_legacy_pond(LOCAL_TENANT, pond_id): pond_id for pond_id in LEGACY_PONDS[name]
    }
    return {
        "description": description,
        "tenant_id": LOCAL_TENANT,
        "source_type": str(observations[0].source.source_type),
        "devices": _unique(item.resolved.device_id for item in observations),
        "components": _unique(item.resolved.component_id for item in observations),
        "legacy_pond_by_asset": legacy_pond_by_asset,
        "observations": [item.model_dump(mode="json") for item in observations],
        "v2_lines": [encode_observation_line(item) for item in observations],
        "legacy_lines": [
            encode_legacy_line(point)
            for point in build_legacy_points(observations, legacy_pond_by_asset)
        ],
    }


def _id_vectors() -> dict[str, list[dict[str, str]]]:
    ponds = [
        (LOCAL_TENANT, LOCAL_POND),
        (LOCAL_TENANT, "pond_west"),
        (LOCAL_TENANT, "pond_east"),
        ("tnt_other_001", LOCAL_POND),
        ("tenant.with:punct-1", "pond.with:punct-1"),
    ]
    devices = [
        (LOCAL_TENANT, LOCAL_DEVICE),
        (LOCAL_TENANT, "relocated-device-001"),
        (LOCAL_TENANT, "dev_" + "2" * 32),
        ("tnt_other_001", LOCAL_DEVICE),
        ("tenant.with:punct-1", "device.with:punct-1"),
    ]
    local_component = default_component_id_for_legacy_device(LOCAL_TENANT, LOCAL_DEVICE)
    events = [
        (LOCAL_TENANT, local_component, "water.temperature", "2026-07-09T12:00:00Z"),
        (LOCAL_TENANT, local_component, "device.rssi", "2026-07-09T12:00:00.750Z"),
        (LOCAL_TENANT, local_component, "water.ph", "2026-07-09T12:00:00.000001Z"),
        (LOCAL_TENANT, "cmp_" + "a1" * 16, "water.dissolved_oxygen", "1970-01-01T00:00:00Z"),
        ("tnt_other_001", "cmp_" + "a2" * 16, "water.salinity", "2261-12-31T23:59:59.999999Z"),
    ]
    return {
        "asset_id_for_legacy_pond": [
            {
                "tenant_id": tenant_id,
                "pond_id": pond_id,
                "asset_id": asset_id_for_legacy_pond(tenant_id, pond_id),
            }
            for tenant_id, pond_id in ponds
        ],
        "default_component_id_for_legacy_device": [
            {
                "tenant_id": tenant_id,
                "device_id": device_id,
                "component_id": default_component_id_for_legacy_device(tenant_id, device_id),
            }
            for tenant_id, device_id in devices
        ],
        "observation_event_id": [
            {
                "tenant_id": tenant_id,
                "component_id": component_id,
                "metric_key": metric_key,
                "observed_at": format_utc_timestamp(_utc(observed_at)),
                "event_id": observation_event_id(
                    tenant_id, component_id, metric_key, _utc(observed_at)
                ),
            }
            for tenant_id, component_id, metric_key, observed_at in events
        ],
    }


def render() -> str:
    document = {
        "versions": {
            "telemetry_schema_version": TELEMETRY_SCHEMA_VERSION,
            "metric_catalog_version": METRIC_CATALOG_VERSION,
            "unit_mapping_version": UNIT_MAPPING_VERSION,
            "quality_policy_version": QUALITY_POLICY_VERSION,
        },
        "measurements": {"observation": OBSERVATION_MEASUREMENT, "legacy": LEGACY_MEASUREMENT},
        "observation_tag_keys": sorted(OBSERVATION_TAG_KEYS),
        "observation_required_tag_keys": sorted(OBSERVATION_REQUIRED_TAG_KEYS),
        "observation_field_keys": sorted(OBSERVATION_FIELD_KEYS),
        "legacy_tag_keys": sorted(LEGACY_TAG_KEYS),
        "legacy_integer_fields": sorted(LEGACY_INTEGER_FIELDS),
        "metric_catalog": [definition.model_dump(mode="json") for definition in metric_catalog()],
        "id_vectors": _id_vectors(),
        "scenarios": {name: _scenario(name) for name in SCENARIOS},
    }
    return json.dumps(document, indent=2) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    rendered = render()
    if args.check:
        return int(not OUTPUT_PATH.exists() or OUTPUT_PATH.read_text(encoding="utf-8") != rendered)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
