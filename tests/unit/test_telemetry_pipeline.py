"""Unit tests for the ack-safe telemetry pipeline (P2-06): normalizer + dual-write, D9 acks."""

import asyncio
import hashlib
import json
from collections import Counter, deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from itertools import count
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pytest

from limnopulse_api.core.errors import TelemetryWriteError
from limnopulse_api.domain.assets import AssetWithPondProfile
from limnopulse_api.domain.entities import Device, Pond, Tenant
from limnopulse_api.domain.ids import default_component_id_for_legacy_device
from limnopulse_api.domain.legacy_projection import (
    asset_and_profile_for_legacy_pond,
    component_for_legacy_device,
    default_site_for_tenant,
    deployment_for_legacy_device,
    device_v2_from_legacy,
)
from limnopulse_api.domain.metric_catalog import METRIC_CATALOGS
from limnopulse_api.domain.telemetry_quality import QUALITY_POLICIES, QualityPolicy
from limnopulse_api.domain.telemetry_units import UNIT_MAPPINGS
from limnopulse_api.domain.telemetry_v2 import (
    MAX_OBSERVATIONS_PER_BATCH,
    CanonicalObservation,
    NormalizationResult,
    NormalizationVersions,
    QualityFlag,
    QualityState,
    QuarantineReason,
    SourceObservation,
    SourceType,
    TrustedSourceContext,
    observation_event_id,
)
from limnopulse_api.domain.telemetry_v2_schema import (
    LegacyPoint,
    encode_observation_line,
    observation_tags,
)
from limnopulse_api.domain.v2_common import IdempotencyReceipt, new_idempotency_receipt
from limnopulse_api.services.telemetry_normalizer import TelemetryNormalizer
from limnopulse_api.services.telemetry_pipeline import PipelineOutcome, TelemetryPipeline
from limnopulse_api.services.telemetry_v2_writes import (
    ObservationWriteService,
    TelemetryWriteOutcome,
)
from tests.contracts.m1_fakes import InMemoryM1Repositories

EXAMPLE_PATH = Path(__file__).resolve().parents[2] / "examples/telemetry/reading.local.json"
TENANT = "tenant-a"
DEVICE = "dev-legacy-1"
POND = "pond-west"
CREATED = datetime(2026, 1, 1, tzinfo=UTC)
FIRST_TS = datetime(2026, 7, 9, 12, 0, tzinfo=UTC)
INGESTED = datetime(2026, 7, 9, 12, 30, tzinfo=UTC)
COMPONENT = default_component_id_for_legacy_device(TENANT, DEVICE)
CLOSED_COUNT_KEYS = frozenset(NormalizationResult(accepted=(), quarantined=()).counts())

_receipts = count()


def receipt() -> IdempotencyReceipt:
    digest = hashlib.sha256(f"p2-06-pipeline-{next(_receipts)}".encode()).hexdigest()
    return new_idempotency_receipt(
        tenant_id=TENANT,
        scope="p2-06/pipeline",
        key_hash=digest,
        request_hash=digest,
        response_snapshot_json="{}",
        created_at=CREATED,
    )


async def _seed(repos: InMemoryM1Repositories) -> AssetWithPondProfile:
    tenant = Tenant(tenant_id=TENANT, name="A", created_at=CREATED, updated_at=CREATED, version=1)
    site = await repos.create_site(default_site_for_tenant(tenant), receipt())
    pond = Pond(
        tenant_id=TENANT,
        pond_id=POND,
        name="West",
        created_at=CREATED,
        updated_at=CREATED,
        version=1,
    )
    asset = asset_and_profile_for_legacy_pond(pond, site.site_id)
    await repos.create_asset(asset.asset, asset.pond_profile, receipt())
    device = Device(
        tenant_id=TENANT,
        pond_id=POND,
        device_id=DEVICE,
        name="Legacy",
        created_at=CREATED,
        updated_at=CREATED,
        version=1,
    )
    await repos.create_device(device_v2_from_legacy(device), receipt())
    await repos.create_component(component_for_legacy_device(device), receipt())
    await repos.start_deployment(
        deployment_for_legacy_device(device, asset.asset.asset_id, site.site_id), receipt()
    )
    return asset


# In-memory InfluxDB stand-in with fault injection ------------------------------------------------


@dataclass
class PointStore:
    """Last writer wins per series key and time; legacy fields merge per field, like InfluxDB."""

    v2: dict[tuple[tuple[tuple[str, str], ...], datetime], str] = field(default_factory=dict)
    v2_observations: dict[tuple[tuple[tuple[str, str], ...], datetime], CanonicalObservation] = (
        field(default_factory=dict)
    )
    legacy: dict[tuple[str, str, str, datetime], dict[str, float | int]] = field(
        default_factory=dict
    )

    def snapshot(self) -> tuple[dict[Any, str], dict[Any, dict[str, float | int]]]:
        return dict(self.v2), {key: dict(value) for key, value in self.legacy.items()}


class FaultyObservationWriter:
    """Writes in chunks; `fail_at_chunk` raises before that chunk, after earlier chunks landed."""

    def __init__(self, store: PointStore, *, chunk_size: int = 3) -> None:
        self.store = store
        self.chunk_size = chunk_size
        self.fail_at_chunk: int | None = None
        self.error: BaseException = TelemetryWriteError("influx unavailable", retryable=True)
        self.calls = 0

    async def write_observations(self, observations: Sequence[CanonicalObservation]) -> None:
        self.calls += 1
        for index, start in enumerate(range(0, len(observations), self.chunk_size)):
            if self.fail_at_chunk is not None and index >= self.fail_at_chunk:
                raise self.error
            for o in observations[start : start + self.chunk_size]:
                key = (tuple(sorted(observation_tags(o).items())), o.observation.observed_at)
                self.store.v2[key] = encode_observation_line(o)
                self.store.v2_observations[key] = o


class FaultyLegacyWriter:
    def __init__(self, store: PointStore) -> None:
        self.store = store
        self.error: BaseException | None = None

    async def write_legacy_points(self, points: Sequence[LegacyPoint]) -> None:
        if self.error is not None:
            raise self.error
        for p in points:
            key = (p.tenant_id, p.pond_id, p.device_id, p.observed_at)
            self.store.legacy.setdefault(key, {}).update(p.fields)


class FaultySiteAssets:
    def __init__(self, inner: InMemoryM1Repositories) -> None:
        self.inner = inner
        self.error: BaseException | None = None

    async def get_site(self, tenant_id: str, site_id: str) -> Any:
        return await self.inner.get_site(tenant_id, site_id)

    async def get_asset(self, tenant_id: str, asset_id: str) -> AssetWithPondProfile | None:
        if self.error is not None:
            raise self.error
        return await self.inner.get_asset(tenant_id, asset_id)


@dataclass
class Harness:
    repos: InMemoryM1Repositories
    store: PointStore
    writer: FaultyObservationWriter
    legacy_writer: FaultyLegacyWriter
    site_assets: FaultySiteAssets
    pipeline: TelemetryPipeline

    def clear_faults(self) -> None:
        self.writer.fail_at_chunk = None
        self.writer.error = TelemetryWriteError("influx unavailable", retryable=True)
        self.legacy_writer.error = None
        self.site_assets.error = None


def build_harness(
    repos: InMemoryM1Repositories | None = None,
    store: PointStore | None = None,
    **normalizer_kwargs: Any,
) -> Harness:
    if repos is None:
        repos = InMemoryM1Repositories(clock=lambda: CREATED)
        asyncio.run(_seed(repos))
    store = store or PointStore()
    writer = FaultyObservationWriter(store)
    legacy_writer = FaultyLegacyWriter(store)
    site_assets = FaultySiteAssets(repos)
    service = ObservationWriteService(
        writer=writer,
        legacy_writer=legacy_writer,
        site_assets=site_assets,
        v2_write_enabled=True,
        legacy_dual_write_enabled=True,
    )
    normalizer = TelemetryNormalizer(
        components=repos, deployments=repos, clock=lambda: INGESTED, **normalizer_kwargs
    )
    return Harness(
        repos=repos,
        store=store,
        writer=writer,
        legacy_writer=legacy_writer,
        site_assets=site_assets,
        pipeline=TelemetryPipeline(normalizer=normalizer, write_service=service),
    )


@pytest.fixture
def harness() -> Harness:
    """Built in synchronous setup: seeding runs its own event loop."""
    return build_harness()


# At-least-once queue fake and a consumer that follows the ack contract ---------------------------


@dataclass
class Message:
    message_id: str
    context: TrustedSourceContext  # first-receipt context, carried unchanged on redelivery (D5)
    payload: Mapping[str, Any]


class AtLeastOnceQueue:
    def __init__(self, messages: Sequence[Message]) -> None:
        self.pending: deque[Message] = deque(messages)
        self.acked: list[str] = []
        self.deliveries: Counter[str] = Counter()

    def receive(self) -> Message | None:
        if not self.pending:
            return None
        message = self.pending.popleft()
        self.deliveries[message.message_id] += 1
        return message

    def ack(self, message: Message) -> None:
        self.acked.append(message.message_id)

    def nack(self, message: Message) -> None:
        self.pending.append(message)


async def consume_one(
    queue: AtLeastOnceQueue, pipeline: TelemetryPipeline
) -> PipelineOutcome | BaseException:
    message = queue.receive()
    assert message is not None
    try:
        outcome = await pipeline.ingest_legacy_reading(message.context, message.payload)
    except Exception as exc:  # noqa: BLE001 - the consumer redelivers on any failure
        queue.nack(message)
        return exc
    queue.ack(message)
    return outcome


async def drain(queue: AtLeastOnceQueue, pipeline: TelemetryPipeline) -> dict[str, PipelineOutcome]:
    outcomes: dict[str, PipelineOutcome] = {}
    while queue.pending:
        message_id = queue.pending[0].message_id
        result = await consume_one(queue, pipeline)
        assert isinstance(result, PipelineOutcome), result
        outcomes[message_id] = result
    return outcomes


def example(**changes: Any) -> dict[str, Any]:
    return json.loads(EXAMPLE_PATH.read_text()) | changes


def legacy_context(received_at: datetime, **changes: Any) -> TrustedSourceContext:
    values: dict[str, Any] = {
        "tenant_id": TENANT,
        "device_id": DEVICE,
        "integration_id": None,
        "integration_type": "legacy_mqtt",
        "source_type": SourceType.LEGACY_MQTT,
        "connector_version": None,
        "received_at": received_at,
    }
    values.update(changes)
    return TrustedSourceContext(**values)


def messages() -> list[Message]:
    second_ts = FIRST_TS + timedelta(minutes=1)
    return [
        Message("m1", legacy_context(FIRST_TS + timedelta(seconds=2)), example()),
        Message(
            "m2",
            legacy_context(second_ts + timedelta(seconds=2)),
            example(ts="2026-07-09T12:01:00Z", seq=2, temp_c=25.3),
        ),
    ]


def baseline() -> tuple[PointStore, dict[str, PipelineOutcome]]:
    harness = build_harness()
    queue = AtLeastOnceQueue(messages())
    outcomes = asyncio.run(drain(queue, harness.pipeline))
    assert queue.acked == ["m1", "m2"]
    assert len(harness.store.v2) == 14
    assert len(harness.store.legacy) == 2
    return harness.store, outcomes


# Happy path ------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_example_reading_is_written_to_v2_and_legacy(harness: Harness) -> None:

    outcome = await harness.pipeline.ingest_legacy_reading(
        legacy_context(FIRST_TS + timedelta(seconds=2)), example()
    )

    assert outcome.written == TelemetryWriteOutcome(v2_points=7, legacy_points=1, legacy_excluded=0)
    assert tuple(outcome.quarantined) == ()
    assert outcome.counts["accepted"] == 7
    assert outcome.counts["state.valid"] == 7
    assert set(outcome.counts) <= CLOSED_COUNT_KEYS  # closed keys, no tenant/device labels
    assert (
        sum(value for key, value in outcome.counts.items() if key.startswith("quarantined.")) == 0
    )
    stored = list(harness.store.v2_observations.values())
    assert {o.resolved.component_id for o in stored} == {COMPONENT}
    assert {o.observation.sequence for o in stored} == {1}
    assert list(harness.store.legacy) == [(TENANT, POND, DEVICE, FIRST_TS)]
    assert harness.store.legacy[(TENANT, POND, DEVICE, FIRST_TS)]["seq"] == 1


@pytest.mark.asyncio
async def test_mapper_and_normalizer_quarantines_are_both_reported(harness: Harness) -> None:
    payload = example(ph="bad", rssi=-67.5, seq=True)

    outcome = await harness.pipeline.ingest_legacy_reading(
        legacy_context(FIRST_TS + timedelta(seconds=2)), payload
    )

    reasons = Counter(q.reason for q in outcome.quarantined)
    assert reasons == Counter({QuarantineReason.VALUE_INVALID: 3})
    assert outcome.counts["quarantined.value_invalid"] == 3
    assert outcome.counts["accepted"] == 5
    assert set(outcome.counts) <= CLOSED_COUNT_KEYS  # closed keys, no tenant/device labels
    assert outcome.written.v2_points == 5
    assert outcome.written.legacy_points == 1
    assert {o.observation.sequence for o in harness.store.v2_observations.values()} == {None}


@pytest.mark.asyncio
async def test_reading_level_quarantine_writes_nothing(harness: Harness) -> None:

    outcome = await harness.pipeline.ingest_legacy_reading(
        legacy_context(FIRST_TS + timedelta(seconds=2)), example(ts="2026-07-09T12:00:00")
    )

    assert [q.reason for q in outcome.quarantined] == [QuarantineReason.OBSERVED_AT_INVALID]
    assert outcome.counts["quarantined.observed_at_invalid"] == 1
    assert outcome.counts["accepted"] == 0
    assert outcome.written.v2_points == 0
    assert outcome.written.legacy_points == 0
    assert harness.store.v2 == {}
    assert harness.store.legacy == {}


@pytest.mark.asyncio
async def test_ingest_writes_only_accepted_observations_and_reports_quarantines(
    harness: Harness,
) -> None:
    received = FIRST_TS + timedelta(seconds=2)
    good = SourceObservation(
        component_id=COMPONENT,
        metric_key="water.ph",
        value=7.2,
        source_unit="pH",
        observed_at=FIRST_TS,
    )
    unowned = good.model_copy(
        update={"component_id": default_component_id_for_legacy_device(TENANT, "dev-other")}
    )
    unresolved = good.model_copy(update={"observed_at": CREATED - timedelta(days=1)})

    outcome = await harness.pipeline.ingest(legacy_context(received), [good, unowned, unresolved])

    assert outcome.written == TelemetryWriteOutcome(v2_points=1, legacy_points=1, legacy_excluded=0)
    assert Counter(q.reason for q in outcome.quarantined) == Counter(
        {QuarantineReason.COMPONENT_NOT_OWNED: 1, QuarantineReason.DEPLOYMENT_UNRESOLVED: 1}
    )
    assert outcome.counts["accepted"] == 1
    assert outcome.counts["quarantined.component_not_owned"] == 1
    assert outcome.counts["quarantined.deployment_unresolved"] == 1
    assert [o.event_id for o in harness.store.v2_observations.values()] == [
        observation_event_id(TENANT, COMPONENT, "water.ph", FIRST_TS)
    ]


@pytest.mark.asyncio
async def test_unknown_pinned_version_propagates_and_writes_nothing(harness: Harness) -> None:
    pinned = legacy_context(
        FIRST_TS + timedelta(seconds=2),
        replay=True,
        mapping_versions=NormalizationVersions(catalog_version="metric-catalog@9"),
    )

    with pytest.raises(ValueError):
        await harness.pipeline.ingest_legacy_reading(pinned, example())

    assert harness.writer.calls == 0
    assert harness.store.v2 == {}


class RecordingWriteService:
    """Mirrors `ObservationWriteService.write`; records every call."""

    def __init__(self) -> None:
        self.calls: list[Sequence[CanonicalObservation]] = []

    async def write(self, observations: Sequence[CanonicalObservation]) -> TelemetryWriteOutcome:
        self.calls.append(observations)
        return TelemetryWriteOutcome(v2_points=0, legacy_points=0, legacy_excluded=0)


@pytest.mark.asyncio
async def test_over_limit_batch_raises_value_error_before_any_write(harness: Harness) -> None:
    write_service = RecordingWriteService()
    pipeline = TelemetryPipeline(
        normalizer=TelemetryNormalizer(
            components=harness.repos, deployments=harness.repos, clock=lambda: INGESTED
        ),
        write_service=write_service,
    )
    source = SourceObservation(
        component_id=COMPONENT,
        metric_key="water.ph",
        value=7.2,
        source_unit="pH",
        observed_at=FIRST_TS,
    )

    # P3 consumer contract: ValueError is caller misuse; TelemetryWriteError is a write failure.
    with pytest.raises(ValueError) as raised:
        await pipeline.ingest(
            legacy_context(FIRST_TS + timedelta(seconds=2)),
            [source] * (MAX_OBSERVATIONS_PER_BATCH + 1),
        )

    assert not isinstance(raised.value, TelemetryWriteError)
    assert write_service.calls == []


# Ack contract (D9): failed writes are never acknowledged; redelivery recovers -------------------


def inject_v2_failure(harness: Harness) -> None:
    harness.writer.fail_at_chunk = 0


def inject_legacy_failure(harness: Harness) -> None:
    harness.legacy_writer.error = TelemetryWriteError("legacy unavailable", retryable=True)


def inject_mid_chunk_failure(harness: Harness) -> None:
    harness.writer.fail_at_chunk = 1


def inject_permanent_failure(harness: Harness) -> None:
    harness.writer.fail_at_chunk = 1
    harness.writer.error = RuntimeError("unclassified writer bug")


def inject_asset_lookup_failure(harness: Harness) -> None:
    harness.site_assets.error = ConnectionError("dynamodb unavailable")


# fault -> (injector, expected retryable, v2 points stored after m1, m2, m1 all failed).
# Chunks are 3 lines, so a mid-chunk failure leaves the first chunk of each reading stored.
FAULTS = {
    "v2-fails": (inject_v2_failure, True, 0),
    "legacy-fails-after-v2": (inject_legacy_failure, True, 14),
    "mid-chunk": (inject_mid_chunk_failure, True, 6),
    "permanent": (inject_permanent_failure, False, 6),
    "asset-lookup-fails": (inject_asset_lookup_failure, True, 14),
}


@pytest.mark.parametrize("fault", sorted(FAULTS))
def test_failed_write_is_never_acknowledged_and_redelivery_matches_the_baseline(
    fault: str,
) -> None:
    inject, retryable, partial_v2 = FAULTS[fault]
    expected_store, expected_outcomes = baseline()
    harness = build_harness()
    queue = AtLeastOnceQueue(messages())
    inject(harness)

    failures = [asyncio.run(consume_one(queue, harness.pipeline)) for _ in range(3)]

    assert all(isinstance(item, TelemetryWriteError) for item in failures), failures
    assert {item.retryable for item in failures if isinstance(item, TelemetryWriteError)} == {
        retryable
    }
    assert queue.acked == []
    assert queue.deliveries["m1"] == 2 and queue.deliveries["m2"] == 1
    assert len(harness.store.v2) == partial_v2
    assert harness.store.legacy == {}

    harness.clear_faults()
    outcomes = asyncio.run(drain(queue, harness.pipeline))

    assert sorted(queue.acked) == ["m1", "m2"]
    assert harness.store.snapshot() == expected_store.snapshot()
    assert outcomes.keys() == expected_outcomes.keys()
    for message_id, outcome in outcomes.items():
        expected = expected_outcomes[message_id]
        assert outcome.written == expected.written
        assert dict(outcome.counts) == dict(expected.counts)
        assert tuple(outcome.quarantined) == tuple(expected.quarantined)


@pytest.mark.asyncio
async def test_write_error_propagates_unchanged_from_ingest(harness: Harness) -> None:
    error = TelemetryWriteError("influx unavailable", retryable=True)
    harness.writer.fail_at_chunk = 0
    harness.writer.error = error

    with pytest.raises(TelemetryWriteError) as raised:
        await harness.pipeline.ingest_legacy_reading(
            legacy_context(FIRST_TS + timedelta(seconds=2)), example()
        )

    assert raised.value is error


@pytest.mark.asyncio
async def test_legacy_failure_after_v2_propagates_from_ingest(harness: Harness) -> None:
    error = TelemetryWriteError("legacy down", retryable=False)
    harness.legacy_writer.error = error
    source = SourceObservation(
        component_id=COMPONENT,
        metric_key="water.ph",
        value=7.2,
        source_unit="pH",
        observed_at=FIRST_TS,
    )

    with pytest.raises(TelemetryWriteError) as raised:
        await harness.pipeline.ingest(legacy_context(FIRST_TS + timedelta(seconds=2)), [source])

    assert raised.value is error
    assert len(harness.store.v2) == 1  # v2 landed first; the caller must still not acknowledge
    assert harness.store.legacy == {}


def test_failed_ingest_is_never_acknowledged_and_redelivery_matches_the_baseline() -> None:
    received = FIRST_TS + timedelta(seconds=2)
    batch = [
        SourceObservation(
            component_id=COMPONENT,
            metric_key=key,
            value=value,
            source_unit=unit,
            observed_at=FIRST_TS,
            sequence=4,
        )
        for key, value, unit in [
            ("water.ph", 7.2, "pH"),
            ("water.temperature", 77.0, "degF"),
            ("device.battery_voltage", 3820, "mV"),
            ("device.rssi", -67, "dBm"),
        ]
    ]

    async def consume(harness: Harness, queue: deque[list[SourceObservation]]) -> bool:
        message = queue.popleft()
        try:
            await harness.pipeline.ingest(legacy_context(received), message)
        except TelemetryWriteError:
            queue.append(message)
            return False
        return True

    expected = build_harness()
    assert asyncio.run(consume(expected, deque([batch]))) is True
    harness = build_harness()
    harness.writer.fail_at_chunk = 1
    queue = deque([batch])

    acked = asyncio.run(consume(harness, queue))

    assert acked is False
    assert len(queue) == 1
    assert len(harness.store.v2) == 3
    harness.clear_faults()
    assert asyncio.run(consume(harness, queue)) is True
    assert harness.store.snapshot() == expected.store.snapshot()
    assert len(harness.store.v2) == 4


# ADR-005 end to end: a pinned replay after a catalog revision restores the original mapping -------

V1 = NormalizationVersions()
V2 = NormalizationVersions(
    catalog_version="metric-catalog@2",
    unit_mapping_version="unit-mapping@2",
    quality_policy_version="quality-policy@2",
)


def revised_registries() -> dict[str, Any]:
    catalog = tuple(
        d.model_copy(update={"plausible_max": 25.0}) if d.metric_key == "water.temperature" else d
        for d in METRIC_CATALOGS["metric-catalog@1"]
    )
    units = dict(UNIT_MAPPINGS["unit-mapping@1"])
    units["water.temperature"] = MappingProxyType({"degC": lambda value: float(value) + 0.5})
    policy = QualityPolicy(
        delay_threshold=timedelta(seconds=10),
        future_skew_threshold=timedelta(seconds=300),
        past_extreme_skew_threshold=timedelta(days=30),
    )
    return {
        "catalogs": MappingProxyType({**METRIC_CATALOGS, "metric-catalog@2": catalog}),
        "unit_mappings": MappingProxyType(
            {**UNIT_MAPPINGS, "unit-mapping@2": MappingProxyType(units)}
        ),
        "policies": MappingProxyType({**QUALITY_POLICIES, "quality-policy@2": policy}),
    }


def test_pinned_replay_through_the_pipeline_restores_the_v1_point_in_the_same_series() -> None:
    original = build_harness(**revised_registries(), default_versions=V1)
    revised = build_harness(
        original.repos, original.store, **revised_registries(), default_versions=V2
    )
    first_context = legacy_context(FIRST_TS + timedelta(seconds=60))
    source = SourceObservation(
        component_id=COMPONENT,
        metric_key="water.temperature",
        value=25.1,
        source_unit="degC",
        observed_at=FIRST_TS,
        sequence=1,
    )
    event_id = observation_event_id(TENANT, COMPONENT, "water.temperature", FIRST_TS)

    asyncio.run(original.pipeline.ingest(first_context, [source]))
    (key,) = original.store.v2_observations
    first = original.store.v2_observations[key]
    asyncio.run(revised.pipeline.ingest(first_context, [source]))
    overwritten = original.store.v2_observations[key]
    replay_context = first_context.model_copy(update={"replay": True, "mapping_versions": V1})
    outcome = asyncio.run(revised.pipeline.ingest(replay_context, [source]))
    replayed = original.store.v2_observations[key]

    assert list(original.store.v2_observations) == [key]
    assert first.mapping == V1 and first.event_id == event_id
    assert overwritten.mapping == V2 and overwritten.event_id == event_id
    assert overwritten.quality.state is QualityState.OUT_OF_RANGE
    assert replayed.mapping == V1
    assert replayed.event_id == event_id
    assert replayed.observation.value == first.observation.value == 25.1
    assert replayed.quality.state is first.quality.state is QualityState.VALID
    assert replayed.quality.flags == (QualityFlag.REPLAY,)
    assert replayed.source.received_at == first.source.received_at
    assert outcome.written.v2_points == 1
    assert outcome.counts["flag.replay"] == 1
    assert len(original.store.legacy) == 1
