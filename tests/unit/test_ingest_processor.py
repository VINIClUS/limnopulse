"""P3-08: `IngestMessageProcessor` — the D8 order, the worker failure table, fences and metrics.

The world is the P3-00 fakes (trusted mapping, quarantine, ingest state) plus the real P2
`TelemetryPipeline` over the in-memory M1 repositories and a recording point writer, so the
context, the event ids and the normalizer quarantine are the production ones.
"""

from __future__ import annotations

import asyncio
import copy
import inspect
import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import pytest

from limnopulse_api.core.errors import TelemetryWriteError
from limnopulse_api.domain.devices_v2 import DeviceStatus
from limnopulse_api.domain.ids import (
    default_component_id_for_legacy_device,
    quarantine_entry_id,
)
from limnopulse_api.domain.ingestion import (
    INGEST_METRICS,
    PERMANENT_VISIBILITY_SECONDS,
    IngressDeferReason,
    IngressEnvelope,
    IngressFailureCategory,
    IngressFence,
    IngressFenceReason,
    IngressQueueMessage,
    QuarantineEntryKind,
    TrustedIngress,
    build_quarantine_entries,
    build_trusted_context,
    ingress_backoff_seconds,
)
from limnopulse_api.domain.integrations import (
    DeviceIntegrationStatus,
    IntegrationAccountStatus,
)
from limnopulse_api.domain.telemetry_v2 import (
    CanonicalObservation,
    NormalizationVersions,
    QualityFlag,
    QuarantineReason,
    TrustedSourceContext,
)
from limnopulse_api.services.ingest_processor import (
    IngestMessageProcessor,
    ProcessDecision,
    RetryPolicy,
)
from limnopulse_api.services.ingest_state import IngestStateService
from limnopulse_api.services.telemetry_normalizer import TelemetryNormalizer
from limnopulse_api.services.telemetry_pipeline import PipelineOutcome, TelemetryPipeline
from limnopulse_api.services.telemetry_v2_writes import ObservationWriteService
from tests.contracts.m1_fakes import InMemoryM1Repositories
from tests.contracts.test_p2_acceptance_gate import seed_legacy_device
from tests.support.ingestion_contract_kits import (
    ACCOUNT,
    DEFAULT_COMPONENT,
    DEVICE,
    IDENTITY_HASH,
    INGRESS,
    INTEGRATION,
    OBSERVED,
    OTHER_DEVICE,
    OTHER_INTEGRATION,
    RECEIVED,
    TENANT,
    device_integration,
    envelope,
    integration_account,
    source_observation,
    store_errors,
)
from tests.support.ingestion_contract_kits import device as kit_device
from tests.support.ingestion_fakes import (
    FakeIngestStateRepository,
    FakeQuarantineRepository,
    FakeTrustedMapping,
    KitSubject,
    RecordingTelemetryMetrics,
)

pytestmark = pytest.mark.asyncio

NOW = RECEIVED + timedelta(seconds=30)
POND = "pond-west"
FOREIGN_COMPONENT = default_component_id_for_legacy_device(TENANT, OTHER_DEVICE)
RECEIPT_HANDLE = "rh-SECRET-receipt-handle-0001"
REPLAY_OF = "ing_" + "9" * 32
HALF = 0.5
# Non-default policy numbers, so an implementation that ignores the injected policy fails.
BASE, CAP, PERMANENT = 7, 50, 61
WORKER_METRICS = frozenset(
    {
        "ingress_messages_total",
        "ingress_observations_total",
        "ingress_quarantined_total",
        "ingress_fenced_total",
        "ingress_deferred_total",
        "ingress_quality_flags_total",
        "ingress_sequence_events_total",
        "ingress_write_failures_total",
        "ingress_normalization_duration_seconds",
        "ingress_queue_age_seconds",
    }
)
IDENTIFIERS = (
    TENANT,
    DEVICE,
    ACCOUNT,
    INTEGRATION,
    DEFAULT_COMPONENT,
    INGRESS,
    IDENTITY_HASH,
    RECEIPT_HANDLE,
)
TERMINAL_FENCES = {
    IngressFenceReason.IDENTITY_UNRESOLVED: {"seed_integration": False},
    IngressFenceReason.INTEGRATION_MISMATCH: {
        "envelope": {"device_integration_id": OTHER_INTEGRATION}
    },
    IngressFenceReason.INTEGRATION_DECOMMISSIONED: {
        "integration": {"status": DeviceIntegrationStatus.DECOMMISSIONED}
    },
    IngressFenceReason.ACCOUNT_REVOKED: {"account": {"status": IntegrationAccountStatus.REVOKED}},
    IngressFenceReason.DEVICE_DECOMMISSIONED: {"device": {"status": DeviceStatus.DECOMMISSIONED}},
}
TEMPORARY_FENCES = [
    (IngressDeferReason.INTEGRATION_NOT_ACTIVE, {"integration": {"status": "provisioning"}}),
    (IngressDeferReason.INTEGRATION_NOT_ACTIVE, {"integration": {"status": "error"}}),
    (IngressDeferReason.ACCOUNT_NOT_ACTIVE, {"account": {"status": "pending"}}),
    (IngressDeferReason.ACCOUNT_NOT_ACTIVE, {"account": {"status": "error"}}),
    (IngressDeferReason.DEVICE_NOT_ACTIVE, {"device": {"status": "provisioning"}}),
    (IngressDeferReason.DEVICE_NOT_ACTIVE, {"device": {"status": "suspended"}}),
]


# World ---------------------------------------------------------------------------------------


def reading(**overrides: Any) -> Any:
    """A SourceObservation the P2 normalizer accepts for the legacy default Component."""
    payload: dict[str, Any] = {
        "component_id": DEFAULT_COMPONENT,
        "metric_key": "water.temperature",
        "source_unit": "degC",
    }
    payload.update(overrides)
    return source_observation(**payload)


def message_for(
    item: IngressEnvelope | str, *, receive_count: int = 1, message_id: str = "msg-1"
) -> IngressQueueMessage:
    body = item if isinstance(item, str) else item.to_json()
    return IngressQueueMessage(
        message_id=message_id,
        receipt_handle=RECEIPT_HANDLE,
        body=body,
        receive_count=receive_count,
        sent_at=RECEIVED,
    )


class Spy:
    """Wraps a port; records `<label>.<method>` for every coroutine call, then delegates."""

    def __init__(self, inner: Any, events: list[str], label: str) -> None:
        self._inner = inner
        self._events = events
        self._label = label

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self._inner, name)
        if not inspect.iscoroutinefunction(attribute):
            return attribute

        async def call(*args: Any, **kwargs: Any) -> Any:
            self._events.append(f"{self._label}.{name}")
            return await attribute(*args, **kwargs)

        return call


class PipelineSpy:
    """The real pipeline, recording each context; `fail_with` raises instead of ingesting."""

    def __init__(self, inner: TelemetryPipeline, events: list[str]) -> None:
        self.inner = inner
        self.events = events
        self.contexts: list[TrustedSourceContext] = []
        self.outcomes: list[PipelineOutcome] = []
        self.fail_with: BaseException | None = None

    async def ingest(
        self, context: TrustedSourceContext, observations: Sequence[Any]
    ) -> PipelineOutcome:
        self.events.append("pipeline.ingest")
        self.contexts.append(context)
        if self.fail_with is not None:
            raise self.fail_with
        outcome = await self.inner.ingest(context, observations)
        self.outcomes.append(outcome)
        return outcome


class RecordingWriter:
    def __init__(self) -> None:
        self.points: list[CanonicalObservation] = []

    async def write_observations(self, observations: Sequence[CanonicalObservation]) -> None:
        self.points.extend(observations)


@dataclass
class World:
    subject: KitSubject
    repos: InMemoryM1Repositories
    writer: RecordingWriter
    pipeline: PipelineSpy
    metrics: RecordingTelemetryMetrics
    processor: IngestMessageProcessor
    events: list[str] = field(default_factory=list)

    @property
    def quarantine_rows(self) -> list[Any]:
        return list(self.subject.store.quarantine.values())

    @property
    def wrote_state(self) -> bool:
        store = self.subject.store
        return bool(store.component_states or store.device_states or store.connector_health)

    def fail_next(self, operation: str, exc: BaseException) -> None:
        self.subject.fail_next(operation, exc)

    def metric(self, name: str) -> list[dict[str, Any]]:
        return [record for record in self.metrics.records if record["metric"] == name]

    def count(self, name: str, **labels: str) -> float:
        return sum(record["value"] for record in self.metric(name) if record["labels"] == labels)


async def make_world(
    *,
    random: Callable[[], float] = lambda: HALF,
    seed_integration: bool = True,
    seed_account: bool = True,
    integration: dict[str, Any] | None = None,
    account: dict[str, Any] | None = None,
    device: dict[str, Any] | None = None,
    wrap_mapping: Callable[[Any], Any] | None = None,
) -> World:
    events: list[str] = []
    subject = KitSubject(
        mapping=lambda s: FakeTrustedMapping(s.store),
        quarantine=lambda s: FakeQuarantineRepository(s.store),
        state=lambda s: FakeIngestStateRepository(s.store),
    )
    subject.set_now(NOW)
    if seed_account:
        await subject.seed_account(integration_account(**(account or {})))
    if seed_integration:
        await subject.seed_device_integration(device_integration(**(integration or {})))
    await subject.seed_device(kit_device(**(device or {})))
    repos = InMemoryM1Repositories(clock=lambda: NOW)
    await seed_legacy_device(repos, tenant_id=TENANT, device_id=DEVICE, pond_id=POND)
    await seed_legacy_device(repos, tenant_id=TENANT, device_id=OTHER_DEVICE, pond_id=POND)
    writer = RecordingWriter()
    pipeline = PipelineSpy(
        TelemetryPipeline(
            normalizer=TelemetryNormalizer(components=repos, deployments=repos, clock=lambda: NOW),
            write_service=ObservationWriteService(
                writer=writer,
                legacy_writer=None,
                site_assets=repos,
                v2_write_enabled=True,
                legacy_dual_write_enabled=False,
            ),
        ),
        events,
    )
    metrics = RecordingTelemetryMetrics()
    processor = IngestMessageProcessor(
        mapping=Spy(
            subject.mapping if wrap_mapping is None else wrap_mapping(subject.mapping),
            events,
            "mapping",
        ),
        pipeline=pipeline,
        quarantine=Spy(subject.quarantine, events, "quarantine"),
        state=IngestStateService(Spy(subject.state, events, "state"), clock=lambda: NOW),
        metrics=metrics,
        clock=lambda: NOW,
        retry=RetryPolicy(base=BASE, cap=CAP, permanent_visibility=PERMANENT, random=random),
    )
    return World(subject, repos, writer, pipeline, metrics, processor, events)


def backoff(receive_count: int) -> int:
    return ingress_backoff_seconds(receive_count, base=BASE, cap=CAP, random=lambda: HALF)


def assert_decision(
    decision: ProcessDecision,
    *,
    action: str,
    category: IngressFailureCategory,
    seconds: int | None = None,
) -> None:
    assert isinstance(decision, ProcessDecision)
    assert decision.action == action
    assert decision.category == category
    if action == "visibility":
        assert decision.seconds == seconds


def assert_nothing_written(world: World) -> None:
    assert world.quarantine_rows == []
    assert not world.wrote_state
    assert world.writer.points == []


def fenced_world_options(reason: IngressFenceReason) -> tuple[dict[str, Any], dict[str, Any]]:
    options = dict(TERMINAL_FENCES[reason])
    return options, options.pop("envelope", {})


# Backoff -------------------------------------------------------------------------------------


def _formula(n: int, *, base: int, cap: int, value: float) -> int:
    return min(max(math.ceil(value * min(cap, base * 2 ** (n - 1))), 1), 43_200)


@pytest.mark.parametrize("value", [0.0, 0.01, 0.37, HALF, 0.999, 1.0])
@pytest.mark.parametrize("n", range(1, 11))
async def test_backoff_matches_the_formula_with_an_injected_random_source(
    n: int, value: float
) -> None:
    policy = RetryPolicy(base=30, cap=900, permanent_visibility=60, random=lambda: value)

    assert policy.backoff(n) == _formula(n, base=30, cap=900, value=value)


async def test_backoff_clamps_to_one_second_and_to_twelve_hours() -> None:
    assert (
        RetryPolicy(base=30, cap=900, permanent_visibility=60, random=lambda: 0.0).backoff(1) == 1
    )
    huge = RetryPolicy(base=40_000, cap=100_000, permanent_visibility=60, random=lambda: 1.0)
    assert huge.backoff(3) == 43_200


async def test_backoff_draws_the_random_source_per_call() -> None:
    draws = iter([0.25, 0.75])
    policy = RetryPolicy(base=30, cap=900, permanent_visibility=60, random=lambda: next(draws))

    assert [policy.backoff(4), policy.backoff(4)] == [60, 180]


async def test_retry_policy_defaults_follow_the_failure_table() -> None:
    policy = RetryPolicy()

    assert (policy.base, policy.cap, policy.permanent_visibility) == (
        30,
        900,
        PERMANENT_VISIBILITY_SECONDS,
    )
    assert 1 <= policy.backoff(1) <= 30


# Failure table: success and envelope_invalid ---------------------------------------------------


async def test_a_trusted_message_is_processed_and_deleted() -> None:
    world = await make_world()
    item = envelope(observations=[reading(sequence=1)])

    decision = await world.processor.process(message_for(item))

    assert_decision(decision, action="delete", category=IngressFailureCategory.PROCESSED)
    assert [point.observation.metric_key for point in world.writer.points] == ["water.temperature"]
    assert world.quarantine_rows == []  # nothing quarantined: no row, no summary row


@pytest.mark.parametrize(
    "body",
    [
        "not json",
        "\udcff",
        "{}",
        "[]",
        '{"schema":"telemetry-ingress@2"}',
        envelope(observations=[reading()]).to_json().replace("telemetry-ingress@1", "other@1"),
        envelope(observations=[reading()]).to_json()[:-1],
        envelope(observations=[reading()]).to_json().replace('"tenant_id"', '"tenant"'),
    ],
    ids=[
        "not-json",
        "lone-surrogate",
        "empty-object",
        "array",
        "unknown-schema-only",
        "unknown-schema",
        "truncated",
        "renamed-field",
    ],
)
async def test_an_invalid_envelope_is_permanent_and_touches_nothing(body: str) -> None:
    world = await make_world()

    decision = await world.processor.process(message_for(body))

    assert_decision(
        decision,
        action="visibility",
        category=IngressFailureCategory.ENVELOPE_INVALID,
        seconds=PERMANENT,
    )
    assert world.events == []  # no resolve, no pipeline, no write
    assert_nothing_written(world)


# Failure table: fences --------------------------------------------------------------------------


@pytest.mark.parametrize("reason", list(TERMINAL_FENCES), ids=lambda r: r.value)
async def test_a_verified_terminal_fence_persists_replayable_rows_then_deletes(
    reason: IngressFenceReason,
) -> None:
    options, envelope_overrides = fenced_world_options(reason)
    world = await make_world(**options)
    first, second = reading(sequence=1), reading(sequence=2, value=28.0)
    item = envelope(observations=[first, second, first], **envelope_overrides)

    decision = await world.processor.process(message_for(item))

    assert_decision(decision, action="delete", category=IngressFailureCategory.FENCED)
    expected = build_quarantine_entries(
        item, fence=IngressFence(reason=reason, account_verified=True), now=NOW
    )
    rows = sorted(world.quarantine_rows, key=lambda row: row.quarantine_id)
    assert rows == sorted(expected, key=lambda row: row.quarantine_id)
    observations = [row for row in rows if row.kind == QuarantineEntryKind.FENCED_OBSERVATION]
    assert len(observations) == 3
    assert all(row.replayable for row in observations)
    assert {row.reason for row in rows} == {f"fenced_{reason.value}"}
    assert len({row.quarantine_id for row in rows}) == 4  # identical excerpts, distinct ordinals
    (summary,) = [row for row in rows if row.kind == QuarantineEntryKind.FENCED_MESSAGE]
    assert summary.quarantine_id == quarantine_entry_id(
        item.ingress_id, -1, summary.envelope_excerpt_json
    )
    assert world.writer.points == []
    assert "pipeline.ingest" not in world.events
    store = world.subject.store
    assert store.component_states == {} and store.device_states == {}
    assert store.connector_health[ACCOUNT]["last_fence_reason"] == reason.value
    assert world.count("ingress_fenced_total", reason=reason.value) == 1


async def test_fence_rows_are_persisted_before_the_fence_mark() -> None:
    world = await make_world(integration={"status": DeviceIntegrationStatus.DECOMMISSIONED})

    await world.processor.process(message_for(envelope(observations=[reading()])))

    assert world.events == [
        "mapping.fence",
        "quarantine.put_quarantine_entries",
        "state.apply_ingest_marks",
    ]


@pytest.mark.parametrize(
    ("reason", "options"),
    TEMPORARY_FENCES,
    ids=[f"{reason.value}-{next(iter(o.values()))['status']}" for reason, o in TEMPORARY_FENCES],
)
@pytest.mark.parametrize("receive_count", [1, 4, 8])
async def test_a_temporary_fence_defers_with_backoff_and_writes_nothing(
    reason: IngressDeferReason, options: dict[str, Any], receive_count: int
) -> None:
    world = await make_world(**options)

    decision = await world.processor.process(
        message_for(envelope(observations=[reading()]), receive_count=receive_count)
    )

    assert_decision(
        decision,
        action="visibility",
        category=IngressFailureCategory.DEFERRED,
        seconds=backoff(receive_count),
    )
    assert_nothing_written(world)
    assert world.count("ingress_deferred_total", reason=reason.value) == 1
    assert world.metric("ingress_fenced_total") == []


@pytest.mark.parametrize(
    "envelope_overrides",
    [
        {},
        {"device_integration_id": OTHER_INTEGRATION},
        {"integration_account_id": "ia_" + "5" * 32},
        {"tenant_id": "tenant-forged"},
    ],
    ids=["no-account-row", "forged-integration", "foreign-account", "foreign-tenant"],
)
async def test_an_unverified_fence_writes_nothing_and_is_permanent(
    envelope_overrides: dict[str, Any],
) -> None:
    # Only the real account row exists, so no row matches the envelope's (tenant, account).
    foreign = "integration_account_id" in envelope_overrides or "tenant_id" in envelope_overrides
    world = await make_world(seed_account=foreign)

    decision = await world.processor.process(
        message_for(envelope(observations=[reading()], **envelope_overrides))
    )

    assert_decision(
        decision,
        action="visibility",
        category=IngressFailureCategory.FENCED_UNVERIFIED,
        seconds=PERMANENT,
    )
    assert_nothing_written(world)
    assert "quarantine.put_quarantine_entries" not in world.events
    assert "state.apply_ingest_marks" not in world.events
    (record,) = world.metric("ingress_fenced_total")
    assert record["labels"]["reason"] in {item.value for item in IngressFenceReason}


# Failure table: store, write, contract and unexpected failures -----------------------------------


@pytest.mark.parametrize("error", store_errors(), ids=["client-error", "endpoint-error"])
@pytest.mark.parametrize(
    "operation", ["fence", "put_quarantine_entries", "apply_ingest_marks", "get_component_states"]
)
async def test_a_store_failure_retries_with_backoff_and_never_deletes(
    operation: str, error: BaseException
) -> None:
    world = await make_world()
    world.fail_next(operation, error)
    item = envelope(observations=[reading(sequence=3), reading(component_id=FOREIGN_COMPONENT)])

    decision = await world.processor.process(message_for(item, receive_count=1))

    assert_decision(
        decision,
        action="visibility",
        category=IngressFailureCategory.STORE_UNAVAILABLE,
        seconds=backoff(1),
    )
    assert f"{_OWNER[operation]}.{operation}" in world.events  # the fault was really hit


_OWNER = {
    "fence": "mapping",
    "put_quarantine_entries": "quarantine",
    "apply_ingest_marks": "state",
    "get_component_states": "state",
}


@pytest.mark.parametrize("error", store_errors(), ids=["client-error", "endpoint-error"])
async def test_a_store_failure_inside_the_pipeline_is_store_unavailable(
    error: BaseException,
) -> None:
    world = await make_world()
    world.pipeline.fail_with = error

    decision = await world.processor.process(
        message_for(envelope(observations=[reading()]), receive_count=3)
    )

    assert_decision(
        decision,
        action="visibility",
        category=IngressFailureCategory.STORE_UNAVAILABLE,
        seconds=backoff(3),
    )
    assert world.quarantine_rows == [] and not world.wrote_state


@pytest.mark.parametrize("error", store_errors(), ids=["client-error", "endpoint-error"])
async def test_a_failed_fence_persist_never_deletes(error: BaseException) -> None:
    world = await make_world(integration={"status": DeviceIntegrationStatus.DECOMMISSIONED})
    world.fail_next("put_quarantine_entries", error)

    decision = await world.processor.process(message_for(envelope(observations=[reading()])))

    assert_decision(
        decision,
        action="visibility",
        category=IngressFailureCategory.STORE_UNAVAILABLE,
        seconds=backoff(1),
    )
    assert "state.apply_ingest_marks" not in world.events  # no fence mark without its rows


@pytest.mark.parametrize(
    ("retryable", "category", "seconds"),
    [
        (True, IngressFailureCategory.WRITE_RETRYABLE, backoff(5)),
        (False, IngressFailureCategory.WRITE_PERMANENT, PERMANENT),
    ],
    ids=["retryable", "permanent"],
)
async def test_a_telemetry_write_error_never_deletes(
    retryable: bool, category: IngressFailureCategory, seconds: int
) -> None:
    world = await make_world()
    world.pipeline.fail_with = TelemetryWriteError("influx down", retryable=retryable)

    decision = await world.processor.process(
        message_for(envelope(observations=[reading()]), receive_count=5)
    )

    assert_decision(decision, action="visibility", category=category, seconds=seconds)
    assert not world.wrote_state
    assert world.count("ingress_write_failures_total", retryable=str(retryable).lower()) == 1


async def test_an_unknown_mapping_version_is_a_contract_violation() -> None:
    world = await make_world()
    item = envelope(
        observations=[reading()],
        mapping_versions=NormalizationVersions(catalog_version="metric-catalog@99"),
    )

    decision = await world.processor.process(message_for(item))

    assert_decision(
        decision,
        action="visibility",
        category=IngressFailureCategory.CONTRACT_VIOLATION,
        seconds=PERMANENT,
    )
    assert_nothing_written(world)


async def test_a_pipeline_value_error_is_a_contract_violation() -> None:
    world = await make_world()
    world.pipeline.fail_with = ValueError("telemetry batch exceeds 1000 observations")

    decision = await world.processor.process(message_for(envelope(observations=[reading()])))

    assert_decision(
        decision,
        action="visibility",
        category=IngressFailureCategory.CONTRACT_VIOLATION,
        seconds=PERMANENT,
    )


@pytest.mark.parametrize(
    "error", [RuntimeError("boom"), KeyError("x"), TypeError("y")], ids=["runtime", "key", "type"]
)
async def test_any_other_exception_is_unexpected_and_retryable(error: Exception) -> None:
    world = await make_world()
    world.pipeline.fail_with = error

    decision = await world.processor.process(
        message_for(envelope(observations=[reading()]), receive_count=6)
    )

    assert_decision(
        decision,
        action="visibility",
        category=IngressFailureCategory.UNEXPECTED,
        seconds=backoff(6),
    )


async def test_an_unexpected_failure_after_the_write_never_deletes() -> None:
    world = await make_world()
    world.fail_next("apply_ingest_marks", RuntimeError("bug"))

    decision = await world.processor.process(
        message_for(envelope(observations=[reading()]), receive_count=2)
    )

    assert_decision(
        decision,
        action="visibility",
        category=IngressFailureCategory.UNEXPECTED,
        seconds=backoff(2),
    )


# Write order -----------------------------------------------------------------------------------


async def test_the_d8_order_is_fence_ingest_quarantine_then_state() -> None:
    world = await make_world()
    item = envelope(observations=[reading(sequence=1), reading(component_id=FOREIGN_COMPONENT)])

    decision = await world.processor.process(message_for(item))

    assert_decision(decision, action="delete", category=IngressFailureCategory.PROCESSED)
    assert world.events == [
        "mapping.fence",
        "pipeline.ingest",
        "quarantine.put_quarantine_entries",
        "state.get_component_states",
        "state.apply_ingest_marks",
    ]


async def test_a_quarantine_failure_skips_the_state_marks() -> None:
    world = await make_world()
    world.fail_next("put_quarantine_entries", store_errors()[0])
    item = envelope(observations=[reading(sequence=1), reading(component_id=FOREIGN_COMPONENT)])

    decision = await world.processor.process(message_for(item))

    assert decision.action == "visibility"
    assert "state.apply_ingest_marks" not in world.events
    assert not world.wrote_state
    assert_no_processed_metrics(world)


def assert_no_processed_metrics(world: World) -> None:
    """D8 step 6: metrics follow the durable writes, so a retried message never counts twice."""
    for name in (
        "ingress_observations_total",
        "ingress_quarantined_total",
        "ingress_quality_flags_total",
        "ingress_sequence_events_total",
    ):
        assert world.metric(name) == []


@pytest.mark.parametrize("operation", ["apply_ingest_marks", "get_component_states"])
async def test_a_state_failure_emits_no_processed_metrics(operation: str) -> None:
    world = await make_world()
    world.fail_next(operation, store_errors()[0])
    item = envelope(
        replay=False,
        observations=[reading(sequence=1), reading(component_id=FOREIGN_COMPONENT)],
    )

    decision = await world.processor.process(message_for(item))

    assert decision.category == IngressFailureCategory.STORE_UNAVAILABLE
    assert_no_processed_metrics(world)


async def test_a_failed_fence_persist_emits_no_fence_metric() -> None:
    world = await make_world(integration={"status": DeviceIntegrationStatus.DECOMMISSIONED})
    world.fail_next("put_quarantine_entries", store_errors()[0])

    await world.processor.process(message_for(envelope(observations=[reading()])))

    assert world.metric("ingress_fenced_total") == []


@pytest.mark.parametrize("error", store_errors(), ids=["client-error", "endpoint-error"])
async def test_a_failed_fence_mark_never_deletes(error: BaseException) -> None:
    world = await make_world(integration={"status": DeviceIntegrationStatus.DECOMMISSIONED})
    world.fail_next("apply_ingest_marks", error)

    decision = await world.processor.process(
        message_for(envelope(observations=[reading()]), receive_count=3)
    )

    assert_decision(
        decision,
        action="visibility",
        category=IngressFailureCategory.STORE_UNAVAILABLE,
        seconds=backoff(3),
    )
    assert len(world.quarantine_rows) == 2  # the rows were persisted first; a retry re-creates them
    assert world.metric("ingress_fenced_total") == []


async def test_a_cancellation_is_never_turned_into_a_decision() -> None:
    """The loop's deadline cancels `process`; swallowing that would hide the timeout."""
    world = await make_world()
    world.pipeline.fail_with = asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await world.processor.process(message_for(envelope(observations=[reading()])))


async def test_a_redelivery_after_a_state_failure_rebuilds_the_same_rows() -> None:
    """The first attempt persists its row, then fails; the redelivery must hit the same id."""
    world = await make_world()
    item = envelope(observations=[reading(sequence=1), reading(component_id=FOREIGN_COMPONENT)])
    world.fail_next("apply_ingest_marks", store_errors()[0])
    first = await world.processor.process(message_for(item, receive_count=1))
    (persisted,) = world.quarantine_rows

    decision = await world.processor.process(message_for(item, receive_count=2))

    assert first.category == IngressFailureCategory.STORE_UNAVAILABLE
    assert_decision(decision, action="delete", category=IngressFailureCategory.PROCESSED)
    (row,) = world.quarantine_rows
    assert row.quarantine_id == persisted.quarantine_id
    assert row.reason == QuarantineReason.COMPONENT_NOT_OWNED.value


@pytest.mark.parametrize("operation", ["put_quarantine_entries", "apply_ingest_marks"])
async def test_a_value_error_outside_the_pipeline_is_unexpected(operation: str) -> None:
    """Only a `ValueError` from the pipeline is a contract violation (failure table)."""
    world = await make_world()
    world.fail_next(operation, ValueError("bug"))
    item = envelope(observations=[reading(sequence=1), reading(component_id=FOREIGN_COMPONENT)])

    decision = await world.processor.process(message_for(item, receive_count=2))

    assert_decision(
        decision,
        action="visibility",
        category=IngressFailureCategory.UNEXPECTED,
        seconds=backoff(2),
    )


# Context, replay and forged components ---------------------------------------------------------


async def test_the_pipeline_gets_exactly_the_context_the_fence_returned() -> None:
    """The context comes from `TrustedIngress` (the re-resolved DI), never from envelope fields."""
    item = envelope(observations=[reading()])
    sentinel = build_trusted_context(item, device_integration()).model_copy(
        update={"connector_version": "ctx-from-mapping"}
    )

    class SentinelMapping:
        def __init__(self, inner: Any) -> None:
            self.inner = inner

        async def fence(self, envelope: IngressEnvelope) -> Any:
            result = await self.inner.fence(envelope)
            assert isinstance(result, TrustedIngress)
            return result.model_copy(update={"context": sentinel})

    world = await make_world(wrap_mapping=SentinelMapping)

    await world.processor.process(message_for(item))

    assert world.pipeline.contexts == [sentinel]


async def test_the_context_is_rebuilt_from_the_re_resolved_integration() -> None:
    world = await make_world()
    item = envelope(observations=[reading()], connector_version="fw-2.1", received_at=RECEIVED)

    await world.processor.process(message_for(item))

    (context,) = world.pipeline.contexts
    resolved = device_integration()
    assert context == build_trusted_context(item, resolved)
    assert (context.tenant_id, context.device_id, context.integration_id) == (
        resolved.tenant_id,
        resolved.device_id,
        resolved.device_integration_id,
    )
    assert context.integration_type == resolved.provider_key
    assert context.received_at == item.received_at
    assert context.replay is False
    assert context.mapping_versions == item.mapping_versions
    assert context.connector_version == "fw-2.1"


async def test_a_replay_reproduces_the_pinned_versions_and_event_ids() -> None:
    world = await make_world()
    original = envelope(observations=[reading(sequence=7)])
    # A later received_at, so an applied mark would visibly advance the rows.
    replay = envelope(
        ingress_id="ing_" + "8" * 32,
        replay=True,
        replay_of=REPLAY_OF,
        received_at=RECEIVED + timedelta(hours=1),
        observations=[reading(sequence=7)],
    )

    await world.processor.process(message_for(original))
    store = world.subject.store
    states_before = copy.deepcopy(
        (store.component_states, store.device_states, store.connector_health)
    )
    decision = await world.processor.process(message_for(replay, message_id="msg-2"))

    assert_decision(decision, action="delete", category=IngressFailureCategory.PROCESSED)
    first, second = world.writer.points
    assert second.event_id == first.event_id
    assert second.mapping == replay.mapping_versions == original.mapping_versions
    assert QualityFlag.REPLAY in second.quality.flags
    assert world.pipeline.contexts[1].replay is True
    assert (
        store.component_states,
        store.device_states,
        store.connector_health,
    ) == states_before  # a replay never advances a mark


async def test_a_forged_foreign_component_is_quarantined_as_component_not_owned() -> None:
    world = await make_world()
    item = envelope(observations=[reading(component_id=FOREIGN_COMPONENT)])

    decision = await world.processor.process(message_for(item))

    assert_decision(decision, action="delete", category=IngressFailureCategory.PROCESSED)
    assert world.writer.points == []
    (row,) = world.quarantine_rows
    assert row.kind == QuarantineEntryKind.OBSERVATION
    assert row.reason == QuarantineReason.COMPONENT_NOT_OWNED.value
    assert world.count("ingress_quarantined_total", reason="component_not_owned") == 1


# Quarantine rows ---------------------------------------------------------------------------------


async def test_quarantine_rows_come_from_build_quarantine_entries() -> None:
    world = await make_world()
    foreign = reading(component_id=FOREIGN_COMPONENT)
    item = envelope(
        observations=[foreign, reading(sequence=1), foreign],
        connector_version="fw-9",
    )

    await world.processor.process(message_for(item))

    (outcome,) = world.pipeline.outcomes
    expected = build_quarantine_entries(item, outcome=outcome, now=NOW)
    rows = sorted(world.quarantine_rows, key=lambda row: row.quarantine_id)
    assert rows == sorted(expected, key=lambda row: row.quarantine_id)
    assert len({row.quarantine_id for row in rows}) == 2  # identical excerpts, ordinals 0 and 1
    for row in rows:
        assert row.mapping_versions == item.mapping_versions
        assert (row.provider_key, row.external_identity_hash) == (
            item.provider_key,
            item.external_identity_hash,
        )
        assert (row.source_type, row.connector_version) == (item.source_type, "fw-9")
        assert (row.device_integration_id, row.device_id) == (INTEGRATION, DEVICE)


# Accepted data never vanishes --------------------------------------------------------------------


@pytest.mark.parametrize(
    "scenario",
    ["accepted", "mixed", "quarantined", *[reason.value for reason in TERMINAL_FENCES]],
)
async def test_every_observation_of_a_deleted_message_is_written_or_persisted(
    scenario: str,
) -> None:
    observations = {
        "accepted": [reading(sequence=1), reading(sequence=2, value=1.0)],
        "mixed": [reading(sequence=1), reading(component_id=FOREIGN_COMPONENT)],
        "quarantined": [reading(metric_key="water.unknown"), reading(source_unit="furlong")],
    }.get(scenario, [reading(), reading(value=3.0)])
    options: dict[str, Any] = {}
    overrides: dict[str, Any] = {}
    if scenario in {reason.value for reason in TERMINAL_FENCES}:
        options, overrides = fenced_world_options(IngressFenceReason(scenario))
    world = await make_world(**options)
    item = envelope(observations=observations, **overrides)

    decision = await world.processor.process(message_for(item))

    assert decision.action == "delete"
    persisted = [
        row
        for row in world.quarantine_rows
        if row.kind != QuarantineEntryKind.FENCED_MESSAGE and row.ingress_id == item.ingress_id
    ]
    assert len(world.writer.points) + len(persisted) == len(item.observations)


# Metrics -----------------------------------------------------------------------------------------


async def test_processed_metrics_count_observations_reasons_and_duration() -> None:
    world = await make_world()
    item = envelope(
        observations=[
            reading(sequence=1),
            reading(sequence=2, value=26.0, observed_at=OBSERVED + timedelta(seconds=1)),
            reading(component_id=FOREIGN_COMPONENT),
        ]
    )

    await world.processor.process(message_for(item))

    assert world.count("ingress_observations_total", result="accepted") == 2
    assert world.count("ingress_observations_total", result="quarantined") == 1
    assert world.count("ingress_quarantined_total", reason="component_not_owned") == 1
    (duration,) = world.metric("ingress_normalization_duration_seconds")
    assert duration["labels"] == {} and duration["value"] >= 0


async def test_quality_flags_are_counted_per_accepted_observation() -> None:
    world = await make_world()
    late = RECEIVED - timedelta(hours=6)
    item = envelope(
        ingress_id="ing_" + "8" * 32,
        replay=True,
        replay_of=REPLAY_OF,
        observations=[
            reading(observed_at=late),
            reading(observed_at=late - timedelta(minutes=1), value=1.0),
        ],
    )

    await world.processor.process(message_for(item))

    flags = [flag for point in world.writer.points for flag in point.quality.flags]
    assert QualityFlag.REPLAY in flags
    for flag in set(flags):
        assert world.count("ingress_quality_flags_total", flag=flag.value) == flags.count(flag)


async def test_sequence_events_are_emitted_on_the_first_delivery_only() -> None:
    world = await make_world()
    first = envelope(observations=[reading(sequence=1)])
    second = envelope(
        ingress_id="ing_" + "5" * 32,
        observations=[reading(sequence=3, observed_at=OBSERVED + timedelta(seconds=5))],
    )

    await world.processor.process(message_for(first, receive_count=1))
    await world.processor.process(message_for(second, receive_count=2, message_id="msg-2"))
    third = envelope(
        ingress_id="ing_" + "6" * 32,
        observations=[reading(sequence=5, observed_at=OBSERVED + timedelta(seconds=9))],
    )
    await world.processor.process(message_for(third, receive_count=1, message_id="msg-3"))

    events = [record["labels"]["event"] for record in world.metric("ingress_sequence_events_total")]
    assert events == ["first", "gap"]


async def test_a_replay_emits_no_sequence_event() -> None:
    world = await make_world()
    item = envelope(replay=True, replay_of=REPLAY_OF, observations=[reading(sequence=1)])

    await world.processor.process(message_for(item))

    assert world.metric("ingress_sequence_events_total") == []


async def test_the_processor_emits_only_closed_worker_metrics_without_identifiers() -> None:
    plain = envelope(observations=[reading()])
    scenarios: list[tuple[dict[str, Any], IngressEnvelope | str]] = [
        ({}, envelope(observations=[reading(sequence=1), reading(component_id=FOREIGN_COMPONENT)])),
        ({"integration": {"status": DeviceIntegrationStatus.DECOMMISSIONED}}, plain),
        ({"device": {"status": DeviceStatus.SUSPENDED}}, plain),
        ({"seed_account": False}, plain),
        ({}, "not json"),
    ]
    emitted: set[str] = set()
    for options, item in scenarios:
        world = await make_world(**options)
        await world.processor.process(message_for(item))
        for record in world.metrics.records:
            name, labels = record["metric"], record["labels"]
            emitted.add(name)
            assert name in WORKER_METRICS
            assert set(labels) == set(INGEST_METRICS[name])
            for label, value in labels.items():
                assert value in INGEST_METRICS[name][label]
                assert not any(identifier in value for identifier in IDENTIFIERS)
    assert {
        "ingress_observations_total",
        "ingress_quarantined_total",
        "ingress_fenced_total",
        "ingress_deferred_total",
    } <= emitted


async def test_the_processor_does_not_emit_the_message_outcome_itself() -> None:
    """`ingress_messages_total` is emitted once per message by the loop, which alone knows the
    final category (a deadline overrides the processor)."""
    world = await make_world()

    await world.processor.process(message_for(envelope(observations=[reading()])))

    assert world.metric("ingress_messages_total") == []


async def test_the_processor_logs_no_identifier(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level("DEBUG")
    for options, error in [
        ({}, RuntimeError(f"failure for {TENANT} {DEVICE}")),
        ({}, store_errors()[0]),
        ({"seed_account": False}, None),
    ]:
        world = await make_world(**options)
        world.pipeline.fail_with = error
        await world.processor.process(message_for(envelope(observations=[reading()])))
        await world.processor.process(message_for("not json"))

    for record in caplog.records:
        text = record.getMessage()
        assert record.exc_info is None
        assert not any(identifier in text for identifier in IDENTIFIERS)
    # Failures are logged by category (a closed value), never by identifier.
    assert any("store_unavailable" in record.getMessage() for record in caplog.records)
