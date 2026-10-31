"""P3-06: `IngestStateService` — marks and best-effort sequence events per message (D11)."""

from collections import Counter
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any

import pytest

from limnopulse_api.domain.ingestion import (
    ComponentIngestState,
    ConnectorHealth,
    IngestMarks,
    IngressFence,
    IngressFenceReason,
    SequenceEvent,
)
from limnopulse_api.domain.telemetry_v2 import (
    CanonicalObservation,
    QualityFlag,
    QuarantinedObservation,
    QuarantineReason,
)
from limnopulse_api.services.ingest_state import IngestStateService
from limnopulse_api.services.telemetry_pipeline import PipelineOutcome
from limnopulse_api.services.telemetry_v2_writes import TelemetryWriteOutcome
from tests.support.ingestion_contract_kits import (
    ACCOUNT,
    COMPONENT,
    DEVICE,
    OBSERVED,
    OTHER_COMPONENT,
    RECEIVED,
    TENANT,
    canonical_observation,
    envelope,
    store_errors,
)
from tests.support.ingestion_fakes import KitSubject, ingest_state_subject

O = OBSERVED
S = timedelta(seconds=1)
NOW = RECEIVED + timedelta(seconds=30)
REPLAY_OF = "ing_" + "9" * 32


def outcome(
    *accepted: CanonicalObservation, quarantined: Sequence[QuarantinedObservation] = ()
) -> PipelineOutcome:
    return PipelineOutcome(
        written=TelemetryWriteOutcome(v2_points=len(accepted), legacy_points=0, legacy_excluded=0),
        quarantined=tuple(quarantined),
        counts={},
        accepted=accepted,
    )


def reading(
    observed_at: datetime,
    sequence: int | None,
    *,
    component_id: str = COMPONENT,
    metric_key: str = "temperature",
    flags: Sequence[QualityFlag] = (),
) -> CanonicalObservation:
    return canonical_observation(
        component_id=component_id,
        metric_key=metric_key,
        observed_at=observed_at,
        sequence=sequence,
        flags=flags,
    )


def quarantined(reason: QuarantineReason) -> QuarantinedObservation:
    return QuarantinedObservation(
        reason=reason,
        tenant_id=TENANT,
        device_id=DEVICE,
        component_id=COMPONENT,
        metric_key="temperature",
        observed_at=O,
        received_at=RECEIVED,
        source_event_id=None,
        detail="quarantined",
        source_excerpt_json='{"metric_key":"temperature"}',
        truncated=False,
    )


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


@pytest.fixture
def subject() -> KitSubject:
    return ingest_state_subject()


@pytest.fixture
def clock() -> Clock:
    return Clock(NOW)


@pytest.fixture
def service(subject: KitSubject, clock: Clock) -> IngestStateService:
    return IngestStateService(subject.repository, clock=clock)


async def component_state(
    subject: KitSubject, component_id: str = COMPONENT
) -> ComponentIngestState:
    return (await subject.repository.get_component_states(TENANT, [component_id]))[component_id]


def record_applied(subject: KitSubject) -> list[IngestMarks]:
    applied: list[IngestMarks] = []
    original = subject.repository.apply_ingest_marks

    async def recording(marks: IngestMarks) -> None:
        applied.append(marks)
        await original(marks)

    subject.repository.apply_ingest_marks = recording
    return applied


def is_empty(marks: IngestMarks) -> bool:
    connector = marks.connector_mark.model_dump().values()
    return (
        not marks.component_marks
        and marks.device_mark is None
        and all(value is None for value in connector)
    )


# Sequence truth table ------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("previous", "pair", "expected"),
    [
        (None, (O, 10), SequenceEvent.FIRST),
        ((O, None), (O + S, 10), SequenceEvent.FIRST),  # state row without an observed pair
        ((O, 10), (O + S, 11), SequenceEvent.CONTIGUOUS),
        ((O, 10), (O + S, 12), SequenceEvent.GAP),
        ((O, 10), (O + S, 10), SequenceEvent.REGRESSION),
        ((O, 10), (O + S, 3), SequenceEvent.REGRESSION),
        ((O, 10), (O, 10), SequenceEvent.DUPLICATE),
        ((O, 10), (O, 11), SequenceEvent.LATE),
        ((O, 10), (O, 9), SequenceEvent.LATE),
        ((O, 10), (O - S, 11), SequenceEvent.LATE),
        ((O, 10), (O - S, 3), SequenceEvent.LATE),
    ],
)
async def test_events_follow_the_truth_table_against_stored_state(
    subject: KitSubject,
    service: IngestStateService,
    previous: tuple[datetime, int | None] | None,
    pair: tuple[datetime, int],
    expected: SequenceEvent,
) -> None:
    if previous is not None:
        await service.record_outcome(
            envelope(ingress_id="ing_" + "1" * 32),
            outcome(reading(previous[0], previous[1])),
            receive_count=1,
        )

    events = await service.record_outcome(envelope(), outcome(reading(*pair)), receive_count=1)

    assert events == (expected,)


@pytest.mark.asyncio
async def test_a_message_is_classified_pair_by_pair_and_per_component(
    subject: KitSubject, service: IngestStateService
) -> None:
    await service.record_outcome(
        envelope(ingress_id="ing_" + "1" * 32),
        outcome(reading(O, 10), reading(O, 100, component_id=OTHER_COMPONENT)),
        receive_count=1,
    )
    message = outcome(
        reading(O - S, 5),  # LATE; does not move prev
        reading(O + S, 11),  # CONTIGUOUS
        reading(O + 2 * S, 12),  # CONTIGUOUS
        reading(O + 2 * S, 12, metric_key="ph"),  # same pair: one event, not two
        reading(O + 3 * S, 20),  # GAP
        reading(O + 4 * S, None),  # no sequence: no event
        reading(O + S, 101, component_id=OTHER_COMPONENT),  # CONTIGUOUS against its own state
        reading(O + 2 * S, 101, component_id=OTHER_COMPONENT),  # REGRESSION
    )

    events = await service.record_outcome(envelope(), message, receive_count=1)

    assert Counter(events) == Counter(
        {
            SequenceEvent.LATE: 1,
            SequenceEvent.CONTIGUOUS: 3,
            SequenceEvent.GAP: 1,
            SequenceEvent.REGRESSION: 1,
        }
    )
    # The stored mark is each Component's maximum (observed_at, sequence) pair of the message.
    state = await component_state(subject)
    other = await component_state(subject, OTHER_COMPONENT)
    assert (state.last_observed_at, state.last_observed_sequence) == (O + 3 * S, 20)
    assert (other.last_observed_at, other.last_observed_sequence) == (O + 2 * S, 101)


@pytest.mark.asyncio
async def test_observations_without_a_sequence_produce_no_event(
    service: IngestStateService,
) -> None:
    events = await service.record_outcome(
        envelope(), outcome(reading(O, None), reading(O + S, None)), receive_count=1
    )

    assert events == ()


@pytest.mark.asyncio
async def test_state_is_read_once_per_message_before_the_marks(
    subject: KitSubject, service: IngestStateService
) -> None:
    reads: list[tuple[str, tuple[str, ...]]] = []
    original = subject.repository.get_component_states

    async def counting(tenant_id: str, component_ids: Sequence[str]) -> dict[str, Any]:
        reads.append((tenant_id, tuple(component_ids)))
        return await original(tenant_id, component_ids)

    subject.repository.get_component_states = counting

    events = await service.record_outcome(
        envelope(),
        outcome(
            reading(O, 10),
            reading(O + S, 11),
            reading(O, 4, component_id=OTHER_COMPONENT),
        ),
        receive_count=1,
    )

    assert len(reads) == 1
    tenant_id, component_ids = reads[0]
    assert tenant_id == TENANT
    assert set(component_ids) == {COMPONENT, OTHER_COMPONENT}
    # Classified against the state before this message's own marks were applied.
    assert Counter(events) == Counter({SequenceEvent.FIRST: 2, SequenceEvent.CONTIGUOUS: 1})


# Best-effort gating --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_redelivery_is_not_classified_but_still_marks(
    subject: KitSubject, service: IngestStateService, clock: Clock
) -> None:
    first = await service.record_outcome(envelope(), outcome(reading(O, 10)), receive_count=1)
    clock.now = NOW + timedelta(minutes=1)

    redelivery = await service.record_outcome(
        envelope(ingress_id="ing_" + "2" * 32, received_at=RECEIVED + S),
        outcome(reading(O + S, 11)),
        receive_count=2,
    )

    assert first == (SequenceEvent.FIRST,)
    assert redelivery == ()
    state = await component_state(subject)
    assert (state.last_received_at, state.last_ingested_at) == (RECEIVED + S, clock.now)
    assert (state.last_observed_at, state.last_observed_sequence) == (O + S, 11)


@pytest.mark.asyncio
async def test_a_redriven_message_is_classified_again(
    subject: KitSubject, service: IngestStateService
) -> None:
    message = envelope()
    accepted = outcome(reading(O, 10))
    assert await service.record_outcome(message, accepted, receive_count=1) == (
        SequenceEvent.FIRST,
    )
    assert await service.record_outcome(message, accepted, receive_count=3) == ()

    # A DLQ redrive comes back with ApproximateReceiveCount == 1 and is classified again
    # (best-effort, may double-count). D11: never pin exact events under redelivery, only that
    # the redrive is classified and is never FIRST/CONTIGUOUS/GAP/REGRESSION.
    redriven = {SequenceEvent.DUPLICATE, SequenceEvent.LATE}
    again = await service.record_outcome(message, accepted, receive_count=1)
    assert again and set(again) <= redriven
    await service.record_outcome(
        envelope(ingress_id="ing_" + "2" * 32), outcome(reading(O + S, 11)), receive_count=1
    )
    later = await service.record_outcome(message, accepted, receive_count=1)
    assert later and set(later) <= redriven


@pytest.mark.asyncio
async def test_a_replay_never_writes_or_classifies(
    subject: KitSubject, service: IngestStateService
) -> None:
    replay = envelope(replay=True, replay_of=REPLAY_OF, received_at=RECEIVED + timedelta(days=1))
    applied = record_applied(subject)

    events = await service.record_outcome(
        replay,
        outcome(
            reading(O + timedelta(days=1), 99),
            quarantined=[quarantined(QuarantineReason.METRIC_UNKNOWN)],
        ),
        receive_count=1,
    )
    await service.record_fence(
        replay, IngressFence(reason=IngressFenceReason.ACCOUNT_REVOKED, account_verified=True)
    )

    assert events == ()
    assert all(is_empty(marks) for marks in applied)  # passing empty marks through is allowed
    assert subject.store.component_states == {}
    assert subject.store.device_states == {}
    assert subject.store.connector_health == {}


@pytest.mark.asyncio
async def test_a_replay_never_makes_a_device_look_current(
    subject: KitSubject, service: IngestStateService, clock: Clock
) -> None:
    await service.record_outcome(envelope(), outcome(reading(O, 10)), receive_count=1)
    before = (
        dict(subject.store.component_states[COMPONENT]),
        dict(subject.store.device_states[DEVICE]),
        dict(subject.store.connector_health[ACCOUNT]),
    )
    clock.now = NOW + timedelta(days=2)

    await service.record_outcome(
        envelope(
            ingress_id="ing_" + "3" * 32,
            replay=True,
            replay_of=REPLAY_OF,
            received_at=RECEIVED + timedelta(days=2),
        ),
        outcome(reading(O + timedelta(days=2), 11)),
        receive_count=1,
    )

    assert (
        subject.store.component_states[COMPONENT],
        subject.store.device_states[DEVICE],
        subject.store.connector_health[ACCOUNT],
    ) == before


# Marks ---------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_marks_use_received_at_for_last_seen_and_the_clock_for_ingested(
    subject: KitSubject, service: IngestStateService
) -> None:
    await service.record_outcome(envelope(), outcome(reading(O, 10)), receive_count=1)

    state = await component_state(subject)
    assert state == ComponentIngestState(
        tenant_id=TENANT,
        component_id=COMPONENT,
        device_id=DEVICE,
        last_received_at=RECEIVED,
        last_ingested_at=NOW,
        last_observed_at=O,
        last_observed_sequence=10,
    )
    device = await subject.device_state(DEVICE)
    assert device is not None
    assert (device.last_received_at, device.last_ingested_at) == (RECEIVED, NOW)


@pytest.mark.asyncio
async def test_a_redelivery_converges_except_last_ingested_at(
    subject: KitSubject, service: IngestStateService, clock: Clock
) -> None:
    message = envelope()
    accepted = outcome(reading(O, 10), quarantined=[quarantined(QuarantineReason.VALUE_INVALID)])
    await service.record_outcome(message, accepted, receive_count=1)
    first = (
        await component_state(subject),
        await subject.repository.get_connector_health(TENANT, ACCOUNT),
    )
    clock.now = NOW + timedelta(minutes=10)

    await service.record_outcome(message, accepted, receive_count=2)

    second = (
        await component_state(subject),
        await subject.repository.get_connector_health(TENANT, ACCOUNT),
    )
    assert second[1] == first[1]
    assert second[0].model_copy(update={"last_ingested_at": None}) == first[0].model_copy(
        update={"last_ingested_at": None}
    )
    assert second[0].last_ingested_at == clock.now


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "flag", [QualityFlag.CLOCK_SKEW_FUTURE, QualityFlag.CLOCK_SKEW_PAST_EXTREME]
)
async def test_a_clock_skewed_observation_never_advances_the_observed_pair(
    subject: KitSubject, service: IngestStateService, flag: QualityFlag
) -> None:
    await service.record_outcome(envelope(), outcome(reading(O, 10)), receive_count=1)

    await service.record_outcome(
        envelope(ingress_id="ing_" + "2" * 32, received_at=RECEIVED + S),
        outcome(reading(O + timedelta(days=365), 99, flags=[flag])),
        receive_count=1,
    )

    state = await component_state(subject)
    assert (state.last_observed_at, state.last_observed_sequence) == (O, 10)
    assert state.last_received_at == RECEIVED + S  # server-time marks still advance


@pytest.mark.asyncio
async def test_within_a_message_skewed_pairs_are_ignored_for_the_observed_mark(
    subject: KitSubject, service: IngestStateService
) -> None:
    await service.record_outcome(
        envelope(),
        outcome(
            reading(O + 5 * S, 12),
            reading(O + timedelta(days=30), 500, flags=[QualityFlag.CLOCK_SKEW_FUTURE]),
            reading(O + 2 * S, 30),
            reading(O - timedelta(days=9000), 900, flags=[QualityFlag.CLOCK_SKEW_PAST_EXTREME]),
        ),
        receive_count=1,
    )

    state = await component_state(subject)
    assert (state.last_observed_at, state.last_observed_sequence) == (O + 5 * S, 12)


# Connector health ----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_connector_health_marks_accepted_messages(
    subject: KitSubject, service: IngestStateService
) -> None:
    await service.record_outcome(envelope(), outcome(reading(O, None)), receive_count=1)

    assert await subject.repository.get_connector_health(TENANT, ACCOUNT) == ConnectorHealth(
        tenant_id=TENANT,
        integration_account_id=ACCOUNT,
        last_received_at=RECEIVED,
        last_accepted_at=RECEIVED,
    )


@pytest.mark.asyncio
async def test_connector_health_marks_unresolved_deployments(
    subject: KitSubject, service: IngestStateService
) -> None:
    """ADR-006 / §10.2: a deployment-resolution miss quarantines and marks connector health."""
    await service.record_outcome(
        envelope(),
        outcome(quarantined=[quarantined(QuarantineReason.DEPLOYMENT_UNRESOLVED)]),
        receive_count=1,
    )

    health = await subject.repository.get_connector_health(TENANT, ACCOUNT)
    assert health == ConnectorHealth(
        tenant_id=TENANT,
        integration_account_id=ACCOUNT,
        last_received_at=RECEIVED,
        last_quarantined_at=RECEIVED,
        last_quarantine_reason="deployment_unresolved",
    )
    assert subject.store.component_states == {}  # nothing accepted: no component mark
    device = await subject.device_state(DEVICE)  # the device was still seen (last-seen)
    assert device is not None
    assert (device.last_received_at, device.last_ingested_at) == (RECEIVED, NOW)


@pytest.mark.asyncio
async def test_connector_health_keeps_the_greatest_reason_of_a_message(
    subject: KitSubject, service: IngestStateService
) -> None:
    await service.record_outcome(
        envelope(),
        outcome(
            reading(O, 1),
            quarantined=[
                quarantined(QuarantineReason.DEPLOYMENT_UNRESOLVED),
                quarantined(QuarantineReason.VALUE_INVALID),
                quarantined(QuarantineReason.METRIC_UNKNOWN),
            ],
        ),
        receive_count=1,
    )

    health = await subject.repository.get_connector_health(TENANT, ACCOUNT)
    assert health is not None
    assert (health.last_accepted_at, health.last_quarantined_at, health.last_quarantine_reason) == (
        RECEIVED,
        RECEIVED,
        "value_invalid",
    )


@pytest.mark.asyncio
async def test_a_verified_fence_marks_only_the_connector_with_the_bare_reason(
    subject: KitSubject, service: IngestStateService
) -> None:
    await service.record_fence(
        envelope(),
        IngressFence(reason=IngressFenceReason.DEVICE_DECOMMISSIONED, account_verified=True),
    )

    assert await subject.repository.get_connector_health(TENANT, ACCOUNT) == ConnectorHealth(
        tenant_id=TENANT,
        integration_account_id=ACCOUNT,
        last_fenced_at=RECEIVED,
        last_fence_reason="device_decommissioned",
    )
    assert subject.store.component_states == {}
    assert subject.store.device_states == {}


@pytest.mark.asyncio
async def test_an_unverified_fence_writes_nothing(
    subject: KitSubject, service: IngestStateService
) -> None:
    applied = record_applied(subject)

    await service.record_fence(
        envelope(),
        IngressFence(reason=IngressFenceReason.IDENTITY_UNRESOLVED, account_verified=False),
    )

    assert all(is_empty(marks) for marks in applied)
    assert subject.store.connector_health == {}


# Store failures ------------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["get_component_states", "apply_ingest_marks"])
async def test_store_failures_propagate_unwrapped(
    subject: KitSubject, service: IngestStateService, operation: str
) -> None:
    for error in store_errors():
        subject.fail_next(operation, error)
        with pytest.raises(type(error)) as raised:
            await service.record_outcome(envelope(), outcome(reading(O, 10)), receive_count=1)
        assert raised.value is error


@pytest.mark.asyncio
async def test_a_failed_state_read_applies_no_marks(
    subject: KitSubject, service: IngestStateService
) -> None:
    subject.fail_next("get_component_states", store_errors()[0])

    with pytest.raises(type(store_errors()[0])):
        await service.record_outcome(envelope(), outcome(reading(O, 10)), receive_count=1)

    assert subject.store.component_states == {}
    assert subject.store.connector_health == {}


@pytest.mark.asyncio
async def test_fence_store_failures_propagate_unwrapped(
    subject: KitSubject, service: IngestStateService
) -> None:
    error = store_errors()[1]
    subject.fail_next("apply_ingest_marks", error)

    with pytest.raises(type(error)) as raised:
        await service.record_fence(
            envelope(),
            IngressFence(reason=IngressFenceReason.ACCOUNT_REVOKED, account_verified=True),
        )
    assert raised.value is error
