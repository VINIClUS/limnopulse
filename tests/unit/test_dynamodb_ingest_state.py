"""P3-06: `DynamoIngestStateRepository` on the in-memory Dynamo fake (D11 monotone marks)."""

import asyncio
import random
from collections.abc import Callable, Sequence
from datetime import timedelta
from typing import Any

import pytest

from limnopulse_api.adapters.dynamodb_ingest_state import DynamoIngestStateRepository
from limnopulse_api.adapters.dynamodb_v2 import (
    DynamoV2KeyBuilder,
    component_ingest_state_from_item,
    connector_health_from_item,
    device_ingest_state_from_item,
)
from limnopulse_api.domain.ingestion import (
    ComponentIngestState,
    ComponentMark,
    ConnectorHealth,
    ConnectorMark,
    DeviceIngestState,
    DeviceMark,
    IngestMarks,
)
from limnopulse_api.repositories.ingestion import IngestStateRepository
from tests.support.ingestion_contract_kits import (
    ACCOUNT,
    COMPONENT,
    DEVICE,
    DYNAMO_EXPRESSION_VECTORS,
    INGEST_STATE_CONTRACT,
    OBSERVED,
    OTHER_COMPONENT,
    RECEIVED,
    TENANT,
    kit_ids,
    marks,
    store_errors,
)
from tests.support.ingestion_dynamo_fake import IngestionDynamoClient
from tests.support.ingestion_fakes import FakeIngestStateRepository, Faults, IngestionStore

TABLE = "LimnopulseDomain"
KEYS = DynamoV2KeyBuilder()

_BOTO_OPERATION = {
    "apply_ingest_marks": "update_item",
    "get_component_states": "get_item",
    "get_connector_health": "get_item",
}


class DynamoIngestStateSubject:
    """`IngestStateKitSubject` over the real adapter and the P3-00 Dynamo fake."""

    def __init__(self) -> None:
        self.client = IngestionDynamoClient()
        self.repository = DynamoIngestStateRepository(TABLE, self.client)

    def fail_next(self, operation: str, exc: BaseException) -> None:
        self.client.fail_next(_BOTO_OPERATION[operation], exc)

    def raw(self, key: dict[str, str]) -> dict[str, Any] | None:
        return self.client.items.get((key["PK"], key["SK"]))

    async def device_state(self, device_id: str) -> DeviceIngestState | None:
        raw = self.raw(KEYS.device_ingest_state(device_id))
        return None if raw is None else device_ingest_state_from_item(raw)


@pytest.fixture
def subject() -> DynamoIngestStateSubject:
    return DynamoIngestStateSubject()


def test_adapter_implements_the_port(subject: DynamoIngestStateSubject) -> None:
    assert isinstance(subject.repository, IngestStateRepository)


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", INGEST_STATE_CONTRACT, ids=kit_ids(INGEST_STATE_CONTRACT))
async def test_adapter_passes_the_ingest_state_contract_kit(
    scenario: Callable[[Any], Any], subject: DynamoIngestStateSubject
) -> None:
    await scenario(subject)


# Rows and identity --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rows_use_the_frozen_keys_and_codecs(subject: DynamoIngestStateSubject) -> None:
    applied = marks(quarantined_at=RECEIVED, quarantine_reason="deployment_unresolved")

    await subject.repository.apply_ingest_marks(applied)

    component = subject.raw(KEYS.component_ingest_state(COMPONENT))
    device = subject.raw(KEYS.device_ingest_state(DEVICE))
    health = subject.raw(KEYS.connector_health(ACCOUNT))
    assert component is not None and device is not None and health is not None
    assert (component["entity_type"], device["entity_type"], health["entity_type"]) == (
        "component_ingest_state",
        "device_ingest_state",
        "connector_health",
    )
    assert component_ingest_state_from_item(component) == ComponentIngestState(
        tenant_id=TENANT,
        component_id=COMPONENT,
        device_id=DEVICE,
        last_received_at=RECEIVED,
        last_ingested_at=RECEIVED + timedelta(seconds=1),
        last_observed_at=OBSERVED,
        last_observed_sequence=10,
    )
    assert device_ingest_state_from_item(device) == DeviceIngestState(
        tenant_id=TENANT,
        device_id=DEVICE,
        last_received_at=RECEIVED,
        last_ingested_at=RECEIVED + timedelta(seconds=1),
    )
    assert connector_health_from_item(health) == ConnectorHealth(
        tenant_id=TENANT,
        integration_account_id=ACCOUNT,
        last_received_at=RECEIVED,
        last_accepted_at=RECEIVED,
        last_quarantined_at=RECEIVED,
        last_quarantine_reason="deployment_unresolved",
    )
    # Canonical fixed-6 strings, so lexicographic order is chronological (never isoformat()).
    assert component["last_received_at"] == "2026-10-30T12:00:00.000000Z"
    assert component["last_observed_sequence"] == 10
    assert isinstance(component["last_observed_sequence"], int)
    # Absent, never NULL: the monotone conditions test attribute_not_exists.
    assert "last_fenced_at" not in health and "last_fence_reason" not in health
    # Exactly the frozen attributes: nothing else (e.g. sequence events) is ever persisted.
    assert set(component) == {
        "PK",
        "SK",
        "entity_type",
        "tenant_id",
        "component_id",
        "device_id",
        "last_received_at",
        "last_ingested_at",
        "last_observed_at",
        "last_observed_sequence",
    }
    assert set(device) == {
        "PK",
        "SK",
        "entity_type",
        "tenant_id",
        "device_id",
        "last_received_at",
        "last_ingested_at",
    }
    assert set(health) == {
        "PK",
        "SK",
        "entity_type",
        "tenant_id",
        "integration_account_id",
        "last_received_at",
        "last_accepted_at",
        "last_quarantined_at",
        "last_quarantine_reason",
    }


@pytest.mark.asyncio
async def test_writes_are_single_item_updates(subject: DynamoIngestStateSubject) -> None:
    calls: list[dict[str, Any]] = []
    original = subject.client.update_item

    def spy(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return original(**kwargs)

    subject.client.update_item = spy  # type: ignore[method-assign]

    await subject.repository.apply_ingest_marks(
        marks(quarantined_at=RECEIVED, quarantine_reason="metric_unknown")
    )

    assert subject.client.transactions == []
    assert calls, "marks are written with conditional UpdateItem"
    for call in calls:
        condition = call["ConditionExpression"]
        assert condition.startswith("attribute_not_exists(#m) OR #m < :t"), condition


@pytest.mark.asyncio
async def test_identity_attributes_keep_the_first_writer(
    subject: DynamoIngestStateSubject,
) -> None:
    await subject.repository.apply_ingest_marks(marks())
    later = marks(received_at=RECEIVED + timedelta(minutes=1)).model_copy(
        update={"device_id": "dev-9"}
    )

    await subject.repository.apply_ingest_marks(later)

    raw = subject.raw(KEYS.component_ingest_state(COMPONENT))
    assert raw is not None
    assert (raw["tenant_id"], raw["component_id"], raw["device_id"]) == (TENANT, COMPONENT, DEVICE)
    assert raw["last_received_at"] == "2026-10-30T12:01:00.000000Z"


@pytest.mark.asyncio
async def test_component_states_read_each_distinct_component_once(
    subject: DynamoIngestStateSubject,
) -> None:
    await subject.repository.apply_ingest_marks(marks())
    subject.client.reads.clear()

    states = await subject.repository.get_component_states(
        TENANT, [COMPONENT, OTHER_COMPONENT, COMPONENT]
    )

    assert list(states) == [COMPONENT]
    component_reads = [pk for _, pk, _ in subject.client.reads]
    assert sorted(component_reads) == sorted(
        {f"COMPONENT#{COMPONENT}", f"COMPONENT#{OTHER_COMPONENT}"}
    )
    assert {kind for kind, _, _ in subject.client.reads} == {"get_item"}  # no Scan, no Query
    assert await subject.repository.get_component_states(TENANT, []) == {}


# Order independence -------------------------------------------------------------------------

COMPONENTS = (COMPONENT, OTHER_COMPONENT)
TIMES = tuple(RECEIVED + timedelta(seconds=step) for step in range(3))
OBSERVED_TIMES = tuple(OBSERVED + timedelta(seconds=step) for step in range(2))
SEQUENCES = (9, 10, 11)  # 9 vs 10 vs 11: numeric order differs from string order
QUARANTINE_REASONS = ("deployment_unresolved", "metric_unknown", "value_invalid")
FENCE_REASONS = ("account_revoked", "device_decommissioned", "integration_mismatch")


def random_message_marks(rng: random.Random) -> IngestMarks:
    received = rng.choice(TIMES)
    ingested = received + timedelta(seconds=rng.randint(1, 3))
    component_marks = []
    for component_id in sorted(rng.sample(COMPONENTS, rng.randint(0, 2))):
        observed = rng.random() < 0.7
        component_marks.append(
            ComponentMark(
                component_id=component_id,
                received_at=received,
                ingested_at=ingested,
                observed_at=rng.choice(OBSERVED_TIMES) if observed else None,
                sequence=rng.choice(SEQUENCES) if observed else None,
            )
        )
    quarantined = rng.random() < 0.5
    fenced = rng.random() < 0.3
    return IngestMarks(
        tenant_id=TENANT,
        device_id=DEVICE,
        integration_account_id=ACCOUNT,
        component_marks=tuple(component_marks),
        device_mark=DeviceMark(received_at=received, ingested_at=ingested),
        connector_mark=ConnectorMark(
            received_at=received,
            accepted_at=received if rng.random() < 0.8 else None,
            quarantined_at=received if quarantined else None,
            quarantine_reason=rng.choice(QUARANTINE_REASONS) if quarantined else None,
            fenced_at=received if fenced else None,
            fence_reason=rng.choice(FENCE_REASONS) if fenced else None,
        ),
    )


def redelivered(message: IngestMarks, rng: random.Random) -> IngestMarks:
    """Same message, processed again later: only the worker clock (`ingested_at`) differs."""
    shift = timedelta(seconds=rng.randint(1, 30))
    return message.model_copy(
        update={
            "component_marks": tuple(
                mark.model_copy(update={"ingested_at": mark.ingested_at + shift})
                for mark in message.component_marks
            ),
            "device_mark": message.device_mark.model_copy(
                update={"ingested_at": message.device_mark.ingested_at + shift}
            ),
        }
    )


def _max(values: Sequence[Any]) -> Any:
    present = [value for value in values if value is not None and value[0] is not None]
    return max(present) if present else None


def expected_rows(messages: Sequence[IngestMarks]) -> dict[str, Any]:
    """The reference final rows: the maximum of every mark, pairs compared as tuples."""
    components: dict[str, dict[str, Any]] = {}
    for message in messages:
        for mark in message.component_marks:
            row = components.setdefault(
                mark.component_id, {"received": [], "ingested": [], "observed": []}
            )
            row["received"].append((mark.received_at,))
            row["ingested"].append((mark.ingested_at,))
            row["observed"].append((mark.observed_at, mark.sequence))
    connector = [message.connector_mark for message in messages]
    return {
        "components": {
            component_id: (
                _max(row["received"])[0],
                _max(row["ingested"])[0],
                _max(row["observed"]) or (None, None),
            )
            for component_id, row in components.items()
        },
        "device": (
            _max([(m.device_mark.received_at,) for m in messages])[0],
            _max([(m.device_mark.ingested_at,) for m in messages])[0],
        ),
        "connector": (
            (_max([(c.received_at,) for c in connector]) or (None,))[0],
            (_max([(c.accepted_at,) for c in connector]) or (None,))[0],
            _max([(c.quarantined_at, c.quarantine_reason) for c in connector]) or (None, None),
            _max([(c.fenced_at, c.fence_reason) for c in connector]) or (None, None),
        ),
    }


async def final_rows(repository: Any, device_state: Callable[[str], Any]) -> dict[str, Any]:
    states = await repository.get_component_states(TENANT, list(COMPONENTS))
    health = await repository.get_connector_health(TENANT, ACCOUNT)
    device = await device_state(DEVICE)
    return {
        "components": {
            component_id: (
                state.last_received_at,
                state.last_ingested_at,
                (state.last_observed_at, state.last_observed_sequence),
            )
            for component_id, state in states.items()
        },
        "device": (device.last_received_at, device.last_ingested_at),
        "connector": (
            health.last_received_at,
            health.last_accepted_at,
            (health.last_quarantined_at, health.last_quarantine_reason),
            (health.last_fenced_at, health.last_fence_reason),
        ),
    }


def without_ingested(rows: dict[str, Any]) -> dict[str, Any]:
    return {
        "components": {key: (value[0], value[2]) for key, value in rows["components"].items()},
        "device": rows["device"][0],
        "connector": rows["connector"],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", range(12))
async def test_shuffled_concurrent_redelivered_marks_converge(seed: int) -> None:
    rng = random.Random(seed)
    messages = [random_message_marks(rng) for _ in range(rng.randint(4, 10))]
    deliveries = [*messages, *(redelivered(rng.choice(messages), rng) for _ in range(6))]
    reference = expected_rows(deliveries)
    finals = []
    for order in range(3):
        shuffled = list(deliveries)
        random.Random(seed * 100 + order).shuffle(shuffled)
        subject = DynamoIngestStateSubject()
        # Concurrent workers: every delivery in flight at once (each runs in a worker thread).
        await asyncio.gather(*(subject.repository.apply_ingest_marks(item) for item in shuffled))
        finals.append(await final_rows(subject.repository, subject.device_state))

    # Equality excludes last_ingested_at (the worker clock); it is still the maximum seen.
    assert all(without_ingested(final) == without_ingested(reference) for final in finals)
    assert all(final == reference for final in finals)

    # The P3-00 fake, applied sequentially, ends on the same rows: fake and adapter agree.
    store = IngestionStore(Faults())
    fake = FakeIngestStateRepository(store)
    for item in deliveries:
        await fake.apply_ingest_marks(item)

    async def fake_device(device_id: str) -> DeviceIngestState:
        return DeviceIngestState(device_id=device_id, **store.device_states[device_id])

    assert await final_rows(fake, fake_device) == finals[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("order", ["low_first", "high_first"])
async def test_sequence_ties_compare_numerically(
    subject: DynamoIngestStateSubject, order: str
) -> None:
    sequences = (9, 10) if order == "low_first" else (10, 9)

    for sequence in sequences:
        await subject.repository.apply_ingest_marks(marks(sequence=sequence))

    state = (await subject.repository.get_component_states(TENANT, [COMPONENT]))[COMPONENT]
    assert (state.last_observed_at, state.last_observed_sequence) == (OBSERVED, 10)


@pytest.mark.asyncio
@pytest.mark.parametrize("order", ["newer_first", "older_first"])
async def test_paired_marks_never_mix_halves(subject: DynamoIngestStateSubject, order: str) -> None:
    newer = marks(observed_at=OBSERVED + timedelta(seconds=1), sequence=3)
    older = marks(observed_at=OBSERVED, sequence=50)
    newer_quarantine = marks(
        quarantined_at=RECEIVED + timedelta(seconds=1), quarantine_reason="deployment_unresolved"
    ).model_copy(update={"component_marks": ()})
    older_quarantine = marks(quarantined_at=RECEIVED, quarantine_reason="value_invalid").model_copy(
        update={"component_marks": ()}
    )
    sequence = [newer, older, newer_quarantine, older_quarantine]
    if order == "older_first":
        sequence = [older, newer, older_quarantine, newer_quarantine]

    for item in sequence:
        await subject.repository.apply_ingest_marks(item)

    state = (await subject.repository.get_component_states(TENANT, [COMPONENT]))[COMPONENT]
    assert (state.last_observed_at, state.last_observed_sequence) == (
        OBSERVED + timedelta(seconds=1),
        3,
    )
    health = await subject.repository.get_connector_health(TENANT, ACCOUNT)
    assert health is not None
    assert (health.last_quarantined_at, health.last_quarantine_reason) == (
        RECEIVED + timedelta(seconds=1),
        "deployment_unresolved",
    )


@pytest.mark.asyncio
async def test_a_mark_without_an_observed_pair_leaves_the_pair_alone(
    subject: DynamoIngestStateSubject,
) -> None:
    await subject.repository.apply_ingest_marks(marks(sequence=10))

    await subject.repository.apply_ingest_marks(
        marks(received_at=RECEIVED + timedelta(minutes=1), observed_at=None, sequence=None)
    )

    state = (await subject.repository.get_component_states(TENANT, [COMPONENT]))[COMPONENT]
    assert state.last_received_at == RECEIVED + timedelta(minutes=1)
    assert (state.last_observed_at, state.last_observed_sequence) == (OBSERVED, 10)


@pytest.mark.asyncio
async def test_a_store_failure_propagates_unwrapped_on_a_later_write(
    subject: DynamoIngestStateSubject,
) -> None:
    error = store_errors()[0]
    original = subject.client.update_item
    sent: list[str] = []

    def fail_third(**kwargs: Any) -> dict[str, Any]:
        sent.append(repr(sorted(kwargs.items())))
        if len(sent) == 3:
            raise error
        return original(**kwargs)

    subject.client.update_item = fail_third  # type: ignore[method-assign]

    with pytest.raises(type(error)) as raised:
        await subject.repository.apply_ingest_marks(marks())
    assert raised.value is error
    assert len(sent) == len(set(sent))  # no request was re-sent (no internal retry)
    subject.client.update_item = original  # type: ignore[method-assign]
    await subject.repository.apply_ingest_marks(marks())  # a redelivery converges
    state = (await subject.repository.get_component_states(TENANT, [COMPONENT]))[COMPONENT]
    assert (state.last_received_at, state.last_observed_sequence) == (RECEIVED, 10)


@pytest.mark.asyncio
async def test_every_emitted_condition_is_a_frozen_dynamo_vector(
    subject: DynamoIngestStateSubject,
) -> None:
    """The adapter's conditions are exactly the frozen monotone forms the P3-00 vectors pin on
    the fake and on DynamoDB Local."""
    emitted: set[str] = set()
    original = subject.client.update_item

    def spy(**kwargs: Any) -> dict[str, Any]:
        emitted.add(kwargs["ConditionExpression"])
        return original(**kwargs)

    subject.client.update_item = spy  # type: ignore[method-assign]
    full = marks(quarantined_at=RECEIVED, quarantine_reason="metric_unknown").model_copy(
        update={
            "connector_mark": ConnectorMark(
                received_at=RECEIVED,
                accepted_at=RECEIVED,
                quarantined_at=RECEIVED,
                quarantine_reason="metric_unknown",
                fenced_at=RECEIVED,
                fence_reason="account_revoked",
            )
        }
    )
    await subject.repository.apply_ingest_marks(full)
    await subject.repository.apply_ingest_marks(full)  # every write now fails its condition

    frozen = {
        vector.request["ConditionExpression"]
        for vector in DYNAMO_EXPRESSION_VECTORS
        if "ConditionExpression" in vector.request
    }
    assert emitted == {
        "attribute_not_exists(#m) OR #m < :t",
        "attribute_not_exists(#m) OR #m < :t OR (#m = :t AND #tb < :tb)",
    }
    assert emitted <= frozen
