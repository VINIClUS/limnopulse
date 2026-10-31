"""P3-06 on DynamoDB Local: the quarantine and ingest-state adapters pass the P3-00 contract kits
(fault scenarios included) and converge under concurrent, shuffled, redelivered marks."""

import asyncio
import math
import random
from collections import defaultdict
from collections.abc import Callable, Iterator
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from limnopulse_api.adapters.dynamodb_ingest_state import DynamoIngestStateRepository
from limnopulse_api.adapters.dynamodb_quarantine import DynamoQuarantineRepository
from limnopulse_api.services.ingest_state import IngestStateService

from limnopulse_api.adapters.dynamodb_v2 import (
    DynamoV2KeyBuilder,
    device_ingest_state_from_item,
)
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.ingestion import (
    QUARANTINE_TTL,
    ComponentMark,
    ConnectorMark,
    DeviceIngestState,
    DeviceMark,
    IngestMarks,
    IngressFence,
    IngressFenceReason,
    QuarantineEntryStatus,
    SequenceEvent,
)
from limnopulse_api.services.telemetry_pipeline import PipelineOutcome
from limnopulse_api.services.telemetry_v2_writes import TelemetryWriteOutcome
from tests.support.ingestion_contract_kits import (
    ACCOUNT,
    COMPONENT,
    DEVICE,
    INGEST_STATE_CONTRACT,
    OBSERVED,
    OTHER_COMPONENT,
    QUARANTINE_REPOSITORY_CONTRACT,
    RECEIVED,
    TENANT,
    canonical_observation,
    envelope,
    fenced_entries,
    kit_ids,
)
from tests.support.ingestion_harness import IngestionTables, dynamodb_client

KEYS = DynamoV2KeyBuilder()
_SERIALIZER = TypeSerializer()
_DESERIALIZER = TypeDeserializer()


def _encode(values: dict[str, Any]) -> dict[str, Any]:
    return {key: _SERIALIZER.serialize(value) for key, value in values.items()}


def _decode(values: dict[str, Any]) -> dict[str, Any]:
    def plain(value: Any) -> Any:
        if isinstance(value, Decimal):
            return int(value) if value == value.to_integral_value() else value
        return value

    return {key: plain(_DESERIALIZER.deserialize(value)) for key, value in values.items()}


_BOTO_OPERATION = {
    "put_quarantine_entries": "put_item",
    "list_quarantine_entries": "query",
    "get_quarantine_entry": "get_item",
    "transition_quarantine_entry": "update_item",
    "apply_ingest_marks": "update_item",
    "get_component_states": "get_item",
    "get_connector_health": "get_item",
}


class FaultingClient:
    """A boto3 DynamoDB client whose next call of an operation can raise once (the kit's
    `fail_next`, injected below the adapter so the adapter's own error handling is exercised)."""

    def __init__(self, client: Any) -> None:
        self._client = client
        self._faults: dict[str, list[BaseException]] = defaultdict(list)

    def fail_next(self, operation: str, exc: BaseException) -> None:
        self._faults[operation].append(exc)

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self._client, name)
        if not callable(attribute):
            return attribute

        def call(*args: Any, **kwargs: Any) -> Any:
            pending = self._faults.get(name)
            if pending:
                raise pending.pop(0)
            return attribute(*args, **kwargs)

        return call


class LocalSubject:
    def __init__(self, tables: IngestionTables) -> None:
        self.client = FaultingClient(dynamodb_client())
        self.table = tables.domain
        self.now = RECEIVED
        self.quarantine = DynamoQuarantineRepository(
            tables.domain, self.client, clock=lambda: self.now
        )
        self.state = DynamoIngestStateRepository(tables.domain, self.client)
        self.repository: Any = None

    def set_now(self, now: datetime) -> None:
        self.now = now

    def fail_next(self, operation: str, exc: BaseException) -> None:
        self.client.fail_next(_BOTO_OPERATION[operation], exc)

    def raw(self, key: dict[str, str]) -> dict[str, Any] | None:
        """The stored row, read with the plain boto3 client (independent of the adapters)."""
        response = self.client.get_item(TableName=self.table, Key=_encode(key), ConsistentRead=True)
        item = response.get("Item")
        return None if item is None else _decode(item)

    async def device_state(self, device_id: str) -> DeviceIngestState | None:
        item = self.raw(KEYS.device_ingest_state(device_id))
        return None if item is None else device_ingest_state_from_item(item)


@pytest.fixture
def local(ingestion_tables: IngestionTables) -> Iterator[LocalSubject]:
    yield LocalSubject(ingestion_tables)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario", QUARANTINE_REPOSITORY_CONTRACT, ids=kit_ids(QUARANTINE_REPOSITORY_CONTRACT)
)
async def test_quarantine_adapter_passes_the_kit_on_dynamodb_local(
    scenario: Callable[[Any], Any], local: LocalSubject
) -> None:
    local.repository = local.quarantine
    await scenario(local)


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", INGEST_STATE_CONTRACT, ids=kit_ids(INGEST_STATE_CONTRACT))
async def test_ingest_state_adapter_passes_the_kit_on_dynamodb_local(
    scenario: Callable[[Any], Any], local: LocalSubject
) -> None:
    local.repository = local.state
    await scenario(local)


@pytest.mark.asyncio
async def test_quarantine_ttl_attribute_on_dynamodb_local(local: LocalSubject) -> None:
    now = RECEIVED + timedelta(microseconds=250_000)
    (entry, *_rest) = fenced_entries(1, now=now)
    await local.quarantine.put_quarantine_entries([entry])

    item = local.raw(KEYS.quarantine_entry(TENANT, entry.received_at, entry.quarantine_id))

    assert item is not None
    assert item["expires_at"] == math.ceil((now + QUARANTINE_TTL).timestamp())


@pytest.mark.asyncio
async def test_quarantine_redelivery_never_resets_on_dynamodb_local(local: LocalSubject) -> None:
    entries = fenced_entries()
    await local.quarantine.put_quarantine_entries(entries)
    dismissed = await local.quarantine.transition_quarantine_entry(
        TENANT,
        entries[0].entry_key,
        QuarantineEntryStatus.DISMISSED,
        expected_version=1,
        at=RECEIVED,
    )

    await asyncio.gather(*(local.quarantine.put_quarantine_entries(entries) for _ in range(4)))

    assert await local.quarantine.get_quarantine_entry(TENANT, entries[0].entry_key) == dismissed


@pytest.mark.asyncio
async def test_quarantine_transition_conflicts_and_expiry_on_dynamodb_local(
    local: LocalSubject,
) -> None:
    entries = fenced_entries()
    await local.quarantine.put_quarantine_entries(entries)
    first, second = entries[0].entry_key, entries[1].entry_key

    with pytest.raises(ConflictError):  # stale version: the real ConditionalCheckFailed shape
        await local.quarantine.transition_quarantine_entry(
            TENANT, first, QuarantineEntryStatus.DISMISSED, expected_version=2, at=RECEIVED
        )
    replayed = await local.quarantine.transition_quarantine_entry(
        TENANT,
        first,
        QuarantineEntryStatus.REPLAYED,
        expected_version=1,
        at=RECEIVED,
        replayed_ingress_id="ing_" + "5" * 32,
    )
    with pytest.raises(ConflictError):  # terminal
        await local.quarantine.transition_quarantine_entry(
            TENANT, first, QuarantineEntryStatus.DISMISSED, expected_version=2, at=RECEIVED
        )
    assert await local.quarantine.get_quarantine_entry(TENANT, first) == replayed

    local.set_now(entries[1].created_at + QUARANTINE_TTL)
    with pytest.raises(NotFoundError):  # logically expired, although DynamoDB still holds it
        await local.quarantine.transition_quarantine_entry(
            TENANT, second, QuarantineEntryStatus.DISMISSED, expected_version=1, at=local.now
        )
    assert local.raw(
        KEYS.quarantine_entry(TENANT, entries[1].received_at, entries[1].quarantine_id)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("attempt", range(3))
async def test_concurrent_transitions_on_one_entry_on_dynamodb_local(
    local: LocalSubject, attempt: int
) -> None:
    entries = fenced_entries()
    await local.quarantine.put_quarantine_entries(entries)
    key = entries[attempt].entry_key

    results = await asyncio.gather(
        local.quarantine.transition_quarantine_entry(
            TENANT, key, QuarantineEntryStatus.DISMISSED, expected_version=1, at=RECEIVED
        ),
        local.quarantine.transition_quarantine_entry(
            TENANT,
            key,
            QuarantineEntryStatus.REPLAYED,
            expected_version=1,
            at=RECEIVED,
            replayed_ingress_id="ing_" + "5" * 32,
        ),
        return_exceptions=True,
    )

    winners = [result for result in results if not isinstance(result, BaseException)]
    losers = [result for result in results if isinstance(result, BaseException)]
    assert len(winners) == 1 and len(losers) == 1
    assert isinstance(losers[0], ConflictError)
    assert await local.quarantine.get_quarantine_entry(TENANT, key) == winners[0]
    assert winners[0].version == 2


# Order independence under real concurrency ---------------------------------------------------

TIMES = tuple(RECEIVED + timedelta(seconds=step) for step in range(3))
OBSERVED_TIMES = tuple(OBSERVED + timedelta(seconds=step) for step in range(2))


def _message(rng: random.Random) -> IngestMarks:
    received = rng.choice(TIMES)
    ingested = received + timedelta(seconds=rng.randint(1, 3))
    observed = rng.random() < 0.8
    quarantined = rng.random() < 0.5
    fenced = rng.random() < 0.3
    return IngestMarks(
        tenant_id=TENANT,
        device_id=DEVICE,
        integration_account_id=ACCOUNT,
        component_marks=tuple(
            ComponentMark(
                component_id=component_id,
                received_at=received,
                ingested_at=ingested,
                observed_at=rng.choice(OBSERVED_TIMES) if observed else None,
                sequence=rng.choice((9, 10, 11)) if observed else None,
            )
            for component_id in (COMPONENT, OTHER_COMPONENT)
        ),
        device_mark=DeviceMark(received_at=received, ingested_at=ingested),
        connector_mark=ConnectorMark(
            received_at=received,
            accepted_at=received if rng.random() < 0.7 else None,
            quarantined_at=received if quarantined else None,
            quarantine_reason=rng.choice(("metric_unknown", "value_invalid"))
            if quarantined
            else None,
            fenced_at=received if fenced else None,
            fence_reason=rng.choice(("account_revoked", "integration_mismatch"))
            if fenced
            else None,
        ),
    )


def _redelivered(message: IngestMarks, shift: timedelta) -> IngestMarks:
    """The same message processed again later: only the worker clock (`ingested_at`) moves."""
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


def _best(pairs: list[tuple[Any, Any]]) -> tuple[Any, Any]:
    present = [pair for pair in pairs if pair[0] is not None]
    return max(present) if present else (None, None)


def _reference(messages: list[IngestMarks]) -> dict[str, Any]:
    rows: dict[str, Any] = {}
    for component_id in (COMPONENT, OTHER_COMPONENT):
        component = [
            mark
            for message in messages
            for mark in message.component_marks
            if mark.component_id == component_id
        ]
        rows[component_id] = (
            max(mark.received_at for mark in component),
            max(mark.ingested_at for mark in component),
            _best([(mark.observed_at, mark.sequence) for mark in component]),
        )
    connector = [message.connector_mark for message in messages]
    rows["device"] = (
        max(message.device_mark.received_at for message in messages),
        max(message.device_mark.ingested_at for message in messages),
    )
    rows["connector"] = (
        _best([(c.received_at, None) for c in connector])[0],
        _best([(c.accepted_at, None) for c in connector])[0],
        _best([(c.quarantined_at, c.quarantine_reason) for c in connector]),
        _best([(c.fenced_at, c.fence_reason) for c in connector]),
    )
    return rows


async def _observed(local: LocalSubject) -> dict[str, Any]:
    states = await local.state.get_component_states(TENANT, [COMPONENT, OTHER_COMPONENT])
    health = await local.state.get_connector_health(TENANT, ACCOUNT)
    device = await local.device_state(DEVICE)
    assert health is not None and device is not None
    rows: dict[str, Any] = {
        component_id: (
            state.last_received_at,
            state.last_ingested_at,
            (state.last_observed_at, state.last_observed_sequence),
        )
        for component_id, state in states.items()
    }
    rows["device"] = (device.last_received_at, device.last_ingested_at)
    rows["connector"] = (
        health.last_received_at,
        health.last_accepted_at,
        (health.last_quarantined_at, health.last_quarantine_reason),
        (health.last_fenced_at, health.last_fence_reason),
    )
    return rows


def _without_ingested(rows: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value[::2] if key in (COMPONENT, OTHER_COMPONENT) else value
        for key, value in rows.items()
        if key != "device"
    } | {"device": rows["device"][0]}


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", range(3))
async def test_concurrent_shuffled_marks_converge_on_dynamodb_local(
    local: LocalSubject, seed: int
) -> None:
    rng = random.Random(seed)
    messages = [_message(rng) for _ in range(8)]
    deliveries = messages + [
        _redelivered(rng.choice(messages), timedelta(seconds=rng.randint(1, 30))) for _ in range(8)
    ]
    rng.shuffle(deliveries)
    reference = _reference(deliveries)

    await asyncio.gather(*(local.state.apply_ingest_marks(item) for item in deliveries))

    observed = await _observed(local)
    assert _without_ingested(observed) == _without_ingested(reference)
    assert observed == reference  # last_ingested_at is still the maximum seen


@pytest.mark.asyncio
async def test_concurrent_ties_resolve_by_the_tie_breaker_on_dynamodb_local(
    local: LocalSubject,
) -> None:
    def tied(sequence: int, reason: str) -> IngestMarks:
        return IngestMarks(
            tenant_id=TENANT,
            device_id=DEVICE,
            integration_account_id=ACCOUNT,
            component_marks=(
                ComponentMark(
                    component_id=COMPONENT,
                    received_at=RECEIVED,
                    ingested_at=RECEIVED,
                    observed_at=OBSERVED,
                    sequence=sequence,
                ),
            ),
            device_mark=DeviceMark(received_at=RECEIVED, ingested_at=RECEIVED),
            connector_mark=ConnectorMark(quarantined_at=RECEIVED, quarantine_reason=reason),
        )

    batch = [
        tied(sequence, reason) for sequence in (8, 9, 10) for reason in ("a_reason", "z_reason")
    ]
    await asyncio.gather(*(local.state.apply_ingest_marks(item) for item in batch * 3))

    state = (await local.state.get_component_states(TENANT, [COMPONENT]))[COMPONENT]
    health = await local.state.get_connector_health(TENANT, ACCOUNT)
    assert state.last_observed_sequence == 10
    assert health is not None and health.last_quarantine_reason == "z_reason"


# Service on DynamoDB Local -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_service_marks_classifies_and_ignores_replays_on_dynamodb_local(
    local: LocalSubject,
) -> None:
    service = IngestStateService(local.state, clock=lambda: RECEIVED + timedelta(seconds=5))

    def accepted(sequence: int, observed_at: datetime) -> PipelineOutcome:
        return PipelineOutcome(
            written=TelemetryWriteOutcome(v2_points=1, legacy_points=0, legacy_excluded=0),
            quarantined=(),
            counts={},
            accepted=(canonical_observation(observed_at=observed_at, sequence=sequence),),
        )

    first = await service.record_outcome(envelope(), accepted(10, OBSERVED), receive_count=1)
    gap = await service.record_outcome(
        envelope(ingress_id="ing_" + "1" * 32, received_at=RECEIVED + timedelta(seconds=1)),
        accepted(13, OBSERVED + timedelta(seconds=1)),
        receive_count=1,
    )
    replay = await service.record_outcome(
        envelope(
            ingress_id="ing_" + "2" * 32,
            replay=True,
            replay_of="ing_" + "3" * 32,
            received_at=RECEIVED + timedelta(days=3),
        ),
        accepted(99, OBSERVED + timedelta(days=3)),
        receive_count=1,
    )
    await service.record_fence(
        envelope(received_at=RECEIVED + timedelta(seconds=2)),
        IngressFence(reason=IngressFenceReason.DEVICE_DECOMMISSIONED, account_verified=True),
    )

    assert (first, gap, replay) == ((SequenceEvent.FIRST,), (SequenceEvent.GAP,), ())
    state = (await local.state.get_component_states(TENANT, [COMPONENT]))[COMPONENT]
    assert (state.last_received_at, state.last_observed_at, state.last_observed_sequence) == (
        RECEIVED + timedelta(seconds=1),
        OBSERVED + timedelta(seconds=1),
        13,
    )
    health = await local.state.get_connector_health(TENANT, ACCOUNT)
    assert health is not None
    assert (health.last_received_at, health.last_fenced_at, health.last_fence_reason) == (
        RECEIVED + timedelta(seconds=1),
        RECEIVED + timedelta(seconds=2),
        "device_decommissioned",
    )
