"""P3-06 on DynamoDB Local: the quarantine and ingest-state adapters pass the P3-00 contract kits
(fault scenarios included) and converge under concurrent, shuffled, redelivered marks."""

import asyncio
import math
import random
from collections import defaultdict
from collections.abc import Callable, Iterator
from datetime import datetime, timedelta
from typing import Any

import pytest

from limnopulse_api.adapters.dynamodb_ingest_state import DynamoIngestStateRepository
from limnopulse_api.adapters.dynamodb_quarantine import DynamoQuarantineRepository
from limnopulse_api.adapters.dynamodb_v2 import (
    DynamoV2KeyBuilder,
    device_ingest_state_from_item,
)
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
from limnopulse_api.services.ingest_state import IngestStateService
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

    async def device_state(self, device_id: str) -> DeviceIngestState | None:
        item = await self.state.get_item(KEYS.device_ingest_state(device_id), consistent=True)
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

    item = await local.quarantine.get_item(
        KEYS.quarantine_entry(TENANT, entry.received_at, entry.quarantine_id), consistent=True
    )

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
            accepted_at=received,
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


def _reference(messages: list[IngestMarks]) -> dict[str, Any]:
    def best(pairs: list[tuple[Any, Any]]) -> tuple[Any, Any]:
        present = [pair for pair in pairs if pair[0] is not None]
        return max(present) if present else (None, None)

    return {
        component_id: best(
            [
                (mark.observed_at, mark.sequence)
                for message in messages
                for mark in message.component_marks
                if mark.component_id == component_id
            ]
        )
        for component_id in (COMPONENT, OTHER_COMPONENT)
    } | {
        "received": max(message.device_mark.received_at for message in messages),
        "ingested": max(message.device_mark.ingested_at for message in messages),
        "quarantine": best(
            [
                (m.connector_mark.quarantined_at, m.connector_mark.quarantine_reason)
                for m in messages
            ]
        ),
        "fence": best(
            [(m.connector_mark.fenced_at, m.connector_mark.fence_reason) for m in messages]
        ),
    }


async def _observed(local: LocalSubject) -> dict[str, Any]:
    states = await local.state.get_component_states(TENANT, [COMPONENT, OTHER_COMPONENT])
    health = await local.state.get_connector_health(TENANT, ACCOUNT)
    device = await local.device_state(DEVICE)
    assert health is not None and device is not None
    return {
        component_id: (state.last_observed_at, state.last_observed_sequence)
        for component_id, state in states.items()
    } | {
        "received": device.last_received_at,
        "ingested": device.last_ingested_at,
        "quarantine": (health.last_quarantined_at, health.last_quarantine_reason),
        "fence": (health.last_fenced_at, health.last_fence_reason),
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", range(3))
async def test_concurrent_shuffled_marks_converge_on_dynamodb_local(
    local: LocalSubject, seed: int
) -> None:
    rng = random.Random(seed)
    messages = [_message(rng) for _ in range(8)]
    deliveries = messages + [rng.choice(messages) for _ in range(8)]  # duplicates
    rng.shuffle(deliveries)

    await asyncio.gather(*(local.state.apply_ingest_marks(item) for item in deliveries))

    assert await _observed(local) == _reference(deliveries)


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
