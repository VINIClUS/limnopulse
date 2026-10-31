"""P3-06: `DynamoQuarantineRepository` on the in-memory Dynamo fake (D9, ADR-006)."""

import json
import math
import random
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

import pytest

from limnopulse_api.adapters.dynamodb_quarantine import DynamoQuarantineRepository
from limnopulse_api.adapters.dynamodb_v2 import DynamoV2KeyBuilder, quarantine_entry_from_item
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.ingestion import (
    QUARANTINE_TTL,
    IngressFence,
    IngressFenceReason,
    QuarantineEntry,
    QuarantineEntryStatus,
    build_quarantine_entries,
)
from limnopulse_api.domain.telemetry_v2 import QuarantinedObservation, QuarantineReason
from limnopulse_api.repositories.ingestion import QuarantineRepository
from limnopulse_api.services.telemetry_pipeline import PipelineOutcome
from limnopulse_api.services.telemetry_v2_writes import TelemetryWriteOutcome
from tests.support.ingestion_contract_kits import (
    COMPONENT,
    DEVICE,
    DYNAMO_EXPRESSION_VECTORS,
    OBSERVED,
    QUARANTINE_REPOSITORY_CONTRACT,
    RECEIVED,
    TENANT,
    envelope,
    fenced_entries,
    kit_ids,
    source_observation,
    store_errors,
)
from tests.support.ingestion_dynamo_fake import IngestionDynamoClient

TABLE = "LimnopulseDomain"
KEYS = DynamoV2KeyBuilder()
REPLAY_ID = "ing_" + "5" * 32

# Port operation -> the boto3 operation the adapter performs for it (the fault point).
_BOTO_OPERATION = {
    "put_quarantine_entries": "put_item",
    "list_quarantine_entries": "query",
    "get_quarantine_entry": "get_item",
    "transition_quarantine_entry": "update_item",
}


class DynamoQuarantineSubject:
    """`QuarantineKitSubject` over the real adapter and the P3-00 Dynamo fake."""

    def __init__(self) -> None:
        self.client = IngestionDynamoClient()
        self.now = RECEIVED
        self.repository = DynamoQuarantineRepository(TABLE, self.client, clock=lambda: self.now)

    def set_now(self, now: datetime) -> None:
        self.now = now

    def fail_next(self, operation: str, exc: BaseException) -> None:
        self.client.fail_next(_BOTO_OPERATION[operation], exc)

    def raw(self, entry: QuarantineEntry) -> dict[str, Any] | None:
        key = KEYS.quarantine_entry(entry.tenant_id, entry.received_at, entry.quarantine_id)
        return self.client.items.get((key["PK"], key["SK"]))


@pytest.fixture
def subject() -> DynamoQuarantineSubject:
    return DynamoQuarantineSubject()


async def _all(
    repository: Any, *, limit: int = 2, tenant_id: str = TENANT, **filters: Any
) -> list[QuarantineEntry]:
    collected: list[QuarantineEntry] = []
    after: str | None = None
    for _ in range(200):
        page = await repository.list_quarantine_entries(
            tenant_id, after_sk=after, limit=limit, **filters
        )
        assert len(page.entries) <= limit
        collected.extend(page.entries)
        if page.next_after_sk is None:
            return collected
        assert after is None or page.next_after_sk > after
        after = page.next_after_sk
    raise AssertionError("pagination did not terminate")


def _quarantined(reason: QuarantineReason, *, value: float = 1.0) -> QuarantinedObservation:
    excerpt = json.dumps(
        source_observation(value=value).model_dump(mode="json", exclude_none=True),
        separators=(",", ":"),
    )
    return QuarantinedObservation(
        reason=reason,
        tenant_id=TENANT,
        device_id=DEVICE,
        component_id=COMPONENT,
        metric_key="temperature",
        observed_at=OBSERVED,
        received_at=RECEIVED,
        source_event_id=None,
        detail="quarantined",
        source_excerpt_json=excerpt,
        truncated=False,
    )


def _outcome(*quarantined: QuarantinedObservation) -> PipelineOutcome:
    return PipelineOutcome(
        written=TelemetryWriteOutcome(v2_points=0, legacy_points=0, legacy_excluded=0),
        quarantined=quarantined,
        counts={},
    )


def _ingress(index: int) -> str:
    return f"ing_{index:032x}"


def _entries_at(
    received_at: datetime, index: int, *, count: int = 2
) -> tuple[QuarantineEntry, ...]:
    observations = [
        source_observation(observed_at=OBSERVED + timedelta(seconds=n)) for n in range(count)
    ]
    return build_quarantine_entries(
        envelope(ingress_id=_ingress(index), received_at=received_at, observations=observations),
        fence=IngressFence(reason=IngressFenceReason.ACCOUNT_REVOKED, account_verified=True),
        now=RECEIVED,
    )


def test_adapter_implements_the_port(subject: DynamoQuarantineSubject) -> None:
    assert isinstance(subject.repository, QuarantineRepository)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario", QUARANTINE_REPOSITORY_CONTRACT, ids=kit_ids(QUARANTINE_REPOSITORY_CONTRACT)
)
async def test_adapter_passes_the_quarantine_contract_kit(
    scenario: Callable[[Any], Any], subject: DynamoQuarantineSubject
) -> None:
    await scenario(subject)


# Idempotent create ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_persisting_the_same_entries_twice_leaves_them_unchanged(
    subject: DynamoQuarantineSubject,
) -> None:
    entries = fenced_entries()
    await subject.repository.put_quarantine_entries(entries)
    first = [subject.raw(entry) for entry in entries]
    # A redelivery rebuilds byte-identical ids later (another worker clock); never an overwrite.
    redelivered = build_quarantine_entries(
        envelope(
            observations=[
                source_observation(observed_at=OBSERVED + timedelta(seconds=n)) for n in range(3)
            ]
        ),
        fence=IngressFence(
            reason=IngressFenceReason.INTEGRATION_DECOMMISSIONED, account_verified=True
        ),
        now=RECEIVED + timedelta(hours=1),
    )
    assert [item.entry_key for item in redelivered] == [item.entry_key for item in entries]

    await subject.repository.put_quarantine_entries(redelivered)

    assert [subject.raw(entry) for entry in entries] == first
    for entry in entries:
        assert await subject.repository.get_quarantine_entry(TENANT, entry.entry_key) == entry


@pytest.mark.asyncio
async def test_every_write_is_a_single_item_conditional_put(
    subject: DynamoQuarantineSubject,
) -> None:
    entries = fenced_entries()
    calls: list[dict[str, Any]] = []
    original = subject.client.put_item

    def spy(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return original(**kwargs)

    subject.client.put_item = spy  # type: ignore[method-assign]

    await subject.repository.put_quarantine_entries(entries)
    first_write = len(calls)
    await subject.repository.put_quarantine_entries(entries)

    assert subject.client.transactions == []
    assert first_write == len(entries)  # one PutItem per row
    assert {call["ConditionExpression"] for call in calls} == {"attribute_not_exists(PK)"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("target", "replayed_ingress_id"),
    [(QuarantineEntryStatus.DISMISSED, None), (QuarantineEntryStatus.REPLAYED, REPLAY_ID)],
)
async def test_redelivery_never_resets_a_terminal_status(
    subject: DynamoQuarantineSubject,
    target: QuarantineEntryStatus,
    replayed_ingress_id: str | None,
) -> None:
    entries = fenced_entries()
    await subject.repository.put_quarantine_entries(entries)
    moved = await subject.repository.transition_quarantine_entry(
        TENANT,
        entries[1].entry_key,
        target,
        expected_version=1,
        at=RECEIVED + timedelta(minutes=1),
        replayed_ingress_id=replayed_ingress_id,
    )

    await subject.repository.put_quarantine_entries(entries)

    stored = await subject.repository.get_quarantine_entry(TENANT, entries[1].entry_key)
    assert stored == moved
    assert (stored.status, stored.version, stored.replayed_ingress_id) == (
        target,
        2,
        replayed_ingress_id,
    )


@pytest.mark.asyncio
async def test_identical_excerpts_with_distinct_ids_both_persist(
    subject: DynamoQuarantineSubject,
) -> None:
    same = source_observation()
    fenced = build_quarantine_entries(
        envelope(observations=[same, same]),
        fence=IngressFence(reason=IngressFenceReason.DEVICE_DECOMMISSIONED, account_verified=True),
        now=RECEIVED,
    )
    duplicate = _quarantined(QuarantineReason.METRIC_UNKNOWN)
    quarantined = build_quarantine_entries(
        envelope(ingress_id=_ingress(9)), outcome=_outcome(duplicate, duplicate), now=RECEIVED
    )
    entries = (*fenced, *quarantined)
    assert len({entry.entry_key for entry in entries}) == len(entries) == 5

    await subject.repository.put_quarantine_entries(entries)

    listed = await _all(subject.repository, limit=10)
    assert sorted(item.entry_key for item in listed) == sorted(item.entry_key for item in entries)


@pytest.mark.asyncio
async def test_a_store_failure_mid_batch_propagates_and_a_retry_completes(
    subject: DynamoQuarantineSubject,
) -> None:
    entries = fenced_entries()
    error = store_errors()[0]
    original = subject.client.put_item
    sent: list[str] = []

    def fail_second(**kwargs: Any) -> dict[str, Any]:
        sent.append(kwargs["Item"]["SK"]["S"])
        if len(sent) == 2:
            raise error
        return original(**kwargs)

    subject.client.put_item = fail_second  # type: ignore[method-assign]

    with pytest.raises(type(error)) as raised:
        await subject.repository.put_quarantine_entries(entries)

    assert raised.value is error  # neither swallowed nor re-wrapped
    assert len(sent) == len(set(sent))  # no internal retry of the failed row
    assert len(await _all(subject.repository, limit=10)) == 1  # the first row is persisted
    await subject.repository.put_quarantine_entries(entries)  # the redelivery completes the rest
    listed = await _all(subject.repository, limit=10)
    assert sorted(item.entry_key for item in listed) == sorted(item.entry_key for item in entries)


# TTL -----------------------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("microseconds", [0, 1, 999_999])
async def test_ttl_attribute_is_the_ceiling_of_the_epoch(
    subject: DynamoQuarantineSubject, microseconds: int
) -> None:
    now = RECEIVED + timedelta(microseconds=microseconds)
    entries = fenced_entries(1, now=now)

    await subject.repository.put_quarantine_entries(entries)

    raw = subject.raw(entries[0])
    assert raw is not None
    expected = math.ceil((now + QUARANTINE_TTL).timestamp())
    assert raw["expires_at"] == expected
    assert isinstance(raw["expires_at"], int)
    assert raw["expires_at"] >= (now + QUARANTINE_TTL).timestamp()
    assert raw["entity_type"] == "telemetry_quarantine"
    assert quarantine_entry_from_item(raw) == entries[0]


# Listing -------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_is_time_ordered_and_pages_from_after_sk(
    subject: DynamoQuarantineSubject,
) -> None:
    received = [RECEIVED + timedelta(seconds=step) for step in (0, 1, 2, 3, 4)]
    # Higher ingress ids get earlier receipt times, so id order differs from time order.
    entries = [
        entry
        for index, at in enumerate(received)
        for entry in _entries_at(at, len(received) - index)
    ]
    shuffled = list(entries)
    random.Random(7).shuffle(shuffled)
    await subject.repository.put_quarantine_entries(shuffled)
    expected = sorted(entries, key=lambda item: item.entry_key)
    assert [item.received_at for item in expected] == sorted(item.received_at for item in entries)

    for limit in (1, 2, 3, 7, 100):
        assert await _all(subject.repository, limit=limit) == expected

    middle = expected[6].entry_key
    page = await subject.repository.list_quarantine_entries(TENANT, after_sk=middle, limit=3)
    assert list(page.entries) == expected[7:10]
    assert page.next_after_sk is not None
    rest = await subject.repository.list_quarantine_entries(
        TENANT, after_sk=page.next_after_sk, limit=100
    )
    assert list(rest.entries) == expected[10:]
    assert rest.next_after_sk is None


@pytest.mark.asyncio
async def test_list_rejects_a_non_positive_limit(subject: DynamoQuarantineSubject) -> None:
    with pytest.raises(ValueError):
        await subject.repository.list_quarantine_entries(TENANT, after_sk=None, limit=0)


@pytest.mark.asyncio
async def test_reason_and_status_filters_combine(subject: DynamoQuarantineSubject) -> None:
    fenced = _entries_at(RECEIVED, 1, count=3)
    quarantined = build_quarantine_entries(
        envelope(ingress_id=_ingress(2), received_at=RECEIVED + timedelta(seconds=1)),
        outcome=_outcome(
            _quarantined(QuarantineReason.METRIC_UNKNOWN, value=1.0),
            _quarantined(QuarantineReason.DEPLOYMENT_UNRESOLVED, value=2.0),
            _quarantined(QuarantineReason.METRIC_UNKNOWN, value=3.0),
        ),
        now=RECEIVED,
    )
    await subject.repository.put_quarantine_entries((*fenced, *quarantined))
    unknown = [entry for entry in quarantined if entry.reason == "metric_unknown"]
    dismissed = await subject.repository.transition_quarantine_entry(
        TENANT,
        unknown[0].entry_key,
        QuarantineEntryStatus.DISMISSED,
        expected_version=1,
        at=RECEIVED,
    )

    def keys(items: list[QuarantineEntry]) -> set[str]:
        return {item.entry_key for item in items}

    for limit in (1, 2, 50):
        assert keys(await _all(subject.repository, limit=limit, reason="metric_unknown")) == keys(
            unknown
        )
        assert keys(
            await _all(
                subject.repository,
                limit=limit,
                reason="metric_unknown",
                status=QuarantineEntryStatus.OPEN,
            )
        ) == keys(unknown[1:])
        assert await _all(
            subject.repository,
            limit=limit,
            reason="metric_unknown",
            status=QuarantineEntryStatus.DISMISSED,
        ) == [dismissed]
        assert keys(
            await _all(subject.repository, limit=limit, reason="fenced_account_revoked")
        ) == keys(list(fenced))
        assert keys(
            await _all(subject.repository, limit=limit, status=QuarantineEntryStatus.OPEN)
        ) == keys([*fenced, *quarantined]) - {dismissed.entry_key}
        assert (
            await _all(subject.repository, limit=limit, status=QuarantineEntryStatus.REPLAYED) == []
        )


# Logical expiry ------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_expiry_is_decided_on_created_at_not_received_at(
    subject: DynamoQuarantineSubject,
) -> None:
    # A message received long ago but quarantined now (e.g. a DLQ redrive) is still live.
    late = build_quarantine_entries(
        envelope(ingress_id=_ingress(1), received_at=RECEIVED - timedelta(days=20)),
        fence=IngressFence(reason=IngressFenceReason.ACCOUNT_REVOKED, account_verified=True),
        now=RECEIVED,
    )
    early = fenced_entries(1, now=RECEIVED - timedelta(days=10))
    subject.set_now(RECEIVED)
    await subject.repository.put_quarantine_entries((*late, *early))

    subject.set_now(RECEIVED + timedelta(days=5))

    assert {item.entry_key for item in await _all(subject.repository, limit=1)} == {
        item.entry_key for item in late
    }
    assert await subject.repository.get_quarantine_entry(TENANT, early[0].entry_key) is None
    assert await subject.repository.get_quarantine_entry(TENANT, late[0].entry_key) == late[0]
    with pytest.raises(NotFoundError):
        await subject.repository.transition_quarantine_entry(
            TENANT,
            early[0].entry_key,
            QuarantineEntryStatus.DISMISSED,
            expected_version=1,
            at=RECEIVED,
        )
    dismissed = await subject.repository.transition_quarantine_entry(
        TENANT, late[0].entry_key, QuarantineEntryStatus.DISMISSED, expected_version=1, at=RECEIVED
    )
    assert dismissed.status == QuarantineEntryStatus.DISMISSED


@pytest.mark.asyncio
async def test_transition_expiry_boundary_is_created_at_plus_ttl(
    subject: DynamoQuarantineSubject,
) -> None:
    entries = fenced_entries()
    await subject.repository.put_quarantine_entries(entries)
    deadline = entries[0].created_at + QUARANTINE_TTL

    subject.set_now(deadline)
    with pytest.raises(NotFoundError):
        await subject.repository.transition_quarantine_entry(
            TENANT,
            entries[0].entry_key,
            QuarantineEntryStatus.DISMISSED,
            expected_version=1,
            at=deadline,
        )

    subject.set_now(deadline - timedelta(microseconds=1))
    dismissed = await subject.repository.transition_quarantine_entry(
        TENANT,
        entries[0].entry_key,
        QuarantineEntryStatus.DISMISSED,
        expected_version=1,
        at=deadline,
    )
    assert (dismissed.status, dismissed.version) == (QuarantineEntryStatus.DISMISSED, 2)


@pytest.mark.asyncio
async def test_expired_entries_are_skipped_across_pages(subject: DynamoQuarantineSubject) -> None:
    expired = fenced_entries(4, now=RECEIVED - QUARANTINE_TTL)  # 5 rows that sort first
    live = _entries_at(RECEIVED + timedelta(seconds=5), 3, count=2)
    subject.set_now(RECEIVED)
    await subject.repository.put_quarantine_entries((*expired, *live))

    assert await _all(subject.repository, limit=1) == sorted(live, key=lambda item: item.entry_key)
    assert await _all(subject.repository, limit=1, reason="fenced_integration_decommissioned") == []


# Transitions ---------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_transition_writes_status_version_and_time(subject: DynamoQuarantineSubject) -> None:
    entries = fenced_entries()
    await subject.repository.put_quarantine_entries(entries)
    at = RECEIVED + timedelta(minutes=3, microseconds=7)

    replayed = await subject.repository.transition_quarantine_entry(
        TENANT,
        entries[0].entry_key,
        QuarantineEntryStatus.REPLAYED,
        expected_version=1,
        at=at,
        replayed_ingress_id=REPLAY_ID,
    )

    assert replayed == entries[0].model_copy(
        update={
            "status": QuarantineEntryStatus.REPLAYED,
            "replayed_ingress_id": REPLAY_ID,
            "updated_at": at,
            "version": 2,
        }
    )
    raw = subject.raw(entries[0])
    assert raw is not None
    assert quarantine_entry_from_item(raw) == replayed
    assert raw["expires_at"] == math.ceil(entries[0].expires_at.timestamp())  # TTL unchanged
    assert subject.client.transactions == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("first", "second"),
    [
        (QuarantineEntryStatus.DISMISSED, QuarantineEntryStatus.DISMISSED),
        (QuarantineEntryStatus.DISMISSED, QuarantineEntryStatus.REPLAYED),
        (QuarantineEntryStatus.REPLAYED, QuarantineEntryStatus.DISMISSED),
        (QuarantineEntryStatus.REPLAYED, QuarantineEntryStatus.REPLAYED),
    ],
)
async def test_terminal_statuses_reject_every_transition(
    subject: DynamoQuarantineSubject,
    first: QuarantineEntryStatus,
    second: QuarantineEntryStatus,
) -> None:
    entries = fenced_entries()
    await subject.repository.put_quarantine_entries(entries)
    key = entries[0].entry_key

    def replay_id(target: QuarantineEntryStatus) -> str | None:
        return REPLAY_ID if target == QuarantineEntryStatus.REPLAYED else None

    moved = await subject.repository.transition_quarantine_entry(
        TENANT, key, first, expected_version=1, at=RECEIVED, replayed_ingress_id=replay_id(first)
    )
    with pytest.raises(ConflictError):
        await subject.repository.transition_quarantine_entry(
            TENANT,
            key,
            second,
            expected_version=2,
            at=RECEIVED,
            replayed_ingress_id=replay_id(second),
        )
    assert await subject.repository.get_quarantine_entry(TENANT, key) == moved


@pytest.mark.asyncio
async def test_open_is_never_a_transition_target(subject: DynamoQuarantineSubject) -> None:
    entries = fenced_entries()
    await subject.repository.put_quarantine_entries(entries)

    with pytest.raises(ConflictError):
        await subject.repository.transition_quarantine_entry(
            TENANT,
            entries[0].entry_key,
            QuarantineEntryStatus.OPEN,
            expected_version=1,
            at=RECEIVED,
        )
    assert await subject.repository.get_quarantine_entry(TENANT, entries[0].entry_key) == entries[0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("target", "replayed_ingress_id"),
    [(QuarantineEntryStatus.REPLAYED, None), (QuarantineEntryStatus.DISMISSED, REPLAY_ID)],
)
async def test_replayed_ingress_id_goes_with_replayed_only(
    subject: DynamoQuarantineSubject,
    target: QuarantineEntryStatus,
    replayed_ingress_id: str | None,
) -> None:
    entries = fenced_entries()
    await subject.repository.put_quarantine_entries(entries)

    with pytest.raises(ValueError):
        await subject.repository.transition_quarantine_entry(
            TENANT,
            entries[0].entry_key,
            target,
            expected_version=1,
            at=RECEIVED,
            replayed_ingress_id=replayed_ingress_id,
        )
    assert await subject.repository.get_quarantine_entry(TENANT, entries[0].entry_key) == entries[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("stale", [0, 2, 3])
async def test_stale_version_is_a_conflict(subject: DynamoQuarantineSubject, stale: int) -> None:
    entries = fenced_entries()
    await subject.repository.put_quarantine_entries(entries)

    with pytest.raises(ConflictError):
        await subject.repository.transition_quarantine_entry(
            TENANT,
            entries[0].entry_key,
            QuarantineEntryStatus.DISMISSED,
            expected_version=stale,
            at=RECEIVED,
        )
    assert await subject.repository.get_quarantine_entry(TENANT, entries[0].entry_key) == entries[0]


@pytest.mark.asyncio
async def test_a_concurrent_transition_never_double_applies(
    subject: DynamoQuarantineSubject,
) -> None:
    """The write itself is version-conditioned: if another writer dismisses the row between the
    adapter's read and its write, the adapter's write must lose with a conflict."""
    entries = fenced_entries()
    await subject.repository.put_quarantine_entries(entries)
    key = entries[0].entry_key
    stored_key = (KEYS.quarantine_partition(TENANT), key)
    original_get, original_update = subject.client.get_item, subject.client.update_item
    state = {"raced": False, "updated": False}

    def racing_get(**kwargs: Any) -> dict[str, Any]:
        response = original_get(**kwargs)
        if not state["raced"] and not state["updated"]:
            state["raced"] = True  # another operator dismisses the row right after this read
            row = dict(subject.client.items[stored_key])
            row.update(status="dismissed", version=2)
            subject.client.items[stored_key] = row
        return response

    def tracking_update(**kwargs: Any) -> dict[str, Any]:
        state["updated"] = True
        return original_update(**kwargs)

    subject.client.get_item = racing_get  # type: ignore[method-assign]
    subject.client.update_item = tracking_update  # type: ignore[method-assign]

    try:
        await subject.repository.transition_quarantine_entry(
            TENANT,
            key,
            QuarantineEntryStatus.REPLAYED,
            expected_version=1,
            at=RECEIVED,
            replayed_ingress_id=REPLAY_ID,
        )
    except ConflictError:
        succeeded = False
    else:
        succeeded = True

    row = subject.client.items[stored_key]
    if state["raced"]:
        assert not succeeded
        assert (row["status"], row["version"]) == ("dismissed", 2)
    else:  # the adapter wrote before reading: no race happened
        assert succeeded
        assert (row["status"], row["version"]) == ("replayed", 2)


# Request shape -------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_queries_only_the_tenant_partition_without_exclusive_start_key(
    subject: DynamoQuarantineSubject,
) -> None:
    await subject.repository.put_quarantine_entries(fenced_entries(5))
    queries: list[dict[str, Any]] = []
    original = subject.client.query

    def spy(**kwargs: Any) -> dict[str, Any]:
        queries.append(kwargs)
        return original(**kwargs)

    subject.client.query = spy  # type: ignore[method-assign]
    listed = await _all(subject.repository, limit=2)

    assert len(listed) == 6
    assert queries
    for query in queries:
        assert "ExclusiveStartKey" not in query  # paging resumes from the SK lower bound
        assert query["ExpressionAttributeValues"][":pk"] == {"S": f"QUARANTINE#{TENANT}"}
    resumed = [q for q in queries if q is not queries[0]]
    assert resumed and all(
        q["KeyConditionExpression"] == "PK = :pk AND SK > :sk_lower" for q in resumed
    )
    assert {kind for kind, _, _ in subject.client.reads} <= {"query"}


@pytest.mark.asyncio
async def test_every_emitted_expression_is_a_frozen_dynamo_vector(
    subject: DynamoQuarantineSubject,
) -> None:
    """Every condition this adapter sends is one the P3-00 vectors pin on the fake and (in
    tests/ingestion) on DynamoDB Local, so fake and emulator agree on it."""
    emitted: set[str] = set()
    for operation in ("put_item", "update_item", "query"):
        original = getattr(subject.client, operation)

        def spy(*, _original: Any = original, **kwargs: Any) -> dict[str, Any]:
            for name in ("ConditionExpression", "KeyConditionExpression"):
                if name in kwargs:
                    emitted.add(kwargs[name])
            return _original(**kwargs)

        setattr(subject.client, operation, spy)
    entries = fenced_entries()
    await subject.repository.put_quarantine_entries(entries)
    await subject.repository.put_quarantine_entries(entries)
    await _all(subject.repository, limit=1)
    await subject.repository.get_quarantine_entry(TENANT, entries[0].entry_key)
    await subject.repository.transition_quarantine_entry(
        TENANT,
        entries[0].entry_key,
        QuarantineEntryStatus.DISMISSED,
        expected_version=1,
        at=RECEIVED,
    )
    with pytest.raises(ConflictError):
        await subject.repository.transition_quarantine_entry(
            TENANT,
            entries[1].entry_key,
            QuarantineEntryStatus.DISMISSED,
            expected_version=7,
            at=RECEIVED,
        )

    frozen = {
        vector.request[name]
        for vector in DYNAMO_EXPRESSION_VECTORS
        for name in ("ConditionExpression", "KeyConditionExpression")
        if name in vector.request
    }
    assert emitted and emitted <= frozen, sorted(emitted - frozen)
