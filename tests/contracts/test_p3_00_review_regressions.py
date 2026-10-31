"""Regressions for the P3-00 GREEN review findings (the approved P3-00 tests stay frozen)."""

import json
from datetime import datetime

import pytest
from boto3.dynamodb.types import TypeSerializer
from botocore.exceptions import ClientError, EndpointConnectionError
from pydantic import ValidationError

from limnopulse_api.api.redacted_validation import redacted_validation_detail
from limnopulse_api.core.errors import IngressQueueError
from limnopulse_api.domain.ids import replay_ingress_id
from limnopulse_api.domain.ingestion import (
    ComponentMark,
    IngestCredentialStatus,
    IngestMarks,
    IngestObservationBatch,
    IngressEnvelope,
    IngressFence,
    IngressFenceReason,
    QuarantineEntry,
    QuarantineEntryStatus,
    build_quarantine_entries,
    ingest_audit_item,
)
from tests.support.ingestion_contract_kits import (
    ACCOUNT,
    AUDIT,
    COMPONENT,
    DEVICE,
    OTHER_TENANT,
    RECEIVED,
    TENANT,
    component,
    credential,
    device,
    device_integration,
    envelope,
    integration_account,
    source_observation,
)
from tests.support.ingestion_dynamo_fake import IngestionDynamoClient
from tests.support.ingestion_fakes import (
    FakeIngressQueue,
    Faults,
    credential_subject,
    quarantine_subject,
    queue_subject,
    trusted_mapping_subject,
)

SENTINEL = "SENTINELr2"
_SERIALIZER = TypeSerializer()


def _encode(values: dict) -> dict:
    return {key: _SERIALIZER.serialize(value) for key, value in values.items()}


def _detail(payload: dict) -> list[dict]:
    with pytest.raises(ValidationError) as exc_info:
        IngestObservationBatch.model_validate_json(json.dumps(payload))
    return redacted_validation_detail(exc_info.value)


@pytest.mark.parametrize(
    "payload",
    [
        {"source_metadata": {SENTINEL: 5}},
        {"source_metadata": {"firmware_version": 7, SENTINEL: "x"}},
        {SENTINEL: 1},
    ],
    ids=["metadata-key-non-string", "metadata-mixed", "unknown-observation-key"],
)
def test_validation_loc_never_echoes_client_keys(payload: dict) -> None:
    observation = {
        "metric_key": "t",
        "value": 1.0,
        "source_unit": "C",
        "observed_at": "2026-10-30T11:59:00Z",
        **payload,
    }

    detail = _detail({"external_device_id": "s", "observations": [observation], SENTINEL: 2})

    assert SENTINEL not in json.dumps(detail)
    assert all(item["loc"][:1] == ["body"] for item in detail)


@pytest.mark.asyncio
async def test_fence_verifies_the_envelope_tenants_account() -> None:
    # Tenant A's real account, but the identity resolves to a DI of tenant B.
    subject = trusted_mapping_subject()
    await subject.seed_account(integration_account())
    await subject.seed_device_integration(device_integration(tenant_id=OTHER_TENANT))
    await subject.seed_device(device(tenant_id=OTHER_TENANT))
    await subject.seed_component(component(tenant_id=OTHER_TENANT))

    assert await subject.mapping.fence(envelope()) == IngressFence(
        reason=IngressFenceReason.INTEGRATION_MISMATCH, account_verified=True
    )


def test_fence_verification_flag_is_required() -> None:
    with pytest.raises(ValidationError):
        IngressFence(reason=IngressFenceReason.ACCOUNT_REVOKED)  # type: ignore[call-arg]


def test_component_and_device_marks_need_a_device_id() -> None:
    with pytest.raises(ValidationError):
        IngestMarks(
            tenant_id=TENANT,
            device_id=None,
            integration_account_id=ACCOUNT,
            component_marks=(ComponentMark(component_id=COMPONENT, received_at=RECEIVED),),
        )
    assert IngestMarks(tenant_id=TENANT, device_id=DEVICE, integration_account_id=ACCOUNT)


def test_replay_of_needs_replay() -> None:
    with pytest.raises(ValidationError):
        envelope(replay_of="ing_" + "5" * 32)


def test_credential_repr_hides_the_digest() -> None:
    value = credential()

    assert value.secret_sha256 not in repr(value)
    assert value.fingerprint in repr(value)


# Dynamo fake ------------------------------------------------------------------------------------


def test_dynamo_fake_faults_by_operation_and_partition() -> None:
    client = IngestionDynamoClient()
    client.put_item(TableName="t", Item=_encode({"PK": "DEVICE#d1", "SK": "META"}))
    client.put_item(TableName="t", Item=_encode({"PK": "COMPONENT#c1", "SK": "META"}))
    error = EndpointConnectionError(endpoint_url="http://dynamodb.invalid")
    client.fail_next("get_item", error, pk_prefix="COMPONENT#")

    assert client.get_item(TableName="t", Key=_encode({"PK": "DEVICE#d1", "SK": "META"}))["Item"]
    with pytest.raises(EndpointConnectionError) as exc_info:
        client.get_item(TableName="t", Key=_encode({"PK": "COMPONENT#c1", "SK": "META"}))
    assert exc_info.value is error
    assert client.get_item(TableName="t", Key=_encode({"PK": "COMPONENT#c1", "SK": "META"}))["Item"]


def test_dynamo_fake_faults_writes_and_transactions() -> None:
    client = IngestionDynamoClient()
    error = ClientError({"Error": {"Code": "InternalServerError", "Message": "x"}}, "x")
    client.fail_next("transact_write_items", error)

    with pytest.raises(ClientError) as exc_info:
        client.transact_write_items(
            TransactItems=[{"Put": {"TableName": "t", "Item": _encode({"PK": "A", "SK": "B"})}}]
        )

    assert exc_info.value is error
    assert client.items == {}


def test_dynamo_fake_rejects_indexes_and_filters_after_limit() -> None:
    client = IngestionDynamoClient()
    for sk, status in (("1", "open"), ("2", "dismissed"), ("3", "open")):
        client.put_item(TableName="t", Item=_encode({"PK": "Q", "SK": sk, "status": status}))
    request = {
        "TableName": "t",
        "KeyConditionExpression": "PK = :pk AND SK > :sk_lower",
        "ExpressionAttributeValues": _encode({":pk": "Q", ":sk_lower": "0", ":open": "open"}),
        "ExpressionAttributeNames": {"#status": "status"},
        "FilterExpression": "#status = :open",
        "Limit": 2,
    }

    response = client.query(**request)

    # Limit applies before the filter: two evaluated, one returned, and a cursor.
    assert [item["SK"]["S"] for item in response["Items"]] == ["1"]
    assert response["LastEvaluatedKey"]["SK"]["S"] == "2"
    with pytest.raises(AssertionError):
        client.query(**{**request, "IndexName": "GSI1"})


def test_dynamo_fake_returns_attributes_only_when_asked() -> None:
    client = IngestionDynamoClient()
    update = {
        "TableName": "t",
        "Key": _encode({"PK": "A", "SK": "B"}),
        "UpdateExpression": "SET #v = :v",
        "ExpressionAttributeNames": {"#v": "v"},
        "ExpressionAttributeValues": _encode({":v": 1}),
    }

    assert client.update_item(**update) == {}
    new = client.update_item(**{**update, "ReturnValues": "ALL_NEW"})
    assert new["Attributes"]["v"]["N"] == "1"


def test_envelope_dumps_validate_back() -> None:
    message = envelope(replay=True, replay_of="ing_" + "5" * 32)

    assert IngressEnvelope.model_validate(message.model_dump()) == message
    assert IngressEnvelope.model_validate_json(message.model_dump_json()) == message
    assert '"schema":' in message.model_dump_json()


def test_quarantine_entry_payload_matches_its_kind_and_status() -> None:
    fence = IngressFence(reason=IngressFenceReason.ACCOUNT_REVOKED, account_verified=True)
    observation_row, summary = build_quarantine_entries(envelope(), fence=fence, now=RECEIVED)

    def rebuild(row: QuarantineEntry, **changes: object) -> None:
        QuarantineEntry.model_validate({**row.model_dump(), **changes})

    with pytest.raises(ValidationError):
        rebuild(observation_row, status=QuarantineEntryStatus.REPLAYED)
    with pytest.raises(ValidationError):
        rebuild(observation_row, replayed_ingress_id="ing_" + "5" * 32)
    with pytest.raises(ValidationError):
        rebuild(observation_row, envelope_excerpt_json="{}")
    with pytest.raises(ValidationError):
        rebuild(summary, envelope_excerpt_json=None)
    rebuild(
        observation_row,
        status=QuarantineEntryStatus.REPLAYED,
        replayed_ingress_id="ing_" + "5" * 32,
    )


def test_audit_items_and_replay_ids_refuse_bad_input() -> None:
    with pytest.raises(ValueError):
        ingest_audit_item(
            action="ingest_credential.issued",
            tenant_id=TENANT,
            resource_id="ic_" + "a" * 32,
            before_status=None,
            after_status="active",
            context=AUDIT,
            now=datetime(2026, 10, 30, 12),
        )
    with pytest.raises(ValueError):
        replay_ingress_id([])


@pytest.mark.asyncio
async def test_fake_queue_refuses_stale_receipt_handles() -> None:
    subject = queue_subject()
    await subject.producer.send(envelope())
    (first,) = await subject.consumer.receive(max_messages=1, wait_seconds=0, visibility_seconds=30)
    await subject.consumer.change_visibility(first, 0)
    (second,) = await subject.consumer.receive(
        max_messages=1, wait_seconds=0, visibility_seconds=30
    )

    for call in (
        subject.consumer.change_visibility(first, 30),
        subject.consumer.delete(first),
    ):
        with pytest.raises(IngressQueueError) as exc_info:
            await call
        assert exc_info.value.retryable is False
    await subject.consumer.delete(second)
    with pytest.raises(IngressQueueError):
        await subject.consumer.change_visibility(second, 30)


@pytest.mark.asyncio
async def test_fake_quarantine_rejects_an_empty_limit() -> None:
    subject = quarantine_subject()

    with pytest.raises(ValueError):
        await subject.repository.list_quarantine_entries(TENANT, after_sk=None, limit=0)


def test_dynamo_fake_rejects_unused_placeholders_and_logs_consistency() -> None:
    client = IngestionDynamoClient()
    with pytest.raises(AssertionError):
        client.put_item(
            TableName="t",
            Item=_encode({"PK": "A", "SK": "B"}),
            ConditionExpression="attribute_not_exists(PK)",
            ExpressionAttributeNames={"#unused": "x"},
        )
    with pytest.raises(AssertionError):
        client.update_item(
            TableName="t",
            Key=_encode({"PK": "A", "SK": "B"}),
            UpdateExpression="ADD n :one",
            ExpressionAttributeValues=_encode({":one": 1}),
        )

    client.get_item(TableName="t", Key=_encode({"PK": "A", "SK": "B"}), ConsistentRead=True)
    client.get_item(TableName="t", Key=_encode({"PK": "C", "SK": "B"}))

    assert client.reads == [("get_item", "A", True), ("get_item", "C", False)]


def test_fence_rows_bound_oversized_observations() -> None:
    # A valid observation can serialize beyond 2048 bytes (StrictInt has no size bound).
    huge = source_observation(value=10**3000)
    small = source_observation()
    fence = IngressFence(reason=IngressFenceReason.ACCOUNT_REVOKED, account_verified=True)

    rows = build_quarantine_entries(envelope(observations=[huge, small]), fence=fence, now=RECEIVED)

    oversized, regular = rows[0], rows[1]
    assert len(oversized.source_observation_json.encode("utf-8")) <= 2_048
    assert (oversized.truncated, oversized.replayable) == (True, False)
    json.loads(oversized.source_observation_json)  # still valid JSON
    assert (regular.truncated, regular.replayable) == (False, True)


# Orchestrator PR audit (21a3fd9) --------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "parameters",
    [
        {"max_messages": 0, "wait_seconds": 0, "visibility_seconds": 30},
        {"max_messages": 11, "wait_seconds": 0, "visibility_seconds": 30},
        {"max_messages": 1, "wait_seconds": 21, "visibility_seconds": 30},
        {"max_messages": 1, "wait_seconds": -1, "visibility_seconds": 30},
        {"max_messages": 1, "wait_seconds": 0, "visibility_seconds": 43_201},
        {"max_messages": 1, "wait_seconds": 0, "visibility_seconds": -1},
    ],
    ids=["max-0", "max-11", "wait-21", "wait-neg", "visibility-high", "visibility-neg"],
)
async def test_fake_queue_rejects_out_of_range_receive_parameters(parameters: dict) -> None:
    subject = queue_subject()
    await subject.producer.send(envelope())

    with pytest.raises(IngressQueueError) as exc_info:
        await subject.consumer.receive(**parameters)

    assert exc_info.value.retryable is False


@pytest.mark.asyncio
@pytest.mark.parametrize("seconds", [-5, 43_201, 99_999])
async def test_fake_queue_rejects_out_of_range_visibility_changes(seconds: int) -> None:
    subject = queue_subject()
    await subject.producer.send(envelope())
    (message,) = await subject.consumer.receive(
        max_messages=1, wait_seconds=0, visibility_seconds=30
    )

    with pytest.raises(IngressQueueError):
        await subject.consumer.change_visibility(message, seconds)
    await subject.consumer.change_visibility(message, 43_200)


@pytest.mark.asyncio
async def test_fake_queue_refuses_heartbeats_after_visibility_lapsed() -> None:
    clock = [100.0]
    queue = FakeIngressQueue(Faults(), clock=lambda: clock[0])
    await queue.send(envelope())
    (message,) = await queue.receive(max_messages=1, wait_seconds=0, visibility_seconds=30)

    await queue.change_visibility(message, 30)  # in flight: extends to 130
    clock[0] = 131.0
    with pytest.raises(IngressQueueError) as exc_info:
        await queue.change_visibility(message, 30)

    assert exc_info.value.retryable is False


@pytest.mark.asyncio
async def test_fake_credentials_issue_only_active_credentials() -> None:
    subject = credential_subject()
    await subject.seed_account(integration_account())
    revoked = credential(
        status=IngestCredentialStatus.REVOKED, revoked_at=RECEIVED, updated_at=RECEIVED
    )

    with pytest.raises(ValueError):
        await subject.repository.create_ingest_credential(revoked, AUDIT)

    assert await subject.repository.get_ingest_credential(revoked.ingest_credential_id) is None
    assert subject.audit_items() == []


@pytest.mark.asyncio
async def test_fake_revoke_frees_only_its_own_slot() -> None:
    subject = credential_subject()
    await subject.seed_account(integration_account())
    # A seeded credential claims slot 1 without owning the slot row (the row is someone else's).
    stray = credential()
    await subject.seed_credential(stray)
    other = "ic_" + "c" * 32
    subject.store.slots[(TENANT, ACCOUNT, 1)] = other

    await subject.repository.revoke_ingest_credential(
        TENANT,
        ACCOUNT,
        stray.ingest_credential_id,
        expected_version=1,
        revoked_at=RECEIVED,
        audit=AUDIT,
    )

    assert subject.store.slots[(TENANT, ACCOUNT, 1)] == other


def test_fake_faults_reject_unknown_operations() -> None:
    subject = credential_subject()

    with pytest.raises(ValueError):
        subject.fail_next("get_credential_typo", RuntimeError("x"))
    subject.fail_next("get_ingest_credential", RuntimeError("x"))
    subject.fail_next("get_component", RuntimeError("x"))


def test_dynamo_transaction_fault_matches_any_item() -> None:
    client = IngestionDynamoClient()
    error = EndpointConnectionError(endpoint_url="http://dynamodb.invalid")
    client.fail_next("transact_write_items", error, pk_prefix="AUDIT#")

    with pytest.raises(EndpointConnectionError) as exc_info:
        client.transact_write_items(
            TransactItems=[
                {"Put": {"TableName": "t", "Item": _encode({"PK": "TENANT#t", "SK": "A"})}},
                {"Put": {"TableName": "t", "Item": _encode({"PK": "AUDIT#t", "SK": "B"})}},
            ]
        )

    assert exc_info.value is error
    assert client.items == {}
