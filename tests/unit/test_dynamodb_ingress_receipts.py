"""P3-07: `DynamoIngressReceiptRepository` on the P3-00 in-memory Dynamo fake (D5).

The P3-00 `INGRESS_RECEIPT_CONTRACT` kit runs against the real adapter, fault scenarios
included: an injected boto3 error propagates as the very same object (no internal retry, no
re-wrap, A44). The stored row uses the frozen key `INGRESS_RECEIPT#<ia>/<key_sha256>`, every read
is strongly consistent, the claim uses the single liveness predicate on `created_at_us`, and
`mark_enqueued` is a conditional update that never touches another request's receipt.
"""

from collections.abc import Callable
from datetime import timedelta
from typing import Any

import pytest
from botocore.exceptions import ClientError

from limnopulse_api.adapters.dynamodb_ingress_receipts import DynamoIngressReceiptRepository
from limnopulse_api.adapters.dynamodb_v2 import (
    DynamoV2KeyBuilder,
    ingress_receipt_from_item,
    ingress_receipt_to_item,
)
from limnopulse_api.domain.ingestion import (
    IngressReceiptState,
    ingress_receipt_cutoff_us,
)
from limnopulse_api.repositories.ingestion import IngressReceiptRepository
from tests.support.ingestion_contract_kits import (
    ACCOUNT,
    INGRESS,
    INGRESS_RECEIPT_CONTRACT,
    KEY_SHA,
    RECEIVED,
    kit_ids,
    receipt,
)
from tests.support.ingestion_dynamo_fake import IngestionDynamoClient

TABLE = "LimnopulseDomain"
KEYS = DynamoV2KeyBuilder()
CLAIM_CONDITION = "attribute_not_exists(PK) OR #created_at_us <= :cutoff_us"

# Port operation -> the boto3 operation the adapter performs for it (the fault point).
_BOTO_OPERATION = {
    "get_ingress_receipt": "get_item",
    "claim_ingress_receipt": "put_item",
    "mark_ingress_receipt_enqueued": "update_item",
}


class RecordingClient:
    """Wraps the Dynamo fake and records every boto3 request."""

    def __init__(self, inner: IngestionDynamoClient) -> None:
        self.inner = inner
        self.requests: list[tuple[str, dict[str, Any]]] = []

    def __getattr__(self, name: str) -> Any:
        target = getattr(self.inner, name)
        if not callable(target):
            return target

        def call(*args: Any, **kwargs: Any) -> Any:
            self.requests.append((name, kwargs))
            return target(*args, **kwargs)

        return call


class DynamoReceiptSubject:
    """`ReceiptKitSubject` over the real adapter and the P3-00 Dynamo fake."""

    def __init__(self) -> None:
        self.store = IngestionDynamoClient()
        self.client = RecordingClient(self.store)
        self.repository = DynamoIngressReceiptRepository(TABLE, self.client)

    def fail_next(self, operation: str, exc: BaseException) -> None:
        self.store.fail_next(_BOTO_OPERATION[operation], exc)

    def raw(self, integration_account_id: str = ACCOUNT, key_sha256: str = KEY_SHA) -> Any:
        key = KEYS.ingress_receipt(integration_account_id, key_sha256)
        return self.store.items.get((key["PK"], key["SK"]))

    def calls(self, operation: str) -> list[dict[str, Any]]:
        return [request for name, request in self.client.requests if name == operation]


@pytest.fixture
def subject() -> DynamoReceiptSubject:
    return DynamoReceiptSubject()


def test_adapter_implements_the_receipt_port(subject: DynamoReceiptSubject) -> None:
    assert isinstance(subject.repository, IngressReceiptRepository)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario", INGRESS_RECEIPT_CONTRACT, ids=kit_ids(INGRESS_RECEIPT_CONTRACT)
)
async def test_receipts_pass_the_p3_00_kit(scenario: Callable[[Any], Any]) -> None:
    await scenario(DynamoReceiptSubject())


@pytest.mark.asyncio
async def test_claim_writes_the_frozen_row(subject: DynamoReceiptSubject) -> None:
    first = receipt()

    await subject.repository.claim_ingress_receipt(first, now=RECEIVED)

    stored = subject.raw()
    assert stored is not None
    assert stored["PK"] == f"INGRESS_RECEIPT#{ACCOUNT}"
    assert stored["SK"] == KEY_SHA
    assert stored["entity_type"] == "ingress_receipt"
    assert stored == ingress_receipt_to_item(first)
    assert ingress_receipt_from_item(stored) == first


@pytest.mark.asyncio
async def test_claim_uses_the_single_liveness_predicate(subject: DynamoReceiptSubject) -> None:
    now = RECEIVED + timedelta(minutes=5)

    await subject.repository.claim_ingress_receipt(receipt(created_at=now), now=now)

    (put,) = subject.calls("put_item")
    assert put["ConditionExpression"] == CLAIM_CONDITION
    assert put["ExpressionAttributeNames"] == {"#created_at_us": "created_at_us"}
    assert put["ExpressionAttributeValues"] == {
        ":cutoff_us": {"N": str(ingress_receipt_cutoff_us(now))}
    }


@pytest.mark.asyncio
async def test_reads_are_strongly_consistent(subject: DynamoReceiptSubject) -> None:
    first = receipt()
    await subject.repository.claim_ingress_receipt(first, now=RECEIVED)
    loser = receipt(ingress_id="ing_" + "5" * 32, created_at=RECEIVED + timedelta(seconds=1))

    assert await subject.repository.get_ingress_receipt(ACCOUNT, KEY_SHA) == first
    assert await subject.repository.claim_ingress_receipt(loser, now=loser.created_at) == first

    assert subject.store.reads, "the adapter must read through get_item"
    assert all(consistent for _, _, consistent in subject.store.reads)


@pytest.mark.asyncio
async def test_a_lost_claim_never_overwrites_the_winner(subject: DynamoReceiptSubject) -> None:
    first = receipt()
    await subject.repository.claim_ingress_receipt(first, now=RECEIVED)
    stored = dict(subject.raw())
    loser = receipt(ingress_id="ing_" + "5" * 32, created_at=RECEIVED + timedelta(seconds=1))

    await subject.repository.claim_ingress_receipt(loser, now=loser.created_at)

    assert subject.raw() == stored


@pytest.mark.asyncio
async def test_mark_enqueued_is_a_conditional_update(subject: DynamoReceiptSubject) -> None:
    first = receipt()
    await subject.repository.claim_ingress_receipt(first, now=RECEIVED)

    await subject.repository.mark_ingress_receipt_enqueued(ACCOUNT, KEY_SHA, INGRESS, at=RECEIVED)

    (update,) = subject.calls("update_item")
    assert "ConditionExpression" in update
    values = update.get("ExpressionAttributeValues", {})
    assert {"S": INGRESS} in values.values()
    assert {"S": IngressReceiptState.PENDING.value} in values.values()
    assert len(subject.calls("put_item")) == 1  # the mark is never a blind put
    stored = subject.raw()
    assert stored["state"] == IngressReceiptState.ENQUEUED.value
    assert {key: value for key, value in stored.items() if key != "state"} == {
        key: value for key, value in ingress_receipt_to_item(first).items() if key != "state"
    }


@pytest.mark.asyncio
async def test_mark_enqueued_never_creates_a_receipt(subject: DynamoReceiptSubject) -> None:
    await subject.repository.mark_ingress_receipt_enqueued(ACCOUNT, KEY_SHA, INGRESS, at=RECEIVED)

    assert subject.raw() is None


@pytest.mark.asyncio
async def test_mark_enqueued_for_another_ingress_id_is_a_no_op(
    subject: DynamoReceiptSubject,
) -> None:
    first = receipt()
    await subject.repository.claim_ingress_receipt(first, now=RECEIVED)
    before = dict(subject.raw())

    await subject.repository.mark_ingress_receipt_enqueued(
        ACCOUNT, KEY_SHA, "ing_" + "9" * 32, at=RECEIVED
    )

    assert subject.raw() == before


@pytest.mark.asyncio
async def test_a_non_conditional_claim_error_propagates(subject: DynamoReceiptSubject) -> None:
    error = ClientError({"Error": {"Code": "ValidationException", "Message": "bad"}}, "PutItem")
    subject.fail_next("claim_ingress_receipt", error)

    with pytest.raises(ClientError) as exc_info:
        await subject.repository.claim_ingress_receipt(receipt(), now=RECEIVED)

    assert exc_info.value is error
    assert subject.raw() is None
