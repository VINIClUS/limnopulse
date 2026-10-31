"""DynamoDB persistence for per-request ingress receipts (P3 D5).

Key: `INGRESS_RECEIPT#<integration_account_id>` / `<key_sha256>`. Liveness is decided only on
`created_at_us` (a receipt is live iff `created_at_us > now_us - 24 h`); `expires_at` drives TTL
cleanup only.

- `claim` is one conditional `PutItem` with the same predicate the service applies at route
  step 8: `attribute_not_exists(PK) OR created_at_us <= :cutoff_us`. When it loses, the winner
  comes from `ALL_OLD`, or from one strongly consistent read if that is unavailable.
- `mark_enqueued` is a conditional `UpdateItem` on `ingress_id = :id AND state = :pending`; a
  condition failure is a no-op, so it never touches another request's receipt.

Errors other than a lost condition propagate unchanged: nothing is retried or re-wrapped here.
"""

from collections.abc import Mapping
from datetime import datetime

from limnopulse_api.adapters.dynamodb_v2 import (
    DynamoConditionalConflict,
    DynamoV2RepositoryBase,
    ingress_receipt_from_item,
    ingress_receipt_to_item,
)
from limnopulse_api.domain.ingestion import (
    IngressReceipt,
    IngressReceiptState,
    ingress_receipt_cutoff_us,
)

_CLAIM_CONDITION = "attribute_not_exists(PK) OR #created_at_us <= :cutoff_us"
_MARK_CONDITION = "#ingress_id = :ingress_id AND #state = :pending"


class DynamoIngressReceiptRepository(DynamoV2RepositoryBase):
    async def get_ingress_receipt(
        self, integration_account_id: str, key_sha256: str
    ) -> IngressReceipt | None:
        item = await self.get_item(
            self.keys.ingress_receipt(integration_account_id, key_sha256), consistent=True
        )
        return None if item is None else ingress_receipt_from_item(item)

    async def claim_ingress_receipt(
        self, receipt: IngressReceipt, *, now: datetime
    ) -> IngressReceipt:
        request = self.put_request(
            ingress_receipt_to_item(receipt),
            condition=_CLAIM_CONDITION,
            names={"#created_at_us": "created_at_us"},
            values={":cutoff_us": ingress_receipt_cutoff_us(now)},
        )
        request["ReturnValuesOnConditionCheckFailure"] = "ALL_OLD"
        try:
            await self.put_item(request)
        except DynamoConditionalConflict as conflict:
            winner = self._old_item(conflict)
            if winner is None:
                winner = await self.get_ingress_receipt(
                    receipt.integration_account_id, receipt.key_sha256
                )
            if winner is None:
                raise
            return winner
        return receipt

    async def mark_ingress_receipt_enqueued(
        self, integration_account_id: str, key_sha256: str, ingress_id: str, *, at: datetime
    ) -> None:
        request = self.update_request(
            self.keys.ingress_receipt(integration_account_id, key_sha256),
            update_expression="SET #state = :enqueued",
            condition=_MARK_CONDITION,
            names={"#ingress_id": "ingress_id", "#state": "state"},
            values={
                ":ingress_id": ingress_id,
                ":pending": IngressReceiptState.PENDING.value,
                ":enqueued": IngressReceiptState.ENQUEUED.value,
            },
        )
        try:
            await self.update_item(request)
        except DynamoConditionalConflict:
            return  # another request's receipt, already enqueued, or absent: a no-op

    def _old_item(self, conflict: DynamoConditionalConflict) -> IngressReceipt | None:
        response = getattr(conflict.__cause__, "response", None)
        item = response.get("Item") if isinstance(response, Mapping) else None
        if not item:
            return None
        return ingress_receipt_from_item(self.deserialize_item(item))
