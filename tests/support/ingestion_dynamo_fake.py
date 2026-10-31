"""P3-00 single-table DynamoDB fake for the ingestion adapters (test-only).

`IngestionDynamoClient` extends `tests.contracts.m1_fakes.ConditionalDynamoClient` and keeps its
condition and update evaluator (`m1_fakes._Condition` and `_value`). It adds only what the P3
adapters need: `SK > :sk_lower` key conditions, `REMOVE` update clauses, and
`ReturnValuesOnConditionCheckFailure=ALL_OLD` on single-item writes and transaction items.
Operations that are not listed (Scan, BatchWriteItem, BatchGetItem, ...) still raise.
"""

from collections.abc import Callable
from typing import Any

from botocore.exceptions import ClientError

from tests.contracts.m1_fakes import ConditionalDynamoClient, _split_top_level

_ALL_OLD = "ALL_OLD"


class IngestionDynamoClient(ConditionalDynamoClient):
    @staticmethod
    def _key_condition(condition: str, values: dict[str, Any]) -> Callable[[str], bool]:
        if condition == "PK = :pk AND SK > :sk_lower":
            return lambda sk: sk > values[":sk_lower"]
        return ConditionalDynamoClient._key_condition(condition, values)

    def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
        actions = kwargs["TransactItems"]
        assert 1 <= len(actions) <= 100
        with self.lock:
            self.write_calls += 1
            self.transactions.append(actions)
            keys = [self._action_key(action) for action in actions]
            assert len(set(keys)) == len(keys), "transaction keys must be distinct"
            reasons = [
                self._reason(action, key) for action, key in zip(actions, keys, strict=True)
            ]
            if any(reason["Code"] != "None" for reason in reasons):
                raise ClientError(
                    {
                        "Error": {"Code": "TransactionCanceledException", "Message": "cancelled"},
                        "CancellationReasons": reasons,
                    },
                    "TransactWriteItems",
                )
            for action, key in zip(actions, keys, strict=True):
                self._commit(action, key)
        return {}

    # Internals ------------------------------------------------------------------------------

    def _reason(self, action: dict[str, Any], key: tuple[str, str]) -> dict[str, Any]:
        if self._holds(action, key):
            return {"Code": "None"}
        reason: dict[str, Any] = {
            "Code": "ConditionalCheckFailed",
            "Message": "The conditional request failed",
        }
        old = self._old_item(action, key)
        if old is not None:
            reason["Item"] = old
        return reason

    def _old_item(self, action: dict[str, Any], key: tuple[str, str]) -> dict[str, Any] | None:
        ((_, request),) = action.items()
        if request.get("ReturnValuesOnConditionCheckFailure") != _ALL_OLD:
            return None
        item = self.items.get(key)
        return None if item is None else self._encode(item)

    def _single(self, kind: str, request: dict[str, Any], operation: str) -> tuple[str, str]:
        action = {kind: request}
        with self.lock:
            self.write_calls += 1
            key = self._action_key(action)
            if not self._holds(action, key):
                response: dict[str, Any] = {
                    "Error": {"Code": "ConditionalCheckFailedException", "Message": "failed"}
                }
                old = self._old_item(action, key)
                if old is not None:
                    response["Item"] = old
                raise ClientError(response, operation)
            self._commit(action, key)
        return key

    def _commit(self, action: dict[str, Any], key: tuple[str, str]) -> None:
        ((kind, request),) = action.items()
        if kind != "Update":
            super()._commit(action, key)
            return
        item = dict(self.items.get(key) or {"PK": key[0], "SK": key[1]})
        names = request.get("ExpressionAttributeNames", {})
        values = self._plain(request.get("ExpressionAttributeValues", {}))
        for clause, body in _clauses(request["UpdateExpression"]):
            if clause == "SET":
                for assignment in _split_top_level(body, ","):
                    target, value = (part.strip() for part in assignment.split("=", 1))
                    item[names.get(target, target)] = self._value(value, item, names, values)
            else:
                for target in _split_top_level(body, ","):
                    item.pop(names.get(target.strip(), target.strip()), None)
        self.items[key] = item


def _clauses(expression: str) -> list[tuple[str, str]]:
    """Split `SET a = :x, b = :y REMOVE c, d` into `[("SET", ...), ("REMOVE", ...)]`."""
    clauses: list[tuple[str, str]] = []
    tokens = expression.split()
    current: str | None = None
    body: list[str] = []
    for token in tokens:
        if token in {"SET", "REMOVE"}:
            if current is not None:
                clauses.append((current, " ".join(body)))
            current, body = token, []
        else:
            assert current is not None, f"unsupported update expression: {expression}"
            body.append(token)
    assert current is not None, f"unsupported update expression: {expression}"
    clauses.append((current, " ".join(body)))
    return clauses
