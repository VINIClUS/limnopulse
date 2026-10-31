"""P3-00 single-table DynamoDB fake for the ingestion adapters (test-only).

`IngestionDynamoClient` extends `tests.contracts.m1_fakes.ConditionalDynamoClient` and keeps its
condition and update evaluator (`m1_fakes._Condition` and `_value`). It adds only what the P3
adapters need: `SK > :sk_lower` key conditions, `REMOVE` update clauses, and
`ReturnValuesOnConditionCheckFailure=ALL_OLD` on single-item writes and transaction items,
`FilterExpression` on Query (applied after `Limit`, as DynamoDB does), `ReturnValues` on
UpdateItem, and `fail_next(operation, exc, pk_prefix=...)` so adapter kit subjects can fault a
store read below their port boundary. Operations that are not listed (Scan, BatchWriteItem,
BatchGetItem, ...) still raise, and so do unsupported request parameters such as `IndexName`.
"""

import re
from collections import defaultdict
from collections.abc import Callable
from typing import Any

from botocore.exceptions import ClientError

from tests.contracts import m1_fakes
from tests.contracts.m1_fakes import ConditionalDynamoClient, _split_top_level

_ALL_OLD = "ALL_OLD"
_RETURN_VALUES = frozenset({"NONE", "ALL_NEW", "ALL_OLD"})
_UNSUPPORTED = frozenset(
    {"IndexName", "ProjectionExpression", "Select", "AttributesToGet", "QueryFilter"}
)
_EXPRESSIONS = (
    "ConditionExpression",
    "UpdateExpression",
    "KeyConditionExpression",
    "FilterExpression",
)


def _assert_placeholders_used(request: dict[str, Any]) -> None:
    """DynamoDB rejects unused `ExpressionAttributeNames`/`Values`; so does the fake."""
    text = " ".join(request.get(name, "") for name in _EXPRESSIONS)
    placeholders = [*request.get("ExpressionAttributeNames", {})]
    placeholders += [*request.get("ExpressionAttributeValues", {})]
    for placeholder in placeholders:
        pattern = rf"(?<![\w#:]){re.escape(placeholder)}(?![\w])"
        assert re.search(pattern, text), f"unused expression placeholder: {placeholder}"


class IngestionDynamoClient(ConditionalDynamoClient):
    def __init__(self) -> None:
        super().__init__()
        self._faults: dict[str, list[tuple[str | None, BaseException]]] = defaultdict(list)
        # (operation, partition key, ConsistentRead) per read, so kits can assert consistency.
        self.reads: list[tuple[str, str | None, bool]] = []

    def fail_next(
        self, operation: str, exc: BaseException, *, pk_prefix: str | None = None
    ) -> None:
        """Raise `exc` once from the next boto3 `operation` (e.g. "get_item", "query",
        "transact_write_items") whose partition key starts with `pk_prefix`, if given."""
        self._faults[operation].append((pk_prefix, exc))

    def _check_fault(self, operation: str, *pks: str | None) -> None:
        pending = self._faults.get(operation)
        if not pending:
            return
        for index, (prefix, exc) in enumerate(pending):
            if prefix is None or any(pk is not None and pk.startswith(prefix) for pk in pks):
                del pending[index]
                raise exc

    def _pk(self, raw: dict[str, Any] | None) -> str | None:
        if raw is None or "PK" not in raw:
            return None
        return self._plain({"PK": raw["PK"]})["PK"]

    # boto3 surface --------------------------------------------------------------------------

    def get_item(self, **kwargs: Any) -> dict[str, Any]:
        assert "ProjectionExpression" not in kwargs, "unsupported GetItem projection"
        pk = self._pk(kwargs.get("Key"))
        self._check_fault("get_item", pk)
        self.reads.append(("get_item", pk, bool(kwargs.get("ConsistentRead", False))))
        return super().get_item(**kwargs)

    def query(self, **kwargs: Any) -> dict[str, Any]:
        unsupported = _UNSUPPORTED & set(kwargs)
        assert not unsupported, f"unsupported Query parameters: {sorted(unsupported)}"
        _assert_placeholders_used(kwargs)
        values = self._plain(kwargs["ExpressionAttributeValues"])
        self._check_fault("query", values.get(":pk"))
        self.reads.append(("query", values.get(":pk"), bool(kwargs.get("ConsistentRead", False))))
        filter_expression = kwargs.pop("FilterExpression", None)
        names = kwargs.pop("ExpressionAttributeNames", {})
        response = super().query(**kwargs)
        if filter_expression is not None:
            response["Items"] = [
                item
                for item in response["Items"]
                if m1_fakes._Condition(
                    filter_expression, names, values, self._plain(item)
                ).evaluate()
            ]
        return response

    def put_item(self, **kwargs: Any) -> dict[str, Any]:
        _assert_placeholders_used(kwargs)
        self._check_fault("put_item", self._pk(kwargs.get("Item")))
        return super().put_item(**kwargs)

    def update_item(self, **kwargs: Any) -> dict[str, Any]:
        _assert_placeholders_used(kwargs)
        self._check_fault("update_item", self._pk(kwargs.get("Key")))
        return_values = kwargs.get("ReturnValues", "NONE")
        assert return_values in _RETURN_VALUES, f"unsupported ReturnValues: {return_values}"
        old = self.items.get(self._action_key({"Update": kwargs}))
        key = self._single("Update", kwargs, "UpdateItem")
        if return_values == "ALL_NEW":
            return {"Attributes": self._encode(self.items[key])}
        if return_values == "ALL_OLD" and old is not None:
            return {"Attributes": self._encode(old)}
        return {}

    def delete_item(self, **kwargs: Any) -> dict[str, Any]:
        _assert_placeholders_used(kwargs)
        self._check_fault("delete_item", self._pk(kwargs.get("Key")))
        return super().delete_item(**kwargs)

    @staticmethod
    def _key_condition(condition: str, values: dict[str, Any]) -> Callable[[str], bool]:
        if condition == "PK = :pk AND SK > :sk_lower":
            return lambda sk: sk > values[":sk_lower"]
        return ConditionalDynamoClient._key_condition(condition, values)

    def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
        actions = kwargs["TransactItems"]
        assert 1 <= len(actions) <= 100
        for action in actions:
            for body in action.values():
                _assert_placeholders_used(body)
        # A partition-scoped fault fires when ANY item of the transaction is in that partition.
        self._check_fault(
            "transact_write_items",
            *(
                self._pk(body.get("Item") or body.get("Key"))
                for action in actions
                for body in action.values()
            ),
        )
        with self.lock:
            self.write_calls += 1
            self.transactions.append(actions)
            keys = [self._action_key(action) for action in actions]
            assert len(set(keys)) == len(keys), "transaction keys must be distinct"
            reasons = [self._reason(action, key) for action, key in zip(actions, keys, strict=True)]
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
            assert current is not None and token not in {"ADD", "DELETE"}, (
                f"unsupported update expression: {expression}"
            )
            body.append(token)
    assert current is not None, f"unsupported update expression: {expression}"
    clauses.append((current, " ".join(body)))
    return clauses
