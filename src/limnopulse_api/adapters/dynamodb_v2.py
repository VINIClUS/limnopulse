"""Shared DynamoDB vocabulary for the `/v2` bounded-context adapters.

Only generic primitives live here: key builders, hashing helpers, idempotency receipt codecs
and a repository base with serialized reads, paginated queries, conditional write builders and
the conditional-failure -> `ConflictError` mapping. Entity codecs belong to each adapter.
"""

import json
import re
from asyncio import to_thread
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from typing import Any

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError

from limnopulse_api.core.errors import ConflictError
from limnopulse_api.domain.v2_common import (
    IDEMPOTENCY_KEY_MAX_LENGTH,
    IDEMPOTENCY_KEY_MIN_LENGTH,
    IDEMPOTENCY_RECEIPT_TTL,
    PROVIDER_KEY_PATTERN,
    IdempotencyReceipt,
    format_utc_timestamp,
)

MAX_TRANSACTION_ACTIONS = 100
RECEIPT_ENTITY_TYPE = "idempotency_receipt"


def utc_now() -> datetime:
    return datetime.now(UTC)


def canonical_timestamp_key(value: datetime) -> str:
    """UTC `YYYY-MM-DDTHH:MM:SS.ffffffZ`; lexicographic order is chronological order."""
    return format_utc_timestamp(value)


def parse_canonical_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed.astimezone(UTC)


def external_identity_hash(provider_key: str, external_identity: str) -> str:
    if re.fullmatch(PROVIDER_KEY_PATTERN, provider_key) is None:
        raise ValueError("invalid provider key")
    if not external_identity:
        raise ValueError("external identity must not be empty")
    return sha256(f"{provider_key}\0{external_identity}".encode()).hexdigest()


def idempotency_key_hash(idempotency_key: str) -> str:
    if not IDEMPOTENCY_KEY_MIN_LENGTH <= len(idempotency_key) <= IDEMPOTENCY_KEY_MAX_LENGTH:
        raise ValueError("Idempotency-Key must have 8 to 128 characters")
    return sha256(idempotency_key.encode()).hexdigest()


def canonical_request_hash(payload: Any) -> str:
    """Hash only the client request (scope, path params, body); never server-generated values."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return sha256(canonical.encode()).hexdigest()


def _part(value: str) -> str:
    if not isinstance(value, str) or not value or "#" in value:
        raise ValueError("key part must be a non-empty string without '#'")
    return value


class DynamoV2KeyBuilder:
    SITE_PREFIX = "SITE#"
    ASSET_PREFIX = "ASSET#"
    DEVICE_PREFIX = "DEVICE#"
    COMPONENT_PREFIX = "COMPONENT#"
    CAPABILITY_PREFIX = "CAPABILITY#"
    INTEGRATION_ACCOUNT_PREFIX = "INTEGRATION_ACCOUNT#"
    DEVICE_INTEGRATION_PREFIX = "DEVICE_INTEGRATION#"
    DEPLOYMENT_PREFIX = "DEPLOYMENT#"
    LEGACY_POND_PREFIX = "POND#"

    def tenant_partition(self, tenant_id: str) -> str:
        return f"TENANT#{_part(tenant_id)}"

    def site(self, tenant_id: str, site_id: str) -> dict[str, str]:
        return {"PK": self.tenant_partition(tenant_id), "SK": f"SITE#{_part(site_id)}"}

    def asset(self, tenant_id: str, asset_id: str) -> dict[str, str]:
        return {"PK": self.tenant_partition(tenant_id), "SK": f"ASSET#{_part(asset_id)}"}

    def pond_profile(self, asset_id: str) -> dict[str, str]:
        return {"PK": f"ASSET#{_part(asset_id)}", "SK": "PROFILE#POND"}

    def device(self, tenant_id: str, device_id: str) -> dict[str, str]:
        return {"PK": self.tenant_partition(tenant_id), "SK": f"DEVICE#{_part(device_id)}"}

    def device_lookup(self, device_id: str) -> dict[str, str]:
        return {"PK": f"DEVICE#{_part(device_id)}", "SK": "META"}

    def component_mirror(self, tenant_id: str, component_id: str) -> dict[str, str]:
        return {"PK": self.tenant_partition(tenant_id), "SK": f"COMPONENT#{_part(component_id)}"}

    def component(self, component_id: str) -> dict[str, str]:
        return {"PK": f"COMPONENT#{_part(component_id)}", "SK": "META"}

    def capability(self, component_id: str, capability_id: str) -> dict[str, str]:
        return {
            "PK": f"COMPONENT#{_part(component_id)}",
            "SK": f"CAPABILITY#{_part(capability_id)}",
        }

    def integration_account(self, tenant_id: str, integration_account_id: str) -> dict[str, str]:
        return {
            "PK": self.tenant_partition(tenant_id),
            "SK": f"INTEGRATION_ACCOUNT#{_part(integration_account_id)}",
        }

    def device_integration(self, tenant_id: str, device_integration_id: str) -> dict[str, str]:
        return {
            "PK": self.tenant_partition(tenant_id),
            "SK": f"DEVICE_INTEGRATION#{_part(device_integration_id)}",
        }

    def external_identity(self, provider_key: str, identity_hash: str) -> dict[str, str]:
        return {"PK": f"EXTERNAL_ID#{_part(provider_key)}#{_part(identity_hash)}", "SK": "META"}

    def deployment_mirror(self, tenant_id: str, deployment_id: str) -> dict[str, str]:
        return {
            "PK": self.tenant_partition(tenant_id),
            "SK": f"DEPLOYMENT#{_part(deployment_id)}",
        }

    def deployment_history(
        self, component_id: str, started_at: datetime, deployment_id: str
    ) -> dict[str, str]:
        return {
            "PK": f"COMPONENT#{_part(component_id)}",
            "SK": f"DEPLOYMENT#{canonical_timestamp_key(started_at)}#{_part(deployment_id)}",
        }

    def current_deployment(self, component_id: str) -> dict[str, str]:
        return {"PK": f"COMPONENT#{_part(component_id)}", "SK": "DEPLOYMENT#CURRENT"}

    def deployment_history_bounds(self) -> tuple[str, str]:
        """Inclusive SK bounds of every history row; `DEPLOYMENT#CURRENT` sorts after them."""
        return "DEPLOYMENT#0", "DEPLOYMENT#:"

    def deployment_history_upper_bound(self, observed_at: datetime) -> str:
        """Inclusive SK bound covering every history row that started at or before the instant."""
        return f"DEPLOYMENT#{canonical_timestamp_key(observed_at)}#~"

    def idempotency_receipt(self, tenant_id: str, scope: str, key_hash: str) -> dict[str, str]:
        return {
            "PK": self.tenant_partition(tenant_id),
            "SK": f"IDEMPOTENCY#{_part(scope)}#{_part(key_hash)}",
        }

    def migration_receipt(self, tenant_id: str) -> dict[str, str]:
        return {
            "PK": self.tenant_partition(tenant_id),
            "SK": "MIGRATION#M1#DEFAULT_SITE_ASSET_DEVICE",
        }

    def legacy_pond(self, tenant_id: str, pond_id: str) -> dict[str, str]:
        return {"PK": self.tenant_partition(tenant_id), "SK": f"POND#{_part(pond_id)}"}

    def legacy_device(self, tenant_id: str, device_id: str) -> dict[str, str]:
        return {"PK": self.tenant_partition(tenant_id), "SK": f"DEVICE#{_part(device_id)}"}


_KEYS = DynamoV2KeyBuilder()


class DynamoConditionalConflict(ConflictError):
    """A conditional write failed; `failed_indexes` names the failed transaction actions."""

    def __init__(self, failed_indexes: tuple[int, ...]) -> None:
        super().__init__("conditional write conflict")
        self.failed_indexes = failed_indexes


def conditional_failure_indexes(exc: BaseException) -> tuple[int, ...]:
    """Indexes of conditional failures; `(0,)` for a single-item write, `()` for anything else.

    A cancelled transaction counts as a conflict whenever ANY reason is `ConditionalCheckFailed`,
    even when other reasons (e.g. `TransactionConflict`) are present.
    """
    if isinstance(exc, DynamoConditionalConflict):
        return exc.failed_indexes
    response = getattr(exc, "response", None)
    if not isinstance(response, Mapping):
        return ()
    error = response.get("Error")
    code = error.get("Code") if isinstance(error, Mapping) else None
    if code == "ConditionalCheckFailedException":
        return (0,)
    if code != "TransactionCanceledException":
        return ()
    reasons = response.get("CancellationReasons")
    if not isinstance(reasons, list):
        return ()
    return tuple(
        index
        for index, reason in enumerate(reasons)
        if isinstance(reason, Mapping) and reason.get("Code") == "ConditionalCheckFailed"
    )


def idempotency_receipt_to_item(receipt: IdempotencyReceipt) -> dict[str, Any]:
    return {
        **_KEYS.idempotency_receipt(receipt.tenant_id, receipt.scope, receipt.key_hash),
        "entity_type": RECEIPT_ENTITY_TYPE,
        "tenant_id": receipt.tenant_id,
        "scope": receipt.scope,
        "key_hash": receipt.key_hash,
        "request_hash": receipt.request_hash,
        "response_snapshot_json": receipt.response_snapshot_json,
        "created_at": canonical_timestamp_key(receipt.created_at),
        # Numeric TTL attribute; only receipts may carry it.
        "expires_at": int(receipt.expires_at.timestamp()),
    }


def idempotency_receipt_from_item(item: Mapping[str, Any]) -> IdempotencyReceipt:
    created_at = parse_canonical_timestamp(item["created_at"])
    return IdempotencyReceipt(
        tenant_id=item["tenant_id"],
        scope=item["scope"],
        key_hash=item["key_hash"],
        request_hash=item["request_hash"],
        response_snapshot_json=item["response_snapshot_json"],
        created_at=created_at,
        expires_at=created_at + IDEMPOTENCY_RECEIPT_TTL,
    )


class DynamoV2RepositoryBase:
    def __init__(
        self,
        table_name: str,
        client: Any,
        *,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.table_name = table_name
        self.client = client
        self.clock = clock
        self.keys = DynamoV2KeyBuilder()
        self._serializer = TypeSerializer()
        self._deserializer = TypeDeserializer()

    # Codec ---------------------------------------------------------------------------------

    def serialize_item(self, item: Mapping[str, Any]) -> dict[str, Any]:
        return {
            key: self._serializer.serialize(self._normalize_to_dynamo(value))
            for key, value in item.items()
        }

    def serialize_values(self, values: Mapping[str, Any]) -> dict[str, Any]:
        return self.serialize_item(values)

    def deserialize_item(self, item: Mapping[str, Any]) -> dict[str, Any]:
        return {
            key: self._normalize_from_dynamo(self._deserializer.deserialize(value))
            for key, value in item.items()
        }

    # Request builders ----------------------------------------------------------------------

    def put_request(
        self,
        item: Mapping[str, Any],
        *,
        condition: str | None = None,
        names: Mapping[str, str] | None = None,
        values: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        request: dict[str, Any] = {
            "TableName": self.table_name,
            "Item": self.serialize_item(item),
        }
        return self._with_condition(request, condition, names, values)

    def put_new_request(self, item: Mapping[str, Any]) -> dict[str, Any]:
        return self.put_request(item, condition="attribute_not_exists(PK)")

    def put_versioned_request(
        self, item: Mapping[str, Any], expected_version: int
    ) -> dict[str, Any]:
        return self.put_request(
            item,
            condition="attribute_exists(PK) AND #version = :expected_version",
            names={"#version": "version"},
            values={":expected_version": expected_version},
        )

    def update_request(
        self,
        key: Mapping[str, str],
        *,
        update_expression: str,
        condition: str | None = None,
        names: Mapping[str, str] | None = None,
        values: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        request: dict[str, Any] = {
            "TableName": self.table_name,
            "Key": self.serialize_item(key),
            "UpdateExpression": update_expression,
        }
        return self._with_condition(request, condition, names, values)

    def condition_check_request(
        self,
        key: Mapping[str, str],
        *,
        condition: str,
        names: Mapping[str, str] | None = None,
        values: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        request: dict[str, Any] = {"TableName": self.table_name, "Key": self.serialize_item(key)}
        return self._with_condition(request, condition, names, values)

    def version_check_request(
        self, key: Mapping[str, str], expected_version: int
    ) -> dict[str, Any]:
        return self.condition_check_request(
            key,
            condition="attribute_exists(PK) AND #version = :expected_version",
            names={"#version": "version"},
            values={":expected_version": expected_version},
        )

    def receipt_put_request(self, receipt: IdempotencyReceipt) -> dict[str, Any]:
        """Receipt Put that succeeds only when no live receipt exists for the same key."""
        return self.put_request(
            idempotency_receipt_to_item(receipt),
            condition="attribute_not_exists(PK) OR #expires_at <= :now",
            names={"#expires_at": "expires_at"},
            values={":now": self._now_epoch()},
        )

    # Sync primitives -----------------------------------------------------------------------

    def get_item_sync(
        self, key: Mapping[str, str], *, consistent: bool = False
    ) -> dict[str, Any] | None:
        response = self.client.get_item(
            TableName=self.table_name,
            Key=self.serialize_item(key),
            ConsistentRead=consistent,
        )
        item = response.get("Item")
        if item is None:
            return None
        return self.deserialize_item(item)

    def query_prefix_sync(
        self, pk: str, sk_prefix: str, *, consistent: bool = False
    ) -> list[dict[str, Any]]:
        return self._query(
            "PK = :pk AND begins_with(SK, :sk_prefix)",
            {":pk": pk, ":sk_prefix": sk_prefix},
            consistent=consistent,
        )

    def query_range_sync(
        self,
        pk: str,
        *,
        lower: str | None = None,
        upper: str | None = None,
        ascending: bool = True,
        limit: int | None = None,
        consistent: bool = False,
    ) -> list[dict[str, Any]]:
        """Inclusive SK range query; at least one bound is required."""
        values: dict[str, Any] = {":pk": pk}
        if lower is not None and upper is not None:
            condition = "PK = :pk AND SK BETWEEN :sk_lower AND :sk_upper"
            values.update({":sk_lower": lower, ":sk_upper": upper})
        elif upper is not None:
            condition = "PK = :pk AND SK <= :sk_upper"
            values[":sk_upper"] = upper
        elif lower is not None:
            condition = "PK = :pk AND SK >= :sk_lower"
            values[":sk_lower"] = lower
        else:
            raise ValueError("query_range requires a lower or upper bound")
        return self._query(
            condition, values, consistent=consistent, ascending=ascending, limit=limit
        )

    def put_item_sync(self, request: Mapping[str, Any]) -> None:
        self._write(self.client.put_item, request)

    def update_item_sync(self, request: Mapping[str, Any]) -> dict[str, Any]:
        response = self._write(self.client.update_item, request)
        return self.deserialize_item(response.get("Attributes") or {})

    def transact_write_sync(self, actions: Sequence[Mapping[str, Any]]) -> None:
        if not 1 <= len(actions) <= MAX_TRANSACTION_ACTIONS:
            raise ValueError("a transaction needs 1 to 100 actions")
        self._write(self.client.transact_write_items, {"TransactItems": list(actions)})

    def get_idempotency_replay_sync(
        self, tenant_id: str, scope: str, key_hash: str, request_hash: str
    ) -> str | None:
        """Snapshot of a live receipt for the same request; a different request is a conflict."""
        item = self.get_item_sync(
            self.keys.idempotency_receipt(tenant_id, scope, key_hash), consistent=True
        )
        if item is None or int(item["expires_at"]) <= self._now_epoch():
            return None
        if item["request_hash"] != request_hash:
            raise ConflictError("idempotency key reused with a different request")
        return item["response_snapshot_json"]

    # Async wrappers ------------------------------------------------------------------------

    async def get_item(
        self, key: Mapping[str, str], *, consistent: bool = False
    ) -> dict[str, Any] | None:
        return await to_thread(self.get_item_sync, key, consistent=consistent)

    async def query_prefix(
        self, pk: str, sk_prefix: str, *, consistent: bool = False
    ) -> list[dict[str, Any]]:
        return await to_thread(self.query_prefix_sync, pk, sk_prefix, consistent=consistent)

    async def query_range(
        self,
        pk: str,
        *,
        lower: str | None = None,
        upper: str | None = None,
        ascending: bool = True,
        limit: int | None = None,
        consistent: bool = False,
    ) -> list[dict[str, Any]]:
        return await to_thread(
            self.query_range_sync,
            pk,
            lower=lower,
            upper=upper,
            ascending=ascending,
            limit=limit,
            consistent=consistent,
        )

    async def put_item(self, request: Mapping[str, Any]) -> None:
        await to_thread(self.put_item_sync, request)

    async def update_item(self, request: Mapping[str, Any]) -> dict[str, Any]:
        return await to_thread(self.update_item_sync, request)

    async def transact_write(self, actions: Sequence[Mapping[str, Any]]) -> None:
        await to_thread(self.transact_write_sync, actions)

    async def get_idempotency_replay(
        self, tenant_id: str, scope: str, key_hash: str, request_hash: str
    ) -> str | None:
        return await to_thread(
            self.get_idempotency_replay_sync, tenant_id, scope, key_hash, request_hash
        )

    # Internals -----------------------------------------------------------------------------

    def _query(
        self,
        condition: str,
        values: Mapping[str, Any],
        *,
        consistent: bool,
        ascending: bool = True,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        last_evaluated_key: dict[str, Any] | None = None
        while True:
            request: dict[str, Any] = {
                "TableName": self.table_name,
                "KeyConditionExpression": condition,
                "ExpressionAttributeValues": self.serialize_values(values),
                "ConsistentRead": consistent,
                "ScanIndexForward": ascending,
            }
            if limit is not None:
                request["Limit"] = limit - len(items)
            if last_evaluated_key is not None:
                request["ExclusiveStartKey"] = last_evaluated_key
            response = self.client.query(**request)
            items.extend(self.deserialize_item(item) for item in response.get("Items", []))
            last_evaluated_key = response.get("LastEvaluatedKey")
            if last_evaluated_key is None or (limit is not None and len(items) >= limit):
                break
        return items if limit is None else items[:limit]

    def _write(self, operation: Callable[..., Any], request: Mapping[str, Any]) -> Any:
        try:
            return operation(**request)
        except ClientError as exc:
            indexes = conditional_failure_indexes(exc)
            if indexes:
                raise DynamoConditionalConflict(indexes) from exc
            raise

    def _with_condition(
        self,
        request: dict[str, Any],
        condition: str | None,
        names: Mapping[str, str] | None,
        values: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        if condition is not None:
            request["ConditionExpression"] = condition
        if names:
            request["ExpressionAttributeNames"] = dict(names)
        if values:
            request["ExpressionAttributeValues"] = self.serialize_values(values)
        return request

    def _now_epoch(self) -> int:
        return int(self.clock().timestamp())

    def _normalize_to_dynamo(self, value: Any) -> Any:
        if isinstance(value, float):
            return Decimal(str(value))
        if isinstance(value, Mapping):
            return {key: self._normalize_to_dynamo(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [self._normalize_to_dynamo(item) for item in value]
        return value

    def _normalize_from_dynamo(self, value: Any) -> Any:
        if isinstance(value, Decimal):
            return int(value) if value % 1 == 0 else float(value)
        if isinstance(value, list):
            return [self._normalize_from_dynamo(item) for item in value]
        if isinstance(value, Mapping):
            return {key: self._normalize_from_dynamo(item) for key, item in value.items()}
        return value
