from collections.abc import Callable, Mapping
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import json
from typing import Any
from uuid import uuid4

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.alert_events import AlertEvent, AlertEventStatus
from limnopulse_api.domain.alerts import AuditContext


AUDIT_RETENTION = timedelta(days=90)
ACTIVE_STATUSES = (
    AlertEventStatus.OPEN,
    AlertEventStatus.ACKNOWLEDGED,
    AlertEventStatus.SUPPRESSED,
)


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


class DynamoAlertEventRepository:
    def __init__(
        self,
        domain_table_name: str,
        audit_table_name: str,
        client: Any,
        *,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.domain_table_name = domain_table_name
        self.audit_table_name = audit_table_name
        self.client = client
        self.clock = clock
        self._serializer = TypeSerializer()
        self._deserializer = TypeDeserializer()

    async def list_events(self, tenant_id: str) -> list[AlertEvent]:
        items: list[dict[str, Any]] = []
        last_key: dict[str, Any] | None = None
        while True:
            request: dict[str, Any] = {
                "TableName": self.domain_table_name,
                "IndexName": "AlertEventsByTenantTime",
                "KeyConditionExpression": "GSI2PK = :pk",
                "ExpressionAttributeValues": self._serialize_values(
                    {":pk": f"TENANT#{tenant_id}#ALERT_EVENTS"}
                ),
                "ScanIndexForward": False,
            }
            if last_key is not None:
                request["ExclusiveStartKey"] = last_key
            response = self.client.query(**request)
            items.extend(self._response_items(response))
            last_key = response.get("LastEvaluatedKey")
            if last_key is None:
                break
        return [self._event_from_item(item) for item in items]

    async def get_event(self, tenant_id: str, event_id: str) -> AlertEvent | None:
        item = self._get_item(self._event_key(tenant_id, event_id), consistent=True)
        if item is None or item.get("entity_type") != "alert_event":
            return None
        return self._event_from_item(item)

    async def acknowledge_event(
        self,
        tenant_id: str,
        event_id: str,
        expected_version: int,
        audit: AuditContext,
    ) -> AlertEvent:
        existing = await self.get_event(tenant_id, event_id)
        if existing is None:
            raise NotFoundError(f"Alert event {event_id} not found")
        if existing.status in {AlertEventStatus.SUPPRESSED, AlertEventStatus.RESOLVED}:
            raise ConflictError("alert event cannot be acknowledged")
        now = self.clock()
        updated = existing.model_copy(
            update={
                "status": AlertEventStatus.ACKNOWLEDGED,
                "acknowledged_at": now,
                "acknowledged_by": audit.actor_id,
                "updated_at": now,
                "version": expected_version + 1,
            }
        )
        self._transact(
            [
                self._event_update(
                    tenant_id,
                    event_id,
                    expected_version,
                    allowed_statuses=(AlertEventStatus.OPEN,),
                    updates={
                        "status": AlertEventStatus.ACKNOWLEDGED,
                        "acknowledged_at": now.isoformat(),
                        "acknowledged_by": audit.actor_id,
                        "updated_at": now.isoformat(),
                        "version": expected_version + 1,
                    },
                ),
                self._transition_put(updated, "acknowledged", audit.actor_id, now),
                self._audit_put(existing, updated, "alert_event.acknowledged", audit, now),
            ]
        )
        return updated

    async def resolve_event(
        self,
        tenant_id: str,
        event_id: str,
        expected_version: int,
        audit: AuditContext,
    ) -> AlertEvent:
        existing = await self.get_event(tenant_id, event_id)
        if existing is None:
            raise NotFoundError(f"Alert event {event_id} not found")
        if existing.status == AlertEventStatus.RESOLVED:
            raise ConflictError("alert event is already resolved")
        now = self.clock()
        updated = existing.model_copy(
            update={
                "status": AlertEventStatus.RESOLVED,
                "resolved_at": now,
                "resolved_by": audit.actor_id,
                "updated_at": now,
                "version": expected_version + 1,
            }
        )
        operations = [
            self._event_update(
                tenant_id,
                event_id,
                expected_version,
                allowed_statuses=ACTIVE_STATUSES,
                updates={
                    "status": AlertEventStatus.RESOLVED,
                    "resolved_at": now.isoformat(),
                    "resolved_by": audit.actor_id,
                    "resolution_reason": "manual",
                    "updated_at": now.isoformat(),
                    "version": expected_version + 1,
                },
            ),
            self._transition_put(updated, "resolved", audit.actor_id, now),
            self._audit_put(existing, updated, "alert_event.resolved", audit, now),
        ]
        state_operation = self._resolved_state_put(existing, now)
        if state_operation is not None:
            operations.insert(1, state_operation)
        self._transact(operations)
        return updated

    def _resolved_state_put(
        self,
        event: AlertEvent,
        now: datetime,
    ) -> dict[str, Any] | None:
        state_key = {
            "PK": f"TENANT#{event.tenant_id}",
            "SK": f"ALERT_STATE#{event.rule_id}",
        }
        item = self._get_item(state_key, consistent=True)
        if item is None:
            return None
        try:
            state = json.loads(str(item["state_json"]))
            revision = int(item["state_revision"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ConflictError("alert evaluation state is invalid") from exc
        if state.get("ActiveEventID") != event.event_id:
            return None
        state.update(
            {
                "Mode": "healthy",
                "ConfirmedSlots": 0,
                "PendingSince": "0001-01-01T00:00:00Z",
                "LastBreachSlot": "0001-01-01T00:00:00Z",
                "ActiveEventID": "",
                "ActiveStatus": "",
                "ActiveOpenedAt": "0001-01-01T00:00:00Z",
                "OpeningOutboxes": None,
                "SuppressionSourceEventID": "",
            }
        )
        updated_item = {
            **item,
            "state_json": json.dumps(state, separators=(",", ":"), sort_keys=True),
            "state_revision": revision + 1,
            "updated_at": now.isoformat(),
        }
        return {
            "Put": {
                "TableName": self.domain_table_name,
                "Item": self._serialize_item(updated_item),
                "ConditionExpression": "#revision = :expected_revision",
                "ExpressionAttributeNames": {"#revision": "state_revision"},
                "ExpressionAttributeValues": self._serialize_values(
                    {":expected_revision": revision}
                ),
            }
        }

    def _event_update(
        self,
        tenant_id: str,
        event_id: str,
        expected_version: int,
        *,
        allowed_statuses: tuple[AlertEventStatus, ...],
        updates: Mapping[str, Any],
    ) -> dict[str, Any]:
        names = {"#version": "version", "#status": "status"}
        values: dict[str, Any] = {":expected_version": expected_version}
        assignments: list[str] = []
        for index, (field_name, value) in enumerate(updates.items()):
            name = f"#field_{index}"
            token = f":value_{index}"
            names[name] = field_name
            values[token] = str(value) if isinstance(value, AlertEventStatus) else value
            assignments.append(f"{name} = {token}")
        statuses: list[str] = []
        for index, event_status in enumerate(allowed_statuses):
            token = f":status_{index}"
            values[token] = str(event_status)
            statuses.append(token)
        return {
            "Update": {
                "TableName": self.domain_table_name,
                "Key": self._serialize_item(self._event_key(tenant_id, event_id)),
                "UpdateExpression": "SET " + ", ".join(assignments),
                "ConditionExpression": (
                    "#version = :expected_version AND #status IN (" + ", ".join(statuses) + ")"
                ),
                "ExpressionAttributeNames": names,
                "ExpressionAttributeValues": self._serialize_values(values),
            }
        }

    def _transition_put(
        self,
        event: AlertEvent,
        transition: str,
        actor_id: str,
        now: datetime,
    ) -> dict[str, Any]:
        transition_id = f"transition_{uuid4().hex}"
        item = {
            "PK": f"TENANT#{event.tenant_id}",
            "SK": f"ALERT_EVENT#{event.event_id}#TRANSITION#{now.isoformat()}#{transition_id}",
            "entity_type": "alert_event_transition",
            "transition_id": transition_id,
            "event_id": event.event_id,
            "tenant_id": event.tenant_id,
            "rule_id": event.rule_id,
            "transition": transition,
            "actor_type": "user",
            "actor_id": actor_id,
            "created_at": now.isoformat(),
        }
        return self._conditioned_put(self.domain_table_name, item)

    def _audit_put(
        self,
        before: AlertEvent,
        after: AlertEvent,
        action: str,
        audit: AuditContext,
        now: datetime,
    ) -> dict[str, Any]:
        event_id = f"audit_{uuid4().hex}"
        item = {
            "PK": f"TENANT#{before.tenant_id}#MONTH#{now:%Y-%m}",
            "SK": f"{now.isoformat()}#{event_id}",
            "entity_type": "audit_event",
            "event_id": event_id,
            "tenant_id": before.tenant_id,
            "actor_type": "user",
            "actor_id": audit.actor_id,
            "action": action,
            "resource_type": "alert_event",
            "resource_id": before.event_id,
            "before_status": str(before.status),
            "after_status": str(after.status),
            "ip": audit.ip,
            "user_agent": audit.user_agent,
            "created_at": now.isoformat(),
            "expires_at": int((now + AUDIT_RETENTION).timestamp()),
        }
        return self._conditioned_put(self.audit_table_name, item)

    def _conditioned_put(self, table: str, item: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "Put": {
                "TableName": table,
                "Item": self._serialize_item(item),
                "ConditionExpression": "attribute_not_exists(PK) AND attribute_not_exists(SK)",
            }
        }

    def _transact(self, operations: list[dict[str, Any]]) -> None:
        try:
            self.client.transact_write_items(TransactItems=operations)
        except Exception as exc:
            if self._is_conflict(exc):
                raise ConflictError("alert event changed concurrently") from exc
            raise

    def _is_conflict(self, exc: Exception) -> bool:
        response = getattr(exc, "response", {})
        code = response.get("Error", {}).get("Code")
        if code == "ConditionalCheckFailedException":
            return True
        if code != "TransactionCanceledException":
            return False
        reasons = response.get("CancellationReasons")
        return reasons is None or any(
            reason.get("Code") in {"ConditionalCheckFailed", "TransactionConflict"}
            for reason in reasons
        )

    def _event_key(self, tenant_id: str, event_id: str) -> dict[str, str]:
        return {"PK": f"TENANT#{tenant_id}", "SK": f"ALERT_EVENT#{event_id}"}

    def _get_item(
        self,
        key: Mapping[str, str],
        *,
        consistent: bool = False,
    ) -> dict[str, Any] | None:
        response = self.client.get_item(
            TableName=self.domain_table_name,
            Key=self._serialize_item(key),
            ConsistentRead=consistent,
        )
        item = response.get("Item")
        return self._deserialize_item(item) if item is not None else None

    def _event_from_item(self, item: Mapping[str, Any]) -> AlertEvent:
        values = {
            key: value
            for key, value in item.items()
            if key not in {"PK", "SK", "GSI2PK", "GSI2SK", "entity_type", "resolution_reason"}
        }
        return AlertEvent.model_validate(values)

    def _response_items(self, response: Mapping[str, Any]) -> list[dict[str, Any]]:
        return [self._deserialize_item(item) for item in response.get("Items", [])]

    def _serialize_item(self, item: Mapping[str, Any]) -> dict[str, Any]:
        return {
            key: self._serializer.serialize(self._normalize_for_dynamodb(value))
            for key, value in item.items()
            if value is not None
        }

    def _serialize_values(self, values: Mapping[str, Any]) -> dict[str, Any]:
        return self._serialize_item(values)

    def _normalize_for_dynamodb(self, value: Any) -> Any:
        if isinstance(value, float):
            return Decimal(str(value))
        if isinstance(value, Mapping):
            return {key: self._normalize_for_dynamodb(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [self._normalize_for_dynamodb(item) for item in value]
        return value

    def _deserialize_item(self, item: Mapping[str, Any]) -> dict[str, Any]:
        return {
            key: self._normalize_from_dynamodb(self._deserializer.deserialize(value))
            for key, value in item.items()
        }

    def _normalize_from_dynamodb(self, value: Any) -> Any:
        if isinstance(value, Decimal):
            return int(value) if value % 1 == 0 else float(value)
        if isinstance(value, list):
            return [self._normalize_from_dynamodb(item) for item in value]
        if isinstance(value, Mapping):
            return {key: self._normalize_from_dynamodb(item) for key, item in value.items()}
        return deepcopy(value)
