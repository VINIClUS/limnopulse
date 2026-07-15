from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
import json
from typing import Any
from uuid import uuid4

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from pydantic import BaseModel

from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.adapters.alert_event_state import (
    decode_evaluator_state,
    reset_pending_evaluator_state,
    resolved_evaluator_state,
)
from limnopulse_api.domain.alerts import (
    AlertRule,
    AlertRuleReplacement,
    AlertRuleUpdates,
    AuditContext,
)
from limnopulse_api.domain.alert_scheduling import (
    EVALUATION_SCHEDULE_FIELDS,
    EVALUATION_SEMANTIC_FIELDS,
    alert_evaluation_schedule,
)


AUDIT_RETENTION = timedelta(days=90)
IDEMPOTENCY_RETENTION = timedelta(hours=24)


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


class DynamoAlertRuleRepository:
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

    async def list_rules(self, tenant_id: str) -> list[AlertRule]:
        items: list[dict[str, Any]] = []
        last_evaluated_key: dict[str, Any] | None = None
        while True:
            request: dict[str, Any] = {
                "TableName": self.domain_table_name,
                "KeyConditionExpression": "PK = :pk AND begins_with(SK, :sk_prefix)",
                "ExpressionAttributeValues": self._serialize_values(
                    {
                        ":pk": f"TENANT#{tenant_id}",
                        ":sk_prefix": "ALERT_RULE#",
                    }
                ),
            }
            if last_evaluated_key is not None:
                request["ExclusiveStartKey"] = last_evaluated_key
            response = self.client.query(**request)
            items.extend(self._response_items(response))
            last_evaluated_key = response.get("LastEvaluatedKey")
            if last_evaluated_key is None:
                break
        return [self._rule_from_item(item) for item in items]

    async def create_rule(self, rule: AlertRule, audit: AuditContext) -> AlertRule:
        now = self.clock()
        rule_item = self._rule_to_item(rule)
        audit_item = self._audit_item(
            tenant_id=rule.tenant_id,
            action="alert_rule.created",
            resource_id=rule.rule_id,
            before=None,
            after=rule,
            context=audit,
            now=now,
        )
        self._transact(
            [
                self._conditioned_put(self.domain_table_name, rule_item),
                self._conditioned_put(self.audit_table_name, audit_item),
            ]
        )
        return rule

    async def update_rule(
        self,
        tenant_id: str,
        rule_id: str,
        expected_version: int,
        updates: AlertRuleUpdates,
        audit: AuditContext,
    ) -> AlertRule:
        existing = self._get_rule(tenant_id, rule_id)
        if existing is None:
            raise NotFoundError(f"Alert rule {rule_id} not found")

        now = self.clock()
        semantic_change = bool(EVALUATION_SEMANTIC_FIELDS.intersection(updates))
        evaluation_revision = existing.evaluation_revision + int(semantic_change)
        updated = AlertRule.model_validate(
            {
                **existing.model_dump(mode="python"),
                **dict(updates),
                "updated_at": now,
                "version": expected_version + 1,
                "evaluation_revision": evaluation_revision,
            }
        )
        audit_item = self._audit_item(
            tenant_id=tenant_id,
            action="alert_rule.updated",
            resource_id=rule_id,
            before=existing,
            after=updated,
            context=audit,
            now=now,
        )
        operational_updates: dict[str, Any] = {
            "evaluation_revision": evaluation_revision,
        }
        removed_fields: set[str] = set()
        if semantic_change and updated.enabled:
            operational_updates.update(alert_evaluation_schedule(tenant_id, rule_id, now))
            removed_fields.update({"lease_owner", "lease_expires_at"})
        elif not updated.enabled:
            removed_fields.update(EVALUATION_SCHEDULE_FIELDS)

        resolution_operations = (
            self._administrative_resolution_operations(
                existing,
                audit,
                now,
                reason="rule_semantics_changed",
            )
            if semantic_change
            else []
        )
        self._transact(
            [
                self._conditioned_rule_update(
                    tenant_id=tenant_id,
                    rule_id=rule_id,
                    expected_version=expected_version,
                    updates={
                        **dict(updates),
                        "updated_at": now.isoformat(),
                        "version": expected_version + 1,
                        **operational_updates,
                    },
                    remove_fields=removed_fields,
                ),
                *resolution_operations,
                self._conditioned_put(self.audit_table_name, audit_item),
            ]
        )
        return updated

    async def replace_rule(
        self,
        tenant_id: str,
        rule_id: str,
        expected_version: int,
        replacement: AlertRule,
        idempotency_key: str,
        request_hash: str,
        audit: AuditContext,
    ) -> AlertRuleReplacement:
        now = self.clock()
        idempotency_key_fields = self._idempotency_key(tenant_id, idempotency_key)
        replay = await self.get_replacement_replay(
            tenant_id,
            idempotency_key,
            request_hash,
        )
        if replay is not None:
            return replay

        existing = self._get_rule(tenant_id, rule_id)
        if existing is None:
            raise NotFoundError(f"Alert rule {rule_id} not found")

        replaced = AlertRule.model_validate(
            {
                **existing.model_dump(mode="python"),
                "enabled": False,
                "status": "replaced",
                "replaced_by_rule_id": replacement.rule_id,
                "updated_at": now,
                "version": expected_version + 1,
                "evaluation_revision": existing.evaluation_revision + 1,
            }
        )
        replacement = AlertRule.model_validate(
            {
                **replacement.model_dump(mode="python"),
                "tenant_id": tenant_id,
                "replaces_rule_id": rule_id,
                "replaced_by_rule_id": None,
                "status": "active",
                "version": 1,
            }
        )
        result = AlertRuleReplacement(replaced=replaced, replacement=replacement)
        audit_item = self._audit_item(
            tenant_id=tenant_id,
            action="alert_rule.replaced",
            resource_id=rule_id,
            before=existing,
            after=result,
            context=audit,
            now=now,
        )
        idempotency_item = {
            **idempotency_key_fields,
            "entity_type": "alert_rule_replace_idempotency",
            "request_hash": request_hash,
            "replaced": replaced.model_dump(mode="json"),
            "replacement": replacement.model_dump(mode="json"),
            "created_at": now.isoformat(),
            "expires_at": int((now + IDEMPOTENCY_RETENTION).timestamp()),
        }
        idempotency_put = {
            "Put": {
                "TableName": self.domain_table_name,
                "Item": self._serialize_item(idempotency_item),
                "ConditionExpression": "attribute_not_exists(PK) OR #expires_at <= :now",
                "ExpressionAttributeNames": {"#expires_at": "expires_at"},
                "ExpressionAttributeValues": self._serialize_values({":now": int(now.timestamp())}),
            }
        }
        try:
            resolution_operations = self._administrative_resolution_operations(
                existing,
                audit,
                now,
                reason="rule_replaced",
            )
            self.client.transact_write_items(
                TransactItems=[
                    self._conditioned_rule_update(
                        tenant_id=tenant_id,
                        rule_id=rule_id,
                        expected_version=expected_version,
                        updates={
                            "enabled": False,
                            "status": "replaced",
                            "replaced_by_rule_id": replacement.rule_id,
                            "updated_at": now.isoformat(),
                            "version": expected_version + 1,
                            "evaluation_revision": existing.evaluation_revision + 1,
                        },
                        remove_fields=EVALUATION_SCHEDULE_FIELDS,
                    ),
                    *resolution_operations,
                    self._conditioned_put(
                        self.domain_table_name,
                        self._rule_to_item(replacement),
                    ),
                    self._conditioned_put(self.audit_table_name, audit_item),
                    idempotency_put,
                ],
                ClientRequestToken=self._client_request_token(
                    tenant_id,
                    idempotency_key,
                    request_hash,
                ),
            )
        except Exception as exc:
            replay = self._replay_idempotency(
                self._get_item(self.domain_table_name, idempotency_key_fields),
                request_hash,
                now,
            )
            if replay is not None:
                return replay
            self._raise_if_conflict(exc)
            raise
        return result

    def _administrative_resolution_operations(
        self,
        rule: AlertRule,
        audit: AuditContext,
        now: datetime,
        *,
        reason: str,
    ) -> list[dict[str, Any]]:
        state_key = {
            "PK": f"TENANT#{rule.tenant_id}",
            "SK": f"ALERT_STATE#{rule.rule_id}",
        }
        state_item = self._get_item(self.domain_table_name, state_key, consistent=True)
        if state_item is None:
            return [self._state_snapshot_condition(state_key)]
        try:
            state, state_revision = decode_evaluator_state(state_item)
        except ValueError as exc:
            raise ConflictError("alert evaluation state is invalid") from exc
        if state.get("Mode") == "pending":
            pending_reset = reset_pending_evaluator_state(state_item, now)
            if pending_reset is None:
                return []
            reset_state, previous_revision = pending_reset
            return [self._state_put(reset_state, previous_revision)]
        event_id = str(state.get("ActiveEventID", ""))
        if not event_id:
            return [self._state_snapshot_condition(state_key, state_revision)]
        try:
            resolved = resolved_evaluator_state(state_item, event_id, now)
        except ValueError as exc:
            raise ConflictError("alert evaluation state is invalid") from exc
        if resolved is None:
            return [self._state_snapshot_condition(state_key, state_revision)]
        resolved_state, state_revision = resolved
        event_key = {
            "PK": f"TENANT#{rule.tenant_id}",
            "SK": f"ALERT_EVENT#{event_id}",
        }
        event_values = self._serialize_values(
            {
                ":open": "open",
                ":acknowledged": "acknowledged",
                ":suppressed": "suppressed",
                ":resolved": "resolved",
                ":resolved_at": now.isoformat(),
                ":resolved_by": audit.actor_id,
                ":reason": reason,
                ":revision": rule.evaluation_revision,
                ":one": 1,
            }
        )
        transition_id = f"transition_{uuid4().hex}"
        transition_item = {
            "PK": f"TENANT#{rule.tenant_id}",
            "SK": (
                f"ALERT_EVENT#{event_id}#TRANSITION#{now.isoformat()}#{transition_id}"
            ),
            "entity_type": "alert_event_transition",
            "transition_id": transition_id,
            "event_id": event_id,
            "tenant_id": rule.tenant_id,
            "rule_id": rule.rule_id,
            "transition": "resolved",
            "reason": reason,
            "actor_type": "user",
            "actor_id": audit.actor_id,
            "created_at": now.isoformat(),
        }
        return [
            {
                "Update": {
                    "TableName": self.domain_table_name,
                    "Key": self._serialize_item(event_key),
                    "UpdateExpression": (
                        "SET #status = :resolved, #resolved_at = :resolved_at, "
                        "#resolved_by = :resolved_by, #resolution_reason = :reason, "
                        "#updated_at = :resolved_at, #version = #version + :one"
                    ),
                    "ConditionExpression": (
                        "#status IN (:open, :acknowledged, :suppressed) "
                        "AND #evaluation_revision = :revision"
                    ),
                    "ExpressionAttributeNames": {
                        "#status": "status",
                        "#resolved_at": "resolved_at",
                        "#resolved_by": "resolved_by",
                        "#resolution_reason": "resolution_reason",
                        "#updated_at": "updated_at",
                        "#version": "version",
                        "#evaluation_revision": "evaluation_revision",
                    },
                    "ExpressionAttributeValues": event_values,
                }
            },
            self._state_put(resolved_state, state_revision),
            self._conditioned_put(self.domain_table_name, transition_item),
        ]

    def _state_snapshot_condition(
        self,
        key: Mapping[str, str],
        expected_revision: int | None = None,
    ) -> dict[str, Any]:
        condition: dict[str, Any] = {
            "TableName": self.domain_table_name,
            "Key": self._serialize_item(dict(key)),
        }
        if expected_revision is None:
            condition["ConditionExpression"] = (
                "attribute_not_exists(PK) AND attribute_not_exists(SK)"
            )
        else:
            condition.update(
                {
                    "ConditionExpression": "#revision = :expected_revision",
                    "ExpressionAttributeNames": {"#revision": "state_revision"},
                    "ExpressionAttributeValues": self._serialize_values(
                        {":expected_revision": expected_revision}
                    ),
                }
            )
        return {"ConditionCheck": condition}

    def _state_put(
        self,
        item: Mapping[str, Any],
        previous_revision: int,
    ) -> dict[str, Any]:
        return {
            "Put": {
                "TableName": self.domain_table_name,
                "Item": self._serialize_item(item),
                "ConditionExpression": "#revision = :expected_revision",
                "ExpressionAttributeNames": {"#revision": "state_revision"},
                "ExpressionAttributeValues": self._serialize_values(
                    {":expected_revision": previous_revision}
                ),
            }
        }

    async def get_replacement_replay(
        self,
        tenant_id: str,
        idempotency_key: str,
        request_hash: str,
    ) -> AlertRuleReplacement | None:
        item = self._get_item(
            self.domain_table_name,
            self._idempotency_key(tenant_id, idempotency_key),
        )
        return self._replay_idempotency(item, request_hash, self.clock())

    def _get_rule(self, tenant_id: str, rule_id: str) -> AlertRule | None:
        item = self._get_item(
            self.domain_table_name,
            self._rule_key(tenant_id, rule_id),
        )
        if item is None:
            return None
        return self._rule_from_item(item)

    def _get_item(
        self,
        table_name: str,
        key: Mapping[str, str],
        *,
        consistent: bool = False,
    ) -> dict[str, Any] | None:
        response = self.client.get_item(
            TableName=table_name,
            Key=self._serialize_item(dict(key)),
            ConsistentRead=consistent,
        )
        item = response.get("Item")
        if item is None:
            return None
        return self._deserialize_item(item)

    def _conditioned_put(
        self,
        table_name: str,
        item: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "Put": {
                "TableName": table_name,
                "Item": self._serialize_item(item),
                "ConditionExpression": "attribute_not_exists(PK) AND attribute_not_exists(SK)",
            }
        }

    def _conditioned_rule_update(
        self,
        *,
        tenant_id: str,
        rule_id: str,
        expected_version: int,
        updates: Mapping[str, Any],
        remove_fields: set[str] | frozenset[str] = frozenset(),
    ) -> dict[str, Any]:
        names = {
            "#version": "version",
            "#status": "status",
        }
        values: dict[str, Any] = {
            ":expected_version": expected_version,
            ":active": "active",
        }
        assignments: list[str] = []
        for index, (field_name, field_value) in enumerate(updates.items()):
            name_token = f"#field_{index}"
            value_token = f":value_{index}"
            names[name_token] = field_name
            values[value_token] = field_value
            assignments.append(f"{name_token} = {value_token}")
        removals: list[str] = []
        for index, field_name in enumerate(sorted(remove_fields)):
            name_token = f"#remove_{index}"
            names[name_token] = field_name
            removals.append(name_token)
        update_expression = "SET " + ", ".join(assignments)
        if removals:
            update_expression += " REMOVE " + ", ".join(removals)
        return {
            "Update": {
                "TableName": self.domain_table_name,
                "Key": self._serialize_item(self._rule_key(tenant_id, rule_id)),
                "UpdateExpression": update_expression,
                "ConditionExpression": (
                    "attribute_exists(PK) AND attribute_exists(SK) "
                    "AND #version = :expected_version AND #status = :active"
                ),
                "ExpressionAttributeNames": names,
                "ExpressionAttributeValues": self._serialize_values(values),
            }
        }

    def _transact(self, operations: list[dict[str, Any]]) -> None:
        try:
            self.client.transact_write_items(TransactItems=operations)
        except Exception as exc:
            self._raise_if_conflict(exc)
            raise

    def _raise_if_conflict(self, exc: Exception) -> None:
        response = getattr(exc, "response", {})
        error_code = response.get("Error", {}).get("Code")
        if error_code in {
            "ConditionalCheckFailedException",
            "IdempotentParameterMismatchException",
        }:
            raise ConflictError(str(exc)) from exc
        if error_code != "TransactionCanceledException":
            return
        reasons = response.get("CancellationReasons")
        if reasons is None or any(
            reason.get("Code") in {"ConditionalCheckFailed", "TransactionConflict"}
            for reason in reasons
        ):
            raise ConflictError(str(exc)) from exc

    def _audit_item(
        self,
        *,
        tenant_id: str,
        action: str,
        resource_id: str,
        before: Any,
        after: Any,
        context: AuditContext,
        now: datetime,
    ) -> dict[str, Any]:
        event_id = f"audit_{uuid4().hex}"
        return {
            "PK": f"TENANT#{tenant_id}#MONTH#{now:%Y-%m}",
            "SK": f"{now.isoformat()}#{event_id}",
            "entity_type": "audit_event",
            "event_id": event_id,
            "tenant_id": tenant_id,
            "actor_type": "user",
            "actor_id": context.actor_id,
            "action": action,
            "resource_type": "alert_rule",
            "resource_id": resource_id,
            "before_hash": self._hash_state(before),
            "after_hash": self._hash_state(after),
            "ip": context.ip,
            "user_agent": context.user_agent,
            "created_at": now.isoformat(),
            "expires_at": int((now + AUDIT_RETENTION).timestamp()),
        }

    def _replay_idempotency(
        self,
        item: dict[str, Any] | None,
        request_hash: str,
        now: datetime,
    ) -> AlertRuleReplacement | None:
        if item is None or int(item.get("expires_at", 0)) <= int(now.timestamp()):
            return None
        if item.get("request_hash") != request_hash:
            raise ConflictError("idempotency key already used with another request")
        return AlertRuleReplacement(
            replaced=AlertRule.model_validate(item["replaced"]),
            replacement=AlertRule.model_validate(item["replacement"]),
        )

    def _rule_key(self, tenant_id: str, rule_id: str) -> dict[str, str]:
        return {
            "PK": f"TENANT#{tenant_id}",
            "SK": f"ALERT_RULE#{rule_id}",
        }

    def _idempotency_key(self, tenant_id: str, idempotency_key: str) -> dict[str, str]:
        digest = sha256(f"{tenant_id}\0{idempotency_key}".encode()).hexdigest()
        return {
            "PK": f"TENANT#{tenant_id}",
            "SK": f"IDEMPOTENCY#ALERT_RULE_REPLACE#{digest}",
        }

    def _client_request_token(
        self,
        tenant_id: str,
        idempotency_key: str,
        request_hash: str,
    ) -> str:
        value = f"{tenant_id}\0{idempotency_key}\0{request_hash}".encode()
        return sha256(value).hexdigest()[:36]

    def _rule_to_item(self, rule: AlertRule) -> dict[str, Any]:
        item = {
            **self._rule_key(rule.tenant_id, rule.rule_id),
            "entity_type": "alert_rule",
            **rule.model_dump(mode="json"),
        }
        if rule.enabled and rule.status == "active":
            item.update(alert_evaluation_schedule(rule.tenant_id, rule.rule_id, rule.created_at))
        return item

    def _rule_from_item(self, item: Mapping[str, Any]) -> AlertRule:
        values = {
            key: value for key, value in item.items() if key not in {"PK", "SK", "entity_type"}
        }
        return AlertRule.model_validate(values)

    def _response_items(self, response: Mapping[str, Any]) -> list[dict[str, Any]]:
        return [self._deserialize_item(item) for item in response.get("Items", [])]

    def _hash_state(self, value: Any) -> str:
        encoded = json.dumps(
            self._jsonable(value),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode()
        return sha256(encoded).hexdigest()

    def _jsonable(self, value: Any) -> Any:
        if isinstance(value, BaseModel):
            return value.model_dump(mode="json")
        return value

    def _serialize_item(self, item: Mapping[str, Any]) -> dict[str, Any]:
        return {
            key: self._serializer.serialize(self._normalize_for_dynamodb(value))
            for key, value in item.items()
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
            if value % 1 == 0:
                return int(value)
            return float(value)
        if isinstance(value, list):
            return [self._normalize_from_dynamodb(item) for item in value]
        if isinstance(value, Mapping):
            return {key: self._normalize_from_dynamodb(item) for key, item in value.items()}
        return value
