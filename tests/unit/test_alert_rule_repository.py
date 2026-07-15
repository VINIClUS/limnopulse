from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import json
from typing import Any

import pytest
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError

from limnopulse_api.adapters.alert_rules import DynamoAlertRuleRepository
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.alerts import (
    AlertAggregation,
    AlertChannel,
    AlertMetric,
    AlertOperator,
    AlertRule,
    AlertSeverity,
    AuditContext,
)


class TransactionFailure(Exception):
    def __init__(self) -> None:
        self.response = {"Error": {"Code": "TransactionCanceledException"}}
        super().__init__("transaction cancelled")


class RecordingDynamoClient:
    def __init__(self) -> None:
        self.serializer = TypeSerializer()
        self.deserializer = TypeDeserializer()
        self.items: dict[tuple[str, str, str], dict[str, Any]] = {}
        self.query_calls: list[dict[str, Any]] = []
        self.get_item_calls: list[dict[str, Any]] = []
        self.transact_write_items_calls: list[dict[str, Any]] = []
        self.scan_calls = 0

    def seed(self, table_name: str, item: dict[str, Any]) -> None:
        self.items[(table_name, item["PK"], item["SK"])] = deepcopy(item)

    def query(self, **kwargs: Any) -> dict[str, Any]:
        self.query_calls.append(kwargs)
        values = self._decode_item(kwargs["ExpressionAttributeValues"])
        table_name = kwargs["TableName"]
        items = [
            item
            for (table, pk, sk), item in self.items.items()
            if table == table_name and pk == values[":pk"] and sk.startswith(values[":sk_prefix"])
        ]
        items.sort(key=lambda item: item["SK"])
        return {"Items": [self._encode_item(item) for item in items]}

    def get_item(self, **kwargs: Any) -> dict[str, Any]:
        self.get_item_calls.append(kwargs)
        key = self._decode_item(kwargs["Key"])
        item = self.items.get((kwargs["TableName"], key["PK"], key["SK"]))
        if item is None:
            return {}
        return {"Item": self._encode_item(item)}

    def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
        self.transact_write_items_calls.append(kwargs)
        candidate = deepcopy(self.items)
        for operation in kwargs["TransactItems"]:
            if "Put" in operation:
                self._apply_put(candidate, operation["Put"])
            elif "Update" in operation:
                self._apply_update(candidate, operation["Update"])
            else:
                self._apply_condition_check(candidate, operation["ConditionCheck"])
        self.items = candidate
        return {}

    def scan(self, **kwargs: Any) -> dict[str, Any]:
        self.scan_calls += 1
        return {}

    def _apply_put(
        self,
        items: dict[tuple[str, str, str], dict[str, Any]],
        put: dict[str, Any],
    ) -> None:
        item = self._decode_item(put["Item"])
        key = (put["TableName"], item["PK"], item["SK"])
        existing = items.get(key)
        condition = put.get("ConditionExpression", "")
        values = self._decode_item(put.get("ExpressionAttributeValues", {}))
        if "#revision = :expected_revision" in condition:
            revision_name = put["ExpressionAttributeNames"]["#revision"]
            if (
                existing is None
                or existing.get(revision_name) != values[":expected_revision"]
            ):
                raise TransactionFailure()
        elif existing is not None and "attribute_not_exists" in condition:
            now = values.get(":now")
            if now is None or existing.get("expires_at", now + 1) > now:
                raise TransactionFailure()
        items[key] = item

    def _apply_update(
        self,
        items: dict[tuple[str, str, str], dict[str, Any]],
        update: dict[str, Any],
    ) -> None:
        key_fields = self._decode_item(update["Key"])
        key = (update["TableName"], key_fields["PK"], key_fields["SK"])
        existing = items.get(key)
        if existing is None:
            raise TransactionFailure()
        values = self._decode_item(update["ExpressionAttributeValues"])
        if ":expected_version" in values:
            if existing.get("version") != values[":expected_version"]:
                raise TransactionFailure()
            if ":active" in values and existing.get("status") != values[":active"]:
                raise TransactionFailure()
        elif ":revision" in values:
            active_statuses = {
                values[":open"],
                values[":acknowledged"],
                values[":suppressed"],
            }
            if (
                existing.get("status") not in active_statuses
                or existing.get("evaluation_revision") != values[":revision"]
            ):
                raise TransactionFailure()
        updated = dict(existing)
        set_expression, _, remove_expression = update["UpdateExpression"].partition(" REMOVE ")
        for assignment in set_expression.removeprefix("SET ").split(", "):
            name_token, assignment_value = assignment.split(" = ")
            field_name = update["ExpressionAttributeNames"][name_token]
            if " + " in assignment_value:
                source_token, value_token = assignment_value.split(" + ")
                source_name = update["ExpressionAttributeNames"][source_token]
                updated[field_name] = updated[source_name] + values[value_token]
            else:
                updated[field_name] = values[assignment_value]
        if remove_expression:
            for name_token in remove_expression.split(", "):
                updated.pop(update["ExpressionAttributeNames"][name_token], None)
        items[key] = updated

    def _apply_condition_check(
        self,
        items: dict[tuple[str, str, str], dict[str, Any]],
        check: dict[str, Any],
    ) -> None:
        key_fields = self._decode_item(check["Key"])
        key = (check["TableName"], key_fields["PK"], key_fields["SK"])
        existing = items.get(key)
        condition = check["ConditionExpression"]
        if "attribute_not_exists" in condition:
            if existing is not None:
                raise TransactionFailure()
            return
        values = self._decode_item(check["ExpressionAttributeValues"])
        revision_name = check["ExpressionAttributeNames"]["#revision"]
        if existing is None or existing.get(revision_name) != values[":expected_revision"]:
            raise TransactionFailure()

    def _encode_item(self, item: dict[str, Any]) -> dict[str, Any]:
        return {
            key: self.serializer.serialize(self._normalize_for_dynamodb(value))
            for key, value in item.items()
        }

    def _decode_item(self, item: dict[str, Any]) -> dict[str, Any]:
        return {key: self.deserializer.deserialize(value) for key, value in item.items()}

    def _normalize_for_dynamodb(self, value: Any) -> Any:
        if isinstance(value, float):
            return Decimal(str(value))
        if isinstance(value, dict):
            return {key: self._normalize_for_dynamodb(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [self._normalize_for_dynamodb(item) for item in value]
        return value


NOW = datetime(2026, 7, 15, 12, 0, tzinfo=UTC)


def make_rule(
    *,
    rule_id: str = "rule_1",
    version: int = 1,
    metric: AlertMetric = AlertMetric.DO_MG_L,
    threshold: float = 5.0,
    status: str = "active",
    enabled: bool = True,
    replaces_rule_id: str | None = None,
    replaced_by_rule_id: str | None = None,
) -> AlertRule:
    return AlertRule(
        tenant_id="tnt_1",
        rule_id=rule_id,
        pond_id="pond_1",
        device_id="dev_1",
        metric=metric,
        name="Low oxygen",
        operator=AlertOperator.LESS_THAN,
        threshold=threshold,
        aggregation=AlertAggregation.MIN,
        window="5m",
        duration="3m",
        severity=AlertSeverity.CRITICAL,
        channels=(AlertChannel.EMAIL, AlertChannel.TELEGRAM),
        cooldown_seconds=1_800,
        enabled=enabled,
        replaces_rule_id=replaces_rule_id,
        replaced_by_rule_id=replaced_by_rule_id,
        status=status,
        created_at=NOW,
        updated_at=NOW,
        version=version,
    )


def make_repository(client: RecordingDynamoClient) -> DynamoAlertRuleRepository:
    return DynamoAlertRuleRepository(
        domain_table_name="LimnopulseDomain",
        audit_table_name="LimnopulseAudit",
        client=client,
        clock=lambda: NOW,
    )


def audit_context() -> AuditContext:
    return AuditContext(actor_id="sub_1", ip="203.0.113.5", user_agent="pytest")


def decode_put(client: RecordingDynamoClient, operation: dict[str, Any]) -> dict[str, Any]:
    return client._decode_item(operation["Put"]["Item"])


@pytest.mark.asyncio
async def test_list_rules_queries_alert_rule_prefix_without_scan() -> None:
    client = RecordingDynamoClient()
    client.seed("LimnopulseDomain", make_repository(client)._rule_to_item(make_rule()))
    repository = make_repository(client)

    rules = await repository.list_rules("tnt_1")

    assert [rule.rule_id for rule in rules] == ["rule_1"]
    assert client.query_calls[0]["KeyConditionExpression"] == (
        "PK = :pk AND begins_with(SK, :sk_prefix)"
    )
    assert client._decode_item(client.query_calls[0]["ExpressionAttributeValues"]) == {
        ":pk": "TENANT#tnt_1",
        ":sk_prefix": "ALERT_RULE#",
    }
    assert client.scan_calls == 0


@pytest.mark.asyncio
async def test_create_rule_atomically_puts_rule_and_redacted_audit() -> None:
    client = RecordingDynamoClient()
    repository = make_repository(client)

    created = await repository.create_rule(make_rule(), audit_context())

    assert created.version == 1
    transaction = client.transact_write_items_calls[0]["TransactItems"]
    assert [operation["Put"]["TableName"] for operation in transaction] == [
        "LimnopulseDomain",
        "LimnopulseAudit",
    ]
    rule_item = decode_put(client, transaction[0])
    assert rule_item["evaluation_revision"] == 1
    assert rule_item["evaluation_bucket"] == 29
    assert rule_item["GSI1PK"] == "ALERT_EVALUATION#V1#BUCKET#29"
    assert rule_item["GSI1SK"].startswith("2026-07-15T12:00:45.000000000Z#")
    audit = decode_put(client, transaction[1])
    assert audit["action"] == "alert_rule.created"
    assert audit["actor_id"] == "sub_1"
    assert audit["resource_id"] == "rule_1"
    assert len(audit["before_hash"]) == 64
    assert len(audit["after_hash"]) == 64
    assert audit["expires_at"] == int((NOW + timedelta(days=90)).timestamp())
    assert "token" not in audit
    assert "payload" not in audit


@pytest.mark.asyncio
async def test_create_rule_maps_conditional_transaction_to_conflict() -> None:
    client = RecordingDynamoClient()
    repository = make_repository(client)
    client.seed("LimnopulseDomain", repository._rule_to_item(make_rule()))

    with pytest.raises(ConflictError):
        await repository.create_rule(make_rule(), audit_context())


@pytest.mark.asyncio
async def test_transaction_throttling_remains_an_infrastructure_error() -> None:
    class ThrottledDynamoClient(RecordingDynamoClient):
        def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
            raise ClientError(
                {
                    "Error": {
                        "Code": "TransactionCanceledException",
                        "Message": "transaction cancelled",
                    },
                    "CancellationReasons": [{"Code": "ThrottlingError"}],
                },
                "TransactWriteItems",
            )

    repository = make_repository(ThrottledDynamoClient())

    with pytest.raises(ClientError) as captured:
        await repository.create_rule(make_rule(), audit_context())

    assert captured.value.response["CancellationReasons"] == [{"Code": "ThrottlingError"}]


@pytest.mark.asyncio
async def test_update_rule_conditions_on_version_and_writes_audit() -> None:
    client = RecordingDynamoClient()
    repository = make_repository(client)
    client.seed("LimnopulseDomain", repository._rule_to_item(make_rule(version=2)))

    updated = await repository.update_rule(
        "tnt_1",
        "rule_1",
        2,
        {"threshold": 4.5, "enabled": False},
        audit_context(),
    )

    assert updated.version == 3
    assert updated.threshold == 4.5
    assert updated.enabled is False
    assert updated.evaluation_revision == 2
    transaction = client.transact_write_items_calls[0]["TransactItems"]
    assert [set(operation) for operation in transaction] == [
        {"Update"},
        {"ConditionCheck"},
        {"Put"},
    ]
    update = transaction[0]["Update"]
    assert client._decode_item(update["ExpressionAttributeValues"])[":expected_version"] == 2
    assert "#status = :active" in update["ConditionExpression"]
    assert " REMOVE " in update["UpdateExpression"]
    state_check = transaction[1]["ConditionCheck"]
    assert "attribute_not_exists(PK)" in state_check["ConditionExpression"]
    stored = client.items[("LimnopulseDomain", "TENANT#tnt_1", "ALERT_RULE#rule_1")]
    assert "GSI1PK" not in stored
    assert "GSI1SK" not in stored
    assert "next_evaluation_at" not in stored
    assert decode_put(client, transaction[2])["action"] == "alert_rule.updated"


@pytest.mark.asyncio
async def test_cosmetic_update_preserves_evaluation_revision_and_schedule() -> None:
    client = RecordingDynamoClient()
    repository = make_repository(client)
    client.seed("LimnopulseDomain", repository._rule_to_item(make_rule(version=2)))

    updated = await repository.update_rule(
        "tnt_1",
        "rule_1",
        2,
        {"name": "Renamed"},
        audit_context(),
    )

    assert updated.version == 3
    assert updated.evaluation_revision == 1
    transaction = client.transact_write_items_calls[0]["TransactItems"]
    assert [set(operation) for operation in transaction] == [{"Update"}, {"Put"}]
    stored = client.items[("LimnopulseDomain", "TENANT#tnt_1", "ALERT_RULE#rule_1")]
    assert stored["GSI1PK"] == "ALERT_EVALUATION#V1#BUCKET#29"


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["active", "pending"])
async def test_noop_semantic_update_preserves_evaluator_generation(mode: str) -> None:
    client = RecordingDynamoClient()
    repository = make_repository(client)
    client.seed("LimnopulseDomain", repository._rule_to_item(make_rule(version=2)))
    state: dict[str, Any] = {"Mode": mode}
    if mode == "active":
        state.update({"ActiveEventID": "alert_1", "ActiveStatus": "open"})
        client.seed(
            "LimnopulseDomain",
            {
                "PK": "TENANT#tnt_1",
                "SK": "ALERT_EVENT#alert_1",
                "entity_type": "alert_event",
                "status": "open",
                "evaluation_revision": 1,
                "version": 1,
            },
        )
    else:
        state.update(
            {
                "ConfirmedSlots": 2,
                "PendingSince": "2026-07-15T11:58:45Z",
                "LastBreachSlot": "2026-07-15T11:59:45Z",
            }
        )
    client.seed(
        "LimnopulseDomain",
        {
            "PK": "TENANT#tnt_1",
            "SK": "ALERT_STATE#rule_1",
            "entity_type": "alert_evaluation_state",
            "state_revision": 4,
            "state_json": json.dumps(state),
        },
    )

    updated = await repository.update_rule(
        "tnt_1", "rule_1", 2, {"threshold": 5.0}, audit_context()
    )

    assert updated.version == 3
    assert updated.evaluation_revision == 1
    state_item = client.items[("LimnopulseDomain", "TENANT#tnt_1", "ALERT_STATE#rule_1")]
    assert state_item["state_revision"] == 4
    assert json.loads(state_item["state_json"])["Mode"] == mode
    if mode == "active":
        event_item = client.items[
            ("LimnopulseDomain", "TENANT#tnt_1", "ALERT_EVENT#alert_1")
        ]
        assert event_item["status"] == "open"
    operations = client.transact_write_items_calls[0]["TransactItems"]
    assert [set(operation) for operation in operations] == [{"Update"}, {"Put"}]


@pytest.mark.asyncio
async def test_noop_semantic_field_with_cosmetic_change_keeps_generation() -> None:
    client = RecordingDynamoClient()
    repository = make_repository(client)
    client.seed("LimnopulseDomain", repository._rule_to_item(make_rule(version=2)))

    updated = await repository.update_rule(
        "tnt_1",
        "rule_1",
        2,
        {"threshold": 5.0, "name": "Renamed"},
        audit_context(),
    )

    assert updated.name == "Renamed"
    assert updated.evaluation_revision == 1
    operations = client.transact_write_items_calls[0]["TransactItems"]
    assert [set(operation) for operation in operations] == [{"Update"}, {"Put"}]


@pytest.mark.asyncio
async def test_noop_semantic_update_on_disabled_rule_has_no_operational_removals() -> None:
    client = RecordingDynamoClient()
    repository = make_repository(client)
    client.seed(
        "LimnopulseDomain",
        repository._rule_to_item(make_rule(version=2, enabled=False)),
    )

    updated = await repository.update_rule(
        "tnt_1", "rule_1", 2, {"threshold": 5.0}, audit_context()
    )

    assert updated.version == 3
    assert updated.evaluation_revision == 1
    operations = client.transact_write_items_calls[0]["TransactItems"]
    assert [set(operation) for operation in operations] == [{"Update"}, {"Put"}]
    assert " REMOVE " not in operations[0]["Update"]["UpdateExpression"]


@pytest.mark.asyncio
async def test_semantic_update_conflicts_when_state_appears_after_snapshot() -> None:
    class RacingClient(RecordingDynamoClient):
        def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
            self.seed(
                "LimnopulseDomain",
                {
                    "PK": "TENANT#tnt_1",
                    "SK": "ALERT_STATE#rule_1",
                    "entity_type": "alert_evaluation_state",
                    "state_revision": 1,
                    "state_json": json.dumps({"Mode": "healthy"}),
                },
            )
            return super().transact_write_items(**kwargs)

    client = RacingClient()
    repository = make_repository(client)
    client.seed("LimnopulseDomain", repository._rule_to_item(make_rule(version=2)))

    with pytest.raises(ConflictError):
        await repository.update_rule(
            "tnt_1", "rule_1", 2, {"threshold": 4.5}, audit_context()
        )

    stored = client.items[("LimnopulseDomain", "TENANT#tnt_1", "ALERT_RULE#rule_1")]
    assert stored["version"] == 2

    updated = await repository.update_rule(
        "tnt_1", "rule_1", 2, {"threshold": 4.5}, audit_context()
    )

    assert updated.version == 3
    stored = client.items[("LimnopulseDomain", "TENANT#tnt_1", "ALERT_RULE#rule_1")]
    assert stored["version"] == 3


@pytest.mark.asyncio
async def test_semantic_update_conflicts_when_healthy_state_revision_changes() -> None:
    class RacingClient(RecordingDynamoClient):
        def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
            state_key = (
                "LimnopulseDomain",
                "TENANT#tnt_1",
                "ALERT_STATE#rule_1",
            )
            self.items[state_key]["state_revision"] = 5
            return super().transact_write_items(**kwargs)

    client = RacingClient()
    repository = make_repository(client)
    client.seed("LimnopulseDomain", repository._rule_to_item(make_rule(version=2)))
    client.seed(
        "LimnopulseDomain",
        {
            "PK": "TENANT#tnt_1",
            "SK": "ALERT_STATE#rule_1",
            "entity_type": "alert_evaluation_state",
            "state_revision": 4,
            "state_json": json.dumps({"Mode": "healthy"}),
        },
    )

    with pytest.raises(ConflictError):
        await repository.update_rule(
            "tnt_1", "rule_1", 2, {"threshold": 4.5}, audit_context()
        )

    stored = client.items[("LimnopulseDomain", "TENANT#tnt_1", "ALERT_RULE#rule_1")]
    assert stored["version"] == 2


@pytest.mark.asyncio
async def test_semantic_update_retries_after_pending_state_revision_race() -> None:
    class RacingClient(RecordingDynamoClient):
        raced = False

        def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
            if not self.raced:
                state_key = (
                    "LimnopulseDomain",
                    "TENANT#tnt_1",
                    "ALERT_STATE#rule_1",
                )
                self.items[state_key]["state_revision"] += 1
                self.raced = True
            return super().transact_write_items(**kwargs)

    client = RacingClient()
    repository = make_repository(client)
    client.seed("LimnopulseDomain", repository._rule_to_item(make_rule(version=2)))
    client.seed(
        "LimnopulseDomain",
        {
            "PK": "TENANT#tnt_1",
            "SK": "ALERT_STATE#rule_1",
            "entity_type": "alert_evaluation_state",
            "state_revision": 4,
            "state_json": json.dumps(
                {
                    "Mode": "pending",
                    "ConfirmedSlots": 2,
                    "PendingSince": "2026-07-15T11:58:45Z",
                    "LastBreachSlot": "2026-07-15T11:59:45Z",
                }
            ),
        },
    )

    with pytest.raises(ConflictError):
        await repository.update_rule(
            "tnt_1", "rule_1", 2, {"threshold": 4.5}, audit_context()
        )

    updated = await repository.update_rule(
        "tnt_1", "rule_1", 2, {"threshold": 4.5}, audit_context()
    )

    assert updated.version == 3
    state_item = client.items[("LimnopulseDomain", "TENANT#tnt_1", "ALERT_STATE#rule_1")]
    assert state_item["state_revision"] == 6
    assert json.loads(state_item["state_json"])["Mode"] == "healthy"


@pytest.mark.asyncio
async def test_semantic_update_retries_after_active_state_revision_race() -> None:
    class RacingClient(RecordingDynamoClient):
        raced = False

        def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
            if not self.raced:
                state_key = (
                    "LimnopulseDomain",
                    "TENANT#tnt_1",
                    "ALERT_STATE#rule_1",
                )
                self.items[state_key]["state_revision"] += 1
                self.raced = True
            return super().transact_write_items(**kwargs)

    client = RacingClient()
    repository = make_repository(client)
    client.seed("LimnopulseDomain", repository._rule_to_item(make_rule(version=2)))
    client.seed(
        "LimnopulseDomain",
        {
            "PK": "TENANT#tnt_1",
            "SK": "ALERT_STATE#rule_1",
            "entity_type": "alert_evaluation_state",
            "state_revision": 4,
            "state_json": json.dumps(
                {"Mode": "active", "ActiveEventID": "alert_1", "ActiveStatus": "open"}
            ),
        },
    )
    client.seed(
        "LimnopulseDomain",
        {
            "PK": "TENANT#tnt_1",
            "SK": "ALERT_EVENT#alert_1",
            "entity_type": "alert_event",
            "status": "open",
            "evaluation_revision": 1,
            "version": 1,
        },
    )

    with pytest.raises(ConflictError):
        await repository.update_rule(
            "tnt_1", "rule_1", 2, {"threshold": 4.5}, audit_context()
        )

    updated = await repository.update_rule(
        "tnt_1", "rule_1", 2, {"threshold": 4.5}, audit_context()
    )

    assert updated.version == 3
    state_item = client.items[("LimnopulseDomain", "TENANT#tnt_1", "ALERT_STATE#rule_1")]
    event_item = client.items[("LimnopulseDomain", "TENANT#tnt_1", "ALERT_EVENT#alert_1")]
    assert state_item["state_revision"] == 6
    assert json.loads(state_item["state_json"])["Mode"] == "healthy"
    assert event_item["status"] == "resolved"


@pytest.mark.asyncio
async def test_semantic_update_resolves_active_generation_without_outbox() -> None:
    class CapturingClient(RecordingDynamoClient):
        def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
            self.transact_write_items_calls.append(kwargs)
            return {}

    client = CapturingClient()
    repository = make_repository(client)
    client.seed("LimnopulseDomain", repository._rule_to_item(make_rule(version=2)))
    client.seed(
        "LimnopulseDomain",
        {
            "PK": "TENANT#tnt_1",
            "SK": "ALERT_STATE#rule_1",
            "entity_type": "alert_evaluation_state",
            "state_revision": 4,
            "state_json": json.dumps(
                {"Mode": "active", "ActiveEventID": "alert_1", "ActiveStatus": "open"}
            ),
        },
    )

    updated = await repository.update_rule(
        "tnt_1", "rule_1", 2, {"threshold": 4.5}, audit_context()
    )

    assert updated.evaluation_revision == 2
    operations = client.transact_write_items_calls[0]["TransactItems"]
    assert [set(operation) for operation in operations] == [
        {"Update"},
        {"Update"},
        {"Put"},
        {"Put"},
        {"Put"},
    ]
    assert all(
        "NOTIFICATION_OUTBOX" not in str(operation)
        for operation in operations
    )
    event_update = operations[1]["Update"]
    assert "#evaluation_revision = :revision" in event_update["ConditionExpression"]


@pytest.mark.asyncio
async def test_semantic_update_resets_pending_confirmation_from_previous_generation() -> None:
    class CapturingClient(RecordingDynamoClient):
        def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
            self.transact_write_items_calls.append(kwargs)
            return {}

    client = CapturingClient()
    repository = make_repository(client)
    client.seed("LimnopulseDomain", repository._rule_to_item(make_rule(version=2)))
    client.seed(
        "LimnopulseDomain",
        {
            "PK": "TENANT#tnt_1",
            "SK": "ALERT_STATE#rule_1",
            "entity_type": "alert_evaluation_state",
            "state_revision": 4,
            "state_json": json.dumps(
                {
                    "Mode": "pending",
                    "ConfirmedSlots": 2,
                    "PendingSince": "2026-07-15T11:58:45Z",
                    "LastBreachSlot": "2026-07-15T11:59:45Z",
                }
            ),
        },
    )

    await repository.update_rule(
        "tnt_1", "rule_1", 2, {"threshold": 4.5}, audit_context()
    )

    operations = client.transact_write_items_calls[0]["TransactItems"]
    assert [set(operation) for operation in operations] == [
        {"Update"},
        {"Put"},
        {"Put"},
    ]
    state_item = client._decode_item(operations[1]["Put"]["Item"])
    state = json.loads(state_item["state_json"])
    assert state["Mode"] == "healthy"
    assert state["ConfirmedSlots"] == 0
    assert state_item["state_revision"] == 5


@pytest.mark.asyncio
async def test_update_rule_missing_target_raises_not_found() -> None:
    repository = make_repository(RecordingDynamoClient())

    with pytest.raises(NotFoundError):
        await repository.update_rule(
            "tnt_1", "rule_missing", 1, {"threshold": 4.5}, audit_context()
        )


@pytest.mark.asyncio
async def test_replace_rule_is_atomic_and_replays_same_request() -> None:
    client = RecordingDynamoClient()
    repository = make_repository(client)
    client.seed("LimnopulseDomain", repository._rule_to_item(make_rule(rule_id="rule_old")))
    replacement = make_rule(
        rule_id="rule_new",
        metric=AlertMetric.PH,
        threshold=6.5,
        replaces_rule_id="rule_old",
    )

    first = await repository.replace_rule(
        "tnt_1",
        "rule_old",
        1,
        replacement,
        "replace-request-123",
        "a" * 64,
        audit_context(),
    )
    replay = await repository.replace_rule(
        "tnt_1",
        "rule_old",
        1,
        replacement,
        "replace-request-123",
        "a" * 64,
        audit_context(),
    )

    assert replay == first
    assert first.replaced.status == "replaced"
    assert first.replaced.enabled is False
    assert first.replaced.version == 2
    assert first.replaced.replaced_by_rule_id == "rule_new"
    assert first.replacement.replaces_rule_id == "rule_old"
    assert len(client.transact_write_items_calls) == 1
    transaction = client.transact_write_items_calls[0]["TransactItems"]
    assert [set(operation) for operation in transaction] == [
        {"Update"},
        {"ConditionCheck"},
        {"Put"},
        {"Put"},
        {"Put"},
    ]
    idempotency = decode_put(client, transaction[4])
    assert idempotency["expires_at"] == int((NOW + timedelta(hours=24)).timestamp())
    assert "replace-request-123" not in str(idempotency)


@pytest.mark.asyncio
async def test_replace_rule_conflicts_when_state_appears_after_snapshot() -> None:
    class RacingClient(RecordingDynamoClient):
        def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
            self.seed(
                "LimnopulseDomain",
                {
                    "PK": "TENANT#tnt_1",
                    "SK": "ALERT_STATE#rule_old",
                    "entity_type": "alert_evaluation_state",
                    "state_revision": 1,
                    "state_json": json.dumps({"Mode": "healthy"}),
                },
            )
            return super().transact_write_items(**kwargs)

    client = RacingClient()
    repository = make_repository(client)
    client.seed(
        "LimnopulseDomain",
        repository._rule_to_item(make_rule(rule_id="rule_old")),
    )

    with pytest.raises(ConflictError):
        await repository.replace_rule(
            "tnt_1",
            "rule_old",
            1,
            make_rule(rule_id="rule_new", replaces_rule_id="rule_old"),
            "replace-request-123",
            "a" * 64,
            audit_context(),
        )

    stored = client.items[("LimnopulseDomain", "TENANT#tnt_1", "ALERT_RULE#rule_old")]
    assert stored["status"] == "active"


@pytest.mark.asyncio
async def test_replace_rule_rejects_idempotency_key_reuse_with_other_payload() -> None:
    client = RecordingDynamoClient()
    repository = make_repository(client)
    client.seed("LimnopulseDomain", repository._rule_to_item(make_rule(rule_id="rule_old")))
    replacement = make_rule(rule_id="rule_new", replaces_rule_id="rule_old")
    await repository.replace_rule(
        "tnt_1",
        "rule_old",
        1,
        replacement,
        "replace-request-123",
        "a" * 64,
        audit_context(),
    )

    with pytest.raises(ConflictError):
        await repository.replace_rule(
            "tnt_1",
            "rule_old",
            1,
            replacement,
            "replace-request-123",
            "b" * 64,
            audit_context(),
        )


@pytest.mark.asyncio
async def test_expired_idempotency_record_does_not_block_replacement() -> None:
    client = RecordingDynamoClient()
    repository = make_repository(client)
    client.seed("LimnopulseDomain", repository._rule_to_item(make_rule(rule_id="rule_old")))
    idempotency_key = repository._idempotency_key("tnt_1", "replace-request-123")
    client.seed(
        "LimnopulseDomain",
        {
            **idempotency_key,
            "entity_type": "idempotency",
            "request_hash": "old",
            "expires_at": int((NOW - timedelta(seconds=1)).timestamp()),
        },
    )

    result = await repository.replace_rule(
        "tnt_1",
        "rule_old",
        1,
        make_rule(rule_id="rule_new", replaces_rule_id="rule_old"),
        "replace-request-123",
        "new",
        audit_context(),
    )

    assert result.replacement.rule_id == "rule_new"
