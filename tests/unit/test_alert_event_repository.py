from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
import json
from typing import Any

import pytest
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from limnopulse_api.adapters.alert_events import DynamoAlertEventRepository
from limnopulse_api.domain.alerts import AuditContext


NOW = datetime(2026, 7, 15, 12, 5, tzinfo=UTC)


class RecordingClient:
    def __init__(self) -> None:
        self.serializer = TypeSerializer()
        self.deserializer = TypeDeserializer()
        self.items: dict[tuple[str, str], dict[str, Any]] = {}
        self.query_calls: list[dict[str, Any]] = []
        self.transactions: list[dict[str, Any]] = []
        self.scan_calls = 0

    def seed(self, item: dict[str, Any]) -> None:
        self.items[(item["PK"], item["SK"])] = deepcopy(item)

    def query(self, **kwargs: Any) -> dict[str, Any]:
        self.query_calls.append(kwargs)
        values = self.decode(kwargs["ExpressionAttributeValues"])
        items = [
            item
            for item in self.items.values()
            if item.get("GSI2PK") == values[":pk"]
        ]
        return {"Items": [self.encode(item) for item in items]}

    def get_item(self, **kwargs: Any) -> dict[str, Any]:
        key = self.decode(kwargs["Key"])
        item = self.items.get((key["PK"], key["SK"]))
        return {"Item": self.encode(item)} if item is not None else {}

    def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
        self.transactions.append(kwargs)
        return {}

    def scan(self, **kwargs: Any) -> dict[str, Any]:
        self.scan_calls += 1
        return {}

    def encode(self, item: dict[str, Any]) -> dict[str, Any]:
        return {
            key: self.serializer.serialize(self.normalize(value))
            for key, value in item.items()
        }

    def decode(self, item: dict[str, Any]) -> dict[str, Any]:
        return {key: self.deserializer.deserialize(value) for key, value in item.items()}

    def normalize(self, value: Any) -> Any:
        if isinstance(value, float):
            return Decimal(str(value))
        if isinstance(value, dict):
            return {key: self.normalize(item) for key, item in value.items()}
        return value


def event_item(status: str = "open") -> dict[str, Any]:
    timestamp = "2026-07-15T12:00:00.000000000Z"
    return {
        "PK": "TENANT#tnt_1",
        "SK": "ALERT_EVENT#alert_1",
        "GSI2PK": "TENANT#tnt_1#ALERT_EVENTS",
        "GSI2SK": f"{timestamp}#EVENT#alert_1",
        "entity_type": "alert_event",
        "tenant_id": "tnt_1",
        "event_id": "alert_1",
        "rule_id": "rule_1",
        "rule_version": 2,
        "evaluation_revision": 3,
        "rule_name": "Low oxygen",
        "pond_id": "pond_1",
        "device_id": "dev_1",
        "metric": "do_mg_l",
        "operator": "<",
        "threshold": 5.0,
        "aggregation": "min",
        "severity": "critical",
        "status": status,
        "opened_at": timestamp,
        "confirmed_open_window_end": timestamp,
        "window_start": timestamp,
        "window_end": timestamp,
        "last_evaluated_at": timestamp,
        "last_evaluation_quality": "sufficient",
        "last_evaluation_value": 4.2,
        "created_at": timestamp,
        "updated_at": timestamp,
        "version": 1,
        "schema_version": 1,
    }


def repository(client: RecordingClient) -> DynamoAlertEventRepository:
    return DynamoAlertEventRepository(
        "LimnopulseDomain", "LimnopulseAudit", client, clock=lambda: NOW
    )


def audit() -> AuditContext:
    return AuditContext(actor_id="sub_1", ip="203.0.113.2", user_agent="pytest")


@pytest.mark.asyncio
async def test_list_uses_event_gsi_query_without_scan() -> None:
    client = RecordingClient()
    client.seed(event_item())

    events = await repository(client).list_events("tnt_1")

    assert [event.event_id for event in events] == ["alert_1"]
    assert client.query_calls[0]["IndexName"] == "AlertEventsByTenantTime"
    assert client.query_calls[0]["ScanIndexForward"] is False
    assert client.scan_calls == 0


@pytest.mark.asyncio
async def test_acknowledge_atomically_updates_event_transition_and_audit() -> None:
    client = RecordingClient()
    client.seed(event_item())

    updated = await repository(client).acknowledge_event("tnt_1", "alert_1", 1, audit())

    assert updated.status == "acknowledged"
    operations = client.transactions[0]["TransactItems"]
    assert [set(operation) for operation in operations] == [{"Update"}, {"Put"}, {"Put"}]
    update = operations[0]["Update"]
    assert "#status IN (:status_0)" in update["ConditionExpression"]
    assert operations[1]["Put"]["TableName"] == "LimnopulseDomain"
    assert operations[2]["Put"]["TableName"] == "LimnopulseAudit"


@pytest.mark.asyncio
async def test_manual_resolution_fences_and_clears_matching_evaluator_state() -> None:
    client = RecordingClient()
    client.seed(event_item())
    client.seed(
        {
            "PK": "TENANT#tnt_1",
            "SK": "ALERT_STATE#rule_1",
            "entity_type": "alert_evaluation_state",
            "state_revision": 4,
            "state_json": json.dumps(
                {"Mode": "active", "ActiveEventID": "alert_1", "ActiveStatus": "open"}
            ),
        }
    )

    updated = await repository(client).resolve_event("tnt_1", "alert_1", 1, audit())

    assert updated.status == "resolved"
    operations = client.transactions[0]["TransactItems"]
    assert [set(operation) for operation in operations] == [
        {"Update"},
        {"Put"},
        {"Put"},
        {"Put"},
    ]
    state_put = client.decode(operations[1]["Put"]["Item"])
    state = json.loads(state_put["state_json"])
    assert state["Mode"] == "healthy"
    assert state["ActiveEventID"] == ""
    assert state_put["state_revision"] == 5
