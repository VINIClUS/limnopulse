"""Cross-language contract for the alert evaluation value (issue #6).

The Go evaluator owns the `state_json` format and the DynamoDB value attributes.
These tests consume the same `testdata` vectors as the Go tests in
`internal/alertevaluator/dynamo/value_vectors_test.go`: Python must rewrite the
state document so that Go decodes the same value, and must read DynamoDB value
attributes with the same presence semantics.
"""

from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any

import pytest
from boto3.dynamodb.types import TypeSerializer

from limnopulse_api.adapters.alert_event_state import (
    reset_pending_evaluator_state,
    resolved_evaluator_state,
)
from limnopulse_api.adapters.alert_events import DynamoAlertEventRepository

FIXTURE_PATH = (
    Path(__file__).resolve().parents[2] / "testdata" / "alert_evaluation_value_vectors.json"
)
NOW = datetime(2026, 7, 15, 12, 5, tzinfo=UTC)
VALUE_KEYS = ("SchemaVersion", "LastValue", "LastQuality")


def _fixture() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _state_vectors() -> list[dict[str, Any]]:
    return _fixture()["state_vectors"]


def _dynamodb_vectors() -> list[dict[str, Any]]:
    return _fixture()["dynamodb_value_vectors"]


def _state_item(state_json: str) -> dict[str, Any]:
    return {
        "PK": "TENANT#tnt_1",
        "SK": "ALERT_STATE#rule_1",
        "entity_type": "alert_evaluation_state",
        "state_revision": 3,
        "state_json": state_json,
    }


def _assert_value_keys_preserved(before: dict[str, Any], after: dict[str, Any]) -> None:
    for key in VALUE_KEYS:
        assert (key in before) == (key in after), key
        if key in before:
            assert after[key] == before[key], key
            assert type(after[key]) is type(before[key]), key


def test_fixture_covers_the_issue_6_value_classes() -> None:
    fixture = _fixture()
    assert fixture["schema_version"] == 1
    names = {vector["name"] for vector in fixture["state_vectors"]}
    assert {
        "v2_present_zero_sufficient",
        "v2_present_zero_after_indeterminate",
        "v2_present_negative_retained",
        "v2_absent_null",
        "v2_absent_missing_key",
        "legacy_sufficient_zero",
        "legacy_placeholder_zero",
        "legacy_retained_nonzero",
        "legacy_negative_zero_placeholder",
        "legacy_null",
        "legacy_missing_key",
    } <= names
    errors = {vector["name"]: vector["expected_error"] for vector in fixture["state_error_vectors"]}
    assert errors["unknown_future_version"] == "unsupported_schema"
    assert set(errors.values()) == {"unsupported_schema", "malformed"}
    attributes = {
        vector["name"]: vector["attribute"] for vector in fixture["dynamodb_value_vectors"]
    }
    assert attributes["present_zero"] == {"N": "0"}
    assert attributes["absent"] is None


@pytest.mark.parametrize("vector", _state_vectors(), ids=lambda vector: vector["name"])
def test_resolution_rewrite_matches_the_document_go_decodes(vector: dict[str, Any]) -> None:
    resolved = resolved_evaluator_state(_state_item(vector["stored_state_json"]), "alert_1", NOW)

    assert resolved is not None
    updated, revision = resolved
    assert revision == 3
    assert updated["state_revision"] == 4
    assert updated["state_json"] == vector["python_resolved_state_json"]
    _assert_value_keys_preserved(
        json.loads(vector["stored_state_json"]), json.loads(updated["state_json"])
    )


@pytest.mark.parametrize("vector", _state_vectors(), ids=lambda vector: vector["name"])
def test_pending_reset_preserves_the_value_keys(vector: dict[str, Any]) -> None:
    pending = json.loads(vector["stored_state_json"])
    pending["Mode"] = "pending"
    reset = reset_pending_evaluator_state(_state_item(json.dumps(pending)), NOW)

    assert reset is not None
    updated, _ = reset
    assert updated["state_json"] == vector["python_pending_reset_state_json"]
    after = json.loads(updated["state_json"])
    assert after["Mode"] == "healthy"
    _assert_value_keys_preserved(pending, after)


class _RawItemClient:
    """Returns one stored DynamoDB item exactly as the Go writer encoded it."""

    def __init__(self, item: dict[str, Any]) -> None:
        self.item = item

    def get_item(self, **_: Any) -> dict[str, Any]:
        return {"Item": self.item}


def _raw_event_item(attribute: dict[str, str] | None) -> dict[str, Any]:
    timestamp = "2026-07-15T12:00:00.000000000Z"
    serializer = TypeSerializer()
    fields: dict[str, Any] = {
        "PK": "TENANT#tnt_1",
        "SK": "ALERT_EVENT#alert_1",
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
        "threshold": 5,
        "aggregation": "min",
        "severity": "critical",
        "status": "open",
        "opened_at": timestamp,
        "confirmed_open_window_end": timestamp,
        "window_start": timestamp,
        "window_end": timestamp,
        "last_evaluated_at": timestamp,
        "last_evaluation_quality": "sufficient",
        "created_at": timestamp,
        "updated_at": timestamp,
        "version": 1,
        "schema_version": 1,
    }
    item = {key: serializer.serialize(value) for key, value in fields.items()}
    if attribute is not None:
        item["last_evaluation_value"] = attribute
    return item


@pytest.mark.asyncio
@pytest.mark.parametrize("vector", _dynamodb_vectors(), ids=lambda vector: vector["name"])
async def test_alert_event_reads_the_value_attribute_with_go_presence(
    vector: dict[str, Any],
) -> None:
    client = _RawItemClient(_raw_event_item(vector["attribute"]))
    repository = DynamoAlertEventRepository(
        "LimnopulseDomain", "LimnopulseAudit", client, clock=lambda: NOW
    )

    event = await repository.get_event("tnt_1", "alert_1")

    assert event is not None
    if vector["value"]["present"]:
        assert event.last_evaluation_value is not None
        assert event.last_evaluation_value == vector["value"]["value"]
    else:
        assert event.last_evaluation_value is None
