import json
import re
from copy import deepcopy
from datetime import UTC, datetime
from hashlib import sha256
from threading import get_ident
from typing import Any

import pytest
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from limnopulse_api.adapters.notification_preferences import (
    DynamoNotificationPreferenceRepository,
)
from limnopulse_api.core.errors import ConflictError
from limnopulse_api.domain.alerts import AlertSeverity, AuditContext
from limnopulse_api.domain.notification_preferences import (
    EmailDeliverability,
    NotificationPreference,
)

NOW = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)


def state_hash(preference: NotificationPreference | None) -> str:
    value = None if preference is None else preference.model_dump(mode="json")
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode()
    return sha256(encoded).hexdigest()


class TransactionFailure(Exception):
    def __init__(self) -> None:
        self.response = {"Error": {"Code": "TransactionCanceledException"}}
        super().__init__("transaction cancelled")


class RecordingDynamoClient:
    def __init__(self) -> None:
        self.serializer = TypeSerializer()
        self.deserializer = TypeDeserializer()
        self.items: dict[tuple[str, str, str], dict[str, Any]] = {}
        self.get_item_calls: list[dict[str, Any]] = []
        self.transact_write_items_calls: list[dict[str, Any]] = []
        self.get_item_thread_ids: list[int] = []
        self.transact_write_thread_ids: list[int] = []

    def seed(self, table_name: str, item: dict[str, Any]) -> None:
        self.items[(table_name, item["PK"], item["SK"])] = deepcopy(item)

    def get_item(self, **kwargs: Any) -> dict[str, Any]:
        self.get_item_thread_ids.append(get_ident())
        self.get_item_calls.append(kwargs)
        key = self._decode(kwargs["Key"])
        item = self.items.get((kwargs["TableName"], key["PK"], key["SK"]))
        if item is None:
            return {}
        return {"Item": self._encode(item)}

    def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
        self.transact_write_thread_ids.append(get_ident())
        self.transact_write_items_calls.append(kwargs)
        candidate = deepcopy(self.items)
        for operation in kwargs["TransactItems"]:
            if "Put" in operation:
                put = operation["Put"]
                item = self._decode(put["Item"])
                key = (put["TableName"], item["PK"], item["SK"])
                existing = candidate.get(key)
                condition = put.get("ConditionExpression", "")
                if "attribute_not_exists" in condition and existing is not None:
                    raise TransactionFailure()
                if ":expected_version" in put.get("ExpressionAttributeValues", {}):
                    values = self._decode(put["ExpressionAttributeValues"])
                    if existing is None or existing.get("version") != values[":expected_version"]:
                        raise TransactionFailure()
                candidate[key] = item
                continue
            if "ConditionCheck" in operation:
                check = operation["ConditionCheck"]
                key = self._decode(check["Key"])
                existing = candidate.get((check["TableName"], key["PK"], key["SK"]))
                if (
                    "attribute_not_exists" in check.get("ConditionExpression", "")
                    and existing is not None
                ):
                    raise TransactionFailure()
                continue
            if "Update" in operation:
                update = operation["Update"]
                key = self._decode(update["Key"])
                item_key = (update["TableName"], key["PK"], key["SK"])
                item = candidate.setdefault(item_key, dict(key))
                names = update["ExpressionAttributeNames"]
                values = self._decode(update["ExpressionAttributeValues"])
                for attribute_name, value_name in re.findall(
                    r"(#[a-z_]+) = :([a-z_]+)",
                    update["UpdateExpression"],
                ):
                    item[names[attribute_name]] = values[f":{value_name}"]
                for attribute_name, value_name in re.findall(
                    r"(#[a-z_]+) = if_not_exists\(#[a-z_]+, :([a-z_]+)\)",
                    update["UpdateExpression"],
                ):
                    item.setdefault(names[attribute_name], values[f":{value_name}"])
                continue
            raise AssertionError(f"unsupported transaction operation: {operation!r}")
        self.items = candidate
        return {}

    def _encode(self, item: dict[str, Any]) -> dict[str, Any]:
        return {key: self.serializer.serialize(value) for key, value in item.items()}

    def _decode(self, item: dict[str, Any]) -> dict[str, Any]:
        return {key: self.deserializer.deserialize(value) for key, value in item.items()}


def make_preference(**updates: object) -> NotificationPreference:
    values: dict[str, object] = {
        "tenant_id": "tnt_1",
        "cognito_sub": "sub_1",
        "version": 1,
        "email_enabled": True,
        "email_address": "verified@example.com",
        "email_verified": True,
        "checked_at": NOW,
        "identity_source": "cognito_get_user",
        "minimum_severity": AlertSeverity.CRITICAL,
        "created_at": NOW,
        "updated_at": NOW,
    }
    values.update(updates)
    return NotificationPreference.model_validate(values)


def preference_item(**updates: object) -> dict[str, Any]:
    preference = make_preference(**updates)
    return {
        "PK": "TENANT#tnt_1",
        "SK": "NOTIFICATION_PREFERENCE#USER#sub_1",
        "entity_type": "notification_preference",
        **preference.model_dump(mode="json"),
        "schema_version": 1,
    }


@pytest.mark.asyncio
async def test_get_reads_exact_tenant_user_key_and_deserializes_preference() -> None:
    client = RecordingDynamoClient()
    client.seed("domain", preference_item())
    repository = DynamoNotificationPreferenceRepository("domain", "audit", client)
    event_loop_thread = get_ident()

    preference = await repository.get("tnt_1", "sub_1")

    assert preference == make_preference()
    key = client._decode(client.get_item_calls[0]["Key"])
    assert key == {
        "PK": "TENANT#tnt_1",
        "SK": "NOTIFICATION_PREFERENCE#USER#sub_1",
    }
    assert client.get_item_calls[0]["ConsistentRead"] is True
    assert client.get_item_thread_ids[0] != event_loop_thread
    assert client.transact_write_items_calls == []


@pytest.mark.asyncio
async def test_get_keeps_v1_email_only_row_compatible_with_telegram_disabled() -> None:
    client = RecordingDynamoClient()
    legacy = preference_item(email_enabled=False)
    legacy.pop("telegram_enabled", None)
    legacy["schema_version"] = 1
    client.seed("domain", legacy)
    repository = DynamoNotificationPreferenceRepository("domain", "audit", client)

    preference = await repository.get("tnt_1", "sub_1")

    assert preference is not None
    assert preference.telegram_enabled is False


@pytest.mark.asyncio
async def test_save_telegram_only_v2_row_omits_absent_email_snapshot() -> None:
    client = RecordingDynamoClient()
    repository = DynamoNotificationPreferenceRepository(
        "domain",
        "audit",
        client,
        clock=lambda: NOW,
    )
    preference = NotificationPreference(
        tenant_id="tnt_1",
        cognito_sub="sub_1",
        version=1,
        email_enabled=False,
        telegram_enabled=True,
        minimum_severity=AlertSeverity.WARNING,
        created_at=NOW,
        updated_at=NOW,
    )

    await repository.save(
        preference,
        None,
        AuditContext(actor_id="sub_1"),
        previous=None,
    )

    transaction = client.transact_write_items_calls[0]["TransactItems"]
    stored = client._decode(transaction[0]["Put"]["Item"])
    audit = client._decode(transaction[1]["Put"]["Item"])
    assert stored["schema_version"] == 2
    assert stored["telegram_enabled"] is True
    assert "email_address" not in stored
    assert "checked_at" not in stored
    assert audit["details"]["telegram_enabled"] is True
    assert audit["details"]["email_hash"] is None


@pytest.mark.asyncio
async def test_deliverability_lookup_uses_email_hash_and_absence_is_none() -> None:
    client = RecordingDynamoClient()
    repository = DynamoNotificationPreferenceRepository("domain", "audit", client)

    assert await repository.get_email_deliverability("verified@example.com") is None
    lookup_key = client._decode(client.get_item_calls[-1]["Key"])
    assert lookup_key == {
        "PK": f"EMAIL_IDENTITY#{sha256(b'verified@example.com').hexdigest()}",
        "SK": "DELIVERABILITY",
    }


@pytest.mark.asyncio
async def test_deliverability_lookup_returns_suppression_state_and_reason() -> None:
    client = RecordingDynamoClient()
    digest = sha256(b"verified@example.com").hexdigest()
    client.seed(
        "domain",
        {
            "PK": f"EMAIL_IDENTITY#{digest}",
            "SK": "DELIVERABILITY",
            "deliverability": "suppressed",
            "suppression_reason": "bounce",
        },
    )
    repository = DynamoNotificationPreferenceRepository("domain", "audit", client)

    record = await repository.get_email_deliverability("verified@example.com")

    assert record is not None
    assert record.deliverability is EmailDeliverability.SUPPRESSED
    assert record.suppression_reason == "bounce"


@pytest.mark.asyncio
async def test_deliverability_lookup_reads_go_feedback_suppression_row() -> None:
    client = RecordingDynamoClient()
    digest = sha256(b"verified@example.com").hexdigest()
    client.seed(
        "domain",
        {
            "PK": f"EMAIL_IDENTITY#{digest}",
            "SK": "DELIVERABILITY",
            "entity_type": "email_deliverability",
            "schema_version": 1,
            "deliverability": "suppressed",
            "suppression_reason": "hard_bounce",
            "suppression_rank": 2,
            "source_delivery_id": "delivery-1",
            "source_attempt_id": "attempt-1",
            "source_provider_message_id": "provider-message-1",
            "suppressed_at": NOW.isoformat(),
            "updated_at": NOW.isoformat(),
        },
    )
    repository = DynamoNotificationPreferenceRepository("domain", "audit", client)

    record = await repository.get_email_deliverability("verified@example.com")

    assert record is not None
    assert record.deliverability is EmailDeliverability.SUPPRESSED
    assert record.suppression_reason == "hard_bounce"


@pytest.mark.asyncio
async def test_deliverability_lookup_canonicalizes_email_casing() -> None:
    client = RecordingDynamoClient()
    digest = sha256(b"verified@example.com").hexdigest()
    client.seed(
        "domain",
        {
            "PK": f"EMAIL_IDENTITY#{digest}",
            "SK": "DELIVERABILITY",
            "deliverability": "suppressed",
            "suppression_reason": "hard_bounce",
        },
    )
    repository = DynamoNotificationPreferenceRepository("domain", "audit", client)

    record = await repository.get_email_deliverability("Verified@Example.COM")

    assert record is not None
    assert record.deliverability is EmailDeliverability.SUPPRESSED
    assert record.suppression_reason == "hard_bounce"


@pytest.mark.asyncio
async def test_deliverability_lookup_reads_legacy_cased_hash_during_rollout() -> None:
    client = RecordingDynamoClient()
    digest = sha256(b"Verified@Example.COM").hexdigest()
    client.seed(
        "domain",
        {
            "PK": f"EMAIL_IDENTITY#{digest}",
            "SK": "DELIVERABILITY",
            "deliverability": "suppressed",
            "suppression_reason": "hard_bounce",
        },
    )
    repository = DynamoNotificationPreferenceRepository("domain", "audit", client)

    record = await repository.get_email_deliverability("Verified@Example.COM")

    assert record is not None
    assert record.deliverability is EmailDeliverability.SUPPRESSED
    assert record.suppression_reason == "hard_bounce"


@pytest.mark.asyncio
async def test_case_only_email_update_migrates_legacy_suppression_atomically() -> None:
    client = RecordingDynamoClient()
    previous = make_preference(version=1, email_address="User@example.com")
    updated = make_preference(version=2, email_address="user@example.com")
    client.seed("domain", preference_item(version=1, email_address="User@example.com"))
    legacy_digest = sha256(b"User@example.com").hexdigest()
    client.seed(
        "domain",
        {
            "PK": f"EMAIL_IDENTITY#{legacy_digest}",
            "SK": "DELIVERABILITY",
            "entity_type": "email_deliverability",
            "schema_version": 1,
            "deliverability": "suppressed",
            "suppression_reason": "hard_bounce",
            "suppression_rank": 2,
            "source_delivery_id": "delivery-1",
            "source_attempt_id": "attempt-1",
            "source_provider_message_id": "provider-message-1",
            "suppressed_at": NOW.isoformat(),
            "updated_at": NOW.isoformat(),
        },
    )
    repository = DynamoNotificationPreferenceRepository(
        "domain",
        "audit",
        client,
        clock=lambda: NOW,
    )

    await repository.save(
        updated,
        1,
        AuditContext(actor_id="sub_1"),
        previous=previous,
    )

    transaction = client.transact_write_items_calls[0]["TransactItems"]
    migration = transaction[2]["Update"]
    migration_key = client._decode(migration["Key"])
    assert migration_key == {
        "PK": f"EMAIL_IDENTITY#{sha256(b'user@example.com').hexdigest()}",
        "SK": "DELIVERABILITY",
    }
    migration_values = client._decode(migration["ExpressionAttributeValues"])
    assert migration_values[":suppressed"] == "suppressed"
    assert migration_values[":suppression_reason"] == "hard_bounce"
    assert migration_values[":suppression_rank"] == 2
    assert "if_not_exists" in migration["UpdateExpression"]
    assert client.get_item_calls[0]["ConsistentRead"] is True


@pytest.mark.asyncio
async def test_address_change_preserves_departed_legacy_suppression_across_return() -> None:
    client = RecordingDynamoClient()
    original = make_preference(version=1, email_address="User@example.com")
    different = make_preference(version=2, email_address="other@example.com")
    returned = make_preference(version=3, email_address="user@example.com")
    client.seed("domain", preference_item(version=1, email_address="User@example.com"))
    client.seed(
        "domain",
        {
            "PK": f"EMAIL_IDENTITY#{sha256(b'User@example.com').hexdigest()}",
            "SK": "DELIVERABILITY",
            "deliverability": "suppressed",
            "suppression_reason": "hard_bounce",
        },
    )
    repository = DynamoNotificationPreferenceRepository(
        "domain",
        "audit",
        client,
        clock=lambda: NOW,
    )

    await repository.save(
        different,
        1,
        AuditContext(actor_id="sub_1"),
        previous=original,
    )

    first_migration = client.transact_write_items_calls[0]["TransactItems"][2]["Update"]
    assert client._decode(first_migration["Key"]) == {
        "PK": f"EMAIL_IDENTITY#{sha256(b'user@example.com').hexdigest()}",
        "SK": "DELIVERABILITY",
    }

    await repository.save(
        returned,
        2,
        AuditContext(actor_id="sub_1"),
        previous=different,
    )

    deliverability = await repository.get_email_deliverability("user@example.com")

    assert deliverability is not None
    assert deliverability.deliverability is EmailDeliverability.SUPPRESSED
    assert deliverability.suppression_reason == "hard_bounce"


@pytest.mark.asyncio
async def test_address_change_fences_observed_legacy_deliverability_state() -> None:
    client = RecordingDynamoClient()
    previous = make_preference(version=1, email_address="User@example.com")
    updated = make_preference(version=2, email_address="other@example.com")
    client.seed("domain", preference_item(version=1, email_address="User@example.com"))
    client.seed(
        "domain",
        {
            "PK": f"EMAIL_IDENTITY#{sha256(b'User@example.com').hexdigest()}",
            "SK": "DELIVERABILITY",
            "deliverability": "deliverable",
        },
    )
    repository = DynamoNotificationPreferenceRepository(
        "domain",
        "audit",
        client,
        clock=lambda: NOW,
    )

    await repository.save(
        updated,
        1,
        AuditContext(actor_id="sub_1"),
        previous=previous,
    )

    fence = client.transact_write_items_calls[0]["TransactItems"][2]["ConditionCheck"]
    assert client._decode(fence["Key"]) == {
        "PK": f"EMAIL_IDENTITY#{sha256(b'User@example.com').hexdigest()}",
        "SK": "DELIVERABILITY",
    }
    assert fence["ConditionExpression"] == "#deliverability = :deliverability"
    assert client._decode(fence["ExpressionAttributeValues"])[":deliverability"] == "deliverable"


@pytest.mark.asyncio
async def test_case_only_email_update_fences_absent_legacy_suppression() -> None:
    client = RecordingDynamoClient()
    previous = make_preference(version=1, email_address="User@example.com")
    updated = make_preference(version=2, email_address="user@example.com")
    client.seed("domain", preference_item(version=1, email_address="User@example.com"))
    repository = DynamoNotificationPreferenceRepository(
        "domain",
        "audit",
        client,
        clock=lambda: NOW,
    )

    await repository.save(
        updated,
        1,
        AuditContext(actor_id="sub_1"),
        previous=previous,
    )

    transaction = client.transact_write_items_calls[0]["TransactItems"]
    fence = transaction[2]["ConditionCheck"]
    legacy_key = client._decode(fence["Key"])
    assert legacy_key == {
        "PK": f"EMAIL_IDENTITY#{sha256(b'User@example.com').hexdigest()}",
        "SK": "DELIVERABILITY",
    }
    assert fence["ConditionExpression"] == "attribute_not_exists(PK) AND attribute_not_exists(SK)"


@pytest.mark.asyncio
async def test_create_conditionally_persists_preference_and_redacted_audit_atomically() -> None:
    client = RecordingDynamoClient()
    repository = DynamoNotificationPreferenceRepository(
        "domain",
        "audit",
        client,
        clock=lambda: NOW,
    )
    preference = make_preference()
    event_loop_thread = get_ident()

    saved = await repository.save(
        preference,
        None,
        AuditContext(actor_id="sub_1", ip="127.0.0.1", user_agent="tests"),
        previous=None,
    )

    assert saved == preference
    assert client.transact_write_thread_ids[0] != event_loop_thread
    transaction = client.transact_write_items_calls[0]["TransactItems"]
    assert len(transaction) == 2
    preference_put = transaction[0]["Put"]
    persisted = client._decode(preference_put["Item"])
    assert persisted["PK"] == "TENANT#tnt_1"
    assert persisted["SK"] == "NOTIFICATION_PREFERENCE#USER#sub_1"
    assert persisted["email_address"] == "verified@example.com"
    assert persisted["email_verified"] is True
    assert persisted["identity_source"] == "cognito_get_user"
    assert "attribute_not_exists(PK)" in preference_put["ConditionExpression"]

    audit = client._decode(transaction[1]["Put"]["Item"])
    assert audit["action"] == "notification_preference.created"
    assert audit["details"] == {
        "email_enabled": True,
        "email_verified": True,
        "telegram_enabled": False,
        "minimum_severity": "critical",
        "email_hash": sha256(b"verified@example.com").hexdigest(),
    }
    assert audit["before_hash"] == state_hash(None)
    assert audit["after_hash"] == state_hash(preference)
    assert "verified@example.com" not in json.dumps(audit, default=str)
    assert "access-token" not in json.dumps(transaction, default=str)


@pytest.mark.asyncio
async def test_update_conditions_on_expected_version_and_maps_stale_write_to_conflict() -> None:
    client = RecordingDynamoClient()
    client.seed("domain", preference_item(version=2))
    repository = DynamoNotificationPreferenceRepository(
        "domain",
        "audit",
        client,
        clock=lambda: NOW,
    )

    with pytest.raises(ConflictError):
        await repository.save(
            make_preference(version=2, email_enabled=False),
            1,
            AuditContext(actor_id="sub_1"),
            previous=make_preference(version=1),
        )

    assert len(client.items) == 1


@pytest.mark.asyncio
async def test_update_audit_hashes_the_exact_previous_and_next_states() -> None:
    client = RecordingDynamoClient()
    previous = make_preference(version=1, email_enabled=True)
    updated = make_preference(version=2, email_enabled=False)
    client.seed("domain", preference_item(version=1, email_enabled=True))
    repository = DynamoNotificationPreferenceRepository(
        "domain",
        "audit",
        client,
        clock=lambda: NOW,
    )

    await repository.save(
        updated,
        1,
        AuditContext(actor_id="sub_1"),
        previous=previous,
    )

    transaction = client.transact_write_items_calls[0]["TransactItems"]
    audit = client._decode(transaction[1]["Put"]["Item"])
    assert audit["action"] == "notification_preference.updated"
    assert audit["before_hash"] == state_hash(previous)
    assert audit["after_hash"] == state_hash(updated)
    assert "verified@example.com" not in json.dumps(audit, default=str)
