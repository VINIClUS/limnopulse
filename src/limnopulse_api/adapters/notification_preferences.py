import json
from asyncio import to_thread
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from typing import Any
from uuid import uuid4

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from limnopulse_api.core.errors import ConflictError
from limnopulse_api.domain.alerts import AuditContext
from limnopulse_api.domain.notification_preferences import (
    EmailDeliverability,
    EmailDeliverabilityRecord,
    NotificationPreference,
)
from limnopulse_api.domain.telegram import TelegramEligibilityFence

AUDIT_RETENTION = timedelta(days=90)


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


class DynamoNotificationPreferenceRepository:
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

    async def get(
        self,
        tenant_id: str,
        cognito_sub: str,
    ) -> NotificationPreference | None:
        item = await to_thread(
            self._get_item,
            self.domain_table_name,
            self._preference_key(tenant_id, cognito_sub),
            consistent=True,
        )
        if item is None:
            return None
        values = {
            key: value
            for key, value in item.items()
            if key not in {"PK", "SK", "entity_type", "schema_version"}
        }
        return NotificationPreference.model_validate(values)

    async def get_email_deliverability(
        self,
        address: str,
    ) -> EmailDeliverabilityRecord | None:
        item = await to_thread(
            self._get_item,
            self.domain_table_name,
            self._deliverability_key(address),
        )
        if item is None and address != address.lower():
            item = await to_thread(
                self._get_item,
                self.domain_table_name,
                self._legacy_deliverability_key(address),
            )
        if item is None:
            return None
        return EmailDeliverabilityRecord(
            deliverability=EmailDeliverability(item.get("deliverability", "unknown")),
            suppression_reason=item.get("suppression_reason"),
        )

    async def save(
        self,
        preference: NotificationPreference,
        expected_version: int | None,
        audit: AuditContext,
        *,
        previous: NotificationPreference | None,
        telegram_fence: TelegramEligibilityFence | None = None,
    ) -> NotificationPreference:
        self._validate_change(preference, expected_version, previous)
        if preference.telegram_enabled and telegram_fence is None:
            raise ValueError("enabled Telegram preference requires an eligibility fence")
        now = self.clock()
        legacy_suppression_migration = await self._legacy_suppression_migration(
            preference,
            previous,
        )
        preference_put: dict[str, Any] = {
            "TableName": self.domain_table_name,
            "Item": self._serialize_item(self._preference_item(preference)),
        }
        if expected_version is None:
            preference_put["ConditionExpression"] = (
                "attribute_not_exists(PK) AND attribute_not_exists(SK)"
            )
            action = "notification_preference.created"
        else:
            preference_put.update(
                {
                    "ConditionExpression": (
                        "attribute_exists(PK) AND attribute_exists(SK) "
                        "AND #version = :expected_version"
                    ),
                    "ExpressionAttributeNames": {"#version": "version"},
                    "ExpressionAttributeValues": self._serialize_values(
                        {":expected_version": expected_version}
                    ),
                }
            )
            action = "notification_preference.updated"

        audit_item = self._audit_item(preference, previous, audit, action, now)
        transaction_items: list[dict[str, Any]] = [
            {"Put": preference_put},
            {
                "Put": {
                    "TableName": self.audit_table_name,
                    "Item": self._serialize_item(audit_item),
                    "ConditionExpression": (
                        "attribute_not_exists(PK) AND attribute_not_exists(SK)"
                    ),
                }
            },
        ]
        if legacy_suppression_migration is not None:
            transaction_items.append(legacy_suppression_migration)
        if telegram_fence is not None:
            transaction_items.extend(
                self._telegram_eligibility_conditions(preference, telegram_fence)
            )
        try:
            await to_thread(
                self.client.transact_write_items,
                TransactItems=transaction_items,
            )
        except Exception as exc:
            self._raise_if_conflict(exc)
            raise
        return preference

    def _telegram_eligibility_conditions(
        self,
        preference: NotificationPreference,
        fence: TelegramEligibilityFence,
    ) -> list[dict[str, Any]]:
        if fence.tenant_id != preference.tenant_id or fence.recipient_id != preference.cognito_sub:
            raise ValueError("Telegram eligibility fence identity does not match preference")
        binding_values = {
            ":entity": "telegram_binding",
            ":tenant_id": fence.tenant_id,
            ":recipient_id": fence.recipient_id,
            ":destination_id": fence.destination_id,
            ":verified": "verified",
            ":binding_version": fence.binding_version,
        }
        destination_values = {
            ":entity": "telegram_destination",
            ":recipient_id": fence.recipient_id,
            ":destination_id": fence.destination_id,
            ":chat_id": fence.chat_id,
            ":active": "active",
            ":destination_version": fence.destination_version,
        }
        return [
            {
                "ConditionCheck": {
                    "TableName": self.domain_table_name,
                    "Key": self._serialize_item(
                        {
                            "PK": f"TENANT#{fence.tenant_id}",
                            "SK": f"TELEGRAM_BINDING#USER#{fence.recipient_id}",
                        }
                    ),
                    "ConditionExpression": (
                        "#entity = :entity AND #tenant_id = :tenant_id AND "
                        "#recipient_id = :recipient_id AND #destination_id = :destination_id "
                        "AND #status = :verified AND #version = :binding_version"
                    ),
                    "ExpressionAttributeNames": {
                        "#entity": "entity_type",
                        "#tenant_id": "tenant_id",
                        "#recipient_id": "recipient_id",
                        "#destination_id": "destination_id",
                        "#status": "status",
                        "#version": "version",
                    },
                    "ExpressionAttributeValues": self._serialize_values(binding_values),
                }
            },
            {
                "ConditionCheck": {
                    "TableName": self.domain_table_name,
                    "Key": self._serialize_item(
                        {
                            "PK": f"TELEGRAM_DESTINATION#{fence.destination_id}",
                            "SK": "META",
                        }
                    ),
                    "ConditionExpression": (
                        "#entity = :entity AND #recipient_id = :recipient_id AND "
                        "#destination_id = :destination_id AND #chat_id = :chat_id "
                        "AND #status = :active AND #version = :destination_version"
                    ),
                    "ExpressionAttributeNames": {
                        "#entity": "entity_type",
                        "#recipient_id": "recipient_id",
                        "#destination_id": "destination_id",
                        "#chat_id": "chat_id",
                        "#status": "status",
                        "#version": "version",
                    },
                    "ExpressionAttributeValues": self._serialize_values(destination_values),
                }
            },
        ]

    async def _legacy_suppression_migration(
        self,
        preference: NotificationPreference,
        previous: NotificationPreference | None,
    ) -> dict[str, Any] | None:
        if (
            previous is None
            or previous.email_address is None
            or preference.email_address is None
            or previous.email_address == preference.email_address
        ):
            return None
        canonical_key = self._deliverability_key(previous.email_address)
        legacy_key = self._legacy_deliverability_key(previous.email_address)
        if canonical_key == legacy_key:
            return None
        legacy = await to_thread(
            self._get_item,
            self.domain_table_name,
            legacy_key,
            consistent=True,
        )
        if legacy is None:
            return {
                "ConditionCheck": {
                    "TableName": self.domain_table_name,
                    "Key": self._serialize_item(legacy_key),
                    "ConditionExpression": "attribute_not_exists(PK) AND attribute_not_exists(SK)",
                }
            }
        if legacy.get("deliverability") != EmailDeliverability.SUPPRESSED.value:
            return {
                "ConditionCheck": {
                    "TableName": self.domain_table_name,
                    "Key": self._serialize_item(legacy_key),
                    "ConditionExpression": "#deliverability = :deliverability",
                    "ExpressionAttributeNames": {"#deliverability": "deliverability"},
                    "ExpressionAttributeValues": self._serialize_values(
                        {":deliverability": legacy.get("deliverability")}
                    ),
                }
            }

        values: dict[str, Any] = {
            ":entity_type": "email_deliverability",
            ":schema_version": 1,
            ":suppressed": EmailDeliverability.SUPPRESSED.value,
        }
        names = {
            "#entity_type": "entity_type",
            "#schema_version": "schema_version",
            "#deliverability": "deliverability",
        }
        sets = [
            "#entity_type = if_not_exists(#entity_type, :entity_type)",
            "#schema_version = if_not_exists(#schema_version, :schema_version)",
            "#deliverability = :suppressed",
        ]
        for attribute in (
            "suppression_reason",
            "suppression_rank",
            "source_delivery_id",
            "source_attempt_id",
            "source_provider_message_id",
            "suppressed_at",
            "updated_at",
        ):
            value = legacy.get(attribute)
            if value is None:
                continue
            name = f"#{attribute}"
            placeholder = f":{attribute}"
            names[name] = attribute
            values[placeholder] = value
            sets.append(f"{name} = if_not_exists({name}, {placeholder})")
        return {
            "Update": {
                "TableName": self.domain_table_name,
                "Key": self._serialize_item(canonical_key),
                "UpdateExpression": f"SET {', '.join(sets)}",
                "ExpressionAttributeNames": names,
                "ExpressionAttributeValues": self._serialize_values(values),
            }
        }

    def _preference_item(self, preference: NotificationPreference) -> dict[str, Any]:
        return {
            **self._preference_key(preference.tenant_id, preference.cognito_sub),
            "entity_type": "notification_preference",
            **preference.model_dump(mode="json", exclude_none=True),
            "schema_version": 2,
        }

    def _audit_item(
        self,
        preference: NotificationPreference,
        previous: NotificationPreference | None,
        context: AuditContext,
        action: str,
        now: datetime,
    ) -> dict[str, Any]:
        event_id = f"audit_{uuid4().hex}"
        return {
            "PK": f"TENANT#{preference.tenant_id}#MONTH#{now:%Y-%m}",
            "SK": f"{now.isoformat()}#{event_id}",
            "entity_type": "audit_event",
            "event_id": event_id,
            "tenant_id": preference.tenant_id,
            "actor_type": "user",
            "actor_id": context.actor_id,
            "action": action,
            "resource_type": "notification_preference",
            "resource_id": f"USER#{preference.cognito_sub}",
            "before_hash": self._hash_state(previous),
            "after_hash": self._hash_state(preference),
            "details": {
                "email_enabled": preference.email_enabled,
                "email_verified": preference.email_verified,
                "telegram_enabled": preference.telegram_enabled,
                "minimum_severity": preference.minimum_severity.value,
                "email_hash": (
                    self._email_hash(preference.email_address)
                    if preference.email_address is not None
                    else None
                ),
            },
            "ip": context.ip,
            "user_agent": context.user_agent,
            "created_at": now.isoformat(),
            "expires_at": int((now + AUDIT_RETENTION).timestamp()),
        }

    def _validate_change(
        self,
        preference: NotificationPreference,
        expected_version: int | None,
        previous: NotificationPreference | None,
    ) -> None:
        if expected_version is None:
            if previous is not None or preference.version != 1:
                raise ValueError("notification preference create state is invalid")
            return
        if (
            previous is None
            or previous.tenant_id != preference.tenant_id
            or previous.cognito_sub != preference.cognito_sub
            or previous.version != expected_version
            or preference.version != expected_version + 1
        ):
            raise ValueError("notification preference update state is invalid")

    def _hash_state(self, preference: NotificationPreference | None) -> str:
        value = None if preference is None else preference.model_dump(mode="json")
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode()
        return sha256(encoded).hexdigest()

    def _preference_key(self, tenant_id: str, cognito_sub: str) -> dict[str, str]:
        return {
            "PK": f"TENANT#{tenant_id}",
            "SK": f"NOTIFICATION_PREFERENCE#USER#{cognito_sub}",
        }

    def _deliverability_key(self, address: str) -> dict[str, str]:
        return {
            "PK": f"EMAIL_IDENTITY#{self._email_hash(address)}",
            "SK": "DELIVERABILITY",
        }

    def _email_hash(self, address: str) -> str:
        return sha256(address.lower().encode("ascii")).hexdigest()

    def _legacy_deliverability_key(self, address: str) -> dict[str, str]:
        return {
            "PK": f"EMAIL_IDENTITY#{sha256(address.encode('ascii')).hexdigest()}",
            "SK": "DELIVERABILITY",
        }

    def _get_item(
        self,
        table_name: str,
        key: Mapping[str, str],
        *,
        consistent: bool = False,
    ) -> dict[str, Any] | None:
        response = self.client.get_item(
            TableName=table_name,
            Key=self._serialize_item(key),
            ConsistentRead=consistent,
        )
        item = response.get("Item")
        if item is None:
            return None
        return self._deserialize_item(item)

    def _raise_if_conflict(self, exc: Exception) -> None:
        response = getattr(exc, "response", {})
        error_code = response.get("Error", {}).get("Code")
        if error_code == "ConditionalCheckFailedException":
            raise ConflictError(str(exc)) from exc
        if error_code != "TransactionCanceledException":
            return
        reasons = response.get("CancellationReasons")
        if reasons is None or any(
            reason.get("Code") in {"ConditionalCheckFailed", "TransactionConflict"}
            for reason in reasons
        ):
            raise ConflictError(str(exc)) from exc

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
