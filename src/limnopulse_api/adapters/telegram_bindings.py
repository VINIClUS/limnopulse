from asyncio import to_thread
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from typing import Any

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from limnopulse_api.domain.telegram import (
    TelegramBinding,
    TelegramBindingRequest,
    TelegramBindingStatus,
    TelegramDestination,
    TelegramDestinationStatus,
)

UPDATE_DEDUPE_TTL = timedelta(days=8)


class DynamoTelegramBindingRepository:
    def __init__(self, table_name: str, client: Any) -> None:
        self.table_name = table_name
        self.client = client
        self._serializer = TypeSerializer()
        self._deserializer = TypeDeserializer()

    async def issue(self, request: TelegramBindingRequest) -> None:
        previous = await self._get_current_request(
            request.tenant_id,
            request.recipient_id,
            consistent=True,
        )
        token_put: dict[str, Any] = {
            "TableName": self.table_name,
            "Item": self._serialize(self._request_item(request, token_lookup=True)),
            "ConditionExpression": "attribute_not_exists(PK) AND attribute_not_exists(SK)",
        }
        pointer_put: dict[str, Any] = {
            "TableName": self.table_name,
            "Item": self._serialize(self._request_item(request, token_lookup=False)),
        }
        if previous is None:
            pointer_put["ConditionExpression"] = (
                "attribute_not_exists(PK) AND attribute_not_exists(SK)"
            )
        else:
            pointer_put.update(
                {
                    "ConditionExpression": "#request_id = :previous_request_id",
                    "ExpressionAttributeNames": {"#request_id": "request_id"},
                    "ExpressionAttributeValues": self._serialize(
                        {":previous_request_id": previous.request_id}
                    ),
                }
            )
        operations: list[dict[str, Any]] = [{"Put": token_put}, {"Put": pointer_put}]
        if (
            previous is not None
            and previous.status is TelegramBindingStatus.PENDING
            and previous.expires_at > request.created_at
        ):
            operations.append(
                {
                    "Update": {
                        "TableName": self.table_name,
                        "Key": self._serialize(self._token_key(previous.token_hash)),
                        "UpdateExpression": "SET #status = :invalidated",
                        "ConditionExpression": (
                            "#request_id = :previous_request_id AND #status = :pending"
                        ),
                        "ExpressionAttributeNames": {
                            "#request_id": "request_id",
                            "#status": "status",
                        },
                        "ExpressionAttributeValues": self._serialize(
                            {
                                ":previous_request_id": previous.request_id,
                                ":pending": TelegramBindingStatus.PENDING.value,
                                ":invalidated": TelegramBindingStatus.INVALIDATED.value,
                            }
                        ),
                    }
                }
            )
        await to_thread(
            self.client.transact_write_items,
            TransactItems=operations,
            ClientRequestToken=self._client_token("tgissue", request.request_id),
        )

    async def get_request(self, token_hash: str) -> TelegramBindingRequest | None:
        item = await self._get_item(self._token_key(token_hash), consistent=True)
        return None if item is None else self._request_from_item(item)

    async def consume(
        self,
        *,
        token_hash: str,
        chat_id: int,
        sender_id: int,
        update_id: int,
        now: datetime,
        membership_active: bool,
    ) -> bool:
        if sender_id != chat_id or not membership_active:
            return False
        request = await self.get_request(token_hash)
        if (
            request is None
            or request.status is not TelegramBindingStatus.PENDING
            or request.expires_at <= now
        ):
            return False
        current_item = await self._get_item(
            self._binding_key(request.tenant_id, request.recipient_id),
            consistent=True,
        )
        current = None if current_item is None else self._binding_from_item(current_item)
        destination_id = TelegramDestination.id_for_chat(chat_id)
        binding = TelegramBinding(
            tenant_id=request.tenant_id,
            recipient_id=request.recipient_id,
            destination_id=destination_id,
            status=TelegramBindingStatus.VERIFIED,
            verified_at=now,
            revoked_at=None,
            version=1 if current is None else current.version + 1,
            created_at=now if current is None else current.created_at,
            updated_at=now,
        )
        binding_put: dict[str, Any] = {
            "TableName": self.table_name,
            "Item": self._serialize(self._binding_item(binding)),
        }
        if current is None:
            binding_put["ConditionExpression"] = (
                "attribute_not_exists(PK) AND attribute_not_exists(SK)"
            )
        else:
            binding_put.update(
                {
                    "ConditionExpression": "#version = :expected_version",
                    "ExpressionAttributeNames": {"#version": "version"},
                    "ExpressionAttributeValues": self._serialize(
                        {":expected_version": current.version}
                    ),
                }
            )
        operations: list[dict[str, Any]] = [
            {"Put": self._dedupe_put(update_id, now)},
            {
                "ConditionCheck": {
                    "TableName": self.table_name,
                    "Key": self._serialize(
                        {
                            "PK": f"USER#{request.recipient_id}",
                            "SK": f"TENANT#{request.tenant_id}",
                        }
                    ),
                    "ConditionExpression": (
                        "#status = :active AND #tenant_id = :tenant_id "
                        "AND #cognito_sub = :recipient_id"
                    ),
                    "ExpressionAttributeNames": {
                        "#status": "status",
                        "#tenant_id": "tenant_id",
                        "#cognito_sub": "cognito_sub",
                    },
                    "ExpressionAttributeValues": self._serialize(
                        {
                            ":active": "active",
                            ":tenant_id": request.tenant_id,
                            ":recipient_id": request.recipient_id,
                        }
                    ),
                }
            },
            {
                "Update": {
                    "TableName": self.table_name,
                    "Key": self._serialize(self._token_key(token_hash)),
                    "UpdateExpression": "SET #status = :consumed, #consumed_at = :consumed_at",
                    "ConditionExpression": (
                        "#status = :pending AND #request_id = :request_id "
                        "AND #expires_at > :now_epoch"
                    ),
                    "ExpressionAttributeNames": {
                        "#status": "status",
                        "#request_id": "request_id",
                        "#expires_at": "expires_at",
                        "#consumed_at": "consumed_at",
                    },
                    "ExpressionAttributeValues": self._serialize(
                        {
                            ":pending": TelegramBindingStatus.PENDING.value,
                            ":consumed": TelegramBindingStatus.CONSUMED.value,
                            ":request_id": request.request_id,
                            ":now_epoch": int(now.timestamp()),
                            ":consumed_at": now.isoformat(),
                        }
                    ),
                }
            },
            {
                "Update": {
                    "TableName": self.table_name,
                    "Key": self._serialize(
                        self._request_pointer_key(request.tenant_id, request.recipient_id)
                    ),
                    "UpdateExpression": "SET #status = :consumed, #consumed_at = :consumed_at",
                    "ConditionExpression": ("#status = :pending AND #request_id = :request_id"),
                    "ExpressionAttributeNames": {
                        "#status": "status",
                        "#request_id": "request_id",
                        "#consumed_at": "consumed_at",
                    },
                    "ExpressionAttributeValues": self._serialize(
                        {
                            ":pending": TelegramBindingStatus.PENDING.value,
                            ":consumed": TelegramBindingStatus.CONSUMED.value,
                            ":request_id": request.request_id,
                            ":consumed_at": now.isoformat(),
                        }
                    ),
                }
            },
            {
                "Update": {
                    "TableName": self.table_name,
                    "Key": self._serialize(self._destination_key(destination_id)),
                    "UpdateExpression": (
                        "SET #entity_type = :entity_type, #schema_version = :schema_version, "
                        "#destination_id = :destination_id, #recipient_id = :recipient_id, "
                        "#chat_id = :chat_id, #status = :active, "
                        "#last_update_id = :update_id, "
                        "#version = if_not_exists(#version, :zero) + :one, "
                        "#created_at = if_not_exists(#created_at, :now), #updated_at = :now "
                        "REMOVE #suppression_reason, #stopped_at"
                    ),
                    "ConditionExpression": (
                        "(attribute_not_exists(#recipient_id) OR "
                        "(#recipient_id = :recipient_id AND #chat_id = :chat_id)) "
                        "AND (attribute_not_exists(#last_update_id) "
                        "OR #last_update_id < :update_id)"
                    ),
                    "ExpressionAttributeNames": {
                        "#entity_type": "entity_type",
                        "#schema_version": "schema_version",
                        "#destination_id": "destination_id",
                        "#recipient_id": "recipient_id",
                        "#chat_id": "chat_id",
                        "#status": "status",
                        "#last_update_id": "last_update_id",
                        "#version": "version",
                        "#created_at": "created_at",
                        "#updated_at": "updated_at",
                        "#suppression_reason": "suppression_reason",
                        "#stopped_at": "stopped_at",
                    },
                    "ExpressionAttributeValues": self._serialize(
                        {
                            ":entity_type": "telegram_destination",
                            ":schema_version": 1,
                            ":destination_id": destination_id,
                            ":recipient_id": request.recipient_id,
                            ":chat_id": chat_id,
                            ":active": TelegramDestinationStatus.ACTIVE.value,
                            ":update_id": update_id,
                            ":zero": 0,
                            ":one": 1,
                            ":now": now.isoformat(),
                        }
                    ),
                }
            },
            {"Put": binding_put},
        ]
        if current is not None and current.destination_id != destination_id:
            history = {
                **self._binding_history_key(current, now),
                "entity_type": "telegram_binding_history",
                **current.model_dump(mode="json"),
                "status": TelegramBindingStatus.REVOKED.value,
                "revoked_at": now.isoformat(),
                "schema_version": 1,
            }
            operations.append(
                {
                    "Put": {
                        "TableName": self.table_name,
                        "Item": self._serialize(history),
                        "ConditionExpression": (
                            "attribute_not_exists(PK) AND attribute_not_exists(SK)"
                        ),
                    }
                }
            )
        try:
            await to_thread(
                self.client.transact_write_items,
                TransactItems=operations,
                ClientRequestToken=self._client_token(
                    "tgupdate",
                    (
                        f"consume:{update_id}:{request.request_id}:"
                        f"{0 if current is None else current.version}:{destination_id}"
                    ),
                ),
            )
        except Exception as exc:
            if self._conditional_failure_indexes(exc) is not None:
                return False
            raise
        return True

    async def stop(
        self,
        *,
        chat_id: int,
        sender_id: int,
        update_id: int,
        now: datetime,
    ) -> bool:
        if sender_id != chat_id:
            return False
        destination_id = TelegramDestination.id_for_chat(chat_id)
        last_conflict: Exception | None = None
        for _ in range(3):
            item = await self._get_item(self._destination_key(destination_id), consistent=True)
            operations: list[dict[str, Any]] = [{"Put": self._dedupe_put(update_id, now)}]
            applied = False
            shape = "absent"
            if item is None:
                operations.append(
                    {
                        "ConditionCheck": {
                            "TableName": self.table_name,
                            "Key": self._serialize(self._destination_key(destination_id)),
                            "ConditionExpression": (
                                "attribute_not_exists(PK) AND attribute_not_exists(SK)"
                            ),
                        }
                    }
                )
            else:
                destination = self._destination_from_item(item)
                if destination.chat_id != chat_id:
                    return False
                if destination.last_update_id >= update_id:
                    return False
                shape = f"{destination.status.value}:{destination.version}"
                applied = True
                if destination.status is TelegramDestinationStatus.SUPPRESSED:
                    operations.append(
                        {
                            "Update": {
                                "TableName": self.table_name,
                                "Key": self._serialize(self._destination_key(destination_id)),
                                "UpdateExpression": (
                                    "SET #last_update_id = :update_id, #updated_at = :now"
                                ),
                                "ConditionExpression": (
                                    "#status = :suppressed AND #version = :version "
                                    "AND (attribute_not_exists(#last_update_id) "
                                    "OR #last_update_id < :update_id)"
                                ),
                                "ExpressionAttributeNames": {
                                    "#status": "status",
                                    "#version": "version",
                                    "#last_update_id": "last_update_id",
                                    "#updated_at": "updated_at",
                                },
                                "ExpressionAttributeValues": self._serialize(
                                    {
                                        ":suppressed": (TelegramDestinationStatus.SUPPRESSED.value),
                                        ":version": destination.version,
                                        ":update_id": update_id,
                                        ":now": now.isoformat(),
                                    }
                                ),
                            }
                        }
                    )
                else:
                    operations.append(
                        {
                            "Update": {
                                "TableName": self.table_name,
                                "Key": self._serialize(self._destination_key(destination_id)),
                                "UpdateExpression": (
                                    "SET #status = :suppressed, "
                                    "#suppression_reason = :reason, #stopped_at = :now, "
                                    "#updated_at = :now, #last_update_id = :update_id, "
                                    "#version = #version + :one"
                                ),
                                "ConditionExpression": (
                                    "#recipient_id = :recipient_id AND #chat_id = :chat_id "
                                    "AND #status = :active AND #version = :version "
                                    "AND (attribute_not_exists(#last_update_id) "
                                    "OR #last_update_id < :update_id)"
                                ),
                                "ExpressionAttributeNames": {
                                    "#status": "status",
                                    "#suppression_reason": "suppression_reason",
                                    "#stopped_at": "stopped_at",
                                    "#updated_at": "updated_at",
                                    "#last_update_id": "last_update_id",
                                    "#version": "version",
                                    "#recipient_id": "recipient_id",
                                    "#chat_id": "chat_id",
                                },
                                "ExpressionAttributeValues": self._serialize(
                                    {
                                        ":active": TelegramDestinationStatus.ACTIVE.value,
                                        ":suppressed": (TelegramDestinationStatus.SUPPRESSED.value),
                                        ":reason": "user_stop",
                                        ":now": now.isoformat(),
                                        ":one": 1,
                                        ":recipient_id": destination.recipient_id,
                                        ":chat_id": chat_id,
                                        ":version": destination.version,
                                        ":update_id": update_id,
                                    }
                                ),
                            }
                        }
                    )
            try:
                await to_thread(
                    self.client.transact_write_items,
                    TransactItems=operations,
                    ClientRequestToken=self._client_token("tgupdate", f"stop:{update_id}:{shape}"),
                )
            except Exception as exc:
                conditional_indexes = self._conditional_failure_indexes(exc)
                if conditional_indexes is not None and (
                    -1 in conditional_indexes or 0 in conditional_indexes
                ):
                    return False
                if conditional_indexes is not None:
                    last_conflict = exc
                    continue
                raise
            return applied
        if last_conflict is not None:
            raise last_conflict
        return False

    async def get_current(
        self,
        tenant_id: str,
        recipient_id: str,
    ) -> tuple[TelegramBinding | None, TelegramBindingRequest | None, TelegramDestination | None]:
        binding_item = await self._get_item(
            self._binding_key(tenant_id, recipient_id),
            consistent=True,
        )
        request_item = await self._get_item(
            self._request_pointer_key(tenant_id, recipient_id),
            consistent=True,
        )
        binding = None if binding_item is None else self._binding_from_item(binding_item)
        request = None if request_item is None else self._request_from_item(request_item)
        destination = None
        if binding is not None:
            destination_item = await self._get_item(
                self._destination_key(binding.destination_id),
                consistent=True,
            )
            if destination_item is not None:
                destination = self._destination_from_item(destination_item)
        return binding, request, destination

    async def revoke(self, tenant_id: str, recipient_id: str, now: datetime) -> None:
        last_conflict: Exception | None = None
        for _ in range(3):
            binding, request, _ = await self.get_current(tenant_id, recipient_id)
            operations: list[dict[str, Any]] = []
            if binding is not None and binding.status is TelegramBindingStatus.VERIFIED:
                revoked = binding.model_copy(
                    update={
                        "status": TelegramBindingStatus.REVOKED,
                        "revoked_at": now,
                        "version": binding.version + 1,
                        "updated_at": now,
                    }
                )
                operations.append(
                    {
                        "Put": {
                            "TableName": self.table_name,
                            "Item": self._serialize(self._binding_item(revoked)),
                            "ConditionExpression": "#version = :expected_version",
                            "ExpressionAttributeNames": {"#version": "version"},
                            "ExpressionAttributeValues": self._serialize(
                                {":expected_version": binding.version}
                            ),
                        }
                    }
                )
            if request is not None and request.status is TelegramBindingStatus.PENDING:
                keys = [self._request_pointer_key(tenant_id, recipient_id)]
                if request.expires_at > now:
                    keys.insert(0, self._token_key(request.token_hash))
                for key in keys:
                    operations.append(
                        {
                            "Update": {
                                "TableName": self.table_name,
                                "Key": self._serialize(key),
                                "UpdateExpression": "SET #status = :invalidated",
                                "ConditionExpression": (
                                    "#request_id = :request_id AND #status = :pending"
                                ),
                                "ExpressionAttributeNames": {
                                    "#request_id": "request_id",
                                    "#status": "status",
                                },
                                "ExpressionAttributeValues": self._serialize(
                                    {
                                        ":request_id": request.request_id,
                                        ":pending": TelegramBindingStatus.PENDING.value,
                                        ":invalidated": (TelegramBindingStatus.INVALIDATED.value),
                                    }
                                ),
                            }
                        }
                    )
            if not operations:
                return
            try:
                await to_thread(self.client.transact_write_items, TransactItems=operations)
            except Exception as exc:
                if self._conditional_failure_indexes(exc) is not None:
                    last_conflict = exc
                    continue
                raise
            return
        if last_conflict is not None:
            raise last_conflict

    async def _get_current_request(
        self,
        tenant_id: str,
        recipient_id: str,
        *,
        consistent: bool,
    ) -> TelegramBindingRequest | None:
        item = await self._get_item(
            self._request_pointer_key(tenant_id, recipient_id),
            consistent=consistent,
        )
        return None if item is None else self._request_from_item(item)

    async def _get_item(
        self,
        key: Mapping[str, str],
        *,
        consistent: bool,
    ) -> dict[str, Any] | None:
        response = await to_thread(
            self.client.get_item,
            TableName=self.table_name,
            Key=self._serialize(key),
            ConsistentRead=consistent,
        )
        item = response.get("Item")
        return None if item is None else self._deserialize(item)

    def _request_item(
        self,
        request: TelegramBindingRequest,
        *,
        token_lookup: bool,
    ) -> dict[str, Any]:
        key = (
            self._token_key(request.token_hash)
            if token_lookup
            else self._request_pointer_key(request.tenant_id, request.recipient_id)
        )
        return {
            **key,
            "entity_type": "telegram_binding_request",
            **request.model_dump(mode="json", exclude={"expires_at"}),
            "expires_at": int(request.expires_at.timestamp()),
            "schema_version": 1,
        }

    def _binding_item(self, binding: TelegramBinding) -> dict[str, Any]:
        return {
            **self._binding_key(binding.tenant_id, binding.recipient_id),
            "entity_type": "telegram_binding",
            **binding.model_dump(mode="json"),
            "schema_version": 1,
        }

    def _request_from_item(self, item: Mapping[str, Any]) -> TelegramBindingRequest:
        values = self._domain_values(item)
        expires_at = values.get("expires_at")
        if isinstance(expires_at, (int, float)):
            values["expires_at"] = datetime.fromtimestamp(expires_at, tz=UTC)
        return TelegramBindingRequest.model_validate(values)

    def _binding_from_item(self, item: Mapping[str, Any]) -> TelegramBinding:
        return TelegramBinding.model_validate(self._domain_values(item))

    def _destination_from_item(self, item: Mapping[str, Any]) -> TelegramDestination:
        return TelegramDestination.model_validate(self._domain_values(item))

    def _domain_values(self, item: Mapping[str, Any]) -> dict[str, Any]:
        return {
            key: value
            for key, value in item.items()
            if key not in {"PK", "SK", "entity_type", "schema_version", "expires_at_ttl"}
        }

    def _dedupe_put(self, update_id: int, now: datetime) -> dict[str, Any]:
        item = {
            "PK": f"TELEGRAM_UPDATE#{update_id}",
            "SK": "META",
            "entity_type": "telegram_update_dedupe",
            "update_id": update_id,
            "created_at": now.isoformat(),
            "expires_at": int((now + UPDATE_DEDUPE_TTL).timestamp()),
            "schema_version": 1,
        }
        return {
            "TableName": self.table_name,
            "Item": self._serialize(item),
            "ConditionExpression": "attribute_not_exists(PK) AND attribute_not_exists(SK)",
        }

    def _token_key(self, token_hash: str) -> dict[str, str]:
        return {"PK": f"TELEGRAM_BINDING_TOKEN#{token_hash}", "SK": "META"}

    def _request_pointer_key(self, tenant_id: str, recipient_id: str) -> dict[str, str]:
        return {
            "PK": f"TENANT#{tenant_id}",
            "SK": f"TELEGRAM_BINDING_REQUEST#USER#{recipient_id}",
        }

    def _binding_key(self, tenant_id: str, recipient_id: str) -> dict[str, str]:
        return {
            "PK": f"TENANT#{tenant_id}",
            "SK": f"TELEGRAM_BINDING#USER#{recipient_id}",
        }

    def _destination_key(self, destination_id: str) -> dict[str, str]:
        return {"PK": f"TELEGRAM_DESTINATION#{destination_id}", "SK": "META"}

    def _binding_history_key(
        self,
        binding: TelegramBinding,
        now: datetime,
    ) -> dict[str, str]:
        return {
            "PK": f"TENANT#{binding.tenant_id}",
            "SK": (
                f"TELEGRAM_BINDING_HISTORY#USER#{binding.recipient_id}#"
                f"{now.isoformat()}#{binding.version}"
            ),
        }

    def _client_token(self, prefix: str, value: str) -> str:
        digest_length = 36 - len(prefix) - 1
        if digest_length < 16:
            raise ValueError("DynamoDB client token prefix is too long")
        return f"{prefix}-{sha256(value.encode()).hexdigest()[:digest_length]}"

    def _serialize(self, item: Mapping[str, Any]) -> dict[str, Any]:
        return {
            key: self._serializer.serialize(self._normalize_to_dynamo(value))
            for key, value in item.items()
        }

    def _deserialize(self, item: Mapping[str, Any]) -> dict[str, Any]:
        return {
            key: self._normalize_from_dynamo(self._deserializer.deserialize(value))
            for key, value in item.items()
        }

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

    def _conditional_failure_indexes(self, exc: Exception) -> tuple[int, ...] | None:
        response = getattr(exc, "response", {})
        error_code = response.get("Error", {}).get("Code")
        if error_code == "ConditionalCheckFailedException":
            return (-1,)
        if error_code != "TransactionCanceledException":
            return None
        reasons = response.get("CancellationReasons")
        if not isinstance(reasons, list) or not reasons:
            return None
        indexes: list[int] = []
        for index, reason in enumerate(reasons):
            reason_code = reason.get("Code") if isinstance(reason, Mapping) else None
            if reason_code == "ConditionalCheckFailed":
                indexes.append(index)
            elif reason_code not in {None, "None"}:
                return None
        return tuple(indexes) if indexes else None
