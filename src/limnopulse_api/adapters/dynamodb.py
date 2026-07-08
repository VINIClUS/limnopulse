from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.entities import Device, Membership, Pond, Tenant
from limnopulse_api.domain.roles import TenantRole


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


class DynamoKeyBuilder:
    def tenant(self, tenant_id: str) -> dict[str, str]:
        return {"PK": f"TENANT#{tenant_id}", "SK": "META"}

    def pond(self, tenant_id: str, pond_id: str) -> dict[str, str]:
        return {"PK": f"TENANT#{tenant_id}", "SK": f"POND#{pond_id}"}

    def device(self, tenant_id: str, device_id: str) -> dict[str, str]:
        return {"PK": f"TENANT#{tenant_id}", "SK": f"DEVICE#{device_id}"}

    def device_lookup(self, device_id: str) -> dict[str, str]:
        return {"PK": f"DEVICE#{device_id}", "SK": "META"}

    def membership(self, cognito_sub: str, tenant_id: str) -> dict[str, str]:
        return {"PK": f"USER#{cognito_sub}", "SK": f"TENANT#{tenant_id}"}

    def tenant_member(self, tenant_id: str, cognito_sub: str) -> dict[str, str]:
        return {"PK": f"TENANT#{tenant_id}", "SK": f"MEMBER#{cognito_sub}"}


class DynamoDomainRepository:
    def __init__(self, table_name: str, client: Any) -> None:
        self.table_name = table_name
        self.client = client
        self.keys = DynamoKeyBuilder()
        self._serializer = TypeSerializer()
        self._deserializer = TypeDeserializer()

    async def get_membership(self, cognito_sub: str, tenant_id: str) -> Membership | None:
        response = self.client.get_item(
            TableName=self.table_name,
            Key=self._serialize_item(self.keys.membership(cognito_sub, tenant_id)),
        )
        item = self._response_item(response)
        if item is None:
            return None
        return self._membership_from_item(item)

    async def list_memberships_for_user(self, cognito_sub: str) -> list[Membership]:
        response = self.client.query(
            TableName=self.table_name,
            KeyConditionExpression="PK = :pk AND begins_with(SK, :sk_prefix)",
            ExpressionAttributeValues=self._serialize_values(
                {
                    ":pk": f"USER#{cognito_sub}",
                    ":sk_prefix": "TENANT#",
                }
            ),
        )
        return [self._membership_from_item(item) for item in self._response_items(response)]

    async def create_tenant_with_owner(self, tenant_id: str, name: str, owner_sub: str) -> Tenant:
        now = utc_now()
        tenant_item = {
            **self.keys.tenant(tenant_id),
            "entity_type": "tenant",
            "tenant_id": tenant_id,
            "name": name,
            "settings": {},
            "status": "active",
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "version": 1,
            "schema_version": 1,
        }
        membership_item = {
            **self.keys.membership(owner_sub, tenant_id),
            "entity_type": "membership",
            "tenant_id": tenant_id,
            "cognito_sub": owner_sub,
            "role": TenantRole.OWNER.value,
            "status": "active",
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "version": 1,
            "schema_version": 1,
        }
        tenant_member_item = {
            **self.keys.tenant_member(tenant_id, owner_sub),
            "entity_type": "tenant_member",
            "tenant_id": tenant_id,
            "cognito_sub": owner_sub,
            "role": TenantRole.OWNER.value,
            "status": "active",
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "version": 1,
            "schema_version": 1,
        }
        try:
            self.client.transact_write_items(
                TransactItems=[
                    self._conditioned_put(tenant_item),
                    self._conditioned_put(membership_item),
                    self._conditioned_put(tenant_member_item),
                ]
            )
        except Exception as exc:  # pragma: no cover - defensive adapter mapping
            self._raise_if_conflict(exc)
            raise
        return self._tenant_from_item(tenant_item)

    async def list_tenants_for_memberships(self, memberships: list[Membership]) -> list[Tenant]:
        tenants: list[Tenant] = []
        for membership in memberships:
            tenant = await self.get_tenant(membership.tenant_id)
            if tenant is not None:
                tenants.append(tenant)
        return tenants

    async def get_tenant(self, tenant_id: str) -> Tenant | None:
        response = self.client.get_item(
            TableName=self.table_name,
            Key=self._serialize_item(self.keys.tenant(tenant_id)),
        )
        item = self._response_item(response)
        if item is None:
            return None
        return self._tenant_from_item(item)

    async def update_tenant(
        self,
        tenant_id: str,
        expected_version: int,
        name: str | None,
    ) -> Tenant:
        existing = await self.get_tenant(tenant_id)
        if existing is None:
            raise NotFoundError(f"Tenant {tenant_id} not found")

        response = self._update_item(
            key=self.keys.tenant(tenant_id),
            expected_version=expected_version,
            updates={"name": name},
        )
        item = self._response_item(response, attribute_name="Attributes")
        if item is None:
            raise NotFoundError(f"Tenant {tenant_id} not found")
        return self._tenant_from_item(item)

    async def list_ponds(self, tenant_id: str) -> list[Pond]:
        response = self.client.query(
            TableName=self.table_name,
            KeyConditionExpression="PK = :pk AND begins_with(SK, :sk_prefix)",
            ExpressionAttributeValues=self._serialize_values(
                {
                    ":pk": f"TENANT#{tenant_id}",
                    ":sk_prefix": "POND#",
                }
            ),
        )
        return [self._pond_from_item(item) for item in self._response_items(response)]

    async def get_pond(self, tenant_id: str, pond_id: str) -> Pond | None:
        response = self.client.get_item(
            TableName=self.table_name,
            Key=self._serialize_item(self.keys.pond(tenant_id, pond_id)),
        )
        item = self._response_item(response)
        if item is None:
            return None
        return self._pond_from_item(item)

    async def create_pond(
        self,
        tenant_id: str,
        pond_id: str,
        name: str,
        description: str | None,
    ) -> Pond:
        now = utc_now()
        item = {
            **self.keys.pond(tenant_id, pond_id),
            "entity_type": "pond",
            "tenant_id": tenant_id,
            "pond_id": pond_id,
            "name": name,
            "description": description,
            "status": "active",
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "version": 1,
            "schema_version": 1,
        }
        try:
            self.client.put_item(**self._conditioned_put(item)["Put"])
        except Exception as exc:  # pragma: no cover - defensive adapter mapping
            self._raise_if_conflict(exc)
            raise
        return self._pond_from_item(item)

    async def update_pond(
        self,
        tenant_id: str,
        pond_id: str,
        expected_version: int,
        name: str | None,
        description: str | None,
    ) -> Pond:
        existing = await self.get_pond(tenant_id, pond_id)
        if existing is None:
            raise NotFoundError(f"Pond {pond_id} not found")

        response = self._update_item(
            key=self.keys.pond(tenant_id, pond_id),
            expected_version=expected_version,
            updates={"name": name, "description": description},
        )
        item = self._response_item(response, attribute_name="Attributes")
        if item is None:
            raise NotFoundError(f"Pond {pond_id} not found")
        return self._pond_from_item(item)

    async def list_devices(self, tenant_id: str) -> list[Device]:
        response = self.client.query(
            TableName=self.table_name,
            KeyConditionExpression="PK = :pk AND begins_with(SK, :sk_prefix)",
            ExpressionAttributeValues=self._serialize_values(
                {
                    ":pk": f"TENANT#{tenant_id}",
                    ":sk_prefix": "DEVICE#",
                }
            ),
        )
        return [self._device_from_item(item) for item in self._response_items(response)]

    async def get_device(self, tenant_id: str, device_id: str) -> Device | None:
        response = self.client.get_item(
            TableName=self.table_name,
            Key=self._serialize_item(self.keys.device(tenant_id, device_id)),
        )
        item = self._response_item(response)
        if item is None:
            return None
        return self._device_from_item(item)

    async def create_device(
        self,
        tenant_id: str,
        pond_id: str,
        device_id: str,
        name: str,
        firmware_version: str | None,
    ) -> Device:
        now = utc_now()
        device_item = {
            **self.keys.device(tenant_id, device_id),
            "entity_type": "device",
            "tenant_id": tenant_id,
            "pond_id": pond_id,
            "device_id": device_id,
            "name": name,
            "auth_type": "mtls",
            "firmware_version": firmware_version,
            "status": "active",
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "version": 1,
            "schema_version": 1,
        }
        lookup_item = {
            **self.keys.device_lookup(device_id),
            "entity_type": "device_lookup",
            "tenant_id": tenant_id,
            "pond_id": pond_id,
            "device_id": device_id,
            "name": name,
            "auth_type": "mtls",
            "firmware_version": firmware_version,
            "status": "active",
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "version": 1,
            "schema_version": 1,
        }
        try:
            self.client.transact_write_items(
                TransactItems=[
                    self._conditioned_put(device_item),
                    self._conditioned_put(lookup_item),
                ]
            )
        except Exception as exc:  # pragma: no cover - defensive adapter mapping
            self._raise_if_conflict(exc)
            raise
        return self._device_from_item(device_item)

    async def update_device(
        self,
        tenant_id: str,
        device_id: str,
        expected_version: int,
        name: str | None,
        pond_id: str | None,
        firmware_version: str | None,
    ) -> Device:
        existing = await self.get_device(tenant_id, device_id)
        if existing is None:
            raise NotFoundError(f"Device {device_id} not found")

        lookup_response = self.client.get_item(
            TableName=self.table_name,
            Key=self._serialize_item(self.keys.device_lookup(device_id)),
        )
        lookup_item = self._response_item(lookup_response)
        if lookup_item is None:
            raise NotFoundError(f"Device {device_id} not found")

        now = utc_now()
        next_version = expected_version + 1
        updates = {
            "name": name,
            "pond_id": pond_id,
            "firmware_version": firmware_version,
        }
        try:
            self.client.transact_write_items(
                TransactItems=[
                    self._conditioned_update(
                        key=self.keys.device(tenant_id, device_id),
                        expected_version=expected_version,
                        next_version=next_version,
                        updated_at=now.isoformat(),
                        updates=updates,
                    ),
                    self._conditioned_update(
                        key=self.keys.device_lookup(device_id),
                        expected_version=expected_version,
                        next_version=next_version,
                        updated_at=now.isoformat(),
                        updates=updates,
                    ),
                ]
            )
        except Exception as exc:  # pragma: no cover - defensive adapter mapping
            self._raise_if_conflict(exc)
            raise

        merged_item = {
            **existing.model_dump(mode="json"),
            **{field: value for field, value in updates.items() if value is not None},
            "updated_at": now.isoformat(),
            "version": next_version,
        }
        return self._device_from_item(merged_item)

    def _conditioned_put(self, item: dict[str, Any]) -> dict[str, Any]:
        return {
            "Put": {
                "TableName": self.table_name,
                "Item": self._serialize_item(item),
                "ConditionExpression": "attribute_not_exists(PK) AND attribute_not_exists(SK)",
            }
        }

    def _conditioned_update(
        self,
        *,
        key: dict[str, str],
        expected_version: int,
        next_version: int,
        updated_at: str,
        updates: dict[str, Any | None],
    ) -> dict[str, Any]:
        expression_names = {
            "#updated_at": "updated_at",
            "#version": "version",
        }
        expression_values: dict[str, Any] = {
            ":updated_at": updated_at,
            ":next_version": next_version,
            ":expected_version": expected_version,
        }
        assignments = [
            "#updated_at = :updated_at",
            "#version = :next_version",
        ]
        for index, (field_name, field_value) in enumerate(updates.items()):
            if field_value is None:
                continue
            name_token = f"#field_{index}"
            value_token = f":value_{index}"
            expression_names[name_token] = field_name
            expression_values[value_token] = field_value
            assignments.append(f"{name_token} = {value_token}")

        return {
            "Update": {
                "TableName": self.table_name,
                "Key": self._serialize_item(key),
                "UpdateExpression": "SET " + ", ".join(assignments),
                "ConditionExpression": "attribute_exists(PK) AND attribute_exists(SK) AND #version = :expected_version",
                "ExpressionAttributeNames": expression_names,
                "ExpressionAttributeValues": self._serialize_values(expression_values),
            }
        }

    def _update_item(
        self,
        *,
        key: dict[str, str],
        expected_version: int,
        updates: dict[str, Any | None],
    ) -> dict[str, Any]:
        now = utc_now()
        expression_names = {
            "#updated_at": "updated_at",
            "#version": "version",
        }
        expression_values: dict[str, Any] = {
            ":updated_at": now.isoformat(),
            ":next_version": expected_version + 1,
            ":expected_version": expected_version,
        }
        assignments = [
            "#updated_at = :updated_at",
            "#version = :next_version",
        ]
        for index, (field_name, field_value) in enumerate(updates.items()):
            if field_value is None:
                continue
            name_token = f"#field_{index}"
            value_token = f":value_{index}"
            expression_names[name_token] = field_name
            expression_values[value_token] = field_value
            assignments.append(f"{name_token} = {value_token}")
        try:
            return self.client.update_item(
                TableName=self.table_name,
                Key=self._serialize_item(key),
                UpdateExpression="SET " + ", ".join(assignments),
                ConditionExpression="attribute_exists(PK) AND attribute_exists(SK) AND #version = :expected_version",
                ExpressionAttributeNames=expression_names,
                ExpressionAttributeValues=self._serialize_values(expression_values),
                ReturnValues="ALL_NEW",
            )
        except Exception as exc:  # pragma: no cover - defensive adapter mapping
            self._raise_if_conflict(exc)
            raise

    def _serialize_item(self, item: dict[str, Any]) -> dict[str, Any]:
        return {key: self._serializer.serialize(value) for key, value in item.items()}

    def _serialize_values(self, values: dict[str, Any]) -> dict[str, Any]:
        return {key: self._serializer.serialize(value) for key, value in values.items()}

    def _response_item(
        self,
        response: dict[str, Any],
        *,
        attribute_name: str = "Item",
    ) -> dict[str, Any] | None:
        item = response.get(attribute_name)
        if item is None:
            return None
        return self._deserialize_item(item)

    def _response_items(self, response: dict[str, Any]) -> list[dict[str, Any]]:
        return [self._deserialize_item(item) for item in response.get("Items", [])]

    def _deserialize_item(self, item: dict[str, Any]) -> dict[str, Any]:
        return {key: self._deserialize_value(value) for key, value in item.items()}

    def _deserialize_value(self, value: Any) -> Any:
        deserialized = self._deserializer.deserialize(value)
        if isinstance(deserialized, Decimal):
            if deserialized % 1 == 0:
                return int(deserialized)
            return float(deserialized)
        if isinstance(deserialized, list):
            return [self._normalize_deserialized_value(item) for item in deserialized]
        if isinstance(deserialized, dict):
            return {
                key: self._normalize_deserialized_value(item)
                for key, item in deserialized.items()
            }
        return deserialized

    def _normalize_deserialized_value(self, value: Any) -> Any:
        if isinstance(value, Decimal):
            if value % 1 == 0:
                return int(value)
            return float(value)
        if isinstance(value, list):
            return [self._normalize_deserialized_value(item) for item in value]
        if isinstance(value, dict):
            return {
                key: self._normalize_deserialized_value(item)
                for key, item in value.items()
            }
        return value

    def _raise_if_conflict(self, exc: Exception) -> None:
        error_code = getattr(exc, "response", {}).get("Error", {}).get("Code")
        if error_code in {
            "ConditionalCheckFailedException",
            "TransactionCanceledException",
        }:
            raise ConflictError(str(exc)) from exc

    def _tenant_from_item(self, item: dict[str, Any]) -> Tenant:
        return Tenant(
            tenant_id=item["tenant_id"],
            name=item["name"],
            settings=item.get("settings", {}),
            status=item.get("status", "active"),
            created_at=datetime.fromisoformat(item["created_at"]),
            updated_at=datetime.fromisoformat(item["updated_at"]),
            version=int(item["version"]),
            schema_version=int(item.get("schema_version", 1)),
        )

    def _membership_from_item(self, item: dict[str, Any]) -> Membership:
        return Membership(
            tenant_id=item["tenant_id"],
            cognito_sub=item["cognito_sub"],
            role=TenantRole(item["role"]),
            status=item.get("status", "active"),
            created_at=datetime.fromisoformat(item["created_at"]),
            updated_at=datetime.fromisoformat(item["updated_at"]),
            version=int(item["version"]),
            schema_version=int(item.get("schema_version", 1)),
        )

    def _pond_from_item(self, item: dict[str, Any]) -> Pond:
        return Pond(
            tenant_id=item["tenant_id"],
            pond_id=item["pond_id"],
            name=item["name"],
            description=item.get("description"),
            status=item.get("status", "active"),
            created_at=datetime.fromisoformat(item["created_at"]),
            updated_at=datetime.fromisoformat(item["updated_at"]),
            version=int(item["version"]),
            schema_version=int(item.get("schema_version", 1)),
        )

    def _device_from_item(self, item: dict[str, Any]) -> Device:
        return Device(
            tenant_id=item["tenant_id"],
            pond_id=item["pond_id"],
            device_id=item["device_id"],
            name=item["name"],
            auth_type=item.get("auth_type", "mtls"),
            firmware_version=item.get("firmware_version"),
            status=item.get("status", "active"),
            created_at=datetime.fromisoformat(item["created_at"]),
            updated_at=datetime.fromisoformat(item["updated_at"]),
            version=int(item["version"]),
            schema_version=int(item.get("schema_version", 1)),
        )
