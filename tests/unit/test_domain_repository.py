from datetime import UTC, datetime

import pytest
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from limnopulse_api.adapters.dynamodb import DynamoDomainRepository, DynamoKeyBuilder
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.entities import Tenant


def test_key_builder_uses_limnopulse_domain_shapes() -> None:
    keys = DynamoKeyBuilder()

    assert keys.tenant("tnt_1") == {"PK": "TENANT#tnt_1", "SK": "META"}
    assert keys.pond("tnt_1", "pond_1") == {"PK": "TENANT#tnt_1", "SK": "POND#pond_1"}
    assert keys.device("tnt_1", "dev_1") == {"PK": "TENANT#tnt_1", "SK": "DEVICE#dev_1"}
    assert keys.device_lookup("dev_1") == {"PK": "DEVICE#dev_1", "SK": "META"}
    assert keys.membership("sub_1", "tnt_1") == {"PK": "USER#sub_1", "SK": "TENANT#tnt_1"}
    assert keys.tenant_member("tnt_1", "sub_1") == {"PK": "TENANT#tnt_1", "SK": "MEMBER#sub_1"}


def test_tenant_settings_cannot_be_mutated_in_place() -> None:
    tenant = Tenant(
        tenant_id="tnt_1",
        name="Tenant 1",
        settings={"timezone": "UTC"},
        created_at=datetime(2026, 1, 1),
        updated_at=datetime(2026, 1, 1),
        version=1,
    )

    with pytest.raises(TypeError):
        tenant.settings["timezone"] = "America/Sao_Paulo"


class ConditionalFailure(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class RecordingDynamoClient:
    def __init__(self) -> None:
        self.serializer = TypeSerializer()
        self.deserializer = TypeDeserializer()
        self.items: dict[tuple[str, str], dict] = {}
        self.get_item_calls: list[dict] = []
        self.query_calls: list[dict] = []
        self.put_item_calls: list[dict] = []
        self.update_item_calls: list[dict] = []
        self.transact_write_items_calls: list[dict] = []
        self.scan_calls = 0
        self.fail_next_update_with: Exception | None = None
        self.fail_next_transaction_with: Exception | None = None

    def seed_item(self, item: dict) -> None:
        self.items[(item["PK"], item["SK"])] = dict(item)

    def get_item(self, **kwargs):
        self.get_item_calls.append(kwargs)
        key = self._decode_key(kwargs["Key"])
        item = self.items.get(key)
        if item is None:
            return {}
        return {"Item": self._encode_item(item)}

    def query(self, **kwargs):
        self.query_calls.append(kwargs)
        values = self._decode_values(kwargs["ExpressionAttributeValues"])
        pk = values[":pk"]
        sk_prefix = values[":sk_prefix"]
        matched = [
            self._encode_item(item)
            for item in self.items.values()
            if item["PK"] == pk and item["SK"].startswith(sk_prefix)
        ]
        matched.sort(key=lambda item: self.deserializer.deserialize(item["SK"]))
        return {"Items": matched}

    def put_item(self, **kwargs):
        self.put_item_calls.append(kwargs)
        item = self._decode_item(kwargs["Item"])
        self.items[(item["PK"], item["SK"])] = item
        return {}

    def update_item(self, **kwargs):
        self.update_item_calls.append(kwargs)
        if self.fail_next_update_with is not None:
            exc = self.fail_next_update_with
            self.fail_next_update_with = None
            raise exc

        key = self._decode_key(kwargs["Key"])
        existing = self.items.get(key)
        if existing is None:
            return {}

        values = self._decode_values(kwargs["ExpressionAttributeValues"])
        expected_version = values[":expected_version"]
        if existing["version"] != expected_version:
            raise ConditionalFailure("ConditionalCheckFailedException")

        updated = dict(existing)
        for clause in kwargs["UpdateExpression"].removeprefix("SET ").split(", "):
            name_token, value_token = clause.split(" = ")
            field_name = kwargs["ExpressionAttributeNames"][name_token]
            updated[field_name] = values[value_token]
        self.items[key] = updated
        return {"Attributes": self._encode_item(updated)}

    def transact_write_items(self, **kwargs):
        self.transact_write_items_calls.append(kwargs)
        if self.fail_next_transaction_with is not None:
            exc = self.fail_next_transaction_with
            self.fail_next_transaction_with = None
            raise exc

        for entry in kwargs["TransactItems"]:
            if "Put" in entry:
                item = self._decode_item(entry["Put"]["Item"])
                self.items[(item["PK"], item["SK"])] = item
                continue

            update = entry["Update"]
            key = self._decode_key(update["Key"])
            existing = self.items.get(key)
            if existing is None:
                raise ConditionalFailure("TransactionCanceledException")

            values = self._decode_values(update["ExpressionAttributeValues"])
            if existing["version"] != values[":expected_version"]:
                raise ConditionalFailure("TransactionCanceledException")

            updated = dict(existing)
            for clause in update["UpdateExpression"].removeprefix("SET ").split(", "):
                name_token, value_token = clause.split(" = ")
                field_name = update["ExpressionAttributeNames"][name_token]
                updated[field_name] = values[value_token]
            self.items[key] = updated

        return {}

    def scan(self, **kwargs):
        self.scan_calls += 1
        return {}

    def _encode_item(self, item: dict) -> dict:
        return {key: self.serializer.serialize(value) for key, value in item.items()}

    def _decode_item(self, item: dict) -> dict:
        return {key: self.deserializer.deserialize(value) for key, value in item.items()}

    def _decode_key(self, key: dict) -> tuple[str, str]:
        decoded = self._decode_item(key)
        return decoded["PK"], decoded["SK"]

    def _decode_values(self, values: dict) -> dict:
        return {key: self.deserializer.deserialize(value) for key, value in values.items()}


@pytest.mark.asyncio
async def test_create_tenant_with_owner_uses_low_level_transaction_and_no_scan() -> None:
    client = RecordingDynamoClient()
    repo = DynamoDomainRepository(table_name="LimnopulseDomain", client=client)

    tenant = await repo.create_tenant_with_owner("tnt_1", "Demo", "sub_1")

    assert tenant.tenant_id == "tnt_1"
    assert tenant.name == "Demo"
    assert tenant.settings == {}
    assert tenant.status == "active"
    assert tenant.created_at.tzinfo == UTC
    assert len(client.transact_write_items_calls) == 1
    items = client.transact_write_items_calls[0]["TransactItems"]
    assert len(items) == 3
    assert all("ConditionExpression" in item["Put"] for item in items)
    assert all(item["Put"]["Item"]["PK"] == {"S": item["Put"]["Item"]["PK"]["S"]} for item in items)
    assert client.scan_calls == 0


@pytest.mark.asyncio
async def test_list_memberships_for_user_uses_low_level_query_values() -> None:
    client = RecordingDynamoClient()
    client.seed_item(
        {
            "PK": "USER#sub_1",
            "SK": "TENANT#tnt_1",
            "entity_type": "membership",
            "tenant_id": "tnt_1",
            "cognito_sub": "sub_1",
            "role": "owner",
            "status": "active",
            "created_at": "2026-07-08T12:00:00+00:00",
            "updated_at": "2026-07-08T12:00:00+00:00",
            "version": 1,
            "schema_version": 1,
        }
    )
    repo = DynamoDomainRepository(table_name="LimnopulseDomain", client=client)

    memberships = await repo.list_memberships_for_user("sub_1")

    assert [membership.tenant_id for membership in memberships] == ["tnt_1"]
    query_call = client.query_calls[0]
    assert query_call["ExpressionAttributeValues"] == {
        ":pk": {"S": "USER#sub_1"},
        ":sk_prefix": {"S": "TENANT#"},
    }


@pytest.mark.asyncio
async def test_update_tenant_missing_target_raises_not_found_before_conditional_update() -> None:
    client = RecordingDynamoClient()
    repo = DynamoDomainRepository(table_name="LimnopulseDomain", client=client)

    with pytest.raises(NotFoundError):
        await repo.update_tenant("missing", expected_version=1, name="Renamed")

    assert client.update_item_calls == []


@pytest.mark.asyncio
async def test_update_tenant_version_conflict_raises_conflict_with_low_level_update() -> None:
    client = RecordingDynamoClient()
    client.seed_item(
        {
            "PK": "TENANT#tnt_1",
            "SK": "META",
            "entity_type": "tenant",
            "tenant_id": "tnt_1",
            "name": "Demo",
            "settings": {"timezone": "UTC"},
            "status": "active",
            "created_at": "2026-07-08T12:00:00+00:00",
            "updated_at": "2026-07-08T12:00:00+00:00",
            "version": 2,
            "schema_version": 1,
        }
    )
    repo = DynamoDomainRepository(table_name="LimnopulseDomain", client=client)

    with pytest.raises(ConflictError):
        await repo.update_tenant("tnt_1", expected_version=1, name="Renamed")

    update_call = client.update_item_calls[0]
    assert update_call["Key"] == {"PK": {"S": "TENANT#tnt_1"}, "SK": {"S": "META"}}
    assert update_call["ExpressionAttributeValues"][":expected_version"] == {"N": "1"}
    assert update_call["ReturnValues"] == "ALL_NEW"


@pytest.mark.asyncio
async def test_update_device_uses_transactional_patch_updates_and_preserves_unmapped_fields() -> None:
    client = RecordingDynamoClient()
    device_item = {
        "PK": "TENANT#tnt_1",
        "SK": "DEVICE#dev_1",
        "entity_type": "device",
        "tenant_id": "tnt_1",
        "pond_id": "pond_1",
        "device_id": "dev_1",
        "name": "Aerator",
        "auth_type": "mtls",
        "firmware_version": "1.0.0",
        "status": "active",
        "created_at": "2026-07-08T12:00:00+00:00",
        "updated_at": "2026-07-08T12:00:00+00:00",
        "version": 3,
        "schema_version": 1,
        "unmapped_field": "keep-me",
    }
    lookup_item = {
        "PK": "DEVICE#dev_1",
        "SK": "META",
        "entity_type": "device_lookup",
        "tenant_id": "tnt_1",
        "pond_id": "pond_1",
        "device_id": "dev_1",
        "name": "Aerator",
        "auth_type": "mtls",
        "firmware_version": "1.0.0",
        "status": "active",
        "created_at": "2026-07-08T12:00:00+00:00",
        "updated_at": "2026-07-08T12:00:00+00:00",
        "version": 3,
        "schema_version": 1,
        "unmapped_field": "keep-me-too",
    }
    client.seed_item(device_item)
    client.seed_item(lookup_item)
    repo = DynamoDomainRepository(table_name="LimnopulseDomain", client=client)

    updated = await repo.update_device(
        "tnt_1",
        "dev_1",
        expected_version=3,
        name="Aerator V2",
        pond_id="pond_2",
        firmware_version="1.1.0",
    )

    assert updated.name == "Aerator V2"
    assert updated.pond_id == "pond_2"
    assert updated.firmware_version == "1.1.0"
    assert updated.version == 4
    assert len(client.transact_write_items_calls) == 1
    txn = client.transact_write_items_calls[0]["TransactItems"]
    assert [set(item.keys()) for item in txn] == [{"Update"}, {"Update"}]
    first_update = txn[0]["Update"]
    assert first_update["Key"] == {"PK": {"S": "TENANT#tnt_1"}, "SK": {"S": "DEVICE#dev_1"}}
    assert first_update["ExpressionAttributeValues"][":expected_version"] == {"N": "3"}
    assert first_update["ExpressionAttributeValues"][":value_0"] == {"S": "Aerator V2"}
    assert first_update["ExpressionAttributeValues"][":value_1"] == {"S": "pond_2"}
    assert first_update["ExpressionAttributeValues"][":value_2"] == {"S": "1.1.0"}
    assert client.items[("TENANT#tnt_1", "DEVICE#dev_1")]["unmapped_field"] == "keep-me"
    assert client.items[("DEVICE#dev_1", "META")]["unmapped_field"] == "keep-me-too"


@pytest.mark.asyncio
async def test_update_pond_missing_target_raises_not_found() -> None:
    client = RecordingDynamoClient()
    repo = DynamoDomainRepository(table_name="LimnopulseDomain", client=client)

    with pytest.raises(NotFoundError):
        await repo.update_pond("tnt_1", "missing", expected_version=1, name="North", description=None)

    assert client.update_item_calls == []
