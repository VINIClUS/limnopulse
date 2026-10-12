import json
from datetime import UTC, datetime, timedelta

import pytest
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError

from limnopulse_api.adapters.dynamodb_integrations import DynamoIntegrationRepository
from limnopulse_api.adapters.dynamodb_v2 import (
    DynamoConditionalConflict,
    external_identity_hash,
)
from limnopulse_api.core.errors import ConflictError
from limnopulse_api.domain.integrations import (
    DeviceIntegration,
    DeviceIntegrationStatus,
    IntegrationAccount,
    IntegrationAccountStatus,
)
from limnopulse_api.domain.v2_common import new_idempotency_receipt
from limnopulse_api.repositories.v2_contracts import IntegrationRepository

NOW = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)
TABLE = "LimnopulseDomain"
TENANT = "tnt_a"
ACCOUNT_ID = "ia_" + "1" * 32
INTEGRATION_ID = "di_" + "2" * 32
DEVICE_ID = "dev_" + "3" * 32
RAW_IDENTITY = "serial-raw-SN-0042"
RAW_CREDENTIAL_REFERENCE = "secretsmanager:vendor/tnt_a"
IDENTITY_HASH = external_identity_hash("vendor.cloud", RAW_IDENTITY)
KEY_HASH = "a" * 64
REQUEST_HASH = "b" * 64


class FakeDynamoClient:
    def __init__(self) -> None:
        self.serializer = TypeSerializer()
        self.deserializer = TypeDeserializer()
        self.items: dict[tuple[str, str], dict] = {}
        self.calls: list[tuple[str, dict]] = []
        self.query_pages: list[dict] | None = None
        self.fail_with: Exception | None = None

    @classmethod
    def rejecting(cls, *reason_codes: str) -> "FakeDynamoClient":
        client = cls()
        client.fail_with = ClientError(
            {
                "Error": {"Code": "TransactionCanceledException", "Message": "cancelled"},
                "CancellationReasons": [{"Code": code} for code in reason_codes],
            },
            "TransactWriteItems",
        )
        return client

    def seed(self, item: dict) -> None:
        self.items[(item["PK"], item["SK"])] = {
            key: self.serializer.serialize(value) for key, value in item.items()
        }

    def get_item(self, **kwargs):
        self.calls.append(("get_item", kwargs))
        key = {name: self.deserializer.deserialize(value) for name, value in kwargs["Key"].items()}
        item = self.items.get((key["PK"], key["SK"]))
        return {} if item is None else {"Item": item}

    def query(self, **kwargs):
        self.calls.append(("query", kwargs))
        if self.query_pages is not None:
            return self.query_pages.pop(0)
        return {"Items": []}

    def transact_write_items(self, **kwargs):
        self.calls.append(("transact_write_items", kwargs))
        if self.fail_with is not None:
            error, self.fail_with = self.fail_with, None
            raise error
        return {}

    def __getattr__(self, name: str):
        raise AssertionError(f"unexpected client call: {name}")

    def transaction(self) -> list[dict]:
        return [kwargs for name, kwargs in self.calls if name == "transact_write_items"][-1][
            "TransactItems"
        ]


def account(**overrides) -> IntegrationAccount:
    values = {
        "tenant_id": TENANT,
        "integration_account_id": ACCOUNT_ID,
        "provider_key": "vendor.cloud",
        "display_name": "Vendor",
        "credential_reference": RAW_CREDENTIAL_REFERENCE,
        "created_at": NOW,
        "updated_at": NOW,
        "version": 1,
    }
    values.update(overrides)
    return IntegrationAccount(**values)


def device_integration(**overrides) -> DeviceIntegration:
    values = {
        "tenant_id": TENANT,
        "device_integration_id": INTEGRATION_ID,
        "device_id": DEVICE_ID,
        "integration_account_id": ACCOUNT_ID,
        "provider_key": "vendor.cloud",
        "external_identity_hash": IDENTITY_HASH,
        "created_at": NOW,
        "updated_at": NOW,
        "version": 1,
    }
    values.update(overrides)
    return DeviceIntegration(**values)


def receipt(scope: str = "device-integration-create", snapshot: dict | None = None):
    return new_idempotency_receipt(
        tenant_id=TENANT,
        scope=scope,
        key_hash=KEY_HASH,
        request_hash=REQUEST_HASH,
        response_snapshot_json=json.dumps(snapshot or {"ok": True}),
        created_at=NOW,
    )


def repository(client: FakeDynamoClient) -> DynamoIntegrationRepository:
    return DynamoIntegrationRepository(TABLE, client, clock=lambda: NOW)


def transaction_item_keys(client: FakeDynamoClient) -> list[tuple[str, str]]:
    deserializer = TypeDeserializer()
    keys = []
    for action in client.transaction():
        item = action["Put"]["Item"] if "Put" in action else action["ConditionCheck"]["Key"]
        keys.append((deserializer.deserialize(item["PK"]), deserializer.deserialize(item["SK"])))
    return keys


def transaction_items(client: FakeDynamoClient) -> list[dict]:
    deserializer = TypeDeserializer()
    return [
        {key: deserializer.deserialize(value) for key, value in action["Put"]["Item"].items()}
        for action in client.transaction()
        if "Put" in action
    ]


def flatten_transaction(client: FakeDynamoClient) -> str:
    return json.dumps(client.calls, default=str, sort_keys=True)


def test_repository_implements_integration_protocol() -> None:
    assert isinstance(repository(FakeDynamoClient()), IntegrationRepository)


@pytest.mark.asyncio
async def test_device_integration_claims_external_identity_once() -> None:
    client = FakeDynamoClient()
    created = await repository(client).create_device_integration(device_integration(), receipt())

    assert created == device_integration()
    assert transaction_item_keys(client) == [
        (f"TENANT#{TENANT}", f"DEVICE_INTEGRATION#{INTEGRATION_ID}"),
        (f"EXTERNAL_ID#vendor.cloud#{IDENTITY_HASH}", "META"),
        (f"TENANT#{TENANT}", f"IDEMPOTENCY#device-integration-create#{KEY_HASH}"),
        (f"TENANT#{TENANT}", f"INTEGRATION_ACCOUNT#{ACCOUNT_ID}"),
        (f"TENANT#{TENANT}", f"DEVICE#{DEVICE_ID}"),
    ]
    actions = client.transaction()
    assert actions[0]["Put"]["ConditionExpression"] == "attribute_not_exists(PK)"
    assert actions[1]["Put"]["ConditionExpression"] == "attribute_not_exists(PK)"
    assert "expires_at" in actions[2]["Put"]["ConditionExpression"]
    account_check = actions[3]["ConditionCheck"]
    assert account_check["ConditionExpression"] == (
        "attribute_exists(PK) AND #status IN (:pending, :active)"
    )
    assert account_check["ExpressionAttributeNames"] == {"#status": "status"}
    assert account_check["ExpressionAttributeValues"] == {
        ":pending": {"S": "pending"},
        ":active": {"S": "active"},
    }
    device_check = actions[4]["ConditionCheck"]
    assert device_check["ConditionExpression"] == (
        "attribute_exists(PK) AND #status <> :decommissioned"
    )
    assert device_check["ExpressionAttributeValues"] == {":decommissioned": {"S": "decommissioned"}}
    assert "external_identity" not in flatten_transaction(client).replace(
        "external_identity_hash", ""
    )


@pytest.mark.asyncio
async def test_external_identity_lookup_holds_only_routing_metadata() -> None:
    client = FakeDynamoClient()
    await repository(client).create_device_integration(device_integration(), receipt())

    tenant_row, lookup, receipt_row = transaction_items(client)
    assert len(client.transaction()) == 5
    assert lookup == {
        "PK": f"EXTERNAL_ID#vendor.cloud#{IDENTITY_HASH}",
        "SK": "META",
        "tenant_id": TENANT,
        "device_id": DEVICE_ID,
        "device_integration_id": INTEGRATION_ID,
        "provider_key": "vendor.cloud",
        "external_identity_hash": IDENTITY_HASH,
        "status": "provisioning",
        "version": 1,
    }
    assert tenant_row["entity_type"] == "device_integration"
    assert tenant_row["external_identity_hash"] == IDENTITY_HASH
    # Only idempotency receipts carry the numeric TTL attribute.
    assert "expires_at" not in tenant_row and "expires_at" not in lookup
    assert "expires_at" in receipt_row


@pytest.mark.asyncio
async def test_no_raw_identity_in_any_serialized_transaction_item() -> None:
    client = FakeDynamoClient()
    await repository(client).create_device_integration(device_integration(), receipt())

    for action in client.transaction():
        assert RAW_IDENTITY not in json.dumps(action, default=str)
    assert RAW_IDENTITY not in flatten_transaction(client)


@pytest.mark.asyncio
async def test_external_identity_collision_maps_to_conflict() -> None:
    client = FakeDynamoClient.rejecting("None", "ConditionalCheckFailed", "None", "None", "None")
    with pytest.raises(ConflictError, match="external identity already claimed") as caught:
        await repository(client).create_device_integration(device_integration(), receipt())
    assert RAW_IDENTITY not in str(caught.value)
    assert isinstance(caught.value.__cause__, DynamoConditionalConflict)


@pytest.mark.asyncio
async def test_receipt_collision_is_reported_as_idempotency_conflict() -> None:
    client = FakeDynamoClient.rejecting(
        "None", "ConditionalCheckFailed", "ConditionalCheckFailed", "None", "None"
    )
    with pytest.raises(ConflictError, match="idempotency key already used"):
        await repository(client).create_device_integration(device_integration(), receipt())


@pytest.mark.asyncio
async def test_account_no_longer_provisionable_at_commit_is_conflict() -> None:
    client = FakeDynamoClient.rejecting("None", "None", "None", "ConditionalCheckFailed", "None")
    with pytest.raises(ConflictError, match="integration account is not provisionable"):
        await repository(client).create_device_integration(device_integration(), receipt())


@pytest.mark.asyncio
async def test_device_decommissioned_at_commit_is_conflict() -> None:
    client = FakeDynamoClient.rejecting("None", "None", "None", "None", "ConditionalCheckFailed")
    with pytest.raises(ConflictError, match="device is decommissioned"):
        await repository(client).create_device_integration(device_integration(), receipt())


@pytest.mark.asyncio
async def test_receipt_failure_is_classified_before_parent_checks() -> None:
    client = FakeDynamoClient.rejecting(
        "None",
        "ConditionalCheckFailed",
        "ConditionalCheckFailed",
        "ConditionalCheckFailed",
        "ConditionalCheckFailed",
    )
    with pytest.raises(ConflictError, match="idempotency key already used"):
        await repository(client).create_device_integration(device_integration(), receipt())


@pytest.mark.asyncio
async def test_device_integration_row_collision_is_conflict() -> None:
    client = FakeDynamoClient.rejecting("ConditionalCheckFailed", "None", "None", "None", "None")
    with pytest.raises(ConflictError, match="device integration already exists"):
        await repository(client).create_device_integration(device_integration(), receipt())


@pytest.mark.asyncio
async def test_non_conditional_transaction_failure_is_not_swallowed() -> None:
    client = FakeDynamoClient.rejecting("None", "TransactionConflict", "None", "None", "None")
    with pytest.raises(ClientError):
        await repository(client).create_device_integration(device_integration(), receipt())


@pytest.mark.asyncio
async def test_integration_account_create_transacts_row_and_receipt() -> None:
    client = FakeDynamoClient()
    created = await repository(client).create_integration_account(
        account(), receipt("integration-account-create")
    )

    assert created == account()
    assert transaction_item_keys(client) == [
        (f"TENANT#{TENANT}", f"INTEGRATION_ACCOUNT#{ACCOUNT_ID}"),
        (f"TENANT#{TENANT}", f"IDEMPOTENCY#integration-account-create#{KEY_HASH}"),
    ]
    row = transaction_items(client)[0]
    assert row["entity_type"] == "integration_account"
    assert row["status"] == "pending"
    assert row["credential_reference"] == RAW_CREDENTIAL_REFERENCE
    assert "public_identifier" not in row


@pytest.mark.asyncio
async def test_integration_account_receipt_collision_is_idempotency_conflict() -> None:
    client = FakeDynamoClient.rejecting("None", "ConditionalCheckFailed")
    with pytest.raises(ConflictError, match="idempotency key already used"):
        await repository(client).create_integration_account(
            account(), receipt("integration-account-create")
        )


@pytest.mark.asyncio
async def test_get_integration_account_round_trips_tenant_row() -> None:
    client = FakeDynamoClient()
    await repository(client).create_integration_account(
        account(status=IntegrationAccountStatus.ACTIVE), receipt("integration-account-create")
    )
    client.seed(transaction_items(client)[0])

    loaded = await repository(client).get_integration_account(TENANT, ACCOUNT_ID)

    assert loaded == account(status=IntegrationAccountStatus.ACTIVE)
    get_call = [kwargs for name, kwargs in client.calls if name == "get_item"][-1]
    assert get_call["ConsistentRead"] is True
    assert get_call["Key"] == {
        "PK": {"S": f"TENANT#{TENANT}"},
        "SK": {"S": f"INTEGRATION_ACCOUNT#{ACCOUNT_ID}"},
    }


@pytest.mark.asyncio
async def test_get_integration_account_is_tenant_scoped() -> None:
    client = FakeDynamoClient()
    await repository(client).create_integration_account(
        account(), receipt("integration-account-create")
    )
    client.seed(transaction_items(client)[0])

    assert await repository(client).get_integration_account("tnt_b", ACCOUNT_ID) is None
    assert await repository(client).get_integration_account(TENANT, "ia_" + "9" * 32) is None


@pytest.mark.asyncio
async def test_list_integration_accounts_queries_tenant_prefix_with_pagination() -> None:
    serializer = TypeSerializer()
    client = FakeDynamoClient()
    await repository(client).create_integration_account(
        account(), receipt("integration-account-create")
    )
    row = transaction_items(client)[0]
    second = {**row, "SK": "INTEGRATION_ACCOUNT#ia_" + "4" * 32}
    second["integration_account_id"] = "ia_" + "4" * 32
    client.query_pages = [
        {
            "Items": [{k: serializer.serialize(v) for k, v in row.items()}],
            "LastEvaluatedKey": {"PK": {"S": "x"}, "SK": {"S": "y"}},
        },
        {"Items": [{k: serializer.serialize(v) for k, v in second.items()}]},
    ]

    accounts = await repository(client).list_integration_accounts(TENANT)

    assert [item.integration_account_id for item in accounts] == [ACCOUNT_ID, "ia_" + "4" * 32]
    queries = [kwargs for name, kwargs in client.calls if name == "query"]
    assert len(queries) == 2
    assert all(query["ConsistentRead"] is True for query in queries)
    assert queries[0]["ExpressionAttributeValues"] == {
        ":pk": {"S": f"TENANT#{TENANT}"},
        ":sk_prefix": {"S": "INTEGRATION_ACCOUNT#"},
    }
    assert queries[1]["ExclusiveStartKey"] == {"PK": {"S": "x"}, "SK": {"S": "y"}}


@pytest.mark.asyncio
async def test_list_device_integrations_filters_by_device_across_pages() -> None:
    serializer = TypeSerializer()
    client = FakeDynamoClient()
    await repository(client).create_device_integration(device_integration(), receipt())
    row = transaction_items(client)[0]
    other_device = {
        **row,
        "SK": "DEVICE_INTEGRATION#di_" + "5" * 32,
        "device_integration_id": "di_" + "5" * 32,
        "device_id": "local-device-001",
    }
    client.query_pages = [
        {
            "Items": [serializer.serialize(other_device)["M"]],
            "LastEvaluatedKey": {"PK": {"S": "x"}, "SK": {"S": "y"}},
        },
        {"Items": [serializer.serialize(row)["M"]]},
    ]

    integrations = await repository(client).list_device_integrations(TENANT, DEVICE_ID)

    assert integrations == [device_integration()]
    queries = [kwargs for name, kwargs in client.calls if name == "query"]
    assert len(queries) == 2
    assert all(query["ConsistentRead"] is True for query in queries)
    assert queries[0]["ExpressionAttributeValues"][":sk_prefix"] == {"S": "DEVICE_INTEGRATION#"}


@pytest.mark.asyncio
async def test_device_integration_round_trips_with_historical_device_id() -> None:
    serializer = TypeSerializer()
    client = FakeDynamoClient()
    legacy = device_integration(
        device_id="local-device-001",
        status=DeviceIntegrationStatus.ACTIVE,
        updated_at=NOW + timedelta(minutes=1),
        version=2,
    )
    await repository(client).create_device_integration(legacy, receipt())
    client.query_pages = [{"Items": [serializer.serialize(transaction_items(client)[0])["M"]]}]

    assert await repository(client).list_device_integrations(TENANT, "local-device-001") == [legacy]
