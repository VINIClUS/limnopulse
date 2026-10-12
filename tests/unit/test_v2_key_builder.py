import json
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256

import pytest
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError

from limnopulse_api.adapters.dynamodb import DynamoKeyBuilder
from limnopulse_api.adapters.dynamodb_v2 import (
    DynamoConditionalConflict,
    DynamoV2KeyBuilder,
    DynamoV2RepositoryBase,
    canonical_request_hash,
    canonical_timestamp_key,
    conditional_failure_indexes,
    external_identity_hash,
    idempotency_key_hash,
    idempotency_receipt_from_item,
    idempotency_receipt_to_item,
    parse_canonical_timestamp,
)
from limnopulse_api.core.errors import ConflictError
from limnopulse_api.domain.v2_common import new_idempotency_receipt

NOW = datetime(2026, 8, 25, 12, 0, 0, 500000, tzinfo=UTC)
HEX_A = "a" * 64
HEX_B = "b" * 64


class FakeDynamoClient:
    def __init__(self) -> None:
        self.serializer = TypeSerializer()
        self.deserializer = TypeDeserializer()
        self.items: dict[tuple[str, str], dict] = {}
        self.calls: list[tuple[str, dict]] = []
        self.query_pages: list[dict] | None = None
        self.fail_with: Exception | None = None

    def seed(self, item: dict) -> None:
        self.items[(item["PK"], item["SK"])] = {
            key: self.serializer.serialize(value) for key, value in item.items()
        }

    def _maybe_fail(self) -> None:
        if self.fail_with is not None:
            error, self.fail_with = self.fail_with, None
            raise error

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

    def put_item(self, **kwargs):
        self.calls.append(("put_item", kwargs))
        self._maybe_fail()
        return {}

    def update_item(self, **kwargs):
        self.calls.append(("update_item", kwargs))
        self._maybe_fail()
        return {"Attributes": {"version": {"N": "3"}}}

    def transact_write_items(self, **kwargs):
        self.calls.append(("transact_write_items", kwargs))
        self._maybe_fail()
        return {}

    def __getattr__(self, name: str):
        raise AssertionError(f"unexpected client call: {name}")


def client_error(code: str, operation: str = "TransactWriteItems", reasons=None) -> ClientError:
    response: dict = {"Error": {"Code": code, "Message": code}}
    if reasons is not None:
        response["CancellationReasons"] = [
            {"Code": reason} if reason is not None else {} for reason in reasons
        ]
    return ClientError(response, operation)


def cancelled(*reasons) -> ClientError:
    return client_error("TransactionCanceledException", reasons=list(reasons))


def build_base(client=None, now: datetime = NOW) -> DynamoV2RepositoryBase:
    return DynamoV2RepositoryBase("domain", client or FakeDynamoClient(), clock=lambda: now)


def plain(serialized: dict) -> dict:
    deserializer = TypeDeserializer()
    return {key: deserializer.deserialize(value) for key, value in serialized.items()}


def receipt(created_at: datetime = NOW, request_hash: str = HEX_B):
    return new_idempotency_receipt(
        tenant_id="tnt_a",
        scope="sites.create",
        key_hash=HEX_A,
        request_hash=request_hash,
        response_snapshot_json='{"site_id":"site_1"}',
        created_at=created_at,
    )


# --- keys ------------------------------------------------------------------------------------


def test_every_frozen_key_shape() -> None:
    keys = DynamoV2KeyBuilder()
    started = datetime(2026, 8, 25, 12, 30, 5, 42, tzinfo=UTC)

    assert keys.site("tnt_1", "site_1") == {"PK": "TENANT#tnt_1", "SK": "SITE#site_1"}
    assert keys.asset("tnt_1", "ast_1") == {"PK": "TENANT#tnt_1", "SK": "ASSET#ast_1"}
    assert keys.pond_profile("ast_1") == {"PK": "ASSET#ast_1", "SK": "PROFILE#POND"}
    assert keys.device("tnt_1", "dev_1") == {"PK": "TENANT#tnt_1", "SK": "DEVICE#dev_1"}
    assert keys.device_lookup("dev_1") == {"PK": "DEVICE#dev_1", "SK": "META"}
    assert keys.component_mirror("tnt_1", "cmp_1") == {
        "PK": "TENANT#tnt_1",
        "SK": "COMPONENT#cmp_1",
    }
    assert keys.component("cmp_1") == {"PK": "COMPONENT#cmp_1", "SK": "META"}
    assert keys.capability("cmp_1", "cap_1") == {
        "PK": "COMPONENT#cmp_1",
        "SK": "CAPABILITY#cap_1",
    }
    assert keys.integration_account("tnt_1", "ia_1") == {
        "PK": "TENANT#tnt_1",
        "SK": "INTEGRATION_ACCOUNT#ia_1",
    }
    assert keys.device_integration("tnt_1", "di_1") == {
        "PK": "TENANT#tnt_1",
        "SK": "DEVICE_INTEGRATION#di_1",
    }
    assert keys.external_identity("vendor.cloud", HEX_A) == {
        "PK": f"EXTERNAL_ID#vendor.cloud#{HEX_A}",
        "SK": "META",
    }
    assert keys.deployment_mirror("tnt_1", "dep_1") == {
        "PK": "TENANT#tnt_1",
        "SK": "DEPLOYMENT#dep_1",
    }
    assert keys.deployment_history("cmp_1", started, "dep_1") == {
        "PK": "COMPONENT#cmp_1",
        "SK": "DEPLOYMENT#2026-08-25T12:30:05.000042Z#dep_1",
    }
    assert keys.current_deployment("cmp_1") == {
        "PK": "COMPONENT#cmp_1",
        "SK": "DEPLOYMENT#CURRENT",
    }
    assert keys.idempotency_receipt("tnt_1", "sites.create", HEX_A) == {
        "PK": "TENANT#tnt_1",
        "SK": f"IDEMPOTENCY#sites.create#{HEX_A}",
    }
    assert keys.migration_receipt("tnt_1") == {
        "PK": "TENANT#tnt_1",
        "SK": "MIGRATION#M1#DEFAULT_SITE_ASSET_DEVICE",
    }


def test_deployment_keys_are_ordered_and_component_scoped() -> None:
    keys = DynamoV2KeyBuilder()
    started = datetime(2026, 8, 25, 12, 30, 5, 42, tzinfo=UTC)
    assert keys.current_deployment("cmp_1") == {"PK": "COMPONENT#cmp_1", "SK": "DEPLOYMENT#CURRENT"}
    assert keys.deployment_history("cmp_1", started, "dep_1") == {
        "PK": "COMPONENT#cmp_1",
        "SK": "DEPLOYMENT#2026-08-25T12:30:05.000042Z#dep_1",
    }


def test_legacy_keys_match_v1_adapter_formats() -> None:
    keys = DynamoV2KeyBuilder()
    legacy = DynamoKeyBuilder()

    assert keys.legacy_pond("tnt_1", "pond_1") == legacy.pond("tnt_1", "pond_1")
    assert keys.legacy_device("tnt_1", "local-device-001") == legacy.device(
        "tnt_1", "local-device-001"
    )
    assert keys.device_lookup("local-device-001") == legacy.device_lookup("local-device-001")
    assert keys.tenant_partition("tnt_1") == legacy.tenant("tnt_1")["PK"]


def test_sort_key_prefixes_do_not_overlap() -> None:
    keys = DynamoV2KeyBuilder()
    tenant_prefixes = [
        keys.SITE_PREFIX,
        keys.ASSET_PREFIX,
        keys.DEVICE_PREFIX,
        keys.COMPONENT_PREFIX,
        keys.INTEGRATION_ACCOUNT_PREFIX,
        keys.DEVICE_INTEGRATION_PREFIX,
        keys.DEPLOYMENT_PREFIX,
        keys.LEGACY_POND_PREFIX,
    ]
    samples = {
        keys.SITE_PREFIX: keys.site("t", "x")["SK"],
        keys.ASSET_PREFIX: keys.asset("t", "x")["SK"],
        keys.DEVICE_PREFIX: keys.device("t", "x")["SK"],
        keys.COMPONENT_PREFIX: keys.component_mirror("t", "x")["SK"],
        keys.INTEGRATION_ACCOUNT_PREFIX: keys.integration_account("t", "x")["SK"],
        keys.DEVICE_INTEGRATION_PREFIX: keys.device_integration("t", "x")["SK"],
        keys.DEPLOYMENT_PREFIX: keys.deployment_mirror("t", "x")["SK"],
        keys.LEGACY_POND_PREFIX: keys.legacy_pond("t", "x")["SK"],
    }
    for prefix, sk in samples.items():
        matching = [candidate for candidate in tenant_prefixes if sk.startswith(candidate)]
        assert matching == [prefix]
    assert keys.capability("c", "x")["SK"].startswith(keys.CAPABILITY_PREFIX)


def test_deployment_history_bounds_exclude_current_pointer() -> None:
    keys = DynamoV2KeyBuilder()
    lower, upper = keys.deployment_history_bounds()
    history = keys.deployment_history("cmp_1", NOW, "dep_" + "f" * 32)["SK"]
    current = keys.current_deployment("cmp_1")["SK"]

    assert lower <= history <= upper
    assert not lower <= current <= upper


def test_deployment_history_upper_bound_includes_interval_starting_at_instant() -> None:
    keys = DynamoV2KeyBuilder()
    at_instant = keys.deployment_history("cmp_1", NOW, "dep_" + "f" * 32)["SK"]
    later = keys.deployment_history("cmp_1", NOW + timedelta(microseconds=1), "dep_0")["SK"]
    bound = keys.deployment_history_upper_bound(NOW)

    assert at_instant <= bound < later
    assert keys.deployment_history_bounds()[0] <= bound


@pytest.mark.parametrize("bad", ["", "a#b", None])
def test_key_parts_reject_separator_and_empty_values(bad) -> None:
    keys = DynamoV2KeyBuilder()
    with pytest.raises(ValueError):
        keys.site("tnt_1", bad)
    with pytest.raises(ValueError):
        keys.idempotency_receipt(bad, "sites.create", HEX_A)


# --- timestamps and hashes -------------------------------------------------------------------


def test_canonical_timestamp_key_is_fixed_width_utc() -> None:
    assert canonical_timestamp_key(datetime(2026, 8, 25, 12, 30, 5, tzinfo=UTC)) == (
        "2026-08-25T12:30:05.000000Z"
    )
    offset = timezone(timedelta(hours=-3))
    assert canonical_timestamp_key(datetime(2026, 8, 25, 21, 0, 0, 1, tzinfo=offset)) == (
        "2026-08-26T00:00:00.000001Z"
    )
    assert canonical_timestamp_key(datetime(999, 1, 1, tzinfo=UTC)) == (
        "0999-01-01T00:00:00.000000Z"
    )


def test_canonical_timestamp_key_rejects_naive_datetimes() -> None:
    with pytest.raises(ValueError):
        canonical_timestamp_key(datetime(2026, 8, 25))  # noqa: DTZ001


def test_canonical_timestamp_order_is_chronological() -> None:
    offset = timezone(timedelta(hours=5))
    instants = [
        datetime(2026, 8, 25, 12, 0, 0, 1, tzinfo=UTC),
        datetime(2026, 8, 25, 12, 0, 0, tzinfo=UTC),
        datetime(2026, 8, 25, 16, 59, 59, 999999, tzinfo=offset),
        datetime(2025, 12, 31, 23, 59, 59, tzinfo=UTC),
        datetime(2026, 8, 25, 12, 0, 0, 10, tzinfo=UTC),
    ]
    assert sorted(instants, key=canonical_timestamp_key) == sorted(instants)


def test_parse_canonical_timestamp_round_trips() -> None:
    assert parse_canonical_timestamp(canonical_timestamp_key(NOW)) == NOW
    assert parse_canonical_timestamp("2026-08-25T12:00:00.500000Z").tzinfo is not None


def test_external_identity_hash_vector() -> None:
    assert external_identity_hash("vendor.cloud", "abc") == (
        sha256(b"vendor.cloud\x00abc").hexdigest()
    )


def test_external_identity_hash_is_provider_scoped() -> None:
    assert external_identity_hash("vendor.cloud", "abc") != external_identity_hash(
        "native.https", "abc"
    )


def test_external_identity_hash_rejects_invalid_provider_key() -> None:
    with pytest.raises(ValueError):
        external_identity_hash("Vendor", "abc")
    with pytest.raises(ValueError):
        external_identity_hash("vendor", "")


def test_idempotency_key_hash_vector_and_length_bounds() -> None:
    assert idempotency_key_hash("key-0001") == sha256(b"key-0001").hexdigest()
    assert idempotency_key_hash("k" * 128) == sha256(b"k" * 128).hexdigest()
    with pytest.raises(ValueError):
        idempotency_key_hash("k" * 7)
    with pytest.raises(ValueError):
        idempotency_key_hash("k" * 129)


def test_canonical_request_hash_uses_sorted_compact_json() -> None:
    payload = {"b": [1, 2], "a": {"y": "ç", "x": None}}

    expected = sha256(b'{"a":{"x":null,"y":"\\u00e7"},"b":[1,2]}').hexdigest()
    assert canonical_request_hash(payload) == expected
    assert canonical_request_hash({"a": {"x": None, "y": "ç"}, "b": [1, 2]}) == expected
    assert canonical_request_hash({"b": [2, 1], "a": {"y": "ç", "x": None}}) != expected


def test_canonical_request_hash_rejects_non_json_values() -> None:
    with pytest.raises(ValueError):
        canonical_request_hash({"a": float("nan")})
    with pytest.raises(TypeError):
        canonical_request_hash({"a": {1, 2}})


# --- base adapter ----------------------------------------------------------------------------


def test_constructor_does_not_touch_client() -> None:
    client = object()
    base = DynamoV2RepositoryBase("domain", client)

    assert base.table_name == "domain"
    assert base.client is client
    assert isinstance(base.keys, DynamoV2KeyBuilder)


def test_base_exposes_only_generic_primitives_and_no_scan() -> None:
    public = {name for name in dir(DynamoV2RepositoryBase) if not name.startswith("_")}

    assert "scan" not in public
    assert not any("scan" in name for name in public)
    assert public == {
        "condition_check_request",
        "deserialize_item",
        "get_idempotency_replay",
        "get_idempotency_replay_sync",
        "get_item",
        "get_item_sync",
        "put_item",
        "put_item_sync",
        "put_new_request",
        "put_request",
        "put_versioned_request",
        "query_prefix",
        "query_prefix_sync",
        "query_range",
        "query_range_sync",
        "receipt_put_request",
        "serialize_item",
        "serialize_values",
        "transact_write",
        "transact_write_sync",
        "update_item",
        "update_item_sync",
        "update_request",
        "version_check_request",
    }


def test_get_item_serializes_key_and_normalizes_numbers() -> None:
    client = FakeDynamoClient()
    client.seed(
        {
            "PK": "TENANT#t",
            "SK": "SITE#s",
            "version": Decimal("3"),
            "ratio": Decimal("0.5"),
            "nested": {"values": [Decimal("1"), Decimal("1.25")]},
        }
    )
    base = build_base(client)

    item = base.get_item_sync({"PK": "TENANT#t", "SK": "SITE#s"}, consistent=True)

    assert item == {
        "PK": "TENANT#t",
        "SK": "SITE#s",
        "version": 3,
        "ratio": 0.5,
        "nested": {"values": [1, 1.25]},
    }
    assert isinstance(item["version"], int)
    name, call = client.calls[0]
    assert name == "get_item"
    assert call["TableName"] == "domain"
    assert call["Key"] == {"PK": {"S": "TENANT#t"}, "SK": {"S": "SITE#s"}}
    assert call["ConsistentRead"] is True
    assert base.get_item_sync({"PK": "TENANT#t", "SK": "SITE#missing"}) is None


def test_serialize_item_converts_floats_and_tuples() -> None:
    base = build_base()
    serialized = base.serialize_item({"ratio": 0.1, "keys": ("a", "b"), "n": {"x": 1.5}})

    assert plain(serialized) == {"ratio": Decimal("0.1"), "keys": ["a", "b"], "n": {"x": 1.5}}
    assert plain(serialized)["n"]["x"] == Decimal("1.5")


def test_query_prefix_paginates_until_last_evaluated_key_is_absent() -> None:
    client = FakeDynamoClient()
    serializer = TypeSerializer()
    page_key = {"PK": serializer.serialize("TENANT#t"), "SK": serializer.serialize("SITE#a")}
    client.query_pages = [
        {
            "Items": [{"PK": {"S": "TENANT#t"}, "SK": {"S": "SITE#a"}}],
            "LastEvaluatedKey": page_key,
        },
        {"Items": [{"PK": {"S": "TENANT#t"}, "SK": {"S": "SITE#b"}}]},
    ]
    base = build_base(client)

    items = base.query_prefix_sync("TENANT#t", "SITE#", consistent=True)

    assert [item["SK"] for item in items] == ["SITE#a", "SITE#b"]
    first, second = (call for name, call in client.calls)
    assert first["KeyConditionExpression"] == "PK = :pk AND begins_with(SK, :sk_prefix)"
    assert plain(first["ExpressionAttributeValues"]) == {":pk": "TENANT#t", ":sk_prefix": "SITE#"}
    assert first["ConsistentRead"] is True
    assert "ExclusiveStartKey" not in first
    assert second["ExclusiveStartKey"] == page_key


def test_query_range_builds_between_and_bounded_conditions() -> None:
    client = FakeDynamoClient()
    base = build_base(client)

    base.query_range_sync("COMPONENT#c", lower="DEPLOYMENT#0", upper="DEPLOYMENT#:")
    base.query_range_sync("COMPONENT#c", upper="DEPLOYMENT#x", ascending=False, limit=1)
    base.query_range_sync("COMPONENT#c", lower="DEPLOYMENT#a")

    between, upper_only, lower_only = (call for name, call in client.calls)
    assert between["KeyConditionExpression"] == "PK = :pk AND SK BETWEEN :sk_lower AND :sk_upper"
    assert plain(between["ExpressionAttributeValues"]) == {
        ":pk": "COMPONENT#c",
        ":sk_lower": "DEPLOYMENT#0",
        ":sk_upper": "DEPLOYMENT#:",
    }
    assert between["ScanIndexForward"] is True
    assert upper_only["KeyConditionExpression"] == "PK = :pk AND SK <= :sk_upper"
    assert upper_only["ScanIndexForward"] is False
    assert upper_only["Limit"] == 1
    assert lower_only["KeyConditionExpression"] == "PK = :pk AND SK >= :sk_lower"
    with pytest.raises(ValueError):
        base.query_range_sync("COMPONENT#c")


def test_query_range_limit_stops_pagination_once_satisfied() -> None:
    client = FakeDynamoClient()
    serializer = TypeSerializer()
    client.query_pages = [
        {
            "Items": [],
            "LastEvaluatedKey": {"PK": serializer.serialize("P"), "SK": serializer.serialize("A")},
        },
        {
            "Items": [{"PK": {"S": "P"}, "SK": {"S": "B"}}],
            "LastEvaluatedKey": {"PK": serializer.serialize("P"), "SK": serializer.serialize("B")},
        },
    ]
    base = build_base(client)

    items = base.query_range_sync("P", upper="Z", ascending=False, limit=1)

    assert [item["SK"] for item in items] == ["B"]
    assert len(client.calls) == 2


def test_put_builders_use_frozen_conditions() -> None:
    base = build_base()

    new = base.put_new_request({"PK": "TENANT#t", "SK": "SITE#s", "version": 1})
    assert new["TableName"] == "domain"
    assert new["ConditionExpression"] == "attribute_not_exists(PK)"
    assert plain(new["Item"]) == {"PK": "TENANT#t", "SK": "SITE#s", "version": 1}

    versioned = base.put_versioned_request({"PK": "TENANT#t", "SK": "SITE#s", "version": 3}, 2)
    assert (
        versioned["ConditionExpression"] == "attribute_exists(PK) AND #version = :expected_version"
    )
    assert versioned["ExpressionAttributeNames"] == {"#version": "version"}
    assert plain(versioned["ExpressionAttributeValues"]) == {":expected_version": 2}

    check = base.version_check_request({"PK": "ASSET#a", "SK": "PROFILE#POND"}, 4)
    assert check["Key"] == {"PK": {"S": "ASSET#a"}, "SK": {"S": "PROFILE#POND"}}
    assert check["ConditionExpression"] == "attribute_exists(PK) AND #version = :expected_version"
    assert plain(check["ExpressionAttributeValues"]) == {":expected_version": 4}

    update = base.update_request(
        {"PK": "TENANT#t", "SK": "POND#p"},
        update_expression="SET #status = :status",
        condition="attribute_exists(PK)",
        names={"#status": "status"},
        values={":status": "archived"},
    )
    assert update["UpdateExpression"] == "SET #status = :status"
    assert update["ConditionExpression"] == "attribute_exists(PK)"
    assert plain(update["ExpressionAttributeValues"]) == {":status": "archived"}

    custom = base.condition_check_request(
        {"PK": "COMPONENT#c", "SK": "META"}, condition="attribute_exists(PK)"
    )
    assert "ExpressionAttributeValues" not in custom
    assert "ExpressionAttributeNames" not in custom


def test_put_and_update_map_conditional_failure_to_conflict() -> None:
    client = FakeDynamoClient()
    base = build_base(client)

    client.fail_with = client_error("ConditionalCheckFailedException", "PutItem")
    with pytest.raises(ConflictError) as put_error:
        base.put_item_sync(base.put_new_request({"PK": "P", "SK": "S"}))
    assert isinstance(put_error.value, DynamoConditionalConflict)
    assert put_error.value.failed_indexes == (0,)
    assert isinstance(put_error.value.__cause__, ClientError)

    client.fail_with = client_error("ConditionalCheckFailedException", "UpdateItem")
    with pytest.raises(ConflictError):
        base.update_item_sync(
            base.update_request({"PK": "P", "SK": "S"}, update_expression="SET a = :a")
        )


def test_update_item_returns_normalized_attributes() -> None:
    client = FakeDynamoClient()
    base = build_base(client)

    request = base.update_request(
        {"PK": "P", "SK": "S"}, update_expression="SET a = :a", return_values="ALL_NEW"
    )

    assert request["ReturnValues"] == "ALL_NEW"
    assert base.update_item_sync(request) == {"version": 3}
    assert client.calls[-1][1]["ReturnValues"] == "ALL_NEW"


def test_update_request_omits_return_values_by_default() -> None:
    base = build_base()

    request = base.update_request({"PK": "P", "SK": "S"}, update_expression="SET a = :a")

    assert "ReturnValues" not in request


def test_get_item_normalizes_huge_integers_fractions_and_number_sets() -> None:
    client = FakeDynamoClient()
    client.seed(
        {
            "PK": "P",
            "SK": "S",
            "huge": Decimal("1E+30"),
            "fraction": Decimal("2.5"),
            "numbers": {Decimal("1E+30"), Decimal("1.5"), Decimal("7")},
        }
    )
    base = build_base(client)

    item = base.get_item_sync({"PK": "P", "SK": "S"})

    assert item["huge"] == 10**30
    assert isinstance(item["huge"], int)
    assert item["fraction"] == 2.5
    assert isinstance(item["fraction"], float)
    assert item["numbers"] == {10**30, 1.5, 7}
    assert not any(isinstance(value, Decimal) for value in item["numbers"])


@pytest.mark.parametrize(
    "code", ["ThrottlingException", "ProvisionedThroughputExceededException", "ValidationException"]
)
def test_put_infrastructure_errors_are_reraised_unchanged(code: str) -> None:
    client = FakeDynamoClient()
    base = build_base(client)
    error = client_error(code, "PutItem")
    client.fail_with = error

    with pytest.raises(ClientError) as raised:
        base.put_item_sync(base.put_new_request({"PK": "P", "SK": "S"}))
    assert raised.value is error


def test_transaction_conditional_failure_maps_to_conflict_with_indexes() -> None:
    client = FakeDynamoClient()
    base = build_base(client)
    client.fail_with = cancelled(None, "ConditionalCheckFailed", "None")

    actions = [{"Put": base.put_new_request({"PK": "P", "SK": str(i)})} for i in range(3)]
    with pytest.raises(ConflictError) as raised:
        base.transact_write_sync(actions)

    assert raised.value.failed_indexes == (1,)
    name, call = client.calls[0]
    assert name == "transact_write_items"
    assert call["TransactItems"] == actions


def test_transaction_mixed_reasons_with_conditional_failure_map_to_conflict() -> None:
    client = FakeDynamoClient()
    base = build_base(client)
    client.fail_with = cancelled("TransactionConflict", "ConditionalCheckFailed", "None")

    with pytest.raises(ConflictError) as raised:
        base.transact_write_sync([{"Put": base.put_new_request({"PK": "P", "SK": "S"})}])
    assert raised.value.failed_indexes == (1,)


@pytest.mark.parametrize(
    "reasons",
    [
        ("TransactionConflict", "None"),
        ("None", "ProvisionedThroughputExceeded"),
        ("ThrottlingError",),
        ("ValidationError", None),
        (),
    ],
)
def test_transaction_cancellation_without_conditional_failure_is_infrastructure(
    reasons,
) -> None:
    client = FakeDynamoClient()
    base = build_base(client)
    error = cancelled(*reasons)
    client.fail_with = error

    with pytest.raises(ClientError) as raised:
        base.transact_write_sync([{"Put": base.put_new_request({"PK": "P", "SK": "S"})}])
    assert raised.value is error
    assert not isinstance(raised.value, ConflictError)


@pytest.mark.parametrize(
    "code",
    ["TransactionInProgressException", "ThrottlingException", "InternalServerError"],
)
def test_transaction_other_errors_are_reraised_unchanged(code: str) -> None:
    client = FakeDynamoClient()
    base = build_base(client)
    error = client_error(code)
    client.fail_with = error

    with pytest.raises(ClientError) as raised:
        base.transact_write_sync([{"Put": base.put_new_request({"PK": "P", "SK": "S"})}])
    assert raised.value is error


def test_transaction_rejects_empty_or_oversized_batches() -> None:
    base = build_base()
    with pytest.raises(ValueError):
        base.transact_write_sync([])
    with pytest.raises(ValueError):
        base.transact_write_sync([{"ConditionCheck": {}}] * 101)


def test_conditional_failure_indexes_helper() -> None:
    assert conditional_failure_indexes(client_error("ConditionalCheckFailedException")) == (0,)
    assert conditional_failure_indexes(
        cancelled("ConditionalCheckFailed", None, "ConditionalCheckFailed")
    ) == (0, 2)
    assert conditional_failure_indexes(cancelled("TransactionConflict")) == ()
    assert conditional_failure_indexes(client_error("ThrottlingException")) == ()
    assert conditional_failure_indexes(ValueError("x")) == ()
    conflict = DynamoConditionalConflict((1, 3))
    assert conditional_failure_indexes(conflict) == (1, 3)


# --- idempotency receipts --------------------------------------------------------------------


def test_receipt_item_stores_hashed_key_and_ttl_epoch() -> None:
    item = idempotency_receipt_to_item(receipt())

    assert item == {
        "PK": "TENANT#tnt_a",
        "SK": f"IDEMPOTENCY#sites.create#{HEX_A}",
        "entity_type": "idempotency_receipt",
        "tenant_id": "tnt_a",
        "scope": "sites.create",
        "key_hash": HEX_A,
        "request_hash": HEX_B,
        "response_snapshot_json": '{"site_id":"site_1"}',
        "created_at": "2026-08-25T12:00:00.500000Z",
        "expires_at": int((NOW + timedelta(hours=24)).timestamp()),
    }
    assert isinstance(item["expires_at"], int)
    assert idempotency_receipt_from_item(item) == receipt()


def test_receipt_put_request_allows_only_absent_or_expired_receipts() -> None:
    base = build_base()

    request = base.receipt_put_request(receipt())

    assert request["ConditionExpression"] == "attribute_not_exists(PK) OR #expires_at <= :now"
    assert request["ExpressionAttributeNames"] == {"#expires_at": "expires_at"}
    assert plain(request["ExpressionAttributeValues"]) == {":now": int(NOW.timestamp())}
    assert plain(request["Item"])["expires_at"] == int((NOW + timedelta(hours=24)).timestamp())


def test_get_idempotency_replay_returns_snapshot_for_same_request() -> None:
    client = FakeDynamoClient()
    client.seed(idempotency_receipt_to_item(receipt()))
    base = build_base(client, now=NOW + timedelta(hours=1))

    replay = base.get_idempotency_replay_sync("tnt_a", "sites.create", HEX_A, HEX_B)

    assert replay == '{"site_id":"site_1"}'
    name, call = client.calls[0]
    assert call["ConsistentRead"] is True
    assert plain(call["Key"]) == {"PK": "TENANT#tnt_a", "SK": f"IDEMPOTENCY#sites.create#{HEX_A}"}


def test_get_idempotency_replay_absent_receipt_returns_none() -> None:
    base = build_base()
    assert base.get_idempotency_replay_sync("tnt_a", "sites.create", HEX_A, HEX_B) is None


def test_get_idempotency_replay_rejects_different_request_with_conflict() -> None:
    client = FakeDynamoClient()
    client.seed(idempotency_receipt_to_item(receipt()))
    base = build_base(client)

    with pytest.raises(ConflictError):
        base.get_idempotency_replay_sync("tnt_a", "sites.create", HEX_A, "c" * 64)


@pytest.mark.parametrize("age", [timedelta(hours=24), timedelta(hours=25)])
def test_get_idempotency_replay_treats_expired_receipt_as_absent(age: timedelta) -> None:
    client = FakeDynamoClient()
    client.seed(idempotency_receipt_to_item(receipt()))
    base = build_base(client, now=NOW + age)

    assert base.get_idempotency_replay_sync("tnt_a", "sites.create", HEX_A, "c" * 64) is None


@pytest.mark.asyncio
async def test_async_wrappers_delegate_to_sync_primitives() -> None:
    client = FakeDynamoClient()
    client.seed(idempotency_receipt_to_item(receipt()))
    base = build_base(client)

    assert await base.get_item({"PK": "TENANT#none", "SK": "X"}) is None
    assert await base.query_prefix("TENANT#t", "SITE#") == []
    assert await base.query_range("COMPONENT#c", upper="DEPLOYMENT#x") == []
    await base.put_item(base.put_new_request({"PK": "P", "SK": "S"}))
    await base.update_item(
        base.update_request({"PK": "P", "SK": "S"}, update_expression="SET a=:a")
    )
    await base.transact_write([{"Put": base.receipt_put_request(receipt())}])
    assert (
        await base.get_idempotency_replay("tnt_a", "sites.create", HEX_A, HEX_B)
        == '{"site_id":"site_1"}'
    )
    client.fail_with = cancelled("ConditionalCheckFailed")
    with pytest.raises(ConflictError):
        await base.transact_write([{"Put": base.receipt_put_request(receipt())}])
    snapshot = json.loads(
        await base.get_idempotency_replay("tnt_a", "sites.create", HEX_A, HEX_B) or "{}"
    )
    assert snapshot == {"site_id": "site_1"}
