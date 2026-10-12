from datetime import UTC, datetime

import pytest
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError

from limnopulse_api.adapters.dynamodb_sites_assets import (
    DynamoSiteAssetRepository,
    asset_from_item,
    asset_to_item,
    pond_profile_from_item,
    pond_profile_to_item,
    site_from_item,
    site_to_item,
)
from limnopulse_api.adapters.dynamodb_v2 import idempotency_receipt_to_item
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.assets import (
    Asset,
    AssetStatus,
    AssetType,
    AssetWithPondProfile,
    PondProfile,
    Site,
    SiteStatus,
)
from limnopulse_api.domain.v2_common import new_idempotency_receipt
from limnopulse_api.repositories.v2_contracts import SiteAssetReader, SiteAssetRepository

TABLE = "LimnopulseDomain"
NOW = datetime(2026, 8, 25, 12, 0, 0, 123456, tzinfo=UTC)
CREATED = datetime(2026, 8, 1, 9, 30, tzinfo=UTC)
TENANT = "tnt_a"
SITE_ID = "site_" + "1" * 32
ASSET_ID = "ast_" + "2" * 32
KEY_HASH = "a" * 64
REQUEST_HASH = "b" * 64

_serializer = TypeSerializer()
_deserializer = TypeDeserializer()


def to_dynamo(item: dict) -> dict:
    return {key: _serializer.serialize(value) for key, value in item.items()}


def plain(item: dict) -> dict:
    return {key: _deserializer.deserialize(value) for key, value in item.items()}


def client_error(code: str, reasons: list[str] | None = None) -> ClientError:
    response: dict = {"Error": {"Code": code, "Message": "botocore detail"}}
    if reasons is not None:
        response["CancellationReasons"] = [{"Code": reason} for reason in reasons]
    return ClientError(response, "TransactWriteItems")


class FakeDynamoClient:
    """Stores plain items by key; records writes; transactions can be scripted to fail."""

    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict] = {}
        self.get_item_calls: list[dict] = []
        self.query_calls: list[dict] = []
        self.update_item_calls: list[dict] = []
        self.transact_write_items_calls: list[dict] = []
        self.transaction_error: ClientError | None = None
        self.page_size: int | None = None

    @classmethod
    def with_asset_and_profile(
        cls, *, asset_version: int = 1, profile_version: int = 1, legacy_pond_id: str | None = None
    ) -> "FakeDynamoClient":
        client = cls()
        client.seed(asset_to_item(asset(version=asset_version)))
        client.seed(
            pond_profile_to_item(
                pond_profile(version=profile_version, legacy_pond_id=legacy_pond_id)
            )
        )
        return client

    def seed(self, item: dict) -> None:
        self.items[(item["PK"], item["SK"])] = dict(item)

    def get_item(self, **kwargs):
        self.get_item_calls.append(kwargs)
        key = plain(kwargs["Key"])
        item = self.items.get((key["PK"], key["SK"]))
        return {} if item is None else {"Item": to_dynamo(item)}

    def query(self, **kwargs):
        self.query_calls.append(kwargs)
        values = plain(kwargs["ExpressionAttributeValues"])
        matches = sorted(
            (sk, item)
            for (pk, sk), item in self.items.items()
            if pk == values[":pk"] and sk.startswith(values[":sk_prefix"])
        )
        start = 0
        if "ExclusiveStartKey" in kwargs:
            start_sk = plain(kwargs["ExclusiveStartKey"])["SK"]
            start = next(index for index, (sk, _) in enumerate(matches) if sk == start_sk) + 1
        size = self.page_size or len(matches) or 1
        page = matches[start : start + size]
        response: dict = {"Items": [to_dynamo(item) for _, item in page]}
        if start + size < len(matches):
            last = page[-1][1]
            response["LastEvaluatedKey"] = to_dynamo({"PK": last["PK"], "SK": last["SK"]})
        return response

    def update_item(self, **kwargs):
        self.update_item_calls.append(kwargs)
        key = plain(kwargs["Key"])
        current = dict(self.items[(key["PK"], key["SK"])])
        names = kwargs["ExpressionAttributeNames"]
        values = plain(kwargs["ExpressionAttributeValues"])
        for assignment in kwargs["UpdateExpression"].removeprefix("SET ").split(", "):
            name, value = assignment.split(" = ")
            current[names[name]] = values[value]
        self.items[(key["PK"], key["SK"])] = current
        return {"Attributes": to_dynamo(current)}

    def transact_write_items(self, **kwargs):
        self.transact_write_items_calls.append(kwargs)
        if self.transaction_error is not None:
            raise self.transaction_error

    def __getattr__(self, name: str):
        raise AssertionError(f"unexpected DynamoDB call: {name}")


def site(status: SiteStatus = SiteStatus.ACTIVE, version: int = 1) -> Site:
    return Site(
        tenant_id=TENANT,
        site_id=SITE_ID,
        name="Farm",
        status=status,
        created_at=CREATED,
        updated_at=CREATED,
        version=version,
    )


def asset(version: int = 1, status: AssetStatus = AssetStatus.ACTIVE) -> Asset:
    return Asset(
        tenant_id=TENANT,
        asset_id=ASSET_ID,
        site_id=SITE_ID,
        asset_type=AssetType.POND,
        name="North",
        status=status,
        created_at=CREATED,
        updated_at=CREATED,
        version=version,
    )


def pond_profile(version: int = 1, legacy_pond_id: str | None = None) -> PondProfile:
    return PondProfile(
        tenant_id=TENANT,
        asset_id=ASSET_ID,
        description="open",
        legacy_pond_id=legacy_pond_id,
        created_at=CREATED,
        updated_at=CREATED,
        version=version,
    )


def receipt(snapshot: str = "{}", scope: str = "asset-create"):
    return new_idempotency_receipt(
        tenant_id=TENANT,
        scope=scope,
        key_hash=KEY_HASH,
        request_hash=REQUEST_HASH,
        response_snapshot_json=snapshot,
        created_at=NOW,
    )


def legacy_pond_item(version: int = 3) -> dict:
    return {
        "PK": f"TENANT#{TENANT}",
        "SK": "POND#pond_legacy",
        "entity_type": "pond",
        "tenant_id": TENANT,
        "pond_id": "pond_legacy",
        "name": "North",
        "description": "open",
        "status": "active",
        "created_at": "2026-08-01T09:30:00+00:00",
        "updated_at": "2026-08-01T09:30:00+00:00",
        "version": version,
        "schema_version": 1,
        "v2_asset_id": ASSET_ID,
    }


def build(client: FakeDynamoClient) -> DynamoSiteAssetRepository:
    return DynamoSiteAssetRepository(TABLE, client, clock=lambda: NOW)


def item_kind(action: dict) -> str:
    if "ConditionCheck" in action:
        sk = plain(action["ConditionCheck"]["Key"])["SK"]
        return "site_check" if sk.startswith("SITE#") else "condition_check"
    request = action.get("Put") or action.get("Update")
    if "Item" in request:
        return plain(request["Item"])["entity_type"].replace("idempotency_receipt", "receipt")
    sk = plain(request["Key"])["SK"]
    if sk == "PROFILE#POND":
        return "pond_profile"
    return sk.split("#", 1)[0].lower()


def transaction_expected_versions(client: FakeDynamoClient) -> list[int]:
    actions = client.transact_write_items_calls[-1]["TransactItems"]
    return [
        plain(action["Update"]["ExpressionAttributeValues"])[":expected_version"]
        for action in actions
        if "Update" in action
    ]


def update_of(client: FakeDynamoClient, kind: str) -> dict:
    actions = client.transact_write_items_calls[-1]["TransactItems"]
    return next(action["Update"] for action in actions if item_kind(action) == kind)


# --- protocol and codecs ----------------------------------------------------------------------


def test_repository_implements_site_asset_protocols() -> None:
    repository = build(FakeDynamoClient())
    assert isinstance(repository, SiteAssetRepository)
    assert isinstance(repository, SiteAssetReader)


def test_codecs_round_trip_with_frozen_keys() -> None:
    site_item = site_to_item(site())
    assert (site_item["PK"], site_item["SK"]) == (f"TENANT#{TENANT}", f"SITE#{SITE_ID}")
    assert site_item["created_at"] == "2026-08-01T09:30:00.000000Z"
    assert site_item["schema_version"] == 2
    assert site_from_item(site_item) == site()

    asset_item = asset_to_item(asset())
    assert (asset_item["PK"], asset_item["SK"]) == (f"TENANT#{TENANT}", f"ASSET#{ASSET_ID}")
    assert asset_item["asset_type"] == "pond"
    assert asset_from_item(asset_item) == asset()

    profile = pond_profile(legacy_pond_id="pond_legacy")
    profile_item = pond_profile_to_item(profile)
    assert (profile_item["PK"], profile_item["SK"]) == (f"ASSET#{ASSET_ID}", "PROFILE#POND")
    assert pond_profile_from_item(profile_item) == profile


def test_codecs_omit_absent_optional_profile_fields_and_carry_no_ttl() -> None:
    profile = PondProfile(
        tenant_id=TENANT, asset_id=ASSET_ID, created_at=CREATED, updated_at=CREATED, version=1
    )
    item = pond_profile_to_item(profile)
    assert "description" not in item and "legacy_pond_id" not in item
    assert pond_profile_from_item(item) == profile
    for entity_item in (site_to_item(site()), asset_to_item(asset()), item):
        assert "expires_at" not in entity_item


# --- reads ------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_sites_and_assets_use_paginated_tenant_prefix_queries() -> None:
    client = FakeDynamoClient()
    client.page_size = 1
    second_site = site().model_copy(update={"site_id": "site_" + "3" * 32})
    client.seed(site_to_item(site()))
    client.seed(site_to_item(second_site))
    client.seed(asset_to_item(asset()))
    client.seed(pond_profile_to_item(pond_profile()))
    repository = build(client)

    assert await repository.list_sites(TENANT) == [site(), second_site]
    assets = await repository.list_assets(TENANT)

    assert assets == [AssetWithPondProfile(asset=asset(), pond_profile=pond_profile())]
    prefixes = [plain(call["ExpressionAttributeValues"]) for call in client.query_calls]
    assert {value[":pk"] for value in prefixes} == {f"TENANT#{TENANT}"}
    assert [value[":sk_prefix"] for value in prefixes] == ["SITE#", "SITE#", "ASSET#"]
    assert all(
        call["KeyConditionExpression"] == "PK = :pk AND begins_with(SK, :sk_prefix)"
        for call in client.query_calls
    )
    assert "ExclusiveStartKey" in client.query_calls[1]


@pytest.mark.asyncio
async def test_get_asset_reads_profile_only_after_tenant_asset_is_known() -> None:
    client = FakeDynamoClient()
    client.seed(pond_profile_to_item(pond_profile()))
    repository = build(client)

    assert await repository.get_asset(TENANT, ASSET_ID) is None
    assert [plain(call["Key"])["SK"] for call in client.get_item_calls] == [f"ASSET#{ASSET_ID}"]

    client.seed(asset_to_item(asset()))
    found = await repository.get_asset(TENANT, ASSET_ID)
    assert found == AssetWithPondProfile(asset=asset(), pond_profile=pond_profile())
    assert [plain(call["Key"]) for call in client.get_item_calls[1:]] == [
        {"PK": f"TENANT#{TENANT}", "SK": f"ASSET#{ASSET_ID}"},
        {"PK": f"ASSET#{ASSET_ID}", "SK": "PROFILE#POND"},
    ]


@pytest.mark.asyncio
async def test_get_asset_rejects_profile_owned_by_another_tenant() -> None:
    client = FakeDynamoClient()
    client.seed(asset_to_item(asset()))
    foreign = pond_profile_to_item(pond_profile())
    foreign["tenant_id"] = "tnt_other"
    client.seed(foreign)
    repository = build(client)

    assert await repository.get_asset(TENANT, ASSET_ID) is None
    assert await repository.list_assets(TENANT) == []


@pytest.mark.asyncio
async def test_get_site_uses_tenant_key_and_misses_other_tenants() -> None:
    client = FakeDynamoClient()
    client.seed(site_to_item(site()))
    repository = build(client)

    assert await repository.get_site(TENANT, SITE_ID) == site()
    assert await repository.get_site("tnt_other", SITE_ID) is None
    assert plain(client.get_item_calls[-1]["Key"]) == {
        "PK": "TENANT#tnt_other",
        "SK": f"SITE#{SITE_ID}",
    }


# --- creates ----------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_site_writes_site_and_receipt_once() -> None:
    client = FakeDynamoClient()
    created = await build(client).create_site(site(), receipt(scope="site-create"))

    assert created == site()
    assert len(client.transact_write_items_calls) == 1
    actions = client.transact_write_items_calls[0]["TransactItems"]
    assert [item_kind(action) for action in actions] == ["site", "receipt"]
    assert actions[0]["Put"]["ConditionExpression"] == "attribute_not_exists(PK)"
    assert plain(actions[1]["Put"]["Item"]) == idempotency_receipt_to_item(
        receipt(scope="site-create")
    )


@pytest.mark.asyncio
async def test_create_asset_writes_asset_profile_and_receipt_once() -> None:
    client = FakeDynamoClient()
    repository = build(client)
    created = await repository.create_asset(asset(), pond_profile(), receipt())

    assert len(client.transact_write_items_calls) == 1
    transaction = client.transact_write_items_calls[0]["TransactItems"]
    assert [item_kind(item) for item in transaction] == [
        "asset",
        "pond_profile",
        "receipt",
        "site_check",
    ]
    assert transaction[0]["Put"]["ConditionExpression"] == "attribute_not_exists(PK)"
    assert transaction[1]["Put"]["ConditionExpression"] == "attribute_not_exists(PK)"
    site_check = transaction[3]["ConditionCheck"]
    assert plain(site_check["Key"]) == {"PK": f"TENANT#{TENANT}", "SK": f"SITE#{SITE_ID}"}
    assert plain(site_check["ExpressionAttributeValues"]) == {":active": "active"}
    assert created.asset.asset_id == asset().asset_id
    assert created.pond_profile == pond_profile()


@pytest.mark.asyncio
async def test_native_asset_create_writes_no_legacy_pond_row() -> None:
    client = FakeDynamoClient()
    await build(client).create_asset(asset(), pond_profile(), receipt())

    actions = client.transact_write_items_calls[0]["TransactItems"]
    keys = [
        plain((action.get("Put") or {}).get("Item") or action["ConditionCheck"]["Key"])["SK"]
        for action in actions
    ]
    assert not [sk for sk in keys if sk.startswith("POND#")]


@pytest.mark.asyncio
async def test_create_asset_receipt_race_replays_same_request_snapshot() -> None:
    client = FakeDynamoClient()
    winner = AssetWithPondProfile(asset=asset(), pond_profile=pond_profile())
    client.seed(idempotency_receipt_to_item(receipt(snapshot=winner.model_dump_json())))
    client.transaction_error = client_error(
        "TransactionCanceledException", ["None", "None", "ConditionalCheckFailed", "None"]
    )
    loser_asset = asset().model_copy(update={"asset_id": "ast_" + "9" * 32})
    loser_profile = pond_profile().model_copy(update={"asset_id": "ast_" + "9" * 32})

    replayed = await build(client).create_asset(loser_asset, loser_profile, receipt())

    assert replayed == winner


@pytest.mark.asyncio
async def test_create_asset_receipt_race_with_other_request_conflicts() -> None:
    client = FakeDynamoClient()
    stored = idempotency_receipt_to_item(receipt())
    stored["request_hash"] = "c" * 64
    client.seed(stored)
    client.transaction_error = client_error(
        "TransactionCanceledException", ["None", "None", "ConditionalCheckFailed", "None"]
    )

    with pytest.raises(ConflictError):
        await build(client).create_asset(asset(), pond_profile(), receipt())


@pytest.mark.asyncio
async def test_create_asset_on_site_archived_concurrently_conflicts() -> None:
    client = FakeDynamoClient()
    client.transaction_error = client_error(
        "TransactionCanceledException", ["None", "None", "None", "ConditionalCheckFailed"]
    )

    with pytest.raises(ConflictError, match="site is not active"):
        await build(client).create_asset(asset(), pond_profile(), receipt())


@pytest.mark.asyncio
async def test_create_site_receipt_race_replays_snapshot() -> None:
    client = FakeDynamoClient()
    client.seed(
        idempotency_receipt_to_item(receipt(snapshot=site().model_dump_json(), scope="site-create"))
    )
    client.transaction_error = client_error(
        "TransactionCanceledException", ["None", "ConditionalCheckFailed"]
    )
    other = site().model_copy(update={"site_id": "site_" + "8" * 32})

    assert await build(client).create_site(other, receipt(scope="site-create")) == site()


@pytest.mark.asyncio
async def test_create_infrastructure_errors_are_not_swallowed() -> None:
    client = FakeDynamoClient()
    client.transaction_error = client_error("ProvisionedThroughputExceededException")

    with pytest.raises(ClientError):
        await build(client).create_asset(asset(), pond_profile(), receipt())


# --- updates ----------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_site_is_version_fenced_and_returns_new_state() -> None:
    client = FakeDynamoClient()
    client.seed(site_to_item(site(version=2)))

    updated = await build(client).update_site(
        TENANT, SITE_ID, 2, name="Farm 2", status=SiteStatus.ARCHIVED
    )

    request = client.update_item_calls[0]
    assert request["ConditionExpression"] == "attribute_exists(PK) AND #version = :expected_version"
    assert request["ReturnValues"] == "ALL_NEW"
    assert plain(request["ExpressionAttributeValues"])[":expected_version"] == 2
    assert updated.name == "Farm 2"
    assert updated.status is SiteStatus.ARCHIVED
    assert updated.version == 3
    assert updated.updated_at == NOW


@pytest.mark.asyncio
async def test_asset_profile_patch_fences_both_versions() -> None:
    client = FakeDynamoClient.with_asset_and_profile(asset_version=4, profile_version=2)
    repository = DynamoSiteAssetRepository(TABLE, client, clock=lambda: NOW)
    updated = await repository.update_asset(
        TENANT,
        ASSET_ID,
        4,
        name="East",
        status=None,
        pond_description="covered",
        pond_profile_expected_version=2,
    )

    assert transaction_expected_versions(client) == [4, 2]
    actions = client.transact_write_items_calls[0]["TransactItems"]
    assert [item_kind(action) for action in actions] == ["asset", "pond_profile"]
    assert updated.asset.name == "East"
    assert updated.asset.version == 5
    assert updated.asset.updated_at == NOW
    assert updated.pond_profile.description == "covered"
    assert updated.pond_profile.version == 3


@pytest.mark.asyncio
async def test_native_asset_patch_without_description_touches_only_asset() -> None:
    client = FakeDynamoClient.with_asset_and_profile(asset_version=1)

    updated = await build(client).update_asset(
        TENANT,
        ASSET_ID,
        1,
        name=None,
        status=AssetStatus.ARCHIVED,
        pond_description=None,
        pond_profile_expected_version=None,
    )

    actions = client.transact_write_items_calls[0]["TransactItems"]
    assert [item_kind(action) for action in actions] == ["asset"]
    assert updated.asset.status is AssetStatus.ARCHIVED
    assert updated.pond_profile == pond_profile()


@pytest.mark.asyncio
async def test_linked_asset_patch_mirrors_and_fences_legacy_pond() -> None:
    client = FakeDynamoClient.with_asset_and_profile(
        asset_version=4, profile_version=2, legacy_pond_id="pond_legacy"
    )
    client.seed(legacy_pond_item(version=7))

    await build(client).update_asset(
        TENANT,
        ASSET_ID,
        4,
        name="East",
        status=AssetStatus.ARCHIVED,
        pond_description="covered",
        pond_profile_expected_version=2,
    )

    assert len(client.transact_write_items_calls) == 1
    actions = client.transact_write_items_calls[0]["TransactItems"]
    assert [item_kind(action) for action in actions] == ["asset", "pond_profile", "pond"]
    assert transaction_expected_versions(client) == [4, 2, 7]
    pond = update_of(client, "pond")
    assert plain(pond["Key"]) == {"PK": f"TENANT#{TENANT}", "SK": "POND#pond_legacy"}
    assert pond["ConditionExpression"] == "attribute_exists(PK) AND #version = :expected_version"
    assert pond["UpdateExpression"].startswith("SET ")
    assert " REMOVE " not in pond["UpdateExpression"]
    values = plain(pond["ExpressionAttributeValues"])
    mirrored = {
        pond["ExpressionAttributeNames"][name]: values[value]
        for name, value in (part.split(" = ") for part in pond["UpdateExpression"][4:].split(", "))
    }
    assert mirrored == {
        "updated_at": "2026-08-25T12:00:00+00:00",
        "version": 8,
        "name": "East",
        "description": "covered",
        "status": "archived",
    }


@pytest.mark.asyncio
async def test_linked_asset_name_patch_mirrors_only_changed_fields() -> None:
    client = FakeDynamoClient.with_asset_and_profile(asset_version=1, legacy_pond_id="pond_legacy")
    client.seed(legacy_pond_item(version=1))

    await build(client).update_asset(
        TENANT,
        ASSET_ID,
        1,
        name="West",
        status=None,
        pond_description=None,
        pond_profile_expected_version=None,
    )

    assert transaction_expected_versions(client) == [1, 1]
    pond = update_of(client, "pond")
    assert set(pond["ExpressionAttributeNames"].values()) == {"updated_at", "version", "name"}


@pytest.mark.asyncio
async def test_linked_asset_patch_losing_legacy_pond_race_conflicts() -> None:
    client = FakeDynamoClient.with_asset_and_profile(asset_version=4, legacy_pond_id="pond_legacy")
    client.seed(legacy_pond_item(version=7))
    client.transaction_error = client_error(
        "TransactionCanceledException", ["None", "None", "ConditionalCheckFailed"]
    )

    with pytest.raises(ConflictError):
        await build(client).update_asset(
            TENANT,
            ASSET_ID,
            4,
            name="West",
            status=None,
            pond_description="covered",
            pond_profile_expected_version=1,
        )

    assert transaction_expected_versions(client) == [4, 1, 7]
    pond = update_of(client, "pond")
    assert plain(pond["ExpressionAttributeValues"])[":expected_version"] == 7
    assert pond["ConditionExpression"] == "attribute_exists(PK) AND #version = :expected_version"


@pytest.mark.asyncio
async def test_linked_asset_patch_with_missing_legacy_pond_conflicts() -> None:
    client = FakeDynamoClient.with_asset_and_profile(legacy_pond_id="pond_legacy")

    with pytest.raises(ConflictError):
        await build(client).update_asset(
            TENANT,
            ASSET_ID,
            1,
            name="West",
            status=None,
            pond_description=None,
            pond_profile_expected_version=None,
        )
    assert client.transact_write_items_calls == []


@pytest.mark.asyncio
async def test_update_asset_missing_asset_is_not_found() -> None:
    client = FakeDynamoClient()

    with pytest.raises(NotFoundError):
        await build(client).update_asset(
            TENANT,
            ASSET_ID,
            1,
            name="West",
            status=None,
            pond_description=None,
            pond_profile_expected_version=None,
        )


@pytest.mark.asyncio
async def test_update_asset_version_conflict_maps_to_conflict() -> None:
    client = FakeDynamoClient.with_asset_and_profile()
    client.transaction_error = client_error(
        "TransactionCanceledException", ["ConditionalCheckFailed"]
    )

    with pytest.raises(ConflictError):
        await build(client).update_asset(
            TENANT,
            ASSET_ID,
            1,
            name="West",
            status=None,
            pond_description=None,
            pond_profile_expected_version=None,
        )


@pytest.mark.asyncio
async def test_update_asset_rejects_unpaired_profile_fields() -> None:
    client = FakeDynamoClient.with_asset_and_profile()

    with pytest.raises(ValueError):
        await build(client).update_asset(
            TENANT,
            ASSET_ID,
            1,
            name=None,
            status=None,
            pond_description="covered",
            pond_profile_expected_version=None,
        )
