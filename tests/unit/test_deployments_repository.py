from datetime import UTC, datetime, timedelta

import pytest
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError
from pydantic import ValidationError

from limnopulse_api.adapters.dynamodb_deployments import (
    DEPLOYMENT_MARKER_SK,
    DynamoDeploymentRepository,
    deployment_from_item,
    deployment_marker_item,
    deployment_marker_key,
    deployment_to_item,
)
from limnopulse_api.adapters.dynamodb_v2 import DynamoV2KeyBuilder
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.deployments import Deployment, DeploymentStatus
from limnopulse_api.domain.v2_common import new_idempotency_receipt
from limnopulse_api.repositories.v2_contracts import DeploymentRepository

TABLE = "LimnopulseDomain"
TENANT = "tnt_a"
OTHER_TENANT = "tnt_b"
COMPONENT_ID = "cmp_" + "1" * 32
DEPLOYMENT_ID = "dep_" + "2" * 32
NEXT_DEPLOYMENT_ID = "dep_" + "3" * 32
SITE_ID = "site_" + "4" * 32
WEST_ASSET_ID = "ast_" + "5" * 32
EAST_ASSET_ID = "ast_" + "6" * 32
STARTED = datetime(2026, 8, 25, 10, tzinfo=UTC)
NOW = datetime(2026, 8, 25, 12, tzinfo=UTC)
KEY_HASH = "a" * 64
REQUEST_HASH = "b" * 64
KEYS = DynamoV2KeyBuilder()


class FakeDynamoClient:
    """Stores items; `query` honours the key condition bounds, direction and Limit."""

    def __init__(self) -> None:
        self.serializer = TypeSerializer()
        self.deserializer = TypeDeserializer()
        self.items: dict[tuple[str, str], dict] = {}
        self.calls: list[tuple[str, dict]] = []
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

    def _plain(self, mapping: dict) -> dict:
        return {key: self.deserializer.deserialize(value) for key, value in mapping.items()}

    def get_item(self, **kwargs):
        self.calls.append(("get_item", kwargs))
        key = self._plain(kwargs["Key"])
        item = self.items.get((key["PK"], key["SK"]))
        return {} if item is None else {"Item": item}

    def query(self, **kwargs):
        self.calls.append(("query", kwargs))
        values = self._plain(kwargs["ExpressionAttributeValues"])
        lower = values.get(":sk_lower", "")
        upper = values.get(":sk_upper", "￿")
        rows = sorted(
            (sk, item)
            for (pk, sk), item in self.items.items()
            if pk == values[":pk"] and lower <= sk <= upper
        )
        if not kwargs.get("ScanIndexForward", True):
            rows.reverse()
        if "Limit" in kwargs:
            rows = rows[: kwargs["Limit"]]
        return {"Items": [item for _, item in rows]}

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

    def queries(self) -> list[dict]:
        return [kwargs for name, kwargs in self.calls if name == "query"]


def deployment(**overrides) -> Deployment:
    values = {
        "tenant_id": TENANT,
        "deployment_id": DEPLOYMENT_ID,
        "component_id": COMPONENT_ID,
        "site_id": SITE_ID,
        "asset_id": WEST_ASSET_ID,
        "started_at": STARTED,
        "created_at": STARTED,
        "updated_at": STARTED,
        "version": 1,
    }
    values.update(overrides)
    return Deployment(**values)


def ended(**overrides) -> Deployment:
    values = {"ended_at": STARTED + timedelta(hours=1), "status": DeploymentStatus.ENDED}
    values.update(overrides)
    return deployment(**values)


def receipt():
    return new_idempotency_receipt(
        tenant_id=TENANT,
        scope="deployment-create",
        key_hash=KEY_HASH,
        request_hash=REQUEST_HASH,
        response_snapshot_json="{}",
        created_at=NOW,
    )


def repository_for(client: FakeDynamoClient) -> DynamoDeploymentRepository:
    return DynamoDeploymentRepository(TABLE, client, clock=lambda: NOW)


def seed_deployment(client: FakeDynamoClient, value: Deployment) -> None:
    client.seed(deployment_to_item(value, form="mirror"))
    client.seed(deployment_to_item(value, form="history"))
    if value.status is DeploymentStatus.ACTIVE:
        client.seed(deployment_to_item(value, form="current"))


def transaction_item_keys(client: FakeDynamoClient) -> list[tuple[str, str]]:
    keys = []
    for action in client.transaction():
        (request,) = action.values()
        raw = request.get("Item") or request["Key"]
        plain = client._plain({"PK": raw["PK"], "SK": raw["SK"]})
        keys.append((plain["PK"], plain["SK"]))
    return keys


def dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def interval(asset_id: str, start: str, end: str | None, deployment_id: str) -> Deployment:
    started_at = dt(start)
    return deployment(
        deployment_id=deployment_id,
        asset_id=asset_id,
        started_at=started_at,
        ended_at=None if end is None else dt(end),
        status=DeploymentStatus.ACTIVE if end is None else DeploymentStatus.ENDED,
        created_at=started_at,
        updated_at=started_at if end is None else dt(end),
    )


# Domain interval -------------------------------------------------------------------------------


def test_deployment_is_a_nonempty_utc_half_open_interval() -> None:
    with pytest.raises(ValidationError):
        ended(ended_at=STARTED)
    with pytest.raises(ValidationError):
        deployment(started_at=STARTED.replace(tzinfo=None))
    with pytest.raises(ValidationError):
        deployment(status=DeploymentStatus.ENDED)


def test_repository_implements_the_frozen_protocol() -> None:
    assert isinstance(repository_for(FakeDynamoClient()), DeploymentRepository)


# Codecs ----------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("form", "key"),
    [
        ("mirror", ("TENANT#tnt_a", f"DEPLOYMENT#{DEPLOYMENT_ID}")),
        (
            "history",
            (
                f"COMPONENT#{COMPONENT_ID}",
                f"DEPLOYMENT#2026-08-25T10:00:00.000000Z#{DEPLOYMENT_ID}",
            ),
        ),
        ("current", (f"COMPONENT#{COMPONENT_ID}", "DEPLOYMENT#CURRENT")),
    ],
)
def test_codecs_round_trip_every_form(form, key) -> None:
    value = ended()
    item = deployment_to_item(value, form=form)
    assert (item["PK"], item["SK"]) == key
    assert item["ended_at"] == "2026-08-25T11:00:00.000000Z"
    assert "expires_at" not in item
    assert deployment_from_item(item) == value


def test_marker_key_sorts_outside_history_bounds() -> None:
    lower, upper = KEYS.deployment_history_bounds()
    assert deployment_marker_key(COMPONENT_ID) == {
        "PK": f"COMPONENT#{COMPONENT_ID}",
        "SK": DEPLOYMENT_MARKER_SK,
    }
    assert not lower <= DEPLOYMENT_MARKER_SK <= upper
    assert not lower <= KEYS.current_deployment(COMPONENT_ID)["SK"] <= upper
    item = deployment_marker_item(TENANT, COMPONENT_ID, seq=3, last_ended_at=STARTED)
    assert item["seq"] == 3
    assert item["last_ended_at"] == "2026-08-25T10:00:00.000000Z"


# Start -----------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_writes_projection_history_pointer_marker_receipt_and_parent_fences() -> None:
    client = FakeDynamoClient()
    await repository_for(client).start_deployment(deployment(), receipt())

    assert transaction_item_keys(client) == [
        ("TENANT#tnt_a", f"DEPLOYMENT#{DEPLOYMENT_ID}"),
        (f"COMPONENT#{COMPONENT_ID}", f"DEPLOYMENT#2026-08-25T10:00:00.000000Z#{DEPLOYMENT_ID}"),
        (f"COMPONENT#{COMPONENT_ID}", "DEPLOYMENT#CURRENT"),
        (f"COMPONENT#{COMPONENT_ID}", "DEPLOYMENT#MARKER"),
        ("TENANT#tnt_a", f"IDEMPOTENCY#deployment-create#{KEY_HASH}"),
        (f"COMPONENT#{COMPONENT_ID}", "META"),
        ("TENANT#tnt_a", f"ASSET#{WEST_ASSET_ID}"),
        ("TENANT#tnt_a", f"SITE#{SITE_ID}"),
    ]
    actions = client.transaction()
    assert len(set(transaction_item_keys(client))) == len(actions)
    for index in (0, 1, 2):
        assert actions[index]["Put"]["ConditionExpression"] == "attribute_not_exists(PK)"
    marker = actions[3]["Update"]
    assert marker["ConditionExpression"] == "attribute_not_exists(PK)"
    for index in (5, 6, 7):
        check = actions[index]["ConditionCheck"]
        assert "#status = :active" in check["ConditionExpression"]
        assert "#tenant_id = :tenant_id" in check["ConditionExpression"]
        assert client._plain(check["ExpressionAttributeValues"])[":tenant_id"] == TENANT


@pytest.mark.asyncio
async def test_start_fences_the_observed_marker_sequence_and_previous_end() -> None:
    client = FakeDynamoClient()
    previous = ended(deployment_id=NEXT_DEPLOYMENT_ID)
    seed_deployment(client, previous)
    client.seed(
        deployment_marker_item(TENANT, COMPONENT_ID, seq=2, last_ended_at=previous.ended_at)
    )
    started = previous.ended_at  # the exact transition boundary is not an overlap

    await repository_for(client).start_deployment(
        deployment(started_at=started, created_at=NOW, updated_at=NOW), receipt()
    )

    marker = client.transaction()[3]["Update"]
    values = client._plain(marker["ExpressionAttributeValues"])
    assert "#seq = :observed_seq" in marker["ConditionExpression"]
    assert "#last_ended_at <= :started_at" in marker["ConditionExpression"]
    assert values[":observed_seq"] == 2
    assert values[":next_seq"] == 3
    assert values[":started_at"] == "2026-08-25T11:00:00.000000Z"


@pytest.mark.asyncio
async def test_start_before_the_previous_end_is_an_overlap() -> None:
    client = FakeDynamoClient()
    previous = ended(deployment_id=NEXT_DEPLOYMENT_ID)
    seed_deployment(client, previous)
    client.seed(
        deployment_marker_item(TENANT, COMPONENT_ID, seq=2, last_ended_at=previous.ended_at)
    )

    with pytest.raises(ConflictError, match="overlaps the previous interval"):
        await repository_for(client).start_deployment(
            deployment(
                started_at=previous.ended_at - timedelta(microseconds=1),
                created_at=NOW,
                updated_at=NOW,
            ),
            receipt(),
        )
    assert not [name for name, _ in client.calls if name == "transact_write_items"]


@pytest.mark.asyncio
async def test_start_rejects_overlap_with_markerless_history() -> None:
    client = FakeDynamoClient()
    seed_deployment(client, ended(deployment_id=NEXT_DEPLOYMENT_ID))
    with pytest.raises(ConflictError, match="overlaps"):
        await repository_for(client).start_deployment(
            deployment(started_at=STARTED + timedelta(minutes=30), created_at=NOW, updated_at=NOW),
            receipt(),
        )


@pytest.mark.asyncio
async def test_start_rejects_open_markerless_history() -> None:
    client = FakeDynamoClient()
    seed_deployment(client, deployment(deployment_id=NEXT_DEPLOYMENT_ID))
    with pytest.raises(ConflictError, match="active deployment"):
        await repository_for(client).start_deployment(
            deployment(started_at=NOW, created_at=NOW, updated_at=NOW), receipt()
        )


@pytest.mark.asyncio
async def test_history_query_ignores_capability_meta_current_and_marker_rows() -> None:
    client = FakeDynamoClient()
    client.seed({"PK": f"COMPONENT#{COMPONENT_ID}", "SK": "META", "tenant_id": TENANT})
    client.seed({"PK": f"COMPONENT#{COMPONENT_ID}", "SK": "CAPABILITY#cap_1", "tenant_id": TENANT})
    client.seed(deployment_marker_item(TENANT, COMPONENT_ID, seq=1, last_ended_at=None))
    value = deployment()
    seed_deployment(client, value)

    assert await repository_for(client).list_component_deployments(TENANT, COMPONENT_ID) == [value]
    (query,) = client.queries()
    values = client._plain(query["ExpressionAttributeValues"])
    assert (values[":sk_lower"], values[":sk_upper"]) == KEYS.deployment_history_bounds()


@pytest.mark.parametrize(
    ("codes", "message"),
    [
        (("None", "None", "ConditionalCheckFailed", "None", "None"), "active deployment"),
        (("None", "None", "None", "ConditionalCheckFailed", "None"), "overlaps"),
        (
            ("None", "None", "ConditionalCheckFailed", "None", "ConditionalCheckFailed"),
            "idempotency key reused",
        ),
        (("None",) * 5 + ("ConditionalCheckFailed",), "component is not active"),
        (("None",) * 6 + ("ConditionalCheckFailed",), "asset is not active"),
        (("None",) * 7 + ("ConditionalCheckFailed",), "site is not active"),
        (("ConditionalCheckFailed",), "deployment already exists"),
    ],
)
@pytest.mark.asyncio
async def test_start_classifies_the_failed_transaction_action(codes, message) -> None:
    client = FakeDynamoClient.rejecting(*codes)
    with pytest.raises(ConflictError, match=message):
        await repository_for(client).start_deployment(deployment(), receipt())


@pytest.mark.asyncio
async def test_concurrent_start_has_one_winner_on_the_current_pointer() -> None:
    client = FakeDynamoClient()
    repository = repository_for(client)
    await repository.start_deployment(deployment(), receipt())
    client.fail_with = ClientError(
        {
            "Error": {"Code": "TransactionCanceledException", "Message": "cancelled"},
            "CancellationReasons": [
                {"Code": "None"},
                {"Code": "None"},
                {"Code": "ConditionalCheckFailed"},
                {"Code": "ConditionalCheckFailed"},
                {"Code": "None"},
            ],
        },
        "TransactWriteItems",
    )
    with pytest.raises(ConflictError, match="active deployment"):
        await repository.start_deployment(deployment(deployment_id=NEXT_DEPLOYMENT_ID), receipt())


@pytest.mark.asyncio
async def test_non_conditional_client_errors_are_not_swallowed() -> None:
    client = FakeDynamoClient.rejecting("ThrottlingError")
    with pytest.raises(ClientError):
        await repository_for(client).start_deployment(deployment(), receipt())


# End -------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_end_updates_rows_deletes_the_exact_pointer_and_advances_the_marker() -> None:
    client = FakeDynamoClient()
    seed_deployment(client, deployment())
    client.seed(deployment_marker_item(TENANT, COMPONENT_ID, seq=1, last_ended_at=None))
    ended_at = STARTED + timedelta(hours=1)

    result = await repository_for(client).end_deployment(
        TENANT, DEPLOYMENT_ID, expected_version=1, ended_at=ended_at
    )

    assert result.status is DeploymentStatus.ENDED
    assert result.ended_at == ended_at
    assert result.version == 2
    assert (result.component_id, result.asset_id, result.started_at) == (
        COMPONENT_ID,
        WEST_ASSET_ID,
        STARTED,
    )
    assert transaction_item_keys(client) == [
        ("TENANT#tnt_a", f"DEPLOYMENT#{DEPLOYMENT_ID}"),
        (f"COMPONENT#{COMPONENT_ID}", f"DEPLOYMENT#2026-08-25T10:00:00.000000Z#{DEPLOYMENT_ID}"),
        (f"COMPONENT#{COMPONENT_ID}", "DEPLOYMENT#CURRENT"),
        (f"COMPONENT#{COMPONENT_ID}", "DEPLOYMENT#MARKER"),
    ]
    actions = client.transaction()
    for index in (0, 1):
        update = actions[index]["Update"]
        values = client._plain(update["ExpressionAttributeValues"])
        assert "#version = :expected_version" in update["ConditionExpression"]
        assert "#status = :active" in update["ConditionExpression"]
        assert values[":expected_version"] == 1
        assert values[":ended_at"] == "2026-08-25T11:00:00.000000Z"
        # Target and start are never rewritten.
        assert "asset_id" not in update["ExpressionAttributeNames"].values()
        assert "started_at" not in update["ExpressionAttributeNames"].values()
    delete = actions[2]["Delete"]
    assert "#deployment_id = :deployment_id" in delete["ConditionExpression"]
    assert client._plain(delete["ExpressionAttributeValues"])[":deployment_id"] == DEPLOYMENT_ID
    marker = actions[3]["Update"]
    values = client._plain(marker["ExpressionAttributeValues"])
    assert (values[":observed_seq"], values[":next_seq"]) == (1, 2)
    assert values[":last_ended_at"] == "2026-08-25T11:00:00.000000Z"


@pytest.mark.asyncio
async def test_end_creates_the_marker_for_markerless_history() -> None:
    client = FakeDynamoClient()
    seed_deployment(client, deployment())
    await repository_for(client).end_deployment(
        TENANT, DEPLOYMENT_ID, expected_version=1, ended_at=NOW
    )
    assert client.transaction()[3]["Update"]["ConditionExpression"] == "attribute_not_exists(PK)"


@pytest.mark.asyncio
async def test_end_rejects_missing_cross_tenant_ended_and_stale_deployments() -> None:
    client = FakeDynamoClient()
    seed_deployment(client, deployment())
    seed_deployment(client, ended(deployment_id=NEXT_DEPLOYMENT_ID))
    repository = repository_for(client)

    with pytest.raises(NotFoundError):
        await repository.end_deployment(
            OTHER_TENANT, DEPLOYMENT_ID, expected_version=1, ended_at=NOW
        )
    with pytest.raises(ConflictError, match="not active"):
        await repository.end_deployment(
            TENANT, NEXT_DEPLOYMENT_ID, expected_version=1, ended_at=NOW
        )
    with pytest.raises(ConflictError, match="version"):
        await repository.end_deployment(TENANT, DEPLOYMENT_ID, expected_version=2, ended_at=NOW)
    assert not [name for name, _ in client.calls if name == "transact_write_items"]


@pytest.mark.asyncio
async def test_end_maps_a_lost_race_to_conflict() -> None:
    client = FakeDynamoClient.rejecting("ConditionalCheckFailed", "None", "None", "None")
    seed_deployment(client, deployment())
    with pytest.raises(ConflictError, match="version conflict"):
        await repository_for(client).end_deployment(
            TENANT, DEPLOYMENT_ID, expected_version=1, ended_at=NOW
        )


# Reads and event-time resolution ---------------------------------------------------------------


def repository_with_history(*intervals: Deployment) -> tuple[DynamoDeploymentRepository, object]:
    client = FakeDynamoClient()
    client.seed({"PK": f"COMPONENT#{COMPONENT_ID}", "SK": "META", "tenant_id": TENANT})
    client.seed({"PK": f"COMPONENT#{COMPONENT_ID}", "SK": "CAPABILITY#cap_9", "tenant_id": TENANT})
    for value in intervals:
        seed_deployment(client, value)
    return repository_for(client), client


@pytest.mark.asyncio
async def test_resolve_returns_the_interval_containing_event_time_after_relocation() -> None:
    west_id, east_id = DEPLOYMENT_ID, NEXT_DEPLOYMENT_ID
    repository, client = repository_with_history(
        interval(WEST_ASSET_ID, "2026-08-25T10:00:00Z", "2026-08-25T11:00:00Z", west_id),
        interval(EAST_ASSET_ID, "2026-08-25T11:00:00Z", None, east_id),
    )

    at = repository.get_deployment_at
    assert await at(TENANT, COMPONENT_ID, dt("2026-08-25T09:59:59.999999Z")) is None
    assert (await at(TENANT, COMPONENT_ID, dt("2026-08-25T10:00:00Z"))).asset_id == WEST_ASSET_ID
    assert (await at(TENANT, COMPONENT_ID, dt("2026-08-25T10:30:00Z"))).asset_id == WEST_ASSET_ID
    west_last = dt("2026-08-25T10:59:59.999999Z")
    assert (await at(TENANT, COMPONENT_ID, west_last)).asset_id == WEST_ASSET_ID
    assert (await at(TENANT, COMPONENT_ID, dt("2026-08-25T11:00:00Z"))).asset_id == EAST_ASSET_ID
    assert (await at(TENANT, COMPONENT_ID, dt("2026-08-25T11:30:00Z"))).asset_id == EAST_ASSET_ID

    query = client.queries()[-1]
    values = client._plain(query["ExpressionAttributeValues"])
    assert values[":sk_lower"] == KEYS.deployment_history_bounds()[0]
    assert values[":sk_upper"] == KEYS.deployment_history_upper_bound(dt("2026-08-25T11:30:00Z"))
    assert query["ScanIndexForward"] is False
    assert query["Limit"] == 1


@pytest.mark.asyncio
async def test_resolve_returns_none_in_a_gap_and_for_another_tenant() -> None:
    repository, _ = repository_with_history(
        interval(WEST_ASSET_ID, "2026-08-25T10:00:00Z", "2026-08-25T11:00:00Z", DEPLOYMENT_ID),
    )
    assert (
        await repository.get_deployment_at(TENANT, COMPONENT_ID, dt("2026-08-25T11:00:00Z")) is None
    )
    assert (
        await repository.get_deployment_at(OTHER_TENANT, COMPONENT_ID, dt("2026-08-25T10:30:00Z"))
        is None
    )


@pytest.mark.asyncio
async def test_point_reads_validate_the_tenant() -> None:
    repository, _ = repository_with_history(deployment())
    assert await repository.get_deployment(TENANT, DEPLOYMENT_ID) == deployment()
    assert await repository.get_current_deployment(TENANT, COMPONENT_ID) == deployment()
    assert await repository.get_current_deployment(OTHER_TENANT, COMPONENT_ID) is None
    assert await repository.get_deployment(OTHER_TENANT, DEPLOYMENT_ID) is None
    assert await repository.list_component_deployments(OTHER_TENANT, COMPONENT_ID) == []
