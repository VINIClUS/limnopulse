import asyncio
import inspect
import re
import threading
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError

from limnopulse_api.adapters.dynamodb import (
    DynamoDomainRepository,
    legacy_pond_from_item,
    legacy_tenant_from_item,
)
from limnopulse_api.adapters.dynamodb_deployments import (
    DynamoDeploymentRepository,
    deployment_marker_key,
)
from limnopulse_api.adapters.dynamodb_devices_v2 import (
    DynamoDeviceComponentRepository,
    device_lookup_to_item,
    device_v2_to_item,
)
from limnopulse_api.adapters.dynamodb_sites_assets import (
    DynamoSiteAssetRepository,
    asset_to_item,
    pond_profile_to_item,
    site_to_item,
)
from limnopulse_api.adapters.dynamodb_v1_pond_device_projection import (
    DynamoV1PondDeviceProjection,
)
from limnopulse_api.adapters.dynamodb_v2 import DynamoV2KeyBuilder
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.assets import Asset, AssetStatus, AssetType, PondProfile, SiteStatus
from limnopulse_api.domain.deployments import Deployment, DeploymentStatus
from limnopulse_api.domain.devices_v2 import DeviceV2
from limnopulse_api.domain.entities import Pond
from limnopulse_api.domain.ids import (
    asset_id_for_legacy_pond,
    default_component_id_for_legacy_device,
    default_probe_profile_id_for_legacy_device,
    default_site_id,
    deployment_id_for_legacy_device,
)
from limnopulse_api.domain.legacy_projection import (
    asset_and_profile_for_legacy_pond,
    default_site_for_tenant,
)
from limnopulse_api.repositories.domain import DomainRepository
from limnopulse_api.repositories.v1_pond_device_projection import V1PondDeviceProjection
from limnopulse_api.services.v1_pond_device_projection import (
    device_v1_projection,
    pond_from_asset,
    transition_instant,
)

TABLE = "LimnopulseDomain"
TENANT = "tnt_a"
OTHER_TENANT = "tnt_b"
KEYS = DynamoV2KeyBuilder()
CREATED = "2026-08-25T09:00:00+00:00"
DEV_1 = "dev_" + "1" * 32
DEV_2 = "dev_" + "2" * 32


def dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


# In-memory DynamoDB that evaluates the condition/update expression subset the adapters use --


_TOKEN = re.compile(
    r"\s*(?:(?P<op><=|>=|<>|=|<|>)|(?P<paren>[(),])|(?P<word>[A-Za-z_#:][A-Za-z0-9_#:]*))"
)


class _Expression:
    def __init__(self, text: str, names: dict, values: dict, item: dict | None) -> None:
        self.tokens = [match for match in _TOKEN.finditer(text) if match.group().strip()]
        self.tokens = [match.group().strip() for match in self.tokens]
        self.position = 0
        self.names = names
        self.values = values
        self.item = item or {}

    def evaluate(self) -> bool:
        result = self._or()
        assert self.position == len(self.tokens), self.tokens
        return result

    def _peek(self) -> str | None:
        return self.tokens[self.position] if self.position < len(self.tokens) else None

    def _take(self) -> str:
        token = self.tokens[self.position]
        self.position += 1
        return token

    def _or(self) -> bool:
        result = self._and()
        while self._peek() == "OR":
            self._take()
            right = self._and()
            result = result or right
        return result

    def _and(self) -> bool:
        result = self._primary()
        while self._peek() == "AND":
            self._take()
            right = self._primary()
            result = result and right
        return result

    def _attribute(self, token: str) -> str:
        return self.names[token] if token.startswith("#") else token

    def _operand(self, token: str):
        if token.startswith(":"):
            return self.values[token]
        return self.item.get(self._attribute(token))

    def _primary(self) -> bool:
        token = self._take()
        if token == "(":
            result = self._or()
            assert self._take() == ")"
            return result
        if token in {"attribute_exists", "attribute_not_exists"}:
            assert self._take() == "("
            attribute = self._attribute(self._take())
            assert self._take() == ")"
            exists = attribute in self.item
            return exists if token == "attribute_exists" else not exists
        operator = self._take()
        left, right = self._operand(token), self._operand(self._take())
        if left is None or right is None:
            return False
        return {
            "=": left == right,
            "<>": left != right,
            "<": left < right,
            "<=": left <= right,
            ">": left > right,
            ">=": left >= right,
        }[operator]


class FakeDynamoClient:
    def __init__(self, *, page_size: int | None = None) -> None:
        self.serializer = TypeSerializer()
        self.deserializer = TypeDeserializer()
        self.items: dict[tuple[str, str], dict] = {}
        self.page_size = page_size
        self.lock = threading.Lock()
        self.transactions: list[list[dict]] = []
        self.queries: list[dict] = []
        self.scan_calls = 0
        self.fail_transactions: list[Exception] = []
        self.site_read_barrier: threading.Barrier | None = None
        self.site_barrier_waits = 0

    def rendezvous_site_reads(self) -> None:
        self.site_read_barrier = threading.Barrier(2)
        self.site_barrier_waits = 2

    # Test helpers ---------------------------------------------------------------------------

    def seed(self, item: dict) -> None:
        self.items[(item["PK"], item["SK"])] = dict(item)

    def item(self, key: dict) -> dict | None:
        return self.items.get((key["PK"], key["SK"]))

    def snapshot(self) -> dict:
        return {key: dict(value) for key, value in self.items.items()}

    # boto3 surface --------------------------------------------------------------------------

    def get_item(self, **kwargs):
        key = self._plain(kwargs["Key"])
        with self.lock:
            item = self.items.get((key["PK"], key["SK"]))
            wait = key["SK"].startswith("SITE#") and self.site_barrier_waits > 0
            self.site_barrier_waits -= int(wait)
        if wait:
            # The first two Site reads both complete before either writer continues, so both
            # observe "no Site yet".
            self.site_read_barrier.wait(timeout=5)
        return {} if item is None else {"Item": self._encode(item)}

    def query(self, **kwargs):
        self.queries.append(kwargs)
        values = self._plain(kwargs["ExpressionAttributeValues"])
        prefix = values.get(":sk_prefix")
        lower = values.get(":sk_lower", "")
        upper = values.get(":sk_upper", "￿")
        with self.lock:
            rows = sorted(
                (sk, item)
                for (pk, sk), item in self.items.items()
                if pk == values[":pk"]
                and (sk.startswith(prefix) if prefix is not None else lower <= sk <= upper)
            )
        if not kwargs.get("ScanIndexForward", True):
            rows.reverse()
        start = kwargs.get("ExclusiveStartKey")
        if start is not None:
            start_sk = self._plain(start)["SK"]
            index = next(i for i, (sk, _) in enumerate(rows) if sk == start_sk)
            rows = rows[index + 1 :]
        limit = kwargs.get("Limit") or self.page_size
        response: dict = {}
        if limit is not None and len(rows) > limit:
            rows = rows[:limit]
            last = rows[-1][1]
            response["LastEvaluatedKey"] = self._encode({"PK": last["PK"], "SK": last["SK"]})
        response["Items"] = [self._encode(item) for _, item in rows]
        return response

    def put_item(self, **kwargs):
        self.transact_write_items(TransactItems=[{"Put": kwargs}])
        return {}

    def update_item(self, **kwargs):
        self.transact_write_items(TransactItems=[{"Update": kwargs}])
        key = self._plain(kwargs["Key"])
        return {"Attributes": self._encode(self.items[(key["PK"], key["SK"])])}

    def transact_write_items(self, **kwargs):
        actions = kwargs["TransactItems"]
        assert 1 <= len(actions) <= 25
        with self.lock:
            self.transactions.append(actions)
            if self.fail_transactions:
                raise self.fail_transactions.pop(0)
            keys = [self._action_key(action) for action in actions]
            assert len(set(keys)) == len(keys), "transaction keys must be distinct"
            reasons = []
            for action, key in zip(actions, keys, strict=True):
                (request,) = action.values()
                ok = self._condition(request, self.items.get(key))
                reasons.append({"Code": "None" if ok else "ConditionalCheckFailed"})
            if any(reason["Code"] != "None" for reason in reasons):
                raise ClientError(
                    {
                        "Error": {"Code": "TransactionCanceledException", "Message": "x"},
                        "CancellationReasons": reasons,
                    },
                    "TransactWriteItems",
                )
            for action, key in zip(actions, keys, strict=True):
                (kind, request), = action.items()
                if kind == "Put":
                    self.items[key] = self._plain(request["Item"])
                elif kind == "Delete":
                    self.items.pop(key, None)
                elif kind == "Update":
                    self.items[key] = self._apply_update(request, self.items.get(key), key)
        return {}

    def scan(self, **kwargs):  # pragma: no cover - must never be called
        self.scan_calls += 1
        raise AssertionError("Scan is forbidden")

    # Internals ------------------------------------------------------------------------------

    def _action_key(self, action: dict) -> tuple[str, str]:
        (request,) = action.values()
        key = self._plain(request["Key"] if "Key" in request else request["Item"])
        return key["PK"], key["SK"]

    def _condition(self, request: dict, item: dict | None) -> bool:
        expression = request.get("ConditionExpression")
        if expression is None:
            return True
        return _Expression(
            expression,
            request.get("ExpressionAttributeNames", {}),
            self._plain(request.get("ExpressionAttributeValues", {})),
            item,
        ).evaluate()

    def _apply_update(self, request: dict, item: dict | None, key: tuple[str, str]) -> dict:
        updated = dict(item or {"PK": key[0], "SK": key[1]})
        names = request.get("ExpressionAttributeNames", {})
        values = self._plain(request.get("ExpressionAttributeValues", {}))
        expression = request["UpdateExpression"]
        assert expression.startswith("SET ")
        for clause in expression.removeprefix("SET ").split(", "):
            name, value = (part.strip() for part in clause.split(" = "))
            updated[names.get(name, name)] = values[value]
        return updated

    def _plain(self, mapping: dict) -> dict:
        return {
            key: _normalize(self.deserializer.deserialize(value)) for key, value in mapping.items()
        }

    def _encode(self, item: dict) -> dict:
        return {key: self.serializer.serialize(value) for key, value in item.items()}


def _normalize(value):
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, dict):
        return {key: _normalize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize(item) for item in value]
    return value


# Fixtures ----------------------------------------------------------------------------------


class Clock:
    def __init__(self, value: str) -> None:
        self.value = dt(value)

    def __call__(self) -> datetime:
        return self.value

    def set(self, value: str) -> None:
        self.value = dt(value)


async def _no_sleep(attempt: int) -> None:
    return None


def tenant_item(tenant_id: str = TENANT) -> dict:
    return {
        "PK": f"TENANT#{tenant_id}",
        "SK": "META",
        "entity_type": "tenant",
        "tenant_id": tenant_id,
        "name": "Demo",
        "settings": {},
        "status": "active",
        "created_at": "2026-08-01T00:00:00+00:00",
        "updated_at": "2026-08-01T00:00:00+00:00",
        "version": 1,
        "schema_version": 1,
    }


def legacy_pond_item(pond_id: str, *, tenant_id: str = TENANT, name: str = "West", **extra) -> dict:
    return {
        "PK": f"TENANT#{tenant_id}",
        "SK": f"POND#{pond_id}",
        "entity_type": "pond",
        "tenant_id": tenant_id,
        "pond_id": pond_id,
        "name": name,
        "description": None,
        "status": "active",
        "created_at": CREATED,
        "updated_at": CREATED,
        "version": 1,
        "schema_version": 1,
        **extra,
    }


def legacy_device_rows(device_id: str, pond_id: str, *, tenant_id: str = TENANT) -> list[dict]:
    body = {
        "tenant_id": tenant_id,
        "pond_id": pond_id,
        "device_id": device_id,
        "name": "Gateway",
        "auth_type": "mtls",
        "firmware_version": "1.0.0",
        "status": "active",
        "created_at": CREATED,
        "updated_at": CREATED,
        "version": 1,
        "schema_version": 1,
    }
    return [
        {"PK": f"TENANT#{tenant_id}", "SK": f"DEVICE#{device_id}", "entity_type": "device", **body},
        {"PK": f"DEVICE#{device_id}", "SK": "META", "entity_type": "device_lookup", **body},
    ]


def native_device(device_id: str = "dev_" + "9" * 32) -> DeviceV2:
    now = dt("2026-08-25T09:30:00Z")
    return DeviceV2(
        tenant_id=TENANT, device_id=device_id, name="Native", created_at=now, updated_at=now,
        version=1,
    )


def seed_native_device(client: FakeDynamoClient) -> DeviceV2:
    device = native_device()
    client.seed(device_v2_to_item(device))
    client.seed(device_lookup_to_item(device))
    return device


def seed_migrated_pond(client: FakeDynamoClient, pond_id: str, *, name: str = "West") -> str:
    """Legacy Pond plus the rows the M1 migration writes for it."""
    site = default_site_for_tenant(legacy_tenant_from_item(tenant_item()))
    if client.item(KEYS.site(TENANT, site.site_id)) is None:
        client.seed(site_to_item(site))
    asset_id = asset_id_for_legacy_pond(TENANT, pond_id)
    pond = legacy_pond_item(
        pond_id,
        name=name,
        v2_site_id=site.site_id,
        v2_asset_id=asset_id,
        v2_projection_version=1,
    )
    client.seed(pond)
    projected = asset_and_profile_for_legacy_pond(legacy_pond_from_item(pond), site.site_id)
    client.seed(asset_to_item(projected.asset))
    client.seed(pond_profile_to_item(projected.pond_profile))
    return asset_id


def store(*, page_size: int | None = None) -> FakeDynamoClient:
    client = FakeDynamoClient(page_size=page_size)
    client.seed(tenant_item())
    return client


def projection(
    client: FakeDynamoClient, clock: Clock | None = None
) -> DynamoV1PondDeviceProjection:
    return DynamoV1PondDeviceProjection(
        TABLE, client, clock=clock or Clock("2026-08-25T10:00:00.123456Z"), retry_sleep=_no_sleep
    )


async def assert_pond_projection_matches(client: FakeDynamoClient, pond_id: str) -> None:
    """Divergence guard: the v2 Asset/PondProfile equals the legacy Pond on shared fields."""
    legacy = await DynamoDomainRepository(TABLE, client).get_pond(TENANT, pond_id)
    pond_row = client.item(KEYS.legacy_pond(TENANT, pond_id))
    combined = await DynamoSiteAssetRepository(TABLE, client).get_asset(
        TENANT, pond_row["v2_asset_id"]
    )
    assert legacy is not None and combined is not None
    projected = pond_from_asset(combined.asset, combined.pond_profile)
    shared = ("tenant_id", "pond_id", "name", "description", "status")
    assert {f: getattr(projected, f) for f in shared} == {f: getattr(legacy, f) for f in shared}
    assert pond_row["v2_site_id"] == combined.asset.site_id
    assert pond_row["v2_projection_version"] == 1


async def assert_device_projection_matches(client: FakeDynamoClient, device_id: str) -> None:
    """Divergence guard: the current default Deployment places the Device in its legacy Pond."""
    legacy = await DynamoDomainRepository(TABLE, client).get_device(TENANT, device_id)
    assert legacy is not None
    device_v2 = await DynamoDeviceComponentRepository(TABLE, client).get_device(TENANT, device_id)
    component_id = default_component_id_for_legacy_device(TENANT, device_id)
    current = await DynamoDeploymentRepository(TABLE, client).get_current_deployment(
        TENANT, component_id
    )
    assert current is not None
    combined = await DynamoSiteAssetRepository(TABLE, client).get_asset(TENANT, current.asset_id)
    assert combined is not None
    projected = device_v1_projection(device_v2, current, combined.pond_profile)
    assert projected is not None
    shared = ("tenant_id", "pond_id", "device_id", "name", "firmware_version", "status", "version")
    assert {f: getattr(projected, f) for f in shared} == {f: getattr(legacy, f) for f in shared}
    row = client.item(KEYS.legacy_device(TENANT, device_id))
    lookup = client.item(KEYS.device_lookup(device_id))
    assert row["default_component_id"] == component_id
    assert row["v2_projection_version"] == 1
    assert row["auth_type"] == "mtls"
    assert {k: v for k, v in lookup.items() if k not in {"PK", "SK", "entity_type"}} == {
        k: v for k, v in row.items() if k not in {"PK", "SK", "entity_type"}
    }


# 1) Pure projection mappers ----------------------------------------------------------------


def asset(**overrides) -> Asset:
    values = {
        "tenant_id": TENANT,
        "asset_id": asset_id_for_legacy_pond(TENANT, "pond_west"),
        "site_id": default_site_id(TENANT),
        "asset_type": AssetType.POND,
        "name": "West",
        "created_at": dt("2026-08-25T10:00:00Z"),
        "updated_at": dt("2026-08-25T10:00:00Z"),
        "version": 1,
    }
    values.update(overrides)
    return Asset(**values)


def pond_profile(legacy_pond_id: str | None = "pond_west") -> PondProfile:
    return PondProfile(
        tenant_id=TENANT,
        asset_id=asset_id_for_legacy_pond(TENANT, "pond_west"),
        legacy_pond_id=legacy_pond_id,
        created_at=dt("2026-08-25T10:00:00Z"),
        updated_at=dt("2026-08-25T10:00:00Z"),
        version=1,
    )


def current_deployment(device_id: str, **overrides) -> Deployment:
    values = {
        "tenant_id": TENANT,
        "deployment_id": deployment_id_for_legacy_device(TENANT, device_id),
        "component_id": default_component_id_for_legacy_device(TENANT, device_id),
        "site_id": default_site_id(TENANT),
        "asset_id": asset_id_for_legacy_pond(TENANT, "pond_west"),
        "started_at": dt("2026-08-25T10:00:00Z"),
        "created_at": dt("2026-08-25T10:00:00Z"),
        "updated_at": dt("2026-08-25T10:00:00Z"),
        "version": 1,
    }
    values.update(overrides)
    return Deployment(**values)


def test_asset_pond_projection_preserves_v1_shape() -> None:
    projected = pond_from_asset(asset(), pond_profile(legacy_pond_id="pond_west"))
    assert projected.model_dump(mode="json") == {
        "created_at": "2026-08-25T10:00:00Z",
        "updated_at": "2026-08-25T10:00:00Z",
        "version": 1,
        "schema_version": 1,
        "status": "active",
        "tenant_id": "tnt_a",
        "pond_id": "pond_west",
        "name": "West",
        "description": None,
    }


def test_asset_without_legacy_alias_has_no_pond() -> None:
    with pytest.raises(ValueError):
        pond_from_asset(asset(), pond_profile(legacy_pond_id=None))


def test_native_v2_device_without_legacy_deployment_has_no_v1_projection() -> None:
    assert device_v1_projection(native_device(), current_deployment=None, pond_profile=None) is None


def test_device_projection_requires_default_component_deployed_to_a_legacy_pond() -> None:
    device = native_device(DEV_1)
    deployment = current_deployment(DEV_1)
    projected = device_v1_projection(device, deployment, pond_profile())
    assert projected is not None
    assert projected.model_dump(mode="json") == {
        "created_at": "2026-08-25T09:30:00Z",
        "updated_at": "2026-08-25T09:30:00Z",
        "version": 1,
        "schema_version": 1,
        "status": "active",
        "tenant_id": TENANT,
        "pond_id": "pond_west",
        "device_id": DEV_1,
        "name": "Native",
        "auth_type": "mtls",
        "firmware_version": None,
    }
    foreign_component = current_deployment(DEV_1, component_id="cmp_" + "1" * 32)
    assert device_v1_projection(device, foreign_component, pond_profile()) is None
    assert device_v1_projection(device, deployment, pond_profile(legacy_pond_id=None)) is None
    other_asset = current_deployment(DEV_1, asset_id="ast_" + "2" * 32)
    assert device_v1_projection(device, other_asset, pond_profile()) is None


def test_transition_never_lands_at_or_before_the_current_start() -> None:
    started = dt("2026-08-25T10:00:00Z")
    assert transition_instant(dt("2026-08-25T11:00:00Z"), started) == dt("2026-08-25T11:00:00Z")
    assert transition_instant(dt("2026-08-25T09:00:00Z"), started) == started + timedelta(
        microseconds=1
    )


def test_projection_protocol_is_the_exact_pond_device_subset_of_domain_repository() -> None:
    names = [
        "list_ponds", "get_pond", "create_pond", "update_pond",
        "list_devices", "get_device", "create_device", "update_device",
    ]
    for name in names:
        assert inspect.signature(getattr(V1PondDeviceProjection, name)) == inspect.signature(
            getattr(DomainRepository, name)
        ), name
    assert isinstance(projection(store()), V1PondDeviceProjection)
    assert isinstance(DynamoDomainRepository(TABLE, store()), V1PondDeviceProjection)


# 2) Transactional creates/updates and atomic relocation ------------------------------------


@pytest.mark.asyncio
async def test_first_pond_provisions_default_site_asset_and_profile_atomically() -> None:
    client = store()
    pond = await projection(client).create_pond(TENANT, "pond_west", "West", "Shallow")

    assert pond.model_dump(mode="json") == {
        "created_at": "2026-08-25T10:00:00Z",
        "updated_at": "2026-08-25T10:00:00Z",
        "version": 1,
        "schema_version": 1,
        "status": "active",
        "tenant_id": TENANT,
        "pond_id": "pond_west",
        "name": "West",
        "description": "Shallow",
    }
    assert len(client.transactions) == 1
    assert len(client.transactions[0]) == 4
    site_id = default_site_id(TENANT)
    asset_id = asset_id_for_legacy_pond(TENANT, "pond_west")
    pond_row = client.item(KEYS.legacy_pond(TENANT, "pond_west"))
    assert pond_row["created_at"] == "2026-08-25T10:00:00+00:00"
    assert (pond_row["v2_site_id"], pond_row["v2_asset_id"]) == (site_id, asset_id)
    assert pond_row["version"] == 1
    assert client.item(KEYS.site(TENANT, site_id)) == site_to_item(
        default_site_for_tenant(legacy_tenant_from_item(tenant_item()))
    )
    assert client.item(KEYS.pond_profile(asset_id))["legacy_pond_id"] == "pond_west"
    assert "pond_id" not in client.item(KEYS.asset(TENANT, asset_id))
    await assert_pond_projection_matches(client, "pond_west")


@pytest.mark.asyncio
async def test_second_pond_condition_checks_the_existing_default_site() -> None:
    client = store()
    repo = projection(client)
    await repo.create_pond(TENANT, "pond_west", "West", None)
    site_before = client.item(KEYS.site(TENANT, default_site_id(TENANT)))

    await repo.create_pond(TENANT, "pond_east", "East", None)

    actions = client.transactions[-1]
    assert sum("ConditionCheck" in action for action in actions) == 1
    assert client.item(KEYS.site(TENANT, default_site_id(TENANT))) == site_before
    await assert_pond_projection_matches(client, "pond_east")


@pytest.mark.asyncio
async def test_concurrent_first_pond_creators_converge_on_one_default_site() -> None:
    client = store()
    client.rendezvous_site_reads()
    results = await asyncio.gather(
        projection(client).create_pond(TENANT, "pond_west", "West", None),
        projection(client).create_pond(TENANT, "pond_east", "East", None),
    )

    assert {pond.pond_id for pond in results} == {"pond_west", "pond_east"}
    sites = [key for key in client.items if key[0] == f"TENANT#{TENANT}" and "SITE#" in key[1]]
    assert sites == [(f"TENANT#{TENANT}", f"SITE#{default_site_id(TENANT)}")]
    # The loser re-read the Site and committed with a ConditionCheck instead of a Put.
    assert len(client.transactions) == 3
    for pond_id in ("pond_west", "pond_east"):
        await assert_pond_projection_matches(client, pond_id)


@pytest.mark.asyncio
async def test_transaction_conflicts_are_retried_but_other_errors_are_not() -> None:
    def cancelled(*codes: str) -> ClientError:
        return ClientError(
            {
                "Error": {"Code": "TransactionCanceledException", "Message": "x"},
                "CancellationReasons": [{"Code": code} for code in codes],
            },
            "TransactWriteItems",
        )

    client = store()
    client.fail_transactions = [cancelled("None", "TransactionConflict", "None", "None")]
    await projection(client).create_pond(TENANT, "pond_west", "West", None)
    assert len(client.transactions) == 2

    client = store()
    client.fail_transactions = [cancelled("TransactionConflict")] * 3
    with pytest.raises(ClientError):
        await projection(client).create_pond(TENANT, "pond_west", "West", None)
    assert len(client.transactions) == 3

    client = store()
    client.fail_transactions = [cancelled("ThrottlingError", "TransactionConflict")]
    with pytest.raises(ClientError):
        await projection(client).create_pond(TENANT, "pond_west", "West", None)
    assert len(client.transactions) == 1


@pytest.mark.asyncio
async def test_pond_create_requires_an_active_default_site() -> None:
    client = store()
    await projection(client).create_pond(TENANT, "pond_west", "West", None)
    site_key = KEYS.site(TENANT, default_site_id(TENANT))
    client.items[(site_key["PK"], site_key["SK"])]["status"] = SiteStatus.ARCHIVED.value

    with pytest.raises(ConflictError):
        await projection(client).create_pond(TENANT, "pond_east", "East", None)
    assert client.item(KEYS.legacy_pond(TENANT, "pond_east")) is None


@pytest.mark.asyncio
async def test_pond_update_dual_writes_legacy_asset_and_profile_with_independent_versions() -> None:
    client = store()
    asset_id = seed_migrated_pond(client, "pond_west")
    asset_key = KEYS.asset(TENANT, asset_id)
    client.items[(asset_key["PK"], asset_key["SK"])]["version"] = 7

    pond = await projection(client).update_pond(TENANT, "pond_west", 1, "North", "Deep")

    assert pond.version == 2
    assert (pond.name, pond.description) == ("North", "Deep")
    assert pond.updated_at.isoformat() == "2026-08-25T10:00:00+00:00"
    assert len(client.transactions) == 1
    assert client.item(asset_key)["version"] == 8
    assert client.item(KEYS.pond_profile(asset_id))["version"] == 2
    assert client.item(KEYS.legacy_pond(TENANT, "pond_west"))["version"] == 2
    await assert_pond_projection_matches(client, "pond_west")


@pytest.mark.asyncio
async def test_empty_pond_patch_still_bumps_version_and_skips_the_profile() -> None:
    client = store()
    asset_id = seed_migrated_pond(client, "pond_west")

    pond = await projection(client).update_pond(TENANT, "pond_west", 1, None, None)

    assert (pond.version, pond.name) == (2, "West")
    assert client.item(KEYS.pond_profile(asset_id))["version"] == 1
    assert len(client.transactions[0]) == 2
    await assert_pond_projection_matches(client, "pond_west")


@pytest.mark.asyncio
async def test_pond_update_fences_the_v1_version_and_reports_missing_ponds() -> None:
    client = store()
    seed_migrated_pond(client, "pond_west")
    before = client.snapshot()

    with pytest.raises(ConflictError):
        await projection(client).update_pond(TENANT, "pond_west", 5, "North", None)
    with pytest.raises(NotFoundError):
        await projection(client).update_pond(TENANT, "pond_missing", 1, "North", None)
    assert client.snapshot() == before


@pytest.mark.asyncio
async def test_unmigrated_pond_update_lazily_provisions_its_projection() -> None:
    client = store()
    client.seed(legacy_pond_item("pond_west"))

    pond = await projection(client).update_pond(TENANT, "pond_west", 1, None, "Deep")

    assert pond.version == 2
    asset_id = asset_id_for_legacy_pond(TENANT, "pond_west")
    assert client.item(KEYS.asset(TENANT, asset_id))["version"] == 2
    assert client.item(KEYS.site(TENANT, default_site_id(TENANT))) is not None
    assert len(client.transactions) == 1
    await assert_pond_projection_matches(client, "pond_west")


@pytest.mark.asyncio
async def test_v1_device_create_writes_default_component_and_deployment_atomically() -> None:
    client = store()
    seed_migrated_pond(client, "pond_west")

    result = await projection(client).create_device(
        TENANT, "pond_west", DEV_1, "Gateway", "1.0.0"
    )

    assert result.model_dump(mode="json") == {
        "created_at": "2026-08-25T10:00:00Z",
        "updated_at": "2026-08-25T10:00:00Z",
        "version": 1,
        "schema_version": 1,
        "status": "active",
        "tenant_id": TENANT,
        "pond_id": "pond_west",
        "device_id": DEV_1,
        "name": "Gateway",
        "auth_type": "mtls",
        "firmware_version": "1.0.0",
    }
    assert len(client.transactions) == 1
    component_id = default_component_id_for_legacy_device(TENANT, DEV_1)
    deployments = DynamoDeploymentRepository(TABLE, client)
    current = await deployments.get_current_deployment(TENANT, component_id)
    assert current.asset_id == asset_id_for_legacy_pond(TENANT, "pond_west")
    assert current.deployment_id == deployment_id_for_legacy_device(TENANT, DEV_1)
    assert current.started_at == dt("2026-08-25T10:00:00Z")
    component = await DynamoDeviceComponentRepository(TABLE, client).get_component(
        TENANT, component_id
    )
    assert component.probe_profile.probe_profile_id == default_probe_profile_id_for_legacy_device(
        TENANT, DEV_1
    )
    assert client.item(KEYS.component_mirror(TENANT, component_id)) is not None
    assert client.item(deployment_marker_key(component_id))["seq"] == 1
    assert client.item(KEYS.deployment_mirror(TENANT, current.deployment_id)) is not None
    await assert_device_projection_matches(client, DEV_1)


@pytest.mark.asyncio
async def test_device_create_into_unmigrated_pond_provisions_it_without_bumping_the_pond() -> None:
    client = store()
    client.seed(legacy_pond_item("pond_west"))

    await projection(client).create_device(TENANT, "pond_west", DEV_1, "Gateway", None)

    pond_row = client.item(KEYS.legacy_pond(TENANT, "pond_west"))
    assert pond_row["version"] == 1
    assert pond_row["v2_asset_id"] == asset_id_for_legacy_pond(TENANT, "pond_west")
    assert len(client.transactions) == 1
    await assert_pond_projection_matches(client, "pond_west")
    await assert_device_projection_matches(client, DEV_1)


@pytest.mark.asyncio
async def test_device_create_with_dangling_pond_is_a_conflict() -> None:
    client = store()
    before = client.snapshot()

    with pytest.raises(ConflictError, match="pond not found"):
        await projection(client).create_device(TENANT, "pond_missing", DEV_1, "Gateway", None)
    assert client.snapshot() == before
    assert client.transactions == []


@pytest.mark.asyncio
async def test_v1_pond_change_closes_history_without_reassignment() -> None:
    client = store()
    seed_migrated_pond(client, "pond_west")
    seed_migrated_pond(client, "pond_east", name="East")
    clock = Clock("2026-08-25T10:00:00Z")
    await projection(client, clock).create_device(TENANT, "pond_west", DEV_1, "Gateway", None)

    clock.set("2026-08-25T11:00:00Z")
    result = await projection(client, clock).update_device(
        TENANT, DEV_1, 1, None, "pond_east", None
    )

    component_id = default_component_id_for_legacy_device(TENANT, DEV_1)
    deployments = DynamoDeploymentRepository(TABLE, client)
    assert result.pond_id == "pond_east"
    assert result.version == 2
    assert len(client.transactions) == 2
    west = await deployments.get_deployment_at(TENANT, component_id, dt("2026-08-25T10:30:00Z"))
    east = await deployments.get_deployment_at(TENANT, component_id, dt("2026-08-25T11:30:00Z"))
    assert west.asset_id == asset_id_for_legacy_pond(TENANT, "pond_west")
    assert west.deployment_id == deployment_id_for_legacy_device(TENANT, DEV_1)
    assert (west.status, west.ended_at, west.version) == (
        DeploymentStatus.ENDED,
        dt("2026-08-25T11:00:00Z"),
        2,
    )
    assert east.asset_id == asset_id_for_legacy_pond(TENANT, "pond_east")
    assert east.started_at == dt("2026-08-25T11:00:00Z")
    history = await deployments.list_component_deployments(TENANT, component_id)
    assert [d.deployment_id for d in history] == [west.deployment_id, east.deployment_id]
    assert (await deployments.get_deployment(TENANT, west.deployment_id)).status == "ended"
    marker = client.item(deployment_marker_key(component_id))
    assert (marker["seq"], marker["last_ended_at"]) == (2, "2026-08-25T11:00:00.000000Z")
    await assert_device_projection_matches(client, DEV_1)


@pytest.mark.asyncio
async def test_pond_move_at_the_same_instant_starts_one_microsecond_later() -> None:
    client = store()
    seed_migrated_pond(client, "pond_west")
    seed_migrated_pond(client, "pond_east", name="East")
    clock = Clock("2026-08-25T10:00:00Z")
    await projection(client, clock).create_device(TENANT, "pond_west", DEV_1, "Gateway", None)

    await projection(client, clock).update_device(TENANT, DEV_1, 1, None, "pond_east", None)

    component_id = default_component_id_for_legacy_device(TENANT, DEV_1)
    current = await DynamoDeploymentRepository(TABLE, client).get_current_deployment(
        TENANT, component_id
    )
    assert current.started_at == dt("2026-08-25T10:00:00.000001Z")


@pytest.mark.asyncio
async def test_unchanged_pond_id_is_not_a_transition() -> None:
    client = store()
    seed_migrated_pond(client, "pond_west")
    await projection(client).create_device(TENANT, "pond_west", DEV_1, "Gateway", None)

    result = await projection(client).update_device(
        TENANT, DEV_1, 1, "Renamed", "pond_west", "2.0.0"
    )

    assert (result.name, result.firmware_version, result.version) == ("Renamed", "2.0.0", 2)
    assert len(client.transactions[-1]) == 2
    component_id = default_component_id_for_legacy_device(TENANT, DEV_1)
    history = await DynamoDeploymentRepository(TABLE, client).list_component_deployments(
        TENANT, component_id
    )
    assert len(history) == 1
    await assert_device_projection_matches(client, DEV_1)


@pytest.mark.asyncio
async def test_empty_device_patch_still_bumps_the_shared_version() -> None:
    client = store()
    seed_migrated_pond(client, "pond_west")
    await projection(client).create_device(TENANT, "pond_west", DEV_1, "Gateway", None)

    result = await projection(client).update_device(TENANT, DEV_1, 1, None, None, None)

    assert result.version == 2
    assert client.item(KEYS.device_lookup(DEV_1))["version"] == 2
    await assert_device_projection_matches(client, DEV_1)


@pytest.mark.asyncio
async def test_device_move_to_dangling_pond_is_a_conflict_and_writes_nothing() -> None:
    client = store()
    seed_migrated_pond(client, "pond_west")
    await projection(client).create_device(TENANT, "pond_west", DEV_1, "Gateway", None)
    before = client.snapshot()

    with pytest.raises(ConflictError, match="pond not found"):
        await projection(client).update_device(TENANT, DEV_1, 1, None, "pond_missing", None)
    assert client.snapshot() == before


@pytest.mark.asyncio
async def test_device_version_fence_rejects_stale_writes() -> None:
    client = store()
    seed_migrated_pond(client, "pond_west")
    await projection(client).create_device(TENANT, "pond_west", DEV_1, "Gateway", None)

    with pytest.raises(ConflictError):
        await projection(client).update_device(TENANT, DEV_1, 3, "X", None, None)


@pytest.mark.asyncio
async def test_unmigrated_device_update_lazily_provisions_component_and_deployment() -> None:
    client = store()
    client.seed(legacy_pond_item("pond_west"))
    for row in legacy_device_rows("local-device-001", "pond_west"):
        client.seed(row)

    result = await projection(client).update_device(
        TENANT, "local-device-001", 1, "Renamed", None, None
    )

    assert result.version == 2
    assert len(client.transactions) == 1
    component_id = default_component_id_for_legacy_device(TENANT, "local-device-001")
    current = await DynamoDeploymentRepository(TABLE, client).get_current_deployment(
        TENANT, component_id
    )
    assert current.deployment_id == deployment_id_for_legacy_device(TENANT, "local-device-001")
    assert current.started_at == dt(CREATED)
    await assert_device_projection_matches(client, "local-device-001")
    await assert_pond_projection_matches(client, "pond_west")


@pytest.mark.asyncio
async def test_unmigrated_device_move_reconstructs_the_old_interval_then_moves() -> None:
    client = store()
    client.seed(legacy_pond_item("pond_west"))
    client.seed(legacy_pond_item("pond_east", name="East"))
    for row in legacy_device_rows(DEV_1, "pond_west"):
        client.seed(row)
    clock = Clock("2026-08-25T11:00:00Z")

    await projection(client, clock).update_device(TENANT, DEV_1, 1, None, "pond_east", None)

    assert len(client.transactions) == 1
    assert len(client.transactions[0]) <= 25
    component_id = default_component_id_for_legacy_device(TENANT, DEV_1)
    deployments = DynamoDeploymentRepository(TABLE, client)
    west = await deployments.get_deployment_at(TENANT, component_id, dt("2026-08-25T10:00:00Z"))
    east = await deployments.get_deployment_at(TENANT, component_id, dt("2026-08-25T12:00:00Z"))
    assert west.asset_id == asset_id_for_legacy_pond(TENANT, "pond_west")
    assert west.started_at == dt(CREATED)
    assert west.ended_at == dt("2026-08-25T11:00:00Z")
    assert east.asset_id == asset_id_for_legacy_pond(TENANT, "pond_east")
    marker = client.item(deployment_marker_key(component_id))
    assert marker["seq"] == 2
    await assert_device_projection_matches(client, DEV_1)


@pytest.mark.asyncio
async def test_concurrent_creates_into_one_unmigrated_pond_both_succeed() -> None:
    client = store()
    client.seed(legacy_pond_item("pond_west"))
    client.rendezvous_site_reads()

    await asyncio.gather(
        projection(client).create_device(TENANT, "pond_west", DEV_1, "A", None),
        projection(client).create_device(TENANT, "pond_west", DEV_2, "B", None),
    )

    for device_id in (DEV_1, DEV_2):
        await assert_device_projection_matches(client, device_id)


# 3) Hardened legacy reads for native v2-only Device rows -----------------------------------


@pytest.mark.asyncio
async def test_native_v2_device_is_invisible_to_both_v1_read_paths() -> None:
    client = store()
    seed_migrated_pond(client, "pond_west")
    for row in legacy_device_rows("dev_legacy", "pond_west"):
        client.seed(row)
    native = seed_native_device(client)

    for repo in (DynamoDomainRepository(TABLE, client), projection(client)):
        assert [d.device_id for d in await repo.list_devices(TENANT)] == ["dev_legacy"]
        assert await repo.get_device(TENANT, native.device_id) is None
        with pytest.raises(NotFoundError):
            await repo.update_device(TENANT, native.device_id, 1, "X", None, None)
    assert client.item(KEYS.device(TENANT, native.device_id)) == device_v2_to_item(native)


@pytest.mark.asyncio
async def test_reads_never_create_or_promote_data() -> None:
    client = store(page_size=1)
    client.seed(legacy_pond_item("pond_west"))
    client.seed(legacy_pond_item("pond_east", name="East"))
    for row in legacy_device_rows(DEV_1, "pond_west"):
        client.seed(row)
    before = client.snapshot()
    repo = projection(client)

    assert [p.pond_id for p in await repo.list_ponds(TENANT)] == ["pond_east", "pond_west"]
    assert (await repo.get_pond(TENANT, "pond_west")).name == "West"
    assert [d.device_id for d in await repo.list_devices(TENANT)] == [DEV_1]
    assert (await repo.get_device(TENANT, DEV_1)).pond_id == "pond_west"
    assert await repo.get_pond(OTHER_TENANT, "pond_west") is None
    assert client.snapshot() == before
    assert client.transactions == []
    assert any("ExclusiveStartKey" in query for query in client.queries)


@pytest.mark.asyncio
async def test_flag_off_after_flag_on_writes_reads_legacy_rows_unchanged() -> None:
    client = store()
    repo = projection(client)
    created_pond = await repo.create_pond(TENANT, "pond_west", "West", None)
    await repo.create_pond(TENANT, "pond_east", "East", None)
    await repo.create_device(TENANT, "pond_west", DEV_1, "Gateway", "1.0.0")
    moved = await repo.update_device(TENANT, DEV_1, 1, "Moved", "pond_east", None)
    seed_native_device(client)

    legacy = DynamoDomainRepository(TABLE, client)
    assert await legacy.get_pond(TENANT, "pond_west") == created_pond
    assert await legacy.list_ponds(TENANT) == await repo.list_ponds(TENANT)
    assert await legacy.get_device(TENANT, DEV_1) == moved
    assert await legacy.list_devices(TENANT) == [moved]
    renamed = await legacy.update_device(TENANT, DEV_1, 2, "Legacy", None, None)
    assert renamed.version == 3
    assert await repo.get_device(TENANT, DEV_1) == renamed


@pytest.mark.asyncio
async def test_projection_writes_never_scan() -> None:
    client = store()
    repo = projection(client)
    await repo.create_pond(TENANT, "pond_west", "West", None)
    await repo.create_device(TENANT, "pond_west", DEV_1, "Gateway", None)
    await repo.list_devices(TENANT)
    assert client.scan_calls == 0


def test_pond_model_round_trip_is_unchanged() -> None:
    # Guard against accidental schema changes of the legacy entity the projection returns.
    assert set(Pond.model_fields) == {
        "created_at", "updated_at", "version", "schema_version", "status",
        "tenant_id", "pond_id", "name", "description",
    }
    assert AssetStatus.ACTIVE.value == "active"


# Audit follow-ups ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_noncanonical_legacy_device_writes_are_409_not_500() -> None:
    client = store()
    seed_migrated_pond(client, "pond_west")
    seed_migrated_pond(client, "pond_east", name="East")
    for row in legacy_device_rows("dev_1", "pond_west"):
        client.seed(row)
    before = client.snapshot()
    repo = projection(client)

    for kwargs in (
        {"name": "Renamed", "pond_id": None},
        {"name": None, "pond_id": "pond_east"},
        {"name": None, "pond_id": None},
    ):
        with pytest.raises(ConflictError, match="device is not compatible with the v2 projection"):
            await repo.update_device(TENANT, "dev_1", 1, firmware_version=None, **kwargs)
    with pytest.raises(ConflictError, match="device is not compatible with the v2 projection"):
        await repo.create_device(TENANT, "pond_west", "dev_bad", "X", None)
    assert client.snapshot() == before
    assert client.transactions == []
    # Reads and the legacy repository are unchanged for the same row.
    assert (await repo.get_device(TENANT, "dev_1")).device_id == "dev_1"
    legacy = DynamoDomainRepository(TABLE, client)
    assert (await legacy.update_device(TENANT, "dev_1", 1, "Legacy", None, None)).version == 2


@pytest.mark.asyncio
async def test_unsafe_legacy_pond_identifier_is_409_and_missing_is_404() -> None:
    client = store()
    client.seed(legacy_pond_item("pond#odd"))
    repo = projection(client)

    assert (await repo.get_pond(TENANT, "pond#odd")).pond_id == "pond#odd"
    with pytest.raises(ConflictError, match="pond is not compatible with the v2 projection"):
        await repo.update_pond(TENANT, "pond#odd", 1, "X", None)
    with pytest.raises(ConflictError, match="pond is not compatible with the v2 projection"):
        await repo.create_device(TENANT, "pond#odd", DEV_1, "X", None)
    with pytest.raises(NotFoundError):
        await repo.update_pond(TENANT, "pond#missing", 1, "X", None)
    with pytest.raises(NotFoundError):
        await repo.update_device(TENANT, "dev#missing", 1, "X", None, None)
    assert client.transactions == []


@pytest.mark.asyncio
async def test_unmigrated_decommissioned_device_gets_an_undeployed_component() -> None:
    client = store()
    seed_migrated_pond(client, "pond_west")
    seed_migrated_pond(client, "pond_east", name="East")
    for row in legacy_device_rows(DEV_1, "pond_west"):
        row["status"] = "decommissioned"
        client.seed(row)
    component_id = default_component_id_for_legacy_device(TENANT, DEV_1)

    with pytest.raises(ConflictError, match="component is not active"):
        await projection(client).update_device(TENANT, DEV_1, 1, None, "pond_east", None)
    assert client.transactions == []

    renamed = await projection(client).update_device(TENANT, DEV_1, 1, "Retired", None, None)

    assert renamed.version == 2
    component = client.item(KEYS.component(component_id))
    assert component["status"] == "decommissioned"
    assert client.item(KEYS.current_deployment(component_id)) is None
    assert client.item(deployment_marker_key(component_id)) is None
    assert not [key for key in client.items if key[0] == f"COMPONENT#{component_id}"
                and key[1].startswith("DEPLOYMENT#")]
    with pytest.raises(ConflictError, match="component is not active"):
        await projection(client).update_device(TENANT, DEV_1, 2, None, "pond_east", None)


@pytest.mark.asyncio
async def test_device_into_unmigrated_archived_pond_is_409() -> None:
    client = store()
    client.seed({**legacy_pond_item("pond_west"), "status": "archived"})
    client.seed(legacy_pond_item("pond_east", name="East"))
    for row in legacy_device_rows(DEV_1, "pond_east"):
        client.seed(row)

    with pytest.raises(ConflictError, match="pond is not active"):
        await projection(client).create_device(TENANT, "pond_west", DEV_2, "X", None)
    with pytest.raises(ConflictError, match="pond is not active"):
        await projection(client).update_device(TENANT, DEV_1, 1, None, "pond_west", None)
    assert client.transactions == []


@pytest.mark.asyncio
async def test_dangling_pond_device_move_starts_a_fresh_interval_without_the_legacy_one() -> None:
    client = store()
    seed_migrated_pond(client, "pond_east", name="East")
    for row in legacy_device_rows(DEV_1, "pond_gone"):
        client.seed(row)
    clock = Clock("2026-08-25T11:00:00.5Z")

    moved = await projection(client, clock).update_device(TENANT, DEV_1, 1, None, "pond_east", None)

    assert moved.pond_id == "pond_east"
    component_id = default_component_id_for_legacy_device(TENANT, DEV_1)
    deployments = DynamoDeploymentRepository(TABLE, client)
    history = await deployments.list_component_deployments(TENANT, component_id)
    assert len(history) == 1
    (current,) = history
    assert current.deployment_id != deployment_id_for_legacy_device(TENANT, DEV_1)
    assert current.started_at == dt("2026-08-25T11:00:00.5Z")
    assert current.asset_id == asset_id_for_legacy_pond(TENANT, "pond_east")
    assert await deployments.get_deployment_at(TENANT, component_id, dt(CREATED)) is None
    assert client.item(deployment_marker_key(component_id))["seq"] == 1
    await assert_device_projection_matches(client, DEV_1)


@pytest.mark.asyncio
async def test_provisioning_conflicts_back_off_before_retrying() -> None:
    client = store()
    client.rendezvous_site_reads()
    sleeps: list[int] = []

    async def record_sleep(attempt: int) -> None:
        sleeps.append(attempt)

    def repo() -> DynamoV1PondDeviceProjection:
        return DynamoV1PondDeviceProjection(
            TABLE, client, clock=Clock("2026-08-25T10:00:00Z"), retry_sleep=record_sleep
        )

    await asyncio.gather(
        repo().create_pond(TENANT, "pond_west", "West", None),
        repo().create_pond(TENANT, "pond_east", "East", None),
    )

    assert sleeps == [1]


@pytest.mark.asyncio
async def test_site_failures_use_a_neutral_detail() -> None:
    client = store()
    await projection(client).create_pond(TENANT, "pond_west", "West", None)
    site_key = KEYS.site(TENANT, default_site_id(TENANT))
    client.items[(site_key["PK"], site_key["SK"])]["status"] = SiteStatus.ARCHIVED.value

    with pytest.raises(ConflictError) as raised:
        await projection(client).create_pond(TENANT, "pond_east", "East", None)
    assert str(raised.value) == "site not available"
