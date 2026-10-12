import re
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError

from limnopulse_api.adapters.dynamodb_deployments import (
    deployment_marker_item,
    deployment_to_item,
)
from limnopulse_api.adapters.dynamodb_devices_v2 import component_to_item
from limnopulse_api.adapters.dynamodb_sites_assets import (
    asset_to_item,
    pond_profile_to_item,
    site_to_item,
)
from limnopulse_api.adapters.dynamodb_v2 import DynamoV2KeyBuilder
from limnopulse_api.domain.deployments import Deployment, DeploymentStatus
from limnopulse_api.domain.entities import Device, Pond, Tenant
from limnopulse_api.domain.ids import (
    asset_id_for_legacy_pond,
    default_component_id_for_legacy_device,
    default_site_id,
    deployment_id_for_legacy_device,
)
from limnopulse_api.domain.legacy_projection import (
    asset_and_profile_for_legacy_pond,
    component_for_legacy_device,
    default_site_for_tenant,
    deployment_for_legacy_device,
)
from limnopulse_api.migrations.default_site_asset import (
    DefaultSiteAssetMigrator,
    exit_code_for,
    normalize_tenant_ids,
)

TABLE = "LimnopulseDomain"
KEYS = DynamoV2KeyBuilder()
TENANT_CREATED = datetime(2026, 1, 2, 3, 4, 5, 600000, tzinfo=UTC)
POND_CREATED = datetime(2026, 2, 3, 4, 5, 6, 700000, tzinfo=UTC)
DEVICE_CREATED = datetime(2026, 3, 4, 5, 6, 7, 800000, tzinfo=UTC)
LATER = datetime(2026, 4, 5, 6, 7, 8, tzinfo=UTC)
FIRST_RUN = datetime(2026, 8, 25, 12, tzinfo=UTC)
SECOND_RUN = datetime(2026, 9, 30, 18, tzinfo=UTC)
WRITE_OPERATIONS = ("put_item", "update_item", "transact_write_items", "delete_item")


def fixed_clock() -> datetime:
    return FIRST_RUN


def second_clock() -> datetime:
    return SECOND_RUN


# Local fake ----------------------------------------------------------------------------------


def _split_top_level(expression: str, separator: str) -> list[str]:
    parts, depth, current, index = [], 0, "", 0
    while index < len(expression):
        if depth == 0 and expression.startswith(separator, index):
            parts.append(current.strip())
            current = ""
            index += len(separator)
            continue
        char = expression[index]
        depth += char == "("
        depth -= char == ")"
        current += char
        index += 1
    parts.append(current.strip())
    return parts


class FakeMigrationDynamo:
    """Single-table fake: consistent GetItem, prefix Query with Limit, conditional writes."""

    def __init__(self) -> None:
        self.serializer = TypeSerializer()
        self.deserializer = TypeDeserializer()
        self.items: dict[tuple[str, str], dict[str, Any]] = {}
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.scan_calls: list[dict[str, Any]] = []
        self.before_transact: Callable[[FakeMigrationDynamo, list[dict]], None] | None = None
        self.fail_operation: tuple[str, Exception] | None = None
        self.fail_once: dict[str, Exception] = {}
        self.before_get_item: Callable[[FakeMigrationDynamo, tuple[str, str]], None] | None = None

    # Builders ---------------------------------------------------------------------------------

    @classmethod
    def with_legacy_tenant(
        cls,
        *,
        tenant_id: str,
        ponds: list[dict[str, Any]],
        devices: list[dict[str, Any]],
    ) -> "FakeMigrationDynamo":
        client = cls()
        client.add_legacy_tenant(tenant_id, ponds=ponds, devices=devices)
        return client

    @classmethod
    def with_conflicting_asset(
        cls, *, tenant_id: str, pond_id: str, site_id: str
    ) -> "FakeMigrationDynamo":
        client = cls.with_legacy_tenant(
            tenant_id=tenant_id, ponds=[legacy_pond(pond_id, tenant_id=tenant_id)], devices=[]
        )
        projection = asset_and_profile_for_legacy_pond(
            pond_model(pond_id, tenant_id=tenant_id), site_id
        )
        client.put_raw(asset_to_item(projection.asset))
        return client

    def add_legacy_tenant(
        self, tenant_id: str, *, ponds: list[dict[str, Any]], devices: list[dict[str, Any]]
    ) -> None:
        self.put_raw(legacy_tenant(tenant_id))
        for pond in ponds:
            self.put_raw({**pond, "PK": f"TENANT#{tenant_id}", "tenant_id": tenant_id})
        for device in devices:
            device = {**device, "tenant_id": tenant_id}
            self.put_raw({**device, "PK": f"TENANT#{tenant_id}"})
            self.put_raw(
                {
                    **device,
                    "PK": f"DEVICE#{device['device_id']}",
                    "SK": "META",
                    "entity_type": "device_lookup",
                }
            )

    def put_raw(self, item: dict[str, Any]) -> None:
        normalized = self.deserialize(self.serialize(item))
        self.items[(normalized["PK"], normalized["SK"])] = normalized

    def get_raw(self, pk: str, sk: str) -> dict[str, Any] | None:
        return self.items.get((pk, sk))

    # Inspection -------------------------------------------------------------------------------

    @property
    def write_calls(self) -> list[tuple[str, dict[str, Any]]]:
        return [call for call in self.calls if call[0] in WRITE_OPERATIONS]

    @property
    def query_calls(self) -> list[dict[str, Any]]:
        return [request for name, request in self.calls if name == "query"]

    @property
    def transactions(self) -> list[list[dict[str, Any]]]:
        return [
            request["TransactItems"]
            for name, request in self.calls
            if name == "transact_write_items"
        ]

    # boto3 surface ----------------------------------------------------------------------------

    def scan(self, **kwargs: Any) -> dict[str, Any]:
        self.scan_calls.append(kwargs)
        raise AssertionError("Scan is forbidden")

    def get_item(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("get_item", kwargs))
        self._maybe_fail("get_item")
        key = self.deserialize(kwargs["Key"])
        if self.before_get_item is not None:
            self.before_get_item(self, (key["PK"], key["SK"]))
        item = self.items.get((key["PK"], key["SK"]))
        return {} if item is None else {"Item": self.serialize(item)}

    def query(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("query", kwargs))
        self._maybe_fail("query")
        assert kwargs["KeyConditionExpression"] == "PK = :pk AND begins_with(SK, :sk_prefix)"
        values = self.deserialize(kwargs["ExpressionAttributeValues"])
        matches = sorted(
            (
                item
                for (pk, sk), item in self.items.items()
                if pk == values[":pk"] and sk.startswith(values[":sk_prefix"])
            ),
            key=lambda item: item["SK"],
        )
        start = kwargs.get("ExclusiveStartKey")
        if start is not None:
            start_sk = self.deserialize(start)["SK"]
            matches = [item for item in matches if item["SK"] > start_sk]
        limit = kwargs.get("Limit")
        page = matches if limit is None else matches[:limit]
        response: dict[str, Any] = {"Items": [self.serialize(item) for item in page]}
        # Like DynamoDB, a page that reaches Limit carries LastEvaluatedKey even at the end.
        if limit is not None and page and len(matches) >= limit:
            response["LastEvaluatedKey"] = self.serialize(
                {"PK": page[-1]["PK"], "SK": page[-1]["SK"]}
            )
        return response

    def put_item(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("put_item", kwargs))
        self._maybe_fail("put_item")
        if not self._evaluate({"Put": kwargs}):
            raise _conditional_failure()
        self._commit({"Put": kwargs})
        return {}

    def update_item(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("update_item", kwargs))
        self._maybe_fail("update_item")
        if not self._evaluate({"Update": kwargs}):
            raise _conditional_failure()
        self._commit({"Update": kwargs})
        return {}

    def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("transact_write_items", kwargs))
        self._maybe_fail("transact_write_items")
        actions = kwargs["TransactItems"]
        assert 1 <= len(actions) <= 25
        keys = [self._action_key(action) for action in actions]
        assert len(set(keys)) == len(keys), "transaction keys must be distinct"
        if self.before_transact is not None:
            hook, self.before_transact = self.before_transact, None
            hook(self, actions)
        reasons = [
            {"Code": "None"} if self._evaluate(action) else {"Code": "ConditionalCheckFailed"}
            for action in actions
        ]
        if any(reason["Code"] != "None" for reason in reasons):
            raise ClientError(
                {
                    "Error": {"Code": "TransactionCanceledException", "Message": "cancelled"},
                    "CancellationReasons": reasons,
                },
                "TransactWriteItems",
            )
        for action in actions:
            self._commit(action)
        return {}

    def apply_unconditionally(self, actions: list[dict[str, Any]]) -> None:
        """A concurrent writer committing the same rows first."""
        for action in actions:
            self._commit(action)

    # Internals --------------------------------------------------------------------------------

    def serialize(self, item: dict[str, Any]) -> dict[str, Any]:
        return {key: self.serializer.serialize(value) for key, value in item.items()}

    def deserialize(self, item: dict[str, Any]) -> dict[str, Any]:
        return {key: self.deserializer.deserialize(value) for key, value in item.items()}

    def _maybe_fail(self, operation: str) -> None:
        if operation in self.fail_once:
            raise self.fail_once.pop(operation)
        if self.fail_operation is not None and self.fail_operation[0] == operation:
            raise self.fail_operation[1]

    def _action_key(self, action: dict[str, Any]) -> tuple[str, str]:
        ((kind, request),) = action.items()
        raw = request["Item"] if kind == "Put" else request["Key"]
        key = self.deserialize({"PK": raw["PK"], "SK": raw["SK"]})
        return key["PK"], key["SK"]

    def _evaluate(self, action: dict[str, Any]) -> bool:
        ((_, request),) = action.items()
        condition = request.get("ConditionExpression")
        if condition is None:
            return True
        item = self.items.get(self._action_key(action))
        names = request.get("ExpressionAttributeNames", {})
        values = self.deserialize(request.get("ExpressionAttributeValues", {}))
        for clause in _split_top_level(condition, " AND "):
            exists = re.fullmatch(r"attribute_(not_)?exists\((\S+)\)", clause)
            if exists:
                name = names.get(exists.group(2), exists.group(2))
                present = item is not None and name in item
                if present == bool(exists.group(1)):
                    return False
                continue
            left, right = (part.strip() for part in clause.split("="))
            if item is None or item.get(names.get(left, left)) != values[right]:
                return False
        return True

    def _commit(self, action: dict[str, Any]) -> None:
        ((kind, request),) = action.items()
        if kind == "ConditionCheck":
            return
        key = self._action_key(action)
        if kind == "Put":
            self.items[key] = self.deserialize(request["Item"])
            return
        item = dict(self.items.get(key) or {"PK": key[0], "SK": key[1]})
        names = request.get("ExpressionAttributeNames", {})
        values = self.deserialize(request.get("ExpressionAttributeValues", {}))
        expression = request["UpdateExpression"]
        assert expression.startswith("SET ")
        for assignment in _split_top_level(expression[4:], ","):
            target, value = (part.strip() for part in assignment.split("=", 1))
            item[names[target]] = self._value(value, item, names, values)
        self.items[key] = item

    def _value(self, expression: str, item: dict, names: dict, values: dict) -> Any:
        terms = _split_top_level(expression, "+")
        if len(terms) > 1:
            return sum(self._value(term, item, names, values) for term in terms)
        default = re.fullmatch(r"if_not_exists\((\S+),\s*(\S+)\)", expression)
        if default:
            name = names[default.group(1)]
            return item[name] if name in item else values[default.group(2)]
        return values[expression]


def _conditional_failure() -> ClientError:
    return ClientError(
        {"Error": {"Code": "ConditionalCheckFailedException", "Message": "failed"}},
        "PutItem",
    )


# Fixtures --------------------------------------------------------------------------------------


def legacy_tenant(tenant_id: str) -> dict[str, Any]:
    return {
        "PK": f"TENANT#{tenant_id}",
        "SK": "META",
        "entity_type": "tenant",
        "tenant_id": tenant_id,
        "name": "Fazenda Azul",
        "settings": {},
        "status": "active",
        "created_at": TENANT_CREATED.isoformat(),
        "updated_at": LATER.isoformat(),
        "version": 4,
        "schema_version": 1,
    }


def legacy_pond(pond_id: str, *, tenant_id: str = "tnt_1", version: int = 2) -> dict[str, Any]:
    return {
        "PK": f"TENANT#{tenant_id}",
        "SK": f"POND#{pond_id}",
        "entity_type": "pond",
        "tenant_id": tenant_id,
        "pond_id": pond_id,
        "name": f"Viveiro {pond_id}",
        "description": None,
        "status": "active",
        "created_at": POND_CREATED.isoformat(),
        "updated_at": LATER.isoformat(),
        "version": version,
        "schema_version": 1,
    }


def legacy_device(
    device_id: str, pond_id: str, *, tenant_id: str = "tnt_1", status: str = "active"
) -> dict[str, Any]:
    return {
        "PK": f"TENANT#{tenant_id}",
        "SK": f"DEVICE#{device_id}",
        "entity_type": "device",
        "tenant_id": tenant_id,
        "pond_id": pond_id,
        "device_id": device_id,
        "name": f"Sonda {device_id}",
        "auth_type": "mtls",
        "firmware_version": "1.0.0",
        "status": status,
        "created_at": DEVICE_CREATED.isoformat(),
        "updated_at": LATER.isoformat(),
        "version": 3,
        "schema_version": 1,
    }


def tenant_model(tenant_id: str = "tnt_1") -> Tenant:
    return Tenant(
        tenant_id=tenant_id,
        name="Fazenda Azul",
        created_at=TENANT_CREATED,
        updated_at=LATER,
        version=4,
    )


def pond_model(pond_id: str, *, tenant_id: str = "tnt_1") -> Pond:
    return Pond(
        tenant_id=tenant_id,
        pond_id=pond_id,
        name=f"Viveiro {pond_id}",
        created_at=POND_CREATED,
        updated_at=LATER,
        version=2,
    )


def device_model(device_id: str, pond_id: str, *, status: str = "active") -> Device:
    return Device(
        tenant_id="tnt_1",
        pond_id=pond_id,
        device_id=device_id,
        name=f"Sonda {device_id}",
        firmware_version="1.0.0",
        status=status,
        created_at=DEVICE_CREATED,
        updated_at=LATER,
        version=3,
    )


def run(client: FakeMigrationDynamo, *, apply: bool, clock=fixed_clock, **kwargs: Any):
    options = {"page_size": 25, "deadline_seconds": 300, **kwargs}
    tenants = options.pop("tenants", ["tnt_1"])
    return DefaultSiteAssetMigrator(client, clock=clock).run(tenants, apply=apply, **options)


def normalized(item: dict[str, Any]) -> dict[str, Any]:
    return FakeMigrationDynamo().deserialize(FakeMigrationDynamo().serialize(item))


def migrated_store() -> FakeMigrationDynamo:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1")],
        devices=[legacy_device("probe-1", "pond_1")],
    )
    assert run(client, apply=True).result == "success"
    return client


# Step 1: dry-run --------------------------------------------------------------------------------


def test_dry_run_has_no_writes_and_reports_exact_scope() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1")],
        devices=[legacy_device("probe-1", "pond_1")],
    )

    result = DefaultSiteAssetMigrator(client, clock=fixed_clock).run(
        ["tnt_1"], apply=False, page_size=25, deadline_seconds=300
    )

    assert result.model_dump() == {
        "apply": False,
        "tenants_requested": 1,
        "tenants_completed": 1,
        "default_sites_created": 0,
        "assets_created": 0,
        "devices_projected": 0,
        "components_created": 0,
        "deployments_created": 0,
        "legacy_links_updated": 0,
        "would_create_default_sites": 1,
        "would_create_assets": 1,
        "would_project_devices": 1,
        "would_create_components": 1,
        "would_create_deployments": 1,
        "would_update_legacy_links": 2,
        "already_migrated": 0,
        "schema_conflicts": 0,
        "conflicts": [],
        "scope_completed": True,
        "result": "success",
    }
    assert client.write_calls == []
    assert client.scan_calls == []


def test_dry_run_is_deterministic_and_reads_are_identical() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1"), legacy_pond("pond_2")],
        devices=[legacy_device("probe-1", "pond_1")],
    )
    first = run(client, apply=False)
    first_calls = list(client.calls)
    client.calls.clear()
    second = run(client, apply=False, clock=second_clock)

    assert first == second
    assert client.calls == first_calls
    assert client.write_calls == []


def test_dry_run_of_migrated_tenant_reports_already_migrated_only() -> None:
    client = migrated_store()
    client.calls.clear()

    result = run(client, apply=False)

    assert result.already_migrated == 2
    assert result.would_create_default_sites == 0
    assert result.would_update_legacy_links == 0
    assert client.write_calls == []


# Step 2: explicit-tenant query planning ----------------------------------------------------------


def test_queries_only_explicit_tenant_partitions_with_paginated_limit() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1"), legacy_pond("pond_2"), legacy_pond("pond_3")],
        devices=[legacy_device("probe-1", "pond_1"), legacy_device("probe-2", "pond_3")],
    )
    client.add_legacy_tenant(
        "tnt_other",
        ponds=[legacy_pond("pond_x", tenant_id="tnt_other")],
        devices=[],
    )

    result = run(client, apply=False, page_size=2)

    assert result.would_create_assets == 3
    assert result.would_project_devices == 2
    queries = client.query_calls
    assert len(queries) == 4  # POND# 2 pages + DEVICE# 2 pages
    for request in queries:
        values = client.deserialize(request["ExpressionAttributeValues"])
        assert values[":pk"] == "TENANT#tnt_1"
        assert values[":sk_prefix"] in {"POND#", "DEVICE#"}
        assert request["Limit"] == 2
        assert request["ConsistentRead"] is True
    assert "ExclusiveStartKey" in queries[1]
    for name, request in client.calls:
        if name == "get_item":
            assert request["ConsistentRead"] is True
            assert "tnt_other" not in str(request["Key"])
    assert client.scan_calls == []


def test_native_v2_device_rows_under_device_prefix_are_skipped() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1", ponds=[legacy_pond("pond_1")], devices=[]
    )
    client.put_raw(
        {
            "PK": "TENANT#tnt_1",
            "SK": "DEVICE#dev_" + "a" * 32,
            "entity_type": "device",
            "tenant_id": "tnt_1",
            "device_id": "dev_" + "a" * 32,
            "name": "Native",
            "status": "active",
            "created_at": FIRST_RUN.isoformat(),
            "updated_at": FIRST_RUN.isoformat(),
            "version": 1,
            "schema_version": 2,
        }
    )

    result = run(client, apply=True)

    assert result.result == "success"
    assert result.devices_projected == 0
    assert result.components_created == 0


def test_missing_tenant_meta_is_a_schema_conflict() -> None:
    client = FakeMigrationDynamo()

    result = run(client, apply=True, tenants=["tnt_missing"])

    assert result.result == "schema_conflict"
    assert [conflict.model_dump() for conflict in result.conflicts] == [
        {"tenant_id": "tnt_missing", "kind": "tenant_missing", "key": "TENANT#tnt_missing/META"}
    ]
    assert result.tenants_completed == 0
    assert result.scope_completed is False
    assert client.write_calls == []
    assert client.query_calls == []


def test_device_referring_to_missing_pond_blocks_the_whole_tenant() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1")],
        devices=[legacy_device("probe-1", "pond_gone")],
    )
    client.add_legacy_tenant("tnt_2", ponds=[legacy_pond("pond_2", tenant_id="tnt_2")], devices=[])

    result = run(client, apply=True, tenants=["tnt_1", "tnt_2"])

    assert result.result == "schema_conflict"
    assert [conflict.model_dump() for conflict in result.conflicts] == [
        {"tenant_id": "tnt_1", "kind": "device_missing_pond", "key": "TENANT#tnt_1/DEVICE#probe-1"}
    ]
    assert result.tenants_completed == 1
    assert result.assets_created == 1  # tnt_2 only
    assert client.get_raw("TENANT#tnt_1", f"SITE#{default_site_id('tnt_1')}") is None
    assert all("tnt_1" not in str(request) for _, request in client.write_calls)


# Step 3: apply twice and divergent known keys ----------------------------------------------------


def test_apply_twice_creates_one_projection_per_legacy_record() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1"), legacy_pond("pond_2")],
        devices=[legacy_device("probe-1", "pond_1")],
    )
    first = DefaultSiteAssetMigrator(client, clock=fixed_clock).run(
        ["tnt_1"], apply=True, page_size=25, deadline_seconds=300
    )
    snapshot = {key: dict(item) for key, item in client.items.items()}
    second = DefaultSiteAssetMigrator(client, clock=second_clock).run(
        ["tnt_1"], apply=True, page_size=25, deadline_seconds=300
    )

    assert (first.default_sites_created, first.assets_created) == (1, 2)
    assert (first.components_created, first.deployments_created) == (1, 1)
    assert (first.devices_projected, first.legacy_links_updated) == (1, 3)
    assert first.already_migrated == 0
    assert second.default_sites_created == 0
    assert second.assets_created == 0
    assert second.devices_projected == 0
    assert second.components_created == 0
    assert second.deployments_created == 0
    assert second.legacy_links_updated == 0
    assert second.already_migrated == 3
    assert (first.result, second.result) == ("success", "success")
    assert client.scan_calls == []

    receipt_key = (KEYS.migration_receipt("tnt_1")["PK"], KEYS.migration_receipt("tnt_1")["SK"])
    changed = {key for key in client.items if client.items[key] != snapshot.get(key)}
    assert changed == {receipt_key}
    receipt = client.items[receipt_key]
    assert receipt["run_count"] == 2
    assert receipt["first_completed_at"] == "2026-08-25T12:00:00.000000Z"
    assert receipt["last_completed_at"] == "2026-09-30T18:00:00.000000Z"
    assert receipt["last_run"]["already_migrated"] == 3
    assert "expires_at" not in receipt


def test_projection_rows_are_codec_identical_and_timestamped_from_legacy_rows() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1")],
        devices=[legacy_device("probe-1", "pond_1")],
    )
    run(client, apply=True, clock=second_clock)

    site = default_site_for_tenant(tenant_model())
    projection = asset_and_profile_for_legacy_pond(pond_model("pond_1"), site.site_id)
    device = device_model("probe-1", "pond_1")
    component = component_for_legacy_device(device)
    deployment = deployment_for_legacy_device(
        device, asset_id_for_legacy_pond("tnt_1", "pond_1"), site.site_id
    )
    expected = [
        site_to_item(site),
        asset_to_item(projection.asset),
        pond_profile_to_item(projection.pond_profile),
        component_to_item(component),
        component_to_item(component, canonical=False),
        deployment_to_item(deployment),
        deployment_to_item(deployment, form="history"),
        deployment_to_item(deployment, form="current"),
        deployment_marker_item("tnt_1", component.component_id, seq=1, last_ended_at=None),
    ]
    for item in expected:
        assert client.get_raw(item["PK"], item["SK"]) == normalized(item)
    assert site.created_at == TENANT_CREATED
    assert deployment.started_at == DEVICE_CREATED
    assert projection.asset.version == 2
    assert "2026-09-30" not in str(
        {key: item for key, item in client.items.items() if "MIGRATION#" not in key[1]}
    )


def test_legacy_rows_gain_only_additive_links() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1")],
        devices=[legacy_device("probe-1", "pond_1")],
    )
    before = {key: dict(item) for key, item in client.items.items()}
    run(client, apply=True)

    component_id = default_component_id_for_legacy_device("tnt_1", "probe-1")
    pond_key = ("TENANT#tnt_1", "POND#pond_1")
    assert client.items[pond_key] == {
        **before[pond_key],
        "v2_site_id": default_site_id("tnt_1"),
        "v2_asset_id": asset_id_for_legacy_pond("tnt_1", "pond_1"),
        "v2_projection_version": Decimal(1),
    }
    for key in (("TENANT#tnt_1", "DEVICE#probe-1"), ("DEVICE#probe-1", "META")):
        assert client.items[key] == {
            **before[key],
            "default_component_id": component_id,
            "v2_projection_version": Decimal(1),
        }
    assert client.items[("TENANT#tnt_1", "META")] == before[("TENANT#tnt_1", "META")]


def test_divergent_existing_asset_is_a_schema_conflict() -> None:
    client = FakeMigrationDynamo.with_conflicting_asset(
        tenant_id="tnt_1", pond_id="pond_1", site_id="site_wrong"
    )
    result = DefaultSiteAssetMigrator(client, clock=fixed_clock).run(
        ["tnt_1"], apply=True, page_size=25, deadline_seconds=300
    )
    assert result.result == "schema_conflict"
    assert result.schema_conflicts == 1
    asset_id = asset_id_for_legacy_pond("tnt_1", "pond_1")
    assert result.conflicts[0].model_dump() == {
        "tenant_id": "tnt_1",
        "kind": "asset",
        "key": f"TENANT#tnt_1/ASSET#{asset_id}",
    }
    assert client.write_calls == []
    assert exit_code_for(result.result) == 2


def test_existing_rows_with_different_names_still_match_on_identity() -> None:
    client = migrated_store()
    asset_key = KEYS.asset("tnt_1", asset_id_for_legacy_pond("tnt_1", "pond_1"))
    client.items[(asset_key["PK"], asset_key["SK"])].update(
        {"name": "Renamed", "status": "archived", "version": Decimal(9)}
    )
    site_key = KEYS.site("tnt_1", default_site_id("tnt_1"))
    client.items[(site_key["PK"], site_key["SK"])]["name"] = "Renamed site"

    result = run(client, apply=True)

    assert result.result == "success"
    assert result.already_migrated == 2


def test_divergent_pond_link_is_a_schema_conflict() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[{**legacy_pond("pond_1"), "v2_asset_id": "ast_" + "f" * 32}],
        devices=[],
    )

    result = run(client, apply=False)

    assert result.result == "schema_conflict"
    assert result.conflicts[0].kind == "pond_link"
    assert result.conflicts[0].key == "TENANT#tnt_1/POND#pond_1"
    assert result.would_create_assets == 0


def test_device_whose_deterministic_deployment_has_moved_is_already_migrated() -> None:
    client = migrated_store()
    component_id = default_component_id_for_legacy_device("tnt_1", "probe-1")
    original = deployment_for_legacy_device(
        device_model("probe-1", "pond_1"),
        asset_id_for_legacy_pond("tnt_1", "pond_1"),
        default_site_id("tnt_1"),
    )
    ended = original.model_copy(
        update={"status": DeploymentStatus.ENDED, "ended_at": LATER, "version": 2}
    )
    moved = Deployment(
        tenant_id="tnt_1",
        deployment_id="dep_" + "9" * 32,
        component_id=component_id,
        site_id=default_site_id("tnt_1"),
        asset_id=asset_id_for_legacy_pond("tnt_1", "pond_2"),
        started_at=LATER,
        created_at=LATER,
        updated_at=LATER,
        version=1,
    )
    client.put_raw({**legacy_pond("pond_2"), "tenant_id": "tnt_1"})
    for item in (
        deployment_to_item(ended),
        deployment_to_item(ended, form="history"),
        deployment_to_item(moved),
        deployment_to_item(moved, form="history"),
        deployment_to_item(moved, form="current"),
    ):
        client.put_raw(item)
    for key in (("TENANT#tnt_1", "DEVICE#probe-1"), ("DEVICE#probe-1", "META")):
        client.items[key]["pond_id"] = "pond_2"
        client.items[key]["version"] = Decimal(4)

    result = run(client, apply=True)

    assert result.result == "success"
    assert result.deployments_created == 0
    assert result.devices_projected == 0
    assert result.already_migrated == 2
    assert result.assets_created == 1  # pond_2 gets its own projection


def test_still_current_deterministic_deployment_on_another_asset_conflicts() -> None:
    client = migrated_store()
    for key in (("TENANT#tnt_1", "DEVICE#probe-1"), ("DEVICE#probe-1", "META")):
        client.items[key]["pond_id"] = "pond_2"
    client.put_raw(legacy_pond("pond_2"))

    result = run(client, apply=False)

    component_id = default_component_id_for_legacy_device("tnt_1", "probe-1")
    assert result.result == "schema_conflict"
    assert [(c.kind, c.key) for c in result.conflicts] == [
        ("deployment_current", f"COMPONENT#{component_id}/DEPLOYMENT#CURRENT")
    ]


def test_foreign_current_deployment_without_deterministic_one_conflicts() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1")],
        devices=[legacy_device("probe-1", "pond_1")],
    )
    component_id = default_component_id_for_legacy_device("tnt_1", "probe-1")
    client.put_raw(
        {
            **KEYS.current_deployment(component_id),
            "tenant_id": "tnt_1",
            "deployment_id": "dep_" + "8" * 32,
        }
    )

    result = run(client, apply=True)

    assert result.result == "schema_conflict"
    assert result.conflicts[0].kind == "deployment_current"
    assert client.write_calls == []


def test_decommissioned_device_gets_decommissioned_component_and_no_deployment() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1")],
        devices=[legacy_device("probe-1", "pond_1", status="decommissioned")],
    )

    result = run(client, apply=True)

    component_id = default_component_id_for_legacy_device("tnt_1", "probe-1")
    assert result.components_created == 1
    assert result.deployments_created == 0
    assert result.devices_projected == 1
    assert client.get_raw(f"COMPONENT#{component_id}", "META")["status"] == "decommissioned"
    assert client.get_raw(f"COMPONENT#{component_id}", "DEPLOYMENT#CURRENT") is None
    deployment_id = deployment_id_for_legacy_device("tnt_1", "probe-1")
    assert client.get_raw("TENANT#tnt_1", f"DEPLOYMENT#{deployment_id}") is None
    assert run(client, apply=True).already_migrated == 2


# Step 4: conditional transactions and reread convergence -----------------------------------------


def test_record_transactions_are_conditional_bounded_and_grouped() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1")],
        devices=[legacy_device("probe-1", "pond_1")],
    )
    run(client, apply=True)

    pond_tx, device_tx = client.transactions
    assert [next(iter(action)) for action in pond_tx] == ["Put", "Put", "Update"]
    assert [next(iter(action)) for action in device_tx] == [
        *["Put"] * 6,
        "ConditionCheck",
        "Update",
        "Update",
    ]
    for action in [*pond_tx, *device_tx]:
        ((kind, request),) = action.items()
        if kind == "Put":
            assert request["ConditionExpression"] == "attribute_not_exists(PK)"
        elif kind == "Update":
            assert "#version = :observed_version" in request["ConditionExpression"]
            assert "version" not in {
                name
                for alias, name in request["ExpressionAttributeNames"].items()
                if f"{alias} = :" in request["UpdateExpression"]
            }
            assert "updated_at" not in request["UpdateExpression"]
    for name, request in client.write_calls:
        if name == "put_item":
            assert request["ConditionExpression"] == "attribute_not_exists(PK)"


def test_identical_concurrent_writer_converges_as_already_migrated() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1")],
        devices=[legacy_device("probe-1", "pond_1")],
    )
    client.before_transact = lambda fake, actions: fake.apply_unconditionally(actions)

    result = run(client, apply=True)

    assert result.result == "success"
    assert result.assets_created == 0
    assert result.already_migrated == 1  # the raced Pond
    assert result.deployments_created == 1
    assert len(client.transactions) == 2


def test_divergent_concurrent_writer_is_a_schema_conflict() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1")],
        devices=[legacy_device("probe-1", "pond_1")],
    )
    component_id = default_component_id_for_legacy_device("tnt_1", "probe-1")
    calls = {"count": 0}

    def race(fake: FakeMigrationDynamo, actions: list[dict]) -> None:
        fake.put_raw(
            {
                **KEYS.component(component_id),
                "tenant_id": "tnt_1",
                "component_id": component_id,
                "device_id": "dev_other",
                "kind": "probe",
            }
        )

    def arm_second(fake: FakeMigrationDynamo, actions: list[dict]) -> None:
        calls["count"] += 1
        fake.before_transact = race

    client.before_transact = arm_second

    result = run(client, apply=True)

    assert result.result == "schema_conflict"
    assert [(c.kind, c.key) for c in result.conflicts] == [
        ("component", f"COMPONENT#{component_id}/META")
    ]
    assert result.assets_created == 1
    assert result.tenants_completed == 0
    assert client.get_raw(*KEYS.migration_receipt("tnt_1").values()) is None


def test_source_row_changed_between_plan_and_write_is_replanned() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1", ponds=[legacy_pond("pond_1")], devices=[]
    )

    def concurrent_v1_edit(fake: FakeMigrationDynamo, actions: list[dict]) -> None:
        pond = fake.items[("TENANT#tnt_1", "POND#pond_1")]
        pond.update({"name": "Edited", "version": Decimal(3)})

    client.before_transact = concurrent_v1_edit

    result = run(client, apply=True)

    assert result.result == "success"
    assert result.assets_created == 1
    assert result.legacy_links_updated == 1
    pond = client.get_raw("TENANT#tnt_1", "POND#pond_1")
    assert pond["version"] == 3
    assert pond["name"] == "Edited"
    asset = client.get_raw(
        *KEYS.asset("tnt_1", asset_id_for_legacy_pond("tnt_1", "pond_1")).values()
    )
    assert asset["version"] == 3
    assert len(client.transactions) == 2


def test_conditional_site_race_converges() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(tenant_id="tnt_1", ponds=[], devices=[])
    original_put = client.put_item

    def racing_put(**kwargs: Any) -> dict[str, Any]:
        client.apply_unconditionally([{"Put": kwargs}])
        return original_put(**kwargs)

    client.put_item = racing_put  # type: ignore[method-assign]

    result = run(client, apply=True)

    assert result.result == "success"
    assert result.default_sites_created == 0
    assert result.tenants_completed == 1


def test_non_conditional_dynamodb_error_stops_with_exit_one() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1", ponds=[legacy_pond("pond_1")], devices=[]
    )
    client.fail_operation = (
        "query",
        ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "slow"}},
            "Query",
        ),
    )

    result = run(client, apply=True)

    assert result.result == "dynamodb_error"
    assert result.scope_completed is False
    assert exit_code_for(result.result) == 1


# Step 5: validation, deadline and exit codes -----------------------------------------------------


class SteppingMonotonic:
    def __init__(self, step: float) -> None:
        self.now = 0.0
        self.step = step

    def __call__(self) -> float:
        current = self.now
        self.now += self.step
        return current


def test_expired_deadline_between_pages_stops_with_exit_one() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond(f"pond_{index}") for index in range(5)],
        devices=[],
    )
    migrator = DefaultSiteAssetMigrator(client, clock=fixed_clock, monotonic=SteppingMonotonic(1))

    result = migrator.run(["tnt_1"], apply=True, page_size=1, deadline_seconds=4)

    assert result.result == "deadline_exceeded"
    assert result.scope_completed is False
    assert result.tenants_completed == 0
    assert client.write_calls == []
    assert len(client.query_calls) < 5
    assert exit_code_for(result.result) == 1


def test_deadline_takes_precedence_over_earlier_conflicts() -> None:
    client = FakeMigrationDynamo()
    client.add_legacy_tenant(
        "tnt_2", ponds=[legacy_pond(f"pond_{i}", tenant_id="tnt_2") for i in range(5)], devices=[]
    )
    migrator = DefaultSiteAssetMigrator(client, clock=fixed_clock, monotonic=SteppingMonotonic(1))

    result = migrator.run(["tnt_missing", "tnt_2"], apply=False, page_size=1, deadline_seconds=5)

    assert result.schema_conflicts == 1
    assert result.result == "deadline_exceeded"
    assert exit_code_for(result.result) == 1


def test_multiple_tenants_are_deduplicated_preserving_order() -> None:
    client = FakeMigrationDynamo()
    client.add_legacy_tenant("tnt_b", ponds=[], devices=[])
    client.add_legacy_tenant("tnt_a", ponds=[], devices=[])

    result = run(client, apply=True, tenants=["tnt_b", "tnt_a", "tnt_b"])

    assert result.tenants_requested == 2
    assert result.tenants_completed == 2
    assert result.default_sites_created == 2
    tenants_read = [
        client.deserialize(request["Key"])["PK"]
        for name, request in client.calls
        if name == "get_item"
        and client.deserialize(request["Key"])["SK"] == "META"
        and client.deserialize(request["Key"])["PK"].startswith("TENANT#")
    ]
    assert tenants_read == ["TENANT#tnt_b", "TENANT#tnt_a"]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"tenants": []},
        {"tenants": [""]},
        {"tenants": ["tnt\0x"]},
        {"tenants": ["tnt#x"]},
        {"tenants": [" tnt_1"]},
        {"page_size": 0},
        {"page_size": 101},
        {"deadline_seconds": 0},
        {"deadline_seconds": 3601},
    ],
)
def test_invalid_scope_or_limits_are_rejected_before_any_call(kwargs: dict[str, Any]) -> None:
    client = FakeMigrationDynamo()
    with pytest.raises(ValueError):
        run(client, apply=False, **kwargs)
    assert client.calls == []


def test_tenant_normalization_and_exit_codes() -> None:
    assert normalize_tenant_ids(["b", "a", "b"]) == ["b", "a"]
    assert exit_code_for("success") == 0
    assert exit_code_for("schema_conflict") == 2
    for result in ("deadline_exceeded", "dynamodb_error", "configuration_error", "failed"):
        assert exit_code_for(result) == 1


# Audit regressions: overlapping runs, /v1 moves, transaction conflicts, counters ---------------


def _transaction_conflict() -> ClientError:
    return ClientError(
        {
            "Error": {"Code": "TransactionCanceledException", "Message": "cancelled"},
            "CancellationReasons": [{"Code": "TransactionConflict"}, {"Code": "None"}],
        },
        "TransactWriteItems",
    )


def _deployment_history_key() -> tuple[str, str]:
    deployment = deployment_for_legacy_device(
        device_model("probe-1", "pond_1"),
        asset_id_for_legacy_pond("tnt_1", "pond_1"),
        default_site_id("tnt_1"),
    )
    key = KEYS.deployment_history(
        deployment.component_id, deployment.started_at, deployment.deployment_id
    )
    return key["PK"], key["SK"]


def _move_device(fake: FakeMigrationDynamo, pond_id: str) -> None:
    for key in (("TENANT#tnt_1", "DEVICE#probe-1"), ("DEVICE#probe-1", "META")):
        fake.items[key]["pond_id"] = pond_id
        fake.items[key]["version"] += 1


@pytest.mark.parametrize(
    "trigger",
    [
        "deployment_history",
        "asset",
        "component_canonical",
    ],
)
@pytest.mark.parametrize("apply", [True, False])
def test_identical_run_committing_between_known_key_reads_converges(
    trigger: str, apply: bool
) -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1")],
        devices=[legacy_device("probe-1", "pond_1")],
    )
    component_id = default_component_id_for_legacy_device("tnt_1", "probe-1")
    trigger_key = {
        "deployment_history": _deployment_history_key(),
        "asset": tuple(KEYS.asset("tnt_1", asset_id_for_legacy_pond("tnt_1", "pond_1")).values()),
        "component_canonical": tuple(KEYS.component(component_id).values()),
    }[trigger]

    def other_run(fake: FakeMigrationDynamo, key: tuple[str, str]) -> None:
        if key == trigger_key:
            fake.before_get_item = None
            other = DefaultSiteAssetMigrator(fake, clock=second_clock).run(
                ["tnt_1"], apply=True, page_size=25, deadline_seconds=300
            )
            assert other.result == "success"

    client.before_get_item = other_run

    result = run(client, apply=apply)

    assert result.result == "success"
    assert result.conflicts == []
    if apply:
        assert result.default_sites_created == 0
        assert result.assets_created == 0
        assert result.components_created == 0
        assert result.deployments_created == 0
        assert result.already_migrated == 2


def test_device_moved_by_v1_during_planning_is_replanned() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1"), legacy_pond("pond_2")],
        devices=[legacy_device("probe-1", "pond_1")],
    )

    def move_on_lookup_read(fake: FakeMigrationDynamo, key: tuple[str, str]) -> None:
        if key == ("DEVICE#probe-1", "META"):
            fake.before_get_item = None
            _move_device(fake, "pond_2")

    client.before_get_item = move_on_lookup_read

    result = run(client, apply=True)

    component_id = default_component_id_for_legacy_device("tnt_1", "probe-1")
    assert result.result == "success"
    assert result.deployments_created == 1
    current = client.get_raw(*KEYS.current_deployment(component_id).values())
    assert current["asset_id"] == asset_id_for_legacy_pond("tnt_1", "pond_2")
    assert client.get_raw("TENANT#tnt_1", "DEVICE#probe-1")["version"] == 4


def test_persistent_lookup_mismatch_is_a_conflict_after_bounded_rereads() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1"), legacy_pond("pond_2")],
        devices=[legacy_device("probe-1", "pond_1")],
    )
    client.items[("DEVICE#probe-1", "META")]["pond_id"] = "pond_2"

    result = run(client, apply=True)

    assert result.result == "schema_conflict"
    assert [(c.kind, c.key) for c in result.conflicts] == [("device_lookup", "DEVICE#probe-1/META")]
    device_reads = [
        request
        for name, request in client.calls
        if name == "get_item" and client.deserialize(request["Key"])["SK"] == "DEVICE#probe-1"
    ]
    assert len(device_reads) == 2  # the Query page plus two bounded re-reads in total
    assert client.write_calls == []


def test_device_moved_to_pond_created_during_planning_plans_that_pond() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1")],
        devices=[legacy_device("probe-1", "pond_1")],
    )

    def create_pond_and_move(fake: FakeMigrationDynamo, key: tuple[str, str]) -> None:
        if key == ("DEVICE#probe-1", "META"):
            fake.before_get_item = None
            fake.put_raw(legacy_pond("pond_new"))
            _move_device(fake, "pond_new")

    client.before_get_item = create_pond_and_move

    result = run(client, apply=True)

    component_id = default_component_id_for_legacy_device("tnt_1", "probe-1")
    new_asset_id = asset_id_for_legacy_pond("tnt_1", "pond_new")
    assert result.result == "success"
    assert result.assets_created == 2
    assert client.get_raw(*KEYS.current_deployment(component_id).values())["asset_id"] == (
        new_asset_id
    )
    assert client.get_raw("TENANT#tnt_1", "POND#pond_new")["v2_asset_id"] == new_asset_id


def test_device_moved_to_new_pond_between_plan_and_write_applies_that_pond_first() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1")],
        devices=[legacy_device("probe-1", "pond_1")],
    )

    def create_pond_and_move(fake: FakeMigrationDynamo, actions: list[dict]) -> None:
        fake.put_raw(legacy_pond("pond_new"))
        _move_device(fake, "pond_new")

    def arm_for_device_transaction(fake: FakeMigrationDynamo, actions: list[dict]) -> None:
        fake.before_transact = create_pond_and_move

    client.before_transact = arm_for_device_transaction

    result = run(client, apply=True)

    component_id = default_component_id_for_legacy_device("tnt_1", "probe-1")
    assert result.result == "success"
    assert result.assets_created == 2
    assert result.deployments_created == 1
    assert client.get_raw(*KEYS.current_deployment(component_id).values())["asset_id"] == (
        asset_id_for_legacy_pond("tnt_1", "pond_new")
    )


def test_transaction_conflict_cancellation_is_retried_after_backoff() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1")],
        devices=[legacy_device("probe-1", "pond_1")],
    )
    client.fail_once["transact_write_items"] = _transaction_conflict()
    client.fail_once["put_item"] = ClientError(
        {"Error": {"Code": "TransactionConflictException", "Message": "busy"}}, "PutItem"
    )
    sleeps: list[float] = []

    result = DefaultSiteAssetMigrator(client, clock=fixed_clock, sleep=sleeps.append).run(
        ["tnt_1"], apply=True, page_size=25, deadline_seconds=300
    )

    assert result.result == "success"
    assert (result.default_sites_created, result.assets_created) == (1, 1)
    assert result.deployments_created == 1
    assert len(sleeps) == 2
    assert all(0 <= delay <= 0.05 for delay in sleeps)


def test_exhausted_transaction_conflicts_fail_only_that_tenant() -> None:
    class BusyTenantDynamo(FakeMigrationDynamo):
        def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
            if "TENANT#tnt_1" in str(kwargs):
                self.calls.append(("transact_write_items", kwargs))
                raise _transaction_conflict()
            return super().transact_write_items(**kwargs)

    client = BusyTenantDynamo()
    client.add_legacy_tenant("tnt_1", ponds=[legacy_pond("pond_1")], devices=[])
    client.add_legacy_tenant("tnt_2", ponds=[legacy_pond("pond_2", tenant_id="tnt_2")], devices=[])
    sleeps: list[float] = []

    result = DefaultSiteAssetMigrator(client, clock=fixed_clock, sleep=sleeps.append).run(
        ["tnt_1", "tnt_2"], apply=True, page_size=25, deadline_seconds=300
    )

    assert result.result == "failed"
    assert exit_code_for(result.result) == 1
    assert result.tenants_completed == 1
    assert result.scope_completed is False
    assert result.assets_created == 1  # tnt_2
    assert len(sleeps) == 3
    assert client.get_raw(*KEYS.migration_receipt("tnt_1").values()) is None
    assert client.get_raw(*KEYS.migration_receipt("tnt_2").values()) is not None


def test_profile_repair_for_existing_asset_is_not_counted_as_asset_created() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1", ponds=[legacy_pond("pond_1")], devices=[]
    )
    projection = asset_and_profile_for_legacy_pond(pond_model("pond_1"), default_site_id("tnt_1"))
    client.put_raw(asset_to_item(projection.asset))

    dry = run(client, apply=False)
    applied = run(client, apply=True)

    assert (dry.would_create_assets, dry.would_update_legacy_links) == (0, 1)
    assert (applied.assets_created, applied.legacy_links_updated) == (0, 1)
    profile = pond_profile_to_item(projection.pond_profile)
    assert client.get_raw(profile["PK"], profile["SK"]) == normalized(profile)


def test_link_only_device_is_not_counted_as_projected() -> None:
    client = migrated_store()
    for key, names in (
        (("TENANT#tnt_1", "POND#pond_1"), ("v2_site_id", "v2_asset_id", "v2_projection_version")),
        (("TENANT#tnt_1", "DEVICE#probe-1"), ("default_component_id", "v2_projection_version")),
        (("DEVICE#probe-1", "META"), ("default_component_id", "v2_projection_version")),
    ):
        for name in names:
            del client.items[key][name]

    dry = run(client, apply=False)
    applied = run(client, apply=True)

    assert (dry.would_project_devices, dry.would_update_legacy_links) == (0, 2)
    assert applied.devices_projected == 0
    assert applied.components_created == 0
    assert applied.deployments_created == 0
    assert applied.legacy_links_updated == 2
    assert applied.already_migrated == 0
