import asyncio
import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError

from limnopulse_api.adapters.dynamodb_deployments import (
    DynamoDeploymentRepository,
    deployment_from_item,
    deployment_marker_item,
    deployment_marker_key,
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
        values = self.deserialize(kwargs["ExpressionAttributeValues"])
        condition = kwargs["KeyConditionExpression"]
        if condition == "PK = :pk AND begins_with(SK, :sk_prefix)":

            def wanted(sk: str) -> bool:
                return sk.startswith(values[":sk_prefix"])
        else:
            # Only the history range read used by `get_deployment_at` (verification only).
            assert condition == "PK = :pk AND SK BETWEEN :sk_lower AND :sk_upper"

            def wanted(sk: str) -> bool:
                return values[":sk_lower"] <= sk <= values[":sk_upper"]

        forward = kwargs.get("ScanIndexForward", True)
        matches = sorted(
            (item for (pk, sk), item in self.items.items() if pk == values[":pk"] and wanted(sk)),
            key=lambda item: item["SK"],
            reverse=not forward,
        )
        start = kwargs.get("ExclusiveStartKey")
        if start is not None:
            start_sk = self.deserialize(start)["SK"]
            matches = [
                item
                for item in matches
                if (item["SK"] > start_sk if forward else item["SK"] < start_sk)
            ]
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
        return self._holds(condition, item, names, values)

    def _holds(self, condition: str, item: dict | None, names: dict, values: dict) -> bool:
        """`OR` of `AND`s of `attribute_(not_)exists(x)` / `a = :b`, with parentheses."""
        condition = condition.strip()
        if condition.startswith("(") and condition.endswith(")"):
            inner = condition[1:-1]
            if _split_top_level(inner, " OR ") and inner.count("(") == inner.count(")"):
                depth, balanced = 0, True
                for char in inner:
                    depth += (char == "(") - (char == ")")
                    balanced = balanced and depth >= 0
                if balanced:
                    condition = inner
        alternatives = _split_top_level(condition, " OR ")
        if len(alternatives) > 1:
            return any(self._holds(part, item, names, values) for part in alternatives)
        for clause in _split_top_level(condition, " AND "):
            if clause.startswith("("):
                if not self._holds(clause, item, names, values):
                    return False
                continue
            exists = re.fullmatch(r"attribute_(not_)?exists\((\S+)\)", clause)
            if exists:
                name = names.get(exists.group(2), exists.group(2))
                present = item is not None and name in item
                if present == bool(exists.group(1)):
                    return False
                continue
            typed = re.fullmatch(r"attribute_type\((\S+),\s*(\S+)\)", clause)
            if typed:
                value = None if item is None else item.get(names.get(typed.group(1)))
                numeric = isinstance(value, (int, Decimal)) and not isinstance(value, bool)
                if values[typed.group(2)] != "N" or not numeric:
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
            operands = [self._value(term, item, names, values) for term in terms]
            if any(isinstance(o, bool) or not isinstance(o, (int, Decimal)) for o in operands):
                raise ClientError(
                    {"Error": {"Code": "ValidationException", "Message": "operand type"}},
                    "UpdateItem",
                )
            return sum(operands)
        default = re.fullmatch(r"if_not_exists\((\S+),\s*(\S+)\)", expression)
        if default:
            name = names[default.group(1)]
            return item[name] if name in item else values[default.group(2)]
        return values[expression]


def on_transaction_with(
    fragment: str, hook: Callable[["FakeMigrationDynamo", list[dict]], None]
) -> Callable[["FakeMigrationDynamo", list[dict]], None]:
    """A `before_transact` hook that fires once, on the first transaction mentioning `fragment`
    (the default Site, each Pond and each Device commit in separate transactions)."""

    def armed(fake: "FakeMigrationDynamo", actions: list[dict]) -> None:
        if fragment not in str(actions):
            fake.before_transact = armed
            return
        hook(fake, actions)

    return armed


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
    sleep = options.pop("sleep", lambda _delay: None)
    return DefaultSiteAssetMigrator(client, clock=clock, sleep=sleep).run(
        tenants, apply=apply, **options
    )


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
    # POND# 2 pages + DEVICE# 2 pages, then the same again for the clean verification pass.
    assert len(queries) == 8
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


def test_malformed_current_deployment_is_a_conflict() -> None:
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

    site_tx, pond_tx, device_tx = client.transactions
    assert [next(iter(action)) for action in site_tx] == ["Put", "ConditionCheck"]
    tenant_fence = site_tx[1]["ConditionCheck"]
    assert tenant_fence["Key"] == client.serialize({"PK": "TENANT#tnt_1", "SK": "META"})
    assert "#version = :observed_version" in tenant_fence["ConditionExpression"]
    assert [next(iter(action)) for action in pond_tx] == ["Put", "Put", "ConditionCheck", "Update"]
    assert [next(iter(action)) for action in device_tx] == [
        *["Put"] * 6,
        "ConditionCheck",  # Asset exists
        "ConditionCheck",  # default Site ACTIVE
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
    client.before_transact = on_transaction_with(
        "ASSET#", lambda fake, actions: fake.apply_unconditionally(actions)
    )

    result = run(client, apply=True)

    assert result.result == "success"
    assert result.assets_created == 0
    assert result.already_migrated == 1  # the raced Pond
    assert result.deployments_created == 1
    assert len(client.transactions) == 3  # Site, raced Pond, Device


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

    client.before_transact = on_transaction_with("ASSET#", concurrent_v1_edit)

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
    assert sum("ASSET#" in str(tx) for tx in client.transactions) == 2  # refused, re-planned


def test_conditional_site_race_converges() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(tenant_id="tnt_1", ponds=[], devices=[])
    client.before_transact = on_transaction_with(
        "SITE#", lambda fake, actions: fake.apply_unconditionally(actions)
    )

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
    sleeps: list[float] = []

    result = run(client, apply=True, sleep=sleeps.append)

    assert len(sleeps) == 2  # jittered backoff before each re-read
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
    client.fail_once["transact_write_items"] = _transaction_conflict()  # the Site transaction

    def busy_pond(fake: FakeMigrationDynamo, actions: list[dict]) -> None:
        raise ClientError(
            {"Error": {"Code": "TransactionConflictException", "Message": "busy"}},
            "TransactWriteItems",
        )

    client.before_transact = on_transaction_with("ASSET#", busy_pond)
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


# Drift repair: CURRENT is authoritative, whoever wrote it ---------------------------------------

MOVED_AT = datetime(2026, 6, 1, 9, tzinfo=UTC)
FOREIGN_STARTED = datetime(2026, 7, 1, 8, tzinfo=UTC)
FOREIGN_DEPLOYMENT_ID = "dep_" + "7" * 32
COMPONENT_ID = default_component_id_for_legacy_device("tnt_1", "probe-1")


def _item_key(item: dict[str, Any]) -> tuple[str, str]:
    return item["PK"], item["SK"]


def _move_device_at(fake: FakeMigrationDynamo, pond_id: str, updated_at: datetime) -> None:
    for key in (("TENANT#tnt_1", "DEVICE#probe-1"), ("DEVICE#probe-1", "META")):
        fake.items[key].update(
            {
                "pond_id": pond_id,
                "version": fake.items[key]["version"] + 1,
                "updated_at": updated_at.isoformat(),
            }
        )


def _deterministic_deployment() -> Deployment:
    return deployment_for_legacy_device(
        device_model("probe-1", "pond_1"),
        asset_id_for_legacy_pond("tnt_1", "pond_1"),
        default_site_id("tnt_1"),
    )


def _foreign_deployment(pond_id: str) -> Deployment:
    """A non-deterministic interval as the flag-on `/v1` projection or `/v2` would write it."""
    return Deployment(
        tenant_id="tnt_1",
        deployment_id=FOREIGN_DEPLOYMENT_ID,
        component_id=COMPONENT_ID,
        site_id=default_site_id("tnt_1"),
        asset_id=asset_id_for_legacy_pond("tnt_1", pond_id),
        started_at=FOREIGN_STARTED,
        created_at=FOREIGN_STARTED,
        updated_at=FOREIGN_STARTED,
        version=1,
    )


def _replace_deterministic_with_foreign(fake: FakeMigrationDynamo, pond_id: str) -> Deployment:
    deterministic = _deterministic_deployment()
    for form in ("mirror", "history", "current"):
        del fake.items[_item_key(deployment_to_item(deterministic, form=form))]
    foreign = _foreign_deployment(pond_id)
    for form in ("mirror", "history", "current"):
        fake.put_raw(deployment_to_item(foreign, form=form))
    return foreign


def _deployment_at(fake: FakeMigrationDynamo, observed_at: datetime) -> Deployment | None:
    repository = DynamoDeploymentRepository(TABLE, fake)
    return asyncio.run(repository.get_deployment_at("tnt_1", COMPONENT_ID, observed_at))


def _history_rows(fake: FakeMigrationDynamo) -> list[dict[str, Any]]:
    lower, upper = KEYS.deployment_history_bounds()
    return [
        item
        for (pk, sk), item in sorted(fake.items.items())
        if pk == f"COMPONENT#{COMPONENT_ID}" and lower <= sk <= upper
    ]


def _current(fake: FakeMigrationDynamo) -> dict[str, Any]:
    return fake.get_raw(*KEYS.current_deployment(COMPONENT_ID).values())


def moved_after_migration() -> FakeMigrationDynamo:
    client = migrated_store()
    client.put_raw(legacy_pond("pond_2"))
    _move_device_at(client, "pond_2", MOVED_AT)
    return client


def test_device_moved_after_migration_is_drift_repaired() -> None:
    client = moved_after_migration()
    client.calls.clear()

    dry = run(client, apply=False)

    assert client.write_calls == []
    assert (dry.would_create_deployments, dry.would_project_devices) == (1, 1)
    assert (dry.would_create_assets, dry.would_create_components) == (1, 0)
    assert dry.result == "success"

    result = run(client, apply=True)

    assert result.result == "success"
    assert (result.deployments_created, result.devices_projected) == (1, 1)
    assert (result.assets_created, result.components_created) == (1, 0)
    deterministic = _deterministic_deployment()
    for form in ("mirror", "history"):
        ended = client.get_raw(*_item_key(deployment_to_item(deterministic, form=form)))
        assert ended["status"] == "ended"
        assert ended["ended_at"] == "2026-06-01T09:00:00.000000Z"
        assert ended["version"] == 2
    current = _current(client)
    pond_2_asset = asset_id_for_legacy_pond("tnt_1", "pond_2")
    assert current["asset_id"] == pond_2_asset
    assert current["started_at"] == "2026-06-01T09:00:00.000000Z"
    assert current["deployment_id"] != deterministic.deployment_id
    assert client.get_raw(*_item_key(deployment_to_item(deployment_from_item(current))))
    marker = client.get_raw(*deployment_marker_key(COMPONENT_ID).values())
    assert (marker["seq"], marker["last_ended_at"]) == (2, "2026-06-01T09:00:00.000000Z")
    before = _deployment_at(client, MOVED_AT - timedelta(microseconds=1))
    after = _deployment_at(client, MOVED_AT)
    assert before.asset_id == asset_id_for_legacy_pond("tnt_1", "pond_1")
    assert after.asset_id == pond_2_asset

    second = run(client, apply=True, clock=second_clock)

    assert second.result == "success"
    assert second.deployments_created == 0
    assert second.devices_projected == 0
    assert second.already_migrated == 3


def test_drift_repair_transaction_is_fenced_and_bounded() -> None:
    client = moved_after_migration()

    run(client, apply=True)

    device_tx = client.transactions[-1]
    assert len(device_tx) <= 25
    assert [next(iter(action)) for action in device_tx] == [
        "Put",
        "Put",
        "Update",
        "Update",
        "Put",
        "Update",
        "ConditionCheck",
        "ConditionCheck",
        "ConditionCheck",
        "ConditionCheck",
        "ConditionCheck",
    ]
    site_check = device_tx[7]["ConditionCheck"]
    assert site_check["Key"] == client.serialize(KEYS.site("tnt_1", default_site_id("tnt_1")))
    assert "#status = :active" in site_check["ConditionExpression"]
    for fence in device_tx[-2:]:
        condition = fence["ConditionCheck"]["ConditionExpression"]
        assert "#version = :observed_version" in condition
        assert "#pond_id = :pond_id" in condition
    replace_current = device_tx[4]["Put"]
    assert "#deployment_id = :deployment_id" in replace_current["ConditionExpression"]
    assert "#version = :expected_version" in replace_current["ConditionExpression"]
    for index in (2, 3):
        assert "#version = :expected_version" in device_tx[index]["Update"]["ConditionExpression"]
    assert "#seq = :observed_seq" in device_tx[5]["Update"]["ConditionExpression"]


def test_non_deterministic_current_on_the_pond_asset_is_already_migrated() -> None:
    client = migrated_store()
    _replace_deterministic_with_foreign(client, "pond_1")
    client.calls.clear()

    result = run(client, apply=True)

    assert result.result == "success"
    assert result.already_migrated == 2
    assert result.deployments_created == 0
    assert client.transactions == []
    assert [name for name, _ in client.write_calls] == ["update_item"]  # migration receipt only


def test_non_deterministic_current_on_another_asset_is_drift_repaired() -> None:
    client = migrated_store()
    client.put_raw(legacy_pond("pond_2"))
    foreign = _replace_deterministic_with_foreign(client, "pond_2")

    result = run(client, apply=True)

    # The Device row (updated_at LATER) predates the foreign start: transition = start + 1µs.
    transition = FOREIGN_STARTED + timedelta(microseconds=1)
    assert result.result == "success"
    assert result.deployments_created == 1
    ended = client.get_raw(*_item_key(deployment_to_item(foreign)))
    assert (ended["status"], ended["ended_at"]) == ("ended", "2026-07-01T08:00:00.000001Z")
    assert _current(client)["asset_id"] == asset_id_for_legacy_pond("tnt_1", "pond_1")
    assert _deployment_at(client, FOREIGN_STARTED).asset_id == foreign.asset_id
    assert _deployment_at(client, transition).asset_id == asset_id_for_legacy_pond(
        "tnt_1", "pond_1"
    )
    assert run(client, apply=True).deployments_created == 0


def test_ended_deterministic_deployment_without_current_is_already_migrated() -> None:
    client = migrated_store()
    deterministic = _deterministic_deployment()
    del client.items[_item_key(deployment_to_item(deterministic, form="current"))]
    for form in ("mirror", "history"):
        client.items[_item_key(deployment_to_item(deterministic, form=form))].update(
            {"status": "ended", "ended_at": "2026-06-01T09:00:00.000000Z", "version": 2}
        )
    client.items[_item_key(deployment_marker_key(COMPONENT_ID))].update(
        {"seq": 2, "last_ended_at": "2026-06-01T09:00:00.000000Z"}
    )
    client.calls.clear()

    result = run(client, apply=True)

    assert result.result == "success"
    assert result.already_migrated == 2
    assert result.deployments_created == 0
    assert client.transactions == []


def test_inactive_component_is_not_drift_repaired() -> None:
    client = moved_after_migration()
    for key in _component_keys():  # v2 updates both Component rows together
        client.items[key]["status"] = "decommissioned"

    result = run(client, apply=True)

    assert result.result == "success"
    assert (result.deployments_created, result.devices_projected) == (0, 0)
    assert result.already_migrated == 2  # pond_1 and the Device; pond_2 gets its Asset
    assert _current(client)["asset_id"] == asset_id_for_legacy_pond("tnt_1", "pond_1")


def _scenario(name: str) -> tuple[FakeMigrationDynamo, tuple[str, str], str, int]:
    """Store, history key named by CURRENT, expected final pond and expected history rows."""
    if name == "moved_after_migration":
        return moved_after_migration(), _deployment_history_key(), "pond_2", 2
    client = migrated_store()
    pond_id = "pond_2" if name == "foreign_wrong_asset" else "pond_1"
    if pond_id == "pond_2":
        client.put_raw(legacy_pond("pond_2"))
    foreign = _replace_deterministic_with_foreign(client, pond_id)
    history_key = _item_key(deployment_to_item(foreign, form="history"))
    return client, history_key, "pond_1", 2 if pond_id == "pond_2" else 1


@pytest.mark.parametrize(
    "scenario", ["moved_after_migration", "foreign_wrong_asset", "foreign_right_asset"]
)
@pytest.mark.parametrize("interleave", ["between_reads", "before_device_transaction"])
def test_concurrent_identical_runs_converge_on_drift(scenario: str, interleave: str) -> None:
    client, history_key, final_pond, history_rows = _scenario(scenario)
    others: list[Any] = []

    def other_run(fake: FakeMigrationDynamo) -> None:
        others.append(
            DefaultSiteAssetMigrator(fake, clock=second_clock).run(
                ["tnt_1"], apply=True, page_size=25, deadline_seconds=300
            )
        )

    if interleave == "between_reads":

        def on_read(fake: FakeMigrationDynamo, key: tuple[str, str]) -> None:
            if key == history_key:
                fake.before_get_item = None
                other_run(fake)

        client.before_get_item = on_read
    else:

        def on_transaction(fake: FakeMigrationDynamo, actions: list[dict]) -> None:
            if "DEPLOYMENT#CURRENT" not in str(actions):
                fake.before_transact = on_transaction
                return
            other_run(fake)

        client.before_transact = on_transaction

    result = run(client, apply=True)

    assert result.result == "success"
    assert result.conflicts == []
    # A Pond-asset CURRENT needs no device transaction, so only the read interleaving applies.
    assert len(others) == int(scenario != "foreign_right_asset" or interleave == "between_reads")
    assert all(other.result == "success" for other in others)
    created = result.deployments_created + sum(other.deployments_created for other in others)
    assert created == (history_rows - 1)
    assert _current(client)["asset_id"] == asset_id_for_legacy_pond("tnt_1", final_pond)
    assert len(_history_rows(client)) == history_rows
    assert sum(row["status"] == "active" for row in _history_rows(client)) == 1


# Final audit: archived drift targets, query shape, unstable-state exhaustion --------------------


def test_move_into_archived_pond_after_migration_is_not_repaired() -> None:
    client = migrated_store()
    client.put_raw({**legacy_pond("pond_2"), "status": "archived"})
    _move_device_at(client, "pond_2", MOVED_AT)
    before = dict(_current(client))

    dry = run(client, apply=False)
    result = run(client, apply=True)

    assert (dry.would_create_deployments, dry.would_project_devices) == (0, 0)
    assert result.result == "success"
    assert exit_code_for(result.result) == 0
    assert (result.deployments_created, result.devices_projected) == (0, 0)
    assert result.assets_created == 1  # the archived Pond still gets its (ARCHIVED) Asset
    assert result.already_migrated == 2  # pond_1 and the Device
    assert _current(client) == before
    archived = client.get_raw(
        *KEYS.asset("tnt_1", asset_id_for_legacy_pond("tnt_1", "pond_2")).values()
    )
    assert archived["status"] == "archived"


def test_asset_archived_between_plan_and_repair_is_fenced_and_already_migrated() -> None:
    client = moved_after_migration()
    run(client, apply=True)  # pond_2 Asset now exists; the drift was repaired
    client.put_raw(legacy_pond("pond_3"))
    run(client, apply=True)  # pond_3 Asset exists, nothing drifts yet
    _move_device_at(client, "pond_3", MOVED_AT + timedelta(days=1))
    pond_3_asset = KEYS.asset("tnt_1", asset_id_for_legacy_pond("tnt_1", "pond_3"))
    before = dict(_current(client))

    def archive_target(fake: FakeMigrationDynamo, actions: list[dict]) -> None:
        if "DEPLOYMENT#CURRENT" not in str(actions):
            fake.before_transact = archive_target
            return
        fake.items[(pond_3_asset["PK"], pond_3_asset["SK"])]["status"] = "archived"

    client.before_transact = archive_target
    client.calls.clear()

    result = run(client, apply=True)

    assert result.result == "success"
    assert result.deployments_created == 0
    assert result.already_migrated == 4
    assert _current(client) == before
    repairs = [tx for tx in client.transactions if "DEPLOYMENT#CURRENT" in str(tx)]
    assert len(repairs) == 1  # fenced, then re-planned as already migrated without a retry
    repair = repairs[0]
    checks = [action["ConditionCheck"] for action in repair if "ConditionCheck" in action]
    assert any("#status = :active" in check["ConditionExpression"] for check in checks)


def test_every_query_is_a_tenant_partition_prefix_query() -> None:
    client = moved_after_migration()
    client.add_legacy_tenant("tnt_2", ponds=[legacy_pond("pond_x", tenant_id="tnt_2")], devices=[])
    client.calls.clear()

    run(client, apply=False, page_size=1, tenants=["tnt_1", "tnt_2"])
    run(client, apply=True, page_size=1, tenants=["tnt_1", "tnt_2"])
    run(client, apply=True, page_size=1, tenants=["tnt_1", "tnt_2"])

    assert client.scan_calls == []
    assert {name for name, _ in client.calls} <= {
        "get_item",
        "query",
        "put_item",
        "update_item",
        "transact_write_items",
    }
    assert client.query_calls
    for request in client.query_calls:
        assert request["KeyConditionExpression"] == "PK = :pk AND begins_with(SK, :sk_prefix)"
        assert "BETWEEN" not in str(request)
        values = client.deserialize(request["ExpressionAttributeValues"])
        assert values[":pk"] in {"TENANT#tnt_1", "TENANT#tnt_2"}
        assert values[":sk_prefix"] in {"POND#", "DEVICE#"}
        assert request["Limit"] == 1


def test_unstable_state_that_keeps_changing_fails_the_tenant_not_a_conflict() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1")],
        devices=[legacy_device("probe-1", "pond_1")],
    )
    client.add_legacy_tenant("tnt_2", ponds=[legacy_pond("pond_2", tenant_id="tnt_2")], devices=[])
    flips = {"count": 0}

    def churn_lookup(fake: FakeMigrationDynamo, key: tuple[str, str]) -> None:
        if key == ("DEVICE#probe-1", "META"):
            flips["count"] += 1
            fake.items[key]["pond_id"] = f"pond_churn_{flips['count']}"

    client.before_get_item = churn_lookup
    sleeps: list[float] = []

    result = run(client, apply=True, sleep=sleeps.append, tenants=["tnt_1", "tnt_2"])

    assert result.result == "failed"
    assert exit_code_for(result.result) == 1
    assert result.conflicts == []
    assert result.schema_conflicts == 0
    assert result.tenants_completed == 1  # tnt_2 still migrated
    assert result.scope_completed is False
    assert len(sleeps) == 2
    assert all(0 <= delay <= 0.1 for delay in sleeps)
    assert all("tnt_1" not in str(request) for _, request in client.write_calls)


def test_stable_partial_deployment_group_stays_a_schema_conflict() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1")],
        devices=[legacy_device("probe-1", "pond_1")],
    )
    client.put_raw(deployment_marker_item("tnt_1", COMPONENT_ID, seq=3, last_ended_at=LATER))
    sleeps: list[float] = []

    result = run(client, apply=True, sleep=sleeps.append)

    assert result.result == "schema_conflict"
    assert [(c.kind, c.key) for c in result.conflicts] == [
        ("deployment_marker", f"COMPONENT#{COMPONENT_ID}/DEPLOYMENT#MARKER")
    ]
    assert len(sleeps) == 2


# Codex findings: Device fences, ended-group validation, malformed versions, deadline -----------


def test_v1_move_between_lookup_and_current_reads_is_not_reverted() -> None:
    client = migrated_store()
    client.put_raw(legacy_pond("pond_2"))
    run(client, apply=True)  # pond_2 Asset exists
    deterministic = _deterministic_deployment()
    moved = Deployment(
        tenant_id="tnt_1",
        deployment_id=FOREIGN_DEPLOYMENT_ID,
        component_id=COMPONENT_ID,
        site_id=default_site_id("tnt_1"),
        asset_id=asset_id_for_legacy_pond("tnt_1", "pond_2"),
        started_at=MOVED_AT,
        created_at=MOVED_AT,
        updated_at=MOVED_AT,
        version=1,
    )
    current_key = _item_key(deployment_to_item(deterministic, form="current"))
    seen = {"lookup": False}

    def projection_move(fake: FakeMigrationDynamo, key: tuple[str, str]) -> None:
        if key == ("DEVICE#probe-1", "META"):
            seen["lookup"] = True
        elif key == current_key and seen["lookup"]:
            fake.before_get_item = None
            # An M1-05-like flag-on move commits after the lookup read, before CURRENT is read.
            _move_device_at(fake, "pond_2", MOVED_AT)
            ended = deterministic.model_copy(
                update={"status": DeploymentStatus.ENDED, "ended_at": MOVED_AT, "version": 2}
            )
            for form in ("mirror", "history"):
                fake.put_raw(deployment_to_item(ended, form=form))
            for form in ("mirror", "history", "current"):
                fake.put_raw(deployment_to_item(moved, form=form))
            fake.items[_item_key(deployment_marker_key(COMPONENT_ID))].update(
                {"seq": 2, "last_ended_at": "2026-06-01T09:00:00.000000Z"}
            )

    client.before_get_item = projection_move
    client.calls.clear()

    result = run(client, apply=True)

    assert result.result == "success"
    assert result.deployments_created == 0
    assert _current(client)["deployment_id"] == FOREIGN_DEPLOYMENT_ID
    assert _current(client)["asset_id"] == asset_id_for_legacy_pond("tnt_1", "pond_2")
    assert len(_history_rows(client)) == 2
    # The stale repair was attempted and refused by the Device fences.
    assert any("DEPLOYMENT#CURRENT" in str(tx) for tx in client.transactions)


def _ended_group_store() -> FakeMigrationDynamo:
    client = migrated_store()
    deterministic = _deterministic_deployment()
    del client.items[_item_key(deployment_to_item(deterministic, form="current"))]
    for form in ("mirror", "history"):
        client.items[_item_key(deployment_to_item(deterministic, form=form))].update(
            {"status": "ended", "ended_at": "2026-06-01T09:00:00.000000Z", "version": 2}
        )
    client.items[_item_key(deployment_marker_key(COMPONENT_ID))].update(
        {"seq": 2, "last_ended_at": "2026-06-01T09:00:00.000000Z"}
    )
    return client


def test_valid_ended_group_is_already_migrated() -> None:
    result = run(_ended_group_store(), apply=True)
    assert result.result == "success"
    assert result.already_migrated == 2


@pytest.mark.parametrize(
    ("damage", "kind"),
    [
        ("history_active", "deployment_history"),
        ("marker_foreign_component", "deployment_marker"),
        ("marker_before_end", "deployment_marker"),
    ],
)
def test_inconsistent_ended_group_is_a_schema_conflict(damage: str, kind: str) -> None:
    client = _ended_group_store()
    deterministic = _deterministic_deployment()
    if damage == "history_active":
        history = client.items[_item_key(deployment_to_item(deterministic, form="history"))]
        history.update({"status": "active"})
        del history["ended_at"]
    elif damage == "marker_foreign_component":
        client.items[_item_key(deployment_marker_key(COMPONENT_ID))]["component_id"] = (
            "cmp_" + "e" * 32
        )
    else:
        client.items[_item_key(deployment_marker_key(COMPONENT_ID))]["last_ended_at"] = (
            "2026-05-01T00:00:00.000000Z"
        )
    client.calls.clear()

    result = run(client, apply=True)

    assert result.result == "schema_conflict"
    assert [c.kind for c in result.conflicts] == [kind]
    assert client.transactions == []


def test_decommissioned_device_with_stray_marker_is_a_schema_conflict() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1")],
        devices=[legacy_device("probe-1", "pond_1", status="decommissioned")],
    )
    client.put_raw(deployment_marker_item("tnt_1", COMPONENT_ID, seq=1, last_ended_at=None))

    result = run(client, apply=True)

    assert result.result == "schema_conflict"
    assert [c.kind for c in result.conflicts] == ["deployment_marker"]
    assert client.write_calls == []


@pytest.mark.parametrize("version", [None, "three", Decimal("2.5"), True])
def test_malformed_lookup_version_is_a_schema_conflict(version: Any) -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1")],
        devices=[legacy_device("probe-1", "pond_1")],
    )
    lookup = client.items[("DEVICE#probe-1", "META")]
    if version is None:
        del lookup["version"]
    else:
        lookup["version"] = version

    result = run(client, apply=True)

    assert result.result == "schema_conflict"
    assert [(c.kind, c.key) for c in result.conflicts] == [("device_lookup", "DEVICE#probe-1/META")]
    assert client.write_calls == []


def test_deadline_passing_during_last_record_planning_writes_nothing() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1")],
        devices=[legacy_device("probe-1", "pond_1")],
    )
    now = {"value": 0.0}

    def expire_during_device_planning(fake: FakeMigrationDynamo, key: tuple[str, str]) -> None:
        if key == ("DEVICE#probe-1", "META"):
            now["value"] = 1_000.0

    client.before_get_item = expire_during_device_planning
    migrator = DefaultSiteAssetMigrator(
        client, clock=fixed_clock, monotonic=lambda: now["value"], sleep=lambda _delay: None
    )

    result = migrator.run(["tnt_1"], apply=True, page_size=25, deadline_seconds=300)

    assert result.result == "deadline_exceeded"
    assert exit_code_for(result.result) == 1
    assert result.scope_completed is False
    assert client.write_calls == []


# Codex round 2: stored types, archived Site, marker fence, apply deadline, Component checks ----


@pytest.mark.parametrize(
    ("row", "value", "kind"),
    [
        ("tenant", "4", "tenant_invalid"),
        ("tenant", True, "tenant_invalid"),
        ("pond", "2", "pond_invalid"),
        ("pond", True, "pond_invalid"),
        ("device", "3", "device_invalid"),
        ("device", Decimal("3.5"), "device_invalid"),
    ],
)
def test_legacy_versions_must_be_stored_integers(row: str, value: Any, kind: str) -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1")],
        devices=[legacy_device("probe-1", "pond_1")],
    )
    key = {
        "tenant": ("TENANT#tnt_1", "META"),
        "pond": ("TENANT#tnt_1", "POND#pond_1"),
        "device": ("TENANT#tnt_1", "DEVICE#probe-1"),
    }[row]
    client.items[key]["version"] = value

    dry = run(client, apply=False)

    assert dry.result == "schema_conflict"
    assert [(c.kind, c.key) for c in dry.conflicts] == [(kind, f"{key[0]}/{key[1]}")]


def _archived_site(tenant_id: str = "tnt_1") -> dict[str, Any]:
    site = default_site_for_tenant(tenant_model(tenant_id))
    return {**site_to_item(site), "status": "archived"}


def test_active_asset_is_never_created_under_an_archived_site() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1", ponds=[legacy_pond("pond_1")], devices=[]
    )
    client.put_raw(_archived_site())

    result = run(client, apply=True)

    assert result.result == "schema_conflict"
    assert [(c.kind, c.key) for c in result.conflicts] == [
        ("site", f"TENANT#tnt_1/SITE#{default_site_id('tnt_1')}")
    ]
    assert client.write_calls == []


def test_archived_pond_asset_may_be_created_under_an_archived_site() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1", ponds=[{**legacy_pond("pond_1"), "status": "archived"}], devices=[]
    )
    client.put_raw(_archived_site())

    result = run(client, apply=True)

    assert result.result == "success"
    assert result.assets_created == 1


def test_site_archived_between_plan_and_pond_commit_is_fenced() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1", ponds=[legacy_pond("pond_1")], devices=[]
    )
    site_key = _item_key(KEYS.site("tnt_1", default_site_id("tnt_1")))

    def archive_site(fake: FakeMigrationDynamo, actions: list[dict]) -> None:
        fake.items[site_key]["status"] = "archived"

    client.before_transact = on_transaction_with("ASSET#", archive_site)

    result = run(client, apply=True)

    assert result.result == "schema_conflict"
    assert [c.kind for c in result.conflicts] == ["site"]
    assert result.assets_created == 0
    assert (
        client.get_raw(*KEYS.asset("tnt_1", asset_id_for_legacy_pond("tnt_1", "pond_1")).values())
        is None
    )


@pytest.mark.parametrize("scenario", ["already_migrated", "drift"])
def test_marker_ended_after_current_start_is_a_conflict(scenario: str) -> None:
    client = migrated_store() if scenario == "already_migrated" else moved_after_migration()
    client.items[_item_key(deployment_marker_key(COMPONENT_ID))]["last_ended_at"] = (
        "2027-01-01T00:00:00.000000Z"
    )
    client.calls.clear()

    result = run(client, apply=True)

    assert result.result == "schema_conflict"
    assert [c.kind for c in result.conflicts] == ["deployment_marker"]
    assert not any("DEPLOYMENT#CURRENT" in str(tx) for tx in client.transactions)


def test_deadline_passing_during_retry_backoff_stops_before_the_next_write() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1", ponds=[legacy_pond("pond_1")], devices=[]
    )
    client.fail_once["transact_write_items"] = _transaction_conflict()
    now = {"value": 0.0}

    def slow_backoff(_delay: float) -> None:
        now["value"] = 1_000.0

    migrator = DefaultSiteAssetMigrator(
        client, clock=fixed_clock, monotonic=lambda: now["value"], sleep=slow_backoff
    )

    result = migrator.run(["tnt_1"], apply=True, page_size=25, deadline_seconds=300)

    assert result.result == "deadline_exceeded"
    assert exit_code_for(result.result) == 1
    assert len(client.transactions) == 1
    assert [name for name, _ in client.write_calls if name == "update_item"] == []  # no receipt
    assert client.get_raw(*KEYS.migration_receipt("tnt_1").values()) is None


def test_component_decommissioned_between_plan_and_commit_gets_no_deployment() -> None:
    client = migrated_store()
    deterministic = _deterministic_deployment()
    for form in ("mirror", "history", "current"):
        del client.items[_item_key(deployment_to_item(deterministic, form=form))]
    del client.items[_item_key(deployment_marker_key(COMPONENT_ID))]

    def decommission(fake: FakeMigrationDynamo, actions: list[dict]) -> None:
        for key in _component_keys():
            fake.items[key]["status"] = "decommissioned"

    client.before_transact = decommission
    client.calls.clear()

    result = run(client, apply=True)

    assert result.result == "success"
    assert (result.deployments_created, result.devices_projected) == (0, 0)
    assert result.already_migrated == 2
    assert _current(client) is None
    refused = client.transactions[0]
    checks = [a["ConditionCheck"]["Key"] for a in refused if "ConditionCheck" in a]
    assert client.serialize(KEYS.component(COMPONENT_ID)) in checks


@pytest.mark.parametrize("row", ["canonical", "mirror"])
@pytest.mark.parametrize("damage", ["wrong", "missing"])
def test_component_probe_profile_id_is_part_of_identity(row: str, damage: str) -> None:
    client = migrated_store()
    key = _item_key(
        KEYS.component(COMPONENT_ID)
        if row == "canonical"
        else KEYS.component_mirror("tnt_1", COMPONENT_ID)
    )
    if damage == "wrong":
        client.items[key]["probe_profile"] = {"probe_profile_id": "prb_" + "f" * 32}
    else:
        del client.items[key]["probe_profile"]

    result = run(client, apply=False)

    assert result.result == "schema_conflict"
    assert [(c.kind, c.key) for c in result.conflicts] == [("component", f"{key[0]}/{key[1]}")]


def test_mutable_probe_profile_fields_are_ignored() -> None:
    client = migrated_store()
    for row_key in _component_keys():
        client.items[row_key]["probe_profile"]["serial_number"] = "SN-42"

    assert run(client, apply=True).already_migrated == 2


def test_current_on_the_pond_asset_under_another_site_is_a_conflict() -> None:
    client = migrated_store()
    deterministic = _deterministic_deployment()
    for form in ("mirror", "history", "current"):
        client.items[_item_key(deployment_to_item(deterministic, form=form))]["site_id"] = (
            "site_" + "d" * 32
        )
    client.calls.clear()

    result = run(client, apply=True)

    assert result.result == "schema_conflict"
    assert [c.kind for c in result.conflicts] == ["deployment_current"]
    assert client.transactions == []


# Codex round 3 + sweep: survivors, source fences, receipt ownership, stored ints ---------------


def _component_keys() -> tuple[tuple[str, str], tuple[str, str]]:
    return (
        _item_key(KEYS.component(COMPONENT_ID)),
        _item_key(KEYS.component_mirror("tnt_1", COMPONENT_ID)),
    )


@pytest.mark.parametrize("survivor", ["canonical", "mirror"])
def test_missing_component_sibling_is_rebuilt_from_the_survivor(survivor: str) -> None:
    client = migrated_store()
    canonical_key, mirror_key = _component_keys()
    survivor_key, missing_key = (
        (canonical_key, mirror_key) if survivor == "canonical" else (mirror_key, canonical_key)
    )
    client.items[survivor_key].update(
        {"name": "Renamed in v2", "status": "decommissioned", "version": Decimal(3)}
    )
    del client.items[missing_key]
    client.calls.clear()

    result = run(client, apply=True)

    assert result.result == "success"
    assert result.components_created == 1
    rebuilt = client.items[missing_key]
    survivor_row = client.items[survivor_key]
    assert {k: v for k, v in rebuilt.items() if k not in {"PK", "SK", "entity_type"}} == {
        k: v for k, v in survivor_row.items() if k not in {"PK", "SK", "entity_type"}
    }
    assert rebuilt["entity_type"] == (
        "component_mirror" if survivor == "canonical" else "component"
    )
    (tx,) = client.transactions
    fences = [a["ConditionCheck"] for a in tx if "ConditionCheck" in a]
    survivor_fence = next(
        f for f in fences if f["Key"] == client.serialize(dict(zip(("PK", "SK"), survivor_key)))
    )
    assert "#version = :observed_version" in survivor_fence["ConditionExpression"]


def test_component_survivor_changed_before_commit_is_replanned_from_the_new_version() -> None:
    client = migrated_store()
    canonical_key, mirror_key = _component_keys()
    del client.items[mirror_key]

    def rename_survivor(fake: FakeMigrationDynamo, actions: list[dict]) -> None:
        fake.items[canonical_key].update({"name": "Renamed", "version": Decimal(2)})

    client.before_transact = on_transaction_with("COMPONENT#", rename_survivor)

    result = run(client, apply=True)

    assert result.result == "success"
    assert client.items[mirror_key]["name"] == "Renamed"
    assert client.items[mirror_key]["version"] == 2


def test_divergent_component_survivor_is_a_conflict() -> None:
    client = migrated_store()
    canonical_key, mirror_key = _component_keys()
    client.items[canonical_key]["device_id"] = "probe-other"
    del client.items[mirror_key]

    result = run(client, apply=True)

    assert result.result == "schema_conflict"
    assert [(c.kind, c.key) for c in result.conflicts] == [
        ("component", f"{canonical_key[0]}/{canonical_key[1]}")
    ]


def test_ended_group_history_on_another_site_is_a_conflict() -> None:
    client = _ended_group_store()
    history_key = _item_key(deployment_to_item(_deterministic_deployment(), form="history"))
    client.items[history_key]["site_id"] = "site_" + "c" * 32

    result = run(client, apply=True)

    assert result.result == "schema_conflict"
    assert [c.kind for c in result.conflicts] == ["deployment_history"]


@pytest.mark.parametrize(
    "damage",
    [
        {"pond_id": None},
        {"pond_id": 7},
        {"pond_id": None, "schema_version": None},
    ],
)
def test_malformed_schema_v1_device_row_is_reported_not_skipped(damage: dict[str, Any]) -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1")],
        devices=[legacy_device("probe-1", "pond_1")],
    )
    row = client.items[("TENANT#tnt_1", "DEVICE#probe-1")]
    for name, value in damage.items():
        if value is None:
            del row[name]
        else:
            row[name] = value

    result = run(client, apply=False)

    assert result.result == "schema_conflict"
    assert [(c.kind, c.key) for c in result.conflicts] == [
        ("device_invalid", "TENANT#tnt_1/DEVICE#probe-1")
    ]


def test_lookup_version_diverging_from_the_device_row_is_a_conflict() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1")],
        devices=[legacy_device("probe-1", "pond_1")],
    )
    client.items[("DEVICE#probe-1", "META")]["version"] = Decimal(9)

    result = run(client, apply=True)

    assert result.result == "schema_conflict"
    assert [(c.kind, c.key) for c in result.conflicts] == [("device_lookup", "DEVICE#probe-1/META")]
    assert client.write_calls == []


def _linked_pond_without_projection() -> FakeMigrationDynamo:
    """A Pond whose links were set (e.g. by the `/v1` projection) but whose rows are absent."""
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[
            {
                **legacy_pond("pond_1"),
                "v2_site_id": default_site_id("tnt_1"),
                "v2_asset_id": asset_id_for_legacy_pond("tnt_1", "pond_1"),
                "v2_projection_version": 1,
            }
        ],
        devices=[],
    )
    return client


def test_pond_plan_without_link_update_fences_the_pond_row() -> None:
    client = _linked_pond_without_projection()

    result = run(client, apply=True)

    assert result.result == "success"
    assert (result.assets_created, result.legacy_links_updated) == (1, 0)
    pond_tx = next(tx for tx in client.transactions if "ASSET#" in str(tx))
    fence = pond_tx[-1]["ConditionCheck"]
    assert fence["Key"] == client.serialize({"PK": "TENANT#tnt_1", "SK": "POND#pond_1"})
    assert "#version = :observed_version" in fence["ConditionExpression"]


def test_pond_edited_before_a_fenced_commit_is_replanned() -> None:
    client = _linked_pond_without_projection()

    def rename_pond(fake: FakeMigrationDynamo, actions: list[dict]) -> None:
        fake.items[("TENANT#tnt_1", "POND#pond_1")].update(
            {"name": "Renamed", "version": Decimal(3)}
        )

    client.before_transact = on_transaction_with("ASSET#", rename_pond)

    result = run(client, apply=True)

    asset_key = KEYS.asset("tnt_1", asset_id_for_legacy_pond("tnt_1", "pond_1"))
    assert result.result == "success"
    assert sum("ASSET#" in str(tx) for tx in client.transactions) == 2
    assert client.get_raw(*asset_key.values())["name"] == "Renamed"
    assert client.get_raw(*asset_key.values())["version"] == 3


def test_foreign_row_at_the_receipt_key_is_never_overwritten() -> None:
    client = FakeMigrationDynamo()
    client.add_legacy_tenant("tnt_1", ponds=[], devices=[])
    client.add_legacy_tenant("tnt_2", ponds=[], devices=[])
    foreign = {**KEYS.migration_receipt("tnt_1"), "entity_type": "something_else", "owner": "x"}
    client.put_raw(foreign)

    result = run(client, apply=True, tenants=["tnt_1", "tnt_2"])

    assert result.result == "schema_conflict"
    assert [(c.tenant_id, c.kind) for c in result.conflicts] == [("tnt_1", "migration_receipt")]
    assert result.tenants_completed == 1
    assert client.get_raw(*KEYS.migration_receipt("tnt_1").values()) == normalized(foreign)
    assert client.get_raw(*KEYS.migration_receipt("tnt_2").values())["run_count"] == 1


def test_ended_group_marker_seq_must_not_be_a_boolean() -> None:
    client = _ended_group_store()
    client.items[_item_key(deployment_marker_key(COMPONENT_ID))]["seq"] = True

    result = run(client, apply=True)

    assert result.result == "schema_conflict"
    assert [c.kind for c in result.conflicts] == ["deployment_marker"]


def test_profile_without_its_asset_is_a_conflict() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1", ponds=[legacy_pond("pond_1")], devices=[]
    )
    projection = asset_and_profile_for_legacy_pond(pond_model("pond_1"), default_site_id("tnt_1"))
    client.put_raw(pond_profile_to_item(projection.pond_profile))

    result = run(client, apply=True)

    assert result.result == "schema_conflict"
    assert [c.kind for c in result.conflicts] == ["asset"]


def test_profile_is_rebuilt_from_a_diverged_asset_survivor() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1", ponds=[legacy_pond("pond_1")], devices=[]
    )
    projection = asset_and_profile_for_legacy_pond(pond_model("pond_1"), default_site_id("tnt_1"))
    client.put_raw(
        {
            **asset_to_item(projection.asset),
            "name": "Renamed in v2",
            "version": 7,
            "updated_at": "2026-09-01T00:00:00.000000Z",
        }
    )

    result = run(client, apply=True)

    profile_key = _item_key(KEYS.pond_profile(projection.asset.asset_id))
    assert result.result == "success"
    assert result.assets_created == 0
    profile = client.items[profile_key]
    assert profile["version"] == 7
    assert profile["updated_at"] == "2026-09-01T00:00:00.000000Z"
    assert profile["legacy_pond_id"] == "pond_1"


def test_tenant_renamed_before_the_site_commit_is_replanned_with_the_new_name() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(tenant_id="tnt_1", ponds=[], devices=[])

    def rename_tenant(fake: FakeMigrationDynamo, actions: list[dict]) -> None:
        fake.items[("TENANT#tnt_1", "META")].update({"name": "Renamed", "version": Decimal(5)})

    client.before_transact = on_transaction_with("SITE#", rename_tenant)

    result = run(client, apply=True)

    assert result.result == "success"
    assert result.default_sites_created == 1
    site = client.get_raw(*KEYS.site("tnt_1", default_site_id("tnt_1")).values())
    assert (site["name"], site["version"]) == ("Renamed", 5)


# Codex round 4 + sweep: parsed siblings, Site parents on Deployment writes, codec parses -------


@pytest.mark.parametrize(
    "change",
    [{"name": "Only canonical renamed"}, {"version": Decimal(4)}, {"status": "decommissioned"}],
)
def test_component_pair_must_agree_on_persisted_fields(change: dict[str, Any]) -> None:
    client = migrated_store()
    canonical_key, _ = _component_keys()
    client.items[canonical_key].update(change)

    result = run(client, apply=False)

    assert result.result == "schema_conflict"
    assert [(c.kind, c.key) for c in result.conflicts] == [
        ("component", f"{canonical_key[0]}/{canonical_key[1]}")
    ]


def test_component_row_that_does_not_parse_is_a_conflict() -> None:
    client = migrated_store()
    _, mirror_key = _component_keys()
    client.items[mirror_key]["status"] = "bogus"

    result = run(client, apply=False)

    assert [(c.kind, c.key) for c in result.conflicts] == [
        ("component", f"{mirror_key[0]}/{mirror_key[1]}")
    ]


def _archive_default_site(fake: FakeMigrationDynamo) -> None:
    fake.items[_item_key(KEYS.site("tnt_1", default_site_id("tnt_1")))]["status"] = "archived"


def test_drift_into_an_asset_under_an_archived_site_is_not_repaired() -> None:
    client = moved_after_migration()
    run(client, apply=False)
    asset_2 = asset_and_profile_for_legacy_pond(pond_model("pond_2"), default_site_id("tnt_1"))
    client.put_raw(asset_to_item(asset_2.asset))
    client.put_raw(pond_profile_to_item(asset_2.pond_profile))
    _archive_default_site(client)
    before = dict(_current(client))
    client.calls.clear()

    result = run(client, apply=True)

    assert result.result == "success"
    assert result.deployments_created == 0
    assert _current(client) == before
    assert not any("DEPLOYMENT#CURRENT" in str(tx) for tx in client.transactions)


def test_site_archived_before_the_drift_commit_is_fenced() -> None:
    client = moved_after_migration()
    client.before_transact = on_transaction_with(
        "DEPLOYMENT#CURRENT", lambda fake, actions: _archive_default_site(fake)
    )
    before = dict(_current(client))

    result = run(client, apply=True)

    assert result.result == "success"
    assert result.deployments_created == 0
    assert _current(client) == before


def test_fresh_deployment_under_an_archived_site_is_a_conflict() -> None:
    client = migrated_store()
    deterministic = _deterministic_deployment()
    for form in ("mirror", "history", "current"):
        del client.items[_item_key(deployment_to_item(deterministic, form=form))]
    del client.items[_item_key(deployment_marker_key(COMPONENT_ID))]
    _archive_default_site(client)
    client.calls.clear()

    result = run(client, apply=True)

    assert result.result == "schema_conflict"
    assert [c.kind for c in result.conflicts] == ["site"]
    assert client.transactions == []


@pytest.mark.parametrize(
    ("form", "change", "kind"),
    [
        ("mirror", {"updated_at": "2026-12-01T00:00:00.000000Z"}, "deployment_current"),
        (
            "mirror",
            {"status": "ended", "ended_at": "2026-12-01T00:00:00.000000Z"},
            "deployment_current",
        ),
        ("history", {"started_at": "not-a-time"}, "deployment_history"),
        ("history", {"created_at": "2020-01-01T00:00:00.000000Z"}, "deployment_history"),
    ],
)
def test_active_current_siblings_must_parse_and_match_exactly(
    form: str, change: dict[str, Any], kind: str
) -> None:
    client = migrated_store()
    deterministic = _deterministic_deployment()
    key = _item_key(deployment_to_item(deterministic, form=form))
    client.items[key].update(change)
    client.calls.clear()

    result = run(client, apply=True)

    assert result.result == "schema_conflict"
    assert [(c.kind, c.key) for c in result.conflicts] == [(kind, f"{key[0]}/{key[1]}")]
    assert client.transactions == []


def test_ended_group_history_must_match_the_mirror_exactly() -> None:
    client = _ended_group_store()
    history_key = _item_key(deployment_to_item(_deterministic_deployment(), form="history"))
    client.items[history_key]["updated_at"] = "2027-01-01T00:00:00.000000Z"

    result = run(client, apply=True)

    assert [c.kind for c in result.conflicts] == ["deployment_history"]


@pytest.mark.parametrize(
    ("row", "change", "kind"),
    [
        ("site", {"status": "bogus"}, "site"),
        ("asset", {"status": "bogus"}, "asset"),
        ("asset", {"version": "2"}, "asset"),
        ("profile", {"created_at": "yesterday"}, "pond_profile"),
        ("lookup", {"created_at": "yesterday"}, "device_lookup"),
    ],
)
def test_existing_rows_must_parse_through_their_codecs(
    row: str, change: dict[str, Any], kind: str
) -> None:
    client = migrated_store()
    asset_id = asset_id_for_legacy_pond("tnt_1", "pond_1")
    key = {
        "site": _item_key(KEYS.site("tnt_1", default_site_id("tnt_1"))),
        "asset": _item_key(KEYS.asset("tnt_1", asset_id)),
        "profile": _item_key(KEYS.pond_profile(asset_id)),
        "lookup": ("DEVICE#probe-1", "META"),
    }[row]
    client.items[key].update(change)

    result = run(client, apply=False)

    assert result.result == "schema_conflict"
    assert (kind, f"{key[0]}/{key[1]}") in [(c.kind, c.key) for c in result.conflicts]


# Codex round 5: stored ints before every codec, receipt run_count ------------------------------


@pytest.mark.parametrize("form", ["mirror", "history"])
@pytest.mark.parametrize("version", [True, "1"])
def test_active_current_sibling_version_must_be_a_stored_integer(form: str, version: Any) -> None:
    client = migrated_store()
    key = _item_key(deployment_to_item(_deterministic_deployment(), form=form))
    client.items[key]["version"] = version
    client.calls.clear()

    result = run(client, apply=True)

    kind = "deployment_current" if form == "mirror" else "deployment_history"
    assert result.result == "schema_conflict"
    assert [(c.kind, c.key) for c in result.conflicts] == [(kind, f"{key[0]}/{key[1]}")]
    assert client.transactions == []


def test_ended_group_history_version_must_be_a_stored_integer() -> None:
    client = _ended_group_store()
    history_key = _item_key(deployment_to_item(_deterministic_deployment(), form="history"))
    client.items[history_key]["version"] = "2"

    result = run(client, apply=True)

    assert [c.kind for c in result.conflicts] == ["deployment_history"]


@pytest.mark.parametrize("run_count", ["3", True])
def test_own_receipt_with_a_non_numeric_run_count_is_a_conflict(run_count: Any) -> None:
    client = migrated_store()
    receipt_key = _item_key(KEYS.migration_receipt("tnt_1"))
    client.items[receipt_key]["run_count"] = run_count
    before = dict(client.items[receipt_key])
    client.add_legacy_tenant("tnt_2", ponds=[], devices=[])

    result = run(client, apply=True, tenants=["tnt_1", "tnt_2"])

    assert result.result == "schema_conflict"
    assert [(c.tenant_id, c.kind) for c in result.conflicts] == [("tnt_1", "migration_receipt")]
    assert client.items[receipt_key] == before
    assert result.tenants_completed == 1


def test_receipt_validation_exception_maps_to_a_receipt_conflict() -> None:
    client = migrated_store()
    client.fail_once["update_item"] = ClientError(
        {"Error": {"Code": "ValidationException", "Message": "operand type"}}, "UpdateItem"
    )

    result = run(client, apply=True)

    assert result.result == "schema_conflict"
    assert [c.kind for c in result.conflicts] == ["migration_receipt"]
    assert exit_code_for(result.result) == 2


@pytest.mark.parametrize("version", ["4", True])
def test_existing_site_version_must_be_a_stored_integer(version: Any) -> None:
    client = migrated_store()
    site_key = _item_key(KEYS.site("tnt_1", default_site_id("tnt_1")))
    client.items[site_key]["version"] = version

    result = run(client, apply=False)

    assert result.result == "schema_conflict"
    assert [(c.kind, c.key) for c in result.conflicts] == [("site", f"{site_key[0]}/{site_key[1]}")]


# Codex round 6: numeric ranges, run_count, ACTIVE Asset for new Deployments --------------------


@pytest.mark.parametrize("run_count", [Decimal("1.5"), -1])
def test_receipt_run_count_must_be_a_non_negative_integer(run_count: Any) -> None:
    client = migrated_store()
    receipt_key = _item_key(KEYS.migration_receipt("tnt_1"))
    client.items[receipt_key]["run_count"] = run_count
    before = dict(client.items[receipt_key])

    result = run(client, apply=True)

    assert result.result == "schema_conflict"
    assert [c.kind for c in result.conflicts] == ["migration_receipt"]
    assert client.items[receipt_key] == before


def _remove_deployment_group(client: FakeMigrationDynamo) -> None:
    deterministic = _deterministic_deployment()
    for form in ("mirror", "history", "current"):
        del client.items[_item_key(deployment_to_item(deterministic, form=form))]
    del client.items[_item_key(deployment_marker_key(COMPONENT_ID))]


@pytest.mark.parametrize(
    ("row", "version", "kind"),
    [
        ("tenant", 0, "tenant_invalid"),
        ("pond", 0, "pond_invalid"),
        ("device", -1, "device_invalid"),
        ("site", 0, "site"),
        ("asset", 0, "asset"),
        ("profile", -2, "pond_profile"),
        ("component", 0, "component"),
        ("mirror", 0, "deployment_current"),
    ],
)
def test_stored_versions_must_be_at_least_one(row: str, version: int, kind: str) -> None:
    client = migrated_store()
    asset_id = asset_id_for_legacy_pond("tnt_1", "pond_1")
    keys = {
        "tenant": [("TENANT#tnt_1", "META")],
        "pond": [("TENANT#tnt_1", "POND#pond_1")],
        "device": [("TENANT#tnt_1", "DEVICE#probe-1"), ("DEVICE#probe-1", "META")],
        "site": [_item_key(KEYS.site("tnt_1", default_site_id("tnt_1")))],
        "asset": [_item_key(KEYS.asset("tnt_1", asset_id))],
        "profile": [_item_key(KEYS.pond_profile(asset_id))],
        "component": list(_component_keys()),
        "mirror": [_item_key(deployment_to_item(_deterministic_deployment()))],
    }[row]
    for key in keys:
        client.items[key]["version"] = version

    result = run(client, apply=False)

    assert result.result == "schema_conflict"
    assert kind in [c.kind for c in result.conflicts]


def test_asset_archived_through_v2_gets_no_initial_deployment() -> None:
    client = migrated_store()
    _remove_deployment_group(client)
    asset_key = _item_key(KEYS.asset("tnt_1", asset_id_for_legacy_pond("tnt_1", "pond_1")))
    client.items[asset_key]["status"] = "archived"
    client.calls.clear()

    result = run(client, apply=True)

    assert result.result == "success"
    assert (result.deployments_created, result.devices_projected) == (0, 0)
    assert result.already_migrated == 2
    assert _current(client) is None
    assert client.transactions == []


def test_asset_archived_between_plan_and_initial_deployment_is_fenced() -> None:
    client = migrated_store()
    _remove_deployment_group(client)
    asset_key = _item_key(KEYS.asset("tnt_1", asset_id_for_legacy_pond("tnt_1", "pond_1")))

    def archive_asset(fake: FakeMigrationDynamo, actions: list[dict]) -> None:
        fake.items[asset_key]["status"] = "archived"

    client.before_transact = on_transaction_with("DEPLOYMENT#CURRENT", archive_asset)
    client.calls.clear()

    result = run(client, apply=True)

    assert result.result == "success"
    assert result.deployments_created == 0
    assert _current(client) is None
    refused = client.transactions[0]
    asset_check = next(
        a["ConditionCheck"]
        for a in refused
        if "ConditionCheck" in a
        and a["ConditionCheck"]["Key"] == client.serialize(dict(zip(("PK", "SK"), asset_key)))
    )
    assert "#status = :active" in asset_check["ConditionExpression"]


def test_archived_legacy_pond_still_projects_its_deployment() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[{**legacy_pond("pond_1"), "status": "archived"}],
        devices=[legacy_device("probe-1", "pond_1")],
    )

    result = run(client, apply=True)

    assert result.result == "success"
    assert result.deployments_created == 1
    device_tx = client.transactions[-1]
    asset_key = client.serialize(KEYS.asset("tnt_1", asset_id_for_legacy_pond("tnt_1", "pond_1")))
    asset_check = next(
        a["ConditionCheck"]
        for a in device_tx
        if "ConditionCheck" in a and a["ConditionCheck"]["Key"] == asset_key
    )
    assert "#status" not in asset_check["ConditionExpression"]


# Codex round 7: contradictory v2 rows, verification passes, receipt deadline ------------------


def test_schema_v2_device_row_with_pond_id_is_invalid_not_skipped() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1")],
        devices=[{**legacy_device("probe-1", "pond_1"), "schema_version": 2}],
    )

    result = run(client, apply=False)

    assert result.result == "schema_conflict"
    assert [(c.kind, c.key) for c in result.conflicts] == [
        ("device_invalid", "TENANT#tnt_1/DEVICE#probe-1")
    ]


def _insert_behind_device_cursor(fake: FakeMigrationDynamo) -> None:
    """A Pond and a Device written while the Device pages are read, behind the cursor."""
    fake.put_raw(legacy_pond("pond_0"))
    for row in (legacy_device("aaa-probe", "pond_0"),):
        fake.put_raw(row)
        fake.put_raw(
            {**row, "PK": "DEVICE#aaa-probe", "SK": "META", "entity_type": "device_lookup"}
        )


@pytest.mark.parametrize("apply", [True, False])
def test_rows_written_behind_the_query_cursor_are_found_by_verification(apply: bool) -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1",
        ponds=[legacy_pond("pond_1")],
        devices=[legacy_device("probe-1", "pond_1"), legacy_device("probe-2", "pond_1")],
    )
    original_query = client.query
    device_pages = {"count": 0}

    def query(**kwargs: Any) -> dict[str, Any]:
        values = client.deserialize(kwargs["ExpressionAttributeValues"])
        if values[":sk_prefix"] == "DEVICE#":
            device_pages["count"] += 1
            if device_pages["count"] == 2:  # page 1 consumed, cursor past "aaa-probe"
                _insert_behind_device_cursor(client)
        return original_query(**kwargs)

    client.query = query  # type: ignore[method-assign]

    result = run(client, apply=apply, page_size=1)

    assert result.result == "success"
    assert result.scope_completed is True
    if apply:
        assert result.assets_created == 2
        assert result.deployments_created == 3
        new_component = default_component_id_for_legacy_device("tnt_1", "aaa-probe")
        assert client.get_raw(*KEYS.current_deployment(new_component).values())["asset_id"] == (
            asset_id_for_legacy_pond("tnt_1", "pond_0")
        )
        assert client.get_raw(*KEYS.migration_receipt("tnt_1").values()) is not None
    else:
        assert (result.would_create_assets, result.would_create_deployments) == (2, 3)
        assert client.write_calls == []


def test_writer_that_keeps_inserting_fails_the_tenant_without_a_receipt() -> None:
    client = FakeMigrationDynamo.with_legacy_tenant(
        tenant_id="tnt_1", ponds=[legacy_pond("pond_m")], devices=[]
    )
    original_query = client.query
    inserted = {"count": 0}

    def query(**kwargs: Any) -> dict[str, Any]:
        values = client.deserialize(kwargs["ExpressionAttributeValues"])
        if values[":sk_prefix"] == "POND#":
            inserted["count"] += 1
            client.put_raw(legacy_pond(f"pond_a{inserted['count']:02d}"))
        return original_query(**kwargs)

    client.query = query  # type: ignore[method-assign]

    result = run(client, apply=True)

    assert result.result == "failed"
    assert exit_code_for(result.result) == 1
    assert result.scope_completed is False
    assert result.tenants_completed == 0
    assert client.get_raw(*KEYS.migration_receipt("tnt_1").values()) is None


def test_deadline_passing_during_the_receipt_read_writes_no_receipt() -> None:
    client = migrated_store()
    now = {"value": 0.0}
    receipt_key = _item_key(KEYS.migration_receipt("tnt_1"))

    def expire_on_receipt_read(fake: FakeMigrationDynamo, key: tuple[str, str]) -> None:
        if key == receipt_key:
            now["value"] = 1_000.0

    client.before_get_item = expire_on_receipt_read
    before = dict(client.items[receipt_key])
    client.calls.clear()
    migrator = DefaultSiteAssetMigrator(
        client, clock=second_clock, monotonic=lambda: now["value"], sleep=lambda _d: None
    )

    result = migrator.run(["tnt_1"], apply=True, page_size=25, deadline_seconds=300)

    assert result.result == "deadline_exceeded"
    assert [name for name, _ in client.calls if name == "update_item"] == []
    assert client.items[receipt_key] == before
