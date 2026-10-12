"""Test-only M1 acceptance harness. Nothing here is a production interface.

* `ConditionalDynamoClient` is a single-table boto3 fake that evaluates the condition/update
  expression subset the M1 adapters and the migration emit, so the real adapters run against it.
* `InMemoryM1Repositories` is an asynchronous implementation of the four frozen M1 repository
  protocols (`repositories/v2_contracts.py`).
* `M1AcceptanceScenario` is a synchronous facade that drives the real M1 services over either
  backend: `create_device`, `create_probe`, `list_components`, `start`, `move` and `resolve`.
"""

import asyncio
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError

from limnopulse_api.adapters.dynamodb_deployments import DynamoDeploymentRepository
from limnopulse_api.adapters.dynamodb_devices_v2 import DynamoDeviceComponentRepository
from limnopulse_api.adapters.dynamodb_integrations import DynamoIntegrationRepository
from limnopulse_api.adapters.dynamodb_sites_assets import DynamoSiteAssetRepository
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
from limnopulse_api.domain.deployments import Deployment, DeploymentStatus
from limnopulse_api.domain.devices_v2 import (
    CapabilityDeclaration,
    ComponentKind,
    ComponentStatus,
    DeviceComponent,
    DeviceStatus,
    DeviceV2,
    ensure_component_transition,
    ensure_device_transition,
)
from limnopulse_api.domain.integrations import (
    DeviceIntegration,
    IntegrationAccount,
    IntegrationAccountStatus,
)
from limnopulse_api.domain.v2_common import IDEMPOTENCY_RECEIPT_TTL, IdempotencyReceipt
from limnopulse_api.services.assets import AssetService
from limnopulse_api.services.components import (
    ComponentCreateCommand,
    ComponentService,
    ProbeProfileCommand,
)
from limnopulse_api.services.deployments import (
    DeploymentEndCommand,
    DeploymentService,
    DeploymentStartCommand,
)
from limnopulse_api.services.devices_v2 import DeviceV2CreateCommand, DeviceV2Service
from limnopulse_api.services.sites import SiteService

TABLE = "LimnopulseDomain"

# Condition-evaluating DynamoDB fake --------------------------------------------------------------

_TOKEN = re.compile(
    r"\s*(?:(?P<op><=|>=|<>|=|<|>)|(?P<paren>[(),])|(?P<word>[A-Za-z_#:][A-Za-z0-9_#:]*))"
)


class _Condition:
    """Recursive-descent evaluator for `OR`/`AND`/parentheses over comparisons, `IN (...)`,
    `attribute_exists`, `attribute_not_exists` and `attribute_type(x, :n)`."""

    def __init__(self, text: str, names: dict, values: dict, item: dict | None) -> None:
        self.tokens = [match.group().strip() for match in _TOKEN.finditer(text)]
        self.tokens = [token for token in self.tokens if token]
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

    def _operand(self, token: str) -> Any:
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
        if token == "attribute_type":
            assert self._take() == "("
            value = self.item.get(self._attribute(self._take()))
            assert self._take() == ","
            expected = self.values[self._take()]
            assert self._take() == ")"
            numeric = isinstance(value, (int, Decimal)) and not isinstance(value, bool)
            return expected == "N" and numeric
        operator = self._take()
        if operator == "IN":
            # `a IN (:x, :y, ...)`: true when the attribute equals any listed value.
            assert self._take() == "("
            candidates = [self._operand(self._take())]
            while self._peek() == ",":
                self._take()
                candidates.append(self._operand(self._take()))
            assert self._take() == ")"
            left = self._operand(token)
            return left is not None and left in candidates
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


def _split_top_level(expression: str, separator: str) -> list[str]:
    parts, depth, current, index = [], 0, "", 0
    while index < len(expression):
        if depth == 0 and expression.startswith(separator, index):
            parts.append(current.strip())
            current, index = "", index + len(separator)
            continue
        char = expression[index]
        depth += (char == "(") - (char == ")")
        current += char
        index += 1
    parts.append(current.strip())
    return parts


def _normalize(value: Any) -> Any:
    """Integral numbers become `int`; non-integral ones stay `Decimal`, which is what DynamoDB
    returns and what `TypeSerializer` accepts again (it rejects `float`)."""
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else value
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {key: _normalize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize(item) for item in value]
    return value


def _cancelled(reasons: list[dict[str, str]]) -> ClientError:
    return ClientError(
        {
            "Error": {"Code": "TransactionCanceledException", "Message": "cancelled"},
            "CancellationReasons": reasons,
        },
        "TransactWriteItems",
    )


def _conditional_failure(operation: str) -> ClientError:
    return ClientError(
        {"Error": {"Code": "ConditionalCheckFailedException", "Message": "failed"}}, operation
    )


class ConditionalDynamoClient:
    """Single table: consistent GetItem, key-condition Query with Limit/LastEvaluatedKey, and
    all-or-nothing conditional writes. `scan` is forbidden and counted."""

    def __init__(self) -> None:
        self.serializer = TypeSerializer()
        self.deserializer = TypeDeserializer()
        self.items: dict[tuple[str, str], dict[str, Any]] = {}
        self.lock = threading.RLock()
        self.transactions: list[list[dict[str, Any]]] = []
        self.write_calls = 0
        self.scan_calls = 0

    # Test helpers ---------------------------------------------------------------------------

    def seed(self, item: dict[str, Any]) -> None:
        normalized = self._plain(self._encode(_normalize(item)))
        self.items[(normalized["PK"], normalized["SK"])] = normalized

    def item(self, pk: str, sk: str) -> dict[str, Any] | None:
        return self.items.get((pk, sk))

    def snapshot(self) -> dict[tuple[str, str], dict[str, Any]]:
        return {key: dict(value) for key, value in self.items.items()}

    def keys_with_sk_prefix(self, prefix: str) -> set[tuple[str, str]]:
        return {key for key in self.items if key[1].startswith(prefix)}

    # boto3 surface --------------------------------------------------------------------------

    def get_item(self, **kwargs: Any) -> dict[str, Any]:
        key = self._plain(kwargs["Key"])
        with self.lock:
            item = self.items.get((key["PK"], key["SK"]))
        return {} if item is None else {"Item": self._encode(item)}

    def query(self, **kwargs: Any) -> dict[str, Any]:
        values = self._plain(kwargs["ExpressionAttributeValues"])
        condition = kwargs["KeyConditionExpression"]
        wanted = self._key_condition(condition, values)
        forward = kwargs.get("ScanIndexForward", True)
        with self.lock:
            rows = sorted(
                (
                    item
                    for (pk, sk), item in self.items.items()
                    if pk == values[":pk"] and wanted(sk)
                ),
                key=lambda item: item["SK"],
                reverse=not forward,
            )
        start = kwargs.get("ExclusiveStartKey")
        if start is not None:
            start_sk = self._plain(start)["SK"]
            rows = [
                row for row in rows if (row["SK"] > start_sk if forward else row["SK"] < start_sk)
            ]
        limit = kwargs.get("Limit")
        page = rows if limit is None else rows[:limit]
        response: dict[str, Any] = {"Items": [self._encode(item) for item in page]}
        # Like DynamoDB, a page that reaches Limit carries LastEvaluatedKey even at the end.
        if limit is not None and page and len(rows) >= limit:
            response["LastEvaluatedKey"] = self._encode(
                {"PK": page[-1]["PK"], "SK": page[-1]["SK"]}
            )
        return response

    def put_item(self, **kwargs: Any) -> dict[str, Any]:
        self._single("Put", kwargs, "PutItem")
        return {}

    def update_item(self, **kwargs: Any) -> dict[str, Any]:
        key = self._single("Update", kwargs, "UpdateItem")
        return {"Attributes": self._encode(self.items[key])}

    def delete_item(self, **kwargs: Any) -> dict[str, Any]:
        self._single("Delete", kwargs, "DeleteItem")
        return {}

    def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
        actions = kwargs["TransactItems"]
        assert 1 <= len(actions) <= 100
        with self.lock:
            self.write_calls += 1
            self.transactions.append(actions)
            keys = [self._action_key(action) for action in actions]
            assert len(set(keys)) == len(keys), "transaction keys must be distinct"
            reasons = [
                {"Code": "None" if self._holds(action, key) else "ConditionalCheckFailed"}
                for action, key in zip(actions, keys, strict=True)
            ]
            if any(reason["Code"] != "None" for reason in reasons):
                raise _cancelled(reasons)
            for action, key in zip(actions, keys, strict=True):
                self._commit(action, key)
        return {}

    def scan(self, **kwargs: Any) -> dict[str, Any]:
        self.scan_calls += 1
        raise AssertionError("Scan is forbidden")

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"unexpected DynamoDB client call: {name}")

    # Internals ------------------------------------------------------------------------------

    def _single(self, kind: str, request: dict[str, Any], operation: str) -> tuple[str, str]:
        action = {kind: request}
        with self.lock:
            self.write_calls += 1
            key = self._action_key(action)
            if not self._holds(action, key):
                raise _conditional_failure(operation)
            self._commit(action, key)
        return key

    @staticmethod
    def _key_condition(condition: str, values: dict[str, Any]) -> Callable[[str], bool]:
        if condition == "PK = :pk":
            return lambda sk: True
        if condition == "PK = :pk AND begins_with(SK, :sk_prefix)":
            return lambda sk: sk.startswith(values[":sk_prefix"])
        if condition == "PK = :pk AND SK BETWEEN :sk_lower AND :sk_upper":
            return lambda sk: values[":sk_lower"] <= sk <= values[":sk_upper"]
        if condition == "PK = :pk AND SK <= :sk_upper":
            return lambda sk: sk <= values[":sk_upper"]
        if condition == "PK = :pk AND SK >= :sk_lower":
            return lambda sk: sk >= values[":sk_lower"]
        raise AssertionError(f"unsupported key condition: {condition}")

    def _action_key(self, action: dict[str, Any]) -> tuple[str, str]:
        ((kind, request),) = action.items()
        raw = request["Item"] if kind == "Put" else request["Key"]
        key = self._plain({"PK": raw["PK"], "SK": raw["SK"]})
        return key["PK"], key["SK"]

    def _holds(self, action: dict[str, Any], key: tuple[str, str]) -> bool:
        ((_, request),) = action.items()
        expression = request.get("ConditionExpression")
        if expression is None:
            return True
        return _Condition(
            expression,
            request.get("ExpressionAttributeNames", {}),
            self._plain(request.get("ExpressionAttributeValues", {})),
            self.items.get(key),
        ).evaluate()

    def _commit(self, action: dict[str, Any], key: tuple[str, str]) -> None:
        ((kind, request),) = action.items()
        if kind == "ConditionCheck":
            return
        if kind == "Delete":
            self.items.pop(key, None)
            return
        if kind == "Put":
            self.items[key] = self._plain(request["Item"])
            return
        item = dict(self.items.get(key) or {"PK": key[0], "SK": key[1]})
        names = request.get("ExpressionAttributeNames", {})
        values = self._plain(request.get("ExpressionAttributeValues", {}))
        expression = request["UpdateExpression"]
        assert expression.startswith("SET "), expression
        for assignment in _split_top_level(expression[4:], ","):
            target, value = (part.strip() for part in assignment.split("=", 1))
            item[names.get(target, target)] = self._value(value, item, names, values)
        self.items[key] = item

    def _value(self, expression: str, item: dict, names: dict, values: dict) -> Any:
        terms = _split_top_level(expression, "+")
        if len(terms) > 1:
            return sum(self._value(term, item, names, values) for term in terms)
        default = re.fullmatch(r"if_not_exists\((\S+),\s*(\S+)\)", expression)
        if default:
            name = names.get(default.group(1), default.group(1))
            return item[name] if name in item else values[default.group(2)]
        return values[expression]

    def _plain(self, mapping: dict[str, Any]) -> dict[str, Any]:
        return {
            key: _normalize(self.deserializer.deserialize(value)) for key, value in mapping.items()
        }

    def _encode(self, item: dict[str, Any]) -> dict[str, Any]:
        return {key: self.serializer.serialize(value) for key, value in item.items()}


@dataclass(frozen=True)
class DynamoM1Repositories:
    """The four real M1 Dynamo adapters sharing one client, exactly as `main.py` wires them."""

    client: Any
    site_asset_repository: DynamoSiteAssetRepository
    device_component_repository: DynamoDeviceComponentRepository
    integration_repository: DynamoIntegrationRepository
    deployment_repository: DynamoDeploymentRepository

    @classmethod
    def over(
        cls, client: Any, *, clock: Callable[[], datetime], table_name: str = TABLE
    ) -> "DynamoM1Repositories":
        return cls(
            client=client,
            site_asset_repository=DynamoSiteAssetRepository(table_name, client, clock=clock),
            device_component_repository=DynamoDeviceComponentRepository(
                table_name, client, clock=clock
            ),
            integration_repository=DynamoIntegrationRepository(table_name, client, clock=clock),
            deployment_repository=DynamoDeploymentRepository(table_name, client, clock=clock),
        )

    def app_state(self) -> dict[str, Any]:
        return {
            "site_asset_repository": self.site_asset_repository,
            "device_component_repository": self.device_component_repository,
            "integration_repository": self.integration_repository,
            "deployment_repository": self.deployment_repository,
        }


# Asynchronous in-memory implementation of the four M1 protocols ---------------------------------


CONDITIONAL_CONFLICT = "conditional write conflict"
DEPLOYMENT_IDEMPOTENCY_CONFLICT = "idempotency key reused with a different request"
_TERMINAL_COMPONENT_STATUSES = frozenset({ComponentStatus.REPLACED, ComponentStatus.DECOMMISSIONED})
_PROVISIONABLE_ACCOUNT_STATUSES = frozenset(
    {IntegrationAccountStatus.PENDING, IntegrationAccountStatus.ACTIVE}
)


def _without_none(changes: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in changes.items() if value is not None}


@dataclass
class InMemoryM1Repositories:
    """Implements `SiteAssetRepository`, `DeviceComponentRepository`, `IntegrationRepository`
    and `DeploymentRepository` with the same observable behavior as the four Dynamo adapters.

    Every write mirrors its adapter step by step: the same global keys (Device lookup,
    Component canonical row, Asset PondProfile, external identity), the same ownership, parent
    and lifecycle checks in the same order, the same version fences, all-or-nothing commits,
    and the same exception types and messages, including the raw "conditional write conflict"
    an adapter surfaces for an unclassified conditional failure. Lifecycle transitions are
    validated where the adapters validate them (Devices, Components, Deployments); Site and
    Asset transitions are validated by `SiteService`/`AssetService` on top of either backend.
    `test_m1_acceptance_gate.py` proves the parity scenario by scenario.
    """

    clock: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))
    sites: dict[tuple[str, str], Site] = field(default_factory=dict)
    assets: dict[tuple[str, str], AssetWithPondProfile] = field(default_factory=dict)
    devices: dict[tuple[str, str], DeviceV2] = field(default_factory=dict)
    components: dict[str, DeviceComponent] = field(default_factory=dict)
    capabilities: dict[tuple[str, str, str], CapabilityDeclaration] = field(default_factory=dict)
    integration_accounts: dict[tuple[str, str], IntegrationAccount] = field(default_factory=dict)
    device_integrations: dict[tuple[str, str], DeviceIntegration] = field(default_factory=dict)
    deployments: dict[str, list[Deployment]] = field(default_factory=dict)
    receipts: dict[tuple[str, str, str], IdempotencyReceipt] = field(default_factory=dict)
    # Global, cross-tenant claims, like the production `EXTERNAL_ID#<provider>#<sha256>` row.
    external_identities: dict[tuple[str, str], str] = field(default_factory=dict)

    # Idempotency ------------------------------------------------------------------------------

    async def get_idempotency_replay(
        self, tenant_id: str, scope: str, key_hash: str, request_hash: str
    ) -> str | None:
        receipt = self.receipts.get((tenant_id, scope, key_hash))
        if receipt is None or self.clock() >= receipt.created_at + IDEMPOTENCY_RECEIPT_TTL:
            return None
        if receipt.request_hash != request_hash:
            raise ConflictError("idempotency key reused with a different request")
        return receipt.response_snapshot_json

    def _receipt_taken(self, receipt: IdempotencyReceipt) -> bool:
        """The adapters' receipt Put condition: `attribute_not_exists(PK) OR expires_at <= now`
        on whole epoch seconds."""
        existing = self.receipts.get((receipt.tenant_id, receipt.scope, receipt.key_hash))
        return existing is not None and int(existing.expires_at.timestamp()) > int(
            self.clock().timestamp()
        )

    def _store_receipt(self, receipt: IdempotencyReceipt) -> None:
        self.receipts[(receipt.tenant_id, receipt.scope, receipt.key_hash)] = receipt

    async def _replay_after_conflict(self, receipt: IdempotencyReceipt) -> str:
        snapshot = await self.get_idempotency_replay(
            receipt.tenant_id, receipt.scope, receipt.key_hash, receipt.request_hash
        )
        if snapshot is None:
            raise ConflictError("idempotency key is being used by a concurrent request")
        return snapshot

    # Sites and Assets -------------------------------------------------------------------------

    async def get_site(self, tenant_id: str, site_id: str) -> Site | None:
        return self.sites.get((tenant_id, site_id))

    async def list_sites(self, tenant_id: str) -> list[Site]:
        return [site for (tenant, _), site in sorted(self.sites.items()) if tenant == tenant_id]

    async def create_site(self, site: Site, receipt: IdempotencyReceipt) -> Site:
        if self._receipt_taken(receipt):
            return Site.model_validate_json(await self._replay_after_conflict(receipt))
        if (site.tenant_id, site.site_id) in self.sites:
            raise ConflictError("site already exists")
        self._store_receipt(receipt)
        self.sites[(site.tenant_id, site.site_id)] = site
        return site

    async def update_site(
        self,
        tenant_id: str,
        site_id: str,
        expected_version: int,
        *,
        name: str | None,
        status: SiteStatus | None,
    ) -> Site:
        current = self.sites.get((tenant_id, site_id))
        if current is None or current.version != expected_version:
            raise ConflictError(CONDITIONAL_CONFLICT)
        updated = Site.model_validate(
            {
                **current.model_dump(),
                **_without_none({"name": name, "status": status}),
                "updated_at": self.clock(),
                "version": expected_version + 1,
            }
        )
        self.sites[(tenant_id, site_id)] = updated
        return updated

    async def get_asset(self, tenant_id: str, asset_id: str) -> AssetWithPondProfile | None:
        return self.assets.get((tenant_id, asset_id))

    async def list_assets(self, tenant_id: str) -> list[AssetWithPondProfile]:
        return [value for (tenant, _), value in sorted(self.assets.items()) if tenant == tenant_id]

    async def create_asset(
        self, asset: Asset, profile: PondProfile, receipt: IdempotencyReceipt
    ) -> AssetWithPondProfile:
        created = AssetWithPondProfile(asset=asset, pond_profile=profile)
        if self._receipt_taken(receipt):
            snapshot = await self._replay_after_conflict(receipt)
            return AssetWithPondProfile.model_validate_json(snapshot)
        site = self.sites.get((asset.tenant_id, asset.site_id))
        if site is None or site.status is not SiteStatus.ACTIVE:
            raise ConflictError("site is not active")
        # The PondProfile row `ASSET#<id>/PROFILE#POND` is global, so an Asset id is unique
        # across tenants.
        if any(asset_id == asset.asset_id for _, asset_id in self.assets):
            raise ConflictError("asset already exists")
        self._store_receipt(receipt)
        self.assets[(asset.tenant_id, asset.asset_id)] = created
        return created

    async def update_asset(
        self,
        tenant_id: str,
        asset_id: str,
        expected_version: int,
        *,
        name: str | None,
        status: AssetStatus | None,
        pond_description: str | None,
        pond_profile_expected_version: int | None,
    ) -> AssetWithPondProfile:
        if (pond_description is None) != (pond_profile_expected_version is None):
            raise ValueError(
                "pond_description and pond_profile_expected_version must be sent together"
            )
        current = self.assets.get((tenant_id, asset_id))
        if current is None:
            raise NotFoundError("asset not found")
        if current.pond_profile.legacy_pond_id is not None:
            raise NotImplementedError("legacy Pond mirroring is only modelled by the Dynamo fake")
        asset, profile = current.asset, current.pond_profile
        now = self.clock()
        if asset.version != expected_version:
            raise ConflictError(CONDITIONAL_CONFLICT)
        if pond_description is not None and profile.version != pond_profile_expected_version:
            raise ConflictError(CONDITIONAL_CONFLICT)
        updated_asset = Asset.model_validate(
            {
                **asset.model_dump(),
                **_without_none({"name": name, "status": status}),
                "updated_at": now,
                "version": expected_version + 1,
            }
        )
        updated_profile = profile
        if pond_description is not None and pond_profile_expected_version is not None:
            updated_profile = PondProfile.model_validate(
                {
                    **profile.model_dump(),
                    "description": pond_description,
                    "updated_at": now,
                    "version": pond_profile_expected_version + 1,
                }
            )
        updated = AssetWithPondProfile(asset=updated_asset, pond_profile=updated_profile)
        self.assets[(tenant_id, asset_id)] = updated
        return updated

    # Devices, Components and Capabilities ----------------------------------------------------

    async def get_device(self, tenant_id: str, device_id: str) -> DeviceV2 | None:
        return self.devices.get((tenant_id, device_id))

    async def list_devices(self, tenant_id: str) -> list[DeviceV2]:
        return [value for (tenant, _), value in sorted(self.devices.items()) if tenant == tenant_id]

    async def create_device(self, device: DeviceV2, receipt: IdempotencyReceipt) -> DeviceV2:
        # Tenant row, global `DEVICE#<id>/META` lookup and receipt commit together; the adapter
        # does not classify which of them failed.
        if any(
            device_id == device.device_id for _, device_id in self.devices
        ) or self._receipt_taken(receipt):
            raise ConflictError(CONDITIONAL_CONFLICT)
        self._store_receipt(receipt)
        self.devices[(device.tenant_id, device.device_id)] = device
        return device

    async def update_device(
        self,
        tenant_id: str,
        device_id: str,
        expected_version: int,
        *,
        name: str | None,
        firmware_version: str | None,
        status: DeviceStatus | None,
    ) -> DeviceV2:
        current = self.devices.get((tenant_id, device_id))
        if current is None:
            raise NotFoundError("device not found")
        if current.version != expected_version:
            raise ConflictError("device version conflict")
        if status is not None:
            ensure_device_transition(current.status, status)
        updated = DeviceV2.model_validate(
            {
                **current.model_dump(),
                **_without_none(
                    {"name": name, "firmware_version": firmware_version, "status": status}
                ),
                "updated_at": max(self.clock(), current.updated_at),
                "version": expected_version + 1,
            }
        )
        self.devices[(tenant_id, device_id)] = updated
        return updated

    async def get_component(self, tenant_id: str, component_id: str) -> DeviceComponent | None:
        component = self.components.get(component_id)
        return component if component is not None and component.tenant_id == tenant_id else None

    async def list_components(self, tenant_id: str, device_id: str) -> list[DeviceComponent]:
        return [
            component
            for _, component in sorted(self.components.items())
            if component.tenant_id == tenant_id and component.device_id == device_id
        ]

    async def create_component(
        self, component: DeviceComponent, receipt: IdempotencyReceipt
    ) -> DeviceComponent:
        device = self.devices.get((component.tenant_id, component.device_id))
        if device is None:
            raise NotFoundError("device not found")
        if device.status is DeviceStatus.DECOMMISSIONED:
            raise ConflictError("device is decommissioned")
        # Tenant mirror, global `COMPONENT#<id>/META`, Device version fence and receipt commit
        # together; the adapter does not classify which of them failed.
        if component.component_id in self.components or self._receipt_taken(receipt):
            raise ConflictError(CONDITIONAL_CONFLICT)
        self._store_receipt(receipt)
        self.components[component.component_id] = component
        return component

    async def update_component(
        self,
        tenant_id: str,
        component_id: str,
        expected_version: int,
        *,
        name: str | None,
        status: ComponentStatus | None,
    ) -> DeviceComponent:
        current = await self.get_component(tenant_id, component_id)
        if current is None:
            raise NotFoundError("component not found")
        if current.version != expected_version:
            raise ConflictError("component version conflict")
        if status is not None:
            ensure_component_transition(current.status, status)
        updated = DeviceComponent.model_validate(
            {
                **current.model_dump(),
                **_without_none({"name": name, "status": status}),
                "updated_at": max(self.clock(), current.updated_at),
                "version": expected_version + 1,
            }
        )
        self.components[component_id] = updated
        return updated

    async def list_capabilities(
        self, tenant_id: str, component_id: str
    ) -> list[CapabilityDeclaration]:
        if await self.get_component(tenant_id, component_id) is None:
            return []
        return [
            value
            for (tenant, component, _), value in sorted(self.capabilities.items())
            if (tenant, component) == (tenant_id, component_id)
        ]

    async def create_capability(
        self, declaration: CapabilityDeclaration, receipt: IdempotencyReceipt
    ) -> CapabilityDeclaration:
        """The Component's ownership and status are checked before anything is written."""
        component = await self.get_component(declaration.tenant_id, declaration.component_id)
        if component is None:
            raise NotFoundError("component not found")
        if component.status in _TERMINAL_COMPONENT_STATUSES:
            raise ConflictError(f"component is {component.status}")
        key = (declaration.tenant_id, declaration.component_id, declaration.capability_id)
        if key in self.capabilities or self._receipt_taken(receipt):
            raise ConflictError(CONDITIONAL_CONFLICT)
        self._store_receipt(receipt)
        self.capabilities[key] = declaration
        return declaration

    # Integrations ----------------------------------------------------------------------------

    async def get_integration_account(
        self, tenant_id: str, integration_account_id: str
    ) -> IntegrationAccount | None:
        return self.integration_accounts.get((tenant_id, integration_account_id))

    async def list_integration_accounts(self, tenant_id: str) -> list[IntegrationAccount]:
        return [
            value
            for (tenant, _), value in sorted(self.integration_accounts.items())
            if tenant == tenant_id
        ]

    async def create_integration_account(
        self, account: IntegrationAccount, receipt: IdempotencyReceipt
    ) -> IntegrationAccount:
        if self._receipt_taken(receipt):
            raise ConflictError("idempotency key already used")
        key = (account.tenant_id, account.integration_account_id)
        if key in self.integration_accounts:
            raise ConflictError("integration account already exists")
        self._store_receipt(receipt)
        self.integration_accounts[key] = account
        return account

    async def list_device_integrations(
        self, tenant_id: str, device_id: str
    ) -> list[DeviceIntegration]:
        return [
            value
            for (tenant, _), value in sorted(self.device_integrations.items())
            if tenant == tenant_id and value.device_id == device_id
        ]

    async def create_device_integration(
        self, integration: DeviceIntegration, receipt: IdempotencyReceipt
    ) -> DeviceIntegration:
        """All-or-nothing, classified in the adapter's order: receipt, account, device, then
        the global external identity claim."""
        key = (integration.tenant_id, integration.device_integration_id)
        identity = (integration.provider_key, integration.external_identity_hash)
        account = self.integration_accounts.get(
            (integration.tenant_id, integration.integration_account_id)
        )
        device = self.devices.get((integration.tenant_id, integration.device_id))
        if self._receipt_taken(receipt):
            raise ConflictError("idempotency key already used")
        if account is None or account.status not in _PROVISIONABLE_ACCOUNT_STATUSES:
            raise ConflictError("integration account is not provisionable")
        if device is None or device.status is DeviceStatus.DECOMMISSIONED:
            raise ConflictError("device is decommissioned")
        if identity in self.external_identities:
            raise ConflictError("external identity already claimed")
        if key in self.device_integrations:
            raise ConflictError("device integration already exists")
        self._store_receipt(receipt)
        self.external_identities[identity] = integration.tenant_id
        self.device_integrations[key] = integration
        return integration

    # Deployments -----------------------------------------------------------------------------

    async def get_deployment(self, tenant_id: str, deployment_id: str) -> Deployment | None:
        for history in self.deployments.values():
            for deployment in history:
                if (deployment.tenant_id, deployment.deployment_id) == (tenant_id, deployment_id):
                    return deployment
        return None

    async def list_component_deployments(
        self, tenant_id: str, component_id: str
    ) -> list[Deployment]:
        return [
            deployment
            for deployment in self.deployments.get(component_id, [])
            if deployment.tenant_id == tenant_id
        ]

    async def get_current_deployment(self, tenant_id: str, component_id: str) -> Deployment | None:
        history = await self.list_component_deployments(tenant_id, component_id)
        return next((d for d in history if d.status is DeploymentStatus.ACTIVE), None)

    async def get_deployment_at(
        self, tenant_id: str, component_id: str, observed_at: datetime
    ) -> Deployment | None:
        for deployment in await self.list_component_deployments(tenant_id, component_id):
            if deployment.started_at <= observed_at and (
                deployment.ended_at is None or observed_at < deployment.ended_at
            ):
                return deployment
        return None

    async def start_deployment(
        self, deployment: Deployment, receipt: IdempotencyReceipt
    ) -> Deployment:
        if deployment.status is not DeploymentStatus.ACTIVE:
            raise ValueError("only an active deployment can be started")
        history = self.deployments.get(deployment.component_id, [])
        # Pre-checks the adapter makes from the marker and the latest history row.
        ended = [item.ended_at for item in history if item.ended_at is not None]
        if ended and deployment.started_at < max(ended):
            raise ConflictError("deployment overlaps the previous interval")
        if history:
            latest = max(history, key=lambda item: item.started_at)
            if latest.ended_at is None:
                raise ConflictError("component already has an active deployment")
            if deployment.started_at < latest.ended_at:
                raise ConflictError("deployment overlaps the previous interval")
        # The transaction, classified in the adapter's order.
        component = self.components.get(deployment.component_id)
        asset = self.assets.get((deployment.tenant_id, deployment.asset_id))
        site = self.sites.get((deployment.tenant_id, deployment.site_id))
        if self._receipt_taken(receipt):
            raise ConflictError(DEPLOYMENT_IDEMPOTENCY_CONFLICT)
        if (
            component is None
            or component.tenant_id != deployment.tenant_id
            or component.status is not ComponentStatus.ACTIVE
        ):
            raise ConflictError("component is not active")
        if asset is None or asset.asset.status is not AssetStatus.ACTIVE:
            raise ConflictError("asset is not active")
        if site is None or site.status is not SiteStatus.ACTIVE:
            raise ConflictError("site is not active")
        if await self.get_deployment(deployment.tenant_id, deployment.deployment_id) is not None:
            raise ConflictError("deployment already exists")
        self._store_receipt(receipt)
        self.deployments.setdefault(deployment.component_id, []).append(deployment)
        self.deployments[deployment.component_id].sort(key=lambda item: item.started_at)
        return deployment

    async def end_deployment(
        self,
        tenant_id: str,
        deployment_id: str,
        *,
        expected_version: int,
        ended_at: datetime,
    ) -> Deployment:
        current = await self.get_deployment(tenant_id, deployment_id)
        if current is None:
            raise NotFoundError("deployment not found")
        if current.status is not DeploymentStatus.ACTIVE:
            raise ConflictError("deployment is not active")
        if current.version != expected_version:
            raise ConflictError("deployment version conflict")
        ended = Deployment.model_validate(
            {
                **current.model_dump(),
                "ended_at": ended_at,
                "status": DeploymentStatus.ENDED,
                "updated_at": max(self.clock(), current.updated_at),
                "version": expected_version + 1,
            }
        )
        history = self.deployments[current.component_id]
        history[history.index(current)] = ended
        return ended


# Synchronous scenario facade over the real M1 services --------------------------------------------


class M1AcceptanceScenario:
    """Drives the real M1 services (Sites, Assets, Devices, Components, Deployments) over any
    implementation of the frozen protocols. Every call is synchronous; each create uses a fresh
    `Idempotency-Key`."""

    def __init__(
        self,
        *,
        site_assets: Any,
        device_components: Any,
        deployments: Any,
        clock: Callable[[], datetime],
    ) -> None:
        self.sites = SiteService(site_assets, clock=clock)
        self.assets = AssetService(site_assets, clock=clock)
        self.devices = DeviceV2Service(device_components, clock=clock)
        self.components = ComponentService(device_components, clock=clock)
        self.deployments = DeploymentService(
            deployments, device_components, site_assets, clock=clock
        )
        self._device_components = device_components
        self._tenants_by_device: dict[str, str] = {}
        self._tenants_by_component: dict[str, str] = {}
        self._keys = 0

    @classmethod
    def in_memory(cls, *, clock: Callable[[], datetime]) -> "M1AcceptanceScenario":
        repositories = InMemoryM1Repositories(clock=clock)
        return cls(
            site_assets=repositories,
            device_components=repositories,
            deployments=repositories,
            clock=clock,
        )

    @classmethod
    def over_dynamo(cls, client: Any, *, clock: Callable[[], datetime]) -> "M1AcceptanceScenario":
        repositories = DynamoM1Repositories.over(client, clock=clock)
        return cls(
            site_assets=repositories.site_asset_repository,
            device_components=repositories.device_component_repository,
            deployments=repositories.deployment_repository,
            clock=clock,
        )

    def _key(self) -> str:
        self._keys += 1
        return f"m1-acceptance-{self._keys:06d}"

    def create_site(self, tenant_id: str, name: str) -> Site:
        return asyncio.run(
            self.sites.create(tenant_id=tenant_id, name=name, idempotency_key=self._key())
        )

    def create_asset(self, tenant_id: str, site_id: str, name: str) -> AssetWithPondProfile:
        return asyncio.run(
            self.assets.create(
                tenant_id=tenant_id,
                site_id=site_id,
                asset_type=AssetType.POND,
                name=name,
                pond_description=None,
                idempotency_key=self._key(),
            )
        )

    def create_device(self, tenant_id: str, name: str) -> DeviceV2:
        outcome = asyncio.run(
            self.devices.create(tenant_id, DeviceV2CreateCommand(name=name), self._key())
        )
        self._tenants_by_device[outcome.entity.device_id] = tenant_id
        return outcome.entity

    def create_probe(self, tenant_id: str, device_id: str, name: str) -> DeviceComponent:
        command = ComponentCreateCommand(
            kind=ComponentKind.PROBE, name=name, probe_profile=ProbeProfileCommand()
        )
        outcome = asyncio.run(self.components.create(tenant_id, device_id, command, self._key()))
        self._tenants_by_component[outcome.entity.component_id] = tenant_id
        return outcome.entity

    def list_components(self, device_id: str) -> list[DeviceComponent]:
        return asyncio.run(self.components.list(self._tenants_by_device[device_id], device_id))

    def start(self, component_id: str, asset_id: str, started_at: datetime) -> Deployment:
        command = DeploymentStartCommand(
            component_id=component_id, asset_id=asset_id, started_at=started_at
        )
        tenant_id = self._tenants_by_component[component_id]
        return asyncio.run(self.deployments.start(tenant_id, command, self._key())).entity

    def end(self, component_id: str, ended_at: datetime) -> Deployment:
        """End the current interval of `component_id` at `ended_at`."""
        tenant_id = self._tenants_by_component[component_id]
        current = asyncio.run(self.deployments.get_current(tenant_id, component_id))
        assert current is not None, "end requires a current deployment"
        command = DeploymentEndCommand(expected_version=current.version, ended_at=ended_at)
        return asyncio.run(self.deployments.end(tenant_id, current.deployment_id, command))

    def move(self, component_id: str, asset_id: str, transition_at: datetime) -> Deployment:
        """End the current interval at `transition_at`, then start the next one there."""
        self.end(component_id, transition_at)
        return self.start(component_id, asset_id, transition_at)

    def resolve(self, component_id: str, observed_at: datetime) -> Deployment | None:
        """The Deployment whose `[started_at, ended_at)` contains `observed_at`."""
        tenant_id = self._tenants_by_component[component_id]
        return asyncio.run(self.deployments.get_at(tenant_id, component_id, observed_at))

    def history(self, component_id: str) -> list[Deployment]:
        tenant_id = self._tenants_by_component[component_id]
        return asyncio.run(self.deployments.list(tenant_id, component_id))
