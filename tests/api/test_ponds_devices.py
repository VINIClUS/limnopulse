import re
from datetime import UTC, datetime
from decimal import Decimal

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from fastapi.testclient import TestClient

from limnopulse_api.adapters.dynamodb_deployments import DynamoDeploymentRepository
from limnopulse_api.adapters.dynamodb_devices_v2 import DynamoDeviceComponentRepository
from limnopulse_api.adapters.dynamodb_integrations import DynamoIntegrationRepository
from limnopulse_api.adapters.dynamodb_sites_assets import DynamoSiteAssetRepository
from limnopulse_api.core.config import Settings
from limnopulse_api.core.errors import ConflictError
from limnopulse_api.domain.entities import Device, Membership, Pond, Tenant
from limnopulse_api.domain.ids import (
    asset_id_for_legacy_pond,
    default_component_id_for_legacy_device,
)
from limnopulse_api.domain.roles import TenantRole
from limnopulse_api.main import create_app


class FakeMembershipService:
    def __init__(self, membership: Membership | None) -> None:
        self.membership = membership

    async def get_active_membership(
        self,
        cognito_sub: str,
        tenant_id: str,
    ) -> Membership | None:
        if self.membership is None:
            return None
        if self.membership.cognito_sub != cognito_sub or self.membership.tenant_id != tenant_id:
            return None
        return self.membership


class FakeDomainRepository:
    def __init__(self) -> None:
        self.tenants: dict[str, Tenant] = {"tnt_1": make_tenant(name="Demo")}
        self.ponds: dict[str, Pond] = {"pond_1": make_pond(name="West")}
        self.devices: dict[str, Device] = {"dev_1": make_device(name="Probe")}

    async def get_membership(self, cognito_sub: str, tenant_id: str) -> Membership | None:
        return None

    async def list_memberships_for_user(self, cognito_sub: str) -> list[Membership]:
        return []

    async def create_tenant_with_owner(self, tenant_id: str, name: str, owner_sub: str) -> Tenant:
        tenant = make_tenant(tenant_id=tenant_id, name=name)
        self.tenants[tenant_id] = tenant
        return tenant

    async def list_tenants_for_memberships(self, memberships: list[Membership]) -> list[Tenant]:
        return [self.tenants[membership.tenant_id] for membership in memberships if membership.tenant_id in self.tenants]

    async def get_tenant(self, tenant_id: str) -> Tenant | None:
        return self.tenants.get(tenant_id)

    async def update_tenant(
        self,
        tenant_id: str,
        expected_version: int,
        name: str | None,
    ) -> Tenant:
        current = self.tenants[tenant_id]
        tenant = current.model_copy(
            update={"name": name or current.name, "version": expected_version + 1, "updated_at": datetime.now(UTC)}
        )
        self.tenants[tenant_id] = tenant
        return tenant

    async def list_ponds(self, tenant_id: str) -> list[Pond]:
        return [pond for pond in self.ponds.values() if pond.tenant_id == tenant_id]

    async def get_pond(self, tenant_id: str, pond_id: str) -> Pond | None:
        pond = self.ponds.get(pond_id)
        if pond is None or pond.tenant_id != tenant_id:
            return None
        return pond

    async def create_pond(
        self,
        tenant_id: str,
        pond_id: str,
        name: str,
        description: str | None,
    ) -> Pond:
        pond = make_pond(tenant_id=tenant_id, pond_id=pond_id, name=name, description=description)
        self.ponds[pond_id] = pond
        return pond

    async def update_pond(
        self,
        tenant_id: str,
        pond_id: str,
        expected_version: int,
        name: str | None,
        description: str | None,
    ) -> Pond:
        current = self.ponds[pond_id]
        pond = current.model_copy(
            update={
                "tenant_id": tenant_id,
                "name": name or current.name,
                "description": description if description is not None else current.description,
                "version": expected_version + 1,
                "updated_at": datetime.now(UTC),
            }
        )
        self.ponds[pond_id] = pond
        return pond

    async def list_devices(self, tenant_id: str) -> list[Device]:
        return [device for device in self.devices.values() if device.tenant_id == tenant_id]

    async def get_device(self, tenant_id: str, device_id: str) -> Device | None:
        device = self.devices.get(device_id)
        if device is None or device.tenant_id != tenant_id:
            return None
        return device

    async def create_device(
        self,
        tenant_id: str,
        pond_id: str,
        device_id: str,
        name: str,
        firmware_version: str | None,
    ) -> Device:
        device = make_device(
            tenant_id=tenant_id,
            pond_id=pond_id,
            device_id=device_id,
            name=name,
            firmware_version=firmware_version,
        )
        self.devices[device_id] = device
        return device

    async def update_device(
        self,
        tenant_id: str,
        device_id: str,
        expected_version: int,
        name: str | None,
        pond_id: str | None,
        firmware_version: str | None,
    ) -> Device:
        current = self.devices[device_id]
        device = current.model_copy(
            update={
                "tenant_id": tenant_id,
                "pond_id": pond_id or current.pond_id,
                "name": name or current.name,
                "firmware_version": firmware_version if firmware_version is not None else current.firmware_version,
                "version": expected_version + 1,
                "updated_at": datetime.now(UTC),
            }
        )
        self.devices[device_id] = device
        return device


def make_tenant(tenant_id: str = "tnt_1", name: str = "Tenant") -> Tenant:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return Tenant(
        tenant_id=tenant_id,
        name=name,
        created_at=now,
        updated_at=now,
        version=1,
    )


def make_pond(
    tenant_id: str = "tnt_1",
    pond_id: str = "pond_1",
    name: str = "Pond",
    description: str | None = None,
) -> Pond:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return Pond(
        tenant_id=tenant_id,
        pond_id=pond_id,
        name=name,
        description=description,
        created_at=now,
        updated_at=now,
        version=1,
    )


def make_device(
    tenant_id: str = "tnt_1",
    pond_id: str = "pond_1",
    device_id: str = "dev_1",
    name: str = "Device",
    firmware_version: str | None = None,
) -> Device:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return Device(
        tenant_id=tenant_id,
        pond_id=pond_id,
        device_id=device_id,
        name=name,
        firmware_version=firmware_version,
        created_at=now,
        updated_at=now,
        version=1,
    )


def make_membership(role: TenantRole) -> Membership:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return Membership(
        tenant_id="tnt_1",
        cognito_sub="sub_1",
        role=role,
        created_at=now,
        updated_at=now,
        version=1,
    )


def app_with_membership(role: TenantRole):
    app = create_app(Settings(app_env="test", auth_mode="dev"))
    app.state.domain_repository = FakeDomainRepository()
    app.state.membership_service = FakeMembershipService(membership=make_membership(role))
    return app


def app_without_membership():
    app = create_app(Settings(app_env="test", auth_mode="dev"))
    app.state.domain_repository = FakeDomainRepository()
    app.state.membership_service = FakeMembershipService(membership=None)
    return app


def test_viewer_can_list_ponds_but_cannot_create() -> None:
    app = app_with_membership(role=TenantRole.VIEWER)
    client = TestClient(app)
    headers = {"X-Dev-User-Sub": "sub_1"}

    assert client.get("/v1/tenants/tnt_1/ponds", headers=headers).status_code == 200
    assert client.get("/v1/tenants/tnt_1/ponds/pond_1", headers=headers).status_code == 200
    assert client.post("/v1/tenants/tnt_1/ponds", json={"name": "North"}, headers=headers).status_code == 403


def test_member_can_list_devices_but_cannot_patch() -> None:
    app = app_with_membership(role=TenantRole.MEMBER)
    client = TestClient(app)
    headers = {"X-Dev-User-Sub": "sub_1"}

    assert client.get("/v1/tenants/tnt_1/devices", headers=headers).status_code == 200
    assert client.get("/v1/tenants/tnt_1/devices/dev_1", headers=headers).status_code == 200
    assert (
        client.patch(
            "/v1/tenants/tnt_1/devices/dev_1",
            json={"expected_version": 1, "name": "Probe"},
            headers=headers,
        ).status_code
        == 403
    )


def test_admin_can_create_and_patch_pond() -> None:
    app = app_with_membership(role=TenantRole.ADMIN)
    client = TestClient(app)
    headers = {"X-Dev-User-Sub": "sub_1"}

    assert client.post("/v1/tenants/tnt_1/ponds", json={"name": "North"}, headers=headers).status_code == 201
    assert (
        client.patch(
            "/v1/tenants/tnt_1/ponds/pond_1",
            json={"expected_version": 1, "name": "South"},
            headers=headers,
        ).status_code
        == 200
    )


def test_owner_can_create_and_patch_device() -> None:
    app = app_with_membership(role=TenantRole.OWNER)
    client = TestClient(app)
    headers = {"X-Dev-User-Sub": "sub_1"}

    assert (
        client.post(
            "/v1/tenants/tnt_1/devices",
            json={"pond_id": "pond_1", "name": "Probe"},
            headers=headers,
        ).status_code
        == 201
    )
    assert (
        client.patch(
            "/v1/tenants/tnt_1/devices/dev_1",
            json={"expected_version": 1, "name": "Probe 2"},
            headers=headers,
        ).status_code
        == 200
    )


def test_user_without_membership_gets_403() -> None:
    app = app_without_membership()
    client = TestClient(app)
    headers = {"X-Dev-User-Sub": "sub_1"}

    assert client.get("/v1/tenants/tnt_1/ponds", headers=headers).status_code == 403
    assert client.get("/v1/tenants/tnt_1/ponds/pond_1", headers=headers).status_code == 403
    assert client.get("/v1/tenants/tnt_1/devices/dev_1", headers=headers).status_code == 403


def test_get_pond_returns_404_when_missing() -> None:
    app = app_with_membership(role=TenantRole.VIEWER)
    client = TestClient(app)

    response = client.get("/v1/tenants/tnt_1/ponds/pond_missing", headers={"X-Dev-User-Sub": "sub_1"})

    assert response.status_code == 404


def test_get_device_returns_404_when_missing() -> None:
    app = app_with_membership(role=TenantRole.MEMBER)
    client = TestClient(app)

    response = client.get("/v1/tenants/tnt_1/devices/dev_missing", headers={"X-Dev-User-Sub": "sub_1"})

    assert response.status_code == 404


# Exact /v1 response contract (flag off: legacy domain repository) -------------------------

POND_1_JSON = {
    "tenant_id": "tnt_1",
    "pond_id": "pond_1",
    "name": "West",
    "description": None,
    "created_at": "2026-01-01T00:00:00+00:00",
    "updated_at": "2026-01-01T00:00:00+00:00",
    "version": 1,
    "status": "active",
}
DEVICE_1_JSON = {
    "tenant_id": "tnt_1",
    "pond_id": "pond_1",
    "device_id": "dev_1",
    "name": "Probe",
    "auth_type": "mtls",
    "firmware_version": None,
    "created_at": "2026-01-01T00:00:00+00:00",
    "updated_at": "2026-01-01T00:00:00+00:00",
    "version": 1,
    "status": "active",
}
HEADERS = {"X-Dev-User-Sub": "sub_1"}
SECOND_PRECISION_UTC = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+00:00$")


def test_legacy_list_and_get_responses_are_exact() -> None:
    client = TestClient(app_with_membership(role=TenantRole.VIEWER))

    assert client.get("/v1/tenants/tnt_1/ponds", headers=HEADERS).json() == {
        "items": [POND_1_JSON]
    }
    assert client.get("/v1/tenants/tnt_1/ponds/pond_1", headers=HEADERS).json() == POND_1_JSON
    assert client.get("/v1/tenants/tnt_1/devices", headers=HEADERS).json() == {
        "items": [DEVICE_1_JSON]
    }
    assert client.get("/v1/tenants/tnt_1/devices/dev_1", headers=HEADERS).json() == DEVICE_1_JSON


def test_legacy_conflicts_map_to_409_and_missing_state_to_503() -> None:
    app = app_with_membership(role=TenantRole.OWNER)

    async def conflict(**kwargs):
        raise ConflictError("pond version conflict")

    app.state.domain_repository.update_pond = conflict
    client = TestClient(app)
    response = client.patch(
        "/v1/tenants/tnt_1/ponds/pond_1", json={"expected_version": 1}, headers=HEADERS
    )
    assert (response.status_code, response.json()) == (409, {"detail": "pond version conflict"})

    del app.state.domain_repository
    assert client.get("/v1/tenants/tnt_1/ponds", headers=HEADERS).status_code == 503
    assert client.get("/v1/tenants/tnt_1/devices", headers=HEADERS).status_code == 503


def test_flag_off_keeps_accepting_a_dangling_pond_id() -> None:
    client = TestClient(app_with_membership(role=TenantRole.OWNER))

    created = client.post(
        "/v1/tenants/tnt_1/devices",
        json={"pond_id": "pond_missing", "name": "Probe"},
        headers=HEADERS,
    )
    patched = client.patch(
        "/v1/tenants/tnt_1/devices/dev_1",
        json={"expected_version": 1, "pond_id": "pond_missing"},
        headers=HEADERS,
    )

    assert created.status_code == 201
    assert created.json()["pond_id"] == "pond_missing"
    assert (patched.status_code, patched.json()["pond_id"]) == (200, "pond_missing")


# Flag on: the v2 projection serves /v1 -----------------------------------------------------


class ProjectionSettings(Settings):
    v1_pond_device_projection_enabled: bool = True


class InMemoryDynamoClient:
    """Applies writes without evaluating conditions; conflict semantics are unit-tested."""

    def __init__(self) -> None:
        self.serializer = TypeSerializer()
        self.deserializer = TypeDeserializer()
        self.items: dict[tuple[str, str], dict] = {}
        self.transactions: list[list[dict]] = []

    def seed(self, item: dict) -> None:
        self.items[(item["PK"], item["SK"])] = dict(item)

    def get_item(self, **kwargs):
        key = self._plain(kwargs["Key"])
        item = self.items.get((key["PK"], key["SK"]))
        return {} if item is None else {"Item": self._encode(item)}

    def query(self, **kwargs):
        values = self._plain(kwargs["ExpressionAttributeValues"])
        prefix = values.get(":sk_prefix")
        lower = values.get(":sk_lower", "")
        upper = values.get(":sk_upper", "￿")
        rows = sorted(
            (sk, item)
            for (pk, sk), item in self.items.items()
            if pk == values[":pk"]
            and (sk.startswith(prefix) if prefix is not None else lower <= sk <= upper)
        )
        if not kwargs.get("ScanIndexForward", True):
            rows.reverse()
        if "Limit" in kwargs:
            rows = rows[: kwargs["Limit"]]
        return {"Items": [self._encode(item) for _, item in rows]}

    def transact_write_items(self, **kwargs):
        self.transactions.append(kwargs["TransactItems"])
        for action in kwargs["TransactItems"]:
            ((kind, request),) = action.items()
            if kind == "Put":
                item = self._plain(request["Item"])
                self.items[(item["PK"], item["SK"])] = item
                continue
            key = self._plain(request["Key"])
            identity = (key["PK"], key["SK"])
            if kind == "Delete":
                self.items.pop(identity, None)
            elif kind == "Update":
                updated = dict(self.items.get(identity, key))
                names = request.get("ExpressionAttributeNames", {})
                update_values = self._plain(request["ExpressionAttributeValues"])
                for clause in request["UpdateExpression"].removeprefix("SET ").split(", "):
                    name, value = clause.split(" = ")
                    updated[names.get(name, name)] = update_values[value]
                self.items[identity] = updated
        return {}

    def _plain(self, mapping: dict) -> dict:
        plain = {key: self.deserializer.deserialize(value) for key, value in mapping.items()}
        return {
            key: int(value) if isinstance(value, Decimal) else value
            for key, value in plain.items()
        }

    def _encode(self, item: dict) -> dict:
        return {key: self.serializer.serialize(value) for key, value in item.items()}


V2_STATE = (
    "site_asset_repository",
    "device_component_repository",
    "integration_repository",
    "deployment_repository",
)


def projection_app(role: TenantRole = TenantRole.OWNER, *, missing: str | None = None):
    app = create_app(ProjectionSettings(app_env="test", auth_mode="dev"))
    dynamo = InMemoryDynamoClient()
    dynamo.seed(
        {
            "PK": "TENANT#tnt_1",
            "SK": "META",
            "entity_type": "tenant",
            "tenant_id": "tnt_1",
            "name": "Demo",
            "settings": {},
            "status": "active",
            "created_at": "2026-01-01T00:00:00+00:00",
            "updated_at": "2026-01-01T00:00:00+00:00",
            "version": 1,
            "schema_version": 1,
        }
    )
    # The legacy repository must not serve /v1 Pond/Device routes while the flag is on.
    app.state.domain_repository = None
    app.state.membership_service = FakeMembershipService(membership=make_membership(role))
    app.state.site_asset_repository = DynamoSiteAssetRepository("LimnopulseDomain", dynamo)
    app.state.device_component_repository = DynamoDeviceComponentRepository(
        "LimnopulseDomain", dynamo
    )
    app.state.integration_repository = DynamoIntegrationRepository("LimnopulseDomain", dynamo)
    app.state.deployment_repository = DynamoDeploymentRepository("LimnopulseDomain", dynamo)
    if missing is not None:
        delattr(app.state, missing)
    return app, dynamo


def test_flag_on_missing_v2_state_returns_503_without_falling_back() -> None:
    for missing in V2_STATE:
        app, _ = projection_app(missing=missing)
        app.state.domain_repository = FakeDomainRepository()
        client = TestClient(app)
        for path in ("/v1/tenants/tnt_1/ponds", "/v1/tenants/tnt_1/devices/dev_1"):
            response = client.get(path, headers=HEADERS)
            assert (response.status_code, response.json()) == (
                503,
                {"detail": "service unavailable"},
            ), missing


def test_flag_on_preserves_roles_and_membership_gates() -> None:
    app, dynamo = projection_app(role=TenantRole.VIEWER)
    client = TestClient(app)

    assert client.get("/v1/tenants/tnt_1/ponds", headers=HEADERS).json() == {"items": []}
    created = client.post("/v1/tenants/tnt_1/ponds", json={"name": "N"}, headers=HEADERS)
    assert created.status_code == 403
    assert client.get("/v1/tenants/tnt_2/ponds", headers=HEADERS).status_code == 403
    assert client.get("/v1/tenants/tnt_1/ponds/pond_x", headers=HEADERS).status_code == 404
    assert client.get("/v1/tenants/tnt_1/devices/dev_x", headers=HEADERS).status_code == 404
    assert dynamo.transactions == []


def test_flag_on_pond_and_device_lifecycle_keeps_the_v1_contract() -> None:
    app, dynamo = projection_app()
    client = TestClient(app)

    created = client.post(
        "/v1/tenants/tnt_1/ponds", json={"name": "West", "description": "Shallow"}, headers=HEADERS
    )
    assert created.status_code == 201
    west = created.json()
    assert set(west) == set(POND_1_JSON)
    assert re.fullmatch(r"pond_[0-9a-f]{32}", west["pond_id"])
    assert (west["name"], west["description"], west["version"], west["status"]) == (
        "West",
        "Shallow",
        1,
        "active",
    )
    assert SECOND_PRECISION_UTC.match(west["created_at"])
    assert west["created_at"] == west["updated_at"]
    pond_path = f"/v1/tenants/tnt_1/ponds/{west['pond_id']}"
    assert client.get(pond_path, headers=HEADERS).json() == west
    assert client.get("/v1/tenants/tnt_1/ponds", headers=HEADERS).json() == {"items": [west]}

    patched = client.patch(
        pond_path, json={"expected_version": 1, "name": "North"}, headers=HEADERS
    )
    assert patched.status_code == 200
    assert patched.json() == {
        **west,
        "name": "North",
        "version": 2,
        "updated_at": patched.json()["updated_at"],
    }
    assert SECOND_PRECISION_UTC.match(patched.json()["updated_at"])
    asset_id = asset_id_for_legacy_pond("tnt_1", west["pond_id"])
    asset = dynamo.items[("TENANT#tnt_1", f"ASSET#{asset_id}")]
    assert asset["name"] == "North"

    east = client.post("/v1/tenants/tnt_1/ponds", json={"name": "East"}, headers=HEADERS).json()
    device = client.post(
        "/v1/tenants/tnt_1/devices",
        json={"pond_id": west["pond_id"], "name": "Gateway", "firmware_version": "1.0.0"},
        headers=HEADERS,
    )
    assert device.status_code == 201
    body = device.json()
    assert set(body) == set(DEVICE_1_JSON)
    assert re.fullmatch(r"dev_[0-9a-f]{32}", body["device_id"])
    assert (body["pond_id"], body["auth_type"], body["version"], body["status"]) == (
        west["pond_id"],
        "mtls",
        1,
        "active",
    )
    assert SECOND_PRECISION_UTC.match(body["created_at"])
    device_path = f"/v1/tenants/tnt_1/devices/{body['device_id']}"
    assert client.get(device_path, headers=HEADERS).json() == body
    assert client.get("/v1/tenants/tnt_1/devices", headers=HEADERS).json() == {"items": [body]}

    component_id = default_component_id_for_legacy_device("tnt_1", body["device_id"])
    current = dynamo.items[(f"COMPONENT#{component_id}", "DEPLOYMENT#CURRENT")]
    assert current["asset_id"] == asset_id_for_legacy_pond("tnt_1", west["pond_id"])

    moved = client.patch(
        device_path, json={"expected_version": 1, "pond_id": east["pond_id"]}, headers=HEADERS
    )
    assert moved.status_code == 200
    assert moved.json() == {
        **body,
        "pond_id": east["pond_id"],
        "version": 2,
        "updated_at": moved.json()["updated_at"],
    }
    history = [
        sk
        for pk, sk in dynamo.items
        if pk == f"COMPONENT#{component_id}" and sk.startswith("DEPLOYMENT#2")
    ]
    assert len(history) == 2
    current = dynamo.items[(f"COMPONENT#{component_id}", "DEPLOYMENT#CURRENT")]
    assert current["asset_id"] == asset_id_for_legacy_pond("tnt_1", east["pond_id"])

    empty = client.patch(device_path, json={"expected_version": 2}, headers=HEADERS)
    assert (empty.status_code, empty.json()["version"]) == (200, 3)


def test_flag_on_dangling_pond_id_is_409() -> None:
    app, dynamo = projection_app()
    client = TestClient(app)

    created = client.post(
        "/v1/tenants/tnt_1/devices",
        json={"pond_id": "pond_missing", "name": "Probe"},
        headers=HEADERS,
    )

    assert (created.status_code, created.json()) == (409, {"detail": "pond not found"})
    assert dynamo.transactions == []


def test_flag_on_native_v2_device_is_hidden_from_v1() -> None:
    app, dynamo = projection_app()
    native = "dev_" + "9" * 32
    dynamo.seed(
        {
            "PK": "TENANT#tnt_1",
            "SK": f"DEVICE#{native}",
            "entity_type": "device",
            "tenant_id": "tnt_1",
            "device_id": native,
            "name": "Native",
            "compatibility_level": "custom",
            "status": "active",
            "created_at": "2026-08-25T10:00:00.000000Z",
            "updated_at": "2026-08-25T10:00:00.000000Z",
            "version": 1,
            "schema_version": 2,
        }
    )
    client = TestClient(app)

    assert client.get("/v1/tenants/tnt_1/devices", headers=HEADERS).json() == {"items": []}
    assert client.get(f"/v1/tenants/tnt_1/devices/{native}", headers=HEADERS).status_code == 404
    patched = client.patch(
        f"/v1/tenants/tnt_1/devices/{native}", json={"expected_version": 1}, headers=HEADERS
    )
    assert patched.status_code == 404
