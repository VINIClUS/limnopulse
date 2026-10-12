import json
import re
from datetime import UTC, datetime

import pytest
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI
from fastapi.testclient import TestClient

from limnopulse_api.adapters.dynamodb_devices_v2 import DynamoDeviceComponentRepository
from limnopulse_api.api.v2.routers.components import router as components_router
from limnopulse_api.api.v2.routers.devices import router as devices_router
from limnopulse_api.auth.dev import DevAuthProvider
from limnopulse_api.domain.entities import Membership
from limnopulse_api.domain.ids import is_canonical_id
from limnopulse_api.domain.roles import TenantRole
from limnopulse_api.main import create_app

NOW = datetime(2026, 8, 25, 12, 0, 0, 500000, tzinfo=UTC)
TENANT = "tnt_a"
OTHER_TENANT = "tnt_b"
SUB = "sub_1"
HEADERS = {"X-Dev-User-Sub": SUB}


class FakeDynamoClient:
    """In-memory table evaluating only the condition shapes this adapter emits."""

    def __init__(self, page_size: int = 2) -> None:
        self.serializer = TypeSerializer()
        self.deserializer = TypeDeserializer()
        self.items: dict[tuple[str, str], dict] = {}
        self.calls: list[tuple[str, dict]] = []
        self.page_size = page_size
        self.fail_with: Exception | None = None

    def seed(self, item: dict) -> None:
        self.items[(item["PK"], item["SK"])] = {
            key: self.serializer.serialize(value) for key, value in item.items()
        }

    def plain(self, pk: str, sk: str) -> dict | None:
        item = self.items.get((pk, sk))
        if item is None:
            return None
        return {key: self.deserializer.deserialize(value) for key, value in item.items()}

    def get_item(self, **kwargs):
        self.calls.append(("get_item", kwargs))
        key = self._key(kwargs["Key"])
        item = self.items.get(key)
        return {} if item is None else {"Item": item}

    def query(self, **kwargs):
        self.calls.append(("query", kwargs))
        values = {
            name: self.deserializer.deserialize(value)
            for name, value in kwargs["ExpressionAttributeValues"].items()
        }
        assert kwargs["KeyConditionExpression"] == "PK = :pk AND begins_with(SK, :sk_prefix)"
        matches = sorted(
            (key, item)
            for key, item in self.items.items()
            if key[0] == values[":pk"] and key[1].startswith(values[":sk_prefix"])
        )
        start = kwargs.get("ExclusiveStartKey")
        if start is not None:
            start_key = self._key(start)
            matches = [match for match in matches if match[0] > start_key]
        page = matches[: self.page_size]
        response: dict = {"Items": [item for _, item in page]}
        if len(matches) > self.page_size:
            last = page[-1][0]
            response["LastEvaluatedKey"] = {"PK": {"S": last[0]}, "SK": {"S": last[1]}}
        return response

    def transact_write_items(self, **kwargs):
        self.calls.append(("transact_write_items", kwargs))
        if self.fail_with is not None:
            error, self.fail_with = self.fail_with, None
            raise error
        actions = kwargs["TransactItems"]
        reasons = []
        for action in actions:
            ((kind, request),) = action.items()
            key = self._key(request["Item"] if kind == "Put" else request["Key"])
            ok = self._condition(request, self.items.get(key))
            reasons.append({"Code": "None" if ok else "ConditionalCheckFailed"})
        if any(reason["Code"] != "None" for reason in reasons):
            raise ClientError(
                {
                    "Error": {"Code": "TransactionCanceledException", "Message": "cancelled"},
                    "CancellationReasons": reasons,
                },
                "TransactWriteItems",
            )
        for action in actions:
            ((kind, request),) = action.items()
            if kind == "Put":
                self.items[self._key(request["Item"])] = dict(request["Item"])
            elif kind == "Update":
                self._apply_update(request)
        return {}

    def __getattr__(self, name: str):
        raise AssertionError(f"unexpected client call: {name}")

    def _key(self, item: dict) -> tuple[str, str]:
        return (
            self.deserializer.deserialize(item["PK"]),
            self.deserializer.deserialize(item["SK"]),
        )

    def _condition(self, request: dict, existing: dict | None) -> bool:
        condition = request.get("ConditionExpression")
        if condition is None:
            return True
        names = request.get("ExpressionAttributeNames", {})
        values = {
            name: self.deserializer.deserialize(value)
            for name, value in request.get("ExpressionAttributeValues", {}).items()
        }
        current = (
            {}
            if existing is None
            else {key: self.deserializer.deserialize(value) for key, value in existing.items()}
        )

        def clause(text: str) -> bool:
            text = text.strip()
            if match := re.fullmatch(r"attribute_exists\((\S+)\)", text):
                return names.get(match[1], match[1]) in current
            if match := re.fullmatch(r"attribute_not_exists\((\S+)\)", text):
                return names.get(match[1], match[1]) not in current
            if match := re.fullmatch(r"(\S+) (=|<=|<>) (\S+)", text):
                attribute = names.get(match[1], match[1])
                if attribute not in current:
                    return False
                left, right = current[attribute], values[match[3]]
                return {"=": left == right, "<=": left <= right, "<>": left != right}[match[2]]
            raise AssertionError(f"unsupported condition clause: {text}")

        if " OR " in condition:
            return any(clause(part) for part in condition.split(" OR "))
        return all(clause(part) for part in condition.split(" AND "))

    def _apply_update(self, request: dict) -> None:
        key = self._key(request["Key"])
        expression = request["UpdateExpression"]
        assert expression.startswith("SET ")
        names = request["ExpressionAttributeNames"]
        values = request["ExpressionAttributeValues"]
        item = self.items[key]
        for assignment in expression[4:].split(", "):
            name, value = assignment.split(" = ")
            item[names[name]] = values[value]


class FakeMembershipService:
    def __init__(self, role: TenantRole | None) -> None:
        self.role = role

    async def get_active_membership(self, cognito_sub: str, tenant_id: str):
        if self.role is None or cognito_sub != SUB or tenant_id != TENANT:
            return None
        return Membership(
            tenant_id=tenant_id,
            cognito_sub=cognito_sub,
            role=self.role,
            created_at=NOW,
            updated_at=NOW,
            version=1,
        )


async def _infrastructure_error(request, exc):
    from fastapi.responses import JSONResponse

    return JSONResponse(status_code=503, content={"detail": "service unavailable"})


def app_with_device_component_repository(
    role: TenantRole | None = TenantRole.OWNER,
    client: FakeDynamoClient | None = None,
    *,
    wire_repository: bool = True,
) -> FastAPI:
    app = FastAPI()
    app.include_router(devices_router)
    app.include_router(components_router)
    app.add_exception_handler(ClientError, _infrastructure_error)
    app.add_exception_handler(BotoCoreError, _infrastructure_error)
    app.state.auth_provider = DevAuthProvider()
    app.state.membership_service = FakeMembershipService(role)
    app.state.fake_dynamo = client or FakeDynamoClient()
    if wire_repository:
        app.state.device_component_repository = DynamoDeviceComponentRepository(
            "LimnopulseDomain", app.state.fake_dynamo
        )
    return app


def http(app: FastAPI) -> TestClient:
    return TestClient(app, raise_server_exceptions=False)


def post(client: TestClient, path: str, body: dict, key: str = "idem-key-0001"):
    return client.post(path, json=body, headers={**HEADERS, "Idempotency-Key": key})


def create_device(client: TestClient, name: str = "Gateway", key: str = "device-key-01") -> dict:
    response = post(client, f"/tenants/{TENANT}/devices", {"name": name}, key)
    assert response.status_code == 201, response.text
    return response.json()


def probe_body(name: str = "Probe A") -> dict:
    return {
        "kind": "probe",
        "name": name,
        "probe_profile": {"serial_number": "SN-1", "measurement_metric_keys": ["ph", "temp"]},
    }


def create_probe(client: TestClient, device_id: str, name: str, key: str) -> dict:
    response = post(
        client, f"/tenants/{TENANT}/devices/{device_id}/components", probe_body(name), key
    )
    assert response.status_code == 201, response.text
    return response.json()


def capability_body(**overrides) -> dict:
    body = {
        "capability_key": "aerator.set_power",
        "operation": "command",
        "support": "supported",
        "availability": "available",
        "provenance": "model_profile",
        "parameter_schema": {"type": "object", "properties": {"power": {"maximum": 1.0}}},
        "result_schema": {"type": "object"},
        "provider_implementation_key": "relay.channel_1",
        "risk_class": "r2",
        "verified_at": "2026-08-25T11:00:00Z",
    }
    body.update(overrides)
    return body


def seed_legacy_device(fake: FakeDynamoClient, device_id: str = "local-device-001") -> None:
    common = {
        "tenant_id": TENANT,
        "pond_id": "pond_1",
        "device_id": device_id,
        "name": "Legacy Probe",
        "auth_type": "mtls",
        "firmware_version": "0.9",
        "status": "active",
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-01T00:00:00+00:00",
        "version": 1,
        "schema_version": 1,
    }
    fake.seed(
        {"PK": f"TENANT#{TENANT}", "SK": f"DEVICE#{device_id}", "entity_type": "device", **common}
    )
    fake.seed({"PK": f"DEVICE#{device_id}", "SK": "META", "entity_type": "device_lookup", **common})


V2_DEVICE_FIELDS = {
    "tenant_id",
    "device_id",
    "name",
    "model_profile_key",
    "firmware_version",
    "compatibility_level",
    "status",
    "schema_version",
    "created_at",
    "updated_at",
    "version",
}


# Devices ---------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "extra",
    [
        {"pond_id": "pond_1"},
        {"auth_type": "mtls"},
        {"pond_id": "pond_1", "auth_type": "mtls"},
        {"aws_thing_name": "thing-1"},
        {"certificate_arn": "arn:aws:iot:us-east-1:1:cert/x"},
        {"shadow_name": "classic"},
        {"tenant_id": TENANT},
        {"status": "active"},
        {"device_id": "dev_" + "1" * 32},
    ],
)
def test_device_create_rejects_legacy_fields(extra: dict) -> None:
    app = app_with_device_component_repository(role=TenantRole.OWNER)
    response = TestClient(app).post(
        f"/tenants/{TENANT}/devices",
        json={"name": "Gateway", **extra},
        headers={**HEADERS, "Idempotency-Key": "device-create-01"},
    )
    assert response.status_code == 422


def test_device_create_get_list_and_replay() -> None:
    client = http(app_with_device_component_repository())
    created = create_device(client)
    assert set(created) == V2_DEVICE_FIELDS
    assert is_canonical_id(created["device_id"], "dev")
    assert created["status"] == "active"
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z", created["created_at"])
    assert created["updated_at"] == created["created_at"]

    fetched = client.get(f"/tenants/{TENANT}/devices/{created['device_id']}", headers=HEADERS)
    assert fetched.status_code == 200
    assert fetched.json() == created

    replay = post(client, f"/tenants/{TENANT}/devices", {"name": "Gateway"}, "device-key-01")
    assert replay.status_code == 201
    assert (
        replay.content
        == post(client, f"/tenants/{TENANT}/devices", {"name": "Gateway"}, "device-key-01").content
    )
    assert replay.json() == created

    listed = client.get(f"/tenants/{TENANT}/devices", headers=HEADERS)
    assert listed.json() == {"items": [created]}

    conflict = post(client, f"/tenants/{TENANT}/devices", {"name": "Other"}, "device-key-01")
    assert conflict.status_code == 409


@pytest.mark.parametrize("key", [None, "short", "k" * 129])
def test_device_create_requires_valid_idempotency_key(key: str | None) -> None:
    client = http(app_with_device_component_repository())
    headers = dict(HEADERS) if key is None else {**HEADERS, "Idempotency-Key": key}
    response = client.post(f"/tenants/{TENANT}/devices", json={"name": "Gateway"}, headers=headers)
    assert response.status_code == 422


def test_device_patch_lifecycle_and_versions() -> None:
    client = http(app_with_device_component_repository())
    device_id = create_device(client)["device_id"]
    path = f"/tenants/{TENANT}/devices/{device_id}"

    patched = client.patch(
        path, json={"expected_version": 1, "status": "decommissioned"}, headers=HEADERS
    )
    assert patched.status_code == 200
    assert patched.json()["version"] == 2
    assert (
        client.patch(path, json={"expected_version": 1, "name": "x"}, headers=HEADERS).status_code
        == 409
    )
    revived = client.patch(path, json={"expected_version": 2, "status": "active"}, headers=HEADERS)
    assert revived.status_code == 409
    assert (
        client.patch(
            path, json={"expected_version": 2, "pond_id": "p"}, headers=HEADERS
        ).status_code
        == 422
    )
    assert client.patch(path, json={"expected_version": 0}, headers=HEADERS).status_code == 422


def test_legacy_device_is_readable_and_patch_preserves_v1_fields() -> None:
    fake = FakeDynamoClient()
    seed_legacy_device(fake)
    client = http(app_with_device_component_repository(client=fake))

    single = client.get(f"/tenants/{TENANT}/devices/local-device-001", headers=HEADERS)
    assert single.status_code == 200
    body = single.json()
    assert set(body) == V2_DEVICE_FIELDS
    assert body["device_id"] == "local-device-001"
    assert body["created_at"] == "2026-01-01T00:00:00.000000Z"
    listed = client.get(f"/tenants/{TENANT}/devices", headers=HEADERS).json()["items"]
    assert listed == [body]

    patched = client.patch(
        f"/tenants/{TENANT}/devices/local-device-001",
        json={"expected_version": 1, "name": "Renamed"},
        headers=HEADERS,
    )
    assert patched.status_code == 200
    assert patched.json()["name"] == "Renamed"
    for pk, sk in (
        (f"TENANT#{TENANT}", "DEVICE#local-device-001"),
        ("DEVICE#local-device-001", "META"),
    ):
        stored = fake.plain(pk, sk)
        assert stored["pond_id"] == "pond_1"
        assert stored["auth_type"] == "mtls"
        assert stored["name"] == "Renamed"
        assert stored["version"] == 2


def test_cross_tenant_and_unknown_devices_are_not_found() -> None:
    fake = FakeDynamoClient()
    client = http(app_with_device_component_repository(client=fake))
    device_id = create_device(client)["device_id"]
    row = fake.plain(f"TENANT#{TENANT}", f"DEVICE#{device_id}")
    fake.seed(
        {
            **row,
            "PK": f"TENANT#{OTHER_TENANT}",
            "tenant_id": OTHER_TENANT,
            "device_id": "dev_" + "f" * 32,
            "SK": "DEVICE#dev_" + "f" * 32,
        }
    )
    for path in (
        f"/tenants/{TENANT}/devices/dev_{'f' * 32}",
        f"/tenants/{TENANT}/devices/dev_{'0' * 32}",
    ):
        assert client.get(path, headers=HEADERS).status_code == 404
    assert (
        client.patch(
            f"/tenants/{TENANT}/devices/dev_{'f' * 32}",
            json={"expected_version": 1, "name": "x"},
            headers=HEADERS,
        ).status_code
        == 404
    )


# Components ------------------------------------------------------------------------------------


def test_one_gateway_owns_multiple_probe_components() -> None:
    client = http(app_with_device_component_repository())
    device_id = create_device(client)["device_id"]
    first = create_probe(client, device_id, "Probe A", "probe-key-01")
    second = create_probe(client, device_id, "Probe B", "probe-key-02")
    assert first["component_id"] != second["component_id"]
    assert first["status"] == "active"
    assert first["probe_profile"]["measurement_metric_keys"] == ["ph", "temp"]
    assert is_canonical_id(first["probe_profile"]["probe_profile_id"], "prb")
    listed = client.get(f"/tenants/{TENANT}/devices/{device_id}/components", headers=HEADERS)
    assert listed.status_code == 200
    assert {item["component_id"] for item in listed.json()["items"]} == {
        first["component_id"],
        second["component_id"],
    }
    fetched = client.get(f"/tenants/{TENANT}/components/{first['component_id']}", headers=HEADERS)
    assert fetched.json() == first


def test_component_create_validation_and_parent_rules() -> None:
    client = http(app_with_device_component_repository())
    device_id = create_device(client)["device_id"]
    path = f"/tenants/{TENANT}/devices/{device_id}/components"
    assert post(client, path, {"kind": "probe", "name": "P"}).status_code == 422
    assert post(client, path, {**probe_body(), "tenant_id": TENANT}).status_code == 422
    assert post(client, path, {**probe_body(), "status": "replaced"}).status_code == 422
    assert (
        post(
            client, f"/tenants/{TENANT}/devices/dev_{'0' * 32}/components", probe_body()
        ).status_code
        == 404
    )
    assert (
        client.get(
            f"/tenants/{TENANT}/devices/dev_{'0' * 32}/components", headers=HEADERS
        ).status_code
        == 404
    )

    client.patch(
        f"/tenants/{TENANT}/devices/{device_id}",
        json={"expected_version": 1, "status": "decommissioned"},
        headers=HEADERS,
    )
    assert post(client, path, probe_body(), "probe-key-99").status_code == 409


@pytest.mark.parametrize(
    "field,value",
    [
        ("device_id", "dev_" + "2" * 32),
        ("kind", "actuator"),
        ("probe_profile", {"serial_number": "x"}),
        ("model_profile_key", "acme.x"),
    ],
)
def test_component_immutable_fields_are_rejected(field: str, value) -> None:
    client = http(app_with_device_component_repository())
    device_id = create_device(client)["device_id"]
    component_id = create_probe(client, device_id, "Probe A", "probe-key-01")["component_id"]
    response = client.patch(
        f"/tenants/{TENANT}/components/{component_id}",
        json={"expected_version": 1, field: value},
        headers=HEADERS,
    )
    assert response.status_code == 422


def test_component_patch_mirrors_and_terminal_lifecycle() -> None:
    fake = FakeDynamoClient()
    client = http(app_with_device_component_repository(client=fake))
    device_id = create_device(client)["device_id"]
    component_id = create_probe(client, device_id, "Probe A", "probe-key-01")["component_id"]
    path = f"/tenants/{TENANT}/components/{component_id}"
    replaced = client.patch(
        path, json={"expected_version": 1, "status": "replaced"}, headers=HEADERS
    )
    assert replaced.status_code == 200
    assert replaced.json()["version"] == 2
    mirror = fake.plain(f"TENANT#{TENANT}", f"COMPONENT#{component_id}")
    canonical = fake.plain(f"COMPONENT#{component_id}", "META")
    assert mirror["version"] == canonical["version"] == 2
    assert mirror["status"] == canonical["status"] == "replaced"
    assert (
        client.patch(
            path, json={"expected_version": 2, "status": "active"}, headers=HEADERS
        ).status_code
        == 409
    )
    assert (
        client.patch(path, json={"expected_version": 1, "name": "x"}, headers=HEADERS).status_code
        == 409
    )


def test_component_of_other_tenant_is_not_found() -> None:
    fake = FakeDynamoClient()
    client = http(app_with_device_component_repository(client=fake))
    device_id = create_device(client)["device_id"]
    component_id = create_probe(client, device_id, "Probe A", "probe-key-01")["component_id"]
    canonical = fake.plain(f"COMPONENT#{component_id}", "META")
    fake.seed({**canonical, "tenant_id": OTHER_TENANT})
    assert (
        client.get(f"/tenants/{TENANT}/components/{component_id}", headers=HEADERS).status_code
        == 404
    )
    assert (
        client.get(
            f"/tenants/{TENANT}/components/{component_id}/capabilities", headers=HEADERS
        ).status_code
        == 404
    )
    assert (
        post(
            client, f"/tenants/{TENANT}/components/{component_id}/capabilities", capability_body()
        ).status_code
        == 404
    )


# Capabilities ----------------------------------------------------------------------------------


def test_capability_exposes_full_declaration_and_replays() -> None:
    client = http(app_with_device_component_repository())
    device_id = create_device(client)["device_id"]
    component_id = create_probe(client, device_id, "Probe A", "probe-key-01")["component_id"]
    path = f"/tenants/{TENANT}/components/{component_id}/capabilities"
    created = post(client, path, capability_body(), "cap-key-0001")
    assert created.status_code == 201
    body = created.json()
    assert is_canonical_id(body["capability_id"], "cap")
    assert body["support"] == "supported"
    assert body["availability"] == "available"
    assert body["provenance"] == "model_profile"
    assert body["parameter_schema"] == {"type": "object", "properties": {"power": {"maximum": 1.0}}}
    assert '"maximum": 1.0' in created.text
    assert body["result_schema"] == {"type": "object"}
    assert body["risk_class"] == "r2"
    assert body["verified_at"] == "2026-08-25T11:00:00.000000Z"
    assert body["provider_implementation_key"] == "relay.channel_1"
    assert post(client, path, capability_body(), "cap-key-0001").content == created.content
    listed = client.get(path, headers=HEADERS)
    assert listed.json() == {"items": [body]}
    assert "1.0" in listed.text
    assert post(client, path, capability_body(support="unknown"), "cap-key-0001").status_code == 409


@pytest.mark.parametrize(
    "overrides",
    [
        {"risk_class": None},
        {"operation": "read"},
        {"capability_key": "Not-A-Key"},
        {"verified_at": "2026-08-25T11:00:00"},
        {"provider_implementation_key": "arn:aws:iot:us-east-1:1:thing/x"},
        {"component_id": "cmp_" + "9" * 32},
    ],
)
def test_capability_validation_is_422(overrides: dict) -> None:
    client = http(app_with_device_component_repository())
    device_id = create_device(client)["device_id"]
    component_id = create_probe(client, device_id, "Probe A", "probe-key-01")["component_id"]
    body = {key: value for key, value in capability_body(**overrides).items() if value is not None}
    response = post(client, f"/tenants/{TENANT}/components/{component_id}/capabilities", body)
    assert response.status_code == 422


def test_capability_is_append_only() -> None:
    app = app_with_device_component_repository(role=TenantRole.ADMIN)
    openapi_paths = app.openapi()["paths"]
    paths = set(openapi_paths)
    assert (
        "/tenants/{tenant_id}/components/{component_id}/capabilities/{capability_id}" not in paths
    )
    methods = {
        (path, method.upper())
        for path, operations in openapi_paths.items()
        for method in operations
    }
    assert not {method for _, method in methods if method in {"DELETE", "PUT"}}
    assert not {
        method
        for path, method in methods
        if path.endswith("/capabilities") and method not in {"GET", "POST"}
    }


# Roles, wiring and isolation -------------------------------------------------------------------


def test_viewer_reads_but_cannot_write() -> None:
    fake = FakeDynamoClient()
    owner = http(app_with_device_component_repository(client=fake))
    device_id = create_device(owner)["device_id"]
    viewer = http(app_with_device_component_repository(TenantRole.VIEWER, client=fake))
    assert viewer.get(f"/tenants/{TENANT}/devices", headers=HEADERS).status_code == 200
    assert post(viewer, f"/tenants/{TENANT}/devices", {"name": "x"}).status_code == 403
    assert (
        viewer.patch(
            f"/tenants/{TENANT}/devices/{device_id}", json={"expected_version": 1}, headers=HEADERS
        ).status_code
        == 403
    )
    assert (
        post(viewer, f"/tenants/{TENANT}/devices/{device_id}/components", probe_body()).status_code
        == 403
    )


@pytest.mark.parametrize("role", [TenantRole.MEMBER])
def test_member_cannot_write(role: TenantRole) -> None:
    client = http(app_with_device_component_repository(role))
    assert post(client, f"/tenants/{TENANT}/devices", {"name": "x"}).status_code == 403


def test_admin_can_write() -> None:
    client = http(app_with_device_component_repository(TenantRole.ADMIN))
    create_device(client)


def test_non_member_tenant_is_denied() -> None:
    client = http(app_with_device_component_repository())
    assert client.get(f"/tenants/{OTHER_TENANT}/devices", headers=HEADERS).status_code == 403


def test_missing_repository_is_503() -> None:
    client = http(app_with_device_component_repository(wire_repository=False))
    assert client.get(f"/tenants/{TENANT}/devices", headers=HEADERS).status_code == 503


def test_infrastructure_errors_are_503_without_botocore_detail() -> None:
    fake = FakeDynamoClient()
    client = http(app_with_device_component_repository(client=fake))
    fake.fail_with = ClientError(
        {"Error": {"Code": "InternalServerError", "Message": "secret internals"}},
        "TransactWriteItems",
    )
    response = post(client, f"/tenants/{TENANT}/devices", {"name": "Gateway"})
    assert response.status_code == 503
    assert response.json() == {"detail": "service unavailable"}


def test_routers_are_not_mounted_and_v1_is_unchanged() -> None:
    from limnopulse_api.core.config import Settings

    app = create_app(Settings(app_env="test", auth_mode="dev"))
    document = app.openapi()
    paths = set(document["paths"])
    assert not any(path.startswith("/v2") for path in paths)
    assert "/v1/tenants/{tenant_id}/devices" in paths
    schemas = document["components"]["schemas"]
    assert not any(name.startswith("V2") for name in schemas)


# Audit round 1 ----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "method,path",
    [
        ("GET", "/devices/dev_short"),
        ("GET", "/devices/bad%23id"),
        ("PATCH", "/devices/dev_short"),
        ("GET", "/devices/dev_short/components"),
        ("POST", "/devices/dev_short/components"),
        ("GET", "/components/cmp_short"),
        ("GET", "/components/local-component"),
        ("PATCH", "/components/cmp_short"),
        ("GET", "/components/cmp_short/capabilities"),
        ("POST", "/components/cmp_short/capabilities"),
    ],
)
def test_malformed_path_ids_are_422(method: str, path: str) -> None:
    client = http(app_with_device_component_repository(TenantRole.OWNER))
    body = {
        "PATCH": {"expected_version": 1},
        "POST": capability_body() if "capabilities" in path else probe_body(),
    }.get(method)
    response = client.request(
        method,
        f"/tenants/{TENANT}{path}",
        json=body,
        headers={**HEADERS, "Idempotency-Key": "idem-key-0001"},
    )
    assert response.status_code == 422


def test_well_formed_unknown_ids_stay_404_and_historical_ids_are_valid() -> None:
    client = http(app_with_device_component_repository(TenantRole.OWNER))
    for path in (
        "/devices/local-device-404",
        f"/devices/dev_{'0' * 32}",
        f"/components/cmp_{'0' * 32}",
        f"/components/cmp_{'0' * 32}/capabilities",
        f"/devices/dev_{'0' * 32}/components",
    ):
        assert client.get(f"/tenants/{TENANT}{path}", headers=HEADERS).status_code == 404


def test_unmappable_legacy_row_does_not_break_device_list(caplog) -> None:
    fake = FakeDynamoClient()
    seed_legacy_device(fake)
    seed_legacy_device(fake, device_id="dev_legacy01")
    client = http(app_with_device_component_repository(client=fake))
    with caplog.at_level("WARNING"):
        listed = client.get(f"/tenants/{TENANT}/devices", headers=HEADERS)
    assert listed.status_code == 200
    assert [item["device_id"] for item in listed.json()["items"]] == ["local-device-001"]
    assert "DEVICE#dev_legacy01" in caplog.text
    assert "Legacy Probe" not in caplog.text


@pytest.mark.parametrize("field", ["parameter_schema", "result_schema"])
@pytest.mark.parametrize("bad", ["NaN", "Infinity", "-Infinity"])
def test_non_finite_capability_schema_is_422(field: str, bad: str) -> None:
    client = http(app_with_device_component_repository())
    device_id = create_device(client)["device_id"]
    component_id = create_probe(client, device_id, "Probe A", "probe-key-01")["component_id"]
    body = json.dumps(capability_body()).replace(f'"{field}": {{', f'"{field}": {{"x": {bad}, ', 1)
    response = client.post(
        f"/tenants/{TENANT}/components/{component_id}/capabilities",
        content=body,
        headers={**HEADERS, "Idempotency-Key": "cap-key-0001", "Content-Type": "application/json"},
    )
    assert response.status_code == 422


def test_oversized_capability_schema_and_metric_keys_are_422() -> None:
    client = http(app_with_device_component_repository())
    device_id = create_device(client)["device_id"]
    component_id = create_probe(client, device_id, "Probe A", "probe-key-01")["component_id"]
    huge = {"type": "object", "description": "x" * (32 * 1024)}
    path = f"/tenants/{TENANT}/components/{component_id}/capabilities"
    assert post(client, path, capability_body(parameter_schema=huge)).status_code == 422
    assert post(client, path, capability_body(result_schema=huge)).status_code == 422

    components = f"/tenants/{TENANT}/devices/{device_id}/components"
    too_many = {**probe_body(), "probe_profile": {"measurement_metric_keys": ["k"] * 65}}
    too_long = {**probe_body(), "probe_profile": {"measurement_metric_keys": ["k" * 129]}}
    assert post(client, components, too_many, "probe-key-02").status_code == 422
    assert post(client, components, too_long, "probe-key-03").status_code == 422
