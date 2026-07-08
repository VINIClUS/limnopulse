from datetime import UTC, datetime

from fastapi.testclient import TestClient

from limnopulse_api.core.config import Settings
from limnopulse_api.domain.entities import Device, Membership, Pond, Tenant
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
