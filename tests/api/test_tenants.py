from datetime import UTC, datetime

from fastapi.testclient import TestClient
from botocore.exceptions import EndpointConnectionError

from limnopulse_api.core.config import Settings
from limnopulse_api.domain.entities import Membership, Tenant
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
        self.created_owner_sub: str | None = None
        self.tenants: dict[str, Tenant] = {"tnt_1": make_tenant(name="Demo")}

    async def list_memberships_for_user(self, cognito_sub: str) -> list[Membership]:
        return []

    async def list_tenants_for_memberships(self, memberships: list[Membership]) -> list[Tenant]:
        return [self.tenants[membership.tenant_id] for membership in memberships if membership.tenant_id in self.tenants]

    async def create_tenant_with_owner(self, tenant_id: str, name: str, owner_sub: str) -> Tenant:
        self.created_owner_sub = owner_sub
        tenant = make_tenant(tenant_id=tenant_id, name=name)
        self.tenants[tenant_id] = tenant
        return tenant

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
            update={
                "name": name or current.name,
                "version": expected_version + 1,
                "updated_at": datetime.now(UTC),
            }
        )
        self.tenants[tenant_id] = tenant
        return tenant


class FailingTenantReadRepository(FakeDomainRepository):
    async def get_tenant(self, tenant_id: str) -> Tenant | None:
        raise EndpointConnectionError(endpoint_url="http://localhost:8000")


def make_tenant(tenant_id: str = "tnt_1", name: str = "Tenant") -> Tenant:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return Tenant(
        tenant_id=tenant_id,
        name=name,
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


def test_valid_identity_without_membership_cannot_read_tenant() -> None:
    app = create_app(Settings(app_env="test", auth_mode="dev"))
    app.state.domain_repository = FakeDomainRepository()
    app.state.membership_service = FakeMembershipService(membership=None)
    client = TestClient(app)

    response = client.get("/v1/tenants/tnt_1", headers={"X-Dev-User-Sub": "sub_1"})

    assert response.status_code == 403


def test_owner_can_create_tenant_and_gets_owner_membership() -> None:
    repo = FakeDomainRepository()
    app = create_app(Settings(app_env="test", auth_mode="dev"))
    app.state.domain_repository = repo
    app.state.membership_service = FakeMembershipService(membership=None)
    client = TestClient(app)

    response = client.post("/v1/tenants", json={"name": "Demo"}, headers={"X-Dev-User-Sub": "sub_1"})

    assert response.status_code == 201
    assert repo.created_owner_sub == "sub_1"


def test_viewer_cannot_patch_tenant() -> None:
    app = app_with_membership(role=TenantRole.VIEWER)
    client = TestClient(app)

    response = client.patch(
        "/v1/tenants/tnt_1",
        json={"name": "New", "expected_version": 1},
        headers={"X-Dev-User-Sub": "sub_1"},
    )

    assert response.status_code == 403


def test_admin_can_patch_tenant() -> None:
    app = app_with_membership(role=TenantRole.ADMIN)
    client = TestClient(app)

    response = client.patch(
        "/v1/tenants/tnt_1",
        json={"name": "New", "expected_version": 1},
        headers={"X-Dev-User-Sub": "sub_1"},
    )

    assert response.status_code == 200
    assert response.json()["name"] == "New"


def test_tenant_read_infra_failure_returns_503() -> None:
    app = create_app(Settings(app_env="test", auth_mode="dev"))
    app.state.domain_repository = FailingTenantReadRepository()
    app.state.membership_service = FakeMembershipService(membership=make_membership(TenantRole.ADMIN))
    client = TestClient(app, raise_server_exceptions=False)

    response = client.get("/v1/tenants/tnt_1", headers={"X-Dev-User-Sub": "sub_1"})

    assert response.status_code == 503
    assert response.json() == {"detail": "service unavailable"}
