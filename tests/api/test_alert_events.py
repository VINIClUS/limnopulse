from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from limnopulse_api.core.config import Settings
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.alert_events import AlertEvent, AlertEventStatus
from limnopulse_api.domain.entities import Membership
from limnopulse_api.domain.roles import TenantRole
from limnopulse_api.main import create_app


NOW = datetime(2026, 7, 15, 12, 0, tzinfo=UTC)


def make_event(status: AlertEventStatus = AlertEventStatus.OPEN, version: int = 1) -> AlertEvent:
    return AlertEvent(
        tenant_id="tnt_1",
        event_id="alert_1",
        rule_id="rule_1",
        rule_version=2,
        evaluation_revision=3,
        rule_name="Low oxygen",
        pond_id="pond_1",
        device_id="dev_1",
        metric="do_mg_l",
        operator="<",
        threshold=5.0,
        aggregation="min",
        severity="critical",
        status=status,
        opened_at=NOW,
        confirmed_open_window_end=NOW,
        window_start=NOW,
        window_end=NOW,
        last_evaluated_at=NOW,
        last_evaluation_quality="sufficient",
        last_evaluation_value=4.2,
        created_at=NOW,
        updated_at=NOW,
        version=version,
    )


class FakeMembershipService:
    def __init__(self, role: TenantRole) -> None:
        self.role = role

    async def get_active_membership(self, cognito_sub: str, tenant_id: str) -> Membership | None:
        if cognito_sub != "sub_1" or tenant_id != "tnt_1":
            return None
        return Membership(
            tenant_id=tenant_id,
            cognito_sub=cognito_sub,
            role=self.role,
            created_at=NOW,
            updated_at=NOW,
            version=1,
        )


class FakeEventRepository:
    def __init__(self, event: AlertEvent | None = None) -> None:
        self.event = event or make_event()
        self.actions: list[str] = []

    async def list_events(self, tenant_id: str) -> list[AlertEvent]:
        return [self.event] if self.event.tenant_id == tenant_id else []

    async def get_event(self, tenant_id: str, event_id: str) -> AlertEvent | None:
        if self.event.tenant_id == tenant_id and self.event.event_id == event_id:
            return self.event
        return None

    async def acknowledge_event(self, tenant_id, event_id, expected_version, audit):
        if await self.get_event(tenant_id, event_id) is None:
            raise NotFoundError("not found")
        if self.event.version != expected_version or self.event.status in {
            AlertEventStatus.SUPPRESSED,
            AlertEventStatus.RESOLVED,
        }:
            raise ConflictError("event cannot be acknowledged")
        self.actions.append("acknowledged")
        self.event = self.event.model_copy(
            update={"status": AlertEventStatus.ACKNOWLEDGED, "version": expected_version + 1}
        )
        return self.event

    async def resolve_event(self, tenant_id, event_id, expected_version, audit):
        if await self.get_event(tenant_id, event_id) is None:
            raise NotFoundError("not found")
        if self.event.version != expected_version or self.event.status == AlertEventStatus.RESOLVED:
            raise ConflictError("event cannot be resolved")
        self.actions.append("resolved")
        self.event = self.event.model_copy(
            update={"status": AlertEventStatus.RESOLVED, "version": expected_version + 1}
        )
        return self.event


def app_for(role: TenantRole, event: AlertEvent | None = None):
    app = create_app(Settings(app_env="test", auth_mode="dev"))
    repository = FakeEventRepository(event)
    app.state.alert_event_repository = repository
    app.state.membership_service = FakeMembershipService(role)
    return app, repository


def headers() -> dict[str, str]:
    return {"X-Dev-User-Sub": "sub_1", "User-Agent": "pytest"}


@pytest.mark.parametrize("role", list(TenantRole))
def test_all_roles_can_list_and_get_events(role: TenantRole) -> None:
    app, _ = app_for(role)
    client = TestClient(app)
    base = "/v1/tenants/tnt_1/alert-events"

    assert client.get(base, headers=headers()).json()["items"][0]["event_id"] == "alert_1"
    assert client.get(f"{base}/alert_1", headers=headers()).status_code == 200


@pytest.mark.parametrize("role", [TenantRole.OWNER, TenantRole.ADMIN, TenantRole.MEMBER])
def test_member_and_write_roles_can_acknowledge(role: TenantRole) -> None:
    app, repository = app_for(role)

    response = TestClient(app).post(
        "/v1/tenants/tnt_1/alert-events/alert_1/acknowledge",
        json={"expected_version": 1},
        headers=headers(),
    )

    assert response.status_code == 200
    assert response.json()["status"] == "acknowledged"
    assert repository.actions == ["acknowledged"]


def test_viewer_cannot_acknowledge_and_member_cannot_resolve() -> None:
    viewer, _ = app_for(TenantRole.VIEWER)
    member, _ = app_for(TenantRole.MEMBER)
    path = "/v1/tenants/tnt_1/alert-events/alert_1"

    assert TestClient(viewer).post(
        f"{path}/acknowledge", json={"expected_version": 1}, headers=headers()
    ).status_code == 403
    assert TestClient(member).post(
        f"{path}/resolve", json={"expected_version": 1}, headers=headers()
    ).status_code == 403


@pytest.mark.parametrize("role", [TenantRole.OWNER, TenantRole.ADMIN])
def test_owner_and_admin_can_resolve(role: TenantRole) -> None:
    app, repository = app_for(role)

    response = TestClient(app).post(
        "/v1/tenants/tnt_1/alert-events/alert_1/resolve",
        json={"expected_version": 1},
        headers=headers(),
    )

    assert response.status_code == 200
    assert response.json()["status"] == "resolved"
    assert repository.actions == ["resolved"]


def test_suppressed_event_cannot_be_acknowledged() -> None:
    app, _ = app_for(TenantRole.ADMIN, make_event(AlertEventStatus.SUPPRESSED))

    response = TestClient(app).post(
        "/v1/tenants/tnt_1/alert-events/alert_1/acknowledge",
        json={"expected_version": 1},
        headers=headers(),
    )

    assert response.status_code == 409
