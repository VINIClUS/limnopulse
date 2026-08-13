from datetime import UTC, datetime

from fastapi import Request
from fastapi.testclient import TestClient

from limnopulse_api.auth.models import Principal
from limnopulse_api.core.config import Settings
from limnopulse_api.domain.entities import Membership
from limnopulse_api.domain.roles import TenantRole
from limnopulse_api.domain.telegram import TelegramBindingView
from limnopulse_api.main import create_app
from limnopulse_api.services.telegram_bindings import IssuedBindingToken

NOW = datetime(2026, 8, 13, 12, 0, tzinfo=UTC)
PATH = "/v1/tenants/tnt_1/me/telegram-binding"


class StaticAuthProvider:
    async def authenticate(self, request: Request) -> Principal:
        return Principal(cognito_sub="sub_1", access_token="token", scopes=frozenset())


class FakeMembershipService:
    def __init__(self, active: bool = True) -> None:
        self.active = active

    async def get_active_membership(
        self,
        cognito_sub: str,
        tenant_id: str,
    ) -> Membership | None:
        if not self.active:
            return None
        return Membership(
            tenant_id=tenant_id,
            cognito_sub=cognito_sub,
            role=TenantRole.MEMBER,
            status="active",
            created_at=NOW,
            updated_at=NOW,
            version=1,
        )


class RecordingBindingService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    async def get(self, tenant_id: str, recipient_id: str) -> TelegramBindingView:
        self.calls.append(("get", tenant_id, recipient_id))
        return TelegramBindingView(
            status="verified",
            version=2,
            verified_at=NOW,
            effective_enabled=True,
        )

    async def issue(self, tenant_id: str, recipient_id: str) -> IssuedBindingToken:
        self.calls.append(("issue", tenant_id, recipient_id))
        return IssuedBindingToken(
            request_id="request_1",
            token="AQIDBAUGBwgJCgsMDQ4PEA",
            token_hash="0" * 64,
            deep_link="https://t.me/limnopulse_test_bot?start=AQIDBAUGBwgJCgsMDQ4PEA",
            expires_at=NOW,
        )

    async def revoke(self, tenant_id: str, recipient_id: str) -> None:
        self.calls.append(("revoke", tenant_id, recipient_id))


def build_client(*, active: bool = True) -> tuple[TestClient, RecordingBindingService]:
    service = RecordingBindingService()
    app = create_app(Settings(app_env="test", auth_mode="dev"))
    app.state.auth_provider = StaticAuthProvider()
    app.state.membership_service = FakeMembershipService(active)
    app.state.telegram_binding_service = service
    return TestClient(app), service


def test_binding_api_is_tenant_scoped_and_never_exposes_chat_or_token_on_get() -> None:
    client, service = build_client()

    response = client.get(PATH)

    assert response.status_code == 200
    assert response.json() == {
        "status": "verified",
        "version": 2,
        "verified_at": "2026-08-13T12:00:00Z",
        "pending_request_id": None,
        "pending_expires_at": None,
        "effective_enabled": True,
    }
    assert "chat" not in response.text
    assert "token" not in response.text
    assert service.calls == [("get", "tnt_1", "sub_1")]


def test_token_endpoint_returns_raw_token_once_without_changing_preferences() -> None:
    client, service = build_client()

    response = client.post(f"{PATH}-token")

    assert response.status_code == 201
    assert response.json() == {
        "request_id": "request_1",
        "token": "AQIDBAUGBwgJCgsMDQ4PEA",
        "deep_link": "https://t.me/limnopulse_test_bot?start=AQIDBAUGBwgJCgsMDQ4PEA",
        "expires_at": "2026-08-13T12:00:00Z",
    }
    assert "token_hash" not in response.json()
    assert service.calls == [("issue", "tnt_1", "sub_1")]


def test_delete_is_idempotent_no_content_and_membership_protected() -> None:
    client, service = build_client()

    first = client.delete(PATH)
    second = client.delete(PATH)
    denied_client, denied_service = build_client(active=False)
    denied = denied_client.delete(PATH)

    assert first.status_code == 204
    assert second.status_code == 204
    assert first.content == b""
    assert service.calls == [
        ("revoke", "tnt_1", "sub_1"),
        ("revoke", "tnt_1", "sub_1"),
    ]
    assert denied.status_code == 403
    assert denied_service.calls == []
