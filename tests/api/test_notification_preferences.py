from datetime import UTC, datetime
from typing import Any

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from fastapi import Request
from fastapi.testclient import TestClient

from limnopulse_api.auth.models import Principal
from limnopulse_api.core.config import Settings
from limnopulse_api.core.errors import AuthError, ConflictError
from limnopulse_api.domain.alerts import AlertSeverity, AuditContext
from limnopulse_api.domain.entities import Membership
from limnopulse_api.domain.notification_preferences import (
    EmailDeliverability,
    EmailDeliverabilityRecord,
    NotificationPreference,
)
from limnopulse_api.domain.roles import TenantRole
from limnopulse_api.main import create_app
from limnopulse_api.services.cognito_identity import (
    COGNITO_GET_USER_SCOPE,
    CognitoIdentityVerifier,
)


NOW = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
PATH = "/v1/tenants/tnt_1/me/notification-preferences"
TOKEN = "same-validated-access-token"


class StaticAuthProvider:
    def __init__(
        self,
        principal: Principal | None = None,
        error: Exception | None = None,
    ) -> None:
        self.principal = principal or Principal(
            cognito_sub="sub_1",
            access_token=TOKEN,
            scopes=frozenset({COGNITO_GET_USER_SCOPE}),
        )
        self.error = error

    async def authenticate(self, request: Request) -> Principal:
        if self.error is not None:
            raise self.error
        return self.principal


class FakeMembershipService:
    def __init__(self, *, active: bool = True) -> None:
        self.active = active

    async def get_active_membership(
        self,
        cognito_sub: str,
        tenant_id: str,
    ) -> Membership | None:
        if not self.active or cognito_sub != "sub_1" or tenant_id != "tnt_1":
            return None
        return Membership(
            tenant_id=tenant_id,
            cognito_sub=cognito_sub,
            role=TenantRole.MEMBER,
            created_at=NOW,
            updated_at=NOW,
            version=1,
        )


class InMemoryNotificationPreferenceRepository:
    def __init__(
        self,
        preference: NotificationPreference | None = None,
        deliverability: EmailDeliverabilityRecord | None = None,
    ) -> None:
        self.preference = preference
        self.deliverability = deliverability
        self.get_calls: list[tuple[str, str]] = []
        self.deliverability_calls: list[str] = []
        self.save_calls: list[tuple[NotificationPreference, int | None, AuditContext]] = []

    async def get(self, tenant_id: str, cognito_sub: str) -> NotificationPreference | None:
        self.get_calls.append((tenant_id, cognito_sub))
        return self.preference

    async def get_email_deliverability(
        self,
        address: str,
    ) -> EmailDeliverabilityRecord | None:
        self.deliverability_calls.append(address)
        return self.deliverability

    async def save(
        self,
        preference: NotificationPreference,
        expected_version: int | None,
        audit: AuditContext,
    ) -> NotificationPreference:
        if expected_version is None:
            if self.preference is not None:
                raise ConflictError("preference already exists")
        elif self.preference is None or self.preference.version != expected_version:
            raise ConflictError("preference version conflict")
        self.save_calls.append((preference, expected_version, audit))
        self.preference = preference
        return preference


class FakeCognitoClient:
    def __init__(
        self,
        user_attributes: list[dict[str, str]] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.user_attributes = user_attributes or cognito_attributes()
        self.error = error
        self.calls: list[dict[str, str]] = []

    def get_user(self, **kwargs: str) -> dict[str, Any]:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return {"UserAttributes": self.user_attributes}


class FailingIdentityVerifier:
    def __init__(self) -> None:
        self.calls = 0

    async def verify(self, principal: Principal):
        self.calls += 1
        raise AssertionError("GET/disable must not call Cognito")


def cognito_attributes(
    *,
    sub: str = "sub_1",
    email: str | None = "verified@example.com",
    verified: str | None = "true",
) -> list[dict[str, str]]:
    values = [{"Name": "sub", "Value": sub}]
    if email is not None:
        values.append({"Name": "email", "Value": email})
    if verified is not None:
        values.append({"Name": "email_verified", "Value": verified})
    return values


def make_preference(**updates: object) -> NotificationPreference:
    values: dict[str, object] = {
        "tenant_id": "tnt_1",
        "cognito_sub": "sub_1",
        "version": 1,
        "email_enabled": True,
        "email_address": "verified@example.com",
        "email_verified": True,
        "checked_at": NOW,
        "identity_source": "cognito_get_user",
        "minimum_severity": AlertSeverity.CRITICAL,
        "created_at": NOW,
        "updated_at": NOW,
    }
    values.update(updates)
    return NotificationPreference.model_validate(values)


def build_app(
    repository: InMemoryNotificationPreferenceRepository,
    identity_verifier: object | None,
    *,
    auth_provider: StaticAuthProvider | None = None,
    membership_active: bool = True,
):
    app = create_app(Settings(app_env="test", auth_mode="dev"))
    app.state.auth_provider = auth_provider or StaticAuthProvider()
    app.state.membership_service = FakeMembershipService(active=membership_active)
    app.state.notification_preference_repository = repository
    if identity_verifier is not None:
        app.state.cognito_identity_verifier = identity_verifier
    return app


def test_get_default_is_read_only_and_returns_exact_unconfigured_shape() -> None:
    repository = InMemoryNotificationPreferenceRepository()
    verifier = FailingIdentityVerifier()

    response = TestClient(build_app(repository, verifier)).get(PATH)

    assert response.status_code == 200
    assert response.json() == {
        "configured": False,
        "version": None,
        "email": {
            "enabled": False,
            "address": None,
            "verified": False,
            "deliverability": "unknown",
            "suppression_reason": None,
            "effective_enabled": False,
        },
        "minimum_severity": "critical",
    }
    assert verifier.calls == 0
    assert repository.deliverability_calls == []
    assert repository.save_calls == []


def test_get_does_not_require_cognito_verifier_dependency() -> None:
    repository = InMemoryNotificationPreferenceRepository()
    app = build_app(repository, FailingIdentityVerifier())
    del app.state.cognito_identity_verifier

    response = TestClient(app).get(PATH)

    assert response.status_code == 200
    assert response.json()["configured"] is False


def test_get_masks_email_and_exposes_suppression() -> None:
    repository = InMemoryNotificationPreferenceRepository(
        make_preference(),
        EmailDeliverabilityRecord(
            deliverability=EmailDeliverability.SUPPRESSED,
            suppression_reason="complaint",
        ),
    )

    response = TestClient(build_app(repository, FailingIdentityVerifier())).get(PATH)

    assert response.status_code == 200
    assert response.json()["email"] == {
        "enabled": True,
        "address": "v***d@example.com",
        "verified": True,
        "deliverability": "suppressed",
        "suppression_reason": "complaint",
        "effective_enabled": False,
    }


def test_put_create_calls_cognito_with_same_token_and_returns_version_one() -> None:
    repository = InMemoryNotificationPreferenceRepository()
    cognito = FakeCognitoClient(cognito_attributes(email="User@b\u00fccher.example"))

    response = TestClient(
        build_app(repository, CognitoIdentityVerifier(cognito, clock=lambda: NOW))
    ).put(
        PATH,
        json={
            "expected_version": None,
            "email_enabled": True,
            "minimum_severity": "warning",
        },
        headers={"User-Agent": "limnopulse-tests"},
    )

    assert response.status_code == 200
    assert response.json()["version"] == 1
    assert response.json()["minimum_severity"] == "warning"
    assert response.json()["email"]["address"] == "U***r@xn--bcher-kva.example"
    assert response.json()["email"]["effective_enabled"] is True
    assert cognito.calls == [{"AccessToken": TOKEN}]
    saved, expected_version, audit = repository.save_calls[0]
    assert expected_version is None
    assert saved.email_address == "User@xn--bcher-kva.example"
    assert audit.actor_id == "sub_1"
    assert audit.user_agent == "limnopulse-tests"


def test_put_enabled_update_rechecks_cognito_and_disable_does_not() -> None:
    repository = InMemoryNotificationPreferenceRepository(make_preference())
    cognito = FakeCognitoClient(cognito_attributes(email="fresh@example.com"))
    client = TestClient(build_app(repository, CognitoIdentityVerifier(cognito, clock=lambda: NOW)))

    enabled = client.put(
        PATH,
        json={"expected_version": 1, "email_enabled": True},
    )
    assert enabled.status_code == 200
    assert enabled.json()["version"] == 2
    assert cognito.calls == [{"AccessToken": TOKEN}]

    app = build_app(repository, FailingIdentityVerifier())
    disabled = TestClient(app).put(
        PATH,
        json={"expected_version": 2, "email_enabled": False},
    )
    assert disabled.status_code == 200
    assert disabled.json()["version"] == 3
    assert disabled.json()["email"]["enabled"] is False
    assert disabled.json()["email"]["effective_enabled"] is False


def test_put_disable_existing_does_not_require_cognito_verifier_wiring() -> None:
    repository = InMemoryNotificationPreferenceRepository(make_preference())

    response = TestClient(build_app(repository, None)).put(
        PATH,
        json={"expected_version": 1, "email_enabled": False},
    )

    assert response.status_code == 200
    assert response.json()["version"] == 2
    assert response.json()["email"]["enabled"] is False


@pytest.mark.parametrize(
    "payload",
    [
        {"email_enabled": True},
        {"expected_version": None, "email_enabled": True, "minimum_severity": None},
        {"expected_version": None, "email_enabled": True, "severities": ["critical"]},
        {"expected_version": None, "email_enabled": True, "unexpected": True},
    ],
)
def test_put_rejects_invalid_payload_contract(payload: dict[str, object]) -> None:
    response = TestClient(
        build_app(
            InMemoryNotificationPreferenceRepository(),
            FailingIdentityVerifier(),
        )
    ).put(PATH, json=payload)

    assert response.status_code == 422


def test_put_stale_version_maps_to_409_without_cognito_when_disabling() -> None:
    repository = InMemoryNotificationPreferenceRepository(make_preference(version=2))

    response = TestClient(build_app(repository, FailingIdentityVerifier())).put(
        PATH,
        json={"expected_version": 1, "email_enabled": False},
    )

    assert response.status_code == 409


def test_missing_membership_maps_to_403_before_cognito() -> None:
    cognito = FakeCognitoClient()

    response = TestClient(
        build_app(
            InMemoryNotificationPreferenceRepository(),
            CognitoIdentityVerifier(cognito),
            membership_active=False,
        )
    ).put(PATH, json={"expected_version": None, "email_enabled": True})

    assert response.status_code == 403
    assert cognito.calls == []


def test_invalid_locally_validated_identity_maps_to_401() -> None:
    response = TestClient(
        build_app(
            InMemoryNotificationPreferenceRepository(),
            FailingIdentityVerifier(),
            auth_provider=StaticAuthProvider(error=AuthError("invalid token")),
        )
    ).get(PATH)

    assert response.status_code == 401


@pytest.mark.parametrize(
    "principal,user_attributes,error",
    [
        (
            Principal(cognito_sub="sub_1", access_token=TOKEN, scopes=frozenset({"openid"})),
            cognito_attributes(),
            None,
        ),
        (None, cognito_attributes(sub="other_sub"), None),
        (
            None,
            cognito_attributes(),
            ClientError(
                {"Error": {"Code": "NotAuthorizedException", "Message": "invalid"}},
                "GetUser",
            ),
        ),
    ],
    ids=["missing-scope", "subject-mismatch", "get-user-invalid-token"],
)
def test_get_user_identity_failures_map_to_401(
    principal: Principal | None,
    user_attributes: list[dict[str, str]],
    error: Exception | None,
) -> None:
    cognito = FakeCognitoClient(user_attributes, error)

    response = TestClient(
        build_app(
            InMemoryNotificationPreferenceRepository(),
            CognitoIdentityVerifier(cognito),
            auth_provider=StaticAuthProvider(principal=principal) if principal else None,
        )
    ).put(PATH, json={"expected_version": None, "email_enabled": True})

    assert response.status_code == 401


@pytest.mark.parametrize(
    "user_attributes",
    [
        cognito_attributes(email=None),
        cognito_attributes(verified="false"),
        cognito_attributes(email="invalid"),
        cognito_attributes(email="\u03b4\u03bf\u03ba\u03b9\u03bc\u03ae@example.com"),
    ],
    ids=["absent", "unverified", "invalid", "smtputf8"],
)
def test_unusable_cognito_email_maps_to_422(
    user_attributes: list[dict[str, str]],
) -> None:
    response = TestClient(
        build_app(
            InMemoryNotificationPreferenceRepository(),
            CognitoIdentityVerifier(FakeCognitoClient(user_attributes)),
        )
    ).put(PATH, json={"expected_version": None, "email_enabled": True})

    assert response.status_code == 422


@pytest.mark.parametrize(
    "error",
    [
        ClientError(
            {"Error": {"Code": "InternalErrorException", "Message": "outage"}},
            "GetUser",
        ),
        EndpointConnectionError(endpoint_url="https://cognito.example.test"),
    ],
    ids=["service", "transport"],
)
def test_cognito_outage_maps_to_503_without_mutation(error: Exception) -> None:
    repository = InMemoryNotificationPreferenceRepository()

    response = TestClient(
        build_app(
            repository,
            CognitoIdentityVerifier(FakeCognitoClient(error=error)),
        )
    ).put(PATH, json={"expected_version": None, "email_enabled": True})

    assert response.status_code == 503
    assert repository.preference is None
    assert repository.save_calls == []
