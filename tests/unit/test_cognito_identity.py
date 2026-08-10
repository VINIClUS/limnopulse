from datetime import UTC, datetime
from threading import get_ident
from typing import Any

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from limnopulse_api.auth.models import Principal
from limnopulse_api.core.errors import (
    AuthError,
    IdentityEmailError,
    IdentityServiceUnavailableError,
)
from limnopulse_api.services.cognito_identity import (
    COGNITO_GET_USER_SCOPE,
    CognitoIdentityVerifier,
)

NOW = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
TOKEN = "same-validated-access-token"


class FakeCognitoClient:
    def __init__(
        self,
        attributes: list[dict[str, str]] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.attributes = attributes or []
        self.error = error
        self.calls: list[dict[str, str]] = []
        self.thread_ids: list[int] = []

    def get_user(self, **kwargs: str) -> dict[str, Any]:
        self.thread_ids.append(get_ident())
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return {"UserAttributes": self.attributes}


def principal(*, scopes: frozenset[str] | None = None) -> Principal:
    return Principal(
        cognito_sub="sub_1",
        access_token=TOKEN,
        scopes=scopes if scopes is not None else frozenset({COGNITO_GET_USER_SCOPE}),
    )


def attributes(
    *,
    sub: str = "sub_1",
    email: str | None = "User@b\u00fccher.example",
    verified: str | None = "true",
) -> list[dict[str, str]]:
    values = [{"Name": "sub", "Value": sub}]
    if email is not None:
        values.append({"Name": "email", "Value": email})
    if verified is not None:
        values.append({"Name": "email_verified", "Value": verified})
    return values


@pytest.mark.asyncio
async def test_get_user_uses_same_token_and_returns_normalized_ascii_identity() -> None:
    client = FakeCognitoClient(attributes())
    event_loop_thread = get_ident()

    identity = await CognitoIdentityVerifier(client, clock=lambda: NOW).verify(principal())

    assert client.calls == [{"AccessToken": TOKEN}]
    assert client.thread_ids[0] != event_loop_thread
    assert identity.address == "User@xn--bcher-kva.example"
    assert identity.verified is True
    assert identity.checked_at == NOW
    assert identity.identity_source == "cognito_get_user"


@pytest.mark.asyncio
async def test_get_user_requires_reserved_scope_before_calling_cognito() -> None:
    client = FakeCognitoClient(attributes())

    with pytest.raises(AuthError):
        await CognitoIdentityVerifier(client).verify(principal(scopes=frozenset({"openid"})))

    assert client.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_attributes",
    [
        attributes(email=None),
        attributes(verified=None),
        attributes(verified="false"),
        attributes(email="not-an-email"),
        attributes(email="\u03b4\u03bf\u03ba\u03b9\u03bc\u03ae@example.com"),
    ],
    ids=["absent", "verification-absent", "unverified", "invalid", "smtputf8"],
)
async def test_get_user_rejects_unusable_email(bad_attributes: list[dict[str, str]]) -> None:
    client = FakeCognitoClient(bad_attributes)

    with pytest.raises(IdentityEmailError):
        await CognitoIdentityVerifier(client).verify(principal())


@pytest.mark.asyncio
async def test_get_user_rejects_subject_mismatch_as_invalid_identity() -> None:
    client = FakeCognitoClient(attributes(sub="other_sub"))

    with pytest.raises(AuthError):
        await CognitoIdentityVerifier(client).verify(principal())


@pytest.mark.asyncio
async def test_get_user_maps_not_authorized_to_auth_error() -> None:
    error = ClientError(
        {"Error": {"Code": "NotAuthorizedException", "Message": "invalid token"}},
        "GetUser",
    )

    with pytest.raises(AuthError):
        await CognitoIdentityVerifier(FakeCognitoClient(error=error)).verify(principal())


@pytest.mark.asyncio
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
async def test_get_user_maps_service_and_transport_failures_to_unavailable(
    error: Exception,
) -> None:
    with pytest.raises(IdentityServiceUnavailableError):
        await CognitoIdentityVerifier(FakeCognitoClient(error=error)).verify(principal())
