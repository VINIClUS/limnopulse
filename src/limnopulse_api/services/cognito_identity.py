from asyncio import to_thread
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Protocol

from botocore.exceptions import BotoCoreError, ClientError
from email_validator import EmailNotValidError, validate_email
from pydantic import BaseModel, ConfigDict

from limnopulse_api.auth.models import Principal
from limnopulse_api.core.errors import (
    AuthError,
    IdentityEmailError,
    IdentityServiceUnavailableError,
)

COGNITO_GET_USER_SCOPE = "aws.cognito.signin.user.admin"


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


class CognitoGetUserClient(Protocol):
    def get_user(self, **kwargs: str) -> dict[str, Any]:
        raise NotImplementedError


class VerifiedEmailIdentity(BaseModel):
    model_config = ConfigDict(frozen=True)

    address: str
    verified: bool
    checked_at: datetime
    identity_source: str


class CognitoIdentityVerifier:
    def __init__(
        self,
        client: CognitoGetUserClient,
        *,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.client = client
        self.clock = clock

    async def verify(self, principal: Principal) -> VerifiedEmailIdentity:
        token = principal.access_token
        if token is None or COGNITO_GET_USER_SCOPE not in principal.scopes:
            raise AuthError("access token cannot authorize Cognito GetUser")

        try:
            response = await to_thread(self.client.get_user, AccessToken=token)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "NotAuthorizedException":
                raise AuthError("invalid Cognito access token") from exc
            raise IdentityServiceUnavailableError("identity service unavailable") from exc
        except BotoCoreError as exc:
            raise IdentityServiceUnavailableError("identity service unavailable") from exc

        attributes = self._attributes(response)
        if attributes.get("sub") != principal.cognito_sub:
            raise AuthError("Cognito subject mismatch")

        email = attributes.get("email")
        if email is None or attributes.get("email_verified", "").lower() != "true":
            raise IdentityEmailError("a verified email address is required")

        try:
            result = validate_email(email, check_deliverability=False)
        except EmailNotValidError as exc:
            raise IdentityEmailError("a valid email address is required") from exc
        if result.smtputf8 or result.ascii_email is None:
            raise IdentityEmailError("an ASCII-compatible email address is required")

        return VerifiedEmailIdentity(
            address=result.ascii_email,
            verified=True,
            checked_at=self.clock(),
            identity_source="cognito_get_user",
        )

    def _attributes(self, response: dict[str, Any]) -> dict[str, str]:
        parsed: dict[str, str] = {}
        for attribute in response.get("UserAttributes", []):
            if not isinstance(attribute, dict):
                continue
            name = attribute.get("Name")
            value = attribute.get("Value")
            if isinstance(name, str) and isinstance(value, str):
                parsed[name] = value
        return parsed


class DevIdentityVerifier:
    """Accept the explicitly supplied local development identity email.

    Development authentication is restricted to local and test environments by
    Settings, so it has no Cognito access token to send to GetUser. It still
    applies the same email syntax and ASCII boundary as the production verifier.
    """

    def __init__(self, *, clock: Callable[[], datetime] = utc_now) -> None:
        self.clock = clock

    async def verify(self, principal: Principal) -> VerifiedEmailIdentity:
        email = principal.email
        if email is None:
            raise IdentityEmailError("a verified email address is required")
        try:
            result = validate_email(email, check_deliverability=False, test_environment=True)
        except EmailNotValidError as exc:
            raise IdentityEmailError("a valid email address is required") from exc
        if result.smtputf8 or result.ascii_email is None:
            raise IdentityEmailError("an ASCII-compatible email address is required")
        return VerifiedEmailIdentity(
            address=result.ascii_email,
            verified=True,
            checked_at=self.clock(),
            identity_source="development_header",
        )
