from datetime import UTC, datetime, timedelta

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import Request

from limnopulse_api.auth.cognito import CognitoJwtAuthProvider
from limnopulse_api.auth.providers import build_auth_provider
from limnopulse_api.core.config import Settings
from limnopulse_api.core.errors import AuthError


TEST_ISSUER = "https://cognito-idp.us-east-1.amazonaws.com/pool_1"


class FakeKeyStore:
    def __init__(self, public_key) -> None:
        self.public_key = public_key
        self.calls: list[str] = []

    async def get_key(self, kid: str):
        self.calls.append(kid)
        return self.public_key


def build_request(token: str) -> Request:
    return Request({"type": "http", "headers": [(b"authorization", f"Bearer {token}".encode())]})


def build_token(private_key, claims: dict[str, object]) -> str:
    return jwt.encode(claims, private_key, algorithm="RS256", headers={"kid": "kid-1"})


def base_claims() -> dict[str, object]:
    now = datetime.now(UTC)
    return {
        "iss": TEST_ISSUER,
        "sub": "sub_1",
        "client_id": "client_1",
        "token_use": "access",
        "email": "u@example.test",
        "iat": int(now.timestamp()),
        "nbf": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
    }


@pytest.fixture
def key_pair():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return private_key, private_key.public_key()


@pytest.mark.asyncio
async def test_cognito_rejects_wrong_token_use(key_pair) -> None:
    private_key, public_key = key_pair
    claims = base_claims() | {"token_use": "id"}
    provider = CognitoJwtAuthProvider(
        settings=Settings(
            app_env="test",
            auth_mode="cognito",
            cognito_user_pool_id="pool_1",
            cognito_client_id="client_1",
            cognito_issuer=TEST_ISSUER,
        ),
        key_store=FakeKeyStore(public_key),
    )

    with pytest.raises(AuthError):
        await provider.authenticate(build_request(build_token(private_key, claims)))


@pytest.mark.asyncio
async def test_cognito_rejects_wrong_client_id(key_pair) -> None:
    private_key, public_key = key_pair
    claims = base_claims() | {"client_id": "other_client"}
    provider = CognitoJwtAuthProvider(
        settings=Settings(
            app_env="test",
            auth_mode="cognito",
            cognito_user_pool_id="pool_1",
            cognito_client_id="client_1",
            cognito_issuer=TEST_ISSUER,
        ),
        key_store=FakeKeyStore(public_key),
    )

    with pytest.raises(AuthError):
        await provider.authenticate(build_request(build_token(private_key, claims)))


@pytest.mark.asyncio
async def test_cognito_accepts_valid_access_token(key_pair) -> None:
    private_key, public_key = key_pair
    provider = CognitoJwtAuthProvider(
        settings=Settings(
            app_env="test",
            auth_mode="cognito",
            cognito_user_pool_id="pool_1",
            cognito_client_id="client_1",
            cognito_issuer=TEST_ISSUER,
        ),
        key_store=FakeKeyStore(public_key),
    )

    principal = await provider.authenticate(build_request(build_token(private_key, base_claims())))

    assert principal.cognito_sub == "sub_1"
    assert principal.email == "u@example.test"


@pytest.mark.asyncio
async def test_cognito_factory_builds_working_jwks_provider(key_pair, monkeypatch) -> None:
    private_key, public_key = key_pair
    calls: list[tuple[str, int]] = []

    class FakeSigningKey:
        def __init__(self, key) -> None:
            self.key = key

    class FakePyJWKClient:
        def __init__(self, jwks_url: str, lifespan: int) -> None:
            calls.append((jwks_url, lifespan))

        def get_signing_key(self, kid: str) -> FakeSigningKey:
            assert kid == "kid-1"
            return FakeSigningKey(public_key)

    monkeypatch.setattr("limnopulse_api.auth.cognito.PyJWKClient", FakePyJWKClient)
    settings = Settings(
        app_env="test",
        auth_mode="cognito",
        aws_region="us-east-1",
        cognito_user_pool_id="pool_1",
        cognito_client_id="client_1",
        cognito_issuer=TEST_ISSUER,
        jwks_cache_ttl_seconds=21_600,
    )

    provider = build_auth_provider(settings)
    principal = await provider.authenticate(build_request(build_token(private_key, base_claims())))

    assert principal.cognito_sub == "sub_1"
    assert principal.email == "u@example.test"
    assert calls == [
        ("https://cognito-idp.us-east-1.amazonaws.com/pool_1/.well-known/jwks.json", 21_600)
    ]
