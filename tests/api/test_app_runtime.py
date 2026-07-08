from datetime import UTC, datetime, timedelta

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import Request
from fastapi.testclient import TestClient
from botocore.exceptions import EndpointConnectionError

from limnopulse_api.adapters.dynamodb import DynamoDomainRepository
from limnopulse_api.adapters.redis import RedisCacheRepository
from limnopulse_api.auth.cognito import CognitoJwtAuthProvider
from limnopulse_api.auth.models import Principal
from limnopulse_api.core.config import Settings
from limnopulse_api.main import create_app
from limnopulse_api.services.memberships import MembershipService


class FakeRedisClient:
    def __init__(self) -> None:
        self.closed = False

    async def aclose(self, close_connection_pool=None) -> None:
        self.closed = True


class FakeAuthProvider:
    def __init__(self) -> None:
        self.calls = 0

    async def authenticate(self, request: Request) -> Principal:
        self.calls += 1
        return Principal(cognito_sub="sub_1", email="u@example.test", groups=())


class FakeMembershipService:
    async def get_active_membership(self, cognito_sub: str, tenant_id: str):
        return None


class FailingMembershipService:
    def __init__(self, error: Exception) -> None:
        self.error = error

    async def get_active_membership(self, cognito_sub: str, tenant_id: str):
        raise self.error


class FakeDomainRepository:
    def __init__(self) -> None:
        self.get_tenant_calls = 0

    async def get_tenant(self, tenant_id: str):
        self.get_tenant_calls += 1
        raise AssertionError("tenant repository should not be reached without membership")


class FakeKeyStore:
    def __init__(self, public_key) -> None:
        self.public_key = public_key

    async def get_key(self, kid: str):
        assert kid == "kid-1"
        return self.public_key


def build_token(private_key) -> str:
    now = datetime.now(UTC)
    claims = {
        "iss": "https://cognito-idp.us-east-1.amazonaws.com/pool_1",
        "sub": "sub_1",
        "client_id": "client_1",
        "token_use": "access",
        "email": "u@example.test",
        "iat": int(now.timestamp()),
        "nbf": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
    }
    return jwt.encode(claims, private_key, algorithm="RS256", headers={"kid": "kid-1"})


def test_app_lifespan_wires_runtime_dependencies(monkeypatch) -> None:
    dynamo_calls: list[dict[str, str | None]] = []
    redis_calls: list[str] = []
    fake_dynamo = object()
    fake_redis = FakeRedisClient()

    def fake_boto3_client(service_name: str, **kwargs):
        dynamo_calls.append({"service_name": service_name, **kwargs})
        return fake_dynamo

    def fake_redis_from_url(url: str):
        redis_calls.append(url)
        return fake_redis

    monkeypatch.setattr("limnopulse_api.main.boto3.client", fake_boto3_client)
    monkeypatch.setattr("limnopulse_api.main.redis.from_url", fake_redis_from_url)
    settings = Settings(
        app_env="test",
        auth_mode="cognito",
        aws_region="us-east-1",
        cognito_user_pool_id="pool_1",
        cognito_client_id="client_1",
        dynamodb_endpoint_url="http://localhost:8000",
        redis_url="redis://localhost:6379/0",
    )

    app = create_app(settings)

    assert dynamo_calls == []
    assert redis_calls == []
    assert not hasattr(app.state, "domain_repository")
    assert not hasattr(app.state, "membership_service")
    assert not hasattr(app.state, "auth_provider")

    with TestClient(app):
        assert dynamo_calls == [
            {
                "service_name": "dynamodb",
                "region_name": "us-east-1",
                "endpoint_url": "http://localhost:8000",
                "aws_access_key_id": "local",
                "aws_secret_access_key": "local",
            }
        ]
        assert redis_calls == ["redis://localhost:6379/0"]
        assert isinstance(app.state.domain_repository, DynamoDomainRepository)
        assert app.state.domain_repository.client is fake_dynamo
        assert isinstance(app.state.cache_repository, RedisCacheRepository)
        assert app.state.cache_repository.redis is fake_redis
        assert isinstance(app.state.membership_service, MembershipService)
        assert app.state.membership_service.domain_repository is app.state.domain_repository
        assert app.state.membership_service.cache is app.state.cache_repository
        assert isinstance(app.state.auth_provider, CognitoJwtAuthProvider)
        assert app.state.auth_provider.key_store.cache is app.state.cache_repository

    assert fake_redis.closed is True


def test_app_lifespan_uses_dummy_credentials_for_local_dynamodb(monkeypatch) -> None:
    dynamo_calls: list[dict[str, str | None]] = []
    fake_dynamo = object()
    fake_redis = FakeRedisClient()

    def fake_boto3_client(service_name: str, **kwargs):
        dynamo_calls.append({"service_name": service_name, **kwargs})
        return fake_dynamo

    monkeypatch.setattr("limnopulse_api.main.boto3.client", fake_boto3_client)
    monkeypatch.setattr("limnopulse_api.main.redis.from_url", lambda url: fake_redis)

    app = create_app(
        Settings(
            app_env="test",
            auth_mode="dev",
            aws_region="us-east-1",
            dynamodb_endpoint_url="http://localhost:8000",
            redis_url="redis://localhost:6379/0",
        )
    )

    with TestClient(app):
        assert dynamo_calls == [
            {
                "service_name": "dynamodb",
                "region_name": "us-east-1",
                "endpoint_url": "http://localhost:8000",
                "aws_access_key_id": "local",
                "aws_secret_access_key": "local",
            }
        ]


def test_me_reuses_app_scoped_auth_provider(monkeypatch) -> None:
    app = create_app(Settings(app_env="test", auth_mode="dev"))
    provider = FakeAuthProvider()
    app.state.auth_provider = provider

    def fail_build_auth_provider(settings, cache=None):
        raise AssertionError("auth provider should be reused from app state")

    monkeypatch.setattr("limnopulse_api.api.dependencies.build_auth_provider", fail_build_auth_provider)

    with TestClient(app) as client:
        first = client.get("/v1/me")
        second = client.get("/v1/me")

    assert first.status_code == 200
    assert second.status_code == 200
    assert provider.calls == 2


def test_cognito_identity_without_membership_gets_403() -> None:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    settings = Settings(
        app_env="test",
        auth_mode="cognito",
        cognito_user_pool_id="pool_1",
        cognito_client_id="client_1",
    )
    app = create_app(settings)
    app.state.domain_repository = FakeDomainRepository()
    app.state.membership_service = FakeMembershipService()
    app.state.auth_provider = CognitoJwtAuthProvider(
        settings=settings,
        key_store=FakeKeyStore(private_key.public_key()),
    )
    token = build_token(private_key)

    with TestClient(app) as client:
        response = client.get("/v1/tenants/tnt_1", headers={"Authorization": f"Bearer {token}"})

    assert response.status_code == 403


def test_membership_infra_failure_returns_503_and_skips_tenant_read() -> None:
    app = create_app(Settings(app_env="test", auth_mode="dev"))
    domain_repository = FakeDomainRepository()
    app.state.domain_repository = domain_repository
    app.state.membership_service = FailingMembershipService(
        EndpointConnectionError(endpoint_url="http://localhost:8000")
    )

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/v1/tenants/tnt_1", headers={"X-Dev-User-Sub": "sub_1"})

    assert response.status_code == 503
    assert response.json() == {"detail": "service unavailable"}
    assert domain_repository.get_tenant_calls == 0
