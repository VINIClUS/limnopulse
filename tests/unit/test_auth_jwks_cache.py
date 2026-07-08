import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from limnopulse_api.auth.cognito import JwksKeyStore


class FakeCacheRepository:
    def __init__(self) -> None:
        self.values: dict[str, object] = {}
        self.get_calls: list[str] = []
        self.set_calls: list[tuple[str, object, int]] = []

    async def get_json(self, key: str) -> object | None:
        self.get_calls.append(key)
        return self.values.get(key)

    async def set_json(self, key: str, value: object, ttl_seconds: int) -> None:
        self.set_calls.append((key, value, ttl_seconds))
        self.values[key] = value

    async def delete(self, key: str) -> None:
        self.values.pop(key, None)


@pytest.mark.asyncio
async def test_jwks_key_store_reuses_cached_key_by_kid(monkeypatch) -> None:
    public_key = rsa.generate_private_key(public_exponent=65537, key_size=2048).public_key()
    jwk_data = jwt.algorithms.RSAAlgorithm.to_jwk(public_key, as_dict=True) | {
        "kid": "kid-1",
        "use": "sig",
        "alg": "RS256",
    }
    client_calls: list[tuple[str, str, int] | tuple[str, str]] = []

    class FakePyJWKClient:
        def __init__(self, jwks_url: str, lifespan: int) -> None:
            client_calls.append(("init", jwks_url, lifespan))

        def get_signing_key(self, kid: str):
            client_calls.append(("get", kid))
            return jwt.PyJWK.from_dict(jwk_data)

    monkeypatch.setattr("limnopulse_api.auth.cognito.PyJWKClient", FakePyJWKClient)
    cache = FakeCacheRepository()
    key_store = JwksKeyStore(
        jwks_url="https://issuer.example/.well-known/jwks.json",
        cache_ttl_seconds=21_600,
        cache=cache,
        cache_key_namespace="pool_1",
    )

    first = await key_store.get_key("kid-1")
    second = await key_store.get_key("kid-1")

    assert first.public_numbers() == public_key.public_numbers()
    assert second.public_numbers() == public_key.public_numbers()
    assert client_calls == [
        ("init", "https://issuer.example/.well-known/jwks.json", 21_600),
        ("get", "kid-1"),
    ]
    assert cache.get_calls == [
        "jwks:cognito:pool_1:kid-1",
        "jwks:cognito:pool_1:kid-1",
    ]
    assert cache.set_calls == [
        ("jwks:cognito:pool_1:kid-1", jwk_data, 21_600),
    ]
