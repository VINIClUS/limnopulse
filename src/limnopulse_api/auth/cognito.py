from __future__ import annotations

from dataclasses import dataclass
from time import time
from typing import Any

import jwt
from jwt import PyJWKClient
from fastapi import Request

from limnopulse_api.auth.models import Principal
from limnopulse_api.core.config import Settings
from limnopulse_api.core.errors import AuthError
from limnopulse_api.repositories.cache import CacheRepository


@dataclass
class StaticJwksKeyStore:
    keys_by_kid: dict[str, Any]

    async def get_key(self, kid: str) -> Any:
        key = self.keys_by_kid.get(kid)
        if key is None:
            raise AuthError("unknown jwt key id")
        return key


class JwksKeyStore:
    def __init__(
        self,
        jwks_url: str,
        cache_ttl_seconds: int,
        cache: CacheRepository | None = None,
        cache_key_namespace: str | None = None,
    ) -> None:
        self.client = PyJWKClient(jwks_url, lifespan=cache_ttl_seconds)
        self.cache = cache
        self.cache_key_namespace = cache_key_namespace or jwks_url
        self.cache_ttl_seconds = cache_ttl_seconds

    async def get_key(self, kid: str) -> Any:
        cached_key = await self._get_cached_key(kid)
        if cached_key is not None:
            return cached_key

        try:
            signing_key = self.client.get_signing_key(kid)
        except jwt.PyJWTError as exc:
            raise AuthError("unknown jwt key id") from exc
        await self._cache_signing_key(kid, signing_key)
        return signing_key.key

    async def _get_cached_key(self, kid: str) -> Any | None:
        if self.cache is None:
            return None

        try:
            cached = await self.cache.get_json(self._cache_key(kid))
        except Exception:
            return None

        if not isinstance(cached, dict):
            return None

        try:
            return jwt.PyJWK.from_dict(cached).key
        except jwt.PyJWTError:
            return None

    async def _cache_signing_key(self, kid: str, signing_key: Any) -> None:
        if self.cache is None:
            return

        jwk_data = getattr(signing_key, "_jwk_data", None)
        if not isinstance(jwk_data, dict):
            return

        try:
            await self.cache.set_json(self._cache_key(kid), jwk_data, self.cache_ttl_seconds)
        except Exception:
            pass

    def _cache_key(self, kid: str) -> str:
        return f"jwks:cognito:{self.cache_key_namespace}:{kid}"


class CognitoJwtAuthProvider:
    def __init__(self, settings: Settings, key_store: Any | None = None, leeway_seconds: int = 60) -> None:
        self.settings = settings
        self.key_store = key_store
        self.leeway_seconds = leeway_seconds

    async def authenticate(self, request: Request) -> Principal:
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            raise AuthError("missing bearer token")

        token = auth_header.removeprefix("Bearer ").strip()
        if not token:
            raise AuthError("missing bearer token")

        try:
            header = jwt.get_unverified_header(token)
        except jwt.PyJWTError as exc:
            raise AuthError("invalid jwt header") from exc

        if header.get("alg") != "RS256":
            raise AuthError("unsupported jwt algorithm")

        kid = header.get("kid")
        if not kid:
            raise AuthError("missing jwt key id")

        if self.key_store is None:
            raise AuthError("jwks key store is not configured")

        key = await self.key_store.get_key(kid)
        issuer = self.settings.cognito_issuer or (
            f"https://cognito-idp.{self.settings.aws_region}.amazonaws.com/"
            f"{self.settings.cognito_user_pool_id}"
        )

        try:
            claims = jwt.decode(
                token,
                key=key,
                algorithms=["RS256"],
                issuer=issuer,
                options={"verify_aud": False},
                leeway=self.leeway_seconds,
            )
        except jwt.PyJWTError as exc:
            raise AuthError("invalid cognito token") from exc

        if claims.get("token_use") != "access":
            raise AuthError("token_use must be access")

        client_id = claims.get("client_id") or claims.get("aud")
        if client_id != self.settings.cognito_client_id:
            raise AuthError("invalid cognito client id")

        sub = claims.get("sub")
        if not sub:
            raise AuthError("missing cognito sub")

        if claims.get("exp", 0) < int(time()) - self.leeway_seconds:
            raise AuthError("expired cognito token")

        groups = tuple(claims.get("cognito:groups", ()))
        return Principal(cognito_sub=sub, email=claims.get("email"), groups=groups)


def build_cognito_key_store(
    settings: Settings,
    cache: CacheRepository | None = None,
) -> JwksKeyStore:
    issuer = settings.cognito_issuer or (
        f"https://cognito-idp.{settings.aws_region}.amazonaws.com/"
        f"{settings.cognito_user_pool_id}"
    )
    jwks_url = f"{issuer.rstrip('/')}/.well-known/jwks.json"
    return JwksKeyStore(
        jwks_url=jwks_url,
        cache_ttl_seconds=settings.jwks_cache_ttl_seconds,
        cache=cache,
        cache_key_namespace=settings.cognito_issuer or settings.cognito_user_pool_id or issuer,
    )
