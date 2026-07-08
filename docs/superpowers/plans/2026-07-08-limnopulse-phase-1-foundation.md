# Limnopulse Phase 1 Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Limnopulse Phase 1 FastAPI foundation with local dev auth, Cognito JWT auth, DynamoDB domain repositories, Redis cache-aside, tenant authorization, CRUD endpoints, local setup, and tests.

**Architecture:** Use a layered FastAPI app: `api -> services -> repositories/adapters`. FastAPI dependencies authenticate identity and enforce tenant membership/role before services run. Repositories hide DynamoDB/Redis details; services enforce domain rules.

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, pydantic-settings, boto3, redis-py, PyJWT, cryptography, httpx, pytest, fakeredis, uvicorn, Docker Compose, DynamoDB Local, Redis.

## Global Constraints

- New implementation artifacts use canonical Limnopulse naming: package `limnopulse_api`, tables `LimnopulseDomain` and `LimnopulseAudit`.
- Do not introduce AquaFarm/Aquafarm names in code, environment examples, compose files, scripts, or new docs except when citing historical documentation.
- `AUTH_MODE=dev` works only when `APP_ENV` is `local` or `test`; app startup/config validation fails for `staging` or `prod`.
- `AUTH_MODE=dev` authenticates identity only; no dev header or group grants tenant access.
- `AUTH_MODE=cognito` uses `CognitoJwtAuthProvider` with JWKS, issuer, expiration, client ID/audience, and `token_use=access` validation.
- Tenant access always requires active DynamoDB membership.
- Owner/admin can write tenants, ponds, and devices. Member/viewer are read-only in Phase 1.
- DynamoDB critical paths use `GetItem`, `Query`, or transactions. No `Scan` in API critical paths.
- Create writes use condition expressions. Patch writes increment `version` and `updated_at`.
- Tenant creation atomically creates tenant, owner membership, and tenant member mirror item.
- Redis is cache-aside only with TTL. It never stores raw JWTs, secrets, or long-lived permissions.
- PostgreSQL, Firebase/Firestore, MQTT, Telegraf, InfluxDB, Go workers, SQS, SES, Telegram, WhatsApp, and SMS are outside Phase 1.
- Prefix shell commands with `rtk` when the wrapper supports the command.

---

## File Structure

Create:

```text
pyproject.toml
.env.example
compose.yaml
tests/conftest.py
tests/unit/test_settings.py
tests/unit/test_auth_dev.py
tests/unit/test_auth_cognito.py
tests/unit/test_domain_repository.py
tests/unit/test_membership_cache.py
tests/api/test_me.py
tests/api/test_tenants.py
tests/api/test_ponds_devices.py
tests/unit/test_no_scan_guard.py
scripts/dev/init_dynamodb.py
scripts/dev/seed_local.py
src/limnopulse_api/__init__.py
src/limnopulse_api/main.py
src/limnopulse_api/core/__init__.py
src/limnopulse_api/core/config.py
src/limnopulse_api/core/errors.py
src/limnopulse_api/api/__init__.py
src/limnopulse_api/api/router.py
src/limnopulse_api/api/dependencies.py
src/limnopulse_api/api/v1/__init__.py
src/limnopulse_api/api/v1/routers/__init__.py
src/limnopulse_api/api/v1/routers/health.py
src/limnopulse_api/api/v1/routers/me.py
src/limnopulse_api/api/v1/routers/tenants.py
src/limnopulse_api/api/v1/routers/ponds.py
src/limnopulse_api/api/v1/routers/devices.py
src/limnopulse_api/api/v1/schemas/__init__.py
src/limnopulse_api/api/v1/schemas/common.py
src/limnopulse_api/api/v1/schemas/me.py
src/limnopulse_api/api/v1/schemas/tenants.py
src/limnopulse_api/api/v1/schemas/ponds.py
src/limnopulse_api/api/v1/schemas/devices.py
src/limnopulse_api/auth/__init__.py
src/limnopulse_api/auth/models.py
src/limnopulse_api/auth/providers.py
src/limnopulse_api/auth/dev.py
src/limnopulse_api/auth/cognito.py
src/limnopulse_api/domain/__init__.py
src/limnopulse_api/domain/entities.py
src/limnopulse_api/domain/roles.py
src/limnopulse_api/domain/ids.py
src/limnopulse_api/repositories/__init__.py
src/limnopulse_api/repositories/domain.py
src/limnopulse_api/repositories/cache.py
src/limnopulse_api/adapters/__init__.py
src/limnopulse_api/adapters/dynamodb.py
src/limnopulse_api/adapters/redis.py
src/limnopulse_api/services/__init__.py
src/limnopulse_api/services/memberships.py
src/limnopulse_api/services/tenants.py
src/limnopulse_api/services/ponds.py
src/limnopulse_api/services/devices.py
```

Modify:

```text
README.md
docs/superpowers/specs/2026-07-08-limnopulse-phase-1-foundation-design.md
```

The spec should only change if implementation exposes a real contradiction or missing constraint. Do not alter architectural scope during implementation.

---

### Task 1: Project Bootstrap And Settings

**Files:**
- Create: `pyproject.toml`
- Create: `.env.example`
- Create: `compose.yaml`
- Create: `src/limnopulse_api/__init__.py`
- Create: `src/limnopulse_api/main.py`
- Create: `src/limnopulse_api/core/__init__.py`
- Create: `src/limnopulse_api/core/config.py`
- Create: `src/limnopulse_api/api/__init__.py`
- Create: `src/limnopulse_api/api/router.py`
- Create: `src/limnopulse_api/api/v1/__init__.py`
- Create: `src/limnopulse_api/api/v1/routers/__init__.py`
- Create: `src/limnopulse_api/api/v1/routers/health.py`
- Test: `tests/unit/test_settings.py`
- Test: `tests/conftest.py`

**Interfaces:**
- Produces: `limnopulse_api.core.config.Settings`
- Produces: `limnopulse_api.core.config.get_settings() -> Settings`
- Produces: `limnopulse_api.main.create_app(settings: Settings | None = None) -> FastAPI`
- Produces: `limnopulse_api.main.app`

- [ ] **Step 1: Write failing settings tests**

Create `tests/unit/test_settings.py`:

```python
import pytest
from pydantic import ValidationError

from limnopulse_api.core.config import Settings


def test_dev_auth_is_allowed_in_local() -> None:
    settings = Settings(app_env="local", auth_mode="dev")
    assert settings.auth_mode == "dev"


def test_dev_auth_is_allowed_in_test() -> None:
    settings = Settings(app_env="test", auth_mode="dev")
    assert settings.auth_mode == "dev"


@pytest.mark.parametrize("app_env", ["staging", "prod"])
def test_dev_auth_is_rejected_outside_local_and_test(app_env: str) -> None:
    with pytest.raises(ValidationError):
        Settings(app_env=app_env, auth_mode="dev")


def test_default_table_names_use_limnopulse() -> None:
    settings = Settings(app_env="test", auth_mode="dev")
    assert settings.dynamodb_domain_table == "LimnopulseDomain"
    assert settings.dynamodb_audit_table == "LimnopulseAudit"
```

- [ ] **Step 2: Add minimal project metadata and dependencies**

Create `pyproject.toml`:

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "limnopulse-api"
version = "0.1.0"
description = "Limnopulse Phase 1 FastAPI foundation"
requires-python = ">=3.12"
dependencies = [
  "boto3>=1.34",
  "cryptography>=42",
  "fastapi>=0.115",
  "httpx>=0.27",
  "pydantic>=2.8",
  "pydantic-settings>=2.4",
  "PyJWT>=2.8",
  "redis>=5",
  "uvicorn[standard]>=0.30",
]

[project.optional-dependencies]
dev = [
  "fakeredis>=2.23",
  "pytest>=8.2",
  "pytest-asyncio>=0.23",
  "pytest-cov>=5",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["src"]

[tool.ruff]
line-length = 100
target-version = "py312"
```

- [ ] **Step 3: Implement settings**

Create `src/limnopulse_api/core/config.py`:

```python
from functools import lru_cache
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


AppEnv = Literal["local", "test", "staging", "prod"]
AuthMode = Literal["dev", "cognito"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: AppEnv = "local"
    auth_mode: AuthMode = "dev"
    aws_region: str = "us-east-1"
    cognito_user_pool_id: str = ""
    cognito_client_id: str = ""
    cognito_issuer: str = ""
    dynamodb_domain_table: str = "LimnopulseDomain"
    dynamodb_audit_table: str = "LimnopulseAudit"
    dynamodb_endpoint_url: str | None = None
    redis_url: str = "redis://localhost:6379/0"
    jwks_cache_ttl_seconds: int = Field(default=43_200, ge=21_600, le=86_400)
    membership_cache_ttl_seconds: int = Field(default=120, ge=60, le=300)
    device_cache_ttl_seconds: int = Field(default=1_800, ge=900, le=3_600)
    tenant_settings_cache_ttl_seconds: int = Field(default=1_800, ge=900, le=3_600)

    @model_validator(mode="after")
    def validate_auth_mode_for_environment(self) -> "Settings":
        if self.auth_mode == "dev" and self.app_env not in {"local", "test"}:
            raise ValueError("AUTH_MODE=dev is only allowed when APP_ENV is local or test")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
```

Create empty package files:

```python
"""Limnopulse API package."""
```

Use that content in `src/limnopulse_api/__init__.py`. Use empty files for package markers under `core`, `api`, `api/v1`, and `api/v1/routers`.

- [ ] **Step 4: Add health router and app factory**

Create `src/limnopulse_api/api/v1/routers/health.py`:

```python
from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/readyz")
def readyz() -> dict[str, str]:
    return {"status": "ready"}
```

Create `src/limnopulse_api/api/router.py`:

```python
from fastapi import APIRouter

from limnopulse_api.api.v1.routers import health

api_router = APIRouter()
api_router.include_router(health.router)
```

Create `src/limnopulse_api/main.py`:

```python
from fastapi import FastAPI

from limnopulse_api.api.router import api_router
from limnopulse_api.core.config import Settings, get_settings


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved_settings = settings or get_settings()
    app = FastAPI(title="Limnopulse API", version="0.1.0")
    app.state.settings = resolved_settings
    app.include_router(api_router)
    return app


app = create_app()
```

- [ ] **Step 5: Add local environment and compose files**

Create `.env.example`:

```dotenv
APP_ENV=local
AUTH_MODE=dev
AWS_REGION=us-east-1
COGNITO_USER_POOL_ID=
COGNITO_CLIENT_ID=
COGNITO_ISSUER=
DYNAMODB_DOMAIN_TABLE=LimnopulseDomain
DYNAMODB_AUDIT_TABLE=LimnopulseAudit
DYNAMODB_ENDPOINT_URL=http://localhost:8001
REDIS_URL=redis://localhost:6379/0
JWKS_CACHE_TTL_SECONDS=43200
MEMBERSHIP_CACHE_TTL_SECONDS=120
DEVICE_CACHE_TTL_SECONDS=1800
TENANT_SETTINGS_CACHE_TTL_SECONDS=1800
```

Create `compose.yaml`:

```yaml
services:
  redis:
    image: redis:7-alpine
    command: redis-server --save "" --appendonly no --maxmemory 128mb --maxmemory-policy allkeys-lfu
    ports:
      - "6379:6379"

  dynamodb-local:
    image: amazon/dynamodb-local:latest
    command: -jar DynamoDBLocal.jar -sharedDb -dbPath ./data
    working_dir: /home/dynamodblocal
    volumes:
      - dynamodb_data:/home/dynamodblocal/data
    ports:
      - "8001:8000"

volumes:
  dynamodb_data:
```

- [ ] **Step 6: Run bootstrap tests**

Run:

```bash
rtk python -m pytest tests/unit/test_settings.py -q
```

Expected: all tests pass.

- [ ] **Step 7: Commit Task 1**

Run:

```bash
rtk git add pyproject.toml .env.example compose.yaml src/limnopulse_api tests/unit/test_settings.py
rtk git commit -m "chore: bootstrap limnopulse api project"
```

---

### Task 2: Auth Providers

**Files:**
- Create: `src/limnopulse_api/auth/__init__.py`
- Create: `src/limnopulse_api/auth/models.py`
- Create: `src/limnopulse_api/auth/providers.py`
- Create: `src/limnopulse_api/auth/dev.py`
- Create: `src/limnopulse_api/auth/cognito.py`
- Create: `src/limnopulse_api/core/errors.py`
- Test: `tests/unit/test_auth_dev.py`
- Test: `tests/unit/test_auth_cognito.py`

**Interfaces:**
- Consumes: `Settings`
- Produces: `Principal(cognito_sub: str, email: str | None, groups: tuple[str, ...])`
- Produces: `PrincipalProvider.authenticate(request: Request) -> Principal`
- Produces: `build_auth_provider(settings: Settings) -> PrincipalProvider`
- Produces: `AuthError`

- [ ] **Step 1: Write dev auth tests**

Create `tests/unit/test_auth_dev.py`:

```python
import pytest
from fastapi import Request

from limnopulse_api.auth.dev import DevAuthProvider
from limnopulse_api.core.errors import AuthError


def build_request(headers: dict[str, str]) -> Request:
    return Request({"type": "http", "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()]})


@pytest.mark.asyncio
async def test_dev_auth_reads_identity_headers() -> None:
    provider = DevAuthProvider()
    principal = await provider.authenticate(
        build_request(
            {
                "X-Dev-User-Sub": "user-1",
                "X-Dev-User-Email": "user@example.test",
                "X-Dev-User-Groups": "ops, support",
            }
        )
    )
    assert principal.cognito_sub == "user-1"
    assert principal.email == "user@example.test"
    assert principal.groups == ("ops", "support")


@pytest.mark.asyncio
async def test_dev_auth_missing_sub_raises_auth_error() -> None:
    provider = DevAuthProvider()
    with pytest.raises(AuthError):
        await provider.authenticate(build_request({}))
```

- [ ] **Step 2: Write Cognito provider tests with generated keys**

Create `tests/unit/test_auth_cognito.py`:

```python
from datetime import UTC, datetime, timedelta

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import Request

from limnopulse_api.auth.cognito import CognitoJwtAuthProvider
from limnopulse_api.core.config import Settings
from limnopulse_api.core.errors import AuthError


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
        "iss": "https://cognito-idp.us-east-1.amazonaws.com/pool_1",
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
        settings=Settings(app_env="test", auth_mode="cognito", cognito_user_pool_id="pool_1", cognito_client_id="client_1"),
        key_store=FakeKeyStore(public_key),
    )
    with pytest.raises(AuthError):
        await provider.authenticate(build_request(build_token(private_key, claims)))


@pytest.mark.asyncio
async def test_cognito_rejects_wrong_client_id(key_pair) -> None:
    private_key, public_key = key_pair
    claims = base_claims() | {"client_id": "other_client"}
    provider = CognitoJwtAuthProvider(
        settings=Settings(app_env="test", auth_mode="cognito", cognito_user_pool_id="pool_1", cognito_client_id="client_1"),
        key_store=FakeKeyStore(public_key),
    )
    with pytest.raises(AuthError):
        await provider.authenticate(build_request(build_token(private_key, claims)))


@pytest.mark.asyncio
async def test_cognito_accepts_valid_access_token(key_pair) -> None:
    private_key, public_key = key_pair
    provider = CognitoJwtAuthProvider(
        settings=Settings(app_env="test", auth_mode="cognito", cognito_user_pool_id="pool_1", cognito_client_id="client_1"),
        key_store=FakeKeyStore(public_key),
    )
    principal = await provider.authenticate(build_request(build_token(private_key, base_claims())))
    assert principal.cognito_sub == "sub_1"
    assert principal.email == "u@example.test"
```

- [ ] **Step 3: Implement auth models and errors**

Create `src/limnopulse_api/core/errors.py`:

```python
class LimnopulseError(Exception):
    """Base application error."""


class AuthError(LimnopulseError):
    """Identity could not be authenticated."""


class AuthorizationError(LimnopulseError):
    """Identity is authenticated but not authorized for the requested tenant/action."""


class NotFoundError(LimnopulseError):
    """Requested resource was not found."""


class ConflictError(LimnopulseError):
    """Conditional write or version conflict."""
```

Create `src/limnopulse_api/auth/models.py`:

```python
from pydantic import BaseModel, ConfigDict


class Principal(BaseModel):
    model_config = ConfigDict(frozen=True)

    cognito_sub: str
    email: str | None = None
    groups: tuple[str, ...] = ()
```

- [ ] **Step 4: Implement provider protocol and factory**

Create `src/limnopulse_api/auth/providers.py`:

```python
from typing import Protocol

from fastapi import Request

from limnopulse_api.auth.models import Principal
from limnopulse_api.core.config import Settings


class PrincipalProvider(Protocol):
    async def authenticate(self, request: Request) -> Principal:
        raise NotImplementedError


def build_auth_provider(settings: Settings) -> PrincipalProvider:
    if settings.auth_mode == "dev":
        from limnopulse_api.auth.dev import DevAuthProvider

        return DevAuthProvider()

    from limnopulse_api.auth.cognito import CognitoJwtAuthProvider

    return CognitoJwtAuthProvider(settings=settings)
```

- [ ] **Step 5: Implement DevAuthProvider**

Create `src/limnopulse_api/auth/dev.py`:

```python
from fastapi import Request

from limnopulse_api.auth.models import Principal
from limnopulse_api.core.errors import AuthError


class DevAuthProvider:
    async def authenticate(self, request: Request) -> Principal:
        sub = request.headers.get("X-Dev-User-Sub")
        if not sub:
            raise AuthError("missing development identity")
        email = request.headers.get("X-Dev-User-Email")
        raw_groups = request.headers.get("X-Dev-User-Groups", "")
        groups = tuple(group.strip() for group in raw_groups.split(",") if group.strip())
        return Principal(cognito_sub=sub, email=email, groups=groups)
```

- [ ] **Step 6: Implement CognitoJwtAuthProvider**

Create `src/limnopulse_api/auth/cognito.py` with:

```python
from __future__ import annotations

from dataclasses import dataclass
from time import time
from typing import Any

import jwt
from fastapi import Request

from limnopulse_api.auth.models import Principal
from limnopulse_api.core.config import Settings
from limnopulse_api.core.errors import AuthError


@dataclass
class StaticJwksKeyStore:
    keys_by_kid: dict[str, Any]

    async def get_key(self, kid: str) -> Any:
        key = self.keys_by_kid.get(kid)
        if key is None:
            raise AuthError("unknown jwt key id")
        return key


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
```

- [ ] **Step 7: Run auth tests**

Run:

```bash
rtk python -m pytest tests/unit/test_auth_dev.py tests/unit/test_auth_cognito.py -q
```

Expected: all tests pass.

- [ ] **Step 8: Commit Task 2**

Run:

```bash
rtk git add src/limnopulse_api/auth src/limnopulse_api/core/errors.py tests/unit/test_auth_dev.py tests/unit/test_auth_cognito.py
rtk git commit -m "feat: add principal auth providers"
```

---

### Task 3: Domain Entities, Roles, And Repository Contract

**Files:**
- Create: `src/limnopulse_api/domain/__init__.py`
- Create: `src/limnopulse_api/domain/roles.py`
- Create: `src/limnopulse_api/domain/ids.py`
- Create: `src/limnopulse_api/domain/entities.py`
- Create: `src/limnopulse_api/repositories/__init__.py`
- Create: `src/limnopulse_api/repositories/domain.py`
- Test: `tests/unit/test_domain_repository.py`

**Interfaces:**
- Produces: `TenantRole` enum.
- Produces: `Membership`, `Tenant`, `Pond`, `Device`, `TenantAccess`.
- Produces: `DomainRepository` protocol with async repository methods.

- [ ] **Step 1: Write repository key tests**

Create `tests/unit/test_domain_repository.py` initial tests:

```python
from limnopulse_api.adapters.dynamodb import DynamoKeyBuilder


def test_key_builder_uses_limnopulse_domain_shapes() -> None:
    keys = DynamoKeyBuilder()
    assert keys.tenant("tnt_1") == {"PK": "TENANT#tnt_1", "SK": "META"}
    assert keys.pond("tnt_1", "pond_1") == {"PK": "TENANT#tnt_1", "SK": "POND#pond_1"}
    assert keys.device("tnt_1", "dev_1") == {"PK": "TENANT#tnt_1", "SK": "DEVICE#dev_1"}
    assert keys.device_lookup("dev_1") == {"PK": "DEVICE#dev_1", "SK": "META"}
    assert keys.membership("sub_1", "tnt_1") == {"PK": "USER#sub_1", "SK": "TENANT#tnt_1"}
    assert keys.tenant_member("tnt_1", "sub_1") == {"PK": "TENANT#tnt_1", "SK": "MEMBER#sub_1"}
```

- [ ] **Step 2: Implement role and entity models**

Create `src/limnopulse_api/domain/roles.py`:

```python
from enum import StrEnum


class TenantRole(StrEnum):
    OWNER = "owner"
    ADMIN = "admin"
    MEMBER = "member"
    VIEWER = "viewer"


READ_ROLES = frozenset({TenantRole.OWNER, TenantRole.ADMIN, TenantRole.MEMBER, TenantRole.VIEWER})
WRITE_ROLES = frozenset({TenantRole.OWNER, TenantRole.ADMIN})
```

Create `src/limnopulse_api/domain/entities.py`:

```python
from datetime import datetime

from pydantic import BaseModel, ConfigDict

from limnopulse_api.auth.models import Principal
from limnopulse_api.domain.roles import TenantRole


class VersionedEntity(BaseModel):
    model_config = ConfigDict(frozen=True)

    created_at: datetime
    updated_at: datetime
    version: int
    schema_version: int = 1
    status: str = "active"


class Tenant(VersionedEntity):
    tenant_id: str
    name: str
    settings: dict[str, object] = {}


class Pond(VersionedEntity):
    tenant_id: str
    pond_id: str
    name: str
    description: str | None = None


class Device(VersionedEntity):
    tenant_id: str
    pond_id: str
    device_id: str
    name: str
    auth_type: str = "mtls"
    firmware_version: str | None = None


class Membership(VersionedEntity):
    tenant_id: str
    cognito_sub: str
    role: TenantRole


class TenantAccess(BaseModel):
    model_config = ConfigDict(frozen=True)

    principal: Principal
    membership: Membership
```

Create `src/limnopulse_api/domain/ids.py`:

```python
from uuid import uuid4


def new_tenant_id() -> str:
    return f"tnt_{uuid4().hex}"


def new_pond_id() -> str:
    return f"pond_{uuid4().hex}"


def new_device_id() -> str:
    return f"dev_{uuid4().hex}"
```

- [ ] **Step 3: Implement repository protocol**

Create `src/limnopulse_api/repositories/domain.py`:

```python
from typing import Protocol

from limnopulse_api.domain.entities import Device, Membership, Pond, Tenant
from limnopulse_api.domain.roles import TenantRole


class DomainRepository(Protocol):
    async def get_membership(self, cognito_sub: str, tenant_id: str) -> Membership | None:
        raise NotImplementedError

    async def list_memberships_for_user(self, cognito_sub: str) -> list[Membership]:
        raise NotImplementedError

    async def create_tenant_with_owner(self, tenant_id: str, name: str, owner_sub: str) -> Tenant:
        raise NotImplementedError

    async def list_tenants_for_memberships(self, memberships: list[Membership]) -> list[Tenant]:
        raise NotImplementedError

    async def get_tenant(self, tenant_id: str) -> Tenant | None:
        raise NotImplementedError

    async def update_tenant(self, tenant_id: str, expected_version: int, name: str | None) -> Tenant:
        raise NotImplementedError

    async def list_ponds(self, tenant_id: str) -> list[Pond]:
        raise NotImplementedError

    async def get_pond(self, tenant_id: str, pond_id: str) -> Pond | None:
        raise NotImplementedError

    async def create_pond(self, tenant_id: str, pond_id: str, name: str, description: str | None) -> Pond:
        raise NotImplementedError

    async def update_pond(self, tenant_id: str, pond_id: str, expected_version: int, name: str | None, description: str | None) -> Pond:
        raise NotImplementedError

    async def list_devices(self, tenant_id: str) -> list[Device]:
        raise NotImplementedError

    async def get_device(self, tenant_id: str, device_id: str) -> Device | None:
        raise NotImplementedError

    async def create_device(self, tenant_id: str, pond_id: str, device_id: str, name: str, firmware_version: str | None) -> Device:
        raise NotImplementedError

    async def update_device(self, tenant_id: str, device_id: str, expected_version: int, name: str | None, pond_id: str | None, firmware_version: str | None) -> Device:
        raise NotImplementedError
```

- [ ] **Step 4: Add DynamoKeyBuilder shell so tests import**

Create `src/limnopulse_api/adapters/__init__.py` as an empty package marker.

Create `src/limnopulse_api/adapters/dynamodb.py` with just `DynamoKeyBuilder`:

```python
class DynamoKeyBuilder:
    def tenant(self, tenant_id: str) -> dict[str, str]:
        return {"PK": f"TENANT#{tenant_id}", "SK": "META"}

    def pond(self, tenant_id: str, pond_id: str) -> dict[str, str]:
        return {"PK": f"TENANT#{tenant_id}", "SK": f"POND#{pond_id}"}

    def device(self, tenant_id: str, device_id: str) -> dict[str, str]:
        return {"PK": f"TENANT#{tenant_id}", "SK": f"DEVICE#{device_id}"}

    def device_lookup(self, device_id: str) -> dict[str, str]:
        return {"PK": f"DEVICE#{device_id}", "SK": "META"}

    def membership(self, cognito_sub: str, tenant_id: str) -> dict[str, str]:
        return {"PK": f"USER#{cognito_sub}", "SK": f"TENANT#{tenant_id}"}

    def tenant_member(self, tenant_id: str, cognito_sub: str) -> dict[str, str]:
        return {"PK": f"TENANT#{tenant_id}", "SK": f"MEMBER#{cognito_sub}"}
```

- [ ] **Step 5: Run domain model tests**

Run:

```bash
rtk python -m pytest tests/unit/test_domain_repository.py -q
```

Expected: pass.

- [ ] **Step 6: Commit Task 3**

Run:

```bash
rtk git add src/limnopulse_api/domain src/limnopulse_api/repositories src/limnopulse_api/adapters tests/unit/test_domain_repository.py
rtk git commit -m "feat: define domain repository contracts"
```

---

### Task 4: DynamoDB Domain Repository Adapter

**Files:**
- Modify: `src/limnopulse_api/adapters/dynamodb.py`
- Modify: `tests/unit/test_domain_repository.py`

**Interfaces:**
- Consumes: `DomainRepository`
- Produces: `DynamoDomainRepository(table_name: str, dynamodb_resource: Any)`
- Produces: mapper functions from DynamoDB items to domain entities.

- [ ] **Step 1: Extend tests for create tenant transaction and no scan**

Add to `tests/unit/test_domain_repository.py`:

```python
from datetime import UTC, datetime

import pytest

from limnopulse_api.adapters.dynamodb import DynamoDomainRepository


class RecordingDynamoClient:
    def __init__(self) -> None:
        self.transact_write_items_calls = []
        self.scan_calls = 0

    def transact_write_items(self, **kwargs):
        self.transact_write_items_calls.append(kwargs)
        return {}

    def scan(self, **kwargs):
        self.scan_calls += 1
        return {}


@pytest.mark.asyncio
async def test_create_tenant_with_owner_uses_transaction() -> None:
    client = RecordingDynamoClient()
    repo = DynamoDomainRepository(table_name="LimnopulseDomain", client=client)
    tenant = await repo.create_tenant_with_owner("tnt_1", "Demo", "sub_1")
    assert tenant.tenant_id == "tnt_1"
    assert len(client.transact_write_items_calls) == 1
    items = client.transact_write_items_calls[0]["TransactItems"]
    assert len(items) == 3
    assert all("ConditionExpression" in item["Put"] for item in items)
    assert client.scan_calls == 0
```

- [ ] **Step 2: Implement repository transaction helpers**

Extend `src/limnopulse_api/adapters/dynamodb.py` with:

```python
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.entities import Device, Membership, Pond, Tenant
from limnopulse_api.domain.roles import TenantRole


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


class DynamoDomainRepository:
    def __init__(self, table_name: str, client: Any) -> None:
        self.table_name = table_name
        self.client = client
        self.keys = DynamoKeyBuilder()

    async def create_tenant_with_owner(self, tenant_id: str, name: str, owner_sub: str) -> Tenant:
        now = utc_now()
        tenant_item = {
            **self.keys.tenant(tenant_id),
            "entity_type": "tenant",
            "tenant_id": tenant_id,
            "name": name,
            "settings": {},
            "status": "active",
            "role": None,
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "version": 1,
            "schema_version": 1,
        }
        membership_item = {
            **self.keys.membership(owner_sub, tenant_id),
            "entity_type": "membership",
            "tenant_id": tenant_id,
            "cognito_sub": owner_sub,
            "role": TenantRole.OWNER.value,
            "status": "active",
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "version": 1,
            "schema_version": 1,
        }
        tenant_member_item = {
            **self.keys.tenant_member(tenant_id, owner_sub),
            "entity_type": "tenant_member",
            "tenant_id": tenant_id,
            "cognito_sub": owner_sub,
            "role": TenantRole.OWNER.value,
            "status": "active",
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "version": 1,
            "schema_version": 1,
        }
        self.client.transact_write_items(
            TransactItems=[
                self._conditioned_put(tenant_item),
                self._conditioned_put(membership_item),
                self._conditioned_put(tenant_member_item),
            ]
        )
        return self._tenant_from_item(tenant_item)

    def _conditioned_put(self, item: dict[str, Any]) -> dict[str, Any]:
        return {
            "Put": {
                "TableName": self.table_name,
                "Item": item,
                "ConditionExpression": "attribute_not_exists(PK) AND attribute_not_exists(SK)",
            }
        }

    def _tenant_from_item(self, item: dict[str, Any]) -> Tenant:
        return Tenant(
            tenant_id=item["tenant_id"],
            name=item["name"],
            settings=item.get("settings", {}),
            status=item["status"],
            created_at=datetime.fromisoformat(item["created_at"]),
            updated_at=datetime.fromisoformat(item["updated_at"]),
            version=int(item["version"]),
            schema_version=int(item.get("schema_version", 1)),
        )
```

Keep the previously defined `DynamoKeyBuilder` class in the same module.

- [ ] **Step 3: Add get/query/update methods in the same adapter**

Add methods in `DynamoDomainRepository`:

```python
async def get_membership(self, cognito_sub: str, tenant_id: str) -> Membership | None
async def list_memberships_for_user(self, cognito_sub: str) -> list[Membership]
async def list_tenants_for_memberships(self, memberships: list[Membership]) -> list[Tenant]
async def get_tenant(self, tenant_id: str) -> Tenant | None
async def update_tenant(self, tenant_id: str, expected_version: int, name: str | None) -> Tenant
async def list_ponds(self, tenant_id: str) -> list[Pond]
async def get_pond(self, tenant_id: str, pond_id: str) -> Pond | None
async def create_pond(self, tenant_id: str, pond_id: str, name: str, description: str | None) -> Pond
async def update_pond(self, tenant_id: str, pond_id: str, expected_version: int, name: str | None, description: str | None) -> Pond
async def list_devices(self, tenant_id: str) -> list[Device]
async def get_device(self, tenant_id: str, device_id: str) -> Device | None
async def create_device(self, tenant_id: str, pond_id: str, device_id: str, name: str, firmware_version: str | None) -> Device
async def update_device(self, tenant_id: str, device_id: str, expected_version: int, name: str | None, pond_id: str | None, firmware_version: str | None) -> Device
```

Implementation requirements for these methods:

- Use `client.get_item`, `client.query`, `client.put_item`, `client.update_item`, or `client.transact_write_items`.
- Do not call or expose `scan`.
- Query list methods by exact `PK` and `begins_with(SK, ...)`.
- Map conditional failures to `ConflictError`.
- Return `None` for missing reads.
- Raise `NotFoundError` from patch operations when update returns no item.

- [ ] **Step 4: Run repository tests**

Run:

```bash
rtk python -m pytest tests/unit/test_domain_repository.py -q
```

Expected: pass.

- [ ] **Step 5: Commit Task 4**

Run:

```bash
rtk git add src/limnopulse_api/adapters/dynamodb.py tests/unit/test_domain_repository.py
rtk git commit -m "feat: add dynamodb domain repository"
```

---

### Task 5: Redis Cache And Membership Service

**Files:**
- Create: `src/limnopulse_api/repositories/cache.py`
- Create: `src/limnopulse_api/adapters/redis.py`
- Create: `src/limnopulse_api/services/memberships.py`
- Test: `tests/unit/test_membership_cache.py`

**Interfaces:**
- Consumes: `DomainRepository.get_membership`
- Produces: `CacheRepository.get_json(key: str) -> dict | list | None`
- Produces: `CacheRepository.set_json(key: str, value: object, ttl_seconds: int) -> None`
- Produces: `MembershipService.get_active_membership(cognito_sub, tenant_id) -> Membership | None`

- [ ] **Step 1: Write cache-aside tests**

Create `tests/unit/test_membership_cache.py`:

```python
from datetime import UTC, datetime

import pytest

from limnopulse_api.domain.entities import Membership
from limnopulse_api.domain.roles import TenantRole
from limnopulse_api.services.memberships import MembershipService


class FakeCache:
    def __init__(self) -> None:
        self.values = {}
        self.set_calls = []

    async def get_json(self, key: str):
        return self.values.get(key)

    async def set_json(self, key: str, value, ttl_seconds: int) -> None:
        self.values[key] = value
        self.set_calls.append((key, ttl_seconds))


class FakeDomainRepository:
    def __init__(self, membership: Membership | None) -> None:
        self.membership = membership
        self.get_membership_calls = 0

    async def get_membership(self, cognito_sub: str, tenant_id: str):
        self.get_membership_calls += 1
        return self.membership


def active_membership() -> Membership:
    now = datetime.now(UTC)
    return Membership(
        tenant_id="tnt_1",
        cognito_sub="sub_1",
        role=TenantRole.OWNER,
        status="active",
        created_at=now,
        updated_at=now,
        version=1,
    )


@pytest.mark.asyncio
async def test_membership_cache_miss_reads_dynamodb_and_sets_short_ttl() -> None:
    repo = FakeDomainRepository(active_membership())
    cache = FakeCache()
    service = MembershipService(repo, cache, membership_ttl_seconds=120)
    result = await service.get_active_membership("sub_1", "tnt_1")
    assert result is not None
    assert repo.get_membership_calls == 1
    assert cache.set_calls == [("user:sub_1:memberships", 120)]


@pytest.mark.asyncio
async def test_inactive_membership_is_not_authorized() -> None:
    membership = active_membership().model_copy(update={"status": "disabled"})
    service = MembershipService(FakeDomainRepository(membership), FakeCache(), membership_ttl_seconds=120)
    assert await service.get_active_membership("sub_1", "tnt_1") is None
```

- [ ] **Step 2: Implement cache protocol and Redis adapter**

Create `src/limnopulse_api/repositories/cache.py`:

```python
from typing import Protocol


class CacheRepository(Protocol):
    async def get_json(self, key: str) -> object | None:
        raise NotImplementedError

    async def set_json(self, key: str, value: object, ttl_seconds: int) -> None:
        raise NotImplementedError

    async def delete(self, key: str) -> None:
        raise NotImplementedError
```

Create `src/limnopulse_api/adapters/redis.py`:

```python
import json
from typing import Any


class RedisCacheRepository:
    def __init__(self, redis_client: Any) -> None:
        self.redis = redis_client

    async def get_json(self, key: str) -> object | None:
        raw = await self.redis.get(key)
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return json.loads(raw)

    async def set_json(self, key: str, value: object, ttl_seconds: int) -> None:
        await self.redis.set(key, json.dumps(value, default=str), ex=ttl_seconds)

    async def delete(self, key: str) -> None:
        await self.redis.delete(key)
```

- [ ] **Step 3: Implement MembershipService**

Create `src/limnopulse_api/services/memberships.py`:

```python
from limnopulse_api.domain.entities import Membership
from limnopulse_api.domain.roles import TenantRole
from limnopulse_api.repositories.cache import CacheRepository
from limnopulse_api.repositories.domain import DomainRepository


class MembershipService:
    def __init__(
        self,
        domain_repository: DomainRepository,
        cache: CacheRepository | None,
        membership_ttl_seconds: int,
    ) -> None:
        self.domain_repository = domain_repository
        self.cache = cache
        self.membership_ttl_seconds = membership_ttl_seconds

    async def get_active_membership(self, cognito_sub: str, tenant_id: str) -> Membership | None:
        cache_key = f"user:{cognito_sub}:memberships"
        if self.cache is not None:
            cached = await self.cache.get_json(cache_key)
            membership = self._membership_from_cached(cached, tenant_id)
            if membership is not None:
                return membership

        membership = await self.domain_repository.get_membership(cognito_sub, tenant_id)
        if membership is None or membership.status != "active":
            return None
        if self.cache is not None:
            await self.cache.set_json(cache_key, [membership.model_dump(mode="json")], self.membership_ttl_seconds)
        return membership

    def _membership_from_cached(self, cached: object | None, tenant_id: str) -> Membership | None:
        if not isinstance(cached, list):
            return None
        for item in cached:
            if isinstance(item, dict) and item.get("tenant_id") == tenant_id and item.get("status") == "active":
                return Membership.model_validate(item)
        return None
```

- [ ] **Step 4: Run membership cache tests**

Run:

```bash
rtk python -m pytest tests/unit/test_membership_cache.py -q
```

Expected: pass.

- [ ] **Step 5: Commit Task 5**

Run:

```bash
rtk git add src/limnopulse_api/repositories/cache.py src/limnopulse_api/adapters/redis.py src/limnopulse_api/services/memberships.py tests/unit/test_membership_cache.py
rtk git commit -m "feat: add membership cache service"
```

---

### Task 6: FastAPI Dependencies And Me/Tenant Routes

**Files:**
- Create: `src/limnopulse_api/api/dependencies.py`
- Create: `src/limnopulse_api/api/v1/schemas/__init__.py`
- Create: `src/limnopulse_api/api/v1/schemas/common.py`
- Create: `src/limnopulse_api/api/v1/schemas/me.py`
- Create: `src/limnopulse_api/api/v1/schemas/tenants.py`
- Create: `src/limnopulse_api/api/v1/routers/me.py`
- Create: `src/limnopulse_api/api/v1/routers/tenants.py`
- Create: `src/limnopulse_api/services/tenants.py`
- Modify: `src/limnopulse_api/api/router.py`
- Test: `tests/api/test_me.py`
- Test: `tests/api/test_tenants.py`

**Interfaces:**
- Consumes: `PrincipalProvider`, `MembershipService`, `DomainRepository`
- Produces: `get_current_principal`
- Produces: `require_tenant_access`
- Produces: `require_tenant_role`
- Produces: `/v1/me`, `/v1/tenants`, `/v1/tenants/{tenant_id}`

- [ ] **Step 1: Write API tests for identity and tenant authorization**

Create `tests/api/test_me.py`:

```python
from fastapi.testclient import TestClient

from limnopulse_api.core.config import Settings
from limnopulse_api.main import create_app


def test_me_without_identity_returns_401() -> None:
    app = create_app(Settings(app_env="test", auth_mode="dev"))
    client = TestClient(app)
    response = client.get("/v1/me")
    assert response.status_code == 401


def test_me_with_dev_identity_returns_principal() -> None:
    app = create_app(Settings(app_env="test", auth_mode="dev"))
    client = TestClient(app)
    response = client.get("/v1/me", headers={"X-Dev-User-Sub": "sub_1", "X-Dev-User-Email": "u@example.test"})
    assert response.status_code == 200
    assert response.json()["cognito_sub"] == "sub_1"
```

Create `tests/api/test_tenants.py` with concrete fake repository and membership service classes. The file must contain these test functions:

```python
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
    response = client.patch("/v1/tenants/tnt_1", json={"name": "New", "expected_version": 1}, headers={"X-Dev-User-Sub": "sub_1"})
    assert response.status_code == 403


def test_admin_can_patch_tenant() -> None:
    app = app_with_membership(role=TenantRole.ADMIN)
    client = TestClient(app)
    response = client.patch("/v1/tenants/tnt_1", json={"name": "New", "expected_version": 1}, headers={"X-Dev-User-Sub": "sub_1"})
    assert response.status_code == 200
    assert response.json()["name"] == "New"
```

The helper `app_with_membership(role: TenantRole)` should set `app.state.domain_repository` and `app.state.membership_service` directly so the test proves route authorization without external services.

- [ ] **Step 2: Implement schemas**

Create schema files with Pydantic models:

```python
from pydantic import BaseModel, Field


class VersionedResponse(BaseModel):
    created_at: str
    updated_at: str
    version: int
    status: str


class ErrorResponse(BaseModel):
    detail: str
```

Tenant schemas:

```python
class TenantCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)


class TenantUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=120)
    expected_version: int


class TenantResponse(VersionedResponse):
    tenant_id: str
    name: str


class TenantListResponse(BaseModel):
    items: list[TenantResponse]
```

Me schema:

```python
class MeResponse(BaseModel):
    cognito_sub: str
    email: str | None = None
    groups: tuple[str, ...] = ()
```

- [ ] **Step 3: Implement dependencies**

Create `src/limnopulse_api/api/dependencies.py`:

```python
from collections.abc import Callable
from typing import Annotated

from fastapi import Depends, HTTPException, Request

from limnopulse_api.auth.models import Principal
from limnopulse_api.auth.providers import build_auth_provider
from limnopulse_api.core.config import Settings, get_settings
from limnopulse_api.core.errors import AuthError, AuthorizationError
from limnopulse_api.domain.entities import TenantAccess
from limnopulse_api.domain.roles import TenantRole


async def get_current_principal(request: Request) -> Principal:
    settings: Settings = request.app.state.settings
    provider = build_auth_provider(settings)
    try:
        return await provider.authenticate(request)
    except AuthError as exc:
        raise HTTPException(status_code=401, detail="authentication required") from exc


PrincipalDep = Annotated[Principal, Depends(get_current_principal)]


def get_domain_repository(request: Request):
    return request.app.state.domain_repository


def get_membership_service(request: Request):
    return request.app.state.membership_service


def require_tenant_role(*allowed_roles: TenantRole):
    async def dependency(
        tenant_id: str,
        principal: PrincipalDep,
        membership_service=Depends(get_membership_service),
    ) -> TenantAccess:
        membership = await membership_service.get_active_membership(principal.cognito_sub, tenant_id)
        if membership is None or membership.role not in allowed_roles:
            raise HTTPException(status_code=403, detail="tenant access denied")
        return TenantAccess(principal=principal, membership=membership)

    return dependency
```

- [ ] **Step 4: Implement tenant service and routers**

Create `src/limnopulse_api/services/tenants.py`:

```python
from limnopulse_api.domain.entities import Membership, Tenant
from limnopulse_api.domain.ids import new_tenant_id
from limnopulse_api.repositories.domain import DomainRepository


class TenantService:
    def __init__(self, repository: DomainRepository) -> None:
        self.repository = repository

    async def list_for_user(self, cognito_sub: str) -> list[Tenant]:
        memberships = await self.repository.list_memberships_for_user(cognito_sub)
        active = [membership for membership in memberships if membership.status == "active"]
        return await self.repository.list_tenants_for_memberships(active)

    async def create(self, name: str, owner_sub: str) -> Tenant:
        return await self.repository.create_tenant_with_owner(new_tenant_id(), name, owner_sub)

    async def get(self, tenant_id: str) -> Tenant | None:
        return await self.repository.get_tenant(tenant_id)

    async def update(self, tenant_id: str, expected_version: int, name: str | None) -> Tenant:
        return await self.repository.update_tenant(tenant_id, expected_version, name)
```

Create routers for `/v1/me` and `/v1/tenants`, mapping `NotFoundError` to `404` and `ConflictError` to `409`.

- [ ] **Step 5: Wire routers into app**

Modify `src/limnopulse_api/api/router.py`:

```python
from fastapi import APIRouter

from limnopulse_api.api.v1.routers import health, me, tenants

api_router = APIRouter()
api_router.include_router(health.router)
api_router.include_router(me.router, prefix="/v1")
api_router.include_router(tenants.router, prefix="/v1")
```

- [ ] **Step 6: Run API tests**

Run:

```bash
rtk python -m pytest tests/api/test_me.py tests/api/test_tenants.py -q
```

Expected: pass.

- [ ] **Step 7: Commit Task 6**

Run:

```bash
rtk git add src/limnopulse_api/api src/limnopulse_api/services/tenants.py tests/api/test_me.py tests/api/test_tenants.py
rtk git commit -m "feat: add tenant api authorization"
```

---

### Task 7: Pond And Device Routes

**Files:**
- Create: `src/limnopulse_api/api/v1/schemas/ponds.py`
- Create: `src/limnopulse_api/api/v1/schemas/devices.py`
- Create: `src/limnopulse_api/api/v1/routers/ponds.py`
- Create: `src/limnopulse_api/api/v1/routers/devices.py`
- Create: `src/limnopulse_api/services/ponds.py`
- Create: `src/limnopulse_api/services/devices.py`
- Modify: `src/limnopulse_api/api/router.py`
- Test: `tests/api/test_ponds_devices.py`

**Interfaces:**
- Consumes: `require_tenant_role`
- Produces: pond/device CRUD endpoints under `/v1/tenants/{tenant_id}`

- [ ] **Step 1: Write API tests for read/write role behavior**

Create `tests/api/test_ponds_devices.py` with fake repository state and these concrete test functions:

```python
def test_viewer_can_list_ponds_but_cannot_create() -> None:
    app = app_with_membership(role=TenantRole.VIEWER)
    client = TestClient(app)
    headers = {"X-Dev-User-Sub": "sub_1"}
    assert client.get("/v1/tenants/tnt_1/ponds", headers=headers).status_code == 200
    assert client.post("/v1/tenants/tnt_1/ponds", json={"name": "North"}, headers=headers).status_code == 403


def test_member_can_list_devices_but_cannot_patch() -> None:
    app = app_with_membership(role=TenantRole.MEMBER)
    client = TestClient(app)
    headers = {"X-Dev-User-Sub": "sub_1"}
    assert client.get("/v1/tenants/tnt_1/devices", headers=headers).status_code == 200
    assert client.patch("/v1/tenants/tnt_1/devices/dev_1", json={"expected_version": 1, "name": "Probe"}, headers=headers).status_code == 403


def test_admin_can_create_and_patch_pond() -> None:
    app = app_with_membership(role=TenantRole.ADMIN)
    client = TestClient(app)
    headers = {"X-Dev-User-Sub": "sub_1"}
    assert client.post("/v1/tenants/tnt_1/ponds", json={"name": "North"}, headers=headers).status_code == 201
    assert client.patch("/v1/tenants/tnt_1/ponds/pond_1", json={"expected_version": 1, "name": "South"}, headers=headers).status_code == 200


def test_owner_can_create_and_patch_device() -> None:
    app = app_with_membership(role=TenantRole.OWNER)
    client = TestClient(app)
    headers = {"X-Dev-User-Sub": "sub_1"}
    assert client.post("/v1/tenants/tnt_1/devices", json={"pond_id": "pond_1", "name": "Probe"}, headers=headers).status_code == 201
    assert client.patch("/v1/tenants/tnt_1/devices/dev_1", json={"expected_version": 1, "name": "Probe 2"}, headers=headers).status_code == 200


def test_user_without_membership_gets_403() -> None:
    app = app_without_membership()
    client = TestClient(app)
    assert client.get("/v1/tenants/tnt_1/ponds", headers={"X-Dev-User-Sub": "sub_1"}).status_code == 403
```

- [ ] **Step 2: Implement pond and device schemas**

Pond schemas:

```python
class PondCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    description: str | None = None


class PondUpdate(BaseModel):
    expected_version: int
    name: str | None = Field(default=None, min_length=1, max_length=120)
    description: str | None = None


class PondResponse(VersionedResponse):
    tenant_id: str
    pond_id: str
    name: str
    description: str | None = None


class PondListResponse(BaseModel):
    items: list[PondResponse]
```

Device schemas:

```python
class DeviceCreate(BaseModel):
    pond_id: str
    name: str = Field(min_length=1, max_length=120)
    firmware_version: str | None = None


class DeviceUpdate(BaseModel):
    expected_version: int
    pond_id: str | None = None
    name: str | None = Field(default=None, min_length=1, max_length=120)
    firmware_version: str | None = None


class DeviceResponse(VersionedResponse):
    tenant_id: str
    pond_id: str
    device_id: str
    name: str
    auth_type: str
    firmware_version: str | None = None


class DeviceListResponse(BaseModel):
    items: list[DeviceResponse]
```

- [ ] **Step 3: Implement pond and device services**

Create `PondService` and `DeviceService` using repository methods and generated IDs. Do not perform membership lookup in services.

- [ ] **Step 4: Implement routers with role dependencies**

Rules:

- List/get endpoints use `require_tenant_role(*READ_ROLES)`.
- Create/patch endpoints use `require_tenant_role(*WRITE_ROLES)`.
- Router functions depend on `TenantAccess` even if they do not use it directly, so authz cannot be skipped.

- [ ] **Step 5: Wire routers**

Modify `api/router.py` to include ponds and devices routers with prefix `/v1`.

- [ ] **Step 6: Run pond/device tests**

Run:

```bash
rtk python -m pytest tests/api/test_ponds_devices.py -q
```

Expected: pass.

- [ ] **Step 7: Commit Task 7**

Run:

```bash
rtk git add src/limnopulse_api/api/v1/schemas/ponds.py src/limnopulse_api/api/v1/schemas/devices.py src/limnopulse_api/api/v1/routers/ponds.py src/limnopulse_api/api/v1/routers/devices.py src/limnopulse_api/services/ponds.py src/limnopulse_api/services/devices.py tests/api/test_ponds_devices.py
rtk git commit -m "feat: add pond and device endpoints"
```

---

### Task 8: Local DynamoDB Scripts And README

**Files:**
- Create: `scripts/dev/init_dynamodb.py`
- Create: `scripts/dev/seed_local.py`
- Modify: `README.md`

**Interfaces:**
- Consumes: Settings values from environment.
- Produces: local table creation and seed data.

- [ ] **Step 1: Create DynamoDB initialization script**

Create `scripts/dev/init_dynamodb.py`:

```python
from limnopulse_api.core.config import get_settings
import boto3


def main() -> None:
    settings = get_settings()
    client = boto3.client(
        "dynamodb",
        region_name=settings.aws_region,
        endpoint_url=settings.dynamodb_endpoint_url,
        aws_access_key_id="local",
        aws_secret_access_key="local",
    )
    existing = client.list_tables()["TableNames"]
    if settings.dynamodb_domain_table not in existing:
        client.create_table(
            TableName=settings.dynamodb_domain_table,
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        client.get_waiter("table_exists").wait(TableName=settings.dynamodb_domain_table)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Create seed script**

Create `scripts/dev/seed_local.py` to call `DynamoDomainRepository.create_tenant_with_owner("tnt_local_001", "Local Tenant", "local-user-001")`. Catch conflict errors and print that seed already exists.

- [ ] **Step 3: Update README**

Replace `README.md` content with:

```markdown
# Limnopulse

Phase 1 FastAPI foundation for Limnopulse.

## Local Setup

```bash
cp .env.example .env
python -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
docker compose up -d redis dynamodb-local
python scripts/dev/init_dynamodb.py
python scripts/dev/seed_local.py
python -m uvicorn limnopulse_api.main:app --reload --host 0.0.0.0 --port 8000
```

## Local Auth

With `APP_ENV=local` and `AUTH_MODE=dev`, use:

```text
X-Dev-User-Sub: local-user-001
X-Dev-User-Email: local@example.test
```

Dev headers authenticate identity only. Tenant access still requires an active membership in `LimnopulseDomain`.

## Tests

```bash
python -m pytest -q
```
```

- [ ] **Step 4: Run script import checks**

Run:

```bash
rtk python -m compileall src scripts tests
```

Expected: no syntax errors.

- [ ] **Step 5: Commit Task 8**

Run:

```bash
rtk git add scripts/dev README.md
rtk git commit -m "docs: add local development workflow"
```

---

### Task 9: No-Scan Guard, Full Tests, And Final Cleanup

**Files:**
- Create: `tests/unit/test_no_scan_guard.py`
- Modify: files needed to fix failing tests only.

**Interfaces:**
- Consumes: all implementation files.
- Produces: final verified Phase 1 foundation.

- [ ] **Step 1: Add static no-scan guard test**

Create `tests/unit/test_no_scan_guard.py`:

```python
from pathlib import Path


def test_no_dynamodb_scan_in_application_code() -> None:
    root = Path("src/limnopulse_api")
    offenders = []
    for path in root.rglob("*.py"):
        text = path.read_text()
        if ".scan(" in text or "scan(" in text:
            offenders.append(str(path))
    assert offenders == []
```

- [ ] **Step 2: Run full test suite**

Run:

```bash
rtk python -m pytest -q
```

Expected: all tests pass.

- [ ] **Step 3: Run compile check**

Run:

```bash
rtk python -m compileall src scripts tests
```

Expected: no syntax errors.

- [ ] **Step 4: Check no disallowed dependencies**

Run:

```bash
rtk rg -n "postgres|psycopg|sqlalchemy|firestore|firebase|AquaFarm|Aquafarm|aquafarm" pyproject.toml .env.example compose.yaml src tests scripts README.md
```

Expected: no matches.

- [ ] **Step 5: Commit final guardrails**

Run:

```bash
rtk git add tests/unit/test_no_scan_guard.py
rtk git commit -m "test: add phase 1 guardrails"
```

---

## Self-Review

- Spec coverage: tasks cover settings, auth providers, dev auth restriction, Cognito validation surface, DynamoDB single-table access, tenant owner transaction, Redis cache-aside, tenant/pond/device endpoints, local setup, required tests, and no-scan guard.
- Placeholder scan: the plan contains no `TBD`, `TODO`, missing file paths, or test placeholders.
- Type consistency: `Principal`, `TenantRole`, `Membership`, `TenantAccess`, `DomainRepository`, `CacheRepository`, and `MembershipService` names are consistent across tasks.
- Scope check: Phase 1 remains bounded to FastAPI, Cognito auth provider, DynamoDB, Redis, CRUD, local setup, and tests. MQTT, InfluxDB, Go workers, SQS, SES, Telegram, WhatsApp, and SMS remain out of scope.
