# Limnopulse Phase 1 Foundation Design

**Date:** 2026-07-08  
**Status:** Design ready for user review  
**Scope:** FastAPI foundation only

## Context

Limnopulse starts from a documentation-only repository. The target architecture is defined in `docs/architecture.md`, but new implementation artifacts must use the canonical Limnopulse name:

- Python package: `limnopulse_api`
- Domain table: `LimnopulseDomain`
- Audit table: `LimnopulseAudit`
- Local examples, docs, environment variables, and compose resources: Limnopulse naming

Historical `AquaFarm` naming may appear only when citing existing historical documentation. New code, local setup, and test artifacts must not introduce AquaFarm/Aquafarm names.

## Goals

Build the Phase 1 foundation:

- FastAPI backend structure.
- Auth provider interface with local dev auth and Cognito JWT auth.
- DynamoDB single-table domain repositories.
- CRUD endpoints for tenants, ponds, and devices.
- Tenant authorization based on active DynamoDB membership.
- Redis cache-aside for memberships, device registry, tenant settings, and JWKS.
- Local development setup using Redis and DynamoDB Local.
- Automated tests for auth, authorization, repositories, cache-aside, and no-scan guardrails.

## Non-Goals

Do not implement real integrations for:

- MQTT Broker.
- Telegraf.
- InfluxDB reads or writes.
- Go workers or Lambdas.
- SQS, SES, Telegram, WhatsApp, or SMS.
- Real device credential rotation.

Interfaces or stubs are allowed only when they preserve future architecture without creating a fake external integration.

## Chosen Approach

Use a layered FastAPI application:

```text
api -> services -> repositories/adapters
```

Routers should stay thin and HTTP-focused. FastAPI dependencies resolve the authenticated principal, tenant access, and minimum role. Services apply domain rules and call repositories. Repositories/adapters encapsulate DynamoDB, Redis, and Cognito/JWKS details.

This approach is preferred over feature folders or a flat app because Phase 1 must make auth/authz boundaries testable. The key invariant is:

```text
identity != tenant access
tenant access = active DynamoDB membership
```

## Proposed File Layout

```text
pyproject.toml
.env.example
compose.yaml
README.md

scripts/dev/init_dynamodb.py
scripts/dev/seed_local.py

src/limnopulse_api/
  __init__.py
  main.py
  core/
    config.py
    errors.py
    logging.py
  api/
    router.py
    dependencies.py
    v1/
      routers/
        health.py
        me.py
        tenants.py
        ponds.py
        devices.py
      schemas/
        common.py
        me.py
        tenants.py
        ponds.py
        devices.py
  auth/
    models.py
    providers.py
    dev.py
    cognito.py
  domain/
    entities.py
    roles.py
    ids.py
  services/
    memberships.py
    tenants.py
    ponds.py
    devices.py
  repositories/
    domain.py
    cache.py
  adapters/
    dynamodb.py
    redis.py
    jwks.py

tests/
  unit/
  api/
  integration/
```

The exact module names may be tightened during implementation, but the boundaries must remain: HTTP, auth providers, domain services, repositories, and adapters are separate.

## Authentication Design

Expose a provider interface:

```text
PrincipalProvider
  authenticate(request) -> Principal
```

Implementations:

- `DevAuthProvider` for `AUTH_MODE=dev`.
- `CognitoJwtAuthProvider` for `AUTH_MODE=cognito`.

### DevAuthProvider

`AUTH_MODE=dev` is for local development and fast tests only.

Allowed headers:

```text
X-Dev-User-Sub
X-Dev-User-Email
X-Dev-User-Groups
```

Rules:

- `AUTH_MODE=dev` may run only when `APP_ENV` is `local` or `test`.
- If `APP_ENV` is `staging` or `prod`, the app must fail during startup/config validation when `AUTH_MODE=dev`.
- Dev headers authenticate identity only.
- Dev headers and groups must never grant tenant access.
- Missing dev identity returns `401`.

### CognitoJwtAuthProvider

`AUTH_MODE=cognito` validates Cognito access tokens. The provider must be real and testable without requiring a live Cognito user pool in local development.

Validation requirements:

- Require `Authorization: Bearer <access_token>`.
- Require JWT header `alg=RS256` and a `kid`.
- Resolve the matching JWK from Cognito JWKS.
- Validate signature.
- Validate issuer.
- Validate expiration and time claims with small clock leeway.
- Validate `client_id` or audience against configured client IDs.
- Validate `token_use=access`.
- Require `sub`.
- Expose a `Principal` with `cognito_sub`, optional email, and optional groups.

JWKS cache:

- Use in-process cache for hot path.
- Use Redis as shared cache when available.
- Redis keys include the user pool identity, for example `jwks:cognito:<user_pool_id>:<kid>`.
- TTL must stay within 6-24 hours.
- Unknown `kid` refreshes JWKS once before rejecting.
- Never store raw JWTs in Redis or logs.

## Authorization Design

FastAPI dependencies own request-level auth/authz composition:

- `get_current_principal`
- `require_tenant_access`
- `require_tenant_role`

Services must not duplicate membership lookup logic per route. Tenant-scoped routes must depend on tenant access before calling services.

Role model:

```text
owner
admin
member
viewer
```

Phase 1 permissions:

| Action | owner | admin | member | viewer |
|---|---:|---:|---:|---:|
| List own tenants | yes | yes | yes | yes |
| Create tenant | yes | yes | yes | yes |
| Read tenant, ponds, devices | yes | yes | yes | yes |
| Create/update tenant | yes | yes | no | no |
| Create/update ponds | yes | yes | no | no |
| Create/update devices | yes | yes | no | no |

For Phase 1, `member` is read-only. `viewer` is also read-only. If later product requirements distinguish them, that must be a new explicit decision.

Tenant access rules:

- `/v1/me` requires authenticated identity.
- `/v1/tenants` returns only tenants from active memberships.
- Any `/v1/tenants/{tenant_id}/...` route requires active membership for that tenant.
- Write routes require owner/admin.
- Read routes allow owner/admin/member/viewer.
- No Cognito claim, Cognito group, or dev header grants tenant access.

## DynamoDB Domain Model

Use a single `LimnopulseDomain` table:

```text
PK: string
SK: string
```

Phase 1 items:

```text
Tenant
  PK = TENANT#<tenant_id>
  SK = META

Pond
  PK = TENANT#<tenant_id>
  SK = POND#<pond_id>

Device
  PK = TENANT#<tenant_id>
  SK = DEVICE#<device_id>

Device lookup
  PK = DEVICE#<device_id>
  SK = META

User profile
  PK = USER#<cognito_sub>
  SK = PROFILE

Membership
  PK = USER#<cognito_sub>
  SK = TENANT#<tenant_id>

Tenant member
  PK = TENANT#<tenant_id>
  SK = MEMBER#<cognito_sub>
```

Mutable items must include:

```text
created_at
updated_at
version
schema_version
status
```

Create behavior:

- Use condition expressions to avoid overwrites.
- Initial `version=1`.
- Server assigns timestamps in UTC.

Patch behavior:

- Require or internally compare expected version where practical.
- Increment `version`.
- Update `updated_at`.
- Return `409` for version/conditional conflicts.

Tenant creation must be atomic:

```text
TransactWrite:
  Tenant
  Membership for creator with role=owner and status=active
  Tenant member mirror for creator with role=owner and status=active
```

Without this transaction, the creator would create a tenant they cannot access, so this is a core correctness requirement.

Device creation should transactionally write the tenant-scoped device item and global device lookup item.

## DynamoDB Access Patterns

No critical API path may use `Scan`.

Allowed primitives:

- `GetItem` by full `PK`/`SK`.
- `Query` by exact `PK`.
- `Query` by exact `PK` plus `begins_with(SK, ...)`.
- `BatchGetItem` for known keys.
- `TransactWriteItems` / `TransactGetItems` where consistency matters.

Required access patterns:

```text
List tenants for user:
  Query PK = USER#<sub>, begins_with(SK, TENANT#)

Authorize tenant access:
  GetItem PK = USER#<sub>, SK = TENANT#<tenant_id>
  require status = active

Get tenant:
  GetItem PK = TENANT#<tenant_id>, SK = META

List ponds:
  Query PK = TENANT#<tenant_id>, begins_with(SK, POND#)

Get pond:
  GetItem PK = TENANT#<tenant_id>, SK = POND#<pond_id>

List devices:
  Query PK = TENANT#<tenant_id>, begins_with(SK, DEVICE#)

Get device:
  GetItem PK = TENANT#<tenant_id>, SK = DEVICE#<device_id>

Lookup device:
  GetItem PK = DEVICE#<device_id>, SK = META
```

Do not use `FilterExpression` for authorization or tenant isolation.

## Redis Cache Design

Redis is cache-aside only. It is never a source of truth.

Keys:

```text
user:<sub>:memberships
device:<device_id>
tenant:<tenant_id>:settings
jwks:cognito:<user_pool_id>:<kid>
```

TTL requirements:

- Membership cache: 60-300 seconds.
- Device cache: 900-3600 seconds.
- Tenant settings cache: 900-3600 seconds.
- JWKS cache: 21600-86400 seconds.

Rules:

- Critical reads must fall back to DynamoDB on cache miss.
- If Redis is unavailable, read from DynamoDB where safe.
- Authorization fails closed if neither Redis nor DynamoDB can confirm membership.
- Never store raw JWTs, secrets, or long-lived permissions.
- Membership changes should invalidate cache immediately when the change happens in API services. DynamoDB Streams invalidation can be added later.

## API Contract

Health:

```text
GET /healthz
GET /readyz
```

User:

```text
GET /v1/me
```

Tenants:

```text
GET    /v1/tenants
POST   /v1/tenants
GET    /v1/tenants/{tenant_id}
PATCH  /v1/tenants/{tenant_id}
```

Ponds:

```text
GET    /v1/tenants/{tenant_id}/ponds
POST   /v1/tenants/{tenant_id}/ponds
GET    /v1/tenants/{tenant_id}/ponds/{pond_id}
PATCH  /v1/tenants/{tenant_id}/ponds/{pond_id}
```

Devices:

```text
GET    /v1/tenants/{tenant_id}/devices
POST   /v1/tenants/{tenant_id}/devices
GET    /v1/tenants/{tenant_id}/devices/{device_id}
PATCH  /v1/tenants/{tenant_id}/devices/{device_id}
```

The broader architecture mentions device credential rotation, telemetry readings, alert rules, alerts, notification preferences, Telegram links, and webhooks. Those remain outside Phase 1.

## Error Handling

HTTP errors:

- `401`: missing or invalid identity.
- `403`: authenticated identity lacks active membership or required role.
- `404`: requested resource does not exist within the authorized tenant.
- `409`: conditional write/version conflict or create conflict.
- `503`: required infrastructure unavailable where the request cannot safely continue.

Logs must not include raw JWTs, secrets, or authorization headers.

## Local Development

Use host-run FastAPI plus Docker Compose services:

- FastAPI runs on the host for reload/debugging.
- Redis runs in Compose.
- DynamoDB Local runs in Compose.
- Cognito real is not required for local development.
- SQS, SES, Telegram, WhatsApp, SMS, InfluxDB, MQTT, and Telegraf are not started for Phase 1.

Required `.env.example` variables:

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

Local seed data must create at least one user, one tenant, and an active owner membership so `/v1/tenants` and tenant-scoped routes work immediately.

## Testing Design

Use a hybrid suite:

- Unit tests with fakes for auth providers, repositories, and cache.
- FastAPI route tests using dependency overrides.
- Repository tests using fake DynamoDB clients and, where practical, DynamoDB Local.
- Redis cache tests using fake Redis or isolated local Redis.

Required tests:

- `AUTH_MODE=dev` allowed in `local`.
- `AUTH_MODE=dev` allowed in `test`.
- `AUTH_MODE=dev` rejected during startup/config validation in `staging`.
- `AUTH_MODE=dev` rejected during startup/config validation in `prod`.
- Missing identity returns `401`.
- Valid dev identity without membership returns `403`.
- Valid Cognito identity without membership returns `403`.
- Active membership allows tenant access.
- Disabled/inactive membership denies tenant access.
- `viewer` cannot create or patch tenants, ponds, or devices.
- `member` cannot create or patch tenants, ponds, or devices in Phase 1.
- `owner` can create and patch tenants, ponds, and devices.
- `admin` can create and patch tenants, ponds, and devices.
- Creating a tenant also creates owner membership and tenant member mirror item.
- Membership cache miss reads DynamoDB and writes Redis with short TTL.
- Membership cache hit avoids DynamoDB.
- Redis outage falls back to DynamoDB for membership lookup.
- Redis never stores raw JWTs or secrets.
- Repository critical paths do not call `Scan`.
- List endpoints use `Query` or known-key reads.
- Create operations use condition expressions.
- Patch operations increment `version` and update `updated_at`.
- Conditional conflicts map to `409`.

## Acceptance Criteria

The implementation of this spec is complete when:

- FastAPI app starts locally.
- `.env.example` exists and contains the required variables.
- Compose starts Redis and DynamoDB Local.
- `/v1/me`, tenants, ponds, and devices endpoints are implemented.
- `AUTH_MODE=dev` works only in `local` and `test`.
- `AUTH_MODE=cognito` is implemented behind `CognitoJwtAuthProvider` with JWKS, issuer, expiration, client ID/audience, and `token_use` validation.
- Tenant authorization always requires active DynamoDB membership.
- Tenant creation atomically creates owner membership and tenant member mirror.
- Redis is used only as cache-aside with TTL.
- No PostgreSQL or Firebase/Firestore dependency is introduced.
- No real secrets are committed.
- Tests for auth, authorization, repositories, cache-aside, and no-scan guardrails pass.
- README or equivalent local docs explain setup and test commands.

## Phase 2 Handoff

After Phase 1, the next work can add telemetry ingestion and reads:

- MQTT Broker TLS/mTLS and topic ACLs.
- Telegraf MQTT consumer.
- InfluxDB buckets and schema.
- Authorized readings endpoints through FastAPI.
- Go alert evaluator, SQS notifications, and notification dispatchers in later phases.
