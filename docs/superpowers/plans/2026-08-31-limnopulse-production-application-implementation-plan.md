# LimnoPulse Production Application Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce the portfolio-demo application release: a deterministic static frontend, a fail-closed FastAPI image, one-shot evaluator and synthetic publisher images, authenticated internal MQTT/Telegraf ingestion, production Compose, deterministic demo seed and credential-renewal-safe runtime.

**Architecture:** Preserve all existing V4 and /v1 domain contracts while adding a production-only composition that omits commercial, cache and notification/delivery capabilities completely. The browser authenticates through Cognito and reads only synthetic demo data. API, InfluxDB, Mosquitto and Telegraf run in isolated containers; evaluator and publisher are one-shot jobs invoked externally.

**Tech Stack:** Python 3.12, FastAPI, Pydantic Settings, boto3, InfluxDB client, Go 1.26, React 18, TypeScript, Vite, oidc-client-ts, Vitest, Playwright, Docker/Compose, Mosquitto 2, Telegraf, pytest.

**Spec:** docs/superpowers/specs/2026-08-29-limnopulse-production-deployment-design.md

## Global Constraints

- Start only after every P0/M1 task in docs/superpowers/plans/2026-08-25-limnopulse-v4-p0-m1.md is merged and its M1-08 gate is green on main. This plan consumes that work and does not recreate it.
- The inspected planning baseline is main@e95d3eee9c813e3946d43f297071a80b62dd123b and lacks frontend, production API image and GitHub workflows. Re-read the head and record the exact execution baseline in each PR.
- Production uses APP_ENV=prod, AUTH_MODE=cognito and AWS_REGION=us-east-2.
- The portfolio-demo profile creates no Redis or Telegram client, route, secret verifier, worker or schedule. It also creates no Stripe, SES, Push, SMS, AWS IoT, vendor connector or command component.
- Disabled is structural absence, not a fake sender or UI-only feature flag.
- Production rejects DynamoDB/SQS/Cognito endpoint overrides and any external Influx URL. Only http://influxdb:8086 is allowed.
- Ordinary membership reads operate directly against DynamoDB when caching is disabled.
- CORS is exactly https://limnopulse.com; methods GET, POST, PUT, PATCH, DELETE, OPTIONS; headers Authorization, Content-Type, Idempotency-Key; allow_credentials=false; no wildcard.
- Frontend runtime configuration is public and contains only API/Cognito/release/capability values.
- Static output is deterministic under web/dist and registers no service worker.
- Compose production does not extend compose.yaml and contains no emulator, local AWS key, fake provider, source build or mutable image tag.
- API is the only loopback-published product port. InfluxDB and MQTT have no host port.
- Mosquitto uses distinct publisher and Telegraf principals with default-deny ACLs.
- Images and base images are pinned by digest at release integration. Mutable tags may appear only in a documented local-development target.
- Test names in new Python tests describe behavior in Portuguese where the current repository convention requires it.
- One task/branch/PR at a time unless the execution graph explicitly permits parallel work.

## File Map

| Path | Responsibility |
|---|---|
| src/limnopulse_api/core/production.py | portfolio-demo invariant validation |
| src/limnopulse_api/composition.py | explicit dev/full versus portfolio composition |
| src/limnopulse_api/api/production_router.py | production-safe route selection |
| src/limnopulse_api/observability.py | redacted JSON logging |
| Dockerfile.api | pinned non-root API image |
| cmd/synthetic-publisher/** | one-shot synthetic MQTT producer |
| Dockerfile.synthetic-publisher | publisher image |
| web/** | versioned static product frontend |
| compose.production.yaml | production-only services/jobs |
| infra/mqtt/production/** | Mosquitto password/ACL templates |
| infra/telegraf/production/** | trusted registry and ingestion configuration |
| scripts/production/seed_demo.py | idempotent tenant/user-facing domain seed |
| tests/production/** | production contract and acceptance tests |

---

### Task 1: Enforce the V4 M1 Dependency Gate

**Branch:** test/prod-001-v4-gate

**Files:**
- Create: tests/production/test_v4_dependency_gate.py
- Create: docs/production-readiness.md

**Interfaces:**
- Consumes the M1-08 acceptance test, feature flags and migration runbook.
- Produces no runtime behavior.

- [ ] **Step 1: Write the failing dependency inventory**

    REQUIRED = (
        "tests/contracts/test_m1_acceptance_gate.py",
        "docs/m1-acceptance-runbook.md",
        "src/limnopulse_api/api/v2/router.py",
    )

    def test_dependencias_v4_m1_estao_integradas() -> None:
        for relative in REQUIRED:
            assert (ROOT / relative).is_file(), relative

- [ ] **Step 2: Run the test on the current baseline**

    uv run --locked --extra dev pytest -q tests/production/test_v4_dependency_gate.py

Expected on the inspected baseline: FAIL. Stop execution of Tasks 2-12 until M1-08 is merged; do not make this test skip or xfail.

- [ ] **Step 3: Document the exact green SHA and commands after integration**

Record make verify, M1 acceptance and /v1 golden results in docs/production-readiness.md.

- [ ] **Step 4: Commit only after the dependency is genuinely green**

    git add tests/production/test_v4_dependency_gate.py docs/production-readiness.md
    git commit -m "test(prod): require integrated V4 M1 baseline"

### Task 2: Add the Fail-Closed Portfolio Settings Contract

**Branch:** feat/prod-002-settings

**Files:**
- Modify: src/limnopulse_api/core/config.py
- Create: src/limnopulse_api/core/production.py
- Modify: .env.example
- Create: tests/production/test_portfolio_settings.py
- Modify: tests/unit/test_settings.py

**Interfaces:**
- Adds deployment_profile: Literal["development", "portfolio-demo"].
- Adds cache_enabled and outbound feature flags, all false in portfolio-demo.
- validate_portfolio_demo(settings) rejects forbidden settings and endpoints.

- [ ] **Step 1: Write failing settings tests**

    def test_portfolio_demo_exige_prod_cognito_e_us_east_2() -> None:
        settings = portfolio_settings()
        assert settings.app_env == "prod"
        assert settings.auth_mode == "cognito"
        assert settings.aws_region == "us-east-2"

    @pytest.mark.parametrize(
        "override",
        [
            {"redis_enabled": True},
            {"telegram_delivery_enabled": True},
            {"dynamodb_endpoint_url": "http://dynamodb-local:8000"},
            {"influxdb_url": "https://external.example"},
        ],
    )
    def test_portfolio_demo_rejeita_recurso_proibido(override) -> None:
        with pytest.raises(ValidationError):
            Settings(**portfolio_values(), **override)

- [ ] **Step 2: Run and verify RED**

- [ ] **Step 3: Add explicit fields and a single production validator**

Use positive enable flags with false defaults. The portfolio validator requires all disabled features false, no endpoint override, no direct Telegram secret/ARN/username dependency and exact internal Influx URL.

- [ ] **Step 4: Preserve current local/test behavior**

All existing test fixtures using Settings(app_env="test", auth_mode="dev") remain valid. Do not make portfolio-demo the default.

- [ ] **Step 5: Run settings suites and commit**

    uv run --locked --extra dev pytest -q tests/unit/test_settings.py tests/production/test_portfolio_settings.py
    git add src/limnopulse_api/core .env.example tests
    git commit -m "feat(prod): add fail-closed portfolio settings"

### Task 3: Split Application Composition and Omit Disabled Features

**Branch:** feat/prod-003-composition

**Files:**
- Create: src/limnopulse_api/composition.py
- Create: src/limnopulse_api/api/production_router.py
- Modify: src/limnopulse_api/main.py
- Modify: src/limnopulse_api/services/memberships.py
- Create: tests/production/test_portfolio_composition.py
- Modify: tests/api/test_app_runtime.py

**Interfaces:**
- build_runtime(settings) returns a context-managed RuntimeComponents.
- portfolio runtime contains DynamoDB, Cognito, Influx and NoCache only.
- build_production_router excludes Telegram webhook/bindings and notification preferences.

- [ ] **Step 1: Write tests that make forbidden constructors explode**

Patch redis.from_url, boto3 secretsmanager, Telegram services and notification repositories to raise AssertionError. Start TestClient with portfolio settings and assert health, membership-backed routes and telemetry read initialize successfully.

- [ ] **Step 2: Assert forbidden OpenAPI paths are absent**

    def test_portfolio_nao_expoe_rotas_desabilitadas() -> None:
        paths = create_app(portfolio_settings()).openapi()["paths"]
        assert not any("telegram" in path for path in paths)
        assert not any("notification-preferences" in path for path in paths)

- [ ] **Step 3: Implement NoCache at the repository boundary**

NoCache returns cache misses and no-op writes without creating/probing Redis. MembershipService still fetches canonical DynamoDB rows on every ordinary read.

- [ ] **Step 4: Move construction out of the lifespan hotspot**

Keep create_app deterministic. Local/default composition preserves all current behavior. Portfolio production router includes health, me, tenants, ponds/sites, devices, telemetry, alert rules/events and approved M1 routes only.

- [ ] **Step 5: Run current and production runtime suites**

    uv run --locked --extra dev pytest -q tests/api/test_app_runtime.py tests/production/test_portfolio_composition.py tests/unit/test_membership_cache.py

- [ ] **Step 6: Commit**

    git add src/limnopulse_api tests
    git commit -m "feat(prod): compose portfolio without cache or delivery"

### Task 4: Add Exact CORS, Health and Redacted JSON Logs

**Branch:** feat/prod-004-http-observability

**Files:**
- Modify: src/limnopulse_api/main.py
- Create: src/limnopulse_api/observability.py
- Modify: src/limnopulse_api/api/v1/routers/health.py
- Create: tests/production/test_cors.py
- Create: tests/production/test_health.py
- Create: tests/production/test_json_logging.py

**Interfaces:**
- GET /health/live does process-only liveness.
- GET /health/ready performs bounded DynamoDB and Influx checks without details.
- JSON logs redact Authorization, cookies, tokens, emails, payload bodies and high-cardinality IDs.

- [ ] **Step 1: Write the exact preflight matrix**

Allowed origin/method/header combinations return expected CORS headers. Another origin, wildcard, cookies and an unlisted header do not receive an allow response.

- [ ] **Step 2: Write independent health tests**

Liveness remains 200 during dependency failure; readiness returns 503 with only {"status":"unavailable"} and a request ID header.

- [ ] **Step 3: Implement CORSMiddleware only for portfolio**

Use the literal arrays from the Spec and allow_credentials=False. Local composition remains independently configurable for tests.

- [ ] **Step 4: Implement one-line redacted JSON formatter**

Fields are timestamp, level, service, event, request_id and bounded outcome. A recursive sanitizer rejects known secret keys and URL query strings.

- [ ] **Step 5: Run and commit**

    uv run --locked --extra dev pytest -q tests/production/test_cors.py tests/production/test_health.py tests/production/test_json_logging.py
    git add src/limnopulse_api tests/production
    git commit -m "feat(prod): add exact cors health and redacted logs"

### Task 5: Build the Non-Root API Image

**Branch:** feat/prod-005-api-image

**Files:**
- Create: Dockerfile.api
- Create: docker/api/entrypoint.sh
- Create: docker/api/healthcheck.py
- Create: .dockerignore additions
- Create: tests/production/test_api_image.py

**Interfaces:**
- Image runs a fixed UID/GID, execs uvicorn and exposes 8000 only as metadata.
- Runtime requires external read-only credential/config and secret files.

- [ ] **Step 1: Write a Dockerfile policy test**

Require multi-stage, lock-respecting dependency install, numeric USER, no shell package in final stage, HEALTHCHECK, no COPY . and no embedded .env/key.

- [ ] **Step 2: Build and inspect the candidate image**

    docker build --pull --tag limnopulse-api:test --file Dockerfile.api .
    docker image inspect limnopulse-api:test

Expected before implementation: Dockerfile missing.

- [ ] **Step 3: Implement pinned multi-stage build**

Copy pyproject/lock first, install locked wheel/runtime dependencies, copy src only, use a minimal digest-pinned Python runtime and set PYTHONDONTWRITEBYTECODE/PYTHONUNBUFFERED.

- [ ] **Step 4: Run container security smoke**

Start with read-only root, tmpfs /tmp, cap-drop ALL, no-new-privileges and portfolio env fixtures. Assert UID nonzero, / write fails and live endpoint passes.

- [ ] **Step 5: Commit**

    git add Dockerfile.api docker/api .dockerignore tests/production/test_api_image.py
    git commit -m "feat(prod): add hardened api image"

### Task 6: Create the Static Portfolio Frontend

**Branch:** feat/prod-006-frontend-foundation

**Files:**
- Create: web/package.json
- Create: web/package-lock.json
- Create: web/tsconfig.json
- Create: web/vite.config.ts
- Create: web/index.html
- Create: web/src/main.tsx
- Create: web/src/App.tsx
- Create: web/src/config.ts
- Create: web/src/styles.css
- Create: web/src/sw-guard.ts
- Create: web/tests/config.test.ts
- Create: web/tests/build.test.ts
- Create: web/public/runtime-config.example.json

**Interfaces:**
- RuntimeConfig contains apiBaseUrl, cognitoIssuer, cognitoClientId, cognitoAuthorizationBaseUrl, awsRegion, releaseId and capabilities.
- Production apiBaseUrl is https://api.limnopulse.com/v1.
- Output is web/dist and contains no service worker.

- [ ] **Step 1: Write failing runtime-config validation tests**

Reject missing release, non-HTTPS API, wrong API origin, secret-like fields, unknown capabilities and production URLs outside the approved domains.

- [ ] **Step 2: Scaffold a locked React/Vite build**

Pin exact dependencies and Node version. Scripts are lint, typecheck, test and build. Do not add analytics, external fonts or third-party scripts.

- [ ] **Step 3: Implement a minimal honest portfolio surface**

Pages: public overview explaining synthetic/demo scope, sign-in callback, authenticated dashboard with tenant selector, synthetic telemetry and alert state. Clearly label every measurement as synthetic.

- [ ] **Step 4: Add deterministic build metadata**

SOURCE_DATE_EPOCH and release ID drive stable output. Hash assets; runtime config and index are release-coupled inputs. sw-guard fails if navigator.serviceWorker.register, service-worker filenames or Workbox appear.

- [ ] **Step 5: Run frontend gates**

    npm --prefix web ci
    npm --prefix web run lint
    npm --prefix web run typecheck
    npm --prefix web test -- --run
    npm --prefix web run build

- [ ] **Step 6: Commit**

    git add web
    git commit -m "feat(web): add deterministic portfolio frontend"

### Task 7: Implement Cognito PKCE and the Authenticated API Client

**Branch:** feat/prod-007-frontend-auth

**Files:**
- Create: web/src/auth/oidc.ts
- Create: web/src/auth/AuthProvider.tsx
- Create: web/src/api/client.ts
- Create: web/src/api/queries.ts
- Create: web/src/routes/**
- Create: web/tests/auth.test.tsx
- Create: web/tests/client.test.ts
- Create: web/tests/e2e/portfolio.spec.ts

**Interfaces:**
- Public Authorization Code + PKCE, no client secret and in-memory tokens.
- API client sends bearer only to api.limnopulse.com and tenant identity only after membership selection.
- No delivery/notification mutation exists.

- [ ] **Step 1: Write token-leak and origin tests**

Reject localStorage/sessionStorage tokens, Authorization to another origin, cookies/credentials and tenant IDs supplied before canonical membership selection.

- [ ] **Step 2: Implement OIDC manager from runtime config**

Issuer/client/authorization base must agree and use HTTPS. Callback/logout are exact limnopulse.com URLs. Scope is openid email profile.

- [ ] **Step 3: Implement one typed fetch boundary**

credentials="omit"; mode="cors"; Authorization set only after URL origin comparison; Idempotency-Key only on approved mutations; errors never serialize response headers containing secrets.

- [ ] **Step 4: Implement synthetic dashboard queries**

Use existing /v1 me, tenants, ponds/sites, devices, telemetry and alert endpoints. Display a capability-disabled explanation instead of notification controls.

- [ ] **Step 5: Run unit/E2E/build and commit**

    npm --prefix web test -- --run
    npm --prefix web run build
    npm --prefix web exec playwright test
    git add web
    git commit -m "feat(web): add cognito login and safe api client"

### Task 8: Add the One-Shot Synthetic Publisher

**Branch:** feat/prod-008-synthetic-publisher

**Files:**
- Create: cmd/synthetic-publisher/main.go
- Create: internal/synthetic/config.go
- Create: internal/synthetic/fixture.go
- Create: internal/synthetic/publisher.go
- Create: internal/synthetic/*_test.go
- Create: fixtures/synthetic/v1.json
- Create: Dockerfile.synthetic-publisher

**Interfaces:**
- Command run publishes exactly one versioned fixture to the exact synthetic topic prefix and exits.
- Payload contains device measurement identity/value/time only; tenant/site enrichment comes from trusted Telegraf registry.
- Exit codes distinguish success, retryable dependency failure and terminal configuration failure.

- [ ] **Step 1: Write Go tests for fixture determinism and topic restriction**

- [ ] **Step 2: Write tests that reject tenant/site fields and arbitrary topics**

- [ ] **Step 3: Implement strict config and one-shot publish**

Require APP_ENV=prod, broker service name, client ID, username/password file and exact topic. Bound connect/publish timeout and message size.

- [ ] **Step 4: Build a distroless non-root image**

Pin builder/runtime digests during integration. No shell, certificate private key or AWS credential is needed.

- [ ] **Step 5: Run and commit**

    go test -race ./...
    docker build --tag limnopulse-synthetic:test --file Dockerfile.synthetic-publisher .
    git add cmd/synthetic-publisher internal/synthetic fixtures/synthetic Dockerfile.synthetic-publisher
    git commit -m "feat(synthetic): add one-shot mqtt publisher"

### Task 9: Harden MQTT and Telegraf Production Ingestion

**Branch:** feat/prod-009-mqtt-telegraf

**Files:**
- Create: infra/mqtt/production/mosquitto.conf
- Create: infra/mqtt/production/acl.template
- Create: infra/mqtt/production/password-file.README
- Create: infra/telegraf/production/telegraf.conf
- Create: infra/telegraf/production/device_registry.star
- Create: tests/production/test_mqtt_telegraf.py
- Create: tests/production/mqtt_matrix.sh

**Interfaces:**
- Principal synthetic-publisher can write only the exact topic and cannot read/subscribe.
- Principal telegraf can read/subscribe only that topic and cannot write.
- Anonymous/other topics are denied; unknown device is dropped and counted.

- [ ] **Step 1: Write static configuration tests**

Require allow_anonymous false, no listener host binding, password_file and acl_file. Reject pattern ACLs broader than the exact synthetic prefix.

- [ ] **Step 2: Create a two-principal integration matrix**

Test allowed publisher->Telegraf flow and every denied cross-operation with generated fixture passwords.

- [ ] **Step 3: Implement production configurations**

Secrets are file paths, not environment values. Starlark registry maps only the deterministic demo device to bounded tenant/site tags and increments unknown-device metric without payload log.

- [ ] **Step 4: Run broker/Telegraf integration**

    docker compose -f tests/production/mqtt-compose.yml up --abort-on-container-exit --exit-code-from verifier
    docker compose -f tests/production/mqtt-compose.yml down -v --remove-orphans

- [ ] **Step 5: Commit**

    git add infra/mqtt/production infra/telegraf/production tests/production
    git commit -m "feat(ingestion): isolate synthetic mqtt principals"

### Task 10: Create Independent Production Compose

**Branch:** feat/prod-010-compose

**Files:**
- Create: compose.production.yaml
- Create: config/production/api.env.schema
- Create: config/production/evaluator.env.schema
- Create: config/production/publisher.env.schema
- Create: tests/production/test_compose_contract.py

**Interfaces:**
- Long-lived services: api, influxdb, mqtt, telegraf.
- One-shot profiles: evaluator, synthetic-publisher, backup-verify.
- API binds one declared loopback port; all other services are internal only.

- [ ] **Step 1: Write a structural policy test**

Reject extends/include of compose.yaml, build, latest/mutable images, emulator names, Redis, notification workers, host network, privileged, docker.sock, public ports and local AWS keys.

- [ ] **Step 2: Add resource/security assertions**

Every service uses read_only where supported, cap_drop ALL, no-new-privileges, non-root user, pids/memory/cpu/log limits, healthcheck and product-specific networks.

- [ ] **Step 3: Implement Compose with digest variables**

Require API_IMAGE, EVALUATOR_IMAGE and SYNTHETIC_PUBLISHER_IMAGE matching @sha256. Mount /run/personal-platform/limnopulse/aws as a directory read-only into API/evaluator only. Mount separate MQTT password files to their owners.

- [ ] **Step 4: Keep Influx persistence outside releases**

Named external volume limnopulse-influxdb-prod. Initialization token is visible only to init/backup boundary; API receives application token file.

- [ ] **Step 5: Validate and commit**

    docker compose -f compose.production.yaml config --quiet
    uv run --locked --extra dev pytest -q tests/production/test_compose_contract.py
    git add compose.production.yaml config/production tests/production/test_compose_contract.py
    git commit -m "feat(prod): add isolated production compose"

### Task 11: Add Idempotent Demo Seed and First-Breach Fixture

**Branch:** feat/prod-011-demo-seed

**Files:**
- Create: scripts/production/seed_demo.py
- Create: src/limnopulse_api/seed/demo.py
- Create: tests/production/test_demo_seed.py
- Create: docs/demo-data.md

**Interfaces:**
- seed_demo creates one reviewed tenant, site/pond, synthetic device, memberships and one deterministic active due alert rule.
- Re-run returns existing matching records; divergent ownership/content fails.
- Cognito user password/message handling is not in this script.

- [ ] **Step 1: Write idempotency and collision tests**

Run twice and assert identical IDs/versions with zero second writes. Existing non-demo tenant or user-authored rule at the deterministic key yields conflict and no overwrite.

- [ ] **Step 2: Write first-breach timing tests**

The one-minute rule and fixture create a sufficient complete window, due no later than the smoke time, and open the expected incident on first evaluation.

- [ ] **Step 3: Implement conditional DynamoDB writes**

Use TransactWriteItems/condition expressions and canonical key helpers. Never Scan or bulk delete.

- [ ] **Step 4: Add visibly synthetic labels**

Names and descriptions include “Demonstração — dados sintéticos”; docs prohibit municipal/customer data.

- [ ] **Step 5: Run and commit**

    uv run --locked --extra dev pytest -q tests/production/test_demo_seed.py tests/unit/test_no_scan_guard.py
    git add scripts/production src/limnopulse_api/seed tests/production/test_demo_seed.py docs/demo-data.md
    git commit -m "feat(prod): seed deterministic synthetic demo"

### Task 12: Build the Local Production Acceptance Gate

**Branch:** test/prod-012-application-acceptance

**Files:**
- Create: tests/production/test_application_acceptance.py
- Create: tests/production/smoke.sh
- Create: docs/runbooks/application-smoke.md
- Modify: Makefile

**Interfaces:**
- Produces make verify-production-application.
- No AWS, Cloudflare, SSH or production credential is required.

- [ ] **Step 1: Add the aggregate contract**

Require V4 M1 green; portfolio app starts with forbidden constructors patched; exact CORS; image policies; web build; no service worker; Compose schema; MQTT ACL; seed idempotency; synthetic publisher/evaluator flow.

- [ ] **Step 2: Add credential-renewal rehearsal hooks**

Mount a fake credential directory, start API, atomically replace it, invoke the fixed graceful replacement hook expected from infra-ansible, and prove the new process uses the new identity while the old drains.

- [ ] **Step 3: Run all local gates**

    make verify
    make verify-production-application
    docker compose -f compose.production.yaml config --quiet
    git diff --check

- [ ] **Step 4: Scan the exact release inputs**

    git grep -nEi '(AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|TELEGRAM_BOT_TOKEN|local-dev-token)' -- compose.production.yaml config/production web/dist

Expected: no matches.

- [ ] **Step 5: Commit**

    git add tests/production docs/runbooks/application-smoke.md Makefile
    git commit -m "test(prod): add portfolio application acceptance gate"

## Execution Order

Task 1 is a hard serial gate. Tasks 2 and 6 may start after it. Task 3 waits for Task 2; Task 4 waits for Task 3. Task 5 waits for Tasks 2-4. Task 7 waits for Task 6. Tasks 8 and 9 may proceed after Task 1 and join before Task 10. Task 10 waits for Tasks 5, 8 and 9. Task 11 waits for Task 3 plus the M1 adapters. Task 12 waits for every prior task.

## Plan Self-Review Record

- V4 relationship: M1 remains the dependency; no M1 entity, adapter or migration is duplicated.
- Disabled features: every forbidden constructor, route, secret and Compose service has a negative test.
- Frontend: deterministic product code, exact public runtime schema, no service worker or secret.
- Runtime: exact prod/Cognito/us-east-2 contract, direct DynamoDB membership, internal Influx only.
- Ingestion: separate MQTT principals, trusted enrichment and unknown-device drop.
- Data safety: deterministic synthetic-only seed with conditional collision behavior.
- Completeness scan: no unresolved marker, fake-success provider or undefined startup behavior.

## Execution Handoff

Implement in worktree PRs after M1-08 is green. This plan ends with locally verifiable release inputs; AWS resources, release manifests, systemd timers and production promotion belong to the companion delivery plan.
