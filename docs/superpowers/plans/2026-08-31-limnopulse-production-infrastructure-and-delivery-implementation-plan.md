# LimnoPulse Production Infrastructure and Delivery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Provision and safely promote the LimnoPulse portfolio-demo AWS resources and immutable application release through GitHub-hosted CI, private S3/CloudFront, Cognito, DynamoDB, base SQS/DLQ, least-privilege IAM and the shared personal VPS delivery gate.

**Architecture:** Product OpenTofu owns only LimnoPulse AWS resources and exports narrow edge contracts. Pull requests validate without credentials. Manual plan/apply and host promotion call immutable reusable gates in personal-infra-live through GitHub OIDC. Static releases use immutable prefixes and a last-write root index; API and one-shot jobs use digest-bound Compose on the VPS.

**Tech Stack:** OpenTofu 1.8+, AWS provider, AWS us-east-2, ACM/WAF/Pricing Plan Manager us-east-1, S3, CloudFront/OAC, Cognito, DynamoDB, SQS/DLQ, CloudWatch, IAM, GitHub Actions, GHCR, Syft, Trivy, OPA/Conftest, pytest.

**Spec:** docs/superpowers/specs/2026-08-29-limnopulse-production-deployment-design.md

## Global Constraints

- Start only after the companion application plan is merged and make verify-production-application is green on main.
- Shared state/KMS/SSM/Budgets/OIDC/Roles Anywhere, Cloudflare and VPS resources are references from personal-infra-live; this state never recreates them.
- Primary provider is us-east-2. Alias global_edge in us-east-1 owns only ACM certificate and CLOUDFRONT-scope WAF.
- State key is limnopulse/prod/opentofu.tfstate in the private shared backend with use_lockfile=true.
- OpenTofu backend bucket/account IDs, Cloudflare IDs and state never appear in this public repository.
- core_dynamodb, core_cognito and core_sqs are true in portfolio-demo. Every commercial/notification/device provider flag is false and creates zero resources, IAM statements, secret containers or outputs.
- The exact managed Cognito domain prefix is limnopulse-portfolio-demo. Unavailability fails planning/rollout; do not invent a suffix.
- DynamoDB is PAY_PER_REQUEST with PITR 35 days and maximum throughput 100 reads/25 writes on tables and every GSI.
- The base notification queue/DLQ exists, but relay/provider scheduling and delivery workers do not.
- Static S3 is private and versioned; CloudFront uses OAC and one dedicated WAF. No S3 website endpoint.
- CloudFront Free is a separate manual idempotent Pricing Plan Manager step and must be ACTIVE before DNS. No local-exec and no paid fallback.
- LimnoPulse managed-service envelope is USD 3; aggregate automation freeze remains USD 15.
- GitHub Actions paid spending is USD 0; no self-hosted runner or production credential on pull requests.
- Merge never deploys. Apply and promotion are workflow_dispatch with exact expiring evidence.

## File Map

| Path | Responsibility |
|---|---|
| infra/opentofu/modules/core-* | enabled DynamoDB/Cognito/SQS |
| infra/opentofu/modules/static-web | S3/CloudFront/OAC/ACM/WAF |
| infra/opentofu/modules/runtime-iam | VPS/job/canary least privilege |
| infra/opentofu/modules/observability | bounded alarms/log retention |
| infra/opentofu/env/prod | product composition and outputs |
| policies | cost/resource/secret/ownership policy |
| release | manifest schemas and builders |
| .github/workflows | validation, release, plan/apply and promotion callers |
| scripts/deploy | static, Free-plan, canary, promotion and rollback helpers |
| docs/runbooks | manual operations and acceptance |

---

### Task 1: Refactor OpenTofu into Explicit Feature Modules

**Branch:** feat/prod-infra-001-feature-boundaries

**Files:**
- Create: infra/opentofu/modules/core-dynamodb/**
- Create: infra/opentofu/modules/core-cognito/**
- Create: infra/opentofu/modules/core-sqs/**
- Create: infra/opentofu/modules/email-delivery/**
- Create: infra/opentofu/modules/telegram-delivery/**
- Modify: infra/opentofu/*.tf
- Create: infra/opentofu/env/prod/main.tf
- Create: infra/opentofu/env/prod/variables.tf
- Create: infra/opentofu/env/prod/outputs.tf
- Create: tests/production/test_opentofu_feature_boundaries.py

**Interfaces:**
- Feature object contains core_dynamodb/core_cognito/core_sqs and all disabled flags from the Spec.
- count/for_each is applied at module boundary; disabled modules have zero resources and outputs.

- [ ] **Step 1: Write a plan-JSON fixture test**

With portfolio variables, assert there is no SES, EventBridge custom bus, Telegram Secrets Manager, Push/SMS, Stripe, IoT or Redis resource/address.

- [ ] **Step 2: Run current OpenTofu tests and confirm unconditional resources fail**

    uv run --locked --extra dev pytest -q tests/unit/test_opentofu_infra.py tests/production/test_opentofu_feature_boundaries.py

- [ ] **Step 3: Move existing resources without changing enabled semantics**

First preserve current cloud example with explicit true flags; then compose portfolio-demo with only core modules. Use moved blocks where state address migration is needed and test the mapping.

- [ ] **Step 4: Add validations that reject forbidden portfolio flags**

- [ ] **Step 5: Validate and commit**

    tofu -chdir=infra/opentofu init -backend=false -input=false
    tofu -chdir=infra/opentofu fmt -check -recursive
    tofu -chdir=infra/opentofu validate -no-color
    uv run --locked --extra dev pytest -q tests/unit/test_opentofu_infra.py tests/production/test_opentofu_feature_boundaries.py
    git add infra/opentofu tests
    git commit -m "refactor(infra): isolate production feature modules"

### Task 2: Harden DynamoDB and Base SQS/DLQ

**Branch:** feat/prod-infra-002-data-queues

**Files:**
- Modify: infra/opentofu/modules/core-dynamodb/**
- Modify: infra/opentofu/modules/core-sqs/**
- Create: tests/production/test_data_queue_plan.py

**Interfaces:**
- Tables LimnopulseDomain and LimnopulseAudit retain approved keys/indexes.
- Base queue uses long polling, encryption, bounded retention/visibility and DLQ redrive.
- Canary policy accepts a non-domain schema and cannot mutate NotificationDelivery.

- [ ] **Step 1: Write plan assertions for table controls**

Require PAY_PER_REQUEST, PITR, TTL, deletion protection, 100/25 maximum throughput, encryption and no global replicas/Contributor Insights/autoscaling.

- [ ] **Step 2: Write queue boundary tests**

Require one base queue plus DLQ only; absent email/Telegram provider queues; redrive maxReceiveCount; no public policy.

- [ ] **Step 3: Implement resources and least-privilege canary role**

The canary role can send/receive/delete only explicit canary bodies and has no DynamoDB delivery mutation.

- [ ] **Step 4: Validate with local queue contract**

    uv run --locked --extra dev pytest -q tests/production/test_data_queue_plan.py tests/integration/test_notifications_local.py

- [ ] **Step 5: Commit**

    git add infra/opentofu/modules tests/production/test_data_queue_plan.py
    git commit -m "feat(infra): harden demo tables and base queue"

### Task 3: Provision Exact Cognito PKCE and Admin-Only Tenancy

**Branch:** feat/prod-infra-003-cognito

**Files:**
- Modify: infra/opentofu/modules/core-cognito/**
- Create: scripts/production/seed_cognito_demo.py
- Create: tests/production/test_cognito_plan.py
- Create: tests/production/test_cognito_seed.py

**Interfaces:**
- User Pool domain prefix is exactly limnopulse-portfolio-demo.
- Public SPA client uses code flow+PKCE, no secret, exact callback/logout URLs and no SMS/social/M2M.
- Seed uses AdminCreateUser MessageAction=SUPPRESS followed by AdminSetUserPassword Permanent=true.

- [ ] **Step 1: Write exact-domain and flow tests**

Reject computed suffixes, implicit domains, client secret, implicit grant, self-registration, SMS MFA and wildcard URLs.

- [ ] **Step 2: Implement User Pool/domain/client**

Set allow_admin_create_user_only=true. Output issuer, client ID and absolute authorization base URL as public values.

- [ ] **Step 3: Write idempotent seed tests**

Consume reviewed email/password through secure file descriptors; never generate/log/store password in release artifacts. Existing mismatched user fails.

- [ ] **Step 4: Validate and commit**

    uv run --locked --extra dev pytest -q tests/production/test_cognito_plan.py tests/production/test_cognito_seed.py
    tofu -chdir=infra/opentofu validate -no-color
    git add infra/opentofu/modules/core-cognito scripts/production tests/production
    git commit -m "feat(auth): provision exact portfolio cognito flow"

### Task 4: Create the Private Static Distribution

**Branch:** feat/prod-infra-004-static-web

**Files:**
- Create: infra/opentofu/modules/static-web/main.tf
- Create: infra/opentofu/modules/static-web/variables.tf
- Create: infra/opentofu/modules/static-web/outputs.tf
- Create: infra/opentofu/modules/static-web/function.js
- Modify: infra/opentofu/env/prod/main.tf
- Create: tests/production/test_static_web_plan.py
- Create: tests/production/test_spa_rewrite.js

**Interfaces:**
- Private us-east-2 S3 origin, OAC, CloudFront, global_edge ACM/WAF and response headers.
- Exports distribution ID/domain, bucket name and ACM validation records only.
- Rewrite maps extensionless app routes to /index.html and excludes assets/metadata.

- [ ] **Step 1: Write resource and public-access tests**

Require account/bucket block, bucket-owner-enforced, TLS policy, OAC SourceArn restriction, versioning, no website endpoint/ACL and dedicated WAF.

- [ ] **Step 2: Write cache/security-header tests**

Immutable release assets one year; root index bounded revalidation; CSP report-only input switch and enforcing prod acceptance; HSTS, nosniff, frame, referrer and permissions policies.

- [ ] **Step 3: Implement rewrite and its table tests**

Cases include /, /dashboard, /assets/app.js, /runtime-config.json, /favicon.ico and malformed URI.

- [ ] **Step 4: Add lifecycle below voluntary 5 GB**

Retain releases eligible for rollback; never expire the current/prior referenced unit.

- [ ] **Step 5: Validate and commit**

    node --test tests/production/test_spa_rewrite.js
    uv run --locked --extra dev pytest -q tests/production/test_static_web_plan.py
    tofu -chdir=infra/opentofu validate -no-color
    git add infra/opentofu/modules/static-web infra/opentofu/env/prod tests/production
    git commit -m "feat(web-infra): add private cloudfront distribution"

### Task 5: Add Runtime IAM, Alarms and Cost Policy

**Branch:** feat/prod-infra-005-iam-observability

**Files:**
- Create: infra/opentofu/modules/runtime-iam/**
- Create: infra/opentofu/modules/observability/**
- Create: policies/limnopulse.rego
- Create: schemas/cost-manifest.schema.json
- Create: tests/production/test_iam_alarm_cost_plan.py

**Interfaces:**
- Distinct API, evaluator, SQS canary and backup policies; product Roles Anywhere role itself remains shared-state owned.
- CloudWatch alarms cover DynamoDB/SQS and bounded application-facing AWS failures.
- Cost manifest max is at most USD 3.

- [ ] **Step 1: Write IAM allow/deny matrices**

API reads exact table/index and Cognito identity operations needed by code; evaluator exact due-rule/event actions; canary exact queue; no wildcard table/prefix or cross-project SSM.

- [ ] **Step 2: Add alarm tests**

Throttles/system errors, queue age/depth/DLQ, budget/anomaly reference. No Container Insights or high-cardinality dimension.

- [ ] **Step 3: Add OPA forbidden-service and tag policy**

Reject NAT, ALB, database/cache/IoT/global/paid messaging. Require Project=limnopulse, Environment=prod, ManagedBy=opentofu, Owner=vinisantana where supported.

- [ ] **Step 4: Validate plan fixtures and commit**

    conftest test tests/fixtures/plans --policy policies
    uv run --locked --extra dev pytest -q tests/production/test_iam_alarm_cost_plan.py
    git add infra/opentofu/modules policies schemas tests
    git commit -m "feat(infra): add least privilege alarms and cost policy"

### Task 6: Build Credential-Free Pull-Request CI

**Branch:** ci/prod-006-validation

**Files:**
- Create: .github/workflows/verify.yml
- Modify: Makefile
- Create: tests/production/test_ci_workflow.py

**Interfaces:**
- Jobs: python, go, frontend, compose, oci, opentofu-policy.
- permissions at workflow root are contents:read.
- No id-token, packages write, secrets or production environment on pull requests.

- [ ] **Step 1: Write workflow contract tests**

Require pinned full-SHA actions, GitHub-hosted runners, locked installs, unconditional integration teardown and uploaded SBOM/test artifacts only.

- [ ] **Step 2: Add Make targets used identically locally/CI**

verify-python, verify-go, verify-web, verify-compose-production, verify-oci and verify-tofu-policy.

- [ ] **Step 3: Implement workflow matrix**

Use cache keys bound to lockfiles; do not cache credentials. Build images but do not push on PR. Generate Syft SBOM and scan with reviewed Trivy severity policy.

- [ ] **Step 4: Run workflow/static tests and local aggregate**

    uv run --locked --extra dev pytest -q tests/production/test_ci_workflow.py
    actionlint .github/workflows/verify.yml
    make verify

- [ ] **Step 5: Commit**

    git add .github/workflows/verify.yml Makefile tests/production/test_ci_workflow.py
    git commit -m "ci(prod): validate immutable portfolio artifacts"

### Task 7: Define and Build the Immutable Release Manifest

**Branch:** feat/prod-delivery-007-release-manifest

**Files:**
- Create: release/manifest.schema.json
- Create: release/build_manifest.py
- Create: release/verify_manifest.py
- Create: tests/production/test_release_manifest.py
- Create: .github/workflows/release.yml

**Interfaces:**
- Manifest binds source SHA, CI run IDs, API/evaluator/publisher digests, frontend checksum, Compose checksum, SBOM checksums and OpenTofu/provider-lock digest.
- Manifest expires and is canonical JSON; no mutable tag is authoritative.

- [ ] **Step 1: Write canonical schema and mixing tests**

Reject image tag without digest, mismatched source/run, missing SBOM, altered Compose/web checksum, future schema, expired manifest and secret-like field.

- [ ] **Step 2: Implement deterministic canonical JSON builder**

Sort keys, UTF-8, no whitespace, SHA-256 sidecar. Validate every path/digest against files produced by the same run.

- [ ] **Step 3: Implement main-only release workflow**

It requires verify workflow success for the exact main SHA, builds once, pushes GHCR by digest, creates web archive and uploads one bounded release artifact. permissions are contents:read, packages:write only for this job.

- [ ] **Step 4: Verify and commit**

    uv run --locked --extra dev pytest -q tests/production/test_release_manifest.py
    actionlint .github/workflows/release.yml
    git add release tests/production/test_release_manifest.py .github/workflows/release.yml
    git commit -m "feat(release): bind portfolio artifacts to one sha"

### Task 8: Add Manual Product Plan and Apply Callers

**Branch:** ci/prod-008-plan-apply

**Files:**
- Create: .github/workflows/infra-plan.yml
- Create: .github/workflows/infra-apply.yml
- Create: release/infra-plan.schema.json
- Create: tests/production/test_infra_workflows.py

**Interfaces:**
- Plan calls the exact immutable LimnoPulse plan gate in personal-infra-live.
- Apply inputs are plan_run_id, artifact_id and artifact_digest; it calls the exact deploy gate.
- Apply never replans or runs on merge.

- [ ] **Step 1: Write trust/gate ordering tests**

Artifact verification is before reusable call/OIDC. Caller permissions are contents:read, actions:read, id-token:write only where reusable gate needs it.

- [ ] **Step 2: Implement plan artifact composition**

Include binary/redacted plan, SHA, backend key, lock digest, policy, cost<=3, aggregate<=15 evidence, expiry and manifest digest.

- [ ] **Step 3: Implement apply caller**

Require main SHA/ref and exact plan identity. No destroy input and no auto-approve except exact reviewed binary plan inside reusable gate.

- [ ] **Step 4: Validate and commit**

    uv run --locked --extra dev pytest -q tests/production/test_infra_workflows.py
    actionlint .github/workflows/infra-*.yml
    git add .github/workflows release tests/production/test_infra_workflows.py
    git commit -m "ci(infra): add manual limnopulse plan and apply"

### Task 9: Implement CloudFront Free Subscription Reconciliation

**Branch:** feat/prod-delivery-009-free-plan

**Files:**
- Create: scripts/deploy/reconcile_cloudfront_free.py
- Create: tests/production/test_cloudfront_free.py
- Create: docs/runbooks/cloudfront-free.md

**Interfaces:**
- Inputs are exact distribution ID/ARN and dedicated WAF ARN plus expected FREE tier.
- Endpoint region is us-east-1.
- Output evidence is subscription ARN, ETag, association hashes and ACTIVE state without account identifiers.

- [ ] **Step 1: Write eligibility/quota/refusal tests**

Reject ineligible account, fewer than two phase-one slots before first cutover, conflicting subscription, paid tier, ambiguous association, replacement/cancel request and non-ACTIVE result.

- [ ] **Step 2: Implement list/read/create-if-absent/re-read**

Use a typed API adapter for fixture tests. Operation is idempotent only for the exact distribution/WAF/FREE tuple.

- [ ] **Step 3: Add drift-only mode**

Every later plan invokes check mode; it never mutates. The deployment mode is explicit and manual, outside OpenTofu/local-exec.

- [ ] **Step 4: Run and commit**

    uv run --locked --extra dev pytest -q tests/production/test_cloudfront_free.py
    git add scripts/deploy tests/production/test_cloudfront_free.py docs/runbooks/cloudfront-free.md
    git commit -m "feat(edge): reconcile exact cloudfront free subscription"

### Task 10: Implement Atomic Static Release and Rollback

**Branch:** feat/prod-delivery-010-static-release

**Files:**
- Create: scripts/deploy/publish_static.py
- Create: scripts/deploy/rollback_static.py
- Create: tests/production/test_static_release.py
- Create: docs/runbooks/static-release.md

**Interfaces:**
- Uploads releases/<release_id>/ immutable unit completely, verifies checksum/content type/cache, then writes root index.html last.
- Rollback copies/restores the prior versioned root index and invalidates only entrypoints.

- [ ] **Step 1: Write failure-injection tests**

Network/error at each object leaves root index unchanged. Index referencing mixed release IDs is rejected. Existing release object with different checksum is immutable conflict.

- [ ] **Step 2: Implement runtime-config generation**

Generate public config from exact OpenTofu outputs and manifest; reject unknown/secret fields. All asset references must begin with one release prefix.

- [ ] **Step 3: Implement last-write and version evidence**

Use conditional write against prior ETag/version where supported; record new/prior version IDs. Invalidate /index.html only.

- [ ] **Step 4: Run emulator/fake tests and commit**

    uv run --locked --extra dev pytest -q tests/production/test_static_release.py
    git add scripts/deploy tests/production/test_static_release.py docs/runbooks/static-release.md
    git commit -m "feat(delivery): publish atomic static release units"

### Task 11: Implement Manual Host Promotion and Timers

**Branch:** feat/prod-delivery-011-host-promotion

**Files:**
- Create: .github/workflows/promote-production.yml
- Create: release/host-envelope.schema.json
- Create: scripts/deploy/build_host_archive.py
- Create: scripts/deploy/verify_promotion.py
- Create: config/systemd/limnopulse-evaluator.service
- Create: config/systemd/limnopulse-evaluator.timer
- Create: config/systemd/limnopulse-synthetic.service
- Create: config/systemd/limnopulse-synthetic.timer
- Create: tests/production/test_promotion.py
- Create: tests/production/test_systemd_units.py

**Interfaces:**
- Workflow verifies a green main release artifact with its repository GITHUB_TOKEN, then calls immutable personal-infra-live LimnoPulse host gate.
- Host archive contains manifest, production Compose and non-secret config only.
- Timers invoke fixed digest-bound one-shot services and share the platform lease.

- [ ] **Step 1: Write workflow evidence tests**

Require actions:read, contents:read and packages:read only; exact run/artifact/SHA/digest/expiry verification before reusable gate. No PAT or SSH secret.

- [ ] **Step 2: Write timer overlap/credential tests**

Evaluator and publisher use flock/host dispatcher, no permanent loop, conservative cadence, bounded timeout and only run after renewed credential evidence where AWS is needed.

- [ ] **Step 3: Implement promotion sequence**

Candidate alternate loopback -> Cognito/membership/Dynamo/Influx read smoke -> Nginx switch -> timer digest update -> static immutable upload/root switch -> synthetic publish -> wait window -> evaluator -> incident/read verification -> redacted evidence.

- [ ] **Step 4: Implement rollback evidence**

Restore prior API upstream/digest, timer digests and root index. Never remove Influx volume or roll DynamoDB backward.

- [ ] **Step 5: Validate and commit**

    uv run --locked --extra dev pytest -q tests/production/test_promotion.py tests/production/test_systemd_units.py
    actionlint .github/workflows/promote-production.yml
    git add .github/workflows release scripts/deploy config/systemd tests/production
    git commit -m "feat(delivery): promote portfolio through shared host gate"

### Task 12: Add Production Acceptance, Backup and Cost-Freeze Drills

**Branch:** test/prod-delivery-012-acceptance

**Files:**
- Create: tests/production/test_delivery_acceptance.py
- Create: docs/runbooks/production-acceptance.md
- Create: docs/runbooks/influx-backup-restore.md
- Create: docs/runbooks/rollback.md
- Create: docs/runbooks/cost-freeze.md

**Interfaces:**
- Produces the final release checklist, not an automatic deployment.
- Requires separate operator evidence for real AWS/Cloudflare/VPS steps.

- [ ] **Step 1: Add the pre-production matrix**

Cover exact CORS, portfolio startup absence, renewal, Compose isolation, MQTT matrix, cross-tenant denial, Dynamo semantics, SQS canary, Influx backup/restore, unknown-device drop, evaluator fencing, deterministic first incident, CDN private origin/release rollback/CSP/no SW, exact Cognito, disabled resource/secret absence, visitor-IP rate limit and aggregate cost.

- [ ] **Step 2: Add production smoke commands**

Verify limnopulse.com release/TLS/private origin, API only through Tunnel, PKCE membership, synthetic MQTT->Influx->API, evaluator incident, zero provider calls, backup checksum and API/static rollback.

- [ ] **Step 3: Add quarterly isolated Influx restore**

Restore into a new volume/container and query one synthetic bounded time range; never replace production volume.

- [ ] **Step 4: Add cost-freeze rehearsal**

Prove promotion/new costly automation denied at USD 15 while API reads, audit/backup and MFA break-glass remain available.

- [ ] **Step 5: Run the complete non-mutating gate**

    make verify
    make verify-production-application
    tofu -chdir=infra/opentofu fmt -check -recursive
    tofu -chdir=infra/opentofu init -backend=false -input=false
    tofu -chdir=infra/opentofu validate -no-color
    conftest test tests/fixtures/plans --policy policies
    uv run --locked --extra dev pytest -q tests/production
    git diff --check

- [ ] **Step 6: Commit**

    git add tests/production/test_delivery_acceptance.py docs/runbooks
    git commit -m "test(prod): define infrastructure and delivery acceptance"

## Execution Order

Tasks 1-3 are serial around existing state addresses. Tasks 4 and 5 may proceed after Task 1. Task 6 waits for the application plan. Task 7 waits for Task 6. Task 8 waits for Tasks 1-7 and the personal-infra-live gate. Task 9 waits for Task 4. Task 10 waits for Tasks 4 and 7. Task 11 waits for Tasks 7-10 and the infra-ansible dispatcher. Task 12 is final. Real deployment remains separate and manual.

## Plan Self-Review Record

- Resource ownership: no shared KMS, SSM values, Budget, OIDC, Roles Anywhere, tunnel, DNS or host resource is recreated.
- Feature absence: unconditional SES/Telegram resources are eliminated; disabled plans contain zero related address/secret/IAM output.
- Region consistency: us-east-2 default; only ACM/WAF/Pricing Plan Manager uses us-east-1.
- Cost consistency: product max USD 3, two-slot Free-plan gate and aggregate USD 15 evidence.
- Delivery consistency: one manifest binds all artifacts; plan/apply/promotion are manual and immutable.
- Rollback consistency: API, timers and root index restore independently without deleting Influx/Dynamo data.
- Completeness scan: no unresolved marker, paid fallback or implicit provider selection.

## Execution Handoff

Use isolated worktrees and reviewed PRs. Merge Tasks 1-12 before requesting the first real OpenTofu plan. The first production rollout must follow LimnoPulse acceptance and rollback drills before any CnesData production work begins.
