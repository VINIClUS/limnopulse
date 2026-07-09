# Limnopulse Phase 2D OpenTofu Cloud Infra Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a safe OpenTofu cloud infrastructure scaffold while preserving Docker Compose as the local development runtime.

**Architecture:** Keep Compose in the repository root for local dependencies. Add `infra/opentofu` for AWS cloud resources: DynamoDB on-demand tables, Cognito User Pool/client, SQS + DLQ, and optional SES identity. Leave Redis cloud, InfluxDB managed provisioning, and production MQTT hardening as explicit later decisions.

**Tech Stack:** OpenTofu HCL, AWS provider, pytest static tests, Docker Compose for local development.

## Global Constraints

- Docker Compose is for development only.
- OpenTofu is for cloud infrastructure.
- Do not run `tofu apply` or create real cloud resources in this slice.
- Do not commit secrets, real AWS account identifiers, real remote-state bucket names, or real SES identities.
- DynamoDB domain and audit tables use string `PK` and `SK` keys with `PAY_PER_REQUEST`.
- Redis remains cache-only.
- Do not add PostgreSQL, Firestore, or Firebase.
- Do not add production MQTT TLS/mTLS, Go workers, SQS consumers, SES sending code, Telegram, WhatsApp, or SMS.
- Shell commands should be prefixed with `rtk`.

---

### Task 1: OpenTofu Static Tests

**Files:**
- Create: `tests/unit/test_opentofu_infra.py`

**Interfaces:**
- Produces static tests for expected OpenTofu layout, resources, examples, and guardrails.

- [x] Add tests that assert `infra/opentofu` contains `versions.tf`, `providers.tf`, `variables.tf`, `dynamodb.tf`, `cognito.tf`, `queues.tf`, `ses.tf`, `outputs.tf`, `backend.example.hcl`, `env/cloud.tfvars.example`, and `README.md`.
- [x] Add tests for AWS provider pinning, OpenTofu required version, and S3 backend placeholder.
- [x] Add tests for DynamoDB on-demand `PK`/`SK` domain and audit tables.
- [x] Add tests for Cognito pool/client outputs matching app environment names.
- [x] Add tests for SQS + DLQ redrive policy.
- [x] Add tests for optional SES identity and no real secrets.
- [x] Run the targeted test and confirm it fails because the scaffold is absent.

### Task 2: OpenTofu Scaffold

**Files:**
- Create: `infra/opentofu/versions.tf`
- Create: `infra/opentofu/providers.tf`
- Create: `infra/opentofu/variables.tf`
- Create: `infra/opentofu/dynamodb.tf`
- Create: `infra/opentofu/cognito.tf`
- Create: `infra/opentofu/queues.tf`
- Create: `infra/opentofu/ses.tf`
- Create: `infra/opentofu/outputs.tf`
- Create: `infra/opentofu/backend.example.hcl`
- Create: `infra/opentofu/env/cloud.tfvars.example`
- Create: `infra/opentofu/README.md`
- Modify: `.gitignore`

**Interfaces:**
- Produces a non-secret cloud IaC baseline that can be formatted/validated by OpenTofu when installed.

- [x] Add OpenTofu and AWS provider configuration.
- [x] Add DynamoDB domain/audit tables.
- [x] Add Cognito User Pool/client and outputs.
- [x] Add SQS alert queue and DLQ.
- [x] Add optional SES identity.
- [x] Add placeholder backend and variable examples.
- [x] Ignore OpenTofu local state, plans, non-example tfvars, and local backend configs.
- [x] Run targeted static tests until they pass.

### Task 3: Docs And Verification

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/plans/2026-07-09-limnopulse-phase-2d-opentofu-cloud-infra.md`

**Interfaces:**
- Produces clear local/cloud run guidance and final evidence.

- [x] Document Compose as development-only.
- [x] Document safe local OpenTofu validation using `tofu init -backend=false`, `tofu fmt -check`, and `tofu validate`.
- [x] Document resources intentionally out of scope.
- [x] Run `python -m pytest -q`.
- [x] Run `python -m compileall src scripts tests`.
- [x] Run `docker compose config`.
- [x] Run OpenTofu validation if `tofu` is installed; otherwise report it as not installed.
- [x] Run anti-requirement search over execution artifacts.

## Self-Review

- The plan does not provision real cloud resources.
- The plan adds cloud IaC only under `infra/opentofu`.
- The plan keeps local Docker Compose separate from cloud OpenTofu.
