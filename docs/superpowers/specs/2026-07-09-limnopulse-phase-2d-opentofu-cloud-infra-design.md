# Limnopulse Phase 2D OpenTofu Cloud Infra Design

**Date:** 2026-07-09
**Status:** Approved by continuation
**Scope:** Cloud infrastructure scaffold and local/cloud infra boundary

## Context

The project now has a FastAPI backend, DynamoDB single-table domain model, Redis cache-aside, InfluxDB telemetry reads, and a local MQTT/Telegraf ingestion scaffold. The user clarified the infrastructure direction: Docker Compose is for development, and OpenTofu is for cloud.

## Goals

- Record the dev/cloud infrastructure split in repository docs.
- Add an `infra/opentofu` scaffold for AWS cloud resources already implied by the architecture.
- Keep the scaffold safe to review and validate locally without AWS credentials.
- Avoid real secrets, real account identifiers, or accidental cloud apply.
- Preserve the target architecture: Cognito, DynamoDB on-demand, Redis cache-only, SQS + DLQ, SES, and no PostgreSQL/Firestore/Firebase.

## Non-Goals

- No `tofu apply` or cloud deployment.
- No real remote state bucket, lock table, AWS account id, domain, SES identity, or secret values.
- No production MQTT broker, TLS/mTLS, broker ACLs, or IoT Core implementation.
- No Go workers, alert evaluator, notification worker, Telegram, WhatsApp, or SMS.
- No Redis/ElastiCache or InfluxDB Cloud provisioning yet; those require network/provider choices outside this slice.

## Architecture

Docker Compose remains the local developer runtime for Redis, DynamoDB Local, InfluxDB, Mosquitto, and Telegraf.

OpenTofu owns cloud infrastructure under `infra/opentofu`:

- AWS provider configuration and remote-state backend example.
- DynamoDB `LimnopulseDomain` and `LimnopulseAudit` tables with `PK`/`SK` string keys and on-demand billing.
- Cognito User Pool and app client for API authentication.
- SQS alert queue plus DLQ as a future worker boundary.
- Optional SES email identity, disabled by default unless a verified email/domain is provided.
- Outputs matching application environment variables where possible.

## Safety Model

The scaffold must be reviewable with static tests and `tofu fmt`/`tofu validate` when OpenTofu is installed. Example variable files use placeholders only. `.gitignore` must exclude state files, local backend configs, plan files, and non-example tfvars.

## Acceptance Criteria

- `infra/opentofu` contains OpenTofu HCL files and examples with no secrets.
- README documents Compose for development and OpenTofu for cloud.
- Static tests verify OpenTofu file layout and required resources.
- DynamoDB cloud tables use `PAY_PER_REQUEST` and `PK`/`SK`.
- Cognito outputs include pool id, client id, and issuer URL.
- SQS includes a DLQ and redrive policy.
- SES identity is optional and disabled by default.
- No PostgreSQL, Firestore, Firebase, real secrets, or local-only Docker services are introduced into the cloud scaffold.
