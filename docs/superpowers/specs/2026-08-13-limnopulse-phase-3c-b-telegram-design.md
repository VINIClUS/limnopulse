# Phase 3C-B Telegram Delivery Design

**Date:** 2026-08-13  
**Status:** Implemented design  
**Depends on:** Phase 3C-A durable notification relay and worker

## Goal

Phase 3C-B adds verified, tenant-scoped Telegram delivery without changing the
email identity, queue, retry, SES feedback, or scheduler contracts. DynamoDB
remains the source of truth. SQS is at-least-once transport and Redis is only a
fail-closed rate limiter.

The initial Telegram webhook runs in the existing FastAPI process:

```text
POST /webhooks/telegram
```

It is public relative to Cognito and authenticates Telegram with
`X-Telegram-Bot-Api-Secret-Token`. There is no dedicated webhook Lambda in this
slice.

## Explicit boundaries

Three independent credentials have three independent purposes:

- the secret header authenticates the Telegram webhook origin;
- a short-lived one-time binding token associates one private `chat_id` with a
  tenant recipient;
- a Cognito access token plus active tenant membership authorizes issuing or
  revoking bindings and changing `telegram_enabled` with optimistic locking.

The webhook validates its secret before reading a bounded payload, accepts only
private messages whose sender matches the chat, parses `/start <token>` and
`/stop`, and delegates to `TelegramBindingService`. It never sends a
notification and never changes `telegram_enabled`.

`/start <token>` only establishes or verifies `TelegramBinding.status=verified`.
An authenticated preference update is still required to enable delivery. A
`/stop` suppresses the destination immediately but leaves binding and preference
history durable. A new valid `/start` can reactivate that same destination.

## Domain and storage model

`TelegramBinding` is separate from `NotificationPreference`. The relevant
single-table records are:

```text
TENANT#<tenant_id> / TELEGRAM_BINDING#USER#<recipient_id>
TENANT#<tenant_id> / TELEGRAM_BINDING_REQUEST#USER#<recipient_id>
TELEGRAM_BINDING_TOKEN#<sha256(token)> / META
TELEGRAM_DESTINATION#<sha256(chat_id)> / META
TELEGRAM_UPDATE#<update_id> / META
```

Only the token hash is persisted. Tokens expire after ten minutes and Telegram
update dedupe rows expire after eight days. Consuming a token uses one
`TransactWriteItems` containing update dedupe, active-membership fence, token
and pointer consumption, destination claim/reactivation, and versioned binding.
The transaction is safe under duplicate webhook updates and concurrent token
use.

The FastAPI service depends on repository and membership ports, so it can move
to another runtime later without changing the domain command contract.

## Authenticated API

```text
GET    /v1/tenants/{tenant_id}/me/telegram-binding
POST   /v1/tenants/{tenant_id}/me/telegram-binding-token
DELETE /v1/tenants/{tenant_id}/me/telegram-binding
GET    /v1/tenants/{tenant_id}/me/notification-preference
PUT    /v1/tenants/{tenant_id}/me/notification-preference
```

Tenant routes reuse the existing membership authorization. Preference writes
retain `expected_version`. Enabling Telegram requires an active membership and
a verified, active binding; the response exposes requested and effective state
without returning a chat ID.

## Relay and immutable snapshots

Email remains relay schema v1. Telegram uses relay schema v2 and a separate SQS
queue. The deterministic delivery identity continues to include event, kind,
channel, and recipient, so email IDs are unchanged.

The relay resolves current membership, preference, binding and destination by
known-key reads. A Telegram Delivery durably snapshots `destination_id`,
`telegram_chat_id`, membership, template ID/version/locale, rendered plain text,
and content hash. The compact SQS job contains only durable identifiers and the
channel; it contains neither chat ID nor rendered content.

Opening and recovery templates are deterministic Portuguese plain text. They
have a 3,800-rune bound, preserve the alert URL under truncation, use an HTTPS
application URL outside local/test, set no parse mode, and disable link preview.
Recovery reuses the opening destination and is cancelled if the opening was not
confirmed or current authorization/destination gates fail.

## Telegram worker

`notifications telegram-worker` continuously consumes only the dedicated
Telegram queue. Before each provider call it consistently rechecks membership,
preference, binding, destination, minimum severity, and Alert Event lifecycle.
`BeginAttempt` repeats those exact values as DynamoDB transaction fences.

The shared durable processor retains the five-call limit, leases, revisions,
Attempts, idempotent duplicate handling, and graceful drain. Telegram adds:

- one global and one destination token bucket in a single Redis Lua operation;
- Redis `TIME`, opaque hash-tagged keys, and no raw token or chat ID in Redis;
- fail-closed durable deferral when Redis is unavailable;
- Bot API 429 as retryable with provider `retry_after` as a floor;
- 401 as fatal credentials failure;
- definite 4xx destination rejection as permanent plus atomic destination
  suppression;
- 5xx as retryable service failure;
- transport loss or malformed/unconfirmed response as `unknown`, with no
  automatic second provider call because Telegram has no feedback channel.

The sender calls `sendMessage` with plain text and disabled link preview. It
stores only the provider message ID. Errors, logs, metrics and command summaries
must not include bot token, chat ID, text, binding token, or webhook secret.

## Secrets and flags

Hosted runtimes retrieve the bot token and webhook secret from separate AWS
Secrets Manager containers. OpenTofu creates containers and least-privilege IAM
policies but never creates a secret version or stores a secret value in state.
Local/test may use direct values.

`TELEGRAM_DELIVERY_ENABLED` is explicit in hosted evaluator, relay and worker
deployments. New Telegram outboxes, publishing and sending stay disabled until
their respective runtime is enabled. Queue separation prevents an email worker
from consuming a Telegram job or the inverse.

## Migration and operations

`notifications backfill-telegram` accepts explicit tenants and supports dry-run
and apply modes. It discovers outboxes only with paginated DynamoDB `Query`,
never `Scan`, and conditionally migrates missing or
`deferred_unsupported_channel` Telegram rows to schema v2. It leaves email rows
unchanged and is idempotent under replays and concurrent updates.

Deployment provisions dependencies and secrets first, deploys the webhook and
compatible binaries with delivery off, verifies binding, runs backfill, starts
the worker, and only then enables relay/evaluator Telegram flags. Rollback stops
new Telegram publication first and preserves durable rows, queue and DLQ.

## Verification

Regression coverage includes:

- missing/wrong secret rejection before body processing and bounded envelope;
- valid, invalid, expired, duplicate and concurrent binding commands;
- authenticated membership and preference optimistic-locking boundaries;
- immutable rendering, Unicode truncation, URL and email identity compatibility;
- channel-specific queue routing and schema fencing;
- backfill pagination/idempotency without Scan;
- worker gate races, retry taxonomy, permanent suppression, ambiguous unknown,
  rate limiting and Redis failure;
- multiprocess DynamoDB Local, ElasticMQ, Redis and WireMock delivery proving one
  durable Attempt and one provider request.

