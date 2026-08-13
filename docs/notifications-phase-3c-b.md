# Telegram Notification Delivery Phase 3C-B Operations

Phase 3C-B adds tenant-scoped Telegram binding and delivery to the Phase 3C-A
durable notification pipeline. It uses a dedicated SQS queue and continuous
`notifications telegram-worker`. The existing one-shot relay remains externally
scheduled and routes each channel to its own queue. DynamoDB remains
authoritative for binding, destination, preference, Delivery, Attempt,
transition and idempotency state. Redis is only a fail-closed rate limiter.

## Security and authorization boundaries

The three credentials are not interchangeable:

- `X-Telegram-Bot-Api-Secret-Token` authenticates the origin of
  `POST /webhooks/telegram` before FastAPI reads its bounded body;
- the one-time binding token is ten-minute proof that associates one private
  Telegram chat with one tenant recipient, and only its SHA-256 digest is
  persisted;
- the Cognito access token authenticates the user whose active tenant
  membership authorizes token issue, binding revocation and preference updates.

The webhook accepts only a valid Telegram envelope from a private chat where
sender and chat IDs match. `/start <token>` verifies the separate binding and
`/stop` suppresses the destination. The webhook sends no notification and does
not change `telegram_enabled`. Enabling the preference remains an authenticated,
optimistically locked API operation that requires an active membership and
verified binding.

The destination stores the greatest applied Telegram `update_id`. A webhook
command with an older ID is acknowledged without changing state, so delayed
`/start` cannot reactivate a destination after a newer `/stop` (and vice versa).

Webhook secret and bot token use separate Secrets Manager secrets and IAM
policies. OpenTofu creates only secret containers; populate values through the
approved secret-management process so no secret version enters source, plan or
state. Hosted FastAPI accepts only the webhook secret ARN. Hosted workers accept
only the bot token secret ARN. Hosted FastAPI also requires an environment-specific
`TELEGRAM_BOT_USERNAME`; the local fixture username is rejected at startup.

## Durable delivery flow

```text
FastAPI binding token -> Telegram /start -> verified binding transaction
Authenticated preference API -> telegram_enabled=true
Alert evaluator -> Telegram NotificationOutbox schema v2
One-shot relay -> immutable Telegram Delivery -> dedicated SQS job
Telegram worker -> fenced Attempt -> Redis limiter -> Bot API sendMessage
```

The Delivery snapshot owns the destination ID, private chat ID and rendered
content. The chat ID never appears in the SQS job, logs, metrics or command
summary. The job contains only durable IDs, kind and channel. Bot token, webhook
secret, binding token and message text are also excluded from logs and metrics.

Opening and recovery use deterministic Portuguese plain text with a 3,800-rune
limit. No Telegram parse mode is selected and link previews are disabled. A
recovery uses the opening destination and is not sent when the opening was not
confirmed or current membership, preference, binding, destination or event
lifecycle gates fail.

Telegram has no provider feedback channel in this phase. A successful Bot API
response records its message ID. A 429 is retried no sooner than its
`retry_after`; definite destination rejection is permanent and atomically
suppresses the destination; 5xx is retryable; lost or malformed confirmation
ends durably as `unknown` after one provider call and is not automatically
resent. Duplicate SQS messages remain safe through leases, revisions,
deterministic identity and conditional transactions.

When the Redis global or destination bucket is empty—or Redis is temporarily
unavailable—the worker waits while its lease guard renews and retries the
limiter. It does not change SQS visibility for limiter contention, preserving
the queue receive budget for actual poison messages.

## Local execution

Initialize the data plane and start the Telegram dependencies:

```bash
docker compose --profile notifications up -d \
  redis dynamodb-local elasticmq telegram-bot-api-fake
python scripts/dev/init_dynamodb.py
```

Run the dedicated worker and the one-shot relay:

```bash
docker compose --profile notifications up telegram-notification-worker
docker compose --profile notifications run --rm notification-relay
```

WireMock listens on `127.0.0.1:8089` for integration inspection and returns a
deterministic Telegram message ID. It must never be configured in staging or
production. The local `.env.example` values are non-production fixtures.

Run the complete opt-in notification integration suite:

```bash
RUN_NOTIFICATION_INTEGRATION=1 \
  python -m pytest -q tests/integration/test_notifications_local.py
```

The Telegram case uses DynamoDB Local, ElasticMQ, Redis and WireMock and proves
one queue message creates one Attempt and one provider call.

## Cloud rollout

Use environment-specific plans and normal change control. Keep
`TELEGRAM_DELIVERY_ENABLED=false` on every delivery runtime until dependencies,
binding and migration are verified.

1. Provision the dedicated Telegram queue and DLQ, Redis connectivity, both
   Secrets Manager containers and the least-privilege worker/FastAPI policies.
2. Populate both secret containers through the approved secret manager, grant
   each runtime only its required secret, configure `TELEGRAM_BOT_USERNAME` on
   FastAPI, and record rotation ownership.
3. Deploy the FastAPI binding and webhook code. Keep evaluator, relay and worker
   Telegram delivery disabled. Verify authenticated issue/get/revoke APIs.
4. Register the Telegram webhook with its HTTPS URL and matching secret header.
   Exercise `/start` with a disposable binding, then enable/disable the
   preference through the Cognito-authenticated API.
5. Deploy the evaluator, relay and worker binaries with Telegram flags still
   explicitly `false`. The explicit relay setting causes it to bypass indexed
   Telegram rows before they consume discovery caps; it still never claims or
   publishes them. Verify the worker has only its queue, data-plane and
   bot-secret IAM.
6. Run `notifications backfill-telegram` for explicit tenant batches. It never
   uses DynamoDB Scan; it uses paginated `Query` and conditional updates:

   Pause the evaluator scheduler before the final backfill. While delivery is
   disabled, new Telegram outboxes are marked `deferred_unsupported_channel`;
   pausing the evaluator prevents a new deferred opening from appearing after
   the final clean dry-run and before relay/evaluator activation.

   ```bash
   notifications backfill-telegram --tenant-file tenants.txt --max-rows 10000 --timeout 5m
   notifications backfill-telegram --tenant-file tenants.txt --apply --max-rows 10000 --timeout 5m
   notifications backfill-telegram --tenant-file tenants.txt --max-rows 10000 --timeout 5m
   ```

   Continue bounded idempotent batches until the dry-run reports zero rows
   needing update. Investigate schema conflicts rather than overwriting them.
   Keep the evaluator paused until the relay and evaluator flags are enabled in
   the next step.
7. Start the Telegram worker with `TELEGRAM_DELIVERY_ENABLED=true` while relay
   and evaluator remain false. Confirm Redis and Bot API health, zero DLQ input,
   and graceful shutdown behavior.
8. Enable Telegram on the relay and evaluator, in that order, for a controlled
   tenant cohort. Confirm one opening and recovery before broader activation.
9. Monitor the queue, DLQ, limiter and durable states. Track queue age,
   throttling, retryable failures, permanent destination suppression,
   `unknown`, active leases, provider-call count and worker drain failures.

Hosted evaluator, relay and worker reject an omitted Telegram flag. The relay
also requires a valid public `LIMNOPULSE_WEB_URL`; hosted workers reject a Bot
API base URL override and direct bot token.

## Rollback

If behavior is unsafe, turn off Telegram on the relay and evaluator first. This
stops new publication and outbox indexing without changing email. Gracefully
drain the Telegram worker, then inspect queued, processing, retryable,
permanent, cancelled and unknown Deliveries. Preserve DynamoDB Delivery and
Attempt rows, bindings, destinations, SQS messages and the DLQ; they are the
replay and incident evidence.

Do not delete a destination or set `telegram_enabled` from the webhook during
rollback. Disable the preference through the authenticated API or suppress the
destination through the established domain path. Restore Redis/Bot credentials
or roll forward the binary, restart the worker, then re-enable relay/evaluator
for a controlled cohort.

## Queue and DLQ handling

The Telegram queue uses server-side encryption, 20-second long polling,
60-second visibility and redrive after eight receives:

```text
limnopulse-telegram-notification-jobs
  -> limnopulse-telegram-notification-jobs-dlq
```

Never run an automatic Telegram DLQ consumer. Sample entries only under
restricted access, correlate through durable identifiers, fix the root cause,
and redrive a bounded batch while watching duplicate suppression and provider
calls. Do not paste jobs, Delivery snapshots or fake/provider request bodies in
tickets or chat.

## Rotation

Webhook secret rotation supports current and previous values in the FastAPI
verifier cache. Write the new secret version, update Telegram `setWebhook` with
the new header, verify traffic, then retire the previous value after the cache
and in-flight request window.

Bot token rotation is a worker restart boundary. Stop new relay publication,
drain workers, write the new secret version, restart one worker and verify a
controlled send before scaling. Redis keys are derived from a bot-token digest,
so rotation naturally starts a new limiter namespace without exposing the
token.
