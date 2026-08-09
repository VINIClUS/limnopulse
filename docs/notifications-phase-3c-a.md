# Notification Delivery Phase 3C-A Operations

Phase 3C-A delivers email notifications through a durable DynamoDB-to-SQS-to-SES
pipeline. `notifications relay` is a one-shot publisher intended for an external
60-second scheduler. `notifications worker` is the continuous jobs and SES
feedback consumer. Telegram is not delivered in Phase 3C-A; existing Telegram
outboxes remain outside the email relay index, and the legacy backfill marks the
rows it visits as deferred for Phase 3C-B.

DynamoDB is authoritative for outboxes, delivery state, attempts, provider
outcomes, suppression and idempotency. SQS is an at-least-once transport. A
crash after `SendMessage` but before the conditional DynamoDB update can publish
the same deterministic job again; the worker claims the deterministic delivery
with a fenced lease, so duplicate jobs do not create duplicate attempts. Redis
is not a source of truth and is not required by this delivery path. There is no
distributed DynamoDB/SQS transaction.

Each new NotificationOutbox carries the immutable evaluation window, evaluation
time and optional observed value that triggered that notification. The relay
renders from that snapshot even when the mutable AlertEvent has since received
later evaluations. Legacy/backfilled outboxes without these fields use a
conservative compatibility path: an opening omits a value unless the event's
last evaluation still matches its original window end, while a recovery derives
a same-duration window ending at the event's last evaluation time.

## Runtime flow

```text
AlertEvent + NotificationOutbox (DynamoDB transaction in the evaluator)
  -> one-shot relay queries NotificationRelayByAvailableAt
  -> deterministic Delivery and compact SQS job
  -> continuous worker claims Delivery and records Attempt
  -> SESv2 SendEmail with delivery_id and attempt_id tags
  -> SES configuration set -> EventBridge -> SES events SQS queue
  -> feedback worker conditionally reconciles Attempt and Delivery
  -> hashed email deliverability suppression after hard bounce/complaint
```

The worker resolves an active membership and immutable email snapshot before a
provider call, checks suppression twice around rate limiting, and writes attempt
start/completion durably. It makes at most five provider calls per delivery.
Retryable failures use full jitter up to 15 minutes. Ambiguous timeouts add a
two-minute confirmation grace; accepted feedback can resolve uncertainty, while
exhausted ambiguity without feedback ends in `unknown`. Permanent recipient
failures end durably and are not retried.

## Local execution

Start the local dependencies and initialize DynamoDB:

```bash
docker compose up -d dynamodb-local elasticmq
python scripts/dev/init_dynamodb.py
```

Run the continuous fake-email worker in one terminal:

```bash
docker compose --profile notifications up notification-worker
```

Run the one-shot relay in another terminal. Its scheduler remains outside the
domain:

```bash
docker compose --profile notifications run --rm notification-relay
```

The Compose worker uses the deterministic `success` sender and never contacts
SES. Other local/test-only modes are `retryable`, `permanent`,
`ambiguous_timeout` and `connection_reset`. Production and staging reject fake
senders. A synthetic, address-free SES event can be published locally with:

```bash
python scripts/dev/publish_ses_event.py \
  Delivery \
  --delivery-id del_example \
  --attempt-id att_example \
  --provider-message-id provider_example
```

Run the opt-in multiprocess suite after both dependencies are healthy:

```bash
RUN_NOTIFICATION_INTEGRATION=1 \
  python -m pytest -q tests/integration/test_notifications_local.py
```

The suite creates isolated tables and queues, uses real AWS SDK clients against
DynamoDB Local and ElasticMQ, and covers recovery dependency, duplicate
publication, retries, ambiguous acceptance, suppression and redrive after eight
receives.

## Cloud rollout

Use an environment-specific OpenTofu plan and normal change controls. The safe
deployment order is deliberately asymmetric: make old data discoverable before
enabling either publisher or consumer.

1. Create the DynamoDB GSI `NotificationRelayByAvailableAt` and wait until its
   status is `ACTIVE`. The sparse index projects `relay_work_kind`; do not start
   the relay against a `KEYS_ONLY` version.
2. Deploy the evaluator writer that persists `relay_schema_version`,
   `available_at`, `relay_work_kind`, `relay_gsi_pk` and `relay_gsi_sk` with each
   new email outbox transaction.
3. Stop legacy evaluators that can still write an outbox without the Phase 3C-A
   relay attributes, then verify only the new writer version is running.
4. Keep relay and worker off while the old rows and SES prerequisites are
   checked. Provision the jobs queue, SES events queue and all three DLQs first.
5. Backfill relay attributes by explicit tenant Query. Run
   `notifications backfill-relay` as a dry-run, review `row_failures`, then use
   `--apply` and repeat the dry-run until `rows_needing_update` is zero:

   ```bash
   notifications backfill-relay --tenant-file tenants.txt --max-rows 10000 --timeout 5m
   notifications backfill-relay --tenant-file tenants.txt --apply --max-rows 10000 --timeout 5m
   notifications backfill-relay --tenant-file tenants.txt --max-rows 10000 --timeout 5m
   ```

   The command never scans DynamoDB. It exits partially with
   `scope_completed=false` and either `row_limit_reached` or `deadline_reached`
   when a bound is reached; increase the explicit bound or narrow the tenant
   batch, then repeat the idempotent run. Telegram rows are marked
   `deferred_unsupported_channel` and are not indexed for email relay.
6. Validate SES: `SES_FROM_EMAIL` must be a verified identity in the same
   region, `SES_CONFIGURATION_SET_NAME` must equal the OpenTofu
   `ses_configuration_set_name` output, and the account must have the intended
   production-access or sandbox recipient coverage. Confirm the EventBridge rule
   can write both the SES events queue and routing DLQ.
7. Start one worker replica with explicit `NOTIFICATION_MAX_SEND_RATE`, monitor
   credentials, quota and throttling, and confirm a controlled notification plus
   feedback reaches durable terminal state before increasing concurrency. The
   source accepts a strict mailbox (`alerts@example.com`) or a conservative
   ASCII friendly-name form (`Limnopulse <alerts@example.com>`); malformed
   values stop the worker before rate limiting, Attempt creation or an SES call.
8. Schedule the relay every 60 seconds. Run `notifications relay` as a one-shot
   task with a timeout below the cadence. systemd timer, Kubernetes CronJob,
   EventBridge Scheduler/ECS or an equivalent external scheduler may invoke the
   same image; the domain contains no scheduling loop.
9. Monitor backlog, unknown deliveries and all three DLQs before completing the
   rollout. Expand relay sharding or worker concurrency only after the initial
   single-replica baseline is healthy.

For deterministic replay or incident diagnosis:

```bash
notifications relay \
  --relay-time=2026-07-17T12:00:00Z \
  --shard=0 \
  --shard-count=1
```

The relay owns virtual bucket `B` when `B % shard_count == shard`, queries the
GSI with pagination, caps work at 250 by default and gives domain work a
45-second global deadline. Final no-PII OTLP export has a separate bounded
2-second shutdown budget, so the process hard envelope is 47 seconds and
remains below the 60-second cadence. A nonzero exit,
`scope_completed=false`, `cap_reached`,
`deadline_reached`, `work_remaining` or `retry_recommended` means the external
scheduler/operator must run it again. Overlapping invocations remain safe due
to leases, fencing and conditional writes, but the scheduler should still avoid
unnecessary overlap.

A systemd deployment uses a oneshot service and a timer; the relay itself must
not contain a loop:

```ini
# limnopulse-notification-relay.service
[Service]
Type=oneshot
EnvironmentFile=/etc/limnopulse/notifications.env
ExecStart=/usr/local/bin/notifications relay --shard=0 --shard-count=1
TimeoutStartSec=50
```

```ini
# limnopulse-notification-relay.timer
[Timer]
OnCalendar=*-*-* *:*:00
Persistent=true
```

A Kubernetes CronJob should use `schedule: "* * * * *"`,
`concurrencyPolicy: Forbid`, `activeDeadlineSeconds: 50` and container arguments
`["relay", "--shard=0", "--shard-count=1"]`. EventBridge Scheduler can launch
the same image as an ECS task every minute. Sharded deployments create one
scheduled invocation for each `shard` under the same `shard-count`; changing
process count needs no DynamoDB migration because the persisted total remains
64 virtual buckets.

## Rollback

If delivery behavior is unsafe, turn off the relay first so no new SQS jobs are
published. Send `SIGTERM` to the worker and allow its 30-second internal drain
budget plus shutdown headroom; do not kill it during a provider call unless the
incident requires accepting an ambiguous outcome, and preserve DynamoDB
delivery and attempt rows, relay index attributes, queues and DLQs. They are the
evidence and replay source; reverting the binary must not delete or rewrite
them.

After the worker is stopped, inspect `processing` leases, ambiguous attempts,
queue age and DLQs. Fix or roll forward the binary, restart one worker, then
re-enable the one-shot relay. Removing the GSI or backfilled attributes is not a
rollback step. If SES itself is unsafe, disable SES sending/configuration-set
traffic while preserving feedback routing for already accepted messages.

## Queues and DLQ recovery

The cloud and local contracts use server-side encryption in AWS, 20-second long
polling, 60-second visibility and `maxReceiveCount=8` for the two consumed
queues:

- `limnopulse-notification-jobs` -> `limnopulse-notification-jobs-dlq`;
- `limnopulse-ses-events` -> `limnopulse-ses-events-dlq`;
- EventBridge target failures -> `limnopulse-ses-events-routing-dlq`.

Never run an automatic DLQ consumer. A DLQ entry can represent malformed input,
a missing durable dependency, a schema rollout mismatch or a persistent outage.
Recovery is manual and bounded:

1. stop the relay if new traffic worsens the incident;
2. sample messages under restricted access without copying email content into
   tickets, chat or logs;
3. correlate only by delivery/attempt/event identifiers and inspect the
   authoritative DynamoDB rows;
4. fix the schema, dependency, IAM or code problem;
5. redrive a reviewed batch to its source queue and watch durable transitions;
6. delete DLQ messages only after their durable outcome is verified.

The routing DLQ is not worker input. Replay those EventBridge envelopes into the
SES events queue only after confirming that the event shape and tags are valid.

## Observability

Both commands emit one bounded JSON result to stdout and never include message
bodies, queue URLs, email addresses, subjects or rendered content. Set
`OTEL_EXPORTER_OTLP_ENDPOINT` for OTLP/HTTP metrics.

Relay alarms should cover `notification_relay_backlog_items`, oldest backlog
age, run duration, deadline/cap reached, work errors, SQS errors and published
jobs. Worker alarms should cover attempts, retries, ambiguity, possible
duplicates, unknown outcomes, limiter waiters, active concurrency, throttling
and daily quota. The worker JSON summary also exposes per-consumer received,
deleted, visibility-changed and queue-error counts plus bounded feedback counts
for applied, duplicate, ignored, malformed, awaiting-DLQ, persistence error and
suppression outcomes.

At minimum, alert on a nonzero DLQ depth, repeated relay partial/fatal exits,
oldest relay backlog above two scheduler periods, sustained worker queue age,
any `unknown` increase, credential/configuration fatal categories, and a
nonzero `active_concurrency` after graceful shutdown.

## PII, retention and API boundary

The normalized email address is durable PII on the immutable Delivery snapshot
because an attempt must keep the exact authorized destination selected during
fanout. Rendered subject/text/HTML are also stored once on that snapshot.
Attempt rows do not duplicate either address or content. Suppression rows use a
SHA-256 email identity key and retain no plain address. SES destination arrays
are discarded during parsing; only provider message ID plus Limnopulse
delivery/attempt tags are used for association.

No TTL is applied to delivery or attempt rows in Phase 3C-A. Access to the domain
table, backups, DLQs and operational tooling must therefore be restricted and
audited. A future policy must define retention, redaction, backup expiry,
subject-access handling and tenant offboarding before production data-lifecycle
commitments are made; see `docs/backlog/notification-pii-retention.md`.

No public Delivery or Attempt API is introduced in Phase 3C-A. Existing Alert
Event routes remain the tenant-facing incident surface. Operational access to
delivery and attempt records is service-side only, and user-facing notification
history requires a separate authorization and redaction design.
