# P3 Ingestion Rollout Runbook

This runbook turns on the V4 P3 generic HTTPS ingress, `POST /ingest/v2/observations` → SQS →
telemetry ingest worker → `observation_numeric`, in a shared environment. It also covers the
operator drills, credential and decommissioning procedures, rollback, and the actions that only
the repository owner may take. P3 ships dark: every step below is an explicit, reversible change
made by a person, and nothing here runs on its own. The P3 plan is
`docs/superpowers/plans/2026-10-30-limnopulse-v4-p3-ingress-integration.md` (Epic #15). The canonical
telemetry rollout that this runbook feeds into is the [P2 telemetry rollout
runbook](p2-telemetry-rollout-runbook.md).

## Preconditions

- **Release gate.** G1–G15 are green on `main` (see [Acceptance matrix](#acceptance-matrix)):
  `make verify-python`, `make verify-go`, `make verify-influx` and `make verify-ingestion` against
  ElasticMQ, DynamoDB Local and InfluxDB 2.7.11. The G7 DLQ drill and the G8 quarantine drill were rehearsed locally on
  real services and are recorded on #103.
- **Production runs the P3 release.** The VPS has no clone of this repository
  ([VPS bootstrap runbook](../ops/vps/README.md)), so merging changes nothing there. Before step 1:
  back up `/opt/limnopulse/compose.production.yaml` and copy the current one over; reinstall
  `ops/vps/limnopulse-deploy.sh` as `/usr/local/bin/limnopulse-deploy` (its recovery fixes apply only
  once installed); then promote a `main-<sha>` tag at or after P3-10 through `deploy.yml` with every
  switch off, and confirm the `/v1` OpenAPI is unchanged. A failed promotion restores the previous
  image tag but not the compose file, so restore the compose backup by hand.
- **M1 is enabled.** `INGEST_V2_ENABLED` requires `API_V2_ENABLED=true`, which mounts the M1 `/v2`
  groups for every tenant the deployment serves. Before step 3, follow the
  [M1 rollout sequence](m1-acceptance-runbook.md#rollout-sequence) through its step 6: migrate every
  served tenant, then enable `API_V2_ENABLED`. Ingestion does not need
  `V1_POND_DEVICE_PROJECTION_ENABLED`.
- **Dark by default.** P3 ships dark: with every switch off, the API behaves exactly as before P3, and nothing
  produces canonical telemetry in production (D16). The `/v1` OpenAPI is identical in every switch
  combination (G13).
- **D16 stays open.** The legacy-device read switches (`V1_TELEMETRY_READ_SOURCE=v2`,
  `ALERT_EVALUATOR_TELEMETRY_SOURCE=v2`) still need a producer for every legacy MQTT device. That is
  Backlog issue #104, not this runbook (see [Owner actions](#owner-actions)).
- **Queue properties (§21.2), provisioned by OpenTofu `ingestion_queues.tf`** behind the
  `telemetry_ingress` variable:
  - `limnopulse-telemetry-ingress` (standard queue): SSE (SQS-managed), 4 days retention, 60 s
    visibility, 20 s long poll, redrive to the DLQ after `maxReceiveCount` 8 receives;
  - `limnopulse-telemetry-ingress-dlq`: SSE, 14 days retention, so the DLQ outlives the source queue
    (DLQ expiry counts from the original enqueue);
  - both queues deny non-TLS access (`aws:SecureTransport`);
  - message bodies are the versioned `telemetry-ingress@1` envelope (schema attribute `schema`), made
    of opaque ids only: never a token, an `Idempotency-Key`, an `Authorization` header or a raw
    `external_device_id`;
  - least privilege: the API principal may only send to the source queue; the worker runs as its
    own `telemetry_ingest` IAM user (receive, delete, change visibility and read the attributes of the
    source queue; `GetItem`, `PutItem`, `Query` and `UpdateItem` on the domain table, without a
    leading-key condition); the operator policy (`<project>-<env>-telemetry-ingress-operator`,
    attached to no principal by default) covers DLQ redrive and quarantine reconciliation.
- **Alarms** exist and route to an on-call target: they publish to the module's KMS-encrypted SNS
  topic (output `telemetry_ingress_alarm_topic_arn`), `telemetry_ingress_alarm_email` subscribes the
  on-call mailbox, and `telemetry_ingress_alarm_actions` adds extra ARNs. The subscription stays
  `PendingConfirmation` until the recipient confirms the email AWS sends; confirm it before step 2.
  See [Alarms and deadlines](#alarms-and-deadlines).

## Enablement order

Each step is a separate change. Verify each one before moving to the next.

1. **Infrastructure flag.** Set the OpenTofu variable `telemetry_ingress` to `true` and apply
   (`tofu plan` / `tofu apply`). This creates the two queues, the alarms, the API send grant, the
   operator policy and the dedicated worker IAM user. Record the outputs
   `telemetry_ingress_queue_url`, `telemetry_ingress_dlq_url` and `telemetry_ingest_iam_user_name`.
2. **Worker.** Create the worker's credentials (see [Production environment](#production-environment)),
   set `TELEMETRY_V2_WRITE_ENABLED` to `true` and `SQS_TELEMETRY_INGRESS_QUEUE_URL`, and start the
   `telemetry-ingest-worker` service with the `ingestion` profile. It runs as its own principal, the
   `telemetry_ingest` IAM user, never the API or the shared `workers` user. It refuses to start
   (exit 2) without v2 writes and a queue, so it can never delete a message it did not write.
   Confirm it logs `telemetry ingest worker started` and the queue-age alarm stays green.
3. **Endpoint.** Set `INGEST_V2_ENABLED` to `true` (it requires `API_V2_ENABLED=true`,
   `TELEMETRY_V2_WRITE_ENABLED=true` and a non-empty `SQS_TELEMETRY_INGRESS_QUEUE_URL`; the API refuses
   to start otherwise) and recreate the API (see **Applying a change** below). Exactly `POST /ingest/v2/observations` and the five
   owner/admin tenant routes appear.
4. **Pilot onboarding,** one pilot at a time: create the `https_direct` integration account, issue an
   ingest credential for it (the token is shown once), create the DeviceIntegration for the pilot
   device under that account, then move the account and the DeviceIntegration to `active` through the
   lifecycle routes. Hand the token to the pilot over a secure channel and watch the first
   `202` and the worker's `ingress_messages_total{outcome="processed"}`. Also watch
   `ingress_observations_total{result="accepted"}`: `processed` is logged for quarantined
   observations too. The production metrics are JSON log lines (`docker compose ... logs`), not a
   scrape endpoint.
   - **The pilot device needs a v2 Device with a Deployment.** While
     `V1_POND_DEVICE_PROJECTION_ENABLED` is off, a Device created through `/v1` has no v2 Device until
     the M1 backfill runs for its tenant (dry run, `--apply`, idempotency rerun). The `scripts/`
     directory is not in the API image, so in production run `DefaultSiteAssetMigrator` from
     `limnopulse_api.migrations.default_site_asset` inside the `api` container. A backfilled device
     keeps its legacy default Component, so the connector may omit `component_id`, and is deployed
     from its `created_at`. Only legacy default Components are dual-written (D7), so a pilot meant to
     exercise step 6 must be a backfilled `/v1` device, not a native `/v2` one.
   - **A connector on the production host itself** (the simulated pilot in
     [`ops/vps/pilot-sim/`](../ops/vps/pilot-sim/README.md)) needs no hand-over: issue the credential
     with `IngestCredentialService.issue` inside the `api` container and redirect its stdout straight
     into a root-owned 0600 file (`umask 077`, `set -o noclobber`), so the token is never printed.
     Record the audit actor you used (for example `operator:pilot-onboarding`), because this path
     bypasses the route's membership check.
5. **Divergence checks.** For every pilot that also reports through the legacy path, run the P2 keyed
   divergence checks between `observation_numeric` and `water_quality` over the pilot window; they
   must report nothing (G15 proves the HTTPS path matches the independent legacy writer).
6. **Legacy dual write.** Set `TELEMETRY_LEGACY_DUAL_WRITE_ENABLED` to `true` (shared `.env`, then recreate the worker and the API) only after G7
   (the durable DLQ redrive gate) has passed on `main` and the DLQ drill below has been rehearsed in
   this environment.
7. **Remaining P2 steps.** Continue with the [P2 telemetry rollout
   runbook](p2-telemetry-rollout-runbook.md) (its rollout sequence, stop-dual-write criteria and
   retention). The read switches stay off until #104 closes D16.

## Production environment

The production worker gets **no** `env_file`: compose interpolates every variable from the shared
`--env-file` sources (`docker compose --env-file .env --env-file .env.image -f
compose.production.yaml --profile ingestion up -d telemetry-ingest-worker`), so it never receives
the API-only secrets.

- **Worker principal.** Create an access key for the IAM user named by the OpenTofu output
  `telemetry_ingest_iam_user_name` and store it as `TELEMETRY_INGEST_AWS_ACCESS_KEY_ID` /
  `TELEMETRY_INGEST_AWS_SECRET_ACCESS_KEY` in `.env`. Never reuse the API keys. Pipe
  `aws iam create-access-key --user-name <user> --query 'AccessKey.[AccessKeyId,SecretAccessKey]' --output text`
  straight into the `.env` update on the host, so the secret never reaches a terminal or a log. Rotate
  the access key like any IAM key: create the second key, recreate the worker, then delete the old key.
- **Worker InfluxDB token.** Create a dedicated write token scoped to the raw bucket only
  (`influx auth create --org <org> --write-bucket <bucket_id> --description telemetry-ingest`) and
  store it as `TELEMETRY_INGEST_INFLUXDB_TOKEN`. Never use the bootstrap admin token or the API's
  token for the worker. In production the `influx` CLI runs inside the `influxdb` container
  (`docker compose ... exec -T influxdb`). Give the bootstrap admin token
  (`INFLUX_TOKEN=$DOCKER_INFLUXDB_INIT_ADMIN_TOKEN`) to those two commands only. Find the bucket id with
  `influx bucket list --org <org> --name <bucket> --hide-headers`. Create the token with `--json`, and
  write its `token` field straight into `.env` without printing it.
- **Every interpolated name must be defined.** An empty interpolation is passed to the container as
  an empty string and overrides the Settings default. Outside `local`/`test` the worker refuses to
  start with exit 2 when its AWS keys or InfluxDB token are empty, so a missing variable fails fast
  instead of polling forever. Define each of these in the production `.env`, never in `.env.image`:
  every promotion through the deploy script rewrites `.env.image` with `IMAGE_TAG` alone, so anything
  else there is gone after the next deploy:

  | Variable | Value |
  |---|---|
  | `IMAGE_TAG` | the deployed image, the same as the API (written to `.env.image` by the deploy script) |
  | `APP_ENV`, `AUTH_MODE` | as for the API |
  | `AWS_REGION` | the queues' region |
  | `TELEMETRY_INGEST_AWS_ACCESS_KEY_ID`, `TELEMETRY_INGEST_AWS_SECRET_ACCESS_KEY` | the `telemetry_ingest` user's access key |
  | `DYNAMODB_DOMAIN_TABLE`, `DYNAMODB_AUDIT_TABLE` | as for the API |
  | `SQS_TELEMETRY_INGRESS_QUEUE_URL` | output `telemetry_ingress_queue_url` |
  | `TELEMETRY_INGEST_INFLUXDB_TOKEN` | the worker write token |
  | `INFLUXDB_ORG`, `INFLUXDB_BUCKET_RAW` | as for the API |
  | `TELEMETRY_V2_WRITE_ENABLED` | `true` (step 2) |
  | `TELEMETRY_LEGACY_DUAL_WRITE_ENABLED` | `false` until step 6 |
  | `TELEMETRY_INGEST_WORKER_CONCURRENCY`, `TELEMETRY_INGEST_WORKER_MAX_MESSAGES`, `TELEMETRY_INGEST_WORKER_VISIBILITY_SECONDS`, `TELEMETRY_INGEST_PROCESSING_DEADLINE_SECONDS`, `TELEMETRY_INGEST_RETRY_BASE_SECONDS`, `TELEMETRY_INGEST_RETRY_CAP_SECONDS` | optional tuning; compose supplies the defaults (4, 10, 60, 45, 30, 900) when unset |

- **One shared `.env`.** The API loads the same `.env` through its `env_file`, so every switch set
  there reaches the API the next time its container is recreated as well. For the ingest route the API also needs
  `API_V2_ENABLED=true`, `TELEMETRY_V2_WRITE_ENABLED=true`, `INGEST_V2_ENABLED` and
  `SQS_TELEMETRY_INGRESS_QUEUE_URL`; operators running the CLIs also set
  `SQS_TELEMETRY_INGRESS_DLQ_URL`. The API reads `TELEMETRY_V2_WRITE_ENABLED` and
  `TELEMETRY_LEGACY_DUAL_WRITE_ENABLED` too (for its own write service), so recreate both services
  after changing either.
- **Applying a change: recreate, never just restart.** Compose sets a container's environment when it
  creates the container; `docker compose restart` keeps the old values. After editing `.env`,
  recreate the affected services with the production env files, for example
  `docker compose --env-file .env --env-file .env.image -f compose.production.yaml up -d api` and
  `docker compose --env-file .env --env-file .env.image -f compose.production.yaml --profile ingestion up -d telemetry-ingest-worker`.
  Confirm the new value took effect (the route set, or the worker's start log) before the next step.
- **`stop_grace_period`.** The worker's `stop_grace_period` (90 s in both compose files) must exceed
  drain + wait + 5 s (A47): `TELEMETRY_INGEST_DRAIN_SECONDS` 30 + `TELEMETRY_INGEST_WORKER_WAIT_SECONDS`
  20 + the 5 s poll grace, more than 55 s at the defaults. SIGTERM first finishes the in-flight long
  poll, then drains; a shorter grace lets Docker's SIGKILL cut the drain short. Keep some headroom for
  a boto3 call still in flight (`SHUTDOWN_CALL_BUDGET_SECONDS` in `workers/telemetry_ingest.py`).
  `TELEMETRY_INGEST_DRAIN_SECONDS` and `TELEMETRY_INGEST_WORKER_WAIT_SECONDS` are not interpolated
  into the worker's environment in either compose file (it has no `env_file`), so setting them in
  `.env` has no effect: changing either needs a compose change, made together with the grace.
- **Restart policy.** `restart: on-failure:5` may leave the worker stopped after a reboot, and the
  default deploy (without `--profile ingestion`) does not recreate it. Redeploy it with the profile
  so producer and consumer run the same image; the queue-age alarm covers a stopped worker.

## Alarms and deadlines

| Alarm | Fires when | First response |
|---|---|---|
| `<project>-<env>-telemetry-ingress-age` (queue age) | the oldest message is older than `telemetry_ingress_age_alarm_threshold_seconds` (default 300 s) | is the worker running? Check its logs and bring it back with `up -d` and the profile; a stopped worker is the usual cause |
| `<project>-<env>-telemetry-ingress-depth` | more than `telemetry_ingress_depth_alarm_threshold` (default 1000) visible messages for 5 minutes | worker throughput or an outage; check `ingress_messages_total{outcome="retry"}` by category |
| `<project>-<env>-telemetry-ingress-dlq` | the DLQ is not empty | run the [DLQ drill](#dlq-drill) |

- **Outage-to-DLQ timing.** A retryable failure (an InfluxDB or DynamoDB outage) backs off with full
  jitter: `backoff(n) = ceil(random() * min(cap, base * 2^(n-1)))` with base 30 s and cap 900 s.
  After `maxReceiveCount` is 8 receives SQS moves the message to the DLQ, so an outage longer than
  roughly 20–45 minutes moves the backlog to the DLQ. That is expected and loses nothing: recovery
  is the DLQ redrive drill, alerted by the DLQ alarm. A permanent failure (an invalid envelope, a
  forged envelope) is retried every 60 s until it reaches the DLQ.
- **Reconciliation deadline: 14 days.** Data that already got a `202` and then failed ends up in
  one of two places, each kept for 14 days: a message that kept failing (an outage, an invalid or
  forged body) in the DLQ (14 days minus the time spent in the source queue), and a quarantined or
  fenced observation in the quarantine rows (TTL 14 days). A message waiting for a stopped worker
  stays only 4 days in the source queue. Reconcile (redrive, replay or dismiss) within 14 days of the original
  request; after that the data is gone for good.

## DLQ drill

Rehearsed end to end on real services by G7,
`tests/ingestion/test_p3_acceptance_ingestion.py::test_gate7_influx_outage_reaches_the_dlq_and_redrive_reproduces_the_baseline`:
an InfluxDB outage after acceptance moves the messages to the DLQ without deleting them, and after
the redrive the worker reproduces the exact v2 and legacy point set of the no-fault baseline. The
drill ran locally against ElasticMQ, DynamoDB Local and InfluxDB 2.7.11 (recorded on #103).

**Rehearsing it in an environment** (required before step 6) needs messages in the DLQ. An empty
DLQ proves nothing. To fail only the worker, recreate it with an invalid InfluxDB token, which
leaves InfluxDB and the API untouched. The shell value wins over `--env-file` in compose
interpolation:
`TELEMETRY_INGEST_INFLUXDB_TOKEN=drill-invalid docker compose --env-file .env --env-file .env.image -f compose.production.yaml --profile ingestion up -d telemetry-ingest-worker`.

- **Why it reaches the DLQ.** InfluxDB answers `401`, which is classified `write_permanent`. A
  permanent failure keeps the message and only hides it for 60 s. After `maxReceiveCount` (8)
  receives, about 8 minutes, SQS moves it to the DLQ, and the DLQ alarm fires.
- **What to send.** Send a few batches while the worker is failing, and note their `ingress_id`s
  and `observed_at`s.
- **Restore before redriving.** Recreate the worker without the override, then run the steps below.

Run from an operator checkout with the operator policy's credentials, `SQS_TELEMETRY_INGRESS_QUEUE_URL`
and `SQS_TELEMETRY_INGRESS_DLQ_URL` set. `sample` only reads and releases; `redrive` is a dry run
unless `--apply` is given, and nothing touches the DLQ unless an operator runs a subcommand (there is no automatic DLQ consumer).

1. **Sample.** Print metadata (message id, receive count, `sent_at`, a diagnosis of the body as a
   `telemetry-ingress@1` envelope) for up to 10 messages; it never prints a body and releases every
   message it saw:

```bash
uv run --locked python scripts/admin/telemetry_dlq.py sample --limit 10
```

2. **Diagnose.** Fix the cause before redriving: an InfluxDB or DynamoDB outage is over, a worker
   bug is fixed and deployed, or a deferred DeviceIntegration, account or Device is active again. Bodies that fail to parse (`envelope_invalid`) or are forged will fail again.
   `redrive` has no filter: it moves every message it receives, up to `--max`. A redriven bad body
   cycles back to the DLQ after about 8 × 60 s as a new message, so its 14-day expiry restarts. When
   such bodies are present, redrive deliberately with a small `--max`, and expect them back.
3. **Dry run.** Count what a redrive would move:

```bash
uv run --locked python scripts/admin/telemetry_dlq.py redrive --max 100
```

4. **Redrive.** Send each raw body back to the source queue byte for byte, then delete it from the
   DLQ (a duplicate is safe: points are idempotent by `event_id` and state marks are monotone):

```bash
uv run --locked python scripts/admin/telemetry_dlq.py redrive --max 100 --apply
```

5. **Verify.** The DLQ alarm clears, `ingress_dlq_redriven_total{result="redriven"}` matches the
   summary, and the worker reports `ingress_messages_total{outcome="processed"}`. Exit code 3 means a
   partial redrive: rerun after checking the failures. A dry run also exits 3 when it stops midway
   after it has seen messages. The summary then carries an `error` category (`queue_unavailable` or
   `unexpected`) next to the counts so far. A Ctrl-C prints that summary with `interrupted` and
   still exits by the interrupt.

## Quarantine reconciliation drill

Rehearsed end to end on real services by G8,
`tests/ingestion/test_p3_acceptance_ingestion.py::test_gate8_unresolved_deployment_is_quarantined_then_replayed_into_event_time`:
a `deployment_unresolved` observation is persisted before its message is deleted, connector health is
marked, the operator adds the covering Deployment and replays, and the point lands in the event-time
Deployment with the `replay` flag, the first `received_at`, the original versions and the same
`event_id`. The drill ran locally against real services (recorded on #103).

Quarantine rows live 14 days. `replay` and `dismiss` are a dry run unless `--apply` is given.

1. **List** the open rows of a tenant (add `--reason deployment_unresolved` to filter, `--after` to
   page, `--show-excerpt` only when the observation itself is needed):

```bash
uv run --locked python scripts/admin/telemetry_quarantine.py list --tenant <tenant_id>
uv run --locked python scripts/admin/telemetry_quarantine.py health --tenant <tenant_id> --account <integration_account_id>
```

2. **Fix the cause.** For `deployment_unresolved`, create the Deployment that covered the event time
   (through `/v2`). Fenced rows (`fenced_<reason>`) are not replayable in practice: a decommissioned
   DeviceIntegration, a revoked account and a decommissioned Device are terminal states with no
   lifecycle transition back, and a replay keeps the original account and DeviceIntegration ids, so
   it would be fenced again (a replacement DeviceIntegration does not help). Dismiss fenced rows, or
   recover them only through a separately approved procedure; mismatch and unresolved-identity rows
   come from forged or corrupted envelopes and are dismissed after investigation.
3. **Replay** (dry run, then apply). One new `telemetry-ingress@1` envelope is sent per
   `(ingress_id, device_integration_id)` group, with `replay=true`, `replay_of` the original
   ingress id, the first `received_at`, the stored mapping versions and the stored observations;
   rows become `replayed` only after the send succeeded. A replay never moves ingest state:

```bash
uv run --locked python scripts/admin/telemetry_quarantine.py replay --tenant <tenant_id> --entry <entry_key>
uv run --locked python scripts/admin/telemetry_quarantine.py replay --tenant <tenant_id> --entry <entry_key> --apply
```

4. **Dismiss** rows that must not be replayed (they become `dismissed`; both states are terminal):

```bash
uv run --locked python scripts/admin/telemetry_quarantine.py dismiss --tenant <tenant_id> --entry <entry_key> --apply
```

- **Reruns.** The replay id is derived from the group's entry keys, so a rerun after a crash before
  any row was marked reuses the same replay id. After a partial mark (exit code 3: each row is
  marked on its own, and some rows of the group failed with a conflict or a store error), a rerun
  skips the rows already `replayed` and its group has different entry keys, so it gets a new replay
  id (P3-09, see #116). A rerun with the same `--entry` list reports the skipped rows as `not_open`
  and therefore exits 3 even when everything it attempted succeeded; check the per-entry lines. The observations
  that were already sent may be written twice; that is safe because points are idempotent by
  `event_id`.

## Credentials

- **Issue.** Owners and admins issue a credential through
  `POST /v2/tenants/{tenant_id}/integration-accounts/{integration_account_id}/ingest-credentials`, for
  an `https_direct` account that is `pending` or `active`. The token is returned exactly once, with
  `Cache-Control: no-store` and `Pragma: no-cache`; only its digest and an 8-character fingerprint are
  stored. Each issue and revoke writes one audit item.
- **Two slots.** An account holds at most two active credentials (two slots).
- **Rotation.** Issue the second credential, deploy it to the connector, confirm traffic on the new
  fingerprint, then revoke the old credential. Never revoke the only active credential of a live
  connector.
- **Revocation** is immediate and terminal: the next request with that token gets `401`, and the slot
  is freed. Revoking a credential does not fence data already queued; to fence queued data, revoke the
  IntegrationAccount or decommission the DeviceIntegration.
- **Lost issuance.** Issuance takes no `Idempotency-Key` and the token is never stored, so a lost
  issuance response still uses up a slot. List the account's credentials, find the one nobody
  received (by creation time and fingerprint), and revoke it.

## Decommissioning

- **DeviceIntegration.** Decommissioning a DeviceIntegration stops new admissions (`403`) and fences
  queued messages: they are persisted as `fenced_integration_decommissioned` rows (replayable in
  form, D9, but a replay is fenced again, so they are dismissed) plus a
  summary row, then deleted. The external identity stays claimed (D22): the `EXTERNAL_ID` row keeps
  `status=decommissioned`, so the same external device id cannot be re-claimed in P3.
- **IntegrationAccount.** Revoking the account fences its queued messages
  (`fenced_account_revoked`) and new requests get `401`.
- **Suspended Device.** A Device that is `suspended` or `provisioning` (or a DeviceIntegration or
  account in `error` or `pending`) is a temporary fence: its messages are retried with backoff and
  processed once it is active again, or reach the DLQ after `maxReceiveCount`.
- **DeviceIntegrations created before P3-04 (D22).** Under an `https_direct` account they carry the
  M1 global identity hash, never resolve, and admission fails closed with `403`. Re-create them: create
  a new DeviceIntegration for the device under the account (it gets the account-scoped hash) and
  decommission the old one.

## Rollback

Rollback follows §24: route pilots back, disable the endpoint, retain the queued messages.

1. **Route pilots back** to their previous (legacy) path, so they stop calling the HTTPS endpoint.
2. **Stop legacy dual write:** set `TELEMETRY_LEGACY_DUAL_WRITE_ENABLED=false` in the shared `.env`
   and recreate the worker (and the API, which reads it too), if it was on.
3. **Disable the endpoint:** set `INGEST_V2_ENABLED=false` and recreate the API (`up -d api`). New requests get
   `404`; everything already accepted stays queued.
4. **Let the worker drain** the source queue (queue depth and age back to zero), unless the worker
   itself is the reason for the rollback.
5. **Stop the worker:** stop the `ingestion` profile and set `TELEMETRY_V2_WRITE_ENABLED=false`.
   Unprocessed messages simply stay on the queue or the DLQ.
6. **Retain the queues.** Never purge the source queue or the DLQ, and keep `telemetry_ingress = true`:
   accepted data stays recoverable for 4 days in the source queue and 14 days in the DLQ, and
   quarantine rows for 14 days. Turning the worker back on resumes processing.

No data deletion is needed: `water_quality` and the legacy reads are untouched while legacy dual write
is off, and `observation_numeric` points are idempotent if the same messages are processed later.

## Owner actions

Agents do not change production. These are owner actions, recorded on #15:

- **Production switch flips.** `telemetry_ingress` in OpenTofu, `TELEMETRY_V2_WRITE_ENABLED`,
  `INGEST_V2_ENABLED` and `TELEMETRY_LEGACY_DUAL_WRITE_ENABLED` in production, and pilot onboarding.
  P3 exits with enablement permitted (G1–G15 green, drills executed); the flips themselves are the
  owner's.
- **D16 open gate.** The read switches stay off until a producer writes every legacy MQTT device to
  `observation_numeric`. That gate is owned by Backlog issue #104 (`[Backlog][P3 follow-up] Producer
  path for legacy MQTT devices (D16 read-switch gate)`), decomposed only after its producer-path
  decision (HTTPS firmware, an approved MQTT bridge, or P5 AWS IoT) is approved.
- **Required status checks.** Add the CI checks to the default-branch ruleset, with strict
  up-to-date branches and required thread resolution: `python`, `go`, `opentofu`, `compose`,
  `frontend`, `influx`, `ingestion` (from `verify.yml`) and `sonar`, plus the Trivy and build workflow
  jobs if wanted.

## ADR gate map

| ADR gate | Proof |
|---|---|
| ADR-001: queued ingest is fenced after decommission or revocation | `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate9_terminal_fences_persist_rows_and_temporary_fences_retry` |
| ADR-006: quarantine persistence and connector-health marking; operator reconciliation | `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate8_unresolved_deployment_is_quarantined_then_replayed_into_event_time` |
| ADR-006: device and component last-seen, duplicate and sequence gap/regression state | `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate10_truth_table_vectors_hold_end_to_end`, `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate10_sequence_truth_table_end_to_end_and_marks_never_pass_the_first_receipt`; point-level flags **DEFERRED → #17** (D12) |
| ADR-006: current-health views from receive or ingest time | **DEFERRED → #17** (P6), from the P3 `INGEST_STATE` rows |
| ADR-007: durable SQS redrive after an InfluxDB outage, recovery without loss | `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate7_influx_outage_reaches_the_dlq_and_redrive_reproduces_the_baseline` |
| ADR-007 / D16: a producer for every legacy device before the read switches | **DEFERRED → #104**; P3 delivers `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate15_https_path_matches_the_independent_legacy_writer` |
| ADR-016: accepted only after the durable SQS write | `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate1_accepted_only_after_the_queue_write` |
| ADR-016: at-least-once replay is safe; `Idempotency-Key` retry creates no second observation | `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate5_idempotent_retries_keep_the_first_received_at`, `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate6_redelivery_and_shuffling_converge_on_points_and_state` |
| ADR-016: authenticate before resolving; reject missing, invalid or mismatched credentials | `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate2_every_bad_credential_or_account_is_401_and_revocation_is_immediate` |
| ADR-016: an IntegrationAccount cannot claim another tenant | `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate3_no_cross_tenant_claim_and_forged_envelopes_are_fenced` |
| ADR-016: ownership only from the mapping | `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate4_ownership_comes_only_from_the_mapping` |
| ADR-016: bounded payload and per-account rate limit before enqueue | `tests/contracts/test_p3_acceptance_gate.py::test_gate11_limits_reject_before_any_queue_admission`; entitlement enforcement **DEFERRED → #20** (P4; P3 ships the pre-enqueue hook) |
| ADR-016: DLQ redrive is tested | `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate7_influx_outage_reaches_the_dlq_and_redrive_reproduces_the_baseline` |
| ADR-019: no correctness dependency on the optional cache | `tests/contracts/test_p3_acceptance_gate.py::test_gate14_no_scan_no_new_index_no_new_table_and_no_cache_dependency` |

## Acceptance matrix

| Gate | Requirement | Proof |
|---|---|---|
| G1 | accepted only after the queue write | `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate1_accepted_only_after_the_queue_write` |
| G2 | authentication, immediate revocation | `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate2_every_bad_credential_or_account_is_401_and_revocation_is_immediate` |
| G3 | no cross-tenant claim, forged envelopes fenced | `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate3_no_cross_tenant_claim_and_forged_envelopes_are_fenced` |
| G4 | ownership only from the mapping | `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate4_ownership_comes_only_from_the_mapping` |
| G5 | idempotency and the first-receipt `received_at` | `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate5_idempotent_retries_keep_the_first_received_at` |
| G6 | redelivery and shuffling converge | `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate6_redelivery_and_shuffling_converge_on_points_and_state` |
| G7 | durable DLQ redrive | `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate7_influx_outage_reaches_the_dlq_and_redrive_reproduces_the_baseline` |
| G8 | quarantine and reconciliation | `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate8_unresolved_deployment_is_quarantined_then_replayed_into_event_time`; current-health views DEFERRED → #17 |
| G9 | terminal and temporary fences | `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate9_terminal_fences_persist_rows_and_temporary_fences_retry` |
| G10 | sequence truth table, marks never pass the first receipt | `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate10_truth_table_vectors_hold_end_to_end`, `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate10_sequence_truth_table_end_to_end_and_marks_never_pass_the_first_receipt`; point-level flags DEFERRED → #17 |
| G11 | limits | `tests/contracts/test_p3_acceptance_gate.py::test_gate11_limits_reject_before_any_queue_admission`, `tests/contracts/test_p3_acceptance_gate.py::test_gate11_envelope_size_guard_never_sends_an_oversized_message`; entitlement enforcement DEFERRED → #20 |
| G12 | closed metrics, no secret anywhere | `tests/contracts/test_p3_acceptance_gate.py::test_gate12_metrics_use_the_closed_name_and_label_sets`, `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate12_no_secret_or_raw_identifier_in_logs_bodies_receipts_or_rows` |
| G13 | switch matrix, worker start guard | `tests/contracts/test_p3_acceptance_gate.py::test_gate13_switch_matrix_keeps_v1_and_mounts_exactly_the_switched_routes`, `tests/contracts/test_p3_acceptance_gate.py::test_gate13_worker_exits_2_unless_it_can_write` |
| G14 | storage rules, optional cache | `tests/contracts/test_p3_acceptance_gate.py::test_gate14_no_scan_no_new_index_no_new_table_and_no_cache_dependency`, `tests/contracts/test_p3_acceptance_gate.py::test_gate14_the_composed_ingest_route_has_no_cache_dependency` |
| G15 | zero divergence against the independent legacy writer | `tests/ingestion/test_p3_acceptance_ingestion.py::test_gate15_https_path_matches_the_independent_legacy_writer`; the legacy-device producer DEFERRED → #104 |
