# Worker runtime runbook (ADR-020)

This runbook covers operating the workers on the VPS after the host bootstrap. Each role runs
as its own supervised process
([ADR-020](adr/ADR-020-each-worker-role-is-a-supervised-process.md)). The Go roles are
systemd units from the workers bundle. The telemetry ingest worker is a Compose service.
The rules live elsewhere, and this page links to them instead of repeating them:

| Topic | Source |
|---|---|
| Role table: units, paths, credentials, principals, alarms | [`ops/workers/roles.yaml`](../ops/workers/roles.yaml) |
| Exit codes, unit health, bundle, units, activation helper, deploy | [`ops/workers/README.md`](../ops/workers/README.md) |
| One-time host bootstrap | [`ops/vps/README.md`](../ops/vps/README.md), section 7 |
| Principals, access keys, alarm actions, the SES check, `limnopulse-admin` | [`infra/opentofu/README.md`](../infra/opentofu/README.md), "Worker roles (ADR-020)" |
| Email and Telegram rollouts | [`docs/notifications-phase-3c-a.md`](notifications-phase-3c-a.md) and [`docs/notifications-phase-3c-b.md`](notifications-phase-3c-b.md), "Cloud rollout" |
| Ingest worker rollout, keys and rollback | [`docs/p3-ingestion-rollout-runbook.md`](p3-ingestion-rollout-runbook.md) |

**Where commands run.**

- **Host:** commands marked *host* run as root on the VPS.
- **Workstation:** commands marked *workstation* run on the operator's workstation, from
  `infra/opentofu` in a checkout, with the operator's own AWS credentials. That covers
  `tofu`, `install_worker_key`, `limnopulse-admin` and every `aws` call made with operator
  credentials. Operator credentials never go onto the VPS, except in the fallback that
  "Access keys" describes for a host whose `sudo` needs a password.
- **Host exceptions:** two steps run on the host. The SES check's test send uses the email
  worker's own key, as root on the host ("The email-worker's SES check", step 2). The
  `influx` CLI runs inside the `influxdb` container.

## Starting state

The first bundled promotion installs every unit and enables none. Each Go role starts out
like this:

- its unit or timer is installed and disabled, and it publishes no `UnitHealthy` datapoint;
- its principal exists or not, as its delivery flag decides, and holds no access key;
- its alarms have `actions_enabled = false`.

The ingest worker runs while its host switch, `TELEMETRY_V2_WRITE_ENABLED`, is on. The
deploy removes it while the switch is off.

## Look at the workers

*Host:*

```bash
/usr/local/sbin/limnopulse-workers-activate status   # current tag, staged releases, floors
systemctl list-unit-files 'limnopulse-*'               # enabled or disabled per role
systemctl list-timers 'limnopulse-*'                   # last and next run of each one-shot
systemctl status limnopulse-<role>.service
journalctl -u limnopulse-<role>.service --since -1h
systemctl show limnopulse-<role>.service -p Result -p ExecMainStatus -p NRestarts
```

The deploy logs to `/var/log/limnopulse-deploy.log`. `/opt/limnopulse/.env.image`
records the live `IMAGE_TAG` and `WORKERS_BUNDLE_SHA256`.

For the ingest worker, run these as the deploy user from `/opt/limnopulse` (*host*):

```bash
sudo -u limnopulse-deploy docker compose --env-file .env --env-file .env.image \
  -f compose.production.yaml --profile ingestion ps telemetry-ingest-worker
sudo -u limnopulse-deploy docker compose --env-file .env --env-file .env.image \
  -f compose.production.yaml --profile ingestion logs --since 1h telemetry-ingest-worker
```

## Read a failure

| Exit | Meaning | What happens | What to do |
|---|---|---|---|
| `1` | runtime failure, or a loop that stopped making progress | a continuous unit restarts, backing off from 5 s to 60 s; a one-shot runs again at the next tick | read the journal |
| `2` | partial one-shot run | the next tick picks up the rest; `UnitHealthy` reads `0` until a run exits `0` | nothing, unless it repeats; then read the journal |
| `78` | configuration error: an env file setting, a credential or a flag | never restarted; a one-shot fails again at every tick | fix the env file or the credential, then `systemctl reset-failed limnopulse-<role>.service` and `systemctl start` it (for a one-shot, let the timer start it) |
| `SIGABRT` | an unrecovered panic (`GOTRACEBACK=crash`), or the systemd watchdog | treated like `1` | the journal holds the goroutine dump |

A continuous worker exits `1` by itself once its loop stops making progress, before the
watchdog fires. A watchdog kill therefore means the process could not even exit.

## Enable a Go role

Enable one role at a time, and finish each before starting the next. Each role's alarm
actions follow "Enabling and disabling a role" in
[`infra/opentofu/README.md`](../infra/opentofu/README.md). The order is:

1. `worker-monitor`. Every other unit-health alarm reads its datapoints.
2. `ses-feedback-worker`.
3. `email-worker`.
4. `telegram-worker`, with Telegram still off on the relay and the evaluator.
5. `notification-relay`.
6. `alert-evaluator`. If this rollout includes Telegram, hold the evaluator until
   [Telegram on the producers](#telegram-on-the-producers), and enable it there with
   Telegram on.

Consumers come before producers. A producer started first leaves its jobs aging in a queue
that nothing reads.

For each role:

1. *Workstation:* copy `ops/workers/env/<role>.env.example` from a checkout to the host's
   `/etc/limnopulse/workers/<role>.env` (root, 0600). The VPS has no clone. Fill each
   placeholder from the OpenTofu outputs. Two are not outputs:
   - `SES_FROM_EMAIL` is the verified SES sender, and its mailbox must equal
     `ses_from_address`.
   - `LIMNOPULSE_WEB_URL` is the public web URL.

   An env file holds no secret and never sets `GOTRACEBACK`, `LIMNOPULSE_SUPERVISOR` or an
   `*_ENDPOINT_URL`.
2. *Workstation:* define `install_worker_key` from "Access keys" in
   [`infra/opentofu/README.md`](../infra/opentofu/README.md), then install the role's key
   with it. *Host:* write the Telegram worker's `redis-password` with the `read -rs`
   pattern in `ops/vps/README.md`, step 7.4. Write the evaluator's `influxdb-token` as its
   gate below says.
3. Pass the role's gate below.
4. *Host:* run `systemctl enable --now limnopulse-<role>.timer` for a one-shot, or
   `systemctl enable --now limnopulse-<role>.service` for a continuous role. The enable
   must be persistent: a unit enabled with `--runtime`, or only linked, publishes nothing.
5. *Host:* check `journalctl -u limnopulse-<role>.service` for the first run.
   *Workstation:* wait until every alarm of the role reads `OK` ([Alarms](#alarms)), then
   set `<role>_enabled = true` and apply it.

| Role | Gate before step 4 |
|---|---|
| `worker-monitor` | The static user exists: `getent passwd limnopulse-worker-monitor` shows a uid below 61184 (`ops/vps/README.md`, step 7.5). |
| `ses-feedback-worker` | `email_delivery` is applied. The 3C-A "Cloud rollout" steps 1 to 6 are done: the relay index is `ACTIVE`, the email outboxes are backfilled and SES is validated. No older feedback worker is running. |
| `email-worker` | The SES-feedback worker is enabled and its alarms are `OK`. The SES check passes (`infra/opentofu/README.md`, "The email-worker's SES check"). Its simulation runs on the *workstation* and its test send on the *host*. The apply that first created `NotificationRelayByAvailableAt` is not the one that enables it ("Notification rollout boundary"). |
| `telegram-worker` | The 3C-B "Cloud rollout" steps 1 to 6 are done: queue, secrets, webhook and binding, binaries with every Telegram flag `false`, and the final `limnopulse-admin backfill-telegram` with the evaluator timer stopped. Redis is published on the host loopback with a password, and in the same change the API's `REDIS_URL` gets that password. `compose.production.yaml` publishes no Redis port and hard-codes a password-less `REDIS_URL`, so that change comes first. The password is the worker's `redis-password` credential. Only the worker's env file sets `TELEGRAM_DELIVERY_ENABLED=true` (3C-B step 7). |
| `notification-relay` | `NotificationRelayByAvailableAt` is `ACTIVE`, and `limnopulse-admin backfill-relay` reports zero rows needing update. The apply that first created the index is not the one that enables it. With Telegram on, the env file also sets `SQS_TELEGRAM_JOBS_URL` and `LIMNOPULSE_WEB_URL`. |
| `alert-evaluator` | The `influxdb-token` credential is a dedicated token, readable only against `INFLUXDB_BUCKET_RAW`. It is never the bootstrap admin `INFLUXDB_TOKEN` that the API and InfluxDB use. Create it as the P3 runbook creates the ingest worker's token ("Production environment"), but with `--read-bucket` and `--description alert-evaluator`. Write its `token` field straight into the credential file without printing it. The data-quality settings in the env file match the devices. `limnopulse-admin backfill-schedule` has been applied ([`docs/alert-evaluator-phase-3b.md`](alert-evaluator-phase-3b.md), "Initial schedule backfill"). |

**Backfills.** *Workstation:* run each backfill under the assumed `limnopulse_admin` role,
as "limnopulse-admin" in [`infra/opentofu/README.md`](../infra/opentofu/README.md)
describes. Name the tenants explicitly. Do a dry run, review it, run with `--apply`, then
dry-run again until it reports nothing left to update.

### Telegram on the producers

Turn Telegram on as 3C-B step 8 describes: first the relay, then the evaluator, for a
controlled tenant cohort. Then confirm one opening and one recovery.

From the final `backfill-telegram` until the evaluator's env file has Telegram on, the
evaluator timer stays stopped. An evaluator run with Telegram off marks each new Telegram
outbox `deferred_unsupported_channel`, and nothing indexes those outboxes later. If the
evaluator did run in that window:

1. Stop its timer.
2. Repeat the `backfill-telegram` dry run, `--apply` and dry run until it reports nothing
   to update.
3. Turn on the flags.

## Disable a Go role

Within a role, reverse the enable steps:

1. *Workstation:* set `<role>_enabled = false` and apply it, so its alarms notify nobody.
2. *Host:* disable the unit:

   ```bash
   systemctl disable --now limnopulse-<role>.timer     # one-shot
   systemctl disable --now limnopulse-<role>.service   # continuous
   ```

3. *Workstation:* delete the role's access key (`aws iam list-access-keys`, then
   `aws iam delete-access-key`).
4. *Host:* remove the files under `/etc/limnopulse/workers/credentials/<role>/`. For the
   evaluator, also delete its dedicated InfluxDB token by id, with `influx auth list` and
   then `influx auth delete --id <id>`. Run `influx` inside the `influxdb` container, as
   the P3 runbook does. Never delete the bootstrap admin token.

Across roles, disable in the reverse of the enable order, and `worker-monitor` last. Every
other unit-health alarm treats missing data as breaching. So stopping the monitor while
another role's alarm actions are on pages for that role within minutes.

## Rotate a Go role's credentials

**AWS key pair.**

1. *Workstation:* run `install_worker_key` again. IAM allows two keys per user.
2. *Host:* make the unit read the new files. `LoadCredential=` copies them when the unit
   starts:
   - continuous role: `systemctl restart limnopulse-<role>.service`;
   - one-shot role: the next tick reads them.
3. *Host:* `cat /etc/limnopulse/workers/credentials/<role>/aws-access-key-id` shows the new
   key's id. It is an id, not a secret.
4. *Workstation:* wait for a healthy run on the new key. Then run
   `aws iam list-access-keys --user-name <user>` and delete the other key with
   `aws iam delete-access-key`.

**InfluxDB token (evaluator).** All steps run on the *host*.

1. Create a second dedicated read token, the same way as in the evaluator's gate.
2. Write its `token` field over `influxdb-token` without printing it.
3. Let the next tick read it.
4. Once a run succeeds, delete the old dedicated token by id, with `influx auth list` and
   then `influx auth delete --id <id>`. Run `influx` inside the `influxdb` container, as
   the P3 runbook does. Never delete the bootstrap admin token.

**Redis password.** It changes the Redis server, the API's `REDIS_URL` and the
`redis-password` credential together, as one change.

The ingest worker's keys live in `/opt/limnopulse/.env`; see the P3 runbook's
"Production environment" section.

## Releases

A promotion is the `deploy.yml` workflow, run by hand with a `main-<40 hex>` tag. The tag's
images and its workers bundle must already be in GHCR. One tag moves the API, the profiled
Compose workers and the bundle together. The deploy order and its rollback are in
`ops/workers/README.md`, "Deploy".

**Automatic rollback.** Any failure after the bundle is staged rolls back to the release
that `.env.image` records: a pull, a Compose update, a health check or the activation.

- The API goes back to the previous tag. If the registry fails that pull, the API starts
  from the cached images and the failure is reported.
- The ingest worker follows the host switch as it is at rollback time. With the switch on,
  it goes back to the previous tag; with it off, it is removed.
- The bundle moves back only if activation started. If it never moved, the log says so.
  The helper then re-activates the previous release, which restarts the workers the failed
  activation stopped. If the previous release had no bundle, the helper deactivates
  instead.

**Manual rollback.** Run `deploy.yml` again with the previous tag. The helper keeps two
releases, but the deploy pulls and verifies the bundle again, so the older tag's bundle
must still be in GHCR.

**Rollback floor.** A release whose role epoch is below a recorded floor is refused with
exit `3`.

- A manual promotion stops at `stage refused ... status=3`, and nothing changes.
- An automatic rollback logs that the refused bundle needs a manual decision.
- Only root, from a real root login, can override the floor (`ops/vps/README.md`,
  "Rollback floor override").

**No bundle.** The deploy refuses a tag without a bundle. It returns to the no-bundle state
only during its own automatic rollback, and only when the previous release had no bundle.

The first `main` push that publishes a bundle can fail `workers-publish` at "Pull the
bundle back", while the `workers` package is still private. Make the package public
(`ops/vps/README.md`, section 4), then re-run the failed job.

## The ingest worker

The ingest worker is the `telemetry-ingest-worker` Compose service, in profile `ingestion`.

- **Running.** Every deploy starts it and waits for it to be healthy while
  `TELEMETRY_V2_WRITE_ENABLED` is on in `/opt/limnopulse/.env`. Docker restarts it
  `unless-stopped`.
- **Health.** Its healthcheck reads the liveness stamp that the loop writes every turn.
  A stalled loop exits `1`, and Docker restarts it.
- **Parked.** A configuration error exits `78`. The entrypoint guard then keeps the
  container up but idle and logs "parked, not restarted", and the container turns
  unhealthy. Fix `.env`, then recreate the container (*host*, from `/opt/limnopulse`):

  ```bash
  sudo -u limnopulse-deploy env COMPOSE_PROFILES= docker compose --env-file .env \
    --env-file .env.image -f compose.production.yaml --profile ingestion \
    up -d --force-recreate telemetry-ingest-worker
  ```

- **Turning it off.** Follow the P3 runbook's
  [Rollback](p3-ingestion-rollout-runbook.md#rollback) in full:
  1. Route pilots back and stop dual write.
  2. Set `INGEST_V2_ENABLED=false` and recreate the API.
  3. Let the worker drain the queue.
  4. Stop the worker:

     ```bash
     sudo -u limnopulse-deploy env COMPOSE_PROFILES= docker compose --env-file .env \
       --env-file .env.image -f compose.production.yaml --profile ingestion \
       stop telemetry-ingest-worker
     ```

  5. Set `TELEMETRY_V2_WRITE_ENABLED=false`. Later deploys then remove the worker instead
     of starting it.

  Never stop the worker or dual write while either read switch is `v2`. Changing the
  switch alone does nothing until the next deploy.

The ingest worker publishes no `UnitHealthy`. Its Compose healthcheck and its own queue
alarms observe it.

## Alarms

| Alarms | Names | Topic | Actions |
|---|---|---|---|
| Go role unit health | `<project>-<env>-<role>-unit-health` | `worker-alarms` | `<role>_enabled` |
| Go consumer queues | `<project>-<env>-<role>-queue-age`, `-queue-depth`, `-queue-dlq` | `worker-alarms` | `<role>_enabled` |
| SES routing DLQ | `<project>-<env>-ses-events-routing-dlq` | `worker-alarms` | on as soon as `email_delivery` is applied |
| Ingest worker queues | `<project>-<env>-telemetry-ingress-age`, `-depth`, `-dlq` | its own ([P3 "Alarms and deadlines"](p3-ingestion-rollout-runbook.md#alarms-and-deadlines)) | on |

While a Go role is disabled:

- its unit-health alarm treats missing data as breaching, and sits in
  `INSUFFICIENT_DATA` or `ALARM`;
- its queue alarms treat missing data as not breaching, and sit in `OK`.

CloudWatch acts only on state changes. An alarm that is already in `ALARM` when its
actions turn on never notifies for that breach. So drain a backlog or a DLQ first, and
turn a role's actions on only once every one of its alarms reads `OK`. The thresholds are
in `infra/opentofu/README.md`, "Alarms".
