# Worker runtime runbook (ADR-020)

This page covers operating the workers on the VPS after the host bootstrap. Each role runs
as its own supervised process
([ADR-020](adr/ADR-020-each-worker-role-is-a-supervised-process.md)). The Go roles are
systemd units from the workers bundle, and the telemetry ingest worker is a Compose service.
The rules live elsewhere, and this page links to them instead of repeating them:

| Topic | Source |
|---|---|
| Role table: units, paths, credentials, principals, alarms | [`ops/workers/roles.yaml`](../ops/workers/roles.yaml) |
| Exit codes, unit health, bundle, units, activation helper, deploy | [`ops/workers/README.md`](../ops/workers/README.md) |
| One-time host bootstrap | [`ops/vps/README.md`](../ops/vps/README.md), section 7 |
| Principals, access keys, enabling a role's alarms, the SES check | [`infra/opentofu/README.md`](../infra/opentofu/README.md), "Worker roles (ADR-020)" |
| Ingest worker rollout and rollback | [`docs/p3-ingestion-rollout-runbook.md`](p3-ingestion-rollout-runbook.md) |

Run the host commands below as root on the VPS, unless a command says otherwise.

## Starting state

The first bundled promotion installs every unit and enables none. Every Go role starts out
like this:

- its unit or timer is installed and disabled, and it publishes no `UnitHealthy` datapoint;
- its principal exists or not, as its delivery flag decides, and holds no access key;
- its alarms have `actions_enabled = false`, so a breaching alarm notifies nobody.

The ingest worker runs whenever its host switch, `TELEMETRY_V2_WRITE_ENABLED`, is on. The
deploy removes it while the switch is off.

## Look at the workers

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

For the ingest worker, run these as the deploy user from `/opt/limnopulse`:

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
| `78` | configuration error: an env file setting, a credential or a flag | never restarted; a one-shot fails again at every tick | fix the env file or the credential, then `systemctl reset-failed limnopulse-<role>.service` and `systemctl start` it (one-shots: let the timer start it) |
| `SIGABRT` | an unrecovered panic (`GOTRACEBACK=crash`), or the systemd watchdog | treated like `1` | the journal holds the goroutine dump |

A continuous worker exits `1` by itself once its loop stops making progress, before the
watchdog fires. A watchdog kill therefore means the process could not even exit.

## Enable a Go role

The AWS side, keys and alarms included, follows "Enabling and disabling a role" in
[`infra/opentofu/README.md`](../infra/opentofu/README.md). Enable `worker-monitor` first,
because every other unit-health alarm reads its datapoints. Enable consumers before their
producers: the email-worker and the SES-feedback worker, then the Telegram worker, then the
relay, then the evaluator. A producer started first leaves its jobs aging in a queue that
nothing reads.

On the host, for each role:

1. Write `/etc/limnopulse/workers/<role>.env` (root, 0600) from
   [`ops/workers/env/<role>.env.example`](../ops/workers/env/). Take every placeholder
   from the OpenTofu outputs. An env file holds no secret and never sets `GOTRACEBACK`,
   `LIMNOPULSE_SUPERVISOR` or an `*_ENDPOINT_URL`.
2. Install the access key with `install_worker_key`
   ([`infra/opentofu/README.md`](../infra/opentofu/README.md), "Access keys"). Write any
   other credential the role lists with the `read -rs` pattern in `ops/vps/README.md`,
   step 7.4.
3. Pass the role's gate below.
4. Run `systemctl enable --now limnopulse-<role>.timer` for a one-shot, or
   `systemctl enable --now limnopulse-<role>.service` for a continuous role. The enable
   must be persistent: a `--runtime` or linked unit publishes nothing.
5. Check `journalctl -u limnopulse-<role>.service` for the first run, then wait for the
   role's alarms to reach `OK` before you set `<role>_enabled`.

| Role | Gate before step 4 |
|---|---|
| `worker-monitor` | The static user exists: `getent passwd limnopulse-worker-monitor` shows a uid below 61184 (`ops/vps/README.md`, step 7.5). |
| `email-worker`, `ses-feedback-worker` | `email_delivery` is applied, the SES identity is verified, and the SES check passes (`infra/opentofu/README.md`, "The email-worker's SES check"). Enable the two together. |
| `telegram-worker` | `telegram_delivery` is applied and the bot-token secret has a version. Redis is published on the host loopback with a password, and that password is the `redis-password` credential: `compose.production.yaml` does not publish Redis yet, so this needs its own change first. Set `TELEGRAM_DELIVERY_ENABLED=true` in the evaluator's and the relay's env files in the same change. |
| `notification-relay` | `NotificationRelayByAvailableAt` is `ACTIVE`, and the `limnopulse-admin backfill-relay` migration has run (`infra/opentofu/README.md`, "Notification rollout boundary"). |
| `alert-evaluator` | The `influxdb-token` credential can read `INFLUXDB_BUCKET_RAW`. The data-quality settings in the env file match the devices. The `limnopulse-admin backfill-schedule` migration has run ([`docs/alert-evaluator-phase-3b.md`](alert-evaluator-phase-3b.md), "Initial schedule backfill"). |

## Disable a Go role

Reverse the order, as "Enabling and disabling a role" says. Turn the alarm actions off
first (`<role>_enabled = false`, apply), then disable the unit:

```bash
systemctl disable --now limnopulse-<role>.timer     # one-shot
systemctl disable --now limnopulse-<role>.service   # continuous
```

Then delete the role's access key and its credential files. With the unit stopped, its
unit-health alarm reads missing data as breaching, but its actions are already off.

## Rotate a credential

Run `install_worker_key` again; IAM allows two keys per user. Then make the unit read the
new files: `LoadCredential=` copies them when a unit starts.

- **Continuous role:** `systemctl restart limnopulse-<role>.service`.
- **One-shot role:** the next tick reads the new files.

Delete the old key last (`aws iam list-access-keys`, then `aws iam delete-access-key`).
For the InfluxDB token or the Redis password, write the new value over the credential file
and restart the same way.

## Releases

A promotion is the `deploy.yml` workflow, run by hand with a `main-<40 hex>` tag. The tag's
images and its workers bundle must already be in GHCR. One tag moves the API, the profiled
Compose workers and the bundle together. The deploy order and its rollback are in
`ops/workers/README.md`, "Deploy".

- **Automatic rollback.** A failed health check or activation moves the API, the ingest
  worker and the bundle back to the release that `.env.image` records. Activation then
  restarts the workers the failed activation stopped.
- **Manual rollback.** Run `deploy.yml` again with the previous tag. The helper keeps two
  releases, but the deploy pulls and verifies the bundle again, so the older tag's bundle
  must still be in GHCR.
- **Rollback floor.** A release whose role epoch is below a recorded floor is refused with
  exit `3`, and the deploy logs that it needs a manual decision. Only root, from a real root
  login, can override it (`ops/vps/README.md`, "Rollback floor override").
- **No bundle.** The deploy refuses a tag without a bundle. It returns to the no-bundle
  state only on its own automatic rollback, and only when the previous release had no
  bundle.

The first `main` push after the bundle job landed fails `workers-publish` at "Pull the
bundle back": the `workers` package is private until someone makes it public. Make it
public (`ops/vps/README.md`, section 4), then re-run the failed job.

## The ingest worker

The ingest worker is the `telemetry-ingest-worker` Compose service, in profile `ingestion`.

- **Running.** Every deploy starts it and waits for it to be healthy while
  `TELEMETRY_V2_WRITE_ENABLED` is on in `/opt/limnopulse/.env`. Docker restarts it
  `unless-stopped`.
- **Health.** Its healthcheck reads the liveness stamp that the loop writes every turn.
  A stalled loop exits `1`, and Docker restarts it.
- **Parked.** A configuration error exits `78`. The entrypoint guard then keeps the
  container up but idle, logs "parked, not restarted", and the container turns unhealthy.
  Fix `.env`, then recreate it (the deploy user, from `/opt/limnopulse`):

  ```bash
  sudo -u limnopulse-deploy env COMPOSE_PROFILES= docker compose --env-file .env \
    --env-file .env.image -f compose.production.yaml --profile ingestion \
    up -d --force-recreate telemetry-ingest-worker
  ```

- **Turning it off.** Follow the rollback order of the P3 runbook: `INGEST_V2_ENABLED`
  goes off before `TELEMETRY_V2_WRITE_ENABLED`. The next deploy then removes the worker.

The ingest worker publishes no `UnitHealthy`. Its Compose healthcheck and the
telemetry-ingress queue age, depth and DLQ alarms observe it.

## Alarms

All worker alarms publish to the `worker-alarms` topic. Their names are
`<project_name>-<environment>-<role>-<alarm>`, where `<alarm>` is `unit-health` or, for a
consumer, `queue-age`, `queue-depth` or `queue-dlq`. While a role is disabled, its
alarms sit in `INSUFFICIENT_DATA` or `ALARM` with actions off. That is expected, and it is
why actions go on only after every alarm of the role reads `OK`. The thresholds and the
SES routing DLQ alarm are in `infra/opentofu/README.md`, "Alarms".
