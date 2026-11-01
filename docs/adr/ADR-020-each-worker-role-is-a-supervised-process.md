# ADR-020 — Each independently operated worker role has its own supervised process; Go workers run as native systemd units.

**Status:** Accepted

## Context

The worker processes have no clear runtime boundary.

- `cmd/notifications` is one binary with five roles: the one-shot relay, the email worker, the Telegram worker and two backfills. Its `worker` subcommand runs both SES consumers under one `Supervisor`: the email-jobs consumer that calls SES and the SES-feedback consumer. They share one process, one failure domain, one exit code and one IAM user, so neither can be restarted, rolled back or observed on its own.
- No Go worker runs in production. `compose.production.yaml` defines none, nothing schedules `alert-evaluator run` or the relay, and CI publishes Go images that nothing pulls.
- The telemetry ingest worker runs in Compose profile `ingestion`, but `limnopulse-deploy` never passes that profile. A promotion or rollback therefore leaves the worker on its previous image. It has no healthcheck and no liveness signal, and `restart: on-failure:5` does not restart it after a Docker daemon or host restart.

## Decision

Each independently operated worker role must have its own supervised process, failure boundary, configuration and observability. Multiple roles may share a compiled executable and internal libraries, but must not share a runtime lifecycle unless explicitly justified. A justified shared lifecycle is recorded, with its reason, in `ops/workers/roles.yaml`; none exists today.

The boundary is therefore the supervised process, not the binary. For packaging, this release still gives every Go role its own executable, so each one ships only its own code and dependencies. The normative role table, with unit names, paths, IAM principals, metrics and enable variables, is `ops/workers/roles.yaml`:

| Role | Entrypoint | Lifecycle | Work |
|---|---|---|---|
| `alert-evaluator` | `cmd/alert-evaluator` | one-shot, systemd timer every 60 s | rules → outbox |
| `notification-relay` | `cmd/notification-relay` | one-shot, systemd timer every 60 s | outbox → channel SQS lanes |
| `email-worker` | `cmd/email-worker` | continuous | SQS email jobs → SES (SES consumer 1) |
| `ses-feedback-worker` | `cmd/ses-feedback-worker` | continuous, single instance | SQS SES events → delivery state (SES consumer 2) |
| `telegram-worker` | `cmd/telegram-worker` | continuous | SQS Telegram jobs → Bot API |
| `worker-monitor` | `cmd/worker-monitor` | one-shot, systemd timer every 60 s | unit health → CloudWatch |
| `limnopulse-admin` | `cmd/limnopulse-admin` | operator CLI, no unit | backfills |
| `telemetry-ingest` | `python -m limnopulse_api.workers.telemetry_ingest` | continuous, Compose | SQS telemetry ingress → observations |

**Go workers are native executables.** They stay in one Go module and share `internal/` packages. CI builds each `cmd/<role>` as a static linux/amd64 executable and packs them, with their systemd units, into one workers bundle per release. The bundle carries a `SHA256SUMS` manifest, its own SHA-256 and a build-provenance attestation. It is stored in GHCR as a non-container OCI artifact under the same `main-<sha>` tag as the images. Docker never runs a Go worker.

**systemd supervises them.** A root-owned helper verifies a bundle, extracts it into `/opt/limnopulse/workers/releases/<tag>`, installs its units and swaps the `current` link with an atomic rename. Continuous workers are `Type=notify` services with a watchdog fed by loop progress and `Restart=on-failure`. One-shot roles are `Type=oneshot` services started by timers. Every unit runs as a `DynamicUser` with a hardened sandbox, reads non-secret settings from `/etc/limnopulse/workers/<role>.env` and receives its AWS keys through `LoadCredential=`. Logs go to journald.

**The scheduler stays outside the domain.** The evaluator and the relay keep no loop, sleep or polling. systemd does not overlap a running one-shot with itself, but correctness still rests on the existing leases, fencing and conditional writes.

**Exit codes are shared.** `0` success, `1` runtime failure, `2` partial one-shot run, `78` configuration error. Units do not restart on `78`. A continuous worker exits `0` only after a stop signal.

**Each role has its own IAM principal**, scoped to its queues, tables, indexes and per-item transaction actions. The shared `workers` user is deprecated.

**Each SES consumer is observed separately**: its own unit, principal, queue age, depth and DLQ alarms, and unit-health metric. The EventBridge SES routing DLQ has its own alarm. `worker-monitor` publishes unit health to the `Limnopulse/Workers` namespace, so a stopped unit, a stalled timer or a silent host raises an alarm.

**A release is one tag.** Promotion and rollback move the API image, the Compose workers in the deploy's profile list and the workers bundle to the same tag. The ingest worker stays in the API image under Compose, and the deploy passes its profile explicitly on promotion and rollback. It restarts `unless-stopped`, writes a heartbeat on every loop turn, exits non-zero when the loop stalls, and exposes a `healthcheck` subcommand. This supersedes the P3 plan's bounded `on-failure` restart (D7) and its rule that the default deploy does not start the worker (D21).

## Consequences

The Go container images and their build and scan matrix entries are retired; local development runs the executables directly against the local emulators. The VPS needs a one-time root bootstrap: the deploy script, the activation helper and its `sudoers` rule, the release directories and the per-role configuration. InfluxDB is published on the host loopback for the evaluator. Redis stays internal until the Telegram worker is enabled, which then needs a loopback port with a password and a matching API `REDIS_URL`.

Roles ship disabled. Their units are installed, their alarms are off, and an owner enables one role at a time. Rolling the `ses-feedback-worker` back below a release that changed feedback recovery semantics needs a manual decision, so the bundle can declare a rollback floor that automatic rollback respects.

## V4 traceability

V4 §7 keeps a modular monolith with specialized workers in one Go module; this record replaces the §7.7 subcommand lanes with one entrypoint per lane, and future lanes such as `push-worker`, `sms-worker` and `sms-feedback-worker` follow the same rule. §7.2 and §7.3 keep the Python ingest worker and the one-shot, sharded, fenced evaluator. §23.2 requires send and feedback queue age and DLQ metrics and SES routing DLQ visibility. §30 keeps one repository.

## Implementation gate

Each independently operated worker role must have its own supervised process, failure boundary, configuration and observability.

Multiple roles may share a compiled executable and internal libraries, but must not share a runtime lifecycle unless explicitly justified; a shared lifecycle must be recorded with its justification in `ops/workers/roles.yaml`.

The email-jobs consumer and the SES-feedback consumer must run as separate units with separate IAM principals, queue alarms and unit-health metrics; the SES routing DLQ must have its own alarm.

The alert evaluator and the notification relay must stay one-shot with their schedule in systemd timers outside the domain; overlapping invocations must remain safe through the existing leases, fencing and conditional writes.

Every promotion and rollback must move the API image, the Compose workers in the deploy profile list and the workers bundle to the same release tag; the deploy must verify the bundle SHA-256 and its `SHA256SUMS` manifest before installing anything.

A continuous worker must exit non-zero when its loop stops making progress, and its supervisor must restart it, including after a host or container-daemon restart.

A configuration error must exit `78`, and no supervisor may restart a unit on that code.

A unit-health alarm must stay disabled while its role is disabled, and a disabled role must publish no unit-health metric.

## Non-goals

This record does not split the Go module or repository, move the API or the ingest worker out of Compose, enable email or Telegram delivery in production, add a DLQ consumer, or change notification, evaluator or ingest semantics.
