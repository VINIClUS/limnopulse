# Worker runtime contract (ADR-020)

`roles.yaml` is the normative role table: unit names, paths, credentials, IAM principals,
alarms and enable variables. `tests/unit/test_worker_roles.py` validates it. This page
records the rules a table cannot express. Every slice that packages, deploys, monitors or
provisions a worker builds against both.

## Exit codes

| Code | Meaning | Supervisor |
|---|---|---|
| `0` | success; a continuous worker returns it only after SIGTERM/SIGINT | no restart |
| `1` | runtime failure, including a loop that stopped making progress | `Restart=on-failure` restarts continuous units |
| `2` | partial one-shot run | the next timer tick runs it again |
| `78` | configuration error | `RestartPreventExitStatus=78`; never restarted |

A timer starting a one-shot on its next tick is scheduling, not a restart. A failed run
shows up as `UnitHealthy=0`. Only exit `0` is a successful run: units never set
`SuccessExitStatus=2`, so a partial run stays visible as a failed run.

Nothing else may exit `2`. Every Go unit sets `GOTRACEBACK=crash` (`go_unit_environment`
in `roles.yaml`), so an unrecovered panic dies by `SIGABRT` instead of exiting `2`.
Entrypoints parse flags with `workerproc.ParseFlags`, which switches the flag set to
`flag.ContinueOnError` and returns a bad flag as a configuration error; the flag package's
default `ExitOnError` would exit `2`.

Go roles use `internal/workerproc`. `workerproc.Continuous` sends `READY=1` and
`STOPPING=1`. It feeds `WATCHDOG=1` only while the loop reports progress, and it exits `1`
once `stall_after_seconds` pass without any. That threshold stays below `watchdog_seconds`
(the unit's `WatchdogSec`), so the clean exit wins and the systemd watchdog is only the
backstop. After a stop signal, a loop that returns `nil` or its context's error exits `0`.

`workerproc.Credentials` reads `LoadCredential=` files from `$CREDENTIALS_DIRECTORY`. Each
unit loads every credential `<name>` from `paths.credential_source`,
`/etc/limnopulse/workers/credentials/<role>/<name>`. The names repeat across roles, but
each role's directory holds its own principal's keys. Every Go unit also sets
`LIMNOPULSE_SUPERVISOR=systemd`. With it, a missing `CREDENTIALS_DIRECTORY` is a
configuration error, so a unit never falls back to the AWS default chain. Local runs leave
it unset and use the default chain. `INVOCATION_ID` cannot serve as the marker: terminals
and CI runners started by systemd inherit it. Secrets never go in the env file: the Telegram worker's Redis password is
the `redis-password` credential.

## Unit health

`worker-monitor` publishes `Limnopulse/Workers` `UnitHealthy` (1 or 0) with dimension
`Role`. It publishes only for roles whose service (continuous) or timer (one-shot) is
enabled, so a disabled role publishes nothing.

- **Continuous role:** healthy when the service is `active`/`running`.
- **One-shot role:** healthy when its timer is active and the last run succeeded within
  `oneshot_stale_after_intervals` schedule intervals.
- **worker-monitor:** reports itself too.
- **Not covered:** the Compose ingest worker and the operator CLI publish no unit health.

The Compose ingest worker restarts `unless-stopped`. It stamps `liveness_file` on every
loop turn, and its Compose healthcheck runs the `healthcheck` subcommand against that
stamp. A stall exits `1` and Compose restarts it. A configuration error exits `78`, and
the entrypoint guard then parks the container idle instead of restarting it.

Unit-health alarms treat missing data as breaching. They have
`actions_enabled = var.<role>_enabled`, so they stay disabled while the role is disabled.

To enable a role:

1. Enable its unit or timer.
2. Wait for the first datapoint.
3. Set the OpenTofu enable variable.

## Workers bundle

The bundle is one deterministic `limnopulse-workers-<tag>.tar.gz`: sorted entries,
mtime 0, uid and gid 0, `gzip -n`. It contains:

```
bundle.env      BUNDLE_FORMAT=1, BUNDLE_TAG=<tag>, EPOCH_<ROLE>=<n>, FLOOR_<ROLE>=<n>
roles.json      roles.yaml rendered as JSON
bin/<role>      static linux/amd64 executable, one per Go role
systemd/*       limnopulse-<role>.service and .timer units
SHA256SUMS      sha256sum lines for every other file, relative paths
```

In `bundle.env`, `<ROLE>` is the role name upper-cased with `-` replaced by `_`.

CI pushes the bundle to GHCR as an OCI artifact,
`ghcr.io/viniclus/limnopulse/workers:<tag>`, with the artifact type from `roles.yaml`. It
attaches a build-provenance attestation for the tarball.

## Activation helper

`/usr/local/sbin/limnopulse-workers-activate` is root-owned. `limnopulse-deploy` may run
it only through the sudoers line in `roles.yaml`. The helper validates its own arguments.

| Command | Effect |
|---|---|
| `stage <tag> <sha256>` | Pulls as root into a root-owned temporary directory. Verifies the tarball SHA-256, then `SHA256SUMS`, then the floor. Extracts to `releases/<tag>` and records the verified SHA-256 in `state/`. Changes no unit. |
| `activate <tag>` | Re-verifies `SHA256SUMS` and the floor. Installs the release's units, runs `daemon-reload` and swaps `current` with an atomic rename. Runs `try-restart` on enabled continuous units and enables nothing. |
| `deactivate` | Removes the installed units and the `current` link. This is the "no bundle" state of releases from before ADR-020. |
| `status` | Prints the current tag, the staged releases and the recorded floors. |

Exit codes: `0` ok, `1` failure, `3` floor refused, `78` usage. It keeps two releases.

**Rollback floor.** Activating a release whose `EPOCH_<ROLE>` is below the highest
`FLOOR_<ROLE>` recorded on the host is refused with `3`. Only a successful `activate`
records a release's floors; `stage` checks them but records nothing, so a staged release
that never went live cannot block a rollback. The deploy's automatic rollback never
overrides the floor. An override exists only for root itself: the helper refuses it
whenever `SUDO_USER` is set.

## Deploy

The SSH forced command accepts exactly `main-<40 hex> <64 hex>`: the release tag and the
bundle SHA-256. `deploy.yml` computes the SHA-256 after pulling the bundle and verifying
its attestation.

Before step 1 the deploy checks the host. `.env.image` must hold a well-formed
`IMAGE_TAG` and, if it records one, a well-formed `WORKERS_BUNDLE_SHA256`. The helper's
current bundle must be that recorded release with that SHA-256, or none when `.env.image`
records none. Otherwise the deploy stops before anything changes, and the host needs a
fix by hand.

1. Stage the bundle. A missing or tampered bundle stops the deploy before anything
   changes.
2. Pull and start Compose with each profile in `deploy.compose_profiles` whose role is
   enabled on the host. Stop and remove each disabled role's service explicitly: a plain
   `up -d` without its profile would leave an old-tag container running. Only these
   `--profile` flags choose profiles. The deploy clears `COMPOSE_PROFILES`, which `.env`
   could otherwise set, and after `up` it checks that each disabled role has no container.
3. Wait for `api` and each enabled profiled worker to be healthy.
4. Activate the bundle.
5. Record `IMAGE_TAG` and `WORKERS_BUNDLE_SHA256` in `.env.image`.

**Host switch.** A Compose role is enabled on the host when its `host_enable_env`, as
`docker compose config` resolves it, parses as true. The deploy parses it the way the
worker's settings parse a boolean: `true`, `1`, `yes`, `on`, `t` and `y` in any case are
on; `false`, `0`, `no`, `off`, `f` and `n` are off. Any other value, or a missing one,
stops the deploy before anything changes. The switch is not `enable_variable`, which
gates the role's AWS resources in OpenTofu. An enabled worker that cannot run, parked on
a configuration error for example, never turns healthy, so it fails the deploy.

**Refinement of ADR-020.** ADR-020 says the deploy passes the ingest worker's profile
explicitly on promotion and rollback. That holds only while the worker's host switch is
on. With the switch off, the worker would park on exit 78, never turn healthy, and so fail
every promotion. The deploy therefore removes it instead. ADR-020 itself is unchanged, and
this section is the rule.

Rollback moves the API and the profiled Compose workers back to the previous tag, applying
the same host-switch rule with the switch as it is at rollback time. The bundle moves back
only if step 4 ran; before that it never moved. Then rollback re-activates the previous
release from its already verified directory, or runs `deactivate` when the previous
release had no bundle.
