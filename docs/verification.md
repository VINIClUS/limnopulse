# Verification

LimnoPulse exposes the same credential-free verification boundaries to local
contributors and GitHub Actions. The gates reproduce repository dependency and
configuration inputs; they do not promise byte-identical local toolchains.

## Prerequisites

Install GNU Make, Python 3.12 or newer, uv, the Go version declared by
`go.mod`, OpenTofu 1.10.0 or newer (required for the S3 backend's native
locking used by `infra/opentofu/backend.example.hcl`), and Docker with the
Compose plugin. The uv and OpenTofu binaries can be overridden with the `UV`
and `TOFU` Make variables when a controlled local binary is required.

## Local gates

- `make verify-python` checks that `uv.lock` matches `pyproject.toml`, syncs
  the `dev` extra from the lock, and runs the normal repository pytest suite.
- `make verify-go` runs all repository Go tests with the race detector using
  `go.mod` and `go.sum`.
- `make verify-tofu` initializes `infra/opentofu` with the configured backend
  disabled, checks recursive formatting, and validates the configuration.
- `make verify-compose` parses and validates `compose.yaml` quietly. It does
  not start, build, pull, or create services.
- `make verify` runs all four boundaries.

The Python suite includes `tests/integration/test_notifications_local.py`, but
that multiprocess integration remains opt-in. It is skipped unless
`RUN_NOTIFICATION_INTEGRATION=1` is set after its required local services have
been started explicitly.

## InfluxDB integration gate

`make verify-influx` runs the real-InfluxDB integration tests against
InfluxDB 2.7.11: the Python tests in `tests/influx/` and every Go test named
`TestInfluxIntegration`, both with `RUN_INFLUX_INTEGRATION=1`.
`make verify-influx` is not part of `make verify`. Without that variable the
default `make verify-python` and `make verify-go` skip these tests, so they
never need InfluxDB. The CI `influx` job runs the target against a throwaway
`influxdb:2.7.11-alpine` service container, which is the only place the dev
token exists.

The harness reads `INFLUX_INTEGRATION_URL` (default `http://127.0.0.1:8086`),
`INFLUX_INTEGRATION_TOKEN` (default `local-dev-token`) and
`INFLUX_INTEGRATION_ORG` (default `limnopulse`); an empty value counts as
unset. Python tests use `tests/support/influx_harness.py`: the
`influx_bucket` fixture gives each test a uniquely named bucket with no
retention rule, deleted at teardown, and `influx_clients` and `write_lines`
write to it synchronously. Go tests in `internal/alertevaluator/influx` use
`newIntegrationBucket` and `writeLines`, which create and delete buckets
through `BucketsAPI()`.

To run the gate locally without Docker:

```
scripts/dev/run_local_influxd.sh
make verify-influx
```

`scripts/dev/run_local_influxd.sh` downloads
`influxdb2-2.7.11_linux_amd64.tar.gz`, checks its SHA-256
`8d7872013cad3524fb728ca8483d0adc30125ad1af262ab826dcf5d1801159cf` and
unpacks it into `${XDG_CACHE_HOME:-$HOME/.cache}/limnopulse/influxd`. It
starts influxd on `127.0.0.1:8086` (`INFLUXD_PORT` overrides the port),
leaves it running in the background with `influxd.pid` and `influxd.log` in
that directory, waits for `/health` and onboards the default org and token
through `POST /api/v2/setup`. A re-run reuses the running instance, and an
"onboarding has already been completed" conflict counts as success. Stop it
with `kill "$(cat ~/.cache/limnopulse/influxd/influxd.pid)"`.

Rules for integration tests: every Flux query string a task introduces must
be executed by at least one integration test, because InfluxDB 2.7.11 panics
on some syntactically valid chains. Tests use absolute `range` bounds;
relative-window code paths are tested with data seeded relative to a `now`
captured once per test, and every comparison asserts the result is non-empty
first.

## Safety and readiness boundary

These gates use no AWS credentials. OpenTofu initialization has its backend
disabled, and verification never runs `tofu plan` or `tofu apply`. Compose is
parsed only and does not start services. Public package and provider registries
may still be contacted to install locked dependencies or provider plugins, so
credential-free does not mean offline.

Passing these checks does not establish AWS sandbox or production readiness.
Cloud deployment, remote-state configuration, environment smoke tests, and
operational readiness remain a later gate.

## Optional M1 end-to-end harness

`ops/e2e/` holds an opt-in end-to-end check for the M1 Site/Asset/Deployment
model. It is not part of `make verify` and it does not start services by
itself. Readings are published with MQTTX (directly, or replayed from a Wokwi
serial log by `ops/e2e/wokwi/replay.py`), a headless Node-RED flow
(`ops/e2e/node-red/flows.json`, generated from `functions/*.js` by
`build_flows.py`) asks the API which deployment owned the device at the
reading's `ts`, and every reading yields one verdict on
`limnopulse/e2e/verdicts`. Legacy devices resolve through `/v1` and the
deterministic default component (reason `match`); canonical `dev_` devices
unknown to `/v1` resolve through `/v2` only (reason `v2-native`).

To run it locally, start the compose dependencies, initialize and seed
DynamoDB, run the M1 backfill
(`python -m scripts.migrations.m1_backfill_default_site_assets --tenant
tnt_local_001`, then again with `--apply`), and start the API with
`API_V2_ENABLED=true` and `V1_POND_DEVICE_PROJECTION_ENABLED=true`. With
`node-red`, `mqttx` and `curl` on `PATH`, run:

```
LIMNOPULSE_API_URL=http://127.0.0.1:8000 bash ops/e2e/run_m1_e2e.sh
RUN_M1_E2E=1 uv run --locked --extra dev pytest -q tests/e2e/test_m1_e2e_stack.py
```

Besides the replayed readings for `local-device-001`, the harness creates a
pond and a new device through `/v1`, moves that device West to East and
expects its readings before and after the move to resolve to each pond's
asset, and creates a native `/v2` device with a deployment. These writes stay
in the table, so use a disposable one (`DYNAMODB_DOMAIN_TABLE`).
`ALLOW_V2_UNAVAILABLE=1` accepts `ok=null` verdicts while `/v2` is off; the
default is strict.

The harness runs from the repository root and keeps its run files (Node-RED
user directory, MQTTX capture, verdicts, CI logs) in the ignored `.e2e-out/`
directory. `E2E_WORK_DIR` is deleted and recreated on every run, so it must be
a subdirectory of `.e2e-out/`; the Python tools only read files inside the
working directory, so `WOKWI_SERIAL_LOG` must point inside the repository. Each published reading is identified by its device, `seq` and
`ts`, and must get exactly one verdict; duplicates from QoS-1 redelivery are
accepted only when they agree.

Telegraf limitation: Telegraf enriches readings from the static registry in
`infra/telegraf/device_registry.star`, which only knows `local-device-001`.
Readings of the devices the harness creates reach the verdict flow but are not
ingested into Influx, and `local-device-001` is never moved. Registry-driven
or `/v2`-driven enrichment is P2/P3 work and out of scope for M1.

The `e2e-m1` workflow (`.github/workflows/e2e-m1.yml`) runs on manual dispatch
and on a weekly schedule only. `ops/e2e/ci_stack.sh up` starts the compose
dependencies on the disposable runner, seeds, runs the M1 backfill (dry-run,
apply, apply again) and starts the API with both M1 flags. The run is strict
unless the dispatch input `allow_v2_unavailable` or the repository variable
`M1_E2E_ALLOW_V2_UNAVAILABLE=1` is set. The optional `wokwi` job compiles and
simulates the probe sketch and replays its serial output, and it runs only
when the `WOKWI_CLI_TOKEN` secret is configured; a separate gate job checks
for the secret, and only the simulation step receives it.

`ops/e2e/ci_stack.sh down` stops the API and runs `docker compose down -v`,
which deletes the local compose volumes (DynamoDB, Influx and broker data). It
therefore refuses to run unless `CI=true` or `GITHUB_ACTIONS=true` is set, as
on a disposable runner; on a workstation it needs an explicit
`bash ops/e2e/ci_stack.sh down --force`.
