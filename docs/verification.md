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
when the `WOKWI_CLI_TOKEN` secret is configured.
