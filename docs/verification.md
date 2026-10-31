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
- `make verify-compose-production` parses `compose.production.yaml` with
  `.env.production.example` and a dummy image tag. It does not start, build,
  pull, or create services. The build workflow uses this same target.
- `make verify` runs these five boundaries. Frontend checks remain separate:
  run `npm ci`, `npm test`, `npm run typecheck` and `npm run build` inside
  `frontend/` with Node 22, matching CI.

`tests/unit/test_vps_deploy.py` executes the real deployment script in a
temporary stack, replacing only Docker and SSH at the command boundary.
It covers literal workflow inputs, promotion, pull/up failures, health
timeout, rollback failure and first-deployment cleanup without deleting
volumes. It never connects to the VPS.

Build jobs for pull requests use read-only repository access and do not
persist checkout credentials. Only the separate publisher, gated on a push
to `main`, receives `packages: write`; both paths use the same four-image
matrix and build steps. The parse-only production gate remains read-only.

Leave rollout variables unset for the normal test command so each test can
choose its settings. Forcing `V1_POND_DEVICE_PROJECTION_ENABLED=false` in the
process environment also overrides the deliberately enabled defaults of test
Settings subclasses. Keep local `.env` rollout flags absent or at their
documented defaults; use explicit flags for the opt-in E2E processes below.

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
InfluxDB 2.7.11 service container, `influxdb:2.7.11@sha256:d92a10e9e75aff18eca38ff3a8f0b4a800706a5dd44d1b0ece264af04458525b`.
It is pinned by digest because the `influxdb:2.7.11-alpine` tag was rebuilt
with InfluxDB 2.7.12, and the integration tests must run the same 2.7.11 as
the local runner. Go integration tests run with `-count=1`, so a cached result
never stands in for a run against the server. The dev token `local-dev-token` is
a fixed, non-secret default that exists only in throwaway instances (that CI
service container and the local influxd below); no repository secret is used.

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
starts influxd on `127.0.0.1:8086`, leaves it running in the background with
`influxd.pid` and `influxd.log` in that directory, waits for `/health`,
checks that the server is v2.7.11 and onboards the default org and token
through `POST /api/v2/setup`. A re-run reuses the instance it started, and an
"onboarding has already been completed" conflict counts as success unless
the cached instance was onboarded with a different org or token. It refuses
to start when another server (for example the compose InfluxDB) already
answers on the port. `INFLUXD_PORT` overrides the port; then also set
`INFLUX_INTEGRATION_URL=http://127.0.0.1:$INFLUXD_PORT` for the tests. Stop it
with `kill "$(cat "${XDG_CACHE_HOME:-$HOME/.cache}/limnopulse/influxd/influxd.pid")"`.

Rules for integration tests: every Flux query string a task introduces must
be executed by at least one integration test, because InfluxDB 2.7.11 panics
on some syntactically valid chains. Tests use absolute `range` bounds;
relative-window code paths are tested with data seeded relative to a `now`
captured once per test, and every comparison asserts the result is non-empty
first.

## Ingestion integration gate

`make verify-ingestion` runs the P3 ingestion harness tests in `tests/ingestion/`
against ElasticMQ 1.6.16 (an SQS emulator), DynamoDB Local 3.3.2 and InfluxDB
2.7.11. It sets both `RUN_INGESTION_INTEGRATION=1` and
`RUN_INFLUX_INTEGRATION=1`: the reused `influx_bucket` fixture skips without the
second flag, so the Influx-backed tests would otherwise be skipped silently.
Under the gate, `tests/ingestion/conftest.py` turns any skip or `xfail` in that
directory into a failure (an xfail hides a failing test just as silently). `make verify-ingestion` is not part of `make verify`, and the
default `make verify-python` skips `tests/ingestion` cleanly without
`RUN_INGESTION_INTEGRATION=1`, so it never needs these services.

The CI `ingestion` job runs the target against three throwaway service
containers, each pinned by digest:

- `softwaremill/elasticmq-native:1.6.16@sha256:8383f29ad746c12981ca2128f689a4b4a93815235f1a7851d2f165316503d132`, on port 9324;
- `amazon/dynamodb-local:3.3.2@sha256:6ada631ec7aa5fc83446855eb17233e3dad184fec47477e8140c6c569cfe0c86`,
  mapped `8001:8000` to match the harness default;
- the same InfluxDB 2.7.11 digest and onboarding as the `influx` job.

No credentials are involved: the harness clients carry dummy credentials set in
code, so the workflow names no AWS credential variable.

The harness, `tests/support/ingestion_harness.py`, reads
`INGESTION_SQS_URL` (default `http://127.0.0.1:9324`) and
`INGESTION_DYNAMODB_URL` (default `http://127.0.0.1:8001`); an empty value
counts as unset. The InfluxDB variables are those of the InfluxDB gate above. It
provides:

- `sqs_client()`, `dynamodb_client()` and `wait_until_ready()`. Tests that use
  the services wait once for both emulators to answer before their first call.
- The `ingestion_queue_pair` factory fixture. `make(max_receive_count=2,
  visibility_seconds=2)` creates a uniquely named source queue whose
  `RedrivePolicy` points at its own DLQ, and both are deleted at teardown.
- The `ingestion_tables` fixture: a uniquely named domain and audit table pair.
  They have exactly the key schemas and GSIs of `scripts/dev/init_dynamodb.py`,
  built by its `table_request()`, and are deleted at teardown. TTL is **not**
  enabled on them (`ensure_table(..., enable_ttl=False)`), because DynamoDB
  Local deletes expired items within seconds once TTL is on. Expiry is tested by
  advancing an injected clock, never by waiting for the emulator.
- `ToggleableTcpProxy`, a loopback TCP forwarder placed in front of InfluxDB to
  simulate an outage without stopping a service. `down()` closes the listener,
  so new connections are refused at once, and resets every established
  forwarded socket, so pooled keep-alive connections cannot bypass it. `up()`
  listens again on the same port.

ElasticMQ 1.6.16 counts message size in characters (1,048,576 at most) and
ignores `MaximumMessageSize`. The 262,144-byte SQS limit is therefore enforced
and tested in the P3 adapter, not in the emulator.

ElasticMQ 1.6.16 is also more lenient than SQS on visibility: it accepts
`ChangeMessageVisibility` on a message that is not in flight (SQS answers
`MessageNotInflight`) and a visibility of 43201 s (SQS allows at most 43200).
Neither can be caught against the emulator, so the unit fake,
`FakeIngressQueue` in `tests/support/ingestion_fakes.py`, refuses both, and the
P3 adapter clamps a visibility to 43200 before calling SQS. The fake's
`send_raw` applies the same size guard as the adapter, and the unit DynamoDB
fake rejects an unaliased reserved word (`state`, `status`, ...) as DynamoDB
does.

To run the gate locally without Docker (Linux x86_64, Java 17 or newer):

```
scripts/dev/run_local_elasticmq.sh
scripts/dev/run_local_dynamodb.sh
scripts/dev/run_local_influxd.sh
make verify-ingestion
```

`scripts/dev/run_local_elasticmq.sh` downloads `elasticmq-server-1.6.16.jar`,
checks its SHA-256
`30a81f56a40b670fafc3f32b0f09f3baf59ad2645edaf17bc2e79170c75479f2` and starts
it on `127.0.0.1:${ELASTICMQ_PORT:-9324}`. Its generated config uses strict SQS
limits and declares no queues. It waits for `ListQueues` to answer.

`scripts/dev/run_local_dynamodb.sh` downloads
`v2.x/dynamodb_local_latest.tar.gz`, checks its SHA-256
`7a94eb65f81aee270738751a4e5e3e6a7ad02a5d25194f55b83e08ec2ba6342b`
(DynamoDB Local 3.3.2) and starts it with `-inMemory` on port
`${DYNAMODB_LOCAL_PORT:-8001}`. DynamoDB Local has no bind-address flag, so it
listens on `0.0.0.0` (every interface), unlike the other two runners, which
bind `127.0.0.1`; run it only on a trusted network or behind a host firewall. That URL always serves the latest release, so a
checksum mismatch stops the script with a message asking for a deliberate
re-pin of the checksum, the CI digest and this page.

Both runners keep their files (download, PID, port and log) under
`${XDG_CACHE_HOME:-$HOME/.cache}/limnopulse/`, leave the server running in the
background, and reuse the instance they started on a re-run. They refuse to
start when another server already answers on the port. With a non-default
`ELASTICMQ_PORT` or `DYNAMODB_LOCAL_PORT`, also set `INGESTION_SQS_URL` or
`INGESTION_DYNAMODB_URL` for the tests. Stop a runner with
`kill "$(cat "${XDG_CACHE_HOME:-$HOME/.cache}/limnopulse/elasticmq/elasticmq.pid")"`,
or the same with `dynamodb-local/dynamodb-local.pid`.

Every ingestion integration test creates its own queues, tables and buckets and
removes them afterwards.

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

### Local Wokwi toolchain and access

Use the versions and SHA-256 checksums in `.github/workflows/e2e-m1.yml`:
Arduino CLI 1.3.1, ESP32 core 3.0.7, Wokwi CLI 0.28.1, MQTTX CLI 1.13.1,
Node-RED 5.0.8, pyserial 3.5 and the four Arduino libraries pinned there.
Install binaries and libraries in a user-owned tool directory; an isolated
Python environment for pyserial avoids changing system Python. Keep Node 22
available for these tools without changing an existing global Node default.

Create a free account at https://wokwi.com/login and a CI token at
https://wokwi.com/dashboard/ci. The Free allowance is 50 simulation minutes
in a rolling 30-day window, with a five-minute maximum per session. Store the
token outside the checkout in a mode-0600 file and register it as the Actions
secret `WOKWI_CLI_TOKEN`. Feed it to `gh secret set` through stdin and to the
simulator through its process environment; never put the value in a command
argument, `.env`, shell history or serial log.

Compile and lint without a token:

```bash
arduino-cli compile --fqbn esp32:esp32:esp32 --warnings default \
  --output-dir ops/e2e/wokwi/build ops/e2e/wokwi/sketch/limnopulse_probe
wokwi-cli lint --warnings-as-errors ops/e2e/wokwi
```

With `WOKWI_CLI_TOKEN` securely loaded into the simulator's environment:

```bash
wokwi-cli ops/e2e/wokwi --scenario scenario.yaml \
  --serial-log-file ops/e2e/wokwi/serial.log --timeout 120000
WOKWI_SERIAL_LOG=ops/e2e/wokwi/serial.log \
  uv run --locked --no-sync python -m pytest -q tests/e2e/test_wokwi_capture.py
```

The scenario waits for the end of each JSON line; the strict replay parser
consumes the capture without dropping malformed records. MQTT access from
inside the simulator is optional: replay the serial output through the local
MQTTX/Node-RED harness. A Private IoT Gateway is not required. Run the M1 E2E
with `WOKWI_SERIAL_LOG` pointing to the capture, and dispatch `e2e-m1` to verify
the GitHub secret too. A skipped job or the deterministic fixture alone does
not establish that online simulation works.

For local integration, select a dedicated `COMPOSE_PROJECT_NAME`, an explicit
Compose file list, disposable DynamoDB tables and local endpoints. Check port
conflicts first. Use the same project/files during cleanup; preserve the
default development stack's volumes. Influx integration uses the exact
2.7.11 digest above, independently of mutable development tags.

`ops/e2e/ci_stack.sh down` stops the API and runs `docker compose down -v`,
which deletes the local compose volumes (DynamoDB, Influx and broker data). It
therefore refuses to run unless `CI=true` or `GITHUB_ACTIONS=true` is set, as
on a disposable runner; on a workstation it needs an explicit
`bash ops/e2e/ci_stack.sh down --force`.
