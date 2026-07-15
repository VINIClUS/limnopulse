# Alert Evaluator Phase 3B Operations

Phase 3B ships `alert-evaluator`, a one-shot Go process. Each `run` captures one
stable logical evaluation time, queries eligible rules, commits durable
decisions, emits a JSON summary and exits. It has no scheduler, infinite loop,
sleep or continuous polling. Invoke it every 60 seconds from the scheduler that
fits the deployment.

## Run locally

Initialize DynamoDB and the local seed before the first evaluation:

```bash
python scripts/dev/init_dynamodb.py
python scripts/dev/seed_local.py
docker compose --profile manual run --rm alert-evaluator run
```

Build or run without Compose:

```bash
go build -o bin/alert-evaluator ./cmd/alert-evaluator
DYNAMODB_ENDPOINT_URL=http://127.0.0.1:8001 \
  ./bin/alert-evaluator run --shard=0 --shard-count=1
```

For deterministic tests and replay, supply an RFC3339 timestamp. Replay remains
safe because the event identity and DynamoDB transaction conditions are
idempotent:

```bash
./bin/alert-evaluator run \
  --evaluation-time=2026-07-15T12:01:00Z \
  --shard=0 \
  --shard-count=1
```

The persisted schedule always has 64 virtual buckets. A process owns bucket
`B` when `B % shard_count == shard`; `1 <= shard_count <= 64`. Changing process
count needs no backfill. Changing the persisted total of 64 requires a new
index version or an explicit migration.

The default run deadline is 45 seconds, with five seconds reserved to drain
work before the next 60-second invocation. Use sharding when 250 rules or the
deadline cannot cover a run. Query and evaluation concurrency are independently
bounded by `--query-parallelism` and `--evaluation-parallelism`.

## Initial schedule backfill

The first deployment of `AlertEvaluationByDue` needs a one-time backfill for
active rules that existed before the index attributes. The command requires
explicit tenant IDs and uses tenant `Query` operations only; it never scans the
table. It is a dry-run unless `--apply` is present:

```bash
./bin/alert-evaluator backfill-schedule \
  --tenant=tnt_001 \
  --tenant=tnt_002 \
  --evaluation-time=2026-07-15T12:00:00Z

./bin/alert-evaluator backfill-schedule \
  --tenant=tnt_001 \
  --tenant=tnt_002 \
  --evaluation-time=2026-07-15T12:00:00Z \
  --apply
```

Review the dry-run counts before applying. Disabled and replaced rules are not
indexed.

## Scheduler examples

The scheduler is deliberately outside the domain. A systemd installation can
use a oneshot service and timer:

```ini
# /etc/systemd/system/limnopulse-alert-evaluator.service
[Service]
Type=oneshot
EnvironmentFile=/etc/limnopulse/alert-evaluator.env
ExecStart=/usr/local/bin/alert-evaluator run --shard=0 --shard-count=1
TimeoutStartSec=50
```

```ini
# /etc/systemd/system/limnopulse-alert-evaluator.timer
[Timer]
OnCalendar=*-*-* *:*:00
Persistent=true

[Install]
WantedBy=timers.target
```

A Kubernetes `CronJob` should use `schedule: "* * * * *"`,
`concurrencyPolicy: Forbid`, `activeDeadlineSeconds: 50` and the same container
command `run`. An EventBridge schedule can launch the same image as an ECS task
every minute. Temporary overlap is still safe: DynamoDB leases, fencing epochs,
deterministic identities and conditional transactions protect duplicate
invocations.

## Results and observability

Every invocation writes one JSON summary to stdout. Exit codes are:

- `0`: the owned scope completed successfully;
- `1`: fatal dependency error, deadline/cap incomplete scope, or lost
  authoritative coordination;
- `2`: isolated rule query failures were recorded durably and other work
  completed.

Set `OTEL_EXPORTER_OTLP_ENDPOINT` to enable OTLP/HTTP metrics. Export and
shutdown are bounded by `ALERT_EVALUATOR_OTLP_FLUSH_TIMEOUT` (two seconds by
default), and telemetry failure never changes a durable decision or exit code.
Metrics cover evaluated, delayed, fired, recovered, skipped and errored rules,
plus run duration. Labels are intentionally low-cardinality.

Alert Events, transitions, cooldown state and notification outboxes live in
DynamoDB. Redis is optional and never authoritative. Phase 3B does not publish
SQS messages and does not call SES or Telegram; outbox publication and delivery
start at the Phase 3C boundary.

Recovery currently occurs on the first complete, sufficiently covered, fresh
and valid clean window. `recovery_duration` and
`recovery_threshold`/hysteresis are reserved schema extensions if tests or
production telemetry later show flapping; opening `duration` is not implicitly
reused for recovery.
