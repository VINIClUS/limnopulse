# Limnopulse Phase 3B Alert Evaluator Design

**Date:** 2026-07-15
**Status:** Approved for implementation

## Execution model

Phase 3B adds a Go binary with a one-shot `alert-evaluator run` command. A run
captures one immutable logical `evaluation_time`, processes eligible work and
exits. It contains no permanent loop, polling cycle or scheduler. systemd,
Kubernetes and EventBridge may invoke the same command every 60 seconds.

The command accepts `--evaluation-time`, `--shard` and `--shard-count`.
Sixty-four persisted virtual buckets are independent of the process count. A
process owns bucket `B` when `B % shard_count == shard`.

## Scheduling and coordination

Enabled rules have a sparse evaluation index. The bucket is the first eight
bytes, interpreted as an unsigned big-endian integer, of
`SHA-256(tenant_id + NUL + rule_id)`, modulo 64.

```text
GSI1PK = ALERT_EVALUATION#V1#BUCKET#<00-63>
GSI1SK = <fixed-width UTC next_evaluation_at>#TENANT#<tenant>#RULE#<rule>
```

Rules are discovered only with paginated DynamoDB Query operations. A
conditional DynamoDB lease with an incrementing `lease_epoch` is authoritative;
Redis is an optional short-lived hint. Every final write checks the public rule
version, evaluation revision, lease owner and fencing epoch.

Late work is coalesced to the most recent complete slot. Missed slots never
confirm `duration`, and pending confirmation restarts after a gap. Active
incidents remain active through gaps and indeterminate data.

## Data quality and evaluation

The evaluator queries a bounded InfluxDB window ending at the complete slot.
Quality buckets use the configured expected sample interval (10 seconds in
local/test); state transitions use a 60-second evaluation cadence. Quality is
one of `sufficient`, `insufficient_data`, `stale_data` or `query_error`, and only
`sufficient` may advance the state machine.

Pond rules build one logical series across devices without frequency weighting.
`min` and `max` use canonical bucket values; `mean` averages devices per bucket
and then occupied buckets; `last` averages each device's last value in the most
recent complete evaluation slot and never falls back to an older slot.

## Incident lifecycle

One `AlertEvent` represents one continuous episode. `duration` confirms only
opening, counting `ceil(duration / 60s)` consecutive sufficient violated slots.
A complete sufficient clean window resolves an active event. No-data, stale
data and query failures never open or resolve an event.

Opening normally creates the event, transition and one ready notification
outbox per channel in one DynamoDB transaction. A rapid reopening during the
durable cooldown still creates an event, but with `status=suppressed` and no
outbox. Recovery creates blocked outboxes whose per-channel dependencies point
to the corresponding opening outboxes. SQS publication and delivery remain in
Phase 3C.

Configuration changes that alter evaluation semantics create a new internal
`evaluation_revision` and administratively resolve an active previous
generation without notification. Cosmetic updates keep the generation.

## API and operations

Phase 3B exposes tenant-scoped alert list/detail, acknowledgement and manual
resolution endpoints. All tenant roles can read, members may acknowledge, and
only owner/admin may resolve. Events, transitions, cooldown and idempotency are
durable DynamoDB state. Redis is never a source of truth.

Every command emits a final structured JSON summary. OTLP metrics/traces are
optional and bounded on shutdown. Canonical exits are `0` success, `1` fatal or
incomplete scope, and `2` isolated partial failures.

