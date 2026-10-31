# ADR-007 — InfluxDB v2 uses generic numeric observations and dual-write migration.

**Status:** Accepted

## Context

The existing `water_quality` measurement and field-oriented reads are live v1 contracts. Replacing them in one cutover would risk evaluator and history divergence.

## Decision

Introduce a generic numeric-observation schema alongside the legacy measurement. Use controlled dual writes and independent v1 projection/read switches until parity is demonstrated.

## Consequences

Migration is rollbackable per write and read path, but temporarily costs extra storage and requires divergence monitoring and idempotent replay handling.

## V4 traceability

V4 §§3, 10, 24 Phase 2, and 27 prescribe InfluxDB continuity, generic observations, dual write, and legacy pivot equivalence.

## Implementation gate

Phase 2 enables v2 reads only after duplicate, ordering, conversion, cardinality, evaluator parity, and byte/semantic v1 visibility tests pass. Before dual write is enabled, Phase 2 must prove an Influx outage after durable queue acceptance leaves telemetry retryable or moves it to a recoverable DLQ, never acknowledges or loses it after a failed write, and recovers it without data loss. Either write path can be disabled without deleting legacy data.

**Phase split (P2).** P2 proves unit conversion, two probes for one metric, trusted tenant mapping, `/v1` pivot equivalence (byte-identical except summary means within relative 1e-12 on completed windows), bounded tag cardinality, `out_of_range` with persisted provenance and versions, evaluator parity, and that an InfluxDB outage after acceptance is never acknowledged and is recovered by redelivery; every write path ships behind its own default-off switch. Before production canonical writes and legacy dual write, P3 must deliver durable queue redrive with quarantine persistence (#15, D13); before `V1_TELEMETRY_READ_SOURCE=v2` or `ALERT_EVALUATOR_TELEMETRY_SOURCE=v2` (D16), a producer for every legacy device and a clean keyed divergence check over max(longest rule window, legacy read window). See the [P2 telemetry rollout runbook](../p2-telemetry-rollout-runbook.md).

**Phase split (P3).** P3 delivers durable queue redrive: an InfluxDB outage after the HTTPS request was accepted leaves every message on the SQS queue until `maxReceiveCount` moves it to the DLQ, and an operator redrive reproduces the exact v2 and legacy point set of the no-fault baseline (G7). With legacy dual write off, the HTTPS path's `observation_numeric` points match `water_quality` written independently by the legacy writer, with zero keyed divergence (G15). Legacy dual write may be enabled only after G7. The D16 read-switch precondition, a producer for every legacy MQTT device, stays open and moves to the Backlog issue #104. See the [P3 ingestion rollout runbook](../p3-ingestion-rollout-runbook.md).

## Non-goals

This record does not mandate a big-bang backfill, remove the legacy measurement, or make dual write permanent.
