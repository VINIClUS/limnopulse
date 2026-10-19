# ADR-006 — Event time, receive time and ingest time are distinct.

**Status:** Accepted

## Context

Devices can buffer, retry, drift, or arrive out of order. One timestamp cannot distinguish when the observation occurred, when LimnoPulse received it, and when durable storage completed.

## Decision

Canonical telemetry records event time, receive time, and ingest time as separate UTC-aware values with provenance and quality treatment.

## Consequences

Historical deployment resolution and latency diagnosis become accurate, while replay and clock-skew policy can be explicit. Storage and queries carry additional fields and must choose the correct time for each operation.

## V4 traceability

V4 §§10, 24 Phase 2, and 27 retain the three-time observation model.

## Implementation gate

Phase 2 must test delayed, duplicated, replayed, and out-of-order observations, resolve Deployment at event time, and prevent receive or ingest time from silently replacing event semantics. Phase 2 must detect negative or extreme clock skew and emit a quality flag while preserving original event-time semantics for delayed, replayed, and out-of-order observations. When no valid Deployment covers event time, Phase 2 must preserve the source event in bounded quarantine/DLQ metadata, mark connector health, and must not fall back to the current Deployment or location. Current-health views must use `received_at` or `ingested_at` so replaying an old `observed_at` cannot refresh current health or mark a device online. Event-time alert windows must apply a completeness delay for supported lateness and produce order-independent outcomes for the same observations.

**Phase split (P2).** P2 proves the event-time half of this gate without production writes: delayed, duplicated, replayed and out-of-order observations are idempotent by identity and carry quality flags, clock skew is flagged, the Deployment is resolved at event time with no fallback to the current Deployment or location (an unresolved one is a bounded quarantine record), and event-time alert windows keep their completeness delay and order independence on the v2 reader. Before production canonical writes (D16), P3 must prove quarantine persistence (DLQ) and connector health marking for unresolved Deployments, device and component last-seen, and durable queue redrive (#15, D13); current-health views on `received_at` or `ingested_at` are P6 (#17). See the [P2 telemetry rollout runbook](../p2-telemetry-rollout-runbook.md).

## Non-goals

This record does not claim device clocks are trustworthy, eliminate clock-quality flags, or define all retention policies.
