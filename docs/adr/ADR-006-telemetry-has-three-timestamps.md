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

Phase 2 must test delayed, duplicated, replayed, and out-of-order observations, resolve Deployment at event time, and prevent receive or ingest time from silently replacing event semantics. Phase 2 must detect negative or extreme clock skew and emit a quality flag while preserving original event-time semantics for delayed, replayed, and out-of-order observations.

## Non-goals

This record does not claim device clocks are trustworthy, eliminate clock-quality flags, or define all retention policies.
