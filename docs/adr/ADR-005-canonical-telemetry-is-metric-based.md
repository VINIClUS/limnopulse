# ADR-005 — Canonical telemetry is metric-based and transport-independent.

**Status:** Accepted

## Context

Fixed water-quality fields couple storage, APIs, alerts, and transports to the initial sensor set and make multiple probes for one metric difficult to distinguish.

## Decision

Represent canonical observations by metric identity, value, unit, source component, provenance, and quality independently of MQTT, HTTPS, AWS IoT, or vendor payload shape.

## Consequences

New metrics and integrations can enter through normalization without altering the core envelope. Catalog governance, unit conversion, cardinality limits, and legacy pivot logic become explicit responsibilities.

## V4 traceability

V4 §§3, 10, 24 Phase 2, and 27 define generic observations and transport-independent normalization.

## Implementation gate

Phase 2 must prove unit conversion, two probes for one metric, trusted tenant mapping, v1 pivot equivalence, and bounded tag cardinality before v2 canonical writes are enabled.

## Non-goals

This record does not require replacing InfluxDB, trusting tenant data from device payloads, or removing legacy telemetry reads during migration.
