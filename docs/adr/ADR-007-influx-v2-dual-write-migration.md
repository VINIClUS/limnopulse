# ADR-007 — InfluxDB v2 uses generic numeric observations and dual-write migration.

**Status:** Accepted

## Context

The existing `water_quality` measurement and field-oriented reads are live v1 contracts. Replacing them in one cutover would risk evaluator and history divergence.

## Decision

Introduce a generic numeric-observation schema alongside the legacy measurement. Use controlled dual writes and independent v1 projection/read switches until parity is demonstrated.

## Consequences

Migration is rollbackable per write and read path, but temporarily costs extra storage and requires divergence monitoring and idempotent replay handling.

## V4 traceability

V4 §§3, 13, 24 Phase 2, and 27 prescribe InfluxDB continuity, generic observations, dual write, and legacy pivot equivalence.

## Implementation gate

Phase 2 enables v2 reads only after duplicate, ordering, conversion, cardinality, evaluator parity, and byte/semantic v1 visibility tests pass. Either write path can be disabled without deleting legacy data.

## Non-goals

This record does not mandate a big-bang backfill, remove the legacy measurement, or make dual write permanent.
