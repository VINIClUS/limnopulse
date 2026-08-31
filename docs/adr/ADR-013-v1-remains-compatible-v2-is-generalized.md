# ADR-013 — `/v1` remains compatible; generalized device domain is `/v2`.

**Status:** Accepted

## Context

Current clients depend on Pond, Device, telemetry, alert, and notification shapes. Generalizing those contracts in place would turn domain migration into an externally breaking rewrite.

## Decision

Preserve every current `/v1` path, shape, status code, role gate, identity, and legacy telemetry behavior. Add generalized Site, Asset, Device, Component, Integration, Deployment, and Capability contracts under `/v2` with deterministic compatibility projections.

## Consequences

Migration can proceed additively and roll back by disabling v2 paths. The application must test two representations, prevent drift, and explicitly migrate legacy identities without renaming them.

## V4 traceability

V4 §§3, 9, 19, 24 Phase 1, 27, and 32 make v1 compatibility the boundary for the first implementation milestone.

## Implementation gate

Phase 1 requires golden v1 compatibility, deterministic projection IDs, idempotent migration, no Scan, and feature-flag rollback before generalized routes are enabled.

## Non-goals

This record does not add new generalized fields to v1 responses, remove historical IDs, or keep the v1 domain canonical indefinitely.
