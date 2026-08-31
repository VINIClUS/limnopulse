# ADR-004 — Effective capability is provider/model/instance/runtime-derived.

**Status:** Accepted

## Context

Hardware family claims, integration support, per-instance configuration, runtime availability, and safety verification can disagree. A single mutable boolean would overstate what a device can currently do.

## Decision

Effective capability is derived from declared provider/model support, instance configuration, runtime evidence, entitlement, risk classification, and required verification rather than stored as one authoritative flag.

## Consequences

APIs can explain support versus availability and avoid stale promises. Consumers must retain provenance and evaluate all required inputs before telemetry or commands rely on a capability.

## V4 traceability

V4 §§8, 11, 16, 24 Phases 1, 5, 6, and 8, plus §27 preserve this layered capability rule.

## Implementation gate

Phase 1 freezes declarations and provenance. Provider, health, and command phases must prove derivation and reject unavailable or unsafe execution even when a commercial plan permits the feature.

## Non-goals

This record does not infer physical safety from billing tier, firmware strings, provider acceptance, or a device's self-assertion alone.
