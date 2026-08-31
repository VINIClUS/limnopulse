# ADR-004 — Effective capability is provider/model/instance/runtime-derived.

**Status:** Accepted

## Context

Provider implementation, model profiles, discovered evidence, per-instance overrides, and current health can disagree. A single mutable boolean would overstate what a device can currently do.

## Decision

Effective capability is derived from connector/provider implementation, model profile, discovered evidence, instance override, and current runtime health rather than stored as one authoritative flag.

## Consequences

APIs can explain support versus availability and avoid stale promises. Consumers retain provenance; entitlement, authorization, risk, operational preconditions, and physical verification remain independent execution gates.

## V4 traceability

V4 §§9, 11, 24 Phases 1, 5, and 6, plus §27 preserve this layered capability rule.

## Implementation gate

Phase 1 freezes declarations and provenance. Provider and health phases must prove derivation; Phase 8 treats effective capability as one input while enforcing entitlement, authorization, risk, preconditions, and physical verification separately.

## Non-goals

This record does not infer physical safety from billing tier, firmware strings, provider acceptance, or a device's self-assertion alone.
