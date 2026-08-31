# ADR-003 — Separate Device, Component, Probe, Actuator and temporal Deployment.

**Status:** Accepted

## Context

The current Device permanently carries `pond_id`, conflating gateway identity, attached hardware, capability, and current placement. Probe replacement or movement would otherwise rewrite history.

## Decision

Model Device separately from DeviceComponent, ProbeProfile, ActuatorProfile, and half-open temporal Deployment. Canonical Deployment targets a Component; a legacy Device receives one deterministic default Component for projection.

## Consequences

A gateway can host multiple probes, and placement at event time remains reconstructable. Services must prevent overlapping Component intervals and preserve immutable ended history.

## V4 traceability

V4 §§3, 9, 20, 24 Phase 1, 27, and 32 define the entity split and Component-scoped deployment history.

## Implementation gate

Phase 1 accepts the model only when a gateway with multiple probes is representable, relocation does not rewrite history, adjacent intervals are valid, and v1 Device pond changes project compatible transitions. Phase 1 must reject overlapping Deployment intervals for the same Component while permitting adjacent half-open intervals where one ends exactly when the next starts.

## Non-goals

This record does not make every Component independently network-addressable or permit historical deployment reassignment.
