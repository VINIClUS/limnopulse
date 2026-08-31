# ADR-015 — Automatic cloud control is deferred; critical interlocks remain local.

**Status:** Accepted

## Context

Cloud latency, outages, stale telemetry, integration ambiguity, and incomplete physical context make unattended actuation materially riskier than advisory or approved commands.

## Decision

Defer automatic cloud control until operational evidence and a separate decision gate exist. Critical interlocks and emergency behavior remain local to customer/vendor equipment.

## Consequences

The first commercial release favors observation, incidents, recommendations, and safety-gated manual/assisted action. Future automation must fund simulation, dry-run history, approvals, monitoring, rollback, and safety review.

## V4 traceability

V4 §§4, 12, 24 Phase 10, 27, and 31 explicitly defer automatic policies and retain local critical interlocks.

## Implementation gate

Phase 10 cannot begin automatic execution without tenant risk acceptance, policy simulation, change management, model monitoring, kill switches, rollback, and an approved safety review. Phase 10 automatic execution requires dry-run history accumulated over time and may not rely on a one-off policy simulation.

## Non-goals

This record does not prohibit analytics or recommendations, promise future automation, or move local emergency controls into the SaaS runtime.
