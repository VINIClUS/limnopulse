# ADR-009 — Edge is optional, software-only and customer-hosted.

**Status:** Accepted

## Context

Some installations need protocol translation, local buffering, or local safety behavior, while others can integrate directly. Mandatory LimnoPulse hardware would add CAPEX and support obligations.

## Decision

Any LimnoPulse edge component is optional software deployed on customer-controlled infrastructure. Direct HTTPS, AWS IoT, and vendor-cloud integrations remain valid without it.

## Consequences

Customers choose topology and retain local infrastructure responsibility. The edge contract must version security, buffering, replay, upgrade, and compatibility behavior without becoming canonical identity.

## V4 traceability

V4 §§4, 7, 13, 15, 24 Phase 9, and 27 define the optional customer-hosted edge boundary.

## Implementation gate

Phase 9 may ship an edge contract only after authentication, local buffer/replay, upgrade, and compatibility acceptance tests pass and direct integrations remain operational. Separately from edge acceptance, Phase 9 may ship the first vendor connector only after tests prove connector upgrades preserve stable canonical metric identity, rate-limit/retry/cursor recovery, and clearly expose the compatibility level.

## Non-goals

This record does not require LimnoPulse appliances, arbitrary customer code in SaaS, a custom broker, or cloud-dependent critical interlocks.
