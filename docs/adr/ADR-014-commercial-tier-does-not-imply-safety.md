# ADR-014 — Commercial tier does not imply hardware compatibility or command safety.

**Status:** Accepted

## Context

Entitlements answer what a customer purchased, not whether a device supports an operation or whether current physical conditions make that operation safe.

## Decision

Evaluate commercial entitlement, effective capability, risk policy, authorization, runtime preconditions, and physical verification as distinct gates. Passing one never substitutes for another.

## Consequences

Billing changes cannot silently authorize unsafe commands or certify hardware. Product messaging and services must expose independent denial reasons and preserve safety behavior during billing degradation.

## V4 traceability

V4 §§11, 16, 17, 24 Phases 4 and 8, 27, and 31 preserve the separation between tier, compatibility, and safety.

## Implementation gate

Phase 4 defines entitlement meaning without safety claims. Phase 8 must prove a permitted plan still fails when capability, approval, precondition, or physical-verification gates fail.

## Non-goals

This record does not remove quotas, bundle safety certification into a plan, or allow payment state to imply monitoring or command health.
