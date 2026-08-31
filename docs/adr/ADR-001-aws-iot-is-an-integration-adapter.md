# ADR-001 — AWS IoT is an integration adapter, not the Device domain.

**Status:** Accepted

## Context

LimnoPulse must support direct devices, vendor clouds, local ingestion, and future customer-hosted edge software. Making AWS Thing, certificate, policy, or shadow identity canonical would couple every device and migration to one transport provider.

## Decision

The core Device and Integration models are provider-neutral. AWS IoT implements provisioning, authentication, trusted mapping, and ingress behind an adapter; its identifiers and credentials stay in integration records.

## Consequences

Customers can use non-AWS paths without synthetic AWS resources, and provider replacement does not rename devices. The adapter must maintain explicit mapping, lifecycle, certificate revocation, and reconciliation behavior.

## V4 traceability

V4 §§3, 9, 13–14, 24 Phase 5, and 27 preserve AWS IoT as an optional supported integration rather than domain identity.

## Implementation gate

Phase 1 freezes provider-neutral IntegrationAccount and DeviceIntegration contracts. Phase 5 may ship the AWS IoT adapter only after cross-device policy denial, trusted mapping, replay, rotation, and decommission tests pass. During Phase 5 enrollment, if LimnoPulse generates an AWS IoT private key, it must return that key only at creation and must never persist it or return it later; only the certificate ID or fingerprint may be stored. Before any queued consumer acts, it must recheck the DeviceIntegration lifecycle state; after decommission, both queued command dispatch and queued ingest must be fenced.

## Non-goals

This record does not require every deployment to use AWS IoT, define a custom broker, or place AWS identifiers on Device, Component, Deployment, or telemetry entities.
