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

Phase 1 freezes provider-neutral IntegrationAccount and DeviceIntegration contracts. Phase 5 may ship the AWS IoT adapter only after cross-device policy denial, trusted mapping, replay, rotation, and decommission tests pass. Phase 5 ingest must resolve tenant ownership exclusively from the authenticated source mapping; any tenant asserted in the payload, including another tenant's ID, must be ignored or rejected and must never override that mapping. Phase 5 certificate rotation must verify the replacement AWS IoT identity can connect before disabling the old certificate; a failed replacement must keep the old certificate enabled. During Phase 5 enrollment, if LimnoPulse generates an AWS IoT private key, it must return that key only at creation and must never persist it or return it later; only the certificate ID or fingerprint may be stored. Before any queued consumer acts, it must recheck the DeviceIntegration lifecycle state; after decommission, both queued command dispatch and queued ingest must be fenced.

Phase 5 ingest must resolve the complete source-to-tenant-to-site-to-asset-to-deployment-to-component ownership chain from trusted authenticated mapping; every payload-supplied site, asset, deployment, or component ID must be ignored or rejected and must never override that chain.

Phase 5 must emit immutable audit events for every DeviceIntegration credential link, credential rotation, credential revocation, Device provisioning, and Device decommissioning; each event must record actor or worker, tenant/device/integration identity, action, outcome, timestamp, and authoritative provider evidence.

Phase 5 AWS IoT policy fixtures must positively allow only the mapped client ID, device telemetry/health/reported publication paths, and required command/shadow subscriptions, and must negatively reject other device IDs, wildcard actions/topics, and command/system publication paths.

## Non-goals

This record does not require every deployment to use AWS IoT, define a custom broker, or place AWS identifiers on Device, Component, Deployment, or telemetry entities.
