# LimnoPulse Architecture Decision Records

These accepted records map the V4 architecture to explicit implementation gates. Launch configuration values remain configuration or release records unless a listed ADR explicitly says otherwise.

| Record | Entry phase |
|---|---|
| [ADR-001](ADR-001-aws-iot-is-an-integration-adapter.md) | Phase 1 contract; Phase 5 adapter |
| [ADR-002](ADR-002-site-and-asset-preserve-v1-pond.md) | Phase 1 |
| [ADR-003](ADR-003-device-component-and-temporal-deployment.md) | Phase 1 |
| [ADR-004](ADR-004-effective-capability-is-derived.md) | Phase 1 contract |
| [ADR-005](ADR-005-canonical-telemetry-is-metric-based.md) | Phase 2 |
| [ADR-006](ADR-006-telemetry-has-three-timestamps.md) | Phase 2 |
| [ADR-007](ADR-007-influx-v2-dual-write-migration.md) | Phase 2 |
| [ADR-008](ADR-008-hardware-accuracy-remains-customer-vendor-owned.md) | Phase 6 |
| [ADR-009](ADR-009-edge-is-optional-and-customer-hosted.md) | Phase 9 |
| [ADR-010](ADR-010-stripe-is-an-adapter-internal-entitlements-are-canonical.md) | Phase 4 |
| [ADR-011](ADR-011-limnopulse-owns-notification-semantics.md) | Phase 7A |
| [ADR-012](ADR-012-commands-use-a-separate-safety-plane.md) | Phase 8 |
| [ADR-013](ADR-013-v1-remains-compatible-v2-is-generalized.md) | Phase 1 |
| [ADR-014](ADR-014-commercial-tier-does-not-imply-safety.md) | Phase 4 contract; Phase 8 safety gate |
| [ADR-015](ADR-015-automatic-cloud-control-is-deferred.md) | Phase 10 decision gate |
| [ADR-016](ADR-016-eventbridge-is-selective-sqs-is-durable.md) | Existing feedback; future bus gate |
| [ADR-017](ADR-017-sns-is-provider-feedback-not-notification-service.md) | Phase 7C |
| [ADR-018](ADR-018-eum-push-and-sms-are-provider-adapters.md) | Phases 7B–7C |
| [ADR-019](ADR-019-redis-valkey-is-optional-acceleration.md) | Phase 7A |
