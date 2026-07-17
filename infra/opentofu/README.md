# Limnopulse Cloud Infrastructure

This directory contains the OpenTofu scaffold for cloud infrastructure. Docker Compose remains the development runtime for local dependencies; OpenTofu owns cloud resources.

## Included

- DynamoDB domain and audit tables with on-demand billing, `PK` / `SK` string
  keys, alert indexes, and the sparse `NotificationRelayByAvailableAt` GSI.
- Cognito User Pool and app client for user authentication.
- Encrypted notification-jobs and SES-events queues with redrive after eight
  receives, plus jobs, feedback, and EventBridge-routing DLQs.
- SESv2 configuration set and EventBridge routing for Send, Delivery,
  DeliveryDelay, Bounce, Complaint, and Reject feedback.
- Placeholder variables for Redis and InfluxDB cloud endpoints.

## Not Included Yet

- Redis cloud provisioning.
- InfluxDB managed provisioning.
- Production MQTT TLS/mTLS, broker ACLs, or device credential rotation.
- SES identity verification, production sending-access requests, DNS records or
  sandbox-recipient verification. `SES_FROM_EMAIL` is a runtime secret/config
  chosen per environment, not a generic stack default.
- Container orchestration, the external 60-second relay/evaluator schedulers,
  worker autoscaling, dashboards or alarms.
- Telegram, WhatsApp, SMS, or mobile push delivery.

## Local Validation

```bash
tofu init -backend=false
tofu fmt -check
tofu validate
```

Use `-backend=false` for local scaffold validation so placeholder backend values are not contacted. Copy `env/cloud.tfvars.example` to an untracked `.tfvars` file only when preparing a real cloud plan. Replace backend placeholders with real remote-state infrastructure outside this repository before any real cloud workflow.

## Notification rollout boundary

Creating the relay GSI is not enough to enable publishing. Existing email
NotificationOutboxes need the explicit tenant-scoped `notifications
backfill-relay` migration, and the index must be `ACTIVE` before the relay runs.
Do not enable the one-shot relay or continuous worker in the same infrastructure
apply that first introduces the GSI. Verify SES identity/production access and
start one worker before enabling the external relay schedule.

The full order, rollback, DLQ handling and PII constraints are documented in
[Phase 3C-A notification operations](../../docs/notifications-phase-3c-a.md).
