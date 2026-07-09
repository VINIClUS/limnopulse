# Limnopulse Cloud Infrastructure

This directory contains the OpenTofu scaffold for cloud infrastructure. Docker Compose remains the development runtime for local dependencies; OpenTofu owns cloud resources.

## Included

- DynamoDB domain and audit tables with on-demand billing and `PK` / `SK` string keys.
- Cognito User Pool and app client for user authentication.
- SQS alert queue and DLQ for future workers.
- Optional SES identity, disabled while `ses_email_identity` is empty.
- Placeholder variables for Redis and InfluxDB cloud endpoints.

## Not Included Yet

- Redis cloud provisioning.
- InfluxDB managed provisioning.
- Production MQTT TLS/mTLS, broker ACLs, or device credential rotation.
- Go workers, notification dispatchers, Telegram, WhatsApp, or SMS.

## Local Validation

```bash
tofu init -backend=false
tofu fmt -check
tofu validate
```

Use `-backend=false` for local scaffold validation so placeholder backend values are not contacted. Copy `env/cloud.tfvars.example` to an untracked `.tfvars` file only when preparing a real cloud plan. Replace backend placeholders with real remote-state infrastructure outside this repository before any real cloud workflow.
