variable "project_name" {
  description = "Short project name used in cloud resource names."
  type        = string
  default     = "limnopulse"
}

variable "environment" {
  description = "Deployment environment name for cloud resources."
  type        = string
  default     = "cloud"
}

variable "aws_region" {
  description = "AWS region for cloud resources."
  type        = string
  default     = "us-east-2"
}

variable "dynamodb_domain_table" {
  description = "DynamoDB single-table domain table name."
  type        = string
  default     = "LimnopulseDomain"
}

variable "dynamodb_audit_table" {
  description = "DynamoDB audit table name."
  type        = string
  default     = "LimnopulseAudit"
}

variable "cognito_user_pool_name" {
  description = "Cognito User Pool name for API users."
  type        = string
  default     = "limnopulse-users"
}

variable "cognito_client_name" {
  description = "Cognito User Pool app client name for the API/frontend."
  type        = string
  default     = "limnopulse-app"
}

variable "notification_jobs_queue_name" {
  description = "SQS queue name consumed by the notification email worker."
  type        = string
  default     = "limnopulse-notification-jobs"
}

variable "notification_jobs_dlq_name" {
  description = "Dead-letter queue name for notification jobs."
  type        = string
  default     = "limnopulse-notification-jobs-dlq"
}

variable "telegram_notification_jobs_queue_name" {
  description = "SQS queue name consumed only by the Telegram notification worker."
  type        = string
  default     = "limnopulse-telegram-notification-jobs"
}

variable "telegram_notification_jobs_dlq_name" {
  description = "Dead-letter queue name for Telegram notification jobs."
  type        = string
  default     = "limnopulse-telegram-notification-jobs-dlq"
}

variable "telegram_bot_token_secret_name" {
  description = "Secrets Manager container name for the Telegram bot token; the value is populated out of band."
  type        = string
  default     = "limnopulse/telegram/bot-token"
}

variable "telegram_webhook_secret_name" {
  description = "Secrets Manager container name for the Telegram webhook secret; the value is populated out of band."
  type        = string
  default     = "limnopulse/telegram/webhook-secret"
}

variable "ses_events_queue_name" {
  description = "SQS queue name consumed by the SES feedback worker."
  type        = string
  default     = "limnopulse-ses-events"
}

variable "ses_events_dlq_name" {
  description = "Dead-letter queue name for malformed or repeatedly failing SES feedback."
  type        = string
  default     = "limnopulse-ses-events-dlq"
}

variable "ses_events_routing_dlq_name" {
  description = "Dead-letter queue name for EventBridge-to-SQS routing failures."
  type        = string
  default     = "limnopulse-ses-events-routing-dlq"
}

variable "ses_configuration_set_name" {
  description = "SESv2 configuration set attached by the notification worker."
  type        = string
  default     = "limnopulse-notifications"
}

variable "ses_eventbridge_rule_name" {
  description = "EventBridge rule that routes SES feedback into SQS."
  type        = string
  default     = "limnopulse-ses-events"
}

variable "ses_from_email" {
  description = "Verified SES sender address (SES_FROM_EMAIL) the notification worker sends from. Scopes iam_runtime.tf's email_worker policy via ses:FromAddress; the SES identity itself is verified out of band (no aws_ses_email_identity resource — see the identity boundary test)."
  type        = string
  default     = ""
}

variable "redis_url" {
  description = "Cloud Redis endpoint for application configuration. Provisioning is intentionally out of scope here."
  type        = string
  default     = ""
}

variable "influxdb_url" {
  description = "Cloud InfluxDB endpoint for application configuration. Provisioning is intentionally out of scope here."
  type        = string
  default     = ""
}

# Feature boundaries from docs/superpowers/specs/2026-08-29-limnopulse-production-deployment-design.md
# §16: "The current unconditional SES, EventBridge and Telegram resources
# must be made conditional before a real production plan." DynamoDB, Cognito
# and the core notification-jobs queue are load-bearing for every profile and
# stay unconditional; only the two delivery channels below are gated.
variable "email_delivery" {
  description = "Provision SES, its EventBridge feedback routing, and the ses_events SQS queues. False until the email channel is actually wired up (ops/vps and Fase 2 do not use it yet)."
  type        = bool
  default     = false
}

variable "telegram_delivery" {
  description = <<-EOT
    Provision the Telegram bot token secret, the outbound telegram-jobs SQS
    queue, and the Telegram worker's IAM policy — the Fase 2 async delivery
    path. Independent of var.telegram_webhook (see telegram.tf); a profile
    can run the inbound webhook without the outbound worker, or vice versa.
  EOT
  type        = bool
  default     = false
}

variable "telegram_webhook" {
  description = <<-EOT
    Provision the Telegram webhook secret and its reader IAM policy
    (telegram.tf). Independent of var.telegram_delivery. Both false (the
    default) is the zero-Telegram-resources profile from §16/§3 of the
    design spec, and is fully bootable in APP_ENV=prod:
    src/limnopulse_api/core/config.py's TELEGRAM_WEBHOOK_ENABLED (default
    true, set to false in that profile) independently gates both the
    inbound POST /webhooks/telegram route and the APP_ENV=prod requirement
    for TELEGRAM_WEBHOOK_SECRET_ARN. Set both this flag and
    TELEGRAM_WEBHOOK_ENABLED=true together when a real bot is wired up —
    see cloud.tfvars.example and .env.production.example.
  EOT
  type        = bool
  default     = false
}

locals {
  # Owner=vinisantana matches the design spec's tagging contract (§ resource
  # tagging): every taggable resource carries Project/Environment/ManagedBy/
  # Owner. Applying it here in the shared local means every resource in this
  # module gets it, not just the ones touched in a given change.
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "opentofu"
    Owner       = "vinisantana"
  }
}
