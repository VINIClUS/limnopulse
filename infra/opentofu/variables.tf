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

locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "opentofu"
  }
}
