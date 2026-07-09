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

variable "alerts_queue_name" {
  description = "SQS queue name for future alert and notification workers."
  type        = string
  default     = "limnopulse-alerts"
}

variable "alerts_dlq_name" {
  description = "SQS dead-letter queue name for future alert and notification workers."
  type        = string
  default     = "limnopulse-alerts-dlq"
}

variable "ses_email_identity" {
  description = "Optional SES email or domain identity. Leave empty to skip SES identity creation."
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

locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "opentofu"
  }
}
