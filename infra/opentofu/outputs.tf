output "aws_region" {
  description = "AWS_REGION"
  value       = var.aws_region
}

output "cognito_user_pool_id" {
  description = "COGNITO_USER_POOL_ID"
  value       = aws_cognito_user_pool.main.id
}

output "cognito_client_id" {
  description = "COGNITO_CLIENT_ID"
  value       = aws_cognito_user_pool_client.api.id
}

output "cognito_issuer" {
  description = "COGNITO_ISSUER"
  value       = "https://cognito-idp.${var.aws_region}.amazonaws.com/${aws_cognito_user_pool.main.id}"
}

output "dynamodb_domain_table" {
  description = "DYNAMODB_DOMAIN_TABLE"
  value       = aws_dynamodb_table.domain.name
}

output "dynamodb_audit_table" {
  description = "DYNAMODB_AUDIT_TABLE"
  value       = aws_dynamodb_table.audit.name
}

output "notification_jobs_queue_url" {
  description = "SQS_NOTIFICATION_JOBS_URL"
  value       = aws_sqs_queue.notification_jobs.id
}

output "notification_jobs_queue_arn" {
  description = "Notification jobs queue ARN."
  value       = aws_sqs_queue.notification_jobs.arn
}

output "notification_jobs_dlq_url" {
  description = "Notification jobs dead-letter queue URL."
  value       = aws_sqs_queue.notification_jobs_dlq.id
}

# The outputs below are null when their owning feature flag
# (var.telegram_delivery / var.email_delivery, see variables.tf) is false.

output "telegram_notification_jobs_queue_url" {
  description = "SQS_TELEGRAM_JOBS_URL (null unless telegram_delivery = true)"
  value       = try(aws_sqs_queue.telegram_notification_jobs[0].id, null)
}

output "telegram_notification_jobs_queue_arn" {
  description = "Telegram notification jobs queue ARN (null unless telegram_delivery = true)."
  value       = try(aws_sqs_queue.telegram_notification_jobs[0].arn, null)
}

output "telegram_notification_jobs_dlq_url" {
  description = "Telegram notification jobs dead-letter queue URL (null unless telegram_delivery = true)."
  value       = try(aws_sqs_queue.telegram_notification_jobs_dlq[0].id, null)
}

output "telegram_bot_token_secret_arn" {
  description = "TELEGRAM_BOT_TOKEN_SECRET_ARN (null unless telegram_delivery = true)"
  value       = try(aws_secretsmanager_secret.telegram_bot_token[0].arn, null)
}

output "telegram_worker_policy_arn" {
  description = "IAM policy ARN to attach to the Telegram worker runtime role (null unless telegram_delivery = true)."
  value       = try(aws_iam_policy.telegram_worker[0].arn, null)
}

# Unconditional (see telegram.tf): the API needs this regardless of delivery.
output "telegram_webhook_secret_arn" {
  description = "TELEGRAM_WEBHOOK_SECRET_ARN"
  value       = aws_secretsmanager_secret.telegram_webhook_secret.arn
}

output "telegram_webhook_secret_reader_policy_arn" {
  description = "IAM policy ARN to attach to the FastAPI runtime role."
  value       = aws_iam_policy.telegram_webhook_secret_reader.arn
}

output "ses_events_queue_url" {
  description = "SQS_SES_EVENTS_URL (null unless email_delivery = true)"
  value       = try(aws_sqs_queue.ses_events[0].id, null)
}

output "ses_events_queue_arn" {
  description = "SES feedback queue ARN (null unless email_delivery = true)."
  value       = try(aws_sqs_queue.ses_events[0].arn, null)
}

output "ses_events_dlq_url" {
  description = "SES feedback dead-letter queue URL (null unless email_delivery = true)."
  value       = try(aws_sqs_queue.ses_events_dlq[0].id, null)
}

output "ses_events_routing_dlq_url" {
  description = "EventBridge SES routing dead-letter queue URL (null unless email_delivery = true)."
  value       = try(aws_sqs_queue.ses_events_routing_dlq[0].id, null)
}

output "ses_configuration_set_name" {
  description = "SES_CONFIGURATION_SET_NAME (null unless email_delivery = true)"
  value       = try(aws_sesv2_configuration_set.notifications[0].configuration_set_name, null)
}

output "redis_url" {
  description = "Cloud Redis endpoint placeholder only. Mark or split sensitive values before real credentials are introduced."
  value       = var.redis_url
}

output "influxdb_url" {
  description = "Cloud InfluxDB endpoint placeholder only. Mark or split sensitive values before real credentials are introduced."
  value       = var.influxdb_url
}
