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

output "ses_events_queue_url" {
  description = "SQS_SES_EVENTS_URL"
  value       = aws_sqs_queue.ses_events.id
}

output "ses_events_queue_arn" {
  description = "SES feedback queue ARN."
  value       = aws_sqs_queue.ses_events.arn
}

output "ses_events_dlq_url" {
  description = "SES feedback dead-letter queue URL."
  value       = aws_sqs_queue.ses_events_dlq.id
}

output "ses_events_routing_dlq_url" {
  description = "EventBridge SES routing dead-letter queue URL."
  value       = aws_sqs_queue.ses_events_routing_dlq.id
}

output "ses_configuration_set_name" {
  description = "SES_CONFIGURATION_SET_NAME"
  value       = aws_sesv2_configuration_set.notifications.configuration_set_name
}

output "redis_url" {
  description = "Cloud Redis endpoint placeholder only. Mark or split sensitive values before real credentials are introduced."
  value       = var.redis_url
}

output "influxdb_url" {
  description = "Cloud InfluxDB endpoint placeholder only. Mark or split sensitive values before real credentials are introduced."
  value       = var.influxdb_url
}
