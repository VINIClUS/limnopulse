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

output "alerts_queue_url" {
  description = "SQS alert queue URL for future workers."
  value       = aws_sqs_queue.alerts.id
}

output "alerts_queue_arn" {
  description = "SQS alert queue ARN for future workers."
  value       = aws_sqs_queue.alerts.arn
}

output "alerts_dlq_url" {
  description = "SQS alert dead-letter queue URL."
  value       = aws_sqs_queue.alerts_dlq.id
}

output "ses_email_identity_arn" {
  description = "Optional SES identity ARN when ses_email_identity is configured."
  value       = var.ses_email_identity == "" ? "" : aws_ses_email_identity.notifications[0].arn
}

output "redis_url" {
  description = "Cloud Redis endpoint placeholder only. Mark or split sensitive values before real credentials are introduced."
  value       = var.redis_url
}

output "influxdb_url" {
  description = "Cloud InfluxDB endpoint placeholder only. Mark or split sensitive values before real credentials are introduced."
  value       = var.influxdb_url
}
