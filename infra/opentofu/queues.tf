resource "aws_sqs_queue" "alerts_dlq" {
  name                      = var.alerts_dlq_name
  message_retention_seconds = 1209600
}

resource "aws_sqs_queue" "alerts" {
  name                       = var.alerts_queue_name
  visibility_timeout_seconds = 60
  message_retention_seconds  = 345600

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.alerts_dlq.arn
    maxReceiveCount     = 5
  })
}

resource "aws_sqs_queue_redrive_allow_policy" "alerts_dlq" {
  queue_url = aws_sqs_queue.alerts_dlq.id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.alerts.arn]
  })
}
