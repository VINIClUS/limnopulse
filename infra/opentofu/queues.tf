# Core dispatch queue: unconditional. Every delivery channel enqueues onto
# this queue regardless of which channel-specific resources below are
# turned on.
resource "aws_sqs_queue" "notification_jobs_dlq" {
  name                      = var.notification_jobs_dlq_name
  message_retention_seconds = 1209600
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue" "notification_jobs" {
  name                       = var.notification_jobs_queue_name
  visibility_timeout_seconds = 60
  message_retention_seconds  = 345600
  receive_wait_time_seconds  = 20
  sqs_managed_sse_enabled    = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.notification_jobs_dlq.arn
    maxReceiveCount     = 8
  })
}

resource "aws_sqs_queue_redrive_allow_policy" "notification_jobs_dlq" {
  queue_url = aws_sqs_queue.notification_jobs_dlq.id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.notification_jobs.arn]
  })
}

# Telegram outbound delivery path (Fase 2 Proxmox worker) — var.telegram_delivery.
resource "aws_sqs_queue" "telegram_notification_jobs_dlq" {
  count = var.telegram_delivery ? 1 : 0

  name                      = var.telegram_notification_jobs_dlq_name
  message_retention_seconds = 1209600
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue" "telegram_notification_jobs" {
  count = var.telegram_delivery ? 1 : 0

  name                       = var.telegram_notification_jobs_queue_name
  visibility_timeout_seconds = 60
  message_retention_seconds  = 345600
  receive_wait_time_seconds  = 20
  sqs_managed_sse_enabled    = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.telegram_notification_jobs_dlq[0].arn
    maxReceiveCount     = 8
  })
}

resource "aws_sqs_queue_redrive_allow_policy" "telegram_notification_jobs_dlq" {
  count = var.telegram_delivery ? 1 : 0

  queue_url = aws_sqs_queue.telegram_notification_jobs_dlq[0].id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.telegram_notification_jobs[0].arn]
  })
}

# SES feedback routing — var.email_delivery.
resource "aws_sqs_queue" "ses_events_dlq" {
  count = var.email_delivery ? 1 : 0

  name                      = var.ses_events_dlq_name
  message_retention_seconds = 1209600
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue" "ses_events" {
  count = var.email_delivery ? 1 : 0

  name                       = var.ses_events_queue_name
  visibility_timeout_seconds = 60
  message_retention_seconds  = 345600
  receive_wait_time_seconds  = 20
  sqs_managed_sse_enabled    = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.ses_events_dlq[0].arn
    maxReceiveCount     = 8
  })
}

resource "aws_sqs_queue_redrive_allow_policy" "ses_events_dlq" {
  count = var.email_delivery ? 1 : 0

  queue_url = aws_sqs_queue.ses_events_dlq[0].id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.ses_events[0].arn]
  })
}

resource "aws_sqs_queue" "ses_events_routing_dlq" {
  count = var.email_delivery ? 1 : 0

  name                      = var.ses_events_routing_dlq_name
  message_retention_seconds = 1209600
  sqs_managed_sse_enabled   = true
}

data "aws_iam_policy_document" "ses_events" {
  count = var.email_delivery ? 1 : 0

  statement {
    sid     = "AllowEventBridgeSESFeedback"
    effect  = "Allow"
    actions = ["sqs:SendMessage"]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }

    resources = [aws_sqs_queue.ses_events[0].arn]

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values = [
        aws_cloudwatch_event_rule.ses_notifications[0].arn,
        aws_cloudwatch_event_rule.ses_notifications_bounce[0].arn,
        aws_cloudwatch_event_rule.ses_notifications_reject[0].arn,
      ]
    }
  }
}

resource "aws_sqs_queue_policy" "ses_events" {
  count = var.email_delivery ? 1 : 0

  queue_url = aws_sqs_queue.ses_events[0].id
  policy    = data.aws_iam_policy_document.ses_events[0].json
}

data "aws_iam_policy_document" "ses_events_routing_dlq" {
  count = var.email_delivery ? 1 : 0

  statement {
    sid     = "AllowEventBridgeRoutingFailures"
    effect  = "Allow"
    actions = ["sqs:SendMessage"]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }

    resources = [aws_sqs_queue.ses_events_routing_dlq[0].arn]

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values = [
        aws_cloudwatch_event_rule.ses_notifications[0].arn,
        aws_cloudwatch_event_rule.ses_notifications_bounce[0].arn,
        aws_cloudwatch_event_rule.ses_notifications_reject[0].arn,
      ]
    }
  }
}

resource "aws_sqs_queue_policy" "ses_events_routing_dlq" {
  count = var.email_delivery ? 1 : 0

  queue_url = aws_sqs_queue.ses_events_routing_dlq[0].id
  policy    = data.aws_iam_policy_document.ses_events_routing_dlq[0].json
}
