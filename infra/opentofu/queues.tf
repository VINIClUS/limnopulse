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

resource "aws_sqs_queue" "ses_events_dlq" {
  name                      = var.ses_events_dlq_name
  message_retention_seconds = 1209600
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue" "ses_events" {
  name                       = var.ses_events_queue_name
  visibility_timeout_seconds = 60
  message_retention_seconds  = 345600
  receive_wait_time_seconds  = 20
  sqs_managed_sse_enabled    = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.ses_events_dlq.arn
    maxReceiveCount     = 8
  })
}

resource "aws_sqs_queue_redrive_allow_policy" "ses_events_dlq" {
  queue_url = aws_sqs_queue.ses_events_dlq.id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.ses_events.arn]
  })
}

resource "aws_sqs_queue" "ses_events_routing_dlq" {
  name                      = var.ses_events_routing_dlq_name
  message_retention_seconds = 1209600
  sqs_managed_sse_enabled   = true
}

data "aws_iam_policy_document" "ses_events" {
  statement {
    sid     = "AllowEventBridgeSESFeedback"
    effect  = "Allow"
    actions = ["sqs:SendMessage"]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }

    resources = [aws_sqs_queue.ses_events.arn]

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values = [
        aws_cloudwatch_event_rule.ses_notifications.arn,
        aws_cloudwatch_event_rule.ses_notifications_bounce.arn,
        aws_cloudwatch_event_rule.ses_notifications_reject.arn,
      ]
    }
  }
}

resource "aws_sqs_queue_policy" "ses_events" {
  queue_url = aws_sqs_queue.ses_events.id
  policy    = data.aws_iam_policy_document.ses_events.json
}

data "aws_iam_policy_document" "ses_events_routing_dlq" {
  statement {
    sid     = "AllowEventBridgeRoutingFailures"
    effect  = "Allow"
    actions = ["sqs:SendMessage"]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }

    resources = [aws_sqs_queue.ses_events_routing_dlq.arn]

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values = [
        aws_cloudwatch_event_rule.ses_notifications.arn,
        aws_cloudwatch_event_rule.ses_notifications_bounce.arn,
        aws_cloudwatch_event_rule.ses_notifications_reject.arn,
      ]
    }
  }
}

resource "aws_sqs_queue_policy" "ses_events_routing_dlq" {
  queue_url = aws_sqs_queue.ses_events_routing_dlq.id
  policy    = data.aws_iam_policy_document.ses_events_routing_dlq.json
}
