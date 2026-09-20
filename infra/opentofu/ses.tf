# All resources in this file are gated by var.email_delivery (see
# variables.tf): none are wired to any current consumer.
data "aws_cloudwatch_event_bus" "default" {
  count = var.email_delivery ? 1 : 0

  name = "default"
}

resource "aws_sesv2_configuration_set" "notifications" {
  count = var.email_delivery ? 1 : 0

  configuration_set_name = var.ses_configuration_set_name

  reputation_options {
    reputation_metrics_enabled = true
  }

  sending_options {
    sending_enabled = true
  }
}

resource "aws_sesv2_configuration_set_event_destination" "eventbridge" {
  count = var.email_delivery ? 1 : 0

  configuration_set_name = aws_sesv2_configuration_set.notifications[0].configuration_set_name
  event_destination_name = "limnopulse-ses-events"

  event_destination {
    enabled = true
    matching_event_types = [
      "SEND",
      "DELIVERY",
      "DELIVERY_DELAY",
      "BOUNCE",
      "COMPLAINT",
      "REJECT",
    ]

    event_bridge_destination {
      event_bus_arn = data.aws_cloudwatch_event_bus.default[0].arn
    }
  }
}

resource "aws_cloudwatch_event_rule" "ses_notifications" {
  count = var.email_delivery ? 1 : 0

  name        = var.ses_eventbridge_rule_name
  description = "Route Limnopulse SES delivery feedback to SQS"

  event_pattern = jsonencode({
    source = ["aws.ses"]
    detail = {
      eventType = ["Send", "Delivery", "DeliveryDelay", "Complaint"]
      mail = {
        tags = {
          "ses:configuration-set" = [var.ses_configuration_set_name]
        }
      }
    }
  })
}

resource "aws_cloudwatch_event_rule" "ses_notifications_bounce" {
  count = var.email_delivery ? 1 : 0

  name        = "${var.ses_eventbridge_rule_name}-bounce"
  description = "Route sanitized Limnopulse SES bounce feedback to SQS"

  event_pattern = jsonencode({
    source = ["aws.ses"]
    detail = {
      eventType = ["Bounce"]
      mail = {
        tags = {
          "ses:configuration-set" = [var.ses_configuration_set_name]
        }
      }
    }
  })
}

resource "aws_cloudwatch_event_rule" "ses_notifications_reject" {
  count = var.email_delivery ? 1 : 0

  name        = "${var.ses_eventbridge_rule_name}-reject"
  description = "Route sanitized Limnopulse SES reject feedback to SQS"

  event_pattern = jsonencode({
    source = ["aws.ses"]
    detail = {
      eventType = ["Reject"]
      mail = {
        tags = {
          "ses:configuration-set" = [var.ses_configuration_set_name]
        }
      }
    }
  })
}

resource "aws_cloudwatch_event_target" "ses_events" {
  count = var.email_delivery ? 1 : 0

  rule      = aws_cloudwatch_event_rule.ses_notifications[0].name
  target_id = "limnopulse-ses-events"
  arn       = aws_sqs_queue.ses_events[0].arn

  input_transformer {
    input_paths = {
      version     = "$.version"
      id          = "$.id"
      detail_type = "$.detail-type"
      source      = "$.source"
      event_type  = "$.detail.eventType"
      message_id  = "$.detail.mail.messageId"
      delivery_id = "$.detail.mail.tags.delivery_id[0]"
      attempt_id  = "$.detail.mail.tags.attempt_id[0]"
    }
    input_template = <<-JSON
      {"version":<version>,"id":<id>,"detail-type":<detail_type>,"source":<source>,"detail":{"eventType":<event_type>,"mail":{"messageId":<message_id>,"tags":{"delivery_id":[<delivery_id>],"attempt_id":[<attempt_id>]}},"deliveryDelay":{},"complaint":{}}}
    JSON
  }

  dead_letter_config {
    arn = aws_sqs_queue.ses_events_routing_dlq[0].arn
  }

  retry_policy {
    maximum_event_age_in_seconds = 86400
    maximum_retry_attempts       = 185
  }

  depends_on = [
    aws_sqs_queue_policy.ses_events,
    aws_sqs_queue_policy.ses_events_routing_dlq,
  ]
}

resource "aws_cloudwatch_event_target" "ses_events_bounce" {
  count = var.email_delivery ? 1 : 0

  rule      = aws_cloudwatch_event_rule.ses_notifications_bounce[0].name
  target_id = "limnopulse-ses-events-bounce"
  arn       = aws_sqs_queue.ses_events[0].arn

  input_transformer {
    input_paths = {
      version     = "$.version"
      id          = "$.id"
      detail_type = "$.detail-type"
      source      = "$.source"
      event_type  = "$.detail.eventType"
      message_id  = "$.detail.mail.messageId"
      delivery_id = "$.detail.mail.tags.delivery_id[0]"
      attempt_id  = "$.detail.mail.tags.attempt_id[0]"
      bounce_type = "$.detail.bounce.bounceType"
    }
    input_template = <<-JSON
      {"version":<version>,"id":<id>,"detail-type":<detail_type>,"source":<source>,"detail":{"eventType":<event_type>,"mail":{"messageId":<message_id>,"tags":{"delivery_id":[<delivery_id>],"attempt_id":[<attempt_id>]}},"bounce":{"bounceType":<bounce_type>}}}
    JSON
  }

  dead_letter_config {
    arn = aws_sqs_queue.ses_events_routing_dlq[0].arn
  }

  retry_policy {
    maximum_event_age_in_seconds = 86400
    maximum_retry_attempts       = 185
  }

  depends_on = [
    aws_sqs_queue_policy.ses_events,
    aws_sqs_queue_policy.ses_events_routing_dlq,
  ]
}

resource "aws_cloudwatch_event_target" "ses_events_reject" {
  count = var.email_delivery ? 1 : 0

  rule      = aws_cloudwatch_event_rule.ses_notifications_reject[0].name
  target_id = "limnopulse-ses-events-reject"
  arn       = aws_sqs_queue.ses_events[0].arn

  input_transformer {
    input_paths = {
      version       = "$.version"
      id            = "$.id"
      detail_type   = "$.detail-type"
      source        = "$.source"
      event_type    = "$.detail.eventType"
      message_id    = "$.detail.mail.messageId"
      delivery_id   = "$.detail.mail.tags.delivery_id[0]"
      attempt_id    = "$.detail.mail.tags.attempt_id[0]"
      reject_reason = "$.detail.reject.reason"
    }
    input_template = <<-JSON
      {"version":<version>,"id":<id>,"detail-type":<detail_type>,"source":<source>,"detail":{"eventType":<event_type>,"mail":{"messageId":<message_id>,"tags":{"delivery_id":[<delivery_id>],"attempt_id":[<attempt_id>]}},"reject":{"reason":<reject_reason>}}}
    JSON
  }

  dead_letter_config {
    arn = aws_sqs_queue.ses_events_routing_dlq[0].arn
  }

  retry_policy {
    maximum_event_age_in_seconds = 86400
    maximum_retry_attempts       = 185
  }

  depends_on = [
    aws_sqs_queue_policy.ses_events,
    aws_sqs_queue_policy.ses_events_routing_dlq,
  ]
}

# Runtime access for the notification-worker's email path
# (cmd/notifications/worker_command.go, internal/notifications/worker/ses).
# Gated the same as the rest of this file: var.email_delivery.
data "aws_iam_policy_document" "email_worker" {
  count = var.email_delivery ? 1 : 0

  statement {
    sid       = "SesSendEmail"
    effect    = "Allow"
    actions   = ["sesv2:SendEmail"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "ses:configuration-set"
      values   = [var.ses_configuration_set_name]
    }
  }

  statement {
    sid    = "SesEventsQueueConsumer"
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:ChangeMessageVisibility",
      "sqs:GetQueueAttributes",
    ]
    resources = [aws_sqs_queue.ses_events[0].arn]
  }
}

resource "aws_iam_policy" "email_worker" {
  count = var.email_delivery ? 1 : 0

  name        = "${var.project_name}-${var.environment}-email-worker"
  description = "Least-privilege data-plane access for the notification worker's email path."
  policy      = data.aws_iam_policy_document.email_worker[0].json

  tags = local.common_tags
}
