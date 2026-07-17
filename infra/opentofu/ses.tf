data "aws_cloudwatch_event_bus" "default" {
  name = "default"
}

resource "aws_sesv2_configuration_set" "notifications" {
  configuration_set_name = var.ses_configuration_set_name

  reputation_options {
    reputation_metrics_enabled = true
  }

  sending_options {
    sending_enabled = true
  }
}

resource "aws_sesv2_configuration_set_event_destination" "eventbridge" {
  configuration_set_name = aws_sesv2_configuration_set.notifications.configuration_set_name
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
      event_bus_arn = data.aws_cloudwatch_event_bus.default.arn
    }
  }
}

resource "aws_cloudwatch_event_rule" "ses_notifications" {
  name        = var.ses_eventbridge_rule_name
  description = "Route Limnopulse SES delivery feedback to SQS"

  event_pattern = jsonencode({
    source = ["aws.ses"]
    detail = {
      eventType = ["Send", "Delivery", "DeliveryDelay", "Bounce", "Complaint", "Reject"]
    }
  })
}

resource "aws_cloudwatch_event_target" "ses_events" {
  rule      = aws_cloudwatch_event_rule.ses_notifications.name
  target_id = "limnopulse-ses-events"
  arn       = aws_sqs_queue.ses_events.arn

  dead_letter_config {
    arn = aws_sqs_queue.ses_events_routing_dlq.arn
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
