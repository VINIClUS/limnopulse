# ADR-020 worker alarms (ops/workers/roles.yaml `alarms` and `standalone_alarms`), routed to
# their own encrypted topic. A role's alarms exist exactly when its principal does
# (workers.tf), and each has actions_enabled = var.<role>_enabled, so it stays silent while
# its role is disabled. README.md, "Worker roles", gives the enable order.

# Unit health. worker-monitor publishes Limnopulse/Workers UnitHealthy (1 or 0, no unit) by
# Role once a minute for every enabled unit. It publishes 0 for short spells by design (a
# one-shot sampled mid-run, the first interval after boot or enable, a service still
# activating), and timer jitter can leave a minute with no datapoint. So each alarm takes the
# Maximum over two-minute periods and fires on 2 of 3: about four minutes of only zeros or
# silence. Missing data is breaching, so a stopped monitor or a silent host also fires.

resource "aws_cloudwatch_metric_alarm" "alert_evaluator_unit_health" {
  alarm_name          = "${var.project_name}-${var.environment}-alert-evaluator-unit-health"
  alarm_description   = "alert-evaluator: timer inactive, last run failed or stale, or no unit-health data."
  namespace           = "Limnopulse/Workers"
  metric_name         = "UnitHealthy"
  dimensions          = { Role = "alert-evaluator" }
  statistic           = "Maximum"
  period              = 120
  evaluation_periods  = 3
  datapoints_to_alarm = 2
  comparison_operator = "LessThanThreshold"
  threshold           = 1
  treat_missing_data  = "breaching"
  actions_enabled     = var.alert_evaluator_enabled
  alarm_actions       = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])
  ok_actions          = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "notification_relay_unit_health" {
  alarm_name          = "${var.project_name}-${var.environment}-notification-relay-unit-health"
  alarm_description   = "notification-relay: timer inactive, last run failed or stale, or no unit-health data."
  namespace           = "Limnopulse/Workers"
  metric_name         = "UnitHealthy"
  dimensions          = { Role = "notification-relay" }
  statistic           = "Maximum"
  period              = 120
  evaluation_periods  = 3
  datapoints_to_alarm = 2
  comparison_operator = "LessThanThreshold"
  threshold           = 1
  treat_missing_data  = "breaching"
  actions_enabled     = var.notification_relay_enabled
  alarm_actions       = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])
  ok_actions          = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "email_worker_unit_health" {
  count = var.email_delivery ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-email-worker-unit-health"
  alarm_description   = "email-worker: service not running, or no unit-health data."
  namespace           = "Limnopulse/Workers"
  metric_name         = "UnitHealthy"
  dimensions          = { Role = "email-worker" }
  statistic           = "Maximum"
  period              = 120
  evaluation_periods  = 3
  datapoints_to_alarm = 2
  comparison_operator = "LessThanThreshold"
  threshold           = 1
  treat_missing_data  = "breaching"
  actions_enabled     = var.email_worker_enabled
  alarm_actions       = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])
  ok_actions          = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "ses_feedback_worker_unit_health" {
  count = var.email_delivery ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-ses-feedback-worker-unit-health"
  alarm_description   = "ses-feedback-worker: service not running, or no unit-health data."
  namespace           = "Limnopulse/Workers"
  metric_name         = "UnitHealthy"
  dimensions          = { Role = "ses-feedback-worker" }
  statistic           = "Maximum"
  period              = 120
  evaluation_periods  = 3
  datapoints_to_alarm = 2
  comparison_operator = "LessThanThreshold"
  threshold           = 1
  treat_missing_data  = "breaching"
  actions_enabled     = var.ses_feedback_worker_enabled
  alarm_actions       = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])
  ok_actions          = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "telegram_worker_unit_health" {
  count = var.telegram_delivery ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-telegram-worker-unit-health"
  alarm_description   = "telegram-worker: service not running, or no unit-health data."
  namespace           = "Limnopulse/Workers"
  metric_name         = "UnitHealthy"
  dimensions          = { Role = "telegram-worker" }
  statistic           = "Maximum"
  period              = 120
  evaluation_periods  = 3
  datapoints_to_alarm = 2
  comparison_operator = "LessThanThreshold"
  threshold           = 1
  treat_missing_data  = "breaching"
  actions_enabled     = var.telegram_worker_enabled
  alarm_actions       = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])
  ok_actions          = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "worker_monitor_unit_health" {
  alarm_name          = "${var.project_name}-${var.environment}-worker-monitor-unit-health"
  alarm_description   = "worker-monitor: its timer is inactive or its last run failed or is stale, or it publishes nothing (host silent)."
  namespace           = "Limnopulse/Workers"
  metric_name         = "UnitHealthy"
  dimensions          = { Role = "worker-monitor" }
  statistic           = "Maximum"
  period              = 120
  evaluation_periods  = 3
  datapoints_to_alarm = 2
  comparison_operator = "LessThanThreshold"
  threshold           = 1
  treat_missing_data  = "breaching"
  actions_enabled     = var.worker_monitor_enabled
  alarm_actions       = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])
  ok_actions          = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])

  tags = local.common_tags
}

# Queue age, depth and DLQ alarms of each consumer role. An idle queue publishes no SQS
# metrics, so missing data is not breaching here.

resource "aws_cloudwatch_metric_alarm" "email_worker_queue_age" {
  count = var.email_delivery ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-email-worker-queue-age"
  alarm_description   = "Oldest email job is older than the threshold; is email-worker running?"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateAgeOfOldestMessage"
  dimensions          = { QueueName = aws_sqs_queue.notification_jobs.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = var.worker_queue_age_alarm_threshold_seconds
  treat_missing_data  = "notBreaching"
  actions_enabled     = var.email_worker_enabled
  alarm_actions       = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])
  ok_actions          = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "email_worker_queue_depth" {
  count = var.email_delivery ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-email-worker-queue-depth"
  alarm_description   = "Email jobs backlog is above the threshold."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.notification_jobs.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 5
  comparison_operator = "GreaterThanThreshold"
  threshold           = var.worker_queue_depth_alarm_threshold
  treat_missing_data  = "notBreaching"
  actions_enabled     = var.email_worker_enabled
  alarm_actions       = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])
  ok_actions          = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "email_worker_queue_dlq" {
  count = var.email_delivery ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-email-worker-queue-dlq"
  alarm_description   = "Email jobs DLQ is not empty; follow the notification DLQ runbook."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.notification_jobs_dlq.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  actions_enabled     = var.email_worker_enabled
  alarm_actions       = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])
  ok_actions          = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "ses_feedback_worker_queue_age" {
  count = var.email_delivery ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-ses-feedback-worker-queue-age"
  alarm_description   = "Oldest SES event is older than the threshold; is ses-feedback-worker running?"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateAgeOfOldestMessage"
  dimensions          = { QueueName = aws_sqs_queue.ses_events[0].name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = var.worker_queue_age_alarm_threshold_seconds
  treat_missing_data  = "notBreaching"
  actions_enabled     = var.ses_feedback_worker_enabled
  alarm_actions       = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])
  ok_actions          = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "ses_feedback_worker_queue_depth" {
  count = var.email_delivery ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-ses-feedback-worker-queue-depth"
  alarm_description   = "SES events backlog is above the threshold."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.ses_events[0].name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 5
  comparison_operator = "GreaterThanThreshold"
  threshold           = var.worker_queue_depth_alarm_threshold
  treat_missing_data  = "notBreaching"
  actions_enabled     = var.ses_feedback_worker_enabled
  alarm_actions       = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])
  ok_actions          = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "ses_feedback_worker_queue_dlq" {
  count = var.email_delivery ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-ses-feedback-worker-queue-dlq"
  alarm_description   = "SES events DLQ is not empty; follow the notification DLQ runbook."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.ses_events_dlq[0].name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  actions_enabled     = var.ses_feedback_worker_enabled
  alarm_actions       = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])
  ok_actions          = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "telegram_worker_queue_age" {
  count = var.telegram_delivery ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-telegram-worker-queue-age"
  alarm_description   = "Oldest Telegram job is older than the threshold; is telegram-worker running?"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateAgeOfOldestMessage"
  dimensions          = { QueueName = aws_sqs_queue.telegram_notification_jobs[0].name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = var.worker_queue_age_alarm_threshold_seconds
  treat_missing_data  = "notBreaching"
  actions_enabled     = var.telegram_worker_enabled
  alarm_actions       = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])
  ok_actions          = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "telegram_worker_queue_depth" {
  count = var.telegram_delivery ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-telegram-worker-queue-depth"
  alarm_description   = "Telegram jobs backlog is above the threshold."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.telegram_notification_jobs[0].name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 5
  comparison_operator = "GreaterThanThreshold"
  threshold           = var.worker_queue_depth_alarm_threshold
  treat_missing_data  = "notBreaching"
  actions_enabled     = var.telegram_worker_enabled
  alarm_actions       = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])
  ok_actions          = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "telegram_worker_queue_dlq" {
  count = var.telegram_delivery ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-telegram-worker-queue-dlq"
  alarm_description   = "Telegram jobs DLQ is not empty; follow the Telegram DLQ runbook."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.telegram_notification_jobs_dlq[0].name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  actions_enabled     = var.telegram_worker_enabled
  alarm_actions       = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])
  ok_actions          = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])

  tags = local.common_tags
}

# The EventBridge SES routing DLQ (standalone_alarms) belongs to no role: it is gated with the
# routing itself, and its actions are on whenever it exists.
resource "aws_cloudwatch_metric_alarm" "ses_events_routing_dlq" {
  count = var.email_delivery ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-ses-events-routing-dlq"
  alarm_description   = "EventBridge could not deliver SES feedback to the SES events queue; inspect the routing DLQ."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.ses_events_routing_dlq[0].name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  alarm_actions       = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])
  ok_actions          = concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])

  tags = local.common_tags
}

# Alarm routing. CloudWatch cannot publish to a topic encrypted with the AWS-managed aws/sns
# key, so the topic has its own rotated customer-managed key whose policy lets CloudWatch
# encrypt. worker_alarm_email subscribes the on-call mailbox, which stays
# PendingConfirmation until the recipient confirms the email AWS sends.
data "aws_iam_policy_document" "worker_alarms_key" {
  statement {
    sid       = "AccountAdministration"
    effect    = "Allow"
    actions   = ["kms:*"]
    resources = ["*"]

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.workers.account_id}:root"]
    }
  }

  statement {
    sid       = "CloudWatchAlarmPublish"
    effect    = "Allow"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey*"]
    resources = ["*"]

    principals {
      type        = "Service"
      identifiers = ["cloudwatch.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.workers.account_id]
    }
  }
}

resource "aws_kms_key" "worker_alarms" {
  description             = "Encrypts the ADR-020 worker alarm topic."
  enable_key_rotation     = true
  deletion_window_in_days = 30
  policy                  = data.aws_iam_policy_document.worker_alarms_key.json

  tags = local.common_tags
}

resource "aws_sns_topic" "worker_alarms" {
  name              = "${var.project_name}-${var.environment}-worker-alarms"
  kms_master_key_id = aws_kms_key.worker_alarms.arn

  tags = local.common_tags

  # An enable variable whose delivery flag is off would have no alarm to turn on. The
  # checks sit on this unconditional topic because those alarms do not exist then.
  lifecycle {
    precondition {
      condition     = !var.email_worker_enabled || var.email_delivery
      error_message = "email_worker_enabled needs email_delivery: without it the role has no principal and no alarms."
    }
    precondition {
      condition     = !var.ses_feedback_worker_enabled || var.email_delivery
      error_message = "ses_feedback_worker_enabled needs email_delivery: without it the role has no principal and no alarms."
    }
    precondition {
      condition     = !var.telegram_worker_enabled || var.telegram_delivery
      error_message = "telegram_worker_enabled needs telegram_delivery: without it the role has no principal and no alarms."
    }
  }
}

data "aws_iam_policy_document" "worker_alarms_topic" {
  statement {
    sid       = "CloudWatchAlarmPublish"
    effect    = "Allow"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.worker_alarms.arn]

    principals {
      type        = "Service"
      identifiers = ["cloudwatch.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.workers.account_id]
    }
  }
}

resource "aws_sns_topic_policy" "worker_alarms" {
  arn    = aws_sns_topic.worker_alarms.arn
  policy = data.aws_iam_policy_document.worker_alarms_topic.json
}

resource "aws_sns_topic_subscription" "worker_alarms_email" {
  count = var.worker_alarm_email != "" ? 1 : 0

  topic_arn = aws_sns_topic.worker_alarms.arn
  protocol  = "email"
  endpoint  = var.worker_alarm_email
}
