# P3 telemetry ingress (HTTPS -> SQS -> normalizer worker). Every resource and
# policy here is gated on var.telemetry_ingress (default false), so the flag-off
# plan is identical to the pre-P3 one. iam_runtime.tf is deliberately not
# edited: its users are referenced by name and the P3 grants are attached here.

resource "aws_sqs_queue" "telemetry_ingress_dlq" {
  count = var.telemetry_ingress ? 1 : 0

  name                      = var.telemetry_ingress_dlq_name
  message_retention_seconds = 1209600
  sqs_managed_sse_enabled   = true
  max_message_size          = 262144
}

# The DLQ (14 days) outlives the source (4 days): SQS counts DLQ expiry from
# the original enqueue time.
resource "aws_sqs_queue" "telemetry_ingress" {
  count = var.telemetry_ingress ? 1 : 0

  name                       = var.telemetry_ingress_queue_name
  visibility_timeout_seconds = 60
  message_retention_seconds  = 345600
  receive_wait_time_seconds  = 20
  sqs_managed_sse_enabled    = true
  max_message_size           = 262144

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.telemetry_ingress_dlq[0].arn
    maxReceiveCount     = 8
  })
}

# Cloud-only: ElasticMQ rejects the RedriveAllowPolicy attribute.
resource "aws_sqs_queue_redrive_allow_policy" "telemetry_ingress_dlq" {
  count = var.telemetry_ingress ? 1 : 0

  queue_url = aws_sqs_queue.telemetry_ingress_dlq[0].id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.telemetry_ingress[0].arn]
  })
}

data "aws_iam_policy_document" "telemetry_ingress_transport" {
  count = var.telemetry_ingress ? 1 : 0

  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["sqs:*"]
    resources = [aws_sqs_queue.telemetry_ingress[0].arn]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_sqs_queue_policy" "telemetry_ingress" {
  count = var.telemetry_ingress ? 1 : 0

  queue_url = aws_sqs_queue.telemetry_ingress[0].id
  policy    = data.aws_iam_policy_document.telemetry_ingress_transport[0].json
}

data "aws_iam_policy_document" "telemetry_ingress_dlq_transport" {
  count = var.telemetry_ingress ? 1 : 0

  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["sqs:*"]
    resources = [aws_sqs_queue.telemetry_ingress_dlq[0].arn]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_sqs_queue_policy" "telemetry_ingress_dlq" {
  count = var.telemetry_ingress ? 1 : 0

  queue_url = aws_sqs_queue.telemetry_ingress_dlq[0].id
  policy    = data.aws_iam_policy_document.telemetry_ingress_dlq_transport[0].json
}

# API principal (iam_runtime.tf's aws_iam_user.api): send-only on ingress.
# TransactWriteItems sub-operations are authorized per item action, so the
# credential and lifecycle transactions need explicit DeleteItem and
# ConditionCheckItem on the domain table, and PutItem on the audit table for
# the D23 audit items (iam_runtime.tf grants only TransactWriteItems there).
data "aws_iam_policy_document" "telemetry_ingress_api" {
  count = var.telemetry_ingress ? 1 : 0

  statement {
    sid       = "TelemetryIngressSend"
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.telemetry_ingress[0].arn]
  }

  statement {
    sid    = "DomainTransactionItemActions"
    effect = "Allow"
    actions = [
      "dynamodb:ConditionCheckItem",
      "dynamodb:DeleteItem",
    ]
    resources = [aws_dynamodb_table.domain.arn]
  }

  statement {
    sid       = "AuditTransactionPut"
    effect    = "Allow"
    actions   = ["dynamodb:PutItem"]
    resources = [aws_dynamodb_table.audit.arn]
  }
}

resource "aws_iam_policy" "telemetry_ingress_api" {
  count = var.telemetry_ingress ? 1 : 0

  name        = "${var.project_name}-${var.environment}-telemetry-ingress-api"
  description = "API: send to the telemetry ingress queue; per-item transaction actions."
  policy      = data.aws_iam_policy_document.telemetry_ingress_api[0].json

  tags = local.common_tags
}

resource "aws_iam_user_policy_attachment" "api_telemetry_ingress" {
  count = var.telemetry_ingress ? 1 : 0

  user       = aws_iam_user.api.name
  policy_arn = aws_iam_policy.telemetry_ingress_api[0].arn
}

# Dedicated normalizer-worker principal. The shared workers user (Go
# notification workers) is not widened. Access keys are created out of band,
# exactly like the users in iam_runtime.tf.
resource "aws_iam_user" "telemetry_ingest" {
  count = var.telemetry_ingress ? 1 : 0

  name          = "${var.project_name}-${var.environment}-telemetry-ingest"
  force_destroy = true

  tags = local.common_tags
}

data "aws_iam_policy_document" "telemetry_ingest_worker" {
  count = var.telemetry_ingress ? 1 : 0

  statement {
    sid    = "TelemetryIngressConsume"
    effect = "Allow"
    actions = [
      "sqs:ChangeMessageVisibility",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:ReceiveMessage",
    ]
    resources = [aws_sqs_queue.telemetry_ingress[0].arn]
  }

  statement {
    sid    = "DomainTableAccess"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:Query",
      "dynamodb:UpdateItem",
    ]
    resources = [aws_dynamodb_table.domain.arn]
  }
}

resource "aws_iam_policy" "telemetry_ingest_worker" {
  count = var.telemetry_ingress ? 1 : 0

  name        = "${var.project_name}-${var.environment}-telemetry-ingest-worker"
  description = "Telemetry ingest worker: consume the ingress queue; domain table read/write."
  policy      = data.aws_iam_policy_document.telemetry_ingest_worker[0].json

  tags = local.common_tags
}

resource "aws_iam_user_policy_attachment" "telemetry_ingest_worker" {
  count = var.telemetry_ingress ? 1 : 0

  user       = aws_iam_user.telemetry_ingest[0].name
  policy_arn = aws_iam_policy.telemetry_ingest_worker[0].arn
}

# Operator reconciliation (scripts/admin/telemetry_dlq.py and
# telemetry_quarantine.py). Deliberately NOT attached: an operator attaches it
# to their own identity for the duration of a runbook step.
data "aws_iam_policy_document" "telemetry_ingress_operator" {
  count = var.telemetry_ingress ? 1 : 0

  statement {
    sid    = "TelemetryIngressDlqDrain"
    effect = "Allow"
    actions = [
      "sqs:ChangeMessageVisibility",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:ReceiveMessage",
    ]
    resources = [aws_sqs_queue.telemetry_ingress_dlq[0].arn]
  }

  statement {
    sid       = "TelemetryIngressRedrive"
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.telemetry_ingress[0].arn]
  }

  statement {
    sid    = "QuarantineReconciliation"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:Query",
      "dynamodb:UpdateItem",
    ]
    resources = [aws_dynamodb_table.domain.arn]
  }
}

resource "aws_iam_policy" "telemetry_ingress_operator" {
  count = var.telemetry_ingress ? 1 : 0

  name        = "${var.project_name}-${var.environment}-telemetry-ingress-operator"
  description = "Operator DLQ redrive and quarantine reconciliation. Not attached to any principal."
  policy      = data.aws_iam_policy_document.telemetry_ingress_operator[0].json

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "telemetry_ingress_age" {
  count = var.telemetry_ingress ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-telemetry-ingress-age"
  alarm_description   = "Oldest telemetry ingress message is older than the threshold; is the worker running?"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateAgeOfOldestMessage"
  dimensions          = { QueueName = aws_sqs_queue.telemetry_ingress[0].name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 5
  comparison_operator = "GreaterThanThreshold"
  threshold           = var.telemetry_ingress_age_alarm_threshold_seconds
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.telemetry_ingress_alarm_actions

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "telemetry_ingress_dlq" {
  count = var.telemetry_ingress ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-telemetry-ingress-dlq"
  alarm_description   = "Telemetry ingress DLQ is not empty; follow the DLQ runbook."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.telemetry_ingress_dlq[0].name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.telemetry_ingress_alarm_actions

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "telemetry_ingress_depth" {
  count = var.telemetry_ingress ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-telemetry-ingress-depth"
  alarm_description   = "Telemetry ingress backlog is above the threshold."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.telemetry_ingress[0].name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 5
  comparison_operator = "GreaterThanThreshold"
  threshold           = var.telemetry_ingress_depth_alarm_threshold
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.telemetry_ingress_alarm_actions

  tags = local.common_tags
}
