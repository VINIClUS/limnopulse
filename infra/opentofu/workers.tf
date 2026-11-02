# ADR-020: one IAM principal per worker role (ops/workers/roles.yaml `iam_principal`). Each
# policy grants exactly the actions its role's Go import closure (cmd/<role>) calls, less a
# file the role links but provably never runs, on the queues, table, indexes, secret and SES
# resources that role uses.
# tests/unit/test_iam_runtime_transactions.py derives those actions from the Go sources and
# fails on any grant that is missing, extra or scoped to the wrong resource. No policy here
# is shared between two principals, and none of the older shared policies (ses.tf
# email_worker, telegram.tf telegram_worker, iam_runtime.tf) is attached to them.
#
# Access keys follow iam_runtime.tf: OpenTofu never creates one, so no secret reaches the
# state. An operator creates a role's key out of band just before enabling its unit and
# deletes it as the last step of disabling it (README.md, "Worker roles").
#
# Users are gated like the resources they use: the email and SES-feedback principals with
# var.email_delivery, the Telegram principal with var.telegram_delivery. The other three
# users and the admin role are unconditional because their queue and table always exist.
# The <role>_enabled variables gate only alarm actions (worker_alarms.tf), never a
# principal: a unit must run with its key before its first datapoint, and the enable
# variable comes last.

data "aws_caller_identity" "workers" {}

locals {
  # The domain of var.ses_from_address, for the email-worker's SES identity grant. The
  # email principal's policy refuses to plan without a bare mailbox, so the empty default
  # never reaches an ARN.
  workers_ses_from_domain = try(split("@", var.ses_from_address)[1], "")

  # The principals the limnopulse-admin trust allows: the operators' list when set, else
  # every IAM principal of this account, which the root trust already allows.
  limnopulse_admin_trusted_principal_arns = (
    length(var.limnopulse_admin_trusted_principal_arns) > 0
    ? var.limnopulse_admin_trusted_principal_arns
    : ["arn:aws:iam::${data.aws_caller_identity.workers.account_id}:*"]
  )
}

# --- alert-evaluator: rules -> outbox ------------------------------------------------------

resource "aws_iam_user" "alert_evaluator" {
  name          = "${var.project_name}-${var.environment}-alert-evaluator"
  force_destroy = true

  tags = local.common_tags
}

data "aws_iam_policy_document" "alert_evaluator_runtime" {
  # GetItem and UpdateItem standalone; PutItem and UpdateItem as transaction items.
  statement {
    sid    = "DomainTableItems"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
    ]
    resources = [aws_dynamodb_table.domain.arn]
  }

  # The due-rule scan reads the AlertEvaluationByDue index, never the table itself. The
  # closure links the schedule backfills (alertevaluator/dynamo/backfill.go), which query the
  # table, but only limnopulse-admin runs them.
  statement {
    sid       = "DomainIndexQuery"
    effect    = "Allow"
    actions   = ["dynamodb:Query"]
    resources = ["${aws_dynamodb_table.domain.arn}/index/AlertEvaluationByDue"]
  }
}

resource "aws_iam_policy" "alert_evaluator_runtime" {
  name        = "${var.project_name}-${var.environment}-alert-evaluator-runtime"
  description = "ADR-020 alert-evaluator: domain table items and the due-rule index."
  policy      = data.aws_iam_policy_document.alert_evaluator_runtime.json

  tags = local.common_tags
}

resource "aws_iam_user_policy_attachment" "alert_evaluator_runtime" {
  user       = aws_iam_user.alert_evaluator.name
  policy_arn = aws_iam_policy.alert_evaluator_runtime.arn
}

# --- notification-relay: outbox -> channel SQS lanes ---------------------------------------

resource "aws_iam_user" "notification_relay" {
  name          = "${var.project_name}-${var.environment}-notification-relay"
  force_destroy = true

  tags = local.common_tags
}

data "aws_iam_policy_document" "notification_relay_runtime" {
  statement {
    sid    = "DomainTableItems"
    effect = "Allow"
    actions = [
      "dynamodb:ConditionCheckItem",
      "dynamodb:GetItem",
      "dynamodb:UpdateItem",
    ]
    resources = [aws_dynamodb_table.domain.arn]
  }

  # Discovery reads the NotificationRelayByAvailableAt index; fan-out and recovery query
  # the table.
  statement {
    sid     = "DomainTableQuery"
    effect  = "Allow"
    actions = ["dynamodb:Query"]
    resources = [
      aws_dynamodb_table.domain.arn,
      "${aws_dynamodb_table.domain.arn}/index/NotificationRelayByAvailableAt",
    ]
  }

  statement {
    sid       = "EmailJobsProducer"
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.notification_jobs.arn]
  }
}

resource "aws_iam_policy" "notification_relay_runtime" {
  name        = "${var.project_name}-${var.environment}-notification-relay-runtime"
  description = "ADR-020 notification-relay: domain table, relay index, email jobs producer."
  policy      = data.aws_iam_policy_document.notification_relay_runtime.json

  tags = local.common_tags
}

resource "aws_iam_user_policy_attachment" "notification_relay_runtime" {
  user       = aws_iam_user.notification_relay.name
  policy_arn = aws_iam_policy.notification_relay_runtime.arn
}

# The Telegram lane exists only with var.telegram_delivery, so its producer grant does too.
data "aws_iam_policy_document" "notification_relay_telegram" {
  count = var.telegram_delivery ? 1 : 0

  statement {
    sid       = "TelegramJobsProducer"
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.telegram_notification_jobs[0].arn]
  }
}

resource "aws_iam_policy" "notification_relay_telegram" {
  count = var.telegram_delivery ? 1 : 0

  name        = "${var.project_name}-${var.environment}-notification-relay-telegram"
  description = "ADR-020 notification-relay: Telegram jobs producer."
  policy      = data.aws_iam_policy_document.notification_relay_telegram[0].json

  tags = local.common_tags
}

resource "aws_iam_user_policy_attachment" "notification_relay_telegram" {
  count = var.telegram_delivery ? 1 : 0

  user       = aws_iam_user.notification_relay.name
  policy_arn = aws_iam_policy.notification_relay_telegram[0].arn
}

# --- email-worker: SQS email jobs -> SES (SES consumer 1) ----------------------------------

resource "aws_iam_user" "email_worker" {
  count = var.email_delivery ? 1 : 0

  name          = "${var.project_name}-${var.environment}-email-worker"
  force_destroy = true

  tags = local.common_tags
}

data "aws_iam_policy_document" "email_worker_runtime" {
  count = var.email_delivery ? 1 : 0

  statement {
    sid    = "DomainTableItems"
    effect = "Allow"
    actions = [
      "dynamodb:ConditionCheckItem",
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
    ]
    resources = [aws_dynamodb_table.domain.arn]
  }

  statement {
    sid    = "EmailJobsConsumer"
    effect = "Allow"
    actions = [
      "sqs:ChangeMessageVisibility",
      "sqs:DeleteMessage",
      "sqs:ReceiveMessage",
    ]
    resources = [aws_sqs_queue.notification_jobs.arn]
  }

  # SES authorizes SendEmail against the sending identity: the From mailbox when that
  # address is verified, its domain otherwise. Both are granted, and ses:FromAddress still
  # pins the one mailbox. Every send also names the configuration set, which SES then
  # authorizes as well.
  statement {
    sid     = "SesSendEmail"
    effect  = "Allow"
    actions = ["ses:SendEmail"]
    resources = [
      "arn:aws:ses:${var.aws_region}:${data.aws_caller_identity.workers.account_id}:identity/${var.ses_from_address}",
      "arn:aws:ses:${var.aws_region}:${data.aws_caller_identity.workers.account_id}:identity/${local.workers_ses_from_domain}",
      aws_sesv2_configuration_set.notifications[0].arn,
    ]

    condition {
      test     = "StringEquals"
      variable = "ses:FromAddress"
      values   = [var.ses_from_address]
    }
  }
}

resource "aws_iam_policy" "email_worker_runtime" {
  count = var.email_delivery ? 1 : 0

  name        = "${var.project_name}-${var.environment}-email-worker-runtime"
  description = "ADR-020 email-worker: email jobs consumer, SES send, domain table items."
  policy      = data.aws_iam_policy_document.email_worker_runtime[0].json

  tags = local.common_tags

  lifecycle {
    precondition {
      condition     = can(regex("^[^@\\s<>]+@[^@\\s<>]+$", var.ses_from_address))
      error_message = "ses_from_address must be a bare mailbox before email_delivery is true; the email-worker SES grant is scoped to it."
    }
  }
}

resource "aws_iam_user_policy_attachment" "email_worker_runtime" {
  count = var.email_delivery ? 1 : 0

  user       = aws_iam_user.email_worker[0].name
  policy_arn = aws_iam_policy.email_worker_runtime[0].arn
}

# --- ses-feedback-worker: SQS SES events -> delivery state (SES consumer 2) ----------------

resource "aws_iam_user" "ses_feedback_worker" {
  count = var.email_delivery ? 1 : 0

  name          = "${var.project_name}-${var.environment}-ses-feedback-worker"
  force_destroy = true

  tags = local.common_tags
}

data "aws_iam_policy_document" "ses_feedback_worker_runtime" {
  count = var.email_delivery ? 1 : 0

  # GetItem standalone; every write is a transaction item.
  statement {
    sid    = "DomainTableItems"
    effect = "Allow"
    actions = [
      "dynamodb:ConditionCheckItem",
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
    ]
    resources = [aws_dynamodb_table.domain.arn]
  }

  statement {
    sid    = "SesEventsConsumer"
    effect = "Allow"
    actions = [
      "sqs:ChangeMessageVisibility",
      "sqs:DeleteMessage",
      "sqs:ReceiveMessage",
    ]
    resources = [aws_sqs_queue.ses_events[0].arn]
  }
}

resource "aws_iam_policy" "ses_feedback_worker_runtime" {
  count = var.email_delivery ? 1 : 0

  name        = "${var.project_name}-${var.environment}-ses-feedback-worker-runtime"
  description = "ADR-020 ses-feedback-worker: SES events consumer, domain table items."
  policy      = data.aws_iam_policy_document.ses_feedback_worker_runtime[0].json

  tags = local.common_tags
}

resource "aws_iam_user_policy_attachment" "ses_feedback_worker_runtime" {
  count = var.email_delivery ? 1 : 0

  user       = aws_iam_user.ses_feedback_worker[0].name
  policy_arn = aws_iam_policy.ses_feedback_worker_runtime[0].arn
}

# --- telegram-worker: SQS Telegram jobs -> Bot API -----------------------------------------

resource "aws_iam_user" "telegram_worker" {
  count = var.telegram_delivery ? 1 : 0

  name          = "${var.project_name}-${var.environment}-telegram-worker"
  force_destroy = true

  tags = local.common_tags
}

data "aws_iam_policy_document" "telegram_worker_runtime" {
  count = var.telegram_delivery ? 1 : 0

  statement {
    sid    = "DomainTableItems"
    effect = "Allow"
    actions = [
      "dynamodb:ConditionCheckItem",
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
    ]
    resources = [aws_dynamodb_table.domain.arn]
  }

  statement {
    sid    = "TelegramJobsConsumer"
    effect = "Allow"
    actions = [
      "sqs:ChangeMessageVisibility",
      "sqs:DeleteMessage",
      "sqs:ReceiveMessage",
    ]
    resources = [aws_sqs_queue.telegram_notification_jobs[0].arn]
  }

  statement {
    sid       = "TelegramBotToken"
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.telegram_bot_token[0].arn]
  }
}

resource "aws_iam_policy" "telegram_worker_runtime" {
  count = var.telegram_delivery ? 1 : 0

  name        = "${var.project_name}-${var.environment}-telegram-worker-runtime"
  description = "ADR-020 telegram-worker: Telegram jobs consumer, bot token, domain table items."
  policy      = data.aws_iam_policy_document.telegram_worker_runtime[0].json

  tags = local.common_tags
}

resource "aws_iam_user_policy_attachment" "telegram_worker_runtime" {
  count = var.telegram_delivery ? 1 : 0

  user       = aws_iam_user.telegram_worker[0].name
  policy_arn = aws_iam_policy.telegram_worker_runtime[0].arn
}

# --- worker-monitor: unit health -> CloudWatch ---------------------------------------------

resource "aws_iam_user" "worker_monitor" {
  name          = "${var.project_name}-${var.environment}-worker-monitor"
  force_destroy = true

  tags = local.common_tags
}

# PutMetricData has no resource type, so the namespace condition is its only scope.
data "aws_iam_policy_document" "worker_monitor_runtime" {
  statement {
    sid       = "UnitHealthMetrics"
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["Limnopulse/Workers"]
    }
  }
}

resource "aws_iam_policy" "worker_monitor_runtime" {
  name        = "${var.project_name}-${var.environment}-worker-monitor-runtime"
  description = "ADR-020 worker-monitor: PutMetricData to Limnopulse/Workers only."
  policy      = data.aws_iam_policy_document.worker_monitor_runtime.json

  tags = local.common_tags
}

resource "aws_iam_user_policy_attachment" "worker_monitor_runtime" {
  user       = aws_iam_user.worker_monitor.name
  policy_arn = aws_iam_policy.worker_monitor_runtime.arn
}

# --- limnopulse-admin: operator backfills --------------------------------------------------

# A role, not a user. The backfills run by hand, rarely, and no enable variable would ever
# retire a standing key, so the operator assumes this role for one run instead. The binary
# then holds exactly these grants and only short-lived credentials. The trust names the
# account itself, so only an identity in this account that is granted sts:AssumeRole on the
# role can assume it. aws:PrincipalArn narrows that to var.limnopulse_admin_trusted_principal_arns
# once the operators set it.
data "aws_iam_policy_document" "limnopulse_admin_trust" {
  statement {
    sid     = "AccountOperators"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.workers.account_id}:root"]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:PrincipalArn"
      values   = local.limnopulse_admin_trusted_principal_arns
    }
  }
}

resource "aws_iam_role" "limnopulse_admin" {
  name                 = "${var.project_name}-${var.environment}-limnopulse-admin"
  description          = "ADR-020 limnopulse-admin: assumed by an operator for one backfill run."
  assume_role_policy   = data.aws_iam_policy_document.limnopulse_admin_trust.json
  max_session_duration = 3600

  tags = local.common_tags
}

# The backfills query the table and update items, nothing else. They share a package with
# the evaluator's store (alertevaluator/dynamo/store.go), so the closure links it, but the
# admin binary runs none of the store's reads or writes.
data "aws_iam_policy_document" "limnopulse_admin_runtime" {
  statement {
    sid       = "DomainTableItems"
    effect    = "Allow"
    actions   = ["dynamodb:UpdateItem"]
    resources = [aws_dynamodb_table.domain.arn]
  }

  statement {
    sid       = "DomainTableQuery"
    effect    = "Allow"
    actions   = ["dynamodb:Query"]
    resources = [aws_dynamodb_table.domain.arn]
  }
}

resource "aws_iam_policy" "limnopulse_admin_runtime" {
  name        = "${var.project_name}-${var.environment}-limnopulse-admin-runtime"
  description = "ADR-020 limnopulse-admin: domain table backfills."
  policy      = data.aws_iam_policy_document.limnopulse_admin_runtime.json

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "limnopulse_admin_runtime" {
  role       = aws_iam_role.limnopulse_admin.name
  policy_arn = aws_iam_policy.limnopulse_admin_runtime.arn
}
