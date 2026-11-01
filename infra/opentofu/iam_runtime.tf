# Least-privilege runtime credentials for the two processes that talk to
# AWS directly: the FastAPI API (src/limnopulse_api) and the Go workers
# (cmd/notifications, cmd/alert-evaluator). Both are aws_iam_user, not
# roles — see the design spec §15/§24 note: this repo's runtime is a VPS
# container and a Proxmox LXC, neither of which can assume an IAM role
# without either instance-profile-style metadata (VPS/LXC has none) or IAM
# Roles Anywhere (deferred to Fase 3, needs a CA and cert renewal this repo
# doesn't have yet). Long-lived access keys are the accepted interim
# tradeoff.
#
# Deliberately NOT managed here: aws_iam_access_key. Generating one in
# OpenTofu writes the secret access key into tfstate in plaintext; with no
# S3 backend/state encryption configured yet (see backend.example.hcl),
# that's a real credential sitting in a file on disk. Access keys are
# created out of band with `aws iam create-access-key` after apply and
# pasted directly into the VPS/.env and the LXC's env — same pattern
# already used for the Telegram bot token and webhook secrets in
# telegram.tf ("populated out of band").

# force_destroy = true: the runbook above creates access keys out of band,
# which AWS otherwise refuses to delete the user underneath (a plain
# `tofu destroy`, or any replacement from changing project_name/
# environment, would fail with keys still attached).
resource "aws_iam_user" "api" {
  name          = "${var.project_name}-${var.environment}-api"
  force_destroy = true

  tags = local.common_tags
}

resource "aws_iam_user" "workers" {
  name          = "${var.project_name}-${var.environment}-workers"
  force_destroy = true

  tags = local.common_tags
}

# DynamoDB authorizes every item of a TransactWriteItems call on its own:
# a Put item needs dynamodb:PutItem, an Update dynamodb:UpdateItem, a
# Delete dynamodb:DeleteItem and a ConditionCheck
# dynamodb:ConditionCheckItem, each on the ARN of the table the item names.
# IAM has no dynamodb:TransactWriteItems action, so granting it authorizes
# nothing. tests/unit/test_iam_runtime_transactions.py derives every
# transaction item and standalone write from the API and worker sources and
# checks it against the grants below.
#
# API (src/limnopulse_api/adapters/*.py): GetItem, PutItem, Query and
# UpdateItem on the domain table, plus the ConditionCheckItem and
# DeleteItem items its transactions carry (ending a deployment and revoking
# an ingest credential each delete a row inside their transaction).
# ingestion_queues.tf grants the API the same item actions again behind its
# feature flag; these base grants no longer depend on that flag.
#
# The audit table is API-only — grep confirms every Go binary
# (cmd/notifications, cmd/alert-evaluator, internal/notifications/**) reads
# DYNAMODB_DOMAIN_TABLE and never DYNAMODB_AUDIT_TABLE, so the workers
# policy (below) omits it entirely. The API never reads, updates or deletes
# an audit item: every audit write is a conditioned Put item inside the
# transaction of the change it records (adapters/alert_rules.py,
# alert_events.py, notification_preferences.py,
# dynamodb_ingest_credentials.py, dynamodb_integrations.py), so its
# audit-table grant is PutItem alone. The policy description below still
# says "transactional-write-only" because changing an aws_iam_policy
# description forces the policy to be replaced.
data "aws_iam_policy_document" "dynamodb_domain_access" {
  statement {
    sid    = "DomainTableAccess"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:Query",
      "dynamodb:UpdateItem",
    ]
    resources = [
      aws_dynamodb_table.domain.arn,
      "${aws_dynamodb_table.domain.arn}/index/*",
    ]
  }

  # Item actions are authorized against the table ARN, never an index, so
  # this statement names the table alone. The item actions the statement
  # above also lists on index/* predate issue #108 and authorize nothing.
  statement {
    sid    = "DomainTableTransactionItems"
    effect = "Allow"
    actions = [
      "dynamodb:ConditionCheckItem",
      "dynamodb:DeleteItem",
    ]
    resources = [aws_dynamodb_table.domain.arn]
  }

  statement {
    sid       = "AuditTablePutOnly"
    effect    = "Allow"
    actions   = ["dynamodb:PutItem"]
    resources = [aws_dynamodb_table.audit.arn]
  }
}

resource "aws_iam_policy" "dynamodb_domain_access" {
  name        = "${var.project_name}-${var.environment}-dynamodb-domain-access"
  description = "Read/write access to the domain table; transactional-write-only access to the audit table."
  policy      = data.aws_iam_policy_document.dynamodb_domain_access.json

  tags = local.common_tags
}

resource "aws_iam_user_policy_attachment" "api_dynamodb" {
  user       = aws_iam_user.api.name
  policy_arn = aws_iam_policy.dynamodb_domain_access.arn
}

data "aws_iam_policy_document" "dynamodb_domain_access_workers" {
  statement {
    sid    = "DomainTableAccess"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:Query",
      "dynamodb:UpdateItem",
    ]
    resources = [
      aws_dynamodb_table.domain.arn,
      "${aws_dynamodb_table.domain.arn}/index/*",
    ]
  }

  # The Go workers never call PutItem directly, but their transactions carry
  # Put and ConditionCheck items (alert-evaluator commits, notification
  # attempts and feedback, relay recovery fences), and each item needs its
  # own action on the table. No DeleteItem: no worker deletes an item.
  statement {
    sid    = "DomainTableTransactionItems"
    effect = "Allow"
    actions = [
      "dynamodb:ConditionCheckItem",
      "dynamodb:PutItem",
    ]
    resources = [aws_dynamodb_table.domain.arn]
  }
}

resource "aws_iam_policy" "dynamodb_domain_access_workers" {
  name        = "${var.project_name}-${var.environment}-dynamodb-domain-access-workers"
  description = "Read/write access to the domain DynamoDB table only - no audit table."
  policy      = data.aws_iam_policy_document.dynamodb_domain_access_workers.json

  tags = local.common_tags
}

resource "aws_iam_user_policy_attachment" "workers_dynamodb" {
  user       = aws_iam_user.workers.name
  policy_arn = aws_iam_policy.dynamodb_domain_access_workers.arn
}

# No IAM policy for cognito-idp:GetUser: it's one of Cognito's
# unauthenticated-API operations (services/cognito_identity.py calls it
# with AccessToken=token, not admin credentials) — authorized entirely by
# the caller's own Cognito access token, and doesn't evaluate the calling
# IAM identity's policies at all. A policy granting it would be a no-op.

# Workers both produce onto and consume from the core notification-jobs
# queue: notification-relay sends (internal/notifications/relay/sqs),
# notification-worker receives/deletes/extends visibility
# (internal/notifications/worker/sqs). Unconditional — this queue always
# exists (queues.tf).
data "aws_iam_policy_document" "notification_jobs_access" {
  statement {
    sid    = "NotificationJobsQueueAccess"
    effect = "Allow"
    actions = [
      "sqs:ChangeMessageVisibility",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:ReceiveMessage",
      "sqs:SendMessage",
    ]
    resources = [aws_sqs_queue.notification_jobs.arn]
  }
}

resource "aws_iam_policy" "notification_jobs_access" {
  name        = "${var.project_name}-${var.environment}-notification-jobs-access"
  description = "Send/receive access to the core notification-jobs SQS queue."
  policy      = data.aws_iam_policy_document.notification_jobs_access.json

  tags = local.common_tags
}

resource "aws_iam_user_policy_attachment" "workers_notification_jobs" {
  user       = aws_iam_user.workers.name
  policy_arn = aws_iam_policy.notification_jobs_access.arn
}

# Attach the already-conditional policies from telegram.tf. count here
# mirrors their own gating exactly, so toggling var.telegram_webhook /
# var.telegram_delivery off removes the attachment in the same apply that
# removes the underlying policy — no separate detach step, and no
# order-of-deletion issue (see the PR #47 discussion on telegram.tf: no
# attachment resource existed anywhere before this file).
resource "aws_iam_user_policy_attachment" "api_telegram_webhook_secret" {
  count = var.telegram_webhook ? 1 : 0

  user       = aws_iam_user.api.name
  policy_arn = aws_iam_policy.telegram_webhook_secret_reader[0].arn
}

resource "aws_iam_user_policy_attachment" "workers_telegram" {
  count = var.telegram_delivery ? 1 : 0

  user       = aws_iam_user.workers.name
  policy_arn = aws_iam_policy.telegram_worker[0].arn
}

resource "aws_iam_user_policy_attachment" "workers_email" {
  count = var.email_delivery ? 1 : 0

  user       = aws_iam_user.workers.name
  policy_arn = aws_iam_policy.email_worker[0].arn
}
