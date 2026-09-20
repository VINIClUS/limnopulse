# Gated by var.telegram_webhook (see variables.tf). recovery_window_in_days
# stays at the default 7: a 0-day window still deletes asynchronously in
# Secrets Manager, so it doesn't actually make a rapid disable/re-enable
# apply race-free — it only trades that unresolved race for irreversible
# loss of an out-of-band-populated secret. Toggling this flag off and back
# on inside 7 days therefore requires `tofu import`-ing the still-pending
# secret rather than recreating it; this is a deliberate, rare-operation
# tradeoff, not an oversight.
resource "aws_secretsmanager_secret" "telegram_webhook_secret" {
  count = var.telegram_webhook ? 1 : 0

  name                    = var.telegram_webhook_secret_name
  recovery_window_in_days = 7

  tags = local.common_tags
}

data "aws_iam_policy_document" "telegram_webhook_secret_reader" {
  count = var.telegram_webhook ? 1 : 0

  statement {
    sid       = "TelegramWebhookSecret"
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.telegram_webhook_secret[0].arn]
  }
}

resource "aws_iam_policy" "telegram_webhook_secret_reader" {
  count = var.telegram_webhook ? 1 : 0

  name        = "${var.project_name}-${var.environment}-telegram-webhook-secret-reader"
  description = "Read-only access to the Telegram webhook authentication secret."
  policy      = data.aws_iam_policy_document.telegram_webhook_secret_reader[0].json

  tags = local.common_tags
}

# Everything below is the outbound delivery path (Fase 2 Proxmox worker) and
# is gated by var.telegram_delivery. Same recovery_window_in_days tradeoff
# as telegram_webhook_secret above.
resource "aws_secretsmanager_secret" "telegram_bot_token" {
  count = var.telegram_delivery ? 1 : 0

  name                    = var.telegram_bot_token_secret_name
  recovery_window_in_days = 7

  tags = local.common_tags
}

data "aws_iam_policy_document" "telegram_worker" {
  count = var.telegram_delivery ? 1 : 0

  statement {
    sid    = "TelegramDeliveryState"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:UpdateItem",
      "dynamodb:TransactWriteItems",
    ]
    resources = [aws_dynamodb_table.domain.arn]
  }

  statement {
    sid    = "TelegramJobsConsumer"
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:ChangeMessageVisibility",
      "sqs:GetQueueAttributes",
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

resource "aws_iam_policy" "telegram_worker" {
  count = var.telegram_delivery ? 1 : 0

  name        = "${var.project_name}-${var.environment}-telegram-worker"
  description = "Least-privilege data-plane access for the Telegram notification worker."
  policy      = data.aws_iam_policy_document.telegram_worker[0].json

  tags = local.common_tags
}
