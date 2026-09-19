# Unconditional: the API's inbound webhook route and its APP_ENV=prod
# startup validator both require this secret regardless of whether outbound
# Telegram delivery (var.telegram_delivery) is turned on. See variables.tf.
resource "aws_secretsmanager_secret" "telegram_webhook_secret" {
  name                    = var.telegram_webhook_secret_name
  recovery_window_in_days = 7

  tags = local.common_tags
}

data "aws_iam_policy_document" "telegram_webhook_secret_reader" {
  statement {
    sid       = "TelegramWebhookSecret"
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.telegram_webhook_secret.arn]
  }
}

resource "aws_iam_policy" "telegram_webhook_secret_reader" {
  name        = "${var.project_name}-${var.environment}-telegram-webhook-secret-reader"
  description = "Read-only access to the Telegram webhook authentication secret."
  policy      = data.aws_iam_policy_document.telegram_webhook_secret_reader.json

  tags = local.common_tags
}

# Everything below is the outbound delivery path (Fase 2 Proxmox worker) and
# is gated by var.telegram_delivery.
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
