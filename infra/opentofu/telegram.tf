resource "aws_secretsmanager_secret" "telegram_bot_token" {
  name                    = var.telegram_bot_token_secret_name
  recovery_window_in_days = 7

  tags = local.common_tags
}

resource "aws_secretsmanager_secret" "telegram_webhook_secret" {
  name                    = var.telegram_webhook_secret_name
  recovery_window_in_days = 7

  tags = local.common_tags
}

data "aws_iam_policy_document" "telegram_worker" {
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
    resources = [aws_sqs_queue.telegram_notification_jobs.arn]
  }

  statement {
    sid       = "TelegramBotToken"
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.telegram_bot_token.arn]
  }
}

resource "aws_iam_policy" "telegram_worker" {
  name        = "${var.project_name}-${var.environment}-telegram-worker"
  description = "Least-privilege data-plane access for the Telegram notification worker."
  policy      = data.aws_iam_policy_document.telegram_worker.json

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
