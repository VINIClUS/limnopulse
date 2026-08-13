from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_compose_has_isolated_telegram_queue_worker_and_fake_transport() -> None:
    compose = yaml.safe_load((ROOT / "compose.yaml").read_text(encoding="utf-8"))
    services = compose["services"]
    relay = services["notification-relay"]
    worker = services["telegram-notification-worker"]
    fake = services["telegram-bot-api-fake"]
    evaluator = services["alert-evaluator"]

    assert worker["command"] == ["telegram-worker"]
    assert worker["profiles"] == ["notifications"]
    assert worker["environment"]["SQS_TELEGRAM_JOBS_URL"].endswith(
        "/limnopulse-telegram-notification-jobs"
    )
    assert worker["environment"]["REDIS_ADDR"] == "redis:6379"
    assert worker["environment"]["TELEGRAM_DELIVERY_ENABLED"] == "true"
    assert worker["environment"]["TELEGRAM_BOT_API_BASE_URL"] == (
        "http://telegram-bot-api-fake:8080"
    )
    assert "redis" in worker["depends_on"]
    assert relay["environment"]["TELEGRAM_DELIVERY_ENABLED"] == "true"
    assert evaluator["environment"]["TELEGRAM_DELIVERY_ENABLED"] == "true"
    assert relay["environment"]["SQS_TELEGRAM_JOBS_URL"].endswith(
        "/limnopulse-telegram-notification-jobs"
    )
    assert fake["profiles"] == ["notifications"]

    elasticmq = (ROOT / "infra" / "elasticmq" / "elasticmq.conf").read_text(encoding="utf-8")
    assert '"limnopulse-telegram-notification-jobs"' in elasticmq
    assert '"limnopulse-telegram-notification-jobs-dlq"' in elasticmq
    assert "maxReceiveCount = 8" in elasticmq


def test_opentofu_declares_telegram_queues_secret_containers_and_least_privilege() -> None:
    queues = (ROOT / "infra" / "opentofu" / "queues.tf").read_text(encoding="utf-8")
    telegram = (ROOT / "infra" / "opentofu" / "telegram.tf").read_text(encoding="utf-8")
    variables = (ROOT / "infra" / "opentofu" / "variables.tf").read_text(encoding="utf-8")
    outputs = (ROOT / "infra" / "opentofu" / "outputs.tf").read_text(encoding="utf-8")

    assert 'resource "aws_sqs_queue" "telegram_notification_jobs"' in queues
    assert 'resource "aws_sqs_queue" "telegram_notification_jobs_dlq"' in queues
    assert (
        'resource "aws_sqs_queue_redrive_allow_policy" "telegram_notification_jobs_dlq"' in queues
    )
    assert 'resource "aws_secretsmanager_secret" "telegram_bot_token"' in telegram
    assert 'resource "aws_secretsmanager_secret" "telegram_webhook_secret"' in telegram
    assert "aws_secretsmanager_secret_version" not in telegram
    assert 'resource "aws_iam_policy" "telegram_worker"' in telegram
    assert 'resource "aws_iam_policy" "telegram_webhook_secret_reader"' in telegram
    for action in (
        '"dynamodb:GetItem"',
        '"dynamodb:UpdateItem"',
        '"dynamodb:TransactWriteItems"',
        '"sqs:ReceiveMessage"',
        '"sqs:DeleteMessage"',
        '"sqs:ChangeMessageVisibility"',
        '"secretsmanager:GetSecretValue"',
    ):
        assert action in telegram
    assert "telegram_notification_jobs_queue_name" in variables
    assert "telegram_notification_jobs_dlq_name" in variables
    assert 'output "telegram_notification_jobs_queue_url"' in outputs
    assert 'output "telegram_bot_token_secret_arn"' in outputs
    assert 'output "telegram_webhook_secret_arn"' in outputs
