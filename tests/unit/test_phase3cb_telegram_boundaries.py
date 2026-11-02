from pathlib import Path

import yaml

from tests.support.local_workers import local_env

ROOT = Path(__file__).resolve().parents[2]


def test_local_telegram_worker_has_an_isolated_queue_and_the_fake_transport() -> None:
    compose = yaml.safe_load((ROOT / "compose.yaml").read_text(encoding="utf-8"))
    fake = compose["services"]["telegram-bot-api-fake"]
    # ADR-020: the worker, the relay and the evaluator run as executables
    # (scripts/dev/run_worker.sh); Compose keeps only the Bot API fake.
    relay = local_env("notification-relay")
    worker = local_env("telegram-worker")
    evaluator = local_env("alert-evaluator")

    assert worker["SQS_TELEGRAM_JOBS_URL"].endswith("/limnopulse-telegram-notification-jobs")
    assert worker["REDIS_ADDR"] == "127.0.0.1:6379"
    assert worker["TELEGRAM_DELIVERY_ENABLED"] == "true"
    assert worker["TELEGRAM_BOT_API_BASE_URL"] == "http://127.0.0.1:8089"
    assert fake["ports"] == ["127.0.0.1:8089:8080"]
    assert relay["TELEGRAM_DELIVERY_ENABLED"] == "true"
    assert evaluator["TELEGRAM_DELIVERY_ENABLED"] == "true"
    assert relay["SQS_TELEGRAM_JOBS_URL"].endswith("/limnopulse-telegram-notification-jobs")
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
        '"sqs:ReceiveMessage"',
        '"sqs:DeleteMessage"',
        '"sqs:ChangeMessageVisibility"',
        '"secretsmanager:GetSecretValue"',
    ):
        assert action in telegram
    # IAM has no dynamodb:TransactWriteItems action: granting it authorizes nothing (issue #108).
    assert '"dynamodb:TransactWriteItems"' not in telegram
    assert "telegram_notification_jobs_queue_name" in variables
    assert "telegram_notification_jobs_dlq_name" in variables
    assert 'output "telegram_notification_jobs_queue_url"' in outputs
    assert 'output "telegram_bot_token_secret_arn"' in outputs
    assert 'output "telegram_webhook_secret_arn"' in outputs
