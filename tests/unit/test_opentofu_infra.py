import json
import re
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
TOFU_ROOT = ROOT / "infra" / "opentofu"


def _read(relative_path: str) -> str:
    return (TOFU_ROOT / relative_path).read_text(encoding="utf-8")


def _opentofu_text_files() -> list[Path]:
    return [
        path for path in TOFU_ROOT.rglob("*") if path.is_file() and ".terraform" not in path.parts
    ]


def _eventbridge_transformers(ses: str) -> dict[str, tuple[dict[str, str], str]]:
    transformers: dict[str, tuple[dict[str, str], str]] = {}
    target_pattern = re.compile(
        r'resource "aws_cloudwatch_event_target" "(?P<name>[^"]+)"\s*\{'
        r"(?P<body>.*?)(?=\nresource |\Z)",
        re.DOTALL,
    )
    for target in target_pattern.finditer(ses):
        body = target.group("body")
        paths_match = re.search(r"input_paths\s*=\s*\{(?P<paths>.*?)\n\s*\}", body, re.DOTALL)
        template_match = re.search(
            r"input_template\s*=\s*<<-JSON\n(?P<template>.*?)\n\s*JSON",
            body,
            re.DOTALL,
        )
        if paths_match is None or template_match is None:
            continue
        paths = dict(re.findall(r'(\w+)\s*=\s*"([^"]+)"', paths_match.group("paths")))
        transformers[target.group("name")] = (paths, template_match.group("template").strip())
    return transformers


def _render_eventbridge_template(
    paths: dict[str, str], template: str, event: dict[str, Any]
) -> dict[str, Any]:
    def read_path(path: str) -> Any:
        value: Any = event
        for component in path.removeprefix("$.").split("."):
            match = re.fullmatch(r"([^[]+)(?:\[(\d+)\])?", component)
            assert match is not None
            value = value[match.group(1)]
            if match.group(2) is not None:
                value = value[int(match.group(2))]
        return value

    rendered = template
    for variable, path in paths.items():
        rendered = rendered.replace(f"<{variable}>", json.dumps(read_path(path)))
    assert not re.search(r"<\w+>", rendered)
    return json.loads(rendered)


def test_opentofu_layout_documents_cloud_boundary() -> None:
    expected_files = {
        "README.md",
        "backend.example.hcl",
        "cognito.tf",
        "dynamodb.tf",
        "env/cloud.tfvars.example",
        "outputs.tf",
        "providers.tf",
        "queues.tf",
        "ses.tf",
        "variables.tf",
        "versions.tf",
    }

    assert {
        str(path.relative_to(TOFU_ROOT)).replace("\\", "/")
        for path in TOFU_ROOT.rglob("*")
        if path.is_file()
    } >= expected_files

    readme = _read("README.md")
    root_readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "Docker Compose" in readme
    assert "development" in readme
    assert "OpenTofu" in readme
    assert "cloud" in readme
    assert "tofu apply" not in readme.lower()
    assert "tofu init -backend=false" in readme
    assert "tofu init -backend=false" in root_readme
    assert "tofu init -backend-config=backend.example.hcl" not in readme
    assert "tofu init -backend-config=backend.example.hcl" not in root_readme


def test_versions_pin_opentofu_and_aws_provider_with_backend_placeholder() -> None:
    versions = _read("versions.tf")
    backend = _read("backend.example.hcl")

    assert 'required_version = ">= 1.8.0"' in versions
    assert 'source  = "hashicorp/aws"' in versions
    assert re.search(r'version\s+=\s+"~>\s*6\.33"', versions)
    assert 'backend "s3" {}' in versions
    assert re.search(r'bucket\s+=\s+"replace-with-remote-state-bucket"', backend)
    assert re.search(r'key\s+=\s+"limnopulse/cloud/terraform.tfstate"', backend)
    assert re.search(r'region\s+=\s+"us-east-2"', backend)


def test_cloud_dynamodb_tables_match_domain_contract() -> None:
    dynamodb = _read("dynamodb.tf")

    assert 'resource "aws_dynamodb_table" "domain"' in dynamodb
    assert 'resource "aws_dynamodb_table" "audit"' in dynamodb
    assert 'billing_mode = "PAY_PER_REQUEST"' in dynamodb
    assert re.search(r'hash_key\s+=\s+"PK"', dynamodb)
    assert re.search(r'range_key\s+=\s+"SK"', dynamodb)
    assert dynamodb.count('name = "PK"') >= 2
    assert dynamodb.count('name = "SK"') >= 2
    assert dynamodb.count('type = "S"') >= 4
    assert "point_in_time_recovery" in dynamodb
    assert "server_side_encryption" in dynamodb
    assert dynamodb.count('attribute_name = "expires_at"') == 2
    assert len(re.findall(r"ttl\s*\{", dynamodb)) == 2
    assert len(re.findall(r"ttl\s*\{[^}]*enabled\s+=\s+true", dynamodb, re.DOTALL)) == 2
    assert re.search(r'name\s+=\s+"AlertEvaluationByDue"', dynamodb)
    assert re.search(r'name\s+=\s+"AlertEventsByTenantTime"', dynamodb)
    assert re.search(r'name\s+=\s+"NotificationRelayByAvailableAt"', dynamodb)
    assert 'hash_key        = "GSI1PK"' in dynamodb
    assert 'range_key       = "GSI1SK"' in dynamodb
    assert 'hash_key        = "GSI2PK"' in dynamodb
    assert 'range_key       = "GSI2SK"' in dynamodb
    assert re.search(r'hash_key\s+=\s+"relay_gsi_pk"', dynamodb)
    assert re.search(r'range_key\s+=\s+"relay_gsi_sk"', dynamodb)
    assert re.search(r'projection_type\s+=\s+"INCLUDE"', dynamodb)
    assert re.search(r'non_key_attributes\s+=\s+\["relay_work_kind"\]', dynamodb)
    assert 'projection_type = "KEYS_ONLY"' in dynamodb
    assert 'projection_type = "ALL"' in dynamodb


def test_cognito_resources_export_application_environment_contract() -> None:
    cognito = _read("cognito.tf")
    outputs = _read("outputs.tf")

    assert 'resource "aws_cognito_user_pool" "main"' in cognito
    assert 'resource "aws_cognito_user_pool_client" "api"' in cognito
    assert 'auto_verified_attributes = ["email"]' in cognito
    assert "generate_secret = false" in cognito
    assert '"ALLOW_USER_SRP_AUTH"' in cognito
    assert 'output "cognito_user_pool_id"' in outputs
    assert 'output "cognito_client_id"' in outputs
    assert 'output "cognito_issuer"' in outputs
    assert (
        "https://cognito-idp.${var.aws_region}.amazonaws.com/${aws_cognito_user_pool.main.id}"
        in outputs
    )
    assert "Cloud Redis endpoint placeholder only" in outputs
    assert "Cloud InfluxDB endpoint placeholder only" in outputs


def test_notification_queues_and_ses_eventbridge_boundary_are_safe_by_default() -> None:
    queues = _read("queues.tf")
    ses = _read("ses.tf")
    variables = _read("variables.tf")
    tfvars = _read("env/cloud.tfvars.example")

    for resource in (
        "notification_jobs",
        "notification_jobs_dlq",
        "telegram_notification_jobs",
        "telegram_notification_jobs_dlq",
        "ses_events",
        "ses_events_dlq",
        "ses_events_routing_dlq",
    ):
        assert f'resource "aws_sqs_queue" "{resource}"' in queues
    assert len(re.findall(r"sqs_managed_sse_enabled\s+=\s+true", queues)) == 7
    assert len(re.findall(r"maxReceiveCount\s+=\s+8", queues)) == 3
    assert 'resource "aws_sqs_queue_redrive_allow_policy" "notification_jobs_dlq"' in queues
    assert (
        'resource "aws_sqs_queue_redrive_allow_policy" "telegram_notification_jobs_dlq"' in queues
    )
    assert 'resource "aws_sqs_queue_redrive_allow_policy" "ses_events_dlq"' in queues
    assert 'resource "aws_sqs_queue_policy" "ses_events"' in queues
    assert 'resource "aws_sqs_queue_policy" "ses_events_routing_dlq"' in queues

    assert 'resource "aws_sesv2_configuration_set" "notifications"' in ses
    assert 'resource "aws_sesv2_configuration_set_event_destination" "eventbridge"' in ses
    assert 'resource "aws_cloudwatch_event_rule" "ses_notifications"' in ses
    assert 'resource "aws_cloudwatch_event_target" "ses_events"' in ses
    for event_type in ("SEND", "DELIVERY", "DELIVERY_DELAY", "BOUNCE", "COMPLAINT", "REJECT"):
        assert f'"{event_type}"' in ses
    assert "dead_letter_config" in ses
    assert "retry_policy" in ses
    assert "aws_ses_email_identity" not in ses
    assert "aws_iam_role" not in ses
    assert "aws_scheduler" not in ses

    for variable in (
        "notification_jobs_queue_name",
        "notification_jobs_dlq_name",
        "ses_events_queue_name",
        "ses_events_dlq_name",
        "ses_events_routing_dlq_name",
        "ses_configuration_set_name",
    ):
        assert f'variable "{variable}"' in variables
        assert re.search(rf"^{variable}\s+=", tfvars, re.MULTILINE)
    assert re.search(
        r'^notification_jobs_queue_name\s+=\s+"limnopulse-notification-jobs"$',
        tfvars,
        re.MULTILINE,
    )
    assert re.search(r'^ses_events_queue_name\s+=\s+"limnopulse-ses-events"$', tfvars, re.MULTILINE)


def test_ses_eventbridge_rules_match_only_the_notifications_configuration_set() -> None:
    ses = _read("ses.tf")
    for rule in ("ses_notifications", "ses_notifications_bounce", "ses_notifications_reject"):
        match = re.search(
            rf'resource "aws_cloudwatch_event_rule" "{rule}"\s*\{{(?P<body>.*?)(?=\nresource |\Z)',
            ses,
            re.DOTALL,
        )
        assert match is not None
        assert '"ses:configuration-set" = [var.ses_configuration_set_name]' in match.group("body")


def test_ses_eventbridge_targets_emit_only_parseable_non_pii_feedback() -> None:
    ses = _read("ses.tf")
    transformers = _eventbridge_transformers(ses)
    assert set(transformers) == {"ses_events", "ses_events_bounce", "ses_events_reject"}

    base_event = {
        "version": "0",
        "id": "evt_transformer_contract",
        "detail-type": "Simple Email Service Email Sending Event",
        "source": "aws.ses",
        "detail": {
            "eventType": "Send",
            "mail": {
                "messageId": "provider_message_1",
                "source": "sender@example.test",
                "destination": ["recipient@example.test"],
                "headers": [{"name": "Subject", "value": "secret subject"}],
                "commonHeaders": {"subject": "secret subject"},
                "tags": {"delivery_id": ["del_1"], "attempt_id": ["att_1"]},
            },
            "bounce": {
                "bounceType": "Permanent",
                "bouncedRecipients": [{"emailAddress": "recipient@example.test"}],
            },
            "complaint": {"complainedRecipients": [{"emailAddress": "recipient@example.test"}]},
            "deliveryDelay": {"delayedRecipients": [{"emailAddress": "recipient@example.test"}]},
            "reject": {"reason": "Bad content"},
        },
    }
    routes = {
        "Send": "ses_events",
        "Delivery": "ses_events",
        "DeliveryDelay": "ses_events",
        "Complaint": "ses_events",
        "Bounce": "ses_events_bounce",
        "Reject": "ses_events_reject",
    }
    forbidden_paths = {
        "$.detail",
        "$.detail.mail.source",
        "$.detail.mail.destination",
        "$.detail.mail.headers",
        "$.detail.mail.commonHeaders",
        "$.detail.bounce.bouncedRecipients",
        "$.detail.complaint.complainedRecipients",
        "$.detail.deliveryDelay.delayedRecipients",
    }
    forbidden_keys = {
        "sourceIp",
        "destination",
        "headers",
        "commonHeaders",
        "subject",
        "bouncedRecipients",
        "complainedRecipients",
        "delayedRecipients",
        "recipients",
        "emailAddress",
    }

    for event_type, route in routes.items():
        paths, template = transformers[route]
        assert forbidden_paths.isdisjoint(paths.values())
        event = json.loads(json.dumps(base_event))
        event["detail"]["eventType"] = event_type
        emitted = _render_eventbridge_template(paths, template, event)
        assert emitted["version"] == "0"
        assert emitted["id"] == "evt_transformer_contract"
        assert emitted["detail-type"] == "Simple Email Service Email Sending Event"
        assert emitted["source"] == "aws.ses"
        detail = emitted["detail"]
        assert detail["eventType"] == event_type
        assert detail["mail"] == {
            "messageId": "provider_message_1",
            "tags": {"delivery_id": ["del_1"], "attempt_id": ["att_1"]},
        }
        serialized = json.dumps(emitted)
        assert "sender@example.test" not in serialized
        assert "recipient@example.test" not in serialized
        assert "secret subject" not in serialized
        assert forbidden_keys.isdisjoint(re.findall(r'"([^"]+)"\s*:', serialized))
        if event_type == "Bounce":
            assert detail["bounce"] == {"bounceType": "Permanent"}
        elif event_type == "Reject":
            assert detail["reject"] == {"reason": "Bad content"}
        elif event_type == "DeliveryDelay":
            assert detail["deliveryDelay"] == {}
        elif event_type == "Complaint":
            assert detail["complaint"] == {}


def test_notification_outputs_expose_runtime_queue_contract() -> None:
    outputs = _read("outputs.tf")

    for output in (
        "notification_jobs_queue_url",
        "notification_jobs_queue_arn",
        "notification_jobs_dlq_url",
        "ses_events_queue_url",
        "ses_events_queue_arn",
        "ses_events_dlq_url",
        "ses_events_routing_dlq_url",
        "ses_configuration_set_name",
    ):
        assert f'output "{output}"' in outputs
    assert 'description = "SQS_NOTIFICATION_JOBS_URL"' in outputs
    assert 'description = "SQS_SES_EVENTS_URL"' in outputs
    assert 'description = "SES_CONFIGURATION_SET_NAME"' in outputs


def test_opentofu_examples_and_gitignore_do_not_commit_state_or_secrets() -> None:
    gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8")
    files = _opentofu_text_files()
    secret_patterns = [
        re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
        re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
        re.compile(r"\bghp_[A-Za-z0-9_]{20,}\b"),
        re.compile(r"\bsk-[A-Za-z0-9]{20,}\b"),
        re.compile(r"\beyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{10,}\b"),
    ]

    assert ".terraform/" in gitignore
    assert "*.tfstate" in gitignore
    assert "*.tfvars" in gitignore
    assert "!*.tfvars.example" in gitignore
    assert "*.tfplan" in gitignore
    assert "*.tfbackend" in gitignore
    assert "!backend.example.hcl" in gitignore

    offenders: list[str] = []
    for path in files:
        text = path.read_text(encoding="utf-8")
        if any(pattern.search(text) for pattern in secret_patterns):
            offenders.append(str(path.relative_to(ROOT)))

    assert offenders == []


def test_opentofu_scaffold_avoids_local_only_and_disallowed_storage_stack() -> None:
    files = _opentofu_text_files()
    forbidden_terms = [
        "post" + "gres",
        "post" + "gresql",
        "psy" + "copg",
        "sql" + "alchemy",
        "fire" + "store",
        "fire" + "base",
        "dynamodb-local",
        "mqtt-broker",
        "telegraf",
        "mosquitto",
    ]
    forbidden = re.compile("|".join(forbidden_terms), re.IGNORECASE)

    offenders: list[str] = []
    for path in files:
        if forbidden.search(path.read_text(encoding="utf-8")):
            offenders.append(str(path.relative_to(ROOT)))

    assert offenders == []
