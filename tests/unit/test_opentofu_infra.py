import hashlib
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

    assert 'required_version = ">= 1.10.0"' in versions
    assert 'source  = "hashicorp/aws"' in versions
    assert re.search(r'version\s+=\s+"~>\s*6\.33"', versions)
    assert 'backend "s3" {}' in versions
    assert re.search(r'bucket\s+=\s+"replace-with-remote-state-bucket"', backend)
    assert re.search(r'key\s+=\s+"limnopulse/cloud/terraform.tfstate"', backend)
    assert re.search(r'region\s+=\s+"us-east-2"', backend)
    assert re.search(r"^use_lockfile\s*=\s*true$", backend, re.MULTILINE)
    assert "dynamodb_table" not in backend


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


def test_email_worker_policy_scopes_send_email_to_configured_sender() -> None:
    ses = _read("ses.tf")

    policy_match = re.search(
        r'data "aws_iam_policy_document" "email_worker"\s*\{(?P<body>.*?)(?=\ndata |\nresource |\Z)',
        ses,
        re.DOTALL,
    )
    assert policy_match is not None
    body = policy_match.group("body")
    assert "count = var.email_delivery ? 1 : 0" in body

    statement_match = re.search(
        r'sid\s*=\s*"SesSendEmail".*?\}(?=\n\s*\}\s*\n\s*statement|\Z)',
        body,
        re.DOTALL,
    )
    assert statement_match is not None
    statement = statement_match.group(0)
    assert '"ses:SendEmail"' in statement
    assert '"sesv2:SendEmail"' not in statement
    assert 'variable = "ses:FromAddress"' in statement
    assert "values   = [var.ses_from_address]" in statement
    assert "precondition" not in body


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


# --- P3-02 telemetry ingress queues (ingestion_queues.tf) -----------------------------
#
# A minimal HCL reader: enough to resolve top-level blocks, their attributes and nested
# blocks without a new dependency. Attribute values stay raw expression text.

INGESTION_GATE = "var.telemetry_ingress ? 1 : 0"
# The on-call email subscription is the only block with a stricter gate: no mailbox, no
# subscription.
ALARM_EMAIL_GATE = 'var.telemetry_ingress && var.telemetry_ingress_alarm_email != "" ? 1 : 0'
ALARM_TOPIC = "aws_sns_topic.telemetry_ingress_alarms[0]"
UNCHANGED_FILE_SHA256 = {
    # ADR-020 S6 and S7 changed only comments: the shared workers user is marked
    # deprecated, and the header names the per-role principals instead of cmd/notifications.
    "iam_runtime.tf": "5cea7356ab074f5c8eefdc738bb12c213ecac8db2110fa06e93d4c22c23b1cf2",
    "queues.tf": "d85972d656c951d6a728fecfe4f762f0e96dc1e4687ee39d068edbea74ad9425",
}
# Digest of every pre-P3 block in variables.tf and outputs.tf (see _pre_p3_digest): P3-02
# may only append telemetry_ingress* / telemetry_ingest* blocks to those files, and ADR-020
# S6 only the ADR020_VARIABLES and ADR020_OUTPUTS blocks.
PRE_P3_BLOCKS_SHA256 = {
    "variables.tf": "05b652d1d9e61efd4622e0ba4593d517708ec5260ca2164e2c5938ac6bee9288",
    "outputs.tf": "67f70e1569ba6bb1fc765025f92b8358101f8b90fc453272e8d46fd960aadda4",
}
MODULE_TF_FILES = {
    "cognito.tf",
    "dynamodb.tf",
    "iam_runtime.tf",
    "ingestion_queues.tf",
    "outputs.tf",
    "providers.tf",
    "queues.tf",
    "ses.tf",
    "telegram.tf",
    "variables.tf",
    "versions.tf",
    "worker_alarms.tf",
    "workers.tf",
}
ADR020_VARIABLES = {
    "alert_evaluator_enabled",
    "notification_relay_enabled",
    "email_worker_enabled",
    "ses_feedback_worker_enabled",
    "telegram_worker_enabled",
    "worker_monitor_enabled",
    "worker_queue_age_alarm_threshold_seconds",
    "worker_queue_depth_alarm_threshold",
    "worker_alarm_actions",
    "worker_alarm_email",
    "limnopulse_admin_trusted_principal_arns",
}
ADR020_OUTPUTS = {
    "alert_evaluator_iam_user_name",
    "notification_relay_iam_user_name",
    "email_worker_iam_user_name",
    "ses_feedback_worker_iam_user_name",
    "telegram_worker_iam_user_name",
    "worker_monitor_iam_user_name",
    "limnopulse_admin_role_arn",
    "worker_alarm_topic_arn",
}
INGRESS_QUEUE = "aws_sqs_queue.telemetry_ingress[0]"
DLQ_QUEUE = "aws_sqs_queue.telemetry_ingress_dlq[0]"
TELEMETRY_VARIABLES = {
    "telemetry_ingress": ("bool", "false"),
    "telemetry_ingress_queue_name": ("string", '"limnopulse-telemetry-ingress"'),
    "telemetry_ingress_dlq_name": ("string", '"limnopulse-telemetry-ingress-dlq"'),
    "telemetry_ingress_age_alarm_threshold_seconds": ("number", "300"),
    "telemetry_ingress_depth_alarm_threshold": ("number", None),
    "telemetry_ingress_alarm_actions": ("list(string)", "[]"),
    "telemetry_ingress_alarm_email": ("string", '""'),
}


class _HclBlock:
    def __init__(self, kind: str, labels: tuple[str, ...], body: str) -> None:
        self.kind = kind
        self.labels = labels
        self.attrs, self.blocks = _parse_hcl_body(body)

    def nested(self, kind: str) -> list["_HclBlock"]:
        return [block for block in self.blocks if block.kind == kind]


def _skip_string(text: str, index: int) -> int:
    index += 1
    while text[index] != '"':
        index += 2 if text[index] == "\\" else 1
    return index + 1


def _skip_heredoc(text: str, index: int) -> int:
    match = re.compile(r"<<-?(\w+)\n").match(text, index)
    assert match is not None
    end = re.compile(rf"^\s*{match.group(1)}\s*$", re.MULTILINE).search(text, match.end())
    assert end is not None
    return end.end()


def _read_expression(text: str, index: int) -> tuple[str, int]:
    start, depth = index, 0
    while index < len(text):
        char = text[index]
        if char == '"':
            index = _skip_string(text, index)
            continue
        if text.startswith("<<", index):
            index = _skip_heredoc(text, index)
            continue
        if text.startswith("/*", index):
            index = text.index("*/", index) + 2
            continue
        if char == "#" or text.startswith("//", index):
            if depth == 0:
                break
            index = text.find("\n", index)
            continue
        if char in "([{":
            depth += 1
        elif char in ")]}":
            depth -= 1
        elif char == "\n" and depth == 0:
            break
        index += 1
    return text[start:index].strip(), index


def _matching_brace(text: str, index: int) -> int:
    depth = 0
    while index < len(text):
        char = text[index]
        if char == '"':
            index = _skip_string(text, index)
            continue
        if text.startswith("<<", index):
            index = _skip_heredoc(text, index)
            continue
        if text.startswith("/*", index):
            index = text.index("*/", index) + 2
            continue
        if char == "#" or text.startswith("//", index):
            index = text.find("\n", index)
            continue
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return index
        index += 1
    raise AssertionError("unbalanced braces")


def _parse_hcl_body(text: str) -> tuple[dict[str, str], list[_HclBlock]]:
    attrs: dict[str, str] = {}
    blocks: list[_HclBlock] = []
    head = re.compile(
        r'\s*(?:(?:(?:#|//)[^\n]*\n|/\*.*?\*/)\s*)*([A-Za-z_][\w-]*)\s*((?:"[^"]*"\s*)*)(=|\{)',
        re.DOTALL,
    )
    index = 0
    while True:
        match = head.match(text, index)
        if match is None:
            assert re.fullmatch(r"(\s*((#|//)[^\n]*|/\*.*?\*/))*\s*", text[index:], re.DOTALL), (
                text[index : index + 80]
            )
            return attrs, blocks
        name = match.group(1)
        if match.group(3) == "=":
            value, index = _read_expression(text, match.end())
            assert name not in attrs, name
            attrs[name] = value
            continue
        close = _matching_brace(text, match.end() - 1)
        labels = tuple(re.findall(r'"([^"]*)"', match.group(2)))
        blocks.append(_HclBlock(name, labels, text[match.end() : close]))
        index = close + 1


def _tofu_blocks(relative_path: str) -> list[_HclBlock]:
    return _parse_hcl_body(_read(relative_path))[1]


def _block_signature(block: _HclBlock) -> list[object]:
    attrs = sorted((name, re.sub(r"\s+", " ", value)) for name, value in block.attrs.items())
    return [block.kind, list(block.labels), attrs, [_block_signature(b) for b in block.blocks]]


def _pre_p3_digest(relative_path: str) -> str:
    signatures = [
        _block_signature(block)
        for block in _tofu_blocks(relative_path)
        if not (
            block.labels
            and (
                block.labels[0].startswith(("telemetry_ingress", "telemetry_ingest"))
                or block.labels[0] in ADR020_VARIABLES | ADR020_OUTPUTS
            )
        )
    ]
    return hashlib.sha256(json.dumps(signatures).encode()).hexdigest()


def _strip_comments(text: str) -> str:
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
    return re.sub(r"(?m)^\s*(#|//).*$", "", text)


def _ingestion_blocks() -> list[_HclBlock]:
    return _tofu_blocks("ingestion_queues.tf")


def _resources(blocks: list[_HclBlock], resource_type: str) -> dict[str, _HclBlock]:
    return {
        block.labels[1]: block
        for block in blocks
        if block.kind == "resource" and block.labels[0] == resource_type
    }


def _policy_documents(blocks: list[_HclBlock]) -> dict[str, _HclBlock]:
    return {
        block.labels[1]: block
        for block in blocks
        if block.kind == "data" and block.labels[0] == "aws_iam_policy_document"
    }


def _strip_expression_comments(expression: str) -> str:
    """Drop #, // and /* */ comments that sit outside string literals."""
    kept, index = [], 0
    while index < len(expression):
        if expression[index] == '"':
            end = _skip_string(expression, index)
            kept.append(expression[index:end])
            index = end
        elif expression.startswith("/*", index):
            index = expression.index("*/", index) + 2
        elif expression[index] == "#" or expression.startswith("//", index):
            newline = expression.find("\n", index)
            index = len(expression) if newline == -1 else newline
        else:
            kept.append(expression[index])
            index += 1
    return "".join(kept).strip()


def _list_items(expression: str) -> list[str]:
    expression = _strip_expression_comments(expression)
    assert expression.startswith("[") and expression.endswith("]"), expression
    items: list[str] = []
    depth, current = 0, ""
    for char in expression[1:-1]:
        if char in "([{":
            depth += 1
        elif char in ")]}":
            depth -= 1
        if char == "," and depth == 0:
            items.append(current.strip())
            current = ""
        else:
            current += char
    if current.strip():
        items.append(current.strip())
    return items


def _unquote(expression: str) -> str:
    assert expression.startswith('"') and expression.endswith('"'), expression
    return expression[1:-1]


def _jsonencode_fields(expression: str) -> dict[str, str]:
    match = re.fullmatch(r"jsonencode\(\{(?P<body>.*)\}\)", expression, re.DOTALL)
    assert match is not None, expression
    attrs, blocks = _parse_hcl_body(match.group("body"))
    assert blocks == []
    return attrs


_RESOURCE_KINDS = {
    f"{INGRESS_QUEUE}.arn": "ingress",
    f"{DLQ_QUEUE}.arn": "dlq",
    "aws_dynamodb_table.domain.arn": "domain",
    "aws_dynamodb_table.audit.arn": "audit",
}


def _assert_plain_document(document: _HclBlock) -> None:
    # No dynamic statements and no source/override documents: every grant must be a literal
    # statement this reader can see.
    assert "count" in document.attrs, document.labels
    assert set(document.attrs) <= {"count", "version", "policy_id"}, document.labels
    assert {block.kind for block in document.blocks} == {"statement"}, document.labels


def _grants(document: _HclBlock) -> set[tuple[str, str, str]]:
    _assert_plain_document(document)
    grants: set[tuple[str, str, str]] = set()
    for statement in document.nested("statement"):
        assert set(statement.attrs) <= {"sid", "effect", "actions", "resources"}, statement.attrs
        assert statement.blocks == []
        assert statement.attrs.get("effect", '"Allow"') == '"Allow"'
        actions = [_unquote(item) for item in _list_items(statement.attrs["actions"])]
        for resource in _list_items(statement.attrs["resources"]):
            assert resource in _RESOURCE_KINDS, f"unexpected resource {resource}"
            for action in actions:
                assert "*" not in action
                grants.add((action, _RESOURCE_KINDS[resource], "Allow"))
    return grants


def _policy_grants(policy: _HclBlock, documents: dict[str, _HclBlock]) -> set[tuple[str, str, str]]:
    match = re.fullmatch(r"data\.aws_iam_policy_document\.(\w+)\[0\]\.json", policy.attrs["policy"])
    assert match is not None, policy.attrs["policy"]
    return _grants(documents[match.group(1)])


def _principal_grants() -> tuple[dict[str, set[tuple[str, str, str]]], set[str]]:
    blocks = _ingestion_blocks()
    documents = _policy_documents(blocks)
    policies = _resources(blocks, "aws_iam_policy")
    by_user: dict[str, set[tuple[str, str, str]]] = {}
    attached: set[str] = set()
    for path in sorted(TOFU_ROOT.glob("*.tf")):
        for block in _parse_hcl_body(path.read_text(encoding="utf-8"))[1]:
            references = " ".join(block.attrs.values())
            if block.kind != "resource" or not any(
                f"aws_iam_policy.{name}" in references for name in policies
            ):
                continue
            assert path.name == "ingestion_queues.tf", path
            assert block.labels[0] == "aws_iam_user_policy_attachment", block.labels
    for attachment in _resources(blocks, "aws_iam_user_policy_attachment").values():
        user = re.fullmatch(r"aws_iam_user\.(\w+)(?:\[0\])?\.name", attachment.attrs["user"])
        policy = re.fullmatch(r"aws_iam_policy\.(\w+)\[0\]\.arn", attachment.attrs["policy_arn"])
        assert user is not None and policy is not None
        attached.add(policy.group(1))
        by_user.setdefault(user.group(1), set()).update(
            _policy_grants(policies[policy.group(1)], documents)
        )
    unattached = set(policies) - attached
    return by_user, unattached


def test_ingestion_queues_tf_count_gate_covers_every_block() -> None:
    blocks = _ingestion_blocks()
    allowed = {
        ("resource", "aws_sqs_queue"),
        ("resource", "aws_sqs_queue_redrive_allow_policy"),
        ("resource", "aws_sqs_queue_policy"),
        ("resource", "aws_iam_user"),
        ("resource", "aws_iam_policy"),
        ("resource", "aws_iam_user_policy_attachment"),
        ("resource", "aws_cloudwatch_metric_alarm"),
        ("resource", "aws_kms_key"),
        ("resource", "aws_sns_topic"),
        ("resource", "aws_sns_topic_policy"),
        ("resource", "aws_sns_topic_subscription"),
        ("data", "aws_iam_policy_document"),
        ("data", "aws_caller_identity"),
    }

    assert blocks
    for block in blocks:
        assert (block.kind, block.labels[0]) in allowed, block.labels
        expected_gate = (
            ALARM_EMAIL_GATE if block.labels[0] == "aws_sns_topic_subscription" else INGESTION_GATE
        )
        assert block.attrs.get("count") == expected_gate, block.labels

    text = _strip_comments(_read("ingestion_queues.tf"))
    gated = {(block.labels[0], block.labels[1]) for block in blocks}
    references = list(re.finditer(r"\b(aws_[a-z0-9_]+)\.(\w+)(\[0\])?", text))
    assert references
    for reference in references:
        if (reference.group(1), reference.group(2)) in gated:
            assert reference.group(3) == "[0]", reference.group(0)

    variables = {
        block.labels[0]: block for block in _tofu_blocks("variables.tf") if block.kind == "variable"
    }
    flag = variables["telemetry_ingress"]
    assert flag.attrs["type"] == "bool"
    assert flag.attrs["default"] == "false"


def test_ingestion_queues_tf_queues_use_sse_and_max_message_size() -> None:
    queues = _resources(_ingestion_blocks(), "aws_sqs_queue")

    assert set(queues) == {"telemetry_ingress", "telemetry_ingress_dlq"}
    for queue in queues.values():
        assert queue.attrs["sqs_managed_sse_enabled"] == "true"
        assert queue.attrs["max_message_size"] == "262144"
        assert "kms_master_key_id" not in queue.attrs


def test_ingestion_queues_tf_retention_visibility_wait_and_redrive_values() -> None:
    queues = _resources(_ingestion_blocks(), "aws_sqs_queue")
    dlq = queues["telemetry_ingress_dlq"]
    ingress = queues["telemetry_ingress"]

    assert dlq.attrs == {
        "count": INGESTION_GATE,
        "name": "var.telemetry_ingress_dlq_name",
        "message_retention_seconds": "1209600",
        "sqs_managed_sse_enabled": "true",
        "max_message_size": "262144",
    }
    redrive = _jsonencode_fields(ingress.attrs.pop("redrive_policy"))
    assert ingress.attrs == {
        "count": INGESTION_GATE,
        "name": "var.telemetry_ingress_queue_name",
        "visibility_timeout_seconds": "60",
        "message_retention_seconds": "345600",
        "receive_wait_time_seconds": "20",
        "sqs_managed_sse_enabled": "true",
        "max_message_size": "262144",
    }
    assert redrive == {
        "deadLetterTargetArn": f"{DLQ_QUEUE}.arn",
        "maxReceiveCount": "8",
    }
    assert int(dlq.attrs["message_retention_seconds"]) > int(
        ingress.attrs["message_retention_seconds"]
    )


def test_ingestion_queues_tf_redrive_allow_policy_is_by_queue() -> None:
    policies = _resources(_ingestion_blocks(), "aws_sqs_queue_redrive_allow_policy")

    assert len(policies) == 1
    (policy,) = policies.values()
    assert policy.attrs["queue_url"] == f"{DLQ_QUEUE}.id"
    fields = _jsonencode_fields(policy.attrs["redrive_allow_policy"])
    assert set(fields) == {"redrivePermission", "sourceQueueArns"}
    assert fields["redrivePermission"] == '"byQueue"'
    assert _list_items(fields["sourceQueueArns"]) == [f"{INGRESS_QUEUE}.arn"]


def test_ingestion_queues_tf_queue_policies_deny_insecure_transport() -> None:
    blocks = _ingestion_blocks()
    documents = _policy_documents(blocks)
    queue_policies = _resources(blocks, "aws_sqs_queue_policy")

    covered: set[str] = set()
    for queue_policy in queue_policies.values():
        queue = re.fullmatch(r"(aws_sqs_queue\.\w+\[0\])\.id", queue_policy.attrs["queue_url"])
        document = re.fullmatch(
            r"data\.aws_iam_policy_document\.(\w+)\[0\]\.json", queue_policy.attrs["policy"]
        )
        assert queue is not None and document is not None
        policy_document = documents[document.group(1)]
        _assert_plain_document(policy_document)
        statements = policy_document.nested("statement")
        assert len(statements) == 1
        (statement,) = statements
        assert set(statement.attrs) <= {"sid", "effect", "actions", "resources"}
        assert {block.kind for block in statement.blocks} == {"principals", "condition"}
        assert statement.attrs["effect"] == '"Deny"'
        assert _list_items(statement.attrs["actions"]) == ['"sqs:*"']
        assert _list_items(statement.attrs["resources"]) == [f"{queue.group(1)}.arn"]
        (principals,) = statement.nested("principals")
        assert principals.attrs["type"] in {'"*"', '"AWS"'}
        assert _list_items(principals.attrs["identifiers"]) == ['"*"']
        (condition,) = statement.nested("condition")
        assert condition.attrs["test"] == '"Bool"'
        assert condition.attrs["variable"] == '"aws:SecureTransport"'
        assert _list_items(condition.attrs["values"]) == ['"false"']
        covered.add(queue.group(1))

    assert covered == {INGRESS_QUEUE, DLQ_QUEUE}
    assert len(queue_policies) == 2


def test_ingestion_queues_tf_iam_actions_per_principal_are_exact() -> None:
    by_user, unattached = _principal_grants()
    users = _resources(_ingestion_blocks(), "aws_iam_user")

    assert set(users) == {"telemetry_ingest"}
    worker_user = users["telemetry_ingest"]
    assert worker_user.attrs["name"] == '"${var.project_name}-${var.environment}-telemetry-ingest"'
    assert worker_user.attrs["force_destroy"] == "true"
    assert worker_user.attrs["tags"] == "local.common_tags"

    assert set(by_user) == {"api", "telemetry_ingest"}
    assert by_user["api"] == {
        ("sqs:SendMessage", "ingress", "Allow"),
        ("dynamodb:DeleteItem", "domain", "Allow"),
        ("dynamodb:ConditionCheckItem", "domain", "Allow"),
        ("dynamodb:PutItem", "audit", "Allow"),
    }
    assert by_user["telemetry_ingest"] == {
        ("sqs:ReceiveMessage", "ingress", "Allow"),
        ("sqs:DeleteMessage", "ingress", "Allow"),
        ("sqs:ChangeMessageVisibility", "ingress", "Allow"),
        ("sqs:GetQueueAttributes", "ingress", "Allow"),
        ("dynamodb:GetItem", "domain", "Allow"),
        ("dynamodb:Query", "domain", "Allow"),
        ("dynamodb:PutItem", "domain", "Allow"),
        ("dynamodb:UpdateItem", "domain", "Allow"),
    }

    blocks = _ingestion_blocks()
    policies = _resources(blocks, "aws_iam_policy")
    assert len(unattached) == 1
    (operator,) = unattached
    assert _policy_grants(policies[operator], _policy_documents(blocks)) == {
        ("sqs:ReceiveMessage", "dlq", "Allow"),
        ("sqs:DeleteMessage", "dlq", "Allow"),
        ("sqs:ChangeMessageVisibility", "dlq", "Allow"),
        ("sqs:GetQueueAttributes", "dlq", "Allow"),
        ("sqs:SendMessage", "ingress", "Allow"),
        ("dynamodb:Query", "domain", "Allow"),
        ("dynamodb:GetItem", "domain", "Allow"),
        ("dynamodb:UpdateItem", "domain", "Allow"),
    }


def test_ingestion_queues_tf_operator_policy_is_unattached_and_workers_not_widened() -> None:
    _, unattached = _principal_grants()
    (operator,) = unattached

    assert {path.name for path in TOFU_ROOT.glob("*.tf")} == MODULE_TF_FILES
    api_references: list[str] = []
    for path in sorted(TOFU_ROOT.glob("*.tf")):
        if path.name in {"iam_runtime.tf", "outputs.tf"}:
            continue
        for block in _parse_hcl_body(path.read_text(encoding="utf-8"))[1]:
            references = " ".join(block.attrs.values())
            assert f"aws_iam_policy.{operator}" not in references, path
            assert "aws_iam_user.workers" not in references, (path, block.labels)
            if "aws_iam_user.api" in references:
                api_references.append(f"{path.name}:{block.labels[0]}")
    assert api_references
    assert set(api_references) == {"ingestion_queues.tf:aws_iam_user_policy_attachment"}

    # Nothing telemetry-related may live outside the P3-02 files: no grant to the new user,
    # no inline policy and no ungated alarm smuggled into another module file.
    for path in sorted(TOFU_ROOT.glob("*.tf")):
        if path.name in {"ingestion_queues.tf", "variables.tf", "outputs.tf"}:
            continue
        text = _strip_comments(path.read_text(encoding="utf-8"))
        assert re.search(r"telemetry[_-]ingest|telemetry[_-]ingress", text) is None, path

    ingestion = _strip_comments(_read("ingestion_queues.tf"))
    assert "aws_iam_user.workers" not in ingestion
    for forbidden in (
        '"aws_iam_user_policy"',
        '"aws_iam_group',
        '"aws_iam_role',
        '"aws_iam_access_key"',
        '"aws_iam_user_group_membership"',
        "attachments_exclusive",
    ):
        assert forbidden not in ingestion
    for name, digest in UNCHANGED_FILE_SHA256.items():
        assert hashlib.sha256((TOFU_ROOT / name).read_bytes()).hexdigest() == digest, name
    for name, digest in PRE_P3_BLOCKS_SHA256.items():
        assert _pre_p3_digest(name) == digest, name


def test_ingestion_queues_tf_alarms_cover_age_dlq_and_depth() -> None:
    alarms = _resources(_ingestion_blocks(), "aws_cloudwatch_metric_alarm")

    observed: dict[tuple[str, str], tuple[str, str]] = {}
    for alarm in alarms.values():
        assert alarm.attrs["namespace"] == '"AWS/SQS"'
        assert alarm.attrs["statistic"] in {'"Maximum"', '"Average"'}
        assert alarm.attrs.get("treat_missing_data") != '"breaching"'
        assert re.fullmatch(r"[1-9][0-9]*", alarm.attrs["period"]), alarm.attrs["period"]
        assert int(alarm.attrs["evaluation_periods"]) >= 1
        assert alarm.attrs["alarm_actions"] == (
            f"concat(var.telemetry_ingress_alarm_actions, [{ALARM_TOPIC}.arn])"
        )
        assert (
            alarm.attrs.get("ok_actions", alarm.attrs["alarm_actions"])
            == (alarm.attrs["alarm_actions"])
        )
        assert "insufficient_data_actions" not in alarm.attrs
        assert "extended_statistic" not in alarm.attrs
        assert alarm.blocks == []
        dimensions = re.fullmatch(r"\{\s*QueueName\s*=\s*(\S+)\s*\}", alarm.attrs["dimensions"])
        assert dimensions is not None, alarm.attrs["dimensions"]
        key = (_unquote(alarm.attrs["metric_name"]), dimensions.group(1))
        assert key not in observed
        operator = _unquote(alarm.attrs["comparison_operator"])
        threshold = alarm.attrs["threshold"]
        # Age/depth may fire at or above the threshold; the DLQ alarm must fire on any message,
        # so ">= 1" is the same alarm as "> 0" but ">= 0" (always firing) is not.
        if operator == "GreaterThanOrEqualToThreshold" and threshold == "1":
            operator, threshold = "GreaterThanThreshold", "0"
        elif operator == "GreaterThanOrEqualToThreshold" and threshold != "0":
            operator = "GreaterThanThreshold"
        observed[key] = (operator, threshold)

    assert observed == {
        ("ApproximateAgeOfOldestMessage", f"{INGRESS_QUEUE}.name"): (
            "GreaterThanThreshold",
            "var.telemetry_ingress_age_alarm_threshold_seconds",
        ),
        ("ApproximateNumberOfMessagesVisible", f"{DLQ_QUEUE}.name"): (
            "GreaterThanThreshold",
            "0",
        ),
        ("ApproximateNumberOfMessagesVisible", f"{INGRESS_QUEUE}.name"): (
            "GreaterThanThreshold",
            "var.telemetry_ingress_depth_alarm_threshold",
        ),
    }


def test_ingestion_queues_tf_outputs_and_descriptions() -> None:
    outputs = {
        block.labels[0]: block for block in _tofu_blocks("outputs.tf") if block.kind == "output"
    }

    def null_safe(reference: str, attribute: str) -> set[str]:
        bare = reference.removesuffix("[0]")
        return {
            f"try({reference}.{attribute}, null)",
            f"one({bare}[*].{attribute})",
            f"var.telemetry_ingress ? {reference}.{attribute} : null",
        }

    for name, description, queue in (
        ("telemetry_ingress_queue_url", '"SQS_TELEMETRY_INGRESS_QUEUE_URL"', INGRESS_QUEUE),
        ("telemetry_ingress_dlq_url", '"SQS_TELEMETRY_INGRESS_DLQ_URL"', DLQ_QUEUE),
    ):
        assert set(outputs[name].attrs) == {"description", "value"}, name
        assert outputs[name].attrs["description"] == description, name
        assert outputs[name].attrs["value"] in null_safe(queue, "id"), name
    # Like api_iam_user_name / workers_iam_user_name: access keys are created out of band.
    assert outputs["telemetry_ingest_iam_user_name"].attrs["value"] in null_safe(
        "aws_iam_user.telemetry_ingest[0]", "name"
    )
    assert outputs["telemetry_ingress_alarm_topic_arn"].attrs["value"] in null_safe(
        ALARM_TOPIC, "arn"
    )
    new_outputs = {
        name for name in outputs if name.startswith(("telemetry_ingress", "telemetry_ingest"))
    }
    assert new_outputs == {
        "telemetry_ingress_queue_url",
        "telemetry_ingress_dlq_url",
        "telemetry_ingest_iam_user_name",
        "telemetry_ingress_alarm_topic_arn",
    }


def _statement_principal(statement: _HclBlock) -> tuple[str, list[str]]:
    (principals,) = statement.nested("principals")
    return principals.attrs["type"], _list_items(principals.attrs["identifiers"])


def _source_account_condition(statement: _HclBlock) -> None:
    (condition,) = statement.nested("condition")
    assert condition.attrs["test"] == '"StringEquals"'
    assert condition.attrs["variable"] == '"aws:SourceAccount"'
    assert _list_items(condition.attrs["values"]) == [
        "data.aws_caller_identity.telemetry_ingress_alarms[0].account_id"
    ]


def test_ingestion_queues_tf_alarm_topic_is_encrypted_and_only_cloudwatch_publishes() -> None:
    blocks = _ingestion_blocks()
    documents = _policy_documents(blocks)

    # CloudWatch cannot use the AWS-managed aws/sns key, so the topic has its own rotated key.
    (key,) = _resources(blocks, "aws_kms_key").values()
    assert key.attrs["enable_key_rotation"] == "true"
    assert key.attrs["policy"] == (
        "data.aws_iam_policy_document.telemetry_ingress_alarms_key[0].json"
    )
    (topic,) = _resources(blocks, "aws_sns_topic").values()
    assert topic.attrs["kms_master_key_id"] == "aws_kms_key.telemetry_ingress_alarms[0].arn"
    assert topic.attrs["name"] == (
        '"${var.project_name}-${var.environment}-telemetry-ingress-alarms"'
    )

    key_statements = {
        _unquote(s.attrs["sid"]): s
        for s in documents["telemetry_ingress_alarms_key"].nested("statement")
    }
    assert set(key_statements) == {"AccountAdministration", "CloudWatchAlarmPublish"}
    assert _statement_principal(key_statements["AccountAdministration"]) == (
        '"AWS"',
        ['"arn:aws:iam::${data.aws_caller_identity.telemetry_ingress_alarms[0].account_id}:root"'],
    )
    cloudwatch_key = key_statements["CloudWatchAlarmPublish"]
    assert _statement_principal(cloudwatch_key) == ('"Service"', ['"cloudwatch.amazonaws.com"'])
    assert [_unquote(a) for a in _list_items(cloudwatch_key.attrs["actions"])] == [
        "kms:Decrypt",
        "kms:GenerateDataKey*",
    ]
    _source_account_condition(cloudwatch_key)

    (topic_policy,) = _resources(blocks, "aws_sns_topic_policy").values()
    assert topic_policy.attrs["arn"] == f"{ALARM_TOPIC}.arn"
    assert topic_policy.attrs["policy"] == (
        "data.aws_iam_policy_document.telemetry_ingress_alarms_topic[0].json"
    )
    (publish,) = documents["telemetry_ingress_alarms_topic"].nested("statement")
    assert publish.attrs.get("effect", '"Allow"') == '"Allow"'
    assert _list_items(publish.attrs["actions"]) == ['"sns:Publish"']
    assert _list_items(publish.attrs["resources"]) == [f"{ALARM_TOPIC}.arn"]
    assert _statement_principal(publish) == ('"Service"', ['"cloudwatch.amazonaws.com"'])
    _source_account_condition(publish)

    (subscription,) = _resources(blocks, "aws_sns_topic_subscription").values()
    assert subscription.attrs == {
        "count": ALARM_EMAIL_GATE,
        "topic_arn": f"{ALARM_TOPIC}.arn",
        "protocol": '"email"',
        "endpoint": "var.telemetry_ingress_alarm_email",
    }
    # The real mailbox lives only in the untracked tfvars.
    tfvars = _read("env/cloud.tfvars.example")
    assert re.search(r'^telemetry_ingress_alarm_email\s+=\s+""$', tfvars, re.MULTILINE)


def test_ingestion_queues_tf_variables_declared_and_in_tfvars_example() -> None:
    variables = {
        block.labels[0]: block for block in _tofu_blocks("variables.tf") if block.kind == "variable"
    }
    tfvars = _read("env/cloud.tfvars.example")
    readme = _read("README.md")

    for name, (expected_type, expected_default) in TELEMETRY_VARIABLES.items():
        variable = variables[name]
        assert variable.attrs["type"] == expected_type, name
        assert "description" in variable.attrs, name
        assert "default" in variable.attrs, name
        if expected_default is not None:
            assert variable.attrs["default"] == expected_default, name
        assert re.search(rf"^{name}\s+=\s+\S", tfvars, re.MULTILINE), name
    assert re.search(r"^telemetry_ingress\s+=\s+false$", tfvars, re.MULTILINE)
    assert re.search(r"^telemetry_ingress_alarm_actions\s+=\s+\[\]$", tfvars, re.MULTILINE)
    assert re.search(
        r'^telemetry_ingress_queue_name\s+=\s+"limnopulse-telemetry-ingress"$',
        tfvars,
        re.MULTILINE,
    )
    assert "telemetry_ingress" in readme
    assert "telemetry_ingest" in readme


def test_ingestion_queues_tf_has_no_forbidden_words_or_secrets() -> None:
    ingestion = _read("ingestion_queues.tf")
    forbidden = re.compile("telegraf|mosquitto|mqtt-broker|dynamodb-local", re.IGNORECASE)
    secret = re.compile(
        r"AKIA[0-9A-Z]{16}|aws_secret_access_key|secret_access_key|PRIVATE KEY|"
        r"aws_iam_access_key|aws_secretsmanager_secret_version",
        re.IGNORECASE,
    )

    assert forbidden.search(ingestion) is None
    assert secret.search(ingestion) is None
    assert "telemetry_ingress" not in _read("queues.tf")
    assert "telemetry_ingress" not in _read("iam_runtime.tf")
    assert set(_resources(_tofu_blocks("queues.tf"), "aws_sqs_queue")) == {
        "notification_jobs",
        "notification_jobs_dlq",
        "telegram_notification_jobs",
        "telegram_notification_jobs_dlq",
        "ses_events",
        "ses_events_dlq",
        "ses_events_routing_dlq",
    }


def test_ingestion_queues_tf_hcl_reader_accepts_inline_comments() -> None:
    attrs, blocks = _parse_hcl_body(
        """
        statement { # trailing
          actions = [
            "dynamodb:GetItem", # resolve
            "dynamodb:Query",   // page
            /* block */ "dynamodb:UpdateItem",
          ]
          resources = [aws_dynamodb_table.domain.arn] // table only
          sid = "A#B" # hash inside a string stays
        }
        """
    )

    assert attrs == {}
    (statement,) = blocks
    assert [_unquote(item) for item in _list_items(statement.attrs["actions"])] == [
        "dynamodb:GetItem",
        "dynamodb:Query",
        "dynamodb:UpdateItem",
    ]
    assert _list_items(statement.attrs["resources"]) == ["aws_dynamodb_table.domain.arn"]
    assert statement.attrs["sid"] == '"A#B"'


# --- ADR-020 S6: per-role worker principals, enable variables and alarms -------------------
#
# ops/workers/roles.yaml is the source: every role's iam_principal, enable_variable and alarms,
# and the table's unit_health and standalone_alarms, must be what workers.tf and
# worker_alarms.tf declare. test_iam_runtime_transactions.py checks the principals' grants.

WORKER_ALARM_ACTIONS = "concat(var.worker_alarm_actions, [aws_sns_topic.worker_alarms.arn])"
WORKER_ALARM_TOPIC = "aws_sns_topic.worker_alarms"
# A role's principal (and so its alarms) exists only with the delivery flag it serves.
WORKER_PRINCIPAL_GATES = {
    "email_worker": "var.email_delivery ? 1 : 0",
    "ses_feedback_worker": "var.email_delivery ? 1 : 0",
    "telegram_worker": "var.telegram_delivery ? 1 : 0",
}
WORKER_TOPIC_EMAIL_GATE = 'var.worker_alarm_email != "" ? 1 : 0'


def _worker_roles() -> list[dict[str, Any]]:
    from tests.support.worker_roles import load_roles

    return load_roles()["roles"]


def _worker_principals() -> dict[str, _HclBlock]:
    return {
        block.labels[1]: block
        for block in _tofu_blocks("workers.tf")
        if block.kind == "resource" and block.labels[0] in {"aws_iam_user", "aws_iam_role"}
    }


def _all_blocks() -> list[_HclBlock]:
    return [
        block
        for path in sorted(TOFU_ROOT.glob("*.tf"))
        for block in _parse_hcl_body(path.read_text(encoding="utf-8"))[1]
    ]


def test_worker_enable_variables_are_false_bools_for_every_unit_role() -> None:
    variables = {
        block.labels[0]: block for block in _tofu_blocks("variables.tf") if block.kind == "variable"
    }
    tfvars = _read("env/cloud.tfvars.example")
    units = [role for role in _worker_roles() if role["lifecycle"] in {"continuous", "oneshot"}]

    enable = {role["enable_variable"] for role in units}
    assert enable == {name for name in ADR020_VARIABLES if name.endswith("_enabled")}
    for name in enable:
        assert variables[name].attrs["type"] == "bool", name
        assert variables[name].attrs["default"] == "false", name
        assert "description" in variables[name].attrs, name
        assert re.search(rf"^{name}\s+=\s+false$", tfvars, re.MULTILINE), name
    for name in ADR020_VARIABLES:
        assert name in variables, name
        assert re.search(rf"^{name}\s+=\s+\S", tfvars, re.MULTILINE), name
    assert variables["worker_alarm_actions"].attrs["default"] == "[]"
    assert re.search(r'^worker_alarm_email\s+=\s+""$', tfvars, re.MULTILINE)
    assert variables["limnopulse_admin_trusted_principal_arns"].attrs["default"] == "[]"
    assert re.search(r"^limnopulse_admin_trusted_principal_arns\s+=\s+\[\]$", tfvars, re.MULTILINE)


def test_worker_principals_are_one_per_go_role_and_gated_with_their_channel() -> None:
    principals = _worker_principals()
    go_roles = [role for role in _worker_roles() if role["entrypoint"].startswith("cmd/")]

    assert set(principals) == {role["iam_principal"] for role in go_roles}
    for role in go_roles:
        principal = principals[role["iam_principal"]]
        # The operator CLI has a role it assumes for one run; every unit has a user.
        kind = "aws_iam_role" if role["lifecycle"] == "cli" else "aws_iam_user"
        assert principal.labels[0] == kind, role["name"]
        assert (
            principal.attrs["name"]
            == f'"${{var.project_name}}-${{var.environment}}-{role["name"]}"'
        )
        assert principal.attrs["tags"] == "local.common_tags"
        assert principal.attrs.get("count") == WORKER_PRINCIPAL_GATES.get(role["iam_principal"])
        assert "permissions_boundary" not in principal.attrs
        if kind == "aws_iam_user":
            assert principal.attrs["force_destroy"] == "true", role["name"]


def test_the_admin_role_trusts_only_this_account() -> None:
    blocks = _tofu_blocks("workers.tf")
    (admin,) = _resources(blocks, "aws_iam_role").values()
    assert admin.attrs["assume_role_policy"] == (
        "data.aws_iam_policy_document.limnopulse_admin_trust.json"
    )
    assert int(admin.attrs["max_session_duration"]) <= 3600
    (statement,) = _policy_documents(blocks)["limnopulse_admin_trust"].nested("statement")
    assert set(statement.attrs) == {"sid", "effect", "actions"}
    assert statement.attrs["effect"] == '"Allow"'
    assert _list_items(statement.attrs["actions"]) == ['"sts:AssumeRole"']
    assert [block.kind for block in statement.blocks] == ["principals", "condition"]
    assert _statement_principal(statement) == (
        '"AWS"',
        ['"arn:aws:iam::${data.aws_caller_identity.workers.account_id}:root"'],
    )
    # aws:PrincipalArn narrows the account to the operators' list; unset, it allows exactly
    # what the root trust allows.
    (condition,) = statement.nested("condition")
    assert condition.attrs == {
        "test": '"ArnLike"',
        "variable": '"aws:PrincipalArn"',
        "values": "local.limnopulse_admin_trusted_principal_arns",
    }
    (local,) = [block for block in blocks if block.kind == "locals"]
    assert re.sub(r"\s+", " ", local.attrs["limnopulse_admin_trusted_principal_arns"]) == (
        "( length(var.limnopulse_admin_trusted_principal_arns) > 0"
        " ? var.limnopulse_admin_trusted_principal_arns"
        ' : ["arn:aws:iam::${data.aws_caller_identity.workers.account_id}:*"] )'
    )


def test_no_access_key_is_managed_and_the_shared_workers_user_is_deprecated() -> None:
    # Keys are created out of band (README.md, "Worker roles"); none ever reaches the state.
    for block in _all_blocks():
        assert block.labels[:1] != ("aws_iam_access_key",), block.labels
        assert not any(re.search(r"aws_iam_access_key|\.secret\b", v) for v in block.attrs.values())
    iam_runtime = _read("iam_runtime.tf")
    assert re.search(
        r'^# DEPRECATED \(ADR-020\)\..*\n(?:#.*\n)*resource "aws_iam_user" "workers" \{',
        iam_runtime,
        re.MULTILINE,
    )
    readme = _read("README.md")
    assert "aws_iam_user.workers" in readme


def _unit_health_attrs(alarm: _HclBlock, role: dict[str, Any], health: dict[str, Any]) -> None:
    attrs = alarm.attrs
    assert attrs["namespace"] == f'"{health["namespace"]}"'
    assert attrs["metric_name"] == f'"{health["metric"]}"'
    assert re.fullmatch(
        rf'\{{\s*{health["dimension"]}\s*=\s*"{role["name"]}"\s*\}}', attrs["dimensions"]
    )
    assert attrs["treat_missing_data"] == f'"{health["treat_missing_data"]}"' == '"breaching"'
    # The monitor publishes UnitHealthy with no unit; an alarm unit would match no datapoint.
    assert "unit" not in attrs
    assert attrs["statistic"] == '"Maximum"'
    assert (attrs["comparison_operator"], attrs["threshold"]) == ('"LessThanThreshold"', "1")
    period = int(attrs["period"])
    evaluations = int(attrs["evaluation_periods"])
    datapoints = int(attrs["datapoints_to_alarm"])
    # M of N: one bad period never fires (the monitor publishes 0 for short spells by design),
    # and one missing period inside the window is tolerated (timer jitter).
    assert period >= 60 and period % 60 == 0
    assert 2 <= datapoints < evaluations


def _queue_alarm_attrs(
    alarm: _HclBlock, kind: str, queue: str, queues: dict[str, _HclBlock]
) -> None:
    attrs = alarm.attrs
    reference = f"aws_sqs_queue.{queue}{'[0]' if 'count' in queues[queue].attrs else ''}"
    if kind == "queue-dlq":
        redrive = _jsonencode_fields(queues[queue].attrs["redrive_policy"])
        dlq = re.fullmatch(r"aws_sqs_queue\.(\w+)(\[0\])?\.arn", redrive["deadLetterTargetArn"])
        assert dlq is not None
        reference = f"aws_sqs_queue.{dlq.group(1)}{dlq.group(2) or ''}"
    metric, threshold = {
        "queue-age": (
            "ApproximateAgeOfOldestMessage",
            "var.worker_queue_age_alarm_threshold_seconds",
        ),
        "queue-depth": (
            "ApproximateNumberOfMessagesVisible",
            "var.worker_queue_depth_alarm_threshold",
        ),
        "queue-dlq": ("ApproximateNumberOfMessagesVisible", "0"),
    }[kind]
    assert attrs["namespace"] == '"AWS/SQS"'
    assert attrs["metric_name"] == f'"{metric}"'
    assert re.fullmatch(
        rf"\{{\s*QueueName\s*=\s*{re.escape(reference)}\.name\s*\}}", attrs["dimensions"]
    )
    assert (attrs["comparison_operator"], attrs["threshold"]) == (
        '"GreaterThanThreshold"',
        threshold,
    )
    assert attrs["statistic"] == '"Maximum"'
    assert attrs["treat_missing_data"] == '"notBreaching"'
    assert int(attrs["evaluation_periods"]) >= 1


def test_worker_alarms_are_exactly_the_role_table_alarms() -> None:
    from tests.support.worker_roles import load_roles

    table = load_roles()
    alarms = _resources(_tofu_blocks("worker_alarms.tf"), "aws_cloudwatch_metric_alarm")
    principals = _worker_principals()
    queues = _resources(_all_blocks(), "aws_sqs_queue")

    expected: set[str] = set()
    for role in table["roles"]:
        if role["lifecycle"] == "compose":
            continue  # the ingest worker's queue alarms live in ingestion_queues.tf
        gate = principals[role["iam_principal"]].attrs.get("count")
        consumed = [queue["queue"] for queue in role["queues"] if queue["access"] == "consume"]
        for kind in role["alarms"]:
            label = f"{role['iam_principal']}_{kind.replace('-', '_')}"
            expected.add(label)
            alarm = alarms[label]
            assert alarm.attrs.get("count") == gate, label
            assert alarm.attrs["alarm_name"] == (
                f'"${{var.project_name}}-${{var.environment}}-{role["name"]}-{kind}"'
            )
            assert alarm.attrs["actions_enabled"] == f"var.{role['enable_variable']}", label
            assert alarm.attrs["alarm_actions"] == WORKER_ALARM_ACTIONS, label
            assert alarm.attrs["ok_actions"] == WORKER_ALARM_ACTIONS, label
            assert "insufficient_data_actions" not in alarm.attrs, label
            assert "extended_statistic" not in alarm.attrs, label
            assert alarm.blocks == [], label
            if kind == "unit-health":
                _unit_health_attrs(alarm, role, table["unit_health"])
            else:
                (queue,) = consumed
                assert queues[queue].attrs.get("count") in {None, gate}, label
                _queue_alarm_attrs(alarm, kind, queue, queues)
    for standalone in table["standalone_alarms"]:
        label = standalone["name"].replace("-", "_")
        expected.add(label)
        alarm = alarms[label]
        # It belongs to no role: its gate is the routing's, and it always acts.
        assert alarm.attrs.get("count") == f"var.{standalone['gate_variable']} ? 1 : 0"
        assert "actions_enabled" not in alarm.attrs
        assert alarm.attrs["alarm_actions"] == WORKER_ALARM_ACTIONS
        queue = standalone["queue"]
        assert re.fullmatch(
            rf"\{{\s*QueueName\s*=\s*aws_sqs_queue\.{queue}\[0\]\.name\s*\}}",
            alarm.attrs["dimensions"],
        )
        assert alarm.attrs["metric_name"] == '"ApproximateNumberOfMessagesVisible"'
        assert (alarm.attrs["comparison_operator"], alarm.attrs["threshold"]) == (
            '"GreaterThanThreshold"',
            "0",
        )
    assert set(alarms) == expected


def test_worker_alarm_topic_is_encrypted_and_only_cloudwatch_publishes() -> None:
    blocks = _tofu_blocks("worker_alarms.tf")
    documents = _policy_documents(blocks)
    account = "data.aws_caller_identity.workers.account_id"

    (key,) = _resources(blocks, "aws_kms_key").values()
    assert key.attrs["enable_key_rotation"] == "true"
    assert key.attrs["policy"] == "data.aws_iam_policy_document.worker_alarms_key.json"
    (topic,) = _resources(blocks, "aws_sns_topic").values()
    assert topic.attrs["kms_master_key_id"] == "aws_kms_key.worker_alarms.arn"
    assert topic.attrs["name"] == '"${var.project_name}-${var.environment}-worker-alarms"'

    statements = {
        _unquote(s.attrs["sid"]): s for s in documents["worker_alarms_key"].nested("statement")
    }
    assert set(statements) == {"AccountAdministration", "CloudWatchAlarmPublish"}
    assert _statement_principal(statements["AccountAdministration"]) == (
        '"AWS"',
        [f'"arn:aws:iam::${{{account}}}:root"'],
    )
    publish_key = statements["CloudWatchAlarmPublish"]
    assert _statement_principal(publish_key) == ('"Service"', ['"cloudwatch.amazonaws.com"'])
    assert [_unquote(a) for a in _list_items(publish_key.attrs["actions"])] == [
        "kms:Decrypt",
        "kms:GenerateDataKey*",
    ]
    for statement in (publish_key, *documents["worker_alarms_topic"].nested("statement")):
        (condition,) = statement.nested("condition")
        assert (condition.attrs["test"], condition.attrs["variable"]) == (
            '"StringEquals"',
            '"aws:SourceAccount"',
        )
        assert _list_items(condition.attrs["values"]) == [account]

    (topic_policy,) = _resources(blocks, "aws_sns_topic_policy").values()
    assert topic_policy.attrs["arn"] == f"{WORKER_ALARM_TOPIC}.arn"
    (publish,) = documents["worker_alarms_topic"].nested("statement")
    assert _list_items(publish.attrs["actions"]) == ['"sns:Publish"']
    assert _list_items(publish.attrs["resources"]) == [f"{WORKER_ALARM_TOPIC}.arn"]
    assert _statement_principal(publish) == ('"Service"', ['"cloudwatch.amazonaws.com"'])

    (subscription,) = _resources(blocks, "aws_sns_topic_subscription").values()
    assert subscription.attrs == {
        "count": WORKER_TOPIC_EMAIL_GATE,
        "topic_arn": f"{WORKER_ALARM_TOPIC}.arn",
        "protocol": '"email"',
        "endpoint": "var.worker_alarm_email",
    }


def test_an_enabled_role_needs_its_delivery_flag() -> None:
    # A gated role's alarms do not exist without its flag, so the check sits on the topic.
    (topic,) = _resources(_tofu_blocks("worker_alarms.tf"), "aws_sns_topic").values()
    (lifecycle,) = topic.nested("lifecycle")
    conditions = {block.attrs["condition"] for block in lifecycle.nested("precondition")}
    expected = set()
    for role in _worker_roles():
        gate = WORKER_PRINCIPAL_GATES.get(role["iam_principal"])
        if gate is not None:
            flag = re.fullmatch(r"(var\.\w+) \? 1 : 0", gate).group(1)
            expected.add(f"!var.{role['enable_variable']} || {flag}")
    assert conditions == expected


def test_worker_outputs_name_principals_and_never_a_secret() -> None:
    outputs = {
        block.labels[0]: block for block in _tofu_blocks("outputs.tf") if block.kind == "output"
    }
    for role in _worker_roles():
        principal = role["iam_principal"]
        if not role["entrypoint"].startswith("cmd/"):
            continue
        if role["lifecycle"] == "cli":
            assert (
                outputs[f"{principal}_role_arn"].attrs["value"] == f"aws_iam_role.{principal}.arn"
            )
            continue
        value = outputs[f"{principal}_iam_user_name"].attrs["value"]
        if principal in WORKER_PRINCIPAL_GATES:
            assert value == f"try(aws_iam_user.{principal}[0].name, null)", principal
        else:
            assert value == f"aws_iam_user.{principal}.name", principal
    assert outputs["worker_alarm_topic_arn"].attrs["value"] == f"{WORKER_ALARM_TOPIC}.arn"
    assert {name for name in outputs if name in ADR020_OUTPUTS} == ADR020_OUTPUTS
