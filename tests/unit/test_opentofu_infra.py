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
IAM_RUNTIME_SHA256 = "1b928edce92709b8a3790e207edb2a0d442dc7d710af608a7e0a606b7d879fa1"
INGRESS_QUEUE = "aws_sqs_queue.telemetry_ingress[0]"
DLQ_QUEUE = "aws_sqs_queue.telemetry_ingress_dlq[0]"
TELEMETRY_VARIABLES = {
    "telemetry_ingress": ("bool", "false"),
    "telemetry_ingress_queue_name": ("string", '"limnopulse-telemetry-ingress"'),
    "telemetry_ingress_dlq_name": ("string", '"limnopulse-telemetry-ingress-dlq"'),
    "telemetry_ingress_age_alarm_threshold_seconds": ("number", "300"),
    "telemetry_ingress_depth_alarm_threshold": ("number", None),
    "telemetry_ingress_alarm_actions": ("list(string)", "[]"),
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
        if char == "#":
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
    head = re.compile(r'\s*(?:(?:#|//)[^\n]*\n\s*)*([A-Za-z_][\w-]*)\s*((?:"[^"]*"\s*)*)(=|\{)')
    index = 0
    while True:
        match = head.match(text, index)
        if match is None:
            assert text[index:].strip() == "" or re.fullmatch(
                r"(\s*(#|//)[^\n]*)*\s*", text[index:]
            ), text[index : index + 80]
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


def _list_items(expression: str) -> list[str]:
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


def _grants(document: _HclBlock) -> set[tuple[str, str, str]]:
    grants: set[tuple[str, str, str]] = set()
    for statement in document.nested("statement"):
        assert statement.attrs.get("effect", '"Allow"') == '"Allow"'
        assert statement.nested("principals") == []
        assert statement.nested("condition") == []
        assert "not_actions" not in statement.attrs
        assert "not_resources" not in statement.attrs
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
    attachments = _resources(blocks, "aws_iam_user_policy_attachment")
    by_user: dict[str, set[tuple[str, str, str]]] = {}
    attached: set[str] = set()
    for attachment in attachments.values():
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
        ("data", "aws_iam_policy_document"),
    }

    assert blocks
    for block in blocks:
        assert (block.kind, block.labels[0]) in allowed, block.labels
        assert block.attrs.get("count") == INGESTION_GATE, block.labels

    text = _read("ingestion_queues.tf")
    gated_names = {block.labels[1] for block in blocks}
    for reference in re.finditer(r"\b(aws_[a-z0-9_]+)\.(\w+)(\[0\])?", text):
        if reference.group(2) in gated_names:
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
    assert _jsonencode_fields(policy.attrs["redrive_allow_policy"]) == {
        "redrivePermission": '"byQueue"',
        "sourceQueueArns": f"[{INGRESS_QUEUE}.arn]",
    }


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
        statements = documents[document.group(1)].nested("statement")
        assert len(statements) == 1
        (statement,) = statements
        assert statement.attrs["effect"] == '"Deny"'
        assert _list_items(statement.attrs["actions"]) == ['"sqs:*"']
        assert _list_items(statement.attrs["resources"]) == [f"{queue.group(1)}.arn"]
        (principals,) = statement.nested("principals")
        assert principals.attrs["type"] == '"*"'
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
    attachment_types = {
        "aws_iam_user_policy_attachment",
        "aws_iam_role_policy_attachment",
        "aws_iam_group_policy_attachment",
        "aws_iam_policy_attachment",
        "aws_iam_user_policy_attachments_exclusive",
        "aws_iam_role_policy_attachments_exclusive",
        "aws_iam_group_policy_attachments_exclusive",
    }

    for path in TOFU_ROOT.glob("*.tf"):
        for block in _parse_hcl_body(path.read_text(encoding="utf-8"))[1]:
            if block.kind != "resource" or block.labels[0] not in attachment_types:
                continue
            references = " ".join(block.attrs.values())
            assert f"aws_iam_policy.{operator}" not in references, path
            if path.name != "iam_runtime.tf":
                assert "aws_iam_user.workers" not in references, path

    ingestion = _read("ingestion_queues.tf")
    assert "aws_iam_user.workers" not in ingestion
    for forbidden in (
        '"aws_iam_user_policy"',
        '"aws_iam_group',
        '"aws_iam_role',
        '"aws_iam_access_key"',
        '"aws_iam_user_group_membership"',
    ):
        assert forbidden not in ingestion
    assert hashlib.sha256((TOFU_ROOT / "iam_runtime.tf").read_bytes()).hexdigest() == (
        IAM_RUNTIME_SHA256
    )


def test_ingestion_queues_tf_alarms_cover_age_dlq_and_depth() -> None:
    alarms = _resources(_ingestion_blocks(), "aws_cloudwatch_metric_alarm")

    observed: dict[tuple[str, str], tuple[str, str]] = {}
    for alarm in alarms.values():
        assert alarm.attrs["namespace"] == '"AWS/SQS"'
        assert alarm.attrs["alarm_actions"] == "var.telemetry_ingress_alarm_actions"
        dimensions = re.fullmatch(r"\{\s*QueueName\s*=\s*(\S+)\s*\}", alarm.attrs["dimensions"])
        assert dimensions is not None, alarm.attrs["dimensions"]
        key = (_unquote(alarm.attrs["metric_name"]), dimensions.group(1))
        assert key not in observed
        observed[key] = (_unquote(alarm.attrs["comparison_operator"]), alarm.attrs["threshold"])

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

    assert outputs["telemetry_ingress_queue_url"].attrs == {
        "description": '"SQS_TELEMETRY_INGRESS_QUEUE_URL"',
        "value": f"try({INGRESS_QUEUE}.id, null)",
    }
    assert outputs["telemetry_ingress_dlq_url"].attrs == {
        "description": '"SQS_TELEMETRY_INGRESS_DLQ_URL"',
        "value": f"try({DLQ_QUEUE}.id, null)",
    }


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
