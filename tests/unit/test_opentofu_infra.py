import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
TOFU_ROOT = ROOT / "infra" / "opentofu"


def _read(relative_path: str) -> str:
    return (TOFU_ROOT / relative_path).read_text(encoding="utf-8")


def _opentofu_text_files() -> list[Path]:
    return [
        path
        for path in TOFU_ROOT.rglob("*")
        if path.is_file() and ".terraform" not in path.parts
    ]


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

    assert {str(path.relative_to(TOFU_ROOT)).replace("\\", "/") for path in TOFU_ROOT.rglob("*") if path.is_file()} >= expected_files

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


def test_cognito_resources_export_application_environment_contract() -> None:
    cognito = _read("cognito.tf")
    outputs = _read("outputs.tf")

    assert 'resource "aws_cognito_user_pool" "main"' in cognito
    assert 'resource "aws_cognito_user_pool_client" "api"' in cognito
    assert 'auto_verified_attributes = ["email"]' in cognito
    assert 'generate_secret = false' in cognito
    assert '"ALLOW_USER_SRP_AUTH"' in cognito
    assert 'output "cognito_user_pool_id"' in outputs
    assert 'output "cognito_client_id"' in outputs
    assert 'output "cognito_issuer"' in outputs
    assert 'https://cognito-idp.${var.aws_region}.amazonaws.com/${aws_cognito_user_pool.main.id}' in outputs
    assert "Cloud Redis endpoint placeholder only" in outputs
    assert "Cloud InfluxDB endpoint placeholder only" in outputs


def test_sqs_dlq_and_optional_ses_scaffold_are_safe_by_default() -> None:
    queues = _read("queues.tf")
    ses = _read("ses.tf")
    variables = _read("variables.tf")
    tfvars = _read("env/cloud.tfvars.example")

    assert 'resource "aws_sqs_queue" "alerts_dlq"' in queues
    assert 'resource "aws_sqs_queue" "alerts"' in queues
    assert "redrive_policy = jsonencode" in queues
    assert "deadLetterTargetArn" in queues
    assert "maxReceiveCount" in queues
    assert 'resource "aws_sqs_queue_redrive_allow_policy" "alerts_dlq"' in queues
    assert 'resource "aws_ses_email_identity" "notifications"' in ses
    assert 'count = var.ses_email_identity == "" ? 0 : 1' in ses
    assert 'variable "ses_email_identity"' in variables
    assert 'ses_email_identity = ""' in tfvars


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
