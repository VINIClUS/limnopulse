from __future__ import annotations

import os
import re
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
INFLUX_IMAGE = (
    "influxdb:2.7.11@sha256:d92a10e9e75aff18eca38ff3a8f0b4a800706a5dd44d1b0ece264af04458525b"
)
ELASTICMQ_IMAGE = (
    "softwaremill/elasticmq-native:1.6.16"
    "@sha256:8383f29ad746c12981ca2128f689a4b4a93815235f1a7851d2f165316503d132"
)
DYNAMODB_LOCAL_IMAGE = (
    "amazon/dynamodb-local:3.3.2"
    "@sha256:6ada631ec7aa5fc83446855eb17233e3dad184fec47477e8140c6c569cfe0c86"
)
ELASTICMQ_JAR_SHA256 = "30a81f56a40b670fafc3f32b0f09f3baf59ad2645edaf17bc2e79170c75479f2"
DYNAMODB_LOCAL_SHA256 = "7a94eb65f81aee270738751a4e5e3e6a7ad02a5d25194f55b83e08ec2ba6342b"
INFLUX_BACKED_JOBS = ("influx", "ingestion")
INGESTION_RECIPE = [
    "uv sync --locked --extra dev",
    "RUN_INGESTION_INTEGRATION=1 RUN_INFLUX_INTEGRATION=1 uv run --locked --no-sync python -m pytest -q tests/ingestion",
]


def _workflow() -> tuple[str, dict[str, Any]]:
    text = (ROOT / ".github/workflows/verify.yml").read_text(encoding="utf-8")
    return text, yaml.load(text, Loader=yaml.BaseLoader)


def _strings(node: Any) -> list[str]:
    if isinstance(node, dict):
        return [s for key, value in node.items() for s in (key, *_strings(value))]
    if isinstance(node, list):
        return [s for item in node for s in _strings(item)]
    return [node] if isinstance(node, str) else []


def test_verify_workflow_is_read_only_and_credential_free() -> None:
    text = (ROOT / ".github/workflows/verify.yml").read_text(encoding="utf-8")

    assert "pull_request:" in text
    assert "branches: [main]" in text
    assert "contents: read" in text
    assert 'AWS_EC2_METADATA_DISABLED: "true"' in text

    lowered = text.lower()
    for forbidden in (
        "secrets.",
        "aws_access_key_id",
        "aws_secret_access_key",
        "tofu plan",
        "tofu apply",
        "docker compose up",
    ):
        assert forbidden not in lowered

    action_refs = re.findall(r"^\s*-\s+uses:\s+(\S+)", text, flags=re.MULTILINE)
    assert action_refs
    assert all(re.fullmatch(r"[^@\s]+@[0-9a-f]{40}", ref) for ref in action_refs)
    assert {ref.partition("@")[0] for ref in action_refs} == {
        "actions/checkout",
        "actions/setup-node",
        "actions/setup-go",
        "actions/setup-python",
        "astral-sh/setup-uv",
        "opentofu/setup-opentofu",
    }

    pins: dict[str, set[str]] = {}
    for ref in action_refs:
        name, _, sha = ref.partition("@")
        pins.setdefault(name, set()).add(sha)
    assert all(len(shas) == 1 for shas in pins.values()), pins

    workflow = yaml.load(text, Loader=yaml.BaseLoader)
    assert set(workflow["jobs"]) == {
        "python",
        "go",
        "opentofu",
        "compose",
        "frontend",
        "influx",
        "ingestion",
    }
    expected_jobs = {
        "python": (
            ["actions/checkout", "actions/setup-python", "astral-sh/setup-uv"],
            "verify-python",
        ),
        "go": (["actions/checkout", "actions/setup-go"], "verify-go"),
        "opentofu": (["actions/checkout", "opentofu/setup-opentofu"], "verify-tofu"),
        "compose": (["actions/checkout"], "verify-compose"),
        "influx": (
            [
                "actions/checkout",
                "actions/setup-python",
                "astral-sh/setup-uv",
                "actions/setup-go",
            ],
            "verify-influx",
        ),
        "ingestion": (
            ["actions/checkout", "actions/setup-python", "astral-sh/setup-uv"],
            "verify-ingestion",
        ),
    }
    for job_name, (expected_actions, target) in expected_jobs.items():
        steps = workflow["jobs"][job_name]["steps"]
        actions = [step["uses"].partition("@")[0] for step in steps if "uses" in step]
        runs = [step["run"] for step in steps if "run" in step]
        if job_name in INFLUX_BACKED_JOBS:
            # Setup order is not locked for the service-backed jobs; the action set is.
            assert sorted(actions) == sorted(expected_actions)
        else:
            assert actions == expected_actions
        assert runs == [f"make {target}"]

    frontend_steps = workflow["jobs"]["frontend"]["steps"]
    frontend_actions = [step["uses"].partition("@")[0] for step in frontend_steps if "uses" in step]
    frontend_runs = [step["run"] for step in frontend_steps if "run" in step]
    assert frontend_actions == ["actions/checkout", "actions/setup-node"]
    assert frontend_runs == ["npm ci", "npm test", "npm run typecheck", "npm run build"]

    assert [name for name, job in workflow["jobs"].items() if "services" in job] == [
        "influx",
        "ingestion",
    ]


def test_influx_job_uses_a_throwaway_influxdb_service_with_the_harness_defaults() -> None:
    text = (ROOT / ".github/workflows/verify.yml").read_text(encoding="utf-8")
    workflow = yaml.load(text, Loader=yaml.BaseLoader)
    job = workflow["jobs"]["influx"]

    services = list(job["services"].values())
    assert len(services) == 1
    service = services[0]
    # A digest-pinned true 2.7.11; the 2.7.11-alpine tag drifted to 2.7.12.
    assert service["image"] == (
        "influxdb:2.7.11@sha256:d92a10e9e75aff18eca38ff3a8f0b4a800706a5dd44d1b0ece264af04458525b"
    )
    assert service["ports"] in (["8086:8086"], ["127.0.0.1:8086:8086"])

    env = service["env"]
    assert env["DOCKER_INFLUXDB_INIT_MODE"] == "setup"
    assert env["DOCKER_INFLUXDB_INIT_ORG"] == "limnopulse"
    assert env["DOCKER_INFLUXDB_INIT_ADMIN_TOKEN"] == "local-dev-token"
    for required in (
        "DOCKER_INFLUXDB_INIT_USERNAME",
        "DOCKER_INFLUXDB_INIT_PASSWORD",
        "DOCKER_INFLUXDB_INIT_BUCKET",
    ):
        assert env[required]

    options = " ".join(service["options"].split())
    assert '--health-cmd "influx ping"' in options

    # Readiness comes from the service health check; the job runs nothing else.
    assert [step["run"] for step in job["steps"] if "run" in step] == ["make verify-influx"]
    # The harness defaults already match the service; no job-level overrides.
    assert "env" not in job
    for step in job["steps"]:
        assert "env" not in step

    # The dev token and password exist only in the throwaway service containers: exactly once
    # in each Influx-backed job and nowhere else.
    assert_once_per_influx_backed_job(text, workflow, "local-dev-token")
    assert_once_per_influx_backed_job(text, workflow, env["DOCKER_INFLUXDB_INIT_PASSWORD"])


def assert_once_per_influx_backed_job(text: str, workflow: dict[str, Any], value: str) -> None:
    for name, job in workflow["jobs"].items():
        expected = 1 if name in INFLUX_BACKED_JOBS else 0
        assert sum(s.count(value) for s in _strings(job)) == expected, (name, value)
    assert text.count(value) == len(INFLUX_BACKED_JOBS), value


def test_ingestion_job_runs_the_gate_against_pinned_emulators_and_influxdb() -> None:
    text, workflow = _workflow()
    job = workflow["jobs"]["ingestion"]

    services = {service["image"]: service for service in job["services"].values()}
    assert set(services) == {ELASTICMQ_IMAGE, DYNAMODB_LOCAL_IMAGE, INFLUX_IMAGE}
    assert len(job["services"]) == 3

    assert services[ELASTICMQ_IMAGE]["ports"] in (["9324:9324"], ["127.0.0.1:9324:9324"])
    # DynamoDB Local listens on 8000 in the container; the harness default is 8001.
    assert services[DYNAMODB_LOCAL_IMAGE]["ports"] in (["8001:8000"], ["127.0.0.1:8001:8000"])

    # The InfluxDB service is the influx job's service: same digest, onboarding and health.
    influx = services[INFLUX_IMAGE]
    influx_job_service = next(iter(workflow["jobs"]["influx"]["services"].values()))
    assert influx["ports"] in (["8086:8086"], ["127.0.0.1:8086:8086"])
    assert influx["env"] == influx_job_service["env"]
    assert " ".join(influx["options"].split()) == " ".join(influx_job_service["options"].split())

    for service in services.values():
        image, _, digest = service["image"].partition("@")
        assert ":" in image and re.fullmatch(r"sha256:[0-9a-f]{64}", digest)
        assert "credentials" not in service
        assert "volumes" not in service

    assert [step["run"] for step in job["steps"] if "run" in step] == ["make verify-ingestion"]
    # The harness defaults (endpoints, dummy credentials) already match the services.
    assert "env" not in job
    for step in job["steps"]:
        assert "env" not in step
    for name in ("aws_access_key_id", "aws_secret_access_key", "aws_session_token"):
        assert name not in text.lower()


def test_makefile_exposes_safe_reproducible_targets() -> None:
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    expected = {
        "verify-python": [
            "uv lock --check",
            "uv sync --locked --extra dev",
            "uv run --locked --no-sync python -m pytest -q",
        ],
        "verify-go": ["go test -race ./..."],
        "verify-tofu": [
            "tofu -chdir=infra/opentofu init -backend=false -input=false",
            "tofu -chdir=infra/opentofu fmt -check -recursive",
            "tofu -chdir=infra/opentofu validate -no-color",
        ],
        "verify-compose": ["docker compose config --quiet"],
        "verify-compose-production": [
            "IMAGE_TAG=main-0000000000000000000000000000000000000000 docker compose --env-file .env.production.example -f compose.production.yaml config --quiet"
        ],
    }
    influx_recipe = [
        "uv sync --locked --extra dev",
        "RUN_INFLUX_INTEGRATION=1 uv run --locked --no-sync python -m pytest -q tests/influx",
        "RUN_INFLUX_INTEGRATION=1 go test -race -count=1 -run 'TestInfluxIntegration' ./...",
    ]

    phony_match = re.search(r"^\.PHONY:\s+(.+)$", makefile, flags=re.MULTILINE)
    assert phony_match is not None
    assert set(phony_match.group(1).split()) == {
        "verify",
        "verify-influx",
        "verify-ingestion",
        *expected,
    }

    verify_rule = re.search(r"^verify:(.*)$", makefile, flags=re.MULTILINE)
    assert verify_rule is not None
    assert verify_rule.group(1).split() == list(expected)

    for target, commands in {
        **expected,
        "verify-influx": influx_recipe,
        "verify-ingestion": INGESTION_RECIPE,
    }.items():
        result = subprocess.run(
            ["make", "--no-print-directory", "-n", "UV=uv", "TOFU=tofu", target],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.splitlines() == commands

    aggregate = subprocess.run(
        ["make", "--no-print-directory", "-n", "UV=uv", "TOFU=tofu", "verify"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert aggregate.returncode == 0, aggregate.stderr
    assert aggregate.stdout.splitlines() == [
        command for commands in expected.values() for command in commands
    ]

    lowered = makefile.lower()
    for forbidden in ("tofu plan", "tofu apply", "docker compose up", "local-dev-token"):
        assert forbidden not in lowered


def test_verification_docs_define_the_non_production_boundary() -> None:
    text = (ROOT / "docs/verification.md").read_text(encoding="utf-8")
    lowered = text.lower()

    for target in (
        "make verify-python",
        "make verify-go",
        "make verify-tofu",
        "make verify-compose",
        "make verify",
    ):
        assert f"`{target}`" in text

    assert "tests/integration/test_notifications_local.py" in text
    assert "RUN_NOTIFICATION_INTEGRATION=1" in text
    assert "backend" in lowered and "disabled" in lowered
    assert "compose" in lowered and "parse" in lowered
    assert "does not start" in lowered
    assert "aws credentials" in lowered
    assert "sandbox" in lowered
    assert "production readiness" in lowered
    assert "later gate" in lowered


def test_verify_ingestion_help_text_says_to_start_the_local_runners_first() -> None:
    lines = (ROOT / "Makefile").read_text(encoding="utf-8").splitlines()
    target = lines.index(next(line for line in lines if line.startswith("verify-ingestion:")))
    comments: list[str] = []
    for line in reversed(lines[:target]):
        if not line.startswith("#"):
            break
        comments.insert(0, line)
    help_text = " ".join(comments)

    for runner in (
        "scripts/dev/run_local_elasticmq.sh",
        "scripts/dev/run_local_dynamodb.sh",
        "scripts/dev/run_local_influxd.sh",
    ):
        assert runner in help_text

    # Like verify-influx, the gate needs services and stays out of the aggregate target.
    verify_rule = re.search(r"^verify:(.*)$", "\n".join(lines), flags=re.MULTILINE)
    assert verify_rule is not None
    assert "verify-ingestion" not in verify_rule.group(1).split()


def test_local_ingestion_runners_pin_their_artifacts_and_bind_loopback() -> None:
    elasticmq = ROOT / "scripts/dev/run_local_elasticmq.sh"
    dynamodb = ROOT / "scripts/dev/run_local_dynamodb.sh"

    for script in (elasticmq, dynamodb):
        assert script.stat().st_mode & stat.S_IXUSR, f"{script.name} must be executable"
        text = script.read_text(encoding="utf-8")
        assert text.startswith("#!/usr/bin/env bash\n")
        assert "set -euo pipefail" in text
        assert "sha256sum -c" in text
        assert "XDG_CACHE_HOME" in text
        result = subprocess.run(["bash", "-n", str(script)], check=False, capture_output=True)
        assert result.returncode == 0, result.stderr

    elasticmq_text = elasticmq.read_text(encoding="utf-8")
    assert "elasticmq-server-1.6.16.jar" in elasticmq_text
    assert ELASTICMQ_JAR_SHA256 in elasticmq_text
    assert "${ELASTICMQ_PORT:-9324}" in elasticmq_text
    # Both the advertised queue URLs and the REST listener stay on loopback.
    assert re.search(r'node-address\s*\{[^}]*\bhost\s*=\s*"127\.0\.0\.1"', elasticmq_text)
    assert re.search(r'rest-sqs\s*\{[^}]*\bbind-hostname\s*=\s*"127\.0\.0\.1"', elasticmq_text)
    assert re.search(r"sqs-limits\s*=\s*strict", elasticmq_text)
    assert "ListQueues" in elasticmq_text
    # No pre-declared queues: each test creates and deletes its own.
    assert not re.search(r"^\s*queues\s*\{", elasticmq_text, flags=re.MULTILINE)

    dynamodb_text = dynamodb.read_text(encoding="utf-8")
    assert "v2.x/dynamodb_local_latest.tar.gz" in dynamodb_text
    assert DYNAMODB_LOCAL_SHA256 in dynamodb_text
    assert "-inMemory" in dynamodb_text
    assert "${DYNAMODB_LOCAL_PORT:-8001}" in dynamodb_text
    # The download URL is mutable: a checksum mismatch must tell the operator to re-pin.
    assert "re-pin" in dynamodb_text.lower()


def _run_ingestion_suite(
    *, set_flags: dict[str, str], extra_args: tuple[str, ...] = ()
) -> tuple[int, str]:
    env = {
        name: value
        for name, value in os.environ.items()
        if name not in {"RUN_INGESTION_INTEGRATION", "RUN_INFLUX_INTEGRATION"}
    }
    env.update(set_flags)
    # Unreachable endpoints: a test that did not skip would fail instead of passing silently.
    env.update(
        INGESTION_SQS_URL="http://127.0.0.1:9",
        INGESTION_DYNAMODB_URL="http://127.0.0.1:9",
        INFLUX_INTEGRATION_URL="http://127.0.0.1:9",
    )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-rs",
            "-p",
            "no:cacheprovider",
            "tests/ingestion",
            *extra_args,
        ],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=300,
    )
    return result.returncode, result.stdout + result.stderr


# The ingestion flag alone gates tests/ingestion: the Influx flag does not open it.
@pytest.mark.parametrize("set_flags", [{}, {"RUN_INFLUX_INTEGRATION": "1"}])
def test_ingestion_tests_skip_cleanly_without_the_integration_flag(
    set_flags: dict[str, str],
) -> None:
    returncode, output = _run_ingestion_suite(set_flags=set_flags)

    assert returncode == 0, output
    summary = output.strip().splitlines()[-1]
    assert re.fullmatch(r"\d+ skipped(, \d+ warnings?)? in [\d.]+s( \([^)]*\))?", summary), summary
    assert int(summary.split()[0]) >= 10
    assert "RUN_INGESTION_INTEGRATION=1" in output


def test_ingestion_tests_fail_instead_of_skipping_under_the_integration_flag() -> None:
    # Without RUN_INFLUX_INTEGRATION the reused influx_bucket fixture skips; under
    # RUN_INGESTION_INTEGRATION=1 that skip must fail the run, never pass it silently.
    returncode, output = _run_ingestion_suite(
        set_flags={"RUN_INGESTION_INTEGRATION": "1"},
        extra_args=("-k", "test_proxy_down_makes_influx_writes_retryable"),
    )

    assert returncode == 1, output
    assert "1 failed" in output, output
    assert "skipped" not in output.strip().splitlines()[-1]
    assert "must not skip under RUN_INGESTION_INTEGRATION=1" in output
    assert "RUN_INFLUX_INTEGRATION" in output


def test_verification_docs_document_the_ingestion_gate() -> None:
    text = (ROOT / "docs/verification.md").read_text(encoding="utf-8")

    for required in (
        "`make verify-ingestion`",
        "scripts/dev/run_local_elasticmq.sh",
        "scripts/dev/run_local_dynamodb.sh",
        "scripts/dev/run_local_influxd.sh",
        "tests/support/ingestion_harness.py",
        "RUN_INGESTION_INTEGRATION=1",
        "RUN_INFLUX_INTEGRATION=1",
        "INGESTION_SQS_URL",
        "INGESTION_DYNAMODB_URL",
        "ELASTICMQ_PORT",
        "DYNAMODB_LOCAL_PORT",
        ELASTICMQ_JAR_SHA256,
        DYNAMODB_LOCAL_SHA256,
        ELASTICMQ_IMAGE,
        DYNAMODB_LOCAL_IMAGE,
        "8001:8000",
        "ToggleableTcpProxy",
    ):
        assert required in text, required

    assert "## Ingestion integration gate" in text
    section = text.split("## Ingestion integration gate", 1)[1].split("\n## ", 1)[0]
    assert "not part of `make verify`" in section
    assert "ttl" in section.lower()
