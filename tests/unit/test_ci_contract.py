from __future__ import annotations

import re
import subprocess
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]


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
    }
    for job_name, (expected_actions, target) in expected_jobs.items():
        steps = workflow["jobs"][job_name]["steps"]
        actions = [step["uses"].partition("@")[0] for step in steps if "uses" in step]
        runs = [step["run"] for step in steps if "run" in step]
        assert actions == expected_actions
        assert runs == [f"make {target}"]

    frontend_steps = workflow["jobs"]["frontend"]["steps"]
    frontend_actions = [
        step["uses"].partition("@")[0] for step in frontend_steps if "uses" in step
    ]
    frontend_runs = [step["run"] for step in frontend_steps if "run" in step]
    assert frontend_actions == ["actions/checkout", "actions/setup-node"]
    assert frontend_runs == ["npm ci", "npm test", "npm run typecheck", "npm run build"]

    assert [name for name, job in workflow["jobs"].items() if "services" in job] == [
        "influx"
    ]


def test_influx_job_uses_a_throwaway_influxdb_service_with_the_harness_defaults() -> None:
    text = (ROOT / ".github/workflows/verify.yml").read_text(encoding="utf-8")
    workflow = yaml.load(text, Loader=yaml.BaseLoader)
    job = workflow["jobs"]["influx"]

    assert job["runs-on"] == "ubuntu-latest"
    assert list(job["services"]) == ["influxdb"]
    service = job["services"]["influxdb"]
    assert service["image"] == "influxdb:2.7.11-alpine"
    assert service["ports"] == ["8086:8086"]

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

    options = service["options"]
    assert '--health-cmd "influx ping"' in options
    for flag in ("--health-interval", "--health-timeout", "--health-retries"):
        assert flag in options

    # Readiness comes from the service health check; the job runs nothing else.
    assert [step["run"] for step in job["steps"] if "run" in step] == [
        "make verify-influx"
    ]
    # The harness defaults already match the service; no job-level overrides.
    assert "env" not in job
    for step in job["steps"]:
        assert "env" not in step

    # The dev token and password exist only in the throwaway service container.
    assert text.count("local-dev-token") == 1
    assert text.count(env["DOCKER_INFLUXDB_INIT_PASSWORD"]) == 1


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
    }
    influx_recipe = [
        "uv sync --locked --extra dev",
        "RUN_INFLUX_INTEGRATION=1 uv run --locked --no-sync python -m pytest -q tests/influx",
        "RUN_INFLUX_INTEGRATION=1 go test -race -run 'TestInfluxIntegration' ./...",
    ]

    phony_match = re.search(r"^\.PHONY:\s+(.+)$", makefile, flags=re.MULTILINE)
    assert phony_match is not None
    assert set(phony_match.group(1).split()) == {"verify", "verify-influx", *expected}

    verify_rule = re.search(r"^verify:(.*)$", makefile, flags=re.MULTILINE)
    assert verify_rule is not None
    assert verify_rule.group(1).split() == list(expected)

    for target, commands in {**expected, "verify-influx": influx_recipe}.items():
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
