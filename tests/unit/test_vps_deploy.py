"""Exercise the deployed Bash script; only Docker/SSH are replaced at the boundary."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
CANDIDATE = "main-abcdef0123456789abcdef0123456789abcdef01"
PREVIOUS = "main-1234567890abcdef1234567890abcdef12345678"


@pytest.fixture
def deploy(tmp_path):
    stack = tmp_path / "stack with spaces"
    stack.mkdir()
    (stack / ".env").write_text("APP_ENV=prod\n")
    (stack / "compose.production.yaml").write_text("services: {}\n")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "docker.jsonl"
    docker = bin_dir / "docker"
    docker.write_text(
        f"#!{sys.executable}\n"
        "import json, os, pathlib, sys\n"
        "args = sys.argv[1:]\n"
        "tag_file = pathlib.Path('.env.image')\n"
        "tag = tag_file.read_text().strip() if tag_file.exists() else None\n"
        "with open(os.environ['FAKE_DOCKER_CALLS'], 'a') as log:\n"
        "    log.write(json.dumps({'argv': args, 'tag': tag}) + '\\n')\n"
        "if args[:1] == ['compose']:\n"
        "    expected = ['compose', '--env-file', '.env', '--env-file', '.env.image', '-f', 'compose.production.yaml']\n"
        "    if args[:7] != expected or not tag_file.exists():\n"
        "        sys.exit(90)\n"
        "    operation = args[7]\n"
        "    suffixes = {'pull': ['pull'], 'up': ['up', '-d', '--remove-orphans'], 'ps': ['ps', '-q', 'api'], 'stop': ['stop']}\n"
        "    if args[7:] != suffixes.get(operation):\n"
        "        sys.exit(93)\n"
        "    candidate = tag == 'IMAGE_TAG=' + os.environ['FAKE_CANDIDATE']\n"
        "    if candidate and operation == os.environ.get('FAKE_FAIL_OPERATION'):\n"
        "        sys.exit(21)\n"
        "    if not candidate and os.environ.get('FAKE_FAIL_ROLLBACK') == '1':\n"
        "        sys.exit(22)\n"
        "    if operation == 'ps':\n"
        "        print('fake-api-container')\n"
        "    elif operation not in ('pull', 'up', 'stop'):\n"
        "        sys.exit(91)\n"
        "elif args[:1] == ['inspect']:\n"
        "    if args != ['inspect', '-f', '{{.State.Health.Status}}', 'fake-api-container']:\n"
        "        sys.exit(94)\n"
        "    print(os.environ.get('FAKE_HEALTH', 'healthy'))\n"
        "elif args != ['image', 'prune', '-f', '--filter', 'until=168h']:\n"
        "    sys.exit(92)\n"
    )
    docker.chmod(0o755)

    def run(*, previous=PREVIOUS, tag=CANDIDATE, fail="", health="healthy", rollback_fail=False):
        image_file = stack / ".env.image"
        if previous is not None:
            image_file.write_text(f"IMAGE_TAG={previous}\n")
        env = {
            **os.environ,
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "SSH_ORIGINAL_COMMAND": tag,
            "LIMNOPULSE_STACK_DIR": str(stack),
            "LIMNOPULSE_DEPLOY_LOG": str(tmp_path / "deploy.log"),
            "LIMNOPULSE_HEALTH_TIMEOUT_SECONDS": "0",
            "FAKE_DOCKER_CALLS": str(calls),
            "FAKE_CANDIDATE": CANDIDATE,
            "FAKE_FAIL_OPERATION": fail,
            "FAKE_HEALTH": health,
            "FAKE_FAIL_ROLLBACK": "1" if rollback_fail else "0",
        }
        script = os.environ.get(
            "LIMNOPULSE_DEPLOY_SCRIPT_UNDER_TEST", str(ROOT / "ops/vps/limnopulse-deploy.sh")
        )
        result = subprocess.run(
            ["bash", script],
            check=False,
            env=env,
            cwd=tmp_path,
            text=True,
            capture_output=True,
            timeout=10,
        )
        history = (
            [json.loads(line) for line in calls.read_text().splitlines()] if calls.exists() else []
        )
        return result, image_file, history, tmp_path / "deploy.log"

    return run


def operations(history):
    return [(entry["argv"][7], entry["tag"]) for entry in history if entry["argv"][0] == "compose"]


def test_healthy_deploy_promotes_candidate_without_rollback(deploy):
    result, image_file, history, _ = deploy()
    assert result.returncode == 0, result.stderr
    assert image_file.read_text() == f"IMAGE_TAG={CANDIDATE}\n"
    assert operations(history) == [
        ("pull", f"IMAGE_TAG={CANDIDATE}"),
        ("up", f"IMAGE_TAG={CANDIDATE}"),
        ("ps", f"IMAGE_TAG={CANDIDATE}"),
    ]


@pytest.mark.parametrize("failure", ["pull", "up", "health"])
def test_failed_promotion_restores_previous_release(deploy, failure):
    result, image_file, history, log = deploy(
        fail=failure if failure != "health" else "",
        health="starting" if failure == "health" else "healthy",
    )
    assert result.returncode != 0
    assert image_file.read_text() == f"IMAGE_TAG={PREVIOUS}\n"
    attempted = {"pull": ["pull"], "up": ["pull", "up"], "health": ["pull", "up", "ps"]}[failure]
    assert operations(history) == [(op, f"IMAGE_TAG={CANDIDATE}") for op in attempted] + [
        ("pull", f"IMAGE_TAG={PREVIOUS}"),
        ("up", f"IMAGE_TAG={PREVIOUS}"),
    ]
    assert "deploy ok" not in log.read_text()


def test_failed_rollback_is_reported_and_does_not_recurse(deploy):
    result, image_file, history, log = deploy(fail="up", rollback_fail=True)
    assert result.returncode != 0
    assert image_file.read_text() == f"IMAGE_TAG={PREVIOUS}\n"
    assert operations(history) == [
        ("pull", f"IMAGE_TAG={CANDIDATE}"),
        ("up", f"IMAGE_TAG={CANDIDATE}"),
        ("pull", f"IMAGE_TAG={PREVIOUS}"),
        ("up", f"IMAGE_TAG={PREVIOUS}"),
    ]
    assert "rollback failed" in log.read_text().lower()
    assert "deploy ok" not in log.read_text()


@pytest.mark.parametrize("failure", ["pull", "up", "health"])
def test_first_deploy_failure_stops_candidate_without_deleting_volumes(deploy, failure):
    result, image_file, history, _ = deploy(
        previous=None,
        fail=failure if failure != "health" else "",
        health="starting" if failure == "health" else "healthy",
    )
    assert result.returncode != 0
    assert not image_file.exists()
    attempted = {"pull": ["pull"], "up": ["pull", "up"], "health": ["pull", "up", "ps"]}[failure]
    assert operations(history) == [(op, f"IMAGE_TAG={CANDIDATE}") for op in attempted + ["stop"]]


@pytest.mark.parametrize(
    "tag", ["", "latest", "main-abcdefg", 'main-abcdef0"; false; #', "$(false)"]
)
def test_invalid_deploy_tag_is_rejected_before_docker(deploy, tag):
    result, image_file, history, _ = deploy(tag=tag)
    assert result.returncode != 0
    assert image_file.read_text() == f"IMAGE_TAG={PREVIOUS}\n"
    assert history == []


@pytest.mark.parametrize(
    "payload",
    [
        "main-abcdef0",
        CANDIDATE,
        'main-abcdef0"; touch injected; #',
        "$(touch injected)",
        "main-abcdef0; touch injected",
    ],
)
def test_workflow_tag_validation_never_executes_input(tmp_path, payload):
    workflow = yaml.load(
        (ROOT / ".github/workflows/deploy.yml").read_text(), Loader=yaml.BaseLoader
    )
    step = next(
        step
        for step in workflow["jobs"]["deploy-vps"]["steps"]
        if step["name"] == "Validate tag format"
    )
    expression = "${{ inputs.image_tag }}"
    # Actions interpolates expressions before the shell executes the step.
    script = step["run"].replace(expression, payload)
    env = {
        **os.environ,
        **{key: value.replace(expression, payload) for key, value in step.get("env", {}).items()},
    }
    result = subprocess.run(
        ["bash", "-e", "-c", script],
        check=False,
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert not (tmp_path / "injected").exists(), "workflow executed image_tag as shell code"
    assert (result.returncode == 0) == (payload in ("main-abcdef0", CANDIDATE))


def test_production_gate_only_parses_the_example_configuration(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    capture = tmp_path / "docker.args"
    docker = bin_dir / "docker"
    docker.write_text(f"#!/bin/sh\nprintf '%s\\n' \"$@\" > '{capture}'\n")
    docker.chmod(0o755)
    result = subprocess.run(
        ["make", "--no-print-directory", "verify-compose-production"],
        check=False,
        cwd=ROOT,
        env={**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"},
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, result.stderr
    assert capture.read_text().splitlines() == [
        "compose",
        "--env-file",
        ".env.production.example",
        "-f",
        "compose.production.yaml",
        "config",
        "--quiet",
    ]


def test_ci_reuses_the_production_configuration_gate():
    workflow = yaml.load((ROOT / ".github/workflows/build.yml").read_text(), Loader=yaml.BaseLoader)
    job = workflow["jobs"]["compose-production-config"]
    assert [step["run"] for step in job["steps"] if "run" in step] == [
        "make verify-compose-production"
    ]


def test_deploy_configuration_keeps_production_defaults():
    # Evaluate only setup declarations, before tag validation or any deploy I/O.
    script = (
        (ROOT / "ops/vps/limnopulse-deploy.sh").read_text().split('TAG="${SSH_ORIGINAL_COMMAND')[0]
    )
    env = {key: value for key, value in os.environ.items() if not key.startswith("LIMNOPULSE_")}
    result = subprocess.run(
        ["bash", "-c", script + '\nprintf "%s\\n" "$STACK_DIR" "$LOG" "$HEALTH_TIMEOUT_SECONDS"'],
        check=False,
        env=env,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "/opt/limnopulse",
        "/var/log/limnopulse-deploy.log",
        "120",
    ]


@pytest.mark.parametrize(
    "payload", [CANDIDATE, 'main-abcdef0"; touch injected; #', "$(touch injected)"]
)
def test_workflow_ssh_receives_the_tag_as_one_literal_argument(tmp_path, payload):
    workflow = yaml.load(
        (ROOT / ".github/workflows/deploy.yml").read_text(), Loader=yaml.BaseLoader
    )
    step = next(
        step for step in workflow["jobs"]["deploy-vps"]["steps"] if step["name"] == "Deploy to VPS"
    )
    expressions = {
        "${{ inputs.image_tag }}": payload,
        "${{ secrets.VPS_SSH_KEY }}": "fake-key",
        "${{ secrets.VPS_KNOWN_HOSTS }}": "fake-host",
    }

    def render(value):
        for expression, replacement in expressions.items():
            value = value.replace(expression, replacement)
        return value

    ssh = tmp_path / "ssh"
    capture = tmp_path / "ssh.json"
    ssh.write_text(
        f"#!{sys.executable}\nimport json, sys\nfrom pathlib import Path\nPath({str(capture)!r}).write_text(json.dumps(sys.argv[1:]))\n"
    )
    ssh.chmod(0o755)
    env = {
        **os.environ,
        **{key: render(value) for key, value in step["env"].items()},
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "TMPDIR": str(tmp_path),
    }
    result = subprocess.run(
        ["bash", "-e", "-c", render(step["run"])],
        check=False,
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, result.stderr
    assert not (tmp_path / "injected").exists()
    args = json.loads(capture.read_text())
    assert args[-2:] == ["limnopulse-deploy@103.199.184.166", payload]
    assert not list(tmp_path.glob("tmp.*")), "temporary SSH credentials must be removed"
