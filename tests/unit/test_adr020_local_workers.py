"""ADR-020 S3: local runs build and run the worker executables directly against the
emulators (`scripts/dev/run_worker.sh`); Compose runs no Go worker."""

from __future__ import annotations

import os
import stat
import subprocess
import sys

import pytest
import yaml

from tests.support.local_workers import LOCAL_ENV_DIR, ROOT, RUN_WORKER, local_env
from tests.support.worker_roles import load_roles

# worker-monitor reads systemd and publishes to CloudWatch; it has nothing to do locally.
LOCAL_ROLES = (
    "alert-evaluator",
    "notification-relay",
    "email-worker",
    "ses-feedback-worker",
    "telegram-worker",
    "limnopulse-admin",
)
SUPERVISION = (
    "NOTIFY_SOCKET",
    "WATCHDOG_USEC",
    "WATCHDOG_PID",
    "CREDENTIALS_DIRECTORY",
    "LIMNOPULSE_SUPERVISOR",
)
COMPOSE_HOSTS = ("dynamodb-local", "elasticmq", "influxdb", "redis:", "telegram-bot-api-fake")


def test_compose_runs_no_go_worker():
    services = yaml.safe_load((ROOT / "compose.yaml").read_text(encoding="utf-8"))["services"]
    go_roles = {role["name"] for role in load_roles()["roles"] if role["lifecycle"] != "compose"}
    for name, service in services.items():
        assert name not in go_roles, name
        assert "notifications" not in str(service.get("build", "")), name
        assert service.get("command") not in (["run"], ["relay"], ["worker"]), name


def test_each_local_role_has_local_settings_on_the_emulator_host_ports():
    assert sorted(path.stem for path in LOCAL_ENV_DIR.iterdir()) == sorted(LOCAL_ROLES)
    go_roles = {role["name"] for role in load_roles()["roles"] if role["lifecycle"] != "compose"}
    assert set(LOCAL_ROLES) <= go_roles
    for role in LOCAL_ROLES:
        values = local_env(role)
        assert not set(values) & set(SUPERVISION), role
        assert values["AWS_EC2_METADATA_DISABLED"] == "true", role
        assert values.get("APP_ENV", "local") == "local", role
        for key, value in values.items():
            assert not any(host in value for host in COMPOSE_HOSTS), (role, key)
            if key.endswith("_URL") and value.startswith("http"):
                assert value.startswith(("http://127.0.0.1:", "http://localhost:")), (role, key)


def _fake_go(tmp_path):
    record = tmp_path / "go-call"
    go = tmp_path / "go"
    go.write_text(
        f"#!{sys.executable}\n"
        "import os, sys\n"
        "args = sys.argv[1:]\n"
        f"open({str(record)!r}, 'w').write(os.getcwd() + '\\n' + '\\n'.join(args))\n"
        "out = args[args.index('-o') + 1]\n"
        "with open(out, 'w') as binary:\n"
        "    binary.write('#!/bin/sh\\nprintf \"%s\\\\n\" \"$@\"; env | sort\\n')\n"
        "os.chmod(out, 0o755)\n"
    )
    go.chmod(0o755)
    return go, record


def _run(tmp_path, *args, **env):
    go, record = _fake_go(tmp_path)
    environment = {
        key: value
        for key, value in os.environ.items()
        if key not in SUPERVISION and not key.startswith(("SQS_", "AWS_", "DYNAMODB_"))
    }
    environment.update(GO=str(go), XDG_CACHE_HOME=str(tmp_path / "cache"), **env)
    result = subprocess.run(
        ["bash", str(RUN_WORKER), *args],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    return result, record


def test_run_worker_builds_the_role_and_runs_it_with_the_local_settings(tmp_path):
    result, record = _run(
        tmp_path,
        "email-worker",
        "--receive-wait=0s",
        NOTIFY_SOCKET="/run/systemd/notify",
        CREDENTIALS_DIRECTORY="/run/credentials/x",
        LIMNOPULSE_SUPERVISOR="systemd",
        SQS_NOTIFICATION_JOBS_URL="http://127.0.0.1:9324/000000000000/other-jobs",
    )
    assert result.returncode == 0, result.stderr
    cwd, *build = record.read_text().splitlines()
    binary = tmp_path / "cache/limnopulse/workers/bin/email-worker"
    assert cwd == str(ROOT)
    assert build == ["build", "-trimpath", "-o", str(binary), "./cmd/email-worker"]
    lines = result.stdout.splitlines()
    assert lines[0] == "--receive-wait=0s"
    environment = dict(line.split("=", 1) for line in lines[1:] if "=" in line)
    for key in SUPERVISION:
        assert key not in environment, key
    # The caller's value wins over the local setting; the rest come from the file.
    assert environment["SQS_NOTIFICATION_JOBS_URL"].endswith("/other-jobs")
    for key, value in local_env("email-worker").items():
        if key != "SQS_NOTIFICATION_JOBS_URL":
            assert environment[key] == value, key


@pytest.mark.parametrize("role", ["worker-monitor", "notifications", "../email-worker", ""])
def test_run_worker_refuses_a_role_without_local_settings(tmp_path, role):
    result, record = _run(tmp_path, role)
    assert result.returncode == 2
    assert "run_worker:" in result.stderr
    assert not record.exists()


def test_run_worker_is_an_executable_bash_script():
    assert RUN_WORKER.stat().st_mode & stat.S_IXUSR
    text = RUN_WORKER.read_text(encoding="utf-8")
    assert text.startswith("#!/usr/bin/env bash\n")
    assert "set -euo pipefail" in text
    assert subprocess.run(["bash", "-n", str(RUN_WORKER)], check=False).returncode == 0
