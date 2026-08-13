from __future__ import annotations

import os
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
LAB = ROOT / "ops" / "lab"


def _run_hook(tmp_path: Path, name: str) -> subprocess.CompletedProcess[str]:
    marker = tmp_path / "vinisantana-lab"
    marker.write_text(
        "managed_by: debian-vps-lab\nenvironment: lab\ntarget: limnopulse\n", encoding="utf-8"
    )
    command_log = tmp_path / "commands.log"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for command in ("docker", "curl", "systemctl", "install", "sha256sum", "tar", "sleep"):
        command_path = bin_dir / command
        response = ""
        if command == "docker":
            response = (
                "if [[ \"$*\" == *'compose ps --status running --services'* ]]; then\n"
                "  printf '%s\\n' redis dynamodb-local influxdb mqtt-broker telegraf elasticmq\n"
                "fi\n"
            )
        command_path.write_text(
            "#!/usr/bin/env bash\n"
            "printf '%s\\n' \"$0 $*\" >> \"$LAB_TEST_COMMAND_LOG\"\n"
            + response
            + "exit 0\n",
            encoding="utf-8",
        )
        command_path.chmod(0o755)
    sudo = bin_dir / "sudo"
    sudo.write_text(
        "#!/usr/bin/env bash\n"
        "printf '%s\\n' \"sudo $*\" >> \"$LAB_TEST_COMMAND_LOG\"\n"
        "exit 0\n",
        encoding="utf-8",
    )
    sudo.chmod(0o755)
    source_root = tmp_path / "source"
    source_root.mkdir()
    (source_root / ".env.example").write_text("APP_ENV=local\n", encoding="utf-8")
    (source_root / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    uv_bin = bin_dir / "lab-uv"
    uv_bin.write_text(
        "#!/usr/bin/env bash\n"
        "printf '%s\\n' \"$0 $*\" >> \"$LAB_TEST_COMMAND_LOG\"\n"
        "exit 0\n",
        encoding="utf-8",
    )
    uv_bin.chmod(0o755)
    environment = os.environ | {
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "LAB_MARKER_PATH": str(marker),
        "LAB_ROOT": str(source_root),
        "LAB_ENV_FILE": str(source_root / ".env"),
        "LIMNOPULSE_LAB_UV": str(uv_bin),
        "LAB_TEST_COMMAND_LOG": str(command_log),
    }
    test_env_file = Path(environment["LAB_ENV_FILE"])
    try:
        completed = subprocess.run(
            [str(LAB / name)],
            cwd=ROOT,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        completed.command_log = command_log.read_text(encoding="utf-8")  # type: ignore[attr-defined]
    finally:
        test_env_file.unlink(missing_ok=True)
    return completed


def test_lab_target_declares_only_the_local_limnopulse_contract() -> None:
    target = yaml.safe_load((LAB / "target.yaml").read_text(encoding="utf-8"))

    assert target == {
        "apiVersion": "lab.vinisantana.dev/v1alpha1",
        "kind": "LabTarget",
        "metadata": {"name": "limnopulse", "ownerRepository": "limnopulse"},
        "spec": {
            "guestProfile": "limnopulse-medium",
            "baseline": {"capability": "debian-container-host"},
            "hooks": {
                "deploy": {"path": "ops/lab/deploy", "timeoutSeconds": 1200},
                "verify": {"path": "ops/lab/verify", "timeoutSeconds": 300},
                "reset": {"path": "ops/lab/reset", "timeoutSeconds": 300},
            },
            "services": [
                {
                    "name": "api",
                    "guestPort": 8000,
                    "preferredHostPort": 18000,
                    "health": {"type": "http", "path": "/healthz", "expectedStatus": 200},
                }
            ],
            "dataPolicy": {
                "productionDataAllowed": False,
                "syntheticFixturesOnly": True,
                "emulatedCapabilities": ["dynamodb", "sqs", "email-sender"],
                "omittedCapabilities": ["cognito", "ses", "eventbridge", "production-mqtt"],
            },
        },
    }


def test_lab_hooks_refuse_to_run_without_the_guest_marker(tmp_path: Path) -> None:
    for name in ("deploy", "verify", "reset"):
        path = LAB / name
        assert os.access(path, os.X_OK)
        completed = subprocess.run(
            [str(path)],
            cwd=ROOT,
            env=os.environ | {"LAB_MARKER_PATH": str(tmp_path / "missing-marker")},
            capture_output=True,
            text=True,
            check=False,
        )
        assert completed.returncode == 9
        assert "lab marker" in completed.stderr.lower()


def test_lab_deploy_uses_only_local_runtime_commands(tmp_path: Path) -> None:
    completed = _run_hook(tmp_path, "deploy")

    assert completed.returncode == 0, completed.stderr
    command_log = completed.command_log  # type: ignore[attr-defined]
    assert "sync --locked --no-dev" in command_log
    assert "docker compose up -d redis dynamodb-local influxdb mqtt-broker telegraf elasticmq" in command_log
    assert "run --locked python scripts/dev/init_dynamodb.py" in command_log
    assert "run --locked python scripts/dev/seed_local.py" in command_log
    assert "systemctl enable --now limnopulse-lab-api.service" in command_log
    assert "aws " not in command_log.lower()
    assert "infisical" not in command_log.lower()


def test_lab_reset_stops_only_limnopulse_services(tmp_path: Path) -> None:
    completed = _run_hook(tmp_path, "reset")

    assert completed.returncode == 0, completed.stderr
    command_log = completed.command_log  # type: ignore[attr-defined]
    assert "systemctl disable --now limnopulse-lab-api.service" in command_log
    assert "docker compose down --volumes --remove-orphans" in command_log


def test_lab_hooks_refuse_a_marker_without_lab_identity(tmp_path: Path) -> None:
    marker = tmp_path / "marker"
    marker.write_text("environment: production\ntarget: limnopulse\n", encoding="utf-8")
    completed = subprocess.run(
        [str(LAB / "reset")],
        cwd=ROOT,
        env=os.environ | {"LAB_MARKER_PATH": str(marker)},
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 9
    assert "lab marker" in completed.stderr.lower()


def test_lab_smoke_builds_a_synthetic_breaching_rule_and_stable_window() -> None:
    from ops.lab.smoke import build_rule, evaluation_time, sample_offsets, sample_payload

    run_at = evaluation_time("2026-08-13T12:01:17Z")

    assert run_at == "2026-08-13T12:02:00Z"
    assert evaluation_time("2026-08-13T12:01:59Z") == "2026-08-13T12:03:00Z"
    assert build_rule()["metric"] == "do_mg_l"
    assert build_rule()["operator"] == "<"
    assert build_rule()["threshold"] == 7.0
    assert build_rule()["window"] == "60s"
    assert sample_payload("2026-08-13T12:01:10Z", 3)["do_mg_l"] == 4.0
    assert sample_payload("2026-08-13T12:01:10Z", 3)["seq"] == 3
    assert sample_offsets() == (5, 15, 25, 35, 45, 55)
