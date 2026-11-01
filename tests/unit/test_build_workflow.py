"""Pin the build workflow and its parse-only production configuration gate."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


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


def test_pr_controlled_build_jobs_cannot_publish_or_persist_checkout_credentials():
    workflow = yaml.load((ROOT / ".github/workflows/build.yml").read_text(), Loader=yaml.BaseLoader)
    defaults = workflow["permissions"]
    assert defaults.get("packages", "none") != "write"
    publishing_jobs = []
    for name, job in workflow["jobs"].items():
        permissions = job.get("permissions", defaults)
        assert permissions.get("contents") == "read"
        if permissions.get("packages") == "write":
            publishing_jobs.append(name)
            assert job["if"] == "github.event_name == 'push' && github.ref == 'refs/heads/main'"
        for step in job["steps"]:
            if step.get("uses", "").startswith("actions/checkout@"):
                assert step["with"]["persist-credentials"] == "false"
    assert set(publishing_jobs) == {"publish"}
    assert workflow["jobs"]["image"]["if"] == "github.event_name == 'pull_request'"
    publisher = workflow["jobs"]["publish"]
    assert publisher["needs"] in ("compose-production-config", ["compose-production-config"])
    assert publisher["strategy"] == workflow["jobs"]["image"]["strategy"]
    assert set(publisher["strategy"]["matrix"]["app"]) == {
        "api",
        "frontend",
        "alert-evaluator",
        "notifications",
    }
    for job_name in ("image", "publish"):
        builder = next(
            step
            for step in workflow["jobs"][job_name]["steps"]
            if step.get("uses", "").startswith("docker/build-push-action@")
        )
        assert builder["with"]["push"] == (
            "${{ github.event_name == 'push' && github.ref == 'refs/heads/main' }}"
        )
