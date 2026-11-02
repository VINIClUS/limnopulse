"""Pin the build workflow and its parse-only production configuration gate."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import yaml

from tests.support.worker_roles import load_roles

ROOT = Path(__file__).resolve().parents[2]
MAIN_PUSH = "github.event_name == 'push' && github.ref == 'refs/heads/main'"
GO_DOCKERFILES = ("Dockerfile.alert-evaluator", "Dockerfile.notifications")


def _workflow(name: str) -> dict:
    return yaml.load((ROOT / ".github/workflows" / name).read_text(), Loader=yaml.BaseLoader)


def _step(job: dict, name: str) -> dict:
    return next(step for step in job["steps"] if step.get("name") == name)


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
    assert set(publishing_jobs) == {"publish", "workers-publish"}
    assert workflow["jobs"]["image"]["if"] == "github.event_name == 'pull_request'"
    publisher = workflow["jobs"]["publish"]
    assert publisher["needs"] in ("compose-production-config", ["compose-production-config"])
    assert publisher["strategy"] == workflow["jobs"]["image"]["strategy"]
    # ADR-020: the Go workers ship in the workers bundle; no Go image is built.
    assert set(publisher["strategy"]["matrix"]["app"]) == {"api", "frontend"}
    for job_name in ("image", "publish"):
        builder = next(
            step
            for step in workflow["jobs"][job_name]["steps"]
            if step.get("uses", "").startswith("docker/build-push-action@")
        )
        assert builder["with"]["push"] == (
            "${{ github.event_name == 'push' && github.ref == 'refs/heads/main' }}"
        )


def test_workers_bundle_is_built_twice_verified_and_smoked_on_pull_requests():
    jobs = _workflow("build.yml")["jobs"]
    job, publisher = jobs["workers-bundle"], jobs["workers-publish"]
    assert job["if"] == "github.event_name == 'pull_request'"
    assert "permissions" not in job
    # The publisher runs the very same steps; only its trigger and token differ.
    assert publisher["steps"] == job["steps"]
    assert publisher["env"] == job["env"]
    assert job["env"]["WORKERS_TAG"] == "main-${{ github.sha }}"
    runs = [step["run"] for step in job["steps"] if "run" in step]
    assert runs[0] == "uv sync --locked --extra dev"
    build = _step(job, "Build the workers bundle")["run"].split("\n")
    assert build[:2] == ["make workers-binaries", "make workers-bundle"]
    rebuild = _step(job, "Rebuild it from a second checkout with an empty build cache")["run"]
    assert "git archive HEAD" in rebuild
    assert 'GOCACHE="$RUNNER_TEMP/gocache-b"' in rebuild
    assert "build_bundle binaries" in rebuild and "build_bundle bundle" in rebuild
    compare = _step(job, "Require byte-identical bundles")["run"]
    assert 'second="$RUNNER_TEMP/source-b/dist/workers"' in compare
    assert 'cmp "dist/workers/$file" "$second/$file"' in compare
    verify = _step(job, "Verify the bundle and its static executables")["run"]
    assert "build_bundle verify" in verify and "--static-elf" in verify
    smoke = _step(job, "Run each unit once on the runner's systemd")["run"]
    assert "scripts/workers/smoke_units.sh" in smoke
    # Building, comparing, verifying and the smoke all run before anything is published.
    names = [step.get("name") for step in job["steps"]]
    assert names.index("Run each unit once on the runner's systemd") < names.index(
        "Attest the bundle"
    )
    for step in job["steps"]:
        publishing = step.get("name") in {
            "Install oras",
            "Attest the bundle",
            "Log in to GHCR",
            "Push the bundle to GHCR",
        }
        assert (step.get("if") == MAIN_PUSH) == publishing, step.get("name")


def test_workers_publisher_pushes_one_attested_file_layer_with_the_roles_yaml_types():
    jobs = _workflow("build.yml")["jobs"]
    publisher = jobs["workers-publish"]
    assert publisher["if"] == MAIN_PUSH
    assert publisher["permissions"] == {
        "contents": "read",
        "packages": "write",
        "id-token": "write",
        "attestations": "write",
    }
    bundle = load_roles()["bundle"]
    file = bundle["file"].format(tag="${WORKERS_TAG}")
    attest = _step(publisher, "Attest the bundle")
    assert attest["uses"].startswith("actions/attest-build-provenance@")
    # The tarball file itself is the subject; deploy.yml runs gh attestation verify on it.
    assert attest["with"] == {
        "subject-path": "${{ github.workspace }}/dist/workers/"
        + bundle["file"].format(tag="main-${{ github.sha }}")
    }
    push = _step(publisher, "Push the bundle to GHCR")
    # oras titles the layer with the path as given, so it runs next to the file.
    assert push["working-directory"] == "dist/workers"
    command = push["run"]
    assert f'file="{file}"' in command
    assert f"--artifact-type {bundle['artifact_type']}" in command
    assert f'"{bundle["oci_repository"]}:${{WORKERS_TAG}}"' in command
    assert f'"${{file}}:{bundle["layer_media_type"]}"' in command
    assert "/" not in command.split('file="', 1)[1].split('"', 1)[0]
    # The same pinned, checksum-verified oras as deploy.yml.
    deploy = _workflow("deploy.yml")
    for key in ("ORAS_VERSION", "ORAS_SHA256"):
        assert publisher["env"][key] == deploy["env"][key], key
    assert "sha256sum --check --strict" in _step(publisher, "Install oras")["run"]


def test_no_go_image_is_built_or_scanned():
    for path in GO_DOCKERFILES:
        assert not (ROOT / path).exists(), path
    trivy = _workflow("trivy.yml")["jobs"]
    assert set(trivy["image"]["strategy"]["matrix"]["app"]) == {"api", "frontend"}
    text = "\n".join(
        (ROOT / ".github/workflows" / name).read_text() for name in ("build.yml", "trivy.yml")
    )
    for path in GO_DOCKERFILES:
        assert path not in text


def test_trivy_scans_the_worker_executables_as_a_rootfs():
    job = _workflow("trivy.yml")["jobs"]["workers"]
    build = _step(job, "Build the worker executables")["run"]
    assert "make workers-binaries" in build
    scan = _step(job, "Trivy rootfs scan (worker executables)")
    assert scan["uses"].startswith("aquasecurity/trivy-action@")
    assert scan["with"]["scan-type"] == "rootfs"
    assert scan["with"]["scan-ref"] == "dist/workers/bin"
    assert scan["with"]["exit-code"] == "1"
    assert scan["with"]["severity"] == "CRITICAL,HIGH"


def test_release_executables_build_with_the_latest_patch_of_go_mods_minor():
    go_mod = (ROOT / "go.mod").read_text()
    minor = next(
        ".".join(line.split()[1].split(".")[:2]) for line in go_mod.splitlines()
        if line.startswith("go ")
    )
    jobs = [
        _workflow("build.yml")["jobs"]["workers-bundle"],
        _workflow("build.yml")["jobs"]["workers-publish"],
        _workflow("trivy.yml")["jobs"]["workers"],
    ]
    for job in jobs:
        setup = next(step for step in job["steps"] if step.get("uses", "").startswith("actions/setup-go@"))
        assert setup["with"] == {"go-version": minor, "check-latest": "true"}
