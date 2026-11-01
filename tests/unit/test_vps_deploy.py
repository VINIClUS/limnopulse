"""Exercise the deployed Bash script; only Docker, sudo, oras, systemctl, gh and SSH are fakes.

The ADR-020 flow (ops/workers/README.md, "Deploy"): stage the workers bundle, start Compose
with every deploy profile, wait for api and each profiled worker, activate the bundle, then
record IMAGE_TAG and WORKERS_BUNDLE_SHA256 in .env.image. A failure after staging moves the
API, the profiled workers and the bundle back to the recorded release.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml

from tests.support.worker_roles import ROOT, load_roles
from tests.support.workers_activate import (
    FAKE_HELPER,
    FAKE_SUDO,
    HELPER,
    HELPER_PATH,
    Host,
    write_tool,
)

SCRIPT = ROOT / "ops/vps/limnopulse-deploy.sh"
WORKFLOW = ROOT / ".github/workflows/deploy.yml"
SUDOERS = ROOT / "ops/vps/sudoers.d/limnopulse-deploy"
CANDIDATE = "main-abcdef0123456789abcdef0123456789abcdef01"
PREVIOUS = "main-1234567890abcdef1234567890abcdef12345678"
CANDIDATE_SHA = "c" * 64
PREVIOUS_SHA = "d" * 64
WORKER = "telemetry-ingest-worker"
WITH_BUNDLE = f"IMAGE_TAG={PREVIOUS}\nWORKERS_BUNDLE_SHA256={PREVIOUS_SHA}\n"
# Production today: a release from before ADR-020, with no workers bundle.
IMAGE_ONLY = f"IMAGE_TAG={PREVIOUS}\n"

FAKE_DOCKER = """\
import json, os, pathlib, sys
args = sys.argv[1:]
entry = {"tool": "docker", "argv": args, "tag": None}
if args[:1] == ["compose"]:
    env_file = pathlib.Path(args[4]) if len(args) > 4 else None
    if env_file is not None and env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.startswith("IMAGE_TAG="):
                entry["tag"] = line.split("=", 1)[1]
with open(os.environ["FAKE_CALLS"], "a") as log:
    log.write(json.dumps(entry) + "\\n")
if args[:1] == ["compose"]:
    prefix = ["compose", "--env-file", ".env", "--env-file"]
    rest = ["-f", "compose.production.yaml", "--profile", "ingestion"]
    if args[:4] != prefix or args[4] not in (".env.image", ".env.image.candidate"):
        sys.exit(90)
    if not pathlib.Path(args[4]).exists() or args[5:9] != rest:
        sys.exit(95)
    operation = args[9]
    suffixes = {"pull": ["pull"], "up": ["up", "-d", "--remove-orphans"], "stop": ["stop"]}
    if operation == "ps":
        if args[10:] not in (["-q", "api"], ["-q", "telemetry-ingest-worker"]):
            sys.exit(93)
    elif args[9:] != suffixes.get(operation):
        sys.exit(93)
    candidate = entry["tag"] == os.environ["FAKE_CANDIDATE"]
    if candidate and operation == os.environ.get("FAKE_FAIL_OPERATION"):
        sys.exit(21)
    if not candidate and os.environ.get("FAKE_FAIL_ROLLBACK") == "1":
        sys.exit(22)
    if operation == "ps":
        print(f"fake-{args[11]}-container")
elif args[:1] == ["inspect"]:
    if args[:3] != ["inspect", "-f", "{{.State.Health.Status}}"] or len(args) != 4:
        sys.exit(94)
    service = args[3][len("fake-"):-len("-container")]
    print("starting" if service == os.environ.get("FAKE_UNHEALTHY") else "healthy")
elif args != ["image", "prune", "-f", "--filter", "until=168h"]:
    sys.exit(92)
"""


@dataclass
class Deploy:
    result: subprocess.CompletedProcess[str]
    image_file: Path
    stack: Path
    calls: list[dict]
    log: str


def steps(calls):
    """The deploy's actions in order: helper calls, Compose operations and the image prune."""
    flow = []
    for call in calls:
        argv = call["argv"]
        if call["tool"] == "sudo":
            flow.append(tuple(argv[2:]))
        elif call["tool"] == "docker" and argv[0] == "compose":
            operation = argv[9]
            flow.append(
                (operation, argv[11], call["tag"])
                if operation == "ps"
                else (operation, call["tag"])
            )
        elif call["tool"] == "docker" and argv[0] == "image":
            flow.append(("prune",))
    return flow


@pytest.fixture
def deploy(tmp_path):
    host = Host(tmp_path / "host")
    stack = tmp_path / "stack with spaces"
    stack.mkdir()
    (stack / ".env").write_text("APP_ENV=prod\n")
    (stack / "compose.production.yaml").write_text("services: {}\n")
    write_tool(host.bin, "docker", FAKE_DOCKER)
    write_tool(host.bin, "sudo", FAKE_SUDO)
    fake_helper = write_tool(host.bin, "fake-activate", FAKE_HELPER)
    log_file = tmp_path / "deploy.log"

    def run(
        *,
        previous=WITH_BUNDLE,
        request=f"{CANDIDATE} {CANDIDATE_SHA}",
        fail="",
        unhealthy="",
        rollback_fail=False,
        helper_exits=None,
        real_helper=False,
    ):
        image_file = stack / ".env.image"
        if previous is not None:
            image_file.write_text(previous)
        host.reset_calls()
        env = {
            **host.env,
            "SSH_ORIGINAL_COMMAND": request,
            "LIMNOPULSE_STACK_DIR": str(stack),
            "LIMNOPULSE_DEPLOY_LOG": str(log_file),
            "LIMNOPULSE_HEALTH_TIMEOUT_SECONDS": "0",
            "FAKE_CANDIDATE": CANDIDATE,
            "FAKE_FAIL_OPERATION": fail,
            "FAKE_UNHEALTHY": unhealthy,
            "FAKE_FAIL_ROLLBACK": "1" if rollback_fail else "0",
            "FAKE_HELPER_EXITS": json.dumps(helper_exits or {}),
            "FAKE_ACTIVATE": str(HELPER if real_helper else fake_helper),
        }
        result = subprocess.run(
            ["bash", str(SCRIPT)],
            check=False,
            env=env,
            cwd=tmp_path,
            text=True,
            capture_output=True,
            timeout=60,
        )
        log = log_file.read_text() if log_file.exists() else ""
        return Deploy(result, image_file, stack, host.calls(), log)

    run.host = host
    return run


def promotion(through):
    """The candidate's steps in order, up to and including `through`."""
    flow = [
        ("stage", CANDIDATE, CANDIDATE_SHA),
        ("pull", CANDIDATE),
        ("up", CANDIDATE),
        ("ps", "api", CANDIDATE),
        ("ps", WORKER, CANDIDATE),
        ("activate", CANDIDATE),
    ]
    return flow[: flow.index(through) + 1]


def rollback(previous):
    bundle = [("activate", PREVIOUS)] if previous == WITH_BUNDLE else [("deactivate",)]
    return [("pull", PREVIOUS), ("up", PREVIOUS), *bundle]


def test_healthy_deploy_stages_starts_waits_activates_then_records(deploy):
    run = deploy()
    assert run.result.returncode == 0, run.result.stderr + run.log
    assert steps(run.calls) == [*promotion(("activate", CANDIDATE)), ("prune",)]
    assert run.image_file.read_text() == (
        f"IMAGE_TAG={CANDIDATE}\nWORKERS_BUNDLE_SHA256={CANDIDATE_SHA}\n"
    )
    assert not (run.stack / ".env.image.candidate").exists()
    assert "deploy ok" in run.log


@pytest.mark.parametrize("status", [1, 3, 78])
def test_a_refused_stage_stops_the_deploy_before_anything_changes(deploy, status):
    run = deploy(helper_exits={"stage": status})
    assert run.result.returncode == status
    assert steps(run.calls) == [("stage", CANDIDATE, CANDIDATE_SHA)]
    assert run.image_file.read_text() == WITH_BUNDLE
    assert not (run.stack / ".env.image.candidate").exists()
    assert "nothing changed" in run.log


FAILURES = {
    "pull": ({"fail": "pull"}, ("pull", CANDIDATE)),
    "up": ({"fail": "up"}, ("up", CANDIDATE)),
    "api health": ({"unhealthy": "api"}, ("ps", "api", CANDIDATE)),
    "worker health": ({"unhealthy": WORKER}, ("ps", WORKER, CANDIDATE)),
    "activate": ({"helper_exits": {f"activate:{CANDIDATE}": 1}}, ("activate", CANDIDATE)),
    "activate floor": ({"helper_exits": {f"activate:{CANDIDATE}": 3}}, ("activate", CANDIDATE)),
}


@pytest.mark.parametrize("previous", [WITH_BUNDLE, IMAGE_ONLY], ids=["bundle", "image-only"])
@pytest.mark.parametrize("failure", sorted(FAILURES))
def test_a_failed_promotion_moves_api_workers_and_bundle_back(deploy, failure, previous):
    options, last = FAILURES[failure]
    run = deploy(previous=previous, **options)
    assert run.result.returncode == 1
    assert steps(run.calls) == promotion(last) + rollback(previous)
    assert run.image_file.read_text() == previous
    assert not (run.stack / ".env.image.candidate").exists()
    assert "deploy ok" not in run.log
    assert "rollback failed" not in run.log


def test_rollback_never_overrides_a_floor_refusal(deploy):
    run = deploy(helper_exits={f"activate:{CANDIDATE}": 1, f"activate:{PREVIOUS}": 3})
    assert run.result.returncode == 1
    assert steps(run.calls) == promotion(("activate", CANDIDATE)) + rollback(WITH_BUNDLE)
    assert not any("--override-floor" in call["argv"] for call in run.calls)
    assert "needs a manual decision" in run.log
    assert "rollback failed" in run.log
    assert run.image_file.read_text() == WITH_BUNDLE


def test_failed_rollback_is_reported_and_does_not_recurse(deploy):
    run = deploy(fail="up", rollback_fail=True)
    assert run.result.returncode == 1
    assert steps(run.calls) == promotion(("up", CANDIDATE)) + rollback(WITH_BUNDLE)
    assert "rollback failed" in run.log.lower()
    assert "deploy ok" not in run.log
    assert run.image_file.read_text() == WITH_BUNDLE


@pytest.mark.parametrize("failure", ["pull", "up", "api health", "activate"])
def test_first_deploy_failure_stops_candidate_and_deactivates(deploy, failure):
    options, last = FAILURES[failure]
    run = deploy(previous=None, **options)
    assert run.result.returncode == 1
    assert steps(run.calls) == promotion(last) + [("stop", CANDIDATE), ("deactivate",)]
    assert not run.image_file.exists()
    assert not (run.stack / ".env.image.candidate").exists()


@pytest.mark.parametrize(
    "request_text",
    [
        "",
        CANDIDATE,
        f"{CANDIDATE} {CANDIDATE_SHA}\n",
        f"{CANDIDATE}\n{CANDIDATE_SHA}",
        f"{CANDIDATE} {CANDIDATE_SHA[:-1]}",
        f"{CANDIDATE[:-1]} {CANDIDATE_SHA}",
        "main-abcdef0 " + CANDIDATE_SHA,
        f"{CANDIDATE.upper()} {CANDIDATE_SHA}",
        f"{CANDIDATE} {CANDIDATE_SHA.upper()}",
        f"{CANDIDATE} {CANDIDATE_SHA} extra",
        f"{CANDIDATE}  {CANDIDATE_SHA}",
        f"{CANDIDATE}\t{CANDIDATE_SHA}",
        f" {CANDIDATE} {CANDIDATE_SHA}",
        f"{CANDIDATE} {CANDIDATE_SHA} ",
        f"{CANDIDATE} {CANDIDATE_SHA}; touch injected",
        f"$(touch injected) {CANDIDATE_SHA}",
        "latest " + CANDIDATE_SHA,
    ],
)
def test_the_forced_command_refuses_anything_but_tag_and_sha(deploy, request_text):
    run = deploy(request=request_text)
    assert run.result.returncode == 1
    assert run.calls == []
    assert run.image_file.read_text() == WITH_BUNDLE
    assert not (deploy.host.base.parent / "injected").exists()
    assert "rejected request" in run.log


def test_the_rejected_request_is_logged_escaped(deploy):
    run = deploy(request=f"{CANDIDATE} {CANDIDATE_SHA}\nforged log line")
    assert run.result.returncode == 1
    assert "\nforged log line" not in run.log


# --- the real helper behind the fake sudo -------------------------------------------------


def test_end_to_end_promotion_from_a_release_without_a_bundle(deploy):
    host = deploy.host
    digest = host.publish(CANDIDATE)
    run = deploy(previous=IMAGE_ONLY, request=f"{CANDIDATE} {digest}", real_helper=True)
    assert run.result.returncode == 0, run.log
    assert os.readlink(host.current) == f"releases/{CANDIDATE}"
    assert run.image_file.read_text() == (
        f"IMAGE_TAG={CANDIDATE}\nWORKERS_BUNDLE_SHA256={digest}\n"
    )
    tools = [call["tool"] for call in run.calls]
    assert tools.index("oras") < tools.index("docker")
    assert ["daemon-reload"] in [call["argv"] for call in run.calls if call["tool"] == "systemctl"]


def test_end_to_end_tampered_bundle_is_refused_before_docker(deploy):
    host = deploy.host
    digest = host.publish(CANDIDATE)
    tarball_path = host.registry / CANDIDATE / f"limnopulse-workers-{CANDIDATE}.tar.gz"
    tarball_path.write_bytes(tarball_path.read_bytes() + b"\0")
    run = deploy(previous=IMAGE_ONLY, request=f"{CANDIDATE} {digest}", real_helper=True)
    assert run.result.returncode == 1
    assert [call["tool"] for call in run.calls] == ["sudo", "oras"]
    assert run.image_file.read_text() == IMAGE_ONLY
    assert not (host.releases / CANDIDATE).exists()


def test_end_to_end_missing_bundle_is_refused_before_docker(deploy):
    run = deploy(previous=IMAGE_ONLY, request=f"{CANDIDATE} {CANDIDATE_SHA}", real_helper=True)
    assert run.result.returncode == 1
    assert "docker" not in [call["tool"] for call in run.calls]
    assert run.image_file.read_text() == IMAGE_ONLY


def test_end_to_end_rollback_reactivates_the_previous_bundle(deploy):
    host = deploy.host
    previous = host.deploy_release(PREVIOUS)
    recorded = f"IMAGE_TAG={PREVIOUS}\nWORKERS_BUNDLE_SHA256={previous}\n"
    digest = host.publish(CANDIDATE)
    run = deploy(
        previous=recorded,
        request=f"{CANDIDATE} {digest}",
        unhealthy=WORKER,
        real_helper=True,
    )
    assert run.result.returncode == 1, run.log
    assert os.readlink(host.current) == f"releases/{PREVIOUS}"
    assert sorted(path.name for path in host.releases.iterdir()) == sorted([PREVIOUS, CANDIDATE])
    assert run.image_file.read_text() == recorded


def test_end_to_end_rollback_to_a_release_without_a_bundle_deactivates(deploy):
    host = deploy.host
    # The activation installs the units and swaps current, then fails its try-restart.
    host.env["FAKE_ENABLED"] = "limnopulse-email-worker.service"
    host.env["FAKE_FAIL_RESTART"] = "limnopulse-email-worker.service"
    digest = host.publish(CANDIDATE)
    run = deploy(previous=IMAGE_ONLY, request=f"{CANDIDATE} {digest}", real_helper=True)
    assert run.result.returncode == 1
    assert steps(run.calls)[-1] == ("deactivate",)
    assert not os.path.lexists(host.current)
    assert list(host.units.iterdir()) == []
    assert not (host.state / "floors.json").exists()
    assert run.image_file.read_text() == IMAGE_ONLY


# --- the script mirrors roles.yaml --------------------------------------------------------


def script_setup(*names):
    """Evaluate only the setup declarations, before request validation or any deploy I/O."""
    setup = SCRIPT.read_text().split("# --- request validation")[0]
    env = {key: value for key, value in os.environ.items() if not key.startswith("LIMNOPULSE_")}
    printer = "\n".join(f'printf "%s\\n" "{name}"' for name in names)
    result = subprocess.run(
        ["bash", "-c", setup + "\n" + printer],
        check=False,
        env=env,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.splitlines()


def test_deploy_configuration_keeps_production_defaults():
    assert script_setup("$STACK_DIR", "$LOG", "$HEALTH_TIMEOUT_SECONDS", "$ACTIVATE") == [
        "/opt/limnopulse",
        "/var/log/limnopulse-deploy.log",
        "120",
        HELPER_PATH,
    ]


def test_deploy_constants_match_the_role_table():
    table = load_roles()
    forced, profiles, workers, activate = script_setup(
        "$FORCED_COMMAND", "${PROFILES[*]}", "${PROFILED_WORKERS[*]}", "$ACTIVATE"
    )
    assert forced == table["deploy"]["forced_command"]
    assert profiles.split() == table["deploy"]["compose_profiles"]
    assert workers.split() == [
        role["compose_service"]
        for role in table["roles"]
        if role.get("compose_profile") in table["deploy"]["compose_profiles"]
    ]
    assert activate == table["activation"]["helper"]


def test_the_sudoers_rule_is_exactly_the_role_table_rule():
    rules = [
        line
        for line in SUDOERS.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    assert rules == [load_roles()["activation"]["sudoers"]]
    assert "." not in SUDOERS.name and not SUDOERS.name.endswith("~")


def test_the_sudoers_rule_parses():
    visudo = next(
        (path for path in ("/usr/sbin/visudo", "/usr/bin/visudo") if os.access(path, os.X_OK)),
        None,
    )
    if visudo is None:
        pytest.skip("visudo is not installed")
    result = subprocess.run(
        [visudo, "-c", "-f", str(SUDOERS)], check=False, capture_output=True, text=True, timeout=10
    )
    assert result.returncode == 0, result.stdout + result.stderr


# --- deploy.yml ---------------------------------------------------------------------------


def workflow():
    return yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)


def step(name):
    return next(item for item in workflow()["jobs"]["deploy-vps"]["steps"] if item["name"] == name)


def run_step(name, tmp_path, expressions, extra_env=None):
    """Render a step the way Actions does (expressions first) and run it with bash -e."""
    item = step(name)

    def render(value):
        for expression, replacement in expressions.items():
            value = value.replace(expression, replacement)
        return value

    assert "${{" not in render(item["run"]), "expressions must reach the shell through env"
    env = {
        **os.environ,
        **workflow().get("env", {}),
        **{key: render(value) for key, value in item.get("env", {}).items()},
        "TMPDIR": str(tmp_path),
        **(extra_env or {}),
    }
    return subprocess.run(
        ["bash", "-e", "-c", render(item["run"])],
        check=False,
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_the_workflow_keeps_its_safeguards():
    flow = workflow()
    job = flow["jobs"]["deploy-vps"]
    assert flow["on"]["workflow_dispatch"]["inputs"]["image_tag"]["required"] == "true"
    assert job["environment"] == "production"
    assert flow["permissions"] == {"contents": "read"}
    assert job["permissions"] == {"contents": "read", "attestations": "read"}
    assert flow["concurrency"] == {"group": "deploy-production", "cancel-in-progress": "false"}
    assert [item["name"] for item in job["steps"]] == [
        "Validate tag format",
        "Install oras",
        "Pull and verify the workers bundle",
        "Deploy to VPS",
    ]
    for item in job["steps"]:
        assert "${{" not in item["run"], item["name"]


def test_the_workflow_installs_the_same_pinned_oras_as_the_vps():
    env = workflow()["env"]
    assert re.fullmatch(r"\d+\.\d+\.\d+", env["ORAS_VERSION"])
    assert re.fullmatch(r"[0-9a-f]{64}", env["ORAS_SHA256"])
    install = step("Install oras")["run"]
    assert "sha256sum --check --strict" in install
    readme = (ROOT / "ops/vps/README.md").read_text()
    assert f"oras_{env['ORAS_VERSION']}_linux_amd64.tar.gz" in readme
    assert env["ORAS_SHA256"] in readme


@pytest.mark.parametrize(
    "payload",
    [
        "main-abcdef0",
        CANDIDATE,
        f"{CANDIDATE}\n",
        'main-abcdef0"; touch injected; #',
        "$(touch injected)",
        "main-abcdef0; touch injected",
    ],
)
def test_workflow_tag_validation_never_executes_input(tmp_path, payload):
    result = run_step("Validate tag format", tmp_path, {"${{ inputs.image_tag }}": payload})
    assert not (tmp_path / "injected").exists(), "workflow executed image_tag as shell code"
    assert (result.returncode == 0) == (payload == CANDIDATE)


@pytest.fixture
def bundle_step(tmp_path):
    host = Host(tmp_path / "host")
    gh = write_tool(
        host.bin,
        "gh",
        "import json, os, sys\n"
        "with open(os.environ['FAKE_CALLS'], 'a') as log:\n"
        "    log.write(json.dumps({'tool': 'gh', 'argv': sys.argv[1:]}) + '\\n')\n"
        "sys.exit(int(os.environ.get('FAKE_GH_EXIT', '0')))\n",
    )
    work = tmp_path / "work"
    work.mkdir()

    def run(payload=CANDIDATE, gh_exit=0):
        output = work / "github_output"
        output.write_text("")
        host.reset_calls()
        result = run_step(
            "Pull and verify the workers bundle",
            work,
            {
                "${{ inputs.image_tag }}": payload,
                "${{ github.repository }}": "VINIClUS/limnopulse",
                "${{ github.token }}": "fake-token",
            },
            {**host.env, "GITHUB_OUTPUT": str(output), "FAKE_GH_EXIT": str(gh_exit)},
        )
        return result, output.read_text(), host.calls()

    run.host, run.gh, run.work = host, gh, work
    return run


def test_the_bundle_step_verifies_the_attestation_then_outputs_the_sha(bundle_step):
    digest = bundle_step.host.publish(CANDIDATE)
    result, output, calls = bundle_step()
    assert result.returncode == 0, result.stderr
    assert output == f"sha256={digest}\n"
    assert [call["tool"] for call in calls] == ["oras", "gh"]
    pulled = calls[0]["argv"]
    assert pulled[:3] == ["pull", "--no-tty", "-o"]
    assert pulled[4] == f"ghcr.io/viniclus/limnopulse/workers:{CANDIDATE}"
    assert calls[1]["argv"] == [
        "attestation",
        "verify",
        f"{pulled[3]}/limnopulse-workers-{CANDIDATE}.tar.gz",
        "--repo",
        "VINIClUS/limnopulse",
        "--signer-workflow",
        "VINIClUS/limnopulse/.github/workflows/build.yml",
        "--source-ref",
        "refs/heads/main",
        "--source-digest",
        CANDIDATE.removeprefix("main-"),
        "--deny-self-hosted-runners",
    ]
    assert not os.path.exists(pulled[3]), "the pulled bundle must be removed"


def test_the_bundle_step_refuses_a_tag_without_a_bundle(bundle_step):
    result, output, calls = bundle_step()
    assert result.returncode == 1
    assert "has no workers bundle" in result.stdout
    assert output == ""
    assert [call["tool"] for call in calls] == ["oras"]


def test_the_bundle_step_refuses_a_failed_attestation(bundle_step):
    bundle_step.host.publish(CANDIDATE)
    result, output, _ = bundle_step(gh_exit=1)
    assert result.returncode != 0
    assert output == ""


def test_the_bundle_step_refuses_extra_pulled_files(bundle_step):
    bundle_step.host.publish(CANDIDATE)
    (bundle_step.host.registry / CANDIDATE / "extra").write_text("x")
    result, output, calls = bundle_step()
    assert result.returncode == 1
    assert output == ""
    assert "gh" not in [call["tool"] for call in calls]


@pytest.mark.parametrize("payload", ["main-abcdef0", "$(touch injected)"])
def test_the_bundle_step_revalidates_the_tag(bundle_step, payload):
    result, _, calls = bundle_step(payload=payload)
    assert result.returncode == 1
    assert calls == []
    assert not (bundle_step.work / "injected").exists()


@pytest.mark.parametrize(
    ("payload", "sha"),
    [
        (CANDIDATE, CANDIDATE_SHA),
        ('main-abcdef0"; touch injected; #', CANDIDATE_SHA),
        ("$(touch injected)", CANDIDATE_SHA),
        (CANDIDATE, ""),
        (CANDIDATE, f"{CANDIDATE_SHA} extra"),
        (CANDIDATE, "$(touch injected)"),
        (CANDIDATE, CANDIDATE_SHA + "\n"),
    ],
)
def test_workflow_ssh_receives_the_request_as_one_literal_argument(tmp_path, payload, sha):
    ssh = tmp_path / "ssh"
    capture = tmp_path / "ssh.json"
    ssh.write_text(
        f"#!{sys.executable}\nimport json, sys\nfrom pathlib import Path\n"
        f"Path({str(capture)!r}).write_text(json.dumps(sys.argv[1:]))\n"
    )
    ssh.chmod(0o755)
    result = run_step(
        "Deploy to VPS",
        tmp_path,
        {
            "${{ inputs.image_tag }}": payload,
            "${{ steps.bundle.outputs.sha256 }}": sha,
            "${{ secrets.VPS_SSH_KEY }}": "fake-key",
            "${{ secrets.VPS_KNOWN_HOSTS }}": "fake-host",
        },
        {"PATH": f"{tmp_path}:{os.environ['PATH']}"},
    )
    assert not (tmp_path / "injected").exists()
    valid = (payload, sha) == (CANDIDATE, CANDIDATE_SHA)
    assert (result.returncode == 0) == valid, result.stderr
    if not valid:
        assert not capture.exists()
        return
    args = json.loads(capture.read_text())
    assert args[-2].startswith("limnopulse-deploy@")
    assert args[-1] == f"{CANDIDATE} {CANDIDATE_SHA}"
    assert re.fullmatch(load_roles()["deploy"]["forced_command"], args[-1])
    assert not list(tmp_path.glob("tmp.*")), "temporary SSH credentials must be removed"


def test_the_workflow_request_is_what_the_forced_command_accepts():
    forced = load_roles()["deploy"]["forced_command"]
    assert f"forced_command='{forced}'" in step("Deploy to VPS")["run"]


def test_the_workflow_never_stages_tags_from_the_old_short_form():
    assert "{7,40}" not in WORKFLOW.read_text()
    assert "{7,40}" not in SCRIPT.read_text()


def test_the_bootstrap_covers_the_helper_dirs_and_every_unit_role():
    readme = (ROOT / "ops/vps/README.md").read_text()
    table = load_roles()
    paths = table["paths"]
    assert table["activation"]["helper"] in readme
    assert "visudo -c -f" in readme and "\nvisudo -c\n" in readme
    assert (
        f"install -d -o root -g root -m 0755 {paths['workers_root']} {paths['releases']} {paths['state']}"
        in readme
    )
    assert f"install -d -o root -g root -m 0700 {paths['credentials']}" in readme
    unit_roles = [role for role in table["roles"] if "credentials" in role]
    for role in unit_roles:
        credentials = ", ".join(f"`{credential}`" for credential in role["credentials"])
        assert f"| `{role['name']}` | {credentials} |" in readme
    names = " ".join(role["name"] for role in unit_roles)
    assert f"for role in {names}; do" in readme
    assert f'install -d -o root -g root -m 0700 "{paths["credentials"]}/$role"' in readme
