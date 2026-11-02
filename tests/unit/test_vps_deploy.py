"""Exercise the deployed Bash script; only Docker, sudo, oras, systemctl, gh and SSH are fakes.

The ADR-020 flow (ops/workers/README.md, "Deploy"): stage the workers bundle, start Compose
with each deploy profile whose worker is enabled on the host (removing each disabled one), wait
for api and each enabled profiled worker, activate the bundle, then record IMAGE_TAG and
WORKERS_BUNDLE_SHA256 in .env.image. A failure after staging moves the API, the profiled
workers and the bundle back to the recorded release, under the same host-switch rule.
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml
from pydantic import TypeAdapter, ValidationError

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
SWITCH = "TELEMETRY_V2_WRITE_ENABLED"
UNSET = "<unset>"
WITH_BUNDLE = f"IMAGE_TAG={PREVIOUS}\nWORKERS_BUNDLE_SHA256={PREVIOUS_SHA}\n"
# Production today: a release from before ADR-020, with no workers bundle.
IMAGE_ONLY = f"IMAGE_TAG={PREVIOUS}\n"
# What the helper's `status` reports on a host consistent with each.
BUNDLE_STATUS = (
    f"current {PREVIOUS}\nrelease {PREVIOUS} sha256={PREVIOUS_SHA} current\n"
    "floor email-worker 1\nunit limnopulse-email-worker.service\n"
)
NO_BUNDLE_STATUS = "current none\n"

# Parses `compose --env-file .env --env-file <file> -f compose.production.yaml
# [--profile ingestion]... <operation> <args>`. The candidate env file is the promotion; the
# recorded .env.image is the rollback. Like Compose, the active profiles are the --profile
# flags, else COMPOSE_PROFILES from the environment when set (even empty), else from the env
# files; `config` lists the profiled worker only with its profile active, and `ps`/`rm` refuse
# it without the profile. FAKE_WORKER_STATE exists while the worker has a container.
FAKE_DOCKER = """\
import json, os, pathlib, sys
args = sys.argv[1:]
entry = {"tool": "docker", "argv": args, "tag": None, "profiles": [], "op": None, "rest": []}
entry["compose_profiles"] = os.environ.get("COMPOSE_PROFILES")
def env_value(path, key):
    value = None
    if path is not None and path.exists():
        for line in path.read_text().splitlines():
            if line.startswith(key + "="):
                value = line.split("=", 1)[1]
    return value
if args[:1] == ["compose"]:
    env_file = pathlib.Path(args[4]) if len(args) > 4 else None
    entry["tag"] = env_value(env_file, "IMAGE_TAG")
    index = 7
    while args[index:index + 1] == ["--profile"]:
        entry["profiles"].append(args[index + 1])
        index += 2
    entry["op"], entry["rest"] = (args[index], args[index + 1:]) if index < len(args) else (None, [])
with open(os.environ["FAKE_CALLS"], "a") as log:
    log.write(json.dumps(entry) + "\\n")
worker = "telemetry-ingest-worker"
state = pathlib.Path(os.environ["FAKE_WORKER_STATE"])
if args[:1] == ["compose"]:
    if args[:4] != ["compose", "--env-file", ".env", "--env-file"]:
        sys.exit(90)
    if args[4] not in (".env.image", ".env.image.candidate") or not env_file.exists():
        sys.exit(90)
    if args[5:7] != ["-f", "compose.production.yaml"] or set(entry["profiles"]) - {"ingestion"}:
        sys.exit(95)
    chosen = ",".join(entry["profiles"]) or entry["compose_profiles"]
    if chosen is None:
        for path in (pathlib.Path(".env"), env_file):
            value = env_value(path, "COMPOSE_PROFILES")
            chosen = chosen if value is None else value
    profiled = "ingestion" in (chosen or "").split(",")
    operation, rest = entry["op"], entry["rest"]
    expected = {
        "config": [["--format", "json"]],
        "pull": [[]],
        "up": [["-d", "--remove-orphans"]],
        "stop": [[]],
        "rm": [["--stop", "--force", worker]],
        "ps": [["-q", "api"], ["-q", worker], ["-a", "-q", worker]],
    }
    if rest not in expected.get(operation, []):
        sys.exit(93)
    if worker in rest and not profiled:
        sys.exit(97)
    key = "ps-a" if rest[:1] == ["-a"] else operation
    candidate = args[4] == ".env.image.candidate"
    if candidate and key == os.environ.get("FAKE_FAIL_OPERATION"):
        sys.exit(21)
    if not candidate and operation != "config" and os.environ["FAKE_FAIL_ROLLBACK"] in ("all", key):
        sys.exit(22)
    if operation == "config":
        switch = os.environ["FAKE_SWITCH" if candidate else "FAKE_ROLLBACK_SWITCH"]
        services = {"api": {"environment": {"APP_ENV": "prod"}}}
        if profiled:
            environment = {"APP_ENV": "prod"}
            if switch != "<unset>":
                environment["TELEMETRY_V2_WRITE_ENABLED"] = switch
            services[worker] = {"profiles": ["ingestion"], "environment": environment}
        print(json.dumps({"name": "limnopulse", "services": services}))
    elif operation == "rm" and os.environ.get("FAKE_KEEP_WORKER") != "1":
        state.unlink(missing_ok=True)
    elif operation == "up" and profiled:
        state.touch()
    elif key == "ps-a":
        if state.exists():
            print("fake-worker-container")
    elif operation == "ps":
        print(f"fake-{rest[1]}-container")
elif args[:1] == ["inspect"]:
    if args[:3] != ["inspect", "-f", "{{.State.Health.Status}}"] or len(args) != 4:
        sys.exit(94)
    service = args[3][len("fake-"):-len("-container")]
    print("unhealthy" if service == os.environ.get("FAKE_UNHEALTHY") else "healthy")
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
    worker_exists: bool


def steps(calls):
    """The deploy's actions in order: helper calls, Compose operations and the image prune."""
    flow = []
    for call in calls:
        argv = call["argv"]
        if call["tool"] == "sudo":
            flow.append(tuple(argv[2:]))
        elif call["tool"] == "docker" and argv[0] == "compose":
            operation, tag, profiles = call["op"], call["tag"], ",".join(call["profiles"])
            if call["rest"][:1] == ["-a"]:
                flow.append(("ps-a", call["rest"][-1], tag))
            elif operation in ("ps", "rm"):
                flow.append((operation, call["rest"][-1], tag))
            elif operation in ("pull", "up", "stop"):
                flow.append((operation, tag, profiles))
            else:
                flow.append((operation, tag))
        elif call["tool"] == "docker" and argv[0] == "image":
            flow.append(("prune",))
    return flow


@pytest.fixture
def deploy(tmp_path):
    host = Host(tmp_path / "host")
    stack = tmp_path / "stack with spaces"
    stack.mkdir()
    (stack / "compose.production.yaml").write_text("services: {}\n")
    write_tool(host.bin, "docker", FAKE_DOCKER)
    write_tool(host.bin, "sudo", FAKE_SUDO)
    fake_helper = write_tool(host.bin, "fake-activate", FAKE_HELPER)
    log_file = tmp_path / "deploy.log"
    worker_state = tmp_path / "worker-container"

    def run(
        *,
        previous=WITH_BUNDLE,
        request=f"{CANDIDATE} {CANDIDATE_SHA}",
        switch="true",
        rollback_switch=None,
        fail="",
        unhealthy="",
        rollback_fail="",
        helper_exits=None,
        status=None,
        keep_worker=False,
        environment=None,
        dotenv="APP_ENV=prod\n",
        real_helper=False,
    ):
        (stack / ".env").write_text(dotenv)
        image_file = stack / ".env.image"
        if previous is not None:
            image_file.write_text(previous)
        if status is None:
            status = BUNDLE_STATUS if previous == WITH_BUNDLE else NO_BUNDLE_STATUS
        # The previous release left the worker running.
        worker_state.touch()
        host.reset_calls()
        env = {
            **host.env,
            "SSH_ORIGINAL_COMMAND": request,
            "LIMNOPULSE_STACK_DIR": str(stack),
            "LIMNOPULSE_DEPLOY_LOG": str(log_file),
            "LIMNOPULSE_HEALTH_TIMEOUT_SECONDS": "0",
            "FAKE_SWITCH": switch,
            "FAKE_ROLLBACK_SWITCH": switch if rollback_switch is None else rollback_switch,
            "FAKE_FAIL_OPERATION": fail,
            "FAKE_UNHEALTHY": unhealthy,
            "FAKE_FAIL_ROLLBACK": rollback_fail,
            "FAKE_HELPER_EXITS": json.dumps(helper_exits or {}),
            "FAKE_HELPER_STATUS": status,
            "FAKE_KEEP_WORKER": "1" if keep_worker else "0",
            "FAKE_WORKER_STATE": str(worker_state),
            "FAKE_ACTIVATE": str(HELPER if real_helper else fake_helper),
            **(environment or {}),
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
        calls = host.calls()
        # Only the script's --profile flags choose profiles, never COMPOSE_PROFILES.
        assert all(
            call["compose_profiles"] == ""
            for call in calls
            if call["tool"] == "docker" and call["argv"][0] == "compose"
        )
        return Deploy(result, image_file, stack, calls, log, worker_state.exists())

    run.host = host
    return run


def start(tag, enabled):
    """Compose's steps for one release: pull, remove a disabled worker, up, check it is gone."""
    profiles = "ingestion" if enabled else ""
    if enabled:
        return [("pull", tag, profiles), ("up", tag, profiles)]
    return [("pull", tag, ""), ("rm", WORKER, tag), ("up", tag, ""), ("ps-a", WORKER, tag)]


def promotion(through, enabled=True):
    """The candidate's steps in order, up to and including `through`."""
    flow = [
        ("status",),
        ("config", CANDIDATE),
        ("stage", CANDIDATE, CANDIDATE_SHA),
        *start(CANDIDATE, enabled),
        ("ps", "api", CANDIDATE),
        *([("ps", WORKER, CANDIDATE)] if enabled else []),
        ("activate", CANDIDATE),
    ]
    return flow[: flow.index(through) + 1]


def rollback(previous, enabled=True, *, activated=True):
    """The rollback's steps. The bundle moves back only if the candidate's activation ran."""
    if not activated:
        bundle = []
    elif previous == WITH_BUNDLE:
        bundle = [("activate", PREVIOUS)]
    else:
        bundle = [("deactivate",)]
    return [("config", PREVIOUS), *start(PREVIOUS, enabled), *bundle]


def promoted(run):
    assert run.result.returncode == 0, run.result.stderr + run.log
    assert run.image_file.read_text() == (
        f"IMAGE_TAG={CANDIDATE}\nWORKERS_BUNDLE_SHA256={CANDIDATE_SHA}\n"
    )
    assert not (run.stack / ".env.image.candidate").exists()
    assert "deploy ok" in run.log


def test_an_enabled_worker_is_started_and_waited_for(deploy):
    run = deploy(switch="true")
    promoted(run)
    assert steps(run.calls) == [*promotion(("activate", CANDIDATE)), ("prune",)]


def test_a_disabled_worker_is_removed_not_started_and_not_waited_for(deploy):
    # A disabled worker would park and never turn healthy; the deploy must not wait on it.
    run = deploy(switch="false", unhealthy=WORKER)
    promoted(run)
    assert steps(run.calls) == [*promotion(("activate", CANDIDATE), enabled=False), ("prune",)]
    assert ("ps", WORKER, CANDIDATE) not in steps(run.calls)
    assert not run.worker_exists


@pytest.mark.parametrize(
    ("dotenv", "environment"),
    [
        ("APP_ENV=prod\nCOMPOSE_PROFILES=ingestion\n", None),
        ("APP_ENV=prod\n", {"COMPOSE_PROFILES": "ingestion"}),
        ("COMPOSE_PROFILES=ingestion\n", {"COMPOSE_PROFILES": "ingestion"}),
    ],
    ids=["env-file", "environment", "both"],
)
def test_compose_profiles_cannot_bring_a_disabled_worker_back(deploy, dotenv, environment):
    # Plain `up` would otherwise take its profiles from COMPOSE_PROFILES and restart the worker.
    run = deploy(switch="false", dotenv=dotenv, environment=environment)
    promoted(run)
    assert steps(run.calls) == [*promotion(("activate", CANDIDATE), enabled=False), ("prune",)]
    assert not run.worker_exists


def test_a_disabled_worker_that_outlives_up_fails_the_deploy(deploy):
    run = deploy(switch="false", keep_worker=True)
    assert run.result.returncode == 1
    assert steps(run.calls) == promotion(("ps-a", WORKER, CANDIDATE), enabled=False) + rollback(
        WITH_BUNDLE, enabled=False, activated=False
    )
    assert "disabled worker telemetry-ingest-worker still has a container" in run.log
    # The rollback cannot remove it either, so it is reported for a manual look.
    assert "rollback failed" in run.log
    assert run.image_file.read_text() == WITH_BUNDLE


def pydantic_bool(value):
    try:
        return TypeAdapter(bool).validate_python(value)
    except ValidationError:
        return None


@pytest.mark.parametrize(
    "value",
    [
        "true",
        "True",
        "TRUE",
        "1",
        "yes",
        "on",
        "t",
        "Y",
        "false",
        "False",
        "0",
        "no",
        "off",
        "f",
        "N",
        "",
        " true",
        "true ",
        "maybe",
        "2",
        "enabled",
        UNSET,
    ],
)
def test_the_host_switch_follows_the_workers_own_boolean_parsing(deploy, value):
    run = deploy(switch=value)
    expected = None if value == UNSET else pydantic_bool(value)
    if expected is None:
        assert run.result.returncode == 1
        assert steps(run.calls) == [("status",), ("config", CANDIDATE)]
        assert run.image_file.read_text() == WITH_BUNDLE
        assert not (run.stack / ".env.image.candidate").exists()
        assert "cannot resolve the profiled workers' host switches" in run.log
        return
    promoted(run)
    assert steps(run.calls)[:-1] == promotion(("activate", CANDIDATE), enabled=expected)


def test_a_failed_switch_read_stops_the_deploy_before_anything_changes(deploy):
    run = deploy(fail="config")
    assert run.result.returncode == 1
    assert steps(run.calls) == [("status",), ("config", CANDIDATE)]
    assert run.image_file.read_text() == WITH_BUNDLE
    assert not (run.stack / ".env.image.candidate").exists()


@pytest.mark.parametrize("status", [1, 3, 78])
def test_a_refused_stage_stops_the_deploy_before_anything_changes(deploy, status):
    run = deploy(helper_exits={"stage": status})
    assert run.result.returncode == status
    assert steps(run.calls) == [
        ("status",),
        ("config", CANDIDATE),
        ("stage", CANDIDATE, CANDIDATE_SHA),
    ]
    assert run.image_file.read_text() == WITH_BUNDLE
    assert not (run.stack / ".env.image.candidate").exists()
    assert "nothing changed" in run.log


FAILURES = {
    "pull": ({"fail": "pull"}, ("pull", CANDIDATE, "ingestion")),
    "up": ({"fail": "up"}, ("up", CANDIDATE, "ingestion")),
    "api health": ({"unhealthy": "api"}, ("ps", "api", CANDIDATE)),
    "parked worker": ({"unhealthy": WORKER}, ("ps", WORKER, CANDIDATE)),
    "activate": ({"helper_exits": {f"activate:{CANDIDATE}": 1}}, ("activate", CANDIDATE)),
    "activate floor": ({"helper_exits": {f"activate:{CANDIDATE}": 3}}, ("activate", CANDIDATE)),
}
DISABLED_FAILURES = {
    "pull": ({"fail": "pull"}, ("pull", CANDIDATE, "")),
    "rm": ({"fail": "rm"}, ("rm", WORKER, CANDIDATE)),
    "up": ({"fail": "up"}, ("up", CANDIDATE, "")),
    "absence check": ({"fail": "ps-a"}, ("ps-a", WORKER, CANDIDATE)),
    "api health": ({"unhealthy": "api"}, ("ps", "api", CANDIDATE)),
    "activate": ({"helper_exits": {f"activate:{CANDIDATE}": 1}}, ("activate", CANDIDATE)),
}
CASES = [(True, name) for name in sorted(FAILURES)]
CASES += [(False, name) for name in sorted(DISABLED_FAILURES)]


@pytest.mark.parametrize("previous", [WITH_BUNDLE, IMAGE_ONLY], ids=["bundle", "image-only"])
@pytest.mark.parametrize(("enabled", "failure"), CASES)
def test_a_failed_promotion_moves_api_workers_and_bundle_back(deploy, enabled, failure, previous):
    options, last = (FAILURES if enabled else DISABLED_FAILURES)[failure]
    run = deploy(previous=previous, switch="true" if enabled else "false", **options)
    assert run.result.returncode == 1
    # Before step 4 the bundle never moved, so the rollback leaves it alone.
    activated = last[0] == "activate"
    expected = promotion(last, enabled) + rollback(previous, enabled, activated=activated)
    assert steps(run.calls) == expected
    assert run.image_file.read_text() == previous
    assert not (run.stack / ".env.image.candidate").exists()
    assert "deploy ok" not in run.log
    assert "rollback failed" not in run.log
    assert ("the workers bundle never moved" in run.log) is not activated


@pytest.mark.parametrize(("switch", "rollback_switch"), [("true", "false"), ("false", "true")])
def test_rollback_applies_the_host_switch_as_it_is_at_rollback(deploy, switch, rollback_switch):
    run = deploy(
        switch=switch,
        rollback_switch=rollback_switch,
        helper_exits={f"activate:{CANDIDATE}": 1},
    )
    assert run.result.returncode == 1
    enabled = switch == "true"
    assert steps(run.calls) == promotion(("activate", CANDIDATE), enabled) + rollback(
        WITH_BUNDLE, not enabled
    )


def test_rollback_keeps_the_promotion_switches_when_it_cannot_reread_them(deploy):
    run = deploy(switch="false", rollback_switch="maybe", fail="up")
    assert run.result.returncode == 1
    assert steps(run.calls) == promotion(("up", CANDIDATE, ""), enabled=False) + rollback(
        WITH_BUNDLE, enabled=False, activated=False
    )
    assert "rolling back with the values read at promotion" in run.log
    assert "rollback failed" not in run.log


def test_rollback_never_overrides_a_floor_refusal(deploy):
    run = deploy(helper_exits={f"activate:{CANDIDATE}": 1, f"activate:{PREVIOUS}": 3})
    assert run.result.returncode == 1
    assert steps(run.calls) == promotion(("activate", CANDIDATE)) + rollback(WITH_BUNDLE)
    assert not any("--override-floor" in call["argv"] for call in run.calls)
    assert "needs a manual decision" in run.log
    assert "needs a manual decision" in run.result.stdout
    assert "rollback failed" in run.log
    assert run.image_file.read_text() == WITH_BUNDLE


def test_failed_rollback_is_reported_and_does_not_recurse(deploy):
    run = deploy(fail="up", rollback_fail="all")
    assert run.result.returncode == 1
    flow = promotion(("up", CANDIDATE, "ingestion"))
    assert steps(run.calls) == flow + [
        ("config", PREVIOUS),
        ("pull", PREVIOUS, "ingestion"),
        ("up", PREVIOUS, "ingestion"),
    ]
    assert run.log.count("deploy failed") == 1
    assert "rollback failed" in run.log.lower()
    assert "deploy ok" not in run.log
    assert run.image_file.read_text() == WITH_BUNDLE


@pytest.mark.parametrize("enabled", [True, False])
@pytest.mark.parametrize("failure", ["api health", "activate"])
def test_a_registry_outage_during_rollback_still_starts_the_cached_previous_tag(
    deploy, failure, enabled
):
    options, last = (FAILURES if enabled else DISABLED_FAILURES)[failure]
    run = deploy(switch="true" if enabled else "false", rollback_fail="pull", **options)
    assert run.result.returncode == 1
    activated = last[0] == "activate"
    assert steps(run.calls) == promotion(last, enabled) + rollback(
        WITH_BUNDLE, enabled, activated=activated
    )
    assert "pull failed; starting from the cached images" in run.log
    # The outage is still reported: the previous images may be older than the recorded tag's.
    assert "rollback failed" in run.log
    assert run.image_file.read_text() == WITH_BUNDLE


@pytest.mark.parametrize("failure", ["up", "api health", "activate", "activate floor"])
def test_the_rollback_is_logged_once_and_reaches_the_workflow(deploy, failure):
    options, _ = FAILURES[failure]
    run = deploy(**options)
    assert run.result.returncode == 1
    assert run.log.count("deploy failed") == 1
    assert run.result.stdout.count("deploy failed") == 1
    assert run.result.stdout.count("rolling back to prev=") == 1


@pytest.mark.parametrize("enabled", [True, False])
@pytest.mark.parametrize("failure", ["pull", "up", "api health", "activate"])
def test_first_deploy_failure_stops_candidate_and_deactivates(deploy, failure, enabled):
    options, last = (FAILURES if enabled else DISABLED_FAILURES)[failure]
    run = deploy(previous=None, switch="true" if enabled else "false", **options)
    assert run.result.returncode == 1
    deactivation = [("deactivate",)] if last[0] == "activate" else []
    assert steps(run.calls) == promotion(last, enabled) + [
        ("stop", CANDIDATE, "ingestion"),
        *deactivation,
    ]
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


def test_a_second_deploy_waits_for_nothing_and_changes_nothing(deploy, tmp_path):
    lock = tmp_path / "stack with spaces" / ".deploy.lock"
    with lock.open("a") as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        run = deploy()
    assert run.result.returncode == 1
    assert run.calls == []
    assert "another deploy is running" in run.log
    assert run.image_file.read_text() == WITH_BUNDLE
    assert not (run.stack / ".env.image.candidate").exists()


# .env.image values the deploy refuses before asking the helper anything.
MALFORMED_RECORDS = {
    "empty": "",
    "no tag": f"WORKERS_BUNDLE_SHA256={PREVIOUS_SHA}\n",
    "empty tag": "IMAGE_TAG=\n",
    "not a release": "IMAGE_TAG=latest\n",
    "short hex": "IMAGE_TAG=main-abcdef\n",
    "long hex": f"IMAGE_TAG={PREVIOUS}0\n",
    "uppercase": f"IMAGE_TAG={PREVIOUS.upper()}\n",
    "trailing space": f"IMAGE_TAG={PREVIOUS} \n",
    "carriage return": f"IMAGE_TAG={PREVIOUS}\r\n",
    "short tag with a bundle": f"IMAGE_TAG=main-abcdef0\nWORKERS_BUNDLE_SHA256={PREVIOUS_SHA}\n",
    "short sha": f"IMAGE_TAG={PREVIOUS}\nWORKERS_BUNDLE_SHA256={PREVIOUS_SHA[:-1]}\n",
    "uppercase sha": f"IMAGE_TAG={PREVIOUS}\nWORKERS_BUNDLE_SHA256={PREVIOUS_SHA.upper()}\n",
    "sha with a space": f"IMAGE_TAG={PREVIOUS}\nWORKERS_BUNDLE_SHA256={PREVIOUS_SHA} x\n",
}


@pytest.mark.parametrize("previous", MALFORMED_RECORDS.values(), ids=MALFORMED_RECORDS.keys())
def test_a_malformed_record_stops_the_deploy_before_anything_changes(deploy, previous):
    run = deploy(previous=previous, status=BUNDLE_STATUS)
    assert run.result.returncode == 1
    assert run.calls == []
    assert "host state inconsistent" in run.log
    assert "fix it by hand" in run.log
    assert run.image_file.read_bytes() == previous.encode()
    assert not (run.stack / ".env.image.candidate").exists()


OTHER = f"main-{'e' * 40}"
# The helper's current bundle against what .env.image records.
INCONSISTENT_HOSTS = {
    "recorded bundle, none current": (WITH_BUNDLE, NO_BUNDLE_STATUS),
    "recorded bundle, another current": (
        WITH_BUNDLE,
        (
            f"current {OTHER}\nrelease {PREVIOUS} sha256={PREVIOUS_SHA}\n"
            f"release {OTHER} sha256={PREVIOUS_SHA} current\n"
        ),
    ),
    "recorded bundle, another sha": (
        WITH_BUNDLE,
        f"current {PREVIOUS}\nrelease {PREVIOUS} sha256={'e' * 64} current\n",
    ),
    "recorded bundle, unrecorded sha": (
        WITH_BUNDLE,
        f"current {PREVIOUS}\nrelease {PREVIOUS} sha256=unrecorded current\n",
    ),
    "recorded bundle, staged but not current": (
        WITH_BUNDLE,
        f"current none\nrelease {PREVIOUS} sha256={PREVIOUS_SHA}\n",
    ),
    "no recorded bundle, one current": (IMAGE_ONLY, BUNDLE_STATUS),
    "no record, one current": (None, BUNDLE_STATUS),
}


@pytest.mark.parametrize(
    ("previous", "status"), INCONSISTENT_HOSTS.values(), ids=INCONSISTENT_HOSTS.keys()
)
def test_a_bundle_other_than_the_recorded_one_stops_the_deploy(deploy, previous, status):
    run = deploy(previous=previous, status=status)
    assert run.result.returncode == 1
    assert steps(run.calls) == [("status",)]
    assert "host state inconsistent" in run.log
    assert "fix it by hand" in run.log
    if previous is None:
        assert not run.image_file.exists()
    else:
        assert run.image_file.read_text() == previous
    assert not (run.stack / ".env.image.candidate").exists()


def test_a_helper_that_cannot_report_its_status_stops_the_deploy(deploy):
    run = deploy(helper_exits={"status": 1})
    assert run.result.returncode == 1
    assert steps(run.calls) == [("status",)]
    assert "the activation helper cannot report its status" in run.log
    assert run.image_file.read_text() == WITH_BUNDLE


def test_a_short_tag_from_before_adr_020_is_still_a_valid_record(deploy):
    run = deploy(previous="IMAGE_TAG=main-abcdef0\n", status=NO_BUNDLE_STATUS)
    promoted(run)


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
    assert tools.index("oras") < tools.index("docker", tools.index("oras"))
    assert ["daemon-reload"] in [call["argv"] for call in run.calls if call["tool"] == "systemctl"]


def test_end_to_end_tampered_bundle_is_refused_before_docker(deploy):
    host = deploy.host
    digest = host.publish(CANDIDATE)
    tarball_path = host.registry / CANDIDATE / f"limnopulse-workers-{CANDIDATE}.tar.gz"
    tarball_path.write_bytes(tarball_path.read_bytes() + b"\0")
    run = deploy(previous=IMAGE_ONLY, request=f"{CANDIDATE} {digest}", real_helper=True)
    assert run.result.returncode == 1
    assert steps(run.calls) == [("status",), ("config", CANDIDATE), ("stage", CANDIDATE, digest)]
    assert [call["tool"] for call in run.calls] == ["sudo", "docker", "sudo", "oras"]
    assert run.image_file.read_text() == IMAGE_ONLY
    assert not (host.releases / CANDIDATE).exists()


def test_end_to_end_missing_bundle_is_refused_before_docker(deploy):
    run = deploy(previous=IMAGE_ONLY, request=f"{CANDIDATE} {CANDIDATE_SHA}", real_helper=True)
    assert run.result.returncode == 1
    assert steps(run.calls) == [
        ("status",),
        ("config", CANDIDATE),
        ("stage", CANDIDATE, CANDIDATE_SHA),
    ]
    assert run.image_file.read_text() == IMAGE_ONLY


def test_end_to_end_promotion_from_a_release_with_a_bundle(deploy):
    host = deploy.host
    previous = host.deploy_release(PREVIOUS)
    digest = host.publish(CANDIDATE)
    run = deploy(
        previous=f"IMAGE_TAG={PREVIOUS}\nWORKERS_BUNDLE_SHA256={previous}\n",
        request=f"{CANDIDATE} {digest}",
        real_helper=True,
    )
    promoted_to = f"IMAGE_TAG={CANDIDATE}\nWORKERS_BUNDLE_SHA256={digest}\n"
    assert run.result.returncode == 0, run.log
    assert run.image_file.read_text() == promoted_to
    assert os.readlink(host.current) == f"releases/{CANDIDATE}"


def test_end_to_end_unhealthy_promotion_leaves_the_previous_bundle_alone(deploy):
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
    assert ("stage", CANDIDATE, digest) in steps(run.calls)
    assert not [step for step in steps(run.calls) if step[0] == "activate"]
    assert os.readlink(host.current) == f"releases/{PREVIOUS}"
    assert sorted(path.name for path in host.releases.iterdir()) == sorted([PREVIOUS, CANDIDATE])
    assert run.image_file.read_text() == recorded


def test_end_to_end_rollback_reactivates_the_previous_bundle(deploy):
    host = deploy.host
    unit = "limnopulse-email-worker.service"
    previous = host.deploy_release(PREVIOUS)
    recorded = f"IMAGE_TAG={PREVIOUS}\nWORKERS_BUNDLE_SHA256={previous}\n"
    digest = host.publish(CANDIDATE)
    # The running worker's candidate binary exits 78: try-restart leaves the unit failed.
    host.env.update(FAKE_ENABLED=unit, FAKE_FAIL_RESTART=unit, FAKE_FAIL_RESTART_RELEASE=CANDIDATE)
    host.set_unit_states({unit: "active"})
    run = deploy(previous=recorded, request=f"{CANDIDATE} {digest}", real_helper=True)
    assert run.result.returncode == 1, run.log
    assert ("activate", CANDIDATE) in steps(run.calls)
    assert steps(run.calls)[-1] == ("activate", PREVIOUS)
    assert "rollback failed" not in run.log
    assert os.readlink(host.current) == f"releases/{PREVIOUS}"
    # The rollback brings the worker back on the previous binary.
    assert host.unit_states()[unit] == "active"
    assert not (host.state / "running.json").exists()
    assert run.image_file.read_text() == recorded


def test_end_to_end_rollback_to_a_release_without_a_bundle_deactivates(deploy):
    host = deploy.host
    # The activation installs the units and swaps current, then systemctl fails its try-restart.
    host.env["FAKE_ENABLED"] = "limnopulse-email-worker.service"
    host.env["FAKE_FAIL_SYSTEMCTL"] = "try-restart"
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
    forced, profiles, activate = script_setup("$FORCED_COMMAND", "${PROFILES[*]}", "$ACTIVATE")
    assert forced == table["deploy"]["forced_command"]
    assert profiles.split() == table["deploy"]["compose_profiles"]
    assert activate == table["activation"]["helper"]
    assert script_setup("${PROFILED_WORKERS[@]}") == [
        f"{role['compose_service']} {role['compose_profile']} {role['host_enable_env']}"
        for role in table["roles"]
        if role.get("compose_profile") in table["deploy"]["compose_profiles"]
    ]


def _real_compose_switches(value):
    """Run the script's switch parser on what the real Compose renders for production."""
    parser = "\n".join(script_setup("$SWITCHES_PY"))
    entries = script_setup("${PROFILED_WORKERS[@]}")
    env = {**os.environ, "IMAGE_TAG": "main-" + "0" * 40}
    env.pop(SWITCH, None)
    if value is not None:
        env[SWITCH] = value
    rendered = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            ".env.production.example",
            "-f",
            "compose.production.yaml",
            "--profile",
            "ingestion",
            "config",
            "--format",
            "json",
        ],
        check=False,
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    if rendered.returncode != 0:
        pytest.skip(f"docker compose config is unavailable: {rendered.stderr.strip()[:200]}")
    return subprocess.run(
        [sys.executable, "-I", "-c", parser, *entries],
        input=rendered.stdout,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker is not installed")
@pytest.mark.parametrize(
    ("value", "state"), [(None, "disabled"), ("false", "disabled"), ("true", "enabled")]
)
def test_the_switch_parser_reads_the_real_production_compose(value, state):
    result = _real_compose_switches(value)
    assert result.returncode == 0, result.stderr
    assert result.stdout == f"{WORKER} {state}\n"


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker is not installed")
def test_the_switch_parser_refuses_a_value_the_worker_would_refuse():
    result = _real_compose_switches("maybe")
    assert result.returncode != 0
    assert "must be a boolean" in result.stderr


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
    # The script takes a short tag only as the release .env.image recorded before ADR-020.
    assert [line.strip() for line in SCRIPT.read_text().splitlines() if "{7,40}" in line] == [
        'if [ -f .env.image ] && ! [[ "$PREV_TAG" =~ ^main-[0-9a-f]{7,40}$ ]]; then'
    ]


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
