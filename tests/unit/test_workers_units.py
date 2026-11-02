"""ADR-020 S3: the systemd units in `ops/workers/systemd` and the env file templates in
`ops/workers/env`, checked against `ops/workers/roles.yaml`.

`scripts/workers/units.py` renders the units; the bundle ships the committed files. Both
systemd tools run against the committed units: `systemd-analyze verify` must accept every
unit without a diagnostic, and `systemd-analyze security` must score each service at or
below EXPOSURE_THRESHOLD.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from scripts.workers import units
from tests.support.go_sources import closure_files
from tests.support.worker_roles import ROOT, load_roles, load_credential_lines, roles_by_name

UNITS_DIR = ROOT / "ops/workers/systemd"
ENV_DIR = ROOT / "ops/workers/env"
CONTINUOUS = ("email-worker", "ses-feedback-worker", "telegram-worker")
ONESHOT = ("alert-evaluator", "notification-relay", "worker-monitor")
MONITOR = "worker-monitor"
# The highest `systemd-analyze security` exposure a unit may score. The units need the
# network (AWS), which alone costs about 1.0.
EXPOSURE_THRESHOLD = 1.2
SYSTEM_UNITS = Path("/usr/lib/systemd/system")
# The sandbox every service shares, beyond the per-role settings checked below.
HARDENING = {
    "DynamicUser": ["yes"],
    "CapabilityBoundingSet": [""],
    "AmbientCapabilities": [""],
    "NoNewPrivileges": ["yes"],
    "PrivateDevices": ["yes"],
    "PrivateTmp": ["yes"],
    "PrivateUsers": ["yes"],
    "ProtectSystem": ["strict"],
    "ProtectHome": ["yes"],
    "ProtectProc": ["invisible"],
    "ProtectKernelTunables": ["yes"],
    "ProtectKernelModules": ["yes"],
    "ProtectControlGroups": ["yes"],
    "MemoryDenyWriteExecute": ["yes"],
    "RestrictNamespaces": ["yes"],
    "RestrictSUIDSGID": ["yes"],
    "LockPersonality": ["yes"],
    "SystemCallArchitectures": ["native"],
    "SystemCallFilter": ["@system-service", "~@privileged @resources"],
    "UMask": ["0077"],
}
# Settings a worker env file must never carry: secrets (LoadCredential= delivers them),
# the unit's own environment (EnvironmentFile= would override it) and the supervision
# variables systemd sets.
FORBIDDEN_ENV = {
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "INFLUXDB_TOKEN",
    "REDIS_PASSWORD",
    "TELEGRAM_BOT_TOKEN",
    "GOTRACEBACK",
    "LIMNOPULSE_SUPERVISOR",
    "CREDENTIALS_DIRECTORY",
    "NOTIFY_SOCKET",
}
# What each role's loader refuses to start without (and, in prod, the explicit switches).
REQUIRED_ENV = {
    "alert-evaluator": {
        "APP_ENV",
        "AWS_REGION",
        "DYNAMODB_DOMAIN_TABLE",
        "INFLUXDB_URL",
        "INFLUXDB_ORG",
        "INFLUXDB_BUCKET_RAW",
        "TELEGRAM_DELIVERY_ENABLED",
    },
    "notification-relay": {
        "APP_ENV",
        "AWS_REGION",
        "DYNAMODB_DOMAIN_TABLE",
        "SQS_NOTIFICATION_JOBS_URL",
        "TELEGRAM_DELIVERY_ENABLED",
    },
    "email-worker": {
        "APP_ENV",
        "AWS_REGION",
        "DYNAMODB_DOMAIN_TABLE",
        "SQS_NOTIFICATION_JOBS_URL",
        "SES_FROM_EMAIL",
        "SES_CONFIGURATION_SET_NAME",
    },
    "ses-feedback-worker": {"APP_ENV", "AWS_REGION", "DYNAMODB_DOMAIN_TABLE", "SQS_SES_EVENTS_URL"},
    "telegram-worker": {
        "APP_ENV",
        "AWS_REGION",
        "DYNAMODB_DOMAIN_TABLE",
        "SQS_TELEGRAM_JOBS_URL",
        "TELEGRAM_DELIVERY_ENABLED",
        "TELEGRAM_BOT_TOKEN_SECRET_ARN",
        "REDIS_ADDR",
    },
    "worker-monitor": {"AWS_REGION"},
}
ENV_KEY = re.compile(r"[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+")
OUR_UNIT = re.compile(r"\blimnopulse-[a-z0-9-]+\.(?:service|timer)\b")


def parse_unit(text: str) -> dict[str, dict[str, list[str]]]:
    """Sections -> key -> every value in order: unit keys such as LoadCredential= repeat."""
    sections: dict[str, dict[str, list[str]]] = {}
    current = None
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        if line.startswith("["):
            current = sections.setdefault(line.strip("[]"), {})
            continue
        key, _, value = line.partition("=")
        current.setdefault(key, []).append(value)
    return sections


def unit(name: str) -> dict[str, dict[str, list[str]]]:
    return parse_unit((UNITS_DIR / name).read_text(encoding="utf-8"))


def role(name: str) -> dict:
    return roles_by_name(load_roles())[name]


def env_file(name: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in (ENV_DIR / f"{name}.env.example").read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        match = re.fullmatch(r"([A-Z][A-Z0-9_]*)=([^\s'\"]*)", line)
        assert match, (name, line)
        assert match.group(1) not in values, (name, line)
        values[match.group(1)] = match.group(2)
    return values


def systemd_analyze() -> str:
    tool = shutil.which("systemd-analyze")
    if tool is None:
        if os.environ.get("GITHUB_ACTIONS") == "true":
            pytest.fail("CI must run the systemd-analyze checks")
        pytest.skip("systemd-analyze is not installed")
    return tool


# --- the unit set ---------------------------------------------------------------------------


def test_the_committed_units_are_the_rendering_of_roles_yaml():
    assert units.drift(load_roles()) == [], (
        "regenerate them: .venv/bin/python -m scripts.workers.units --write"
    )
    assert units.main([]) == 0


def test_the_unit_set_is_exactly_what_roles_yaml_declares():
    declared = set()
    for entry in load_roles()["roles"]:
        if entry["lifecycle"] == "continuous":
            assert set(entry["units"]) == {"service"}
        elif entry["lifecycle"] == "oneshot":
            assert set(entry["units"]) == {"service", "timer"}
        else:
            assert "units" not in entry
        declared |= set(entry.get("units", {}).values())
    assert {path.name for path in UNITS_DIR.iterdir()} == declared
    assert len(declared) == 9
    assert {name for entry in units.unit_roles(load_roles()) for name in [entry["name"]]} == set(
        CONTINUOUS + ONESHOT
    )


def test_check_fails_and_write_repairs_a_drifted_unit(tmp_path, capsys):
    units.write(load_roles(), tmp_path)
    (tmp_path / "limnopulse-email-worker.service").write_text("[Service]\n")
    (tmp_path / "limnopulse-notifications.service").write_text("[Service]\n")
    assert units.drift(load_roles(), tmp_path) == [
        "limnopulse-email-worker.service",
        "limnopulse-notifications.service",
    ]
    assert units.main([], directory=tmp_path) == 1
    assert "limnopulse-email-worker.service" in capsys.readouterr().err
    assert units.main(["--write"], directory=tmp_path) == 0
    assert units.main([], directory=tmp_path) == 0
    assert not (tmp_path / "limnopulse-notifications.service").exists()


def test_the_timers_fire_once_a_minute_only():
    table = load_roles()
    entry = dict(role("alert-evaluator"), schedule_seconds=120)
    with pytest.raises(ValueError, match="once a minute"):
        units.render_timer(table, entry)


# --- continuous services ---------------------------------------------------------------------


@pytest.mark.parametrize("name", CONTINUOUS)
def test_a_continuous_service_is_a_watched_notify_unit_that_never_restarts_on_78(name):
    entry, parsed = role(name), unit(f"limnopulse-{name}.service")
    service = parsed["Service"]
    assert service["Type"] == ["notify"]
    assert service["NotifyAccess"] == ["main"]
    assert service["WatchdogSec"] == [str(entry["watchdog_seconds"])]
    assert service["Restart"] == ["on-failure"]
    assert service["RestartPreventExitStatus"] == ["78"]
    assert service["TimeoutStopSec"] == [str(entry["timeout_stop_seconds"])]
    assert "SuccessExitStatus" not in service
    # The stall exit fires before the watchdog (roles.yaml validates the numbers).
    assert entry["stall_after_seconds"] < entry["watchdog_seconds"]
    assert parsed["Install"] == {"WantedBy": ["multi-user.target"]}


@pytest.mark.parametrize("name", CONTINUOUS + ONESHOT)
def test_each_service_runs_its_binary_with_its_env_file_credentials_and_go_environment(name):
    table, entry = load_roles(), role(name)
    service = unit(f"limnopulse-{name}.service")["Service"]
    argv = ["run"] if name == "alert-evaluator" else []
    assert service["ExecStart"] == [" ".join([entry["binary"], *argv])]
    assert service["EnvironmentFile"] == [entry["env_file"]]
    assert service["Environment"] == [
        f"{key}={value}" for key, value in table["go_unit_environment"].items()
    ]
    assert service["LoadCredential"] == load_credential_lines(table, entry)
    # A missing env file fails the start; it is never optional.
    assert not entry["env_file"].startswith("-")


def test_the_alert_evaluator_unit_passes_the_only_command_it_accepts():
    source = (ROOT / "cmd/alert-evaluator/main.go").read_text(encoding="utf-8")
    assert 'args[0] != "run"' in source
    assert units.EXEC_ARGS == {"alert-evaluator": ("run",)}


@pytest.mark.parametrize("name", CONTINUOUS + ONESHOT)
def test_each_service_has_the_shared_sandbox(name):
    service = unit(f"limnopulse-{name}.service")["Service"]
    for key, values in HARDENING.items():
        assert service.get(key) == values, (name, key)
    # Every role talks to AWS; none is cut off from the network.
    for key in ("PrivateNetwork", "IPAddressDeny", "User", "Group", "SupplementaryGroups"):
        assert key not in service, (name, key)


@pytest.mark.parametrize("name", CONTINUOUS + ONESHOT)
def test_af_unix_and_the_system_bus_are_only_where_a_role_needs_them(name):
    service = unit(f"limnopulse-{name}.service")["Service"]
    families = service["RestrictAddressFamilies"]
    if name in CONTINUOUS:
        # sd_notify (READY=1, WATCHDOG=1) is an AF_UNIX datagram; the bus stays hidden.
        assert families == ["AF_UNIX AF_INET AF_INET6"]
        assert service["InaccessiblePaths"] == ["-/run/dbus/system_bus_socket"]
    elif name == MONITOR:
        # systemctl show reaches systemd over the system D-Bus socket.
        assert families == ["AF_UNIX AF_INET AF_INET6"]
        for key in ("InaccessiblePaths", "TemporaryFileSystem", "BindPaths", "BindReadOnlyPaths"):
            assert key not in service, key
    else:
        assert families == ["AF_INET AF_INET6"]


# --- one-shots and their timers -------------------------------------------------------------


@pytest.mark.parametrize("name", ONESHOT)
def test_a_oneshot_service_ends_before_the_next_tick_and_has_no_install(name):
    entry, parsed = role(name), unit(f"limnopulse-{name}.service")
    service = parsed["Service"]
    assert service["Type"] == ["oneshot"]
    assert service["TimeoutStartSec"] == [str(entry["timeout_start_seconds"])]
    stop = int(service["TimeoutStopSec"][0])
    assert entry["timeout_start_seconds"] + stop == entry["schedule_seconds"]
    for key in ("Restart", "RestartPreventExitStatus", "SuccessExitStatus", "WatchdogSec"):
        assert key not in service, key
    # The timer is what an owner enables; the service has no [Install] of its own.
    assert "Install" not in parsed


@pytest.mark.parametrize("name", ONESHOT)
def test_each_oneshot_has_a_minutely_timer_with_one_second_accuracy(name):
    entry = role(name)
    parsed = unit(entry["units"]["timer"])
    assert entry["units"]["timer"] == f"limnopulse-{name}.timer"
    assert entry["schedule_seconds"] == 60
    second = 30 if name == MONITOR else 0
    assert parsed["Timer"] == {
        "OnCalendar": [f"*-*-* *:*:{second:02d}"],
        "AccuracySec": ["1s"],
        "Unit": [entry["units"]["service"]],
    }
    assert parsed["Install"] == {"WantedBy": ["timers.target"]}


def test_the_monitor_samples_the_other_oneshots_between_their_runs():
    parsed = unit("limnopulse-worker-monitor.service")
    assert parsed["Unit"]["After"] == [
        "network-online.target",
        "limnopulse-alert-evaluator.service limnopulse-notification-relay.service",
    ]
    # Ordering only: the monitor must never start or hold up another role.
    for key in ("Wants", "Requires", "BindsTo", "Requisite"):
        assert "limnopulse-" not in " ".join(parsed["Unit"].get(key, [])), key
    assert parsed["Service"]["ExecStart"] == ["/opt/limnopulse/workers/current/bin/worker-monitor"]
    # Half an interval after the other timers, which fire on the minute.
    assert unit("limnopulse-worker-monitor.timer")["Timer"]["OnCalendar"] == ["*-*-* *:*:30"]


def test_only_the_monitor_orders_itself_after_other_roles():
    for path in UNITS_DIR.glob("*.service"):
        if path.name != "limnopulse-worker-monitor.service":
            assert parse_unit(path.read_text())["Unit"]["After"] == ["network-online.target"]


# --- the env file templates --------------------------------------------------------------


def test_there_is_one_env_template_per_unit_role_at_its_env_file_name():
    names = {path.name for path in ENV_DIR.iterdir()}
    assert names == {f"{name}.env.example" for name in CONTINUOUS + ONESHOT}
    for name in CONTINUOUS + ONESHOT:
        assert Path(role(name)["env_file"]).name + ".example" in names


@pytest.mark.parametrize("name", CONTINUOUS + ONESHOT)
def test_an_env_template_holds_only_settings_its_role_reads(name):
    values = env_file(name)
    assert REQUIRED_ENV[name] <= set(values), REQUIRED_ENV[name] - set(values)
    assert not set(values) & FORBIDDEN_ENV
    assert not [key for key in values if key.endswith("_ENDPOINT_URL")]
    assert values.get("APP_ENV", "prod") == "prod"
    # Every key is one the role's own code names (AWS_REGION is read by the AWS SDK too).
    read = {"AWS_REGION"}
    for file in closure_files(f"cmd/{name}"):
        read |= set(re.findall(rf'"({ENV_KEY.pattern})"', file.code))
    assert set(values) <= read, set(values) - read
    # Placeholders only: no account id or real resource name.
    for key, value in values.items():
        assert not re.search(r"\d{12}", value), key


def test_the_secret_settings_have_credentials_instead():
    assert "influxdb-token" in role("alert-evaluator")["credentials"]
    assert "redis-password" in role("telegram-worker")["credentials"]
    assert "TELEGRAM_BOT_TOKEN" not in env_file("telegram-worker")


# --- systemd-analyze ----------------------------------------------------------------------


def _system_root(tmp_path: Path, unit_files: dict[str, str]) -> Path:
    """A root with the host's system units, our units and a fake binary per role."""
    root = tmp_path / "root"
    shutil.copytree(SYSTEM_UNITS, root / SYSTEM_UNITS.relative_to("/"), symlinks=True)
    target = root / "etc/systemd/system"
    target.mkdir(parents=True)
    for name, text in unit_files.items():
        (target / name).write_text(text, encoding="utf-8")
    for entry in units.unit_roles(load_roles()):
        binary = root / entry["binary"].lstrip("/")
        binary.parent.mkdir(parents=True, exist_ok=True)
        binary.write_text("#!/bin/sh\n")
        binary.chmod(0o755)
    return root


def _verify(tmp_path: Path, unit_files: dict[str, str]) -> tuple[int, list[str]]:
    root = _system_root(tmp_path, unit_files)
    result = subprocess.run(
        [systemd_analyze(), "verify", f"--root={root}", *sorted(unit_files)],
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )
    # The copied host units may log about each other; only lines naming our units count.
    ours = [
        line
        for line in (result.stdout + result.stderr).splitlines()
        if OUR_UNIT.search(line)
    ]
    return result.returncode, ours


def test_systemd_analyze_verify_accepts_every_unit(tmp_path):
    returncode, diagnostics = _verify(tmp_path, units.committed())
    assert returncode == 0
    assert diagnostics == []


def test_systemd_analyze_verify_would_catch_a_bad_unit(tmp_path):
    files = units.committed()
    name = "limnopulse-email-worker.service"
    files[name] = files[name].replace("WatchdogSec=", "WatchdogSecs=")
    _, diagnostics = _verify(tmp_path, files)
    assert any("WatchdogSecs" in line for line in diagnostics), diagnostics


@pytest.mark.parametrize("name", CONTINUOUS + ONESHOT)
def test_systemd_analyze_security_exposure_stays_at_or_below_the_threshold(name):
    path = UNITS_DIR / f"limnopulse-{name}.service"
    result = subprocess.run(
        [
            systemd_analyze(),
            "security",
            "--offline=yes",
            f"--threshold={round(EXPOSURE_THRESHOLD * 10)}",
            "--no-pager",
            str(path),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    match = re.search(r"Overall exposure level for \S+: (\d+\.\d+)", result.stdout)
    assert match, result.stdout + result.stderr
    assert float(match.group(1)) <= EXPOSURE_THRESHOLD, result.stdout
    assert result.returncode == 0, result.stdout + result.stderr
