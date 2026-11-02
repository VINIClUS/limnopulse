"""Render the ADR-020 systemd units from `ops/workers/roles.yaml`.

`ops/workers/systemd/` holds the rendered units, and the workers bundle ships exactly those
files. `ops/workers/sysusers.d/` declares the static users they name; the host installs it.
`tests/unit/test_workers_units.py` checks that both equal this rendering, so edit this file
or roles.yaml, never a unit, and regenerate them from the repository root:

    .venv/bin/python -m scripts.workers.units --write

`--check` (the default) exits 1 when a committed unit differs from the rendering.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
ROLES_PATH = ROOT / "ops/workers/roles.yaml"
UNITS_DIR = ROOT / "ops/workers/systemd"
DOCUMENTATION = "https://github.com/VINIClUS/limnopulse/blob/main/ops/workers/README.md"
UNIT_LIFECYCLES = ("continuous", "oneshot")
HEADER = (
    "# Rendered from ops/workers/roles.yaml by scripts/workers/units.py; do not edit by hand.\n"
)

# The argv each role's ExecStart passes after its binary. cmd/alert-evaluator accepts only
# `run` (anything else exits 78); every other role takes flags only and runs without any.
EXEC_ARGS: dict[str, tuple[str, ...]] = {"alert-evaluator": ("run",)}
# Roles that query systemd over the system D-Bus (internal/workermonitor/doc.go). As a
# non-root user, `systemctl show` reaches PID 1 through /run/dbus/system_bus_socket.
SYSTEM_BUS_ROLES = frozenset({"worker-monitor"})
SYSTEM_BUS_SOCKET = "/run/dbus/system_bus_socket"
# PID 1 starts dbus-daemon with SYSTEMD_NSS_DYNAMIC_BYPASS=1, so the bus cannot resolve a
# dynamic user and drops its connection. A system-bus role therefore names a static user
# that ops/workers/sysusers.d declares: with DynamicUser=yes, systemd uses an existing
# user of that name instead of allocating one, and the rest of the sandbox is unchanged.
SYSUSERS_PATH = ROOT / "ops/workers/sysusers.d/limnopulse-workers.conf"
# Continuous units back off from 5 s to 60 s between restarts, so a crash loop never hits
# the start rate limit and never gives up.
RESTART = ("RestartSec=5s", "RestartSteps=5", "RestartMaxDelaySec=60s")

# The sandbox every unit shares. A role's network access, the families below and its
# credentials are the only differences between units.
HARDENING = (
    "DynamicUser=yes",
    "CapabilityBoundingSet=",
    "AmbientCapabilities=",
    "NoNewPrivileges=yes",
    "DevicePolicy=closed",
    "KeyringMode=private",
    "LockPersonality=yes",
    "MemoryDenyWriteExecute=yes",
    "PrivateDevices=yes",
    "PrivateIPC=yes",
    "PrivateTmp=yes",
    "PrivateUsers=yes",
    "ProcSubset=pid",
    "ProtectClock=yes",
    "ProtectControlGroups=yes",
    "ProtectHome=yes",
    "ProtectHostname=yes",
    "ProtectKernelLogs=yes",
    "ProtectKernelModules=yes",
    "ProtectKernelTunables=yes",
    "ProtectProc=invisible",
    "ProtectSystem=strict",
    "RemoveIPC=yes",
    "RestrictNamespaces=yes",
    "RestrictRealtime=yes",
    "RestrictSUIDSGID=yes",
    "SystemCallArchitectures=native",
    "SystemCallErrorNumber=EPERM",
    "SystemCallFilter=@system-service",
    "SystemCallFilter=~@privileged @resources",
    "UMask=0077",
)


def load_roles(path: Path = ROLES_PATH) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def unit_roles(table: dict[str, Any]) -> list[dict[str, Any]]:
    return [role for role in table["roles"] if role["lifecycle"] in UNIT_LIFECYCLES]


def address_families(role: dict[str, Any]) -> str:
    """AF_UNIX only where a unit needs it: sd_notify (continuous) or the system D-Bus."""
    families = ["AF_INET", "AF_INET6"]
    if role["lifecycle"] == "continuous" or role["name"] in SYSTEM_BUS_ROLES:
        families.insert(0, "AF_UNIX")
    return " ".join(families)


def static_user(role: dict[str, Any]) -> str | None:
    """The static user a role runs as, or None for a dynamic one."""
    return f"limnopulse-{role['name']}" if role["name"] in SYSTEM_BUS_ROLES else None


def render_sysusers(table: dict[str, Any]) -> str:
    """The sysusers.d entries for every static user the units name."""
    lines = [
        "# Rendered from ops/workers/roles.yaml by scripts/workers/units.py; do not edit by hand.",
        "# The static users the ADR-020 units name (ops/workers/README.md, \"Units\"). The host",
        "# installs this file as /etc/sysusers.d/limnopulse-workers.conf and runs",
        "# systemd-sysusers before it enables a unit that names one of them.",
    ]
    for role in unit_roles(table):
        user = static_user(role)
        if user:
            lines.append(f'u {user} - "LimnoPulse {role["name"]}" / /usr/sbin/nologin')
    return "\n".join(lines) + "\n"


def exec_start(role: dict[str, Any]) -> str:
    return " ".join((role["binary"], *EXEC_ARGS.get(role["name"], ())))


def credential_lines(table: dict[str, Any], role: dict[str, Any]) -> list[str]:
    source = table["paths"]["credential_source"]
    return [
        f"LoadCredential={credential}:{source.format(role=role['name'], credential=credential)}"
        for credential in role["credentials"]
    ]


def _unit_section(role: dict[str, Any], after: list[str]) -> list[str]:
    lines = [
        "[Unit]",
        f"Description=LimnoPulse {role['name']}: {role['work']}",
        f"Documentation={DOCUMENTATION}",
        "Wants=network-online.target",
        "After=network-online.target",
    ]
    if after:
        lines.append(f"After={' '.join(after)}")
    return lines


def _lifecycle_lines(role: dict[str, Any]) -> list[str]:
    if role["lifecycle"] == "continuous":
        return [
            "Type=notify",
            "NotifyAccess=main",
            f"WatchdogSec={role['watchdog_seconds']}",
            "Restart=on-failure",
            *RESTART,
            "RestartPreventExitStatus=78",
            f"TimeoutStopSec={role['timeout_stop_seconds']}",
        ]
    # A run that times out is stopped before the next tick: the start timeout plus the stop
    # timeout is one schedule interval.
    return [
        "Type=oneshot",
        f"TimeoutStartSec={role['timeout_start_seconds']}",
        f"TimeoutStopSec={role['schedule_seconds'] - role['timeout_start_seconds']}",
    ]


def render_service(table: dict[str, Any], role: dict[str, Any]) -> str:
    after = []
    if role["name"] in SYSTEM_BUS_ROLES:
        # Sample the other one-shots between their runs: a start job queued while theirs is
        # running waits for it (internal/workermonitor/doc.go).
        after = [
            other["units"]["service"]
            for other in unit_roles(table)
            if other["lifecycle"] == "oneshot" and other["name"] not in SYSTEM_BUS_ROLES
        ]
    service = [
        "[Service]",
        *_lifecycle_lines(role),
        f"ExecStart={exec_start(role)}",
        f"EnvironmentFile={role['env_file']}",
        *(f"Environment={key}={value}" for key, value in table["go_unit_environment"].items()),
        *credential_lines(table, role),
        *([f"User={static_user(role)}"] if static_user(role) else []),
        *HARDENING,
        f"RestrictAddressFamilies={address_families(role)}",
    ]
    if "AF_UNIX" in address_families(role) and role["name"] not in SYSTEM_BUS_ROLES:
        # AF_UNIX is for sd_notify only; the system bus stays out of reach.
        service.append(f"InaccessiblePaths=-{SYSTEM_BUS_SOCKET}")
    sections = [_unit_section(role, after), service]
    if role["lifecycle"] == "continuous":
        sections.append(["[Install]", "WantedBy=multi-user.target"])
    return _join(sections)


def timer_second(table: dict[str, Any], role: dict[str, Any]) -> int:
    """The second of each minute the timer fires: 0, or half an interval for the monitor."""
    if role["schedule_seconds"] != 60:
        raise ValueError(f"{role['name']}: the timers fire once a minute")
    return role["schedule_seconds"] // 2 if role["name"] in SYSTEM_BUS_ROLES else 0


def render_timer(table: dict[str, Any], role: dict[str, Any]) -> str:
    service = role["units"]["service"]
    # AccuracySec=1s keeps each timer at its own second: with the default of one minute,
    # systemd coalesces timers that elapse in the same window and starts them together.
    return _join(
        [
            [
                "[Unit]",
                f"Description=Run {service} every {role['schedule_seconds']} s",
                f"Documentation={DOCUMENTATION}",
            ],
            [
                "[Timer]",
                f"OnCalendar=*-*-* *:*:{timer_second(table, role):02d}",
                "AccuracySec=1s",
                f"Unit={service}",
            ],
            ["[Install]", "WantedBy=timers.target"],
        ]
    )


def _join(sections: list[list[str]]) -> str:
    return HEADER + "\n" + "\n\n".join("\n".join(lines) for lines in sections) + "\n"


def render(table: dict[str, Any]) -> dict[str, str]:
    """Every unit roles.yaml declares, by file name."""
    units: dict[str, str] = {}
    for role in unit_roles(table):
        units[role["units"]["service"]] = render_service(table, role)
        if role["lifecycle"] == "oneshot":
            units[role["units"]["timer"]] = render_timer(table, role)
    return dict(sorted(units.items()))


def committed(directory: Path = UNITS_DIR) -> dict[str, str]:
    return {
        path.name: path.read_text(encoding="utf-8")
        for path in sorted(directory.iterdir())
        if path.is_file()
    }


def drift(table: dict[str, Any], directory: Path = UNITS_DIR) -> list[str]:
    """The unit files that differ from the rendering, are missing or are not declared."""
    expected, present = render(table), committed(directory) if directory.is_dir() else {}
    return sorted(
        name
        for name in set(expected) | set(present)
        if expected.get(name) != present.get(name)
    )


def sysusers_drift(table: dict[str, Any], path: Path = SYSUSERS_PATH) -> bool:
    return not path.is_file() or path.read_text(encoding="utf-8") != render_sysusers(table)


def write(
    table: dict[str, Any], directory: Path = UNITS_DIR, sysusers: Path = SYSUSERS_PATH
) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    expected = render(table)
    for path in directory.iterdir():
        if path.name not in expected:
            path.unlink()
    for name, text in expected.items():
        (directory / name).write_text(text, encoding="utf-8")
    sysusers.parent.mkdir(parents=True, exist_ok=True)
    sysusers.write_text(render_sysusers(table), encoding="utf-8")


def main(
    argv: list[str] | None = None,
    directory: Path = UNITS_DIR,
    sysusers: Path = SYSUSERS_PATH,
) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--write", action="store_true", help="rewrite ops/workers/systemd and sysusers.d"
    )
    args = parser.parse_args(argv)
    table = load_roles()
    if args.write:
        write(table, directory, sysusers)
        return 0
    stale = drift(table, directory)
    if sysusers_drift(table, sysusers):
        stale.append(sysusers.name)
    if stale:
        print(
            f"ops/workers/systemd or sysusers.d differs from roles.yaml: {stale}; "
            "run .venv/bin/python -m scripts.workers.units --write",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
