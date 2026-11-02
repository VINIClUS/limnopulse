"""Fakes for the ADR-020 workers activation helper and deploy tests.

They build workers bundles in the layout `ops/workers/README.md` specifies, publish them to a
directory that a fake `oras` pulls from, and record every `oras`, `systemctl` and `sudo` call
in one JSON-lines file, so a test can assert the order of the calls across tools.
"""

from __future__ import annotations

import copy
import gzip
import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tests.support.worker_roles import ROOT, load_roles

HELPER = ROOT / "ops/vps/limnopulse-workers-activate"
HELPER_PATH = "/usr/local/sbin/limnopulse-workers-activate"
TEST_ROOT_ENV = "LIMNOPULSE_WORKERS_ACTIVATE_TEST_ROOT"
OCI_REPOSITORY = "ghcr.io/viniclus/limnopulse/workers"
GO_LIFECYCLES = ("continuous", "oneshot", "cli")

FAKE_ORAS = """\
import json, os, shutil, sys
from pathlib import Path
args = sys.argv[1:]
with open(os.environ["FAKE_CALLS"], "a") as log:
    log.write(json.dumps({"tool": "oras", "argv": args}) + "\\n")
if len(args) != 5 or args[:3] != ["pull", "--no-tty", "-o"]:
    sys.exit(64)
repository, _, tag = args[4].rpartition(":")
if repository != "ghcr.io/viniclus/limnopulse/workers":
    sys.exit(65)
source = Path(os.environ["FAKE_REGISTRY"]) / tag
if not source.is_dir():
    print(f"Error: {args[4]}: not found", file=sys.stderr)
    sys.exit(1)
for item in sorted(source.iterdir()):
    shutil.copyfile(item, Path(args[3]) / item.name)
"""

# Tracks each unit's active state in FAKE_UNIT_STATE, as systemd would:
# - `try-restart` acts only on an active unit, and `start` makes a unit active.
# - A unit in FAKE_FAIL_RESTART fails to start, like a binary exiting 78, and is left `failed`.
#   FAKE_FAIL_RESTART_RELEASE limits this to the named `current` release.
# - A unit in FAKE_CRASH_AFTER_START starts, then crashes, so it ends up `activating`
#   (auto-restart).
# - `daemon-reload` stops an active unit in FAKE_RELOAD_STOPS, as a broken new unit file
#   would.
FAKE_SYSTEMCTL = """\
import json, os, sys
from pathlib import Path
args = sys.argv[1:]
current = os.environ.get("FAKE_CURRENT", "")
target = os.readlink(current) if current and os.path.islink(current) else None
with open(os.environ["FAKE_CALLS"], "a") as log:
    log.write(json.dumps({"tool": "systemctl", "argv": args, "current": target}) + "\\n")
split = lambda name: [item for item in os.environ.get(name, "").split(",") if item]
if args[:1] and args[0] in split("FAKE_FAIL_SYSTEMCTL"):
    sys.exit(1)
state_file = Path(os.environ["FAKE_UNIT_STATE"])
states = json.loads(state_file.read_text()) if state_file.exists() else {}
failing_release = os.environ.get("FAKE_FAIL_RESTART_RELEASE")
def failing(unit):
    if unit not in split("FAKE_FAIL_RESTART"):
        return False
    return not failing_release or target == f"releases/{failing_release}"
def start(unit):
    if failing(unit):
        states[unit] = "failed"
        return 1
    states[unit] = "activating" if unit in split("FAKE_CRASH_AFTER_START") else "active"
    return 0
status = 0
if args[:1] == ["is-enabled"]:
    enabled = args[1] in split("FAKE_ENABLED")
    print("enabled" if enabled else "disabled")
    status = 0 if enabled else 1
elif args[:1] == ["is-active"]:
    print(states.get(args[1], "inactive"))
    status = 0 if states.get(args[1]) == "active" else 3
elif args[:1] == ["try-restart"]:
    for unit in args[1:]:
        if states.get(unit) == "active":
            status |= start(unit)
elif args[:1] == ["start"]:
    for unit in args[1:]:
        status |= start(unit)
elif args[:1] == ["reset-failed"]:
    for unit in args[1:]:
        if states.get(unit) == "failed":
            states[unit] = "inactive"
elif args[:1] == ["stop"]:
    for unit in args[1:]:
        states[unit] = "inactive"
elif args[:1] == ["daemon-reload"]:
    for unit in split("FAKE_RELOAD_STOPS"):
        if states.get(unit) == "active":
            states[unit] = "inactive"
state_file.write_text(json.dumps(states))
sys.exit(status)
"""

# Stands in for sudo with the sudoers rule: only the helper path, with -n, as root via sudo.
FAKE_SUDO = """\
import json, os, sys
args = sys.argv[1:]
with open(os.environ["FAKE_CALLS"], "a") as log:
    log.write(json.dumps({"tool": "sudo", "argv": args}) + "\\n")
if args[:2] != ["-n", "/usr/local/sbin/limnopulse-workers-activate"]:
    print("sudo: not allowed by the sudoers rule", file=sys.stderr)
    sys.exit(1)
env = dict(os.environ, SUDO_USER="limnopulse-deploy")
helper = os.environ["FAKE_ACTIVATE"]
os.execve(sys.executable, [sys.executable, "-I", helper, *args[2:]], env)
"""

# A scripted helper for deploy-flow tests: FAKE_HELPER_EXITS maps "status", "stage",
# "activate", "activate:<tag>" or "deactivate" to an exit code (default 0). `status` prints
# FAKE_HELPER_STATUS.
FAKE_HELPER = """\
import json, os, sys
args = sys.argv[1:]
exits = json.loads(os.environ.get("FAKE_HELPER_EXITS") or "{}")
key = f"{args[0]}:{args[1]}" if len(args) > 1 and args[0] == "activate" else args[0]
if args == ["status"]:
    sys.stdout.write(os.environ["FAKE_HELPER_STATUS"])
sys.exit(exits.get(key, exits.get(args[0], 0)))
"""


def tag(char: str) -> str:
    return "main-" + char * 40


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def env_key(role: str) -> str:
    return role.upper().replace("-", "_")


def write_tool(directory: Path, name: str, body: str) -> Path:
    path = directory / name
    path.write_text(f"#!{sys.executable}\n{body}")
    path.chmod(0o755)
    return path


def bundle_files(
    release: str,
    *,
    epochs: dict[str, int] | None = None,
    floors: dict[str, int] | None = None,
    drop_roles: tuple[str, ...] = (),
    edit: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, bytes]:
    """The files of a bundle built from roles.yaml, without SHA256SUMS.

    `edit` changes the role table before the bundle is built from it.
    """
    table = copy.deepcopy(load_roles())
    table["roles"] = [role for role in table["roles"] if role["name"] not in drop_roles]
    if edit is not None:
        edit(table)
    files: dict[str, bytes] = {}
    env = ["BUNDLE_FORMAT=1", f"BUNDLE_TAG={release}"]
    for role in table["roles"]:
        name = role["name"]
        if role["lifecycle"] not in GO_LIFECYCLES:
            continue
        rollback = role["rollback"]
        rollback["epoch"] = (epochs or {}).get(name, rollback["epoch"])
        rollback["floor"] = (floors or {}).get(name, rollback["floor"])
        env += [f"EPOCH_{env_key(name)}={rollback['epoch']}"]
        env += [f"FLOOR_{env_key(name)}={rollback['floor']}"]
        files[f"bin/{name}"] = f"#!/bin/sh\n# {name} {release}\n".encode()
        for unit in role.get("units", {}).values():
            files[f"systemd/{unit}"] = f"[Unit]\nDescription={unit} {release}\n".encode()
    files["bundle.env"] = ("\n".join(env) + "\n").encode()
    files["roles.json"] = (json.dumps(table, indent=2, sort_keys=True) + "\n").encode()
    return files


def sha256sums(files: dict[str, bytes]) -> bytes:
    lines = [f"{sha256(data)}  {path}\n" for path, data in sorted(files.items())]
    return "".join(lines).encode()


def complete(files: dict[str, bytes]) -> dict[str, bytes]:
    return {**files, "SHA256SUMS": sha256sums(files)}


def tarball(
    files: dict[str, bytes],
    *,
    extra: tuple[tuple[tarfile.TarInfo, bytes | None], ...] = (),
    dot_prefix: bool = False,
) -> bytes:
    """A deterministic tar.gz: sorted entries, mtime 0, uid and gid 0, gzip without a name."""
    prefix = "./" if dot_prefix else ""
    raw = io.BytesIO()
    with (
        gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, filename="") as compressed,
        tarfile.open(fileobj=compressed, mode="w", format=tarfile.GNU_FORMAT) as archive,
    ):
        directories = sorted({path.split("/")[0] for path in files if "/" in path})
        for directory in directories:
            info = tarfile.TarInfo(prefix + directory)
            info.type, info.mode = tarfile.DIRTYPE, 0o755
            archive.addfile(info)
        for path, data in sorted(files.items()):
            info = tarfile.TarInfo(prefix + path)
            info.size = len(data)
            info.mode = 0o755 if path.startswith("bin/") else 0o644
            archive.addfile(info, io.BytesIO(data))
        for info, data in extra:
            archive.addfile(info, io.BytesIO(data) if data is not None else None)
    return raw.getvalue()


@dataclass
class Host:
    """A temp root laid out like the VPS after the workers bootstrap."""

    base: Path
    env: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.root = self.base / "root"
        self.workers = self.root / "opt/limnopulse/workers"
        self.releases = self.workers / "releases"
        self.state = self.workers / "state"
        self.current = self.workers / "current"
        self.units = self.root / "etc/systemd/system"
        self.registry = self.base / "registry"
        self.bin = self.base / "bin"
        self.calls_file = self.base / "calls.jsonl"
        self.unit_state = self.base / "unit-state.json"
        for directory in (self.releases, self.state, self.units, self.registry, self.bin):
            directory.mkdir(parents=True, exist_ok=True)
        for directory in (self.workers, self.releases, self.state, self.units):
            directory.chmod(0o755)
        write_tool(self.bin, "oras", FAKE_ORAS)
        write_tool(self.bin, "systemctl", FAKE_SYSTEMCTL)
        environment = {
            key: value
            for key, value in os.environ.items()
            if key not in ("SUDO_USER", TEST_ROOT_ENV) and not key.startswith("FAKE_")
        }
        self.env = {
            **environment,
            "PATH": f"{self.bin}:{os.environ['PATH']}",
            TEST_ROOT_ENV: str(self.root),
            "FAKE_CALLS": str(self.calls_file),
            "FAKE_REGISTRY": str(self.registry),
            "FAKE_CURRENT": str(self.current),
            "FAKE_UNIT_STATE": str(self.unit_state),
            **self.env,
        }

    def publish(self, release: str, data: bytes | None = None, **build: Any) -> str:
        """Publish a bundle tarball for `release` and return its SHA-256."""
        if data is None:
            data = tarball(complete(bundle_files(release, **build)))
        directory = self.registry / release
        directory.mkdir(exist_ok=True)
        (directory / f"limnopulse-workers-{release}.tar.gz").write_bytes(data)
        return sha256(data)

    def run(self, *args: str, **env: str | None) -> subprocess.CompletedProcess[str]:
        environment = dict(self.env)
        for key, value in env.items():
            if value is None:
                environment.pop(key, None)
            else:
                environment[key] = value
        return subprocess.run(
            [sys.executable, "-I", str(HELPER), *args],
            check=False,
            env=environment,
            capture_output=True,
            text=True,
            timeout=60,
        )

    def deploy_release(self, release: str, **build: Any) -> str:
        digest = self.publish(release, **build)
        for args in (("stage", release, digest), ("activate", release)):
            result = self.run(*args)
            assert result.returncode == 0, result.stderr
        return digest

    def set_unit_states(self, states: dict[str, str]) -> None:
        """Set units' active states (`active`, `failed`, ...) as the fake systemctl sees them."""
        self.unit_state.write_text(json.dumps({**self.unit_states(), **states}))

    def unit_states(self) -> dict[str, str]:
        return json.loads(self.unit_state.read_text()) if self.unit_state.exists() else {}

    def calls(self, tool: str | None = None) -> list[dict[str, Any]]:
        if not self.calls_file.exists():
            return []
        entries = [json.loads(line) for line in self.calls_file.read_text().splitlines()]
        return [entry for entry in entries if tool is None or entry["tool"] == tool]

    def reset_calls(self) -> None:
        self.calls_file.unlink(missing_ok=True)

    def snapshot(self) -> dict[str, Any]:
        """Everything a refused command must leave untouched."""

        def tree(path: Path) -> dict[str, Any]:
            return {
                str(item.relative_to(path)): (
                    os.readlink(item)
                    if item.is_symlink()
                    else item.read_bytes()
                    if item.is_file()
                    else None
                )
                for item in sorted(path.rglob("*"))
                if item.name != ".lock"
            }

        return {
            "workers": tree(self.workers),
            "units": tree(self.units),
        }
