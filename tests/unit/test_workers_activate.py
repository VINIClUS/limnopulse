"""ADR-020 S4: the real workers activation helper against a temp root.

Only `oras` and `systemctl` are fakes on PATH; see tests/support/workers_activate.py.
"""

from __future__ import annotations

import fcntl
import importlib.machinery
import importlib.util
import io
import os
import sys
import tarfile
import threading

import pytest

from tests.support.worker_roles import load_roles, roles_by_name
from tests.support.workers_activate import (
    HELPER,
    HELPER_PATH,
    OCI_REPOSITORY,
    TEST_ROOT_ENV,
    Host,
    bundle_files,
    complete,
    sha256,
    sha256sums,
    tag,
    tarball,
)

A, B, C, D = tag("a"), tag("b"), tag("c"), tag("d")
FEEDBACK = "ses-feedback-worker"
CONTINUOUS = (
    "limnopulse-email-worker.service",
    "limnopulse-ses-feedback-worker.service",
    "limnopulse-telegram-worker.service",
)


@pytest.fixture
def host(tmp_path):
    return Host(tmp_path)


def release_link(name: str) -> str:
    return f"releases/{name}"


def declared_units(drop_roles=()) -> list[str]:
    return sorted(
        unit
        for role in load_roles()["roles"]
        if role["name"] not in drop_roles
        for unit in role.get("units", {}).values()
    )


def info(name, kind, **fields):
    member = tarfile.TarInfo(name)
    member.type = kind
    for key, value in fields.items():
        setattr(member, key, value)
    return member


# --- the helper mirrors roles.yaml ------------------------------------------------------


def load_helper():
    loader = importlib.machinery.SourceFileLoader("limnopulse_workers_activate", str(HELPER))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    sys.modules[loader.name] = module  # dataclasses resolve annotations through sys.modules
    try:
        loader.exec_module(module)
    finally:
        del sys.modules[loader.name]
    return module


def test_helper_constants_match_the_role_table():
    helper, table = load_helper(), load_roles()
    paths, bundle, activation = table["paths"], table["bundle"], table["activation"]
    assert (helper.WORKERS_ROOT, helper.RELEASES, helper.CURRENT, helper.STATE) == (
        paths["workers_root"],
        paths["releases"],
        paths["current"],
        paths["state"],
    )
    assert helper.UNITS_DIR == paths["units"]
    assert helper.OCI_REPOSITORY == bundle["oci_repository"] == OCI_REPOSITORY
    assert helper.BUNDLE_FILE == bundle["file"]
    assert (helper.CHECKSUM_MANIFEST, helper.METADATA, helper.ROLE_TABLE) == (
        bundle["checksum_manifest"],
        bundle["metadata"],
        bundle["role_table"],
    )
    assert (helper.BINARIES, helper.UNITS) == (bundle["binaries"], bundle["units"])
    assert helper.BUNDLE_FORMAT == bundle["format"]
    assert helper.RELEASES_KEPT == bundle["releases_kept"]
    assert list(helper.COMMANDS) == activation["commands"]
    codes = activation["exit_codes"]
    assert (
        helper.EXIT_OK,
        helper.EXIT_FAILURE,
        helper.EXIT_FLOOR_REFUSED,
        helper.EXIT_USAGE,
    ) == (codes["success"], codes["failure"], codes["floor_refused"], codes["usage"])
    assert activation["helper"] == HELPER_PATH
    assert helper.TEST_ROOT_ENV == TEST_ROOT_ENV


def test_helper_is_executable_and_runs_isolated_python():
    assert os.access(HELPER, os.X_OK)
    assert HELPER.read_text().splitlines()[0] == "#!/usr/bin/python3 -I"


# --- arguments --------------------------------------------------------------------------

SHA = "0" * 64


@pytest.mark.parametrize(
    "args",
    [
        (),
        ("bogus",),
        ("Stage", A, SHA),
        ("stage",),
        ("stage", A),
        ("stage", A, SHA, "extra"),
        ("stage", A + "\n", SHA),
        ("stage", A, SHA + "\n"),
        ("stage", "main-" + "a" * 39, SHA),
        ("stage", "main-" + "A" * 40, SHA),
        ("stage", "main-abcdef0", SHA),
        ("stage", A, "0" * 63),
        ("stage", A, "F" * 64),
        ("stage", " " + A, SHA),
        ("stage", f"{A} {SHA}"),
        ("stage", "main-" + "١" * 40, SHA),
        ("stage", "--override-floor", A, SHA),
        ("stage", A, SHA, "--override-floor", "--override-floor"),
        ("activate",),
        ("activate", A, "x"),
        ("activate", "../" + A),
        ("activate", A, "--force"),
        ("deactivate", A),
        ("status", "--verbose"),
        ("deactivate", "--override-floor"),
    ],
)
def test_invalid_arguments_are_usage_errors_before_any_io(host, args):
    before = host.snapshot()
    result = host.run(*args)
    assert result.returncode == 78, result.stderr
    assert host.calls() == []
    assert host.snapshot() == before
    assert not (host.state / ".lock").exists()


@pytest.mark.parametrize("sudo_user", ["limnopulse-deploy", ""])
@pytest.mark.parametrize(
    "args", [("stage", A, SHA, "--override-floor"), ("activate", A, "--override-floor")]
)
def test_the_floor_override_is_refused_whenever_sudo_user_is_set(host, args, sudo_user):
    result = host.run(*args, SUDO_USER=sudo_user)
    assert result.returncode == 78
    assert "refused under sudo" in result.stderr
    assert host.calls() == []


def test_an_unprivileged_run_without_the_test_root_is_refused(host):
    result = host.run("status", **{TEST_ROOT_ENV: None})
    assert result.returncode == 1
    assert "must run as root" in result.stderr


@pytest.mark.parametrize("broken", ["group-writable", "symlink", "missing"])
def test_the_helper_refuses_a_directory_root_does_not_own_alone(host, tmp_path, broken):
    if broken == "group-writable":
        host.releases.chmod(0o775)
    elif broken == "symlink":
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        host.releases.rmdir()
        host.releases.symlink_to(elsewhere)
    else:
        host.state.rmdir()
    digest = host.publish(A)
    result = host.run("stage", A, digest)
    assert result.returncode == 1
    assert host.calls() == []


def test_a_concurrent_run_is_refused(host):
    digest = host.publish(A)
    fd = os.open(host.state / ".lock", os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        result = host.run("stage", A, digest)
    finally:
        os.close(fd)
    assert result.returncode == 1
    assert "another activation is running" in result.stderr
    assert host.calls() == []


# --- stage --------------------------------------------------------------------------------


def test_stage_verifies_and_extracts_without_touching_units(host):
    digest = host.publish(A)
    result = host.run("stage", A, digest)
    assert result.returncode == 0, result.stderr
    assert host.calls() == [
        {
            "tool": "oras",
            "argv": ["pull", "--no-tty", "-o", host.calls()[0]["argv"][3], f"{OCI_REPOSITORY}:{A}"],
        }
    ]
    pulled_into = host.calls()[0]["argv"][3]
    assert pulled_into.startswith(str(host.workers / ".stage-"))
    assert not os.path.exists(pulled_into)
    release = host.releases / A
    files = complete(bundle_files(A))
    assert {
        str(path.relative_to(release)): path.read_bytes()
        for path in release.rglob("*")
        if path.is_file()
    } == files
    for path in release.rglob("*"):
        mode = path.stat().st_mode & 0o777
        expected = 0o755 if path.is_dir() or path.parent.name == "bin" else 0o644
        assert mode == expected, path
    record = (host.state / f"{A}.json").read_text()
    assert digest in record and sha256(files["SHA256SUMS"]) in record
    assert not (host.state / "floors.json").exists()
    assert not os.path.lexists(host.current)
    assert list(host.units.iterdir()) == []


def test_stage_accepts_dot_prefixed_members(host):
    digest = host.publish(A, tarball(complete(bundle_files(A)), dot_prefix=True))
    assert host.run("stage", A, digest).returncode == 0
    assert (host.releases / A / "bin/email-worker").is_file()


def _members_with(**changes):
    files = bundle_files(A)
    files.update({path: data for path, data in changes.items() if data is not None})
    for path in [path for path, data in changes.items() if data is None]:
        files.pop(path)
    return files


def _tampered_sums():
    files = bundle_files(A)
    sums = sha256sums(files).replace(sha256(files["bin/email-worker"]).encode(), b"f" * 64)
    return tarball({**files, "SHA256SUMS": sums})


def _unlisted_file():
    files = bundle_files(A)
    return tarball({**files, "SHA256SUMS": sha256sums(files), "bin/extra": b"x"})


def _listed_missing_file():
    files = bundle_files(A)
    sums = sha256sums({**files, "bin/ghost": b"x"})
    return tarball({**files, "SHA256SUMS": sums})


def _with_member(member, data=None):
    return tarball(complete(bundle_files(A)), extra=((member, data),))


BAD_BUNDLES = {
    "SHA256SUMS lists a wrong digest": _tampered_sums,
    "a file SHA256SUMS does not list": _unlisted_file,
    "SHA256SUMS lists a missing file": _listed_missing_file,
    "no SHA256SUMS": lambda: tarball(bundle_files(A)),
    "a symlink member": lambda: _with_member(
        info("bin/link", tarfile.SYMTYPE, linkname="/etc/shadow")
    ),
    "a hardlink member": lambda: _with_member(
        info("bin/hard", tarfile.LNKTYPE, linkname="bin/email-worker")
    ),
    "a device member": lambda: _with_member(info("bin/dev", tarfile.CHRTYPE)),
    "a fifo member": lambda: _with_member(info("bin/fifo", tarfile.FIFOTYPE)),
    "an absolute member": lambda: _with_member(
        info("/etc/cron.d/x", tarfile.REGTYPE, size=1), b"x"
    ),
    "a parent-directory member": lambda: _with_member(
        info("bin/../../escape", tarfile.REGTYPE, size=1), b"x"
    ),
    "a nested directory": lambda: _with_member(info("bin/nested", tarfile.DIRTYPE)),
    "an unexpected top-level file": lambda: tarball(complete(_members_with(**{"extra.sh": b"x"}))),
    "a duplicated member": lambda: _with_member(info("bundle.env", tarfile.REGTYPE, size=1), b"x"),
    "a wrong BUNDLE_TAG": lambda: tarball(
        complete(_members_with(**{"bundle.env": bundle_files(B)["bundle.env"]}))
    ),
    "a wrong BUNDLE_FORMAT": lambda: tarball(
        complete(
            _members_with(
                **{
                    "bundle.env": bundle_files(A)["bundle.env"].replace(
                        b"BUNDLE_FORMAT=1", b"BUNDLE_FORMAT=2"
                    )
                }
            )
        )
    ),
    "an epoch that disagrees with roles.json": lambda: tarball(
        complete(
            _members_with(
                **{
                    "bundle.env": bundle_files(A, epochs={FEEDBACK: 2})["bundle.env"],
                }
            )
        )
    ),
    "a sourced shell line in bundle.env": lambda: tarball(
        complete(_members_with(**{"bundle.env": bundle_files(A)["bundle.env"] + b"X=$(id)\n"}))
    ),
    "an undeclared unit": lambda: tarball(
        complete(_members_with(**{"systemd/limnopulse-rogue.service": b"[Service]\n"}))
    ),
    "a missing unit": lambda: tarball(
        complete(_members_with(**{"systemd/limnopulse-email-worker.service": None}))
    ),
    "a missing binary": lambda: tarball(complete(_members_with(**{"bin/telegram-worker": None}))),
    "not a gzip tarball": lambda: b"not a tarball",
}

REFUSAL_REASONS = {
    "SHA256SUMS lists a wrong digest": "SHA256SUMS check failed",
    "a file SHA256SUMS does not list": "unlisted ['bin/extra']",
    "SHA256SUMS lists a missing file": "missing ['bin/ghost']",
    "no SHA256SUMS": "has no SHA256SUMS",
    "a symlink member": "not a regular file",
    "a hardlink member": "not a regular file",
    "a device member": "not a regular file",
    "a fifo member": "not a regular file",
    "an absolute member": "unsafe name",
    "a parent-directory member": "unsafe name",
    "a nested directory": "unexpected directory",
    "an unexpected top-level file": "outside the bundle layout",
    "a duplicated member": "appears twice",
    "a wrong BUNDLE_TAG": "BUNDLE_TAG does not match",
    "a wrong BUNDLE_FORMAT": "BUNDLE_FORMAT must be 1",
    "an epoch that disagrees with roles.json": "must match roles.json",
    "a sourced shell line in bundle.env": "malformed or repeated line",
    "an undeclared unit": "extra ['systemd/limnopulse-rogue.service']",
    "a missing unit": "missing ['systemd/limnopulse-email-worker.service']",
    "a missing binary": "missing ['bin/telegram-worker']",
    "not a gzip tarball": "not a gzip tarball",
}


def test_every_bad_bundle_has_a_refusal_reason():
    assert set(REFUSAL_REASONS) == set(BAD_BUNDLES)


@pytest.mark.parametrize("case", sorted(BAD_BUNDLES))
def test_stage_refuses_a_bad_bundle_before_anything_changes(host, case):
    digest = host.publish(A, BAD_BUNDLES[case]())
    before = host.snapshot()
    result = host.run("stage", A, digest)
    assert result.returncode == 1, result.stderr
    assert REFUSAL_REASONS[case] in result.stderr
    assert host.snapshot() == before
    assert [call["tool"] for call in host.calls()] == ["oras"]


def test_stage_refuses_a_tampered_tarball(host):
    digest = host.publish(A)
    tarball_path = host.registry / A / f"limnopulse-workers-{A}.tar.gz"
    tarball_path.write_bytes(tarball(complete(bundle_files(A, floors={FEEDBACK: 0}))) + b"\0")
    before = host.snapshot()
    result = host.run("stage", A, digest)
    assert result.returncode == 1
    assert "does not match the requested SHA-256" in result.stderr
    assert host.snapshot() == before


def test_stage_refuses_a_missing_bundle(host):
    before = host.snapshot()
    result = host.run("stage", A, SHA)
    assert result.returncode == 1
    assert "no bundle" in result.stderr
    assert host.snapshot() == before


def test_stage_refuses_a_pull_with_extra_files(host):
    digest = host.publish(A)
    (host.registry / A / "README").write_text("extra")
    before = host.snapshot()
    assert host.run("stage", A, digest).returncode == 1
    assert host.snapshot() == before


def test_restaging_a_verified_release_is_idempotent(host):
    digest = host.publish(A)
    assert host.run("stage", A, digest).returncode == 0
    host.reset_calls()
    before = host.snapshot()
    assert host.run("stage", A, digest).returncode == 0
    assert host.calls() == []
    assert host.snapshot() == before


def test_restaging_the_current_release_with_another_sha_is_refused(host):
    host.deploy_release(A)
    other = host.publish(A, tarball(complete(bundle_files(A)), dot_prefix=True))
    before = host.snapshot()
    result = host.run("stage", A, other)
    assert result.returncode == 1
    assert host.snapshot() == before


def test_restaging_a_release_that_is_not_current_replaces_it(host):
    first = host.publish(A)
    assert host.run("stage", A, first).returncode == 0
    second = host.publish(A, tarball(complete(bundle_files(A)), dot_prefix=True))
    assert host.run("stage", A, second).returncode == 0
    assert second in (host.state / f"{A}.json").read_text()


def test_a_corrupted_staged_release_is_staged_again(host):
    digest = host.publish(A)
    assert host.run("stage", A, digest).returncode == 0
    (host.releases / A / "bin/email-worker").write_bytes(b"corrupt")
    assert host.run("stage", A, digest).returncode == 0
    assert (host.releases / A / "bin/email-worker").read_bytes() == bundle_files(A)[
        "bin/email-worker"
    ]


# --- activate -----------------------------------------------------------------------------


def test_activate_installs_units_reloads_swaps_and_restarts_only_enabled_continuous(host):
    host.env["FAKE_ENABLED"] = "limnopulse-email-worker.service,limnopulse-alert-evaluator.timer"
    digest = host.publish(A)
    assert host.run("stage", A, digest).returncode == 0
    host.reset_calls()
    result = host.run("activate", A)
    assert result.returncode == 0, result.stderr
    files = bundle_files(A)
    assert sorted(path.name for path in host.units.iterdir()) == declared_units()
    for unit in declared_units():
        assert (host.units / unit).read_bytes() == files[f"systemd/{unit}"]
        assert (host.units / unit).stat().st_mode & 0o777 == 0o644
    assert os.readlink(host.current) == release_link(A)
    calls = [call["argv"] for call in host.calls("systemctl")]
    assert calls == [
        ["daemon-reload"],
        ["is-enabled", CONTINUOUS[0]],
        ["try-restart", CONTINUOUS[0]],
        ["is-enabled", CONTINUOUS[1]],
        ["is-enabled", CONTINUOUS[2]],
    ]
    assert not any(call[0] in ("enable", "start", "restart") for call in calls)
    assert (host.state / "units.json").exists()


def test_activate_swaps_current_atomically_after_daemon_reload(host):
    host.env["FAKE_ENABLED"] = CONTINUOUS[0]
    host.deploy_release(A)
    digest = host.publish(B)
    assert host.run("stage", B, digest).returncode == 0
    host.reset_calls()
    seen, missing, done = set(), [], threading.Event()

    def watch():
        while not done.is_set():
            try:
                seen.add(os.readlink(host.current))
            except FileNotFoundError:
                missing.append(True)

    watcher = threading.Thread(target=watch)
    watcher.start()
    try:
        result = host.run("activate", B)
    finally:
        done.set()
        watcher.join()
    assert result.returncode == 0, result.stderr
    assert not missing, "current vanished during the swap"
    assert seen <= {release_link(A), release_link(B)}
    assert os.readlink(host.current) == release_link(B)
    calls = host.calls("systemctl")
    assert calls[0] == {"tool": "systemctl", "argv": ["daemon-reload"], "current": release_link(A)}
    assert {
        "tool": "systemctl",
        "argv": ["try-restart", CONTINUOUS[0]],
        "current": release_link(B),
    } in calls
    assert not [path for path in host.workers.iterdir() if path.name.startswith(".current-")]


def test_activate_refuses_a_release_that_is_not_staged(host):
    before = host.snapshot()
    result = host.run("activate", A)
    assert result.returncode == 1
    assert "not staged" in result.stderr
    assert host.snapshot() == before
    assert host.calls() == []


def _tamper_binary(host):
    (host.releases / A / "bin/email-worker").write_bytes(b"#!/bin/sh\nrm -rf /\n")


def _tamper_binary_and_sums(host):
    release = host.releases / A
    (release / "bin/email-worker").write_bytes(b"evil")
    files = {
        str(path.relative_to(release)): path.read_bytes()
        for path in release.rglob("*")
        if path.is_file() and path.name != "SHA256SUMS"
    }
    (release / "SHA256SUMS").write_bytes(sha256sums(files))


def _extra_file(host):
    (host.releases / A / "bin/extra").write_bytes(b"x")


def _group_writable(host):
    (host.releases / A / "bin/email-worker").chmod(0o775)


def _symlinked_binary(host):
    path = host.releases / A / "bin/email-worker"
    path.unlink()
    path.symlink_to("/bin/sh")


def _forged_record(host):
    record = host.state / f"{A}.json"
    record.write_text(record.read_text().replace('"manifest_sha256": "', '"manifest_sha256": "0'))


@pytest.mark.parametrize(
    "tamper",
    [
        _tamper_binary,
        _tamper_binary_and_sums,
        _extra_file,
        _group_writable,
        _symlinked_binary,
        _forged_record,
    ],
)
def test_activate_reverifies_the_staged_release(host, tamper):
    digest = host.publish(A)
    assert host.run("stage", A, digest).returncode == 0
    tamper(host)
    host.reset_calls()
    before = host.snapshot()
    result = host.run("activate", A)
    assert result.returncode == 1, result.stderr
    assert host.snapshot() == before
    assert host.calls() == []


def test_activate_refuses_a_current_that_is_a_directory(host):
    digest = host.publish(A)
    assert host.run("stage", A, digest).returncode == 0
    host.current.mkdir()
    result = host.run("activate", A)
    assert result.returncode == 1
    assert host.current.is_dir() and not host.current.is_symlink()


def test_activate_removes_the_units_a_release_dropped(host):
    host.deploy_release(A)
    host.reset_calls()
    host.deploy_release(B, drop_roles=("telegram-worker",))
    assert sorted(path.name for path in host.units.iterdir()) == declared_units(
        drop_roles=("telegram-worker",)
    )
    stops = [call["argv"] for call in host.calls("systemctl") if call["argv"][0] == "stop"]
    assert stops == [["stop", "limnopulse-telegram-worker.service"]]


def test_a_failed_try_restart_fails_the_activation_and_records_no_floor(host):
    host.env["FAKE_ENABLED"] = CONTINUOUS[1]
    host.env["FAKE_FAIL_RESTART"] = CONTINUOUS[1]
    digest = host.publish(A, epochs={FEEDBACK: 2}, floors={FEEDBACK: 2})
    assert host.run("stage", A, digest).returncode == 0
    assert host.run("activate", A).returncode == 1
    assert not (host.state / "floors.json").exists()
    del host.env["FAKE_FAIL_RESTART"]
    host.deploy_release(B)  # epoch 1 is still allowed: the failed activation set no floor


# --- rollback floors ----------------------------------------------------------------------


def test_stage_checks_floors_but_records_none(host):
    digest = host.publish(A, epochs={FEEDBACK: 2}, floors={FEEDBACK: 2})
    assert host.run("stage", A, digest).returncode == 0
    assert not (host.state / "floors.json").exists()
    host.deploy_release(B)
    assert os.readlink(host.current) == release_link(B)


def test_activate_records_floors_and_refuses_an_older_epoch(host):
    old = host.publish(A)
    assert host.run("stage", A, old).returncode == 0
    host.deploy_release(B, epochs={FEEDBACK: 2}, floors={FEEDBACK: 2})
    floors = (host.state / "floors.json").read_text()
    assert f'"{FEEDBACK}": 2' in floors
    host.reset_calls()
    before = host.snapshot()
    activated = host.run("activate", A)
    assert activated.returncode == 3, activated.stderr
    assert "rollback floor refused" in activated.stderr
    assert host.snapshot() == before
    assert host.calls() == []
    host.publish(C)
    staged = host.run("stage", C, host.publish(C))
    assert staged.returncode == 3
    assert not (host.releases / C).exists()


def test_a_role_missing_from_a_release_counts_as_epoch_zero(host):
    host.deploy_release(A, epochs={"telegram-worker": 2}, floors={"telegram-worker": 1})
    digest = host.publish(B, drop_roles=("telegram-worker",))
    assert host.run("stage", B, digest).returncode == 3


def test_floors_only_rise(host):
    host.deploy_release(A, epochs={FEEDBACK: 3}, floors={FEEDBACK: 2})
    host.deploy_release(B, epochs={FEEDBACK: 3}, floors={FEEDBACK: 0})
    assert f'"{FEEDBACK}": 2' in (host.state / "floors.json").read_text()


def test_root_itself_may_override_the_floor(host):
    host.deploy_release(A)
    host.deploy_release(B, epochs={FEEDBACK: 2}, floors={FEEDBACK: 2})
    refused = host.run("activate", A, "--override-floor", SUDO_USER="limnopulse-deploy")
    assert refused.returncode == 78
    assert os.readlink(host.current) == release_link(B)
    result = host.run("activate", A, "--override-floor")
    assert result.returncode == 0, result.stderr
    assert "overrides the rollback floor" in result.stderr
    assert os.readlink(host.current) == release_link(A)
    assert f'"{FEEDBACK}": 2' in (host.state / "floors.json").read_text()


# --- releases kept ------------------------------------------------------------------------


def test_two_releases_are_kept_and_never_the_current_one(host):
    host.deploy_release(A)
    host.deploy_release(B)
    assert sorted(path.name for path in host.releases.iterdir()) == [A, B]
    assert host.run("stage", C, host.publish(C)).returncode == 0
    # Staging keeps the current release (the rollback target) and the candidate.
    assert sorted(path.name for path in host.releases.iterdir()) == [B, C]
    assert not (host.state / f"{A}.json").exists()
    assert host.run("activate", C).returncode == 0
    assert sorted(path.name for path in host.releases.iterdir()) == [B, C]
    assert host.run("stage", D, host.publish(D)).returncode == 0
    assert sorted(path.name for path in host.releases.iterdir()) == [C, D]
    assert sorted(path.name for path in host.state.glob("main-*.json")) == [
        f"{C}.json",
        f"{D}.json",
    ]


def test_without_a_current_release_the_newest_two_are_kept(host):
    for name in (A, B, C):
        assert host.run("stage", name, host.publish(name)).returncode == 0
    assert sorted(path.name for path in host.releases.iterdir()) == [B, C]


# --- deactivate and status ----------------------------------------------------------------


def test_deactivate_stops_and_removes_the_installed_units(host):
    host.deploy_release(A)
    floors_before = (host.state / "floors.json").read_text()
    host.reset_calls()
    result = host.run("deactivate")
    assert result.returncode == 0, result.stderr
    assert list(host.units.iterdir()) == []
    assert not os.path.lexists(host.current)
    assert (host.releases / A).is_dir()
    assert (host.state / "floors.json").read_text() == floors_before
    calls = [call["argv"] for call in host.calls("systemctl")]
    assert calls[-1] == ["daemon-reload"]
    stop = calls[0]
    assert stop[0] == "stop" and sorted(stop[1:]) == declared_units()
    timers = [index for index, unit in enumerate(stop[1:]) if unit.endswith(".timer")]
    assert timers == list(range(len(timers))), "timers stop before their services"
    host.reset_calls()
    assert host.run("deactivate").returncode == 0
    assert host.calls() == []


def test_deactivate_on_a_host_that_never_had_a_bundle_changes_nothing(host):
    before = host.snapshot()
    result = host.run("deactivate")
    assert result.returncode == 0, result.stderr
    assert host.calls() == []
    assert host.snapshot() == before


def test_status_prints_current_releases_and_floors(host):
    digest = host.deploy_release(A, epochs={FEEDBACK: 2}, floors={FEEDBACK: 2})
    result = host.run("status")
    assert result.returncode == 0, result.stderr
    lines = result.stdout.splitlines()
    assert lines[0] == f"current {A}"
    assert f"release {A} sha256={digest} current" in lines
    assert f"floor {FEEDBACK} 2" in lines
    assert all(f"unit {unit}" in lines for unit in declared_units())


def test_only_continuous_roles_are_try_restarted(host):
    roles = roles_by_name(load_roles())
    continuous = sorted(
        role["units"]["service"] for role in roles.values() if role["lifecycle"] == "continuous"
    )
    assert continuous == sorted(CONTINUOUS)


def test_the_fakes_build_bundles_in_the_documented_layout():
    with tarfile.open(fileobj=io.BytesIO(tarball(complete(bundle_files(A))))) as archive:
        names = set(archive.getnames())
    assert {"bundle.env", "roles.json", "SHA256SUMS", "bin", "systemd"} <= names
    assert {name.split("/")[0] for name in names} == {
        "bundle.env",
        "roles.json",
        "SHA256SUMS",
        "bin",
        "systemd",
    }
