"""ADR-020 S3: the workers bundle `scripts/workers/build_bundle.py` packs, checked against
`ops/workers/roles.yaml`, the README layout and the S4 activation helper.

Binaries are fakes here: Go is compiled only in CI, which builds the real executables twice
and compares the bundles (`.github/workflows/build.yml`, `workers-bundle`).
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import struct
import sys
import tarfile
from pathlib import Path

import pytest

from scripts.workers import build_bundle, units
from tests.support.worker_roles import load_roles, roles_by_name
from tests.support.workers_activate import Host, sha256

TAG = "main-" + "a" * 40
GO_ROLES = (
    "alert-evaluator",
    "notification-relay",
    "email-worker",
    "ses-feedback-worker",
    "telegram-worker",
    "worker-monitor",
    "limnopulse-admin",
)
DECLARED_UNITS = sorted(
    unit for role in load_roles()["roles"] for unit in role.get("units", {}).values()
)


def fake_binaries(directory: Path, names=GO_ROLES) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    for name in names:
        (directory / name).write_bytes(f"#!/bin/sh\n# fake {name}\n".encode())
    return directory


@pytest.fixture
def bundle(tmp_path) -> Path:
    return build_bundle.build_bundle(TAG, fake_binaries(tmp_path / "bin"), tmp_path / "out")


def members(path: Path) -> list[tarfile.TarInfo]:
    with tarfile.open(path, mode="r:gz") as archive:
        return archive.getmembers()


def read(path: Path) -> dict[str, bytes]:
    with tarfile.open(path, mode="r:gz") as archive:
        return {
            member.name: archive.extractfile(member).read()
            for member in archive.getmembers()
            if member.isreg()
        }


def elf(*, machine=62, elf_type=2, klass=2, program_types=(1,)) -> bytes:
    """A minimal ELF64 header with one program header per type."""
    header = bytearray(64)
    header[:4] = b"\x7fELF"
    header[4], header[5], header[6] = klass, 1, 1
    struct.pack_into("<HHI", header, 16, elf_type, machine, 1)
    struct.pack_into("<Q", header, 32, 64)
    struct.pack_into("<HH", header, 54, 56, len(program_types))
    programs = b"".join(struct.pack("<I", kind) + bytes(52) for kind in program_types)
    return bytes(header) + programs


# --- the layout ops/workers/README.md specifies ----------------------------------------


def test_the_bundle_is_named_after_its_tag_and_holds_exactly_the_layout(bundle):
    assert bundle.name == f"limnopulse-workers-{TAG}.tar.gz"
    names = [member.name for member in members(bundle)]
    assert names == sorted(names)
    assert names == [
        "SHA256SUMS",
        "bin",
        *(f"bin/{name}" for name in sorted(GO_ROLES)),
        "bundle.env",
        "roles.json",
        "systemd",
        *(f"systemd/{unit}" for unit in DECLARED_UNITS),
    ]
    assert len(DECLARED_UNITS) == 9


def test_every_entry_is_normalized(bundle):
    for member in members(bundle):
        assert (member.mtime, member.uid, member.gid) == (0, 0, 0), member.name
        assert (member.uname, member.gname) == ("", ""), member.name
        if member.isdir():
            assert member.mode == 0o755, member.name
        else:
            assert member.isreg(), member.name
            assert member.mode == (0o755 if member.name.startswith("bin/") else 0o644)
    raw = bundle.read_bytes()
    # gzip -n: no file name, no extra fields, mtime 0.
    assert raw[:4] == b"\x1f\x8b\x08\x00" and raw[4:8] == b"\0\0\0\0"
    # Every header is POSIX ustar: no GNU or PAX extensions.
    tar = gzip.decompress(raw)
    offset = 0
    for member in members(bundle):
        assert tar[offset + 257 : offset + 265] == b"ustar\x0000", member.name
        offset += 512 + -(-member.size // 512) * 512


def test_bundle_env_pins_each_go_role_epoch_and_floor_from_roles_yaml(bundle):
    lines = read(bundle)["bundle.env"].decode("ascii").splitlines()
    expected = ["BUNDLE_FORMAT=1", f"BUNDLE_TAG={TAG}"]
    roles = roles_by_name(load_roles())
    for name in GO_ROLES:
        key = name.upper().replace("-", "_")
        rollback = roles[name]["rollback"]
        expected += [f"EPOCH_{key}={rollback['epoch']}", f"FLOOR_{key}={rollback['floor']}"]
    assert lines == expected
    # No EPOCH_/FLOOR_ key for the Compose role.
    assert not [line for line in lines if "TELEMETRY_INGEST" in line]


def test_roles_json_is_roles_yaml_with_the_same_keys_and_values(bundle):
    assert json.loads(read(bundle)["roles.json"]) == load_roles()


def test_sha256sums_lists_every_other_file_once(bundle):
    files = read(bundle)
    listed = {}
    for line in files["SHA256SUMS"].decode("ascii").splitlines():
        digest, _, path = line.partition("  ")
        assert path not in listed
        listed[path] = digest
    assert set(listed) == set(files) - {"SHA256SUMS"}
    for path, digest in listed.items():
        assert hashlib.sha256(files[path]).hexdigest() == digest, path


def test_the_bundle_ships_the_committed_units(bundle):
    files = read(bundle)
    assert {
        name.removeprefix("systemd/"): data
        for name, data in files.items()
        if name.startswith("systemd/")
    } == {path.name: path.read_bytes() for path in units.UNITS_DIR.iterdir()}


# --- reproducibility -----------------------------------------------------------------------


def test_two_builds_produce_the_same_bytes_whatever_the_input_metadata(tmp_path):
    first = build_bundle.build_bundle(TAG, fake_binaries(tmp_path / "a"), tmp_path / "out-a")
    second_bin = fake_binaries(tmp_path / "b")
    for path in second_bin.iterdir():
        os.utime(path, (1_000_000_000, 1_000_000_000))
        path.chmod(0o600)
    second = build_bundle.build_bundle(TAG, second_bin, tmp_path / "out-b")
    assert first.read_bytes() == second.read_bytes()


def test_verify_requires_the_canonical_packing(bundle, tmp_path):
    files = read(bundle)
    roles = load_roles()
    # The same files with a non-zero mtime: every file matches, the packing does not.
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for info, data in build_bundle.entries(roles, files):
            info.mtime = 1
            archive.addfile(info, io.BytesIO(data) if info.isreg() else None)
    repacked = tmp_path / bundle.name
    repacked.write_bytes(gzip.compress(raw.getvalue(), mtime=0))
    with pytest.raises(build_bundle.BundleError, match="canonical"):
        build_bundle.verify_bundle(repacked, TAG)


@pytest.mark.parametrize(
    "path",
    ["bundle.env", "roles.json", "SHA256SUMS", "systemd/limnopulse-email-worker.service"],
)
def test_verify_rejects_a_tampered_file(bundle, tmp_path, path):
    files = read(bundle)
    files[path] = files[path] + b"\n"
    tampered = tmp_path / bundle.name
    tampered.write_bytes(build_bundle.pack(load_roles(), files))
    with pytest.raises(build_bundle.BundleError, match=path):
        build_bundle.verify_bundle(tampered, TAG)


def test_verify_rejects_another_tag_and_a_renamed_tarball(bundle, tmp_path):
    other = "main-" + "b" * 40
    with pytest.raises(build_bundle.BundleError, match="named"):
        build_bundle.verify_bundle(bundle, other)
    renamed = tmp_path / f"limnopulse-workers-{other}.tar.gz"
    renamed.write_bytes(bundle.read_bytes())
    with pytest.raises(build_bundle.BundleError, match="bundle.env"):
        build_bundle.verify_bundle(renamed, other)


@pytest.mark.parametrize("tag", ["main-abc", "main-" + "A" * 40, "v1.0", "main-" + "a" * 41])
def test_the_tag_must_be_a_main_release(tmp_path, tag):
    with pytest.raises(build_bundle.BundleError, match="tag"):
        build_bundle.build_bundle(tag, fake_binaries(tmp_path / "bin"), tmp_path / "out")


# --- binaries ------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda d: (d / "email-worker").unlink(), "missing \\['email-worker'\\]"),
        (lambda d: (d / "notifications").write_bytes(b"x"), "extra \\['notifications'\\]"),
        (lambda d: (d / "worker-monitor").write_bytes(b""), "worker-monitor must be"),
        (
            lambda d: ((d / "telegram-worker").unlink(), (d / "telegram-worker").symlink_to("x")),
            "telegram-worker must be",
        ),
    ],
)
def test_the_binaries_directory_must_hold_exactly_the_go_roles(tmp_path, change, message):
    directory = fake_binaries(tmp_path / "bin")
    change(directory)
    with pytest.raises(build_bundle.BundleError, match=message):
        build_bundle.build_bundle(TAG, directory, tmp_path / "out")


def test_the_bundle_refuses_units_that_drifted_from_roles_yaml(tmp_path):
    drifted = tmp_path / "systemd"
    units.write(load_roles(), drifted)
    assert set(build_bundle.unit_files(load_roles(), drifted)) == set(DECLARED_UNITS)
    path = drifted / "limnopulse-email-worker.service"
    path.write_text(path.read_text().replace("WatchdogSec=120", "WatchdogSec=600"))
    with pytest.raises(build_bundle.BundleError, match="limnopulse-email-worker.service"):
        build_bundle.unit_files(load_roles(), drifted)
    path.unlink()
    (drifted / "limnopulse-notifications.service").write_text("[Service]\n")
    with pytest.raises(build_bundle.BundleError, match="limnopulse-notifications.service"):
        build_bundle.unit_files(load_roles(), drifted)


def test_static_elf_accepts_only_a_static_amd64_executable():
    assert build_bundle.static_elf_problem(elf()) is None
    assert "PT_INTERP" in build_bundle.static_elf_problem(elf(program_types=(1, 3)))
    assert "PT_DYNAMIC" in build_bundle.static_elf_problem(elf(program_types=(1, 2)))
    assert "x86-64" in build_bundle.static_elf_problem(elf(machine=183))
    assert "64-bit" in build_bundle.static_elf_problem(elf(klass=1))
    assert "executable" in build_bundle.static_elf_problem(elf(elf_type=3))
    assert build_bundle.static_elf_problem(b"#!/bin/sh\n") == "not an ELF file"
    assert "program headers" in build_bundle.static_elf_problem(elf(program_types=()))


def test_verify_with_static_elf_checks_every_binary(tmp_path):
    directory = tmp_path / "bin"
    directory.mkdir()
    for name in GO_ROLES:
        (directory / name).write_bytes(elf())
    path = build_bundle.build_bundle(TAG, directory, tmp_path / "out")
    assert build_bundle.verify_bundle(path, TAG, static_elf=True)
    (directory / "email-worker").write_bytes(elf(program_types=(3,)))
    dynamic = build_bundle.build_bundle(TAG, directory, tmp_path / "out-dynamic")
    with pytest.raises(build_bundle.BundleError, match="bin/email-worker.*PT_INTERP"):
        build_bundle.verify_bundle(dynamic, TAG, static_elf=True)


def test_binaries_builds_every_go_role_static_trimmed_and_without_a_build_id(tmp_path):
    record = tmp_path / "go.json"
    go = tmp_path / "go"
    go.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "args = sys.argv[1:]\n"
        "keys = ('CGO_ENABLED', 'GOOS', 'GOARCH', 'GOAMD64', 'GOFLAGS')\n"
        f"json.dump({{'args': args, 'cwd': os.getcwd(), "
        f"'env': {{k: os.environ.get(k) for k in keys}}}}, open({str(record)!r}, 'w'))\n"
        "out = args[args.index('-o') + 1]\n"
        "for package in args[args.index('-o') + 2:]:\n"
        "    open(os.path.join(out, os.path.basename(package)), 'w').write('bin')\n"
    )
    go.chmod(0o755)
    out = tmp_path / "bin"
    built = build_bundle.build_binaries(out, go=str(go))
    assert [path.name for path in built] == sorted(GO_ROLES)
    call = json.loads(record.read_text())
    assert call["cwd"] == str(units.ROOT)
    assert call["env"] == {
        "CGO_ENABLED": "0",
        "GOOS": "linux",
        "GOARCH": "amd64",
        "GOAMD64": "v1",
        "GOFLAGS": "-mod=readonly",
    }
    assert call["args"] == [
        "build",
        "-trimpath",
        "-buildvcs=false",
        "-ldflags=-s -w -buildid=",
        "-o",
        f"{out}{os.sep}",
        *(f"./cmd/{name}" for name in GO_ROLES),
    ]
    with pytest.raises(build_bundle.BundleError, match="not empty"):
        build_bundle.build_binaries(out, go=str(go))


def test_the_command_line_works_only_in_dist_workers(tmp_path, capsys, monkeypatch):
    workspace = tmp_path / "dist/workers"
    monkeypatch.setattr(build_bundle, "WORKSPACE", workspace)
    fake_binaries(workspace / "bin")
    assert build_bundle.main(["bundle", "--tag", TAG]) == 0
    path, digest = capsys.readouterr().out.split()
    assert Path(path) == workspace / f"limnopulse-workers-{TAG}.tar.gz"
    assert digest == sha256(Path(path).read_bytes())
    assert build_bundle.main(["verify", "--tag", TAG]) == 0
    assert "SHA256SUMS" in capsys.readouterr().out.split()
    assert build_bundle.main(["verify", "--tag", TAG, "--static-elf"]) == 1
    assert "not an ELF file" in capsys.readouterr().err
    assert build_bundle.main(["verify", "--tag", "main-" + "1" * 40]) == 1
    assert "No such file" in capsys.readouterr().err
    assert build_bundle.main(["verify", "--tag", "../" + TAG]) == 1
    assert "the tag must be" in capsys.readouterr().err
    # Nothing names a path: the subcommands take no file or directory argument.
    for extra in (["binaries", "--out", "x"], ["bundle", "--tag", TAG, "--out", "x"]):
        with pytest.raises(SystemExit):
            build_bundle.main(extra)


def test_the_binaries_command_builds_into_dist_workers_bin(tmp_path, capsys, monkeypatch):
    calls = []
    monkeypatch.setattr(build_bundle, "WORKSPACE", tmp_path / "dist/workers")
    monkeypatch.setattr(
        build_bundle, "build_binaries", lambda out: calls.append(out) or [out / "alert-evaluator"]
    )
    assert build_bundle.main(["binaries"]) == 0
    assert calls == [tmp_path / "dist/workers/bin"]
    assert capsys.readouterr().out == f"{tmp_path / 'dist/workers/bin/alert-evaluator'}\n"


# --- the S4 activation helper accepts it ---------------------------------------------------


def test_the_activation_helper_stages_and_activates_a_built_bundle(tmp_path):
    bundle = build_bundle.build_bundle(TAG, fake_binaries(tmp_path / "bin"), tmp_path / "out")
    host = Host(tmp_path / "host")
    digest = host.publish(TAG, bundle.read_bytes())
    assert digest == sha256(bundle.read_bytes())

    staged = host.run("stage", TAG, digest)
    assert staged.returncode == 0, staged.stderr
    assert host.calls("oras")[0]["argv"] == [
        "pull",
        "--no-tty",
        "-o",
        host.calls("oras")[0]["argv"][3],
        f"ghcr.io/viniclus/limnopulse/workers:{TAG}",
    ]

    activated = host.run("activate", TAG)
    assert activated.returncode == 0, activated.stderr
    assert os.readlink(host.current) == f"releases/{TAG}"
    installed = {path.name: path.read_bytes() for path in host.units.iterdir()}
    assert installed == {path.name: path.read_bytes() for path in units.UNITS_DIR.iterdir()}
    for name in GO_ROLES:
        binary = host.current / "bin" / name
        assert binary.read_bytes() == f"#!/bin/sh\n# fake {name}\n".encode()
        assert binary.stat().st_mode & 0o777 == 0o755
    status = host.run("status")
    assert status.stdout.splitlines()[0] == f"current {TAG}"
