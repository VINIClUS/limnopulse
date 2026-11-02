"""Build and verify the ADR-020 workers bundle (`ops/workers/README.md`, "Workers bundle").

    build_bundle.py binaries --out DIR
        `go build` every Go role of roles.yaml into DIR as a static linux/amd64 executable.
    build_bundle.py bundle --tag TAG --binaries DIR --out DIR
        Write DIR/limnopulse-workers-TAG.tar.gz from those executables, roles.yaml and
        ops/workers/systemd, then verify it. Prints the tarball path and its SHA-256.
    build_bundle.py verify --tag TAG [--static-elf] TARBALL
        Check a tarball against roles.yaml and the committed units, and that it is the
        canonical (byte-reproducible) packing of its own contents.

The tarball is deterministic: USTAR entries sorted by path, mtime 0, uid and gid 0 with no
owner names, fixed modes, compressed with `gzip -9 -n`. Binaries are built with
CGO_ENABLED=0, -trimpath, -buildvcs=false and an empty build ID, so two builds of one commit
with one Go toolchain produce the same bytes.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import stat
import struct
import subprocess
import sys
import tarfile
from pathlib import Path
from typing import Any

from scripts.workers import units

GO_LIFECYCLES = ("continuous", "oneshot", "cli")
TAG_RE = re.compile(r"main-[0-9a-f]{40}")
BINARY_MODE, FILE_MODE, DIRECTORY_MODE = 0o755, 0o644, 0o755
GO_ENVIRONMENT = {
    "CGO_ENABLED": "0",
    "GOOS": "linux",
    "GOARCH": "amd64",
    "GOAMD64": "v1",
    "GOFLAGS": "-mod=readonly",
}
GO_BUILD_FLAGS = ("-trimpath", "-buildvcs=false", "-ldflags=-s -w -buildid=")

# ELF identification and the fields a static linux/amd64 executable must have.
ELF_MAGIC = b"\x7fELF"
ELFCLASS64, ELFDATA2LSB, EM_X86_64 = 2, 1, 62
ET_EXEC = 2
PT_DYNAMIC, PT_INTERP = 2, 3


class BundleError(Exception):
    pass


def table() -> dict[str, Any]:
    return units.load_roles()


def go_roles(roles: dict[str, Any]) -> list[str]:
    return [role["name"] for role in roles["roles"] if role["lifecycle"] in GO_LIFECYCLES]


def env_key(role: str) -> str:
    return role.upper().replace("-", "_")


def bundle_file(roles: dict[str, Any], tag: str) -> str:
    return roles["bundle"]["file"].format(tag=tag)


def check_tag(tag: str) -> str:
    if not TAG_RE.fullmatch(tag):
        raise BundleError(f"the tag must be main-<40 lowercase hex>, got {tag!r}")
    return tag


# --- binaries ---------------------------------------------------------------------------


def build_binaries(out: Path, go: str = "go") -> list[Path]:
    """Build each Go role's entrypoint into `out`, which must not exist or be empty."""
    roles = table()
    if out.exists() and any(out.iterdir()):
        raise BundleError(f"{out} is not empty")
    out.mkdir(parents=True, exist_ok=True)
    entrypoints = [
        f"./{role['entrypoint']}" for role in roles["roles"] if role["lifecycle"] in GO_LIFECYCLES
    ]
    subprocess.run(
        [go, "build", *GO_BUILD_FLAGS, "-o", f"{out}{os.sep}", *entrypoints],
        cwd=units.ROOT,
        env={**os.environ, **GO_ENVIRONMENT},
        check=True,
    )
    return [path for _, path in sorted(read_binaries(roles, out).items())]


def read_binaries(roles: dict[str, Any], directory: Path) -> dict[str, Path]:
    """Exactly one regular, non-empty file per Go role, and nothing else."""
    expected = set(go_roles(roles))
    present = {path.name: path for path in directory.iterdir()} if directory.is_dir() else {}
    if set(present) != expected:
        raise BundleError(
            f"{directory} must hold exactly the Go roles: extra {sorted(set(present) - expected)}"
            f", missing {sorted(expected - set(present))}"
        )
    for name, path in present.items():
        info = os.lstat(path)
        if not stat.S_ISREG(info.st_mode) or info.st_size == 0:
            raise BundleError(f"{name} must be a non-empty regular file")
    return present


def static_elf_problem(data: bytes) -> str | None:
    """Why `data` is not a static linux/amd64 executable, or None."""
    if len(data) < 64 or data[:4] != ELF_MAGIC:
        return "not an ELF file"
    if data[4] != ELFCLASS64 or data[5] != ELFDATA2LSB:
        return "not a 64-bit little-endian ELF"
    e_type, e_machine = struct.unpack_from("<HH", data, 16)
    if e_machine != EM_X86_64:
        return f"machine {e_machine} is not x86-64"
    if e_type != ET_EXEC:
        return f"ELF type {e_type} is not a fixed-address executable"
    (e_phoff,) = struct.unpack_from("<Q", data, 32)
    e_phentsize, e_phnum = struct.unpack_from("<HH", data, 54)
    if e_phnum == 0 or e_phentsize < 4 or e_phoff + e_phnum * e_phentsize > len(data):
        return "no readable program headers"
    for index in range(e_phnum):
        (p_type,) = struct.unpack_from("<I", data, e_phoff + index * e_phentsize)
        if p_type == PT_INTERP:
            return "it has a dynamic loader (PT_INTERP)"
        if p_type == PT_DYNAMIC:
            return "it is dynamically linked (PT_DYNAMIC)"
    return None


# --- bundle contents --------------------------------------------------------------------


def bundle_env(roles: dict[str, Any], tag: str) -> bytes:
    lines = [f"BUNDLE_FORMAT={roles['bundle']['format']}", f"BUNDLE_TAG={tag}"]
    for role in roles["roles"]:
        if role["lifecycle"] in GO_LIFECYCLES:
            key = env_key(role["name"])
            lines += [
                f"EPOCH_{key}={role['rollback']['epoch']}",
                f"FLOOR_{key}={role['rollback']['floor']}",
            ]
    return ("\n".join(lines) + "\n").encode("ascii")


def roles_json(roles: dict[str, Any]) -> bytes:
    """roles.yaml as JSON: the same keys and values, for worker-monitor and the helper."""
    return (json.dumps(roles, indent=2, sort_keys=True) + "\n").encode("ascii")


def unit_files(roles: dict[str, Any], directory: Path = units.UNITS_DIR) -> dict[str, bytes]:
    """The committed units, which must be exactly the rendering of roles.yaml."""
    stale = units.drift(roles, directory)
    if stale:
        raise BundleError(f"ops/workers/systemd differs from roles.yaml: {stale}")
    return {name: text.encode("utf-8") for name, text in units.committed(directory).items()}


def manifest(files: dict[str, bytes]) -> bytes:
    return "".join(
        f"{hashlib.sha256(data).hexdigest()}  {path}\n" for path, data in sorted(files.items())
    ).encode("ascii")


def contents(
    roles: dict[str, Any], tag: str, binaries: dict[str, bytes], unit_texts: dict[str, bytes]
) -> dict[str, bytes]:
    layout = roles["bundle"]
    files = {
        layout["metadata"]: bundle_env(roles, tag),
        layout["role_table"]: roles_json(roles),
        **{f"{layout['binaries']}/{name}": data for name, data in binaries.items()},
        **{f"{layout['units']}/{name}": data for name, data in unit_texts.items()},
    }
    return {**files, layout["checksum_manifest"]: manifest(files)}


# --- packing ------------------------------------------------------------------------------


def _entry(name: str, kind: bytes, mode: int, size: int = 0) -> tarfile.TarInfo:
    info = tarfile.TarInfo(name)
    info.type, info.mode, info.size = kind, mode, size
    info.mtime, info.uid, info.gid, info.uname, info.gname = 0, 0, 0, "", ""
    return info


def entries(roles: dict[str, Any], files: dict[str, bytes]) -> list[tuple[tarfile.TarInfo, bytes]]:
    """The tar entries, sorted by path: a directory entry for bin and systemd, then files."""
    binaries = roles["bundle"]["binaries"]
    directories = {path.split("/", 1)[0] for path in files if "/" in path}
    items = [(name, None) for name in directories] + list(files.items())
    out = []
    for name, data in sorted(items, key=lambda item: item[0]):
        if data is None:
            out.append((_entry(name, tarfile.DIRTYPE, DIRECTORY_MODE), b""))
        else:
            mode = BINARY_MODE if name.startswith(f"{binaries}/") else FILE_MODE
            out.append((_entry(name, tarfile.REGTYPE, mode, len(data)), data))
    return out


def pack(roles: dict[str, Any], files: dict[str, bytes]) -> bytes:
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for info, data in entries(roles, files):
            archive.addfile(info, io.BytesIO(data) if info.isreg() else None)
    # `gzip -n` stores no name and a zero mtime; its deflate output is stable across hosts.
    return subprocess.run(
        ["gzip", "-9", "-n", "-c"], input=raw.getvalue(), capture_output=True, check=True
    ).stdout


def build_bundle(tag: str, binaries: Path, out: Path) -> Path:
    roles = table()
    check_tag(tag)
    data = {name: path.read_bytes() for name, path in read_binaries(roles, binaries).items()}
    tarball = pack(roles, contents(roles, tag, data, unit_files(roles)))
    out.mkdir(parents=True, exist_ok=True)
    path = out / bundle_file(roles, tag)
    path.write_bytes(tarball)
    verify_bundle(path, tag)
    return path


# --- verification ---------------------------------------------------------------------


def unpack(tarball: bytes) -> tuple[list[tarfile.TarInfo], dict[str, bytes]]:
    if tarball[:3] != b"\x1f\x8b\x08" or tarball[3] != 0 or tarball[4:8] != b"\0\0\0\0":
        raise BundleError("the gzip header must carry no name, no extra fields and mtime 0")
    members: list[tarfile.TarInfo] = []
    files: dict[str, bytes] = {}
    try:
        with tarfile.open(fileobj=io.BytesIO(tarball), mode="r:gz") as archive:
            for member in archive:
                members.append(member)
                if member.isreg():
                    handle = archive.extractfile(member)
                    files[member.name] = handle.read() if handle else b""
    except (tarfile.TarError, OSError, EOFError) as error:
        raise BundleError(f"the bundle is not a readable tar.gz: {error}") from None
    return members, files


def verify_bundle(path: Path, tag: str, static_elf: bool = False) -> list[str]:
    """Check a tarball; return its file list or raise BundleError naming the first problem."""
    roles = table()
    check_tag(tag)
    if path.name != bundle_file(roles, tag):
        raise BundleError(f"the tarball must be named {bundle_file(roles, tag)}")
    tarball = path.read_bytes()
    members, files = unpack(tarball)
    layout = roles["bundle"]
    names = [member.name for member in members]
    if len(set(names)) != len(names):
        raise BundleError("the bundle lists an entry twice")
    for member in members:
        if not (member.isreg() or member.isdir()):
            raise BundleError(f"{member.name} is neither a file nor a directory")
    binaries = {
        name.split("/", 1)[1]: data
        for name, data in files.items()
        if name.startswith(f"{layout['binaries']}/")
    }
    if sorted(binaries) != sorted(go_roles(roles)):
        raise BundleError(f"bin/ must hold exactly the Go roles, not {sorted(binaries)}")
    if static_elf:
        for name, data in sorted(binaries.items()):
            problem = static_elf_problem(data)
            if problem:
                raise BundleError(f"bin/{name} is not a static linux/amd64 executable: {problem}")
    expected = contents(roles, tag, binaries, unit_files(roles))
    # The manifest last, so a changed file is named rather than the manifest that lists it.
    checksums = layout["checksum_manifest"]
    for name in sorted(set(expected) | set(files), key=lambda name: (name == checksums, name)):
        if expected.get(name) != files.get(name):
            raise BundleError(f"{name} does not match roles.yaml and ops/workers/systemd")
    if pack(roles, expected) != tarball:
        raise BundleError("the tarball is not the canonical packing of its contents")
    return sorted(files)


# --- command line -------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    binaries = commands.add_parser("binaries", help="go build every Go role")
    binaries.add_argument("--out", type=Path, required=True)
    bundle = commands.add_parser("bundle", help="pack and verify the bundle tarball")
    bundle.add_argument("--tag", required=True)
    bundle.add_argument("--binaries", type=Path, required=True)
    bundle.add_argument("--out", type=Path, required=True)
    verify = commands.add_parser("verify", help="verify a bundle tarball")
    verify.add_argument("--tag", required=True)
    verify.add_argument("--static-elf", action="store_true")
    verify.add_argument("tarball", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "binaries":
            for path in build_binaries(args.out):
                print(path)
        elif args.command == "bundle":
            path = build_bundle(args.tag, args.binaries, args.out)
            print(f"{path} {hashlib.sha256(path.read_bytes()).hexdigest()}")
        else:
            for name in verify_bundle(args.tarball, args.tag, args.static_elf):
                print(name)
    except (BundleError, subprocess.CalledProcessError, OSError) as error:
        print(f"build_bundle: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
