"""Read the local worker settings `scripts/dev/run_worker.sh` exports (ADR-020: the Go
workers run as executables against the local emulators, not in Compose)."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUN_WORKER = ROOT / "scripts/dev/run_worker.sh"
LOCAL_ENV_DIR = ROOT / "scripts/dev/workers"
_LINE = re.compile(r"([A-Z][A-Z0-9_]*)=(.*)")


def local_env(role: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in (LOCAL_ENV_DIR / f"{role}.env").read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        match = _LINE.fullmatch(line)
        if match is None or match.group(1) in values:
            raise ValueError(f"{role}.env: malformed or repeated line {line!r}")
        values[match.group(1)] = match.group(2)
    return values
