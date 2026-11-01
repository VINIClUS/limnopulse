"""ADR-020 S5: the production worker's park-on-78 entrypoint guard, run as compose runs it.

Docker has no `RestartPreventExitStatus`, so the guard keeps the container up but idle after a
configuration error (exit 78) and passes every other exit through to `restart: unless-stopped`.
tini (`init: true`) signals only the guard shell, so the guard must forward SIGTERM and SIGINT
and wait for the worker's drain. The script comes from `compose.production.yaml` itself and runs
under `/bin/sh` with a fake `python` and a fake `sleep` first on PATH, so no Docker is needed.
"""

import os
import shutil
import signal
import subprocess
import time
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
PARK_LINE = "telemetry ingest worker refused its configuration (exit 78); parked, not restarted"
BOUND_SECONDS = 10


def _guard() -> str:
    compose = yaml.safe_load((ROOT / "compose.production.yaml").read_text(encoding="utf-8"))
    shell, flag, script = compose["services"]["telemetry-ingest-worker"]["entrypoint"]
    assert (shell, flag) == ("/bin/sh", "-c")
    return script.replace("$$", "$")  # what compose hands to the shell


class Guard:
    """The guard with a fake worker (`python`) and a fake `sleep` that only records the park."""

    def __init__(self, tmp_path: Path, mode: str) -> None:
        self.log = tmp_path / "guard.log"
        self.log.touch()
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        real_sleep = shutil.which("sleep")
        assert real_sleep is not None
        _script(
            bin_dir / "python",
            f"""
            echo "python $*" >> "$GUARD_LOG"
            case "$FAKE_MODE" in
              drain)
                trap 'echo term >> "$GUARD_LOG"; {real_sleep} 0.3; \\
                      echo drained >> "$GUARD_LOG"; exit 0' TERM
                echo ready >> "$GUARD_LOG"
                while :; do {real_sleep} 0.02; done
                ;;
              *) exit "$FAKE_MODE" ;;
            esac
            """,
        )
        _script(bin_dir / "sleep", 'echo "sleep $*" >> "$GUARD_LOG"')
        self.environment = {
            **os.environ,
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            "GUARD_LOG": str(self.log),
            "FAKE_MODE": mode,
        }

    def start(self) -> subprocess.Popen[str]:
        return subprocess.Popen(
            ["/bin/sh", "-c", _guard()],
            env=self.environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def run(self) -> subprocess.CompletedProcess[str]:
        process = self.start()
        stdout, stderr = process.communicate(timeout=BOUND_SECONDS)
        return subprocess.CompletedProcess(process.args, process.returncode, stdout, stderr)

    def lines(self) -> list[str]:
        return self.log.read_text(encoding="utf-8").splitlines()


def _script(path: Path, body: str) -> None:
    lines = [line[12:] if line.startswith(" " * 12) else line for line in body.splitlines()]
    path.write_text("#!/bin/sh\n" + "\n".join(lines).strip() + "\n", encoding="utf-8")
    path.chmod(0o755)


def test_exit_78_parks_the_container_instead_of_exiting(tmp_path: Path) -> None:
    guard = Guard(tmp_path, "78")

    result = guard.run()

    assert guard.lines() == ["python -m limnopulse_api.workers.telemetry_ingest", "sleep infinity"]
    assert PARK_LINE in result.stderr
    # The fake `sleep` returns; the real one never does, so Docker never sees the 78.
    assert result.returncode == 0


@pytest.mark.parametrize("code", [0, 1, 2, 137])
def test_every_other_exit_passes_through_to_the_restart_policy(tmp_path: Path, code: int) -> None:
    guard = Guard(tmp_path, str(code))

    result = guard.run()

    assert result.returncode == code
    assert guard.lines() == ["python -m limnopulse_api.workers.telemetry_ingest"]
    assert PARK_LINE not in result.stderr


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT], ids=["SIGTERM", "SIGINT"])
def test_a_stop_signal_reaches_the_worker_and_waits_for_its_drain(
    tmp_path: Path, signum: int
) -> None:
    """Without the trap, SIGTERM would end the shell at once, tini would exit, and the kernel
    would kill the worker mid-drain."""
    guard = Guard(tmp_path, "drain")
    process = guard.start()
    try:
        deadline = time.monotonic() + BOUND_SECONDS
        while "ready" not in guard.lines():
            assert time.monotonic() < deadline, "the fake worker never started"
            time.sleep(0.01)

        process.send_signal(signum)
        process.wait(timeout=BOUND_SECONDS)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()

    assert process.returncode == 0
    assert guard.lines()[1:] == ["ready", "term", "drained"]
