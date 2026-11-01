"""Liveness of the telemetry ingest worker (ADR-020): the stamp, the stall watchdog and the
`healthcheck` subcommand. `heartbeat` already names the SQS visibility extension (P3-08), so
everything here says liveness.

- **Stamp.** The loop calls `LivenessStamp.mark()` at the top of every turn and after every
  receive. A mark keeps a monotonic time in memory for the watchdog and writes the wall-clock time,
  with the worker's stall threshold, to `liveness_path()`: a temp file renamed over the stamp, so
  a reader never sees half a write. The temp directory is the only writable place in the
  read-only container (its tmpfs `/tmp`). A failed write is logged once per failing streak and
  never raised: liveness never decides what happens to a message.
- **Watchdog.** A thread, because an asyncio task cannot run while the event loop is blocked.
  When no mark came for the threshold it calls `on_stall` once, the clean path (the worker cancels
  its loop through the event loop and exits 1). If the process still runs `exit_grace_seconds`
  later it calls `os._exit(1)`: a blocked event loop never runs that clean path, and a pool thread
  stuck in a call would hold the interpreter's shutdown forever, so only `os._exit` cannot hang.
  Nothing is lost: SQS hands every in-flight message out again once its visibility lapses, and
  every write is idempotent. A stop disarms it, so a drain may outlast the threshold.
- **Healthcheck.** `healthcheck()` answers 0 while the stamp is fresher than the threshold written
  with it, else 1, so it needs no settings. It imports only the standard library, so the compose
  probe stays cheap inside the worker's CPU limit.

The watchdog writes its two lines straight to file descriptor 2: a blocked loop thread may hold
the logging lock. Lines carry closed text only.
"""

import json
import logging
import math
import os
import tempfile
import threading
import time
from collections.abc import Callable
from pathlib import Path

logger = logging.getLogger("limnopulse.telemetry.ingest")

HEALTHCHECK = "healthcheck"
LIVENESS_FILE_NAME = "limnopulse-telemetry-ingest.liveness"
EXIT_HEALTHY = 0
EXIT_UNHEALTHY = 1
_EXIT_STALLED = 1


def liveness_path() -> Path:
    return Path(tempfile.gettempdir()) / LIVENESS_FILE_NAME


class LivenessStamp:
    def __init__(
        self,
        path: Path,
        *,
        stall_after_seconds: float,
        wall_clock: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.path = path
        self.stall_after_seconds = stall_after_seconds
        self._wall_clock = wall_clock
        self._monotonic = monotonic
        # Counted from creation: a loop that never reaches its first turn is a stall too.
        self._last_mark = monotonic()
        self._failing = False

    def mark(self) -> None:
        self._last_mark = self._monotonic()
        body = json.dumps(
            {"marked_at": self._wall_clock(), "stall_after_seconds": self.stall_after_seconds}
        )
        temporary = self.path.with_name(f"{self.path.name}.tmp")
        try:
            temporary.write_text(body, encoding="utf-8")
            os.replace(temporary, self.path)
        except OSError as error:
            if not self._failing:
                logger.warning(
                    "telemetry ingest liveness stamp not written (%s)", type(error).__name__
                )
            self._failing = True
        else:
            self._failing = False

    def seconds_since_mark(self) -> float:
        return self._monotonic() - self._last_mark


def healthcheck(path: Path | None = None, *, wall_clock: Callable[[], float] = time.time) -> int:
    """Exit 0 while the stamp is fresher than its threshold, else 1. A parked worker (exit 78) or
    one that never reached its loop has no stamp; a clock stepped back reads as fresh."""
    path = liveness_path() if path is None else path
    try:
        stamp = json.loads(path.read_text(encoding="utf-8"))
        marked_at = float(stamp["marked_at"])
        stall_after = float(stamp["stall_after_seconds"])
    except FileNotFoundError:
        print("telemetry ingest worker unhealthy: no liveness stamp")
        return EXIT_UNHEALTHY
    except (OSError, ValueError, TypeError, KeyError):
        print("telemetry ingest worker unhealthy: unreadable liveness stamp")
        return EXIT_UNHEALTHY
    age = wall_clock() - marked_at
    if not (math.isfinite(age) and age <= stall_after):
        print(
            f"telemetry ingest worker unhealthy: last liveness mark {age:.0f} s ago "
            f"(threshold {stall_after:.0f} s)"
        )
        return EXIT_UNHEALTHY
    return EXIT_HEALTHY


class LivenessWatchdog:
    def __init__(
        self,
        stamp: LivenessStamp,
        *,
        on_stall: Callable[[], None],
        exit_grace_seconds: float,
        check_every_seconds: float = 5.0,
        force_exit: Callable[[int], object] = os._exit,
    ) -> None:
        self._stamp = stamp
        self._on_stall = on_stall
        self._exit_grace = exit_grace_seconds
        self._check_every = check_every_seconds
        self._force_exit = force_exit
        self._disarmed = threading.Event()
        self._stalled = threading.Event()
        self._thread = threading.Thread(
            target=self._watch, name="telemetry-ingest-liveness", daemon=True
        )

    @property
    def stalled(self) -> bool:
        return self._stalled.is_set()

    @property
    def alive(self) -> bool:
        return self._thread.is_alive()

    @property
    def daemon(self) -> bool:
        return self._thread.daemon

    def start(self) -> None:
        self._thread.start()

    def disarm(self) -> None:
        """A stop was requested: no stall from now on. A stall already seen still exits."""
        self._disarmed.set()

    def close(self) -> None:
        """Disarm and join, unless a stall already fired: its forced exit must outlive the loop."""
        self._disarmed.set()
        if not self._stalled.is_set() and self._thread.is_alive():
            self._thread.join()

    def join(self, timeout: float | None = None) -> None:
        self._thread.join(timeout)

    def _watch(self) -> None:
        while not self._disarmed.wait(self._check_every):
            silent = self._stamp.seconds_since_mark()
            if silent <= self._stamp.stall_after_seconds:
                continue
            self._stalled.set()
            _say(
                f"telemetry ingest worker stalled: no liveness mark for {int(silent)} s "
                f"(threshold {int(self._stamp.stall_after_seconds)} s)"
            )
            try:
                self._on_stall()
            except Exception:  # noqa: BLE001, S110 - the forced exit below still runs.
                pass
            time.sleep(self._exit_grace)
            _say("telemetry ingest worker did not exit after the stall; forcing exit 1")
            self._force_exit(_EXIT_STALLED)
            return


def _say(line: str) -> None:
    try:
        os.write(2, f"{line}\n".encode())
    except OSError:
        pass
