"""ADR-020 S5: the telemetry ingest worker's liveness stamp, stall watchdog and healthcheck.

`heartbeat` already names the SQS visibility extension (P3-08), so these say liveness. The stamp
is a wall-clock time written atomically under the temp directory (the container's only tmpfs),
the watchdog is a thread that sees a blocked event loop, and the healthcheck reads only the stamp.
"""

import json
import logging
import tempfile
import threading
import time
from pathlib import Path

import pytest

from limnopulse_api.workers import telemetry_ingest_liveness as liveness
from limnopulse_api.workers.telemetry_ingest_liveness import (
    LivenessStamp,
    LivenessWatchdog,
    healthcheck,
    liveness_path,
)

BOUND_SECONDS = 5  # every wait below is bounded, so a broken watchdog fails instead of hanging


class Clock:
    """A settable clock, so ages are exact and no test sleeps through a threshold."""

    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


def _stamp(tmp_path: Path, *, stall_after: float = 100, **clocks: Clock) -> LivenessStamp:
    return LivenessStamp(tmp_path / "worker.liveness", stall_after_seconds=stall_after, **clocks)


def _until(predicate, *, timeout: float = BOUND_SECONDS) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition not reached"
        time.sleep(0.005)


# --- the stamp ---------------------------------------------------------------------------------


def test_the_stamp_lives_under_the_temp_directory(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """The container is read-only with a tmpfs at /tmp only; TMPDIR moves it for tests."""
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))

    assert liveness_path() == tmp_path / liveness.LIVENESS_FILE_NAME


def test_mark_writes_the_wall_clock_time_and_the_threshold(tmp_path: Path) -> None:
    wall = Clock(1_730_000_000.25)
    stamp = _stamp(tmp_path, stall_after=111, wall_clock=wall)

    stamp.mark()

    assert json.loads(stamp.path.read_text(encoding="utf-8")) == {
        "marked_at": 1_730_000_000.25,
        "stall_after_seconds": 111,
    }


def test_mark_replaces_the_stamp_atomically_and_leaves_no_temp_file(tmp_path: Path) -> None:
    wall = Clock(10.0)
    stamp = _stamp(tmp_path, wall_clock=wall)
    replaced: list[tuple[str, str]] = []
    real_replace = liveness.os.replace

    def spy(source: str, target: str) -> None:
        replaced.append((str(source), str(target)))
        real_replace(source, target)

    stamp.mark()
    wall.now = 20.0
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(liveness.os, "replace", spy)
        stamp.mark()

    assert json.loads(stamp.path.read_text(encoding="utf-8"))["marked_at"] == 20.0
    # Written beside the stamp (same filesystem) and renamed over it in one step.
    ((source, target),) = replaced
    assert target == str(stamp.path) and Path(source).parent == stamp.path.parent
    assert sorted(path.name for path in tmp_path.iterdir()) == [stamp.path.name]


def test_seconds_since_mark_uses_the_monotonic_clock_not_the_wall_clock(tmp_path: Path) -> None:
    """An NTP step of the wall clock must never look like a stall to the watchdog."""
    wall, monotonic = Clock(5_000.0), Clock(50.0)
    stamp = _stamp(tmp_path, wall_clock=wall, monotonic=monotonic)
    stamp.mark()

    wall.now += 3_600
    monotonic.now += 7

    assert stamp.seconds_since_mark() == 7


def test_a_new_stamp_counts_from_its_creation_before_the_first_mark(tmp_path: Path) -> None:
    monotonic = Clock(50.0)
    stamp = _stamp(tmp_path, monotonic=monotonic)

    monotonic.now = 62.0

    assert stamp.seconds_since_mark() == 12
    assert not stamp.path.exists()  # nothing on disk until the loop really runs


def test_a_failed_write_never_raises_and_still_feeds_the_watchdog(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    monotonic = Clock(0.0)
    stamp = LivenessStamp(
        tmp_path / "missing-dir" / "worker.liveness", stall_after_seconds=100, monotonic=monotonic
    )
    caplog.set_level(logging.WARNING)

    for now in (1.0, 2.0, 3.0):
        monotonic.now = now
        stamp.mark()

    assert stamp.seconds_since_mark() == 0
    failures = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    # Once per failing streak, by category only, never once per loop turn.
    assert failures == ["telemetry ingest liveness stamp not written (FileNotFoundError)"]
    assert "missing-dir" not in caplog.text


# --- healthcheck -------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("age", "expected"),
    [(0, 0), (60, 0), (111, 0), (111.5, 1), (5_000, 1), (-30, 0)],
    ids=["just-marked", "fresh", "at-threshold", "stale", "long-dead", "clock-stepped-back"],
)
def test_healthcheck_answers_from_the_stamp_and_its_own_threshold(
    tmp_path: Path, age: float, expected: int
) -> None:
    wall = Clock(1_000_000.0)
    stamp = _stamp(tmp_path, stall_after=111, wall_clock=wall)
    stamp.mark()

    assert healthcheck(stamp.path, wall_clock=Clock(wall.now + age)) == expected


def test_healthcheck_uses_the_threshold_the_worker_wrote(tmp_path: Path) -> None:
    """The healthcheck runs before any Settings, so the worker writes its threshold too."""
    path = tmp_path / "worker.liveness"
    path.write_text(json.dumps({"marked_at": 100.0, "stall_after_seconds": 500}), "utf-8")

    assert healthcheck(path, wall_clock=Clock(550.0)) == 0
    path.write_text(json.dumps({"marked_at": 100.0, "stall_after_seconds": 400}), "utf-8")
    assert healthcheck(path, wall_clock=Clock(550.0)) == 1


@pytest.mark.parametrize(
    "content",
    [None, "", "not json", "[]", '{"marked_at": 1}', '{"marked_at": "x", "stall_after_seconds": 9}'],
    ids=["missing", "empty", "garbage", "list", "no-threshold", "not-a-number"],
)
def test_healthcheck_fails_without_a_readable_stamp(tmp_path: Path, content: str | None) -> None:
    """A parked (exit 78) or not yet running worker has no stamp, so it is never healthy."""
    path = tmp_path / "worker.liveness"
    if content is not None:
        path.write_text(content, encoding="utf-8")

    assert healthcheck(path, wall_clock=Clock(0.0)) == 1


def test_healthcheck_defaults_to_the_worker_stamp(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    assert healthcheck() == 1

    LivenessStamp(liveness_path(), stall_after_seconds=100).mark()

    assert healthcheck() == 0


# --- the watchdog ------------------------------------------------------------------------------


class Recorder:
    def __init__(self) -> None:
        self.stalls = 0
        self.exits: list[int] = []
        self.event = threading.Event()

    def on_stall(self) -> None:
        self.stalls += 1

    def force_exit(self, code: int) -> None:
        self.exits.append(code)
        self.event.set()


def _watchdog(
    stamp: LivenessStamp, recorder: Recorder, *, exit_grace: float = 0.05
) -> LivenessWatchdog:
    return LivenessWatchdog(
        stamp,
        on_stall=recorder.on_stall,
        exit_grace_seconds=exit_grace,
        check_every_seconds=0.005,
        force_exit=recorder.force_exit,
    )


def test_the_watchdog_leaves_a_live_loop_alone(tmp_path: Path) -> None:
    monotonic = Clock(0.0)
    stamp = _stamp(tmp_path, stall_after=10, monotonic=monotonic)
    recorder = Recorder()
    watchdog = _watchdog(stamp, recorder)
    watchdog.start()
    try:
        for now in range(0, 40, 2):
            monotonic.now = float(now)
            stamp.mark()
            time.sleep(0.01)
    finally:
        watchdog.close()

    assert (recorder.stalls, recorder.exits, watchdog.stalled) == (0, [], False)


def test_a_stall_stops_the_loop_then_forces_exit_1_after_the_grace(
    tmp_path: Path, capfd: pytest.CaptureFixture[str]
) -> None:
    monotonic = Clock(0.0)
    stamp = _stamp(tmp_path, stall_after=10, monotonic=monotonic)
    recorder = Recorder()
    watchdog = _watchdog(stamp, recorder, exit_grace=0.2)
    watchdog.start()

    monotonic.now = 10.4
    _until(lambda: recorder.stalls == 1)
    assert watchdog.stalled is True
    assert recorder.exits == []  # the clean path (`on_stall`) gets its grace first
    assert recorder.event.wait(BOUND_SECONDS)
    watchdog.join(BOUND_SECONDS)

    assert (recorder.stalls, recorder.exits) == (1, [1])
    err = capfd.readouterr().err
    # Written straight to fd 2: a blocked loop thread may hold the logging lock.
    assert "telemetry ingest worker stalled: no liveness mark for 10 s (threshold 10 s)" in err
    assert "telemetry ingest worker did not exit after the stall; forcing exit 1" in err


def test_a_failing_on_stall_still_forces_the_exit(tmp_path: Path) -> None:
    monotonic = Clock(0.0)
    stamp = _stamp(tmp_path, stall_after=1, monotonic=monotonic)
    recorder = Recorder()

    def broken() -> None:
        raise RuntimeError("the event loop is already closed")

    watchdog = LivenessWatchdog(
        stamp,
        on_stall=broken,
        exit_grace_seconds=0.01,
        check_every_seconds=0.005,
        force_exit=recorder.force_exit,
    )
    monotonic.now = 5.0  # five seconds without a mark
    watchdog.start()

    assert recorder.event.wait(BOUND_SECONDS)
    watchdog.join(BOUND_SECONDS)
    assert recorder.exits == [1]


def test_a_disarmed_watchdog_never_fires(tmp_path: Path) -> None:
    """A stop (SIGTERM) disarms it: the A47 drain may take longer than the threshold."""
    monotonic = Clock(0.0)
    stamp = _stamp(tmp_path, stall_after=10, monotonic=monotonic)
    recorder = Recorder()
    watchdog = _watchdog(stamp, recorder)
    watchdog.start()

    watchdog.disarm()
    monotonic.now = 1_000.0
    time.sleep(0.05)
    watchdog.close()

    assert (recorder.stalls, recorder.exits, watchdog.stalled) == (0, [], False)


def test_close_stops_and_joins_the_thread(tmp_path: Path) -> None:
    """No watchdog may outlive an in-process `main()`: it could `os._exit` the test run."""
    watchdog = _watchdog(_stamp(tmp_path, monotonic=Clock(0.0)), Recorder())
    watchdog.start()
    assert watchdog.alive

    watchdog.close()

    assert not watchdog.alive


def test_close_after_a_stall_keeps_the_forced_exit(tmp_path: Path) -> None:
    """After a stall the forced exit must outlive the loop: a hung pool thread would otherwise
    hold the interpreter's shutdown forever."""
    monotonic = Clock(0.0)
    stamp = _stamp(tmp_path, stall_after=1, monotonic=monotonic)
    recorder = Recorder()
    watchdog = _watchdog(stamp, recorder, exit_grace=0.1)
    watchdog.start()
    monotonic.now = 2.0
    _until(lambda: watchdog.stalled)

    watchdog.close()

    assert recorder.event.wait(BOUND_SECONDS)
    watchdog.join(BOUND_SECONDS)
    assert recorder.exits == [1]


def test_the_watchdog_thread_is_a_daemon(tmp_path: Path) -> None:
    """A daemon thread never holds the interpreter's exit after a clean stop."""
    watchdog = _watchdog(_stamp(tmp_path, monotonic=Clock(0.0)), Recorder())
    watchdog.start()
    try:
        assert watchdog.daemon is True
    finally:
        watchdog.close()
