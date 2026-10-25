"""Validate an actual simulator capture without requiring a broker or a token."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.skipif(
    not os.environ.get("WOKWI_SERIAL_LOG"), reason="requires a captured Wokwi serial log"
)
def test_simulator_capture_contains_at_least_two_replayable_readings():
    result = subprocess.run(
        [sys.executable, str(ROOT / "ops/e2e/wokwi/replay.py"), "--dry-run"],
        check=False,
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    replayed = [line for line in result.stdout.splitlines() if line.startswith("REPLAYED ")]
    assert len(replayed) == 1
    assert int(replayed[0].split()[1]) >= 2
