"""Live M1 e2e run against an already running local stack and API.

Opt-in: ``RUN_M1_E2E=1``. Requires mosquitto on MQTT_HOST:MQTT_PORT, the API
at LIMNOPULSE_API_URL (seeded, M1 migration applied, API_V2_ENABLED=true), and
``node-red`` + ``mqttx`` on PATH. Set ``ALLOW_V2_UNAVAILABLE=1`` to accept
``ok=null`` verdicts while /v2 is not deployed.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "ops/e2e/run_m1_e2e.sh"

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_M1_E2E") != "1",
    reason="set RUN_M1_E2E=1 to run the live M1 e2e harness",
)


def test_m1_e2e_harness_passes_against_running_stack() -> None:
    missing = [tool for tool in ("bash", "node-red", "mqttx", "curl") if shutil.which(tool) is None]
    assert missing == [], f"missing tools: {missing}"

    result = subprocess.run(
        ["bash", str(SCRIPT)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=int(os.environ.get("M1_E2E_PYTEST_TIMEOUT", "240")),
    )

    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    assert "SUMMARY verdicts=" in output
