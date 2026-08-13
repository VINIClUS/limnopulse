#!/usr/bin/env python3
"""Synthetic telemetry-to-alert smoke test for a disposable lab guest."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4

from scripts.dev.publish_sample_reading import DEFAULT_TOPIC, publish

ROOT = Path(__file__).resolve().parents[2]
MARKER_PATH = Path(os.environ.get("LAB_MARKER_PATH", "/etc/vinisantana-lab"))
API_URL = os.environ.get("LIMNOPULSE_LAB_API_URL", "http://127.0.0.1:8000")
TENANT_ID = "tnt_local_001"
POND_ID = "pond_local_001"
DEVICE_ID = "local-device-001"
HEADERS = {
    "X-Dev-User-Sub": "local-user-001",
    "X-Dev-User-Email": "local@example.test",
    "Content-Type": "application/json",
}


def evaluation_time(value: str | None = None) -> str:
    current = datetime.fromisoformat(value) if value else datetime.now(UTC)
    current = current.astimezone(UTC).replace(microsecond=0)
    shifted = current + timedelta(seconds=15)
    return (shifted.replace(second=0) + timedelta(minutes=1)).isoformat().replace("+00:00", "Z")


def build_rule() -> dict[str, Any]:
    return {
        "pond_id": POND_ID,
        "device_id": DEVICE_ID,
        "metric": "do_mg_l",
        "name": f"Lab low dissolved oxygen {uuid4()}",
        "operator": "<",
        "threshold": 7.0,
        "aggregation": "min",
        "window": "60s",
        "duration": "60s",
        "severity": "critical",
        "channels": ["email"],
        "cooldown_seconds": 60,
        "enabled": True,
    }


def sample_payload(timestamp: str, sequence: int) -> dict[str, Any]:
    return {
        "ts": timestamp,
        "seq": sequence,
        "temp_c": 25.0,
        "ph": 7.1,
        "do_mg_l": 4.0,
        "turbidity_ntu": 3.0,
        "salinity_ppt": 11.0,
        "battery_v": 3.8,
        "rssi": -65,
    }


def sample_offsets() -> tuple[int, ...]:
    """Return samples strictly inside the evaluator's half-open 60-second window."""

    return (5, 15, 25, 35, 45, 55)


def request_json(method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = json.dumps(body).encode("utf-8") if body is not None else None
    request = Request(f"{API_URL}{path}", data=payload, headers=HEADERS, method=method)
    try:
        with urlopen(request, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path} returned {exc.code}: {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"{method} {path} could not reach the lab API: {exc}") from exc


def wait_for_health() -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            result = request_json("GET", "/healthz")
        except RuntimeError:
            time.sleep(2)
            continue
        if result == {"status": "ok"}:
            return
        time.sleep(2)
    raise RuntimeError("LimnoPulse API did not become healthy within 60 seconds")


def wait_for_readings(start: str, stop: str, expected_sequences: set[int]) -> None:
    deadline = time.monotonic() + 90
    path = f"/v1/tenants/{TENANT_ID}/ponds/{POND_ID}/readings?" + urlencode(
        {"start": start, "stop": stop, "limit": 100}
    )
    while time.monotonic() < deadline:
        items = request_json("GET", path).get("items", [])
        present = {item.get("seq") for item in items if item.get("do_mg_l") == 4.0}
        if expected_sequences <= present:
            return
        time.sleep(3)
    raise RuntimeError("synthetic telemetry was not visible through the API")


def run_evaluator(logical_time: str) -> None:
    completed = subprocess.run(
        [
            "docker",
            "compose",
            "--profile",
            "manual",
            "run",
            "--rm",
            "alert-evaluator",
            "run",
            f"--evaluation-time={logical_time}",
            "--max-sample-age=2m",
        ],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"alert evaluator failed ({completed.returncode}): {completed.stdout}{completed.stderr}"
        )


def matching_open_events(rule_id: str) -> list[dict[str, Any]]:
    items = request_json("GET", f"/v1/tenants/{TENANT_ID}/alert-events").get("items", [])
    return [item for item in items if item.get("rule_id") == rule_id and item.get("status") == "open"]


def require_lab_marker() -> None:
    required = {
        "managed_by: debian-vps-lab",
        "environment: lab",
        "target: limnopulse",
    }
    if not MARKER_PATH.is_file() or not required <= set(MARKER_PATH.read_text(encoding="utf-8").splitlines()):
        raise RuntimeError(f"lab marker is required at {MARKER_PATH}")


def main() -> int:
    try:
        require_lab_marker()
        wait_for_health()
        rule = request_json("POST", f"/v1/tenants/{TENANT_ID}/alert-rules", build_rule())
        rule_id = str(rule["rule_id"])

        run_at = evaluation_time()
        window_end = datetime.fromisoformat(run_at) - timedelta(seconds=15)
        window_start = window_end - timedelta(seconds=60)
        sequences: set[int] = set()
        for sequence, offset in enumerate(sample_offsets(), start=1):
            measured_at = window_start + timedelta(seconds=offset)
            payload = sample_payload(measured_at.isoformat().replace("+00:00", "Z"), sequence)
            publish("127.0.0.1", 1883, DEFAULT_TOPIC, json.dumps(payload).encode("utf-8"))
            sequences.add(sequence)

        wait_for_readings(
            window_start.isoformat().replace("+00:00", "Z"),
            window_end.isoformat().replace("+00:00", "Z"),
            sequences,
        )
        wait_seconds = (datetime.fromisoformat(run_at) - datetime.now(UTC)).total_seconds()
        if wait_seconds > 0:
            time.sleep(wait_seconds + 1)

        run_evaluator(run_at)
        if len(matching_open_events(rule_id)) != 1:
            raise RuntimeError("expected exactly one open alert event after the first evaluation")
        run_evaluator(run_at)
        if len(matching_open_events(rule_id)) != 1:
            raise RuntimeError("replaying the evaluation created a duplicate alert event")
    except (KeyError, RuntimeError) as exc:
        print(f"limnopulse lab smoke: {exc}", file=sys.stderr)
        return 7
    print("limnopulse lab smoke: telemetry-to-alert path passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
