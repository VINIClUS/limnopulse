#!/usr/bin/env python3
"""Replay ``READING <json>`` lines from a Wokwi serial log onto the local broker.

Source selection (first match wins):
  1. ``--serial-log PATH``
  2. ``$WOKWI_SERIAL_LOG``
  3. ``ops/e2e/wokwi/serial.log`` (written by ``wokwi-cli --serial-log-file``)
  4. ``ops/e2e/wokwi/fixtures/serial.log`` (deterministic, mirrors the sketch)

Each payload must carry exactly the legacy reading keys. It is published with
``mqttx pub`` to ``limnopulse/v1/devices/<device_id>/readings``.

``--ts-mode now`` re-anchors timestamps so the first reading is "now" while
keeping the original spacing; deployments created by the M1 migration start at
the device ``created_at``, so fixture timestamps from the past would otherwise
fall outside every deployment interval.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT_CAPTURE = HERE / "serial.log"
FIXTURE = HERE / "fixtures" / "serial.log"
MARKER = "READING {"
EXPECTED_KEYS = frozenset(
    {
        "ts",
        "seq",
        "temp_c",
        "ph",
        "do_mg_l",
        "turbidity_ntu",
        "salinity_ppt",
        "battery_v",
        "rssi",
    }
)
TOPIC_TEMPLATE = "limnopulse/v1/devices/{device_id}/readings"


class ReplayError(Exception):
    pass


def resolve_source(explicit: str | None) -> Path:
    for candidate in (explicit, os.environ.get("WOKWI_SERIAL_LOG")):
        if candidate:
            path = Path(candidate)
            if not path.is_file():
                raise ReplayError(f"serial log not found: {path}")
            return path
    if DEFAULT_CAPTURE.is_file():
        return DEFAULT_CAPTURE
    return FIXTURE


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(UTC)


def format_ts(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def extract_readings(text: str) -> list[dict]:
    readings: list[dict] = []
    for lineno, raw in enumerate(text.splitlines(), start=1):
        index = raw.find(MARKER)
        if index < 0:
            continue
        body = raw[index + len("READING ") :].strip()
        try:
            payload = json.loads(body)
        except json.JSONDecodeError as exc:
            raise ReplayError(f"line {lineno}: invalid JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise ReplayError(f"line {lineno}: payload is not an object")
        keys = set(payload)
        if keys != EXPECTED_KEYS:
            raise ReplayError(
                f"line {lineno}: key set mismatch "
                f"(extra={sorted(keys - EXPECTED_KEYS)}, missing={sorted(EXPECTED_KEYS - keys)})"
            )
        parse_ts(payload["ts"])
        readings.append(payload)
    return readings


def reanchor(readings: list[dict], now: datetime) -> list[dict]:
    if not readings:
        return readings
    first = parse_ts(readings[0]["ts"])
    out = []
    for reading in readings:
        offset = parse_ts(reading["ts"]) - first
        offset = max(offset, timedelta(0))
        out.append({**reading, "ts": format_ts(now + offset)})
    return out


def publish(mqttx: str, host: str, port: int, topic: str, qos: int, payload: dict) -> None:
    body = json.dumps(payload, separators=(",", ":"))
    result = subprocess.run(
        [mqttx, "pub", "-h", host, "-p", str(port), "-t", topic, "-q", str(qos), "-m", body],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        raise ReplayError(f"mqttx pub failed ({result.returncode}): {result.stderr.strip()}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--serial-log")
    parser.add_argument("--device-id", default=os.environ.get("E2E_DEVICE_ID", "local-device-001"))
    parser.add_argument("--host", default=os.environ.get("MQTT_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("MQTT_PORT", "1883")))
    parser.add_argument("--qos", type=int, choices=(0, 1, 2), default=1)
    parser.add_argument("--ts-mode", choices=("keep", "now"), default="keep")
    parser.add_argument("--delay", type=float, default=0.2, help="seconds between publishes")
    parser.add_argument("--mqttx", default=os.environ.get("MQTTX_BIN", "mqttx"))
    parser.add_argument("--dry-run", action="store_true", help="validate and print, do not publish")
    args = parser.parse_args(argv)

    try:
        source = resolve_source(args.serial_log)
        readings = extract_readings(source.read_text(encoding="utf-8", errors="replace"))
        if not readings:
            raise ReplayError(f"no '{MARKER}' lines in {source}")
        if args.ts_mode == "now":
            readings = reanchor(readings, datetime.now(UTC))
        topic = TOPIC_TEMPLATE.format(device_id=args.device_id)
        if not args.dry_run and shutil.which(args.mqttx) is None:
            raise ReplayError(f"{args.mqttx} not found on PATH")
        print(f"source={source} topic={topic} count={len(readings)}", file=sys.stderr)
        for index, reading in enumerate(readings):
            if args.dry_run:
                print(json.dumps(reading, separators=(",", ":")))
            else:
                publish(args.mqttx, args.host, args.port, topic, args.qos, reading)
                print(f"published seq={reading['seq']} ts={reading['ts']}", file=sys.stderr)
                if args.delay and index < len(readings) - 1:
                    time.sleep(args.delay)
    except ReplayError as exc:
        print(f"replay: {exc}", file=sys.stderr)
        return 2
    print(f"REPLAYED {len(readings)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
