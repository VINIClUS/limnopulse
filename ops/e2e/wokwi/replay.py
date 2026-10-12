#!/usr/bin/env python3
"""Replay ``READING <json>`` lines from a Wokwi serial log onto the local broker.

Source selection (first match wins):
  1. ``--serial-log PATH``
  2. ``$WOKWI_SERIAL_LOG``
  3. ``ops/e2e/wokwi/serial.log`` (written by ``wokwi-cli --serial-log-file``)
  4. ``ops/e2e/wokwi/fixtures/serial.log`` (deterministic, mirrors the sketch)

A serial log given by ``--serial-log`` or ``$WOKWI_SERIAL_LOG`` must resolve to
a regular file of at most 4 MiB inside the current working directory (run
from the repository root).

Each payload must carry exactly the legacy reading keys. It is published with
``mqttx pub`` (resolved on ``PATH``) to
``limnopulse/v1/devices/<device_id>/readings``; the host, port, device id and
payload are validated before they reach the command line.

``--ts-mode now`` re-anchors timestamps so the first reading is "now" while
keeping the original spacing; deployments created by the M1 migration start at
the device ``created_at``, so fixture timestamps from the past would otherwise
fall outside every deployment interval.
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
import re
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
MAX_SERIAL_LOG_BYTES = 4 * 1024 * 1024
# Legacy device ids as accepted by the topic (and by the /v1 API).
DEVICE_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
HOST_LABEL_PATTERN = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?")
MQTTX_COMMAND = "mqttx"


class ReplayError(Exception):
    pass


def contained_file(value: str, max_bytes: int) -> Path:
    """`value` resolved (symlinks included) to a regular file inside the working directory."""
    base = Path.cwd().resolve()
    try:
        resolved = (base / value).resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise ReplayError(f"serial log not found: {value} ({type(exc).__name__})") from exc
    if not resolved.is_relative_to(base):
        raise ReplayError(f"serial log must be inside the working directory {base}: {value}")
    if not resolved.is_file():
        raise ReplayError(f"serial log is not a regular file: {value}")
    if resolved.stat().st_size > max_bytes:
        raise ReplayError(f"serial log is larger than {max_bytes} bytes: {value}")
    return resolved


def resolve_source(explicit: str | None) -> Path:
    for candidate in (explicit, os.environ.get("WOKWI_SERIAL_LOG")):
        if candidate:
            return contained_file(candidate, MAX_SERIAL_LOG_BYTES)
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


def valid_host(value: str) -> str:
    """An IP address or a DNS hostname; anything else never reaches the command line."""
    try:
        return str(ipaddress.ip_address(value))
    except ValueError:
        pass
    labels = value.split(".")
    if len(value) > 253 or not all(HOST_LABEL_PATTERN.fullmatch(label) for label in labels):
        raise ReplayError(f"invalid MQTT host: {value!r}")
    return ".".join(labels)


def valid_port(value: object) -> int:
    try:
        port = int(str(value), 10)
    except ValueError as exc:
        raise ReplayError(f"invalid MQTT port: {value!r}") from exc
    if not 1 <= port <= 65535:
        raise ReplayError(f"MQTT port out of range: {port}")
    return port


def valid_device_id(value: str) -> str:
    match = DEVICE_ID_PATTERN.fullmatch(value or "")
    if match is None:
        raise ReplayError(f"invalid device id: {value!r}")
    return match.group(0)


def valid_qos(value: object) -> int:
    if value not in (0, 1, 2):
        raise ReplayError(f"invalid QoS: {value!r}")
    return int(str(value))


def reading_message(payload: dict) -> str:
    """Re-serialized JSON object with exactly the legacy reading keys and scalar values."""
    reading = json.loads(json.dumps(payload))
    if not isinstance(reading, dict) or set(reading) != EXPECTED_KEYS:
        raise ReplayError("reading payload must carry exactly the legacy reading keys")
    for key, value in reading.items():
        if isinstance(value, bool) or not isinstance(value, (int, float, str)):
            raise ReplayError(f"reading value for {key!r} must be a number or a string")
    return json.dumps(reading, separators=(",", ":"))


def mqttx_executable() -> str:
    """Absolute path of `mqttx` on PATH (a fixed command name, never user input)."""
    found = shutil.which(MQTTX_COMMAND)
    if found is None:
        raise ReplayError(f"{MQTTX_COMMAND} not found on PATH")
    return str(Path(found).resolve())


def publish_argv(host: str, port: object, device_id: str, qos: object, payload: dict) -> list[str]:
    """`mqttx pub` argv built only from validated values; no shell is involved."""
    topic = TOPIC_TEMPLATE.format(device_id=valid_device_id(device_id))
    return [
        mqttx_executable(),
        "pub",
        "--hostname",
        valid_host(host),
        "--port",
        str(valid_port(port)),
        "--topic",
        topic,
        "--qos",
        str(valid_qos(qos)),
        "--message",
        reading_message(payload),
    ]


def publish(host: str, port: object, device_id: str, qos: object, payload: dict) -> None:
    result = subprocess.run(
        publish_argv(host, port, device_id, qos, payload),
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
    parser.add_argument("--dry-run", action="store_true", help="validate and print, do not publish")
    args = parser.parse_args(argv)

    try:
        source = resolve_source(args.serial_log)
        readings = extract_readings(source.read_text(encoding="utf-8", errors="replace"))
        if not readings:
            raise ReplayError(f"no '{MARKER}' lines in {source}")
        if args.ts_mode == "now":
            readings = reanchor(readings, datetime.now(UTC))
        topic = TOPIC_TEMPLATE.format(device_id=valid_device_id(args.device_id))
        if not args.dry_run:
            mqttx_executable()
        print(f"source={source} topic={topic} count={len(readings)}", file=sys.stderr)
        for index, reading in enumerate(readings):
            if args.dry_run:
                print(json.dumps(reading, separators=(",", ":")))
            else:
                publish(args.host, args.port, args.device_id, args.qos, reading)
                print(f"published seq={reading['seq']} ts={reading['ts']}", file=sys.stderr)
                print(f"PUBLISHED {args.device_id} {reading['seq']} {reading['ts']}", flush=True)
                if args.delay and index < len(readings) - 1:
                    time.sleep(args.delay)
    except ReplayError as exc:
        print(f"replay: {exc}", file=sys.stderr)
        return 2
    print(f"REPLAYED {len(readings)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
