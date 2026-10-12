#!/usr/bin/env python3
"""Summarize Node-RED verdicts captured by ``mqttx sub --output-mode clean``.

Accepts either the clean-mode stream (concatenated JSON packet objects whose
``payload`` is the verdict JSON string) or plain JSON lines of verdicts.

Exit codes: 0 pass, 1 a verdict failed (ok=false, or ok=null without
--allow-v2-unavailable) or --scenarios-failed > 0, 3 fewer verdicts than --expect.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

VERDICT_KEYS = (
    "device_id",
    "seq",
    "ts",
    "v1_pond_id",
    "resolved_asset_id",
    "expected_asset_id",
    "ok",
    "reason",
)


def _as_verdict(obj: object) -> dict | None:
    if isinstance(obj, dict) and isinstance(obj.get("payload"), str):
        try:
            obj = json.loads(obj["payload"])
        except json.JSONDecodeError:
            return None
    if isinstance(obj, dict) and "ok" in obj and "reason" in obj:
        return obj
    return None


def parse_stream(text: str) -> list[dict]:
    decoder = json.JSONDecoder()
    verdicts: list[dict] = []
    index = 0
    length = len(text)
    while index < length:
        brace = text.find("{", index)
        if brace < 0:
            break
        try:
            obj, end = decoder.raw_decode(text, brace)
        except json.JSONDecodeError:
            index = brace + 1
            continue
        verdict = _as_verdict(obj)
        if verdict is not None:
            verdicts.append(verdict)
        index = end
    return verdicts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", required=True, type=Path)
    parser.add_argument(
        "--device-id", action="append", default=[], help="repeatable; default: every device"
    )
    parser.add_argument("--expect", type=int, default=0)
    parser.add_argument("--allow-v2-unavailable", action="store_true")
    parser.add_argument(
        "--scenarios-failed",
        type=int,
        default=0,
        help="failed API-driven scenarios; each one counts as a failure",
    )
    parser.add_argument("--count-only", action="store_true")
    parser.add_argument("--jsonl-out", type=Path)
    args = parser.parse_args(argv)

    text = args.raw.read_text(encoding="utf-8", errors="replace") if args.raw.exists() else ""
    verdicts = parse_stream(text)
    if args.device_id:
        verdicts = [v for v in verdicts if v.get("device_id") in args.device_id]

    if args.count_only:
        print(len(verdicts))
        return 0

    if args.jsonl_out:
        args.jsonl_out.write_text(
            "".join(json.dumps(v, sort_keys=True) + "\n" for v in verdicts), encoding="utf-8"
        )

    print(f"{'seq':>5}  {'ok':<5}  {'reason':<26}  device / ts / v1_pond_id / resolved / expected")
    failures = 0
    unavailable = 0
    for v in verdicts:
        ok = v.get("ok")
        if ok is None:
            unavailable += 1
            if not args.allow_v2_unavailable:
                failures += 1
        elif ok is not True:
            failures += 1
        print(
            f"{v.get('seq')!s:>5}  {json.dumps(ok):<5}  {v.get('reason')!s:<26}  "
            f"{v.get('device_id')} / {v.get('ts')} / {v.get('v1_pond_id')} / {v.get('resolved_asset_id')} / "
            f"{v.get('expected_asset_id')}"
        )
        missing = [key for key in VERDICT_KEYS if key not in v]
        if missing:
            print(f"       verdict missing keys: {missing}")
            failures += 1

    failures += max(args.scenarios_failed, 0)
    passed = sum(1 for v in verdicts if v.get("ok") is True)
    ok_false = sum(1 for v in verdicts if v.get("ok") is False)
    reasons = Counter(f"{json.dumps(v.get('ok'))}:{v.get('reason')}" for v in verdicts)
    print("REASONS " + " ".join(f"{key}={count}" for key, count in sorted(reasons.items())))
    print(
        f"SUMMARY verdicts={len(verdicts)} expected={args.expect} ok_true={passed} "
        f"ok_false={ok_false} ok_null={unavailable} failures={failures} "
        f"allow_v2_unavailable={int(args.allow_v2_unavailable)} "
        f"scenarios_failed={args.scenarios_failed}"
    )
    if failures:
        return 1
    if len(verdicts) < args.expect:
        print("FAIL: missing verdicts", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
