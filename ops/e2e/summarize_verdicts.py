#!/usr/bin/env python3
"""Summarize Node-RED verdicts captured by ``mqttx sub --output-mode clean``.

Accepts either the clean-mode stream (concatenated JSON packet objects whose
``payload`` is the verdict JSON string) or plain JSON lines of verdicts.

Verdicts are counted once per published reading, identified by
(device_id, seq, ts): QoS-1 redelivery may duplicate a verdict, which is
tolerated only when the duplicates agree. With ``--expect-reading`` every
listed reading needs a verdict and no other reading may have one.

``--raw`` and ``--jsonl-out`` must stay inside the working directory (run from
the repository root).

Exit codes: 0 pass, 1 a verdict failed (ok=false, ok=null without
--allow-v2-unavailable, conflicting duplicates, an unexpected reading) or
--scenarios-failed > 0, 2 bad arguments, 3 missing verdicts.
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


MAX_RAW_BYTES = 16 * 1024 * 1024
# A verdict "agrees" with a duplicate (QoS-1 redelivery) when these match.
AGREEMENT_KEYS = ("ok", "reason", "resolved_asset_id", "expected_asset_id", "v1_pond_id")


class SummaryError(Exception):
    pass


def contained_path(value: str) -> Path:
    """`value` resolved (symlinks included) inside the working directory; run from the repo root."""
    base = Path.cwd().resolve()
    resolved = (base / value).resolve()
    if not resolved.is_relative_to(base):
        raise SummaryError(f"path must be inside the working directory {base}: {value}")
    if resolved.exists() and not resolved.is_file():
        raise SummaryError(f"not a regular file: {value}")
    return resolved


def read_raw(value: str) -> str:
    path = contained_path(value)
    if not path.exists():
        return ""
    if path.stat().st_size > MAX_RAW_BYTES:
        raise SummaryError(f"{value} is larger than {MAX_RAW_BYTES} bytes")
    return path.read_text(encoding="utf-8", errors="replace")


def identity(verdict: dict) -> tuple[str, str, str]:
    return (str(verdict.get("device_id")), str(verdict.get("seq")), str(verdict.get("ts")))


def parse_expected(values: list[str]) -> list[tuple[str, str, str]]:
    expected = []
    for value in values:
        parts = value.split(",")
        if len(parts) != 3 or not all(parts):
            raise SummaryError(f"--expect-reading must be DEVICE,SEQ,TS: {value!r}")
        expected.append((parts[0], parts[1], parts[2]))
    return list(dict.fromkeys(expected))


def dedupe(verdicts: list[dict]) -> tuple[dict[tuple[str, str, str], dict], list[str]]:
    """One verdict per published reading; duplicates must agree on the outcome."""
    unique: dict[tuple[str, str, str], dict] = {}
    conflicts = []
    for verdict in verdicts:
        key = identity(verdict)
        first = unique.setdefault(key, verdict)
        if first is not verdict and any(
            first.get(name) != verdict.get(name) for name in AGREEMENT_KEYS
        ):
            conflicts.append(f"conflicting duplicate verdicts for {key}")
    return unique, list(dict.fromkeys(conflicts))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", required=True, help="capture file inside the working directory")
    parser.add_argument(
        "--device-id", action="append", default=[], help="repeatable; default: every device"
    )
    parser.add_argument(
        "--expect-reading",
        action="append",
        default=[],
        help="DEVICE,SEQ,TS of a published reading; repeatable. Each needs exactly one verdict"
        " (agreeing duplicates are tolerated); verdicts for other readings fail.",
    )
    parser.add_argument(
        "--expect", type=int, default=0, help="minimum number of distinct readings with a verdict"
    )
    parser.add_argument("--allow-v2-unavailable", action="store_true")
    parser.add_argument(
        "--scenarios-failed",
        type=int,
        default=0,
        help="failed API-driven scenarios; each one counts as a failure",
    )
    parser.add_argument("--count-only", action="store_true")
    parser.add_argument("--jsonl-out", help="output file inside the working directory")
    args = parser.parse_args(argv)

    try:
        expected = parse_expected(args.expect_reading)
        verdicts = parse_stream(read_raw(args.raw))
        jsonl_out = contained_path(args.jsonl_out) if args.jsonl_out else None
    except SummaryError as exc:
        print(f"summarize: {exc}", file=sys.stderr)
        return 2
    if args.device_id:
        verdicts = [v for v in verdicts if v.get("device_id") in args.device_id]
    unique, conflicts = dedupe(verdicts)

    if args.count_only:
        covered = [key for key in expected if key in unique] if expected else list(unique)
        print(len(covered))
        return 0

    if jsonl_out is not None:
        jsonl_out.write_text(
            "".join(json.dumps(v, sort_keys=True) + "\n" for v in verdicts), encoding="utf-8"
        )

    print(f"{'seq':>5}  {'ok':<5}  {'reason':<26}  device / ts / v1_pond_id / resolved / expected")
    failures = 0
    unavailable = 0
    for v in unique.values():
        ok = v.get("ok")
        if ok is None:
            unavailable += 1
            if not args.allow_v2_unavailable:
                failures += 1
        elif ok is not True:
            failures += 1
        print(
            f"{v.get('seq')!s:>5}  {json.dumps(ok):<5}  {v.get('reason')!s:<26}  "
            f"{v.get('device_id')} / {v.get('ts')} / {v.get('v1_pond_id')} / "
            f"{v.get('resolved_asset_id')} / {v.get('expected_asset_id')}"
        )
        missing_keys = [key for key in VERDICT_KEYS if key not in v]
        if missing_keys:
            print(f"       verdict missing keys: {missing_keys}")
            failures += 1

    problems = list(conflicts)
    missing = [key for key in expected if key not in unique]
    if expected:
        problems += [f"unexpected verdict for {key}" for key in unique if key not in expected]
    for problem in problems:
        print(f"FAIL: {problem}")
    failures += len(problems)
    for key in missing:
        print(f"FAIL: no verdict for {key}")

    failures += max(args.scenarios_failed, 0)
    values = list(unique.values())
    passed = sum(1 for v in values if v.get("ok") is True)
    ok_false = sum(1 for v in values if v.get("ok") is False)
    reasons = Counter(f"{json.dumps(v.get('ok'))}:{v.get('reason')}" for v in values)
    print("REASONS " + " ".join(f"{key}={count}" for key, count in sorted(reasons.items())))
    print(
        f"SUMMARY verdicts={len(values)} duplicates={len(verdicts) - len(values)} "
        f"expected={max(len(expected), args.expect)} missing={len(missing)} ok_true={passed} "
        f"ok_false={ok_false} ok_null={unavailable} failures={failures} "
        f"allow_v2_unavailable={int(args.allow_v2_unavailable)} "
        f"scenarios_failed={args.scenarios_failed}"
    )
    if failures:
        return 1
    if missing or len(values) < args.expect:
        print("FAIL: missing verdicts", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
