#!/usr/bin/env python3
"""Fail unless a migration summary (JSON on stdin) describes a no-op re-apply.

The M1 backfill's second ``--apply`` must succeed without writing anything:
every ``*_created``, ``devices_projected`` and ``legacy_links_updated``
counter is zero, there are no conflicts and the scope completed. Exit 0 when
that holds, 1 otherwise (each problem is printed on stderr).
"""

from __future__ import annotations

import json
import sys

WRITE_COUNTERS = (
    "default_sites_created",
    "assets_created",
    "devices_projected",
    "components_created",
    "deployments_created",
    "legacy_links_updated",
)


def problems(summary: object) -> list[str]:
    if not isinstance(summary, dict):
        return ["summary is not a JSON object"]
    found = []
    if summary.get("apply") is not True:
        found.append(f"apply={summary.get('apply')!r} (expected true)")
    if summary.get("result") != "success" or summary.get("scope_completed") is not True:
        found.append(
            f"result={summary.get('result')!r} scope_completed={summary.get('scope_completed')!r}"
        )
    if summary.get("schema_conflicts") != 0 or summary.get("conflicts"):
        found.append(f"conflicts: {summary.get('conflicts')!r}")
    for key in WRITE_COUNTERS:
        value = summary.get(key)
        if value != 0 or isinstance(value, bool):
            found.append(f"{key}={value!r} (expected 0)")
    return found


def main() -> int:
    try:
        summary = json.loads(sys.stdin.read())
    except json.JSONDecodeError as exc:
        print(f"assert_migration_noop: invalid JSON: {exc}", file=sys.stderr)
        return 1
    found = problems(summary)
    for problem in found:
        print(f"assert_migration_noop: {problem}", file=sys.stderr)
    if found:
        return 1
    print("assert_migration_noop: second apply wrote nothing", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
