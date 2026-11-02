"""ADR-020 S2: worker-monitor's Go tests read the part of roles.yaml the monitor decodes.

`internal/workermonitor/testdata/roles.json` holds only the fields `workermonitor.Table`
decodes, so an edit to any other field of roles.yaml (credentials, queues, alarms, rollback,
...) needs no change here. Regenerate the file after editing one of these fields with:

    .venv/bin/python -m tests.unit.test_worker_monitor_table
"""

from __future__ import annotations

import json
import re
from typing import Any

from tests.support.worker_roles import ROOT, load_roles

GO_TEST_TABLE = ROOT / "internal/workermonitor/testdata/roles.json"
GO_TABLE_SOURCE = ROOT / "internal/workermonitor/table.go"
TOP_LEVEL_FIELDS = ("schema_version", "unit_health", "roles")
UNIT_HEALTH_FIELDS = (
    "namespace",
    "metric",
    "dimension",
    "oneshot_stale_after_intervals",
    "oneshot_success_exit_codes",
    "published_when",
)
ROLE_FIELDS = ("name", "lifecycle", "schedule_seconds", "units", "unit_health")
UNITS_FIELDS = ("service", "timer")


def monitor_projection(table: dict[str, Any]) -> dict[str, Any]:
    """Return the fields of the role table that worker-monitor decodes."""
    return {
        "schema_version": table["schema_version"],
        "unit_health": {key: table["unit_health"][key] for key in UNIT_HEALTH_FIELDS},
        "roles": [
            {key: role[key] for key in ROLE_FIELDS if key in role} for role in table["roles"]
        ],
    }


def render() -> str:
    return json.dumps(monitor_projection(load_roles()), indent=2, ensure_ascii=False) + "\n"


def test_go_test_table_is_the_monitor_projection_of_roles_yaml():
    rendered = json.loads(GO_TEST_TABLE.read_text(encoding="utf-8"))
    assert rendered == monitor_projection(load_roles()), (
        "regenerate it: .venv/bin/python -m tests.unit.test_worker_monitor_table"
    )


def test_projection_covers_exactly_the_fields_the_go_table_decodes():
    tags = set(re.findall(r'`json:"([a-z_]+)"`', GO_TABLE_SOURCE.read_text(encoding="utf-8")))
    projected = {*TOP_LEVEL_FIELDS, *UNIT_HEALTH_FIELDS, *ROLE_FIELDS, *UNITS_FIELDS}
    assert tags == projected


if __name__ == "__main__":
    GO_TEST_TABLE.write_text(render(), encoding="utf-8")
