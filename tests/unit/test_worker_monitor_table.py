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


def role_projection(role: dict[str, Any]) -> dict[str, Any]:
    projected = {key: role[key] for key in ROLE_FIELDS if key in role}
    if "units" in projected:
        units = role["units"]
        projected["units"] = {key: units[key] for key in UNITS_FIELDS if key in units}
    return projected


def monitor_projection(table: dict[str, Any]) -> dict[str, Any]:
    """Return the fields of the role table that worker-monitor decodes."""
    return {
        "schema_version": table["schema_version"],
        "unit_health": {key: table["unit_health"][key] for key in UNIT_HEALTH_FIELDS},
        "roles": [role_projection(role) for role in table["roles"]],
    }


def render() -> str:
    return json.dumps(monitor_projection(load_roles()), indent=2, ensure_ascii=False) + "\n"


def test_go_test_table_is_the_monitor_projection_of_roles_yaml():
    rendered = json.loads(GO_TEST_TABLE.read_text(encoding="utf-8"))
    assert rendered == monitor_projection(load_roles()), (
        "regenerate it: .venv/bin/python -m tests.unit.test_worker_monitor_table"
    )


GO_STRUCT = re.compile(r"^type (\w+) struct \{\n(.*?)^\}", re.MULTILINE | re.DOTALL)
GO_FIELD = re.compile(r"^\t(\w+)\s+[^\s`]+(?:\s+`([^`]*)`)?", re.MULTILINE)
GO_JSON_KEY = re.compile(r'json:"([^",]*)')


def go_json_keys(source: str) -> dict[str, set[str]]:
    """Map each Go struct in source to the JSON keys encoding/json decodes into it.

    A tag's key is the part before any option (json:"name,omitempty" decodes "name"). A
    field without a json key decodes under its Go field name.
    """
    keys: dict[str, set[str]] = {}
    for struct, body in GO_STRUCT.findall(source):
        keys[struct] = set()
        for field, tag in GO_FIELD.findall(body):
            match = GO_JSON_KEY.search(tag)
            keys[struct].add(match.group(1) if match and match.group(1) else field)
    return keys


def test_go_json_keys_reads_options_any_case_and_untagged_fields():
    source = (
        "type Sample struct {\n"
        "\t// A comment line is not a field.\n"
        '\tA int `json:"extra,omitempty"`\n'
        '\tB int `json:"Extra2"`\n'
        '\tC int `yaml:"c" json:"plain"`\n'
        '\tD int `json:",string"`\n'
        "\tE int\n"
        "}\n"
        "\n"
        "type Other struct {\n"
        '\tF []string `json:"f"`\n'
        "}\n"
    )
    assert go_json_keys(source) == {
        "Sample": {"extra", "Extra2", "plain", "D", "E"},
        "Other": {"f"},
    }


def test_projection_covers_exactly_the_fields_the_go_table_decodes():
    keys = go_json_keys(GO_TABLE_SOURCE.read_text(encoding="utf-8"))
    assert keys == {
        "Table": set(TOP_LEVEL_FIELDS),
        "UnitHealth": set(UNIT_HEALTH_FIELDS),
        "Role": set(ROLE_FIELDS),
        "Units": set(UNITS_FIELDS),
    }


if __name__ == "__main__":
    GO_TEST_TABLE.write_text(render(), encoding="utf-8")
