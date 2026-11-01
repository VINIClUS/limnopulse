"""ADR-020 S2: worker-monitor's Go tests read roles.yaml rendered as JSON, as the bundle ships it."""

from __future__ import annotations

import json

from tests.support.worker_roles import ROOT, load_roles

GO_TEST_TABLE = ROOT / "internal/workermonitor/testdata/roles.json"
REGENERATE = (
    ".venv/bin/python -c 'import json, yaml; print(json.dumps(yaml.safe_load("
    'open("ops/workers/roles.yaml", encoding="utf-8")), indent=2, ensure_ascii=False))\''
    " > internal/workermonitor/testdata/roles.json"
)


def test_worker_monitor_test_table_is_the_rendered_role_table():
    rendered = json.loads(GO_TEST_TABLE.read_text(encoding="utf-8"))
    assert rendered == load_roles(), f"regenerate the Go test table: {REGENERATE}"
