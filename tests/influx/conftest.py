from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.support.influx_harness import influx_bucket  # noqa: F401

_HERE = Path(__file__).resolve().parent


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    # This hook sees every collected item, so only mark the ones in this directory.
    if os.environ.get("RUN_INFLUX_INTEGRATION") == "1":
        return
    skip = pytest.mark.skip(reason="set RUN_INFLUX_INTEGRATION=1 to run tests/influx")
    for item in items:
        if _HERE in Path(item.path).resolve().parents:
            item.add_marker(skip)
