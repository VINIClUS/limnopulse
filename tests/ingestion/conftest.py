from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.support.influx_harness import influx_bucket  # noqa: F401
from tests.support.ingestion_harness import (  # noqa: F401
    RUN_ENV,
    ensure_services_ready,
    ingestion_queue_pair,
    ingestion_tables,
)

_HERE = Path(__file__).resolve().parent
# Tests using these need neither ElasticMQ nor DynamoDB Local, so they skip the readiness wait.
_NON_SERVICE_FIXTURES = frozenset({"influx_bucket", "echo_server"})


def _in_this_directory(item: pytest.Item) -> bool:
    return _HERE in Path(item.path).resolve().parents


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    # This hook sees every collected item, so only mark the ones in this directory.
    if os.environ.get(RUN_ENV) == "1":
        return
    skip = pytest.mark.skip(reason=f"set {RUN_ENV}=1 to run tests/ingestion")
    for item in items:
        if _in_this_directory(item):
            item.add_marker(skip)


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[None]):
    report = yield
    # Under the gate, a skip (for example influx_bucket without RUN_INFLUX_INTEGRATION=1) would
    # silently drop an Influx-backed test, so it fails the run instead.
    if (
        report.skipped
        and os.environ.get(RUN_ENV) == "1"
        and _in_this_directory(item)
        and not hasattr(report, "wasxfail")
    ):
        reason = report.longrepr[2] if isinstance(report.longrepr, tuple) else report.longrepr
        report.outcome = "failed"
        report.longrepr = f"tests/ingestion must not skip under {RUN_ENV}=1: {reason}"
    return report


@pytest.hookimpl(wrapper=True)
def pytest_make_collect_report(collector: pytest.Collector):
    report = yield
    # A module-level skip under the gate is just as silent, so it fails too.
    if (
        report.skipped
        and os.environ.get(RUN_ENV) == "1"
        and _HERE in Path(collector.path).resolve().parents
    ):
        report.outcome = "failed"
        report.longrepr = f"tests/ingestion must not skip under {RUN_ENV}=1: {report.longrepr}"
    return report


@pytest.fixture(autouse=True)
def _ingestion_services_ready(request: pytest.FixtureRequest) -> Iterator[None]:
    if os.environ.get(RUN_ENV) == "1" and not _NON_SERVICE_FIXTURES.intersection(
        request.fixturenames
    ):
        ensure_services_ready()
    yield
