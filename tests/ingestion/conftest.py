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
# Fixtures that talk to ElasticMQ or DynamoDB Local directly; their tests wait for readiness.
_SERVICE_FIXTURES = frozenset({"sqs", "dynamodb"})


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


@pytest.fixture(autouse=True)
def _ingestion_services_ready(request: pytest.FixtureRequest) -> Iterator[None]:
    if _SERVICE_FIXTURES.intersection(request.fixturenames):
        ensure_services_ready()
    yield
