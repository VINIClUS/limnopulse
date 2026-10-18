"""Real-InfluxDB integration harness for ``tests/influx`` (P2-01).

Importing this module never skips or connects. The ``influx_bucket`` fixture
skips unless ``RUN_INFLUX_INTEGRATION=1``; otherwise it creates a uniquely
named bucket with no retention rule and deletes it at teardown.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from typing import NamedTuple

import pytest
from influxdb_client import InfluxDBClient
from influxdb_client.client.query_api import QueryApi
from influxdb_client.client.write_api import SYNCHRONOUS, WriteApi

RUN_ENV = "RUN_INFLUX_INTEGRATION"


class InfluxSettings(NamedTuple):
    url: str
    token: str
    org: str


def influx_settings() -> InfluxSettings:
    """Resolve the connection settings; an empty variable counts as unset."""
    return InfluxSettings(
        url=os.environ.get("INFLUX_INTEGRATION_URL") or "http://127.0.0.1:8086",
        token=os.environ.get("INFLUX_INTEGRATION_TOKEN") or "local-dev-token",
        org=os.environ.get("INFLUX_INTEGRATION_ORG") or "limnopulse",
    )


def _client() -> InfluxDBClient:
    settings = influx_settings()
    return InfluxDBClient(url=settings.url, token=settings.token, org=settings.org)


@pytest.fixture
def influx_bucket() -> Iterator[str]:
    """Yield the name of a fresh bucket that is deleted at teardown."""
    if os.environ.get(RUN_ENV) != "1":
        pytest.skip(f"set {RUN_ENV}=1 to run against a real InfluxDB")
    name = f"it_{uuid.uuid4().hex}"
    client = _client()
    try:
        bucket = client.buckets_api().create_bucket(bucket_name=name)
        try:
            yield name
        finally:
            client.buckets_api().delete_bucket(bucket)
    finally:
        client.close()


@contextmanager
def influx_clients(bucket: str) -> Iterator[tuple[QueryApi, WriteApi]]:
    """Yield a ``(query_api, write_api)`` pair; writes are synchronous."""
    client = _client()
    try:
        with client.write_api(write_options=SYNCHRONOUS) as write_api:
            yield client.query_api(), write_api
    finally:
        client.close()


def write_lines(bucket: str, lines: Iterable[str]) -> None:
    """Write line-protocol ``lines`` to ``bucket`` and wait for the server."""
    with influx_clients(bucket) as (_query_api, write_api):
        write_api.write(bucket=bucket, record=list(lines))
