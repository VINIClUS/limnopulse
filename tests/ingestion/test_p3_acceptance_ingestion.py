"""P3-11 acceptance gate on real services: ElasticMQ 1.6.16, DynamoDB Local 3.3.2, InfluxDB 2.7.11.

Every test drives the merged production composition, never a hand-built stack:

- the API is `create_app(settings)` with `INGEST_V2_ENABLED=true`, its lifespan run in-process and
  requests sent over ASGI, so the route, authenticator, trusted mapping, receipts and SQS producer
  are the `_wire_ingestion` objects over DynamoDB Local and ElasticMQ;
- the worker is `build_worker(settings)` (the P3-10 entrypoint composition) over the same tables,
  queue and an InfluxDB bucket. The one test seam is the processor's permanent visibility (60 s in
  production), shortened to 1 s so a permanent failure reaches the DLQ within the test budget;
- the operator CLIs are `scripts/admin/telemetry_dlq.py` and `telemetry_quarantine.py` `main`.

Gates that need no service (G11, G13, G14, the closed metric sets of G12 and the documents) live
in `tests/contracts/test_p3_acceptance_gate.py`.

Skipped unless `RUN_INGESTION_INTEGRATION=1` and `RUN_INFLUX_INTEGRATION=1`
(`make verify-ingestion`, with the three `scripts/dev/run_local_*.sh` runners started).
"""

from __future__ import annotations

import asyncio
import functools
import io
import json
import logging
import random
import signal
import threading
from collections.abc import AsyncIterator, Callable, Iterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from urllib.parse import urlparse

import httpx
import pytest
from botocore.exceptions import ClientError
from fastapi import FastAPI
from influxdb_client import InfluxDBClient

from limnopulse_api.adapters.dynamodb import DynamoDomainRepository
from limnopulse_api.adapters.dynamodb_deployments import deployment_to_item
from limnopulse_api.adapters.dynamodb_ingest_credentials import DynamoIngestCredentialRepository
from limnopulse_api.adapters.dynamodb_ingest_state import DynamoIngestStateRepository
from limnopulse_api.adapters.dynamodb_ingress_receipts import DynamoIngressReceiptRepository
from limnopulse_api.adapters.dynamodb_quarantine import DynamoQuarantineRepository
from limnopulse_api.adapters.dynamodb_v2 import (
    account_scoped_identity_hash,
    device_ingest_state_from_item,
    idempotency_key_hash,
)
from limnopulse_api.adapters.sqs_ingestion import SqsIngressQueue
from limnopulse_api.adapters.telemetry_metrics_log import METRICS_LOGGER
from limnopulse_api.core.config import Settings
from limnopulse_api.domain.deployments import Deployment
from limnopulse_api.domain.devices_v2 import DeviceStatus
from limnopulse_api.domain.entities import Membership
from limnopulse_api.domain.ingestion import (
    INGEST_METRICS,
    IngressEnvelope,
    IngressReceiptState,
    QuarantineEntryKind,
    QuarantineEntryStatus,
    ingest_secret_sha256,
    ingest_token,
)
from limnopulse_api.domain.integrations import DeviceIntegrationStatus, IntegrationAccountStatus
from limnopulse_api.domain.roles import TenantRole
from limnopulse_api.domain.telemetry_v2 import SourceObservation, observation_event_id
from limnopulse_api.domain.telemetry_v2_schema import LEGACY_MEASUREMENT, OBSERVATION_MEASUREMENT
from limnopulse_api.main import create_app
from limnopulse_api.migrations.default_site_asset import DefaultSiteAssetMigrator
from limnopulse_api.services.ingest_processor import RetryPolicy
from limnopulse_api.workers import telemetry_ingest
from limnopulse_api.workers.telemetry_ingest import build_worker
from scripts.admin import telemetry_dlq, telemetry_quarantine
from scripts.dev import seed_ingest_local
from tests.influx.test_p2_acceptance_influx import (
    COMPONENT as LEGACY_COMPONENT,
)
from tests.influx.test_p2_acceptance_influx import (
    DEVICE as LEGACY_DEVICE,
)
from tests.influx.test_p2_acceptance_influx import (
    POND as LEGACY_POND,
)
from tests.influx.test_p2_acceptance_influx import (
    TENANT as LEGACY_TENANT,
)
from tests.influx.test_p2_acceptance_influx import (
    run_divergence_check,
)
from tests.support.influx_harness import influx_clients, influx_settings, write_lines
from tests.support.ingestion_contract_kits import (
    ACCOUNT,
    AUDIT,
    COMPONENT,
    CREATED,
    CREDENTIAL,
    DEVICE,
    EXTERNAL_DEVICE,
    INTEGRATION,
    OTHER_ACCOUNT,
    OTHER_COMPONENT,
    OTHER_CREDENTIAL,
    OTHER_DEVICE,
    OTHER_INTEGRATION,
    OTHER_TENANT,
    PROVIDER,
    SECRET,
    TENANT,
    component,
    credential,
    device,
    device_integration,
    integration_account,
)
from tests.support.ingestion_harness import (
    IngestionQueuePair,
    IngestionTables,
    ToggleableTcpProxy,
    dynamodb_client,
    sqs_client,
)
from tests.unit.test_dynamodb_integration_resolver import DynamoIngestionSubject

pytestmark = pytest.mark.asyncio

QueueFactory = Callable[..., IngestionQueuePair]
GUARD_SECONDS = 90
TIME_MIN = "1970-01-01T00:00:00Z"
TIME_MAX = "2262-01-01T00:00:00Z"
INGEST_PATH = "/ingest/v2/observations"
TOKEN = ingest_token(CREDENTIAL, SECRET)
OTHER_TOKEN = ingest_token(OTHER_CREDENTIAL, SECRET)
WRONG_SECRET = "Z" * 43
DEPLOYMENT = Deployment(
    tenant_id=TENANT,
    deployment_id="dep_" + "c" * 32,
    component_id=COMPONENT,
    site_id="site_" + "a" * 32,
    asset_id="ast_" + "b" * 32,
    started_at=CREATED,
    created_at=CREATED,
    updated_at=CREATED,
    version=1,
)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def ns(value: datetime) -> int:
    return (value - EPOCH) // timedelta(microseconds=1) * 1000


def iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


@pytest.fixture(autouse=True)
def _hard_deadline() -> Iterator[None]:
    """A SIGALRM bound that fires even if a loop never yields (the fakes never suspend)."""

    def expire(signum: int, frame: Any) -> None:
        raise TimeoutError("test exceeded its hard deadline")

    previous = signal.signal(signal.SIGALRM, expire)
    signal.setitimer(signal.ITIMER_REAL, 4 * GUARD_SECONDS)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


@pytest.fixture(autouse=True)
def _fast_permanent_visibility(monkeypatch: pytest.MonkeyPatch) -> None:
    """The only seam in `build_worker`: a 1 s permanent visibility instead of 60 s."""
    monkeypatch.setattr(
        telemetry_ingest, "RetryPolicy", functools.partial(RetryPolicy, permanent_visibility=1)
    )


# --- Metrics capture -----------------------------------------------------------------------------


class MetricLines(logging.Handler):
    """Collects the structured metric lines `StructuredLogTelemetryMetrics` writes."""

    def __init__(self) -> None:
        super().__init__(level=logging.INFO)
        self.records: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def emit(self, record: logging.LogRecord) -> None:
        with self._lock:
            self.records.append(json.loads(record.getMessage()))

    def total(self, metric: str, **labels: str) -> int:
        with self._lock:
            return sum(
                int(r["value"])
                for r in self.records
                if r["metric"] == metric and all(r["labels"].get(k) == v for k, v in labels.items())
            )

    def labels(self, metric: str) -> list[dict[str, str]]:
        with self._lock:
            return [dict(r["labels"]) for r in self.records if r["metric"] == metric]


@pytest.fixture
def metric_lines() -> Iterator[MetricLines]:
    logger = logging.getLogger(METRICS_LOGGER)
    handler = MetricLines()
    previous = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        yield handler
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous)


# --- Settings, API and worker --------------------------------------------------------------------


def gate_settings(
    tables: IngestionTables,
    queues: IngestionQueuePair,
    bucket: str,
    *,
    influx_url: str | None = None,
    legacy_dual_write: bool = False,
    **overrides: Any,
) -> Settings:
    influx = influx_settings()
    values: dict[str, Any] = {
        "app_env": "test",
        "auth_mode": "dev",
        "aws_region": "us-east-1",
        "dynamodb_endpoint_url": dynamodb_client().meta.endpoint_url,
        "dynamodb_domain_table": tables.domain,
        "dynamodb_audit_table": tables.audit,
        "sqs_endpoint_url": sqs_client().meta.endpoint_url,
        "sqs_telemetry_ingress_queue_url": queues.source_url,
        "sqs_telemetry_ingress_dlq_url": queues.dlq_url,
        "influxdb_url": influx_url or influx.url,
        "influxdb_token": influx.token,
        "influxdb_org": influx.org,
        "influxdb_bucket_raw": bucket,
        "api_v2_enabled": True,
        "telemetry_v2_write_enabled": True,
        "telemetry_legacy_dual_write_enabled": legacy_dual_write,
        "ingest_v2_enabled": True,
        "ingest_rate_limit_per_minute": 6_000,
        "ingest_rate_limit_burst": 1_000,
        "ingest_preauth_rate_limit_per_minute": 6_000,
        "ingest_preauth_global_per_minute": 60_000,
        "telemetry_ingest_worker_wait_seconds": 1,
        "telemetry_ingest_worker_visibility_seconds": 10,
        "telemetry_ingest_worker_concurrency": 4,
        "telemetry_ingest_retry_base_seconds": 1,
        "telemetry_ingest_retry_cap_seconds": 2,
        "telemetry_ingest_drain_seconds": 5,
        "telemetry_ingest_processing_deadline_seconds": 20,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


@asynccontextmanager
async def composed_api(settings: Settings) -> AsyncIterator[tuple[FastAPI, httpx.AsyncClient]]:
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="https://testserver") as http:
            yield app, http


def ingest_headers(token: str | None = TOKEN, key: str = "gate-key-0001") -> dict[str, str]:
    headers = {
        "content-type": "application/json",
        "idempotency-key": key,
        "x-connector-version": "fw-1.0",
    }
    if token is not None:
        headers["authorization"] = f"Bearer {token}"
    return headers


async def post_batch(
    http: httpx.AsyncClient,
    observations: Sequence[dict[str, Any]],
    *,
    key: str,
    token: str | None = TOKEN,
    external_device_id: str = EXTERNAL_DEVICE,
    headers: dict[str, str] | None = None,
    **extra: Any,
) -> httpx.Response:
    body = {"external_device_id": external_device_id, "observations": list(observations), **extra}
    return await http.post(
        INGEST_PATH, content=json.dumps(body), headers=headers or ingest_headers(token, key)
    )


def reading(at: datetime, value: float, **overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "component_id": COMPONENT,
        "metric_key": "water.temperature",
        "value": value,
        "source_unit": "degC",
        "observed_at": iso(at),
    }
    payload.update(overrides)
    return {key: item for key, item in payload.items() if item is not None}


def recent() -> datetime:
    return datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=10)


class Worker:
    """`build_worker(settings)` running in the test's event loop until stopped."""

    def __init__(self, settings: Settings, metrics: MetricLines) -> None:
        self.metrics = metrics
        self._loop = build_worker(settings)
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    def handled(self, **labels: str) -> int:
        return self.metrics.total("ingress_messages_total", **labels)

    async def wait_for(self, condition: Callable[[], bool], what: str) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + GUARD_SECONDS
        while not condition():
            assert self._task is not None and not self._task.done(), "the worker loop exited"
            assert loop.time() < deadline, f"timed out waiting for {what}"
            await asyncio.sleep(0.05)

    async def wait_handled(self, count: int, **labels: str) -> None:
        await self.wait_for(lambda: self.handled(**labels) >= count, f"{count} handled {labels}")


@asynccontextmanager
async def running_worker(settings: Settings, metrics: MetricLines) -> AsyncIterator[Worker]:
    worker = Worker(settings, metrics)
    worker._task = asyncio.create_task(worker._loop.run(worker._stop))
    try:
        yield worker
    finally:
        worker._stop.set()
        await asyncio.wait_for(worker._task, GUARD_SECONDS)


# --- Queues, receipts, rows and points -----------------------------------------------------------


def queue_counts(url: str) -> tuple[int, int]:
    attributes = sqs_client().get_queue_attributes(
        QueueUrl=url,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]
    return (
        int(attributes["ApproximateNumberOfMessages"]),
        int(attributes["ApproximateNumberOfMessagesNotVisible"]),
    )


def drain_bodies(url: str, *, expected: int, attempts: int = 20) -> list[str]:
    """Receive and delete until `expected` bodies arrived (`expected=0` polls `attempts` times)."""
    sqs = sqs_client()
    bodies: list[str] = []
    for _ in range(attempts):
        if expected and len(bodies) >= expected:
            break
        response = sqs.receive_message(
            QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1, VisibilityTimeout=30
        )
        for message in response.get("Messages") or []:
            bodies.append(message["Body"])
            sqs.delete_message(QueueUrl=url, ReceiptHandle=message["ReceiptHandle"])
    return bodies


def peek_dlq(url: str, *, expected: int) -> list[str]:
    """Bodies on a DLQ (no redrive policy, so receiving there never moves anything)."""
    sqs = sqs_client()
    seen: dict[str, str] = {}
    for _ in range(20):
        if len(seen) >= expected:
            break
        response = sqs.receive_message(
            QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1, VisibilityTimeout=0
        )
        for message in response.get("Messages") or []:
            seen[message["MessageId"]] = message["Body"]
    return list(seen.values())


def resend(url: str, bodies: Sequence[str]) -> None:
    sqs = sqs_client()
    for body in bodies:
        sqs.send_message(QueueUrl=url, MessageBody=body)


async def receipt_of(tables: IngestionTables, key: str, account: str = ACCOUNT) -> Any:
    repository = DynamoIngressReceiptRepository(tables.domain, dynamodb_client())
    return await repository.get_ingress_receipt(account, idempotency_key_hash(key))


async def quarantine_rows(tables: IngestionTables, tenant: str = TENANT) -> list[Any]:
    repository = DynamoQuarantineRepository(tables.domain, dynamodb_client())
    page = await repository.list_quarantine_entries(tenant, after_sk=None, limit=100)
    return list(page.entries)


async def state_rows(
    tables: IngestionTables,
    *,
    tenant: str = TENANT,
    component_id: str = COMPONENT,
    device_id: str = DEVICE,
    account: str = ACCOUNT,
) -> dict[str, Any]:
    client = dynamodb_client()
    repository = DynamoIngestStateRepository(tables.domain, client)
    subject = DynamoIngestionSubject(client, domain_table=tables.domain, audit_table=tables.audit)
    components = await repository.get_component_states(tenant, [component_id])
    device_item = subject.item(subject.integrations.keys.device_ingest_state(device_id))
    health = await repository.get_connector_health(tenant, account)
    return {
        "component": None
        if component_id not in components
        else components[component_id].model_dump(exclude={"last_ingested_at"}),
        "device": None
        if device_item is None
        else device_ingest_state_from_item(device_item).model_dump(exclude={"last_ingested_at"}),
        "connector": None if health is None else health.model_dump(),
    }


def points(bucket: str) -> dict[tuple[Any, ...], dict[str, Any]]:
    """Every point of both measurements, keyed by (measurement, tag set, time), with every field
    except `ingested_at_ns` (A4 point equality)."""
    flux = (
        f'from(bucket: "{bucket}") |> range(start: {TIME_MIN}, stop: {TIME_MAX}) '
        f'|> filter(fn: (r) => r._measurement == "{OBSERVATION_MEASUREMENT}" or '
        f'r._measurement == "{LEGACY_MEASUREMENT}")'
    )
    with influx_clients(bucket) as (query_api, _write_api):
        tables = query_api.query(flux, org=influx_settings().org)
    found: dict[tuple[Any, ...], dict[str, Any]] = {}
    for table in tables:
        for record in table.records:
            values = dict(record.values)
            tags = tuple(
                sorted(
                    (key, value)
                    for key, value in values.items()
                    if not key.startswith("_") and key not in {"result", "table"}
                )
            )
            key = (values["_measurement"], tags, values["_time"])
            if values["_field"] != "ingested_at_ns":
                found.setdefault(key, {})[values["_field"]] = values["_value"]
    return found


def measurements(found: dict[tuple[Any, ...], dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for measurement, *_rest in found:
        counts[measurement] = counts.get(measurement, 0) + 1
    return counts


@pytest.fixture
def control_bucket(influx_bucket: str) -> Iterator[str]:
    settings = influx_settings()
    client = InfluxDBClient(url=settings.url, token=settings.token, org=settings.org)
    try:
        bucket = client.buckets_api().create_bucket(bucket_name=f"{influx_bucket}_control")
        try:
            yield bucket.name
        finally:
            client.buckets_api().delete_bucket(bucket)
    finally:
        client.close()


# --- Worlds --------------------------------------------------------------------------------------


@dataclass
class KitWorld:
    """The P3-00 kit tenant on DynamoDB Local: an active `https_direct` account with one active
    credential, its DeviceIntegration for `EXTERNAL_DEVICE`, the v2-native Device with one active
    Component and (unless asked otherwise) a Deployment covering every observation."""

    tables: IngestionTables
    subject: DynamoIngestionSubject
    credentials: DynamoIngestCredentialRepository
    extra: dict[str, Any] = field(default_factory=dict)

    async def transition_account(self, status: IntegrationAccountStatus) -> None:
        current = await self.subject.integrations.get_integration_account(TENANT, ACCOUNT)
        assert current is not None
        await self.subject.integrations.transition_integration_account(
            TENANT,
            ACCOUNT,
            status,
            expected_version=current.version,
            updated_at=datetime.now(UTC),
            audit=AUDIT,
        )

    async def transition_integration(self, status: DeviceIntegrationStatus) -> None:
        (current,) = [
            item
            for item in await self.subject.integrations.list_device_integrations(TENANT, DEVICE)
            if item.device_integration_id == INTEGRATION
        ]
        await self.subject.integrations.transition_device_integration(
            TENANT,
            INTEGRATION,
            status,
            expected_version=current.version,
            updated_at=datetime.now(UTC),
            audit=AUDIT,
        )

    def add_deployment(self) -> None:
        for form in ("mirror", "history", "current"):
            self.subject.put(deployment_to_item(DEPLOYMENT, form=form))


async def kit_world(tables: IngestionTables, *, deployment: bool = True) -> KitWorld:
    client = dynamodb_client()
    subject = DynamoIngestionSubject(client, domain_table=tables.domain, audit_table=tables.audit)
    credentials = DynamoIngestCredentialRepository(
        tables.domain, client, audit_table_name=tables.audit
    )
    world = KitWorld(tables=tables, subject=subject, credentials=credentials)
    await subject.seed_account(integration_account())
    await subject.seed_device_integration(device_integration())
    await subject.seed_device(device())
    await subject.seed_component(component())
    await credentials.create_ingest_credential(credential(), AUDIT)
    if deployment:
        world.add_deployment()
    return world


@dataclass
class LegacyWorld:
    """The G15 fixture, seeded through the production path: `seed_local.py`'s legacy tenant, pond
    and device (`DynamoDomainRepository`), the M1 backfill (`DefaultSiteAssetMigrator`: the v1 pond
    projection, the default Component and its Deployment) and `seed_ingest_local.seed` (the
    `https_direct` IntegrationAccount, its DeviceIntegration for the device and a credential)."""

    token: str
    seeded_at: datetime
    integration_account_id: str


async def legacy_world(settings: Settings) -> LegacyWorld:
    client = dynamodb_client()
    repository = DynamoDomainRepository(table_name=settings.dynamodb_domain_table, client=client)
    await repository.create_tenant_with_owner(LEGACY_TENANT, "Local Tenant", "local-user-001")
    await repository.create_pond(LEGACY_TENANT, LEGACY_POND, "Local Pond", None)
    await repository.create_device(
        LEGACY_TENANT, LEGACY_POND, LEGACY_DEVICE, "Local Device 001", "local-dev"
    )
    summary = DefaultSiteAssetMigrator(client, table_name=settings.dynamodb_domain_table).run(
        [LEGACY_TENANT], apply=True, page_size=100, deadline_seconds=60
    )
    assert summary.result == "success", summary
    token = await seed_ingest_local.seed(settings)
    credential_id = token.split("_", 2)[1] + "_" + token.split("_", 2)[2][:32]
    stored = await DynamoIngestCredentialRepository(
        settings.dynamodb_domain_table, client, audit_table_name=settings.dynamodb_audit_table
    ).get_ingest_credential(credential_id)
    assert stored is not None
    await asyncio.sleep(0.01)
    return LegacyWorld(
        token=token,
        seeded_at=datetime.now(UTC),
        integration_account_id=stored.integration_account_id,
    )


LEGACY_FIELDS = (
    # (legacy field, metric key, source unit)
    ("temp_c", "water.temperature", "degC"),
    ("ph", "water.ph", "pH"),
    ("do_mg_l", "water.dissolved_oxygen", "mg/L"),
    ("turbidity_ntu", "water.turbidity", "NTU"),
    ("salinity_ppt", "water.salinity", "ppt"),
    ("battery_v", "device.battery_voltage", "V"),
    ("rssi", "device.rssi", "dBm"),
)


def legacy_readings(start: datetime, count: int) -> list[tuple[datetime, int, dict[str, float]]]:
    """`count` legacy readings 250 ms apart: (observed_at, seq, field -> value)."""
    readings = []
    for index in range(count):
        values = {
            "temp_c": 24.0 + 0.1 * index,
            "ph": 7.0 + 0.01 * index,
            "do_mg_l": 6.0 + 0.05 * index,
            "turbidity_ntu": 3.0 + index,
            "salinity_ppt": 11.5 + 0.1 * index,
            "battery_v": 3.8 - 0.01 * index,
            "rssi": float(-60 - index),
        }
        readings.append((start + timedelta(milliseconds=250 * (index + 1)), 100 + index, values))
    return readings


def https_observations(reading_: tuple[datetime, int, dict[str, float]]) -> list[dict[str, Any]]:
    observed_at, seq, values = reading_
    return [
        {
            "metric_key": metric,
            "value": int(values[field_]) if field_ == "rssi" else values[field_],
            "source_unit": unit,
            "observed_at": iso(observed_at),
            "sequence": seq,
        }
        for field_, metric, unit in LEGACY_FIELDS
    ]


def telegraf_line(reading_: tuple[datetime, int, dict[str, float]]) -> str:
    """The `water_quality` line Telegraf writes for one legacy MQTT reading."""
    observed_at, seq, values = reading_
    fields = ",".join(
        f"{name}={int(values[name])}i" if name == "rssi" else f"{name}={values[name]!r}"
        for name, _metric, _unit in LEGACY_FIELDS
    )
    return (
        f"water_quality,device_id={LEGACY_DEVICE},pond_id={LEGACY_POND},schema_version=1,"
        f"source=mqtt,tenant_id={LEGACY_TENANT},topic=limnopulse/v1/devices/{LEGACY_DEVICE}/readings "
        f"{fields},seq={seq}i {ns(observed_at)}"
    )


async def post_legacy_readings(
    http: httpx.AsyncClient, world: LegacyWorld, readings: Sequence[Any], prefix: str
) -> list[dict[str, Any]]:
    accepted = []
    for index, item in enumerate(readings):
        response = await post_batch(
            http,
            https_observations(item),
            key=f"{prefix}-reading-{index:04d}",
            token=world.token,
            external_device_id=LEGACY_DEVICE,
        )
        assert response.status_code == 202, response.text
        accepted.append(response.json())
    return accepted


class _ConfiguredMembership:
    """Owner of every tenant for the dev user `sub_1` (the composed app's dev auth)."""

    async def get_active_membership(self, cognito_sub: str, tenant_id: str) -> Membership | None:
        if cognito_sub != "sub_1":
            return None
        return Membership(
            tenant_id=tenant_id,
            cognito_sub=cognito_sub,
            role=TenantRole.OWNER,
            created_at=CREATED,
            updated_at=CREATED,
            version=1,
        )


class FaultingDynamo:
    """A real DynamoDB client whose next call of one operation raises once."""

    def __init__(self, client: Any, operation: str) -> None:
        self._client = client
        self._pending = {operation: 1}

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self._client, name)
        if not callable(attribute):
            return attribute

        def call(*args: Any, **kwargs: Any) -> Any:
            if self._pending.get(name):
                self._pending[name] -= 1
                raise ClientError(
                    {"Error": {"Code": "InternalServerError", "Message": "injected"}}, name
                )
            return attribute(*args, **kwargs)

        return call


def faulting_receipts(tables: IngestionTables, operation: str) -> DynamoIngressReceiptRepository:
    return DynamoIngressReceiptRepository(
        tables.domain, FaultingDynamo(dynamodb_client(), operation)
    )


def v2_received_at_ns(bucket: str) -> dict[str, int]:
    """`event_id` -> `received_at_ns` of every v2 point."""
    found = points(bucket)
    return {
        fields["event_id"]: fields["received_at_ns"]
        for (measurement, _tags, _time), fields in found.items()
        if measurement == OBSERVATION_MEASUREMENT
    }


# --- G1 ------------------------------------------------------------------------------------------


async def test_gate1_accepted_only_after_the_queue_write(
    ingestion_tables: IngestionTables,
    ingestion_queue_pair: QueueFactory,
    influx_bucket: str,
    metric_lines: MetricLines,
) -> None:
    queues = ingestion_queue_pair(max_receive_count=8, visibility_seconds=30)
    settings = gate_settings(ingestion_tables, queues, influx_bucket)
    await kit_world(ingestion_tables)
    at = recent()
    observations = [reading(at, 21.5, sequence=1)]
    async with composed_api(settings) as (app, http):
        producer = app.state.ingress_queue_producer
        receipts = app.state.ingress_receipt_repository

        # A failing queue: 503, the receipt stays pending and nothing is queued.
        app.state.ingress_queue_producer = SqsIngressQueue(
            client=sqs_client(), queue_url=queues.source_url + "-missing"
        )
        failed = await post_batch(http, observations, key="g1-queue-down")
        assert (failed.status_code, failed.json()) == (503, {"detail": "service unavailable"})
        pending = await receipt_of(ingestion_tables, "g1-queue-down")
        assert pending is not None and pending.state == IngressReceiptState.PENDING
        app.state.ingress_queue_producer = producer

        # A receipt store failing before the send (lookup, then claim): 503 and nothing queued.
        for operation, key in (("get_item", "g1-lookup-down"), ("put_item", "g1-claim-down")):
            app.state.ingress_receipt_repository = faulting_receipts(ingestion_tables, operation)
            response = await post_batch(http, observations, key=key)
            assert response.status_code == 503, (operation, response.text)
            assert await receipt_of(ingestion_tables, key) is None
        assert queue_counts(queues.source_url) == (0, 0)

        # A mark failure after a successful send is still 202 (route step 15).
        app.state.ingress_receipt_repository = faulting_receipts(ingestion_tables, "update_item")
        marked = await post_batch(http, observations, key="g1-mark-down")
        assert marked.status_code == 202, marked.text
        assert queue_counts(queues.source_url)[0] == 1
        app.state.ingress_receipt_repository = receipts

        # After recovery the retry enqueues the first `received_at`.
        await asyncio.sleep(0.05)
        retried = await post_batch(http, observations, key="g1-queue-down")
        assert retried.status_code == 202, retried.text
        assert retried.json() == {
            "ingress_id": pending.ingress_id,
            "accepted_observations": 1,
            "replayed": True,
        }

    bodies = drain_bodies(queues.source_url, expected=2)
    envelopes = {IngressEnvelope.model_validate_json(body).ingress_id: body for body in bodies}
    first = IngressEnvelope.model_validate_json(envelopes[pending.ingress_id])
    assert first.received_at == pending.received_at
    resend(queues.source_url, [envelopes[pending.ingress_id]])
    async with running_worker(settings, metric_lines) as worker:
        await worker.wait_handled(1, outcome="processed")
    event_id = observation_event_id(TENANT, COMPONENT, "water.temperature", at)
    assert v2_received_at_ns(influx_bucket) == {event_id: ns(pending.received_at)}
    assert queue_counts(queues.source_url) == (0, 0)


# --- G2 ------------------------------------------------------------------------------------------


async def test_gate2_every_bad_credential_or_account_is_401_and_revocation_is_immediate(
    ingestion_tables: IngestionTables, ingestion_queue_pair: QueueFactory, influx_bucket: str
) -> None:
    queues = ingestion_queue_pair(max_receive_count=8, visibility_seconds=30)
    settings = gate_settings(ingestion_tables, queues, influx_bucket)
    world = await kit_world(ingestion_tables)
    # A second credential of the same account, for the revocation case.
    second = credential(ingest_credential_id=OTHER_CREDENTIAL, slot=2)
    await world.credentials.create_ingest_credential(second, AUDIT)
    # A pending https account and a non-HTTPS account, each with a stored credential.
    pending_ic, foreign_ic = "ic_" + "c" * 32, "ic_" + "d" * 32
    pending_ia, foreign_ia = "ia_" + "c" * 32, "ia_" + "d" * 32
    await world.subject.seed_account(
        integration_account(
            integration_account_id=pending_ia, status=IntegrationAccountStatus.PENDING
        )
    )
    await world.credentials.create_ingest_credential(
        credential(ingest_credential_id=pending_ic, integration_account_id=pending_ia), AUDIT
    )
    await world.subject.seed_account(integration_account(integration_account_id=foreign_ia))
    await world.credentials.create_ingest_credential(
        credential(ingest_credential_id=foreign_ic, integration_account_id=foreign_ia), AUDIT
    )
    await world.subject.seed_account(
        integration_account(integration_account_id=foreign_ia, provider_key="vendor_cloud")
    )
    at = recent()
    observations = [reading(at, 20.0, sequence=1)]
    cases = {
        "missing": None,
        "malformed": "Bearer not-a-token",
        "not bearer": f"Basic {TOKEN}",
        "unknown": f"Bearer {ingest_token('ic_' + 'e' * 32, SECRET)}",
        "wrong digest": f"Bearer {ingest_token(CREDENTIAL, WRONG_SECRET)}",
        "inactive account": f"Bearer {ingest_token(pending_ic, SECRET)}",
        "non-https account": f"Bearer {ingest_token(foreign_ic, SECRET)}",
    }
    async with composed_api(settings) as (_app, http):
        for index, (case, authorization) in enumerate(cases.items()):
            headers = ingest_headers(None, key=f"g2-case-{index:04d}")
            if authorization is not None:
                headers["authorization"] = authorization
            response = await post_batch(http, observations, key="unused", headers=headers)
            assert response.status_code == 401, case
            assert response.json() == {"detail": "unauthorized"}, case
            assert response.headers["www-authenticate"] == "Bearer", case

        accepted = await post_batch(http, observations, key="g2-before-revoke", token=OTHER_TOKEN)
        assert accepted.status_code == 202, accepted.text
        await world.credentials.revoke_ingest_credential(
            TENANT,
            ACCOUNT,
            OTHER_CREDENTIAL,
            expected_version=1,
            revoked_at=datetime.now(UTC),
            audit=AUDIT,
        )
        revoked = await post_batch(http, observations, key="g2-after-revoke", token=OTHER_TOKEN)
        assert revoked.status_code == 401
        # The other credential of the account keeps working.
        still = await post_batch(http, observations, key="g2-other-credential")
        assert still.status_code == 202, still.text

    assert len(drain_bodies(queues.source_url, expected=2)) == 2
    assert drain_bodies(queues.source_url, expected=0, attempts=2) == []


# --- G3 ------------------------------------------------------------------------------------------


async def test_gate3_no_cross_tenant_claim_and_forged_envelopes_are_fenced(
    ingestion_tables: IngestionTables,
    ingestion_queue_pair: QueueFactory,
    influx_bucket: str,
    metric_lines: MetricLines,
) -> None:
    queues = ingestion_queue_pair(max_receive_count=2, visibility_seconds=30)
    settings = gate_settings(ingestion_tables, queues, influx_bucket)
    world = await kit_world(ingestion_tables)
    subject = world.subject
    # Another tenant's https account claims the same external device id under its own namespace.
    other_hash = account_scoped_identity_hash(PROVIDER, OTHER_ACCOUNT, EXTERNAL_DEVICE)
    await subject.seed_account(
        integration_account(tenant_id=OTHER_TENANT, integration_account_id=OTHER_ACCOUNT)
    )
    await world.credentials.create_ingest_credential(
        credential(
            tenant_id=OTHER_TENANT,
            ingest_credential_id=OTHER_CREDENTIAL,
            integration_account_id=OTHER_ACCOUNT,
        ),
        AUDIT,
    )
    at = recent()
    other_obs = [reading(at, 30.0, component_id=OTHER_COMPONENT, sequence=1)]
    own_obs = [reading(at, 20.0, sequence=1)]
    async with composed_api(settings) as (app, http):
        # Before the other tenant maps the id, its credential cannot reach our device.
        claimed = await post_batch(http, other_obs, key="g3-claim", token=OTHER_TOKEN)
        assert (claimed.status_code, claimed.json()) == (403, {"detail": "forbidden"})

        await subject.seed_device(device(tenant_id=OTHER_TENANT, device_id=OTHER_DEVICE))
        await subject.seed_component(
            component(OTHER_COMPONENT, tenant_id=OTHER_TENANT, device_id=OTHER_DEVICE)
        )
        await subject.seed_device_integration(
            device_integration(
                tenant_id=OTHER_TENANT,
                device_integration_id=OTHER_INTEGRATION,
                device_id=OTHER_DEVICE,
                integration_account_id=OTHER_ACCOUNT,
                external_identity_hash=other_hash,
            )
        )
        theirs = await post_batch(http, other_obs, key="g3-theirs", token=OTHER_TOKEN)
        ours = await post_batch(http, own_obs, key="g3-ours-0001")
        assert (theirs.status_code, ours.status_code) == (202, 202)

        # A cross-tenant credential revoke is 404 and leaves the credential working.
        app.state.membership_service = _ConfiguredMembership()
        revoke = await http.post(
            f"/v2/tenants/{OTHER_TENANT}/integration-accounts/{OTHER_ACCOUNT}"
            f"/ingest-credentials/{CREDENTIAL}/revoke",
            json={"expected_version": 1},
            headers={"X-Dev-User-Sub": "sub_1"},
        )
        assert revoke.status_code == 404, revoke.text
        stored = await world.credentials.get_ingest_credential(CREDENTIAL)
        assert stored is not None and stored.status.value == "active"

    # No namespace collision: each admission carries its own tenant, account, DI and device.
    by_tenant = {
        e.tenant_id: e
        for e in map(
            IngressEnvelope.model_validate_json, drain_bodies(queues.source_url, expected=2)
        )
    }
    assert set(by_tenant) == {TENANT, OTHER_TENANT}
    assert (
        by_tenant[TENANT].integration_account_id,
        by_tenant[TENANT].device_integration_id,
        by_tenant[TENANT].device_id,
    ) == (ACCOUNT, INTEGRATION, DEVICE)
    assert (
        by_tenant[OTHER_TENANT].integration_account_id,
        by_tenant[OTHER_TENANT].device_integration_id,
        by_tenant[OTHER_TENANT].device_id,
    ) == (OTHER_ACCOUNT, OTHER_INTEGRATION, OTHER_DEVICE)
    assert by_tenant[TENANT].external_identity_hash != other_hash

    genuine = by_tenant[TENANT]
    # Forged: a mismatched DI under a verified account -> fenced rows; a mismatched tenant whose
    # (tenant, account) row does not exist -> nothing written, the message ends in the DLQ.
    mismatched_di = genuine.model_copy(
        update={"ingress_id": "ing_" + "5" * 32, "device_integration_id": OTHER_INTEGRATION}
    )
    unverified = genuine.model_copy(
        update={"ingress_id": "ing_" + "6" * 32, "tenant_id": OTHER_TENANT}
    )
    resend(queues.source_url, [mismatched_di.to_json(), unverified.to_json()])
    async with running_worker(settings, metric_lines) as worker:
        await worker.wait_handled(1, outcome="fenced")
        await worker.wait_for(
            lambda: queue_counts(queues.dlq_url)[0] + queue_counts(queues.dlq_url)[1] == 1,
            "the unverified forged envelope in the DLQ",
        )
    rows = await quarantine_rows(ingestion_tables)
    assert sorted(r.kind.value for r in rows) == ["fenced_message", "fenced_observation"]
    assert {r.reason for r in rows} == {"fenced_integration_mismatch"}
    assert {r.ingress_id for r in rows} == {mismatched_di.ingress_id}
    assert await quarantine_rows(ingestion_tables, OTHER_TENANT) == []
    assert (await state_rows(ingestion_tables, tenant=OTHER_TENANT))["connector"] is None
    assert peek_dlq(queues.dlq_url, expected=1) == [unverified.to_json()]
    assert queue_counts(queues.source_url) == (0, 0)
    assert metric_lines.total("ingress_fenced_total", reason="integration_mismatch") >= 2
    assert points(influx_bucket) == {}


# --- G4 ------------------------------------------------------------------------------------------


async def test_gate4_ownership_comes_only_from_the_mapping(
    ingestion_tables: IngestionTables,
    ingestion_queue_pair: QueueFactory,
    influx_bucket: str,
    metric_lines: MetricLines,
) -> None:
    queues = ingestion_queue_pair(max_receive_count=8, visibility_seconds=30)
    settings = gate_settings(ingestion_tables, queues, influx_bucket)
    world = await kit_world(ingestion_tables)
    # A Component of another Device of the same tenant.
    await world.subject.seed_device(device(device_id=OTHER_DEVICE))
    await world.subject.seed_component(component(OTHER_COMPONENT, device_id=OTHER_DEVICE))
    at = recent()
    async with composed_api(settings) as (_app, http):
        for index, claim in enumerate(
            ("tenant_id", "site_id", "asset_id", "deployment_id", "pond_id")
        ):
            response = await post_batch(
                http, [reading(at, 1.0)], key=f"g4-claim-{index:04d}", **{claim: "x-1"}
            )
            assert response.status_code == 422, claim
            assert "x-1" not in response.text
            nested = await post_batch(
                http, [reading(at, 1.0, **{claim: "x-1"})], key=f"g4-nested-{index:04d}"
            )
            assert nested.status_code == 422, claim
        for index, foreign in enumerate((OTHER_COMPONENT, "cmp_" + "e" * 32)):
            response = await post_batch(
                http,
                [reading(at, 1.0), reading(at, 1.5, component_id=foreign)],
                key=f"g4-foreign-{index:04d}",
            )
            assert (response.status_code, response.json()) == (
                422,
                {
                    "detail": [
                        {
                            "loc": ["body", "observations", 1, "component_id"],
                            "msg": "component_id is not valid for this device",
                            "type": "value_error",
                        }
                    ]
                },
            ), foreign
        # The kit Device is v2-native: it has no legacy default Component to fill in.
        omitted = await post_batch(http, [reading(at, 1.0, component_id=None)], key="g4-omitted")
        assert (omitted.status_code, omitted.json()) == (
            422,
            {
                "detail": [
                    {
                        "loc": ["body", "observations", 0, "component_id"],
                        "msg": "component_id is required for this device",
                        "type": "missing",
                    }
                ]
            },
        )
        genuine = await post_batch(http, [reading(at, 2.0, sequence=1)], key="g4-genuine")
        assert genuine.status_code == 202, genuine.text

    (body,) = drain_bodies(queues.source_url, expected=1)
    envelope = IngressEnvelope.model_validate_json(body)
    forged = envelope.model_copy(
        update={
            "ingress_id": "ing_" + "7" * 32,
            "observations": tuple(
                o.model_copy(update={"component_id": OTHER_COMPONENT})
                for o in envelope.observations
            ),
        }
    )
    resend(queues.source_url, [forged.to_json()])
    async with running_worker(settings, metric_lines) as worker:
        await worker.wait_handled(1, outcome="processed")
    (row,) = await quarantine_rows(ingestion_tables)
    assert (row.kind, row.reason) == (QuarantineEntryKind.OBSERVATION, "component_not_owned")
    assert row.ingress_id == forged.ingress_id
    assert points(influx_bucket) == {}
    assert queue_counts(queues.source_url) == (0, 0)


# --- G5 ------------------------------------------------------------------------------------------


async def test_gate5_idempotent_retries_keep_the_first_received_at(
    ingestion_tables: IngestionTables,
    ingestion_queue_pair: QueueFactory,
    influx_bucket: str,
    metric_lines: MetricLines,
) -> None:
    queues = ingestion_queue_pair(max_receive_count=8, visibility_seconds=30)
    settings = gate_settings(ingestion_tables, queues, influx_bucket)
    await kit_world(ingestion_tables)
    at = recent()
    lost = [reading(at, 20.0, sequence=1)]
    marked = [reading(at + timedelta(seconds=1), 21.0, sequence=2)]
    async with composed_api(settings) as (app, http):
        receipts = app.state.ingress_receipt_repository
        # Lost response: the retry is answered from the enqueued receipt, nothing new is queued.
        first = await post_batch(http, lost, key="g5-lost-response")
        await asyncio.sleep(0.05)
        again = await post_batch(http, lost, key="g5-lost-response")
        assert first.status_code == again.status_code == 202
        assert again.json() == {**first.json(), "replayed": True}
        assert queue_counts(queues.source_url)[0] == 1

        # Send OK, mark failed: the receipt stays pending; the retry re-enqueues the same message.
        app.state.ingress_receipt_repository = faulting_receipts(ingestion_tables, "update_item")
        sent = await post_batch(http, marked, key="g5-mark-failed")
        app.state.ingress_receipt_repository = receipts
        assert sent.status_code == 202, sent.text
        pending = await receipt_of(ingestion_tables, "g5-mark-failed")
        assert pending is not None and pending.state == IngressReceiptState.PENDING
        await asyncio.sleep(0.05)
        reenqueued = await post_batch(http, marked, key="g5-mark-failed")
        assert reenqueued.json() == {**sent.json(), "replayed": True}
        enqueued = await receipt_of(ingestion_tables, "g5-mark-failed")
        assert enqueued == pending.model_copy(update={"state": IngressReceiptState.ENQUEUED})

    lost_receipt = await receipt_of(ingestion_tables, "g5-lost-response")
    async with running_worker(settings, metric_lines) as worker:
        await worker.wait_handled(3, outcome="processed")
    assert queue_counts(queues.source_url) == (0, 0)
    assert v2_received_at_ns(influx_bucket) == {
        observation_event_id(TENANT, COMPONENT, "water.temperature", at): ns(
            lost_receipt.received_at
        ),
        observation_event_id(TENANT, COMPONENT, "water.temperature", at + timedelta(seconds=1)): ns(
            pending.received_at
        ),
    }


# --- G6 ------------------------------------------------------------------------------------------


async def _reset_worker_rows(settings: Settings, world: LegacyWorld) -> None:
    """Forget the worker's rows (state, health, quarantine) so a second run starts clean."""
    client = dynamodb_client()
    subject = DynamoIngestionSubject(
        client,
        domain_table=settings.dynamodb_domain_table,
        audit_table=settings.dynamodb_audit_table,
    )
    keys = subject.integrations.keys
    tables = IngestionTables(settings.dynamodb_domain_table, settings.dynamodb_audit_table)
    for key in (
        keys.component_ingest_state(LEGACY_COMPONENT),
        keys.device_ingest_state(LEGACY_DEVICE),
        keys.connector_health(world.integration_account_id),
        *(
            {"PK": keys.quarantine_partition(LEGACY_TENANT), "SK": row.entry_key}
            for row in await quarantine_rows(tables, LEGACY_TENANT)
        ),
    ):
        client.delete_item(
            TableName=settings.dynamodb_domain_table, Key=subject.integrations.serialize_item(key)
        )


async def _legacy_snapshot(settings: Settings, world: LegacyWorld) -> dict[str, Any]:
    tables = IngestionTables(settings.dynamodb_domain_table, settings.dynamodb_audit_table)
    return {
        "points": points(settings.influxdb_bucket_raw),
        "state": await state_rows(
            tables,
            tenant=LEGACY_TENANT,
            component_id=LEGACY_COMPONENT,
            device_id=LEGACY_DEVICE,
            account=world.integration_account_id,
        ),
        "quarantine": sorted(
            (r.entry_key, r.reason, r.status.value)
            for r in await quarantine_rows(tables, LEGACY_TENANT)
        ),
    }


async def test_gate6_redelivery_and_shuffling_converge_on_points_and_state(
    ingestion_tables: IngestionTables,
    ingestion_queue_pair: QueueFactory,
    influx_bucket: str,
    control_bucket: str,
    metric_lines: MetricLines,
) -> None:
    queues = ingestion_queue_pair(max_receive_count=8, visibility_seconds=30)
    ordered_settings = gate_settings(
        ingestion_tables, queues, influx_bucket, legacy_dual_write=True
    )
    world = await legacy_world(ordered_settings)
    readings = legacy_readings(world.seeded_at, 4)
    async with composed_api(ordered_settings) as (_app, http):
        await post_legacy_readings(http, world, readings, "g6")
        unknown = await post_batch(
            http,
            [{**https_observations(readings[0])[0], "metric_key": "water.unknown"}],
            key="g6-quarantined",
            token=world.token,
            external_device_id=LEGACY_DEVICE,
        )
        assert unknown.status_code == 202, unknown.text
    bodies = drain_bodies(queues.source_url, expected=5)
    assert len(bodies) == 5

    resend(queues.source_url, bodies)
    async with running_worker(ordered_settings, metric_lines) as worker:
        await worker.wait_handled(5, outcome="processed")
    ordered = await _legacy_snapshot(ordered_settings, world)

    await _reset_worker_rows(ordered_settings, world)
    shuffled_settings = gate_settings(
        ingestion_tables, queues, control_bucket, legacy_dual_write=True
    )
    deliveries = [*bodies, *bodies, bodies[1]]
    random.Random(11).shuffle(deliveries)
    resend(queues.source_url, deliveries)
    async with running_worker(shuffled_settings, metric_lines) as worker:
        await worker.wait_handled(5 + len(deliveries), outcome="processed")
    shuffled = await _legacy_snapshot(shuffled_settings, world)

    assert measurements(ordered["points"]) == {OBSERVATION_MEASUREMENT: 28, LEGACY_MEASUREMENT: 4}
    assert shuffled["points"] == ordered["points"]
    assert ordered["state"]["component"]["last_observed_sequence"] == 103
    assert ordered["state"]["device"] is not None and ordered["state"]["connector"] is not None
    assert shuffled["state"] == ordered["state"]
    assert len(ordered["quarantine"]) == 1
    assert shuffled["quarantine"] == ordered["quarantine"]


# --- G7 ------------------------------------------------------------------------------------------


async def test_gate7_influx_outage_reaches_the_dlq_and_redrive_reproduces_the_baseline(
    ingestion_tables: IngestionTables,
    ingestion_queue_pair: QueueFactory,
    influx_bucket: str,
    control_bucket: str,
    metric_lines: MetricLines,
) -> None:
    queues = ingestion_queue_pair(max_receive_count=3, visibility_seconds=30)
    control_queues = ingestion_queue_pair(max_receive_count=8, visibility_seconds=30)
    influx_url = urlparse(influx_settings().url)
    with ToggleableTcpProxy(influx_url.hostname or "127.0.0.1", influx_url.port or 8086) as proxy:
        settings = gate_settings(
            ingestion_tables, queues, influx_bucket, influx_url=proxy.url, legacy_dual_write=True
        )
        world = await legacy_world(settings)
        readings = legacy_readings(world.seeded_at, 2)
        proxy.down()
        async with composed_api(settings) as (_app, http):
            accepted = await post_legacy_readings(http, world, readings, "g7")

        # The outage: every attempt fails, nothing is deleted, maxReceiveCount moves both.
        async with running_worker(settings, metric_lines) as worker:
            await worker.wait_for(
                lambda: sum(queue_counts(queues.dlq_url)) == 2, "both messages in the DLQ"
            )
        assert queue_counts(queues.source_url) == (0, 0)
        assert metric_lines.total("ingress_messages_total", outcome="processed") == 0
        assert metric_lines.total("ingress_write_failures_total", retryable="true") >= 2 * 3
        assert await quarantine_rows(ingestion_tables, LEGACY_TENANT) == []
        dead = peek_dlq(queues.dlq_url, expected=2)
        assert sorted(IngressEnvelope.model_validate_json(b).ingress_id for b in dead) == sorted(
            item["ingress_id"] for item in accepted
        )

        # Recovery: the proxy is back, the operator redrives, the worker writes.
        proxy.up()
        out = io.StringIO()
        code = await asyncio.to_thread(
            telemetry_dlq.main,
            ["redrive", "--apply"],
            settings=settings,
            sqs_client=sqs_client(),
            metrics=_NullMetrics(),
            out=out,
            err=io.StringIO(),
        )
        assert code == 0, out.getvalue()
        assert json.loads(out.getvalue().splitlines()[-1])["redriven"] == 2
        async with running_worker(settings, metric_lines) as worker:
            await worker.wait_handled(2, outcome="processed")
        assert queue_counts(queues.dlq_url) == (0, 0)
        assert queue_counts(queues.source_url) == (0, 0)

    # The no-fault baseline: the same bodies through a healthy worker into a control bucket.
    control = gate_settings(
        ingestion_tables, control_queues, control_bucket, legacy_dual_write=True
    )
    resend(control_queues.source_url, dead)
    async with running_worker(control, metric_lines) as worker:
        await worker.wait_handled(4, outcome="processed")
    expected = points(control_bucket)
    assert measurements(expected) == {OBSERVATION_MEASUREMENT: 14, LEGACY_MEASUREMENT: 2}
    assert points(influx_bucket) == expected


class _NullMetrics:
    def increment(self, name: str, value: int = 1, **labels: str) -> None:
        return None

    def observe(self, name: str, value: float, **labels: str) -> None:
        return None


# --- G8 ------------------------------------------------------------------------------------------


async def test_gate8_unresolved_deployment_is_quarantined_then_replayed_into_event_time(
    ingestion_tables: IngestionTables,
    ingestion_queue_pair: QueueFactory,
    influx_bucket: str,
    metric_lines: MetricLines,
) -> None:
    queues = ingestion_queue_pair(max_receive_count=8, visibility_seconds=30)
    settings = gate_settings(ingestion_tables, queues, influx_bucket)
    world = await kit_world(ingestion_tables, deployment=False)
    at = recent()
    async with composed_api(settings) as (_app, http):
        response = await post_batch(http, [reading(at, 22.5, sequence=7)], key="g8-unresolved")
        assert response.status_code == 202, response.text
    first = await receipt_of(ingestion_tables, "g8-unresolved")

    async with running_worker(settings, metric_lines) as worker:
        await worker.wait_handled(1, outcome="processed")
    assert queue_counts(queues.source_url) == (0, 0)  # deleted only after the row was persisted
    (row,) = await quarantine_rows(ingestion_tables)
    assert (row.kind, row.reason, row.status) == (
        QuarantineEntryKind.OBSERVATION,
        "deployment_unresolved",
        QuarantineEntryStatus.OPEN,
    )
    assert row.replayable is True and row.received_at == first.received_at
    health = (await state_rows(ingestion_tables))["connector"]
    assert health is not None and health["last_quarantine_reason"] == "deployment_unresolved"
    assert points(influx_bucket) == {}

    # The operator adds the covering Deployment and replays the row.
    world.add_deployment()
    out = io.StringIO()
    code = await asyncio.to_thread(
        telemetry_quarantine.main,
        ["replay", "--tenant", TENANT, "--entry", row.entry_key, "--apply"],
        settings=settings,
        dynamodb_client=dynamodb_client(),
        sqs_client=sqs_client(),
        metrics=_NullMetrics(),
        out=out,
        err=io.StringIO(),
    )
    assert code == 0, out.getvalue()
    async with running_worker(settings, metric_lines) as worker:
        await worker.wait_handled(2, outcome="processed")

    replayed = await DynamoQuarantineRepository(
        ingestion_tables.domain, dynamodb_client()
    ).get_quarantine_entry(TENANT, row.entry_key)
    assert replayed is not None and replayed.status == QuarantineEntryStatus.REPLAYED
    ((measurement, tags, _time), fields_), *rest = points(influx_bucket).items()
    assert rest == [] and measurement == OBSERVATION_MEASUREMENT
    tag_map = dict(tags)
    assert (tag_map["deployment_id"], tag_map["site_id"], tag_map["asset_id"]) == (
        DEPLOYMENT.deployment_id,
        DEPLOYMENT.site_id,
        DEPLOYMENT.asset_id,
    )
    assert fields_["event_id"] == observation_event_id(TENANT, COMPONENT, "water.temperature", at)
    assert fields_["received_at_ns"] == ns(first.received_at)
    assert "replay" in json.loads(fields_["quality_flags_json"])
    versions = row.mapping_versions
    assert (fields_["catalog_version"], fields_["quality_policy_version"]) == (
        versions.catalog_version,
        versions.quality_policy_version,
    )
    assert fields_["unit_mapping_version"] == versions.unit_mapping_version
    assert fields_["value"] == 22.5 and fields_["sequence"] == 7


# --- G9 ------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "case", ["integration_decommissioned", "account_revoked", "integration_mismatch", "suspended"]
)
async def test_gate9_terminal_fences_persist_rows_and_temporary_fences_retry(
    ingestion_tables: IngestionTables,
    ingestion_queue_pair: QueueFactory,
    influx_bucket: str,
    metric_lines: MetricLines,
    case: str,
) -> None:
    queues = ingestion_queue_pair(max_receive_count=8, visibility_seconds=30)
    settings = gate_settings(ingestion_tables, queues, influx_bucket)
    world = await kit_world(ingestion_tables)
    at = recent()
    observations = [reading(at, 20.0, sequence=1), reading(at + timedelta(seconds=1), 20.5)]
    async with composed_api(settings) as (_app, http):
        admitted = await post_batch(http, observations, key="g9-admitted")
        assert admitted.status_code == 202, admitted.text
        (body,) = drain_bodies(queues.source_url, expected=1)
        envelope = IngressEnvelope.model_validate_json(body)

        if case == "integration_decommissioned":
            await world.transition_integration(DeviceIntegrationStatus.DECOMMISSIONED)
            resend(queues.source_url, [body])
            later = await post_batch(http, observations, key="g9-later")
            assert (later.status_code, later.json()) == (403, {"detail": "forbidden"})
        elif case == "account_revoked":
            await world.transition_account(IntegrationAccountStatus.REVOKED)
            resend(queues.source_url, [body])
            later = await post_batch(http, observations, key="g9-later")
            assert (later.status_code, later.json()) == (401, {"detail": "unauthorized"})
        elif case == "integration_mismatch":
            # Another account of the same tenant cannot use this mapping (403) ...
            await world.subject.seed_account(
                integration_account(integration_account_id=OTHER_ACCOUNT)
            )
            await world.credentials.create_ingest_credential(
                credential(
                    ingest_credential_id=OTHER_CREDENTIAL, integration_account_id=OTHER_ACCOUNT
                ),
                AUDIT,
            )
            later = await post_batch(http, observations, key="g9-later", token=OTHER_TOKEN)
            assert (later.status_code, later.json()) == (403, {"detail": "forbidden"})
            # ... and a forged envelope naming another DI is fenced.
            forged = envelope.model_copy(update={"device_integration_id": OTHER_INTEGRATION})
            resend(queues.source_url, [forged.to_json()])
        else:
            await world.subject.seed_device(device(status=DeviceStatus.SUSPENDED, version=2))
            resend(queues.source_url, [body])

    if case == "suspended":
        async with running_worker(settings, metric_lines) as worker:
            await worker.wait_handled(2, outcome="retry", category="deferred")
            assert await quarantine_rows(ingestion_tables) == []
            assert points(influx_bucket) == {}
            assert sum(queue_counts(queues.dlq_url)) == 0
            await world.subject.seed_device(device(status=DeviceStatus.ACTIVE, version=3))
            await worker.wait_handled(1, outcome="processed")
        assert metric_lines.total("ingress_deferred_total", reason="device_not_active") >= 2
        assert len(points(influx_bucket)) == 2
        assert queue_counts(queues.source_url) == (0, 0)
        return

    async with running_worker(settings, metric_lines) as worker:
        await worker.wait_handled(1, outcome="fenced")
    assert queue_counts(queues.source_url) == (0, 0)
    rows = await quarantine_rows(ingestion_tables)
    assert sorted(r.kind.value for r in rows) == [
        "fenced_message",
        "fenced_observation",
        "fenced_observation",
    ]
    assert {r.reason for r in rows} == {f"fenced_{case}"}
    observation_rows = [r for r in rows if r.kind == QuarantineEntryKind.FENCED_OBSERVATION]
    assert all(r.replayable for r in observation_rows)
    assert sorted(
        SourceObservation.model_validate_json(r.source_observation_json or "").model_dump_json()
        for r in observation_rows
    ) == sorted(o.model_dump_json() for o in envelope.observations)
    assert points(influx_bucket) == {}
    state = await state_rows(ingestion_tables)
    assert state["component"] is None and state["device"] is None
    assert state["connector"]["last_fence_reason"] == case


# --- G10 -----------------------------------------------------------------------------------------


async def test_gate10_sequence_truth_table_end_to_end_and_marks_never_pass_the_first_receipt(
    ingestion_tables: IngestionTables,
    ingestion_queue_pair: QueueFactory,
    influx_bucket: str,
    metric_lines: MetricLines,
) -> None:
    queues = ingestion_queue_pair(max_receive_count=8, visibility_seconds=30)
    settings = gate_settings(ingestion_tables, queues, influx_bucket)
    await kit_world(ingestion_tables)
    t0 = recent()
    t1, t2, t3, t4 = (t0 + timedelta(seconds=s) for s in (10, 20, 30, 40))
    future = datetime.now(UTC).replace(microsecond=0) + timedelta(days=1)
    requests: list[tuple[list[tuple[datetime, int]], list[str]]] = [
        ([(t1, 1), (t2, 2)], ["first", "contiguous"]),
        ([(t3, 5)], ["gap"]),
        ([(t3, 5)], ["duplicate"]),
        ([(t4, 3)], ["regression"]),
        ([(t4, 4)], ["late"]),  # same observed_at, other sequence
        ([(t0, 9)], ["late"]),  # older than the observed mark: never a regression
    ]
    async with composed_api(settings) as (_app, http):
        bodies = []
        for index, (pairs, _events) in enumerate([*requests, ([(future, 100)], [])]):
            response = await post_batch(
                http,
                [reading(o, 20.0 + s, sequence=s) for o, s in pairs],
                key=f"g10-request-{index:04d}",
            )
            assert response.status_code == 202, response.text
            bodies.extend(drain_bodies(queues.source_url, expected=1))
    assert len(bodies) == len(requests) + 1

    async with running_worker(settings, metric_lines) as worker:
        for index, (_pairs, events) in enumerate(requests, start=1):
            resend(queues.source_url, [bodies[index - 1]])
            await worker.wait_handled(index, outcome="processed")
            observed = [
                labels["event"] for labels in metric_lines.labels("ingress_sequence_events_total")
            ]
            assert observed[-len(events) :] == events, (index, observed)
        receipts = [await receipt_of(ingestion_tables, f"g10-request-{i:04d}") for i in range(7)]
        mark_before_future = await state_rows(ingestion_tables)
        assert mark_before_future["component"]["last_observed_at"] == t4
        # Equal observed_at: the greater sequence wins the tie (D11), whatever the arrival order.
        assert mark_before_future["component"]["last_observed_sequence"] == 4

        # A future-skewed observation never pins the observed mark (it still lands as a point).
        resend(queues.source_url, [bodies[-1]])
        await worker.wait_handled(len(requests) + 1, outcome="processed")
        settled = await state_rows(ingestion_tables)
        assert (
            settled["component"]["last_observed_at"],
            settled["component"]["last_observed_sequence"],
        ) == (t4, 4)
        newest = max(r.received_at for r in receipts)
        assert settled["component"]["last_received_at"] == newest
        assert settled["device"]["last_received_at"] == newest

        # A DLQ-redriven old message (the same body) and a replay of it (newer observations,
        # replay=true) never move any mark beyond the first receipts.
        old = IngressEnvelope.model_validate_json(bodies[0])
        replay = old.model_copy(
            update={
                "ingress_id": "ing_" + "8" * 32,
                "replay": True,
                "replay_of": old.ingress_id,
                "observations": tuple(
                    o.model_copy(update={"observed_at": t4 + timedelta(seconds=5), "sequence": 50})
                    for o in old.observations[:1]
                ),
            }
        )
        resend(queues.source_url, [bodies[0], replay.to_json()])
        await worker.wait_handled(len(requests) + 3, outcome="processed")
    assert await state_rows(ingestion_tables) == settled


# --- G12 -----------------------------------------------------------------------------------------


def _raw_items(tables: IngestionTables, pk: str) -> list[dict[str, Any]]:
    client = dynamodb_client()
    response = client.query(
        TableName=tables.domain,
        KeyConditionExpression="PK = :pk",
        ExpressionAttributeValues={":pk": {"S": pk}},
        ConsistentRead=True,
    )
    return list(response.get("Items") or [])


async def test_gate12_no_secret_or_raw_identifier_in_logs_bodies_receipts_or_rows(
    ingestion_tables: IngestionTables,
    ingestion_queue_pair: QueueFactory,
    influx_bucket: str,
    metric_lines: MetricLines,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    caplog.set_level(logging.DEBUG, logger="limnopulse")
    queues = ingestion_queue_pair(max_receive_count=2, visibility_seconds=30)
    settings = gate_settings(ingestion_tables, queues, influx_bucket)
    await kit_world(ingestion_tables, deployment=False)
    key = "g12-raw-idempotency-key-0001"
    sentinel = "SENTINEL-g12-do-not-echo"
    at = recent()
    async with composed_api(settings) as (app, http):
        await post_batch(
            http,
            [reading(at, 1.0)],
            key="g12-bad-0001",
            token=ingest_token(CREDENTIAL, WRONG_SECRET),
        )
        invalid = await post_batch(
            http,
            [reading(at, 1.0, source_metadata={sentinel: sentinel}, metric_key=sentinel)],
            key="g12-invalid",
        )
        assert invalid.status_code == 422 and sentinel not in invalid.text
        accepted = await post_batch(
            http,
            [reading(at, 1.0, sequence=1), reading(at, 2.0, metric_key="water.unknown")],
            key=key,
        )
        assert accepted.status_code == 202, accepted.text
        app.state.ingress_queue_producer = SqsIngressQueue(
            client=sqs_client(), queue_url=queues.source_url + "-missing"
        )
        unavailable = await post_batch(http, [reading(at, 3.0)], key="g12-unavailable")
        assert unavailable.status_code == 503
    bodies = drain_bodies(queues.source_url, expected=1)
    resend(queues.source_url, [*bodies, "not json " + sentinel])
    async with running_worker(settings, metric_lines) as worker:
        await worker.wait_handled(1, outcome="processed")
        await worker.wait_for(
            lambda: sum(queue_counts(queues.dlq_url)) == 1, "the bad body in the DLQ"
        )

    digest = ingest_secret_sha256(SECRET)
    forbidden = (TOKEN, SECRET, digest, key, EXTERNAL_DEVICE, "Bearer ", WRONG_SECRET)
    receipts = [
        item for pk in (f"INGRESS_RECEIPT#{ACCOUNT}",) for item in _raw_items(ingestion_tables, pk)
    ]
    quarantine = _raw_items(ingestion_tables, f"QUARANTINE#{TENANT}")
    health = _raw_items(ingestion_tables, f"INTEGRATION_ACCOUNT#{ACCOUNT}")
    assert len(receipts) == 2 and len(quarantine) == 2 and health
    surfaces = {
        "logs": caplog.text,
        "queue bodies": "\n".join(bodies),
        "receipts": json.dumps(receipts),
        "quarantine rows": json.dumps(quarantine),
        "connector health": json.dumps(health),
        "metrics": json.dumps(metric_lines.records),
    }
    for surface, text in surfaces.items():
        for value in forbidden:
            assert value not in text, (surface, value[:6])
    assert caplog.records, "nothing was logged: the scan would be vacuous"
    for record in metric_lines.records:
        allowed = INGEST_METRICS[record["metric"]]
        assert set(record["labels"]) == set(allowed), record
        for label, value in record["labels"].items():
            assert value in allowed[label], record


# --- G15 -----------------------------------------------------------------------------------------


def _count(bucket: str, start: datetime, stop: datetime, measurement: str, field_: str) -> int:
    flux = (
        f'from(bucket: "{bucket}") |> range(start: {iso(start)}, stop: {iso(stop)}) '
        f'|> filter(fn: (r) => r._measurement == "{measurement}" and r._field == "{field_}" '
        f'and r.tenant_id == "{LEGACY_TENANT}") |> group() |> count()'
    )
    with influx_clients(bucket) as (query_api, _write_api):
        tables = query_api.query(flux, org=influx_settings().org)
    return sum(int(record.get_value()) for table in tables for record in table.records)


async def test_gate15_https_path_matches_the_independent_legacy_writer(
    ingestion_tables: IngestionTables,
    ingestion_queue_pair: QueueFactory,
    influx_bucket: str,
    metric_lines: MetricLines,
) -> None:
    queues = ingestion_queue_pair(max_receive_count=8, visibility_seconds=30)
    # D16: legacy dual write OFF, so `water_quality` comes only from the legacy writer.
    settings = gate_settings(ingestion_tables, queues, influx_bucket, legacy_dual_write=False)
    world = await legacy_world(settings)
    readings = legacy_readings(world.seeded_at, 6)
    await asyncio.sleep(max(0.0, (readings[-1][0] - datetime.now(UTC)).total_seconds()) + 0.1)

    async with composed_api(settings) as (_app, http):
        await post_legacy_readings(http, world, readings, "g15")
    async with running_worker(settings, metric_lines) as worker:
        await worker.wait_handled(len(readings), outcome="processed")
    assert await quarantine_rows(ingestion_tables, LEGACY_TENANT) == []
    start, stop = readings[0][0], readings[-1][0] + timedelta(seconds=1)
    # With legacy dual write off the worker wrote no `water_quality` point at all ...
    assert _count(influx_bucket, start, stop, LEGACY_MEASUREMENT, "temp_c") == 0
    assert _count(influx_bucket, start, stop, OBSERVATION_MEASUREMENT, "value") == 7 * len(readings)

    # ... so `water_quality` comes only from the independent legacy writer (Telegraf's shape).
    write_lines(influx_bucket, [telegraf_line(item) for item in readings])
    assert _count(influx_bucket, start, stop, LEGACY_MEASUREMENT, "temp_c") == len(readings)

    # The P2 runbook's keyed checks over [now - 2 h, now], which covers [min, max] observed_at.
    now = datetime.now(UTC) + timedelta(seconds=1)
    assert now - timedelta(hours=2) < start and stop <= now + timedelta(seconds=1)
    for name in ("points", "latest", "summary"):
        assert run_divergence_check(SimpleNamespace(bucket=influx_bucket), name, now) == [], name
