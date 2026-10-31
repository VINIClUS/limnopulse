"""P3-08 end to end on ElasticMQ, DynamoDB Local and InfluxDB 2.7.

The real worker stack: `SqsIngressQueue` -> `IngestWorkerLoop` -> `IngestMessageProcessor` over
the P3-04 `TrustedMappingService`, the P2 `TelemetryPipeline` (Dynamo component and Deployment
reads, the Influx writer), the P3-06 quarantine and ingest-state adapters.

Skipped unless `RUN_INGESTION_INTEGRATION=1` and `RUN_INFLUX_INTEGRATION=1`
(`make verify-ingestion`, with `scripts/dev/run_local_influxd.sh` running).
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from influxdb_client import InfluxDBClient

from limnopulse_api.adapters.dynamodb_deployments import (
    DynamoDeploymentRepository,
    deployment_to_item,
)
from limnopulse_api.adapters.dynamodb_ingest_state import DynamoIngestStateRepository
from limnopulse_api.adapters.dynamodb_quarantine import DynamoQuarantineRepository
from limnopulse_api.adapters.dynamodb_v2 import device_ingest_state_from_item
from limnopulse_api.adapters.influxdb_v2_writer import InfluxObservationWriter
from limnopulse_api.adapters.sqs_ingestion import SqsIngressQueue
from limnopulse_api.domain.deployments import Deployment
from limnopulse_api.domain.ingestion import (
    IngressEnvelope,
    QuarantineEntryKind,
    build_trusted_context,
)
from limnopulse_api.domain.integrations import DeviceIntegrationStatus
from limnopulse_api.domain.telemetry_v2 import QuarantineReason
from limnopulse_api.domain.telemetry_v2_schema import OBSERVATION_MEASUREMENT
from limnopulse_api.services.ingest_processor import IngestMessageProcessor, RetryPolicy
from limnopulse_api.services.ingest_state import IngestStateService
from limnopulse_api.services.telemetry_normalizer import TelemetryNormalizer
from limnopulse_api.services.telemetry_pipeline import TelemetryPipeline
from limnopulse_api.services.telemetry_v2_writes import ObservationWriteService
from limnopulse_api.workers.telemetry_ingest_loop import IngestWorkerConfig, IngestWorkerLoop
from tests.support.influx_harness import influx_clients, influx_settings
from tests.support.ingestion_contract_kits import (
    ACCOUNT,
    COMPONENT,
    CREATED,
    DEVICE,
    OBSERVED,
    RECEIVED,
    TENANT,
    component,
    device,
    device_integration,
    envelope,
    integration_account,
    source_observation,
)
from tests.support.ingestion_fakes import RecordingTelemetryMetrics
from tests.support.ingestion_harness import (
    IngestionQueuePair,
    IngestionTables,
    dynamodb_client,
    sqs_client,
    table_pair,
)
from tests.unit.test_dynamodb_integration_resolver import DynamoIngestionSubject
from tests.unit.test_trusted_mapping import with_mapping

pytestmark = pytest.mark.asyncio

TIME_MIN = "1970-01-01T00:00:00Z"
TIME_MAX = "2262-01-01T00:00:00Z"
GUARD_SECONDS = 60
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
WORKER_CONFIG = {
    "max_messages": 10,
    "wait_seconds": 1,
    "visibility_seconds": 30,
    "concurrency": 4,
    "drain_seconds": 10,
    "processing_deadline_seconds": 30,
}


def reading(**overrides: Any) -> Any:
    payload: dict[str, Any] = {
        "component_id": COMPONENT,
        "metric_key": "water.temperature",
        "source_unit": "degC",
    }
    payload.update(overrides)
    return source_observation(**payload)


@dataclass
class Stack:
    subject: DynamoIngestionSubject
    tables: IngestionTables
    queue_url: str
    bucket: str
    write_api: Any
    metrics: RecordingTelemetryMetrics
    queue: SqsIngressQueue
    quarantine: DynamoQuarantineRepository
    state: DynamoIngestStateRepository

    def pipeline(self) -> TelemetryPipeline:
        client = dynamodb_client()
        return TelemetryPipeline(
            normalizer=TelemetryNormalizer(
                components=self.subject.devices,
                deployments=DynamoDeploymentRepository(self.tables.domain, client),
                clock=lambda: datetime.now(UTC),
            ),
            write_service=ObservationWriteService(
                writer=InfluxObservationWriter(
                    write_api=self.write_api, bucket=self.bucket, org=influx_settings().org
                ),
                legacy_writer=None,
                site_assets=None,  # type: ignore[arg-type] - unused without legacy dual-write
                v2_write_enabled=True,
                legacy_dual_write_enabled=False,
            ),
        )

    def loop(self) -> IngestWorkerLoop:
        processor = IngestMessageProcessor(
            mapping=self.subject.mapping,
            pipeline=self.pipeline(),
            quarantine=self.quarantine,
            state=IngestStateService(self.state),
            metrics=self.metrics,
            clock=lambda: datetime.now(UTC),
            retry=RetryPolicy(),
        )
        return IngestWorkerLoop(
            consumer=self.queue,
            processor=processor,
            config=IngestWorkerConfig(**WORKER_CONFIG),
            metrics=self.metrics,
            clock=lambda: datetime.now(UTC),
        )

    def handled(self) -> int:
        return sum(
            record["value"]
            for record in self.metrics.records
            if record["metric"] == "ingress_messages_total"
        )

    async def run_until_handled(self, count: int) -> None:
        stop = asyncio.Event()
        task = asyncio.create_task(self.loop().run(stop))
        try:
            deadline = asyncio.get_running_loop().time() + GUARD_SECONDS
            while self.handled() < count:
                assert asyncio.get_running_loop().time() < deadline, "worker did not finish"
                assert not task.done(), "the worker loop exited early"
                await asyncio.sleep(0.05)
        finally:
            stop.set()
            await asyncio.wait_for(task, GUARD_SECONDS)

    async def send(self, envelopes: Sequence[IngressEnvelope]) -> None:
        for item in envelopes:
            await self.queue.send(item)

    def queue_counts(self, url: str | None = None) -> tuple[int, int]:
        attributes = sqs_client().get_queue_attributes(
            QueueUrl=url or self.queue_url,
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
        )["Attributes"]
        return (
            int(attributes["ApproximateNumberOfMessages"]),
            int(attributes["ApproximateNumberOfMessagesNotVisible"]),
        )

    async def quarantine_rows(self) -> list[Any]:
        page = await self.quarantine.list_quarantine_entries(TENANT, after_sk=None, limit=100)
        return list(page.entries)

    async def state_rows(self) -> dict[str, Any]:
        components = await self.state.get_component_states(TENANT, [COMPONENT])
        item = self.subject.item(self.subject.integrations.keys.device_ingest_state(DEVICE))
        health = await self.state.get_connector_health(TENANT, ACCOUNT)
        return {
            "component": None
            if COMPONENT not in components
            else components[COMPONENT].model_dump(exclude={"last_ingested_at"}),
            "device": None
            if item is None
            else device_ingest_state_from_item(item).model_dump(exclude={"last_ingested_at"}),
            "connector": None if health is None else health.model_dump(),
        }


async def seed(subject: DynamoIngestionSubject) -> None:
    await subject.seed_account(integration_account())
    await subject.seed_device_integration(device_integration())
    await subject.seed_device(device())
    await subject.seed_component(component())
    for form in ("mirror", "history", "current"):
        subject.put(deployment_to_item(DEPLOYMENT, form=form))


@contextmanager
def stack(tables: IngestionTables, queues: IngestionQueuePair, bucket: str) -> Iterator[Stack]:
    client = dynamodb_client()
    subject = with_mapping(
        DynamoIngestionSubject(client, domain_table=tables.domain, audit_table=tables.audit)
    )
    with influx_clients(bucket) as (_query_api, write_api):
        yield Stack(
            subject=subject,
            tables=tables,
            queue_url=queues.source_url,
            bucket=bucket,
            write_api=write_api,
            metrics=RecordingTelemetryMetrics(),
            queue=SqsIngressQueue(client=sqs_client(), queue_url=queues.source_url),
            quarantine=DynamoQuarantineRepository(tables.domain, client),
            state=DynamoIngestStateRepository(tables.domain, client),
        )


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


def points(bucket: str) -> dict[tuple[Any, ...], dict[str, Any]]:
    """Points keyed by (measurement, tag set, time); every field except `ingested_at_ns`."""
    settings = influx_settings()
    flux = (
        f'from(bucket: "{bucket}") |> range(start: {TIME_MIN}, stop: {TIME_MAX}) '
        f'|> filter(fn: (r) => r._measurement == "{OBSERVATION_MEASUREMENT}")'
    )
    with influx_clients(bucket) as (query_api, _write_api):
        tables = query_api.query(flux, org=settings.org)
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


def first_envelope() -> IngressEnvelope:
    return envelope(
        observations=[
            reading(sequence=1),
            reading(sequence=1, metric_key="water.unknown"),
            reading(sequence=2, value=28.25, observed_at=OBSERVED + timedelta(seconds=30)),
        ]
    )


def numbered_envelopes(count: int) -> list[IngressEnvelope]:
    return [
        envelope(
            ingress_id="ing_" + f"{index:x}" * 32,
            received_at=RECEIVED + timedelta(seconds=index),
            observations=[
                reading(
                    sequence=10 + index,
                    value=20.0 + index,
                    observed_at=OBSERVED + timedelta(minutes=index),
                ),
                *(
                    [reading(metric_key="water.unknown", observed_at=OBSERVED)]
                    if index == 2
                    else []
                ),
            ],
        )
        for index in range(1, count + 1)
    ]


async def test_an_envelope_on_a_real_queue_is_written_quarantined_marked_and_deleted(
    ingestion_tables: IngestionTables,
    ingestion_queue_pair: Any,
    influx_bucket: str,
    control_bucket: str,
) -> None:
    queues = ingestion_queue_pair(max_receive_count=8, visibility_seconds=30)
    item = first_envelope()
    with stack(ingestion_tables, queues, influx_bucket) as worker:
        await seed(worker.subject)
        await worker.send([item])

        await worker.run_until_handled(1)

        assert worker.queue_counts() == (0, 0)  # deleted, not merely hidden
        assert worker.queue_counts(queues.dlq_url) == (0, 0)
        rows = await worker.quarantine_rows()
        state = await worker.state_rows()

    # The P2 pipeline baseline: the same observations, the same context, directly.
    with stack(ingestion_tables, queues, control_bucket) as baseline:
        outcome = await baseline.pipeline().ingest(
            build_trusted_context(item, device_integration()), item.observations
        )
    expected = points(control_bucket)
    assert outcome.written.v2_points == 2
    assert len(expected) == 2
    assert points(influx_bucket) == expected

    (row,) = rows
    assert row.kind == QuarantineEntryKind.OBSERVATION
    assert row.reason == QuarantineReason.METRIC_UNKNOWN.value
    assert (row.ingress_id, row.mapping_versions) == (item.ingress_id, item.mapping_versions)
    assert row.replayable is True

    assert state["component"]["last_received_at"] == RECEIVED
    assert state["component"]["last_observed_at"] == OBSERVED + timedelta(seconds=30)
    assert state["component"]["last_observed_sequence"] == 2
    assert state["device"]["last_received_at"] == RECEIVED
    assert state["connector"]["last_accepted_at"] == RECEIVED
    assert state["connector"]["last_quarantine_reason"] == "metric_unknown"
    labels = [
        r["labels"] for r in worker.metrics.records if r["metric"] == "ingress_messages_total"
    ]
    assert labels == [{"outcome": "processed", "category": "processed"}]


async def test_a_decommissioned_integration_fences_the_queued_message(
    ingestion_tables: IngestionTables, ingestion_queue_pair: Any, influx_bucket: str
) -> None:
    queues = ingestion_queue_pair(max_receive_count=8, visibility_seconds=30)
    item = envelope(observations=[reading(sequence=1), reading(sequence=2, value=1.5)])
    with stack(ingestion_tables, queues, influx_bucket) as worker:
        await seed(worker.subject)
        await worker.send([item])
        # Decommissioned after the 202 and before consumption (ADR-001).
        await worker.subject.seed_device_integration(
            device_integration(status=DeviceIntegrationStatus.DECOMMISSIONED)
        )

        await worker.run_until_handled(1)

        assert worker.queue_counts() == (0, 0)
        rows = await worker.quarantine_rows()
        state = await worker.state_rows()

    assert points(influx_bucket) == {}
    kinds = sorted(row.kind.value for row in rows)
    assert kinds == ["fenced_message", "fenced_observation", "fenced_observation"]
    assert {row.reason for row in rows} == {"fenced_integration_decommissioned"}
    assert all(row.replayable for row in rows if row.kind == QuarantineEntryKind.FENCED_OBSERVATION)
    assert state["component"] is None and state["device"] is None
    assert state["connector"]["last_fence_reason"] == "integration_decommissioned"


async def test_duplicate_and_shuffled_delivery_converge(
    ingestion_tables: IngestionTables,
    ingestion_queue_pair: Any,
    influx_bucket: str,
    control_bucket: str,
) -> None:
    envelopes = numbered_envelopes(4)
    ordered_queues = ingestion_queue_pair(max_receive_count=8, visibility_seconds=30)
    shuffled_queues = ingestion_queue_pair(max_receive_count=8, visibility_seconds=30)

    with stack(ingestion_tables, ordered_queues, influx_bucket) as ordered:
        await seed(ordered.subject)
        await ordered.send(envelopes)
        await ordered.run_until_handled(len(envelopes))
        assert ordered.queue_counts() == (0, 0)
        ordered_state = await ordered.state_rows()
        ordered_rows = {(r.entry_key, r.reason) for r in await ordered.quarantine_rows()}

    deliveries = [*envelopes, *envelopes]
    random.Random(8).shuffle(deliveries)
    with (
        table_pair(dynamodb_client()) as other_tables,
        stack(other_tables, shuffled_queues, control_bucket) as shuffled,
    ):
        await seed(shuffled.subject)
        await shuffled.send(deliveries)
        await shuffled.run_until_handled(len(deliveries))
        assert shuffled.queue_counts() == (0, 0)
        shuffled_state = await shuffled.state_rows()
        shuffled_rows = {(r.entry_key, r.reason) for r in await shuffled.quarantine_rows()}

    ordered_points = points(influx_bucket)
    assert len(ordered_points) == 4
    assert points(control_bucket) == ordered_points
    assert shuffled_state == ordered_state
    assert ordered_state["component"]["last_observed_sequence"] == 14
    assert ordered_state["device"]["last_received_at"] == RECEIVED + timedelta(seconds=4)
    assert shuffled_rows == ordered_rows and len(ordered_rows) == 1
