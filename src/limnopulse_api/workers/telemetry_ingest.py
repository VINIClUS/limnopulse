"""Entrypoint and composition of the telemetry ingest worker (P3-10, D7).

Run as `python -m limnopulse_api.workers.telemetry_ingest` from the API image (an entrypoint
override; no new image). It has its own composition, never `create_app`, so the API's optional
cache client is never built, and it runs as the dedicated `telemetry_ingest` principal.

- **Start guard.** `main()` exits 2 unless `TELEMETRY_V2_WRITE_ENABLED=true` and
  `SQS_TELEMETRY_INGRESS_QUEUE_URL` is set: a worker that cannot write must never delete.
- **Shutdown.** SIGTERM and SIGINT set the loop's `stop`; the P3-08 loop drains in-flight
  messages for `TELEMETRY_INGEST_DRAIN_SECONDS`.
- **Timeouts.** botocore connect and read timeouts stay finite, because the drain waits for an
  in-flight heartbeat or final action to return; the SQS read timeout outlasts the long poll.

Log lines carry closed categories only, never a setting value or an exception message.
"""

import asyncio
import logging
import signal
import sys
from collections.abc import Callable
from typing import Any

import boto3
from botocore.config import Config
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from pydantic import ValidationError

from limnopulse_api.adapters.dynamodb_deployments import DynamoDeploymentRepository
from limnopulse_api.adapters.dynamodb_devices_v2 import DynamoDeviceComponentRepository
from limnopulse_api.adapters.dynamodb_ingest_state import DynamoIngestStateRepository
from limnopulse_api.adapters.dynamodb_integrations import DynamoIntegrationRepository
from limnopulse_api.adapters.dynamodb_quarantine import DynamoQuarantineRepository
from limnopulse_api.adapters.dynamodb_sites_assets import DynamoSiteAssetRepository
from limnopulse_api.adapters.dynamodb_v2 import utc_now
from limnopulse_api.adapters.influxdb_v2_writer import InfluxObservationWriter
from limnopulse_api.adapters.sqs_ingestion import SqsIngressQueue
from limnopulse_api.adapters.telemetry_metrics_log import StructuredLogTelemetryMetrics
from limnopulse_api.core.config import Settings
from limnopulse_api.services.ingest_processor import IngestMessageProcessor, RetryPolicy
from limnopulse_api.services.ingest_state import IngestStateService
from limnopulse_api.services.telemetry_normalizer import TelemetryNormalizer
from limnopulse_api.services.telemetry_pipeline import TelemetryPipeline
from limnopulse_api.services.telemetry_v2_writes import ObservationWriteService
from limnopulse_api.services.trusted_mapping import TrustedMappingService
from limnopulse_api.workers.telemetry_ingest_loop import IngestWorkerConfig, IngestWorkerLoop

logger = logging.getLogger("limnopulse.telemetry.ingest")

EXIT_REFUSED = 2
_CONNECT_TIMEOUT_SECONDS = 10
_DYNAMODB_READ_TIMEOUT_SECONDS = 30
# Added to the long-poll wait, so an idle receive returns well before the read times out.
_SQS_READ_TIMEOUT_MARGIN_SECONDS = 20
_LOCAL_ENVIRONMENTS = {"local", "test"}


def dynamodb_client_kwargs(settings: Settings) -> dict[str, str]:
    kwargs: dict[str, str] = {"region_name": settings.aws_region}
    if settings.dynamodb_endpoint_url is not None:
        kwargs["endpoint_url"] = settings.dynamodb_endpoint_url
        if settings.app_env in _LOCAL_ENVIRONMENTS:
            kwargs["aws_access_key_id"] = "local"
            kwargs["aws_secret_access_key"] = "local"
    return kwargs


def sqs_client_kwargs(settings: Settings) -> dict[str, str]:
    """Mirrors the DynamoDB kwargs: dummy credentials only for a local endpoint in local/test."""
    kwargs: dict[str, str] = {"region_name": settings.aws_region}
    if settings.sqs_endpoint_url is not None:
        kwargs["endpoint_url"] = settings.sqs_endpoint_url
        if settings.app_env in _LOCAL_ENVIRONMENTS:
            kwargs["aws_access_key_id"] = "local"
            kwargs["aws_secret_access_key"] = "local"
    return kwargs


def build_worker(
    settings: Settings,
    *,
    client_factory: Callable[..., Any] = boto3.client,
    influx_client_factory: Callable[..., Any] = InfluxDBClient,
) -> IngestWorkerLoop:
    if not settings.telemetry_v2_write_enabled or not settings.sqs_telemetry_ingress_queue_url:
        raise ValueError("the telemetry ingest worker needs v2 writes and the ingress queue")
    dynamodb = client_factory(
        "dynamodb",
        **dynamodb_client_kwargs(settings),
        config=Config(
            connect_timeout=_CONNECT_TIMEOUT_SECONDS, read_timeout=_DYNAMODB_READ_TIMEOUT_SECONDS
        ),
    )
    sqs = client_factory(
        "sqs",
        **sqs_client_kwargs(settings),
        config=Config(
            connect_timeout=_CONNECT_TIMEOUT_SECONDS,
            read_timeout=settings.telemetry_ingest_worker_wait_seconds
            + _SQS_READ_TIMEOUT_MARGIN_SECONDS,
        ),
    )
    influxdb = influx_client_factory(
        url=settings.influxdb_url, token=settings.influxdb_token, org=settings.influxdb_org
    )
    # A synchronous write API: the batching default would acknowledge failed writes.
    writer = InfluxObservationWriter(
        write_api=influxdb.write_api(write_options=SYNCHRONOUS),
        bucket=settings.influxdb_bucket_raw,
        org=settings.influxdb_org,
    )
    table = settings.dynamodb_domain_table
    devices = DynamoDeviceComponentRepository(table, dynamodb)
    integrations = DynamoIntegrationRepository(table, dynamodb)
    legacy_dual_write = settings.telemetry_legacy_dual_write_enabled
    pipeline = TelemetryPipeline(
        normalizer=TelemetryNormalizer(
            components=devices,
            deployments=DynamoDeploymentRepository(table, dynamodb),
            clock=utc_now,
        ),
        write_service=ObservationWriteService(
            writer=writer,
            legacy_writer=writer if legacy_dual_write else None,
            site_assets=DynamoSiteAssetRepository(table, dynamodb),
            v2_write_enabled=settings.telemetry_v2_write_enabled,
            legacy_dual_write_enabled=legacy_dual_write,
        ),
    )
    metrics = StructuredLogTelemetryMetrics()
    processor = IngestMessageProcessor(
        mapping=TrustedMappingService(
            resolver=integrations,
            integrations=integrations,
            devices=devices,
            components=devices,
        ),
        pipeline=pipeline,
        quarantine=DynamoQuarantineRepository(table, dynamodb),
        state=IngestStateService(DynamoIngestStateRepository(table, dynamodb)),
        metrics=metrics,
        clock=utc_now,
        retry=RetryPolicy(
            base=settings.telemetry_ingest_retry_base_seconds,
            cap=settings.telemetry_ingest_retry_cap_seconds,
        ),
    )
    return IngestWorkerLoop(
        consumer=SqsIngressQueue(client=sqs, queue_url=settings.sqs_telemetry_ingress_queue_url),
        processor=processor,
        config=IngestWorkerConfig(
            max_messages=settings.telemetry_ingest_worker_max_messages,
            wait_seconds=settings.telemetry_ingest_worker_wait_seconds,
            visibility_seconds=settings.telemetry_ingest_worker_visibility_seconds,
            concurrency=settings.telemetry_ingest_worker_concurrency,
            drain_seconds=settings.telemetry_ingest_drain_seconds,
            processing_deadline_seconds=settings.telemetry_ingest_processing_deadline_seconds,
        ),
        metrics=metrics,
        clock=utc_now,
    )


async def _run(settings: Settings) -> int:
    worker = build_worker(settings)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    signals = (signal.SIGTERM, signal.SIGINT)
    for signum in signals:
        loop.add_signal_handler(signum, stop.set)
    try:
        logger.info("telemetry ingest worker started")
        await worker.run(stop)
    except Exception as error:  # noqa: BLE001 - a crash exits non-zero, by category only
        logger.error("telemetry ingest worker crashed (%s)", type(error).__name__)
        return 1
    finally:
        for signum in signals:
            loop.remove_signal_handler(signum)
    logger.info("telemetry ingest worker stopped")
    return 0


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        settings = Settings()
    except ValidationError as error:
        # `hide_input_in_errors`: the count only, never a value.
        logger.error(
            "telemetry ingest worker refused to start: %d invalid settings", error.error_count()
        )
        return EXIT_REFUSED
    if not settings.telemetry_v2_write_enabled:
        logger.error("telemetry ingest worker refused to start: TELEMETRY_V2_WRITE_ENABLED is off")
        return EXIT_REFUSED
    if not settings.sqs_telemetry_ingress_queue_url:
        logger.error(
            "telemetry ingest worker refused to start: SQS_TELEMETRY_INGRESS_QUEUE_URL is empty"
        )
        return EXIT_REFUSED
    return asyncio.run(_run(settings))


if __name__ == "__main__":
    sys.exit(main())
