"""Entrypoint and composition of the telemetry ingest worker (P3-10, D7).

Run as `python -m limnopulse_api.workers.telemetry_ingest` from the API image (an entrypoint
override; no new image). It has its own composition, never `create_app`, so the API's optional
cache client is never built, and it runs as the dedicated `telemetry_ingest` principal.

- **Start guard.** `main()` exits 2 unless `TELEMETRY_V2_WRITE_ENABLED=true` and
  `SQS_TELEMETRY_INGRESS_QUEUE_URL` is set: a worker that cannot write must never delete. Outside
  local/test it also exits 2 when its own AWS keys or InfluxDB write token are empty.
- **Shutdown.** SIGTERM and SIGINT set the loop's `stop`; the P3-08 loop drains in-flight
  messages for `TELEMETRY_INGEST_DRAIN_SECONDS`. The InfluxDB client is closed on every exit:
  a stop, a crash, or a composition failure after it was created.
- **Timeouts.** botocore connect and read timeouts stay finite, because the drain waits for an
  in-flight heartbeat or final action to return; the SQS read timeout outlasts the long poll.

Log lines carry closed categories only, never a setting value or an exception message.
"""

import asyncio
import logging
import os
import signal
import sys
from collections.abc import Callable
from typing import Any

import boto3
from botocore.config import Config
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from pydantic import ValidationError

from limnopulse_api.adapters.aws_clients import dynamodb_client_kwargs, sqs_client_kwargs
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

__all__ = ["build_worker", "dynamodb_client_kwargs", "main", "sqs_client_kwargs"]

logger = logging.getLogger("limnopulse.telemetry.ingest")

EXIT_REFUSED = 2
# Shutdown budget (A47): after SIGTERM an in-flight long poll may run `wait + 5` s, then the
# drain gets `drain_seconds`, and the loop still waits for any boto3 call already in flight
# (heartbeat, delete, visibility change or a DynamoDB request). Each client's worst single call
# is bounded below, and SHUTDOWN_CALL_BUDGET_SECONDS is the larger of the two, so
# `wait + 5 + drain + SHUTDOWN_CALL_BUDGET_SECONDS` must stay below the compose stop grace.
_CONNECT_TIMEOUT_SECONDS = 3
# SQS: no botocore retry (the loop retries a failed poll, and a failed heartbeat, delete or
# visibility change only means a later redelivery). The read timeout must outlast the long poll.
_SQS_READ_TIMEOUT_MARGIN_SECONDS = 5
_SQS_RETRIES = {"mode": "standard", "max_attempts": 1}
# DynamoDB: one bounded retry for throttling or a transient error.
_DYNAMODB_READ_TIMEOUT_SECONDS = 10
_DYNAMODB_RETRIES = {"mode": "standard", "max_attempts": 2}


def shutdown_call_budget_seconds(settings: Settings) -> int:
    """The longest a single in-flight SQS or DynamoDB call can take, retries included."""
    sqs = _SQS_RETRIES["max_attempts"] * (
        _CONNECT_TIMEOUT_SECONDS
        + settings.telemetry_ingest_worker_wait_seconds
        + _SQS_READ_TIMEOUT_MARGIN_SECONDS
    )
    dynamodb = _DYNAMODB_RETRIES["max_attempts"] * (
        _CONNECT_TIMEOUT_SECONDS + _DYNAMODB_READ_TIMEOUT_SECONDS
    )
    return max(sqs, dynamodb)


_LOCAL_ENVIRONMENTS = {"local", "test"}


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
            connect_timeout=_CONNECT_TIMEOUT_SECONDS,
            read_timeout=_DYNAMODB_READ_TIMEOUT_SECONDS,
            retries=_DYNAMODB_RETRIES,
        ),
    )
    sqs = client_factory(
        "sqs",
        **sqs_client_kwargs(settings),
        config=Config(
            connect_timeout=_CONNECT_TIMEOUT_SECONDS,
            read_timeout=settings.telemetry_ingest_worker_wait_seconds
            + _SQS_READ_TIMEOUT_MARGIN_SECONDS,
            retries=_SQS_RETRIES,
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
    influx_clients: list[Any] = []

    def open_influx(**kwargs: Any) -> Any:
        # Looked up at call time, and kept so the client is closed on every exit (#116).
        client = InfluxDBClient(**kwargs)
        influx_clients.append(client)
        return client

    try:
        return await _run_worker(settings, open_influx)
    finally:
        for client in influx_clients:
            try:
                client.close()
            except Exception as error:  # noqa: BLE001 - closing must not mask the exit code
                logger.warning("influx client close failed (%s)", type(error).__name__)


async def _run_worker(settings: Settings, open_influx: Callable[..., Any]) -> int:
    try:
        worker = build_worker(settings, influx_client_factory=open_influx)
    except Exception as error:  # noqa: BLE001 - by category only, never the message
        logger.error("telemetry ingest worker could not be composed (%s)", type(error).__name__)
        return 1
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


def _has_own_credentials(settings: Settings) -> bool:
    return all(
        (
            os.environ.get("AWS_ACCESS_KEY_ID", "").strip(),
            os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip(),
            settings.influxdb_token.strip(),
        )
    )


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
    if settings.app_env not in _LOCAL_ENVIRONMENTS and not _has_own_credentials(settings):
        # Compose interpolates a missing variable to "": fail fast rather than poll forever
        # with NoCredentialsError while the API keeps answering 202.
        logger.error(
            "telemetry ingest worker refused to start: its AWS credentials or InfluxDB write "
            "token are empty"
        )
        return EXIT_REFUSED
    return asyncio.run(_run(settings))


if __name__ == "__main__":
    sys.exit(main())
