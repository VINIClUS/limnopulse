"""Entrypoint and composition of the telemetry ingest worker (P3-10, D7; ADR-020).

Run as `python -m limnopulse_api.workers.telemetry_ingest` from the API image (an entrypoint
override; no new image). It has its own composition, never `create_app`, so the API's optional
cache client is never built, and it runs as the dedicated `telemetry_ingest` principal.

- **Exit codes (ADR-020).** 0 only after a stop signal; 1 for a runtime failure (a composition
  failure, a crash, a loop that ends without a stop, or a stall); 78 for a configuration error.
  Compose parks the container on 78 instead of restarting it.
- **Start guard.** `main()` exits 78 unless `TELEMETRY_V2_WRITE_ENABLED=true` and
  `SQS_TELEMETRY_INGRESS_QUEUE_URL` is set: a worker that cannot write must never delete. Outside
  local/test it also exits 78 when its own AWS keys or InfluxDB write token are empty. Invalid
  settings and unknown arguments exit 78 too.
- **Liveness (ADR-020).** The loop marks a stamp (`telemetry_ingest_liveness`) every turn. A
  watchdog thread cancels a loop that stays silent for `stall_after_seconds(settings)` and exits 1;
  if the process still runs after the A47 stop budget it forces `os._exit(1)`. `healthcheck`
  answers from the stamp alone, before any setting is read.
- **Shutdown.** SIGTERM and SIGINT disarm the watchdog and set the loop's `stop`; the P3-08 loop
  drains in-flight messages for `TELEMETRY_INGEST_DRAIN_SECONDS`. The InfluxDB client is closed
  on every exit: a stop, a crash, a stall, or a composition failure after it was created.
- **Timeouts.** botocore connect and read timeouts and the InfluxDB timeout stay finite, because
  the drain and the stall threshold wait for in-flight calls to return; the SQS read timeout
  outlasts the long poll.

Log lines carry closed categories only, never a setting value or an exception message.
"""

# ruff: noqa: E402
# The healthcheck fast path below must run before the worker's imports.

import sys

from limnopulse_api.workers import telemetry_ingest_liveness as _liveness

if __name__ == "__main__" and sys.argv[1:] == [_liveness.HEALTHCHECK]:
    # The compose healthcheck runs inside the worker's 0.5-CPU limit: answer it before the
    # imports below, which cost about 1.6 s of CPU and 70 MB.
    sys.exit(_liveness.healthcheck())

import asyncio
import logging
import os
import signal
from collections.abc import Callable, Sequence
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
from limnopulse_api.workers.telemetry_ingest_liveness import (
    HEALTHCHECK,
    LivenessStamp,
    LivenessWatchdog,
    healthcheck,
    liveness_path,
)
from limnopulse_api.workers.telemetry_ingest_loop import (
    _POLL_GRACE_SECONDS,
    IngestWorkerConfig,
    IngestWorkerLoop,
    no_liveness,
)

__all__ = ["build_worker", "dynamodb_client_kwargs", "main", "sqs_client_kwargs"]

logger = logging.getLogger("limnopulse.telemetry.ingest")

# ADR-020 exit codes, shared by every worker role (2, a partial one-shot run, is never used here).
EXIT_STOPPED = 0
EXIT_FAILURE = 1
EXIT_CONFIG = 78
# Shutdown budget (A47): after SIGTERM an in-flight long poll may run `wait + 5` s, then the
# drain gets `drain_seconds`, and the loop still waits for any boto3 call already in flight
# (heartbeat, delete, visibility change or a DynamoDB request). Each client's worst single call
# is bounded below, and `shutdown_call_budget_seconds(settings)` is the larger of the two, so
# `wait + 5 + drain + shutdown_call_budget_seconds(settings)` must stay below the stop grace.
# The retries use `total_max_attempts`, the attempts in all: botocore's `max_attempts` counts
# retries, so `max_attempts: N` would make N + 1 attempts.
_CONNECT_TIMEOUT_SECONDS = 3
# SQS: no botocore retry (the loop retries a failed poll, and a failed heartbeat, delete or
# visibility change only means a later redelivery). The read timeout must outlast the long poll.
_SQS_READ_TIMEOUT_MARGIN_SECONDS = 5
_SQS_RETRIES = {"mode": "standard", "total_max_attempts": 1}
# DynamoDB: one bounded retry for throttling or a transient error.
_DYNAMODB_READ_TIMEOUT_SECONDS = 10
_DYNAMODB_RETRIES = {"mode": "standard", "total_max_attempts": 2}
# InfluxDB: the client's own 10 s default (a total per request, no retry), pinned so the stall
# threshold below can count it.
INFLUX_TIMEOUT_SECONDS = 10
# How often the liveness watchdog looks at the stamp.
_WATCHDOG_CHECK_SECONDS = 5.0
# The watchdog's last resort after a stall; tests replace it.
_force_exit = os._exit


def _worst_call_seconds(retries: dict[str, Any], attempt_seconds: int) -> int:
    """Every attempt timing out, plus the longest backoff botocore's standard mode sleeps before
    each retry (at most min(2 ** (i - 1), 20) s before retry i)."""
    attempts = retries["total_max_attempts"]
    return attempts * attempt_seconds + sum(min(2 ** (i - 1), 20) for i in range(1, attempts))


def shutdown_call_budget_seconds(settings: Settings) -> int:
    """The longest a single in-flight SQS or DynamoDB call can take, retries included."""
    sqs = _worst_call_seconds(
        _SQS_RETRIES,
        _CONNECT_TIMEOUT_SECONDS
        + settings.telemetry_ingest_worker_wait_seconds
        + _SQS_READ_TIMEOUT_MARGIN_SECONDS,
    )
    dynamodb = _worst_call_seconds(
        _DYNAMODB_RETRIES, _CONNECT_TIMEOUT_SECONDS + _DYNAMODB_READ_TIMEOUT_SECONDS
    )
    return max(sqs, dynamodb)


def stall_after_seconds(settings: Settings) -> int:
    """The liveness threshold (ADR-020): the worst legitimate message lifetime.

    The loop marks at the top of every turn and after every receive. A turn is one receive (the
    SQS worst call, 28 s at the defaults, plus the 0.5 s pause after a failed poll) or one
    5 s tick while every permit is busy; the receive may first wait for a pool thread behind a
    call already in flight. A message holds its permit for the processing deadline, plus one
    InfluxDB write the deadline cannot interrupt (anyio waits for its thread), plus an in-flight
    heartbeat the loop awaits, plus the final action. That sum, 45 + 10 + 28 + 28 = 111 s at the
    defaults, is the threshold: above any legitimate turn for every allowed setting, and the
    point past which a permit held by one message counts as wedged (no more busy-tick marks).
    """
    return (
        settings.telemetry_ingest_processing_deadline_seconds
        + INFLUX_TIMEOUT_SECONDS
        + 2 * shutdown_call_budget_seconds(settings)
    )


def _stall_exit_grace_seconds(settings: Settings) -> float:
    """After a stall the clean exit gets the A47 stop budget before the forced `os._exit(1)`."""
    return (
        settings.telemetry_ingest_worker_wait_seconds
        + _POLL_GRACE_SECONDS
        + settings.telemetry_ingest_drain_seconds
        + shutdown_call_budget_seconds(settings)
    )


_LOCAL_ENVIRONMENTS = {"local", "test"}


def build_worker(
    settings: Settings,
    *,
    client_factory: Callable[..., Any] = boto3.client,
    influx_client_factory: Callable[..., Any] = InfluxDBClient,
    liveness: Callable[[], None] = no_liveness,
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
        url=settings.influxdb_url,
        token=settings.influxdb_token,
        org=settings.influxdb_org,
        timeout=INFLUX_TIMEOUT_SECONDS * 1000,
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
        liveness=liveness,
        overdue_after_seconds=stall_after_seconds(settings),
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
    stamp = LivenessStamp(liveness_path(), stall_after_seconds=stall_after_seconds(settings))
    try:
        worker = build_worker(settings, influx_client_factory=open_influx, liveness=stamp.mark)
    except Exception as error:  # noqa: BLE001 - by category only, never the message
        logger.error("telemetry ingest worker could not be composed (%s)", type(error).__name__)
        return EXIT_FAILURE
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    run = asyncio.ensure_future(worker.run(stop))
    watchdog = LivenessWatchdog(
        stamp,
        # The clean path: from the watchdog thread, cancel the loop through the event loop.
        on_stall=lambda: loop.call_soon_threadsafe(run.cancel),
        exit_grace_seconds=_stall_exit_grace_seconds(settings),
        check_every_seconds=_WATCHDOG_CHECK_SECONDS,
        force_exit=_force_exit,
    )

    def request_stop() -> None:
        watchdog.disarm()  # the A47 drain may outlast the stall threshold
        stop.set()

    signals = (signal.SIGTERM, signal.SIGINT)
    for signum in signals:
        loop.add_signal_handler(signum, request_stop)
    watchdog.start()
    try:
        logger.info("telemetry ingest worker started")
        # Never raises for the loop's own end; its outcome is read from the task below.
        await asyncio.wait({run})
    finally:
        watchdog.close()
        for signum in signals:
            loop.remove_signal_handler(signum)
    if run.cancelled() and watchdog.stalled:
        logger.error("telemetry ingest worker loop cancelled after a stall")
        return EXIT_FAILURE
    try:
        run.result()  # re-raises any cancellation that was not the watchdog's
    except Exception as error:  # noqa: BLE001 - a crash exits non-zero, by category only
        logger.error("telemetry ingest worker crashed (%s)", type(error).__name__)
        return EXIT_FAILURE
    if not stop.is_set():
        logger.error("telemetry ingest worker loop ended without a stop")
        return EXIT_FAILURE
    logger.info("telemetry ingest worker stopped")
    return EXIT_STOPPED


def _has_own_credentials(settings: Settings) -> bool:
    return all(
        (
            os.environ.get("AWS_ACCESS_KEY_ID", "").strip(),
            os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip(),
            settings.influxdb_token.strip(),
        )
    )


def _remove_old_stamp() -> None:
    """A refused (parked) or restarted worker must never look alive through an old stamp."""
    try:
        liveness_path().unlink(missing_ok=True)
    except OSError as error:
        logger.warning("telemetry ingest liveness stamp not removed (%s)", type(error).__name__)


def main(argv: Sequence[str] = ()) -> int:
    arguments = list(argv)
    if arguments == [HEALTHCHECK]:
        return healthcheck()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if arguments:
        # Never argparse: its usage error exits 2, which ADR-020 keeps for a partial run.
        logger.error("telemetry ingest worker refused to start: unknown arguments")
        return EXIT_CONFIG
    _remove_old_stamp()
    try:
        settings = Settings()
    except ValidationError as error:
        # `hide_input_in_errors`: the count only, never a value.
        logger.error(
            "telemetry ingest worker refused to start: %d invalid settings", error.error_count()
        )
        return EXIT_CONFIG
    if not settings.telemetry_v2_write_enabled:
        logger.error("telemetry ingest worker refused to start: TELEMETRY_V2_WRITE_ENABLED is off")
        return EXIT_CONFIG
    if not settings.sqs_telemetry_ingress_queue_url:
        logger.error(
            "telemetry ingest worker refused to start: SQS_TELEMETRY_INGRESS_QUEUE_URL is empty"
        )
        return EXIT_CONFIG
    if settings.app_env not in _LOCAL_ENVIRONMENTS and not _has_own_credentials(settings):
        # Compose interpolates a missing variable to "": fail fast rather than poll forever
        # with NoCredentialsError while the API keeps answering 202.
        logger.error(
            "telemetry ingest worker refused to start: its AWS credentials or InfluxDB write "
            "token are empty"
        )
        return EXIT_CONFIG
    return asyncio.run(_run(settings))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
