"""Ingest one legacy MQTT reading through the P2 canonical telemetry pipeline (P2-08 dev CLI).

P2 ships dark (D16): this CLI is the only caller of `TelemetryPipeline` in P2. The trusted
context is built from the CLI arguments only (D2); a tenant, pond or device in the payload is
never authority. Writes follow the two switches `TELEMETRY_V2_WRITE_ENABLED` and
`TELEMETRY_LEGACY_DUAL_WRITE_ENABLED`; with both off the run is a dry run that normalizes and
writes zero points. Prints ONE JSON object of counts on stdout and never prints settings.

Exit codes: 0 nothing quarantined (everything accepted was written, or the dry run); 2 anything
quarantined; 1 any error, usage errors included (2 is reserved for quarantine). On an error
nothing is reported as written, and the reading must be ingested again (D9).

Run from the repository root:
    python scripts/dev/ingest_reading_v2.py --tenant tnt_local_001 --device local-device-001 \
        [--file examples/telemetry/reading.local.json] [--observed-at now|ISO] [--received-at ISO]
"""

import argparse
import asyncio
import json
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NoReturn

import boto3
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from pydantic import TypeAdapter

from limnopulse_api.adapters.dynamodb_deployments import DynamoDeploymentRepository
from limnopulse_api.adapters.dynamodb_devices_v2 import DynamoDeviceComponentRepository
from limnopulse_api.adapters.dynamodb_sites_assets import DynamoSiteAssetRepository
from limnopulse_api.adapters.influxdb_v2_writer import InfluxObservationWriter
from limnopulse_api.core.config import Settings
from limnopulse_api.domain.telemetry_v2 import (
    SourceType,
    TelemetryDatetime,
    TrustedSourceContext,
)
from limnopulse_api.services.telemetry_normalizer import TelemetryNormalizer
from limnopulse_api.services.telemetry_pipeline import PipelineOutcome, TelemetryPipeline
from limnopulse_api.services.telemetry_v2_writes import ObservationWriteService

DEFAULT_READING = Path("examples/telemetry/reading.local.json")
LEGACY_INTEGRATION_TYPE = "legacy_mqtt"
OBSERVED_AT_NOW = "now"
EXIT_OK = 0
EXIT_ERROR = 1
EXIT_QUARANTINED = 2

_TIMESTAMP = TypeAdapter(TelemetryDatetime)


class _UsageError(Exception):
    pass


class _ArgumentParser(argparse.ArgumentParser):
    """Usage errors exit 1, never argparse's default 2 (which means quarantined here)."""

    def error(self, message: str) -> NoReturn:
        raise _UsageError(message)


def build_parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(
        description=(
            "Ingest one legacy reading through the canonical telemetry pipeline. With "
            "TELEMETRY_V2_WRITE_ENABLED and TELEMETRY_LEGACY_DUAL_WRITE_ENABLED both off this is "
            "a dry run: the reading is normalized and zero points are written."
        )
    )
    parser.add_argument("--tenant", required=True, help="trusted tenant ID (never the payload's)")
    parser.add_argument("--device", required=True, help="trusted legacy device ID")
    parser.add_argument("--file", type=Path, default=DEFAULT_READING, help="legacy JSON reading")
    parser.add_argument(
        "--observed-at",
        help="'now' or an ISO-8601 timestamp that replaces the payload 'ts' (rehearsals)",
    )
    parser.add_argument("--received-at", help="ISO-8601 receive time (default: now)")
    return parser


def _parse_time(name: str, value: str) -> datetime:
    try:
        return _TIMESTAMP.validate_python(value)
    except ValueError as exc:
        raise _UsageError(f"{name} must be an ISO-8601 timestamp with a timezone") from exc


def _read_payload(path: Path, observed_at: str | None) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise _UsageError("the reading must be a JSON object")
    if observed_at == OBSERVED_AT_NOW:
        # `isoformat()` of an aware UTC datetime is the Telegraf layout: `T`, `+00:00`, µs.
        payload["ts"] = datetime.now(UTC).isoformat()
    elif observed_at is not None:
        payload["ts"] = _parse_time("--observed-at", observed_at).isoformat()
    return payload


def dynamodb_client_kwargs(settings: Settings) -> dict[str, str]:
    """Mirror of the application's DynamoDB client construction (without importing the app)."""
    kwargs: dict[str, str] = {"region_name": settings.aws_region}
    if settings.dynamodb_endpoint_url is not None:
        kwargs["endpoint_url"] = settings.dynamodb_endpoint_url
    if settings.app_env in {"local", "test"} and settings.dynamodb_endpoint_url is not None:
        kwargs["aws_access_key_id"] = "local"
        kwargs["aws_secret_access_key"] = "local"
    return kwargs


async def _ingest(
    settings: Settings, context: TrustedSourceContext, payload: Mapping[str, Any]
) -> PipelineOutcome:
    dynamodb_client = boto3.client("dynamodb", **dynamodb_client_kwargs(settings))
    table_name = settings.dynamodb_domain_table
    v2_write_enabled = settings.telemetry_v2_write_enabled
    legacy_dual_write_enabled = settings.telemetry_legacy_dual_write_enabled
    influxdb_client = InfluxDBClient(
        url=settings.influxdb_url, token=settings.influxdb_token, org=settings.influxdb_org
    )
    try:
        writer = None
        if v2_write_enabled or legacy_dual_write_enabled:
            writer = InfluxObservationWriter(
                write_api=influxdb_client.write_api(write_options=SYNCHRONOUS),
                bucket=settings.influxdb_bucket_raw,
                org=settings.influxdb_org,
            )
        pipeline = TelemetryPipeline(
            normalizer=TelemetryNormalizer(
                components=DynamoDeviceComponentRepository(table_name, dynamodb_client),
                deployments=DynamoDeploymentRepository(table_name, dynamodb_client),
                clock=lambda: datetime.now(UTC),
            ),
            write_service=ObservationWriteService(
                writer=writer if v2_write_enabled else None,
                legacy_writer=writer if legacy_dual_write_enabled else None,
                site_assets=DynamoSiteAssetRepository(table_name, dynamodb_client),
                v2_write_enabled=v2_write_enabled,
                legacy_dual_write_enabled=legacy_dual_write_enabled,
            ),
        )
        return await pipeline.ingest_legacy_reading(context, payload)
    finally:
        influxdb_client.close()


def _summary(outcome: PipelineOutcome) -> dict[str, Any]:
    return {
        "written": asdict(outcome.written),
        "quarantined": len(outcome.quarantined),
        "counts": dict(outcome.counts),
    }


def _usage_detail(exc: Exception) -> str:
    # Our own messages are safe; library messages may echo the payload, so name them only.
    return str(exc) if isinstance(exc, _UsageError) else "invalid input"


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
        received_at = (
            _parse_time("--received-at", args.received_at)
            if args.received_at is not None
            else datetime.now(UTC)
        )
        payload = _read_payload(args.file, args.observed_at)
        context = TrustedSourceContext(
            tenant_id=args.tenant,
            device_id=args.device,
            integration_id=None,
            integration_type=LEGACY_INTEGRATION_TYPE,
            source_type=SourceType.LEGACY_MQTT,
            connector_version=None,
            received_at=received_at,
        )
    except (_UsageError, OSError, ValueError) as exc:
        # ValueError covers malformed JSON and an invalid trusted context (pydantic).
        sys.stderr.write(f"{parser.prog}: error: {type(exc).__name__}: {_usage_detail(exc)}\n")
        return EXIT_ERROR

    try:
        outcome = asyncio.run(_ingest(Settings(), context, payload))
    except Exception as exc:  # noqa: BLE001 - every failure maps to exit 1; nothing was written.
        sys.stderr.write(f"{parser.prog}: ingest failed: {type(exc).__name__}\n")
        return EXIT_ERROR
    sys.stdout.write(json.dumps(_summary(outcome)) + "\n")
    return EXIT_QUARANTINED if outcome.quarantined else EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
