"""Operator CLI for the telemetry ingress DLQ (P3-09, D10, §23.3).

    python -m scripts.admin.telemetry_dlq sample [--limit 10]
    python -m scripts.admin.telemetry_dlq redrive [--max 100] [--apply]

`sample` prints metadata only (never a body or a receipt handle), diagnoses each body as a
`telemetry-ingress@1` envelope, counts each message once and releases every message it saw.
`redrive` delegates to the P3-05 `redrive_dlq` core and is a dry run unless `--apply` is given.
Nothing here runs on its own: the DLQ is only touched when an operator runs a subcommand.

Output is JSON lines on stdout. Exit codes: 0 ok (a dry run included), 1 error, 3 partial (an
applied redrive with failures). Metrics go to an injected `TelemetryMetrics`; the default sink
writes JSON lines to stderr.
"""

import argparse
import asyncio
import json
import logging
import sys
from collections.abc import Callable, Sequence
from typing import Any, NoReturn, TextIO

import boto3
from pydantic import ValidationError

from limnopulse_api.adapters.sqs_ingestion import SqsIngressQueue
from limnopulse_api.core.config import Settings
from limnopulse_api.core.errors import IngressQueueError
from limnopulse_api.domain.ingestion import (
    IngressEnvelope,
    IngressQueueMessage,
    require_metric_labels,
)
from limnopulse_api.domain.v2_common import format_utc_timestamp
from limnopulse_api.repositories.ingestion import (
    IngressQueueConsumer,
    IngressQueueProducer,
    TelemetryMetrics,
)
from limnopulse_api.services.ingress_redrive import redrive_dlq

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_PARTIAL = 3
MAX_SAMPLE_LIMIT = 1_000
MAX_REDRIVE = 10_000
REDRIVEN_METRIC = "ingress_dlq_redriven_total"
_MAX_BATCH = 10
_WAIT_SECONDS = 1
_HOLD_SECONDS = 300
_LOGGER = logging.getLogger(__name__)


class JsonLinesMetrics:
    """The runtime `TelemetryMetrics` sink: one `{"metric", "value", "labels"}` line per call."""

    def __init__(self, stream: TextIO) -> None:
        self._stream = stream

    def increment(self, name: str, value: int = 1, **labels: str) -> None:
        self._write(name, value, labels)

    def observe(self, name: str, value: float, **labels: str) -> None:
        self._write(name, value, labels)

    def _write(self, name: str, value: float, labels: dict[str, str]) -> None:
        require_metric_labels(name, labels)
        self._stream.write(json.dumps({"metric": name, "value": value, "labels": labels}) + "\n")
        self._stream.flush()


class _UsageError(Exception):
    pass


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise _UsageError


def _bounded_int(low: int, high: int) -> Callable[[str], int]:
    def parse(text: str) -> int:
        value = int(text)
        if not low <= value <= high:
            raise ValueError
        return value

    return parse


def _parser() -> _ArgumentParser:
    parser = _ArgumentParser(prog="telemetry_dlq", description="Telemetry ingress DLQ tools.")
    commands = parser.add_subparsers(dest="command", required=True)
    sample = commands.add_parser("sample", help="print DLQ message metadata and release them")
    sample.add_argument("--limit", type=_bounded_int(1, MAX_SAMPLE_LIMIT), default=10)
    redrive = commands.add_parser("redrive", help="move DLQ messages back (dry run by default)")
    redrive.add_argument("--max", type=_bounded_int(1, MAX_REDRIVE), default=100)
    redrive.add_argument("--apply", action="store_true")
    return parser


def _emit(out: TextIO, payload: dict[str, Any]) -> None:
    out.write(json.dumps(payload, separators=(",", ":")) + "\n")


async def run(
    argv: Sequence[str],
    *,
    dlq: IngressQueueConsumer,
    source: IngressQueueProducer | None,
    metrics: TelemetryMetrics,
    out: TextIO,
) -> int:
    try:
        args = _parser().parse_args(list(argv))
    except _UsageError:
        _emit(out, {"error": "usage"})
        return EXIT_ERROR
    try:
        if args.command == "sample":
            return await _sample(dlq, args.limit, out)
        if source is None:
            _emit(out, {"error": "queue_not_configured"})
            return EXIT_ERROR
        return await _redrive(dlq, source, metrics, out, max_messages=args.max, apply=args.apply)
    except IngressQueueError:
        _emit(out, {"error": "queue_unavailable"})
        return EXIT_ERROR
    except Exception:  # noqa: BLE001 - never the text: a cause can carry a receipt handle
        _emit(out, {"error": "unexpected"})
        return EXIT_ERROR


async def _redrive(
    dlq: IngressQueueConsumer,
    source: IngressQueueProducer,
    metrics: TelemetryMetrics,
    out: TextIO,
    *,
    max_messages: int,
    apply: bool,
) -> int:
    report = await redrive_dlq(dlq=dlq, source=source, max_messages=max_messages, apply=apply)
    if apply:
        for result, value in (("redriven", report.redriven), ("failed", report.failed)):
            if value:
                metrics.increment(REDRIVEN_METRIC, value, result=result)
    _emit(
        out,
        {
            "command": "redrive",
            "apply": apply,
            "seen": report.seen,
            "redriven": report.redriven,
            "failed": report.failed,
        },
    )
    return EXIT_PARTIAL if report.failed else EXIT_OK


def _diagnose(message: IngressQueueMessage) -> dict[str, Any]:
    line: dict[str, Any] = {
        "message_id": message.message_id,
        "receive_count": message.receive_count,
        "sent_at": format_utc_timestamp(message.sent_at),
        "body_bytes": len(message.body.encode("utf-8")),
    }
    try:
        json.loads(message.body)
    except (ValueError, RecursionError):  # a deeply nested body is still just "not json"
        return {**line, "diagnosis": "not_json"}
    try:
        envelope = IngressEnvelope.model_validate_json(message.body)
    except ValidationError:
        return {**line, "diagnosis": "invalid_envelope"}
    return {
        **line,
        "diagnosis": "valid_envelope",
        "ingress_id": envelope.ingress_id,
        "replay": envelope.replay,
        "observations": len(envelope.observations),
    }


async def _sample(dlq: IngressQueueConsumer, limit: int, out: TextIO) -> int:
    held: dict[str, IngressQueueMessage] = {}
    try:
        while len(held) < limit:
            batch = await dlq.receive(
                max_messages=min(_MAX_BATCH, limit - len(held)),
                wait_seconds=_WAIT_SECONDS,
                visibility_seconds=_HOLD_SECONDS,
            )
            if not batch:
                break
            for message in batch:
                first_seen = message.message_id not in held
                # Held before anything else, so the finally block releases it whatever follows.
                held[message.message_id] = message
                if first_seen:
                    _emit(out, _diagnose(message))
    finally:
        await _release(dlq, list(held.values()))
    _emit(out, {"command": "sample", "sampled": len(held)})
    return EXIT_OK


async def _release(dlq: IngressQueueConsumer, messages: Sequence[IngressQueueMessage]) -> None:
    not_released = 0
    for message in messages:
        try:
            await dlq.change_visibility(message, 0)
        except IngressQueueError:
            not_released += 1
    if not_released:
        _LOGGER.warning("dlq sample could not release %d message(s)", not_released)


def main(
    argv: Sequence[str] | None = None,
    *,
    settings: Settings | None = None,
    sqs_client: Any = None,
    metrics: TelemetryMetrics | None = None,
    out: TextIO | None = None,
    err: TextIO | None = None,
) -> int:
    out = out or sys.stdout
    err = err or sys.stderr
    try:
        settings = settings or Settings()
    except ValidationError:
        _emit(out, {"error": "settings"})
        return EXIT_ERROR
    if not settings.sqs_telemetry_ingress_dlq_url:
        _emit(out, {"error": "queue_not_configured"})
        return EXIT_ERROR
    try:
        client = sqs_client or boto3.client(
            "sqs", region_name=settings.aws_region, endpoint_url=settings.sqs_endpoint_url
        )
    except Exception:  # noqa: BLE001 - a bad endpoint error quotes the URL, which may hold a secret
        _emit(out, {"error": "settings"})
        return EXIT_ERROR
    source_url = settings.sqs_telemetry_ingress_queue_url
    return asyncio.run(
        run(
            sys.argv[1:] if argv is None else argv,
            dlq=SqsIngressQueue(client=client, queue_url=settings.sqs_telemetry_ingress_dlq_url),
            # `sample` needs only the DLQ; `redrive` refuses to run without the source queue.
            source=SqsIngressQueue(client=client, queue_url=source_url) if source_url else None,
            metrics=metrics or JsonLinesMetrics(err),
            out=out,
        )
    )


if __name__ == "__main__":
    sys.exit(main())
