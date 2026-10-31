"""Operator reconciliation CLI for telemetry quarantine rows (P3-09, D9/D10, §10.2).

Run from an operator checkout (repository root, AWS credential chain):

    python -m scripts.admin.telemetry_quarantine list --tenant T [--status open] [--reason R]
        [--limit 50] [--after SK] [--show-excerpt]
    python -m scripts.admin.telemetry_quarantine replay --tenant T --entry KEY... [--apply]
    python -m scripts.admin.telemetry_quarantine dismiss --tenant T --entry KEY... [--apply]
    python -m scripts.admin.telemetry_quarantine health --tenant T --account IA

`replay` and `dismiss` are dry runs unless `--apply` is given. A replay groups the requested open,
replayable rows by `(ingress_id, device_integration_id)` and sends one NEW `telemetry-ingress@1`
envelope per group through the normal producer: a deterministic `ingress_id` derived from the
group's entry keys (a rerun after a crash before any row was marked reuses it), `replay_of` the original ingress id,
`replay=true`, the first `received_at`, the stored mapping versions and the stored observations.
Rows are marked `replayed` only after their group's send succeeded. `dismiss` is versioned on the
row it read. `health` prints the D11 connector-health row and writes nothing.

Output is JSON lines on stdout: one object per entry and a final summary carrying `command`.
Excerpts are printed only by `list --show-excerpt`; errors are reported as a fixed category and
argv is never echoed. Exit codes: 0 ok (a dry run included), 1 error, 3 partial (an applied replay
or dismiss with a skipped or failed entry). Metrics go to an injected `TelemetryMetrics`; the
default sink writes JSON lines to stderr.
"""

import argparse
import asyncio
import json
import sys
from collections import defaultdict
from collections.abc import Callable, Sequence
from datetime import datetime
from typing import Any, NoReturn, TextIO

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import ValidationError

from limnopulse_api.adapters.dynamodb_ingest_state import DynamoIngestStateRepository
from limnopulse_api.adapters.dynamodb_quarantine import DynamoQuarantineRepository
from limnopulse_api.adapters.dynamodb_v2 import utc_now
from limnopulse_api.adapters.sqs_ingestion import SqsIngressQueue
from limnopulse_api.core.config import Settings
from limnopulse_api.core.errors import (
    ConflictError,
    IngressMessageTooLarge,
    IngressQueueError,
    NotFoundError,
)
from limnopulse_api.domain.ids import replay_ingress_id
from limnopulse_api.domain.ingestion import (
    INGRESS_ENVELOPE_SCHEMA,
    IngressEnvelope,
    QuarantineEntry,
    QuarantineEntryStatus,
    require_metric_labels,
)
from limnopulse_api.domain.telemetry_v2 import SourceObservation
from limnopulse_api.domain.v2_common import format_utc_timestamp
from limnopulse_api.repositories.ingestion import (
    IngestStateRepository,
    IngressQueueProducer,
    QuarantineRepository,
    TelemetryMetrics,
)

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_PARTIAL = 3
MAX_LIST_LIMIT = 1_000
REPLAYED_METRIC = "ingress_quarantine_replayed_total"
_STATUSES = tuple(status.value for status in QuarantineEntryStatus)

Clock = Callable[[], datetime]


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
        # argparse messages quote the offending argv token, which may be a pasted secret.
        raise _UsageError


def _bounded_int(low: int, high: int) -> Callable[[str], int]:
    def parse(text: str) -> int:
        value = int(text)
        if not low <= value <= high:
            raise ValueError
        return value

    return parse


def _parser() -> _ArgumentParser:
    parser = _ArgumentParser(prog="telemetry_quarantine", description="Quarantine reconciliation.")
    commands = parser.add_subparsers(dest="command", required=True)
    listing = commands.add_parser("list", help="list quarantine rows of a tenant")
    listing.add_argument("--tenant", required=True)
    listing.add_argument("--status", choices=_STATUSES, default=QuarantineEntryStatus.OPEN.value)
    listing.add_argument("--reason")
    listing.add_argument("--limit", type=_bounded_int(1, MAX_LIST_LIMIT), default=50)
    listing.add_argument("--after")
    listing.add_argument("--show-excerpt", action="store_true")
    for name in ("replay", "dismiss"):
        command = commands.add_parser(name, help=f"{name} quarantine rows (dry run by default)")
        command.add_argument("--tenant", required=True)
        command.add_argument("--entry", nargs="+", required=True)
        command.add_argument("--apply", action="store_true")
    health = commands.add_parser("health", help="print the connector-health row")
    health.add_argument("--tenant", required=True)
    health.add_argument("--account", required=True)
    return parser


def _emit(out: TextIO, payload: dict[str, Any]) -> None:
    # ASCII-escaped, so a non-UTF-8 stdout can never fail a listing halfway through.
    out.write(json.dumps(payload, separators=(",", ":")) + "\n")


def _error_category(error: BaseException) -> str:
    if isinstance(error, IngressQueueError | IngressMessageTooLarge):
        return "queue_unavailable"
    if isinstance(error, BotoCoreError | ClientError):
        return "store_unavailable"
    return "unexpected"


class _Context:
    def __init__(
        self,
        *,
        quarantine: QuarantineRepository,
        ingest_state: IngestStateRepository,
        producer: IngressQueueProducer | None,
        metrics: TelemetryMetrics,
        out: TextIO,
        clock: Clock,
    ) -> None:
        self.quarantine = quarantine
        self.ingest_state = ingest_state
        self.producer = producer
        self.metrics = metrics
        self.out = out
        self.clock = clock


async def run(
    argv: Sequence[str],
    *,
    quarantine: QuarantineRepository,
    ingest_state: IngestStateRepository,
    producer: IngressQueueProducer | None,
    metrics: TelemetryMetrics,
    out: TextIO,
    clock: Clock = utc_now,
) -> int:
    try:
        args = _parser().parse_args(list(argv))
    except _UsageError:
        _emit(out, {"error": "usage"})
        return EXIT_ERROR
    context = _Context(
        quarantine=quarantine,
        ingest_state=ingest_state,
        producer=producer,
        metrics=metrics,
        out=out,
        clock=clock,
    )
    try:
        return await _COMMANDS[args.command](args, context)
    except Exception as error:  # noqa: BLE001 - reported as a category, never as its text
        _emit(out, {"error": _error_category(error)})
        return EXIT_ERROR


async def _list(args: argparse.Namespace, context: _Context) -> int:
    page = await context.quarantine.list_quarantine_entries(
        args.tenant,
        status=QuarantineEntryStatus(args.status),
        reason=args.reason,
        after_sk=args.after,
        limit=args.limit,
    )
    for entry in page.entries:
        _emit(context.out, _entry_line(entry, show_excerpt=args.show_excerpt))
    _emit(
        context.out,
        {"command": "list", "count": len(page.entries), "next_after": page.next_after_sk},
    )
    return EXIT_OK


def _entry_line(entry: QuarantineEntry, *, show_excerpt: bool) -> dict[str, Any]:
    line: dict[str, Any] = {
        "entry_key": entry.entry_key,
        "kind": entry.kind.value,
        "reason": entry.reason,
        "status": entry.status.value,
        "replayable": entry.replayable,
        "truncated": entry.truncated,
        "received_at": format_utc_timestamp(entry.received_at),
        "created_at": format_utc_timestamp(entry.created_at),
        "ingress_id": entry.ingress_id,
        "replay_of": entry.replay_of,
        "replayed_ingress_id": entry.replayed_ingress_id,
        "integration_account_id": entry.integration_account_id,
        "device_integration_id": entry.device_integration_id,
        "device_id": entry.device_id,
        "version": entry.version,
    }
    if show_excerpt:
        line["source_observation_json"] = entry.source_observation_json
        line["envelope_excerpt_json"] = entry.envelope_excerpt_json
    return line


async def _read_open(
    args: argparse.Namespace, context: _Context, results: "_Results"
) -> list[QuarantineEntry]:
    entries: list[QuarantineEntry] = []
    for key in dict.fromkeys(args.entry):
        entry = await context.quarantine.get_quarantine_entry(args.tenant, key)
        if entry is None:
            results.skip(key, "not_found")
        elif entry.status != QuarantineEntryStatus.OPEN:
            results.skip(key, "not_open")
        else:
            entries.append(entry)
    return entries


class _Results:
    def __init__(self, command: str, apply: bool, out: TextIO, done: str, planned: str) -> None:
        self.command = command
        self.apply = apply
        self.out = out
        self.counts = dict.fromkeys((done, planned, "skipped", "failed"), 0)

    def record(self, key: str, result: str, **fields: Any) -> None:
        self.counts[result] += 1
        _emit(self.out, {"entry_key": key, "result": result, **fields})

    def skip(self, key: str, reason: str) -> None:
        self.record(key, "skipped", reason=reason)

    def finish(self) -> int:
        _emit(self.out, {"command": self.command, "apply": self.apply, **self.counts})
        if self.apply and (self.counts["skipped"] or self.counts["failed"]):
            return EXIT_PARTIAL
        return EXIT_OK


def _replay_envelope(group: Sequence[QuarantineEntry], ingress_id: str) -> IngressEnvelope:
    first = group[0]
    return IngressEnvelope(
        schema=INGRESS_ENVELOPE_SCHEMA,
        ingress_id=ingress_id,
        replay_of=first.replay_of or first.ingress_id,
        tenant_id=first.tenant_id,
        integration_account_id=first.integration_account_id,
        provider_key=first.provider_key,
        external_identity_hash=first.external_identity_hash,
        device_integration_id=first.device_integration_id,
        device_id=first.device_id,
        source_type=first.source_type,
        connector_version=first.connector_version,
        received_at=min(entry.received_at for entry in group),
        replay=True,
        mapping_versions=first.mapping_versions,
        observations=tuple(
            SourceObservation.model_validate_json(entry.source_observation_json or "")
            for entry in group
        ),
    )


async def _replay(args: argparse.Namespace, context: _Context) -> int:
    producer = context.producer
    if args.apply and producer is None:
        _emit(context.out, {"error": "queue_not_configured"})
        return EXIT_ERROR
    results = _Results("replay", args.apply, context.out, "replayed", "would_replay")
    groups: dict[tuple[str, str], list[QuarantineEntry]] = defaultdict(list)
    for entry in await _read_open(args, context, results):
        if entry.replayable:
            groups[(entry.ingress_id, entry.device_integration_id)].append(entry)
        else:
            results.skip(entry.entry_key, "not_replayable")
    if args.apply:
        for _ in range(results.counts["skipped"]):
            context.metrics.increment(REPLAYED_METRIC, result="skipped")
    for _, group in sorted(groups.items()):
        group.sort(key=lambda entry: entry.entry_key)
        ingress_id = replay_ingress_id(entry.entry_key for entry in group)
        try:
            envelope = _replay_envelope(group, ingress_id)
        except ValidationError:  # rows that no longer form a valid envelope are never sent
            for entry in group:
                results.skip(entry.entry_key, "invalid_envelope")
                if args.apply:
                    context.metrics.increment(REPLAYED_METRIC, result="skipped")
            continue
        if producer is None or not args.apply:
            for entry in group:
                results.record(entry.entry_key, "would_replay", replay_ingress_id=ingress_id)
            continue
        try:
            await producer.send(envelope)
        except (IngressQueueError, IngressMessageTooLarge):
            for entry in group:
                results.record(entry.entry_key, "failed", reason="send_failed")
                context.metrics.increment(REPLAYED_METRIC, result="failed")
            continue
        for entry in group:
            try:
                await context.quarantine.transition_quarantine_entry(
                    args.tenant,
                    entry.entry_key,
                    QuarantineEntryStatus.REPLAYED,
                    expected_version=entry.version,
                    at=context.clock(),
                    replayed_ingress_id=ingress_id,
                )
            except (ConflictError, NotFoundError):
                results.record(entry.entry_key, "failed", reason="conflict")
                context.metrics.increment(REPLAYED_METRIC, result="failed")
            except (BotoCoreError, ClientError):
                # Sent but not marked: the row stays open and a rerun sends it again (safe).
                results.record(entry.entry_key, "failed", reason="store_unavailable")
                context.metrics.increment(REPLAYED_METRIC, result="failed")
            else:
                results.record(entry.entry_key, "replayed", replay_ingress_id=ingress_id)
                context.metrics.increment(REPLAYED_METRIC, result="replayed")
    return results.finish()


async def _dismiss(args: argparse.Namespace, context: _Context) -> int:
    results = _Results("dismiss", args.apply, context.out, "dismissed", "would_dismiss")
    for entry in await _read_open(args, context, results):
        if not args.apply:
            results.record(entry.entry_key, "would_dismiss")
            continue
        try:
            await context.quarantine.transition_quarantine_entry(
                args.tenant,
                entry.entry_key,
                QuarantineEntryStatus.DISMISSED,
                expected_version=entry.version,
                at=context.clock(),
            )
        except (ConflictError, NotFoundError):
            results.record(entry.entry_key, "failed", reason="conflict")
        else:
            results.record(entry.entry_key, "dismissed")
    return results.finish()


async def _health(args: argparse.Namespace, context: _Context) -> int:
    health = await context.ingest_state.get_connector_health(args.tenant, args.account)
    _emit(
        context.out,
        {
            "command": "health",
            "found": health is not None,
            "health": None if health is None else health.model_dump(mode="json"),
        },
    )
    return EXIT_OK


_COMMANDS = {"list": _list, "replay": _replay, "dismiss": _dismiss, "health": _health}


def main(
    argv: Sequence[str] | None = None,
    *,
    settings: Settings | None = None,
    dynamodb_client: Any = None,
    sqs_client: Any = None,
    metrics: TelemetryMetrics | None = None,
    out: TextIO | None = None,
    err: TextIO | None = None,
    clock: Clock | None = None,
) -> int:
    out = out or sys.stdout
    err = err or sys.stderr
    try:
        settings = settings or Settings()
    except ValidationError:
        _emit(out, {"error": "settings"})
        return EXIT_ERROR
    clock = clock or utc_now
    try:
        dynamodb = dynamodb_client or boto3.client(
            "dynamodb",
            region_name=settings.aws_region,
            endpoint_url=settings.dynamodb_endpoint_url,
        )
        producer: IngressQueueProducer | None = None
        if settings.sqs_telemetry_ingress_queue_url:
            sqs = sqs_client or boto3.client(
                "sqs", region_name=settings.aws_region, endpoint_url=settings.sqs_endpoint_url
            )
            producer = SqsIngressQueue(
                client=sqs, queue_url=settings.sqs_telemetry_ingress_queue_url
            )
    except Exception:  # noqa: BLE001 - a bad endpoint error quotes the URL, which may hold a secret
        _emit(out, {"error": "settings"})
        return EXIT_ERROR
    table = settings.dynamodb_domain_table
    return asyncio.run(
        run(
            sys.argv[1:] if argv is None else argv,
            quarantine=DynamoQuarantineRepository(table, dynamodb, clock=clock),
            ingest_state=DynamoIngestStateRepository(table, dynamodb, clock=clock),
            producer=producer,
            metrics=metrics or JsonLinesMetrics(err),
            out=out,
            clock=clock,
        )
    )


if __name__ == "__main__":
    sys.exit(main())
