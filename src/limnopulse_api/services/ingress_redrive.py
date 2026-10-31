"""Operator DLQ redrive core (P3-05, D10); the P3-09 `telemetry_dlq.py` CLI drives it.

Dry-run is the default: it only counts, then releases every message it saw with visibility 0.
With `apply`, each raw body is sent unchanged to the source queue and only then deleted from
the DLQ, so a failed send never loses a message (a duplicate after a failed delete is safe).
Messages are released only after the whole scan, so one run never counts a message twice.
"""

from dataclasses import dataclass

from limnopulse_api.core.errors import IngressMessageTooLarge, IngressQueueError
from limnopulse_api.domain.ingestion import IngressQueueMessage
from limnopulse_api.repositories.ingestion import IngressQueueConsumer, IngressQueueProducer

_MAX_BATCH = 10
_WAIT_SECONDS = 1
# Held for the whole run, so a message seen once is not received again before the run ends.
_HOLD_SECONDS = 300


@dataclass(frozen=True, slots=True)
class RedriveReport:
    seen: int
    redriven: int
    failed: int


async def redrive_dlq(
    *,
    dlq: IngressQueueConsumer,
    source: IngressQueueProducer,
    max_messages: int,
    apply: bool = False,
) -> RedriveReport:
    seen: list[IngressQueueMessage] = []
    redriven = failed = 0
    while len(seen) < max_messages:
        batch = await dlq.receive(
            max_messages=min(_MAX_BATCH, max_messages - len(seen)),
            wait_seconds=_WAIT_SECONDS,
            visibility_seconds=_HOLD_SECONDS,
        )
        if not batch:
            break
        seen.extend(batch)
        if not apply:
            continue
        for message in batch:
            if await _move(dlq, source, message):
                redriven += 1
            else:
                failed += 1
    if not apply:
        for message in seen:
            await dlq.change_visibility(message, 0)
    return RedriveReport(seen=len(seen), redriven=redriven, failed=failed)


async def _move(
    dlq: IngressQueueConsumer, source: IngressQueueProducer, message: IngressQueueMessage
) -> bool:
    try:
        await source.send_raw(message.body)
    except (IngressQueueError, IngressMessageTooLarge):
        return False
    try:
        await dlq.delete(message)
    except IngressQueueError:
        return False
    return True
