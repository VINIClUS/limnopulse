"""Operator DLQ redrive core (P3-05, D10); the P3-09 `telemetry_dlq.py` CLI drives it.

Dry-run is the default: it only counts, then releases every message it saw with visibility 0.
With `apply`, each raw body is sent unchanged to the source queue and only then deleted from
the DLQ, so a failed send never loses a message (a duplicate after a failed delete is safe).
Messages are held for the whole run and released only after the scan, so one run never counts a
message twice; the release is best-effort and also runs when the scan fails, and with `apply` it
covers the messages that could not be moved, so an immediate rerun sees them again.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass

from limnopulse_api.core.errors import IngressMessageTooLarge, IngressQueueError
from limnopulse_api.domain.ingestion import IngressQueueMessage
from limnopulse_api.repositories.ingestion import IngressQueueConsumer, IngressQueueProducer

_LOGGER = logging.getLogger(__name__)
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
    seen = 0
    redriven = failed = 0
    held: list[IngressQueueMessage] = []
    try:
        while seen < max_messages:
            batch = await dlq.receive(
                max_messages=min(_MAX_BATCH, max_messages - seen),
                wait_seconds=_WAIT_SECONDS,
                visibility_seconds=_HOLD_SECONDS,
            )
            if not batch:
                break
            seen += len(batch)
            if not apply:
                held.extend(batch)
                continue
            for message in batch:
                if await _move(dlq, source, message):
                    redriven += 1
                else:
                    failed += 1
                    held.append(message)
    finally:
        await _release(dlq, held)
    return RedriveReport(seen=seen, redriven=redriven, failed=failed)


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


async def _release(dlq: IngressQueueConsumer, messages: Sequence[IngressQueueMessage]) -> None:
    not_released = 0
    for message in messages:
        try:
            await dlq.change_visibility(message, 0)
        except IngressQueueError:
            not_released += 1
    if not_released:
        _LOGGER.warning(
            "dlq redrive could not release %d message(s); they reappear after %d s",
            not_released,
            _HOLD_SECONDS,
        )
