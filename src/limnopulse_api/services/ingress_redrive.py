"""Operator DLQ redrive core (P3-05, D10); the P3-09 `telemetry_dlq.py` CLI drives it.

Dry-run is the default: it only counts, then releases every message it saw with visibility 0.
With `apply`, each raw body is sent unchanged to the source queue and only then deleted from
the DLQ, so a failed send never loses a message (a duplicate after a failed delete is safe).
Messages are held for the whole run and released only after the scan. A message whose hold lapsed
in a slow run comes back with a new receipt handle; it is recognised by its message id and never
counted or moved twice, and its newest handle is the one released. The release is best-effort,
also runs when the scan fails, and with `apply` covers the messages that could not be moved, so
an immediate rerun sees them again.
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
    seen: set[str] = set()
    redriven = failed = 0
    # message id -> its latest receive; a hold that lapsed in a slow run brings a message back
    # with a new receipt handle, and only the newest handle can release it.
    held: dict[str, IngressQueueMessage] = {}
    try:
        while len(seen) < max_messages:
            batch = await dlq.receive(
                max_messages=min(_MAX_BATCH, max_messages - len(seen)),
                wait_seconds=_WAIT_SECONDS,
                visibility_seconds=_HOLD_SECONDS,
            )
            if not batch:
                break
            for message in batch:
                if message.message_id in seen:
                    held[message.message_id] = message  # seen before: never counted twice
                    continue
                seen.add(message.message_id)
                if not apply:
                    held[message.message_id] = message
                elif await _move(dlq, source, message):
                    redriven += 1
                else:
                    failed += 1
                    held[message.message_id] = message
    finally:
        await _release(dlq, list(held.values()))
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
