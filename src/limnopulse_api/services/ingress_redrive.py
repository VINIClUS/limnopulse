"""Operator DLQ redrive core (P3-05, D10); the P3-09 `telemetry_dlq.py` CLI drives it.

Dry-run is the default: it only counts, then releases every message it saw with visibility 0.
With `apply`, each raw body is sent unchanged to the source queue and only then deleted from
the DLQ, so a failed send never loses a message (a duplicate after a failed delete is safe).
Messages are held for the whole run and released only after the scan. A message whose hold lapsed
in a slow run comes back with a new receipt handle; it is recognised by its message id and never
counted or moved twice, and its newest handle is the one released. The release is best-effort,
also runs when the scan fails, and covers every received message that did not move (with
`apply`: the failed ones, and those of the batch a failure stopped before), so an immediate rerun
sees them again.

A receive cannot be stopped once its boto3 call is in flight, so a cancelled run (Ctrl-C) first
waits for that call (about `_WAIT_SECONDS`) and releases what it returned, instead of leaving it
hidden for the whole hold. A caller that passes a `RedriveProgress` still has the counts when the
run raises midway (the P3-09 CLI then reports a partial run).
"""

import asyncio
import contextlib
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


@dataclass(slots=True)
class RedriveProgress:
    """The running counts of one `redrive_dlq` run, kept up to date as it goes."""

    seen: int = 0
    redriven: int = 0
    failed: int = 0

    def report(self) -> RedriveReport:
        return RedriveReport(seen=self.seen, redriven=self.redriven, failed=self.failed)


async def redrive_dlq(
    *,
    dlq: IngressQueueConsumer,
    source: IngressQueueProducer,
    max_messages: int,
    apply: bool = False,
    progress: RedriveProgress | None = None,
) -> RedriveReport:
    counts = RedriveProgress() if progress is None else progress
    seen: set[str] = set()
    # message id -> its latest receive; a hold that lapsed in a slow run brings a message back
    # with a new receipt handle, and only the newest handle can release it.
    held: dict[str, IngressQueueMessage] = {}
    try:
        while len(seen) < max_messages:
            batch = await receive_held(dlq, max_messages=min(_MAX_BATCH, max_messages - len(seen)))
            if not batch:
                break
            # The whole batch is held until each message moves, so a run that raises or is
            # cancelled midway still releases the messages it had not reached yet (#116).
            held.update((message.message_id, message) for message in batch)
            for message in batch:
                if message.message_id in seen:
                    continue  # seen before: never counted twice
                seen.add(message.message_id)
                counts.seen = len(seen)
                if not apply:
                    continue
                if await _move(dlq, source, message):
                    del held[message.message_id]
                    counts.redriven += 1
                else:
                    counts.failed += 1
    finally:
        await _release(dlq, list(held.values()))
    return counts.report()


async def receive_held(
    dlq: IngressQueueConsumer, *, max_messages: int
) -> list[IngressQueueMessage]:
    """One DLQ receive under the run's hold (also used by the P3-09 `sample`). If the caller is
    cancelled while the call is in flight, the call is awaited and its messages released before
    the cancellation propagates."""
    receive = asyncio.ensure_future(
        dlq.receive(
            max_messages=max_messages,
            wait_seconds=_WAIT_SECONDS,
            visibility_seconds=_HOLD_SECONDS,
        )
    )
    try:
        return await asyncio.shield(receive)
    except asyncio.CancelledError:
        with contextlib.suppress(Exception):  # a failed receive took nothing to release
            await _release(dlq, await receive)
        raise


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
