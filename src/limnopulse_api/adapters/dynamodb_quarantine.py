"""DynamoDB persistence for telemetry quarantine rows (D9).

Rows live under the dedicated partition `QUARANTINE#<tenant_id>` with the time-ordered SK
`<received_at_key>#<quarantine_id>`. Every write is a single-item conditional request, because
the worker principal has no `TransactWriteItems`/`BatchWriteItem`:

- a create is `PutItem` with `attribute_not_exists(PK)`; a conditional failure means the row
  already exists and counts as success, so a redelivery never resets `replayed`/`dismissed`;
- a status transition is a version-conditioned `UpdateItem` (`open → replayed|dismissed`).

Expiry is logical: a row is absent once `created_at + QUARANTINE_TTL <= now`; the numeric
`expires_at` attribute only drives TTL cleanup. Listing is a `Query` resumed strictly after
`after_sk`, filtered on status and reason in code. No Scan and no GSI.
"""

from asyncio import to_thread
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from limnopulse_api.adapters.dynamodb_v2 import (
    DynamoConditionalConflict,
    DynamoV2RepositoryBase,
    canonical_timestamp_key,
    quarantine_entry_from_item,
    quarantine_entry_to_item,
)
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.ingestion import QuarantineEntry, QuarantineEntryStatus, QuarantinePage

QUARANTINE_NOT_FOUND = "quarantine entry not found"
QUARANTINE_TERMINAL = "quarantine entry is terminal"
QUARANTINE_VERSION_CONFLICT = "quarantine entry version conflict"
# Sorts before every quarantine SK: each starts with a canonical timestamp in [1970, 2262).
_FIRST_KEY = "0"


class DynamoQuarantineRepository(DynamoV2RepositoryBase):
    async def put_quarantine_entries(self, entries: Sequence[QuarantineEntry]) -> None:
        for entry in entries:
            try:
                await self.put_item(self.put_new_request(quarantine_entry_to_item(entry)))
            except DynamoConditionalConflict:
                continue  # already persisted: never reset a row on redelivery

    async def list_quarantine_entries(
        self,
        tenant_id: str,
        *,
        status: QuarantineEntryStatus | None = None,
        reason: str | None = None,
        after_sk: str | None,
        limit: int,
    ) -> QuarantinePage:
        if limit < 1:
            raise ValueError("limit must be at least 1")
        entries: list[QuarantineEntry] = []
        cursor = after_sk
        now = self.clock()
        while True:
            items, resume = await to_thread(self._page_sync, tenant_id, cursor, limit)
            for index, item in enumerate(items):
                entry = quarantine_entry_from_item(item)
                if not self._is_live(entry, now) or not _matches(entry, status, reason):
                    continue
                entries.append(entry)
                if len(entries) == limit:
                    remaining = resume is not None or index < len(items) - 1
                    return QuarantinePage(
                        entries=tuple(entries),
                        next_after_sk=entry.entry_key if remaining else None,
                    )
            if resume is None:
                return QuarantinePage(entries=tuple(entries), next_after_sk=None)
            cursor = resume

    async def get_quarantine_entry(self, tenant_id: str, entry_key: str) -> QuarantineEntry | None:
        item = await self.get_item(self._key(tenant_id, entry_key), consistent=True)
        if item is None or item.get("tenant_id") != tenant_id:
            return None
        entry = quarantine_entry_from_item(item)
        return entry if self._is_live(entry, self.clock()) else None

    async def transition_quarantine_entry(
        self,
        tenant_id: str,
        entry_key: str,
        target: QuarantineEntryStatus,
        *,
        expected_version: int,
        at: datetime,
        replayed_ingress_id: str | None = None,
    ) -> QuarantineEntry:
        target = QuarantineEntryStatus(target)
        if (target == QuarantineEntryStatus.REPLAYED) != (replayed_ingress_id is not None):
            raise ValueError("replayed_ingress_id is set exactly for a replay")
        current = await self.get_quarantine_entry(tenant_id, entry_key)
        if current is None:
            raise NotFoundError(QUARANTINE_NOT_FOUND)
        if current.status != QuarantineEntryStatus.OPEN or target == QuarantineEntryStatus.OPEN:
            raise ConflictError(QUARANTINE_TERMINAL)
        if current.version != expected_version:
            raise ConflictError(QUARANTINE_VERSION_CONFLICT)
        # Validates the target row (status/replayed_ingress_id pairing) before writing it.
        updated = QuarantineEntry.model_validate(
            {
                **current.model_dump(),
                "status": target,
                "replayed_ingress_id": replayed_ingress_id,
                "updated_at": at,
                "version": expected_version + 1,
            }
        )
        assignments = "#status = :status, updated_at = :updated_at, #version = :next_version"
        values: dict[str, Any] = {
            ":status": updated.status.value,
            ":updated_at": canonical_timestamp_key(updated.updated_at),
            ":next_version": updated.version,
            ":v": expected_version,
        }
        if replayed_ingress_id is not None:
            assignments += ", replayed_ingress_id = :replayed_ingress_id"
            values[":replayed_ingress_id"] = replayed_ingress_id
        request = self.update_request(
            self._key(tenant_id, entry_key),
            update_expression=f"SET {assignments}",
            # Every status change bumps the version, so the version alone fences a racing write.
            condition="attribute_exists(PK) AND #version = :v",
            names={"#status": "status", "#version": "version"},
            values=values,
            return_values="ALL_NEW",
        )
        try:
            item = await self.update_item(request)
        except DynamoConditionalConflict as exc:
            raise ConflictError(QUARANTINE_VERSION_CONFLICT) from exc
        return quarantine_entry_from_item(item)

    # Internals -------------------------------------------------------------------------------

    def _key(self, tenant_id: str, entry_key: str) -> dict[str, str]:
        return {"PK": self.keys.quarantine_partition(tenant_id), "SK": entry_key}

    @staticmethod
    def _is_live(entry: QuarantineEntry, now: datetime) -> bool:
        return now < entry.expires_at

    def _page_sync(
        self, tenant_id: str, after_sk: str | None, limit: int
    ) -> tuple[list[dict[str, Any]], str | None]:
        """One `Query` page of at most `limit` rows strictly after `after_sk`, plus the SK to
        resume from (`LastEvaluatedKey`), or `None` when the partition is exhausted."""
        values = {
            ":pk": self.keys.quarantine_partition(tenant_id),
            ":sk_lower": _FIRST_KEY if after_sk is None else after_sk,
        }
        response = self.client.query(
            TableName=self.table_name,
            KeyConditionExpression="PK = :pk AND SK > :sk_lower",
            ExpressionAttributeValues=self.serialize_values(values),
            Limit=limit,
        )
        items = [self.deserialize_item(item) for item in response.get("Items", [])]
        last = response.get("LastEvaluatedKey")
        return items, None if last is None else self.deserialize_item(last)["SK"]


def _matches(
    entry: QuarantineEntry, status: QuarantineEntryStatus | None, reason: str | None
) -> bool:
    return (status is None or entry.status == status) and (reason is None or entry.reason == reason)
