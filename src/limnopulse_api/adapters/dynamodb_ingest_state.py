"""DynamoDB persistence for ingest state and connector health (D11).

Rows: `COMPONENT#<c>/INGEST_STATE#CURRENT`, `DEVICE#<d>/INGEST_STATE#CURRENT` and
`INTEGRATION_ACCOUNT#<ia>/CONNECTOR_HEALTH#CURRENT`. Every attribute is a monotone high-water
mark written by its own conditional `UpdateItem` (no read-modify-write, no version):

- simple marks: `attribute_not_exists(#m) OR #m < :t`;
- paired marks, written together on the timestamp `#m` with the tie-breaker `#tb` (the sequence
  or the reason): `attribute_not_exists(#m) OR #m < :t OR (#m = :t AND #tb < :tb)`.

A `ConditionalCheckFailed` means the stored mark is already newer and counts as success, so
updates are idempotent and order-independent across concurrent workers and redeliveries.
Identity attributes are set with `if_not_exists` (the first writer wins). Timestamps are the
canonical fixed-6 `Z` strings, so lexicographic order is chronological.
"""

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from limnopulse_api.adapters.dynamodb_v2 import (
    DynamoConditionalConflict,
    DynamoV2RepositoryBase,
    canonical_timestamp_key,
    component_ingest_state_from_item,
    connector_health_from_item,
)
from limnopulse_api.domain.ingestion import ComponentIngestState, ConnectorHealth, IngestMarks

_SIMPLE = "attribute_not_exists(#m) OR #m < :t"
_PAIRED = "attribute_not_exists(#m) OR #m < :t OR (#m = :t AND #tb < :tb)"


class DynamoIngestStateRepository(DynamoV2RepositoryBase):
    async def get_component_states(
        self, tenant_id: str, component_ids: Sequence[str]
    ) -> dict[str, ComponentIngestState]:
        states: dict[str, ComponentIngestState] = {}
        for component_id in dict.fromkeys(component_ids):
            item = await self.get_item(
                self.keys.component_ingest_state(component_id), consistent=True
            )
            if item is not None and item.get("tenant_id") == tenant_id:
                states[component_id] = component_ingest_state_from_item(item)
        return states

    async def apply_ingest_marks(self, marks: IngestMarks) -> None:
        # IngestMarks guarantees a verified device_id whenever component marks are present.
        for mark in marks.component_marks:
            key = self.keys.component_ingest_state(mark.component_id)
            identity = {
                "entity_type": "component_ingest_state",
                "tenant_id": marks.tenant_id,
                "component_id": mark.component_id,
                "device_id": marks.device_id,
            }
            await self._advance(key, identity, "last_received_at", mark.received_at)
            await self._advance(key, identity, "last_ingested_at", mark.ingested_at)
            await self._advance(
                key,
                identity,
                "last_observed_at",
                mark.observed_at,
                breaker=("last_observed_sequence", mark.sequence),
            )
        if marks.device_mark is not None and marks.device_id is not None:
            key = self.keys.device_ingest_state(marks.device_id)
            identity = {
                "entity_type": "device_ingest_state",
                "tenant_id": marks.tenant_id,
                "device_id": marks.device_id,
            }
            await self._advance(key, identity, "last_received_at", marks.device_mark.received_at)
            await self._advance(key, identity, "last_ingested_at", marks.device_mark.ingested_at)
        connector = marks.connector_mark
        key = self.keys.connector_health(marks.integration_account_id)
        identity = {
            "entity_type": "connector_health",
            "tenant_id": marks.tenant_id,
            "integration_account_id": marks.integration_account_id,
        }
        await self._advance(key, identity, "last_received_at", connector.received_at)
        await self._advance(key, identity, "last_accepted_at", connector.accepted_at)
        await self._advance(
            key,
            identity,
            "last_quarantined_at",
            connector.quarantined_at,
            breaker=("last_quarantine_reason", connector.quarantine_reason),
        )
        await self._advance(
            key,
            identity,
            "last_fenced_at",
            connector.fenced_at,
            breaker=("last_fence_reason", connector.fence_reason),
        )

    async def get_connector_health(
        self, tenant_id: str, integration_account_id: str
    ) -> ConnectorHealth | None:
        item = await self.get_item(
            self.keys.connector_health(integration_account_id), consistent=True
        )
        if item is None or item.get("tenant_id") != tenant_id:
            return None
        return connector_health_from_item(item)

    async def _advance(
        self,
        key: Mapping[str, str],
        identity: Mapping[str, Any],
        name: str,
        value: datetime | None,
        *,
        breaker: tuple[str, Any] | None = None,
    ) -> None:
        """One conditional `UpdateItem` raising mark `name` to `value` (with its tie-breaker)."""
        if value is None:
            return
        names = {"#m": name}
        values: dict[str, Any] = {":t": canonical_timestamp_key(value)}
        assignments = ["#m = :t"]
        condition = _SIMPLE
        if breaker is not None:
            names["#tb"] = breaker[0]
            values[":tb"] = breaker[1]
            assignments.append("#tb = :tb")
            condition = _PAIRED
        for attribute, identity_value in identity.items():
            assignments.append(f"{attribute} = if_not_exists({attribute}, :{attribute})")
            values[f":{attribute}"] = identity_value
        request = self.update_request(
            key,
            update_expression="SET " + ", ".join(assignments),
            condition=condition,
            names=names,
            values=values,
        )
        try:
            await self.update_item(request)
        except DynamoConditionalConflict:
            return  # the stored mark is already newer (or equal): success
