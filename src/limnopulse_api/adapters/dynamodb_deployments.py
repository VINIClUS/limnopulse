"""DynamoDB persistence for temporal Component Deployments.

Every Deployment is stored three times in one transaction: the tenant mirror
`TENANT#<t>/DEPLOYMENT#<dep>`, the immutable history row
`COMPONENT#<c>/DEPLOYMENT#<started_at fixed-6>#<dep>` and, while active, the current pointer
`COMPONENT#<c>/DEPLOYMENT#CURRENT`. A per-Component marker `COMPONENT#<c>/DEPLOYMENT#MARKER`
holds the latest `last_ended_at` and a `seq` counter bumped by every start and end, so a start
that observed an older marker cannot commit after a concurrent end (D10): intervals never
overlap. CURRENT and MARKER sort after `deployment_history_bounds()`, so history queries never
see them. No Scan and no GSI.
"""

from collections.abc import Mapping
from datetime import datetime
from typing import Any, Literal

from limnopulse_api.adapters.dynamodb_v2 import (
    DynamoConditionalConflict,
    DynamoV2KeyBuilder,
    DynamoV2RepositoryBase,
    canonical_timestamp_key,
    parse_canonical_timestamp,
)
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.assets import AssetStatus, SiteStatus
from limnopulse_api.domain.deployments import Deployment, DeploymentStatus
from limnopulse_api.domain.devices_v2 import ComponentStatus
from limnopulse_api.domain.v2_common import IdempotencyReceipt

DEPLOYMENT_ENTITY_TYPE = "deployment"
DEPLOYMENT_HISTORY_ENTITY_TYPE = "deployment_history"
DEPLOYMENT_CURRENT_ENTITY_TYPE = "deployment_current"
DEPLOYMENT_MARKER_ENTITY_TYPE = "deployment_marker"
DEPLOYMENT_MARKER_SK = "DEPLOYMENT#MARKER"

DeploymentItemForm = Literal["mirror", "history", "current"]

IDEMPOTENCY_CONFLICT = "idempotency key reused with a different request"
ACTIVE_DEPLOYMENT_CONFLICT = "component already has an active deployment"
OVERLAP_CONFLICT = "deployment overlaps the previous interval"
DEPLOYMENT_NOT_ACTIVE = "deployment is not active"
DEPLOYMENT_VERSION_CONFLICT = "deployment version conflict"

# Transaction action indexes used to classify a cancelled start.
_START_MIRROR_INDEX = 0
_START_HISTORY_INDEX = 1
_START_CURRENT_INDEX = 2
_START_MARKER_INDEX = 3
_START_RECEIPT_INDEX = 4
_START_COMPONENT_CHECK_INDEX = 5
_START_ASSET_CHECK_INDEX = 6
_START_SITE_CHECK_INDEX = 7

# Transaction action indexes used to classify a cancelled end.
_END_MARKER_INDEX = 3

_ACTIVE_TENANT_CONDITION = "attribute_exists(PK) AND #status = :active AND #tenant_id = :tenant_id"
_KEYS = DynamoV2KeyBuilder()


def deployment_marker_key(component_id: str) -> dict[str, str]:
    """Key of the per-Component D10 marker; its SK lies outside the history bounds."""
    return {"PK": _KEYS.component(component_id)["PK"], "SK": DEPLOYMENT_MARKER_SK}


def deployment_marker_item(
    tenant_id: str, component_id: str, *, seq: int, last_ended_at: datetime | None
) -> dict[str, Any]:
    """Marker row for writers that persist Deployments directly (e.g. compatibility/migration)."""
    item: dict[str, Any] = {
        **deployment_marker_key(component_id),
        "entity_type": DEPLOYMENT_MARKER_ENTITY_TYPE,
        "tenant_id": tenant_id,
        "component_id": component_id,
        "seq": seq,
    }
    if last_ended_at is not None:
        item["last_ended_at"] = canonical_timestamp_key(last_ended_at)
    return item


def deployment_item_key(deployment: Deployment, form: DeploymentItemForm) -> dict[str, str]:
    if form == "mirror":
        return _KEYS.deployment_mirror(deployment.tenant_id, deployment.deployment_id)
    if form == "history":
        return _KEYS.deployment_history(
            deployment.component_id, deployment.started_at, deployment.deployment_id
        )
    return _KEYS.current_deployment(deployment.component_id)


_ENTITY_TYPES: dict[str, str] = {
    "mirror": DEPLOYMENT_ENTITY_TYPE,
    "history": DEPLOYMENT_HISTORY_ENTITY_TYPE,
    "current": DEPLOYMENT_CURRENT_ENTITY_TYPE,
}


def deployment_to_item(
    deployment: Deployment, *, form: DeploymentItemForm = "mirror"
) -> dict[str, Any]:
    """Tenant mirror (default), timestamped history row, or current pointer of a Deployment."""
    item: dict[str, Any] = {
        **deployment_item_key(deployment, form),
        "entity_type": _ENTITY_TYPES[form],
        "tenant_id": deployment.tenant_id,
        "deployment_id": deployment.deployment_id,
        "component_id": deployment.component_id,
        "site_id": deployment.site_id,
        "asset_id": deployment.asset_id,
        "started_at": canonical_timestamp_key(deployment.started_at),
        "status": deployment.status.value,
        "created_at": canonical_timestamp_key(deployment.created_at),
        "updated_at": canonical_timestamp_key(deployment.updated_at),
        "version": deployment.version,
        "schema_version": deployment.schema_version,
    }
    if deployment.ended_at is not None:
        item["ended_at"] = canonical_timestamp_key(deployment.ended_at)
    return item


def deployment_from_item(item: Mapping[str, Any]) -> Deployment:
    ended_at = item.get("ended_at")
    return Deployment(
        tenant_id=item["tenant_id"],
        deployment_id=item["deployment_id"],
        component_id=item["component_id"],
        site_id=item["site_id"],
        asset_id=item["asset_id"],
        started_at=parse_canonical_timestamp(item["started_at"]),
        ended_at=None if ended_at is None else parse_canonical_timestamp(ended_at),
        status=DeploymentStatus(item["status"]),
        created_at=parse_canonical_timestamp(item["created_at"]),
        updated_at=parse_canonical_timestamp(item["updated_at"]),
        version=int(item["version"]),
    )


def _contains(deployment: Deployment, observed_at: datetime) -> bool:
    if observed_at < deployment.started_at:
        return False
    return deployment.ended_at is None or observed_at < deployment.ended_at


class DynamoDeploymentRepository(DynamoV2RepositoryBase):
    """Implements `DeploymentRepository` over the shared single table."""

    # Reads -----------------------------------------------------------------------------------

    async def get_deployment(self, tenant_id: str, deployment_id: str) -> Deployment | None:
        item = await self.get_item(
            self.keys.deployment_mirror(tenant_id, deployment_id), consistent=True
        )
        if item is None or item.get("tenant_id") != tenant_id:
            return None
        return deployment_from_item(item)

    async def list_component_deployments(
        self, tenant_id: str, component_id: str
    ) -> list[Deployment]:
        lower, upper = self.keys.deployment_history_bounds()
        items = await self.query_range(
            self.keys.component(component_id)["PK"], lower=lower, upper=upper, consistent=True
        )
        return [deployment_from_item(item) for item in items if item.get("tenant_id") == tenant_id]

    async def get_current_deployment(self, tenant_id: str, component_id: str) -> Deployment | None:
        item = await self.get_item(self.keys.current_deployment(component_id), consistent=True)
        if item is None or item.get("tenant_id") != tenant_id:
            return None
        return deployment_from_item(item)

    async def get_deployment_at(
        self, tenant_id: str, component_id: str, observed_at: datetime
    ) -> Deployment | None:
        """The interval `[started_at, ended_at)` containing `observed_at`, if any."""
        items = await self.query_range(
            self.keys.component(component_id)["PK"],
            lower=self.keys.deployment_history_bounds()[0],
            upper=self.keys.deployment_history_upper_bound(observed_at),
            ascending=False,
            limit=1,
        )
        if not items or items[0].get("tenant_id") != tenant_id:
            return None
        deployment = deployment_from_item(items[0])
        return deployment if _contains(deployment, observed_at) else None

    # Writes ----------------------------------------------------------------------------------

    async def start_deployment(
        self, deployment: Deployment, receipt: IdempotencyReceipt
    ) -> Deployment:
        if deployment.status is not DeploymentStatus.ACTIVE:
            raise ValueError("only an active deployment can be started")
        marker = await self.get_item(
            deployment_marker_key(deployment.component_id), consistent=True
        )
        await self._ensure_after_previous(deployment, marker)
        actions = [
            {"Put": self.put_new_request(deployment_to_item(deployment, form="mirror"))},
            {"Put": self.put_new_request(deployment_to_item(deployment, form="history"))},
            {"Put": self.put_new_request(deployment_to_item(deployment, form="current"))},
            {"Update": self._start_marker_request(deployment, marker)},
            {"Put": self.receipt_put_request(receipt)},
            # Parents are fenced at commit time, not only by the service's earlier reads.
            {
                "ConditionCheck": self._active_check_request(
                    self.keys.component(deployment.component_id),
                    deployment.tenant_id,
                    ComponentStatus.ACTIVE.value,
                )
            },
            {
                "ConditionCheck": self._active_check_request(
                    self.keys.asset(deployment.tenant_id, deployment.asset_id),
                    deployment.tenant_id,
                    AssetStatus.ACTIVE.value,
                )
            },
            {
                "ConditionCheck": self._active_check_request(
                    self.keys.site(deployment.tenant_id, deployment.site_id),
                    deployment.tenant_id,
                    SiteStatus.ACTIVE.value,
                )
            },
        ]
        try:
            await self.transact_write(actions)
        except DynamoConditionalConflict as exc:
            raise ConflictError(_classify_start_conflict(exc.failed_indexes)) from exc
        return deployment

    async def end_deployment(
        self,
        tenant_id: str,
        deployment_id: str,
        *,
        expected_version: int,
        ended_at: datetime,
    ) -> Deployment:
        current = await self.get_deployment(tenant_id, deployment_id)
        if current is None:
            raise NotFoundError("deployment not found")
        if current.status is not DeploymentStatus.ACTIVE:
            raise ConflictError(DEPLOYMENT_NOT_ACTIVE)
        if current.version != expected_version:
            raise ConflictError(DEPLOYMENT_VERSION_CONFLICT)
        # Only status, end, updated_at and version change; target and start are immutable.
        ended = Deployment.model_validate(
            {
                **current.model_dump(),
                "ended_at": ended_at,
                "status": DeploymentStatus.ENDED,
                "updated_at": max(self.clock(), current.updated_at),
                "version": expected_version + 1,
            }
        )
        marker = await self.get_item(deployment_marker_key(current.component_id), consistent=True)
        changes = {
            "status": ended.status.value,
            "ended_at": canonical_timestamp_key(ended_at),
            "updated_at": canonical_timestamp_key(ended.updated_at),
            "version": ended.version,
        }
        actions = [
            {
                "Update": self._end_row_request(
                    deployment_item_key(current, "mirror"), changes, current
                )
            },
            {
                "Update": self._end_row_request(
                    deployment_item_key(current, "history"), changes, current
                )
            },
            {
                # Delete takes the same Key/Condition shape as a ConditionCheck.
                "Delete": self.condition_check_request(
                    deployment_item_key(current, "current"),
                    condition=(
                        "attribute_exists(PK) AND #deployment_id = :deployment_id"
                        " AND #version = :expected_version AND #tenant_id = :tenant_id"
                    ),
                    names={
                        "#deployment_id": "deployment_id",
                        "#version": "version",
                        "#tenant_id": "tenant_id",
                    },
                    values={
                        ":deployment_id": deployment_id,
                        ":expected_version": expected_version,
                        ":tenant_id": tenant_id,
                    },
                )
            },
            {"Update": self._end_marker_request(ended, marker)},
        ]
        try:
            await self.transact_write(actions)
        except DynamoConditionalConflict as exc:
            if _END_MARKER_INDEX in exc.failed_indexes and len(exc.failed_indexes) == 1:
                raise ConflictError("deployment changed concurrently") from exc
            raise ConflictError(DEPLOYMENT_VERSION_CONFLICT) from exc
        return ended

    # Internals -------------------------------------------------------------------------------

    async def _ensure_after_previous(
        self, deployment: Deployment, marker: Mapping[str, Any] | None
    ) -> None:
        """Half-open intervals: a start equal to the previous end is the exact transition."""
        last_ended_at = None if marker is None else marker.get("last_ended_at")
        if last_ended_at is not None and deployment.started_at < parse_canonical_timestamp(
            last_ended_at
        ):
            raise ConflictError(OVERLAP_CONFLICT)
        # Defensive for history written without a marker (compatibility/migration writers).
        lower, upper = self.keys.deployment_history_bounds()
        latest = await self.query_range(
            self.keys.component(deployment.component_id)["PK"],
            lower=lower,
            upper=upper,
            ascending=False,
            limit=1,
            consistent=True,
        )
        if not latest:
            return
        previous = deployment_from_item(latest[0])
        if previous.ended_at is None:
            raise ConflictError(ACTIVE_DEPLOYMENT_CONFLICT)
        if deployment.started_at < previous.ended_at:
            raise ConflictError(OVERLAP_CONFLICT)

    def _active_check_request(
        self, key: Mapping[str, str], tenant_id: str, active: str
    ) -> dict[str, Any]:
        return self.condition_check_request(
            key,
            condition=_ACTIVE_TENANT_CONDITION,
            names={"#status": "status", "#tenant_id": "tenant_id"},
            values={":active": active, ":tenant_id": tenant_id},
        )

    def _start_marker_request(
        self, deployment: Deployment, marker: Mapping[str, Any] | None
    ) -> dict[str, Any]:
        names = {
            "#seq": "seq",
            "#entity_type": "entity_type",
            "#tenant_id": "tenant_id",
            "#component_id": "component_id",
        }
        values: dict[str, Any] = {
            ":entity_type": DEPLOYMENT_MARKER_ENTITY_TYPE,
            ":tenant_id": deployment.tenant_id,
            ":component_id": deployment.component_id,
        }
        if marker is None:
            condition = "attribute_not_exists(PK)"
            values[":next_seq"] = 1
        else:
            observed_seq = int(marker["seq"])
            names["#last_ended_at"] = "last_ended_at"
            condition = (
                "#seq = :observed_seq AND #tenant_id = :tenant_id AND "
                "(attribute_not_exists(#last_ended_at) OR #last_ended_at <= :started_at)"
            )
            values.update(
                {
                    ":observed_seq": observed_seq,
                    ":next_seq": observed_seq + 1,
                    ":started_at": canonical_timestamp_key(deployment.started_at),
                }
            )
        return self.update_request(
            deployment_marker_key(deployment.component_id),
            update_expression=(
                "SET #seq = :next_seq, #entity_type = :entity_type, "
                "#tenant_id = :tenant_id, #component_id = :component_id"
            ),
            condition=condition,
            names=names,
            values=values,
        )

    def _end_marker_request(
        self, ended: Deployment, marker: Mapping[str, Any] | None
    ) -> dict[str, Any]:
        if ended.ended_at is None:
            raise ValueError("an ended deployment needs ended_at")
        names = {
            "#seq": "seq",
            "#last_ended_at": "last_ended_at",
            "#entity_type": "entity_type",
            "#tenant_id": "tenant_id",
            "#component_id": "component_id",
        }
        values: dict[str, Any] = {
            ":last_ended_at": canonical_timestamp_key(ended.ended_at),
            ":entity_type": DEPLOYMENT_MARKER_ENTITY_TYPE,
            ":tenant_id": ended.tenant_id,
            ":component_id": ended.component_id,
        }
        if marker is None:
            condition = "attribute_not_exists(PK)"
            values[":next_seq"] = 1
        else:
            observed_seq = int(marker["seq"])
            condition = "#seq = :observed_seq AND #tenant_id = :tenant_id"
            values.update({":observed_seq": observed_seq, ":next_seq": observed_seq + 1})
        return self.update_request(
            deployment_marker_key(ended.component_id),
            update_expression=(
                "SET #seq = :next_seq, #last_ended_at = :last_ended_at, "
                "#entity_type = :entity_type, #tenant_id = :tenant_id, "
                "#component_id = :component_id"
            ),
            condition=condition,
            names=names,
            values=values,
        )

    def _end_row_request(
        self, key: Mapping[str, str], changes: Mapping[str, Any], current: Deployment
    ) -> dict[str, Any]:
        names = {
            "#status": "status",
            "#ended_at": "ended_at",
            "#updated_at": "updated_at",
            "#version": "version",
            "#tenant_id": "tenant_id",
            "#deployment_id": "deployment_id",
        }
        return self.update_request(
            key,
            update_expression=(
                "SET #status = :status, #ended_at = :ended_at, "
                "#updated_at = :updated_at, #version = :version"
            ),
            condition=(
                "attribute_exists(PK) AND #version = :expected_version AND #status = :active"
                " AND #tenant_id = :tenant_id AND #deployment_id = :deployment_id"
            ),
            names=names,
            values={
                ":status": changes["status"],
                ":ended_at": changes["ended_at"],
                ":updated_at": changes["updated_at"],
                ":version": changes["version"],
                ":expected_version": current.version,
                ":active": DeploymentStatus.ACTIVE.value,
                ":tenant_id": current.tenant_id,
                ":deployment_id": current.deployment_id,
            },
        )


def _classify_start_conflict(failed_indexes: tuple[int, ...]) -> str:
    # The receipt wins so the service can reread it and replay a same-key/same-body snapshot.
    if _START_RECEIPT_INDEX in failed_indexes:
        return IDEMPOTENCY_CONFLICT
    if _START_CURRENT_INDEX in failed_indexes:
        return ACTIVE_DEPLOYMENT_CONFLICT
    if _START_MARKER_INDEX in failed_indexes:
        return OVERLAP_CONFLICT
    if _START_COMPONENT_CHECK_INDEX in failed_indexes:
        return "component is not active"
    if _START_ASSET_CHECK_INDEX in failed_indexes:
        return "asset is not active"
    if _START_SITE_CHECK_INDEX in failed_indexes:
        return "site is not active"
    if {_START_MIRROR_INDEX, _START_HISTORY_INDEX} & set(failed_indexes):
        return "deployment already exists"
    return "deployment conflict"
