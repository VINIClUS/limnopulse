"""`/v1` Pond/Device persistence that projects every mutation onto the v2 domain atomically.

Reads come only from the legacy `TENANT#<t>/POND#` and `TENANT#<t>/DEVICE#` rows (paginated
Query or GetItem) and never create or promote data; a native v2 Device row has no legacy
`pond_id` and is invisible here.

Every write is one `TransactWriteItems` that keeps the legacy rows authoritative for `/v1`
(their `version` is the `/v1` optimistic fence) and fences each v2 row on its freshly observed
version:

- Pond create: `POND#` + Asset + PondProfile, plus the deterministic default Site (Put when
  absent, ConditionCheck when present), so the first Pond of a tenant converges on one Site
  under concurrent creators.
- Pond update: `POND#` + Asset (+ PondProfile when the description changes).
- Device create: shared Device tenant row + lookup, deterministic default Probe Component
  (tenant + canonical rows) and its Deployment (mirror, history, CURRENT, marker).
- Device `pond_id` change: ends the current interval at the transition instant and starts a
  new one; history is never rewritten or reassigned.

A write that touches a Pond or Device not yet migrated provisions its deterministic v2 rows in
the same transaction with the M1-00 builders and M1-01/02/04 codecs, so the content equals what
the M1 migration writes for the same legacy row. Provisioning races are re-read and retried a
bounded number of times; every other conditional failure is a `ConflictError`.
"""

import asyncio
import random
from collections.abc import Awaitable, Callable, Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime
from typing import Any

from botocore.exceptions import ClientError

from limnopulse_api.adapters.dynamodb import (
    DynamoKeyBuilder,
    is_legacy_device_item,
    legacy_device_from_item,
    legacy_pond_from_item,
    legacy_tenant_from_item,
)
from limnopulse_api.adapters.dynamodb_deployments import (
    MARKER_LAST_ENDED_AT_ATTRIBUTE,
    MARKER_SEQ_ATTRIBUTE,
    deployment_from_item,
    deployment_item_key,
    deployment_marker_advance_request,
    deployment_marker_item,
    deployment_marker_key,
    deployment_to_item,
)
from limnopulse_api.adapters.dynamodb_devices_v2 import component_from_item, component_to_item
from limnopulse_api.adapters.dynamodb_sites_assets import (
    asset_from_item,
    asset_to_item,
    pond_profile_from_item,
    pond_profile_to_item,
    site_from_item,
    site_to_item,
)
from limnopulse_api.adapters.dynamodb_v2 import (
    DynamoConditionalConflict,
    DynamoV2RepositoryBase,
    canonical_timestamp_key,
    parse_canonical_timestamp,
    utc_now,
)
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.assets import AssetStatus, SiteStatus
from limnopulse_api.domain.deployments import Deployment, DeploymentStatus
from limnopulse_api.domain.devices_v2 import ComponentStatus, DeviceComponent, DeviceStatus
from limnopulse_api.domain.entities import Device, Pond
from limnopulse_api.domain.ids import (
    asset_id_for_legacy_pond,
    default_component_id_for_legacy_device,
    default_site_id,
    new_deployment_id,
)
from limnopulse_api.domain.legacy_projection import (
    asset_and_profile_for_legacy_pond,
    asset_status_from_legacy,
    component_for_legacy_device,
    default_site_for_tenant,
    deployment_for_legacy_device,
    device_status_from_legacy,
)
from limnopulse_api.services.v1_pond_device_projection import (
    DEVICE_COMPONENT_LINK_ATTRIBUTE,
    LEGACY_AUTH_TYPE,
    POND_ASSET_LINK_ATTRIBUTE,
    POND_SITE_LINK_ATTRIBUTE,
    PROJECTION_VERSION_ATTRIBUTE,
    V2_PROJECTION_VERSION,
    ended_deployment,
    legacy_now,
    started_deployment,
    transition_instant,
)

MAX_PROJECTION_ACTIONS = 25
MAX_ATTEMPTS = 3
SITE_NOT_AVAILABLE = "site not available"
COMPONENT_NOT_ACTIVE = "component is not active"

# Failures caused by a concurrent writer provisioning or moving the same v2 rows: re-read and
# retry. Any other failed label is a definitive conflict.
_RETRYABLE_LABELS = frozenset(
    {
        "site_put",
        "asset_put",
        "profile_put",
        "pond_link",
        "asset_update",
        "profile_update",
        "component_put",
        "deployment_put",
        "deployment_end",
        "current_put",
        "marker",
    }
)
_CONFLICT_MESSAGES = {
    "pond_put": "pond already exists",
    "pond_update": "pond version conflict",
    "device_put": "device already exists",
    "device_update": "device version conflict",
    "asset_check": "pond is not active",
    "site_check": SITE_NOT_AVAILABLE,
    "component_check": COMPONENT_NOT_ACTIVE,
}
# Site requirements merge: a Put implies an active check, which implies existence.
_SITE_STRENGTH = {"exists": 1, "active": 2, "put": 3}

# Reads use the legacy key shapes verbatim so `/v1` reads match the legacy repository.
_LEGACY_KEYS = DynamoKeyBuilder()
_ACTIVE_TENANT_CONDITION = "attribute_exists(PK) AND #status = :active AND #tenant_id = :tenant_id"
_TENANT_CONDITION = "attribute_exists(PK) AND #tenant_id = :tenant_id"
_FENCE_CONDITION = (
    "attribute_exists(PK) AND #version = :expected_version AND #tenant_id = :tenant_id"
)


class _Plan:
    """Ordered transaction with distinct keys and a label per action for conflict mapping."""

    def __init__(self) -> None:
        self.actions: list[dict[str, Any]] = []
        self.labels: list[str] = []
        self._indexes: dict[tuple[str, str], int] = {}
        self._site_strength: dict[tuple[str, str], int] = {}

    def has(self, key: Mapping[str, str]) -> bool:
        return (key["PK"], key["SK"]) in self._indexes

    def add(self, label: str, key: Mapping[str, str], action: dict[str, Any]) -> None:
        identity = (key["PK"], key["SK"])
        if identity in self._indexes:
            raise ValueError(f"duplicate transaction key {identity}")
        self._indexes[identity] = len(self.actions)
        self.actions.append(action)
        self.labels.append(label)

    def add_site(
        self, strength: str, label: str, key: Mapping[str, str], action: dict[str, Any]
    ) -> None:
        identity = (key["PK"], key["SK"])
        index = self._indexes.get(identity)
        if index is None:
            self.add(label, key, action)
        elif _SITE_STRENGTH[strength] > self._site_strength[identity]:
            self.actions[index], self.labels[index] = action, label
        else:
            return
        self._site_strength[identity] = _SITE_STRENGTH[strength]

    def failed_labels(self, indexes: tuple[int, ...]) -> set[str]:
        return {self.labels[index] for index in indexes if index < len(self.labels)}


def _only_transaction_conflicts(exc: ClientError) -> bool:
    response = exc.response
    if response.get("Error", {}).get("Code") != "TransactionCanceledException":
        return False
    reasons = response.get("CancellationReasons")
    if not isinstance(reasons, list):
        return False
    codes = {reason.get("Code") for reason in reasons if isinstance(reason, Mapping)}
    codes.discard("None")
    codes.discard(None)
    return codes == {"TransactionConflict"}


@contextmanager
def _v2_compatible(entity: str) -> Iterator[None]:
    """Legacy identifiers the v2 codecs reject (non-canonical `dev_`, '#', ...) are a 409."""
    try:
        yield
    except ValueError as exc:  # includes pydantic ValidationError
        raise ConflictError(f"{entity} is not compatible with the v2 projection") from exc


async def _jittered_sleep(attempt: int) -> None:
    await asyncio.sleep(random.uniform(0.01, 0.05) * attempt)


class DynamoV1PondDeviceProjection(DynamoV2RepositoryBase):
    """Implements `V1PondDeviceProjection` over the shared single table."""

    def __init__(
        self,
        table_name: str,
        client: Any,
        *,
        clock: Callable[[], datetime] = utc_now,
        retry_sleep: Callable[[int], Awaitable[None]] = _jittered_sleep,
    ) -> None:
        super().__init__(table_name, client, clock=clock)
        self._retry_sleep = retry_sleep

    # Reads (legacy rows only) ------------------------------------------------------------

    async def list_ponds(self, tenant_id: str) -> list[Pond]:
        items = await self.query_prefix(f"TENANT#{tenant_id}", self.keys.LEGACY_POND_PREFIX)
        return [legacy_pond_from_item(item) for item in items if item.get("tenant_id") == tenant_id]

    async def get_pond(self, tenant_id: str, pond_id: str) -> Pond | None:
        item = await self.get_item(_LEGACY_KEYS.pond(tenant_id, pond_id))
        if item is None or item.get("tenant_id") != tenant_id:
            return None
        return legacy_pond_from_item(item)

    async def list_devices(self, tenant_id: str) -> list[Device]:
        items = await self.query_prefix(f"TENANT#{tenant_id}", self.keys.DEVICE_PREFIX)
        return [
            legacy_device_from_item(item)
            for item in items
            if item.get("tenant_id") == tenant_id and is_legacy_device_item(item)
        ]

    async def get_device(self, tenant_id: str, device_id: str) -> Device | None:
        item = await self.get_item(_LEGACY_KEYS.device(tenant_id, device_id))
        if not self._is_v1_device(item, tenant_id):
            return None
        return legacy_device_from_item(item)

    # Pond writes ---------------------------------------------------------------------------

    async def create_pond(
        self,
        tenant_id: str,
        pond_id: str,
        name: str,
        description: str | None,
    ) -> Pond:
        async def plan_create(plan: _Plan) -> Pond:
            now = legacy_now(self.clock())
            site_id = await self._default_site(plan, tenant_id, "active")
            asset_id = asset_id_for_legacy_pond(tenant_id, pond_id)
            item = {
                **self.keys.legacy_pond(tenant_id, pond_id),
                "entity_type": "pond",
                "tenant_id": tenant_id,
                "pond_id": pond_id,
                "name": name,
                "description": description,
                "status": "active",
                "created_at": now.isoformat(),
                "updated_at": now.isoformat(),
                "version": 1,
                "schema_version": 1,
                POND_SITE_LINK_ATTRIBUTE: site_id,
                POND_ASSET_LINK_ATTRIBUTE: asset_id,
                PROJECTION_VERSION_ATTRIBUTE: V2_PROJECTION_VERSION,
            }
            pond = legacy_pond_from_item(item)
            plan.add(
                "pond_put",
                self.keys.legacy_pond(tenant_id, pond_id),
                {"Put": self.put_new_request(item)},
            )
            self._put_asset_and_profile(plan, pond, site_id)
            return pond

        return await self._transact(plan_create, "pond")

    async def update_pond(
        self,
        tenant_id: str,
        pond_id: str,
        expected_version: int,
        name: str | None,
        description: str | None,
    ) -> Pond:
        async def plan_update(plan: _Plan) -> Pond:
            # Legacy key shape first: a missing Pond is a 404 whatever its identifier.
            pond_key = _LEGACY_KEYS.pond(tenant_id, pond_id)
            pond_item = await self.get_item(pond_key, consistent=True)
            if pond_item is None or pond_item.get("tenant_id") != tenant_id:
                raise NotFoundError(f"Pond {pond_id} not found")
            if int(pond_item["version"]) != expected_version:
                raise ConflictError("pond version conflict")
            now = legacy_now(self.clock())
            changes: dict[str, Any] = {
                field: value
                for field, value in {"name": name, "description": description}.items()
                if value is not None
            }
            changes.update({"updated_at": now.isoformat(), "version": expected_version + 1})
            updated = legacy_pond_from_item({**pond_item, **changes})

            asset_id = self._pond_asset_id(tenant_id, pond_item)
            asset_item = await self._tenant_item(
                self.keys.asset(tenant_id, asset_id), tenant_id
            )
            profile_item = await self._tenant_item(self.keys.pond_profile(asset_id), tenant_id)
            if asset_item is None:
                if profile_item is not None:
                    raise ConflictError("pond projection is inconsistent")
                site_id = await self._default_site(plan, tenant_id, "exists")
                self._put_asset_and_profile(plan, updated, site_id)
            else:
                if profile_item is None:
                    raise ConflictError("pond projection is inconsistent")
                asset = asset_from_item(asset_item)
                site_id = asset.site_id
                plan.add(
                    "asset_update",
                    self.keys.asset(tenant_id, asset_id),
                    {
                        "Update": self._fenced_set(
                            self.keys.asset(tenant_id, asset_id),
                            {
                                **({"name": name} if name is not None else {}),
                                "updated_at": canonical_timestamp_key(
                                    max(now, asset.updated_at)
                                ),
                                "version": asset.version + 1,
                            },
                            expected_version=asset.version,
                            tenant_id=tenant_id,
                        )
                    },
                )
                if description is not None:
                    profile = pond_profile_from_item(profile_item)
                    plan.add(
                        "profile_update",
                        self.keys.pond_profile(asset_id),
                        {
                            "Update": self._fenced_set(
                                self.keys.pond_profile(asset_id),
                                {
                                    "description": description,
                                    "updated_at": canonical_timestamp_key(
                                        max(now, profile.updated_at)
                                    ),
                                    "version": profile.version + 1,
                                },
                                expected_version=profile.version,
                                tenant_id=tenant_id,
                            )
                        },
                    )
            changes.update(self._pond_links(site_id, asset_id))
            plan.add(
                "pond_update",
                pond_key,
                {
                    "Update": self._fenced_set(
                        pond_key, changes, expected_version=expected_version, tenant_id=tenant_id
                    )
                },
            )
            return updated

        return await self._transact(plan_update, "pond")

    # Device writes -------------------------------------------------------------------------

    async def create_device(
        self,
        tenant_id: str,
        pond_id: str,
        device_id: str,
        name: str,
        firmware_version: str | None,
    ) -> Device:
        async def plan_create(plan: _Plan) -> Device:
            pond_item = await self._legacy_pond_item(tenant_id, pond_id)
            if pond_item is None:
                raise ConflictError("pond not found")
            site_id, asset_id = await self._pond_target(plan, tenant_id, pond_item, active=True)
            now = legacy_now(self.clock())
            component_id = default_component_id_for_legacy_device(tenant_id, device_id)
            row = {
                "tenant_id": tenant_id,
                "pond_id": pond_id,
                "device_id": device_id,
                "name": name,
                "auth_type": LEGACY_AUTH_TYPE,
                "firmware_version": firmware_version,
                "status": "active",
                "created_at": now.isoformat(),
                "updated_at": now.isoformat(),
                "version": 1,
                "schema_version": 1,
                DEVICE_COMPONENT_LINK_ATTRIBUTE: component_id,
                PROJECTION_VERSION_ATTRIBUTE: V2_PROJECTION_VERSION,
            }
            device_key = self.keys.legacy_device(tenant_id, device_id)
            lookup_key = self.keys.device_lookup(device_id)
            device_item = {**device_key, "entity_type": "device", **row}
            lookup_item = {**lookup_key, "entity_type": "device_lookup", **row}
            plan.add("device_put", device_key, {"Put": self.put_new_request(device_item)})
            plan.add("device_put", lookup_key, {"Put": self.put_new_request(lookup_item)})
            device = legacy_device_from_item(device_item)
            self._put_component(plan, device)
            deployment = deployment_for_legacy_device(device, asset_id, site_id)
            self._put_deployment(plan, deployment, current=True)
            self._put_marker(plan, deployment, seq=1, last_ended_at=None)
            return device

        return await self._transact(plan_create, "device")

    async def update_device(
        self,
        tenant_id: str,
        device_id: str,
        expected_version: int,
        name: str | None,
        pond_id: str | None,
        firmware_version: str | None,
    ) -> Device:
        async def plan_update(plan: _Plan) -> Device:
            # Legacy key shapes first: a missing Device is a 404 whatever its identifier.
            device_key = _LEGACY_KEYS.device(tenant_id, device_id)
            lookup_key = _LEGACY_KEYS.device_lookup(device_id)
            device_item = await self.get_item(device_key, consistent=True)
            if not self._is_v1_device(device_item, tenant_id):
                raise NotFoundError(f"Device {device_id} not found")
            if await self._tenant_item(lookup_key, tenant_id) is None:
                raise NotFoundError(f"Device {device_id} not found")
            if int(device_item["version"]) != expected_version:
                raise ConflictError("device version conflict")
            now = self.clock()
            changes: dict[str, Any] = {
                field: value
                for field, value in {
                    "name": name,
                    "pond_id": pond_id,
                    "firmware_version": firmware_version,
                }.items()
                if value is not None
            }
            changes.update(
                {
                    "updated_at": legacy_now(now).isoformat(),
                    "version": expected_version + 1,
                    DEVICE_COMPONENT_LINK_ATTRIBUTE: default_component_id_for_legacy_device(
                        tenant_id, device_id
                    ),
                    PROJECTION_VERSION_ATTRIBUTE: V2_PROJECTION_VERSION,
                }
            )
            for key in (device_key, lookup_key):
                plan.add(
                    "device_update",
                    key,
                    {
                        "Update": self._fenced_set(
                            key, changes, expected_version=expected_version, tenant_id=tenant_id
                        )
                    },
                )
            updated = legacy_device_from_item({**device_item, **changes})
            await self._project_device_update(plan, device_item["pond_id"], updated, now)
            return updated

        return await self._transact(plan_update, "device")

    # Projection planning -------------------------------------------------------------------

    async def _project_device_update(
        self, plan: _Plan, previous_pond_id: str, device: Device, now: datetime
    ) -> None:
        tenant_id = device.tenant_id
        component_id = default_component_id_for_legacy_device(tenant_id, device.device_id)
        moving = device.pond_id != previous_pond_id
        target: tuple[str, str] | None = None
        if moving:
            target_pond = await self._legacy_pond_item(tenant_id, device.pond_id)
            if target_pond is None:
                raise ConflictError("pond not found")
            target = await self._pond_target(plan, tenant_id, target_pond, active=True)

        component_item = await self._tenant_item(self.keys.component(component_id), tenant_id)
        marker = await self._tenant_item(deployment_marker_key(component_id), tenant_id)
        if component_item is None:
            await self._provision_device(plan, previous_pond_id, device, target, now)
            return
        if target is None:
            return
        if component_from_item(component_item).status is not ComponentStatus.ACTIVE:
            raise ConflictError(COMPONENT_NOT_ACTIVE)
        plan.add(
            "component_check",
            self.keys.component(component_id),
            {
                "ConditionCheck": self._status_check(
                    self.keys.component(component_id), tenant_id, ComponentStatus.ACTIVE.value
                )
            },
        )
        current_item = await self._tenant_item(
            self.keys.current_deployment(component_id), tenant_id
        )
        site_id, asset_id = target
        if current_item is None:
            last_ended_at = None if marker is None else marker.get(MARKER_LAST_ENDED_AT_ATTRIBUTE)
            started_at = now
            if last_ended_at is not None:
                started_at = max(now, parse_canonical_timestamp(last_ended_at))
            self._start(plan, tenant_id, component_id, site_id, asset_id, started_at, marker)
            return
        current = deployment_from_item(current_item)
        if current.asset_id == asset_id:
            return
        transition_at = transition_instant(now, current.started_at)
        self._end_current(plan, current, transition_at, now)
        new = started_deployment(
            tenant_id=tenant_id,
            deployment_id=new_deployment_id(),
            component_id=component_id,
            site_id=site_id,
            asset_id=asset_id,
            started_at=transition_at,
        )
        self._put_deployment(plan, new, current=False)
        current_key = self.keys.current_deployment(component_id)
        plan.add(
            "current_put",
            current_key,
            {
                # CURRENT is a full copy: one overwrite conditioned on the interval it replaces.
                "Put": self.put_request(
                    deployment_to_item(new, form="current"),
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
                        ":deployment_id": current.deployment_id,
                        ":expected_version": current.version,
                        ":tenant_id": tenant_id,
                    },
                )
            },
        )
        plan.add(
            "marker",
            deployment_marker_key(component_id),
            {
                "Update": deployment_marker_advance_request(
                    self.table_name,
                    tenant_id,
                    component_id,
                    observed_seq=None if marker is None else int(marker[MARKER_SEQ_ATTRIBUTE]),
                    ended_at=transition_at,
                )
            },
        )

    async def _provision_device(
        self,
        plan: _Plan,
        previous_pond_id: str,
        device: Device,
        target: tuple[str, str] | None,
        now: datetime,
    ) -> None:
        """Deterministic default Component and Deployment for a not-yet-migrated Device."""
        tenant_id = device.tenant_id
        component = self._put_component(plan, device)
        if device_status_from_legacy(device.status) is DeviceStatus.DECOMMISSIONED:
            # Same as the migration: a decommissioned Component is never deployed.
            if target is not None:
                raise ConflictError(COMPONENT_NOT_ACTIVE)
            return
        previous_pond = await self._legacy_pond_item(tenant_id, previous_pond_id)
        if previous_pond is None:
            # A dangling legacy pond_id has no Asset: there is no interval to reconstruct.
            if target is not None:
                site_id, asset_id = target
                self._start(plan, tenant_id, component.component_id, site_id, asset_id, now, None)
            return
        site_id, asset_id = await self._pond_target(plan, tenant_id, previous_pond, active=False)
        initial = deployment_for_legacy_device(device, asset_id, site_id)
        if target is None:
            self._put_deployment(plan, initial, current=True)
            self._put_marker(plan, initial, seq=1, last_ended_at=None)
            return
        transition_at = transition_instant(now, initial.started_at)
        # Equivalent to migrating the Device and then moving it: start (seq 1) + end (seq 2).
        self._put_deployment(plan, ended_deployment(initial, transition_at, now), current=False)
        target_site_id, target_asset_id = target
        new = started_deployment(
            tenant_id=tenant_id,
            deployment_id=new_deployment_id(),
            component_id=component.component_id,
            site_id=target_site_id,
            asset_id=target_asset_id,
            started_at=transition_at,
        )
        self._put_deployment(plan, new, current=True)
        self._put_marker(plan, new, seq=2, last_ended_at=transition_at)

    async def _legacy_pond_item(self, tenant_id: str, pond_id: str) -> dict[str, Any] | None:
        with _v2_compatible("pond"):
            key = self.keys.legacy_pond(tenant_id, pond_id)
        return await self._tenant_item(key, tenant_id)

    async def _pond_target(
        self, plan: _Plan, tenant_id: str, pond_item: Mapping[str, Any], *, active: bool
    ) -> tuple[str, str]:
        """`(site_id, asset_id)` of a legacy Pond, provisioning its Asset when not migrated."""
        with _v2_compatible("pond"):
            return await self._plan_pond_target(plan, tenant_id, pond_item, active=active)

    async def _plan_pond_target(
        self, plan: _Plan, tenant_id: str, pond_item: Mapping[str, Any], *, active: bool
    ) -> tuple[str, str]:
        asset_id = self._pond_asset_id(tenant_id, pond_item)
        asset_key = self.keys.asset(tenant_id, asset_id)
        asset_item = await self._tenant_item(asset_key, tenant_id)
        if asset_item is not None:
            asset = asset_from_item(asset_item)
            if active and asset.status is not AssetStatus.ACTIVE:
                raise ConflictError("pond is not active")
            check = (
                self._status_check(asset_key, tenant_id, AssetStatus.ACTIVE.value)
                if active
                else self._tenant_check(asset_key, tenant_id)
            )
            plan.add("asset_check", asset_key, {"ConditionCheck": check})
            if active:
                await self._active_site(plan, tenant_id, asset.site_id)
            return asset.site_id, asset_id
        pond = legacy_pond_from_item(pond_item)
        if active and asset_status_from_legacy(pond.status) is AssetStatus.ARCHIVED:
            raise ConflictError("pond is not active")
        site_id = await self._default_site(plan, tenant_id, "active" if active else "exists")
        self._put_asset_and_profile(plan, pond, site_id)
        pond_key = self.keys.legacy_pond(tenant_id, pond_item["pond_id"])
        if not plan.has(pond_key):
            # Link only; the `/v1` Pond version is untouched by a Device write.
            observed = int(pond_item["version"])
            plan.add(
                "pond_link",
                pond_key,
                {
                    "Update": self._fenced_set(
                        pond_key,
                        self._pond_links(site_id, asset_id),
                        expected_version=observed,
                        tenant_id=tenant_id,
                    )
                },
            )
        return site_id, asset_id

    async def _default_site(self, plan: _Plan, tenant_id: str, strength: str) -> str:
        """Deterministic default Site: Put when absent, else a ConditionCheck (#37).

        `strength` is what an existing Site must satisfy: "active" or merely "exists".
        """
        site_id = default_site_id(tenant_id)
        key = self.keys.site(tenant_id, site_id)
        site_item = await self._tenant_item(key, tenant_id)
        if site_item is None:
            tenant_item = await self.get_item(
                {"PK": self.keys.tenant_partition(tenant_id), "SK": "META"}, consistent=True
            )
            if tenant_item is None or tenant_item.get("tenant_id") != tenant_id:
                raise ConflictError("tenant not found")
            site = default_site_for_tenant(legacy_tenant_from_item(tenant_item))
            plan.add_site("put", "site_put", key, {"Put": self.put_new_request(site_to_item(site))})
            return site_id
        if strength == "exists":
            plan.add_site(
                "exists", "site_check", key, {"ConditionCheck": self._tenant_check(key, tenant_id)}
            )
            return site_id
        if site_from_item(site_item).status is not SiteStatus.ACTIVE:
            raise ConflictError(SITE_NOT_AVAILABLE)
        plan.add_site(
            "active",
            "site_check",
            key,
            {"ConditionCheck": self._status_check(key, tenant_id, SiteStatus.ACTIVE.value)},
        )
        return site_id

    async def _active_site(self, plan: _Plan, tenant_id: str, site_id: str) -> None:
        key = self.keys.site(tenant_id, site_id)
        if plan.has(key):
            # Already a Put or an active check from this plan.
            plan.add_site(
                "active",
                "site_check",
                key,
                {"ConditionCheck": self._status_check(key, tenant_id, SiteStatus.ACTIVE.value)},
            )
            return
        site_item = await self._tenant_item(key, tenant_id)
        if site_item is None or site_from_item(site_item).status is not SiteStatus.ACTIVE:
            raise ConflictError(SITE_NOT_AVAILABLE)
        plan.add_site(
            "active",
            "site_check",
            key,
            {"ConditionCheck": self._status_check(key, tenant_id, SiteStatus.ACTIVE.value)},
        )

    def _put_asset_and_profile(self, plan: _Plan, pond: Pond, site_id: str) -> None:
        projected = asset_and_profile_for_legacy_pond(pond, site_id)
        asset, profile = projected.asset, projected.pond_profile
        plan.add(
            "asset_put",
            self.keys.asset(asset.tenant_id, asset.asset_id),
            {"Put": self.put_new_request(asset_to_item(asset))},
        )
        plan.add(
            "profile_put",
            self.keys.pond_profile(profile.asset_id),
            {"Put": self.put_new_request(pond_profile_to_item(profile))},
        )

    def _put_component(self, plan: _Plan, device: Device) -> DeviceComponent:
        component = component_for_legacy_device(device)
        for canonical in (False, True):
            item = component_to_item(component, canonical=canonical)
            plan.add(
                "component_put",
                {"PK": item["PK"], "SK": item["SK"]},
                {"Put": self.put_new_request(item)},
            )
        return component

    def _put_deployment(self, plan: _Plan, deployment: Deployment, *, current: bool) -> None:
        forms = ("mirror", "history", "current") if current else ("mirror", "history")
        for form in forms:
            plan.add(
                "current_put" if form == "current" else "deployment_put",
                deployment_item_key(deployment, form),
                {"Put": self.put_new_request(deployment_to_item(deployment, form=form))},
            )

    def _put_marker(
        self, plan: _Plan, deployment: Deployment, *, seq: int, last_ended_at: datetime | None
    ) -> None:
        item = deployment_marker_item(
            deployment.tenant_id, deployment.component_id, seq=seq, last_ended_at=last_ended_at
        )
        plan.add(
            "marker",
            deployment_marker_key(deployment.component_id),
            {"Put": self.put_new_request(item)},
        )

    def _start(
        self,
        plan: _Plan,
        tenant_id: str,
        component_id: str,
        site_id: str,
        asset_id: str,
        started_at: datetime,
        marker: Mapping[str, Any] | None,
    ) -> None:
        """Start an interval when the Component has no active one."""
        deployment = started_deployment(
            tenant_id=tenant_id,
            deployment_id=new_deployment_id(),
            component_id=component_id,
            site_id=site_id,
            asset_id=asset_id,
            started_at=started_at,
        )
        self._put_deployment(plan, deployment, current=True)
        if marker is None:
            self._put_marker(plan, deployment, seq=1, last_ended_at=None)
            return
        observed_seq = int(marker[MARKER_SEQ_ATTRIBUTE])
        plan.add(
            "marker",
            deployment_marker_key(component_id),
            {
                "Update": self.update_request(
                    deployment_marker_key(component_id),
                    update_expression="SET #seq = :next_seq",
                    condition=(
                        "#seq = :observed_seq AND #tenant_id = :tenant_id AND "
                        "(attribute_not_exists(#last_ended_at) OR #last_ended_at <= :started_at)"
                    ),
                    names={
                        "#seq": MARKER_SEQ_ATTRIBUTE,
                        "#tenant_id": "tenant_id",
                        "#last_ended_at": MARKER_LAST_ENDED_AT_ATTRIBUTE,
                    },
                    values={
                        ":observed_seq": observed_seq,
                        ":next_seq": observed_seq + 1,
                        ":tenant_id": tenant_id,
                        ":started_at": canonical_timestamp_key(started_at),
                    },
                )
            },
        )

    def _end_current(
        self, plan: _Plan, current: Deployment, ended_at: datetime, now: datetime
    ) -> None:
        ended = ended_deployment(current, ended_at, now)
        changes = {
            "status": ended.status.value,
            "ended_at": canonical_timestamp_key(ended_at),
            "updated_at": canonical_timestamp_key(ended.updated_at),
            "version": ended.version,
        }
        for form in ("mirror", "history"):
            key = deployment_item_key(current, form)
            plan.add(
                "deployment_end",
                key,
                {
                    "Update": self.update_request(
                        key,
                        update_expression=(
                            "SET #status = :status, #ended_at = :ended_at, "
                            "#updated_at = :updated_at, #version = :version"
                        ),
                        condition=(
                            "attribute_exists(PK) AND #version = :expected_version"
                            " AND #status = :active AND #tenant_id = :tenant_id"
                            " AND #deployment_id = :deployment_id"
                        ),
                        names={
                            "#status": "status",
                            "#ended_at": "ended_at",
                            "#updated_at": "updated_at",
                            "#version": "version",
                            "#tenant_id": "tenant_id",
                            "#deployment_id": "deployment_id",
                        },
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
                },
            )

    # Internals -----------------------------------------------------------------------------

    async def _transact(self, build: Callable[[_Plan], Awaitable[Any]], entity: str) -> Any:
        for attempt in range(1, MAX_ATTEMPTS + 1):
            plan = _Plan()
            with _v2_compatible(entity):
                result = await build(plan)
            if len(plan.actions) > MAX_PROJECTION_ACTIONS:
                raise ValueError("projection transaction exceeds 25 actions")
            try:
                await self.transact_write(plan.actions)
            except DynamoConditionalConflict as exc:
                labels = plan.failed_labels(exc.failed_indexes)
                definitive = labels - _RETRYABLE_LABELS
                if definitive:
                    label = sorted(definitive)[0]
                    raise ConflictError(_CONFLICT_MESSAGES.get(label, "conflict")) from exc
                if attempt == MAX_ATTEMPTS:
                    raise ConflictError("concurrent projection update") from exc
                await self._retry_sleep(attempt)
            except ClientError as exc:
                if attempt == MAX_ATTEMPTS or not _only_transaction_conflicts(exc):
                    raise
                await self._retry_sleep(attempt)
            else:
                return result
        raise AssertionError("unreachable")  # pragma: no cover

    async def _tenant_item(
        self, key: Mapping[str, str], tenant_id: str
    ) -> dict[str, Any] | None:
        item = await self.get_item(key, consistent=True)
        if item is None or item.get("tenant_id") != tenant_id:
            return None
        return item

    def _pond_asset_id(self, tenant_id: str, pond_item: Mapping[str, Any]) -> str:
        linked = pond_item.get(POND_ASSET_LINK_ATTRIBUTE)
        if isinstance(linked, str):
            return linked
        return asset_id_for_legacy_pond(tenant_id, pond_item["pond_id"])

    @staticmethod
    def _pond_links(site_id: str, asset_id: str) -> dict[str, Any]:
        return {
            POND_SITE_LINK_ATTRIBUTE: site_id,
            POND_ASSET_LINK_ATTRIBUTE: asset_id,
            PROJECTION_VERSION_ATTRIBUTE: V2_PROJECTION_VERSION,
        }

    @staticmethod
    def _is_v1_device(item: Mapping[str, Any] | None, tenant_id: str) -> bool:
        return (
            item is not None and item.get("tenant_id") == tenant_id and is_legacy_device_item(item)
        )

    def _fenced_set(
        self,
        key: Mapping[str, str],
        changes: Mapping[str, Any],
        *,
        expected_version: int,
        tenant_id: str,
    ) -> dict[str, Any]:
        names = {"#tenant_id": "tenant_id", "#version": "version"}
        values: dict[str, Any] = {":expected_version": expected_version, ":tenant_id": tenant_id}
        assignments = []
        for index, (attribute, value) in enumerate(changes.items()):
            names[f"#f{index}"] = attribute
            values[f":v{index}"] = value
            assignments.append(f"#f{index} = :v{index}")
        return self.update_request(
            key,
            update_expression="SET " + ", ".join(assignments),
            condition=_FENCE_CONDITION,
            names=names,
            values=values,
        )

    def _status_check(
        self, key: Mapping[str, str], tenant_id: str, active: str
    ) -> dict[str, Any]:
        return self.condition_check_request(
            key,
            condition=_ACTIVE_TENANT_CONDITION,
            names={"#status": "status", "#tenant_id": "tenant_id"},
            values={":active": active, ":tenant_id": tenant_id},
        )

    def _tenant_check(self, key: Mapping[str, str], tenant_id: str) -> dict[str, Any]:
        return self.condition_check_request(
            key,
            condition=_TENANT_CONDITION,
            names={"#tenant_id": "tenant_id"},
            values={":tenant_id": tenant_id},
        )
