"""M1 backfill: legacy Tenant/Pond/Device rows -> deterministic v2 Site/Asset/Component/Deployment.

Scope is always an explicit tenant list; there is no discovery and no Scan. Dry-run is the
default and performs reads only, reporting deterministic `would_*` counters.

Per tenant the migrator reads `TENANT#<t>/META`, pages through `POND#` and `DEVICE#` with
`Limit=page_size`, and plans every record before writing anything. A tenant with any schema or
ownership conflict receives no writes; other tenants continue. Planned writes are:

- one default Site (`default_site_for_tenant`), named and timestamped from the Tenant row;
- per Pond one Asset + PondProfile (version and timestamps copied from the Pond) and SET-only
  links on the Pond row;
- per legacy Device the deterministic probe Component (canonical + tenant mirror), an ACTIVE
  Deployment starting at the Device `created_at` (mirror, history, CURRENT and the D10 marker
  with `seq=1`), and SET-only links on the Device row and its lookup. A DECOMMISSIONED Device
  gets a DECOMMISSIONED Component and no Deployment, because a decommissioned Component cannot
  hold an active interval.

Legacy state is mirrored; v2 start/move rules apply only to new writes. A Device in an archived
Pond therefore gets an ACTIVE Deployment on the ARCHIVED Asset, exactly as the legacy rows say.
A drift repair (below) is such a new write: like the `/v1` projection it never moves a Deployment
into an ARCHIVED Asset; that Device counts as already migrated (links may still be set), and the
repair transaction is fenced on the target Asset being ACTIVE.

The default Component's `DEPLOYMENT#CURRENT` is authoritative history whoever wrote it (this
migration, the flag-on `/v1` projection, which also starts non-deterministic intervals, or
`/v2`). It is validated (tenant, Component, ACTIVE, and the active mirror/history rows it names)
together with the marker (present, `seq >= 1`):

- CURRENT on the Asset of the Device's `pond_id`: already migrated (links may still be set).
- CURRENT on another Asset (a `/v1` move while the projection flag was off): DRIFT REPAIR in the
  Device transaction. CURRENT's mirror and history are ended at `transition_at`, a new
  `new_deployment_id()` interval starts there on the Pond's Asset (mirror, history, CURRENT
  overwritten only while it still names the old Deployment and version), the marker advances
  fenced on its observed `seq`, and the target Asset and the Component are checked. The real
  move time is unknown, so `transition_at = max(device.updated_at, CURRENT.started_at + 1µs)`
  approximates it from the legacy row; the clock is never used. A repair counts under
  `deployments_created` and `devices_projected`. Concurrent identical repairs converge: the
  loser re-reads a CURRENT already on the right Asset and reports already migrated.
- A DECOMMISSIONED Device or an inactive Component is never repaired or deployed: it counts as
  already migrated.
- No CURRENT and an ended deterministic Deployment: already migrated. Only an explicit `/v2` end
  produces that state; restarting an interval would invent history, and a conflict would block
  the tenant on every rerun with nothing the migration could repair.
- No CURRENT and no deterministic Deployment: the deterministic projection is created; a stray
  history row or marker there is a conflict.

All v2 content comes from `domain.legacy_projection` builders and the bounded-context codecs, so
it is byte-identical to the `/v1` compatibility projection. Timestamps and versions come from the
legacy rows; the injected clock only stamps the per-tenant migration receipt.

Each Pond and each Device is written in one transaction of `attribute_not_exists` puts and
version-fenced link updates. A conditional failure, or a cancellation caused only by
`TransactionConflict` (after a small jittered backoff), re-reads the source row and the known keys
and re-plans the record: content that now matches on ownership/identity converges as already
migrated, divergent content is a schema conflict. Known keys of one transaction group are read
siblings-first and the primary row last (Deployment CURRENT/history/marker before its mirror), so
a present primary proves the group. Any mixed state that a concurrent commit or a `/v1` move can
explain (a lookup that disagrees with the queried Device row, a Device whose Pond is unknown, a
partial Deployment group) re-reads the record after a small jittered backoff, bounded by
`MAX_RECORD_ATTEMPTS`. A state observed identically on every re-read is stable and divergent and
becomes a schema conflict; one that kept changing fails the tenant (rerun to converge). A Pond
created during the run is read and planned before its Device.
Exhausted retries mark only that tenant failed (result `failed`, exit 1); other tenants continue.
The run is idempotent, so rerunning it converges any tenant left incomplete.

Invariants every plan keeps:

- Every plan with actions fences each legacy source row it read (Tenant row for the Site,
  Pond row, Device row and lookup) on its observed version, through a link update or a
  `ConditionCheck`; a refused commit re-plans from fresh reads.
- A partial group is rebuilt only from its survivor, never from legacy data over it: a missing
  Component row copies the surviving canonical/mirror row (fenced on its version); a missing
  PondProfile takes its version and timestamps from the surviving Asset (fenced), only the
  description comes from the Pond; an Asset cannot be rebuilt from its PondProfile (conflict).
  Deployment groups are never rebuilt; inconsistent ones are re-read and then reported.
- Rows of one Deployment (mirror, history, CURRENT) are compared on every identity field, Site
  and Asset included; stored versions and `seq` values must be stored integers, never bools.
- Every write to a deterministic key is conditioned on absence or on observed ownership; the
  migration receipt is written only to a free key or this tenant's own M1 receipt.
- Only `schema_version == 2` rows under `DEVICE#` are native v2 and skipped; any other row is
  legacy and must parse, so a malformed one is reported as `device_invalid`.

Counters: `assets_created` counts Asset puts only. A PondProfile written for an existing Asset
counts under `legacy_links_updated`, because the profile's `legacy_pond_id` is the reverse
v2 -> v1 link. `devices_projected` counts Devices whose Component or Deployment is created; a
Device that only gains links counts under `legacy_links_updated`. Ownership comparisons never look at names,
descriptions, status, version or `updated_at`, which later `/v1` or `/v2` edits may change. A
Device whose deterministic Deployment exists but is no longer CURRENT (it has since moved) is
already migrated. The receipt records each completed run and is never used to skip validation.
"""

import json
import logging
import random
import time
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Literal

from botocore.exceptions import BotoCoreError, ClientError
from pydantic import BaseModel, ConfigDict

from limnopulse_api.adapters.dynamodb_default_site_asset_migration import (
    DEFAULT_COMPONENT_ID_ATTRIBUTE,
    LEGACY_DEVICE_PREFIX,
    LEGACY_POND_PREFIX,
    V2_ASSET_ID_ATTRIBUTE,
    V2_PROJECTION_VERSION,
    V2_PROJECTION_VERSION_ATTRIBUTE,
    V2_SITE_ID_ATTRIBUTE,
    DynamoDefaultSiteAssetMigrationStore,
    is_legacy_device_item,
    key_label,
    legacy_device_from_item,
    legacy_pond_from_item,
    legacy_tenant_from_item,
    stored_int,
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
from limnopulse_api.adapters.dynamodb_devices_v2 import (
    COMPONENT_ENTITY_TYPE,
    COMPONENT_MIRROR_ENTITY_TYPE,
    component_to_item,
)
from limnopulse_api.adapters.dynamodb_sites_assets import (
    asset_to_item,
    pond_profile_to_item,
    site_to_item,
)
from limnopulse_api.adapters.dynamodb_v2 import (
    DynamoConditionalConflict,
    parse_canonical_timestamp,
    utc_now,
)
from limnopulse_api.domain.assets import AssetStatus, PondProfile, SiteStatus
from limnopulse_api.domain.deployments import Deployment, DeploymentStatus
from limnopulse_api.domain.devices_v2 import ComponentStatus, DeviceComponent
from limnopulse_api.domain.entities import Device, Pond, Tenant
from limnopulse_api.domain.ids import asset_id_for_legacy_pond, new_deployment_id
from limnopulse_api.domain.legacy_projection import (
    asset_and_profile_for_legacy_pond,
    asset_status_from_legacy,
    component_for_legacy_device,
    default_site_for_tenant,
    deployment_for_legacy_device,
    legacy_timestamp_to_utc,
)

DEFAULT_TABLE_NAME = "LimnopulseDomain"
MIN_PAGE_SIZE = 1
MAX_PAGE_SIZE = 100
MIN_DEADLINE_SECONDS = 1
MAX_DEADLINE_SECONDS = 3600
MAX_RECORD_ATTEMPTS = 3
BACKOFF_BASE_SECONDS = 0.05

MigrationResult = Literal[
    "success",
    "schema_conflict",
    "deadline_exceeded",
    "dynamodb_error",
    "configuration_error",
    "failed",
]

# Ownership/identity attributes compared against an existing row; never names or versions.
_SITE_FIELDS = ("tenant_id", "site_id")
_ASSET_FIELDS = ("tenant_id", "asset_id", "site_id", "asset_type")
_PROFILE_FIELDS = ("tenant_id", "asset_id", "legacy_pond_id")
_COMPONENT_FIELDS = (
    "tenant_id",
    "component_id",
    "device_id",
    "kind",
    "probe_profile.probe_profile_id",
)
# The deterministic Deployment always targets the default Site; its Asset follows the Pond the
# Device was in when it was projected, so expectations built from the current Pond compare
# the Asset only where that Pond is authoritative (CURRENT). Rows of ONE Deployment (mirror,
# history, CURRENT) are always compared on every identity field, Site and Asset included.
_DEPLOYMENT_FIELDS = ("tenant_id", "deployment_id", "component_id", "site_id", "started_at")
_CURRENT_FIELDS = (*_DEPLOYMENT_FIELDS, "asset_id")
_SIBLING_FIELDS = (*_CURRENT_FIELDS, "status", "version")
_MARKER_FIELDS = ("tenant_id", "component_id")

_CREATED_TO_WOULD = {
    "default_sites_created": "would_create_default_sites",
    "assets_created": "would_create_assets",
    "devices_projected": "would_project_devices",
    "components_created": "would_create_components",
    "deployments_created": "would_create_deployments",
    "legacy_links_updated": "would_update_legacy_links",
}

_LOGGER = logging.getLogger(__name__)


class MigrationConflict(BaseModel):
    """A schema/ownership conflict; identifiers and key only, never names or other PII."""

    model_config = ConfigDict(frozen=True)

    tenant_id: str
    kind: str
    key: str


class MigrationSummary(BaseModel):
    """Run summary; the field order is the stable JSON key order printed by the CLI."""

    model_config = ConfigDict(frozen=True)

    apply: bool
    tenants_requested: int
    tenants_completed: int
    default_sites_created: int
    assets_created: int
    devices_projected: int
    components_created: int
    deployments_created: int
    legacy_links_updated: int
    would_create_default_sites: int
    would_create_assets: int
    would_project_devices: int
    would_create_components: int
    would_create_deployments: int
    would_update_legacy_links: int
    already_migrated: int
    schema_conflicts: int
    conflicts: list[MigrationConflict]
    scope_completed: bool
    result: MigrationResult


def empty_summary(
    *, apply: bool, tenants_requested: int, result: MigrationResult
) -> MigrationSummary:
    """Summary for a run that stopped before any tenant was processed."""
    return _RunState(apply=apply, tenants_requested=tenants_requested).summary(result)


def exit_code_for(result: MigrationResult) -> int:
    """0 success, 2 schema/ownership conflict, 1 anything else (it takes precedence)."""
    if result == "success":
        return 0
    if result == "schema_conflict":
        return 2
    return 1


def validate_tenant_id(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("tenant IDs must not be blank")
    if "\0" in value:
        raise ValueError("tenant IDs must not contain NUL bytes")
    if "#" in value:
        raise ValueError("tenant IDs must not contain '#'")
    if value != value.strip():
        raise ValueError("tenant IDs must not have surrounding whitespace")
    return value


def normalize_tenant_ids(values: Iterable[object]) -> list[str]:
    """Validate and de-duplicate tenant IDs, preserving first-seen order."""
    return list(dict.fromkeys(validate_tenant_id(value) for value in values))


class _SchemaConflict(Exception):
    def __init__(self, kind: str, key: Mapping[str, str]) -> None:
        super().__init__(kind)
        self.kind = kind
        self.key = key_label(key)


class _DeadlineExceeded(Exception):
    pass


class _Unstable(Exception):
    """A mixed state that a concurrent commit can explain; re-read and re-plan the record."""

    def __init__(self, kind: str, key: Mapping[str, str], observed: Any = None) -> None:
        super().__init__(kind)
        self.kind = kind
        self.key = dict(key)
        # The rows whose mix was inconsistent: identical on every re-read means a stable state.
        self.observed = observed


class _RetryExhausted(Exception):
    pass


def _is_transaction_conflict(exc: ClientError) -> bool:
    """A write lost only to concurrent transactions (no conditional failure): retryable."""
    code = exc.response.get("Error", {}).get("Code")
    if code == "TransactionConflictException":
        return True
    if code != "TransactionCanceledException":
        return False
    reasons = [
        reason.get("Code")
        for reason in exc.response.get("CancellationReasons") or []
        if isinstance(reason, Mapping) and reason.get("Code") not in (None, "None")
    ]
    return bool(reasons) and all(reason == "TransactionConflict" for reason in reasons)


@dataclass(frozen=True)
class _RecordPlan:
    kind: Literal["pond", "device"]
    source_key: dict[str, str]
    actions: tuple[dict[str, Any], ...]
    asset_created: bool = False
    profile_created: bool = False
    component_created: bool = False
    deployment_created: bool = False
    link_updated: bool = False

    def counts(self) -> Counter[str]:
        if not self.actions:
            return Counter(already_migrated=1)
        counts: Counter[str] = Counter()
        counts["devices_projected"] += int(self.component_created or self.deployment_created)
        counts["assets_created"] += int(self.asset_created)
        counts["components_created"] += int(self.component_created)
        counts["deployments_created"] += int(self.deployment_created)
        # A PondProfile repaired for an existing Asset carries `legacy_pond_id`, the reverse
        # v2 -> v1 link; one created with its Asset is part of the new projection instead.
        profile_repaired = self.profile_created and not self.asset_created
        counts["legacy_links_updated"] += int(self.link_updated or profile_repaired)
        return counts


@dataclass
class _TenantPlan:
    tenant: Tenant
    site_id: str
    site_action: tuple[dict[str, Any], ...] | None
    ponds: dict[str, Pond] = field(default_factory=dict)
    records: list[_RecordPlan] = field(default_factory=list)
    conflicts: list[_SchemaConflict] = field(default_factory=list)

    def counts(self) -> Counter[str]:
        counts: Counter[str] = Counter()
        if self.site_action is not None:
            counts["default_sites_created"] += 1
        for record in self.records:
            counts.update(record.counts())
        return counts


@dataclass
class _RunState:
    apply: bool
    tenants_requested: int
    tenants_completed: int = 0
    tenants_failed: int = 0
    counts: Counter[str] = field(default_factory=Counter)
    conflicts: list[MigrationConflict] = field(default_factory=list)

    def add_conflicts(self, tenant_id: str, conflicts: Sequence[_SchemaConflict]) -> None:
        for conflict in conflicts:
            entry = MigrationConflict(tenant_id=tenant_id, kind=conflict.kind, key=conflict.key)
            if entry not in self.conflicts:
                self.conflicts.append(entry)

    def summary(self, fatal: MigrationResult | None) -> MigrationSummary:
        result: MigrationResult = fatal or (
            "failed" if self.tenants_failed else "schema_conflict" if self.conflicts else "success"
        )
        values = {name: self.counts[name] for name in (*_CREATED_TO_WOULD, "already_migrated")}
        values.update({would: self.counts[would] for would in _CREATED_TO_WOULD.values()})
        return MigrationSummary(
            apply=self.apply,
            tenants_requested=self.tenants_requested,
            tenants_completed=self.tenants_completed,
            schema_conflicts=len(self.conflicts),
            conflicts=list(self.conflicts),
            scope_completed=fatal is None and self.tenants_completed == self.tenants_requested,
            result=result,
            **values,
        )


def _field(row: Mapping[str, Any] | None, name: str) -> Any:
    """Attribute value; a dotted name walks nested maps (`None` when any level is absent)."""
    value: Any = row
    for part in name.split("."):
        if not isinstance(value, Mapping):
            return None
        value = value.get(part)
    return value


def _require_identity(
    existing: Mapping[str, Any],
    expected: Mapping[str, Any],
    fields: Sequence[str],
    kind: str,
    key: Mapping[str, str],
) -> None:
    if any(_field(existing, name) != _field(expected, name) for name in fields):
        raise _SchemaConflict(kind, key)


def _links_needed(
    item: Mapping[str, Any], expected: Mapping[str, Any], kind: str, key: Mapping[str, str]
) -> bool:
    """True when any link is absent; an existing different link is a conflict."""
    needed = False
    for name, value in expected.items():
        if name not in item:
            needed = True
        elif item[name] != value:
            raise _SchemaConflict(kind, key)
    return needed


def _stored_int(row: Mapping[str, Any], name: str, kind: str, key: Mapping[str, str]) -> int:
    """A stored integer attribute (via `stored_int`); anything else is a schema conflict."""
    try:
        return stored_int(row.get(name))
    except TypeError as exc:
        raise _SchemaConflict(kind, key) from exc


def _parse_deployment(item: Mapping[str, Any], kind: str, key: Mapping[str, str]) -> Deployment:
    """A stored Deployment row; its version must be a stored integer."""
    _stored_int(item, "version", kind, key)
    try:
        return deployment_from_item(item)
    except (KeyError, TypeError, ValueError) as exc:
        raise _SchemaConflict(kind, key) from exc


def _source_key(item: Mapping[str, Any]) -> dict[str, str]:
    return {"PK": item["PK"], "SK": item["SK"]}


class DefaultSiteAssetMigrator:
    """Explicit-tenant, dry-run-first, idempotent M1 backfill (synchronous)."""

    def __init__(
        self,
        client: Any,
        *,
        table_name: str = DEFAULT_TABLE_NAME,
        clock: Callable[[], datetime] = utc_now,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.store = DynamoDefaultSiteAssetMigrationStore(table_name, client, clock=clock)
        self.keys = self.store.keys
        self._clock = clock
        self._monotonic = monotonic
        self._sleep = sleep

    def run(
        self,
        tenants: Iterable[str],
        *,
        apply: bool,
        page_size: int,
        deadline_seconds: int,
    ) -> MigrationSummary:
        tenant_ids = normalize_tenant_ids(tenants)
        if not tenant_ids:
            raise ValueError("at least one explicit tenant is required")
        if not MIN_PAGE_SIZE <= page_size <= MAX_PAGE_SIZE:
            raise ValueError("page_size must be between 1 and 100")
        if not MIN_DEADLINE_SECONDS <= deadline_seconds <= MAX_DEADLINE_SECONDS:
            raise ValueError("deadline_seconds must be between 1 and 3600")

        state = _RunState(apply=apply, tenants_requested=len(tenant_ids))
        deadline = self._monotonic() + deadline_seconds
        fatal: MigrationResult | None = None
        try:
            for tenant_id in tenant_ids:
                self._check_deadline(deadline)
                self._migrate_tenant(tenant_id, apply, page_size, deadline, state)
        except _DeadlineExceeded:
            fatal = "deadline_exceeded"
        except (ClientError, BotoCoreError) as exc:
            _LOGGER.error("migration aborted by DynamoDB error: %s", type(exc).__name__)
            fatal = "dynamodb_error"
        except Exception as exc:  # noqa: BLE001 - any other failure maps to exit 1.
            _LOGGER.error("migration aborted by unclassified error: %s", type(exc).__name__)
            fatal = "failed"
        return state.summary(fatal)

    # Tenant ---------------------------------------------------------------------------------

    def _migrate_tenant(
        self, tenant_id: str, apply: bool, page_size: int, deadline: float, state: _RunState
    ) -> None:
        try:
            plan = self._plan_tenant(tenant_id, page_size, deadline)
        except _SchemaConflict as conflict:
            state.add_conflicts(tenant_id, [conflict])
            return
        except _RetryExhausted as exc:
            _LOGGER.error("tenant %s not planned: retries exhausted on %s", tenant_id, exc)
            state.tenants_failed += 1
            return
        if plan.conflicts:
            state.add_conflicts(tenant_id, plan.conflicts)
            return
        if not apply:
            for name, count in plan.counts().items():
                state.counts[_CREATED_TO_WOULD.get(name, name)] += count
            state.tenants_completed += 1
            return

        # Planning may have run past the deadline: nothing is written after it.
        self._check_deadline(deadline)
        tenant_counts: Counter[str] = Counter()
        try:
            self._apply_site(plan, tenant_counts, deadline)
            for record in plan.records:
                self._apply_record(record, plan, tenant_counts, deadline)
        except _SchemaConflict as conflict:
            state.add_conflicts(tenant_id, [conflict])
            return
        except _RetryExhausted as exc:
            _LOGGER.error("tenant %s not completed: retries exhausted on %s", tenant_id, exc)
            state.tenants_failed += 1
            return
        finally:
            state.counts.update(tenant_counts)
        self._check_deadline(deadline)
        try:
            self.store.record_migration_receipt(
                tenant_id,
                completed_at=self._clock(),
                counts={
                    name: tenant_counts[name] for name in (*_CREATED_TO_WOULD, "already_migrated")
                },
            )
        except DynamoConditionalConflict:
            # A foreign row occupies the receipt key: never overwrite it.
            state.add_conflicts(
                tenant_id,
                [_SchemaConflict("migration_receipt", self.keys.migration_receipt(tenant_id))],
            )
            return
        state.tenants_completed += 1

    def _reload_tenant(self, tenant_id: str) -> Tenant:
        meta_key = self.store.tenant_meta_key(tenant_id)
        item = self.store.get_tenant_item(tenant_id)
        if item is None:
            raise _SchemaConflict("tenant_missing", meta_key)
        tenant = self._parse(legacy_tenant_from_item, item, "tenant_invalid", meta_key)
        if tenant.tenant_id != tenant_id:
            raise _SchemaConflict("tenant_invalid", meta_key)
        return tenant

    def _plan_tenant(self, tenant_id: str, page_size: int, deadline: float) -> _TenantPlan:
        meta_key = self.store.tenant_meta_key(tenant_id)
        tenant = self._reload_tenant(tenant_id)
        try:
            site_id, site_action = self._plan_site(tenant)
        except ValueError as exc:
            raise _SchemaConflict("tenant_invalid", meta_key) from exc
        plan = _TenantPlan(tenant=tenant, site_id=site_id, site_action=site_action)

        for page in self.store.query_tenant_prefix_pages(
            tenant_id, LEGACY_POND_PREFIX, page_size=page_size
        ):
            self._check_deadline(deadline)
            for pond_item in page:
                self._check_deadline(deadline)
                try:
                    plan.records.append(
                        self._plan_record("pond", pond_item, plan, plan.records.append)
                    )
                except _SchemaConflict as conflict:
                    plan.conflicts.append(conflict)
                self._check_deadline(deadline)

        for page in self.store.query_tenant_prefix_pages(
            tenant_id, LEGACY_DEVICE_PREFIX, page_size=page_size
        ):
            self._check_deadline(deadline)
            for device_item in page:
                self._check_deadline(deadline)
                if not is_legacy_device_item(device_item):
                    continue
                try:
                    plan.records.append(
                        self._plan_record("device", device_item, plan, plan.records.append)
                    )
                except _SchemaConflict as conflict:
                    plan.conflicts.append(conflict)
                self._check_deadline(deadline)
        return plan

    # Planning -------------------------------------------------------------------------------

    def _plan_record(
        self,
        kind: Literal["pond", "device"],
        item: Mapping[str, Any],
        plan: _TenantPlan,
        on_new_pond: Callable[[_RecordPlan], None],
    ) -> _RecordPlan:
        """Plan one record; a mixed state re-reads the source row and re-plans, bounded.

        When re-reads run out, a state observed identically every time is stable and divergent
        (a schema conflict); one that kept changing is a failure of this run (rerun to converge).
        """
        source_key = _source_key(item)
        observations: list[str] = []
        unstable: _Unstable | None = None
        for attempt in range(MAX_RECORD_ATTEMPTS):
            if attempt:
                self._backoff(attempt - 1)
                fresh = self.store.get_known_item(source_key)
                if fresh is None:
                    raise _SchemaConflict(f"{kind}_missing", source_key)
                item = fresh
            try:
                return self._plan_from_item(kind, item, plan, on_new_pond)
            except _Unstable as exc:
                unstable = exc
                observations.append(
                    json.dumps([item, exc.kind, exc.key, exc.observed], sort_keys=True, default=str)
                )
        assert unstable is not None
        if len(set(observations)) == 1:
            raise _SchemaConflict(unstable.kind, unstable.key)
        raise _RetryExhausted(f"{kind} {unstable.kind}")

    def _plan_from_item(
        self,
        kind: Literal["pond", "device"],
        item: Mapping[str, Any],
        plan: _TenantPlan,
        on_new_pond: Callable[[_RecordPlan], None],
    ) -> _RecordPlan:
        tenant_id = plan.tenant.tenant_id
        if kind == "pond":
            pond = self._parse_pond(tenant_id, item)
            plan.ponds[pond.pond_id] = pond
            return self._plan_pond(
                pond, item, plan.site_id, site_planned=plan.site_action is not None
            )
        if not is_legacy_device_item(item):
            raise _SchemaConflict("device_invalid", _source_key(item))
        device = self._parse_device(tenant_id, item)
        self._ensure_pond(device, item, plan, on_new_pond)
        return self._plan_device(device, item, plan.site_id, plan.ponds[device.pond_id])

    def _ensure_pond(
        self,
        device: Device,
        item: Mapping[str, Any],
        plan: _TenantPlan,
        on_new_pond: Callable[[_RecordPlan], None],
    ) -> None:
        """Resolve the Device's Pond; one created during the run is read and planned first."""
        if device.pond_id in plan.ponds:
            return
        pond_item = self.store.get_known_item(
            self.keys.legacy_pond(device.tenant_id, device.pond_id)
        )
        if pond_item is None:
            # The Device row may be stale (moved since it was read): re-read it before failing.
            raise _Unstable("device_missing_pond", _source_key(item))
        on_new_pond(self._plan_record("pond", pond_item, plan, on_new_pond))

    def _plan_site(self, tenant: Tenant) -> tuple[str, tuple[dict[str, Any], ...] | None]:
        """Site id and the Put creating it, or `None` when it already exists."""
        site = default_site_for_tenant(tenant)
        item = site_to_item(site)
        key = self.keys.site(site.tenant_id, site.site_id)
        existing = self.store.get_known_item(key)
        if existing is None:
            # The Site is named from the Tenant row: fence that source row at commit time.
            return site.site_id, (
                {"Put": self.store.put_new_request(item)},
                {
                    "ConditionCheck": self.store.fence_check_request(
                        self.store.tenant_meta_key(tenant.tenant_id),
                        tenant_id=tenant.tenant_id,
                        observed_version=tenant.version,
                    )
                },
            )
        _require_identity(existing, item, _SITE_FIELDS, "site", key)
        return site.site_id, None

    def _parse_pond(self, tenant_id: str, item: Mapping[str, Any]) -> Pond:
        key = _source_key(item)
        pond = self._parse(legacy_pond_from_item, item, "pond_invalid", key)
        if pond.tenant_id != tenant_id or key["SK"] != f"{LEGACY_POND_PREFIX}{pond.pond_id}":
            raise _SchemaConflict("pond_invalid", key)
        return pond

    def _parse_device(self, tenant_id: str, item: Mapping[str, Any]) -> Device:
        key = _source_key(item)
        device = self._parse(legacy_device_from_item, item, "device_invalid", key)
        if (
            device.tenant_id != tenant_id
            or key["SK"] != f"{LEGACY_DEVICE_PREFIX}{device.device_id}"
        ):
            raise _SchemaConflict("device_invalid", key)
        return device

    def _plan_pond(
        self, pond: Pond, item: Mapping[str, Any], site_id: str, *, site_planned: bool
    ) -> _RecordPlan:
        source_key = _source_key(item)
        try:
            projection = asset_and_profile_for_legacy_pond(pond, site_id)
        except ValueError as exc:
            raise _SchemaConflict("pond_invalid", source_key) from exc
        asset_item = asset_to_item(projection.asset)
        profile_item = pond_profile_to_item(projection.pond_profile)
        asset_key = {"PK": asset_item["PK"], "SK": asset_item["SK"]}
        profile_key = {"PK": profile_item["PK"], "SK": profile_item["SK"]}
        # Asset and PondProfile always commit together, so the PondProfile is read first: a
        # present PondProfile proves its Asset. Absent rows are conditional puts; a concurrent
        # commit between these reads only fails the transaction and re-plans the record.
        profile = self.store.get_known_item(profile_key)
        asset = self.store.get_known_item(asset_key)
        if profile is not None:
            _require_identity(profile, profile_item, _PROFILE_FIELDS, "pond_profile", profile_key)
        if asset is not None:
            _require_identity(asset, asset_item, _ASSET_FIELDS, "asset", asset_key)
        asset_put = profile_put = None
        actions: list[dict[str, Any]] = []
        if asset is None and profile is None:
            asset_put = {"Put": self.store.put_new_request(asset_item)}
            profile_put = {"Put": self.store.put_new_request(profile_item)}
            actions += [asset_put, profile_put]
        elif asset is None:
            # A PondProfile cannot rebuild its Asset (name/status live on the Asset only).
            raise _SchemaConflict("asset", asset_key)
        elif profile is None:
            # Rebuild from the surviving Asset (version and timestamps), never from legacy
            # timestamps over it; only the description, absent from the Asset, comes from the
            # Pond. The Asset is fenced on the version observed here.
            profile_put = {
                "Put": self.store.put_new_request(
                    self._profile_from_asset(asset, asset_key, pond, profile_item)
                )
            }
            actions += [
                profile_put,
                {
                    "ConditionCheck": self.store.fence_check_request(
                        asset_key,
                        tenant_id=pond.tenant_id,
                        observed_version=_stored_int(asset, "version", "asset", asset_key),
                    )
                },
            ]
        if asset_put is not None and projection.asset.status is AssetStatus.ACTIVE:
            # An ACTIVE Asset needs an ACTIVE Site, checked again at commit time (the Site Put
            # is never part of this transaction). ARCHIVED Assets mirror legacy state as is.
            # The Site is read here (not taken from the tenant plan) so a re-plan after the
            # commit-time check failed sees its current status.
            site_key = self.keys.site(pond.tenant_id, site_id)
            site = self.store.get_known_item(site_key)
            if site is None:
                if not site_planned:
                    raise _SchemaConflict("site", site_key)
            elif site.get("status") != SiteStatus.ACTIVE.value:
                raise _SchemaConflict("site", site_key)
            actions.append(
                {"ConditionCheck": self.store.site_active_check_request(pond.tenant_id, site_id)}
            )

        asset_id = projection.asset.asset_id
        link_updated = _links_needed(
            item,
            {
                V2_SITE_ID_ATTRIBUTE: site_id,
                V2_ASSET_ID_ATTRIBUTE: asset_id,
                V2_PROJECTION_VERSION_ATTRIBUTE: V2_PROJECTION_VERSION,
            },
            "pond_link",
            source_key,
        )
        if link_updated:
            actions.append(
                {
                    "Update": self.store.pond_link_request(
                        pond.tenant_id,
                        pond.pond_id,
                        site_id=site_id,
                        asset_id=asset_id,
                        observed_version=pond.version,
                    )
                }
            )
        elif actions:
            # Every plan with actions fences the source row it read.
            actions.append(
                {
                    "ConditionCheck": self.store.fence_check_request(
                        source_key, tenant_id=pond.tenant_id, observed_version=pond.version
                    )
                }
            )
        return _RecordPlan(
            kind="pond",
            source_key=source_key,
            actions=tuple(actions),
            asset_created=asset_put is not None,
            profile_created=profile_put is not None,
            link_updated=link_updated,
        )

    def _plan_device(
        self, device: Device, item: Mapping[str, Any], site_id: str, pond: Pond
    ) -> _RecordPlan:
        source_key = _source_key(item)
        tenant_id = device.tenant_id
        asset_id = asset_id_for_legacy_pond(tenant_id, device.pond_id)
        try:
            component = component_for_legacy_device(device)
            deployment = deployment_for_legacy_device(device, asset_id, site_id)
        except ValueError as exc:
            raise _SchemaConflict("device_invalid", source_key) from exc

        lookup_key = self.keys.device_lookup(device.device_id)
        lookup = self.store.get_known_item(lookup_key)
        if (
            lookup is None
            or lookup.get("tenant_id") != tenant_id
            or lookup.get("device_id") != device.device_id
            or lookup.get("pond_id") != device.pond_id
        ):
            # The Device row came from an earlier Query page; a /v1 move since then shows up
            # as a lookup mismatch, so re-read the row before calling it a conflict.
            raise _Unstable("device_lookup", lookup_key, lookup)
        lookup_version = _stored_int(lookup, "version", "device_lookup", lookup_key)
        if lookup_version != device.version:
            # Legacy writes bump both rows together; a gap is a commit between the reads or a
            # stable divergence (schema conflict after the bounded re-reads).
            raise _Unstable("device_lookup", lookup_key, lookup)

        component_puts, component_row, canonical_planned, survivor = self._plan_component(component)
        # A DECOMMISSIONED Device or an inactive Component never gets a Deployment written.
        active = (
            component.status is not ComponentStatus.DECOMMISSIONED
            and component_row.get("status") == ComponentStatus.ACTIVE.value
        )

        deployment_actions = self._plan_deployment(device, deployment, pond, active=active)
        # One commit-time check per existing Component row: ACTIVE when a Deployment is
        # written against it, and unchanged when it is the survivor a sibling is rebuilt from.
        component_checks: list[dict[str, Any]] = []
        for form, key in (
            ("canonical", self.keys.component(component.component_id)),
            ("mirror", self.keys.component_mirror(tenant_id, component.component_id)),
        ):
            require_active = form == "canonical" and bool(deployment_actions)
            if form == "canonical" and canonical_planned:
                continue
            fenced_version = survivor[1] if survivor and survivor[0] == form else None
            if require_active or fenced_version is not None:
                component_checks.append(
                    {
                        "ConditionCheck": self.store.fence_check_request(
                            key,
                            tenant_id=tenant_id,
                            observed_version=fenced_version,
                            require_active=require_active,
                        )
                    }
                )
        expected_links = {
            DEFAULT_COMPONENT_ID_ATTRIBUTE: component.component_id,
            V2_PROJECTION_VERSION_ATTRIBUTE: V2_PROJECTION_VERSION,
        }
        updates: list[dict[str, Any]] = []
        fences: list[dict[str, Any]] = []
        for key, row, version, kind in (
            (source_key, item, device.version, "device_link"),
            (lookup_key, lookup, lookup_version, "device_lookup_link"),
        ):
            if _links_needed(row, expected_links, kind, key):
                updates.append(
                    {
                        "Update": self.store.device_link_request(
                            key,
                            tenant_id=tenant_id,
                            pond_id=device.pond_id,
                            component_id=component.component_id,
                            observed_version=version,
                        )
                    }
                )
            else:
                # Without a link update, fence the row anyway: a /v1 move committed after these
                # reads must fail the transaction instead of being "repaired" back.
                fences.append(
                    {
                        "ConditionCheck": self.store.device_fence_check_request(
                            key,
                            tenant_id=tenant_id,
                            pond_id=device.pond_id,
                            observed_version=version,
                        )
                    }
                )
        actions = (*component_puts, *deployment_actions, *component_checks, *updates)
        return _RecordPlan(
            kind="device",
            source_key=source_key,
            actions=(*actions, *fences) if actions else (),
            component_created=bool(component_puts),
            deployment_created=bool(deployment_actions),
            link_updated=bool(updates),
        )

    def _plan_deployment(
        self,
        device: Device,
        deployment: Deployment,
        pond: Pond,
        *,
        active: bool,
    ) -> list[dict[str, Any]]:
        """Actions creating or repairing the default Component's Deployment, or `[]`.

        CURRENT is authoritative whoever wrote it (this migration, the `/v1` projection or
        `/v2`): on the Pond's Asset nothing changes, elsewhere the drift is repaired. Without
        CURRENT the deterministic Deployment is created unless it already exists (ended).
        """
        tenant_id = device.tenant_id
        current_key = self.keys.current_deployment(deployment.component_id)
        # CURRENT changes in every Deployment transaction of the Component, so it is read
        # first; the rows it names and the marker are then checked against it, and any mix a
        # concurrent commit can explain is re-read before it may become a conflict.
        current_item = self.store.get_known_item(current_key)
        if current_item is not None:
            current = self._current_deployment(current_item, deployment, current_key)
            marker = self._require_marker(
                tenant_id, deployment.component_id, current_started_at=current.started_at
            )
            if current.asset_id == deployment.asset_id:
                if current.site_id != deployment.site_id:
                    # Same Asset under another Site cannot be right; re-read before conflict.
                    raise _Unstable("deployment_current", current_key, current_item)
                return []
            if not active:
                return []
            if not self._asset_active(tenant_id, deployment.asset_id, pond):
                # Like the `/v1` projection, never reconcile into an archived Asset.
                return []
            return self._drift_repair_actions(device, current, marker, deployment)

        mirror_item = deployment_to_item(deployment)
        history_item = deployment_to_item(deployment, form="history")
        marker_item = deployment_marker_item(
            tenant_id, deployment.component_id, seq=1, last_ended_at=None
        )
        mirror_key = {"PK": mirror_item["PK"], "SK": mirror_item["SK"]}
        history_key = {"PK": history_item["PK"], "SK": history_item["SK"]}
        marker_key = deployment_marker_key(deployment.component_id)
        history = self.store.get_known_item(history_key)
        marker = self.store.get_known_item(marker_key)
        mirror = self.store.get_known_item(mirror_key)
        if mirror is not None:
            _require_identity(mirror, mirror_item, _DEPLOYMENT_FIELDS, "deployment", mirror_key)
            if mirror.get("status") == DeploymentStatus.ACTIVE.value:
                # Active but not CURRENT: CURRENT was absent when read, so this is a commit
                # landing between the reads.
                raise _Unstable("deployment_current", current_key, mirror)
            self._require_ended_group(mirror, history, marker, mirror_key, history_key, marker_key)
            # Ended with nothing CURRENT: already migrated. The migration never restarts an
            # ended interval, because that would invent history.
            return []
        # Stray rows are rejected before any early return for inactive Devices/Components.
        if history is not None:
            raise _Unstable("deployment_history", history_key, [history, marker])
        if marker is not None:
            raise _Unstable("deployment_marker", marker_key, marker)
        if not active:
            return []
        return [
            *(
                {"Put": self.store.put_new_request(target)}
                for target in (
                    mirror_item,
                    history_item,
                    deployment_to_item(deployment, form="current"),
                    marker_item,
                )
            ),
            {
                "ConditionCheck": self.store.asset_exists_check_request(
                    tenant_id, deployment.asset_id
                )
            },
        ]

    def _require_ended_group(
        self,
        mirror: Mapping[str, Any],
        history: Mapping[str, Any] | None,
        marker: Mapping[str, Any] | None,
        mirror_key: Mapping[str, str],
        history_key: Mapping[str, str],
        marker_key: Mapping[str, str],
    ) -> None:
        """An ended deterministic Deployment with no CURRENT must be a complete ended group."""
        ended = _parse_deployment(mirror, "deployment", mirror_key)
        if ended.status is not DeploymentStatus.ENDED or ended.ended_at is None:
            raise _Unstable("deployment", mirror_key, mirror)
        named = deployment_to_item(ended)
        if history is None or any(
            history.get(name) != named.get(name) for name in (*_SIBLING_FIELDS, "ended_at")
        ):
            raise _Unstable("deployment_history", history_key, [mirror, history])
        last_ended_at = None if marker is None else marker.get(MARKER_LAST_ENDED_AT_ATTRIBUTE)
        try:
            marker_ok = (
                marker is not None
                and marker.get("tenant_id") == ended.tenant_id
                and marker.get("component_id") == ended.component_id
                and stored_int(marker.get(MARKER_SEQ_ATTRIBUTE)) >= 1
                and isinstance(last_ended_at, str)
                and parse_canonical_timestamp(last_ended_at) >= ended.ended_at
            )
        except (TypeError, ValueError):
            marker_ok = False
        if not marker_ok:
            raise _Unstable("deployment_marker", marker_key, [mirror, marker])

    def _asset_active(self, tenant_id: str, asset_id: str, pond: Pond) -> bool:
        """Status of the Pond's Asset; one not written yet will mirror the Pond's status."""
        asset = self.store.get_known_item(self.keys.asset(tenant_id, asset_id))
        if asset is None:
            return asset_status_from_legacy(pond.status) is AssetStatus.ACTIVE
        return asset.get("status") == AssetStatus.ACTIVE.value

    def _current_deployment(
        self, current_item: Mapping[str, Any], expected: Deployment, current_key: Mapping[str, str]
    ) -> Deployment:
        """Validate CURRENT and the active mirror/history rows it names."""
        current = _parse_deployment(current_item, "deployment_current", current_key)
        if (
            current.tenant_id != expected.tenant_id
            or current.component_id != expected.component_id
            or current.status is not DeploymentStatus.ACTIVE
        ):
            raise _SchemaConflict("deployment_current", current_key)
        if current.deployment_id == expected.deployment_id:
            _require_identity(
                current_item,
                deployment_to_item(expected, form="current"),
                _DEPLOYMENT_FIELDS,
                "deployment_current",
                current_key,
            )
        named = deployment_to_item(current)
        for form in ("mirror", "history"):
            key = deployment_item_key(current, form)
            row = self.store.get_known_item(key)
            if row is None or any(row.get(name) != named.get(name) for name in _SIBLING_FIELDS):
                raise _Unstable("deployment_current", current_key, [current_item, row])
        return current

    def _require_marker(
        self, tenant_id: str, component_id: str, *, current_started_at: datetime
    ) -> Mapping[str, Any]:
        """Marker of a Component with an active CURRENT: owned, `seq >= 1` and never ahead.

        `last_ended_at`, when present, must not be after CURRENT's start, so the D10 fence a
        repair advances from it never moves backwards.
        """
        marker_key = deployment_marker_key(component_id)
        marker = self.store.get_known_item(marker_key)
        if marker is None:
            raise _Unstable("deployment_marker", marker_key)
        seq = _stored_int(marker, MARKER_SEQ_ATTRIBUTE, "deployment_marker", marker_key)
        if (
            marker.get("tenant_id") != tenant_id
            or marker.get("component_id") != component_id
            or seq < 1
        ):
            raise _SchemaConflict("deployment_marker", marker_key)
        last_ended_at = marker.get(MARKER_LAST_ENDED_AT_ATTRIBUTE)
        if last_ended_at is not None:
            try:
                ok = isinstance(last_ended_at, str) and (
                    parse_canonical_timestamp(last_ended_at) <= current_started_at
                )
            except ValueError:
                ok = False
            if not ok:
                raise _Unstable("deployment_marker", marker_key, marker)
        return marker

    def _drift_repair_actions(
        self,
        device: Device,
        current: Deployment,
        marker: Mapping[str, Any],
        target: Deployment,
    ) -> list[dict[str, Any]]:
        """End CURRENT and start a new interval on the Pond's Asset in one transaction.

        The real move time is unknown; `transition_at` approximates it from the legacy row as
        `max(device.updated_at, CURRENT.started_at + 1µs)` and never reads the clock.
        """
        transition_at = max(
            legacy_timestamp_to_utc(device.updated_at),
            current.started_at + timedelta(microseconds=1),
        )
        ended = Deployment.model_validate(
            {
                **current.model_dump(),
                "status": DeploymentStatus.ENDED,
                "ended_at": transition_at,
                "updated_at": max(transition_at, current.updated_at),
                "version": current.version + 1,
            }
        )
        started = Deployment(
            tenant_id=current.tenant_id,
            deployment_id=new_deployment_id(),
            component_id=current.component_id,
            site_id=target.site_id,
            asset_id=target.asset_id,
            started_at=transition_at,
            status=DeploymentStatus.ACTIVE,
            created_at=transition_at,
            updated_at=transition_at,
            version=1,
        )
        actions: list[dict[str, Any]] = [
            {"Put": self.store.put_new_request(deployment_to_item(started))},
            {"Put": self.store.put_new_request(deployment_to_item(started, form="history"))},
            {
                "Update": self.store.end_deployment_row_request(
                    deployment_item_key(current, "mirror"), current, ended
                )
            },
            {
                "Update": self.store.end_deployment_row_request(
                    deployment_item_key(current, "history"), current, ended
                )
            },
            {"Put": self.store.replace_current_request(current, started)},
            {
                "Update": deployment_marker_advance_request(
                    self.store.table_name,
                    current.tenant_id,
                    current.component_id,
                    observed_seq=marker[MARKER_SEQ_ATTRIBUTE],
                    ended_at=transition_at,
                )
            },
            {
                "ConditionCheck": self.store.asset_active_check_request(
                    current.tenant_id, target.asset_id
                )
            },
        ]
        return actions

    # Apply ----------------------------------------------------------------------------------

    def _apply_site(self, plan: _TenantPlan, counts: Counter[str], deadline: float) -> None:
        action = plan.site_action
        for attempt in range(MAX_RECORD_ATTEMPTS + 1):
            if action is None:
                return
            if attempt == MAX_RECORD_ATTEMPTS:
                break
            self._check_deadline(deadline)
            try:
                self.store.transact_write_sync(action)
            except DynamoConditionalConflict:
                pass
            except ClientError as exc:
                if not _is_transaction_conflict(exc):
                    raise
                self._backoff(attempt)
            else:
                counts["default_sites_created"] += 1
                return
            plan.tenant = self._reload_tenant(plan.tenant.tenant_id)
            _, action = self._plan_site(plan.tenant)
        raise _RetryExhausted("default site")

    def _apply_record(
        self, record: _RecordPlan, plan: _TenantPlan, counts: Counter[str], deadline: float
    ) -> None:
        for attempt in range(MAX_RECORD_ATTEMPTS + 1):
            if not record.actions:
                counts["already_migrated"] += 1
                return
            if attempt == MAX_RECORD_ATTEMPTS:
                break
            # Checked before every write, retries included: nothing is written after it.
            self._check_deadline(deadline)
            try:
                self.store.transact_write_sync(record.actions)
            except DynamoConditionalConflict:
                pass
            except ClientError as exc:
                if not _is_transaction_conflict(exc):
                    raise
                self._backoff(attempt)
            else:
                counts.update(record.counts())
                return
            record = self._replan(record, plan, counts, deadline)
        raise _RetryExhausted(record.kind)

    def _replan(
        self, record: _RecordPlan, plan: _TenantPlan, counts: Counter[str], deadline: float
    ) -> _RecordPlan:
        """Re-read the source row and known keys after a failed write."""
        item = self.store.get_known_item(record.source_key)
        if item is None:
            raise _SchemaConflict(f"{record.kind}_missing", record.source_key)
        return self._plan_record(
            record.kind,
            item,
            plan,
            lambda pond_record: self._apply_record(pond_record, plan, counts, deadline),
        )

    # Helpers --------------------------------------------------------------------------------

    def _plan_component(
        self, component: DeviceComponent
    ) -> tuple[list[dict[str, Any]], Mapping[str, Any], bool, tuple[str, int] | None]:
        """Puts for the Component pair, the row whose status rules, whether the canonical row
        is written in this transaction, and the survivor (form, version) a sibling copies.

        Both rows commit together and are read mirror first. With both absent they come from
        the legacy builder; with one absent the missing sibling is rebuilt from the SURVIVOR
        (same version and status) and the survivor is fenced on its observed version, so a
        Component renamed, versioned or decommissioned through v2 is never reset.
        """
        canonical_item = component_to_item(component)
        mirror_item = component_to_item(component, canonical=False)
        canonical_key = {"PK": canonical_item["PK"], "SK": canonical_item["SK"]}
        mirror_key = {"PK": mirror_item["PK"], "SK": mirror_item["SK"]}
        mirror = self.store.get_known_item(mirror_key)
        canonical = self.store.get_known_item(canonical_key)
        for row, expected, key in (
            (mirror, mirror_item, mirror_key),
            (canonical, canonical_item, canonical_key),
        ):
            if row is not None:
                _require_identity(row, expected, _COMPONENT_FIELDS, "component", key)
        if canonical is None and mirror is None:
            puts = [
                {"Put": self.store.put_new_request(canonical_item)},
                {"Put": self.store.put_new_request(mirror_item)},
            ]
            return puts, canonical_item, True, None
        if canonical is not None and mirror is not None:
            return [], canonical, False, None
        if canonical is not None:
            version = _stored_int(canonical, "version", "component", canonical_key)
            sibling = {**canonical, **mirror_key, "entity_type": COMPONENT_MIRROR_ENTITY_TYPE}
            return (
                [{"Put": self.store.put_new_request(sibling)}],
                canonical,
                False,
                ("canonical", version),
            )
        assert mirror is not None
        version = _stored_int(mirror, "version", "component", mirror_key)
        sibling = {**mirror, **canonical_key, "entity_type": COMPONENT_ENTITY_TYPE}
        return [{"Put": self.store.put_new_request(sibling)}], mirror, True, ("mirror", version)

    def _profile_from_asset(
        self,
        asset: Mapping[str, Any],
        asset_key: Mapping[str, str],
        pond: Pond,
        expected: Mapping[str, Any],
    ) -> dict[str, Any]:
        try:
            profile = PondProfile(
                tenant_id=pond.tenant_id,
                asset_id=asset["asset_id"],
                description=pond.description,
                legacy_pond_id=pond.pond_id,
                created_at=parse_canonical_timestamp(asset["created_at"]),
                updated_at=parse_canonical_timestamp(asset["updated_at"]),
                version=_stored_int(asset, "version", "asset", asset_key),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise _SchemaConflict("asset", asset_key) from exc
        item = pond_profile_to_item(profile)
        _require_identity(item, expected, _PROFILE_FIELDS, "pond_profile", asset_key)
        return item

    def _backoff(self, attempt: int) -> None:
        self._sleep(random.uniform(0, BACKOFF_BASE_SECONDS * 2**attempt))

    def _check_deadline(self, deadline: float) -> None:
        if self._monotonic() >= deadline:
            raise _DeadlineExceeded

    @staticmethod
    def _parse[T](
        parser: Callable[[Mapping[str, Any]], T],
        item: Mapping[str, Any],
        kind: str,
        key: Mapping[str, str],
    ) -> T:
        try:
            return parser(item)
        except (KeyError, TypeError, ValueError) as exc:
            raise _SchemaConflict(kind, key) from exc
