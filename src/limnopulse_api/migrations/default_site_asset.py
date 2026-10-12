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
partial Deployment group) re-reads the record, bounded by `MAX_RECORD_ATTEMPTS`, before it is
reported as a conflict; a Pond created during the run is read and planned before its Device.
Exhausted retries mark only that tenant failed (result `failed`, exit 1); other tenants continue.
The run is idempotent, so rerunning it converges any tenant left incomplete.

Counters: `assets_created` counts Asset puts only. A PondProfile written for an existing Asset
counts under `legacy_links_updated`, because the profile's `legacy_pond_id` is the reverse
v2 -> v1 link. `devices_projected` counts Devices whose Component or Deployment is created; a
Device that only gains links counts under `legacy_links_updated`. Ownership comparisons never look at names,
descriptions, status, version or `updated_at`, which later `/v1` or `/v2` edits may change. A
Device whose deterministic Deployment exists but is no longer CURRENT (it has since moved) is
already migrated. The receipt records each completed run and is never used to skip validation.
"""

import logging
import random
import time
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
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
)
from limnopulse_api.adapters.dynamodb_deployments import (
    deployment_marker_item,
    deployment_marker_key,
    deployment_to_item,
)
from limnopulse_api.adapters.dynamodb_devices_v2 import component_to_item
from limnopulse_api.adapters.dynamodb_sites_assets import (
    asset_to_item,
    pond_profile_to_item,
    site_to_item,
)
from limnopulse_api.adapters.dynamodb_v2 import DynamoConditionalConflict, utc_now
from limnopulse_api.domain.deployments import Deployment
from limnopulse_api.domain.devices_v2 import ComponentStatus
from limnopulse_api.domain.entities import Device, Pond, Tenant
from limnopulse_api.domain.ids import asset_id_for_legacy_pond
from limnopulse_api.domain.legacy_projection import (
    asset_and_profile_for_legacy_pond,
    component_for_legacy_device,
    default_site_for_tenant,
    deployment_for_legacy_device,
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
_COMPONENT_FIELDS = ("tenant_id", "component_id", "device_id", "kind")
_DEPLOYMENT_FIELDS = ("tenant_id", "deployment_id", "component_id", "started_at")
_CURRENT_FIELDS = (*_DEPLOYMENT_FIELDS, "asset_id")
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

    def __init__(self, kind: str, key: Mapping[str, str]) -> None:
        super().__init__(kind)
        self.kind = kind
        self.key = dict(key)


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
        # The PondProfile carries `legacy_pond_id`, the reverse v2 -> v1 link.
        counts["legacy_links_updated"] += int(self.link_updated or self.profile_created)
        return counts


@dataclass
class _TenantPlan:
    tenant: Tenant
    site_id: str
    site_action: dict[str, Any] | None
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


def _require_identity(
    existing: Mapping[str, Any],
    expected: Mapping[str, Any],
    fields: Sequence[str],
    kind: str,
    key: Mapping[str, str],
) -> None:
    if any(existing.get(name) != expected.get(name) for name in fields):
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
        if plan.conflicts:
            state.add_conflicts(tenant_id, plan.conflicts)
            return
        if not apply:
            for name, count in plan.counts().items():
                state.counts[_CREATED_TO_WOULD.get(name, name)] += count
            state.tenants_completed += 1
            return

        tenant_counts: Counter[str] = Counter()
        try:
            self._apply_site(plan, tenant_counts)
            for record in plan.records:
                self._check_deadline(deadline)
                self._apply_record(record, plan, tenant_counts)
        except _SchemaConflict as conflict:
            state.add_conflicts(tenant_id, [conflict])
            return
        except _RetryExhausted as exc:
            _LOGGER.error("tenant %s not completed: retries exhausted on %s", tenant_id, exc)
            state.tenants_failed += 1
            return
        finally:
            state.counts.update(tenant_counts)
        self.store.record_migration_receipt(
            tenant_id,
            completed_at=self._clock(),
            counts={name: tenant_counts[name] for name in (*_CREATED_TO_WOULD, "already_migrated")},
        )
        state.tenants_completed += 1

    def _plan_tenant(self, tenant_id: str, page_size: int, deadline: float) -> _TenantPlan:
        meta_key = self.store.tenant_meta_key(tenant_id)
        item = self.store.get_tenant_item(tenant_id)
        if item is None:
            raise _SchemaConflict("tenant_missing", meta_key)
        tenant = self._parse(legacy_tenant_from_item, item, "tenant_invalid", meta_key)
        if tenant.tenant_id != tenant_id:
            raise _SchemaConflict("tenant_invalid", meta_key)
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
        return plan

    # Planning -------------------------------------------------------------------------------

    def _plan_record(
        self,
        kind: Literal["pond", "device"],
        item: Mapping[str, Any],
        plan: _TenantPlan,
        on_new_pond: Callable[[_RecordPlan], None],
    ) -> _RecordPlan:
        """Plan one record; a mixed state re-reads the source row and re-plans, bounded."""
        source_key = _source_key(item)
        unstable: _Unstable | None = None
        for attempt in range(MAX_RECORD_ATTEMPTS):
            if attempt:
                fresh = self.store.get_known_item(source_key)
                if fresh is None:
                    raise _SchemaConflict(f"{kind}_missing", source_key)
                item = fresh
            try:
                return self._plan_from_item(kind, item, plan, on_new_pond)
            except _Unstable as exc:
                unstable = exc
        assert unstable is not None
        raise _SchemaConflict(unstable.kind, unstable.key)

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
            return self._plan_pond(pond, item, plan.site_id)
        if not is_legacy_device_item(item):
            raise _SchemaConflict("device_invalid", _source_key(item))
        device = self._parse_device(tenant_id, item)
        self._ensure_pond(device, item, plan, on_new_pond)
        return self._plan_device(device, item, plan.site_id)

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

    def _plan_site(self, tenant: Tenant) -> tuple[str, dict[str, Any] | None]:
        site = default_site_for_tenant(tenant)
        item = site_to_item(site)
        key = self.keys.site(site.tenant_id, site.site_id)
        existing = self.store.get_known_item(key)
        if existing is None:
            return site.site_id, self.store.put_new_request(item)
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

    def _plan_pond(self, pond: Pond, item: Mapping[str, Any], site_id: str) -> _RecordPlan:
        source_key = _source_key(item)
        try:
            projection = asset_and_profile_for_legacy_pond(pond, site_id)
        except ValueError as exc:
            raise _SchemaConflict("pond_invalid", source_key) from exc
        asset_item = asset_to_item(projection.asset)
        profile_item = pond_profile_to_item(projection.pond_profile)
        # Asset, PondProfile and the Pond links commit in one transaction and absent rows are
        # planned as conditional puts, so a concurrent commit between these reads only makes
        # the transaction fail and the record re-plan; it never raises a conflict.
        profile_put = self._plan_put(profile_item, _PROFILE_FIELDS, "pond_profile")
        asset_put = self._plan_put(asset_item, _ASSET_FIELDS, "asset")
        actions = [action for action in (asset_put, profile_put) if action is not None]

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
        return _RecordPlan(
            kind="pond",
            source_key=source_key,
            actions=tuple(actions),
            asset_created=asset_put is not None,
            profile_created=profile_put is not None,
            link_updated=link_updated,
        )

    def _plan_device(self, device: Device, item: Mapping[str, Any], site_id: str) -> _RecordPlan:
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
            raise _Unstable("device_lookup", lookup_key)

        # Mirror, then canonical; both commit together and absent rows are conditional puts.
        component_puts = [
            action
            for action in (
                self._plan_put(
                    component_to_item(component, canonical=False), _COMPONENT_FIELDS, "component"
                ),
                self._plan_put(component_to_item(component), _COMPONENT_FIELDS, "component"),
            )
            if action is not None
        ]
        puts = list(reversed(component_puts))
        component_created = bool(component_puts)

        deployment_puts = self._plan_deployment(
            deployment, active=component.status is not ComponentStatus.DECOMMISSIONED
        )
        puts.extend(deployment_puts)
        checks: list[dict[str, Any]] = []
        if deployment_puts:
            checks.append(
                {"ConditionCheck": self.store.asset_exists_check_request(tenant_id, asset_id)}
            )

        expected_links = {
            DEFAULT_COMPONENT_ID_ATTRIBUTE: component.component_id,
            V2_PROJECTION_VERSION_ATTRIBUTE: V2_PROJECTION_VERSION,
        }
        updates: list[dict[str, Any]] = []
        for key, row, kind in (
            (source_key, item, "device_link"),
            (lookup_key, lookup, "device_lookup_link"),
        ):
            if _links_needed(row, expected_links, kind, key):
                updates.append(
                    {
                        "Update": self.store.device_link_request(
                            key,
                            tenant_id=tenant_id,
                            pond_id=device.pond_id,
                            component_id=component.component_id,
                            observed_version=int(row["version"]),
                        )
                    }
                )
        return _RecordPlan(
            kind="device",
            source_key=source_key,
            actions=(*puts, *checks, *updates),
            component_created=component_created,
            deployment_created=bool(deployment_puts),
            link_updated=bool(updates),
        )

    def _plan_deployment(self, deployment: Deployment, *, active: bool) -> list[dict[str, Any]]:
        """Puts creating the deterministic Deployment, or `[]` when it exists or must not."""
        mirror_item = deployment_to_item(deployment)
        history_item = deployment_to_item(deployment, form="history")
        current_item = deployment_to_item(deployment, form="current")
        marker_item = deployment_marker_item(
            deployment.tenant_id, deployment.component_id, seq=1, last_ended_at=None
        )
        mirror_key = {"PK": mirror_item["PK"], "SK": mirror_item["SK"]}
        history_key = {"PK": history_item["PK"], "SK": history_item["SK"]}
        current_key = {"PK": current_item["PK"], "SK": current_item["SK"]}
        marker_key = deployment_marker_key(deployment.component_id)

        # All four rows commit in one transaction. Siblings are read first and the mirror last,
        # so a present mirror proves the whole group; any other mix is re-read before it can
        # become a conflict (it may be a concurrent commit landing between these reads).
        current = self.store.get_known_item(current_key)
        history = self.store.get_known_item(history_key)
        marker = self.store.get_known_item(marker_key)
        mirror = self.store.get_known_item(mirror_key)
        if mirror is not None:
            _require_identity(mirror, mirror_item, _DEPLOYMENT_FIELDS, "deployment", mirror_key)
            if history is None:
                raise _Unstable("deployment_history", history_key)
            _require_identity(
                history, history_item, _DEPLOYMENT_FIELDS, "deployment_history", history_key
            )
            if marker is None:
                raise _Unstable("deployment_marker", marker_key)
            _require_identity(marker, marker_item, _MARKER_FIELDS, "deployment_marker", marker_key)
            # A later CURRENT means the Device has since moved: already migrated.
            if current is not None and current.get("deployment_id") == deployment.deployment_id:
                _require_identity(
                    current, current_item, _CURRENT_FIELDS, "deployment_current", current_key
                )
            return []
        if current is not None:
            raise _Unstable("deployment_current", current_key)
        if not active:
            return []
        if history is not None:
            raise _Unstable("deployment_history", history_key)
        if marker is not None:
            raise _Unstable("deployment_marker", marker_key)
        return [
            {"Put": self.store.put_new_request(target)}
            for target in (mirror_item, history_item, current_item, marker_item)
        ]

    # Apply ----------------------------------------------------------------------------------

    def _apply_site(self, plan: _TenantPlan, counts: Counter[str]) -> None:
        action = plan.site_action
        for attempt in range(MAX_RECORD_ATTEMPTS + 1):
            if action is None:
                return
            if attempt == MAX_RECORD_ATTEMPTS:
                break
            try:
                self.store.put_item_sync(action)
            except DynamoConditionalConflict:
                pass
            except ClientError as exc:
                if not _is_transaction_conflict(exc):
                    raise
                self._backoff(attempt)
            else:
                counts["default_sites_created"] += 1
                return
            _, action = self._plan_site(plan.tenant)
        raise _RetryExhausted("default site")

    def _apply_record(self, record: _RecordPlan, plan: _TenantPlan, counts: Counter[str]) -> None:
        for attempt in range(MAX_RECORD_ATTEMPTS + 1):
            if not record.actions:
                counts["already_migrated"] += 1
                return
            if attempt == MAX_RECORD_ATTEMPTS:
                break
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
            record = self._replan(record, plan, counts)
        raise _RetryExhausted(record.kind)

    def _replan(self, record: _RecordPlan, plan: _TenantPlan, counts: Counter[str]) -> _RecordPlan:
        """Re-read the source row and known keys after a failed write."""
        item = self.store.get_known_item(record.source_key)
        if item is None:
            raise _SchemaConflict(f"{record.kind}_missing", record.source_key)
        return self._plan_record(
            record.kind,
            item,
            plan,
            lambda pond_record: self._apply_record(pond_record, plan, counts),
        )

    # Helpers --------------------------------------------------------------------------------

    def _plan_put(
        self, target: Mapping[str, Any], fields: Sequence[str], kind: str
    ) -> dict[str, Any] | None:
        """Conditional put for an absent row; an existing row must match on identity."""
        key = {"PK": target["PK"], "SK": target["SK"]}
        existing = self.store.get_known_item(key)
        if existing is None:
            return {"Put": self.store.put_new_request(target)}
        _require_identity(existing, target, fields, kind, key)
        return None

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
