"""Unit tests for the telemetry normalizer (P2-06): ownership, event-time Deployment, versions."""

import asyncio
import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from itertools import count
from types import MappingProxyType
from typing import Any

import pytest

from limnopulse_api.core.errors import TelemetryWriteError
from limnopulse_api.domain.assets import AssetWithPondProfile, Site
from limnopulse_api.domain.deployments import Deployment
from limnopulse_api.domain.devices_v2 import (
    ActuatorProfile,
    ComponentKind,
    DeviceComponent,
    ProbeProfile,
)
from limnopulse_api.domain.entities import Device, Pond, Tenant
from limnopulse_api.domain.ids import (
    default_component_id_for_legacy_device,
    default_probe_profile_id_for_legacy_device,
    new_actuator_profile_id,
    new_component_id,
    new_deployment_id,
    new_probe_profile_id,
)
from limnopulse_api.domain.legacy_projection import (
    asset_and_profile_for_legacy_pond,
    component_for_legacy_device,
    default_site_for_tenant,
    deployment_for_legacy_device,
    device_v2_from_legacy,
)
from limnopulse_api.domain.metric_catalog import METRIC_CATALOGS
from limnopulse_api.domain.telemetry_quality import QUALITY_POLICIES, QualityPolicy
from limnopulse_api.domain.telemetry_units import UNIT_MAPPINGS
from limnopulse_api.domain.telemetry_v2 import (
    MAX_OBSERVATIONS_PER_BATCH,
    MAX_QUARANTINE_EXCERPT_BYTES,
    CanonicalObservation,
    NormalizationResult,
    NormalizationVersions,
    QualityFlag,
    QualityProvenance,
    QualityState,
    QuarantinedObservation,
    QuarantineReason,
    SourceObservation,
    SourceType,
    TrustedSourceContext,
    observation_event_id,
)
from limnopulse_api.domain.v2_common import IdempotencyReceipt, new_idempotency_receipt
from limnopulse_api.services.telemetry_normalizer import TelemetryNormalizer
from tests.contracts.m1_fakes import InMemoryM1Repositories, M1AcceptanceScenario

TENANT = "tenant-a"
OTHER_TENANT = "tenant-b"
DEVICE = "dev-legacy-1"
SECOND_DEVICE = "dev-legacy-2"
OTHER_TENANT_DEVICE = "dev-tenant-b-1"
CREATED = datetime(2026, 1, 1, tzinfo=UTC)
MOVE = datetime(2026, 7, 1, 8, 30, tzinfo=UTC)
OBSERVED = datetime(2026, 7, 9, 12, 0, tzinfo=UTC)
RECEIVED = OBSERVED + timedelta(seconds=5)
INGESTED = datetime(2026, 7, 9, 12, 30, tzinfo=UTC)  # far after receipt: never a delay input
COMPONENT = default_component_id_for_legacy_device(TENANT, DEVICE)

_receipts = count()


def receipt(tenant_id: str) -> IdempotencyReceipt:
    digest = hashlib.sha256(f"p2-06-{next(_receipts)}".encode()).hexdigest()
    return new_idempotency_receipt(
        tenant_id=tenant_id,
        scope="p2-06/test",
        key_hash=digest,
        request_hash=digest,
        response_snapshot_json="{}",
        created_at=CREATED,
    )


def legacy_tenant(tenant_id: str) -> Tenant:
    return Tenant(
        tenant_id=tenant_id,
        name=f"Tenant {tenant_id}",
        created_at=CREATED,
        updated_at=CREATED,
        version=1,
    )


def legacy_pond(tenant_id: str, pond_id: str) -> Pond:
    return Pond(
        tenant_id=tenant_id,
        pond_id=pond_id,
        name=pond_id,
        created_at=CREATED,
        updated_at=CREATED,
        version=1,
    )


def legacy_device(tenant_id: str, pond_id: str, device_id: str) -> Device:
    return Device(
        tenant_id=tenant_id,
        pond_id=pond_id,
        device_id=device_id,
        name=device_id,
        created_at=CREATED,
        updated_at=CREATED,
        version=1,
    )


@dataclass
class World:
    repos: InMemoryM1Repositories
    site: Site
    west: AssetWithPondProfile
    east: AssetWithPondProfile
    west_deployment: Deployment
    east_deployment: Deployment
    second_component: str
    other_tenant_component: str
    native_device: str = ""
    probes: list[DeviceComponent] = field(default_factory=list)
    native_deployments: dict[str, Deployment] = field(default_factory=dict)
    hybrid: DeviceComponent | None = None
    actuator: DeviceComponent | None = None


async def _seed_legacy(
    repos: InMemoryM1Repositories, tenant_id: str, pond_id: str, device_id: str
) -> tuple[Site, AssetWithPondProfile, Deployment]:
    site = await repos.get_site(
        tenant_id, default_site_for_tenant(legacy_tenant(tenant_id)).site_id
    )
    if site is None:
        site = await repos.create_site(
            default_site_for_tenant(legacy_tenant(tenant_id)), receipt(tenant_id)
        )
    pond = asset_and_profile_for_legacy_pond(legacy_pond(tenant_id, pond_id), site.site_id)
    asset = await repos.get_asset(tenant_id, pond.asset.asset_id)
    if asset is None:
        asset = await repos.create_asset(pond.asset, pond.pond_profile, receipt(tenant_id))
    device = legacy_device(tenant_id, pond_id, device_id)
    await repos.create_device(device_v2_from_legacy(device), receipt(tenant_id))
    await repos.create_component(component_for_legacy_device(device), receipt(tenant_id))
    deployment = await repos.start_deployment(
        deployment_for_legacy_device(device, asset.asset.asset_id, site.site_id), receipt(tenant_id)
    )
    return site, asset, deployment


async def _move_legacy(
    repos: InMemoryM1Repositories, west_deployment: Deployment, east: AssetWithPondProfile
) -> tuple[Deployment, Deployment]:
    """West -> East relocation of a legacy default Component at `MOVE` (repository level)."""
    ended = await repos.end_deployment(
        west_deployment.tenant_id,
        west_deployment.deployment_id,
        expected_version=west_deployment.version,
        ended_at=MOVE,
    )
    started = await repos.start_deployment(
        Deployment(
            tenant_id=west_deployment.tenant_id,
            deployment_id=new_deployment_id(),
            component_id=west_deployment.component_id,
            site_id=east.asset.site_id,
            asset_id=east.asset.asset_id,
            started_at=MOVE,
            created_at=MOVE,
            updated_at=MOVE,
            version=1,
        ),
        receipt(west_deployment.tenant_id),
    )
    return ended, started


async def _seed_world(repos: InMemoryM1Repositories) -> World:
    site, west, west_deployment = await _seed_legacy(repos, TENANT, "pond-west", DEVICE)
    _, east, _ = await _seed_legacy(repos, TENANT, "pond-east", SECOND_DEVICE)
    _, _, _ = await _seed_legacy(repos, OTHER_TENANT, "pond-b", OTHER_TENANT_DEVICE)
    ended, east_deployment = await _move_legacy(repos, west_deployment, east)
    return World(
        repos=repos,
        site=site,
        west=west,
        east=east,
        west_deployment=ended,
        east_deployment=east_deployment,
        second_component=default_component_id_for_legacy_device(TENANT, SECOND_DEVICE),
        other_tenant_component=default_component_id_for_legacy_device(
            OTHER_TENANT, OTHER_TENANT_DEVICE
        ),
    )


def _add_native(world: World) -> None:
    """Native probes through the real M1 services, plus a hybrid and an actuator Component."""
    scenario = M1AcceptanceScenario(
        site_assets=world.repos,
        device_components=world.repos,
        deployments=world.repos,
        clock=lambda: CREATED,
    )
    site = scenario.create_site(TENANT, "Native site")
    asset = scenario.create_asset(TENANT, site.site_id, "Native pond")
    device = scenario.create_device(TENANT, "Native device")
    world.native_device = device.device_id
    for name in ("Probe A", "Probe B"):
        probe = scenario.create_probe(TENANT, device.device_id, name)
        world.probes.append(probe)
        world.native_deployments[probe.component_id] = scenario.start(
            probe.component_id, asset.asset.asset_id, CREATED
        )

    def component(kind: ComponentKind, *, probe: bool, actuator: bool) -> DeviceComponent:
        return DeviceComponent(
            tenant_id=TENANT,
            component_id=new_component_id(),
            device_id=device.device_id,
            kind=kind,
            name=kind.value,
            probe_profile=ProbeProfile(probe_profile_id=new_probe_profile_id()) if probe else None,
            actuator_profile=(
                ActuatorProfile(actuator_profile_id=new_actuator_profile_id(), actuator_type="pump")
                if actuator
                else None
            ),
            created_at=CREATED,
            updated_at=CREATED,
            version=1,
        )

    async def seed() -> None:
        world.hybrid = await world.repos.create_component(
            component(ComponentKind.HYBRID, probe=True, actuator=True), receipt(TENANT)
        )
        world.actuator = await world.repos.create_component(
            component(ComponentKind.ACTUATOR, probe=False, actuator=True), receipt(TENANT)
        )
        for item in (world.hybrid, world.actuator):
            world.native_deployments[item.component_id] = await world.repos.start_deployment(
                Deployment(
                    tenant_id=TENANT,
                    deployment_id=new_deployment_id(),
                    component_id=item.component_id,
                    site_id=site.site_id,
                    asset_id=asset.asset.asset_id,
                    started_at=CREATED,
                    created_at=CREATED,
                    updated_at=CREATED,
                    version=1,
                ),
                receipt(TENANT),
            )

    asyncio.run(seed())


@pytest.fixture
def world() -> World:
    repos = InMemoryM1Repositories(clock=lambda: CREATED)
    built = asyncio.run(_seed_world(repos))
    _add_native(built)
    return built


class SpyRepositories:
    """Delegates the two allowed lookups and fails on any current-Deployment fallback."""

    def __init__(self, inner: InMemoryM1Repositories) -> None:
        self.inner = inner
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.forbidden: list[str] = []

    async def get_component(self, tenant_id: str, component_id: str) -> DeviceComponent | None:
        self.calls.append(("get_component", (tenant_id, component_id)))
        return await self.inner.get_component(tenant_id, component_id)

    async def get_deployment_at(
        self, tenant_id: str, component_id: str, observed_at: datetime
    ) -> Deployment | None:
        self.calls.append(("get_deployment_at", (tenant_id, component_id, observed_at)))
        return await self.inner.get_deployment_at(tenant_id, component_id, observed_at)

    async def get_current_deployment(self, *args: object, **kwargs: object) -> None:
        self.forbidden.append("get_current_deployment")
        raise AssertionError("the normalizer must never fall back to the current Deployment")

    async def list_component_deployments(self, *args: object, **kwargs: object) -> None:
        self.forbidden.append("list_component_deployments")
        raise AssertionError("the normalizer must resolve through get_deployment_at only")

    async def get_deployment(self, *args: object, **kwargs: object) -> None:
        self.forbidden.append("get_deployment")
        raise AssertionError("the normalizer must resolve through get_deployment_at only")

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    def count(self, name: str) -> int:
        return sum(1 for call, _ in self.calls if call == name)


def normalizer(
    repos: Any,
    *,
    clock: Any = None,
    **kwargs: Any,
) -> TelemetryNormalizer:
    return TelemetryNormalizer(
        components=repos,
        deployments=repos,
        clock=clock or (lambda: INGESTED),
        **kwargs,
    )


def context(**changes: Any) -> TrustedSourceContext:
    values: dict[str, Any] = {
        "tenant_id": TENANT,
        "device_id": DEVICE,
        "integration_id": "di_0123456789abcdef0123456789abcdef",
        "integration_type": "vendor.acme",
        "source_type": SourceType.MQTT,
        "connector_version": "1.2.3",
        "received_at": RECEIVED,
    }
    values.update(changes)
    return TrustedSourceContext(**values)


def observation(**changes: Any) -> SourceObservation:
    values: dict[str, Any] = {
        "component_id": COMPONENT,
        "metric_key": "water.temperature",
        "value": 25.1,
        "source_unit": "degC",
        "observed_at": OBSERVED,
        "sequence": 7,
        "source_event_id": "src-evt-1",
        "source_metadata": {"firmware_version": "fw-1.0"},
    }
    values.update(changes)
    return SourceObservation(**values)


def only_accepted(result: NormalizationResult) -> CanonicalObservation:
    assert result.quarantined == ()
    assert len(result.accepted) == 1
    return result.accepted[0]


def only_quarantined(result: NormalizationResult) -> QuarantinedObservation:
    assert result.accepted == ()
    assert len(result.quarantined) == 1
    return result.quarantined[0]


# Canonical observation shape --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_accepted_observation_carries_resolution_source_and_versions(world: World) -> None:
    source = observation()

    canonical = only_accepted(await normalizer(world.repos).normalize(context(), [source]))

    assert canonical.event_id == observation_event_id(
        TENANT, COMPONENT, "water.temperature", OBSERVED
    )
    resolved = canonical.resolved
    assert resolved.tenant_id == TENANT
    assert resolved.device_id == DEVICE
    assert resolved.component_id == COMPONENT
    assert resolved.probe_id == default_probe_profile_id_for_legacy_device(TENANT, DEVICE)
    assert resolved.deployment_id == world.east_deployment.deployment_id
    assert resolved.asset_id == world.east.asset.asset_id
    assert resolved.site_id == world.east.asset.site_id
    assert canonical.source.integration_id == "di_0123456789abcdef0123456789abcdef"
    assert canonical.source.integration_type == "vendor.acme"
    assert canonical.source.source_type is SourceType.MQTT
    assert canonical.source.connector_version == "1.2.3"
    assert canonical.source.received_at == RECEIVED
    assert canonical.source_event_id == "src-evt-1"
    assert dict(canonical.source_metadata) == {"firmware_version": "fw-1.0"}
    measured = canonical.observation
    assert measured.metric_key == "water.temperature"
    assert measured.value == 25.1
    assert measured.source_unit == "degC"
    assert measured.canonical_unit == "degC"
    assert measured.observed_at == OBSERVED
    assert measured.sequence == 7
    assert canonical.delivery.ingested_at == INGESTED
    assert canonical.delivery.delayed is False
    assert canonical.delivery.replay is False
    assert canonical.quality.state is QualityState.VALID
    assert canonical.quality.flags == ()
    assert canonical.quality.provenance == (
        QualityProvenance.SCHEMA_VALIDATION,
        QualityProvenance.UNIT_MAPPING,
    )
    assert canonical.mapping == NormalizationVersions()


@pytest.mark.asyncio
async def test_ingested_at_comes_from_the_clock_at_normalization(world: World) -> None:
    now = [INGESTED]

    subject = normalizer(world.repos, clock=lambda: now[0])
    first = only_accepted(await subject.normalize(context(), [observation()]))
    now[0] = INGESTED + timedelta(hours=1)
    second = only_accepted(await subject.normalize(context(), [observation()]))

    assert first.delivery.ingested_at == INGESTED
    assert second.delivery.ingested_at == INGESTED + timedelta(hours=1)
    assert first.event_id == second.event_id
    assert first.quality == second.quality


@pytest.mark.asyncio
async def test_accepted_and_quarantined_keep_input_order(world: World) -> None:
    items = [
        observation(metric_key="water.ph", value=7.0, source_unit="pH"),
        observation(metric_key="water.bogus"),
        observation(metric_key="water.salinity", value=11.0, source_unit="ppt"),
        observation(metric_key="water.ph", value=7.0, source_unit="mol"),
    ]

    result = await normalizer(world.repos).normalize(context(), items)

    assert [o.observation.metric_key for o in result.accepted] == ["water.ph", "water.salinity"]
    assert [q.reason for q in result.quarantined] == [
        QuarantineReason.METRIC_UNKNOWN,
        QuarantineReason.UNIT_UNSUPPORTED,
    ]


# Ownership and measurement capability ----------------------------------------------------------


@pytest.mark.asyncio
async def test_component_of_another_tenant_is_not_owned(world: World) -> None:
    source = observation(component_id=world.other_tenant_component)

    record = only_quarantined(await normalizer(world.repos).normalize(context(), [source]))

    assert record.reason is QuarantineReason.COMPONENT_NOT_OWNED
    assert record.tenant_id == TENANT
    assert record.component_id == world.other_tenant_component


@pytest.mark.asyncio
async def test_other_tenant_context_owns_only_its_own_device_component(world: World) -> None:
    source = observation(component_id=world.other_tenant_component)
    claimed = context(tenant_id=OTHER_TENANT, device_id=OTHER_TENANT_DEVICE)

    canonical = only_accepted(await normalizer(world.repos).normalize(claimed, [source]))

    assert canonical.resolved.tenant_id == OTHER_TENANT
    assert canonical.resolved.device_id == OTHER_TENANT_DEVICE


@pytest.mark.asyncio
async def test_component_of_another_device_is_not_owned(world: World) -> None:
    source = observation(component_id=world.second_component)

    record = only_quarantined(await normalizer(world.repos).normalize(context(), [source]))

    assert record.reason is QuarantineReason.COMPONENT_NOT_OWNED
    assert record.device_id == DEVICE
    assert record.component_id == world.second_component


@pytest.mark.asyncio
async def test_unknown_component_is_not_owned(world: World) -> None:
    source = observation(component_id=new_component_id())

    record = only_quarantined(await normalizer(world.repos).normalize(context(), [source]))

    assert record.reason is QuarantineReason.COMPONENT_NOT_OWNED


@pytest.mark.asyncio
async def test_actuator_component_does_not_measure(world: World) -> None:
    assert world.actuator is not None
    source = observation(component_id=world.actuator.component_id)

    record = only_quarantined(
        await normalizer(world.repos).normalize(context(device_id=world.native_device), [source])
    )

    assert record.reason is QuarantineReason.COMPONENT_NOT_MEASURING
    assert record.component_id == world.actuator.component_id


@pytest.mark.asyncio
async def test_hybrid_component_measures_with_its_probe_profile(world: World) -> None:
    assert world.hybrid is not None and world.hybrid.probe_profile is not None
    source = observation(component_id=world.hybrid.component_id)

    canonical = only_accepted(
        await normalizer(world.repos).normalize(context(device_id=world.native_device), [source])
    )

    assert canonical.resolved.probe_id == world.hybrid.probe_profile.probe_profile_id
    deployment = world.native_deployments[world.hybrid.component_id]
    assert canonical.resolved.deployment_id == deployment.deployment_id


@pytest.mark.asyncio
async def test_ownership_is_checked_before_the_metric(world: World) -> None:
    source = observation(component_id=world.second_component, metric_key="water.bogus")

    record = only_quarantined(await normalizer(world.repos).normalize(context(), [source]))

    assert record.reason is QuarantineReason.COMPONENT_NOT_OWNED


@pytest.mark.asyncio
async def test_measurement_capability_is_checked_before_the_metric(world: World) -> None:
    assert world.actuator is not None
    source = observation(component_id=world.actuator.component_id, metric_key="water.bogus")

    record = only_quarantined(
        await normalizer(world.repos).normalize(context(device_id=world.native_device), [source])
    )

    assert record.reason is QuarantineReason.COMPONENT_NOT_MEASURING


# Metric, unit and value ------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unknown_metric_is_quarantined(world: World) -> None:
    source = observation(metric_key="water.bogus", source_unit="nope")

    record = only_quarantined(await normalizer(world.repos).normalize(context(), [source]))

    assert record.reason is QuarantineReason.METRIC_UNKNOWN
    assert record.metric_key == "water.bogus"
    assert record.component_id == COMPONENT
    assert record.observed_at == OBSERVED
    assert record.received_at == RECEIVED
    assert record.source_event_id == "src-evt-1"


@pytest.mark.asyncio
async def test_unsupported_unit_is_quarantined_before_the_deployment(world: World) -> None:
    before_any_deployment = CREATED - timedelta(days=1)
    source = observation(source_unit="%sat", observed_at=before_any_deployment)

    record = only_quarantined(await normalizer(world.repos).normalize(context(), [source]))

    assert record.reason is QuarantineReason.UNIT_UNSUPPORTED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("metric_key", "value", "unit"),
    [
        ("device.rssi", -67.5, "dBm"),
        ("water.temperature", 2**53, "degC"),
    ],
    ids=["non-integral-rssi", "inexact-integer"],
)
async def test_invalid_value_is_quarantined_before_the_deployment(
    world: World, metric_key: str, value: float, unit: str
) -> None:
    before_any_deployment = CREATED - timedelta(days=1)
    source = observation(
        metric_key=metric_key, value=value, source_unit=unit, observed_at=before_any_deployment
    )

    record = only_quarantined(await normalizer(world.repos).normalize(context(), [source]))

    assert record.reason is QuarantineReason.VALUE_INVALID
    assert record.metric_key == metric_key


@pytest.mark.asyncio
async def test_unit_conversion_keeps_the_original_source_unit(world: World) -> None:
    source = observation(value=77.0, source_unit="degF")

    canonical = only_accepted(await normalizer(world.repos).normalize(context(), [source]))

    assert math.isclose(canonical.observation.value, 25.0, rel_tol=1e-12, abs_tol=1e-12)
    assert canonical.observation.source_unit == "degF"
    assert canonical.observation.canonical_unit == "degC"


@pytest.mark.asyncio
async def test_integer_metric_is_stamped_with_its_value_type(world: World) -> None:
    source = observation(metric_key="device.rssi", value=-67, source_unit="dBm")

    canonical = only_accepted(await normalizer(world.repos).normalize(context(), [source]))

    assert canonical.observation.value == -67.0
    assert canonical.observation.value_type.value == "integer"
    assert canonical.observation.canonical_unit == "dBm"


# Quality ---------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_out_of_range_value_is_accepted_with_provenance(world: World) -> None:
    source = observation(value=70.0)

    canonical = only_accepted(await normalizer(world.repos).normalize(context(), [source]))

    assert canonical.quality.state is QualityState.OUT_OF_RANGE
    assert canonical.quality.provenance == (
        QualityProvenance.SCHEMA_VALIDATION,
        QualityProvenance.UNIT_MAPPING,
        QualityProvenance.PLAUSIBLE_RANGE,
    )


@pytest.mark.asyncio
async def test_delay_is_measured_from_received_at_not_from_the_clock(world: World) -> None:
    on_time = observation(observed_at=RECEIVED - timedelta(seconds=300))
    late = observation(observed_at=RECEIVED - timedelta(seconds=301))

    result = await normalizer(world.repos).normalize(context(), [on_time, late])

    assert result.quarantined == ()
    first, second = result.accepted
    assert first.quality.flags == ()
    assert first.delivery.delayed is False
    assert second.quality.flags == (QualityFlag.DELAYED,)
    assert second.delivery.delayed is True
    assert QualityProvenance.TIMELINESS in second.quality.provenance


@pytest.mark.asyncio
async def test_replay_context_flags_the_observation(world: World) -> None:
    canonical = only_accepted(
        await normalizer(world.repos).normalize(context(replay=True), [observation()])
    )

    assert canonical.quality.flags == (QualityFlag.REPLAY,)
    assert canonical.delivery.replay is True
    assert canonical.quality.provenance[-1] is QualityProvenance.REPLAY
    assert canonical.source.received_at == RECEIVED


@pytest.mark.asyncio
async def test_clock_skew_flags_come_from_observed_and_received_at(world: World) -> None:
    future = observation(observed_at=RECEIVED + timedelta(seconds=301))
    ancient = observation(observed_at=RECEIVED - timedelta(days=31))

    result = await normalizer(world.repos).normalize(context(), [future, ancient])

    assert result.quarantined == ()
    skewed, late = result.accepted
    assert skewed.quality.flags == (QualityFlag.CLOCK_SKEW_FUTURE,)
    assert QualityProvenance.CLOCK_SKEW in skewed.quality.provenance
    assert late.quality.flags == (QualityFlag.CLOCK_SKEW_PAST_EXTREME, QualityFlag.DELAYED)
    assert late.delivery.delayed is True
    assert late.resolved.deployment_id == world.west_deployment.deployment_id
    counts = result.counts()
    assert counts["flag.clock_skew_future"] == 1
    assert counts["flag.clock_skew_past_extreme"] == 1
    assert counts["flag.delayed"] == 1


# Event-time Deployment (ADR-006), no fallback ---------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("observed_at", "side"),
    [
        (CREATED, "west"),
        (MOVE - timedelta(microseconds=1), "west"),
        (MOVE, "east"),
        (MOVE + timedelta(microseconds=1), "east"),
    ],
    ids=["west-start", "west-last-instant", "east-boundary", "east-after"],
)
async def test_deployment_is_resolved_at_event_time(
    world: World, observed_at: datetime, side: str
) -> None:
    spy = SpyRepositories(world.repos)
    received = observed_at + timedelta(seconds=1)

    canonical = only_accepted(
        await normalizer(spy).normalize(
            context(received_at=received), [observation(observed_at=observed_at)]
        )
    )

    deployment = world.west_deployment if side == "west" else world.east_deployment
    asset = world.west if side == "west" else world.east
    assert canonical.resolved.deployment_id == deployment.deployment_id
    assert canonical.resolved.asset_id == asset.asset.asset_id
    assert canonical.resolved.site_id == asset.asset.site_id
    assert ("get_deployment_at", (TENANT, COMPONENT, observed_at)) in spy.calls
    assert spy.forbidden == []


@pytest.mark.asyncio
async def test_replay_after_the_move_resolves_the_old_deployment_with_the_same_identity() -> None:
    repos = InMemoryM1Repositories(clock=lambda: CREATED)
    _, west, west_deployment = await _seed_legacy(repos, TENANT, "pond-west", DEVICE)
    _, east, _ = await _seed_legacy(repos, TENANT, "pond-east", SECOND_DEVICE)
    before_move = MOVE - timedelta(days=2)
    first_context = context(received_at=before_move + timedelta(seconds=2))
    source = observation(observed_at=before_move)

    first = only_accepted(await normalizer(repos).normalize(first_context, [source]))
    await _move_legacy(repos, west_deployment, east)
    replay_context = first_context.model_copy(update={"replay": True})
    replayed = only_accepted(await normalizer(repos).normalize(replay_context, [source]))
    live = only_accepted(
        await normalizer(repos).normalize(
            context(received_at=MOVE + timedelta(seconds=2)),
            [observation(observed_at=MOVE + timedelta(seconds=1))],
        )
    )

    assert first.resolved.deployment_id == west_deployment.deployment_id
    assert replayed.resolved.deployment_id == west_deployment.deployment_id
    assert replayed.resolved.asset_id == west.asset.asset_id
    assert replayed.event_id == first.event_id
    assert replayed.quality.flags == (QualityFlag.REPLAY,)
    assert live.resolved.asset_id == east.asset.asset_id


@pytest.mark.asyncio
async def test_no_deployment_at_event_time_is_unresolved_despite_a_current_one(
    world: World,
) -> None:
    spy = SpyRepositories(world.repos)
    before_any_deployment = CREATED - timedelta(microseconds=1)
    assert await world.repos.get_current_deployment(TENANT, COMPONENT) is not None

    record = only_quarantined(
        await normalizer(spy).normalize(context(), [observation(observed_at=before_any_deployment)])
    )

    assert record.reason is QuarantineReason.DEPLOYMENT_UNRESOLVED
    assert record.component_id == COMPONENT
    assert record.metric_key == "water.temperature"
    assert record.observed_at == before_any_deployment
    assert record.received_at == RECEIVED
    assert record.source_event_id == "src-evt-1"
    assert len(record.detail) <= 256
    assert spy.forbidden == []


@pytest.mark.asyncio
async def test_gap_between_deployments_is_unresolved() -> None:
    repos = InMemoryM1Repositories(clock=lambda: CREATED)
    _, _, west_deployment = await _seed_legacy(repos, TENANT, "pond-west", DEVICE)
    _, east, _ = await _seed_legacy(repos, TENANT, "pond-east", SECOND_DEVICE)
    await repos.end_deployment(
        TENANT, west_deployment.deployment_id, expected_version=1, ended_at=MOVE
    )
    resumed = MOVE + timedelta(days=1)
    await repos.start_deployment(
        Deployment(
            tenant_id=TENANT,
            deployment_id=new_deployment_id(),
            component_id=COMPONENT,
            site_id=east.asset.site_id,
            asset_id=east.asset.asset_id,
            started_at=resumed,
            created_at=resumed,
            updated_at=resumed,
            version=1,
        ),
        receipt(TENANT),
    )
    in_gap = MOVE + timedelta(hours=1)

    spy = SpyRepositories(repos)

    record = only_quarantined(
        await normalizer(spy).normalize(
            context(received_at=in_gap), [observation(observed_at=in_gap)]
        )
    )

    assert record.reason is QuarantineReason.DEPLOYMENT_UNRESOLVED
    assert spy.forbidden == []


# Identity --------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_two_probes_of_the_same_metric_get_distinct_identities(world: World) -> None:
    first, second = world.probes
    items = [
        observation(component_id=first.component_id),
        observation(component_id=second.component_id),
    ]

    result = await normalizer(world.repos).normalize(context(device_id=world.native_device), items)

    assert result.quarantined == ()
    a, b = result.accepted
    assert a.event_id != b.event_id
    assert a.event_id == observation_event_id(
        TENANT, first.component_id, "water.temperature", OBSERVED
    )
    assert b.event_id == observation_event_id(
        TENANT, second.component_id, "water.temperature", OBSERVED
    )
    assert first.probe_profile is not None and second.probe_profile is not None
    assert a.resolved.probe_id == first.probe_profile.probe_profile_id
    assert b.resolved.probe_id == second.probe_profile.probe_profile_id
    assert a.resolved.device_id == b.resolved.device_id == world.native_device


# Versions (ADR-005) ----------------------------------------------------------------------------

V1 = NormalizationVersions()
V2 = NormalizationVersions(
    catalog_version="metric-catalog@2",
    unit_mapping_version="unit-mapping@2",
    quality_policy_version="quality-policy@2",
)


def _catalogs_with_v2() -> Mapping[str, Any]:
    revised = tuple(
        d.model_copy(update={"plausible_max": 25.0}) if d.metric_key == "water.temperature" else d
        for d in METRIC_CATALOGS["metric-catalog@1"]
        if d.metric_key != "water.turbidity"
    )
    return MappingProxyType({**METRIC_CATALOGS, "metric-catalog@2": revised})


def _units_with_v2() -> Mapping[str, Any]:
    table = dict(UNIT_MAPPINGS["unit-mapping@1"])
    table["water.temperature"] = MappingProxyType({"degC": lambda value: float(value) + 0.5})
    return MappingProxyType({**UNIT_MAPPINGS, "unit-mapping@2": MappingProxyType(table)})


def _policies_with_v2() -> Mapping[str, QualityPolicy]:
    revised = QualityPolicy(
        delay_threshold=timedelta(seconds=10),
        future_skew_threshold=timedelta(seconds=300),
        past_extreme_skew_threshold=timedelta(days=30),
    )
    return MappingProxyType({**QUALITY_POLICIES, "quality-policy@2": revised})


def revised_normalizer(
    repos: Any, default_versions: NormalizationVersions = V2
) -> TelemetryNormalizer:
    return normalizer(
        repos,
        catalogs=_catalogs_with_v2(),
        unit_mappings=_units_with_v2(),
        policies=_policies_with_v2(),
        default_versions=default_versions,
    )


VERSIONED_OBSERVED = RECEIVED - timedelta(seconds=60)  # delayed under @2 (10 s), not under @1


@pytest.mark.asyncio
async def test_default_versions_stamp_the_revised_mapping(world: World) -> None:
    source = observation(observed_at=VERSIONED_OBSERVED)

    canonical = only_accepted(await revised_normalizer(world.repos).normalize(context(), [source]))

    assert canonical.mapping == V2
    assert math.isclose(canonical.observation.value, 25.6, rel_tol=1e-12)
    assert canonical.quality.state is QualityState.OUT_OF_RANGE
    assert canonical.quality.flags == (QualityFlag.DELAYED,)


@pytest.mark.asyncio
async def test_revised_catalog_is_pinned_for_metric_lookup(world: World) -> None:
    source = observation(metric_key="water.turbidity", value=3.1, source_unit="NTU")

    revised = await revised_normalizer(world.repos).normalize(context(), [source])
    pinned = await revised_normalizer(world.repos).normalize(
        context(replay=True, mapping_versions=V1), [source]
    )

    assert only_quarantined(revised).reason is QuarantineReason.METRIC_UNKNOWN
    assert only_accepted(pinned).mapping == V1


@pytest.mark.asyncio
async def test_replay_pinned_to_v1_reproduces_the_original_mapping(world: World) -> None:
    source = observation(observed_at=VERSIONED_OBSERVED)
    first_context = context()

    original = only_accepted(await normalizer(world.repos).normalize(first_context, [source]))
    revised = only_accepted(
        await revised_normalizer(world.repos).normalize(first_context, [source])
    )
    replay_context = first_context.model_copy(update={"replay": True, "mapping_versions": V1})
    replayed = only_accepted(
        await revised_normalizer(world.repos).normalize(replay_context, [source])
    )

    assert original.mapping == V1
    assert revised.mapping == V2
    assert replayed.mapping == V1
    assert replayed.observation.value == original.observation.value == 25.1
    assert replayed.quality.state is original.quality.state is QualityState.VALID
    assert original.quality.flags == ()
    assert replayed.quality.flags == (QualityFlag.REPLAY,)
    assert replayed.event_id == original.event_id == revised.event_id
    assert replayed.resolved == original.resolved
    assert replayed.source == original.source


@pytest.mark.asyncio
async def test_pinned_versions_override_the_defaults_without_replay(
    world: World,
) -> None:
    pinned = context(mapping_versions=V2)

    canonical = only_accepted(
        await revised_normalizer(world.repos, default_versions=V1).normalize(
            pinned, [observation(observed_at=VERSIONED_OBSERVED)]
        )
    )

    assert canonical.mapping == V2
    assert math.isclose(canonical.observation.value, 25.6, rel_tol=1e-12)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "versions",
    [
        NormalizationVersions(catalog_version="metric-catalog@9"),
        NormalizationVersions(unit_mapping_version="unit-mapping@9"),
        NormalizationVersions(quality_policy_version="quality-policy@9"),
    ],
    ids=["catalog", "unit-mapping", "quality-policy"],
)
async def test_unknown_pinned_version_raises(world: World, versions: NormalizationVersions) -> None:
    with pytest.raises(ValueError):
        await normalizer(world.repos).normalize(context(mapping_versions=versions), [observation()])


@pytest.mark.asyncio
async def test_unknown_default_version_raises(world: World) -> None:
    with pytest.raises(ValueError):
        subject = normalizer(
            world.repos, default_versions=NormalizationVersions(catalog_version="metric-catalog@9")
        )
        await subject.normalize(context(), [observation()])


# Batch bound, lookup cache and bounded quarantine ----------------------------------------------


@pytest.mark.asyncio
async def test_batch_above_the_bound_raises_without_lookups(world: World) -> None:
    spy = SpyRepositories(world.repos)
    items = [observation()] * (MAX_OBSERVATIONS_PER_BATCH + 1)

    with pytest.raises(ValueError) as raised:
        await normalizer(spy).normalize(context(), items)

    # A caller error, not a write failure: only TelemetryWriteError means "not durably written".
    assert not isinstance(raised.value, TelemetryWriteError)
    assert spy.calls == []


@pytest.mark.asyncio
async def test_batch_at_the_bound_is_normalized_with_cached_lookups(world: World) -> None:
    spy = SpyRepositories(world.repos)
    items = [
        observation(observed_at=OBSERVED - timedelta(microseconds=index % 2))
        for index in range(MAX_OBSERVATIONS_PER_BATCH)
    ]

    result = await normalizer(spy).normalize(context(), items)

    assert len(result.accepted) == MAX_OBSERVATIONS_PER_BATCH
    assert spy.count("get_component") == 1
    assert 1 <= spy.count("get_deployment_at") <= 2


@pytest.mark.asyncio
async def test_negative_lookups_are_cached_within_a_call(world: World) -> None:
    spy = SpyRepositories(world.repos)
    items = [observation(component_id=world.second_component)] * 5 + [
        observation(observed_at=CREATED - timedelta(days=1))
    ] * 5

    result = await normalizer(spy).normalize(context(), items)

    assert [q.reason for q in result.quarantined] == [QuarantineReason.COMPONENT_NOT_OWNED] * 5 + [
        QuarantineReason.DEPLOYMENT_UNRESOLVED
    ] * 5
    assert spy.count("get_component") == 2
    assert spy.count("get_deployment_at") == 1


@pytest.mark.asyncio
async def test_lookup_cache_does_not_outlive_a_call(world: World) -> None:
    spy = SpyRepositories(world.repos)
    subject = normalizer(spy)

    first = only_accepted(await subject.normalize(context(), [observation()]))
    await world.repos.end_deployment(
        TENANT,
        world.east_deployment.deployment_id,
        expected_version=world.east_deployment.version,
        ended_at=OBSERVED - timedelta(hours=1),
    )
    second = only_quarantined(await subject.normalize(context(), [observation()]))

    assert first.resolved.deployment_id == world.east_deployment.deployment_id
    assert second.reason is QuarantineReason.DEPLOYMENT_UNRESOLVED
    assert spy.count("get_component") == 2
    assert spy.count("get_deployment_at") == 2


@pytest.mark.asyncio
async def test_seven_metrics_of_one_reading_share_one_lookup_each(world: World) -> None:
    spy = SpyRepositories(world.repos)
    items = [
        observation(metric_key=key, value=value, source_unit=unit)
        for key, value, unit in [
            ("water.temperature", 25.1, "degC"),
            ("water.ph", 7.2, "pH"),
            ("water.dissolved_oxygen", 6.4, "mg/L"),
            ("water.turbidity", 3.1, "NTU"),
            ("water.salinity", 11.8, "ppt"),
            ("device.battery_voltage", 3.82, "V"),
            ("device.rssi", -67, "dBm"),
        ]
    ]

    result = await normalizer(spy).normalize(context(), items)

    assert len(result.accepted) == 7
    assert len({o.event_id for o in result.accepted}) == 7
    assert spy.count("get_component") == 1
    assert spy.count("get_deployment_at") == 1
    assert {o.resolved.deployment_id for o in result.accepted} == {
        world.east_deployment.deployment_id
    }


@pytest.mark.asyncio
async def test_quarantine_excerpt_is_bounded_json_of_the_source(world: World) -> None:
    worst = observation(
        metric_key="\U0001f4a5" * 64,
        source_unit="é" * 16,
        source_event_id="\U0001f4a5" * 128,
        source_metadata={"firmware_version": "\U0001f4a5" * 64},
    )
    small = observation(metric_key="water.bogus")

    result = await normalizer(world.repos).normalize(context(), [worst, small])

    big_record, small_record = result.quarantined
    for record in (big_record, small_record):
        assert record.reason is QuarantineReason.METRIC_UNKNOWN
        assert len(record.source_excerpt_json.encode("utf-8")) <= MAX_QUARANTINE_EXCERPT_BYTES
        assert len(record.detail) <= 256
        if not record.truncated:
            json.loads(record.source_excerpt_json)
    assert small_record.truncated is False
    assert "water.bogus" in small_record.source_excerpt_json
    assert "src-evt-1" in small_record.source_excerpt_json


@pytest.mark.asyncio
async def test_empty_batch_is_a_no_op(world: World) -> None:
    spy = SpyRepositories(world.repos)

    result = await normalizer(spy).normalize(context(), [])

    assert result.accepted == ()
    assert result.quarantined == ()
    assert spy.calls == []


# Regression (post-approval review) --------------------------------------------------------------


@pytest.mark.asyncio
async def test_quarantine_excerpt_survives_unencodable_metadata_and_huge_values(
    world: World,
) -> None:
    surrogate = observation(
        component_id=world.second_component, source_metadata={"firmware_version": "\ud800"}
    )
    huge = observation(value=10**5000)

    result = await normalizer(world.repos).normalize(context(), [surrogate, huge])

    assert [q.reason for q in result.quarantined] == [
        QuarantineReason.COMPONENT_NOT_OWNED,
        QuarantineReason.VALUE_INVALID,
    ]
    for record in result.quarantined:
        assert len(record.source_excerpt_json.encode("utf-8")) <= MAX_QUARANTINE_EXCERPT_BYTES
        json.loads(record.source_excerpt_json)


class CrossTenantComponents:
    """A misbehaving reader that ignores the tenant argument."""

    def __init__(self, component: DeviceComponent) -> None:
        self.component = component

    async def get_component(self, tenant_id: str, component_id: str) -> DeviceComponent | None:
        return self.component


@pytest.mark.asyncio
async def test_component_of_another_tenant_is_rejected_even_if_the_reader_returns_it(
    world: World,
) -> None:
    foreign = await world.repos.get_component(OTHER_TENANT, world.other_tenant_component)
    assert foreign is not None
    subject = TelemetryNormalizer(
        components=CrossTenantComponents(foreign.model_copy(update={"device_id": DEVICE})),
        deployments=world.repos,
        clock=lambda: INGESTED,
    )

    record = only_quarantined(
        await subject.normalize(context(), [observation(component_id=foreign.component_id)])
    )

    assert record.reason is QuarantineReason.COMPONENT_NOT_OWNED


class MismatchedDeployments:
    """A misbehaving reader returning a Deployment that does not belong to the request."""

    def __init__(self, deployment: Deployment) -> None:
        self.deployment = deployment

    async def get_deployment_at(
        self, tenant_id: str, component_id: str, observed_at: datetime
    ) -> Deployment | None:
        return self.deployment


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", ["tenant", "component"])
async def test_deployment_of_another_tenant_or_component_is_unresolved(
    world: World, mismatch: str
) -> None:
    if mismatch == "tenant":
        foreign = await world.repos.get_current_deployment(
            OTHER_TENANT, world.other_tenant_component
        )
        assert foreign is not None
        foreign = foreign.model_copy(update={"component_id": COMPONENT})
    else:
        foreign = world.east_deployment.model_copy(update={"component_id": world.second_component})
    subject = TelemetryNormalizer(
        components=world.repos, deployments=MismatchedDeployments(foreign), clock=lambda: INGESTED
    )

    record = only_quarantined(await subject.normalize(context(), [observation()]))

    assert record.reason is QuarantineReason.DEPLOYMENT_UNRESOLVED
