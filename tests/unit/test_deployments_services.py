import json
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.assets import (
    Asset,
    AssetStatus,
    AssetType,
    AssetWithPondProfile,
    PondProfile,
    Site,
    SiteStatus,
)
from limnopulse_api.domain.deployments import Deployment, DeploymentStatus
from limnopulse_api.domain.devices_v2 import (
    ComponentKind,
    ComponentStatus,
    DeviceComponent,
    ProbeProfile,
)
from limnopulse_api.domain.ids import default_component_id_for_legacy_device
from limnopulse_api.services.deployments import (
    DEPLOYMENT_CREATE_SCOPE,
    DeploymentEndCommand,
    DeploymentInputError,
    DeploymentService,
    DeploymentStartCommand,
)

TENANT = "tnt_a"
OTHER_TENANT = "tnt_b"
DEVICE_ID = "dev_" + "0" * 32
COMPONENT_ID = "cmp_" + "1" * 32
DEPLOYMENT_ID = "dep_" + "2" * 32
NEXT_DEPLOYMENT_ID = "dep_" + "3" * 32
SITE_ID = "site_" + "4" * 32
WEST_ASSET_ID = "ast_" + "5" * 32
EAST_ASSET_ID = "ast_" + "6" * 32
CREATED = datetime(2026, 8, 1, tzinfo=UTC)
STARTED = datetime(2026, 8, 25, 10, tzinfo=UTC)
NOW = datetime(2026, 8, 25, 12, tzinfo=UTC)


def fixed_clock() -> datetime:
    return NOW


class FakeDeploymentRepository:
    """In-memory store with the current-pointer and previous-end rules of the real adapter."""

    def __init__(self, *deployments: Deployment) -> None:
        self.rows = {(d.tenant_id, d.deployment_id): d for d in deployments}
        self.receipts: dict = {}
        self.calls: list[str] = []
        self.reject_start: ConflictError | None = None

    async def get_idempotency_replay(self, tenant_id, scope, key_hash, request_hash):
        self.calls.append("get_idempotency_replay")
        receipt = self.receipts.get((tenant_id, scope, key_hash))
        if receipt is None:
            return None
        if receipt.request_hash != request_hash:
            raise ConflictError("idempotency key reused with a different request")
        return receipt.response_snapshot_json

    async def get_deployment(self, tenant_id, deployment_id):
        return self.rows.get((tenant_id, deployment_id))

    async def list_component_deployments(self, tenant_id, component_id):
        return sorted(
            (
                d
                for d in self.rows.values()
                if d.tenant_id == tenant_id and d.component_id == component_id
            ),
            key=lambda d: d.started_at,
        )

    async def get_current_deployment(self, tenant_id, component_id):
        self.calls.append("get_current_deployment")
        for d in self.rows.values():
            active = d.status is DeploymentStatus.ACTIVE
            if d.tenant_id == tenant_id and d.component_id == component_id and active:
                return d
        return None

    async def get_deployment_at(self, tenant_id, component_id, observed_at):
        for d in await self.list_component_deployments(tenant_id, component_id):
            if d.started_at <= observed_at and (d.ended_at is None or observed_at < d.ended_at):
                return d
        return None

    async def start_deployment(self, deployment, receipt):
        self.calls.append("start_deployment")
        if self.reject_start is not None:
            raise self.reject_start
        history = await self.list_component_deployments(
            deployment.tenant_id, deployment.component_id
        )
        if any(d.status is DeploymentStatus.ACTIVE for d in history):
            raise ConflictError("component already has an active deployment")
        if history and deployment.started_at < history[-1].ended_at:
            raise ConflictError("deployment overlaps the previous interval")
        self.receipts[(receipt.tenant_id, receipt.scope, receipt.key_hash)] = receipt
        self.rows[(deployment.tenant_id, deployment.deployment_id)] = deployment
        return deployment

    async def end_deployment(self, tenant_id, deployment_id, *, expected_version, ended_at):
        current = self.rows[(tenant_id, deployment_id)]
        ended = Deployment.model_validate(
            {
                **current.model_dump(),
                "ended_at": ended_at,
                "status": DeploymentStatus.ENDED,
                "updated_at": NOW,
                "version": expected_version + 1,
            }
        )
        self.rows[(tenant_id, deployment_id)] = ended
        return ended


class FakeComponentReader:
    def __init__(self, tenant_id: str = TENANT, **overrides) -> None:
        values = {
            "tenant_id": tenant_id,
            "component_id": COMPONENT_ID,
            "device_id": DEVICE_ID,
            "kind": ComponentKind.PROBE,
            "name": "Probe",
            "probe_profile": ProbeProfile(probe_profile_id="prb_" + "7" * 32),
            "created_at": CREATED,
            "updated_at": CREATED,
            "version": 1,
        }
        values.update(overrides)
        self.component = DeviceComponent(**values)

    async def get_component(self, tenant_id, component_id):
        if (tenant_id, component_id) != (self.component.tenant_id, self.component.component_id):
            return None
        return self.component


class FakeSiteAssetReader:
    def __init__(
        self,
        tenant_id: str = TENANT,
        *,
        asset_status: AssetStatus = AssetStatus.ACTIVE,
        site_status: SiteStatus = SiteStatus.ACTIVE,
    ) -> None:
        stamps = {"created_at": CREATED, "updated_at": CREATED, "version": 1}
        self.site = Site(
            tenant_id=tenant_id, site_id=SITE_ID, name="Farm", status=site_status, **stamps
        )
        self.assets = {
            asset_id: AssetWithPondProfile(
                asset=Asset(
                    tenant_id=tenant_id,
                    asset_id=asset_id,
                    site_id=SITE_ID,
                    asset_type=AssetType.POND,
                    name=asset_id,
                    status=asset_status,
                    **stamps,
                ),
                pond_profile=PondProfile(tenant_id=tenant_id, asset_id=asset_id, **stamps),
            )
            for asset_id in (WEST_ASSET_ID, EAST_ASSET_ID)
        }

    async def get_site(self, tenant_id, site_id):
        if (tenant_id, site_id) != (self.site.tenant_id, self.site.site_id):
            return None
        return self.site

    async def get_asset(self, tenant_id, asset_id):
        found = self.assets.get(asset_id)
        if found is None or found.asset.tenant_id != tenant_id:
            return None
        return found


def start_request(**overrides) -> DeploymentStartCommand:
    values = {"component_id": COMPONENT_ID, "asset_id": WEST_ASSET_ID, "started_at": STARTED}
    values.update(overrides)
    return DeploymentStartCommand(**values)


def service_for(
    deployments: FakeDeploymentRepository | None = None,
    components: FakeComponentReader | None = None,
    sites_assets: FakeSiteAssetReader | None = None,
    ids: list[str] | None = None,
) -> DeploymentService:
    pending = list(ids or [DEPLOYMENT_ID, NEXT_DEPLOYMENT_ID])
    return DeploymentService(
        deployments=deployments if deployments is not None else FakeDeploymentRepository(),
        components=components if components is not None else FakeComponentReader(),
        sites_assets=sites_assets if sites_assets is not None else FakeSiteAssetReader(),
        clock=fixed_clock,
        id_factory=lambda: pending.pop(0),
    )


# Command validation ----------------------------------------------------------------------------


def test_start_command_rejects_naive_site_and_malformed_ids() -> None:
    with pytest.raises(ValidationError):
        start_request(started_at=STARTED.replace(tzinfo=None))
    with pytest.raises(ValidationError):
        start_request(site_id=SITE_ID)
    with pytest.raises(ValidationError):
        start_request(component_id="cmp_1")
    with pytest.raises(ValidationError):
        start_request(asset_id="pond_1")
    with pytest.raises(ValidationError):
        DeploymentEndCommand(expected_version=0, ended_at=NOW)


def test_offset_timestamps_are_normalized_to_utc() -> None:
    plus_two = datetime.fromisoformat("2026-08-25T12:00:00+02:00")
    command = start_request(started_at=plus_two)
    assert command.started_at == STARTED
    assert command.started_at.utcoffset() == timedelta(0)


# Start -----------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_derives_site_from_the_tenant_owned_asset() -> None:
    repository = FakeDeploymentRepository()
    outcome = await service_for(repository).start(TENANT, start_request(), "deployment-create-01")

    deployment = outcome.entity
    assert not outcome.replayed
    assert (deployment.site_id, deployment.asset_id, deployment.component_id) == (
        SITE_ID,
        WEST_ASSET_ID,
        COMPONENT_ID,
    )
    assert deployment.status is DeploymentStatus.ACTIVE
    assert deployment.version == 1
    assert json.loads(outcome.snapshot_json)["started_at"] == "2026-08-25T10:00:00.000000Z"
    (receipt,) = repository.receipts.values()
    assert receipt.scope == DEPLOYMENT_CREATE_SCOPE == "deployment-create"


@pytest.mark.asyncio
async def test_start_rejects_component_or_asset_from_another_tenant() -> None:
    service = DeploymentService(
        deployments=FakeDeploymentRepository(),
        components=FakeComponentReader(tenant_id=OTHER_TENANT),
        sites_assets=FakeSiteAssetReader(tenant_id=TENANT),
        clock=fixed_clock,
    )
    with pytest.raises(NotFoundError):
        await service.start(TENANT, start_request(), "deployment-create-01")

    service = service_for(sites_assets=FakeSiteAssetReader(tenant_id=OTHER_TENANT))
    with pytest.raises(NotFoundError):
        await service.start(TENANT, start_request(), "deployment-create-01")


@pytest.mark.parametrize(
    ("components", "sites_assets", "message"),
    [
        (FakeComponentReader(status=ComponentStatus.REPLACED), None, "component is replaced"),
        (None, FakeSiteAssetReader(asset_status=AssetStatus.ARCHIVED), "asset is archived"),
        (None, FakeSiteAssetReader(site_status=SiteStatus.ARCHIVED), "site is not active"),
        (
            FakeComponentReader(
                component_id=default_component_id_for_legacy_device(TENANT, DEVICE_ID)
            ),
            None,
            "managed through /v1",
        ),
    ],
)
@pytest.mark.asyncio
async def test_start_requires_active_parents_and_a_non_legacy_component(
    components, sites_assets, message
) -> None:
    component_id = (components or FakeComponentReader()).component.component_id
    service = service_for(components=components, sites_assets=sites_assets)
    with pytest.raises(ConflictError, match=message):
        await service.start(
            TENANT, start_request(component_id=component_id), "deployment-create-01"
        )


@pytest.mark.parametrize(
    "started_at",
    [NOW + timedelta(microseconds=1), NOW + timedelta(days=1)],
)
@pytest.mark.asyncio
async def test_start_rejects_future_instants(started_at) -> None:
    with pytest.raises(DeploymentInputError):
        await service_for().start(
            TENANT, start_request(started_at=started_at), "deployment-create-01"
        )


@pytest.mark.asyncio
async def test_start_rejects_a_second_active_deployment() -> None:
    repository = FakeDeploymentRepository()
    service = service_for(repository)
    await service.start(TENANT, start_request(), "deployment-create-01")
    with pytest.raises(ConflictError, match="active deployment"):
        await service.start(TENANT, start_request(asset_id=EAST_ASSET_ID), "deployment-create-02")


@pytest.mark.asyncio
async def test_replay_is_checked_before_parents_and_the_current_pointer() -> None:
    repository = FakeDeploymentRepository()
    components = FakeComponentReader()
    service = service_for(repository, components=components)
    first = await service.start(TENANT, start_request(), "deployment-create-01")

    # State advances: the deployment ends and the Component is retired.
    await service.end(TENANT, DEPLOYMENT_ID, DeploymentEndCommand(expected_version=1, ended_at=NOW))
    components.component = components.component.model_copy(
        update={"status": ComponentStatus.DECOMMISSIONED}
    )
    repository.calls.clear()

    replay = await service.start(TENANT, start_request(), "deployment-create-01")

    assert replay.replayed
    assert replay.snapshot_json == first.snapshot_json
    assert repository.calls == ["get_idempotency_replay"]


@pytest.mark.asyncio
async def test_same_key_with_a_different_body_conflicts() -> None:
    service = service_for()
    await service.start(TENANT, start_request(), "deployment-create-01")
    with pytest.raises(ConflictError, match="idempotency"):
        await service.start(TENANT, start_request(asset_id=EAST_ASSET_ID), "deployment-create-01")


@pytest.mark.asyncio
async def test_concurrent_same_key_retry_replays_after_the_winner_commits() -> None:
    repository = FakeDeploymentRepository()
    service = service_for(repository)
    first = await service.start(TENANT, start_request(), "deployment-create-01")
    # Simulate a concurrent same-key request that passed the replay check before the winner.
    stored = dict(repository.receipts)
    repository.receipts.clear()
    original_replay = repository.get_idempotency_replay
    calls = 0

    async def racing_replay(*args):
        nonlocal calls
        calls += 1
        if calls == 1:
            return None
        repository.receipts.update(stored)
        return await original_replay(*args)

    repository.get_idempotency_replay = racing_replay
    repository.reject_start = ConflictError("idempotency key reused with a different request")

    replay = await service.start(TENANT, start_request(), "deployment-create-01")
    assert replay.replayed
    assert replay.snapshot_json == first.snapshot_json


# End and relocation ----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_move_west_to_east_preserves_event_time_resolution() -> None:
    repository = FakeDeploymentRepository()
    service = service_for(repository)
    move_at = STARTED + timedelta(hours=1)
    await service.start(TENANT, start_request(), "deployment-create-01")
    await service.end(
        TENANT, DEPLOYMENT_ID, DeploymentEndCommand(expected_version=1, ended_at=move_at)
    )
    await service.start(
        TENANT,
        start_request(asset_id=EAST_ASSET_ID, started_at=move_at),
        "deployment-create-02",
    )

    before = await service.get_at(TENANT, COMPONENT_ID, move_at - timedelta(minutes=30))
    after = await service.get_at(TENANT, COMPONENT_ID, move_at + timedelta(minutes=30))
    assert before.asset_id == WEST_ASSET_ID
    assert after.asset_id == EAST_ASSET_ID
    assert (await service.get_current(TENANT, COMPONENT_ID)).deployment_id == NEXT_DEPLOYMENT_ID
    assert [d.asset_id for d in await service.list(TENANT, COMPONENT_ID)] == [
        WEST_ASSET_ID,
        EAST_ASSET_ID,
    ]


@pytest.mark.asyncio
async def test_end_validates_interval_status_version_and_ownership() -> None:
    repository = FakeDeploymentRepository()
    service = service_for(repository)
    await service.start(TENANT, start_request(), "deployment-create-01")

    with pytest.raises(DeploymentInputError):
        await service.end(
            TENANT, DEPLOYMENT_ID, DeploymentEndCommand(expected_version=1, ended_at=STARTED)
        )
    with pytest.raises(DeploymentInputError):
        await service.end(
            TENANT,
            DEPLOYMENT_ID,
            DeploymentEndCommand(expected_version=1, ended_at=NOW + timedelta(seconds=1)),
        )
    with pytest.raises(ConflictError, match="version"):
        await service.end(
            TENANT, DEPLOYMENT_ID, DeploymentEndCommand(expected_version=2, ended_at=NOW)
        )
    with pytest.raises(NotFoundError):
        await service.end(
            OTHER_TENANT, DEPLOYMENT_ID, DeploymentEndCommand(expected_version=1, ended_at=NOW)
        )
    with pytest.raises(NotFoundError):
        await service.end(
            TENANT, "not-a-deployment", DeploymentEndCommand(expected_version=1, ended_at=NOW)
        )

    await service.end(TENANT, DEPLOYMENT_ID, DeploymentEndCommand(expected_version=1, ended_at=NOW))
    with pytest.raises(ConflictError, match="not active"):
        await service.end(
            TENANT, DEPLOYMENT_ID, DeploymentEndCommand(expected_version=2, ended_at=NOW)
        )


@pytest.mark.asyncio
async def test_end_refuses_legacy_default_component_deployments() -> None:
    legacy_component_id = default_component_id_for_legacy_device(TENANT, DEVICE_ID)
    legacy = Deployment(
        tenant_id=TENANT,
        deployment_id=DEPLOYMENT_ID,
        component_id=legacy_component_id,
        site_id=SITE_ID,
        asset_id=WEST_ASSET_ID,
        started_at=STARTED,
        created_at=STARTED,
        updated_at=STARTED,
        version=1,
    )
    service = service_for(
        FakeDeploymentRepository(legacy),
        components=FakeComponentReader(component_id=legacy_component_id),
    )
    with pytest.raises(ConflictError, match="managed through /v1"):
        await service.end(
            TENANT, DEPLOYMENT_ID, DeploymentEndCommand(expected_version=1, ended_at=NOW)
        )


@pytest.mark.asyncio
async def test_list_requires_a_tenant_owned_component() -> None:
    with pytest.raises(NotFoundError):
        await service_for().list(OTHER_TENANT, COMPONENT_ID)
    with pytest.raises(NotFoundError):
        await service_for().list(TENANT, "cmp_" + "f" * 32)
