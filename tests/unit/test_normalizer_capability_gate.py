"""P3-04 (P2 D14, D4): the normalizer's measurement capability gate.

A probe or hybrid Component whose `probe_profile.measurement_metric_keys` is non-empty measures
only those metrics; any other metric is quarantined as `component_not_measuring`. An empty set
(legacy defaults) is not gated. The P2 normalizer tests stay unedited.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from limnopulse_api.domain.deployments import Deployment, DeploymentStatus
from limnopulse_api.domain.devices_v2 import (
    ActuatorProfile,
    ComponentKind,
    DeviceComponent,
    ProbeProfile,
)
from limnopulse_api.domain.telemetry_v2 import (
    QuarantineReason,
    SourceObservation,
    SourceType,
    TrustedSourceContext,
)
from limnopulse_api.services.telemetry_normalizer import TelemetryNormalizer

TENANT = "tenant-a"
DEVICE = "dev-1"
CREATED = datetime(2026, 1, 1, tzinfo=UTC)
OBSERVED = datetime(2026, 10, 30, 11, 59, tzinfo=UTC)
RECEIVED = OBSERVED + timedelta(seconds=5)
GATED = "cmp_" + "1" * 32
UNGATED = "cmp_" + "2" * 32
HYBRID = "cmp_" + "3" * 32
GATED_HYBRID = "cmp_" + "4" * 32


def probe(component_id: str, keys: tuple[str, ...], *, kind: ComponentKind) -> DeviceComponent:
    return DeviceComponent(
        tenant_id=TENANT,
        component_id=component_id,
        device_id=DEVICE,
        kind=kind,
        name=component_id,
        probe_profile=ProbeProfile(
            probe_profile_id="prb_" + component_id[-32:], measurement_metric_keys=keys
        ),
        actuator_profile=(
            ActuatorProfile(actuator_profile_id="act_" + component_id[-32:], actuator_type="pump")
            if kind is ComponentKind.HYBRID
            else None
        ),
        created_at=CREATED,
        updated_at=CREATED,
        version=1,
    )


COMPONENTS = {
    GATED: probe(GATED, ("water.temperature", "water.ph"), kind=ComponentKind.PROBE),
    UNGATED: probe(UNGATED, (), kind=ComponentKind.PROBE),
    HYBRID: probe(HYBRID, (), kind=ComponentKind.HYBRID),
    GATED_HYBRID: probe(GATED_HYBRID, ("water.dissolved_oxygen",), kind=ComponentKind.HYBRID),
}


class Components:
    async def get_component(self, tenant_id: str, component_id: str) -> DeviceComponent | None:
        component = COMPONENTS.get(component_id)
        return component if component is not None and component.tenant_id == tenant_id else None


class Deployments:
    async def get_deployment_at(
        self, tenant_id: str, component_id: str, observed_at: datetime
    ) -> Deployment | None:
        return Deployment(
            tenant_id=tenant_id,
            deployment_id="dep_" + component_id[-32:],
            component_id=component_id,
            site_id="site_" + "a" * 32,
            asset_id="ast_" + "b" * 32,
            started_at=CREATED,
            status=DeploymentStatus.ACTIVE,
            created_at=CREATED,
            updated_at=CREATED,
            version=1,
        )


def normalizer() -> TelemetryNormalizer:
    return TelemetryNormalizer(
        components=Components(), deployments=Deployments(), clock=lambda: RECEIVED
    )


CONTEXT = TrustedSourceContext(
    tenant_id=TENANT,
    device_id=DEVICE,
    integration_id="di_" + "5" * 32,
    integration_type="https_direct",
    source_type=SourceType.HTTPS,
    connector_version=None,
    received_at=RECEIVED,
)

READINGS: dict[str, dict[str, Any]] = {
    "water.temperature": {"value": 21.5, "source_unit": "degC"},
    "water.ph": {"value": 7.1, "source_unit": "pH"},
    "water.dissolved_oxygen": {"value": 6.4, "source_unit": "mg/L"},
    "water.turbidity": {"value": 12.0, "source_unit": "NTU"},
}


def observation(component_id: str, metric_key: str, **overrides: Any) -> SourceObservation:
    payload: dict[str, Any] = {
        "component_id": component_id,
        "metric_key": metric_key,
        "observed_at": OBSERVED,
        **READINGS.get(metric_key, {"value": 1.0, "source_unit": "degC"}),
    }
    payload.update(overrides)
    return SourceObservation.model_validate(payload)


@pytest.mark.asyncio
async def test_a_metric_outside_a_declared_set_is_not_measuring() -> None:
    result = await normalizer().normalize(
        CONTEXT,
        [
            observation(GATED, "water.temperature"),
            observation(GATED, "water.dissolved_oxygen"),
            observation(GATED, "water.ph"),
        ],
    )

    assert [item.observation.metric_key for item in result.accepted] == [
        "water.temperature",
        "water.ph",
    ]
    (record,) = result.quarantined
    assert record.reason is QuarantineReason.COMPONENT_NOT_MEASURING
    assert (record.component_id, record.metric_key) == (GATED, "water.dissolved_oxygen")
    assert record.tenant_id == TENANT


@pytest.mark.asyncio
async def test_a_declared_hybrid_is_gated_too() -> None:
    result = await normalizer().normalize(
        CONTEXT,
        [
            observation(GATED_HYBRID, "water.dissolved_oxygen"),
            observation(GATED_HYBRID, "water.ph"),
        ],
    )

    assert [item.observation.metric_key for item in result.accepted] == ["water.dissolved_oxygen"]
    (record,) = result.quarantined
    assert record.reason is QuarantineReason.COMPONENT_NOT_MEASURING
    assert record.metric_key == "water.ph"


@pytest.mark.asyncio
@pytest.mark.parametrize("component_id", [UNGATED, HYBRID], ids=["probe", "hybrid"])
async def test_an_empty_set_is_not_gated(component_id: str) -> None:
    result = await normalizer().normalize(
        CONTEXT, [observation(component_id, metric) for metric in READINGS]
    )

    assert result.quarantined == ()
    assert [item.observation.metric_key for item in result.accepted] == list(READINGS)


@pytest.mark.asyncio
async def test_the_gate_runs_before_the_catalog_check() -> None:
    # Component capability is checked before the metric catalog (normalizer order).
    result = await normalizer().normalize(CONTEXT, [observation(GATED, "water.bogus")])

    (record,) = result.quarantined
    assert record.reason is QuarantineReason.COMPONENT_NOT_MEASURING


@pytest.mark.asyncio
async def test_a_catalog_unknown_metric_on_an_ungated_probe_stays_metric_unknown() -> None:
    result = await normalizer().normalize(CONTEXT, [observation(UNGATED, "water.bogus")])

    (record,) = result.quarantined
    assert record.reason is QuarantineReason.METRIC_UNKNOWN


@pytest.mark.asyncio
async def test_ownership_still_wins_over_the_gate() -> None:
    foreign = TrustedSourceContext.model_validate({**CONTEXT.model_dump(), "device_id": "dev-2"})

    result = await normalizer().normalize(foreign, [observation(GATED, "water.dissolved_oxygen")])

    (record,) = result.quarantined
    assert record.reason is QuarantineReason.COMPONENT_NOT_OWNED


@pytest.mark.asyncio
async def test_the_gate_matches_metric_keys_exactly() -> None:
    result = await normalizer().normalize(
        CONTEXT, [observation(GATED, "water.ph"), observation(GATED, "water.pH", value=7.0)]
    )

    assert [item.observation.metric_key for item in result.accepted] == ["water.ph"]
    assert [item.reason for item in result.quarantined] == [
        QuarantineReason.COMPONENT_NOT_MEASURING
    ]
