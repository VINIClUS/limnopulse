"""Trusted source observations -> canonical V4 observations (P2-06).

Per observation, in order: Component ownership and measurement capability, catalog metric,
unit conversion, then the Deployment at event time. The Deployment is resolved only with
`get_deployment_at` (ADR-006): there is no fallback to the current Deployment, so an
observation outside every interval is quarantined as `deployment_unresolved`.

The pinned versions are `context.mapping_versions` when set (a replay), otherwise
`default_versions`, so a replay after a catalog or unit revision reproduces the original
mapping (ADR-005). An unknown version or an over-limit batch is a caller error and raises
`ValueError`; neither is ever quarantined.
"""

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime

from limnopulse_api.domain.deployments import Deployment
from limnopulse_api.domain.devices_v2 import ComponentKind, DeviceComponent
from limnopulse_api.domain.legacy_telemetry_mapping import bounded_detail, bounded_excerpt
from limnopulse_api.domain.metric_catalog import (
    METRIC_CATALOGS,
    metric_catalog,
    metric_definition,
)
from limnopulse_api.domain.telemetry_quality import (
    QUALITY_POLICIES,
    QualityPolicy,
    classify_quality,
)
from limnopulse_api.domain.telemetry_units import (
    UNIT_MAPPINGS,
    UnitConversionError,
    UnitTable,
    convert_to_canonical,
)
from limnopulse_api.domain.telemetry_v2 import (
    MAX_OBSERVATIONS_PER_BATCH,
    CanonicalObservation,
    MetricDefinition,
    NormalizationResult,
    NormalizationVersions,
    ObservationDelivery,
    ObservationMeasurement,
    ObservationResolution,
    ObservationSource,
    QualityFlag,
    QuarantinedObservation,
    QuarantineReason,
    SourceObservation,
    TrustedSourceContext,
    observation_event_id,
)
from limnopulse_api.repositories.v2_contracts import ComponentReader, DeploymentRepository

DEFAULT_VERSIONS = NormalizationVersions()


class _Quarantine(Exception):
    def __init__(self, reason: QuarantineReason, detail: str) -> None:
        super().__init__(reason, detail)
        self.reason = reason
        self.detail = detail


class TelemetryNormalizer:
    def __init__(
        self,
        *,
        components: ComponentReader,
        deployments: DeploymentRepository,
        clock: Callable[[], datetime],
        catalogs: Mapping[str, tuple[MetricDefinition, ...]] = METRIC_CATALOGS,
        unit_mappings: Mapping[str, UnitTable] = UNIT_MAPPINGS,
        policies: Mapping[str, QualityPolicy] = QUALITY_POLICIES,
        default_versions: NormalizationVersions = DEFAULT_VERSIONS,
    ) -> None:
        self.components = components
        self.deployments = deployments
        self.clock = clock
        self.catalogs = catalogs
        self.unit_mappings = unit_mappings
        self.policies = policies
        self.default_versions = default_versions

    async def normalize(
        self, context: TrustedSourceContext, observations: Sequence[SourceObservation]
    ) -> NormalizationResult:
        if len(observations) > MAX_OBSERVATIONS_PER_BATCH:
            raise ValueError(f"telemetry batch exceeds {MAX_OBSERVATIONS_PER_BATCH} observations")
        versions = context.mapping_versions or self.default_versions
        self._require_known(versions)
        call = _NormalizationCall(self, context, versions)
        accepted: list[CanonicalObservation] = []
        quarantined: list[QuarantinedObservation] = []
        for source in observations:
            try:
                accepted.append(await call.normalize_one(source))
            except _Quarantine as exc:
                quarantined.append(_quarantine_record(context, source, exc))
        return NormalizationResult(accepted=tuple(accepted), quarantined=tuple(quarantined))

    def _require_known(self, versions: NormalizationVersions) -> None:
        metric_catalog(versions.catalog_version, catalogs=self.catalogs)
        if versions.unit_mapping_version not in self.unit_mappings:
            raise ValueError(f"unknown unit mapping version: {versions.unit_mapping_version}")
        if versions.quality_policy_version not in self.policies:
            raise ValueError(f"unknown quality policy version: {versions.quality_policy_version}")


class _NormalizationCall:
    """State of one `normalize` call: pinned versions and its lookup caches."""

    def __init__(
        self,
        normalizer: TelemetryNormalizer,
        context: TrustedSourceContext,
        versions: NormalizationVersions,
    ) -> None:
        self.normalizer = normalizer
        self.context = context
        self.versions = versions
        self.components: dict[str, DeviceComponent | None] = {}
        self.deployments: dict[tuple[str, datetime], Deployment | None] = {}
        self.ingested_at: datetime | None = None

    async def normalize_one(self, source: SourceObservation) -> CanonicalObservation:
        context, versions, normalizer = self.context, self.versions, self.normalizer
        component = await self._component(source.component_id)
        if component is None or component.device_id != context.device_id:
            raise _Quarantine(
                QuarantineReason.COMPONENT_NOT_OWNED,
                "component is not owned by the trusted tenant and device",
            )
        if component.kind is ComponentKind.ACTUATOR:
            raise _Quarantine(
                QuarantineReason.COMPONENT_NOT_MEASURING, "actuator components do not measure"
            )
        metric = metric_definition(
            source.metric_key, version=versions.catalog_version, catalogs=normalizer.catalogs
        )
        if metric is None:
            raise _Quarantine(
                QuarantineReason.METRIC_UNKNOWN,
                f"metric is not in {versions.catalog_version}",
            )
        try:
            value = convert_to_canonical(
                metric,
                source.value,
                source.source_unit,
                unit_mapping_version=versions.unit_mapping_version,
                unit_mappings=normalizer.unit_mappings,
            )
        except UnitConversionError as exc:
            raise _Quarantine(exc.reason, exc.detail) from exc
        deployment = await self._deployment_at(source.component_id, source.observed_at)
        if deployment is None:
            raise _Quarantine(
                QuarantineReason.DEPLOYMENT_UNRESOLVED,
                "no deployment contains observed_at",
            )
        quality = classify_quality(
            metric,
            value,
            observed_at=source.observed_at,
            received_at=context.received_at,
            replay=context.replay,
            quality_policy_version=versions.quality_policy_version,
            policies=normalizer.policies,
        )
        if self.ingested_at is None:
            self.ingested_at = normalizer.clock()
        probe = component.probe_profile
        return CanonicalObservation(
            event_id=observation_event_id(
                context.tenant_id, source.component_id, metric.metric_key, source.observed_at
            ),
            source_event_id=source.source_event_id,
            source=ObservationSource(
                integration_id=context.integration_id,
                integration_type=context.integration_type,
                source_type=context.source_type,
                connector_version=context.connector_version,
                received_at=context.received_at,
            ),
            resolved=ObservationResolution(
                tenant_id=context.tenant_id,
                site_id=deployment.site_id,
                asset_id=deployment.asset_id,
                deployment_id=deployment.deployment_id,
                device_id=context.device_id,
                component_id=source.component_id,
                probe_id=probe.probe_profile_id if probe is not None else None,
            ),
            observation=ObservationMeasurement(
                metric_key=metric.metric_key,
                value=value,
                value_type=metric.value_type,
                source_unit=source.source_unit,
                canonical_unit=metric.canonical_unit,
                observed_at=source.observed_at,
                sequence=source.sequence,
            ),
            delivery=ObservationDelivery(
                ingested_at=self.ingested_at,
                delayed=QualityFlag.DELAYED in quality.flags,
                replay=QualityFlag.REPLAY in quality.flags,
            ),
            quality=quality,
            mapping=versions,
            source_metadata=dict(source.source_metadata),
        )

    async def _component(self, component_id: str) -> DeviceComponent | None:
        if component_id not in self.components:
            self.components[component_id] = await self.normalizer.components.get_component(
                self.context.tenant_id, component_id
            )
        return self.components[component_id]

    async def _deployment_at(self, component_id: str, observed_at: datetime) -> Deployment | None:
        key = (component_id, observed_at)
        if key not in self.deployments:
            self.deployments[key] = await self.normalizer.deployments.get_deployment_at(
                self.context.tenant_id, component_id, observed_at
            )
        return self.deployments[key]


def _quarantine_record(
    context: TrustedSourceContext, source: SourceObservation, exc: _Quarantine
) -> QuarantinedObservation:
    excerpt, truncated = bounded_excerpt(source.model_dump(mode="json"))
    return QuarantinedObservation(
        reason=exc.reason,
        tenant_id=context.tenant_id,
        device_id=context.device_id,
        component_id=source.component_id,
        metric_key=source.metric_key,
        observed_at=source.observed_at,
        received_at=context.received_at,
        source_event_id=source.source_event_id,
        detail=bounded_detail(exc.detail),
        source_excerpt_json=excerpt,
        truncated=truncated,
    )
