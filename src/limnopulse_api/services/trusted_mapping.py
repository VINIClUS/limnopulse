"""Trusted mapping (P3 D4/D8): who a request or a queued envelope really belongs to.

`admit` resolves the authenticated account's identity with `account_scoped_identity_hash`, reads
the DeviceIntegration, the IntegrationAccount and the Device strongly consistent, and applies the
frozen condition -> enum table (`classify_admission`). Then it checks every component selector
with bounded `get_component` reads (one per distinct id, plus the legacy default when an id is
omitted) and fills the default in, so payload ownership never overrides the mapping.

`fence` re-resolves an envelope at consumption, re-reads the IntegrationAccount for the
envelope's tenant and account, and applies `classify_fence`: a terminal `IngressFence`, a
retryable `IngressDefer`, or a `TrustedIngress` whose context comes from the re-resolved row.

Store failures propagate unchanged (503 at admission, a retry in the worker); a failed read is
never turned into a denial or a fence.
"""

from typing import Protocol

from limnopulse_api.adapters.dynamodb_v2 import account_scoped_identity_hash
from limnopulse_api.domain.devices_v2 import ComponentStatus, DeviceV2
from limnopulse_api.domain.ids import default_component_id_for_legacy_device
from limnopulse_api.domain.ingestion import (
    AuthenticatedSource,
    IngestObservationBatch,
    IngressAdmission,
    IngressDefer,
    IngressDenial,
    IngressEnvelope,
    IngressFence,
    TrustedIngress,
    build_trusted_context,
    classify_admission,
    classify_fence,
    resolve_component_selectors,
)
from limnopulse_api.domain.integrations import DeviceIntegration, IntegrationAccount
from limnopulse_api.domain.telemetry_v2 import SourceObservation
from limnopulse_api.repositories.ingestion import ConsistentDeviceReader, ExternalIdentityResolver
from limnopulse_api.repositories.v2_contracts import ComponentReader


class IntegrationAccountReader(Protocol):
    """The one `IntegrationRepository` read the mapping needs (consistent in the adapter)."""

    async def get_integration_account(
        self, tenant_id: str, integration_account_id: str
    ) -> IntegrationAccount | None: ...


class TrustedMappingService:
    def __init__(
        self,
        *,
        resolver: ExternalIdentityResolver,
        integrations: IntegrationAccountReader,
        devices: ConsistentDeviceReader,
        components: ComponentReader,
    ) -> None:
        self.resolver = resolver
        self.integrations = integrations
        self.devices = devices
        self.components = components

    async def admit(
        self, source: AuthenticatedSource, batch: IngestObservationBatch
    ) -> IngressAdmission | IngressDenial:
        identity_hash = account_scoped_identity_hash(
            source.provider_key, source.integration_account_id, batch.external_device_id
        )
        integration = await self.resolver.resolve_external_identity(
            source.provider_key, identity_hash
        )
        if integration is None or not _owned_by(integration, source):
            # Never read another tenant's rows for a mapping that is not the caller's.
            return IngressDenial(reason=classify_admission(source, integration, None, None))
        account = await self.integrations.get_integration_account(
            source.tenant_id, source.integration_account_id
        )
        device = await self.devices.get_device(
            integration.tenant_id, integration.device_id, consistent=True
        )
        denied = classify_admission(source, integration, account, device)
        if denied is not None:
            return IngressDenial(reason=denied)
        return await self._resolve_components(source, batch, integration, device)

    async def fence(
        self, envelope: IngressEnvelope
    ) -> TrustedIngress | IngressFence | IngressDefer:
        integration = await self.resolver.resolve_external_identity(
            envelope.provider_key, envelope.external_identity_hash
        )
        # `account_verified` means an authoritative row exists for the ENVELOPE's tenant and
        # account (D8); when the DI tenant differs the fence is a mismatch anyway.
        account = await self.integrations.get_integration_account(
            envelope.tenant_id, envelope.integration_account_id
        )
        device = None
        if integration is not None and _matches_envelope(integration, envelope):
            device = await self.devices.get_device(
                integration.tenant_id, integration.device_id, consistent=True
            )
        result = classify_fence(envelope, integration, account, device)
        if result is not None:
            return result
        return TrustedIngress(
            context=build_trusted_context(envelope, integration),
            observations=envelope.observations,
        )

    async def _resolve_components(
        self,
        source: AuthenticatedSource,
        batch: IngestObservationBatch,
        integration: DeviceIntegration,
        device: DeviceV2,
    ) -> IngressAdmission | IngressDenial:
        default_component_id = default_component_id_for_legacy_device(
            integration.tenant_id, integration.device_id
        )
        wanted = {item.component_id for item in batch.observations} - {None}
        if any(item.component_id is None for item in batch.observations):
            wanted.add(default_component_id)
        active: set[str] = set()
        for component_id in sorted(wanted):
            component = await self.components.get_component(integration.tenant_id, component_id)
            if (
                component is not None
                and component.tenant_id == integration.tenant_id
                and component.device_id == integration.device_id
                and component.status == ComponentStatus.ACTIVE
            ):
                active.add(component_id)
        resolved = resolve_component_selectors(
            batch.observations,
            default_component_id=default_component_id,
            active_component_ids=active,
        )
        if isinstance(resolved, IngressDenial):
            return resolved
        observations = tuple(
            SourceObservation.model_validate(
                {**item.model_dump(mode="json"), "component_id": component_id}
            )
            for item, component_id in zip(batch.observations, resolved, strict=True)
        )
        return IngressAdmission(
            source=source,
            device_integration=integration,
            device=device,
            observations=observations,
        )


def _owned_by(integration: DeviceIntegration, source: AuthenticatedSource) -> bool:
    return (
        integration.tenant_id,
        integration.integration_account_id,
        integration.provider_key,
    ) == (source.tenant_id, source.integration_account_id, source.provider_key)


def _matches_envelope(integration: DeviceIntegration, envelope: IngressEnvelope) -> bool:
    return (
        integration.tenant_id,
        integration.integration_account_id,
        integration.provider_key,
        integration.device_integration_id,
        integration.device_id,
    ) == (
        envelope.tenant_id,
        envelope.integration_account_id,
        envelope.provider_key,
        envelope.device_integration_id,
        envelope.device_id,
    )
