"""P3 ingestion ports. The M1 protocols in `v2_contracts.py` stay frozen; adapters may add
methods beyond these. Every contract kit in `tests/support/ingestion_contract_kits.py` runs
against the fakes and, in the owning task, against the real adapter."""

from collections.abc import Sequence
from datetime import datetime
from typing import Protocol, runtime_checkable

from limnopulse_api.domain.alerts import AuditContext
from limnopulse_api.domain.devices_v2 import DeviceV2
from limnopulse_api.domain.ingestion import (
    AuthenticatedSource,
    ComponentIngestState,
    ConnectorHealth,
    IngestCredential,
    IngestMarks,
    IngestObservationBatch,
    IngressAdmission,
    IngressDefer,
    IngressDenial,
    IngressEnvelope,
    IngressFence,
    IngressQueueMessage,
    IngressReceipt,
    QuarantineEntry,
    QuarantineEntryStatus,
    QuarantinePage,
    TrustedIngress,
)
from limnopulse_api.domain.integrations import (
    DeviceIntegration,
    DeviceIntegrationStatus,
    IntegrationAccount,
    IntegrationAccountStatus,
)

__all__ = [
    "ConnectorAuthenticator",
    "ConsistentDeviceReader",
    "ExternalIdentityResolver",
    "IngestCredentialRepository",
    "IngestMarks",
    "IngestStateRepository",
    "IngressAdmission",
    "IngressDefer",
    "IngressDenial",
    "IngressEntitlementCheck",
    "IngressFence",
    "IngressQueueConsumer",
    "IngressQueueMessage",
    "IngressQueueProducer",
    "IngressRateLimiter",
    "IngressReceiptRepository",
    "IntegrationLifecycleRepository",
    "QuarantinePage",
    "QuarantineRepository",
    "TelemetryMetrics",
    "TrustedIngress",
    "TrustedMapping",
]


@runtime_checkable
class IngestCredentialRepository(Protocol):
    async def get_ingest_credential(self, ingest_credential_id: str) -> IngestCredential | None: ...

    async def list_ingest_credentials(
        self, tenant_id: str, integration_account_id: str
    ) -> list[IngestCredential]: ...

    async def create_ingest_credential(
        self, credential: IngestCredential, audit: AuditContext
    ) -> IngestCredential:
        """`ConflictError` when the slot is taken."""
        ...

    async def revoke_ingest_credential(
        self,
        tenant_id: str,
        integration_account_id: str,
        ingest_credential_id: str,
        *,
        expected_version: int,
        revoked_at: datetime,
        audit: AuditContext,
    ) -> IngestCredential: ...


@runtime_checkable
class ConnectorAuthenticator(Protocol):
    async def authenticate(self, token: str) -> AuthenticatedSource | None:
        """`None` means 401; a store failure raises (503)."""
        ...


@runtime_checkable
class ExternalIdentityResolver(Protocol):
    async def resolve_external_identity(
        self, provider_key: str, external_identity_hash: str
    ) -> DeviceIntegration | None: ...


@runtime_checkable
class IntegrationLifecycleRepository(Protocol):
    async def transition_integration_account(
        self,
        tenant_id: str,
        integration_account_id: str,
        target: IntegrationAccountStatus,
        *,
        expected_version: int,
        updated_at: datetime,
        audit: AuditContext,
    ) -> IntegrationAccount: ...

    async def transition_device_integration(
        self,
        tenant_id: str,
        device_integration_id: str,
        target: DeviceIntegrationStatus,
        *,
        expected_version: int,
        updated_at: datetime,
        audit: AuditContext,
        device_id: str | None = None,
    ) -> DeviceIntegration:
        """A same-status target is a no-op (no write, no audit) behind the version fence. With
        `device_id`, a DeviceIntegration of another Device is 404, decided on the consistent
        pre-read before the version check."""
        ...


@runtime_checkable
class ConsistentDeviceReader(Protocol):
    """P3 port; the M1 `DeviceReader` stays frozen."""

    async def get_device(
        self, tenant_id: str, device_id: str, *, consistent: bool
    ) -> DeviceV2 | None: ...


@runtime_checkable
class TrustedMapping(Protocol):
    async def admit(
        self, source: AuthenticatedSource, batch: IngestObservationBatch
    ) -> IngressAdmission | IngressDenial: ...

    async def fence(
        self, envelope: IngressEnvelope
    ) -> TrustedIngress | IngressFence | IngressDefer: ...


@runtime_checkable
class IngressReceiptRepository(Protocol):
    async def get_ingress_receipt(
        self, integration_account_id: str, key_sha256: str
    ) -> IngressReceipt | None: ...

    async def claim_ingress_receipt(
        self, receipt: IngressReceipt, *, now: datetime
    ) -> IngressReceipt:
        """Returns the winner: the new receipt, or the live one that was already there."""
        ...

    async def mark_ingress_receipt_enqueued(
        self, integration_account_id: str, key_sha256: str, ingress_id: str, *, at: datetime
    ) -> None: ...


@runtime_checkable
class IngressQueueProducer(Protocol):
    async def send(self, envelope: IngressEnvelope) -> str:
        """Message id; `IngressMessageTooLarge` or `IngressQueueError`."""
        ...

    async def send_raw(self, body: str) -> str:
        """Redrive only."""
        ...


@runtime_checkable
class IngressQueueConsumer(Protocol):
    async def receive(
        self, *, max_messages: int, wait_seconds: int, visibility_seconds: int
    ) -> list[IngressQueueMessage]: ...

    async def delete(self, message: IngressQueueMessage) -> None: ...

    async def change_visibility(self, message: IngressQueueMessage, seconds: int) -> None: ...


@runtime_checkable
class QuarantineRepository(Protocol):
    async def put_quarantine_entries(self, entries: Sequence[QuarantineEntry]) -> None:
        """Idempotent create: an existing row is never reset."""
        ...

    async def list_quarantine_entries(
        self,
        tenant_id: str,
        *,
        status: QuarantineEntryStatus | None = None,
        reason: str | None = None,
        after_sk: str | None,
        limit: int,
    ) -> QuarantinePage: ...

    async def get_quarantine_entry(
        self, tenant_id: str, entry_key: str
    ) -> QuarantineEntry | None: ...

    async def transition_quarantine_entry(
        self,
        tenant_id: str,
        entry_key: str,
        target: QuarantineEntryStatus,
        *,
        expected_version: int,
        at: datetime,
        replayed_ingress_id: str | None = None,
    ) -> QuarantineEntry: ...


@runtime_checkable
class IngestStateRepository(Protocol):
    async def get_component_states(
        self, tenant_id: str, component_ids: Sequence[str]
    ) -> dict[str, ComponentIngestState]:
        """Rows of other tenants are ignored."""
        ...

    async def apply_ingest_marks(self, marks: IngestMarks) -> None:
        """Monotone and idempotent."""
        ...

    async def get_connector_health(
        self, tenant_id: str, integration_account_id: str
    ) -> ConnectorHealth | None: ...


@runtime_checkable
class IngressRateLimiter(Protocol):
    def try_acquire(self, key: str, *, now: float) -> float | None:
        """`None` when allowed, else the retry-after seconds."""
        ...


@runtime_checkable
class IngressEntitlementCheck(Protocol):
    async def allows(
        self, tenant_id: str, integration_account_id: str, observation_count: int
    ) -> bool: ...


@runtime_checkable
class TelemetryMetrics(Protocol):
    def increment(self, name: str, value: int = 1, **labels: str) -> None: ...

    def observe(self, name: str, value: float, **labels: str) -> None: ...
