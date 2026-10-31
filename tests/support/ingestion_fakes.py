"""P3-00 in-memory fakes of every ingestion port, plus the kit subjects that seed them.

Every fake reads and writes one shared `IngestionStore`. `fail_next(operation, exc)` arms a
one-shot failure at or below the port boundary: `operation` is a port method name, or one of
`ingestion_contract_kits.STORE_READS` for the reads a multi-read port performs. Subject helpers
(`audit_items`, `device_state`, `set_now`) act on the store itself, never on a side log.

These fakes are frozen with the P3-00 contract kits; waves 1-4 use them without editing them.
"""

import hmac
import time
from collections import OrderedDict, defaultdict
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from limnopulse_api.adapters.dynamodb_v2 import account_scoped_identity_hash
from limnopulse_api.core.errors import (
    ConflictError,
    IngressMessageTooLarge,
    IngressQueueError,
    NotFoundError,
)
from limnopulse_api.domain.alerts import AuditContext
from limnopulse_api.domain.devices_v2 import ComponentStatus, DeviceComponent, DeviceV2
from limnopulse_api.domain.ids import default_component_id_for_legacy_device
from limnopulse_api.domain.ingestion import (
    HTTPS_PROVIDER_KEYS,
    MAX_INGRESS_MESSAGE_BYTES,
    AuthenticatedSource,
    ComponentIngestState,
    ConnectorHealth,
    DeviceIngestState,
    IngestCredential,
    IngestCredentialStatus,
    IngestMarks,
    IngestObservationBatch,
    IngressAdmission,
    IngressDefer,
    IngressDenial,
    IngressEnvelope,
    IngressFence,
    IngressQueueMessage,
    IngressReceipt,
    IngressReceiptState,
    QuarantineEntry,
    QuarantineEntryStatus,
    QuarantinePage,
    TrustedIngress,
    build_trusted_context,
    classify_admission,
    classify_fence,
    ingest_audit_item,
    ingest_secret_sha256,
    ingress_message_bytes,
    is_ingress_receipt_live,
    parse_ingest_token,
    require_metric_labels,
    resolve_component_selectors,
)
from limnopulse_api.domain.integrations import (
    DeviceIntegration,
    DeviceIntegrationStatus,
    IntegrationAccount,
    IntegrationAccountStatus,
    ensure_device_integration_transition,
    ensure_integration_account_transition,
)
from limnopulse_api.domain.telemetry_v2 import SourceObservation

_ISSUABLE = frozenset({IntegrationAccountStatus.PENDING, IntegrationAccountStatus.ACTIVE})
_DUMMY_DIGEST = "0" * 64


def _now() -> datetime:
    return datetime.now(UTC)


class Faults:
    """One-shot failures keyed by operation name."""

    def __init__(self) -> None:
        self._pending: dict[str, list[BaseException]] = defaultdict(list)

    def fail_next(self, operation: str, exc: BaseException) -> None:
        self._pending[operation].append(exc)

    def check(self, operation: str) -> None:
        pending = self._pending.get(operation)
        if pending:
            raise pending.pop(0)


class IngestionStore:
    """The shared in-memory rows; the named reads are the `STORE_READS` fault points."""

    def __init__(self, faults: Faults) -> None:
        self.faults = faults
        self.now = _now()
        self.accounts: dict[tuple[str, str], IntegrationAccount] = {}
        self.device_integrations: dict[tuple[str, str], DeviceIntegration] = {}
        self.external_ids: dict[tuple[str, str], tuple[str, str]] = {}
        self.devices: dict[tuple[str, str], DeviceV2] = {}
        self.components: dict[tuple[str, str], DeviceComponent] = {}
        self.credentials: dict[str, IngestCredential] = {}
        self.slots: dict[tuple[str, str, int], str] = {}
        self.audit: list[dict[str, Any]] = []
        self.receipts: dict[tuple[str, str], IngressReceipt] = {}
        self.quarantine: dict[tuple[str, str], QuarantineEntry] = {}
        self.component_states: dict[str, dict[str, Any]] = {}
        self.device_states: dict[str, dict[str, Any]] = {}
        self.connector_health: dict[str, dict[str, Any]] = {}

    def get_ingest_credential(self, ingest_credential_id: str) -> IngestCredential | None:
        self.faults.check("get_ingest_credential")
        return self.credentials.get(ingest_credential_id)

    def get_integration_account(
        self, tenant_id: str, integration_account_id: str
    ) -> IntegrationAccount | None:
        self.faults.check("get_integration_account")
        return self.accounts.get((tenant_id, integration_account_id))

    def resolve_external_identity(
        self, provider_key: str, external_identity_hash: str
    ) -> DeviceIntegration | None:
        self.faults.check("resolve_external_identity")
        key = self.external_ids.get((provider_key, external_identity_hash))
        integration = None if key is None else self.device_integrations.get(key)
        if integration is None or (
            integration.provider_key,
            integration.external_identity_hash,
        ) != (
            provider_key,
            external_identity_hash,
        ):
            return None  # missing or inconsistent rows resolve to nothing
        return integration

    def get_device(self, tenant_id: str, device_id: str) -> DeviceV2 | None:
        self.faults.check("get_device")
        return self.devices.get((tenant_id, device_id))

    def get_component(self, tenant_id: str, component_id: str) -> DeviceComponent | None:
        self.faults.check("get_component")
        return self.components.get((tenant_id, component_id))


# Ports ---------------------------------------------------------------------------------------


class FakeIngestCredentialRepository:
    def __init__(self, store: IngestionStore) -> None:
        self.store = store

    async def get_ingest_credential(self, ingest_credential_id: str) -> IngestCredential | None:
        self.store.faults.check("get_ingest_credential")
        return self.store.credentials.get(ingest_credential_id)

    async def list_ingest_credentials(
        self, tenant_id: str, integration_account_id: str
    ) -> list[IngestCredential]:
        self.store.faults.check("list_ingest_credentials")
        return sorted(
            (
                item
                for item in self.store.credentials.values()
                if (item.tenant_id, item.integration_account_id)
                == (tenant_id, integration_account_id)
            ),
            key=lambda item: item.ingest_credential_id,
        )

    async def create_ingest_credential(
        self, credential: IngestCredential, audit: AuditContext
    ) -> IngestCredential:
        self.store.faults.check("create_ingest_credential")
        # One atomic step: the account check, the slot, the row and the audit item.
        account = self.store.accounts.get((credential.tenant_id, credential.integration_account_id))
        if account is None:
            raise NotFoundError("integration account not found")
        if account.status not in _ISSUABLE or account.provider_key not in HTTPS_PROVIDER_KEYS:
            raise ConflictError("integration account cannot issue ingest credentials")
        slot = (credential.tenant_id, credential.integration_account_id, credential.slot)
        if slot in self.store.slots or credential.ingest_credential_id in self.store.credentials:
            raise ConflictError("ingest credential slot is taken")
        self.store.credentials[credential.ingest_credential_id] = credential
        self.store.slots[slot] = credential.ingest_credential_id
        self.store.audit.append(
            ingest_audit_item(
                action="ingest_credential.issued",
                tenant_id=credential.tenant_id,
                resource_id=credential.ingest_credential_id,
                before_status=None,
                after_status=credential.status.value,
                context=audit,
                now=credential.created_at,
                fingerprint=credential.fingerprint,
            )
        )
        return credential

    async def revoke_ingest_credential(
        self,
        tenant_id: str,
        integration_account_id: str,
        ingest_credential_id: str,
        *,
        expected_version: int,
        revoked_at: datetime,
        audit: AuditContext,
    ) -> IngestCredential:
        self.store.faults.check("revoke_ingest_credential")
        current = self.store.credentials.get(ingest_credential_id)
        if (
            current is None
            or (current.tenant_id, current.integration_account_id)
            != (tenant_id, integration_account_id)
            or current.status != IngestCredentialStatus.ACTIVE
        ):
            raise NotFoundError("ingest credential not found")
        if current.version != expected_version:
            raise ConflictError("ingest credential version conflict")
        revoked = IngestCredential.model_validate(
            {
                **current.model_dump(),
                "status": IngestCredentialStatus.REVOKED,
                "revoked_at": revoked_at,
                "updated_at": revoked_at,
                "version": current.version + 1,
            }
        )
        self.store.credentials[ingest_credential_id] = revoked
        self.store.slots.pop((tenant_id, integration_account_id, current.slot), None)
        self.store.audit.append(
            ingest_audit_item(
                action="ingest_credential.revoked",
                tenant_id=tenant_id,
                resource_id=ingest_credential_id,
                before_status=current.status.value,
                after_status=revoked.status.value,
                context=audit,
                now=revoked_at,
                fingerprint=current.fingerprint,
            )
        )
        return revoked


class FakeConnectorAuthenticator:
    def __init__(self, store: IngestionStore) -> None:
        self.store = store

    async def authenticate(self, token: str) -> AuthenticatedSource | None:
        self.store.faults.check("authenticate")
        parsed = parse_ingest_token(token)
        if parsed is None:
            return None
        credential_id, secret = parsed
        credential = self.store.get_ingest_credential(credential_id)
        digest = ingest_secret_sha256(secret)
        if credential is None:
            hmac.compare_digest(digest, _DUMMY_DIGEST)  # one dummy comparison
            return None
        if not hmac.compare_digest(digest, credential.secret_sha256):
            return None
        if credential.status != IngestCredentialStatus.ACTIVE:
            return None
        account = self.store.get_integration_account(
            credential.tenant_id, credential.integration_account_id
        )
        if (
            account is None
            or account.status != IntegrationAccountStatus.ACTIVE
            or account.provider_key not in HTTPS_PROVIDER_KEYS
        ):
            return None
        return AuthenticatedSource(
            tenant_id=credential.tenant_id,
            integration_account_id=credential.integration_account_id,
            provider_key=account.provider_key,
            ingest_credential_id=credential.ingest_credential_id,
        )


class FakeExternalIdentityResolver:
    def __init__(self, store: IngestionStore) -> None:
        self.store = store

    async def resolve_external_identity(
        self, provider_key: str, external_identity_hash: str
    ) -> DeviceIntegration | None:
        return self.store.resolve_external_identity(provider_key, external_identity_hash)


class FakeIntegrationLifecycleRepository:
    def __init__(self, store: IngestionStore) -> None:
        self.store = store

    async def transition_integration_account(
        self,
        tenant_id: str,
        integration_account_id: str,
        target: IntegrationAccountStatus,
        *,
        expected_version: int,
        updated_at: datetime,
        audit: AuditContext,
    ) -> IntegrationAccount:
        self.store.faults.check("transition_integration_account")
        current = self.store.accounts.get((tenant_id, integration_account_id))
        if current is None:
            raise NotFoundError("integration account not found")
        if current.version != expected_version:
            raise ConflictError("integration account version conflict")
        ensure_integration_account_transition(current.status, target)
        updated = IntegrationAccount.model_validate(
            {
                **current.model_dump(),
                "status": target,
                "updated_at": updated_at,
                "version": current.version + 1,
            }
        )
        self.store.accounts[(tenant_id, integration_account_id)] = updated
        self.store.audit.append(
            ingest_audit_item(
                action="integration_account.status_changed",
                tenant_id=tenant_id,
                resource_id=integration_account_id,
                before_status=current.status.value,
                after_status=updated.status.value,
                context=audit,
                now=updated_at,
            )
        )
        return updated

    async def transition_device_integration(
        self,
        tenant_id: str,
        device_integration_id: str,
        target: DeviceIntegrationStatus,
        *,
        expected_version: int,
        updated_at: datetime,
        audit: AuditContext,
    ) -> DeviceIntegration:
        self.store.faults.check("transition_device_integration")
        current = self.store.device_integrations.get((tenant_id, device_integration_id))
        if current is None:
            raise NotFoundError("device integration not found")
        if current.version != expected_version:
            raise ConflictError("device integration version conflict")
        ensure_device_integration_transition(current.status, target)
        updated = DeviceIntegration.model_validate(
            {
                **current.model_dump(),
                "status": target,
                "updated_at": updated_at,
                "version": current.version + 1,
            }
        )
        self.store.device_integrations[(tenant_id, device_integration_id)] = updated
        self.store.audit.append(
            ingest_audit_item(
                action="device_integration.status_changed",
                tenant_id=tenant_id,
                resource_id=device_integration_id,
                before_status=current.status.value,
                after_status=updated.status.value,
                context=audit,
                now=updated_at,
            )
        )
        return updated


class FakeConsistentDeviceReader:
    def __init__(self, store: IngestionStore) -> None:
        self.store = store

    async def get_device(
        self, tenant_id: str, device_id: str, *, consistent: bool
    ) -> DeviceV2 | None:
        return self.store.get_device(tenant_id, device_id)


class FakeTrustedMapping:
    """Admission and fence over the store, through the pure P3-00 classifiers."""

    def __init__(self, store: IngestionStore) -> None:
        self.store = store

    async def admit(
        self, source: AuthenticatedSource, batch: IngestObservationBatch
    ) -> IngressAdmission | IngressDenial:
        self.store.faults.check("admit")
        identity_hash = account_scoped_identity_hash(
            source.provider_key, source.integration_account_id, batch.external_device_id
        )
        integration = self.store.resolve_external_identity(source.provider_key, identity_hash)
        account = self.store.get_integration_account(
            source.tenant_id, source.integration_account_id
        )
        device = None
        if integration is not None:
            device = self.store.get_device(integration.tenant_id, integration.device_id)
        denied = classify_admission(source, integration, account, device)
        if denied is not None:
            return IngressDenial(reason=denied)
        default_component_id = default_component_id_for_legacy_device(
            integration.tenant_id, integration.device_id
        )
        wanted = {item.component_id for item in batch.observations} - {None}
        if any(item.component_id is None for item in batch.observations):
            wanted.add(default_component_id)
        active = set()
        for component_id in sorted(wanted):
            component = self.store.get_component(integration.tenant_id, component_id)
            if (
                component is not None
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

    async def fence(
        self, envelope: IngressEnvelope
    ) -> TrustedIngress | IngressFence | IngressDefer:
        self.store.faults.check("fence")
        integration = self.store.resolve_external_identity(
            envelope.provider_key, envelope.external_identity_hash
        )
        # `account_verified` means an authoritative row exists for the ENVELOPE's tenant and
        # account (D8). When the DI tenant differs, the fence is a mismatch anyway.
        account = self.store.get_integration_account(
            envelope.tenant_id, envelope.integration_account_id
        )
        device = None
        if integration is not None:
            device = self.store.get_device(integration.tenant_id, integration.device_id)
        result = classify_fence(envelope, integration, account, device)
        if result is not None:
            return result
        return TrustedIngress(
            context=build_trusted_context(envelope, integration),
            observations=envelope.observations,
        )


class FakeIngressReceiptRepository:
    def __init__(self, store: IngestionStore) -> None:
        self.store = store

    async def get_ingress_receipt(
        self, integration_account_id: str, key_sha256: str
    ) -> IngressReceipt | None:
        self.store.faults.check("get_ingress_receipt")
        return self.store.receipts.get((integration_account_id, key_sha256))

    async def claim_ingress_receipt(
        self, receipt: IngressReceipt, *, now: datetime
    ) -> IngressReceipt:
        self.store.faults.check("claim_ingress_receipt")
        key = (receipt.integration_account_id, receipt.key_sha256)
        current = self.store.receipts.get(key)
        if current is not None and is_ingress_receipt_live(current, now):
            return current
        self.store.receipts[key] = receipt
        return receipt

    async def mark_ingress_receipt_enqueued(
        self, integration_account_id: str, key_sha256: str, ingress_id: str, *, at: datetime
    ) -> None:
        self.store.faults.check("mark_ingress_receipt_enqueued")
        key = (integration_account_id, key_sha256)
        current = self.store.receipts.get(key)
        if (
            current is not None
            and current.ingress_id == ingress_id
            and current.state == IngressReceiptState.PENDING
        ):
            self.store.receipts[key] = current.model_copy(
                update={"state": IngressReceiptState.ENQUEUED}
            )


class FakeIngressQueue:
    """Producer and consumer over one standard queue with visibility timeouts."""

    def __init__(self, faults: Faults, *, clock: Any = time.monotonic) -> None:
        self.faults = faults
        self.clock = clock
        self.messages: OrderedDict[str, dict[str, Any]] = OrderedDict()

    async def send(self, envelope: IngressEnvelope) -> str:
        self.faults.check("send")
        if ingress_message_bytes(envelope) > MAX_INGRESS_MESSAGE_BYTES:
            raise IngressMessageTooLarge("ingress envelope exceeds the message size guard")
        return self._append(envelope.to_json())

    async def send_raw(self, body: str) -> str:
        self.faults.check("send_raw")
        return self._append(body)

    async def receive(
        self, *, max_messages: int, wait_seconds: int, visibility_seconds: int
    ) -> list[IngressQueueMessage]:
        self.faults.check("receive")
        now = self.clock()
        received: list[IngressQueueMessage] = []
        for message_id, message in self.messages.items():
            if len(received) >= max_messages:
                break
            if message["visible_at"] > now:
                continue
            message["receive_count"] += 1
            message["visible_at"] = now + visibility_seconds
            message["receipt_handle"] = uuid4().hex
            received.append(
                IngressQueueMessage(
                    message_id=message_id,
                    receipt_handle=message["receipt_handle"],
                    body=message["body"],
                    receive_count=message["receive_count"],
                    sent_at=message["sent_at"],
                )
            )
        return received

    async def delete(self, message: IngressQueueMessage) -> None:
        self.faults.check("delete")
        stored = self.messages.get(message.message_id)
        if stored is not None:
            self._require_current_handle(stored, message)
            del self.messages[message.message_id]

    async def change_visibility(self, message: IngressQueueMessage, seconds: int) -> None:
        self.faults.check("change_visibility")
        stored = self.messages.get(message.message_id)
        if stored is None:
            raise IngressQueueError("message is no longer on the queue", retryable=False)
        self._require_current_handle(stored, message)
        stored["visible_at"] = self.clock() + seconds

    @staticmethod
    def _require_current_handle(stored: dict[str, Any], message: IngressQueueMessage) -> None:
        # Like SQS, a receive issues a new handle; an older one (e.g. a late heartbeat of a
        # worker whose visibility ran out) is refused instead of touching the new receiver's copy.
        if stored["receipt_handle"] != message.receipt_handle:
            raise IngressQueueError("stale receipt handle", retryable=False)

    def _append(self, body: str) -> str:
        message_id = uuid4().hex
        self.messages[message_id] = {
            "body": body,
            "sent_at": _now(),
            "receive_count": 0,
            "visible_at": float("-inf"),
            "receipt_handle": "",
        }
        return message_id


class FakeQuarantineRepository:
    def __init__(self, store: IngestionStore) -> None:
        self.store = store

    def _live(self, entry: QuarantineEntry | None) -> QuarantineEntry | None:
        return entry if entry is not None and self.store.now < entry.expires_at else None

    async def put_quarantine_entries(self, entries: Sequence[QuarantineEntry]) -> None:
        self.store.faults.check("put_quarantine_entries")
        for entry in entries:
            # attribute_not_exists(PK): a redelivery never resets a replayed or dismissed row.
            self.store.quarantine.setdefault((entry.tenant_id, entry.entry_key), entry)

    async def list_quarantine_entries(
        self,
        tenant_id: str,
        *,
        status: QuarantineEntryStatus | None = None,
        reason: str | None = None,
        after_sk: str | None,
        limit: int,
    ) -> QuarantinePage:
        self.store.faults.check("list_quarantine_entries")
        if limit < 1:
            raise ValueError("limit must be at least 1")
        matching = [
            entry
            for (owner, key), entry in sorted(self.store.quarantine.items())
            if owner == tenant_id
            and (after_sk is None or key > after_sk)
            and self._live(entry) is not None
            and (status is None or entry.status == status)
            and (reason is None or entry.reason == reason)
        ]
        page = tuple(matching[:limit])
        more = len(matching) > limit
        return QuarantinePage(entries=page, next_after_sk=page[-1].entry_key if more else None)

    async def get_quarantine_entry(self, tenant_id: str, entry_key: str) -> QuarantineEntry | None:
        self.store.faults.check("get_quarantine_entry")
        return self._live(self.store.quarantine.get((tenant_id, entry_key)))

    async def transition_quarantine_entry(
        self,
        tenant_id: str,
        entry_key: str,
        target: QuarantineEntryStatus,
        *,
        expected_version: int,
        at: datetime,
        replayed_ingress_id: str | None = None,
    ) -> QuarantineEntry:
        self.store.faults.check("transition_quarantine_entry")
        current = self._live(self.store.quarantine.get((tenant_id, entry_key)))
        if current is None:
            raise NotFoundError("quarantine entry not found")
        if (target == QuarantineEntryStatus.REPLAYED) != (replayed_ingress_id is not None):
            raise ValueError("replayed_ingress_id is set exactly for a replay")
        if current.status != QuarantineEntryStatus.OPEN or target == QuarantineEntryStatus.OPEN:
            raise ConflictError("quarantine entry is terminal")
        if current.version != expected_version:
            raise ConflictError("quarantine entry version conflict")
        updated = QuarantineEntry.model_validate(
            {
                **current.model_dump(),
                "status": target,
                "replayed_ingress_id": replayed_ingress_id,
                "updated_at": at,
                "version": current.version + 1,
            }
        )
        self.store.quarantine[(tenant_id, entry_key)] = updated
        return updated


def _advance(row: dict[str, Any], name: str, value: datetime | None) -> None:
    """Simple monotone mark: `attribute_not_exists(#m) OR #m < :t`."""
    if value is not None and (row.get(name) is None or row[name] < value):
        row[name] = value


def _advance_pair(
    row: dict[str, Any], name: str, value: datetime | None, breaker: str, tie: Any
) -> None:
    """Paired mark: `... OR (#m = :t AND #tb < :tb)`; the tie-breaker is written with it."""
    if value is None:
        return
    current = row.get(name)
    if current is None or current < value or (current == value and row[breaker] < tie):
        row[name] = value
        row[breaker] = tie


class FakeIngestStateRepository:
    def __init__(self, store: IngestionStore) -> None:
        self.store = store

    async def get_component_states(
        self, tenant_id: str, component_ids: Sequence[str]
    ) -> dict[str, ComponentIngestState]:
        self.store.faults.check("get_component_states")
        states: dict[str, ComponentIngestState] = {}
        for component_id in component_ids:
            row = self.store.component_states.get(component_id)
            if row is not None and row["tenant_id"] == tenant_id:
                states[component_id] = ComponentIngestState(component_id=component_id, **row)
        return states

    async def apply_ingest_marks(self, marks: IngestMarks) -> None:
        self.store.faults.check("apply_ingest_marks")
        identity = {"tenant_id": marks.tenant_id}
        for mark in marks.component_marks:
            row = self.store.component_states.setdefault(
                mark.component_id, {**identity, "device_id": marks.device_id}
            )
            _advance(row, "last_received_at", mark.received_at)
            _advance(row, "last_ingested_at", mark.ingested_at)
            _advance_pair(
                row, "last_observed_at", mark.observed_at, "last_observed_sequence", mark.sequence
            )
        if marks.device_mark is not None and marks.device_id is not None:
            row = self.store.device_states.setdefault(marks.device_id, dict(identity))
            _advance(row, "last_received_at", marks.device_mark.received_at)
            _advance(row, "last_ingested_at", marks.device_mark.ingested_at)
        connector = marks.connector_mark
        if any(value is not None for value in connector.model_dump().values()):
            row = self.store.connector_health.setdefault(
                marks.integration_account_id, dict(identity)
            )
            _advance(row, "last_received_at", connector.received_at)
            _advance(row, "last_accepted_at", connector.accepted_at)
            _advance_pair(
                row,
                "last_quarantined_at",
                connector.quarantined_at,
                "last_quarantine_reason",
                connector.quarantine_reason,
            )
            _advance_pair(
                row,
                "last_fenced_at",
                connector.fenced_at,
                "last_fence_reason",
                connector.fence_reason,
            )

    async def get_connector_health(
        self, tenant_id: str, integration_account_id: str
    ) -> ConnectorHealth | None:
        self.store.faults.check("get_connector_health")
        row = self.store.connector_health.get(integration_account_id)
        if row is None or row["tenant_id"] != tenant_id:
            return None
        return ConnectorHealth(integration_account_id=integration_account_id, **row)


class InMemoryRateLimiter:
    """Token buckets per key with LRU eviction beyond `max_keys`."""

    def __init__(self, *, capacity: int, refill_per_second: float, max_keys: int) -> None:
        self.capacity = capacity
        self.refill_per_second = refill_per_second
        self.max_keys = max_keys
        self.buckets: OrderedDict[str, tuple[float, float]] = OrderedDict()

    def try_acquire(self, key: str, *, now: float) -> float | None:
        if key in self.buckets:
            self.buckets.move_to_end(key)
            tokens, last = self.buckets[key]
            tokens = min(self.capacity, tokens + (now - last) * self.refill_per_second)
        else:
            while len(self.buckets) >= self.max_keys:
                self.buckets.popitem(last=False)
            tokens = float(self.capacity)
        if tokens >= 1:
            self.buckets[key] = (tokens - 1, now)
            return None
        self.buckets[key] = (tokens, now)
        return (1 - tokens) / self.refill_per_second


class FakeEntitlementCheck:
    """Default allow, like the P3 `AllowDirectIngressEntitlements` hook."""

    def __init__(self, faults: Faults, *, allow: bool = True) -> None:
        self.faults = faults
        self.allow = allow

    async def allows(
        self, tenant_id: str, integration_account_id: str, observation_count: int
    ) -> bool:
        self.faults.check("allows")
        return self.allow


class RecordingTelemetryMetrics:
    """Records `{"metric", "value", "labels"}` objects, the structured-log shape."""

    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def increment(self, name: str, value: int = 1, **labels: str) -> None:
        require_metric_labels(name, labels)
        self.records.append({"metric": name, "value": value, "labels": dict(labels)})

    def observe(self, name: str, value: float, **labels: str) -> None:
        require_metric_labels(name, labels)
        self.records.append({"metric": name, "value": value, "labels": dict(labels)})


# Kit subjects ---------------------------------------------------------------------------------


class KitSubject:
    """One port (or a producer/consumer pair) plus the seeding helpers the kits use."""

    def __init__(self, **ports: Any) -> None:
        self.faults = Faults()
        self.store = IngestionStore(self.faults)
        for name, factory in ports.items():
            setattr(self, name, factory(self))

    def fail_next(self, operation: str, exc: BaseException) -> None:
        self.faults.fail_next(operation, exc)

    def set_now(self, now: datetime) -> None:
        self.store.now = now

    def audit_items(self) -> list[dict[str, Any]]:
        return list(self.store.audit)

    async def seed_account(self, account: IntegrationAccount) -> None:
        self.store.accounts[(account.tenant_id, account.integration_account_id)] = account

    async def seed_credential(self, credential: IngestCredential) -> None:
        self.store.credentials[credential.ingest_credential_id] = credential

    async def seed_device_integration(self, integration: DeviceIntegration) -> None:
        key = (integration.tenant_id, integration.device_integration_id)
        self.store.device_integrations[key] = integration
        identity = (integration.provider_key, integration.external_identity_hash)
        self.store.external_ids[identity] = key

    async def seed_device(self, device: DeviceV2) -> None:
        self.store.devices[(device.tenant_id, device.device_id)] = device

    async def seed_component(self, component: DeviceComponent) -> None:
        self.store.components[(component.tenant_id, component.component_id)] = component

    async def device_state(self, device_id: str) -> DeviceIngestState | None:
        row = self.store.device_states.get(device_id)
        return None if row is None else DeviceIngestState(device_id=device_id, **row)

    def recorded(self) -> list[dict[str, Any]]:
        return list(self.metrics.records)


def credential_subject() -> KitSubject:
    return KitSubject(repository=lambda s: FakeIngestCredentialRepository(s.store))


def authenticator_subject() -> KitSubject:
    return KitSubject(authenticator=lambda s: FakeConnectorAuthenticator(s.store))


def resolver_subject() -> KitSubject:
    return KitSubject(resolver=lambda s: FakeExternalIdentityResolver(s.store))


def lifecycle_subject() -> KitSubject:
    return KitSubject(repository=lambda s: FakeIntegrationLifecycleRepository(s.store))


def device_reader_subject() -> KitSubject:
    return KitSubject(reader=lambda s: FakeConsistentDeviceReader(s.store))


def trusted_mapping_subject() -> KitSubject:
    return KitSubject(mapping=lambda s: FakeTrustedMapping(s.store))


def receipt_subject() -> KitSubject:
    return KitSubject(repository=lambda s: FakeIngressReceiptRepository(s.store))


def queue_subject() -> KitSubject:
    subject = KitSubject(producer=lambda s: FakeIngressQueue(s.faults))
    subject.consumer = subject.producer
    return subject


def quarantine_subject() -> KitSubject:
    return KitSubject(repository=lambda s: FakeQuarantineRepository(s.store))


def ingest_state_subject() -> KitSubject:
    return KitSubject(repository=lambda s: FakeIngestStateRepository(s.store))


def entitlement_subject() -> KitSubject:
    return KitSubject(check=lambda s: FakeEntitlementCheck(s.faults))


def metrics_subject() -> KitSubject:
    return KitSubject(metrics=lambda s: RecordingTelemetryMetrics())
