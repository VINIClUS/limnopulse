"""Tenant-owned, provider-neutral IntegrationAccounts and DeviceIntegrations.

`external_identity` is write-only. It is hashed here, together with the account's provider key,
and only the digest is handed to persistence; the raw value is never stored, logged, echoed in
responses or placed in exception messages. The idempotency request hash is a digest of the
request as given, so the raw identity contributes to it without ever being stored.

The external-identity lookup is global across tenants so one provider identity can be claimed
by exactly one DeviceIntegration. Consequently a create that collides with another tenant's
claim returns 409 "external identity already claimed": an accepted existence oracle, limited to
authenticated tenant writers who already know the raw provider identity.

Parent status is checked twice: the service reads the Device and IntegrationAccount for precise
404/409 answers, and the repository fences both statuses at commit time with ConditionChecks in
the same transaction, so a parent decommissioned or revoked in between still yields 409.
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from limnopulse_api.adapters.dynamodb_v2 import (
    canonical_request_hash,
    external_identity_hash,
    idempotency_key_hash,
    utc_now,
)
from limnopulse_api.api.v2.schemas.integrations import (
    DeviceIntegrationCreate,
    DeviceIntegrationResponse,
    IntegrationAccountCreate,
    IntegrationAccountResponse,
)
from limnopulse_api.core.errors import ConflictError, LimnopulseError, NotFoundError
from limnopulse_api.domain.devices_v2 import DeviceStatus, DeviceV2
from limnopulse_api.domain.ids import new_device_integration_id, new_integration_account_id
from limnopulse_api.domain.integrations import (
    DeviceIntegration,
    DeviceIntegrationStatus,
    IntegrationAccount,
    IntegrationAccountStatus,
)
from limnopulse_api.domain.v2_common import new_idempotency_receipt
from limnopulse_api.repositories.v2_contracts import DeviceReader, IntegrationRepository

INTEGRATION_ACCOUNT_CREATE_SCOPE = "integration-account-create"
DEVICE_INTEGRATION_CREATE_SCOPE = "device-integration-create"

PROVISIONABLE_ACCOUNT_STATUSES = frozenset(
    {IntegrationAccountStatus.PENDING, IntegrationAccountStatus.ACTIVE}
)


class IntegrationInputError(LimnopulseError):
    """Client input rejected by hashing; the message never contains the rejected value."""


def _key_hash(idempotency_key: str) -> str:
    try:
        return idempotency_key_hash(idempotency_key)
    except ValueError as exc:
        raise IntegrationInputError("invalid Idempotency-Key") from exc


def _identity_hash(provider_key: str, external_identity: str) -> str:
    try:
        return external_identity_hash(provider_key, external_identity)
    except ValueError as exc:
        raise IntegrationInputError("invalid external identity") from exc


@dataclass(frozen=True)
class IdempotentCreateResult:
    """The 201 body; identical for the original create and every same-key replay."""

    snapshot_json: str
    replayed: bool


class IntegrationService:
    def __init__(
        self,
        repository: IntegrationRepository,
        device_reader: DeviceReader | None = None,
        *,
        clock: Callable[[], datetime] = utc_now,
        integration_account_id_factory: Callable[[], str] = new_integration_account_id,
        device_integration_id_factory: Callable[[], str] = new_device_integration_id,
    ) -> None:
        self.repository = repository
        self.device_reader = device_reader
        self.clock = clock
        self.integration_account_id_factory = integration_account_id_factory
        self.device_integration_id_factory = device_integration_id_factory

    # IntegrationAccount --------------------------------------------------------------------

    async def list_integration_accounts(self, tenant_id: str) -> list[IntegrationAccount]:
        return await self.repository.list_integration_accounts(tenant_id)

    async def create_integration_account(
        self, tenant_id: str, payload: IntegrationAccountCreate, idempotency_key: str
    ) -> IdempotentCreateResult:
        scope = INTEGRATION_ACCOUNT_CREATE_SCOPE
        key_hash = _key_hash(idempotency_key)
        request_hash = canonical_request_hash(
            {
                "scope": scope,
                "path": {"tenant_id": tenant_id},
                "body": payload.model_dump(mode="json"),
            }
        )
        replay = await self.repository.get_idempotency_replay(
            tenant_id, scope, key_hash, request_hash
        )
        if replay is not None:
            return IdempotentCreateResult(replay, replayed=True)

        now = self.clock()
        account = IntegrationAccount(
            tenant_id=tenant_id,
            integration_account_id=self.integration_account_id_factory(),
            provider_key=payload.provider_key,
            display_name=payload.display_name,
            credential_reference=payload.credential_reference,
            public_identifier=payload.public_identifier,
            status=IntegrationAccountStatus.PENDING,
            created_at=now,
            updated_at=now,
            version=1,
        )
        snapshot = IntegrationAccountResponse.from_domain(account).model_dump_json()
        receipt = new_idempotency_receipt(
            tenant_id=tenant_id,
            scope=scope,
            key_hash=key_hash,
            request_hash=request_hash,
            response_snapshot_json=snapshot,
            created_at=now,
        )
        return await self._create_once(
            lambda: self.repository.create_integration_account(account, receipt),
            tenant_id,
            scope,
            key_hash,
            request_hash,
            snapshot,
        )

    # DeviceIntegration ---------------------------------------------------------------------

    async def list_device_integrations(
        self, tenant_id: str, device_id: str
    ) -> list[DeviceIntegration]:
        await self._require_tenant_device(tenant_id, device_id)
        return await self.repository.list_device_integrations(tenant_id, device_id)

    async def create_device_integration(
        self,
        tenant_id: str,
        device_id: str,
        payload: DeviceIntegrationCreate,
        idempotency_key: str,
    ) -> IdempotentCreateResult:
        scope = DEVICE_INTEGRATION_CREATE_SCOPE
        key_hash = _key_hash(idempotency_key)
        # The body includes the raw identity; only its digest is ever persisted.
        request_hash = canonical_request_hash(
            {
                "scope": scope,
                "path": {"tenant_id": tenant_id, "device_id": device_id},
                "body": payload.model_dump(mode="json"),
            }
        )
        replay = await self.repository.get_idempotency_replay(
            tenant_id, scope, key_hash, request_hash
        )
        if replay is not None:
            return IdempotentCreateResult(replay, replayed=True)

        device = await self._require_tenant_device(tenant_id, device_id)
        if device.status is DeviceStatus.DECOMMISSIONED:
            raise ConflictError("device is decommissioned")
        account = await self.repository.get_integration_account(
            tenant_id, payload.integration_account_id
        )
        if account is None or account.tenant_id != tenant_id:
            raise NotFoundError("integration account not found")
        if account.status not in PROVISIONABLE_ACCOUNT_STATUSES:
            raise ConflictError(f"integration account is {account.status.value}")

        # The provider comes from the tenant's account; the client cannot choose it.
        identity_hash = _identity_hash(account.provider_key, payload.external_identity)
        now = self.clock()
        integration = DeviceIntegration(
            tenant_id=tenant_id,
            device_integration_id=self.device_integration_id_factory(),
            device_id=device_id,
            integration_account_id=account.integration_account_id,
            provider_key=account.provider_key,
            external_identity_hash=identity_hash,
            status=DeviceIntegrationStatus.PROVISIONING,
            created_at=now,
            updated_at=now,
            version=1,
        )
        snapshot = DeviceIntegrationResponse.from_domain(integration).model_dump_json()
        receipt = new_idempotency_receipt(
            tenant_id=tenant_id,
            scope=scope,
            key_hash=key_hash,
            request_hash=request_hash,
            response_snapshot_json=snapshot,
            created_at=now,
        )
        return await self._create_once(
            lambda: self.repository.create_device_integration(integration, receipt),
            tenant_id,
            scope,
            key_hash,
            request_hash,
            snapshot,
        )

    # Internals -----------------------------------------------------------------------------

    async def _require_tenant_device(self, tenant_id: str, device_id: str) -> DeviceV2:
        if self.device_reader is None:
            raise RuntimeError("device reader is required for device integrations")
        device = await self.device_reader.get_device(tenant_id, device_id)
        if device is None or device.tenant_id != tenant_id:
            raise NotFoundError("device not found")
        return device

    async def _create_once(
        self,
        create: Callable[[], Any],
        tenant_id: str,
        scope: str,
        key_hash: str,
        request_hash: str,
        snapshot: str,
    ) -> IdempotentCreateResult:
        try:
            await create()
        except ConflictError:
            # A concurrent request with the same key may have committed first: reread the
            # receipt. Same request -> replay; different request -> the reread raises 409;
            # no receipt -> the original conflict (e.g. identity already claimed) stands.
            replay = await self.repository.get_idempotency_replay(
                tenant_id, scope, key_hash, request_hash
            )
            if replay is None:
                raise
            return IdempotentCreateResult(replay, replayed=True)
        return IdempotentCreateResult(snapshot, replayed=False)
