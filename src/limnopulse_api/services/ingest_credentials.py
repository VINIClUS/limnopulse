"""Ingest credential issuance, listing and revocation (D2).

At most two credentials per IntegrationAccount are active, one per fixed slot. The token
`lpi_<ic_id>_<secret>` is returned once; only `secret_sha256` and its fingerprint are stored.
Credentials are issued only for `pending` or `active` accounts with an HTTPS provider key; the
repository re-checks that at commit time, so a concurrent account change is a 409.

Revocation is bound to the tenant and account in the path. The repository answers 404 for any
condition failure except a stale version; the service turns that 404 into a 409 only when a
consistent re-read shows a revoked credential of the same tenant and account.
"""

import secrets
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

from limnopulse_api.adapters.dynamodb_v2 import utc_now
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.alerts import AuditContext
from limnopulse_api.domain.ids import new_ingest_credential_id
from limnopulse_api.domain.ingestion import (
    HTTPS_PROVIDER_KEYS,
    MAX_ACTIVE_INGEST_CREDENTIALS,
    IngestCredential,
    IngestCredentialStatus,
    ingest_credential_fingerprint,
    ingest_secret_sha256,
    ingest_token,
)
from limnopulse_api.domain.integrations import IntegrationAccount, IntegrationAccountStatus
from limnopulse_api.repositories.ingestion import IngestCredentialRepository
from limnopulse_api.repositories.v2_contracts import IntegrationRepository
from limnopulse_api.services.integrations import IntegrationInputError

ISSUABLE_ACCOUNT_STATUSES = frozenset(
    {IntegrationAccountStatus.PENDING, IntegrationAccountStatus.ACTIVE}
)
_SLOTS = tuple(range(1, MAX_ACTIVE_INGEST_CREDENTIALS + 1))


def new_ingest_secret() -> str:
    """32 random bytes as unpadded base64url (43 characters)."""
    return secrets.token_urlsafe(32)


@dataclass(frozen=True)
class IssuedIngestCredential:
    credential: IngestCredential
    token: str = field(repr=False)  # returned once; never stored or logged


class IngestCredentialService:
    def __init__(
        self,
        *,
        credentials: IngestCredentialRepository,
        integrations: IntegrationRepository,
        clock: Callable[[], datetime] = utc_now,
        secret_factory: Callable[[], str] = new_ingest_secret,
        id_factory: Callable[[], str] = new_ingest_credential_id,
    ) -> None:
        self.credentials = credentials
        self.integrations = integrations
        self.clock = clock
        self.secret_factory = secret_factory
        self.id_factory = id_factory

    async def issue(
        self, tenant_id: str, integration_account_id: str, *, audit: AuditContext
    ) -> IssuedIngestCredential:
        account = await self._require_account(tenant_id, integration_account_id)
        if (
            account.provider_key not in HTTPS_PROVIDER_KEYS
            or account.status not in ISSUABLE_ACCOUNT_STATUSES
        ):
            raise IntegrationInputError("integration account cannot issue ingest credentials")
        existing = await self.credentials.list_ingest_credentials(tenant_id, integration_account_id)
        taken = {item.slot for item in existing if item.status == IngestCredentialStatus.ACTIVE}
        free = [slot for slot in _SLOTS if slot not in taken]
        if not free:
            raise ConflictError("integration account already has two active ingest credentials")
        secret = self.secret_factory()
        ingest_credential_id = self.id_factory()
        token = ingest_token(ingest_credential_id, secret)
        digest = ingest_secret_sha256(secret)
        now = self.clock()
        credential = IngestCredential(
            tenant_id=tenant_id,
            ingest_credential_id=ingest_credential_id,
            integration_account_id=integration_account_id,
            slot=free[0],
            secret_sha256=digest,
            fingerprint=ingest_credential_fingerprint(digest),
            status=IngestCredentialStatus.ACTIVE,
            created_at=now,
            updated_at=now,
            version=1,
        )
        created = await self.credentials.create_ingest_credential(credential, audit)
        return IssuedIngestCredential(credential=created, token=token)

    async def list(self, tenant_id: str, integration_account_id: str) -> list[IngestCredential]:
        await self._require_account(tenant_id, integration_account_id)
        return await self.credentials.list_ingest_credentials(tenant_id, integration_account_id)

    async def revoke(
        self,
        tenant_id: str,
        integration_account_id: str,
        ingest_credential_id: str,
        *,
        expected_version: int,
        audit: AuditContext,
    ) -> IngestCredential:
        try:
            return await self.credentials.revoke_ingest_credential(
                tenant_id,
                integration_account_id,
                ingest_credential_id,
                expected_version=expected_version,
                revoked_at=self.clock(),
                audit=audit,
            )
        except NotFoundError:
            current = await self.credentials.get_ingest_credential(ingest_credential_id)
            if (
                current is not None
                and current.tenant_id == tenant_id
                and current.integration_account_id == integration_account_id
                and current.status == IngestCredentialStatus.REVOKED
            ):
                raise ConflictError("ingest credential is already revoked") from None
            raise

    async def _require_account(
        self, tenant_id: str, integration_account_id: str
    ) -> IntegrationAccount:
        account = await self.integrations.get_integration_account(tenant_id, integration_account_id)
        if account is None:
            raise NotFoundError("integration account not found")
        return account
