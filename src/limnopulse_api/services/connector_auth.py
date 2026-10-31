"""ADR-016 connector authentication with a per-IntegrationAccount bearer ingest credential.

The token is parsed with `INGEST_TOKEN_PATTERN` before any store read. Digests are compared in
constant time, and an unknown credential still costs one dummy comparison. Nothing is cached, so
a revoke is effective on the very next call. A store failure propagates (503); only a missing,
invalid or mismatched credential, or an account that is not an active HTTPS account, is `None`
(401). The token, its secret and its digest are never logged.
"""

import hmac

from limnopulse_api.domain.ingestion import (
    HTTPS_PROVIDER_KEYS,
    AuthenticatedSource,
    IngestCredentialStatus,
    ingest_secret_sha256,
    parse_ingest_token,
)
from limnopulse_api.domain.integrations import IntegrationAccountStatus
from limnopulse_api.repositories.ingestion import IngestCredentialRepository
from limnopulse_api.repositories.v2_contracts import IntegrationRepository

_DUMMY_DIGEST = "0" * 64


class IngestCredentialAuthenticator:
    def __init__(
        self, *, credentials: IngestCredentialRepository, integrations: IntegrationRepository
    ) -> None:
        self.credentials = credentials
        self.integrations = integrations

    async def authenticate(self, token: str) -> AuthenticatedSource | None:
        parsed = parse_ingest_token(token)
        if parsed is None:
            return None
        ingest_credential_id, secret = parsed
        digest = ingest_secret_sha256(secret)
        credential = await self.credentials.get_ingest_credential(ingest_credential_id)
        if credential is None:
            hmac.compare_digest(digest, _DUMMY_DIGEST)
            return None
        if not hmac.compare_digest(digest, credential.secret_sha256):
            return None
        if credential.status != IngestCredentialStatus.ACTIVE:
            return None
        account = await self.integrations.get_integration_account(
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
