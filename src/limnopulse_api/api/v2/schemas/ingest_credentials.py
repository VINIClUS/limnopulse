"""`/v2` ingest-credential schemas. Responses carry metadata and the fingerprint only, never
`secret_sha256`; the token appears once, in the issuance response."""

from typing import Literal

from pydantic import BaseModel

from limnopulse_api.api.v2.schemas.common import (
    UtcTimestamp,
    V2ExpectedVersionRequest,
    V2VersionedResponse,
)
from limnopulse_api.domain.ingestion import IngestCredential


class IngestCredentialResponse(V2VersionedResponse):
    ingest_credential_id: str
    integration_account_id: str
    slot: Literal[1, 2]
    fingerprint: str
    status: Literal["active", "revoked"]
    revoked_at: UtcTimestamp | None = None

    @classmethod
    def from_domain(cls, credential: IngestCredential) -> "IngestCredentialResponse":
        return cls(
            tenant_id=credential.tenant_id,
            ingest_credential_id=credential.ingest_credential_id,
            integration_account_id=credential.integration_account_id,
            slot=credential.slot,
            fingerprint=credential.fingerprint,
            status=credential.status.value,
            revoked_at=credential.revoked_at,
            created_at=credential.created_at,
            updated_at=credential.updated_at,
            version=credential.version,
        )


class IngestCredentialIssuedResponse(IngestCredentialResponse):
    token: str


class IngestCredentialListResponse(BaseModel):
    items: list[IngestCredentialResponse]


class IngestCredentialRevokeRequest(V2ExpectedVersionRequest):
    pass
