"""Unmounted `/v2` ingest-credential routes (D2); P3-10 mounts them under `/v2`.

Owner and admin only. Issuance takes no Idempotency-Key and returns the token exactly once with
`Cache-Control: no-store` and `Pragma: no-cache`.
"""

from collections.abc import Awaitable
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Path, Request, status
from fastapi.responses import JSONResponse
from pydantic import AfterValidator

from limnopulse_api.api.dependencies import require_tenant_role
from limnopulse_api.api.ingestion_dependencies import get_ingest_credential_repository
from limnopulse_api.api.redacted_validation import _RedactedValidationRoute
from limnopulse_api.api.v1.schemas.common import ErrorResponse
from limnopulse_api.api.v2.dependencies import IntegrationRepositoryDep
from limnopulse_api.api.v2.schemas.common import V2_ERROR_RESPONSES
from limnopulse_api.api.v2.schemas.ingest_credentials import (
    IngestCredentialIssuedResponse,
    IngestCredentialListResponse,
    IngestCredentialResponse,
    IngestCredentialRevokeRequest,
)
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.alerts import AuditContext
from limnopulse_api.domain.entities import TenantAccess
from limnopulse_api.domain.ids import require_canonical_id
from limnopulse_api.domain.roles import WRITE_ROLES
from limnopulse_api.repositories.ingestion import IngestCredentialRepository
from limnopulse_api.services.ingest_credentials import IngestCredentialService
from limnopulse_api.services.integrations import IntegrationInputError

router = APIRouter(
    prefix="/tenants/{tenant_id}/integration-accounts/{integration_account_id}/ingest-credentials",
    tags=["v2-ingest-credentials"],
    route_class=_RedactedValidationRoute,
)

IngestCredentialPathId = Annotated[
    str, Path(), AfterValidator(lambda value: require_canonical_id(value, "ic"))
]
TenantWriterDep = Annotated[TenantAccess, Depends(require_tenant_role(*tuple(WRITE_ROLES)))]
IngestCredentialRepositoryDep = Annotated[
    IngestCredentialRepository, Depends(get_ingest_credential_repository)
]
_RESPONSES = {403: {"model": ErrorResponse}, 422: {"model": ErrorResponse}, **V2_ERROR_RESPONSES}
_NO_STORE = {"Cache-Control": "no-store", "Pragma": "no-cache"}


def _audit_context(request: Request, access: TenantAccess) -> AuditContext:
    return AuditContext(
        actor_id=access.principal.cognito_sub,
        ip=request.client.host if request.client is not None else None,
        user_agent=request.headers.get("user-agent"),
    )


async def _run(call: Awaitable[Any]) -> Any:
    """Fixed error bodies: never an exception message, an id or input."""
    try:
        return await call
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail="not found") from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail="conflict") from exc
    except IntegrationInputError as exc:
        raise HTTPException(status_code=422, detail="invalid request") from exc


@router.post(
    "",
    response_model=IngestCredentialIssuedResponse,
    status_code=status.HTTP_201_CREATED,
    responses=_RESPONSES,
)
async def issue_ingest_credential(
    tenant_id: str,
    integration_account_id: str,
    request: Request,
    credentials: IngestCredentialRepositoryDep,
    integrations: IntegrationRepositoryDep,
    access: TenantWriterDep,
) -> JSONResponse:
    service = IngestCredentialService(credentials=credentials, integrations=integrations)
    issued = await _run(
        service.issue(tenant_id, integration_account_id, audit=_audit_context(request, access))
    )
    body = IngestCredentialIssuedResponse(
        **IngestCredentialResponse.from_domain(issued.credential).model_dump(),
        token=issued.token,
    )
    return JSONResponse(
        content=body.model_dump(mode="json"),
        status_code=status.HTTP_201_CREATED,
        headers=_NO_STORE,
    )


@router.get("", response_model=IngestCredentialListResponse, responses=_RESPONSES)
async def list_ingest_credentials(
    tenant_id: str,
    integration_account_id: str,
    credentials: IngestCredentialRepositoryDep,
    integrations: IntegrationRepositoryDep,
    _access: TenantWriterDep,
) -> IngestCredentialListResponse:
    service = IngestCredentialService(credentials=credentials, integrations=integrations)
    listed = await _run(service.list(tenant_id, integration_account_id))
    return IngestCredentialListResponse(
        items=[IngestCredentialResponse.from_domain(item) for item in listed]
    )


@router.post(
    "/{ingest_credential_id}/revoke",
    response_model=IngestCredentialResponse,
    responses=_RESPONSES,
)
async def revoke_ingest_credential(
    tenant_id: str,
    integration_account_id: str,
    ingest_credential_id: IngestCredentialPathId,
    payload: IngestCredentialRevokeRequest,
    request: Request,
    credentials: IngestCredentialRepositoryDep,
    integrations: IntegrationRepositoryDep,
    access: TenantWriterDep,
) -> IngestCredentialResponse:
    service = IngestCredentialService(credentials=credentials, integrations=integrations)
    revoked = await _run(
        service.revoke(
            tenant_id,
            integration_account_id,
            ingest_credential_id,
            expected_version=payload.expected_version,
            audit=_audit_context(request, access),
        )
    )
    return IngestCredentialResponse.from_domain(revoked)
