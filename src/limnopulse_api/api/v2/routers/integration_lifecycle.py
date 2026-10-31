"""Unmounted `/v2` lifecycle routes for IntegrationAccounts and DeviceIntegrations (P3-04).

Owner and admin only. P3-10 mounts the router under `/v2` when `INGEST_V2_ENABLED` is on. Every
transition is version-fenced and writes one D23 audit item; a DeviceIntegration that the path
Device does not own is 404.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Request
from pydantic import AfterValidator

from limnopulse_api.api.dependencies import require_tenant_role
from limnopulse_api.api.ingestion_dependencies import get_integration_lifecycle_repository
from limnopulse_api.api.redacted_validation import _RedactedValidationRoute
from limnopulse_api.api.v1.schemas.common import ErrorResponse
from limnopulse_api.api.v2.routers.integrations import DevicePathId
from limnopulse_api.api.v2.schemas.common import V2_ERROR_RESPONSES
from limnopulse_api.api.v2.schemas.integration_lifecycle import (
    V2DeviceIntegrationStatusUpdate,
    V2IntegrationAccountStatusUpdate,
)
from limnopulse_api.api.v2.schemas.integrations import (
    DeviceIntegrationResponse,
    IntegrationAccountResponse,
)
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.alerts import AuditContext
from limnopulse_api.domain.entities import TenantAccess
from limnopulse_api.domain.ids import require_canonical_id
from limnopulse_api.domain.integrations import DeviceIntegrationStatus, IntegrationAccountStatus
from limnopulse_api.domain.roles import WRITE_ROLES
from limnopulse_api.repositories.ingestion import IntegrationLifecycleRepository
from limnopulse_api.services.integration_lifecycle import IntegrationLifecycleService

router = APIRouter(
    prefix="/tenants/{tenant_id}",
    tags=["v2-integration-lifecycle"],
    route_class=_RedactedValidationRoute,
)

# Canonical ids only: a malformed id (e.g. one containing `#`) is a redacted 422, never a key error.
IntegrationAccountPathId = Annotated[
    str, Path(), AfterValidator(lambda value: require_canonical_id(value, "ia"))
]
DeviceIntegrationPathId = Annotated[
    str, Path(), AfterValidator(lambda value: require_canonical_id(value, "di"))
]
TenantWriterDep = Annotated[TenantAccess, Depends(require_tenant_role(*tuple(WRITE_ROLES)))]
LifecycleRepositoryDep = Annotated[
    IntegrationLifecycleRepository, Depends(get_integration_lifecycle_repository)
]
_RESPONSES = {403: {"model": ErrorResponse}, **V2_ERROR_RESPONSES}


def _audit_context(request: Request, access: TenantAccess) -> AuditContext:
    return AuditContext(
        actor_id=access.principal.cognito_sub,
        ip=request.client.host if request.client is not None else None,
        user_agent=request.headers.get("user-agent"),
    )


def _http_error(exc: NotFoundError | ConflictError) -> HTTPException:
    if isinstance(exc, NotFoundError):
        return HTTPException(status_code=404, detail="not found")
    return HTTPException(status_code=409, detail=str(exc) or "conflict")


@router.post(
    "/integration-accounts/{integration_account_id}/status",
    response_model=IntegrationAccountResponse,
    responses=_RESPONSES,
)
async def transition_integration_account(
    tenant_id: str,
    integration_account_id: IntegrationAccountPathId,
    payload: V2IntegrationAccountStatusUpdate,
    request: Request,
    repository: LifecycleRepositoryDep,
    access: TenantWriterDep,
) -> IntegrationAccountResponse:
    service = IntegrationLifecycleService(repository)
    try:
        account = await service.transition_integration_account(
            tenant_id,
            integration_account_id,
            IntegrationAccountStatus(payload.status),
            expected_version=payload.expected_version,
            audit=_audit_context(request, access),
        )
    except (NotFoundError, ConflictError) as exc:
        raise _http_error(exc) from exc
    return IntegrationAccountResponse.from_domain(account)


@router.post(
    "/devices/{device_id}/integrations/{device_integration_id}/status",
    response_model=DeviceIntegrationResponse,
    responses=_RESPONSES,
)
async def transition_device_integration(
    tenant_id: str,
    device_id: DevicePathId,
    device_integration_id: DeviceIntegrationPathId,
    payload: V2DeviceIntegrationStatusUpdate,
    request: Request,
    repository: LifecycleRepositoryDep,
    access: TenantWriterDep,
) -> DeviceIntegrationResponse:
    service = IntegrationLifecycleService(repository)
    try:
        integration = await service.transition_device_integration(
            tenant_id,
            device_id,
            device_integration_id,
            DeviceIntegrationStatus(payload.status),
            expected_version=payload.expected_version,
            audit=_audit_context(request, access),
        )
    except (NotFoundError, ConflictError) as exc:
        raise _http_error(exc) from exc
    return DeviceIntegrationResponse.from_domain(integration)
