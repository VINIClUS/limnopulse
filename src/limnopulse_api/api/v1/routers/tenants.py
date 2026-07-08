from fastapi import APIRouter, Depends, HTTPException, status

from limnopulse_api.api.dependencies import (
    DomainRepositoryDep,
    PrincipalDep,
    require_tenant_read_access,
    require_tenant_role,
)
from limnopulse_api.api.v1.schemas import ErrorResponse, TenantCreate, TenantListResponse, TenantResponse, TenantUpdate
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.entities import Tenant, TenantAccess
from limnopulse_api.domain.roles import TenantRole, WRITE_ROLES
from limnopulse_api.services.tenants import TenantService

router = APIRouter(prefix="/tenants", tags=["tenants"])


def _tenant_service(repository) -> TenantService:
    return TenantService(repository)


def _to_tenant_response(tenant: Tenant) -> TenantResponse:
    return TenantResponse(
        tenant_id=tenant.tenant_id,
        name=tenant.name,
        created_at=tenant.created_at.isoformat(),
        updated_at=tenant.updated_at.isoformat(),
        version=tenant.version,
        status=tenant.status,
    )


@router.get("", response_model=TenantListResponse)
async def list_tenants(
    principal: PrincipalDep,
    repository: DomainRepositoryDep,
) -> TenantListResponse:
    service = _tenant_service(repository)
    tenants = await service.list_for_user(principal.cognito_sub)
    return TenantListResponse(items=[_to_tenant_response(tenant) for tenant in tenants])


@router.post(
    "",
    response_model=TenantResponse,
    status_code=status.HTTP_201_CREATED,
    responses={409: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
)
async def create_tenant(
    payload: TenantCreate,
    principal: PrincipalDep,
    repository: DomainRepositoryDep,
) -> TenantResponse:
    service = _tenant_service(repository)
    try:
        tenant = await service.create(payload.name, principal.cognito_sub)
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc) or "conflict") from exc
    return _to_tenant_response(tenant)


@router.get(
    "/{tenant_id}",
    response_model=TenantResponse,
    responses={403: {"model": ErrorResponse}, 404: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
)
async def get_tenant(
    tenant_id: str,
    repository: DomainRepositoryDep,
    _access: TenantAccess = Depends(require_tenant_read_access()),
) -> TenantResponse:
    service = _tenant_service(repository)
    try:
        tenant = await service.get(tenant_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc) or "not found") from exc
    if tenant is None:
        raise HTTPException(status_code=404, detail="tenant not found")
    return _to_tenant_response(tenant)


@router.patch(
    "/{tenant_id}",
    response_model=TenantResponse,
    responses={403: {"model": ErrorResponse}, 404: {"model": ErrorResponse}, 409: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
)
async def update_tenant(
    tenant_id: str,
    payload: TenantUpdate,
    repository: DomainRepositoryDep,
    _access: TenantAccess = Depends(require_tenant_role(*tuple(WRITE_ROLES))),
) -> TenantResponse:
    service = _tenant_service(repository)
    try:
        tenant = await service.update(tenant_id, payload.expected_version, payload.name)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc) or "not found") from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc) or "conflict") from exc
    return _to_tenant_response(tenant)
