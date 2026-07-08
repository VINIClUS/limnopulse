from fastapi import APIRouter, Depends, HTTPException, status

from limnopulse_api.api.dependencies import DomainRepositoryDep, require_tenant_role
from limnopulse_api.api.v1.schemas.common import ErrorResponse
from limnopulse_api.api.v1.schemas.ponds import PondCreate, PondListResponse, PondResponse, PondUpdate
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.entities import Pond, TenantAccess
from limnopulse_api.domain.roles import READ_ROLES, WRITE_ROLES
from limnopulse_api.services.ponds import PondService

router = APIRouter(prefix="/tenants/{tenant_id}/ponds", tags=["ponds"])


def _pond_service(repository) -> PondService:
    return PondService(repository)


def _to_pond_response(pond: Pond) -> PondResponse:
    return PondResponse(
        tenant_id=pond.tenant_id,
        pond_id=pond.pond_id,
        name=pond.name,
        description=pond.description,
        created_at=pond.created_at.isoformat(),
        updated_at=pond.updated_at.isoformat(),
        version=pond.version,
        status=pond.status,
    )


@router.get(
    "",
    response_model=PondListResponse,
    responses={403: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
)
async def list_ponds(
    tenant_id: str,
    repository: DomainRepositoryDep,
    _access: TenantAccess = Depends(require_tenant_role(*tuple(READ_ROLES))),
) -> PondListResponse:
    service = _pond_service(repository)
    ponds = await service.list(tenant_id)
    return PondListResponse(items=[_to_pond_response(pond) for pond in ponds])


@router.get(
    "/{pond_id}",
    response_model=PondResponse,
    responses={403: {"model": ErrorResponse}, 404: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
)
async def get_pond(
    tenant_id: str,
    pond_id: str,
    repository: DomainRepositoryDep,
    _access: TenantAccess = Depends(require_tenant_role(*tuple(READ_ROLES))),
) -> PondResponse:
    service = _pond_service(repository)
    pond = await service.get(tenant_id, pond_id)
    if pond is None:
        raise HTTPException(status_code=404, detail="not found")
    return _to_pond_response(pond)


@router.post(
    "",
    response_model=PondResponse,
    status_code=status.HTTP_201_CREATED,
    responses={403: {"model": ErrorResponse}, 409: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
)
async def create_pond(
    tenant_id: str,
    payload: PondCreate,
    repository: DomainRepositoryDep,
    _access: TenantAccess = Depends(require_tenant_role(*tuple(WRITE_ROLES))),
) -> PondResponse:
    service = _pond_service(repository)
    try:
        pond = await service.create(tenant_id, payload.name, payload.description)
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc) or "conflict") from exc
    return _to_pond_response(pond)


@router.patch(
    "/{pond_id}",
    response_model=PondResponse,
    responses={403: {"model": ErrorResponse}, 404: {"model": ErrorResponse}, 409: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
)
async def update_pond(
    tenant_id: str,
    pond_id: str,
    payload: PondUpdate,
    repository: DomainRepositoryDep,
    _access: TenantAccess = Depends(require_tenant_role(*tuple(WRITE_ROLES))),
) -> PondResponse:
    service = _pond_service(repository)
    try:
        pond = await service.update(
            tenant_id=tenant_id,
            pond_id=pond_id,
            expected_version=payload.expected_version,
            name=payload.name,
            description=payload.description,
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc) or "not found") from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc) or "conflict") from exc
    return _to_pond_response(pond)
