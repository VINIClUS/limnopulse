from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, status

from limnopulse_api.api.dependencies import require_tenant_role
from limnopulse_api.api.v2.dependencies import SiteAssetRepositoryDep
from limnopulse_api.api.v2.schemas.common import V2_ERROR_RESPONSES, IdempotencyKeyHeader
from limnopulse_api.api.v2.schemas.sites import (
    SITE_ID_PATTERN,
    SiteCreate,
    SiteListResponse,
    SitePatch,
    SiteResponse,
)
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.entities import TenantAccess
from limnopulse_api.domain.roles import READ_ROLES, WRITE_ROLES
from limnopulse_api.services.sites import SiteService

router = APIRouter(prefix="/tenants/{tenant_id}/sites", tags=["sites"])

SiteIdPath = Annotated[str, Path(pattern=SITE_ID_PATTERN)]
ReadAccess = Annotated[TenantAccess, Depends(require_tenant_role(*tuple(READ_ROLES)))]
WriteAccess = Annotated[TenantAccess, Depends(require_tenant_role(*tuple(WRITE_ROLES)))]


@router.get("", response_model=SiteListResponse, responses=V2_ERROR_RESPONSES)
async def list_sites(
    tenant_id: str, repository: SiteAssetRepositoryDep, _access: ReadAccess
) -> SiteListResponse:
    sites = await SiteService(repository).list(tenant_id)
    return SiteListResponse(items=[SiteResponse.from_domain(site) for site in sites])


@router.post(
    "",
    response_model=SiteResponse,
    status_code=status.HTTP_201_CREATED,
    responses=V2_ERROR_RESPONSES,
)
async def create_site(
    tenant_id: str,
    payload: SiteCreate,
    idempotency_key: IdempotencyKeyHeader,
    repository: SiteAssetRepositoryDep,
    _access: WriteAccess,
) -> SiteResponse:
    try:
        site = await SiteService(repository).create(
            tenant_id=tenant_id, name=payload.name, idempotency_key=idempotency_key
        )
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail="conflict") from exc
    return SiteResponse.from_domain(site)


@router.get("/{site_id}", response_model=SiteResponse, responses=V2_ERROR_RESPONSES)
async def get_site(
    tenant_id: str, site_id: SiteIdPath, repository: SiteAssetRepositoryDep, _access: ReadAccess
) -> SiteResponse:
    try:
        site = await SiteService(repository).get(tenant_id, site_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail="not found") from exc
    return SiteResponse.from_domain(site)


@router.patch("/{site_id}", response_model=SiteResponse, responses=V2_ERROR_RESPONSES)
async def update_site(
    tenant_id: str,
    site_id: SiteIdPath,
    payload: SitePatch,
    repository: SiteAssetRepositoryDep,
    _access: WriteAccess,
) -> SiteResponse:
    try:
        site = await SiteService(repository).update(
            tenant_id=tenant_id,
            site_id=site_id,
            expected_version=payload.expected_version,
            name=payload.name,
            status=payload.status,
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail="not found") from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail="conflict") from exc
    return SiteResponse.from_domain(site)
