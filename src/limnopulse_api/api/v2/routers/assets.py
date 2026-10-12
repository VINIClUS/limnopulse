from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, status

from limnopulse_api.api.dependencies import require_tenant_role
from limnopulse_api.api.v2.dependencies import SiteAssetRepositoryDep
from limnopulse_api.api.v2.schemas.assets import (
    ASSET_ID_PATTERN,
    AssetCreate,
    AssetListResponse,
    AssetPatch,
    AssetResponse,
)
from limnopulse_api.api.v2.schemas.common import V2_ERROR_RESPONSES, IdempotencyKeyHeader
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.entities import TenantAccess
from limnopulse_api.domain.roles import READ_ROLES, WRITE_ROLES
from limnopulse_api.services.assets import AssetService

router = APIRouter(prefix="/tenants/{tenant_id}/assets", tags=["assets"])

AssetIdPath = Annotated[str, Path(pattern=ASSET_ID_PATTERN)]
ReadAccess = Annotated[TenantAccess, Depends(require_tenant_role(*tuple(READ_ROLES)))]
WriteAccess = Annotated[TenantAccess, Depends(require_tenant_role(*tuple(WRITE_ROLES)))]


@router.get("", response_model=AssetListResponse, responses=V2_ERROR_RESPONSES)
async def list_assets(
    tenant_id: str, repository: SiteAssetRepositoryDep, _access: ReadAccess
) -> AssetListResponse:
    assets = await AssetService(repository).list(tenant_id)
    return AssetListResponse(items=[AssetResponse.from_domain(asset) for asset in assets])


@router.post(
    "",
    response_model=AssetResponse,
    status_code=status.HTTP_201_CREATED,
    responses=V2_ERROR_RESPONSES,
)
async def create_asset(
    tenant_id: str,
    payload: AssetCreate,
    idempotency_key: IdempotencyKeyHeader,
    repository: SiteAssetRepositoryDep,
    _access: WriteAccess,
) -> AssetResponse:
    try:
        asset = await AssetService(repository).create(
            tenant_id=tenant_id,
            site_id=payload.site_id,
            asset_type=payload.asset_type,
            name=payload.name,
            pond_description=payload.pond_description,
            idempotency_key=idempotency_key,
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail="not found") from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail="conflict") from exc
    return AssetResponse.from_domain(asset)


@router.get("/{asset_id}", response_model=AssetResponse, responses=V2_ERROR_RESPONSES)
async def get_asset(
    tenant_id: str,
    asset_id: AssetIdPath,
    repository: SiteAssetRepositoryDep,
    _access: ReadAccess,
) -> AssetResponse:
    try:
        asset = await AssetService(repository).get(tenant_id, asset_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail="not found") from exc
    return AssetResponse.from_domain(asset)


@router.patch("/{asset_id}", response_model=AssetResponse, responses=V2_ERROR_RESPONSES)
async def update_asset(
    tenant_id: str,
    asset_id: AssetIdPath,
    payload: AssetPatch,
    repository: SiteAssetRepositoryDep,
    _access: WriteAccess,
) -> AssetResponse:
    try:
        asset = await AssetService(repository).update(
            tenant_id=tenant_id,
            asset_id=asset_id,
            expected_version=payload.expected_version,
            name=payload.name,
            status=payload.status,
            pond_description=payload.pond_description,
            pond_profile_expected_version=payload.pond_profile_expected_version,
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail="not found") from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail="conflict") from exc
    return AssetResponse.from_domain(asset)
