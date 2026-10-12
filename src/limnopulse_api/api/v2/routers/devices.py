"""Unmounted `/v2` Device router; M1-07 mounts it under `/v2`."""

from fastapi import APIRouter, Depends, HTTPException, Response, status

from limnopulse_api.api.dependencies import require_tenant_role
from limnopulse_api.api.v1.schemas.common import ErrorResponse
from limnopulse_api.api.v2.dependencies import DeviceComponentRepositoryDep
from limnopulse_api.api.v2.schemas.common import V2_ERROR_RESPONSES, IdempotencyKeyHeader
from limnopulse_api.api.v2.schemas.devices import (
    V2DeviceCreateRequest,
    V2DeviceListResponse,
    V2DeviceResponse,
    V2DeviceUpdateRequest,
)
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.entities import TenantAccess
from limnopulse_api.domain.roles import READ_ROLES, WRITE_ROLES
from limnopulse_api.services.devices_v2 import DeviceV2Service

router = APIRouter(prefix="/tenants/{tenant_id}/devices", tags=["v2-devices"])

_RESPONSES: dict[int | str, dict[str, object]] = {
    403: {"model": ErrorResponse},
    **V2_ERROR_RESPONSES,
}


@router.get("", response_model=V2DeviceListResponse, responses=_RESPONSES)
async def list_devices(
    tenant_id: str,
    repository: DeviceComponentRepositoryDep,
    _access: TenantAccess = Depends(require_tenant_role(*READ_ROLES)),
) -> V2DeviceListResponse:
    devices = await DeviceV2Service(repository).list(tenant_id)
    return V2DeviceListResponse(items=[V2DeviceResponse.from_domain(item) for item in devices])


@router.post(
    "",
    response_model=V2DeviceResponse,
    status_code=status.HTTP_201_CREATED,
    responses=_RESPONSES,
)
async def create_device(
    tenant_id: str,
    payload: V2DeviceCreateRequest,
    idempotency_key: IdempotencyKeyHeader,
    repository: DeviceComponentRepositoryDep,
    _access: TenantAccess = Depends(require_tenant_role(*WRITE_ROLES)),
) -> Response:
    try:
        outcome = await DeviceV2Service(repository).create(tenant_id, payload, idempotency_key)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail="not found") from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return Response(
        content=outcome.snapshot_json,
        status_code=status.HTTP_201_CREATED,
        media_type="application/json",
    )


@router.get("/{device_id}", response_model=V2DeviceResponse, responses=_RESPONSES)
async def get_device(
    tenant_id: str,
    device_id: str,
    repository: DeviceComponentRepositoryDep,
    _access: TenantAccess = Depends(require_tenant_role(*READ_ROLES)),
) -> V2DeviceResponse:
    device = await DeviceV2Service(repository).get(tenant_id, device_id)
    if device is None:
        raise HTTPException(status_code=404, detail="not found")
    return V2DeviceResponse.from_domain(device)


@router.patch("/{device_id}", response_model=V2DeviceResponse, responses=_RESPONSES)
async def update_device(
    tenant_id: str,
    device_id: str,
    payload: V2DeviceUpdateRequest,
    repository: DeviceComponentRepositoryDep,
    _access: TenantAccess = Depends(require_tenant_role(*WRITE_ROLES)),
) -> V2DeviceResponse:
    try:
        device = await DeviceV2Service(repository).update(tenant_id, device_id, payload)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail="not found") from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return V2DeviceResponse.from_domain(device)
