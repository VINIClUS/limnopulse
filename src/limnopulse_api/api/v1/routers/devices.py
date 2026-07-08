from fastapi import APIRouter, Depends, HTTPException, status

from limnopulse_api.api.dependencies import DomainRepositoryDep, require_tenant_role
from limnopulse_api.api.v1.schemas.common import ErrorResponse
from limnopulse_api.api.v1.schemas.devices import DeviceCreate, DeviceListResponse, DeviceResponse, DeviceUpdate
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.entities import Device, TenantAccess
from limnopulse_api.domain.roles import READ_ROLES, WRITE_ROLES
from limnopulse_api.services.devices import DeviceService

router = APIRouter(prefix="/tenants/{tenant_id}/devices", tags=["devices"])


def _device_service(repository) -> DeviceService:
    return DeviceService(repository)


def _to_device_response(device: Device) -> DeviceResponse:
    return DeviceResponse(
        tenant_id=device.tenant_id,
        pond_id=device.pond_id,
        device_id=device.device_id,
        name=device.name,
        auth_type=device.auth_type,
        firmware_version=device.firmware_version,
        created_at=device.created_at.isoformat(),
        updated_at=device.updated_at.isoformat(),
        version=device.version,
        status=device.status,
    )


@router.get(
    "",
    response_model=DeviceListResponse,
    responses={403: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
)
async def list_devices(
    tenant_id: str,
    repository: DomainRepositoryDep,
    _access: TenantAccess = Depends(require_tenant_role(*tuple(READ_ROLES))),
) -> DeviceListResponse:
    service = _device_service(repository)
    devices = await service.list(tenant_id)
    return DeviceListResponse(items=[_to_device_response(device) for device in devices])


@router.post(
    "",
    response_model=DeviceResponse,
    status_code=status.HTTP_201_CREATED,
    responses={403: {"model": ErrorResponse}, 409: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
)
async def create_device(
    tenant_id: str,
    payload: DeviceCreate,
    repository: DomainRepositoryDep,
    _access: TenantAccess = Depends(require_tenant_role(*tuple(WRITE_ROLES))),
) -> DeviceResponse:
    service = _device_service(repository)
    try:
        device = await service.create(tenant_id, payload.pond_id, payload.name, payload.firmware_version)
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc) or "conflict") from exc
    return _to_device_response(device)


@router.patch(
    "/{device_id}",
    response_model=DeviceResponse,
    responses={403: {"model": ErrorResponse}, 404: {"model": ErrorResponse}, 409: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
)
async def update_device(
    tenant_id: str,
    device_id: str,
    payload: DeviceUpdate,
    repository: DomainRepositoryDep,
    _access: TenantAccess = Depends(require_tenant_role(*tuple(WRITE_ROLES))),
) -> DeviceResponse:
    service = _device_service(repository)
    try:
        device = await service.update(
            tenant_id=tenant_id,
            device_id=device_id,
            expected_version=payload.expected_version,
            name=payload.name,
            pond_id=payload.pond_id,
            firmware_version=payload.firmware_version,
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc) or "not found") from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc) or "conflict") from exc
    return _to_device_response(device)
