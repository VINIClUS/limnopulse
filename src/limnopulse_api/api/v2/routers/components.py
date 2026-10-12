"""Unmounted `/v2` Component and append-only Capability router; M1-07 mounts it under `/v2`.

Capability declarations have no PATCH or DELETE route in M1.
"""

from fastapi import APIRouter, Depends, HTTPException, Response, status

from limnopulse_api.api.dependencies import require_tenant_role
from limnopulse_api.api.v1.schemas.common import ErrorResponse
from limnopulse_api.api.v2.dependencies import DeviceComponentRepositoryDep
from limnopulse_api.api.v2.routers.devices import V2JsonSafeRoute
from limnopulse_api.api.v2.schemas.capabilities import (
    V2CapabilityCreateRequest,
    V2CapabilityListResponse,
    V2CapabilityResponse,
)
from limnopulse_api.api.v2.schemas.common import V2_ERROR_RESPONSES, IdempotencyKeyHeader
from limnopulse_api.api.v2.schemas.components import (
    V2ComponentCreateRequest,
    V2ComponentListResponse,
    V2ComponentPathId,
    V2ComponentResponse,
    V2ComponentUpdateRequest,
)
from limnopulse_api.api.v2.schemas.devices import V2DevicePathId
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.entities import TenantAccess
from limnopulse_api.domain.roles import READ_ROLES, WRITE_ROLES
from limnopulse_api.services.capabilities import CapabilityService
from limnopulse_api.services.components import ComponentService

router = APIRouter(
    prefix="/tenants/{tenant_id}", tags=["v2-components"], route_class=V2JsonSafeRoute
)

_RESPONSES: dict[int | str, dict[str, object]] = {
    403: {"model": ErrorResponse},
    **V2_ERROR_RESPONSES,
}


def _http_error(exc: NotFoundError | ConflictError) -> HTTPException:
    if isinstance(exc, NotFoundError):
        return HTTPException(status_code=404, detail="not found")
    return HTTPException(status_code=409, detail=str(exc))


def _created(snapshot_json: str) -> Response:
    return Response(
        content=snapshot_json,
        status_code=status.HTTP_201_CREATED,
        media_type="application/json",
    )


@router.get(
    "/devices/{device_id}/components",
    response_model=V2ComponentListResponse,
    responses=_RESPONSES,
)
async def list_device_components(
    tenant_id: str,
    device_id: V2DevicePathId,
    repository: DeviceComponentRepositoryDep,
    _access: TenantAccess = Depends(require_tenant_role(*READ_ROLES)),
) -> V2ComponentListResponse:
    try:
        components = await ComponentService(repository).list(tenant_id, device_id)
    except NotFoundError as exc:
        raise _http_error(exc) from exc
    return V2ComponentListResponse(
        items=[V2ComponentResponse.from_domain(item) for item in components]
    )


@router.post(
    "/devices/{device_id}/components",
    response_model=V2ComponentResponse,
    status_code=status.HTTP_201_CREATED,
    responses=_RESPONSES,
)
async def create_device_component(
    tenant_id: str,
    device_id: V2DevicePathId,
    payload: V2ComponentCreateRequest,
    idempotency_key: IdempotencyKeyHeader,
    repository: DeviceComponentRepositoryDep,
    _access: TenantAccess = Depends(require_tenant_role(*WRITE_ROLES)),
) -> Response:
    try:
        outcome = await ComponentService(repository).create(
            tenant_id, device_id, payload, idempotency_key
        )
    except (NotFoundError, ConflictError) as exc:
        raise _http_error(exc) from exc
    return _created(outcome.snapshot_json)


@router.get(
    "/components/{component_id}",
    response_model=V2ComponentResponse,
    responses=_RESPONSES,
)
async def get_component(
    tenant_id: str,
    component_id: V2ComponentPathId,
    repository: DeviceComponentRepositoryDep,
    _access: TenantAccess = Depends(require_tenant_role(*READ_ROLES)),
) -> V2ComponentResponse:
    component = await ComponentService(repository).get(tenant_id, component_id)
    if component is None:
        raise HTTPException(status_code=404, detail="not found")
    return V2ComponentResponse.from_domain(component)


@router.patch(
    "/components/{component_id}",
    response_model=V2ComponentResponse,
    responses=_RESPONSES,
)
async def update_component(
    tenant_id: str,
    component_id: V2ComponentPathId,
    payload: V2ComponentUpdateRequest,
    repository: DeviceComponentRepositoryDep,
    _access: TenantAccess = Depends(require_tenant_role(*WRITE_ROLES)),
) -> V2ComponentResponse:
    try:
        component = await ComponentService(repository).update(tenant_id, component_id, payload)
    except (NotFoundError, ConflictError) as exc:
        raise _http_error(exc) from exc
    return V2ComponentResponse.from_domain(component)


@router.get(
    "/components/{component_id}/capabilities",
    response_model=V2CapabilityListResponse,
    responses=_RESPONSES,
)
async def list_component_capabilities(
    tenant_id: str,
    component_id: V2ComponentPathId,
    repository: DeviceComponentRepositoryDep,
    _access: TenantAccess = Depends(require_tenant_role(*READ_ROLES)),
) -> V2CapabilityListResponse:
    try:
        declarations = await CapabilityService(repository).list(tenant_id, component_id)
    except NotFoundError as exc:
        raise _http_error(exc) from exc
    return V2CapabilityListResponse(
        items=[V2CapabilityResponse.from_domain(item) for item in declarations]
    )


@router.post(
    "/components/{component_id}/capabilities",
    response_model=V2CapabilityResponse,
    status_code=status.HTTP_201_CREATED,
    responses=_RESPONSES,
)
async def create_component_capability(
    tenant_id: str,
    component_id: V2ComponentPathId,
    payload: V2CapabilityCreateRequest,
    idempotency_key: IdempotencyKeyHeader,
    repository: DeviceComponentRepositoryDep,
    _access: TenantAccess = Depends(require_tenant_role(*WRITE_ROLES)),
) -> Response:
    try:
        outcome = await CapabilityService(repository).create(
            tenant_id, component_id, payload, idempotency_key
        )
    except (NotFoundError, ConflictError) as exc:
        raise _http_error(exc) from exc
    return _created(outcome.snapshot_json)
