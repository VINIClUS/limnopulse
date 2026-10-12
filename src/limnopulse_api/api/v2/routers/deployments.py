"""Unmounted `/v2` temporal Deployment router; M1-07 mounts it under `/v2`.

History is listed only per Component partition: `component_id` is required.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from pydantic import AfterValidator

from limnopulse_api.api.dependencies import require_tenant_role
from limnopulse_api.api.v1.schemas.common import ErrorResponse
from limnopulse_api.api.v2.dependencies import (
    ComponentReaderDep,
    DeploymentRepositoryDep,
    SiteAssetReaderDep,
)
from limnopulse_api.api.v2.routers.devices import V2JsonSafeRoute
from limnopulse_api.api.v2.schemas.common import V2_ERROR_RESPONSES, IdempotencyKeyHeader
from limnopulse_api.api.v2.schemas.deployments import (
    V2DeploymentEndRequest,
    V2DeploymentListResponse,
    V2DeploymentPathId,
    V2DeploymentResponse,
    V2DeploymentStartRequest,
)
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.entities import TenantAccess
from limnopulse_api.domain.ids import require_canonical_id
from limnopulse_api.domain.roles import READ_ROLES, WRITE_ROLES
from limnopulse_api.services.deployments import DeploymentInputError, DeploymentService

router = APIRouter(
    prefix="/tenants/{tenant_id}/deployments",
    tags=["v2-deployments"],
    route_class=V2JsonSafeRoute,
)

_RESPONSES: dict[int | str, dict[str, object]] = {
    403: {"model": ErrorResponse},
    **V2_ERROR_RESPONSES,
}


def _require_component_id(value: str) -> str:
    return require_canonical_id(value, "cmp")


ComponentQueryId = Annotated[str, Query(), AfterValidator(_require_component_id)]


def _http_error(exc: NotFoundError | ConflictError | DeploymentInputError) -> HTTPException:
    if isinstance(exc, NotFoundError):
        return HTTPException(status_code=404, detail="not found")
    if isinstance(exc, DeploymentInputError):
        return HTTPException(status_code=422, detail=str(exc))
    return HTTPException(status_code=409, detail=str(exc) or "conflict")


def _service(
    deployments: DeploymentRepositoryDep,
    components: ComponentReaderDep,
    sites_assets: SiteAssetReaderDep,
) -> DeploymentService:
    return DeploymentService(deployments, components, sites_assets)


DeploymentServiceDep = Annotated[DeploymentService, Depends(_service)]
TenantReaderDep = Annotated[TenantAccess, Depends(require_tenant_role(*READ_ROLES))]
TenantWriterDep = Annotated[TenantAccess, Depends(require_tenant_role(*WRITE_ROLES))]


@router.get("", response_model=V2DeploymentListResponse, responses=_RESPONSES)
async def list_deployments(
    tenant_id: str,
    component_id: ComponentQueryId,
    service: DeploymentServiceDep,
    _access: TenantReaderDep,
) -> V2DeploymentListResponse:
    try:
        deployments = await service.list(tenant_id, component_id)
    except NotFoundError as exc:
        raise _http_error(exc) from exc
    return V2DeploymentListResponse(
        items=[V2DeploymentResponse.from_domain(item) for item in deployments]
    )


@router.post(
    "",
    response_model=V2DeploymentResponse,
    status_code=status.HTTP_201_CREATED,
    responses=_RESPONSES,
)
async def start_deployment(
    tenant_id: str,
    payload: V2DeploymentStartRequest,
    idempotency_key: IdempotencyKeyHeader,
    service: DeploymentServiceDep,
    _access: TenantWriterDep,
) -> Response:
    try:
        outcome = await service.start(tenant_id, payload, idempotency_key)
    except (NotFoundError, ConflictError, DeploymentInputError) as exc:
        raise _http_error(exc) from exc
    return Response(
        content=outcome.snapshot_json,
        status_code=status.HTTP_201_CREATED,
        media_type="application/json",
    )


@router.post(
    "/{deployment_id}/end",
    response_model=V2DeploymentResponse,
    responses=_RESPONSES,
)
async def end_deployment(
    tenant_id: str,
    deployment_id: V2DeploymentPathId,
    payload: V2DeploymentEndRequest,
    service: DeploymentServiceDep,
    _access: TenantWriterDep,
) -> V2DeploymentResponse:
    try:
        deployment = await service.end(tenant_id, deployment_id, payload)
    except (NotFoundError, ConflictError, DeploymentInputError) as exc:
        raise _http_error(exc) from exc
    return V2DeploymentResponse.from_domain(deployment)
