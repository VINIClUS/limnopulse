from fastapi import APIRouter, Depends, HTTPException, Request

from limnopulse_api.api.dependencies import AlertEventRepositoryDep, require_tenant_role
from limnopulse_api.api.v1.schemas.alert_events import (
    AlertEventListResponse,
    AlertEventResponse,
    AlertEventTransitionRequest,
)
from limnopulse_api.api.v1.schemas.common import ErrorResponse
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.alert_events import AlertEvent
from limnopulse_api.domain.alerts import AuditContext
from limnopulse_api.domain.entities import TenantAccess
from limnopulse_api.domain.roles import READ_ROLES, TenantRole, WRITE_ROLES
from limnopulse_api.services.alert_events import AlertEventService


router = APIRouter(prefix="/tenants/{tenant_id}/alert-events", tags=["alert-events"])
ACKNOWLEDGE_ROLES = frozenset({TenantRole.OWNER, TenantRole.ADMIN, TenantRole.MEMBER})


def _service(repository: AlertEventRepositoryDep) -> AlertEventService:
    return AlertEventService(repository)


def _response(event: AlertEvent) -> AlertEventResponse:
    return AlertEventResponse.model_validate(event.model_dump(mode="json"))


def _audit_context(request: Request, access: TenantAccess) -> AuditContext:
    return AuditContext(
        actor_id=access.principal.cognito_sub,
        ip=request.client.host if request.client is not None else None,
        user_agent=request.headers.get("user-agent"),
    )


@router.get("", response_model=AlertEventListResponse)
async def list_alert_events(
    tenant_id: str,
    repository: AlertEventRepositoryDep,
    _access: TenantAccess = Depends(require_tenant_role(*tuple(READ_ROLES))),
) -> AlertEventListResponse:
    events = await _service(repository).list(tenant_id)
    return AlertEventListResponse(items=[_response(event) for event in events])


@router.get(
    "/{event_id}",
    response_model=AlertEventResponse,
    responses={404: {"model": ErrorResponse}},
)
async def get_alert_event(
    tenant_id: str,
    event_id: str,
    repository: AlertEventRepositoryDep,
    _access: TenantAccess = Depends(require_tenant_role(*tuple(READ_ROLES))),
) -> AlertEventResponse:
    try:
        return _response(await _service(repository).get(tenant_id, event_id))
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post(
    "/{event_id}/acknowledge",
    response_model=AlertEventResponse,
    responses={404: {"model": ErrorResponse}, 409: {"model": ErrorResponse}},
)
async def acknowledge_alert_event(
    tenant_id: str,
    event_id: str,
    payload: AlertEventTransitionRequest,
    request: Request,
    repository: AlertEventRepositoryDep,
    access: TenantAccess = Depends(require_tenant_role(*tuple(ACKNOWLEDGE_ROLES))),
) -> AlertEventResponse:
    try:
        event = await _service(repository).acknowledge(
            tenant_id,
            event_id,
            payload.expected_version,
            _audit_context(request, access),
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _response(event)


@router.post(
    "/{event_id}/resolve",
    response_model=AlertEventResponse,
    responses={404: {"model": ErrorResponse}, 409: {"model": ErrorResponse}},
)
async def resolve_alert_event(
    tenant_id: str,
    event_id: str,
    payload: AlertEventTransitionRequest,
    request: Request,
    repository: AlertEventRepositoryDep,
    access: TenantAccess = Depends(require_tenant_role(*tuple(WRITE_ROLES))),
) -> AlertEventResponse:
    try:
        event = await _service(repository).resolve(
            tenant_id,
            event_id,
            payload.expected_version,
            _audit_context(request, access),
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _response(event)
