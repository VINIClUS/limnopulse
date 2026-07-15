from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status

from limnopulse_api.api.dependencies import (
    AlertRuleRepositoryDep,
    DomainRepositoryDep,
    require_tenant_role,
)
from limnopulse_api.api.v1.schemas.alert_rules import (
    AlertRuleCreate,
    AlertRuleListResponse,
    AlertRuleReplace,
    AlertRuleReplacementResponse,
    AlertRuleResponse,
    AlertRuleUpdate,
)
from limnopulse_api.api.v1.schemas.common import ErrorResponse
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.alerts import AlertRule, AuditContext
from limnopulse_api.domain.entities import TenantAccess
from limnopulse_api.domain.roles import READ_ROLES, WRITE_ROLES
from limnopulse_api.services.alert_rules import AlertRuleService


router = APIRouter(prefix="/tenants/{tenant_id}/alert-rules", tags=["alert-rules"])


def _service(
    repository: AlertRuleRepositoryDep,
    domain_repository: DomainRepositoryDep,
) -> AlertRuleService:
    return AlertRuleService(repository, domain_repository)


def _response(rule: AlertRule) -> AlertRuleResponse:
    return AlertRuleResponse.model_validate(rule.model_dump(mode="json"))


def _audit_context(request: Request, access: TenantAccess) -> AuditContext:
    return AuditContext(
        actor_id=access.principal.cognito_sub,
        ip=request.client.host if request.client is not None else None,
        user_agent=request.headers.get("user-agent"),
    )


@router.get(
    "",
    response_model=AlertRuleListResponse,
    responses={403: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
)
async def list_alert_rules(
    tenant_id: str,
    repository: AlertRuleRepositoryDep,
    domain_repository: DomainRepositoryDep,
    _access: TenantAccess = Depends(require_tenant_role(*tuple(READ_ROLES))),
) -> AlertRuleListResponse:
    rules = await _service(repository, domain_repository).list(tenant_id)
    return AlertRuleListResponse(items=[_response(rule) for rule in rules])


@router.post(
    "",
    response_model=AlertRuleResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
)
async def create_alert_rule(
    tenant_id: str,
    payload: AlertRuleCreate,
    request: Request,
    repository: AlertRuleRepositoryDep,
    domain_repository: DomainRepositoryDep,
    access: TenantAccess = Depends(require_tenant_role(*tuple(WRITE_ROLES))),
) -> AlertRuleResponse:
    try:
        rule = await _service(repository, domain_repository).create(
            tenant_id,
            payload.model_dump(mode="python"),
            _audit_context(request, access),
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc) or "not found") from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc) or "conflict") from exc
    return _response(rule)


@router.patch(
    "/{rule_id}",
    response_model=AlertRuleResponse,
    responses={
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
)
async def update_alert_rule(
    tenant_id: str,
    rule_id: str,
    payload: AlertRuleUpdate,
    request: Request,
    repository: AlertRuleRepositoryDep,
    domain_repository: DomainRepositoryDep,
    access: TenantAccess = Depends(require_tenant_role(*tuple(WRITE_ROLES))),
) -> AlertRuleResponse:
    try:
        rule = await _service(repository, domain_repository).update(
            tenant_id,
            rule_id,
            payload.expected_version,
            payload.updates(),
            _audit_context(request, access),
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc) or "not found") from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc) or "conflict") from exc
    return _response(rule)


@router.post(
    "/{rule_id}/replace",
    response_model=AlertRuleReplacementResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
)
async def replace_alert_rule(
    tenant_id: str,
    rule_id: str,
    payload: AlertRuleReplace,
    request: Request,
    repository: AlertRuleRepositoryDep,
    domain_repository: DomainRepositoryDep,
    idempotency_key: Annotated[
        str,
        Header(alias="Idempotency-Key", min_length=8, max_length=128),
    ],
    access: TenantAccess = Depends(require_tenant_role(*tuple(WRITE_ROLES))),
) -> AlertRuleReplacementResponse:
    definition = payload.model_dump(
        mode="python",
        exclude={"expected_version"},
    )
    try:
        result = await _service(repository, domain_repository).replace(
            tenant_id,
            rule_id,
            payload.expected_version,
            definition,
            idempotency_key,
            _audit_context(request, access),
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc) or "not found") from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc) or "conflict") from exc
    return AlertRuleReplacementResponse(
        replaced=_response(result.replaced),
        replacement=_response(result.replacement),
    )
