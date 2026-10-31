"""Unmounted `/v2` Integration routes; M1-07 mounts them under `/v2`."""

from collections.abc import Coroutine
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Path, Response, status
from pydantic import AfterValidator

from limnopulse_api.api.dependencies import require_tenant_role
from limnopulse_api.api.redacted_validation import _RedactedValidationRoute
from limnopulse_api.api.v1.schemas.common import ErrorResponse
from limnopulse_api.api.v2.dependencies import DeviceReaderDep, IntegrationRepositoryDep
from limnopulse_api.api.v2.schemas.common import V2_ERROR_RESPONSES, IdempotencyKeyHeader
from limnopulse_api.api.v2.schemas.integrations import (
    DeviceIntegrationCreate,
    DeviceIntegrationListResponse,
    DeviceIntegrationResponse,
    IntegrationAccountCreate,
    IntegrationAccountListResponse,
    IntegrationAccountResponse,
)
from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.entities import TenantAccess
from limnopulse_api.domain.ids import require_device_id
from limnopulse_api.domain.roles import READ_ROLES, WRITE_ROLES
from limnopulse_api.services.integrations import (
    IdempotentCreateResult,
    IntegrationInputError,
    IntegrationService,
)


router = APIRouter(
    prefix="/tenants/{tenant_id}",
    tags=["v2-integrations"],
    route_class=_RedactedValidationRoute,
)

DevicePathId = Annotated[str, Path(), AfterValidator(require_device_id)]
TenantReaderDep = Annotated[TenantAccess, Depends(require_tenant_role(*tuple(READ_ROLES)))]
TenantWriterDep = Annotated[TenantAccess, Depends(require_tenant_role(*tuple(WRITE_ROLES)))]
_RESPONSES = {403: {"model": ErrorResponse}, **V2_ERROR_RESPONSES}


def _created(result: IdempotentCreateResult) -> Response:
    return Response(
        content=result.snapshot_json,
        status_code=status.HTTP_201_CREATED,
        media_type="application/json",
    )


async def _run_create(create: Coroutine[Any, Any, IdempotentCreateResult]) -> Response:
    try:
        return _created(await create)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail="not found") from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc) or "conflict") from exc
    except IntegrationInputError as exc:
        raise HTTPException(status_code=422, detail="invalid request") from exc


@router.get(
    "/integration-accounts",
    response_model=IntegrationAccountListResponse,
    responses=_RESPONSES,
)
async def list_integration_accounts(
    tenant_id: str,
    repository: IntegrationRepositoryDep,
    _access: TenantReaderDep,
) -> IntegrationAccountListResponse:
    accounts = await IntegrationService(repository).list_integration_accounts(tenant_id)
    return IntegrationAccountListResponse(
        items=[IntegrationAccountResponse.from_domain(account) for account in accounts]
    )


@router.post(
    "/integration-accounts",
    response_model=IntegrationAccountResponse,
    status_code=status.HTTP_201_CREATED,
    responses=_RESPONSES,
)
async def create_integration_account(
    tenant_id: str,
    payload: IntegrationAccountCreate,
    idempotency_key: IdempotencyKeyHeader,
    repository: IntegrationRepositoryDep,
    _access: TenantWriterDep,
) -> Response:
    service = IntegrationService(repository)
    return await _run_create(
        service.create_integration_account(tenant_id, payload, idempotency_key)
    )


@router.get(
    "/devices/{device_id}/integrations",
    response_model=DeviceIntegrationListResponse,
    responses=_RESPONSES,
)
async def list_device_integrations(
    tenant_id: str,
    device_id: DevicePathId,
    repository: IntegrationRepositoryDep,
    device_reader: DeviceReaderDep,
    _access: TenantReaderDep,
) -> DeviceIntegrationListResponse:
    service = IntegrationService(repository, device_reader)
    try:
        integrations = await service.list_device_integrations(tenant_id, device_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail="not found") from exc
    return DeviceIntegrationListResponse(
        items=[DeviceIntegrationResponse.from_domain(item) for item in integrations]
    )


@router.post(
    "/devices/{device_id}/integrations",
    response_model=DeviceIntegrationResponse,
    status_code=status.HTTP_201_CREATED,
    responses=_RESPONSES,
)
async def create_device_integration(
    tenant_id: str,
    device_id: DevicePathId,
    payload: DeviceIntegrationCreate,
    idempotency_key: IdempotencyKeyHeader,
    repository: IntegrationRepositoryDep,
    device_reader: DeviceReaderDep,
    _access: TenantWriterDep,
) -> Response:
    service = IntegrationService(repository, device_reader)
    return await _run_create(
        service.create_device_integration(tenant_id, device_id, payload, idempotency_key)
    )
