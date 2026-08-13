from fastapi import APIRouter, Response, status

from limnopulse_api.api.dependencies import TelegramBindingServiceDep, TenantAccessDep
from limnopulse_api.api.v1.schemas.telegram_bindings import (
    TelegramBindingResponse,
    TelegramBindingTokenResponse,
)

router = APIRouter(prefix="/tenants/{tenant_id}/me", tags=["telegram-bindings"])


@router.get("/telegram-binding", response_model=TelegramBindingResponse)
async def get_telegram_binding(
    tenant_id: str,
    access: TenantAccessDep,
    service: TelegramBindingServiceDep,
) -> TelegramBindingResponse:
    view = await service.get(tenant_id, access.principal.cognito_sub)
    return TelegramBindingResponse.model_validate(view.model_dump(mode="python"))


@router.post(
    "/telegram-binding-token",
    response_model=TelegramBindingTokenResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_telegram_binding_token(
    tenant_id: str,
    access: TenantAccessDep,
    service: TelegramBindingServiceDep,
) -> TelegramBindingTokenResponse:
    issued = await service.issue(tenant_id, access.principal.cognito_sub)
    return TelegramBindingTokenResponse(
        request_id=issued.request_id,
        token=issued.token,
        deep_link=issued.deep_link,
        expires_at=issued.expires_at,
    )


@router.delete("/telegram-binding", status_code=status.HTTP_204_NO_CONTENT)
async def delete_telegram_binding(
    tenant_id: str,
    access: TenantAccessDep,
    service: TelegramBindingServiceDep,
) -> Response:
    await service.revoke(tenant_id, access.principal.cognito_sub)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
