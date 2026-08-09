from fastapi import APIRouter, HTTPException, Request

from limnopulse_api.api.dependencies import (
    CognitoIdentityVerifierDep,
    NotificationPreferenceRepositoryDep,
    TenantAccessDep,
)
from limnopulse_api.api.v1.schemas.common import ErrorResponse
from limnopulse_api.api.v1.schemas.notification_preferences import (
    NotificationPreferenceResponse,
    NotificationPreferenceUpdate,
)
from limnopulse_api.core.errors import (
    AuthError,
    ConflictError,
    IdentityEmailError,
    IdentityServiceUnavailableError,
)
from limnopulse_api.domain.alerts import AuditContext
from limnopulse_api.domain.entities import TenantAccess
from limnopulse_api.domain.notification_preferences import NotificationPreferenceView
from limnopulse_api.services.notification_preferences import NotificationPreferenceService

router = APIRouter(
    prefix="/tenants/{tenant_id}/me/notification-preferences",
    tags=["notification-preferences"],
)


def _service(
    repository: NotificationPreferenceRepositoryDep,
    identity_verifier: CognitoIdentityVerifierDep | None = None,
) -> NotificationPreferenceService:
    return NotificationPreferenceService(repository, identity_verifier)


def _response(view: NotificationPreferenceView) -> NotificationPreferenceResponse:
    return NotificationPreferenceResponse.model_validate(view.model_dump(mode="python"))


def _audit_context(request: Request, access: TenantAccess) -> AuditContext:
    return AuditContext(
        actor_id=access.principal.cognito_sub,
        ip=request.client.host if request.client is not None else None,
        user_agent=request.headers.get("user-agent"),
    )


@router.get(
    "",
    response_model=NotificationPreferenceResponse,
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
)
async def get_notification_preferences(
    tenant_id: str,
    access: TenantAccessDep,
    repository: NotificationPreferenceRepositoryDep,
) -> NotificationPreferenceResponse:
    view = await _service(repository).get(
        tenant_id,
        access.principal.cognito_sub,
    )
    return _response(view)


@router.put(
    "",
    response_model=NotificationPreferenceResponse,
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        422: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
)
async def put_notification_preferences(
    tenant_id: str,
    payload: NotificationPreferenceUpdate,
    request: Request,
    access: TenantAccessDep,
    repository: NotificationPreferenceRepositoryDep,
    identity_verifier: CognitoIdentityVerifierDep,
) -> NotificationPreferenceResponse:
    try:
        view = await _service(repository, identity_verifier).put(
            tenant_id,
            access.principal,
            expected_version=payload.expected_version,
            email_enabled=payload.email_enabled,
            minimum_severity=payload.minimum_severity,
            audit=_audit_context(request, access),
        )
    except AuthError as exc:
        raise HTTPException(status_code=401, detail="authentication required") from exc
    except IdentityEmailError as exc:
        raise HTTPException(status_code=422, detail="verified email required") from exc
    except IdentityServiceUnavailableError as exc:
        raise HTTPException(status_code=503, detail="service unavailable") from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail="version conflict") from exc
    return _response(view)
