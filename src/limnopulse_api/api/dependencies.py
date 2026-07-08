from collections.abc import Callable
from typing import Annotated, Any

from fastapi import Depends, HTTPException, Request

from limnopulse_api.auth.models import Principal
from limnopulse_api.auth.providers import build_auth_provider
from limnopulse_api.core.errors import AuthError
from limnopulse_api.domain.entities import TenantAccess
from limnopulse_api.domain.roles import READ_ROLES, TenantRole
from limnopulse_api.repositories.domain import DomainRepository
from limnopulse_api.services.memberships import MembershipService


async def get_current_principal(request: Request) -> Principal:
    provider = build_auth_provider(request.app.state.settings)
    try:
        return await provider.authenticate(request)
    except AuthError as exc:
        raise HTTPException(status_code=401, detail="authentication required") from exc


PrincipalDep = Annotated[Principal, Depends(get_current_principal)]


def _get_state_dependency(request: Request, attribute_name: str) -> Any:
    try:
        return getattr(request.app.state, attribute_name)
    except AttributeError as exc:
        raise HTTPException(status_code=503, detail="service unavailable") from exc


def get_domain_repository(request: Request) -> DomainRepository:
    return _get_state_dependency(request, "domain_repository")


def get_membership_service(request: Request) -> MembershipService:
    return _get_state_dependency(request, "membership_service")


DomainRepositoryDep = Annotated[DomainRepository, Depends(get_domain_repository)]
MembershipServiceDep = Annotated[MembershipService, Depends(get_membership_service)]


async def require_tenant_access(
    tenant_id: str,
    principal: PrincipalDep,
    membership_service: MembershipServiceDep,
) -> TenantAccess:
    membership = await membership_service.get_active_membership(principal.cognito_sub, tenant_id)
    if membership is None:
        raise HTTPException(status_code=403, detail="tenant access denied")
    return TenantAccess(principal=principal, membership=membership)


TenantAccessDep = Annotated[TenantAccess, Depends(require_tenant_access)]


def require_tenant_role(*allowed_roles: TenantRole) -> Callable[..., TenantAccess]:
    async def dependency(access: TenantAccessDep) -> TenantAccess:
        if access.membership.role not in allowed_roles:
            raise HTTPException(status_code=403, detail="tenant access denied")
        return access

    return dependency


def require_tenant_read_access() -> Callable[..., TenantAccess]:
    return require_tenant_role(*tuple(READ_ROLES))
