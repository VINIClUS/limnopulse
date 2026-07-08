from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from limnopulse_api.auth.models import Principal
from limnopulse_api.domain.roles import TenantRole


class VersionedEntity(BaseModel):
    model_config = ConfigDict(frozen=True)

    created_at: datetime
    updated_at: datetime
    version: int
    schema_version: int = 1
    status: str = "active"


class Tenant(VersionedEntity):
    tenant_id: str
    name: str
    settings: dict[str, object] = Field(default_factory=dict)


class Pond(VersionedEntity):
    tenant_id: str
    pond_id: str
    name: str
    description: str | None = None


class Device(VersionedEntity):
    tenant_id: str
    pond_id: str
    device_id: str
    name: str
    auth_type: str = "mtls"
    firmware_version: str | None = None


class Membership(VersionedEntity):
    tenant_id: str
    cognito_sub: str
    role: TenantRole


class TenantAccess(BaseModel):
    model_config = ConfigDict(frozen=True)

    principal: Principal
    membership: Membership
