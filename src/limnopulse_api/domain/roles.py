from enum import StrEnum


class TenantRole(StrEnum):
    OWNER = "owner"
    ADMIN = "admin"
    MEMBER = "member"
    VIEWER = "viewer"


READ_ROLES = frozenset({TenantRole.OWNER, TenantRole.ADMIN, TenantRole.MEMBER, TenantRole.VIEWER})
WRITE_ROLES = frozenset({TenantRole.OWNER, TenantRole.ADMIN})
