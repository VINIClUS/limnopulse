"""Strict `/v2` Integration schemas.

Create bodies forbid unknown fields, so secret material (`api_key`, `secret`, `password`,
`token`, ...) is rejected instead of silently dropped. `external_identity` is write-only:
responses expose only `external_identity_hash`.
"""

from typing import Annotated, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field

from limnopulse_api.api.v2.schemas.common import V2VersionedResponse
from limnopulse_api.domain.ids import require_canonical_id
from limnopulse_api.domain.integrations import DeviceIntegration, IntegrationAccount
from limnopulse_api.domain.v2_common import PROVIDER_KEY_PATTERN

PROVIDER_KEY_MAX_LENGTH = 128
EXTERNAL_IDENTITY_MAX_LENGTH = 512


def _require_integration_account_id(value: str) -> str:
    return require_canonical_id(value, "ia")


IntegrationAccountId = Annotated[str, AfterValidator(_require_integration_account_id)]
ProviderKeyField = Annotated[
    str, Field(pattern=PROVIDER_KEY_PATTERN, max_length=PROVIDER_KEY_MAX_LENGTH)
]


class IntegrationAccountCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider_key: ProviderKeyField
    display_name: str = Field(min_length=1, max_length=120)
    credential_reference: str | None = Field(
        default=None,
        min_length=1,
        max_length=256,
        description="Opaque pointer to a secret stored elsewhere; never the secret itself.",
    )
    public_identifier: str | None = Field(default=None, min_length=1, max_length=256)


class DeviceIntegrationCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    integration_account_id: IntegrationAccountId
    external_identity: str = Field(
        min_length=1,
        max_length=EXTERNAL_IDENTITY_MAX_LENGTH,
        description="Write-only provider identity; stored and returned only as a SHA-256 digest.",
    )


class IntegrationAccountResponse(V2VersionedResponse):
    integration_account_id: str
    provider_key: str
    display_name: str
    credential_reference: str | None = None
    public_identifier: str | None = None
    status: Literal["pending", "active", "error", "revoked"]

    @classmethod
    def from_domain(cls, account: IntegrationAccount) -> "IntegrationAccountResponse":
        return cls(
            tenant_id=account.tenant_id,
            integration_account_id=account.integration_account_id,
            provider_key=account.provider_key,
            display_name=account.display_name,
            credential_reference=account.credential_reference,
            public_identifier=account.public_identifier,
            status=account.status.value,
            created_at=account.created_at,
            updated_at=account.updated_at,
            version=account.version,
        )


class IntegrationAccountListResponse(BaseModel):
    items: list[IntegrationAccountResponse]


class DeviceIntegrationResponse(V2VersionedResponse):
    device_integration_id: str
    device_id: str
    integration_account_id: str
    provider_key: str
    external_identity_hash: str
    status: Literal["provisioning", "active", "error", "decommissioned"]

    @classmethod
    def from_domain(cls, integration: DeviceIntegration) -> "DeviceIntegrationResponse":
        return cls(
            tenant_id=integration.tenant_id,
            device_integration_id=integration.device_integration_id,
            device_id=integration.device_id,
            integration_account_id=integration.integration_account_id,
            provider_key=integration.provider_key,
            external_identity_hash=integration.external_identity_hash,
            status=integration.status.value,
            created_at=integration.created_at,
            updated_at=integration.updated_at,
            version=integration.version,
        )


class DeviceIntegrationListResponse(BaseModel):
    items: list[DeviceIntegrationResponse]
