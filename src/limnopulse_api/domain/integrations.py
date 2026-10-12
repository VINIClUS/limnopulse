from enum import StrEnum

from limnopulse_api.domain.v2_common import (
    DeviceId,
    ProviderKey,
    SafeId,
    Sha256Hex,
    V2VersionedEntity,
    ensure_status_transition,
)


class IntegrationAccountStatus(StrEnum):
    PENDING = "pending"
    ACTIVE = "active"
    ERROR = "error"
    REVOKED = "revoked"


class DeviceIntegrationStatus(StrEnum):
    PROVISIONING = "provisioning"
    ACTIVE = "active"
    ERROR = "error"
    DECOMMISSIONED = "decommissioned"


_ACCOUNT_TRANSITIONS = {
    IntegrationAccountStatus.PENDING: frozenset(
        {
            IntegrationAccountStatus.ACTIVE,
            IntegrationAccountStatus.ERROR,
            IntegrationAccountStatus.REVOKED,
        }
    ),
    IntegrationAccountStatus.ACTIVE: frozenset(
        {IntegrationAccountStatus.ERROR, IntegrationAccountStatus.REVOKED}
    ),
    IntegrationAccountStatus.ERROR: frozenset(
        {IntegrationAccountStatus.ACTIVE, IntegrationAccountStatus.REVOKED}
    ),
}
_DEVICE_INTEGRATION_TRANSITIONS = {
    DeviceIntegrationStatus.PROVISIONING: frozenset(
        {
            DeviceIntegrationStatus.ACTIVE,
            DeviceIntegrationStatus.ERROR,
            DeviceIntegrationStatus.DECOMMISSIONED,
        }
    ),
    DeviceIntegrationStatus.ACTIVE: frozenset(
        {DeviceIntegrationStatus.ERROR, DeviceIntegrationStatus.DECOMMISSIONED}
    ),
    DeviceIntegrationStatus.ERROR: frozenset(
        {DeviceIntegrationStatus.ACTIVE, DeviceIntegrationStatus.DECOMMISSIONED}
    ),
}


class IntegrationAccount(V2VersionedEntity):
    tenant_id: SafeId
    integration_account_id: SafeId
    provider_key: ProviderKey
    display_name: str
    credential_reference: str | None = None
    public_identifier: str | None = None
    status: IntegrationAccountStatus = IntegrationAccountStatus.PENDING


class DeviceIntegration(V2VersionedEntity):
    tenant_id: SafeId
    device_integration_id: SafeId
    device_id: DeviceId
    integration_account_id: SafeId
    provider_key: ProviderKey
    external_identity_hash: Sha256Hex
    status: DeviceIntegrationStatus = DeviceIntegrationStatus.PROVISIONING


def ensure_integration_account_transition(
    current: IntegrationAccountStatus, target: IntegrationAccountStatus
) -> None:
    ensure_status_transition(current, target, _ACCOUNT_TRANSITIONS, entity="integration account")


def ensure_device_integration_transition(
    current: DeviceIntegrationStatus, target: DeviceIntegrationStatus
) -> None:
    ensure_status_transition(
        current, target, _DEVICE_INTEGRATION_TRANSITIONS, entity="device integration"
    )
