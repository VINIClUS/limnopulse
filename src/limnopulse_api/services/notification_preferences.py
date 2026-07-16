from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol

from limnopulse_api.auth.models import Principal
from limnopulse_api.core.errors import ConflictError, IdentityServiceUnavailableError
from limnopulse_api.domain.alerts import AlertSeverity, AuditContext
from limnopulse_api.domain.notification_preferences import (
    EmailDeliverability,
    EmailDeliverabilityRecord,
    NotificationPreference,
    NotificationPreferenceEmailView,
    NotificationPreferenceView,
    email_is_effectively_enabled,
    mask_email_address,
)
from limnopulse_api.repositories.notification_preferences import (
    NotificationPreferenceRepository,
)
from limnopulse_api.services.cognito_identity import VerifiedEmailIdentity


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


class IdentityVerifier(Protocol):
    async def verify(self, principal: Principal) -> VerifiedEmailIdentity:
        raise NotImplementedError


class NotificationPreferenceService:
    def __init__(
        self,
        repository: NotificationPreferenceRepository,
        identity_verifier: IdentityVerifier | None = None,
        *,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.repository = repository
        self.identity_verifier = identity_verifier
        self.clock = clock

    async def get(self, tenant_id: str, cognito_sub: str) -> NotificationPreferenceView:
        preference = await self.repository.get(tenant_id, cognito_sub)
        if preference is None:
            return NotificationPreferenceView(
                configured=False,
                version=None,
                email=NotificationPreferenceEmailView(
                    enabled=False,
                    address=None,
                    verified=False,
                    deliverability=EmailDeliverability.UNKNOWN,
                    suppression_reason=None,
                    effective_enabled=False,
                ),
                minimum_severity=AlertSeverity.CRITICAL,
            )

        deliverability_record = await self.repository.get_email_deliverability(
            preference.email_address
        )
        return self._view(preference, deliverability_record)

    async def put(
        self,
        tenant_id: str,
        principal: Principal,
        *,
        expected_version: int | None,
        email_enabled: bool,
        minimum_severity: AlertSeverity,
        audit: AuditContext,
    ) -> NotificationPreferenceView:
        existing = await self.repository.get(tenant_id, principal.cognito_sub)
        identity = None
        if expected_version is None or email_enabled:
            if self.identity_verifier is None:
                raise IdentityServiceUnavailableError("identity verifier unavailable")
            identity = await self.identity_verifier.verify(principal)
        elif existing is None:
            raise ConflictError("notification preference does not exist")

        if expected_version is not None and existing is None:
            raise ConflictError("notification preference does not exist")

        now = self.clock()
        preference = NotificationPreference(
            tenant_id=tenant_id,
            cognito_sub=principal.cognito_sub,
            version=1 if expected_version is None else expected_version + 1,
            email_enabled=email_enabled,
            email_address=(identity.address if identity is not None else existing.email_address),
            email_verified=(identity.verified if identity is not None else existing.email_verified),
            checked_at=(identity.checked_at if identity is not None else existing.checked_at),
            identity_source=(
                identity.identity_source if identity is not None else existing.identity_source
            ),
            minimum_severity=minimum_severity,
            created_at=(
                existing.created_at
                if expected_version is not None and existing is not None
                else now
            ),
            updated_at=now,
        )
        deliverability_record = await self.repository.get_email_deliverability(
            preference.email_address
        )
        saved = await self.repository.save(preference, expected_version, audit)
        return self._view(saved, deliverability_record)

    def _view(
        self,
        preference: NotificationPreference,
        deliverability_record: EmailDeliverabilityRecord | None,
    ) -> NotificationPreferenceView:
        deliverability = (
            deliverability_record.deliverability
            if deliverability_record is not None
            else EmailDeliverability.UNKNOWN
        )
        return NotificationPreferenceView(
            configured=True,
            version=preference.version,
            email=NotificationPreferenceEmailView(
                enabled=preference.email_enabled,
                address=mask_email_address(preference.email_address),
                verified=preference.email_verified,
                deliverability=deliverability,
                suppression_reason=(
                    deliverability_record.suppression_reason
                    if deliverability_record is not None
                    else None
                ),
                effective_enabled=email_is_effectively_enabled(
                    preference,
                    deliverability,
                ),
            ),
            minimum_severity=preference.minimum_severity,
        )
