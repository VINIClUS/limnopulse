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
    NotificationPreferenceTelegramView,
    NotificationPreferenceView,
    email_is_effectively_enabled,
    mask_email_address,
)
from limnopulse_api.domain.telegram import (
    TelegramBinding,
    TelegramBindingRequest,
    TelegramDestination,
    telegram_binding_view,
    telegram_is_effectively_enabled,
)
from limnopulse_api.repositories.notification_preferences import (
    NotificationPreferenceRepository,
)
from limnopulse_api.services.cognito_identity import VerifiedEmailIdentity


class TelegramPreferenceRepository(Protocol):
    async def get_current(
        self,
        tenant_id: str,
        recipient_id: str,
    ) -> tuple[TelegramBinding | None, TelegramBindingRequest | None, TelegramDestination | None]:
        raise NotImplementedError


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
        telegram_repository: TelegramPreferenceRepository | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.repository = repository
        self.identity_verifier = identity_verifier
        self.telegram_repository = telegram_repository
        self.clock = clock

    async def get(self, tenant_id: str, cognito_sub: str) -> NotificationPreferenceView:
        preference = await self.repository.get(tenant_id, cognito_sub)
        binding_state = await self._telegram_state(tenant_id, cognito_sub)
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
                telegram=self._telegram_view(False, *binding_state),
                minimum_severity=AlertSeverity.CRITICAL,
            )

        deliverability_record = None
        if preference.email_address is not None:
            deliverability_record = await self.repository.get_email_deliverability(
                preference.email_address
            )
        return self._view(preference, deliverability_record, binding_state)

    async def put(
        self,
        tenant_id: str,
        principal: Principal,
        *,
        expected_version: int | None,
        email_enabled: bool,
        telegram_enabled: bool | None = None,
        minimum_severity: AlertSeverity,
        audit: AuditContext,
    ) -> NotificationPreferenceView:
        existing = await self.repository.get(tenant_id, principal.cognito_sub)
        if expected_version is None:
            if existing is not None:
                raise ConflictError("notification preference already exists")
        elif existing is None or existing.version != expected_version:
            raise ConflictError("notification preference version conflict")

        desired_telegram_enabled = (
            False
            if existing is None and telegram_enabled is None
            else (
                existing.telegram_enabled
                if telegram_enabled is None and existing is not None
                else bool(telegram_enabled)
            )
        )
        binding_state = await self._telegram_state(tenant_id, principal.cognito_sub)
        binding, _, destination = binding_state
        if desired_telegram_enabled and not telegram_is_effectively_enabled(
            True,
            binding,
            destination,
        ):
            raise ConflictError("verified Telegram binding required")

        identity = None
        if email_enabled or (existing is None and telegram_enabled is None):
            if self.identity_verifier is None:
                raise IdentityServiceUnavailableError("identity verifier unavailable")
            identity = await self.identity_verifier.verify(principal)

        desired_email_address = (
            identity.address
            if identity is not None
            else (existing.email_address if existing is not None else None)
        )
        desired_email_verified = (
            identity.verified
            if identity is not None
            else (existing.email_verified if existing is not None else False)
        )
        desired_identity_source = (
            identity.identity_source
            if identity is not None
            else (existing.identity_source if existing is not None else None)
        )
        if (
            existing is not None
            and existing.email_enabled is email_enabled
            and existing.email_address == desired_email_address
            and existing.email_verified is desired_email_verified
            and existing.identity_source == desired_identity_source
            and existing.telegram_enabled is desired_telegram_enabled
            and existing.minimum_severity is minimum_severity
        ):
            deliverability_record = None
            if existing.email_address is not None:
                deliverability_record = await self.repository.get_email_deliverability(
                    existing.email_address
                )
            return self._view(existing, deliverability_record, binding_state)

        now = self.clock()
        preference = NotificationPreference(
            tenant_id=tenant_id,
            cognito_sub=principal.cognito_sub,
            version=1 if expected_version is None else expected_version + 1,
            email_enabled=email_enabled,
            email_address=desired_email_address,
            email_verified=desired_email_verified,
            checked_at=(
                identity.checked_at
                if identity is not None
                else (existing.checked_at if existing is not None else None)
            ),
            identity_source=desired_identity_source,
            telegram_enabled=desired_telegram_enabled,
            minimum_severity=minimum_severity,
            created_at=(
                existing.created_at
                if expected_version is not None and existing is not None
                else now
            ),
            updated_at=now,
        )
        deliverability_record = None
        if preference.email_address is not None:
            deliverability_record = await self.repository.get_email_deliverability(
                preference.email_address
            )
        if (
            deliverability_record is None
            and existing is not None
            and preference.email_address is not None
            and existing.email_address is not None
            and existing.email_address != preference.email_address
            and existing.email_address.lower() == preference.email_address.lower()
        ):
            deliverability_record = await self.repository.get_email_deliverability(
                existing.email_address
            )
        saved = await self.repository.save(
            preference,
            expected_version,
            audit,
            previous=existing,
        )
        return self._view(saved, deliverability_record, binding_state)

    def _view(
        self,
        preference: NotificationPreference,
        deliverability_record: EmailDeliverabilityRecord | None,
        binding_state: tuple[
            TelegramBinding | None,
            TelegramBindingRequest | None,
            TelegramDestination | None,
        ],
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
                address=(
                    mask_email_address(preference.email_address)
                    if preference.email_address is not None
                    else None
                ),
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
            telegram=self._telegram_view(preference.telegram_enabled, *binding_state),
            minimum_severity=preference.minimum_severity,
        )

    async def _telegram_state(
        self,
        tenant_id: str,
        recipient_id: str,
    ) -> tuple[TelegramBinding | None, TelegramBindingRequest | None, TelegramDestination | None]:
        if self.telegram_repository is None:
            return None, None, None
        binding, pending, destination = await self.telegram_repository.get_current(
            tenant_id,
            recipient_id,
        )
        return binding, pending, destination

    def _telegram_view(
        self,
        enabled: bool,
        binding: TelegramBinding | None,
        pending: TelegramBindingRequest | None,
        destination: TelegramDestination | None,
    ) -> NotificationPreferenceTelegramView:
        view = telegram_binding_view(
            enabled=enabled,
            binding=binding,
            pending=pending,
            destination=destination,
            now=self.clock().astimezone(UTC).replace(microsecond=0),
        )
        return NotificationPreferenceTelegramView.from_binding_view(enabled, view)
