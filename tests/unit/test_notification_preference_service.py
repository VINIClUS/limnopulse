from datetime import UTC, datetime

import pytest

from limnopulse_api.auth.models import Principal
from limnopulse_api.core.errors import ConflictError, IdentityEmailError
from limnopulse_api.domain.alerts import AlertSeverity, AuditContext
from limnopulse_api.domain.notification_preferences import (
    EmailDeliverability,
    EmailDeliverabilityRecord,
    NotificationPreference,
)
from limnopulse_api.domain.telegram import (
    TelegramBinding,
    TelegramBindingRequest,
    TelegramBindingStatus,
    TelegramDestination,
    TelegramDestinationStatus,
)
from limnopulse_api.services.cognito_identity import VerifiedEmailIdentity
from limnopulse_api.services.notification_preferences import NotificationPreferenceService

NOW = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)


class FakeNotificationPreferenceRepository:
    def __init__(
        self,
        preference: NotificationPreference | None = None,
        deliverability: EmailDeliverabilityRecord | None = None,
    ) -> None:
        self.preference = preference
        self.deliverability = deliverability
        self.get_calls: list[tuple[str, str]] = []
        self.deliverability_calls: list[str] = []
        self.save_calls: list[
            tuple[
                NotificationPreference,
                int | None,
                AuditContext,
                NotificationPreference | None,
            ]
        ] = []

    async def get(self, tenant_id: str, cognito_sub: str) -> NotificationPreference | None:
        self.get_calls.append((tenant_id, cognito_sub))
        return self.preference

    async def get_email_deliverability(
        self,
        address: str,
    ) -> EmailDeliverabilityRecord | None:
        self.deliverability_calls.append(address)
        return self.deliverability

    async def save(
        self,
        preference: NotificationPreference,
        expected_version: int | None,
        audit: AuditContext,
        *,
        previous: NotificationPreference | None,
    ) -> NotificationPreference:
        if expected_version is None:
            if self.preference is not None:
                raise ConflictError("preference already exists")
        elif self.preference is None or self.preference.version != expected_version:
            raise ConflictError("preference version conflict")
        self.save_calls.append((preference, expected_version, audit, previous))
        self.preference = preference
        return preference


class FakeTelegramBindingRepository:
    def __init__(
        self,
        *,
        verified: bool = False,
        active: bool = True,
    ) -> None:
        destination_id = TelegramDestination.id_for_chat(123)
        self.binding = (
            TelegramBinding(
                tenant_id="tnt_1",
                recipient_id="sub_1",
                destination_id=destination_id,
                status=TelegramBindingStatus.VERIFIED,
                verified_at=NOW,
                revoked_at=None,
                version=1,
                created_at=NOW,
                updated_at=NOW,
            )
            if verified
            else None
        )
        self.destination = (
            TelegramDestination(
                destination_id=destination_id,
                recipient_id="sub_1",
                chat_id=123,
                status=(
                    TelegramDestinationStatus.ACTIVE
                    if active
                    else TelegramDestinationStatus.SUPPRESSED
                ),
                suppression_reason=None if active else "user_stop",
                stopped_at=None if active else NOW,
                version=1,
                created_at=NOW,
                updated_at=NOW,
            )
            if verified
            else None
        )
        self.calls: list[tuple[str, str]] = []

    async def get_current(self, tenant_id: str, recipient_id: str):
        self.calls.append((tenant_id, recipient_id))
        return self.binding, None, self.destination


class DeliverabilityMustPrecedeCommitRepository(FakeNotificationPreferenceRepository):
    async def get_email_deliverability(
        self,
        address: str,
    ) -> EmailDeliverabilityRecord | None:
        if self.save_calls:
            raise AssertionError("deliverability lookup happened after commit")
        return await super().get_email_deliverability(address)


class CaseTransitionRepository(FakeNotificationPreferenceRepository):
    async def get_email_deliverability(
        self,
        address: str,
    ) -> EmailDeliverabilityRecord | None:
        self.deliverability_calls.append(address)
        if address == "User@example.com":
            return EmailDeliverabilityRecord(
                deliverability=EmailDeliverability.SUPPRESSED,
                suppression_reason="hard_bounce",
            )
        return None


class FailingIdentityVerifier:
    async def verify(self, principal):
        raise AssertionError("GET must not call Cognito")


class FakeIdentityVerifier:
    def __init__(
        self,
        identity: VerifiedEmailIdentity | None = None,
        error: Exception | None = None,
    ) -> None:
        self.identity = identity or VerifiedEmailIdentity(
            address="verified@example.com",
            verified=True,
            checked_at=NOW,
            identity_source="cognito_get_user",
        )
        self.error = error
        self.calls: list[Principal] = []

    async def verify(self, principal: Principal) -> VerifiedEmailIdentity:
        self.calls.append(principal)
        if self.error is not None:
            raise self.error
        return self.identity


PRINCIPAL = Principal(
    cognito_sub="sub_1",
    access_token="validated-token",
    scopes=frozenset({"aws.cognito.signin.user.admin"}),
)
AUDIT = AuditContext(actor_id="sub_1", ip="127.0.0.1", user_agent="tests")


def make_preference(**updates: object) -> NotificationPreference:
    values: dict[str, object] = {
        "tenant_id": "tnt_1",
        "cognito_sub": "sub_1",
        "version": 2,
        "email_enabled": True,
        "email_address": "alice@example.com",
        "email_verified": True,
        "checked_at": NOW,
        "identity_source": "cognito_get_user",
        "minimum_severity": AlertSeverity.WARNING,
        "telegram_enabled": False,
        "created_at": NOW,
        "updated_at": NOW,
    }
    values.update(updates)
    return NotificationPreference.model_validate(values)


@pytest.mark.asyncio
async def test_telegram_only_create_requires_binding_and_does_not_call_cognito() -> None:
    repository = FakeNotificationPreferenceRepository()
    telegram = FakeTelegramBindingRepository(verified=True)
    verifier = FakeIdentityVerifier(error=AssertionError("Telegram-only must not call Cognito"))

    result = await NotificationPreferenceService(
        repository,
        verifier,
        telegram_repository=telegram,
        clock=lambda: NOW,
    ).put(
        "tnt_1",
        PRINCIPAL,
        expected_version=None,
        email_enabled=False,
        telegram_enabled=True,
        minimum_severity=AlertSeverity.WARNING,
        audit=AUDIT,
    )

    saved = repository.save_calls[0][0]
    assert verifier.calls == []
    assert saved.email_address is None
    assert saved.telegram_enabled is True
    assert result.telegram.status == "verified"
    assert result.telegram.effective_enabled is True


@pytest.mark.asyncio
async def test_legacy_email_disabled_create_still_captures_cognito_identity() -> None:
    repository = FakeNotificationPreferenceRepository()
    verifier = FakeIdentityVerifier()

    result = await NotificationPreferenceService(
        repository,
        verifier,
        clock=lambda: NOW,
    ).put(
        "tnt_1",
        PRINCIPAL,
        expected_version=None,
        email_enabled=False,
        telegram_enabled=None,
        minimum_severity=AlertSeverity.WARNING,
        audit=AUDIT,
    )

    saved = repository.save_calls[0][0]
    assert verifier.calls == [PRINCIPAL]
    assert saved.email_enabled is False
    assert saved.email_address == "verified@example.com"
    assert saved.email_verified is True
    assert saved.checked_at == NOW
    assert saved.identity_source == "cognito_get_user"
    assert result.email.address == "v***d@example.com"


@pytest.mark.asyncio
async def test_email_enable_without_verified_email_is_rejected() -> None:
    repository = FakeNotificationPreferenceRepository()
    verifier = FakeIdentityVerifier(error=IdentityEmailError("unverified"))

    with pytest.raises(IdentityEmailError):
        await NotificationPreferenceService(repository, verifier).put(
            "tnt_1",
            PRINCIPAL,
            expected_version=None,
            email_enabled=True,
            telegram_enabled=False,
            minimum_severity=AlertSeverity.CRITICAL,
            audit=AUDIT,
        )

    assert repository.save_calls == []


@pytest.mark.asyncio
async def test_telegram_enable_without_verified_binding_is_conflict() -> None:
    repository = FakeNotificationPreferenceRepository()
    verifier = FakeIdentityVerifier(error=AssertionError("must not call Cognito"))

    with pytest.raises(ConflictError):
        await NotificationPreferenceService(
            repository,
            verifier,
            telegram_repository=FakeTelegramBindingRepository(),
        ).put(
            "tnt_1",
            PRINCIPAL,
            expected_version=None,
            email_enabled=False,
            telegram_enabled=True,
            minimum_severity=AlertSeverity.CRITICAL,
            audit=AUDIT,
        )

    assert verifier.calls == []
    assert repository.save_calls == []


@pytest.mark.asyncio
async def test_legacy_email_disabled_row_defaults_telegram_disabled() -> None:
    preference = NotificationPreference.model_validate(
        {
            key: value
            for key, value in make_preference(email_enabled=False).model_dump().items()
            if key != "telegram_enabled"
        }
    )
    repository = FakeNotificationPreferenceRepository(preference)

    result = await NotificationPreferenceService(repository).get("tnt_1", "sub_1")

    assert result.telegram.enabled is False
    assert result.telegram.effective_enabled is False


@pytest.mark.asyncio
async def test_expired_binding_request_is_not_reported_as_pending() -> None:
    repository = FakeNotificationPreferenceRepository()

    class ExpiredRequestRepository:
        async def get_current(self, tenant_id: str, recipient_id: str):
            return (
                None,
                TelegramBindingRequest(
                    request_id="request_expired",
                    tenant_id=tenant_id,
                    recipient_id=recipient_id,
                    token_hash="a" * 64,
                    status=TelegramBindingStatus.PENDING,
                    expires_at=NOW,
                    created_at=NOW,
                ),
                None,
            )

    result = await NotificationPreferenceService(
        repository,
        telegram_repository=ExpiredRequestRepository(),
        clock=lambda: NOW,
    ).get("tnt_1", "sub_1")

    assert result.telegram.status == "absent"
    assert result.telegram.pending_request_id is None
    assert result.telegram.pending_expires_at is None


@pytest.mark.asyncio
async def test_telegram_only_update_does_not_call_cognito() -> None:
    repository = FakeNotificationPreferenceRepository(make_preference(email_enabled=False))
    verifier = FakeIdentityVerifier(error=AssertionError("Telegram-only must not call Cognito"))

    result = await NotificationPreferenceService(
        repository,
        verifier,
        telegram_repository=FakeTelegramBindingRepository(verified=True),
        clock=lambda: NOW,
    ).put(
        "tnt_1",
        PRINCIPAL,
        expected_version=2,
        email_enabled=False,
        telegram_enabled=True,
        minimum_severity=AlertSeverity.WARNING,
        audit=AUDIT,
    )

    assert verifier.calls == []
    assert result.version == 3


@pytest.mark.asyncio
async def test_omitted_telegram_preserves_value_and_noop_does_not_write() -> None:
    existing = make_preference(email_enabled=False, telegram_enabled=True)
    repository = FakeNotificationPreferenceRepository(existing)

    result = await NotificationPreferenceService(
        repository,
        telegram_repository=FakeTelegramBindingRepository(verified=True),
    ).put(
        "tnt_1",
        PRINCIPAL,
        expected_version=2,
        email_enabled=False,
        telegram_enabled=None,
        minimum_severity=AlertSeverity.WARNING,
        audit=AUDIT,
    )

    assert result.version == 2
    assert result.telegram.enabled is True
    assert repository.save_calls == []


@pytest.mark.asyncio
async def test_get_returns_unconfigured_defaults_without_cognito_or_deliverability_lookup() -> None:
    repository = FakeNotificationPreferenceRepository()
    service = NotificationPreferenceService(repository, FailingIdentityVerifier())

    result = await service.get("tnt_1", "sub_1")

    assert repository.get_calls == [("tnt_1", "sub_1")]
    assert repository.deliverability_calls == []
    assert repository.save_calls == []
    assert result.model_dump(mode="json") == {
        "configured": False,
        "version": None,
        "email": {
            "enabled": False,
            "address": None,
            "verified": False,
            "deliverability": "unknown",
            "suppression_reason": None,
            "effective_enabled": False,
        },
        "telegram": {
            "enabled": False,
            "status": "absent",
            "version": None,
            "verified_at": None,
            "pending_request_id": None,
            "pending_expires_at": None,
            "effective_enabled": False,
        },
        "minimum_severity": "critical",
    }


@pytest.mark.asyncio
async def test_get_masks_address_and_applies_deliverability_suppression() -> None:
    repository = FakeNotificationPreferenceRepository(
        make_preference(),
        EmailDeliverabilityRecord(
            deliverability=EmailDeliverability.SUPPRESSED,
            suppression_reason="complaint",
        ),
    )
    service = NotificationPreferenceService(repository, FailingIdentityVerifier())

    result = await service.get("tnt_1", "sub_1")

    assert repository.deliverability_calls == ["alice@example.com"]
    assert result.configured is True
    assert result.version == 2
    assert result.email.address == "a***e@example.com"
    assert result.email.deliverability is EmailDeliverability.SUPPRESSED
    assert result.email.suppression_reason == "complaint"
    assert result.email.effective_enabled is False
    assert result.minimum_severity is AlertSeverity.WARNING


@pytest.mark.asyncio
async def test_get_treats_absent_deliverability_record_as_allowed_unknown() -> None:
    repository = FakeNotificationPreferenceRepository(make_preference())

    result = await NotificationPreferenceService(
        repository,
        FailingIdentityVerifier(),
    ).get("tnt_1", "sub_1")

    assert result.email.deliverability is EmailDeliverability.UNKNOWN
    assert result.email.suppression_reason is None
    assert result.email.effective_enabled is True


@pytest.mark.asyncio
async def test_email_create_verifies_cognito_and_conditionally_creates_version_one() -> None:
    repository = FakeNotificationPreferenceRepository()
    verifier = FakeIdentityVerifier()

    result = await NotificationPreferenceService(
        repository,
        verifier,
        clock=lambda: NOW,
    ).put(
        "tnt_1",
        PRINCIPAL,
        expected_version=None,
        email_enabled=True,
        minimum_severity=AlertSeverity.WARNING,
        audit=AUDIT,
    )

    assert verifier.calls == [PRINCIPAL]
    saved, expected_version, audit, previous = repository.save_calls[0]
    assert expected_version is None
    assert audit == AUDIT
    assert previous is None
    assert saved.version == 1
    assert saved.email_enabled is True
    assert saved.email_address == "verified@example.com"
    assert saved.email_verified is True
    assert saved.checked_at == NOW
    assert saved.identity_source == "cognito_get_user"
    assert saved.minimum_severity is AlertSeverity.WARNING
    assert result.version == 1


@pytest.mark.asyncio
async def test_enabled_update_refreshes_identity_and_increments_expected_version() -> None:
    existing = make_preference()
    repository = FakeNotificationPreferenceRepository(existing)
    verifier = FakeIdentityVerifier(
        VerifiedEmailIdentity(
            address="fresh@example.com",
            verified=True,
            checked_at=NOW,
            identity_source="cognito_get_user",
        )
    )

    await NotificationPreferenceService(repository, verifier, clock=lambda: NOW).put(
        "tnt_1",
        PRINCIPAL,
        expected_version=2,
        email_enabled=True,
        minimum_severity=AlertSeverity.CRITICAL,
        audit=AUDIT,
    )

    saved, expected_version, _, previous = repository.save_calls[0]
    assert verifier.calls == [PRINCIPAL]
    assert expected_version == 2
    assert previous == existing
    assert saved.version == 3
    assert saved.email_address == "fresh@example.com"
    assert saved.created_at == existing.created_at
    assert saved.minimum_severity is AlertSeverity.CRITICAL


@pytest.mark.asyncio
async def test_disable_existing_preference_preserves_identity_without_cognito() -> None:
    existing = make_preference()
    repository = FakeNotificationPreferenceRepository(existing)
    verifier = FakeIdentityVerifier(error=AssertionError("Cognito must not be called"))

    result = await NotificationPreferenceService(
        repository,
        verifier,
        clock=lambda: NOW,
    ).put(
        "tnt_1",
        PRINCIPAL,
        expected_version=2,
        email_enabled=False,
        minimum_severity=AlertSeverity.CRITICAL,
        audit=AUDIT,
    )

    saved, _, _, previous = repository.save_calls[0]
    assert verifier.calls == []
    assert saved.version == 3
    assert saved.email_enabled is False
    assert saved.email_address == existing.email_address
    assert saved.checked_at == existing.checked_at
    assert previous == existing
    assert result.email.effective_enabled is False


@pytest.mark.asyncio
async def test_update_of_absent_preference_is_conflict_without_mutation() -> None:
    repository = FakeNotificationPreferenceRepository()
    verifier = FakeIdentityVerifier(error=AssertionError("Cognito must not be called"))

    with pytest.raises(ConflictError):
        await NotificationPreferenceService(repository, verifier).put(
            "tnt_1",
            PRINCIPAL,
            expected_version=1,
            email_enabled=False,
            minimum_severity=AlertSeverity.CRITICAL,
            audit=AUDIT,
        )

    assert verifier.calls == []
    assert repository.save_calls == []


@pytest.mark.asyncio
async def test_duplicate_create_preflight_returns_conflict_without_cognito() -> None:
    repository = FakeNotificationPreferenceRepository(make_preference(version=1))
    verifier = FakeIdentityVerifier(error=IdentityEmailError("must not win over conflict"))

    with pytest.raises(ConflictError):
        await NotificationPreferenceService(repository, verifier).put(
            "tnt_1",
            PRINCIPAL,
            expected_version=None,
            email_enabled=True,
            minimum_severity=AlertSeverity.CRITICAL,
            audit=AUDIT,
        )

    assert verifier.calls == []
    assert repository.save_calls == []


@pytest.mark.asyncio
async def test_enabled_stale_update_preflight_returns_conflict_without_cognito() -> None:
    repository = FakeNotificationPreferenceRepository(make_preference(version=3))
    verifier = FakeIdentityVerifier(error=IdentityEmailError("must not win over conflict"))

    with pytest.raises(ConflictError):
        await NotificationPreferenceService(repository, verifier).put(
            "tnt_1",
            PRINCIPAL,
            expected_version=2,
            email_enabled=True,
            minimum_severity=AlertSeverity.CRITICAL,
            audit=AUDIT,
        )

    assert verifier.calls == []
    assert repository.save_calls == []


@pytest.mark.asyncio
async def test_enabled_absent_update_preflight_returns_conflict_without_cognito() -> None:
    repository = FakeNotificationPreferenceRepository()
    verifier = FakeIdentityVerifier(error=IdentityEmailError("must not win over conflict"))

    with pytest.raises(ConflictError):
        await NotificationPreferenceService(repository, verifier).put(
            "tnt_1",
            PRINCIPAL,
            expected_version=1,
            email_enabled=True,
            minimum_severity=AlertSeverity.CRITICAL,
            audit=AUDIT,
        )

    assert verifier.calls == []
    assert repository.save_calls == []


@pytest.mark.asyncio
async def test_identity_failure_leaves_repository_unmodified() -> None:
    repository = FakeNotificationPreferenceRepository()
    verifier = FakeIdentityVerifier(error=IdentityEmailError("invalid email"))

    with pytest.raises(IdentityEmailError):
        await NotificationPreferenceService(repository, verifier).put(
            "tnt_1",
            PRINCIPAL,
            expected_version=None,
            email_enabled=True,
            minimum_severity=AlertSeverity.CRITICAL,
            audit=AUDIT,
        )

    assert repository.save_calls == []


@pytest.mark.asyncio
async def test_stale_version_is_reported_by_conditional_save() -> None:
    repository = FakeNotificationPreferenceRepository(make_preference(version=3))

    with pytest.raises(ConflictError):
        await NotificationPreferenceService(
            repository,
            FakeIdentityVerifier(error=AssertionError("Cognito must not be called")),
        ).put(
            "tnt_1",
            PRINCIPAL,
            expected_version=2,
            email_enabled=False,
            minimum_severity=AlertSeverity.CRITICAL,
            audit=AUDIT,
        )


@pytest.mark.asyncio
async def test_put_loads_deliverability_before_the_atomic_commit() -> None:
    repository = DeliverabilityMustPrecedeCommitRepository()

    result = await NotificationPreferenceService(
        repository,
        FakeIdentityVerifier(),
        clock=lambda: NOW,
    ).put(
        "tnt_1",
        PRINCIPAL,
        expected_version=None,
        email_enabled=True,
        minimum_severity=AlertSeverity.CRITICAL,
        audit=AUDIT,
    )

    assert result.version == 1
    assert repository.deliverability_calls == ["verified@example.com"]
    assert len(repository.save_calls) == 1


@pytest.mark.asyncio
async def test_put_preserves_legacy_suppression_view_across_case_only_email_change() -> None:
    existing = make_preference(version=2, email_address="User@example.com")
    repository = CaseTransitionRepository(existing)
    verifier = FakeIdentityVerifier(
        VerifiedEmailIdentity(
            address="user@example.com",
            verified=True,
            checked_at=NOW,
            identity_source="cognito_get_user",
        )
    )

    result = await NotificationPreferenceService(repository, verifier, clock=lambda: NOW).put(
        "tnt_1",
        PRINCIPAL,
        expected_version=2,
        email_enabled=True,
        minimum_severity=AlertSeverity.WARNING,
        audit=AUDIT,
    )

    assert repository.deliverability_calls == ["user@example.com", "User@example.com"]
    assert result.email.deliverability is EmailDeliverability.SUPPRESSED
    assert result.email.effective_enabled is False
