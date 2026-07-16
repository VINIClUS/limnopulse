from datetime import UTC, datetime
from pathlib import Path
import tomllib

import pytest
from pydantic import ValidationError

from limnopulse_api.domain.alerts import AlertSeverity
from limnopulse_api.domain.notification_preferences import (
    EmailDeliverability,
    NotificationPreference,
    email_is_effectively_enabled,
    mask_email_address,
    severity_meets_minimum,
    severity_rank,
)


NOW = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)


def test_project_declares_email_validator_dependency() -> None:
    project = tomllib.loads(Path("pyproject.toml").read_text())["project"]

    assert any(dependency.startswith("email-validator>=") for dependency in project["dependencies"])


def make_preference(*, email_enabled: bool = True) -> NotificationPreference:
    return NotificationPreference(
        tenant_id="tnt_1",
        cognito_sub="sub_1",
        version=1,
        email_enabled=email_enabled,
        email_address="alice@example.com",
        email_verified=True,
        checked_at=NOW,
        identity_source="cognito_get_user",
        minimum_severity=AlertSeverity.CRITICAL,
        created_at=NOW,
        updated_at=NOW,
    )


def test_severity_order_is_explicit_and_warning_is_below_critical() -> None:
    assert severity_rank(AlertSeverity.WARNING) < severity_rank(AlertSeverity.CRITICAL)
    assert severity_meets_minimum(AlertSeverity.CRITICAL, AlertSeverity.WARNING) is True
    assert severity_meets_minimum(AlertSeverity.WARNING, AlertSeverity.CRITICAL) is False


def test_mask_email_address_preserves_only_safe_hints() -> None:
    assert mask_email_address("alice@example.com") == "a***e@example.com"
    assert mask_email_address("u@example.com") == "u***@example.com"


def test_email_is_effectively_enabled_only_when_intended_verified_and_not_suppressed() -> None:
    preference = make_preference()

    assert email_is_effectively_enabled(preference, EmailDeliverability.UNKNOWN) is True
    assert email_is_effectively_enabled(preference, EmailDeliverability.SUPPRESSED) is False
    assert (
        email_is_effectively_enabled(
            preference.model_copy(update={"email_verified": False}),
            EmailDeliverability.UNKNOWN,
        )
        is False
    )


def test_notification_preference_rejects_non_ascii_email_snapshot() -> None:
    values = make_preference().model_dump(mode="python")
    values["email_address"] = "δοκιμή@example.com"

    with pytest.raises(ValidationError):
        NotificationPreference.model_validate(values)
    assert (
        email_is_effectively_enabled(
            make_preference(email_enabled=False), EmailDeliverability.UNKNOWN
        )
        is False
    )
