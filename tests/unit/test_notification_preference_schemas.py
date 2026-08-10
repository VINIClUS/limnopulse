import pytest
from pydantic import ValidationError

from limnopulse_api.api.v1.schemas.notification_preferences import (
    NotificationPreferenceUpdate,
)
from limnopulse_api.domain.alerts import AlertSeverity


def test_expected_version_is_required_but_nullable_and_severity_defaults_to_critical() -> None:
    payload = NotificationPreferenceUpdate.model_validate(
        {"expected_version": None, "email_enabled": True}
    )

    assert payload.expected_version is None
    assert payload.email_enabled is True
    assert payload.minimum_severity is AlertSeverity.CRITICAL


@pytest.mark.parametrize(
    "payload",
    [
        {"email_enabled": True},
        {"expected_version": None},
        {"expected_version": 0, "email_enabled": True},
        {"expected_version": None, "email_enabled": True, "minimum_severity": None},
        {"expected_version": None, "email_enabled": True, "minimum_severity": "info"},
        {"expected_version": None, "email_enabled": True, "severities": ["critical"]},
        {"expected_version": None, "email_enabled": True, "extra": "forbidden"},
    ],
    ids=[
        "missing-version",
        "missing-enabled",
        "invalid-version",
        "null-severity",
        "invalid-severity",
        "legacy-plural",
        "extra-field",
    ],
)
def test_update_rejects_invalid_contracts(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        NotificationPreferenceUpdate.model_validate(payload)
