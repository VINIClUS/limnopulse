import pytest
from pydantic import ValidationError

from limnopulse_api.core.config import Settings


def test_dev_auth_is_allowed_in_local() -> None:
    settings = Settings(app_env="local", auth_mode="dev")
    assert settings.auth_mode == "dev"


def test_dev_auth_is_allowed_in_test() -> None:
    settings = Settings(app_env="test", auth_mode="dev")
    assert settings.auth_mode == "dev"


@pytest.mark.parametrize("app_env", ["staging", "prod"])
def test_dev_auth_is_rejected_outside_local_and_test(app_env: str) -> None:
    with pytest.raises(ValidationError):
        Settings(app_env=app_env, auth_mode="dev")


def test_default_table_names_use_limnopulse() -> None:
    settings = Settings(app_env="test", auth_mode="dev")
    assert settings.dynamodb_domain_table == "LimnopulseDomain"
    assert settings.dynamodb_audit_table == "LimnopulseAudit"


def test_default_influxdb_settings_use_local_limnopulse_values() -> None:
    settings = Settings(app_env="test", auth_mode="dev")
    assert settings.influxdb_url == "http://localhost:8086"
    assert settings.influxdb_org == "limnopulse"
    assert settings.influxdb_bucket_raw == "limnopulse_raw"
    assert settings.telemetry_default_range == "-1h"
    assert settings.telemetry_max_limit == 1000
