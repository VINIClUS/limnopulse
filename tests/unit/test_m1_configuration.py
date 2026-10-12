from pathlib import Path

import pytest

from limnopulse_api.core.config import Settings

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def _isolate_flag_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("API_V2_ENABLED", raising=False)
    monkeypatch.delenv("V1_POND_DEVICE_PROJECTION_ENABLED", raising=False)


def test_m1_feature_flags_default_off() -> None:
    settings = Settings(_env_file=None, app_env="test", auth_mode="dev")
    assert settings.api_v2_enabled is False
    assert settings.v1_pond_device_projection_enabled is False


@pytest.mark.parametrize(
    ("api_v2", "projection"),
    [("true", "false"), ("false", "true"), ("true", "true")],
)
def test_m1_feature_flags_read_environment_independently(
    monkeypatch: pytest.MonkeyPatch, api_v2: str, projection: str
) -> None:
    monkeypatch.setenv("API_V2_ENABLED", api_v2)
    monkeypatch.setenv("V1_POND_DEVICE_PROJECTION_ENABLED", projection)

    settings = Settings(_env_file=None, app_env="test", auth_mode="dev")

    assert settings.api_v2_enabled is (api_v2 == "true")
    assert settings.v1_pond_device_projection_enabled is (projection == "true")


def test_m1_feature_flags_change_independently_in_code() -> None:
    v2_only = Settings(_env_file=None, app_env="test", auth_mode="dev", api_v2_enabled=True)
    projection_only = Settings(
        _env_file=None,
        app_env="test",
        auth_mode="dev",
        v1_pond_device_projection_enabled=True,
    )

    assert (v2_only.api_v2_enabled, v2_only.v1_pond_device_projection_enabled) == (True, False)
    assert (
        projection_only.api_v2_enabled,
        projection_only.v1_pond_device_projection_enabled,
    ) == (False, True)


def test_example_environment_documents_both_flags() -> None:
    lines = (ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
    assert "API_V2_ENABLED=false" in lines
    assert "V1_POND_DEVICE_PROJECTION_ENABLED=false" in lines
