from fastapi.testclient import TestClient

from limnopulse_api.core.config import Settings
from limnopulse_api.main import create_app


def test_me_without_identity_returns_401() -> None:
    app = create_app(Settings(app_env="test", auth_mode="dev"))
    client = TestClient(app)

    response = client.get("/v1/me")

    assert response.status_code == 401


def test_me_with_dev_identity_returns_principal() -> None:
    app = create_app(Settings(app_env="test", auth_mode="dev"))
    client = TestClient(app)

    response = client.get(
        "/v1/me",
        headers={"X-Dev-User-Sub": "sub_1", "X-Dev-User-Email": "u@example.test"},
    )

    assert response.status_code == 200
    assert response.json()["cognito_sub"] == "sub_1"
