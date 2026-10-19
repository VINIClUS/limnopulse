import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.testclient import TestClient

from limnopulse_api.api.dependencies import require_tenant_access
from limnopulse_api.api.openapi_contract import build_v1_openapi_contract, iter_effective_api_routes
from limnopulse_api.api.router import build_api_router
from limnopulse_api.api.v2.router import v2_safe_request_validation_exception_handler
from limnopulse_api.core.config import Settings
from limnopulse_api.domain.entities import Membership
from limnopulse_api.domain.roles import TenantRole
from limnopulse_api.main import create_app

ROOT = Path(__file__).resolve().parents[2]
GOLDEN = ROOT / "tests/contracts/openapi/v1.json"
NOW = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)
TENANT = "tnt_a"
DEVICE_ID = "dev_" + "3" * 32
COMPONENT_ID = "cmp_" + "1" * 32
AUTH = {"X-Dev-User-Sub": "sub_1", "Idempotency-Key": "composition-key-0001"}

M1_V2_OPERATIONS = {
    ("/v2/tenants/{tenant_id}/sites", "get"),
    ("/v2/tenants/{tenant_id}/sites", "post"),
    ("/v2/tenants/{tenant_id}/sites/{site_id}", "get"),
    ("/v2/tenants/{tenant_id}/sites/{site_id}", "patch"),
    ("/v2/tenants/{tenant_id}/assets", "get"),
    ("/v2/tenants/{tenant_id}/assets", "post"),
    ("/v2/tenants/{tenant_id}/assets/{asset_id}", "get"),
    ("/v2/tenants/{tenant_id}/assets/{asset_id}", "patch"),
    ("/v2/tenants/{tenant_id}/devices", "get"),
    ("/v2/tenants/{tenant_id}/devices", "post"),
    ("/v2/tenants/{tenant_id}/devices/{device_id}", "get"),
    ("/v2/tenants/{tenant_id}/devices/{device_id}", "patch"),
    ("/v2/tenants/{tenant_id}/devices/{device_id}/components", "get"),
    ("/v2/tenants/{tenant_id}/devices/{device_id}/components", "post"),
    ("/v2/tenants/{tenant_id}/devices/{device_id}/integrations", "get"),
    ("/v2/tenants/{tenant_id}/devices/{device_id}/integrations", "post"),
    ("/v2/tenants/{tenant_id}/components/{component_id}", "get"),
    ("/v2/tenants/{tenant_id}/components/{component_id}", "patch"),
    ("/v2/tenants/{tenant_id}/components/{component_id}/capabilities", "get"),
    ("/v2/tenants/{tenant_id}/components/{component_id}/capabilities", "post"),
    ("/v2/tenants/{tenant_id}/integration-accounts", "get"),
    ("/v2/tenants/{tenant_id}/integration-accounts", "post"),
    ("/v2/tenants/{tenant_id}/deployments", "get"),
    ("/v2/tenants/{tenant_id}/deployments", "post"),
    ("/v2/tenants/{tenant_id}/deployments/{deployment_id}/end", "post"),
}
V1_TENANT_ROUTE_COUNT = 28
V2_ON_TENANT_ROUTE_COUNT = V1_TENANT_ROUTE_COUNT + len(M1_V2_OPERATIONS)
FORBIDDEN_V2_FRAGMENTS = (
    "telemetry",
    "observation",
    "health",
    "calibration",
    "billing",
    "iot",
    "notification",
    "destination",
    "command",
)


@pytest.fixture(autouse=True)
def _isolate_flag_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("API_V2_ENABLED", raising=False)
    monkeypatch.delenv("V1_POND_DEVICE_PROJECTION_ENABLED", raising=False)
    monkeypatch.delenv("TELEMETRY_V2_WRITE_ENABLED", raising=False)
    monkeypatch.delenv("TELEMETRY_LEGACY_DUAL_WRITE_ENABLED", raising=False)
    monkeypatch.delenv("API_V2_TELEMETRY_ENABLED", raising=False)
    monkeypatch.delenv("V1_TELEMETRY_READ_SOURCE", raising=False)


def _settings(**overrides: object) -> Settings:
    return Settings(_env_file=None, app_env="test", auth_mode="dev", **overrides)


def _operations(paths: dict[str, dict[str, object]]) -> set[tuple[str, str]]:
    return {(path, method) for path, methods in paths.items() for method in methods}


def _dependency_calls(dependant):
    yield dependant.call
    for dependency in dependant.dependencies:
        yield from _dependency_calls(dependency)


def test_v2_paths_are_absent_when_flag_is_off() -> None:
    app = create_app(_settings())
    paths = app.openapi()["paths"]
    assert not any(path == "/v2" or path.startswith("/v2/") for path in paths)


def test_v2_flag_mounts_exactly_the_m1_groups() -> None:
    app = create_app(_settings(api_v2_enabled=True))
    paths = app.openapi()["paths"]
    v2_paths = {path: methods for path, methods in paths.items() if path.startswith("/v2/")}

    assert _operations(v2_paths) == M1_V2_OPERATIONS
    for path in v2_paths:
        assert not any(fragment in path.lower() for fragment in FORBIDDEN_V2_FRAGMENTS)


def test_v2_flag_changes_only_router_presence() -> None:
    off = create_app(_settings()).openapi()["paths"]
    on = create_app(_settings(api_v2_enabled=True)).openapi()["paths"]
    non_v2_on = {path: methods for path, methods in on.items() if not path.startswith("/v2/")}
    assert non_v2_on == off


def test_projection_flag_does_not_mount_v2() -> None:
    app = create_app(_settings(v1_pond_device_projection_enabled=True))
    assert not any(path.startswith("/v2/") for path in app.openapi()["paths"])


def _mounted_routes(router) -> list[tuple[str, list[str]]]:
    app = FastAPI()
    app.include_router(router)
    return [(route.path_format, sorted(route.methods)) for route in iter_effective_api_routes(app)]


@pytest.mark.parametrize("telegram_webhook_enabled", [True, False])
@pytest.mark.parametrize("enable_v2", [True, False])
def test_router_factory_keeps_current_mounts(
    telegram_webhook_enabled: bool, enable_v2: bool
) -> None:
    baseline = _mounted_routes(build_api_router(telegram_webhook_enabled=telegram_webhook_enabled))
    routes = _mounted_routes(
        build_api_router(telegram_webhook_enabled=telegram_webhook_enabled, enable_v2=enable_v2)
    )

    assert [route for route in routes if not route[0].startswith("/v2/")] == baseline
    v2_operations = {
        (path, method.lower())
        for path, methods in routes
        for method in methods
        if path.startswith("/v2/")
    }
    assert v2_operations == (M1_V2_OPERATIONS if enable_v2 else set())
    assert ("/webhooks/telegram" in {path for path, _ in routes}) is telegram_webhook_enabled


def test_openapi_generation_does_not_enter_lifespan(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*args: object, **kwargs: object) -> object:
        raise AssertionError("OpenAPI generation must not create runtime clients")

    monkeypatch.setattr("limnopulse_api.main.boto3.client", fail)
    monkeypatch.setattr("limnopulse_api.main.redis.from_url", fail)
    monkeypatch.setattr("limnopulse_api.main.InfluxDBClient", fail)

    app = create_app(_settings(api_v2_enabled=True))
    assert app.openapi()["paths"]
    assert not hasattr(app.state, "site_asset_repository")


def test_v1_contract_is_unchanged_with_v2_enabled() -> None:
    app = create_app(_settings(api_v2_enabled=True))
    assert build_v1_openapi_contract(app) == json.loads(GOLDEN.read_text(encoding="utf-8"))


def test_every_tenant_route_requires_membership_with_v2_enabled() -> None:
    app = create_app(_settings(api_v2_enabled=True))
    routes = [
        route for route in iter_effective_api_routes(app) if "{tenant_id}" in route.path_format
    ]

    assert len(routes) == V2_ON_TENANT_ROUTE_COUNT
    assert sum(route.path_format.startswith("/v2/") for route in routes) == len(M1_V2_OPERATIONS)
    for route in routes:
        assert require_tenant_access in set(_dependency_calls(route.dependant))


class _WriterMembershipService:
    async def get_active_membership(self, cognito_sub: str, tenant_id: str) -> Membership | None:
        if cognito_sub != "sub_1" or tenant_id != TENANT:
            return None
        return Membership(
            tenant_id=tenant_id,
            cognito_sub=cognito_sub,
            role=TenantRole.OWNER,
            created_at=NOW,
            updated_at=NOW,
            version=1,
        )


class _UnreachableRepository:
    def __getattr__(self, name: str) -> object:
        raise AssertionError(f"repository must not be reached on invalid input: {name}")


@pytest.fixture
def v2_client() -> TestClient:
    app = create_app(_settings(api_v2_enabled=True))
    app.state.membership_service = _WriterMembershipService()
    for name in (
        "site_asset_repository",
        "device_component_repository",
        "integration_repository",
        "deployment_repository",
    ):
        setattr(app.state, name, _UnreachableRepository())
    with TestClient(app, raise_server_exceptions=False) as client:
        yield client


NAN_WRITE_ROUTES = {
    "sites": f"/v2/tenants/{TENANT}/sites",
    "assets": f"/v2/tenants/{TENANT}/assets",
    "devices": f"/v2/tenants/{TENANT}/devices",
    "components": f"/v2/tenants/{TENANT}/devices/{DEVICE_ID}/components",
    "capabilities": f"/v2/tenants/{TENANT}/components/{COMPONENT_ID}/capabilities",
    "integration-accounts": f"/v2/tenants/{TENANT}/integration-accounts",
    "device-integrations": f"/v2/tenants/{TENANT}/devices/{DEVICE_ID}/integrations",
    "deployments": f"/v2/tenants/{TENANT}/deployments",
}
REDACTED_GROUPS = {"integration-accounts", "device-integrations"}


@pytest.mark.parametrize("group", sorted(NAN_WRITE_ROUTES))
def test_non_finite_body_values_return_json_422_on_every_v2_group(
    v2_client: TestClient, group: str
) -> None:
    body = '{"name": NaN, "display_name": Infinity, "provider": -Infinity, "kind": NaN}'

    response = v2_client.post(
        NAN_WRITE_ROUTES[group],
        content=body,
        headers={**AUTH, "Content-Type": "application/json"},
    )

    assert response.status_code == 422
    payload = json.loads(response.text)
    assert isinstance(payload["detail"], list) and payload["detail"]
    assert "NaN" not in response.text and "Infinity" not in response.text
    if group in REDACTED_GROUPS:
        assert all("input" not in error for error in payload["detail"])


def test_v1_validation_errors_keep_framework_default_body() -> None:
    app = create_app(_settings(api_v2_enabled=True))
    default_app = FastAPI()
    custom_app = FastAPI()
    custom_app.add_exception_handler(
        RequestValidationError, v2_safe_request_validation_exception_handler
    )
    for target in (default_app, custom_app):

        @target.get("/v1/probe")
        async def probe(limit: int) -> dict[str, int]:
            return {"limit": limit}

    default = TestClient(default_app).get("/v1/probe", params={"limit": "x"})
    custom = TestClient(custom_app).get("/v1/probe", params={"limit": "x"})
    assert app.exception_handlers[RequestValidationError] is (
        v2_safe_request_validation_exception_handler
    )
    assert (custom.status_code, custom.json()) == (default.status_code, default.json())
    assert default.json()["detail"][0]["input"] == "x"


def test_non_finite_body_returns_json_422_behind_root_path() -> None:
    app = create_app(_settings(api_v2_enabled=True))
    app.state.membership_service = _WriterMembershipService()
    app.state.site_asset_repository = _UnreachableRepository()
    app.state.alert_event_repository = _UnreachableRepository()

    with TestClient(app, root_path="/api", raise_server_exceptions=False) as client:
        response = client.post(
            f"/api/v2/tenants/{TENANT}/sites",
            content='{"name": NaN}',
            headers={**AUTH, "Content-Type": "application/json"},
        )
        v1_response = client.get(
            f"/api/v1/tenants/{TENANT}/alert-events/active", params={"limit": "x"}, headers=AUTH
        )

    assert response.status_code == 422
    assert "NaN" not in response.text
    assert json.loads(response.text)["detail"]
    assert v1_response.status_code == 422
    assert v1_response.json()["detail"][0]["input"] == "x"
