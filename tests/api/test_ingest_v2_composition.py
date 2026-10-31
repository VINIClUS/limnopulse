"""P3-10 composition: `INGEST_V2_ENABLED` mounts exactly the D15 surface, the lifespan builds the
frozen P3 `app.state` ports only behind that switch, and every switch off equals `main`."""

import asyncio
import inspect
import json
from pathlib import Path
from typing import Any, ClassVar

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError

from limnopulse_api.adapters.dynamodb_devices_v2 import DynamoDeviceComponentRepository
from limnopulse_api.adapters.dynamodb_ingest_credentials import DynamoIngestCredentialRepository
from limnopulse_api.adapters.dynamodb_ingress_receipts import DynamoIngressReceiptRepository
from limnopulse_api.adapters.dynamodb_integrations import DynamoIntegrationRepository
from limnopulse_api.adapters.sqs_ingestion import SqsIngressQueue
from limnopulse_api.adapters.telemetry_metrics_log import StructuredLogTelemetryMetrics
from limnopulse_api.api.dependencies import require_tenant_access
from limnopulse_api.api.ingestion_dependencies import INGESTION_STATE_NAMES
from limnopulse_api.api.openapi_contract import iter_effective_api_routes
from limnopulse_api.api.router import build_api_router
from limnopulse_api.api.v2.router import build_v2_router
from limnopulse_api.core.config import Settings, get_settings
from limnopulse_api.domain.entities import Membership
from limnopulse_api.domain.roles import TenantRole
from limnopulse_api.main import create_app, sqs_client_kwargs
from limnopulse_api.services.connector_auth import IngestCredentialAuthenticator
from limnopulse_api.services.ingress_entitlements import AllowDirectIngressEntitlements
from limnopulse_api.services.ingress_rate_limit import TokenBucketRateLimiter
from limnopulse_api.services.trusted_mapping import TrustedMappingService

ROOT = Path(__file__).resolve().parents[2]
GOLDEN = ROOT / "tests/contracts/openapi/v1.json"
TENANT = "tnt_a"
HEADERS = {"X-Dev-User-Sub": "sub_1"}
QUEUE_URL = "http://localhost:9324/000000000000/limnopulse-telemetry-ingress"
SQS_ENDPOINT = "http://localhost:9324"
DOMAIN_TABLE = "DomainUnderTest"
AUDIT_TABLE = "AuditUnderTest"
INGEST_PATH = "/ingest/v2/observations"
INGEST_OPERATION = (INGEST_PATH, "post")
CREDENTIALS = (
    "/v2/tenants/{tenant_id}/integration-accounts/{integration_account_id}/ingest-credentials"
)
INGEST_TENANT_OPERATIONS = {
    (CREDENTIALS, "post"),
    (CREDENTIALS, "get"),
    (CREDENTIALS + "/{ingest_credential_id}/revoke", "post"),
    ("/v2/tenants/{tenant_id}/integration-accounts/{integration_account_id}/status", "post"),
    (
        "/v2/tenants/{tenant_id}/devices/{device_id}/integrations/{device_integration_id}/status",
        "post",
    ),
}
ALLOWED_SCHEMA_PREFIXES = ("Ingest", "Ingress", "Quarantine", "Connector", "V2")
FLAG_ENVIRONMENT = (
    "API_V2_ENABLED",
    "V1_POND_DEVICE_PROJECTION_ENABLED",
    "TELEMETRY_V2_WRITE_ENABLED",
    "TELEMETRY_LEGACY_DUAL_WRITE_ENABLED",
    "API_V2_TELEMETRY_ENABLED",
    "V1_TELEMETRY_READ_SOURCE",
    "INGEST_V2_ENABLED",
    "SQS_ENDPOINT_URL",
    "SQS_TELEMETRY_INGRESS_QUEUE_URL",
    "SQS_TELEMETRY_INGRESS_DLQ_URL",
)
INGEST_ON: dict[str, object] = {
    "api_v2_enabled": True,
    "telemetry_v2_write_enabled": True,
    "ingest_v2_enabled": True,
    "sqs_telemetry_ingress_queue_url": QUEUE_URL,
}
# Every combination with `INGEST_V2_ENABLED` off, including its prerequisites switched on.
INGEST_OFF_COMBINATIONS: tuple[dict[str, object], ...] = (
    {},
    {"api_v2_enabled": True},
    {"api_v2_enabled": True, "api_v2_telemetry_enabled": True},
    {"api_v2_enabled": True, "telemetry_v2_write_enabled": True},
    {
        "api_v2_enabled": True,
        "telemetry_v2_write_enabled": True,
        "sqs_telemetry_ingress_queue_url": QUEUE_URL,
        "sqs_endpoint_url": SQS_ENDPOINT,
    },
)


@pytest.fixture(autouse=True)
def _isolate_flag_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in FLAG_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "app_env": "test",
        "auth_mode": "dev",
        "dynamodb_domain_table": DOMAIN_TABLE,
        "dynamodb_audit_table": AUDIT_TABLE,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def _operations(app: FastAPI) -> set[tuple[str, str]]:
    return {
        (path, method) for path, methods in app.openapi()["paths"].items() for method in methods
    }


def _router_operations(router) -> set[tuple[str, str]]:
    app = FastAPI()
    app.include_router(router)
    return {
        (route.path_format, method.lower())
        for route in iter_effective_api_routes(app)
        for method in route.methods
    }


def _dependency_calls(dependant):
    yield dependant.call
    for dependency in dependant.dependencies:
        yield from _dependency_calls(dependency)


def _route(app: FastAPI, path: str, method: str) -> Any:
    matches = [
        route
        for route in iter_effective_api_routes(app)
        if route.path_format == path and method.upper() in route.methods
    ]
    assert len(matches) == 1, (path, method)
    return matches[0]


def _schemas(app: FastAPI) -> set[str]:
    return set(app.openapi().get("components", {}).get("schemas", {}))


# --- Route sets: D15 -----------------------------------------------------------------------------


@pytest.mark.parametrize("overrides", INGEST_OFF_COMBINATIONS, ids=repr)
def test_ingest_off_openapi_equals_the_same_switches_without_any_p3_setting(
    overrides: dict[str, object],
) -> None:
    baseline_overrides = {
        name: value
        for name, value in overrides.items()
        if name not in ("sqs_telemetry_ingress_queue_url", "sqs_endpoint_url")
    }
    baseline = create_app(_settings(**baseline_overrides)).openapi()
    switched = create_app(_settings(**overrides)).openapi()

    assert switched == baseline
    assert INGEST_PATH not in switched["paths"]
    assert not any(path.startswith("/ingest") for path in switched["paths"])


@pytest.mark.parametrize("overrides", INGEST_OFF_COMBINATIONS, ids=repr)
def test_ingest_off_mounts_none_of_the_p3_routes(overrides: dict[str, object]) -> None:
    app = create_app(_settings(**overrides))
    operations = _operations(app)

    assert INGEST_OPERATION not in operations
    assert INGEST_TENANT_OPERATIONS.isdisjoint(operations)
    assert not any(
        "ingest-credentials" in path or path.endswith("/status") for path, _ in operations
    )


def test_ingest_on_adds_exactly_the_d15_operations() -> None:
    baseline = _operations(create_app(_settings(api_v2_enabled=True)))
    app = create_app(_settings(**INGEST_ON))

    assert _operations(app) == baseline | {INGEST_OPERATION} | INGEST_TENANT_OPERATIONS
    assert baseline.isdisjoint({INGEST_OPERATION} | INGEST_TENANT_OPERATIONS)


def test_ingest_on_changes_nothing_outside_the_d15_operations() -> None:
    baseline = create_app(_settings(api_v2_enabled=True)).openapi()["paths"]
    switched = create_app(_settings(**INGEST_ON)).openapi()["paths"]

    for path, methods in baseline.items():
        assert switched[path] == methods, path


def test_every_new_tenant_route_requires_tenant_membership() -> None:
    app = create_app(_settings(**INGEST_ON))

    for path, method in INGEST_TENANT_OPERATIONS:
        route = _route(app, path, method)
        assert require_tenant_access in set(_dependency_calls(route.dependant)), (path, method)


def test_ingest_route_is_not_tenant_scoped_and_has_no_user_authentication() -> None:
    app = create_app(_settings(**INGEST_ON))
    route = _route(app, *INGEST_OPERATION)

    assert "{tenant_id}" not in route.path_format
    calls = set(_dependency_calls(route.dependant))
    assert require_tenant_access not in calls
    # No user authentication of any kind (Cognito or dev): the ingest credential is the only
    # authority, read by the route itself.
    for call in calls:
        module = getattr(call, "__module__", "") or ""
        assert not module.startswith(("limnopulse_api.auth", "limnopulse_api.api.dependencies")), (
            call
        )


def test_no_new_component_name_collides_with_a_v1_component() -> None:
    v1_schemas = set(json.loads(GOLDEN.read_text(encoding="utf-8"))["components"]["schemas"])
    baseline = _schemas(create_app(_settings(api_v2_enabled=True, api_v2_telemetry_enabled=True)))
    switched = _schemas(create_app(_settings(**INGEST_ON, api_v2_telemetry_enabled=True)))
    new = switched - baseline

    assert new, "the ingest routes must add their own component schemas"
    assert new.isdisjoint(v1_schemas)
    # FastAPI renames a colliding name with a module prefix, so the prefix rule also guards it.
    for name in new:
        assert name.startswith(ALLOWED_SCHEMA_PREFIXES), name


def test_router_builders_default_to_ingest_off() -> None:
    assert inspect.signature(build_api_router).parameters["enable_ingest"].default is False
    assert inspect.signature(build_v2_router).parameters["enable_ingest"].default is False


@pytest.mark.parametrize("telegram_webhook_enabled", [True, False])
@pytest.mark.parametrize("telemetry", [False, True])
def test_api_router_builder_gates_the_d15_surface_behind_enable_ingest(
    telegram_webhook_enabled: bool, telemetry: bool
) -> None:
    kwargs = {
        "telegram_webhook_enabled": telegram_webhook_enabled,
        "enable_v2": True,
        "enable_v2_telemetry": telemetry,
    }
    default = _router_operations(build_api_router(**kwargs))
    explicit_off = _router_operations(build_api_router(**kwargs, enable_ingest=False))
    on = _router_operations(build_api_router(**kwargs, enable_ingest=True))

    assert explicit_off == default
    assert on == default | {INGEST_OPERATION} | INGEST_TENANT_OPERATIONS


def test_v2_router_builder_mounts_only_the_five_tenant_routes() -> None:
    m1 = _router_operations(build_v2_router())
    on = _router_operations(build_v2_router(enable_ingest=True))
    expected = {(path.removeprefix("/v2"), method) for path, method in INGEST_TENANT_OPERATIONS}

    assert _router_operations(build_v2_router(enable_ingest=False)) == m1
    assert on == m1 | expected


# --- Settings validator, pinned from the app side --------------------------------------------


@pytest.mark.parametrize(
    "missing",
    ["API_V2_ENABLED", "TELEMETRY_V2_WRITE_ENABLED", "SQS_TELEMETRY_INGRESS_QUEUE_URL"],
)
def test_ingest_env_without_a_prerequisite_env_is_rejected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, missing: str
) -> None:
    """`create_app()` reads the settings from the environment; it must refuse the switch."""
    monkeypatch.chdir(tmp_path)  # no stray `.env`
    environment = {
        "APP_ENV": "test",
        "AUTH_MODE": "dev",
        "API_V2_ENABLED": "true",
        "TELEMETRY_V2_WRITE_ENABLED": "true",
        "SQS_TELEMETRY_INGRESS_QUEUE_URL": QUEUE_URL,
        "INGEST_V2_ENABLED": "true",
    }
    for name, value in environment.items():
        if name != missing:
            monkeypatch.setenv(name, value)
    get_settings.cache_clear()
    try:
        with pytest.raises(ValidationError) as error:
            create_app()
    finally:
        get_settings.cache_clear()

    assert QUEUE_URL not in str(error.value)


def test_ingest_env_with_every_prerequisite_mounts_the_ingest_route(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("AUTH_MODE", "dev")
    monkeypatch.setenv("API_V2_ENABLED", "true")
    monkeypatch.setenv("TELEMETRY_V2_WRITE_ENABLED", "true")
    monkeypatch.setenv("SQS_TELEMETRY_INGRESS_QUEUE_URL", QUEUE_URL)
    monkeypatch.setenv("INGEST_V2_ENABLED", "true")
    get_settings.cache_clear()
    try:
        app = create_app()
    finally:
        get_settings.cache_clear()

    assert INGEST_OPERATION in _operations(app)


# --- OpenAPI generation and the lifespan ---------------------------------------------------------


def test_openapi_generation_with_ingest_on_creates_no_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*args: object, **kwargs: object) -> object:
        raise AssertionError("OpenAPI generation must not create runtime clients")

    monkeypatch.setattr("limnopulse_api.main.boto3.client", fail)
    monkeypatch.setattr("limnopulse_api.main.redis.from_url", fail)
    monkeypatch.setattr("limnopulse_api.main.InfluxDBClient", fail)

    app = create_app(_settings(**INGEST_ON, api_v2_telemetry_enabled=True))

    assert INGEST_PATH in app.openapi()["paths"]
    for name in INGESTION_STATE_NAMES:
        assert not hasattr(app.state, name), name


class FakeRedisClient:
    async def aclose(self, close_connection_pool=None) -> None:
        return None


class FakeInfluxClient:
    def __init__(self, **kwargs: object) -> None:
        self.closed = False

    def query_api(self, *args: object, **kwargs: object) -> object:
        return object()

    def write_api(self, *args: object, **kwargs: object) -> object:
        return object()

    def close(self) -> None:
        self.closed = True


class FakeDynamoClient:
    """Answers every `GetItem` with no item and records it."""

    def __init__(self) -> None:
        self.get_item_calls: list[dict[str, Any]] = []

    def get_item(self, **kwargs: Any) -> dict[str, Any]:
        self.get_item_calls.append(kwargs)
        return {}

    def __getattr__(self, name: str) -> object:
        raise AssertionError(f"unexpected DynamoDB call: {name}")


class FakeSqsClient:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []

    def send_message(self, **kwargs: Any) -> dict[str, str]:
        self.sent.append(kwargs)
        return {"MessageId": "msg-1"}

    def __getattr__(self, name: str) -> object:
        raise AssertionError(f"the API never calls SQS {name}")


class Runtime:
    instances: ClassVar[list["Runtime"]] = []

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.dynamo = FakeDynamoClient()
        self.sqs = FakeSqsClient()

    def client(self, service_name: str, **kwargs: Any) -> object:
        self.calls.append((service_name, kwargs))
        if service_name == "dynamodb":
            return self.dynamo
        if service_name == "sqs":
            return self.sqs
        raise AssertionError(f"unexpected boto3 client: {service_name}")

    def services(self) -> list[str]:
        return [name for name, _ in self.calls]


@pytest.fixture
def runtime(monkeypatch: pytest.MonkeyPatch) -> Runtime:
    fake = Runtime()
    monkeypatch.setattr("limnopulse_api.main.boto3.client", fake.client)
    monkeypatch.setattr("limnopulse_api.main.redis.from_url", lambda url: FakeRedisClient())
    monkeypatch.setattr("limnopulse_api.main.InfluxDBClient", FakeInfluxClient)
    return fake


@pytest.mark.parametrize("overrides", INGEST_OFF_COMBINATIONS, ids=repr)
def test_lifespan_with_ingest_off_builds_no_p3_port_and_no_sqs_client(
    runtime: Runtime, overrides: dict[str, object]
) -> None:
    app = create_app(_settings(**overrides))

    with TestClient(app):
        assert runtime.services() == ["dynamodb"]
        for name in INGESTION_STATE_NAMES:
            assert not hasattr(app.state, name), name


def test_lifespan_with_ingest_on_builds_every_frozen_port(runtime: Runtime) -> None:
    settings = _settings(
        **INGEST_ON,
        sqs_endpoint_url=SQS_ENDPOINT,
        ingest_rate_limit_per_minute=240,
        ingest_rate_limit_burst=7,
        ingest_rate_limiter_max_keys=500,
        ingest_preauth_rate_limit_per_minute=90,
        ingest_preauth_global_per_minute=1200,
    )
    app = create_app(settings)

    with TestClient(app):
        assert sorted(runtime.services()) == ["dynamodb", "sqs"]
        state = app.state
        for name in INGESTION_STATE_NAMES:
            assert hasattr(state, name), name

        credentials = state.ingest_credential_repository
        assert isinstance(credentials, DynamoIngestCredentialRepository)
        assert credentials.client is runtime.dynamo
        assert credentials.table_name == DOMAIN_TABLE
        assert credentials.audit_table_name == AUDIT_TABLE

        authenticator = state.connector_authenticator
        assert isinstance(authenticator, IngestCredentialAuthenticator)
        assert authenticator.credentials is credentials
        assert isinstance(authenticator.integrations, DynamoIntegrationRepository)
        assert authenticator.integrations.client is runtime.dynamo

        lifecycle = state.integration_lifecycle_repository
        assert isinstance(lifecycle, DynamoIntegrationRepository)
        assert lifecycle.client is runtime.dynamo
        assert lifecycle.table_name == DOMAIN_TABLE
        assert lifecycle.audit_table_name == AUDIT_TABLE

        mapping = state.trusted_mapping
        assert isinstance(mapping, TrustedMappingService)
        assert isinstance(mapping.resolver, DynamoIntegrationRepository)
        assert isinstance(mapping.integrations, DynamoIntegrationRepository)
        assert isinstance(mapping.devices, DynamoDeviceComponentRepository)
        assert isinstance(mapping.components, DynamoDeviceComponentRepository)
        for port in (mapping.resolver, mapping.integrations, mapping.devices, mapping.components):
            assert port.client is runtime.dynamo
            assert port.table_name == DOMAIN_TABLE

        receipts = state.ingress_receipt_repository
        assert isinstance(receipts, DynamoIngressReceiptRepository)
        assert receipts.client is runtime.dynamo
        assert receipts.table_name == DOMAIN_TABLE

        assert isinstance(state.ingress_queue_producer, SqsIngressQueue)
        assert isinstance(state.ingress_entitlement_check, AllowDirectIngressEntitlements)
        assert isinstance(state.telemetry_metrics, StructuredLogTelemetryMetrics)


def test_lifespan_producer_sends_to_the_configured_queue_through_the_sqs_client(
    runtime: Runtime,
) -> None:
    # The DLQ URL is set too: admission must enqueue on the ingress queue, never the DLQ.
    app = create_app(
        _settings(
            **INGEST_ON,
            sqs_endpoint_url=SQS_ENDPOINT,
            sqs_telemetry_ingress_dlq_url=QUEUE_URL + "-dlq",
        )
    )

    with TestClient(app):
        message_id = asyncio.run(app.state.ingress_queue_producer.send_raw("{}"))

    assert message_id == "msg-1"
    assert [call["QueueUrl"] for call in runtime.sqs.sent] == [QUEUE_URL]


def test_lifespan_sqs_client_uses_the_sqs_client_kwargs(runtime: Runtime) -> None:
    settings = _settings(**INGEST_ON, sqs_endpoint_url=SQS_ENDPOINT)
    app = create_app(settings)

    with TestClient(app):
        (sqs_kwargs,) = [kwargs for name, kwargs in runtime.calls if name == "sqs"]

    expected = sqs_client_kwargs(settings)
    assert {key: sqs_kwargs.get(key) for key in expected} == expected


def test_lifespan_builds_three_distinct_limiters_from_the_settings(runtime: Runtime) -> None:
    app = create_app(
        _settings(
            **INGEST_ON,
            ingest_rate_limit_per_minute=240,
            ingest_rate_limit_burst=7,
            ingest_rate_limiter_max_keys=500,
            ingest_preauth_rate_limit_per_minute=90,
            ingest_preauth_global_per_minute=1200,
        )
    )

    with TestClient(app):
        global_limiter = app.state.ingress_preauth_global_rate_limiter
        preauth = app.state.ingress_preauth_rate_limiter
        account = app.state.ingress_rate_limiter

    limiters = (global_limiter, preauth, account)
    assert all(isinstance(limiter, TokenBucketRateLimiter) for limiter in limiters)
    assert len({id(limiter) for limiter in limiters}) == 3
    # Pre-auth capacity = max(1, per_minute // 10); refill = per_minute / 60 per second.
    assert (global_limiter.capacity, global_limiter.refill_per_second) == (120, 20.0)
    assert global_limiter.max_keys == 1  # one process-global bucket, keyed "*"
    assert (preauth.capacity, preauth.refill_per_second, preauth.max_keys) == (9, 1.5, 500)
    assert (account.capacity, account.refill_per_second, account.max_keys) == (7, 4.0, 500)


@pytest.mark.parametrize("prewired_name", INGESTION_STATE_NAMES)
def test_prewired_ingestion_state_skips_runtime_wiring(
    monkeypatch: pytest.MonkeyPatch, prewired_name: str
) -> None:
    def fail(*args: object, **kwargs: object) -> object:
        raise AssertionError("prewired test apps must not create runtime clients")

    monkeypatch.setattr("limnopulse_api.main.boto3.client", fail)
    monkeypatch.setattr("limnopulse_api.main.redis.from_url", fail)
    monkeypatch.setattr("limnopulse_api.main.InfluxDBClient", fail)
    app = create_app(_settings(**INGEST_ON))
    prewired = object()
    setattr(app.state, prewired_name, prewired)

    with TestClient(app):
        assert getattr(app.state, prewired_name) is prewired
        assert not hasattr(app.state, "domain_repository")
        for name in INGESTION_STATE_NAMES:
            if name != prewired_name:
                assert not hasattr(app.state, name), name


# --- sqs_client_kwargs -----------------------------------------------------------------------------


def test_api_and_worker_share_one_sqs_client_kwargs_helper() -> None:
    from limnopulse_api.workers import telemetry_ingest

    assert sqs_client_kwargs is telemetry_ingest.sqs_client_kwargs


def test_sqs_client_kwargs_use_local_credentials_only_for_a_local_endpoint() -> None:
    local = _settings(aws_region="us-east-2", sqs_endpoint_url=SQS_ENDPOINT)
    no_endpoint = _settings(aws_region="us-east-2")

    assert sqs_client_kwargs(local) == {
        "region_name": "us-east-2",
        "endpoint_url": SQS_ENDPOINT,
        "aws_access_key_id": "local",
        "aws_secret_access_key": "local",
    }
    assert sqs_client_kwargs(no_endpoint) == {"region_name": "us-east-2"}


def test_sqs_client_kwargs_never_inject_dummy_credentials_outside_local_and_test() -> None:
    settings = Settings(
        _env_file=None,
        app_env="prod",
        auth_mode="cognito",
        aws_region="us-east-2",
        telegram_webhook_enabled=False,
        sqs_endpoint_url=SQS_ENDPOINT,
    )

    kwargs = sqs_client_kwargs(settings)

    assert kwargs == {"region_name": "us-east-2", "endpoint_url": SQS_ENDPOINT}


# --- The mounted surface over the composed app -------------------------------------------------


class _Membership:
    def __init__(self, role: TenantRole | None) -> None:
        self.role = role
        self.calls = 0

    async def get_active_membership(self, cognito_sub: str, tenant_id: str) -> Membership | None:
        from datetime import UTC, datetime

        self.calls += 1
        if self.role is None or cognito_sub != "sub_1" or tenant_id != TENANT:
            return None
        now = datetime(2026, 10, 31, 12, 0, tzinfo=UTC)
        return Membership(
            tenant_id=tenant_id,
            cognito_sub=cognito_sub,
            role=self.role,
            created_at=now,
            updated_at=now,
            version=1,
        )


class _Unreachable:
    def __getattr__(self, name: str) -> object:
        raise AssertionError(f"must not be reached: {name}")


def test_composed_ingest_route_rejects_a_missing_bearer_before_any_store_read(
    runtime: Runtime,
) -> None:
    app = create_app(_settings(**INGEST_ON))

    with TestClient(app, raise_server_exceptions=False) as http:
        response = http.post(
            INGEST_PATH, content=b"{}", headers={"content-type": "application/json"}
        )

    assert response.status_code == 401
    assert response.json() == {"detail": "unauthorized"}
    assert response.headers["www-authenticate"] == "Bearer"
    assert runtime.dynamo.get_item_calls == []
    assert runtime.sqs.sent == []


def test_composed_ingest_route_authenticates_through_the_credential_store(
    runtime: Runtime,
) -> None:
    ingest_credential_id = "ic_" + "a" * 32
    token = f"lpi_{ingest_credential_id}_" + "B" * 43
    app = create_app(_settings(**INGEST_ON))

    with TestClient(app, raise_server_exceptions=False) as http:
        response = http.post(
            INGEST_PATH,
            content=b"{}",
            headers={"content-type": "application/json", "authorization": f"Bearer {token}"},
        )

    assert response.status_code == 401
    assert response.json() == {"detail": "unauthorized"}
    (call,) = runtime.dynamo.get_item_calls
    assert call["TableName"] == DOMAIN_TABLE
    assert call["ConsistentRead"] is True
    assert ingest_credential_id in json.dumps(call["Key"])
    assert token not in json.dumps(call)
    assert runtime.sqs.sent == []


@pytest.mark.parametrize("overrides", INGEST_OFF_COMBINATIONS, ids=repr)
def test_ingest_route_is_404_while_the_switch_is_off(
    runtime: Runtime, overrides: dict[str, object]
) -> None:
    app = create_app(_settings(**overrides))

    with TestClient(app, raise_server_exceptions=False) as http:
        response = http.post(
            INGEST_PATH, content=b"{}", headers={"content-type": "application/json"}
        )

    assert response.status_code == 404


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("get", f"/v2/tenants/{TENANT}/integration-accounts/ia_{'1' * 32}/ingest-credentials"),
        ("post", f"/v2/tenants/{TENANT}/integration-accounts/ia_{'1' * 32}/status"),
    ],
)
def test_composed_tenant_routes_deny_a_viewer_before_any_store_read(
    runtime: Runtime, method: str, path: str
) -> None:
    app = create_app(_settings(**INGEST_ON))

    with TestClient(app, raise_server_exceptions=False) as http:
        membership = _Membership(TenantRole.VIEWER)
        app.state.membership_service = membership
        app.state.integration_repository = _Unreachable()
        app.state.ingest_credential_repository = _Unreachable()
        app.state.integration_lifecycle_repository = _Unreachable()
        response = http.request(
            method, path, headers=HEADERS, json={"status": "active", "expected_version": 1}
        )

    assert response.status_code == 403
    assert response.json() == {"detail": "tenant access denied"}
    assert membership.calls == 1


@pytest.mark.parametrize(
    "path",
    [
        f"/v2/tenants/{TENANT}/integration-accounts/ia_{'1' * 32}/ingest-credentials",
        f"/v2/tenants/{TENANT}/integration-accounts/ia_{'1' * 32}/status",
    ],
)
@pytest.mark.parametrize("overrides", INGEST_OFF_COMBINATIONS, ids=repr)
def test_tenant_routes_are_404_while_the_switch_is_off(
    runtime: Runtime, path: str, overrides: dict[str, object]
) -> None:
    app = create_app(_settings(**overrides))

    with TestClient(app, raise_server_exceptions=False) as http:
        app.state.membership_service = _Membership(TenantRole.OWNER)
        response = http.post(
            path, headers=HEADERS, json={"status": "active", "expected_version": 1}
        )

    assert response.status_code == 404
