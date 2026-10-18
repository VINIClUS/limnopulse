"""P2-08 composition: switch-gated `/v2` telemetry routes, lifespan reader/writer wiring and the
`/v1` read-source swap, with every switch off equal to the M1 baseline."""

import inspect
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from influxdb_client.client.write_api import SYNCHRONOUS, WriteOptions, WriteType

from limnopulse_api.adapters.dynamodb import DynamoDomainRepository
from limnopulse_api.adapters.dynamodb_sites_assets import DynamoSiteAssetRepository
from limnopulse_api.adapters.influxdb import InfluxTelemetryRepository
from limnopulse_api.adapters.influxdb_v1_projection import V2ProjectedTelemetryRepository
from limnopulse_api.adapters.influxdb_v2_reader import InfluxObservationReader
from limnopulse_api.adapters.influxdb_v2_writer import InfluxObservationWriter
from limnopulse_api.api.dependencies import require_tenant_access
from limnopulse_api.api.openapi_contract import build_v1_openapi_contract, iter_effective_api_routes
from limnopulse_api.api.router import build_api_router
from limnopulse_api.api.v2.router import build_v2_router
from limnopulse_api.core.config import Settings
from limnopulse_api.domain.entities import Membership
from limnopulse_api.domain.metric_catalog import metric_catalog
from limnopulse_api.domain.roles import TenantRole
from limnopulse_api.domain.telemetry_v2 import METRIC_CATALOG_VERSION
from limnopulse_api.main import create_app
from limnopulse_api.services.telemetry_v2_writes import ObservationWriteService

ROOT = Path(__file__).resolve().parents[2]
GOLDEN = ROOT / "tests/contracts/openapi/v1.json"
TENANT = "tnt_a"
ASSET_ID = "ast_" + "1" * 32
DEVICE_ID = "dev_" + "3" * 32
HEADERS = {"X-Dev-User-Sub": "sub_1"}
INFLUX_ORG = "org_under_test"
INFLUX_BUCKET = "raw_under_test"

M1_V2_PATHS = {
    "/v2/tenants/{tenant_id}/sites",
    "/v2/tenants/{tenant_id}/sites/{site_id}",
    "/v2/tenants/{tenant_id}/assets",
    "/v2/tenants/{tenant_id}/assets/{asset_id}",
    "/v2/tenants/{tenant_id}/devices",
    "/v2/tenants/{tenant_id}/devices/{device_id}",
    "/v2/tenants/{tenant_id}/devices/{device_id}/components",
    "/v2/tenants/{tenant_id}/devices/{device_id}/integrations",
    "/v2/tenants/{tenant_id}/components/{component_id}",
    "/v2/tenants/{tenant_id}/components/{component_id}/capabilities",
    "/v2/tenants/{tenant_id}/integration-accounts",
    "/v2/tenants/{tenant_id}/deployments",
    "/v2/tenants/{tenant_id}/deployments/{deployment_id}/end",
}
TELEMETRY_OPERATIONS = {
    ("/v2/tenants/{tenant_id}/metric-definitions", "get"),
    ("/v2/tenants/{tenant_id}/observations", "get"),
    ("/v2/tenants/{tenant_id}/observations/latest", "get"),
}
TELEMETRY_SCHEMAS = {
    "V2MetricDefinitionResponse",
    "V2MetricDefinitionListResponse",
    "V2ObservationResponse",
    "V2ObservationListResponse",
    "V2ObservationQualityResponse",
}
M1_TENANT_ROUTE_COUNT = 53
TELEMETRY_TENANT_ROUTE_COUNT = 56
FLAG_ENVIRONMENT = (
    "API_V2_ENABLED",
    "V1_POND_DEVICE_PROJECTION_ENABLED",
    "TELEMETRY_V2_WRITE_ENABLED",
    "TELEMETRY_LEGACY_DUAL_WRITE_ENABLED",
    "API_V2_TELEMETRY_ENABLED",
    "V1_TELEMETRY_READ_SOURCE",
)
# Switches that must never change routing or any OpenAPI document.
NON_ROUTING_SWITCHES = (
    {"telemetry_v2_write_enabled": True},
    {"telemetry_legacy_dual_write_enabled": True},
    {"v1_telemetry_read_source": "v2"},
    {
        "telemetry_v2_write_enabled": True,
        "telemetry_legacy_dual_write_enabled": True,
        "v1_telemetry_read_source": "v2",
    },
)
SWITCH_COMBINATIONS = (
    {},
    {"api_v2_enabled": True},
    {"api_v2_enabled": True, "api_v2_telemetry_enabled": True},
    *NON_ROUTING_SWITCHES,
    {
        "api_v2_enabled": True,
        "api_v2_telemetry_enabled": True,
        "telemetry_v2_write_enabled": True,
        "telemetry_legacy_dual_write_enabled": True,
        "v1_telemetry_read_source": "v2",
    },
)


@pytest.fixture(autouse=True)
def _isolate_flag_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in FLAG_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)


def _settings(**overrides: object) -> Settings:
    return Settings(
        _env_file=None,
        app_env="test",
        auth_mode="dev",
        influxdb_org=INFLUX_ORG,
        influxdb_bucket_raw=INFLUX_BUCKET,
        **overrides,
    )


def _operations(paths: dict[str, dict[str, object]]) -> set[tuple[str, str]]:
    return {(path, method) for path, methods in paths.items() for method in methods}


def _v2_operations(app: FastAPI) -> set[tuple[str, str]]:
    paths = app.openapi()["paths"]
    return _operations({p: m for p, m in paths.items() if p == "/v2" or p.startswith("/v2/")})


def _m1_operations() -> set[tuple[str, str]]:
    return _v2_operations(create_app(_settings(api_v2_enabled=True)))


def _dependency_calls(dependant):
    yield dependant.call
    for dependency in dependant.dependencies:
        yield from _dependency_calls(dependency)


def _tenant_routes(app: FastAPI) -> list[Any]:
    return [r for r in iter_effective_api_routes(app) if "{tenant_id}" in r.path_format]


def _router_operations(router) -> set[tuple[str, str]]:
    app = FastAPI()
    app.include_router(router)
    return {
        (route.path_format, method.lower())
        for route in iter_effective_api_routes(app)
        for method in route.methods
    }


# --- Route sets -------------------------------------------------------------------------------


def test_everything_off_mounts_no_v2_route() -> None:
    paths = create_app(_settings()).openapi()["paths"]

    assert not any(path == "/v2" or path.startswith("/v2/") for path in paths)


def test_api_v2_alone_mounts_exactly_the_13_m1_paths_and_no_telemetry() -> None:
    app = create_app(_settings(api_v2_enabled=True))
    operations = _v2_operations(app)

    assert {path for path, _ in operations} == M1_V2_PATHS
    assert not any(
        "observation" in path or "metric-definitions" in path or "telemetry" in path
        for path, _ in operations
    )
    assert len(_tenant_routes(app)) == M1_TENANT_ROUTE_COUNT
    assert not TELEMETRY_SCHEMAS & set(app.openapi()["components"]["schemas"])


def test_api_v2_telemetry_adds_exactly_the_three_telemetry_operations() -> None:
    app = create_app(_settings(api_v2_enabled=True, api_v2_telemetry_enabled=True))

    assert _v2_operations(app) == _m1_operations() | TELEMETRY_OPERATIONS
    assert TELEMETRY_OPERATIONS.isdisjoint(_m1_operations())
    assert TELEMETRY_SCHEMAS <= set(app.openapi()["components"]["schemas"])


def test_api_v2_telemetry_raises_tenant_routes_from_53_to_56_all_requiring_membership() -> None:
    app = create_app(_settings(api_v2_enabled=True, api_v2_telemetry_enabled=True))
    routes = _tenant_routes(app)

    assert len(routes) == TELEMETRY_TENANT_ROUTE_COUNT
    for route in routes:
        assert require_tenant_access in set(_dependency_calls(route.dependant)), route.path
    telemetry = {
        (route.path_format, method.lower())
        for route in routes
        for method in route.methods
        if (route.path_format, method.lower()) in TELEMETRY_OPERATIONS
    }
    assert telemetry == TELEMETRY_OPERATIONS


def test_api_v2_telemetry_changes_nothing_outside_v2() -> None:
    off = create_app(_settings()).openapi()["paths"]
    on = create_app(_settings(api_v2_enabled=True, api_v2_telemetry_enabled=True)).openapi()[
        "paths"
    ]

    assert {p: m for p, m in on.items() if not p.startswith("/v2/")} == off


@pytest.mark.parametrize("overrides", SWITCH_COMBINATIONS, ids=repr)
def test_v1_golden_is_identical_in_every_switch_combination(overrides: dict[str, object]) -> None:
    app = create_app(_settings(**overrides))

    assert build_v1_openapi_contract(app) == json.loads(GOLDEN.read_text(encoding="utf-8"))


@pytest.mark.parametrize("overrides", NON_ROUTING_SWITCHES, ids=repr)
@pytest.mark.parametrize("api_v2_enabled", [False, True])
def test_write_and_read_source_switches_never_change_the_openapi_document(
    overrides: dict[str, object], api_v2_enabled: bool
) -> None:
    baseline = create_app(_settings(api_v2_enabled=api_v2_enabled)).openapi()
    switched = create_app(_settings(api_v2_enabled=api_v2_enabled, **overrides)).openapi()

    assert switched == baseline


def test_v2_router_builder_defaults_to_the_m1_routes() -> None:
    parameter = inspect.signature(build_v2_router).parameters["enable_telemetry"]
    assert parameter.default is False
    m1 = {(path.removeprefix("/v2"), method) for path, method in _m1_operations()}
    telemetry = {(path.removeprefix("/v2"), method) for path, method in TELEMETRY_OPERATIONS}

    assert _router_operations(build_v2_router()) == m1
    assert _router_operations(build_v2_router(enable_telemetry=False)) == m1
    assert _router_operations(build_v2_router(enable_telemetry=True)) == m1 | telemetry


@pytest.mark.parametrize("telegram_webhook_enabled", [True, False])
def test_api_router_builder_gates_telemetry_behind_its_own_flag(
    telegram_webhook_enabled: bool,
) -> None:
    parameter = inspect.signature(build_api_router).parameters["enable_v2_telemetry"]
    assert parameter.default is False
    base = _router_operations(build_api_router(telegram_webhook_enabled=telegram_webhook_enabled))
    m1 = _router_operations(
        build_api_router(telegram_webhook_enabled=telegram_webhook_enabled, enable_v2=True)
    )
    m1_explicit = _router_operations(
        build_api_router(
            telegram_webhook_enabled=telegram_webhook_enabled,
            enable_v2=True,
            enable_v2_telemetry=False,
        )
    )
    with_telemetry = _router_operations(
        build_api_router(
            telegram_webhook_enabled=telegram_webhook_enabled,
            enable_v2=True,
            enable_v2_telemetry=True,
        )
    )

    assert m1_explicit == m1
    assert {op for op in m1 if not op[0].startswith("/v2/")} == base
    assert with_telemetry == m1 | TELEMETRY_OPERATIONS


def test_openapi_generation_with_every_switch_on_does_not_enter_lifespan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*args: object, **kwargs: object) -> object:
        raise AssertionError("OpenAPI generation must not create runtime clients")

    monkeypatch.setattr("limnopulse_api.main.boto3.client", fail)
    monkeypatch.setattr("limnopulse_api.main.redis.from_url", fail)
    monkeypatch.setattr("limnopulse_api.main.InfluxDBClient", fail)

    app = create_app(_settings(**SWITCH_COMBINATIONS[-1]))

    assert app.openapi()["paths"]
    for name in ("observation_reader", "observation_writer", "observation_write_service"):
        assert not hasattr(app.state, name)


# --- Lifespan wiring --------------------------------------------------------------------------


class FakeRedisClient:
    async def aclose(self, close_connection_pool=None) -> None:
        return None


class FakeQueryApi:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def query(self, query: str, org: str | None = None) -> list[Any]:
        self.calls.append((query, org))
        return []


class FakeWriteApi:
    def __init__(self, write_options: WriteOptions) -> None:
        self._write_options = write_options

    def write(self, *args: object, **kwargs: object) -> None:
        raise AssertionError("composition tests never write")


class FakeInfluxClient:
    instances: ClassVar[list["FakeInfluxClient"]] = []

    def __init__(self, **kwargs: object) -> None:
        self.init_kwargs = kwargs
        self.query_api_instance = FakeQueryApi()
        self.write_api_calls: list[WriteOptions] = []
        self.write_api_instances: list[FakeWriteApi] = []
        self.close_calls = 0
        FakeInfluxClient.instances.append(self)

    def query_api(self, *args: object, **kwargs: object) -> FakeQueryApi:
        return self.query_api_instance

    def write_api(self, *args: object, **kwargs: object) -> FakeWriteApi:
        options = kwargs.get("write_options", args[0] if args else None)
        # Mirrors the real client: no write_options means the batching default.
        options = options if options is not None else WriteOptions()
        self.write_api_calls.append(options)
        write_api = FakeWriteApi(options)
        self.write_api_instances.append(write_api)
        return write_api

    def close(self) -> None:
        self.close_calls += 1


@pytest.fixture
def runtime(monkeypatch: pytest.MonkeyPatch):
    FakeInfluxClient.instances = []
    fake_dynamo = object()
    monkeypatch.setattr("limnopulse_api.main.boto3.client", lambda *args, **kwargs: fake_dynamo)
    monkeypatch.setattr("limnopulse_api.main.redis.from_url", lambda url: FakeRedisClient())
    monkeypatch.setattr("limnopulse_api.main.InfluxDBClient", FakeInfluxClient)
    return SimpleNamespace(dynamo=fake_dynamo, influx=FakeInfluxClient.instances)


def _single_client(runtime) -> FakeInfluxClient:
    assert len(runtime.influx) == 1
    return runtime.influx[0]


@pytest.mark.parametrize(
    "overrides",
    [
        {},
        {"api_v2_enabled": True},
        {"api_v2_enabled": True, "api_v2_telemetry_enabled": True},
        {"v1_telemetry_read_source": "v2"},
    ],
    ids=repr,
)
def test_lifespan_without_a_write_switch_creates_no_write_api(
    runtime, overrides: dict[str, object]
) -> None:
    app = create_app(_settings(**overrides))

    with TestClient(app):
        client = _single_client(runtime)
        assert client.write_api_calls == []
        assert app.state.observation_writer is None
        service = app.state.observation_write_service
        assert isinstance(service, ObservationWriteService)
        assert service.v2_write_enabled is False
        assert service.legacy_dual_write_enabled is False
        assert service.writer is None
        assert service.legacy_writer is None
        assert service.site_assets is app.state.site_asset_repository

    assert client.close_calls == 1


@pytest.mark.parametrize(
    "overrides",
    [
        {},
        {"api_v2_enabled": True, "api_v2_telemetry_enabled": True},
        {"telemetry_v2_write_enabled": True},
    ],
    ids=repr,
)
def test_lifespan_creates_the_observation_reader_from_the_shared_client(
    runtime, overrides: dict[str, object]
) -> None:
    app = create_app(_settings(**overrides))

    with TestClient(app):
        client = _single_client(runtime)
        reader = app.state.observation_reader
        assert isinstance(reader, InfluxObservationReader)
        assert reader.query_api is client.query_api_instance
        assert reader.org == INFLUX_ORG
        assert reader.bucket == INFLUX_BUCKET
        assert app.state.influxdb_client is client


@pytest.mark.parametrize(("v2_write", "legacy_write"), [(True, False), (False, True), (True, True)])
def test_lifespan_creates_one_synchronous_writer_only_when_a_write_switch_is_on(
    runtime, v2_write: bool, legacy_write: bool
) -> None:
    app = create_app(
        _settings(
            telemetry_v2_write_enabled=v2_write, telemetry_legacy_dual_write_enabled=legacy_write
        )
    )

    with TestClient(app):
        client = _single_client(runtime)
        assert len(client.write_api_calls) == 1
        options = client.write_api_calls[0]
        assert options is SYNCHRONOUS or options.write_type is WriteType.synchronous
        writer = app.state.observation_writer
        assert isinstance(writer, InfluxObservationWriter)
        assert writer.write_api is client.write_api_instances[0]
        assert writer.bucket == INFLUX_BUCKET
        assert writer.org == INFLUX_ORG
        service = app.state.observation_write_service
        assert isinstance(service, ObservationWriteService)
        assert service.v2_write_enabled is v2_write
        assert service.legacy_dual_write_enabled is legacy_write
        assert service.site_assets is app.state.site_asset_repository
        assert isinstance(service.site_assets, DynamoSiteAssetRepository)
        if v2_write:
            assert service.writer is writer
        else:
            assert service.writer in (None, writer)
        if legacy_write:
            assert service.legacy_writer is writer
        else:
            assert service.legacy_writer in (None, writer)
        assert type(app.state.telemetry_repository) is InfluxTelemetryRepository

    assert client.close_calls == 1


@pytest.mark.parametrize(
    "overrides",
    [
        {},
        {"telemetry_v2_write_enabled": True},
        {"api_v2_enabled": True, "api_v2_telemetry_enabled": True},
    ],
    ids=repr,
)
def test_default_read_source_keeps_the_legacy_v1_repository(
    runtime, overrides: dict[str, object]
) -> None:
    app = create_app(_settings(**overrides))

    with TestClient(app):
        client = _single_client(runtime)
        repository = app.state.telemetry_repository
        assert type(repository) is InfluxTelemetryRepository
        assert repository.query_api is client.query_api_instance
        assert repository.org == INFLUX_ORG
        assert repository.bucket == INFLUX_BUCKET


def test_explicit_legacy_read_source_keeps_the_legacy_v1_repository(runtime) -> None:
    app = create_app(_settings(v1_telemetry_read_source="legacy"))

    with TestClient(app):
        assert type(app.state.telemetry_repository) is InfluxTelemetryRepository


def test_v2_read_source_swaps_the_v1_repository_for_the_projection(runtime) -> None:
    app = create_app(_settings(v1_telemetry_read_source="v2"))

    with TestClient(app):
        client = _single_client(runtime)
        repository = app.state.telemetry_repository
        assert isinstance(repository, V2ProjectedTelemetryRepository)
        assert repository.query_api is client.query_api_instance
        assert repository.org == INFLUX_ORG
        assert repository.bucket == INFLUX_BUCKET
        assert repository.domain_repository is app.state.domain_repository
        assert isinstance(app.state.domain_repository, DynamoDomainRepository)
        assert client.write_api_calls == []
        assert app.state.observation_writer is None


@pytest.mark.parametrize(
    "prewired_name", ["observation_reader", "observation_writer", "observation_write_service"]
)
def test_prewired_p2_state_skips_runtime_wiring(
    monkeypatch: pytest.MonkeyPatch, prewired_name: str
) -> None:
    def fail(*args: object, **kwargs: object) -> object:
        raise AssertionError("prewired test apps must not create runtime clients")

    monkeypatch.setattr("limnopulse_api.main.boto3.client", fail)
    monkeypatch.setattr("limnopulse_api.main.redis.from_url", fail)
    monkeypatch.setattr("limnopulse_api.main.InfluxDBClient", fail)
    app = create_app(_settings(api_v2_enabled=True, api_v2_telemetry_enabled=True))
    prewired = object()
    setattr(app.state, prewired_name, prewired)

    with TestClient(app):
        assert getattr(app.state, prewired_name) is prewired
        assert not hasattr(app.state, "domain_repository")


# --- Mounted routes over the composed app -----------------------------------------------------


class _Membership:
    def __init__(self, role: TenantRole | None) -> None:
        self.role = role
        self.calls = 0

    async def get_active_membership(self, cognito_sub: str, tenant_id: str) -> Membership | None:
        self.calls += 1
        if self.role is None or cognito_sub != "sub_1" or tenant_id != TENANT:
            return None
        now = datetime(2026, 10, 18, 12, 0, tzinfo=UTC)
        return Membership(
            tenant_id=tenant_id,
            cognito_sub=cognito_sub,
            role=self.role,
            created_at=now,
            updated_at=now,
            version=1,
        )


class _Assets:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def get_asset(self, tenant_id: str, asset_id: str) -> object | None:
        self.calls.append((tenant_id, asset_id))
        return SimpleNamespace(tenant_id=tenant_id, asset_id=asset_id)

    def __getattr__(self, name: str) -> object:
        raise AssertionError(f"unexpected site/asset call: {name}")


class _Unreachable:
    def __getattr__(self, name: str) -> object:
        raise AssertionError(f"must not be reached: {name}")


def test_composed_observation_route_reads_through_the_lifespan_reader(runtime) -> None:
    app = create_app(_settings(api_v2_enabled=True, api_v2_telemetry_enabled=True))

    with TestClient(app, raise_server_exceptions=False) as http:
        client = _single_client(runtime)
        app.state.membership_service = _Membership(TenantRole.VIEWER)
        assets = _Assets()
        app.state.site_asset_repository = assets
        app.state.device_component_repository = _Unreachable()
        response = http.get(
            f"/v2/tenants/{TENANT}/observations/latest",
            params={"asset_id": ASSET_ID},
            headers=HEADERS,
        )

    assert response.status_code == 200, response.text
    assert response.json() == {"items": []}
    assert assets.calls == [(TENANT, ASSET_ID)]
    queries = client.query_api_instance.calls
    assert len(queries) == 1
    query, org = queries[0]
    assert org == INFLUX_ORG
    assert json.dumps(INFLUX_BUCKET) in query
    assert "observation_numeric" in query


def test_composed_metric_definitions_route_returns_the_catalog(runtime) -> None:
    app = create_app(_settings(api_v2_enabled=True, api_v2_telemetry_enabled=True))

    with TestClient(app, raise_server_exceptions=False) as http:
        app.state.membership_service = _Membership(TenantRole.VIEWER)
        response = http.get(f"/v2/tenants/{TENANT}/metric-definitions", headers=HEADERS)

    assert response.status_code == 200, response.text
    items = response.json()["items"]
    assert [item["metric_key"] for item in items] == [
        definition.metric_key for definition in metric_catalog(METRIC_CATALOG_VERSION)
    ]


@pytest.mark.parametrize(
    "path",
    [
        f"/v2/tenants/{TENANT}/metric-definitions",
        f"/v2/tenants/{TENANT}/observations?device_id={DEVICE_ID}",
        f"/v2/tenants/{TENANT}/observations/latest?device_id={DEVICE_ID}",
    ],
)
def test_composed_telemetry_routes_deny_non_members_before_any_lookup(runtime, path: str) -> None:
    app = create_app(_settings(api_v2_enabled=True, api_v2_telemetry_enabled=True))

    with TestClient(app, raise_server_exceptions=False) as http:
        client = _single_client(runtime)
        membership = _Membership(None)
        app.state.membership_service = membership
        app.state.site_asset_repository = _Unreachable()
        app.state.device_component_repository = _Unreachable()
        response = http.get(path, headers=HEADERS)

    assert response.status_code == 403
    assert response.json() == {"detail": "tenant access denied"}
    assert membership.calls == 1
    assert client.query_api_instance.calls == []


@pytest.mark.parametrize(
    "path",
    [
        f"/v2/tenants/{TENANT}/metric-definitions",
        f"/v2/tenants/{TENANT}/observations?device_id={DEVICE_ID}",
        f"/v2/tenants/{TENANT}/observations/latest?device_id={DEVICE_ID}",
    ],
)
@pytest.mark.parametrize(
    "overrides",
    [{"api_v2_enabled": True}, {"telemetry_v2_write_enabled": True}, {}],
    ids=repr,
)
def test_telemetry_routes_are_404_unless_api_v2_telemetry_is_on(
    runtime, path: str, overrides: dict[str, object]
) -> None:
    app = create_app(_settings(**overrides))

    with TestClient(app, raise_server_exceptions=False) as http:
        app.state.membership_service = _Membership(TenantRole.OWNER)
        response = http.get(path, headers=HEADERS)

    assert response.status_code == 404
