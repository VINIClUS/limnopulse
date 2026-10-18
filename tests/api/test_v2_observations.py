"""P2-04 `/v2` telemetry API on an isolated app with the unmounted routers."""

import inspect
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

from limnopulse_api.api.openapi_contract import build_v1_openapi_contract
from limnopulse_api.api.v2 import telemetry_dependencies
from limnopulse_api.api.v2.routers import metric_definitions as metric_definitions_router
from limnopulse_api.api.v2.routers import observations as observations_router
from limnopulse_api.api.v2.routers.devices import V2JsonSafeRoute
from limnopulse_api.api.v2.schemas import metric_definitions as metric_definitions_schemas
from limnopulse_api.api.v2.schemas import observations as observations_schemas
from limnopulse_api.auth.dev import DevAuthProvider
from limnopulse_api.core.config import Settings
from limnopulse_api.core.errors import TelemetryQueryError
from limnopulse_api.domain.entities import Membership
from limnopulse_api.domain.metric_catalog import metric_catalog
from limnopulse_api.domain.roles import TenantRole
from limnopulse_api.domain.telemetry_v2 import (
    MAX_TELEMETRY_TIME,
    METRIC_CATALOG_VERSION,
    MIN_TELEMETRY_TIME,
    POINT_QUALITY_STATES,
    QualityFlag,
    QualityProvenance,
    QualityState,
    SourceType,
    StoredObservation,
)
from limnopulse_api.domain.v2_common import format_utc_timestamp
from limnopulse_api.main import create_app
from limnopulse_api.repositories.telemetry_v2 import ObservationSelector

ROOT = Path(__file__).resolve().parents[2]
GOLDEN = ROOT / "tests/contracts/openapi/v1.json"
NOW = datetime(2026, 10, 18, 12, 0, tzinfo=UTC)
TENANT = "tnt_a"
OTHER_TENANT = "tnt_other"
ASSET_ID = "ast_" + "1" * 32
COMPONENT_ID = "cmp_" + "2" * 32
DEVICE_ID = "dev_" + "3" * 32
OTHER_ASSET_ID = "ast_" + "4" * 32
HEADERS = {"X-Dev-User-Sub": "sub_1"}
OBSERVATIONS = f"/tenants/{TENANT}/observations"
LATEST = f"/tenants/{TENANT}/observations/latest"
DEFINITIONS = f"/tenants/{TENANT}/metric-definitions"
ITEM_KEYS = {
    "event_id",
    "observed_at",
    "received_at",
    "ingested_at",
    "metric_key",
    "value",
    "unit",
    "site_id",
    "asset_id",
    "device_id",
    "component_id",
    "probe_id",
    "deployment_id",
    "quality",
    "sequence",
    "source_type",
}


def stored(
    *,
    metric_key: str = "water.dissolved_oxygen",
    probe_id: str | None = "prb_" + "5" * 32,
    sequence: int | None = 7,
) -> StoredObservation:
    return StoredObservation(
        event_id="obs_" + "6" * 32,
        observed_at=datetime(2026, 10, 18, 11, 30, 0, 250000, tzinfo=UTC),
        received_at=datetime(2026, 10, 18, 11, 30, 1, tzinfo=UTC),
        ingested_at=datetime(2026, 10, 18, 11, 30, 2, 500, tzinfo=UTC),
        metric_key=metric_key,
        value=6.4,
        canonical_unit="mg/L",
        site_id="site_" + "7" * 32,
        asset_id=ASSET_ID,
        device_id=DEVICE_ID,
        component_id=COMPONENT_ID,
        probe_id=probe_id,
        deployment_id="dep_" + "8" * 32,
        quality_state=QualityState.OUT_OF_RANGE,
        quality_flags=(QualityFlag.DELAYED, QualityFlag.REPLAY),
        quality_provenance=(
            QualityProvenance.SCHEMA_VALIDATION,
            QualityProvenance.UNIT_MAPPING,
            QualityProvenance.PLAUSIBLE_RANGE,
            QualityProvenance.TIMELINESS,
            QualityProvenance.REPLAY,
        ),
        sequence=sequence,
        source_type=SourceType.HTTPS,
    )


class FakeMembershipService:
    def __init__(self, role: TenantRole) -> None:
        self.role = role

    async def get_active_membership(self, cognito_sub: str, tenant_id: str) -> Membership | None:
        if (cognito_sub, tenant_id) != ("sub_1", TENANT):
            return None
        return Membership(
            tenant_id=tenant_id,
            cognito_sub=cognito_sub,
            role=self.role,
            created_at=NOW,
            updated_at=NOW,
            version=1,
        )


class FakeReader:
    def __init__(
        self, items: list[StoredObservation] | None = None, error: Exception | None = None
    ):
        self.items = items if items is not None else []
        self.error = error
        self.calls: list[tuple[str, dict[str, object]]] = []

    async def query_observations(self, **kwargs: object) -> list[StoredObservation]:
        self.calls.append(("query_observations", kwargs))
        if self.error is not None:
            raise self.error
        return list(self.items)

    async def query_latest_observations(self, **kwargs: object) -> list[StoredObservation]:
        self.calls.append(("query_latest_observations", kwargs))
        if self.error is not None:
            raise self.error
        return list(self.items)


class FakeEntities:
    """`site_asset_repository` and `device_component_repository` are separate objects."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []
        self.site_assets = FakeSiteAssets(self.calls)
        self.device_components = FakeDeviceComponents(self.calls)


class FakeSiteAssets:
    def __init__(self, calls: list[tuple[str, str, str]]) -> None:
        self.calls = calls
        self.assets = {(TENANT, ASSET_ID), (OTHER_TENANT, OTHER_ASSET_ID)}

    async def get_site(self, tenant_id: str, site_id: str) -> object | None:
        self.calls.append(("get_site", tenant_id, site_id))
        return None

    async def get_asset(self, tenant_id: str, asset_id: str) -> object | None:
        self.calls.append(("get_asset", tenant_id, asset_id))
        found = (tenant_id, asset_id) in self.assets
        return SimpleNamespace(tenant_id=tenant_id, asset_id=asset_id) if found else None


class FakeDeviceComponents:
    def __init__(self, calls: list[tuple[str, str, str]]) -> None:
        self.calls = calls
        self.devices = {(TENANT, DEVICE_ID)}
        self.components = {(TENANT, COMPONENT_ID)}

    async def get_device(self, tenant_id: str, device_id: str) -> object | None:
        self.calls.append(("get_device", tenant_id, device_id))
        found = (tenant_id, device_id) in self.devices
        return SimpleNamespace(tenant_id=tenant_id, device_id=device_id) if found else None

    async def get_component(self, tenant_id: str, component_id: str) -> object | None:
        self.calls.append(("get_component", tenant_id, component_id))
        found = (tenant_id, component_id) in self.components
        return SimpleNamespace(tenant_id=tenant_id, component_id=component_id) if found else None


class Harness(SimpleNamespace):
    client: TestClient
    reader: FakeReader
    entities: FakeEntities
    app: FastAPI


def build_app(
    *,
    role: TenantRole = TenantRole.VIEWER,
    reader: FakeReader | None = None,
    with_reader: bool = True,
) -> Harness:
    app = FastAPI()
    app.include_router(metric_definitions_router.router)
    app.include_router(observations_router.router)
    app.state.auth_provider = DevAuthProvider()
    app.state.membership_service = FakeMembershipService(role)
    entities = FakeEntities()
    app.state.site_asset_repository = entities.site_assets
    app.state.device_component_repository = entities.device_components
    reader = reader if reader is not None else FakeReader([stored()])
    app.state.observation_reader = reader if with_reader else None
    app.dependency_overrides[telemetry_dependencies.get_clock] = lambda: lambda: NOW
    return Harness(
        client=TestClient(app, headers=HEADERS), reader=reader, entities=entities, app=app
    )


def iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def get(harness: Harness, path: str, params: dict | list | None = None):
    return harness.client.get(path, params=params)


def query_kwargs(harness: Harness) -> dict[str, object]:
    assert len(harness.reader.calls) == 1
    name, kwargs = harness.reader.calls[0]
    assert name == "query_observations"
    return kwargs


# --- routers stay unmounted ----------------------------------------------------------------


def _settings(**overrides: object) -> Settings:
    return Settings(_env_file=None, app_env="test", auth_mode="dev", **overrides)


@pytest.mark.parametrize("overrides", [{}, {"api_v2_enabled": True}])
def test_routers_are_not_mounted_by_the_application(
    monkeypatch: pytest.MonkeyPatch, overrides: dict[str, object]
) -> None:
    monkeypatch.delenv("API_V2_ENABLED", raising=False)
    monkeypatch.delenv("V1_POND_DEVICE_PROJECTION_ENABLED", raising=False)
    paths = create_app(_settings(**overrides)).openapi()["paths"]
    assert not [p for p in paths if "observations" in p or "metric-definitions" in p]


def test_routers_follow_m1_route_conventions() -> None:
    for module in (observations_router, metric_definitions_router):
        assert module.router.route_class is V2JsonSafeRoute
    paths = {route.path for route in observations_router.router.routes}
    assert paths == {
        "/tenants/{tenant_id}/observations",
        "/tenants/{tenant_id}/observations/latest",
    }
    assert {route.path for route in metric_definitions_router.router.routes} == {
        "/tenants/{tenant_id}/metric-definitions"
    }
    for module in (observations_router, metric_definitions_router):
        for route in module.router.routes:
            assert route.methods == {"GET"}


# --- metric definitions --------------------------------------------------------------------


def test_metric_definitions_equal_the_catalog_in_order() -> None:
    harness = build_app(with_reader=False)
    response = get(harness, DEFINITIONS)

    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {**definition.model_dump(mode="json"), "catalog_version": METRIC_CATALOG_VERSION}
            for definition in metric_catalog()
        ]
    }
    first = response.json()["items"][0]
    assert set(first) == {
        "metric_key",
        "display_name",
        "canonical_unit",
        "value_type",
        "plausible_min",
        "plausible_max",
        "legacy_field",
        "catalog_version",
    }
    assert first["metric_key"] == "water.temperature" and first["plausible_min"] == -5.0
    assert response.json()["items"][-1]["value_type"] == "integer"


def test_metric_definitions_require_membership() -> None:
    harness = build_app()
    response = get(harness, f"/tenants/{OTHER_TENANT}/metric-definitions")
    assert response.status_code == 403
    assert response.json() == {"detail": "tenant access denied"}


# --- roles ---------------------------------------------------------------------------------


@pytest.mark.parametrize("role", list(TenantRole))
def test_every_read_role_can_read(role: TenantRole) -> None:
    harness = build_app(role=role)
    for path in (DEFINITIONS, OBSERVATIONS, LATEST):
        params = None if path == DEFINITIONS else {"asset_id": ASSET_ID}
        assert get(harness, path, params).status_code == 200


@pytest.mark.parametrize("path", ["observations", "observations/latest"])
def test_non_member_gets_403_before_any_lookup_or_read(path: str) -> None:
    harness = build_app(role=TenantRole.OWNER)
    response = get(harness, f"/tenants/{OTHER_TENANT}/{path}", {"asset_id": OTHER_ASSET_ID})

    assert response.status_code == 403
    assert response.json() == {"detail": "tenant access denied"}
    assert harness.entities.calls == []
    assert harness.reader.calls == []


# --- observations: response shape ----------------------------------------------------------


def expected_item(item: StoredObservation) -> dict[str, object]:
    return {
        "event_id": item.event_id,
        "observed_at": format_utc_timestamp(item.observed_at),
        "received_at": format_utc_timestamp(item.received_at),
        "ingested_at": format_utc_timestamp(item.ingested_at),
        "metric_key": item.metric_key,
        "value": item.value,
        "unit": item.canonical_unit,
        "site_id": item.site_id,
        "asset_id": item.asset_id,
        "device_id": item.device_id,
        "component_id": item.component_id,
        "probe_id": item.probe_id,
        "deployment_id": item.deployment_id,
        "quality": {
            "state": str(item.quality_state),
            "flags": [str(flag) for flag in item.quality_flags],
            "provenance": [str(entry) for entry in item.quality_provenance],
        },
        "sequence": item.sequence,
        "source_type": str(item.source_type),
    }


@pytest.mark.parametrize("path", [OBSERVATIONS, LATEST])
def test_items_expose_exactly_the_contract_fields(path: str) -> None:
    items = [stored(), stored(metric_key="water.ph", probe_id=None, sequence=None)]
    harness = build_app(reader=FakeReader(items))
    response = get(harness, path, {"asset_id": ASSET_ID})

    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"items"}
    assert body["items"] == [expected_item(item) for item in items]
    for item in body["items"]:
        assert set(item) == ITEM_KEYS
        assert set(item["quality"]) == {"state", "flags", "provenance"}
    assert body["items"][1]["probe_id"] is None and body["items"][1]["sequence"] is None
    for hidden in ("integration_id", "source_event_id", "source_metadata", "metadata"):
        assert hidden not in response.text


@pytest.mark.parametrize("path", [OBSERVATIONS, LATEST])
def test_empty_result_is_an_empty_items_list(path: str) -> None:
    harness = build_app(reader=FakeReader([]))
    response = get(harness, path, {"device_id": DEVICE_ID})
    assert response.status_code == 200
    assert response.json() == {"items": []}


# --- observations: query parameters --------------------------------------------------------


def test_defaults_are_last_hour_limit_500_and_no_filters() -> None:
    harness = build_app()
    assert get(harness, OBSERVATIONS, {"asset_id": ASSET_ID}).status_code == 200

    kwargs = query_kwargs(harness)
    assert kwargs == {
        "tenant_id": TENANT,
        "selector": ObservationSelector("asset", ASSET_ID),
        "metric_keys": None,
        "quality_states": None,
        "start": NOW - timedelta(hours=1),
        "stop": NOW,
        "limit": 500,
    }


@pytest.mark.parametrize(
    ("params", "selector"),
    [
        ({"asset_id": ASSET_ID}, ObservationSelector("asset", ASSET_ID)),
        ({"component_id": COMPONENT_ID}, ObservationSelector("component", COMPONENT_ID)),
        ({"device_id": DEVICE_ID}, ObservationSelector("device", DEVICE_ID)),
    ],
)
def test_each_selector_kind_reaches_the_reader(params: dict, selector: ObservationSelector) -> None:
    harness = build_app()
    assert get(harness, OBSERVATIONS, params).status_code == 200
    assert query_kwargs(harness)["selector"] == selector


@pytest.mark.parametrize("path", [OBSERVATIONS, LATEST])
@pytest.mark.parametrize(
    "params",
    [
        {},
        {"asset_id": ASSET_ID, "device_id": DEVICE_ID},
        {"asset_id": ASSET_ID, "component_id": COMPONENT_ID},
        {"component_id": COMPONENT_ID, "device_id": DEVICE_ID},
        {"asset_id": ASSET_ID, "component_id": COMPONENT_ID, "device_id": DEVICE_ID},
        {"asset_id": "pond_1"},
        {"asset_id": "cmp_" + "2" * 32},
        {"component_id": "ast_" + "1" * 32},
        {"device_id": "bad device"},
        {"device_id": 'dev_") |> drop(columns: ["tenant_id"])'},
        {"asset_id": ASSET_ID, "unknown": "1"},
    ],
)
def test_selector_must_be_exactly_one_valid_id(path: str, params: dict) -> None:
    harness = build_app()
    response = get(harness, path, params)
    assert response.status_code == 422
    assert harness.reader.calls == []


def test_repeated_metric_keys_and_quality_states_become_frozensets() -> None:
    harness = build_app()
    params = [
        ("asset_id", ASSET_ID),
        ("metric_key", "water.ph"),
        ("metric_key", "device.rssi"),
        ("quality_state", "valid"),
        ("quality_state", "out_of_range"),
    ]
    assert get(harness, OBSERVATIONS, params).status_code == 200

    kwargs = query_kwargs(harness)
    assert kwargs["metric_keys"] == frozenset({"water.ph", "device.rssi"})
    assert kwargs["quality_states"] == frozenset({QualityState.VALID, QualityState.OUT_OF_RANGE})
    assert isinstance(kwargs["metric_keys"], frozenset)
    assert isinstance(kwargs["quality_states"], frozenset)


@pytest.mark.parametrize("path", [OBSERVATIONS, LATEST])
@pytest.mark.parametrize("metric_key", ["water.oxygen", "temp_c", "", 'water.ph" or true'])
def test_unknown_metric_key_is_422(path: str, metric_key: str) -> None:
    harness = build_app()
    params = [("asset_id", ASSET_ID), ("metric_key", "water.ph"), ("metric_key", metric_key)]
    assert get(harness, path, params).status_code == 422
    assert harness.reader.calls == []


@pytest.mark.parametrize(
    "state", ["stale", "missing", "duplicate", "delayed", "bogus", "VALID", ""]
)
def test_non_point_quality_state_is_422(state: str) -> None:
    harness = build_app()
    params = [("asset_id", ASSET_ID), ("quality_state", "valid"), ("quality_state", state)]
    assert get(harness, OBSERVATIONS, params).status_code == 422
    assert harness.reader.calls == []


@pytest.mark.parametrize("state", sorted(str(state) for state in QualityState))
def test_every_point_quality_state_is_accepted(state: str) -> None:
    point_states = {str(item) for item in POINT_QUALITY_STATES}
    harness = build_app()
    response = get(harness, OBSERVATIONS, {"asset_id": ASSET_ID, "quality_state": state})
    assert response.status_code == (200 if state in point_states else 422)


@pytest.mark.parametrize(
    ("limit", "status"),
    [("1", 200), ("1000", 200), ("0", 422), ("1001", 422), ("-1", 422), ("ten", 422)],
)
def test_limit_bounds(limit: str, status: int) -> None:
    harness = build_app()
    response = get(harness, OBSERVATIONS, {"asset_id": ASSET_ID, "limit": limit})
    assert response.status_code == status
    if status == 200:
        assert query_kwargs(harness)["limit"] == int(limit)
    else:
        assert harness.reader.calls == []


# --- observations: time window -------------------------------------------------------------


def test_explicit_window_is_normalized_to_utc() -> None:
    harness = build_app()
    params = {
        "asset_id": ASSET_ID,
        "start": "2026-10-18T09:00:00.123456+02:00",
        "stop": "2026-10-18T10:30:00Z",
    }
    assert get(harness, OBSERVATIONS, params).status_code == 200

    kwargs = query_kwargs(harness)
    assert kwargs["start"] == datetime(2026, 10, 18, 7, 0, 0, 123456, tzinfo=UTC)
    assert kwargs["stop"] == datetime(2026, 10, 18, 10, 30, tzinfo=UTC)
    assert kwargs["start"].utcoffset() == timedelta(0)
    assert kwargs["stop"].utcoffset() == timedelta(0)


def test_only_stop_defaults_start_to_an_hour_before() -> None:
    harness = build_app()
    stop = datetime(2026, 10, 1, 8, 0, tzinfo=UTC)
    assert get(harness, OBSERVATIONS, {"asset_id": ASSET_ID, "stop": iso(stop)}).status_code == 200
    kwargs = query_kwargs(harness)
    assert (kwargs["start"], kwargs["stop"]) == (stop - timedelta(hours=1), stop)


def test_only_start_defaults_stop_to_now() -> None:
    harness = build_app()
    start = NOW - timedelta(days=3)
    assert (
        get(harness, OBSERVATIONS, {"asset_id": ASSET_ID, "start": iso(start)}).status_code == 200
    )
    kwargs = query_kwargs(harness)
    assert (kwargs["start"], kwargs["stop"]) == (start, NOW)


@pytest.mark.parametrize(
    ("start", "stop"),
    [
        (MIN_TELEMETRY_TIME, MIN_TELEMETRY_TIME + timedelta(hours=1)),
        (MAX_TELEMETRY_TIME - timedelta(hours=1), MAX_TELEMETRY_TIME),
        (MIN_TELEMETRY_TIME, MIN_TELEMETRY_TIME + timedelta(days=31)),
        (NOW - timedelta(days=31), NOW),
    ],
)
def test_bounds_inside_the_closed_range_are_accepted(start: datetime, stop: datetime) -> None:
    harness = build_app()
    params = {"asset_id": ASSET_ID, "start": iso(start), "stop": iso(stop)}
    response = get(harness, OBSERVATIONS, params)

    assert response.status_code == 200
    kwargs = query_kwargs(harness)
    assert (kwargs["start"], kwargs["stop"]) == (start, stop)


@pytest.mark.parametrize(
    "params",
    [
        {"start": "1969-12-31T23:59:59.999999Z", "stop": "1970-01-01T00:30:00Z"},
        {"start": "2261-12-31T23:30:00Z", "stop": "2262-01-01T00:00:00.000001Z"},
        {"stop": "0001-01-01T00:00:00Z"},
        {"start": "0001-01-01T00:00:00Z"},
        {"stop": "0001-01-01T00:00:00+01:00"},
        {"start": "9999-12-31T23:59:59-01:00"},
        {"stop": "9999-12-31T23:59:59Z"},
        {"stop": "1969-12-31T23:59:59Z"},
        {"start": "2262-01-01T00:00:00.000001Z"},
    ],
)
def test_bounds_outside_the_closed_range_are_422_never_500(params: dict) -> None:
    harness = build_app()
    response = get(harness, OBSERVATIONS, {"asset_id": ASSET_ID, **params})
    assert response.status_code == 422
    assert harness.reader.calls == []


@pytest.mark.parametrize(
    "params",
    [
        {"start": "2026-10-18T11:00:00"},
        {"stop": "2026-10-18T11:00:00"},
        {"start": "2026-10-18T10:00:00", "stop": "2026-10-18T11:00:00Z"},
        {"start": "not-a-time"},
    ],
)
def test_naive_or_malformed_timestamps_are_422(params: dict) -> None:
    harness = build_app()
    response = get(harness, OBSERVATIONS, {"asset_id": ASSET_ID, **params})
    assert response.status_code == 422
    assert harness.reader.calls == []


@pytest.mark.parametrize(
    ("start", "stop"),
    [
        (NOW, NOW),
        (NOW, NOW - timedelta(microseconds=1)),
        (NOW - timedelta(days=31, microseconds=1), NOW),
        (NOW - timedelta(days=40), NOW),
    ],
)
def test_stop_must_follow_start_within_31_days(start: datetime, stop: datetime) -> None:
    harness = build_app()
    params = {"asset_id": ASSET_ID, "start": iso(start), "stop": iso(stop)}
    assert get(harness, OBSERVATIONS, params).status_code == 422
    assert harness.reader.calls == []


def test_start_in_the_future_without_stop_is_422() -> None:
    harness = build_app()
    params = {"asset_id": ASSET_ID, "start": iso(NOW + timedelta(minutes=1))}
    assert get(harness, OBSERVATIONS, params).status_code == 422
    assert harness.reader.calls == []


# --- latest --------------------------------------------------------------------------------


def test_latest_reads_24h_to_now_with_optional_metric_key() -> None:
    harness = build_app()
    params = {"component_id": COMPONENT_ID, "metric_key": "water.temperature"}
    assert get(harness, LATEST, params).status_code == 200

    assert harness.reader.calls == [
        (
            "query_latest_observations",
            {
                "tenant_id": TENANT,
                "selector": ObservationSelector("component", COMPONENT_ID),
                "metric_keys": frozenset({"water.temperature"}),
                "lookback": timedelta(hours=24),
                "now": NOW,
            },
        )
    ]


def test_latest_without_metric_key_passes_none() -> None:
    harness = build_app()
    assert get(harness, LATEST, {"device_id": DEVICE_ID}).status_code == 200
    assert harness.reader.calls[0][1]["metric_keys"] is None


@pytest.mark.parametrize(
    "extra",
    [
        {"start": "2026-10-18T10:00:00Z"},
        {"stop": "2026-10-18T10:00:00Z"},
        {"limit": "5"},
        {"quality_state": "valid"},
    ],
)
def test_latest_rejects_window_limit_and_quality_parameters(extra: dict) -> None:
    harness = build_app()
    assert get(harness, LATEST, {"asset_id": ASSET_ID, **extra}).status_code == 422
    assert harness.reader.calls == []


# --- ownership, availability and errors ----------------------------------------------------


@pytest.mark.parametrize("path", [OBSERVATIONS, LATEST])
@pytest.mark.parametrize(
    "params",
    [
        {"asset_id": OTHER_ASSET_ID},
        {"asset_id": "ast_" + "9" * 32},
        {"device_id": "dev_" + "9" * 32},
        {"device_id": "local-device-404"},
        {"component_id": "cmp_" + "9" * 32},
    ],
)
def test_selector_missing_in_tenant_is_404_with_zero_reader_calls(path: str, params: dict) -> None:
    harness = build_app()
    response = get(harness, path, params)

    assert response.status_code == 404
    assert response.json() == {"detail": "not found"}
    assert harness.reader.calls == []
    assert {call[1] for call in harness.entities.calls} == {TENANT}


@pytest.mark.parametrize("path", [OBSERVATIONS, LATEST])
def test_telemetry_query_error_is_503(path: str) -> None:
    harness = build_app(reader=FakeReader(error=TelemetryQueryError("influx down")))
    response = get(harness, path, {"asset_id": ASSET_ID})
    assert response.status_code == 503
    assert response.json() == {"detail": "service unavailable"}
    assert "influx down" not in response.text
    assert len(harness.reader.calls) == 1


@pytest.mark.parametrize("path", [OBSERVATIONS, LATEST])
def test_missing_reader_state_is_503(path: str) -> None:
    harness = build_app(with_reader=False)
    response = get(harness, path, {"asset_id": ASSET_ID})
    assert response.status_code == 503


@pytest.mark.parametrize("path", ["observations", "observations/latest"])
def test_non_member_gets_403_even_when_the_reader_is_missing(path: str) -> None:
    harness = build_app(role=TenantRole.OWNER, with_reader=False)
    response = get(harness, f"/tenants/{OTHER_TENANT}/{path}", {"asset_id": OTHER_ASSET_ID})
    assert response.status_code == 403
    assert response.json() == {"detail": "tenant access denied"}
    assert harness.entities.calls == []


def test_routes_document_the_m1_error_responses() -> None:
    paths = build_app().app.openapi()["paths"]
    for path in (OBSERVATIONS, LATEST):
        documented = set(paths[path.replace(TENANT, "{tenant_id}")]["get"]["responses"])
        assert {"200", "403", "404", "422", "503"} <= documented, (path, documented)
    documented = set(paths["/tenants/{tenant_id}/metric-definitions"]["get"]["responses"])
    assert {"200", "403"} <= documented


def test_clock_dependency_defaults_to_aware_utc_now() -> None:
    clock = telemetry_dependencies.get_clock()
    before = datetime.now(UTC)
    value = clock()
    after = datetime.now(UTC)
    assert value.utcoffset() == timedelta(0)
    assert before <= value <= after


# --- schema names --------------------------------------------------------------------------


SHARED_SCHEMAS = {"ErrorResponse", "HTTPValidationError", "ValidationError"}


def test_schema_models_are_v2_prefixed() -> None:
    for module in (observations_schemas, metric_definitions_schemas):
        models = [
            obj
            for _, obj in inspect.getmembers(module, inspect.isclass)
            if issubclass(obj, BaseModel)
            and obj.__module__ == module.__name__
            and not obj.__name__.startswith("_")
        ]
        assert models, module.__name__
        for model in models:
            assert model.__name__.startswith("V2"), model.__name__


def test_openapi_schema_names_are_v2_prefixed_and_disjoint_from_v1() -> None:
    harness = build_app()
    names = set(harness.app.openapi()["components"]["schemas"])
    v1_names = set(json.loads(GOLDEN.read_text(encoding="utf-8"))["components"]["schemas"])

    assert {"V2ObservationResponse", "V2MetricDefinitionResponse"} <= names
    for name in names - SHARED_SCHEMAS:
        assert name.startswith("V2"), name
        assert name not in v1_names, name


def test_mounting_the_routers_next_to_v1_keeps_the_v1_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("API_V2_ENABLED", raising=False)
    monkeypatch.delenv("V1_POND_DEVICE_PROJECTION_ENABLED", raising=False)
    app = create_app(_settings())
    app.include_router(metric_definitions_router.router, prefix="/v2")
    app.include_router(observations_router.router, prefix="/v2")

    assert build_v1_openapi_contract(app) == json.loads(GOLDEN.read_text(encoding="utf-8"))
    paths = app.openapi()["paths"]
    assert "/v2/tenants/{tenant_id}/observations/latest" in paths
