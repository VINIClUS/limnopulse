import inspect
import json
import typing
from datetime import UTC, datetime
from inspect import Parameter
from pathlib import Path
from typing import Annotated

import pytest
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import Depends, FastAPI
from fastapi.requests import Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from limnopulse_api.api.v2 import dependencies as v2_dependencies
from limnopulse_api.api.v2.dependencies import (
    ComponentReaderDep,
    DeploymentRepositoryDep,
    DeviceComponentRepositoryDep,
    DeviceReaderDep,
    IntegrationRepositoryDep,
    SiteAssetReaderDep,
    SiteAssetRepositoryDep,
)
from limnopulse_api.api.v2.schemas.common import (
    IdempotencyKeyHeader,
    V2ExpectedVersionRequest,
    V2VersionedResponse,
)
from limnopulse_api.domain.assets import (
    Asset,
    AssetStatus,
    AssetWithPondProfile,
    PondProfile,
    Site,
    SiteStatus,
)
from limnopulse_api.domain.deployments import Deployment
from limnopulse_api.domain.devices_v2 import (
    CapabilityDeclaration,
    ComponentStatus,
    DeviceComponent,
    DeviceStatus,
    DeviceV2,
)
from limnopulse_api.domain.integrations import DeviceIntegration, IntegrationAccount
from limnopulse_api.domain.v2_common import IdempotencyReceipt
from limnopulse_api.repositories import v2_contracts
from limnopulse_api.repositories.v2_contracts import (
    ComponentReader,
    DeploymentRepository,
    DeviceComponentRepository,
    DeviceReader,
    IntegrationRepository,
    SiteAssetReader,
    SiteAssetRepository,
)

ROOT = Path(__file__).resolve().parents[2]
P = Parameter.POSITIONAL_OR_KEYWORD
K = Parameter.KEYWORD_ONLY
T = ("tenant_id", P, str)
REPLAY = {
    "get_idempotency_replay": (
        [T, ("scope", P, str), ("key_hash", P, str), ("request_hash", P, str)],
        str | None,
    )
}

SITE_ASSET_READER = {
    "get_site": ([T, ("site_id", P, str)], Site | None),
    "get_asset": ([T, ("asset_id", P, str)], AssetWithPondProfile | None),
}
SITE_ASSET_REPOSITORY = {
    **SITE_ASSET_READER,
    **REPLAY,
    "list_sites": ([T], list[Site]),
    "create_site": ([("site", P, Site), ("receipt", P, IdempotencyReceipt)], Site),
    "update_site": (
        [
            T,
            ("site_id", P, str),
            ("expected_version", P, int),
            ("name", K, str | None),
            ("status", K, SiteStatus | None),
        ],
        Site,
    ),
    "list_assets": ([T], list[AssetWithPondProfile]),
    "create_asset": (
        [("asset", P, Asset), ("profile", P, PondProfile), ("receipt", P, IdempotencyReceipt)],
        AssetWithPondProfile,
    ),
    "update_asset": (
        [
            T,
            ("asset_id", P, str),
            ("expected_version", P, int),
            ("name", K, str | None),
            ("status", K, AssetStatus | None),
            ("pond_description", K, str | None),
            ("pond_profile_expected_version", K, int | None),
        ],
        AssetWithPondProfile,
    ),
}
DEVICE_READER = {"get_device": ([T, ("device_id", P, str)], DeviceV2 | None)}
COMPONENT_READER = {"get_component": ([T, ("component_id", P, str)], DeviceComponent | None)}
DEVICE_COMPONENT_REPOSITORY = {
    **DEVICE_READER,
    **COMPONENT_READER,
    **REPLAY,
    "list_devices": ([T], list[DeviceV2]),
    "create_device": ([("device", P, DeviceV2), ("receipt", P, IdempotencyReceipt)], DeviceV2),
    "update_device": (
        [
            T,
            ("device_id", P, str),
            ("expected_version", P, int),
            ("name", K, str | None),
            ("firmware_version", K, str | None),
            ("status", K, DeviceStatus | None),
        ],
        DeviceV2,
    ),
    "list_components": ([T, ("device_id", P, str)], list[DeviceComponent]),
    "create_component": (
        [("component", P, DeviceComponent), ("receipt", P, IdempotencyReceipt)],
        DeviceComponent,
    ),
    "update_component": (
        [
            T,
            ("component_id", P, str),
            ("expected_version", P, int),
            ("name", K, str | None),
            ("status", K, ComponentStatus | None),
        ],
        DeviceComponent,
    ),
    "list_capabilities": ([T, ("component_id", P, str)], list[CapabilityDeclaration]),
    "create_capability": (
        [("declaration", P, CapabilityDeclaration), ("receipt", P, IdempotencyReceipt)],
        CapabilityDeclaration,
    ),
}
INTEGRATION_REPOSITORY = {
    **REPLAY,
    "get_integration_account": (
        [T, ("integration_account_id", P, str)],
        IntegrationAccount | None,
    ),
    "list_integration_accounts": ([T], list[IntegrationAccount]),
    "create_integration_account": (
        [("account", P, IntegrationAccount), ("receipt", P, IdempotencyReceipt)],
        IntegrationAccount,
    ),
    "list_device_integrations": ([T, ("device_id", P, str)], list[DeviceIntegration]),
    "create_device_integration": (
        [("integration", P, DeviceIntegration), ("receipt", P, IdempotencyReceipt)],
        DeviceIntegration,
    ),
}
DEPLOYMENT_REPOSITORY = {
    **REPLAY,
    "get_deployment": ([T, ("deployment_id", P, str)], Deployment | None),
    "list_component_deployments": ([T, ("component_id", P, str)], list[Deployment]),
    "get_current_deployment": ([T, ("component_id", P, str)], Deployment | None),
    "get_deployment_at": (
        [T, ("component_id", P, str), ("observed_at", P, datetime)],
        Deployment | None,
    ),
    "start_deployment": (
        [("deployment", P, Deployment), ("receipt", P, IdempotencyReceipt)],
        Deployment,
    ),
    "end_deployment": (
        [
            T,
            ("deployment_id", P, str),
            ("expected_version", K, int),
            ("ended_at", K, datetime),
        ],
        Deployment,
    ),
}

PROTOCOLS = [
    (SiteAssetReader, SITE_ASSET_READER),
    (SiteAssetRepository, SITE_ASSET_REPOSITORY),
    (DeviceReader, DEVICE_READER),
    (ComponentReader, COMPONENT_READER),
    (DeviceComponentRepository, DEVICE_COMPONENT_REPOSITORY),
    (IntegrationRepository, INTEGRATION_REPOSITORY),
    (DeploymentRepository, DEPLOYMENT_REPOSITORY),
]


def protocol_methods(protocol: type) -> set[str]:
    return {
        name
        for name in dir(protocol)
        if not name.startswith("_") and inspect.iscoroutinefunction(getattr(protocol, name))
    }


@pytest.mark.parametrize(("protocol", "expected"), PROTOCOLS)
def test_protocol_method_set_is_frozen(protocol, expected) -> None:
    assert protocol_methods(protocol) == set(expected)


@pytest.mark.parametrize(("protocol", "expected"), PROTOCOLS)
def test_protocol_signatures_match_names_kinds_and_types(protocol, expected) -> None:
    for name, (parameters, return_type) in expected.items():
        method = getattr(protocol, name)
        assert inspect.iscoroutinefunction(method), name
        signature = inspect.signature(method)
        hints = typing.get_type_hints(method)
        actual = [
            (param.name, param.kind, hints[param.name])
            for param in signature.parameters.values()
            if param.name != "self"
        ]
        assert actual == parameters, name
        assert all(param.default is Parameter.empty for param in signature.parameters.values()), (
            name
        )
        assert hints["return"] == return_type, name


@pytest.mark.parametrize(("protocol", "_expected"), PROTOCOLS)
def test_protocols_are_runtime_checkable(protocol, _expected) -> None:
    assert getattr(protocol, "_is_runtime_protocol", False)
    assert getattr(protocol, "_is_protocol", False)


def test_reader_protocols_are_narrow_bases() -> None:
    assert issubclass(SiteAssetRepository, SiteAssetReader)
    assert issubclass(DeviceComponentRepository, DeviceReader)
    assert issubclass(DeviceComponentRepository, ComponentReader)


def test_write_protocols_expose_idempotency_replay() -> None:
    for protocol in (
        SiteAssetRepository,
        DeviceComponentRepository,
        IntegrationRepository,
        DeploymentRepository,
    ):
        assert "get_idempotency_replay" in protocol_methods(protocol)
    for reader in (SiteAssetReader, DeviceReader, ComponentReader):
        assert "get_idempotency_replay" not in protocol_methods(reader)


def _fake_class(name: str, methods: set[str]) -> type:
    async def method(self, *args, **kwargs):
        return None

    return type(name, (), {method_name: method for method_name in methods})


@pytest.mark.parametrize(("protocol", "expected"), PROTOCOLS)
def test_isinstance_accepts_complete_fakes_and_rejects_incomplete_ones(protocol, expected) -> None:
    complete = _fake_class("FakeComplete", set(expected))()
    assert isinstance(complete, protocol)

    for missing in expected:
        incomplete = _fake_class("FakeIncomplete", set(expected) - {missing})()
        assert not isinstance(incomplete, protocol), missing


def test_deployment_repository_exposes_get_deployment() -> None:
    signature = inspect.signature(DeploymentRepository.get_deployment)
    assert list(signature.parameters) == ["self", "tenant_id", "deployment_id"]


def test_contract_modules_do_not_reference_provider_identity_or_cache_terms() -> None:
    forbidden = ("redis", "aws_iot", "thing_name", "certificate_arn", "shadow_name")
    paths = [
        ROOT / "src/limnopulse_api/repositories/v2_contracts.py",
        *(
            ROOT / "src/limnopulse_api/domain" / name
            for name in (
                "v2_common.py",
                "assets.py",
                "devices_v2.py",
                "integrations.py",
                "deployments.py",
                "legacy_projection.py",
                "ids.py",
            )
        ),
    ]
    for path in paths:
        text = path.read_text(encoding="utf-8").lower()
        assert not [term for term in forbidden if term in text], path
    assert v2_contracts.__name__.endswith("v2_contracts")


# --- app-state dependencies ------------------------------------------------------------------


STATE_NAMES = {
    "site_asset_repository",
    "device_component_repository",
    "integration_repository",
    "deployment_repository",
}


def _infrastructure_error(request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=503, content={"detail": "service unavailable"})


def build_dependency_app() -> FastAPI:
    app = FastAPI()
    app.add_exception_handler(BotoCoreError, _infrastructure_error)
    app.add_exception_handler(ClientError, _infrastructure_error)

    @app.get("/site-asset")
    async def site_asset(repository: SiteAssetRepositoryDep) -> dict:
        return {"value": repository.marker}

    @app.get("/site-asset-reader")
    async def site_asset_reader(repository: SiteAssetReaderDep) -> dict:
        return {"value": repository.marker}

    @app.get("/device-component")
    async def device_component(repository: DeviceComponentRepositoryDep) -> dict:
        return {"value": repository.marker}

    @app.get("/device-reader")
    async def device_reader(repository: DeviceReaderDep) -> dict:
        return {"value": repository.marker}

    @app.get("/component-reader")
    async def component_reader(repository: ComponentReaderDep) -> dict:
        return {"value": repository.marker}

    @app.get("/integration")
    async def integration(repository: IntegrationRepositoryDep) -> dict:
        return {"value": repository.marker}

    @app.get("/deployment")
    async def deployment(repository: DeploymentRepositoryDep) -> dict:
        return {"value": repository.marker}

    @app.post("/idempotent")
    async def idempotent(idempotency_key: IdempotencyKeyHeader) -> dict:
        return {"key": idempotency_key}

    return app


class FakeStateRepository:
    def __init__(self, marker: str) -> None:
        self.marker = marker


ROUTE_STATE = {
    "/site-asset": "site_asset_repository",
    "/site-asset-reader": "site_asset_repository",
    "/device-component": "device_component_repository",
    "/device-reader": "device_component_repository",
    "/component-reader": "device_component_repository",
    "/integration": "integration_repository",
    "/deployment": "deployment_repository",
}


def test_dependencies_read_the_four_locked_app_state_names() -> None:
    assert set(ROUTE_STATE.values()) == STATE_NAMES
    app = build_dependency_app()
    for name in STATE_NAMES:
        setattr(app.state, name, FakeStateRepository(name))

    with TestClient(app) as client:
        for path, name in ROUTE_STATE.items():
            response = client.get(path)
            assert response.status_code == 200, path
            assert response.json() == {"value": name}


@pytest.mark.parametrize(("path", "name"), sorted(ROUTE_STATE.items()))
def test_missing_app_state_maps_to_503(path: str, name: str) -> None:
    app = build_dependency_app()
    for other in STATE_NAMES - {name}:
        setattr(app.state, other, FakeStateRepository(other))

    with TestClient(app) as client:
        response = client.get(path)

    assert response.status_code == 503
    assert response.json() == {"detail": "service unavailable"}


def test_none_app_state_maps_to_503() -> None:
    app = build_dependency_app()
    app.state.deployment_repository = None

    with TestClient(app) as client:
        response = client.get("/deployment")

    assert response.status_code == 503
    assert response.json() == {"detail": "service unavailable"}


def test_dependency_overrides_use_public_getters() -> None:
    app = build_dependency_app()
    app.dependency_overrides[v2_dependencies.get_deployment_repository] = lambda: (
        FakeStateRepository("override")
    )

    with TestClient(app) as client:
        assert client.get("/deployment").json() == {"value": "override"}


def test_state_dependency_helper_mirrors_v1_text() -> None:
    from fastapi import HTTPException

    request = Request({"type": "http", "app": FastAPI()})
    with pytest.raises(HTTPException) as raised:
        v2_dependencies._get_state_dependency(request, "site_asset_repository")
    assert raised.value.status_code == 503
    assert raised.value.detail == "service unavailable"


@pytest.mark.parametrize(
    ("key", "status"),
    [("k" * 8, 200), ("k" * 128, 200), ("k" * 7, 422), ("k" * 129, 422)],
)
def test_idempotency_key_header_bounds(key: str, status: int) -> None:
    with TestClient(build_dependency_app()) as client:
        response = client.post("/idempotent", headers={"Idempotency-Key": key})

    assert response.status_code == status
    if status == 200:
        assert response.json() == {"key": key}


def test_idempotency_key_header_is_required() -> None:
    with TestClient(build_dependency_app()) as client:
        response = client.post("/idempotent")
    assert response.status_code == 422


def test_idempotency_key_header_is_documented_under_its_wire_name() -> None:
    schema = build_dependency_app().openapi()
    parameters = schema["paths"]["/idempotent"]["post"]["parameters"]
    assert parameters[0]["name"] == "Idempotency-Key"
    assert parameters[0]["in"] == "header"
    assert parameters[0]["required"] is True


# --- shared response schemas -----------------------------------------------------------------


def test_v2_schema_names_do_not_collide_with_v1_components() -> None:
    golden = json.loads((ROOT / "tests/contracts/openapi/v1.json").read_text(encoding="utf-8"))
    v1_names = set(golden["components"]["schemas"])

    assert {V2VersionedResponse.__name__, V2ExpectedVersionRequest.__name__}.isdisjoint(v1_names)


def test_v2_versioned_response_serializes_fixed_width_utc() -> None:
    class FakeSiteResponse(V2VersionedResponse):
        site_id: str

    response = FakeSiteResponse(
        tenant_id="tnt_a",
        site_id="site_1",
        status="active",
        created_at=datetime(2026, 8, 25, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 8, 25, 12, 0, 0, 7, tzinfo=UTC),
        version=1,
    )

    assert response.model_dump(mode="json") == {
        "tenant_id": "tnt_a",
        "status": "active",
        "schema_version": 2,
        "created_at": "2026-08-25T12:00:00.000000Z",
        "updated_at": "2026-08-25T12:00:00.000007Z",
        "version": 1,
        "site_id": "site_1",
    }


def test_v2_expected_version_request_rejects_invalid_versions_and_extras() -> None:
    from pydantic import ValidationError

    assert V2ExpectedVersionRequest(expected_version=1).expected_version == 1
    with pytest.raises(ValidationError):
        V2ExpectedVersionRequest(expected_version=0)
    with pytest.raises(ValidationError):
        V2ExpectedVersionRequest(expected_version=1, tenant_id="tnt_a")


def test_v2_package_markers_are_empty() -> None:
    for marker in ("__init__.py", "routers/__init__.py", "schemas/__init__.py"):
        assert (ROOT / "src/limnopulse_api/api/v2" / marker).read_text(encoding="utf-8") == ""


def test_dependency_aliases_are_annotated_depends() -> None:
    for alias in (
        SiteAssetRepositoryDep,
        DeviceComponentRepositoryDep,
        IntegrationRepositoryDep,
        DeploymentRepositoryDep,
        SiteAssetReaderDep,
        DeviceReaderDep,
        ComponentReaderDep,
    ):
        assert typing.get_origin(alias) is Annotated
        assert isinstance(typing.get_args(alias)[1], type(Depends(lambda: None)))
