import importlib
import json
from pathlib import Path
from typing import Annotated

import fastapi.routing as fastapi_routing
from fastapi import FastAPI, Security
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer

from limnopulse_api.api import openapi_contract
from limnopulse_api.api.openapi_contract import build_v1_openapi_contract
from limnopulse_api.api.v1.schemas.common import ErrorResponse
from limnopulse_api.core.config import Settings
from limnopulse_api.main import create_app

ROOT = Path(__file__).resolve().parents[2]
GOLDEN = ROOT / "tests/contracts/openapi/v1.json"


def test_v1_openapi_matches_checked_in_golden() -> None:
    app = create_app(Settings(app_env="test", auth_mode="dev"))
    actual = build_v1_openapi_contract(app)
    assert all(path.startswith("/v1/") for path in actual["paths"])
    assert actual == json.loads(GOLDEN.read_text(encoding="utf-8"))


def test_v1_openapi_normalizes_only_framework_default_422_description() -> None:
    app = FastAPI()

    @app.put("/v1/default", responses={422: {"model": ErrorResponse}})
    async def default_response() -> dict[str, bool]:
        return {"ok": True}

    @app.put(
        "/v1/custom",
        responses={
            422: {
                "model": ErrorResponse,
                "description": "Verified email required",
            }
        },
    )
    async def custom_response() -> dict[str, bool]:
        return {"ok": True}

    @app.put(
        "/v1/collision",
        responses={
            422: {
                "model": ErrorResponse,
                "description": "Unprocessable Content",
            }
        },
    )
    async def collision_response() -> dict[str, bool]:
        return {"ok": True}

    @app.put(
        "/v1/other-schema",
        responses={
            422: {
                "model": dict[str, str],
                "description": "Unprocessable Content",
            }
        },
    )
    async def other_schema_response() -> dict[str, bool]:
        return {"ok": True}

    source_schema = app.openapi()
    source_snapshot = json.loads(json.dumps(source_schema))

    actual = build_v1_openapi_contract(app)

    assert actual["paths"]["/v1/default"]["put"]["responses"]["422"]["description"] == (
        "Unprocessable Entity"
    )
    assert actual["paths"]["/v1/custom"]["put"]["responses"]["422"]["description"] == (
        "Verified email required"
    )
    assert actual["paths"]["/v1/collision"]["put"]["responses"]["422"]["description"] == (
        "Unprocessable Content"
    )
    assert actual["paths"]["/v1/other-schema"]["put"]["responses"]["422"][
        "description"
    ] == "Unprocessable Content"
    assert source_schema == source_snapshot


def test_effective_api_routes_fall_back_without_route_context_helper(
    monkeypatch,
) -> None:
    app = FastAPI()

    @app.get("/v1/items/{item_id}")
    async def get_item(item_id: str) -> dict[str, str]:
        return {"item_id": item_id}

    with monkeypatch.context() as patch:
        patch.delattr(fastapi_routing, "iter_route_contexts", raising=False)
        compatibility_module = importlib.reload(openapi_contract)
        routes = list(compatibility_module.iter_effective_api_routes(app))

    assert [(route.path_format, route.methods) for route in routes] == [
        ("/v1/items/{item_id}", {"GET"})
    ]


def test_v1_openapi_keeps_only_security_schemes_used_by_v1_operations() -> None:
    app = FastAPI()
    used_bearer = HTTPBearer(scheme_name="UsedBearer")
    unused_key = APIKeyHeader(name="X-Unused-Key", scheme_name="UnusedKey")

    @app.get("/v1/secure")
    async def secure(
        credentials: Annotated[HTTPAuthorizationCredentials, Security(used_bearer)],
    ) -> dict[str, str]:
        return {"scheme": credentials.scheme}

    @app.get("/internal/secure")
    async def internal_secure(
        key: Annotated[str, Security(unused_key)],
    ) -> dict[str, str]:
        return {"key": key}

    actual = build_v1_openapi_contract(app)

    assert actual["paths"]["/v1/secure"]["get"]["security"] == [
        {"UsedBearer": []}
    ]
    assert actual["components"]["securitySchemes"] == {
        "UsedBearer": {"scheme": "bearer", "type": "http"}
    }
