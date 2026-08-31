import json
from pathlib import Path

from fastapi import FastAPI

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
