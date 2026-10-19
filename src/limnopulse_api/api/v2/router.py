"""Aggregate of the `/v2` routers; mounted under `/v2` only when `API_V2_ENABLED`.

The P2 telemetry routers join only with `enable_telemetry` (`API_V2_TELEMETRY_ENABLED`).
"""

import math
from typing import Any

from fastapi import APIRouter, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from starlette._utils import get_route_path

from limnopulse_api.api.v2.routers import (
    assets,
    components,
    deployments,
    devices,
    integrations,
    metric_definitions,
    observations,
    sites,
)

V2_PREFIX = "/v2"


def build_v2_router(enable_telemetry: bool = False) -> APIRouter:
    v2_router = APIRouter()
    v2_router.include_router(sites.router)
    v2_router.include_router(assets.router)
    v2_router.include_router(devices.router)
    v2_router.include_router(components.router)
    v2_router.include_router(integrations.router)
    v2_router.include_router(deployments.router)
    if enable_telemetry:
        v2_router.include_router(metric_definitions.router)
        v2_router.include_router(observations.router)
    return v2_router


def _json_safe(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        return repr(value)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _is_v2_request(request: Request) -> bool:
    path = get_route_path(request.scope)
    return path == V2_PREFIX or path.startswith(f"{V2_PREFIX}/")


async def v2_safe_request_validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> Response:
    """Render `/v2` 422 bodies with non-finite floats as strings; `/v1` keeps FastAPI's default."""
    if not _is_v2_request(request):
        return await request_validation_exception_handler(request, exc)
    detail = _json_safe(jsonable_encoder(exc.errors()))
    return JSONResponse(status_code=422, content={"detail": detail})
