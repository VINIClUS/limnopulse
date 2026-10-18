"""Unmounted `/v2` observation router; P2-08 mounts it behind `API_V2_TELEMETRY_ENABLED`.

`observations/latest` is a latest-by-`observed_at` data view, not a health or freshness view.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.exceptions import RequestValidationError

from limnopulse_api.api.dependencies import require_tenant_role
from limnopulse_api.api.v1.schemas.common import ErrorResponse
from limnopulse_api.api.v2.dependencies import (
    ComponentReaderDep,
    DeviceReaderDep,
    SiteAssetReaderDep,
)
from limnopulse_api.api.v2.routers.devices import V2JsonSafeRoute
from limnopulse_api.api.v2.schemas.common import V2_ERROR_RESPONSES
from limnopulse_api.api.v2.schemas.observations import (
    V2LatestObservationQuery,
    V2ObservationListResponse,
    V2ObservationQuery,
    V2ObservationResponse,
)
from limnopulse_api.api.v2.telemetry_dependencies import ClockDep, ObservationReaderDep
from limnopulse_api.core.errors import NotFoundError, TelemetryQueryError
from limnopulse_api.domain.entities import TenantAccess
from limnopulse_api.domain.roles import READ_ROLES
from limnopulse_api.domain.telemetry_v2 import StoredObservation
from limnopulse_api.services.telemetry_v2 import ObservationReadService, ObservationWindowError

router = APIRouter(
    prefix="/tenants/{tenant_id}", tags=["v2-telemetry"], route_class=V2JsonSafeRoute
)

_RESPONSES: dict[int | str, dict[str, object]] = {
    403: {"model": ErrorResponse},
    **V2_ERROR_RESPONSES,
}

# The access dependency is declared before the reader so a non-member gets 403, never 503.
TenantReadAccess = Annotated[TenantAccess, Depends(require_tenant_role(*READ_ROLES))]


def _list_response(items: list[StoredObservation]) -> V2ObservationListResponse:
    return V2ObservationListResponse(
        items=[V2ObservationResponse.from_domain(item) for item in items]
    )


def _http_error(exc: NotFoundError | TelemetryQueryError) -> HTTPException:
    if isinstance(exc, NotFoundError):
        return HTTPException(status_code=404, detail="not found")
    return HTTPException(status_code=503, detail="service unavailable")


@router.get("/observations", response_model=V2ObservationListResponse, responses=_RESPONSES)
async def list_observations(
    tenant_id: str,
    _access: TenantReadAccess,
    query: Annotated[V2ObservationQuery, Query()],
    reader: ObservationReaderDep,
    site_assets: SiteAssetReaderDep,
    devices: DeviceReaderDep,
    components: ComponentReaderDep,
    clock: ClockDep,
) -> V2ObservationListResponse:
    service = ObservationReadService(
        reader=reader, site_assets=site_assets, devices=devices, components=components, clock=clock
    )
    try:
        items = await service.query_observations(
            tenant_id=tenant_id,
            selector=query.selector(),
            metric_keys=query.metric_keys(),
            quality_states=query.quality_states(),
            start=query.start,
            stop=query.stop,
            limit=query.limit,
        )
    except ObservationWindowError as exc:
        raise RequestValidationError(
            [{"type": "value_error", "loc": ("query", "stop"), "msg": str(exc), "input": None}]
        ) from exc
    except (NotFoundError, TelemetryQueryError) as exc:
        raise _http_error(exc) from exc
    return _list_response(items)


@router.get("/observations/latest", response_model=V2ObservationListResponse, responses=_RESPONSES)
async def list_latest_observations(
    tenant_id: str,
    _access: TenantReadAccess,
    query: Annotated[V2LatestObservationQuery, Query()],
    reader: ObservationReaderDep,
    site_assets: SiteAssetReaderDep,
    devices: DeviceReaderDep,
    components: ComponentReaderDep,
    clock: ClockDep,
) -> V2ObservationListResponse:
    service = ObservationReadService(
        reader=reader, site_assets=site_assets, devices=devices, components=components, clock=clock
    )
    try:
        items = await service.query_latest_observations(
            tenant_id=tenant_id, selector=query.selector(), metric_keys=query.metric_keys()
        )
    except (NotFoundError, TelemetryQueryError) as exc:
        raise _http_error(exc) from exc
    return _list_response(items)
