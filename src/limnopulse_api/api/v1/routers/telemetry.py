from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from limnopulse_api.api.dependencies import (
    DomainRepositoryDep,
    TelemetryRepositoryDep,
    require_tenant_role,
)
from limnopulse_api.api.v1.schemas.common import ErrorResponse
from limnopulse_api.api.v1.schemas.telemetry import (
    LatestMetricsResponse,
    TelemetryReadingListResponse,
    TelemetryReadingResponse,
)
from limnopulse_api.core.errors import NotFoundError
from limnopulse_api.domain.entities import TenantAccess
from limnopulse_api.domain.roles import READ_ROLES
from limnopulse_api.domain.telemetry import LatestMetrics, TelemetryReading, validate_flux_time_bound
from limnopulse_api.services.telemetry import PondTelemetryService

router = APIRouter(prefix="/tenants/{tenant_id}/ponds/{pond_id}", tags=["telemetry"])


def _telemetry_service(domain_repository, telemetry_repository) -> PondTelemetryService:
    return PondTelemetryService(
        domain_repository=domain_repository,
        telemetry_repository=telemetry_repository,
    )


def _to_reading_response(reading: TelemetryReading) -> TelemetryReadingResponse:
    return TelemetryReadingResponse(
        measured_at=reading.measured_at.isoformat(),
        tenant_id=reading.tenant_id,
        pond_id=reading.pond_id,
        device_id=reading.device_id,
        temp_c=reading.temp_c,
        ph=reading.ph,
        do_mg_l=reading.do_mg_l,
        turbidity_ntu=reading.turbidity_ntu,
        salinity_ppt=reading.salinity_ppt,
        battery_v=reading.battery_v,
        rssi=reading.rssi,
        seq=reading.seq,
    )


def _to_latest_response(metrics: LatestMetrics) -> LatestMetricsResponse:
    return LatestMetricsResponse(
        measured_at=metrics.measured_at.isoformat() if metrics.measured_at is not None else None,
        tenant_id=metrics.tenant_id,
        pond_id=metrics.pond_id,
        temp_c=metrics.temp_c,
        ph=metrics.ph,
        do_mg_l=metrics.do_mg_l,
        turbidity_ntu=metrics.turbidity_ntu,
        salinity_ppt=metrics.salinity_ppt,
        battery_v=metrics.battery_v,
        rssi=metrics.rssi,
        seq=metrics.seq,
    )


@router.get(
    "/readings",
    response_model=TelemetryReadingListResponse,
    responses={403: {"model": ErrorResponse}, 404: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
)
async def query_readings(
    tenant_id: str,
    pond_id: str,
    repository: DomainRepositoryDep,
    telemetry_repository: TelemetryRepositoryDep,
    start: Annotated[str, Query()] = "-1h",
    stop: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 500,
    _access: TenantAccess = Depends(require_tenant_role(*tuple(READ_ROLES))),
) -> TelemetryReadingListResponse:
    try:
        validate_flux_time_bound(start)
        if stop is not None:
            validate_flux_time_bound(stop)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="invalid telemetry time bound") from exc

    service = _telemetry_service(repository, telemetry_repository)
    try:
        readings = await service.query_readings(
            tenant_id=tenant_id,
            pond_id=pond_id,
            start=start,
            stop=stop,
            limit=limit,
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc) or "not found") from exc
    return TelemetryReadingListResponse(items=[_to_reading_response(reading) for reading in readings])


@router.get(
    "/metrics/latest",
    response_model=LatestMetricsResponse,
    responses={403: {"model": ErrorResponse}, 404: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
)
async def query_latest_metrics(
    tenant_id: str,
    pond_id: str,
    repository: DomainRepositoryDep,
    telemetry_repository: TelemetryRepositoryDep,
    _access: TenantAccess = Depends(require_tenant_role(*tuple(READ_ROLES))),
) -> LatestMetricsResponse:
    service = _telemetry_service(repository, telemetry_repository)
    try:
        metrics = await service.query_latest_metrics(tenant_id=tenant_id, pond_id=pond_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc) or "not found") from exc
    return _to_latest_response(metrics)
