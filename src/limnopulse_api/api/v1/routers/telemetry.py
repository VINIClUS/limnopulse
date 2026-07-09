from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query

from limnopulse_api.api.dependencies import (
    DomainRepositoryDep,
    TelemetryRepositoryDep,
    require_tenant_role,
)
from limnopulse_api.api.v1.schemas.common import ErrorResponse
from limnopulse_api.api.v1.schemas.telemetry import (
    LatestMetricsResponse,
    TelemetryReadingResponse,
    TelemetryReadingsResponse,
)
from limnopulse_api.core.errors import NotFoundError
from limnopulse_api.domain.entities import TenantAccess
from limnopulse_api.domain.roles import READ_ROLES
from limnopulse_api.domain.telemetry import TelemetryReading, WATER_QUALITY_FIELDS
from limnopulse_api.services.telemetry import TelemetryService

router = APIRouter(prefix="/tenants/{tenant_id}/ponds/{pond_id}", tags=["telemetry"])


def _telemetry_service(domain_repository, telemetry_repository) -> TelemetryService:
    return TelemetryService(
        domain_repository=domain_repository,
        telemetry_repository=telemetry_repository,
    )


def _to_reading_response(reading: TelemetryReading) -> TelemetryReadingResponse:
    return TelemetryReadingResponse(
        ts=reading.timestamp,
        tenant_id=reading.tenant_id,
        pond_id=reading.pond_id,
        device_id=reading.device_id,
        metrics=dict(reading.metrics),
    )


def _resolve_metric_fields(fields: list[str] | None) -> tuple[str, ...]:
    if not fields:
        return WATER_QUALITY_FIELDS

    deduped_fields = tuple(dict.fromkeys(fields))
    invalid_fields = sorted(set(deduped_fields) - set(WATER_QUALITY_FIELDS))
    if invalid_fields:
        raise HTTPException(
            status_code=422,
            detail=f"unsupported telemetry fields: {', '.join(invalid_fields)}",
        )
    return deduped_fields


def _as_utc(value: datetime, parameter_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise HTTPException(status_code=422, detail=f"{parameter_name} must include timezone")
    return value.astimezone(UTC)


def _validate_time_range(start: datetime, stop: datetime) -> tuple[datetime, datetime]:
    resolved_start = _as_utc(start, "start")
    resolved_stop = _as_utc(stop, "stop")
    if resolved_start >= resolved_stop:
        raise HTTPException(status_code=422, detail="start must be before stop")
    return resolved_start, resolved_stop


@router.get(
    "/readings",
    response_model=TelemetryReadingsResponse,
    responses={
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        422: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
)
async def list_readings(
    tenant_id: str,
    pond_id: str,
    domain_repository: DomainRepositoryDep,
    telemetry_repository: TelemetryRepositoryDep,
    _access: TenantAccess = Depends(require_tenant_role(*tuple(READ_ROLES))),
    start: datetime = Query(..., description="Inclusive RFC3339 timestamp."),
    stop: datetime | None = Query(default=None, description="Exclusive RFC3339 timestamp."),
    limit: int = Query(default=100, ge=1, le=1000),
    fields: list[str] | None = Query(default=None),
) -> TelemetryReadingsResponse:
    resolved_fields = _resolve_metric_fields(fields)
    resolved_stop = stop or datetime.now(UTC)
    resolved_start, resolved_stop = _validate_time_range(start, resolved_stop)
    service = _telemetry_service(domain_repository, telemetry_repository)
    try:
        readings = await service.list_readings(
            tenant_id=tenant_id,
            pond_id=pond_id,
            start=resolved_start,
            stop=resolved_stop,
            limit=limit,
            fields=resolved_fields,
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc) or "not found") from exc
    return TelemetryReadingsResponse(items=[_to_reading_response(reading) for reading in readings])


@router.get(
    "/metrics/latest",
    response_model=LatestMetricsResponse,
    responses={
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        422: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
)
async def latest_metrics(
    tenant_id: str,
    pond_id: str,
    domain_repository: DomainRepositoryDep,
    telemetry_repository: TelemetryRepositoryDep,
    _access: TenantAccess = Depends(require_tenant_role(*tuple(READ_ROLES))),
    stop: datetime | None = Query(default=None, description="End of the lookback window."),
    lookback_seconds: int = Query(default=3600, ge=60, le=86_400),
    fields: list[str] | None = Query(default=None),
) -> LatestMetricsResponse:
    resolved_fields = _resolve_metric_fields(fields)
    resolved_stop = _as_utc(stop, "stop") if stop is not None else datetime.now(UTC)
    resolved_start = resolved_stop - timedelta(seconds=lookback_seconds)
    service = _telemetry_service(domain_repository, telemetry_repository)
    try:
        reading = await service.latest_metrics(
            tenant_id=tenant_id,
            pond_id=pond_id,
            start=resolved_start,
            stop=resolved_stop,
            fields=resolved_fields,
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc) or "not found") from exc
    if reading is None:
        return LatestMetricsResponse(tenant_id=tenant_id, pond_id=pond_id)
    return LatestMetricsResponse(
        tenant_id=reading.tenant_id,
        pond_id=reading.pond_id,
        ts=reading.timestamp,
        device_id=reading.device_id,
        metrics=dict(reading.metrics),
    )
