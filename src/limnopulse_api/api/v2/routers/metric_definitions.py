"""Unmounted `/v2` metric-definition router; P2-08 mounts it behind `API_V2_TELEMETRY_ENABLED`."""

from typing import Annotated

from fastapi import APIRouter, Depends

from limnopulse_api.api.dependencies import require_tenant_role
from limnopulse_api.api.v1.schemas.common import ErrorResponse
from limnopulse_api.api.v2.routers.devices import V2JsonSafeRoute
from limnopulse_api.api.v2.schemas.metric_definitions import (
    V2MetricDefinitionListResponse,
    V2MetricDefinitionResponse,
)
from limnopulse_api.domain.entities import TenantAccess
from limnopulse_api.domain.metric_catalog import metric_catalog
from limnopulse_api.domain.roles import READ_ROLES
from limnopulse_api.domain.telemetry_v2 import METRIC_CATALOG_VERSION

router = APIRouter(
    prefix="/tenants/{tenant_id}", tags=["v2-telemetry"], route_class=V2JsonSafeRoute
)


@router.get(
    "/metric-definitions",
    response_model=V2MetricDefinitionListResponse,
    responses={403: {"model": ErrorResponse}},
)
async def list_metric_definitions(
    tenant_id: str,
    _access: Annotated[TenantAccess, Depends(require_tenant_role(*READ_ROLES))],
) -> V2MetricDefinitionListResponse:
    return V2MetricDefinitionListResponse(
        items=[
            V2MetricDefinitionResponse.from_domain(definition, METRIC_CATALOG_VERSION)
            for definition in metric_catalog(METRIC_CATALOG_VERSION)
        ]
    )
