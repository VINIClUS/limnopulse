from fastapi import APIRouter

from limnopulse_api.api.v1.routers import (
    alert_rules,
    devices,
    health,
    me,
    ponds,
    telemetry,
    tenants,
)

api_router = APIRouter()
api_router.include_router(health.router)
api_router.include_router(me.router, prefix="/v1")
api_router.include_router(tenants.router, prefix="/v1")
api_router.include_router(ponds.router, prefix="/v1")
api_router.include_router(telemetry.router, prefix="/v1")
api_router.include_router(devices.router, prefix="/v1")
api_router.include_router(alert_rules.router, prefix="/v1")
