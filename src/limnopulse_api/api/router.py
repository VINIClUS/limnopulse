from fastapi import APIRouter

from limnopulse_api.api import telegram_webhook
from limnopulse_api.api.v1.routers import (
    alert_events,
    alert_rules,
    devices,
    health,
    leads,
    me,
    notification_preferences,
    ponds,
    telegram_bindings,
    telemetry,
    tenants,
)

def build_api_router(*, telegram_webhook_enabled: bool = True) -> APIRouter:
    api_router = APIRouter()
    if telegram_webhook_enabled:
        api_router.include_router(telegram_webhook.router)
    api_router.include_router(health.router)
    api_router.include_router(me.router, prefix="/v1")
    api_router.include_router(notification_preferences.router, prefix="/v1")
    api_router.include_router(telegram_bindings.router, prefix="/v1")
    api_router.include_router(tenants.router, prefix="/v1")
    api_router.include_router(ponds.router, prefix="/v1")
    api_router.include_router(telemetry.router, prefix="/v1")
    api_router.include_router(telemetry.tenant_router, prefix="/v1")
    api_router.include_router(devices.router, prefix="/v1")
    api_router.include_router(alert_rules.router, prefix="/v1")
    api_router.include_router(alert_events.router, prefix="/v1")
    api_router.include_router(leads.router, prefix="/v1")
    return api_router


api_router = build_api_router()
