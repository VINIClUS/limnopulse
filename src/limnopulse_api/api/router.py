from fastapi import APIRouter

from limnopulse_api.api.v1.routers import health, me, tenants

api_router = APIRouter()
api_router.include_router(health.router)
api_router.include_router(me.router, prefix="/v1")
api_router.include_router(tenants.router, prefix="/v1")
