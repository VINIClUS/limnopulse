from fastapi import FastAPI

from limnopulse_api.api.router import api_router
from limnopulse_api.core.config import Settings, get_settings


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved_settings = settings or get_settings()
    app = FastAPI(title="Limnopulse API", version="0.1.0")
    app.state.settings = resolved_settings
    app.include_router(api_router)
    return app


app = create_app()
