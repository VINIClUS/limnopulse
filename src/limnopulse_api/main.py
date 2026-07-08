from contextlib import asynccontextmanager

import boto3
import redis.asyncio as redis
from fastapi import FastAPI

from limnopulse_api.adapters.dynamodb import DynamoDomainRepository
from limnopulse_api.adapters.redis import RedisCacheRepository
from limnopulse_api.api.router import api_router
from limnopulse_api.auth.providers import build_auth_provider
from limnopulse_api.core.config import Settings, get_settings
from limnopulse_api.services.memberships import MembershipService


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved_settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if any(
            hasattr(app.state, attribute_name)
            for attribute_name in (
                "domain_repository",
                "membership_service",
                "auth_provider",
                "cache_repository",
                "redis_client",
            )
        ):
            yield
            return

        redis_client = redis.from_url(resolved_settings.redis_url)
        app.state.redis_client = redis_client
        app.state.cache_repository = RedisCacheRepository(redis_client)
        app.state.domain_repository = DynamoDomainRepository(
            table_name=resolved_settings.dynamodb_domain_table,
            client=boto3.client(
                "dynamodb",
                region_name=resolved_settings.aws_region,
                endpoint_url=resolved_settings.dynamodb_endpoint_url,
            ),
        )
        app.state.membership_service = MembershipService(
            domain_repository=app.state.domain_repository,
            cache=app.state.cache_repository,
            membership_ttl_seconds=resolved_settings.membership_cache_ttl_seconds,
        )
        app.state.auth_provider = build_auth_provider(
            resolved_settings,
            cache=app.state.cache_repository,
        )

        try:
            yield
        finally:
            await redis_client.aclose()

    app = FastAPI(title="Limnopulse API", version="0.1.0", lifespan=lifespan)
    app.state.settings = resolved_settings
    app.include_router(api_router)
    return app


app = create_app()
