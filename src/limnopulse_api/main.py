from contextlib import asynccontextmanager

import boto3
import redis.asyncio as redis
from fastapi import FastAPI
from fastapi.requests import Request
from fastapi.responses import JSONResponse
from botocore.exceptions import BotoCoreError, ClientError
from influxdb_client import InfluxDBClient

from limnopulse_api.adapters.dynamodb import DynamoDomainRepository
from limnopulse_api.adapters.alert_rules import DynamoAlertRuleRepository
from limnopulse_api.adapters.alert_events import DynamoAlertEventRepository
from limnopulse_api.adapters.influxdb import InfluxTelemetryRepository
from limnopulse_api.adapters.redis import RedisCacheRepository
from limnopulse_api.api.router import api_router
from limnopulse_api.auth.providers import build_auth_provider
from limnopulse_api.core.config import Settings, get_settings
from limnopulse_api.core.errors import TelemetryQueryError
from limnopulse_api.services.memberships import MembershipService


def _dynamodb_client_kwargs(settings: Settings) -> dict[str, str]:
    kwargs: dict[str, str] = {"region_name": settings.aws_region}
    if settings.dynamodb_endpoint_url is not None:
        kwargs["endpoint_url"] = settings.dynamodb_endpoint_url
    if settings.app_env in {"local", "test"} and settings.dynamodb_endpoint_url is not None:
        kwargs["aws_access_key_id"] = "local"
        kwargs["aws_secret_access_key"] = "local"
    return kwargs


async def _handle_infrastructure_error(request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=503, content={"detail": "service unavailable"})


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved_settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if any(
            hasattr(app.state, attribute_name)
            for attribute_name in (
                "domain_repository",
                "alert_rule_repository",
                "alert_event_repository",
                "membership_service",
                "auth_provider",
                "cache_repository",
                "redis_client",
                "telemetry_repository",
                "influxdb_client",
            )
        ):
            yield
            return

        redis_client = redis.from_url(resolved_settings.redis_url)
        app.state.redis_client = redis_client
        app.state.cache_repository = RedisCacheRepository(redis_client)
        dynamodb_client = boto3.client(
            "dynamodb",
            **_dynamodb_client_kwargs(resolved_settings),
        )
        app.state.domain_repository = DynamoDomainRepository(
            table_name=resolved_settings.dynamodb_domain_table,
            client=dynamodb_client,
        )
        app.state.alert_rule_repository = DynamoAlertRuleRepository(
            domain_table_name=resolved_settings.dynamodb_domain_table,
            audit_table_name=resolved_settings.dynamodb_audit_table,
            client=dynamodb_client,
        )
        app.state.alert_event_repository = DynamoAlertEventRepository(
            domain_table_name=resolved_settings.dynamodb_domain_table,
            audit_table_name=resolved_settings.dynamodb_audit_table,
            client=dynamodb_client,
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
        influxdb_client = InfluxDBClient(
            url=resolved_settings.influxdb_url,
            token=resolved_settings.influxdb_token,
            org=resolved_settings.influxdb_org,
        )
        app.state.influxdb_client = influxdb_client
        app.state.telemetry_repository = InfluxTelemetryRepository(
            query_api=influxdb_client.query_api(),
            org=resolved_settings.influxdb_org,
            bucket=resolved_settings.influxdb_bucket_raw,
        )

        try:
            yield
        finally:
            await redis_client.aclose()
            influxdb_client.close()

    app = FastAPI(title="Limnopulse API", version="0.1.0", lifespan=lifespan)
    app.state.settings = resolved_settings
    app.add_exception_handler(BotoCoreError, _handle_infrastructure_error)
    app.add_exception_handler(ClientError, _handle_infrastructure_error)
    app.add_exception_handler(TelemetryQueryError, _handle_infrastructure_error)
    app.include_router(api_router)
    return app


app = create_app()
