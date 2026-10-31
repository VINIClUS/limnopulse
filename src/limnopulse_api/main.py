from contextlib import asynccontextmanager

import boto3
import redis.asyncio as redis
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.requests import Request
from fastapi.responses import JSONResponse
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

from limnopulse_api.adapters.alert_events import DynamoAlertEventRepository
from limnopulse_api.adapters.alert_rules import DynamoAlertRuleRepository
from limnopulse_api.adapters.dynamodb import DynamoDomainRepository
from limnopulse_api.adapters.dynamodb_deployments import DynamoDeploymentRepository
from limnopulse_api.adapters.dynamodb_devices_v2 import DynamoDeviceComponentRepository
from limnopulse_api.adapters.dynamodb_ingest_credentials import DynamoIngestCredentialRepository
from limnopulse_api.adapters.dynamodb_ingress_receipts import DynamoIngressReceiptRepository
from limnopulse_api.adapters.dynamodb_integrations import DynamoIntegrationRepository
from limnopulse_api.adapters.dynamodb_sites_assets import DynamoSiteAssetRepository
from limnopulse_api.adapters.influxdb import InfluxTelemetryRepository
from limnopulse_api.adapters.influxdb_v1_projection import V2ProjectedTelemetryRepository
from limnopulse_api.adapters.influxdb_v2_reader import InfluxObservationReader
from limnopulse_api.adapters.influxdb_v2_writer import InfluxObservationWriter
from limnopulse_api.adapters.leads import DynamoLeadRepository
from limnopulse_api.adapters.notification_preferences import (
    DynamoNotificationPreferenceRepository,
)
from limnopulse_api.adapters.redis import RedisCacheRepository
from limnopulse_api.adapters.sqs_ingestion import SqsIngressQueue
from limnopulse_api.adapters.telegram_bindings import DynamoTelegramBindingRepository
from limnopulse_api.adapters.telemetry_metrics_log import StructuredLogTelemetryMetrics
from limnopulse_api.api.ingestion_dependencies import INGESTION_STATE_NAMES
from limnopulse_api.api.router import build_api_router
from limnopulse_api.api.v2.router import v2_safe_request_validation_exception_handler
from limnopulse_api.auth.providers import build_auth_provider
from limnopulse_api.core.config import Settings, get_settings
from limnopulse_api.core.errors import TelemetryQueryError
from limnopulse_api.services.cognito_identity import CognitoIdentityVerifier, DevIdentityVerifier
from limnopulse_api.services.connector_auth import IngestCredentialAuthenticator
from limnopulse_api.services.ingress_entitlements import AllowDirectIngressEntitlements
from limnopulse_api.services.ingress_rate_limit import ingress_rate_limiters
from limnopulse_api.services.memberships import MembershipService
from limnopulse_api.services.telegram_bindings import TelegramBindingService
from limnopulse_api.services.telegram_webhook_secret import (
    SecretsManagerTelegramWebhookSecretVerifier,
    StaticTelegramWebhookSecretVerifier,
)
from limnopulse_api.services.telemetry_v2_writes import ObservationWriteService
from limnopulse_api.services.trusted_mapping import TrustedMappingService
from limnopulse_api.workers.telemetry_ingest import sqs_client_kwargs

__all__ = ["app", "create_app", "sqs_client_kwargs"]


def _dynamodb_client_kwargs(settings: Settings) -> dict[str, str]:
    kwargs: dict[str, str] = {"region_name": settings.aws_region}
    if settings.dynamodb_endpoint_url is not None:
        kwargs["endpoint_url"] = settings.dynamodb_endpoint_url
    if settings.app_env in {"local", "test"} and settings.dynamodb_endpoint_url is not None:
        kwargs["aws_access_key_id"] = "local"
        kwargs["aws_secret_access_key"] = "local"
    return kwargs


def _cognito_client_kwargs(settings: Settings) -> dict[str, str]:
    kwargs = {"region_name": settings.aws_region}
    if settings.app_env in {"local", "test"}:
        kwargs["aws_access_key_id"] = "local"
        kwargs["aws_secret_access_key"] = "local"
    return kwargs


def _wire_ingestion(app: FastAPI, settings: Settings, dynamodb_client: object) -> None:
    """The frozen P3 `app.state` ports (D15); built only when `INGEST_V2_ENABLED`."""
    table = settings.dynamodb_domain_table
    audit_table = settings.dynamodb_audit_table
    credentials = DynamoIngestCredentialRepository(
        table, dynamodb_client, audit_table_name=audit_table
    )
    integrations = DynamoIntegrationRepository(table, dynamodb_client)
    devices = DynamoDeviceComponentRepository(table, dynamodb_client)
    limiters = ingress_rate_limiters(settings)
    sqs_client = boto3.client("sqs", **sqs_client_kwargs(settings))
    app.state.ingest_credential_repository = credentials
    app.state.connector_authenticator = IngestCredentialAuthenticator(
        credentials=credentials, integrations=integrations
    )
    app.state.integration_lifecycle_repository = DynamoIntegrationRepository(
        table, dynamodb_client, audit_table_name=audit_table
    )
    app.state.trusted_mapping = TrustedMappingService(
        resolver=integrations, integrations=integrations, devices=devices, components=devices
    )
    app.state.ingress_receipt_repository = DynamoIngressReceiptRepository(table, dynamodb_client)
    app.state.ingress_queue_producer = SqsIngressQueue(
        client=sqs_client, queue_url=settings.sqs_telemetry_ingress_queue_url
    )
    app.state.ingress_preauth_global_rate_limiter = limiters.preauth_global
    app.state.ingress_preauth_rate_limiter = limiters.preauth
    app.state.ingress_rate_limiter = limiters.account
    app.state.ingress_entitlement_check = AllowDirectIngressEntitlements()
    app.state.telemetry_metrics = StructuredLogTelemetryMetrics()


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
                "notification_preference_repository",
                "telegram_binding_repository",
                "telegram_binding_service",
                "telegram_webhook_secret_verifier",
                "cognito_identity_verifier",
                "cognito_client",
                "membership_service",
                "auth_provider",
                "cache_repository",
                "redis_client",
                "telemetry_repository",
                "influxdb_client",
                "site_asset_repository",
                "device_component_repository",
                "integration_repository",
                "deployment_repository",
                "observation_reader",
                "observation_writer",
                "observation_write_service",
                *INGESTION_STATE_NAMES,
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
        app.state.lead_repository = DynamoLeadRepository(
            resolved_settings.dynamodb_domain_table, dynamodb_client
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
        app.state.notification_preference_repository = DynamoNotificationPreferenceRepository(
            domain_table_name=resolved_settings.dynamodb_domain_table,
            audit_table_name=resolved_settings.dynamodb_audit_table,
            client=dynamodb_client,
        )
        app.state.telegram_binding_repository = DynamoTelegramBindingRepository(
            table_name=resolved_settings.dynamodb_domain_table,
            client=dynamodb_client,
        )
        app.state.site_asset_repository = DynamoSiteAssetRepository(
            resolved_settings.dynamodb_domain_table, dynamodb_client
        )
        app.state.device_component_repository = DynamoDeviceComponentRepository(
            resolved_settings.dynamodb_domain_table, dynamodb_client
        )
        app.state.integration_repository = DynamoIntegrationRepository(
            resolved_settings.dynamodb_domain_table, dynamodb_client
        )
        app.state.deployment_repository = DynamoDeploymentRepository(
            resolved_settings.dynamodb_domain_table, dynamodb_client
        )
        if resolved_settings.ingest_v2_enabled:
            _wire_ingestion(app, resolved_settings, dynamodb_client)
        if resolved_settings.auth_mode == "dev":
            app.state.cognito_identity_verifier = DevIdentityVerifier()
        else:
            cognito_client = boto3.client(
                "cognito-idp",
                **_cognito_client_kwargs(resolved_settings),
            )
            app.state.cognito_client = cognito_client
            app.state.cognito_identity_verifier = CognitoIdentityVerifier(cognito_client)
        app.state.membership_service = MembershipService(
            domain_repository=app.state.domain_repository,
            cache=app.state.cache_repository,
            membership_ttl_seconds=resolved_settings.membership_cache_ttl_seconds,
        )
        app.state.telegram_binding_service = TelegramBindingService(
            app.state.telegram_binding_repository,
            app.state.membership_service,
            bot_username=resolved_settings.telegram_bot_username,
        )
        if resolved_settings.telegram_webhook_enabled:
            if resolved_settings.app_env in {"local", "test"}:
                app.state.telegram_webhook_secret_verifier = StaticTelegramWebhookSecretVerifier(
                    resolved_settings.telegram_webhook_secret or "local-telegram-webhook-secret"
                )
            else:
                secrets_client = boto3.client(
                    "secretsmanager",
                    region_name=resolved_settings.aws_region,
                )
                app.state.telegram_webhook_secret_verifier = (
                    SecretsManagerTelegramWebhookSecretVerifier(
                        secrets_client,
                        resolved_settings.telegram_webhook_secret_arn or "",
                        cache_ttl_seconds=(
                            resolved_settings.telegram_webhook_secret_cache_ttl_seconds
                        ),
                    )
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
        query_api = influxdb_client.query_api()
        influxdb_org = resolved_settings.influxdb_org
        influxdb_bucket = resolved_settings.influxdb_bucket_raw
        if resolved_settings.v1_telemetry_read_source == "v2":
            app.state.telemetry_repository = V2ProjectedTelemetryRepository(
                query_api=query_api,
                org=influxdb_org,
                bucket=influxdb_bucket,
                domain_repository=app.state.domain_repository,
            )
        else:
            app.state.telemetry_repository = InfluxTelemetryRepository(
                query_api=query_api, org=influxdb_org, bucket=influxdb_bucket
            )
        app.state.observation_reader = InfluxObservationReader(
            query_api=query_api, org=influxdb_org, bucket=influxdb_bucket
        )
        v2_write_enabled = resolved_settings.telemetry_v2_write_enabled
        legacy_dual_write_enabled = resolved_settings.telemetry_legacy_dual_write_enabled
        # The default write_api() batches in a background thread and would acknowledge failed
        # writes; a synchronous one is created only when a write switch is on.
        observation_writer = None
        if v2_write_enabled or legacy_dual_write_enabled:
            observation_writer = InfluxObservationWriter(
                write_api=influxdb_client.write_api(write_options=SYNCHRONOUS),
                bucket=influxdb_bucket,
                org=influxdb_org,
            )
        app.state.observation_writer = observation_writer
        app.state.observation_write_service = ObservationWriteService(
            writer=observation_writer if v2_write_enabled else None,
            legacy_writer=observation_writer if legacy_dual_write_enabled else None,
            site_assets=app.state.site_asset_repository,
            v2_write_enabled=v2_write_enabled,
            legacy_dual_write_enabled=legacy_dual_write_enabled,
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
    app.add_exception_handler(RequestValidationError, v2_safe_request_validation_exception_handler)
    app.include_router(
        build_api_router(
            telegram_webhook_enabled=resolved_settings.telegram_webhook_enabled,
            enable_v2=resolved_settings.api_v2_enabled,
            enable_v2_telemetry=resolved_settings.api_v2_telemetry_enabled,
            enable_ingest=resolved_settings.ingest_v2_enabled,
        )
    )
    return app


app = create_app()
