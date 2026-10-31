from functools import lru_cache
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

AppEnv = Literal["local", "test", "staging", "prod"]
AuthMode = Literal["dev", "cognito"]
TelemetryReadSource = Literal["legacy", "v2"]


class Settings(BaseSettings):
    # Validation errors never echo input values: the settings carry tokens and secrets.
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", hide_input_in_errors=True)

    app_env: AppEnv = "local"
    auth_mode: AuthMode = "dev"
    aws_region: str = "us-east-1"
    cognito_user_pool_id: str = ""
    cognito_client_id: str = ""
    cognito_issuer: str = ""
    dynamodb_domain_table: str = "LimnopulseDomain"
    dynamodb_audit_table: str = "LimnopulseAudit"
    dynamodb_endpoint_url: str | None = None
    lead_rate_limit_per_minute: int = Field(default=5, ge=1, le=100)
    redis_url: str = "redis://localhost:6379/0"
    influxdb_url: str = "http://localhost:8086"
    influxdb_token: str = "local-dev-token"
    influxdb_org: str = "limnopulse"
    influxdb_bucket_raw: str = "limnopulse_raw"
    telemetry_default_range: str = "-1h"
    telemetry_max_limit: int = Field(default=1_000, ge=1, le=10_000)
    jwks_cache_ttl_seconds: int = Field(default=43_200, ge=21_600, le=86_400)
    membership_cache_ttl_seconds: int = Field(default=120, ge=60, le=300)
    device_cache_ttl_seconds: int = Field(default=1_800, ge=900, le=3_600)
    tenant_settings_cache_ttl_seconds: int = Field(default=1_800, ge=900, le=3_600)
    telegram_webhook_enabled: bool = True
    telegram_bot_username: str = Field(
        default="limnopulse_local_bot",
        pattern=r"^[A-Za-z0-9_]{5,32}$",
    )
    telegram_webhook_secret: str | None = Field(
        default=None,
        min_length=1,
        max_length=256,
        pattern=r"^[A-Za-z0-9_-]+$",
    )
    telegram_webhook_secret_arn: str | None = None
    telegram_webhook_secret_cache_ttl_seconds: int = Field(default=60, ge=5, le=300)
    api_v2_enabled: bool = False
    v1_pond_device_projection_enabled: bool = False
    telemetry_v2_write_enabled: bool = False
    telemetry_legacy_dual_write_enabled: bool = False
    api_v2_telemetry_enabled: bool = False
    v1_telemetry_read_source: TelemetryReadSource = "legacy"
    # P3 ingestion (dark by default; P3-10 wires the composition).
    ingest_v2_enabled: bool = False
    sqs_endpoint_url: str | None = None
    sqs_telemetry_ingress_queue_url: str = ""
    sqs_telemetry_ingress_dlq_url: str = ""
    ingest_rate_limit_per_minute: int = Field(default=120, ge=1, le=6_000)
    ingest_rate_limit_burst: int = Field(default=20, ge=1, le=1_000)
    ingest_rate_limiter_max_keys: int = Field(default=10_000, ge=100, le=1_000_000)
    # Pre-auth bucket capacity = max(1, per_minute // 10); refill = per_minute / 60 per second.
    ingest_preauth_rate_limit_per_minute: int = Field(default=60, ge=1, le=6_000)
    ingest_preauth_global_per_minute: int = Field(default=600, ge=1, le=60_000)
    telemetry_ingest_processing_deadline_seconds: int = Field(default=45, ge=5, le=900)
    telemetry_ingest_worker_concurrency: int = Field(default=4, ge=1, le=10)
    telemetry_ingest_worker_max_messages: int = Field(default=10, ge=1, le=10)
    telemetry_ingest_worker_wait_seconds: int = Field(default=20, ge=0, le=20)
    telemetry_ingest_worker_visibility_seconds: int = Field(default=60, ge=10, le=43_200)
    telemetry_ingest_retry_base_seconds: int = Field(default=30, ge=1, le=900)
    telemetry_ingest_retry_cap_seconds: int = Field(default=900, ge=1, le=43_200)
    telemetry_ingest_drain_seconds: int = Field(default=30, ge=1, le=300)

    @model_validator(mode="after")
    def validate_auth_mode_for_environment(self) -> "Settings":
        if self.auth_mode == "dev" and self.app_env not in {"local", "test"}:
            raise ValueError("AUTH_MODE=dev is only allowed when APP_ENV is local or test")
        if self.app_env in {"staging", "prod"} and self.telegram_webhook_enabled:
            if self.telegram_webhook_secret is not None:
                raise ValueError(
                    "TELEGRAM_WEBHOOK_SECRET is only allowed when APP_ENV is local or test"
                )
            if not self.telegram_webhook_secret_arn:
                raise ValueError("TELEGRAM_WEBHOOK_SECRET_ARN is required")
            if self.telegram_bot_username == "limnopulse_local_bot":
                raise ValueError("TELEGRAM_BOT_USERNAME must be configured")
        if self.api_v2_telemetry_enabled and not self.api_v2_enabled:
            raise ValueError("API_V2_TELEMETRY_ENABLED requires API_V2_ENABLED=true")
        if self.ingest_v2_enabled:
            if not self.api_v2_enabled:
                raise ValueError("INGEST_V2_ENABLED requires API_V2_ENABLED=true")
            if not self.telemetry_v2_write_enabled:
                raise ValueError("INGEST_V2_ENABLED requires TELEMETRY_V2_WRITE_ENABLED=true")
            if not self.sqs_telemetry_ingress_queue_url:
                raise ValueError("INGEST_V2_ENABLED requires SQS_TELEMETRY_INGRESS_QUEUE_URL")
        if self.telemetry_ingest_retry_base_seconds > self.telemetry_ingest_retry_cap_seconds:
            raise ValueError(
                "TELEMETRY_INGEST_RETRY_BASE_SECONDS must not exceed "
                "TELEMETRY_INGEST_RETRY_CAP_SECONDS"
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
