from functools import lru_cache
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


AppEnv = Literal["local", "test", "staging", "prod"]
AuthMode = Literal["dev", "cognito"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: AppEnv = "local"
    auth_mode: AuthMode = "dev"
    aws_region: str = "us-east-1"
    cognito_user_pool_id: str = ""
    cognito_client_id: str = ""
    cognito_issuer: str = ""
    dynamodb_domain_table: str = "LimnopulseDomain"
    dynamodb_audit_table: str = "LimnopulseAudit"
    dynamodb_endpoint_url: str | None = None
    redis_url: str = "redis://localhost:6379/0"
    jwks_cache_ttl_seconds: int = Field(default=43_200, ge=21_600, le=86_400)
    membership_cache_ttl_seconds: int = Field(default=120, ge=60, le=300)
    device_cache_ttl_seconds: int = Field(default=1_800, ge=900, le=3_600)
    tenant_settings_cache_ttl_seconds: int = Field(default=1_800, ge=900, le=3_600)

    @model_validator(mode="after")
    def validate_auth_mode_for_environment(self) -> "Settings":
        if self.auth_mode == "dev" and self.app_env not in {"local", "test"}:
            raise ValueError("AUTH_MODE=dev is only allowed when APP_ENV is local or test")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
