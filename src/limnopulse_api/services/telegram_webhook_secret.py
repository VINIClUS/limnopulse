import hmac
import time
from asyncio import to_thread
from collections.abc import Callable
from typing import Any, Protocol


class TelegramWebhookSecretVerifier(Protocol):
    async def verify(self, supplied: str | None) -> bool:
        raise NotImplementedError


class StaticTelegramWebhookSecretVerifier:
    def __init__(self, secret: str) -> None:
        if not secret:
            raise ValueError("Telegram webhook secret must not be empty")
        self.secret = secret

    async def verify(self, supplied: str | None) -> bool:
        return hmac.compare_digest(supplied or "", self.secret)


class SecretsManagerTelegramWebhookSecretVerifier:
    def __init__(
        self,
        client: Any,
        secret_id: str,
        *,
        cache_ttl_seconds: float = 60,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not secret_id:
            raise ValueError("Telegram webhook secret ID must not be empty")
        if cache_ttl_seconds <= 0:
            raise ValueError("Telegram webhook secret cache TTL must be positive")
        self.client = client
        self.secret_id = secret_id
        self.cache_ttl_seconds = cache_ttl_seconds
        self.monotonic = monotonic
        self._cached: tuple[str, ...] = ()
        self._cached_until = 0.0

    async def verify(self, supplied: str | None) -> bool:
        secrets = await self._secrets()
        candidate = supplied or ""
        matched = False
        for secret in secrets:
            matched = hmac.compare_digest(candidate, secret) or matched
        return matched

    async def _secrets(self) -> tuple[str, ...]:
        now = self.monotonic()
        if self._cached and now < self._cached_until:
            return self._cached
        values: list[str] = []
        for stage in ("AWSCURRENT", "AWSPREVIOUS"):
            try:
                response = await to_thread(
                    self.client.get_secret_value,
                    SecretId=self.secret_id,
                    VersionStage=stage,
                )
            except Exception:
                if stage == "AWSPREVIOUS":
                    continue
                raise
            secret = response.get("SecretString")
            if isinstance(secret, str) and secret and secret not in values:
                values.append(secret)
        if not values:
            raise RuntimeError("Telegram webhook secret has no string value")
        self._cached = tuple(values[:2])
        self._cached_until = now + self.cache_ttl_seconds
        return self._cached
