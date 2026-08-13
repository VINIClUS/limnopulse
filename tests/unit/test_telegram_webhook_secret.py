import pytest

from limnopulse_api.services.telegram_webhook_secret import (
    SecretsManagerTelegramWebhookSecretVerifier,
)


class FakeSecretsManager:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    def get_secret_value(self, **kwargs: str) -> dict[str, str]:
        self.calls.append(kwargs)
        return {
            "SecretString": (
                "current-secret" if kwargs["VersionStage"] == "AWSCURRENT" else "previous-secret"
            )
        }


@pytest.mark.asyncio
async def test_secret_verifier_accepts_current_and_previous_with_bounded_cache() -> None:
    client = FakeSecretsManager()
    now = [10.0]
    verifier = SecretsManagerTelegramWebhookSecretVerifier(
        client,
        "secret-arn",
        cache_ttl_seconds=60,
        monotonic=lambda: now[0],
    )

    assert await verifier.verify("current-secret") is True
    assert await verifier.verify("previous-secret") is True
    assert await verifier.verify("wrong") is False
    assert client.calls == [
        {"SecretId": "secret-arn", "VersionStage": "AWSCURRENT"},
        {"SecretId": "secret-arn", "VersionStage": "AWSPREVIOUS"},
    ]

    now[0] = 71.0
    assert await verifier.verify("current-secret") is True
    assert len(client.calls) == 4
