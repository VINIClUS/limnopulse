from collections.abc import AsyncIterator
from typing import Any, ClassVar

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

import limnopulse_api.api.telegram_webhook as telegram_webhook_module
from limnopulse_api.api.telegram_webhook import (
    MAX_TELEGRAM_UPDATE_BYTES,
    _read_update_body,
)
from limnopulse_api.core.config import Settings
from limnopulse_api.main import create_app
from limnopulse_api.services.telegram_bindings import BindingCommand, BindingCommandResult

PATH = "/webhooks/telegram"
SECRET = "telegram-webhook-secret"


class StaticSecretVerifier:
    def __init__(self, expected: str = SECRET) -> None:
        self.expected = expected
        self.calls: list[str | None] = []

    async def verify(self, supplied: str | None) -> bool:
        self.calls.append(supplied)
        return supplied == self.expected


class RecordingBindingService:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[BindingCommand] = []

    async def handle(self, command: BindingCommand) -> BindingCommandResult:
        self.calls.append(command)
        if self.error is not None:
            raise self.error
        return BindingCommandResult(applied=True)


def build_client(
    service: RecordingBindingService,
    verifier: StaticSecretVerifier | None = None,
) -> TestClient:
    app = create_app(Settings(app_env="test", auth_mode="dev"))
    app.state.telegram_binding_service = service
    app.state.telegram_webhook_secret_verifier = verifier or StaticSecretVerifier()
    return TestClient(app)


def telegram_update(text: str, *, update_id: int = 10, chat_id: int = 123) -> dict[str, Any]:
    return {
        "update_id": update_id,
        "message": {
            "message_id": 20,
            "from": {"id": chat_id, "is_bot": False, "first_name": "Alice"},
            "chat": {"id": chat_id, "type": "private", "first_name": "Alice"},
            "date": 1_786_620_000,
            "text": text,
        },
    }


def test_webhook_authenticates_secret_before_parsing_body() -> None:
    service = RecordingBindingService()
    verifier = StaticSecretVerifier()
    client = build_client(service, verifier)

    missing = client.post(PATH, content=b"not-json")
    wrong = client.post(
        PATH,
        content=b"not-json",
        headers={"X-Telegram-Bot-Api-Secret-Token": "wrong"},
    )

    assert missing.status_code == 401
    assert wrong.status_code == 401
    assert verifier.calls == [None, "wrong"]
    assert service.calls == []


def test_webhook_extracts_start_and_stop_from_private_chat() -> None:
    service = RecordingBindingService()
    client = build_client(service)

    started = client.post(
        PATH,
        json=telegram_update("/start AQIDBAUGBwgJCgsMDQ4PEA", update_id=11),
        headers={"X-Telegram-Bot-Api-Secret-Token": SECRET},
    )
    stopped = client.post(
        PATH,
        json=telegram_update("/stop", update_id=12),
        headers={"X-Telegram-Bot-Api-Secret-Token": SECRET},
    )

    assert started.status_code == 200
    assert stopped.status_code == 200
    assert [call.model_dump(mode="json") for call in service.calls] == [
        {
            "kind": "start",
            "update_id": 11,
            "chat_id": 123,
            "sender_id": 123,
            "token": "AQIDBAUGBwgJCgsMDQ4PEA",
        },
        {
            "kind": "stop",
            "update_id": 12,
            "chat_id": 123,
            "sender_id": 123,
            "token": None,
        },
    ]


def test_webhook_returns_generic_success_for_unsupported_or_invalid_token() -> None:
    service = RecordingBindingService()
    client = build_client(service)
    headers = {"X-Telegram-Bot-Api-Secret-Token": SECRET}

    unsupported = client.post(PATH, json=telegram_update("hello"), headers=headers)
    invalid_token = client.post(PATH, json=telegram_update("/start short"), headers=headers)

    assert unsupported.status_code == 200
    assert invalid_token.status_code == 200
    assert unsupported.json() == {"ok": True}
    assert invalid_token.json() == {"ok": True}
    assert service.calls == []


def test_webhook_rejects_malformed_envelope_and_non_private_chat() -> None:
    service = RecordingBindingService()
    client = build_client(service)
    headers = {"X-Telegram-Bot-Api-Secret-Token": SECRET}

    malformed = client.post(PATH, json={"update_id": 1}, headers=headers)
    group = telegram_update("/stop")
    group["message"]["chat"]["type"] = "group"
    non_private = client.post(PATH, json=group, headers=headers)

    assert malformed.status_code == 400
    assert non_private.status_code == 400
    assert service.calls == []


def test_webhook_maps_transient_service_failure_to_retryable_503() -> None:
    client = build_client(RecordingBindingService(error=RuntimeError("dynamodb unavailable")))

    response = client.post(
        PATH,
        json=telegram_update("/stop"),
        headers={"X-Telegram-Bot-Api-Secret-Token": SECRET},
    )

    assert response.status_code == 503
    assert response.json() == {"detail": "service unavailable"}


def test_webhook_rejects_oversized_authenticated_body_before_domain_service() -> None:
    service = RecordingBindingService()
    client = build_client(service)

    response = client.post(
        PATH,
        content=b"{" + (b" " * (64 * 1024)) + b"}",
        headers={"X-Telegram-Bot-Api-Secret-Token": SECRET},
    )

    assert response.status_code == 413
    assert service.calls == []


@pytest.mark.asyncio
async def test_bounded_reader_rejects_chunk_before_buffering_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ChunkedRequest:
        headers: ClassVar[dict[str, str]] = {}

        async def stream(self) -> AsyncIterator[bytes]:
            yield b"x" * (MAX_TELEGRAM_UPDATE_BYTES + 1)

    class RecordingBuffer:
        extend_calls = 0

        def __len__(self) -> int:
            return 0

        def extend(self, chunk: bytes) -> None:
            type(self).extend_calls += 1

    monkeypatch.setattr(telegram_webhook_module, "bytearray", RecordingBuffer, raising=False)

    with pytest.raises(HTTPException) as captured:
        await _read_update_body(ChunkedRequest())  # type: ignore[arg-type]

    assert captured.value.status_code == 413
    assert RecordingBuffer.extend_calls == 0
