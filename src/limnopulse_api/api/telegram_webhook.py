import json
from typing import Annotated, Literal, Protocol

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from limnopulse_api.services.telegram_bindings import (
    BindingCommand,
    BindingCommandKind,
    BindingCommandResult,
)
from limnopulse_api.services.telegram_webhook_secret import TelegramWebhookSecretVerifier

router = APIRouter(tags=["telegram-webhook"])
MAX_TELEGRAM_UPDATE_BYTES = 64 * 1024


class BindingCommandHandler(Protocol):
    async def handle(self, command: BindingCommand) -> BindingCommandResult:
        raise NotImplementedError


class TelegramSender(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int = Field(gt=0)
    is_bot: Literal[False]


class TelegramChat(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int = Field(gt=0)
    type: Literal["private"]


class TelegramMessage(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    message_id: int = Field(ge=0)
    sender: TelegramSender = Field(alias="from")
    chat: TelegramChat
    date: int = Field(ge=0)
    text: str = Field(min_length=1, max_length=4_096)

    @model_validator(mode="after")
    def validate_private_sender(self) -> "TelegramMessage":
        if self.sender.id != self.chat.id:
            raise ValueError("private chat sender must match chat")
        return self


class TelegramUpdate(BaseModel):
    model_config = ConfigDict(extra="ignore")

    update_id: int = Field(ge=0)
    message: TelegramMessage


def _state_dependency(request: Request, name: str):
    try:
        return getattr(request.app.state, name)
    except AttributeError as exc:
        raise HTTPException(status_code=503, detail="service unavailable") from exc


def _command(update: TelegramUpdate) -> BindingCommand | None:
    text = update.message.text.strip()
    if text == "/stop":
        return BindingCommand(
            kind=BindingCommandKind.STOP,
            update_id=update.update_id,
            chat_id=update.message.chat.id,
            sender_id=update.message.sender.id,
        )
    if text.startswith("/start "):
        token = text.removeprefix("/start ")
        try:
            return BindingCommand(
                kind=BindingCommandKind.START,
                update_id=update.update_id,
                chat_id=update.message.chat.id,
                sender_id=update.message.sender.id,
                token=token,
            )
        except ValidationError:
            return None
    return None


async def _read_update_body(request: Request) -> bytes:
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            declared_length = int(content_length)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="invalid Telegram update") from exc
        if declared_length < 0:
            raise HTTPException(status_code=400, detail="invalid Telegram update")
        if declared_length > MAX_TELEGRAM_UPDATE_BYTES:
            raise HTTPException(status_code=413, detail="Telegram update too large")
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > MAX_TELEGRAM_UPDATE_BYTES:
            raise HTTPException(status_code=413, detail="Telegram update too large")
        body.extend(chunk)
    return bytes(body)


@router.post("/webhooks/telegram")
async def telegram_webhook(
    request: Request,
    secret: Annotated[
        str | None,
        Header(alias="X-Telegram-Bot-Api-Secret-Token"),
    ] = None,
) -> dict[str, bool]:
    verifier: TelegramWebhookSecretVerifier = _state_dependency(
        request,
        "telegram_webhook_secret_verifier",
    )
    try:
        authenticated = await verifier.verify(secret)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="service unavailable") from exc
    if not authenticated:
        raise HTTPException(status_code=401, detail="authentication required")

    try:
        payload = json.loads(await _read_update_body(request))
        update = TelegramUpdate.model_validate(payload)
    except (ValueError, ValidationError) as exc:
        raise HTTPException(status_code=400, detail="invalid Telegram update") from exc
    command = _command(update)
    if command is None:
        return {"ok": True}
    service: BindingCommandHandler = _state_dependency(request, "telegram_binding_service")
    try:
        await service.handle(command)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="service unavailable") from exc
    return {"ok": True}
