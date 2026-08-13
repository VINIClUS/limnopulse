import re
import secrets
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from hashlib import sha256
from typing import Protocol
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from limnopulse_api.domain.telegram import (
    TelegramBindingRequest,
    TelegramBindingStatus,
    TelegramBindingView,
    telegram_binding_view,
)
from limnopulse_api.repositories.telegram_bindings import TelegramBindingRepository

TOKEN_TTL = timedelta(minutes=10)
_TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9_-]{22}$")
_BOT_USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9_]{5,32}$")


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


class ActiveMembershipLookup(Protocol):
    async def is_active(self, recipient_id: str, tenant_id: str) -> bool:
        raise NotImplementedError


class BindingCommandKind(StrEnum):
    START = "start"
    STOP = "stop"


class BindingCommand(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: BindingCommandKind
    update_id: int = Field(ge=0)
    chat_id: int = Field(gt=0)
    sender_id: int = Field(gt=0)
    token: str | None = None

    @model_validator(mode="after")
    def validate_token_for_start(self) -> "BindingCommand":
        if self.kind is BindingCommandKind.START:
            if self.token is None or not _TOKEN_PATTERN.fullmatch(self.token):
                raise ValueError("start command requires a valid binding token")
        elif self.token is not None:
            raise ValueError("stop command does not accept a token")
        return self


class BindingCommandResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    applied: bool


class IssuedBindingToken(BaseModel):
    model_config = ConfigDict(frozen=True)

    request_id: str
    token: str
    token_hash: str
    deep_link: str
    expires_at: datetime


class TelegramBindingService:
    def __init__(
        self,
        repository: TelegramBindingRepository,
        membership_lookup: ActiveMembershipLookup,
        *,
        bot_username: str,
        clock: Callable[[], datetime] = utc_now,
        token_factory: Callable[[], str] = lambda: secrets.token_urlsafe(16),
    ) -> None:
        if not _BOT_USERNAME_PATTERN.fullmatch(bot_username):
            raise ValueError("Telegram bot username is invalid")
        self.repository = repository
        self.membership_lookup = membership_lookup
        self.bot_username = bot_username
        self.clock = clock
        self.token_factory = token_factory

    async def issue(self, tenant_id: str, recipient_id: str) -> IssuedBindingToken:
        token = self.token_factory()
        if not _TOKEN_PATTERN.fullmatch(token):
            raise ValueError("binding token factory returned an invalid token")
        now = self.clock().astimezone(UTC).replace(microsecond=0)
        token_hash = sha256(token.encode("ascii")).hexdigest()
        request = TelegramBindingRequest(
            request_id=f"telegram_binding_request_{uuid4().hex}",
            tenant_id=tenant_id,
            recipient_id=recipient_id,
            token_hash=token_hash,
            status=TelegramBindingStatus.PENDING,
            expires_at=now + TOKEN_TTL,
            created_at=now,
        )
        await self.repository.issue(request)
        return IssuedBindingToken(
            request_id=request.request_id,
            token=token,
            token_hash=token_hash,
            deep_link=f"https://t.me/{self.bot_username}?start={token}",
            expires_at=request.expires_at,
        )

    async def get(self, tenant_id: str, recipient_id: str) -> TelegramBindingView:
        binding, pending, destination = await self.repository.get_current(
            tenant_id,
            recipient_id,
        )
        now = self.clock().astimezone(UTC).replace(microsecond=0)
        return telegram_binding_view(
            enabled=True,
            binding=binding,
            pending=pending,
            destination=destination,
            now=now,
        )

    async def revoke(self, tenant_id: str, recipient_id: str) -> None:
        now = self.clock().astimezone(UTC).replace(microsecond=0)
        await self.repository.revoke(tenant_id, recipient_id, now)

    async def handle(self, command: BindingCommand) -> BindingCommandResult:
        now = self.clock().astimezone(UTC).replace(microsecond=0)
        if command.kind is BindingCommandKind.STOP:
            applied = await self.repository.stop(
                chat_id=command.chat_id,
                sender_id=command.sender_id,
                update_id=command.update_id,
                now=now,
            )
            return BindingCommandResult(applied=applied)

        token = command.token or ""
        token_hash = sha256(token.encode("ascii")).hexdigest()
        request = await self.repository.get_request(token_hash)
        if (
            request is None
            or request.status is not TelegramBindingStatus.PENDING
            or request.expires_at <= now
        ):
            return BindingCommandResult(applied=False)
        membership_active = await self.membership_lookup.is_active(
            request.recipient_id,
            request.tenant_id,
        )
        applied = await self.repository.consume(
            token_hash=token_hash,
            chat_id=command.chat_id,
            sender_id=command.sender_id,
            update_id=command.update_id,
            now=now,
            membership_active=membership_active,
        )
        return BindingCommandResult(applied=applied)
