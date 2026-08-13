from datetime import datetime
from typing import Protocol

from limnopulse_api.domain.telegram import (
    TelegramBinding,
    TelegramBindingRequest,
    TelegramDestination,
)


class TelegramBindingRepository(Protocol):
    async def issue(self, request: TelegramBindingRequest) -> None:
        raise NotImplementedError

    async def get_request(self, token_hash: str) -> TelegramBindingRequest | None:
        raise NotImplementedError

    async def consume(
        self,
        *,
        token_hash: str,
        chat_id: int,
        sender_id: int,
        update_id: int,
        now: datetime,
        membership_active: bool,
    ) -> bool:
        raise NotImplementedError

    async def stop(
        self,
        *,
        chat_id: int,
        sender_id: int,
        update_id: int,
        now: datetime,
    ) -> bool:
        raise NotImplementedError

    async def get_current(
        self,
        tenant_id: str,
        recipient_id: str,
    ) -> tuple[TelegramBinding | None, TelegramBindingRequest | None, TelegramDestination | None]:
        raise NotImplementedError

    async def revoke(self, tenant_id: str, recipient_id: str, now: datetime) -> None:
        raise NotImplementedError
