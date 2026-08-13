from datetime import datetime
from typing import Literal

from pydantic import BaseModel


class TelegramBindingResponse(BaseModel):
    status: Literal["absent", "pending", "verified", "suppressed", "revoked"]
    version: int | None
    verified_at: datetime | None
    pending_request_id: str | None
    pending_expires_at: datetime | None
    effective_enabled: bool


class TelegramBindingTokenResponse(BaseModel):
    request_id: str
    token: str
    deep_link: str
    expires_at: datetime
