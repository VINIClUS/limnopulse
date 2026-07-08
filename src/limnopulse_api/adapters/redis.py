from __future__ import annotations

import json
from typing import Any


class RedisCacheRepository:
    def __init__(self, redis_client: Any) -> None:
        self.redis = redis_client

    async def get_json(self, key: str) -> object | None:
        raw = await self.redis.get(key)
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return json.loads(raw)

    async def set_json(self, key: str, value: object, ttl_seconds: int) -> None:
        await self.redis.set(key, json.dumps(value, default=str), ex=ttl_seconds)

    async def delete(self, key: str) -> None:
        await self.redis.delete(key)
