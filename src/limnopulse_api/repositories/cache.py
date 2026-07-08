from typing import Protocol


class CacheRepository(Protocol):
    async def get_json(self, key: str) -> object | None:
        raise NotImplementedError

    async def set_json(self, key: str, value: object, ttl_seconds: int) -> None:
        raise NotImplementedError

    async def delete(self, key: str) -> None:
        raise NotImplementedError
