from __future__ import annotations

from pydantic import ValidationError

from limnopulse_api.domain.entities import Membership
from limnopulse_api.repositories.cache import CacheRepository
from limnopulse_api.repositories.domain import DomainRepository


class MembershipService:
    def __init__(
        self,
        domain_repository: DomainRepository,
        cache: CacheRepository | None,
        membership_ttl_seconds: int,
    ) -> None:
        self.domain_repository = domain_repository
        self.cache = cache
        self.membership_ttl_seconds = membership_ttl_seconds

    async def get_active_membership(self, cognito_sub: str, tenant_id: str) -> Membership | None:
        cache_key = self._cache_key(cognito_sub)
        if self.cache is not None:
            try:
                cached = await self.cache.get_json(cache_key)
            except Exception:
                cached = None
            else:
                membership = self._membership_from_cached(cached, tenant_id)
                if membership is not None:
                    return membership

        membership = await self.domain_repository.get_membership(cognito_sub, tenant_id)
        if membership is None or membership.status != "active":
            return None

        if self.cache is not None:
            try:
                await self.cache.set_json(
                    cache_key,
                    [membership.model_dump(mode="json")],
                    self.membership_ttl_seconds,
                )
            except Exception:
                pass
        return membership

    def _membership_from_cached(self, cached: object | None, tenant_id: str) -> Membership | None:
        if not isinstance(cached, list):
            return None

        for item in cached:
            if not isinstance(item, dict):
                continue
            if item.get("tenant_id") != tenant_id or item.get("status") != "active":
                continue
            try:
                return Membership.model_validate(item)
            except ValidationError:
                return None
        return None

    def _cache_key(self, cognito_sub: str) -> str:
        return f"user:{cognito_sub}:memberships"
