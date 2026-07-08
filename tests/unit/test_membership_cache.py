from datetime import UTC, datetime

import pytest

from limnopulse_api.domain.entities import Membership
from limnopulse_api.domain.roles import TenantRole
from limnopulse_api.services.memberships import MembershipService


class FakeCache:
    def __init__(self) -> None:
        self.values: dict[str, object] = {}
        self.set_calls: list[tuple[str, int]] = []
        self.raise_on_get = False
        self.raise_on_set = False

    async def get_json(self, key: str) -> object | None:
        if self.raise_on_get:
            raise RuntimeError("cache get failed")
        return self.values.get(key)

    async def set_json(self, key: str, value: object, ttl_seconds: int) -> None:
        if self.raise_on_set:
            raise RuntimeError("cache set failed")
        self.values[key] = value
        self.set_calls.append((key, ttl_seconds))

    async def delete(self, key: str) -> None:
        self.values.pop(key, None)


class FakeDomainRepository:
    def __init__(self, membership: Membership | None) -> None:
        self.membership = membership
        self.get_membership_calls = 0

    async def get_membership(self, cognito_sub: str, tenant_id: str) -> Membership | None:
        self.get_membership_calls += 1
        return self.membership


def active_membership() -> Membership:
    now = datetime.now(UTC)
    return Membership(
        tenant_id="tnt_1",
        cognito_sub="sub_1",
        role=TenantRole.OWNER,
        status="active",
        created_at=now,
        updated_at=now,
        version=1,
    )


@pytest.mark.asyncio
async def test_membership_cache_hit_returns_validated_membership_without_dynamodb_read() -> None:
    repo = FakeDomainRepository(active_membership())
    cache = FakeCache()
    cached_membership = active_membership().model_dump(mode="json")
    cache.values["user:sub_1:memberships"] = [cached_membership]
    service = MembershipService(repo, cache, membership_ttl_seconds=120)

    result = await service.get_active_membership("sub_1", "tnt_1")

    assert result == Membership.model_validate(cached_membership)
    assert repo.get_membership_calls == 0
    assert cache.set_calls == []


@pytest.mark.asyncio
async def test_membership_cache_miss_reads_dynamodb_and_sets_short_ttl() -> None:
    repo = FakeDomainRepository(active_membership())
    cache = FakeCache()
    service = MembershipService(repo, cache, membership_ttl_seconds=120)

    result = await service.get_active_membership("sub_1", "tnt_1")

    assert result is not None
    assert repo.get_membership_calls == 1
    assert cache.set_calls == [("user:sub_1:memberships", 120)]


@pytest.mark.asyncio
async def test_invalid_cached_membership_falls_back_to_dynamodb() -> None:
    membership = active_membership()
    repo = FakeDomainRepository(membership)
    cache = FakeCache()
    cache.values["user:sub_1:memberships"] = [
        {
            "tenant_id": "tnt_1",
            "cognito_sub": "sub_1",
            "role": "owner",
            "status": "active",
            "created_at": "not-a-date",
            "updated_at": "2026-07-08T12:00:00+00:00",
            "version": 1,
            "schema_version": 1,
        }
    ]
    service = MembershipService(repo, cache, membership_ttl_seconds=120)

    result = await service.get_active_membership("sub_1", "tnt_1")

    assert result == membership
    assert repo.get_membership_calls == 1
    assert cache.set_calls == [("user:sub_1:memberships", 120)]


@pytest.mark.asyncio
async def test_cache_get_failure_falls_back_to_dynamodb() -> None:
    membership = active_membership()
    repo = FakeDomainRepository(membership)
    cache = FakeCache()
    cache.raise_on_get = True
    service = MembershipService(repo, cache, membership_ttl_seconds=120)

    result = await service.get_active_membership("sub_1", "tnt_1")

    assert result == membership
    assert repo.get_membership_calls == 1


@pytest.mark.asyncio
async def test_cache_set_failure_does_not_block_authorized_membership() -> None:
    membership = active_membership()
    repo = FakeDomainRepository(membership)
    cache = FakeCache()
    cache.raise_on_set = True
    service = MembershipService(repo, cache, membership_ttl_seconds=120)

    result = await service.get_active_membership("sub_1", "tnt_1")

    assert result == membership
    assert repo.get_membership_calls == 1


@pytest.mark.asyncio
async def test_inactive_membership_is_not_authorized() -> None:
    membership = active_membership().model_copy(update={"status": "disabled"})
    service = MembershipService(
        FakeDomainRepository(membership),
        FakeCache(),
        membership_ttl_seconds=120,
    )

    assert await service.get_active_membership("sub_1", "tnt_1") is None
