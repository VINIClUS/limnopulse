from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest

from limnopulse_api.domain.telegram import (
    TelegramBinding,
    TelegramBindingRequest,
    TelegramBindingStatus,
    TelegramDestination,
    TelegramDestinationStatus,
)
from limnopulse_api.services.telegram_bindings import (
    BindingCommand,
    BindingCommandKind,
    TelegramBindingService,
)

NOW = datetime(2026, 8, 13, 12, 0, tzinfo=UTC)


class FakeMembershipLookup:
    def __init__(self, active: bool = True) -> None:
        self.active = active
        self.calls: list[tuple[str, str]] = []

    async def is_active(self, recipient_id: str, tenant_id: str) -> bool:
        self.calls.append((recipient_id, tenant_id))
        return self.active


class FakeBindingRepository:
    def __init__(self) -> None:
        self.requests: dict[str, TelegramBindingRequest] = {}
        self.current: dict[tuple[str, str], TelegramBinding] = {}
        self.destinations: dict[str, TelegramDestination] = {}
        self.seen_updates: set[int] = set()

    async def issue(self, request: TelegramBindingRequest) -> None:
        for token_hash, existing in list(self.requests.items()):
            if (
                existing.tenant_id == request.tenant_id
                and existing.recipient_id == request.recipient_id
                and existing.status is TelegramBindingStatus.PENDING
            ):
                self.requests[token_hash] = existing.model_copy(
                    update={"status": TelegramBindingStatus.INVALIDATED}
                )
        self.requests[request.token_hash] = request

    async def get_request(self, token_hash: str) -> TelegramBindingRequest | None:
        return self.requests.get(token_hash)

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
        if update_id in self.seen_updates:
            return False
        request = self.requests.get(token_hash)
        if (
            request is None
            or request.status is not TelegramBindingStatus.PENDING
            or request.expires_at <= now
            or not membership_active
            or sender_id != chat_id
        ):
            return False
        destination_id = TelegramDestination.id_for_chat(chat_id)
        destination = self.destinations.get(destination_id)
        if destination is not None and destination.recipient_id != request.recipient_id:
            return False
        if destination is None:
            destination = TelegramDestination(
                destination_id=destination_id,
                recipient_id=request.recipient_id,
                chat_id=chat_id,
                status=TelegramDestinationStatus.ACTIVE,
                suppression_reason=None,
                stopped_at=None,
                version=1,
                created_at=now,
                updated_at=now,
            )
        elif destination.status is TelegramDestinationStatus.SUPPRESSED:
            destination = destination.model_copy(
                update={
                    "status": TelegramDestinationStatus.ACTIVE,
                    "suppression_reason": None,
                    "stopped_at": None,
                    "version": destination.version + 1,
                    "updated_at": now,
                }
            )
        self.destinations[destination_id] = destination
        key = (request.tenant_id, request.recipient_id)
        previous = self.current.get(key)
        version = 1 if previous is None else previous.version + 1
        self.current[key] = TelegramBinding(
            tenant_id=request.tenant_id,
            recipient_id=request.recipient_id,
            destination_id=destination_id,
            status=TelegramBindingStatus.VERIFIED,
            verified_at=now,
            revoked_at=None,
            version=version,
            created_at=previous.created_at if previous is not None else now,
            updated_at=now,
        )
        self.requests[token_hash] = request.model_copy(
            update={"status": TelegramBindingStatus.CONSUMED, "consumed_at": now}
        )
        self.seen_updates.add(update_id)
        return True

    async def stop(
        self,
        *,
        chat_id: int,
        sender_id: int,
        update_id: int,
        now: datetime,
    ) -> bool:
        if update_id in self.seen_updates or sender_id != chat_id:
            return False
        destination_id = TelegramDestination.id_for_chat(chat_id)
        destination = self.destinations.get(destination_id)
        if destination is None:
            return False
        if destination.status is TelegramDestinationStatus.ACTIVE:
            self.destinations[destination_id] = destination.model_copy(
                update={
                    "status": TelegramDestinationStatus.SUPPRESSED,
                    "suppression_reason": "user_stop",
                    "stopped_at": now,
                    "version": destination.version + 1,
                    "updated_at": now,
                }
            )
        self.seen_updates.add(update_id)
        return True

    async def get_current(self, tenant_id: str, recipient_id: str):
        binding = self.current.get((tenant_id, recipient_id))
        pending = next(
            (
                request
                for request in self.requests.values()
                if request.tenant_id == tenant_id
                and request.recipient_id == recipient_id
                and request.status is TelegramBindingStatus.PENDING
            ),
            None,
        )
        destination = self.destinations.get(binding.destination_id) if binding is not None else None
        return binding, pending, destination

    async def revoke(self, tenant_id: str, recipient_id: str, now: datetime) -> None:
        key = (tenant_id, recipient_id)
        binding = self.current.get(key)
        if binding is not None and binding.status is TelegramBindingStatus.VERIFIED:
            self.current[key] = binding.model_copy(
                update={
                    "status": TelegramBindingStatus.REVOKED,
                    "revoked_at": now,
                    "version": binding.version + 1,
                    "updated_at": now,
                }
            )
        for token_hash, request in list(self.requests.items()):
            if (
                request.tenant_id == tenant_id
                and request.recipient_id == recipient_id
                and request.status is TelegramBindingStatus.PENDING
            ):
                self.requests[token_hash] = request.model_copy(
                    update={"status": TelegramBindingStatus.INVALIDATED}
                )


def token_values() -> Iterator[str]:
    yield "AQIDBAUGBwgJCgsMDQ4PEA"
    yield "ERITFBUWFxgZGhscHR4fIA"


@pytest.mark.asyncio
async def test_new_request_invalidates_only_previous_pending_request() -> None:
    repository = FakeBindingRepository()
    values = token_values()
    service = TelegramBindingService(
        repository,
        FakeMembershipLookup(),
        clock=lambda: NOW,
        token_factory=lambda: next(values),
        bot_username="limnopulse_test_bot",
    )

    first = await service.issue("tnt_1", "sub_1")
    second = await service.issue("tnt_1", "sub_1")

    assert first.token == "AQIDBAUGBwgJCgsMDQ4PEA"
    assert second.deep_link == "https://t.me/limnopulse_test_bot?start=ERITFBUWFxgZGhscHR4fIA"
    assert second.expires_at == NOW + timedelta(minutes=10)
    assert repository.requests[first.token_hash].status is TelegramBindingStatus.INVALIDATED
    assert repository.requests[second.token_hash].status is TelegramBindingStatus.PENDING


@pytest.mark.asyncio
async def test_start_consumes_token_once_and_reactivates_same_suppressed_chat() -> None:
    repository = FakeBindingRepository()
    membership = FakeMembershipLookup()
    service = TelegramBindingService(
        repository,
        membership,
        clock=lambda: NOW,
        token_factory=lambda: "AQIDBAUGBwgJCgsMDQ4PEA",
        bot_username="limnopulse_test_bot",
    )
    issued = await service.issue("tnt_1", "sub_1")
    destination_id = TelegramDestination.id_for_chat(123)
    repository.destinations[destination_id] = TelegramDestination(
        destination_id=destination_id,
        recipient_id="sub_1",
        chat_id=123,
        status=TelegramDestinationStatus.SUPPRESSED,
        suppression_reason="user_stop",
        stopped_at=NOW - timedelta(days=1),
        version=2,
        created_at=NOW - timedelta(days=2),
        updated_at=NOW - timedelta(days=1),
    )

    first = await service.handle(
        BindingCommand(
            kind=BindingCommandKind.START,
            update_id=101,
            chat_id=123,
            sender_id=123,
            token=issued.token,
        )
    )
    duplicate = await service.handle(
        BindingCommand(
            kind=BindingCommandKind.START,
            update_id=101,
            chat_id=123,
            sender_id=123,
            token=issued.token,
        )
    )

    assert first.applied is True
    assert duplicate.applied is False
    assert membership.calls == [("sub_1", "tnt_1")]
    assert repository.current[("tnt_1", "sub_1")].status is TelegramBindingStatus.VERIFIED
    assert repository.destinations[destination_id].status is TelegramDestinationStatus.ACTIVE


@pytest.mark.asyncio
async def test_stop_suppresses_destination_without_revoking_tenant_bindings() -> None:
    repository = FakeBindingRepository()
    service = TelegramBindingService(
        repository,
        FakeMembershipLookup(),
        clock=lambda: NOW,
        token_factory=lambda: "AQIDBAUGBwgJCgsMDQ4PEA",
        bot_username="limnopulse_test_bot",
    )
    destination_id = TelegramDestination.id_for_chat(123)
    repository.destinations[destination_id] = TelegramDestination(
        destination_id=destination_id,
        recipient_id="sub_1",
        chat_id=123,
        status=TelegramDestinationStatus.ACTIVE,
        suppression_reason=None,
        stopped_at=None,
        version=1,
        created_at=NOW,
        updated_at=NOW,
    )
    repository.current[("tnt_1", "sub_1")] = TelegramBinding(
        tenant_id="tnt_1",
        recipient_id="sub_1",
        destination_id=destination_id,
        status=TelegramBindingStatus.VERIFIED,
        verified_at=NOW,
        revoked_at=None,
        version=1,
        created_at=NOW,
        updated_at=NOW,
    )

    result = await service.handle(
        BindingCommand(
            kind=BindingCommandKind.STOP,
            update_id=102,
            chat_id=123,
            sender_id=123,
        )
    )

    assert result.applied is True
    assert repository.destinations[destination_id].status is TelegramDestinationStatus.SUPPRESSED
    assert repository.current[("tnt_1", "sub_1")].status is TelegramBindingStatus.VERIFIED


@pytest.mark.asyncio
async def test_start_rejects_expired_token_and_cross_recipient_chat_claim() -> None:
    repository = FakeBindingRepository()
    service = TelegramBindingService(
        repository,
        FakeMembershipLookup(),
        clock=lambda: NOW,
        token_factory=lambda: "AQIDBAUGBwgJCgsMDQ4PEA",
        bot_username="limnopulse_test_bot",
    )
    issued = await service.issue("tnt_1", "sub_1")
    request = repository.requests[issued.token_hash]
    repository.requests[issued.token_hash] = request.model_copy(
        update={"expires_at": NOW - timedelta(seconds=1)}
    )

    expired = await service.handle(
        BindingCommand(
            kind=BindingCommandKind.START,
            update_id=103,
            chat_id=123,
            sender_id=123,
            token=issued.token,
        )
    )
    assert expired.applied is False

    repository.requests[issued.token_hash] = request
    destination_id = TelegramDestination.id_for_chat(123)
    repository.destinations[destination_id] = TelegramDestination(
        destination_id=destination_id,
        recipient_id="sub_other",
        chat_id=123,
        status=TelegramDestinationStatus.ACTIVE,
        suppression_reason=None,
        stopped_at=None,
        version=1,
        created_at=NOW,
        updated_at=NOW,
    )
    claimed = await service.handle(
        BindingCommand(
            kind=BindingCommandKind.START,
            update_id=104,
            chat_id=123,
            sender_id=123,
            token=issued.token,
        )
    )
    assert claimed.applied is False


@pytest.mark.asyncio
async def test_get_exposes_no_chat_or_token_and_revoke_preserves_destination() -> None:
    repository = FakeBindingRepository()
    service = TelegramBindingService(
        repository,
        FakeMembershipLookup(),
        clock=lambda: NOW,
        token_factory=lambda: "AQIDBAUGBwgJCgsMDQ4PEA",
        bot_username="limnopulse_test_bot",
    )
    issued = await service.issue("tnt_1", "sub_1")
    await service.handle(
        BindingCommand(
            kind=BindingCommandKind.START,
            update_id=105,
            chat_id=123,
            sender_id=123,
            token=issued.token,
        )
    )
    await service.issue("tnt_1", "sub_1")

    view = await service.get("tnt_1", "sub_1")
    await service.revoke("tnt_1", "sub_1")
    revoked = await service.get("tnt_1", "sub_1")

    assert view.status == "verified"
    assert view.pending_request_id is not None
    assert "chat" not in view.model_dump(mode="json")
    assert "token" not in view.model_dump(mode="json")
    assert revoked.status == "revoked"
    assert revoked.pending_request_id is None
    assert repository.destinations[TelegramDestination.id_for_chat(123)].status is (
        TelegramDestinationStatus.ACTIVE
    )


@pytest.mark.asyncio
async def test_verified_binding_without_destination_is_not_effectively_enabled() -> None:
    repository = FakeBindingRepository()
    repository.current[("tnt_1", "sub_1")] = TelegramBinding(
        tenant_id="tnt_1",
        recipient_id="sub_1",
        destination_id=TelegramDestination.id_for_chat(123),
        status=TelegramBindingStatus.VERIFIED,
        verified_at=NOW,
        revoked_at=None,
        version=1,
        created_at=NOW,
        updated_at=NOW,
    )
    service = TelegramBindingService(
        repository,
        FakeMembershipLookup(),
        clock=lambda: NOW,
        bot_username="limnopulse_test_bot",
    )

    view = await service.get("tnt_1", "sub_1")

    assert view.status == "verified"
    assert view.effective_enabled is False
