from copy import deepcopy
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any, ClassVar

import pytest
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from limnopulse_api.adapters.telegram_bindings import DynamoTelegramBindingRepository
from limnopulse_api.domain.telegram import (
    TelegramBindingRequest,
    TelegramBindingStatus,
    TelegramDestination,
)

NOW = datetime(2026, 8, 13, 12, 0, tzinfo=UTC)
TOKEN_HASH = sha256(b"AQIDBAUGBwgJCgsMDQ4PEA").hexdigest()


class RecordingDynamoClient:
    def __init__(self) -> None:
        self.serializer = TypeSerializer()
        self.deserializer = TypeDeserializer()
        self.items: dict[tuple[str, str], dict[str, Any]] = {}
        self.get_item_calls: list[dict[str, Any]] = []
        self.transact_write_items_calls: list[dict[str, Any]] = []

    def seed(self, item: dict[str, Any]) -> None:
        self.items[(item["PK"], item["SK"])] = deepcopy(item)

    def get_item(self, **kwargs: Any) -> dict[str, Any]:
        self.get_item_calls.append(kwargs)
        key = self.decode(kwargs["Key"])
        item = self.items.get((key["PK"], key["SK"]))
        return {} if item is None else {"Item": self.encode(item)}

    def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
        self.transact_write_items_calls.append(kwargs)
        return {}

    def encode(self, item: dict[str, Any]) -> dict[str, Any]:
        return {key: self.serializer.serialize(value) for key, value in item.items()}

    def decode(self, item: dict[str, Any]) -> dict[str, Any]:
        return {key: self.deserializer.deserialize(value) for key, value in item.items()}


class TransactionConflict(Exception):
    response: ClassVar[dict[str, Any]] = {
        "Error": {"Code": "TransactionCanceledException"},
        "CancellationReasons": [{"Code": "ConditionalCheckFailed"}],
    }


class OperationalCancellation(Exception):
    response: ClassVar[dict[str, Any]] = {
        "Error": {"Code": "TransactionCanceledException"},
        "CancellationReasons": [{"Code": "ProvisionedThroughputExceeded"}],
    }


class DestinationConditionRace(Exception):
    response: ClassVar[dict[str, Any]] = {
        "Error": {"Code": "TransactionCanceledException"},
        "CancellationReasons": [
            {"Code": "None"},
            {"Code": "ConditionalCheckFailed"},
        ],
    }


class ConflictingDynamoClient(RecordingDynamoClient):
    def __init__(
        self,
        failures: int,
        error: type[Exception] = TransactionConflict,
    ) -> None:
        super().__init__()
        self.failures = failures
        self.error = error

    def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
        self.transact_write_items_calls.append(kwargs)
        if self.failures > 0:
            self.failures -= 1
            raise self.error
        return {}


def request() -> TelegramBindingRequest:
    return TelegramBindingRequest(
        request_id="request_1",
        tenant_id="tnt_1",
        recipient_id="sub_1",
        token_hash=TOKEN_HASH,
        status=TelegramBindingStatus.PENDING,
        expires_at=NOW + timedelta(minutes=10),
        created_at=NOW,
    )


def token_item() -> dict[str, Any]:
    return {
        "PK": f"TELEGRAM_BINDING_TOKEN#{TOKEN_HASH}",
        "SK": "META",
        "entity_type": "telegram_binding_request",
        **request().model_dump(mode="json"),
        "expires_at": int((NOW + timedelta(minutes=10)).timestamp()),
        "schema_version": 1,
    }


@pytest.mark.asyncio
async def test_issue_writes_hash_only_lookup_and_current_pointer_atomically() -> None:
    client = RecordingDynamoClient()
    repository = DynamoTelegramBindingRepository("domain", client)

    await repository.issue(request())

    call = client.transact_write_items_calls[0]
    assert call["ClientRequestToken"] == f"tgissue-{sha256(b'request_1').hexdigest()[:28]}"
    operations = call["TransactItems"]
    token = client.decode(operations[0]["Put"]["Item"])
    pointer = client.decode(operations[1]["Put"]["Item"])
    assert token["PK"] == f"TELEGRAM_BINDING_TOKEN#{TOKEN_HASH}"
    assert token["token_hash"] == TOKEN_HASH
    assert "token" not in token
    assert pointer["PK"] == "TENANT#tnt_1"
    assert pointer["SK"] == "TELEGRAM_BINDING_REQUEST#USER#sub_1"


@pytest.mark.asyncio
async def test_consume_fences_membership_token_destination_binding_and_update_dedupe() -> None:
    client = RecordingDynamoClient()
    client.seed(token_item())
    repository = DynamoTelegramBindingRepository("domain", client)

    applied = await repository.consume(
        token_hash=TOKEN_HASH,
        chat_id=123,
        sender_id=123,
        update_id=55,
        now=NOW,
        membership_active=True,
    )

    assert applied is True
    call = client.transact_write_items_calls[0]
    assert call["ClientRequestToken"].startswith("tgupdate-")
    assert len(call["ClientRequestToken"]) <= 36
    operations = call["TransactItems"]
    assert len(operations) == 6
    dedupe = client.decode(operations[0]["Put"]["Item"])
    membership = client.decode(operations[1]["ConditionCheck"]["Key"])
    token_update = operations[2]["Update"]
    pointer_update = operations[3]["Update"]
    destination_update = operations[4]["Update"]
    binding = client.decode(operations[5]["Put"]["Item"])
    assert dedupe["PK"] == "TELEGRAM_UPDATE#55"
    assert dedupe["expires_at"] == int((NOW + timedelta(days=8)).timestamp())
    assert membership == {"PK": "USER#sub_1", "SK": "TENANT#tnt_1"}
    assert "#expires_at > :now_epoch" in token_update["ConditionExpression"]
    assert client.decode(pointer_update["Key"]) == {
        "PK": "TENANT#tnt_1",
        "SK": "TELEGRAM_BINDING_REQUEST#USER#sub_1",
    }
    assert "#status = :pending" in pointer_update["ConditionExpression"]
    destination_key = client.decode(destination_update["Key"])
    assert destination_key == {
        "PK": f"TELEGRAM_DESTINATION#{TelegramDestination.id_for_chat(123)}",
        "SK": "META",
    }
    assert "recipient_id" in destination_update["ExpressionAttributeNames"].values()
    assert binding["PK"] == "TENANT#tnt_1"
    assert binding["SK"] == "TELEGRAM_BINDING#USER#sub_1"
    assert binding["destination_id"] == TelegramDestination.id_for_chat(123)


@pytest.mark.asyncio
async def test_consume_rejects_non_private_sender_without_writes() -> None:
    client = RecordingDynamoClient()
    client.seed(token_item())
    repository = DynamoTelegramBindingRepository("domain", client)

    applied = await repository.consume(
        token_hash=TOKEN_HASH,
        chat_id=123,
        sender_id=456,
        update_id=56,
        now=NOW,
        membership_active=True,
    )

    assert applied is False
    assert client.transact_write_items_calls == []


@pytest.mark.asyncio
async def test_consume_propagates_operational_transaction_cancellation() -> None:
    client = ConflictingDynamoClient(failures=1, error=OperationalCancellation)
    client.seed(token_item())
    repository = DynamoTelegramBindingRepository("domain", client)

    with pytest.raises(OperationalCancellation):
        await repository.consume(
            token_hash=TOKEN_HASH,
            chat_id=123,
            sender_id=123,
            update_id=56,
            now=NOW,
            membership_active=True,
        )


@pytest.mark.asyncio
async def test_stop_propagates_operational_transaction_cancellation() -> None:
    client = ConflictingDynamoClient(failures=1, error=OperationalCancellation)
    destination_id = TelegramDestination.id_for_chat(123)
    client.seed(
        {
            "PK": f"TELEGRAM_DESTINATION#{destination_id}",
            "SK": "META",
            "entity_type": "telegram_destination",
            "destination_id": destination_id,
            "recipient_id": "sub_1",
            "chat_id": 123,
            "status": "active",
            "version": 1,
            "created_at": NOW.isoformat(),
            "updated_at": NOW.isoformat(),
            "schema_version": 1,
        }
    )
    repository = DynamoTelegramBindingRepository("domain", client)

    with pytest.raises(OperationalCancellation):
        await repository.stop(chat_id=123, sender_id=123, update_id=59, now=NOW)


@pytest.mark.asyncio
async def test_stop_surfaces_repeated_destination_condition_race() -> None:
    client = ConflictingDynamoClient(failures=10, error=DestinationConditionRace)
    destination_id = TelegramDestination.id_for_chat(123)
    client.seed(
        {
            "PK": f"TELEGRAM_DESTINATION#{destination_id}",
            "SK": "META",
            "entity_type": "telegram_destination",
            "destination_id": destination_id,
            "recipient_id": "sub_1",
            "chat_id": 123,
            "status": "active",
            "version": 1,
            "created_at": NOW.isoformat(),
            "updated_at": NOW.isoformat(),
            "schema_version": 1,
        }
    )
    repository = DynamoTelegramBindingRepository("domain", client)

    with pytest.raises(DestinationConditionRace):
        await repository.stop(chat_id=123, sender_id=123, update_id=60, now=NOW)

    assert len(client.transact_write_items_calls) == 3


@pytest.mark.asyncio
async def test_stop_uses_hashed_key_and_never_revokes_bindings() -> None:
    client = RecordingDynamoClient()
    destination_id = TelegramDestination.id_for_chat(123)
    client.seed(
        {
            "PK": f"TELEGRAM_DESTINATION#{destination_id}",
            "SK": "META",
            "entity_type": "telegram_destination",
            "destination_id": destination_id,
            "recipient_id": "sub_1",
            "chat_id": 123,
            "status": "active",
            "suppression_reason": None,
            "stopped_at": None,
            "version": 1,
            "created_at": NOW.isoformat(),
            "updated_at": NOW.isoformat(),
            "schema_version": 1,
        }
    )
    repository = DynamoTelegramBindingRepository("domain", client)

    applied = await repository.stop(
        chat_id=123,
        sender_id=123,
        update_id=57,
        now=NOW,
    )

    assert applied is True
    call = client.transact_write_items_calls[0]
    assert call["ClientRequestToken"].startswith("tgupdate-")
    assert len(call["ClientRequestToken"]) <= 36
    operations = call["TransactItems"]
    assert len(operations) == 2
    update = operations[1]["Update"]
    assert client.decode(update["Key"])["PK"] == f"TELEGRAM_DESTINATION#{destination_id}"
    assert "TELEGRAM_BINDING" not in repr(operations)


@pytest.mark.asyncio
async def test_stop_without_destination_dedupes_update_and_fences_absence() -> None:
    client = RecordingDynamoClient()
    repository = DynamoTelegramBindingRepository("domain", client)

    applied = await repository.stop(
        chat_id=123,
        sender_id=123,
        update_id=58,
        now=NOW,
    )

    assert applied is False
    operations = client.transact_write_items_calls[0]["TransactItems"]
    assert len(operations) == 2
    assert client.decode(operations[0]["Put"]["Item"])["PK"] == "TELEGRAM_UPDATE#58"
    absence = operations[1]["ConditionCheck"]
    assert client.decode(absence["Key"])["PK"] == (
        f"TELEGRAM_DESTINATION#{TelegramDestination.id_for_chat(123)}"
    )
    assert absence["ConditionExpression"] == (
        "attribute_not_exists(PK) AND attribute_not_exists(SK)"
    )


@pytest.mark.asyncio
async def test_revoke_retries_transaction_conflict_instead_of_reporting_false_success() -> None:
    client = ConflictingDynamoClient(failures=1)
    destination_id = TelegramDestination.id_for_chat(123)
    client.seed(
        {
            "PK": "TENANT#tnt_1",
            "SK": "TELEGRAM_BINDING#USER#sub_1",
            "entity_type": "telegram_binding",
            "tenant_id": "tnt_1",
            "recipient_id": "sub_1",
            "destination_id": destination_id,
            "status": "verified",
            "verified_at": NOW.isoformat(),
            "revoked_at": None,
            "version": 1,
            "created_at": NOW.isoformat(),
            "updated_at": NOW.isoformat(),
            "schema_version": 1,
        }
    )
    repository = DynamoTelegramBindingRepository("domain", client)

    await repository.revoke("tnt_1", "sub_1", NOW)

    assert len(client.transact_write_items_calls) == 2


@pytest.mark.asyncio
async def test_revoke_surfaces_persistent_transaction_conflict() -> None:
    client = ConflictingDynamoClient(failures=10)
    destination_id = TelegramDestination.id_for_chat(123)
    client.seed(
        {
            "PK": "TENANT#tnt_1",
            "SK": "TELEGRAM_BINDING#USER#sub_1",
            "entity_type": "telegram_binding",
            "tenant_id": "tnt_1",
            "recipient_id": "sub_1",
            "destination_id": destination_id,
            "status": "verified",
            "verified_at": NOW.isoformat(),
            "revoked_at": None,
            "version": 1,
            "created_at": NOW.isoformat(),
            "updated_at": NOW.isoformat(),
            "schema_version": 1,
        }
    )
    repository = DynamoTelegramBindingRepository("domain", client)

    with pytest.raises(TransactionConflict):
        await repository.revoke("tnt_1", "sub_1", NOW)

    assert len(client.transact_write_items_calls) == 3


@pytest.mark.asyncio
async def test_active_destination_decodes_after_optional_suppression_fields_are_removed() -> None:
    client = RecordingDynamoClient()
    destination_id = TelegramDestination.id_for_chat(123)
    client.seed(
        {
            "PK": "TENANT#tnt_1",
            "SK": "TELEGRAM_BINDING#USER#sub_1",
            "entity_type": "telegram_binding",
            "tenant_id": "tnt_1",
            "recipient_id": "sub_1",
            "destination_id": destination_id,
            "status": "verified",
            "verified_at": NOW.isoformat(),
            "revoked_at": None,
            "version": 1,
            "created_at": NOW.isoformat(),
            "updated_at": NOW.isoformat(),
            "schema_version": 1,
        }
    )
    client.seed(
        {
            "PK": f"TELEGRAM_DESTINATION#{destination_id}",
            "SK": "META",
            "entity_type": "telegram_destination",
            "destination_id": destination_id,
            "recipient_id": "sub_1",
            "chat_id": 123,
            "status": "active",
            "version": 2,
            "created_at": NOW.isoformat(),
            "updated_at": NOW.isoformat(),
            "schema_version": 1,
        }
    )

    _, _, destination = await DynamoTelegramBindingRepository("domain", client).get_current(
        "tnt_1", "sub_1"
    )

    assert destination is not None
    assert destination.suppression_reason is None
    assert destination.stopped_at is None


def test_repository_never_exposes_scan_operation() -> None:
    client = RecordingDynamoClient()
    repository = DynamoTelegramBindingRepository("domain", client)

    assert not hasattr(repository, "scan")
    assert not hasattr(client, "scan")
