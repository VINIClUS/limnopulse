from datetime import UTC, datetime

import pytest

from limnopulse_api.adapters.dynamodb import DynamoDomainRepository, DynamoKeyBuilder
from limnopulse_api.domain.entities import Tenant


def test_key_builder_uses_limnopulse_domain_shapes() -> None:
    keys = DynamoKeyBuilder()

    assert keys.tenant("tnt_1") == {"PK": "TENANT#tnt_1", "SK": "META"}
    assert keys.pond("tnt_1", "pond_1") == {"PK": "TENANT#tnt_1", "SK": "POND#pond_1"}
    assert keys.device("tnt_1", "dev_1") == {"PK": "TENANT#tnt_1", "SK": "DEVICE#dev_1"}
    assert keys.device_lookup("dev_1") == {"PK": "DEVICE#dev_1", "SK": "META"}
    assert keys.membership("sub_1", "tnt_1") == {"PK": "USER#sub_1", "SK": "TENANT#tnt_1"}
    assert keys.tenant_member("tnt_1", "sub_1") == {"PK": "TENANT#tnt_1", "SK": "MEMBER#sub_1"}


def test_tenant_settings_cannot_be_mutated_in_place() -> None:
    tenant = Tenant(
        tenant_id="tnt_1",
        name="Tenant 1",
        settings={"timezone": "UTC"},
        created_at=datetime(2026, 1, 1),
        updated_at=datetime(2026, 1, 1),
        version=1,
    )

    with pytest.raises(TypeError):
        tenant.settings["timezone"] = "America/Sao_Paulo"


class RecordingDynamoClient:
    def __init__(self) -> None:
        self.transact_write_items_calls: list[dict] = []
        self.scan_calls = 0

    def transact_write_items(self, **kwargs):
        self.transact_write_items_calls.append(kwargs)
        return {}

    def scan(self, **kwargs):
        self.scan_calls += 1
        return {}


@pytest.mark.asyncio
async def test_create_tenant_with_owner_uses_transaction() -> None:
    client = RecordingDynamoClient()
    repo = DynamoDomainRepository(table_name="LimnopulseDomain", client=client)

    tenant = await repo.create_tenant_with_owner("tnt_1", "Demo", "sub_1")

    assert tenant.tenant_id == "tnt_1"
    assert tenant.name == "Demo"
    assert tenant.settings == {}
    assert tenant.status == "active"
    assert tenant.created_at.tzinfo == UTC
    assert len(client.transact_write_items_calls) == 1
    items = client.transact_write_items_calls[0]["TransactItems"]
    assert len(items) == 3
    assert all("ConditionExpression" in item["Put"] for item in items)
    assert client.scan_calls == 0
