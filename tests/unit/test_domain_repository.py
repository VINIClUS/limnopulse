from datetime import datetime

import pytest

from limnopulse_api.adapters.dynamodb import DynamoKeyBuilder
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
