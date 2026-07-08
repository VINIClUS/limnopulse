from limnopulse_api.adapters.dynamodb import DynamoKeyBuilder


def test_key_builder_uses_limnopulse_domain_shapes() -> None:
    keys = DynamoKeyBuilder()

    assert keys.tenant("tnt_1") == {"PK": "TENANT#tnt_1", "SK": "META"}
    assert keys.pond("tnt_1", "pond_1") == {"PK": "TENANT#tnt_1", "SK": "POND#pond_1"}
    assert keys.device("tnt_1", "dev_1") == {"PK": "TENANT#tnt_1", "SK": "DEVICE#dev_1"}
    assert keys.device_lookup("dev_1") == {"PK": "DEVICE#dev_1", "SK": "META"}
    assert keys.membership("sub_1", "tnt_1") == {"PK": "USER#sub_1", "SK": "TENANT#tnt_1"}
    assert keys.tenant_member("tnt_1", "sub_1") == {"PK": "TENANT#tnt_1", "SK": "MEMBER#sub_1"}
