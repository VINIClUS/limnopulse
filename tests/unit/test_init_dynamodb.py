from typing import Any

from scripts.dev.init_dynamodb import ensure_table


class FakeWaiter:
    def __init__(self, client: "FakeDynamoClient") -> None:
        self.client = client

    def wait(self, **kwargs: Any) -> None:
        self.client.wait_calls.append(kwargs)


class FakeDynamoClient:
    def __init__(self, *, exists: bool, ttl_status: str = "DISABLED") -> None:
        self.exists = exists
        self.ttl_status = ttl_status
        self.create_calls: list[dict[str, Any]] = []
        self.wait_calls: list[dict[str, Any]] = []
        self.describe_ttl_calls: list[dict[str, Any]] = []
        self.update_ttl_calls: list[dict[str, Any]] = []

    def list_tables(self) -> dict[str, list[str]]:
        return {"TableNames": ["LimnopulseDomain"] if self.exists else []}

    def create_table(self, **kwargs: Any) -> None:
        self.create_calls.append(kwargs)

    def get_waiter(self, name: str) -> FakeWaiter:
        assert name == "table_exists"
        return FakeWaiter(self)

    def describe_time_to_live(self, **kwargs: Any) -> dict[str, Any]:
        self.describe_ttl_calls.append(kwargs)
        return {"TimeToLiveDescription": {"TimeToLiveStatus": self.ttl_status}}

    def update_time_to_live(self, **kwargs: Any) -> None:
        self.update_ttl_calls.append(kwargs)


def expected_ttl_call() -> dict[str, Any]:
    return {
        "TableName": "LimnopulseDomain",
        "TimeToLiveSpecification": {
            "Enabled": True,
            "AttributeName": "expires_at",
        },
    }


def test_existing_local_table_has_ttl_enabled() -> None:
    client = FakeDynamoClient(exists=True)

    ensure_table(client, "LimnopulseDomain")

    assert client.create_calls == []
    assert client.describe_ttl_calls == [{"TableName": "LimnopulseDomain"}]
    assert client.update_ttl_calls == [expected_ttl_call()]


def test_new_local_table_is_created_before_ttl_is_enabled() -> None:
    client = FakeDynamoClient(exists=False)

    ensure_table(client, "LimnopulseDomain")

    assert len(client.create_calls) == 1
    assert client.wait_calls == [{"TableName": "LimnopulseDomain"}]
    assert client.update_ttl_calls == [expected_ttl_call()]


def test_already_enabled_ttl_is_left_unchanged() -> None:
    client = FakeDynamoClient(exists=True, ttl_status="ENABLED")

    ensure_table(client, "LimnopulseDomain")

    assert client.describe_ttl_calls == [{"TableName": "LimnopulseDomain"}]
    assert client.update_ttl_calls == []
