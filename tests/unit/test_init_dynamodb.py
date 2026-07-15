from typing import Any

import pytest

from scripts.dev import init_dynamodb
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
        self.update_table_calls: list[dict[str, Any]] = []

    def list_tables(self) -> dict[str, list[str]]:
        return {"TableNames": ["LimnopulseDomain"] if self.exists else []}

    def create_table(self, **kwargs: Any) -> None:
        self.create_calls.append(kwargs)

    def describe_table(self, **kwargs: Any) -> dict[str, Any]:
        return {"Table": {"GlobalSecondaryIndexes": []}}

    def update_table(self, **kwargs: Any) -> None:
        self.update_table_calls.append(kwargs)

    def get_waiter(self, name: str) -> FakeWaiter:
        assert name == "table_exists"
        return FakeWaiter(self)

    def describe_time_to_live(self, **kwargs: Any) -> dict[str, Any]:
        self.describe_ttl_calls.append(kwargs)
        return {"TimeToLiveDescription": {"TimeToLiveStatus": self.ttl_status}}

    def update_time_to_live(self, **kwargs: Any) -> None:
        self.update_ttl_calls.append(kwargs)


class IndexLifecycleClient(FakeDynamoClient):
    def __init__(
        self,
        *,
        exists: bool = True,
        initial_indexes: dict[str, str] | None = None,
        activate_created_indexes: bool = True,
    ) -> None:
        super().__init__(exists=exists)
        self.indexes = dict(initial_indexes or {})
        self.activate_created_indexes = activate_created_indexes
        self.index_describe_calls = 0

    def create_table(self, **kwargs: Any) -> None:
        super().create_table(**kwargs)
        for index in kwargs.get("GlobalSecondaryIndexes", []):
            self.indexes[index["IndexName"]] = "CREATING"

    def describe_table(self, **kwargs: Any) -> dict[str, Any]:
        self.index_describe_calls += 1
        indexes = [
            {"IndexName": name, "IndexStatus": status}
            for name, status in self.indexes.items()
        ]
        if self.activate_created_indexes:
            for name, status in tuple(self.indexes.items()):
                if status == "CREATING":
                    self.indexes[name] = "ACTIVE"
        return {"Table": {"GlobalSecondaryIndexes": indexes}}

    def update_table(self, **kwargs: Any) -> None:
        if any(status != "ACTIVE" for status in self.indexes.values()):
            raise AssertionError("attempted to create a GSI while another index was not ACTIVE")
        super().update_table(**kwargs)
        index_name = kwargs["GlobalSecondaryIndexUpdates"][0]["Create"]["IndexName"]
        self.indexes[index_name] = "CREATING"


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


def test_new_domain_table_includes_alert_evaluation_and_event_indexes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(init_dynamodb, "GSI_WAIT_DELAY_SECONDS", 0, raising=False)
    client = IndexLifecycleClient(exists=False)

    ensure_table(client, "LimnopulseDomain", include_alert_indexes=True)

    create = client.create_calls[0]
    assert {definition["AttributeName"] for definition in create["AttributeDefinitions"]} == {
        "PK",
        "SK",
        "GSI1PK",
        "GSI1SK",
        "GSI2PK",
        "GSI2SK",
    }
    assert [index["IndexName"] for index in create["GlobalSecondaryIndexes"]] == [
        "AlertEvaluationByDue",
        "AlertEventsByTenantTime",
    ]
    assert create["GlobalSecondaryIndexes"][0]["Projection"] == {
        "ProjectionType": "KEYS_ONLY"
    }
    assert create["GlobalSecondaryIndexes"][1]["Projection"] == {"ProjectionType": "ALL"}


def test_already_enabled_ttl_is_left_unchanged() -> None:
    client = FakeDynamoClient(exists=True, ttl_status="ENABLED")

    ensure_table(client, "LimnopulseDomain")

    assert client.describe_ttl_calls == [{"TableName": "LimnopulseDomain"}]
    assert client.update_ttl_calls == []


def test_existing_table_waits_for_each_index_before_creating_next(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(init_dynamodb, "GSI_WAIT_DELAY_SECONDS", 0, raising=False)
    client = IndexLifecycleClient()

    ensure_table(client, "LimnopulseDomain", include_alert_indexes=True)

    created = [
        call["GlobalSecondaryIndexUpdates"][0]["Create"]["IndexName"]
        for call in client.update_table_calls
    ]
    assert created == ["AlertEvaluationByDue", "AlertEventsByTenantTime"]
    assert client.indexes == {
        "AlertEvaluationByDue": "ACTIVE",
        "AlertEventsByTenantTime": "ACTIVE",
    }


def test_new_table_waits_for_all_indexes_before_returning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(init_dynamodb, "GSI_WAIT_DELAY_SECONDS", 0, raising=False)
    client = IndexLifecycleClient(exists=False)

    ensure_table(client, "LimnopulseDomain", include_alert_indexes=True)

    assert client.update_table_calls == []
    assert client.indexes == {
        "AlertEvaluationByDue": "ACTIVE",
        "AlertEventsByTenantTime": "ACTIVE",
    }
    assert client.index_describe_calls >= 3


def test_existing_creating_index_is_awaited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(init_dynamodb, "GSI_WAIT_DELAY_SECONDS", 0, raising=False)
    client = IndexLifecycleClient(
        initial_indexes={
            "AlertEvaluationByDue": "CREATING",
            "AlertEventsByTenantTime": "ACTIVE",
        }
    )

    ensure_table(client, "LimnopulseDomain", include_alert_indexes=True)

    assert client.update_table_calls == []
    assert client.indexes["AlertEvaluationByDue"] == "ACTIVE"
    assert client.index_describe_calls >= 2


def test_index_wait_times_out_with_index_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(init_dynamodb, "GSI_WAIT_DELAY_SECONDS", 0, raising=False)
    monkeypatch.setattr(init_dynamodb, "GSI_WAIT_MAX_ATTEMPTS", 2, raising=False)
    client = IndexLifecycleClient(activate_created_indexes=False)

    with pytest.raises(TimeoutError, match="AlertEvaluationByDue.*CREATING"):
        ensure_table(client, "LimnopulseDomain", include_alert_indexes=True)
