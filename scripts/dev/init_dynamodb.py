from __future__ import annotations

import time
from typing import Any

import boto3

from limnopulse_api.core.config import get_settings


ALERT_INDEXES = (
    {
        "IndexName": "AlertEvaluationByDue",
        "KeySchema": [
            {"AttributeName": "GSI1PK", "KeyType": "HASH"},
            {"AttributeName": "GSI1SK", "KeyType": "RANGE"},
        ],
        "Projection": {"ProjectionType": "KEYS_ONLY"},
    },
    {
        "IndexName": "AlertEventsByTenantTime",
        "KeySchema": [
            {"AttributeName": "GSI2PK", "KeyType": "HASH"},
            {"AttributeName": "GSI2SK", "KeyType": "RANGE"},
        ],
        "Projection": {"ProjectionType": "ALL"},
    },
)
GSI_WAIT_DELAY_SECONDS = 20
GSI_WAIT_MAX_ATTEMPTS = 30


def ensure_table(
    client: boto3.client,
    table_name: str,
    *,
    include_alert_indexes: bool = False,
) -> None:
    existing_tables = set(client.list_tables()["TableNames"])
    if table_name in existing_tables:
        print(f"Table {table_name} already exists")
        if include_alert_indexes:
            ensure_alert_indexes(client, table_name)
    else:
        definitions = [
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
        ]
        if include_alert_indexes:
            definitions.extend(
                {"AttributeName": name, "AttributeType": "S"}
                for name in ("GSI1PK", "GSI1SK", "GSI2PK", "GSI2SK")
            )
        request = {
            "TableName": table_name,
            "KeySchema": [
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            "AttributeDefinitions": definitions,
            "BillingMode": "PAY_PER_REQUEST",
        }
        if include_alert_indexes:
            request["GlobalSecondaryIndexes"] = list(ALERT_INDEXES)
        client.create_table(**request)
        client.get_waiter("table_exists").wait(TableName=table_name)
        if include_alert_indexes:
            for index in ALERT_INDEXES:
                index_name = index["IndexName"]
                wait_for_index_active(client, table_name, index_name)
                print(f"Index {index_name} is ACTIVE on {table_name}")
        print(f"Created table {table_name}")

    ensure_ttl(client, table_name)


def ensure_alert_indexes(client: boto3.client, table_name: str) -> None:
    for index in ALERT_INDEXES:
        index_name = index["IndexName"]
        description = client.describe_table(TableName=table_name)["Table"]
        status = index_status(description, index_name)
        if status == "ACTIVE":
            continue
        if status is None:
            client.update_table(
                TableName=table_name,
                AttributeDefinitions=[
                    {"AttributeName": key["AttributeName"], "AttributeType": "S"}
                    for key in index["KeySchema"]
                ],
                GlobalSecondaryIndexUpdates=[{"Create": index}],
            )
        wait_for_index_active(client, table_name, index_name)
        print(f"Index {index_name} is ACTIVE on {table_name}")


def wait_for_index_active(
    client: boto3.client,
    table_name: str,
    index_name: str,
) -> None:
    last_status = "MISSING"
    for attempt in range(GSI_WAIT_MAX_ATTEMPTS):
        description = client.describe_table(TableName=table_name)["Table"]
        last_status = index_status(description, index_name) or "MISSING"
        if last_status == "ACTIVE":
            return
        if attempt + 1 < GSI_WAIT_MAX_ATTEMPTS:
            time.sleep(GSI_WAIT_DELAY_SECONDS)
    raise TimeoutError(
        f"Index {index_name} on {table_name} did not become ACTIVE; "
        f"last status was {last_status}"
    )


def index_status(description: dict[str, Any], index_name: str) -> str | None:
    return next(
        (
            str(index.get("IndexStatus", "UNKNOWN"))
            for index in description.get("GlobalSecondaryIndexes", [])
            if index["IndexName"] == index_name
        ),
        None,
    )


def ensure_ttl(client: boto3.client, table_name: str) -> None:
    response = client.describe_time_to_live(TableName=table_name)
    status = response.get("TimeToLiveDescription", {}).get("TimeToLiveStatus")
    if status in {"ENABLED", "ENABLING"}:
        print(f"TTL already enabled for {table_name}")
        return
    client.update_time_to_live(
        TableName=table_name,
        TimeToLiveSpecification={
            "Enabled": True,
            "AttributeName": "expires_at",
        },
    )
    print(f"Enabled TTL for {table_name}")


def main() -> None:
    settings = get_settings()
    client = boto3.client(
        "dynamodb",
        region_name=settings.aws_region,
        endpoint_url=settings.dynamodb_endpoint_url,
        aws_access_key_id="local",
        aws_secret_access_key="local",
    )
    ensure_table(client, settings.dynamodb_domain_table, include_alert_indexes=True)
    ensure_table(client, settings.dynamodb_audit_table)


if __name__ == "__main__":
    main()
