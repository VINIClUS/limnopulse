from __future__ import annotations

import boto3

from limnopulse_api.core.config import get_settings


def ensure_table(client: boto3.client, table_name: str) -> None:
    existing_tables = set(client.list_tables()["TableNames"])
    if table_name in existing_tables:
        print(f"Table {table_name} already exists")
        return

    client.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    client.get_waiter("table_exists").wait(TableName=table_name)
    print(f"Created table {table_name}")


def main() -> None:
    settings = get_settings()
    client = boto3.client(
        "dynamodb",
        region_name=settings.aws_region,
        endpoint_url=settings.dynamodb_endpoint_url,
        aws_access_key_id="local",
        aws_secret_access_key="local",
    )
    ensure_table(client, settings.dynamodb_domain_table)
    ensure_table(client, settings.dynamodb_audit_table)


if __name__ == "__main__":
    main()
