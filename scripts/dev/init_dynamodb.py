from __future__ import annotations

import boto3

from limnopulse_api.core.config import get_settings


def main() -> None:
    settings = get_settings()
    client = boto3.client(
        "dynamodb",
        region_name=settings.aws_region,
        endpoint_url=settings.dynamodb_endpoint_url,
        aws_access_key_id="local",
        aws_secret_access_key="local",
    )
    existing_tables = set(client.list_tables()["TableNames"])
    if settings.dynamodb_domain_table in existing_tables:
        print(f"Table {settings.dynamodb_domain_table} already exists")
        return

    client.create_table(
        TableName=settings.dynamodb_domain_table,
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
    client.get_waiter("table_exists").wait(TableName=settings.dynamodb_domain_table)
    print(f"Created table {settings.dynamodb_domain_table}")


if __name__ == "__main__":
    main()
