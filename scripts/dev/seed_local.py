from __future__ import annotations

import asyncio

import boto3
from botocore.exceptions import ClientError

from limnopulse_api.adapters.dynamodb import DynamoDomainRepository
from limnopulse_api.core.config import get_settings
from limnopulse_api.core.errors import ConflictError


async def seed() -> None:
    settings = get_settings()
    client = boto3.client(
        "dynamodb",
        region_name=settings.aws_region,
        endpoint_url=settings.dynamodb_endpoint_url,
        aws_access_key_id="local",
        aws_secret_access_key="local",
    )
    repository = DynamoDomainRepository(
        table_name=settings.dynamodb_domain_table,
        client=client,
    )

    try:
        tenant = await repository.create_tenant_with_owner(
            "tnt_local_001",
            "Local Tenant",
            "local-user-001",
        )
        print(f"Seeded tenant {tenant.tenant_id} with owner local-user-001")
    except ConflictError:
        print("Local tenant already exists")
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code == "ResourceNotFoundException":
            print(
                "Domain table not found. Run scripts/dev/init_dynamodb.py before seeding local data."
            )
            return
        raise

    try:
        pond = await repository.create_pond(
            "tnt_local_001",
            "pond_local_001",
            "Local Pond",
            "Local seeded pond for telemetry development.",
        )
        print(f"Seeded pond {pond.pond_id}")
    except ConflictError:
        print("Local pond already exists")

    try:
        device = await repository.create_device(
            "tnt_local_001",
            "pond_local_001",
            "local-device-001",
            "Local Device 001",
            "local-dev",
        )
        print(f"Seeded device {device.device_id}")
    except ConflictError:
        print("Local device already exists")


def main() -> None:
    asyncio.run(seed())


if __name__ == "__main__":
    main()
