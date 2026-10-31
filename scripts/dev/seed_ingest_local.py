"""Seed a local HTTPS ingest connector for `local-device-001` (V4 P3, local development only).

Run after `init_dynamodb.py`, `seed_local.py` and the M1 backfill
(`python -m scripts.migrations.m1_backfill_default_site_assets --tenant tnt_local_001 --apply`):

    python scripts/dev/seed_ingest_local.py

Safe to re-run: it reuses the seeded `https_direct` IntegrationAccount (by display name) and the
device's DeviceIntegration under it, creating either through the M1 service if missing, moves both
to `active` through the P3 lifecycle repository (one D23 audit item each), then issues a new ingest
credential. An account holds at most two active credentials, so a re-run first revokes the oldest
one when both slots are taken. The token is printed to stdout exactly once; status lines go to
stderr and carry ids and the fingerprint only, never the token or its secret. Nothing is written to
the repository.

Refuses to run outside APP_ENV=local/test or without DYNAMODB_ENDPOINT_URL (DynamoDB Local).
"""

import asyncio
import json
import sys

import boto3

from limnopulse_api.adapters.dynamodb_devices_v2 import DynamoDeviceComponentRepository
from limnopulse_api.adapters.dynamodb_ingest_credentials import DynamoIngestCredentialRepository
from limnopulse_api.adapters.dynamodb_integrations import DynamoIntegrationRepository
from limnopulse_api.adapters.dynamodb_v2 import utc_now
from limnopulse_api.api.v2.schemas.integrations import (
    DeviceIntegrationCreate,
    IntegrationAccountCreate,
)
from limnopulse_api.core.config import Settings
from limnopulse_api.domain.alerts import AuditContext
from limnopulse_api.domain.devices_v2 import DeviceStatus
from limnopulse_api.domain.ingestion import IngestCredentialStatus
from limnopulse_api.domain.integrations import (
    DeviceIntegrationStatus,
    IntegrationAccountStatus,
)
from limnopulse_api.services.ingest_credentials import IngestCredentialService
from limnopulse_api.services.integrations import IntegrationService
from limnopulse_api.workers.telemetry_ingest import dynamodb_client_kwargs

TENANT_ID = "tnt_local_001"
DEVICE_ID = "local-device-001"
PROVIDER_KEY = "https_direct"
ACCOUNT_NAME = "Local HTTPS connector"
ACCOUNT_KEY = "seed-ingest-local-account-001"
INTEGRATION_KEY = "seed-ingest-local-integration-001"
AUDIT = AuditContext(actor_id="seed_ingest_local")
_MAX_ACTIVE_CREDENTIALS = 2  # the two D2 slots
_REUSABLE_ACCOUNT_STATUSES = {IntegrationAccountStatus.PENDING, IntegrationAccountStatus.ACTIVE}


def _status(message: str) -> None:
    print(message, file=sys.stderr)


async def seed(settings: Settings) -> str:
    # The worker's helper: dummy credentials only for a local endpoint in APP_ENV local/test.
    client = boto3.client("dynamodb", **dynamodb_client_kwargs(settings))
    table = settings.dynamodb_domain_table
    audit_table = settings.dynamodb_audit_table
    integrations = DynamoIntegrationRepository(table, client, audit_table_name=audit_table)
    devices = DynamoDeviceComponentRepository(table, client)
    service = IntegrationService(integrations, devices)

    device = await devices.get_device(TENANT_ID, DEVICE_ID, consistent=True)
    if device is None or device.status is not DeviceStatus.ACTIVE:
        raise SystemExit(
            "the local device has no active v2 Device; run seed_local.py and the M1 backfill first"
        )

    accounts = [
        account
        for account in await integrations.list_integration_accounts(TENANT_ID)
        if account.provider_key == PROVIDER_KEY and account.display_name == ACCOUNT_NAME
    ]
    unusable = [item for item in accounts if item.status not in _REUSABLE_ACCOUNT_STATUSES]
    if accounts and len(unusable) == len(accounts):
        raise SystemExit(
            "the seeded integration account is no longer pending or active; reset DynamoDB Local "
            "(init_dynamodb.py) and seed again"
        )
    accounts = [item for item in accounts if item.status in _REUSABLE_ACCOUNT_STATUSES]
    if accounts:
        account = accounts[0]
    else:
        created = await service.create_integration_account(
            TENANT_ID,
            IntegrationAccountCreate(provider_key=PROVIDER_KEY, display_name=ACCOUNT_NAME),
            ACCOUNT_KEY,
        )
        account = await integrations.get_integration_account(
            TENANT_ID, json.loads(created.snapshot_json)["integration_account_id"]
        )
        assert account is not None
    if account.status is not IntegrationAccountStatus.ACTIVE:
        account = await integrations.transition_integration_account(
            TENANT_ID,
            account.integration_account_id,
            IntegrationAccountStatus.ACTIVE,
            expected_version=account.version,
            updated_at=utc_now(),
            audit=AUDIT,
        )
    _status(f"integration account {account.integration_account_id} is active")

    existing = [
        item
        for item in await integrations.list_device_integrations(TENANT_ID, DEVICE_ID)
        if item.integration_account_id == account.integration_account_id
    ]
    integration = next(
        (i for i in existing if i.status is not DeviceIntegrationStatus.DECOMMISSIONED), None
    )
    if integration is None and existing:
        raise SystemExit(
            "the seeded device integration is decommissioned (its identity stays claimed, D22); "
            "reset DynamoDB Local (init_dynamodb.py) and seed again"
        )
    if integration is None:
        await service.create_device_integration(
            TENANT_ID,
            DEVICE_ID,
            DeviceIntegrationCreate(
                integration_account_id=account.integration_account_id,
                external_identity=DEVICE_ID,
            ),
            f"{INTEGRATION_KEY}-{account.integration_account_id}",
        )
        integration = next(
            item
            for item in await integrations.list_device_integrations(TENANT_ID, DEVICE_ID)
            if item.integration_account_id == account.integration_account_id
        )
    if integration.status is not DeviceIntegrationStatus.ACTIVE:
        integration = await integrations.transition_device_integration(
            TENANT_ID,
            integration.device_integration_id,
            DeviceIntegrationStatus.ACTIVE,
            expected_version=integration.version,
            updated_at=utc_now(),
            audit=AUDIT,
        )
    _status(f"device integration {integration.device_integration_id} is active")

    credential_repository = DynamoIngestCredentialRepository(
        table, client, audit_table_name=audit_table
    )
    credentials = IngestCredentialService(
        credentials=credential_repository, integrations=integrations
    )
    active = sorted(
        (
            item
            for item in await credentials.list(TENANT_ID, account.integration_account_id)
            if item.status is IngestCredentialStatus.ACTIVE
        ),
        key=lambda item: item.created_at,
    )
    if len(active) >= _MAX_ACTIVE_CREDENTIALS:
        oldest = active[0]
        await credentials.revoke(
            TENANT_ID,
            account.integration_account_id,
            oldest.ingest_credential_id,
            expected_version=oldest.version,
            audit=AUDIT,
        )
        _status(f"revoked the oldest ingest credential {oldest.ingest_credential_id}")
    issued = await credentials.issue(TENANT_ID, account.integration_account_id, audit=AUDIT)
    _status(
        f"issued ingest credential {issued.credential.ingest_credential_id} "
        f"(fingerprint {issued.credential.fingerprint}); the token follows on stdout, once"
    )
    return issued.token


def main() -> int:
    settings = Settings()
    if settings.app_env not in {"local", "test"} or not settings.dynamodb_endpoint_url:
        _status("seed_ingest_local.py runs only against DynamoDB Local (APP_ENV=local/test)")
        return 1
    token = asyncio.run(seed(settings))
    print(token)
    return 0


if __name__ == "__main__":
    sys.exit(main())
