import json
from datetime import UTC, datetime

import pytest
from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from limnopulse_api.adapters.dynamodb_integrations import DynamoIntegrationRepository
from limnopulse_api.adapters.dynamodb_v2 import external_identity_hash
from limnopulse_api.api.v2.routers import integrations
from limnopulse_api.auth.dev import DevAuthProvider
from limnopulse_api.core.errors import ConflictError
from limnopulse_api.domain.devices_v2 import DeviceStatus, DeviceV2
from limnopulse_api.domain.entities import Membership
from limnopulse_api.domain.integrations import IntegrationAccount, IntegrationAccountStatus
from limnopulse_api.domain.roles import TenantRole

NOW = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)
TENANT = "tnt_a"
OTHER_TENANT = "tnt_b"
ACCOUNT_ID = "ia_" + "1" * 32
DEVICE_ID = "dev_" + "3" * 32
LEGACY_DEVICE_ID = "local-device-001"
RAW_IDENTITY = "serial-raw-SN-0042"
ACCOUNTS_PATH = f"/tenants/{TENANT}/integration-accounts"
INTEGRATIONS_PATH = f"/tenants/{TENANT}/devices/{DEVICE_ID}/integrations"
AUTH = {"X-Dev-User-Sub": "sub_1"}


class FakeMembershipService:
    def __init__(self, role: TenantRole) -> None:
        self.role = role

    async def get_active_membership(self, cognito_sub: str, tenant_id: str) -> Membership | None:
        if cognito_sub != "sub_1" or tenant_id != TENANT:
            return None
        return Membership(
            tenant_id=tenant_id,
            cognito_sub=cognito_sub,
            role=self.role,
            created_at=NOW,
            updated_at=NOW,
            version=1,
        )


class FakeDeviceReader:
    def __init__(self, *devices: DeviceV2) -> None:
        self.devices = {(device.tenant_id, device.device_id): device for device in devices}

    async def get_device(self, tenant_id: str, device_id: str) -> DeviceV2 | None:
        return self.devices.get((tenant_id, device_id))


class FakeIntegrationRepository:
    def __init__(self, *accounts: IntegrationAccount) -> None:
        self.accounts = {(a.tenant_id, a.integration_account_id): a for a in accounts}
        self.integrations = []
        self.receipts = {}
        self.claimed = set()
        self.stored: list[str] = []

    async def get_idempotency_replay(self, tenant_id, scope, key_hash, request_hash):
        receipt = self.receipts.get((tenant_id, scope, key_hash))
        if receipt is None:
            return None
        if receipt.request_hash != request_hash:
            raise ConflictError("idempotency key reused with a different request")
        return receipt.response_snapshot_json

    async def get_integration_account(self, tenant_id, integration_account_id):
        return self.accounts.get((tenant_id, integration_account_id))

    async def list_integration_accounts(self, tenant_id):
        return [a for (tenant, _), a in self.accounts.items() if tenant == tenant_id]

    async def create_integration_account(self, account, receipt):
        self.receipts[(receipt.tenant_id, receipt.scope, receipt.key_hash)] = receipt
        self.accounts[(account.tenant_id, account.integration_account_id)] = account
        self.stored += [account.model_dump_json(), receipt.model_dump_json()]
        return account

    async def list_device_integrations(self, tenant_id, device_id):
        return [
            i for i in self.integrations if i.tenant_id == tenant_id and i.device_id == device_id
        ]

    async def create_device_integration(self, integration, receipt):
        lookup = (integration.provider_key, integration.external_identity_hash)
        if lookup in self.claimed:
            raise ConflictError("external identity already claimed")
        self.claimed.add(lookup)
        self.receipts[(receipt.tenant_id, receipt.scope, receipt.key_hash)] = receipt
        self.integrations.append(integration)
        self.stored += [integration.model_dump_json(), receipt.model_dump_json()]
        return integration


class FailingDynamoClient:
    def __init__(self, error: Exception) -> None:
        self.error = error

    def get_item(self, **kwargs):
        raise self.error

    def query(self, **kwargs):
        raise self.error

    def transact_write_items(self, **kwargs):
        raise self.error


async def _handle_infrastructure_error(request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=503, content={"detail": "service unavailable"})


def device(**overrides) -> DeviceV2:
    values = {
        "tenant_id": TENANT,
        "device_id": DEVICE_ID,
        "name": "Probe hub",
        "created_at": NOW,
        "updated_at": NOW,
        "version": 1,
    }
    values.update(overrides)
    return DeviceV2(**values)


def account(**overrides) -> IntegrationAccount:
    values = {
        "tenant_id": TENANT,
        "integration_account_id": ACCOUNT_ID,
        "provider_key": "vendor.cloud",
        "display_name": "Vendor",
        "created_at": NOW,
        "updated_at": NOW,
        "version": 1,
    }
    values.update(overrides)
    return IntegrationAccount(**values)


def app_with_integration_repository(
    role: str = "owner",
    *,
    repository: object | None = None,
    device_reader: object | None = None,
    skip_state: tuple[str, ...] = (),
) -> FastAPI:
    app = FastAPI()
    app.include_router(integrations.router)
    app.add_exception_handler(BotoCoreError, _handle_infrastructure_error)
    app.add_exception_handler(ClientError, _handle_infrastructure_error)
    app.state.auth_provider = DevAuthProvider()
    app.state.membership_service = FakeMembershipService(TenantRole(role))
    state = {
        "integration_repository": repository
        if repository is not None
        else FakeIntegrationRepository(account()),
        "device_component_repository": device_reader
        if device_reader is not None
        else FakeDeviceReader(device(), device(device_id=LEGACY_DEVICE_ID)),
    }
    for name, value in state.items():
        if name not in skip_state:
            setattr(app.state, name, value)
    return app


def client_for(app: FastAPI) -> TestClient:
    return TestClient(app, headers=AUTH)


def post_integration(client: TestClient, path: str = INTEGRATIONS_PATH, **overrides):
    body = {"integration_account_id": ACCOUNT_ID, "external_identity": RAW_IDENTITY}
    headers = {"Idempotency-Key": overrides.pop("key", "device-integration-01")}
    body.update(overrides)
    return client.post(path, json=body, headers=headers)


def sha256_for_fixture(provider_key: str, identity: str) -> str:
    return external_identity_hash(provider_key, identity)


def test_router_is_unmounted_and_unprefixed() -> None:
    from limnopulse_api.main import create_app

    paths = {route.path for route in integrations.router.routes}
    assert paths == {
        "/tenants/{tenant_id}/integration-accounts",
        "/tenants/{tenant_id}/devices/{device_id}/integrations",
    }
    mounted = {getattr(route, "path", "") for route in create_app().routes}
    assert not any("integration" in path for path in mounted)


def test_device_integration_response_omits_external_identity() -> None:
    repository = FakeIntegrationRepository(account())
    app = app_with_integration_repository(role="owner", repository=repository)

    response = post_integration(client_for(app))

    assert response.status_code == 201
    body = response.json()
    assert "external_identity" not in body
    assert body["external_identity_hash"] == sha256_for_fixture("vendor.cloud", RAW_IDENTITY)
    assert body["status"] == "provisioning"
    assert body["provider_key"] == "vendor.cloud"
    assert body["device_id"] == DEVICE_ID
    assert body["device_integration_id"].startswith("di_")
    assert RAW_IDENTITY not in response.text
    assert all(RAW_IDENTITY not in stored for stored in repository.stored)


def test_viewer_cannot_create_integration_account() -> None:
    app = app_with_integration_repository(role="viewer")
    response = client_for(app).post(
        ACCOUNTS_PATH,
        json={"provider_key": "vendor.cloud", "display_name": "Vendor"},
        headers={"Idempotency-Key": "integration-account-01"},
    )
    assert response.status_code == 403


@pytest.mark.parametrize("role", ["viewer", "member"])
def test_non_writer_cannot_create_device_integration(role: str) -> None:
    response = post_integration(client_for(app_with_integration_repository(role=role)))
    assert response.status_code == 403


@pytest.mark.parametrize("role", ["owner", "admin", "member", "viewer"])
def test_every_member_role_can_list(role: str) -> None:
    client = client_for(app_with_integration_repository(role=role))
    assert client.get(ACCOUNTS_PATH).status_code == 200
    assert client.get(INTEGRATIONS_PATH).status_code == 200


def test_non_member_tenant_is_forbidden() -> None:
    client = client_for(app_with_integration_repository())
    assert client.get(f"/tenants/{OTHER_TENANT}/integration-accounts").status_code == 403


def test_create_and_list_integration_accounts() -> None:
    repository = FakeIntegrationRepository()
    client = client_for(app_with_integration_repository(role="admin", repository=repository))

    response = client.post(
        ACCOUNTS_PATH,
        json={
            "provider_key": "vendor.cloud",
            "display_name": "Vendor",
            "credential_reference": "secretsmanager:vendor/tnt_a",
            "public_identifier": "org-42",
        },
        headers={"Idempotency-Key": "integration-account-01"},
    )

    assert response.status_code == 201
    created = response.json()
    assert created["status"] == "pending"
    assert created["tenant_id"] == TENANT
    assert created["schema_version"] == 2
    assert created["version"] == 1
    assert created["integration_account_id"].startswith("ia_")
    listed = client.get(ACCOUNTS_PATH)
    assert listed.status_code == 200
    assert listed.json() == {"items": [created]}


def test_integration_account_create_replays_and_rejects_changed_body() -> None:
    client = client_for(app_with_integration_repository())
    body = {"provider_key": "vendor.cloud", "display_name": "Vendor"}
    headers = {"Idempotency-Key": "integration-account-01"}

    first = client.post(ACCOUNTS_PATH, json=body, headers=headers)
    replay = client.post(ACCOUNTS_PATH, json=body, headers=headers)
    changed = client.post(ACCOUNTS_PATH, json={**body, "display_name": "x"}, headers=headers)

    assert first.status_code == replay.status_code == 201
    assert replay.json() == first.json()
    assert changed.status_code == 409


@pytest.mark.parametrize("key", [None, "short", "k" * 129])
def test_create_requires_valid_idempotency_key(key: str | None) -> None:
    client = client_for(app_with_integration_repository())
    headers = {} if key is None else {"Idempotency-Key": key}
    response = client.post(
        ACCOUNTS_PATH, json={"provider_key": "vendor.cloud", "display_name": "V"}, headers=headers
    )
    assert response.status_code == 422


@pytest.mark.parametrize("field", ["api_key", "secret", "password", "token"])
def test_account_create_rejects_secret_material_without_echoing_it(field: str) -> None:
    client = client_for(app_with_integration_repository())
    response = client.post(
        ACCOUNTS_PATH,
        json={"provider_key": "vendor.cloud", "display_name": "V", field: "plaintext-secret"},
        headers={"Idempotency-Key": "integration-account-01"},
    )
    assert response.status_code == 422
    assert "plaintext-secret" not in response.text


def test_account_create_rejects_aws_enum_provider_key() -> None:
    client = client_for(app_with_integration_repository())
    response = client.post(
        ACCOUNTS_PATH,
        json={"provider_key": "AWS_IOT", "display_name": "V"},
        headers={"Idempotency-Key": "integration-account-01"},
    )
    assert response.status_code == 422


def test_device_integration_rejects_client_supplied_provider_without_echo() -> None:
    client = client_for(app_with_integration_repository())
    response = post_integration(client, provider_key="other.cloud")
    assert response.status_code == 422
    assert RAW_IDENTITY not in response.text


def test_oversized_external_identity_is_rejected_without_echo() -> None:
    oversized = "S" * 600
    response = post_integration(
        client_for(app_with_integration_repository()), external_identity=oversized
    )
    assert response.status_code == 422
    assert oversized not in response.text


@pytest.mark.parametrize("role", ["owner", "admin"])
def test_malformed_ids_are_rejected_with_422(role: str) -> None:
    client = client_for(app_with_integration_repository(role=role))
    bad_device = post_integration(client, f"/tenants/{TENANT}/devices/dev_123/integrations")
    bad_device_get = client.get(f"/tenants/{TENANT}/devices/dev_{'G' * 32}/integrations")
    bad_account = post_integration(client, integration_account_id="ia_123")
    assert bad_device.status_code == 422
    assert bad_device_get.status_code == 422
    assert bad_account.status_code == 422


def test_historical_device_id_path_is_accepted() -> None:
    repository = FakeIntegrationRepository(account())
    client = client_for(app_with_integration_repository(repository=repository))
    path = f"/tenants/{TENANT}/devices/{LEGACY_DEVICE_ID}/integrations"

    created = post_integration(client, path)
    listed = client.get(path)

    assert created.status_code == 201
    assert created.json()["device_id"] == LEGACY_DEVICE_ID
    assert listed.status_code == 200
    assert [item["device_id"] for item in listed.json()["items"]] == [LEGACY_DEVICE_ID]
    assert client.get(INTEGRATIONS_PATH).json() == {"items": []}


def test_device_integration_replay_and_body_mismatch() -> None:
    client = client_for(app_with_integration_repository())
    first = post_integration(client)
    replay = post_integration(client)
    mismatch = post_integration(client, external_identity="another-serial")

    assert first.status_code == replay.status_code == 201
    assert replay.json() == first.json()
    assert mismatch.status_code == 409
    assert "another-serial" not in mismatch.text


def test_external_identity_claimed_twice_is_conflict_without_raw_identity() -> None:
    client = client_for(app_with_integration_repository())
    assert post_integration(client).status_code == 201

    second = post_integration(client, key="device-integration-02")

    assert second.status_code == 409
    assert second.json() == {"detail": "external identity already claimed"}


@pytest.mark.parametrize(
    "status", [IntegrationAccountStatus.ERROR, IntegrationAccountStatus.REVOKED]
)
def test_error_or_revoked_account_rejects_new_integration(status) -> None:
    repository = FakeIntegrationRepository(account(status=status))
    response = post_integration(client_for(app_with_integration_repository(repository=repository)))
    assert response.status_code == 409


def test_active_account_permits_provisioning() -> None:
    repository = FakeIntegrationRepository(account(status=IntegrationAccountStatus.ACTIVE))
    response = post_integration(client_for(app_with_integration_repository(repository=repository)))
    assert response.status_code == 201
    assert response.json()["status"] == "provisioning"


def test_missing_or_cross_tenant_parents_are_not_found() -> None:
    other_account = "ia_" + "9" * 32
    repository = FakeIntegrationRepository(
        account(), account(tenant_id=OTHER_TENANT, integration_account_id=other_account)
    )
    reader = FakeDeviceReader(device(), device(tenant_id=OTHER_TENANT, device_id="dev_" + "4" * 32))
    client = client_for(
        app_with_integration_repository(repository=repository, device_reader=reader)
    )
    foreign_device_path = f"/tenants/{TENANT}/devices/dev_{'4' * 32}/integrations"

    assert post_integration(client, foreign_device_path).status_code == 404
    assert client.get(foreign_device_path).status_code == 404
    assert post_integration(client, integration_account_id=other_account).status_code == 404
    assert post_integration(client, integration_account_id="ia_" + "8" * 32).status_code == 404


def test_decommissioned_device_rejects_new_integration() -> None:
    reader = FakeDeviceReader(device(status=DeviceStatus.DECOMMISSIONED))
    response = post_integration(client_for(app_with_integration_repository(device_reader=reader)))
    assert response.status_code == 409


@pytest.mark.parametrize("missing", ["integration_repository", "device_component_repository"])
def test_missing_repository_state_returns_503(missing: str) -> None:
    client = client_for(app_with_integration_repository(skip_state=(missing,)))
    assert post_integration(client).status_code == 503
    assert client.get(INTEGRATIONS_PATH).status_code == 503


@pytest.mark.parametrize(
    "error",
    [
        ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "boom"}},
            "Query",
        ),
        EndpointConnectionError(endpoint_url="http://dynamodb.local"),
    ],
)
def test_infrastructure_errors_return_503_without_botocore_text(error: Exception) -> None:
    repository = DynamoIntegrationRepository("LimnopulseDomain", FailingDynamoClient(error))
    client = client_for(app_with_integration_repository(repository=repository))

    listed = client.get(ACCOUNTS_PATH)
    created = client.post(
        ACCOUNTS_PATH,
        json={"provider_key": "vendor.cloud", "display_name": "V"},
        headers={"Idempotency-Key": "integration-account-01"},
    )

    for response in (listed, created):
        assert response.status_code == 503
        assert response.json() == {"detail": "service unavailable"}


def test_dynamo_backed_identity_collision_returns_409() -> None:
    cancelled = ClientError(
        {
            "Error": {"Code": "TransactionCanceledException", "Message": "cancelled"},
            "CancellationReasons": [
                {"Code": "None"},
                {"Code": "ConditionalCheckFailed"},
                {"Code": "None"},
            ],
        },
        "TransactWriteItems",
    )

    class CollidingClient:
        def __init__(self) -> None:
            self.transactions: list[dict] = []

        def get_item(self, **kwargs):
            if kwargs["Key"]["SK"]["S"].startswith("INTEGRATION_ACCOUNT#"):
                item = {
                    "PK": f"TENANT#{TENANT}",
                    "SK": f"INTEGRATION_ACCOUNT#{ACCOUNT_ID}",
                    "entity_type": "integration_account",
                    "tenant_id": TENANT,
                    "integration_account_id": ACCOUNT_ID,
                    "provider_key": "vendor.cloud",
                    "display_name": "Vendor",
                    "status": "active",
                    "created_at": "2026-08-25T12:00:00.000000Z",
                    "updated_at": "2026-08-25T12:00:00.000000Z",
                    "version": 1,
                    "schema_version": 2,
                }
                return {
                    "Item": {
                        k: {"N" if k.endswith("version") else "S": str(v)} for k, v in item.items()
                    }
                }
            return {}

        def transact_write_items(self, **kwargs):
            self.transactions.append(kwargs)
            raise cancelled

    dynamo = CollidingClient()
    repository = DynamoIntegrationRepository("LimnopulseDomain", dynamo)
    response = post_integration(client_for(app_with_integration_repository(repository=repository)))

    assert response.status_code == 409
    assert response.json() == {"detail": "external identity already claimed"}
    assert RAW_IDENTITY not in json.dumps(dynamo.transactions)
