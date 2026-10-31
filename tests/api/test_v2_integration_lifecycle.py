"""P3-04: the unmounted IntegrationAccount / DeviceIntegration status routes.

`POST .../integration-accounts/{ia}/status` and `POST .../devices/{d}/integrations/{di}/status`,
both owner/admin only, with body `{status, expected_version}`, over the real Dynamo adapter on the
in-memory Dynamo fake. P3-10 mounts the router under `/v2`.
"""

from typing import Any

import pytest
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from limnopulse_api.api.v2.routers import integration_lifecycle
from limnopulse_api.api.v2.schemas import integration_lifecycle as schemas
from limnopulse_api.auth.dev import DevAuthProvider
from limnopulse_api.domain.entities import Membership
from limnopulse_api.domain.integrations import (
    DeviceIntegrationStatus,
    IntegrationAccountStatus,
)
from limnopulse_api.domain.roles import TenantRole
from tests.support.ingestion_contract_kits import (
    ACCOUNT,
    CREATED,
    DEVICE,
    IDENTITY_HASH,
    INTEGRATION,
    OTHER_DEVICE,
    OTHER_TENANT,
    PROVIDER,
    TENANT,
    device_integration,
    integration_account,
)
from tests.unit.test_dynamodb_integration_resolver import (
    WRITE_OPERATIONS,
    DynamoIngestionSubject,
    fake_subject,
)

SUB = "sub_1"
AUTH = {"X-Dev-User-Sub": SUB, "User-Agent": "lifecycle-test"}
ACCOUNT_PATH = f"/tenants/{TENANT}/integration-accounts/{ACCOUNT}/status"
INTEGRATION_PATH = f"/tenants/{TENANT}/devices/{DEVICE}/integrations/{INTEGRATION}/status"
SENTINEL = "SENTINEL-raw-value-7f3a"


class FakeMembershipService:
    def __init__(self, role: TenantRole) -> None:
        self.role = role

    async def get_active_membership(self, cognito_sub: str, tenant_id: str) -> Membership | None:
        if cognito_sub != SUB or tenant_id != TENANT:
            return None
        return Membership(
            tenant_id=tenant_id,
            cognito_sub=cognito_sub,
            role=self.role,
            created_at=CREATED,
            updated_at=CREATED,
            version=1,
        )


async def _unavailable(request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=503, content={"detail": "service unavailable"})


async def _seeded() -> DynamoIngestionSubject:
    subject = fake_subject()
    await subject.seed_account(integration_account(status=IntegrationAccountStatus.PENDING))
    await subject.seed_device_integration(
        device_integration(status=DeviceIntegrationStatus.PROVISIONING)
    )
    return subject


def client_for(subject: DynamoIngestionSubject, role: str = "owner") -> TestClient:
    app = FastAPI()
    app.include_router(integration_lifecycle.router)
    app.add_exception_handler(BotoCoreError, _unavailable)
    app.add_exception_handler(ClientError, _unavailable)
    app.state.auth_provider = DevAuthProvider()
    app.state.membership_service = FakeMembershipService(TenantRole(role))
    app.state.integration_repository = subject.integrations
    app.state.integration_lifecycle_repository = subject.integrations
    return TestClient(app, headers=AUTH)


def _writes(subject: DynamoIngestionSubject) -> list[Any]:
    return [call for call in subject.client.calls if call.operation in WRITE_OPERATIONS]


# Router shape --------------------------------------------------------------------------------


def test_router_is_unmounted_and_has_exactly_the_two_status_routes() -> None:
    from limnopulse_api.main import create_app

    routes = {
        (route.path, method)
        for route in integration_lifecycle.router.routes
        for method in route.methods
    }
    assert routes == {
        ("/tenants/{tenant_id}/integration-accounts/{integration_account_id}/status", "POST"),
        (
            "/tenants/{tenant_id}/devices/{device_id}/integrations/{device_integration_id}/status",
            "POST",
        ),
    }
    mounted = {getattr(route, "path", "") for route in create_app().routes}
    assert not any(path.endswith("/status") for path in mounted)


def test_new_schema_names_are_v2_prefixed() -> None:
    names = [
        name
        for name, value in vars(schemas).items()
        if isinstance(value, type) and value.__module__ == schemas.__name__
    ]
    assert names
    assert all(name.startswith("V2") for name in names)


# Account status ------------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["owner", "admin"])
async def test_writers_transition_an_integration_account(role: str) -> None:
    subject = await _seeded()

    response = client_for(subject, role).post(
        ACCOUNT_PATH, json={"status": "active", "expected_version": 1}
    )

    assert response.status_code == 200
    body = response.json()
    assert (body["integration_account_id"], body["status"], body["version"]) == (
        ACCOUNT,
        "active",
        2,
    )
    assert body["tenant_id"] == TENANT
    (audit,) = subject.audit_items()
    assert (audit["action"], audit["before_status"], audit["after_status"]) == (
        "integration_account.status_changed",
        "pending",
        "active",
    )
    assert audit["actor_id"] == SUB
    assert audit["user_agent"] == "lifecycle-test"


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["owner", "admin"])
async def test_writers_transition_a_device_integration(role: str) -> None:
    subject = await _seeded()

    response = client_for(subject, role).post(
        INTEGRATION_PATH, json={"status": "active", "expected_version": 1}
    )

    assert response.status_code == 200
    body = response.json()
    assert (body["device_integration_id"], body["device_id"], body["status"], body["version"]) == (
        INTEGRATION,
        DEVICE,
        "active",
        2,
    )
    assert body["external_identity_hash"] == IDENTITY_HASH
    lookup = subject.item(subject.integrations.keys.external_identity(PROVIDER, IDENTITY_HASH))
    assert lookup["status"] == "active"
    (audit,) = subject.audit_items()
    assert audit["action"] == "device_integration.status_changed"
    assert audit["actor_id"] == SUB


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["member", "viewer"])
@pytest.mark.parametrize("path", [ACCOUNT_PATH, INTEGRATION_PATH], ids=["account", "integration"])
async def test_non_writers_are_forbidden(role: str, path: str) -> None:
    subject = await _seeded()

    response = client_for(subject, role).post(
        path, json={"status": "active", "expected_version": 1}
    )

    assert response.status_code == 403
    assert _writes(subject) == []
    assert subject.audit_items() == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        f"/tenants/{OTHER_TENANT}/integration-accounts/{ACCOUNT}/status",
        f"/tenants/{OTHER_TENANT}/devices/{DEVICE}/integrations/{INTEGRATION}/status",
    ],
    ids=["account", "integration"],
)
async def test_non_member_tenant_is_forbidden(path: str) -> None:
    subject = await _seeded()

    response = client_for(subject).post(path, json={"status": "active", "expected_version": 1})

    assert response.status_code == 403
    assert _writes(subject) == []


@pytest.mark.asyncio
async def test_missing_account_is_not_found() -> None:
    subject = fake_subject()

    response = client_for(subject).post(
        ACCOUNT_PATH, json={"status": "active", "expected_version": 1}
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "not found"}


@pytest.mark.asyncio
async def test_illegal_or_stale_account_transition_is_a_conflict() -> None:
    subject = await _seeded()
    client = client_for(subject)

    stale = client.post(ACCOUNT_PATH, json={"status": "active", "expected_version": 2})
    revoked = client.post(ACCOUNT_PATH, json={"status": "revoked", "expected_version": 1})
    illegal = client.post(ACCOUNT_PATH, json={"status": "active", "expected_version": 2})

    assert stale.status_code == 409
    assert revoked.status_code == 200
    assert illegal.status_code == 409
    assert len(subject.audit_items()) == 1


# Device integration status -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_device_integration_route_is_404_when_the_path_device_differs() -> None:
    subject = await _seeded()

    response = client_for(subject).post(
        f"/tenants/{TENANT}/devices/{OTHER_DEVICE}/integrations/{INTEGRATION}/status",
        json={"status": "decommissioned", "expected_version": 1},
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "not found"}
    assert _writes(subject) == []
    assert subject.item(subject.integrations.keys.device_integration(TENANT, INTEGRATION))[
        "status"
    ] == ("provisioning")


@pytest.mark.asyncio
async def test_missing_device_integration_is_not_found() -> None:
    subject = fake_subject()

    response = client_for(subject).post(
        INTEGRATION_PATH, json={"status": "active", "expected_version": 1}
    )

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_stale_or_terminal_device_integration_transition_is_a_conflict() -> None:
    subject = await _seeded()
    client = client_for(subject)

    stale = client.post(INTEGRATION_PATH, json={"status": "active", "expected_version": 3})
    retired = client.post(
        INTEGRATION_PATH, json={"status": "decommissioned", "expected_version": 1}
    )
    revived = client.post(INTEGRATION_PATH, json={"status": "active", "expected_version": 2})

    assert (stale.status_code, retired.status_code, revived.status_code) == (409, 200, 409)


# Validation ----------------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("path", [ACCOUNT_PATH, INTEGRATION_PATH], ids=["account", "integration"])
@pytest.mark.parametrize(
    "body",
    [
        pytest.param({"status": SENTINEL, "expected_version": 1}, id="unknown status"),
        pytest.param({"status": "active", "expected_version": SENTINEL}, id="bad version"),
        pytest.param({"status": "active", "expected_version": 0}, id="version below one"),
        pytest.param({"status": "active"}, id="missing version"),
        pytest.param({"expected_version": 1}, id="missing status"),
        pytest.param(
            {"status": "active", "expected_version": 1, "tenant_id": SENTINEL}, id="extra field"
        ),
    ],
)
async def test_invalid_bodies_are_redacted_422s(path: str, body: dict[str, Any]) -> None:
    subject = await _seeded()

    response = client_for(subject).post(path, json=body)

    assert response.status_code == 422
    assert SENTINEL not in response.text
    assert all("input" not in error and "ctx" not in error for error in response.json()["detail"])
    assert _writes(subject) == []


@pytest.mark.asyncio
async def test_account_statuses_are_not_device_integration_statuses() -> None:
    subject = await _seeded()
    client = client_for(subject)

    assert (
        client.post(ACCOUNT_PATH, json={"status": "decommissioned", "expected_version": 1})
    ).status_code == 422
    assert (
        client.post(INTEGRATION_PATH, json={"status": "revoked", "expected_version": 1})
    ).status_code == 422


@pytest.mark.asyncio
async def test_store_failures_are_503() -> None:
    subject = await _seeded()
    subject.client.fail_next(
        "transact_write_items",
        ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "busy"}},
            "TransactWriteItems",
        ),
    )

    response = client_for(subject).post(
        ACCOUNT_PATH, json={"status": "active", "expected_version": 1}
    )

    assert response.status_code == 503
    assert subject.audit_items() == []


@pytest.mark.asyncio
@pytest.mark.parametrize("path", [ACCOUNT_PATH, INTEGRATION_PATH], ids=["account", "integration"])
async def test_missing_lifecycle_repository_state_is_503(path: str) -> None:
    subject = await _seeded()
    client = client_for(subject)
    del client.app.state.integration_lifecycle_repository

    response = client.post(path, json={"status": "active", "expected_version": 1})

    assert response.status_code == 503
    assert response.json() == {"detail": "service unavailable"}
    assert _writes(subject) == []
