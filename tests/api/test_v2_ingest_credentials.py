"""P3-03 ingest-credential routes (unmounted): owner/admin only, the token exactly once with
`no-store`, metadata-only listings, versioned and tenant-bound revocation, redacted 422s.

The routes run over the real Dynamo adapters on the P3-00 in-memory Dynamo fake, so the stored
items can be inspected directly.
"""

import json
import logging
import re
from datetime import UTC, datetime
from typing import Any

import pytest
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from limnopulse_api.adapters.dynamodb_ingest_credentials import DynamoIngestCredentialRepository
from limnopulse_api.adapters.dynamodb_integrations import (
    DynamoIntegrationRepository,
    integration_account_to_item,
)
from limnopulse_api.adapters.dynamodb_v2 import DynamoV2KeyBuilder
from limnopulse_api.api.v2.routers import ingest_credentials
from limnopulse_api.auth.dev import DevAuthProvider
from limnopulse_api.domain.entities import Membership
from limnopulse_api.domain.ingestion import INGEST_TOKEN_PATTERN, parse_ingest_token
from limnopulse_api.domain.integrations import IntegrationAccount, IntegrationAccountStatus
from limnopulse_api.domain.roles import TenantRole
from tests.support.ingestion_contract_kits import (
    AUDIT,
    credential,
    integration_account,
)
from tests.support.ingestion_dynamo_fake import IngestionDynamoClient

NOW = datetime(2026, 10, 31, 9, 30, tzinfo=UTC)
TENANT = "tnt_a"
OTHER_TENANT = "tnt_b"
ACCOUNT = "ia_" + "1" * 32
OTHER_ACCOUNT = "ia_" + "2" * 32
FOREIGN_CREDENTIAL = "ic_" + "f" * 32
TABLE = "LimnopulseDomain"
AUDIT_TABLE = "LimnopulseAudit"
CREDENTIALS = f"/tenants/{TENANT}/integration-accounts/{ACCOUNT}/ingest-credentials"
AUTH = {"X-Dev-User-Sub": "sub_1", "User-Agent": "p3-03-tests"}
SENTINEL = "SENTINEL-p303-echo"
KEYS = DynamoV2KeyBuilder()
_SERIALIZER = TypeSerializer()
_DESERIALIZER = TypeDeserializer()


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


class StaleAccountReads:
    """An integration repository whose account read predates a concurrent status change."""

    def __init__(self, inner: DynamoIntegrationRepository, stale: IntegrationAccount) -> None:
        self.inner = inner
        self.stale = stale

    async def get_integration_account(self, tenant_id: str, integration_account_id: str):
        return self.stale

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)


async def _unavailable(request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=503, content={"detail": "service unavailable"})


class Api:
    def __init__(self, role: str = "owner", *accounts: IntegrationAccount) -> None:
        self.dynamo = IngestionDynamoClient()
        for account in accounts or (self.account(),):
            self.seed_account(account)
        self.repository = DynamoIngestCredentialRepository(
            TABLE, self.dynamo, clock=lambda: NOW, audit_table_name=AUDIT_TABLE
        )
        self.integrations = DynamoIntegrationRepository(TABLE, self.dynamo, clock=lambda: NOW)
        self.app = FastAPI()
        self.app.include_router(ingest_credentials.router)
        self.app.add_exception_handler(BotoCoreError, _unavailable)
        self.app.add_exception_handler(ClientError, _unavailable)
        self.app.state.auth_provider = DevAuthProvider()
        self.app.state.membership_service = FakeMembershipService(TenantRole(role))
        self.app.state.ingest_credential_repository = self.repository
        self.app.state.integration_repository = self.integrations
        self.client = TestClient(self.app, headers=AUTH)

    @staticmethod
    def account(**overrides: Any) -> IntegrationAccount:
        return integration_account(
            **{"tenant_id": TENANT, "integration_account_id": ACCOUNT, **overrides}
        )

    def seed_account(self, account: IntegrationAccount) -> None:
        item = integration_account_to_item(account, KEYS)
        self.dynamo.put_item(
            TableName=TABLE, Item={k: _SERIALIZER.serialize(v) for k, v in item.items()}
        )

    def issue(self, path: str = CREDENTIALS):
        return self.client.post(path)

    def revoke(self, credential_id: str, body: Any = None, path: str = CREDENTIALS):
        return self.client.post(
            f"{path}/{credential_id}/revoke",
            json={"expected_version": 1} if body is None else body,
        )

    def items(self) -> list[dict[str, Any]]:
        return [dict(item) for item in self.dynamo.items.values()]

    def written(self) -> str:
        """Every stored item plus every transaction request, rendered as text."""
        requests = [
            {kind: {k: v for k, v in body.items()} for kind, body in action.items()}
            for transaction in self.dynamo.transactions
            for action in transaction
        ]
        return json.dumps([self.items(), requests], default=str)

    def audit(self) -> list[dict[str, Any]]:
        return [item for item in self.items() if item.get("entity_type") == "audit_event"]


def _decode(item: dict[str, Any]) -> dict[str, Any]:
    return {key: _DESERIALIZER.deserialize(value) for key, value in item.items()}


# Mounting ------------------------------------------------------------------------------------


def test_router_is_unmounted_and_exposes_exactly_the_credential_routes() -> None:
    from limnopulse_api.main import create_app

    routes = {
        (route.path, method)
        for route in ingest_credentials.router.routes
        for method in getattr(route, "methods", ())
    }
    assert routes == {
        (
            "/tenants/{tenant_id}/integration-accounts/{integration_account_id}/ingest-credentials",
            "POST",
        ),
        (
            "/tenants/{tenant_id}/integration-accounts/{integration_account_id}/ingest-credentials",
            "GET",
        ),
        (
            (
                "/tenants/{tenant_id}/integration-accounts/{integration_account_id}"
                "/ingest-credentials/{ingest_credential_id}/revoke"
            ),
            "POST",
        ),
    }
    mounted = {getattr(route, "path", "") for route in create_app().routes}
    assert not any("ingest-credentials" in path for path in mounted)


# Roles ---------------------------------------------------------------------------------------


@pytest.mark.parametrize("role", ["member", "viewer"])
def test_member_and_viewer_are_forbidden_on_every_route(role: str) -> None:
    api = Api(role)

    responses = [
        api.issue(),
        api.client.get(CREDENTIALS),
        api.revoke(FOREIGN_CREDENTIAL),
    ]

    assert [response.status_code for response in responses] == [403, 403, 403]
    assert api.dynamo.transactions == []
    assert api.dynamo.reads == []  # the role check runs before any store access


@pytest.mark.parametrize("role", ["owner", "admin"])
def test_owner_and_admin_manage_credentials(role: str) -> None:
    api = Api(role)

    issued = api.issue()
    listed = api.client.get(CREDENTIALS)
    revoked = api.revoke(issued.json()["ingest_credential_id"])

    assert (issued.status_code, listed.status_code, revoked.status_code) == (201, 200, 200)


def test_non_member_tenant_is_forbidden() -> None:
    api = Api()

    response = api.issue(
        f"/tenants/{OTHER_TENANT}/integration-accounts/{ACCOUNT}/ingest-credentials"
    )

    assert response.status_code == 403
    assert api.dynamo.transactions == []
    assert api.dynamo.reads == []


# Issuance ------------------------------------------------------------------------------------


def test_issue_returns_the_token_once_with_no_store_headers() -> None:
    api = Api()

    response = api.issue()

    assert response.status_code == 201
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["pragma"] == "no-cache"
    body = response.json()
    token = body["token"]
    assert re.fullmatch(INGEST_TOKEN_PATTERN, token)
    credential_id, secret = parse_ingest_token(token)
    assert body["ingest_credential_id"] == credential_id
    assert body["tenant_id"] == TENANT
    assert body["integration_account_id"] == ACCOUNT
    assert body["slot"] == 1
    assert body["status"] == "active"
    assert body["version"] == 1
    assert len(body["fingerprint"]) == 8
    assert "secret_sha256" not in body
    (stored,) = [item for item in api.items() if item.get("entity_type") == "ingest_credential"]
    assert stored["secret_sha256"] not in response.text
    assert body["fingerprint"] == stored["secret_sha256"][:8]
    # The token and its secret appear in this response only, never in storage.
    written = api.written()
    assert token not in written and secret not in written


def test_storage_holds_the_digest_only_on_the_authoritative_row() -> None:
    api = Api()
    token = api.issue().json()["token"]
    credential_id, secret = parse_ingest_token(token)
    api.revoke(credential_id)

    (authoritative,) = [i for i in api.items() if i.get("entity_type") == "ingest_credential"]
    digest = authoritative["secret_sha256"]
    for item in api.items():
        for name, value in item.items():
            if digest in json.dumps(value, default=str):
                assert item is not None and (item["PK"], item["SK"], name) == (
                    f"INGEST_CREDENTIAL#{credential_id}",
                    "META",
                    "secret_sha256",
                )
    assert len(api.audit()) == 2
    for item in api.audit():
        rendered = json.dumps(item, default=str)
        assert digest not in rendered and token not in rendered and secret not in rendered
        assert item["fingerprint"] == digest[:8]


def test_listing_and_revoke_responses_never_show_the_token_or_digest() -> None:
    api = Api()
    issued = api.issue().json()
    digest = next(
        i["secret_sha256"] for i in api.items() if i.get("entity_type") == "ingest_credential"
    )

    listed = api.client.get(CREDENTIALS)
    revoked = api.revoke(issued["ingest_credential_id"])

    for response in (listed, revoked):
        assert issued["token"] not in response.text
        assert digest not in response.text
        assert "secret_sha256" not in response.text
        assert "token" not in response.text
    (item,) = listed.json()["items"]
    assert item["ingest_credential_id"] == issued["ingest_credential_id"]
    assert item["fingerprint"] == issued["fingerprint"]
    assert revoked.json()["status"] == "revoked"
    assert revoked.json()["version"] == 2


def test_third_active_credential_is_a_conflict() -> None:
    api = Api()

    statuses = [api.issue().status_code for _ in range(3)]

    assert statuses == [201, 201, 409]


def test_revoke_frees_the_slot_for_the_next_issue() -> None:
    api = Api()
    first = api.issue().json()
    api.issue()

    assert api.revoke(first["ingest_credential_id"]).status_code == 200
    replacement = api.issue()

    assert replacement.status_code == 201
    assert replacement.json()["slot"] == first["slot"]


def test_issue_for_a_missing_account_is_not_found() -> None:
    api = Api()

    response = api.issue(
        f"/tenants/{TENANT}/integration-accounts/{OTHER_ACCOUNT}/ingest-credentials"
    )

    assert response.status_code == 404


@pytest.mark.parametrize(
    "account",
    [
        {"provider_key": "vendor_cloud"},
        {"status": IntegrationAccountStatus.REVOKED},
        {"status": IntegrationAccountStatus.ERROR},
    ],
    ids=["non_https_provider", "revoked", "error"],
)
def test_issue_for_an_ineligible_account_is_unprocessable(account: dict) -> None:
    api = Api("owner", Api.account(**account))

    response = api.issue()

    assert response.status_code == 422
    assert api.dynamo.transactions == []


@pytest.mark.parametrize(
    "changed",
    [{"status": IntegrationAccountStatus.REVOKED}, {"provider_key": "vendor_cloud"}],
    ids=["status_changed", "provider_changed"],
)
def test_issue_racing_an_account_change_fails_the_condition_check_with_409(changed: dict) -> None:
    api = Api("owner", Api.account(**changed))
    # The service read saw the account while it was still an active HTTPS account.
    api.app.state.integration_repository = StaleAccountReads(api.integrations, Api.account())

    response = api.issue()

    assert response.status_code == 409
    assert [i for i in api.items() if i.get("entity_type") == "ingest_credential"] == []
    assert api.audit() == []


def test_issue_writes_an_audit_item_with_the_request_actor() -> None:
    api = Api()

    issued = api.issue().json()

    (audit,) = api.audit()
    assert audit["action"] == "ingest_credential.issued"
    assert audit["actor_id"] == "sub_1"
    assert audit["user_agent"] == "p3-03-tests"
    assert audit["tenant_id"] == TENANT
    assert audit["resource_id"] == issued["ingest_credential_id"]
    assert audit["fingerprint"] == issued["fingerprint"]


def test_issue_ignores_a_request_body_and_never_echoes_it() -> None:
    api = Api()

    response = api.client.post(CREDENTIALS, json={"token": SENTINEL, "secret": SENTINEL})

    assert response.status_code == 201
    assert SENTINEL not in response.text
    assert SENTINEL not in api.written()


# Revocation ----------------------------------------------------------------------------------


def test_revoke_requires_expected_version() -> None:
    api = Api()
    credential_id = api.issue().json()["ingest_credential_id"]

    for body in ({}, {"expected_version": 0}, {"expected_version": None}):
        assert api.revoke(credential_id, body).status_code == 422
    assert api.revoke(credential_id, {"expected_version": 1, "extra": True}).status_code == 422
    stored = [i for i in api.items() if i.get("entity_type") == "ingest_credential"]
    assert [item["status"] for item in stored] == ["active"]


def test_revoke_with_a_stale_version_is_a_conflict() -> None:
    api = Api()
    credential_id = api.issue().json()["ingest_credential_id"]

    response = api.revoke(credential_id, {"expected_version": 4})

    assert response.status_code == 409
    assert len(api.audit()) == 1


def test_revoking_an_already_revoked_credential_is_a_conflict() -> None:
    api = Api()
    credential_id = api.issue().json()["ingest_credential_id"]
    assert api.revoke(credential_id).status_code == 200

    again = api.revoke(credential_id, {"expected_version": 2})
    stale = api.revoke(credential_id, {"expected_version": 1})

    assert (again.status_code, stale.status_code) == (409, 409)
    assert len(api.audit()) == 2


@pytest.mark.asyncio
async def test_cross_tenant_revoke_is_not_found_and_leaves_the_row_unchanged() -> None:
    api = Api(
        "owner",
        Api.account(),
        Api.account(tenant_id=OTHER_TENANT),
    )
    foreign = credential(
        tenant_id=OTHER_TENANT,
        integration_account_id=ACCOUNT,
        ingest_credential_id=FOREIGN_CREDENTIAL,
    )
    await api.repository.create_ingest_credential(foreign, AUDIT)
    before = api.items()

    for version in (1, 2):
        response = api.revoke(FOREIGN_CREDENTIAL, {"expected_version": version})
        assert response.status_code == 404
        assert FOREIGN_CREDENTIAL not in response.text

    assert api.items() == before
    assert (await api.repository.get_ingest_credential(FOREIGN_CREDENTIAL)) == foreign


def test_revoke_under_another_account_is_not_found() -> None:
    api = Api("owner", Api.account(), Api.account(integration_account_id=OTHER_ACCOUNT))
    credential_id = api.issue().json()["ingest_credential_id"]
    other = f"/tenants/{TENANT}/integration-accounts/{OTHER_ACCOUNT}/ingest-credentials"

    assert api.revoke(credential_id, path=other).status_code == 404
    assert api.revoke(credential_id).status_code == 200


def test_revoke_of_an_unknown_credential_is_not_found() -> None:
    api = Api()

    assert api.revoke(FOREIGN_CREDENTIAL).status_code == 404


def test_revoke_writes_one_audit_item_in_the_same_transaction() -> None:
    api = Api()
    issued = api.issue().json()

    api.revoke(issued["ingest_credential_id"])

    revoke_transaction = api.dynamo.transactions[-1]
    audit_puts = [
        _decode(action["Put"]["Item"])
        for action in revoke_transaction
        if "Put" in action and action["Put"]["TableName"] == AUDIT_TABLE
    ]
    assert [item["action"] for item in audit_puts] == ["ingest_credential.revoked"]
    assert audit_puts[0]["fingerprint"] == issued["fingerprint"]
    assert [item["action"] for item in api.audit()] == [
        "ingest_credential.issued",
        "ingest_credential.revoked",
    ]


# Redaction and failures ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "body",
    [
        {"expected_version": SENTINEL},
        {"expected_version": [SENTINEL]},
        {"expected_version": {"x": SENTINEL}},
    ],
)
def test_revoke_422_never_echoes_input(body: dict) -> None:
    api = Api()
    credential_id = api.issue().json()["ingest_credential_id"]

    response = api.revoke(credential_id, body)

    assert response.status_code == 422
    assert SENTINEL not in response.text


@pytest.mark.parametrize(
    "path",
    [
        f"/tenants/{TENANT}/integration-accounts/{ACCOUNT}/ingest-credentials/ic_{SENTINEL}/revoke",
        (
            f"/tenants/{TENANT}/integration-accounts/ia_{SENTINEL}/ingest-credentials/"
            f"{FOREIGN_CREDENTIAL}/revoke"
        ),
    ],
)
def test_bad_path_ids_are_rejected_without_echo(path: str) -> None:
    api = Api()

    response = api.client.post(path, json={"expected_version": 1})

    assert SENTINEL not in response.text
    assert api.dynamo.transactions == []
    if f"ic_{SENTINEL}" in path:
        # `IngestCredentialId` is canonical by contract: rejected before any store access.
        assert response.status_code == 422
    else:
        # `integration_account_id` is a SafeId: a non-echoing 404 is fine too.
        assert response.status_code in (404, 422)
    if response.status_code == 422:
        assert api.dynamo.reads == []


@pytest.mark.parametrize(
    "error",
    [
        ClientError({"Error": {"Code": "InternalServerError", "Message": "x"}}, "Dynamo"),
        EndpointConnectionError(endpoint_url="http://dynamodb.invalid"),
    ],
    ids=["client_error", "endpoint_down"],
)
@pytest.mark.parametrize("route", ["issue", "list", "revoke"])
def test_store_failures_are_503_not_404_or_409(route: str, error: Exception) -> None:
    api = Api()
    credential_id = api.issue().json()["ingest_credential_id"]
    operation = {"issue": "transact_write_items", "list": "query", "revoke": "transact_write_items"}
    api.dynamo.fail_next(operation[route], error)

    response = {
        "issue": lambda: api.issue(),
        "list": lambda: api.client.get(CREDENTIALS),
        "revoke": lambda: api.revoke(credential_id),
    }[route]()

    assert response.status_code == 503


def test_issuance_takes_no_idempotency_key_and_writes_no_receipt() -> None:
    api = Api()
    headers = {"Idempotency-Key": "issue-credential-01"}

    first = api.client.post(CREDENTIALS, headers=headers)
    second = api.client.post(CREDENTIALS, headers=headers)

    assert (first.status_code, second.status_code) == (201, 201)
    assert first.json()["ingest_credential_id"] != second.json()["ingest_credential_id"]
    assert "issue-credential-01" not in api.written()
    assert not [i for i in api.items() if "receipt" in str(i.get("entity_type"))]


@pytest.mark.parametrize(
    "status", [IntegrationAccountStatus.REVOKED, IntegrationAccountStatus.ERROR]
)
def test_revoke_still_works_once_the_account_is_ineligible(status) -> None:
    api = Api()
    credential_id = api.issue().json()["ingest_credential_id"]
    api.seed_account(Api.account(status=status))

    response = api.revoke(credential_id)

    assert response.status_code == 200
    assert response.json()["status"] == "revoked"


def test_credential_routes_never_log_secret_material(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    api = Api()

    issued = api.issue().json()
    api.client.get(CREDENTIALS)
    api.revoke(issued["ingest_credential_id"])
    api.revoke(issued["ingest_credential_id"], {"expected_version": 2})

    digest = next(
        i["secret_sha256"] for i in api.items() if i.get("entity_type") == "ingest_credential"
    )
    _, secret = parse_ingest_token(issued["token"])
    for material in (issued["token"], secret, digest):
        assert material not in caplog.text


def test_listing_is_scoped_to_the_path_account_and_404s_for_a_missing_one() -> None:
    api = Api("owner", Api.account(), Api.account(integration_account_id=OTHER_ACCOUNT))
    other = f"/tenants/{TENANT}/integration-accounts/{OTHER_ACCOUNT}/ingest-credentials"
    mine = api.issue().json()["ingest_credential_id"]
    api.issue(other)

    listed = api.client.get(CREDENTIALS)
    missing = api.client.get(
        f"/tenants/{TENANT}/integration-accounts/{'ia_' + '9' * 32}/ingest-credentials"
    )

    assert [item["ingest_credential_id"] for item in listed.json()["items"]] == [mine]
    assert missing.status_code == 404
