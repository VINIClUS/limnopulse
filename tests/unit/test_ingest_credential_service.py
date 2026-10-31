"""P3-03 `IngestCredentialService`: issuance, two-slot rotation and immediate revocation."""

import asyncio
import json
import re
from datetime import timedelta
from typing import Any

import pytest

from limnopulse_api.core.errors import ConflictError, NotFoundError
from limnopulse_api.domain.ingestion import (
    INGEST_TOKEN_PATTERN,
    IngestCredential,
    IngestCredentialStatus,
    ingest_credential_fingerprint,
    ingest_secret_sha256,
    parse_ingest_token,
)
from limnopulse_api.domain.integrations import IntegrationAccount, IntegrationAccountStatus
from limnopulse_api.services.connector_auth import IngestCredentialAuthenticator
from limnopulse_api.services.ingest_credentials import (
    IngestCredentialService,
    IssuedIngestCredential,
)
from limnopulse_api.services.integrations import IntegrationInputError
from tests.support.ingestion_contract_kits import (
    ACCOUNT,
    AUDIT,
    CREATED,
    OTHER_ACCOUNT,
    OTHER_TENANT,
    TENANT,
    integration_account,
)
from tests.support.ingestion_fakes import FakeIngestCredentialRepository, Faults, IngestionStore

IDS = ["ic_" + digit * 32 for digit in "abcdef"]
SECRETS = [letter * 43 for letter in "ABCDEF"]


class StoreIntegrations:
    def __init__(self, store: IngestionStore) -> None:
        self.store = store

    async def get_integration_account(
        self, tenant_id: str, integration_account_id: str
    ) -> IntegrationAccount | None:
        return self.store.get_integration_account(tenant_id, integration_account_id)


class Clock:
    def __init__(self) -> None:
        self.now = CREATED

    def __call__(self):
        return self.now


class Harness:
    def __init__(self, *accounts: IntegrationAccount) -> None:
        self.store = IngestionStore(Faults())
        for account in accounts:
            self.store.accounts[(account.tenant_id, account.integration_account_id)] = account
        self.repository = FakeIngestCredentialRepository(self.store)
        self.integrations = StoreIntegrations(self.store)
        self.clock = Clock()
        self.ids = iter(IDS)
        self.secrets = iter(SECRETS)
        self.service = self.make_service()

    def make_service(self, repository: Any = None) -> IngestCredentialService:
        return IngestCredentialService(
            credentials=repository or self.repository,
            integrations=self.integrations,
            clock=self.clock,
            secret_factory=lambda: next(self.secrets),
            id_factory=lambda: next(self.ids),
        )

    async def issue(self, tenant_id: str = TENANT, account_id: str = ACCOUNT):
        return await self.service.issue(tenant_id, account_id, audit=AUDIT)

    async def revoke(self, credential_id: str, version: int = 1, **path: str) -> IngestCredential:
        return await self.service.revoke(
            path.get("tenant_id", TENANT),
            path.get("account_id", ACCOUNT),
            credential_id,
            expected_version=version,
            audit=AUDIT,
        )


# Issuance ------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_issue_returns_a_grammar_token_and_stores_only_digest_and_fingerprint() -> None:
    harness = Harness(integration_account())

    issued = await harness.issue()

    assert isinstance(issued, IssuedIngestCredential)
    assert re.fullmatch(INGEST_TOKEN_PATTERN, issued.token)
    assert parse_ingest_token(issued.token) == (IDS[0], SECRETS[0])
    stored = harness.store.credentials[IDS[0]]
    assert issued.credential == stored
    assert stored.secret_sha256 == ingest_secret_sha256(SECRETS[0])
    assert stored.fingerprint == ingest_credential_fingerprint(stored.secret_sha256)
    assert (stored.tenant_id, stored.integration_account_id) == (TENANT, ACCOUNT)
    assert (stored.status, stored.revoked_at, stored.version) == (
        IngestCredentialStatus.ACTIVE,
        None,
        1,
    )
    assert stored.created_at == stored.updated_at == CREATED
    persisted = json.dumps(
        [item.model_dump(mode="json") for item in harness.store.credentials.values()]
        + harness.store.audit,
        default=str,
    )
    assert SECRETS[0] not in persisted
    assert issued.token not in persisted
    assert issued.token not in repr(issued) and SECRETS[0] not in repr(issued)


@pytest.mark.asyncio
async def test_default_secret_factory_yields_43_char_base64url_secrets() -> None:
    harness = Harness(integration_account())
    service = IngestCredentialService(
        credentials=harness.repository, integrations=harness.integrations, clock=harness.clock
    )

    first = await service.issue(TENANT, ACCOUNT, audit=AUDIT)
    second = await service.issue(TENANT, ACCOUNT, audit=AUDIT)

    for issued in (first, second):
        parsed = parse_ingest_token(issued.token)
        assert parsed is not None
        assert parsed[0] == issued.credential.ingest_credential_id
    assert first.token != second.token
    assert first.credential.ingest_credential_id != second.credential.ingest_credential_id


@pytest.mark.asyncio
async def test_two_slots_then_a_third_active_credential_is_a_conflict() -> None:
    harness = Harness(integration_account())

    first = await harness.issue()
    second = await harness.issue()

    assert (first.credential.slot, second.credential.slot) == (1, 2)
    with pytest.raises(ConflictError):
        await harness.issue()
    assert set(harness.store.credentials) == {IDS[0], IDS[1]}


@pytest.mark.asyncio
async def test_revoke_frees_its_slot_and_the_next_issue_reuses_it() -> None:
    harness = Harness(integration_account())
    first = await harness.issue()
    await harness.issue()

    harness.clock.now = CREATED + timedelta(hours=2)
    revoked = await harness.revoke(first.credential.ingest_credential_id)
    replacement = await harness.issue()

    assert revoked.status == IngestCredentialStatus.REVOKED
    assert revoked.revoked_at == revoked.updated_at == CREATED + timedelta(hours=2)
    assert revoked.version == 2
    assert replacement.credential.slot == first.credential.slot == 1


@pytest.mark.asyncio
async def test_rotation_keeps_the_older_credential_valid_until_it_is_revoked() -> None:
    harness = Harness(integration_account())
    authenticator = IngestCredentialAuthenticator(
        credentials=harness.repository, integrations=harness.integrations
    )
    old = await harness.issue()
    new = await harness.issue()

    assert await authenticator.authenticate(old.token) is not None
    assert await authenticator.authenticate(new.token) is not None
    await harness.revoke(old.credential.ingest_credential_id)
    assert await authenticator.authenticate(old.token) is None
    assert await authenticator.authenticate(new.token) is not None


@pytest.mark.asyncio
async def test_pending_account_may_issue() -> None:
    harness = Harness(integration_account(status=IntegrationAccountStatus.PENDING))

    issued = await harness.issue()

    assert issued.credential.integration_account_id == ACCOUNT


@pytest.mark.asyncio
async def test_missing_or_foreign_account_is_not_found() -> None:
    harness = Harness(integration_account(tenant_id=OTHER_TENANT))

    with pytest.raises(NotFoundError):
        await harness.issue()
    with pytest.raises(NotFoundError):
        await harness.issue(account_id=OTHER_ACCOUNT)
    assert harness.store.credentials == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "account",
    [
        {"provider_key": "vendor_cloud"},
        {"status": IntegrationAccountStatus.REVOKED},
        {"status": IntegrationAccountStatus.ERROR},
    ],
    ids=["non_https_provider", "revoked", "error"],
)
async def test_ineligible_account_is_rejected_as_invalid_input(account: dict) -> None:
    harness = Harness(integration_account(**account))

    with pytest.raises(IntegrationInputError):
        await harness.issue()
    assert harness.store.credentials == {}
    assert harness.store.audit == []


@pytest.mark.asyncio
async def test_concurrent_issues_for_one_free_slot_have_exactly_one_winner() -> None:
    """Both callers see slot 1 free; the slot condition lets exactly one commit."""
    harness = Harness(integration_account())
    await harness.issue()  # slot 1 taken; slot 2 is the one free slot
    barrier = asyncio.Barrier(2)

    class RacingRepository(FakeIngestCredentialRepository):
        async def list_ingest_credentials(self, tenant_id: str, integration_account_id: str):
            listed = await super().list_ingest_credentials(tenant_id, integration_account_id)
            await barrier.wait()  # both callers read before either writes
            return listed

    racing = harness.make_service(RacingRepository(harness.store))

    results = await asyncio.gather(
        racing.issue(TENANT, ACCOUNT, audit=AUDIT),
        racing.issue(TENANT, ACCOUNT, audit=AUDIT),
        return_exceptions=True,
    )

    winners = [result for result in results if isinstance(result, IssuedIngestCredential)]
    assert len(winners) == 1
    assert sum(isinstance(result, ConflictError) for result in results) == 1
    assert winners[0].credential.slot == 2
    active = [c for c in harness.store.credentials.values() if c.status == "active"]
    assert len(active) == 2


@pytest.mark.asyncio
async def test_issue_passes_the_audit_context_to_the_repository() -> None:
    harness = Harness(integration_account())

    issued = await harness.issue()

    (audit,) = harness.store.audit
    assert audit["action"] == "ingest_credential.issued"
    assert audit["actor_id"] == AUDIT.actor_id
    assert audit["resource_id"] == issued.credential.ingest_credential_id


# Listing -------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_returns_the_accounts_credentials_only() -> None:
    harness = Harness(
        integration_account(),
        integration_account(integration_account_id=OTHER_ACCOUNT),
    )
    mine = await harness.issue()
    await harness.issue(account_id=OTHER_ACCOUNT)

    listed = await harness.service.list(TENANT, ACCOUNT)

    assert listed == [mine.credential]


@pytest.mark.asyncio
async def test_list_for_a_missing_account_is_not_found() -> None:
    harness = Harness(integration_account())

    with pytest.raises(NotFoundError):
        await harness.service.list(OTHER_TENANT, ACCOUNT)


# Revocation ----------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_revoke_with_a_stale_version_is_a_conflict() -> None:
    harness = Harness(integration_account())
    issued = await harness.issue()

    with pytest.raises(ConflictError) as exc_info:
        await harness.revoke(issued.credential.ingest_credential_id, version=5)

    assert not isinstance(exc_info.value, NotFoundError)
    assert harness.store.credentials[IDS[0]] == issued.credential


@pytest.mark.asyncio
async def test_revoking_an_already_revoked_credential_is_a_conflict() -> None:
    harness = Harness(integration_account())
    issued = await harness.issue()
    credential_id = issued.credential.ingest_credential_id
    await harness.revoke(credential_id)
    audit_before = list(harness.store.audit)

    for version in (1, 2):
        with pytest.raises(ConflictError) as exc_info:
            await harness.revoke(credential_id, version=version)
        assert not isinstance(exc_info.value, NotFoundError)
    assert harness.store.audit == audit_before


@pytest.mark.asyncio
async def test_cross_tenant_or_cross_account_revoke_is_not_found_and_changes_nothing() -> None:
    harness = Harness(
        integration_account(),
        integration_account(tenant_id=OTHER_TENANT),
        integration_account(integration_account_id=OTHER_ACCOUNT),
    )
    victim = await harness.issue(tenant_id=OTHER_TENANT)
    victim_id = victim.credential.ingest_credential_id

    for path in ({"tenant_id": TENANT}, {"account_id": OTHER_ACCOUNT}):
        with pytest.raises(NotFoundError):
            await harness.revoke(victim_id, **path)
    # Even once revoked, a foreign credential is still "not found", never "conflict".
    await harness.revoke(victim_id, tenant_id=OTHER_TENANT)
    with pytest.raises(NotFoundError):
        await harness.revoke(victim_id, version=2, tenant_id=TENANT)

    assert harness.store.credentials[victim_id].tenant_id == OTHER_TENANT
    assert [item["tenant_id"] for item in harness.store.audit] == [OTHER_TENANT, OTHER_TENANT]


@pytest.mark.asyncio
async def test_revoke_of_an_unknown_credential_is_not_found() -> None:
    harness = Harness(integration_account())

    with pytest.raises(NotFoundError):
        await harness.revoke(IDS[5])


@pytest.mark.asyncio
async def test_store_failures_propagate() -> None:
    harness = Harness(integration_account())
    issued = await harness.issue()
    for operation, call in (
        ("create_ingest_credential", lambda: harness.issue()),
        (
            "revoke_ingest_credential",
            lambda: harness.revoke(issued.credential.ingest_credential_id),
        ),
        ("list_ingest_credentials", lambda: harness.service.list(TENANT, ACCOUNT)),
    ):
        error = RuntimeError(operation)
        harness.store.faults.fail_next(operation, error)
        with pytest.raises(RuntimeError) as exc_info:
            await call()
        assert exc_info.value is error
