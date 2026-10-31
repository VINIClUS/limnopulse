"""P3-03 `IngestCredentialAuthenticator`: ADR-016 bearer authentication of connectors.

Missing, malformed, unknown, mismatched or revoked credentials, and credentials whose account is
not an active HTTPS account, all authenticate to `None` (401). Store failures propagate (503).
"""

import hmac
import logging
import secrets
from typing import Any

import pytest

from limnopulse_api.domain.ingestion import (
    AuthenticatedSource,
    IngestCredentialStatus,
    ingest_secret_sha256,
    ingest_token,
)
from limnopulse_api.domain.integrations import IntegrationAccount, IntegrationAccountStatus
from limnopulse_api.repositories.ingestion import ConnectorAuthenticator
from limnopulse_api.services import connector_auth
from limnopulse_api.services.connector_auth import IngestCredentialAuthenticator
from tests.support.ingestion_contract_kits import (
    ACCOUNT,
    AUDIT,
    CONNECTOR_AUTHENTICATOR_CONTRACT,
    CREDENTIAL,
    OTHER_CREDENTIAL,
    OTHER_TENANT,
    PROVIDER,
    RECEIVED,
    SECRET,
    TENANT,
    credential,
    integration_account,
    kit_ids,
)
from tests.support.ingestion_fakes import FakeIngestCredentialRepository, IngestionStore, KitSubject

TOKEN = ingest_token(CREDENTIAL, SECRET)


class StoreIntegrations:
    """The `get_integration_account` read of an `IntegrationRepository`, over the P3-00 store."""

    def __init__(self, store: IngestionStore) -> None:
        self.store = store
        self.reads: list[tuple[str, str]] = []

    async def get_integration_account(
        self, tenant_id: str, integration_account_id: str
    ) -> IntegrationAccount | None:
        self.reads.append((tenant_id, integration_account_id))
        return self.store.get_integration_account(tenant_id, integration_account_id)


class RecordingCredentials(FakeIngestCredentialRepository):
    def __init__(self, store: IngestionStore) -> None:
        super().__init__(store)
        self.reads: list[str] = []

    async def get_ingest_credential(self, ingest_credential_id: str):
        self.reads.append(ingest_credential_id)
        return await super().get_ingest_credential(ingest_credential_id)


class AuthenticatorSubject(KitSubject):
    def __init__(self) -> None:
        super().__init__()
        self.credentials = RecordingCredentials(self.store)
        self.integrations = StoreIntegrations(self.store)
        self.authenticator = IngestCredentialAuthenticator(
            credentials=self.credentials, integrations=self.integrations
        )

    def fail_next(self, operation: str, exc: BaseException) -> None:
        # `authenticate` itself has no store below it: fault its first read.
        super().fail_next(
            "get_ingest_credential" if operation == "authenticate" else operation, exc
        )


@pytest.fixture
def compare_calls(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Any, Any]]:
    calls: list[tuple[Any, Any]] = []
    original = hmac.compare_digest

    def spy(a: Any, b: Any) -> bool:
        calls.append(tuple(x.decode() if isinstance(x, bytes) else x for x in (a, b)))
        return original(a, b)

    # Patched where an implementation may bind it: the `hmac` module, `secrets`, or a
    # `from hmac import compare_digest` in the authenticator module.
    monkeypatch.setattr(hmac, "compare_digest", spy)
    monkeypatch.setattr(secrets, "compare_digest", spy)
    if hasattr(connector_auth, "compare_digest"):
        monkeypatch.setattr(connector_auth, "compare_digest", spy)
    return calls


async def _seeded(**account: Any) -> AuthenticatorSubject:
    subject = AuthenticatorSubject()
    await subject.seed_account(integration_account(**account))
    await subject.seed_credential(credential())
    return subject


def test_authenticator_implements_the_port() -> None:
    assert isinstance(AuthenticatorSubject().authenticator, ConnectorAuthenticator)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario", CONNECTOR_AUTHENTICATOR_CONTRACT, ids=kit_ids(CONNECTOR_AUTHENTICATOR_CONTRACT)
)
async def test_authenticator_passes_the_kit_on_the_fakes(scenario) -> None:
    await scenario(AuthenticatorSubject())


@pytest.mark.asyncio
async def test_valid_token_returns_the_exact_source() -> None:
    subject = await _seeded()

    source = await subject.authenticator.authenticate(TOKEN)

    assert source == AuthenticatedSource(
        tenant_id=TENANT,
        integration_account_id=ACCOUNT,
        provider_key=PROVIDER,
        ingest_credential_id=CREDENTIAL,
    )
    assert subject.integrations.reads == [(TENANT, ACCOUNT)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "token",
    [
        "",
        "Bearer " + TOKEN,
        TOKEN + "\n",
        " " + TOKEN,
        TOKEN[:-1],
        TOKEN + "A",
        TOKEN.replace("lpi_", "lpx_"),
        "lpi_" + CREDENTIAL.upper() + "_" + SECRET,
        "lpi_" + CREDENTIAL + "_" + SECRET[:-1] + "=",
        "lpi_" + CREDENTIAL + "_" + SECRET[:-1] + "+",
        "lpi_ic_" + "a" * 31 + "_" + SECRET,
    ],
)
async def test_malformed_tokens_are_rejected_before_any_store_read(token: str) -> None:
    subject = await _seeded()

    assert await subject.authenticator.authenticate(token) is None

    assert subject.credentials.reads == []
    assert subject.integrations.reads == []


@pytest.mark.asyncio
@pytest.mark.parametrize("token", [None, 42, b"lpi_"])
async def test_non_string_tokens_are_rejected_without_reads(token: Any) -> None:
    subject = await _seeded()

    assert await subject.authenticator.authenticate(token) is None
    assert subject.credentials.reads == []


@pytest.mark.asyncio
async def test_unknown_credential_performs_one_dummy_comparison(compare_calls: list) -> None:
    subject = await _seeded()

    assert await subject.authenticator.authenticate(ingest_token(OTHER_CREDENTIAL, SECRET)) is None

    assert subject.credentials.reads == [OTHER_CREDENTIAL]
    assert len(compare_calls) == 1
    # The presented secret is still hashed, so unknown ids cost the same as known ones.
    assert ingest_secret_sha256(SECRET) in compare_calls[0]
    assert subject.integrations.reads == []


@pytest.mark.asyncio
async def test_digests_are_compared_in_constant_time(compare_calls: list) -> None:
    subject = await _seeded()

    assert await subject.authenticator.authenticate(TOKEN) is not None

    digest = ingest_secret_sha256(SECRET)
    assert len(compare_calls) == 1
    assert list(compare_calls[0]) == [digest, digest]


@pytest.mark.asyncio
async def test_digest_mismatch_is_rejected(compare_calls: list) -> None:
    subject = await _seeded()

    assert await subject.authenticator.authenticate(ingest_token(CREDENTIAL, "B" * 43)) is None

    assert len(compare_calls) == 1
    assert credential().secret_sha256 in compare_calls[0]
    assert subject.integrations.reads == []


@pytest.mark.asyncio
async def test_revoked_credential_is_rejected() -> None:
    subject = AuthenticatorSubject()
    await subject.seed_account(integration_account())
    await subject.seed_credential(
        credential(
            status=IngestCredentialStatus.REVOKED,
            revoked_at=RECEIVED,
            updated_at=RECEIVED,
            version=2,
        )
    )

    assert await subject.authenticator.authenticate(TOKEN) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "account",
    [
        {"status": IntegrationAccountStatus.PENDING},
        {"status": IntegrationAccountStatus.ERROR},
        {"status": IntegrationAccountStatus.REVOKED},
        {"provider_key": "vendor_cloud"},
    ],
    ids=["pending", "error", "revoked", "non_https_provider"],
)
async def test_account_must_be_active_with_an_https_provider(account: dict) -> None:
    subject = await _seeded(**account)

    assert await subject.authenticator.authenticate(TOKEN) is None


@pytest.mark.asyncio
async def test_account_is_read_in_the_credentials_tenant() -> None:
    """An account with the same id in another tenant never authenticates the credential."""
    subject = AuthenticatorSubject()
    await subject.seed_account(integration_account(tenant_id=OTHER_TENANT))
    await subject.seed_credential(credential())

    assert await subject.authenticator.authenticate(TOKEN) is None
    assert subject.integrations.reads == [(TENANT, ACCOUNT)]


@pytest.mark.asyncio
async def test_revocation_takes_effect_on_the_very_next_call() -> None:
    subject = AuthenticatorSubject()
    await subject.seed_account(integration_account())
    repository = subject.credentials
    await repository.create_ingest_credential(credential(), AUDIT)

    assert await subject.authenticator.authenticate(TOKEN) is not None
    await repository.revoke_ingest_credential(
        TENANT, ACCOUNT, CREDENTIAL, expected_version=1, revoked_at=RECEIVED, audit=AUDIT
    )
    assert await subject.authenticator.authenticate(TOKEN) is None
    assert subject.credentials.reads == [CREDENTIAL, CREDENTIAL]


@pytest.mark.asyncio
async def test_account_changes_take_effect_on_the_very_next_call() -> None:
    subject = await _seeded()

    assert await subject.authenticator.authenticate(TOKEN) is not None
    await subject.seed_account(integration_account(status=IntegrationAccountStatus.REVOKED))
    assert await subject.authenticator.authenticate(TOKEN) is None
    assert subject.integrations.reads == [(TENANT, ACCOUNT), (TENANT, ACCOUNT)]


@pytest.mark.asyncio
async def test_account_store_failure_propagates() -> None:
    subject = await _seeded()
    error = RuntimeError("account store down")
    subject.fail_next("get_integration_account", error)

    with pytest.raises(RuntimeError) as exc_info:
        await subject.authenticator.authenticate(TOKEN)

    assert exc_info.value is error


@pytest.mark.asyncio
async def test_authentication_never_logs_the_token_secret_or_digest(
    caplog: pytest.LogCaptureFixture,
) -> None:
    subject = await _seeded()
    caplog.set_level(logging.DEBUG)

    for token in (
        TOKEN,
        ingest_token(CREDENTIAL, "B" * 43),
        ingest_token(OTHER_CREDENTIAL, SECRET),
    ):
        await subject.authenticator.authenticate(token)

    for secret_material in (SECRET, TOKEN, ingest_secret_sha256(SECRET), "B" * 43):
        assert secret_material not in caplog.text
