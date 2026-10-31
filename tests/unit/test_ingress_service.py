"""P3-07: `IngressService` follows the frozen route order (steps 1-4 and 8-16) and the receipt
replay table exactly, over the P3-00 fakes of every port.

`IngressService.authenticate(authorization)` runs steps 1-4 (grammar, the two pre-auth buckets,
the authenticator, the per-account bucket); `IngressService.admit(source, batch, ...)` runs steps
8-16 (receipt lookup, trusted admission, the component rule, the entitlement hook, the envelope
and its size guard, the claim, the SQS send and the mark). A rejection raises `IngestRejected`
with the fixed status, body and headers of the route order table. Every port is wrapped in a spy
that records the call order, so each short-circuit is asserted as "nothing called after it".
"""

import asyncio
import json
import logging
from collections.abc import Callable
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Any

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from limnopulse_api.adapters.dynamodb_v2 import canonical_request_hash, idempotency_key_hash
from limnopulse_api.core.errors import IngressMessageTooLarge, IngressQueueError
from limnopulse_api.domain.devices_v2 import DeviceStatus
from limnopulse_api.domain.ingestion import (
    INGRESS_ENVELOPE_SCHEMA,
    MAX_INGRESS_MESSAGE_BYTES,
    AuthenticatedSource,
    IngestObservationBatch,
    IngressAdmission,
    IngressAdmissionDenied,
    IngressEnvelope,
    IngressReceipt,
    IngressReceiptState,
    ingest_token,
    ingress_message_bytes,
    new_ingress_receipt,
    preauth_bucket_capacity,
)
from limnopulse_api.domain.integrations import DeviceIntegrationStatus, IntegrationAccountStatus
from limnopulse_api.domain.telemetry_v2 import NormalizationVersions
from limnopulse_api.repositories.ingestion import IngressEntitlementCheck
from limnopulse_api.services.ingress_entitlements import AllowDirectIngressEntitlements
from limnopulse_api.services.ingestion import IngestAccepted, IngestRejected, IngressService
from tests.support.ingestion_contract_kits import (
    ACCOUNT,
    COMPONENT,
    CREDENTIAL,
    DEFAULT_COMPONENT,
    DEVICE,
    EXTERNAL_DEVICE,
    IDENTITY_HASH,
    INTEGRATION,
    OBSERVED,
    OTHER_ACCOUNT,
    OTHER_COMPONENT,
    OTHER_TENANT,
    PROVIDER,
    RECEIVED,
    SECRET,
    TENANT,
    component,
    credential,
    device,
    device_integration,
    entitlement_default_allows,
    integration_account,
    oversized_envelope,
)
from tests.support.ingestion_fakes import (
    FakeConnectorAuthenticator,
    FakeEntitlementCheck,
    FakeIngressQueue,
    FakeIngressReceiptRepository,
    FakeTrustedMapping,
    InMemoryRateLimiter,
    KitSubject,
    RecordingTelemetryMetrics,
)

TOKEN = ingest_token(CREDENTIAL, SECRET)
BEARER = f"Bearer {TOKEN}"
IDEMPOTENCY_KEY = "idem-key-0001-SENTINEL"
OTHER_KEY = "idem-key-0002-SENTINEL"
CONNECTOR_VERSION = "fw-2.4.1"
SOURCE = AuthenticatedSource(
    tenant_id=TENANT,
    integration_account_id=ACCOUNT,
    provider_key=PROVIDER,
    ingest_credential_id=CREDENTIAL,
)
PINNED = NormalizationVersions(
    catalog_version="metric-catalog@0",
    unit_mapping_version="unit-mapping@0",
    quality_policy_version="quality-policy@0",
)
UNAUTHORIZED = (401, "unauthorized", {"WWW-Authenticate": "Bearer"})
FORBIDDEN = (403, "forbidden", {})
CONFLICT = (409, "idempotency key reused with a different request", {})
TOO_LARGE = (413, "request body too large", {})
UNAVAILABLE = (503, "service unavailable", {})
ADMIT_STEPS = [
    "receipts.get_ingress_receipt",
    "trusted_mapping.admit",
    "entitlements.allows",
    "receipts.claim_ingress_receipt",
    "producer.send",
    "receipts.mark_ingress_receipt_enqueued",
]


def store_errors() -> tuple[BaseException, ...]:
    return (
        ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "busy"}},
            "GetItem",
        ),
        EndpointConnectionError(endpoint_url="http://dynamodb.invalid"),
    )


class Spy:
    """Forwards every call to `target` and records `(name.method, args, kwargs)` in `log`."""

    def __init__(self, name: str, target: Any, log: list[tuple[str, tuple, dict]]) -> None:
        self._name = name
        self._target = target
        self._log = log

    def __getattr__(self, attribute: str) -> Any:
        value = getattr(self._target, attribute)
        if not callable(value):
            return value
        label = f"{self._name}.{attribute}"
        if asyncio.iscoroutinefunction(value):

            async def call_async(*args: Any, **kwargs: Any) -> Any:
                self._log.append((label, args, kwargs))
                return await value(*args, **kwargs)

            return call_async

        def call(*args: Any, **kwargs: Any) -> Any:
            self._log.append((label, args, kwargs))
            return value(*args, **kwargs)

        return call


class FixedLimiter:
    """A limiter that always answers `retry_after` (None = allowed)."""

    def __init__(self, retry_after: float | None) -> None:
        self.retry_after = retry_after

    def try_acquire(self, key: str, *, now: float) -> float | None:
        return self.retry_after


def generous_limiter() -> InMemoryRateLimiter:
    return InMemoryRateLimiter(capacity=1_000_000, refill_per_second=1_000_000.0, max_keys=1_000)


class Ingress:
    """The service over one shared P3-00 fake store, with a spy on every port."""

    def __init__(
        self,
        *,
        preauth_global: Any = None,
        preauth: Any = None,
        account: Any = None,
        receipts: Callable[[KitSubject], Any] | None = None,
        mapping: Callable[[KitSubject], Any] | None = None,
    ) -> None:
        self.subject = KitSubject(
            authenticator=lambda s: FakeConnectorAuthenticator(s.store),
            mapping=lambda s: FakeTrustedMapping(s.store),
            receipts=lambda s: FakeIngressReceiptRepository(s.store),
            queue=lambda s: FakeIngressQueue(s.faults),
            entitlements=lambda s: FakeEntitlementCheck(s.faults),
        )
        self.store = self.subject.store
        self.queue = self.subject.queue
        self.metrics = RecordingTelemetryMetrics()
        self.log: list[tuple[str, tuple, dict]] = []
        self.now = RECEIVED
        self.mono = 1_000.0
        self.issued: list[str] = []
        receipt_port = receipts(self.subject) if receipts else self.subject.receipts
        mapping_port = mapping(self.subject) if mapping else self.subject.mapping
        self.service = IngressService(
            authenticator=Spy("authenticator", self.subject.authenticator, self.log),
            preauth_global_limiter=Spy(
                "preauth_global", preauth_global or generous_limiter(), self.log
            ),
            preauth_limiter=Spy("preauth", preauth or generous_limiter(), self.log),
            account_limiter=Spy("account", account or generous_limiter(), self.log),
            receipts=Spy("receipts", receipt_port, self.log),
            trusted_mapping=Spy("trusted_mapping", mapping_port, self.log),
            entitlements=Spy("entitlements", self.subject.entitlements, self.log),
            producer=Spy("producer", self.queue, self.log),
            metrics=self.metrics,
            clock=lambda: self.now,
            monotonic=lambda: self.mono,
            new_ingress_id=self._next_ingress_id,
        )

    def _next_ingress_id(self) -> str:
        ingress_id = f"ing_{len(self.issued) + 1:032x}"
        self.issued.append(ingress_id)
        return ingress_id

    async def seed(self, *components: Any) -> None:
        await self.subject.seed_account(integration_account())
        await self.subject.seed_credential(credential())
        await self.subject.seed_device_integration(device_integration())
        await self.subject.seed_device(device())
        for item in components or (component(), component(DEFAULT_COMPONENT)):
            await self.subject.seed_component(item)

    def calls(self) -> list[str]:
        return [name for name, _, _ in self.log]

    def calls_of(self, name: str) -> list[tuple[tuple, dict]]:
        return [(args, kwargs) for label, args, kwargs in self.log if label == name]

    def bodies(self) -> list[dict[str, Any]]:
        return [json.loads(stored["body"]) for stored in self.queue.messages.values()]

    def raw_bodies(self) -> list[str]:
        return [stored["body"] for stored in self.queue.messages.values()]

    def receipt(self, key: str = IDEMPOTENCY_KEY) -> IngressReceipt | None:
        return self.store.receipts.get((ACCOUNT, idempotency_key_hash(key)))

    def outcomes(self) -> list[str]:
        return [
            record["labels"]["outcome"]
            for record in self.metrics.records
            if record["metric"] == "ingest_requests_total"
        ]

    def denials(self) -> list[str]:
        return [
            record["labels"]["reason"]
            for record in self.metrics.records
            if record["metric"] == "ingest_admission_denied_total"
        ]

    async def admit(
        self,
        batch: IngestObservationBatch | None = None,
        *,
        key: str = IDEMPOTENCY_KEY,
        connector_version: str | None = CONNECTOR_VERSION,
        source: AuthenticatedSource = SOURCE,
    ) -> IngestAccepted:
        return await self.service.admit(
            source,
            batch or make_batch(),
            idempotency_key=key,
            connector_version=connector_version,
        )


def reading(index: int = 0, **overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "component_id": COMPONENT,
        "metric_key": "temperature",
        "value": 27.5 + index,
        "source_unit": "C",
        "observed_at": OBSERVED.isoformat(),
        "sequence": index + 1,
    }
    payload.update(overrides)
    return {key: value for key, value in payload.items() if value is not None}


def make_batch(*observations: dict[str, Any], external_device_id: str = EXTERNAL_DEVICE) -> Any:
    return IngestObservationBatch.model_validate(
        {
            "external_device_id": external_device_id,
            "observations": list(observations) or [reading(0), reading(1)],
        }
    )


def expected_request_sha256(batch: IngestObservationBatch, connector_version: str | None) -> str:
    return canonical_request_hash(
        {
            "external_device_id": batch.external_device_id,
            "observations": [
                item.model_dump(mode="json", exclude_none=True) for item in batch.observations
            ],
            "connector_version": connector_version,
        }
    )


def expected_envelope(
    batch: IngestObservationBatch,
    *,
    ingress_id: str,
    received_at: datetime = RECEIVED,
    connector_version: str | None = CONNECTOR_VERSION,
    mapping_versions: NormalizationVersions | None = None,
) -> IngressEnvelope:
    observations = []
    for item in batch.observations:
        payload = item.model_dump(mode="json")
        payload["component_id"] = item.component_id or DEFAULT_COMPONENT
        observations.append(payload)
    return IngressEnvelope.model_validate(
        {
            "schema": INGRESS_ENVELOPE_SCHEMA,
            "ingress_id": ingress_id,
            "tenant_id": TENANT,
            "integration_account_id": ACCOUNT,
            "provider_key": PROVIDER,
            "external_identity_hash": IDENTITY_HASH,
            "device_integration_id": INTEGRATION,
            "device_id": DEVICE,
            "source_type": "https",
            "connector_version": connector_version,
            "received_at": received_at,
            "mapping_versions": mapping_versions or NormalizationVersions(),
            "observations": observations,
        }
    )


def stored_receipt(
    batch: IngestObservationBatch,
    *,
    state: IngressReceiptState,
    ingress_id: str = "ing_" + "e" * 32,
    created_at: datetime = RECEIVED - timedelta(hours=1),
    key: str = IDEMPOTENCY_KEY,
    connector_version: str | None = CONNECTOR_VERSION,
    mapping_versions: NormalizationVersions = PINNED,
    request_sha256: str | None = None,
) -> IngressReceipt:
    pending = new_ingress_receipt(
        integration_account_id=ACCOUNT,
        tenant_id=TENANT,
        key_sha256=idempotency_key_hash(key),
        request_sha256=request_sha256 or expected_request_sha256(batch, connector_version),
        ingress_id=ingress_id,
        received_at=created_at,
        mapping_versions=mapping_versions,
        created_at=created_at,
    )
    return pending.model_copy(update={"state": state})


def put_receipt(ingress: Ingress, receipt: IngressReceipt) -> None:
    ingress.store.receipts[(receipt.integration_account_id, receipt.key_sha256)] = receipt


def assert_rejected(
    exc_info: pytest.ExceptionInfo[IngestRejected], expected: tuple[int, Any, dict[str, str]]
) -> None:
    status, detail, headers = expected
    assert exc_info.value.status_code == status
    assert exc_info.value.detail == detail
    assert dict(exc_info.value.headers or {}) == headers


@pytest.fixture
def ingress() -> Ingress:
    return Ingress()


# Steps 1-4: authentication and rate limits --------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "authorization",
    [
        None,
        "",
        "Bearer",
        "Bearer ",
        f"Basic {TOKEN}",
        f"Token {TOKEN}",
        f"Bearer {TOKEN}x",
        f"Bearer  {TOKEN} extra",
        f"Bearer lpi_{CREDENTIAL}_short",
        f"Bearer lpi_ic_{'A' * 32}_{SECRET}",
        TOKEN,
    ],
)
async def test_step1_bad_authorization_is_401_before_any_limiter_or_store(
    ingress: Ingress, authorization: str | None
) -> None:
    await ingress.seed()

    with pytest.raises(IngestRejected) as exc_info:
        await ingress.service.authenticate(authorization)

    assert_rejected(exc_info, UNAUTHORIZED)
    assert ingress.calls() == []
    assert ingress.outcomes() == ["unauthorized"]


@pytest.mark.asyncio
async def test_step1_bearer_scheme_is_case_insensitive(ingress: Ingress) -> None:
    await ingress.seed()

    assert await ingress.service.authenticate(f"bearer {TOKEN}") == SOURCE


@pytest.mark.asyncio
async def test_steps_1_to_4_run_in_order_with_the_injected_monotonic_clock(
    ingress: Ingress,
) -> None:
    await ingress.seed()
    ingress.mono = 4_242.5

    assert await ingress.service.authenticate(BEARER) == SOURCE

    assert ingress.calls() == [
        "preauth_global.try_acquire",
        "preauth.try_acquire",
        "authenticator.authenticate",
        "account.try_acquire",
    ]
    assert ingress.calls_of("preauth_global.try_acquire") == [(("*",), {"now": 4_242.5})]
    assert ingress.calls_of("preauth.try_acquire") == [((CREDENTIAL,), {"now": 4_242.5})]
    assert ingress.calls_of("authenticator.authenticate") == [((TOKEN,), {})]
    assert ingress.calls_of("account.try_acquire") == [((ACCOUNT,), {"now": 4_242.5})]
    assert ingress.outcomes() == []


@pytest.mark.asyncio
@pytest.mark.parametrize(("retry_after", "header"), [(0.01, "1"), (1.0, "1"), (2.5, "3")])
async def test_step2_global_bucket_is_429_before_the_per_ic_bucket_and_the_store(
    retry_after: float, header: str
) -> None:
    ingress = Ingress(preauth_global=FixedLimiter(retry_after))
    await ingress.seed()

    with pytest.raises(IngestRejected) as exc_info:
        await ingress.service.authenticate(BEARER)

    assert_rejected(exc_info, (429, "rate limited", {"Retry-After": header}))
    assert ingress.calls() == ["preauth_global.try_acquire"]
    assert ingress.outcomes() == ["rate_limited"]


@pytest.mark.asyncio
async def test_step2_per_ic_bucket_is_429_before_the_store() -> None:
    ingress = Ingress(preauth=FixedLimiter(7.2))
    await ingress.seed()

    with pytest.raises(IngestRejected) as exc_info:
        await ingress.service.authenticate(BEARER)

    assert_rejected(exc_info, (429, "rate limited", {"Retry-After": "8"}))
    assert ingress.calls() == ["preauth_global.try_acquire", "preauth.try_acquire"]
    assert ingress.outcomes() == ["rate_limited"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    ["unknown_credential", "wrong_secret", "revoked", "account_revoked", "account_missing"],
)
async def test_step3_unauthenticated_is_401_before_the_account_bucket(
    ingress: Ingress, case: str
) -> None:
    await ingress.subject.seed_account(
        integration_account(
            status=IntegrationAccountStatus.REVOKED
            if case == "account_revoked"
            else IntegrationAccountStatus.ACTIVE
        )
    )
    if case == "account_missing":
        ingress.store.accounts.clear()
    if case == "revoked":
        await ingress.subject.seed_credential(
            credential(status="revoked", revoked_at=RECEIVED, version=2)
        )
    elif case != "unknown_credential":
        await ingress.subject.seed_credential(credential())
    token = ingest_token(CREDENTIAL, "Z" * 43) if case == "wrong_secret" else TOKEN

    with pytest.raises(IngestRejected) as exc_info:
        await ingress.service.authenticate(f"Bearer {token}")

    assert_rejected(exc_info, UNAUTHORIZED)
    assert ingress.calls()[-1] == "authenticator.authenticate"
    assert "account.try_acquire" not in ingress.calls()
    assert ingress.outcomes() == ["unauthorized"]


@pytest.mark.asyncio
@pytest.mark.parametrize("error", store_errors(), ids=["client_error", "endpoint"])
@pytest.mark.parametrize(
    "operation", ["authenticate", "get_ingest_credential", "get_integration_account"]
)
async def test_step3_store_failure_is_503_and_fails_closed(
    ingress: Ingress, operation: str, error: BaseException
) -> None:
    await ingress.seed()
    ingress.subject.fail_next(operation, error)

    with pytest.raises(IngestRejected) as exc_info:
        await ingress.service.authenticate(BEARER)

    assert_rejected(exc_info, UNAVAILABLE)
    assert "account.try_acquire" not in ingress.calls()
    assert ingress.outcomes() == ["unavailable"]


@pytest.mark.asyncio
async def test_step4_account_bucket_is_429_after_authentication() -> None:
    ingress = Ingress(account=FixedLimiter(0.4))
    await ingress.seed()

    with pytest.raises(IngestRejected) as exc_info:
        await ingress.service.authenticate(BEARER)

    assert_rejected(exc_info, (429, "rate limited", {"Retry-After": "1"}))
    assert ingress.calls() == [
        "preauth_global.try_acquire",
        "preauth.try_acquire",
        "authenticator.authenticate",
        "account.try_acquire",
    ]
    assert ingress.outcomes() == ["rate_limited"]


@pytest.mark.asyncio
async def test_random_credential_ids_cannot_amplify_store_reads() -> None:
    global_per_minute, per_ic_per_minute = 600, 60
    ingress = Ingress(
        preauth_global=InMemoryRateLimiter(
            capacity=preauth_bucket_capacity(global_per_minute),
            refill_per_second=global_per_minute / 60,
            max_keys=10,
        ),
        preauth=InMemoryRateLimiter(
            capacity=preauth_bucket_capacity(per_ic_per_minute),
            refill_per_second=per_ic_per_minute / 60,
            max_keys=10_000,
        ),
    )
    reads = 0
    original = ingress.store.get_ingest_credential

    def counting_read(ingest_credential_id: str) -> Any:
        nonlocal reads
        reads += 1
        return original(ingest_credential_id)

    ingress.store.get_ingest_credential = counting_read  # type: ignore[method-assign]
    statuses: list[int] = []
    for index in range(10_000):
        ingress.mono = 1_000.0 + index * 0.006  # 10,000 requests spread over one minute
        token = ingest_token(f"ic_{sha256(str(index).encode()).hexdigest()[:32]}", SECRET)
        with pytest.raises(IngestRejected) as exc_info:
            await ingress.service.authenticate(f"Bearer {token}")
        statuses.append(exc_info.value.status_code)

    # Bucket capacity plus one minute of refill bounds the store reads of a whole minute.
    bound = preauth_bucket_capacity(global_per_minute) + global_per_minute
    assert 0 < reads <= bound
    assert statuses.count(401) == reads
    assert statuses.count(429) == 10_000 - reads


# Steps 8-16: the happy path, the envelope and the receipt ---------------------------------


@pytest.mark.asyncio
async def test_fresh_request_is_accepted_after_the_send(ingress: Ingress) -> None:
    await ingress.seed()
    batch = make_batch()

    accepted = await ingress.admit(batch)

    assert accepted == IngestAccepted(
        ingress_id=ingress.issued[0], accepted_observations=2, replayed=False
    )
    assert ingress.calls() == ADMIT_STEPS
    assert ingress.raw_bodies() == [
        expected_envelope(batch, ingress_id=ingress.issued[0]).to_json()
    ]
    assert ingress.receipt() == stored_receipt(
        batch,
        state=IngressReceiptState.ENQUEUED,
        ingress_id=ingress.issued[0],
        created_at=RECEIVED,
        mapping_versions=NormalizationVersions(),
    )
    assert ingress.calls_of("receipts.mark_ingress_receipt_enqueued") == [
        ((ACCOUNT, idempotency_key_hash(IDEMPOTENCY_KEY), ingress.issued[0]), {"at": RECEIVED})
    ]
    assert ingress.calls_of("entitlements.allows") == [((TENANT, ACCOUNT, 2), {})]
    assert ingress.outcomes() == ["accepted"]
    assert {
        "metric": "ingest_observations_admitted_total",
        "value": 2,
        "labels": {},
    } in ingress.metrics.records


@pytest.mark.asyncio
async def test_envelope_fills_the_default_component_and_omits_absent_fields(
    ingress: Ingress,
) -> None:
    await ingress.seed()
    batch = make_batch(reading(0, component_id=None), reading(1))

    accepted = await ingress.admit(batch, connector_version=None)

    (raw,) = ingress.raw_bodies()
    assert (
        raw
        == expected_envelope(
            batch, ingress_id=accepted.ingress_id, connector_version=None
        ).to_json()
    )
    body = json.loads(raw)
    assert [item["component_id"] for item in body["observations"]] == [
        DEFAULT_COMPONENT,
        COMPONENT,
    ]
    assert {"connector_version", "replay", "replay_of"}.isdisjoint(body)
    assert body["mapping_versions"] == NormalizationVersions().model_dump(mode="json")
    assert set(body["mapping_versions"]) == {
        "catalog_version",
        "unit_mapping_version",
        "quality_policy_version",
    }
    assert (body["device_integration_id"], body["device_id"]) == (INTEGRATION, DEVICE)
    assert body["external_identity_hash"] == IDENTITY_HASH
    assert EXTERNAL_DEVICE not in raw
    assert ", " not in raw and ": " not in raw


@pytest.mark.asyncio
async def test_envelope_carries_the_admission_time_mapping_not_the_payload(
    ingress: Ingress,
) -> None:
    await ingress.seed()
    batch = make_batch()

    await ingress.admit(batch)

    (body,) = ingress.bodies()
    assert body["schema"] == "telemetry-ingress@1"
    assert body["tenant_id"] == TENANT
    assert body["integration_account_id"] == ACCOUNT
    assert body["provider_key"] == PROVIDER
    assert body["source_type"] == "https"
    assert body["connector_version"] == CONNECTOR_VERSION
    assert body["received_at"] == "2026-10-30T12:00:00.000000Z"


@pytest.mark.asyncio
async def test_request_hash_is_computed_before_the_default_component_fill(
    ingress: Ingress,
) -> None:
    await ingress.seed()
    omitted = make_batch(reading(0, component_id=None))
    explicit = make_batch(reading(0, component_id=DEFAULT_COMPONENT))

    await ingress.admit(omitted)
    stored = ingress.receipt()

    assert stored is not None
    assert stored.request_sha256 == expected_request_sha256(omitted, CONNECTOR_VERSION)
    with pytest.raises(IngestRejected) as exc_info:
        await ingress.admit(explicit)
    assert_rejected(exc_info, CONFLICT)


@pytest.mark.asyncio
async def test_connector_version_is_part_of_the_request_hash(ingress: Ingress) -> None:
    await ingress.seed()
    await ingress.admit(make_batch(), connector_version=None)

    assert ingress.receipt().request_sha256 == expected_request_sha256(make_batch(), None)
    with pytest.raises(IngestRejected) as exc_info:
        await ingress.admit(make_batch(), connector_version=CONNECTOR_VERSION)
    assert_rejected(exc_info, CONFLICT)


# Receipt replay table --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enqueued_same_request_replays_without_sending(ingress: Ingress) -> None:
    await ingress.seed()
    batch = make_batch()
    first = await ingress.admit(batch)
    ingress.log.clear()
    ingress.now = RECEIVED + timedelta(hours=3)

    again = await ingress.admit(batch)

    assert again == IngestAccepted(
        ingress_id=first.ingress_id, accepted_observations=2, replayed=True
    )
    assert ingress.calls() == ["receipts.get_ingress_receipt"]
    assert len(ingress.raw_bodies()) == 1
    assert ingress.outcomes() == ["accepted", "replayed"]


@pytest.mark.asyncio
@pytest.mark.parametrize("state", list(IngressReceiptState))
async def test_live_receipt_for_a_different_request_is_409(
    ingress: Ingress, state: IngressReceiptState
) -> None:
    await ingress.seed()
    put_receipt(ingress, stored_receipt(make_batch(), state=state, request_sha256="0" * 64))

    with pytest.raises(IngestRejected) as exc_info:
        await ingress.admit(make_batch())

    assert_rejected(exc_info, CONFLICT)
    assert ingress.calls() == ["receipts.get_ingress_receipt"]
    assert ingress.raw_bodies() == []
    assert ingress.outcomes() == ["conflict"]


@pytest.mark.asyncio
async def test_pending_same_request_resumes_with_the_stored_values_and_no_claim(
    ingress: Ingress,
) -> None:
    await ingress.seed()
    batch = make_batch()
    first_received = RECEIVED - timedelta(hours=2)
    pending = stored_receipt(batch, state=IngressReceiptState.PENDING, created_at=first_received)
    put_receipt(ingress, pending)

    accepted = await ingress.admit(batch)

    assert accepted == IngestAccepted(
        ingress_id=pending.ingress_id, accepted_observations=2, replayed=True
    )
    assert "receipts.claim_ingress_receipt" not in ingress.calls()
    assert ingress.calls() == [
        step for step in ADMIT_STEPS if step != "receipts.claim_ingress_receipt"
    ]
    assert ingress.raw_bodies() == [
        expected_envelope(
            batch,
            ingress_id=pending.ingress_id,
            received_at=first_received,
            mapping_versions=PINNED,
        ).to_json()
    ]
    assert ingress.calls_of("receipts.mark_ingress_receipt_enqueued")[0][0][2] == pending.ingress_id
    assert ingress.receipt() == pending.model_copy(update={"state": IngressReceiptState.ENQUEUED})
    assert ingress.issued == []
    assert ingress.outcomes() == ["replayed"]


@pytest.mark.asyncio
async def test_pending_retry_with_a_fresh_denial_is_403_and_sends_nothing(
    ingress: Ingress,
) -> None:
    await ingress.seed()
    await ingress.subject.seed_device_integration(
        device_integration(status=DeviceIntegrationStatus.DECOMMISSIONED, version=2)
    )
    pending = stored_receipt(make_batch(), state=IngressReceiptState.PENDING)
    put_receipt(ingress, pending)

    with pytest.raises(IngestRejected) as exc_info:
        await ingress.admit(make_batch())

    assert_rejected(exc_info, FORBIDDEN)
    assert ingress.raw_bodies() == []
    assert ingress.receipt() == pending


@pytest.mark.asyncio
async def test_failed_send_leaves_the_receipt_pending_and_the_retry_keeps_received_at(
    ingress: Ingress,
) -> None:
    await ingress.seed()
    batch = make_batch()
    ingress.subject.fail_next("send", IngressQueueError("down", retryable=True))

    with pytest.raises(IngestRejected) as exc_info:
        await ingress.admit(batch)

    assert_rejected(exc_info, UNAVAILABLE)
    first = ingress.receipt()
    assert first is not None and first.state == IngressReceiptState.PENDING
    assert ingress.raw_bodies() == []
    assert ingress.outcomes() == ["unavailable"]

    ingress.log.clear()
    ingress.now = RECEIVED + timedelta(minutes=10)
    retried = await ingress.admit(batch)

    assert retried == IngestAccepted(
        ingress_id=first.ingress_id, accepted_observations=2, replayed=True
    )
    assert "receipts.claim_ingress_receipt" not in ingress.calls()
    assert ingress.raw_bodies() == [
        expected_envelope(batch, ingress_id=first.ingress_id, received_at=RECEIVED).to_json()
    ]
    assert ingress.receipt().state == IngressReceiptState.ENQUEUED


@pytest.mark.asyncio
@pytest.mark.parametrize("retryable", [True, False])
async def test_any_queue_error_is_503_never_202(ingress: Ingress, retryable: bool) -> None:
    await ingress.seed()
    ingress.subject.fail_next("send", IngressQueueError("down", retryable=retryable))

    with pytest.raises(IngestRejected) as exc_info:
        await ingress.admit()

    assert_rejected(exc_info, UNAVAILABLE)
    assert "receipts.mark_ingress_receipt_enqueued" not in ingress.calls()


@pytest.mark.asyncio
async def test_mark_failure_after_a_send_is_still_202_and_the_retry_resends_the_same_ids(
    ingress: Ingress,
) -> None:
    await ingress.seed()
    batch = make_batch()
    ingress.subject.fail_next("mark_ingress_receipt_enqueued", store_errors()[0])

    first = await ingress.admit(batch)

    assert first == IngestAccepted(
        ingress_id=ingress.issued[0], accepted_observations=2, replayed=False
    )
    assert ingress.receipt().state == IngressReceiptState.PENDING
    assert ingress.outcomes() == ["accepted"]

    ingress.now = RECEIVED + timedelta(minutes=5)
    retried = await ingress.admit(batch)

    assert retried == IngestAccepted(
        ingress_id=first.ingress_id, accepted_observations=2, replayed=True
    )
    expected = expected_envelope(batch, ingress_id=first.ingress_id, received_at=RECEIVED)
    assert ingress.raw_bodies() == [expected.to_json(), expected.to_json()]
    assert ingress.receipt().state == IngressReceiptState.ENQUEUED
    assert ingress.issued == [first.ingress_id]


@pytest.mark.asyncio
async def test_logical_expiry_uses_one_predicate_at_steps_8_and_13(ingress: Ingress) -> None:
    await ingress.seed()
    batch = make_batch()
    created = RECEIVED - timedelta(hours=24)
    old = stored_receipt(
        batch, state=IngressReceiptState.ENQUEUED, created_at=created, request_sha256="0" * 64
    )

    # One microsecond before expiry the receipt is live: a different request conflicts.
    put_receipt(ingress, old)
    ingress.now = RECEIVED - timedelta(microseconds=1)
    with pytest.raises(IngestRejected) as exc_info:
        await ingress.admit(batch)
    assert_rejected(exc_info, CONFLICT)

    # At exactly created_at + 24 h it is expired at step 8 AND claimable at step 13.
    ingress.log.clear()
    ingress.now = RECEIVED
    accepted = await ingress.admit(batch)

    assert accepted == IngestAccepted(
        ingress_id=ingress.issued[0], accepted_observations=2, replayed=False
    )
    assert ingress.calls().count("receipts.claim_ingress_receipt") == 1
    assert ingress.receipt().ingress_id == ingress.issued[0]
    assert ingress.receipt().created_at == RECEIVED


@pytest.mark.asyncio
async def test_expired_same_request_is_admitted_again_with_a_new_received_at(
    ingress: Ingress,
) -> None:
    await ingress.seed()
    batch = make_batch()
    put_receipt(
        ingress,
        stored_receipt(
            batch, state=IngressReceiptState.PENDING, created_at=RECEIVED - timedelta(hours=24)
        ),
    )

    accepted = await ingress.admit(batch)

    assert accepted.replayed is False
    assert accepted.ingress_id == ingress.issued[0]
    assert ingress.bodies()[0]["received_at"] == "2026-10-30T12:00:00.000000Z"
    assert ingress.bodies()[0]["mapping_versions"] == NormalizationVersions().model_dump(
        mode="json"
    )


class RacingReceipts:
    """Step 8 sees no receipt; every claim then returns `winners` in order (a lost race)."""

    def __init__(self, inner: FakeIngressReceiptRepository, winners: list[IngressReceipt]):
        self.inner = inner
        self.winners = list(winners)

    async def get_ingress_receipt(self, integration_account_id: str, key_sha256: str) -> Any:
        return None

    async def claim_ingress_receipt(self, receipt: IngressReceipt, *, now: datetime) -> Any:
        winner = self.winners.pop(0) if self.winners else receipt
        self.inner.store.receipts[(winner.integration_account_id, winner.key_sha256)] = winner
        return winner

    async def mark_ingress_receipt_enqueued(self, *args: Any, **kwargs: Any) -> None:
        await self.inner.mark_ingress_receipt_enqueued(*args, **kwargs)


def racing(*winners: IngressReceipt) -> Ingress:
    return Ingress(receipts=lambda s: RacingReceipts(s.receipts, list(winners)))


@pytest.mark.asyncio
async def test_claim_race_lost_to_a_pending_same_request_uses_the_winners_values() -> None:
    batch = make_batch()
    winner = stored_receipt(batch, state=IngressReceiptState.PENDING, created_at=RECEIVED)
    ingress = racing(winner)
    await ingress.seed()

    accepted = await ingress.admit(batch)

    assert accepted == IngestAccepted(
        ingress_id=winner.ingress_id, accepted_observations=2, replayed=True
    )
    assert ingress.calls().count("receipts.claim_ingress_receipt") == 1
    assert ingress.raw_bodies() == [
        expected_envelope(batch, ingress_id=winner.ingress_id, mapping_versions=PINNED).to_json()
    ]
    assert ingress.calls_of("receipts.mark_ingress_receipt_enqueued")[0][0][2] == winner.ingress_id


@pytest.mark.asyncio
async def test_claim_race_lost_to_an_enqueued_same_request_replays() -> None:
    batch = make_batch()
    winner = stored_receipt(batch, state=IngressReceiptState.ENQUEUED, created_at=RECEIVED)
    ingress = racing(winner)
    await ingress.seed()

    accepted = await ingress.admit(batch)

    assert accepted == IngestAccepted(
        ingress_id=winner.ingress_id, accepted_observations=2, replayed=True
    )
    assert ingress.raw_bodies() == []
    assert "receipts.mark_ingress_receipt_enqueued" not in ingress.calls()


@pytest.mark.asyncio
async def test_claim_race_lost_to_a_different_request_is_409() -> None:
    batch = make_batch()
    winner = stored_receipt(
        batch, state=IngressReceiptState.PENDING, created_at=RECEIVED, request_sha256="0" * 64
    )
    ingress = racing(winner)
    await ingress.seed()

    with pytest.raises(IngestRejected) as exc_info:
        await ingress.admit(batch)

    assert_rejected(exc_info, CONFLICT)
    assert ingress.raw_bodies() == []


@pytest.mark.asyncio
async def test_claim_race_lost_twice_is_503_without_a_loop() -> None:
    batch = make_batch()
    stale = stored_receipt(
        batch, state=IngressReceiptState.PENDING, created_at=RECEIVED - timedelta(hours=30)
    )
    ingress = racing(stale, stale, stale, stale)
    await ingress.seed()

    with pytest.raises(IngestRejected) as exc_info:
        await ingress.admit(batch)

    assert_rejected(exc_info, UNAVAILABLE)
    assert ingress.calls().count("receipts.claim_ingress_receipt") == 2
    assert ingress.raw_bodies() == []


class BarrierReceipts:
    """Holds every step-8 read until `parties` requests have read, so they race to claim."""

    def __init__(self, inner: FakeIngressReceiptRepository, parties: int) -> None:
        self.inner = inner
        self.barrier = asyncio.Barrier(parties)

    async def get_ingress_receipt(self, integration_account_id: str, key_sha256: str) -> Any:
        found = await self.inner.get_ingress_receipt(integration_account_id, key_sha256)
        await self.barrier.wait()
        return found

    async def claim_ingress_receipt(self, receipt: IngressReceipt, *, now: datetime) -> Any:
        return await self.inner.claim_ingress_receipt(receipt, now=now)

    async def mark_ingress_receipt_enqueued(self, *args: Any, **kwargs: Any) -> None:
        await self.inner.mark_ingress_receipt_enqueued(*args, **kwargs)


@pytest.mark.asyncio
async def test_two_concurrent_same_key_requests_share_one_ingress_id() -> None:
    ingress = Ingress(receipts=lambda s: BarrierReceipts(s.receipts, 2))
    await ingress.seed()
    batch = make_batch()

    first, second = await asyncio.wait_for(
        asyncio.gather(ingress.admit(batch), ingress.admit(batch)), timeout=5
    )

    assert first.ingress_id == second.ingress_id
    assert sorted([first.replayed, second.replayed]) == [False, True]
    assert ingress.calls().count("receipts.get_ingress_receipt") == 2
    assert len(ingress.store.receipts) == 1
    bodies = ingress.bodies()
    assert 1 <= len(bodies) <= 2
    assert {(body["ingress_id"], body["received_at"]) for body in bodies} == {
        (first.ingress_id, "2026-10-30T12:00:00.000000Z")
    }
    assert ingress.receipt().state == IngressReceiptState.ENQUEUED


# Steps 9-12: admission, components, entitlement and the size guard --------------------------


async def _seed_denial(ingress: Ingress, case: str) -> None:
    await ingress.seed()
    if case == "unknown_identity":
        ingress.store.external_ids.clear()
    elif case == "foreign_account_identity":
        await ingress.subject.seed_device_integration(
            device_integration(integration_account_id=OTHER_ACCOUNT)
        )
    elif case == "foreign_tenant_identity":
        await ingress.subject.seed_device_integration(device_integration(tenant_id=OTHER_TENANT))
    elif case == "integration_inactive":
        await ingress.subject.seed_device_integration(
            device_integration(status=DeviceIntegrationStatus.ERROR, version=2)
        )
    elif case == "device_inactive":
        await ingress.subject.seed_device(device(status=DeviceStatus.SUSPENDED, version=2))
    elif case == "account_inactive":
        await ingress.subject.seed_account(
            integration_account(status=IntegrationAccountStatus.ERROR, version=2)
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "reason"),
    [
        ("unknown_identity", IngressAdmissionDenied.IDENTITY_UNRESOLVED),
        ("foreign_account_identity", IngressAdmissionDenied.INTEGRATION_MISMATCH),
        ("foreign_tenant_identity", IngressAdmissionDenied.INTEGRATION_MISMATCH),
        ("integration_inactive", IngressAdmissionDenied.INTEGRATION_INACTIVE),
        ("device_inactive", IngressAdmissionDenied.DEVICE_INACTIVE),
        ("account_inactive", IngressAdmissionDenied.ACCOUNT_INACTIVE),
    ],
)
async def test_step9_denials_share_one_403_and_leave_no_receipt(
    ingress: Ingress, case: str, reason: IngressAdmissionDenied
) -> None:
    await _seed_denial(ingress, case)

    with pytest.raises(IngestRejected) as exc_info:
        await ingress.admit()

    assert_rejected(exc_info, FORBIDDEN)
    assert ingress.calls() == ["receipts.get_ingress_receipt", "trusted_mapping.admit"]
    assert ingress.store.receipts == {}
    assert ingress.raw_bodies() == []
    assert ingress.outcomes() == ["forbidden"]
    assert ingress.denials() == [reason.value]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("observations", "components", "expected", "reason"),
    [
        (
            [reading(0), reading(1, component_id=None)],
            (component(),),
            {
                "loc": ["body", "observations", 1, "component_id"],
                "msg": "component_id is required for this device",
                "type": "missing",
            },
            IngressAdmissionDenied.COMPONENT_REQUIRED,
        ),
        (
            [reading(0), reading(1), reading(2, component_id=OTHER_COMPONENT)],
            (component(), component(DEFAULT_COMPONENT)),
            {
                "loc": ["body", "observations", 2, "component_id"],
                "msg": "component_id is not valid for this device",
                "type": "value_error",
            },
            IngressAdmissionDenied.COMPONENT_INVALID,
        ),
    ],
    ids=["required", "invalid"],
)
async def test_step10_component_rule_is_a_fixed_422(
    ingress: Ingress,
    observations: list[dict[str, Any]],
    components: tuple[Any, ...],
    expected: dict[str, Any],
    reason: IngressAdmissionDenied,
) -> None:
    await ingress.seed(*components)

    with pytest.raises(IngestRejected) as exc_info:
        await ingress.admit(make_batch(*observations))

    assert_rejected(exc_info, (422, [expected], {}))
    assert ingress.calls() == ["receipts.get_ingress_receipt", "trusted_mapping.admit"]
    assert ingress.store.receipts == {}
    assert ingress.outcomes() == ["invalid"]
    assert ingress.denials() == [reason.value]


@pytest.mark.asyncio
async def test_step11_entitlement_denial_is_403_before_the_claim(ingress: Ingress) -> None:
    await ingress.seed()
    ingress.subject.entitlements.allow = False

    with pytest.raises(IngestRejected) as exc_info:
        await ingress.admit()

    assert_rejected(exc_info, FORBIDDEN)
    assert ingress.calls() == [
        "receipts.get_ingress_receipt",
        "trusted_mapping.admit",
        "entitlements.allows",
    ]
    assert ingress.store.receipts == {}
    assert ingress.raw_bodies() == []
    assert ingress.outcomes() == ["forbidden"]
    assert ingress.denials() == [IngressAdmissionDenied.ENTITLEMENT_DENIED.value]


class PaddingMapping:
    """Admits like the fake, then pads the resolved observations past the queue size guard."""

    def __init__(self, inner: FakeTrustedMapping) -> None:
        self.inner = inner

    async def admit(self, source: AuthenticatedSource, batch: IngestObservationBatch) -> Any:
        admission = await self.inner.admit(source, batch)
        assert isinstance(admission, IngressAdmission)
        return admission.model_copy(update={"observations": oversized_envelope().observations})


@pytest.mark.asyncio
async def test_step12_size_guard_is_413_before_the_claim_and_leaves_no_receipt() -> None:
    ingress = Ingress(mapping=lambda s: PaddingMapping(s.mapping))
    await ingress.seed()
    assert ingress_message_bytes(oversized_envelope()) > MAX_INGRESS_MESSAGE_BYTES

    with pytest.raises(IngestRejected) as exc_info:
        await ingress.admit()

    assert_rejected(exc_info, TOO_LARGE)
    assert ingress.calls() == [
        "receipts.get_ingress_receipt",
        "trusted_mapping.admit",
        "entitlements.allows",
    ]
    assert ingress.store.receipts == {}
    assert ingress.raw_bodies() == []
    assert ingress.outcomes() == ["too_large"]


@pytest.mark.asyncio
async def test_a_producer_size_refusal_is_413_never_202(ingress: Ingress) -> None:
    await ingress.seed()
    ingress.subject.fail_next("send", IngressMessageTooLarge("too large"))

    with pytest.raises(IngestRejected) as exc_info:
        await ingress.admit()

    assert_rejected(exc_info, TOO_LARGE)
    assert ingress.raw_bodies() == []


# Store failures and G1 -------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("error", store_errors(), ids=["client_error", "endpoint"])
@pytest.mark.parametrize(
    "operation",
    [
        "get_ingress_receipt",
        "admit",
        "resolve_external_identity",
        "get_integration_account",
        "get_device",
        "get_component",
        "allows",
        "claim_ingress_receipt",
    ],
)
async def test_store_failures_before_the_send_are_503_and_send_nothing(
    ingress: Ingress, operation: str, error: BaseException
) -> None:
    await ingress.seed()
    ingress.subject.fail_next(operation, error)

    with pytest.raises(IngestRejected) as exc_info:
        await ingress.admit()

    assert_rejected(exc_info, UNAVAILABLE)
    assert ingress.raw_bodies() == []
    assert "producer.send" not in ingress.calls()
    assert ingress.outcomes() == ["unavailable"]
    assert ingress.denials() == []
    if operation != "claim_ingress_receipt":
        assert ingress.store.receipts == {}


@pytest.mark.asyncio
async def test_a_succeeding_producer_yields_202_exactly_once_per_request(
    ingress: Ingress,
) -> None:
    await ingress.seed()
    results = []
    for index in range(5):
        results.append(await ingress.admit(make_batch(reading(index)), key=f"idem-key-{index:04d}"))
        results.append(await ingress.admit(make_batch(reading(index)), key=f"idem-key-{index:04d}"))

    fresh = [result for result in results if not result.replayed]
    assert len(fresh) == 5
    assert len(ingress.raw_bodies()) == 5
    assert sorted(body["ingress_id"] for body in ingress.bodies()) == sorted(
        result.ingress_id for result in fresh
    )


# Redaction ---------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_logs_metrics_bodies_and_receipts_never_carry_secrets_or_raw_ids(
    ingress: Ingress, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    await ingress.seed()
    batch = make_batch()
    ingress.subject.fail_next("mark_ingress_receipt_enqueued", store_errors()[0])
    await ingress.admit(batch)
    await ingress.admit(batch)
    for operation in ("get_ingress_receipt", "admit", "claim_ingress_receipt", "send"):
        error = (
            IngressQueueError("down", retryable=True) if operation == "send" else store_errors()[1]
        )
        ingress.subject.fail_next(operation, error)
        with pytest.raises(IngestRejected):
            await ingress.admit(batch, key=OTHER_KEY)
    for authorization in (BEARER + "x", f"Bearer {ingest_token(CREDENTIAL, 'Q' * 43)}"):
        with pytest.raises(IngestRejected):
            await ingress.service.authenticate(authorization)
    ingress.subject.fail_next("authenticate", store_errors()[0])
    with pytest.raises(IngestRejected):
        await ingress.service.authenticate(BEARER)
    with pytest.raises(IngestRejected):
        await ingress.admit(make_batch(reading(9)))

    surfaces = {
        "logs": caplog.text,
        "metrics": json.dumps(ingress.metrics.records),
        "bodies": "\n".join(ingress.raw_bodies()),
        "receipts": "\n".join(item.model_dump_json() for item in ingress.store.receipts.values()),
    }
    secrets = {
        "token": TOKEN,
        "secret": SECRET,
        "key": IDEMPOTENCY_KEY,
        "other_key": OTHER_KEY,
        "external_device_id": EXTERNAL_DEVICE,
    }
    for surface, text in surfaces.items():
        for name, secret in secrets.items():
            assert secret not in text, f"{name} leaked into {surface}"


# Entitlement hook (D13) ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_allow_direct_ingress_entitlements_allows_everything() -> None:
    class Subject:
        check = AllowDirectIngressEntitlements()

    assert isinstance(Subject.check, IngressEntitlementCheck)
    await entitlement_default_allows(Subject())
    assert await Subject.check.allows(TENANT, ACCOUNT, 0) is True


def test_service_rejection_carries_the_fixed_fields() -> None:
    rejection = IngestRejected(
        429, "rate limited", outcome="rate_limited", headers={"Retry-After": "1"}
    )

    assert (rejection.status_code, rejection.detail, rejection.outcome) == (
        429,
        "rate limited",
        "rate_limited",
    )
    assert rejection.headers == {"Retry-After": "1"}
