"""HTTPS ingress admission (P3 D2-D6, D13; ADR-016): the frozen route order and replay table.

`IngressService.authenticate` runs route steps 1-4: the token grammar, the process-global and
per-`ic_` pre-auth buckets (no store read before them), the authenticator, then the
per-account bucket. `IngressService.admit` runs steps 8-16 on a parsed batch: the receipt
lookup, trusted admission, the component rule, the entitlement hook, the envelope and its size
guard, the receipt claim, the SQS send and the mark. The router owns steps 5-7 (media type,
bounded body, redacted validation) and reports them through `reject`.

`202 Accepted` is answered only after the queue write succeeds (G1); a failed mark after a
successful send is logged and still answered 202. A store failure fails closed with 503. Each
request reads the wall clock once, so steps 8 and 13 judge liveness at the same instant, and
`received_at`/`created_at` are that instant. Nothing here logs or emits a token, a raw
Idempotency-Key or a raw `external_device_id`: logs carry error categories only.
"""

import logging
import math
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from limnopulse_api.adapters.dynamodb_v2 import canonical_request_hash, idempotency_key_hash
from limnopulse_api.domain.ids import new_ingress_id as default_new_ingress_id
from limnopulse_api.domain.ingestion import (
    INGRESS_ENVELOPE_SCHEMA,
    MAX_INGRESS_MESSAGE_BYTES,
    AuthenticatedSource,
    IngestErrorDetail,
    IngestObservationBatch,
    IngressAdmission,
    IngressAdmissionDenied,
    IngressDenial,
    IngressEnvelope,
    IngressReceipt,
    IngressReceiptAction,
    component_denial_detail,
    decide_receipt_replay,
    ingress_message_bytes,
    new_ingress_receipt,
    parse_ingest_token,
)
from limnopulse_api.domain.telemetry_v2 import NormalizationVersions, SourceType
from limnopulse_api.repositories.ingestion import (
    ConnectorAuthenticator,
    IngressEntitlementCheck,
    IngressQueueProducer,
    IngressRateLimiter,
    IngressReceiptRepository,
    TelemetryMetrics,
    TrustedMapping,
)
from limnopulse_api.services.ingress_rate_limit import PREAUTH_GLOBAL_KEY

logger = logging.getLogger(__name__)

_REQUESTS = "ingest_requests_total"
_DENIED = "ingest_admission_denied_total"
_ADMITTED = "ingest_observations_admitted_total"
_COMPONENT_DENIALS = frozenset(
    {IngressAdmissionDenied.COMPONENT_REQUIRED, IngressAdmissionDenied.COMPONENT_INVALID}
)
_CLAIM_ATTEMPTS = 2  # a lost race re-reads once (the claim returns the winner); then 503


class IngestRejected(Exception):
    """A fixed route-order rejection: status, body `detail`, headers and the metric outcome."""

    def __init__(
        self,
        status_code: int,
        detail: str | list[dict[str, Any]],
        *,
        outcome: str,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(outcome)
        self.status_code = status_code
        self.detail = detail
        self.outcome = outcome
        self.headers = headers or {}


@dataclass(frozen=True)
class IngestAccepted:
    ingress_id: str
    accepted_observations: int
    replayed: bool


def ingest_request_sha256(batch: IngestObservationBatch, connector_version: str | None) -> str:
    """D5: canonical hash of the client request, taken before the default component fill."""
    return canonical_request_hash(
        {
            "external_device_id": batch.external_device_id,
            "observations": [
                item.model_dump(mode="json", exclude_none=True) for item in batch.observations
            ],
            "connector_version": connector_version,
        }
    )


def _bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer":
        return None
    return token.lstrip(" ") or None


class IngressService:
    def __init__(
        self,
        *,
        authenticator: ConnectorAuthenticator,
        preauth_global_limiter: IngressRateLimiter,
        preauth_limiter: IngressRateLimiter,
        account_limiter: IngressRateLimiter,
        receipts: IngressReceiptRepository,
        trusted_mapping: TrustedMapping,
        entitlements: IngressEntitlementCheck,
        producer: IngressQueueProducer,
        metrics: TelemetryMetrics,
        clock: Callable[[], datetime],
        monotonic: Callable[[], float],
        new_ingress_id: Callable[[], str] = default_new_ingress_id,
    ) -> None:
        self.authenticator = authenticator
        self.preauth_global_limiter = preauth_global_limiter
        self.preauth_limiter = preauth_limiter
        self.account_limiter = account_limiter
        self.receipts = receipts
        self.trusted_mapping = trusted_mapping
        self.entitlements = entitlements
        self.producer = producer
        self.metrics = metrics
        self.clock = clock
        self.monotonic = monotonic
        self.new_ingress_id = new_ingress_id

    # Rejections ------------------------------------------------------------------------------

    def reject(
        self,
        status_code: int,
        detail: str | list[dict[str, Any]],
        *,
        outcome: str,
        headers: dict[str, str] | None = None,
    ) -> IngestRejected:
        """Count the outcome and build the rejection; the caller raises it."""
        self.metrics.increment(_REQUESTS, outcome=outcome)
        return IngestRejected(status_code, detail, outcome=outcome, headers=headers)

    def _unauthorized(self) -> IngestRejected:
        return self.reject(
            401,
            IngestErrorDetail.UNAUTHORIZED.value,
            outcome="unauthorized",
            headers={"WWW-Authenticate": "Bearer"},
        )

    def _unavailable(self, step: str, error: BaseException) -> IngestRejected:
        logger.warning("ingest %s failed: %s", step, type(error).__name__)
        return self.reject(503, IngestErrorDetail.UNAVAILABLE.value, outcome="unavailable")

    def _denied(self, reason: IngressAdmissionDenied, detail: Any, status: int) -> IngestRejected:
        self.metrics.increment(_DENIED, reason=reason.value)
        outcome = "invalid" if status == 422 else "forbidden"
        return self.reject(status, detail, outcome=outcome)

    def _limit(self, limiter: IngressRateLimiter, key: str, now: float) -> None:
        retry_after = limiter.try_acquire(key, now=now)
        if retry_after is not None:
            raise self.reject(
                429,
                IngestErrorDetail.RATE_LIMITED.value,
                outcome="rate_limited",
                headers={"Retry-After": str(max(1, math.ceil(retry_after)))},
            )

    # Steps 1-4 ------------------------------------------------------------------------------

    async def authenticate(self, authorization: str | None) -> AuthenticatedSource:
        token = _bearer_token(authorization)
        parsed = None if token is None else parse_ingest_token(token)
        if token is None or parsed is None:
            raise self._unauthorized()
        now = self.monotonic()
        self._limit(self.preauth_global_limiter, PREAUTH_GLOBAL_KEY, now)
        self._limit(self.preauth_limiter, parsed[0], now)
        try:
            source = await self.authenticator.authenticate(token)
        except Exception as error:  # noqa: BLE001 - a store failure fails closed (503)
            raise self._unavailable("authentication", error) from None
        if source is None:
            raise self._unauthorized()
        self._limit(self.account_limiter, source.integration_account_id, now)
        return source

    # Steps 8-16 -----------------------------------------------------------------------------

    async def admit(
        self,
        source: AuthenticatedSource,
        batch: IngestObservationBatch,
        *,
        idempotency_key: str,
        connector_version: str | None,
    ) -> IngestAccepted:
        now = self.clock()
        account_id = source.integration_account_id
        key_sha256 = idempotency_key_hash(idempotency_key)
        request_sha256 = ingest_request_sha256(batch, connector_version)
        count = len(batch.observations)
        try:
            receipt = await self.receipts.get_ingress_receipt(account_id, key_sha256)
        except Exception as error:  # noqa: BLE001 - a store failure fails closed (503)
            raise self._unavailable("receipt lookup", error) from None
        action = decide_receipt_replay(receipt, request_sha256=request_sha256, now=now)
        if action == IngressReceiptAction.CONFLICT:
            raise self._conflict()
        if action == IngressReceiptAction.REPLAY:
            return self._replayed(receipt.ingress_id, count)

        admission = await self._admission(source, batch)
        await self._check_entitlement(source, count)
        if action == IngressReceiptAction.RESUME:
            envelope = self._guarded_envelope(admission, connector_version, receipt)
            return await self._send(envelope, key_sha256, count, now=now, replayed=True)

        mapping_versions = NormalizationVersions()  # admission pins the normalizer defaults
        claim = new_ingress_receipt(
            integration_account_id=account_id,
            tenant_id=source.tenant_id,
            key_sha256=key_sha256,
            request_sha256=request_sha256,
            ingress_id=self.new_ingress_id(),
            received_at=now,
            mapping_versions=mapping_versions,
            created_at=now,
        )
        envelope = self._guarded_envelope(admission, connector_version, claim)  # before claim
        for _ in range(_CLAIM_ATTEMPTS):
            try:
                winner = await self.receipts.claim_ingress_receipt(claim, now=now)
            except Exception as error:  # noqa: BLE001 - a store failure fails closed (503)
                raise self._unavailable("receipt claim", error) from None
            if winner.ingress_id == claim.ingress_id:
                return await self._send(envelope, key_sha256, count, now=now, replayed=False)
            action = decide_receipt_replay(winner, request_sha256=request_sha256, now=now)
            if action == IngressReceiptAction.CONFLICT:
                raise self._conflict()
            if action == IngressReceiptAction.REPLAY:
                return self._replayed(winner.ingress_id, count)
            if action == IngressReceiptAction.RESUME:
                resumed = self._guarded_envelope(admission, connector_version, winner)
                return await self._send(resumed, key_sha256, count, now=now, replayed=True)
        logger.warning("ingest receipt claim lost twice")
        raise self.reject(503, IngestErrorDetail.UNAVAILABLE.value, outcome="unavailable")

    def _conflict(self) -> IngestRejected:
        return self.reject(409, IngestErrorDetail.IDEMPOTENCY_CONFLICT.value, outcome="conflict")

    def _replayed(self, ingress_id: str, count: int) -> IngestAccepted:
        self.metrics.increment(_REQUESTS, outcome="replayed")
        return IngestAccepted(ingress_id=ingress_id, accepted_observations=count, replayed=True)

    async def _admission(
        self, source: AuthenticatedSource, batch: IngestObservationBatch
    ) -> IngressAdmission:
        try:
            result = await self.trusted_mapping.admit(source, batch)
        except Exception as error:  # noqa: BLE001 - a store failure fails closed (503)
            raise self._unavailable("admission", error) from None
        if isinstance(result, IngressDenial):
            if result.reason in _COMPONENT_DENIALS:
                raise self._denied(result.reason, component_denial_detail(result), 422)
            raise self._denied(result.reason, IngestErrorDetail.FORBIDDEN.value, 403)
        return result

    async def _check_entitlement(self, source: AuthenticatedSource, count: int) -> None:
        try:
            allowed = await self.entitlements.allows(
                source.tenant_id, source.integration_account_id, count
            )
        except Exception as error:  # noqa: BLE001 - a store failure fails closed (503)
            raise self._unavailable("entitlement check", error) from None
        if not allowed:
            raise self._denied(
                IngressAdmissionDenied.ENTITLEMENT_DENIED, IngestErrorDetail.FORBIDDEN.value, 403
            )

    def _guarded_envelope(
        self,
        admission: IngressAdmission,
        connector_version: str | None,
        receipt: IngressReceipt,
    ) -> IngressEnvelope:
        """Step 12: the envelope with the receipt's id, first `received_at` and pinned mapping
        versions; over the D6 size guard it is a 413, before any claim or send."""
        integration = admission.device_integration
        envelope = IngressEnvelope.model_validate(
            {
                "schema": INGRESS_ENVELOPE_SCHEMA,
                "ingress_id": receipt.ingress_id,
                "tenant_id": admission.source.tenant_id,
                "integration_account_id": admission.source.integration_account_id,
                "provider_key": integration.provider_key,
                "external_identity_hash": integration.external_identity_hash,
                "device_integration_id": integration.device_integration_id,
                "device_id": integration.device_id,
                "source_type": SourceType.HTTPS,
                "connector_version": connector_version,
                "received_at": receipt.received_at,
                "mapping_versions": receipt.mapping_versions,
                "observations": admission.observations,
            }
        )
        if ingress_message_bytes(envelope) > MAX_INGRESS_MESSAGE_BYTES:
            raise self.reject(413, IngestErrorDetail.TOO_LARGE.value, outcome="too_large")
        return envelope

    async def _send(
        self,
        envelope: IngressEnvelope,
        key_sha256: str,
        count: int,
        *,
        now: datetime,
        replayed: bool,
    ) -> IngestAccepted:
        try:
            await self.producer.send(envelope)
        except Exception as error:  # noqa: BLE001 - never 202 without the write; stays pending
            raise self._unavailable("queue send", error) from None
        try:
            await self.receipts.mark_ingress_receipt_enqueued(
                envelope.integration_account_id, key_sha256, envelope.ingress_id, at=now
            )
        except Exception as error:  # noqa: BLE001 - the message is durable: still 202 (G1)
            logger.warning("ingest receipt mark failed after the send: %s", type(error).__name__)
        self.metrics.increment(_REQUESTS, outcome="replayed" if replayed else "accepted")
        self.metrics.increment(_ADMITTED, count)
        return IngestAccepted(
            ingress_id=envelope.ingress_id, accepted_observations=count, replayed=replayed
        )
