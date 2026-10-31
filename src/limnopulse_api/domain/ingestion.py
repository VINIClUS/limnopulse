"""P3 ingestion contract spine: versions, limits, models and the frozen pure builders.

Everything here is shared vocabulary for the HTTPS ingress, the ingest worker, the quarantine
and state stores and the operator CLIs. A payload never carries tenant, site, asset or
deployment authority: tenant and account come from the ingest credential, the Device from the
trusted DeviceIntegration mapping.
"""

import json
import math
import random as _random
import re
from collections import Counter
from collections.abc import Callable, Collection, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from hashlib import sha256
from typing import Annotated, Any, Literal
from uuid import uuid4

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, StrictBool, model_validator

from limnopulse_api.domain.alerts import AuditContext
from limnopulse_api.domain.devices_v2 import DeviceStatus, DeviceV2
from limnopulse_api.domain.ids import quarantine_entry_id, require_canonical_id
from limnopulse_api.domain.integrations import (
    DeviceIntegration,
    DeviceIntegrationStatus,
    IntegrationAccount,
    IntegrationAccountStatus,
)
from limnopulse_api.domain.telemetry_v2 import (
    ComponentId,
    IntegrationId,
    MetricKey,
    NormalizationVersions,
    QualityFlag,
    QuarantinedObservation,
    QuarantineReason,
    SequenceNumber,
    SourceEventId,
    SourceMetadata,
    SourceObservation,
    SourceType,
    SourceValue,
    TelemetryDatetime,
    TrustedSourceContext,
    UnitCode,
)
from limnopulse_api.domain.v2_common import (
    EXTERNAL_IDENTITY_MAX_LENGTH,
    EXTERNAL_IDENTITY_PATTERN,
    SHA256_HEX_PATTERN,
    DeviceId,
    ProviderKey,
    SafeId,
    Sha256Hex,
    V2VersionedEntity,
    format_utc_timestamp,
    require_utc,
)

# Versions and limits ---------------------------------------------------------------------------

INGRESS_ENVELOPE_SCHEMA = "telemetry-ingress@1"
HTTPS_PROVIDER_KEYS = frozenset({"https_direct"})
MAX_INGEST_BODY_BYTES = 163_840  # 160 KiB, bounded streaming read
MAX_INGEST_OBSERVATIONS = 1_000  # == MAX_OBSERVATIONS_PER_BATCH (P2)
MAX_INGEST_DISTINCT_COMPONENTS = 32  # bounded GetItem reads at admission
MAX_INGRESS_MESSAGE_BYTES = 262_144  # UTF-8 body + attribute bytes; stricter than SQS's 1 MiB
INGRESS_SCHEMA_ATTRIBUTE_BYTES = 31  # len("schema") + len("String") + len("telemetry-ingress@1")
MAX_VALIDATION_ERRORS = 20  # 422 detail entries
MAX_ENVELOPE_EXCERPT_BYTES = 2_048  # fenced-message quarantine excerpt
INGRESS_RECEIPT_TTL = timedelta(hours=24)
QUARANTINE_TTL = timedelta(days=14)
MAX_ACTIVE_INGEST_CREDENTIALS = 2  # slot numbers 1 and 2
INGEST_TOKEN_PATTERN = r"^lpi_(ic_[0-9a-f]{32})_([A-Za-z0-9_-]{43})$"
INGEST_SECRET_DIGEST_PREFIX = b"limnopulse.ingest-credential.v1\0"
CONNECTOR_VERSION_PATTERN = r"^[\x21-\x7e]{1,64}$"
PERMANENT_VISIBILITY_SECONDS = 60
MAX_VISIBILITY_SECONDS = 43_200
AUDIT_RETENTION = timedelta(days=90)

_TOKEN = re.compile(INGEST_TOKEN_PATTERN)
_FINGERPRINT_PATTERN = r"^[0-9a-f]{8}$"
_RECEIPT_TTL_US = 86_400_000_000
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_SKEW_FLAGS = frozenset({QualityFlag.CLOCK_SKEW_FUTURE, QualityFlag.CLOCK_SKEW_PAST_EXTREME})


def _canonical(prefix: str):
    return Annotated[str, AfterValidator(lambda value: require_canonical_id(value, prefix))]


IngestCredentialId = _canonical("ic")
IngressId = _canonical("ing")
QuarantineId = _canonical("qtn")
ConnectorVersion = Annotated[str, Field(pattern=CONNECTOR_VERSION_PATTERN)]


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


# Tokens and digests ---------------------------------------------------------------------------


def parse_ingest_token(token: str) -> tuple[str, str] | None:
    """`(ingest_credential_id, secret)`; never split on "_", which base64url also uses."""
    if not isinstance(token, str):
        return None
    match = _TOKEN.fullmatch(token)
    return None if match is None else (match.group(1), match.group(2))


def ingest_token(ingest_credential_id: str, secret: str) -> str:
    token = f"lpi_{ingest_credential_id}_{secret}"
    if parse_ingest_token(token) != (ingest_credential_id, secret):
        raise ValueError("ingest token does not match the token grammar")
    return token


def ingest_secret_sha256(secret: str) -> str:
    return sha256(INGEST_SECRET_DIGEST_PREFIX + secret.encode("ascii")).hexdigest()


def ingest_credential_fingerprint(secret_sha256: str) -> str:
    if (
        not isinstance(secret_sha256, str)
        or re.fullmatch(SHA256_HEX_PATTERN, secret_sha256) is None
    ):
        raise ValueError("secret_sha256 must be 64 lowercase hex characters")
    return secret_sha256[:8]


# Enums -----------------------------------------------------------------------------------------


class IngestCredentialStatus(StrEnum):
    ACTIVE = "active"
    REVOKED = "revoked"


class IngressReceiptState(StrEnum):
    PENDING = "pending"
    ENQUEUED = "enqueued"


class IngressFenceReason(StrEnum):
    """Terminal: rows are persisted and the message is deleted."""

    IDENTITY_UNRESOLVED = "identity_unresolved"
    INTEGRATION_MISMATCH = "integration_mismatch"
    INTEGRATION_DECOMMISSIONED = "integration_decommissioned"
    ACCOUNT_REVOKED = "account_revoked"
    DEVICE_DECOMMISSIONED = "device_decommissioned"


class IngressDeferReason(StrEnum):
    """Temporary: retryable visibility, then the DLQ."""

    INTEGRATION_NOT_ACTIVE = "integration_not_active"
    ACCOUNT_NOT_ACTIVE = "account_not_active"
    DEVICE_NOT_ACTIVE = "device_not_active"


class IngressAdmissionDenied(StrEnum):
    """Admission-time 403/422 causes; metric labels only."""

    IDENTITY_UNRESOLVED = "identity_unresolved"
    INTEGRATION_MISMATCH = "integration_mismatch"
    INTEGRATION_INACTIVE = "integration_inactive"
    ACCOUNT_INACTIVE = "account_inactive"
    DEVICE_INACTIVE = "device_inactive"
    COMPONENT_REQUIRED = "component_required"
    COMPONENT_INVALID = "component_invalid"
    ENTITLEMENT_DENIED = "entitlement_denied"


class QuarantineEntryKind(StrEnum):
    OBSERVATION = "observation"
    FENCED_OBSERVATION = "fenced_observation"
    FENCED_MESSAGE = "fenced_message"


class QuarantineEntryStatus(StrEnum):
    OPEN = "open"
    REPLAYED = "replayed"
    DISMISSED = "dismissed"


class SequenceEvent(StrEnum):
    FIRST = "first"
    CONTIGUOUS = "contiguous"
    GAP = "gap"
    REGRESSION = "regression"
    DUPLICATE = "duplicate"
    LATE = "late"


class IngressReceiptAction(StrEnum):
    """Receipt replay table (route steps 8 and 13)."""

    ADMIT = "admit"  # none or expired: continue to step 9, then claim
    REPLAY = "replay"  # enqueued, same request: 202 replayed, nothing sent
    RESUME = "resume"  # pending, same request: re-enqueue with the stored values
    CONFLICT = "conflict"  # live receipt for a different request: 409


class IngressFailureCategory(StrEnum):
    ENVELOPE_INVALID = "envelope_invalid"
    FENCED = "fenced"
    FENCED_UNVERIFIED = "fenced_unverified"
    DEFERRED = "deferred"
    UNEXPECTED = "unexpected"
    STORE_UNAVAILABLE = "store_unavailable"
    WRITE_RETRYABLE = "write_retryable"
    WRITE_PERMANENT = "write_permanent"
    CONTRACT_VIOLATION = "contract_violation"
    PROCESSED = "processed"


class IngressMessageAction(StrEnum):
    DELETE = "delete"
    PERMANENT = "permanent"  # change_visibility(PERMANENT_VISIBILITY_SECONDS)
    BACKOFF = "backoff"  # change_visibility(ingress_backoff_seconds(n))


class IngressMessageOutcome(StrEnum):
    PROCESSED = "processed"
    FENCED = "fenced"
    RETRY = "retry"
    PERMANENT = "permanent"


class IngestErrorDetail(StrEnum):
    """Fixed `/ingest` error bodies (`{"detail": <value>}`); they never echo input."""

    UNAUTHORIZED = "unauthorized"
    RATE_LIMITED = "rate limited"
    UNSUPPORTED_MEDIA_TYPE = "unsupported media type"
    TOO_LARGE = "request body too large"
    IDEMPOTENCY_CONFLICT = "idempotency key reused with a different request"
    FORBIDDEN = "forbidden"
    UNAVAILABLE = "service unavailable"


# Credentials and request models ---------------------------------------------------------------


class IngestCredential(V2VersionedEntity):
    tenant_id: SafeId
    ingest_credential_id: IngestCredentialId
    integration_account_id: SafeId
    slot: Literal[1, 2]
    secret_sha256: Sha256Hex = Field(repr=False)  # never in reprs, tracebacks or logs
    fingerprint: Annotated[str, Field(pattern=_FINGERPRINT_PATTERN)]
    status: IngestCredentialStatus
    revoked_at: datetime | None = None

    @model_validator(mode="after")
    def validate_credential(self) -> "IngestCredential":
        if self.fingerprint != self.secret_sha256[:8]:
            raise ValueError("fingerprint must be the first 8 characters of secret_sha256")
        if (self.status == IngestCredentialStatus.REVOKED) != (self.revoked_at is not None):
            raise ValueError("revoked_at is set exactly when the credential is revoked")
        if self.revoked_at is not None:
            require_utc(self.revoked_at, "revoked_at")
        return self


class AuthenticatedSource(_Frozen):
    tenant_id: SafeId
    integration_account_id: SafeId
    provider_key: ProviderKey
    ingest_credential_id: IngestCredentialId


class IngestSourceObservation(BaseModel):
    """Public ingest observation: `SourceObservation` with an optional `component_id`."""

    model_config = ConfigDict(extra="forbid")

    component_id: ComponentId | None = None
    metric_key: MetricKey
    value: SourceValue
    source_unit: UnitCode
    observed_at: TelemetryDatetime
    sequence: SequenceNumber | None = None
    source_event_id: SourceEventId | None = None
    source_metadata: SourceMetadata = Field(default_factory=dict, validate_default=True)


class IngestObservationBatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    external_device_id: str = Field(
        min_length=1, max_length=EXTERNAL_IDENTITY_MAX_LENGTH, pattern=EXTERNAL_IDENTITY_PATTERN
    )
    observations: list[IngestSourceObservation] = Field(
        min_length=1, max_length=MAX_INGEST_OBSERVATIONS
    )

    @model_validator(mode="after")
    def validate_distinct_components(self) -> "IngestObservationBatch":
        distinct = {item.component_id for item in self.observations} - {None}
        if len(distinct) > MAX_INGEST_DISTINCT_COMPONENTS:
            raise ValueError(
                f"at most {MAX_INGEST_DISTINCT_COMPONENTS} distinct component_id values per request"
            )
        return self


# Envelope ----------------------------------------------------------------------------------------


def _json(payload: Any) -> str:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _observation_payload(observation: SourceObservation) -> dict[str, Any]:
    return observation.model_dump(mode="json", exclude_none=True, exclude_defaults=True)


class IngressEnvelope(_Frozen):
    """`telemetry-ingress@1`: compact queue body of opaque ids; never a secret or raw identity.

    `to_json()` is the wire form; `model_dump`/`model_dump_json` also use the `schema` key, so
    any dump validates back."""

    model_config = ConfigDict(
        frozen=True, extra="forbid", validate_by_name=True, serialize_by_alias=True
    )

    schema_: Literal["telemetry-ingress@1"] = Field(alias="schema")
    ingress_id: IngressId
    replay_of: IngressId | None = None
    tenant_id: SafeId
    integration_account_id: SafeId
    provider_key: ProviderKey
    external_identity_hash: Sha256Hex
    device_integration_id: IntegrationId
    device_id: DeviceId
    source_type: SourceType
    connector_version: ConnectorVersion | None = None
    received_at: TelemetryDatetime
    replay: StrictBool = False
    mapping_versions: NormalizationVersions
    observations: tuple[SourceObservation, ...] = Field(
        min_length=1, max_length=MAX_INGEST_OBSERVATIONS
    )

    @model_validator(mode="after")
    def validate_replay(self) -> "IngressEnvelope":
        if self.replay_of is not None and not self.replay:
            raise ValueError("replay_of is only set on replay envelopes")
        return self

    def to_json(self) -> str:
        """Compact UTF-8-preserving wire form. `None` and default values are omitted, except
        `mapping_versions`, which always carries all three strings."""
        payload: dict[str, Any] = {}
        for name, info in type(self).model_fields.items():
            value = getattr(self, name)
            key = info.alias or name
            if name == "received_at":
                payload[key] = format_utc_timestamp(value)
            elif name == "mapping_versions":
                payload[key] = value.model_dump(mode="json")
            elif name == "observations":
                payload[key] = [_observation_payload(item) for item in value]
            elif value is None or (not info.is_required() and value == info.default):
                continue
            else:
                payload[key] = value.value if isinstance(value, StrEnum) else value
        return _json(payload)


def ingress_message_bytes(envelope: IngressEnvelope) -> int:
    """The size the D6 guard checks: UTF-8 body bytes plus the schema attribute bytes."""
    return len(envelope.to_json().encode("utf-8")) + INGRESS_SCHEMA_ATTRIBUTE_BYTES


def build_trusted_context(
    envelope: IngressEnvelope, integration: DeviceIntegration
) -> TrustedSourceContext:
    """Tenant, device and integration from the re-resolved DI; the rest from the envelope."""
    return TrustedSourceContext(
        tenant_id=integration.tenant_id,
        device_id=integration.device_id,
        integration_id=integration.device_integration_id,
        integration_type=integration.provider_key,
        source_type=envelope.source_type,
        connector_version=envelope.connector_version,
        received_at=envelope.received_at,
        replay=envelope.replay,
        mapping_versions=envelope.mapping_versions,
    )


# Receipts ----------------------------------------------------------------------------------------


def epoch_microseconds(value: datetime) -> int:
    """Exact integer microseconds since the epoch; float timestamps lose precision."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    delta = value - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def _receipt_expires_at(created_at_us: int) -> int:
    return -(-(created_at_us + _RECEIPT_TTL_US) // 1_000_000)  # epoch seconds, rounded up


class IngressReceipt(_Frozen):
    integration_account_id: SafeId
    tenant_id: SafeId
    key_sha256: Sha256Hex
    request_sha256: Sha256Hex
    ingress_id: IngressId
    received_at: TelemetryDatetime
    mapping_versions: NormalizationVersions  # pinned at the first claim; reused on retries
    state: IngressReceiptState
    created_at: datetime
    created_at_us: int
    expires_at: int  # TTL attribute only; liveness is decided on created_at_us

    @model_validator(mode="after")
    def validate_receipt(self) -> "IngressReceipt":
        require_utc(self.created_at, "created_at")
        if self.created_at_us != epoch_microseconds(self.created_at):
            raise ValueError("created_at_us must be the exact epoch microseconds of created_at")
        if self.expires_at != _receipt_expires_at(self.created_at_us):
            raise ValueError("expires_at must be created_at + 24 h in epoch seconds, rounded up")
        return self


def new_ingress_receipt(
    *,
    integration_account_id: str,
    tenant_id: str,
    key_sha256: str,
    request_sha256: str,
    ingress_id: str,
    received_at: datetime,
    mapping_versions: NormalizationVersions,
    created_at: datetime,
) -> IngressReceipt:
    created_at_us = epoch_microseconds(created_at)
    return IngressReceipt(
        integration_account_id=integration_account_id,
        tenant_id=tenant_id,
        key_sha256=key_sha256,
        request_sha256=request_sha256,
        ingress_id=ingress_id,
        received_at=received_at,
        mapping_versions=mapping_versions,
        state=IngressReceiptState.PENDING,
        created_at=created_at,
        created_at_us=created_at_us,
        expires_at=_receipt_expires_at(created_at_us),
    )


def ingress_receipt_cutoff_us(now: datetime) -> int:
    """A receipt is live iff `created_at_us > cutoff`; the claim condition uses the same value."""
    return epoch_microseconds(now) - _RECEIPT_TTL_US


def is_ingress_receipt_live(receipt: IngressReceipt, now: datetime) -> bool:
    return receipt.created_at_us > ingress_receipt_cutoff_us(now)


def decide_receipt_replay(
    receipt: IngressReceipt | None, *, request_sha256: str, now: datetime
) -> IngressReceiptAction:
    if receipt is None or not is_ingress_receipt_live(receipt, now):
        return IngressReceiptAction.ADMIT
    if receipt.request_sha256 != request_sha256:
        return IngressReceiptAction.CONFLICT
    if receipt.state == IngressReceiptState.ENQUEUED:
        return IngressReceiptAction.REPLAY
    return IngressReceiptAction.RESUME


# Admission and fence results -------------------------------------------------------------------


class IngressFence(_Frozen):
    reason: IngressFenceReason
    # Whether an authoritative IntegrationAccount row exists for the envelope's tenant and account.
    # Required: a forgotten flag must never default to "verified" and write rows for a forgery.
    account_verified: bool


class IngressDefer(_Frozen):
    reason: IngressDeferReason


class IngressDenial(_Frozen):
    reason: IngressAdmissionDenied
    observation_index: int | None = None  # set for COMPONENT_REQUIRED / COMPONENT_INVALID


class IngressAdmission(_Frozen):
    source: AuthenticatedSource
    device_integration: DeviceIntegration
    device: DeviceV2
    observations: tuple[SourceObservation, ...]


class TrustedIngress(_Frozen):
    context: TrustedSourceContext
    observations: tuple[SourceObservation, ...]


class IngressQueueMessage(_Frozen):
    message_id: str
    receipt_handle: str
    body: str
    receive_count: int
    sent_at: datetime


def _account_is_https_active(account: IntegrationAccount) -> bool:
    return (
        account.status == IntegrationAccountStatus.ACTIVE
        and account.provider_key in HTTPS_PROVIDER_KEYS
    )


def _device_matches(device: DeviceV2 | None, integration: DeviceIntegration) -> bool:
    return (
        device is not None
        and device.tenant_id == integration.tenant_id
        and device.device_id == integration.device_id
    )


def classify_admission(
    source: AuthenticatedSource,
    integration: DeviceIntegration | None,
    account: IntegrationAccount | None,
    device: DeviceV2 | None,
) -> IngressAdmissionDenied | None:
    """The `admit` column of the condition -> enum table; the first failure wins."""
    if integration is None:
        return IngressAdmissionDenied.IDENTITY_UNRESOLVED
    if (integration.tenant_id, integration.integration_account_id, integration.provider_key) != (
        source.tenant_id,
        source.integration_account_id,
        source.provider_key,
    ):
        return IngressAdmissionDenied.INTEGRATION_MISMATCH
    if integration.status != DeviceIntegrationStatus.ACTIVE:
        return IngressAdmissionDenied.INTEGRATION_INACTIVE
    if (
        account is None
        or (account.tenant_id, account.integration_account_id)
        != (source.tenant_id, source.integration_account_id)
        or not _account_is_https_active(account)
    ):
        return IngressAdmissionDenied.ACCOUNT_INACTIVE
    if not _device_matches(device, integration) or device.status != DeviceStatus.ACTIVE:
        return IngressAdmissionDenied.DEVICE_INACTIVE
    return None


def classify_fence(
    envelope: IngressEnvelope,
    integration: DeviceIntegration | None,
    account: IntegrationAccount | None,
    device: DeviceV2 | None,
) -> IngressFence | IngressDefer | None:
    """The `fence` column of the condition -> enum table; the first failure wins."""
    verified = (
        account is not None
        and account.tenant_id == envelope.tenant_id
        and account.integration_account_id == envelope.integration_account_id
    )
    if integration is None or integration.external_identity_hash != envelope.external_identity_hash:
        return IngressFence(
            reason=IngressFenceReason.IDENTITY_UNRESOLVED, account_verified=verified
        )
    if (
        integration.tenant_id,
        integration.integration_account_id,
        integration.provider_key,
        integration.device_integration_id,
        integration.device_id,
    ) != (
        envelope.tenant_id,
        envelope.integration_account_id,
        envelope.provider_key,
        envelope.device_integration_id,
        envelope.device_id,
    ):
        return IngressFence(
            reason=IngressFenceReason.INTEGRATION_MISMATCH, account_verified=verified
        )
    if integration.status == DeviceIntegrationStatus.DECOMMISSIONED:
        return IngressFence(
            reason=IngressFenceReason.INTEGRATION_DECOMMISSIONED, account_verified=verified
        )
    if integration.status != DeviceIntegrationStatus.ACTIVE:
        return IngressDefer(reason=IngressDeferReason.INTEGRATION_NOT_ACTIVE)
    if not verified:
        return IngressFence(reason=IngressFenceReason.IDENTITY_UNRESOLVED, account_verified=False)
    if (
        account.status == IntegrationAccountStatus.REVOKED
        or account.provider_key not in HTTPS_PROVIDER_KEYS
    ):
        return IngressFence(reason=IngressFenceReason.ACCOUNT_REVOKED, account_verified=True)
    if account.status != IntegrationAccountStatus.ACTIVE:
        return IngressDefer(reason=IngressDeferReason.ACCOUNT_NOT_ACTIVE)
    if not _device_matches(device, integration) or device.status == DeviceStatus.DECOMMISSIONED:
        return IngressFence(reason=IngressFenceReason.DEVICE_DECOMMISSIONED, account_verified=True)
    if device.status != DeviceStatus.ACTIVE:
        return IngressDefer(reason=IngressDeferReason.DEVICE_NOT_ACTIVE)
    return None


def resolve_component_selectors(
    observations: Sequence[IngestSourceObservation],
    *,
    default_component_id: str,
    active_component_ids: Collection[str],
) -> tuple[str, ...] | IngressDenial:
    """Fill the legacy default and check each selector against the Device's active Components."""
    resolved: list[str] = []
    for index, observation in enumerate(observations):
        if observation.component_id is None:
            if default_component_id not in active_component_ids:
                return IngressDenial(
                    reason=IngressAdmissionDenied.COMPONENT_REQUIRED, observation_index=index
                )
            resolved.append(default_component_id)
        elif observation.component_id not in active_component_ids:
            return IngressDenial(
                reason=IngressAdmissionDenied.COMPONENT_INVALID, observation_index=index
            )
        else:
            resolved.append(observation.component_id)
    return tuple(resolved)


_COMPONENT_DENIALS = {
    IngressAdmissionDenied.COMPONENT_REQUIRED: (
        "component_id is required for this device",
        "missing",
    ),
    IngressAdmissionDenied.COMPONENT_INVALID: (
        "component_id is not valid for this device",
        "value_error",
    ),
}


def component_denial_detail(denial: IngressDenial) -> list[dict[str, Any]]:
    """Route step 10 body: fixed message, no echo."""
    if denial.reason not in _COMPONENT_DENIALS or denial.observation_index is None:
        raise ValueError("only component denials with an observation index have a 422 body")
    message, error_type = _COMPONENT_DENIALS[denial.reason]
    return [
        {
            "loc": ["body", "observations", denial.observation_index, "component_id"],
            "msg": message,
            "type": error_type,
        }
    ]


# Quarantine ------------------------------------------------------------------------------------


def _utf8_bounded(value: str | None, field: str) -> None:
    if value is not None and len(value.encode("utf-8")) > MAX_ENVELOPE_EXCERPT_BYTES:
        raise ValueError(f"{field} must be at most {MAX_ENVELOPE_EXCERPT_BYTES} UTF-8 bytes")


def _is_replayable(kind: QuarantineEntryKind, source_json: str | None, truncated: bool) -> bool:
    if kind == QuarantineEntryKind.FENCED_MESSAGE or source_json is None or truncated:
        return False
    try:
        SourceObservation.model_validate_json(source_json)
    except ValueError:
        return False
    return True


class QuarantineEntry(V2VersionedEntity):
    tenant_id: SafeId
    quarantine_id: QuarantineId
    kind: QuarantineEntryKind
    reason: str  # QuarantineReason value, or "fenced_<IngressFenceReason>"
    ingress_id: IngressId
    replay_of: IngressId | None
    integration_account_id: SafeId
    provider_key: ProviderKey
    external_identity_hash: Sha256Hex
    source_type: SourceType
    connector_version: ConnectorVersion | None
    device_integration_id: IntegrationId
    device_id: DeviceId
    received_at: TelemetryDatetime
    mapping_versions: NormalizationVersions
    observation: QuarantinedObservation | None
    source_observation_json: str | None
    envelope_excerpt_json: str | None
    truncated: StrictBool
    replayable: StrictBool
    status: QuarantineEntryStatus
    replayed_ingress_id: IngressId | None
    expires_at: datetime

    @model_validator(mode="after")
    def validate_entry(self) -> "QuarantineEntry":
        if (self.status == QuarantineEntryStatus.REPLAYED) != (
            self.replayed_ingress_id is not None
        ):
            raise ValueError("replayed_ingress_id is set exactly when the entry was replayed")
        if (self.kind == QuarantineEntryKind.OBSERVATION) != (self.observation is not None):
            raise ValueError("only observation rows carry the quarantined observation")
        if (self.kind == QuarantineEntryKind.FENCED_MESSAGE) != (
            self.envelope_excerpt_json is not None
        ):
            raise ValueError("only fenced_message rows carry the envelope excerpt")
        if self.kind == QuarantineEntryKind.FENCED_MESSAGE and self.source_observation_json:
            raise ValueError("fenced_message rows carry no source observation")
        _utf8_bounded(self.source_observation_json, "source_observation_json")
        _utf8_bounded(self.envelope_excerpt_json, "envelope_excerpt_json")
        if self.expires_at != self.created_at + QUARANTINE_TTL:
            raise ValueError("expires_at must be exactly 14 days after created_at")
        expected = _is_replayable(self.kind, self.source_observation_json, self.truncated)
        if self.replayable != expected:
            raise ValueError("replayable must follow the D9 replayable rule")
        return self

    @property
    def entry_key(self) -> str:
        """The quarantine sort key `<received_at_key>#<quarantine_id>`."""
        return f"{format_utc_timestamp(self.received_at)}#{self.quarantine_id}"


class QuarantinePage(_Frozen):
    entries: tuple[QuarantineEntry, ...]
    next_after_sk: str | None


def _bounded_prefix(text: str) -> tuple[str, bool]:
    raw = text.encode("utf-8")
    if len(raw) <= MAX_ENVELOPE_EXCERPT_BYTES:
        return text, False
    # Dropping the incomplete trailing character keeps the cut on a UTF-8 boundary.
    return raw[:MAX_ENVELOPE_EXCERPT_BYTES].decode("utf-8", errors="ignore"), True


def build_quarantine_entries(
    envelope: IngressEnvelope,
    *,
    outcome: Any | None = None,
    fence: IngressFence | None = None,
    now: datetime,
) -> tuple[QuarantineEntry, ...]:
    """Rows for a processed message (`outcome.quarantined`) or a verified terminal fence.

    `outcome` is a `services.telemetry_pipeline.PipelineOutcome`; an unverified fence writes
    nothing (D8). Ids are content-derived, so a redelivered message rebuilds the same rows.
    """
    if (outcome is None) == (fence is None):
        raise ValueError("exactly one of outcome or fence is required")
    if fence is not None and not fence.account_verified:
        return ()
    seen: Counter[str] = Counter()
    rows: list[QuarantineEntry] = []

    def add(
        kind: QuarantineEntryKind,
        reason: str,
        excerpt: str,
        *,
        truncated: bool,
        ordinal: int | None = None,
        observation: QuarantinedObservation | None = None,
        source_json: str | None = None,
        envelope_json: str | None = None,
    ) -> None:
        if ordinal is None:
            ordinal = seen[excerpt]
            seen[excerpt] += 1
        rows.append(
            QuarantineEntry(
                tenant_id=envelope.tenant_id,
                quarantine_id=quarantine_entry_id(envelope.ingress_id, ordinal, excerpt),
                kind=kind,
                reason=reason,
                ingress_id=envelope.ingress_id,
                replay_of=envelope.replay_of,
                integration_account_id=envelope.integration_account_id,
                provider_key=envelope.provider_key,
                external_identity_hash=envelope.external_identity_hash,
                source_type=envelope.source_type,
                connector_version=envelope.connector_version,
                device_integration_id=envelope.device_integration_id,
                device_id=envelope.device_id,
                received_at=envelope.received_at,
                mapping_versions=envelope.mapping_versions,
                observation=observation,
                source_observation_json=source_json,
                envelope_excerpt_json=envelope_json,
                truncated=truncated,
                replayable=_is_replayable(kind, source_json, truncated),
                status=QuarantineEntryStatus.OPEN,
                replayed_ingress_id=None,
                created_at=now,
                updated_at=now,
                version=1,
                expires_at=now + QUARANTINE_TTL,
            )
        )

    if outcome is not None:
        for item in outcome.quarantined:
            add(
                QuarantineEntryKind.OBSERVATION,
                item.reason.value,
                item.source_excerpt_json,
                truncated=item.truncated,
                observation=item,
                source_json=item.source_excerpt_json,
            )
        return tuple(rows)
    reason = f"fenced_{fence.reason.value}"
    for source in envelope.observations:
        source_json = _json(_observation_payload(source))
        add(
            QuarantineEntryKind.FENCED_OBSERVATION,
            reason,
            source_json,
            truncated=False,
            source_json=source_json,
        )
    excerpt, truncated = _bounded_prefix(envelope.to_json())
    add(
        QuarantineEntryKind.FENCED_MESSAGE,
        reason,
        excerpt,
        truncated=truncated,
        ordinal=-1,
        envelope_json=excerpt,
    )
    return tuple(rows)


# Audit items (D23) -----------------------------------------------------------------------------

_AUDIT_RESOURCE_TYPES = {
    "ingest_credential.issued": "ingest_credential",
    "ingest_credential.revoked": "ingest_credential",
    "integration_account.status_changed": "integration_account",
    "device_integration.status_changed": "device_integration",
}


def ingest_audit_item(
    *,
    action: str,
    tenant_id: str,
    resource_id: str,
    before_status: str | None,
    after_status: str,
    context: AuditContext,
    now: datetime,
    fingerprint: str | None = None,
) -> dict[str, Any]:
    """The single `LimnopulseAudit` item layout (mirrors `adapters/alert_rules.py`): actor,
    tenant, resource, statuses and the credential fingerprint only; never a token or digest."""
    if action not in _AUDIT_RESOURCE_TYPES:
        raise ValueError("unknown ingest audit action")
    require_utc(now, "now")
    if fingerprint is not None and re.fullmatch(_FINGERPRINT_PATTERN, fingerprint) is None:
        raise ValueError("fingerprint must be 8 lowercase hex characters")
    event_id = f"audit_{uuid4().hex}"
    return {
        "PK": f"TENANT#{tenant_id}#MONTH#{now:%Y-%m}",
        "SK": f"{now.isoformat()}#{event_id}",
        "entity_type": "audit_event",
        "event_id": event_id,
        "tenant_id": tenant_id,
        "actor_type": "user",
        "actor_id": context.actor_id,
        "action": action,
        "resource_type": _AUDIT_RESOURCE_TYPES[action],
        "resource_id": resource_id,
        "before_status": before_status,
        "after_status": after_status,
        "fingerprint": fingerprint,
        "ip": context.ip,
        "user_agent": context.user_agent,
        "created_at": now.isoformat(),
        "expires_at": int((now + AUDIT_RETENTION).timestamp()),
    }


# Ingest state and marks (D11) --------------------------------------------------------------------


def _paired(first: object, second: object, names: str) -> None:
    if (first is None) != (second is None):
        raise ValueError(f"{names} are written together")


class ComponentMark(_Frozen):
    component_id: SafeId
    received_at: datetime | None = None
    ingested_at: datetime | None = None
    observed_at: datetime | None = None
    sequence: int | None = None

    @model_validator(mode="after")
    def validate_pair(self) -> "ComponentMark":
        _paired(self.observed_at, self.sequence, "observed_at and sequence")
        return self


class DeviceMark(_Frozen):
    received_at: datetime
    ingested_at: datetime


class ConnectorMark(_Frozen):
    received_at: datetime | None = None
    accepted_at: datetime | None = None
    quarantined_at: datetime | None = None
    quarantine_reason: str | None = None
    fenced_at: datetime | None = None
    fence_reason: str | None = None

    @model_validator(mode="after")
    def validate_pairs(self) -> "ConnectorMark":
        _paired(self.quarantined_at, self.quarantine_reason, "quarantined_at and its reason")
        _paired(self.fenced_at, self.fence_reason, "fenced_at and its reason")
        return self


class IngestMarks(_Frozen):
    tenant_id: SafeId
    device_id: str | None
    integration_account_id: SafeId
    component_marks: tuple[ComponentMark, ...] = ()
    device_mark: DeviceMark | None = None
    connector_mark: ConnectorMark = Field(default_factory=ConnectorMark)

    @model_validator(mode="after")
    def validate_device(self) -> "IngestMarks":
        if self.device_id is None and (self.component_marks or self.device_mark is not None):
            raise ValueError("component and device marks need the verified device_id")
        return self


class ComponentIngestState(_Frozen):
    tenant_id: SafeId
    component_id: SafeId
    device_id: DeviceId
    last_received_at: datetime | None = None
    last_ingested_at: datetime | None = None
    last_observed_at: datetime | None = None
    last_observed_sequence: int | None = None


class DeviceIngestState(_Frozen):
    tenant_id: SafeId
    device_id: DeviceId
    last_received_at: datetime | None = None
    last_ingested_at: datetime | None = None


class ConnectorHealth(_Frozen):
    tenant_id: SafeId
    integration_account_id: SafeId
    last_received_at: datetime | None = None
    last_accepted_at: datetime | None = None
    last_quarantined_at: datetime | None = None
    last_quarantine_reason: str | None = None
    last_fenced_at: datetime | None = None
    last_fence_reason: str | None = None


def classify_component_sequence(
    state: ComponentIngestState | None, pairs: Sequence[tuple[datetime, int | None]]
) -> tuple[SequenceEvent, ...]:
    """Sequence truth table over the distinct `(observed_at, sequence)` pairs, sorted."""
    previous: tuple[datetime, int] | None = None
    if (
        state is not None
        and state.last_observed_at is not None
        and state.last_observed_sequence is not None
    ):
        previous = (state.last_observed_at, state.last_observed_sequence)
    events: list[SequenceEvent] = []
    for observed_at, sequence in sorted({pair for pair in pairs if pair[1] is not None}):
        if previous is None:
            event = SequenceEvent.FIRST
        elif observed_at > previous[0]:
            if sequence == previous[1] + 1:
                event = SequenceEvent.CONTIGUOUS
            elif sequence > previous[1] + 1:
                event = SequenceEvent.GAP
            else:
                event = SequenceEvent.REGRESSION
        elif observed_at == previous[0]:
            event = SequenceEvent.DUPLICATE if sequence == previous[1] else SequenceEvent.LATE
        else:
            event = SequenceEvent.LATE  # reordering is normal, never a regression
        events.append(event)
        if event != SequenceEvent.LATE:
            previous = (observed_at, sequence)
    return tuple(events)


def build_ingest_marks(
    envelope: IngressEnvelope,
    outcome: Any | None,
    *,
    fence: IngressFence | None = None,
    ingested_at: datetime,
) -> IngestMarks:
    """Per-message monotone marks (D11). A replay or an unverified fence yields no mark; a
    verified fence marks only the connector, because the envelope's device is unverified."""
    if (outcome is None) == (fence is None):
        raise ValueError("exactly one of outcome or fence is required")
    if ingested_at.tzinfo is None or ingested_at.utcoffset() is None:
        raise ValueError("ingested_at must be timezone-aware")
    empty = IngestMarks(
        tenant_id=envelope.tenant_id,
        device_id=None,
        integration_account_id=envelope.integration_account_id,
    )
    if envelope.replay:
        return empty
    received = envelope.received_at
    if fence is not None:
        if not fence.account_verified:
            return empty
        return empty.model_copy(
            update={
                "connector_mark": ConnectorMark(fenced_at=received, fence_reason=fence.reason.value)
            }
        )
    best: dict[str, tuple[datetime, int] | None] = {}
    for observation in outcome.accepted:
        component_id = observation.resolved.component_id
        best.setdefault(component_id, None)
        sequence = observation.observation.sequence
        if sequence is None or _SKEW_FLAGS & set(observation.quality.flags):
            continue
        pair = (observation.observation.observed_at, sequence)
        current = best[component_id]
        if current is None or pair > current:
            best[component_id] = pair
    reasons = [item.reason.value for item in outcome.quarantined]
    return IngestMarks(
        tenant_id=envelope.tenant_id,
        device_id=envelope.device_id,
        integration_account_id=envelope.integration_account_id,
        component_marks=tuple(
            ComponentMark(
                component_id=component_id,
                received_at=received,
                ingested_at=ingested_at,
                observed_at=None if pair is None else pair[0],
                sequence=None if pair is None else pair[1],
            )
            for component_id, pair in sorted(best.items())
        ),
        device_mark=DeviceMark(received_at=received, ingested_at=ingested_at),
        connector_mark=ConnectorMark(
            received_at=received,
            accepted_at=received if outcome.accepted else None,
            quarantined_at=received if reasons else None,
            quarantine_reason=max(reasons) if reasons else None,
        ),
    )


# Rate limits, failure table and metrics --------------------------------------------------------


def preauth_bucket_capacity(per_minute: int) -> int:
    return max(1, per_minute // 10)


def bucket_refill_per_second(per_minute: int) -> float:
    return per_minute / 60


INGRESS_FAILURE_TABLE: Mapping[
    IngressFailureCategory, tuple[IngressMessageOutcome, IngressMessageAction]
] = {
    IngressFailureCategory.ENVELOPE_INVALID: (
        IngressMessageOutcome.PERMANENT,
        IngressMessageAction.PERMANENT,
    ),
    IngressFailureCategory.FENCED: (IngressMessageOutcome.FENCED, IngressMessageAction.DELETE),
    IngressFailureCategory.FENCED_UNVERIFIED: (
        IngressMessageOutcome.PERMANENT,
        IngressMessageAction.PERMANENT,
    ),
    IngressFailureCategory.DEFERRED: (IngressMessageOutcome.RETRY, IngressMessageAction.BACKOFF),
    IngressFailureCategory.UNEXPECTED: (IngressMessageOutcome.RETRY, IngressMessageAction.BACKOFF),
    IngressFailureCategory.STORE_UNAVAILABLE: (
        IngressMessageOutcome.RETRY,
        IngressMessageAction.BACKOFF,
    ),
    IngressFailureCategory.WRITE_RETRYABLE: (
        IngressMessageOutcome.RETRY,
        IngressMessageAction.BACKOFF,
    ),
    IngressFailureCategory.WRITE_PERMANENT: (
        IngressMessageOutcome.PERMANENT,
        IngressMessageAction.PERMANENT,
    ),
    IngressFailureCategory.CONTRACT_VIOLATION: (
        IngressMessageOutcome.PERMANENT,
        IngressMessageAction.PERMANENT,
    ),
    IngressFailureCategory.PROCESSED: (
        IngressMessageOutcome.PROCESSED,
        IngressMessageAction.DELETE,
    ),
}


def ingress_backoff_seconds(
    receive_count: int,
    *,
    base: int = 30,
    cap: int = 900,
    random: Callable[[], float] = _random.random,
) -> int:
    """Full-jitter backoff: `clamp(ceil(random() * min(cap, base * 2**(n-1))), 1, 43200)`."""
    window = min(cap, base * 2 ** (max(receive_count, 1) - 1))
    return min(max(math.ceil(random() * window), 1), MAX_VISIBILITY_SECONDS)


def _values(enum: type[StrEnum]) -> frozenset[str]:
    return frozenset(item.value for item in enum)


INGEST_METRICS: Mapping[str, Mapping[str, frozenset[str]]] = {
    "ingest_requests_total": {
        "outcome": frozenset(
            {
                "accepted",
                "replayed",
                "unauthorized",
                "rate_limited",
                "unsupported_media",
                "too_large",
                "invalid",
                "conflict",
                "forbidden",
                "unavailable",
            }
        )
    },
    "ingest_admission_denied_total": {"reason": _values(IngressAdmissionDenied)},
    "ingest_observations_admitted_total": {},
    "ingress_messages_total": {
        "outcome": _values(IngressMessageOutcome),
        "category": _values(IngressFailureCategory),
    },
    "ingress_observations_total": {"result": frozenset({"accepted", "quarantined"})},
    "ingress_quarantined_total": {"reason": _values(QuarantineReason)},
    "ingress_fenced_total": {"reason": _values(IngressFenceReason)},
    "ingress_deferred_total": {"reason": _values(IngressDeferReason)},
    "ingress_quality_flags_total": {"flag": _values(QualityFlag)},
    "ingress_sequence_events_total": {"event": _values(SequenceEvent)},
    "ingress_write_failures_total": {"retryable": frozenset({"true", "false"})},
    "ingress_normalization_duration_seconds": {},
    "ingress_queue_age_seconds": {},
    "ingress_dlq_redriven_total": {"result": frozenset({"redriven", "failed"})},
    "ingress_quarantine_replayed_total": {"result": frozenset({"replayed", "skipped", "failed"})},
}


def require_metric_labels(name: str, labels: Mapping[str, str]) -> None:
    """Closed names and closed-enum label values only; never a tenant or identity label."""
    spec = INGEST_METRICS.get(name)
    if spec is None:
        raise ValueError("unknown metric name")
    if set(labels) != set(spec):
        raise ValueError("metric labels must match the closed label set")
    for label, value in labels.items():
        if value not in spec[label]:
            raise ValueError("metric label value is not in the closed set")
