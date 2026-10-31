"""P3-00 frozen contracts: constants, identifiers, models, the envelope, receipts, quarantine
rows, audit items, the condition -> enum table, the fixed 422 messages and the failure table."""

import dataclasses
import hashlib
import json
import math
import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import pytest
from pydantic import BaseModel, ValidationError

from limnopulse_api.adapters.dynamodb_v2 import canonical_timestamp_key
from limnopulse_api.api.redacted_validation import redacted_validation_detail
from limnopulse_api.api.v2.schemas import integrations as integration_schemas
from limnopulse_api.core.errors import (
    IngressMessageTooLarge,
    IngressQueueError,
    LimnopulseError,
)
from limnopulse_api.domain import ingestion, v2_common
from limnopulse_api.domain.devices_v2 import DeviceStatus
from limnopulse_api.domain.ids import (
    new_ingest_credential_id,
    new_ingress_id,
    quarantine_entry_id,
    replay_ingress_id,
)
from limnopulse_api.domain.ingestion import (
    HTTPS_PROVIDER_KEYS,
    INGEST_METRICS,
    INGRESS_FAILURE_TABLE,
    AuthenticatedSource,
    ComponentIngestState,
    ComponentMark,
    ConnectorHealth,
    ConnectorMark,
    DeviceIngestState,
    DeviceMark,
    IngestCredential,
    IngestCredentialStatus,
    IngestErrorDetail,
    IngestMarks,
    IngestObservationBatch,
    IngestSourceObservation,
    IngressAdmission,
    IngressAdmissionDenied,
    IngressDefer,
    IngressDeferReason,
    IngressDenial,
    IngressEnvelope,
    IngressFailureCategory,
    IngressFence,
    IngressFenceReason,
    IngressMessageAction,
    IngressMessageOutcome,
    IngressQueueMessage,
    IngressReceipt,
    IngressReceiptAction,
    IngressReceiptState,
    QuarantineEntry,
    QuarantineEntryKind,
    QuarantineEntryStatus,
    QuarantinePage,
    SequenceEvent,
    TrustedIngress,
    bucket_refill_per_second,
    build_quarantine_entries,
    build_trusted_context,
    classify_admission,
    classify_fence,
    component_denial_detail,
    decide_receipt_replay,
    epoch_microseconds,
    ingest_audit_item,
    ingest_credential_fingerprint,
    ingest_secret_sha256,
    ingest_token,
    ingress_backoff_seconds,
    ingress_message_bytes,
    ingress_receipt_cutoff_us,
    is_ingress_receipt_live,
    new_ingress_receipt,
    parse_ingest_token,
    preauth_bucket_capacity,
    require_metric_labels,
    resolve_component_selectors,
)
from limnopulse_api.domain.integrations import (
    DeviceIntegrationStatus,
    IntegrationAccountStatus,
)
from limnopulse_api.domain.legacy_telemetry_mapping import bounded_excerpt
from limnopulse_api.domain.telemetry_v2 import (
    MAX_OBSERVATIONS_PER_BATCH,
    NormalizationVersions,
    QualityFlag,
    QuarantinedObservation,
    QuarantineReason,
    SourceObservation,
    SourceType,
    TrustedSourceContext,
)
from limnopulse_api.services.telemetry_pipeline import PipelineOutcome, TelemetryPipeline
from tests.support.ingestion_contract_kits import (
    ACCOUNT,
    AUDIT,
    COMPONENT,
    CREDENTIAL,
    DEFAULT_COMPONENT,
    DEVICE,
    IDENTITY_HASH,
    INGRESS,
    INTEGRATION,
    OBSERVED,
    OTHER_ACCOUNT,
    OTHER_COMPONENT,
    OTHER_DEVICE,
    OTHER_INTEGRATION,
    OTHER_TENANT,
    PROVIDER,
    RECEIVED,
    SECRET,
    TENANT,
    canonical_observation,
    credential,
    envelope_of_message_bytes,
    multibyte_envelope,
    device,
    device_integration,
    envelope,
    integration_account,
    source_observation,
)

SOURCE = AuthenticatedSource(
    tenant_id=TENANT,
    integration_account_id=ACCOUNT,
    provider_key=PROVIDER,
    ingest_credential_id=CREDENTIAL,
)
EMOJI = "\U0001f600"


def _fields(model: type) -> set[str]:
    if dataclasses.is_dataclass(model):
        return {item.name for item in dataclasses.fields(model)}
    return {info.alias or name for name, info in model.model_fields.items()}


# Constants and identifiers -------------------------------------------------------------------


def test_every_constant_has_its_exact_value() -> None:
    assert ingestion.INGRESS_ENVELOPE_SCHEMA == "telemetry-ingress@1"
    assert HTTPS_PROVIDER_KEYS == frozenset({"https_direct"})
    assert isinstance(HTTPS_PROVIDER_KEYS, frozenset)
    assert ingestion.MAX_INGEST_BODY_BYTES == 163_840
    assert ingestion.MAX_INGEST_OBSERVATIONS == 1_000 == MAX_OBSERVATIONS_PER_BATCH
    assert ingestion.MAX_INGEST_DISTINCT_COMPONENTS == 32
    assert ingestion.MAX_INGRESS_MESSAGE_BYTES == 262_144
    assert ingestion.INGRESS_SCHEMA_ATTRIBUTE_BYTES == 31
    assert ingestion.INGRESS_SCHEMA_ATTRIBUTE_BYTES == len("schema") + len("String") + len(
        "telemetry-ingress@1"
    )
    assert ingestion.MAX_VALIDATION_ERRORS == 20
    assert ingestion.MAX_ENVELOPE_EXCERPT_BYTES == 2_048
    assert ingestion.INGRESS_RECEIPT_TTL == timedelta(hours=24)
    assert ingestion.QUARANTINE_TTL == timedelta(days=14)
    assert ingestion.MAX_ACTIVE_INGEST_CREDENTIALS == 2
    assert ingestion.INGEST_TOKEN_PATTERN == r"^lpi_(ic_[0-9a-f]{32})_([A-Za-z0-9_-]{43})$"
    assert ingestion.INGEST_SECRET_DIGEST_PREFIX == b"limnopulse.ingest-credential.v1\0"
    assert ingestion.CONNECTOR_VERSION_PATTERN == r"^[\x21-\x7e]{1,64}$"
    assert ingestion.PERMANENT_VISIBILITY_SECONDS == 60
    assert ingestion.MAX_VISIBILITY_SECONDS == 43_200


@pytest.mark.parametrize(
    "secret",
    ["A" * 43, "_" * 43, "-" * 43, "a_b-" + "Z" * 39, SECRET],
    ids=["letters", "underscores", "dashes", "mixed", "kit"],
)
def test_token_parsing_keeps_secrets_that_contain_separators(secret: str) -> None:
    token = f"lpi_{CREDENTIAL}_{secret}"

    assert ingest_token(CREDENTIAL, secret) == token
    assert parse_ingest_token(token) == (CREDENTIAL, secret)


@pytest.mark.parametrize(
    "token",
    [
        f"lpi_{CREDENTIAL}_{'A' * 42}",
        f"lpi_{CREDENTIAL}_{'A' * 44}",
        f"lpx_{CREDENTIAL}_{'A' * 43}",
        f"lpi_ic_{'A' * 32}_{'A' * 43}",
        f"lpi_ic_{'a' * 31}_{'A' * 44}",
        f"lpi_{CREDENTIAL}_{'A' * 42}=",
        f"lpi_{CREDENTIAL}_{'A' * 42}+",
        f"Bearer lpi_{CREDENTIAL}_{'A' * 43}",
        f"lpi_{CREDENTIAL}_{'A' * 43}\n",
        "",
    ],
    ids=[
        "secret-42",
        "secret-44",
        "wrong-prefix",
        "uppercase-hex",
        "short-id",
        "padding",
        "plus",
        "scheme",
        "newline",
        "empty",
    ],
)
def test_token_parsing_rejects_everything_else(token: str) -> None:
    assert parse_ingest_token(token) is None


def test_ingest_token_refuses_to_build_an_unparseable_token() -> None:
    with pytest.raises(ValueError):
        ingest_token(CREDENTIAL, "short")
    with pytest.raises(ValueError):
        ingest_token("ic_" + "A" * 32, SECRET)


def test_secret_digest_and_fingerprint_vectors_are_exact() -> None:
    digest = ingest_secret_sha256(SECRET)

    assert digest == "7a04a39336b34d9d66678f47ca42cfcb8e4287fbd8a5a142f98265a40c5016da"
    assert (
        digest == hashlib.sha256(b"limnopulse.ingest-credential.v1\0" + SECRET.encode()).hexdigest()
    )
    assert digest != hashlib.sha256(SECRET.encode()).hexdigest()
    assert ingest_credential_fingerprint(digest) == "7a04a393"
    with pytest.raises(ValueError):
        ingest_credential_fingerprint("Z" * 64)


def test_id_functions_use_their_prefix_and_are_random() -> None:
    credentials = {new_ingest_credential_id() for _ in range(32)}
    ingresses = {new_ingress_id() for _ in range(32)}

    assert len(credentials) == len(ingresses) == 32
    assert all(re.fullmatch(r"ic_[0-9a-f]{32}", item) for item in credentials)
    assert all(re.fullmatch(r"ing_[0-9a-f]{32}", item) for item in ingresses)
    assert all(parse_ingest_token(ingest_token(item, SECRET)) for item in credentials)


def test_quarantine_entry_id_is_deterministic() -> None:
    excerpt = '{"a":1}'
    expected = "qtn_a4d19b4d5542541689fd06db00b81bfc"
    discriminator = (
        f"limnopulse:quarantine:{INGRESS}:0:{hashlib.sha256(excerpt.encode()).hexdigest()}"
    )

    assert quarantine_entry_id(INGRESS, 0, excerpt) == expected
    assert expected == "qtn_" + uuid5(NAMESPACE_URL, discriminator).hex
    first, second = (quarantine_entry_id(INGRESS, 0, excerpt) for _ in range(2))
    assert first == second
    assert quarantine_entry_id(INGRESS, 1, excerpt) != expected
    assert quarantine_entry_id(INGRESS, -1, excerpt) != expected
    assert quarantine_entry_id(INGRESS, 0, '{"a":2}') != expected
    assert quarantine_entry_id("ing_" + "5" * 32, 0, excerpt) != expected


def test_replay_ingress_id_is_deterministic_over_the_sorted_group_keys() -> None:
    expected = "ing_6561872adeea5908a1df5fb6b651456e"

    assert replay_ingress_id(["a#1", "b#2"]) == expected
    assert replay_ingress_id(["b#2", "a#1"]) == expected
    assert replay_ingress_id(("a#1",)) != expected
    assert expected == "ing_" + uuid5(NAMESPACE_URL, "limnopulse:replay:a#1,b#2").hex


def test_queue_errors_are_application_errors() -> None:
    retryable = IngressQueueError("send failed", retryable=True)
    permanent = IngressQueueError("bad queue", retryable=False)
    too_large = IngressMessageTooLarge("envelope too large")

    assert isinstance(retryable, LimnopulseError) and retryable.retryable is True
    assert permanent.retryable is False
    assert isinstance(too_large, LimnopulseError)
    with pytest.raises(TypeError):
        IngressQueueError("positional retryable", True)  # type: ignore[misc]


# Enums and model shapes ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("enum", "values"),
    [
        (IngestCredentialStatus, {"active", "revoked"}),
        (IngressReceiptState, {"pending", "enqueued"}),
        (
            IngressFenceReason,
            {
                "identity_unresolved",
                "integration_mismatch",
                "integration_decommissioned",
                "account_revoked",
                "device_decommissioned",
            },
        ),
        (
            IngressDeferReason,
            {"integration_not_active", "account_not_active", "device_not_active"},
        ),
        (
            IngressAdmissionDenied,
            {
                "identity_unresolved",
                "integration_mismatch",
                "integration_inactive",
                "account_inactive",
                "device_inactive",
                "component_required",
                "component_invalid",
                "entitlement_denied",
            },
        ),
        (QuarantineEntryKind, {"observation", "fenced_observation", "fenced_message"}),
        (QuarantineEntryStatus, {"open", "replayed", "dismissed"}),
        (SequenceEvent, {"first", "contiguous", "gap", "regression", "duplicate", "late"}),
        (IngressReceiptAction, {"admit", "replay", "resume", "conflict"}),
        (
            IngressFailureCategory,
            {
                "envelope_invalid",
                "fenced",
                "fenced_unverified",
                "deferred",
                "unexpected",
                "store_unavailable",
                "write_retryable",
                "write_permanent",
                "contract_violation",
                "processed",
            },
        ),
        (IngressMessageAction, {"delete", "permanent", "backoff"}),
        (IngressMessageOutcome, {"processed", "fenced", "retry", "permanent"}),
        (
            IngestErrorDetail,
            {
                "unauthorized",
                "rate limited",
                "unsupported media type",
                "request body too large",
                "idempotency key reused with a different request",
                "forbidden",
                "service unavailable",
            },
        ),
    ],
    ids=lambda value: getattr(value, "__name__", ""),
)
def test_enum_value_sets_are_frozen(enum: type[StrEnum], values: set[str]) -> None:
    assert issubclass(enum, StrEnum)
    assert {item.value for item in enum} == values


VERSIONED = {"created_at", "updated_at", "version", "schema_version"}
MODEL_FIELDS: dict[type, set[str]] = {
    IngestCredential: VERSIONED
    | {
        "tenant_id",
        "ingest_credential_id",
        "integration_account_id",
        "slot",
        "secret_sha256",
        "fingerprint",
        "status",
        "revoked_at",
    },
    AuthenticatedSource: {
        "tenant_id",
        "integration_account_id",
        "provider_key",
        "ingest_credential_id",
    },
    IngestSourceObservation: {
        "component_id",
        "metric_key",
        "value",
        "source_unit",
        "observed_at",
        "sequence",
        "source_event_id",
        "source_metadata",
    },
    IngestObservationBatch: {"external_device_id", "observations"},
    IngressEnvelope: {
        "schema",
        "ingress_id",
        "replay_of",
        "tenant_id",
        "integration_account_id",
        "provider_key",
        "external_identity_hash",
        "device_integration_id",
        "device_id",
        "source_type",
        "connector_version",
        "received_at",
        "replay",
        "mapping_versions",
        "observations",
    },
    IngressReceipt: {
        "integration_account_id",
        "tenant_id",
        "key_sha256",
        "request_sha256",
        "ingress_id",
        "received_at",
        "mapping_versions",
        "state",
        "created_at",
        "created_at_us",
        "expires_at",
    },
    QuarantineEntry: VERSIONED
    | {
        "tenant_id",
        "quarantine_id",
        "kind",
        "reason",
        "ingress_id",
        "replay_of",
        "integration_account_id",
        "provider_key",
        "external_identity_hash",
        "source_type",
        "connector_version",
        "device_integration_id",
        "device_id",
        "received_at",
        "mapping_versions",
        "observation",
        "source_observation_json",
        "envelope_excerpt_json",
        "truncated",
        "replayable",
        "status",
        "replayed_ingress_id",
        "expires_at",
    },
    QuarantinePage: {"entries", "next_after_sk"},
    IngestMarks: {
        "tenant_id",
        "device_id",
        "integration_account_id",
        "component_marks",
        "device_mark",
        "connector_mark",
    },
    ComponentMark: {"component_id", "received_at", "ingested_at", "observed_at", "sequence"},
    DeviceMark: {"received_at", "ingested_at"},
    ConnectorMark: {
        "received_at",
        "accepted_at",
        "quarantined_at",
        "quarantine_reason",
        "fenced_at",
        "fence_reason",
    },
    ComponentIngestState: {
        "tenant_id",
        "component_id",
        "device_id",
        "last_received_at",
        "last_ingested_at",
        "last_observed_at",
        "last_observed_sequence",
    },
    DeviceIngestState: {"tenant_id", "device_id", "last_received_at", "last_ingested_at"},
    ConnectorHealth: {
        "tenant_id",
        "integration_account_id",
        "last_received_at",
        "last_accepted_at",
        "last_quarantined_at",
        "last_quarantine_reason",
        "last_fenced_at",
        "last_fence_reason",
    },
    IngressFence: {"reason", "account_verified"},
    IngressDefer: {"reason"},
    IngressDenial: {"reason", "observation_index"},
    IngressAdmission: {"source", "device_integration", "device", "observations"},
    TrustedIngress: {"context", "observations"},
    IngressQueueMessage: {"message_id", "receipt_handle", "body", "receive_count", "sent_at"},
}


@pytest.mark.parametrize("model", list(MODEL_FIELDS), ids=lambda model: model.__name__)
def test_model_field_sets_are_frozen(model: type) -> None:
    assert _fields(model) == MODEL_FIELDS[model]


@pytest.mark.parametrize(
    "model",
    [model for model in MODEL_FIELDS if issubclass(model, BaseModel)],
    ids=lambda model: model.__name__,
)
def test_models_forbid_extra_fields(model: type[BaseModel]) -> None:
    assert model.model_config.get("extra") == "forbid"


@pytest.mark.parametrize(
    "model",
    [
        model
        for model in MODEL_FIELDS
        if model not in {IngestSourceObservation, IngestObservationBatch}
    ],
    ids=lambda model: model.__name__,
)
def test_contract_values_are_frozen(model: type) -> None:
    if dataclasses.is_dataclass(model):
        assert model.__dataclass_params__.frozen  # type: ignore[attr-defined]
    else:
        assert model.model_config.get("frozen") is True


def test_ingest_credential_validates_fingerprint_and_revocation() -> None:
    assert credential().fingerprint == credential().secret_sha256[:8]
    with pytest.raises(ValidationError):
        credential(fingerprint="00000000")
    with pytest.raises(ValidationError):
        credential(slot=3)
    with pytest.raises(ValidationError):
        credential(ingest_credential_id="ic_" + "A" * 32)
    with pytest.raises(ValidationError):  # revoked needs revoked_at
        credential(status=IngestCredentialStatus.REVOKED)
    with pytest.raises(ValidationError):  # active never carries revoked_at
        credential(revoked_at=RECEIVED)
    revoked = credential(status=IngestCredentialStatus.REVOKED, revoked_at=RECEIVED)
    assert revoked.revoked_at == RECEIVED


# Ingest request schema -----------------------------------------------------------------------


def _observation(**overrides: Any) -> dict[str, Any]:
    item: dict[str, Any] = {
        "metric_key": "temperature",
        "value": 27.5,
        "source_unit": "C",
        "observed_at": "2026-10-30T11:59:00Z",
    }
    item.update(overrides)
    return item


def _batch(observations: list[dict[str, Any]], **overrides: Any) -> dict[str, Any]:
    return {"external_device_id": "sensor-01", "observations": observations, **overrides}


@pytest.mark.parametrize("count", [1, MAX_OBSERVATIONS_PER_BATCH])
def test_batch_accepts_one_to_one_thousand_observations(count: int) -> None:
    batch = IngestObservationBatch.model_validate(_batch([_observation()] * count))

    assert len(batch.observations) == count
    assert batch.observations[0].component_id is None


@pytest.mark.parametrize("count", [0, MAX_OBSERVATIONS_PER_BATCH + 1])
def test_batch_rejects_empty_and_oversized_batches(count: int) -> None:
    with pytest.raises(ValidationError):
        IngestObservationBatch.model_validate(_batch([_observation()] * count))


@pytest.mark.parametrize(
    "field", ["tenant_id", "site_id", "asset_id", "deployment_id", "pond_id", "device_id"]
)
def test_payload_ownership_fields_are_rejected_at_both_levels(field: str) -> None:
    with pytest.raises(ValidationError):
        IngestObservationBatch.model_validate(_batch([_observation()], **{field: "x"}))
    with pytest.raises(ValidationError):
        IngestObservationBatch.model_validate(_batch([_observation(**{field: "x"})]))


@pytest.mark.parametrize(
    "external_device_id",
    ["", " sensor", "sensor ", "sen\x00sor", "sen\x1fsor", "sen\x7fsor", "x" * 513],
    ids=["empty", "leading-space", "trailing-space", "nul", "unit-separator", "del", "513"],
)
def test_batch_rejects_bad_external_device_ids(external_device_id: str) -> None:
    with pytest.raises(ValidationError):
        IngestObservationBatch.model_validate(
            _batch([_observation()], external_device_id=external_device_id)
        )


def _components(count: int) -> list[str]:
    return [f"cmp_{index:032x}" for index in range(count)]


def test_batch_accepts_thirty_two_distinct_components() -> None:
    observations = [_observation(component_id=item) for item in _components(32)]
    observations += [_observation(), _observation(component_id=_components(1)[0])]

    batch = IngestObservationBatch.model_validate(_batch(observations))

    assert len({item.component_id for item in batch.observations} - {None}) == 32
    assert ingestion.MAX_INGEST_DISTINCT_COMPONENTS == 32


def test_batch_rejects_more_than_thirty_two_distinct_components() -> None:
    observations = [_observation(component_id=item) for item in _components(33)]
    body = json.dumps(_batch(observations))

    with pytest.raises(ValidationError) as exc_info:
        IngestObservationBatch.model_validate_json(body)

    (error,) = exc_info.value.errors()
    assert error["type"] == "value_error"
    assert "at most 32 distinct component_id values" in error["msg"]
    assert redacted_validation_detail(exc_info.value) == [
        {"loc": ["body"], "msg": "invalid value", "type": "value_error"}
    ]


def test_batch_accepts_the_longest_external_device_id() -> None:
    batch = IngestObservationBatch.model_validate(
        _batch([_observation()], external_device_id="x" * 512)
    )

    assert batch.external_device_id == "x" * 512


def test_external_identity_constants_moved_to_the_domain_layer() -> None:
    assert v2_common.EXTERNAL_IDENTITY_MAX_LENGTH == 512
    assert v2_common.EXTERNAL_IDENTITY_PATTERN == (
        r"^[^\s\x00-\x1f\x7f](?:[^\x00-\x1f\x7f]*[^\s\x00-\x1f\x7f])?$"
    )
    assert integration_schemas.EXTERNAL_IDENTITY_PATTERN is v2_common.EXTERNAL_IDENTITY_PATTERN
    assert (
        integration_schemas.EXTERNAL_IDENTITY_MAX_LENGTH == v2_common.EXTERNAL_IDENTITY_MAX_LENGTH
    )
    source = Path(v2_common.__file__).read_text(encoding="utf-8") + Path(
        ingestion.__file__
    ).read_text(encoding="utf-8")
    assert "limnopulse_api.api" not in source
    assert "limnopulse_api.adapters" not in source


@pytest.mark.parametrize(
    "overrides",
    [
        {"component_id": "cmp_" + "A" * 32},
        {"metric_key": ""},
        {"metric_key": "m" * 65},
        {"value": True},
        {"value": "1.0"},
        {"source_unit": "u" * 17},
        {"source_unit": "\x07"},
        {"observed_at": 1_700_000_000},
        {"observed_at": "2026-10-30T11:59:00"},
        {"observed_at": "2262-01-01T00:00:00Z"},
        {"observed_at": "2026-10-30T11:59:00.0000001Z"},
        {"sequence": -1},
        {"sequence": 2**63},
        {"source_event_id": ""},
        {"source_event_id": "e" * 129},
        {"source_metadata": {"serial": "1"}},
        {"source_metadata": {"firmware_version": "v" * 65}},
    ],
    ids=lambda overrides: next(iter(overrides)) + "-" + str(next(iter(overrides.values())))[:12],
)
def test_ingest_observations_reuse_the_source_observation_validators(
    overrides: dict[str, Any],
) -> None:
    with pytest.raises(ValidationError):
        IngestObservationBatch.model_validate(_batch([_observation(**overrides)]))
    with pytest.raises(ValidationError):
        SourceObservation.model_validate(
            {**_observation(**overrides), "component_id": COMPONENT, **overrides}
        )


def test_ingest_observation_keeps_every_optional_field() -> None:
    raw = _observation(
        component_id=COMPONENT,
        sequence=7,
        source_event_id="evt-1",
        source_metadata={"firmware_version": "1.2.3"},
    )

    observation = IngestObservationBatch.model_validate(_batch([raw])).observations[0]

    assert observation.component_id == COMPONENT
    assert observation.sequence == 7
    assert observation.source_event_id == "evt-1"
    assert dict(observation.source_metadata) == {"firmware_version": "1.2.3"}
    assert observation.observed_at == OBSERVED


# Envelope ------------------------------------------------------------------------------------


def test_envelope_serialization_is_compact_and_omits_defaults() -> None:
    message = envelope()

    text = message.to_json()
    payload = json.loads(text)

    assert text == json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    assert payload == {
        "schema": "telemetry-ingress@1",
        "ingress_id": INGRESS,
        "tenant_id": TENANT,
        "integration_account_id": ACCOUNT,
        "provider_key": PROVIDER,
        "external_identity_hash": IDENTITY_HASH,
        "device_integration_id": INTEGRATION,
        "device_id": DEVICE,
        "source_type": "https",
        "received_at": "2026-10-30T12:00:00.000000Z",
        "mapping_versions": {
            "catalog_version": "metric-catalog@1",
            "unit_mapping_version": "unit-mapping@1",
            "quality_policy_version": "quality-policy@1",
        },
        "observations": [
            source_observation().model_dump(mode="json", exclude_none=True, exclude_defaults=True)
        ],
    }
    assert payload["observations"][0]["component_id"] == COMPONENT
    assert "source_metadata" not in payload["observations"][0]


def test_envelope_always_writes_the_three_mapping_versions_literally() -> None:
    text = envelope().to_json()

    for literal in (
        '"catalog_version":"metric-catalog@1"',
        '"unit_mapping_version":"unit-mapping@1"',
        '"quality_policy_version":"quality-policy@1"',
    ):
        assert literal in text


@pytest.mark.parametrize("version", ["!", "~" * 64, "gw/1.0+build.7"])
def test_envelope_accepts_printable_ascii_connector_versions(version: str) -> None:
    assert envelope(connector_version=version).connector_version == version


def test_envelope_round_trip_is_lossless_with_every_optional_field() -> None:
    message = envelope(
        replay_of="ing_" + "5" * 32,
        replay=True,
        connector_version="gw/1.0+build.7",
        received_at=datetime(2026, 10, 30, 12, 0, 0, 123456, tzinfo=UTC),
        mapping_versions=NormalizationVersions(catalog_version="metric-catalog@0"),
        observations=[
            source_observation(
                sequence=3,
                source_event_id="evt-1",
                source_metadata={"firmware_version": "fw " + EMOJI},
            ),
            source_observation(component_id=OTHER_COMPONENT, value=4),
        ],
    )

    text = message.to_json()
    parsed = IngressEnvelope.model_validate_json(text)

    assert parsed == message
    assert parsed.to_json() == text
    payload = json.loads(text)
    assert payload["replay"] is True
    assert payload["replay_of"] == "ing_" + "5" * 32
    assert payload["connector_version"] == "gw/1.0+build.7"
    assert payload["received_at"] == "2026-10-30T12:00:00.123456Z"
    assert payload["mapping_versions"]["unit_mapping_version"] == "unit-mapping@1"
    assert payload["observations"][1]["value"] == 4
    assert EMOJI in text


@pytest.mark.parametrize(
    "overrides",
    [
        {"schema": "telemetry-ingress@2"},
        {"schema": None},
        {"ingress_id": "ing_" + "A" * 32},
        {"replay_of": "rpl_" + "5" * 32},
        {"external_identity_hash": "E" * 64},
        {"device_integration_id": "di-plain"},
        {"connector_version": "has space"},
        {"connector_version": ""},
        {"connector_version": "v\x7f"},
        {"connector_version": "v\u00e9"},
        {"connector_version": "v" * 65},
        {"observations": []},
        {"unexpected": 1},
    ],
    ids=lambda overrides: next(iter(overrides)),
)
def test_envelope_rejects_unknown_schema_and_bad_fields(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        envelope(**overrides)


def test_envelope_requires_its_schema_and_mapping_versions_on_the_wire() -> None:
    payload = json.loads(envelope().to_json())
    for field in ("schema", "mapping_versions"):
        broken = {key: value for key, value in payload.items() if key != field}
        with pytest.raises(ValidationError):
            IngressEnvelope.model_validate_json(json.dumps(broken))


def test_envelope_received_at_is_canonical_utc() -> None:
    offset = datetime.fromisoformat("2026-10-30T09:00:00-03:00")

    payload = json.loads(envelope(received_at=offset).to_json())

    assert payload["received_at"] == "2026-10-30T12:00:00.000000Z"
    assert envelope(received_at=offset).received_at == RECEIVED


def test_ingress_message_bytes_counts_utf8_body_and_attribute_bytes() -> None:
    message = envelope(
        observations=[source_observation(source_metadata={"firmware_version": EMOJI})]
    )

    assert ingress_message_bytes(message) == len(message.to_json().encode("utf-8")) + 31


def _largest_body(make: Callable[[int, int], str], count: int, max_pad: int) -> bytes:
    prefix = '{"external_device_id":"' + "x" * 512 + '","observations":['

    def body(pad: int) -> bytes:
        observations = ",".join(make(index, pad) for index in range(count))
        return (prefix + observations + "]}").encode("utf-8")

    pad = 1
    assert len(body(pad)) <= ingestion.MAX_INGEST_BODY_BYTES
    while pad < max_pad and len(body(pad + 1)) <= ingestion.MAX_INGEST_BODY_BYTES:
        pad += 1
    return body(pad)


def _many_small(index: int, pad: int) -> str:
    # `1E15` and `.1Z` re-serialize longer; component_id is filled in by admission.
    return (
        '{"metric_key":"t","value":1E15,"source_unit":"C",'
        f'"observed_at":"2026-10-30T11:59:00.1Z","sequence":{index},'
        f'"source_event_id":"{"e" * pad}"}}'
    )


def _many_emoji(index: int, pad: int) -> str:
    return (
        '{"metric_key":"t","value":1E15,"source_unit":"C",'
        '"observed_at":"2026-10-30T11:59:00.1Z",'
        f'"source_metadata":{{"firmware_version":"{EMOJI * pad}"}}}}'
    )


def _every_field_at_max(index: int, pad: int) -> str:
    return (
        f'{{"metric_key":"{"m" * 64}","value":1E15,"source_unit":"{"u" * 16}",'
        '"observed_at":"2026-10-30T11:59:00.1Z","sequence":9223372036854775807,'
        f'"source_event_id":"{"e" * 128}",'
        f'"source_metadata":{{"firmware_version":"{EMOJI * pad}"}}}}'
    )


@pytest.mark.parametrize(
    ("make", "count", "max_pad"),
    [
        (_many_small, 1_000, 128),
        (_many_emoji, 1_000, 64),
        (_every_field_at_max, 300, 64),
    ],
    ids=["1000-small", "1000-emoji", "every-field-at-max"],
)
def test_largest_valid_body_never_exceeds_the_message_guard(
    make: Callable[[int, int], str], count: int, max_pad: int
) -> None:
    body = _largest_body(make, count, max_pad)
    batch = IngestObservationBatch.model_validate_json(body)
    message = envelope(
        tenant_id="t" * 128,
        device_id="d" * 128,
        replay=True,
        replay_of="ing_" + "5" * 32,
        connector_version="v" * 64,
        observations=[
            SourceObservation.model_validate(
                {**item.model_dump(mode="json"), "component_id": COMPONENT}
            )
            for item in batch.observations
        ],
    )

    size = ingress_message_bytes(message)

    assert len(body) > ingestion.MAX_INGEST_BODY_BYTES - 4_096
    assert size == len(message.to_json().encode("utf-8")) + ingestion.INGRESS_SCHEMA_ATTRIBUTE_BYTES
    assert size <= ingestion.MAX_INGRESS_MESSAGE_BYTES


def test_kit_size_helpers_build_exact_envelopes() -> None:
    limit = ingestion.MAX_INGRESS_MESSAGE_BYTES

    for target in (limit, limit + 1):
        message = envelope_of_message_bytes(target)
        assert len(message.to_json().encode("utf-8")) + 31 == target
        assert len(message.observations) <= ingestion.MAX_INGEST_OBSERVATIONS
    multibyte = multibyte_envelope().to_json()
    assert len(multibyte) + 31 <= limit < len(multibyte.encode("utf-8")) + 31


def test_unicode_expansion_vector_stays_within_the_bound() -> None:
    body = _largest_body(_many_emoji, 1_000, 64)
    batch = IngestObservationBatch.model_validate_json(body)
    message = envelope(
        observations=[
            SourceObservation.model_validate(
                {**item.model_dump(mode="json"), "component_id": COMPONENT}
            )
            for item in batch.observations
        ]
    )

    text = message.to_json()

    assert EMOJI in text and "\\ud83d" not in text
    assert ingress_message_bytes(message) <= ingestion.MAX_INGRESS_MESSAGE_BYTES


# Receipts ------------------------------------------------------------------------------------


def test_epoch_microseconds_is_exact_integer_arithmetic() -> None:
    instant = datetime(2200, 1, 1, 0, 0, 0, 1, tzinfo=UTC)

    assert epoch_microseconds(instant) == 7_258_118_400_000_001
    assert epoch_microseconds(datetime(1970, 1, 1, tzinfo=UTC)) == 0
    with pytest.raises(ValueError):
        epoch_microseconds(datetime(2026, 1, 1))


def test_new_receipt_holds_the_ttl_invariant() -> None:
    created = datetime(2026, 10, 30, 12, 0, 0, 123456, tzinfo=UTC)
    created_us = 1_793_361_600_123_456

    receipt = new_ingress_receipt(
        integration_account_id=ACCOUNT,
        tenant_id=TENANT,
        key_sha256="c" * 64,
        request_sha256="d" * 64,
        ingress_id=INGRESS,
        received_at=created,
        mapping_versions=NormalizationVersions(),
        created_at=created,
    )

    assert receipt.state == IngressReceiptState.PENDING
    assert receipt.created_at_us == created_us == epoch_microseconds(created)
    assert receipt.expires_at == math.ceil((created + timedelta(hours=24)).timestamp())
    assert receipt.expires_at == (created_us + 86_400_000_000 + 999_999) // 1_000_000
    whole = new_ingress_receipt(
        integration_account_id=ACCOUNT,
        tenant_id=TENANT,
        key_sha256="c" * 64,
        request_sha256="d" * 64,
        ingress_id=INGRESS,
        received_at=RECEIVED,
        mapping_versions=NormalizationVersions(),
        created_at=RECEIVED,
    )
    assert whole.expires_at == int((RECEIVED + timedelta(hours=24)).timestamp())


@pytest.mark.parametrize(
    "overrides",
    [
        {"expires_at": 1_793_448_000},
        {"created_at_us": 1_793_361_600_123_457},
        {"key_sha256": "C" * 64},
        {"ingress_id": "ing_" + "A" * 32},
        {"created_at": datetime(2026, 10, 30, 12, 0, 0, 123456)},
    ],
    ids=["expires_at", "created_at_us", "key", "ingress", "naive"],
)
def test_receipt_rejects_inconsistent_fields(overrides: dict[str, Any]) -> None:
    created = datetime(2026, 10, 30, 12, 0, 0, 123456, tzinfo=UTC)
    payload: dict[str, Any] = {
        "integration_account_id": ACCOUNT,
        "tenant_id": TENANT,
        "key_sha256": "c" * 64,
        "request_sha256": "d" * 64,
        "ingress_id": INGRESS,
        "received_at": created,
        "mapping_versions": NormalizationVersions(),
        "state": "pending",
        "created_at": created,
        "created_at_us": 1_793_361_600_123_456,
        "expires_at": 1_793_448_001,
    }
    IngressReceipt.model_validate(payload)

    with pytest.raises(ValidationError):
        IngressReceipt.model_validate({**payload, **overrides})


def _receipt(
    created: datetime, state: IngressReceiptState = IngressReceiptState.PENDING
) -> IngressReceipt:
    return new_ingress_receipt(
        integration_account_id=ACCOUNT,
        tenant_id=TENANT,
        key_sha256="c" * 64,
        request_sha256="d" * 64,
        ingress_id=INGRESS,
        received_at=created,
        mapping_versions=NormalizationVersions(),
        created_at=created,
    ).model_copy(update={"state": state})


def test_receipt_liveness_uses_only_created_at_us() -> None:
    live = _receipt(RECEIVED)
    deadline = RECEIVED + timedelta(hours=24)

    assert ingress_receipt_cutoff_us(deadline) == epoch_microseconds(deadline) - 86_400_000_000
    assert is_ingress_receipt_live(live, deadline - timedelta(microseconds=1)) is True
    assert is_ingress_receipt_live(live, deadline) is False
    # The TTL epoch is rounded up, so it can lie in the future of an expired receipt.
    fractional = _receipt(RECEIVED + timedelta(microseconds=1))
    later = RECEIVED + timedelta(hours=24, microseconds=1)
    assert fractional.expires_at > later.timestamp()
    assert is_ingress_receipt_live(fractional, later) is False
    assert fractional.created_at_us <= ingress_receipt_cutoff_us(later)


@pytest.mark.parametrize(
    ("state", "age", "same", "expected"),
    [
        (None, timedelta(0), True, IngressReceiptAction.ADMIT),
        (IngressReceiptState.ENQUEUED, timedelta(hours=24), True, IngressReceiptAction.ADMIT),
        (IngressReceiptState.PENDING, timedelta(hours=24), False, IngressReceiptAction.ADMIT),
        (IngressReceiptState.ENQUEUED, timedelta(hours=1), True, IngressReceiptAction.REPLAY),
        (IngressReceiptState.PENDING, timedelta(hours=1), True, IngressReceiptAction.RESUME),
        (IngressReceiptState.PENDING, timedelta(0), False, IngressReceiptAction.CONFLICT),
        (IngressReceiptState.ENQUEUED, timedelta(hours=23), False, IngressReceiptAction.CONFLICT),
    ],
)
def test_receipt_replay_table(
    state: IngressReceiptState | None,
    age: timedelta,
    same: bool,
    expected: IngressReceiptAction,
) -> None:
    now = RECEIVED + age
    stored = None if state is None else _receipt(RECEIVED, state)

    action = decide_receipt_replay(stored, request_sha256="d" * 64 if same else "e" * 64, now=now)

    assert action == expected


# Quarantine rows -----------------------------------------------------------------------------


def _quarantined(
    reason: QuarantineReason, source: dict[str, Any] | str, **overrides: Any
) -> QuarantinedObservation:
    if isinstance(source, str):
        excerpt, truncated = source, False
    else:
        excerpt, truncated = bounded_excerpt(source)
    payload: dict[str, Any] = {
        "reason": reason,
        "tenant_id": TENANT,
        "device_id": DEVICE,
        "component_id": COMPONENT,
        "metric_key": "temperature",
        "observed_at": OBSERVED,
        "received_at": RECEIVED,
        "source_event_id": None,
        "detail": "rejected",
        "source_excerpt_json": excerpt,
        "truncated": truncated,
    }
    payload.update(overrides)
    return QuarantinedObservation.model_validate(payload)


def _outcome(*quarantined: QuarantinedObservation) -> PipelineOutcome:
    return PipelineOutcome(
        written=None, quarantined=tuple(quarantined), counts=MappingProxyType({})
    )  # type: ignore[arg-type]


def test_quarantine_rows_for_normalizer_outcomes() -> None:
    source = source_observation().model_dump(mode="json")
    same = _quarantined(QuarantineReason.METRIC_UNKNOWN, source)
    again = _quarantined(QuarantineReason.UNIT_UNSUPPORTED, source)
    broken = _quarantined(QuarantineReason.VALUE_INVALID, '{"value":"NaN"}')
    cut = _quarantined(QuarantineReason.VALUE_INVALID, '"' + "x" * 100 + '"', truncated=True)
    message = envelope(replay_of="ing_" + "5" * 32, replay=True, connector_version="gw/1")
    now = RECEIVED + timedelta(minutes=1)

    rows = build_quarantine_entries(message, outcome=_outcome(same, again, broken, cut), now=now)

    assert [row.kind for row in rows] == [QuarantineEntryKind.OBSERVATION] * 4
    assert [row.reason for row in rows] == [
        "metric_unknown",
        "unit_unsupported",
        "value_invalid",
        "value_invalid",
    ]
    assert [row.quarantine_id for row in rows] == [
        quarantine_entry_id(INGRESS, 0, same.source_excerpt_json),
        quarantine_entry_id(INGRESS, 1, same.source_excerpt_json),
        quarantine_entry_id(INGRESS, 0, broken.source_excerpt_json),
        quarantine_entry_id(INGRESS, 0, cut.source_excerpt_json),
    ]
    assert [row.observation for row in rows] == [same, again, broken, cut]
    assert [row.source_observation_json for row in rows] == [
        item.source_excerpt_json for item in (same, again, broken, cut)
    ]
    assert [row.replayable for row in rows] == [True, True, False, False]
    assert [row.truncated for row in rows] == [False, False, False, True]
    for row in rows:
        assert row.tenant_id == TENANT
        assert row.ingress_id == INGRESS
        assert row.replay_of == "ing_" + "5" * 32
        assert row.integration_account_id == ACCOUNT
        assert row.provider_key == PROVIDER
        assert row.external_identity_hash == IDENTITY_HASH
        assert row.source_type == SourceType.HTTPS
        assert row.connector_version == "gw/1"
        assert (row.device_integration_id, row.device_id) == (INTEGRATION, DEVICE)
        assert row.received_at == RECEIVED
        assert row.mapping_versions == message.mapping_versions
        assert row.envelope_excerpt_json is None
        assert row.status == QuarantineEntryStatus.OPEN
        assert row.replayed_ingress_id is None
        assert (row.created_at, row.updated_at, row.version) == (now, now, 1)
        assert row.expires_at == now + timedelta(days=14)
        assert row.entry_key == f"{canonical_timestamp_key(RECEIVED)}#{row.quarantine_id}"


def test_quarantine_rows_for_a_terminal_fence() -> None:
    first = source_observation()
    other = source_observation(component_id=OTHER_COMPONENT, sequence=4)
    message = envelope(observations=[first, first, other])
    fence = IngressFence(reason=IngressFenceReason.DEVICE_DECOMMISSIONED, account_verified=True)

    rows = build_quarantine_entries(message, fence=fence, now=RECEIVED)

    fenced = [row for row in rows if row.kind == QuarantineEntryKind.FENCED_OBSERVATION]
    (summary,) = [row for row in rows if row.kind == QuarantineEntryKind.FENCED_MESSAGE]
    assert len(rows) == 4
    assert {row.reason for row in rows} == {"fenced_device_decommissioned"}
    assert [
        SourceObservation.model_validate_json(row.source_observation_json) for row in fenced
    ] == [
        first,
        first,
        other,
    ]
    assert fenced[0].source_observation_json == fenced[1].source_observation_json
    assert [row.quarantine_id for row in fenced] == [
        quarantine_entry_id(INGRESS, 0, fenced[0].source_observation_json),
        quarantine_entry_id(INGRESS, 1, fenced[1].source_observation_json),
        quarantine_entry_id(INGRESS, 0, fenced[2].source_observation_json),
    ]
    assert all(row.replayable and not row.truncated and row.observation is None for row in fenced)
    assert all(row.envelope_excerpt_json is None for row in fenced)
    assert summary.envelope_excerpt_json == message.to_json()
    assert summary.quarantine_id == quarantine_entry_id(INGRESS, -1, message.to_json())
    assert summary.source_observation_json is None and summary.observation is None
    assert (summary.replayable, summary.truncated) == (False, False)
    assert len({row.quarantine_id for row in rows}) == 4


def _envelope_cut_inside_an_emoji() -> IngressEnvelope:
    observations = [
        source_observation(sequence=index, source_metadata={"firmware_version": EMOJI * 64})
        for index in range(12)
    ]
    for shift in range(256):
        message = envelope(
            tenant_id="t" * (1 + min(shift, 127)),
            device_id="d" * (1 + max(0, shift - 127)),
            observations=observations,
        )
        try:
            message.to_json().encode("utf-8")[:2_048].decode("utf-8")
        except UnicodeDecodeError:
            return message
    raise AssertionError("no layout puts byte 2048 inside a multi-byte character")


def test_fenced_message_excerpt_is_cut_on_a_utf8_boundary() -> None:
    message = _envelope_cut_inside_an_emoji()
    full = message.to_json().encode("utf-8")
    fence = IngressFence(reason=IngressFenceReason.ACCOUNT_REVOKED, account_verified=True)

    rows = build_quarantine_entries(message, fence=fence, now=RECEIVED)

    (summary,) = [row for row in rows if row.kind == QuarantineEntryKind.FENCED_MESSAGE]
    excerpt = summary.envelope_excerpt_json.encode("utf-8")
    assert len(full) > 2_048
    assert 2_048 - 4 < len(excerpt) < 2_048  # byte 2048 splits an emoji, so the cut backs off
    assert excerpt.decode("utf-8") == summary.envelope_excerpt_json
    assert full.startswith(excerpt)
    assert summary.truncated is True and summary.replayable is False


def test_unverified_fences_write_no_rows() -> None:
    fence = IngressFence(reason=IngressFenceReason.IDENTITY_UNRESOLVED, account_verified=False)

    assert build_quarantine_entries(envelope(), fence=fence, now=RECEIVED) == ()


def test_quarantine_builder_needs_exactly_one_input() -> None:
    fence = IngressFence(reason=IngressFenceReason.ACCOUNT_REVOKED, account_verified=True)
    with pytest.raises(ValueError):
        build_quarantine_entries(envelope(), now=RECEIVED)
    with pytest.raises(ValueError):
        build_quarantine_entries(envelope(), outcome=_outcome(), fence=fence, now=RECEIVED)


def test_quarantine_entry_enforces_the_replayable_rule_and_bounds() -> None:
    fence = IngressFence(reason=IngressFenceReason.ACCOUNT_REVOKED, account_verified=True)
    observation_row, summary = build_quarantine_entries(envelope(), fence=fence, now=RECEIVED)
    cut = build_quarantine_entries(
        envelope(),
        outcome=_outcome(
            _quarantined(QuarantineReason.VALUE_INVALID, '"' + "x" * 10 + '"', truncated=True)
        ),
        now=RECEIVED,
    )[0]

    def rebuild(row: QuarantineEntry, **changes: Any) -> None:
        QuarantineEntry.model_validate({**row.model_dump(), **changes})

    rebuild(observation_row)
    with pytest.raises(ValidationError):
        rebuild(summary, replayable=True)
    with pytest.raises(ValidationError):
        rebuild(observation_row, replayable=False)
    with pytest.raises(ValidationError):
        rebuild(observation_row, source_observation_json='{"value":1}')
    with pytest.raises(ValidationError):
        rebuild(cut, replayable=True)
    # Truncated but still parseable as a SourceObservation: the truncation alone forbids replay.
    parseable = observation_row.source_observation_json
    with pytest.raises(ValidationError):
        rebuild(observation_row, truncated=True)
    rebuild(observation_row, truncated=True, replayable=False, source_observation_json=parseable)
    with pytest.raises(ValidationError):
        rebuild(summary, envelope_excerpt_json='"' + "x" * 2_047 + '"')
    with pytest.raises(ValidationError):
        rebuild(observation_row, expires_at=RECEIVED + timedelta(days=15))
    with pytest.raises(ValidationError):
        rebuild(observation_row, quarantine_id="qtn_" + "Z" * 32)


# Audit items ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("action", "resource_type", "before", "after", "fingerprint"),
    [
        ("ingest_credential.issued", "ingest_credential", None, "active", "7a04a393"),
        ("ingest_credential.revoked", "ingest_credential", "active", "revoked", "7a04a393"),
        ("integration_account.status_changed", "integration_account", "pending", "active", None),
        ("device_integration.status_changed", "device_integration", "active", "error", None),
    ],
)
def test_ingest_audit_item_layout(
    action: str, resource_type: str, before: str | None, after: str, fingerprint: str | None
) -> None:
    now = datetime(2026, 10, 30, 12, 0, 0, 5, tzinfo=UTC)

    item = ingest_audit_item(
        action=action,
        tenant_id=TENANT,
        resource_id=CREDENTIAL,
        before_status=before,
        after_status=after,
        context=AUDIT,
        now=now,
        fingerprint=fingerprint,
    )

    event_id = item["event_id"]
    assert re.fullmatch(r"audit_[0-9a-f]{32}", event_id)
    assert item == {
        "PK": f"TENANT#{TENANT}#MONTH#2026-10",
        "SK": f"{now.isoformat()}#{event_id}",
        "entity_type": "audit_event",
        "event_id": event_id,
        "tenant_id": TENANT,
        "actor_type": "user",
        "actor_id": "user-1",
        "action": action,
        "resource_type": resource_type,
        "resource_id": CREDENTIAL,
        "before_status": before,
        "after_status": after,
        "fingerprint": fingerprint,
        "ip": "203.0.113.7",
        "user_agent": "kit",
        "created_at": now.isoformat(),
        "expires_at": int((now + timedelta(days=90)).timestamp()),
    }


def test_ingest_audit_item_refuses_unknown_actions_and_digests() -> None:
    common: dict[str, Any] = {
        "tenant_id": TENANT,
        "resource_id": CREDENTIAL,
        "before_status": None,
        "after_status": "active",
        "context": AUDIT,
        "now": RECEIVED,
    }
    with pytest.raises(ValueError):
        ingest_audit_item(action="alert_rule.created", **common)
    with pytest.raises(ValueError):
        ingest_audit_item(
            action="ingest_credential.issued", fingerprint=ingest_secret_sha256(SECRET), **common
        )
    first = ingest_audit_item(action="ingest_credential.issued", **common)
    second = ingest_audit_item(action="ingest_credential.issued", **common)
    assert first["event_id"] != second["event_id"]


# Condition -> enum table -----------------------------------------------------------------------

ADMIT_ROWS: list[tuple[str, dict[str, Any], IngressAdmissionDenied | None]] = [
    ("all active", {}, None),
    ("identity unresolved", {"integration": None}, IngressAdmissionDenied.IDENTITY_UNRESOLVED),
    (
        "tenant mismatch",
        {"integration": {"tenant_id": OTHER_TENANT}},
        IngressAdmissionDenied.INTEGRATION_MISMATCH,
    ),
    (
        "account mismatch",
        {"integration": {"integration_account_id": OTHER_ACCOUNT}},
        IngressAdmissionDenied.INTEGRATION_MISMATCH,
    ),
    (
        "provider mismatch",
        {"integration": {"provider_key": "https_other"}},
        IngressAdmissionDenied.INTEGRATION_MISMATCH,
    ),
    (
        "integration decommissioned",
        {"integration": {"status": DeviceIntegrationStatus.DECOMMISSIONED}},
        IngressAdmissionDenied.INTEGRATION_INACTIVE,
    ),
    (
        "integration provisioning",
        {"integration": {"status": DeviceIntegrationStatus.PROVISIONING}},
        IngressAdmissionDenied.INTEGRATION_INACTIVE,
    ),
    (
        "integration error",
        {"integration": {"status": DeviceIntegrationStatus.ERROR}},
        IngressAdmissionDenied.INTEGRATION_INACTIVE,
    ),
    (
        "account revoked",
        {"account": {"status": IntegrationAccountStatus.REVOKED}},
        IngressAdmissionDenied.ACCOUNT_INACTIVE,
    ),
    (
        "account not https",
        {"account": {"provider_key": "vendor_cloud"}},
        IngressAdmissionDenied.ACCOUNT_INACTIVE,
    ),
    (
        "account pending",
        {"account": {"status": IntegrationAccountStatus.PENDING}},
        IngressAdmissionDenied.ACCOUNT_INACTIVE,
    ),
    (
        "account error",
        {"account": {"status": IntegrationAccountStatus.ERROR}},
        IngressAdmissionDenied.ACCOUNT_INACTIVE,
    ),
    ("device missing", {"device": None}, IngressAdmissionDenied.DEVICE_INACTIVE),
    (
        "device decommissioned",
        {"device": {"status": DeviceStatus.DECOMMISSIONED}},
        IngressAdmissionDenied.DEVICE_INACTIVE,
    ),
    (
        "device provisioning",
        {"device": {"status": DeviceStatus.PROVISIONING}},
        IngressAdmissionDenied.DEVICE_INACTIVE,
    ),
    (
        "device suspended",
        {"device": {"status": DeviceStatus.SUSPENDED}},
        IngressAdmissionDenied.DEVICE_INACTIVE,
    ),
    (
        "device of another tenant",
        {"device": {"tenant_id": OTHER_TENANT}},
        IngressAdmissionDenied.DEVICE_INACTIVE,
    ),
    (
        "device row for another device",
        {"device": {"device_id": OTHER_DEVICE}},
        IngressAdmissionDenied.DEVICE_INACTIVE,
    ),
    (
        "first failure wins",
        {
            "integration": {"status": DeviceIntegrationStatus.DECOMMISSIONED},
            "account": {"status": IntegrationAccountStatus.REVOKED},
            "device": None,
        },
        IngressAdmissionDenied.INTEGRATION_INACTIVE,
    ),
    (
        "account before device",
        {"account": {"status": IntegrationAccountStatus.REVOKED}, "device": None},
        IngressAdmissionDenied.ACCOUNT_INACTIVE,
    ),
]


def _rows(case: dict[str, Any]) -> tuple[Any, Any, Any]:
    def build(name: str, factory: Callable[..., Any]) -> Any:
        if name not in case:
            return factory()
        return None if case[name] is None else factory(**case[name])

    return (
        build("integration", device_integration),
        build("account", integration_account),
        build("device", device),
    )


@pytest.mark.parametrize(
    ("name", "case", "expected"), ADMIT_ROWS, ids=[row[0] for row in ADMIT_ROWS]
)
def test_admission_condition_table(
    name: str, case: dict[str, Any], expected: IngressAdmissionDenied | None
) -> None:
    integration, account, found_device = _rows(case)

    assert classify_admission(SOURCE, integration, account, found_device) == expected


def _fence(reason: IngressFenceReason, verified: bool = True) -> IngressFence:
    return IngressFence(reason=reason, account_verified=verified)


FENCE_ROWS: list[tuple[str, dict[str, Any], dict[str, Any], object]] = [
    ("trusted", {}, {}, None),
    (
        "identity unresolved",
        {"integration": None},
        {},
        _fence(IngressFenceReason.IDENTITY_UNRESOLVED),
    ),
    (
        "identity unresolved, no account",
        {"integration": None, "account": None},
        {},
        _fence(IngressFenceReason.IDENTITY_UNRESOLVED, False),
    ),
    (
        "tenant mismatch",
        {},
        {"tenant_id": OTHER_TENANT},
        _fence(IngressFenceReason.INTEGRATION_MISMATCH, False),
    ),
    (
        "account mismatch",
        {},
        {"integration_account_id": OTHER_ACCOUNT},
        _fence(IngressFenceReason.INTEGRATION_MISMATCH, False),
    ),
    (
        "provider mismatch",
        {"integration": {"provider_key": "https_other"}},
        {},
        _fence(IngressFenceReason.INTEGRATION_MISMATCH),
    ),
    (
        "integration id mismatch",
        {},
        {"device_integration_id": OTHER_INTEGRATION},
        _fence(IngressFenceReason.INTEGRATION_MISMATCH),
    ),
    (
        "device id mismatch",
        {},
        {"device_id": OTHER_DEVICE},
        _fence(IngressFenceReason.INTEGRATION_MISMATCH),
    ),
    (
        "integration decommissioned",
        {"integration": {"status": DeviceIntegrationStatus.DECOMMISSIONED}},
        {},
        _fence(IngressFenceReason.INTEGRATION_DECOMMISSIONED),
    ),
    (
        "integration provisioning",
        {"integration": {"status": DeviceIntegrationStatus.PROVISIONING}},
        {},
        IngressDefer(reason=IngressDeferReason.INTEGRATION_NOT_ACTIVE),
    ),
    (
        "integration error",
        {"integration": {"status": DeviceIntegrationStatus.ERROR}},
        {},
        IngressDefer(reason=IngressDeferReason.INTEGRATION_NOT_ACTIVE),
    ),
    (
        "account missing",
        {"account": None},
        {},
        _fence(IngressFenceReason.IDENTITY_UNRESOLVED, False),
    ),
    (
        "integration decommissioned, no account",
        {"integration": {"status": DeviceIntegrationStatus.DECOMMISSIONED}, "account": None},
        {},
        _fence(IngressFenceReason.INTEGRATION_DECOMMISSIONED, False),
    ),
    (
        "integration error, no account",
        {"integration": {"status": DeviceIntegrationStatus.ERROR}, "account": None},
        {},
        IngressDefer(reason=IngressDeferReason.INTEGRATION_NOT_ACTIVE),
    ),
    (
        "account of another tenant",
        {"account": {"tenant_id": OTHER_TENANT}},
        {},
        _fence(IngressFenceReason.IDENTITY_UNRESOLVED, False),
    ),
    (
        "account revoked",
        {"account": {"status": IntegrationAccountStatus.REVOKED}},
        {},
        _fence(IngressFenceReason.ACCOUNT_REVOKED),
    ),
    (
        "account not https",
        {"account": {"provider_key": "vendor_cloud"}},
        {},
        _fence(IngressFenceReason.ACCOUNT_REVOKED),
    ),
    (
        "account pending",
        {"account": {"status": IntegrationAccountStatus.PENDING}},
        {},
        IngressDefer(reason=IngressDeferReason.ACCOUNT_NOT_ACTIVE),
    ),
    (
        "account error",
        {"account": {"status": IntegrationAccountStatus.ERROR}},
        {},
        IngressDefer(reason=IngressDeferReason.ACCOUNT_NOT_ACTIVE),
    ),
    ("device missing", {"device": None}, {}, _fence(IngressFenceReason.DEVICE_DECOMMISSIONED)),
    (
        "device decommissioned",
        {"device": {"status": DeviceStatus.DECOMMISSIONED}},
        {},
        _fence(IngressFenceReason.DEVICE_DECOMMISSIONED),
    ),
    (
        "device provisioning",
        {"device": {"status": DeviceStatus.PROVISIONING}},
        {},
        IngressDefer(reason=IngressDeferReason.DEVICE_NOT_ACTIVE),
    ),
    (
        "device suspended",
        {"device": {"status": DeviceStatus.SUSPENDED}},
        {},
        IngressDefer(reason=IngressDeferReason.DEVICE_NOT_ACTIVE),
    ),
    (
        "device of another tenant",
        {"device": {"tenant_id": OTHER_TENANT}},
        {},
        _fence(IngressFenceReason.DEVICE_DECOMMISSIONED),
    ),
    (
        "integration row inconsistent with the identity",
        {"integration": {"external_identity_hash": "e" * 64}},
        {},
        _fence(IngressFenceReason.IDENTITY_UNRESOLVED),
    ),
    (
        "integration before account",
        {
            "integration": {"status": DeviceIntegrationStatus.ERROR},
            "account": {"status": IntegrationAccountStatus.REVOKED},
        },
        {},
        IngressDefer(reason=IngressDeferReason.INTEGRATION_NOT_ACTIVE),
    ),
    (
        "account before device",
        {"account": {"status": IntegrationAccountStatus.PENDING}, "device": None},
        {},
        IngressDefer(reason=IngressDeferReason.ACCOUNT_NOT_ACTIVE),
    ),
]


@pytest.mark.parametrize(
    ("name", "case", "forged", "expected"), FENCE_ROWS, ids=[row[0] for row in FENCE_ROWS]
)
def test_fence_condition_table(
    name: str, case: dict[str, Any], forged: dict[str, Any], expected: object
) -> None:
    integration, account, found_device = _rows(case)

    assert classify_fence(envelope(**forged), integration, account, found_device) == expected


def _ingest_observations(*component_ids: str | None) -> list[IngestSourceObservation]:
    return [
        IngestSourceObservation.model_validate(
            _observation() if item is None else _observation(component_id=item)
        )
        for item in component_ids
    ]


def test_component_selectors_fill_the_default() -> None:
    resolved = resolve_component_selectors(
        _ingest_observations(COMPONENT, None, COMPONENT),
        default_component_id=DEFAULT_COMPONENT,
        active_component_ids={COMPONENT, DEFAULT_COMPONENT},
    )

    assert resolved == (COMPONENT, DEFAULT_COMPONENT, COMPONENT)


@pytest.mark.parametrize(
    ("component_ids", "active", "expected"),
    [
        (
            (COMPONENT, OTHER_COMPONENT),
            {COMPONENT, DEFAULT_COMPONENT},
            IngressDenial(reason=IngressAdmissionDenied.COMPONENT_INVALID, observation_index=1),
        ),
        (
            (COMPONENT, None),
            {COMPONENT},
            IngressDenial(reason=IngressAdmissionDenied.COMPONENT_REQUIRED, observation_index=1),
        ),
        (
            (COMPONENT, OTHER_COMPONENT, None),
            {COMPONENT},
            IngressDenial(reason=IngressAdmissionDenied.COMPONENT_INVALID, observation_index=1),
        ),
        (
            (None, OTHER_COMPONENT),
            {COMPONENT},
            IngressDenial(reason=IngressAdmissionDenied.COMPONENT_REQUIRED, observation_index=0),
        ),
    ],
    ids=["invalid", "required", "first-invalid-wins", "first-required-wins"],
)
def test_component_selector_denials(
    component_ids: tuple[str | None, ...], active: set[str], expected: IngressDenial
) -> None:
    resolved = resolve_component_selectors(
        _ingest_observations(*component_ids),
        default_component_id=DEFAULT_COMPONENT,
        active_component_ids=active,
    )

    assert resolved == expected


def test_trusted_context_comes_from_the_resolved_integration_and_the_envelope() -> None:
    message = envelope(
        replay=True,
        replay_of="ing_" + "5" * 32,
        connector_version="gw/2",
        mapping_versions=NormalizationVersions(quality_policy_version="quality-policy@0"),
    )

    resolved = device_integration(
        tenant_id=OTHER_TENANT,
        device_id=OTHER_DEVICE,
        device_integration_id=OTHER_INTEGRATION,
        provider_key="https_other",
    )

    context = build_trusted_context(message, resolved)

    assert context == TrustedSourceContext(
        tenant_id=OTHER_TENANT,
        device_id=OTHER_DEVICE,
        integration_id=OTHER_INTEGRATION,
        integration_type="https_other",
        source_type=SourceType.HTTPS,
        connector_version="gw/2",
        received_at=RECEIVED,
        replay=True,
        mapping_versions=NormalizationVersions(quality_policy_version="quality-policy@0"),
    )


# Fixed error bodies ----------------------------------------------------------------------------

SENTINEL = "SENTINELq7"


def _validation_error(body: str) -> ValidationError:
    with pytest.raises(ValidationError) as exc_info:
        IngestObservationBatch.model_validate_json(body)
    return exc_info.value


def test_validation_detail_never_echoes_sentinels() -> None:
    body = json.dumps(
        _batch(
            [
                _observation(source_metadata={f"{SENTINEL}_key": "v"}),
                _observation(component_id=f"cmp_{SENTINEL}"),
                _observation(source_unit=f"{SENTINEL}" + "u" * 12),
            ]
        )
    )

    detail = redacted_validation_detail(_validation_error(body))

    rendered = json.dumps(detail)
    assert SENTINEL not in rendered
    assert len(detail) == 3
    assert all(set(item) == {"loc", "msg", "type"} for item in detail)
    assert [item["loc"] for item in detail] == [
        ["body", "observations", 0, "source_metadata"],
        ["body", "observations", 1, "component_id"],
        ["body", "observations", 2, "source_unit"],
    ]
    assert {item["msg"] for item in detail} == {"invalid value"}


def test_validation_detail_uses_fixed_messages_per_type() -> None:
    body = json.dumps({"observations": [{**_observation(), "tenant_id": SENTINEL}], SENTINEL: 1})

    detail = redacted_validation_detail(_validation_error(body))

    by_type = {item["type"]: item["msg"] for item in detail}
    assert by_type["missing"] == "field required"
    assert by_type["extra_forbidden"] == "extra fields not permitted"
    assert all(
        item["msg"] in {"field required", "extra fields not permitted", "invalid value"}
        for item in detail
    )
    assert all(item["loc"][0] == "body" for item in detail)


def test_validation_detail_is_capped_at_twenty_entries() -> None:
    body = json.dumps(_batch([_observation(value="bad")] * 25))

    detail = redacted_validation_detail(_validation_error(body))

    assert len(detail) == ingestion.MAX_VALIDATION_ERRORS == 20


@pytest.mark.parametrize("literal", ["NaN", "Infinity", "-Infinity", "1e999"])
def test_validation_detail_handles_non_finite_numbers(literal: str) -> None:
    body = (
        '{"external_device_id":"sensor-01","observations":[{"metric_key":"t",'
        f'"value":{literal},"source_unit":"C","observed_at":"2026-10-30T11:59:00Z"}}]}}'
    )

    detail = redacted_validation_detail(_validation_error(body))

    assert json.loads(json.dumps(detail, allow_nan=False)) == detail
    assert all(item["msg"] == "invalid value" for item in detail)


def test_validation_detail_handles_malformed_json() -> None:
    detail = redacted_validation_detail(_validation_error('{"external_device_id": "x", '))

    assert detail and all(item["msg"] == "invalid value" for item in detail)
    assert all(item["loc"][0] == "body" for item in detail)


@pytest.mark.parametrize(
    ("reason", "message", "error_type"),
    [
        (
            IngressAdmissionDenied.COMPONENT_REQUIRED,
            "component_id is required for this device",
            "missing",
        ),
        (
            IngressAdmissionDenied.COMPONENT_INVALID,
            "component_id is not valid for this device",
            "value_error",
        ),
    ],
)
def test_component_denials_have_fixed_bodies(
    reason: IngressAdmissionDenied, message: str, error_type: str
) -> None:
    detail = component_denial_detail(IngressDenial(reason=reason, observation_index=3))

    assert detail == [
        {"loc": ["body", "observations", 3, "component_id"], "msg": message, "type": error_type}
    ]


def test_component_denial_detail_refuses_other_denials() -> None:
    with pytest.raises(ValueError):
        component_denial_detail(
            IngressDenial(reason=IngressAdmissionDenied.DEVICE_INACTIVE, observation_index=None)
        )
    with pytest.raises(ValueError):
        component_denial_detail(
            IngressDenial(reason=IngressAdmissionDenied.COMPONENT_INVALID, observation_index=None)
        )


def test_fixed_error_details() -> None:
    assert IngestErrorDetail.UNAUTHORIZED == "unauthorized"
    assert IngestErrorDetail.RATE_LIMITED == "rate limited"
    assert IngestErrorDetail.UNSUPPORTED_MEDIA_TYPE == "unsupported media type"
    assert IngestErrorDetail.TOO_LARGE == "request body too large"
    assert IngestErrorDetail.IDEMPOTENCY_CONFLICT == (
        "idempotency key reused with a different request"
    )
    assert IngestErrorDetail.FORBIDDEN == "forbidden"
    assert IngestErrorDetail.UNAVAILABLE == "service unavailable"


# Limiter shape, failure table and metrics -----------------------------------------------------


@pytest.mark.parametrize(
    ("per_minute", "capacity"), [(1, 1), (9, 1), (10, 1), (19, 1), (20, 2), (60, 6), (600, 60)]
)
def test_preauth_bucket_capacity(per_minute: int, capacity: int) -> None:
    assert preauth_bucket_capacity(per_minute) == capacity


def test_bucket_refill_is_per_minute_over_sixty() -> None:
    assert bucket_refill_per_second(120) == 2.0
    assert bucket_refill_per_second(60) == 1.0
    assert bucket_refill_per_second(1) == pytest.approx(1 / 60)


def test_failure_table_is_frozen() -> None:
    category = IngressFailureCategory
    outcome = IngressMessageOutcome
    action = IngressMessageAction

    assert dict(INGRESS_FAILURE_TABLE) == {
        category.ENVELOPE_INVALID: (outcome.PERMANENT, action.PERMANENT),
        category.FENCED: (outcome.FENCED, action.DELETE),
        category.FENCED_UNVERIFIED: (outcome.PERMANENT, action.PERMANENT),
        category.DEFERRED: (outcome.RETRY, action.BACKOFF),
        category.UNEXPECTED: (outcome.RETRY, action.BACKOFF),
        category.STORE_UNAVAILABLE: (outcome.RETRY, action.BACKOFF),
        category.WRITE_RETRYABLE: (outcome.RETRY, action.BACKOFF),
        category.WRITE_PERMANENT: (outcome.PERMANENT, action.PERMANENT),
        category.CONTRACT_VIOLATION: (outcome.PERMANENT, action.PERMANENT),
        category.PROCESSED: (outcome.PROCESSED, action.DELETE),
    }


@pytest.mark.parametrize(
    ("receive_count", "draw", "base", "cap", "expected"),
    [
        (1, 1.0, 30, 900, 30),
        (2, 1.0, 30, 900, 60),
        (5, 1.0, 30, 900, 480),
        (6, 1.0, 30, 900, 900),
        (500, 1.0, 30, 900, 900),
        (1, 0.5, 30, 900, 15),
        (1, 0.01, 30, 900, 1),
        (3, 0.0, 30, 900, 1),
        (2, 0.333, 30, 900, 20),
        (10, 1.0, 900, 43_200, 43_200),
        (10, 1.0, 900, 50_000, 43_200),
    ],
)
def test_backoff_is_full_jitter_and_clamped(
    receive_count: int, draw: float, base: int, cap: int, expected: int
) -> None:
    assert (
        ingress_backoff_seconds(receive_count, base=base, cap=cap, random=lambda: draw) == expected
    )


def test_backoff_defaults_are_thirty_and_nine_hundred() -> None:
    assert ingress_backoff_seconds(1, random=lambda: 1.0) == 30
    assert ingress_backoff_seconds(9, random=lambda: 1.0) == 900


EXPECTED_METRICS = {
    "ingest_requests_total": {
        "outcome": {
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
    },
    "ingest_admission_denied_total": {"reason": {item.value for item in IngressAdmissionDenied}},
    "ingest_observations_admitted_total": {},
    "ingress_messages_total": {
        "outcome": {"processed", "fenced", "retry", "permanent"},
        "category": {item.value for item in IngressFailureCategory},
    },
    "ingress_observations_total": {"result": {"accepted", "quarantined"}},
    "ingress_quarantined_total": {"reason": {item.value for item in QuarantineReason}},
    "ingress_fenced_total": {"reason": {item.value for item in IngressFenceReason}},
    "ingress_deferred_total": {"reason": {item.value for item in IngressDeferReason}},
    "ingress_quality_flags_total": {"flag": {item.value for item in QualityFlag}},
    "ingress_sequence_events_total": {"event": {item.value for item in SequenceEvent}},
    "ingress_write_failures_total": {"retryable": {"true", "false"}},
    "ingress_normalization_duration_seconds": {},
    "ingress_queue_age_seconds": {},
    "ingress_dlq_redriven_total": {"result": {"redriven", "failed"}},
    "ingress_quarantine_replayed_total": {"result": {"replayed", "skipped", "failed"}},
}


def test_metric_names_and_label_values_are_closed() -> None:
    assert {
        name: {label: set(values) for label, values in labels.items()}
        for name, labels in INGEST_METRICS.items()
    } == EXPECTED_METRICS


def test_require_metric_labels_accepts_only_closed_values() -> None:
    require_metric_labels("ingest_requests_total", {"outcome": "accepted"})
    require_metric_labels("ingress_queue_age_seconds", {})
    require_metric_labels(
        "ingress_messages_total", {"outcome": "retry", "category": "store_unavailable"}
    )

    for name, labels in [
        ("ingest_unknown_total", {}),
        ("ingest_requests_total", {}),
        ("ingest_requests_total", {"outcome": "tenant-a"}),
        ("ingest_requests_total", {"outcome": "accepted", "tenant_id": TENANT}),
        ("ingress_queue_age_seconds", {"device_id": DEVICE}),
        ("ingress_messages_total", {"outcome": "retry"}),
    ]:
        with pytest.raises(ValueError):
            require_metric_labels(name, labels)


# PipelineOutcome.accepted ----------------------------------------------------------------------


def test_pipeline_outcome_accepted_is_additive_and_last() -> None:
    fields = dataclasses.fields(PipelineOutcome)

    assert [item.name for item in fields] == ["written", "quarantined", "counts", "accepted"]
    legacy = PipelineOutcome(written=None, quarantined=(), counts=MappingProxyType({}))  # type: ignore[arg-type]
    assert legacy.accepted == ()


@pytest.mark.asyncio
async def test_pipeline_reports_the_accepted_observations() -> None:
    accepted = (canonical_observation(sequence=1), canonical_observation(metric_key="ph"))

    class _Normalizer:
        async def normalize(self, context: TrustedSourceContext, observations: Any) -> Any:
            from limnopulse_api.domain.telemetry_v2 import NormalizationResult

            return NormalizationResult(accepted=accepted, quarantined=())

    class _Writer:
        def __init__(self) -> None:
            self.written: tuple[Any, ...] = ()

        async def write(self, observations: Any) -> str:
            self.written = tuple(observations)
            return "written"

    writer = _Writer()
    pipeline = TelemetryPipeline(normalizer=_Normalizer(), write_service=writer)  # type: ignore[arg-type]

    outcome = await pipeline.ingest(
        build_trusted_context(envelope(), device_integration()), [source_observation()]
    )

    assert outcome.accepted == accepted == writer.written
    assert outcome.written == "written"
    assert outcome.counts["accepted"] == 2
