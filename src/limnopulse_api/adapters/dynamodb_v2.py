"""Shared DynamoDB vocabulary for the `/v2` bounded-context adapters.

Only generic primitives live here: key builders, hashing helpers, idempotency receipt codecs
and a repository base with serialized reads, paginated queries, conditional write builders and
the conditional-failure -> `ConflictError` mapping. M1 entity codecs belong to each adapter; the
P3 ingestion codecs (`ingest_credential_*`, `ingress_receipt_*`, `quarantine_entry_*` and the
ingest-state `*_to_item`/`*_from_item`) are frozen here by P3-00, and the P3 adapters reuse them.
"""

import json
import math
import re
from asyncio import to_thread
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from typing import Any

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError

from limnopulse_api.core.errors import ConflictError
from limnopulse_api.domain.ingestion import (
    QUARANTINE_TTL,
    ComponentIngestState,
    ConnectorHealth,
    DeviceIngestState,
    IngestCredential,
    IngressReceipt,
    QuarantineEntry,
)
from limnopulse_api.domain.telemetry_v2 import NormalizationVersions, QuarantinedObservation
from limnopulse_api.domain.v2_common import (
    IDEMPOTENCY_KEY_MAX_LENGTH,
    IDEMPOTENCY_KEY_MIN_LENGTH,
    IDEMPOTENCY_RECEIPT_TTL,
    PROVIDER_KEY_PATTERN,
    IdempotencyReceipt,
    format_utc_timestamp,
)

MAX_TRANSACTION_ACTIONS = 100
RECEIPT_ENTITY_TYPE = "idempotency_receipt"


def utc_now() -> datetime:
    return datetime.now(UTC)


def canonical_timestamp_key(value: datetime) -> str:
    """UTC `YYYY-MM-DDTHH:MM:SS.ffffffZ`; lexicographic order is chronological order."""
    return format_utc_timestamp(value)


def parse_canonical_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed.astimezone(UTC)


def external_identity_hash(provider_key: str, external_identity: str) -> str:
    if re.fullmatch(PROVIDER_KEY_PATTERN, provider_key) is None:
        raise ValueError("invalid provider key")
    if not external_identity:
        raise ValueError("external identity must not be empty")
    return sha256(f"{provider_key}\0{external_identity}".encode()).hexdigest()


def account_scoped_identity_hash(
    provider_key: str, integration_account_id: str, external_device_id: str
) -> str:
    """D4: HTTPS identities are scoped to their IntegrationAccount. `\x1f` cannot occur in an
    external identity (`EXTERNAL_IDENTITY_PATTERN`), so the joined value is unambiguous."""
    if not integration_account_id or "\x1f" in integration_account_id:
        raise ValueError("invalid integration account id")
    if not external_device_id:
        raise ValueError("external identity must not be empty")
    return external_identity_hash(provider_key, f"{integration_account_id}\x1f{external_device_id}")


def idempotency_key_hash(idempotency_key: str) -> str:
    if not IDEMPOTENCY_KEY_MIN_LENGTH <= len(idempotency_key) <= IDEMPOTENCY_KEY_MAX_LENGTH:
        raise ValueError("Idempotency-Key must have 8 to 128 characters")
    return sha256(idempotency_key.encode()).hexdigest()


def canonical_request_hash(payload: Any) -> str:
    """Hash only the client request (scope, path params, body); never server-generated values."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return sha256(canonical.encode()).hexdigest()


def _part(value: str) -> str:
    if not isinstance(value, str) or not value or "#" in value:
        raise ValueError("key part must be a non-empty string without '#'")
    return value


class DynamoV2KeyBuilder:
    SITE_PREFIX = "SITE#"
    ASSET_PREFIX = "ASSET#"
    DEVICE_PREFIX = "DEVICE#"
    COMPONENT_PREFIX = "COMPONENT#"
    CAPABILITY_PREFIX = "CAPABILITY#"
    INTEGRATION_ACCOUNT_PREFIX = "INTEGRATION_ACCOUNT#"
    DEVICE_INTEGRATION_PREFIX = "DEVICE_INTEGRATION#"
    DEPLOYMENT_PREFIX = "DEPLOYMENT#"
    LEGACY_POND_PREFIX = "POND#"

    def tenant_partition(self, tenant_id: str) -> str:
        return f"TENANT#{_part(tenant_id)}"

    def site(self, tenant_id: str, site_id: str) -> dict[str, str]:
        return {"PK": self.tenant_partition(tenant_id), "SK": f"SITE#{_part(site_id)}"}

    def asset(self, tenant_id: str, asset_id: str) -> dict[str, str]:
        return {"PK": self.tenant_partition(tenant_id), "SK": f"ASSET#{_part(asset_id)}"}

    def pond_profile(self, asset_id: str) -> dict[str, str]:
        return {"PK": f"ASSET#{_part(asset_id)}", "SK": "PROFILE#POND"}

    def device(self, tenant_id: str, device_id: str) -> dict[str, str]:
        return {"PK": self.tenant_partition(tenant_id), "SK": f"DEVICE#{_part(device_id)}"}

    def device_lookup(self, device_id: str) -> dict[str, str]:
        return {"PK": f"DEVICE#{_part(device_id)}", "SK": "META"}

    def component_mirror(self, tenant_id: str, component_id: str) -> dict[str, str]:
        return {"PK": self.tenant_partition(tenant_id), "SK": f"COMPONENT#{_part(component_id)}"}

    def component(self, component_id: str) -> dict[str, str]:
        return {"PK": f"COMPONENT#{_part(component_id)}", "SK": "META"}

    def capability(self, component_id: str, capability_id: str) -> dict[str, str]:
        return {
            "PK": f"COMPONENT#{_part(component_id)}",
            "SK": f"CAPABILITY#{_part(capability_id)}",
        }

    def integration_account(self, tenant_id: str, integration_account_id: str) -> dict[str, str]:
        return {
            "PK": self.tenant_partition(tenant_id),
            "SK": f"INTEGRATION_ACCOUNT#{_part(integration_account_id)}",
        }

    def device_integration(self, tenant_id: str, device_integration_id: str) -> dict[str, str]:
        return {
            "PK": self.tenant_partition(tenant_id),
            "SK": f"DEVICE_INTEGRATION#{_part(device_integration_id)}",
        }

    def external_identity(self, provider_key: str, identity_hash: str) -> dict[str, str]:
        return {"PK": f"EXTERNAL_ID#{_part(provider_key)}#{_part(identity_hash)}", "SK": "META"}

    def deployment_mirror(self, tenant_id: str, deployment_id: str) -> dict[str, str]:
        return {
            "PK": self.tenant_partition(tenant_id),
            "SK": f"DEPLOYMENT#{_part(deployment_id)}",
        }

    def deployment_history(
        self, component_id: str, started_at: datetime, deployment_id: str
    ) -> dict[str, str]:
        return {
            "PK": f"COMPONENT#{_part(component_id)}",
            "SK": f"DEPLOYMENT#{canonical_timestamp_key(started_at)}#{_part(deployment_id)}",
        }

    def current_deployment(self, component_id: str) -> dict[str, str]:
        return {"PK": f"COMPONENT#{_part(component_id)}", "SK": "DEPLOYMENT#CURRENT"}

    def deployment_history_bounds(self) -> tuple[str, str]:
        """Inclusive SK bounds of every history row; `DEPLOYMENT#CURRENT` sorts after them."""
        return "DEPLOYMENT#0", "DEPLOYMENT#:"

    def deployment_history_upper_bound(self, observed_at: datetime) -> str:
        """Inclusive SK bound covering every history row that started at or before the instant.

        Point-in-time queries must also pass `lower=deployment_history_bounds()[0]`, because
        `COMPONENT#` partitions also hold `CAPABILITY#` rows that sort below this bound.
        """
        return f"DEPLOYMENT#{canonical_timestamp_key(observed_at)}#~"

    def idempotency_receipt(self, tenant_id: str, scope: str, key_hash: str) -> dict[str, str]:
        return {
            "PK": self.tenant_partition(tenant_id),
            "SK": f"IDEMPOTENCY#{_part(scope)}#{_part(key_hash)}",
        }

    def migration_receipt(self, tenant_id: str) -> dict[str, str]:
        return {
            "PK": self.tenant_partition(tenant_id),
            "SK": "MIGRATION#M1#DEFAULT_SITE_ASSET_DEVICE",
        }

    def legacy_pond(self, tenant_id: str, pond_id: str) -> dict[str, str]:
        return {"PK": self.tenant_partition(tenant_id), "SK": f"POND#{_part(pond_id)}"}

    def legacy_device(self, tenant_id: str, device_id: str) -> dict[str, str]:
        return {"PK": self.tenant_partition(tenant_id), "SK": f"DEVICE#{_part(device_id)}"}

    # P3 ingestion rows -------------------------------------------------------------------------

    def ingest_credential(self, ingest_credential_id: str) -> dict[str, str]:
        return {"PK": f"INGEST_CREDENTIAL#{_part(ingest_credential_id)}", "SK": "META"}

    def ingest_credential_listing(
        self, tenant_id: str, integration_account_id: str, ingest_credential_id: str
    ) -> dict[str, str]:
        return {
            "PK": self.tenant_partition(tenant_id),
            "SK": f"{self.ingest_credential_listing_prefix(integration_account_id)}"
            f"{_part(ingest_credential_id)}",
        }

    def ingest_credential_listing_prefix(self, integration_account_id: str) -> str:
        return f"INGEST_CREDENTIAL#{_part(integration_account_id)}#"

    def ingest_credential_slot(
        self, tenant_id: str, integration_account_id: str, slot: int
    ) -> dict[str, str]:
        if type(slot) is not int or slot not in (1, 2):
            raise ValueError("credential slot must be 1 or 2")
        return {
            "PK": self.tenant_partition(tenant_id),
            "SK": f"INGEST_CREDENTIAL_SLOT#{_part(integration_account_id)}#{slot}",
        }

    def ingress_receipt(self, integration_account_id: str, key_sha256: str) -> dict[str, str]:
        return {"PK": f"INGRESS_RECEIPT#{_part(integration_account_id)}", "SK": _part(key_sha256)}

    def quarantine_partition(self, tenant_id: str) -> str:
        return f"QUARANTINE#{_part(tenant_id)}"

    def quarantine_entry(
        self, tenant_id: str, received_at: datetime, quarantine_id: str
    ) -> dict[str, str]:
        return {
            "PK": self.quarantine_partition(tenant_id),
            "SK": f"{canonical_timestamp_key(received_at)}#{_part(quarantine_id)}",
        }

    def component_ingest_state(self, component_id: str) -> dict[str, str]:
        return {"PK": f"COMPONENT#{_part(component_id)}", "SK": "INGEST_STATE#CURRENT"}

    def device_ingest_state(self, device_id: str) -> dict[str, str]:
        return {"PK": f"DEVICE#{_part(device_id)}", "SK": "INGEST_STATE#CURRENT"}

    def connector_health(self, integration_account_id: str) -> dict[str, str]:
        return {
            "PK": f"INTEGRATION_ACCOUNT#{_part(integration_account_id)}",
            "SK": "CONNECTOR_HEALTH#CURRENT",
        }


_KEYS = DynamoV2KeyBuilder()


class DynamoConditionalConflict(ConflictError):
    """A conditional write failed; `failed_indexes` names the failed transaction actions."""

    def __init__(self, failed_indexes: tuple[int, ...]) -> None:
        super().__init__("conditional write conflict")
        self.failed_indexes = failed_indexes


def conditional_failure_indexes(exc: BaseException) -> tuple[int, ...]:
    """Indexes of conditional failures; `(0,)` for a single-item write, `()` for anything else.

    A cancelled transaction counts as a conflict whenever ANY reason is `ConditionalCheckFailed`,
    even when other reasons (e.g. `TransactionConflict`) are present.
    """
    if isinstance(exc, DynamoConditionalConflict):
        return exc.failed_indexes
    response = getattr(exc, "response", None)
    if not isinstance(response, Mapping):
        return ()
    error = response.get("Error")
    code = error.get("Code") if isinstance(error, Mapping) else None
    if code == "ConditionalCheckFailedException":
        return (0,)
    if code != "TransactionCanceledException":
        return ()
    reasons = response.get("CancellationReasons")
    if not isinstance(reasons, list):
        return ()
    return tuple(
        index
        for index, reason in enumerate(reasons)
        if isinstance(reason, Mapping) and reason.get("Code") == "ConditionalCheckFailed"
    )


def idempotency_receipt_to_item(receipt: IdempotencyReceipt) -> dict[str, Any]:
    return {
        **_KEYS.idempotency_receipt(receipt.tenant_id, receipt.scope, receipt.key_hash),
        "entity_type": RECEIPT_ENTITY_TYPE,
        "tenant_id": receipt.tenant_id,
        "scope": receipt.scope,
        "key_hash": receipt.key_hash,
        "request_hash": receipt.request_hash,
        "response_snapshot_json": receipt.response_snapshot_json,
        "created_at": canonical_timestamp_key(receipt.created_at),
        # Numeric TTL attribute; only receipts (this one and the P3 ingress receipt) and P3
        # quarantine rows may carry it. Rounded up so TTL deletion and the overwrite condition
        # never act before the exact 24-hour deadline.
        "expires_at": math.ceil(receipt.expires_at.timestamp()),
    }


def idempotency_receipt_from_item(item: Mapping[str, Any]) -> IdempotencyReceipt:
    created_at = parse_canonical_timestamp(item["created_at"])
    return IdempotencyReceipt(
        tenant_id=item["tenant_id"],
        scope=item["scope"],
        key_hash=item["key_hash"],
        request_hash=item["request_hash"],
        response_snapshot_json=item["response_snapshot_json"],
        created_at=created_at,
        expires_at=created_at + IDEMPOTENCY_RECEIPT_TTL,
    )


# P3 ingestion codecs (frozen by P3-00) ----------------------------------------------------------


def _timestamp(value: datetime | None) -> str | None:
    return None if value is None else canonical_timestamp_key(value)


def _optional_timestamp(item: Mapping[str, Any], name: str) -> datetime | None:
    value = item.get(name)
    return None if value is None else parse_canonical_timestamp(value)


def _without_none(item: dict[str, Any]) -> dict[str, Any]:
    """Absent attributes, never NULL: monotone conditions test `attribute_not_exists`."""
    return {key: value for key, value in item.items() if value is not None}


def _versions(item: Mapping[str, Any]) -> NormalizationVersions:
    return NormalizationVersions.model_validate(item["mapping_versions"])


def ingest_credential_to_item(credential: IngestCredential) -> dict[str, Any]:
    return _without_none(
        {
            **_KEYS.ingest_credential(credential.ingest_credential_id),
            "entity_type": "ingest_credential",
            "tenant_id": credential.tenant_id,
            "ingest_credential_id": credential.ingest_credential_id,
            "integration_account_id": credential.integration_account_id,
            "slot": credential.slot,
            "secret_sha256": credential.secret_sha256,
            "fingerprint": credential.fingerprint,
            "status": credential.status.value,
            "revoked_at": _timestamp(credential.revoked_at),
            "created_at": canonical_timestamp_key(credential.created_at),
            "updated_at": canonical_timestamp_key(credential.updated_at),
            "version": credential.version,
            "schema_version": credential.schema_version,
        }
    )


def ingest_credential_from_item(item: Mapping[str, Any]) -> IngestCredential:
    return IngestCredential(
        tenant_id=item["tenant_id"],
        ingest_credential_id=item["ingest_credential_id"],
        integration_account_id=item["integration_account_id"],
        slot=item["slot"],
        secret_sha256=item["secret_sha256"],
        fingerprint=item["fingerprint"],
        status=item["status"],
        revoked_at=_optional_timestamp(item, "revoked_at"),
        created_at=parse_canonical_timestamp(item["created_at"]),
        updated_at=parse_canonical_timestamp(item["updated_at"]),
        version=item["version"],
    )


def ingress_receipt_to_item(receipt: IngressReceipt) -> dict[str, Any]:
    return {
        **_KEYS.ingress_receipt(receipt.integration_account_id, receipt.key_sha256),
        "entity_type": "ingress_receipt",
        "integration_account_id": receipt.integration_account_id,
        "tenant_id": receipt.tenant_id,
        "key_sha256": receipt.key_sha256,
        "request_sha256": receipt.request_sha256,
        "ingress_id": receipt.ingress_id,
        "received_at": canonical_timestamp_key(receipt.received_at),
        "mapping_versions": receipt.mapping_versions.model_dump(mode="json"),
        "state": receipt.state.value,
        "created_at": canonical_timestamp_key(receipt.created_at),
        "created_at_us": receipt.created_at_us,
        "expires_at": receipt.expires_at,  # TTL only; liveness is decided on created_at_us
    }


def ingress_receipt_from_item(item: Mapping[str, Any]) -> IngressReceipt:
    return IngressReceipt(
        integration_account_id=item["integration_account_id"],
        tenant_id=item["tenant_id"],
        key_sha256=item["key_sha256"],
        request_sha256=item["request_sha256"],
        ingress_id=item["ingress_id"],
        received_at=parse_canonical_timestamp(item["received_at"]),
        mapping_versions=_versions(item),
        state=item["state"],
        created_at=parse_canonical_timestamp(item["created_at"]),
        created_at_us=item["created_at_us"],
        expires_at=item["expires_at"],
    )


def quarantine_entry_to_item(entry: QuarantineEntry) -> dict[str, Any]:
    return _without_none(
        {
            **_KEYS.quarantine_entry(entry.tenant_id, entry.received_at, entry.quarantine_id),
            "entity_type": "telemetry_quarantine",
            "tenant_id": entry.tenant_id,
            "quarantine_id": entry.quarantine_id,
            "kind": entry.kind.value,
            "reason": entry.reason,
            "ingress_id": entry.ingress_id,
            "replay_of": entry.replay_of,
            "integration_account_id": entry.integration_account_id,
            "provider_key": entry.provider_key,
            "external_identity_hash": entry.external_identity_hash,
            "source_type": entry.source_type.value,
            "connector_version": entry.connector_version,
            "device_integration_id": entry.device_integration_id,
            "device_id": entry.device_id,
            "received_at": canonical_timestamp_key(entry.received_at),
            "mapping_versions": entry.mapping_versions.model_dump(mode="json"),
            "observation_json": (
                None if entry.observation is None else entry.observation.model_dump_json()
            ),
            "source_observation_json": entry.source_observation_json,
            "envelope_excerpt_json": entry.envelope_excerpt_json,
            "truncated": entry.truncated,
            "replayable": entry.replayable,
            "status": entry.status.value,
            "replayed_ingress_id": entry.replayed_ingress_id,
            "created_at": canonical_timestamp_key(entry.created_at),
            "updated_at": canonical_timestamp_key(entry.updated_at),
            "version": entry.version,
            "schema_version": entry.schema_version,
            "expires_at": math.ceil(entry.expires_at.timestamp()),
        }
    )


def quarantine_entry_from_item(item: Mapping[str, Any]) -> QuarantineEntry:
    created_at = parse_canonical_timestamp(item["created_at"])
    observation = item.get("observation_json")
    return QuarantineEntry(
        tenant_id=item["tenant_id"],
        quarantine_id=item["quarantine_id"],
        kind=item["kind"],
        reason=item["reason"],
        ingress_id=item["ingress_id"],
        replay_of=item.get("replay_of"),
        integration_account_id=item["integration_account_id"],
        provider_key=item["provider_key"],
        external_identity_hash=item["external_identity_hash"],
        source_type=item["source_type"],
        connector_version=item.get("connector_version"),
        device_integration_id=item["device_integration_id"],
        device_id=item["device_id"],
        received_at=parse_canonical_timestamp(item["received_at"]),
        mapping_versions=_versions(item),
        observation=(
            None if observation is None else QuarantinedObservation.model_validate_json(observation)
        ),
        source_observation_json=item.get("source_observation_json"),
        envelope_excerpt_json=item.get("envelope_excerpt_json"),
        truncated=item["truncated"],
        replayable=item["replayable"],
        status=item["status"],
        replayed_ingress_id=item.get("replayed_ingress_id"),
        created_at=created_at,
        updated_at=parse_canonical_timestamp(item["updated_at"]),
        version=item["version"],
        # The exact deadline; the epoch TTL attribute is rounded up.
        expires_at=created_at + QUARANTINE_TTL,
    )


def component_ingest_state_to_item(state: ComponentIngestState) -> dict[str, Any]:
    return _without_none(
        {
            **_KEYS.component_ingest_state(state.component_id),
            "entity_type": "component_ingest_state",
            "tenant_id": state.tenant_id,
            "component_id": state.component_id,
            "device_id": state.device_id,
            "last_received_at": _timestamp(state.last_received_at),
            "last_ingested_at": _timestamp(state.last_ingested_at),
            "last_observed_at": _timestamp(state.last_observed_at),
            "last_observed_sequence": state.last_observed_sequence,
        }
    )


def component_ingest_state_from_item(item: Mapping[str, Any]) -> ComponentIngestState:
    return ComponentIngestState(
        tenant_id=item["tenant_id"],
        component_id=item["component_id"],
        device_id=item["device_id"],
        last_received_at=_optional_timestamp(item, "last_received_at"),
        last_ingested_at=_optional_timestamp(item, "last_ingested_at"),
        last_observed_at=_optional_timestamp(item, "last_observed_at"),
        last_observed_sequence=item.get("last_observed_sequence"),
    )


def device_ingest_state_to_item(state: DeviceIngestState) -> dict[str, Any]:
    return _without_none(
        {
            **_KEYS.device_ingest_state(state.device_id),
            "entity_type": "device_ingest_state",
            "tenant_id": state.tenant_id,
            "device_id": state.device_id,
            "last_received_at": _timestamp(state.last_received_at),
            "last_ingested_at": _timestamp(state.last_ingested_at),
        }
    )


def device_ingest_state_from_item(item: Mapping[str, Any]) -> DeviceIngestState:
    return DeviceIngestState(
        tenant_id=item["tenant_id"],
        device_id=item["device_id"],
        last_received_at=_optional_timestamp(item, "last_received_at"),
        last_ingested_at=_optional_timestamp(item, "last_ingested_at"),
    )


def connector_health_to_item(health: ConnectorHealth) -> dict[str, Any]:
    return _without_none(
        {
            **_KEYS.connector_health(health.integration_account_id),
            "entity_type": "connector_health",
            "tenant_id": health.tenant_id,
            "integration_account_id": health.integration_account_id,
            "last_received_at": _timestamp(health.last_received_at),
            "last_accepted_at": _timestamp(health.last_accepted_at),
            "last_quarantined_at": _timestamp(health.last_quarantined_at),
            "last_quarantine_reason": health.last_quarantine_reason,
            "last_fenced_at": _timestamp(health.last_fenced_at),
            "last_fence_reason": health.last_fence_reason,
        }
    )


def connector_health_from_item(item: Mapping[str, Any]) -> ConnectorHealth:
    return ConnectorHealth(
        tenant_id=item["tenant_id"],
        integration_account_id=item["integration_account_id"],
        last_received_at=_optional_timestamp(item, "last_received_at"),
        last_accepted_at=_optional_timestamp(item, "last_accepted_at"),
        last_quarantined_at=_optional_timestamp(item, "last_quarantined_at"),
        last_quarantine_reason=item.get("last_quarantine_reason"),
        last_fenced_at=_optional_timestamp(item, "last_fenced_at"),
        last_fence_reason=item.get("last_fence_reason"),
    )


class DynamoV2RepositoryBase:
    def __init__(
        self,
        table_name: str,
        client: Any,
        *,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.table_name = table_name
        self.client = client
        self.clock = clock
        self.keys = DynamoV2KeyBuilder()
        self._serializer = TypeSerializer()
        self._deserializer = TypeDeserializer()

    # Codec ---------------------------------------------------------------------------------

    def serialize_item(self, item: Mapping[str, Any]) -> dict[str, Any]:
        return {
            key: self._serializer.serialize(self._normalize_to_dynamo(value))
            for key, value in item.items()
        }

    def serialize_values(self, values: Mapping[str, Any]) -> dict[str, Any]:
        return self.serialize_item(values)

    def deserialize_item(self, item: Mapping[str, Any]) -> dict[str, Any]:
        return {
            key: self._normalize_from_dynamo(self._deserializer.deserialize(value))
            for key, value in item.items()
        }

    # Request builders ----------------------------------------------------------------------

    def put_request(
        self,
        item: Mapping[str, Any],
        *,
        condition: str | None = None,
        names: Mapping[str, str] | None = None,
        values: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        request: dict[str, Any] = {
            "TableName": self.table_name,
            "Item": self.serialize_item(item),
        }
        return self._with_condition(request, condition, names, values)

    def put_new_request(self, item: Mapping[str, Any]) -> dict[str, Any]:
        return self.put_request(item, condition="attribute_not_exists(PK)")

    def put_versioned_request(
        self, item: Mapping[str, Any], expected_version: int
    ) -> dict[str, Any]:
        return self.put_request(
            item,
            condition="attribute_exists(PK) AND #version = :expected_version",
            names={"#version": "version"},
            values={":expected_version": expected_version},
        )

    def update_request(
        self,
        key: Mapping[str, str],
        *,
        update_expression: str,
        condition: str | None = None,
        names: Mapping[str, str] | None = None,
        values: Mapping[str, Any] | None = None,
        return_values: str | None = None,
    ) -> dict[str, Any]:
        """`return_values` is for standalone `update_item` only; transactions reject it."""
        request: dict[str, Any] = {
            "TableName": self.table_name,
            "Key": self.serialize_item(key),
            "UpdateExpression": update_expression,
        }
        if return_values is not None:
            request["ReturnValues"] = return_values
        return self._with_condition(request, condition, names, values)

    def condition_check_request(
        self,
        key: Mapping[str, str],
        *,
        condition: str,
        names: Mapping[str, str] | None = None,
        values: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        request: dict[str, Any] = {"TableName": self.table_name, "Key": self.serialize_item(key)}
        return self._with_condition(request, condition, names, values)

    def version_check_request(
        self, key: Mapping[str, str], expected_version: int
    ) -> dict[str, Any]:
        return self.condition_check_request(
            key,
            condition="attribute_exists(PK) AND #version = :expected_version",
            names={"#version": "version"},
            values={":expected_version": expected_version},
        )

    def receipt_put_request(self, receipt: IdempotencyReceipt) -> dict[str, Any]:
        """Receipt Put that succeeds only when no live receipt exists for the same key."""
        return self.put_request(
            idempotency_receipt_to_item(receipt),
            condition="attribute_not_exists(PK) OR #expires_at <= :now",
            names={"#expires_at": "expires_at"},
            values={":now": self._now_epoch()},
        )

    # Sync primitives -----------------------------------------------------------------------

    def get_item_sync(
        self, key: Mapping[str, str], *, consistent: bool = False
    ) -> dict[str, Any] | None:
        response = self.client.get_item(
            TableName=self.table_name,
            Key=self.serialize_item(key),
            ConsistentRead=consistent,
        )
        item = response.get("Item")
        if item is None:
            return None
        return self.deserialize_item(item)

    def query_prefix_sync(
        self, pk: str, sk_prefix: str, *, consistent: bool = False
    ) -> list[dict[str, Any]]:
        return self._query(
            "PK = :pk AND begins_with(SK, :sk_prefix)",
            {":pk": pk, ":sk_prefix": sk_prefix},
            consistent=consistent,
        )

    def query_range_sync(
        self,
        pk: str,
        *,
        lower: str | None = None,
        upper: str | None = None,
        ascending: bool = True,
        limit: int | None = None,
        consistent: bool = False,
    ) -> list[dict[str, Any]]:
        """Inclusive SK range query; at least one bound is required."""
        values: dict[str, Any] = {":pk": pk}
        if lower is not None and upper is not None:
            condition = "PK = :pk AND SK BETWEEN :sk_lower AND :sk_upper"
            values.update({":sk_lower": lower, ":sk_upper": upper})
        elif upper is not None:
            condition = "PK = :pk AND SK <= :sk_upper"
            values[":sk_upper"] = upper
        elif lower is not None:
            condition = "PK = :pk AND SK >= :sk_lower"
            values[":sk_lower"] = lower
        else:
            raise ValueError("query_range requires a lower or upper bound")
        return self._query(
            condition, values, consistent=consistent, ascending=ascending, limit=limit
        )

    def put_item_sync(self, request: Mapping[str, Any]) -> None:
        self._write(self.client.put_item, request)

    def update_item_sync(self, request: Mapping[str, Any]) -> dict[str, Any]:
        response = self._write(self.client.update_item, request)
        return self.deserialize_item(response.get("Attributes") or {})

    def transact_write_sync(self, actions: Sequence[Mapping[str, Any]]) -> None:
        if not 1 <= len(actions) <= MAX_TRANSACTION_ACTIONS:
            raise ValueError("a transaction needs 1 to 100 actions")
        self._write(self.client.transact_write_items, {"TransactItems": list(actions)})

    def get_idempotency_replay_sync(
        self, tenant_id: str, scope: str, key_hash: str, request_hash: str
    ) -> str | None:
        """Snapshot of a live receipt for the same request; a different request is a conflict."""
        item = self.get_item_sync(
            self.keys.idempotency_receipt(tenant_id, scope, key_hash), consistent=True
        )
        if item is None:
            return None
        # TTL deletion is lazy and the epoch attribute is whole seconds; expiry is decided on the
        # exact deadline derived from the stored creation instant.
        expires_at = parse_canonical_timestamp(item["created_at"]) + IDEMPOTENCY_RECEIPT_TTL
        if self.clock() >= expires_at:
            return None
        if item["request_hash"] != request_hash:
            raise ConflictError("idempotency key reused with a different request")
        return item["response_snapshot_json"]

    # Async wrappers ------------------------------------------------------------------------

    async def get_item(
        self, key: Mapping[str, str], *, consistent: bool = False
    ) -> dict[str, Any] | None:
        return await to_thread(self.get_item_sync, key, consistent=consistent)

    async def query_prefix(
        self, pk: str, sk_prefix: str, *, consistent: bool = False
    ) -> list[dict[str, Any]]:
        return await to_thread(self.query_prefix_sync, pk, sk_prefix, consistent=consistent)

    async def query_range(
        self,
        pk: str,
        *,
        lower: str | None = None,
        upper: str | None = None,
        ascending: bool = True,
        limit: int | None = None,
        consistent: bool = False,
    ) -> list[dict[str, Any]]:
        return await to_thread(
            self.query_range_sync,
            pk,
            lower=lower,
            upper=upper,
            ascending=ascending,
            limit=limit,
            consistent=consistent,
        )

    async def put_item(self, request: Mapping[str, Any]) -> None:
        await to_thread(self.put_item_sync, request)

    async def update_item(self, request: Mapping[str, Any]) -> dict[str, Any]:
        return await to_thread(self.update_item_sync, request)

    async def transact_write(self, actions: Sequence[Mapping[str, Any]]) -> None:
        await to_thread(self.transact_write_sync, actions)

    async def get_idempotency_replay(
        self, tenant_id: str, scope: str, key_hash: str, request_hash: str
    ) -> str | None:
        return await to_thread(
            self.get_idempotency_replay_sync, tenant_id, scope, key_hash, request_hash
        )

    # Internals -----------------------------------------------------------------------------

    def _query(
        self,
        condition: str,
        values: Mapping[str, Any],
        *,
        consistent: bool,
        ascending: bool = True,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        last_evaluated_key: dict[str, Any] | None = None
        while True:
            request: dict[str, Any] = {
                "TableName": self.table_name,
                "KeyConditionExpression": condition,
                "ExpressionAttributeValues": self.serialize_values(values),
                "ConsistentRead": consistent,
                "ScanIndexForward": ascending,
            }
            if limit is not None:
                request["Limit"] = limit - len(items)
            if last_evaluated_key is not None:
                request["ExclusiveStartKey"] = last_evaluated_key
            response = self.client.query(**request)
            items.extend(self.deserialize_item(item) for item in response.get("Items", []))
            last_evaluated_key = response.get("LastEvaluatedKey")
            if last_evaluated_key is None or (limit is not None and len(items) >= limit):
                break
        return items if limit is None else items[:limit]

    def _write(self, operation: Callable[..., Any], request: Mapping[str, Any]) -> Any:
        try:
            return operation(**request)
        except ClientError as exc:
            indexes = conditional_failure_indexes(exc)
            if indexes:
                raise DynamoConditionalConflict(indexes) from exc
            raise

    def _with_condition(
        self,
        request: dict[str, Any],
        condition: str | None,
        names: Mapping[str, str] | None,
        values: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        if condition is not None:
            request["ConditionExpression"] = condition
        if names:
            request["ExpressionAttributeNames"] = dict(names)
        if values:
            request["ExpressionAttributeValues"] = self.serialize_values(values)
        return request

    def _now_epoch(self) -> int:
        return int(self.clock().timestamp())

    def _normalize_to_dynamo(self, value: Any) -> Any:
        if isinstance(value, float):
            return Decimal(str(value))
        if isinstance(value, Mapping):
            return {key: self._normalize_to_dynamo(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [self._normalize_to_dynamo(item) for item in value]
        return value

    def _normalize_from_dynamo(self, value: Any) -> Any:
        if isinstance(value, Decimal):
            return int(value) if value == value.to_integral_value() else float(value)
        if isinstance(value, (set, frozenset)):
            return {self._normalize_from_dynamo(item) for item in value}
        if isinstance(value, list):
            return [self._normalize_from_dynamo(item) for item in value]
        if isinstance(value, Mapping):
            return {key: self._normalize_from_dynamo(item) for key, item in value.items()}
        return value
