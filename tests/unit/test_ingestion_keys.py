"""P3-00 key schema: builders, `_part` guards, listing-prefix collisions, the account-scoped
identity hash, item codecs and the canonical timestamp vector."""

import math
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from boto3.dynamodb.types import TypeSerializer

from limnopulse_api.adapters.dynamodb_v2 import (
    DynamoV2KeyBuilder,
    account_scoped_identity_hash,
    canonical_timestamp_key,
    component_ingest_state_from_item,
    component_ingest_state_to_item,
    connector_health_from_item,
    connector_health_to_item,
    device_ingest_state_from_item,
    device_ingest_state_to_item,
    external_identity_hash,
    ingest_credential_from_item,
    ingest_credential_to_item,
    ingress_receipt_from_item,
    ingress_receipt_to_item,
    quarantine_entry_from_item,
    quarantine_entry_to_item,
)
from limnopulse_api.domain.ingestion import (
    ComponentIngestState,
    ConnectorHealth,
    DeviceIngestState,
    IngestCredentialStatus,
    IngressFence,
    IngressFenceReason,
    IngressReceiptState,
    build_quarantine_entries,
)
from limnopulse_api.domain.telemetry_v2 import QuarantinedObservation, QuarantineReason
from tests.support.ingestion_contract_kits import (
    ACCOUNT,
    COMPONENT,
    CREDENTIAL,
    DEVICE,
    OBSERVED,
    RECEIVED,
    TENANT,
    credential,
    envelope,
    receipt,
)

KEYS = DynamoV2KeyBuilder()
QTN = "qtn_" + "e" * 32
KEY_SHA = "c" * 64
EXISTING_LISTING_PREFIXES = (
    "SITE#",
    "ASSET#",
    "DEVICE#",
    "COMPONENT#",
    "CAPABILITY#",
    "INTEGRATION_ACCOUNT#",
    "DEVICE_INTEGRATION#",
    "DEPLOYMENT#",
    "POND#",
    "IDEMPOTENCY#",
)


def test_key_builders_produce_the_exact_keys() -> None:
    assert KEYS.ingest_credential(CREDENTIAL) == {
        "PK": f"INGEST_CREDENTIAL#{CREDENTIAL}",
        "SK": "META",
    }
    assert KEYS.ingest_credential_listing(TENANT, ACCOUNT, CREDENTIAL) == {
        "PK": f"TENANT#{TENANT}",
        "SK": f"INGEST_CREDENTIAL#{ACCOUNT}#{CREDENTIAL}",
    }
    assert KEYS.ingest_credential_listing_prefix(ACCOUNT) == f"INGEST_CREDENTIAL#{ACCOUNT}#"
    assert KEYS.ingest_credential_slot(TENANT, ACCOUNT, 1) == {
        "PK": f"TENANT#{TENANT}",
        "SK": f"INGEST_CREDENTIAL_SLOT#{ACCOUNT}#1",
    }
    assert KEYS.ingest_credential_slot(TENANT, ACCOUNT, 2)["SK"].endswith("#2")
    assert KEYS.ingress_receipt(ACCOUNT, KEY_SHA) == {
        "PK": f"INGRESS_RECEIPT#{ACCOUNT}",
        "SK": KEY_SHA,
    }
    assert KEYS.quarantine_partition(TENANT) == f"QUARANTINE#{TENANT}"
    assert KEYS.quarantine_entry(TENANT, RECEIVED, QTN) == {
        "PK": f"QUARANTINE#{TENANT}",
        "SK": f"2026-10-30T12:00:00.000000Z#{QTN}",
    }
    assert KEYS.component_ingest_state(COMPONENT) == {
        "PK": f"COMPONENT#{COMPONENT}",
        "SK": "INGEST_STATE#CURRENT",
    }
    assert KEYS.device_ingest_state(DEVICE) == {
        "PK": f"DEVICE#{DEVICE}",
        "SK": "INGEST_STATE#CURRENT",
    }
    assert KEYS.connector_health(ACCOUNT) == {
        "PK": f"INTEGRATION_ACCOUNT#{ACCOUNT}",
        "SK": "CONNECTOR_HEALTH#CURRENT",
    }


def test_quarantine_sort_key_is_the_entry_key() -> None:
    (entry, _summary) = build_quarantine_entries(
        envelope(),
        fence=IngressFence(reason=IngressFenceReason.ACCOUNT_REVOKED, account_verified=True),
        now=RECEIVED,
    )

    assert KEYS.quarantine_entry(TENANT, entry.received_at, entry.quarantine_id)["SK"] == (
        entry.entry_key
    )


BUILDERS: list[tuple[str, Callable[[str], Any]]] = [
    ("ingest_credential", lambda bad: KEYS.ingest_credential(bad)),
    ("listing-tenant", lambda bad: KEYS.ingest_credential_listing(bad, ACCOUNT, CREDENTIAL)),
    ("listing-account", lambda bad: KEYS.ingest_credential_listing(TENANT, bad, CREDENTIAL)),
    ("listing-credential", lambda bad: KEYS.ingest_credential_listing(TENANT, ACCOUNT, bad)),
    ("listing-prefix", lambda bad: KEYS.ingest_credential_listing_prefix(bad)),
    ("slot-tenant", lambda bad: KEYS.ingest_credential_slot(bad, ACCOUNT, 1)),
    ("slot-account", lambda bad: KEYS.ingest_credential_slot(TENANT, bad, 1)),
    ("receipt-account", lambda bad: KEYS.ingress_receipt(bad, KEY_SHA)),
    ("receipt-key", lambda bad: KEYS.ingress_receipt(ACCOUNT, bad)),
    ("quarantine-partition", lambda bad: KEYS.quarantine_partition(bad)),
    ("quarantine-tenant", lambda bad: KEYS.quarantine_entry(bad, RECEIVED, QTN)),
    ("quarantine-id", lambda bad: KEYS.quarantine_entry(TENANT, RECEIVED, bad)),
    ("component-state", lambda bad: KEYS.component_ingest_state(bad)),
    ("device-state", lambda bad: KEYS.device_ingest_state(bad)),
    ("connector-health", lambda bad: KEYS.connector_health(bad)),
]


@pytest.mark.parametrize("bad", ["", "a#b", None], ids=["empty", "hash", "none"])
@pytest.mark.parametrize(("name", "build"), BUILDERS, ids=[item[0] for item in BUILDERS])
def test_key_parts_are_guarded(name: str, build: Callable[[Any], Any], bad: Any) -> None:
    with pytest.raises(ValueError):
        build(bad)


@pytest.mark.parametrize("slot", [0, 3, -1, "1", True])
def test_credential_slots_are_one_or_two(slot: Any) -> None:
    with pytest.raises(ValueError):
        KEYS.ingest_credential_slot(TENANT, ACCOUNT, slot)


def test_quarantine_entry_key_needs_an_aware_timestamp() -> None:
    with pytest.raises(ValueError):
        KEYS.quarantine_entry(TENANT, datetime(2026, 10, 30, 12), QTN)


def _new_tenant_sks() -> list[str]:
    return [
        KEYS.ingest_credential_listing(TENANT, ACCOUNT, CREDENTIAL)["SK"],
        KEYS.ingest_credential_listing_prefix(ACCOUNT),
        KEYS.ingest_credential_slot(TENANT, ACCOUNT, 1)["SK"],
        KEYS.ingest_credential_slot(TENANT, ACCOUNT, 2)["SK"],
    ]


def test_new_tenant_sort_keys_never_extend_an_existing_listing_prefix() -> None:
    for sk in _new_tenant_sks():
        assert sk.startswith(("INGEST_CREDENTIAL#", "INGEST_CREDENTIAL_SLOT#")), sk
        for prefix in EXISTING_LISTING_PREFIXES:
            assert not sk.startswith(prefix), (sk, prefix)
    # Listing credentials by account must never return slot rows (and vice versa).
    listing = KEYS.ingest_credential_listing_prefix(ACCOUNT)
    assert not KEYS.ingest_credential_slot(TENANT, ACCOUNT, 1)["SK"].startswith(listing)


def test_new_component_and_device_rows_stay_out_of_existing_ranges() -> None:
    lower, upper = KEYS.deployment_history_bounds()
    for key in (KEYS.component_ingest_state(COMPONENT), KEYS.device_ingest_state(DEVICE)):
        assert not lower <= key["SK"] <= upper
        assert not key["SK"].startswith(("CAPABILITY#", "DEPLOYMENT#", "META"))
        assert not key["SK"].startswith(EXISTING_LISTING_PREFIXES)
    assert KEYS.component_ingest_state(COMPONENT)["PK"] == KEYS.component(COMPONENT)["PK"]
    assert KEYS.device_ingest_state(DEVICE)["PK"] == KEYS.device_lookup(DEVICE)["PK"]
    assert KEYS.connector_health(ACCOUNT)["SK"] != "HEALTH#CURRENT"
    assert KEYS.component_ingest_state(COMPONENT)["SK"] != "HEALTH#CURRENT"


def test_new_partitions_do_not_collide_with_existing_partitions() -> None:
    existing = {
        KEYS.tenant_partition(TENANT),
        KEYS.external_identity("https_direct", "a" * 64)["PK"],
        KEYS.component(COMPONENT)["PK"],
        KEYS.device_lookup(DEVICE)["PK"],
    }
    new = {
        KEYS.ingest_credential(CREDENTIAL)["PK"],
        KEYS.ingress_receipt(ACCOUNT, KEY_SHA)["PK"],
        KEYS.quarantine_partition(TENANT),
        KEYS.connector_health(ACCOUNT)["PK"],
    }
    assert not existing & new
    assert KEYS.connector_health(ACCOUNT)["PK"] != KEYS.integration_account(TENANT, ACCOUNT)["PK"]


# Account-scoped identity hash ---------------------------------------------------------------


def test_account_scoped_identity_hash_vector() -> None:
    expected = "92e52299cbef6deb00855e3f1dbd434c0542ae013b2f88eff9c6fbd9fa86b235"

    assert account_scoped_identity_hash("https_direct", ACCOUNT, "sensor-01") == expected
    assert expected == external_identity_hash("https_direct", f"{ACCOUNT}\x1fsensor-01")
    assert expected != external_identity_hash("https_direct", "sensor-01")
    assert account_scoped_identity_hash("https_direct", "ia_" + "6" * 32, "sensor-01") != expected
    assert account_scoped_identity_hash("https_direct", ACCOUNT, "sensor-02") != expected


@pytest.mark.parametrize(
    ("provider", "account", "external"),
    [
        ("Bad Provider", ACCOUNT, "sensor-01"),
        ("https_direct", "", "sensor-01"),
        ("https_direct", "ia\x1fx", "sensor-01"),
        ("https_direct", ACCOUNT, ""),
    ],
    ids=["provider", "empty-account", "separator-in-account", "empty-identity"],
)
def test_account_scoped_identity_hash_rejects_ambiguous_input(
    provider: str, account: str, external: str
) -> None:
    with pytest.raises(ValueError):
        account_scoped_identity_hash(provider, account, external)


# Canonical timestamps ------------------------------------------------------------------------


def test_canonical_timestamp_vector_and_why_isoformat_is_forbidden() -> None:
    whole = datetime(2026, 10, 30, 12, 0, tzinfo=UTC)
    later = whole + timedelta(microseconds=1)

    assert canonical_timestamp_key(whole) == "2026-10-30T12:00:00.000000Z"
    assert canonical_timestamp_key(later) == "2026-10-30T12:00:00.000001Z"
    assert canonical_timestamp_key(whole) == whole.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    assert canonical_timestamp_key(whole) < canonical_timestamp_key(later)
    iso_whole = whole.isoformat().replace("+00:00", "Z")
    iso_later = later.isoformat().replace("+00:00", "Z")
    assert iso_whole == "2026-10-30T12:00:00Z"
    assert iso_whole > iso_later  # wrong order: "Z" sorts after "."


# Item codecs ---------------------------------------------------------------------------------


def _assert_dynamo_safe(item: Mapping[str, Any]) -> None:
    serializer = TypeSerializer()
    for value in item.values():
        serializer.serialize(value)  # rejects float and datetime


def test_ingest_credential_item_round_trip() -> None:
    value = credential(
        status=IngestCredentialStatus.REVOKED,
        revoked_at=RECEIVED + timedelta(microseconds=7),
        updated_at=RECEIVED + timedelta(microseconds=7),
        version=2,
    )

    item = ingest_credential_to_item(value)

    _assert_dynamo_safe(item)
    assert {key: item[key] for key in ("PK", "SK", "entity_type")} == {
        **KEYS.ingest_credential(CREDENTIAL),
        "entity_type": "ingest_credential",
    }
    assert item["revoked_at"] == "2026-10-30T12:00:00.000007Z"
    assert item["created_at"] == "2026-01-01T00:00:00.000000Z"
    assert ingest_credential_from_item(item) == value
    assert ingest_credential_from_item(ingest_credential_to_item(credential())) == credential()


def test_ingress_receipt_item_round_trip() -> None:
    value = receipt(created_at=RECEIVED + timedelta(microseconds=1)).model_copy(
        update={"state": IngressReceiptState.ENQUEUED}
    )

    item = ingress_receipt_to_item(value)

    _assert_dynamo_safe(item)
    assert {key: item[key] for key in ("PK", "SK", "entity_type")} == {
        **KEYS.ingress_receipt(ACCOUNT, value.key_sha256),
        "entity_type": "ingress_receipt",
    }
    assert item["created_at_us"] == value.created_at_us
    assert item["expires_at"] == math.ceil(
        (RECEIVED + timedelta(hours=24, microseconds=1)).timestamp()
    )
    assert isinstance(item["expires_at"], int)
    assert item["state"] == "enqueued"
    assert ingress_receipt_from_item(item) == value


def test_quarantine_entry_item_round_trip() -> None:
    fenced = build_quarantine_entries(
        envelope(connector_version="gw/1", replay=True, replay_of="ing_" + "5" * 32),
        fence=IngressFence(reason=IngressFenceReason.ACCOUNT_REVOKED, account_verified=True),
        now=RECEIVED + timedelta(microseconds=1),
    )
    quarantined = QuarantinedObservation(
        reason=QuarantineReason.METRIC_UNKNOWN,
        tenant_id=TENANT,
        device_id=DEVICE,
        component_id=COMPONENT,
        metric_key="nope",
        observed_at=OBSERVED,
        received_at=RECEIVED,
        source_event_id=None,
        detail="unknown metric",
        source_excerpt_json='{"metric_key":"nope"}',
        truncated=False,
    )
    from limnopulse_api.services.telemetry_pipeline import PipelineOutcome

    observation_rows = build_quarantine_entries(
        envelope(received_at=RECEIVED + timedelta(microseconds=3)),
        outcome=PipelineOutcome(written=None, quarantined=(quarantined,), counts={}),  # type: ignore[arg-type]
        now=RECEIVED + timedelta(microseconds=1),
    )

    for entry in (*fenced, *observation_rows):
        item = quarantine_entry_to_item(entry)
        _assert_dynamo_safe(item)
        assert {key: item[key] for key in ("PK", "SK", "entity_type")} == {
            "PK": f"QUARANTINE#{TENANT}",
            "SK": entry.entry_key,
            "entity_type": "telemetry_quarantine",
        }
        assert item["expires_at"] == math.ceil(entry.expires_at.timestamp())
        assert item["received_at"] == canonical_timestamp_key(entry.received_at)
        assert quarantine_entry_from_item(item) == entry


def test_ingest_state_items_round_trip_and_tolerate_missing_marks() -> None:
    full = ComponentIngestState(
        tenant_id=TENANT,
        component_id=COMPONENT,
        device_id=DEVICE,
        last_received_at=RECEIVED,
        last_ingested_at=RECEIVED + timedelta(seconds=1),
        last_observed_at=OBSERVED,
        last_observed_sequence=2**63 - 1,
    )
    device_state = DeviceIngestState(
        tenant_id=TENANT, device_id=DEVICE, last_received_at=RECEIVED, last_ingested_at=None
    )
    health = ConnectorHealth(
        tenant_id=TENANT,
        integration_account_id=ACCOUNT,
        last_received_at=RECEIVED,
        last_accepted_at=None,
        last_quarantined_at=RECEIVED,
        last_quarantine_reason="metric_unknown",
        last_fenced_at=None,
        last_fence_reason=None,
    )
    cases: list[tuple[Any, Callable[[Any], dict], Callable[[Mapping[str, Any]], Any], dict]] = [
        (
            full,
            component_ingest_state_to_item,
            component_ingest_state_from_item,
            {**KEYS.component_ingest_state(COMPONENT), "entity_type": "component_ingest_state"},
        ),
        (
            device_state,
            device_ingest_state_to_item,
            device_ingest_state_from_item,
            {**KEYS.device_ingest_state(DEVICE), "entity_type": "device_ingest_state"},
        ),
        (
            health,
            connector_health_to_item,
            connector_health_from_item,
            {**KEYS.connector_health(ACCOUNT), "entity_type": "connector_health"},
        ),
    ]

    for value, to_item, from_item, identity in cases:
        item = to_item(value)
        _assert_dynamo_safe(item)
        assert {key: item[key] for key in identity} == identity
        assert all(stored is not None for stored in item.values()), item
        assert from_item(item) == value

    assert component_ingest_state_to_item(full)["last_received_at"] == (
        "2026-10-30T12:00:00.000000Z"
    )
    minimal = {
        **KEYS.component_ingest_state(COMPONENT),
        "entity_type": "component_ingest_state",
        "tenant_id": TENANT,
        "component_id": COMPONENT,
        "device_id": DEVICE,
    }
    assert component_ingest_state_from_item(minimal) == ComponentIngestState(
        tenant_id=TENANT,
        component_id=COMPONENT,
        device_id=DEVICE,
        last_received_at=None,
        last_ingested_at=None,
        last_observed_at=None,
        last_observed_sequence=None,
    )


def test_receipt_and_quarantine_are_the_new_ttl_rows() -> None:
    entry = build_quarantine_entries(
        envelope(),
        fence=IngressFence(reason=IngressFenceReason.ACCOUNT_REVOKED, account_verified=True),
        now=RECEIVED,
    )[0]

    assert "expires_at" in ingress_receipt_to_item(receipt())
    assert "expires_at" in quarantine_entry_to_item(entry)
    assert "expires_at" not in ingest_credential_to_item(credential())
    state = ComponentIngestState(
        tenant_id=TENANT,
        component_id=COMPONENT,
        device_id=DEVICE,
        last_received_at=RECEIVED,
        last_ingested_at=RECEIVED,
        last_observed_at=None,
        last_observed_sequence=None,
    )
    assert "expires_at" not in component_ingest_state_to_item(state)
    assert "expires_at" not in device_ingest_state_to_item(
        DeviceIngestState(
            tenant_id=TENANT, device_id=DEVICE, last_received_at=RECEIVED, last_ingested_at=None
        )
    )
    assert "expires_at" not in connector_health_to_item(
        ConnectorHealth(
            tenant_id=TENANT,
            integration_account_id=ACCOUNT,
            last_received_at=RECEIVED,
            last_accepted_at=None,
            last_quarantined_at=None,
            last_quarantine_reason=None,
            last_fenced_at=None,
            last_fence_reason=None,
        )
    )
