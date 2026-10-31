"""P3-00 runs every contract kit against its in-memory fake, and every DynamoDB expression vector
against the Dynamo fake that reuses the `tests/contracts/m1_fakes.py` evaluator."""

from collections.abc import Callable, Sequence
from typing import Any

import pytest

from limnopulse_api.repositories import ingestion as ports
from tests.contracts.m1_fakes import ConditionalDynamoClient
from tests.support import ingestion_fakes as fakes
from tests.support.ingestion_contract_kits import (
    CONNECTOR_AUTHENTICATOR_CONTRACT,
    CONSISTENT_DEVICE_READER_CONTRACT,
    CREDENTIAL_REPOSITORY_CONTRACT,
    DYNAMO_EXPRESSION_VECTORS,
    ENTITLEMENT_CHECK_CONTRACT,
    EXTERNAL_IDENTITY_RESOLVER_CONTRACT,
    INGEST_STATE_CONTRACT,
    INGRESS_QUEUE_CONTRACT,
    INGRESS_RECEIPT_CONTRACT,
    INTEGRATION_LIFECYCLE_CONTRACT,
    QUARANTINE_REPOSITORY_CONTRACT,
    RATE_LIMITER_CONTRACT,
    TELEMETRY_METRICS_CONTRACT,
    TRUSTED_MAPPING_CONTRACT,
    DynamoVector,
    run_dynamo_vector,
)
from tests.support.ingestion_dynamo_fake import IngestionDynamoClient

# kit name -> (scenarios, fake subject factory, (subject attribute, protocol) pairs)
KITS: dict[
    str, tuple[Sequence[Callable[..., Any]], Callable[[], Any], tuple[tuple[str, type], ...]]
] = {
    "credentials": (
        CREDENTIAL_REPOSITORY_CONTRACT,
        fakes.credential_subject,
        (("repository", ports.IngestCredentialRepository),),
    ),
    "authenticator": (
        CONNECTOR_AUTHENTICATOR_CONTRACT,
        fakes.authenticator_subject,
        (("authenticator", ports.ConnectorAuthenticator),),
    ),
    "resolver": (
        EXTERNAL_IDENTITY_RESOLVER_CONTRACT,
        fakes.resolver_subject,
        (("resolver", ports.ExternalIdentityResolver),),
    ),
    "lifecycle": (
        INTEGRATION_LIFECYCLE_CONTRACT,
        fakes.lifecycle_subject,
        (("repository", ports.IntegrationLifecycleRepository),),
    ),
    "devices": (
        CONSISTENT_DEVICE_READER_CONTRACT,
        fakes.device_reader_subject,
        (("reader", ports.ConsistentDeviceReader),),
    ),
    "trusted_mapping": (
        TRUSTED_MAPPING_CONTRACT,
        fakes.trusted_mapping_subject,
        (("mapping", ports.TrustedMapping),),
    ),
    "receipts": (
        INGRESS_RECEIPT_CONTRACT,
        fakes.receipt_subject,
        (("repository", ports.IngressReceiptRepository),),
    ),
    "queue": (
        INGRESS_QUEUE_CONTRACT,
        fakes.queue_subject,
        (("producer", ports.IngressQueueProducer), ("consumer", ports.IngressQueueConsumer)),
    ),
    "quarantine": (
        QUARANTINE_REPOSITORY_CONTRACT,
        fakes.quarantine_subject,
        (("repository", ports.QuarantineRepository),),
    ),
    "ingest_state": (
        INGEST_STATE_CONTRACT,
        fakes.ingest_state_subject,
        (("repository", ports.IngestStateRepository),),
    ),
    "entitlement": (
        ENTITLEMENT_CHECK_CONTRACT,
        fakes.entitlement_subject,
        (("check", ports.IngressEntitlementCheck),),
    ),
    "metrics": (
        TELEMETRY_METRICS_CONTRACT,
        fakes.metrics_subject,
        (("metrics", ports.TelemetryMetrics),),
    ),
}
CASES = [(kit, scenario) for kit, (scenarios, _, _) in KITS.items() for scenario in scenarios]


def test_every_port_has_a_kit() -> None:
    covered = {protocol for _, _, pairs in KITS.values() for _, protocol in pairs}
    covered.add(ports.IngressRateLimiter)
    protocols = {
        value
        for value in vars(ports).values()
        if isinstance(value, type)
        and getattr(value, "_is_protocol", False)
        and value.__module__ == ports.__name__
    }

    assert covered == protocols


@pytest.mark.parametrize("kit", list(KITS))
def test_fakes_implement_their_ports(kit: str) -> None:
    _, make, pairs = KITS[kit]
    subject = make()

    for attribute, protocol in pairs:
        assert isinstance(getattr(subject, attribute), protocol), (kit, attribute)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kit", "scenario"), CASES, ids=[f"{kit}-{scenario.__name__}" for kit, scenario in CASES]
)
async def test_fake_passes_its_contract_kit(kit: str, scenario: Callable[[Any], Any]) -> None:
    await scenario(KITS[kit][1]())


@pytest.mark.parametrize("scenario", RATE_LIMITER_CONTRACT, ids=lambda item: item.__name__)
def test_fake_rate_limiter_passes_its_contract_kit(scenario: Callable[[Any], None]) -> None:
    scenario(fakes.InMemoryRateLimiter)


def test_fake_rate_limiter_implements_its_port() -> None:
    limiter = fakes.InMemoryRateLimiter(capacity=1, refill_per_second=1.0, max_keys=100)

    assert isinstance(limiter, ports.IngressRateLimiter)


def test_fake_metrics_refuse_open_names_and_labels() -> None:
    subject = fakes.metrics_subject()

    with pytest.raises(ValueError):
        subject.metrics.increment("ingest_requests_total", outcome="accepted", tenant_id="t")
    with pytest.raises(ValueError):
        subject.metrics.increment("ingest_unknown_total")
    with pytest.raises(ValueError):
        subject.metrics.observe("ingress_queue_age_seconds", 1.0, device_id="d")
    assert subject.recorded() == []


# DynamoDB fake ---------------------------------------------------------------------------------


def test_dynamo_fake_reuses_the_m1_evaluator() -> None:
    assert issubclass(IngestionDynamoClient, ConditionalDynamoClient)


@pytest.mark.parametrize("vector", DYNAMO_EXPRESSION_VECTORS, ids=lambda vector: vector.name)
def test_dynamo_fake_passes_every_expression_vector(vector: DynamoVector) -> None:
    run_dynamo_vector(IngestionDynamoClient(), "LimnopulseDomain", vector)


def test_vectors_cover_every_frozen_expression() -> None:
    names = {vector.name for vector in DYNAMO_EXPRESSION_VECTORS}
    expressions = " ".join(str(vector.request) for vector in DYNAMO_EXPRESSION_VECTORS)

    assert len(names) == len(DYNAMO_EXPRESSION_VECTORS)
    for fragment in (
        "attribute_not_exists(PK)",
        "attribute_not_exists(PK) OR #created_at_us <= :cutoff_us",
        "attribute_exists(PK) AND #status IN (:pending, :active) AND provider_key = :https",
        "#version = :v",
        "tenant_id = :t AND integration_account_id = :ia AND #status = :active AND #version = :v",
        "attribute_not_exists(#m) OR #m < :t",
        "attribute_not_exists(#m) OR #m < :t OR (#m = :t AND #tb < :tb)",
        "attribute_exists(PK) AND #version = :v",
        "attribute_exists(PK) AND #status <> :decommissioned",
        "if_not_exists(tenant_id, :tenant)",
        "REMOVE lease_owner",
        "ReturnValuesOnConditionCheckFailure",
        "begins_with(SK, :sk_prefix)",
        "SK BETWEEN :sk_lower AND :sk_upper",
        "SK > :sk_lower",
        "ScanIndexForward",
        "'ConditionCheck'",
        "'Delete'",
    ):
        assert fragment in expressions, fragment


@pytest.mark.parametrize("operation", ["scan", "batch_write_item", "batch_get_item"])
def test_dynamo_fake_refuses_unlisted_operations(operation: str) -> None:
    client = IngestionDynamoClient()

    with pytest.raises(AssertionError):
        getattr(client, operation)(TableName="LimnopulseDomain")
