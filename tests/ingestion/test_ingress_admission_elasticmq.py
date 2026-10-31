"""P3-07 on DynamoDB Local and ElasticMQ: the receipt adapter passes the P3-00 receipt kit (fault
scenarios included), and one full admission runs the real stack end to end: request -> real
authenticator, trusted mapping and receipts on DynamoDB Local -> a message on a real ElasticMQ
queue whose body equals the expected `telemetry-ingress@1` envelope. A failed real send answers
503 and leaves the receipt pending; the retry answers 202 with the first `received_at`.

Skips unless ``RUN_INGESTION_INTEGRATION=1`` (``make verify-ingestion``).
"""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable
from typing import Any

import pytest
from fastapi import FastAPI

from limnopulse_api.adapters.dynamodb_ingest_credentials import DynamoIngestCredentialRepository
from limnopulse_api.adapters.dynamodb_ingress_receipts import DynamoIngressReceiptRepository
from limnopulse_api.adapters.dynamodb_v2 import idempotency_key_hash
from limnopulse_api.adapters.sqs_ingestion import SqsIngressQueue
from limnopulse_api.api import ingest_v2
from limnopulse_api.core.config import Settings
from limnopulse_api.domain.ingestion import (
    INGRESS_ENVELOPE_SCHEMA,
    IngressEnvelope,
    IngressReceiptState,
    ingest_token,
)
from limnopulse_api.domain.telemetry_v2 import NormalizationVersions
from limnopulse_api.services.connector_auth import IngestCredentialAuthenticator
from limnopulse_api.services.ingress_entitlements import AllowDirectIngressEntitlements
from limnopulse_api.services.ingress_rate_limit import ingress_rate_limiters
from limnopulse_api.services.trusted_mapping import TrustedMappingService
from tests.api.test_ingest_v2_route import headers, post, reading
from tests.support.ingestion_contract_kits import (
    ACCOUNT,
    AUDIT,
    COMPONENT,
    CREDENTIAL,
    DEFAULT_COMPONENT,
    DEVICE,
    EXTERNAL_DEVICE,
    IDENTITY_HASH,
    INGRESS_RECEIPT_CONTRACT,
    INTEGRATION,
    PROVIDER,
    SECRET,
    TENANT,
    component,
    credential,
    device,
    device_integration,
    integration_account,
    kit_ids,
)
from tests.support.ingestion_fakes import RecordingTelemetryMetrics
from tests.support.ingestion_harness import (
    IngestionQueuePair,
    IngestionTables,
    dynamodb_client,
    sqs_client,
)
from tests.unit.test_dynamodb_integration_resolver import DynamoIngestionSubject

KEY = "elasticmq-key-0001"
_BOTO_OPERATION = {
    "get_ingress_receipt": "get_item",
    "claim_ingress_receipt": "put_item",
    "mark_ingress_receipt_enqueued": "update_item",
}


class FaultingClient:
    """A real boto3 DynamoDB client whose next call of an operation can raise once."""

    def __init__(self, client: Any) -> None:
        self._client = client
        self._faults: dict[str, list[BaseException]] = defaultdict(list)

    def fail_next(self, operation: str, exc: BaseException) -> None:
        self._faults[operation].append(exc)

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self._client, name)
        if not callable(attribute):
            return attribute

        def call(*args: Any, **kwargs: Any) -> Any:
            pending = self._faults.get(name)
            if pending:
                raise pending.pop(0)
            return attribute(*args, **kwargs)

        return call


class LocalReceiptSubject:
    def __init__(self, tables: IngestionTables) -> None:
        self.client = FaultingClient(dynamodb_client())
        self.repository = DynamoIngressReceiptRepository(tables.domain, self.client)

    def fail_next(self, operation: str, exc: BaseException) -> None:
        self.client.fail_next(_BOTO_OPERATION[operation], exc)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario", INGRESS_RECEIPT_CONTRACT, ids=kit_ids(INGRESS_RECEIPT_CONTRACT)
)
async def test_receipt_kit_on_dynamodb_local(
    ingestion_tables: IngestionTables, scenario: Callable[[Any], Any]
) -> None:
    await scenario(LocalReceiptSubject(ingestion_tables))


class Stack:
    """The whole ingress over DynamoDB Local and ElasticMQ, mounted on a bare app."""

    def __init__(self, tables: IngestionTables, queues: IngestionQueuePair) -> None:
        dynamo = dynamodb_client()
        self.sqs = sqs_client()
        self.domain = DynamoIngestionSubject(
            dynamo, domain_table=tables.domain, audit_table=tables.audit
        )
        self.credentials = DynamoIngestCredentialRepository(
            tables.domain, dynamo, audit_table_name=tables.audit
        )
        self.receipts = DynamoIngressReceiptRepository(tables.domain, dynamo)
        self.queue = SqsIngressQueue(client=self.sqs, queue_url=queues.source_url)
        self.metrics = RecordingTelemetryMetrics()
        limiters = ingress_rate_limiters(Settings(_env_file=None, app_env="test", auth_mode="dev"))
        self.app = FastAPI()
        self.app.include_router(ingest_v2.router)
        state = self.app.state
        state.connector_authenticator = IngestCredentialAuthenticator(
            credentials=self.credentials, integrations=self.domain.integrations
        )
        state.trusted_mapping = TrustedMappingService(
            resolver=self.domain.integrations,
            integrations=self.domain.integrations,
            devices=self.domain.devices,
            components=self.domain.devices,
        )
        state.ingress_receipt_repository = self.receipts
        state.ingress_queue_producer = self.queue
        state.ingress_preauth_global_rate_limiter = limiters.preauth_global
        state.ingress_preauth_rate_limiter = limiters.preauth
        state.ingress_rate_limiter = limiters.account
        state.ingress_entitlement_check = AllowDirectIngressEntitlements()
        state.telemetry_metrics = self.metrics

    async def seed(self) -> None:
        await self.domain.seed_account(integration_account())
        await self.domain.seed_device_integration(device_integration())
        await self.domain.seed_device(device())
        await self.domain.seed_component(component())
        await self.domain.seed_component(component(DEFAULT_COMPONENT))
        await self.credentials.create_ingest_credential(credential(), AUDIT)

    async def drain(self) -> list[Any]:
        return await self.queue.receive(max_messages=10, wait_seconds=1, visibility_seconds=60)


def _request() -> dict[str, Any]:
    return {
        "external_device_id": EXTERNAL_DEVICE,
        "observations": [reading(0), reading(1, component_id=None)],
    }


def _expected_body(ingress_id: str, received_at: Any) -> str:
    observations = []
    for item in _request()["observations"]:
        observations.append({**item, "component_id": item.get("component_id", DEFAULT_COMPONENT)})
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
            "connector_version": "fw-1.0",
            "received_at": received_at,
            "mapping_versions": NormalizationVersions(),
            "observations": observations,
        }
    ).to_json()


@pytest.mark.asyncio
async def test_full_admission_puts_the_expected_envelope_on_a_real_queue(
    ingestion_tables: IngestionTables, ingestion_queue_pair: Callable[..., IngestionQueuePair]
) -> None:
    stack = Stack(ingestion_tables, ingestion_queue_pair(visibility_seconds=60))
    await stack.seed()
    body = json.dumps(_request()).encode()

    response = await post(stack.app, body, header_map=headers(idempotency_key=KEY))

    assert response.status == 202, response.body
    accepted = response.json()
    assert accepted["accepted_observations"] == 2 and accepted["replayed"] is False
    receipt = await stack.receipts.get_ingress_receipt(ACCOUNT, idempotency_key_hash(KEY))
    assert receipt is not None
    assert receipt.state == IngressReceiptState.ENQUEUED
    assert receipt.ingress_id == accepted["ingress_id"]
    (message,) = await stack.drain()
    assert message.body == _expected_body(accepted["ingress_id"], receipt.received_at)
    assert COMPONENT in message.body and DEFAULT_COMPONENT in message.body
    for secret in (ingest_token(CREDENTIAL, SECRET), SECRET, KEY, EXTERNAL_DEVICE):
        assert secret not in message.body

    replay = await post(stack.app, body, header_map=headers(idempotency_key=KEY))

    assert replay.status == 202
    assert replay.json() == {**accepted, "replayed": True}
    assert await stack.drain() == []


@pytest.mark.asyncio
async def test_a_failed_real_send_is_503_and_the_retry_keeps_the_first_received_at(
    ingestion_tables: IngestionTables, ingestion_queue_pair: Callable[..., IngestionQueuePair]
) -> None:
    queues = ingestion_queue_pair(visibility_seconds=60)
    stack = Stack(ingestion_tables, queues)
    await stack.seed()
    body = json.dumps(_request()).encode()
    stack.app.state.ingress_queue_producer = SqsIngressQueue(
        client=stack.sqs, queue_url=queues.source_url + "-missing"
    )

    failed = await post(stack.app, body, header_map=headers(idempotency_key=KEY))

    assert (failed.status, failed.json()) == (503, {"detail": "service unavailable"})
    pending = await stack.receipts.get_ingress_receipt(ACCOUNT, idempotency_key_hash(KEY))
    assert pending is not None and pending.state == IngressReceiptState.PENDING
    assert await stack.drain() == []

    stack.app.state.ingress_queue_producer = stack.queue
    retried = await post(stack.app, body, header_map=headers(idempotency_key=KEY))

    assert retried.status == 202
    assert retried.json() == {
        "ingress_id": pending.ingress_id,
        "accepted_observations": 2,
        "replayed": True,
    }
    (message,) = await stack.drain()
    assert message.body == _expected_body(pending.ingress_id, pending.received_at)
    enqueued = await stack.receipts.get_ingress_receipt(ACCOUNT, idempotency_key_hash(KEY))
    assert enqueued == pending.model_copy(update={"state": IngressReceiptState.ENQUEUED})
