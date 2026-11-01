"""#116 regressions against DynamoDB Local (new file; skipped unless
`RUN_INGESTION_INTEGRATION=1`, as `make verify-ingestion` sets it).

- P3-04: the same-status no-op and the ownership check run the unit scenarios on the real store.
- P3-06: a page of exactly `limit` rows needs one Query and no follow-up page.
- P3-00: DynamoDB Local rejects the unaliased reserved words the unit fake now rejects too.
"""

from __future__ import annotations

from typing import Any

import pytest
from botocore.exceptions import ClientError

from limnopulse_api.adapters.dynamodb_quarantine import DynamoQuarantineRepository
from tests.support.ingestion_contract_kits import RECEIVED, TENANT, fenced_entries
from tests.support.ingestion_dynamo_fake import IngestionDynamoClient
from tests.support.ingestion_harness import IngestionTables, dynamodb_client
from tests.unit.test_116_review_regressions import (  # noqa: F401 - re-run on DynamoDB Local
    RESERVED_CASES,
    reserved_request,
    test_a_same_status_account_transition_is_a_no_op,
    test_a_same_status_device_integration_transition_is_a_no_op,
    test_a_stale_same_status_transition_is_still_a_version_conflict,
    test_every_same_status_account_transition_is_a_no_op,
    test_every_same_status_device_integration_transition_is_a_no_op,
    test_the_repository_404s_another_devices_integration_before_the_version,
)
from tests.unit.test_dynamodb_integration_resolver import DynamoIngestionSubject


@pytest.fixture
def lifecycle(ingestion_tables: IngestionTables) -> DynamoIngestionSubject:
    """The unit lifecycle scenarios' subject, on DynamoDB Local with its real audit table."""
    client = dynamodb_client()
    subject = DynamoIngestionSubject(
        client, domain_table=ingestion_tables.domain, audit_table=ingestion_tables.audit
    )

    def audit_items() -> list[dict[str, Any]]:
        response = client.scan(TableName=ingestion_tables.audit, ConsistentRead=True)
        assert "LastEvaluatedKey" not in response  # a throwaway table, one page
        return [subject.integrations.deserialize_item(item) for item in response["Items"]]

    subject._audit_reader = audit_items
    return subject


class _CountingClient:
    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.queries = 0

    def query(self, **kwargs: Any) -> Any:
        self.queries += 1
        return self.inner.query(**kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)


@pytest.mark.asyncio
@pytest.mark.parametrize(("rows", "more"), [(3, False), (4, True)], ids=["exact", "one_more"])
async def test_a_page_of_exactly_limit_rows_costs_one_query_on_dynamodb_local(
    ingestion_tables: IngestionTables, rows: int, more: bool
) -> None:
    client = _CountingClient(dynamodb_client())
    repository = DynamoQuarantineRepository(ingestion_tables.domain, client, clock=lambda: RECEIVED)
    await repository.put_quarantine_entries(fenced_entries(rows - 1))

    page = await repository.list_quarantine_entries(TENANT, after_sk=None, limit=3)

    assert len(page.entries) == 3
    assert client.queries == 1
    assert (page.next_after_sk is not None) is more


@pytest.mark.parametrize("case", RESERVED_CASES)
def test_dynamodb_local_and_the_fake_both_reject_an_unaliased_reserved_word(
    ingestion_tables: IngestionTables, case: str
) -> None:
    codes = []
    for client, table in (
        (dynamodb_client(), ingestion_tables.domain),
        (IngestionDynamoClient(), "LimnopulseDomain"),
    ):
        operation, request = reserved_request(case, table)
        with pytest.raises(ClientError) as exc_info:
            getattr(client, operation)(**request)
        error = exc_info.value.response["Error"]
        assert "reserved keyword" in error["Message"], error
        codes.append(error["Code"])

    assert codes == ["ValidationException", "ValidationException"]
