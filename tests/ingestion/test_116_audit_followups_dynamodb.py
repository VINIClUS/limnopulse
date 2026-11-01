"""#116 audit follow-ups against DynamoDB Local (new file; skipped unless
`RUN_INGESTION_INTEGRATION=1`, as `make verify-ingestion` sets it).

- Finding 1: the two-page quarantine listing, on the real store.
- Finding 4: the EXTERNAL_ID lookup-row version fence, on the real store's condition evaluator.
"""

from __future__ import annotations

from typing import Any

import pytest

from limnopulse_api.adapters.dynamodb_quarantine import DynamoQuarantineRepository
from tests.support.ingestion_contract_kits import RECEIVED
from tests.support.ingestion_harness import IngestionTables, dynamodb_client
from tests.unit.test_116_audit_followups import (  # noqa: F401 - re-run on DynamoDB Local
    test_a_drifted_lookup_row_version_fails_the_transition,
    two_page_listing,
)
from tests.unit.test_dynamodb_integration_resolver import DynamoIngestionSubject


@pytest.fixture
def dynamo_lifecycle(ingestion_tables: IngestionTables) -> DynamoIngestionSubject:
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


@pytest.mark.asyncio
async def test_a_short_last_read_that_still_shows_a_row_keeps_the_cursor_on_dynamodb_local(
    ingestion_tables: IngestionTables,
) -> None:
    repository = DynamoQuarantineRepository(
        ingestion_tables.domain, dynamodb_client(), clock=lambda: RECEIVED
    )
    await two_page_listing(repository)
