"""P3-06: the P3-00 Dynamo-fake expression vectors, run against DynamoDB Local.

The unit suite runs `DYNAMO_EXPRESSION_VECTORS` against the in-memory fake
(`tests/unit/test_ingestion_fakes_contract_kits.py`); running the same vectors here proves the
fake and the real emulator agree on every expression the P3 adapters emit.
"""

from collections.abc import Iterator

import pytest

from tests.support.ingestion_contract_kits import (
    DYNAMO_EXPRESSION_VECTORS,
    DynamoVector,
    run_dynamo_vector,
)
from tests.support.ingestion_harness import (
    IngestionTables,
    dynamodb_client,
    ensure_services_ready,
    table_pair,
)


@pytest.fixture(scope="module")
def vector_tables() -> Iterator[IngestionTables]:
    # One table pair for the module: `run_dynamo_vector` first deletes every row a vector
    # touches, so vectors share a table in any order. The pair is removed at module teardown.
    ensure_services_ready()
    with table_pair(dynamodb_client()) as tables:
        yield tables


@pytest.mark.parametrize("vector", DYNAMO_EXPRESSION_VECTORS, ids=lambda vector: vector.name)
def test_dynamodb_local_passes_every_expression_vector(
    vector: DynamoVector, vector_tables: IngestionTables
) -> None:
    run_dynamo_vector(dynamodb_client(), vector_tables.domain, vector)
