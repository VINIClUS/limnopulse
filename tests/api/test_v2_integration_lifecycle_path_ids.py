"""P3-04 diff-review regression: malformed path ids on the lifecycle routes are redacted 422s.

A non-canonical id (one containing `#`, or far over DynamoDB's key size) used to reach the key
builder and surface as a 500 (or as a 503 for an oversized key). Both routes now validate
`ia_<32 hex>` / `di_<32 hex>` path ids before any store call.
"""

from urllib.parse import quote

import pytest

from tests.api.test_v2_integration_lifecycle import (
    WRITE_OPERATIONS,
    _seeded,
    client_for,
)
from tests.support.ingestion_contract_kits import ACCOUNT, DEVICE, INTEGRATION, TENANT

SENTINEL = "SENTINEL#bad-id-5c1e"
BAD_IDS = [SENTINEL, "ia_" + "1" * 31, "x" * 2_000]


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", BAD_IDS, ids=["hash-sign", "short", "oversized"])
@pytest.mark.parametrize("route", ["account", "integration"])
async def test_malformed_path_ids_are_redacted_422s(route: str, bad: str) -> None:
    subject = await _seeded()
    if route == "account":
        path = f"/tenants/{TENANT}/integration-accounts/{quote(bad, safe='')}/status"
    else:
        path = f"/tenants/{TENANT}/devices/{DEVICE}/integrations/{quote(bad, safe='')}/status"

    response = client_for(subject).post(path, json={"status": "active", "expected_version": 1})

    assert response.status_code == 422
    assert SENTINEL not in response.text
    assert subject.client.calls == []
    assert not [call for call in subject.client.calls if call.operation in WRITE_OPERATIONS]


@pytest.mark.asyncio
async def test_canonical_path_ids_still_reach_the_store() -> None:
    subject = await _seeded()
    client = client_for(subject)

    account = client.post(
        f"/tenants/{TENANT}/integration-accounts/{ACCOUNT}/status",
        json={"status": "active", "expected_version": 1},
    )
    integration = client.post(
        f"/tenants/{TENANT}/devices/{DEVICE}/integrations/{INTEGRATION}/status",
        json={"status": "active", "expected_version": 1},
    )

    assert (account.status_code, integration.status_code) == (200, 200)
