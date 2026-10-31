"""P3-07 diff-review regressions (GREEN-phase test file, disclosed in the PR; the approved test
files stay frozen at 758f462).

- An over-long `Content-Length` (beyond CPython's 4300-digit `int()` cap) is a 413, never a 500,
  and leading zeros do not hide a length within the limit.
- `Content-Type` may carry empty parameters (RFC 9110): `application/json;` is accepted.
- The token bucket never moves time backwards: an out-of-order reading refills nothing and a
  later reading is credited only once.
- The per-account bucket (step 4) reads the monotonic clock after the authenticator's await.
"""

import pytest

from limnopulse_api.domain.ingestion import MAX_INGEST_BODY_BYTES
from limnopulse_api.services.ingress_rate_limit import TokenBucketRateLimiter
from tests.api.test_ingest_v2_route import Route, encode, headers, payload, post
from tests.unit.test_ingress_service import BEARER, SOURCE, Ingress


@pytest.fixture
def route() -> Route:
    return Route()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value",
    ["1" * 5_000, "9" * 4_301, "0" * 5_000 + str(MAX_INGEST_BODY_BYTES + 1)],
    ids=["5000_digits", "4301_nines", "zero_padded_over_limit"],
)
async def test_huge_content_length_is_413_not_500(route: Route, value: str) -> None:
    await route.seed()

    response = await post(route.app, encode(payload()), content_length=value)

    assert (response.status, response.json()) == (413, {"detail": "request body too large"})
    assert response.bytes_read == 0


@pytest.mark.asyncio
async def test_leading_zeros_do_not_hide_a_length_within_the_limit(route: Route) -> None:
    await route.seed()
    body = encode(payload())

    response = await post(route.app, body, content_length="0" * 5_000 + str(len(body)))

    assert response.status == 202


@pytest.mark.asyncio
@pytest.mark.parametrize("content_type", ["application/json;", "application/json; charset=utf-8;"])
async def test_empty_content_type_parameters_are_accepted(route: Route, content_type: str) -> None:
    await route.seed()

    response = await post(
        route.app, encode(payload()), header_map=headers(content_type=content_type)
    )

    assert response.status == 202


def test_an_out_of_order_reading_never_moves_the_bucket_back() -> None:
    limiter = TokenBucketRateLimiter(capacity=1, refill_per_second=1.0, max_keys=10)

    assert limiter.try_acquire("a", now=100.0) is None
    assert limiter.try_acquire("a", now=99.5) is not None
    assert limiter.try_acquire("a", now=100.5) is not None  # only 0.5 s since the token
    assert limiter.try_acquire("a", now=101.0) is None


@pytest.mark.asyncio
async def test_account_bucket_reads_the_clock_after_authentication() -> None:
    ingress = Ingress()
    await ingress.seed()
    ingress.mono = 50.0
    original = ingress.store.get_ingest_credential

    def slow_read(ingest_credential_id: str):  # the store read takes 2.5 s of monotonic time
        ingress.mono += 2.5
        return original(ingest_credential_id)

    ingress.store.get_ingest_credential = slow_read  # type: ignore[method-assign]

    assert await ingress.service.authenticate(BEARER) == SOURCE

    (preauth,) = ingress.calls_of("preauth.try_acquire")
    (account,) = ingress.calls_of("account.try_acquire")
    assert preauth["now"] == 50.0
    assert account["now"] == 52.5
