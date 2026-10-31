"""P3-07: `TokenBucketRateLimiter` (D13) passes the P3-00 limiter kit, keeps a bounded number
of keys with LRU eviction, and the three in-process limiters follow the frozen Settings formulas:
pre-auth capacity `max(1, per_minute // 10)` refilled at `per_minute / 60` per second, and the
per-account bucket `burst` refilled at `per_minute / 60` per second."""

from collections.abc import Callable
from typing import Any

import pytest

from limnopulse_api.core.config import Settings
from limnopulse_api.repositories.ingestion import IngressRateLimiter
from limnopulse_api.services.ingress_rate_limit import (
    PREAUTH_GLOBAL_KEY,
    TokenBucketRateLimiter,
    ingress_rate_limiters,
)
from tests.support.ingestion_contract_kits import RATE_LIMITER_CONTRACT, kit_ids


def _settings(**overrides: Any) -> Settings:
    return Settings(_env_file=None, app_env="test", auth_mode="dev", **overrides)


def _admitted(limiter: Any, key: str, *, attempts: int, now: float = 0.0) -> int:
    return sum(limiter.try_acquire(key, now=now) is None for _ in range(attempts))


@pytest.mark.parametrize("scenario", RATE_LIMITER_CONTRACT, ids=kit_ids(RATE_LIMITER_CONTRACT))
def test_token_bucket_passes_the_p3_00_limiter_kit(scenario: Callable[[Any], None]) -> None:
    scenario(TokenBucketRateLimiter)


def test_token_bucket_implements_the_port() -> None:
    limiter = TokenBucketRateLimiter(capacity=1, refill_per_second=1.0, max_keys=1)

    assert isinstance(limiter, IngressRateLimiter)


def test_key_count_stays_bounded_under_many_distinct_keys() -> None:
    limiter = TokenBucketRateLimiter(capacity=2, refill_per_second=1.0, max_keys=100)

    for index in range(10_000):
        assert limiter.try_acquire(f"ic_{index:032x}", now=float(index)) is None
        assert len(limiter) <= 100

    assert len(limiter) == 100


def test_retry_after_is_the_time_until_the_next_token() -> None:
    limiter = TokenBucketRateLimiter(capacity=2, refill_per_second=0.5, max_keys=10)

    assert _admitted(limiter, "a", attempts=2) == 2
    assert limiter.try_acquire("a", now=0.0) == pytest.approx(2.0)
    assert limiter.try_acquire("a", now=1.5) == pytest.approx(0.5)
    assert limiter.try_acquire("a", now=2.0) is None


def test_refill_never_exceeds_the_capacity() -> None:
    limiter = TokenBucketRateLimiter(capacity=3, refill_per_second=10.0, max_keys=10)

    assert _admitted(limiter, "a", attempts=3) == 3
    assert _admitted(limiter, "a", attempts=10, now=3_600.0) == 3


def test_a_denied_attempt_consumes_no_token() -> None:
    limiter = TokenBucketRateLimiter(capacity=1, refill_per_second=1.0, max_keys=10)

    assert limiter.try_acquire("a", now=0.0) is None
    for step in range(9):
        assert limiter.try_acquire("a", now=step / 10) is not None
    assert limiter.try_acquire("a", now=1.0) is None


@pytest.mark.parametrize(
    "kwargs",
    [
        {"capacity": 0, "refill_per_second": 1.0, "max_keys": 1},
        {"capacity": 1, "refill_per_second": 0.0, "max_keys": 1},
        {"capacity": 1, "refill_per_second": -1.0, "max_keys": 1},
        {"capacity": 1, "refill_per_second": 1.0, "max_keys": 0},
    ],
)
def test_invalid_bucket_parameters_are_rejected(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        TokenBucketRateLimiter(**kwargs)


def test_settings_limiters_follow_the_frozen_formulas() -> None:
    limiters = ingress_rate_limiters(
        _settings(
            ingest_preauth_global_per_minute=600,
            ingest_preauth_rate_limit_per_minute=60,
            ingest_rate_limit_per_minute=120,
            ingest_rate_limit_burst=20,
        )
    )

    # Global pre-auth: capacity 600 // 10 = 60, refill 10 per second, single key "*".
    assert PREAUTH_GLOBAL_KEY == "*"
    assert _admitted(limiters.preauth_global, PREAUTH_GLOBAL_KEY, attempts=100) == 60
    assert _admitted(limiters.preauth_global, PREAUTH_GLOBAL_KEY, attempts=100, now=1.0) == 10
    # Per-`ic_` pre-auth: capacity 60 // 10 = 6, refill 1 per second.
    assert _admitted(limiters.preauth, "ic_" + "a" * 32, attempts=50) == 6
    assert _admitted(limiters.preauth, "ic_" + "a" * 32, attempts=50, now=1.0) == 1
    # Per account: capacity = burst 20, refill 120 / 60 = 2 per second.
    assert _admitted(limiters.account, "ia_" + "1" * 32, attempts=50) == 20
    assert _admitted(limiters.account, "ia_" + "1" * 32, attempts=50, now=1.0) == 2


def test_tiny_preauth_limits_keep_a_capacity_of_one() -> None:
    limiters = ingress_rate_limiters(
        _settings(ingest_preauth_global_per_minute=5, ingest_preauth_rate_limit_per_minute=1)
    )

    assert _admitted(limiters.preauth_global, PREAUTH_GLOBAL_KEY, attempts=10) == 1
    assert _admitted(limiters.preauth, "ic_" + "a" * 32, attempts=10) == 1


def test_settings_limiters_bound_their_keys() -> None:
    limiters = ingress_rate_limiters(_settings(ingest_rate_limiter_max_keys=100))

    for index in range(500):
        limiters.preauth.try_acquire(f"ic_{index:032x}", now=0.0)
        limiters.account.try_acquire(f"ia_{index:032x}", now=0.0)

    assert len(limiters.preauth) <= 100
    assert len(limiters.account) <= 100
