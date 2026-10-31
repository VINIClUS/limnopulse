"""In-process ingress rate limiters (P3 D13).

`TokenBucketRateLimiter` implements the `IngressRateLimiter` port: one token bucket per key,
refilled continuously, with at most `max_keys` buckets kept in LRU order. The caller passes
`now` from an injected monotonic clock. There is no distributed cache (ADR-019), so with N API
replicas the bound is N x the limit.

`ingress_rate_limiters(settings)` builds the three limiters of the route order with the frozen
Settings formulas: the process-global pre-auth bucket (single key `"*"`) and the per-`ic_`
pre-auth bucket have capacity `max(1, per_minute // 10)`, and the per-account bucket has
capacity `ingest_rate_limit_burst`; every bucket refills at `per_minute / 60` per second.
"""

from collections import OrderedDict
from typing import NamedTuple

from limnopulse_api.core.config import Settings
from limnopulse_api.domain.ingestion import bucket_refill_per_second, preauth_bucket_capacity

PREAUTH_GLOBAL_KEY = "*"
_GLOBAL_KEYS = 1


class TokenBucketRateLimiter:
    def __init__(self, *, capacity: int, refill_per_second: float, max_keys: int) -> None:
        if capacity < 1:
            raise ValueError("capacity must be at least 1")
        if not refill_per_second > 0:
            raise ValueError("refill_per_second must be positive")
        if max_keys < 1:
            raise ValueError("max_keys must be at least 1")
        self.capacity = capacity
        self.refill_per_second = refill_per_second
        self.max_keys = max_keys
        self._buckets: OrderedDict[str, tuple[float, float]] = OrderedDict()

    def __len__(self) -> int:
        return len(self._buckets)

    def try_acquire(self, key: str, *, now: float) -> float | None:
        """`None` when a token was taken, else the seconds until the next token."""
        bucket = self._buckets.get(key)
        if bucket is None:
            while len(self._buckets) >= self.max_keys:
                self._buckets.popitem(last=False)
            tokens = float(self.capacity)
        else:
            self._buckets.move_to_end(key)
            stored, last = bucket
            # A reading older than the bucket (callers racing across an await) refills nothing
            # and never moves the bucket back, so elapsed time is credited only once.
            elapsed = max(0.0, now - last)
            tokens = min(float(self.capacity), stored + elapsed * self.refill_per_second)
            now = max(now, last)
        if tokens >= 1:
            self._buckets[key] = (tokens - 1, now)
            return None
        self._buckets[key] = (tokens, now)
        return (1 - tokens) / self.refill_per_second


class IngressRateLimiters(NamedTuple):
    preauth_global: TokenBucketRateLimiter
    preauth: TokenBucketRateLimiter
    account: TokenBucketRateLimiter


def ingress_rate_limiters(settings: Settings) -> IngressRateLimiters:
    global_per_minute = settings.ingest_preauth_global_per_minute
    preauth_per_minute = settings.ingest_preauth_rate_limit_per_minute
    account_per_minute = settings.ingest_rate_limit_per_minute
    max_keys = settings.ingest_rate_limiter_max_keys
    return IngressRateLimiters(
        preauth_global=TokenBucketRateLimiter(
            capacity=preauth_bucket_capacity(global_per_minute),
            refill_per_second=bucket_refill_per_second(global_per_minute),
            max_keys=_GLOBAL_KEYS,
        ),
        preauth=TokenBucketRateLimiter(
            capacity=preauth_bucket_capacity(preauth_per_minute),
            refill_per_second=bucket_refill_per_second(preauth_per_minute),
            max_keys=max_keys,
        ),
        account=TokenBucketRateLimiter(
            capacity=settings.ingest_rate_limit_burst,
            refill_per_second=bucket_refill_per_second(account_per_minute),
            max_keys=max_keys,
        ),
    )
