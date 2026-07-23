"""Process-wide shared rate limiting for SDK-side data providers.

This module is the single SDK-process-side home for client-side rate
limiting (ALM-2943 phase-3 consolidation). Providers that talk to the same
upstream API must draw from the same named bucket so their combined call
rate cannot exceed the provider's cap — previously each module carried its
own private ``_TokenBucket`` copy, so N call sites could emit N times the
configured rate against one upstream.

Usage:
    from almanak.framework.data.ratelimit import get_bucket

    limiter = get_bucket("defillama", rate=10, period=1.0)
    if not limiter.acquire():
        raise DataSourceUnavailable(...)

Semantics:
    - ``get_bucket(name, ...)`` returns one shared, thread-safe bucket per
      ``name`` for the lifetime of the process.
    - If two call sites configure different rates for the same name, the
      STRICTER rate wins (a bucket only ever tightens, never loosens).
    - ``TokenBucket`` remains directly instantiable for callers that
      genuinely need private state (e.g. tests).

Scope note: gateway-side limiters (``almanak/gateway/...``, e.g.
``_TokenBucket``/``_ObservableTokenBucket`` in
``almanak/gateway/data/pool_history/_base.py``) budget egress inside the
gateway process and are intentionally NOT part of this registry.
"""

from __future__ import annotations

import logging
import threading
import time

logger = logging.getLogger(__name__)

__all__ = ["TokenBucket", "get_bucket", "reset_buckets"]


class TokenBucket:
    """Thread-safe token bucket rate limiter.

    Allows ``rate`` requests per ``period`` seconds. Tokens are refilled
    lazily on each call to :meth:`acquire`.
    """

    def __init__(self, rate: int = 10, period: float = 1.0) -> None:
        if rate <= 0:
            raise ValueError("rate must be positive")
        if period <= 0:
            raise ValueError("period must be positive")
        self._rate = rate
        self._period = period
        self._tokens = float(rate)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    @property
    def rate(self) -> int:
        """Configured requests per period."""
        return self._rate

    @property
    def period(self) -> float:
        """Configured period in seconds."""
        return self._period

    def acquire(self) -> bool:
        """Try to acquire a token. Returns True if allowed, False if rate limited."""
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(float(self._rate), self._tokens + elapsed * (self._rate / self._period))
            self._last_refill = now

            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False

    def tighten(self, rate: int, period: float = 1.0) -> None:
        """Adopt ``rate``/``period`` only if it is stricter than the current config.

        Used by the registry so that when multiple call sites configure the
        same named bucket with different rates, the strictest cap wins.
        Never loosens an existing bucket.
        """
        if rate <= 0:
            raise ValueError("rate must be positive")
        if period <= 0:
            raise ValueError("period must be positive")
        with self._lock:
            if rate / period < self._rate / self._period:
                self._rate = rate
                self._period = period
                self._tokens = min(self._tokens, float(rate))


_BUCKETS: dict[str, TokenBucket] = {}
_REGISTRY_LOCK = threading.Lock()


def get_bucket(name: str, rate: int = 10, period: float = 1.0) -> TokenBucket:
    """Return the process-wide shared bucket for upstream provider ``name``.

    The first caller creates the bucket; later callers share it. If a later
    caller requests a stricter rate than the bucket currently enforces, the
    bucket tightens to that rate (it never loosens), so the strictest
    configured cap always wins.

    Args:
        name: Upstream provider key (e.g. ``"defillama"``). All call sites
            hitting the same upstream must use the same name.
        rate: Requests allowed per ``period`` seconds.
        period: Bucket period in seconds.

    Returns:
        The shared, thread-safe :class:`TokenBucket` for ``name``.
    """
    if rate <= 0 or period <= 0:
        raise ValueError(f"get_bucket({name!r}) requires positive rate and period, got rate={rate}, period={period}")
    with _REGISTRY_LOCK:
        bucket = _BUCKETS.get(name)
        if bucket is None:
            bucket = TokenBucket(rate=rate, period=period)
            _BUCKETS[name] = bucket
        elif rate / period < bucket.rate / bucket.period:
            logger.info(
                "ratelimit bucket %r tightened: %d/%.3fs -> %d/%.3fs",
                name,
                bucket.rate,
                bucket.period,
                rate,
                period,
            )
            bucket.tighten(rate, period)
        return bucket


def reset_buckets() -> None:
    """Drop all shared buckets. Test isolation helper — not for production use."""
    with _REGISTRY_LOCK:
        _BUCKETS.clear()
