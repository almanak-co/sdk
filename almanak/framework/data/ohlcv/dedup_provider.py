"""Per-strategy in-process OHLCV deduper -- VIB-3783 stop-gap.

A thin wrapper around an inner ``OHLCVProvider`` that coalesces requests for the
same ``(token, quote, timeframe)`` within a single iteration. When indicators on
the same token (e.g. MACD with ``limit=85`` and ATR with ``limit=34``) run in
the same ``decide()`` call, this saves the second upstream fetch by tail-slicing
the previously-cached series.

Every OHLCV provider in the framework returns the most recent ``N`` candles, so
``cached[-requested_limit:]`` is bit-for-bit identical to a fresh fetch with the
smaller limit.

This is a temporary stop-gap. The full cache redesign (key on
``(token, quote, timeframe)`` everywhere, ditch the ``limit``-keyed layers) is
tracked in VIB-3783. Do not extend this wrapper -- redesign instead.

Lifecycle: per ``IntentStrategy`` instance. The cache is cleared at the start of
each iteration via ``IntentStrategy.create_market_snapshot()`` so subsequent
iterations get fresh data, matching the per-iteration semantics of the existing
``_macd_cache`` / ``_atr_cache`` dicts on ``MarketSnapshot``.
"""

from __future__ import annotations

import logging

from almanak.framework.data.interfaces import OHLCVCandle, OHLCVProvider

logger = logging.getLogger(__name__)


class DedupingOHLCVProvider(OHLCVProvider):
    """Coalesce OHLCV fetches for the same key within an iteration. See VIB-3783."""

    def __init__(self, inner: OHLCVProvider) -> None:
        """Wrap an inner OHLCV provider with a per-instance dedup cache.

        Args:
            inner: The underlying provider that actually fetches candles. The
                wrapper presents the same ``OHLCVProvider`` protocol surface.
        """
        self._inner = inner
        # Cache key = (token, quote, timeframe). Values are the full candle
        # series from the largest fetch seen so far for that key.
        self._cache: dict[tuple[str, str, str], list[OHLCVCandle]] = {}

    @property
    def supported_timeframes(self) -> list[str]:
        """Pass through the inner provider's supported timeframes."""
        return self._inner.supported_timeframes

    async def get_ohlcv(
        self,
        token: str,
        quote: str = "USD",
        timeframe: str = "1h",
        limit: int = 100,
    ) -> list[OHLCVCandle]:
        """Return ``limit`` most-recent candles, fetching upstream only when needed.

        On a cache hit with ``len(cached) >= limit``, return ``cached[-limit:]``.
        Otherwise call the inner provider, store its result (replacing any
        smaller cached entry), and return it.

        Concurrency note: if two coroutines race for the same key, both may go
        upstream. That matches the existing pre-wrapper behaviour and avoids the
        complexity of a per-key ``asyncio.Lock``. The bug we are patching is
        sequential MACD-then-ATR within a single ``decide()``, not parallel
        coroutines, so the simpler design is correct here.

        Args:
            token: Token symbol (e.g. ``"cbBTC"``, ``"WETH"``).
            quote: Quote currency (default ``"USD"``).
            timeframe: Candle timeframe (e.g. ``"1h"``).
            limit: Number of most-recent candles to return.

        Returns:
            List of ``OHLCVCandle`` sorted oldest-first, length ``<= limit``.

        Raises:
            ValueError: If ``limit`` is not a positive integer. ``cached[-0:]``
                returns the entire series in Python, and negative slices yield
                arbitrary windows -- both are wrong here, so reject up front.
        """
        if limit <= 0:
            raise ValueError(f"limit must be a positive integer, got {limit}")
        key = (token, quote, timeframe)
        cached = self._cache.get(key)
        if cached is not None and len(cached) >= limit:
            logger.debug(
                "OHLCV dedup HIT key=%s requested=%d cached=%d",
                key,
                limit,
                len(cached),
            )
            return cached[-limit:]

        logger.debug(
            "OHLCV dedup MISS key=%s requested=%d cached=%s -> upstream",
            key,
            limit,
            "none" if cached is None else str(len(cached)),
        )
        candles = await self._inner.get_ohlcv(
            token=token,
            quote=quote,
            timeframe=timeframe,
            limit=limit,
        )
        # Only replace the cached entry if the new fetch is at least as large
        # as what we already have -- never shrink the cache on a smaller fetch
        # that happens to miss the prior entry.
        if cached is None or len(candles) >= len(cached):
            self._cache[key] = candles
        # Return a tail-slice copy so callers cannot mutate the cached list,
        # and so the result length is bounded by ``limit`` even if an upstream
        # provider over-fetches.
        return candles[-limit:]

    def clear(self) -> None:
        """Drop all cached series. Called per iteration to force fresh fetches."""
        self._cache.clear()


__all__ = ["DedupingOHLCVProvider"]
