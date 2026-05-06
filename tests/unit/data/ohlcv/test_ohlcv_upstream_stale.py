"""Tests for the OHLCV upstream-staleness guard (ALM-2697).

Covers:

- The router refuses to cache or return a Binance-style response whose
  newest candle is far behind wall-clock for the requested timeframe.
- With multiple upstreams, a stale primary causes failover to the next
  provider in the chain.
- A previously-cached snapshot whose youngest candle is now stale gets
  evicted on read and the upstream is re-queried.
- End-to-end: when the RSI calculator pulls OHLCV via the router and the
  primary upstream is dead, the indicator output advances iteration over
  iteration because the failover fed it fresh data — i.e. the
  ``prev_rsi == current_rsi`` freeze observed in the deployment is gone.

Mirrors the test layout of ``tests/unit/data/ohlcv/`` and uses the same
candle / mock-provider patterns the rest of the OHLCV suite uses.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.data.indicators.rsi import RSICalculator
from almanak.framework.data.interfaces import DataSourceUnavailable, OHLCVCandle
from almanak.framework.data.models import (
    DataClassification,
    DataEnvelope,
    DataMeta,
)
from almanak.framework.data.ohlcv.ohlcv_router import (
    OHLCVRouter,
    _is_upstream_stale,
    _staleness_budget,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _candle(ts: datetime, close: float = 100.0) -> OHLCVCandle:
    return OHLCVCandle(
        timestamp=ts,
        open=Decimal(str(close - 1)),
        high=Decimal(str(close + 1)),
        low=Decimal(str(close - 2)),
        close=Decimal(str(close)),
        volume=Decimal("1000"),
    )


def _series(*, count: int, end: datetime, step: timedelta, base_close: float = 100.0) -> list[OHLCVCandle]:
    """Build a sorted candle series of ``count`` rows ending at ``end``.

    Each candle is ``step`` newer than the previous one. Closes follow a
    saw-tooth pattern around ``base_close`` so RSI computed over the
    series is well-defined and non-degenerate (i.e. avg_loss > 0, so the
    RSI = 100 fast-path doesn't fire).
    """
    # +2 / -1 sawtooth: gains average ~+1, losses average ~-0.5 -> RSI
    # lands well inside (0, 100), and nudging base_close shifts the whole
    # series by a constant so the first-difference vector is unchanged
    # (RSI invariant under additive shift). Production fixtures use a
    # different *fresh* series each iteration, so we vary the offset
    # pattern instead by passing distinct ``base_close`` values; see
    # the regression test for how we exploit that.
    pattern = [(2.0 if i % 2 == 0 else -1.0) for i in range(count)]
    closes: list[float] = []
    running = base_close
    for delta in pattern:
        running += delta
        closes.append(running)
    return [_candle(end - step * (count - 1 - i), close=closes[i]) for i in range(count)]


def _envelope(candles: list[OHLCVCandle], source: str) -> DataEnvelope[list[OHLCVCandle]]:
    meta = DataMeta(
        source=source,
        observed_at=datetime.now(UTC),
        finality="off_chain",
        staleness_ms=0,
        latency_ms=10,
        confidence=1.0,
        cache_hit=False,
    )
    return DataEnvelope(value=candles, meta=meta, classification=DataClassification.INFORMATIONAL)


def _provider(name: str, candles: list[OHLCVCandle]) -> MagicMock:
    p = MagicMock()
    p.name = name
    p.fetch.return_value = _envelope(candles, source=name)
    return p


# ---------------------------------------------------------------------------
# Helper unit tests
# ---------------------------------------------------------------------------


class TestStalenessHelpers:
    """Direct tests of ``_is_upstream_stale`` and ``_staleness_budget``."""

    def test_budget_scales_with_timeframe(self):
        # 5m -> 2 * 300s = 600s, but the 300s floor doesn't bind here
        assert _staleness_budget("5m") == timedelta(seconds=600)
        assert _staleness_budget("1h") == timedelta(seconds=7200)
        assert _staleness_budget("1d") == timedelta(seconds=172800)

    def test_budget_floor_applied_for_1m(self):
        # 1m -> 2 * 60s = 120s, raised to the 5-minute floor
        assert _staleness_budget("1m") == timedelta(seconds=300)

    def test_unknown_timeframe_falls_back_to_1h(self):
        assert _staleness_budget("nonsense") == timedelta(seconds=7200)

    def test_fresh_response_not_stale(self):
        now = datetime.now(UTC)
        candles = _series(count=10, end=now - timedelta(minutes=2), step=timedelta(minutes=5))
        is_stale, lag = _is_upstream_stale(candles, "5m", now)
        assert is_stale is False
        assert lag is not None
        assert lag.total_seconds() < 600

    def test_stale_response_detected(self):
        now = datetime.now(UTC)
        # Youngest candle 6h behind wall-clock -- way over the 5m * 2 budget
        candles = _series(count=10, end=now - timedelta(hours=6), step=timedelta(minutes=5))
        is_stale, lag = _is_upstream_stale(candles, "5m", now)
        assert is_stale is True
        assert lag is not None
        assert lag.total_seconds() > 600

    def test_empty_response_not_flagged(self):
        is_stale, lag = _is_upstream_stale([], "5m", datetime.now(UTC))
        assert is_stale is False
        assert lag is None

    def test_unsorted_input_uses_max_timestamp(self):
        """Stale check must use the youngest timestamp regardless of order."""
        now = datetime.now(UTC)
        recent = _candle(now - timedelta(minutes=2), close=100.0)
        old = _candle(now - timedelta(days=10), close=99.0)
        # Order with the old candle last
        is_stale, _ = _is_upstream_stale([recent, old], "5m", now)
        assert is_stale is False


# ---------------------------------------------------------------------------
# Router-level: stale Binance response is NOT cached or returned
# ---------------------------------------------------------------------------


class TestRouterRejectsStaleUpstream:
    """The router must refuse to cache or return a stalled CEX response."""

    def test_stale_binance_for_polusdt_does_not_poison_cache(self, tmp_path):
        """ALM-2697 production case.

        Simulates Binance answering a ``MATICUSDT`` (post-rebrand dead
        ticker) request with klines that are 30 hours behind wall-clock.
        Expectation: router treats the response as a provider miss, does
        NOT write it to the disk cache, and surfaces an error if no
        other provider is available.
        """
        router = OHLCVRouter(disk_cache_dir=tmp_path, default_chain="polygon")
        now = datetime.now(UTC)
        stale = _series(
            count=34,
            end=now - timedelta(hours=30),
            step=timedelta(minutes=5),
        )
        binance = _provider("binance", stale)
        router.register_provider(binance)

        with pytest.raises(DataSourceUnavailable) as exc_info:
            router.get_ohlcv("WMATIC/USDT", chain="polygon", timeframe="5m", limit=34)
        assert "stale" in exc_info.value.reason.lower()

        # Crucially, no disk-cache file got written. If the bug regressed,
        # the cache would now contain the stale bag and every subsequent
        # call would be served from disk.
        cached = router._disk_cache.get("WMATIC:USDT:polygon:5m:34:auto")
        assert cached is None

    def test_router_fails_over_to_next_cex_when_primary_is_stale(self, tmp_path):
        """Two upstreams, the first returns stale data, the second is
        healthy. Router must fall through to the second."""
        router = OHLCVRouter(disk_cache_dir=tmp_path, default_chain="polygon")
        now = datetime.now(UTC)

        # Binance returns 30h-old klines for MATICUSDT (the production bug)
        binance = _provider(
            "binance",
            _series(count=34, end=now - timedelta(hours=30), step=timedelta(minutes=5)),
        )
        # CoinGecko has fresh data
        coingecko = _provider(
            "coingecko",
            _series(count=34, end=now - timedelta(minutes=2), step=timedelta(minutes=5), base_close=200.0),
        )
        router.register_provider(binance)
        router.register_provider(coingecko)

        envelope = router.get_ohlcv("WMATIC/USDT", chain="polygon", timeframe="5m", limit=34)

        # cex_primary chain is [binance, coingecko, defillama] — Binance is
        # tried first, fails the staleness gate, router falls through.
        assert envelope.meta.source == "coingecko"
        assert binance.fetch.call_count == 1
        assert coingecko.fetch.call_count == 1
        # Youngest candle anchors freshness — must be within the budget.
        youngest_lag = now - max(c.timestamp for c in envelope.value)
        assert youngest_lag < _staleness_budget("5m")

    def test_router_logs_staleness_warning(self, tmp_path, caplog):
        router = OHLCVRouter(disk_cache_dir=tmp_path, default_chain="polygon")
        now = datetime.now(UTC)
        stale = _series(count=10, end=now - timedelta(hours=6), step=timedelta(minutes=5))
        coingecko = _provider(
            "coingecko",
            _series(count=10, end=now - timedelta(minutes=2), step=timedelta(minutes=5), base_close=50.0),
        )
        binance = _provider("binance", stale)
        router.register_provider(binance)
        router.register_provider(coingecko)

        with caplog.at_level("WARNING", logger="almanak.framework.data.ohlcv.ohlcv_router"):
            router.get_ohlcv("WMATIC/USDT", chain="polygon", timeframe="5m", limit=10)

        # The actionable diagnostic that would have caught ALM-2697 in
        # deployment if it had existed at the time.
        assert "ohlcv_upstream_stale" in caplog.text
        assert "binance" in caplog.text
        assert "WMATIC/USDT" in caplog.text


# ---------------------------------------------------------------------------
# Disk-cache poisoning guard
# ---------------------------------------------------------------------------


class TestDiskCachePoisoningGuard:
    """A previously-cached stale snapshot must not be re-served."""

    def test_stale_cache_evicted_and_upstream_refetched(self, tmp_path):
        router = OHLCVRouter(disk_cache_dir=tmp_path, default_chain="polygon")
        now = datetime.now(UTC)

        # Pre-poison: write candles whose youngest is 4 days behind now.
        # This mirrors what would happen if a previous run cached a stale
        # Binance response under the pre-ALM-2697 router.
        poisoned = _series(count=20, end=now - timedelta(days=4), step=timedelta(minutes=5))
        router._disk_cache.put("WMATIC:USDT:polygon:5m:20:auto", poisoned)
        # Confirm pre-condition: cache file exists.
        assert router._disk_cache.get("WMATIC:USDT:polygon:5m:20:auto") is not None

        # A healthy upstream is registered.
        fresh = _series(count=20, end=now - timedelta(minutes=2), step=timedelta(minutes=5), base_close=42.0)
        coingecko = _provider("coingecko", fresh)
        binance = _provider("binance", fresh)
        router.register_provider(binance)
        router.register_provider(coingecko)

        envelope = router.get_ohlcv("WMATIC/USDT", chain="polygon", timeframe="5m", limit=20)

        # Cache was evicted and upstream was queried.
        assert envelope.meta.cache_hit is False
        assert envelope.meta.source == "binance"
        # Cache file is gone (evict() called) — _disk_cache.get() returns None
        # because the on-disk row was unlinked. The post-fetch path *may*
        # have re-populated the cache with the new finalized rows; that's
        # fine — what matters is the poisoned bag is not served.
        post = router._disk_cache.get("WMATIC:USDT:polygon:5m:20:auto")
        if post is not None:
            # If repopulated, every cached row's youngest must be fresh.
            youngest = max(c.timestamp for c in post)
            # Will only contain finalized rows (>24h); but in this fixture
            # the entire response is <24h so nothing is finalized -> not
            # written. This branch is defensive only.
            assert (now - youngest) <= _staleness_budget("5m")


# ---------------------------------------------------------------------------
# End-to-end regression: RSI advances when failover delivers fresh data
# ---------------------------------------------------------------------------


class _RouterBackedOHLCVProvider:
    """Adapt an ``OHLCVRouter`` to the OHLCVProvider protocol expected by
    ``RSICalculator``. The router is sync; ``get_ohlcv`` is awaited by
    the calculator, so we wrap it in an async shim. This mirrors the way
    the production framework wires the router into the gateway-backed
    OHLCV provider chain.
    """

    def __init__(self, router: OHLCVRouter, chain: str) -> None:
        self._router = router
        self._chain = chain

    async def get_ohlcv(
        self,
        token: str,
        quote: str = "USD",
        timeframe: str = "1h",
        limit: int = 100,
    ) -> list[OHLCVCandle]:
        envelope = self._router.get_ohlcv(
            token=token,
            chain=self._chain,
            timeframe=timeframe,
            limit=limit,
            quote=quote,
        )
        return envelope.value


class TestRSIRegressionWithFailover:
    """ALM-2697 regression at the indicator level.

    Pre-fix: a stale Binance response froze the close-price series so
    consecutive RSI calls returned the byte-identical value.

    Post-fix: the router fails over to the second CEX in the chain, and
    advancing fresh data anchors the close-price series, so consecutive
    RSI calls return *different* values when the underlying market moves.
    """

    def test_rsi_advances_when_primary_is_stale_and_secondary_is_fresh(self, tmp_path):
        router = OHLCVRouter(disk_cache_dir=tmp_path, default_chain="polygon")
        now = datetime.now(UTC)

        # Binance: dead — returns 30h-old klines (the production bug)
        binance_stale = _series(count=34, end=now - timedelta(hours=30), step=timedelta(minutes=5), base_close=10.0)
        binance = _provider("binance", binance_stale)

        # CoinGecko: live. Two iterations return *distinct* series — the
        # second iteration's tail diverges so the close-price first
        # differences (which RSI integrates) are different. This mirrors
        # production where each iteration sees a new just-closed candle
        # plus shifted earlier closes after upstream backfills a gap.
        cg_iter1 = _series(count=34, end=now - timedelta(minutes=2), step=timedelta(minutes=5), base_close=10.0)
        # Replace the last 5 closes with a downward-skewed pattern so RSI
        # registers losses on iter2 it didn't see on iter1.
        cg_iter2 = list(cg_iter1)
        for offset in range(1, 6):
            old = cg_iter2[-offset]
            cg_iter2[-offset] = OHLCVCandle(
                timestamp=old.timestamp,
                open=old.open,
                high=old.high,
                low=old.low,
                close=old.close - Decimal("3"),
                volume=old.volume,
            )

        coingecko = MagicMock()
        coingecko.name = "coingecko"
        coingecko.fetch.side_effect = [
            _envelope(cg_iter1, source="coingecko"),
            _envelope(cg_iter2, source="coingecko"),
        ]

        router.register_provider(binance)
        router.register_provider(coingecko)

        # Adapt router -> async OHLCVProvider for the calculator
        provider = _RouterBackedOHLCVProvider(router, chain="polygon")
        calculator = RSICalculator(ohlcv_provider=provider)

        async def _drive() -> tuple[float, float]:
            # Two iterations, simulating the strategy's deployment loop
            r1 = await calculator.calculate_rsi("WMATIC", period=14, timeframe="5m")
            r2 = await calculator.calculate_rsi("WMATIC", period=14, timeframe="5m")
            return r1, r2

        rsi1, rsi2 = asyncio.run(_drive())

        # Pre-fix: rsi1 == rsi2 because the stale Binance disk cache
        # served the same bag forever. Post-fix: failover brings fresh
        # data each iteration and the RSI series advances.
        assert rsi1 != rsi2, (
            f"RSI froze across iterations (rsi1={rsi1}, rsi2={rsi2}); "
            "the staleness guard / failover regressed and the deployment "
            "would once again sit in HOLD with prev_rsi == current_rsi."
        )
        # Both RSI values are well-formed scalars in [0, 100]
        assert 0.0 <= rsi1 <= 100.0
        assert 0.0 <= rsi2 <= 100.0

        # Binance was tried each iteration but failed staleness; CoinGecko
        # served the fresh data. The cache never poisoned the indicator
        # input on the second tick.
        assert binance.fetch.call_count == 2
        assert coingecko.fetch.call_count == 2
