"""Tests for the DEX quiet-pool OHLCV forward-fill + relaxed staleness budget (VIB-4875).

Background: on-chain DEX sources (GeckoTerminal) only emit a candle when a swap
occurs. For a genuinely quiet pool the newest real candle lags wall-clock, which
the ALM-2697 staleness guard would reject as a dead feed — stranding the strategy
in ``DATA_ERROR`` even though the pool is alive and GeckoTerminal has data. The
``ethereum-nvda-activity-hourly`` deployment hit exactly this (NVDAON/USDC 1h).

The fix is two-fold and DEX-scoped:

- **Trailing-edge forward-fill**: synthesize flat candles (carry last close,
  zero volume) from the youngest real candle up to the current wall-clock bucket,
  so indicators get a continuous, current series and the staleness guard passes.
- **Relaxed dead-pool budget**: DEX sources get ``_DEX_STALE_TIMEFRAME_MULTIPLE``
  timeframes of slack; beyond that the pool is presumed dead and is NOT
  forward-filled (the guard then correctly rejects it).

CEX sources (binance/coingecko) are deliberately excluded from both — there a
stale response means a dead/rebranded ticker (the ALM-2697 case) and must still
fail the strict budget.

Mirrors the candle / mock-provider patterns of ``test_ohlcv_upstream_stale.py``.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable, OHLCVCandle
from almanak.framework.data.models import (
    DataClassification,
    DataEnvelope,
    DataMeta,
)
from almanak.framework.data.ohlcv.ohlcv_router import (
    _DEX_FORWARD_FILL_CONFIDENCE,
    _DEX_STALE_TIMEFRAME_MULTIPLE,
    OHLCVRouter,
    _forward_fill_dex_candles,
    _staleness_budget,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _candle(ts: datetime, close: float = 100.0, volume: float = 1000.0) -> OHLCVCandle:
    return OHLCVCandle(
        timestamp=ts,
        open=Decimal(str(close - 1)),
        high=Decimal(str(close + 1)),
        low=Decimal(str(close - 2)),
        close=Decimal(str(close)),
        volume=Decimal(str(volume)),
    )


def _hourly_series_ending(end: datetime, count: int = 5) -> list[OHLCVCandle]:
    """Ascending 1h candles ending at ``end`` (inclusive), distinct closes."""
    return [_candle(end - timedelta(hours=count - 1 - i), close=100.0 + i) for i in range(count)]


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


def _daily_series_ending(end: datetime, count: int = 5) -> list[OHLCVCandle]:
    """Ascending 1d candles ending at ``end`` (inclusive), distinct closes."""
    return [_candle(end - timedelta(days=count - 1 - i), close=100.0 + i) for i in range(count)]


def _floor_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def _floor_day(dt: datetime) -> datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


# ---------------------------------------------------------------------------
# Relaxed staleness budget
# ---------------------------------------------------------------------------


class TestDexStalenessBudget:
    def test_dex_budget_is_relaxed_multiple(self):
        # 1h: strict 2x = 7200s; DEX 24x = 86400s
        assert _staleness_budget("1h").total_seconds() == 7200
        assert _staleness_budget("1h", is_dex=True).total_seconds() == 3600 * _DEX_STALE_TIMEFRAME_MULTIPLE

    def test_dex_budget_scales_with_timeframe(self):
        assert _staleness_budget("5m", is_dex=True).total_seconds() == 300 * _DEX_STALE_TIMEFRAME_MULTIPLE
        assert _staleness_budget("1d", is_dex=True).total_seconds() == 86400 * _DEX_STALE_TIMEFRAME_MULTIPLE

    def test_strict_budget_unchanged_when_not_dex(self):
        assert _staleness_budget("1h", is_dex=False) == _staleness_budget("1h")


# ---------------------------------------------------------------------------
# Forward-fill helper
# ---------------------------------------------------------------------------


class TestForwardFillHelper:
    def test_quiet_pool_fills_to_current_bucket(self):
        now = datetime(2026, 5, 28, 6, 20, 0, tzinfo=UTC)
        last_trade = datetime(2026, 5, 28, 3, 0, 0, tzinfo=UTC)
        real = _hourly_series_ending(last_trade, count=3)  # 01:00, 02:00, 03:00
        filled, n_synth = _forward_fill_dex_candles(real, "1h", now)

        # 04:00, 05:00, 06:00 synthesised
        assert n_synth == 3
        assert max(c.timestamp for c in filled) == datetime(2026, 5, 28, 6, 0, 0, tzinfo=UTC)

    def test_synthetic_candles_are_flat_carry_forward_zero_volume(self):
        now = datetime(2026, 5, 28, 6, 20, 0, tzinfo=UTC)
        last_trade = datetime(2026, 5, 28, 3, 0, 0, tzinfo=UTC)
        real = _hourly_series_ending(last_trade, count=3)
        last_close = max(real, key=lambda c: c.timestamp).close
        filled, n_synth = _forward_fill_dex_candles(real, "1h", now)

        synth = filled[len(real):]
        assert len(synth) == n_synth
        for s in synth:
            assert s.open == s.high == s.low == s.close == last_close
            assert s.volume == Decimal(0)

    def test_dead_pool_beyond_horizon_not_filled(self):
        now = datetime(2026, 5, 28, 6, 20, 0, tzinfo=UTC)
        # 30h > 24h dead-pool horizon
        real = [_candle(now - timedelta(hours=30), close=1.0)]
        filled, n_synth = _forward_fill_dex_candles(real, "1h", now)
        assert n_synth == 0
        assert filled == real

    def test_synthetic_count_bounded_by_horizon(self):
        now = datetime(2026, 5, 28, 6, 20, 0, tzinfo=UTC)
        # Just inside the horizon (23h) -> at most _DEX_STALE_TIMEFRAME_MULTIPLE synths
        real = [_candle(_floor_hour(now) - timedelta(hours=23), close=5.0)]
        _, n_synth = _forward_fill_dex_candles(real, "1h", now)
        assert 0 < n_synth <= _DEX_STALE_TIMEFRAME_MULTIPLE

    def test_high_liquidity_current_bucket_present_no_fill(self):
        now = datetime(2026, 5, 28, 6, 20, 0, tzinfo=UTC)
        real = _hourly_series_ending(_floor_hour(now), count=5)  # newest = 06:00
        filled, n_synth = _forward_fill_dex_candles(real, "1h", now)
        assert n_synth == 0
        assert filled == real

    def test_empty_input_no_fill(self):
        assert _forward_fill_dex_candles([], "1h", datetime.now(UTC)) == ([], 0)


# ---------------------------------------------------------------------------
# Router level: the VIB-4875 NVDAON regression
# ---------------------------------------------------------------------------


class TestRouterDexQuietPool:
    """A quiet DEX pool must return a fresh, continuous series — not DATA_ERROR."""

    def test_quiet_geckoterminal_pool_is_forward_filled_and_returned(self, tmp_path):
        router = OHLCVRouter(disk_cache_dir=tmp_path, default_chain="ethereum")
        now = datetime.now(UTC)
        # Last swap 3h ago, then quiet — the NVDAON/USDC deployment scenario.
        last_trade = _floor_hour(now) - timedelta(hours=3)
        real = _hourly_series_ending(last_trade, count=5)
        gecko = _provider("geckoterminal", real)
        router.register_provider(gecko)

        # limit=10 comfortably exceeds 5 real + 3 synthetic, so no trimming.
        envelope = router.get_ohlcv("NVDAON/USDC", chain="ethereum", timeframe="1h", limit=10)

        # The pre-fix bug: this raised DataSourceUnavailable (stale). Post-fix it
        # returns a forward-filled, current series.
        assert envelope.meta.source == "geckoterminal"
        assert envelope.meta.forward_filled is True
        assert envelope.meta.confidence <= _DEX_FORWARD_FILL_CONFIDENCE
        # Newest candle is now within the strict budget (advanced to wall-clock).
        youngest_lag = now - max(c.timestamp for c in envelope.value)
        assert youngest_lag < _staleness_budget("1h")
        # Real candles preserved, synthetics appended.
        assert len(envelope.value) > len(real)

    def test_quiet_pool_logs_forward_fill(self, tmp_path, caplog):
        router = OHLCVRouter(disk_cache_dir=tmp_path, default_chain="ethereum")
        now = datetime.now(UTC)
        real = _hourly_series_ending(_floor_hour(now) - timedelta(hours=3), count=5)
        router.register_provider(_provider("geckoterminal", real))

        with caplog.at_level("INFO", logger="almanak.framework.data.ohlcv.ohlcv_router"):
            router.get_ohlcv("NVDAON/USDC", chain="ethereum", timeframe="1h", limit=5)

        assert "ohlcv_dex_forward_fill" in caplog.text
        assert "geckoterminal" in caplog.text

    def test_dead_geckoterminal_pool_still_rejected(self, tmp_path):
        """Beyond the dead-pool horizon, a DEX source is NOT forward-filled and
        the staleness guard rejects it (no other provider registered)."""
        router = OHLCVRouter(disk_cache_dir=tmp_path, default_chain="ethereum")
        now = datetime.now(UTC)
        # 30h with no trade > 24h DEX horizon.
        dead = _hourly_series_ending(_floor_hour(now) - timedelta(hours=30), count=5)
        router.register_provider(_provider("geckoterminal", dead))

        with pytest.raises(DataSourceUnavailable) as exc_info:
            router.get_ohlcv("NVDAON/USDC", chain="ethereum", timeframe="1h", limit=5)
        assert "stale" in exc_info.value.reason.lower()

    def test_high_liquidity_dex_pool_not_marked_forward_filled(self, tmp_path):
        router = OHLCVRouter(disk_cache_dir=tmp_path, default_chain="ethereum")
        now = datetime.now(UTC)
        fresh = _hourly_series_ending(_floor_hour(now), count=10)  # traded this hour
        router.register_provider(_provider("geckoterminal", fresh))

        envelope = router.get_ohlcv("NVDAON/USDC", chain="ethereum", timeframe="1h", limit=10)

        assert envelope.meta.forward_filled is False
        assert len(envelope.value) == len(fresh)


class TestForwardFillRespectsLimit:
    """Forward-fill must not widen the lookback window past the requested ``limit``."""

    def test_limit_is_respected_after_forward_fill(self, tmp_path):
        router = OHLCVRouter(disk_cache_dir=tmp_path, default_chain="ethereum")
        now = datetime.now(UTC)
        # Pool returns exactly `limit` real candles, then is quiet for 3h. Without
        # trimming the response would be 5 real + 3 synthetic = 8 candles.
        real = _hourly_series_ending(_floor_hour(now) - timedelta(hours=3), count=5)
        router.register_provider(_provider("geckoterminal", real))

        envelope = router.get_ohlcv("NVDAON/USDC", chain="ethereum", timeframe="1h", limit=5)

        assert envelope.meta.forward_filled is True
        # Never more than requested.
        assert len(envelope.value) == 5
        # The kept window is the newest 5 buckets: oldest real candles dropped,
        # synthetic tail (anchored at now) retained so the series stays current.
        youngest_lag = now - max(c.timestamp for c in envelope.value)
        assert youngest_lag < _staleness_budget("1h")
        # 3 synthetic (zero-volume) + 2 surviving real candles.
        assert sum(1 for c in envelope.value if c.volume == 0) == 3

    def test_limit_smaller_than_synthetic_count(self, tmp_path):
        """Degenerate case: limit < number of synthetic buckets. Still capped."""
        router = OHLCVRouter(disk_cache_dir=tmp_path, default_chain="ethereum")
        now = datetime.now(UTC)
        # Quiet for 5h but caller only wants 2 candles.
        real = _hourly_series_ending(_floor_hour(now) - timedelta(hours=5), count=5)
        router.register_provider(_provider("geckoterminal", real))

        envelope = router.get_ohlcv("NVDAON/USDC", chain="ethereum", timeframe="1h", limit=2)

        assert len(envelope.value) == 2
        assert envelope.meta.forward_filled is True


class TestSyntheticCandlesNotFinalized:
    """Synthetic forward-fill buckets must never be persisted as finalized history."""

    def test_old_synthetic_candles_not_written_to_finalized_cache(self, tmp_path):
        router = OHLCVRouter(disk_cache_dir=tmp_path, default_chain="ethereum")
        now = datetime.now(UTC)
        # 1d pool, last trade 10 days ago. Within the 24-day DEX horizon, so it is
        # forward-filled — but the synthetic daily buckets from ~9d..1d ago are far
        # older than the 24h finalization cutoff. The pre-fix bug persisted them to
        # the disk cache as immutable source="disk_cache" confidence=1.0 history.
        last_trade = _floor_day(now) - timedelta(days=10)
        real = _daily_series_ending(last_trade, count=5)
        router.register_provider(_provider("geckoterminal", real))

        envelope = router.get_ohlcv("NVDAON/USDC", chain="ethereum", timeframe="1d", limit=30)
        assert envelope.meta.forward_filled is True

        # Inspect everything the router wrote to the finalized disk cache.
        cached: list[dict] = []
        for path in tmp_path.glob("*.json"):
            cached.extend(json.loads(path.read_text()).get("candles", []))

        # Real candles (all >24h old) are legitimately finalized and cached.
        assert cached, "expected real candles to be cached as finalized"
        # ...but NOT a single synthetic (zero-volume) bucket.
        assert all(
            Decimal(c["volume"]) != 0 for c in cached
        ), "synthetic zero-volume candles must never be persisted as finalized OHLCV"


class TestCexSourceExcludedFromForwardFill:
    """CEX sources must keep the strict budget — the ALM-2697 dead-feed guard."""

    def test_stale_cex_for_defi_token_not_forward_filled(self, tmp_path):
        router = OHLCVRouter(disk_cache_dir=tmp_path, default_chain="ethereum")
        now = datetime.now(UTC)
        # 5h stale on 1h: within the DEX horizon (24h) but past the strict 2h
        # budget. A DEX source would be forward-filled; binance must NOT be.
        stale = _hourly_series_ending(_floor_hour(now) - timedelta(hours=5), count=10)
        router.register_provider(_provider("binance", stale))

        with pytest.raises(DataSourceUnavailable) as exc_info:
            router.get_ohlcv("NVDAON/USDC", chain="ethereum", timeframe="1h", limit=10)
        assert "stale" in exc_info.value.reason.lower()
