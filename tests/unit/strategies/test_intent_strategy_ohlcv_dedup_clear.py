"""Strategy-level test: IntentStrategy.create_market_snapshot() clears the
per-strategy OHLCV deduper between iterations (VIB-3783).

The deduper coalesces upstream fetches *within* a single decide() call (so MACD
and ATR on the same token only hit upstream once). The reset hook in
create_market_snapshot() is what guarantees subsequent iterations actually
re-fetch fresh candles -- without it, the wrapper would silently freeze the
strategy on stale data forever.

This test injects a real DedupingOHLCVProvider into a minimal IntentStrategy
subclass (bypassing the heavyweight __init__), pre-populates the cache, calls
create_market_snapshot(), and asserts the cache was cleared.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from almanak import IntentStrategy
from almanak.framework.data.interfaces import OHLCVCandle
from almanak.framework.data.ohlcv.dedup_provider import DedupingOHLCVProvider


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_candles(n: int) -> list[OHLCVCandle]:
    base = datetime(2026, 4, 30, tzinfo=UTC)
    return [
        OHLCVCandle(
            timestamp=base + timedelta(hours=i),
            open=Decimal(f"{1000 + i}"),
            high=Decimal(f"{1010 + i}"),
            low=Decimal(f"{990 + i}"),
            close=Decimal(f"{1005 + i}"),
            volume=Decimal(f"{100 + i}"),
        )
        for i in range(n)
    ]


class _StubInner:
    """Stand-in OHLCV provider; we never actually call get_ohlcv in this test."""

    supported_timeframes = ["1h"]

    async def get_ohlcv(
        self,
        token: str,
        quote: str = "USD",
        timeframe: str = "1h",
        limit: int = 100,
    ) -> list[OHLCVCandle]:  # pragma: no cover - not exercised here
        raise AssertionError("inner provider should not be called in this test")


@dataclass
class _Config:
    deployment_id: str = "test"
    strategy_name: str = "test"
    chain: str = "ethereum"

    def to_dict(self) -> dict:
        return {}

    def update(self, **kwargs) -> None:
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)


class _Strat(IntentStrategy):
    """Minimal IntentStrategy subclass for unit-testing the cache-reset hook."""

    def decide(self, market):  # pragma: no cover - not exercised here
        return None

    def get_open_positions(self):  # pragma: no cover - not exercised here
        from almanak.framework.teardown.models import TeardownPositionSummary

        return TeardownPositionSummary.empty("test")

    def generate_teardown_intents(self, mode=None, market=None):  # pragma: no cover
        return []


def _make_strategy_with_deduper() -> tuple[_Strat, DedupingOHLCVProvider]:
    """Build a strategy instance bypassing __init__ and wire only what
    create_market_snapshot() actually touches.
    """
    s = object.__new__(_Strat)
    # Attributes touched by create_market_snapshot() before construction of
    # MarketSnapshot. We stop the test at the clear() side-effect, so we don't
    # need a fully-formed strategy.
    s._chain = "ethereum"
    s._wallet_address = "0x" + "0" * 40
    s._deployment_id = "test"
    s._price_oracle = None
    s._rsi_provider = None
    s._balance_provider = None
    s._wallet_activity_provider = None
    s._prediction_provider = None
    s._indicator_provider = None
    s._multi_dex_service = None
    s._rate_monitor = None
    s._funding_rate_provider = None
    s._gateway_client = None
    s._multi_chain_price_oracle = None
    s._multi_chain_balance_provider = None
    s._aave_health_factor_provider = None

    # Real deduper injected as the runner would do it.
    deduper = DedupingOHLCVProvider(_StubInner())
    s._ohlcv_dedup_provider = deduper

    # Stub get_config so create_market_snapshot's data_granularity lookup works.
    s.get_config = lambda key, default=None: default  # type: ignore[method-assign]

    return s, deduper


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCreateMarketSnapshotClearsDeduper:
    """create_market_snapshot() must clear the OHLCV deduper cache each call."""

    def test_clears_populated_cache(self) -> None:
        """A populated deduper cache is empty after create_market_snapshot()."""
        strat, deduper = _make_strategy_with_deduper()

        # Pre-populate as if MACD on iter N had already fetched 85 candles.
        deduper._cache[("cbBTC", "USD", "1h")] = _make_candles(85)
        assert len(deduper._cache) == 1

        strat.create_market_snapshot()

        # The hook ran -- cache is empty so iter N+1 will refetch from upstream.
        assert deduper._cache == {}

    def test_clears_between_two_iterations(self) -> None:
        """Two successive iterations: cache is reset before each."""
        strat, deduper = _make_strategy_with_deduper()

        # iter 1: populate, then snapshot resets.
        deduper._cache[("cbBTC", "USD", "1h")] = _make_candles(85)
        strat.create_market_snapshot()
        assert deduper._cache == {}

        # iter 2: populate again with different keys, then snapshot resets.
        deduper._cache[("WETH", "USD", "1h")] = _make_candles(50)
        deduper._cache[("WETH", "USD", "4h")] = _make_candles(50)
        assert len(deduper._cache) == 2
        strat.create_market_snapshot()
        assert deduper._cache == {}

    def test_no_deduper_wired_is_safe(self) -> None:
        """Strategies without indicators (no _wire_indicators call) leave the
        attribute as None; create_market_snapshot() must produce a snapshot
        without blowing up.
        """
        strat, _ = _make_strategy_with_deduper()
        strat._ohlcv_dedup_provider = None

        snapshot = strat.create_market_snapshot()

        # Snapshot was constructed (the clear() guard correctly short-circuited).
        assert snapshot is not None
        # Attribute remains None -- no accidental re-wiring side-effect.
        assert strat._ohlcv_dedup_provider is None
