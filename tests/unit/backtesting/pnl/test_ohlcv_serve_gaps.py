"""Serve-gap fixes for market.ohlcv() / market.pool_analytics() refusals
observed on staging: CoinGecko-only backtests went hollow (0 intents) because
the strategy's decision inputs refused every tick.

Pinned contracts:
1. Symbol calls resolve through the offline token registry when the run's
   buffers are ADDRESS-keyed and the symbol is not config-declared (the
   ohlcv:WAVAX shape — the run held WAVAX prices yet refused every tick).
2. The pool-proxy warn-once line fires only AFTER a successful serve — a run
   whose every pool-scoped read refuses must not log "served as ... proxy".
3. pool_analytics stays refused (no historical analytics plane to serve) but
   under the truthful ledger key `not_simulated` with a message that says
   what to gate on instead — only in backtest-built snapshots; live
   snapshots keep the `unconfigured` misconfiguration key.
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    BacktestOHLCVView,
    create_market_snapshot_from_state,
)
from almanak.framework.backtesting.pnl.indicator_engine import BacktestIndicatorEngine

BOUND_TS = datetime(2026, 4, 20, 12, tzinfo=UTC)
BASE_WETH_USDC_POOL = "0xd0b53D9277642d899DF5C87A3966A349A798F224"
WAVAX_KEY = ("avalanche", "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7")
WAVAX_DISPLAY = "avalanche:0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7"


def _engine_with_series(token: str, closes: list[float]) -> BacktestIndicatorEngine:
    engine = BacktestIndicatorEngine(required_indicators={"rsi"}, max_history=200)
    for close in closes:
        engine.append_price(token, Decimal(str(close)))
    return engine


def _view(engine, token_addresses=None, chain=None) -> BacktestOHLCVView:
    view = BacktestOHLCVView(engine, 3600, token_addresses, chain=chain)
    view.bind(BOUND_TS)
    return view


def _snapshot(view, chain="base", prices=None):
    state = MarketState(
        timestamp=BOUND_TS,
        prices=prices or {"WETH": Decimal("3000"), "USDC": Decimal("1")},
        chain=chain,
        block_number=1,
    )
    return create_market_snapshot_from_state(market_state=state, chain=chain, ohlcv_module=view)


class TestRegistryFallback:
    def test_wavax_serves_from_address_keyed_buffers_via_registry(self):
        """Address-keyed buffers + undeclared symbol now resolve through
        the offline registry (the run held WAVAX prices yet symbol reads
        refused every tick)."""
        engine = _engine_with_series(WAVAX_DISPLAY, [40.0 + i * 0.1 for i in range(30)])
        view = _view(engine, token_addresses=None, chain="avalanche")
        snapshot = _snapshot(view, chain="avalanche", prices={WAVAX_KEY: Decimal("40")})

        df = snapshot.ohlcv("WAVAX", timeframe="1h", limit=10)

        assert len(df) == 10
        assert df.attrs["source"] == "backtest_price_series:close_only"
        assert not snapshot._critical_data_failures

    def test_unknown_symbol_still_refuses(self):
        """The fallback must not turn genuine misses into serves."""
        engine = _engine_with_series(WAVAX_DISPLAY, [40.0] * 30)
        view = _view(engine, token_addresses=None, chain="avalanche")
        snapshot = _snapshot(view, chain="avalanche", prices={WAVAX_KEY: Decimal("40")})

        with pytest.raises(ValueError, match="no backtest price series"):
            snapshot.ohlcv("NOT_A_TOKEN_XYZ", timeframe="1h", limit=10)
        assert ("ohlcv", "NOT_A_TOKEN_XYZ") in snapshot._critical_data_failures

    def test_chainless_view_keeps_legacy_behavior(self):
        """No chain → no registry fallback → the pre-fix miss is unchanged."""
        engine = _engine_with_series(WAVAX_DISPLAY, [40.0] * 30)
        view = _view(engine, token_addresses=None, chain=None)
        snapshot = _snapshot(view, chain="avalanche", prices={WAVAX_KEY: Decimal("40")})

        with pytest.raises(ValueError, match="no backtest price series for token 'WAVAX'"):
            snapshot.ohlcv("WAVAX", timeframe="1h", limit=10)


class TestProxyWarnOrdering:
    def test_refusing_pool_scoped_run_never_logs_proxy_served(self, caplog):
        """A run whose pool-scoped reads all refuse must log NO
        'served as ... proxy' line (it used to warn at startup and then
        refuse every tick)."""
        engine = _engine_with_series("WETH", [3000.0 + i for i in range(30)])
        engine.set_data_granularity(86400, 3600)  # daily data under hourly ticks
        view = _view(engine)
        snapshot = _snapshot(view)

        with caplog.at_level(logging.WARNING):
            with pytest.raises(ValueError, match="ALM-2957"):
                snapshot.ohlcv("WETH/USDC", timeframe="1h", limit=10, pool_address=BASE_WETH_USDC_POOL)

        assert not [r for r in caplog.records if "price-series proxy" in r.message]
        assert ("ohlcv", "WETH/USDC:pool_scoped") in snapshot._critical_data_failures

    def test_successful_pool_scoped_serve_warns_once(self, caplog):
        engine = _engine_with_series("WETH", [3000.0 + i for i in range(30)])
        view = _view(engine)

        with caplog.at_level(logging.WARNING):
            for _ in range(3):
                snapshot = _snapshot(view)
                df = snapshot.ohlcv("WETH/USDC", timeframe="1h", limit=10, pool_address=BASE_WETH_USDC_POOL)
                assert df.attrs["source"].endswith(":pool_pair_proxy")

        proxy_warnings = [r for r in caplog.records if "price-series proxy" in r.message]
        assert len(proxy_warnings) == 1


class TestPoolAnalyticsTruthfulRefusal:
    def test_backtest_refusal_key_is_not_simulated_with_guidance(self):
        view = _view(_engine_with_series("WETH", [3000.0] * 30))
        snapshot = _snapshot(view)

        with pytest.raises(ValueError, match="not simulated in backtests"):
            snapshot.pool_analytics(BASE_WETH_USDC_POOL)

        assert ("pool_analytics", "not_simulated") in snapshot._critical_data_failures
        detail = snapshot._critical_data_failures[("pool_analytics", "not_simulated")]
        assert "pool_price" in detail and "ohlcv" in detail
        assert ("pool_analytics", "unconfigured") not in snapshot._critical_data_failures

    def test_best_pool_shares_the_truthful_key(self):
        view = _view(_engine_with_series("WETH", [3000.0] * 30))
        snapshot = _snapshot(view)

        with pytest.raises(ValueError, match="not simulated in backtests"):
            snapshot.best_pool("WETH", "USDC")
        assert ("best_pool", "not_simulated") in snapshot._critical_data_failures

    def test_live_snapshot_keeps_unconfigured_key(self):
        """Snapshots without the backtest stamp keep live semantics."""
        view = _view(_engine_with_series("WETH", [3000.0] * 30))
        snapshot = _snapshot(view)
        # Simulate the live default: the stamp is None unless a backtest
        # factory sets it (declared in MarketSnapshot.__init__).
        snapshot._pool_analytics_refusal_detail = None

        with pytest.raises(ValueError, match="No pool analytics reader configured"):
            snapshot.pool_analytics(BASE_WETH_USDC_POOL)
        assert ("pool_analytics", "unconfigured") in snapshot._critical_data_failures
