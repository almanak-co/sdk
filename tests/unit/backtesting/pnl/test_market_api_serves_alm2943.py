"""Market-API serves from engine-owned data (ALM-2943 part B).

Pins the three accessor groups this batch flips from REFUSED to SERVED in
backtests:

- ``pool_price`` / ``pool_price_by_pair``: the tick's pair-ratio proxy
  (token0_usd / token1_usd, pool-canonical orientation) with proxy
  provenance, registry-known-pool resolution, and honest refusals.
- ``realized_vol`` / ``vol_cone``: close-to-close realized vol over the
  run's own close series; intrabar estimators refuse.
- ``estimate_slippage``: the engine's own fill slippage model, one plane.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    BacktestOHLCVView,
    BacktestPoolPriceView,
    BacktestVolatilityCalculator,
    DefaultSlippageModel,
    LinearImpactSlippageModel,
    SimulatedSlippageView,
    create_market_snapshot_from_state,
)
from almanak.framework.backtesting.pnl.indicator_engine import BacktestIndicatorEngine
from almanak.framework.market.errors import (
    PoolPriceUnavailableError,
    SlippageEstimateUnavailableError,
)

D = Decimal

CHAIN = "ethereum"
WETH_ADDR = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
USDC_ADDR = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
# Canonical USDC/WETH 0.05% Uniswap V3 pool (registry-known).
UNI_V3_POOL_500 = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"

TOKEN_ADDRESSES = {
    "WETH": (CHAIN, WETH_ADDR),
    "USDC": (CHAIN, USDC_ADDR),
}

TS = datetime(2026, 7, 1, tzinfo=UTC)

WETH_USD = D("3000")
USDC_USD = D("1")
# Pool-canonical orientation: token0 = lower-sorted address = USDC, so the
# served price is USDC expressed in WETH units (matches the live reader's
# "price of token0 in terms of token1" contract).
EXPECTED_PAIR_RATIO = USDC_USD / WETH_USD


class _StubBacktester:
    """Just the slippage-model plane of PnLBacktester (same lookup rule)."""

    def __init__(self, slippage_models: dict[str, Any]) -> None:
        self.slippage_models = slippage_models

    def get_slippage_model(self, protocol: str) -> Any:
        return self.slippage_models.get(protocol, self.slippage_models["default"])


def _market_state(prices: dict[tuple[str, str], Decimal] | None = None) -> MarketState:
    if prices is None:
        prices = {(CHAIN, WETH_ADDR): WETH_USD, (CHAIN, USDC_ADDR): USDC_USD}
    state = MarketState(timestamp=TS, prices=dict(prices), chain=CHAIN)
    # The engine loop registers symbol aliases on the state each tick before
    # snapshot creation; the views rely on the same plain-symbol lookup plane.
    state.register_symbol_aliases(TOKEN_ADDRESSES)
    return state


def _snapshot(
    market_state: MarketState | None = None,
    *,
    ohlcv_module: Any | None = None,
    pool_price_view: BacktestPoolPriceView | None = None,
    slippage_view: SimulatedSlippageView | None = None,
    volatility_calculator: BacktestVolatilityCalculator | None = None,
):
    state = market_state or _market_state()
    if pool_price_view is not None:
        pool_price_view.bind(state, TS)
    if slippage_view is not None:
        slippage_view.bind(state, TS)
    return create_market_snapshot_from_state(
        market_state=state,
        chain=CHAIN,
        token_addresses=TOKEN_ADDRESSES,
        ohlcv_module=ohlcv_module,
        pool_price_view=pool_price_view,
        slippage_view=slippage_view,
        volatility_calculator=volatility_calculator,
    )


# ---------------------------------------------------------------------------
# pool_price / pool_price_by_pair — pair-ratio proxy
# ---------------------------------------------------------------------------


class TestPoolPricePairRatioProxy:
    def test_pair_orientation_and_value(self):
        view = BacktestPoolPriceView(CHAIN, TOKEN_ADDRESSES)
        snapshot = _snapshot(pool_price_view=view)

        envelope = snapshot.pool_price_by_pair("WETH", "USDC", fee_tier=500)
        # token0 = USDC (lower address): price is USDC in WETH units, NOT 3000.
        assert envelope.value.price == EXPECTED_PAIR_RATIO
        assert envelope.value.price == D("1") / D("3000")

    def test_caller_argument_order_does_not_flip_orientation(self):
        view = BacktestPoolPriceView(CHAIN, TOKEN_ADDRESSES)
        snapshot = _snapshot(pool_price_view=view)

        a = snapshot.pool_price_by_pair("WETH", "USDC", fee_tier=500)
        b = snapshot.pool_price_by_pair("USDC", "WETH", fee_tier=500)
        assert a.value.price == b.value.price == EXPECTED_PAIR_RATIO

    def test_pool_address_scoped_known_pool_serves(self):
        view = BacktestPoolPriceView(CHAIN, TOKEN_ADDRESSES)
        snapshot = _snapshot(pool_price_view=view)

        envelope = snapshot.pool_price(UNI_V3_POOL_500)
        assert envelope.value.price == EXPECTED_PAIR_RATIO
        assert envelope.value.pool_address == UNI_V3_POOL_500
        assert envelope.value.fee_tier == 500
        # No unconfigured ledger entry — the accessor served.
        assert ("pool_price", "unconfigured") not in snapshot._critical_data_failures

    def test_proxy_provenance_marked_and_warned_once(self, caplog):
        view = BacktestPoolPriceView(CHAIN, TOKEN_ADDRESSES)
        snapshot = _snapshot(pool_price_view=view)

        with caplog.at_level(logging.WARNING):
            envelope = snapshot.pool_price_by_pair("WETH", "USDC", fee_tier=500)
            snapshot.pool_price_by_pair("WETH", "USDC", fee_tier=500)

        assert "pair_ratio_proxy" in envelope.meta.source
        assert envelope.meta.proxy_source == "USDC/WETH"
        # Never claimed as venue pool spot.
        assert not envelope.is_execution_grade
        assert envelope.value.tick is None
        # Empty != Zero: unmeasured liquidity is None, never a fabricated
        # measured-empty pool (a strategy gating on liquidity must not read
        # the proxy as "empty pool").
        assert envelope.value.liquidity is None
        proxy_warnings = [r for r in caplog.records if "PAIR-RATIO proxy" in r.getMessage()]
        assert len(proxy_warnings) == 1

    def test_unknown_pool_address_refuses_with_ledger(self):
        view = BacktestPoolPriceView(CHAIN, TOKEN_ADDRESSES)
        snapshot = _snapshot(pool_price_view=view)
        unknown = "0x" + "d" * 40

        with pytest.raises(PoolPriceUnavailableError):
            snapshot.pool_price(unknown)
        keys = set(snapshot._critical_data_failures)
        assert ("pool_price", f"{unknown}:unknown_pool") in keys

    def test_unpriceable_leg_refuses_with_ledger(self):
        # DAI resolves offline but has no price in the run's series.
        view = BacktestPoolPriceView(CHAIN, TOKEN_ADDRESSES)
        snapshot = _snapshot(pool_price_view=view)

        with pytest.raises(PoolPriceUnavailableError):
            snapshot.pool_price_by_pair("DAI", "WETH")
        assert any(
            source == "pool_price_by_pair" and key.endswith(":unpriceable")
            for source, key in snapshot._critical_data_failures
        )

    def test_no_ledger_entry_on_successful_serve(self):
        view = BacktestPoolPriceView(CHAIN, TOKEN_ADDRESSES)
        snapshot = _snapshot(pool_price_view=view)

        snapshot.pool_price_by_pair("WETH", "USDC", fee_tier=500)
        assert not any(source == "pool_price_by_pair" for source, _ in snapshot._critical_data_failures)


# ---------------------------------------------------------------------------
# realized_vol / vol_cone — close-to-close over the run's series
# ---------------------------------------------------------------------------


def _ohlcv_view(prices: list[Decimal]) -> BacktestOHLCVView:
    engine = BacktestIndicatorEngine()
    for price in prices:
        engine.append_price("WETH", price)
    view = BacktestOHLCVView(engine, 3600, TOKEN_ADDRESSES)
    view.bind(TS)
    return view


class TestBacktestVolatility:
    def test_constant_series_zero_vol(self):
        view = _ohlcv_view([D("3000")] * 200)
        snapshot = _snapshot(ohlcv_module=view, volatility_calculator=BacktestVolatilityCalculator())

        result = snapshot.realized_vol("WETH", window_days=2)
        assert result.value.annualized_vol == 0.0
        assert result.value.daily_vol == 0.0
        assert result.value.estimator == "close_to_close"

    def test_moving_series_positive_vol(self):
        prices = [D("3000") + D(i % 10) * 3 - D(i % 7) for i in range(200)]
        view = _ohlcv_view(prices)
        snapshot = _snapshot(ohlcv_module=view, volatility_calculator=BacktestVolatilityCalculator())

        result = snapshot.realized_vol("WETH", window_days=2)
        assert result.value.annualized_vol > 0.0

    def test_vol_cone_served(self):
        prices = [D("3000") + D(i % 10) * 3 - D(i % 7) for i in range(200)]
        view = _ohlcv_view(prices)
        snapshot = _snapshot(ohlcv_module=view, volatility_calculator=BacktestVolatilityCalculator())

        result = snapshot.vol_cone("WETH", windows=[1, 2])
        assert len(result.value.entries) == 2
        assert result.value.entries[0].current_vol >= 0.0

    def test_default_vol_windows_serve_with_lazily_sized_retention(self):
        # Review pin (#3346): tick retention must cover the largest DEFAULT
        # vol_cone window (90d) WHEN vol is used. With the plain 200-tick
        # buffer, default vol_cone() calls refused FOREVER mid-run
        # (InsufficientDataError: the 14/30/90d windows need more candles
        # than the buffer could ever hold), however long the run.
        # Round 2: the raise is LAZY — the first vol accessor call sizes the
        # buffer (eager sizing for every run made per-tick indicator cost
        # grow with buffer depth and tripped the 1-year perf SLAs).
        from almanak.framework.data.volatility.realized import DEFAULT_VOL_CONE_WINDOWS_DAYS

        tick_seconds = 3600
        # Same ceil-div the calculator's lazy sizing uses.
        needed = -(-(max(DEFAULT_VOL_CONE_WINDOWS_DAYS) * 86400) // tick_seconds)
        engine = BacktestIndicatorEngine()
        for i in range(300):
            engine.append_price("WETH", D("3000") + D(i % 10) * 3 - D(i % 7))
        # Never-used vol => cheap default retention.
        assert engine._max_history == 200
        view = BacktestOHLCVView(engine, tick_seconds, TOKEN_ADDRESSES)
        view.bind(TS)
        snapshot = _snapshot(
            ohlcv_module=view,
            volatility_calculator=BacktestVolatilityCalculator(
                indicator_engine=engine, tick_interval_seconds=tick_seconds
            ),
        )

        # First vol read: not enough history yet (honest warm-up refusal is
        # acceptable) but it MUST raise the retention so the windows can ever
        # fill — the round-1 refuse-forever bug.
        try:
            snapshot.vol_cone("WETH")
        except Exception:
            pass
        assert engine._max_history >= needed

        for i in range(needed):
            engine.append_price("WETH", D("3000") + D(i % 10) * 3 - D(i % 7))
        assert engine.get_buffer_size("WETH") == needed

        cone = snapshot.vol_cone("WETH")  # DEFAULT windows
        assert [entry.window_days for entry in cone.value.entries] == sorted(DEFAULT_VOL_CONE_WINDOWS_DAYS)
        vol = snapshot.realized_vol("WETH")  # default 30d window
        assert vol.value.sample_count > 0

    def test_vol_unused_keeps_default_retention_and_bounded_eager_window(self):
        # Perf pin (#3346 round 2): a run that never reads vol keeps the
        # 200-tick buffer, and even a deepened buffer feeds the EAGER per-tick
        # indicator plane at most `_base_max_history` prices — per-tick cost
        # must not grow with vol retention (1-year SLA regression).
        engine = BacktestIndicatorEngine()
        calc = BacktestVolatilityCalculator(indicator_engine=engine, tick_interval_seconds=3600)
        _ = calc  # constructed but never called
        for i in range(5000):
            engine.append_price("WETH", D(3000 + i % 50))
        assert engine._max_history == 200
        assert engine.get_buffer_size("WETH") == 200

    def test_ensure_capacity_never_shrinks_and_keeps_data(self):
        engine = BacktestIndicatorEngine()
        for i in range(50):
            engine.append_price("WETH", D(3000 + i))
        engine.ensure_capacity(500)
        assert engine.get_buffer_size("WETH") == 50  # data preserved
        engine.ensure_capacity(100)  # lower ask: no shrink
        for _ in range(500):
            engine.append_price("WETH", D("1"))
        assert engine.get_buffer_size("WETH") == 500

    def test_intrabar_estimator_refuses(self):
        view = _ohlcv_view([D("3000")] * 200)
        snapshot = _snapshot(ohlcv_module=view, volatility_calculator=BacktestVolatilityCalculator())

        with pytest.raises(ValueError, match="close-only"):
            snapshot.realized_vol("WETH", window_days=2, estimator="parkinson")
        with pytest.raises(ValueError, match="close-only"):
            snapshot.vol_cone("WETH", windows=[1, 2], estimator="parkinson")


# ---------------------------------------------------------------------------
# estimate_slippage — the engine's own fill model
# ---------------------------------------------------------------------------


class TestSimulatedSlippage:
    def test_accessor_matches_engine_model_output(self):
        model = LinearImpactSlippageModel()
        backtester = _StubBacktester({"default": DefaultSlippageModel(), "uniswap_v3": model})
        view = SimulatedSlippageView(backtester)
        state = _market_state()
        snapshot = _snapshot(state, slippage_view=view)

        envelope = snapshot.estimate_slippage("WETH", "USDC", D("1"), protocol="uniswap_v3")

        from almanak.framework.backtesting.models import IntentType

        model_pct = model.calculate_slippage(
            intent_type=IntentType.SWAP,
            amount_usd=D("1") * WETH_USD,
            market_state=state,
            protocol="uniswap_v3",
        )
        assert envelope.value.effective_slippage_bps == int(model_pct * 10000)
        assert envelope.value.price_impact_bps == envelope.value.effective_slippage_bps
        # Caller-oriented expected price: USDC per WETH after slippage.
        assert envelope.value.expected_price == (WETH_USD / USDC_USD) * (D("1") - model_pct)
        assert envelope.meta.source == "backtest_slippage_model:linear_impact"

    def test_protocol_falls_back_to_default_model(self):
        default_model = DefaultSlippageModel()
        backtester = _StubBacktester({"default": default_model})
        view = SimulatedSlippageView(backtester)
        state = _market_state()
        snapshot = _snapshot(state, slippage_view=view)

        envelope = snapshot.estimate_slippage("WETH", "USDC", D("2"))
        # DefaultSlippageModel: flat 0.1% -> 10 bps.
        assert envelope.value.effective_slippage_bps == 10
        assert envelope.meta.source == "backtest_slippage_model:default"

    def test_unpriceable_token_refuses_with_ledger(self):
        backtester = _StubBacktester({"default": DefaultSlippageModel()})
        view = SimulatedSlippageView(backtester)
        snapshot = _snapshot(slippage_view=view)

        with pytest.raises(SlippageEstimateUnavailableError):
            snapshot.estimate_slippage("WETH", "DOGE", D("1"))
        assert any(
            source == "estimate_slippage" and key.endswith(":unpriceable")
            for source, key in snapshot._critical_data_failures
        )

    def test_non_positive_amount_refuses(self):
        backtester = _StubBacktester({"default": DefaultSlippageModel()})
        view = SimulatedSlippageView(backtester)
        snapshot = _snapshot(slippage_view=view)

        with pytest.raises(SlippageEstimateUnavailableError):
            snapshot.estimate_slippage("WETH", "USDC", D("0"))
