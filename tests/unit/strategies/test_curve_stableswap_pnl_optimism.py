"""Tests for Curve StableSwap PnL Backtest strategy on Optimism.

Validates depeg-based swap decisions, state persistence, teardown,
and intent generation for stablecoin arbitrage via Curve.

Kitchen Loop iteration 120, VIB-1716.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def strategy():
    from strategies.incubating.curve_stableswap_pnl_optimism.strategy import (
        CurveStableswapPnLOptimismStrategy,
    )

    strat = CurveStableswapPnLOptimismStrategy.__new__(CurveStableswapPnLOptimismStrategy)
    strat.config = {}
    strat._chain = "optimism"
    strat._wallet_address = "0x" + "0" * 40
    strat._strategy_id = "test-curve-stableswap-pnl-optimism"
    strat.trade_size_usd = Decimal("100")
    strat.base_token = "USDC"
    strat.quote_token = "USDT"
    strat.depeg_threshold_bps = 30
    strat.max_slippage_bps = 50
    strat._consecutive_holds = 0
    strat._total_swaps = 0
    strat._last_direction = None
    return strat


def _mock_market(
    base_price: float = 1.0000,
    quote_price: float = 1.0000,
) -> MagicMock:
    market = MagicMock()

    def price_fn(token):
        if token == "USDC":
            return Decimal(str(base_price))
        elif token == "USDT":
            return Decimal(str(quote_price))
        raise ValueError(f"Unknown token: {token}")

    market.price = MagicMock(side_effect=price_fn)
    return market


class TestDecision:
    def test_hold_when_no_depeg(self, strategy):
        """No swap when prices are at peg."""
        market = _mock_market(base_price=1.0000, quote_price=1.0000)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "No depeg" in intent.reason

    def test_hold_when_small_depeg(self, strategy):
        """No swap when deviation is below threshold."""
        # 20bps deviation, threshold is 30bps
        market = _mock_market(base_price=0.9980, quote_price=1.0000)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_buy_base_when_base_cheaper(self, strategy):
        """Swap USDT -> USDC when USDC is cheaper (depeg)."""
        # USDC at 0.9960 vs USDT at 1.0 = 40bps deviation > 30bps threshold
        market = _mock_market(base_price=0.9960, quote_price=1.0000)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDT"
        assert intent.to_token == "USDC"
        assert intent.amount_usd == Decimal("100")

    def test_buy_quote_when_quote_cheaper(self, strategy):
        """Swap USDC -> USDT when USDT is cheaper (depeg)."""
        # USDC at 1.0 vs USDT at 0.9960 = ratio > 1, 40bps deviation
        market = _mock_market(base_price=1.0000, quote_price=0.9960)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDC"
        assert intent.to_token == "USDT"

    def test_hold_on_repeated_same_direction(self, strategy):
        """Don't swap same direction twice in a row."""
        strategy._last_direction = "buy_base"
        market = _mock_market(base_price=0.9960, quote_price=1.0000)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "Same direction" in intent.reason

    def test_swap_after_direction_change(self, strategy):
        """Allow swap when direction changes."""
        strategy._last_direction = "buy_quote"
        market = _mock_market(base_price=0.9960, quote_price=1.0000)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDT"
        assert intent.to_token == "USDC"

    def test_hold_on_price_error(self, strategy):
        """Hold when price data is unavailable."""
        market = MagicMock()
        market.price = MagicMock(side_effect=ValueError("price unavailable"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason.lower()

    def test_hold_on_zero_price(self, strategy):
        """Hold when price is zero."""
        market = _mock_market(base_price=0.0, quote_price=1.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_slippage_from_config(self, strategy):
        """Max slippage is correctly derived from bps config."""
        market = _mock_market(base_price=0.9960, quote_price=1.0000)
        intent = strategy.decide(market)
        assert intent.max_slippage == Decimal("50") / Decimal("10000")

    def test_consecutive_holds_increment(self, strategy):
        """Consecutive holds counter increments on hold."""
        market = _mock_market(base_price=1.0, quote_price=1.0)
        strategy.decide(market)
        assert strategy._consecutive_holds == 1
        strategy.decide(market)
        assert strategy._consecutive_holds == 2

    def test_consecutive_holds_reset_on_swap(self, strategy):
        """Consecutive holds counter resets after swap."""
        strategy._consecutive_holds = 5
        market = _mock_market(base_price=0.9960, quote_price=1.0000)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert strategy._consecutive_holds == 0


class TestOnIntentExecuted:
    def test_tracks_buy_base(self, strategy):
        """Records buy_base direction after successful swap."""
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"
        mock_intent.from_token = "USDT"
        mock_intent.to_token = "USDC"

        mock_result = MagicMock()
        mock_result.swap_amounts = None

        strategy.on_intent_executed(mock_intent, True, mock_result)
        assert strategy._last_direction == "buy_base"
        assert strategy._total_swaps == 1

    def test_tracks_buy_quote(self, strategy):
        """Records buy_quote direction after successful swap."""
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"
        mock_intent.from_token = "USDC"
        mock_intent.to_token = "USDT"

        mock_result = MagicMock()
        mock_result.swap_amounts = None

        strategy.on_intent_executed(mock_intent, True, mock_result)
        assert strategy._last_direction == "buy_quote"
        assert strategy._total_swaps == 1

    def test_no_tracking_on_failure(self, strategy):
        """Does not update state on failed execution."""
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"
        strategy.on_intent_executed(mock_intent, False, MagicMock())
        assert strategy._total_swaps == 0
        assert strategy._last_direction is None

    def test_ignores_non_swap(self, strategy):
        """Ignores non-SWAP intents."""
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "HOLD"
        strategy.on_intent_executed(mock_intent, True, MagicMock())
        assert strategy._total_swaps == 0


class TestStatePersistence:
    def test_get_persistent_state(self, strategy):
        strategy._consecutive_holds = 3
        strategy._total_swaps = 2
        strategy._last_direction = "buy_base"
        state = strategy.get_persistent_state()
        assert state == {
            "consecutive_holds": 3,
            "total_swaps": 2,
            "last_direction": "buy_base",
        }

    def test_load_persistent_state(self, strategy):
        strategy.load_persistent_state({
            "consecutive_holds": 5,
            "total_swaps": 10,
            "last_direction": "buy_quote",
        })
        assert strategy._consecutive_holds == 5
        assert strategy._total_swaps == 10
        assert strategy._last_direction == "buy_quote"

    def test_load_empty_state(self, strategy):
        strategy.load_persistent_state({})
        assert strategy._consecutive_holds == 0
        assert strategy._total_swaps == 0
        assert strategy._last_direction is None


class TestTeardown:
    def test_supports_teardown(self, strategy):
        assert strategy.supports_teardown() is True

    def test_no_open_positions(self, strategy):
        """Pure swap strategy has no positions to tear down."""
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_no_teardown_intents(self, strategy):
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert intents == []

        intents = strategy.generate_teardown_intents(TeardownMode.HARD)
        assert intents == []


class TestStatus:
    def test_get_status(self, strategy):
        status = strategy.get_status()
        assert status["strategy"] == "curve_stableswap_pnl_optimism"
        assert status["config"]["depeg_threshold_bps"] == 30
        assert status["config"]["pair"] == "USDC/USDT"
        assert status["state"]["total_swaps"] == 0


class TestEdgeCases:
    def test_large_depeg_triggers_swap(self, strategy):
        """Large depeg (100bps) triggers swap."""
        market = _mock_market(base_price=0.99, quote_price=1.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"

    def test_exact_threshold_triggers_swap(self, strategy):
        """Deviation exactly at threshold triggers swap."""
        # 30bps = 0.003, so ratio = 0.997
        market = _mock_market(base_price=0.9970, quote_price=1.0000)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"

    def test_protocol_is_curve(self, strategy):
        """Swap intent uses Curve protocol."""
        market = _mock_market(base_price=0.9960, quote_price=1.0000)
        intent = strategy.decide(market)
        assert intent.protocol == "curve"
