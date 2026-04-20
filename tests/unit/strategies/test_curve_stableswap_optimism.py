"""Tests for Curve StableSwap NG strategy on Optimism.

Validates buy/sell phase transitions, teardown, and intent generation
with Curve StableSwap NG pool (USDC/crvUSD) on Optimism.

Kitchen Loop iteration 115, VIB-1667.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def strategy():
    from strategies.incubating.curve_stableswap_optimism.strategy import (
        CurveStableswapOptimismStrategy,
    )

    strat = CurveStableswapOptimismStrategy.__new__(CurveStableswapOptimismStrategy)
    strat.config = {}
    strat._chain = "optimism"
    strat._wallet_address = "0x" + "0" * 40
    strat._strategy_id = "test-curve-stableswap-optimism"
    strat.trade_size_usd = Decimal("50")
    strat.from_token = "USDC"
    strat.to_token = "crvUSD"
    strat.max_slippage_pct = 0.5
    strat.force_action = "swap"
    strat._phase = "buy"
    strat._buy_executed = False
    strat._sell_executed = False
    return strat


def _mock_market() -> MagicMock:
    market = MagicMock()

    def price_fn(token):
        return Decimal("1.00")

    market.price = MagicMock(side_effect=price_fn)
    return market


class TestDecision:
    def test_buy_phase_emits_swap(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDC"
        assert intent.to_token == "crvUSD"
        assert intent.amount == Decimal("50")

    def test_sell_phase_emits_swap(self, strategy):
        strategy._phase = "sell"
        strategy._buy_executed = True
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "crvUSD"
        assert intent.to_token == "USDC"
        assert intent.amount == "all"

    def test_done_phase_holds(self, strategy):
        strategy._phase = "done"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_price_unavailable_holds(self, strategy):
        market = MagicMock()
        market.price = MagicMock(side_effect=ValueError("No price"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"


class TestPhaseTransitions:
    def test_buy_success_advances_to_sell(self, strategy):
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"
        mock_result = MagicMock()
        mock_result.swap_amounts = None
        strategy.on_intent_executed(mock_intent, success=True, result=mock_result)
        assert strategy._phase == "sell"
        assert strategy._buy_executed is True

    def test_sell_success_advances_to_done(self, strategy):
        strategy._phase = "sell"
        strategy._buy_executed = True
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"
        mock_result = MagicMock()
        mock_result.swap_amounts = None
        strategy.on_intent_executed(mock_intent, success=True, result=mock_result)
        assert strategy._phase == "done"
        assert strategy._sell_executed is True

    def test_failure_does_not_advance(self, strategy):
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"
        strategy.on_intent_executed(mock_intent, success=False, result=MagicMock())
        assert strategy._phase == "buy"
        assert strategy._buy_executed is False


class TestTeardown:
    def test_teardown_supported(self, strategy):
        assert strategy.supports_teardown() is True

    def test_open_positions_with_crvusd(self, strategy):
        strategy._buy_executed = True
        strategy._sell_executed = False
        positions = strategy.get_open_positions()
        assert len(positions.positions) == 1
        assert positions.positions[0].details["asset"] == "crvUSD"

    def test_no_positions_before_buy(self, strategy):
        positions = strategy.get_open_positions()
        assert len(positions.positions) == 0

    def test_no_positions_after_sell(self, strategy):
        strategy._buy_executed = True
        strategy._sell_executed = True
        positions = strategy.get_open_positions()
        assert len(positions.positions) == 0

    def test_teardown_intents_with_position(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._buy_executed = True
        strategy._sell_executed = False
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].from_token == "crvUSD"
        assert intents[0].to_token == "USDC"

    def test_teardown_hard_uses_higher_slippage(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._buy_executed = True
        strategy._sell_executed = False
        intents = strategy.generate_teardown_intents(TeardownMode.HARD)
        assert intents[0].max_slippage == Decimal("0.03")

    def test_no_teardown_when_done(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._buy_executed = True
        strategy._sell_executed = True
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 0


class TestState:
    def test_get_persistent_state(self, strategy):
        state = strategy.get_persistent_state()
        assert state == {"phase": "buy", "buy_executed": False, "sell_executed": False}

    def test_load_persistent_state(self, strategy):
        strategy.load_persistent_state(
            {"phase": "sell", "buy_executed": True, "sell_executed": False}
        )
        assert strategy._phase == "sell"
        assert strategy._buy_executed is True
        assert strategy._sell_executed is False

    def test_get_status(self, strategy):
        status = strategy.get_status()
        assert status["strategy"] == "curve_stableswap_optimism"
        assert status["chain"] == "optimism"
        assert status["phase"] == "buy"
