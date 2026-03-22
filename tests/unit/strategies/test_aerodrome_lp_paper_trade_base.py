"""Tests for Aerodrome LP Paper Trade strategy on Base.

Validates price-rebalancing LP decisions, tick rotation, state persistence,
teardown, and intent generation for paper trading.

Kitchen Loop iteration 120, VIB-1700.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def strategy():
    from strategies.incubating.aerodrome_lp_paper_trade_base.strategy import (
        AerodromeLPPaperTradeBaseStrategy,
    )

    strat = AerodromeLPPaperTradeBaseStrategy.__new__(AerodromeLPPaperTradeBaseStrategy)
    strat.config = {}
    strat._chain = "base"
    strat._wallet_address = "0x" + "0" * 40
    strat._strategy_id = "test-aerodrome-lp-paper-trade-base"
    strat.pool = "WETH/USDC"
    strat.token0 = "WETH"
    strat.token1 = "USDC"
    strat.stable = False
    strat.amount0 = Decimal("0.001")
    strat.amount1 = Decimal("3")
    strat.rebalance_threshold_pct = Decimal("2.0")
    strat.max_ticks_in_position = 5
    strat._has_position = False
    strat._lp_token_balance = Decimal("0")
    strat._ticks_in_position = 0
    strat._entry_price = Decimal("0")
    strat._total_opens = 0
    strat._total_closes = 0
    return strat


def _mock_market(
    token0_price: float = 3000.0,
    token1_price: float = 1.0,
    token0_balance: float = 1.0,
    token1_balance: float = 10000.0,
) -> MagicMock:
    market = MagicMock()

    def price_fn(token):
        if token == "WETH":
            return Decimal(str(token0_price))
        elif token == "USDC":
            return Decimal(str(token1_price))
        raise ValueError(f"Unknown token: {token}")

    market.price = MagicMock(side_effect=price_fn)

    def balance_fn(token):
        bal = MagicMock()
        if token == "WETH":
            bal.balance = Decimal(str(token0_balance))
        elif token == "USDC":
            bal.balance = Decimal(str(token1_balance))
        return bal

    market.balance = MagicMock(side_effect=balance_fn)
    return market


class TestDecision:
    def test_open_lp_when_no_position_and_has_funds(self, strategy):
        """Opens LP when no position exists and funds are available."""
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_OPEN"
        assert strategy._entry_price == Decimal("3000")

    def test_hold_when_no_position_and_insufficient_funds(self, strategy):
        """Holds when no position and insufficient funds."""
        market = _mock_market(token0_balance=0.0, token1_balance=0.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "insufficient funds" in intent.reason.lower()

    def test_hold_when_position_active_within_threshold(self, strategy):
        """Holds when position is active and price hasn't moved enough."""
        strategy._has_position = True
        strategy._entry_price = Decimal("3000")
        market = _mock_market(token0_price=3010.0)  # 0.33% move, below 2%
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "LP active" in intent.reason

    def test_close_on_price_rebalance(self, strategy):
        """Closes LP when price moves beyond rebalance threshold."""
        strategy._has_position = True
        strategy._entry_price = Decimal("3000")
        # 3% price move exceeds 2% threshold
        market = _mock_market(token0_price=3090.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_CLOSE"

    def test_close_on_max_ticks(self, strategy):
        """Closes LP when max ticks reached."""
        strategy._has_position = True
        strategy._entry_price = Decimal("3000")
        strategy._ticks_in_position = 4  # Will be incremented to 5 in decide()
        market = _mock_market(token0_price=3000.0)  # No price change
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_CLOSE"

    def test_hold_on_price_error(self, strategy):
        """Holds when price data is unavailable."""
        market = MagicMock()
        market.price = MagicMock(side_effect=ValueError("price unavailable"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_hold_on_zero_price(self, strategy):
        """Holds when price is zero."""
        market = _mock_market(token0_price=0.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_ticks_increment_each_decide(self, strategy):
        """Ticks counter increments with each decide while position active."""
        strategy._has_position = True
        strategy._entry_price = Decimal("3000")
        strategy.max_ticks_in_position = 100  # High to avoid rotation
        market = _mock_market()
        strategy.decide(market)
        assert strategy._ticks_in_position == 1
        strategy.decide(market)
        assert strategy._ticks_in_position == 2

    def test_rebalance_priority_over_max_ticks(self, strategy):
        """Price rebalance triggers before max ticks check."""
        strategy._has_position = True
        strategy._entry_price = Decimal("3000")
        strategy._ticks_in_position = 3
        strategy.max_ticks_in_position = 5
        market = _mock_market(token0_price=3100.0)  # 3.3% move
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_CLOSE"


class TestIntentCreation:
    def test_open_intent_pool_format(self, strategy):
        """LP_OPEN intent uses correct pool format."""
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.pool == "WETH/USDC/volatile"
        assert intent.protocol == "aerodrome"

    def test_close_intent_pool_format(self, strategy):
        """LP_CLOSE intent uses correct pool format."""
        strategy._has_position = True
        strategy._entry_price = Decimal("3000")
        strategy._ticks_in_position = 5
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.pool == "WETH/USDC/volatile"
        assert intent.protocol == "aerodrome"

    def test_stable_pool_type(self, strategy):
        """Stable pool uses 'stable' in pool string."""
        strategy.stable = True
        market = _mock_market()
        intent = strategy.decide(market)
        assert "stable" in intent.pool


class TestOnIntentExecuted:
    def test_lp_open_updates_state(self, strategy):
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "LP_OPEN"
        mock_result = MagicMock()
        mock_result.extracted_data = None

        strategy.on_intent_executed(mock_intent, True, mock_result)
        assert strategy._has_position is True
        assert strategy._total_opens == 1
        assert strategy._ticks_in_position == 0

    def test_lp_close_updates_state(self, strategy):
        strategy._has_position = True
        strategy._ticks_in_position = 3
        strategy._entry_price = Decimal("3000")

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "LP_CLOSE"

        strategy.on_intent_executed(mock_intent, True, MagicMock())
        assert strategy._has_position is False
        assert strategy._lp_token_balance == Decimal("0")
        assert strategy._entry_price == Decimal("0")
        assert strategy._total_closes == 1

    def test_no_update_on_failure(self, strategy):
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "LP_OPEN"
        strategy.on_intent_executed(mock_intent, False, MagicMock())
        assert strategy._has_position is False
        assert strategy._total_opens == 0

    def test_extracts_liquidity_from_result(self, strategy):
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "LP_OPEN"
        mock_result = MagicMock()
        mock_result.extracted_data = {"liquidity": "12345"}

        strategy.on_intent_executed(mock_intent, True, mock_result)
        assert strategy._lp_token_balance == Decimal("12345")


class TestStatePersistence:
    def test_get_persistent_state(self, strategy):
        strategy._has_position = True
        strategy._lp_token_balance = Decimal("100")
        strategy._ticks_in_position = 3
        strategy._entry_price = Decimal("3000")
        strategy._total_opens = 5
        strategy._total_closes = 4

        state = strategy.get_persistent_state()
        assert state["has_position"] is True
        assert state["lp_token_balance"] == "100"
        assert state["ticks_in_position"] == 3
        assert state["entry_price"] == "3000"
        assert state["total_opens"] == 5
        assert state["total_closes"] == 4

    def test_load_persistent_state(self, strategy):
        strategy.load_persistent_state({
            "has_position": True,
            "lp_token_balance": "200",
            "ticks_in_position": 2,
            "entry_price": "2800",
            "total_opens": 3,
            "total_closes": 2,
        })
        assert strategy._has_position is True
        assert strategy._lp_token_balance == Decimal("200")
        assert strategy._ticks_in_position == 2
        assert strategy._entry_price == Decimal("2800")
        assert strategy._total_opens == 3
        assert strategy._total_closes == 2

    def test_load_empty_state(self, strategy):
        strategy.load_persistent_state({})
        assert strategy._has_position is False
        assert strategy._total_opens == 0

    def test_lp_balance_implies_position(self, strategy):
        """If lp_token_balance > 0 in state, has_position is forced True."""
        strategy.load_persistent_state({
            "has_position": False,
            "lp_token_balance": "50",
        })
        assert strategy._has_position is True


class TestTeardown:
    def test_supports_teardown(self, strategy):
        assert strategy.supports_teardown() is True

    def test_no_positions_when_empty(self, strategy):
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_has_position_in_summary(self, strategy):
        strategy._has_position = True
        strategy._lp_token_balance = Decimal("100")
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].protocol == "aerodrome"

    def test_teardown_intents_when_position(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._has_position = True
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "LP_CLOSE"

    def test_no_teardown_when_empty(self, strategy):
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.HARD)
        assert intents == []


class TestStatus:
    def test_get_status(self, strategy):
        status = strategy.get_status()
        assert status["strategy"] == "aerodrome_lp_paper_trade_base"
        assert status["config"]["pool"] == "WETH/USDC"
        assert status["state"]["total_opens"] == 0
        assert status["state"]["total_closes"] == 0


class TestLifecycle:
    def test_full_open_close_cycle(self, strategy):
        """Simulates a full LP open -> hold -> close -> reopen cycle."""
        market = _mock_market()

        # Open
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_OPEN"

        # Simulate execution
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "LP_OPEN"
        mock_result = MagicMock()
        mock_result.extracted_data = None
        strategy.on_intent_executed(mock_intent, True, mock_result)

        # Hold for a few ticks
        for _ in range(3):
            intent = strategy.decide(market)
            assert intent.intent_type.value == "HOLD"

        # Force close via max ticks (2 more ticks to hit 5)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_CLOSE"

        # Simulate close execution
        mock_intent.intent_type.value = "LP_CLOSE"
        strategy.on_intent_executed(mock_intent, True, MagicMock())

        assert strategy._total_opens == 1
        assert strategy._total_closes == 1

        # Reopen
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_OPEN"
