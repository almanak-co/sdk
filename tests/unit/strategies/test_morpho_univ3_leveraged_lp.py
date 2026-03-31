"""Unit tests for Morpho Blue + Uniswap V3 Leveraged LP strategy (VIB-2125).

Tests the state machine logic, intent generation, and teardown sequence.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.demo_strategies.morpho_univ3_leveraged_lp.strategy import (
    MorphoUniV3LeveragedLPStrategy,
)
from almanak.framework.intents.vocabulary import (
    BorrowIntent,
    HoldIntent,
    IntentType,
    LPCloseIntent,
    LPOpenIntent,
    RepayIntent,
    SupplyIntent,
    SwapIntent,
    WithdrawIntent,
)


@pytest.fixture()
def strategy():
    """Create strategy with mock gateway."""
    with patch(
        "almanak.framework.strategies.intent_strategy.IntentStrategy.__init__",
        return_value=None,
    ):
        s = MorphoUniV3LeveragedLPStrategy.__new__(MorphoUniV3LeveragedLPStrategy)
        # Set attributes that __init__ would set
        s._strategy_id = "test_morpho_univ3_lp"
        s.name = "demo_morpho_univ3_leveraged_lp"
        s._chain = "ethereum"
        s._config = {
            "market_id": "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc",
            "collateral_token": "wstETH",
            "borrow_token": "USDC",
            "collateral_amount": "0.014",
            "target_ltv": "0.50",
            "min_health_factor": "1.5",
            "lp_pool": "WETH/USDC/500",
            "lp_range_width_pct": "0.20",
            "swap_slippage": "0.005",
            "force_action": "",
        }
        s.get_config = lambda key, default=None: s._config.get(key, default)

        # Initialize state
        s.market_id = s._config["market_id"]
        s.collateral_token = "wstETH"
        s.borrow_token = "USDC"
        s.collateral_amount = Decimal("0.014")
        s.target_ltv = Decimal("0.50")
        s.min_health_factor = Decimal("1.5")
        s.lp_pool = "WETH/USDC/500"
        s.lp_range_width_pct = Decimal("0.20")
        s.swap_slippage = Decimal("0.005")
        s._state = "idle"
        s._collateral_supplied = Decimal("0")
        s._borrowed_amount = Decimal("0")
        s._lp_position_id = None
        s._force_action = ""

        return s


@pytest.fixture()
def market():
    """Create a mock MarketSnapshot."""
    m = MagicMock()
    m.price.side_effect = lambda token: {
        "wstETH": Decimal("3800"),
        "WETH": Decimal("3400"),
        "USDC": Decimal("1"),
    }.get(token, Decimal("0"))
    return m


class TestStateMachine:
    """Test the strategy state machine transitions."""

    def test_idle_emits_supply(self, strategy, market):
        intent = strategy.decide(market)
        assert isinstance(intent, SupplyIntent)
        assert intent.token == "wstETH"
        assert intent.amount == Decimal("0.014")
        assert strategy._state == "supplying"

    def test_supplying_holds(self, strategy, market):
        strategy._state = "supplying"
        intent = strategy.decide(market)
        assert isinstance(intent, HoldIntent)

    def test_supplied_emits_borrow(self, strategy, market):
        strategy._state = "supplied"
        intent = strategy.decide(market)
        assert isinstance(intent, BorrowIntent)
        assert intent.borrow_token == "USDC"
        # 0.014 wstETH * $3800 * 0.50 LTV = $26.60
        assert intent.borrow_amount == Decimal("26.60")
        assert strategy._state == "borrowing"

    def test_borrow_capped_by_min_health_factor(self, strategy, market):
        """When target_ltv would breach min_health_factor, borrow is capped."""
        strategy._state = "supplied"
        strategy.target_ltv = Decimal("0.90")  # Aggressive LTV
        strategy.min_health_factor = Decimal("1.5")  # Conservative HF
        intent = strategy.decide(market)
        assert isinstance(intent, BorrowIntent)
        # Without cap: 0.014 * 3800 * 0.90 = $47.88
        # With cap: 0.014 * 3800 / 1.5 = $35.46 (rounded down to 0.01)
        assert intent.borrow_amount == Decimal("35.46")

    def test_borrowed_emits_lp_open(self, strategy, market):
        strategy._state = "borrowed"
        strategy._borrowed_amount = Decimal("26.60")
        intent = strategy.decide(market)
        assert isinstance(intent, LPOpenIntent)
        assert intent.pool == "WETH/USDC/500"
        assert intent.protocol == "uniswap_v3"
        assert strategy._state == "opening_lp"

    def test_active_holds(self, strategy, market):
        strategy._state = "active"
        intent = strategy.decide(market)
        assert isinstance(intent, HoldIntent)


class TestOnIntentExecuted:
    """Test state transitions on intent execution."""

    def test_supply_success_advances_to_supplied(self, strategy):
        strategy._state = "supplying"
        strategy.on_intent_executed(MagicMock(), success=True, result=MagicMock())
        assert strategy._state == "supplied"
        assert strategy._collateral_supplied == Decimal("0.014")

    def test_borrow_success_advances_to_borrowed(self, strategy):
        strategy._state = "borrowing"
        mock_intent = MagicMock()
        mock_intent.borrow_amount = Decimal("26.60")
        strategy.on_intent_executed(mock_intent, success=True, result=MagicMock())
        assert strategy._state == "borrowed"
        assert strategy._borrowed_amount == Decimal("26.60")

    def test_lp_open_success_advances_to_active(self, strategy):
        strategy._state = "opening_lp"
        mock_result = MagicMock()
        mock_result.position_id = 12345
        strategy.on_intent_executed(MagicMock(), success=True, result=mock_result)
        assert strategy._state == "active"
        assert strategy._lp_position_id == 12345

    def test_lp_open_no_position_id_advances_to_active(self, strategy):
        """LP open without position_id moves to active to prevent duplicate LP opens."""
        strategy._state = "opening_lp"
        mock_result = MagicMock()
        mock_result.position_id = None
        strategy.on_intent_executed(MagicMock(), success=True, result=mock_result)
        assert strategy._state == "active"
        assert strategy._lp_position_id is None

    def test_supply_failure_reverts_to_idle(self, strategy):
        strategy._state = "supplying"
        strategy.on_intent_executed(MagicMock(), success=False, result=MagicMock())
        assert strategy._state == "idle"

    def test_borrow_failure_reverts_to_supplied(self, strategy):
        strategy._state = "borrowing"
        strategy.on_intent_executed(MagicMock(), success=False, result=MagicMock())
        assert strategy._state == "supplied"

    def test_lp_failure_reverts_to_borrowed(self, strategy):
        strategy._state = "opening_lp"
        strategy.on_intent_executed(MagicMock(), success=False, result=MagicMock())
        assert strategy._state == "borrowed"


class TestTeardown:
    """Test teardown intent generation."""

    def test_full_teardown_order(self, strategy):
        """Teardown should close LP -> swap WETH -> repay -> withdraw."""
        from almanak.framework.teardown import TeardownMode

        strategy._lp_position_id = 12345
        strategy._borrowed_amount = Decimal("26.60")
        strategy._collateral_supplied = Decimal("0.014")

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

        assert len(intents) == 4
        # 1: Close LP
        assert isinstance(intents[0], LPCloseIntent)
        assert intents[0].position_id == "12345"
        # 2: Swap WETH to USDC
        assert isinstance(intents[1], SwapIntent)
        assert intents[1].from_token == "WETH"
        assert intents[1].to_token == "USDC"
        # 3: Repay
        assert isinstance(intents[2], RepayIntent)
        assert intents[2].token == "USDC"
        # 4: Withdraw collateral
        assert isinstance(intents[3], WithdrawIntent)
        assert intents[3].token == "wstETH"

    def test_teardown_no_lp_position(self, strategy):
        """Teardown without LP position skips LP close and swap."""
        from almanak.framework.teardown import TeardownMode

        strategy._lp_position_id = None
        strategy._borrowed_amount = Decimal("26.60")
        strategy._collateral_supplied = Decimal("0.014")

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

        # No LP close or swap, just: repay + withdraw
        assert len(intents) == 2
        assert isinstance(intents[0], RepayIntent)
        assert isinstance(intents[1], WithdrawIntent)

    def test_teardown_hard_mode_higher_slippage(self, strategy):
        """HARD mode uses higher slippage for swaps."""
        from almanak.framework.teardown import TeardownMode

        strategy._lp_position_id = 12345
        strategy._borrowed_amount = Decimal("26.60")
        strategy._collateral_supplied = Decimal("0.014")

        intents = strategy.generate_teardown_intents(TeardownMode.HARD)
        # intents[0] is LP close, intents[1] is swap
        swap = intents[1]
        assert isinstance(swap, SwapIntent)
        assert swap.max_slippage == Decimal("0.03")  # 3% for HARD

    def test_get_open_positions(self, strategy):
        """Positions summary includes both LP and lending positions."""
        mock_market = MagicMock()
        mock_market.price.return_value = Decimal("3800")
        strategy.create_market_snapshot = MagicMock(return_value=mock_market)
        strategy._lp_position_id = 12345
        strategy._collateral_supplied = Decimal("0.014")
        strategy._borrowed_amount = Decimal("26.60")

        summary = strategy.get_open_positions()
        assert len(summary.positions) == 3
        assert summary.positions[0].protocol == "uniswap_v3"
        assert summary.positions[0].value_usd == Decimal("53.20")  # 26.60 * 2
        assert summary.positions[1].protocol == "morpho_blue"
        assert summary.positions[1].value_usd == Decimal("53.2")  # 0.014 * 3800
        # Borrow position
        assert summary.positions[2].protocol == "morpho_blue"
        assert summary.positions[2].value_usd == Decimal("26.60")


class TestForceAction:
    """Test force_action config override for testing."""

    def test_force_supply(self, strategy, market):
        strategy._force_action = "supply"
        intent = strategy.decide(market)
        assert isinstance(intent, SupplyIntent)
        assert strategy._force_action == ""  # Cleared after use

    def test_force_borrow(self, strategy, market):
        strategy._force_action = "borrow"
        intent = strategy.decide(market)
        assert isinstance(intent, BorrowIntent)

    def test_force_lp_open_with_borrow(self, strategy, market):
        strategy._force_action = "lp_open"
        strategy._borrowed_amount = Decimal("26.60")
        intent = strategy.decide(market)
        assert isinstance(intent, LPOpenIntent)

    def test_force_lp_open_without_borrow_holds(self, strategy, market):
        """LP open without borrowed amount fails closed with a hold."""
        strategy._force_action = "lp_open"
        strategy._borrowed_amount = Decimal("0")
        intent = strategy.decide(market)
        assert isinstance(intent, HoldIntent)


class TestStatePersistence:
    """Test state persistence round-trip."""

    def test_get_persistent_state(self, strategy):
        strategy._state = "active"
        strategy._collateral_supplied = Decimal("0.014")
        strategy._borrowed_amount = Decimal("26.60")
        strategy._lp_position_id = 12345

        state = strategy.get_persistent_state()
        assert state["state"] == "active"
        assert state["collateral_supplied"] == "0.014"
        assert state["borrowed_amount"] == "26.60"
        assert state["lp_position_id"] == 12345

    def test_load_persistent_state(self, strategy):
        state = {
            "state": "active",
            "collateral_supplied": "0.014",
            "borrowed_amount": "26.60",
            "lp_position_id": 12345,
        }
        strategy.load_persistent_state(state)
        assert strategy._state == "active"
        assert strategy._collateral_supplied == Decimal("0.014")
        assert strategy._borrowed_amount == Decimal("26.60")
        assert strategy._lp_position_id == 12345
