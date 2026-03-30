"""Unit tests for BENQI Lending Lifecycle demo strategy state machine.

Validates the strategy's decide() logic and state transitions without
requiring Anvil or gateway. Tests the full lifecycle:
idle -> borrowing -> borrowed -> repaying -> repaid -> withdrawing -> complete
"""

from decimal import Decimal
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from almanak.demo_strategies.benqi_lending_lifecycle.strategy import BenqiLendingLifecycleStrategy


@pytest.fixture
def mock_market():
    """Create a mock MarketSnapshot with Avalanche prices."""
    market = MagicMock()
    market.price.side_effect = lambda token: {
        "USDC": Decimal("1.00"),
        "USDT": Decimal("1.00"),
        "AVAX": Decimal("25.00"),
        "WAVAX": Decimal("25.00"),
    }.get(token, Decimal("0"))
    return market


@pytest.fixture
def strategy():
    """Create a BenqiLendingLifecycleStrategy with mock config."""
    with (
        patch.object(BenqiLendingLifecycleStrategy, "chain", new_callable=PropertyMock, return_value="avalanche"),
        patch.object(BenqiLendingLifecycleStrategy, "get_config", side_effect=lambda k, d=None: {
            "collateral_token": "USDC",
            "collateral_amount": "500",
            "borrow_token": "USDT",
            "ltv_target": "0.2",
        }.get(k, d)),
        patch.object(BenqiLendingLifecycleStrategy, "STRATEGY_NAME", "benqi_lending_lifecycle"),
    ):
        s = BenqiLendingLifecycleStrategy.__new__(BenqiLendingLifecycleStrategy)
        s._chain = "avalanche"
        s.collateral_token = "USDC"
        s.collateral_amount = Decimal("500")
        s.borrow_token = "USDT"
        s.ltv_target = Decimal("0.2")
        s._loop_state = "idle"
        s._previous_stable_state = "idle"
        s._collateral_supplied = Decimal("0")
        s._borrowed_amount = Decimal("0")
        type(s).chain = PropertyMock(return_value="avalanche")
        type(s).STRATEGY_NAME = PropertyMock(return_value="benqi_lending_lifecycle")
        return s


class TestBenqiLifecycleStateMachine:
    """Test the full lending lifecycle state machine."""

    def test_idle_emits_borrow_intent(self, strategy, mock_market):
        """First iteration: idle -> borrowing, returns BorrowIntent."""
        intent = strategy.decide(mock_market)
        assert intent is not None
        assert intent.intent_type.value == "BORROW"
        assert strategy._loop_state == "borrowing"

    def test_borrow_success_transitions_to_borrowed(self, strategy, mock_market):
        """Successful borrow moves state to 'borrowed'."""
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._loop_state == "borrowed"
        assert strategy._collateral_supplied == Decimal("500")

    def test_borrowed_emits_repay_intent(self, strategy, mock_market):
        """After borrowing, next decide() returns RepayIntent."""
        # Borrow
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        # Repay
        intent = strategy.decide(mock_market)
        assert intent is not None
        assert intent.intent_type.value == "REPAY"
        assert strategy._loop_state == "repaying"

    def test_repay_success_transitions_to_repaid(self, strategy, mock_market):
        """Successful repay moves state to 'repaid'."""
        # Borrow -> success
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        # Repay -> success
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._loop_state == "repaid"
        assert strategy._borrowed_amount == Decimal("0")

    def test_repaid_emits_withdraw_intent(self, strategy, mock_market):
        """After repaying, next decide() returns WithdrawIntent."""
        # Borrow -> Repay -> Withdraw
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        intent = strategy.decide(mock_market)
        assert intent is not None
        assert intent.intent_type.value == "WITHDRAW"
        assert strategy._loop_state == "withdrawing"

    def test_full_lifecycle_completes(self, strategy, mock_market):
        """Full lifecycle: idle -> borrow -> repay -> withdraw -> complete."""
        # Step 1: Borrow
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        # Step 2: Repay
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        # Step 3: Withdraw
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)

        assert strategy._loop_state == "complete"
        assert strategy._collateral_supplied == Decimal("0")
        assert strategy._borrowed_amount == Decimal("0")

        # Step 4: Hold
        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "HOLD"
        assert "complete" in intent.reason.lower()

    def test_borrow_failure_reverts_to_idle(self, strategy, mock_market):
        """Failed borrow reverts state back to idle."""
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._loop_state == "idle"

    def test_persistent_state_round_trip(self, strategy, mock_market):
        """Persistent state survives save/load cycle."""
        # Advance to borrowed state
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)

        # Save state
        state = strategy.get_persistent_state()
        assert state["loop_state"] == "borrowed"

        # Load into fresh strategy
        strategy._loop_state = "idle"
        strategy._collateral_supplied = Decimal("0")
        strategy.load_persistent_state(state)

        assert strategy._loop_state == "borrowed"
        assert strategy._collateral_supplied == Decimal("500")

    def test_borrow_amount_calculation(self, strategy, mock_market):
        """Borrow amount = collateral_value * ltv_target / borrow_price."""
        intent = strategy.decide(mock_market)
        # 500 USDC * $1 * 0.2 / $1 (USDT) = 100 USDT
        assert intent.borrow_amount == Decimal("100.00")

    def test_status_report(self, strategy, mock_market):
        """get_status() returns current state info."""
        status = strategy.get_status()
        assert status["state"] == "idle"
        assert status["protocol"] == "benqi"
        assert status["chain"] == "avalanche"
