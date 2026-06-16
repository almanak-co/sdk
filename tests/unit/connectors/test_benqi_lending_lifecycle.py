"""Unit tests for BENQI Lending Lifecycle demo strategy state machine.

Validates the strategy's decide() logic and state transitions without
requiring Anvil or gateway. Tests the full lifecycle:
idle -> supplying -> supplied -> borrowing -> borrowed -> repaying -> repaid
     -> withdrawing -> complete

Regression coverage for VIB-3586: the strategy emits a *standalone* SUPPLY
intent for the collateral leg before the BORROW intent, rather than bundling
the collateral into ``Intent.borrow(collateral_amount>0)``. Bundling collapses
the supply into the single BORROW accounting event (one ``transaction_ledger``
row -> one ``accounting_events`` row), silently dropping the SUPPLY
``accounting_events`` row and its ``supply:`` FIFO lot. A fail-closed guard now
rejects ``Intent.borrow(collateral_amount > 0)``.
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

    def test_idle_emits_supply_intent(self, strategy, mock_market):
        """VIB-3586: first iteration emits SUPPLY first, not a bundled BORROW."""
        intent = strategy.decide(mock_market)
        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert intent.use_as_collateral is True
        assert strategy._loop_state == "supplying"

    def test_supply_success_transitions_to_supplied(self, strategy, mock_market):
        """Successful supply moves state to 'supplied' and records collateral."""
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._loop_state == "supplied"
        assert strategy._collateral_supplied == Decimal("500")

    def test_supplied_emits_borrow_intent(self, strategy, mock_market):
        """After supplying, next decide() returns a BorrowIntent."""
        # Supply
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        # Borrow
        intent = strategy.decide(mock_market)
        assert intent is not None
        assert intent.intent_type.value == "BORROW"
        assert strategy._loop_state == "borrowing"

    def test_borrow_does_not_rebundle_collateral(self, strategy, mock_market):
        """VIB-3586 core regression: BORROW carries collateral_amount == 0."""
        # Supply
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        # Borrow
        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "BORROW"
        assert intent.collateral_token == "USDC"
        assert intent.borrow_token == "USDT"
        assert intent.collateral_amount == Decimal("0")

    def test_borrow_success_transitions_to_borrowed(self, strategy, mock_market):
        """Successful borrow moves state to 'borrowed'."""
        # Supply -> Borrow
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._loop_state == "borrowed"
        # Collateral tracked by the SUPPLY leg, not the borrow.
        assert strategy._collateral_supplied == Decimal("500")

    def test_borrowed_emits_repay_intent(self, strategy, mock_market):
        """After borrowing, next decide() returns RepayIntent."""
        # Supply -> Borrow
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        # Repay
        intent = strategy.decide(mock_market)
        assert intent is not None
        assert intent.intent_type.value == "REPAY"
        assert strategy._loop_state == "repaying"

    def test_repay_success_transitions_to_repaid(self, strategy, mock_market):
        """Successful repay moves state to 'repaid'."""
        # Supply -> Borrow -> Repay -> success
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._loop_state == "repaid"
        assert strategy._borrowed_amount == Decimal("0")

    def test_repaid_emits_withdraw_intent(self, strategy, mock_market):
        """After repaying, next decide() returns WithdrawIntent."""
        # Supply -> Borrow -> Repay -> Withdraw
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        intent = strategy.decide(mock_market)
        assert intent is not None
        assert intent.intent_type.value == "WITHDRAW"
        assert strategy._loop_state == "withdrawing"

    def test_full_lifecycle_completes(self, strategy, mock_market):
        """Full lifecycle: idle -> supply -> borrow -> repay -> withdraw -> complete."""
        # Step 1: Supply
        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "SUPPLY"
        strategy.on_intent_executed(intent, success=True, result=None)
        # Step 2: Borrow
        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "BORROW"
        strategy.on_intent_executed(intent, success=True, result=None)
        # Step 3: Repay
        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "REPAY"
        strategy.on_intent_executed(intent, success=True, result=None)
        # Step 4: Withdraw
        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "WITHDRAW"
        strategy.on_intent_executed(intent, success=True, result=None)

        assert strategy._loop_state == "complete"
        assert strategy._collateral_supplied == Decimal("0")
        assert strategy._borrowed_amount == Decimal("0")

        # Step 5: Hold
        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "HOLD"
        assert "complete" in intent.reason.lower()

    def test_supply_failure_reverts_to_idle(self, strategy, mock_market):
        """Failed supply reverts state back to idle."""
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._loop_state == "idle"

    def test_borrow_failure_reverts_to_supplied(self, strategy, mock_market):
        """Failed borrow reverts state back to supplied (collateral already on-chain)."""
        # Supply
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        # Borrow -> fail
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._loop_state == "supplied"

    def test_persistent_state_round_trip(self, strategy, mock_market):
        """Persistent state survives save/load cycle."""
        # Advance to supplied state
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)

        # Save state
        state = strategy.get_persistent_state()
        assert state["loop_state"] == "supplied"

        # Load into fresh strategy
        strategy._loop_state = "idle"
        strategy._collateral_supplied = Decimal("0")
        strategy.load_persistent_state(state)

        assert strategy._loop_state == "supplied"
        assert strategy._collateral_supplied == Decimal("500")

    def test_supply_intent_params(self, strategy, mock_market):
        """SUPPLY intent carries the configured collateral leg."""
        intent = strategy.decide(mock_market)
        assert intent.protocol == "benqi"
        assert intent.token == "USDC"
        assert intent.amount == Decimal("500")
        assert intent.use_as_collateral is True
        assert intent.chain == "avalanche"

    def test_supply_tracks_executed_amount_not_config(self, strategy, mock_market):
        """VIB-3586: _collateral_supplied comes from the executed intent's
        amount, so a config drift mid-flight does not corrupt it."""
        intent = strategy.decide(mock_market)
        strategy.collateral_amount = Decimal("999")  # config drifts after emit
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._collateral_supplied == Decimal("500")

    def test_borrow_amount_calculation(self, strategy, mock_market):
        """Borrow amount = collateral_value * ltv_target / borrow_price."""
        # Supply first, then borrow.
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        intent = strategy.decide(mock_market)
        # 500 USDC * $1 * 0.2 / $1 (USDT) = 100 USDT
        assert intent.borrow_amount == Decimal("100.00")

    def test_status_report(self, strategy, mock_market):
        """get_status() returns current state info."""
        status = strategy.get_status()
        assert status["state"] == "idle"
        assert status["protocol"] == "benqi"
        assert status["chain"] == "avalanche"
