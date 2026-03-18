"""Unit tests for Ethena sUSDe Yield strategy PnL backtest compatibility.

Validates:
1. Multi-iteration decision sequences (swap -> stake -> hold cycle)
2. State persistence across backtest iterations
3. Intent structure compatibility with PnL backtester
4. Staking protocol intent metadata (protocol, chain, token_in)
5. Edge cases: insufficient funds, oracle failures, already-staked guard

Part of VIB-1458: PnL Backtest Ethena sUSDe Yield Strategy on Ethereum.
"""

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helper: lightweight strategy factory
# ---------------------------------------------------------------------------


def _make_strategy(
    min_stake_amount: str = "100",
    swap_usdc_to_usde: bool = False,
    min_usdc_amount: str = "100",
    max_slippage_pct: float = 0.5,
    force_action: str = "",
) -> Any:
    """Create an EthenaYieldStrategy with mocked framework wiring."""
    from strategies.demo.ethena_yield.strategy import EthenaYieldStrategy

    with patch.object(EthenaYieldStrategy, "__init__", lambda self, *a, **kw: None):
        strat = EthenaYieldStrategy.__new__(EthenaYieldStrategy)

    strat.min_stake_amount = Decimal(min_stake_amount)
    strat.swap_usdc_to_usde = swap_usdc_to_usde
    strat.min_usdc_amount = Decimal(min_usdc_amount)
    strat.max_slippage_pct = max_slippage_pct
    strat.force_action = force_action.lower()
    strat._chain = "ethereum"
    strat._strategy_id = "test_ethena_pnl"
    strat._wallet_address = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
    strat._swapped = False
    strat._swapped_amount = Decimal("0")
    strat._staked = False
    strat._staked_amount = Decimal("0")
    return strat


def _mock_market(
    usde_balance: Decimal = Decimal("0"),
    usdc_balance: Decimal = Decimal("0"),
    usde_unavailable: bool = False,
    usdc_unavailable: bool = False,
) -> MagicMock:
    """Create a mock MarketSnapshot for Ethena backtesting."""
    market = MagicMock()

    def balance_fn(symbol: str) -> Any:
        if symbol == "USDe":
            if usde_unavailable:
                raise ValueError("USDe balance unavailable")
            bal = MagicMock()
            bal.balance = usde_balance
            bal.balance_usd = usde_balance
            return bal
        elif symbol == "USDC":
            if usdc_unavailable:
                raise ValueError("USDC balance unavailable")
            bal = MagicMock()
            bal.balance = usdc_balance
            bal.balance_usd = usdc_balance
            return bal
        raise ValueError(f"No balance for {symbol}")

    market.balance = balance_fn
    return market


# ===========================================================================
# Multi-Iteration Backtest Sequence
# ===========================================================================


class TestMultiIterationSequence:
    """Test the full swap -> stake -> hold lifecycle across backtest iterations."""

    def test_swap_then_stake_then_hold_sequence(self):
        """Full 3-step lifecycle: USDC->USDe swap, USDe->sUSDe stake, hold."""
        strat = _make_strategy(swap_usdc_to_usde=True, min_usdc_amount="100", min_stake_amount="100")

        # Iteration 1: Has USDC, no USDe -> should SWAP
        market1 = _mock_market(usde_balance=Decimal("0"), usdc_balance=Decimal("500"))
        intent1 = strat.decide(market1)
        assert intent1.intent_type.value == "SWAP"
        assert intent1.from_token == "USDC"
        assert intent1.to_token == "USDe"
        assert intent1.protocol == "enso"

        # Simulate swap success
        strat.on_intent_executed(intent1, success=True, result=MagicMock())
        assert strat._swapped is True

        # Iteration 2: Now has USDe from swap -> should STAKE
        market2 = _mock_market(usde_balance=Decimal("490"), usdc_balance=Decimal("0"))
        intent2 = strat.decide(market2)
        assert intent2.intent_type.value == "STAKE"
        assert intent2.protocol == "ethena"
        assert intent2.token_in == "USDe"

        # Simulate stake success
        strat.on_intent_executed(intent2, success=True, result=MagicMock())
        assert strat._staked is True

        # Iteration 3+: Already staked -> should HOLD
        market3 = _mock_market(usde_balance=Decimal("0"), usdc_balance=Decimal("0"))
        intent3 = strat.decide(market3)
        assert intent3.intent_type.value == "HOLD"
        assert "Already staked" in intent3.reason

    def test_direct_stake_without_swap(self):
        """Direct USDe staking without USDC swap (swap disabled)."""
        strat = _make_strategy(swap_usdc_to_usde=False, min_stake_amount="100")

        # Has USDe directly -> should STAKE
        market = _mock_market(usde_balance=Decimal("1000"))
        intent = strat.decide(market)
        assert intent.intent_type.value == "STAKE"
        assert intent.amount == Decimal("1000")

        # Simulate stake success -> next iteration should HOLD
        strat.on_intent_executed(intent, success=True, result=MagicMock())
        intent2 = strat.decide(market)
        assert intent2.intent_type.value == "HOLD"

    def test_hold_until_balance_sufficient(self):
        """Strategy should hold across multiple iterations until balance grows."""
        strat = _make_strategy(min_stake_amount="500")

        # Iteration 1: Insufficient USDe
        market1 = _mock_market(usde_balance=Decimal("100"))
        intent1 = strat.decide(market1)
        assert intent1.intent_type.value == "HOLD"

        # Iteration 2: Still insufficient
        market2 = _mock_market(usde_balance=Decimal("300"))
        intent2 = strat.decide(market2)
        assert intent2.intent_type.value == "HOLD"

        # Iteration 3: Now sufficient
        market3 = _mock_market(usde_balance=Decimal("500"))
        intent3 = strat.decide(market3)
        assert intent3.intent_type.value == "STAKE"
        assert intent3.amount == Decimal("500")


# ===========================================================================
# Intent Metadata for PnL Tracking
# ===========================================================================


class TestIntentMetadata:
    """Test intent structure is compatible with PnL backtester."""

    def test_stake_intent_has_required_fields(self):
        """StakeIntent must have protocol, token_in, amount, chain for PnL tracking."""
        strat = _make_strategy()
        market = _mock_market(usde_balance=Decimal("1000"))
        intent = strat.decide(market)

        assert intent.intent_type.value == "STAKE"
        assert intent.protocol == "ethena"
        assert intent.token_in == "USDe"
        assert intent.amount == Decimal("1000")
        assert intent.chain == "ethereum"
        assert intent.receive_wrapped is False

    def test_swap_intent_has_required_fields(self):
        """SwapIntent must have from_token, to_token, amount, max_slippage for PnL tracking."""
        strat = _make_strategy(swap_usdc_to_usde=True)
        market = _mock_market(usde_balance=Decimal("0"), usdc_balance=Decimal("500"))
        intent = strat.decide(market)

        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDC"
        assert intent.to_token == "USDe"
        assert intent.amount == Decimal("500")
        assert intent.max_slippage == Decimal("0.005")  # 0.5% default
        assert intent.protocol == "enso"
        assert intent.chain == "ethereum"

    def test_hold_intent_has_reason(self):
        """HoldIntent must have reason string for PnL journal entries."""
        strat = _make_strategy()
        market = _mock_market(usde_balance=Decimal("0"))
        intent = strat.decide(market)

        assert intent.intent_type.value == "HOLD"
        assert isinstance(intent.reason, str)
        assert len(intent.reason) > 0


# ===========================================================================
# State Persistence for Backtest Crash Recovery
# ===========================================================================


class TestStatePersistence:
    """Test state save/restore for backtest checkpoint recovery."""

    def test_roundtrip_fresh_state(self):
        """Fresh strategy state should roundtrip correctly."""
        strat = _make_strategy()
        state = strat.get_persistent_state()

        strat2 = _make_strategy()
        strat2.load_persistent_state(state)

        assert strat2._swapped == strat._swapped
        assert strat2._staked == strat._staked
        assert strat2._swapped_amount == strat._swapped_amount
        assert strat2._staked_amount == strat._staked_amount

    def test_roundtrip_mid_execution_state(self):
        """Mid-execution state (swapped, not staked) should restore correctly."""
        strat = _make_strategy()
        strat._swapped = True
        strat._swapped_amount = Decimal("500")

        state = strat.get_persistent_state()

        strat2 = _make_strategy()
        strat2.load_persistent_state(state)

        assert strat2._swapped is True
        assert strat2._swapped_amount == Decimal("500")
        assert strat2._staked is False

    def test_roundtrip_completed_state(self):
        """Completed state (swapped + staked) should restore correctly."""
        strat = _make_strategy()
        strat._swapped = True
        strat._swapped_amount = Decimal("500")
        strat._staked = True
        strat._staked_amount = Decimal("490")

        state = strat.get_persistent_state()

        strat2 = _make_strategy()
        strat2.load_persistent_state(state)

        assert strat2._swapped is True
        assert strat2._staked is True
        assert strat2._staked_amount == Decimal("490")

    def test_restored_state_produces_correct_decision(self):
        """Strategy restored from staked state should HOLD."""
        strat = _make_strategy()
        strat.load_persistent_state({"staked": True, "staked_amount": "1000"})

        market = _mock_market(usde_balance=Decimal("0"))
        intent = strat.decide(market)

        assert intent.intent_type.value == "HOLD"
        assert "Already staked" in intent.reason


# ===========================================================================
# Failed Execution Recovery
# ===========================================================================


class TestFailedExecution:
    """Test strategy behavior after failed intents (backtest retry scenarios)."""

    def test_swap_failure_allows_retry(self):
        """Failed swap should not mark as swapped -- allows retry in next iteration."""
        strat = _make_strategy(swap_usdc_to_usde=True)

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"
        mock_intent.amount = Decimal("500")

        strat.on_intent_executed(mock_intent, success=False, result=None)

        assert strat._swapped is False
        assert strat._swapped_amount == Decimal("0")

    def test_stake_failure_allows_retry(self):
        """Failed stake should not mark as staked -- allows retry in next iteration."""
        strat = _make_strategy()

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "STAKE"
        mock_intent.amount = Decimal("1000")

        strat.on_intent_executed(mock_intent, success=False, result=None)

        assert strat._staked is False
        assert strat._staked_amount == Decimal("0")

    def test_retry_after_swap_failure(self):
        """After swap failure, strategy should retry swap on next iteration."""
        strat = _make_strategy(swap_usdc_to_usde=True)

        # First attempt: swap
        market = _mock_market(usde_balance=Decimal("0"), usdc_balance=Decimal("500"))
        intent1 = strat.decide(market)
        assert intent1.intent_type.value == "SWAP"

        # Simulate failure
        strat.on_intent_executed(intent1, success=False, result=None)

        # Second attempt: should retry swap (not stale state)
        intent2 = strat.decide(market)
        assert intent2.intent_type.value == "SWAP"


# ===========================================================================
# Oracle / Data Edge Cases
# ===========================================================================


class TestDataEdgeCases:
    """Test graceful handling of data issues during PnL backtesting."""

    def test_usde_balance_unavailable(self):
        """Should hold when USDe balance oracle fails."""
        strat = _make_strategy()
        market = _mock_market(usde_unavailable=True)
        intent = strat.decide(market)

        assert intent.intent_type.value == "HOLD"

    def test_usdc_balance_unavailable_with_swap_enabled(self):
        """Should hold when USDC balance unavailable and swap enabled."""
        strat = _make_strategy(swap_usdc_to_usde=True)
        market = _mock_market(usde_balance=Decimal("0"), usdc_unavailable=True)
        intent = strat.decide(market)

        assert intent.intent_type.value == "HOLD"

    def test_zero_balance(self):
        """Should hold with zero balances."""
        strat = _make_strategy()
        market = _mock_market(usde_balance=Decimal("0"))
        intent = strat.decide(market)

        assert intent.intent_type.value == "HOLD"

    def test_exact_minimum_stake_amount(self):
        """Exact minimum stake amount should trigger stake."""
        strat = _make_strategy(min_stake_amount="100")
        market = _mock_market(usde_balance=Decimal("100"))
        intent = strat.decide(market)

        assert intent.intent_type.value == "STAKE"
        assert intent.amount == Decimal("100")

    def test_just_below_minimum_stake(self):
        """One unit below minimum should hold."""
        strat = _make_strategy(min_stake_amount="100")
        market = _mock_market(usde_balance=Decimal("99.99"))
        intent = strat.decide(market)

        assert intent.intent_type.value == "HOLD"


# ===========================================================================
# Teardown for PnL Cleanup
# ===========================================================================


class TestTeardownPnL:
    """Test teardown works for PnL backtest position cleanup."""

    def test_get_open_positions_returns_summary(self):
        """Should return a TeardownPositionSummary (even if empty)."""
        strat = _make_strategy()
        positions = strat.get_open_positions()
        assert positions is not None

    def test_generate_teardown_intents_returns_list(self):
        """Should return a list of intents (even if empty)."""
        strat = _make_strategy()
        intents = strat.generate_teardown_intents()
        assert isinstance(intents, list)


# ===========================================================================
# Slippage Configuration for Backtest Accuracy
# ===========================================================================


class TestSlippageBacktest:
    """Test slippage config affects PnL calculation accuracy."""

    def test_default_slippage_half_percent(self):
        """Default 0.5% slippage should be Decimal('0.005')."""
        strat = _make_strategy(swap_usdc_to_usde=True, max_slippage_pct=0.5)
        market = _mock_market(usde_balance=Decimal("0"), usdc_balance=Decimal("500"))
        intent = strat.decide(market)

        assert intent.max_slippage == Decimal("0.005")

    def test_custom_slippage_1_percent(self):
        """Custom 1% slippage should be Decimal('0.01')."""
        strat = _make_strategy(swap_usdc_to_usde=True, max_slippage_pct=1.0)
        market = _mock_market(usde_balance=Decimal("0"), usdc_balance=Decimal("500"))
        intent = strat.decide(market)

        assert intent.max_slippage == Decimal("0.01")

    def test_custom_slippage_point_1_percent(self):
        """Custom 0.1% slippage should be Decimal('0.001')."""
        strat = _make_strategy(swap_usdc_to_usde=True, max_slippage_pct=0.1)
        market = _mock_market(usde_balance=Decimal("0"), usdc_balance=Decimal("500"))
        intent = strat.decide(market)

        assert intent.max_slippage == Decimal("0.001")
