"""Unit tests for CompoundPaperTradeStrategy.

Validates:
1. Strategy decision logic (supply/withdraw/hold based on price thresholds)
2. State transitions after intent execution
3. Teardown support
4. Paper trading compatibility (PnL tracking with lending intents)

Part of VIB-667: Paper Trade Compound V3 Lending Strategy on Base.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helper: lightweight strategy factory (avoids full framework bootstrap)
# ---------------------------------------------------------------------------

def _make_strategy(
    supply_token: str = "USDC",
    supply_amount: str = "100",
    market: str = "usdc",
    chain: str = "base",
) -> Any:
    """Create a CompoundPaperTradeStrategy with mocked framework wiring."""
    from strategies.demo.compound_paper_trade.strategy import CompoundPaperTradeStrategy

    with patch.object(CompoundPaperTradeStrategy, "__init__", lambda self, *a, **kw: None):
        strat = CompoundPaperTradeStrategy.__new__(CompoundPaperTradeStrategy)

    # Wire up the fields that __init__ normally sets
    strat.supply_token = supply_token
    strat.supply_amount = Decimal(supply_amount)
    strat.market = market
    strat._chain = chain
    strat._strategy_id = "test_compound_paper"
    strat._wallet_address = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
    strat.price_supply_above = Decimal("2000")
    strat.price_withdraw_below = Decimal("1500")
    strat._has_supply = False
    strat._supplied_amount = Decimal("0")
    strat._ticks_with_supply = 0
    return strat


def _mock_market(eth_price: Decimal = Decimal("2500"), usdc_balance: Decimal = Decimal("10000")) -> MagicMock:
    """Create a mock MarketSnapshot."""
    market = MagicMock()

    def price_fn(symbol: str) -> Decimal:
        prices = {"ETH": eth_price, "USDC": Decimal("1")}
        if symbol in prices:
            return prices[symbol]
        raise ValueError(f"No price for {symbol}")

    market.price = price_fn

    balance_obj = MagicMock()
    balance_obj.balance = usdc_balance
    balance_obj.balance_usd = usdc_balance

    def balance_fn(symbol: str) -> Any:
        if symbol == "USDC":
            return balance_obj
        raise ValueError(f"No balance for {symbol}")

    market.balance = balance_fn
    return market


# ===========================================================================
# Decision Logic Tests
# ===========================================================================


class TestDecisionLogic:
    """Test supply/withdraw/hold decision thresholds."""

    def test_supply_when_eth_above_threshold_and_has_funds(self):
        """Should supply when ETH > $2000 and has USDC."""
        strat = _make_strategy()
        market = _mock_market(eth_price=Decimal("2500"), usdc_balance=Decimal("10000"))
        intent = strat.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert intent.token == "USDC"
        assert intent.amount == Decimal("100")

    def test_hold_when_eth_below_threshold(self):
        """Should hold when ETH < $2000 and no supply."""
        strat = _make_strategy()
        market = _mock_market(eth_price=Decimal("1800"))
        intent = strat.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "HOLD"

    def test_hold_when_insufficient_funds(self):
        """Should hold when balance < supply amount."""
        strat = _make_strategy(supply_amount="100")
        market = _mock_market(eth_price=Decimal("2500"), usdc_balance=Decimal("50"))
        intent = strat.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "HOLD"

    def test_withdraw_when_eth_drops_below_threshold(self):
        """Should withdraw when ETH < $1500 and has supply."""
        strat = _make_strategy()
        strat._has_supply = True
        strat._supplied_amount = Decimal("100")
        market = _mock_market(eth_price=Decimal("1400"))
        intent = strat.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "WITHDRAW"

    def test_hold_when_supply_active_and_price_normal(self):
        """Should hold when supply is active and ETH is in normal range."""
        strat = _make_strategy()
        strat._has_supply = True
        strat._supplied_amount = Decimal("100")
        market = _mock_market(eth_price=Decimal("1800"))
        intent = strat.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "HOLD"

    def test_hold_increments_ticks(self):
        """Ticks with supply should increment on hold."""
        strat = _make_strategy()
        strat._has_supply = True
        strat._supplied_amount = Decimal("100")
        strat._ticks_with_supply = 3
        market = _mock_market(eth_price=Decimal("1800"))
        strat.decide(market)
        assert strat._ticks_with_supply == 4

    def test_hold_when_price_unavailable(self):
        """Should hold when ETH price oracle fails."""
        strat = _make_strategy()
        market = MagicMock()
        market.price.side_effect = ValueError("oracle unavailable")

        intent = strat.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason.lower()
        market.balance.assert_not_called()

    def test_custom_thresholds(self):
        """Non-default thresholds should control supply/withdraw decisions."""
        strat = _make_strategy()
        strat.price_supply_above = Decimal("3000")
        strat.price_withdraw_below = Decimal("2500")

        # ETH=2800 is below custom supply threshold (3000), should hold
        market = _mock_market(eth_price=Decimal("2800"), usdc_balance=Decimal("10000"))
        intent = strat.decide(market)
        assert intent.intent_type.value == "HOLD"

        # ETH=3100 is above custom supply threshold, should supply
        market = _mock_market(eth_price=Decimal("3100"), usdc_balance=Decimal("10000"))
        intent = strat.decide(market)
        assert intent.intent_type.value == "SUPPLY"

        # With active supply, ETH=2400 is below custom withdraw threshold, should withdraw
        strat._has_supply = True
        strat._supplied_amount = Decimal("100")
        market = _mock_market(eth_price=Decimal("2400"))
        intent = strat.decide(market)
        assert intent.intent_type.value == "WITHDRAW"


# ===========================================================================
# State Transition Tests
# ===========================================================================


class TestStateTransitions:
    """Test on_intent_executed state updates."""

    def test_supply_success_updates_state(self):
        """Supply success should mark position active."""
        strat = _make_strategy()
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SUPPLY"
        mock_result = MagicMock()

        strat.on_intent_executed(mock_intent, success=True, result=mock_result)

        assert strat._has_supply is True
        assert strat._supplied_amount == Decimal("100")
        assert strat._ticks_with_supply == 0

    def test_withdraw_success_clears_state(self):
        """Withdraw success should clear position."""
        strat = _make_strategy()
        strat._has_supply = True
        strat._supplied_amount = Decimal("100")
        strat._ticks_with_supply = 5

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "WITHDRAW"
        mock_result = MagicMock()

        strat.on_intent_executed(mock_intent, success=True, result=mock_result)

        assert strat._has_supply is False
        assert strat._supplied_amount == Decimal("0")
        assert strat._ticks_with_supply == 0

    def test_failed_intent_does_not_change_state(self):
        """Failed intent should not modify state."""
        strat = _make_strategy()
        strat._has_supply = False
        strat._supplied_amount = Decimal("0")

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SUPPLY"

        strat.on_intent_executed(mock_intent, success=False, result=None)

        assert strat._has_supply is False
        assert strat._supplied_amount == Decimal("0")

    def test_cumulative_supply(self):
        """Multiple supplies should accumulate."""
        strat = _make_strategy()
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SUPPLY"

        strat.on_intent_executed(mock_intent, success=True, result=MagicMock())
        strat.on_intent_executed(mock_intent, success=True, result=MagicMock())

        assert strat._supplied_amount == Decimal("200")


# ===========================================================================
# Persistence Tests
# ===========================================================================


class TestPersistence:
    """Test state save/restore."""

    def test_save_and_restore_state(self):
        """State should round-trip through persistence."""
        strat = _make_strategy()
        strat._has_supply = True
        strat._supplied_amount = Decimal("500")
        strat._ticks_with_supply = 7

        saved = strat.get_persistent_state()
        assert saved["has_supply"] is True
        assert saved["supplied_amount"] == "500"
        assert saved["ticks_with_supply"] == 7

        strat2 = _make_strategy()
        strat2.load_persistent_state(saved)
        assert strat2._has_supply is True
        assert strat2._supplied_amount == Decimal("500")
        assert strat2._ticks_with_supply == 7


# ===========================================================================
# Teardown Tests
# ===========================================================================


class TestTeardown:
    """Test teardown support."""

    def test_no_positions_when_idle(self):
        strat = _make_strategy()
        summary = strat.get_open_positions()
        assert len(summary.positions) == 0

    def test_reports_supply_position(self):
        strat = _make_strategy()
        strat._has_supply = True
        strat._supplied_amount = Decimal("500")
        summary = strat.get_open_positions()
        assert len(summary.positions) == 1
        pos = summary.positions[0]
        assert pos.position_type.value == "SUPPLY"
        assert pos.protocol == "compound_v3"
        assert pos.value_usd == Decimal("500")

    def test_teardown_generates_withdraw(self):
        from almanak.framework.teardown import TeardownMode

        strat = _make_strategy()
        strat._has_supply = True
        strat._supplied_amount = Decimal("500")
        intents = strat.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "WITHDRAW"

    def test_teardown_empty_when_no_position(self):
        from almanak.framework.teardown import TeardownMode

        strat = _make_strategy()
        intents = strat.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 0


# ===========================================================================
# Paper Trading PnL Compatibility
# ===========================================================================


class TestPaperTradingCompat:
    """Verify lending trades produce valid PaperTrade records."""

    def test_supply_trade_record(self):
        """Supply should produce a valid PaperTrade with tokens_out."""
        from almanak.framework.backtesting.paper.models import PaperTrade

        trade = PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=12345,
            intent={"type": "SUPPLY"},
            tx_hash="0x" + "a" * 64,
            gas_used=200000,
            gas_cost_usd=Decimal("0.10"),
            tokens_in={},  # Supply: no tokens received immediately
            tokens_out={"USDC": Decimal("100")},  # USDC leaves wallet
            token_prices_usd={"USDC": Decimal("1")},
            protocol="compound_v3",
        )

        # Token flow: sent $100 USDC, received nothing (cToken accrual is implicit)
        assert trade.net_token_flow_usd == Decimal("-100")
        # Net PnL includes gas
        assert trade.net_pnl_usd == Decimal("-100") - Decimal("0.10")

    def test_withdraw_trade_record(self):
        """Withdraw should produce a valid PaperTrade with tokens_in."""
        from almanak.framework.backtesting.paper.models import PaperTrade

        trade = PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=12346,
            intent={"type": "WITHDRAW"},
            tx_hash="0x" + "b" * 64,
            gas_used=200000,
            gas_cost_usd=Decimal("0.10"),
            tokens_in={"USDC": Decimal("101")},  # Withdraw: USDC with interest
            tokens_out={},
            token_prices_usd={"USDC": Decimal("1")},
            protocol="compound_v3",
        )

        # Token flow: received $101 USDC (principal + interest)
        assert trade.net_token_flow_usd == Decimal("101")
        assert trade.net_pnl_usd == Decimal("101") - Decimal("0.10")

    def test_supply_withdraw_round_trip_pnl(self):
        """Supply + withdraw round trip should show interest as profit."""
        from almanak.framework.backtesting.paper.models import PaperTrade

        supply = PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=12345,
            intent={"type": "SUPPLY"},
            tx_hash="0x" + "a" * 64,
            gas_used=200000,
            gas_cost_usd=Decimal("0.10"),
            tokens_in={},
            tokens_out={"USDC": Decimal("1000")},
            token_prices_usd={"USDC": Decimal("1")},
            protocol="compound_v3",
        )

        withdraw = PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=12446,
            intent={"type": "WITHDRAW"},
            tx_hash="0x" + "b" * 64,
            gas_used=200000,
            gas_cost_usd=Decimal("0.10"),
            tokens_in={"USDC": Decimal("1002.50")},  # 0.25% interest
            tokens_out={},
            token_prices_usd={"USDC": Decimal("1")},
            protocol="compound_v3",
        )

        total_pnl = supply.net_pnl_usd + withdraw.net_pnl_usd
        # -$1000 - $0.10 + $1002.50 - $0.10 = $2.30 profit
        assert total_pnl == Decimal("2.30")


# ===========================================================================
# Portfolio Tracker Compatibility
# ===========================================================================


class TestPortfolioTracker:
    """Verify PaperPortfolioTracker handles lending trades."""

    def test_tracker_records_supply_and_withdraw(self):
        from almanak.framework.backtesting.paper.models import PaperTrade
        from almanak.framework.backtesting.paper.portfolio_tracker import PaperPortfolioTracker

        tracker = PaperPortfolioTracker(
            strategy_id="compound_paper_test",
            chain="base",
        )
        tracker.start_session({"USDC": Decimal("10000")})

        # Record supply (USDC leaves wallet)
        supply_trade = PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=12345,
            intent={"type": "SUPPLY"},
            tx_hash="0x" + "a" * 64,
            gas_used=200000,
            gas_cost_usd=Decimal("0.10"),
            tokens_in={},
            tokens_out={"USDC": Decimal("1000")},
            protocol="compound_v3",
        )
        tracker.record_trade(supply_trade)

        assert tracker.current_balances["USDC"] == Decimal("9000")
        assert tracker.total_gas_cost_usd == Decimal("0.10")

        # Record withdraw (USDC returns with interest)
        withdraw_trade = PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=12446,
            intent={"type": "WITHDRAW"},
            tx_hash="0x" + "b" * 64,
            gas_used=200000,
            gas_cost_usd=Decimal("0.10"),
            tokens_in={"USDC": Decimal("1002")},
            tokens_out={},
            protocol="compound_v3",
        )
        tracker.record_trade(withdraw_trade)

        # 10000 - 1000 + 1002 = 10002
        assert tracker.current_balances["USDC"] == Decimal("10002")
        assert tracker.total_gas_cost_usd == Decimal("0.20")
        assert len(tracker.trades) == 2

    def test_pnl_calculation_with_lending(self):
        from almanak.framework.backtesting.paper.models import PaperTrade
        from almanak.framework.backtesting.paper.portfolio_tracker import PaperPortfolioTracker

        tracker = PaperPortfolioTracker(
            strategy_id="compound_pnl_test",
            chain="base",
        )
        tracker.start_session({"USDC": Decimal("10000")})

        # Supply 1000 USDC
        tracker.record_trade(PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=12345,
            intent={"type": "SUPPLY"},
            tx_hash="0x" + "a" * 64,
            gas_used=200000,
            gas_cost_usd=Decimal("0.10"),
            tokens_in={},
            tokens_out={"USDC": Decimal("1000")},
            protocol="compound_v3",
        ))

        # Withdraw 1005 USDC (interest earned)
        tracker.record_trade(PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=12446,
            intent={"type": "WITHDRAW"},
            tx_hash="0x" + "b" * 64,
            gas_used=200000,
            gas_cost_usd=Decimal("0.10"),
            tokens_in={"USDC": Decimal("1005")},
            tokens_out={},
            protocol="compound_v3",
        ))

        # Get PnL (portfolio value delta, gas tracked separately)
        pnl = tracker.get_pnl_usd({"USDC": Decimal("1")})
        # Initial: $10,000. Final: $10,005. PnL = $5
        # (gas is tracked via total_gas_cost_usd, not deducted from balances)
        assert pnl is not None
        assert pnl == Decimal("5")
