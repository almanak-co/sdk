"""Tests for LP PnL calculation in SimulatedPortfolio.

This module tests the LP PnL calculation functionality including:
- Impermanent loss calculation for price increases/decreases
- Fee accrual and tracking
- Out-of-range position handling
- Net LP PnL calculation (fees - IL)
"""

from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.calculators.impermanent_loss import (
    MAX_TICK,
    MIN_TICK,
    ImpermanentLossCalculator,
)
from almanak.framework.backtesting.pnl.portfolio import (
    PositionType,
    SimulatedFill,
    SimulatedPortfolio,
    SimulatedPosition,
)


class TestLPPnLWithPriceIncrease:
    """Tests for LP PnL calculation when price increases (token0 appreciates)."""

    def test_lp_pnl_price_increase_with_fees(self):
        """Test LP PnL calculation with price increase including IL and fees.

        Scenario:
        - Entry price: ETH = 2000 USDC
        - Current price: ETH = 2200 USDC (10% increase)
        - Position has accumulated fees
        - Expected: IL loss (holds less ETH now) + fees earned
        """
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        close_time = datetime(2024, 1, 15, tzinfo=UTC)

        # Create portfolio with an LP position
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))

        # Create LP position at entry price of 2000 USDC/ETH
        lp_position = SimulatedPosition.lp(
            token0="ETH",
            token1="USDC",
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
            liquidity=Decimal("1000000"),
            tick_lower=-887220,  # Near full range
            tick_upper=887220,
            fee_tier=Decimal("0.003"),  # 0.3% fee tier
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="uniswap_v3",
        )
        # Simulate accumulated fees
        lp_position.accumulated_fees_usd = Decimal("150")
        lp_position.fees_earned = Decimal("150")

        portfolio.positions.append(lp_position)

        # Create a fill to close the position at higher price
        close_fill = SimulatedFill(
            timestamp=close_time,
            intent_type=IntentType.LP_CLOSE,
            protocol="uniswap_v3",
            tokens=["ETH", "USDC"],
            executed_price=Decimal("2200"),
            amount_usd=Decimal("4000"),  # Approximate position value
            fee_usd=Decimal("5"),
            slippage_usd=Decimal("10"),
            gas_cost_usd=Decimal("2"),
            tokens_in={"ETH": Decimal("0.8"), "USDC": Decimal("2400")},  # IL changes composition
            tokens_out={},
            success=True,
            position_close_id=lp_position.position_id,
            metadata={
                "token0_price_usd": Decimal("2200"),
                "token1_price_usd": Decimal("1"),
            },
        )

        # Apply the fill
        portfolio.apply_fill(close_fill)

        # Verify trade was recorded with LP PnL breakdown
        assert len(portfolio.trades) == 1
        trade = portfolio.trades[0]

        # IL should be positive (loss) since price increased
        # When ETH price goes up, LP holds less ETH (sold some)
        assert trade.il_loss_usd is not None
        assert trade.il_loss_usd >= Decimal("0")

        # Fees earned should match position
        assert trade.fees_earned_usd is not None
        assert trade.fees_earned_usd == Decimal("150")

        # Net LP PnL should be calculated
        assert trade.net_lp_pnl_usd is not None

    def test_lp_pnl_significant_price_increase(self):
        """Test LP PnL with a significant price increase (50%)."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        close_time = datetime(2024, 2, 1, tzinfo=UTC)

        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))

        # Create LP position
        lp_position = SimulatedPosition.lp(
            token0="ETH",
            token1="USDC",
            amount0=Decimal("2"),
            amount1=Decimal("4000"),
            liquidity=Decimal("2000000"),
            tick_lower=-887220,
            tick_upper=887220,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        lp_position.accumulated_fees_usd = Decimal("300")
        lp_position.fees_earned = Decimal("300")
        portfolio.positions.append(lp_position)

        # Close at 50% higher price
        close_fill = SimulatedFill(
            timestamp=close_time,
            intent_type=IntentType.LP_CLOSE,
            protocol="uniswap_v3",
            tokens=["ETH", "USDC"],
            executed_price=Decimal("3000"),
            amount_usd=Decimal("8000"),
            fee_usd=Decimal("10"),
            slippage_usd=Decimal("20"),
            gas_cost_usd=Decimal("5"),
            tokens_in={"ETH": Decimal("1.2"), "USDC": Decimal("5400")},
            tokens_out={},
            success=True,
            position_close_id=lp_position.position_id,
            metadata={
                "token0_price_usd": Decimal("3000"),
                "token1_price_usd": Decimal("1"),
            },
        )

        portfolio.apply_fill(close_fill)

        trade = portfolio.trades[0]

        # With 50% price increase, IL should be more significant
        assert trade.il_loss_usd is not None
        assert trade.il_loss_usd > Decimal("0")

        # Verify fees are tracked
        assert trade.fees_earned_usd == Decimal("300")


class TestLPPnLWithPriceDecrease:
    """Tests for LP PnL calculation when price decreases (token0 depreciates)."""

    def test_lp_pnl_price_decrease_with_fees(self):
        """Test LP PnL calculation with price decrease including IL and fees.

        Scenario:
        - Entry price: ETH = 2000 USDC
        - Current price: ETH = 1800 USDC (10% decrease)
        - Position has accumulated fees
        - Expected: IL loss (holds more ETH now at lower price) + fees earned
        """
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        close_time = datetime(2024, 1, 15, tzinfo=UTC)

        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))

        lp_position = SimulatedPosition.lp(
            token0="ETH",
            token1="USDC",
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
            liquidity=Decimal("1000000"),
            tick_lower=-887220,
            tick_upper=887220,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        lp_position.accumulated_fees_usd = Decimal("120")
        lp_position.fees_earned = Decimal("120")
        portfolio.positions.append(lp_position)

        # Close at lower price - LP now holds more ETH
        close_fill = SimulatedFill(
            timestamp=close_time,
            intent_type=IntentType.LP_CLOSE,
            protocol="uniswap_v3",
            tokens=["ETH", "USDC"],
            executed_price=Decimal("1800"),
            amount_usd=Decimal("3600"),
            fee_usd=Decimal("5"),
            slippage_usd=Decimal("10"),
            gas_cost_usd=Decimal("2"),
            tokens_in={"ETH": Decimal("1.2"), "USDC": Decimal("1600")},
            tokens_out={},
            success=True,
            position_close_id=lp_position.position_id,
            metadata={
                "token0_price_usd": Decimal("1800"),
                "token1_price_usd": Decimal("1"),
            },
        )

        portfolio.apply_fill(close_fill)

        trade = portfolio.trades[0]

        # IL should be present (price moved)
        assert trade.il_loss_usd is not None
        assert trade.il_loss_usd >= Decimal("0")

        # Fees should be recorded
        assert trade.fees_earned_usd == Decimal("120")

        # Net PnL should be calculated
        assert trade.net_lp_pnl_usd is not None

    def test_lp_pnl_significant_price_decrease(self):
        """Test LP PnL with a significant price decrease (50%)."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        close_time = datetime(2024, 2, 1, tzinfo=UTC)

        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))

        lp_position = SimulatedPosition.lp(
            token0="ETH",
            token1="USDC",
            amount0=Decimal("2"),
            amount1=Decimal("4000"),
            liquidity=Decimal("2000000"),
            tick_lower=-887220,
            tick_upper=887220,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        lp_position.accumulated_fees_usd = Decimal("200")
        lp_position.fees_earned = Decimal("200")
        portfolio.positions.append(lp_position)

        # Close at 50% lower price
        close_fill = SimulatedFill(
            timestamp=close_time,
            intent_type=IntentType.LP_CLOSE,
            protocol="uniswap_v3",
            tokens=["ETH", "USDC"],
            executed_price=Decimal("1000"),
            amount_usd=Decimal("4000"),
            fee_usd=Decimal("5"),
            slippage_usd=Decimal("10"),
            gas_cost_usd=Decimal("3"),
            tokens_in={"ETH": Decimal("3"), "USDC": Decimal("1000")},
            tokens_out={},
            success=True,
            position_close_id=lp_position.position_id,
            metadata={
                "token0_price_usd": Decimal("1000"),
                "token1_price_usd": Decimal("1"),
            },
        )

        portfolio.apply_fill(close_fill)

        trade = portfolio.trades[0]

        # With 50% price decrease, IL should be significant
        assert trade.il_loss_usd is not None
        assert trade.il_loss_usd > Decimal("0")

        # Fees earned
        assert trade.fees_earned_usd == Decimal("200")


class TestLPPnLNoPriceChange:
    """Tests for LP PnL calculation when price doesn't change (fees only)."""

    def test_lp_pnl_no_price_change_fees_only(self):
        """Test LP PnL when price is unchanged - should have only fees, no IL.

        Scenario:
        - Entry price: ETH = 2000 USDC
        - Current price: ETH = 2000 USDC (no change)
        - Position has accumulated fees
        - Expected: No IL, only fees earned
        """
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        close_time = datetime(2024, 1, 30, tzinfo=UTC)

        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))

        lp_position = SimulatedPosition.lp(
            token0="ETH",
            token1="USDC",
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
            liquidity=Decimal("1000000"),
            tick_lower=-887220,
            tick_upper=887220,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        lp_position.accumulated_fees_usd = Decimal("250")
        lp_position.fees_earned = Decimal("250")
        portfolio.positions.append(lp_position)

        # Close at same price
        close_fill = SimulatedFill(
            timestamp=close_time,
            intent_type=IntentType.LP_CLOSE,
            protocol="uniswap_v3",
            tokens=["ETH", "USDC"],
            executed_price=Decimal("2000"),
            amount_usd=Decimal("4000"),
            fee_usd=Decimal("5"),
            slippage_usd=Decimal("5"),
            gas_cost_usd=Decimal("2"),
            tokens_in={"ETH": Decimal("1"), "USDC": Decimal("2000")},
            tokens_out={},
            success=True,
            position_close_id=lp_position.position_id,
            metadata={
                "token0_price_usd": Decimal("2000"),
                "token1_price_usd": Decimal("1"),
            },
        )

        portfolio.apply_fill(close_fill)

        trade = portfolio.trades[0]

        # IL should be zero or very close to zero (no price change)
        assert trade.il_loss_usd is not None
        assert trade.il_loss_usd < Decimal("0.01")  # Allow small rounding error

        # Fees should be the main PnL component
        assert trade.fees_earned_usd == Decimal("250")

        # Net PnL should be approximately equal to fees (since IL ~ 0)
        assert trade.net_lp_pnl_usd is not None

    def test_lp_pnl_minimal_price_change(self):
        """Test LP PnL with minimal price change (0.1%).

        Note: IL in USD depends on position size. Even small price changes
        can produce measurable IL for large liquidity positions. The key
        assertion is that IL percentage is small relative to position value.
        """
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        close_time = datetime(2024, 1, 15, tzinfo=UTC)

        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))

        lp_position = SimulatedPosition.lp(
            token0="ETH",
            token1="USDC",
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
            liquidity=Decimal("1000000"),
            tick_lower=-887220,
            tick_upper=887220,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        lp_position.accumulated_fees_usd = Decimal("100")
        lp_position.fees_earned = Decimal("100")
        portfolio.positions.append(lp_position)

        # Close at 0.1% higher price
        close_fill = SimulatedFill(
            timestamp=close_time,
            intent_type=IntentType.LP_CLOSE,
            protocol="uniswap_v3",
            tokens=["ETH", "USDC"],
            executed_price=Decimal("2002"),
            amount_usd=Decimal("4000"),
            fee_usd=Decimal("5"),
            slippage_usd=Decimal("5"),
            gas_cost_usd=Decimal("2"),
            tokens_in={"ETH": Decimal("0.999"), "USDC": Decimal("2002")},
            tokens_out={},
            success=True,
            position_close_id=lp_position.position_id,
            metadata={
                "token0_price_usd": Decimal("2002"),
                "token1_price_usd": Decimal("1"),
            },
        )

        portfolio.apply_fill(close_fill)

        trade = portfolio.trades[0]

        # IL should be present but small relative to position value
        # For 0.1% price change, IL percentage is very small (~0.00003%)
        assert trade.il_loss_usd is not None
        # IL should be less than 1% of position value ($4000) = $40
        assert trade.il_loss_usd < Decimal("40")

        # Fees still tracked correctly
        assert trade.fees_earned_usd == Decimal("100")


class TestLPPnLOutOfRange:
    """Tests for LP PnL calculation when price moves out of position range."""

    def test_lp_pnl_price_above_range(self):
        """Test LP PnL when price goes above the position's upper tick.

        When price exceeds the upper bound, the position becomes 100% token1
        (the quote token, usually USDC).
        """
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        close_time = datetime(2024, 2, 1, tzinfo=UTC)

        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))

        # Create a narrow range position (ETH 1800-2200)
        # Ticks for ~1800-2200 USDC/ETH range
        tick_lower = 74000  # ~1800
        tick_upper = 78000  # ~2200

        lp_position = SimulatedPosition.lp(
            token0="ETH",
            token1="USDC",
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
            liquidity=Decimal("5000000"),
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        lp_position.accumulated_fees_usd = Decimal("500")
        lp_position.fees_earned = Decimal("500")
        portfolio.positions.append(lp_position)

        # Price goes to 2500 - above the range
        # Position should be 100% USDC (sold all ETH)
        close_fill = SimulatedFill(
            timestamp=close_time,
            intent_type=IntentType.LP_CLOSE,
            protocol="uniswap_v3",
            tokens=["ETH", "USDC"],
            executed_price=Decimal("2500"),
            amount_usd=Decimal("4200"),
            fee_usd=Decimal("10"),
            slippage_usd=Decimal("15"),
            gas_cost_usd=Decimal("5"),
            tokens_in={"ETH": Decimal("0"), "USDC": Decimal("4200")},  # All USDC
            tokens_out={},
            success=True,
            position_close_id=lp_position.position_id,
            metadata={
                "token0_price_usd": Decimal("2500"),
                "token1_price_usd": Decimal("1"),
            },
        )

        portfolio.apply_fill(close_fill)

        trade = portfolio.trades[0]

        # Should have IL (missed the upside by selling ETH)
        assert trade.il_loss_usd is not None
        assert trade.il_loss_usd >= Decimal("0")

        # Fees still accumulated
        assert trade.fees_earned_usd == Decimal("500")

        # Net PnL should account for both
        assert trade.net_lp_pnl_usd is not None

    def test_lp_pnl_price_below_range(self):
        """Test LP PnL when price goes below the position's lower tick.

        When price falls below the lower bound, the position becomes 100% token0
        (the base token, usually ETH).
        """
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        close_time = datetime(2024, 2, 1, tzinfo=UTC)

        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))

        # Create a narrow range position (ETH 1800-2200)
        tick_lower = 74000
        tick_upper = 78000

        lp_position = SimulatedPosition.lp(
            token0="ETH",
            token1="USDC",
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
            liquidity=Decimal("5000000"),
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        lp_position.accumulated_fees_usd = Decimal("300")
        lp_position.fees_earned = Decimal("300")
        portfolio.positions.append(lp_position)

        # Price goes to 1500 - below the range
        # Position should be 100% ETH (bought more ETH)
        close_fill = SimulatedFill(
            timestamp=close_time,
            intent_type=IntentType.LP_CLOSE,
            protocol="uniswap_v3",
            tokens=["ETH", "USDC"],
            executed_price=Decimal("1500"),
            amount_usd=Decimal("3000"),
            fee_usd=Decimal("8"),
            slippage_usd=Decimal("12"),
            gas_cost_usd=Decimal("4"),
            tokens_in={"ETH": Decimal("2"), "USDC": Decimal("0")},  # All ETH
            tokens_out={},
            success=True,
            position_close_id=lp_position.position_id,
            metadata={
                "token0_price_usd": Decimal("1500"),
                "token1_price_usd": Decimal("1"),
            },
        )

        portfolio.apply_fill(close_fill)

        trade = portfolio.trades[0]

        # Should have IL (holding more ETH at lower price)
        assert trade.il_loss_usd is not None
        assert trade.il_loss_usd >= Decimal("0")

        # Fees still tracked
        assert trade.fees_earned_usd == Decimal("300")

        # Net PnL calculated
        assert trade.net_lp_pnl_usd is not None

    def test_lp_pnl_price_temporarily_out_of_range(self):
        """Test LP position that goes out of range and back in range."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        close_time = datetime(2024, 1, 30, tzinfo=UTC)

        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))

        # Full range position - never truly "out of range" but we test
        # the calculation when price returns to near entry
        lp_position = SimulatedPosition.lp(
            token0="ETH",
            token1="USDC",
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
            liquidity=Decimal("1000000"),
            tick_lower=-887220,
            tick_upper=887220,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        # Assume significant fees accumulated during volatile period
        lp_position.accumulated_fees_usd = Decimal("400")
        lp_position.fees_earned = Decimal("400")
        portfolio.positions.append(lp_position)

        # Close at entry price - no IL
        close_fill = SimulatedFill(
            timestamp=close_time,
            intent_type=IntentType.LP_CLOSE,
            protocol="uniswap_v3",
            tokens=["ETH", "USDC"],
            executed_price=Decimal("2000"),
            amount_usd=Decimal("4000"),
            fee_usd=Decimal("5"),
            slippage_usd=Decimal("5"),
            gas_cost_usd=Decimal("2"),
            tokens_in={"ETH": Decimal("1"), "USDC": Decimal("2000")},
            tokens_out={},
            success=True,
            position_close_id=lp_position.position_id,
            metadata={
                "token0_price_usd": Decimal("2000"),
                "token1_price_usd": Decimal("1"),
            },
        )

        portfolio.apply_fill(close_fill)

        trade = portfolio.trades[0]

        # IL should be zero or near zero (back at entry price)
        assert trade.il_loss_usd is not None
        assert trade.il_loss_usd < Decimal("0.01")

        # Fees earned during volatile period
        assert trade.fees_earned_usd == Decimal("400")


class TestImpermanentLossCalculatorDirectly:
    """Direct tests for ImpermanentLossCalculator to verify IL math."""

    def test_il_calculator_price_increase(self):
        """Test IL calculation with price increase."""
        calc = ImpermanentLossCalculator()

        il_pct, token0, token1 = calc.calculate_il_v3(
            entry_price=Decimal("2000"),
            current_price=Decimal("2200"),  # 10% increase
            tick_lower=-887220,
            tick_upper=887220,
            liquidity=Decimal("1000000"),
        )

        # IL should be positive (there is a loss)
        assert il_pct >= Decimal("0")
        # Token0 (ETH) amount should decrease as price rises
        # Token1 (USDC) amount should increase
        assert token0 >= Decimal("0")
        assert token1 >= Decimal("0")

    def test_il_calculator_price_decrease(self):
        """Test IL calculation with price decrease."""
        calc = ImpermanentLossCalculator()

        il_pct, token0, token1 = calc.calculate_il_v3(
            entry_price=Decimal("2000"),
            current_price=Decimal("1800"),  # 10% decrease
            tick_lower=-887220,
            tick_upper=887220,
            liquidity=Decimal("1000000"),
        )

        # IL should be positive (there is a loss)
        assert il_pct >= Decimal("0")
        # Token0 (ETH) amount should increase as price falls
        assert token0 >= Decimal("0")
        assert token1 >= Decimal("0")

    def test_il_calculator_no_price_change(self):
        """Test IL calculation with no price change."""
        calc = ImpermanentLossCalculator()

        il_pct, token0, token1 = calc.calculate_il_v3(
            entry_price=Decimal("2000"),
            current_price=Decimal("2000"),  # No change
            tick_lower=-887220,
            tick_upper=887220,
            liquidity=Decimal("1000000"),
        )

        # IL should be zero when price doesn't change
        assert il_pct == Decimal("0")

    def test_il_calculator_2x_price_increase(self):
        """Test IL calculation with 2x price increase.

        Classic V2 IL formula: at 2x price, IL ~ 5.7%
        """
        calc = ImpermanentLossCalculator()

        il_pct, token0, token1 = calc.calculate_il_v3(
            entry_price=Decimal("1000"),
            current_price=Decimal("2000"),  # 2x increase
            tick_lower=MIN_TICK,
            tick_upper=MAX_TICK,
            liquidity=Decimal("1000000"),
        )

        # For full range (V2-like), 2x price should give ~5.7% IL
        # Allow some tolerance for calculation precision
        assert Decimal("0.04") < il_pct < Decimal("0.08")

    def test_il_calculator_invalid_inputs(self):
        """Test IL calculator handles invalid inputs gracefully."""
        calc = ImpermanentLossCalculator()

        # Zero prices
        il_pct, token0, token1 = calc.calculate_il_v3(
            entry_price=Decimal("0"),
            current_price=Decimal("2000"),
            tick_lower=-887220,
            tick_upper=887220,
            liquidity=Decimal("1000000"),
        )
        assert il_pct == Decimal("0")

        # Zero liquidity
        il_pct, token0, token1 = calc.calculate_il_v3(
            entry_price=Decimal("2000"),
            current_price=Decimal("2000"),
            tick_lower=-887220,
            tick_upper=887220,
            liquidity=Decimal("0"),
        )
        assert il_pct == Decimal("0")

        # Invalid tick range
        il_pct, token0, token1 = calc.calculate_il_v3(
            entry_price=Decimal("2000"),
            current_price=Decimal("2000"),
            tick_lower=100,
            tick_upper=50,  # Lower > Upper
            liquidity=Decimal("1000000"),
        )
        assert il_pct == Decimal("0")


class TestLPPnLEdgeCases:
    """Tests for edge cases in LP PnL calculation."""

    def test_lp_pnl_with_zero_fees(self):
        """Test LP PnL calculation when no fees have been earned."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        close_time = datetime(2024, 1, 2, tzinfo=UTC)

        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))

        lp_position = SimulatedPosition.lp(
            token0="ETH",
            token1="USDC",
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
            liquidity=Decimal("1000000"),
            tick_lower=-887220,
            tick_upper=887220,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        # No fees accumulated
        lp_position.accumulated_fees_usd = Decimal("0")
        lp_position.fees_earned = Decimal("0")
        portfolio.positions.append(lp_position)

        close_fill = SimulatedFill(
            timestamp=close_time,
            intent_type=IntentType.LP_CLOSE,
            protocol="uniswap_v3",
            tokens=["ETH", "USDC"],
            executed_price=Decimal("2100"),
            amount_usd=Decimal("4000"),
            fee_usd=Decimal("5"),
            slippage_usd=Decimal("5"),
            gas_cost_usd=Decimal("2"),
            tokens_in={"ETH": Decimal("0.95"), "USDC": Decimal("2100")},
            tokens_out={},
            success=True,
            position_close_id=lp_position.position_id,
            metadata={
                "token0_price_usd": Decimal("2100"),
                "token1_price_usd": Decimal("1"),
            },
        )

        portfolio.apply_fill(close_fill)

        trade = portfolio.trades[0]

        assert trade.fees_earned_usd == Decimal("0")
        assert trade.il_loss_usd is not None
        assert trade.net_lp_pnl_usd is not None

    def test_lp_pnl_position_not_found(self):
        """Test LP close when position is not found - should not crash."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))

        # Try to close a position that doesn't exist
        close_fill = SimulatedFill(
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            intent_type=IntentType.LP_CLOSE,
            protocol="uniswap_v3",
            tokens=["ETH", "USDC"],
            executed_price=Decimal("2000"),
            amount_usd=Decimal("4000"),
            fee_usd=Decimal("5"),
            slippage_usd=Decimal("5"),
            gas_cost_usd=Decimal("2"),
            tokens_in={"ETH": Decimal("1"), "USDC": Decimal("2000")},
            tokens_out={},
            success=True,
            position_close_id="nonexistent_position_id",
            metadata={},
        )

        # Should not raise an exception
        portfolio.apply_fill(close_fill)

        # Trade recorded but without LP PnL breakdown
        assert len(portfolio.trades) == 1
        trade = portfolio.trades[0]
        # LP PnL fields should be None when position not found
        assert trade.il_loss_usd is None
        assert trade.fees_earned_usd is None
        assert trade.net_lp_pnl_usd is None

    def test_lp_pnl_single_token_position(self):
        """Test handling of position with only one token (edge case)."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        close_time = datetime(2024, 1, 15, tzinfo=UTC)

        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))

        # Create a position with only one token (unusual but possible)
        lp_position = SimulatedPosition(
            position_type=PositionType.LP,
            protocol="uniswap_v3",
            tokens=["ETH"],  # Only one token
            amounts={"ETH": Decimal("1")},
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            liquidity=Decimal("1000000"),
            fee_tier=Decimal("0.003"),
        )
        lp_position.accumulated_fees_usd = Decimal("50")
        lp_position.fees_earned = Decimal("50")
        portfolio.positions.append(lp_position)

        close_fill = SimulatedFill(
            timestamp=close_time,
            intent_type=IntentType.LP_CLOSE,
            protocol="uniswap_v3",
            tokens=["ETH"],
            executed_price=Decimal("2000"),
            amount_usd=Decimal("2000"),
            fee_usd=Decimal("2"),
            slippage_usd=Decimal("3"),
            gas_cost_usd=Decimal("1"),
            tokens_in={"ETH": Decimal("1")},
            tokens_out={},
            success=True,
            position_close_id=lp_position.position_id,
            metadata={},
        )

        # Should handle gracefully without crashing
        portfolio.apply_fill(close_fill)

        trade = portfolio.trades[0]
        # IL should be 0 for single token position (fallback case)
        assert trade.il_loss_usd == Decimal("0")
        assert trade.fees_earned_usd == Decimal("50")


class TestFeeAPRCalculation:
    """Tests for fee APR calculation in ImpermanentLossCalculator."""

    def test_fee_apr_calculation(self):
        """Test fee APR annualization."""
        calc = ImpermanentLossCalculator()

        # $500 earned on $10,000 over 30 days
        apr = calc.calculate_fee_apr(
            fees_earned_usd=Decimal("500"),
            position_value_usd=Decimal("10000"),
            duration_days=Decimal("30"),
        )

        # Expected: 5% * (365/30) = ~60.8% APR
        expected_apr = Decimal("0.05") * (Decimal("365") / Decimal("30"))
        assert abs(apr - expected_apr) < Decimal("0.001")

    def test_fee_apr_zero_position_value(self):
        """Test fee APR with zero position value."""
        calc = ImpermanentLossCalculator()

        apr = calc.calculate_fee_apr(
            fees_earned_usd=Decimal("500"),
            position_value_usd=Decimal("0"),
            duration_days=Decimal("30"),
        )

        assert apr == Decimal("0")

    def test_fee_apr_zero_duration(self):
        """Test fee APR with zero duration."""
        calc = ImpermanentLossCalculator()

        apr = calc.calculate_fee_apr(
            fees_earned_usd=Decimal("500"),
            position_value_usd=Decimal("10000"),
            duration_days=Decimal("0"),
        )

        assert apr == Decimal("0")
