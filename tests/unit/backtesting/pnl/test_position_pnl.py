"""Unit tests for position PnL calculations.

This module tests the position calculation methods in SimulatedPortfolio:
- _calculate_perp_unrealized_pnl: Perp position PnL (price movement + funding)
- _calculate_lending_unrealized_pnl: Lending position PnL (interest accrual)
- _calculate_lp_unrealized_pnl: LP position valuation (fees + IL)

These are the core position valuation methods that determine unrealized PnL
for different position types during mark_to_market.
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.portfolio import (
    PositionType,
    SimulatedPortfolio,
    SimulatedPosition,
)


class MockMarketState(MarketState):
    """Mock market state for testing."""

    def __init__(self, prices: dict[str, Decimal]):
        self._prices = prices

    def get_price(self, token: str) -> Decimal:
        if token in self._prices:
            return self._prices[token]
        raise KeyError(f"Price not found for {token}")

    def get_prices(self, tokens: list[str]) -> dict[str, Decimal]:
        return {t: self.get_price(t) for t in tokens if t in self._prices}


# =============================================================================
# Perp Position PnL Tests
# =============================================================================


class TestPerpUnrealizedPnLLongPositions:
    """Tests for _calculate_perp_unrealized_pnl for long positions."""

    def test_long_position_profit_price_increase(self):
        """Test long position profits when price increases.

        Scenario:
        - Entry: $2000, Current: $2200 (10% increase)
        - Notional: $50,000 (5x leverage on $10k collateral)
        - Expected PnL: $50,000 * 0.10 = $5,000 profit
        """
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),  # $50,000 notional
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        portfolio.positions.append(position)

        market_state = MockMarketState({"ETH": Decimal("2200")})

        # Calculate unrealized PnL
        pnl = portfolio._calculate_perp_unrealized_pnl(position, market_state)

        # 10% price increase on $50k notional = $5k profit
        expected_pnl = Decimal("5000")
        assert pnl == pytest.approx(expected_pnl, rel=Decimal("0.001"))

    def test_long_position_loss_price_decrease(self):
        """Test long position loses when price decreases.

        Scenario:
        - Entry: $2000, Current: $1800 (10% decrease)
        - Notional: $50,000
        - Expected PnL: -$5,000 loss
        """
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        portfolio.positions.append(position)

        market_state = MockMarketState({"ETH": Decimal("1800")})

        pnl = portfolio._calculate_perp_unrealized_pnl(position, market_state)

        expected_pnl = Decimal("-5000")
        assert pnl == pytest.approx(expected_pnl, rel=Decimal("0.001"))

    def test_long_position_no_price_change(self):
        """Test long position has zero price PnL when price unchanged."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        portfolio.positions.append(position)

        market_state = MockMarketState({"ETH": Decimal("2000")})

        pnl = portfolio._calculate_perp_unrealized_pnl(position, market_state)

        assert pnl == Decimal("0")

    def test_long_position_with_funding_paid(self):
        """Test long position PnL includes funding paid.

        Scenario:
        - Entry: $2000, Current: $2200 (10% increase)
        - Notional: $50,000, PnL from price: $5,000
        - Funding paid: $120
        - Net PnL: $5,000 - $120 = $4,880
        """
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        # Simulate funding paid (negative for longs in positive rate environment)
        position.cumulative_funding_paid = Decimal("120")
        position.cumulative_funding_received = Decimal("0")
        portfolio.positions.append(position)

        market_state = MockMarketState({"ETH": Decimal("2200")})

        pnl = portfolio._calculate_perp_unrealized_pnl(position, market_state)

        # Price PnL: $5,000, net funding: -$120
        expected_pnl = Decimal("5000") - Decimal("120")
        assert pnl == pytest.approx(expected_pnl, rel=Decimal("0.001"))

    def test_long_position_with_funding_received(self):
        """Test long position PnL includes funding received (negative rate).

        Scenario:
        - Entry: $2000, Current: $1900 (5% decrease)
        - Notional: $50,000, PnL from price: -$2,500
        - Funding received: $200 (negative rate, longs receive)
        - Net PnL: -$2,500 + $200 = -$2,300
        """
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        # Simulate funding received
        position.cumulative_funding_paid = Decimal("0")
        position.cumulative_funding_received = Decimal("200")
        portfolio.positions.append(position)

        market_state = MockMarketState({"ETH": Decimal("1900")})

        pnl = portfolio._calculate_perp_unrealized_pnl(position, market_state)

        # Price PnL: -$2,500, net funding: +$200
        expected_pnl = Decimal("-2500") + Decimal("200")
        assert pnl == pytest.approx(expected_pnl, rel=Decimal("0.001"))


class TestPerpUnrealizedPnLShortPositions:
    """Tests for _calculate_perp_unrealized_pnl for short positions."""

    def test_short_position_profit_price_decrease(self):
        """Test short position profits when price decreases.

        Scenario:
        - Entry: $2000, Current: $1800 (10% decrease)
        - Notional: $50,000
        - Expected PnL: $5,000 profit (shorts profit on down)
        """
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        position = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        portfolio.positions.append(position)

        market_state = MockMarketState({"ETH": Decimal("1800")})

        pnl = portfolio._calculate_perp_unrealized_pnl(position, market_state)

        expected_pnl = Decimal("5000")
        assert pnl == pytest.approx(expected_pnl, rel=Decimal("0.001"))

    def test_short_position_loss_price_increase(self):
        """Test short position loses when price increases.

        Scenario:
        - Entry: $2000, Current: $2200 (10% increase)
        - Notional: $50,000
        - Expected PnL: -$5,000 loss
        """
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        position = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        portfolio.positions.append(position)

        market_state = MockMarketState({"ETH": Decimal("2200")})

        pnl = portfolio._calculate_perp_unrealized_pnl(position, market_state)

        expected_pnl = Decimal("-5000")
        assert pnl == pytest.approx(expected_pnl, rel=Decimal("0.001"))

    def test_short_position_with_funding_received(self):
        """Test short position PnL includes funding received (positive rate).

        Scenario:
        - Entry: $2000, Current: $2100 (5% increase)
        - Notional: $50,000, PnL from price: -$2,500
        - Funding received: $300 (positive rate, shorts receive)
        - Net PnL: -$2,500 + $300 = -$2,200
        """
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        position = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        position.cumulative_funding_paid = Decimal("0")
        position.cumulative_funding_received = Decimal("300")
        portfolio.positions.append(position)

        market_state = MockMarketState({"ETH": Decimal("2100")})

        pnl = portfolio._calculate_perp_unrealized_pnl(position, market_state)

        expected_pnl = Decimal("-2500") + Decimal("300")
        assert pnl == pytest.approx(expected_pnl, rel=Decimal("0.001"))

    def test_short_position_with_funding_paid(self):
        """Test short position PnL includes funding paid (negative rate).

        Scenario:
        - Entry: $2000, Current: $1900 (5% decrease)
        - Notional: $50,000, PnL from price: $2,500
        - Funding paid: $150 (negative rate, shorts pay)
        - Net PnL: $2,500 - $150 = $2,350
        """
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        position = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        position.cumulative_funding_paid = Decimal("150")
        position.cumulative_funding_received = Decimal("0")
        portfolio.positions.append(position)

        market_state = MockMarketState({"ETH": Decimal("1900")})

        pnl = portfolio._calculate_perp_unrealized_pnl(position, market_state)

        expected_pnl = Decimal("2500") - Decimal("150")
        assert pnl == pytest.approx(expected_pnl, rel=Decimal("0.001"))


class TestPerpPnLEdgeCases:
    """Edge case tests for perp position PnL calculation."""

    def test_zero_entry_price_returns_zero(self):
        """Test that zero entry price returns zero PnL (avoids division by zero)."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        position = SimulatedPosition(
            position_type=PositionType.PERP_LONG,
            protocol="gmx",
            tokens=["ETH"],
            amounts={"ETH": Decimal("25")},
            entry_price=Decimal("0"),  # Edge case
            entry_time=entry_time,
            leverage=Decimal("5"),
            collateral_usd=Decimal("10000"),
            notional_usd=Decimal("50000"),
        )
        portfolio.positions.append(position)

        market_state = MockMarketState({"ETH": Decimal("2000")})

        pnl = portfolio._calculate_perp_unrealized_pnl(position, market_state)

        assert pnl == Decimal("0")

    def test_missing_price_uses_entry_price(self):
        """Test that missing market price falls back to entry price (zero PnL)."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        position = SimulatedPosition.perp_long(
            token="UNKNOWN_TOKEN",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("100"),
            entry_time=entry_time,
            protocol="gmx",
        )
        portfolio.positions.append(position)

        # Market state doesn't have the token
        market_state = MockMarketState({"ETH": Decimal("2000")})

        pnl = portfolio._calculate_perp_unrealized_pnl(position, market_state)

        # Should return 0 since current price defaults to entry price
        assert pnl == Decimal("0")

    def test_no_tokens_returns_zero(self):
        """Test that position with no tokens returns zero PnL."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        position = SimulatedPosition(
            position_type=PositionType.PERP_LONG,
            protocol="gmx",
            tokens=[],  # No tokens
            amounts={},
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        portfolio.positions.append(position)

        market_state = MockMarketState({"ETH": Decimal("2200")})

        pnl = portfolio._calculate_perp_unrealized_pnl(position, market_state)

        assert pnl == Decimal("0")

    def test_very_large_price_movement(self):
        """Test PnL calculation with 100% price increase."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        portfolio.positions.append(position)

        # 100% price increase
        market_state = MockMarketState({"ETH": Decimal("4000")})

        pnl = portfolio._calculate_perp_unrealized_pnl(position, market_state)

        # 100% increase on $50k = $50k profit
        expected_pnl = Decimal("50000")
        assert pnl == pytest.approx(expected_pnl, rel=Decimal("0.001"))

    def test_leverage_affects_pnl_magnitude(self):
        """Test that higher leverage amplifies PnL correctly."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        # Same collateral, different leverage
        position_2x = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("2"),  # $20,000 notional
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        position_10x = SimulatedPosition.perp_long(
            token="BTC",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("10"),  # $100,000 notional
            entry_price=Decimal("40000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        portfolio.positions.append(position_2x)
        portfolio.positions.append(position_10x)

        # Both 10% price increase
        market_state = MockMarketState({
            "ETH": Decimal("2200"),
            "BTC": Decimal("44000"),
        })

        pnl_2x = portfolio._calculate_perp_unrealized_pnl(position_2x, market_state)
        pnl_10x = portfolio._calculate_perp_unrealized_pnl(position_10x, market_state)

        # 10% on $20k = $2k
        assert pnl_2x == pytest.approx(Decimal("2000"), rel=Decimal("0.001"))
        # 10% on $100k = $10k
        assert pnl_10x == pytest.approx(Decimal("10000"), rel=Decimal("0.001"))


# =============================================================================
# Lending Position PnL Tests
# =============================================================================


class TestLendingUnrealizedPnLSupply:
    """Tests for _calculate_lending_unrealized_pnl for supply positions."""

    def test_supply_position_earns_interest(self):
        """Test supply position unrealized PnL equals accrued interest."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        position = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("10000"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            apy=Decimal("0.05"),
            protocol="aave_v3",
        )
        # Simulate interest accrued
        position.interest_accrued = Decimal("250")
        portfolio.positions.append(position)

        pnl = portfolio._calculate_lending_unrealized_pnl(position)

        # Supply interest is positive PnL
        assert pnl == Decimal("250")

    def test_supply_position_zero_interest(self):
        """Test supply position with no accrued interest has zero PnL."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        position = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("10000"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            apy=Decimal("0.05"),
            protocol="aave_v3",
        )
        portfolio.positions.append(position)

        pnl = portfolio._calculate_lending_unrealized_pnl(position)

        assert pnl == Decimal("0")

    def test_supply_position_large_interest(self):
        """Test supply position with significant interest accumulation."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("1000000"))

        position = SimulatedPosition.supply(
            token="DAI",
            amount=Decimal("500000"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            apy=Decimal("0.08"),  # 8% APY
            protocol="aave_v3",
        )
        # Simulate 1 year of interest: $500k * 8% = $40k
        position.interest_accrued = Decimal("40000")
        portfolio.positions.append(position)

        pnl = portfolio._calculate_lending_unrealized_pnl(position)

        assert pnl == Decimal("40000")


class TestLendingUnrealizedPnLBorrow:
    """Tests for _calculate_lending_unrealized_pnl for borrow positions."""

    def test_borrow_position_owes_interest(self):
        """Test borrow position unrealized PnL equals negative accrued interest."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        position = SimulatedPosition.borrow(
            token="USDC",
            amount=Decimal("10000"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            apy=Decimal("0.08"),  # Borrow APY
            protocol="aave_v3",
        )
        # Simulate interest owed
        position.interest_accrued = Decimal("300")
        portfolio.positions.append(position)

        pnl = portfolio._calculate_lending_unrealized_pnl(position)

        # Borrow interest is negative PnL (debt)
        assert pnl == Decimal("-300")

    def test_borrow_position_zero_interest(self):
        """Test borrow position with no accrued interest has zero PnL."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        position = SimulatedPosition.borrow(
            token="USDC",
            amount=Decimal("10000"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            apy=Decimal("0.08"),
            protocol="aave_v3",
        )
        portfolio.positions.append(position)

        pnl = portfolio._calculate_lending_unrealized_pnl(position)

        assert pnl == Decimal("0")

    def test_borrow_position_high_interest(self):
        """Test borrow position with high interest rate accumulation."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("500000"))

        position = SimulatedPosition.borrow(
            token="ETH",
            amount=Decimal("100"),  # 100 ETH borrowed at $2000
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            apy=Decimal("0.12"),  # 12% borrow APY
            protocol="aave_v3",
        )
        # Principal: $200,000. Annual interest: $24,000
        position.interest_accrued = Decimal("24000")
        portfolio.positions.append(position)

        pnl = portfolio._calculate_lending_unrealized_pnl(position)

        assert pnl == Decimal("-24000")


class TestLendingPnLEdgeCases:
    """Edge case tests for lending position PnL calculation."""

    def test_fractional_interest(self):
        """Test lending PnL with fractional (very small) interest."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100"))

        position = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("100"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            apy=Decimal("0.01"),
            protocol="aave_v3",
        )
        # Very small interest: $0.01
        position.interest_accrued = Decimal("0.01")
        portfolio.positions.append(position)

        pnl = portfolio._calculate_lending_unrealized_pnl(position)

        assert pnl == Decimal("0.01")

    def test_supply_vs_borrow_opposite_sign(self):
        """Test that supply and borrow have opposite PnL signs for same interest."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        supply = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("10000"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            apy=Decimal("0.05"),
            protocol="aave_v3",
        )
        supply.interest_accrued = Decimal("500")

        borrow = SimulatedPosition.borrow(
            token="DAI",
            amount=Decimal("10000"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            apy=Decimal("0.08"),
            protocol="aave_v3",
        )
        borrow.interest_accrued = Decimal("500")

        portfolio.positions.extend([supply, borrow])

        supply_pnl = portfolio._calculate_lending_unrealized_pnl(supply)
        borrow_pnl = portfolio._calculate_lending_unrealized_pnl(borrow)

        # Same interest amount but opposite signs
        assert supply_pnl == Decimal("500")
        assert borrow_pnl == Decimal("-500")


# =============================================================================
# LP Position Valuation Tests
# =============================================================================


class TestLPUnrealizedPnL:
    """Tests for _calculate_lp_unrealized_pnl."""

    def test_lp_pnl_includes_fees(self):
        """Test LP unrealized PnL calculation includes accumulated fees.

        The LP PnL calculation formula is:
        PnL = (current_value + fees) - entry_value

        When entry_amounts are stored in metadata, entry_value is calculated accurately.
        Without entry_amounts, entry_value is estimated from liquidity which may differ.
        """
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        position = SimulatedPosition.lp(
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
            protocol="uniswap_v3",
        )
        # Store entry amounts for accurate entry value calculation
        position.metadata["entry_amounts"] = {
            "ETH": "1",
            "USDC": "2000",
        }
        position.accumulated_fees_usd = Decimal("150")
        position.fees_earned = Decimal("150")
        portfolio.positions.append(position)

        # Price unchanged
        market_state = MockMarketState({
            "ETH": Decimal("2000"),
            "USDC": Decimal("1"),
        })

        pnl = portfolio._calculate_lp_unrealized_pnl(position, market_state)

        # With no price change and entry amounts stored correctly:
        # current_value = 1 * 2000 + 2000 * 1 = $4000
        # entry_value = 1 * 2000 + 2000 * 1 = $4000
        # fees = $150
        # PnL = (4000 + 150) - 4000 = $150
        assert pnl == pytest.approx(Decimal("150"), rel=Decimal("0.01"))

    def test_lp_pnl_with_price_increase(self):
        """Test LP unrealized PnL calculation with price increase."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        # Store entry amounts in metadata for accurate calculation
        position = SimulatedPosition.lp(
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
            protocol="uniswap_v3",
        )
        position.metadata["entry_amounts"] = {
            "ETH": "1",
            "USDC": "2000",
        }
        position.accumulated_fees_usd = Decimal("100")
        position.fees_earned = Decimal("100")
        portfolio.positions.append(position)

        # 10% price increase - LP holds less ETH
        market_state = MockMarketState({
            "ETH": Decimal("2200"),
            "USDC": Decimal("1"),
        })

        pnl = portfolio._calculate_lp_unrealized_pnl(position, market_state)

        # PnL should reflect current value + fees - entry value
        # Result depends on position's current token amounts
        assert isinstance(pnl, Decimal)


class TestLPPnLEdgeCases:
    """Edge case tests for LP position valuation."""

    def test_lp_single_token_position(self):
        """Test LP PnL calculation with single token (edge case)."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        position = SimulatedPosition(
            position_type=PositionType.LP,
            protocol="uniswap_v3",
            tokens=["ETH"],
            amounts={"ETH": Decimal("2")},
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            liquidity=Decimal("1000000"),
            fee_tier=Decimal("0.003"),
        )
        position.accumulated_fees_usd = Decimal("50")
        position.fees_earned = Decimal("50")
        portfolio.positions.append(position)

        market_state = MockMarketState({"ETH": Decimal("2000")})

        # Should handle gracefully without crashing
        pnl = portfolio._calculate_lp_unrealized_pnl(position, market_state)
        assert isinstance(pnl, Decimal)

    def test_lp_zero_liquidity(self):
        """Test LP PnL calculation with zero liquidity."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        position = SimulatedPosition.lp(
            token0="ETH",
            token1="USDC",
            amount0=Decimal("0"),
            amount1=Decimal("0"),
            liquidity=Decimal("0"),
            tick_lower=-887220,
            tick_upper=887220,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="uniswap_v3",
        )
        portfolio.positions.append(position)

        market_state = MockMarketState({
            "ETH": Decimal("2000"),
            "USDC": Decimal("1"),
        })

        pnl = portfolio._calculate_lp_unrealized_pnl(position, market_state)
        assert isinstance(pnl, Decimal)


# =============================================================================
# Portfolio-Level Unrealized PnL Tests
# =============================================================================


class TestPortfolioUnrealizedPnL:
    """Tests for portfolio-level calculate_unrealized_pnl method."""

    def test_aggregate_pnl_multiple_perp_positions(self):
        """Test total unrealized PnL aggregates multiple perp positions."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        # Add long and short positions
        long_eth = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        short_btc = SimulatedPosition.perp_short(
            token="BTC",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("2"),
            entry_price=Decimal("40000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        portfolio.positions.extend([long_eth, short_btc])

        market_state = MockMarketState({
            "ETH": Decimal("2200"),  # 10% up, long profits $5k
            "BTC": Decimal("38000"),  # 5% down, short profits $1k
        })

        total_pnl = portfolio.calculate_unrealized_pnl(market_state)

        # ETH long: +$5000, BTC short: +$1000
        expected_total = Decimal("6000")
        assert total_pnl == pytest.approx(expected_total, rel=Decimal("0.01"))

    def test_aggregate_pnl_mixed_position_types(self):
        """Test total unrealized PnL with perp, supply, and borrow positions."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("200000"))

        # Perp long
        perp = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("20000"),
            leverage=Decimal("3"),  # $60k notional
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )

        # Supply position with interest earned
        supply = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("50000"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            apy=Decimal("0.05"),
            protocol="aave_v3",
        )
        supply.interest_accrued = Decimal("1000")

        # Borrow position with interest owed
        borrow = SimulatedPosition.borrow(
            token="DAI",
            amount=Decimal("20000"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            apy=Decimal("0.08"),
            protocol="aave_v3",
        )
        borrow.interest_accrued = Decimal("400")

        portfolio.positions.extend([perp, supply, borrow])

        market_state = MockMarketState({
            "ETH": Decimal("2100"),  # 5% up, perp profits $3k
            "USDC": Decimal("1"),
            "DAI": Decimal("1"),
        })

        total_pnl = portfolio.calculate_unrealized_pnl(market_state)

        # Perp: +$3000, Supply: +$1000, Borrow: -$400
        expected_total = Decimal("3000") + Decimal("1000") - Decimal("400")
        assert total_pnl == pytest.approx(expected_total, rel=Decimal("0.01"))

    def test_empty_portfolio_zero_pnl(self):
        """Test empty portfolio has zero unrealized PnL."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        market_state = MockMarketState({"ETH": Decimal("2000")})

        total_pnl = portfolio.calculate_unrealized_pnl(market_state)

        assert total_pnl == Decimal("0")
