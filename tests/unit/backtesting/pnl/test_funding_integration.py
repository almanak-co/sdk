"""Integration tests for perp funding application during backtests.

This module tests the funding calculation and accumulation over multi-day
backtests, including:
- Funding accumulation over multiple ticks (mark_to_market calls)
- Positive and negative funding rates
- Long vs short position funding behavior
- Accuracy of total_funding metrics in BacktestMetrics
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.calculators.funding import FundingCalculator
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.portfolio import (
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


class TestFundingAccumulationMultipleTicks:
    """Tests for funding accumulation over multiple mark_to_market calls."""

    def test_funding_accumulates_over_24_hours_long(self):
        """Test that funding accumulates correctly for a long position over 24 hours.

        Scenario:
        - PERP_LONG position with $50,000 notional
        - 0.01% hourly funding rate (longs pay)
        - 24 hours of mark_to_market calls (hourly)
        - Expected: ~$120 in funding paid
        """
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        # Create a long position
        long_position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),  # 5x leverage = $50,000 notional
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            entry_funding_index=Decimal("0"),
            protocol="gmx",
        )
        portfolio.positions.append(long_position)

        # Mock market state with constant price
        market_state = MockMarketState({"ETH": Decimal("2000")})

        # Simulate 24 hourly mark_to_market calls
        for hour in range(24):
            timestamp = entry_time + timedelta(hours=hour + 1)
            portfolio.mark_to_market(market_state, timestamp)

        # Verify funding has accumulated
        position = portfolio.positions[0]

        # Expected funding: $50,000 * 0.0001 * 24 = $120 (paid by long)
        expected_funding_paid = Decimal("50000") * Decimal("0.0001") * Decimal("24")

        # Long pays funding when rate is positive, so accumulated_funding should be negative
        assert position.accumulated_funding < Decimal("0")
        assert abs(position.accumulated_funding) == pytest.approx(
            expected_funding_paid, rel=Decimal("0.01")
        )

        # Cumulative funding paid should track the total
        assert position.cumulative_funding_paid == pytest.approx(
            expected_funding_paid, rel=Decimal("0.01")
        )
        assert position.cumulative_funding_received == Decimal("0")

    def test_funding_accumulates_over_24_hours_short(self):
        """Test that funding accumulates correctly for a short position over 24 hours.

        Scenario:
        - PERP_SHORT position with $50,000 notional
        - 0.01% hourly funding rate (shorts receive)
        - 24 hours of mark_to_market calls (hourly)
        - Expected: ~$120 in funding received
        """
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        # Create a short position
        short_position = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),  # 5x leverage = $50,000 notional
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            entry_funding_index=Decimal("0"),
            protocol="gmx",
        )
        portfolio.positions.append(short_position)

        # Mock market state with constant price
        market_state = MockMarketState({"ETH": Decimal("2000")})

        # Simulate 24 hourly mark_to_market calls
        for hour in range(24):
            timestamp = entry_time + timedelta(hours=hour + 1)
            portfolio.mark_to_market(market_state, timestamp)

        # Verify funding has accumulated
        position = portfolio.positions[0]

        # Expected funding: $50,000 * 0.0001 * 24 = $120 (received by short)
        expected_funding_received = Decimal("50000") * Decimal("0.0001") * Decimal("24")

        # Short receives funding when rate is positive, so accumulated_funding should be positive
        assert position.accumulated_funding > Decimal("0")
        assert position.accumulated_funding == pytest.approx(
            expected_funding_received, rel=Decimal("0.01")
        )

        # Cumulative funding received should track the total
        assert position.cumulative_funding_received == pytest.approx(
            expected_funding_received, rel=Decimal("0.01")
        )
        assert position.cumulative_funding_paid == Decimal("0")

    def test_funding_accumulates_over_7_days(self):
        """Test funding accumulation over a 7-day backtest period.

        Scenario:
        - PERP_LONG position with $100,000 notional
        - 0.01% hourly funding rate
        - 7 days of daily mark_to_market calls
        - Expected: ~$1,680 in funding paid (100000 * 0.0001 * 168 hours)
        """
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("200000"))

        # Create a long position with larger size
        long_position = SimulatedPosition.perp_long(
            token="BTC",
            collateral_usd=Decimal("20000"),
            leverage=Decimal("5"),  # $100,000 notional
            entry_price=Decimal("40000"),
            entry_time=entry_time,
            entry_funding_index=Decimal("0"),
            protocol="gmx",
        )
        portfolio.positions.append(long_position)

        # Mock market state
        market_state = MockMarketState({"BTC": Decimal("40000")})

        # Simulate 7 daily mark_to_market calls
        for day in range(7):
            timestamp = entry_time + timedelta(days=day + 1)
            portfolio.mark_to_market(market_state, timestamp)

        position = portfolio.positions[0]

        # Expected: $100,000 * 0.0001 * 168 hours = $1,680
        expected_funding = Decimal("100000") * Decimal("0.0001") * Decimal("168")

        assert position.cumulative_funding_paid == pytest.approx(
            expected_funding, rel=Decimal("0.01")
        )

    def test_funding_with_variable_time_intervals(self):
        """Test funding accumulation with irregular time intervals.

        Scenario:
        - PERP_SHORT position
        - Mark_to_market calls at: 1h, 6h, 12h, 24h
        - Funding should accumulate proportionally to time elapsed
        """
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("50000"))

        short_position = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("5000"),
            leverage=Decimal("3"),  # $15,000 notional
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        portfolio.positions.append(short_position)

        market_state = MockMarketState({"ETH": Decimal("2000")})

        # Mark at irregular intervals
        timestamps = [
            entry_time + timedelta(hours=1),
            entry_time + timedelta(hours=6),
            entry_time + timedelta(hours=12),
            entry_time + timedelta(hours=24),
        ]

        for ts in timestamps:
            portfolio.mark_to_market(market_state, ts)

        position = portfolio.positions[0]

        # Total time: 24 hours
        # Expected: $15,000 * 0.0001 * 24 = $36 received
        expected_funding = Decimal("15000") * Decimal("0.0001") * Decimal("24")

        assert position.cumulative_funding_received == pytest.approx(
            expected_funding, rel=Decimal("0.01")
        )


class TestPositiveFundingRates:
    """Tests for positive funding rates (longs pay, shorts receive)."""

    def test_positive_rate_long_pays(self):
        """Test that long positions pay funding with positive rates."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("50000"))

        long_position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("5000"),
            leverage=Decimal("2"),  # $10,000 notional
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",  # GMX uses 0.01% hourly = positive rate
        )
        portfolio.positions.append(long_position)

        market_state = MockMarketState({"ETH": Decimal("2000")})

        # Mark after 10 hours
        portfolio.mark_to_market(market_state, entry_time + timedelta(hours=10))

        position = portfolio.positions[0]

        # Positive funding rate: longs pay
        assert position.accumulated_funding < Decimal("0")
        assert position.cumulative_funding_paid > Decimal("0")
        assert position.cumulative_funding_received == Decimal("0")

    def test_positive_rate_short_receives(self):
        """Test that short positions receive funding with positive rates."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("50000"))

        short_position = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("5000"),
            leverage=Decimal("2"),  # $10,000 notional
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        portfolio.positions.append(short_position)

        market_state = MockMarketState({"ETH": Decimal("2000")})

        # Mark after 10 hours
        portfolio.mark_to_market(market_state, entry_time + timedelta(hours=10))

        position = portfolio.positions[0]

        # Positive funding rate: shorts receive
        assert position.accumulated_funding > Decimal("0")
        assert position.cumulative_funding_received > Decimal("0")
        assert position.cumulative_funding_paid == Decimal("0")


class TestNegativeFundingRates:
    """Tests for negative funding rates (shorts pay, longs receive)."""

    def test_funding_calculator_negative_rate_long_receives(self):
        """Test FundingCalculator with negative rate - long receives funding."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

        long_position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),  # $50,000 notional
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )

        calculator = FundingCalculator()

        # Negative funding rate: -0.01% per hour
        result = calculator.calculate_funding_payment(
            position=long_position,
            funding_rate=Decimal("-0.0001"),
            time_delta_hours=Decimal("24"),
        )

        # Long receives when rate is negative
        assert result.payment > Decimal("0")
        assert result.is_payer is False

        # Apply to position
        calculator.apply_funding_to_position(long_position, result)

        assert long_position.accumulated_funding > Decimal("0")
        assert long_position.cumulative_funding_received > Decimal("0")
        assert long_position.cumulative_funding_paid == Decimal("0")

    def test_funding_calculator_negative_rate_short_pays(self):
        """Test FundingCalculator with negative rate - short pays funding."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

        short_position = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),  # $50,000 notional
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )

        calculator = FundingCalculator()

        # Negative funding rate: -0.01% per hour
        result = calculator.calculate_funding_payment(
            position=short_position,
            funding_rate=Decimal("-0.0001"),
            time_delta_hours=Decimal("24"),
        )

        # Short pays when rate is negative
        assert result.payment < Decimal("0")
        assert result.is_payer is True

        # Apply to position
        calculator.apply_funding_to_position(short_position, result)

        assert short_position.accumulated_funding < Decimal("0")
        assert short_position.cumulative_funding_paid > Decimal("0")
        assert short_position.cumulative_funding_received == Decimal("0")


class TestLongVsShortFunding:
    """Tests comparing funding behavior between long and short positions."""

    def test_long_vs_short_opposite_funding(self):
        """Test that long and short positions have opposite funding directions.

        With positive funding rate, the amounts should be equal but opposite.
        """
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        # Create identical long and short positions
        long_position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),  # $50,000 notional
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        short_position = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),  # $50,000 notional
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )

        portfolio.positions.append(long_position)
        portfolio.positions.append(short_position)

        market_state = MockMarketState({"ETH": Decimal("2000")})

        # Mark after 24 hours
        portfolio.mark_to_market(market_state, entry_time + timedelta(hours=24))

        # The funding amounts should be equal but opposite
        long_funding = portfolio.positions[0].accumulated_funding
        short_funding = portfolio.positions[1].accumulated_funding

        # Long pays (negative), short receives (positive)
        assert long_funding < Decimal("0")
        assert short_funding > Decimal("0")

        # Amounts should be equal in absolute value
        assert abs(long_funding) == pytest.approx(abs(short_funding), rel=Decimal("0.001"))

    def test_long_short_cancel_out_in_metrics(self):
        """Test that equal long/short positions have funding that cancels out.

        If you have both a long and short of equal size, net funding should be zero.
        """
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        long_position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        short_position = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )

        portfolio.positions.append(long_position)
        portfolio.positions.append(short_position)

        market_state = MockMarketState({"ETH": Decimal("2000")})

        # Initial equity point
        portfolio.mark_to_market(market_state, entry_time)

        # Mark after 24 hours
        portfolio.mark_to_market(market_state, entry_time + timedelta(hours=24))

        metrics = portfolio.get_metrics()

        # Net funding = received - paid
        net_funding = metrics.total_funding_received - metrics.total_funding_paid

        # Should be approximately zero (positions cancel out)
        assert abs(net_funding) < Decimal("0.01")

    def test_multiple_long_positions_aggregate_funding(self):
        """Test that funding from multiple long positions aggregates correctly."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("200000"))

        # Create 3 long positions with different sizes
        positions = [
            SimulatedPosition.perp_long(
                token="ETH",
                collateral_usd=Decimal("10000"),
                leverage=Decimal("5"),  # $50,000 notional
                entry_price=Decimal("2000"),
                entry_time=entry_time,
                protocol="gmx",
            ),
            SimulatedPosition.perp_long(
                token="BTC",
                collateral_usd=Decimal("5000"),
                leverage=Decimal("2"),  # $10,000 notional
                entry_price=Decimal("40000"),
                entry_time=entry_time,
                protocol="gmx",
            ),
            SimulatedPosition.perp_long(
                token="SOL",
                collateral_usd=Decimal("2000"),
                leverage=Decimal("10"),  # $20,000 notional
                entry_price=Decimal("100"),
                entry_time=entry_time,
                protocol="gmx",
            ),
        ]

        for p in positions:
            portfolio.positions.append(p)

        market_state = MockMarketState({
            "ETH": Decimal("2000"),
            "BTC": Decimal("40000"),
            "SOL": Decimal("100"),
        })

        # Initial equity point
        portfolio.mark_to_market(market_state, entry_time)

        # Mark after 24 hours
        portfolio.mark_to_market(market_state, entry_time + timedelta(hours=24))

        metrics = portfolio.get_metrics()

        # Total notional: $50,000 + $10,000 + $20,000 = $80,000
        # Expected funding: $80,000 * 0.0001 * 24 = $192
        expected_funding = Decimal("80000") * Decimal("0.0001") * Decimal("24")

        assert metrics.total_funding_paid == pytest.approx(
            expected_funding, rel=Decimal("0.01")
        )
        assert metrics.total_funding_received == Decimal("0")


class TestTotalFundingMetricsAccuracy:
    """Tests for accuracy of total_funding metrics in BacktestMetrics."""

    def test_metrics_total_funding_paid_long_position(self):
        """Test that total_funding_paid is accurate for long positions."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        long_position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),  # $50,000 notional
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        portfolio.positions.append(long_position)

        market_state = MockMarketState({"ETH": Decimal("2000")})

        # Record initial equity
        portfolio.mark_to_market(market_state, entry_time)

        # Mark after 48 hours
        portfolio.mark_to_market(market_state, entry_time + timedelta(hours=48))

        metrics = portfolio.get_metrics()

        # Expected: $50,000 * 0.0001 * 48 = $240
        expected_paid = Decimal("50000") * Decimal("0.0001") * Decimal("48")

        assert metrics.total_funding_paid == pytest.approx(
            expected_paid, rel=Decimal("0.01")
        )
        assert metrics.total_funding_received == Decimal("0")

    def test_metrics_total_funding_received_short_position(self):
        """Test that total_funding_received is accurate for short positions."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        short_position = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),  # $50,000 notional
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        portfolio.positions.append(short_position)

        market_state = MockMarketState({"ETH": Decimal("2000")})

        # Record initial equity
        portfolio.mark_to_market(market_state, entry_time)

        # Mark after 48 hours
        portfolio.mark_to_market(market_state, entry_time + timedelta(hours=48))

        metrics = portfolio.get_metrics()

        # Expected: $50,000 * 0.0001 * 48 = $240
        expected_received = Decimal("50000") * Decimal("0.0001") * Decimal("48")

        assert metrics.total_funding_received == pytest.approx(
            expected_received, rel=Decimal("0.01")
        )
        assert metrics.total_funding_paid == Decimal("0")

    def test_metrics_include_closed_positions(self):
        """Test that metrics include funding from closed positions."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        # Create position
        short_position = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        portfolio.positions.append(short_position)

        market_state = MockMarketState({"ETH": Decimal("2000")})

        # Record initial equity
        portfolio.mark_to_market(market_state, entry_time)

        # Mark after 24 hours - accumulates funding
        portfolio.mark_to_market(market_state, entry_time + timedelta(hours=24))

        # Manually close the position (simulate PERP_CLOSE)
        close_time = entry_time + timedelta(hours=24)
        portfolio._close_position(short_position.position_id, close_time)

        # Get metrics after position is closed
        metrics = portfolio.get_metrics()

        # Expected: $50,000 * 0.0001 * 24 = $120
        expected_received = Decimal("50000") * Decimal("0.0001") * Decimal("24")

        # Funding from closed position should be included
        assert metrics.total_funding_received == pytest.approx(
            expected_received, rel=Decimal("0.01")
        )

    def test_metrics_mixed_long_short_positions(self):
        """Test metrics with a mix of long and short positions.

        Creates positions with different sizes to verify proper aggregation.
        """
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("200000"))

        # Long position: $60,000 notional
        long_position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("12000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )

        # Short position: $40,000 notional
        short_position = SimulatedPosition.perp_short(
            token="BTC",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("4"),
            entry_price=Decimal("40000"),
            entry_time=entry_time,
            protocol="gmx",
        )

        portfolio.positions.append(long_position)
        portfolio.positions.append(short_position)

        market_state = MockMarketState({
            "ETH": Decimal("2000"),
            "BTC": Decimal("40000"),
        })

        # Record initial equity
        portfolio.mark_to_market(market_state, entry_time)

        # Mark after 24 hours
        portfolio.mark_to_market(market_state, entry_time + timedelta(hours=24))

        metrics = portfolio.get_metrics()

        # Long pays: $60,000 * 0.0001 * 24 = $144
        expected_paid = Decimal("60000") * Decimal("0.0001") * Decimal("24")

        # Short receives: $40,000 * 0.0001 * 24 = $96
        expected_received = Decimal("40000") * Decimal("0.0001") * Decimal("24")

        assert metrics.total_funding_paid == pytest.approx(
            expected_paid, rel=Decimal("0.01")
        )
        assert metrics.total_funding_received == pytest.approx(
            expected_received, rel=Decimal("0.01")
        )

    def test_metrics_serialization_roundtrip(self):
        """Test that funding metrics survive serialization roundtrip."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        long_position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        portfolio.positions.append(long_position)

        market_state = MockMarketState({"ETH": Decimal("2000")})
        portfolio.mark_to_market(market_state, entry_time)
        portfolio.mark_to_market(market_state, entry_time + timedelta(hours=24))

        metrics = portfolio.get_metrics()

        # Serialize
        metrics_dict = metrics.to_dict()

        # Verify serialized values
        assert "total_funding_paid" in metrics_dict
        assert "total_funding_received" in metrics_dict

        # The serialized values should be string representations
        assert metrics_dict["total_funding_paid"] == str(metrics.total_funding_paid)
        assert metrics_dict["total_funding_received"] == str(metrics.total_funding_received)


class TestFundingEdgeCases:
    """Tests for edge cases in funding calculation."""

    def test_zero_time_elapsed_no_funding(self):
        """Test that zero time elapsed results in no funding."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        long_position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        portfolio.positions.append(long_position)

        market_state = MockMarketState({"ETH": Decimal("2000")})

        # Mark at entry time (zero elapsed)
        portfolio.mark_to_market(market_state, entry_time)

        position = portfolio.positions[0]
        assert position.accumulated_funding == Decimal("0")
        assert position.cumulative_funding_paid == Decimal("0")
        assert position.cumulative_funding_received == Decimal("0")

    def test_very_small_position_funding(self):
        """Test funding calculation for very small positions."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("1000"))

        # Very small position: $100 notional
        small_position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("100"),
            leverage=Decimal("1"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        portfolio.positions.append(small_position)

        market_state = MockMarketState({"ETH": Decimal("2000")})

        portfolio.mark_to_market(market_state, entry_time + timedelta(hours=24))

        position = portfolio.positions[0]

        # Expected: $100 * 0.0001 * 24 = $0.24
        expected_funding = Decimal("100") * Decimal("0.0001") * Decimal("24")

        assert position.cumulative_funding_paid == pytest.approx(
            expected_funding, rel=Decimal("0.01")
        )

    def test_very_large_position_funding(self):
        """Test funding calculation for very large positions."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000000"))

        # Large position: $1,000,000 notional
        large_position = SimulatedPosition.perp_long(
            token="BTC",
            collateral_usd=Decimal("100000"),
            leverage=Decimal("10"),
            entry_price=Decimal("40000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        portfolio.positions.append(large_position)

        market_state = MockMarketState({"BTC": Decimal("40000")})

        portfolio.mark_to_market(market_state, entry_time + timedelta(hours=24))

        position = portfolio.positions[0]

        # Expected: $1,000,000 * 0.0001 * 24 = $2,400
        expected_funding = Decimal("1000000") * Decimal("0.0001") * Decimal("24")

        assert position.cumulative_funding_paid == pytest.approx(
            expected_funding, rel=Decimal("0.01")
        )

    def test_funding_with_price_change(self):
        """Test that funding is calculated based on notional, not affected by price change."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100000"))

        long_position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),  # $50,000 notional at entry
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        portfolio.positions.append(long_position)

        # Price doubles - but notional is based on entry
        market_state = MockMarketState({"ETH": Decimal("4000")})

        portfolio.mark_to_market(market_state, entry_time + timedelta(hours=24))

        position = portfolio.positions[0]

        # Funding still based on entry notional of $50,000
        expected_funding = Decimal("50000") * Decimal("0.0001") * Decimal("24")

        assert position.cumulative_funding_paid == pytest.approx(
            expected_funding, rel=Decimal("0.01")
        )

    def test_protocol_specific_funding_rate(self):
        """Test that different protocols can have different funding rates."""
        calculator = FundingCalculator()

        # Check protocol-specific rates
        gmx_rate = calculator.get_funding_rate_for_protocol("gmx")
        binance_rate = calculator.get_funding_rate_for_protocol("binance_perp")

        # GMX: 0.01% per hour
        assert gmx_rate == Decimal("0.0001")

        # Binance: ~0.0125% per hour (0.1% per 8h / 8)
        assert binance_rate == Decimal("0.000125")

    def test_funding_rate_clamping(self):
        """Test that extreme funding rates are clamped to bounds."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

        long_position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )

        calculator = FundingCalculator()

        # Extreme positive rate (should be clamped to 1%)
        result = calculator.calculate_funding_payment(
            position=long_position,
            funding_rate=Decimal("0.05"),  # 5% - exceeds max
            time_delta_hours=Decimal("1"),
        )

        # Should be clamped to max (1%)
        assert result.funding_rate == Decimal("0.01")

        # Extreme negative rate (should be clamped to -1%)
        result = calculator.calculate_funding_payment(
            position=long_position,
            funding_rate=Decimal("-0.05"),  # -5% - exceeds min
            time_delta_hours=Decimal("1"),
        )

        # Should be clamped to min (-1%)
        assert result.funding_rate == Decimal("-0.01")
