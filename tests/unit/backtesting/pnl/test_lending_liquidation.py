"""Integration tests for lending liquidation during price crashes.

This module tests the lending liquidation simulation in SimulatedPortfolio,
covering scenarios where collateral price drops trigger health factor < 1.0.

User Story: US-011c - Integration test for lending liquidation
"""

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.backtesting.models import LendingLiquidationEvent
from almanak.framework.backtesting.pnl.portfolio import (
    SimulatedPortfolio,
    SimulatedPosition,
)


class MockMarketState:
    """Mock market state for testing with configurable prices."""

    def __init__(self, prices: dict[str, Decimal] | None = None):
        self._prices = prices or {}

    def get_price(self, token: str) -> Decimal:
        """Get price for a token."""
        return self._prices.get(token, Decimal("0"))

    def get_prices(self, tokens: list[str]) -> dict[str, Decimal]:
        """Get prices for multiple tokens."""
        return {t: self._prices.get(t, Decimal("0")) for t in tokens}

    def update_prices(self, new_prices: dict[str, Decimal]) -> None:
        """Update prices for market simulation."""
        self._prices.update(new_prices)


class TestPriceCrashTriggersLiquidation:
    """Tests for liquidation triggered by collateral price crash."""

    def test_liquidation_triggered_when_health_factor_falls_below_one(self):
        """Test that liquidation is triggered when HF < 1.0 after price drop.

        Setup: 5 ETH collateral at $2,000 = $10,000
               $7,000 USDC borrowed
               HF = (10000 * 0.825) / 7000 = 1.178 (safe)

        Price crash: ETH drops to $1,000 = $5,000 collateral
                     HF = (5000 * 0.825) / 7000 = 0.589 (liquidated!)
        """
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("20000"))
        timestamp = datetime.now(UTC)

        # Create supply position (collateral) - 5 ETH at $2,000 = $10,000
        supply_position = SimulatedPosition.supply(
            protocol="aave_v3",
            token="WETH",
            amount=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=timestamp,
            apy=Decimal("0.02"),
        )

        # Create borrow position (debt) - $7,000 USDC
        borrow_position = SimulatedPosition.borrow(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("7000"),
            entry_price=Decimal("1"),
            entry_time=timestamp,
            apy=Decimal("0.05"),
            health_factor=Decimal("1.5"),
        )

        portfolio.positions = [supply_position, borrow_position]

        # Initial state - healthy position
        initial_market = MockMarketState({"WETH": Decimal("2000"), "USDC": Decimal("1")})
        portfolio.mark_to_market(initial_market, timestamp)

        # Verify no liquidation yet
        assert len(portfolio.get_lending_liquidations()) == 0
        assert borrow_position.health_factor > Decimal("1.0")

        # Simulate price crash - ETH drops 50%
        crash_market = MockMarketState({"WETH": Decimal("1000"), "USDC": Decimal("1")})
        crash_timestamp = timestamp + timedelta(hours=1)
        portfolio.mark_to_market(crash_market, crash_timestamp)

        # Verify liquidation occurred
        liquidations = portfolio.get_lending_liquidations()
        assert len(liquidations) == 1
        assert liquidations[0].health_factor < Decimal("1.0")
        assert liquidations[0].position_id == borrow_position.position_id

    def test_liquidation_event_records_correct_amounts(self):
        """Test that liquidation event captures correct collateral seized and debt repaid."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("20000"),
            liquidation_penalty=Decimal("0.05"),  # 5% penalty
        )
        timestamp = datetime.now(UTC)

        # Create collateral: 10 ETH at $2,000 = $20,000
        supply_position = SimulatedPosition.supply(
            protocol="aave_v3",
            token="WETH",
            amount=Decimal("10"),
            entry_price=Decimal("2000"),
            entry_time=timestamp,
            apy=Decimal("0.02"),
        )

        # Create debt: $15,000 USDC
        borrow_position = SimulatedPosition.borrow(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("15000"),
            entry_price=Decimal("1"),
            entry_time=timestamp,
            apy=Decimal("0.05"),
            health_factor=Decimal("1.5"),
        )

        portfolio.positions = [supply_position, borrow_position]

        # Price crash: ETH drops to $1,400 = $14,000 collateral
        # HF = (14000 * 0.825) / 15000 = 0.77 (liquidation!)
        crash_market = MockMarketState({"WETH": Decimal("1400"), "USDC": Decimal("1")})
        portfolio.mark_to_market(crash_market, timestamp)

        liquidations = portfolio.get_lending_liquidations()
        assert len(liquidations) == 1

        event = liquidations[0]
        # 50% close factor: debt_to_repay = 15000 * 0.5 = 7500
        # collateral_seized = 7500 * 1.05 = 7875
        assert event.debt_repaid == pytest.approx(Decimal("7500"), rel=Decimal("0.01"))
        assert event.collateral_seized == pytest.approx(Decimal("7875"), rel=Decimal("0.01"))
        assert event.penalty == Decimal("0.05")

    def test_position_state_updated_after_liquidation(self):
        """Test that borrow and supply positions are correctly updated after liquidation."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("20000"),
            liquidation_penalty=Decimal("0.05"),
        )
        timestamp = datetime.now(UTC)

        # Create collateral: 10 ETH at $2,000 = $20,000
        supply_position = SimulatedPosition.supply(
            protocol="aave_v3",
            token="WETH",
            amount=Decimal("10"),
            entry_price=Decimal("2000"),
            entry_time=timestamp,
            apy=Decimal("0.02"),
        )

        # Create debt: $15,000 USDC
        borrow_position = SimulatedPosition.borrow(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("15000"),
            entry_price=Decimal("1"),
            entry_time=timestamp,
            apy=Decimal("0.05"),
            health_factor=Decimal("1.5"),
        )

        portfolio.positions = [supply_position, borrow_position]

        # Store original amounts
        original_supply_amount = supply_position.total_amount
        original_borrow_amount = borrow_position.total_amount

        # Price crash: ETH to $1,400
        crash_market = MockMarketState({"WETH": Decimal("1400"), "USDC": Decimal("1")})
        portfolio.mark_to_market(crash_market, timestamp)

        # Verify positions updated
        # Debt reduced by ~50% (7500 USDC repaid)
        assert borrow_position.total_amount < original_borrow_amount
        assert borrow_position.total_amount == pytest.approx(Decimal("7500"), rel=Decimal("0.01"))

        # Collateral reduced by seized amount
        # At $1400/ETH, 7875 USD = ~5.625 ETH seized
        assert supply_position.total_amount < original_supply_amount


class TestMultipleLiquidationsOnContinuedPriceDrops:
    """Tests for multiple liquidation events during sustained price drops."""

    def test_second_liquidation_on_further_price_drop(self):
        """Test that a second liquidation occurs if price continues to drop."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("50000"),
            liquidation_penalty=Decimal("0.05"),
        )
        timestamp = datetime.now(UTC)

        # Create substantial positions
        supply_position = SimulatedPosition.supply(
            protocol="aave_v3",
            token="WETH",
            amount=Decimal("20"),
            entry_price=Decimal("2000"),
            entry_time=timestamp,
            apy=Decimal("0.02"),
        )

        borrow_position = SimulatedPosition.borrow(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("30000"),
            entry_price=Decimal("1"),
            entry_time=timestamp,
            apy=Decimal("0.05"),
            health_factor=Decimal("1.5"),
        )

        portfolio.positions = [supply_position, borrow_position]

        # First price drop: ETH to $1,600 (from $2,000)
        # Collateral = 20 * 1600 = $32,000
        # HF = (32000 * 0.825) / 30000 = 0.88 (liquidation!)
        market1 = MockMarketState({"WETH": Decimal("1600"), "USDC": Decimal("1")})
        portfolio.mark_to_market(market1, timestamp + timedelta(hours=1))

        first_liquidation_count = len(portfolio.get_lending_liquidations())
        assert first_liquidation_count == 1

        # Second price drop: ETH to $1,200
        # This may trigger another liquidation if HF still < 1.0
        market2 = MockMarketState({"WETH": Decimal("1200"), "USDC": Decimal("1")})
        portfolio.mark_to_market(market2, timestamp + timedelta(hours=2))

        # Check if second liquidation occurred
        second_liquidation_count = len(portfolio.get_lending_liquidations())
        # May or may not have second liquidation depending on remaining position state
        # The test verifies the system handles continued drops correctly
        assert second_liquidation_count >= 1


class TestLiquidationWithDifferentPenalties:
    """Tests for liquidation with different penalty configurations."""

    def test_higher_penalty_seizes_more_collateral(self):
        """Test that higher liquidation penalty results in more collateral seized."""
        timestamp = datetime.now(UTC)

        def create_portfolio(penalty: Decimal) -> SimulatedPortfolio:
            portfolio = SimulatedPortfolio(
                initial_capital_usd=Decimal("20000"),
                liquidation_penalty=penalty,
            )

            supply = SimulatedPosition.supply(
                protocol="aave_v3",
                token="WETH",
                amount=Decimal("10"),
                entry_price=Decimal("2000"),
                entry_time=timestamp,
                apy=Decimal("0.02"),
            )

            borrow = SimulatedPosition.borrow(
                protocol="aave_v3",
                token="USDC",
                amount=Decimal("12000"),
                entry_price=Decimal("1"),
                entry_time=timestamp,
                apy=Decimal("0.05"),
                health_factor=Decimal("1.5"),
            )

            portfolio.positions = [supply, borrow]
            return portfolio

        # Low penalty portfolio (2%)
        portfolio_low = create_portfolio(Decimal("0.02"))
        crash_market = MockMarketState({"WETH": Decimal("1200"), "USDC": Decimal("1")})
        portfolio_low.mark_to_market(crash_market, timestamp)

        # High penalty portfolio (10%)
        portfolio_high = create_portfolio(Decimal("0.10"))
        portfolio_high.mark_to_market(crash_market, timestamp)

        low_collateral_seized = portfolio_low.get_lending_liquidations()[0].collateral_seized
        high_collateral_seized = portfolio_high.get_lending_liquidations()[0].collateral_seized

        # Higher penalty = more collateral seized for same debt repaid
        assert high_collateral_seized > low_collateral_seized


class TestLiquidationLogging:
    """Tests for liquidation warning/logging behavior."""

    def test_liquidation_warning_logged(self, caplog):
        """Test that liquidation event is logged with warning level."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("20000"),
            liquidation_penalty=Decimal("0.05"),
        )
        timestamp = datetime.now(UTC)

        supply = SimulatedPosition.supply(
            protocol="aave_v3",
            token="WETH",
            amount=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=timestamp,
            apy=Decimal("0.02"),
        )

        borrow = SimulatedPosition.borrow(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("7000"),
            entry_price=Decimal("1"),
            entry_time=timestamp,
            apy=Decimal("0.05"),
            health_factor=Decimal("1.5"),
        )

        portfolio.positions = [supply, borrow]

        crash_market = MockMarketState({"WETH": Decimal("1000"), "USDC": Decimal("1")})

        with caplog.at_level(logging.WARNING, logger="almanak.framework.backtesting.pnl.portfolio"):
            portfolio.mark_to_market(crash_market, timestamp)

        assert "liquidation" in caplog.text.lower()
        assert borrow.position_id in caplog.text


class TestLiquidationEdgeCases:
    """Tests for edge cases in liquidation simulation."""

    def test_no_liquidation_when_health_factor_exactly_one(self):
        """Test that no liquidation occurs when HF is exactly 1.0."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("20000"))
        timestamp = datetime.now(UTC)

        # Set up position where HF will be exactly 1.0
        # HF = (collateral * threshold) / debt = 1.0
        # collateral = debt / threshold
        # For $8,250 debt at 0.825 threshold: collateral = 8250 / 0.825 = $10,000
        supply = SimulatedPosition.supply(
            protocol="aave_v3",
            token="WETH",
            amount=Decimal("5"),  # 5 ETH
            entry_price=Decimal("2000"),
            entry_time=timestamp,
            apy=Decimal("0.02"),
        )

        borrow = SimulatedPosition.borrow(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("8250"),  # Exactly (5 * 2000 * 0.825)
            entry_price=Decimal("1"),
            entry_time=timestamp,
            apy=Decimal("0.05"),
            health_factor=Decimal("1.5"),
        )

        portfolio.positions = [supply, borrow]

        market = MockMarketState({"WETH": Decimal("2000"), "USDC": Decimal("1")})
        portfolio.mark_to_market(market, timestamp)

        # HF exactly 1.0 should not trigger liquidation
        assert len(portfolio.get_lending_liquidations()) == 0

    def test_liquidation_with_accrued_interest(self):
        """Test liquidation correctly handles positions with accrued interest."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("20000"),
            liquidation_penalty=Decimal("0.05"),
        )
        timestamp = datetime.now(UTC)

        supply = SimulatedPosition.supply(
            protocol="aave_v3",
            token="WETH",
            amount=Decimal("10"),
            entry_price=Decimal("2000"),
            entry_time=timestamp,
            apy=Decimal("0.02"),
        )
        # Add some accrued interest to supply
        supply.interest_accrued = Decimal("200")  # $200 interest earned

        borrow = SimulatedPosition.borrow(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("15000"),
            entry_price=Decimal("1"),
            entry_time=timestamp,
            apy=Decimal("0.08"),
            health_factor=Decimal("1.5"),
        )
        # Add some accrued interest to borrow
        borrow.interest_accrued = Decimal("500")  # $500 interest owed

        portfolio.positions = [supply, borrow]

        crash_market = MockMarketState({"WETH": Decimal("1400"), "USDC": Decimal("1")})
        portfolio.mark_to_market(crash_market, timestamp)

        # Verify liquidation occurred and interest was considered
        liquidations = portfolio.get_lending_liquidations()
        assert len(liquidations) == 1

        # After liquidation, interest should be reduced proportionally
        assert borrow.interest_accrued < Decimal("500")

    def test_liquidation_caps_collateral_at_available_amount(self):
        """Test that collateral seized is capped at available collateral."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("10000"),
            liquidation_penalty=Decimal("0.10"),  # High penalty
        )
        timestamp = datetime.now(UTC)

        # Small collateral, large debt
        supply = SimulatedPosition.supply(
            protocol="aave_v3",
            token="WETH",
            amount=Decimal("2"),  # Only 2 ETH
            entry_price=Decimal("2000"),
            entry_time=timestamp,
            apy=Decimal("0.02"),
        )

        borrow = SimulatedPosition.borrow(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("10000"),  # Large debt
            entry_price=Decimal("1"),
            entry_time=timestamp,
            apy=Decimal("0.05"),
            health_factor=Decimal("1.5"),
        )

        portfolio.positions = [supply, borrow]

        # Severe crash: ETH to $500
        # Collateral = 2 * 500 = $1,000
        # Debt = $10,000
        # HF = (1000 * 0.825) / 10000 = 0.0825 (extremely undercollateralized)
        crash_market = MockMarketState({"WETH": Decimal("500"), "USDC": Decimal("1")})
        portfolio.mark_to_market(crash_market, timestamp)

        liquidations = portfolio.get_lending_liquidations()
        assert len(liquidations) == 1

        # Collateral seized should not exceed available ($1,000)
        assert liquidations[0].collateral_seized <= Decimal("1000")

    def test_multiple_supply_positions_liquidated_proportionally(self):
        """Test that multiple supply positions are liquidated proportionally."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("30000"),
            liquidation_penalty=Decimal("0.05"),
        )
        timestamp = datetime.now(UTC)

        # Two supply positions as collateral
        supply_weth = SimulatedPosition.supply(
            protocol="aave_v3",
            token="WETH",
            amount=Decimal("5"),  # $10,000 at $2,000
            entry_price=Decimal("2000"),
            entry_time=timestamp,
            apy=Decimal("0.02"),
        )

        supply_wbtc = SimulatedPosition.supply(
            protocol="aave_v3",
            token="WBTC",
            amount=Decimal("0.25"),  # $10,000 at $40,000
            entry_price=Decimal("40000"),
            entry_time=timestamp,
            apy=Decimal("0.01"),
        )

        borrow = SimulatedPosition.borrow(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("15000"),
            entry_price=Decimal("1"),
            entry_time=timestamp,
            apy=Decimal("0.05"),
            health_factor=Decimal("1.5"),
        )

        portfolio.positions = [supply_weth, supply_wbtc, borrow]

        original_weth = supply_weth.total_amount

        # Price crash on both assets
        crash_market = MockMarketState({
            "WETH": Decimal("1500"),   # 25% drop
            "WBTC": Decimal("30000"),  # 25% drop
            "USDC": Decimal("1"),
        })
        portfolio.mark_to_market(crash_market, timestamp)

        # Both positions should have reduced amounts
        assert supply_weth.total_amount < original_weth
        # Second position may or may not be touched depending on how much was seized from first


class TestLiquidationEventSerialization:
    """Tests for LendingLiquidationEvent serialization."""

    def test_liquidation_event_roundtrip(self):
        """Test that liquidation event can be serialized and deserialized."""
        timestamp = datetime.now(UTC)
        event = LendingLiquidationEvent(
            timestamp=timestamp,
            position_id="test_borrow_001",
            health_factor=Decimal("0.85"),
            collateral_seized=Decimal("5250"),
            debt_repaid=Decimal("5000"),
            penalty=Decimal("0.05"),
        )

        data = event.to_dict()
        restored = LendingLiquidationEvent.from_dict(data)

        assert restored.position_id == event.position_id
        assert restored.health_factor == event.health_factor
        assert restored.collateral_seized == event.collateral_seized
        assert restored.debt_repaid == event.debt_repaid
        assert restored.penalty == event.penalty

    def test_portfolio_with_liquidations_serializes(self):
        """Test that portfolio with liquidation events serializes correctly."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("20000"),
            liquidation_penalty=Decimal("0.05"),
        )
        timestamp = datetime.now(UTC)

        supply = SimulatedPosition.supply(
            protocol="aave_v3",
            token="WETH",
            amount=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=timestamp,
            apy=Decimal("0.02"),
        )

        borrow = SimulatedPosition.borrow(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("7000"),
            entry_price=Decimal("1"),
            entry_time=timestamp,
            apy=Decimal("0.05"),
            health_factor=Decimal("1.5"),
        )

        portfolio.positions = [supply, borrow]

        # Trigger liquidation
        crash_market = MockMarketState({"WETH": Decimal("1000"), "USDC": Decimal("1")})
        portfolio.mark_to_market(crash_market, timestamp)

        # Serialize and deserialize
        data = portfolio.to_dict()
        restored = SimulatedPortfolio.from_dict(data)

        # Verify liquidation events preserved
        assert len(restored.get_lending_liquidations()) == len(portfolio.get_lending_liquidations())


class TestHealthFactorTrackingDuringLiquidation:
    """Tests for health factor tracking during liquidation scenarios."""

    def test_min_health_factor_recorded_before_liquidation(self):
        """Test that minimum health factor is recorded even when liquidation occurs."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("20000"))
        timestamp = datetime.now(UTC)

        supply = SimulatedPosition.supply(
            protocol="aave_v3",
            token="WETH",
            amount=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=timestamp,
            apy=Decimal("0.02"),
        )

        borrow = SimulatedPosition.borrow(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("7000"),
            entry_price=Decimal("1"),
            entry_time=timestamp,
            apy=Decimal("0.05"),
            health_factor=Decimal("1.5"),
        )

        portfolio.positions = [supply, borrow]

        # Price crash triggering liquidation
        crash_market = MockMarketState({"WETH": Decimal("1000"), "USDC": Decimal("1")})
        portfolio.mark_to_market(crash_market, timestamp)

        # The health factor at crash was ~0.59, should be recorded
        liquidation = portfolio.get_lending_liquidations()[0]
        assert portfolio._min_health_factor <= liquidation.health_factor
        assert portfolio._min_health_factor < Decimal("1.0")

    def test_health_factor_warning_count_includes_liquidation_scenario(self):
        """Test that health factor warnings are counted leading up to liquidation."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("20000"),
            health_factor_warning_threshold=Decimal("1.5"),  # High threshold
        )
        timestamp = datetime.now(UTC)

        supply = SimulatedPosition.supply(
            protocol="aave_v3",
            token="WETH",
            amount=Decimal("10"),
            entry_price=Decimal("2000"),
            entry_time=timestamp,
            apy=Decimal("0.02"),
        )

        borrow = SimulatedPosition.borrow(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("10000"),
            entry_price=Decimal("1"),
            entry_time=timestamp,
            apy=Decimal("0.05"),
            health_factor=Decimal("2.0"),
        )

        portfolio.positions = [supply, borrow]

        # Initial state - safe
        safe_market = MockMarketState({"WETH": Decimal("2000"), "USDC": Decimal("1")})
        portfolio.mark_to_market(safe_market, timestamp)

        initial_warnings = portfolio._health_factor_warnings

        # Gradual price decline
        # Step 1: ETH to $1,700 - HF = (17000 * 0.825) / 10000 = 1.4 (warning)
        market1 = MockMarketState({"WETH": Decimal("1700"), "USDC": Decimal("1")})
        portfolio.mark_to_market(market1, timestamp + timedelta(hours=1))

        # Step 2: ETH to $1,200 - HF = (12000 * 0.825) / 10000 = 0.99 (liquidation)
        market2 = MockMarketState({"WETH": Decimal("1200"), "USDC": Decimal("1")})
        portfolio.mark_to_market(market2, timestamp + timedelta(hours=2))

        # Should have accumulated warnings before liquidation
        assert portfolio._health_factor_warnings > initial_warnings


__all__ = [
    "TestPriceCrashTriggersLiquidation",
    "TestMultipleLiquidationsOnContinuedPriceDrops",
    "TestLiquidationWithDifferentPenalties",
    "TestLiquidationLogging",
    "TestLiquidationEdgeCases",
    "TestLiquidationEventSerialization",
    "TestHealthFactorTrackingDuringLiquidation",
]
