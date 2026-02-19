"""Integration tests for lending backtest adapter functionality.

This module tests the LendingBacktestAdapter, focusing on:
- Interest accrual accuracy over time (compound and simple)
- Health factor tracking and warnings
- Liquidation triggered by undercollateralization
- Position valuation with accrued interest
- Collateral/debt management

User Story: US-044c - Integration test for lending adapter
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.backtesting.adapters.lending_adapter import (
    LendingBacktestAdapter,
    LendingBacktestConfig,
)
from almanak.framework.backtesting.pnl.portfolio import (
    PositionType,
    SimulatedPortfolio,
    SimulatedPosition,
)

# =============================================================================
# Mock Classes
# =============================================================================


@dataclass
class MockMarketState:
    """Mock market state for testing."""

    prices: dict[str, Decimal] = field(default_factory=dict)

    def get_price(self, token: str) -> Decimal:
        """Get price for a token.

        Raises:
            KeyError: If token not found in prices.
        """
        if token not in self.prices:
            raise KeyError(f"Price not found for {token}")
        return self.prices[token]

    def get_prices(self, tokens: list[str]) -> dict[str, Decimal]:
        """Get prices for multiple tokens."""
        return {t: self.get_price(t) for t in tokens if t in self.prices}


def create_supply_position(
    token: str = "WETH",
    amount: Decimal = Decimal("10"),
    entry_price: Decimal = Decimal("2000"),
    entry_time: datetime | None = None,
    apy: Decimal = Decimal("0.03"),
    protocol: str = "aave_v3",
) -> SimulatedPosition:
    """Create a mock supply position for testing."""
    if entry_time is None:
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

    return SimulatedPosition.supply(
        protocol=protocol,
        token=token,
        amount=amount,
        entry_price=entry_price,
        entry_time=entry_time,
        apy=apy,
    )


def create_borrow_position(
    token: str = "USDC",
    amount: Decimal = Decimal("10000"),
    entry_price: Decimal = Decimal("1"),
    entry_time: datetime | None = None,
    apy: Decimal = Decimal("0.05"),
    protocol: str = "aave_v3",
    health_factor: Decimal = Decimal("1.5"),
) -> SimulatedPosition:
    """Create a mock borrow position for testing."""
    if entry_time is None:
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

    return SimulatedPosition.borrow(
        protocol=protocol,
        token=token,
        amount=amount,
        entry_price=entry_price,
        entry_time=entry_time,
        apy=apy,
        health_factor=health_factor,
    )


# =============================================================================
# LendingBacktestConfig Tests
# =============================================================================


class TestLendingBacktestConfig:
    """Tests for LendingBacktestConfig."""

    def test_default_config(self) -> None:
        """Test default configuration values."""
        config = LendingBacktestConfig(strategy_type="lending")

        assert config.strategy_type == "lending"
        assert config.interest_accrual_method == "compound"
        assert config.health_factor_tracking_enabled is True
        assert config.liquidation_threshold == Decimal("0.825")
        assert config.health_factor_warning_threshold == Decimal("1.2")
        assert config.health_factor_critical_threshold == Decimal("1.05")
        assert config.liquidation_model_enabled is True
        assert config.liquidation_penalty == Decimal("0.05")
        assert config.liquidation_close_factor == Decimal("0.5")
        assert config.default_supply_apy == Decimal("0.03")
        assert config.default_borrow_apy == Decimal("0.05")
        assert config.interest_rate_source == "fixed"
        assert config.protocol == "aave_v3"

    def test_custom_config(self) -> None:
        """Test custom configuration values."""
        config = LendingBacktestConfig(
            strategy_type="lending",
            interest_accrual_method="simple",
            health_factor_tracking_enabled=False,
            liquidation_threshold=Decimal("0.80"),
            liquidation_penalty=Decimal("0.10"),
            default_supply_apy=Decimal("0.05"),
            default_borrow_apy=Decimal("0.08"),
            interest_rate_source="historical",
            protocol="compound_v3",
        )

        assert config.interest_accrual_method == "simple"
        assert config.health_factor_tracking_enabled is False
        assert config.liquidation_threshold == Decimal("0.80")
        assert config.liquidation_penalty == Decimal("0.10")
        assert config.default_supply_apy == Decimal("0.05")
        assert config.default_borrow_apy == Decimal("0.08")
        assert config.interest_rate_source == "historical"
        assert config.protocol == "compound_v3"

    def test_invalid_strategy_type(self) -> None:
        """Test validation rejects non-lending strategy type."""
        with pytest.raises(ValueError, match="requires strategy_type='lending'"):
            LendingBacktestConfig(strategy_type="perp")

    def test_invalid_interest_accrual_method(self) -> None:
        """Test validation rejects invalid interest accrual method."""
        with pytest.raises(ValueError, match="interest_accrual_method must be one of"):
            LendingBacktestConfig(strategy_type="lending", interest_accrual_method="linear")  # type: ignore[arg-type]

    def test_invalid_interest_rate_source(self) -> None:
        """Test validation rejects invalid interest rate source."""
        with pytest.raises(ValueError, match="interest_rate_source must be one of"):
            LendingBacktestConfig(strategy_type="lending", interest_rate_source="invalid")  # type: ignore[arg-type]

    def test_invalid_liquidation_threshold(self) -> None:
        """Test validation rejects invalid liquidation threshold."""
        with pytest.raises(ValueError, match="liquidation_threshold must be in"):
            LendingBacktestConfig(strategy_type="lending", liquidation_threshold=Decimal("0"))

        with pytest.raises(ValueError, match="liquidation_threshold must be in"):
            LendingBacktestConfig(strategy_type="lending", liquidation_threshold=Decimal("1.5"))

    def test_invalid_health_factor_thresholds(self) -> None:
        """Test validation rejects invalid health factor thresholds."""
        # Warning threshold must be > 1
        with pytest.raises(ValueError, match="health_factor_warning_threshold must be > 1"):
            LendingBacktestConfig(strategy_type="lending", health_factor_warning_threshold=Decimal("0.9"))

        # Critical must be < warning
        with pytest.raises(ValueError, match=r"health_factor_critical_threshold.*must be <"):
            LendingBacktestConfig(
                strategy_type="lending",
                health_factor_warning_threshold=Decimal("1.2"),
                health_factor_critical_threshold=Decimal("1.3"),
            )

    def test_to_dict(self) -> None:
        """Test serialization to dictionary."""
        config = LendingBacktestConfig(
            strategy_type="lending",
            interest_accrual_method="simple",
            default_supply_apy=Decimal("0.04"),
        )

        d = config.to_dict()

        assert d["strategy_type"] == "lending"
        assert d["interest_accrual_method"] == "simple"
        assert d["default_supply_apy"] == "0.04"

    def test_from_dict(self) -> None:
        """Test deserialization from dictionary."""
        data = {
            "strategy_type": "lending",
            "interest_accrual_method": "compound",
            "default_supply_apy": "0.05",
            "liquidation_penalty": "0.08",
        }

        config = LendingBacktestConfig.from_dict(data)

        assert config.strategy_type == "lending"
        assert config.interest_accrual_method == "compound"
        assert config.default_supply_apy == Decimal("0.05")
        assert config.liquidation_penalty == Decimal("0.08")

    def test_roundtrip_serialization(self) -> None:
        """Test config survives roundtrip serialization."""
        original = LendingBacktestConfig(
            strategy_type="lending",
            interest_accrual_method="simple",
            liquidation_penalty=Decimal("0.07"),
            protocol="morpho",
        )

        restored = LendingBacktestConfig.from_dict(original.to_dict())

        assert restored.strategy_type == original.strategy_type
        assert restored.interest_accrual_method == original.interest_accrual_method
        assert restored.liquidation_penalty == original.liquidation_penalty
        assert restored.protocol == original.protocol


# =============================================================================
# Interest Accrual Accuracy Tests
# =============================================================================


class TestInterestAccrualAccuracy:
    """Tests for interest accrual accuracy over time."""

    def test_compound_interest_supply_24_hours(self) -> None:
        """Test compound interest accrual for supply position over 24 hours.

        Scenario:
        - Supply position: 10 WETH at $2,000 = $20,000
        - 3% APY (compound)
        - 24 hours of updates (hourly)
        - Expected: ~$1.64 interest (20000 * 0.03 / 365 * 1 day)
        """
        config = LendingBacktestConfig(
            strategy_type="lending",
            interest_accrual_method="compound",
            default_supply_apy=Decimal("0.03"),
        )
        adapter = LendingBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_supply_position(
            token="WETH",
            amount=Decimal("10"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            apy=Decimal("0.03"),
        )

        market = MockMarketState(prices={"WETH": Decimal("2000")})

        # Simulate 24 hourly updates
        for _ in range(24):
            adapter.update_position(position, market, elapsed_seconds=3600)

        # Expected interest: $20,000 * 0.03 / 365 = ~$1.64 per day
        # With compound interest (daily compounding): slightly higher
        expected_daily_interest = Decimal("20000") * Decimal("0.03") / Decimal("365")

        # Allow 5% tolerance due to compound vs simple differences
        assert position.interest_accrued == pytest.approx(
            expected_daily_interest, rel=Decimal("0.05")
        )

    def test_compound_interest_borrow_24_hours(self) -> None:
        """Test compound interest accrual for borrow position over 24 hours.

        Scenario:
        - Borrow position: $10,000 USDC
        - 5% APY (compound)
        - 24 hours of updates
        - Expected: ~$1.37 interest (10000 * 0.05 / 365 * 1 day)
        """
        config = LendingBacktestConfig(
            strategy_type="lending",
            interest_accrual_method="compound",
            default_borrow_apy=Decimal("0.05"),
        )
        adapter = LendingBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_borrow_position(
            token="USDC",
            amount=Decimal("10000"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            apy=Decimal("0.05"),
        )

        market = MockMarketState(prices={"USDC": Decimal("1")})

        # Simulate 24 hourly updates
        for _ in range(24):
            adapter.update_position(position, market, elapsed_seconds=3600)

        # Expected interest: $10,000 * 0.05 / 365 = ~$1.37 per day
        expected_daily_interest = Decimal("10000") * Decimal("0.05") / Decimal("365")

        assert position.interest_accrued == pytest.approx(
            expected_daily_interest, rel=Decimal("0.05")
        )

    def test_simple_interest_supply_24_hours(self) -> None:
        """Test simple interest accrual for supply position over 24 hours."""
        config = LendingBacktestConfig(
            strategy_type="lending",
            interest_accrual_method="simple",
            default_supply_apy=Decimal("0.04"),
        )
        adapter = LendingBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_supply_position(
            token="WETH",
            amount=Decimal("5"),
            entry_price=Decimal("3000"),  # 5 ETH at $3000 = $15,000
            entry_time=entry_time,
            apy=Decimal("0.04"),
        )

        market = MockMarketState(prices={"WETH": Decimal("3000")})

        # Simulate 24 hourly updates
        for _ in range(24):
            adapter.update_position(position, market, elapsed_seconds=3600)

        # Expected: $15,000 * 0.04 / 365 = ~$1.64 per day
        expected_daily_interest = Decimal("15000") * Decimal("0.04") / Decimal("365")

        assert position.interest_accrued == pytest.approx(
            expected_daily_interest, rel=Decimal("0.05")
        )

    def test_interest_accrual_over_7_days(self) -> None:
        """Test interest accumulation over a 7-day period."""
        config = LendingBacktestConfig(
            strategy_type="lending",
            interest_accrual_method="compound",
            default_supply_apy=Decimal("0.05"),
        )
        adapter = LendingBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_supply_position(
            token="WETH",
            amount=Decimal("20"),
            entry_price=Decimal("2500"),  # $50,000
            entry_time=entry_time,
            apy=Decimal("0.05"),
        )

        market = MockMarketState(prices={"WETH": Decimal("2500")})

        # Simulate 7 days with 4-hour intervals
        for _ in range(7 * 6):  # 42 updates
            adapter.update_position(position, market, elapsed_seconds=14400)  # 4 hours

        # Expected: $50,000 * 0.05 / 365 * 7 = ~$47.95 over 7 days
        expected_weekly_interest = Decimal("50000") * Decimal("0.05") / Decimal("365") * Decimal("7")

        assert position.interest_accrued == pytest.approx(
            expected_weekly_interest, rel=Decimal("0.05")
        )

    def test_interest_accrual_over_30_days(self) -> None:
        """Test interest accumulation over a 30-day period."""
        config = LendingBacktestConfig(
            strategy_type="lending",
            interest_accrual_method="compound",
            default_borrow_apy=Decimal("0.08"),
        )
        adapter = LendingBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_borrow_position(
            token="USDC",
            amount=Decimal("100000"),  # $100,000 borrow
            entry_price=Decimal("1"),
            entry_time=entry_time,
            apy=Decimal("0.08"),
        )

        market = MockMarketState(prices={"USDC": Decimal("1")})

        # Simulate 30 days with daily updates
        for _ in range(30):
            adapter.update_position(position, market, elapsed_seconds=86400)

        # Expected: $100,000 * 0.08 / 365 * 30 = ~$657.53 over 30 days
        expected_monthly_interest = Decimal("100000") * Decimal("0.08") / Decimal("365") * Decimal("30")

        assert position.interest_accrued == pytest.approx(
            expected_monthly_interest, rel=Decimal("0.05")
        )

    def test_no_interest_for_non_lending_position(self) -> None:
        """Test that non-lending positions are not affected by interest."""
        adapter = LendingBacktestAdapter()

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = SimulatedPosition(
            position_type=PositionType.SPOT,
            protocol="spot",
            tokens=["ETH"],
            amounts={"ETH": Decimal("10")},
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        market = MockMarketState(prices={"ETH": Decimal("2000")})

        adapter.update_position(position, market, elapsed_seconds=86400)

        # No interest should be applied
        assert position.interest_accrued == Decimal("0")

    def test_no_interest_for_zero_elapsed(self) -> None:
        """Test that zero elapsed time results in no interest."""
        adapter = LendingBacktestAdapter()

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_supply_position(entry_time=entry_time)

        market = MockMarketState(prices={"WETH": Decimal("2000")})

        adapter.update_position(position, market, elapsed_seconds=0)

        assert position.interest_accrued == Decimal("0")


# =============================================================================
# Health Factor and Liquidation Tests
# =============================================================================


class TestHealthFactorTracking:
    """Tests for health factor tracking on borrow positions."""

    def test_health_factor_updates_on_position_update(self) -> None:
        """Test that health factor is updated when position is updated."""
        config = LendingBacktestConfig(
            strategy_type="lending",
            health_factor_tracking_enabled=True,
        )
        adapter = LendingBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        borrow_position = create_borrow_position(
            token="USDC",
            amount=Decimal("10000"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            health_factor=Decimal("2.0"),
        )

        # Set collateral for health factor calculation
        adapter.set_position_collateral(borrow_position.position_id, Decimal("20000"))

        market = MockMarketState(prices={"USDC": Decimal("1")})

        adapter.update_position(borrow_position, market, elapsed_seconds=3600)

        # Health factor should be updated
        assert borrow_position.health_factor is not None
        # HF = (20000 * 0.825) / 10000 = 1.65
        assert borrow_position.health_factor == pytest.approx(Decimal("1.65"), rel=Decimal("0.01"))

    def test_health_factor_with_portfolio_sync(self) -> None:
        """Test health factor calculation with portfolio collateral sync."""
        config = LendingBacktestConfig(
            strategy_type="lending",
            health_factor_tracking_enabled=True,
        )
        adapter = LendingBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

        # Create supply position as collateral
        supply_position = create_supply_position(
            token="WETH",
            amount=Decimal("10"),
            entry_price=Decimal("2000"),  # $20,000
            entry_time=entry_time,
        )

        # Create borrow position
        borrow_position = create_borrow_position(
            token="USDC",
            amount=Decimal("10000"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
        )

        # Create portfolio with both positions
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("20000"))
        portfolio.positions = [supply_position, borrow_position]

        market = MockMarketState(prices={"WETH": Decimal("2000"), "USDC": Decimal("1")})

        # Update with portfolio sync
        adapter.update_position_with_portfolio(borrow_position, portfolio, market, elapsed_seconds=3600)

        # HF = (20000 * 0.825) / 10000 = 1.65
        assert borrow_position.health_factor == pytest.approx(Decimal("1.65"), rel=Decimal("0.01"))


class TestLiquidationSimulation:
    """Tests for liquidation simulation when health factor < 1.0."""

    def test_liquidation_triggered_when_hf_below_one(self) -> None:
        """Test that liquidation is triggered when health factor < 1.0."""
        config = LendingBacktestConfig(
            strategy_type="lending",
            liquidation_model_enabled=True,
            liquidation_penalty=Decimal("0.05"),
            liquidation_close_factor=Decimal("0.5"),
        )
        adapter = LendingBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        borrow_position = create_borrow_position(
            token="USDC",
            amount=Decimal("10000"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            health_factor=Decimal("0.9"),  # Below 1.0
        )

        # Set small collateral to trigger liquidation
        adapter.set_position_collateral(borrow_position.position_id, Decimal("8000"))

        market = MockMarketState(prices={"USDC": Decimal("1")})
        timestamp = entry_time + timedelta(hours=1)

        event = adapter.check_and_simulate_liquidation(borrow_position, market, timestamp)

        # Liquidation should have occurred
        assert event is not None
        assert event.position_id == borrow_position.position_id
        assert event.health_factor < Decimal("1.0")
        assert event.penalty == Decimal("0.05")

    def test_liquidation_penalty_applied(self) -> None:
        """Test that liquidation penalty is correctly applied."""
        config = LendingBacktestConfig(
            strategy_type="lending",
            liquidation_model_enabled=True,
            liquidation_penalty=Decimal("0.10"),  # 10% penalty
            liquidation_close_factor=Decimal("0.5"),
        )
        adapter = LendingBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        borrow_position = create_borrow_position(
            token="USDC",
            amount=Decimal("10000"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            health_factor=Decimal("0.8"),
        )

        # Collateral that won't fully cover liquidation with penalty
        adapter.set_position_collateral(borrow_position.position_id, Decimal("6000"))

        market = MockMarketState(prices={"USDC": Decimal("1")})
        timestamp = entry_time + timedelta(hours=1)

        event = adapter.check_and_simulate_liquidation(borrow_position, market, timestamp)

        assert event is not None
        # Debt to repay = 10000 * 0.5 = 5000
        # Collateral seized = 5000 * 1.10 = 5500 (but capped at available)
        assert event.debt_repaid == pytest.approx(Decimal("5000"), rel=Decimal("0.01"))
        assert event.penalty == Decimal("0.10")

    def test_no_liquidation_when_hf_above_one(self) -> None:
        """Test that no liquidation occurs when health factor >= 1.0."""
        config = LendingBacktestConfig(
            strategy_type="lending",
            liquidation_model_enabled=True,
        )
        adapter = LendingBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        borrow_position = create_borrow_position(
            token="USDC",
            amount=Decimal("10000"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            health_factor=Decimal("1.5"),
        )

        adapter.set_position_collateral(borrow_position.position_id, Decimal("20000"))

        market = MockMarketState(prices={"USDC": Decimal("1")})
        timestamp = entry_time + timedelta(hours=1)

        event = adapter.check_and_simulate_liquidation(borrow_position, market, timestamp)

        assert event is None
        assert borrow_position.is_liquidated is False

    def test_no_liquidation_when_disabled(self) -> None:
        """Test that no liquidation occurs when disabled in config."""
        config = LendingBacktestConfig(
            strategy_type="lending",
            liquidation_model_enabled=False,
        )
        adapter = LendingBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        borrow_position = create_borrow_position(
            token="USDC",
            amount=Decimal("10000"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            health_factor=Decimal("0.5"),  # Very low HF
        )

        adapter.set_position_collateral(borrow_position.position_id, Decimal("5000"))

        market = MockMarketState(prices={"USDC": Decimal("1")})
        timestamp = entry_time + timedelta(hours=1)

        event = adapter.check_and_simulate_liquidation(borrow_position, market, timestamp)

        assert event is None
        assert borrow_position.is_liquidated is False

    def test_already_liquidated_position_skipped(self) -> None:
        """Test that already liquidated positions are skipped."""
        adapter = LendingBacktestAdapter()

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        borrow_position = create_borrow_position(entry_time=entry_time)
        borrow_position.is_liquidated = True

        market = MockMarketState(prices={"USDC": Decimal("1")})

        event = adapter.check_and_simulate_liquidation(
            borrow_position, market, entry_time + timedelta(hours=1)
        )

        assert event is None


# =============================================================================
# Position Valuation Tests
# =============================================================================


class TestPositionValuation:
    """Tests for position valuation including accrued interest."""

    def test_supply_position_value_includes_interest(self) -> None:
        """Test that supply position value includes accrued interest."""
        adapter = LendingBacktestAdapter()

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_supply_position(
            token="WETH",
            amount=Decimal("10"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        # Manually set some accrued interest
        position.interest_accrued = Decimal("100")

        market = MockMarketState(prices={"WETH": Decimal("2000")})

        value = adapter.value_position(position, market)

        # Value = principal (20000) + interest (100) = 20100
        assert value == Decimal("20100")

    def test_borrow_position_value_includes_interest(self) -> None:
        """Test that borrow position value (debt) includes accrued interest."""
        adapter = LendingBacktestAdapter()

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_borrow_position(
            token="USDC",
            amount=Decimal("10000"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
        )

        # Manually set some accrued interest
        position.interest_accrued = Decimal("50")

        market = MockMarketState(prices={"USDC": Decimal("1")})

        value = adapter.value_position(position, market)

        # Debt value = principal (10000) + interest (50) = 10050
        assert value == Decimal("10050")

    def test_position_value_with_price_change(self) -> None:
        """Test position value reflects price changes."""
        adapter = LendingBacktestAdapter()

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_supply_position(
            token="WETH",
            amount=Decimal("10"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        # Price increased to $2500
        market = MockMarketState(prices={"WETH": Decimal("2500")})

        value = adapter.value_position(position, market)

        # Value = 10 ETH * $2500 = $25,000
        assert value == Decimal("25000")


# =============================================================================
# Should Rebalance Tests
# =============================================================================


class TestShouldRebalance:
    """Tests for should_rebalance method based on health factor."""

    def test_rebalance_when_hf_below_warning(self) -> None:
        """Test rebalance suggested when HF below warning threshold."""
        config = LendingBacktestConfig(
            strategy_type="lending",
            health_factor_warning_threshold=Decimal("1.2"),
        )
        adapter = LendingBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_borrow_position(
            entry_time=entry_time,
            health_factor=Decimal("1.1"),  # Below 1.2 warning
        )

        market = MockMarketState(prices={"USDC": Decimal("1")})

        should_rebalance = adapter.should_rebalance(position, market)
        assert should_rebalance is True

    def test_no_rebalance_when_hf_above_warning(self) -> None:
        """Test no rebalance when HF above warning threshold."""
        config = LendingBacktestConfig(
            strategy_type="lending",
            health_factor_warning_threshold=Decimal("1.2"),
        )
        adapter = LendingBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_borrow_position(
            entry_time=entry_time,
            health_factor=Decimal("1.5"),  # Above 1.2 warning
        )

        market = MockMarketState(prices={"USDC": Decimal("1")})

        should_rebalance = adapter.should_rebalance(position, market)
        assert should_rebalance is False

    def test_no_rebalance_for_supply_position(self) -> None:
        """Test no rebalance check for supply positions."""
        adapter = LendingBacktestAdapter()

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_supply_position(entry_time=entry_time)

        market = MockMarketState(prices={"WETH": Decimal("2000")})

        should_rebalance = adapter.should_rebalance(position, market)
        assert should_rebalance is False


# =============================================================================
# Integration Tests - Combined Scenarios
# =============================================================================


class TestIntegrationScenarios:
    """Integration tests combining multiple adapter features."""

    def test_interest_accrual_and_valuation_combined(self) -> None:
        """Test position value reflects both principal and accrued interest over time."""
        config = LendingBacktestConfig(
            strategy_type="lending",
            interest_accrual_method="compound",
            default_supply_apy=Decimal("0.05"),
        )
        adapter = LendingBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_supply_position(
            token="WETH",
            amount=Decimal("10"),
            entry_price=Decimal("2000"),  # $20,000
            entry_time=entry_time,
            apy=Decimal("0.05"),
        )

        market = MockMarketState(prices={"WETH": Decimal("2000")})

        # Simulate 7 days
        for _ in range(7):
            adapter.update_position(position, market, elapsed_seconds=86400)

        value = adapter.value_position(position, market)

        # Value = principal + 7 days of interest
        # Expected interest: ~$19.18 (20000 * 0.05 / 365 * 7)
        expected_interest = Decimal("20000") * Decimal("0.05") / Decimal("365") * Decimal("7")
        expected_value = Decimal("20000") + expected_interest

        assert value == pytest.approx(expected_value, rel=Decimal("0.05"))

    def test_price_drop_leading_to_liquidation(self) -> None:
        """Test scenario: collateral price drops leading to liquidation."""
        config = LendingBacktestConfig(
            strategy_type="lending",
            liquidation_model_enabled=True,
            health_factor_tracking_enabled=True,
        )
        adapter = LendingBacktestAdapter(config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

        # Create borrow position with healthy initial HF
        borrow_position = create_borrow_position(
            token="USDC",
            amount=Decimal("10000"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            health_factor=Decimal("1.65"),  # Healthy
        )

        # Initial collateral: $20,000 (10 ETH at $2000)
        # HF = (20000 * 0.825) / 10000 = 1.65
        adapter.set_position_collateral(borrow_position.position_id, Decimal("20000"))

        # Simulate price drop: collateral now $10,000 (ETH at $1000)
        # New HF = (10000 * 0.825) / 10000 = 0.825 (liquidation!)
        adapter.set_position_collateral(borrow_position.position_id, Decimal("10000"))
        borrow_position.health_factor = Decimal("0.825")

        market = MockMarketState(prices={"USDC": Decimal("1")})
        timestamp = entry_time + timedelta(hours=24)

        event = adapter.check_and_simulate_liquidation(borrow_position, market, timestamp)

        assert event is not None
        assert event.health_factor < Decimal("1.0")

    def test_adapter_serialization(self) -> None:
        """Test adapter configuration serialization."""
        config = LendingBacktestConfig(
            strategy_type="lending",
            interest_accrual_method="simple",
            liquidation_penalty=Decimal("0.08"),
            protocol="compound_v3",
        )
        adapter = LendingBacktestAdapter(config)

        data = adapter.to_dict()

        assert data["adapter_name"] == "lending"
        assert data["config"]["interest_accrual_method"] == "simple"
        assert data["config"]["liquidation_penalty"] == "0.08"
        assert data["config"]["protocol"] == "compound_v3"

    def test_batch_collateral_sync(self) -> None:
        """Test syncing collateral for multiple borrow positions."""
        adapter = LendingBacktestAdapter()

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

        # Create supply positions (collateral)
        supply1 = create_supply_position(
            token="WETH",
            amount=Decimal("5"),
            entry_price=Decimal("2000"),  # $10,000
            entry_time=entry_time,
        )
        supply2 = create_supply_position(
            token="WBTC",
            amount=Decimal("0.25"),
            entry_price=Decimal("40000"),  # $10,000
            entry_time=entry_time,
        )

        # Create borrow positions
        borrow1 = create_borrow_position(
            token="USDC",
            amount=Decimal("8000"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
        )
        borrow2 = create_borrow_position(
            token="DAI",
            amount=Decimal("5000"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
        )

        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("20000"))
        portfolio.positions = [supply1, supply2, borrow1, borrow2]

        market = MockMarketState(prices={
            "WETH": Decimal("2000"),
            "WBTC": Decimal("40000"),
            "USDC": Decimal("1"),
            "DAI": Decimal("1"),
        })

        # Sync collateral for all borrow positions
        collateral_map = adapter.sync_collateral_from_portfolio(portfolio, market)

        # Both borrows should have same total collateral ($20,000)
        assert collateral_map[borrow1.position_id] == Decimal("20000")
        assert collateral_map[borrow2.position_id] == Decimal("20000")


# =============================================================================
# Historical APY Integration Tests
# =============================================================================


class TestHistoricalAPYIntegration:
    """Tests for historical APY integration with BacktestDataConfig."""

    def test_use_historical_apy_flag_with_data_config(self) -> None:
        """Test that data_config.use_historical_apy is respected."""
        from almanak.framework.backtesting.config import BacktestDataConfig

        # With use_historical_apy=True
        data_config = BacktestDataConfig(use_historical_apy=True)
        adapter = LendingBacktestAdapter(data_config=data_config)
        assert adapter._use_historical_apy() is True

        # With use_historical_apy=False
        data_config = BacktestDataConfig(use_historical_apy=False)
        adapter = LendingBacktestAdapter(data_config=data_config)
        assert adapter._use_historical_apy() is False

    def test_use_historical_apy_flag_without_data_config(self) -> None:
        """Test that _use_historical_apy() returns False when no data_config.

        Without data_config, the new BacktestDataConfig provider system is not used.
        Instead, the legacy InterestCalculator approach handles historical APY
        when interest_rate_source="historical".
        """
        # With interest_rate_source="historical" but no data_config
        # _use_historical_apy() returns False because new provider system needs data_config
        config = LendingBacktestConfig(
            strategy_type="lending",
            interest_rate_source="historical",
        )
        adapter = LendingBacktestAdapter(config=config)
        assert adapter._use_historical_apy() is False  # New providers not used without data_config

        # With interest_rate_source="fixed"
        config = LendingBacktestConfig(
            strategy_type="lending",
            interest_rate_source="fixed",
        )
        adapter = LendingBacktestAdapter(config=config)
        assert adapter._use_historical_apy() is False

    def test_supply_apy_fallback_from_data_config(self) -> None:
        """Test that supply APY fallback is taken from data_config."""
        from almanak.framework.backtesting.config import BacktestDataConfig

        data_config = BacktestDataConfig(supply_apy_fallback=Decimal("0.06"))
        adapter = LendingBacktestAdapter(data_config=data_config)
        assert adapter._get_supply_apy_fallback() == Decimal("0.06")

    def test_supply_apy_fallback_from_config(self) -> None:
        """Test that supply APY fallback is taken from config when no data_config."""
        config = LendingBacktestConfig(
            strategy_type="lending",
            default_supply_apy=Decimal("0.04"),
        )
        adapter = LendingBacktestAdapter(config=config)
        assert adapter._get_supply_apy_fallback() == Decimal("0.04")

    def test_borrow_apy_fallback_from_data_config(self) -> None:
        """Test that borrow APY fallback is taken from data_config."""
        from almanak.framework.backtesting.config import BacktestDataConfig

        data_config = BacktestDataConfig(borrow_apy_fallback=Decimal("0.08"))
        adapter = LendingBacktestAdapter(data_config=data_config)
        assert adapter._get_borrow_apy_fallback() == Decimal("0.08")

    def test_protocol_routing_aave_v3(self) -> None:
        """Test that aave_v3 protocol routes to AaveV3APYProvider."""
        from unittest.mock import MagicMock

        from almanak.framework.backtesting.config import BacktestDataConfig

        data_config = BacktestDataConfig(use_historical_apy=True)
        mock_provider = MagicMock()

        adapter = LendingBacktestAdapter(data_config=data_config, aave_v3_provider=mock_provider)

        provider = adapter._get_provider_for_protocol("aave_v3")
        assert provider is mock_provider

    def test_protocol_routing_compound_v3(self) -> None:
        """Test that compound_v3 protocol routes to CompoundV3APYProvider."""
        from unittest.mock import MagicMock

        from almanak.framework.backtesting.config import BacktestDataConfig

        data_config = BacktestDataConfig(use_historical_apy=True)
        mock_provider = MagicMock()

        adapter = LendingBacktestAdapter(data_config=data_config, compound_v3_provider=mock_provider)

        provider = adapter._get_provider_for_protocol("compound_v3")
        assert provider is mock_provider

    def test_protocol_routing_morpho_blue(self) -> None:
        """Test that morpho_blue protocol routes to MorphoBlueAPYProvider."""
        from unittest.mock import MagicMock

        from almanak.framework.backtesting.config import BacktestDataConfig

        data_config = BacktestDataConfig(use_historical_apy=True)
        mock_provider = MagicMock()

        adapter = LendingBacktestAdapter(data_config=data_config, morpho_provider=mock_provider)

        provider = adapter._get_provider_for_protocol("morpho_blue")
        assert provider is mock_provider

    def test_protocol_routing_spark(self) -> None:
        """Test that spark protocol routes to SparkAPYProvider."""
        from unittest.mock import MagicMock

        from almanak.framework.backtesting.config import BacktestDataConfig

        data_config = BacktestDataConfig(use_historical_apy=True)
        mock_provider = MagicMock()

        adapter = LendingBacktestAdapter(data_config=data_config, spark_provider=mock_provider)

        provider = adapter._get_provider_for_protocol("spark")
        assert provider is mock_provider

    def test_protocol_routing_unknown_returns_none(self) -> None:
        """Test that unknown protocol returns None."""
        from almanak.framework.backtesting.config import BacktestDataConfig

        data_config = BacktestDataConfig(use_historical_apy=True)
        adapter = LendingBacktestAdapter(data_config=data_config)

        provider = adapter._get_provider_for_protocol("unknown_protocol")
        assert provider is None

    def test_position_apy_confidence_tracking(self) -> None:
        """Test that position.apy_confidence is set during update."""
        from almanak.framework.backtesting.config import BacktestDataConfig

        data_config = BacktestDataConfig(
            use_historical_apy=False,  # Use fallback
        )
        adapter = LendingBacktestAdapter(data_config=data_config)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_supply_position(
            token="WETH",
            amount=Decimal("10"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            apy=Decimal("0"),  # No entry APY, will use fallback
        )

        market = MockMarketState(prices={"WETH": Decimal("2000")})
        market.timestamp = entry_time + timedelta(hours=1)

        adapter.update_position(position, market, elapsed_seconds=3600, timestamp=market.timestamp)

        # Position should have confidence tracking set
        assert position.apy_confidence is not None
        assert position.apy_data_source is not None

    def test_position_apy_confidence_with_entry_apy(self) -> None:
        """Test that position.apy_confidence is MEDIUM when using entry APY."""
        adapter = LendingBacktestAdapter()

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_supply_position(
            token="WETH",
            amount=Decimal("10"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            apy=Decimal("0.05"),  # Has entry APY
        )

        market = MockMarketState(prices={"WETH": Decimal("2000")})
        market.timestamp = entry_time + timedelta(hours=1)

        adapter.update_position(position, market, elapsed_seconds=3600, timestamp=market.timestamp)

        # Should use entry APY with MEDIUM confidence
        assert position.apy_confidence == "medium"
        assert position.apy_data_source == "position_entry"

    def test_historical_apy_with_mock_provider(self) -> None:
        """Test historical APY fetching with a mocked provider."""
        from unittest.mock import AsyncMock, MagicMock, patch

        from almanak.framework.backtesting.config import BacktestDataConfig
        from almanak.framework.backtesting.pnl.types import APYResult, DataConfidence, DataSourceInfo

        data_config = BacktestDataConfig(use_historical_apy=True)

        # Create mock APY result
        mock_result = APYResult(
            supply_apy=Decimal("0.04"),
            borrow_apy=Decimal("0.07"),
            source_info=DataSourceInfo(
                source="aave_v3_subgraph",
                confidence=DataConfidence.HIGH,
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            ),
        )

        mock_provider = MagicMock()
        mock_provider.get_apy = AsyncMock(return_value=[mock_result])

        adapter = LendingBacktestAdapter(data_config=data_config, aave_v3_provider=mock_provider)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_supply_position(
            token="USDC",
            amount=Decimal("10000"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            apy=Decimal("0"),  # No entry APY
            protocol="aave_v3",
        )

        market = MockMarketState(prices={"USDC": Decimal("1")})
        market.timestamp = entry_time + timedelta(hours=1)

        adapter.update_position(position, market, elapsed_seconds=3600, timestamp=market.timestamp)

        # Should have HIGH confidence from mocked provider
        assert position.apy_confidence == "high"
        assert position.apy_data_source == "aave_v3_subgraph"

    def test_historical_apy_fallback_on_error(self) -> None:
        """Test that fallback is used when historical APY provider fails."""
        from unittest.mock import AsyncMock, MagicMock, patch

        from almanak.framework.backtesting.config import BacktestDataConfig

        data_config = BacktestDataConfig(
            use_historical_apy=True,
            supply_apy_fallback=Decimal("0.025"),
            borrow_apy_fallback=Decimal("0.045"),
        )

        # Create mock provider that raises an exception
        mock_provider = MagicMock()
        mock_provider.get_apy = AsyncMock(side_effect=Exception("Network error"))

        adapter = LendingBacktestAdapter(data_config=data_config, aave_v3_provider=mock_provider)

        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        position = create_supply_position(
            token="USDC",
            amount=Decimal("10000"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            apy=Decimal("0"),  # No entry APY
            protocol="aave_v3",
        )

        market = MockMarketState(prices={"USDC": Decimal("1")})
        market.timestamp = entry_time + timedelta(hours=1)

        adapter.update_position(position, market, elapsed_seconds=3600, timestamp=market.timestamp)

        # Should fall back to LOW confidence
        assert position.apy_confidence == "low"
        assert "fallback" in position.apy_data_source.lower()

    def test_adapter_with_all_providers(self) -> None:
        """Test adapter initialization with all APY providers."""
        from unittest.mock import MagicMock

        from almanak.framework.backtesting.config import BacktestDataConfig

        data_config = BacktestDataConfig(use_historical_apy=True)

        aave_provider = MagicMock()
        compound_provider = MagicMock()
        morpho_provider = MagicMock()
        spark_provider = MagicMock()

        adapter = LendingBacktestAdapter(
            data_config=data_config,
            aave_v3_provider=aave_provider,
            compound_v3_provider=compound_provider,
            morpho_provider=morpho_provider,
            spark_provider=spark_provider,
        )

        # All providers should be available
        assert adapter._get_provider_for_protocol("aave_v3") is aave_provider
        assert adapter._get_provider_for_protocol("compound_v3") is compound_provider
        assert adapter._get_provider_for_protocol("morpho_blue") is morpho_provider
        assert adapter._get_provider_for_protocol("spark") is spark_provider
