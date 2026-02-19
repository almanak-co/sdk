"""Tests for backtest reproducibility with simulation timestamps.

This module tests that adapter methods use simulation timestamps correctly
for deterministic backtest results. Two runs with the same config and
market state should produce identical results.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.backtesting.adapters.arbitrage_adapter import (
    ArbitrageBacktestAdapter,
    ArbitrageBacktestConfig,
)
from almanak.framework.backtesting.adapters.lending_adapter import (
    LendingBacktestAdapter,
    LendingBacktestConfig,
)
from almanak.framework.backtesting.adapters.lp_adapter import (
    LPBacktestAdapter,
    LPBacktestConfig,
)
from almanak.framework.backtesting.adapters.perp_adapter import (
    PerpBacktestAdapter,
    PerpBacktestConfig,
)
from almanak.framework.backtesting.pnl.portfolio import (
    SimulatedPosition,
)

# =============================================================================
# Mock Classes with Timestamp Support
# =============================================================================


@dataclass
class MockMarketStateWithTimestamp:
    """Mock market state with timestamp for reproducibility testing."""

    prices: dict[str, Decimal] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC))

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


# =============================================================================
# Test: LP Adapter Reproducibility
# =============================================================================


class TestLPAdapterReproducibility:
    """Test LP adapter produces deterministic results."""

    def test_update_position_uses_simulation_timestamp(self) -> None:
        """Verify update_position uses the simulation timestamp for position.last_updated."""
        adapter = LPBacktestAdapter(LPBacktestConfig(strategy_type="lp"))

        # Create LP position
        position = SimulatedPosition.lp(
            token0="ETH",
            token1="USDC",
            amount0=Decimal("10"),
            amount1=Decimal("20000"),
            liquidity=Decimal("1000000"),
            tick_lower=-887220,
            tick_upper=887220,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("2000"),
            entry_time=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            protocol="uniswap_v3",
        )

        # Create market state with specific timestamp
        sim_timestamp = datetime(2024, 1, 15, 12, 30, 45, tzinfo=UTC)
        market_state = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=sim_timestamp,
        )

        # Update position
        adapter.update_position(position, market_state, elapsed_seconds=3600.0)

        # Verify position.last_updated matches simulation timestamp
        assert position.last_updated == sim_timestamp

    def test_two_runs_produce_identical_last_updated(self) -> None:
        """Two runs with same config produce identical position timestamps."""
        config = LPBacktestConfig(strategy_type="lp")
        adapter1 = LPBacktestAdapter(config)
        adapter2 = LPBacktestAdapter(config)

        sim_timestamp = datetime(2024, 1, 15, 12, 30, 45, tzinfo=UTC)
        market_state = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=sim_timestamp,
        )

        # Create identical positions
        position1 = SimulatedPosition.lp(
            token0="ETH",
            token1="USDC",
            amount0=Decimal("10"),
            amount1=Decimal("20000"),
            liquidity=Decimal("1000000"),
            tick_lower=-887220,
            tick_upper=887220,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("2000"),
            entry_time=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            protocol="uniswap_v3",
        )
        position2 = SimulatedPosition.lp(
            token0="ETH",
            token1="USDC",
            amount0=Decimal("10"),
            amount1=Decimal("20000"),
            liquidity=Decimal("1000000"),
            tick_lower=-887220,
            tick_upper=887220,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("2000"),
            entry_time=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            protocol="uniswap_v3",
        )

        # Update positions
        adapter1.update_position(position1, market_state, elapsed_seconds=3600.0)
        adapter2.update_position(position2, market_state, elapsed_seconds=3600.0)

        # Both should have identical timestamps
        assert position1.last_updated == position2.last_updated
        assert position1.last_updated == sim_timestamp


# =============================================================================
# Test: Perp Adapter Reproducibility
# =============================================================================


class TestPerpAdapterReproducibility:
    """Test perp adapter produces deterministic results."""

    def test_update_position_uses_simulation_timestamp(self) -> None:
        """Verify update_position uses the simulation timestamp."""
        adapter = PerpBacktestAdapter(PerpBacktestConfig(strategy_type="perp"))

        position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            protocol="gmx",
        )

        sim_timestamp = datetime(2024, 1, 15, 12, 30, 45, tzinfo=UTC)
        market_state = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2100")},
            timestamp=sim_timestamp,
        )

        adapter.update_position(position, market_state, elapsed_seconds=3600.0)

        assert position.last_updated == sim_timestamp

    def test_funding_uses_simulation_timestamp(self) -> None:
        """Verify funding calculation uses simulation timestamp for tracking."""
        adapter = PerpBacktestAdapter(PerpBacktestConfig(strategy_type="perp"))

        position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            protocol="gmx",
        )

        sim_timestamp = datetime(2024, 1, 15, 12, 30, 45, tzinfo=UTC)
        market_state = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2100")},
            timestamp=sim_timestamp,
        )

        # First update initializes funding tracking
        adapter.update_position(position, market_state, elapsed_seconds=3600.0)

        # Second update with later timestamp
        sim_timestamp_2 = datetime(2024, 1, 15, 13, 30, 45, tzinfo=UTC)
        market_state_2 = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2100")},
            timestamp=sim_timestamp_2,
        )

        adapter.update_position(position, market_state_2, elapsed_seconds=3600.0)

        assert position.last_updated == sim_timestamp_2

    def test_two_runs_produce_identical_funding(self) -> None:
        """Two runs with same config produce identical funding results."""
        config = PerpBacktestConfig(strategy_type="perp", funding_application_frequency="continuous")
        adapter1 = PerpBacktestAdapter(config)
        adapter2 = PerpBacktestAdapter(config)

        sim_timestamp = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        market_state = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2100")},
            timestamp=sim_timestamp,
        )

        # Create identical positions
        position1 = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            protocol="gmx",
        )
        position2 = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            protocol="gmx",
        )

        # Update both with same elapsed time
        adapter1.update_position(position1, market_state, elapsed_seconds=3600.0)
        adapter2.update_position(position2, market_state, elapsed_seconds=3600.0)

        # Both should have identical funding
        assert position1.accumulated_funding == position2.accumulated_funding
        assert position1.last_updated == position2.last_updated


# =============================================================================
# Test: Lending Adapter Reproducibility
# =============================================================================


class TestLendingAdapterReproducibility:
    """Test lending adapter produces deterministic results."""

    def test_update_position_uses_simulation_timestamp(self) -> None:
        """Verify update_position uses the simulation timestamp."""
        adapter = LendingBacktestAdapter(LendingBacktestConfig(strategy_type="lending"))

        position = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("10000"),
            entry_price=Decimal("1"),
            entry_time=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            apy=Decimal("0.05"),
            protocol="aave_v3",
        )

        sim_timestamp = datetime(2024, 1, 15, 12, 30, 45, tzinfo=UTC)
        market_state = MockMarketStateWithTimestamp(
            prices={"USDC": Decimal("1")},
            timestamp=sim_timestamp,
        )

        adapter.update_position(position, market_state, elapsed_seconds=86400.0)  # 1 day

        assert position.last_updated == sim_timestamp

    def test_two_runs_produce_identical_interest(self) -> None:
        """Two runs with same config produce identical interest accrual."""
        config = LendingBacktestConfig(strategy_type="lending")
        adapter1 = LendingBacktestAdapter(config)
        adapter2 = LendingBacktestAdapter(config)

        sim_timestamp = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        market_state = MockMarketStateWithTimestamp(
            prices={"USDC": Decimal("1")},
            timestamp=sim_timestamp,
        )

        # Create identical positions
        position1 = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("10000"),
            entry_price=Decimal("1"),
            entry_time=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            apy=Decimal("0.05"),
            protocol="aave_v3",
        )
        position2 = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("10000"),
            entry_price=Decimal("1"),
            entry_time=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            apy=Decimal("0.05"),
            protocol="aave_v3",
        )

        # Update both with same elapsed time
        adapter1.update_position(position1, market_state, elapsed_seconds=86400.0)
        adapter2.update_position(position2, market_state, elapsed_seconds=86400.0)

        # Both should have identical interest
        assert position1.interest_accrued == position2.interest_accrued
        assert position1.last_updated == position2.last_updated


# =============================================================================
# Test: Arbitrage Adapter Reproducibility
# =============================================================================


class TestArbitrageAdapterReproducibility:
    """Test arbitrage adapter produces deterministic results."""

    def test_update_position_uses_simulation_timestamp(self) -> None:
        """Verify update_position uses the simulation timestamp."""
        adapter = ArbitrageBacktestAdapter(ArbitrageBacktestConfig(strategy_type="arbitrage"))

        # Arbitrage uses spot positions
        position = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("10"),
            entry_price=Decimal("2000"),
            entry_time=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
        )

        sim_timestamp = datetime(2024, 1, 15, 12, 30, 45, tzinfo=UTC)
        market_state = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2100")},
            timestamp=sim_timestamp,
        )

        adapter.update_position(position, market_state, elapsed_seconds=3600.0)

        assert position.last_updated == sim_timestamp

    def test_two_runs_produce_identical_results(self) -> None:
        """Two runs with same config produce identical position state."""
        config = ArbitrageBacktestConfig(strategy_type="arbitrage")
        adapter1 = ArbitrageBacktestAdapter(config)
        adapter2 = ArbitrageBacktestAdapter(config)

        sim_timestamp = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        market_state = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2100")},
            timestamp=sim_timestamp,
        )

        # Create identical positions
        position1 = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("10"),
            entry_price=Decimal("2000"),
            entry_time=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
        )
        position2 = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("10"),
            entry_price=Decimal("2000"),
            entry_time=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
        )

        adapter1.update_position(position1, market_state, elapsed_seconds=3600.0)
        adapter2.update_position(position2, market_state, elapsed_seconds=3600.0)

        assert position1.last_updated == position2.last_updated
        assert position1.last_updated == sim_timestamp


# =============================================================================
# Test: Explicit Timestamp Parameter Overrides market_state.timestamp
# =============================================================================


class TestExplicitTimestampOverride:
    """Test that explicit timestamp parameter takes precedence."""

    def test_lp_explicit_timestamp_overrides_market_state(self) -> None:
        """Explicit timestamp parameter takes precedence over market_state.timestamp."""
        adapter = LPBacktestAdapter(LPBacktestConfig(strategy_type="lp"))

        position = SimulatedPosition.lp(
            token0="ETH",
            token1="USDC",
            amount0=Decimal("10"),
            amount1=Decimal("20000"),
            liquidity=Decimal("1000000"),
            tick_lower=-887220,
            tick_upper=887220,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("2000"),
            entry_time=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            protocol="uniswap_v3",
        )

        market_timestamp = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        explicit_timestamp = datetime(2024, 1, 20, 18, 30, 0, tzinfo=UTC)

        market_state = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=market_timestamp,
        )

        # Pass explicit timestamp
        adapter.update_position(
            position,
            market_state,
            elapsed_seconds=3600.0,
            timestamp=explicit_timestamp,
        )

        # Should use explicit timestamp, not market_state.timestamp
        assert position.last_updated == explicit_timestamp
        assert position.last_updated != market_timestamp

    def test_perp_explicit_timestamp_overrides_market_state(self) -> None:
        """Explicit timestamp parameter takes precedence for perp adapter."""
        adapter = PerpBacktestAdapter(PerpBacktestConfig(strategy_type="perp"))

        position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            protocol="gmx",
        )

        market_timestamp = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        explicit_timestamp = datetime(2024, 1, 20, 18, 30, 0, tzinfo=UTC)

        market_state = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2100")},
            timestamp=market_timestamp,
        )

        adapter.update_position(
            position,
            market_state,
            elapsed_seconds=3600.0,
            timestamp=explicit_timestamp,
        )

        assert position.last_updated == explicit_timestamp
        assert position.last_updated != market_timestamp

    def test_lending_explicit_timestamp_overrides_market_state(self) -> None:
        """Explicit timestamp parameter takes precedence for lending adapter."""
        adapter = LendingBacktestAdapter(LendingBacktestConfig(strategy_type="lending"))

        position = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("10000"),
            entry_price=Decimal("1"),
            entry_time=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            apy=Decimal("0.05"),
            protocol="aave_v3",
        )

        market_timestamp = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        explicit_timestamp = datetime(2024, 1, 20, 18, 30, 0, tzinfo=UTC)

        market_state = MockMarketStateWithTimestamp(
            prices={"USDC": Decimal("1")},
            timestamp=market_timestamp,
        )

        adapter.update_position(
            position,
            market_state,
            elapsed_seconds=86400.0,
            timestamp=explicit_timestamp,
        )

        assert position.last_updated == explicit_timestamp
        assert position.last_updated != market_timestamp
