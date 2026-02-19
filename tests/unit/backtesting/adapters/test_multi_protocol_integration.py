"""Integration tests for multi-protocol backtest adapter with execution coordination.

This module tests the MultiProtocolBacktestAdapter's execution coordination feature,
ensuring proper execution order, delay handling, and aggregation across protocols.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.backtesting.adapters.multi_protocol_adapter import (
    CoordinatedExecution,
    ExecutionPriority,
    MultiProtocolBacktestAdapter,
    MultiProtocolBacktestConfig,
)
from almanak.framework.backtesting.pnl.portfolio import (
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

    def get_price(self, token: str) -> Decimal | None:
        """Get price for a token."""
        if token not in self.prices:
            raise KeyError(f"Price not found for {token}")
        return self.prices.get(token)

    def get_prices(self, tokens: list[str]) -> dict[str, Decimal]:
        """Get prices for multiple tokens."""
        return {t: self.get_price(t) for t in tokens if t in self.prices}


@dataclass
class MockSupplyIntent:
    """Mock supply intent for testing."""

    token: str = "USDC"
    amount: Decimal = Decimal("10000")


@dataclass
class MockBorrowIntent:
    """Mock borrow intent for testing."""

    token: str = "ETH"
    amount: Decimal = Decimal("2")


@dataclass
class MockRepayIntent:
    """Mock repay intent for testing."""

    token: str = "ETH"
    amount: Decimal = Decimal("1")


@dataclass
class MockWithdrawIntent:
    """Mock withdraw intent for testing."""

    token: str = "USDC"
    amount: Decimal = Decimal("5000")


@dataclass
class MockSwapIntent:
    """Mock swap intent for testing."""

    from_token: str = "USDC"
    to_token: str = "ETH"
    amount: Decimal = Decimal("1000")


@dataclass
class MockLPOpenIntent:
    """Mock LP open intent for testing."""

    token0: str = "ETH"
    token1: str = "USDC"
    amount0: Decimal = Decimal("1")
    amount1: Decimal = Decimal("2000")


@dataclass
class MockLPCloseIntent:
    """Mock LP close intent for testing."""

    position_id: str = "lp-1"


@dataclass
class MockPerpOpenIntent:
    """Mock perp open intent for testing."""

    token: str = "ETH"
    collateral_usd: Decimal = Decimal("5000")
    leverage: Decimal = Decimal("5")
    is_long: bool = True


@dataclass
class MockPerpCloseIntent:
    """Mock perp close intent for testing."""

    position_id: str = "perp-1"


@dataclass
class MockUnstakeIntent:
    """Mock unstake intent for testing."""

    token: str = "stETH"
    amount: Decimal = Decimal("1")


# =============================================================================
# Helper Functions
# =============================================================================


def create_portfolio_with_positions(*positions: SimulatedPosition) -> SimulatedPortfolio:
    """Create a mock portfolio with positions."""
    portfolio = SimulatedPortfolio(
        initial_capital_usd=Decimal("100000"),
        cash_usd=Decimal("100000"),
        tokens={"USDC": Decimal("100000")},
    )
    for pos in positions:
        portfolio.positions.append(pos)
    return portfolio


def create_lp_position() -> SimulatedPosition:
    """Create a mock LP position for testing."""
    return SimulatedPosition.lp(
        token0="ETH",
        token1="USDC",
        amount0=Decimal("5"),
        amount1=Decimal("10000"),
        liquidity=Decimal("1000000"),
        entry_price=Decimal("2000"),
        entry_time=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
        fee_tier=Decimal("0.003"),
        tick_lower=-887220,
        tick_upper=887220,
        protocol="uniswap_v3",
    )


def create_perp_position() -> SimulatedPosition:
    """Create a mock perp position for testing."""
    return SimulatedPosition.perp_long(
        token="ETH",
        collateral_usd=Decimal("10000"),
        leverage=Decimal("5"),
        entry_price=Decimal("2000"),
        entry_time=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
        protocol="gmx",
    )


def create_lending_positions() -> tuple[SimulatedPosition, SimulatedPosition]:
    """Create mock supply and borrow positions."""
    supply = SimulatedPosition.supply(
        token="USDC",
        amount=Decimal("10000"),
        apy=Decimal("0.05"),
        entry_price=Decimal("1"),
        entry_time=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
        protocol="aave_v3",
    )
    borrow = SimulatedPosition.borrow(
        token="ETH",
        amount=Decimal("2"),
        apy=Decimal("0.08"),
        entry_price=Decimal("2000"),
        entry_time=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
        health_factor=Decimal("1.8"),
        protocol="aave_v3",
    )
    return supply, borrow


# =============================================================================
# Execution Priority Tests
# =============================================================================


class TestExecutionPriority:
    """Tests for intent priority assignment."""

    def test_supply_intent_gets_highest_priority(self):
        """Test that supply intents get COLLATERAL_FIRST priority."""
        adapter = MultiProtocolBacktestAdapter()
        intent = MockSupplyIntent()

        priority = adapter._get_intent_priority(intent)

        assert priority == ExecutionPriority.COLLATERAL_FIRST

    def test_close_intents_get_close_priority(self):
        """Test that close intents get CLOSE_POSITIONS priority."""
        adapter = MultiProtocolBacktestAdapter()

        for intent_cls in [MockRepayIntent, MockLPCloseIntent, MockPerpCloseIntent, MockUnstakeIntent]:
            intent = intent_cls()
            priority = adapter._get_intent_priority(intent)
            assert priority == ExecutionPriority.CLOSE_POSITIONS, f"Failed for {intent_cls.__name__}"

    def test_open_intents_get_open_priority(self):
        """Test that open intents get OPEN_POSITIONS priority."""
        adapter = MultiProtocolBacktestAdapter()

        for intent_cls in [MockSwapIntent, MockBorrowIntent, MockLPOpenIntent, MockPerpOpenIntent]:
            intent = intent_cls()
            priority = adapter._get_intent_priority(intent)
            assert priority == ExecutionPriority.OPEN_POSITIONS, f"Failed for {intent_cls.__name__}"

    def test_withdraw_intent_gets_lowest_priority(self):
        """Test that withdraw intents get WITHDRAWALS priority."""
        adapter = MultiProtocolBacktestAdapter()
        intent = MockWithdrawIntent()

        priority = adapter._get_intent_priority(intent)

        assert priority == ExecutionPriority.WITHDRAWALS


# =============================================================================
# Execution Delay Tests
# =============================================================================


class TestExecutionDelay:
    """Tests for execution delay calculation."""

    def test_no_delay_when_disabled(self):
        """Test that delay is 0 when coordination is disabled."""
        config = MultiProtocolBacktestConfig(
            strategy_type="multi_protocol",
            execution_coordination_enabled=False,
        )
        adapter = MultiProtocolBacktestAdapter(config)
        intent = MockSwapIntent()

        delay = adapter._calculate_execution_delay(intent, position_in_sequence=2, total_intents=5)

        assert delay == 0.0

    def test_no_delay_for_single_intent(self):
        """Test that delay is 0 for single intent."""
        adapter = MultiProtocolBacktestAdapter()
        intent = MockSwapIntent()

        delay = adapter._calculate_execution_delay(intent, position_in_sequence=0, total_intents=1)

        assert delay == 0.0

    def test_delay_distributed_evenly(self):
        """Test that delay is distributed evenly across intents."""
        config = MultiProtocolBacktestConfig(
            strategy_type="multi_protocol",
            max_execution_delay_seconds=10.0,
        )
        adapter = MultiProtocolBacktestAdapter(config)
        intent = MockSwapIntent()

        # With 5 intents, delay should be 10/(5-1) = 2.5s per step
        delays = [
            adapter._calculate_execution_delay(intent, i, 5)
            for i in range(5)
        ]

        assert delays[0] == 0.0  # First has no delay
        assert delays[1] == pytest.approx(2.5)
        assert delays[2] == pytest.approx(5.0)
        assert delays[3] == pytest.approx(7.5)
        assert delays[4] == pytest.approx(10.0)  # Last gets max delay


# =============================================================================
# Coordinated Execution Tests
# =============================================================================


class TestCoordinatedExecution:
    """Tests for coordinated intent execution."""

    def test_empty_intents_returns_success(self):
        """Test that empty intent list returns success."""
        adapter = MultiProtocolBacktestAdapter()
        portfolio = create_portfolio_with_positions()
        market_state = MockMarketState(prices={"ETH": Decimal("2000"), "USDC": Decimal("1")})

        result = adapter.execute_coordinated_intents([], portfolio, market_state)

        assert result.success is True
        assert result.partial_success is False
        assert result.total_delay_seconds == 0.0
        assert result.execution_order == []

    def test_intents_ordered_by_priority(self):
        """Test that intents are executed in priority order."""
        adapter = MultiProtocolBacktestAdapter()
        portfolio = create_portfolio_with_positions()
        market_state = MockMarketState(prices={"ETH": Decimal("2000"), "USDC": Decimal("1")})

        # Create intents in wrong order
        intents = [
            MockWithdrawIntent(),  # Should be last
            MockBorrowIntent(),  # Should be third
            MockRepayIntent(),  # Should be second
            MockSupplyIntent(),  # Should be first
        ]

        result = adapter.execute_coordinated_intents(intents, portfolio, market_state)

        # Check execution order follows priority
        assert result.execution_order[0] == "MockSupplyIntent"
        assert result.execution_order[1] == "MockRepayIntent"
        assert result.execution_order[2] == "MockBorrowIntent"
        assert result.execution_order[3] == "MockWithdrawIntent"

    def test_coordination_disabled_preserves_order(self):
        """Test that disabled coordination preserves original order."""
        config = MultiProtocolBacktestConfig(
            strategy_type="multi_protocol",
            execution_coordination_enabled=False,
        )
        adapter = MultiProtocolBacktestAdapter(config)
        portfolio = create_portfolio_with_positions()
        market_state = MockMarketState(prices={"ETH": Decimal("2000"), "USDC": Decimal("1")})

        intents = [
            MockWithdrawIntent(),
            MockSupplyIntent(),
            MockBorrowIntent(),
        ]

        result = adapter.execute_coordinated_intents(intents, portfolio, market_state)

        # Order should be preserved when coordination disabled
        assert result.execution_order[0] == "MockWithdrawIntent"
        assert result.execution_order[1] == "MockSupplyIntent"
        assert result.execution_order[2] == "MockBorrowIntent"
        assert result.coordination_strategy == "sequential"

    def test_total_delay_accumulated(self):
        """Test that total delay is accumulated correctly."""
        config = MultiProtocolBacktestConfig(
            strategy_type="multi_protocol",
            max_execution_delay_seconds=6.0,
        )
        adapter = MultiProtocolBacktestAdapter(config)
        portfolio = create_portfolio_with_positions()
        market_state = MockMarketState(prices={"ETH": Decimal("2000"), "USDC": Decimal("1")})

        intents = [MockSwapIntent(), MockSwapIntent(), MockSwapIntent()]

        result = adapter.execute_coordinated_intents(intents, portfolio, market_state)

        # Total delay = 0 + 3 + 6 = 9s (for 3 intents with 6s max)
        assert result.total_delay_seconds == pytest.approx(9.0)

    def test_coordinated_execution_result_serialization(self):
        """Test that CoordinatedExecution can be serialized."""
        adapter = MultiProtocolBacktestAdapter()
        portfolio = create_portfolio_with_positions()
        market_state = MockMarketState(prices={"ETH": Decimal("2000"), "USDC": Decimal("1")})

        intents = [MockSupplyIntent(), MockSwapIntent()]
        result = adapter.execute_coordinated_intents(intents, portfolio, market_state)

        data = result.to_dict()

        assert "total_delay_seconds" in data
        assert "coordination_strategy" in data
        assert "execution_order" in data
        assert "num_executions" in data
        assert "success" in data


# =============================================================================
# Multi-Protocol Integration Tests
# =============================================================================


class TestMultiProtocolStrategyIntegration:
    """Integration tests for multi-protocol strategy scenarios."""

    def test_lp_perp_lending_strategy_aggregation(self):
        """Test aggregation of LP + Perp + Lending positions."""
        adapter = MultiProtocolBacktestAdapter()

        # Create diverse portfolio
        lp = create_lp_position()
        perp = create_perp_position()
        supply, borrow = create_lending_positions()

        portfolio = create_portfolio_with_positions(lp, perp, supply, borrow)

        # Sync positions
        adapter.sync_positions_to_aggregator(portfolio)

        assert adapter.portfolio_aggregator.position_count == 4
        assert "uniswap_v3" in adapter.portfolio_aggregator.protocols
        assert "gmx" in adapter.portfolio_aggregator.protocols
        assert "aave_v3" in adapter.portfolio_aggregator.protocols

    def test_coordinated_execution_with_multi_protocol_intents(self):
        """Test coordinated execution with intents across multiple protocols."""
        adapter = MultiProtocolBacktestAdapter()
        portfolio = create_portfolio_with_positions()
        market_state = MockMarketState(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")}
        )

        # Multi-protocol intent sequence:
        # 1. Supply collateral (Aave)
        # 2. Borrow (Aave)
        # 3. Swap (Uniswap)
        # 4. Open perp (GMX)
        intents = [
            MockSwapIntent(),
            MockPerpOpenIntent(),
            MockBorrowIntent(),
            MockSupplyIntent(),
        ]

        result = adapter.execute_coordinated_intents(intents, portfolio, market_state)

        # Verify priority ordering: Supply -> Open positions (Borrow, Swap, Perp)
        assert result.execution_order[0] == "MockSupplyIntent"
        # Open positions are all same priority, order preserved within priority
        assert "MockBorrowIntent" in result.execution_order
        assert "MockSwapIntent" in result.execution_order
        assert "MockPerpOpenIntent" in result.execution_order

    def test_unified_risk_with_coordinated_execution(self):
        """Test unified risk calculation after coordinated execution."""
        adapter = MultiProtocolBacktestAdapter()

        # Create portfolio with positions
        perp = create_perp_position()
        supply, borrow = create_lending_positions()
        portfolio = create_portfolio_with_positions(perp, supply, borrow)
        market_state = MockMarketState(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")}
        )

        # Calculate unified risk
        risk_score = adapter.calculate_unified_risk_score(portfolio, market_state)

        # Should have tracked risk from perp leverage and lending health factor
        assert risk_score.max_leverage >= Decimal("1")
        assert risk_score.score >= Decimal("0")
        assert risk_score.score <= Decimal("1")

    def test_exposure_aggregation_across_protocols(self):
        """Test net exposure calculation across protocols."""
        adapter = MultiProtocolBacktestAdapter()

        # Create positions with ETH exposure
        perp = create_perp_position()  # Long ETH via perp
        supply, borrow = create_lending_positions()  # Short ETH via borrow
        portfolio = create_portfolio_with_positions(perp, supply, borrow)
        market_state = MockMarketState(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")}
        )

        exposures = adapter.get_net_exposure_by_asset(portfolio, market_state)

        # Should have ETH and USDC exposures
        assert "ETH" in exposures
        assert "USDC" in exposures

    def test_leverage_calculation_with_multi_protocol_positions(self):
        """Test total leverage calculation with positions across protocols."""
        adapter = MultiProtocolBacktestAdapter()

        # Create leveraged positions
        perp = create_perp_position()  # 5x leverage
        supply, borrow = create_lending_positions()  # Lending leverage
        portfolio = create_portfolio_with_positions(perp, supply, borrow)
        market_state = MockMarketState(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")}
        )

        total_leverage = adapter.get_total_leverage(portfolio, market_state)
        leverage_by_protocol = adapter.get_leverage_by_protocol(portfolio)

        # Should have non-trivial leverage
        assert total_leverage >= Decimal("1")
        assert len(leverage_by_protocol) >= 1

    def test_adapter_serialization_with_coordination_config(self):
        """Test adapter serialization includes coordination config."""
        config = MultiProtocolBacktestConfig(
            strategy_type="multi_protocol",
            execution_coordination_enabled=True,
            max_execution_delay_seconds=10.0,
        )
        adapter = MultiProtocolBacktestAdapter(config)

        data = adapter.to_dict()

        assert "execution_coordination" in data
        assert data["execution_coordination"]["enabled"] is True
        assert data["execution_coordination"]["max_delay_seconds"] == 10.0


# =============================================================================
# Intent Sequence Tests
# =============================================================================


class TestIntentSequenceExecution:
    """Tests for executing IntentSequence with coordination."""

    def test_execute_intent_sequence(self):
        """Test execute_intent_sequence method."""
        adapter = MultiProtocolBacktestAdapter()
        portfolio = create_portfolio_with_positions()
        market_state = MockMarketState(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")}
        )

        # Create a mock sequence
        @dataclass
        class MockSequence:
            intents: list = field(default_factory=list)

        sequence = MockSequence(intents=[MockSupplyIntent(), MockBorrowIntent()])

        result = adapter.execute_intent_sequence(sequence, portfolio, market_state)

        assert isinstance(result, CoordinatedExecution)
        assert len(result.execution_order) == 2


# =============================================================================
# Edge Cases and Error Handling Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_all_same_priority_preserves_order(self):
        """Test that intents with same priority preserve original order."""
        adapter = MultiProtocolBacktestAdapter()
        portfolio = create_portfolio_with_positions()
        market_state = MockMarketState(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")}
        )

        # All these are OPEN_POSITIONS priority
        intents = [
            MockSwapIntent(from_token="USDC", to_token="ETH"),
            MockBorrowIntent(token="ETH"),
            MockPerpOpenIntent(token="ETH"),
        ]

        result = adapter.execute_coordinated_intents(intents, portfolio, market_state)

        # Within same priority, order should be preserved
        assert result.execution_order == [
            "MockSwapIntent",
            "MockBorrowIntent",
            "MockPerpOpenIntent",
        ]

    def test_single_intent_execution(self):
        """Test coordinated execution with single intent."""
        adapter = MultiProtocolBacktestAdapter()
        portfolio = create_portfolio_with_positions()
        market_state = MockMarketState(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")}
        )

        result = adapter.execute_coordinated_intents(
            [MockSwapIntent()],
            portfolio,
            market_state,
        )

        assert len(result.execution_order) == 1
        assert result.total_delay_seconds == 0.0  # Single intent = no delay

    def test_max_delay_zero_produces_no_delays(self):
        """Test that max_delay_seconds=0 produces no delays."""
        config = MultiProtocolBacktestConfig(
            strategy_type="multi_protocol",
            max_execution_delay_seconds=0.0,
        )
        adapter = MultiProtocolBacktestAdapter(config)
        portfolio = create_portfolio_with_positions()
        market_state = MockMarketState(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")}
        )

        intents = [MockSupplyIntent(), MockBorrowIntent(), MockSwapIntent()]
        result = adapter.execute_coordinated_intents(intents, portfolio, market_state)

        assert result.total_delay_seconds == 0.0


# =============================================================================
# Performance and Stress Tests
# =============================================================================


class TestPerformance:
    """Performance tests for coordinated execution."""

    def test_many_intents_coordinated_execution(self):
        """Test coordinated execution with many intents."""
        adapter = MultiProtocolBacktestAdapter()
        portfolio = create_portfolio_with_positions()
        market_state = MockMarketState(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")}
        )

        # Create 20 intents
        intents = [MockSwapIntent() for _ in range(20)]

        result = adapter.execute_coordinated_intents(intents, portfolio, market_state)

        assert len(result.execution_order) == 20
        assert result.coordination_strategy == "priority_ordered"


# =============================================================================
# Run Tests
# =============================================================================


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
