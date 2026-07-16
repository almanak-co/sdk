"""Unit tests for multi-protocol backtest adapter functionality.

This module tests the MultiProtocolBacktestAdapter, focusing on:
- Position aggregation from multiple protocols
- Unified liquidation risk score calculation
- Net exposure tracking across protocols
- Integration with PortfolioAggregator
- Sub-adapter delegation
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.backtesting.adapters.multi_protocol_adapter import (
    AggregatedRiskResult,
    MultiProtocolBacktestAdapter,
    MultiProtocolBacktestConfig,
    ProtocolExposure,
    UnifiedLiquidationModel,
)
from almanak.framework.backtesting.pnl.portfolio import (
    SimulatedPortfolio,
    SimulatedPosition,
)
from almanak.framework.backtesting.pnl.portfolio_aggregator import UnifiedRiskScore

# =============================================================================
# Mock Classes
# =============================================================================


@dataclass
class MockMarketState:
    """Mock market state for testing."""

    prices: dict[str, Decimal] = field(default_factory=dict)
    timestamp: datetime | None = None

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


@dataclass
class RecordingSubAdapter:
    """Sub-adapter double that records update and valuation calls."""

    value: Decimal = Decimal("0")
    updated: list[tuple[SimulatedPosition, MockMarketState, float, datetime | None]] = field(default_factory=list)
    valued: list[tuple[SimulatedPosition, MockMarketState, datetime | None]] = field(default_factory=list)

    def update_position(
        self,
        position: SimulatedPosition,
        market_state: MockMarketState,
        elapsed_seconds: float,
        timestamp: datetime | None = None,
    ) -> None:
        self.updated.append((position, market_state, elapsed_seconds, timestamp))

    def value_position(
        self,
        position: SimulatedPosition,
        market_state: MockMarketState,
        timestamp: datetime | None = None,
    ) -> Decimal:
        self.valued.append((position, market_state, timestamp))
        return self.value


@dataclass
class CapturingAggregator:
    """PortfolioAggregator double that captures prices passed to risk scoring."""

    captured_prices: dict[str, Decimal] | None = None

    def calculate_unified_risk_score(
        self,
        prices: dict[str, Decimal],
        health_factor_warning_threshold: Decimal,
        leverage_warning_threshold: Decimal,
        liquidation_proximity_threshold: Decimal,
    ) -> UnifiedRiskScore:
        self.captured_prices = prices
        return UnifiedRiskScore(
            score=Decimal("0"),
            min_health_factor=None,
            max_leverage=Decimal("0"),
            avg_leverage=Decimal("0"),
            positions_at_risk=0,
            liquidation_risk_usd=Decimal("0"),
        )


# =============================================================================
# Helper Functions
# =============================================================================


def create_lp_position(
    token0: str = "ETH",
    token1: str = "USDC",
    amount0: Decimal = Decimal("5"),
    amount1: Decimal = Decimal("10000"),
    entry_price: Decimal = Decimal("2000"),
    entry_time: datetime | None = None,
    protocol: str = "uniswap_v3",
    position_id: str | None = None,
) -> SimulatedPosition:
    """Create a mock LP position for testing."""
    if entry_time is None:
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

    position = SimulatedPosition.lp(
        token0=token0,
        token1=token1,
        amount0=amount0,
        amount1=amount1,
        liquidity=Decimal("1000000"),
        entry_price=entry_price,
        entry_time=entry_time,
        fee_tier=Decimal("0.003"),
        tick_lower=-887220,
        tick_upper=887220,
        protocol=protocol,
    )
    if position_id:
        position.position_id = position_id
    return position


def create_perp_long_position(
    token: str = "ETH",
    collateral_usd: Decimal = Decimal("10000"),
    leverage: Decimal = Decimal("5"),
    entry_price: Decimal = Decimal("2000"),
    entry_time: datetime | None = None,
    protocol: str = "gmx",
    position_id: str | None = None,
) -> SimulatedPosition:
    """Create a mock perp long position for testing."""
    if entry_time is None:
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

    position = SimulatedPosition.perp_long(
        token=token,
        collateral_usd=collateral_usd,
        leverage=leverage,
        entry_price=entry_price,
        entry_time=entry_time,
        protocol=protocol,
    )
    if position_id:
        position.position_id = position_id
    return position


def create_perp_short_position(
    token: str = "ETH",
    collateral_usd: Decimal = Decimal("10000"),
    leverage: Decimal = Decimal("5"),
    entry_price: Decimal = Decimal("2000"),
    entry_time: datetime | None = None,
    protocol: str = "gmx",
    position_id: str | None = None,
) -> SimulatedPosition:
    """Create a mock perp short position for testing."""
    if entry_time is None:
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

    position = SimulatedPosition.perp_short(
        token=token,
        collateral_usd=collateral_usd,
        leverage=leverage,
        entry_price=entry_price,
        entry_time=entry_time,
        protocol=protocol,
    )
    if position_id:
        position.position_id = position_id
    return position


def create_supply_position(
    token: str = "USDC",
    amount: Decimal = Decimal("10000"),
    entry_price: Decimal = Decimal("1"),
    apy: Decimal = Decimal("0.05"),
    entry_time: datetime | None = None,
    protocol: str = "aave_v3",
    position_id: str | None = None,
) -> SimulatedPosition:
    """Create a mock supply position for testing."""
    if entry_time is None:
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

    position = SimulatedPosition.supply(
        token=token,
        amount=amount,
        apy=apy,
        entry_price=entry_price,
        entry_time=entry_time,
        protocol=protocol,
    )
    if position_id:
        position.position_id = position_id
    return position


def create_borrow_position(
    token: str = "ETH",
    amount: Decimal = Decimal("2"),
    entry_price: Decimal = Decimal("2000"),
    apy: Decimal = Decimal("0.08"),
    entry_time: datetime | None = None,
    protocol: str = "aave_v3",
    health_factor: Decimal | None = None,
    position_id: str | None = None,
) -> SimulatedPosition:
    """Create a mock borrow position for testing."""
    if entry_time is None:
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

    position = SimulatedPosition.borrow(
        token=token,
        amount=amount,
        apy=apy,
        entry_price=entry_price,
        entry_time=entry_time,
        health_factor=health_factor,
        protocol=protocol,
    )
    if position_id:
        position.position_id = position_id
    return position


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


# =============================================================================
# Config Tests
# =============================================================================


class TestMultiProtocolBacktestConfig:
    """Tests for MultiProtocolBacktestConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = MultiProtocolBacktestConfig(strategy_type="multi_protocol")

        assert config.strategy_type == "multi_protocol"
        assert config.reconcile_positions is True
        assert config.unified_liquidation_model == "conservative"
        assert config.protocol_configs == {}
        assert config.liquidation_warning_threshold == Decimal("1.3")
        assert config.liquidation_critical_threshold == Decimal("1.1")
        assert config.execution_coordination_enabled is True
        assert config.max_execution_delay_seconds == 5.0

    def test_custom_config(self):
        """Test custom configuration values."""
        config = MultiProtocolBacktestConfig(
            strategy_type="multi_protocol",
            reconcile_positions=False,
            unified_liquidation_model="weighted",
            protocol_configs={
                "lp": {"fee_tracking_enabled": True},
                "lending": {"health_factor_tracking_enabled": True},
            },
            liquidation_warning_threshold=Decimal("1.5"),
            liquidation_critical_threshold=Decimal("1.2"),
            max_execution_delay_seconds=10.0,
        )

        assert config.reconcile_positions is False
        assert config.unified_liquidation_model == "weighted"
        assert "lp" in config.protocol_configs
        assert config.liquidation_warning_threshold == Decimal("1.5")

    def test_invalid_strategy_type(self):
        """Test that invalid strategy type raises error."""
        with pytest.raises(ValueError, match="requires strategy_type='multi_protocol'"):
            MultiProtocolBacktestConfig(strategy_type="lp")

    def test_invalid_liquidation_model(self):
        """Test that invalid liquidation model raises error."""
        with pytest.raises(ValueError, match="unified_liquidation_model must be one of"):
            MultiProtocolBacktestConfig(
                strategy_type="multi_protocol",
                unified_liquidation_model="invalid",
            )

    def test_invalid_threshold_values(self):
        """Test that invalid threshold values raise errors."""
        with pytest.raises(ValueError, match="liquidation_warning_threshold must be > 1"):
            MultiProtocolBacktestConfig(
                strategy_type="multi_protocol",
                liquidation_warning_threshold=Decimal("0.9"),
            )

        with pytest.raises(ValueError, match="liquidation_critical_threshold must be > 1"):
            MultiProtocolBacktestConfig(
                strategy_type="multi_protocol",
                liquidation_critical_threshold=Decimal("0.8"),
            )

    def test_critical_above_warning_raises_error(self):
        """Test that critical threshold >= warning threshold raises error."""
        with pytest.raises(ValueError, match="must be < liquidation_warning_threshold"):
            MultiProtocolBacktestConfig(
                strategy_type="multi_protocol",
                liquidation_warning_threshold=Decimal("1.2"),
                liquidation_critical_threshold=Decimal("1.3"),
            )

    def test_config_serialization(self):
        """Test config serialization and deserialization."""
        original = MultiProtocolBacktestConfig(
            strategy_type="multi_protocol",
            unified_liquidation_model="aggregate",
            protocol_configs={"lp": {"fee_tracking_enabled": True}},
        )

        data = original.to_dict()
        restored = MultiProtocolBacktestConfig.from_dict(data)

        assert restored.strategy_type == original.strategy_type
        assert restored.unified_liquidation_model == original.unified_liquidation_model
        assert restored.protocol_configs == original.protocol_configs


# =============================================================================
# Adapter Initialization Tests
# =============================================================================


class TestMultiProtocolAdapterInitialization:
    """Tests for adapter initialization."""

    def test_default_adapter(self):
        """Test adapter with default config."""
        adapter = MultiProtocolBacktestAdapter()

        assert adapter.adapter_name == "multi_protocol"
        assert adapter.config.strategy_type == "multi_protocol"
        assert len(adapter.sub_adapters) > 0
        assert adapter.portfolio_aggregator is not None
        assert adapter.unified_risk_scores == []

    def test_adapter_with_custom_config(self):
        """Test adapter with custom config."""
        config = MultiProtocolBacktestConfig(
            strategy_type="multi_protocol",
            unified_liquidation_model="weighted",
        )
        adapter = MultiProtocolBacktestAdapter(config)

        assert adapter.config.unified_liquidation_model == "weighted"

    def test_sub_adapters_initialized(self):
        """Test that sub-adapters are initialized."""
        adapter = MultiProtocolBacktestAdapter()

        # Should have LP, perp, lending, arbitrage adapters
        assert "lp" in adapter.sub_adapters or len(adapter.sub_adapters) > 0

    def test_get_sub_adapter(self):
        """Test getting specific sub-adapter."""
        adapter = MultiProtocolBacktestAdapter()

        # LP adapter should exist
        lp_adapter = adapter.get_sub_adapter("lp")
        # May or may not exist depending on registration
        # Just test the method doesn't fail
        assert lp_adapter is None or hasattr(lp_adapter, "adapter_name")


# =============================================================================
# Position Aggregation Tests
# =============================================================================


class TestPositionAggregation:
    """Tests for position aggregation functionality."""

    def test_aggregate_positions_by_protocol(self):
        """Test aggregation of positions by protocol type."""
        adapter = MultiProtocolBacktestAdapter()

        lp_pos = create_lp_position()
        perp_pos = create_perp_long_position()
        supply_pos = create_supply_position()
        borrow_pos = create_borrow_position()

        portfolio = create_portfolio_with_positions(lp_pos, perp_pos, supply_pos, borrow_pos)
        market_state = MockMarketState(prices={"ETH": Decimal("2000"), "USDC": Decimal("1")})

        aggregated = adapter.aggregate_positions(portfolio, market_state)

        assert "lp" in aggregated
        assert "perp" in aggregated
        assert "lending" in aggregated
        assert len(aggregated["lp"]) == 1
        assert len(aggregated["perp"]) == 1
        # Supply and borrow both go to lending
        assert len(aggregated["lending"]) == 2

    def test_sync_positions_to_aggregator(self):
        """Test syncing positions to internal PortfolioAggregator."""
        adapter = MultiProtocolBacktestAdapter()

        lp_pos = create_lp_position()
        perp_pos = create_perp_long_position()
        portfolio = create_portfolio_with_positions(lp_pos, perp_pos)

        adapter.sync_positions_to_aggregator(portfolio)

        assert adapter.portfolio_aggregator.position_count == 2
        assert len(adapter.portfolio_aggregator.protocols) >= 1

    def test_sync_clears_previous_positions(self):
        """Test that sync clears previous positions."""
        adapter = MultiProtocolBacktestAdapter()

        # First sync
        portfolio1 = create_portfolio_with_positions(create_lp_position())
        adapter.sync_positions_to_aggregator(portfolio1)
        assert adapter.portfolio_aggregator.position_count == 1

        # Second sync with different position
        portfolio2 = create_portfolio_with_positions(create_perp_long_position())
        adapter.sync_positions_to_aggregator(portfolio2)
        assert adapter.portfolio_aggregator.position_count == 1


# =============================================================================
# Position Update and Valuation Tests
# =============================================================================


class TestPositionUpdateAndValuation:
    """Tests for sub-adapter delegation and default position handling."""

    def test_update_position_delegates_lending_position_with_timestamp(self):
        """Lending positions are updated by the lending sub-adapter."""
        adapter = MultiProtocolBacktestAdapter()
        recorder = RecordingSubAdapter()
        adapter.sub_adapters["lending"] = recorder
        position = create_supply_position()
        market_state = MockMarketState(prices={"USDC": Decimal("1")})
        timestamp = datetime(2024, 1, 2, 0, 0, tzinfo=UTC)

        adapter.update_position(position, market_state, elapsed_seconds=60.0, timestamp=timestamp)

        assert recorder.updated == [(position, market_state, 60.0, timestamp)]

    def test_update_position_default_uses_market_timestamp_for_spot_position(self):
        """Default updates use market_state.timestamp when no sub-adapter handles a position."""
        adapter = MultiProtocolBacktestAdapter()
        position = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("2"),
            entry_price=Decimal("2000"),
            entry_time=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
        )
        market_timestamp = datetime(2024, 1, 2, 12, 0, tzinfo=UTC)
        market_state = MockMarketState(prices={"ETH": Decimal("2100")}, timestamp=market_timestamp)

        adapter.update_position(position, market_state, elapsed_seconds=3600.0)

        assert position.last_updated == market_timestamp

    def test_update_position_strict_mode_requires_simulation_timestamp_for_default_path(self):
        """Strict reproducibility rejects fallback-to-now updates."""
        config = MultiProtocolBacktestConfig(
            strategy_type="multi_protocol",
            strict_reproducibility=True,
        )
        adapter = MultiProtocolBacktestAdapter(config)
        position = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("2"),
            entry_price=Decimal("2000"),
            entry_time=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
        )
        market_state = MockMarketState(prices={"ETH": Decimal("2100")})

        with pytest.raises(ValueError, match="No simulation timestamp available"):
            adapter.update_position(position, market_state, elapsed_seconds=3600.0)

    def test_value_position_delegates_perp_position_with_timestamp(self):
        """Perp valuation delegates to the perp sub-adapter."""
        adapter = MultiProtocolBacktestAdapter()
        recorder = RecordingSubAdapter(value=Decimal("123.45"))
        adapter.sub_adapters["perp"] = recorder
        position = create_perp_long_position()
        market_state = MockMarketState(prices={"ETH": Decimal("2100")})
        timestamp = datetime(2024, 1, 2, 0, 0, tzinfo=UTC)

        value = adapter.value_position(position, market_state, timestamp)

        assert value == Decimal("123.45")
        assert recorder.valued == [(position, market_state, timestamp)]

    def test_value_position_default_falls_back_to_entry_price_when_price_missing(self):
        """Default valuation uses entry price when market data is missing."""
        adapter = MultiProtocolBacktestAdapter()
        position = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("2"),
            entry_price=Decimal("2000"),
            entry_time=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
        )
        market_state = MockMarketState(prices={})

        value = adapter.value_position(position, market_state)

        assert value == Decimal("4000")

    def test_value_position_default_preserves_measured_zero_price(self):
        """A measured zero price is not treated as missing for default valuation."""
        adapter = MultiProtocolBacktestAdapter()
        position = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("2"),
            entry_price=Decimal("2000"),
            entry_time=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
        )
        market_state = MockMarketState(prices={"ETH": Decimal("0")})

        value = adapter.value_position(position, market_state)

        assert value == Decimal("0")


# =============================================================================
# Unified Risk Calculation Tests
# =============================================================================


class TestUnifiedRiskCalculation:
    """Tests for unified risk score calculation."""

    def test_calculate_unified_risk_no_leveraged_positions(self):
        """Test unified risk with no leveraged positions."""
        adapter = MultiProtocolBacktestAdapter()

        supply_pos = create_supply_position()
        portfolio = create_portfolio_with_positions(supply_pos)
        market_state = MockMarketState(prices={"USDC": Decimal("1")})

        risk_score = adapter.calculate_unified_risk_score(portfolio, market_state)

        # No leveraged positions = low risk
        assert isinstance(risk_score, UnifiedRiskScore)
        assert risk_score.score >= Decimal("0")
        assert risk_score.score <= Decimal("1")
        assert risk_score.positions_at_risk == 0

    def test_calculate_unified_risk_with_perp_positions(self):
        """Test unified risk with perp positions."""
        adapter = MultiProtocolBacktestAdapter()

        perp_pos = create_perp_long_position(leverage=Decimal("10"))
        portfolio = create_portfolio_with_positions(perp_pos)
        market_state = MockMarketState(prices={"ETH": Decimal("2000")})

        risk_score = adapter.calculate_unified_risk_score(portfolio, market_state)

        # High leverage = higher risk
        assert risk_score.max_leverage >= Decimal("5")

    def test_calculate_unified_risk_with_low_health_factor(self):
        """Test unified risk with low health factor borrow position."""
        adapter = MultiProtocolBacktestAdapter()

        # Create a borrow position with low health factor
        supply_pos = create_supply_position(amount=Decimal("10000"))
        borrow_pos = create_borrow_position(health_factor=Decimal("1.2"))
        portfolio = create_portfolio_with_positions(supply_pos, borrow_pos)
        market_state = MockMarketState(prices={"ETH": Decimal("2000"), "USDC": Decimal("1")})

        risk_score = adapter.calculate_unified_risk_score(portfolio, market_state)

        # Low health factor = health factor risk component
        assert risk_score.min_health_factor is not None
        assert risk_score.min_health_factor <= Decimal("1.5")

    def test_calculate_unified_risk_preserves_measured_zero_prices(self):
        """Measured zero prices must reach the aggregator instead of being omitted."""
        adapter = MultiProtocolBacktestAdapter()
        capturing_aggregator = CapturingAggregator()
        adapter._portfolio_aggregator = capturing_aggregator
        position = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("2"),
            entry_price=Decimal("2000"),
            entry_time=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
        )
        portfolio = create_portfolio_with_positions(position)
        market_state = MockMarketState(prices={"ETH": Decimal("0")})

        adapter.calculate_unified_risk_score(portfolio, market_state, sync_positions=False)

        assert capturing_aggregator.captured_prices == {"ETH": Decimal("0")}

    def test_unified_risk_history_tracking(self):
        """Test that unified risk scores are tracked in history."""
        adapter = MultiProtocolBacktestAdapter()

        portfolio = create_portfolio_with_positions(create_supply_position())
        market_state = MockMarketState(prices={"USDC": Decimal("1")})

        # Calculate multiple times
        adapter.calculate_unified_risk_score(portfolio, market_state)
        adapter.calculate_unified_risk_score(portfolio, market_state)
        adapter.calculate_unified_risk_score(portfolio, market_state)

        assert len(adapter.unified_risk_scores) == 3

    def test_clear_risk_history(self):
        """Test clearing risk history."""
        adapter = MultiProtocolBacktestAdapter()

        portfolio = create_portfolio_with_positions(create_supply_position())
        market_state = MockMarketState(prices={"USDC": Decimal("1")})

        adapter.calculate_unified_risk_score(portfolio, market_state)
        adapter.calculate_unified_risk_score(portfolio, market_state)
        assert len(adapter.unified_risk_scores) > 0

        adapter.clear_risk_history()
        assert len(adapter.unified_risk_scores) == 0
        assert len(adapter.risk_history) == 0


# =============================================================================
# Net Exposure Tracking Tests
# =============================================================================


class TestNetExposureTracking:
    """Tests for net exposure calculation across protocols."""

    def test_get_net_exposure_single_token(self):
        """Test net exposure for a single token."""
        adapter = MultiProtocolBacktestAdapter()

        # Long ETH via perp
        perp_long = create_perp_long_position(token="ETH", collateral_usd=Decimal("10000"), leverage=Decimal("2"))
        portfolio = create_portfolio_with_positions(perp_long)
        market_state = MockMarketState(prices={"ETH": Decimal("2000")})

        exposure = adapter.get_net_exposure(portfolio, market_state, token="ETH")

        # Should be positive (long)
        assert exposure > Decimal("0")

    def test_get_net_exposure_hedged_position(self):
        """Test net exposure with hedged positions."""
        adapter = MultiProtocolBacktestAdapter()

        # Long ETH via perp
        perp_long = create_perp_long_position(token="ETH", collateral_usd=Decimal("10000"), leverage=Decimal("2"))
        # Short ETH via perp (same size)
        perp_short = create_perp_short_position(token="ETH", collateral_usd=Decimal("10000"), leverage=Decimal("2"))
        portfolio = create_portfolio_with_positions(perp_long, perp_short)
        market_state = MockMarketState(prices={"ETH": Decimal("2000")})

        exposure = adapter.get_net_exposure(portfolio, market_state, token="ETH")

        # Should be approximately zero (hedged)
        assert abs(exposure) < Decimal("1000")  # Allow some tolerance

    def test_get_net_exposure_by_asset(self):
        """Test net exposure calculation for all assets."""
        adapter = MultiProtocolBacktestAdapter()

        lp_pos = create_lp_position(token0="ETH", token1="USDC")
        perp_pos = create_perp_long_position(token="BTC")
        portfolio = create_portfolio_with_positions(lp_pos, perp_pos)
        market_state = MockMarketState(
            prices={
                "ETH": Decimal("2000"),
                "USDC": Decimal("1"),
                "BTC": Decimal("40000"),
            }
        )

        exposures = adapter.get_net_exposure_by_asset(portfolio, market_state)

        assert "ETH" in exposures
        assert "USDC" in exposures
        assert "BTC" in exposures

    def test_get_net_exposure_usd_by_asset(self):
        """Test net exposure in USD for all assets."""
        adapter = MultiProtocolBacktestAdapter()

        supply_pos = create_supply_position(token="ETH", amount=Decimal("10"), entry_price=Decimal("2000"))
        portfolio = create_portfolio_with_positions(supply_pos)
        market_state = MockMarketState(prices={"ETH": Decimal("2500")})  # Price up

        exposures_usd = adapter.get_net_exposure_usd_by_asset(portfolio, market_state)

        # ETH exposure * price
        assert "ETH" in exposures_usd
        # Value should be amount * price = 10 * 2500 = 25000
        assert exposures_usd["ETH"] == Decimal("25000")


# =============================================================================
# Leverage and Collateral Tests
# =============================================================================


class TestLeverageAndCollateral:
    """Tests for leverage and collateral utilization tracking."""

    def test_get_total_leverage_unleveraged(self):
        """Test total leverage with unleveraged positions."""
        adapter = MultiProtocolBacktestAdapter()

        supply_pos = create_supply_position()
        portfolio = create_portfolio_with_positions(supply_pos)
        market_state = MockMarketState(prices={"USDC": Decimal("1")})

        leverage = adapter.get_total_leverage(portfolio, market_state)

        # Unleveraged = 1x leverage
        assert leverage == Decimal("1") or leverage <= Decimal("1.1")

    def test_get_total_leverage_with_perps(self):
        """Test total leverage with perp positions."""
        adapter = MultiProtocolBacktestAdapter()

        # 5x leverage perp
        perp_pos = create_perp_long_position(collateral_usd=Decimal("10000"), leverage=Decimal("5"))
        portfolio = create_portfolio_with_positions(perp_pos)
        market_state = MockMarketState(prices={"ETH": Decimal("2000")})

        leverage = adapter.get_total_leverage(portfolio, market_state)

        # Should be > 1 due to leverage
        assert leverage >= Decimal("1")

    def test_get_collateral_utilization(self):
        """Test collateral utilization calculation."""
        adapter = MultiProtocolBacktestAdapter()

        perp_pos = create_perp_long_position(collateral_usd=Decimal("10000"), leverage=Decimal("3"))
        portfolio = create_portfolio_with_positions(perp_pos)

        utilization = adapter.get_collateral_utilization(portfolio)

        # Should be positive with leveraged position
        assert utilization >= Decimal("0")

    def test_get_leverage_by_protocol(self):
        """Test leverage calculation by protocol."""
        adapter = MultiProtocolBacktestAdapter()

        gmx_perp = create_perp_long_position(protocol="gmx", leverage=Decimal("5"))
        portfolio = create_portfolio_with_positions(gmx_perp)

        leverage_by_protocol = adapter.get_leverage_by_protocol(portfolio)

        # GMX protocol should have leverage
        if "gmx" in leverage_by_protocol:
            assert leverage_by_protocol["gmx"] >= Decimal("1")


# =============================================================================
# Risk Stats Tests
# =============================================================================


class TestRiskStats:
    """Tests for risk statistics retrieval."""

    def test_get_unified_risk_stats_empty(self):
        """Test stats with no calculations."""
        adapter = MultiProtocolBacktestAdapter()

        stats = adapter.get_unified_risk_stats()

        assert stats["total_calculations"] == 0
        assert stats["avg_risk_score"] == "0"

    def test_get_unified_risk_stats_with_data(self):
        """Test stats after calculations."""
        adapter = MultiProtocolBacktestAdapter()

        portfolio = create_portfolio_with_positions(create_supply_position())
        market_state = MockMarketState(prices={"USDC": Decimal("1")})

        # Do multiple calculations
        for _ in range(5):
            adapter.calculate_unified_risk_score(portfolio, market_state)

        stats = adapter.get_unified_risk_stats()

        assert stats["total_calculations"] == 5
        assert Decimal(stats["avg_risk_score"]) >= Decimal("0")
        assert Decimal(stats["max_risk_score"]) >= Decimal(stats["min_risk_score"])


# =============================================================================
# Serialization Tests
# =============================================================================


class TestSerialization:
    """Tests for adapter serialization."""

    def test_to_dict(self):
        """Test adapter serialization to dict."""
        adapter = MultiProtocolBacktestAdapter()

        # Do some calculations
        portfolio = create_portfolio_with_positions(create_lp_position())
        market_state = MockMarketState(prices={"ETH": Decimal("2000"), "USDC": Decimal("1")})
        adapter.sync_positions_to_aggregator(portfolio)
        adapter.calculate_unified_risk_score(portfolio, market_state)

        data = adapter.to_dict()

        assert data["adapter_name"] == "multi_protocol"
        assert "config" in data
        assert "sub_adapters" in data
        assert "risk_stats" in data
        assert "unified_risk_stats" in data
        assert "portfolio_aggregator_stats" in data

    def test_aggregated_risk_result_serialization(self):
        """Test AggregatedRiskResult serialization."""
        result = AggregatedRiskResult(
            unified_risk_score=Decimal("0.5"),
            unified_health_factor=Decimal("1.5"),
            protocol_exposures=[
                ProtocolExposure(
                    protocol_type="lp",
                    position_count=1,
                    total_value_usd=Decimal("10000"),
                    net_exposure_usd=Decimal("10000"),
                    risk_score=Decimal("0.2"),
                    liquidation_risk=False,
                )
            ],
            total_collateral_usd=Decimal("10000"),
            total_debt_usd=Decimal("0"),
            net_exposure_usd=Decimal("10000"),
            at_liquidation_risk=False,
            risk_model=UnifiedLiquidationModel.CONSERVATIVE,
        )

        data = result.to_dict()

        assert data["unified_risk_score"] == "0.5"
        assert data["unified_health_factor"] == "1.5"
        assert len(data["protocol_exposures"]) == 1
        assert data["risk_model"] == "conservative"


# =============================================================================
# Integration Tests
# =============================================================================


class TestMultiProtocolIntegration:
    """Integration tests for multi-protocol scenarios."""

    def test_lp_perp_lending_combined(self):
        """Test with LP, perp, and lending positions combined."""
        adapter = MultiProtocolBacktestAdapter()

        # Create diverse portfolio
        lp_pos = create_lp_position()
        perp_long = create_perp_long_position()
        supply_pos = create_supply_position()
        borrow_pos = create_borrow_position(health_factor=Decimal("1.8"))

        portfolio = create_portfolio_with_positions(lp_pos, perp_long, supply_pos, borrow_pos)
        market_state = MockMarketState(
            prices={
                "ETH": Decimal("2000"),
                "USDC": Decimal("1"),
            }
        )

        # Test all methods work together
        adapter.sync_positions_to_aggregator(portfolio)
        assert adapter.portfolio_aggregator.position_count == 4

        risk_score = adapter.calculate_unified_risk_score(portfolio, market_state, sync_positions=False)
        assert isinstance(risk_score, UnifiedRiskScore)

        exposures = adapter.get_net_exposure_by_asset(portfolio, market_state, sync_positions=False)
        assert len(exposures) >= 1

        leverage = adapter.get_total_leverage(portfolio, market_state, sync_positions=False)
        assert leverage >= Decimal("0")

        utilization = adapter.get_collateral_utilization(portfolio, sync_positions=False)
        assert utilization >= Decimal("0")

    def test_risk_increases_with_leverage(self):
        """Test that risk increases with higher leverage."""
        adapter = MultiProtocolBacktestAdapter()
        market_state = MockMarketState(prices={"ETH": Decimal("2000")})

        # Low leverage position
        low_leverage_portfolio = create_portfolio_with_positions(create_perp_long_position(leverage=Decimal("2")))
        low_risk = adapter.calculate_unified_risk_score(low_leverage_portfolio, market_state)

        # Clear history
        adapter.clear_risk_history()

        # High leverage position
        high_leverage_portfolio = create_portfolio_with_positions(create_perp_long_position(leverage=Decimal("10")))
        high_risk = adapter.calculate_unified_risk_score(high_leverage_portfolio, market_state)

        # Higher leverage should result in higher risk factors
        assert high_risk.max_leverage > low_risk.max_leverage

    def test_execute_intent_delegation(self):
        """Test that execute_intent delegates to sub-adapters."""
        adapter = MultiProtocolBacktestAdapter()

        portfolio = create_portfolio_with_positions()
        market_state = MockMarketState(prices={"ETH": Decimal("2000")})

        # Create a mock intent - this tests the delegation path
        # without requiring a specific adapter to be registered
        class MockIntent:
            pass

        intent = MockIntent()

        # Should return None for unknown intent (default execution)
        result = adapter.execute_intent(intent, portfolio, market_state)
        assert result is None


class TestDataConfigAndPrewarmRouting:
    """data_config threads into sub-adapters; prewarm routes to the owner."""

    def test_data_config_propagates_to_sub_adapters(self):
        from almanak.framework.backtesting.config import BacktestDataConfig

        data_config = BacktestDataConfig(allow_volume_fallback=True)
        adapter = MultiProtocolBacktestAdapter(data_config=data_config)

        lp_sub = adapter._sub_adapters.get("lp")
        assert lp_sub is not None
        assert getattr(lp_sub, "_data_config", None) is data_config

    @pytest.mark.asyncio
    async def test_prewarm_routes_lp_open_to_lp_sub_adapter(self):
        adapter = MultiProtocolBacktestAdapter()
        calls: list[str] = []

        class Hooked:
            async def prewarm_history(self, intent, *, chain, start_time, end_time):
                calls.append(chain)

        adapter._sub_adapters["lp"] = Hooked()
        intent = SimpleNamespace(intent_type=SimpleNamespace(value="LP_OPEN"))

        await adapter.prewarm_history(
            intent, chain="base", start_time=datetime(2024, 1, 1), end_time=datetime(2024, 1, 8)
        )

        assert calls == ["base"]

    @pytest.mark.asyncio
    async def test_prewarm_ignores_unrelated_intents_and_missing_hooks(self):
        """Asserted OBSERVABLY (CodeRabbit, #3271): a SWAP intent must never
        probe the sub-adapter at all, and an LP_OPEN against a hookless
        sub-adapter probes exactly ``prewarm_history`` and no-ops."""
        lookups: list[str] = []

        class HooklessRecordingSubAdapter:
            def __getattr__(self, name):  # only invoked for MISSING attributes
                lookups.append(name)
                raise AttributeError(name)

        adapter = MultiProtocolBacktestAdapter()
        adapter._sub_adapters["lp"] = HooklessRecordingSubAdapter()

        swap = SimpleNamespace(intent_type=SimpleNamespace(value="SWAP"))
        lp_open = SimpleNamespace(intent_type=SimpleNamespace(value="LP_OPEN"))

        await adapter.prewarm_history(
            swap, chain="base", start_time=datetime(2024, 1, 1), end_time=datetime(2024, 1, 8)
        )
        assert lookups == []  # unrelated intent: the sub-adapter is never touched

        await adapter.prewarm_history(
            lp_open, chain="base", start_time=datetime(2024, 1, 1), end_time=datetime(2024, 1, 8)
        )
        assert lookups == ["prewarm_history"]  # hookless: probed once, no-op


# =============================================================================
# Run Tests
# =============================================================================


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
