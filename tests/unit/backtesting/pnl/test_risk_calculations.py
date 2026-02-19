"""Unit tests for risk metric calculations in PortfolioAggregator.

This module tests the risk calculation methods:
- calculate_total_leverage: Total portfolio leverage ratio
- calculate_net_delta: Net directional exposure per asset
- calculate_cascade_risk: Liquidation cascade risk analysis

User Story: US-022d - Unit tests for risk calculations
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition
from almanak.framework.backtesting.pnl.portfolio_aggregator import (
    CascadeRiskResult,
    CascadeRiskWarning,
    PortfolioAggregator,
)


class TestTotalLeverageCalculation:
    """Tests for calculate_total_leverage method."""

    @pytest.fixture
    def entry_time(self) -> datetime:
        """Fixture providing a standard entry time."""
        return datetime(2024, 1, 1, tzinfo=UTC)

    def test_empty_portfolio_returns_zero(self):
        """Test that empty portfolio has zero leverage."""
        aggregator = PortfolioAggregator()
        leverage = aggregator.calculate_total_leverage()
        assert leverage == Decimal("0")

    def test_spot_only_portfolio_returns_one(self, entry_time: datetime):
        """Test that spot-only portfolio has 1x leverage (no leverage)."""
        aggregator = PortfolioAggregator()

        spot = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("10"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        aggregator.add_position(spot)

        prices = {"ETH": Decimal("2000")}
        leverage = aggregator.calculate_total_leverage(prices)

        # Spot: notional = equity, so leverage = 1
        assert leverage == Decimal("1")

    def test_single_perp_leverage(self, entry_time: datetime):
        """Test leverage calculation for single perp position."""
        aggregator = PortfolioAggregator()

        # 5x leveraged perp long
        perp = SimulatedPosition.perp_long(
            token="BTC",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("40000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        aggregator.add_position(perp)

        leverage = aggregator.calculate_total_leverage()

        # Notional = $50000, Equity = $10000, Leverage = 5
        assert leverage == Decimal("5")

    def test_multiple_perp_positions_combined_leverage(self, entry_time: datetime):
        """Test leverage calculation with multiple perp positions."""
        aggregator = PortfolioAggregator()

        # 5x leveraged BTC long: $10000 collateral, $50000 notional
        btc_long = SimulatedPosition.perp_long(
            token="BTC",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("40000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        aggregator.add_position(btc_long)

        # 3x leveraged ETH short: $5000 collateral, $15000 notional
        eth_short = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("5000"),
            leverage=Decimal("3"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="hyperliquid",
        )
        aggregator.add_position(eth_short)

        leverage = aggregator.calculate_total_leverage()

        # Total notional = $65000, Total equity = $15000
        # Leverage = 65000 / 15000 = 4.333...
        expected_leverage = Decimal("65000") / Decimal("15000")
        assert abs(leverage - expected_leverage) < Decimal("0.001")

    def test_mixed_portfolio_leverage(self, entry_time: datetime):
        """Test leverage with spot, perp, and lending positions."""
        aggregator = PortfolioAggregator()

        # Spot: $20000 ETH (1x leverage)
        spot = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("10"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        aggregator.add_position(spot)

        # Perp: $10000 collateral, 5x leverage = $50000 notional
        perp = SimulatedPosition.perp_long(
            token="BTC",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("40000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        aggregator.add_position(perp)

        prices = {"ETH": Decimal("2000"), "BTC": Decimal("40000")}
        leverage = aggregator.calculate_total_leverage(prices)

        # Total notional = $20000 (spot) + $50000 (perp) = $70000
        # Total equity = $20000 (spot) + $10000 (perp collateral) = $30000
        # Leverage = 70000 / 30000 = 2.333...
        expected = Decimal("70000") / Decimal("30000")
        assert abs(leverage - expected) < Decimal("0.001")

    def test_borrow_reduces_equity(self, entry_time: datetime):
        """Test that borrow positions reduce equity in leverage calculation."""
        aggregator = PortfolioAggregator()

        # Supply: $10000 USDC
        supply = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("10000"),
            apy=Decimal("0.05"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            protocol="aave_v3",
        )
        aggregator.add_position(supply)

        # Borrow: $3000 worth of ETH
        borrow = SimulatedPosition.borrow(
            token="ETH",
            amount=Decimal("1.5"),
            apy=Decimal("0.03"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            health_factor=Decimal("2.0"),
            protocol="aave_v3",
        )
        aggregator.add_position(borrow)

        prices = {"USDC": Decimal("1"), "ETH": Decimal("2000")}
        leverage = aggregator.calculate_total_leverage(prices)

        # Total notional = $10000 (supply) + $3000 (borrow exposure) = $13000
        # Total equity = $10000 (supply) - $3000 (debt) = $7000
        # Leverage = 13000 / 7000 = 1.857...
        expected = Decimal("13000") / Decimal("7000")
        assert abs(leverage - expected) < Decimal("0.001")

    def test_lp_position_leverage(self, entry_time: datetime):
        """Test that LP positions contribute to leverage correctly."""
        aggregator = PortfolioAggregator()

        # LP: 2 ETH + 4000 USDC at $2000/ETH = $8000 total value
        lp = SimulatedPosition.lp(
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
            protocol="uniswap_v3",
        )
        aggregator.add_position(lp)

        prices = {"ETH": Decimal("2000"), "USDC": Decimal("1")}
        leverage = aggregator.calculate_total_leverage(prices)

        # LP: notional = equity = $8000 (no leverage)
        # Leverage = 8000 / 8000 = 1
        assert leverage == Decimal("1")

    def test_max_leverage_cap(self, entry_time: datetime):
        """Test that leverage returns 999 when equity is zero or negative."""
        aggregator = PortfolioAggregator()

        # Borrow without supply (should not happen in practice but edge case)
        borrow = SimulatedPosition.borrow(
            token="ETH",
            amount=Decimal("5"),
            apy=Decimal("0.03"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            health_factor=Decimal("1.1"),
            protocol="aave_v3",
        )
        aggregator.add_position(borrow)

        prices = {"ETH": Decimal("2000")}
        leverage = aggregator.calculate_total_leverage(prices)

        # Notional = $10000, Equity = -$10000 -> returns 999 (max leverage cap)
        assert leverage == Decimal("999")


class TestNetDeltaCalculation:
    """Tests for calculate_net_delta method."""

    @pytest.fixture
    def entry_time(self) -> datetime:
        """Fixture providing a standard entry time."""
        return datetime(2024, 1, 1, tzinfo=UTC)

    def test_spot_position_full_delta(self, entry_time: datetime):
        """Test that spot positions have full positive delta."""
        aggregator = PortfolioAggregator()

        spot = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("10"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        aggregator.add_position(spot)

        delta = aggregator.calculate_net_delta("ETH")

        # Spot has +1 delta per unit
        assert delta == Decimal("10")

    def test_supply_position_positive_delta(self, entry_time: datetime):
        """Test that supply positions have positive delta."""
        aggregator = PortfolioAggregator()

        supply = SimulatedPosition.supply(
            token="ETH",
            amount=Decimal("5"),
            apy=Decimal("0.05"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="aave_v3",
        )
        aggregator.add_position(supply)

        delta = aggregator.calculate_net_delta("ETH")

        # Supply benefits from appreciation -> +1 delta
        assert delta == Decimal("5")

    def test_borrow_position_negative_delta(self, entry_time: datetime):
        """Test that borrow positions have negative delta."""
        aggregator = PortfolioAggregator()

        borrow = SimulatedPosition.borrow(
            token="ETH",
            amount=Decimal("3"),
            apy=Decimal("0.03"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            health_factor=Decimal("1.8"),
            protocol="aave_v3",
        )
        aggregator.add_position(borrow)

        delta = aggregator.calculate_net_delta("ETH")

        # Borrow debt increases with price -> -1 delta
        assert delta == Decimal("-3")

    def test_perp_long_positive_delta(self, entry_time: datetime):
        """Test that perp long positions have positive delta."""
        aggregator = PortfolioAggregator()

        # $20000 notional at $2000/ETH = 10 ETH equivalent
        perp_long = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("4000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        aggregator.add_position(perp_long)

        prices = {"ETH": Decimal("2000")}
        delta = aggregator.calculate_net_delta("ETH", prices)

        # Perp long: $20000 notional / $2000 = 10 ETH delta
        assert delta == Decimal("10")

    def test_perp_short_negative_delta(self, entry_time: datetime):
        """Test that perp short positions have negative delta."""
        aggregator = PortfolioAggregator()

        # $15000 notional at $2000/ETH = 7.5 ETH equivalent
        perp_short = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("5000"),
            leverage=Decimal("3"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="hyperliquid",
        )
        aggregator.add_position(perp_short)

        prices = {"ETH": Decimal("2000")}
        delta = aggregator.calculate_net_delta("ETH", prices)

        # Perp short: -$15000 notional / $2000 = -7.5 ETH delta
        assert delta == Decimal("-7.5")

    def test_lp_position_reduced_delta(self, entry_time: datetime):
        """Test that LP positions have reduced delta due to IL effect."""
        aggregator = PortfolioAggregator()

        lp = SimulatedPosition.lp(
            token0="ETH",
            token1="USDC",
            amount0=Decimal("10"),
            amount1=Decimal("20000"),
            liquidity=Decimal("10000000"),
            tick_lower=-887220,
            tick_upper=887220,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="uniswap_v3",
        )
        aggregator.add_position(lp)

        delta = aggregator.calculate_net_delta("ETH")

        # LP has 0.5x delta multiplier due to IL
        # 10 ETH * 0.5 = 5 ETH effective delta
        assert delta == Decimal("5")

    def test_hedged_portfolio_delta(self, entry_time: datetime):
        """Test net delta with hedged portfolio."""
        aggregator = PortfolioAggregator()

        # Spot: 10 ETH
        spot = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("10"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        aggregator.add_position(spot)

        # Perp short: 8 ETH equivalent ($16000 notional)
        perp_short = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("4000"),
            leverage=Decimal("4"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        aggregator.add_position(perp_short)

        prices = {"ETH": Decimal("2000")}
        delta = aggregator.calculate_net_delta("ETH", prices)

        # 10 ETH spot - 8 ETH short = 2 ETH net delta
        assert delta == Decimal("2")

    def test_fully_hedged_delta_zero(self, entry_time: datetime):
        """Test that fully hedged position has zero delta."""
        aggregator = PortfolioAggregator()

        # Spot: 5 ETH
        spot = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        aggregator.add_position(spot)

        # Perp short: 5 ETH equivalent ($10000 notional at $2000)
        perp_short = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("2000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        aggregator.add_position(perp_short)

        prices = {"ETH": Decimal("2000")}
        delta = aggregator.calculate_net_delta("ETH", prices)

        # 5 ETH spot - 5 ETH short = 0 delta
        assert delta == Decimal("0")

    def test_calculate_all_net_deltas(self, entry_time: datetime):
        """Test calculating delta for all assets at once."""
        aggregator = PortfolioAggregator()

        # ETH spot
        eth_spot = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("10"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        aggregator.add_position(eth_spot)

        # BTC perp long
        btc_perp = SimulatedPosition.perp_long(
            token="BTC",
            collateral_usd=Decimal("5000"),
            leverage=Decimal("4"),
            entry_price=Decimal("40000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        aggregator.add_position(btc_perp)

        prices = {"ETH": Decimal("2000"), "BTC": Decimal("40000")}
        deltas = aggregator.calculate_all_net_deltas(prices)

        assert "ETH" in deltas
        assert "BTC" in deltas
        assert deltas["ETH"] == Decimal("10")
        # $20000 notional / $40000 = 0.5 BTC
        assert deltas["BTC"] == Decimal("0.5")

    def test_net_delta_usd(self, entry_time: datetime):
        """Test USD-denominated delta calculation."""
        aggregator = PortfolioAggregator()

        spot = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("10"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        aggregator.add_position(spot)

        prices = {"ETH": Decimal("2500")}
        delta_usd = aggregator.calculate_net_delta_usd("ETH", prices)

        # 10 ETH * $2500 = $25000
        assert delta_usd == Decimal("25000")

    def test_nonexistent_asset_delta(self, entry_time: datetime):
        """Test delta for asset not in portfolio returns zero."""
        aggregator = PortfolioAggregator()

        spot = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("10"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        aggregator.add_position(spot)

        delta = aggregator.calculate_net_delta("BTC")
        assert delta == Decimal("0")


class TestCascadeRiskCalculation:
    """Tests for calculate_cascade_risk method."""

    @pytest.fixture
    def entry_time(self) -> datetime:
        """Fixture providing a standard entry time."""
        return datetime(2024, 1, 1, tzinfo=UTC)

    def test_no_cascade_risk_with_single_position(self, entry_time: datetime):
        """Test that single position has no cascade risk."""
        aggregator = PortfolioAggregator()

        perp = SimulatedPosition.perp_long(
            token="BTC",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("40000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        aggregator.add_position(perp)

        result = aggregator.calculate_cascade_risk(emit_warnings=False)

        assert result.risk_score == Decimal("0")
        assert result.positions_with_shared_collateral == 0
        assert result.cascade_chains == []
        assert result.max_cascade_depth == 0

    def test_cascade_risk_with_shared_protocol_positions(self, entry_time: datetime):
        """Test cascade risk when multiple positions share same protocol."""
        aggregator = PortfolioAggregator()

        # Multiple perp positions on same protocol = shared collateral risk
        perp1 = SimulatedPosition.perp_long(
            token="BTC",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("40000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        perp1.liquidation_price = Decimal("35000")
        aggregator.add_position(perp1)

        perp2 = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("5000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=datetime(2024, 1, 2, tzinfo=UTC),  # Different time for unique ID
            protocol="gmx",
        )
        perp2.liquidation_price = Decimal("1750")
        aggregator.add_position(perp2)

        prices = {"BTC": Decimal("40000"), "ETH": Decimal("2000")}
        result = aggregator.calculate_cascade_risk(prices, emit_warnings=False)

        # Two leveraged positions on same protocol = shared collateral
        assert result.positions_with_shared_collateral == 2
        assert result.risk_score > Decimal("0")

    def test_cascade_risk_with_at_risk_position(self, entry_time: datetime):
        """Test cascade detection when position is near liquidation."""
        aggregator = PortfolioAggregator()

        # Position very close to liquidation (< 20% distance)
        perp_at_risk = SimulatedPosition.perp_long(
            token="BTC",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("10"),
            entry_price=Decimal("40000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        perp_at_risk.liquidation_price = Decimal("38000")  # Only 5% away from current price
        aggregator.add_position(perp_at_risk)

        # Another position that could be affected
        perp_healthy = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("8000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=datetime(2024, 1, 2, tzinfo=UTC),  # Different time for unique ID
            protocol="gmx",
        )
        perp_healthy.liquidation_price = Decimal("1600")
        aggregator.add_position(perp_healthy)

        prices = {"BTC": Decimal("40000"), "ETH": Decimal("2000")}
        result = aggregator.calculate_cascade_risk(prices, emit_warnings=False)

        # Should detect positions at risk
        assert result.positions_with_shared_collateral >= 2
        assert result.risk_score > Decimal("0")
        # May have cascade chains if trigger position is detected
        assert result.total_collateral_at_risk_usd > Decimal("0")

    def test_cascade_risk_with_lending_positions(self, entry_time: datetime):
        """Test cascade risk with multiple borrow positions."""
        aggregator = PortfolioAggregator()

        # Supply position
        supply = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("20000"),
            apy=Decimal("0.05"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            protocol="aave_v3",
        )
        aggregator.add_position(supply)

        # Risky borrow - low health factor
        borrow1 = SimulatedPosition.borrow(
            token="ETH",
            amount=Decimal("3"),
            apy=Decimal("0.03"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            health_factor=Decimal("1.15"),  # Close to liquidation
            protocol="aave_v3",
        )
        borrow1.collateral_usd = Decimal("10000")  # For cascade calculation
        aggregator.add_position(borrow1)

        # Another borrow on same protocol
        borrow2 = SimulatedPosition.borrow(
            token="BTC",
            amount=Decimal("0.25"),
            apy=Decimal("0.02"),
            entry_price=Decimal("40000"),
            entry_time=entry_time,
            health_factor=Decimal("1.8"),
            protocol="aave_v3",
        )
        borrow2.collateral_usd = Decimal("8000")
        aggregator.add_position(borrow2)

        prices = {"ETH": Decimal("2000"), "BTC": Decimal("40000"), "USDC": Decimal("1")}
        result = aggregator.calculate_cascade_risk(prices, emit_warnings=False)

        # Borrow positions share collateral on same protocol
        assert result.positions_with_shared_collateral >= 2
        assert result.risk_score > Decimal("0")

    def test_cascade_risk_different_protocols_no_shared_risk(self, entry_time: datetime):
        """Test that positions on different protocols don't share cascade risk."""
        aggregator = PortfolioAggregator()

        # GMX position
        gmx_perp = SimulatedPosition.perp_long(
            token="BTC",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("40000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        aggregator.add_position(gmx_perp)

        # Hyperliquid position
        hl_perp = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("5000"),
            leverage=Decimal("3"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="hyperliquid",
        )
        aggregator.add_position(hl_perp)

        result = aggregator.calculate_cascade_risk(emit_warnings=False)

        # Different protocols = no shared collateral
        assert result.positions_with_shared_collateral == 0
        assert result.cascade_chains == []

    def test_cascade_risk_warning_severity(self, entry_time: datetime):
        """Test that cascade warnings have appropriate severity levels."""
        aggregator = PortfolioAggregator()

        # Create several positions on same protocol with risky position
        for i in range(4):
            perp = SimulatedPosition.perp_long(
                token="BTC",
                collateral_usd=Decimal("5000"),
                leverage=Decimal("8"),
                entry_price=Decimal("40000"),
                entry_time=datetime(2024, 1, i + 1, tzinfo=UTC),  # Different times for unique IDs
                protocol="gmx",
            )
            perp.liquidation_price = Decimal("38500")  # Near liquidation
            aggregator.add_position(perp)

        prices = {"BTC": Decimal("40000")}
        result = aggregator.calculate_cascade_risk(
            prices, cascade_threshold=Decimal("0"), emit_warnings=False
        )

        # With multiple high-risk positions, should generate warnings
        assert result.positions_with_shared_collateral >= 4

    def test_cascade_result_serialization(self, entry_time: datetime):
        """Test CascadeRiskResult serialization roundtrip."""
        aggregator = PortfolioAggregator()

        perp1 = SimulatedPosition.perp_long(
            token="BTC",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("40000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        aggregator.add_position(perp1)

        perp2 = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("5000"),
            leverage=Decimal("3"),
            entry_price=Decimal("2000"),
            entry_time=datetime(2024, 1, 2, tzinfo=UTC),
            protocol="gmx",
        )
        aggregator.add_position(perp2)

        result = aggregator.calculate_cascade_risk(emit_warnings=False)

        # Serialize and deserialize
        serialized = result.to_dict()
        deserialized = CascadeRiskResult.from_dict(serialized)

        assert deserialized.risk_score == result.risk_score
        assert deserialized.positions_with_shared_collateral == result.positions_with_shared_collateral
        assert deserialized.max_cascade_depth == result.max_cascade_depth
        assert deserialized.cascade_chains == result.cascade_chains

    def test_cascade_warning_serialization(self):
        """Test CascadeRiskWarning serialization roundtrip."""
        warning = CascadeRiskWarning(
            severity="high",
            message="Test warning message",
            affected_positions=["pos-1", "pos-2"],
            trigger_position_id="pos-0",
            estimated_cascade_loss_usd=Decimal("5000"),
            collateral_at_risk_usd=Decimal("20000"),
        )

        serialized = warning.to_dict()
        deserialized = CascadeRiskWarning.from_dict(serialized)

        assert deserialized.severity == warning.severity
        assert deserialized.message == warning.message
        assert deserialized.affected_positions == warning.affected_positions
        assert deserialized.trigger_position_id == warning.trigger_position_id
        assert deserialized.estimated_cascade_loss_usd == warning.estimated_cascade_loss_usd
        assert deserialized.collateral_at_risk_usd == warning.collateral_at_risk_usd

    def test_protocol_correlation_risk(self, entry_time: datetime):
        """Test that protocol correlation risk is calculated."""
        aggregator = PortfolioAggregator()

        # Add multiple positions on same protocol
        for i in range(3):
            perp = SimulatedPosition.perp_long(
                token="BTC",
                collateral_usd=Decimal("5000"),
                leverage=Decimal("3"),
                entry_price=Decimal("40000"),
                entry_time=datetime(2024, 1, i + 1, tzinfo=UTC),
                protocol="gmx",
            )
            aggregator.add_position(perp)

        result = aggregator.calculate_cascade_risk(emit_warnings=False)

        # Should have protocol correlation entry for gmx
        assert "gmx" in result.protocol_correlations
        assert result.protocol_correlations["gmx"] > Decimal("0")

    def test_empty_portfolio_no_cascade_risk(self):
        """Test that empty portfolio has zero cascade risk."""
        aggregator = PortfolioAggregator()
        result = aggregator.calculate_cascade_risk(emit_warnings=False)

        assert result.risk_score == Decimal("0")
        assert result.positions_with_shared_collateral == 0
        assert result.cascade_chains == []
        assert result.max_cascade_depth == 0
        assert result.total_collateral_at_risk_usd == Decimal("0")
        assert result.estimated_cascade_loss_usd == Decimal("0")

    def test_cascade_chain_depth(self, entry_time: datetime):
        """Test that cascade chain depth is calculated correctly."""
        aggregator = PortfolioAggregator()

        # Create risky position that could trigger cascade
        trigger = SimulatedPosition.perp_long(
            token="BTC",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("15"),
            entry_price=Decimal("40000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        trigger.liquidation_price = Decimal("39000")  # Very close to current
        aggregator.add_position(trigger)

        # Add positions that could be affected
        for i in range(3):
            perp = SimulatedPosition.perp_long(
                token="ETH",
                collateral_usd=Decimal("3000"),
                leverage=Decimal("10"),
                entry_price=Decimal("2000"),
                entry_time=datetime(2024, 1, i + 2, tzinfo=UTC),
                protocol="gmx",
            )
            perp.liquidation_price = Decimal("1850")
            aggregator.add_position(perp)

        prices = {"BTC": Decimal("40000"), "ETH": Decimal("2000")}
        result = aggregator.calculate_cascade_risk(prices, emit_warnings=False)

        # All positions on same protocol with high leverage
        assert result.positions_with_shared_collateral >= 4


class TestRiskCalculationIntegration:
    """Integration tests combining multiple risk calculations."""

    @pytest.fixture
    def entry_time(self) -> datetime:
        """Fixture providing a standard entry time."""
        return datetime(2024, 1, 1, tzinfo=UTC)

    def test_complete_risk_profile(self, entry_time: datetime):
        """Test calculating complete risk profile for complex portfolio."""
        aggregator = PortfolioAggregator()

        # LP position
        lp = SimulatedPosition.lp(
            token0="ETH",
            token1="USDC",
            amount0=Decimal("5"),
            amount1=Decimal("10000"),
            liquidity=Decimal("5000000"),
            tick_lower=-887220,
            tick_upper=887220,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="uniswap_v3",
        )
        aggregator.add_position(lp)

        # Perp positions on same protocol
        perp_long = SimulatedPosition.perp_long(
            token="BTC",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("40000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        aggregator.add_position(perp_long)

        perp_short = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("5000"),
            leverage=Decimal("3"),
            entry_price=Decimal("2000"),
            entry_time=datetime(2024, 1, 2, tzinfo=UTC),
            protocol="gmx",
        )
        aggregator.add_position(perp_short)

        # Lending positions
        supply = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("20000"),
            apy=Decimal("0.05"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            protocol="aave_v3",
        )
        aggregator.add_position(supply)

        borrow = SimulatedPosition.borrow(
            token="ETH",
            amount=Decimal("2"),
            apy=Decimal("0.03"),
            entry_price=Decimal("2000"),
            entry_time=datetime(2024, 1, 3, tzinfo=UTC),
            health_factor=Decimal("1.6"),
            protocol="aave_v3",
        )
        aggregator.add_position(borrow)

        prices = {
            "ETH": Decimal("2000"),
            "BTC": Decimal("40000"),
            "USDC": Decimal("1"),
        }

        # Test all risk calculations
        leverage = aggregator.calculate_total_leverage(prices)
        assert leverage > Decimal("1")  # Has leveraged positions

        eth_delta = aggregator.calculate_net_delta("ETH", prices)
        # LP: 5 * 0.5 = 2.5, Perp short: -7.5, Borrow: -2 = 2.5 - 7.5 - 2 = -7
        assert eth_delta < Decimal("0")  # Net short ETH

        btc_delta = aggregator.calculate_net_delta("BTC", prices)
        assert btc_delta > Decimal("0")  # Long BTC via perp

        cascade_risk = aggregator.calculate_cascade_risk(prices, emit_warnings=False)
        # GMX has 2 positions, Aave has 2 positions = shared collateral
        assert cascade_risk.positions_with_shared_collateral > 0

    def test_risk_metrics_consistency(self, entry_time: datetime):
        """Test that risk metrics are internally consistent."""
        aggregator = PortfolioAggregator()

        # Add positions
        spot = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("10"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        aggregator.add_position(spot)

        perp = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("5000"),
            leverage=Decimal("4"),
            entry_price=Decimal("2000"),
            entry_time=datetime(2024, 1, 2, tzinfo=UTC),
            protocol="gmx",
        )
        aggregator.add_position(perp)

        prices = {"ETH": Decimal("2000")}

        # Leverage and delta should be consistent
        leverage = aggregator.calculate_total_leverage(prices)
        delta = aggregator.calculate_net_delta("ETH", prices)

        # 10 ETH spot + 10 ETH perp ($20000 notional / $2000) = 20 ETH delta
        assert delta == Decimal("20")

        # Total notional: $20000 (spot) + $20000 (perp) = $40000
        # Total equity: $20000 (spot) + $5000 (perp collateral) = $25000
        # Leverage = 40000 / 25000 = 1.6
        expected_leverage = Decimal("40000") / Decimal("25000")
        assert abs(leverage - expected_leverage) < Decimal("0.001")
