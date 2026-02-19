"""Integration tests for PortfolioAggregator with multi-protocol strategies.

This module tests the PortfolioAggregator class with combined LP, perp, and lending
positions to verify:
- Correct net exposure calculation across protocols
- Unified risk score reflects combined risk
- Position aggregation and filtering works correctly
- Serialization roundtrip preserves all data

User Story: US-021d - Integration test for portfolio aggregation
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.portfolio import (
    PositionType,
    SimulatedPosition,
)
from almanak.framework.backtesting.pnl.portfolio_aggregator import (
    PortfolioAggregator,
    PortfolioSnapshot,
    UnifiedRiskScore,
)


class TestMultiProtocolAggregation:
    """Tests for aggregating positions across multiple protocols."""

    @pytest.fixture
    def multi_protocol_portfolio(self) -> PortfolioAggregator:
        """Create a portfolio with LP, perp, and lending positions."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        aggregator = PortfolioAggregator()

        # LP position: ETH/USDC Uniswap V3
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
            protocol="uniswap_v3",
        )
        aggregator.add_position(lp_position)

        # Perp long position: BTC on GMX
        perp_long = SimulatedPosition.perp_long(
            token="BTC",
            collateral_usd=Decimal("5000"),
            leverage=Decimal("5"),
            entry_price=Decimal("40000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        aggregator.add_position(perp_long)

        # Perp short position: ETH on Hyperliquid
        perp_short = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("3000"),
            leverage=Decimal("3"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="hyperliquid",
        )
        aggregator.add_position(perp_short)

        # Supply position: USDC on Aave
        supply_position = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("10000"),
            apy=Decimal("0.05"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            protocol="aave_v3",
        )
        aggregator.add_position(supply_position)

        # Borrow position: ETH on Aave
        borrow_position = SimulatedPosition.borrow(
            token="ETH",
            amount=Decimal("1"),
            apy=Decimal("0.03"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            health_factor=Decimal("1.8"),
            protocol="aave_v3",
        )
        aggregator.add_position(borrow_position)

        return aggregator

    def test_position_count(self, multi_protocol_portfolio: PortfolioAggregator):
        """Test that all positions are tracked."""
        assert multi_protocol_portfolio.position_count == 5

    def test_protocol_filtering(self, multi_protocol_portfolio: PortfolioAggregator):
        """Test filtering positions by protocol."""
        uniswap_positions = multi_protocol_portfolio.get_positions(protocol="uniswap_v3")
        assert len(uniswap_positions) == 1
        assert uniswap_positions[0].position_type == PositionType.LP

        aave_positions = multi_protocol_portfolio.get_positions(protocol="aave_v3")
        assert len(aave_positions) == 2

        gmx_positions = multi_protocol_portfolio.get_positions(protocol="gmx")
        assert len(gmx_positions) == 1

    def test_type_filtering(self, multi_protocol_portfolio: PortfolioAggregator):
        """Test filtering positions by type."""
        lp_positions = multi_protocol_portfolio.get_positions(
            position_type=PositionType.LP
        )
        assert len(lp_positions) == 1

        perp_longs = multi_protocol_portfolio.get_positions(
            position_type=PositionType.PERP_LONG
        )
        assert len(perp_longs) == 1

        perp_shorts = multi_protocol_portfolio.get_positions(
            position_type=PositionType.PERP_SHORT
        )
        assert len(perp_shorts) == 1

        borrow_positions = multi_protocol_portfolio.get_positions(
            position_type=PositionType.BORROW
        )
        assert len(borrow_positions) == 1

    def test_token_filtering(self, multi_protocol_portfolio: PortfolioAggregator):
        """Test filtering positions by token."""
        eth_positions = multi_protocol_portfolio.get_positions(token="ETH")
        # LP (ETH/USDC), perp short (ETH), borrow (ETH) = 3
        assert len(eth_positions) == 3

        btc_positions = multi_protocol_portfolio.get_positions(token="BTC")
        # perp long (BTC) = 1
        assert len(btc_positions) == 1

        usdc_positions = multi_protocol_portfolio.get_positions(token="USDC")
        # LP (ETH/USDC), supply (USDC) = 2
        assert len(usdc_positions) == 2

    def test_protocols_list(self, multi_protocol_portfolio: PortfolioAggregator):
        """Test that all protocols are listed."""
        protocols = set(multi_protocol_portfolio.protocols)
        assert protocols == {"uniswap_v3", "gmx", "hyperliquid", "aave_v3"}


class TestNetExposureCalculation:
    """Tests for net exposure calculation across protocols."""

    @pytest.fixture
    def hedged_portfolio(self) -> PortfolioAggregator:
        """Create a portfolio with partially hedged positions."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        aggregator = PortfolioAggregator()

        # Spot ETH: 5 ETH at $2000
        spot_eth = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        aggregator.add_position(spot_eth)

        # Perp short ETH: $6000 notional at 3x = 3 ETH equivalent
        perp_short = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("2000"),
            leverage=Decimal("3"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        aggregator.add_position(perp_short)

        # Supply USDC: 10000 USDC
        supply_usdc = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("10000"),
            apy=Decimal("0.04"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
        )
        aggregator.add_position(supply_usdc)

        # Borrow USDC: 5000 USDC
        borrow_usdc = SimulatedPosition.borrow(
            token="USDC",
            amount=Decimal("5000"),
            apy=Decimal("0.06"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            health_factor=Decimal("2.0"),
        )
        aggregator.add_position(borrow_usdc)

        return aggregator

    def test_eth_net_exposure_with_hedge(self, hedged_portfolio: PortfolioAggregator):
        """Test ETH exposure calculation with partial hedge.

        5 ETH spot - 3 ETH perp short = 2 ETH net long exposure
        """
        prices = {"ETH": Decimal("2000"), "USDC": Decimal("1")}
        eth_exposure = hedged_portfolio.calculate_net_exposure("ETH", prices)

        # Spot: +5 ETH
        # Perp short: -3 ETH ($6000 notional / $2000 price)
        # Expected: 5 - 3 = 2 ETH
        assert eth_exposure == Decimal("2")

    def test_usdc_net_exposure_with_borrow(self, hedged_portfolio: PortfolioAggregator):
        """Test USDC exposure calculation with supply and borrow.

        10000 USDC supply - 5000 USDC borrow = 5000 USDC net
        """
        usdc_exposure = hedged_portfolio.calculate_net_exposure("USDC")

        # Supply: +10000 USDC
        # Borrow: -5000 USDC
        # Expected: 10000 - 5000 = 5000 USDC
        assert usdc_exposure == Decimal("5000")

    def test_all_net_exposures(self, hedged_portfolio: PortfolioAggregator):
        """Test calculating all exposures at once."""
        prices = {"ETH": Decimal("2000"), "USDC": Decimal("1")}
        exposures = hedged_portfolio.calculate_all_net_exposures(prices)

        assert "ETH" in exposures
        assert "USDC" in exposures
        assert exposures["ETH"] == Decimal("2")
        assert exposures["USDC"] == Decimal("5000")

    def test_net_exposure_usd(self, hedged_portfolio: PortfolioAggregator):
        """Test USD-denominated exposure calculation."""
        prices = {"ETH": Decimal("2000"), "USDC": Decimal("1")}

        eth_exposure_usd = hedged_portfolio.calculate_net_exposure_usd("ETH", prices)
        # 2 ETH * $2000 = $4000
        assert eth_exposure_usd == Decimal("4000")

        usdc_exposure_usd = hedged_portfolio.calculate_net_exposure_usd("USDC", prices)
        # 5000 USDC * $1 = $5000
        assert usdc_exposure_usd == Decimal("5000")

    def test_fully_hedged_exposure(self):
        """Test that fully hedged position shows zero net exposure."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        aggregator = PortfolioAggregator()

        # Spot: 5 ETH
        spot_eth = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        aggregator.add_position(spot_eth)

        # Perp short: 5 ETH equivalent
        perp_short = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("2000"),
            leverage=Decimal("5"),  # $10000 notional / $2000 = 5 ETH
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        aggregator.add_position(perp_short)

        prices = {"ETH": Decimal("2000")}
        eth_exposure = aggregator.calculate_net_exposure("ETH", prices)

        # 5 ETH spot - 5 ETH short = 0
        assert eth_exposure == Decimal("0")


class TestUnifiedRiskScore:
    """Tests for unified risk score calculation."""

    @pytest.fixture
    def risky_portfolio(self) -> PortfolioAggregator:
        """Create a portfolio with elevated risk (low health factor, high leverage)."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        aggregator = PortfolioAggregator()

        # High leverage perp: 8x on BTC
        high_leverage_perp = SimulatedPosition.perp_long(
            token="BTC",
            collateral_usd=Decimal("5000"),
            leverage=Decimal("8"),
            entry_price=Decimal("40000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        aggregator.add_position(high_leverage_perp)

        # Risky borrow: low health factor
        risky_borrow = SimulatedPosition.borrow(
            token="USDC",
            amount=Decimal("8000"),
            apy=Decimal("0.08"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            health_factor=Decimal("1.1"),  # Just above liquidation
            protocol="aave_v3",
        )
        aggregator.add_position(risky_borrow)

        return aggregator

    @pytest.fixture
    def safe_portfolio(self) -> PortfolioAggregator:
        """Create a portfolio with low risk (no leverage, high health factor)."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        aggregator = PortfolioAggregator()

        # Spot only: no leverage
        spot_eth = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("10"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        aggregator.add_position(spot_eth)

        # Conservative supply
        supply_usdc = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("50000"),
            apy=Decimal("0.03"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
            protocol="aave_v3",
        )
        aggregator.add_position(supply_usdc)

        return aggregator

    def test_risky_portfolio_has_high_score(self, risky_portfolio: PortfolioAggregator):
        """Test that risky portfolio has elevated risk score."""
        prices = {"BTC": Decimal("40000"), "USDC": Decimal("1")}
        risk = risky_portfolio.calculate_unified_risk_score(prices)

        # Should have significant risk due to:
        # - High leverage (8x)
        # - Low health factor (1.1)
        assert risk.score > Decimal("0.3")
        assert risk.max_leverage == Decimal("8")
        assert risk.min_health_factor == Decimal("1.1")
        assert risk.positions_at_risk >= 1

    def test_safe_portfolio_has_low_score(self, safe_portfolio: PortfolioAggregator):
        """Test that safe portfolio has low risk score."""
        prices = {"ETH": Decimal("2000"), "USDC": Decimal("1")}
        risk = safe_portfolio.calculate_unified_risk_score(prices)

        # Should have minimal risk:
        # - No leverage
        # - No borrow positions (no health factor)
        assert risk.score < Decimal("0.2")
        assert risk.max_leverage == Decimal("0")
        assert risk.min_health_factor is None
        assert risk.positions_at_risk == 0

    def test_risk_score_components(self, risky_portfolio: PortfolioAggregator):
        """Test that risk score includes all component factors."""
        prices = {"BTC": Decimal("40000"), "USDC": Decimal("1")}
        risk = risky_portfolio.calculate_unified_risk_score(prices)

        assert "health_factor_risk" in risk.risk_factors
        assert "leverage_risk" in risk.risk_factors
        assert "liquidation_proximity_risk" in risk.risk_factors
        assert "concentration_risk" in risk.risk_factors

    def test_risk_score_serialization(self, risky_portfolio: PortfolioAggregator):
        """Test risk score roundtrip serialization."""
        prices = {"BTC": Decimal("40000"), "USDC": Decimal("1")}
        risk = risky_portfolio.calculate_unified_risk_score(prices)

        serialized = risk.to_dict()
        deserialized = UnifiedRiskScore.from_dict(serialized)

        assert deserialized.score == risk.score
        assert deserialized.min_health_factor == risk.min_health_factor
        assert deserialized.max_leverage == risk.max_leverage
        assert deserialized.positions_at_risk == risk.positions_at_risk


class TestCollateralAndLeverage:
    """Tests for collateral utilization and leverage calculations."""

    @pytest.fixture
    def leveraged_portfolio(self) -> PortfolioAggregator:
        """Create a portfolio with multiple leveraged positions."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        aggregator = PortfolioAggregator()

        # GMX long: $5000 collateral, 5x leverage = $25000 notional
        gmx_long = SimulatedPosition.perp_long(
            token="BTC",
            collateral_usd=Decimal("5000"),
            leverage=Decimal("5"),
            entry_price=Decimal("40000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        aggregator.add_position(gmx_long)

        # Hyperliquid short: $3000 collateral, 3x leverage = $9000 notional
        hl_short = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("3000"),
            leverage=Decimal("3"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="hyperliquid",
        )
        aggregator.add_position(hl_short)

        # Supply: $10000 USDC
        supply = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("10000"),
            apy=Decimal("0.04"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
        )
        aggregator.add_position(supply)

        return aggregator

    def test_total_collateral(self, leveraged_portfolio: PortfolioAggregator):
        """Test total collateral calculation."""
        total_collateral = leveraged_portfolio.get_total_collateral_usd()

        # GMX: $5000 + Hyperliquid: $3000 + Supply: $10000 = $18000
        assert total_collateral == Decimal("18000")

    def test_total_notional(self, leveraged_portfolio: PortfolioAggregator):
        """Test total notional calculation."""
        total_notional = leveraged_portfolio.get_total_notional_usd()

        # GMX: $25000 + Hyperliquid: $9000 = $34000
        assert total_notional == Decimal("34000")

    def test_collateral_utilization(self, leveraged_portfolio: PortfolioAggregator):
        """Test collateral utilization ratio."""
        utilization = leveraged_portfolio.calculate_collateral_utilization()

        # Notional ($34000) / Collateral ($18000) = 1.888...
        expected = Decimal("34000") / Decimal("18000")
        assert abs(utilization - expected) < Decimal("0.001")

    def test_leverage_by_protocol(self, leveraged_portfolio: PortfolioAggregator):
        """Test leverage calculation per protocol."""
        leverage = leveraged_portfolio.calculate_leverage_by_protocol()

        # GMX: $25000 / $5000 = 5x
        assert leverage["gmx"] == Decimal("5")

        # Hyperliquid: $9000 / $3000 = 3x
        assert leverage["hyperliquid"] == Decimal("3")


class TestPortfolioSnapshot:
    """Tests for portfolio snapshot creation and serialization."""

    def test_create_snapshot(self):
        """Test creating a portfolio snapshot with all data."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        aggregator = PortfolioAggregator()

        # Add various positions
        spot = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        aggregator.add_position(spot)

        perp = SimulatedPosition.perp_long(
            token="BTC",
            collateral_usd=Decimal("5000"),
            leverage=Decimal("3"),
            entry_price=Decimal("40000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        aggregator.add_position(perp)

        # Create snapshot
        snapshot_time = datetime(2024, 1, 15, tzinfo=UTC)
        prices = {"ETH": Decimal("2200"), "BTC": Decimal("42000")}

        snapshot = aggregator.create_snapshot(
            timestamp=snapshot_time,
            total_value_usd=Decimal("30000"),
            prices=prices,
            include_risk_score=True,
            metadata={"reason": "test"},
        )

        assert snapshot.timestamp == snapshot_time
        assert snapshot.total_value_usd == Decimal("30000")
        assert len(snapshot.positions) == 2
        assert "ETH" in snapshot.net_exposures
        assert snapshot.risk_score is not None
        assert snapshot.metadata["reason"] == "test"

    def test_snapshot_serialization(self):
        """Test snapshot roundtrip serialization."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        aggregator = PortfolioAggregator()

        spot = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        aggregator.add_position(spot)

        snapshot = aggregator.create_snapshot(
            timestamp=datetime(2024, 1, 15, tzinfo=UTC),
            total_value_usd=Decimal("12000"),
            prices={"ETH": Decimal("2400")},
        )

        # Serialize and deserialize
        serialized = snapshot.to_dict()
        deserialized = PortfolioSnapshot.from_dict(serialized)

        assert deserialized.timestamp == snapshot.timestamp
        assert deserialized.total_value_usd == snapshot.total_value_usd
        assert len(deserialized.positions) == len(snapshot.positions)
        assert deserialized.collateral_utilization == snapshot.collateral_utilization


class TestAggregatorSerialization:
    """Tests for PortfolioAggregator serialization roundtrip."""

    def test_full_roundtrip(self):
        """Test complete serialization roundtrip preserves all data."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        aggregator = PortfolioAggregator()

        # Add LP
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
        )
        aggregator.add_position(lp)

        # Add perp
        perp = SimulatedPosition.perp_long(
            token="BTC",
            collateral_usd=Decimal("5000"),
            leverage=Decimal("5"),
            entry_price=Decimal("40000"),
            entry_time=entry_time,
            protocol="gmx",
        )
        aggregator.add_position(perp)

        # Add supply
        supply = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("10000"),
            apy=Decimal("0.05"),
            entry_price=Decimal("1"),
            entry_time=entry_time,
        )
        aggregator.add_position(supply)

        # Serialize and deserialize
        serialized = aggregator.to_dict()
        restored = PortfolioAggregator.from_dict(serialized)

        # Verify
        assert restored.position_count == aggregator.position_count
        assert set(restored.protocols) == set(aggregator.protocols)
        assert set(restored.position_types) == set(aggregator.position_types)

        # Verify individual positions
        for orig_pos in aggregator.positions:
            restored_pos = restored.get_position(orig_pos.position_id)
            assert restored_pos is not None
            assert restored_pos.position_type == orig_pos.position_type
            assert restored_pos.protocol == orig_pos.protocol
            assert restored_pos.tokens == orig_pos.tokens

    def test_from_positions_factory(self):
        """Test creating aggregator from position list."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)

        positions = [
            SimulatedPosition.spot(
                token="ETH",
                amount=Decimal("5"),
                entry_price=Decimal("2000"),
                entry_time=entry_time,
            ),
            SimulatedPosition.supply(
                token="USDC",
                amount=Decimal("10000"),
                apy=Decimal("0.04"),
                entry_price=Decimal("1"),
                entry_time=entry_time,
            ),
        ]

        aggregator = PortfolioAggregator.from_positions(positions)

        assert aggregator.position_count == 2
        assert PositionType.SPOT in aggregator.position_types
        assert PositionType.SUPPLY in aggregator.position_types


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_empty_portfolio_risk_score(self):
        """Test risk score for empty portfolio."""
        aggregator = PortfolioAggregator()
        risk = aggregator.calculate_unified_risk_score()

        assert risk.score == Decimal("0")
        assert risk.min_health_factor is None
        assert risk.max_leverage == Decimal("0")
        assert risk.positions_at_risk == 0

    def test_duplicate_position_raises(self):
        """Test that adding duplicate position ID raises error."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        aggregator = PortfolioAggregator()

        position = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        position.position_id = "test-position-1"

        aggregator.add_position(position)

        with pytest.raises(ValueError, match="already exists"):
            aggregator.add_position(position)

    def test_update_position(self):
        """Test updating an existing position."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        aggregator = PortfolioAggregator()

        position = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        position.position_id = "test-position-1"
        aggregator.add_position(position)

        # Create updated version
        updated = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("10"),  # Double the amount
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        updated.position_id = "test-position-1"

        aggregator.update_position(updated)

        assert aggregator.position_count == 1
        retrieved = aggregator.get_position("test-position-1")
        assert retrieved is not None
        assert retrieved.total_amount == Decimal("10")

    def test_remove_position(self):
        """Test removing a position."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        aggregator = PortfolioAggregator()

        position = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        aggregator.add_position(position)

        removed = aggregator.remove_position(position.position_id)

        assert removed is not None
        assert aggregator.position_count == 0
        assert aggregator.get_position(position.position_id) is None

    def test_remove_nonexistent_position(self):
        """Test removing a position that doesn't exist."""
        aggregator = PortfolioAggregator()
        removed = aggregator.remove_position("nonexistent-id")
        assert removed is None

    def test_clear_portfolio(self):
        """Test clearing all positions."""
        aggregator = PortfolioAggregator()

        # Add several positions with different timestamps to get unique IDs
        for i in range(3):
            entry_time = datetime(2024, 1, i + 1, tzinfo=UTC)
            pos = SimulatedPosition.spot(
                token="ETH",
                amount=Decimal(str(i + 1)),
                entry_price=Decimal("2000"),
                entry_time=entry_time,
            )
            aggregator.add_position(pos)

        assert aggregator.position_count == 3

        aggregator.clear()

        assert aggregator.position_count == 0
        assert aggregator.protocols == []
        assert aggregator.position_types == []

    def test_exposure_with_no_prices(self):
        """Test net exposure calculation without prices."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        aggregator = PortfolioAggregator()

        spot = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        aggregator.add_position(spot)

        # Should work without prices for spot positions
        exposure = aggregator.calculate_net_exposure("ETH")
        assert exposure == Decimal("5")

    def test_collateral_utilization_no_collateral(self):
        """Test collateral utilization with no collateral."""
        entry_time = datetime(2024, 1, 1, tzinfo=UTC)
        aggregator = PortfolioAggregator()

        # Only spot positions (no collateral tracked)
        spot = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )
        aggregator.add_position(spot)

        utilization = aggregator.calculate_collateral_utilization()
        # No notional, no collateral -> 0
        assert utilization == Decimal("0")
