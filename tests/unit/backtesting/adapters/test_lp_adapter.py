"""Tests for LP backtest adapter functionality.

This module tests the LPBacktestAdapter, focusing on:
- Out-of-range detection and handling
- Partial range exit scenarios
- Range status calculations
- Tick-to-price conversions
- Fee accrual
- Position valuation with IL
"""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

import pytest

from almanak.framework.backtesting.adapters.lp_adapter import (
    LPBacktestAdapter,
    LPBacktestConfig,
    RangeStatus,
    RangeStatusResult,
)
from almanak.framework.backtesting.pnl.portfolio import (
    PositionType,
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


def create_lp_position(
    token0: str = "ETH",
    token1: str = "USDC",
    tick_lower: int = -887272,
    tick_upper: int = 887272,
    entry_price: Decimal = Decimal("2000"),
    liquidity: Decimal = Decimal("1000000"),
    fee_tier: Decimal = Decimal("0.003"),
    amounts: dict[str, Decimal] | None = None,
) -> SimulatedPosition:
    """Create a mock LP position for testing."""
    if amounts is None:
        amounts = {token0: Decimal("1"), token1: Decimal("2000")}

    return SimulatedPosition(
        position_type=PositionType.LP,
        protocol="uniswap_v3",
        tokens=[token0, token1],
        amounts=amounts,
        entry_price=entry_price,
        entry_time=datetime.now(),
        tick_lower=tick_lower,
        tick_upper=tick_upper,
        liquidity=liquidity,
        fee_tier=fee_tier,
    )


# =============================================================================
# LPBacktestConfig Tests
# =============================================================================


class TestLPBacktestConfig:
    """Tests for LPBacktestConfig."""

    def test_default_config(self) -> None:
        """Test default configuration values."""
        config = LPBacktestConfig(strategy_type="lp")

        assert config.strategy_type == "lp"
        assert config.il_calculation_method == "standard"
        assert config.rebalance_on_out_of_range is True
        assert config.rebalance_on_partial_exit is False
        assert config.boundary_margin_pct == Decimal("5")
        assert config.volume_multiplier == Decimal("10")
        assert config.base_liquidity == Decimal("1000000")

    def test_custom_config(self) -> None:
        """Test custom configuration values."""
        config = LPBacktestConfig(
            strategy_type="lp",
            il_calculation_method="concentrated",
            rebalance_on_out_of_range=True,
            rebalance_on_partial_exit=True,
            boundary_margin_pct=Decimal("10"),
            volume_multiplier=Decimal("20"),
            base_liquidity=Decimal("5000000"),
        )

        assert config.il_calculation_method == "concentrated"
        assert config.rebalance_on_partial_exit is True
        assert config.boundary_margin_pct == Decimal("10")
        assert config.volume_multiplier == Decimal("20")
        assert config.base_liquidity == Decimal("5000000")

    def test_invalid_strategy_type(self) -> None:
        """Test validation rejects non-LP strategy type."""
        with pytest.raises(ValueError, match="requires strategy_type='lp'"):
            LPBacktestConfig(strategy_type="perp")

    def test_invalid_il_method(self) -> None:
        """Test validation rejects invalid IL calculation method."""
        with pytest.raises(ValueError, match="il_calculation_method must be one of"):
            LPBacktestConfig(strategy_type="lp", il_calculation_method="invalid")

    def test_to_dict(self) -> None:
        """Test serialization to dictionary."""
        config = LPBacktestConfig(
            strategy_type="lp",
            rebalance_on_partial_exit=True,
            boundary_margin_pct=Decimal("7.5"),
        )

        d = config.to_dict()

        assert d["strategy_type"] == "lp"
        assert d["rebalance_on_partial_exit"] is True
        assert d["boundary_margin_pct"] == "7.5"

    def test_from_dict(self) -> None:
        """Test deserialization from dictionary."""
        data = {
            "strategy_type": "lp",
            "rebalance_on_partial_exit": True,
            "boundary_margin_pct": "7.5",
            "volume_multiplier": "15",
        }

        config = LPBacktestConfig.from_dict(data)

        assert config.strategy_type == "lp"
        assert config.rebalance_on_partial_exit is True
        assert config.boundary_margin_pct == Decimal("7.5")
        assert config.volume_multiplier == Decimal("15")

    def test_roundtrip_serialization(self) -> None:
        """Test config survives roundtrip serialization."""
        original = LPBacktestConfig(
            strategy_type="lp",
            il_calculation_method="simplified",
            rebalance_on_partial_exit=True,
            boundary_margin_pct=Decimal("12.5"),
        )

        restored = LPBacktestConfig.from_dict(original.to_dict())

        assert restored.strategy_type == original.strategy_type
        assert restored.il_calculation_method == original.il_calculation_method
        assert restored.rebalance_on_partial_exit == original.rebalance_on_partial_exit
        assert restored.boundary_margin_pct == original.boundary_margin_pct


# =============================================================================
# Tick-to-Price Conversion Tests
# =============================================================================


class TestTickToPrice:
    """Tests for tick-to-price conversion."""

    def test_tick_zero(self) -> None:
        """Test tick 0 equals price 1."""
        adapter = LPBacktestAdapter()
        price = adapter._tick_to_price(0)
        assert price == Decimal("1")

    def test_positive_tick(self) -> None:
        """Test positive tick gives price > 1."""
        adapter = LPBacktestAdapter()
        # tick 1000 should give price = 1.0001^1000 ≈ 1.105
        price = adapter._tick_to_price(1000)
        assert price > Decimal("1")
        assert price < Decimal("2")

    def test_negative_tick(self) -> None:
        """Test negative tick gives price < 1."""
        adapter = LPBacktestAdapter()
        # tick -1000 should give price = 1.0001^(-1000) ≈ 0.905
        price = adapter._tick_to_price(-1000)
        assert price > Decimal("0")
        assert price < Decimal("1")

    def test_tick_symmetry(self) -> None:
        """Test that positive and negative ticks are multiplicative inverses."""
        adapter = LPBacktestAdapter()
        tick = 5000
        price_pos = adapter._tick_to_price(tick)
        price_neg = adapter._tick_to_price(-tick)

        # price_pos * price_neg should approximately equal 1
        product = price_pos * price_neg
        assert abs(product - Decimal("1")) < Decimal("0.0001")


# =============================================================================
# Range Status Tests
# =============================================================================


class TestGetRangeStatus:
    """Tests for get_range_status method."""

    def test_in_range(self) -> None:
        """Test detection of price within range."""
        adapter = LPBacktestAdapter()

        # Create position with tick range that maps to price range ~0.5 to ~2.0
        # At tick=-7000: price ≈ 0.497, at tick=7000: price ≈ 2.013
        position = create_lp_position(
            tick_lower=-7000,
            tick_upper=7000,
        )

        # ETH at $1000, USDC at $1 -> ratio = 1000
        # But that's outside our tick range...
        # Let's use a ratio within range: ETH at $1, USDC at $1 -> ratio = 1
        market = MockMarketState(prices={"ETH": Decimal("1"), "USDC": Decimal("1")})

        result = adapter.get_range_status(position, market)

        assert result is not None
        assert result.status == RangeStatus.IN_RANGE
        assert not result.is_out_of_range
        assert not result.is_approaching_boundary

    def test_below_range(self) -> None:
        """Test detection of price below range."""
        adapter = LPBacktestAdapter()

        # Position with narrow range around price ratio 1
        position = create_lp_position(
            tick_lower=-1000,  # price ≈ 0.905
            tick_upper=1000,   # price ≈ 1.105
        )

        # Price ratio = 0.5 (well below the range)
        market = MockMarketState(prices={"ETH": Decimal("0.5"), "USDC": Decimal("1")})

        result = adapter.get_range_status(position, market)

        assert result is not None
        assert result.status == RangeStatus.BELOW_RANGE
        assert result.is_out_of_range
        assert not result.is_approaching_boundary

    def test_above_range(self) -> None:
        """Test detection of price above range."""
        adapter = LPBacktestAdapter()

        # Position with narrow range around price ratio 1
        position = create_lp_position(
            tick_lower=-1000,  # price ≈ 0.905
            tick_upper=1000,   # price ≈ 1.105
        )

        # Price ratio = 2.0 (well above the range)
        market = MockMarketState(prices={"ETH": Decimal("2"), "USDC": Decimal("1")})

        result = adapter.get_range_status(position, market)

        assert result is not None
        assert result.status == RangeStatus.ABOVE_RANGE
        assert result.is_out_of_range
        assert not result.is_approaching_boundary

    def test_partial_below(self) -> None:
        """Test detection of price approaching lower boundary."""
        config = LPBacktestConfig(
            strategy_type="lp",
            boundary_margin_pct=Decimal("10"),
        )
        adapter = LPBacktestAdapter(config)

        # Position with range around price ratio 1
        position = create_lp_position(
            tick_lower=-1000,  # price ≈ 0.9048
            tick_upper=1000,   # price ≈ 1.1052
        )

        # Price ratio = 0.93 (within 10% of lower bound 0.9048)
        market = MockMarketState(prices={"ETH": Decimal("0.93"), "USDC": Decimal("1")})

        result = adapter.get_range_status(position, market)

        assert result is not None
        assert result.status == RangeStatus.PARTIAL_BELOW
        assert not result.is_out_of_range
        assert result.is_approaching_boundary

    def test_partial_above(self) -> None:
        """Test detection of price approaching upper boundary."""
        config = LPBacktestConfig(
            strategy_type="lp",
            boundary_margin_pct=Decimal("10"),
        )
        adapter = LPBacktestAdapter(config)

        # Position with range around price ratio 1
        position = create_lp_position(
            tick_lower=-1000,  # price ≈ 0.9048
            tick_upper=1000,   # price ≈ 1.1052
        )

        # Price ratio = 1.05 (within 10% of upper bound 1.1052)
        market = MockMarketState(prices={"ETH": Decimal("1.05"), "USDC": Decimal("1")})

        result = adapter.get_range_status(position, market)

        assert result is not None
        assert result.status == RangeStatus.PARTIAL_ABOVE
        assert not result.is_out_of_range
        assert result.is_approaching_boundary

    def test_non_lp_position_returns_none(self) -> None:
        """Test that non-LP positions return None."""
        adapter = LPBacktestAdapter()

        position = SimulatedPosition(
            position_type=PositionType.SPOT,
            protocol="spot",
            tokens=["ETH"],
            amounts={"ETH": Decimal("1")},
            entry_price=Decimal("2000"),
            entry_time=datetime.now(),
        )

        market = MockMarketState(prices={"ETH": Decimal("2000")})

        result = adapter.get_range_status(position, market)
        assert result is None

    def test_missing_tick_bounds_returns_none(self) -> None:
        """Test that LP position without tick bounds returns None."""
        adapter = LPBacktestAdapter()

        position = create_lp_position()
        position.tick_lower = None  # Remove tick bounds

        market = MockMarketState(prices={"ETH": Decimal("2000"), "USDC": Decimal("1")})

        result = adapter.get_range_status(position, market)
        assert result is None

    def test_missing_price_returns_none(self) -> None:
        """Test that missing token price returns None."""
        adapter = LPBacktestAdapter()

        position = create_lp_position()
        market = MockMarketState(prices={})  # No prices

        result = adapter.get_range_status(position, market)
        assert result is None

    def test_range_status_result_to_dict(self) -> None:
        """Test RangeStatusResult serialization."""
        result = RangeStatusResult(
            status=RangeStatus.IN_RANGE,
            current_price_ratio=Decimal("1.5"),
            price_lower=Decimal("1.0"),
            price_upper=Decimal("2.0"),
            distance_to_lower_pct=Decimal("50"),
            distance_to_upper_pct=Decimal("25"),
        )

        d = result.to_dict()

        assert d["status"] == "IN_RANGE"
        assert d["current_price_ratio"] == "1.5"
        assert d["price_lower"] == "1.0"
        assert d["price_upper"] == "2.0"
        assert d["distance_to_lower_pct"] == "50"
        assert d["distance_to_upper_pct"] == "25"
        assert d["is_out_of_range"] is False
        assert d["is_approaching_boundary"] is False


# =============================================================================
# Should Rebalance Tests
# =============================================================================


class TestShouldRebalance:
    """Tests for should_rebalance method."""

    def test_rebalance_disabled_returns_false(self) -> None:
        """Test that disabled rebalance always returns False."""
        config = LPBacktestConfig(
            strategy_type="lp",
            rebalance_on_out_of_range=False,
            rebalance_on_partial_exit=False,
        )
        adapter = LPBacktestAdapter(config)

        position = create_lp_position(tick_lower=-1000, tick_upper=1000)
        # Price well outside range
        market = MockMarketState(prices={"ETH": Decimal("0.1"), "USDC": Decimal("1")})

        result = adapter.should_rebalance(position, market)
        assert result is False

    def test_rebalance_on_out_of_range(self) -> None:
        """Test rebalance triggered when fully out of range."""
        config = LPBacktestConfig(
            strategy_type="lp",
            rebalance_on_out_of_range=True,
        )
        adapter = LPBacktestAdapter(config)

        position = create_lp_position(tick_lower=-1000, tick_upper=1000)
        # Price below range
        market = MockMarketState(prices={"ETH": Decimal("0.5"), "USDC": Decimal("1")})

        result = adapter.should_rebalance(position, market)
        assert result is True

    def test_no_rebalance_when_in_range(self) -> None:
        """Test no rebalance when price is in range."""
        config = LPBacktestConfig(
            strategy_type="lp",
            rebalance_on_out_of_range=True,
        )
        adapter = LPBacktestAdapter(config)

        position = create_lp_position(tick_lower=-1000, tick_upper=1000)
        # Price within range
        market = MockMarketState(prices={"ETH": Decimal("1"), "USDC": Decimal("1")})

        result = adapter.should_rebalance(position, market)
        assert result is False

    def test_rebalance_on_partial_exit(self) -> None:
        """Test rebalance triggered when approaching boundary."""
        config = LPBacktestConfig(
            strategy_type="lp",
            rebalance_on_out_of_range=False,
            rebalance_on_partial_exit=True,
            boundary_margin_pct=Decimal("10"),
        )
        adapter = LPBacktestAdapter(config)

        position = create_lp_position(tick_lower=-1000, tick_upper=1000)
        # Price approaching lower boundary
        market = MockMarketState(prices={"ETH": Decimal("0.93"), "USDC": Decimal("1")})

        result = adapter.should_rebalance(position, market)
        assert result is True

    def test_no_partial_rebalance_when_disabled(self) -> None:
        """Test no rebalance on partial exit when disabled."""
        config = LPBacktestConfig(
            strategy_type="lp",
            rebalance_on_out_of_range=True,
            rebalance_on_partial_exit=False,
            boundary_margin_pct=Decimal("10"),
        )
        adapter = LPBacktestAdapter(config)

        position = create_lp_position(tick_lower=-1000, tick_upper=1000)
        # Price approaching boundary but not out
        market = MockMarketState(prices={"ETH": Decimal("0.93"), "USDC": Decimal("1")})

        result = adapter.should_rebalance(position, market)
        assert result is False  # Not out of range, partial exit disabled


# =============================================================================
# Integration Tests - LP Strategy Accuracy
# =============================================================================


class TestLPStrategyAccuracy:
    """Integration tests validating LP adapter accuracy."""

    def test_full_range_position_always_in_range(self) -> None:
        """Test that a full-range position is always in range."""
        adapter = LPBacktestAdapter()

        # Full range position (like Uniswap V2)
        position = create_lp_position(
            tick_lower=-887272,
            tick_upper=887272,
        )

        # Test various price points
        for price in [Decimal("0.001"), Decimal("1"), Decimal("1000"), Decimal("1000000")]:
            market = MockMarketState(prices={"ETH": price, "USDC": Decimal("1")})
            result = adapter.get_range_status(position, market)

            assert result is not None
            assert result.status == RangeStatus.IN_RANGE
            assert not result.is_out_of_range

    def test_narrow_range_out_of_range_behavior(self) -> None:
        """Test that narrow range position goes out of range as expected."""
        # Use adapter with 0% margin for this test (no partial detection)
        config = LPBacktestConfig(
            strategy_type="lp",
            boundary_margin_pct=Decimal("0"),
        )
        adapter = LPBacktestAdapter(config)

        # Narrow range around price ratio 1 (roughly 0.95 to 1.05)
        position = create_lp_position(
            tick_lower=-500,   # price ≈ 0.9512
            tick_upper=500,    # price ≈ 1.0513
        )

        # In range
        market = MockMarketState(prices={"ETH": Decimal("1"), "USDC": Decimal("1")})
        result = adapter.get_range_status(position, market)
        assert result is not None
        assert result.status == RangeStatus.IN_RANGE

        # Below range
        market = MockMarketState(prices={"ETH": Decimal("0.9"), "USDC": Decimal("1")})
        result = adapter.get_range_status(position, market)
        assert result is not None
        assert result.status == RangeStatus.BELOW_RANGE

        # Above range
        market = MockMarketState(prices={"ETH": Decimal("1.1"), "USDC": Decimal("1")})
        result = adapter.get_range_status(position, market)
        assert result is not None
        assert result.status == RangeStatus.ABOVE_RANGE

    def test_position_value_includes_fees(self) -> None:
        """Test that position valuation includes accumulated fees."""
        config = LPBacktestConfig(
            strategy_type="lp",
            fee_tracking_enabled=True,
        )
        adapter = LPBacktestAdapter(config)

        position = create_lp_position(
            tick_lower=-887272,
            tick_upper=887272,
            liquidity=Decimal("1000000"),
        )
        position.accumulated_fees_usd = Decimal("100")  # Pre-accumulated fees

        market = MockMarketState(prices={"ETH": Decimal("2000"), "USDC": Decimal("1")})

        value = adapter.value_position(position, market)

        # Value should include the accumulated fees
        # The exact token amounts depend on IL calculation, but fees should be added
        assert value > Decimal("0")

    def test_update_position_accrues_fees(self) -> None:
        """Test that update_position accrues fees over time."""
        config = LPBacktestConfig(
            strategy_type="lp",
            fee_tracking_enabled=True,
        )
        adapter = LPBacktestAdapter(config)

        position = create_lp_position()
        initial_fees = position.accumulated_fees_usd

        market = MockMarketState(prices={"ETH": Decimal("2000"), "USDC": Decimal("1")})

        # Simulate 1 day of time passage
        adapter.update_position(position, market, elapsed_seconds=86400)

        # Fees should have increased
        assert position.accumulated_fees_usd > initial_fees

    def test_update_position_updates_token_amounts(self) -> None:
        """Test that update_position updates token amounts based on price."""
        adapter = LPBacktestAdapter()

        position = create_lp_position(
            entry_price=Decimal("2000"),
            amounts={"ETH": Decimal("1"), "USDC": Decimal("2000")},
        )

        # Price increased 10%
        market = MockMarketState(prices={"ETH": Decimal("2200"), "USDC": Decimal("1")})

        adapter.update_position(position, market, elapsed_seconds=3600)

        # Token amounts should have changed based on IL calculation
        # With V3 math, the position composition shifts as price moves
        assert "ETH" in position.amounts
        assert "USDC" in position.amounts

    def test_fee_tracking_disabled(self) -> None:
        """Test that fees are not tracked when disabled."""
        config = LPBacktestConfig(
            strategy_type="lp",
            fee_tracking_enabled=False,
        )
        adapter = LPBacktestAdapter(config)

        position = create_lp_position()
        initial_fees = position.accumulated_fees_usd

        market = MockMarketState(prices={"ETH": Decimal("2000"), "USDC": Decimal("1")})

        adapter.update_position(position, market, elapsed_seconds=86400)

        # Fees should not have changed
        assert position.accumulated_fees_usd == initial_fees

    def test_stablecoin_pair_range(self) -> None:
        """Test LP position with stablecoin pair narrow range."""
        # Use adapter with 0% margin for this test (no partial detection)
        config = LPBacktestConfig(
            strategy_type="lp",
            boundary_margin_pct=Decimal("0"),
        )
        adapter = LPBacktestAdapter(config)

        # Stablecoin pair with very narrow range (0.999 to 1.001)
        position = create_lp_position(
            token0="USDC",
            token1="USDT",
            tick_lower=-10,   # price ≈ 0.999
            tick_upper=10,    # price ≈ 1.001
            fee_tier=Decimal("0.0001"),  # 0.01% fee tier
        )

        # Price at 1:1 - in range
        market = MockMarketState(prices={"USDC": Decimal("1"), "USDT": Decimal("1")})
        result = adapter.get_range_status(position, market)
        assert result is not None
        assert result.status == RangeStatus.IN_RANGE

        # Slight depeg - might be out of range
        market = MockMarketState(prices={"USDC": Decimal("0.995"), "USDT": Decimal("1")})
        result = adapter.get_range_status(position, market)
        assert result is not None
        assert result.status == RangeStatus.BELOW_RANGE

    def test_volatile_pair_wide_range(self) -> None:
        """Test LP position with volatile pair and wide range."""
        adapter = LPBacktestAdapter()

        # ETH/BTC pair with wider range
        position = create_lp_position(
            token0="ETH",
            token1="BTC",
            tick_lower=-10000,  # price ≈ 0.368
            tick_upper=10000,   # price ≈ 2.718
            fee_tier=Decimal("0.003"),  # 0.3% fee tier
        )

        # Price at 0.05 (ETH worth 5% of BTC) - in range
        market = MockMarketState(prices={"ETH": Decimal("2000"), "BTC": Decimal("40000")})
        result = adapter.get_range_status(position, market)
        # Price ratio is 0.05, but our range is 0.368 to 2.718
        # So we're actually BELOW range
        assert result is not None
        assert result.status == RangeStatus.BELOW_RANGE

        # Price at 1.0 (ETH = BTC) - in range
        market = MockMarketState(prices={"ETH": Decimal("40000"), "BTC": Decimal("40000")})
        result = adapter.get_range_status(position, market)
        assert result is not None
        assert result.status == RangeStatus.IN_RANGE


# =============================================================================
# Edge Cases
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_zero_liquidity_position(self) -> None:
        """Test handling of zero liquidity position."""
        adapter = LPBacktestAdapter()

        position = create_lp_position(liquidity=Decimal("0"))
        market = MockMarketState(prices={"ETH": Decimal("2000"), "USDC": Decimal("1")})

        # Should not crash
        result = adapter.get_range_status(position, market)
        assert result is not None

    def test_negative_tick_range(self) -> None:
        """Test handling of negative tick range."""
        adapter = LPBacktestAdapter()

        # Both ticks negative (price range < 1)
        position = create_lp_position(
            tick_lower=-5000,  # price ≈ 0.607
            tick_upper=-1000,  # price ≈ 0.905
        )

        # Price at 0.75 - in range
        market = MockMarketState(prices={"ETH": Decimal("0.75"), "USDC": Decimal("1")})
        result = adapter.get_range_status(position, market)
        assert result is not None
        assert result.status == RangeStatus.IN_RANGE

    def test_positive_tick_range(self) -> None:
        """Test handling of positive tick range."""
        adapter = LPBacktestAdapter()

        # Both ticks positive (price range > 1)
        position = create_lp_position(
            tick_lower=1000,   # price ≈ 1.105
            tick_upper=5000,   # price ≈ 1.649
        )

        # Price at 1.3 - in range
        market = MockMarketState(prices={"ETH": Decimal("1.3"), "USDC": Decimal("1")})
        result = adapter.get_range_status(position, market)
        assert result is not None
        assert result.status == RangeStatus.IN_RANGE

    def test_single_token_position(self) -> None:
        """Test handling of position with only one token."""
        adapter = LPBacktestAdapter()

        position = create_lp_position()
        position.tokens = ["ETH"]  # Remove second token

        market = MockMarketState(prices={"ETH": Decimal("2000")})

        result = adapter.get_range_status(position, market)
        assert result is None

    def test_token1_price_default_to_one(self) -> None:
        """Test that missing token1 price defaults to $1 (stablecoin assumption)."""
        adapter = LPBacktestAdapter()

        position = create_lp_position(
            tick_lower=-1000,
            tick_upper=1000,
        )

        # Only ETH price available, USDC missing
        market = MockMarketState(prices={"ETH": Decimal("1")})

        result = adapter.get_range_status(position, market)
        assert result is not None
        # Should use USDC=$1 assumption, giving ratio=1
        assert result.current_price_ratio == Decimal("1")
        assert result.status == RangeStatus.IN_RANGE

    def test_extreme_tick_values(self) -> None:
        """Test handling of extreme tick values."""
        adapter = LPBacktestAdapter()

        # Near max tick range
        position = create_lp_position(
            tick_lower=-800000,
            tick_upper=800000,
        )

        market = MockMarketState(prices={"ETH": Decimal("1"), "USDC": Decimal("1")})

        result = adapter.get_range_status(position, market)
        assert result is not None
        assert result.status == RangeStatus.IN_RANGE


# =============================================================================
# Execute Intent Tests - LP Open
# =============================================================================


@dataclass
class MockMarketStateWithTimestamp:
    """Mock market state with timestamp for execute_intent testing."""

    prices: dict[str, Decimal] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)

    def get_price(self, token: str) -> Decimal | None:
        """Get price for a token."""
        if token not in self.prices:
            raise KeyError(f"Price not found for {token}")
        return self.prices.get(token)


@dataclass
class MockPortfolio:
    """Mock portfolio for testing."""

    cash_balance: Decimal = Decimal("100000")
    positions: list = field(default_factory=list)


class TestExecuteIntentLPOpen:
    """Tests for execute_intent method with LPOpenIntent."""

    def test_execute_lp_open_creates_position(self) -> None:
        """Test LP open intent creates a proper position."""
        from almanak.framework.intents.vocabulary import LPOpenIntent

        adapter = LPBacktestAdapter()

        intent = LPOpenIntent(
            pool="ETH/USDC",
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
            range_lower=Decimal("0.5"),
            range_upper=Decimal("2.0"),
            protocol="uniswap_v3",
        )

        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )
        portfolio = MockPortfolio()

        fill = adapter.execute_intent(intent, portfolio, market)

        assert fill is not None
        assert fill.success is True
        assert fill.position_delta is not None
        assert fill.position_delta.is_lp is True
        assert fill.tokens == ["ETH", "USDC"]
        assert fill.tokens_out == {"ETH": Decimal("1"), "USDC": Decimal("2000")}

    def test_execute_lp_open_calculates_amount_usd(self) -> None:
        """Test LP open intent calculates correct USD amount."""
        from almanak.framework.intents.vocabulary import LPOpenIntent

        adapter = LPBacktestAdapter()

        intent = LPOpenIntent(
            pool="ETH/USDC",
            amount0=Decimal("1"),  # 1 ETH at $2000 = $2000
            amount1=Decimal("2000"),  # 2000 USDC = $2000
            range_lower=Decimal("0.5"),
            range_upper=Decimal("2.0"),
            protocol="uniswap_v3",
        )

        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )
        portfolio = MockPortfolio()

        fill = adapter.execute_intent(intent, portfolio, market)

        assert fill is not None
        assert fill.amount_usd == Decimal("4000")  # $2000 + $2000

    def test_execute_lp_open_sets_ticks(self) -> None:
        """Test LP open intent converts prices to ticks."""
        from almanak.framework.intents.vocabulary import LPOpenIntent

        adapter = LPBacktestAdapter()

        intent = LPOpenIntent(
            pool="ETH/USDC",
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
            range_lower=Decimal("1"),  # Should give tick 0
            range_upper=Decimal("2"),  # Should give tick ~6931
            protocol="uniswap_v3",
        )

        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("1.5"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )
        portfolio = MockPortfolio()

        fill = adapter.execute_intent(intent, portfolio, market)

        assert fill is not None
        assert fill.position_delta is not None
        assert fill.position_delta.tick_lower == 0  # log(1) / log(1.0001) = 0
        assert fill.position_delta.tick_upper > 0  # log(2) / log(1.0001) > 0

    def test_execute_lp_open_initializes_fee_tracking(self) -> None:
        """Test LP open intent initializes fee tracking fields."""
        from almanak.framework.intents.vocabulary import LPOpenIntent

        adapter = LPBacktestAdapter()

        intent = LPOpenIntent(
            pool="ETH/USDC",
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
            range_lower=Decimal("0.5"),
            range_upper=Decimal("2.0"),
            protocol="uniswap_v3",
        )

        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )
        portfolio = MockPortfolio()

        fill = adapter.execute_intent(intent, portfolio, market)

        assert fill is not None
        assert fill.position_delta is not None
        assert fill.position_delta.accumulated_fees_usd == Decimal("0")
        assert fill.position_delta.fees_token0 == Decimal("0")
        assert fill.position_delta.fees_token1 == Decimal("0")

    def test_execute_lp_open_returns_fill_with_metadata(self) -> None:
        """Test LP open intent returns fill with LP-specific metadata."""
        from almanak.framework.intents.vocabulary import LPOpenIntent

        adapter = LPBacktestAdapter()

        intent = LPOpenIntent(
            pool="ETH/USDC",
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
            range_lower=Decimal("0.5"),
            range_upper=Decimal("2.0"),
            protocol="uniswap_v3",
        )

        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )
        portfolio = MockPortfolio()

        fill = adapter.execute_intent(intent, portfolio, market)

        assert fill is not None
        assert "pool" in fill.metadata
        assert "tick_lower" in fill.metadata
        assert "tick_upper" in fill.metadata
        assert "fee_tier" in fill.metadata
        assert "liquidity" in fill.metadata
        assert fill.metadata["pool"] == "ETH/USDC"

    def test_execute_non_lp_intent_returns_none(self) -> None:
        """Test that non-LP intents return None for default handling."""
        adapter = LPBacktestAdapter()

        # Create a mock non-LP intent
        class MockSwapIntent:
            pass

        mock_intent = MockSwapIntent()
        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000")},
            timestamp=datetime.now(),
        )
        portfolio = MockPortfolio()

        fill = adapter.execute_intent(mock_intent, portfolio, market)
        assert fill is None

    def test_execute_lp_open_with_address_pool(self) -> None:
        """Test LP open intent with address-based pool identifier."""
        from almanak.framework.intents.vocabulary import LPOpenIntent

        adapter = LPBacktestAdapter()

        # Using an address-like pool identifier
        intent = LPOpenIntent(
            pool="0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",  # Address format
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
            range_lower=Decimal("0.5"),
            range_upper=Decimal("2.0"),
            protocol="uniswap_v3",
        )

        market = MockMarketStateWithTimestamp(
            prices={"WETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )
        portfolio = MockPortfolio()

        fill = adapter.execute_intent(intent, portfolio, market)

        assert fill is not None
        # Should default to WETH/USDC when pool is an address
        assert fill.tokens == ["WETH", "USDC"]

    def test_price_to_tick_conversion(self) -> None:
        """Test internal price-to-tick conversion function."""
        adapter = LPBacktestAdapter()

        # Price = 1 should give tick = 0
        tick = adapter._price_to_tick_int(Decimal("1"))
        assert tick == 0

        # Price > 1 should give positive tick
        tick = adapter._price_to_tick_int(Decimal("2"))
        assert tick > 0

        # Price < 1 should give negative tick
        tick = adapter._price_to_tick_int(Decimal("0.5"))
        assert tick < 0

        # Price = 0 should give MIN_TICK
        tick = adapter._price_to_tick_int(Decimal("0"))
        assert tick == -887272


# =============================================================================
# Execute Intent Tests - LP Close
# =============================================================================


class TestExecuteIntentLPClose:
    """Tests for execute_intent method with LPCloseIntent."""

    def test_execute_lp_close_returns_correct_amounts(self) -> None:
        """Test LP close intent returns correct token amounts."""
        from almanak.framework.intents.vocabulary import LPCloseIntent

        adapter = LPBacktestAdapter()

        # Create a position with known amounts
        position = create_lp_position(
            token0="ETH",
            token1="USDC",
            tick_lower=-887272,
            tick_upper=887272,
            entry_price=Decimal("2000"),
            liquidity=Decimal("4000"),  # $4000 initial value
            amounts={"ETH": Decimal("1"), "USDC": Decimal("2000")},
        )
        position.accumulated_fees_usd = Decimal("50")  # $50 in fees
        position.metadata["entry_amounts"] = {"ETH": "1", "USDC": "2000"}

        # Create a mock portfolio with the position
        portfolio = MockPortfolio()
        portfolio.positions = [position]

        intent = LPCloseIntent(
            position_id=position.position_id,
            pool="ETH/USDC",
            collect_fees=True,
            protocol="uniswap_v3",
        )

        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )

        fill = adapter.execute_intent(intent, portfolio, market)

        assert fill is not None
        assert fill.success is True
        assert fill.position_close_id == position.position_id
        assert "ETH" in fill.tokens_in
        assert "USDC" in fill.tokens_in
        # Tokens received should be positive
        assert fill.tokens_in["ETH"] > 0
        assert fill.tokens_in["USDC"] > 0

    def test_execute_lp_close_includes_fees_when_collected(self) -> None:
        """Test LP close includes fees in tokens_in when collect_fees=True."""
        from almanak.framework.intents.vocabulary import LPCloseIntent

        adapter = LPBacktestAdapter()

        position = create_lp_position()
        position.accumulated_fees_usd = Decimal("100")  # $100 in fees
        position.metadata["entry_amounts"] = {"ETH": "1", "USDC": "2000"}

        portfolio = MockPortfolio()
        portfolio.positions = [position]

        intent = LPCloseIntent(
            position_id=position.position_id,
            collect_fees=True,
            protocol="uniswap_v3",
        )

        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )

        fill = adapter.execute_intent(intent, portfolio, market)

        assert fill is not None
        assert fill.success is True
        # Amount should include fees
        assert fill.amount_usd > Decimal("0")
        assert fill.metadata.get("fees_earned_usd") == "100"
        assert fill.metadata.get("collect_fees") is True

    def test_execute_lp_close_excludes_fees_when_not_collected(self) -> None:
        """Test LP close excludes fees when collect_fees=False."""
        from almanak.framework.intents.vocabulary import LPCloseIntent

        adapter = LPBacktestAdapter()

        position = create_lp_position()
        position.accumulated_fees_usd = Decimal("100")
        position.metadata["entry_amounts"] = {"ETH": "1", "USDC": "2000"}

        portfolio = MockPortfolio()
        portfolio.positions = [position]

        # Close with fees collected
        intent_with_fees = LPCloseIntent(
            position_id=position.position_id,
            collect_fees=True,
            protocol="uniswap_v3",
        )

        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )

        fill_with_fees = adapter.execute_intent(intent_with_fees, portfolio, market)
        amount_with_fees = fill_with_fees.amount_usd

        # Close without fees
        intent_no_fees = LPCloseIntent(
            position_id=position.position_id,
            collect_fees=False,
            protocol="uniswap_v3",
        )

        fill_no_fees = adapter.execute_intent(intent_no_fees, portfolio, market)
        amount_no_fees = fill_no_fees.amount_usd

        # Amount with fees should be higher
        assert amount_with_fees > amount_no_fees
        assert fill_no_fees.metadata.get("collect_fees") is False

    def test_execute_lp_close_calculates_il(self) -> None:
        """Test LP close calculates impermanent loss correctly."""
        from almanak.framework.intents.vocabulary import LPCloseIntent

        adapter = LPBacktestAdapter()

        # Create position at entry price of 2000
        position = create_lp_position(
            entry_price=Decimal("2000"),
            liquidity=Decimal("4000"),
        )
        position.metadata["entry_amounts"] = {"ETH": "1", "USDC": "2000"}

        portfolio = MockPortfolio()
        portfolio.positions = [position]

        intent = LPCloseIntent(
            position_id=position.position_id,
            protocol="uniswap_v3",
        )

        # Price changed by 50% (2000 -> 3000)
        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("3000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )

        fill = adapter.execute_intent(intent, portfolio, market)

        assert fill is not None
        assert fill.success is True
        # IL should be recorded in metadata
        assert "il_percentage" in fill.metadata
        assert "il_loss_usd" in fill.metadata
        # IL should be non-zero when price changed
        il_pct = Decimal(fill.metadata["il_percentage"])
        assert il_pct != Decimal("0")

    def test_execute_lp_close_calculates_net_pnl(self) -> None:
        """Test LP close calculates net PnL correctly."""
        from almanak.framework.intents.vocabulary import LPCloseIntent

        adapter = LPBacktestAdapter()

        position = create_lp_position(
            entry_price=Decimal("2000"),
            liquidity=Decimal("4000"),
        )
        position.accumulated_fees_usd = Decimal("200")  # Good fee earnings
        position.metadata["entry_amounts"] = {"ETH": "1", "USDC": "2000"}

        portfolio = MockPortfolio()
        portfolio.positions = [position]

        intent = LPCloseIntent(
            position_id=position.position_id,
            collect_fees=True,
            protocol="uniswap_v3",
        )

        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )

        fill = adapter.execute_intent(intent, portfolio, market)

        assert fill is not None
        assert fill.success is True
        assert "net_lp_pnl_usd" in fill.metadata
        # Net PnL includes fees earned
        net_pnl = Decimal(fill.metadata["net_lp_pnl_usd"])
        # Should be positive when we earned fees and no IL (price unchanged)
        assert net_pnl >= Decimal("0")

    def test_execute_lp_close_position_not_found(self) -> None:
        """Test LP close fails gracefully when position not found."""
        from almanak.framework.intents.vocabulary import LPCloseIntent

        adapter = LPBacktestAdapter()

        portfolio = MockPortfolio()
        portfolio.positions = []  # Empty portfolio

        intent = LPCloseIntent(
            position_id="nonexistent_position_id",
            protocol="uniswap_v3",
        )

        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )

        fill = adapter.execute_intent(intent, portfolio, market)

        assert fill is not None
        assert fill.success is False
        assert fill.position_close_id == "nonexistent_position_id"
        assert "not found" in fill.metadata.get("failure_reason", "").lower()

    def test_execute_lp_close_returns_fill_with_metadata(self) -> None:
        """Test LP close returns fill with detailed metadata."""
        from almanak.framework.intents.vocabulary import LPCloseIntent

        adapter = LPBacktestAdapter()

        position = create_lp_position()
        position.metadata["entry_amounts"] = {"ETH": "1", "USDC": "2000"}

        portfolio = MockPortfolio()
        portfolio.positions = [position]

        intent = LPCloseIntent(
            position_id=position.position_id,
            pool="ETH/USDC",
            protocol="uniswap_v3",
        )

        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )

        fill = adapter.execute_intent(intent, portfolio, market)

        assert fill is not None
        # Check all expected metadata fields
        assert "position_id" in fill.metadata
        assert "pool" in fill.metadata
        assert "current_price_ratio" in fill.metadata
        assert "il_percentage" in fill.metadata
        assert "il_loss_usd" in fill.metadata
        assert "fees_earned_usd" in fill.metadata
        assert "net_lp_pnl_usd" in fill.metadata
        assert "initial_value_usd" in fill.metadata
        assert "current_value_usd" in fill.metadata
        assert "token0_price_usd" in fill.metadata
        assert "token1_price_usd" in fill.metadata

    def test_execute_lp_close_sets_position_close_id(self) -> None:
        """Test LP close sets position_close_id for portfolio handling."""
        from almanak.framework.intents.vocabulary import LPCloseIntent

        adapter = LPBacktestAdapter()

        position = create_lp_position()
        position.metadata["entry_amounts"] = {"ETH": "1", "USDC": "2000"}
        position_id = position.position_id

        portfolio = MockPortfolio()
        portfolio.positions = [position]

        intent = LPCloseIntent(
            position_id=position_id,
            protocol="uniswap_v3",
        )

        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )

        fill = adapter.execute_intent(intent, portfolio, market)

        assert fill is not None
        assert fill.position_close_id == position_id

    def test_execute_lp_close_with_price_increase(self) -> None:
        """Test LP close with price increase shows IL but potential profit from fees."""
        from almanak.framework.intents.vocabulary import LPCloseIntent

        adapter = LPBacktestAdapter()

        position = create_lp_position(
            entry_price=Decimal("2000"),
            liquidity=Decimal("4000"),
        )
        position.accumulated_fees_usd = Decimal("500")  # Significant fees
        position.metadata["entry_amounts"] = {"ETH": "1", "USDC": "2000"}

        portfolio = MockPortfolio()
        portfolio.positions = [position]

        intent = LPCloseIntent(
            position_id=position.position_id,
            collect_fees=True,
            protocol="uniswap_v3",
        )

        # 50% price increase
        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("3000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )

        fill = adapter.execute_intent(intent, portfolio, market)

        assert fill is not None
        assert fill.success is True
        # IL should be positive (represents a loss)
        il_pct = Decimal(fill.metadata["il_percentage"])
        assert il_pct > Decimal("0")
        # But with high fees, net PnL could still be positive
        fees = Decimal(fill.metadata["fees_earned_usd"])
        assert fees == Decimal("500")

    def test_execute_lp_close_with_price_decrease(self) -> None:
        """Test LP close with price decrease shows IL correctly."""
        from almanak.framework.intents.vocabulary import LPCloseIntent

        adapter = LPBacktestAdapter()

        position = create_lp_position(
            entry_price=Decimal("2000"),
            liquidity=Decimal("4000"),
        )
        position.accumulated_fees_usd = Decimal("50")
        position.metadata["entry_amounts"] = {"ETH": "1", "USDC": "2000"}

        portfolio = MockPortfolio()
        portfolio.positions = [position]

        intent = LPCloseIntent(
            position_id=position.position_id,
            collect_fees=True,
            protocol="uniswap_v3",
        )

        # 25% price decrease
        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("1500"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )

        fill = adapter.execute_intent(intent, portfolio, market)

        assert fill is not None
        assert fill.success is True
        # IL should be non-zero
        il_pct = Decimal(fill.metadata["il_percentage"])
        assert il_pct != Decimal("0")


# =============================================================================
# Historical Volume Integration Tests
# =============================================================================


class TestHistoricalVolumeIntegration:
    """Tests for historical volume integration in fee accrual."""

    def test_config_historical_volume_defaults(self) -> None:
        """Test default historical volume configuration."""
        config = LPBacktestConfig(strategy_type="lp")

        assert config.use_historical_volume is True
        assert config.chain == "arbitrum"
        assert config.subgraph_api_key is None

    def test_config_historical_volume_custom(self) -> None:
        """Test custom historical volume configuration."""
        config = LPBacktestConfig(
            strategy_type="lp",
            use_historical_volume=False,
            chain="ethereum",
            subgraph_api_key="test_api_key",
        )

        assert config.use_historical_volume is False
        assert config.chain == "ethereum"
        assert config.subgraph_api_key == "test_api_key"

    def test_config_serialization_with_volume_settings(self) -> None:
        """Test config serialization includes volume settings."""
        config = LPBacktestConfig(
            strategy_type="lp",
            use_historical_volume=True,
            chain="base",
            subgraph_api_key="my_api_key",
        )

        d = config.to_dict()

        assert d["use_historical_volume"] is True
        assert d["chain"] == "base"
        assert d["subgraph_api_key"] == "my_api_key"

    def test_config_deserialization_with_volume_settings(self) -> None:
        """Test config deserialization restores volume settings."""
        data = {
            "strategy_type": "lp",
            "use_historical_volume": False,
            "chain": "optimism",
            "subgraph_api_key": "restored_key",
        }

        config = LPBacktestConfig.from_dict(data)

        assert config.use_historical_volume is False
        assert config.chain == "optimism"
        assert config.subgraph_api_key == "restored_key"

    def test_adapter_with_volume_provider_disabled(self) -> None:
        """Test adapter with historical volume disabled uses heuristic."""
        config = LPBacktestConfig(
            strategy_type="lp",
            use_historical_volume=False,
        )
        adapter = LPBacktestAdapter(config)

        # Ensure volume provider is not initialized
        provider = adapter._ensure_volume_provider()
        assert provider is None

    def test_adapter_stores_pool_address_in_metadata(self) -> None:
        """Test LP open stores pool address in position metadata."""
        from almanak.framework.intents.vocabulary import LPOpenIntent

        adapter = LPBacktestAdapter()

        # Use address format pool
        intent = LPOpenIntent(
            pool="0xc31e54c7a869b9fcbecc14363cf510d1c41fa443",
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
            range_lower=Decimal("0.5"),
            range_upper=Decimal("2.0"),
            protocol="uniswap_v3",
        )

        market = MockMarketStateWithTimestamp(
            prices={"WETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )
        portfolio = MockPortfolio()

        fill = adapter.execute_intent(intent, portfolio, market)

        assert fill is not None
        assert fill.position_delta is not None
        assert "pool_address" in fill.position_delta.metadata
        assert fill.position_delta.metadata["pool_address"] == "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443"

    def test_adapter_pool_address_none_for_token_format(self) -> None:
        """Test LP open with token format pool has None pool_address."""
        from almanak.framework.intents.vocabulary import LPOpenIntent

        adapter = LPBacktestAdapter()

        # Use token pair format pool (not an address)
        intent = LPOpenIntent(
            pool="ETH/USDC",
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
            range_lower=Decimal("0.5"),
            range_upper=Decimal("2.0"),
            protocol="uniswap_v3",
        )

        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )
        portfolio = MockPortfolio()

        fill = adapter.execute_intent(intent, portfolio, market)

        assert fill is not None
        assert fill.position_delta is not None
        assert "pool_address" in fill.position_delta.metadata
        # Token pair format doesn't provide a pool address
        assert fill.position_delta.metadata["pool_address"] is None

    def test_fee_accrual_uses_estimated_volume_without_provider(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test fee accrual uses estimated volume when provider not available."""
        import logging

        config = LPBacktestConfig(
            strategy_type="lp",
            use_historical_volume=False,  # Disable historical volume
            fee_tracking_enabled=True,
        )
        adapter = LPBacktestAdapter(config)

        position = create_lp_position()

        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )

        with caplog.at_level(logging.DEBUG, logger="almanak.framework.backtesting.adapters.lp_adapter"):
            adapter.update_position(position, market, elapsed_seconds=86400)

        # Should have used estimated volume (look for "estimated" in log)
        assert position.accumulated_fees_usd > Decimal("0")

    def test_fee_accrual_falls_back_to_estimated_on_error(self) -> None:
        """Test fee accrual falls back to estimated when historical lookup fails."""
        config = LPBacktestConfig(
            strategy_type="lp",
            use_historical_volume=True,
            fee_tracking_enabled=True,
        )
        adapter = LPBacktestAdapter(config)

        position = create_lp_position()
        # Set a pool address that will fail lookup
        position.metadata["pool_address"] = "0x0000000000000000000000000000000000000000"

        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )

        # Should not raise, should fall back to estimated
        adapter.update_position(position, market, elapsed_seconds=86400)

        # Fees should still be accrued using fallback
        assert position.accumulated_fees_usd > Decimal("0")

    def test_adapter_caches_volume_lookups(self) -> None:
        """Test that volume lookups are cached."""
        from almanak.framework.backtesting.pnl.types import DataConfidence

        config = LPBacktestConfig(
            strategy_type="lp",
            use_historical_volume=True,
        )
        adapter = LPBacktestAdapter(config)

        # Simulate a failed lookup that gets cached
        pool_address = "0xtest"
        timestamp = datetime.now()

        # First call - will fail but cache the result (tuple of value, confidence)
        result1_volume, result1_confidence = adapter._get_historical_volume(pool_address, timestamp)

        # Verify it's cached (cache key exists)
        cache_key = (pool_address.lower(), timestamp.date())
        assert cache_key in adapter._volume_cache

        # Second call should return cached result
        result2_volume, result2_confidence = adapter._get_historical_volume(pool_address, timestamp)

        # Both should have LOW confidence (cached failure or fallback result)
        # The volume value may be 0 (fallback) or None depending on provider behavior
        assert result1_confidence == DataConfidence.LOW
        assert result2_confidence == DataConfidence.LOW
        # Verify cached values match
        assert result1_volume == result2_volume
        assert result1_confidence == result2_confidence

    def test_adapter_accepts_external_volume_provider(self) -> None:
        """Test adapter can accept an external volume provider."""
        from unittest.mock import MagicMock

        mock_provider = MagicMock()
        mock_provider.chain = "arbitrum"

        adapter = LPBacktestAdapter(volume_provider=mock_provider)

        # Should use the provided provider
        assert adapter._volume_provider is mock_provider
        assert adapter._volume_provider_initialized is True

    def test_config_roundtrip_with_all_volume_settings(self) -> None:
        """Test full roundtrip serialization of config with volume settings."""
        original = LPBacktestConfig(
            strategy_type="lp",
            use_historical_volume=True,
            chain="polygon",
            subgraph_api_key="secret_key",
            volume_multiplier=Decimal("15"),
        )

        restored = LPBacktestConfig.from_dict(original.to_dict())

        assert restored.use_historical_volume == original.use_historical_volume
        assert restored.chain == original.chain
        assert restored.subgraph_api_key == original.subgraph_api_key
        assert restored.volume_multiplier == original.volume_multiplier
