"""Tests for LP backtest adapter functionality.

This module tests the LPBacktestAdapter, focusing on:
- Out-of-range detection and handling
- Partial range exit scenarios
- Range status calculations
- Tick-to-price conversions
- Fee accrual
- Position valuation with IL
"""

from dataclasses import dataclass, field, replace
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.backtesting.adapters.lp_adapter import (
    HeuristicValidationSample,
    LPBacktestAdapter,
    LPBacktestConfig,
    RangeStatus,
    RangeStatusResult,
)
from almanak.framework.backtesting.exceptions import DataSourceUnavailableError
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

    def test_zero_explicit_pool_liquidity_raises(self) -> None:
        """A zero TVL is a nonsensical share denominator: fail at construction
        instead of silently degrading to the 0.5-share fallback in fee accrual."""
        with pytest.raises(ValueError, match="explicit_pool_liquidity_usd must be positive"):
            LPBacktestConfig(strategy_type="lp", explicit_pool_liquidity_usd=Decimal("0"))

    def test_negative_explicit_pool_liquidity_raises(self) -> None:
        with pytest.raises(ValueError, match="explicit_pool_liquidity_usd must be positive"):
            LPBacktestConfig(strategy_type="lp", explicit_pool_liquidity_usd=Decimal("-100"))

    def test_positive_explicit_pool_liquidity_accepted(self) -> None:
        config = LPBacktestConfig(strategy_type="lp", explicit_pool_liquidity_usd=Decimal("2000000"))
        assert config.explicit_pool_liquidity_usd == Decimal("2000000")

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
            tick_upper=1000,  # price ≈ 1.105
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
            tick_upper=1000,  # price ≈ 1.105
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
            tick_upper=1000,  # price ≈ 1.1052
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
            tick_upper=1000,  # price ≈ 1.1052
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

    def test_strict_missing_token0_price_raises(self) -> None:
        """Strict mode must not silently skip range checks when token0 is missing."""
        from almanak.framework.backtesting.exceptions import HistoricalDataUnavailableError

        adapter = LPBacktestAdapter(LPBacktestConfig(strategy_type="lp", strict_reproducibility=True))

        position = create_lp_position()
        market = MockMarketState(prices={"USDC": Decimal("1")})

        with pytest.raises(HistoricalDataUnavailableError, match="ETH"):
            adapter.get_range_status(position, market)

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
            tick_lower=-500,  # price ≈ 0.9512
            tick_upper=500,  # price ≈ 1.0513
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

    def test_value_position_strict_missing_token0_price_raises(self) -> None:
        """Strict valuation must not silently reuse entry price for missing token0."""
        from almanak.framework.backtesting.exceptions import HistoricalDataUnavailableError

        config = LPBacktestConfig(
            strategy_type="lp",
            fee_tracking_enabled=False,
            strict_reproducibility=True,
        )
        adapter = LPBacktestAdapter(config)

        position = create_lp_position()
        market = MockMarketState(prices={"USDC": Decimal("1")})

        with pytest.raises(HistoricalDataUnavailableError, match="ETH"):
            adapter.value_position(position, market)

    def test_update_position_accrues_fees(self) -> None:
        """Test that update_position accrues fees over time."""
        config = LPBacktestConfig(
            strategy_type="lp",
            fee_tracking_enabled=True,
            # VIB-4849: no subgraph/explicit volume here -> opt into the heuristic.
            allow_volume_fallback=True,
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
        # VIB-4849: this test checks token-amount math, not fees. Disable fee
        # tracking so it does not require a volume source.
        adapter = LPBacktestAdapter(LPBacktestConfig(strategy_type="lp", fee_tracking_enabled=False))

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
            tick_lower=-10,  # price ≈ 0.999
            tick_upper=10,  # price ≈ 1.001
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
            tick_upper=10000,  # price ≈ 2.718
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
            tick_lower=1000,  # price ≈ 1.105
            tick_upper=5000,  # price ≈ 1.649
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

    def test_malformed_value_position_skips_missing_prices(self) -> None:
        """Simple fallback valuation should use measured prices and skip missing ones."""
        adapter = LPBacktestAdapter()

        position = create_lp_position()
        position.tokens = ["ETH"]
        position.amounts = {"ETH": Decimal("2"), "MISSING": Decimal("5")}
        market = MockMarketState(prices={"ETH": Decimal("1000")})

        assert adapter.value_position(position, market) == Decimal("2000")

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

    def test_execute_lp_open_zero_notional_plan_rejects(self, monkeypatch) -> None:
        """A plan that resolves to $0 must reject, not open a zero-liquidity position.

        The typed vocabulary rejects zero amounts and prices fall back to $1,
        so this is a defense-in-depth invariant on the plan itself.
        """
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

        real_plan = adapter._build_lp_open_plan(intent, market)
        zero_plan = replace(real_plan, amount_usd=Decimal("0"))
        monkeypatch.setattr(adapter, "_build_lp_open_plan", lambda *_args, **_kwargs: zero_plan)

        fill = adapter.execute_intent(intent, MockPortfolio(), market)

        assert fill is not None
        assert fill.success is False
        assert "zero-notional" in fill.metadata.get("failure_reason", "")

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

    def test_execute_lp_close_excludes_uncollected_fees_from_net_pnl(self) -> None:
        """Uncollected fees are reported but not realized into close PnL."""
        from almanak.framework.intents.vocabulary import LPCloseIntent

        adapter = LPBacktestAdapter()

        position = create_lp_position()
        position.accumulated_fees_usd = Decimal("100")
        position.metadata["entry_amounts"] = {"ETH": "1", "USDC": "2000"}

        portfolio = MockPortfolio()
        portfolio.positions = [position]

        intent = LPCloseIntent(
            position_id=position.position_id,
            collect_fees=False,
            protocol="uniswap_v3",
        )
        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )

        fill = adapter.execute_intent(intent, portfolio, market)

        assert fill is not None
        assert fill.success is True
        initial_value = Decimal(fill.metadata["initial_value_usd"])
        current_value = Decimal(fill.metadata["current_value_usd"])
        assert fill.amount_usd == current_value
        assert Decimal(fill.metadata["fees_earned_usd"]) == Decimal("100")
        assert Decimal(fill.metadata["net_lp_pnl_usd"]) == current_value - initial_value

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

    def test_config_historical_volume_custom(self) -> None:
        """Test custom historical volume configuration."""
        config = LPBacktestConfig(
            strategy_type="lp",
            use_historical_volume=False,
            chain="ethereum",
        )

        assert config.use_historical_volume is False
        assert config.chain == "ethereum"

    def test_config_serialization_with_volume_settings(self) -> None:
        """Test config serialization includes volume settings."""
        config = LPBacktestConfig(
            strategy_type="lp",
            use_historical_volume=True,
            chain="base",
        )

        d = config.to_dict()

        assert d["use_historical_volume"] is True
        assert d["chain"] == "base"
        # The dead subgraph_api_key field was removed: the gateway DEX-volume
        # lane needs no operator-side API key, and a serialized secret slot
        # that nothing consumes is a footgun.
        assert "subgraph_api_key" not in d

    def test_config_deserialization_with_volume_settings(self) -> None:
        """Test config deserialization restores volume settings."""
        data = {
            "strategy_type": "lp",
            "use_historical_volume": False,
            "chain": "optimism",
        }

        config = LPBacktestConfig.from_dict(data)

        assert config.use_historical_volume is False
        assert config.chain == "optimism"

    def test_config_deserialization_ignores_legacy_subgraph_api_key(self) -> None:
        """Configs serialized by older SDK versions still deserialize cleanly."""
        data = {
            "strategy_type": "lp",
            "use_historical_volume": True,
            "chain": "optimism",
            "subgraph_api_key": "legacy_key",
        }

        config = LPBacktestConfig.from_dict(data)

        assert config.use_historical_volume is True
        assert config.chain == "optimism"
        assert not hasattr(config, "subgraph_api_key")

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

    def test_adapter_normalizes_uppercase_pool_address_in_metadata(self) -> None:
        """Pool-address detection is case-insensitive and stores lowercase."""
        from almanak.framework.intents.vocabulary import LPOpenIntent

        adapter = LPBacktestAdapter()
        intent = LPOpenIntent(
            pool="0XC31E54C7A869B9FCBECC14363CF510D1C41FA443",
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

    def test_fee_accrual_raises_without_volume_source_or_optin(self) -> None:
        """VIB-4849: no volume source + no opt-in must fail loud, not fabricate."""
        config = LPBacktestConfig(
            strategy_type="lp",
            use_historical_volume=False,  # No historical volume
            fee_tracking_enabled=True,
            # allow_volume_fallback defaults to False -> must raise
        )
        adapter = LPBacktestAdapter(config)

        position = create_lp_position()
        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )

        with pytest.raises(DataSourceUnavailableError) as exc_info:
            adapter.update_position(position, market, elapsed_seconds=86400)

        # Error must tell the user exactly what to provide.
        message = str(exc_info.value)
        assert "use_historical_volume" in message
        assert "explicit_pool_volume_usd_daily" in message
        assert "allow_volume_fallback" in message
        # The historical path is gateway-backed (VIB-4851 Phase D); the removed
        # subgraph_api_key field was never consumed, so recommending it would
        # send users down a dead end.
        assert "subgraph_api_key" not in message
        # And it must NOT have fabricated any fees.
        assert position.accumulated_fees_usd == Decimal("0")

    def test_update_position_missing_volume_is_atomic(self) -> None:
        """A missing-volume failure must not leave token amounts half-updated."""
        config = LPBacktestConfig(
            strategy_type="lp",
            use_historical_volume=False,
            fee_tracking_enabled=True,
        )
        adapter = LPBacktestAdapter(config)

        position = create_lp_position(
            entry_price=Decimal("2000"),
            amounts={"ETH": Decimal("1"), "USDC": Decimal("2000")},
        )
        before_amounts = dict(position.amounts)
        before_metadata = dict(position.metadata)
        before_last_updated = position.last_updated
        before_fees = (
            position.fees_earned,
            position.accumulated_fees_usd,
            position.fees_token0,
            position.fees_token1,
            position.fee_confidence,
            position.slippage_confidence,
        )
        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2200"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )

        with pytest.raises(DataSourceUnavailableError):
            adapter.update_position(position, market, elapsed_seconds=86400)

        assert position.amounts == before_amounts
        assert position.metadata == before_metadata
        assert position.last_updated == before_last_updated
        assert (
            position.fees_earned,
            position.accumulated_fees_usd,
            position.fees_token0,
            position.fees_token1,
            position.fee_confidence,
            position.slippage_confidence,
        ) == before_fees

    def test_fee_accrual_uses_estimated_volume_with_optin(self, caplog: pytest.LogCaptureFixture) -> None:
        """Fee accrual uses the heuristic when the caller explicitly opts in."""
        import logging

        config = LPBacktestConfig(
            strategy_type="lp",
            use_historical_volume=False,  # No historical volume
            fee_tracking_enabled=True,
            allow_volume_fallback=True,  # Explicit opt-in to the heuristic
        )
        adapter = LPBacktestAdapter(config)

        position = create_lp_position()
        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )

        with caplog.at_level(logging.WARNING, logger="almanak.framework.backtesting.adapters.lp_adapter"):
            adapter.update_position(position, market, elapsed_seconds=86400)

        assert position.accumulated_fees_usd > Decimal("0")
        # Heuristic use must be loudly flagged.
        assert any("OPT-IN fallback volume multiplier" in r.message for r in caplog.records)

    def test_fee_accrual_raises_when_historical_lookup_fails_without_optin(self) -> None:
        """VIB-4849: failed historical lookup + no opt-in must raise, not fabricate."""
        config = LPBacktestConfig(
            strategy_type="lp",
            use_historical_volume=True,
            fee_tracking_enabled=True,
        )
        adapter = LPBacktestAdapter(config)

        position = create_lp_position()
        # Set a pool address that will fail lookup (returns LOW-confidence None)
        position.metadata["pool_address"] = "0x0000000000000000000000000000000000000000"

        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )

        with pytest.raises(DataSourceUnavailableError):
            adapter.update_position(position, market, elapsed_seconds=86400)
        assert position.accumulated_fees_usd == Decimal("0")

    def test_fee_accrual_uses_explicit_volume_without_subgraph(self) -> None:
        """VIB-4849: explicit caller-provided volume works without any subgraph."""
        config = LPBacktestConfig(
            strategy_type="lp",
            use_historical_volume=False,
            fee_tracking_enabled=True,
            explicit_pool_volume_usd_daily=Decimal("5000000"),  # $5M/day
            explicit_pool_liquidity_usd=Decimal("2000000"),  # $2M TVL
        )
        adapter = LPBacktestAdapter(config)

        position = create_lp_position()
        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )

        # Should not raise; uses explicit volume directly.
        adapter.update_position(position, market, elapsed_seconds=86400)
        assert position.accumulated_fees_usd > Decimal("0")
        # Explicit volume is a trusted source -> not LOW confidence.
        assert position.fee_confidence != "low"

    def test_fee_slippage_result_records_fractional_units(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Fee slippage metadata uses fractional units, not display percentage."""
        from almanak.framework.backtesting.pnl.fee_models.slippage_guard import HistoricalSlippageResult
        from almanak.framework.backtesting.pnl.types import DataConfidence

        adapter = LPBacktestAdapter(LPBacktestConfig(strategy_type="lp"))

        def fake_calculate_slippage(**_kwargs):
            return HistoricalSlippageResult(
                slippage=Decimal("0.0125"),
                slippage_bps=125,
                liquidity_usd=Decimal("2500000"),
                confidence=DataConfidence.HIGH,
                data_source="test",
                pool_type="v3",
                was_fallback=False,
            )

        monkeypatch.setattr(adapter, "_calculate_slippage", fake_calculate_slippage)

        result = adapter._fee_slippage_result(
            fees_usd=Decimal("10"),
            position_value_usd=Decimal("10000"),
            timestamp=datetime(2024, 1, 15),
            pool_address="0x0000000000000000000000000000000000000001",
            protocol="uniswap_v3",
        )

        assert result.confidence == "high"
        assert result.pct == Decimal("0.0125")
        assert result.liquidity_usd == Decimal("2500000")

    def test_adapter_does_not_cache_retryable_volume_misses(self) -> None:
        """Transport/retryable misses must not poison the per-day cache."""
        from almanak.framework.backtesting.pnl.types import DataConfidence

        config = LPBacktestConfig(
            strategy_type="lp",
            use_historical_volume=True,
        )
        adapter = LPBacktestAdapter(config)

        # With no connected gateway both measured lanes miss retryably.
        pool_address = "0xtest"
        timestamp = datetime.now()

        result1_volume, result1_confidence = adapter._get_historical_volume(pool_address, timestamp)

        cache_key = (pool_address.lower(), timestamp.date())
        assert cache_key not in adapter._volume_cache

        result2_volume, result2_confidence = adapter._get_historical_volume(pool_address, timestamp)

        # Both should have LOW confidence (cached failure or fallback result)
        # The volume value may be 0 (fallback) or None depending on provider behavior
        assert result1_confidence == DataConfidence.LOW
        assert result2_confidence == DataConfidence.LOW
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
            volume_multiplier=Decimal("15"),
        )

        restored = LPBacktestConfig.from_dict(original.to_dict())

        assert restored.use_historical_volume == original.use_historical_volume
        assert restored.chain == original.chain
        assert restored.volume_multiplier == original.volume_multiplier

    def test_config_roundtrip_preserves_vib4849_fields(self) -> None:
        """Roundtrip serialization preserves the VIB-4849 explicit/opt-in fields."""
        original = LPBacktestConfig(
            strategy_type="lp",
            allow_volume_fallback=True,
            explicit_pool_volume_usd_daily=Decimal("1234567"),
            explicit_pool_liquidity_usd=Decimal("9876543"),
        )

        restored = LPBacktestConfig.from_dict(original.to_dict())

        assert restored.allow_volume_fallback is True
        assert restored.explicit_pool_volume_usd_daily == Decimal("1234567")
        assert restored.explicit_pool_liquidity_usd == Decimal("9876543")

    def test_config_default_vib4849_fields(self) -> None:
        """New VIB-4849 config fields default to safe (non-fabricating) values."""
        config = LPBacktestConfig(strategy_type="lp")
        assert config.allow_volume_fallback is False
        assert config.explicit_pool_volume_usd_daily is None
        assert config.explicit_pool_liquidity_usd is None


class TestValidateHeuristics:
    """Tests for LPBacktestAdapter.validate_heuristics (VIB-4849)."""

    def _adapter(self) -> LPBacktestAdapter:
        return LPBacktestAdapter(LPBacktestConfig(strategy_type="lp", allow_volume_fallback=True))

    def test_validate_heuristics_warns_on_large_error(self, caplog: pytest.LogCaptureFixture) -> None:
        """A sample whose observed fees are >50% off the heuristic warns and flags."""
        import logging

        adapter = self._adapter()
        # Heuristic for this sample is non-trivial; observed is deliberately tiny so
        # the relative error far exceeds 50%.
        sample = HeuristicValidationSample(
            position_value_usd=Decimal("10000"),
            liquidity=Decimal("1000000"),
            fee_tier=Decimal("0.003"),
            elapsed_seconds=86400,
            observed_fees_usd=Decimal("0.01"),
            label="WETH/USDC 0.3% 2024-01-15",
        )

        with caplog.at_level(logging.WARNING, logger="almanak.framework.backtesting.adapters.lp_adapter"):
            results = adapter.validate_heuristics([sample])

        assert len(results) == 1
        assert results[0].exceeds_threshold is True
        assert results[0].error_pct > Decimal("0.5")
        assert any("heuristic validation FAILED" in r.message for r in caplog.records)

    def test_validate_heuristics_ok_when_close(self, caplog: pytest.LogCaptureFixture) -> None:
        """A sample whose observed fees match the heuristic does not warn."""
        import logging

        adapter = self._adapter()
        sample = HeuristicValidationSample(
            position_value_usd=Decimal("10000"),
            liquidity=Decimal("1000000"),
            fee_tier=Decimal("0.003"),
            elapsed_seconds=86400,
            observed_fees_usd=Decimal("0"),  # placeholder; replaced below
            label="close-match",
        )
        # Make observed exactly equal to the heuristic estimate.
        estimate = adapter._estimate_heuristic_fees(sample)
        sample.observed_fees_usd = estimate

        with caplog.at_level(logging.WARNING, logger="almanak.framework.backtesting.adapters.lp_adapter"):
            results = adapter.validate_heuristics([sample])

        assert results[0].exceeds_threshold is False
        assert results[0].error_pct == Decimal("0")
        assert not any("heuristic validation FAILED" in r.message for r in caplog.records)

    def test_validate_heuristics_empty_samples(self) -> None:
        """Empty sample list returns an empty result without error."""
        adapter = self._adapter()
        assert adapter.validate_heuristics([]) == []

    def test_validate_heuristics_custom_threshold(self) -> None:
        """A stricter threshold flags an otherwise-acceptable sample."""
        adapter = self._adapter()
        sample = HeuristicValidationSample(
            position_value_usd=Decimal("10000"),
            liquidity=Decimal("1000000"),
            fee_tier=Decimal("0.003"),
            elapsed_seconds=86400,
            observed_fees_usd=Decimal("0"),
            label="threshold",
        )
        estimate = adapter._estimate_heuristic_fees(sample)
        # Observed 20% below the estimate.
        sample.observed_fees_usd = estimate * Decimal("0.8")

        # 25% relative error -> OK under default 50%, FAIL under 10%.
        assert adapter.validate_heuristics([sample], warn_threshold_pct=Decimal("0.5"))[0].exceeds_threshold is False
        assert adapter.validate_heuristics([sample], warn_threshold_pct=Decimal("0.1"))[0].exceeds_threshold is True


class TestMeasuredZeroVolume:
    """VIB-4849 (P2): Empty != Zero -- a measured zero volume is a valid source.

    A real ``0`` daily volume (explicit or observed) must produce zero fees, NOT
    trigger the missing-source ``DataSourceUnavailableError``. Only an *absent*
    (unmeasured) source raises.
    """

    def test_explicit_zero_volume_is_accepted_and_yields_zero_fees(self) -> None:
        """An explicit measured-zero daily volume is valid and accrues zero fees."""
        config = LPBacktestConfig(
            strategy_type="lp",
            use_historical_volume=False,
            fee_tracking_enabled=True,
            explicit_pool_volume_usd_daily=Decimal("0"),  # measured zero, not absent
            explicit_pool_liquidity_usd=Decimal("2000000"),
        )
        adapter = LPBacktestAdapter(config)

        position = create_lp_position()
        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )

        # Must NOT raise: zero is a valid measured volume.
        adapter.update_position(position, market, elapsed_seconds=86400)

        # Zero volume -> zero fees, but the source is trusted (HIGH), not "low".
        assert position.accumulated_fees_usd == Decimal("0")
        assert position.fee_confidence == "high"

    def test_negative_explicit_volume_is_rejected(self) -> None:
        """A negative explicit volume is nonsensical and must raise ValueError."""
        config = LPBacktestConfig(
            strategy_type="lp",
            use_historical_volume=False,
            fee_tracking_enabled=True,
            explicit_pool_volume_usd_daily=Decimal("-1"),
        )
        adapter = LPBacktestAdapter(config)

        position = create_lp_position()
        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )

        with pytest.raises(ValueError, match="must be >= 0"):
            adapter.update_position(position, market, elapsed_seconds=86400)

    def test_resolver_accepts_explicit_zero_without_raising(self) -> None:
        """Directly exercise the resolver: explicit zero -> 'explicit' source, no raise."""
        config = LPBacktestConfig(
            strategy_type="lp",
            use_historical_volume=False,
            explicit_pool_volume_usd_daily=Decimal("0"),
        )
        adapter = LPBacktestAdapter(config)
        position = create_lp_position()

        resolution = adapter._resolve_pool_volume(
            position=position,
            position_value_usd=Decimal("10000"),
            timestamp=None,
            pool_address=None,
            protocol="uniswap_v3",
        )

        assert resolution.source == "explicit"
        assert resolution.volume_usd == Decimal("0")

    def test_historical_measured_zero_volume_is_accepted(self) -> None:
        """A non-LOW-confidence measured-zero historical volume is used (zero fees)."""
        from almanak.framework.backtesting.pnl.types import DataConfidence

        config = LPBacktestConfig(
            strategy_type="lp",
            use_historical_volume=True,
            fee_tracking_enabled=True,
        )
        adapter = LPBacktestAdapter(config)

        # Subgraph genuinely observed zero volume that day (HIGH confidence).
        def _fake_volume(pool_address, timestamp, protocol=None):  # noqa: ANN001, ANN202
            return Decimal("0"), DataConfidence.HIGH

        adapter._get_historical_volume = _fake_volume  # type: ignore[method-assign]

        position = create_lp_position()
        position.metadata["pool_address"] = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"

        resolution = adapter._resolve_pool_volume(
            position=position,
            position_value_usd=Decimal("10000"),
            timestamp=datetime.now(),
            pool_address=position.metadata["pool_address"],
            protocol="uniswap_v3",
        )

        # Measured zero from the subgraph is used directly -- not a fall-through.
        assert resolution.source == "historical"
        assert resolution.volume_usd == Decimal("0")
        assert resolution.confidence == DataConfidence.HIGH

    def test_absent_historical_volume_still_raises(self) -> None:
        """An *absent* (None) historical volume + no opt-in must still raise."""
        from almanak.framework.backtesting.pnl.types import DataConfidence

        config = LPBacktestConfig(
            strategy_type="lp",
            use_historical_volume=True,
            fee_tracking_enabled=True,
        )
        adapter = LPBacktestAdapter(config)

        def _absent_volume(pool_address, timestamp, protocol=None):  # noqa: ANN001, ANN202
            return None, DataConfidence.LOW

        adapter._get_historical_volume = _absent_volume  # type: ignore[method-assign]

        position = create_lp_position()
        position.metadata["pool_address"] = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"

        with pytest.raises(DataSourceUnavailableError):
            adapter._resolve_pool_volume(
                position=position,
                position_value_usd=Decimal("10000"),
                timestamp=datetime.now(),
                pool_address=position.metadata["pool_address"],
                protocol="uniswap_v3",
            )


class TestVolumePolicyViaDataConfig:
    """BacktestDataConfig-driven volume policy (the CLI flag wiring path).

    `almanak backtest pnl` cannot construct an LPBacktestConfig (the adapter
    registry builds the adapter with defaults), so the volume honesty-guard
    knobs are also exposed on BacktestDataConfig. These tests pin the
    precedence contract: data_config wins when set, falls through to the
    adapter config when absent, and the refuse-to-fabricate default survives
    an otherwise-default data_config.
    """

    def test_explicit_volume_via_data_config_resolves_high_confidence(self) -> None:
        """data_config explicit volume is used directly: 'explicit' + HIGH."""
        from almanak.framework.backtesting.config import BacktestDataConfig
        from almanak.framework.backtesting.pnl.types import DataConfidence

        adapter = LPBacktestAdapter(
            config=LPBacktestConfig(strategy_type="lp", use_historical_volume=False),
            data_config=BacktestDataConfig(
                use_historical_volume=False,
                explicit_pool_volume_usd_daily=Decimal("5000000"),
            ),
        )
        position = create_lp_position()

        resolution = adapter._resolve_pool_volume(
            position=position,
            position_value_usd=Decimal("10000"),
            timestamp=None,
            pool_address=None,
            protocol="uniswap_v3",
        )

        assert resolution.source == "explicit"
        assert resolution.volume_usd == Decimal("5000000")
        assert resolution.confidence == DataConfidence.HIGH

    def test_data_config_explicit_volume_takes_precedence_over_adapter_config(self) -> None:
        """When both surfaces provide a volume, data_config wins."""
        from almanak.framework.backtesting.config import BacktestDataConfig

        adapter = LPBacktestAdapter(
            config=LPBacktestConfig(
                strategy_type="lp",
                use_historical_volume=False,
                explicit_pool_volume_usd_daily=Decimal("111"),
            ),
            data_config=BacktestDataConfig(
                use_historical_volume=False,
                explicit_pool_volume_usd_daily=Decimal("999"),
            ),
        )
        position = create_lp_position()

        resolution = adapter._resolve_pool_volume(
            position=position,
            position_value_usd=Decimal("10000"),
            timestamp=None,
            pool_address=None,
            protocol="uniswap_v3",
        )

        assert resolution.volume_usd == Decimal("999")

    def test_adapter_config_volume_survives_default_data_config(self) -> None:
        """A data_config that doesn't set a volume falls through to the adapter config."""
        from almanak.framework.backtesting.config import BacktestDataConfig

        adapter = LPBacktestAdapter(
            config=LPBacktestConfig(
                strategy_type="lp",
                use_historical_volume=False,
                explicit_pool_volume_usd_daily=Decimal("1234567"),
            ),
            data_config=BacktestDataConfig(use_historical_volume=False),
        )
        position = create_lp_position()

        resolution = adapter._resolve_pool_volume(
            position=position,
            position_value_usd=Decimal("10000"),
            timestamp=None,
            pool_address=None,
            protocol="uniswap_v3",
        )

        assert resolution.source == "explicit"
        assert resolution.volume_usd == Decimal("1234567")

    def test_data_config_allow_fallback_enables_heuristic(self) -> None:
        """data_config opt-in alone enables the LOW-confidence heuristic."""
        from almanak.framework.backtesting.config import BacktestDataConfig
        from almanak.framework.backtesting.pnl.types import DataConfidence

        adapter = LPBacktestAdapter(
            config=LPBacktestConfig(strategy_type="lp", use_historical_volume=False),
            data_config=BacktestDataConfig(
                use_historical_volume=False,
                allow_volume_fallback=True,
            ),
        )
        position = create_lp_position()

        resolution = adapter._resolve_pool_volume(
            position=position,
            position_value_usd=Decimal("10000"),
            timestamp=None,
            pool_address=None,
            protocol="uniswap_v3",
        )

        assert resolution.source == "fallback"
        assert resolution.confidence == DataConfidence.LOW
        # position_value * volume_fallback_multiplier (data_config default 10)
        assert resolution.volume_usd == Decimal("100000")

    def test_non_positive_fallback_multiplier_disables_heuristic(self) -> None:
        """A zero multiplier is not a measured volume; it disables fabricated fallback."""
        from almanak.framework.backtesting.config import BacktestDataConfig

        adapter = LPBacktestAdapter(
            config=LPBacktestConfig(strategy_type="lp", use_historical_volume=False),
            data_config=BacktestDataConfig(
                use_historical_volume=False,
                allow_volume_fallback=True,
                volume_fallback_multiplier=Decimal("0"),
            ),
        )
        position = create_lp_position()

        resolution = adapter._fallback_pool_volume_resolution(position, Decimal("10000"))

        assert resolution is None

    def test_default_data_config_does_not_revoke_adapter_optin(self) -> None:
        """OR semantics: a default data_config never withdraws a config-level opt-in."""
        from almanak.framework.backtesting.config import BacktestDataConfig

        adapter = LPBacktestAdapter(
            config=LPBacktestConfig(
                strategy_type="lp",
                use_historical_volume=False,
                allow_volume_fallback=True,
            ),
            data_config=BacktestDataConfig(use_historical_volume=False),
        )
        position = create_lp_position()

        resolution = adapter._resolve_pool_volume(
            position=position,
            position_value_usd=Decimal("10000"),
            timestamp=None,
            pool_address=None,
            protocol="uniswap_v3",
        )

        assert resolution.source == "fallback"

    def test_default_data_config_preserves_refuse_to_fabricate(self) -> None:
        """An otherwise-default data_config must not weaken the honesty guard."""
        from almanak.framework.backtesting.config import BacktestDataConfig

        adapter = LPBacktestAdapter(
            config=LPBacktestConfig(strategy_type="lp", use_historical_volume=False),
            data_config=BacktestDataConfig(use_historical_volume=False),
        )
        position = create_lp_position()

        with pytest.raises(DataSourceUnavailableError):
            adapter._resolve_pool_volume(
                position=position,
                position_value_usd=Decimal("10000"),
                timestamp=None,
                pool_address=None,
                protocol="uniswap_v3",
            )

    def test_data_config_explicit_liquidity_grounds_fee_accrual(self) -> None:
        """Explicit volume + TVL via data_config produce HIGH-confidence fees."""
        from almanak.framework.backtesting.config import BacktestDataConfig

        adapter = LPBacktestAdapter(
            config=LPBacktestConfig(
                strategy_type="lp",
                use_historical_volume=False,
                fee_tracking_enabled=True,
            ),
            data_config=BacktestDataConfig(
                use_historical_volume=False,
                explicit_pool_volume_usd_daily=Decimal("5000000"),
                explicit_pool_liquidity_usd=Decimal("2000000"),
            ),
        )
        position = create_lp_position()
        market = MockMarketStateWithTimestamp(
            prices={"ETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(),
        )

        adapter.update_position(position, market, elapsed_seconds=86400)

        assert position.accumulated_fees_usd > Decimal("0")
        assert position.fee_confidence == "high"

    def test_data_config_explicit_liquidity_takes_precedence(self) -> None:
        """The liquidity-share denominator prefers the data_config TVL."""
        from almanak.framework.backtesting.config import BacktestDataConfig

        adapter = LPBacktestAdapter(
            config=LPBacktestConfig(
                strategy_type="lp",
                explicit_pool_liquidity_usd=Decimal("111"),
            ),
            data_config=BacktestDataConfig(explicit_pool_liquidity_usd=Decimal("999")),
        )

        assert adapter._explicit_pool_liquidity_usd() == Decimal("999")

    def test_estimate_heuristic_fees_honors_data_config_liquidity(self) -> None:
        """The heuristic-validation path uses the same TVL source as accrual.

        Regression for the VIB-5079 review: _estimate_heuristic_fees read pool
        TVL straight off self._config, ignoring data_config precedence, so
        validate_heuristics could score against a different liquidity-share
        model than the fees the engine actually accrues at runtime.
        """
        from almanak.framework.backtesting.config import BacktestDataConfig

        sample = HeuristicValidationSample(
            position_value_usd=Decimal("10000"),
            liquidity=Decimal("1000000"),
            fee_tier=Decimal("0.003"),
            elapsed_seconds=86400,
            observed_fees_usd=Decimal("0"),
            label="data-config-tvl",
        )

        # data_config TVL ($5M) must win over the adapter-config TVL ($1k)...
        via_data_config = LPBacktestAdapter(
            config=LPBacktestConfig(strategy_type="lp", explicit_pool_liquidity_usd=Decimal("1000")),
            data_config=BacktestDataConfig(explicit_pool_liquidity_usd=Decimal("5000000")),
        )._estimate_heuristic_fees(sample)

        # ...so the estimate equals a config-only adapter using that same $5M TVL...
        via_config_only = LPBacktestAdapter(
            config=LPBacktestConfig(strategy_type="lp", explicit_pool_liquidity_usd=Decimal("5000000")),
        )._estimate_heuristic_fees(sample)

        # ...and differs from one that (wrongly) used the ignored $1k config TVL.
        via_small_tvl = LPBacktestAdapter(
            config=LPBacktestConfig(strategy_type="lp", explicit_pool_liquidity_usd=Decimal("1000")),
        )._estimate_heuristic_fees(sample)

        assert via_data_config == via_config_only
        assert via_data_config != via_small_tvl


# =============================================================================
# Historical volume helper decomposition
# =============================================================================


class StubVolumeProvider:
    """Async volume provider stub that records calls and returns canned data."""

    def __init__(
        self,
        results: "list | None" = None,
        error: Exception | None = None,
    ) -> None:
        self.results = results if results is not None else []
        self.error = error
        self.calls: list[dict] = []

    async def get_volume(self, **kwargs: object) -> list:
        self.calls.append(dict(kwargs))
        if self.error is not None:
            raise self.error
        return self.results


def make_volume_adapter(
    strict: bool = False,
    chain: str = "ethereum",
    provider: "StubVolumeProvider | None" = None,
) -> LPBacktestAdapter:
    """Build an adapter wired for historical-volume tests."""
    from almanak.framework.backtesting.config import BacktestDataConfig

    return LPBacktestAdapter(
        config=LPBacktestConfig(strategy_type="lp", use_historical_volume=True, chain=chain),
        data_config=BacktestDataConfig(strict_historical_mode=strict),
        volume_provider=provider,
    )


class TestVolumeUnavailableHelper:
    """The strict-raise-or-degrade fidelity contract in _volume_data_unavailable."""

    def test_non_strict_caches_and_invokes_fallback(self) -> None:
        """Non-strict mode logs (via on_fallback), caches (None, LOW), returns it."""
        from almanak.framework.backtesting.pnl.types import DataConfidence

        adapter = make_volume_adapter(strict=False)
        ts = datetime(2024, 1, 15, 12, 0, 0)
        key = ("0xpool", ts.date())
        fallback_calls: list[str] = []

        result = adapter._volume_data_unavailable(
            identifier="0xpool",
            timestamp=ts,
            message="lookup failed",
            chain="ethereum",
            protocol="uniswap_v3",
            cache_key=key,
            on_fallback=lambda: fallback_calls.append("logged"),
        )

        assert result == (None, DataConfidence.LOW)
        assert adapter._volume_cache[key] == (None, DataConfidence.LOW)
        assert fallback_calls == ["logged"]

    def test_non_strict_without_cache_key_skips_cache(self) -> None:
        """The early-exit sites (no pool, no provider) must not write the cache."""
        from almanak.framework.backtesting.pnl.types import DataConfidence

        adapter = make_volume_adapter(strict=False)

        result = adapter._volume_data_unavailable(
            identifier="0xpool",
            timestamp=datetime(2024, 1, 15),
            message="lookup failed",
            chain="ethereum",
            protocol=None,
        )

        assert result == (None, DataConfidence.LOW)
        assert adapter._volume_cache == {}

    def test_strict_raises_with_fields_and_no_side_effects(self) -> None:
        """Strict mode raises with all fields set; no cache write, no fallback."""
        from almanak.framework.backtesting.exceptions import HistoricalDataUnavailableError

        adapter = make_volume_adapter(strict=True)
        ts = datetime(2024, 1, 15, 12, 0, 0)
        fallback_calls: list[str] = []

        with pytest.raises(HistoricalDataUnavailableError) as exc_info:
            adapter._volume_data_unavailable(
                identifier="0xpool",
                timestamp=ts,
                message="lookup failed",
                chain="ethereum",
                protocol="uniswap_v3",
                cache_key=("0xpool", ts.date()),
                on_fallback=lambda: fallback_calls.append("logged"),
            )

        err = exc_info.value
        assert err.data_type == "volume"
        assert err.identifier == "0xpool"
        assert err.timestamp == ts
        assert err.message == "lookup failed"
        assert err.chain == "ethereum"
        assert err.protocol == "uniswap_v3"
        assert adapter._volume_cache == {}
        assert fallback_calls == []

    def test_strict_chaining_modes(self) -> None:
        """Default raises bare; cause=None suppresses context; cause=e chains it."""
        from almanak.framework.backtesting.exceptions import HistoricalDataUnavailableError

        adapter = make_volume_adapter(strict=True)
        common: dict = {
            "identifier": "0xpool",
            "timestamp": datetime(2024, 1, 15),
            "message": "lookup failed",
            "chain": "ethereum",
            "protocol": None,
        }

        with pytest.raises(HistoricalDataUnavailableError) as plain:
            adapter._volume_data_unavailable(**common)
        assert plain.value.__cause__ is None
        assert plain.value.__suppress_context__ is False

        with pytest.raises(HistoricalDataUnavailableError) as from_none:
            adapter._volume_data_unavailable(**common, cause=None)
        assert from_none.value.__cause__ is None
        assert from_none.value.__suppress_context__ is True

        root = RuntimeError("root cause")
        with pytest.raises(HistoricalDataUnavailableError) as chained:
            adapter._volume_data_unavailable(**common, cause=root)
        assert chained.value.__cause__ is root


class TestResolveVolumeChain:
    """Config-string to canonical chain name resolution for the volume lane."""

    def test_known_chain_returns_enum(self) -> None:
        adapter = make_volume_adapter(chain="ethereum")
        key = ("0xpool", datetime(2024, 1, 15).date())

        assert adapter._resolve_volume_chain(datetime(2024, 1, 15), None, key) == "ethereum"
        assert adapter._volume_cache == {}

    def test_unknown_chain_non_strict_warns_and_caches_low(self, caplog: pytest.LogCaptureFixture) -> None:
        import logging

        from almanak.framework.backtesting.pnl.types import DataConfidence

        adapter = make_volume_adapter(chain="notachain")
        ts = datetime(2024, 1, 15)
        key = ("0xpool", ts.date())

        with caplog.at_level(logging.WARNING, logger="almanak.framework.backtesting.adapters.lp_adapter"):
            result = adapter._resolve_volume_chain(ts, None, key)

        assert result is None
        assert adapter._volume_cache[key] == (None, DataConfidence.LOW)
        assert any("Unknown chain 'notachain'" in record.getMessage() for record in caplog.records)

    def test_unknown_chain_strict_raises_with_suppressed_keyerror_context(self) -> None:
        from almanak.framework.backtesting.exceptions import HistoricalDataUnavailableError

        adapter = make_volume_adapter(strict=True, chain="notachain")
        ts = datetime(2024, 1, 15)

        with pytest.raises(HistoricalDataUnavailableError) as exc_info:
            adapter._resolve_volume_chain(ts, "uniswap_v3", ("0xpool", ts.date()))

        err = exc_info.value
        assert err.chain == "notachain"
        assert err.identifier == "0xpool"
        assert err.protocol == "uniswap_v3"
        # `cause=None` still raises `from None` (suppressed context); the
        # registry lookup is a non-raising `try_resolve`, so no in-flight
        # KeyError exists anymore.
        assert err.__suppress_context__ is True
        assert err.__context__ is None
        assert adapter._volume_cache == {}


class TestCacheVolumeSuccess:
    """Result unpacking, cache write, and source logging for successful lookups."""

    def test_stores_logs_and_returns(self, caplog: pytest.LogCaptureFixture) -> None:
        import logging
        from datetime import UTC

        from almanak.framework.backtesting.pnl.types import (
            DataConfidence,
            DataSourceInfo,
            VolumeResult,
        )

        adapter = make_volume_adapter()
        key = ("0xabcdef123456", datetime(2024, 1, 15).date())
        result = VolumeResult(
            value=Decimal("1500000"),
            source_info=DataSourceInfo(
                source="gateway_dex_volume",
                confidence=DataConfidence.HIGH,
                timestamp=datetime(2024, 1, 15, tzinfo=UTC),
            ),
        )

        with caplog.at_level(logging.DEBUG, logger="almanak.framework.backtesting.adapters.lp_adapter"):
            volume, confidence = adapter._cache_volume_success(key, result)

        assert (volume, confidence) == (Decimal("1500000"), DataConfidence.HIGH)
        assert adapter._volume_cache[key] == (Decimal("1500000"), DataConfidence.HIGH)
        log_line = next(r.getMessage() for r in caplog.records if "Fetched historical volume" in r.getMessage())
        assert "0xabcdef12" in log_line
        assert "gateway_dex_volume" in log_line


class TestGetHistoricalVolumeOrchestration:
    """End-to-end behaviour of _get_historical_volume through the helpers."""

    @staticmethod
    def _stub_result() -> "object":
        from datetime import UTC

        from almanak.framework.backtesting.pnl.types import (
            DataConfidence,
            DataSourceInfo,
            VolumeResult,
        )

        return VolumeResult(
            value=Decimal("1500000"),
            source_info=DataSourceInfo(
                source="gateway_dex_volume",
                confidence=DataConfidence.HIGH,
                timestamp=datetime(2024, 1, 15, tzinfo=UTC),
            ),
        )

    def test_success_via_stub_provider(self) -> None:
        from almanak.framework.backtesting.pnl.types import DataConfidence

        stub = StubVolumeProvider(results=[self._stub_result()])
        adapter = make_volume_adapter(provider=stub)
        ts = datetime(2024, 1, 15, 12, 0, 0)

        volume, confidence = adapter._get_historical_volume("0xPOOL", ts, protocol="uniswap_v3")

        assert volume == Decimal("1500000")
        assert confidence is DataConfidence.HIGH
        assert adapter._volume_cache[("0xpool", ts.date())] == (volume, confidence)
        assert stub.calls == [
            {
                "pool_address": "0xpool",
                "chain": "ethereum",
                "start_date": ts.date(),
                "end_date": ts.date(),
                "protocol": "uniswap_v3",
            }
        ]

        # Second lookup is served from the cache without another provider call.
        assert adapter._get_historical_volume("0xPOOL", ts, protocol="uniswap_v3") == (volume, confidence)
        assert len(stub.calls) == 1

    def test_empty_results_non_strict_caches_low(self) -> None:
        from almanak.framework.backtesting.pnl.types import DataConfidence

        stub = StubVolumeProvider(results=[])
        adapter = make_volume_adapter(provider=stub)
        ts = datetime(2024, 1, 15)

        assert adapter._get_historical_volume("0xpool", ts) == (None, DataConfidence.LOW)
        assert adapter._volume_cache[("0xpool", ts.date())] == (None, DataConfidence.LOW)

    def test_empty_results_strict_raises_gateway_lane_message(self) -> None:
        from almanak.framework.backtesting.exceptions import HistoricalDataUnavailableError

        stub = StubVolumeProvider(results=[])
        adapter = make_volume_adapter(strict=True, provider=stub)

        with pytest.raises(HistoricalDataUnavailableError) as exc_info:
            adapter._get_historical_volume("0xpool", datetime(2024, 1, 15))

        assert exc_info.value.message == (
            "No historical volume data returned from the gateway DEX-volume lane (GetDexVolumeHistory)"
        )
        assert adapter._volume_cache == {}

    def test_provider_error_non_strict_retryable_ladder_miss_stays_uncached(self) -> None:
        from almanak.framework.backtesting.pnl.types import DataConfidence

        stub = StubVolumeProvider(error=RuntimeError("provider down"))
        adapter = make_volume_adapter(provider=stub)
        ts = datetime(2024, 1, 15)

        assert adapter._get_historical_volume("0xpool", ts) == (None, DataConfidence.LOW)
        assert ("0xpool", ts.date()) not in adapter._volume_cache

    def test_provider_error_strict_chains_cause(self) -> None:
        from almanak.framework.backtesting.exceptions import HistoricalDataUnavailableError

        stub = StubVolumeProvider(error=RuntimeError("provider down"))
        adapter = make_volume_adapter(strict=True, provider=stub)

        with pytest.raises(HistoricalDataUnavailableError) as exc_info:
            adapter._get_historical_volume("0xpool", datetime(2024, 1, 15))

        assert exc_info.value.message == "Failed to fetch historical volume: provider down"
        assert exc_info.value.__cause__ is stub.error
        assert adapter._volume_cache == {}

    def test_refuses_to_block_inside_async_task_non_strict(self) -> None:
        import asyncio

        from almanak.framework.backtesting.pnl.types import DataConfidence

        stub = StubVolumeProvider(results=[self._stub_result()])
        adapter = make_volume_adapter(provider=stub)
        ts = datetime(2024, 1, 15)

        async def lookup() -> tuple:
            return adapter._get_historical_volume("0xpool", ts)

        assert asyncio.run(lookup()) == (None, DataConfidence.LOW)
        assert adapter._volume_cache[("0xpool", ts.date())] == (None, DataConfidence.LOW)
        assert stub.calls == []

    def test_no_provider_ladder_refuses_to_block_inside_async_task(self, monkeypatch) -> None:
        # CodeRabbit #3283: the no-primary-provider branch dials the ladder via a
        # BLOCKING daily_history() call — inside the engine's async task it must
        # refuse to block (not call the blocking ladder), mirroring
        # _fetch_and_cache_volume's guard.
        import asyncio

        from almanak.framework.backtesting.pnl.types import DataConfidence

        adapter = LPBacktestAdapter()  # use_historical_volume defaults True
        adapter._volume_provider = None  # no primary provider
        adapter._volume_provider_initialized = True
        called: list[int] = []
        monkeypatch.setattr(adapter, "_pool_history_ladder_volume", lambda *a, **k: (called.append(1), None)[1])
        ts = datetime(2024, 1, 15)

        async def lookup() -> tuple:
            return adapter._get_historical_volume("0xpool", ts)

        assert asyncio.run(lookup()) == (None, DataConfidence.LOW)
        assert called == []  # the blocking ladder was NOT dialed inside the loop

    def test_refuses_to_block_inside_async_task_strict(self) -> None:
        import asyncio

        from almanak.framework.backtesting.exceptions import HistoricalDataUnavailableError

        stub = StubVolumeProvider(results=[self._stub_result()])
        adapter = make_volume_adapter(strict=True, provider=stub)

        async def lookup() -> tuple:
            return adapter._get_historical_volume("0xpool", datetime(2024, 1, 15))

        with pytest.raises(HistoricalDataUnavailableError) as exc_info:
            asyncio.run(lookup())

        assert exc_info.value.message == "Cannot fetch historical volume in async context"
        assert adapter._volume_cache == {}
        assert stub.calls == []

    def test_missing_pool_address_non_strict(self) -> None:
        from almanak.framework.backtesting.pnl.types import DataConfidence

        adapter = make_volume_adapter()

        assert adapter._get_historical_volume(None, datetime(2024, 1, 15)) == (None, DataConfidence.LOW)
        assert adapter._volume_cache == {}

    def test_missing_pool_address_strict_uses_unknown_identifier(self) -> None:
        from almanak.framework.backtesting.exceptions import HistoricalDataUnavailableError

        adapter = make_volume_adapter(strict=True)

        with pytest.raises(HistoricalDataUnavailableError) as exc_info:
            adapter._get_historical_volume(None, datetime(2024, 1, 15))

        assert exc_info.value.identifier == "unknown"
        assert adapter._volume_cache == {}

    def test_unavailable_provider_non_strict_skips_cache(self) -> None:
        from almanak.framework.backtesting.config import BacktestDataConfig
        from almanak.framework.backtesting.pnl.types import DataConfidence

        adapter = LPBacktestAdapter(
            config=LPBacktestConfig(strategy_type="lp", use_historical_volume=True),
            data_config=BacktestDataConfig(use_historical_volume=False),
        )

        assert adapter._get_historical_volume("0xpool", datetime(2024, 1, 15)) == (None, DataConfidence.LOW)
        assert adapter._volume_cache == {}

    def test_unavailable_provider_strict_keeps_original_case_identifier(self) -> None:
        from almanak.framework.backtesting.config import BacktestDataConfig
        from almanak.framework.backtesting.exceptions import HistoricalDataUnavailableError

        adapter = LPBacktestAdapter(
            config=LPBacktestConfig(strategy_type="lp", use_historical_volume=True),
            data_config=BacktestDataConfig(strict_historical_mode=True, use_historical_volume=False),
        )

        with pytest.raises(HistoricalDataUnavailableError) as exc_info:
            adapter._get_historical_volume("0xPOOL", datetime(2024, 1, 15))

        assert exc_info.value.identifier == "0xPOOL"
        assert adapter._volume_cache == {}

    def test_unknown_chain_returns_low_without_provider_call(self) -> None:
        from almanak.framework.backtesting.pnl.types import DataConfidence

        stub = StubVolumeProvider(results=[self._stub_result()])
        adapter = make_volume_adapter(chain="notachain", provider=stub)

        assert adapter._get_historical_volume("0xpool", datetime(2024, 1, 15)) == (None, DataConfidence.LOW)
        assert stub.calls == []


# =============================================================================
# Historical liquidity helper decomposition
# =============================================================================


class StubLiquidityProvider:
    """Async liquidity provider stub that records calls and returns canned data."""

    def __init__(
        self,
        result: "object | None" = None,
        error: Exception | None = None,
    ) -> None:
        self.result = result
        self.error = error
        self.calls: list[dict] = []

    async def get_liquidity_depth(self, **kwargs: object) -> "object":
        self.calls.append(dict(kwargs))
        if self.error is not None:
            raise self.error
        return self.result


def make_liquidity_adapter(
    strict: bool = False,
    chain: str = "ethereum",
    provider: "StubLiquidityProvider | None" = None,
) -> LPBacktestAdapter:
    """Build an adapter wired for historical-liquidity tests."""
    from almanak.framework.backtesting.config import BacktestDataConfig

    return LPBacktestAdapter(
        config=LPBacktestConfig(strategy_type="lp", chain=chain),
        data_config=BacktestDataConfig(strict_historical_mode=strict, use_historical_liquidity=True),
        liquidity_provider=provider,
    )


def make_liquidity_result(depth: Decimal, confidence: "object | None" = None) -> "object":
    """Build a LiquidityResult stamped with a subgraph source."""
    from datetime import UTC

    from almanak.framework.backtesting.pnl.types import (
        DataConfidence,
        DataSourceInfo,
        LiquidityResult,
    )

    return LiquidityResult(
        depth=depth,
        source_info=DataSourceInfo(
            source="uniswap_v3_subgraph",
            confidence=confidence if confidence is not None else DataConfidence.HIGH,
            timestamp=datetime(2024, 1, 15, tzinfo=UTC),
        ),
    )


class TestLiquidityUnavailableHelper:
    """The strict-raise-or-degrade fidelity contract in _liquidity_data_unavailable."""

    def test_non_strict_returns_none_and_invokes_fallback(self) -> None:
        """Non-strict mode logs (via on_fallback) and degrades without caching."""
        adapter = make_liquidity_adapter(strict=False)
        fallback_calls: list[str] = []

        result = adapter._liquidity_data_unavailable(
            identifier="0xpool",
            timestamp=datetime(2024, 1, 15, 12, 0, 0),
            message="lookup failed",
            chain="ethereum",
            protocol="uniswap_v3",
            on_fallback=lambda: fallback_calls.append("logged"),
        )

        assert result is None
        assert adapter._liquidity_cache == {}
        assert fallback_calls == ["logged"]

    def test_strict_raises_with_fields_and_no_side_effects(self) -> None:
        """Strict mode raises with all fields set; no cache write, no fallback."""
        from almanak.framework.backtesting.exceptions import HistoricalDataUnavailableError

        adapter = make_liquidity_adapter(strict=True)
        ts = datetime(2024, 1, 15, 12, 0, 0)
        fallback_calls: list[str] = []

        with pytest.raises(HistoricalDataUnavailableError) as exc_info:
            adapter._liquidity_data_unavailable(
                identifier="0xpool",
                timestamp=ts,
                message="lookup failed",
                chain="ethereum",
                protocol="uniswap_v3",
                on_fallback=lambda: fallback_calls.append("logged"),
            )

        err = exc_info.value
        assert err.data_type == "liquidity"
        assert err.identifier == "0xpool"
        assert err.timestamp == ts
        assert err.message == "lookup failed"
        assert err.chain == "ethereum"
        assert err.protocol == "uniswap_v3"
        assert adapter._liquidity_cache == {}
        assert fallback_calls == []

    def test_strict_chaining_modes(self) -> None:
        """Default raises bare; cause=None suppresses context; cause=e chains it."""
        from almanak.framework.backtesting.exceptions import HistoricalDataUnavailableError

        adapter = make_liquidity_adapter(strict=True)
        common: dict = {
            "identifier": "0xpool",
            "timestamp": datetime(2024, 1, 15),
            "message": "lookup failed",
            "chain": "ethereum",
            "protocol": None,
        }

        with pytest.raises(HistoricalDataUnavailableError) as plain:
            adapter._liquidity_data_unavailable(**common)
        assert plain.value.__cause__ is None
        assert plain.value.__suppress_context__ is False

        with pytest.raises(HistoricalDataUnavailableError) as from_none:
            adapter._liquidity_data_unavailable(**common, cause=None)
        assert from_none.value.__cause__ is None
        assert from_none.value.__suppress_context__ is True

        root = RuntimeError("root cause")
        with pytest.raises(HistoricalDataUnavailableError) as chained:
            adapter._liquidity_data_unavailable(**common, cause=root)
        assert chained.value.__cause__ is root


class TestResolveLiquidityChain:
    """Config-string to canonical chain name resolution for the liquidity lane."""

    def test_known_chain_returns_enum(self) -> None:
        adapter = make_liquidity_adapter(chain="ethereum")

        assert adapter._resolve_liquidity_chain(datetime(2024, 1, 15), None, "0xpool") == "ethereum"
        assert adapter._liquidity_cache == {}

    def test_unknown_chain_non_strict_warns_without_caching(self, caplog: pytest.LogCaptureFixture) -> None:
        """The liquidity lane never caches failures -- unlike the volume lane."""
        import logging

        adapter = make_liquidity_adapter(chain="notachain")

        with caplog.at_level(logging.WARNING, logger="almanak.framework.backtesting.adapters.lp_adapter"):
            result = adapter._resolve_liquidity_chain(datetime(2024, 1, 15), None, "0xpool")

        assert result is None
        assert adapter._liquidity_cache == {}
        assert any("Unknown chain 'notachain'" in record.getMessage() for record in caplog.records)

    def test_unknown_chain_strict_raises_with_suppressed_keyerror_context(self) -> None:
        from almanak.framework.backtesting.exceptions import HistoricalDataUnavailableError

        adapter = make_liquidity_adapter(strict=True, chain="notachain")

        with pytest.raises(HistoricalDataUnavailableError) as exc_info:
            adapter._resolve_liquidity_chain(datetime(2024, 1, 15), "uniswap_v3", "0xpool")

        err = exc_info.value
        assert err.chain == "notachain"
        assert err.identifier == "0xpool"
        assert err.protocol == "uniswap_v3"
        # `cause=None` still raises `from None` (suppressed context); the
        # registry lookup is a non-raising `try_resolve`, so no in-flight
        # KeyError exists anymore.
        assert err.__suppress_context__ is True
        assert err.__context__ is None
        assert adapter._liquidity_cache == {}


class TestCacheLiquiditySuccess:
    """Cache write and source logging for successful liquidity lookups."""

    def test_stores_logs_and_returns_same_object(self, caplog: pytest.LogCaptureFixture) -> None:
        import logging

        adapter = make_liquidity_adapter()
        key = ("0xabcdef123456", datetime(2024, 1, 15).date())
        result = make_liquidity_result(Decimal("50000000"))

        with caplog.at_level(logging.DEBUG, logger="almanak.framework.backtesting.adapters.lp_adapter"):
            returned = adapter._cache_liquidity_success(key, result)

        assert returned is result
        assert adapter._liquidity_cache[key] is result
        log_line = next(r.getMessage() for r in caplog.records if "Fetched historical liquidity" in r.getMessage())
        assert "0xabcdef12" in log_line
        assert "uniswap_v3_subgraph" in log_line


class TestGetHistoricalLiquidityOrchestration:
    """End-to-end behaviour of _get_historical_liquidity through the helpers."""

    def test_success_via_stub_provider(self) -> None:
        stub = StubLiquidityProvider(result=make_liquidity_result(Decimal("50000000")))
        adapter = make_liquidity_adapter(provider=stub)
        ts = datetime(2024, 1, 15, 12, 0, 0)

        result = adapter._get_historical_liquidity("0xPOOL", ts, protocol="uniswap_v3")

        assert result is stub.result
        assert adapter._liquidity_cache[("0xpool", ts.date())] is stub.result
        assert stub.calls == [
            {
                "pool_address": "0xpool",
                "chain": "ethereum",
                "timestamp": ts,
                "protocol": "uniswap_v3",
            }
        ]

        # Second lookup is served from the cache without another provider call.
        assert adapter._get_historical_liquidity("0xPOOL", ts, protocol="uniswap_v3") is stub.result
        assert len(stub.calls) == 1

    def test_low_confidence_fallthrough_non_strict_caches_and_returns(self) -> None:
        """A zero-depth LOW result is cached and returned like a success."""
        from almanak.framework.backtesting.pnl.types import DataConfidence

        stub = StubLiquidityProvider(result=make_liquidity_result(Decimal("0"), DataConfidence.LOW))
        adapter = make_liquidity_adapter(provider=stub)
        ts = datetime(2024, 1, 15)

        result = adapter._get_historical_liquidity("0xpool", ts)

        assert result is stub.result
        assert adapter._liquidity_cache[("0xpool", ts.date())] is stub.result

    def test_low_confidence_strict_raises_subgraph_worded_message(self) -> None:
        from almanak.framework.backtesting.exceptions import HistoricalDataUnavailableError
        from almanak.framework.backtesting.pnl.types import DataConfidence

        stub = StubLiquidityProvider(result=make_liquidity_result(Decimal("0"), DataConfidence.LOW))
        adapter = make_liquidity_adapter(strict=True, provider=stub)

        with pytest.raises(HistoricalDataUnavailableError) as exc_info:
            adapter._get_historical_liquidity("0xpool", datetime(2024, 1, 15))

        assert exc_info.value.message == ("No historical liquidity data available (returned low-confidence fallback)")
        assert exc_info.value.__cause__ is None
        assert adapter._liquidity_cache == {}

    def test_provider_error_non_strict_returns_none_without_caching(self) -> None:
        stub = StubLiquidityProvider(error=RuntimeError("provider down"))
        adapter = make_liquidity_adapter(provider=stub)
        ts = datetime(2024, 1, 15)

        assert adapter._get_historical_liquidity("0xpool", ts) is None
        assert adapter._liquidity_cache == {}

    def test_provider_error_strict_chains_cause(self) -> None:
        from almanak.framework.backtesting.exceptions import HistoricalDataUnavailableError

        stub = StubLiquidityProvider(error=RuntimeError("provider down"))
        adapter = make_liquidity_adapter(strict=True, provider=stub)

        with pytest.raises(HistoricalDataUnavailableError) as exc_info:
            adapter._get_historical_liquidity("0xpool", datetime(2024, 1, 15))

        assert exc_info.value.message == "Failed to fetch historical liquidity: provider down"
        assert exc_info.value.__cause__ is stub.error
        assert adapter._liquidity_cache == {}

    def test_refuses_to_block_inside_async_task_non_strict(self) -> None:
        import asyncio

        stub = StubLiquidityProvider(result=make_liquidity_result(Decimal("50000000")))
        adapter = make_liquidity_adapter(provider=stub)

        async def lookup() -> "object":
            return adapter._get_historical_liquidity("0xpool", datetime(2024, 1, 15))

        assert asyncio.run(lookup()) is None
        assert adapter._liquidity_cache == {}
        assert stub.calls == []

    def test_refuses_to_block_inside_async_task_strict(self) -> None:
        import asyncio

        from almanak.framework.backtesting.exceptions import HistoricalDataUnavailableError

        stub = StubLiquidityProvider(result=make_liquidity_result(Decimal("50000000")))
        adapter = make_liquidity_adapter(strict=True, provider=stub)

        async def lookup() -> "object":
            return adapter._get_historical_liquidity("0xpool", datetime(2024, 1, 15))

        with pytest.raises(HistoricalDataUnavailableError) as exc_info:
            asyncio.run(lookup())

        assert exc_info.value.message == "Cannot fetch historical liquidity in async context"
        assert adapter._liquidity_cache == {}
        assert stub.calls == []

    def test_missing_pool_address_non_strict(self) -> None:
        adapter = make_liquidity_adapter()

        assert adapter._get_historical_liquidity(None, datetime(2024, 1, 15)) is None
        assert adapter._liquidity_cache == {}

    def test_missing_pool_address_strict_uses_unknown_identifier(self) -> None:
        from almanak.framework.backtesting.exceptions import HistoricalDataUnavailableError

        adapter = make_liquidity_adapter(strict=True)

        with pytest.raises(HistoricalDataUnavailableError) as exc_info:
            adapter._get_historical_liquidity(None, datetime(2024, 1, 15))

        assert exc_info.value.identifier == "unknown"
        assert adapter._liquidity_cache == {}

    def test_unavailable_provider_non_strict_skips_cache(self) -> None:
        from almanak.framework.backtesting.config import BacktestDataConfig

        adapter = LPBacktestAdapter(
            config=LPBacktestConfig(strategy_type="lp"),
            data_config=BacktestDataConfig(use_historical_liquidity=False),
        )

        assert adapter._get_historical_liquidity("0xpool", datetime(2024, 1, 15)) is None
        assert adapter._liquidity_cache == {}

    def test_unavailable_provider_strict_keeps_original_case_identifier(self) -> None:
        from almanak.framework.backtesting.config import BacktestDataConfig
        from almanak.framework.backtesting.exceptions import HistoricalDataUnavailableError

        adapter = LPBacktestAdapter(
            config=LPBacktestConfig(strategy_type="lp"),
            data_config=BacktestDataConfig(strict_historical_mode=True, use_historical_liquidity=False),
        )

        with pytest.raises(HistoricalDataUnavailableError) as exc_info:
            adapter._get_historical_liquidity("0xPOOL", datetime(2024, 1, 15))

        assert exc_info.value.identifier == "0xPOOL"
        assert adapter._liquidity_cache == {}

    def test_unknown_chain_returns_none_without_provider_call(self) -> None:
        stub = StubLiquidityProvider(result=make_liquidity_result(Decimal("50000000")))
        adapter = make_liquidity_adapter(chain="notachain", provider=stub)

        assert adapter._get_historical_liquidity("0xpool", datetime(2024, 1, 15)) is None
        assert adapter._liquidity_cache == {}
        assert stub.calls == []


class TestPrewarmHistory:
    """prewarm_history populates the sync-read caches before the sim loop reads them (ALM-2930 #4)."""

    @pytest.mark.asyncio
    async def test_prewarm_populates_volume_and_liquidity_caches(self):
        from datetime import UTC, date, datetime
        from decimal import Decimal
        from types import SimpleNamespace
        from unittest.mock import AsyncMock

        from almanak.framework.backtesting.adapters.lp_adapter import LPBacktestAdapter
        from almanak.framework.backtesting.pnl.types import (
            DataConfidence,
            DataSourceInfo,
            LiquidityResult,
            VolumeResult,
        )

        adapter = LPBacktestAdapter()

        def volume_row(day: int) -> VolumeResult:
            return VolumeResult(
                value=Decimal("1000000"),
                source_info=DataSourceInfo(
                    source="test",
                    confidence=DataConfidence.HIGH,
                    timestamp=datetime(2026, 6, day, tzinfo=UTC),
                ),
            )

        source = DataSourceInfo(
            source="test", confidence=DataConfidence.HIGH, timestamp=datetime(2026, 6, 20, tzinfo=UTC)
        )
        volume_provider = SimpleNamespace(
            get_volume=AsyncMock(return_value=[volume_row(20), volume_row(21), volume_row(22)])
        )
        liquidity_provider = SimpleNamespace(
            get_liquidity_depth=AsyncMock(return_value=LiquidityResult(depth=Decimal("5000000"), source_info=source))
        )
        adapter._volume_provider = volume_provider
        adapter._volume_provider_initialized = True
        adapter._liquidity_provider = liquidity_provider
        adapter._liquidity_provider_initialized = True

        intent = SimpleNamespace(pool="0xAbCd000000000000000000000000000000000001", protocol="uniswap_v3")
        await adapter.prewarm_history(
            intent,
            chain="base",
            start_time=datetime(2026, 6, 20, tzinfo=UTC),
            end_time=datetime(2026, 6, 22, tzinfo=UTC),
        )

        pool = intent.pool.lower()
        assert (pool, date(2026, 6, 20)) in adapter._volume_cache
        assert (pool, date(2026, 6, 22)) in adapter._volume_cache
        assert adapter._volume_cache[(pool, date(2026, 6, 21))][0] == Decimal("1000000")
        assert (pool, date(2026, 6, 21)) in adapter._liquidity_cache
        assert volume_provider.get_volume.await_count == 1
        assert liquidity_provider.get_liquidity_depth.await_count == 3

    @pytest.mark.asyncio
    async def test_prewarm_volume_survives_a_transient_mid_window_error(self):
        # Regression: a single transient per-day fetch error must NOT abort the
        # rest of the window — later days still prewarm so accrual keeps a warm
        # cache instead of falling to the chain-default DEX miss path. Two
        # CONSECUTIVE errors is the sticky-abort threshold; one is not.
        from datetime import UTC, date, datetime
        from decimal import Decimal
        from types import SimpleNamespace
        from unittest.mock import AsyncMock

        from almanak.framework.backtesting.adapters.lp_adapter import LPBacktestAdapter
        from almanak.framework.backtesting.pnl.types import (
            DataConfidence,
            DataSourceInfo,
            VolumeResult,
        )

        adapter = LPBacktestAdapter()

        def ok(day: int) -> list[VolumeResult]:
            return [
                VolumeResult(
                    value=Decimal("1000000"),
                    source_info=DataSourceInfo(
                        source="test",
                        confidence=DataConfidence.HIGH,
                        timestamp=datetime(2026, 6, day, tzinfo=UTC),
                    ),
                )
            ]

        # Range fails, then day0 ok, day1 raises (isolated), day2 ok — day2
        # must still be warmed by the bounded per-day recovery path.
        volume_provider = SimpleNamespace(
            get_volume=AsyncMock(
                side_effect=[RuntimeError("range transient"), ok(20), RuntimeError("transient"), ok(22)]
            )
        )
        adapter._volume_provider = volume_provider
        adapter._volume_provider_initialized = True
        adapter._liquidity_provider = None
        adapter._liquidity_provider_initialized = True

        intent = SimpleNamespace(pool="0xAbCd000000000000000000000000000000000001", protocol="curve")
        await adapter.prewarm_history(
            intent,
            chain="ethereum",
            start_time=datetime(2026, 6, 20, tzinfo=UTC),
            end_time=datetime(2026, 6, 22, tzinfo=UTC),
        )

        pool = intent.pool.lower()
        assert volume_provider.get_volume.await_count == 4  # range attempt + all 3 per-day retries
        assert (pool, date(2026, 6, 20)) in adapter._volume_cache
        assert (pool, date(2026, 6, 22)) in adapter._volume_cache  # day AFTER the error still warmed
        assert (pool, date(2026, 6, 21)) not in adapter._volume_cache  # the erroring day is skipped

    @pytest.mark.asyncio
    async def test_prewarm_volume_aborts_after_two_consecutive_errors(self):
        # Two consecutive fetch errors = a sick lane; stop dialing it once per
        # day. A 4-day window that errors on day1+day2 must stop there (3 calls:
        # day0 ok, day1 err, day2 err -> abort), never reaching day3.
        from datetime import UTC, datetime
        from decimal import Decimal
        from types import SimpleNamespace
        from unittest.mock import AsyncMock

        from almanak.framework.backtesting.adapters.lp_adapter import LPBacktestAdapter
        from almanak.framework.backtesting.pnl.types import (
            DataConfidence,
            DataSourceInfo,
            VolumeResult,
        )

        adapter = LPBacktestAdapter()
        ok = [
            VolumeResult(
                value=Decimal("1000000"),
                source_info=DataSourceInfo(
                    source="test",
                    confidence=DataConfidence.HIGH,
                    timestamp=datetime(2026, 6, 20, tzinfo=UTC),
                ),
            )
        ]
        volume_provider = SimpleNamespace(
            get_volume=AsyncMock(
                side_effect=[RuntimeError("range down"), ok, RuntimeError("down"), RuntimeError("down")]
            )
        )
        adapter._volume_provider = volume_provider
        adapter._volume_provider_initialized = True
        adapter._liquidity_provider = None
        adapter._liquidity_provider_initialized = True

        intent = SimpleNamespace(pool="0xAbCd000000000000000000000000000000000001", protocol="curve")
        await adapter.prewarm_history(
            intent,
            chain="ethereum",
            start_time=datetime(2026, 6, 20, tzinfo=UTC),
            end_time=datetime(2026, 6, 23, tzinfo=UTC),  # 4 days
        )
        assert volume_provider.get_volume.await_count == 4  # range + day0/day1/day2; never dialed day3

    @pytest.mark.asyncio
    async def test_prewarm_liquidity_survives_a_transient_mid_window_error(self):
        # Regression (CodeRabbit #3271): the liquidity prewarm lane must handle
        # a transient per-day error like the volume lane — one exception must
        # NOT abort the rest of the window.
        from datetime import UTC, date, datetime
        from decimal import Decimal
        from types import SimpleNamespace
        from unittest.mock import AsyncMock

        from almanak.framework.backtesting.adapters.lp_adapter import LPBacktestAdapter
        from almanak.framework.backtesting.pnl.types import (
            DataConfidence,
            DataSourceInfo,
            LiquidityResult,
        )

        adapter = LPBacktestAdapter()
        source = DataSourceInfo(
            source="test", confidence=DataConfidence.HIGH, timestamp=datetime(2026, 6, 20, tzinfo=UTC)
        )
        ok = LiquidityResult(depth=Decimal("5000000"), source_info=source)
        # day0 ok, day1 raises (isolated), day2 ok — day2 must still be warmed.
        liquidity_provider = SimpleNamespace(
            get_liquidity_depth=AsyncMock(side_effect=[ok, RuntimeError("transient"), ok])
        )
        adapter._volume_provider = None
        adapter._volume_provider_initialized = True
        adapter._liquidity_provider = liquidity_provider
        adapter._liquidity_provider_initialized = True

        intent = SimpleNamespace(pool="0xAbCd000000000000000000000000000000000001", protocol="curve")
        await adapter.prewarm_history(
            intent,
            chain="ethereum",
            start_time=datetime(2026, 6, 20, tzinfo=UTC),
            end_time=datetime(2026, 6, 22, tzinfo=UTC),
        )

        pool = intent.pool.lower()
        assert liquidity_provider.get_liquidity_depth.await_count == 3  # NOT aborted after day1
        assert (pool, date(2026, 6, 20)) in adapter._liquidity_cache
        assert (pool, date(2026, 6, 22)) in adapter._liquidity_cache  # day AFTER the error still warmed
        assert (pool, date(2026, 6, 21)) not in adapter._liquidity_cache  # the erroring day is skipped

    @pytest.mark.asyncio
    async def test_prewarm_liquidity_aborts_after_two_consecutive_errors(self):
        # Two consecutive liquidity errors = a sick lane; stop dialing it.
        from datetime import UTC, datetime
        from decimal import Decimal
        from types import SimpleNamespace
        from unittest.mock import AsyncMock

        from almanak.framework.backtesting.adapters.lp_adapter import LPBacktestAdapter
        from almanak.framework.backtesting.pnl.types import (
            DataConfidence,
            DataSourceInfo,
            LiquidityResult,
        )

        adapter = LPBacktestAdapter()
        source = DataSourceInfo(
            source="test", confidence=DataConfidence.HIGH, timestamp=datetime(2026, 6, 20, tzinfo=UTC)
        )
        ok = LiquidityResult(depth=Decimal("5000000"), source_info=source)
        liquidity_provider = SimpleNamespace(
            get_liquidity_depth=AsyncMock(side_effect=[ok, RuntimeError("down"), RuntimeError("down"), ok])
        )
        adapter._volume_provider = None
        adapter._volume_provider_initialized = True
        adapter._liquidity_provider = liquidity_provider
        adapter._liquidity_provider_initialized = True

        intent = SimpleNamespace(pool="0xAbCd000000000000000000000000000000000001", protocol="curve")
        await adapter.prewarm_history(
            intent,
            chain="ethereum",
            start_time=datetime(2026, 6, 20, tzinfo=UTC),
            end_time=datetime(2026, 6, 23, tzinfo=UTC),  # 4 days
        )
        assert liquidity_provider.get_liquidity_depth.await_count == 3  # aborted, never dialed day3

    @pytest.mark.asyncio
    async def test_prewarm_symbolic_pool_resolves_via_dexscreener(self):
        """Symbolic pools resolve pair->address before warming."""
        from datetime import UTC, datetime
        from types import SimpleNamespace
        from unittest.mock import AsyncMock

        from almanak.framework.backtesting.adapters.lp_adapter import LPBacktestAdapter

        adapter = LPBacktestAdapter()
        adapter._resolve_symbolic_pool_address = AsyncMock(return_value="0xabc0000000000000000000000000000000000001")
        adapter._volume_provider = None
        adapter._volume_provider_initialized = True
        adapter._liquidity_provider = None
        adapter._liquidity_provider_initialized = True

        intent = SimpleNamespace(pool="WETH/USDC", protocol="uniswap_v3")
        await adapter.prewarm_history(
            intent,
            chain="base",
            start_time=datetime(2026, 6, 20, tzinfo=UTC),
            end_time=datetime(2026, 6, 22, tzinfo=UTC),
        )
        adapter._resolve_symbolic_pool_address.assert_awaited_once_with("WETH/USDC", "uniswap_v3", "base")

    @pytest.mark.asyncio
    async def test_prewarm_unresolvable_symbolic_pool_is_a_noop(self):
        from datetime import UTC, datetime
        from types import SimpleNamespace
        from unittest.mock import AsyncMock

        from almanak.framework.backtesting.adapters.lp_adapter import LPBacktestAdapter

        adapter = LPBacktestAdapter()
        adapter._resolve_symbolic_pool_address = AsyncMock(return_value=None)
        intent = SimpleNamespace(pool="WETH/USDC", protocol="uniswap_v3")
        await adapter.prewarm_history(
            intent,
            chain="base",
            start_time=datetime(2026, 6, 20, tzinfo=UTC),
            end_time=datetime(2026, 6, 22, tzinfo=UTC),
        )
        assert not adapter._volume_cache
        assert not adapter._liquidity_cache


class TestRangeGatingScope:
    """Fee range gating applies to concentrated-liquidity families only."""

    @staticmethod
    def _position(protocol: str, tick_lower: int = 0, tick_upper: int = 6931):
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition

        return SimulatedPosition.lp(
            token0="WETH",
            token1="USDT",
            amount0=Decimal("1"),
            amount1=Decimal("1765"),
            liquidity=Decimal("1000"),
            entry_price=Decimal("1765"),
            entry_time=datetime(2024, 1, 1),
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            fee_tier=Decimal("0.0004"),
            protocol=protocol,
        )

    def test_family_classification_from_connector_decls(self) -> None:
        # Invariant: position economics come from connector-owned
        # lp_economic_family declarations, covering aliases and forks;
        # unknown venues default to fungible (no gating) — wrongly zeroing
        # an earning position is worse than skipping the range refinement.
        adapter = LPBacktestAdapter()
        expected = {
            "uniswap_v3": True,
            "uniswap": True,
            "uniswap_v2": False,
            "agni_finance": True,
            "pancakeswap_v3": True,
            "sushiswap": True,
            "sushiswap_v3": True,
            "traderjoe_v2": True,
            "aerodrome_slipstream": True,
            "velodrome_slipstream": True,
            "aerodrome": False,
            "curve": False,
            "balancer": False,
            "raydium_clmm": True,
            "orca_whirlpools": True,
            "meteora_dlmm": True,
            "agni": True,
            "pendle": False,
            "fluid_dex_lp": False,
            "uniswap_v4": True,
            "unknown_dex": False,
        }
        for protocol, concentrated in expected.items():
            assert adapter._is_concentrated_position(self._position(protocol)) is concentrated, protocol

    def test_tick_upper_boundary_is_out_of_range(self) -> None:
        # V3 ranges are [lower, upper): price exactly at the upper tick is out.
        adapter = LPBacktestAdapter()
        position = self._position("uniswap_v3", tick_lower=0, tick_upper=4054)
        assert adapter._position_out_of_range(position, Decimal("1.5"), Decimal("1")) is True

    def test_out_of_range_zeroes_fees_without_touching_data_lanes(self, monkeypatch) -> None:
        # The range verdict is decided from prices alone — strict runs must not
        # raise over volume data an out-of-range position does not need.
        adapter = LPBacktestAdapter()
        position = self._position("uniswap_v3")  # in-range bounds 1..2, price ratio 1765 -> out

        def _boom(*_args, **_kwargs):
            raise AssertionError("no data lane may run for an out-of-range position")

        # Invariant: fees AND slippage AND the formula context (which
        # resolves volume) are all skipped — the range verdict needs prices only.
        monkeypatch.setattr(adapter, "_fee_amount_from_resolution", _boom)
        monkeypatch.setattr(adapter, "_fee_slippage_result", _boom)
        monkeypatch.setattr(adapter, "_fee_formula_context", _boom)
        result = adapter._calculate_fee_accrual(
            position=position,
            position_value_usd=Decimal("3530"),
            elapsed_seconds=3600,
            token0="WETH",
            token1="USDT",
            token0_price=Decimal("1765"),
            token1_price=Decimal("1"),
        )
        assert result.fees_usd == Decimal("0")
        assert result.data_source == "out_of_range"
        assert result.slippage_pct is None
        # None, not $0: no volume was measured — the zero fee is a range
        # verdict, and "measured zero volume" would read as a dead pool.
        assert result.volume_usd is None
        # Measured prices -> the verdict legitimately claims high confidence.
        assert result.fee_confidence == "high"

    def test_out_of_range_verdict_from_fallback_prices_is_not_high_confidence(self) -> None:
        # A range verdict computed from FALLBACK prices (entry-price / $1
        # substitutes for a missing market price) is not a measured verdict:
        # the zero fee stands, but it must not read as high confidence and
        # the provenance must name the fabricated input (CodeRabbit find,
        # #3271: a WETH/WBTC pool missing its token1 price computes a wildly
        # wrong ratio and could zero fees while claiming "high").
        adapter = LPBacktestAdapter()
        position = self._position("uniswap_v3")  # bounds 1..2, ratio 1765 -> out

        result = adapter._calculate_fee_accrual(
            position=position,
            position_value_usd=Decimal("3530"),
            elapsed_seconds=3600,
            token0="WETH",
            token1="USDT",
            token0_price=Decimal("1765"),
            token1_price=Decimal("1"),
            prices_are_fallback=True,
        )
        assert result.fees_usd == Decimal("0")
        assert result.fee_confidence == "low"
        assert result.data_source == "out_of_range:fallback_price"
        assert result.volume_usd is None

    def test_unknown_family_with_tick_bounds_degrades_confidence(self, monkeypatch) -> None:
        # A venue with NO declared lp_economic_family whose position carries
        # real tick bounds is treated as fungible (accrues, never gated) but
        # must say so: LOW confidence + an ":unknown_lp_family" marker —
        # never a silent declared-family-quality number.
        from types import SimpleNamespace

        adapter = LPBacktestAdapter()
        position = self._position("unknown_dex", tick_lower=0, tick_upper=4054)

        monkeypatch.setattr(
            adapter,
            "_fee_formula_context",
            lambda **_kwargs: SimpleNamespace(),
        )
        monkeypatch.setattr(
            adapter,
            "_fee_amount_from_resolution",
            lambda *_args, **_kwargs: SimpleNamespace(
                fees_usd=Decimal("10"),
                fee_confidence="high",
                data_source="subgraph:test",
                volume_usd=Decimal("1000"),
            ),
        )
        monkeypatch.setattr(
            adapter,
            "_fee_slippage_result",
            lambda **_kwargs: SimpleNamespace(confidence=None, pct=None, liquidity_usd=None),
        )

        result = adapter._calculate_fee_accrual(
            position=position,
            position_value_usd=Decimal("3530"),
            elapsed_seconds=3600,
            token0="WETH",
            token1="USDT",
            token0_price=Decimal("1.5"),
            token1_price=Decimal("1"),
        )
        assert result.fees_usd == Decimal("10")  # accrues — never zeroed
        assert result.fee_confidence == "low"
        assert result.data_source == "subgraph:test:unknown_lp_family"

    def test_declared_family_is_not_degraded(self, monkeypatch) -> None:
        from types import SimpleNamespace

        adapter = LPBacktestAdapter()
        # curve declares fungible: tick fields are vocabulary defaults and
        # must NOT trigger the unknown-family degrade.
        position = self._position("curve", tick_lower=0, tick_upper=4054)

        monkeypatch.setattr(adapter, "_fee_formula_context", lambda **_kwargs: SimpleNamespace())
        monkeypatch.setattr(
            adapter,
            "_fee_amount_from_resolution",
            lambda *_args, **_kwargs: SimpleNamespace(
                fees_usd=Decimal("10"),
                fee_confidence="high",
                data_source="subgraph:test",
                volume_usd=Decimal("1000"),
            ),
        )
        monkeypatch.setattr(
            adapter,
            "_fee_slippage_result",
            lambda **_kwargs: SimpleNamespace(confidence=None, pct=None, liquidity_usd=None),
        )

        result = adapter._calculate_fee_accrual(
            position=position,
            position_value_usd=Decimal("3530"),
            elapsed_seconds=3600,
            token0="WETH",
            token1="USDT",
            token0_price=Decimal("1.5"),
            token1_price=Decimal("1"),
        )
        assert result.fee_confidence == "high"
        assert result.data_source == "subgraph:test"


class TestCoinAmountsOpenFailsClosed:
    """A multi-coin allocation vector is not modeled: the result must say so.

    Invariant (result honesty): an unmodeled deposit shape must never
    produce a success — a $0-notional, zero-flow "position" is
    machine-indistinguishable from a real one in the result doc.
    """

    def test_coin_amounts_open_rejects_with_machine_visible_reason(self) -> None:
        from almanak.framework.intents import Intent

        adapter = LPBacktestAdapter()
        intent = Intent.lp_open(
            pool="USDC/USDT/DAI",
            coin_amounts=[Decimal("0"), Decimal("500"), Decimal("500")],
            protocol="curve",
            chain="ethereum",
        )
        market = MockMarketStateWithTimestamp(
            prices={"USDC": Decimal("1"), "USDT": Decimal("1"), "DAI": Decimal("1")},
            timestamp=datetime.now(),
        )

        fill = adapter.execute_intent(intent, MockPortfolio(), market)

        assert fill is not None
        assert fill.success is False
        assert "coin_amounts" in fill.metadata.get("failure_reason", "")
        assert fill.position_delta is None


class TestPoolCoinExitSelectorsFailClosed:
    """coin_index / imbalanced_amounts closes are not modeled: reject, no mutation.

    Executing them as a standard proportional close records token flows the
    venue would never pay out (single-sided and exact-amounts exits reshape
    the withdrawal).
    """

    @staticmethod
    def _close_intent(**selector):
        from almanak.framework.intents import Intent

        return Intent.lp_close(position_id="pos-1", protocol="curve", chain="ethereum", **selector)

    def test_coin_index_close_rejects_without_mutation(self) -> None:
        adapter = LPBacktestAdapter()
        portfolio = MockPortfolio()

        fill = adapter._execute_lp_close(
            self._close_intent(coin_index=1),
            portfolio,
            MockMarketStateWithTimestamp(prices={"USDC": Decimal("1")}, timestamp=datetime.now()),
        )

        assert fill.success is False
        assert "coin_index" in fill.metadata.get("failure_reason", "")

    def test_imbalanced_amounts_close_rejects_without_mutation(self) -> None:
        adapter = LPBacktestAdapter()
        portfolio = MockPortfolio()

        fill = adapter._execute_lp_close(
            self._close_intent(imbalanced_amounts=[Decimal("100"), Decimal("0")]),
            portfolio,
            MockMarketStateWithTimestamp(prices={"USDC": Decimal("1")}, timestamp=datetime.now()),
        )

        assert fill.success is False
        assert "imbalanced_amounts" in fill.metadata.get("failure_reason", "")


class TestDataSourceProvenanceAccumulates:
    """metadata["data_sources"] records every distinct source, append-only."""

    def test_commit_accumulates_distinct_sources(self) -> None:
        from types import SimpleNamespace

        from almanak.framework.backtesting.adapters.lp_adapter import _LPUpdatePlan

        adapter = LPBacktestAdapter()
        position = SimulatedPosition(
            position_type=PositionType.LP,
            protocol="uniswap_v3",
            tokens=["WETH", "USDT"],
            amounts={"WETH": Decimal("1"), "USDT": Decimal("1765")},
            entry_price=Decimal("1765"),
            entry_time=datetime(2024, 1, 1),
        )

        def _plan(source: str) -> _LPUpdatePlan:
            return _LPUpdatePlan(
                update_time=datetime(2024, 1, 2),
                prices=SimpleNamespace(token0="WETH", token1="USDT", current_price=Decimal("1765")),
                amounts=SimpleNamespace(
                    token0_amount=Decimal("1"),
                    token1_amount=Decimal("1765"),
                    il_pct=Decimal("0"),
                ),
                fee_result=SimpleNamespace(
                    fees_usd=Decimal("1"),
                    fees_token0=Decimal("0"),
                    fees_token1=Decimal("1"),
                    fee_confidence="high",
                    slippage_confidence=None,
                    data_source=source,
                    volume_usd=Decimal("100"),
                ),
            )

        adapter._commit_lp_update(position, _plan("subgraph:uniswap_v3"))
        adapter._commit_lp_update(position, _plan("fallback_multiplier:10x"))
        adapter._commit_lp_update(position, _plan("subgraph:uniswap_v3"))

        # Latest wins the back-compat key; the cumulative list keeps both.
        assert position.metadata["data_source"] == "subgraph:uniswap_v3"
        assert position.metadata["data_sources"] == ["subgraph:uniswap_v3", "fallback_multiplier:10x"]

    def test_legacy_singular_key_seeds_the_cumulative_list(self) -> None:
        """A position written before the list existed keeps its first source."""
        from types import SimpleNamespace

        from almanak.framework.backtesting.adapters.lp_adapter import _LPUpdatePlan

        adapter = LPBacktestAdapter()
        position = SimulatedPosition(
            position_type=PositionType.LP,
            protocol="uniswap_v3",
            tokens=["WETH", "USDT"],
            amounts={"WETH": Decimal("1"), "USDT": Decimal("1765")},
            entry_price=Decimal("1765"),
            entry_time=datetime(2024, 1, 1),
        )
        position.metadata["data_source"] = "subgraph:legacy"  # pre-list state

        plan = _LPUpdatePlan(
            update_time=datetime(2024, 1, 2),
            prices=SimpleNamespace(token0="WETH", token1="USDT", current_price=Decimal("1765")),
            amounts=SimpleNamespace(token0_amount=Decimal("1"), token1_amount=Decimal("1765"), il_pct=Decimal("0")),
            fee_result=SimpleNamespace(
                fees_usd=Decimal("1"),
                fees_token0=Decimal("0"),
                fees_token1=Decimal("1"),
                fee_confidence="high",
                slippage_confidence=None,
                data_source="out_of_range",
                volume_usd=None,
            ),
        )
        adapter._commit_lp_update(position, plan)

        assert position.metadata["data_sources"] == ["subgraph:legacy", "out_of_range"]

    def test_partially_migrated_empty_list_still_seeds_the_singular(self) -> None:
        """{data_source: x, data_sources: []} must not lose x on the next commit."""
        from types import SimpleNamespace

        from almanak.framework.backtesting.adapters.lp_adapter import _LPUpdatePlan

        adapter = LPBacktestAdapter()
        position = SimulatedPosition(
            position_type=PositionType.LP,
            protocol="uniswap_v3",
            tokens=["WETH", "USDT"],
            amounts={"WETH": Decimal("1"), "USDT": Decimal("1765")},
            entry_price=Decimal("1765"),
            entry_time=datetime(2024, 1, 1),
        )
        position.metadata["data_source"] = "subgraph:legacy"
        position.metadata["data_sources"] = []  # partially migrated

        plan = _LPUpdatePlan(
            update_time=datetime(2024, 1, 2),
            prices=SimpleNamespace(token0="WETH", token1="USDT", current_price=Decimal("1765")),
            amounts=SimpleNamespace(token0_amount=Decimal("1"), token1_amount=Decimal("1765"), il_pct=Decimal("0")),
            fee_result=SimpleNamespace(
                fees_usd=Decimal("1"),
                fees_token0=Decimal("0"),
                fees_token1=Decimal("1"),
                fee_confidence="high",
                slippage_confidence=None,
                data_source="out_of_range",
                volume_usd=None,
            ),
        )
        adapter._commit_lp_update(position, plan)

        assert position.metadata["data_sources"] == ["subgraph:legacy", "out_of_range"]

    def test_metrics_export_reads_the_cumulative_list(self) -> None:
        """The exported coverage metrics must surface EVERY source a position
        touched — latest-wins alone hides a mid-run degradation."""
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100"))
        position = SimulatedPosition(
            position_type=PositionType.LP,
            protocol="uniswap_v3",
            tokens=["WETH", "USDT"],
            amounts={"WETH": Decimal("1"), "USDT": Decimal("1765")},
            entry_price=Decimal("1765"),
            entry_time=datetime(2024, 1, 1),
        )
        position.metadata["data_source"] = "out_of_range"  # latest only
        position.metadata["data_sources"] = ["subgraph:uniswap_v3", "fallback_multiplier:10x", "out_of_range"]

        metrics = portfolio._lp_data_coverage_metrics([position])

        assert metrics.data_sources == ["subgraph:uniswap_v3", "fallback_multiplier:10x", "out_of_range"]

    def test_metrics_export_falls_back_to_singular_key(self) -> None:
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("100"))
        position = SimulatedPosition(
            position_type=PositionType.LP,
            protocol="uniswap_v3",
            tokens=["WETH", "USDT"],
            amounts={"WETH": Decimal("1"), "USDT": Decimal("1765")},
            entry_price=Decimal("1765"),
            entry_time=datetime(2024, 1, 1),
        )
        position.metadata["data_source"] = "subgraph:legacy"  # no list at all

        metrics = portfolio._lp_data_coverage_metrics([position])

        assert metrics.data_sources == ["subgraph:legacy"]


class TestResolutionVersionLabels:
    """Version-labelled candidates must match the protocol's version."""

    @staticmethod
    def _candidate(pair_address: str, labels: list[str], liquidity_usd: float):
        from types import SimpleNamespace

        return SimpleNamespace(
            pair_address=pair_address,
            chain_id="ethereum",
            dex_id="uniswap",
            labels=labels,
            liquidity=SimpleNamespace(usd=liquidity_usd),
            base_token=SimpleNamespace(address="0x" + "a" * 40, symbol="WETH"),
            quote_token=SimpleNamespace(address="0x" + "b" * 40, symbol="DAI"),
        )

    def test_deeper_v2_pool_is_excluded_for_v3(self) -> None:
        deep_v2 = self._candidate("0x" + "1" * 40, ["v2"], 50_000_000)
        v3 = self._candidate("0x" + "2" * 40, ["v3"], 5_000_000)

        best, _kind = LPBacktestAdapter._pick_deepest_pair_candidate(
            [deep_v2, v3],
            chain="ethereum",
            dex_root="uniswap",
            wanted_addresses={"0x" + "a" * 40, "0x" + "b" * 40},
            wanted_symbols={"WETH", "DAI"},
            required_version_label="v3",
        )
        assert best is v3

    def test_unlabelled_candidates_serve_only_without_labeled_match(self) -> None:
        unlabelled = self._candidate("0x" + "3" * 40, [], 1_000_000)

        best, kind = LPBacktestAdapter._pick_deepest_pair_candidate(
            [unlabelled],
            chain="ethereum",
            dex_root="uniswap",
            wanted_addresses={"0x" + "a" * 40, "0x" + "b" * 40},
            wanted_symbols={"WETH", "DAI"},
            required_version_label="v3",
        )
        assert best is unlabelled
        assert "unlabeled" in kind

    def test_deep_unlabelled_never_outranks_labeled_correct(self) -> None:
        # Invariant: an explicit correct version label beats ANY depth of
        # unlabeled candidate — a $50m unlabeled pool may be the wrong version.
        deep_unlabelled = self._candidate("0x" + "4" * 40, [], 50_000_000)
        labeled_v3 = self._candidate("0x" + "5" * 40, ["v3"], 5_000_000)

        best, kind = LPBacktestAdapter._pick_deepest_pair_candidate(
            [deep_unlabelled, labeled_v3],
            chain="ethereum",
            dex_root="uniswap",
            wanted_addresses={"0x" + "a" * 40, "0x" + "b" * 40},
            wanted_symbols={"WETH", "DAI"},
            required_version_label="v3",
        )
        assert best is labeled_v3
        assert "version-labeled" in kind

    def test_non_version_labels_are_ignored_not_excluding(self) -> None:
        # "stable"/"volatile" are solidly-fork vocabulary, not version claims:
        # a candidate carrying only such labels must stay eligible (as
        # version-unlabeled), not be excluded as a version mismatch.
        stable_labeled = self._candidate("0x" + "6" * 40, ["stable"], 1_000_000)

        best, kind = LPBacktestAdapter._pick_deepest_pair_candidate(
            [stable_labeled],
            chain="ethereum",
            dex_root="uniswap",
            wanted_addresses={"0x" + "a" * 40, "0x" + "b" * 40},
            wanted_symbols={"WETH", "DAI"},
            required_version_label="v3",
        )
        assert best is stable_labeled
        assert "unlabeled" in kind

    def test_contradictory_version_labels_exclude_candidate(self) -> None:
        # ["v2","v3"] claims two versions at once — the metadata cannot be
        # trusted either way, so the candidate is excluded outright.
        contradictory = self._candidate("0x" + "7" * 40, ["v2", "v3"], 50_000_000)

        best, kind = LPBacktestAdapter._pick_deepest_pair_candidate(
            [contradictory],
            chain="ethereum",
            dex_root="uniswap",
            wanted_addresses={"0x" + "a" * 40, "0x" + "b" * 40},
            wanted_symbols={"WETH", "DAI"},
            required_version_label="v3",
        )
        assert best is None
        assert kind == "no-match"

    def test_unlabeled_address_match_beats_labeled_symbol_match(self) -> None:
        # Address-exact token identity dominates aggregator labels: a
        # symbol-only match may never override ANY address-exact match.
        from types import SimpleNamespace

        labeled_symbol_only = SimpleNamespace(
            pair_address="0x" + "8" * 40,
            chain_id="ethereum",
            dex_id="uniswap",
            labels=["v3"],
            liquidity=SimpleNamespace(usd=50_000_000),
            base_token=SimpleNamespace(address="0x" + "c" * 40, symbol="WETH"),
            quote_token=SimpleNamespace(address="0x" + "d" * 40, symbol="DAI"),
        )
        unlabeled_address = self._candidate("0x" + "9" * 40, [], 1_000_000)

        best, kind = LPBacktestAdapter._pick_deepest_pair_candidate(
            [labeled_symbol_only, unlabeled_address],
            chain="ethereum",
            dex_root="uniswap",
            wanted_addresses={"0x" + "a" * 40, "0x" + "b" * 40},
            wanted_symbols={"WETH", "DAI"},
            required_version_label="v3",
        )
        assert best is unlabeled_address
        assert "address-exact" in kind

    def test_filtered_duplicate_does_not_shadow_valid_entry(self) -> None:
        # The two token windows repeat pools: an entry rejected by the
        # filters (wrong chain here) must not mark its pair_address as seen
        # and shadow the valid duplicate arriving later.
        from types import SimpleNamespace

        wrong_chain = SimpleNamespace(
            pair_address="0x" + "e" * 40,
            chain_id="base",
            dex_id="uniswap",
            labels=["v3"],
            liquidity=SimpleNamespace(usd=5_000_000),
            base_token=SimpleNamespace(address="0x" + "a" * 40, symbol="WETH"),
            quote_token=SimpleNamespace(address="0x" + "b" * 40, symbol="DAI"),
        )
        valid = self._candidate("0x" + "e" * 40, ["v3"], 5_000_000)

        best, _kind = LPBacktestAdapter._pick_deepest_pair_candidate(
            [wrong_chain, valid],
            chain="ethereum",
            dex_root="uniswap",
            wanted_addresses={"0x" + "a" * 40, "0x" + "b" * 40},
            wanted_symbols={"WETH", "DAI"},
            required_version_label="v3",
        )
        assert best is valid

    def test_malformed_first_copy_does_not_shadow_valid_duplicate(self) -> None:
        # Order-independence: a copy passing the chain/dex/version filters
        # but failing TOKEN IDENTITY must not suppress a later valid copy of
        # the same pair_address.
        from types import SimpleNamespace

        malformed = SimpleNamespace(
            pair_address="0x" + "f" * 40,
            chain_id="ethereum",
            dex_id="uniswap",
            labels=["v3"],
            liquidity=SimpleNamespace(usd=5_000_000),
            base_token=SimpleNamespace(address="0x" + "c" * 40, symbol="OTHER"),
            quote_token=SimpleNamespace(address="0x" + "d" * 40, symbol="TOKENS"),
        )
        valid = self._candidate("0x" + "f" * 40, ["v3"], 5_000_000)

        best, _kind = LPBacktestAdapter._pick_deepest_pair_candidate(
            [malformed, valid],
            chain="ethereum",
            dex_root="uniswap",
            wanted_addresses={"0x" + "a" * 40, "0x" + "b" * 40},
            wanted_symbols={"WETH", "DAI"},
            required_version_label="v3",
        )
        assert best is valid

    def test_bare_alias_protocols_demand_the_version_label(self) -> None:
        # Strategies name the venue by bare alias too ("uniswap" means the
        # v3 connector in the detection namespace): the label policy must
        # cover those keys, or a deep v2 pool wins a v3 resolution.
        for protocol in ("uniswap", "pancakeswap", "sushiswap"):
            assert LPBacktestAdapter._DEXSCREENER_VERSION_LABELS.get(protocol) == "v3", protocol
        assert LPBacktestAdapter._DEXSCREENER_VERSION_LABELS.get("uniswap_v4") == "v4"
        assert LPBacktestAdapter._DEXSCREENER_VERSION_LABELS.get("uniswap_v2") == "v2"

        deep_v2 = self._candidate("0x" + "1" * 40, ["v2"], 50_000_000)
        v3 = self._candidate("0x" + "2" * 40, ["v3"], 1_000_000)
        best, _kind = LPBacktestAdapter._pick_deepest_pair_candidate(
            [deep_v2, v3],
            chain="ethereum",
            dex_root="uniswap",
            wanted_addresses={"0x" + "a" * 40, "0x" + "b" * 40},
            wanted_symbols={"WETH", "DAI"},
            required_version_label=LPBacktestAdapter._DEXSCREENER_VERSION_LABELS.get("uniswap"),
        )
        assert best is v3


class TestRankingLiquiditySanitation:
    """NaN / Infinity / negative liquidity must never influence ranking."""

    @staticmethod
    def _candidate(pair_address: str, liquidity_usd):
        from types import SimpleNamespace

        return SimpleNamespace(
            pair_address=pair_address,
            chain_id="ethereum",
            dex_id="uniswap",
            labels=["v3"],
            liquidity=SimpleNamespace(usd=liquidity_usd),
            base_token=SimpleNamespace(address="0x" + "a" * 40, symbol="WETH"),
            quote_token=SimpleNamespace(address="0x" + "b" * 40, symbol="DAI"),
        )

    def _pick(self, cands):
        best, _kind = LPBacktestAdapter._pick_deepest_pair_candidate(
            cands,
            chain="ethereum",
            dex_root="uniswap",
            wanted_addresses={"0x" + "a" * 40, "0x" + "b" * 40},
            wanted_symbols={"WETH", "DAI"},
            required_version_label="v3",
        )
        return best

    def test_malformed_liquidity_never_wins_in_any_order(self) -> None:
        import itertools

        nan_pool = self._candidate("0x" + "1" * 40, float("nan"))
        inf_pool = self._candidate("0x" + "2" * 40, float("inf"))
        neg_pool = self._candidate("0x" + "3" * 40, -5.0)
        valid = self._candidate("0x" + "4" * 40, 1_000_000)

        for perm in itertools.permutations([nan_pool, inf_pool, neg_pool, valid]):
            best = self._pick(list(perm))
            assert best is valid, [c.pair_address[:6] for c in perm]

    def test_malformed_only_is_no_match(self) -> None:
        assert self._pick([self._candidate("0x" + "1" * 40, float("nan"))]) is None

    def test_none_liquidity_is_still_eligible_at_zero_rank(self) -> None:
        # None = unmeasured, not malformed: a lone pool with unknown depth
        # still resolves (it ranks at zero, below any measured pool).
        lone = self._candidate("0x" + "5" * 40, None)
        assert self._pick([lone]) is lone
        measured = self._candidate("0x" + "6" * 40, 10.0)
        assert self._pick([lone, measured]) is measured

    def test_kind_claims_no_version_when_none_required(self) -> None:
        cand = self._candidate("0x" + "7" * 40, 1_000_000)
        _best, kind = LPBacktestAdapter._pick_deepest_pair_candidate(
            [cand],
            chain="ethereum",
            dex_root="uniswap",
            wanted_addresses={"0x" + "a" * 40, "0x" + "b" * 40},
            wanted_symbols={"WETH", "DAI"},
            required_version_label=None,
        )
        assert kind == "address-exact"
        assert "version" not in kind


class TestProductAmbiguousResolution:
    """Aerodrome/Velodrome symbolic pools resolve product-exactly or not at all."""

    @staticmethod
    def _row(address: str, dex_id: str, reserve: str, base="0x" + "a" * 40, quote="0x" + "b" * 40):
        from types import SimpleNamespace

        return SimpleNamespace(
            pool_address=address,
            dex_id=dex_id,
            name="WETH / USDC",
            reserve_usd=reserve,
            base_token_address=base,
            quote_token_address=quote,
        )

    def _resolve(self, monkeypatch, protocol: str, rows, gateway_error: Exception | None = None):
        import asyncio
        from types import SimpleNamespace

        adapter = LPBacktestAdapter()
        monkeypatch.setattr(
            adapter,
            "_resolve_token_addresses_for_test",  # marker only; real seam below
            None,
            raising=False,
        )

        async def fake_call(func, request, timeout=None):
            return SimpleNamespace(success=True, error="", pools=rows, complete=True)

        def fake_client():
            if gateway_error is not None:
                raise gateway_error

            class _Pb2:
                @staticmethod
                def TokenPoolsRequest(**kwargs):
                    return kwargs

            return SimpleNamespace(pool_analytics=SimpleNamespace(ListTokenPools=lambda *a, **k: None)), _Pb2

        import almanak.framework.backtesting.pnl.providers.perp._gateway_history as gh

        monkeypatch.setattr(gh, "get_connected_gateway_client", fake_client)
        monkeypatch.setattr(gh, "run_sync_gateway_call", fake_call)

        return asyncio.run(
            adapter._resolve_product_ambiguous_pool(
                "WETH/USDC", protocol, "base", ("WETH", "USDC"), "0x" + "a" * 40, "0x" + "b" * 40
            )
        )

    def test_classic_never_gets_a_slipstream_pool(self, monkeypatch) -> None:
        rows = [
            self._row("0x" + "1" * 40, "aerodrome-slipstream", "50000000"),
            self._row("0x" + "2" * 40, "aerodrome-base", "5000000"),
        ]
        assert self._resolve(monkeypatch, "aerodrome", rows) == "0x" + "2" * 40

    def test_slipstream_never_gets_a_classic_pool(self, monkeypatch) -> None:
        rows = [
            self._row("0x" + "1" * 40, "aerodrome-base", "50000000"),
            self._row("0x" + "2" * 40, "aerodrome-slipstream-3", "5000000"),
        ]
        assert self._resolve(monkeypatch, "aerodrome_slipstream", rows) == "0x" + "2" * 40

    def test_gateway_unavailable_fails_closed(self, monkeypatch) -> None:
        assert self._resolve(monkeypatch, "aerodrome", [], gateway_error=RuntimeError("no gateway")) is None

    def test_no_product_match_fails_closed(self, monkeypatch) -> None:
        rows = [self._row("0x" + "1" * 40, "aerodrome-slipstream", "50000000")]
        assert self._resolve(monkeypatch, "aerodrome", rows) is None

    def test_malformed_reserve_is_skipped(self, monkeypatch) -> None:
        rows = [
            self._row("0x" + "1" * 40, "aerodrome-base", "nan"),
            self._row("0x" + "2" * 40, "aerodrome-base", "1000"),
        ]
        assert self._resolve(monkeypatch, "aerodrome", rows) == "0x" + "2" * 40

    def test_product_dex_id_predicate(self) -> None:
        cases = {
            ("aerodrome", "aerodrome-base"): True,
            ("aerodrome", "aerodrome-slipstream"): False,
            ("aerodrome_slipstream", "aerodrome-slipstream-2"): True,
            ("aerodrome_slipstream", "aerodrome-base"): False,
            ("velodrome", "velodrome-finance-v2"): True,
            ("velodrome_slipstream", "velodrome-finance-slipstream"): True,
            ("velodrome_slipstream", "velodrome-slipstream-v2-optimism"): True,
            ("velodrome", "aerodrome-base"): False,
            ("aerodrome", "uniswap-v3-base"): False,
        }
        for (protocol, dex_id), expected in cases.items():
            assert LPBacktestAdapter._product_dex_id_matches(protocol, dex_id) is expected, (protocol, dex_id)

    def test_unknown_namespaces_never_classify(self) -> None:
        # ANCHORED patterns: near-prefix lookalikes and future/renamed ids
        # must fail closed, never be classified by substring heuristics.
        rejected = [
            ("aerodrome", "aerodrome-v3"),
            ("aerodrome", "aerodrome-fork"),
            ("aerodrome", "aerodromeevil"),
            ("aerodrome_slipstream", "aerodrome-slipstream-fork"),
            ("aerodrome_slipstream", "aerodrome-slipstreamperps"),
            ("velodrome", "velodromeevil"),
            ("velodrome_slipstream", "velodrome-slipstream-perps"),
        ]
        for protocol, dex_id in rejected:
            assert LPBacktestAdapter._product_dex_id_matches(protocol, dex_id) is False, (protocol, dex_id)

    def test_ranked_window_selection_semantics(self, monkeypatch) -> None:
        """Selection is FIRST-match-per-ranked-window (the canonical pool),
        never a deepest-of-all claim: a deeper exact match later in the
        window does not displace the first, and a pair with no match within
        the bounded windows fails closed."""
        import asyncio
        from types import SimpleNamespace

        import almanak.framework.backtesting.pnl.providers.perp._gateway_history as gh

        first_match = self._row("0x" + "1" * 40, "aerodrome-base", "1000")
        deeper_later = self._row("0x" + "2" * 40, "aerodrome-base", "9000000")
        requested_pages: list[int] = []

        async def fake_call(func, request, timeout=None):
            requested_pages.append(request["page"])
            return SimpleNamespace(success=True, error="", pools=[first_match, deeper_later], complete=False)

        def fake_client():
            class _Pb2:
                @staticmethod
                def TokenPoolsRequest(**kwargs):
                    return kwargs

            return SimpleNamespace(pool_analytics=SimpleNamespace(ListTokenPools=lambda *a, **k: None)), _Pb2

        monkeypatch.setattr(gh, "get_connected_gateway_client", fake_client)
        monkeypatch.setattr(gh, "run_sync_gateway_call", fake_call)

        args = ("WETH/USDC", "aerodrome", "base", ("WETH", "USDC"), "0x" + "a" * 40, "0x" + "b" * 40)
        resolved = asyncio.run(LPBacktestAdapter()._resolve_product_ambiguous_pool(*args))
        assert resolved == first_match.pool_address  # upstream rank wins, not raw reserve
        assert set(requested_pages) == {0}  # atomic mode: gateway owns pagination

        # No match within the (possibly incomplete) windows -> fail closed.
        async def no_match_call(func, request, timeout=None):
            return SimpleNamespace(
                success=True,
                error="",
                pools=[self._row("0x" + "3" * 40, "aerodrome-slipstream", "1000")],
                complete=False,
            )

        monkeypatch.setattr(gh, "run_sync_gateway_call", no_match_call)
        assert asyncio.run(LPBacktestAdapter()._resolve_product_ambiguous_pool(*args)) is None

    def test_transient_failure_memo_expires_semantic_memo_does_not(self, monkeypatch) -> None:
        """Failure-kind memo semantics: transport failures retry after the
        expiry window (a rate-limited burst must not blank the family for the
        whole run); a semantic no-match is final for the run."""
        import asyncio
        from types import SimpleNamespace

        import almanak.framework.backtesting.adapters.lp_adapter as lp_mod
        import almanak.framework.backtesting.pnl.providers.perp._gateway_history as gh

        dials: list[int] = []

        def failing_client():
            dials.append(1)
            raise RuntimeError("no gateway")

        monkeypatch.setattr(gh, "get_connected_gateway_client", failing_client)

        adapter = LPBacktestAdapter()
        args = ("WETH/USDC", "aerodrome", "base", ("WETH", "USDC"), "0x" + "a" * 40, "0x" + "b" * 40)
        assert asyncio.run(adapter._resolve_product_ambiguous_pool(*args)) is None
        assert asyncio.run(adapter._resolve_product_ambiguous_pool(*args)) is None
        assert len(dials) == 1  # inside the expiry window: served from memo

        # Simulate recovery past the window: the memo expires and retries.
        key = ("aerodrome", frozenset(("WETH", "USDC")))
        adapter._ambiguous_resolution_failed[key] = 0.0  # already expired
        assert asyncio.run(adapter._resolve_product_ambiguous_pool(*args)) is None
        assert len(dials) == 2  # retried after expiry

        # Semantic no-match is permanent.
        empty_adapter = LPBacktestAdapter()
        semantic_dials: list[int] = []

        async def empty_call(func, request, timeout=None):
            return SimpleNamespace(success=True, error="", pools=[], complete=True)

        def ok_client():
            semantic_dials.append(1)

            class _Pb2:
                @staticmethod
                def TokenPoolsRequest(**kwargs):
                    return kwargs

            return SimpleNamespace(pool_analytics=SimpleNamespace(ListTokenPools=lambda *a, **k: None)), _Pb2

        monkeypatch.setattr(gh, "get_connected_gateway_client", ok_client)
        monkeypatch.setattr(gh, "run_sync_gateway_call", empty_call)
        assert asyncio.run(empty_adapter._resolve_product_ambiguous_pool(*args)) is None
        memo = empty_adapter._ambiguous_resolution_failed[key]
        assert memo == lp_mod._PERMANENT_MEMO
        assert asyncio.run(empty_adapter._resolve_product_ambiguous_pool(*args)) is None
        assert len(semantic_dials) == 1  # never re-dialed


class TestGuessedTierConfidenceCap:
    """An unverified slug-guessed fee tier caps fee confidence at medium."""

    def test_slug_guess_caps_high_volume_confidence(self) -> None:
        from almanak.framework.backtesting.adapters.lp_adapter import _FeeFormulaContext, _VolumeResolution
        from almanak.framework.backtesting.pnl.types import DataConfidence

        adapter = LPBacktestAdapter()
        position = TestRangeGatingScope._position("uniswap_v3")
        position.metadata["fee_tier_source"] = "slug_guess"
        resolution = _VolumeResolution(
            volume_usd=Decimal("1000000"),
            source="historical",
            confidence=DataConfidence.HIGH,
        )
        context = _FeeFormulaContext(
            days_elapsed=Decimal("1"),
            liquidity_share=Decimal("0.001"),
            base_apr=Decimal("0"),
            resolution=resolution,
        )

        result = adapter._fee_amount_from_resolution(position, Decimal("1000"), context, "0xpool", None)

        assert result.fee_confidence == "medium"
        assert "guessed_fee_tier" in result.data_source

    def test_guessed_tier_annotates_all_confidence_levels(self) -> None:
        # Invariant (result honesty): the guessed-tier provenance marks the
        # data_source at EVERY confidence level and on the fallback path —
        # not only when capping high.
        from almanak.framework.backtesting.adapters.lp_adapter import _FeeFormulaContext, _VolumeResolution
        from almanak.framework.backtesting.pnl.types import DataConfidence

        adapter = LPBacktestAdapter()
        for source, confidence in (("historical", DataConfidence.MEDIUM), ("fallback", DataConfidence.LOW)):
            position = TestRangeGatingScope._position("uniswap_v3")
            position.metadata["fee_tier_source"] = "slug_guess"
            context = _FeeFormulaContext(
                days_elapsed=Decimal("1"),
                liquidity_share=Decimal("0.001"),
                base_apr=Decimal("0.05"),
                resolution=_VolumeResolution(volume_usd=Decimal("1000000"), source=source, confidence=confidence),
            )
            result = adapter._fee_amount_from_resolution(position, Decimal("1000"), context, "0xpool", None)
            assert "guessed_fee_tier" in result.data_source, source

    def test_verified_tier_equal_to_guess_marks_verified(self) -> None:
        adapter = LPBacktestAdapter()
        position = TestRangeGatingScope._position("uniswap_v3")
        position.metadata["fee_tier_source"] = "slug_guess"
        position.metadata["pool_address"] = "0xpool"
        adapter._resolved_fee_tiers["0xpool"] = position.fee_tier  # equals the guess

        from almanak.framework.backtesting.pnl.data_provider import MarketState

        state = MarketState(
            timestamp=datetime.now(),
            prices={"WETH": Decimal("1765"), "USDT": Decimal("1")},
            chain="ethereum",
            block_number=1,
        )
        adapter.update_position(position, state, elapsed_seconds=3600)

        assert position.metadata["fee_tier_source"] == "subgraph"

    def test_fee_data_source_persists_to_position_metadata(self) -> None:
        # Invariant: provenance must be result-visible — the metrics
        # aggregator reads metadata["data_source"], not logs.
        from almanak.framework.backtesting.config import BacktestDataConfig
        from almanak.framework.backtesting.pnl.data_provider import MarketState

        adapter = LPBacktestAdapter(data_config=BacktestDataConfig(allow_volume_fallback=True))
        position = TestRangeGatingScope._position("uniswap_v3", tick_lower=0, tick_upper=6931)
        state = MarketState(
            timestamp=datetime.now(),
            prices={"WETH": Decimal("1.5"), "USDT": Decimal("1")},
            chain="ethereum",
            block_number=1,
        )
        adapter.update_position(position, state, elapsed_seconds=3600)

        assert position.metadata.get("data_source")

    def test_explicit_tier_keeps_high_confidence(self) -> None:
        from almanak.framework.backtesting.adapters.lp_adapter import _FeeFormulaContext, _VolumeResolution
        from almanak.framework.backtesting.pnl.types import DataConfidence

        adapter = LPBacktestAdapter()
        position = TestRangeGatingScope._position("uniswap_v3")
        position.metadata["fee_tier_source"] = "explicit"
        resolution = _VolumeResolution(
            volume_usd=Decimal("1000000"),
            source="historical",
            confidence=DataConfidence.HIGH,
        )
        context = _FeeFormulaContext(
            days_elapsed=Decimal("1"),
            liquidity_share=Decimal("0.001"),
            base_apr=Decimal("0"),
            resolution=resolution,
        )

        result = adapter._fee_amount_from_resolution(position, Decimal("1000"), context, "0xpool", None)

        assert result.fee_confidence == "high"


class TestPrewarmFeeTier:
    """Branch coverage for the fee-tier prewarm (CRAP gate, #3271): the
    slug-guessed tier can be 6x off, so the real feeTier is fetched from
    v3-family subgraphs — best-effort, with every miss leaving the guess."""

    @staticmethod
    def _adapter_with_client(query_result=None, exc: Exception | None = None):
        adapter = LPBacktestAdapter()
        calls: list[dict] = []

        class _Client:
            async def query(self, **kwargs):
                calls.append(kwargs)
                if exc is not None:
                    raise exc
                return query_result

        class _Provider:
            _client = _Client()

        adapter._liquidity_provider = _Provider()
        adapter._liquidity_provider_initialized = True
        return adapter, calls

    @pytest.mark.asyncio
    async def test_v3_family_pool_resolves_and_caches_real_tier(self):
        adapter, calls = self._adapter_with_client({"pool": {"feeTier": "500"}})

        await adapter._prewarm_fee_tier("0xpool", "uniswap_v3", "ethereum")

        assert adapter._resolved_fee_tiers["0xpool"] == Decimal("0.0005")  # 500 hundredths-of-a-bip
        assert calls[0]["variables"] == {"poolAddress": "0xpool"}

    @pytest.mark.asyncio
    async def test_memoized_pool_skips_the_query(self):
        adapter, calls = self._adapter_with_client({"pool": {"feeTier": "500"}})
        adapter._resolved_fee_tiers["0xpool"] = Decimal("0.003")

        await adapter._prewarm_fee_tier("0xpool", "uniswap_v3", "ethereum")

        assert calls == []
        assert adapter._resolved_fee_tiers["0xpool"] == Decimal("0.003")  # untouched

    @pytest.mark.asyncio
    async def test_non_v3_family_and_unknown_protocol_never_query(self):
        adapter, calls = self._adapter_with_client({"pool": {"feeTier": "500"}})

        await adapter._prewarm_fee_tier("0xpool", "curve", "ethereum")  # messari family
        await adapter._prewarm_fee_tier("0xpool", "not_a_protocol", "ethereum")  # no entry
        await adapter._prewarm_fee_tier("0xpool", "uniswap_v3", "bsc")  # no declared deployment

        assert calls == []
        assert "0xpool" not in adapter._resolved_fee_tiers

    @pytest.mark.asyncio
    async def test_missing_fee_tier_and_zero_tier_leave_the_guess(self):
        for payload in ({"pool": {"feeTier": None}}, {"pool": None}, None, {"pool": {"feeTier": "0"}}):
            adapter, _calls = self._adapter_with_client(payload)
            await adapter._prewarm_fee_tier("0xpool", "uniswap_v3", "ethereum")
            assert "0xpool" not in adapter._resolved_fee_tiers

    @pytest.mark.asyncio
    async def test_out_of_range_fee_tier_is_rejected(self):
        # A finite tier in (0, 1] may replace the guess; anything else
        # (>100%, non-finite) is a malformed row that would mint unbounded
        # simulated fees through volume * fee_tier * share — reject it.
        for raw in ("2000000", "1000001", "Infinity", "1e30"):
            adapter, _calls = self._adapter_with_client({"pool": {"feeTier": raw}})
            await adapter._prewarm_fee_tier("0xpool", "uniswap_v3", "ethereum")
            assert "0xpool" not in adapter._resolved_fee_tiers, raw
        # The 100% boundary is accepted (0, 1].
        adapter, _calls = self._adapter_with_client({"pool": {"feeTier": "1000000"}})
        await adapter._prewarm_fee_tier("0xpool", "uniswap_v3", "ethereum")
        assert adapter._resolved_fee_tiers["0xpool"] == Decimal("1")

    @pytest.mark.asyncio
    async def test_query_failure_is_best_effort(self):
        adapter, _calls = self._adapter_with_client(exc=RuntimeError("subgraph down"))

        await adapter._prewarm_fee_tier("0xpool", "uniswap_v3", "ethereum")  # must not raise

        assert "0xpool" not in adapter._resolved_fee_tiers

    @pytest.mark.asyncio
    async def test_missing_provider_client_is_a_noop(self, monkeypatch):
        adapter = LPBacktestAdapter()
        monkeypatch.setattr(adapter, "_ensure_liquidity_provider", lambda: None)

        await adapter._prewarm_fee_tier("0xpool", "uniswap_v3", "ethereum")

        assert "0xpool" not in adapter._resolved_fee_tiers


class TestFeeAccrualRoutesProtocol:
    """Regression: the per-tick fee accrual must route the volume/liquidity
    lanes to the position's REAL protocol. Without it, an accrual cache MISS
    re-resolves with protocol=None, which MultiDEXVolumeProvider maps to the
    chain-DEFAULT DEX — silently sending curve/balancer/sushiswap pools to
    uniswap_v3's subgraph and undoing the schema-family fixes."""

    @pytest.mark.asyncio
    async def test_maybe_accrual_forwards_position_protocol(self, monkeypatch):
        adapter = LPBacktestAdapter()
        position = SimulatedPosition.lp(
            token0="WETH",
            token1="USDT",
            amount0=Decimal("1"),
            amount1=Decimal("1765"),
            liquidity=Decimal("1000"),
            entry_price=Decimal("1765"),
            entry_time=datetime(2024, 1, 1),
            tick_lower=0,
            tick_upper=6931,
            fee_tier=Decimal("0.0004"),
            protocol="curve",  # a non-default DEX — the bug's blast radius
        )
        from almanak.framework.backtesting.adapters.lp_adapter import (
            _LPUpdateAmounts,
            _LPUpdatePrices,
        )

        prices = _LPUpdatePrices(
            token0="WETH",
            token1="USDT",
            token0_price=Decimal("1765"),
            token1_price=Decimal("1"),
            current_price=Decimal("1765"),
        )
        amounts = _LPUpdateAmounts(
            il_pct=Decimal("0"),
            token0_amount=Decimal("1"),
            token1_amount=Decimal("1765"),
            position_value_usd=Decimal("3530"),
        )

        captured: dict = {}

        def _spy(*_args, **kwargs):
            captured.update(kwargs)
            return None

        monkeypatch.setattr(adapter, "_calculate_fee_accrual", _spy)
        adapter._maybe_calculate_lp_fee_accrual(
            position=position,
            prices=prices,
            amounts=amounts,
            elapsed_seconds=3600,
            update_time=datetime(2024, 1, 2),
        )
        assert captured["protocol"] == "curve"


class TestResolveSymbolicPoolAddress:
    """Branch coverage for the symbolic-resolution ORCHESTRATOR (CRAP gate,
    #3271). Its sub-pieces (ranking, product-exact resolution) carry their
    own suites; these tests drive the method itself through every outcome:
    parse/memo/registry guards, the product-ambiguous split, DexScreener
    failure, picker miss, composite-id honesty, and the happy path."""

    @staticmethod
    def _resolver_returning(addresses: dict[str, str | None]):
        class _Info:
            def __init__(self, address):
                self.address = address

        class _Resolver:
            def resolve(self, symbol, chain, **_kwargs):
                addr = addresses.get(symbol.upper())
                return _Info(addr) if addr else None

        return _Resolver()

    @staticmethod
    def _dexscreener_returning(candidates=None, exc: Exception | None = None):
        class _Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_a):
                return False

            async def get_token_pairs(self, chain, token_address):
                if exc is not None:
                    raise exc
                return list(candidates or [])

        return _Client

    def _patched(self, monkeypatch, *, addresses=None, candidates=None, dex_exc=None, best=None):
        import almanak.framework.data.dexscreener.client as dex_mod
        import almanak.framework.data.tokens as tokens_mod

        adapter = LPBacktestAdapter()
        monkeypatch.setattr(
            tokens_mod,
            "get_token_resolver",
            lambda: self._resolver_returning(addresses or {"WETH": "0x" + "a" * 40, "USDC": "0x" + "b" * 40}),
        )
        monkeypatch.setattr(dex_mod, "DexScreenerClient", self._dexscreener_returning(candidates, dex_exc))
        if best is not None:
            monkeypatch.setattr(adapter, "_pick_deepest_pair_candidate", lambda *a, **k: best)
        return adapter

    @staticmethod
    def _best(pair_address: str):
        from types import SimpleNamespace

        return (SimpleNamespace(pair_address=pair_address, liquidity=SimpleNamespace(usd=1000000.0)), "address")

    @pytest.mark.asyncio
    async def test_unparseable_pool_returns_none_before_any_lookup(self, monkeypatch):
        import almanak.framework.data.tokens as tokens_mod

        adapter = LPBacktestAdapter()
        monkeypatch.setattr(
            tokens_mod, "get_token_resolver", lambda: pytest.fail("resolver must not run for unparseable pools")
        )
        assert await adapter._resolve_symbolic_pool_address("0xdeadbeef", "uniswap_v3", "base") is None

    @pytest.mark.asyncio
    async def test_memo_hit_short_circuits(self, monkeypatch):
        import almanak.framework.data.tokens as tokens_mod

        adapter = LPBacktestAdapter()
        adapter._resolved_pool_addresses[("uniswap_v3", frozenset({"WETH", "USDC"}), None)] = "0xcached"
        monkeypatch.setattr(tokens_mod, "get_token_resolver", lambda: pytest.fail("memo hit must not resolve"))

        assert await adapter._resolve_symbolic_pool_address("WETH/USDC", "uniswap_v3", "base") == "0xcached"

    @pytest.mark.asyncio
    async def test_unresolvable_token_fails_closed(self, monkeypatch):
        adapter = self._patched(monkeypatch, addresses={"WETH": "0x" + "a" * 40, "USDC": None})

        assert await adapter._resolve_symbolic_pool_address("WETH/USDC", "uniswap_v3", "base") is None

    @pytest.mark.asyncio
    async def test_product_ambiguous_routes_to_gateway_and_stamps_provenance(self, monkeypatch):
        adapter = self._patched(monkeypatch)
        target = "0x" + "c" * 40

        async def fake_product_exact(pool, protocol, chain, pair, t0, t1):
            return target

        monkeypatch.setattr(adapter, "_resolve_product_ambiguous_pool", fake_product_exact)

        resolved = await adapter._resolve_symbolic_pool_address("WETH/USDC", "aerodrome", "base")

        assert resolved == target
        assert adapter._resolved_pool_provenance[target] == "gateway_onchain:product-exact-ranked"
        assert adapter._resolved_pool_addresses[("aerodrome", frozenset({"WETH", "USDC"}), None)] == target

    @pytest.mark.asyncio
    async def test_product_ambiguous_miss_never_falls_back_to_dexscreener(self, monkeypatch):
        import almanak.framework.data.dexscreener.client as dex_mod

        adapter = self._patched(monkeypatch)

        async def fake_product_exact(*_args):
            return None

        monkeypatch.setattr(adapter, "_resolve_product_ambiguous_pool", fake_product_exact)
        monkeypatch.setattr(
            dex_mod, "DexScreenerClient", lambda: pytest.fail("product-ambiguous must NEVER use DexScreener")
        )

        assert await adapter._resolve_symbolic_pool_address("WETH/USDC", "aerodrome", "base") is None
        assert ("aerodrome", frozenset({"WETH", "USDC"}), None) not in adapter._resolved_pool_addresses

    @pytest.mark.asyncio
    async def test_dexscreener_failure_is_best_effort(self, monkeypatch):
        adapter = self._patched(monkeypatch, dex_exc=RuntimeError("dexscreener down"))

        assert await adapter._resolve_symbolic_pool_address("WETH/USDC", "uniswap_v3", "base") is None

    @pytest.mark.asyncio
    async def test_no_candidate_returns_none(self, monkeypatch):
        adapter = self._patched(monkeypatch, candidates=[], best=(None, ""))

        assert await adapter._resolve_symbolic_pool_address("WETH/USDC", "uniswap_v3", "base") is None

    @pytest.mark.asyncio
    async def test_composite_non_address_id_is_refused(self, monkeypatch):
        adapter = self._patched(monkeypatch, candidates=[], best=self._best("weirdpool-0xaaa-0xbbb"))

        assert await adapter._resolve_symbolic_pool_address("WETH/USDC", "curve", "ethereum") is None

    @pytest.mark.asyncio
    async def test_happy_path_memoizes_with_dexscreener_provenance(self, monkeypatch):
        pool_address = "0x" + "d" * 40
        adapter = self._patched(monkeypatch, candidates=[], best=self._best(pool_address))

        resolved = await adapter._resolve_symbolic_pool_address("WETH/USDC", "uniswap_v3", "base")

        assert resolved == pool_address
        assert adapter._resolved_pool_provenance[pool_address] == "dexscreener:address"
        # Composite curve-style ids keep only the leading plain address.
        composite = self._patched(
            monkeypatch,
            addresses={"USDT": "0x" + "e" * 40, "USDC": "0x" + "b" * 40},
            candidates=[],
            best=self._best(pool_address + "-0xt0-0xt1"),
        )
        assert await composite._resolve_symbolic_pool_address("USDT/USDC", "curve", "ethereum") == pool_address


class TestFeeExactPoolResolution:
    """ALM-2949: a declared fee segment ("WETH/USDC/3000") resolves FEE-EXACT
    via the factory getPool lane — never depth-ranked, never silently swapped
    for another tier's pool, and the declared tier is never "corrected"."""

    def _no_dexscreener(self, monkeypatch):
        import almanak.framework.data.dexscreener.client as dex_mod
        import almanak.framework.data.tokens as tokens_mod

        monkeypatch.setattr(
            tokens_mod,
            "get_token_resolver",
            lambda: TestResolveSymbolicPoolAddress._resolver_returning(
                {"WETH": "0x" + "a" * 40, "USDC": "0x" + "b" * 40}
            ),
        )
        monkeypatch.setattr(
            dex_mod,
            "DexScreenerClient",
            lambda: pytest.fail("a declared V3 tier must NEVER be depth-ranked via DexScreener"),
        )

    @pytest.mark.asyncio
    async def test_declared_tier_routes_to_factory_lane_and_stamps_provenance(self, monkeypatch):
        adapter = LPBacktestAdapter()
        self._no_dexscreener(monkeypatch)
        target = "0x" + "f" * 40
        seen: list[int] = []

        async def fake_fee_exact(pool, protocol, chain, t0, t1, fee_units):
            seen.append(fee_units)
            return target

        monkeypatch.setattr(adapter, "_resolve_fee_exact_pool", fake_fee_exact)

        resolved = await adapter._resolve_symbolic_pool_address("WETH/USDC/3000", "uniswap_v3", "base")

        assert resolved == target
        assert seen == [3000]
        assert adapter._resolved_pool_provenance[target] == "factory:fee-exact"
        assert adapter._resolved_pool_addresses[("uniswap_v3", frozenset({"WETH", "USDC"}), 3000)] == target

    @pytest.mark.asyncio
    async def test_fee_exact_failure_never_falls_back_to_depth_ranking(self, monkeypatch):
        adapter = LPBacktestAdapter()
        self._no_dexscreener(monkeypatch)

        async def fake_fee_exact(*_args):
            return None

        monkeypatch.setattr(adapter, "_resolve_fee_exact_pool", fake_fee_exact)

        assert await adapter._resolve_symbolic_pool_address("WETH/USDC/3000", "uniswap_v3", "base") is None
        assert ("uniswap_v3", frozenset({"WETH", "USDC"}), 3000) not in adapter._resolved_pool_addresses

    @pytest.mark.asyncio
    async def test_tiers_resolve_to_distinct_addresses(self, monkeypatch):
        adapter = LPBacktestAdapter()
        self._no_dexscreener(monkeypatch)

        async def fake_fee_exact(pool, protocol, chain, t0, t1, fee_units):
            return f"0xpool{fee_units}"

        monkeypatch.setattr(adapter, "_resolve_fee_exact_pool", fake_fee_exact)

        assert await adapter._resolve_symbolic_pool_address("WETH/USDC/500", "uniswap_v3", "base") == "0xpool500"
        assert await adapter._resolve_symbolic_pool_address("WETH/USDC/3000", "uniswap_v3", "base") == "0xpool3000"
        # The memo is fee-aware: the second tier is NOT the first's cache hit.
        assert adapter._resolved_pool_addresses[("uniswap_v3", frozenset({"WETH", "USDC"}), 500)] == "0xpool500"
        assert adapter._resolved_pool_addresses[("uniswap_v3", frozenset({"WETH", "USDC"}), 3000)] == "0xpool3000"

    @pytest.mark.asyncio
    async def test_non_v3_family_fee_segment_keeps_depth_ranking(self, monkeypatch):
        # A bin venue's third segment is a BIN STEP, not a V3 fee — it must
        # not dial the factory lane; the segment still keys the memo.
        import almanak.framework.data.dexscreener.client as dex_mod
        import almanak.framework.data.tokens as tokens_mod

        adapter = LPBacktestAdapter()
        pool_address = "0x" + "d" * 40
        monkeypatch.setattr(
            tokens_mod,
            "get_token_resolver",
            lambda: TestResolveSymbolicPoolAddress._resolver_returning(
                {"WETH": "0x" + "a" * 40, "USDC": "0x" + "b" * 40}
            ),
        )
        monkeypatch.setattr(
            dex_mod, "DexScreenerClient", TestResolveSymbolicPoolAddress._dexscreener_returning([])
        )
        monkeypatch.setattr(
            adapter, "_pick_deepest_pair_candidate", lambda *a, **k: TestResolveSymbolicPoolAddress._best(pool_address)
        )

        async def forbidden(*_args):
            pytest.fail("bin-step venues must not dial the V3 factory lane")

        monkeypatch.setattr(adapter, "_resolve_fee_exact_pool", forbidden)

        resolved = await adapter._resolve_symbolic_pool_address("WETH/USDC/25", "traderjoe_v2", "avalanche")

        assert resolved == pool_address
        assert adapter._resolved_pool_addresses[("traderjoe_v2", frozenset({"WETH", "USDC"}), 25)] == pool_address


class TestResolveFeeExactPool:
    """The factory getPool leg itself: confirmed / zero-address / unverifiable
    / gateway-down outcomes (all but the first fail closed)."""

    @staticmethod
    def _result(exists, pool_address=None, warning=None, error=None):
        from almanak.connectors._strategy_base.pool_validation_base import (
            PoolValidationReason,
            PoolValidationResult,
        )

        reason = (
            PoolValidationReason.CONFIRMED
            if exists
            else (PoolValidationReason.NOT_FOUND if exists is False else PoolValidationReason.RPC_UNAVAILABLE)
        )
        return PoolValidationResult(
            exists=exists, reason=reason, pool_address=pool_address, warning=warning, error=error
        )

    def _patched(self, monkeypatch, *, result=None, gateway_exc=None):
        import almanak.connectors._strategy_base.v3_pool_validation as v3v
        import almanak.framework.backtesting.pnl.providers.perp._gateway_history as gh

        adapter = LPBacktestAdapter()
        calls: list[tuple] = []

        if gateway_exc is not None:

            def _raise():
                raise gateway_exc

            monkeypatch.setattr(gh, "get_connected_gateway_client", _raise)
        else:
            client = SimpleNamespace(name="gateway-client")
            monkeypatch.setattr(gh, "get_connected_gateway_client", lambda: (client, SimpleNamespace()))

        def fake_validate(chain, protocol, token_a, token_b, fee_tier, rpc_url, gateway_client):
            calls.append((chain, protocol, token_a, token_b, fee_tier, rpc_url, gateway_client))
            if result is None:
                pytest.fail("validate_v3_pool must not run when the gateway is unavailable")
            return result

        monkeypatch.setattr(v3v, "validate_v3_pool", fake_validate)
        return adapter, calls

    @pytest.mark.asyncio
    async def test_confirmed_pool_returns_lowercased_address(self, monkeypatch):
        adapter, calls = self._patched(
            monkeypatch, result=self._result(True, pool_address="0x" + "AB" * 20)
        )

        resolved = await adapter._resolve_fee_exact_pool(
            "WETH/USDC/3000", "uniswap_v3", "base", "0xtoken0", "0xtoken1", 3000
        )

        assert resolved == ("0x" + "AB" * 20).lower()
        assert len(calls) == 1
        chain, protocol, t0, t1, fee, rpc_url, _client = calls[0]
        assert (chain, protocol, t0, t1, fee, rpc_url) == ("base", "uniswap_v3", "0xtoken0", "0xtoken1", 3000, None)

    @pytest.mark.asyncio
    async def test_zero_address_fails_closed(self, monkeypatch):
        adapter, _ = self._patched(monkeypatch, result=self._result(False, error="no pool"))

        assert (
            await adapter._resolve_fee_exact_pool("WETH/USDC/123", "uniswap_v3", "base", "0xa", "0xb", 123) is None
        )

    @pytest.mark.asyncio
    async def test_unverifiable_fails_closed(self, monkeypatch):
        adapter, _ = self._patched(monkeypatch, result=self._result(None, warning="rpc down"))

        assert (
            await adapter._resolve_fee_exact_pool("WETH/USDC/500", "uniswap_v3", "base", "0xa", "0xb", 500) is None
        )

    @pytest.mark.asyncio
    async def test_gateway_unavailable_fails_closed_without_validating(self, monkeypatch):
        adapter, calls = self._patched(monkeypatch, gateway_exc=RuntimeError("gateway down"))

        assert (
            await adapter._resolve_fee_exact_pool("WETH/USDC/500", "uniswap_v3", "base", "0xa", "0xb", 500) is None
        )
        assert calls == []


class TestPoolDeclaredFeeTier:
    """The pool-id fee segment is a DECLARED tier: it prices the position and
    is never subgraph-"corrected" (ALM-2949)."""

    def test_pool_segment_declares_the_tier(self):
        adapter = LPBacktestAdapter()
        intent = SimpleNamespace(pool="WETH/USDC/3000", protocol_params={})

        tier = adapter._lp_open_fee_tier(intent, "uniswap_v3")

        assert tier == Decimal("0.003")
        assert adapter._last_fee_tier_explicit is True

    def test_params_override_wins_over_pool_segment(self):
        adapter = LPBacktestAdapter()
        intent = SimpleNamespace(pool="WETH/USDC/3000", protocol_params={"fee_tier": "0.0005"})

        assert adapter._lp_open_fee_tier(intent, "uniswap_v3") == Decimal("0.0005")
        assert adapter._last_fee_tier_explicit is True

    def test_bin_step_segment_is_not_a_declared_fee(self):
        adapter = LPBacktestAdapter()
        intent = SimpleNamespace(pool="WETH/USDC/25", protocol_params={})

        tier = adapter._lp_open_fee_tier(intent, "traderjoe_v2")

        assert tier == Decimal("0.003")  # slug default, not 25/1e6
        assert adapter._last_fee_tier_explicit is False

    def test_declared_tier_survives_subgraph_correction(self):
        from almanak.framework.backtesting.pnl.data_provider import MarketState

        adapter = LPBacktestAdapter()
        position = TestRangeGatingScope._position("uniswap_v3")
        position.metadata["fee_tier_source"] = "explicit"
        position.metadata["pool_address"] = "0xpool"
        declared = position.fee_tier
        adapter._resolved_fee_tiers["0xpool"] = declared * 6  # the WRONG pool's tier

        state = MarketState(
            timestamp=datetime.now(),
            prices={"WETH": Decimal("1765"), "USDT": Decimal("1")},
            chain="ethereum",
            block_number=1,
        )
        adapter.update_position(position, state, elapsed_seconds=3600)

        assert position.fee_tier == declared
        assert position.metadata["fee_tier_source"] == "explicit"

    def test_accrual_backfill_reconstructs_the_fee_aware_key(self):
        from almanak.framework.backtesting.pnl.data_provider import MarketState

        adapter = LPBacktestAdapter()
        position = TestRangeGatingScope._position("uniswap_v3")
        position.metadata["pool_address"] = None
        position.metadata["declared_fee_units"] = 3000
        resolved = "0x" + "9" * 40
        adapter._resolved_pool_addresses[("uniswap_v3", frozenset({"WETH", "USDT"}), 3000)] = resolved
        adapter._resolved_pool_provenance[resolved] = "factory:fee-exact"

        state = MarketState(
            timestamp=datetime.now(),
            prices={"WETH": Decimal("1765"), "USDT": Decimal("1")},
            chain="ethereum",
            block_number=1,
        )
        adapter.update_position(position, state, elapsed_seconds=3600)

        assert position.metadata["pool_address"] == resolved
        assert position.metadata["pool_resolution"] == "factory:fee-exact"

    def test_annotate_carries_declared_fee_units_for_symbolic_pools(self):
        adapter = LPBacktestAdapter()
        position = TestRangeGatingScope._position("uniswap_v3")
        plan = SimpleNamespace(
            token0="WETH",
            token1="USDC",
            amount0=Decimal("1"),
            amount1=Decimal("3000"),
            entry_price=Decimal("3000"),
        )

        adapter._annotate_lp_open_position(position, "WETH/USDC/3000", plan)
        assert position.metadata["declared_fee_units"] == 3000

        address_position = TestRangeGatingScope._position("uniswap_v3")
        adapter._annotate_lp_open_position(address_position, "0x" + "c" * 40, plan)
        assert "declared_fee_units" not in address_position.metadata


class TestFeeExactReviewRound:
    """Review round on #3308: exception containment, malformed-tier fail-closed,
    and the identity-vs-economics mismatch warning."""

    @pytest.mark.asyncio
    async def test_raising_validator_fails_closed(self, monkeypatch):
        import almanak.connectors._strategy_base.v3_pool_validation as v3v
        import almanak.framework.backtesting.pnl.providers.perp._gateway_history as gh

        adapter = LPBacktestAdapter()
        monkeypatch.setattr(gh, "get_connected_gateway_client", lambda: (SimpleNamespace(), SimpleNamespace()))

        def boom(*_args, **_kwargs):
            raise RuntimeError("garbage RPC response at decode")

        monkeypatch.setattr(v3v, "validate_v3_pool", boom)

        assert (
            await adapter._resolve_fee_exact_pool("WETH/USDC/500", "uniswap_v3", "base", "0xa", "0xb", 500) is None
        )

    @pytest.mark.asyncio
    async def test_malformed_declared_tier_fails_closed_never_depth_ranked(self, monkeypatch):
        # "WETH/USDC/0" is a MALFORMED declaration, not an undeclared one —
        # it must not silently fall through to depth-ranking.
        import almanak.framework.data.dexscreener.client as dex_mod
        import almanak.framework.data.tokens as tokens_mod

        adapter = LPBacktestAdapter()
        monkeypatch.setattr(
            tokens_mod,
            "get_token_resolver",
            lambda: TestResolveSymbolicPoolAddress._resolver_returning(
                {"WETH": "0x" + "a" * 40, "USDC": "0x" + "b" * 40}
            ),
        )
        monkeypatch.setattr(
            dex_mod, "DexScreenerClient", lambda: pytest.fail("malformed declared tier must not depth-rank")
        )

        async def forbidden(*_args):
            pytest.fail("malformed declared tier must not reach the factory lane")

        monkeypatch.setattr(adapter, "_resolve_fee_exact_pool", forbidden)

        assert await adapter._resolve_symbolic_pool_address("WETH/USDC/0", "uniswap_v3", "base") is None
        assert await adapter._resolve_symbolic_pool_address("WETH/USDC/1000000", "uniswap_v3", "base") is None
        assert adapter._resolved_pool_addresses == {}

    def test_malformed_declared_tier_prices_at_slug_guess(self):
        adapter = LPBacktestAdapter()
        intent = SimpleNamespace(pool="WETH/USDC/0", protocol_params={})

        tier = adapter._lp_open_fee_tier(intent, "uniswap_v3")

        assert tier == Decimal("0.003")  # slug default
        assert adapter._last_fee_tier_explicit is False

    def test_override_segment_mismatch_warns_but_override_prices(self, caplog):
        import logging

        adapter = LPBacktestAdapter()
        intent = SimpleNamespace(pool="WETH/USDC/3000", protocol_params={"fee_tier": "0.0005"})

        with caplog.at_level(logging.WARNING, logger="almanak.framework.backtesting.adapters.lp_adapter"):
            tier = adapter._lp_open_fee_tier(intent, "uniswap_v3")

        assert tier == Decimal("0.0005")
        assert adapter._last_fee_tier_explicit is True
        assert any("differs from the tier declared in pool id" in r.message for r in caplog.records)

    def test_override_matching_segment_does_not_warn(self, caplog):
        import logging

        adapter = LPBacktestAdapter()
        intent = SimpleNamespace(pool="WETH/USDC/3000", protocol_params={"fee_tier": "0.003"})

        with caplog.at_level(logging.WARNING, logger="almanak.framework.backtesting.adapters.lp_adapter"):
            tier = adapter._lp_open_fee_tier(intent, "uniswap_v3")

        assert tier == Decimal("0.003")
        assert not any("differs from the tier declared" in r.message for r in caplog.records)

    def test_multi_coin_pool_name_keeps_depth_ranking_path(self, monkeypatch):
        # "DAI/USDC/USDT" (curve tri-pool) has a TOKEN third segment — no
        # declared fee, so the fee-exact lane must not activate and pricing
        # stays on the slug guess.
        adapter = LPBacktestAdapter()
        intent = SimpleNamespace(pool="DAI/USDC/USDT", protocol_params={})

        tier = adapter._lp_open_fee_tier(intent, "uniswap_v3")

        assert tier == Decimal("0.003")
        assert adapter._last_fee_tier_explicit is False
