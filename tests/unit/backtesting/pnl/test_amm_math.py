"""Unit tests for AMM math module.

This module tests V2 and V3 pool state classes and price impact calculations
with known inputs/outputs to verify mathematical correctness.
"""

from decimal import Decimal

from almanak.framework.backtesting.pnl.fee_models.amm_math import (
    MAX_TICK,
    MIN_TICK,
    Q96,
    TICK_SPACING_MAP,
    PriceImpactResult,
    V2PoolState,
    V3PoolState,
    calculate_v2_output_amount,
    calculate_v2_price_impact,
    calculate_v2_price_impact_usd,
    calculate_v3_delta_amounts,
    calculate_v3_price_impact,
    calculate_v3_price_impact_usd,
    calculate_v3_swap_output,
    estimate_concentration_factor,
    get_pool_type_from_protocol,
    sqrt_price_x96_to_price,
    sqrt_price_x96_to_tick,
    tick_to_sqrt_price_x96,
)

# =============================================================================
# Constants Tests
# =============================================================================


class TestConstants:
    """Tests for module constants."""

    def test_q96_value(self) -> None:
        """Q96 should be 2^96."""
        assert Q96 == 2**96
        assert Q96 == 79228162514264337593543950336

    def test_min_tick_value(self) -> None:
        """MIN_TICK should be the minimum Uniswap V3 tick."""
        assert MIN_TICK == -887272

    def test_max_tick_value(self) -> None:
        """MAX_TICK should be the maximum Uniswap V3 tick."""
        assert MAX_TICK == 887272

    def test_tick_spacing_map(self) -> None:
        """Tick spacing map should have correct values for each fee tier."""
        assert TICK_SPACING_MAP[100] == 1  # 0.01% fee
        assert TICK_SPACING_MAP[500] == 10  # 0.05% fee
        assert TICK_SPACING_MAP[3000] == 60  # 0.3% fee
        assert TICK_SPACING_MAP[10000] == 200  # 1% fee


# =============================================================================
# V2PoolState Tests
# =============================================================================


class TestV2PoolState:
    """Tests for V2PoolState dataclass."""

    def test_basic_creation(self) -> None:
        """Pool state should be created with basic parameters."""
        pool = V2PoolState(
            reserve_in=Decimal("1000000"),
            reserve_out=Decimal("500"),
        )
        assert pool.reserve_in == Decimal("1000000")
        assert pool.reserve_out == Decimal("500")
        assert pool.fee_bps == 30  # Default 0.3%

    def test_custom_fee(self) -> None:
        """Pool state should accept custom fee."""
        pool = V2PoolState(
            reserve_in=Decimal("1000"),
            reserve_out=Decimal("1000"),
            fee_bps=25,  # 0.25%
        )
        assert pool.fee_bps == 25

    def test_fee_factor_calculation(self) -> None:
        """Fee factor should be 1 - fee_bps/10000."""
        pool = V2PoolState(
            reserve_in=Decimal("1000"),
            reserve_out=Decimal("1000"),
            fee_bps=30,  # 0.3%
        )
        assert pool.fee_factor == Decimal("0.997")

    def test_fee_factor_different_rates(self) -> None:
        """Fee factor should work for different fee rates."""
        # 0.25% fee
        pool_025 = V2PoolState(Decimal("1000"), Decimal("1000"), fee_bps=25)
        assert pool_025.fee_factor == Decimal("0.9975")

        # 0.05% fee
        pool_005 = V2PoolState(Decimal("1000"), Decimal("1000"), fee_bps=5)
        assert pool_005.fee_factor == Decimal("0.9995")

    def test_constant_product_k(self) -> None:
        """K should be reserve_in * reserve_out."""
        pool = V2PoolState(
            reserve_in=Decimal("1000000"),  # $1M
            reserve_out=Decimal("500"),  # 500 ETH
        )
        assert pool.k == Decimal("500000000")

    def test_spot_price(self) -> None:
        """Spot price should be reserve_in / reserve_out."""
        pool = V2PoolState(
            reserve_in=Decimal("2000000"),  # $2M USDC
            reserve_out=Decimal("1000"),  # 1000 ETH
        )
        assert pool.spot_price == Decimal("2000")  # $2000 per ETH

    def test_spot_price_zero_reserve_out(self) -> None:
        """Spot price should be 0 when reserve_out is 0."""
        pool = V2PoolState(
            reserve_in=Decimal("1000"),
            reserve_out=Decimal("0"),
        )
        assert pool.spot_price == Decimal("0")

    def test_total_liquidity_usd_with_values(self) -> None:
        """Total liquidity should be sum of USD reserves."""
        pool = V2PoolState(
            reserve_in=Decimal("1000"),
            reserve_out=Decimal("1000"),
            reserve_in_usd=Decimal("1000000"),
            reserve_out_usd=Decimal("1500000"),
        )
        assert pool.total_liquidity_usd == Decimal("2500000")

    def test_total_liquidity_usd_none(self) -> None:
        """Total liquidity should be None when USD values missing."""
        pool = V2PoolState(
            reserve_in=Decimal("1000"),
            reserve_out=Decimal("1000"),
        )
        assert pool.total_liquidity_usd is None

    def test_to_dict(self) -> None:
        """Pool state should serialize to dict."""
        pool = V2PoolState(
            reserve_in=Decimal("1000000"),
            reserve_out=Decimal("500"),
            fee_bps=30,
            reserve_in_usd=Decimal("1000000"),
            reserve_out_usd=Decimal("1500000"),
        )
        data = pool.to_dict()
        assert data["reserve_in"] == "1000000"
        assert data["reserve_out"] == "500"
        assert data["fee_bps"] == 30
        assert data["reserve_in_usd"] == "1000000"
        assert data["reserve_out_usd"] == "1500000"

    def test_from_dict(self) -> None:
        """Pool state should deserialize from dict."""
        data = {
            "reserve_in": "1000000",
            "reserve_out": "500",
            "fee_bps": 25,
            "reserve_in_usd": "1000000",
            "reserve_out_usd": "1500000",
        }
        pool = V2PoolState.from_dict(data)
        assert pool.reserve_in == Decimal("1000000")
        assert pool.reserve_out == Decimal("500")
        assert pool.fee_bps == 25

    def test_roundtrip_serialization(self) -> None:
        """Pool state should survive serialization roundtrip."""
        original = V2PoolState(
            reserve_in=Decimal("1234567.89"),
            reserve_out=Decimal("987.654"),
            fee_bps=30,
            reserve_in_usd=Decimal("1234567.89"),
            reserve_out_usd=Decimal("2961000"),
        )
        restored = V2PoolState.from_dict(original.to_dict())
        assert restored.reserve_in == original.reserve_in
        assert restored.reserve_out == original.reserve_out
        assert restored.fee_bps == original.fee_bps


# =============================================================================
# V3PoolState Tests
# =============================================================================


class TestV3PoolState:
    """Tests for V3PoolState dataclass."""

    def test_basic_creation(self) -> None:
        """Pool state should be created with basic parameters."""
        # sqrtPriceX96 for price = 1.0: sqrt(1) * 2^96 = 2^96
        pool = V3PoolState(
            sqrt_price_x96=Q96,  # price = 1.0
            liquidity=10**18,
        )
        assert pool.sqrt_price_x96 == Q96
        assert pool.liquidity == 10**18
        assert pool.fee_bps == 3000  # Default 0.3%

    def test_fee_factor_calculation(self) -> None:
        """Fee factor should be 1 - fee_bps/1000000 for V3."""
        # Note: V3 uses fee_bps / 1_000_000 (not 10_000)
        pool = V3PoolState(
            sqrt_price_x96=Q96,
            liquidity=10**18,
            fee_bps=3000,  # 0.3%
        )
        # 1 - 3000/1000000 = 0.997
        assert pool.fee_factor == Decimal("0.997")

    def test_sqrt_price_property(self) -> None:
        """sqrt_price should convert sqrtPriceX96 to Decimal."""
        pool = V3PoolState(
            sqrt_price_x96=Q96,  # sqrt(1) * 2^96
            liquidity=10**18,
        )
        assert pool.sqrt_price == Decimal("1")

    def test_price_property(self) -> None:
        """price should be sqrt_price^2."""
        # sqrtPriceX96 for price = 4.0: sqrt(4) * 2^96 = 2 * 2^96
        pool = V3PoolState(
            sqrt_price_x96=2 * Q96,
            liquidity=10**18,
        )
        assert pool.price == Decimal("4")  # 2^2 = 4

    def test_tick_spacing(self) -> None:
        """Tick spacing should be looked up from fee tier."""
        pool_001 = V3PoolState(Q96, 10**18, fee_bps=100)  # 0.01%
        assert pool_001.tick_spacing == 1

        pool_030 = V3PoolState(Q96, 10**18, fee_bps=3000)  # 0.3%
        assert pool_030.tick_spacing == 60

    def test_is_full_range(self) -> None:
        """is_full_range should detect full tick range positions."""
        # Full range
        pool_full = V3PoolState(
            Q96, 10**18, tick_lower=MIN_TICK, tick_upper=MAX_TICK
        )
        assert pool_full.is_full_range is True

        # Concentrated
        pool_conc = V3PoolState(
            Q96, 10**18, tick_lower=-1000, tick_upper=1000
        )
        assert pool_conc.is_full_range is False

    def test_to_dict(self) -> None:
        """Pool state should serialize to dict."""
        pool = V3PoolState(
            sqrt_price_x96=Q96,
            liquidity=10**18,
            tick=0,
            tick_lower=-100,
            tick_upper=100,
            fee_bps=3000,
            liquidity_usd=Decimal("1000000"),
        )
        data = pool.to_dict()
        assert data["sqrt_price_x96"] == str(Q96)
        assert data["liquidity"] == str(10**18)
        assert data["tick"] == 0
        assert data["fee_bps"] == 3000
        assert data["liquidity_usd"] == "1000000"

    def test_from_dict(self) -> None:
        """Pool state should deserialize from dict."""
        data = {
            "sqrt_price_x96": str(Q96),
            "liquidity": str(10**18),
            "tick": 100,
            "tick_lower": -1000,
            "tick_upper": 1000,
            "fee_bps": 500,
            "liquidity_usd": "5000000",
        }
        pool = V3PoolState.from_dict(data)
        assert pool.sqrt_price_x96 == Q96
        assert pool.liquidity == 10**18
        assert pool.tick == 100
        assert pool.fee_bps == 500


# =============================================================================
# PriceImpactResult Tests
# =============================================================================


class TestPriceImpactResult:
    """Tests for PriceImpactResult dataclass."""

    def test_basic_creation(self) -> None:
        """Result should be created with all fields."""
        result = PriceImpactResult(
            price_impact=Decimal("0.01"),
            effective_price=Decimal("2020"),
            amount_out=Decimal("495"),
            slippage_bps=100,
            pool_type="v2",
        )
        assert result.price_impact == Decimal("0.01")
        assert result.slippage_bps == 100
        assert result.pool_type == "v2"

    def test_slippage_pct(self) -> None:
        """slippage_pct should convert to percentage."""
        result = PriceImpactResult(
            price_impact=Decimal("0.015"),  # 1.5%
            effective_price=Decimal("1"),
            amount_out=Decimal("1"),
            slippage_bps=150,
            pool_type="v2",
        )
        assert result.slippage_pct == Decimal("1.5")

    def test_with_warning(self) -> None:
        """Result should include warning when provided."""
        result = PriceImpactResult(
            price_impact=Decimal("0.08"),
            effective_price=Decimal("1"),
            amount_out=Decimal("1"),
            slippage_bps=800,
            pool_type="v3",
            warning="High price impact: 8.00%",
        )
        assert result.warning is not None
        assert "8.00%" in result.warning

    def test_to_dict(self) -> None:
        """Result should serialize to dict."""
        result = PriceImpactResult(
            price_impact=Decimal("0.025"),
            effective_price=Decimal("2050"),
            amount_out=Decimal("487"),
            slippage_bps=250,
            pool_type="v2",
            warning="Test warning",
        )
        data = result.to_dict()
        assert data["price_impact"] == "0.025"
        assert data["slippage_bps"] == 250
        # slippage_pct is a string representation of the Decimal
        assert "2.5" in data["slippage_pct"]
        assert data["pool_type"] == "v2"
        assert data["warning"] == "Test warning"


# =============================================================================
# V2 Calculation Tests
# =============================================================================


class TestCalculateV2OutputAmount:
    """Tests for calculate_v2_output_amount function."""

    def test_basic_swap(self) -> None:
        """Basic swap should follow constant product formula."""
        pool = V2PoolState(
            reserve_in=Decimal("1000000"),  # 1M USDC
            reserve_out=Decimal("500"),  # 500 ETH
            fee_bps=30,  # 0.3%
        )
        # Swap 10000 USDC for ETH
        amount_out = calculate_v2_output_amount(pool, Decimal("10000"))

        # Expected calculation:
        # amount_in_after_fee = 10000 * 0.997 = 9970
        # amount_out = 500 * 9970 / (1000000 + 9970) = 4985000 / 1009970 ≈ 4.936
        assert amount_out > Decimal("4.9")
        assert amount_out < Decimal("5.0")

    def test_zero_input(self) -> None:
        """Zero input should return zero output."""
        pool = V2PoolState(Decimal("1000000"), Decimal("500"))
        assert calculate_v2_output_amount(pool, Decimal("0")) == Decimal("0")

    def test_negative_input(self) -> None:
        """Negative input should return zero output."""
        pool = V2PoolState(Decimal("1000000"), Decimal("500"))
        assert calculate_v2_output_amount(pool, Decimal("-100")) == Decimal("0")

    def test_empty_pool(self) -> None:
        """Empty pool should return zero output."""
        pool = V2PoolState(Decimal("0"), Decimal("0"))
        assert calculate_v2_output_amount(pool, Decimal("1000")) == Decimal("0")

    def test_no_fee(self) -> None:
        """Zero fee should give expected output."""
        pool = V2PoolState(
            reserve_in=Decimal("1000"),
            reserve_out=Decimal("1000"),
            fee_bps=0,  # No fee
        )
        # With no fee: amount_out = 1000 * 100 / (1000 + 100) = 100000/1100 ≈ 90.909
        amount_out = calculate_v2_output_amount(pool, Decimal("100"))
        expected = Decimal("1000") * Decimal("100") / Decimal("1100")
        assert abs(amount_out - expected) < Decimal("0.001")

    def test_large_trade_moves_price_significantly(self) -> None:
        """Large trade should result in significant slippage."""
        pool = V2PoolState(
            reserve_in=Decimal("1000000"),
            reserve_out=Decimal("1000"),
        )
        # Trade 50% of reserve_in
        amount_out = calculate_v2_output_amount(pool, Decimal("500000"))
        # Should get less than 50% of reserve_out due to price impact
        assert amount_out < Decimal("500")  # Much less than half
        assert amount_out > Decimal("300")  # But still significant


class TestCalculateV2PriceImpact:
    """Tests for calculate_v2_price_impact function."""

    def test_basic_price_impact(self) -> None:
        """Price impact should be calculated correctly."""
        pool = V2PoolState(
            reserve_in=Decimal("1000000"),
            reserve_out=Decimal("500"),
        )
        result = calculate_v2_price_impact(pool, Decimal("50000"))

        # Price impact = amount_in / (reserve_in + amount_in)
        # = 50000 / (1000000 + 50000) = 50000 / 1050000 ≈ 0.0476
        assert result.price_impact > Decimal("0.04")
        assert result.price_impact < Decimal("0.05")
        assert result.pool_type == "v2"

    def test_small_trade_minimal_impact(self) -> None:
        """Small trade relative to reserves should have minimal impact."""
        pool = V2PoolState(
            reserve_in=Decimal("10000000"),
            reserve_out=Decimal("5000"),
        )
        result = calculate_v2_price_impact(pool, Decimal("1000"))

        # 1000 / (10000000 + 1000) ≈ 0.0001 (0.01%)
        assert result.price_impact < Decimal("0.001")
        assert result.warning is None

    def test_large_trade_generates_warning(self) -> None:
        """Large trade should generate warning."""
        pool = V2PoolState(
            reserve_in=Decimal("100000"),
            reserve_out=Decimal("50"),
        )
        result = calculate_v2_price_impact(pool, Decimal("50000"))

        # 50000 / (100000 + 50000) = 0.333 (33%)
        assert result.price_impact > Decimal("0.05")  # Above 5% warning threshold
        assert result.warning is not None

    def test_zero_input_no_impact(self) -> None:
        """Zero input should have no price impact."""
        pool = V2PoolState(Decimal("1000000"), Decimal("500"))
        result = calculate_v2_price_impact(pool, Decimal("0"))

        assert result.price_impact == Decimal("0")
        assert result.amount_out == Decimal("0")
        assert result.slippage_bps == 0

    def test_slippage_bps_calculation(self) -> None:
        """Slippage BPS should be price_impact * 10000."""
        pool = V2PoolState(
            reserve_in=Decimal("100000"),
            reserve_out=Decimal("100"),
        )
        result = calculate_v2_price_impact(pool, Decimal("10000"))

        # 10000 / (100000 + 10000) = 0.0909 (9.09%)
        # BPS = 0.0909 * 10000 ≈ 909
        assert result.slippage_bps > 800
        assert result.slippage_bps < 1000


class TestCalculateV2PriceImpactUsd:
    """Tests for calculate_v2_price_impact_usd function."""

    def test_basic_calculation(self) -> None:
        """USD-based price impact should use 50/50 reserve assumption."""
        result = calculate_v2_price_impact_usd(
            total_liquidity_usd=Decimal("2000000"),  # $2M TVL
            trade_amount_usd=Decimal("50000"),  # $50k trade
        )

        # reserve_in = 1M (50% of TVL)
        # impact = 50000 / (1000000 + 50000) ≈ 0.0476 (4.76%)
        assert result.price_impact > Decimal("0.04")
        assert result.price_impact < Decimal("0.05")
        assert result.pool_type == "v2"

    def test_zero_liquidity(self) -> None:
        """Zero liquidity should return zero impact."""
        result = calculate_v2_price_impact_usd(
            total_liquidity_usd=Decimal("0"),
            trade_amount_usd=Decimal("1000"),
        )
        assert result.price_impact == Decimal("0")
        assert result.amount_out == Decimal("1000")

    def test_high_impact_warning(self) -> None:
        """High impact trades should generate warnings."""
        result = calculate_v2_price_impact_usd(
            total_liquidity_usd=Decimal("200000"),  # $200k pool
            trade_amount_usd=Decimal("50000"),  # $50k trade (25% of pool)
        )

        # 50000 / (100000 + 50000) = 0.333 (33%)
        assert result.price_impact > Decimal("0.05")
        assert result.warning is not None


# =============================================================================
# V3 Tick/Price Conversion Tests
# =============================================================================


class TestTickPriceConversions:
    """Tests for tick to/from sqrtPriceX96 conversions."""

    def test_tick_zero_is_price_one(self) -> None:
        """Tick 0 should correspond to price = 1.0."""
        sqrt_price_x96 = tick_to_sqrt_price_x96(0)
        price = sqrt_price_x96_to_price(sqrt_price_x96)
        assert abs(price - Decimal("1.0")) < Decimal("0.001")

    def test_positive_tick_higher_price(self) -> None:
        """Positive tick should give price > 1.0."""
        sqrt_price_x96 = tick_to_sqrt_price_x96(1000)
        price = sqrt_price_x96_to_price(sqrt_price_x96)
        # price = 1.0001^1000 ≈ 1.105
        assert price > Decimal("1.0")
        assert price < Decimal("1.2")

    def test_negative_tick_lower_price(self) -> None:
        """Negative tick should give price < 1.0."""
        sqrt_price_x96 = tick_to_sqrt_price_x96(-1000)
        price = sqrt_price_x96_to_price(sqrt_price_x96)
        # price = 1.0001^-1000 ≈ 0.905
        assert price < Decimal("1.0")
        assert price > Decimal("0.8")

    def test_sqrt_price_to_tick_roundtrip(self) -> None:
        """Converting tick -> sqrtPrice -> tick should preserve value."""
        original_tick = 5000
        sqrt_price_x96 = tick_to_sqrt_price_x96(original_tick)
        recovered_tick = sqrt_price_x96_to_tick(sqrt_price_x96)
        # Allow small rounding error
        assert abs(recovered_tick - original_tick) <= 1

    def test_sqrt_price_x96_to_price_known_value(self) -> None:
        """Known sqrtPriceX96 should give known price."""
        # sqrtPriceX96 = 2^96 means sqrt(price) = 1, so price = 1
        price = sqrt_price_x96_to_price(Q96)
        assert price == Decimal("1")

        # sqrtPriceX96 = 2 * 2^96 means sqrt(price) = 2, so price = 4
        price = sqrt_price_x96_to_price(2 * Q96)
        assert price == Decimal("4")

    def test_extreme_ticks(self) -> None:
        """Extreme tick values should still work."""
        # Very negative tick
        tick_to_sqrt_price_x96(-800000)

        # Very positive tick
        tick_to_sqrt_price_x96(800000)


# =============================================================================
# V3 Calculation Tests
# =============================================================================


class TestCalculateV3DeltaAmounts:
    """Tests for calculate_v3_delta_amounts function."""

    def test_basic_delta_calculation(self) -> None:
        """Delta amounts should be calculated correctly."""
        liquidity = 10**18
        sqrt_lower = tick_to_sqrt_price_x96(-1000)
        sqrt_upper = tick_to_sqrt_price_x96(1000)

        delta_x, delta_y = calculate_v3_delta_amounts(
            liquidity, sqrt_lower, sqrt_upper
        )

        # Both amounts should be positive for valid range
        assert delta_x > 0
        assert delta_y > 0

    def test_zero_liquidity(self) -> None:
        """Zero liquidity should give zero deltas."""
        delta_x, delta_y = calculate_v3_delta_amounts(0, Q96, 2 * Q96)
        assert delta_x == Decimal("0")
        assert delta_y == Decimal("0")

    def test_invalid_sqrt_price(self) -> None:
        """Invalid sqrt prices should give zero deltas."""
        delta_x, delta_y = calculate_v3_delta_amounts(10**18, 0, 0)
        assert delta_x == Decimal("0")
        assert delta_y == Decimal("0")


class TestCalculateV3SwapOutput:
    """Tests for calculate_v3_swap_output function."""

    def test_basic_swap(self) -> None:
        """Basic V3 swap should return positive output."""
        pool = V3PoolState(
            sqrt_price_x96=Q96,  # price = 1.0
            liquidity=10**18,  # 1e18 liquidity
            fee_bps=3000,  # 0.3%
        )
        amount_out, new_sqrt_price = calculate_v3_swap_output(
            pool, Decimal("1000"), is_token0_in=True
        )

        assert amount_out > Decimal("0")
        assert new_sqrt_price != pool.sqrt_price_x96

    def test_zero_input(self) -> None:
        """Zero input should give zero output."""
        pool = V3PoolState(Q96, 10**18)
        amount_out, new_sqrt_price = calculate_v3_swap_output(
            pool, Decimal("0"), is_token0_in=True
        )

        assert amount_out == Decimal("0")
        assert new_sqrt_price == Q96

    def test_zero_liquidity(self) -> None:
        """Zero liquidity should give zero output."""
        pool = V3PoolState(Q96, 0)
        amount_out, _ = calculate_v3_swap_output(
            pool, Decimal("1000"), is_token0_in=True
        )
        assert amount_out == Decimal("0")

    def test_token1_in_swap(self) -> None:
        """Swapping token1 for token0 should also work."""
        pool = V3PoolState(
            sqrt_price_x96=Q96,
            liquidity=10**18,
        )
        amount_out, new_sqrt_price = calculate_v3_swap_output(
            pool, Decimal("1000"), is_token0_in=False
        )

        assert amount_out > Decimal("0")
        # Price should increase when buying token0 with token1
        assert new_sqrt_price > Q96


class TestCalculateV3PriceImpact:
    """Tests for calculate_v3_price_impact function."""

    def test_basic_price_impact(self) -> None:
        """V3 price impact should be calculated."""
        pool = V3PoolState(
            sqrt_price_x96=Q96,
            liquidity=10**18,
        )
        result = calculate_v3_price_impact(pool, Decimal("10000"))

        assert result.price_impact >= Decimal("0")
        assert result.pool_type == "v3"

    def test_zero_input_no_impact(self) -> None:
        """Zero input should have no impact."""
        pool = V3PoolState(Q96, 10**18)
        result = calculate_v3_price_impact(pool, Decimal("0"))

        assert result.price_impact == Decimal("0")
        assert result.amount_out == Decimal("0")

    def test_zero_liquidity_no_impact(self) -> None:
        """Zero liquidity pool should return zero impact."""
        pool = V3PoolState(Q96, 0)
        result = calculate_v3_price_impact(pool, Decimal("1000"))

        assert result.price_impact == Decimal("0")
        assert result.slippage_bps == 0


class TestCalculateV3PriceImpactUsd:
    """Tests for calculate_v3_price_impact_usd function."""

    def test_basic_calculation(self) -> None:
        """USD-based V3 price impact should work."""
        result = calculate_v3_price_impact_usd(
            liquidity_usd=Decimal("5000000"),  # $5M
            trade_amount_usd=Decimal("50000"),  # $50k
            fee_bps=3000,
        )

        assert result.price_impact > Decimal("0")
        assert result.pool_type == "v3"

    def test_concentration_factor(self) -> None:
        """Higher concentration factor should reduce price impact."""
        result_low = calculate_v3_price_impact_usd(
            liquidity_usd=Decimal("1000000"),
            trade_amount_usd=Decimal("50000"),
            concentration_factor=Decimal("1.0"),
        )
        result_high = calculate_v3_price_impact_usd(
            liquidity_usd=Decimal("1000000"),
            trade_amount_usd=Decimal("50000"),
            concentration_factor=Decimal("5.0"),
        )

        # Higher concentration = more effective liquidity = less impact
        assert result_high.price_impact < result_low.price_impact

    def test_zero_liquidity(self) -> None:
        """Zero liquidity should return zero impact."""
        result = calculate_v3_price_impact_usd(
            liquidity_usd=Decimal("0"),
            trade_amount_usd=Decimal("1000"),
        )
        assert result.price_impact == Decimal("0")


# =============================================================================
# Helper Function Tests
# =============================================================================


class TestEstimateConcentrationFactor:
    """Tests for estimate_concentration_factor function."""

    def test_full_range_returns_one(self) -> None:
        """Full range position should return factor of 1.0."""
        factor = estimate_concentration_factor(MIN_TICK, MAX_TICK, 0)
        assert factor == Decimal("1.0")

    def test_narrow_range_higher_factor(self) -> None:
        """Narrow tick range should give higher concentration factor."""
        factor = estimate_concentration_factor(-100, 100, 0)
        assert factor > Decimal("1.0")

    def test_invalid_range_returns_one(self) -> None:
        """Invalid range (lower >= upper) should return 1.0."""
        factor = estimate_concentration_factor(100, -100, 0)
        assert factor == Decimal("1.0")

        factor = estimate_concentration_factor(0, 0, 0)
        assert factor == Decimal("1.0")


class TestGetPoolTypeFromProtocol:
    """Tests for get_pool_type_from_protocol function."""

    def test_v3_protocols(self) -> None:
        """V3 protocol names should return 'v3'."""
        assert get_pool_type_from_protocol("uniswap_v3") == "v3"
        assert get_pool_type_from_protocol("UniswapV3") == "v3"
        assert get_pool_type_from_protocol("pancakeswap_v3") == "v3"
        assert get_pool_type_from_protocol("PCS_V3") == "v3"

    def test_v2_protocols(self) -> None:
        """V2 protocol names should return 'v2'."""
        assert get_pool_type_from_protocol("uniswap_v2") == "v2"
        assert get_pool_type_from_protocol("sushiswap") == "v2"
        assert get_pool_type_from_protocol("quickswap") == "v2"
        assert get_pool_type_from_protocol("traderjoe_v1") == "v2"

    def test_unknown_defaults_to_v3(self) -> None:
        """Unknown protocol should default to 'v3'."""
        assert get_pool_type_from_protocol("unknown_dex") == "v3"
        assert get_pool_type_from_protocol("new_amm") == "v3"
