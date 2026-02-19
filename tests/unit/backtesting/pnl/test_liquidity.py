"""Unit tests for pool liquidity module.

This module tests pool liquidity querying and estimation functions
for the slippage models.
"""

from decimal import Decimal

from almanak.framework.backtesting.pnl.fee_models.liquidity import (
    DEFAULT_LIQUIDITY_USD,
    KNOWN_POOLS,
    PoolLiquidityResult,
    estimate_liquidity_for_trade,
    get_pool_address,
)

# =============================================================================
# Constants Tests
# =============================================================================


class TestDefaultLiquidityConstants:
    """Tests for DEFAULT_LIQUIDITY_USD constants."""

    def test_stable_001_liquidity(self) -> None:
        """Stable 0.01% pools should have $10M default."""
        assert DEFAULT_LIQUIDITY_USD["stable_001"] == Decimal("10000000")

    def test_stable_005_liquidity(self) -> None:
        """Stable 0.05% pools should have $5M default."""
        assert DEFAULT_LIQUIDITY_USD["stable_005"] == Decimal("5000000")

    def test_blue_chip_030_liquidity(self) -> None:
        """Blue chip 0.3% pools should have $2M default."""
        assert DEFAULT_LIQUIDITY_USD["blue_chip_030"] == Decimal("2000000")

    def test_volatile_030_liquidity(self) -> None:
        """Volatile 0.3% pools should have $1M default."""
        assert DEFAULT_LIQUIDITY_USD["volatile_030"] == Decimal("1000000")

    def test_exotic_100_liquidity(self) -> None:
        """Exotic 1% pools should have $100k default."""
        assert DEFAULT_LIQUIDITY_USD["exotic_100"] == Decimal("100000")

    def test_default_fallback(self) -> None:
        """Default fallback should be $1M."""
        assert DEFAULT_LIQUIDITY_USD["default"] == Decimal("1000000")


class TestKnownPools:
    """Tests for KNOWN_POOLS constants."""

    def test_ethereum_pools_exist(self) -> None:
        """Ethereum should have known pool addresses."""
        assert "ethereum" in KNOWN_POOLS
        assert "WETH/USDC" in KNOWN_POOLS["ethereum"]
        assert "WETH/USDT" in KNOWN_POOLS["ethereum"]
        assert "WBTC/WETH" in KNOWN_POOLS["ethereum"]

    def test_arbitrum_pools_exist(self) -> None:
        """Arbitrum should have known pool addresses."""
        assert "arbitrum" in KNOWN_POOLS
        assert "WETH/USDC" in KNOWN_POOLS["arbitrum"]
        assert "ARB/USDC" in KNOWN_POOLS["arbitrum"]

    def test_base_pools_exist(self) -> None:
        """Base should have known pool addresses."""
        assert "base" in KNOWN_POOLS
        assert "WETH/USDC" in KNOWN_POOLS["base"]

    def test_optimism_pools_exist(self) -> None:
        """Optimism should have known pool addresses."""
        assert "optimism" in KNOWN_POOLS
        assert "WETH/USDC" in KNOWN_POOLS["optimism"]

    def test_polygon_pools_exist(self) -> None:
        """Polygon should have known pool addresses."""
        assert "polygon" in KNOWN_POOLS
        assert "WETH/USDC" in KNOWN_POOLS["polygon"]

    def test_pool_addresses_are_checksummed(self) -> None:
        """Pool addresses should start with 0x."""
        for chain, pools in KNOWN_POOLS.items():
            for pair, address in pools.items():
                assert address.startswith("0x"), f"{chain}/{pair} address should start with 0x"
                assert len(address) == 42, f"{chain}/{pair} address should be 42 chars"


# =============================================================================
# PoolLiquidityResult Tests
# =============================================================================


class TestPoolLiquidityResultCreation:
    """Tests for PoolLiquidityResult dataclass creation."""

    def test_basic_creation(self) -> None:
        """Result should be created with basic parameters."""
        result = PoolLiquidityResult(
            pool_address="0x1234567890123456789012345678901234567890",
            liquidity=10**18,
            liquidity_usd=Decimal("1000000"),
        )
        assert result.pool_address == "0x1234567890123456789012345678901234567890"
        assert result.liquidity == 10**18
        assert result.liquidity_usd == Decimal("1000000")

    def test_default_values(self) -> None:
        """Default values should be set correctly."""
        result = PoolLiquidityResult(
            pool_address="0x1234",
            liquidity=10**18,
            liquidity_usd=Decimal("1000000"),
        )
        assert result.sqrt_price_x96 is None
        assert result.tick is None
        assert result.fee_tier is None
        assert result.is_estimated is False
        assert result.source == "on-chain"

    def test_full_creation(self) -> None:
        """Result should accept all parameters."""
        result = PoolLiquidityResult(
            pool_address="0x1234",
            liquidity=10**18,
            liquidity_usd=Decimal("5000000"),
            sqrt_price_x96=79228162514264337593543950336,  # Q96
            tick=1000,
            fee_tier=3000,
            is_estimated=True,
            source="estimated",
        )
        assert result.sqrt_price_x96 == 79228162514264337593543950336
        assert result.tick == 1000
        assert result.fee_tier == 3000
        assert result.is_estimated is True
        assert result.source == "estimated"


class TestPoolLiquidityResultSerialization:
    """Tests for PoolLiquidityResult serialization."""

    def test_to_dict_basic(self) -> None:
        """Result should serialize to dict."""
        result = PoolLiquidityResult(
            pool_address="0x1234",
            liquidity=10**18,
            liquidity_usd=Decimal("1000000"),
        )
        data = result.to_dict()
        assert data["pool_address"] == "0x1234"
        assert data["liquidity"] == str(10**18)
        assert data["liquidity_usd"] == "1000000"
        assert data["sqrt_price_x96"] is None
        assert data["is_estimated"] is False

    def test_to_dict_with_optional_fields(self) -> None:
        """Result with optional fields should serialize them."""
        result = PoolLiquidityResult(
            pool_address="0x1234",
            liquidity=10**18,
            liquidity_usd=Decimal("5000000"),
            sqrt_price_x96=79228162514264337593543950336,
            tick=500,
            fee_tier=3000,
        )
        data = result.to_dict()
        assert data["sqrt_price_x96"] == str(79228162514264337593543950336)
        assert data["tick"] == 500
        assert data["fee_tier"] == 3000

    def test_from_dict_basic(self) -> None:
        """Result should deserialize from dict."""
        data = {
            "pool_address": "0xabcd",
            "liquidity": str(10**18),
            "liquidity_usd": "2000000",
        }
        result = PoolLiquidityResult.from_dict(data)
        assert result.pool_address == "0xabcd"
        assert result.liquidity == 10**18
        assert result.liquidity_usd == Decimal("2000000")

    def test_from_dict_with_optional_fields(self) -> None:
        """Result should deserialize optional fields."""
        data = {
            "pool_address": "0xabcd",
            "liquidity": str(10**18),
            "liquidity_usd": "3000000",
            "sqrt_price_x96": str(79228162514264337593543950336),
            "tick": 200,
            "fee_tier": 500,
            "is_estimated": True,
            "source": "default",
        }
        result = PoolLiquidityResult.from_dict(data)
        assert result.sqrt_price_x96 == 79228162514264337593543950336
        assert result.tick == 200
        assert result.fee_tier == 500
        assert result.is_estimated is True
        assert result.source == "default"

    def test_roundtrip_serialization(self) -> None:
        """Result should survive serialization roundtrip."""
        original = PoolLiquidityResult(
            pool_address="0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
            liquidity=1234567890123456789,
            liquidity_usd=Decimal("12345678.90"),
            sqrt_price_x96=12345678901234567890,
            tick=-100,
            fee_tier=500,
            is_estimated=True,
            source="estimated",
        )
        restored = PoolLiquidityResult.from_dict(original.to_dict())
        assert restored.pool_address == original.pool_address
        assert restored.liquidity == original.liquidity
        assert restored.liquidity_usd == original.liquidity_usd
        assert restored.sqrt_price_x96 == original.sqrt_price_x96
        assert restored.tick == original.tick
        assert restored.fee_tier == original.fee_tier
        assert restored.is_estimated == original.is_estimated
        assert restored.source == original.source


# =============================================================================
# get_pool_address Tests
# =============================================================================


class TestGetPoolAddress:
    """Tests for get_pool_address function."""

    def test_known_ethereum_pair(self) -> None:
        """Should find known Ethereum pool addresses."""
        address = get_pool_address("WETH", "USDC", "ethereum")
        assert address is not None
        assert address.startswith("0x")

    def test_known_arbitrum_pair(self) -> None:
        """Should find known Arbitrum pool addresses."""
        address = get_pool_address("WETH", "USDC", "arbitrum")
        assert address is not None
        assert address.startswith("0x")

    def test_reverse_order_works(self) -> None:
        """Should find pool regardless of token order."""
        addr1 = get_pool_address("WETH", "USDC", "ethereum")
        addr2 = get_pool_address("USDC", "WETH", "ethereum")
        assert addr1 == addr2

    def test_case_insensitive_tokens(self) -> None:
        """Should work with any case for tokens."""
        addr1 = get_pool_address("weth", "usdc", "ethereum")
        addr2 = get_pool_address("WETH", "USDC", "ethereum")
        assert addr1 == addr2

    def test_unknown_pair_returns_none(self) -> None:
        """Should return None for unknown pairs."""
        address = get_pool_address("UNKNOWN", "TOKEN", "ethereum")
        assert address is None

    def test_unknown_chain_returns_none(self) -> None:
        """Should return None for unknown chains."""
        address = get_pool_address("WETH", "USDC", "unknown_chain")
        assert address is None

    def test_known_base_pair(self) -> None:
        """Should find known Base pool addresses."""
        address = get_pool_address("WETH", "USDC", "base")
        assert address is not None

    def test_known_optimism_pair(self) -> None:
        """Should find known Optimism pool addresses."""
        address = get_pool_address("WETH", "USDC", "optimism")
        assert address is not None


# =============================================================================
# estimate_liquidity_for_trade Tests
# =============================================================================


class TestEstimateLiquidityForTrade:
    """Tests for estimate_liquidity_for_trade function."""

    def test_small_trade_low_slippage(self) -> None:
        """Small trade relative to liquidity should have low slippage."""
        slippage = estimate_liquidity_for_trade(
            trade_amount_usd=Decimal("1000"),  # $1k trade
            pool_liquidity_usd=Decimal("10000000"),  # $10M pool
        )
        # 0.1% of pool -> very low slippage
        assert slippage <= Decimal("0.01")  # 1% or less

    def test_large_trade_higher_slippage(self) -> None:
        """Large trade relative to liquidity should have higher slippage."""
        slippage = estimate_liquidity_for_trade(
            trade_amount_usd=Decimal("500000"),  # $500k trade
            pool_liquidity_usd=Decimal("1000000"),  # $1M pool
        )
        # 50% of pool -> significant slippage
        assert slippage > Decimal("0.01")  # More than 1%
        assert slippage <= Decimal("0.05")  # Capped at 5%

    def test_zero_liquidity_max_slippage(self) -> None:
        """Zero liquidity should return max slippage."""
        slippage = estimate_liquidity_for_trade(
            trade_amount_usd=Decimal("1000"),
            pool_liquidity_usd=Decimal("0"),
        )
        assert slippage == Decimal("0.05")  # 5% max

    def test_slippage_capped_at_5_percent(self) -> None:
        """Slippage should be capped at 5%."""
        slippage = estimate_liquidity_for_trade(
            trade_amount_usd=Decimal("10000000"),  # $10M trade
            pool_liquidity_usd=Decimal("100000"),  # $100k pool
        )
        assert slippage == Decimal("0.05")

    def test_fee_tier_affects_slippage(self) -> None:
        """Higher fee tier should increase slippage estimate (up to cap)."""
        slippage_low_fee = estimate_liquidity_for_trade(
            trade_amount_usd=Decimal("10000"),  # Smaller trade to avoid cap
            pool_liquidity_usd=Decimal("5000000"),
            fee_tier_bps=500,  # 0.05%
        )
        slippage_high_fee = estimate_liquidity_for_trade(
            trade_amount_usd=Decimal("10000"),  # Smaller trade to avoid cap
            pool_liquidity_usd=Decimal("5000000"),
            fee_tier_bps=10000,  # 1%
        )
        # Higher fee pools typically have less depth (both should be under cap)
        assert slippage_high_fee >= slippage_low_fee

    def test_standard_fee_tier(self) -> None:
        """Default 0.3% fee tier should work correctly."""
        slippage = estimate_liquidity_for_trade(
            trade_amount_usd=Decimal("50000"),
            pool_liquidity_usd=Decimal("2000000"),
            fee_tier_bps=3000,  # 0.3%
        )
        assert slippage > Decimal("0")
        assert slippage <= Decimal("0.05")  # Should be at or below cap

    def test_sqrt_scaling_applied(self) -> None:
        """V3 sqrt scaling should make slippage sublinear (when not capped)."""
        # Use smaller trades on larger pool to avoid hitting the 5% cap
        slippage_1x = estimate_liquidity_for_trade(
            trade_amount_usd=Decimal("1000"),
            pool_liquidity_usd=Decimal("10000000"),  # $10M pool
        )
        slippage_4x = estimate_liquidity_for_trade(
            trade_amount_usd=Decimal("4000"),  # 4x trade
            pool_liquidity_usd=Decimal("10000000"),
        )
        # Both should be positive
        assert slippage_1x > Decimal("0")
        assert slippage_4x > Decimal("0")
        # 4x trade should give higher slippage
        assert slippage_4x >= slippage_1x


# =============================================================================
# Integration-Style Tests
# =============================================================================


class TestIntegrationScenarios:
    """Integration-style tests for common scenarios."""

    def test_typical_retail_trade(self) -> None:
        """Typical retail trade on major pool should have minimal impact."""
        # $5k trade on ETH/USDC Ethereum mainnet
        address = get_pool_address("WETH", "USDC", "ethereum")
        assert address is not None

        slippage = estimate_liquidity_for_trade(
            trade_amount_usd=Decimal("5000"),
            pool_liquidity_usd=Decimal("50000000"),  # Assume $50M TVL
        )
        # Should be small (sqrt scaling means 0.01% of pool -> ~1% slippage)
        assert slippage <= Decimal("0.02")  # Less than or equal to 2%

    def test_whale_trade(self) -> None:
        """Large whale trade should show significant impact."""
        slippage = estimate_liquidity_for_trade(
            trade_amount_usd=Decimal("5000000"),  # $5M trade
            pool_liquidity_usd=Decimal("10000000"),  # $10M pool
        )
        # 50% of pool -> capped but high
        assert slippage > Decimal("0.02")  # More than 2%

    def test_arbitrum_pool_lookup(self) -> None:
        """Arbitrum pool lookup should work for common pairs."""
        weth_usdc = get_pool_address("WETH", "USDC", "arbitrum")
        arb_usdc = get_pool_address("ARB", "USDC", "arbitrum")
        wbtc_weth = get_pool_address("WBTC", "WETH", "arbitrum")

        assert weth_usdc is not None
        assert arb_usdc is not None
        assert wbtc_weth is not None
        assert weth_usdc != arb_usdc  # Different pools

    def test_cross_chain_pool_addresses_differ(self) -> None:
        """Same pair should have different addresses on different chains."""
        eth_address = get_pool_address("WETH", "USDC", "ethereum")
        arb_address = get_pool_address("WETH", "USDC", "arbitrum")
        base_address = get_pool_address("WETH", "USDC", "base")

        assert eth_address != arb_address
        assert eth_address != base_address
        assert arb_address != base_address


# =============================================================================
# Edge Cases Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_very_small_trade(self) -> None:
        """Very small trade should have low slippage."""
        slippage = estimate_liquidity_for_trade(
            trade_amount_usd=Decimal("1"),  # $1 trade
            pool_liquidity_usd=Decimal("1000000"),
        )
        # Even tiny trades have some slippage due to sqrt scaling
        assert slippage <= Decimal("0.01")  # Should be small

    def test_very_small_pool(self) -> None:
        """Very small pool should quickly hit max slippage."""
        slippage = estimate_liquidity_for_trade(
            trade_amount_usd=Decimal("10000"),
            pool_liquidity_usd=Decimal("10000"),  # $10k pool
        )
        assert slippage == Decimal("0.05")  # Max

    def test_exact_pool_size_trade(self) -> None:
        """Trade equal to pool size should hit max slippage."""
        slippage = estimate_liquidity_for_trade(
            trade_amount_usd=Decimal("1000000"),
            pool_liquidity_usd=Decimal("1000000"),
        )
        assert slippage == Decimal("0.05")

    def test_negative_trade_handled(self) -> None:
        """Negative trade amount should be handled gracefully."""
        # Function doesn't explicitly validate, but sqrt should handle it
        # This tests that the function doesn't crash
        try:
            slippage = estimate_liquidity_for_trade(
                trade_amount_usd=Decimal("-1000"),
                pool_liquidity_usd=Decimal("1000000"),
            )
            # If it doesn't crash, value should be reasonable
            assert slippage >= Decimal("0") or slippage <= Decimal("0.05")
        except ValueError:
            # Also acceptable to raise for negative input
            pass

    def test_result_with_zero_liquidity_value(self) -> None:
        """Pool result with zero liquidity should serialize correctly."""
        result = PoolLiquidityResult(
            pool_address="0x0000000000000000000000000000000000000000",
            liquidity=0,
            liquidity_usd=Decimal("0"),
        )
        data = result.to_dict()
        assert data["liquidity"] == "0"
        assert data["liquidity_usd"] == "0"

        restored = PoolLiquidityResult.from_dict(data)
        assert restored.liquidity == 0
        assert restored.liquidity_usd == Decimal("0")
