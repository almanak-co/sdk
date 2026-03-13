"""Tests for Curve virtual_price LP estimation fix (VIB-581).

Mature Curve pools have virtual_price > 1.0. The naive LP estimate
(sum of normalized deposits) must be divided by virtual_price to avoid
setting min_lp too high, which causes add_liquidity to revert on-chain.
"""

from decimal import Decimal

import pytest

from almanak.framework.connectors.curve.adapter import (
    CURVE_POOLS,
    CurveAdapter,
    CurveConfig,
    PoolInfo,
    PoolType,
)


@pytest.fixture
def adapter() -> CurveAdapter:
    """Create Curve adapter for Ethereum."""
    config = CurveConfig(
        chain="ethereum",
        wallet_address="0x1234567890123456789012345678901234567890",
    )
    return CurveAdapter(config)


@pytest.fixture
def adapter_arb() -> CurveAdapter:
    """Create Curve adapter for Arbitrum."""
    config = CurveConfig(
        chain="arbitrum",
        wallet_address="0x1234567890123456789012345678901234567890",
    )
    return CurveAdapter(config)


class TestVirtualPriceInPoolData:
    """Verify virtual_price is present in all registered pools."""

    def test_all_pools_have_virtual_price(self) -> None:
        """Every pool in CURVE_POOLS should have a virtual_price entry."""
        for chain, pools in CURVE_POOLS.items():
            for pool_name, pool_data in pools.items():
                assert "virtual_price" in pool_data, (
                    f"Missing virtual_price for {chain}/{pool_name}"
                )
                assert pool_data["virtual_price"] >= Decimal("1.0"), (
                    f"virtual_price must be >= 1.0 for {chain}/{pool_name}"
                )

    def test_arbitrum_2pool_virtual_price(self) -> None:
        """Arbitrum 2pool should have virtual_price ~1.022 (from on-chain query)."""
        vp = CURVE_POOLS["arbitrum"]["2pool"]["virtual_price"]
        assert vp >= 1.02, f"Expected >= 1.02, got {vp}"

    def test_ethereum_3pool_virtual_price(self) -> None:
        """Ethereum 3pool should have virtual_price > 1.0 (mature pool)."""
        vp = CURVE_POOLS["ethereum"]["3pool"]["virtual_price"]
        assert vp > 1.0, f"Expected > 1.0, got {vp}"


class TestPoolInfoVirtualPrice:
    """Verify PoolInfo carries virtual_price from pool data."""

    def test_get_pool_info_includes_virtual_price(self, adapter: CurveAdapter) -> None:
        """get_pool_info should populate virtual_price from pool data."""
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]
        pool_info = adapter.get_pool_info(pool_address)
        assert pool_info is not None
        assert pool_info.virtual_price == CURVE_POOLS["ethereum"]["3pool"]["virtual_price"]

    def test_get_pool_by_name_includes_virtual_price(self, adapter: CurveAdapter) -> None:
        """get_pool_by_name should also populate virtual_price from pool data."""
        pool_info = adapter.get_pool_by_name("3pool")
        assert pool_info is not None
        assert pool_info.virtual_price == CURVE_POOLS["ethereum"]["3pool"]["virtual_price"]

    def test_pool_info_default_virtual_price(self) -> None:
        """PoolInfo should default virtual_price to 1.0."""
        pool = PoolInfo(
            address="0x0000000000000000000000000000000000000000",
            lp_token="0x0000000000000000000000000000000000000001",
            coins=["A", "B"],
            coin_addresses=["0x" + "0" * 40, "0x" + "1" * 40],
            pool_type=PoolType.STABLESWAP,
            n_coins=2,
        )
        assert pool.virtual_price == Decimal("1.0")


class TestEstimateAddLiquidityWithVirtualPrice:
    """Tests for _estimate_add_liquidity accounting for virtual_price."""

    def test_estimate_reduced_by_virtual_price(self, adapter: CurveAdapter) -> None:
        """LP estimate should be divided by virtual_price for mature pools."""
        pool_info = PoolInfo(
            address="0x0000000000000000000000000000000000000000",
            lp_token="0x0000000000000000000000000000000000000001",
            coins=["USDC", "USDT"],
            coin_addresses=["0x" + "0" * 40, "0x" + "1" * 40],
            pool_type=PoolType.STABLESWAP,
            n_coins=2,
            virtual_price=Decimal("1.022"),
        )
        # 100 USDC (6 dec) + 100 USDT (6 dec) = 200e18 naive, / 1.022 = ~195.7e18
        amounts = [100_000_000, 100_000_000]  # 100 each in 6-decimal wei
        result = adapter._estimate_add_liquidity(pool_info, amounts)

        naive = 200 * 10**18
        assert result < naive, "Estimate should be less than naive sum for vp > 1"
        expected = int(Decimal(naive) / Decimal("1.022"))
        assert result == expected

    def test_estimate_unchanged_for_vp_1(self, adapter: CurveAdapter) -> None:
        """LP estimate should be unchanged when virtual_price == 1.0."""
        pool_info = PoolInfo(
            address="0x0000000000000000000000000000000000000000",
            lp_token="0x0000000000000000000000000000000000000001",
            coins=["USDC", "USDT"],
            coin_addresses=["0x" + "0" * 40, "0x" + "1" * 40],
            pool_type=PoolType.STABLESWAP,
            n_coins=2,
            virtual_price=Decimal("1.0"),
        )
        amounts = [100_000_000, 100_000_000]
        result = adapter._estimate_add_liquidity(pool_info, amounts)

        naive = 200 * 10**18
        assert result == naive

    def test_add_liquidity_min_lp_accounts_for_virtual_price(self, adapter_arb: CurveAdapter) -> None:
        """Full add_liquidity flow: min_lp should be lower than naive for Arb 2pool."""
        pool_address = CURVE_POOLS["arbitrum"]["2pool"]["address"]
        vp = CURVE_POOLS["arbitrum"]["2pool"]["virtual_price"]

        result = adapter_arb.add_liquidity(
            pool_address=pool_address,
            amounts=[Decimal("100"), Decimal("100")],
            slippage_bps=50,
        )
        assert result.success is True

        # The min_lp in calldata should reflect virtual_price adjustment.
        # With vp=1.022 and slippage=50bps: min_lp = (200e18 / 1.022) * 0.995
        # Without the fix it would be: 200e18 * 0.995 (too high, causes revert)
        naive_min_lp = 200 * 10**18 * 9950 // 10000
        adjusted_min_lp = int(Decimal(200 * 10**18) / vp) * 9950 // 10000
        assert adjusted_min_lp < naive_min_lp, "Adjusted min_lp must be lower"
        assert result.lp_amount == adjusted_min_lp, (
            f"Expected lp_amount={adjusted_min_lp}, got {result.lp_amount}"
        )

    def test_estimate_3pool_18dec_tokens(self, adapter: CurveAdapter) -> None:
        """Test with 18-decimal tokens like DAI in 3pool."""
        pool_info = PoolInfo(
            address="0x0000000000000000000000000000000000000000",
            lp_token="0x0000000000000000000000000000000000000001",
            coins=["DAI", "USDC", "USDT"],
            coin_addresses=[
                "0x6B175474E89094C44Da98b954EedeAC495271d0F",
                "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                "0xdAC17F958D2ee523a2206206994597C13D831ec7",
            ],
            pool_type=PoolType.STABLESWAP,
            n_coins=3,
            virtual_price=Decimal("1.04"),
        )
        # 100 DAI (18 dec) + 100 USDC (6 dec) + 100 USDT (6 dec)
        amounts = [100 * 10**18, 100_000_000, 100_000_000]
        result = adapter._estimate_add_liquidity(pool_info, amounts)

        naive = 300 * 10**18
        expected = int(Decimal(naive) / Decimal("1.04"))
        assert result == expected
        assert result < naive
