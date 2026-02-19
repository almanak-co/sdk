"""Integration tests for MultiDexPriceService.

These tests verify the multi-DEX price service works correctly with actual
DEX interactions on Anvil forks. They test real quote fetching and price
comparison across Uniswap V3, Curve, and Enso.

To run these tests:
    pytest src/data/price/tests/test_multi_dex_integration.py -v

Requirements:
    - Anvil fork of Ethereum mainnet running on port 8545
    - Or set RPC_URL environment variable to an Ethereum RPC endpoint
"""

import os
from decimal import Decimal

import pytest

from ..multi_dex import (
    MultiDexPriceService,
)

# =============================================================================
# Test Configuration
# =============================================================================


# Skip integration tests if no RPC available
RPC_URL = os.environ.get("RPC_URL", "http://localhost:8545")
SKIP_INTEGRATION = os.environ.get("SKIP_INTEGRATION_TESTS", "false").lower() == "true"


@pytest.fixture
def service():
    """Create a MultiDexPriceService for integration testing."""
    return MultiDexPriceService(chain="ethereum")


@pytest.fixture
def arbitrum_service():
    """Create a MultiDexPriceService for Arbitrum integration testing."""
    return MultiDexPriceService(chain="arbitrum")


# =============================================================================
# Integration Test Cases
# =============================================================================


@pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled")
class TestMultiDexIntegration:
    """Integration tests for multi-DEX price comparison."""

    @pytest.mark.asyncio
    async def test_get_prices_usdc_weth(self, service):
        """Test fetching prices for USDC -> WETH across all DEXs."""
        result = await service.get_prices_across_dexs(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),  # 10k USDC
        )

        # Should get quotes from all DEXs
        assert len(result.quotes) >= 1
        assert result.token_in == "USDC"
        assert result.token_out == "WETH"

        # All quotes should have positive amounts
        for dex, quote in result.quotes.items():
            assert quote.amount_in > 0
            assert quote.amount_out > 0
            print(f"{dex}: {quote.amount_out} WETH for 10k USDC")

    @pytest.mark.asyncio
    async def test_get_prices_stablecoin_pair(self, service):
        """Test fetching prices for stablecoin pair (Curve specialty)."""
        result = await service.get_prices_across_dexs(
            token_in="USDC",
            token_out="DAI",
            amount_in=Decimal("10000"),  # 10k USDC
        )

        # Should get quotes from DEXs that support this pair
        assert len(result.quotes) >= 1

        # Stablecoin swaps should have very tight spreads
        for dex, quote in result.quotes.items():
            # Should get close to 10000 DAI for 10000 USDC
            assert quote.amount_out > Decimal("9900")  # At least 99%
            assert quote.price_impact_bps < 100  # Less than 1% impact
            print(f"{dex}: {quote.amount_out} DAI for 10k USDC, impact: {quote.price_impact_bps} bps")

    @pytest.mark.asyncio
    async def test_best_dex_selection(self, service):
        """Test that best DEX is correctly identified."""
        result = await service.get_best_dex_price(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
        )

        assert result.best_dex is not None
        assert result.best_quote is not None
        assert result.best_quote.amount_out > 0

        print(f"Best DEX: {result.best_dex}")
        print(f"Output: {result.best_quote.amount_out} WETH")
        print(f"Savings vs worst: {result.savings_vs_worst_bps} bps")

    @pytest.mark.asyncio
    async def test_slippage_estimation(self, service):
        """Test that slippage estimates are reasonable."""
        # Small trade
        small_result = await service.get_best_dex_price(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("100"),  # $100
        )

        # Large trade
        large_result = await service.get_best_dex_price(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1000000"),  # $1M
        )

        if small_result.best_quote and large_result.best_quote:
            print(f"Small trade slippage: {small_result.best_quote.slippage_estimate_bps} bps")
            print(f"Large trade slippage: {large_result.best_quote.slippage_estimate_bps} bps")

            # Large trade should have higher or equal slippage
            assert large_result.best_quote.slippage_estimate_bps >= small_result.best_quote.slippage_estimate_bps

    @pytest.mark.asyncio
    async def test_gas_estimates_reasonable(self, service):
        """Test that gas estimates are within expected ranges."""
        result = await service.get_prices_across_dexs(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
        )

        for dex, quote in result.quotes.items():
            # Gas estimates should be reasonable (100k - 500k for swaps)
            assert quote.gas_estimate >= 100000
            assert quote.gas_estimate <= 500000
            print(f"{dex} gas estimate: {quote.gas_estimate}")


@pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled")
class TestArbitrumIntegration:
    """Integration tests for Arbitrum chain."""

    @pytest.mark.asyncio
    async def test_arbitrum_prices(self, arbitrum_service):
        """Test fetching prices on Arbitrum."""
        result = await arbitrum_service.get_prices_across_dexs(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
        )

        assert len(result.quotes) >= 1
        assert result.chain == "arbitrum"

        for dex, quote in result.quotes.items():
            assert quote.chain == "arbitrum"
            print(f"Arbitrum {dex}: {quote.amount_out} WETH")


@pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled")
class TestPriceComparisonScenarios:
    """Test specific price comparison scenarios."""

    @pytest.mark.asyncio
    async def test_large_trade_routing(self, service):
        """Test that large trades are routed optimally."""
        result = await service.get_best_dex_price(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1000000"),  # $1M
        )

        if result.best_quote:
            # For large trades, aggregators often win due to split routing
            print(f"Large trade best DEX: {result.best_dex}")
            print(f"Price impact: {result.best_quote.price_impact_bps} bps")

    @pytest.mark.asyncio
    async def test_volatile_pair_pricing(self, service):
        """Test pricing for volatile asset pair."""
        result = await service.get_prices_across_dexs(
            token_in="WETH",
            token_out="WBTC",
            amount_in=Decimal("10"),  # 10 ETH
        )

        assert len(result.quotes) >= 1

        for dex, quote in result.quotes.items():
            # ETH/BTC typically around 0.05-0.07 WBTC per ETH
            assert quote.amount_out > 0
            print(f"{dex}: {quote.amount_out} WBTC for 10 ETH")


# =============================================================================
# MarketSnapshot Integration Tests
# =============================================================================


@pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled")
class TestMarketSnapshotIntegration:
    """Test MarketSnapshot integration with MultiDexPriceService."""

    @pytest.mark.asyncio
    async def test_price_across_dexs_method(self):
        """Test MarketSnapshot.price_across_dexs method."""
        from almanak.framework.data.market_snapshot import MarketSnapshot

        from ..multi_dex import MultiDexPriceService

        service = MultiDexPriceService(chain="ethereum")
        snapshot = MarketSnapshot(
            chain="ethereum",
            wallet_address="0x0000000000000000000000000000000000000000",
            multi_dex_service=service,
        )

        result = snapshot.price_across_dexs(
            token_in="USDC",
            token_out="WETH",
            amount=Decimal("10000"),
        )

        assert len(result.quotes) >= 1
        print(f"Best quote: {result.best_quote}")

    @pytest.mark.asyncio
    async def test_best_dex_price_method(self):
        """Test MarketSnapshot.best_dex_price method."""
        from almanak.framework.data.market_snapshot import MarketSnapshot

        from ..multi_dex import MultiDexPriceService

        service = MultiDexPriceService(chain="ethereum")
        snapshot = MarketSnapshot(
            chain="ethereum",
            wallet_address="0x0000000000000000000000000000000000000000",
            multi_dex_service=service,
        )

        result = snapshot.best_dex_price(
            token_in="USDC",
            token_out="WETH",
            amount=Decimal("10000"),
        )

        if result.best_quote:
            print(f"Best DEX: {result.best_dex}")
            print(f"Output: {result.best_quote.amount_out}")
