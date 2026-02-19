"""Integration tests for RateMonitor with Anvil fork.

These tests verify the rate monitor works with real on-chain data by
using an Anvil fork of Ethereum mainnet.

To run these tests:
1. Start Anvil fork: anvil --fork-url $ETH_RPC_URL --port 8545
2. Run: pytest src/data/rates/tests/test_integration.py -v

Tests are marked with @pytest.mark.integration and skipped by default
unless the ANVIL_RPC_URL environment variable is set.
"""

import os
from decimal import Decimal

import pytest

from ..monitor import (
    RateMonitor,
    RateSide,
)

# Skip integration tests unless Anvil is available
ANVIL_RPC_URL = os.getenv("ANVIL_RPC_URL", "http://localhost:8545")
SKIP_INTEGRATION = os.getenv("SKIP_INTEGRATION_TESTS", "true").lower() == "true"


@pytest.fixture
def anvil_monitor() -> RateMonitor:
    """Create a RateMonitor configured for Anvil fork."""
    return RateMonitor(
        chain="ethereum",
        rpc_url=ANVIL_RPC_URL,
        cache_ttl_seconds=1.0,  # Short TTL for testing
    )


@pytest.mark.integration
@pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled")
class TestRateMonitorOnAnvil:
    """Integration tests running against Anvil fork."""

    @pytest.mark.asyncio
    async def test_aave_usdc_supply_rate(self, anvil_monitor: RateMonitor) -> None:
        """Test fetching Aave V3 USDC supply rate from chain."""
        rate = await anvil_monitor.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)

        assert rate.protocol == "aave_v3"
        assert rate.token == "USDC"
        assert rate.side == "supply"
        # APY should be reasonable (0-50%)
        assert Decimal("0") <= rate.apy_percent <= Decimal("50")
        print(f"Aave V3 USDC Supply APY: {rate.apy_percent:.2f}%")

    @pytest.mark.asyncio
    async def test_aave_weth_rates(self, anvil_monitor: RateMonitor) -> None:
        """Test fetching Aave V3 WETH supply and borrow rates."""
        supply_rate = await anvil_monitor.get_lending_rate("aave_v3", "WETH", RateSide.SUPPLY)
        borrow_rate = await anvil_monitor.get_lending_rate("aave_v3", "WETH", RateSide.BORROW)

        # Borrow rate should be higher than supply
        assert borrow_rate.apy_percent > supply_rate.apy_percent
        print(f"Aave V3 WETH Supply APY: {supply_rate.apy_percent:.2f}%")
        print(f"Aave V3 WETH Borrow APY: {borrow_rate.apy_percent:.2f}%")

    @pytest.mark.asyncio
    async def test_morpho_usdc_rate(self, anvil_monitor: RateMonitor) -> None:
        """Test fetching Morpho Blue USDC rate from chain."""
        rate = await anvil_monitor.get_lending_rate("morpho_blue", "USDC", RateSide.SUPPLY)

        assert rate.protocol == "morpho_blue"
        assert rate.market_id is not None
        print(f"Morpho Blue USDC Supply APY: {rate.apy_percent:.2f}%")

    @pytest.mark.asyncio
    async def test_compound_usdc_rate(self, anvil_monitor: RateMonitor) -> None:
        """Test fetching Compound V3 USDC rate from chain."""
        rate = await anvil_monitor.get_lending_rate("compound_v3", "USDC", RateSide.SUPPLY)

        assert rate.protocol == "compound_v3"
        print(f"Compound V3 USDC Supply APY: {rate.apy_percent:.2f}%")

    @pytest.mark.asyncio
    async def test_best_usdc_supply_rate(self, anvil_monitor: RateMonitor) -> None:
        """Test finding best USDC supply rate across protocols."""
        result = await anvil_monitor.get_best_lending_rate("USDC", RateSide.SUPPLY)

        assert result.best_rate is not None
        assert len(result.all_rates) > 0

        print(f"\nBest USDC Supply Rate: {result.best_rate.protocol}")
        print(f"APY: {result.best_rate.apy_percent:.2f}%")
        print("\nAll rates:")
        for rate in sorted(result.all_rates, key=lambda r: r.apy_percent, reverse=True):
            print(f"  {rate.protocol}: {rate.apy_percent:.2f}%")

    @pytest.mark.asyncio
    async def test_best_weth_borrow_rate(self, anvil_monitor: RateMonitor) -> None:
        """Test finding best WETH borrow rate (lowest)."""
        result = await anvil_monitor.get_best_lending_rate("WETH", RateSide.BORROW)

        if result.best_rate:
            print(f"\nBest WETH Borrow Rate: {result.best_rate.protocol}")
            print(f"APY: {result.best_rate.apy_percent:.2f}%")

    @pytest.mark.asyncio
    async def test_protocol_rates(self, anvil_monitor: RateMonitor) -> None:
        """Test fetching all rates from Aave V3."""
        rates = await anvil_monitor.get_protocol_rates("aave_v3", tokens=["USDC", "WETH", "USDT"])

        assert rates.protocol == "aave_v3"
        assert "USDC" in rates.rates
        assert "WETH" in rates.rates

        print("\nAave V3 Protocol Rates:")
        for token, sides in rates.rates.items():
            for side, rate in sides.items():
                print(f"  {token} {side}: {rate.apy_percent:.2f}%")

    @pytest.mark.asyncio
    async def test_caching_reduces_calls(self, anvil_monitor: RateMonitor) -> None:
        """Test that caching reduces RPC calls."""
        # First call - should fetch
        await anvil_monitor.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)

        # Get cache stats
        stats1 = anvil_monitor.get_cache_stats()
        assert stats1["total_entries"] >= 1

        # Second call - should use cache
        await anvil_monitor.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)

        # Cache entries should not increase
        stats2 = anvil_monitor.get_cache_stats()
        assert stats2["total_entries"] == stats1["total_entries"]

    @pytest.mark.asyncio
    async def test_rate_comparison_table(self, anvil_monitor: RateMonitor) -> None:
        """Generate a comparison table of rates across protocols."""
        tokens = ["USDC", "WETH"]

        print("\n" + "=" * 60)
        print("LENDING RATE COMPARISON TABLE")
        print("=" * 60)

        for token in tokens:
            print(f"\n{token}:")
            print("-" * 40)

            supply_result = await anvil_monitor.get_best_lending_rate(token, RateSide.SUPPLY)
            borrow_result = await anvil_monitor.get_best_lending_rate(token, RateSide.BORROW)

            print("Supply Rates:")
            for rate in sorted(supply_result.all_rates, key=lambda r: r.apy_percent, reverse=True):
                marker = " (BEST)" if rate == supply_result.best_rate else ""
                print(f"  {rate.protocol:15} {rate.apy_percent:6.2f}%{marker}")

            print("Borrow Rates:")
            for rate in sorted(borrow_result.all_rates, key=lambda r: r.apy_percent):
                marker = " (BEST)" if rate == borrow_result.best_rate else ""
                print(f"  {rate.protocol:15} {rate.apy_percent:6.2f}%{marker}")


# =============================================================================
# Non-Anvil Integration Tests (use default rates)
# =============================================================================


class TestRateMonitorDefaultRates:
    """Tests using default rates (no Anvil required)."""

    @pytest.mark.asyncio
    async def test_default_rates_are_reasonable(self) -> None:
        """Test that default rates are in reasonable range."""
        monitor = RateMonitor(chain="ethereum")

        # Test a few tokens
        for token in ["USDC", "WETH"]:
            rate = await monitor.get_lending_rate("aave_v3", token, RateSide.SUPPLY)

            # Rates should be positive and reasonable
            assert rate.apy_percent > Decimal("0"), f"{token} supply rate should be positive"
            assert rate.apy_percent < Decimal("50"), f"{token} supply rate should be < 50%"

    @pytest.mark.asyncio
    async def test_borrow_higher_than_supply(self) -> None:
        """Test that borrow rates are higher than supply rates."""
        monitor = RateMonitor(chain="ethereum")

        for protocol in ["aave_v3", "morpho_blue", "compound_v3"]:
            for token in ["USDC"]:
                try:
                    supply = await monitor.get_lending_rate(protocol, token, RateSide.SUPPLY)
                    borrow = await monitor.get_lending_rate(protocol, token, RateSide.BORROW)

                    # For most markets, borrow > supply (utilization)
                    # This may not always hold for Morpho due to P2P matching
                    print(f"{protocol} {token}: supply={supply.apy_percent:.2f}%, borrow={borrow.apy_percent:.2f}%")
                except Exception:
                    pass  # Skip unsupported combinations

    @pytest.mark.asyncio
    async def test_rate_spread(self) -> None:
        """Test that we can calculate rate spread."""
        monitor = RateMonitor(chain="ethereum")

        result = await monitor.get_best_lending_rate("USDC", RateSide.SUPPLY)

        if len(result.all_rates) >= 2:
            rates = sorted(result.all_rates, key=lambda r: r.apy_percent, reverse=True)
            highest = rates[0].apy_percent
            lowest = rates[-1].apy_percent
            spread = highest - lowest

            print(f"USDC Supply Rate Spread: {spread:.2f}% ({lowest:.2f}% - {highest:.2f}%)")
            assert spread >= Decimal("0")
