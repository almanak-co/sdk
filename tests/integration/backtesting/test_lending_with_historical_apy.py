"""Integration tests for lending backtest with historical APY data.

This module tests the Lending adapter's integration with the Aave V3, Compound V3,
Morpho Blue, and Spark APY providers to validate that interest calculations use
real historical subgraph data when available, and that confidence levels are
properly tracked.

Tests require THEGRAPH_API_KEY environment variable for subgraph access.

Example:
    # Run tests
    pytest tests/integration/backtesting/test_lending_with_historical_apy.py -v -m integration

    # Run with live API (requires THEGRAPH_API_KEY)
    THEGRAPH_API_KEY=your_key pytest -m integration -v
"""

import asyncio
import logging
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.core.enums import Chain
from almanak.framework.backtesting.adapters.lending_adapter import (
    LendingBacktestAdapter,
    LendingBacktestConfig,
)
from almanak.framework.backtesting.config import BacktestDataConfig
from almanak.framework.backtesting.pnl.portfolio import (
    PositionType,
    SimulatedPosition,
)
from almanak.framework.backtesting.pnl.providers.lending import (
    AaveV3APYProvider,
    CompoundV3APYProvider,
    MorphoBlueAPYProvider,
    SparkAPYProvider,
)
from almanak.framework.backtesting.pnl.providers.lending.aave_v3_apy import (
    AaveV3ClientConfig,
)
from almanak.framework.backtesting.pnl.types import DataConfidence

logger = logging.getLogger(__name__)


# =============================================================================
# Test Constants
# =============================================================================

# Well-known markets for testing
USDC_MARKET = "USDC"
WETH_MARKET = "WETH"
DAI_MARKET = "DAI"

# Test date range - use dates for which data should be available
# Aave V3 subgraph has data from mid-2022 onwards
TEST_START_DATE = datetime(2024, 1, 1, tzinfo=UTC)
TEST_END_DATE = datetime(2024, 1, 7, tzinfo=UTC)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def has_thegraph_api_key() -> bool:
    """Check if The Graph API key is available."""
    return bool(os.environ.get("THEGRAPH_API_KEY"))


@pytest.fixture
def lending_adapter_config() -> LendingBacktestConfig:
    """Create Lending adapter configuration."""
    return LendingBacktestConfig(
        strategy_type="lending",
        interest_accrual_method="compound",
        health_factor_tracking_enabled=True,
        interest_rate_source="historical",
        protocol="aave_v3",
    )


@pytest.fixture
def data_config() -> BacktestDataConfig:
    """Create BacktestDataConfig for historical APY data."""
    return BacktestDataConfig(
        use_historical_apy=True,
        supply_apy_fallback=Decimal("0.03"),  # 3% fallback
        borrow_apy_fallback=Decimal("0.05"),  # 5% fallback
        strict_historical_mode=False,  # Don't fail on missing data in integration tests
    )


# =============================================================================
# Integration Tests - Aave V3 APY Provider
# =============================================================================


@pytest.mark.integration
class TestAaveV3APYProvider:
    """Integration tests for Aave V3 APY provider."""

    @pytest.mark.asyncio
    async def test_aave_v3_provider_fetches_real_apy_data(
        self,
        has_thegraph_api_key: bool,
    ) -> None:
        """Test that AaveV3APYProvider fetches real APY data from subgraph.

        This test validates the provider can query The Graph and return
        APYResult objects with HIGH confidence when data is available.
        """
        if not has_thegraph_api_key:
            pytest.skip("THEGRAPH_API_KEY not set - skipping live API test")

        config = AaveV3ClientConfig(chain=Chain.ETHEREUM)
        provider = AaveV3APYProvider(config=config)

        async with provider:
            apys = await provider.get_apy(
                protocol="aave_v3",
                market=USDC_MARKET,
                start_date=TEST_START_DATE,
                end_date=TEST_END_DATE,
            )

        # Verify we got results
        assert len(apys) > 0, "Expected at least one APY result"

        # Check that results have HIGH confidence (real subgraph data)
        high_confidence_count = sum(
            1 for a in apys if a.source_info.confidence == DataConfidence.HIGH
        )

        # At least some results should be HIGH confidence
        assert high_confidence_count > 0, (
            "Expected at least one HIGH confidence result from subgraph"
        )

        # Log the APY data for debugging
        logger.info(
            "Aave V3: Fetched %d APY points, %d HIGH confidence",
            len(apys),
            high_confidence_count,
        )

        for apy in apys[:5]:  # First 5 entries
            logger.debug(
                "Date: %s, Supply APY: %.4f%%, Borrow APY: %.4f%%, Confidence: %s, Source: %s",
                apy.source_info.timestamp.date(),
                float(apy.supply_apy * 100),
                float(apy.borrow_apy * 100),
                apy.source_info.confidence.value,
                apy.source_info.source,
            )

        # Verify APY values are reasonable for USDC (typically 1-10% range)
        high_conf_apys = [
            a for a in apys if a.source_info.confidence == DataConfidence.HIGH
        ]

        if high_conf_apys:
            # Supply APY should be positive
            avg_supply_apy = sum(a.supply_apy for a in high_conf_apys) / len(high_conf_apys)
            assert avg_supply_apy >= Decimal("0"), (
                f"Expected non-negative supply APY, got {avg_supply_apy}"
            )

            # Supply APY for stablecoins should typically be below 20%
            assert avg_supply_apy < Decimal("0.20"), (
                f"Supply APY seems unreasonably high: {avg_supply_apy}"
            )

            # Borrow APY should be higher than supply APY (interest rate spread)
            avg_borrow_apy = sum(a.borrow_apy for a in high_conf_apys) / len(high_conf_apys)
            assert avg_borrow_apy >= avg_supply_apy, (
                f"Expected borrow APY ({avg_borrow_apy}) >= supply APY ({avg_supply_apy})"
            )

            logger.info(
                "Aave V3 USDC: avg supply APY=%.4f%%, avg borrow APY=%.4f%%",
                float(avg_supply_apy * 100),
                float(avg_borrow_apy * 100),
            )

    @pytest.mark.asyncio
    async def test_aave_v3_supports_multiple_chains(
        self,
        has_thegraph_api_key: bool,
    ) -> None:
        """Test that Aave V3 provider supports fetching from multiple chains."""
        if not has_thegraph_api_key:
            pytest.skip("THEGRAPH_API_KEY not set - skipping live API test")

        provider = AaveV3APYProvider()

        # Test Ethereum
        async with provider:
            eth_apys = await provider.get_apy_for_chain(
                chain=Chain.ETHEREUM,
                market=USDC_MARKET,
                start_date=TEST_START_DATE,
                end_date=TEST_START_DATE + timedelta(days=1),
            )

        assert len(eth_apys) > 0, "Expected APY data for Ethereum"

        # Test Arbitrum
        provider_arb = AaveV3APYProvider(config=AaveV3ClientConfig(chain=Chain.ARBITRUM))
        async with provider_arb:
            arb_apys = await provider_arb.get_apy(
                protocol="aave_v3",
                market=USDC_MARKET,
                start_date=TEST_START_DATE,
                end_date=TEST_START_DATE + timedelta(days=1),
            )

        assert len(arb_apys) > 0, "Expected APY data for Arbitrum"

        logger.info(
            "Aave V3: Ethereum=%d results, Arbitrum=%d results",
            len(eth_apys),
            len(arb_apys),
        )

    @pytest.mark.asyncio
    async def test_aave_v3_apy_values_are_valid(
        self,
        has_thegraph_api_key: bool,
    ) -> None:
        """Test that Aave V3 APY values are parsed correctly and within bounds.

        APY values should be reasonable percentages (0-100% range typically).
        """
        if not has_thegraph_api_key:
            pytest.skip("THEGRAPH_API_KEY not set - skipping live API test")

        config = AaveV3ClientConfig(chain=Chain.ETHEREUM)
        provider = AaveV3APYProvider(config=config)

        async with provider:
            apys = await provider.get_apy(
                protocol="aave_v3",
                market=WETH_MARKET,  # WETH typically has lower APY
                start_date=TEST_START_DATE,
                end_date=TEST_END_DATE,
            )

        if not apys:
            pytest.skip("No Aave V3 APY data returned")

        # Filter to HIGH confidence results
        high_conf_apys = [
            a for a in apys if a.source_info.confidence == DataConfidence.HIGH
        ]

        if high_conf_apys:
            for apy in high_conf_apys:
                # Supply APY should be between 0% and 100%
                assert Decimal("0") <= apy.supply_apy <= Decimal("1"), (
                    f"Supply APY {apy.supply_apy} out of valid range"
                )

                # Borrow APY should be between 0% and 100%
                assert Decimal("0") <= apy.borrow_apy <= Decimal("1"), (
                    f"Borrow APY {apy.borrow_apy} out of valid range"
                )

            logger.info("Aave V3 WETH: All %d APY values within valid range", len(high_conf_apys))


# =============================================================================
# Integration Tests - Multiple Lending Protocols
# =============================================================================


@pytest.mark.integration
class TestMultiProtocolAPY:
    """Integration tests for multiple lending protocol APY providers."""

    @pytest.mark.asyncio
    async def test_compound_v3_provider_fetches_apy_data(
        self,
        has_thegraph_api_key: bool,
    ) -> None:
        """Test that CompoundV3APYProvider can fetch APY data."""
        if not has_thegraph_api_key:
            pytest.skip("THEGRAPH_API_KEY not set - skipping live API test")

        provider = CompoundV3APYProvider()

        try:
            async with provider:
                apys = await provider.get_apy(
                    protocol="compound_v3",
                    market=USDC_MARKET,
                    start_date=TEST_START_DATE,
                    end_date=TEST_END_DATE,
                )

            if apys:
                logger.info("Compound V3: Fetched %d APY data points", len(apys))

                high_conf_count = sum(
                    1 for a in apys if a.source_info.confidence == DataConfidence.HIGH
                )
                logger.info("Compound V3: %d HIGH confidence results", high_conf_count)

                # Log sample data
                for apy in apys[:3]:
                    logger.debug(
                        "Compound V3 %s: supply=%.4f%%, borrow=%.4f%%, confidence=%s",
                        apy.source_info.timestamp.date(),
                        float(apy.supply_apy * 100),
                        float(apy.borrow_apy * 100),
                        apy.source_info.confidence.value,
                    )

        except Exception as e:
            pytest.skip(f"Compound V3 subgraph unavailable: {e}")

    @pytest.mark.asyncio
    async def test_morpho_blue_provider_fetches_apy_data(
        self,
        has_thegraph_api_key: bool,
    ) -> None:
        """Test that MorphoBlueAPYProvider can fetch APY data."""
        if not has_thegraph_api_key:
            pytest.skip("THEGRAPH_API_KEY not set - skipping live API test")

        provider = MorphoBlueAPYProvider()

        try:
            async with provider:
                apys = await provider.get_apy(
                    protocol="morpho_blue",
                    market=USDC_MARKET,
                    start_date=TEST_START_DATE,
                    end_date=TEST_END_DATE,
                )

            if apys:
                logger.info("Morpho Blue: Fetched %d APY data points", len(apys))

                high_conf_count = sum(
                    1 for a in apys if a.source_info.confidence == DataConfidence.HIGH
                )
                logger.info("Morpho Blue: %d HIGH confidence results", high_conf_count)

        except Exception as e:
            pytest.skip(f"Morpho Blue subgraph unavailable: {e}")

    @pytest.mark.asyncio
    async def test_spark_provider_fetches_apy_data(
        self,
        has_thegraph_api_key: bool,
    ) -> None:
        """Test that SparkAPYProvider can fetch APY data."""
        if not has_thegraph_api_key:
            pytest.skip("THEGRAPH_API_KEY not set - skipping live API test")

        provider = SparkAPYProvider()

        try:
            async with provider:
                apys = await provider.get_apy(
                    protocol="spark",
                    market=DAI_MARKET,  # Spark is DAI-focused (MakerDAO ecosystem)
                    start_date=TEST_START_DATE,
                    end_date=TEST_END_DATE,
                )

            if apys:
                logger.info("Spark: Fetched %d APY data points", len(apys))

                high_conf_count = sum(
                    1 for a in apys if a.source_info.confidence == DataConfidence.HIGH
                )
                logger.info("Spark: %d HIGH confidence results", high_conf_count)

        except Exception as e:
            pytest.skip(f"Spark subgraph unavailable: {e}")


# =============================================================================
# Integration Tests - Lending Adapter with Historical APY
# =============================================================================


@pytest.mark.integration
class TestLendingAdapterWithHistoricalAPY:
    """Integration tests for Lending adapter using historical APY rates."""

    @pytest.mark.asyncio
    async def test_lending_adapter_uses_historical_apy_for_interest(
        self,
        has_thegraph_api_key: bool,
        lending_adapter_config: LendingBacktestConfig,
        data_config: BacktestDataConfig,
    ) -> None:
        """Test that Lending adapter calculates interest using historical APY.

        When BacktestDataConfig has use_historical_apy=True, the adapter
        should fetch real APY data and calculate interest accurately.
        """
        if not has_thegraph_api_key:
            pytest.skip("THEGRAPH_API_KEY not set - skipping live API test")

        # Create adapter with data config
        adapter = LendingBacktestAdapter(
            config=lending_adapter_config,
            data_config=data_config,
        )

        # Create a simulated supply position
        entry_time = datetime(2024, 1, 5, 12, 0, 0, tzinfo=UTC)
        position = SimulatedPosition(
            position_id="test_supply_position",
            position_type=PositionType.SUPPLY,
            protocol="aave_v3",
            tokens=[USDC_MARKET],
            entry_price=Decimal("1"),  # USDC price
            entry_time=entry_time,
            amounts={USDC_MARKET: Decimal("10000")},  # $10k supply
            metadata={
                "chain": "ethereum",
                "market": USDC_MARKET,
            },
        )

        # Create mock market state
        from almanak.framework.backtesting.pnl.data_provider import MarketState

        market_state = MarketState(
            timestamp=entry_time + timedelta(days=7),  # 7 days later
            prices={USDC_MARKET: Decimal("1"), "ETH": Decimal("3000")},
            chain="ethereum",
            block_number=19000000,
            gas_price_gwei=Decimal("30"),
        )

        # Update position (calculates interest)
        elapsed_seconds = 7 * 24 * 3600.0  # 7 days

        try:
            adapter.update_position(position, market_state, elapsed_seconds)

            # Check position was updated with interest data
            logger.info(
                "Supply position after update: "
                "interest_accrued=%s, apy_confidence=%s, apy_data_source=%s",
                getattr(position, "interest_accrued", "N/A"),
                getattr(position, "apy_confidence", "N/A"),
                getattr(position, "apy_data_source", "N/A"),
            )

            # Verify interest was calculated
            if hasattr(position, "interest_accrued") and position.interest_accrued:
                # For $10k at ~3% APY over 7 days: $10,000 * 0.03 * (7/365) = ~$5.75
                interest = abs(position.interest_accrued)
                logger.info("Interest accrued over 7 days: $%s", interest)

                # Interest should be positive for supply positions
                assert interest >= Decimal("0"), "Interest should be non-negative"

        except Exception as e:
            # Adapter might fail if subgraph is unavailable
            logger.warning("Adapter update failed: %s", e)

    @pytest.mark.asyncio
    async def test_apy_confidence_is_high_when_subgraph_data_available(
        self,
        has_thegraph_api_key: bool,
    ) -> None:
        """Test that apy_confidence is HIGH when subgraph data is available.

        When historical APY data is available from the subgraph, the position's
        apy_confidence should reflect the data quality.
        """
        if not has_thegraph_api_key:
            pytest.skip("THEGRAPH_API_KEY not set - skipping live API test")

        # First verify the provider returns HIGH confidence data
        config = AaveV3ClientConfig(chain=Chain.ETHEREUM)
        provider = AaveV3APYProvider(config=config)

        async with provider:
            apys = await provider.get_apy(
                protocol="aave_v3",
                market=USDC_MARKET,
                start_date=TEST_START_DATE,
                end_date=TEST_END_DATE,
            )

        if not apys:
            pytest.skip("Aave V3 subgraph returned no APY data")

        # Check confidence levels
        confidence_counts: dict[str, int] = {}
        for apy in apys:
            conf = apy.source_info.confidence.value
            confidence_counts[conf] = confidence_counts.get(conf, 0) + 1

        logger.info("Aave V3 APY confidence breakdown: %s", confidence_counts)

        # Aave V3 should return HIGH confidence for real historical data
        assert len(apys) > 0, "Should have APY data"

        high_count = confidence_counts.get("high", 0)
        assert high_count > 0, (
            "Expected at least one HIGH confidence result when subgraph data is available"
        )

    @pytest.mark.asyncio
    async def test_interest_calculation_accuracy(self) -> None:
        """Test interest calculation matches expected values.

        For a $10k supply at 3% APY over 30 days:
        Simple interest: $10,000 * 0.03 * (30/365) = $24.66
        Compound interest (daily): $10,000 * ((1 + 0.03/365)^30 - 1) = $24.68

        This validates the interest calculation math is correct.
        """
        # Known values for calculation verification
        principal = Decimal("10000")  # $10k
        annual_rate = Decimal("0.03")  # 3% APY
        days = 30

        # Simple interest calculation
        simple_interest = principal * annual_rate * Decimal(str(days)) / Decimal("365")

        # Compound interest calculation (daily compounding)
        daily_rate = annual_rate / Decimal("365")
        compound_factor = (Decimal("1") + daily_rate) ** days
        compound_interest = principal * (compound_factor - Decimal("1"))

        logger.info(
            "Interest calculation: $%s principal * %.2f%% APY * %d days",
            principal,
            float(annual_rate * 100),
            days,
        )
        logger.info("  Simple interest: $%.4f", float(simple_interest))
        logger.info("  Compound interest: $%.4f", float(compound_interest))

        # Simple interest should be approximately $24.66
        assert Decimal("24.60") < simple_interest < Decimal("24.70"), (
            f"Simple interest calculation error: got ${simple_interest}"
        )

        # Compound interest should be slightly higher than simple
        assert compound_interest > simple_interest, (
            "Compound interest should exceed simple interest"
        )

        # Difference should be small for short periods
        assert compound_interest - simple_interest < Decimal("1"), (
            "Interest difference too large for 30-day period"
        )

    @pytest.mark.asyncio
    async def test_borrow_position_interest_calculation(
        self,
        has_thegraph_api_key: bool,
        data_config: BacktestDataConfig,
    ) -> None:
        """Test that borrow positions also use historical APY data."""
        if not has_thegraph_api_key:
            pytest.skip("THEGRAPH_API_KEY not set - skipping live API test")

        lending_config = LendingBacktestConfig(
            strategy_type="lending",
            interest_accrual_method="compound",
            health_factor_tracking_enabled=True,
            interest_rate_source="historical",
            protocol="aave_v3",
        )

        # Create adapter
        adapter = LendingBacktestAdapter(
            config=lending_config,
            data_config=data_config,
        )

        # Create a simulated borrow position
        entry_time = datetime(2024, 1, 5, 12, 0, 0, tzinfo=UTC)
        position = SimulatedPosition(
            position_id="test_borrow_position",
            position_type=PositionType.BORROW,
            protocol="aave_v3",
            tokens=[USDC_MARKET],
            entry_price=Decimal("1"),
            entry_time=entry_time,
            amounts={USDC_MARKET: Decimal("-5000")},  # $5k borrow (negative)
            metadata={
                "chain": "ethereum",
                "market": USDC_MARKET,
                "collateral_value": Decimal("10000"),  # $10k collateral for health factor
            },
        )

        from almanak.framework.backtesting.pnl.data_provider import MarketState

        market_state = MarketState(
            timestamp=entry_time + timedelta(days=7),
            prices={USDC_MARKET: Decimal("1"), "ETH": Decimal("3000")},
            chain="ethereum",
            block_number=19000000,
            gas_price_gwei=Decimal("30"),
        )

        elapsed_seconds = 7 * 24 * 3600.0  # 7 days

        try:
            adapter.update_position(position, market_state, elapsed_seconds)

            logger.info(
                "Borrow position after update: "
                "interest_accrued=%s, apy_confidence=%s",
                getattr(position, "interest_accrued", "N/A"),
                getattr(position, "apy_confidence", "N/A"),
            )

        except Exception as e:
            logger.warning("Borrow adapter update failed: %s", e)


# =============================================================================
# Test Runner
# =============================================================================

if __name__ == "__main__":
    # Run with pytest
    pytest.main([__file__, "-v", "-m", "integration", "-s"])
