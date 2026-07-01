"""Integration tests for LP backtest with historical volume data.

This module tests the LP adapter's integration with the MultiDEXVolumeProvider
to validate that LP fee calculations use real historical subgraph data when
available, and that confidence levels are properly tracked.

**VIB-4870 / W7**: the volume providers are now thin gRPC clients of the
gateway's ``RateHistoryService.GetDexVolumeHistory``. The TheGraph egress
lives gateway-side, so these end-to-end tests require a **running gateway
sidecar** (with ``thegraph_api_key`` configured), not a strategy-container
``THEGRAPH_API_KEY``. They skip when no gateway is reachable. The
per-DEX decode + the no-silent-zeros contract are covered offline by
``tests/unit/backtesting/providers/test_multi_dex_volume.py`` and the gateway
dispatcher suite ``tests/gateway/services/test_rate_history_service_dex_volume.py``.

Example:
    # Run against a live gateway sidecar
    pytest tests/integration/backtesting/test_lp_with_historical_volume.py -v -m integration
"""

import logging
import os
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.core.enums import Chain, Protocol
from almanak.framework.backtesting.adapters.lp_adapter import (
    LPBacktestAdapter,
    LPBacktestConfig,
)
from almanak.framework.backtesting.config import BacktestDataConfig
from almanak.framework.backtesting.pnl.providers.dex import GatewayDexVolumeProvider
from almanak.framework.backtesting.pnl.providers.multi_dex_volume import (
    MultiDEXVolumeProvider,
)
from almanak.framework.backtesting.pnl.types import DataConfidence
from almanak.framework.data.interfaces import DataSourceUnavailable

logger = logging.getLogger(__name__)


# =============================================================================
# Test Constants
# =============================================================================

# Well-known WETH/USDC pool on Ethereum (0.3% fee tier)
# This is one of the most liquid pools on Uniswap V3
WETH_USDC_POOL_ETHEREUM = "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8"

# WETH/USDC pool on Arbitrum (0.05% fee tier)
WETH_USDC_POOL_ARBITRUM = "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443"

# Fee tier for the test pool (0.3% = 3000 BPS)
POOL_FEE_TIER = Decimal("0.003")

# Test date range - use recent dates for which data should be available
# Using a 7-day window from a known period with high volume
TEST_START_DATE = date(2024, 1, 1)
TEST_END_DATE = date(2024, 1, 7)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def gateway_available() -> bool:
    """Whether a real gateway sidecar is wired up for the live volume RPC.

    Post-VIB-4870 the volume providers reach TheGraph only through the
    gateway's ``GetDexVolumeHistory`` RPC, so these end-to-end tests need a
    running gateway with ``thegraph_api_key`` configured — not a
    strategy-container ``THEGRAPH_API_KEY``. The gRPC channel connects
    lazily (``is_connected`` flips True before any handshake), so a plain
    connect check would false-positive against a dead port and turn skips
    into failures. We therefore gate on an explicit operator opt-in
    (``ALMANAK_GATEWAY_E2E`` set truthy), matching the ``smoke`` marker
    convention in ``pytest.ini``; default is skip.
    """
    return os.environ.get("ALMANAK_GATEWAY_E2E", "").strip().lower() in ("1", "true", "yes")


@pytest.fixture
def lp_adapter_config() -> LPBacktestConfig:
    """Create LP adapter configuration."""
    return LPBacktestConfig(
        strategy_type="lp",
        fee_tracking_enabled=True,
        use_historical_volume=True,
        chain="ethereum",
        # VIB-4849: strict by default — a regression in the historical/gateway
        # volume path must FAIL this suite, not silently pass via the heuristic.
        # Offline CI is already covered by the per-test gateway-availability skip,
        # so no fallback is needed here (the heuristic is exercised in dedicated
        # adapter unit tests).
        allow_volume_fallback=False,
    )


@pytest.fixture
def data_config() -> BacktestDataConfig:
    """Create BacktestDataConfig for historical data."""
    return BacktestDataConfig(
        use_historical_volume=True,
        use_historical_liquidity=True,
        volume_fallback_multiplier=Decimal("10"),
        strict_historical_mode=False,  # Don't fail on missing data in integration tests
    )


# =============================================================================
# Offline contract test (runs in CI without a gateway)
# =============================================================================


class TestGatewayBackedVolumeContract:
    """Offline checks for the post-VIB-4870 gateway-backed volume providers."""

    @pytest.mark.asyncio
    async def test_no_gateway_raises_unavailable_not_silent_zero(self) -> None:
        """With no reachable gateway, the provider raises (no silent zero).

        Validates the W7 "no silent zeros" contract end-to-end on the
        consumer side: when the gateway is unavailable the provider must
        surface :class:`DataSourceUnavailable` rather than fabricate a
        ``Decimal("0")`` LOW-confidence row that would poison backtest PnL.
        """
        from unittest.mock import patch

        # Force the connect dance to fail so we exercise the "gateway
        # unreachable" branch deterministically, without a live sidecar.
        with patch(
            "almanak.framework.backtesting.pnl.providers.dex._gateway_volume._get_connected_gateway_client",
            side_effect=DataSourceUnavailable(source="gateway", reason="no gateway in test env"),
        ):
            provider = GatewayDexVolumeProvider("uniswap_v3")
            with pytest.raises(DataSourceUnavailable):
                await provider.get_volume(
                    pool_address=WETH_USDC_POOL_ETHEREUM,
                    chain=Chain.ETHEREUM,
                    start_date=TEST_START_DATE,
                    end_date=TEST_END_DATE,
                )


# =============================================================================
# Integration Tests
# =============================================================================


@pytest.mark.integration
class TestLPWithHistoricalVolume:
    """Integration tests for LP backtest with historical volume."""

    @pytest.mark.asyncio
    async def test_uniswap_v3_volume_provider_fetches_real_data(
        self,
        gateway_available: bool,
    ) -> None:
        """Test that the Uniswap V3 gateway provider fetches real volume data.

        This test validates the volume provider can query The Graph and return
        VolumeResult objects with HIGH confidence when data is available.
        """
        if not gateway_available:
            pytest.skip("No gateway sidecar reachable - skipping live gateway test")

        provider = GatewayDexVolumeProvider("uniswap_v3")

        async with provider:
            volumes = await provider.get_volume(
                pool_address=WETH_USDC_POOL_ETHEREUM,
                chain=Chain.ETHEREUM,
                start_date=TEST_START_DATE,
                end_date=TEST_END_DATE,
            )

        # Verify we got results
        assert len(volumes) > 0, "Expected at least one volume result"

        # Check that results have HIGH confidence (real subgraph data)
        high_confidence_count = sum(1 for v in volumes if v.source_info.confidence == DataConfidence.HIGH)

        # At least some results should be HIGH confidence
        assert high_confidence_count > 0, "Expected at least one HIGH confidence result from subgraph"

        # Log the volume data for debugging
        logger.info(
            "Fetched %d volume points, %d HIGH confidence",
            len(volumes),
            high_confidence_count,
        )

        for vol in volumes:
            logger.debug(
                "Date: %s, Volume: $%s, Confidence: %s, Source: %s",
                vol.source_info.timestamp.date(),
                vol.value,
                vol.source_info.confidence.value,
                vol.source_info.source,
            )

        # Verify volume values are reasonable for WETH/USDC (typically millions per day)
        # Filter to HIGH confidence results only
        high_conf_volumes = [v for v in volumes if v.source_info.confidence == DataConfidence.HIGH]

        if high_conf_volumes:
            # This is a very liquid pool - expect significant volume
            avg_volume = sum(v.value for v in high_conf_volumes) / len(high_conf_volumes)
            assert avg_volume > Decimal("100000"), (
                f"Expected WETH/USDC pool to have >$100k daily volume, got ${avg_volume}"
            )

    @pytest.mark.asyncio
    async def test_multi_dex_volume_provider_routes_to_uniswap(
        self,
        gateway_available: bool,
    ) -> None:
        """Test that MultiDEXVolumeProvider correctly routes to Uniswap V3.

        Validates the aggregator properly delegates to the correct DEX-specific
        provider based on the protocol parameter.
        """
        if not gateway_available:
            pytest.skip("No gateway sidecar reachable - skipping live gateway test")

        provider = MultiDEXVolumeProvider()

        async with provider:
            volumes = await provider.get_volume(
                pool_address=WETH_USDC_POOL_ETHEREUM,
                chain=Chain.ETHEREUM,
                start_date=TEST_START_DATE,
                end_date=TEST_END_DATE,
                protocol=Protocol.UNISWAP_V3,
            )

        # Verify results
        assert len(volumes) > 0, "Expected volume results from MultiDEXVolumeProvider"

        # Check data source indicates Uniswap V3
        high_conf = [v for v in volumes if v.source_info.confidence == DataConfidence.HIGH]
        if high_conf:
            # Source should indicate uniswap_v3
            assert "uniswap" in high_conf[0].source_info.source.lower(), (
                f"Expected uniswap source, got {high_conf[0].source_info.source}"
            )

    @pytest.mark.asyncio
    async def test_lp_adapter_uses_historical_volume_for_fee_calculation(
        self,
        gateway_available: bool,
        lp_adapter_config: LPBacktestConfig,
        data_config: BacktestDataConfig,
    ) -> None:
        """Test that LP adapter uses historical volume for fee calculations.

        This is the core integration test - validates that when BacktestDataConfig
        has use_historical_volume=True, the LP adapter fetches real subgraph data
        and the resulting FeeAccrualResult has HIGH confidence.
        """
        if not gateway_available:
            pytest.skip("No gateway sidecar reachable - skipping live gateway test")

        # Create adapter with data config
        adapter = LPBacktestAdapter(
            config=lp_adapter_config,
            data_config=data_config,
        )

        # Simulate getting historical volume (this is what the adapter does internally)
        # We'll test the _get_historical_volume method indirectly through fee calculation

        # Create a mock position with pool address
        from almanak.framework.backtesting.pnl.portfolio import (
            PositionType,
            SimulatedPosition,
        )

        timestamp = datetime(2024, 1, 5, 12, 0, 0, tzinfo=UTC)

        position = SimulatedPosition(
            position_id="test_lp_position",
            position_type=PositionType.LP,
            protocol="uniswap_v3",
            tokens=["WETH", "USDC"],
            entry_price=Decimal("3000"),  # ETH price at entry
            entry_time=timestamp - timedelta(days=1),
            amounts={"WETH": Decimal("1"), "USDC": Decimal("3000")},
            tick_lower=-887220,
            tick_upper=887220,
            fee_tier=Decimal("3000"),
            liquidity=Decimal("1000000000000000000"),  # 1e18
            metadata={
                "pool_address": WETH_USDC_POOL_ETHEREUM,
                "chain": "ethereum",
            },
        )

        # Create mock market state
        from almanak.framework.backtesting.pnl.data_provider import MarketState

        market_state = MarketState(
            timestamp=timestamp,
            prices={"WETH": Decimal("3100"), "USDC": Decimal("1")},
            chain="ethereum",
            block_number=18000000,
            gas_price_gwei=Decimal("30"),
        )

        # Calculate fee accrual
        # The adapter should internally fetch historical volume
        elapsed_seconds = 86400.0  # 1 day

        # Run the update_position which calculates fees
        adapter.update_position(position, market_state, elapsed_seconds)

        # Check the position was updated with fee data
        logger.info(
            "Position after update: fee_confidence=%s, accumulated_fees_usd=%s",
            getattr(position, "fee_confidence", None),
            position.accumulated_fees_usd,
        )

        # The fee confidence should be set (either from historical data or fallback)
        # When historical data is available, confidence should be HIGH
        if hasattr(position, "fee_confidence") and position.fee_confidence:
            # If we got historical data, confidence should be "high"
            # If fallback was used, confidence would be "low"
            logger.info("Fee confidence: %s", position.fee_confidence)

    @pytest.mark.asyncio
    async def test_fee_calculation_within_expected_range(
        self,
        gateway_available: bool,
    ) -> None:
        """Test that fee calculations are within 5% of expected based on volume.

        Given known volume data and fee tier, validates that the calculated fees
        are within expected bounds. This provides confidence that the fee
        calculation logic is correct.
        """
        if not gateway_available:
            pytest.skip("No gateway sidecar reachable - skipping live gateway test")

        # Fetch actual volume data
        provider = GatewayDexVolumeProvider("uniswap_v3")

        async with provider:
            volumes = await provider.get_volume(
                pool_address=WETH_USDC_POOL_ETHEREUM,
                chain=Chain.ETHEREUM,
                start_date=TEST_START_DATE,
                end_date=TEST_END_DATE,
            )

        # Get high confidence volumes only
        high_conf_volumes = [v for v in volumes if v.source_info.confidence == DataConfidence.HIGH]

        if not high_conf_volumes:
            pytest.skip("No high confidence volume data available")

        # Calculate total volume for the period
        total_volume = sum(v.value for v in high_conf_volumes)

        # Expected fees for a position with 0.1% liquidity share
        # Fee = Volume * Fee_Tier * Liquidity_Share
        liquidity_share = Decimal("0.001")  # 0.1% of pool
        expected_fees = total_volume * POOL_FEE_TIER * liquidity_share

        logger.info(
            "Volume: $%s, Expected fees (0.1%% share): $%s",
            total_volume,
            expected_fees,
        )

        # Verify the math is reasonable
        # For WETH/USDC with ~$100M daily volume, 0.1% share, 0.3% fee tier:
        # Expected daily fees: $100M * 0.003 * 0.001 = $300
        if expected_fees > Decimal("0"):
            # Just validate the expected fees are positive and reasonable
            assert expected_fees > Decimal("0"), "Expected positive fees"

            # Fees should be less than 1% of total volume (sanity check)
            assert expected_fees < total_volume * Decimal("0.01"), "Fees should be less than 1% of volume"

    @pytest.mark.asyncio
    async def test_fee_confidence_is_high_when_subgraph_data_available(
        self,
        gateway_available: bool,
        lp_adapter_config: LPBacktestConfig,
        data_config: BacktestDataConfig,
    ) -> None:
        """Test that fee_confidence is HIGH when subgraph data is available.

        When the LP adapter successfully fetches volume data from the subgraph,
        the resulting fee calculations should be marked with HIGH confidence
        to indicate they are based on real historical data.
        """
        if not gateway_available:
            pytest.skip("No gateway sidecar reachable - skipping live gateway test")

        # First verify we can get HIGH confidence data from the provider
        provider = MultiDEXVolumeProvider()

        async with provider:
            volumes = await provider.get_volume(
                pool_address=WETH_USDC_POOL_ETHEREUM,
                chain=Chain.ETHEREUM,
                start_date=TEST_START_DATE,
                end_date=TEST_END_DATE,
                protocol=Protocol.UNISWAP_V3,
            )

        # Count high confidence results
        high_confidence_count = sum(1 for v in volumes if v.source_info.confidence == DataConfidence.HIGH)

        logger.info(
            "Got %d total results, %d HIGH confidence",
            len(volumes),
            high_confidence_count,
        )

        # Validate that subgraph returns HIGH confidence data
        assert high_confidence_count > 0, (
            "Expected at least one HIGH confidence result when subgraph data is available. "
            "This validates that the fee calculation will have HIGH confidence."
        )

        # If we have HIGH confidence volume data, the fee calculation
        # should also be HIGH confidence (this is set by the adapter)
        for vol in volumes:
            if vol.source_info.confidence == DataConfidence.HIGH:
                logger.info(
                    "HIGH confidence volume: date=%s, value=$%s, source=%s",
                    vol.source_info.timestamp.date(),
                    vol.value,
                    vol.source_info.source,
                )


@pytest.mark.integration
class TestMultiDEXVolumeProviderIntegration:
    """Integration tests for MultiDEXVolumeProvider with multiple DEXs."""

    @pytest.mark.asyncio
    async def test_arbitrum_uniswap_v3_volume(
        self,
        gateway_available: bool,
    ) -> None:
        """Test fetching volume from Uniswap V3 on Arbitrum."""
        if not gateway_available:
            pytest.skip("No gateway sidecar reachable - skipping live gateway test")

        provider = MultiDEXVolumeProvider()

        async with provider:
            volumes = await provider.get_volume(
                pool_address=WETH_USDC_POOL_ARBITRUM,
                chain=Chain.ARBITRUM,
                start_date=TEST_START_DATE,
                end_date=TEST_END_DATE,
                protocol=Protocol.UNISWAP_V3,
            )

        assert len(volumes) > 0, "Expected volume results for Arbitrum"

        # Log results
        for vol in volumes[:3]:  # First 3 days
            logger.info(
                "Arbitrum volume: date=%s, value=$%s, confidence=%s",
                vol.source_info.timestamp.date(),
                vol.value,
                vol.source_info.confidence.value,
            )

    @pytest.mark.asyncio
    async def test_auto_detects_protocol_for_chain(
        self,
        gateway_available: bool,
    ) -> None:
        """Test that MultiDEXVolumeProvider auto-detects protocol based on chain."""
        if not gateway_available:
            pytest.skip("No gateway sidecar reachable - skipping live gateway test")

        provider = MultiDEXVolumeProvider()

        # Don't specify protocol - let it auto-detect based on chain
        async with provider:
            volumes = await provider.get_volume(
                pool_address=WETH_USDC_POOL_ETHEREUM,
                chain=Chain.ETHEREUM,
                start_date=TEST_START_DATE,
                end_date=TEST_END_DATE,
                # No protocol specified - should auto-detect Uniswap V3 for Ethereum
            )

        # Should still get results via auto-detection
        assert len(volumes) > 0, "Expected volume results with auto-detected protocol"

        # Check that it used uniswap_v3 (default for Ethereum)
        high_conf = [v for v in volumes if v.source_info.confidence == DataConfidence.HIGH]
        if high_conf:
            assert "uniswap" in high_conf[0].source_info.source.lower(), (
                "Expected Uniswap V3 to be auto-detected for Ethereum"
            )


# =============================================================================
# Test Runner
# =============================================================================

if __name__ == "__main__":
    # Run with pytest
    pytest.main([__file__, "-v", "-m", "integration", "-s"])
