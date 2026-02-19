"""Unit tests for LiquidityDepthProvider.

Tests the historical liquidity depth provider functionality with mocked
subgraph responses. Covers V3-style pools, V2-style pools, Liquidity Book,
Balancer, and Curve protocols.
"""

import pytest
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

from almanak.core.enums import Chain, Protocol
from almanak.framework.backtesting.pnl.providers.liquidity_depth import (
    DATA_SOURCE_AERODROME,
    DATA_SOURCE_BALANCER,
    DATA_SOURCE_CURVE,
    DATA_SOURCE_FALLBACK,
    DATA_SOURCE_PANCAKESWAP_V3,
    DATA_SOURCE_SUSHISWAP_V3,
    DATA_SOURCE_TRADERJOE_V2,
    DATA_SOURCE_UNISWAP_V3,
    DEFAULT_TWAP_WINDOW_HOURS,
    LIQUIDITY_BOOK_PROTOCOLS,
    STABLESWAP_PROTOCOLS,
    SUPPORTED_CHAINS,
    V2_PROTOCOLS,
    V3_PROTOCOLS,
    WEIGHTED_POOL_PROTOCOLS,
    LiquidityDepthProvider,
)
from almanak.framework.backtesting.pnl.types import DataConfidence


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_subgraph_client():
    """Create a mock SubgraphClient."""
    client = MagicMock()
    client.query = AsyncMock()
    client.close = AsyncMock()
    return client


@pytest.fixture
def provider(mock_subgraph_client):
    """Create a LiquidityDepthProvider with mocked client."""
    return LiquidityDepthProvider(
        client=mock_subgraph_client,
        fallback_depth=Decimal("0"),
    )


@pytest.fixture
def provider_with_twap(mock_subgraph_client):
    """Create a LiquidityDepthProvider with TWAP enabled."""
    return LiquidityDepthProvider(
        client=mock_subgraph_client,
        fallback_depth=Decimal("0"),
        use_twap=True,
        twap_window_hours=24,
    )


# =============================================================================
# Test Constants
# =============================================================================


class TestConstants:
    """Test provider constants."""

    def test_data_sources_defined(self):
        """Test that data source constants are defined."""
        assert DATA_SOURCE_UNISWAP_V3 == "uniswap_v3_subgraph"
        assert DATA_SOURCE_SUSHISWAP_V3 == "sushiswap_v3_subgraph"
        assert DATA_SOURCE_PANCAKESWAP_V3 == "pancakeswap_v3_subgraph"
        assert DATA_SOURCE_AERODROME == "aerodrome_subgraph"
        assert DATA_SOURCE_TRADERJOE_V2 == "traderjoe_v2_subgraph"
        assert DATA_SOURCE_CURVE == "curve_subgraph"
        assert DATA_SOURCE_BALANCER == "balancer_subgraph"
        assert DATA_SOURCE_FALLBACK == "liquidity_fallback"

    def test_protocol_lists_defined(self):
        """Test that protocol lists are defined."""
        assert "uniswap_v3" in V3_PROTOCOLS
        assert "sushiswap_v3" in V3_PROTOCOLS
        assert "pancakeswap_v3" in V3_PROTOCOLS
        assert "aerodrome" in V2_PROTOCOLS
        assert "traderjoe_v2" in LIQUIDITY_BOOK_PROTOCOLS
        assert "balancer" in WEIGHTED_POOL_PROTOCOLS
        assert "curve" in STABLESWAP_PROTOCOLS

    def test_supported_chains(self):
        """Test that supported chains include major EVM chains."""
        assert Chain.ETHEREUM in SUPPORTED_CHAINS
        assert Chain.ARBITRUM in SUPPORTED_CHAINS
        assert Chain.BASE in SUPPORTED_CHAINS
        assert Chain.OPTIMISM in SUPPORTED_CHAINS
        assert Chain.POLYGON in SUPPORTED_CHAINS
        assert Chain.AVALANCHE in SUPPORTED_CHAINS
        assert Chain.BSC in SUPPORTED_CHAINS

    def test_default_twap_window(self):
        """Test default TWAP window."""
        assert DEFAULT_TWAP_WINDOW_HOURS == 24


# =============================================================================
# Test Initialization
# =============================================================================


class TestInitialization:
    """Test provider initialization."""

    def test_init_with_client(self, mock_subgraph_client):
        """Test initialization with provided client."""
        provider = LiquidityDepthProvider(client=mock_subgraph_client)
        assert provider._client == mock_subgraph_client
        assert provider._owns_client is False

    def test_init_without_client(self):
        """Test initialization without client creates one."""
        with patch(
            "almanak.framework.backtesting.pnl.providers.liquidity_depth.SubgraphClient"
        ) as mock_class:
            mock_class.return_value = MagicMock()
            provider = LiquidityDepthProvider()
            assert provider._owns_client is True
            mock_class.assert_called_once()

    def test_init_with_fallback_depth(self, mock_subgraph_client):
        """Test initialization with custom fallback depth."""
        provider = LiquidityDepthProvider(
            client=mock_subgraph_client,
            fallback_depth=Decimal("1000000"),
        )
        assert provider._fallback_depth == Decimal("1000000")

    def test_init_with_twap_enabled(self, mock_subgraph_client):
        """Test initialization with TWAP enabled."""
        provider = LiquidityDepthProvider(
            client=mock_subgraph_client,
            use_twap=True,
            twap_window_hours=48,
        )
        assert provider._use_twap is True
        assert provider._twap_window_hours == 48

    def test_supported_chains_property(self, provider):
        """Test supported_chains property returns copy."""
        chains = provider.supported_chains
        assert chains == SUPPORTED_CHAINS
        # Verify it's a copy
        chains.append(Chain.SONIC)
        assert Chain.SONIC not in provider.supported_chains


# =============================================================================
# Test Context Manager
# =============================================================================


class TestContextManager:
    """Test async context manager functionality."""

    @pytest.mark.asyncio
    async def test_context_manager_entry(self, provider):
        """Test entering context manager returns provider."""
        async with provider as p:
            assert p is provider

    @pytest.mark.asyncio
    async def test_context_manager_closes_owned_client(self, mock_subgraph_client):
        """Test context manager closes owned client."""
        with patch(
            "almanak.framework.backtesting.pnl.providers.liquidity_depth.SubgraphClient"
        ) as mock_class:
            mock_instance = MagicMock()
            mock_instance.close = AsyncMock()
            mock_class.return_value = mock_instance

            provider = LiquidityDepthProvider()
            async with provider:
                pass

            mock_instance.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager_does_not_close_provided_client(
        self, provider, mock_subgraph_client
    ):
        """Test context manager doesn't close provided client."""
        async with provider:
            pass
        # Should not be called since client was provided
        mock_subgraph_client.close.assert_not_called()


# =============================================================================
# Test Protocol Detection
# =============================================================================


class TestProtocolDetection:
    """Test protocol detection from chain."""

    def test_detect_base_chain_defaults_to_aerodrome(self, provider):
        """Test Base chain defaults to Aerodrome."""
        protocol = provider._detect_protocol_from_chain(Chain.BASE)
        assert protocol == "aerodrome"

    def test_detect_avalanche_defaults_to_traderjoe(self, provider):
        """Test Avalanche chain defaults to TraderJoe V2."""
        protocol = provider._detect_protocol_from_chain(Chain.AVALANCHE)
        assert protocol == "traderjoe_v2"

    def test_detect_ethereum_defaults_to_uniswap_v3(self, provider):
        """Test Ethereum defaults to Uniswap V3."""
        protocol = provider._detect_protocol_from_chain(Chain.ETHEREUM)
        assert protocol == "uniswap_v3"

    def test_detect_arbitrum_defaults_to_uniswap_v3(self, provider):
        """Test Arbitrum defaults to Uniswap V3."""
        protocol = provider._detect_protocol_from_chain(Chain.ARBITRUM)
        assert protocol == "uniswap_v3"


# =============================================================================
# Test V3 Liquidity Query
# =============================================================================


class TestV3LiquidityQuery:
    """Test V3-style subgraph queries."""

    @pytest.mark.asyncio
    async def test_query_uniswap_v3_liquidity(self, provider, mock_subgraph_client):
        """Test querying Uniswap V3 liquidity."""
        # Mock response
        mock_subgraph_client.query.return_value = {
            "poolDayDatas": [
                {
                    "id": "0x123-19723",
                    "date": 1704067200,  # Jan 1, 2024
                    "tvlUSD": "5000000",
                    "liquidity": "1000000000000000000",
                },
            ]
        }

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await provider.get_liquidity_depth(
            pool_address="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
            chain=Chain.ARBITRUM,
            timestamp=timestamp,
            protocol=Protocol.UNISWAP_V3,
        )

        assert result.depth == Decimal("5000000")
        assert result.source_info.confidence == DataConfidence.HIGH
        assert result.source_info.source == DATA_SOURCE_UNISWAP_V3

    @pytest.mark.asyncio
    async def test_query_sushiswap_v3_liquidity(self, provider, mock_subgraph_client):
        """Test querying SushiSwap V3 liquidity."""
        mock_subgraph_client.query.return_value = {
            "poolDayDatas": [
                {
                    "id": "0x456-19723",
                    "date": 1704067200,
                    "tvlUSD": "2000000",
                    "liquidity": "500000000000000000",
                },
            ]
        }

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await provider.get_liquidity_depth(
            pool_address="0x456",
            chain=Chain.ETHEREUM,
            timestamp=timestamp,
            protocol="sushiswap_v3",
        )

        assert result.depth == Decimal("2000000")
        assert result.source_info.source == DATA_SOURCE_SUSHISWAP_V3

    @pytest.mark.asyncio
    async def test_query_pancakeswap_v3_liquidity(self, provider, mock_subgraph_client):
        """Test querying PancakeSwap V3 liquidity."""
        mock_subgraph_client.query.return_value = {
            "poolDayDatas": [
                {
                    "id": "0x789-19723",
                    "date": 1704067200,
                    "tvlUSD": "10000000",
                    "liquidity": "2000000000000000000",
                },
            ]
        }

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await provider.get_liquidity_depth(
            pool_address="0x789",
            chain=Chain.BSC,
            timestamp=timestamp,
            protocol=Protocol.PANCAKESWAP_V3,
        )

        assert result.depth == Decimal("10000000")
        assert result.source_info.source == DATA_SOURCE_PANCAKESWAP_V3


# =============================================================================
# Test V2 Liquidity Query
# =============================================================================


class TestV2LiquidityQuery:
    """Test V2/Solidly-style subgraph queries."""

    @pytest.mark.asyncio
    async def test_query_aerodrome_liquidity(self, provider, mock_subgraph_client):
        """Test querying Aerodrome liquidity."""
        mock_subgraph_client.query.return_value = {
            "pairDayDatas": [
                {
                    "id": "0xaero-19723",
                    "date": 1704067200,
                    "reserveUSD": "8000000",
                    "dailyVolumeUSD": "1000000",
                },
            ]
        }

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await provider.get_liquidity_depth(
            pool_address="0xaero",
            chain=Chain.BASE,
            timestamp=timestamp,
            protocol=Protocol.AERODROME,
        )

        assert result.depth == Decimal("8000000")
        assert result.source_info.source == DATA_SOURCE_AERODROME
        assert result.source_info.confidence == DataConfidence.HIGH


# =============================================================================
# Test Liquidity Book Query
# =============================================================================


class TestLiquidityBookQuery:
    """Test TraderJoe V2 Liquidity Book queries."""

    @pytest.mark.asyncio
    async def test_query_traderjoe_liquidity(self, provider, mock_subgraph_client):
        """Test querying TraderJoe V2 Liquidity Book liquidity."""
        mock_subgraph_client.query.return_value = {
            "lbPairDayDatas": [
                {
                    "id": "0xtjoe-19723",
                    "date": 1704067200,
                    "totalValueLockedUSD": "3000000",
                    "volumeUSD": "500000",
                },
            ]
        }

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await provider.get_liquidity_depth(
            pool_address="0xtjoe",
            chain=Chain.AVALANCHE,
            timestamp=timestamp,
            protocol=Protocol.TRADERJOE_V2,
        )

        assert result.depth == Decimal("3000000")
        assert result.source_info.source == DATA_SOURCE_TRADERJOE_V2
        assert result.source_info.confidence == DataConfidence.HIGH


# =============================================================================
# Test Balancer Query
# =============================================================================


class TestBalancerQuery:
    """Test Balancer subgraph queries."""

    @pytest.mark.asyncio
    async def test_query_balancer_liquidity(self, provider, mock_subgraph_client):
        """Test querying Balancer liquidity."""
        mock_subgraph_client.query.return_value = {
            "poolSnapshots": [
                {
                    "id": "0xbal-1704067200",
                    "timestamp": 1704067200,
                    "liquidity": "15000000",
                    "swapVolume": "2000000",
                    "swapFees": "6000",
                },
            ]
        }

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await provider.get_liquidity_depth(
            pool_address="0xbal",
            chain=Chain.ETHEREUM,
            timestamp=timestamp,
            protocol="balancer",
        )

        assert result.depth == Decimal("15000000")
        assert result.source_info.source == DATA_SOURCE_BALANCER
        assert result.source_info.confidence == DataConfidence.HIGH


# =============================================================================
# Test Curve Query
# =============================================================================


class TestCurveQuery:
    """Test Curve (Messari schema) subgraph queries."""

    @pytest.mark.asyncio
    async def test_query_curve_liquidity(self, provider, mock_subgraph_client):
        """Test querying Curve liquidity."""
        mock_subgraph_client.query.return_value = {
            "liquidityPoolDailySnapshots": [
                {
                    "id": "0xcurve-19723",
                    "day": 19723,  # Jan 1, 2024
                    "totalValueLockedUSD": "100000000",
                    "dailyVolumeUSD": "5000000",
                },
            ]
        }

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await provider.get_liquidity_depth(
            pool_address="0xcurve",
            chain=Chain.ETHEREUM,
            timestamp=timestamp,
            protocol="curve",
        )

        assert result.depth == Decimal("100000000")
        assert result.source_info.source == DATA_SOURCE_CURVE
        assert result.source_info.confidence == DataConfidence.HIGH


# =============================================================================
# Test TWAP Calculation
# =============================================================================


class TestTWAPCalculation:
    """Test time-weighted average depth calculation."""

    @pytest.mark.asyncio
    async def test_twap_with_multiple_data_points(
        self, provider_with_twap, mock_subgraph_client
    ):
        """Test TWAP calculation with multiple data points."""
        # Multiple data points within the 24-hour TWAP window
        # Target timestamp is Jan 2, 2024 at 12:00 UTC
        # TWAP window is 24 hours, so window starts Jan 1, 2024 at 12:00 UTC
        mock_subgraph_client.query.return_value = {
            "poolDayDatas": [
                {
                    "id": "0x123-day1",
                    "date": 1704110400,  # Jan 1, 2024 12:00 UTC (in window)
                    "tvlUSD": "4000000",
                    "liquidity": "1000000000000000000",
                },
                {
                    "id": "0x123-day2",
                    "date": 1704153600,  # Jan 2, 2024 00:00 UTC (in window)
                    "tvlUSD": "6000000",
                    "liquidity": "1000000000000000000",
                },
            ]
        }

        timestamp = datetime(2024, 1, 2, 12, 0, tzinfo=UTC)
        result = await provider_with_twap.get_liquidity_depth(
            pool_address="0x123",
            chain=Chain.ARBITRUM,
            timestamp=timestamp,
            protocol=Protocol.UNISWAP_V3,
        )

        # TWAP should be between the two values (time-weighted average)
        assert result.depth >= Decimal("4000000")
        assert result.depth <= Decimal("6000000")
        assert result.source_info.confidence == DataConfidence.HIGH

    @pytest.mark.asyncio
    async def test_twap_with_single_data_point(
        self, provider_with_twap, mock_subgraph_client
    ):
        """Test TWAP with single data point returns that value."""
        mock_subgraph_client.query.return_value = {
            "poolDayDatas": [
                {
                    "id": "0x123-day0",
                    "date": 1704067200,
                    "tvlUSD": "5000000",
                    "liquidity": "1000000000000000000",
                },
            ]
        }

        timestamp = datetime(2024, 1, 2, 12, 0, tzinfo=UTC)
        result = await provider_with_twap.get_liquidity_depth(
            pool_address="0x123",
            chain=Chain.ARBITRUM,
            timestamp=timestamp,
            protocol=Protocol.UNISWAP_V3,
        )

        # Should return the single data point value
        assert result.depth == Decimal("5000000")


# =============================================================================
# Test Fallback Behavior
# =============================================================================


class TestFallbackBehavior:
    """Test fallback behavior when data is unavailable."""

    @pytest.mark.asyncio
    async def test_fallback_when_no_data(self, provider, mock_subgraph_client):
        """Test fallback when subgraph returns no data."""
        mock_subgraph_client.query.return_value = {"poolDayDatas": []}

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await provider.get_liquidity_depth(
            pool_address="0xnone",
            chain=Chain.ARBITRUM,
            timestamp=timestamp,
            protocol=Protocol.UNISWAP_V3,
        )

        assert result.depth == Decimal("0")
        assert result.source_info.confidence == DataConfidence.LOW
        assert result.source_info.source == DATA_SOURCE_FALLBACK

    @pytest.mark.asyncio
    async def test_fallback_when_chain_not_supported(self, provider, mock_subgraph_client):
        """Test fallback when chain is not supported by protocol."""
        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await provider.get_liquidity_depth(
            pool_address="0xtest",
            chain=Chain.SONIC,  # Not supported
            timestamp=timestamp,
            protocol=Protocol.UNISWAP_V3,
        )

        assert result.depth == Decimal("0")
        assert result.source_info.confidence == DataConfidence.LOW
        assert result.source_info.source == DATA_SOURCE_FALLBACK

    @pytest.mark.asyncio
    async def test_fallback_when_protocol_not_specified(self, provider, mock_subgraph_client):
        """Test protocol auto-detection when not specified."""
        mock_subgraph_client.query.return_value = {
            "poolDayDatas": [
                {
                    "id": "0x123-19723",
                    "date": 1704067200,
                    "tvlUSD": "5000000",
                    "liquidity": "1000000000000000000",
                },
            ]
        }

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await provider.get_liquidity_depth(
            pool_address="0x123",
            chain=Chain.ARBITRUM,
            timestamp=timestamp,
            # No protocol specified - should auto-detect Uniswap V3
        )

        assert result.depth == Decimal("5000000")
        assert result.source_info.confidence == DataConfidence.HIGH

    @pytest.mark.asyncio
    async def test_custom_fallback_depth(self, mock_subgraph_client):
        """Test custom fallback depth value."""
        provider = LiquidityDepthProvider(
            client=mock_subgraph_client,
            fallback_depth=Decimal("1000000"),
        )
        mock_subgraph_client.query.return_value = {"poolDayDatas": []}

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await provider.get_liquidity_depth(
            pool_address="0xnone",
            chain=Chain.ARBITRUM,
            timestamp=timestamp,
            protocol=Protocol.UNISWAP_V3,
        )

        assert result.depth == Decimal("1000000")
        assert result.source_info.confidence == DataConfidence.LOW


# =============================================================================
# Test Error Handling
# =============================================================================


class TestErrorHandling:
    """Test error handling."""

    @pytest.mark.asyncio
    async def test_handles_subgraph_error(self, provider, mock_subgraph_client):
        """Test handling of subgraph query error."""
        from almanak.framework.backtesting.pnl.providers.subgraph_client import (
            SubgraphQueryError,
        )

        mock_subgraph_client.query.side_effect = SubgraphQueryError("Query failed")

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await provider.get_liquidity_depth(
            pool_address="0x123",
            chain=Chain.ARBITRUM,
            timestamp=timestamp,
            protocol=Protocol.UNISWAP_V3,
        )

        assert result.depth == Decimal("0")
        assert result.source_info.confidence == DataConfidence.LOW
        assert result.source_info.source == DATA_SOURCE_FALLBACK

    @pytest.mark.asyncio
    async def test_handles_rate_limit_error(self, provider, mock_subgraph_client):
        """Test handling of rate limit error."""
        from almanak.framework.backtesting.pnl.providers.subgraph_client import (
            SubgraphRateLimitError,
        )

        mock_subgraph_client.query.side_effect = SubgraphRateLimitError()

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await provider.get_liquidity_depth(
            pool_address="0x123",
            chain=Chain.ARBITRUM,
            timestamp=timestamp,
            protocol=Protocol.UNISWAP_V3,
        )

        assert result.depth == Decimal("0")
        assert result.source_info.confidence == DataConfidence.LOW

    @pytest.mark.asyncio
    async def test_handles_unexpected_error(self, provider, mock_subgraph_client):
        """Test handling of unexpected errors."""
        mock_subgraph_client.query.side_effect = Exception("Unexpected error")

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await provider.get_liquidity_depth(
            pool_address="0x123",
            chain=Chain.ARBITRUM,
            timestamp=timestamp,
            protocol=Protocol.UNISWAP_V3,
        )

        assert result.depth == Decimal("0")
        assert result.source_info.confidence == DataConfidence.LOW


# =============================================================================
# Test Range Query
# =============================================================================


class TestRangeQuery:
    """Test get_liquidity_depth_range method."""

    @pytest.mark.asyncio
    async def test_range_query_returns_multiple_results(
        self, provider, mock_subgraph_client
    ):
        """Test range query returns results for each day."""
        mock_subgraph_client.query.return_value = {
            "poolDayDatas": [
                {
                    "id": "0x123-19723",
                    "date": 1704067200,
                    "tvlUSD": "5000000",
                    "liquidity": "1000000000000000000",
                },
            ]
        }

        start_time = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
        end_time = datetime(2024, 1, 3, 12, 0, tzinfo=UTC)

        results = await provider.get_liquidity_depth_range(
            pool_address="0x123",
            chain=Chain.ARBITRUM,
            start_time=start_time,
            end_time=end_time,
            protocol=Protocol.UNISWAP_V3,
        )

        # Should have 3 results (Jan 1, 2, 3)
        assert len(results) == 3
        for result in results:
            assert result.depth == Decimal("5000000")
            assert result.source_info.confidence == DataConfidence.HIGH


# =============================================================================
# Test Protocol ID Normalization
# =============================================================================


class TestProtocolNormalization:
    """Test protocol ID normalization."""

    def test_protocol_enum_to_string(self, provider):
        """Test Protocol enum is converted to lowercase string."""
        assert provider._get_protocol_id(Protocol.UNISWAP_V3) == "uniswap_v3"
        assert provider._get_protocol_id(Protocol.AERODROME) == "aerodrome"

    def test_string_to_lowercase(self, provider):
        """Test string protocols are lowercased."""
        assert provider._get_protocol_id("CURVE") == "curve"
        assert provider._get_protocol_id("Balancer") == "balancer"

    def test_none_returns_none(self, provider):
        """Test None protocol returns None."""
        assert provider._get_protocol_id(None) is None


# =============================================================================
# Test Timestamp Handling
# =============================================================================


class TestTimestampHandling:
    """Test timestamp handling."""

    @pytest.mark.asyncio
    async def test_naive_timestamp_gets_utc(self, provider, mock_subgraph_client):
        """Test naive timestamps are converted to UTC."""
        mock_subgraph_client.query.return_value = {
            "poolDayDatas": [
                {
                    "id": "0x123-19723",
                    "date": 1704067200,
                    "tvlUSD": "5000000",
                    "liquidity": "1000000000000000000",
                },
            ]
        }

        # Naive timestamp (no timezone)
        naive_timestamp = datetime(2024, 1, 15, 12, 0)
        result = await provider.get_liquidity_depth(
            pool_address="0x123",
            chain=Chain.ARBITRUM,
            timestamp=naive_timestamp,
            protocol=Protocol.UNISWAP_V3,
        )

        # Result timestamp should have UTC
        assert result.source_info.timestamp.tzinfo is not None
