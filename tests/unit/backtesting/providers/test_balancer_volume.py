"""Unit tests for Balancer Volume Provider.

This module tests the BalancerVolumeProvider class in providers/dex/balancer_volume.py,
covering:
- Provider initialization and configuration
- Supported chains and subgraph ID mapping
- Volume fetching with mocked responses (PoolSnapshot entity)
- Fallback behavior when data unavailable
- Error handling for subgraph failures
"""

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.core.enums import Chain
from almanak.framework.backtesting.pnl.providers.dex.balancer_volume import (
    BALANCER_SUBGRAPH_IDS,
    DATA_SOURCE,
    SUPPORTED_CHAINS,
    BalancerVolumeProvider,
)
from almanak.framework.backtesting.pnl.providers.subgraph_client import (
    SubgraphClient,
    SubgraphQueryError,
    SubgraphRateLimitError,
)
from almanak.framework.backtesting.pnl.types import DataConfidence


class TestBalancerVolumeProviderInitialization:
    """Tests for BalancerVolumeProvider initialization."""

    def test_init_default(self):
        """Test provider initializes with default settings."""
        provider = BalancerVolumeProvider()
        assert provider.supported_chains == SUPPORTED_CHAINS
        assert provider._fallback_volume == Decimal("0")
        assert provider._owns_client is True

    def test_init_with_custom_fallback(self):
        """Test provider initializes with custom fallback volume."""
        provider = BalancerVolumeProvider(fallback_volume=Decimal("1000"))
        assert provider._fallback_volume == Decimal("1000")

    def test_init_with_custom_rate_limit(self):
        """Test provider initializes with custom rate limit."""
        provider = BalancerVolumeProvider(requests_per_minute=50)
        assert provider._client.config.requests_per_minute == 50

    def test_init_with_provided_client(self):
        """Test provider uses provided client and doesn't own it."""
        mock_client = MagicMock(spec=SubgraphClient)
        provider = BalancerVolumeProvider(client=mock_client)
        assert provider._client is mock_client
        assert provider._owns_client is False

    def test_supported_chains_property_returns_copy(self):
        """Test supported_chains returns a copy, not the original."""
        provider = BalancerVolumeProvider()
        chains1 = provider.supported_chains
        chains2 = provider.supported_chains
        assert chains1 == chains2
        assert chains1 is not chains2


class TestSupportedChains:
    """Tests for supported chains configuration."""

    def test_supported_chains_include_required_networks(self):
        """Test that required networks are supported (Ethereum, Arbitrum, Polygon)."""
        # US-013 requires: Ethereum, Arbitrum, Polygon
        assert Chain.ETHEREUM in SUPPORTED_CHAINS
        assert Chain.ARBITRUM in SUPPORTED_CHAINS
        assert Chain.POLYGON in SUPPORTED_CHAINS

    def test_all_supported_chains_have_subgraph_ids(self):
        """Test all supported chains have subgraph IDs."""
        for chain in SUPPORTED_CHAINS:
            assert chain in BALANCER_SUBGRAPH_IDS
            assert BALANCER_SUBGRAPH_IDS[chain]  # Non-empty

    def test_subgraph_ids_are_valid_format(self):
        """Test subgraph IDs are non-empty strings."""
        for chain, subgraph_id in BALANCER_SUBGRAPH_IDS.items():
            assert isinstance(subgraph_id, str)
            assert len(subgraph_id) > 10  # Reasonable length for deployment ID


class TestGetVolume:
    """Tests for get_volume method."""

    @pytest.mark.asyncio
    async def test_get_volume_success(self):
        """Test successfully fetching volume data with PoolSnapshot entity."""
        mock_client = MagicMock(spec=SubgraphClient)
        # Balancer uses poolSnapshots with timestamp and swapVolume
        mock_client.query = AsyncMock(
            return_value={
                "poolSnapshots": [
                    {
                        "id": "0x5c6ee304399dbdb9c8ef030ab642b10820db8f56-1705276800",
                        "timestamp": 1705276800,  # 2024-01-15 00:00:00 UTC
                        "swapVolume": "2500000.75",
                        "swapFees": "7500.00",
                        "liquidity": "150000000.00",
                        "totalShares": "100000.00",
                    }
                ]
            }
        )

        provider = BalancerVolumeProvider(client=mock_client)

        volumes = await provider.get_volume(
            pool_address="0x5c6Ee304399DBdB9C8Ef030aB642B10820DB8F56",
            chain=Chain.ETHEREUM,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        assert len(volumes) == 1
        assert volumes[0].value == Decimal("2500000.75")
        assert volumes[0].source_info.source == DATA_SOURCE
        assert volumes[0].source_info.confidence == DataConfidence.HIGH

        # Verify client was called with correct parameters
        mock_client.query.assert_called_once()
        call_args = mock_client.query.call_args
        assert call_args.kwargs["subgraph_id"] == BALANCER_SUBGRAPH_IDS[Chain.ETHEREUM]
        # Pool address should be lowercased
        assert call_args.kwargs["variables"]["poolId"] == "0x5c6ee304399dbdb9c8ef030ab642b10820db8f56"

    @pytest.mark.asyncio
    async def test_get_volume_multiple_days(self):
        """Test fetching volume for multiple days."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            return_value={
                "poolSnapshots": [
                    {"id": "1", "timestamp": 1705276800, "swapVolume": "1000000"},  # 2024-01-15
                    {"id": "2", "timestamp": 1705363200, "swapVolume": "1100000"},  # 2024-01-16
                    {"id": "3", "timestamp": 1705449600, "swapVolume": "1200000"},  # 2024-01-17
                ]
            }
        )

        provider = BalancerVolumeProvider(client=mock_client)

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.ETHEREUM,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 17),
        )

        assert len(volumes) == 3
        assert volumes[0].value == Decimal("1000000")
        assert volumes[1].value == Decimal("1100000")
        assert volumes[2].value == Decimal("1200000")

    @pytest.mark.asyncio
    async def test_get_volume_no_data_returns_fallback(self):
        """Test that empty response returns fallback results."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(return_value={"poolSnapshots": []})

        provider = BalancerVolumeProvider(
            client=mock_client,
            fallback_volume=Decimal("1000"),
        )

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.ETHEREUM,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 17),
        )

        # Should return fallback for each day in range
        assert len(volumes) == 3
        for vol in volumes:
            assert vol.value == Decimal("1000")
            assert vol.source_info.confidence == DataConfidence.LOW
            assert vol.source_info.source == "fallback"

    @pytest.mark.asyncio
    async def test_get_volume_unsupported_chain_raises(self):
        """Test that unsupported chain raises ValueError with helpful message."""
        provider = BalancerVolumeProvider()

        with pytest.raises(ValueError) as exc_info:
            await provider.get_volume(
                pool_address="0x123",
                chain=Chain.BSC,  # Not supported
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 15),
            )

        assert "Unsupported chain" in str(exc_info.value)
        assert "BSC" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_volume_normalizes_address(self):
        """Test that pool address is normalized to lowercase."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            return_value={
                "poolSnapshots": [
                    {"id": "1", "timestamp": 1705276800, "swapVolume": "1000000"}
                ]
            }
        )

        provider = BalancerVolumeProvider(client=mock_client)

        await provider.get_volume(
            pool_address="0xABC123DEF",  # Mixed case
            chain=Chain.ETHEREUM,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        # Verify address was lowercased in query
        call_args = mock_client.query.call_args
        assert call_args.kwargs["variables"]["poolId"] == "0xabc123def"


class TestErrorHandling:
    """Tests for error handling in volume fetching."""

    @pytest.mark.asyncio
    async def test_rate_limit_error_returns_fallback(self):
        """Test that rate limit error returns fallback results."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            side_effect=SubgraphRateLimitError("Rate limit exceeded")
        )

        provider = BalancerVolumeProvider(
            client=mock_client,
            fallback_volume=Decimal("500"),
        )

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.ETHEREUM,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        assert len(volumes) == 1
        assert volumes[0].value == Decimal("500")
        assert volumes[0].source_info.confidence == DataConfidence.LOW

    @pytest.mark.asyncio
    async def test_query_error_returns_fallback(self):
        """Test that query error returns fallback results."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            side_effect=SubgraphQueryError("Invalid query")
        )

        provider = BalancerVolumeProvider(client=mock_client)

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.ETHEREUM,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        assert len(volumes) == 1
        assert volumes[0].value == Decimal("0")  # Default fallback
        assert volumes[0].source_info.confidence == DataConfidence.LOW

    @pytest.mark.asyncio
    async def test_unexpected_error_returns_fallback(self):
        """Test that unexpected errors return fallback results."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(side_effect=Exception("Unexpected error"))

        provider = BalancerVolumeProvider(client=mock_client)

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.ETHEREUM,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        assert len(volumes) == 1
        assert volumes[0].source_info.confidence == DataConfidence.LOW


class TestContextManager:
    """Tests for async context manager behavior."""

    @pytest.mark.asyncio
    async def test_context_manager_closes_owned_client(self):
        """Test that context manager closes client when owned."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.close = AsyncMock()

        # Create provider that owns the client (default)
        provider = BalancerVolumeProvider()
        provider._client = mock_client
        provider._owns_client = True

        async with provider:
            pass

        mock_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager_does_not_close_provided_client(self):
        """Test that context manager doesn't close provided client."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.close = AsyncMock()

        provider = BalancerVolumeProvider(client=mock_client)
        assert provider._owns_client is False

        async with provider:
            pass

        mock_client.close.assert_not_called()


class TestDataParsing:
    """Tests for parsing subgraph response data (PoolSnapshot entity)."""

    @pytest.mark.asyncio
    async def test_parse_volume_with_decimal_precision(self):
        """Test that volume values maintain decimal precision."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            return_value={
                "poolSnapshots": [
                    {
                        "id": "1",
                        "timestamp": 1705276800,
                        "swapVolume": "1234567.89012345",
                    }
                ]
            }
        )

        provider = BalancerVolumeProvider(client=mock_client)

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.ETHEREUM,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        assert volumes[0].value == Decimal("1234567.89012345")

    @pytest.mark.asyncio
    async def test_parse_volume_handles_missing_fields(self):
        """Test handling of missing optional fields in response."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            return_value={
                "poolSnapshots": [
                    {
                        "id": "1",
                        "timestamp": 1705276800,
                        # No swapVolume field
                    }
                ]
            }
        )

        provider = BalancerVolumeProvider(client=mock_client)

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.ETHEREUM,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        # Should default to 0 when swapVolume is missing
        assert volumes[0].value == Decimal("0")

    @pytest.mark.asyncio
    async def test_timestamp_conversion(self):
        """Test that timestamps are correctly converted to datetime."""
        mock_client = MagicMock(spec=SubgraphClient)
        # 1705276800 = 2024-01-15 00:00:00 UTC
        mock_client.query = AsyncMock(
            return_value={
                "poolSnapshots": [
                    {
                        "id": "1",
                        "timestamp": 1705276800,
                        "swapVolume": "1000000",
                    }
                ]
            }
        )

        provider = BalancerVolumeProvider(client=mock_client)

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.ETHEREUM,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        assert volumes[0].source_info.timestamp.date() == date(2024, 1, 15)

    def test_date_to_timestamp_conversion(self):
        """Test date to timestamp conversion is correct."""
        provider = BalancerVolumeProvider()

        # 2024-01-15 00:00:00 UTC should be 1705276800
        timestamp = provider._date_to_timestamp(date(2024, 1, 15))
        assert timestamp == 1705276800

        # 1970-01-01 (Unix epoch) should be 0
        epoch_timestamp = provider._date_to_timestamp(date(1970, 1, 1))
        assert epoch_timestamp == 0


class TestAllSupportedChains:
    """Tests for all supported chains."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("chain", SUPPORTED_CHAINS)
    async def test_can_query_all_supported_chains(self, chain: Chain):
        """Test that all supported chains can be queried."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            return_value={
                "poolSnapshots": [
                    {"id": "1", "timestamp": 1705276800, "swapVolume": "1000000"}
                ]
            }
        )

        provider = BalancerVolumeProvider(client=mock_client)

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=chain,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        assert len(volumes) == 1
        # Verify correct subgraph ID was used
        call_args = mock_client.query.call_args
        assert call_args.kwargs["subgraph_id"] == BALANCER_SUBGRAPH_IDS[chain]


class TestBalancerSpecificBehavior:
    """Tests for Balancer-specific behavior."""

    @pytest.mark.asyncio
    async def test_arbitrum_chain_support(self):
        """Test that Arbitrum chain is supported."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            return_value={
                "poolSnapshots": [
                    {"id": "1", "timestamp": 1705276800, "swapVolume": "500000"}
                ]
            }
        )

        provider = BalancerVolumeProvider(client=mock_client)

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.ARBITRUM,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        assert len(volumes) == 1
        call_args = mock_client.query.call_args
        assert call_args.kwargs["subgraph_id"] == BALANCER_SUBGRAPH_IDS[Chain.ARBITRUM]

    @pytest.mark.asyncio
    async def test_polygon_chain_support(self):
        """Test that Polygon chain is supported."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            return_value={
                "poolSnapshots": [
                    {"id": "1", "timestamp": 1705276800, "swapVolume": "300000"}
                ]
            }
        )

        provider = BalancerVolumeProvider(client=mock_client)

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.POLYGON,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        assert len(volumes) == 1
        call_args = mock_client.query.call_args
        assert call_args.kwargs["subgraph_id"] == BALANCER_SUBGRAPH_IDS[Chain.POLYGON]

    def test_data_source_identifier(self):
        """Test that data source identifier is correct for Balancer."""
        assert DATA_SOURCE == "balancer_v2_subgraph"

    @pytest.mark.asyncio
    async def test_pool_snapshot_query_structure(self):
        """Test that query uses PoolSnapshot entity structure."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            return_value={"poolSnapshots": []}
        )

        provider = BalancerVolumeProvider(client=mock_client)

        await provider.get_volume(
            pool_address="0x123",
            chain=Chain.ETHEREUM,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        # Verify query contains PoolSnapshot entities
        call_args = mock_client.query.call_args
        query = call_args.kwargs["query"]
        assert "poolSnapshots" in query
        assert "swapVolume" in query
        # Balancer uses timestamp (Unix timestamp) not day number
        assert "timestamp_gte" in query or "timestamp:" in query

    @pytest.mark.asyncio
    async def test_swap_fees_available_in_response(self):
        """Test that swap fees data is available when parsing (for future use)."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            return_value={
                "poolSnapshots": [
                    {
                        "id": "1",
                        "timestamp": 1705276800,
                        "swapVolume": "1000000",
                        "swapFees": "3000",  # 0.3% fee on $1M volume
                        "liquidity": "50000000",
                        "totalShares": "100000",
                    }
                ]
            }
        )

        provider = BalancerVolumeProvider(client=mock_client)

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.ETHEREUM,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        # Currently we return swapVolume as the volume
        # swapFees could be used in future for fee calculations
        assert volumes[0].value == Decimal("1000000")
