"""Unit tests for Curve Volume Provider.

This module tests the CurveVolumeProvider class in providers/dex/curve_volume.py,
covering:
- Provider initialization and configuration
- Supported chains and subgraph ID mapping
- Volume fetching with mocked responses (Messari schema)
- Fallback behavior when data unavailable
- Error handling for subgraph failures
"""

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.core.enums import Chain
from almanak.framework.backtesting.pnl.providers.dex.curve_volume import (
    CURVE_SUBGRAPH_IDS,
    DATA_SOURCE,
    SUPPORTED_CHAINS,
    CurveVolumeProvider,
)
from almanak.framework.backtesting.pnl.providers.subgraph_client import (
    SubgraphClient,
    SubgraphQueryError,
    SubgraphRateLimitError,
)
from almanak.framework.backtesting.pnl.types import DataConfidence


class TestCurveVolumeProviderInitialization:
    """Tests for CurveVolumeProvider initialization."""

    def test_init_default(self):
        """Test provider initializes with default settings."""
        provider = CurveVolumeProvider()
        assert provider.supported_chains == SUPPORTED_CHAINS
        assert provider._fallback_volume == Decimal("0")
        assert provider._owns_client is True

    def test_init_with_custom_fallback(self):
        """Test provider initializes with custom fallback volume."""
        provider = CurveVolumeProvider(fallback_volume=Decimal("1000"))
        assert provider._fallback_volume == Decimal("1000")

    def test_init_with_custom_rate_limit(self):
        """Test provider initializes with custom rate limit."""
        provider = CurveVolumeProvider(requests_per_minute=50)
        assert provider._client.config.requests_per_minute == 50

    def test_init_with_provided_client(self):
        """Test provider uses provided client and doesn't own it."""
        mock_client = MagicMock(spec=SubgraphClient)
        provider = CurveVolumeProvider(client=mock_client)
        assert provider._client is mock_client
        assert provider._owns_client is False

    def test_supported_chains_property_returns_copy(self):
        """Test supported_chains returns a copy, not the original."""
        provider = CurveVolumeProvider()
        chains1 = provider.supported_chains
        chains2 = provider.supported_chains
        assert chains1 == chains2
        assert chains1 is not chains2


class TestSupportedChains:
    """Tests for supported chains configuration."""

    def test_supported_chains_include_required_networks(self):
        """Test that required networks are supported (Ethereum, Optimism only for now)."""
        # US-012 requires: Ethereum, Arbitrum, Optimism, Polygon
        # But only Ethereum and Optimism are on decentralized network
        assert Chain.ETHEREUM in SUPPORTED_CHAINS
        assert Chain.OPTIMISM in SUPPORTED_CHAINS

    def test_arbitrum_and_polygon_not_yet_supported(self):
        """Test that Arbitrum and Polygon are not yet supported (pending migration)."""
        # These are on hosted service (deprecated), not decentralized network yet
        assert Chain.ARBITRUM not in SUPPORTED_CHAINS
        assert Chain.POLYGON not in SUPPORTED_CHAINS

    def test_all_supported_chains_have_subgraph_ids(self):
        """Test all supported chains have subgraph IDs."""
        for chain in SUPPORTED_CHAINS:
            assert chain in CURVE_SUBGRAPH_IDS
            assert CURVE_SUBGRAPH_IDS[chain]  # Non-empty

    def test_subgraph_ids_are_valid_format(self):
        """Test subgraph IDs are non-empty strings."""
        for chain, subgraph_id in CURVE_SUBGRAPH_IDS.items():
            assert isinstance(subgraph_id, str)
            assert len(subgraph_id) > 10  # Reasonable length for deployment ID


class TestGetVolume:
    """Tests for get_volume method."""

    @pytest.mark.asyncio
    async def test_get_volume_success(self):
        """Test successfully fetching volume data with Messari schema."""
        mock_client = MagicMock(spec=SubgraphClient)
        # Messari schema uses liquidityPoolDailySnapshots with day number and dailyVolumeUSD
        mock_client.query = AsyncMock(
            return_value={
                "liquidityPoolDailySnapshots": [
                    {
                        "id": "0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7-19737",
                        "day": 19737,  # 2024-01-15 (days since Unix epoch)
                        "dailyVolumeUSD": "2500000.75",
                        "totalValueLockedUSD": "500000000.00",
                        "cumulativeVolumeUSD": "150000000000.00",
                    }
                ]
            }
        )

        provider = CurveVolumeProvider(client=mock_client)

        volumes = await provider.get_volume(
            pool_address="0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",
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
        assert call_args.kwargs["subgraph_id"] == CURVE_SUBGRAPH_IDS[Chain.ETHEREUM]
        # Pool address should be lowercased
        assert call_args.kwargs["variables"]["poolAddress"] == "0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7"

    @pytest.mark.asyncio
    async def test_get_volume_multiple_days(self):
        """Test fetching volume for multiple days."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            return_value={
                "liquidityPoolDailySnapshots": [
                    {"id": "1", "day": 19737, "dailyVolumeUSD": "1000000"},
                    {"id": "2", "day": 19739, "dailyVolumeUSD": "1100000"},
                    {"id": "3", "day": 19740, "dailyVolumeUSD": "1200000"},
                ]
            }
        )

        provider = CurveVolumeProvider(client=mock_client)

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
        mock_client.query = AsyncMock(return_value={"liquidityPoolDailySnapshots": []})

        provider = CurveVolumeProvider(
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
        provider = CurveVolumeProvider()

        with pytest.raises(ValueError) as exc_info:
            await provider.get_volume(
                pool_address="0x123",
                chain=Chain.ARBITRUM,  # Not supported yet (on hosted service)
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 15),
            )

        assert "Unsupported chain" in str(exc_info.value)
        assert "ARBITRUM" in str(exc_info.value)
        assert "pending" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_get_volume_polygon_unsupported(self):
        """Test that Polygon raises ValueError (pending migration)."""
        provider = CurveVolumeProvider()

        with pytest.raises(ValueError) as exc_info:
            await provider.get_volume(
                pool_address="0x123",
                chain=Chain.POLYGON,
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 15),
            )

        assert "Unsupported chain" in str(exc_info.value)
        assert "POLYGON" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_volume_normalizes_address(self):
        """Test that pool address is normalized to lowercase."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            return_value={
                "liquidityPoolDailySnapshots": [
                    {"id": "1", "day": 19737, "dailyVolumeUSD": "1000000"}
                ]
            }
        )

        provider = CurveVolumeProvider(client=mock_client)

        await provider.get_volume(
            pool_address="0xABC123DEF",  # Mixed case
            chain=Chain.ETHEREUM,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        # Verify address was lowercased in query
        call_args = mock_client.query.call_args
        assert call_args.kwargs["variables"]["poolAddress"] == "0xabc123def"


class TestErrorHandling:
    """Tests for error handling in volume fetching."""

    @pytest.mark.asyncio
    async def test_rate_limit_error_returns_fallback(self):
        """Test that rate limit error returns fallback results."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            side_effect=SubgraphRateLimitError("Rate limit exceeded")
        )

        provider = CurveVolumeProvider(
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

        provider = CurveVolumeProvider(client=mock_client)

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

        provider = CurveVolumeProvider(client=mock_client)

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
        provider = CurveVolumeProvider()
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

        provider = CurveVolumeProvider(client=mock_client)
        assert provider._owns_client is False

        async with provider:
            pass

        mock_client.close.assert_not_called()


class TestDataParsing:
    """Tests for parsing subgraph response data (Messari schema)."""

    @pytest.mark.asyncio
    async def test_parse_volume_with_decimal_precision(self):
        """Test that volume values maintain decimal precision."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            return_value={
                "liquidityPoolDailySnapshots": [
                    {
                        "id": "1",
                        "day": 19737,
                        "dailyVolumeUSD": "1234567.89012345",
                    }
                ]
            }
        )

        provider = CurveVolumeProvider(client=mock_client)

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
                "liquidityPoolDailySnapshots": [
                    {
                        "id": "1",
                        "day": 19737,
                        # No dailyVolumeUSD field
                    }
                ]
            }
        )

        provider = CurveVolumeProvider(client=mock_client)

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.ETHEREUM,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        # Should default to 0 when dailyVolumeUSD is missing
        assert volumes[0].value == Decimal("0")

    @pytest.mark.asyncio
    async def test_day_number_conversion(self):
        """Test that day numbers (days since epoch) are correctly converted to datetime."""
        mock_client = MagicMock(spec=SubgraphClient)
        # Day 19737 = 2024-01-15 (19737 days since 1970-01-01)
        mock_client.query = AsyncMock(
            return_value={
                "liquidityPoolDailySnapshots": [
                    {
                        "id": "1",
                        "day": 19737,
                        "dailyVolumeUSD": "1000000",
                    }
                ]
            }
        )

        provider = CurveVolumeProvider(client=mock_client)

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.ETHEREUM,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        assert volumes[0].source_info.timestamp.date() == date(2024, 1, 15)

    @pytest.mark.asyncio
    async def test_date_to_day_number_conversion(self):
        """Test date to day number conversion is correct."""
        provider = CurveVolumeProvider()

        # 2024-01-15 should be 19737 days since epoch
        day_num = provider._date_to_day_number(date(2024, 1, 15))
        assert day_num == 19737

        # 1970-01-01 (Unix epoch) should be day 0
        epoch_day = provider._date_to_day_number(date(1970, 1, 1))
        assert epoch_day == 0


class TestAllSupportedChains:
    """Tests for all supported chains."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("chain", SUPPORTED_CHAINS)
    async def test_can_query_all_supported_chains(self, chain: Chain):
        """Test that all supported chains can be queried."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            return_value={
                "liquidityPoolDailySnapshots": [
                    {"id": "1", "day": 19737, "dailyVolumeUSD": "1000000"}
                ]
            }
        )

        provider = CurveVolumeProvider(client=mock_client)

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=chain,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        assert len(volumes) == 1
        # Verify correct subgraph ID was used
        call_args = mock_client.query.call_args
        assert call_args.kwargs["subgraph_id"] == CURVE_SUBGRAPH_IDS[chain]


class TestCurveSpecificBehavior:
    """Tests for Curve-specific behavior."""

    @pytest.mark.asyncio
    async def test_optimism_chain_support(self):
        """Test that Optimism chain is supported."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            return_value={
                "liquidityPoolDailySnapshots": [
                    {"id": "1", "day": 19737, "dailyVolumeUSD": "500000"}
                ]
            }
        )

        provider = CurveVolumeProvider(client=mock_client)

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.OPTIMISM,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        assert len(volumes) == 1
        call_args = mock_client.query.call_args
        assert call_args.kwargs["subgraph_id"] == CURVE_SUBGRAPH_IDS[Chain.OPTIMISM]

    def test_data_source_identifier(self):
        """Test that data source identifier is correct for Curve."""
        assert DATA_SOURCE == "curve_messari_subgraph"

    @pytest.mark.asyncio
    async def test_messari_schema_query_structure(self):
        """Test that query uses Messari schema structure (liquidityPoolDailySnapshots)."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            return_value={"liquidityPoolDailySnapshots": []}
        )

        provider = CurveVolumeProvider(client=mock_client)

        await provider.get_volume(
            pool_address="0x123",
            chain=Chain.ETHEREUM,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        # Verify query contains Messari schema entities
        call_args = mock_client.query.call_args
        query = call_args.kwargs["query"]
        assert "liquidityPoolDailySnapshots" in query
        assert "dailyVolumeUSD" in query
        # Messari uses day (days since epoch) not date timestamp
        assert "day_gte" in query or "day:" in query
