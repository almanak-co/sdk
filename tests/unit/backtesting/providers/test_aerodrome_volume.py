"""Unit tests for Aerodrome Volume Provider.

This module tests the AerodromeVolumeProvider class in providers/dex/aerodrome_volume.py,
covering:
- Provider initialization and configuration
- Supported chains (Base only)
- Volume fetching with mocked responses
- Solidly-style pairDayDatas parsing
- Fallback behavior when data unavailable
- Error handling for subgraph failures
"""

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.core.enums import Chain
from almanak.framework.backtesting.pnl.providers.dex.aerodrome_volume import (
    AERODROME_SUBGRAPH_IDS,
    DATA_SOURCE,
    SUPPORTED_CHAINS,
    AerodromeVolumeProvider,
)
from almanak.framework.backtesting.pnl.providers.subgraph_client import (
    SubgraphClient,
    SubgraphQueryError,
    SubgraphRateLimitError,
)
from almanak.framework.backtesting.pnl.types import DataConfidence


class TestAerodromeVolumeProviderInitialization:
    """Tests for AerodromeVolumeProvider initialization."""

    def test_init_default(self):
        """Test provider initializes with default settings."""
        provider = AerodromeVolumeProvider()
        assert provider.supported_chains == SUPPORTED_CHAINS
        assert provider._fallback_volume == Decimal("0")
        assert provider._owns_client is True

    def test_init_with_custom_fallback(self):
        """Test provider initializes with custom fallback volume."""
        provider = AerodromeVolumeProvider(fallback_volume=Decimal("1000"))
        assert provider._fallback_volume == Decimal("1000")

    def test_init_with_custom_rate_limit(self):
        """Test provider initializes with custom rate limit."""
        provider = AerodromeVolumeProvider(requests_per_minute=50)
        assert provider._client.config.requests_per_minute == 50

    def test_init_with_provided_client(self):
        """Test provider uses provided client and doesn't own it."""
        mock_client = MagicMock(spec=SubgraphClient)
        provider = AerodromeVolumeProvider(client=mock_client)
        assert provider._client is mock_client
        assert provider._owns_client is False

    def test_supported_chains_property_returns_copy(self):
        """Test supported_chains returns a copy, not the original."""
        provider = AerodromeVolumeProvider()
        chains1 = provider.supported_chains
        chains2 = provider.supported_chains
        assert chains1 == chains2
        assert chains1 is not chains2


class TestSupportedChains:
    """Tests for supported chains configuration."""

    def test_base_is_only_supported_chain(self):
        """Test that Base is the only supported chain (Aerodrome is Base-native)."""
        assert Chain.BASE in SUPPORTED_CHAINS
        assert len(SUPPORTED_CHAINS) == 1  # Only Base

    def test_all_supported_chains_have_subgraph_ids(self):
        """Test all supported chains have subgraph IDs."""
        for chain in SUPPORTED_CHAINS:
            assert chain in AERODROME_SUBGRAPH_IDS
            assert AERODROME_SUBGRAPH_IDS[chain]  # Non-empty

    def test_subgraph_ids_are_valid_format(self):
        """Test subgraph IDs are non-empty strings."""
        for chain, subgraph_id in AERODROME_SUBGRAPH_IDS.items():
            assert isinstance(subgraph_id, str)
            assert len(subgraph_id) > 10  # Reasonable length for deployment ID


class TestGetVolume:
    """Tests for get_volume method."""

    @pytest.mark.asyncio
    async def test_get_volume_success(self):
        """Test successfully fetching volume data."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            return_value={
                "pairDayDatas": [
                    {
                        "id": "0x6cDcb1C4-12345",
                        "date": 1705276800,  # 2024-01-15 00:00:00 UTC
                        "dailyVolumeUSD": "2500000.75",
                        "dailyVolumeToken0": "1000000",
                        "dailyVolumeToken1": "1250",
                        "reserveUSD": "15000000.00",
                        "totalSupply": "8000000",
                    }
                ]
            }
        )

        provider = AerodromeVolumeProvider(client=mock_client)

        volumes = await provider.get_volume(
            pool_address="0x6cDcb1C4A4D1C3C6d054b27AC5B77e89eAFb971d",
            chain=Chain.BASE,
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
        assert call_args.kwargs["subgraph_id"] == AERODROME_SUBGRAPH_IDS[Chain.BASE]
        # Aerodrome uses pairAddress variable (Solidly-style)
        assert "pairAddress" in str(call_args.kwargs["variables"])

    @pytest.mark.asyncio
    async def test_get_volume_multiple_days(self):
        """Test fetching volume for multiple days."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            return_value={
                "pairDayDatas": [
                    {"id": "1", "date": 1705276800, "dailyVolumeUSD": "1000000"},
                    {"id": "2", "date": 1705363200, "dailyVolumeUSD": "1100000"},
                    {"id": "3", "date": 1705449600, "dailyVolumeUSD": "1200000"},
                ]
            }
        )

        provider = AerodromeVolumeProvider(client=mock_client)

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.BASE,
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
        mock_client.query = AsyncMock(return_value={"pairDayDatas": []})

        provider = AerodromeVolumeProvider(
            client=mock_client,
            fallback_volume=Decimal("1000"),
        )

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.BASE,
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
        """Test that unsupported chain raises ValueError."""
        provider = AerodromeVolumeProvider()

        with pytest.raises(ValueError) as exc_info:
            await provider.get_volume(
                pool_address="0x123",
                chain=Chain.ETHEREUM,  # Not supported for Aerodrome (Base only)
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 15),
            )

        assert "Unsupported chain" in str(exc_info.value)
        assert "ETHEREUM" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_volume_arbitrum_unsupported(self):
        """Test that Arbitrum raises ValueError (Aerodrome is Base-only)."""
        provider = AerodromeVolumeProvider()

        with pytest.raises(ValueError) as exc_info:
            await provider.get_volume(
                pool_address="0x123",
                chain=Chain.ARBITRUM,
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 15),
            )

        assert "Unsupported chain" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_volume_normalizes_address(self):
        """Test that pool address is normalized to lowercase."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            return_value={
                "pairDayDatas": [
                    {"id": "1", "date": 1705276800, "dailyVolumeUSD": "1000000"}
                ]
            }
        )

        provider = AerodromeVolumeProvider(client=mock_client)

        await provider.get_volume(
            pool_address="0xABC123DEF",  # Mixed case
            chain=Chain.BASE,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        # Verify address was lowercased in query
        call_args = mock_client.query.call_args
        assert call_args.kwargs["variables"]["pairAddress"] == "0xabc123def"


class TestErrorHandling:
    """Tests for error handling in volume fetching."""

    @pytest.mark.asyncio
    async def test_rate_limit_error_returns_fallback(self):
        """Test that rate limit error returns fallback results."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            side_effect=SubgraphRateLimitError("Rate limit exceeded")
        )

        provider = AerodromeVolumeProvider(
            client=mock_client,
            fallback_volume=Decimal("500"),
        )

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.BASE,
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

        provider = AerodromeVolumeProvider(client=mock_client)

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.BASE,
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

        provider = AerodromeVolumeProvider(client=mock_client)

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.BASE,
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
        provider = AerodromeVolumeProvider()
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

        provider = AerodromeVolumeProvider(client=mock_client)
        assert provider._owns_client is False

        async with provider:
            pass

        mock_client.close.assert_not_called()


class TestDataParsing:
    """Tests for parsing subgraph response data."""

    @pytest.mark.asyncio
    async def test_parse_volume_with_decimal_precision(self):
        """Test that volume values maintain decimal precision."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            return_value={
                "pairDayDatas": [
                    {
                        "id": "1",
                        "date": 1705276800,
                        "dailyVolumeUSD": "1234567.89012345",
                    }
                ]
            }
        )

        provider = AerodromeVolumeProvider(client=mock_client)

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.BASE,
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
                "pairDayDatas": [
                    {
                        "id": "1",
                        "date": 1705276800,
                        # Only id and date, no dailyVolumeUSD
                    }
                ]
            }
        )

        provider = AerodromeVolumeProvider(client=mock_client)

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.BASE,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        # Should default to 0 when dailyVolumeUSD is missing
        assert volumes[0].value == Decimal("0")

    @pytest.mark.asyncio
    async def test_timestamp_conversion(self):
        """Test that timestamps are correctly converted to datetime."""
        mock_client = MagicMock(spec=SubgraphClient)
        # 1705276800 = 2024-01-15 00:00:00 UTC
        mock_client.query = AsyncMock(
            return_value={
                "pairDayDatas": [
                    {
                        "id": "1",
                        "date": 1705276800,
                        "dailyVolumeUSD": "1000000",
                    }
                ]
            }
        )

        provider = AerodromeVolumeProvider(client=mock_client)

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.BASE,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        assert volumes[0].source_info.timestamp.date() == date(2024, 1, 15)


class TestSolidlyStyleData:
    """Tests specific to Solidly-style pool data format."""

    @pytest.mark.asyncio
    async def test_uses_daily_volume_usd_field(self):
        """Test that provider uses dailyVolumeUSD (Solidly) not volumeUSD (Uniswap V3)."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            return_value={
                "pairDayDatas": [
                    {
                        "id": "1",
                        "date": 1705276800,
                        "dailyVolumeUSD": "5000000",  # Solidly-style field
                        "volumeUSD": "9999999",  # Should be ignored
                    }
                ]
            }
        )

        provider = AerodromeVolumeProvider(client=mock_client)

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.BASE,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        # Should use dailyVolumeUSD, not volumeUSD
        assert volumes[0].value == Decimal("5000000")

    @pytest.mark.asyncio
    async def test_uses_pair_address_variable(self):
        """Test that query uses pairAddress variable (Solidly) not poolAddress (V3)."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            return_value={"pairDayDatas": []}
        )

        provider = AerodromeVolumeProvider(client=mock_client)

        await provider.get_volume(
            pool_address="0x123",
            chain=Chain.BASE,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        call_args = mock_client.query.call_args
        # Verify pairAddress is used (not poolAddress)
        assert "pairAddress" in call_args.kwargs["variables"]
        assert "poolAddress" not in call_args.kwargs["variables"]


class TestAllSupportedChains:
    """Tests for all supported chains."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("chain", SUPPORTED_CHAINS)
    async def test_can_query_all_supported_chains(self, chain: Chain):
        """Test that all supported chains can be queried."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(
            return_value={
                "pairDayDatas": [
                    {"id": "1", "date": 1705276800, "dailyVolumeUSD": "1000000"}
                ]
            }
        )

        provider = AerodromeVolumeProvider(client=mock_client)

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=chain,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        assert len(volumes) == 1
        # Verify correct subgraph ID was used
        call_args = mock_client.query.call_args
        assert call_args.kwargs["subgraph_id"] == AERODROME_SUBGRAPH_IDS[chain]
