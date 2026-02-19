"""Unit tests for Multi-DEX Volume Provider Aggregator.

This module tests the MultiDEXVolumeProvider class in providers/multi_dex_volume.py,
covering:
- Provider initialization and configuration
- Protocol routing to correct DEX-specific providers
- Protocol detection from chain
- Fallback behavior when no provider available
- Error handling and graceful degradation
"""

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.core.enums import Chain, Protocol
from almanak.framework.backtesting.pnl.providers.multi_dex_volume import (
    FALLBACK_DATA_SOURCE,
    PROTOCOL_CHAIN_SUPPORT,
    PROTOCOL_PROVIDER_MAP,
    STRING_PROTOCOL_MAP,
    MultiDEXVolumeProvider,
)
from almanak.framework.backtesting.pnl.types import DataConfidence, DataSourceInfo, VolumeResult


class TestMultiDEXVolumeProviderInitialization:
    """Tests for MultiDEXVolumeProvider initialization."""

    def test_init_default(self):
        """Test provider initializes with default settings."""
        provider = MultiDEXVolumeProvider()
        assert provider._fallback_volume == Decimal("0")
        assert provider._requests_per_minute == 100
        assert provider._providers == {}

    def test_init_with_custom_fallback(self):
        """Test provider initializes with custom fallback volume."""
        provider = MultiDEXVolumeProvider(fallback_volume=Decimal("1000"))
        assert provider._fallback_volume == Decimal("1000")

    def test_init_with_custom_rate_limit(self):
        """Test provider initializes with custom rate limit."""
        provider = MultiDEXVolumeProvider(requests_per_minute=50)
        assert provider._requests_per_minute == 50


class TestProtocolMappings:
    """Tests for protocol to provider mappings."""

    def test_protocol_enum_mapping_covers_dex_protocols(self):
        """Test Protocol enum mapping includes all DEX protocols."""
        # These protocols should have providers
        expected_protocols = [
            Protocol.UNISWAP_V3,
            Protocol.SUSHISWAP_V3,
            Protocol.PANCAKESWAP_V3,
            Protocol.AERODROME,
            Protocol.TRADERJOE_V2,
        ]
        for protocol in expected_protocols:
            assert protocol in PROTOCOL_PROVIDER_MAP

    def test_string_mapping_includes_all_protocol_ids(self):
        """Test string mapping includes all protocol identifiers."""
        expected_ids = [
            "uniswap_v3",
            "sushiswap_v3",
            "pancakeswap_v3",
            "aerodrome",
            "traderjoe_v2",
            "curve",
            "balancer",
        ]
        for protocol_id in expected_ids:
            assert protocol_id in STRING_PROTOCOL_MAP

    def test_string_mapping_includes_aliases(self):
        """Test string mapping includes common aliases."""
        aliases = {
            "uni_v3": "uniswap_v3",
            "sushi_v3": "sushiswap_v3",
            "pancake_v3": "pancakeswap_v3",
            "joe_v2": "traderjoe_v2",
            "bal": "balancer",
            "crv": "curve",
        }
        for alias, canonical in aliases.items():
            assert alias in STRING_PROTOCOL_MAP
            # Alias should map to same provider class as canonical
            assert STRING_PROTOCOL_MAP[alias] == STRING_PROTOCOL_MAP[canonical]

    def test_protocol_chain_support_mapping(self):
        """Test protocol chain support is properly configured."""
        assert "uniswap_v3" in PROTOCOL_CHAIN_SUPPORT
        assert "curve" in PROTOCOL_CHAIN_SUPPORT
        assert "balancer" in PROTOCOL_CHAIN_SUPPORT

        # Check some specific chain support
        assert Chain.ETHEREUM in PROTOCOL_CHAIN_SUPPORT["uniswap_v3"]
        assert Chain.BASE in PROTOCOL_CHAIN_SUPPORT["aerodrome"]
        assert Chain.AVALANCHE in PROTOCOL_CHAIN_SUPPORT["traderjoe_v2"]


class TestProtocolNormalization:
    """Tests for protocol normalization."""

    def test_normalize_protocol_enum(self):
        """Test Protocol enum is normalized to string."""
        provider = MultiDEXVolumeProvider()
        assert provider._get_protocol_id(Protocol.UNISWAP_V3) == "uniswap_v3"
        assert provider._get_protocol_id(Protocol.AERODROME) == "aerodrome"

    def test_normalize_string_protocol(self):
        """Test string protocol is normalized to lowercase."""
        provider = MultiDEXVolumeProvider()
        assert provider._get_protocol_id("UNISWAP_V3") == "uniswap_v3"
        assert provider._get_protocol_id("Curve") == "curve"
        assert provider._get_protocol_id("balancer") == "balancer"

    def test_normalize_none_returns_none(self):
        """Test None protocol returns None."""
        provider = MultiDEXVolumeProvider()
        assert provider._get_protocol_id(None) is None


class TestProtocolDetection:
    """Tests for chain-based protocol detection."""

    def test_detect_aerodrome_for_base(self):
        """Test Aerodrome is detected for Base chain."""
        provider = MultiDEXVolumeProvider()
        assert provider._detect_protocol_from_chain(Chain.BASE) == "aerodrome"

    def test_detect_traderjoe_for_avalanche(self):
        """Test TraderJoe V2 is detected for Avalanche chain."""
        provider = MultiDEXVolumeProvider()
        assert provider._detect_protocol_from_chain(Chain.AVALANCHE) == "traderjoe_v2"

    def test_detect_uniswap_for_ethereum(self):
        """Test Uniswap V3 is detected for Ethereum chain."""
        provider = MultiDEXVolumeProvider()
        assert provider._detect_protocol_from_chain(Chain.ETHEREUM) == "uniswap_v3"

    def test_detect_uniswap_for_arbitrum(self):
        """Test Uniswap V3 is detected for Arbitrum chain."""
        provider = MultiDEXVolumeProvider()
        assert provider._detect_protocol_from_chain(Chain.ARBITRUM) == "uniswap_v3"


class TestGetVolume:
    """Tests for get_volume method routing."""

    @pytest.mark.asyncio
    async def test_get_volume_with_protocol_enum(self):
        """Test volume fetching with Protocol enum."""
        provider = MultiDEXVolumeProvider()

        # Mock the inner provider
        mock_inner = MagicMock()
        mock_inner.get_volume = AsyncMock(
            return_value=[
                VolumeResult(
                    value=Decimal("1000000"),
                    source_info=DataSourceInfo(
                        source="uniswap_v3_subgraph",
                        confidence=DataConfidence.HIGH,
                        timestamp=date(2024, 1, 15),
                    ),
                )
            ]
        )
        provider._providers["uniswap_v3"] = mock_inner

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.ARBITRUM,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
            protocol=Protocol.UNISWAP_V3,
        )

        assert len(volumes) == 1
        assert volumes[0].value == Decimal("1000000")
        mock_inner.get_volume.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_volume_with_string_protocol(self):
        """Test volume fetching with string protocol identifier."""
        provider = MultiDEXVolumeProvider()

        # Mock the inner provider
        mock_inner = MagicMock()
        mock_inner.get_volume = AsyncMock(
            return_value=[
                VolumeResult(
                    value=Decimal("500000"),
                    source_info=DataSourceInfo(
                        source="curve_subgraph",
                        confidence=DataConfidence.HIGH,
                        timestamp=date(2024, 1, 15),
                    ),
                )
            ]
        )
        provider._providers["curve"] = mock_inner

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.ETHEREUM,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
            protocol="curve",
        )

        assert len(volumes) == 1
        assert volumes[0].value == Decimal("500000")

    @pytest.mark.asyncio
    async def test_get_volume_auto_detect_protocol(self):
        """Test volume fetching with auto-detected protocol."""
        provider = MultiDEXVolumeProvider()

        # Mock the inner provider for Aerodrome (auto-detected for Base)
        mock_inner = MagicMock()
        mock_inner.get_volume = AsyncMock(
            return_value=[
                VolumeResult(
                    value=Decimal("750000"),
                    source_info=DataSourceInfo(
                        source="aerodrome_subgraph",
                        confidence=DataConfidence.HIGH,
                        timestamp=date(2024, 1, 15),
                    ),
                )
            ]
        )
        provider._providers["aerodrome"] = mock_inner

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.BASE,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
            # No protocol specified - should auto-detect Aerodrome for Base
        )

        assert len(volumes) == 1
        assert volumes[0].value == Decimal("750000")

    @pytest.mark.asyncio
    async def test_get_volume_unsupported_chain_returns_fallback(self):
        """Test that unsupported chain returns fallback."""
        provider = MultiDEXVolumeProvider(fallback_volume=Decimal("100"))

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.BSC,  # BSC not supported by Uniswap V3
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 17),
            protocol=Protocol.UNISWAP_V3,
        )

        assert len(volumes) == 3
        for vol in volumes:
            assert vol.value == Decimal("100")
            assert vol.source_info.confidence == DataConfidence.LOW
            assert vol.source_info.source == FALLBACK_DATA_SOURCE

    @pytest.mark.asyncio
    async def test_get_volume_unknown_protocol_returns_fallback(self):
        """Test that unknown protocol returns fallback."""
        provider = MultiDEXVolumeProvider(fallback_volume=Decimal("50"))

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.ETHEREUM,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
            protocol="unknown_protocol",
        )

        assert len(volumes) == 1
        assert volumes[0].value == Decimal("50")
        assert volumes[0].source_info.confidence == DataConfidence.LOW


class TestFallbackBehavior:
    """Tests for fallback behavior."""

    @pytest.mark.asyncio
    async def test_fallback_results_cover_date_range(self):
        """Test fallback results cover the entire date range."""
        provider = MultiDEXVolumeProvider(fallback_volume=Decimal("1000"))

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.SONIC,  # Unsupported chain
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 20),  # 6 days
        )

        assert len(volumes) == 6
        for vol in volumes:
            assert vol.value == Decimal("1000")
            assert vol.source_info.confidence == DataConfidence.LOW

    @pytest.mark.asyncio
    async def test_provider_error_returns_fallback(self):
        """Test that provider errors return fallback."""
        provider = MultiDEXVolumeProvider(fallback_volume=Decimal("200"))

        # Mock the inner provider to raise an error
        mock_inner = MagicMock()
        mock_inner.get_volume = AsyncMock(side_effect=Exception("Subgraph error"))
        provider._providers["uniswap_v3"] = mock_inner

        volumes = await provider.get_volume(
            pool_address="0x123",
            chain=Chain.ARBITRUM,
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
            protocol=Protocol.UNISWAP_V3,
        )

        assert len(volumes) == 1
        assert volumes[0].value == Decimal("200")
        assert volumes[0].source_info.confidence == DataConfidence.LOW


class TestContextManager:
    """Tests for async context manager behavior."""

    @pytest.mark.asyncio
    async def test_context_manager_closes_providers(self):
        """Test that context manager closes all providers."""
        provider = MultiDEXVolumeProvider()

        # Add mock providers
        mock_provider1 = MagicMock()
        mock_provider1.close = AsyncMock()
        mock_provider2 = MagicMock()
        mock_provider2.close = AsyncMock()

        provider._providers["proto1"] = mock_provider1
        provider._providers["proto2"] = mock_provider2

        # Mock the shared client
        provider._shared_client.close = AsyncMock()

        async with provider:
            pass

        mock_provider1.close.assert_called_once()
        mock_provider2.close.assert_called_once()
        provider._shared_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager_handles_close_errors(self):
        """Test that context manager handles provider close errors gracefully."""
        provider = MultiDEXVolumeProvider()

        # Add mock provider that fails on close
        mock_provider = MagicMock()
        mock_provider.close = AsyncMock(side_effect=Exception("Close failed"))
        provider._providers["proto"] = mock_provider
        provider._shared_client.close = AsyncMock()

        # Should not raise
        async with provider:
            pass


class TestHelperMethods:
    """Tests for helper methods."""

    def test_get_supported_protocols(self):
        """Test get_supported_protocols returns all protocols."""
        provider = MultiDEXVolumeProvider()
        protocols = provider.get_supported_protocols()

        assert "uniswap_v3" in protocols
        assert "curve" in protocols
        assert "balancer" in protocols
        assert "aerodrome" in protocols

    def test_get_supported_chains_for_protocol(self):
        """Test get_supported_chains returns chains for a protocol."""
        provider = MultiDEXVolumeProvider()

        # Test with Protocol enum
        chains = provider.get_supported_chains(Protocol.UNISWAP_V3)
        assert Chain.ETHEREUM in chains
        assert Chain.ARBITRUM in chains

        # Test with string
        chains = provider.get_supported_chains("balancer")
        assert Chain.ETHEREUM in chains
        assert Chain.POLYGON in chains

    def test_get_supported_chains_unknown_protocol(self):
        """Test get_supported_chains returns empty for unknown protocol."""
        provider = MultiDEXVolumeProvider()
        chains = provider.get_supported_chains("unknown")
        assert chains == []


class TestProviderCreation:
    """Tests for lazy provider creation."""

    @pytest.mark.asyncio
    async def test_provider_created_on_first_use(self):
        """Test that providers are created lazily on first use."""
        provider = MultiDEXVolumeProvider()
        assert "uniswap_v3" not in provider._providers

        # Trigger provider creation by getting it
        inner_provider = provider._get_provider("uniswap_v3")
        assert inner_provider is not None
        assert "uniswap_v3" in provider._providers

    @pytest.mark.asyncio
    async def test_provider_cached_after_creation(self):
        """Test that providers are cached after creation."""
        provider = MultiDEXVolumeProvider()

        provider1 = provider._get_provider("uniswap_v3")
        provider2 = provider._get_provider("uniswap_v3")

        assert provider1 is provider2

    def test_get_provider_unknown_returns_none(self):
        """Test that unknown protocol returns None."""
        provider = MultiDEXVolumeProvider()
        result = provider._get_provider("unknown_protocol")
        assert result is None


class TestAllProtocols:
    """Tests for all supported protocols."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "protocol_id",
        ["uniswap_v3", "sushiswap_v3", "pancakeswap_v3", "aerodrome", "traderjoe_v2", "curve", "balancer"],
    )
    async def test_can_create_all_providers(self, protocol_id: str):
        """Test that all protocol providers can be created."""
        provider = MultiDEXVolumeProvider()
        inner_provider = provider._get_provider(protocol_id)

        assert inner_provider is not None
        assert hasattr(inner_provider, "get_volume")
