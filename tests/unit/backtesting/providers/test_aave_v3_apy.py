"""Unit tests for Aave V3 APY Provider.

This module tests the AaveV3APYProvider class in providers/lending/aave_v3_apy.py,
covering:
- Provider initialization and configuration
- Supported chains and subgraph ID mapping
- APY fetching with mocked responses
- RAY unit conversion to decimal APY
- Fallback behavior when subgraph unavailable
- Error handling for query failures
- Reserve ID finding and caching
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.core.enums import Chain
from almanak.framework.backtesting.pnl.providers.lending.aave_v3_apy import (
    AAVE_V3_SUBGRAPH_IDS,
    DATA_SOURCE,
    DEFAULT_BORROW_APY_FALLBACK,
    DEFAULT_SUPPLY_APY_FALLBACK,
    RAY,
    SUPPORTED_CHAINS,
    AaveV3APYProvider,
    AaveV3ClientConfig,
)
from almanak.framework.backtesting.pnl.providers.subgraph_client import (
    SubgraphClient,
    SubgraphQueryError,
    SubgraphRateLimitError,
)
from almanak.framework.backtesting.pnl.types import DataConfidence


class TestAaveV3APYProviderInitialization:
    """Tests for AaveV3APYProvider initialization."""

    def test_init_default(self):
        """Test provider initializes with default settings."""
        provider = AaveV3APYProvider()
        assert provider.supported_chains == SUPPORTED_CHAINS
        assert provider.config.chain == Chain.ETHEREUM
        assert provider.config.requests_per_minute == 100
        assert provider._owns_client is True

    def test_init_with_custom_config(self):
        """Test provider initializes with custom config."""
        config = AaveV3ClientConfig(
            chain=Chain.ARBITRUM,
            requests_per_minute=50,
            supply_apy_fallback=Decimal("0.05"),
            borrow_apy_fallback=Decimal("0.08"),
        )
        provider = AaveV3APYProvider(config=config)
        assert provider.config.chain == Chain.ARBITRUM
        assert provider.config.requests_per_minute == 50
        assert provider.config.supply_apy_fallback == Decimal("0.05")
        assert provider.config.borrow_apy_fallback == Decimal("0.08")

    def test_init_with_provided_client(self):
        """Test provider uses provided SubgraphClient."""
        mock_client = MagicMock(spec=SubgraphClient)
        provider = AaveV3APYProvider(client=mock_client)
        assert provider._client is mock_client
        assert provider._owns_client is False

    def test_supported_chains_property_returns_copy(self):
        """Test supported_chains returns a copy, not the original."""
        provider = AaveV3APYProvider()
        chains1 = provider.supported_chains
        chains2 = provider.supported_chains
        assert chains1 == chains2
        assert chains1 is not chains2


class TestSupportedChains:
    """Tests for supported chains configuration."""

    def test_supported_chains_include_required_networks(self):
        """Test that all required networks are supported (US-017)."""
        # US-017 requires: Ethereum, Arbitrum, Optimism, Polygon, Base, Avalanche
        assert Chain.ETHEREUM in SUPPORTED_CHAINS
        assert Chain.ARBITRUM in SUPPORTED_CHAINS
        assert Chain.OPTIMISM in SUPPORTED_CHAINS
        assert Chain.POLYGON in SUPPORTED_CHAINS
        assert Chain.BASE in SUPPORTED_CHAINS
        assert Chain.AVALANCHE in SUPPORTED_CHAINS

    def test_all_supported_chains_have_subgraph_ids(self):
        """Test all supported chains have subgraph IDs."""
        for chain in SUPPORTED_CHAINS:
            assert chain in AAVE_V3_SUBGRAPH_IDS
            assert AAVE_V3_SUBGRAPH_IDS[chain]  # Non-empty

    def test_subgraph_ids_are_valid_format(self):
        """Test subgraph IDs have valid format (base58-like)."""
        for chain, subgraph_id in AAVE_V3_SUBGRAPH_IDS.items():
            # Subgraph IDs are base58-like strings
            assert len(subgraph_id) >= 40
            assert all(c.isalnum() for c in subgraph_id)


class TestRAYConversion:
    """Tests for RAY unit conversion."""

    def test_ray_to_decimal_basic(self):
        """Test basic RAY to decimal conversion."""
        provider = AaveV3APYProvider()

        # 3% APY in RAY = 0.03 * 1e27 = 3e25
        ray_value = "30000000000000000000000000"  # 3e25
        result = provider._ray_to_decimal(ray_value)
        assert result == Decimal("0.03")

    def test_ray_to_decimal_high_precision(self):
        """Test RAY conversion with high precision values."""
        provider = AaveV3APYProvider()

        # 5.5% APY in RAY
        ray_value = "55000000000000000000000000"  # 5.5e25
        result = provider._ray_to_decimal(ray_value)
        assert result == Decimal("0.055")

    def test_ray_to_decimal_zero(self):
        """Test RAY conversion with zero value."""
        provider = AaveV3APYProvider()
        result = provider._ray_to_decimal("0")
        assert result == Decimal("0")

    def test_ray_to_decimal_invalid_returns_zero(self):
        """Test RAY conversion with invalid value returns zero."""
        provider = AaveV3APYProvider()
        assert provider._ray_to_decimal("invalid") == Decimal("0")
        assert provider._ray_to_decimal(None) == Decimal("0")


class TestMarketSymbolNormalization:
    """Tests for market symbol normalization."""

    def test_normalize_uppercase(self):
        """Test symbols are converted to uppercase."""
        provider = AaveV3APYProvider()
        assert provider._normalize_market_symbol("usdc") == "USDC"
        assert provider._normalize_market_symbol("USDC") == "USDC"
        assert provider._normalize_market_symbol("Usdc") == "USDC"

    def test_normalize_strips_whitespace(self):
        """Test whitespace is stripped."""
        provider = AaveV3APYProvider()
        assert provider._normalize_market_symbol("  USDC  ") == "USDC"

    def test_normalize_eth_to_weth(self):
        """Test ETH is converted to WETH."""
        provider = AaveV3APYProvider()
        assert provider._normalize_market_symbol("ETH") == "WETH"
        assert provider._normalize_market_symbol("eth") == "WETH"


class TestGetAPY:
    """Tests for get_apy method."""

    @pytest.mark.asyncio
    async def test_get_apy_success(self):
        """Test successfully fetching APY data."""
        provider = AaveV3APYProvider()

        # Mock reserve lookup
        reserve_response = {
            "reserves": [
                {
                    "id": "0xusdc-0xpool-0",
                    "symbol": "USDC",
                    "name": "USD Coin",
                    "decimals": 6,
                    "underlyingAsset": "0xusdc",
                }
            ]
        }

        # Mock history lookup - 3% supply, 5% borrow in RAY
        history_response = {
            "reserveParamsHistoryItems": [
                {
                    "id": "item1",
                    "timestamp": 1704067200,  # 2024-01-01 00:00:00 UTC
                    "liquidityRate": "30000000000000000000000000",  # 3% in RAY
                    "variableBorrowRate": "50000000000000000000000000",  # 5% in RAY
                    "stableBorrowRate": "60000000000000000000000000",
                    "utilizationRate": "800000000000000000000000000",
                },
                {
                    "id": "item2",
                    "timestamp": 1704153600,  # 2024-01-02 00:00:00 UTC
                    "liquidityRate": "32000000000000000000000000",  # 3.2% in RAY
                    "variableBorrowRate": "52000000000000000000000000",  # 5.2% in RAY
                    "stableBorrowRate": "62000000000000000000000000",
                    "utilizationRate": "820000000000000000000000000",
                },
            ]
        }

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(side_effect=[reserve_response, history_response])

        provider._client = mock_client
        provider._owns_client = False

        apys = await provider.get_apy(
            protocol="aave_v3",
            market="USDC",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 2, tzinfo=UTC),
        )

        assert len(apys) == 2
        assert apys[0].supply_apy == Decimal("0.03")
        assert apys[0].borrow_apy == Decimal("0.05")
        assert apys[0].source_info.source == DATA_SOURCE
        assert apys[0].source_info.confidence == DataConfidence.HIGH

        assert apys[1].supply_apy == Decimal("0.032")
        assert apys[1].borrow_apy == Decimal("0.052")

    @pytest.mark.asyncio
    async def test_get_apy_reserve_not_found(self):
        """Test behavior when reserve not found."""
        provider = AaveV3APYProvider()

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(return_value={"reserves": []})

        provider._client = mock_client
        provider._owns_client = False

        apys = await provider.get_apy(
            protocol="aave_v3",
            market="NONEXISTENT",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 2, tzinfo=UTC),
        )

        # Should return fallback results
        assert len(apys) >= 1
        for apy in apys:
            assert apy.source_info.confidence == DataConfidence.LOW
            assert apy.source_info.source == "fallback"
            assert apy.supply_apy == DEFAULT_SUPPLY_APY_FALLBACK
            assert apy.borrow_apy == DEFAULT_BORROW_APY_FALLBACK

    @pytest.mark.asyncio
    async def test_get_apy_no_history_data(self):
        """Test behavior when no history data available."""
        provider = AaveV3APYProvider()

        reserve_response = {
            "reserves": [
                {
                    "id": "0xusdc-0xpool-0",
                    "symbol": "USDC",
                    "name": "USD Coin",
                    "decimals": 6,
                    "underlyingAsset": "0xusdc",
                }
            ]
        }
        history_response = {"reserveParamsHistoryItems": []}

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(side_effect=[reserve_response, history_response])

        provider._client = mock_client
        provider._owns_client = False

        apys = await provider.get_apy(
            protocol="aave_v3",
            market="USDC",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 2, tzinfo=UTC),
        )

        # Should return fallback results
        assert len(apys) >= 1
        for apy in apys:
            assert apy.source_info.confidence == DataConfidence.LOW

    @pytest.mark.asyncio
    async def test_get_apy_adds_timezone_if_missing(self):
        """Test that timezone is added to naive datetimes."""
        provider = AaveV3APYProvider()

        reserve_response = {
            "reserves": [{"id": "0xusdc-0xpool-0", "symbol": "USDC"}]
        }
        history_response = {
            "reserveParamsHistoryItems": [
                {
                    "timestamp": 1704067200,
                    "liquidityRate": "30000000000000000000000000",
                    "variableBorrowRate": "50000000000000000000000000",
                }
            ]
        }

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(side_effect=[reserve_response, history_response])

        provider._client = mock_client
        provider._owns_client = False

        # Pass naive datetimes (no timezone)
        apys = await provider.get_apy(
            protocol="aave_v3",
            market="USDC",
            start_date=datetime(2024, 1, 1),  # No timezone
            end_date=datetime(2024, 1, 2),  # No timezone
        )

        # Should work without error
        assert len(apys) >= 1


class TestErrorHandling:
    """Tests for error handling in APY fetching."""

    @pytest.mark.asyncio
    async def test_rate_limit_error_returns_fallback(self):
        """Test that rate limit error returns fallback results."""
        config = AaveV3ClientConfig(
            supply_apy_fallback=Decimal("0.04"),
            borrow_apy_fallback=Decimal("0.07"),
        )
        provider = AaveV3APYProvider(config=config)

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(side_effect=SubgraphRateLimitError("Rate limit exceeded"))

        provider._client = mock_client
        provider._owns_client = False

        apys = await provider.get_apy(
            protocol="aave_v3",
            market="USDC",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 2, tzinfo=UTC),
        )

        assert len(apys) >= 1
        for apy in apys:
            assert apy.supply_apy == Decimal("0.04")
            assert apy.borrow_apy == Decimal("0.07")
            assert apy.source_info.confidence == DataConfidence.LOW

    @pytest.mark.asyncio
    async def test_query_error_returns_fallback(self):
        """Test that query error returns fallback results."""
        provider = AaveV3APYProvider()

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(side_effect=SubgraphQueryError("Query failed"))

        provider._client = mock_client
        provider._owns_client = False

        apys = await provider.get_apy(
            protocol="aave_v3",
            market="USDC",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 2, tzinfo=UTC),
        )

        assert len(apys) >= 1
        for apy in apys:
            assert apy.source_info.confidence == DataConfidence.LOW

    @pytest.mark.asyncio
    async def test_unexpected_error_returns_fallback(self):
        """Test that unexpected error returns fallback results."""
        provider = AaveV3APYProvider()

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(side_effect=Exception("Unexpected error"))

        provider._client = mock_client
        provider._owns_client = False

        apys = await provider.get_apy(
            protocol="aave_v3",
            market="USDC",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 2, tzinfo=UTC),
        )

        assert len(apys) >= 1
        for apy in apys:
            assert apy.source_info.confidence == DataConfidence.LOW


class TestGetAPYForChain:
    """Tests for get_apy_for_chain method."""

    @pytest.mark.asyncio
    async def test_get_apy_for_chain_overrides_config_chain(self):
        """Test that get_apy_for_chain uses specified chain."""
        config = AaveV3ClientConfig(chain=Chain.ETHEREUM)
        provider = AaveV3APYProvider(config=config)

        reserve_response = {
            "reserves": [{"id": "0xusdc-0xpool-0", "symbol": "USDC"}]
        }
        history_response = {
            "reserveParamsHistoryItems": [
                {
                    "timestamp": 1704067200,
                    "liquidityRate": "30000000000000000000000000",
                    "variableBorrowRate": "50000000000000000000000000",
                }
            ]
        }

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(side_effect=[reserve_response, history_response])

        provider._client = mock_client
        provider._owns_client = False

        # Query for Arbitrum instead of default Ethereum
        await provider.get_apy_for_chain(
            chain=Chain.ARBITRUM,
            market="USDC",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 1, tzinfo=UTC),
        )

        # Verify the subgraph ID used was for Arbitrum
        call_args = mock_client.query.call_args_list[0]
        subgraph_id = call_args.kwargs.get("subgraph_id") or call_args.args[0]
        assert subgraph_id == AAVE_V3_SUBGRAPH_IDS[Chain.ARBITRUM]

    @pytest.mark.asyncio
    async def test_get_apy_for_chain_restores_original_chain(self):
        """Test that original chain is restored after query."""
        config = AaveV3ClientConfig(chain=Chain.ETHEREUM)
        provider = AaveV3APYProvider(config=config)

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(return_value={"reserves": []})

        provider._client = mock_client
        provider._owns_client = False

        # Query for Arbitrum
        await provider.get_apy_for_chain(
            chain=Chain.ARBITRUM,
            market="USDC",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 1, tzinfo=UTC),
        )

        # Original chain should be restored
        assert provider.config.chain == Chain.ETHEREUM


class TestGetCurrentAPY:
    """Tests for get_current_apy method."""

    @pytest.mark.asyncio
    async def test_get_current_apy_returns_latest(self):
        """Test that get_current_apy returns the most recent APY."""
        provider = AaveV3APYProvider()

        reserve_response = {
            "reserves": [{"id": "0xusdc-0xpool-0", "symbol": "USDC"}]
        }
        history_response = {
            "reserveParamsHistoryItems": [
                {
                    "timestamp": 1704067200,  # Earlier
                    "liquidityRate": "30000000000000000000000000",
                    "variableBorrowRate": "50000000000000000000000000",
                },
                {
                    "timestamp": 1704153600,  # Later
                    "liquidityRate": "35000000000000000000000000",  # 3.5%
                    "variableBorrowRate": "55000000000000000000000000",  # 5.5%
                },
            ]
        }

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(side_effect=[reserve_response, history_response])

        provider._client = mock_client
        provider._owns_client = False

        apy = await provider.get_current_apy("USDC")

        # Should return the latest (second) result
        assert apy.supply_apy == Decimal("0.035")
        assert apy.borrow_apy == Decimal("0.055")

    @pytest.mark.asyncio
    async def test_get_current_apy_no_data_returns_fallback(self):
        """Test that get_current_apy returns fallback when no data."""
        provider = AaveV3APYProvider()

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(return_value={"reserves": []})

        provider._client = mock_client
        provider._owns_client = False

        apy = await provider.get_current_apy("NONEXISTENT")

        assert apy.source_info.confidence == DataConfidence.LOW


class TestContextManager:
    """Tests for async context manager behavior."""

    @pytest.mark.asyncio
    async def test_context_manager_closes_client(self):
        """Test that context manager closes client on exit."""
        provider = AaveV3APYProvider()

        # Create a mock client
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.close = AsyncMock()
        provider._client = mock_client
        provider._owns_client = True  # We own it, so should close

        async with provider:
            pass

        mock_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager_does_not_close_external_client(self):
        """Test that context manager doesn't close externally provided client."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.close = AsyncMock()

        provider = AaveV3APYProvider(client=mock_client)
        assert provider._owns_client is False

        async with provider:
            pass

        # Should NOT close external client
        mock_client.close.assert_not_called()


class TestReserveIDCaching:
    """Tests for reserve ID caching."""

    @pytest.mark.asyncio
    async def test_reserve_id_is_cached(self):
        """Test that reserve ID is cached after first lookup."""
        provider = AaveV3APYProvider()

        reserve_response = {
            "reserves": [{"id": "0xusdc-0xpool-0", "symbol": "USDC"}]
        }

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(return_value=reserve_response)

        provider._client = mock_client
        provider._owns_client = False

        # First lookup
        reserve_id1 = await provider._find_reserve_id(Chain.ETHEREUM, "USDC")
        assert reserve_id1 == "0xusdc-0xpool-0"
        assert mock_client.query.call_count == 1

        # Second lookup should use cache
        reserve_id2 = await provider._find_reserve_id(Chain.ETHEREUM, "USDC")
        assert reserve_id2 == "0xusdc-0xpool-0"
        # Query count should still be 1 (cached)
        assert mock_client.query.call_count == 1


class TestUnsupportedChain:
    """Tests for unsupported chain handling."""

    @pytest.mark.asyncio
    async def test_unsupported_chain_returns_fallback(self):
        """Test that unsupported chain returns fallback results."""
        # Use a chain not in SUPPORTED_CHAINS
        config = AaveV3ClientConfig(chain=Chain.PLASMA)
        provider = AaveV3APYProvider(config=config)

        apys = await provider.get_apy(
            protocol="aave_v3",
            market="USDC",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 2, tzinfo=UTC),
        )

        assert len(apys) >= 1
        for apy in apys:
            assert apy.source_info.confidence == DataConfidence.LOW

    def test_get_subgraph_id_unsupported_returns_none(self):
        """Test that _get_subgraph_id returns None for unsupported chain."""
        provider = AaveV3APYProvider()
        # PLASMA is not in AAVE_V3_SUBGRAPH_IDS
        assert provider._get_subgraph_id(Chain.PLASMA) is None


class TestFallbackResultGeneration:
    """Tests for fallback result generation."""

    def test_generate_fallback_results_daily(self):
        """Test fallback results are generated for each day."""
        config = AaveV3ClientConfig(
            supply_apy_fallback=Decimal("0.025"),
            borrow_apy_fallback=Decimal("0.045"),
        )
        provider = AaveV3APYProvider(config=config)

        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 5, tzinfo=UTC)

        results = provider._generate_fallback_results(start, end)

        # Should have 5 days of results
        assert len(results) == 5
        for result in results:
            assert result.supply_apy == Decimal("0.025")
            assert result.borrow_apy == Decimal("0.045")
            assert result.source_info.confidence == DataConfidence.LOW
            assert result.source_info.source == "fallback"

    def test_create_fallback_result(self):
        """Test single fallback result creation."""
        provider = AaveV3APYProvider()
        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)

        result = provider._create_fallback_result(timestamp)

        assert result.supply_apy == DEFAULT_SUPPLY_APY_FALLBACK
        assert result.borrow_apy == DEFAULT_BORROW_APY_FALLBACK
        assert result.source_info.timestamp == timestamp
        assert result.source_info.confidence == DataConfidence.LOW


class TestAPYDataParsing:
    """Tests for APY data parsing."""

    def test_parse_apy_data(self):
        """Test parsing APY data from subgraph response."""
        provider = AaveV3APYProvider()

        history_item = {
            "id": "item1",
            "timestamp": 1704067200,  # 2024-01-01 00:00:00 UTC
            "liquidityRate": "30000000000000000000000000",  # 3% in RAY
            "variableBorrowRate": "50000000000000000000000000",  # 5% in RAY
            "stableBorrowRate": "60000000000000000000000000",
            "utilizationRate": "800000000000000000000000000",
        }

        result = provider._parse_apy_data(history_item)

        assert result.supply_apy == Decimal("0.03")
        assert result.borrow_apy == Decimal("0.05")
        assert result.source_info.source == DATA_SOURCE
        assert result.source_info.confidence == DataConfidence.HIGH
        assert result.source_info.timestamp == datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

    def test_parse_apy_data_missing_fields(self):
        """Test parsing with missing optional fields returns zeros."""
        provider = AaveV3APYProvider()

        history_item = {
            "timestamp": 1704067200,
            # Missing liquidityRate and variableBorrowRate
        }

        result = provider._parse_apy_data(history_item)

        assert result.supply_apy == Decimal("0")
        assert result.borrow_apy == Decimal("0")
