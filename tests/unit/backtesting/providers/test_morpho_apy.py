"""Unit tests for Morpho Blue APY Provider.

This module tests the MorphoBlueAPYProvider class in providers/lending/morpho_apy.py,
covering:
- Provider initialization and configuration
- Supported chains and subgraph ID mapping
- APY fetching with mocked responses
- Messari schema rate extraction (LENDER/BORROWER sides)
- Fallback behavior when subgraph unavailable
- Error handling for query failures
- Market ID resolution and caching
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.core.enums import Chain
from almanak.framework.backtesting.pnl.providers.lending.morpho_apy import (
    BORROWER_SIDE,
    DATA_SOURCE,
    DEFAULT_BORROW_APY_FALLBACK,
    DEFAULT_SUPPLY_APY_FALLBACK,
    LENDER_SIDE,
    MORPHO_BLUE_SUBGRAPH_IDS,
    SUPPORTED_CHAINS,
    MorphoBlueAPYProvider,
    MorphoBlueClientConfig,
)
from almanak.framework.backtesting.pnl.providers.subgraph_client import (
    SubgraphClient,
    SubgraphQueryError,
    SubgraphRateLimitError,
)
from almanak.framework.backtesting.pnl.types import DataConfidence


class TestMorphoBlueAPYProviderInitialization:
    """Tests for MorphoBlueAPYProvider initialization."""

    def test_init_default(self):
        """Test provider initializes with default settings."""
        provider = MorphoBlueAPYProvider()
        assert provider.supported_chains == SUPPORTED_CHAINS
        assert provider.config.chain == Chain.ETHEREUM
        assert provider.config.requests_per_minute == 100
        assert provider._owns_client is True

    def test_init_with_custom_config(self):
        """Test provider initializes with custom config."""
        config = MorphoBlueClientConfig(
            chain=Chain.BASE,
            requests_per_minute=50,
            supply_apy_fallback=Decimal("0.05"),
            borrow_apy_fallback=Decimal("0.08"),
        )
        provider = MorphoBlueAPYProvider(config=config)
        assert provider.config.chain == Chain.BASE
        assert provider.config.requests_per_minute == 50
        assert provider.config.supply_apy_fallback == Decimal("0.05")
        assert provider.config.borrow_apy_fallback == Decimal("0.08")

    def test_init_with_provided_client(self):
        """Test provider uses provided SubgraphClient."""
        mock_client = MagicMock(spec=SubgraphClient)
        provider = MorphoBlueAPYProvider(client=mock_client)
        assert provider._client is mock_client
        assert provider._owns_client is False

    def test_supported_chains_property_returns_copy(self):
        """Test supported_chains returns a copy, not the original."""
        provider = MorphoBlueAPYProvider()
        chains1 = provider.supported_chains
        chains2 = provider.supported_chains
        assert chains1 == chains2
        assert chains1 is not chains2


class TestSupportedChains:
    """Tests for supported chains configuration."""

    def test_supported_chains_include_required_networks(self):
        """Test that all required networks are supported (US-019)."""
        # US-019 requires: Ethereum, Base
        assert Chain.ETHEREUM in SUPPORTED_CHAINS
        assert Chain.BASE in SUPPORTED_CHAINS

    def test_all_supported_chains_have_subgraph_ids(self):
        """Test all supported chains have subgraph IDs."""
        for chain in SUPPORTED_CHAINS:
            assert chain in MORPHO_BLUE_SUBGRAPH_IDS
            assert MORPHO_BLUE_SUBGRAPH_IDS[chain]  # Non-empty

    def test_subgraph_ids_are_valid_format(self):
        """Test subgraph IDs have valid format (base58-like)."""
        for chain, subgraph_id in MORPHO_BLUE_SUBGRAPH_IDS.items():
            # Subgraph IDs are base58-like strings
            assert len(subgraph_id) >= 40
            assert all(c.isalnum() for c in subgraph_id)


class TestDecimalParsing:
    """Tests for decimal parsing and rate conversion."""

    def test_parse_decimal_basic(self):
        """Test basic decimal parsing with percentage conversion."""
        provider = MorphoBlueAPYProvider()

        # 3% APY in Messari format = "3.0" (percentage)
        result = provider._parse_decimal("3.0")
        assert result == Decimal("0.03")  # Converted to decimal

    def test_parse_decimal_high_precision(self):
        """Test decimal parsing with high precision values."""
        provider = MorphoBlueAPYProvider()

        # 5.5% APY
        result = provider._parse_decimal("5.5")
        assert result == Decimal("0.055")

    def test_parse_decimal_zero(self):
        """Test decimal parsing with zero value."""
        provider = MorphoBlueAPYProvider()
        result = provider._parse_decimal("0")
        assert result == Decimal("0")

    def test_parse_decimal_invalid_returns_zero(self):
        """Test decimal parsing with invalid value returns zero."""
        provider = MorphoBlueAPYProvider()
        assert provider._parse_decimal("invalid") == Decimal("0")
        assert provider._parse_decimal(None) == Decimal("0")


class TestMarketIdNormalization:
    """Tests for market ID normalization."""

    def test_normalize_lowercase(self):
        """Test market IDs are converted to lowercase."""
        provider = MorphoBlueAPYProvider()
        assert provider._normalize_market_id("0xABC123") == "0xabc123"
        assert provider._normalize_market_id("0xabc123") == "0xabc123"

    def test_normalize_strips_whitespace(self):
        """Test whitespace is stripped."""
        provider = MorphoBlueAPYProvider()
        assert provider._normalize_market_id("  0xabc123  ") == "0xabc123"


class TestRateExtraction:
    """Tests for extracting rates from Messari schema rates array."""

    def test_extract_rates_both_sides(self):
        """Test extracting both supply and borrow rates."""
        provider = MorphoBlueAPYProvider()

        rates = [
            {"id": "rate1", "rate": "3.5", "side": LENDER_SIDE, "type": "VARIABLE"},
            {"id": "rate2", "rate": "5.0", "side": BORROWER_SIDE, "type": "VARIABLE"},
        ]

        supply_apy, borrow_apy = provider._extract_rates_from_snapshot(rates)

        assert supply_apy == Decimal("0.035")  # 3.5% -> 0.035
        assert borrow_apy == Decimal("0.05")  # 5.0% -> 0.05

    def test_extract_rates_only_lender(self):
        """Test extracting when only lender rate present."""
        provider = MorphoBlueAPYProvider()

        rates = [
            {"id": "rate1", "rate": "3.5", "side": LENDER_SIDE, "type": "VARIABLE"},
        ]

        supply_apy, borrow_apy = provider._extract_rates_from_snapshot(rates)

        assert supply_apy == Decimal("0.035")
        assert borrow_apy == Decimal("0")

    def test_extract_rates_empty(self):
        """Test extracting from empty rates array."""
        provider = MorphoBlueAPYProvider()

        supply_apy, borrow_apy = provider._extract_rates_from_snapshot([])

        assert supply_apy == Decimal("0")
        assert borrow_apy == Decimal("0")


class TestGetAPY:
    """Tests for get_apy method."""

    @pytest.mark.asyncio
    async def test_get_apy_success(self):
        """Test successfully fetching APY data."""
        provider = MorphoBlueAPYProvider()

        # Mock snapshots response
        snapshots_response = {
            "marketDailySnapshots": [
                {
                    "id": "snapshot1",
                    "days": 19723,  # 2024-01-01
                    "timestamp": 1704067200,  # 2024-01-01 00:00:00 UTC
                    "rates": [
                        {"id": "r1", "rate": "3.0", "side": LENDER_SIDE, "type": "VARIABLE"},
                        {"id": "r2", "rate": "5.0", "side": BORROWER_SIDE, "type": "VARIABLE"},
                    ],
                },
                {
                    "id": "snapshot2",
                    "days": 19723,  # 2024-01-02
                    "timestamp": 1704153600,  # 2024-01-02 00:00:00 UTC
                    "rates": [
                        {"id": "r3", "rate": "3.2", "side": LENDER_SIDE, "type": "VARIABLE"},
                        {"id": "r4", "rate": "5.2", "side": BORROWER_SIDE, "type": "VARIABLE"},
                    ],
                },
            ]
        }

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(return_value=snapshots_response)

        provider._client = mock_client
        provider._owns_client = False

        # Use a market ID directly (0x prefix)
        apys = await provider.get_apy(
            protocol="morpho_blue",
            market="0x1234567890abcdef1234567890abcdef12345678",
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
    async def test_get_apy_with_token_symbol(self):
        """Test fetching APY by token symbol."""
        provider = MorphoBlueAPYProvider()

        # Mock market lookup response
        market_response = {
            "markets": [
                {
                    "id": "0xmarket123",
                    "name": "USDC Market",
                    "inputToken": {"id": "0xusdc", "symbol": "USDC", "name": "USD Coin"},
                }
            ]
        }

        # Mock snapshots response
        snapshots_response = {
            "marketDailySnapshots": [
                {
                    "id": "snapshot1",
                    "days": 19723,
                    "timestamp": 1704067200,
                    "rates": [
                        {"id": "r1", "rate": "3.0", "side": LENDER_SIDE, "type": "VARIABLE"},
                        {"id": "r2", "rate": "5.0", "side": BORROWER_SIDE, "type": "VARIABLE"},
                    ],
                }
            ]
        }

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(side_effect=[market_response, snapshots_response])

        provider._client = mock_client
        provider._owns_client = False

        apys = await provider.get_apy(
            protocol="morpho_blue",
            market="USDC",  # Token symbol
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 2, tzinfo=UTC),
        )

        assert len(apys) == 1
        assert apys[0].supply_apy == Decimal("0.03")

    @pytest.mark.asyncio
    async def test_get_apy_market_not_found(self):
        """Test behavior when market not found by symbol."""
        provider = MorphoBlueAPYProvider()

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(return_value={"markets": []})

        provider._client = mock_client
        provider._owns_client = False

        apys = await provider.get_apy(
            protocol="morpho_blue",
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
    async def test_get_apy_no_snapshot_data(self):
        """Test behavior when no snapshot data available."""
        provider = MorphoBlueAPYProvider()

        snapshots_response = {"marketDailySnapshots": []}

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(return_value=snapshots_response)

        provider._client = mock_client
        provider._owns_client = False

        apys = await provider.get_apy(
            protocol="morpho_blue",
            market="0x1234567890abcdef1234567890abcdef12345678",
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
        provider = MorphoBlueAPYProvider()

        snapshots_response = {
            "marketDailySnapshots": [
                {
                    "days": 19723,
                    "timestamp": 1704067200,
                    "rates": [
                        {"rate": "3.0", "side": LENDER_SIDE, "type": "VARIABLE"},
                        {"rate": "5.0", "side": BORROWER_SIDE, "type": "VARIABLE"},
                    ],
                }
            ]
        }

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(return_value=snapshots_response)

        provider._client = mock_client
        provider._owns_client = False

        # Pass naive datetimes (no timezone)
        apys = await provider.get_apy(
            protocol="morpho_blue",
            market="0x1234567890abcdef1234567890abcdef12345678",
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
        config = MorphoBlueClientConfig(
            supply_apy_fallback=Decimal("0.04"),
            borrow_apy_fallback=Decimal("0.07"),
        )
        provider = MorphoBlueAPYProvider(config=config)

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(side_effect=SubgraphRateLimitError("Rate limit exceeded"))

        provider._client = mock_client
        provider._owns_client = False

        apys = await provider.get_apy(
            protocol="morpho_blue",
            market="0x1234567890abcdef1234567890abcdef12345678",
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
        provider = MorphoBlueAPYProvider()

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(side_effect=SubgraphQueryError("Query failed"))

        provider._client = mock_client
        provider._owns_client = False

        apys = await provider.get_apy(
            protocol="morpho_blue",
            market="0x1234567890abcdef1234567890abcdef12345678",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 2, tzinfo=UTC),
        )

        assert len(apys) >= 1
        for apy in apys:
            assert apy.source_info.confidence == DataConfidence.LOW

    @pytest.mark.asyncio
    async def test_unexpected_error_returns_fallback(self):
        """Test that unexpected error returns fallback results."""
        provider = MorphoBlueAPYProvider()

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(side_effect=Exception("Unexpected error"))

        provider._client = mock_client
        provider._owns_client = False

        apys = await provider.get_apy(
            protocol="morpho_blue",
            market="0x1234567890abcdef1234567890abcdef12345678",
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
        config = MorphoBlueClientConfig(chain=Chain.ETHEREUM)
        provider = MorphoBlueAPYProvider(config=config)

        snapshots_response = {
            "marketDailySnapshots": [
                {
                    "days": 19723,
                    "timestamp": 1704067200,
                    "rates": [
                        {"rate": "3.0", "side": LENDER_SIDE, "type": "VARIABLE"},
                        {"rate": "5.0", "side": BORROWER_SIDE, "type": "VARIABLE"},
                    ],
                }
            ]
        }

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(return_value=snapshots_response)

        provider._client = mock_client
        provider._owns_client = False

        # Query for Base instead of default Ethereum
        await provider.get_apy_for_chain(
            chain=Chain.BASE,
            market="0x1234567890abcdef1234567890abcdef12345678",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 1, tzinfo=UTC),
        )

        # Verify the subgraph ID used was for Base
        call_args = mock_client.query.call_args_list[0]
        subgraph_id = call_args.kwargs.get("subgraph_id") or call_args.args[0]
        assert subgraph_id == MORPHO_BLUE_SUBGRAPH_IDS[Chain.BASE]

    @pytest.mark.asyncio
    async def test_get_apy_for_chain_restores_original_chain(self):
        """Test that original chain is restored after query."""
        config = MorphoBlueClientConfig(chain=Chain.ETHEREUM)
        provider = MorphoBlueAPYProvider(config=config)

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(return_value={"marketDailySnapshots": []})

        provider._client = mock_client
        provider._owns_client = False

        # Query for Base
        await provider.get_apy_for_chain(
            chain=Chain.BASE,
            market="0x1234567890abcdef1234567890abcdef12345678",
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
        provider = MorphoBlueAPYProvider()

        snapshots_response = {
            "marketDailySnapshots": [
                {
                    "days": 19723,  # Earlier
                    "timestamp": 1704067200,
                    "rates": [
                        {"rate": "3.0", "side": LENDER_SIDE, "type": "VARIABLE"},
                        {"rate": "5.0", "side": BORROWER_SIDE, "type": "VARIABLE"},
                    ],
                },
                {
                    "days": 19724,  # Later
                    "timestamp": 1704153600,
                    "rates": [
                        {"rate": "3.5", "side": LENDER_SIDE, "type": "VARIABLE"},
                        {"rate": "5.5", "side": BORROWER_SIDE, "type": "VARIABLE"},
                    ],
                },
            ]
        }

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(return_value=snapshots_response)

        provider._client = mock_client
        provider._owns_client = False

        apy = await provider.get_current_apy("0x1234567890abcdef1234567890abcdef12345678")

        # Should return the latest (second) result
        assert apy.supply_apy == Decimal("0.035")
        assert apy.borrow_apy == Decimal("0.055")

    @pytest.mark.asyncio
    async def test_get_current_apy_no_data_returns_fallback(self):
        """Test that get_current_apy returns fallback when no data."""
        provider = MorphoBlueAPYProvider()

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(return_value={"marketDailySnapshots": []})

        provider._client = mock_client
        provider._owns_client = False

        apy = await provider.get_current_apy("0x1234567890abcdef1234567890abcdef12345678")

        assert apy.source_info.confidence == DataConfidence.LOW


class TestContextManager:
    """Tests for async context manager behavior."""

    @pytest.mark.asyncio
    async def test_context_manager_closes_client(self):
        """Test that context manager closes client on exit."""
        provider = MorphoBlueAPYProvider()

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

        provider = MorphoBlueAPYProvider(client=mock_client)
        assert provider._owns_client is False

        async with provider:
            pass

        # Should NOT close external client
        mock_client.close.assert_not_called()


class TestMarketIDCaching:
    """Tests for market ID caching."""

    @pytest.mark.asyncio
    async def test_market_id_is_cached(self):
        """Test that market ID is cached after first lookup."""
        provider = MorphoBlueAPYProvider()

        market_response = {
            "markets": [
                {
                    "id": "0xmarket123",
                    "name": "USDC Market",
                    "inputToken": {"id": "0xusdc", "symbol": "USDC", "name": "USD Coin"},
                }
            ]
        }

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(return_value=market_response)

        provider._client = mock_client
        provider._owns_client = False

        # First lookup
        market_id1 = await provider._find_market_by_token(Chain.ETHEREUM, "USDC")
        assert market_id1 == "0xmarket123"
        assert mock_client.query.call_count == 1

        # Second lookup should use cache
        market_id2 = await provider._find_market_by_token(Chain.ETHEREUM, "USDC")
        assert market_id2 == "0xmarket123"
        # Query count should still be 1 (cached)
        assert mock_client.query.call_count == 1


class TestUnsupportedChain:
    """Tests for unsupported chain handling."""

    @pytest.mark.asyncio
    async def test_unsupported_chain_returns_fallback(self):
        """Test that unsupported chain returns fallback results."""
        # Use a chain not in SUPPORTED_CHAINS
        config = MorphoBlueClientConfig(chain=Chain.ARBITRUM)  # Not supported
        provider = MorphoBlueAPYProvider(config=config)

        apys = await provider.get_apy(
            protocol="morpho_blue",
            market="0x1234567890abcdef1234567890abcdef12345678",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 2, tzinfo=UTC),
        )

        assert len(apys) >= 1
        for apy in apys:
            assert apy.source_info.confidence == DataConfidence.LOW

    def test_get_subgraph_id_unsupported_returns_none(self):
        """Test that _get_subgraph_id returns None for unsupported chain."""
        provider = MorphoBlueAPYProvider()
        # ARBITRUM is not in MORPHO_BLUE_SUBGRAPH_IDS
        assert provider._get_subgraph_id(Chain.ARBITRUM) is None


class TestFallbackResultGeneration:
    """Tests for fallback result generation."""

    def test_generate_fallback_results_daily(self):
        """Test fallback results are generated for each day."""
        config = MorphoBlueClientConfig(
            supply_apy_fallback=Decimal("0.025"),
            borrow_apy_fallback=Decimal("0.045"),
        )
        provider = MorphoBlueAPYProvider(config=config)

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
        provider = MorphoBlueAPYProvider()
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
        provider = MorphoBlueAPYProvider()

        snapshot = {
            "id": "snapshot1",
            "days": 19724,
            "timestamp": 1704067200,  # 2024-01-01 00:00:00 UTC
            "rates": [
                {"id": "r1", "rate": "3.0", "side": LENDER_SIDE, "type": "VARIABLE"},
                {"id": "r2", "rate": "5.0", "side": BORROWER_SIDE, "type": "VARIABLE"},
            ],
        }

        result = provider._parse_apy_data(snapshot)

        assert result.supply_apy == Decimal("0.03")
        assert result.borrow_apy == Decimal("0.05")
        assert result.source_info.source == DATA_SOURCE
        assert result.source_info.confidence == DataConfidence.HIGH
        assert result.source_info.timestamp == datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

    def test_parse_apy_data_missing_rates(self):
        """Test parsing with missing rates array returns zeros."""
        provider = MorphoBlueAPYProvider()

        snapshot = {
            "timestamp": 1704067200,
            "rates": [],  # Empty rates
        }

        result = provider._parse_apy_data(snapshot)

        assert result.supply_apy == Decimal("0")
        assert result.borrow_apy == Decimal("0")


class TestDateToDayConversion:
    """Tests for date to day number conversion."""

    def test_date_to_day_number(self):
        """Test converting datetime to day number since epoch."""
        provider = MorphoBlueAPYProvider()

        # January 1, 2024 is day 19723 since epoch (1970-01-01)
        dt = datetime(2024, 1, 1, tzinfo=UTC)
        day_num = provider._date_to_day_number(dt)

        # Calculate expected: (date(2024, 1, 1) - date(1970, 1, 1)).days = 19723
        assert day_num == 19723

    def test_date_to_day_number_with_date_object(self):
        """Test converting date object to day number."""
        from datetime import date

        provider = MorphoBlueAPYProvider()

        d = date(2024, 1, 1)
        day_num = provider._date_to_day_number(d)

        assert day_num == 19723


class TestListMarkets:
    """Tests for list_markets method."""

    @pytest.mark.asyncio
    async def test_list_markets_success(self):
        """Test successfully listing markets."""
        provider = MorphoBlueAPYProvider()

        markets_response = {
            "markets": [
                {
                    "id": "0xmarket1",
                    "name": "USDC Market",
                    "inputToken": {"id": "0xusdc", "symbol": "USDC", "name": "USD Coin"},
                },
                {
                    "id": "0xmarket2",
                    "name": "WETH Market",
                    "inputToken": {"id": "0xweth", "symbol": "WETH", "name": "Wrapped Ether"},
                },
            ]
        }

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(return_value=markets_response)

        provider._client = mock_client
        provider._owns_client = False

        markets = await provider.list_markets()

        assert len(markets) == 2
        assert markets[0]["id"] == "0xmarket1"
        assert markets[1]["id"] == "0xmarket2"

    @pytest.mark.asyncio
    async def test_list_markets_unsupported_chain(self):
        """Test listing markets for unsupported chain returns empty."""
        provider = MorphoBlueAPYProvider()

        markets = await provider.list_markets(chain=Chain.ARBITRUM)

        assert markets == []

    @pytest.mark.asyncio
    async def test_list_markets_error_returns_empty(self):
        """Test that query error returns empty list."""
        provider = MorphoBlueAPYProvider()

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(side_effect=SubgraphQueryError("Query failed"))

        provider._client = mock_client
        provider._owns_client = False

        markets = await provider.list_markets()

        assert markets == []
