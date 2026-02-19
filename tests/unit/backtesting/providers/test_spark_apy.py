"""Unit tests for Spark APY Provider.

This module tests the SparkAPYProvider class in providers/lending/spark_apy.py,
covering:
- Provider initialization and configuration
- Supported chains and subgraph ID mapping
- APY fetching with mocked responses
- Messari schema rate parsing (LENDER/BORROWER sides)
- Fallback behavior when subgraph unavailable
- Error handling for query failures
- Market ID resolution by symbol
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.core.enums import Chain
from almanak.framework.backtesting.pnl.providers.lending.spark_apy import (
    BORROWER_SIDE,
    DATA_SOURCE,
    DEFAULT_BORROW_APY_FALLBACK,
    DEFAULT_SUPPLY_APY_FALLBACK,
    LENDER_SIDE,
    SPARK_SUBGRAPH_IDS,
    SUPPORTED_CHAINS,
    SparkAPYProvider,
    SparkClientConfig,
)
from almanak.framework.backtesting.pnl.providers.subgraph_client import (
    SubgraphClient,
    SubgraphQueryError,
    SubgraphRateLimitError,
)
from almanak.framework.backtesting.pnl.types import DataConfidence


class TestSparkAPYProviderInitialization:
    """Tests for SparkAPYProvider initialization."""

    def test_init_default(self):
        """Test provider initializes with default settings."""
        provider = SparkAPYProvider()
        assert provider.supported_chains == SUPPORTED_CHAINS
        assert provider.config.chain == Chain.ETHEREUM
        assert provider.config.requests_per_minute == 100
        assert provider._owns_client is True

    def test_init_with_custom_config(self):
        """Test provider initializes with custom config."""
        config = SparkClientConfig(
            chain=Chain.ETHEREUM,
            requests_per_minute=50,
            supply_apy_fallback=Decimal("0.05"),
            borrow_apy_fallback=Decimal("0.08"),
        )
        provider = SparkAPYProvider(config=config)
        assert provider.config.chain == Chain.ETHEREUM
        assert provider.config.requests_per_minute == 50
        assert provider.config.supply_apy_fallback == Decimal("0.05")
        assert provider.config.borrow_apy_fallback == Decimal("0.08")

    def test_init_with_provided_client(self):
        """Test provider uses provided SubgraphClient."""
        mock_client = MagicMock(spec=SubgraphClient)
        provider = SparkAPYProvider(client=mock_client)
        assert provider._client is mock_client
        assert provider._owns_client is False

    def test_supported_chains_property_returns_copy(self):
        """Test supported_chains returns a copy, not the original."""
        provider = SparkAPYProvider()
        chains1 = provider.supported_chains
        chains2 = provider.supported_chains
        assert chains1 == chains2
        assert chains1 is not chains2


class TestSupportedChains:
    """Tests for supported chains configuration."""

    def test_supported_chains_include_ethereum(self):
        """Test that Ethereum is supported (US-020 requirement)."""
        assert Chain.ETHEREUM in SUPPORTED_CHAINS

    def test_all_supported_chains_have_subgraph_ids(self):
        """Test all supported chains have subgraph IDs."""
        for chain in SUPPORTED_CHAINS:
            assert chain in SPARK_SUBGRAPH_IDS
            assert SPARK_SUBGRAPH_IDS[chain]  # Non-empty

    def test_subgraph_ids_are_valid_format(self):
        """Test subgraph IDs have valid format (base58-like)."""
        for chain, subgraph_id in SPARK_SUBGRAPH_IDS.items():
            # Subgraph IDs are base58-like strings
            assert len(subgraph_id) >= 40
            assert all(c.isalnum() for c in subgraph_id)


class TestPercentageConversion:
    """Tests for percentage to decimal conversion."""

    def test_parse_decimal_basic(self):
        """Test basic percentage to decimal conversion."""
        provider = SparkAPYProvider()

        # 3% APY = 3.0 in percentage format
        result = provider._parse_decimal("3.0")
        assert result == Decimal("0.03")

    def test_parse_decimal_high_precision(self):
        """Test conversion with high precision values."""
        provider = SparkAPYProvider()

        # 5.5% APY
        result = provider._parse_decimal("5.5")
        assert result == Decimal("0.055")

    def test_parse_decimal_zero(self):
        """Test conversion with zero value."""
        provider = SparkAPYProvider()
        result = provider._parse_decimal("0")
        assert result == Decimal("0")

    def test_parse_decimal_none_returns_zero(self):
        """Test conversion with None returns zero."""
        provider = SparkAPYProvider()
        assert provider._parse_decimal(None) == Decimal("0")

    def test_parse_decimal_invalid_returns_zero(self):
        """Test conversion with invalid value returns zero."""
        provider = SparkAPYProvider()
        assert provider._parse_decimal("invalid") == Decimal("0")


class TestMarketSymbolNormalization:
    """Tests for market symbol normalization."""

    def test_normalize_uppercase(self):
        """Test symbols are converted to uppercase."""
        provider = SparkAPYProvider()
        assert provider._normalize_market_symbol("dai") == "DAI"
        assert provider._normalize_market_symbol("DAI") == "DAI"
        assert provider._normalize_market_symbol("Dai") == "DAI"

    def test_normalize_eth_to_weth(self):
        """Test ETH is normalized to WETH."""
        provider = SparkAPYProvider()
        assert provider._normalize_market_symbol("eth") == "WETH"
        assert provider._normalize_market_symbol("ETH") == "WETH"

    def test_normalize_strips_whitespace(self):
        """Test whitespace is stripped."""
        provider = SparkAPYProvider()
        assert provider._normalize_market_symbol("  dai  ") == "DAI"


class TestRateExtraction:
    """Tests for extracting rates from Messari schema snapshots."""

    def test_extract_rates_lender_and_borrower(self):
        """Test extracting both supply and borrow rates."""
        provider = SparkAPYProvider()

        rates = [
            {"id": "1", "rate": "3.5", "side": LENDER_SIDE, "type": "VARIABLE"},
            {"id": "2", "rate": "5.5", "side": BORROWER_SIDE, "type": "VARIABLE"},
        ]

        supply, borrow = provider._extract_rates_from_snapshot(rates)
        assert supply == Decimal("0.035")
        assert borrow == Decimal("0.055")

    def test_extract_rates_only_lender(self):
        """Test extracting when only lender rate exists."""
        provider = SparkAPYProvider()

        rates = [
            {"id": "1", "rate": "3.5", "side": LENDER_SIDE, "type": "VARIABLE"},
        ]

        supply, borrow = provider._extract_rates_from_snapshot(rates)
        assert supply == Decimal("0.035")
        assert borrow == Decimal("0")

    def test_extract_rates_only_borrower(self):
        """Test extracting when only borrower rate exists."""
        provider = SparkAPYProvider()

        rates = [
            {"id": "2", "rate": "5.5", "side": BORROWER_SIDE, "type": "VARIABLE"},
        ]

        supply, borrow = provider._extract_rates_from_snapshot(rates)
        assert supply == Decimal("0")
        assert borrow == Decimal("0.055")

    def test_extract_rates_empty_list(self):
        """Test extracting from empty rates list."""
        provider = SparkAPYProvider()

        supply, borrow = provider._extract_rates_from_snapshot([])
        assert supply == Decimal("0")
        assert borrow == Decimal("0")


class TestDayNumberConversion:
    """Tests for date to day number conversion."""

    def test_date_to_day_number_epoch(self):
        """Test day number for Unix epoch (should be 0)."""
        provider = SparkAPYProvider()
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        assert provider._date_to_day_number(epoch) == 0

    def test_date_to_day_number_known_date(self):
        """Test day number for a known date."""
        provider = SparkAPYProvider()
        # January 1, 2024 is day 19723 (not 19724, off-by-one is common error)
        jan_1_2024 = datetime(2024, 1, 1, tzinfo=UTC)
        assert provider._date_to_day_number(jan_1_2024) == 19723


class TestAPYFetching:
    """Tests for APY data fetching."""

    @pytest.fixture
    def mock_client(self) -> MagicMock:
        """Create a mock SubgraphClient."""
        client = MagicMock(spec=SubgraphClient)
        client.query = AsyncMock()
        client.close = AsyncMock()
        return client

    @pytest.mark.asyncio
    async def test_get_apy_success(self, mock_client: MagicMock):
        """Test successful APY fetch."""
        # Mock market lookup
        mock_client.query.side_effect = [
            # First call: market lookup
            {
                "markets": [
                    {
                        "id": "0xmarket123",
                        "name": "DAI Market",
                        "inputToken": {"id": "0xdai", "symbol": "DAI", "name": "Dai"},
                    }
                ]
            },
            # Second call: daily snapshots
            {
                "marketDailySnapshots": [
                    {
                        "id": "snap1",
                        "days": 19723,
                        "timestamp": 1704067200,  # Jan 1, 2024
                        "rates": [
                            {"id": "r1", "rate": "3.5", "side": LENDER_SIDE, "type": "VARIABLE"},
                            {"id": "r2", "rate": "5.5", "side": BORROWER_SIDE, "type": "VARIABLE"},
                        ],
                    },
                    {
                        "id": "snap2",
                        "days": 19724,
                        "timestamp": 1704153600,  # Jan 2, 2024
                        "rates": [
                            {"id": "r3", "rate": "3.6", "side": LENDER_SIDE, "type": "VARIABLE"},
                            {"id": "r4", "rate": "5.6", "side": BORROWER_SIDE, "type": "VARIABLE"},
                        ],
                    },
                ]
            },
        ]

        provider = SparkAPYProvider(client=mock_client)
        results = await provider.get_apy(
            protocol="spark",
            market="DAI",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 2, tzinfo=UTC),
        )

        assert len(results) == 2
        assert results[0].supply_apy == Decimal("0.035")
        assert results[0].borrow_apy == Decimal("0.055")
        assert results[0].source_info.confidence == DataConfidence.HIGH
        assert results[0].source_info.source == DATA_SOURCE

        assert results[1].supply_apy == Decimal("0.036")
        assert results[1].borrow_apy == Decimal("0.056")

    @pytest.mark.asyncio
    async def test_get_apy_unsupported_chain_returns_fallback(self, mock_client: MagicMock):
        """Test fallback when chain is not supported."""
        config = SparkClientConfig(chain=Chain.ARBITRUM)  # Not supported
        provider = SparkAPYProvider(config=config, client=mock_client)

        results = await provider.get_apy(
            protocol="spark",
            market="DAI",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 2, tzinfo=UTC),
        )

        # Should return fallback results
        assert len(results) == 2
        assert all(r.source_info.confidence == DataConfidence.LOW for r in results)
        assert all(r.source_info.source == "fallback" for r in results)

    @pytest.mark.asyncio
    async def test_get_apy_market_not_found_returns_fallback(self, mock_client: MagicMock):
        """Test fallback when market is not found."""
        mock_client.query.return_value = {"markets": []}  # Empty response

        provider = SparkAPYProvider(client=mock_client)
        results = await provider.get_apy(
            protocol="spark",
            market="UNKNOWN",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 2, tzinfo=UTC),
        )

        # Should return fallback results
        assert len(results) == 2
        assert all(r.source_info.confidence == DataConfidence.LOW for r in results)

    @pytest.mark.asyncio
    async def test_get_apy_no_data_returns_fallback(self, mock_client: MagicMock):
        """Test fallback when no data points exist."""
        mock_client.query.side_effect = [
            {"markets": [{"id": "0xmarket123", "name": "DAI", "inputToken": {"symbol": "DAI"}}]},
            {"marketDailySnapshots": []},  # No snapshots
        ]

        provider = SparkAPYProvider(client=mock_client)
        results = await provider.get_apy(
            protocol="spark",
            market="DAI",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 2, tzinfo=UTC),
        )

        assert len(results) == 2
        assert all(r.source_info.confidence == DataConfidence.LOW for r in results)

    @pytest.mark.asyncio
    async def test_get_apy_rate_limit_error_returns_fallback(self, mock_client: MagicMock):
        """Test fallback on rate limit error."""
        mock_client.query.side_effect = [
            {"markets": [{"id": "0xmarket123", "name": "DAI", "inputToken": {"symbol": "DAI"}}]},
            SubgraphRateLimitError("Rate limited"),
        ]

        provider = SparkAPYProvider(client=mock_client)
        results = await provider.get_apy(
            protocol="spark",
            market="DAI",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 2, tzinfo=UTC),
        )

        assert len(results) == 2
        assert all(r.source_info.confidence == DataConfidence.LOW for r in results)

    @pytest.mark.asyncio
    async def test_get_apy_query_error_returns_fallback(self, mock_client: MagicMock):
        """Test fallback on query error."""
        mock_client.query.side_effect = [
            {"markets": [{"id": "0xmarket123", "name": "DAI", "inputToken": {"symbol": "DAI"}}]},
            SubgraphQueryError("Query failed"),
        ]

        provider = SparkAPYProvider(client=mock_client)
        results = await provider.get_apy(
            protocol="spark",
            market="DAI",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 2, tzinfo=UTC),
        )

        assert len(results) == 2
        assert all(r.source_info.confidence == DataConfidence.LOW for r in results)


class TestMarketResolution:
    """Tests for market ID resolution."""

    @pytest.fixture
    def mock_client(self) -> MagicMock:
        """Create a mock SubgraphClient."""
        client = MagicMock(spec=SubgraphClient)
        client.query = AsyncMock()
        client.close = AsyncMock()
        return client

    @pytest.mark.asyncio
    async def test_resolve_market_by_symbol(self, mock_client: MagicMock):
        """Test market resolution by symbol."""
        mock_client.query.return_value = {
            "markets": [
                {
                    "id": "0xmarket123",
                    "name": "DAI Market",
                    "inputToken": {"id": "0xdai", "symbol": "DAI", "name": "Dai"},
                }
            ]
        }

        provider = SparkAPYProvider(client=mock_client)
        market_id = await provider._resolve_market_id(Chain.ETHEREUM, "DAI")

        assert market_id == "0xmarket123"
        # Verify it's cached
        assert "ETHEREUM:DAI" in provider._market_cache

    @pytest.mark.asyncio
    async def test_resolve_market_by_id(self, mock_client: MagicMock):
        """Test market resolution by ID (passthrough)."""
        provider = SparkAPYProvider(client=mock_client)

        # Long hex string should pass through
        market_id = await provider._resolve_market_id(
            Chain.ETHEREUM, "0x1234567890abcdef1234567890abcdef12345678"
        )

        assert market_id == "0x1234567890abcdef1234567890abcdef12345678"
        # No query should be made
        mock_client.query.assert_not_called()

    @pytest.mark.asyncio
    async def test_market_cache_used(self, mock_client: MagicMock):
        """Test that cached market IDs are used."""
        mock_client.query.return_value = {
            "markets": [{"id": "0xmarket123", "name": "DAI", "inputToken": {"symbol": "DAI"}}]
        }

        provider = SparkAPYProvider(client=mock_client)

        # First call - should query
        await provider._find_market_by_token(Chain.ETHEREUM, "DAI")
        assert mock_client.query.call_count == 1

        # Second call - should use cache
        await provider._find_market_by_token(Chain.ETHEREUM, "DAI")
        assert mock_client.query.call_count == 1  # Still 1


class TestContextManager:
    """Tests for async context manager."""

    @pytest.mark.asyncio
    async def test_context_manager_closes_owned_client(self):
        """Test context manager closes owned client."""
        provider = SparkAPYProvider()
        mock_close = AsyncMock()
        provider._client.close = mock_close

        async with provider:
            pass

        mock_close.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager_does_not_close_provided_client(self):
        """Test context manager doesn't close provided client."""
        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.close = AsyncMock()

        provider = SparkAPYProvider(client=mock_client)

        async with provider:
            pass

        mock_client.close.assert_not_called()


class TestConvenienceMethods:
    """Tests for convenience methods."""

    @pytest.fixture
    def mock_client(self) -> MagicMock:
        """Create a mock SubgraphClient."""
        client = MagicMock(spec=SubgraphClient)
        client.query = AsyncMock()
        client.close = AsyncMock()
        return client

    @pytest.mark.asyncio
    async def test_get_apy_for_chain(self, mock_client: MagicMock):
        """Test get_apy_for_chain method."""
        mock_client.query.side_effect = [
            {"markets": [{"id": "0xmarket123", "name": "DAI", "inputToken": {"symbol": "DAI"}}]},
            {
                "marketDailySnapshots": [
                    {
                        "id": "snap1",
                        "days": 19723,
                        "timestamp": 1704067200,
                        "rates": [
                            {"rate": "3.5", "side": LENDER_SIDE},
                            {"rate": "5.5", "side": BORROWER_SIDE},
                        ],
                    }
                ]
            },
        ]

        provider = SparkAPYProvider(client=mock_client)
        results = await provider.get_apy_for_chain(
            chain=Chain.ETHEREUM,
            market="DAI",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 1, tzinfo=UTC),
        )

        assert len(results) == 1
        assert results[0].supply_apy == Decimal("0.035")

    @pytest.mark.asyncio
    async def test_get_current_apy(self, mock_client: MagicMock):
        """Test get_current_apy method."""
        mock_client.query.side_effect = [
            {"markets": [{"id": "0xmarket123", "name": "DAI", "inputToken": {"symbol": "DAI"}}]},
            {
                "marketDailySnapshots": [
                    {
                        "id": "snap1",
                        "days": 19723,
                        "timestamp": 1704067200,
                        "rates": [
                            {"rate": "4.0", "side": LENDER_SIDE},
                            {"rate": "6.0", "side": BORROWER_SIDE},
                        ],
                    }
                ]
            },
        ]

        provider = SparkAPYProvider(client=mock_client)
        result = await provider.get_current_apy("DAI")

        assert result.supply_apy == Decimal("0.04")
        assert result.borrow_apy == Decimal("0.06")
        assert result.source_info.confidence == DataConfidence.HIGH

    @pytest.mark.asyncio
    async def test_list_markets(self, mock_client: MagicMock):
        """Test list_markets method."""
        mock_client.query.return_value = {
            "markets": [
                {"id": "0x1", "name": "DAI Market", "inputToken": {"symbol": "DAI"}},
                {"id": "0x2", "name": "USDC Market", "inputToken": {"symbol": "USDC"}},
            ]
        }

        provider = SparkAPYProvider(client=mock_client)
        markets = await provider.list_markets()

        assert len(markets) == 2
        assert markets[0]["inputToken"]["symbol"] == "DAI"


class TestFallbackBehavior:
    """Tests for fallback result generation."""

    def test_create_fallback_result(self):
        """Test fallback result creation."""
        provider = SparkAPYProvider()
        ts = datetime(2024, 1, 1, tzinfo=UTC)

        result = provider._create_fallback_result(ts)

        assert result.supply_apy == DEFAULT_SUPPLY_APY_FALLBACK
        assert result.borrow_apy == DEFAULT_BORROW_APY_FALLBACK
        assert result.source_info.confidence == DataConfidence.LOW
        assert result.source_info.source == "fallback"
        assert result.source_info.timestamp == ts

    def test_generate_fallback_results(self):
        """Test fallback results generation for date range."""
        provider = SparkAPYProvider()
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 3, tzinfo=UTC)

        results = provider._generate_fallback_results(start, end)

        assert len(results) == 3  # Jan 1, 2, 3
        assert all(r.source_info.confidence == DataConfidence.LOW for r in results)


class TestDataSourceConstants:
    """Tests for data source constants."""

    def test_data_source_value(self):
        """Test data source identifier."""
        assert DATA_SOURCE == "spark_subgraph"

    def test_default_fallback_values(self):
        """Test default fallback APY values."""
        assert DEFAULT_SUPPLY_APY_FALLBACK == Decimal("0.03")
        assert DEFAULT_BORROW_APY_FALLBACK == Decimal("0.05")

    def test_side_constants(self):
        """Test interest rate side constants."""
        assert LENDER_SIDE == "LENDER"
        assert BORROWER_SIDE == "BORROWER"
