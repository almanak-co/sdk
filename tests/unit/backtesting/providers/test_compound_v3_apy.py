"""Unit tests for Compound V3 APY Provider.

This module tests the CompoundV3APYProvider class in providers/lending/compound_v3_apy.py,
covering:
- Provider initialization and configuration
- Supported chains and subgraph ID mapping
- APY fetching with mocked responses
- Date to day number conversion
- Fallback behavior when subgraph unavailable
- Error handling for query failures
- Market resolution (symbol to comet address)
"""

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.core.enums import Chain
from almanak.framework.backtesting.pnl.providers.lending.compound_v3_apy import (
    COMPOUND_V3_SUBGRAPH_IDS,
    DATA_SOURCE,
    DEFAULT_BORROW_APY_FALLBACK,
    DEFAULT_SUPPLY_APY_FALLBACK,
    KNOWN_COMET_ADDRESSES,
    SUPPORTED_CHAINS,
    CompoundV3APYProvider,
    CompoundV3ClientConfig,
)
from almanak.framework.backtesting.pnl.providers.subgraph_client import (
    SubgraphClient,
    SubgraphQueryError,
    SubgraphRateLimitError,
)
from almanak.framework.backtesting.pnl.types import DataConfidence


class TestCompoundV3APYProviderInitialization:
    """Tests for CompoundV3APYProvider initialization."""

    def test_init_default(self):
        """Test provider initializes with default settings."""
        provider = CompoundV3APYProvider()
        assert provider.supported_chains == SUPPORTED_CHAINS
        assert provider.config.chain == Chain.ETHEREUM
        assert provider.config.requests_per_minute == 100
        assert provider._owns_client is True

    def test_init_with_custom_config(self):
        """Test provider initializes with custom config."""
        config = CompoundV3ClientConfig(
            chain=Chain.ARBITRUM,
            requests_per_minute=50,
            supply_apy_fallback=Decimal("0.05"),
            borrow_apy_fallback=Decimal("0.08"),
            use_net_rates=True,
        )
        provider = CompoundV3APYProvider(config=config)
        assert provider.config.chain == Chain.ARBITRUM
        assert provider.config.requests_per_minute == 50
        assert provider.config.supply_apy_fallback == Decimal("0.05")
        assert provider.config.borrow_apy_fallback == Decimal("0.08")
        assert provider.config.use_net_rates is True

    def test_init_with_provided_client(self):
        """Test provider uses provided SubgraphClient."""
        mock_client = MagicMock(spec=SubgraphClient)
        provider = CompoundV3APYProvider(client=mock_client)
        assert provider._client is mock_client
        assert provider._owns_client is False

    def test_supported_chains_property_returns_copy(self):
        """Test supported_chains returns a copy, not the original."""
        provider = CompoundV3APYProvider()
        chains1 = provider.supported_chains
        chains2 = provider.supported_chains
        assert chains1 == chains2
        assert chains1 is not chains2


class TestSupportedChains:
    """Tests for supported chains configuration."""

    def test_supported_chains_include_required_networks(self):
        """Test that all required networks are supported (US-018)."""
        # US-018 requires: Ethereum, Arbitrum, Polygon, Base
        assert Chain.ETHEREUM in SUPPORTED_CHAINS
        assert Chain.ARBITRUM in SUPPORTED_CHAINS
        assert Chain.POLYGON in SUPPORTED_CHAINS
        assert Chain.BASE in SUPPORTED_CHAINS

    def test_all_supported_chains_have_subgraph_ids(self):
        """Test all supported chains have subgraph IDs."""
        for chain in SUPPORTED_CHAINS:
            assert chain in COMPOUND_V3_SUBGRAPH_IDS
            assert COMPOUND_V3_SUBGRAPH_IDS[chain]  # Non-empty

    def test_subgraph_ids_are_valid_format(self):
        """Test subgraph IDs have valid format (base58-like)."""
        for chain, subgraph_id in COMPOUND_V3_SUBGRAPH_IDS.items():
            # Subgraph IDs are base58-like strings
            assert len(subgraph_id) >= 40
            assert all(c.isalnum() for c in subgraph_id)


class TestDateConversion:
    """Tests for date to day number conversion."""

    def test_date_to_day_number_from_date(self):
        """Test conversion from date object."""
        provider = CompoundV3APYProvider()

        # January 1, 2024 = day 19723 (19723 days since epoch)
        day_num = provider._date_to_day_number(date(2024, 1, 1))
        expected = (date(2024, 1, 1) - date(1970, 1, 1)).days
        assert day_num == expected
        assert day_num == 19723

    def test_date_to_day_number_from_datetime(self):
        """Test conversion from datetime object."""
        provider = CompoundV3APYProvider()

        dt = datetime(2024, 1, 1, 12, 30, 0, tzinfo=UTC)
        day_num = provider._date_to_day_number(dt)
        expected = (date(2024, 1, 1) - date(1970, 1, 1)).days
        assert day_num == expected

    def test_date_to_day_number_epoch(self):
        """Test that epoch returns 0."""
        provider = CompoundV3APYProvider()
        day_num = provider._date_to_day_number(date(1970, 1, 1))
        assert day_num == 0


class TestDecimalParsing:
    """Tests for decimal parsing."""

    def test_parse_decimal_string(self):
        """Test parsing decimal from string."""
        provider = CompoundV3APYProvider()
        assert provider._parse_decimal("0.03") == Decimal("0.03")
        assert provider._parse_decimal("0.055") == Decimal("0.055")

    def test_parse_decimal_float(self):
        """Test parsing decimal from float."""
        provider = CompoundV3APYProvider()
        result = provider._parse_decimal(0.03)
        assert result == Decimal("0.03")

    def test_parse_decimal_zero(self):
        """Test parsing zero value."""
        provider = CompoundV3APYProvider()
        assert provider._parse_decimal("0") == Decimal("0")
        assert provider._parse_decimal(0) == Decimal("0")

    def test_parse_decimal_none_returns_zero(self):
        """Test parsing None returns zero."""
        provider = CompoundV3APYProvider()
        assert provider._parse_decimal(None) == Decimal("0")

    def test_parse_decimal_invalid_returns_zero(self):
        """Test parsing invalid value returns zero."""
        provider = CompoundV3APYProvider()
        assert provider._parse_decimal("invalid") == Decimal("0")


class TestMarketSymbolNormalization:
    """Tests for market symbol normalization."""

    def test_normalize_uppercase(self):
        """Test symbols are converted to uppercase."""
        provider = CompoundV3APYProvider()
        assert provider._normalize_market_symbol("usdc") == "USDC"
        assert provider._normalize_market_symbol("USDC") == "USDC"
        assert provider._normalize_market_symbol("Usdc") == "USDC"

    def test_normalize_strips_whitespace(self):
        """Test whitespace is stripped."""
        provider = CompoundV3APYProvider()
        assert provider._normalize_market_symbol("  USDC  ") == "USDC"

    def test_normalize_eth_to_weth(self):
        """Test ETH is converted to WETH."""
        provider = CompoundV3APYProvider()
        assert provider._normalize_market_symbol("ETH") == "WETH"
        assert provider._normalize_market_symbol("eth") == "WETH"


class TestCometAddressResolution:
    """Tests for comet address resolution."""

    def test_get_comet_address_known_markets(self):
        """Test getting comet address for known markets."""
        provider = CompoundV3APYProvider()

        # Ethereum USDC
        address = provider._get_comet_address(Chain.ETHEREUM, "USDC")
        assert address == KNOWN_COMET_ADDRESSES[Chain.ETHEREUM]["USDC"].lower()

        # Arbitrum USDC
        address = provider._get_comet_address(Chain.ARBITRUM, "USDC")
        assert address == KNOWN_COMET_ADDRESSES[Chain.ARBITRUM]["USDC"].lower()

    def test_get_comet_address_unknown_returns_none(self):
        """Test getting comet address for unknown market returns None."""
        provider = CompoundV3APYProvider()
        assert provider._get_comet_address(Chain.ETHEREUM, "NONEXISTENT") is None

    def test_resolve_market_id_from_symbol(self):
        """Test resolving market ID from symbol."""
        provider = CompoundV3APYProvider()
        market_id = provider._resolve_market_id(Chain.ETHEREUM, "USDC")
        assert market_id == KNOWN_COMET_ADDRESSES[Chain.ETHEREUM]["USDC"].lower()

    def test_resolve_market_id_from_address(self):
        """Test resolving market ID from address."""
        provider = CompoundV3APYProvider()
        address = "0xc3d688B66703497DAA19211EEdff47f25384cdc3"
        market_id = provider._resolve_market_id(Chain.ETHEREUM, address)
        assert market_id == address.lower()


class TestGetAPY:
    """Tests for get_apy method."""

    @pytest.mark.asyncio
    async def test_get_apy_success(self):
        """Test successfully fetching APY data."""
        provider = CompoundV3APYProvider()

        # Mock subgraph response with 3% supply, 5% borrow
        accounting_response = {
            "dailyMarketAccountings": [
                {
                    "id": "item1",
                    "day": "19723",  # 2024-01-01
                    "timestamp": "1704067200",  # 2024-01-01 00:00:00 UTC
                    "accounting": {
                        "supplyApr": "0.03",
                        "borrowApr": "0.05",
                        "rewardSupplyApr": "0.01",
                        "rewardBorrowApr": "0.005",
                        "netSupplyApr": "0.04",
                        "netBorrowApr": "0.045",
                        "utilization": "0.8",
                    },
                },
                {
                    "id": "item2",
                    "day": "19724",  # 2024-01-02
                    "timestamp": "1704153600",  # 2024-01-02 00:00:00 UTC
                    "accounting": {
                        "supplyApr": "0.032",
                        "borrowApr": "0.052",
                        "rewardSupplyApr": "0.01",
                        "rewardBorrowApr": "0.005",
                        "netSupplyApr": "0.042",
                        "netBorrowApr": "0.047",
                        "utilization": "0.82",
                    },
                },
            ]
        }

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(return_value=accounting_response)

        provider._client = mock_client
        provider._owns_client = False

        apys = await provider.get_apy(
            protocol="compound_v3",
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
    async def test_get_apy_uses_net_rates_when_configured(self):
        """Test that net rates are used when use_net_rates=True."""
        config = CompoundV3ClientConfig(use_net_rates=True)
        provider = CompoundV3APYProvider(config=config)

        accounting_response = {
            "dailyMarketAccountings": [
                {
                    "day": "19723",
                    "timestamp": "1704067200",
                    "accounting": {
                        "supplyApr": "0.03",
                        "borrowApr": "0.05",
                        "netSupplyApr": "0.04",
                        "netBorrowApr": "0.045",
                    },
                },
            ]
        }

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(return_value=accounting_response)

        provider._client = mock_client
        provider._owns_client = False

        apys = await provider.get_apy(
            protocol="compound_v3",
            market="USDC",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 1, tzinfo=UTC),
        )

        assert len(apys) == 1
        # Should use net rates
        assert apys[0].supply_apy == Decimal("0.04")
        assert apys[0].borrow_apy == Decimal("0.045")

    @pytest.mark.asyncio
    async def test_get_apy_market_not_found(self):
        """Test behavior when market not found."""
        provider = CompoundV3APYProvider()

        apys = await provider.get_apy(
            protocol="compound_v3",
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
        provider = CompoundV3APYProvider()

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(return_value={"dailyMarketAccountings": []})

        provider._client = mock_client
        provider._owns_client = False

        apys = await provider.get_apy(
            protocol="compound_v3",
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
        provider = CompoundV3APYProvider()

        accounting_response = {
            "dailyMarketAccountings": [
                {
                    "day": "19723",
                    "timestamp": "1704067200",
                    "accounting": {
                        "supplyApr": "0.03",
                        "borrowApr": "0.05",
                    },
                }
            ]
        }

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(return_value=accounting_response)

        provider._client = mock_client
        provider._owns_client = False

        # Pass naive datetimes (no timezone)
        apys = await provider.get_apy(
            protocol="compound_v3",
            market="USDC",
            start_date=datetime(2024, 1, 1),  # No timezone
            end_date=datetime(2024, 1, 2),  # No timezone
        )

        # Should work without error
        assert len(apys) >= 1

    @pytest.mark.asyncio
    async def test_get_apy_with_address_directly(self):
        """Test get_apy with comet address instead of symbol."""
        provider = CompoundV3APYProvider()

        accounting_response = {
            "dailyMarketAccountings": [
                {
                    "day": "19723",
                    "timestamp": "1704067200",
                    "accounting": {
                        "supplyApr": "0.03",
                        "borrowApr": "0.05",
                    },
                }
            ]
        }

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(return_value=accounting_response)

        provider._client = mock_client
        provider._owns_client = False

        # Use address directly
        apys = await provider.get_apy(
            protocol="compound_v3",
            market="0xc3d688B66703497DAA19211EEdff47f25384cdc3",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 1, tzinfo=UTC),
        )

        assert len(apys) >= 1
        # Verify query was called with lowercase address
        call_args = mock_client.query.call_args
        assert "0xc3d688b66703497daa19211eedff47f25384cdc3" in str(call_args)


class TestErrorHandling:
    """Tests for error handling in APY fetching."""

    @pytest.mark.asyncio
    async def test_rate_limit_error_returns_fallback(self):
        """Test that rate limit error returns fallback results."""
        config = CompoundV3ClientConfig(
            supply_apy_fallback=Decimal("0.04"),
            borrow_apy_fallback=Decimal("0.07"),
        )
        provider = CompoundV3APYProvider(config=config)

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(side_effect=SubgraphRateLimitError("Rate limit exceeded"))

        provider._client = mock_client
        provider._owns_client = False

        apys = await provider.get_apy(
            protocol="compound_v3",
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
        provider = CompoundV3APYProvider()

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(side_effect=SubgraphQueryError("Query failed"))

        provider._client = mock_client
        provider._owns_client = False

        apys = await provider.get_apy(
            protocol="compound_v3",
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
        provider = CompoundV3APYProvider()

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(side_effect=Exception("Unexpected error"))

        provider._client = mock_client
        provider._owns_client = False

        apys = await provider.get_apy(
            protocol="compound_v3",
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
        config = CompoundV3ClientConfig(chain=Chain.ETHEREUM)
        provider = CompoundV3APYProvider(config=config)

        accounting_response = {
            "dailyMarketAccountings": [
                {
                    "day": "19723",
                    "timestamp": "1704067200",
                    "accounting": {
                        "supplyApr": "0.03",
                        "borrowApr": "0.05",
                    },
                }
            ]
        }

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(return_value=accounting_response)

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
        assert subgraph_id == COMPOUND_V3_SUBGRAPH_IDS[Chain.ARBITRUM]

    @pytest.mark.asyncio
    async def test_get_apy_for_chain_restores_original_chain(self):
        """Test that original chain is restored after query."""
        config = CompoundV3ClientConfig(chain=Chain.ETHEREUM)
        provider = CompoundV3APYProvider(config=config)

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(return_value={"dailyMarketAccountings": []})

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
        provider = CompoundV3APYProvider()

        accounting_response = {
            "dailyMarketAccountings": [
                {
                    "day": "19723",  # Earlier
                    "timestamp": "1704067200",
                    "accounting": {
                        "supplyApr": "0.03",
                        "borrowApr": "0.05",
                    },
                },
                {
                    "day": "19724",  # Later
                    "timestamp": "1704153600",
                    "accounting": {
                        "supplyApr": "0.035",  # 3.5%
                        "borrowApr": "0.055",  # 5.5%
                    },
                },
            ]
        }

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(return_value=accounting_response)

        provider._client = mock_client
        provider._owns_client = False

        apy = await provider.get_current_apy("USDC")

        # Should return the latest (second) result
        assert apy.supply_apy == Decimal("0.035")
        assert apy.borrow_apy == Decimal("0.055")

    @pytest.mark.asyncio
    async def test_get_current_apy_no_data_returns_fallback(self):
        """Test that get_current_apy returns fallback when no data."""
        provider = CompoundV3APYProvider()

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(return_value={"dailyMarketAccountings": []})

        provider._client = mock_client
        provider._owns_client = False

        apy = await provider.get_current_apy("USDC")

        assert apy.source_info.confidence == DataConfidence.LOW


class TestContextManager:
    """Tests for async context manager behavior."""

    @pytest.mark.asyncio
    async def test_context_manager_closes_client(self):
        """Test that context manager closes client on exit."""
        provider = CompoundV3APYProvider()

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

        provider = CompoundV3APYProvider(client=mock_client)
        assert provider._owns_client is False

        async with provider:
            pass

        # Should NOT close external client
        mock_client.close.assert_not_called()


class TestUnsupportedChain:
    """Tests for unsupported chain handling."""

    @pytest.mark.asyncio
    async def test_unsupported_chain_returns_fallback(self):
        """Test that unsupported chain returns fallback results."""
        # Use a chain not in SUPPORTED_CHAINS
        config = CompoundV3ClientConfig(chain=Chain.AVALANCHE)
        provider = CompoundV3APYProvider(config=config)

        apys = await provider.get_apy(
            protocol="compound_v3",
            market="USDC",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 2, tzinfo=UTC),
        )

        assert len(apys) >= 1
        for apy in apys:
            assert apy.source_info.confidence == DataConfidence.LOW

    def test_get_subgraph_id_unsupported_returns_none(self):
        """Test that _get_subgraph_id returns None for unsupported chain."""
        provider = CompoundV3APYProvider()
        # AVALANCHE is not in COMPOUND_V3_SUBGRAPH_IDS
        assert provider._get_subgraph_id(Chain.AVALANCHE) is None


class TestFallbackResultGeneration:
    """Tests for fallback result generation."""

    def test_generate_fallback_results_daily(self):
        """Test fallback results are generated for each day."""
        config = CompoundV3ClientConfig(
            supply_apy_fallback=Decimal("0.025"),
            borrow_apy_fallback=Decimal("0.045"),
        )
        provider = CompoundV3APYProvider(config=config)

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
        provider = CompoundV3APYProvider()
        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)

        result = provider._create_fallback_result(timestamp)

        assert result.supply_apy == DEFAULT_SUPPLY_APY_FALLBACK
        assert result.borrow_apy == DEFAULT_BORROW_APY_FALLBACK
        assert result.source_info.timestamp == timestamp
        assert result.source_info.confidence == DataConfidence.LOW


class TestAPYDataParsing:
    """Tests for APY data parsing."""

    def test_parse_apy_data_base_rates(self):
        """Test parsing APY data from subgraph response (base rates)."""
        provider = CompoundV3APYProvider()

        daily_accounting = {
            "day": "19723",
            "timestamp": "1704067200",  # 2024-01-01 00:00:00 UTC
            "accounting": {
                "supplyApr": "0.03",  # 3%
                "borrowApr": "0.05",  # 5%
                "netSupplyApr": "0.04",
                "netBorrowApr": "0.045",
            },
        }

        result = provider._parse_apy_data(daily_accounting)

        assert result.supply_apy == Decimal("0.03")
        assert result.borrow_apy == Decimal("0.05")
        assert result.source_info.source == DATA_SOURCE
        assert result.source_info.confidence == DataConfidence.HIGH
        assert result.source_info.timestamp == datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

    def test_parse_apy_data_net_rates(self):
        """Test parsing APY data with net rates enabled."""
        config = CompoundV3ClientConfig(use_net_rates=True)
        provider = CompoundV3APYProvider(config=config)

        daily_accounting = {
            "day": "19723",
            "timestamp": "1704067200",
            "accounting": {
                "supplyApr": "0.03",
                "borrowApr": "0.05",
                "netSupplyApr": "0.04",
                "netBorrowApr": "0.045",
            },
        }

        result = provider._parse_apy_data(daily_accounting)

        # Should use net rates
        assert result.supply_apy == Decimal("0.04")
        assert result.borrow_apy == Decimal("0.045")

    def test_parse_apy_data_missing_accounting(self):
        """Test parsing with missing accounting object returns zeros."""
        provider = CompoundV3APYProvider()

        daily_accounting = {
            "day": "19723",
            "timestamp": "1704067200",
            # Missing "accounting" key
        }

        result = provider._parse_apy_data(daily_accounting)

        assert result.supply_apy == Decimal("0")
        assert result.borrow_apy == Decimal("0")


class TestListMarkets:
    """Tests for list_markets method."""

    @pytest.mark.asyncio
    async def test_list_markets_success(self):
        """Test successfully listing markets."""
        provider = CompoundV3APYProvider()

        markets_response = {
            "markets": [
                {
                    "id": "0xc3d688b66703497daa19211eedff47f25384cdc3",
                    "cometProxy": "0xc3d688B66703497DAA19211EEdff47f25384cdc3",
                    "protocol": {"id": "ethereum"},
                },
                {
                    "id": "0xa17581a9e3356d9a858b789d68b4d866e593ae94",
                    "cometProxy": "0xA17581A9E3356d9A858b789D68B4d866e593aE94",
                    "protocol": {"id": "ethereum"},
                },
            ]
        }

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(return_value=markets_response)

        provider._client = mock_client
        provider._owns_client = False

        markets = await provider.list_markets()

        assert len(markets) == 2
        assert markets[0]["cometProxy"] == "0xc3d688B66703497DAA19211EEdff47f25384cdc3"

    @pytest.mark.asyncio
    async def test_list_markets_error_returns_empty(self):
        """Test that list_markets returns empty list on error."""
        provider = CompoundV3APYProvider()

        mock_client = MagicMock(spec=SubgraphClient)
        mock_client.query = AsyncMock(side_effect=SubgraphQueryError("Query failed"))

        provider._client = mock_client
        provider._owns_client = False

        markets = await provider.list_markets()

        assert markets == []

    @pytest.mark.asyncio
    async def test_list_markets_unsupported_chain_returns_empty(self):
        """Test that list_markets returns empty list for unsupported chain."""
        provider = CompoundV3APYProvider()

        markets = await provider.list_markets(chain=Chain.AVALANCHE)

        assert markets == []


class TestKnownCometAddresses:
    """Tests for known comet addresses configuration."""

    def test_ethereum_markets_exist(self):
        """Test Ethereum has known markets."""
        assert Chain.ETHEREUM in KNOWN_COMET_ADDRESSES
        assert "USDC" in KNOWN_COMET_ADDRESSES[Chain.ETHEREUM]
        assert "WETH" in KNOWN_COMET_ADDRESSES[Chain.ETHEREUM]

    def test_arbitrum_markets_exist(self):
        """Test Arbitrum has known markets."""
        assert Chain.ARBITRUM in KNOWN_COMET_ADDRESSES
        assert "USDC" in KNOWN_COMET_ADDRESSES[Chain.ARBITRUM]

    def test_polygon_markets_exist(self):
        """Test Polygon has known markets."""
        assert Chain.POLYGON in KNOWN_COMET_ADDRESSES
        assert "USDC" in KNOWN_COMET_ADDRESSES[Chain.POLYGON]

    def test_base_markets_exist(self):
        """Test Base has known markets."""
        assert Chain.BASE in KNOWN_COMET_ADDRESSES
        assert "USDC" in KNOWN_COMET_ADDRESSES[Chain.BASE]

    def test_all_addresses_are_valid_format(self):
        """Test all addresses are valid Ethereum address format."""
        for chain, markets in KNOWN_COMET_ADDRESSES.items():
            for symbol, address in markets.items():
                assert address.startswith("0x"), f"Invalid address for {chain}:{symbol}"
                assert len(address) == 42, f"Wrong length for {chain}:{symbol}"
