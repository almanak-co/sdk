"""Unit tests for LiquidityDepthProvider.

Tests the historical liquidity depth provider functionality with mocked
subgraph responses. Covers V3-style pools, V2-style pools, Liquidity Book,
Balancer, and Curve protocols.
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.core.chains import ChainRegistry
from almanak.connectors._strategy_base.dex_volume_registry import DexVolumeRegistry
from almanak.framework.backtesting.exceptions import DataSourceUnavailableError
from almanak.framework.backtesting.pnl.providers.liquidity_depth import (
    DATA_SOURCE_FALLBACK,
    DEFAULT_TWAP_WINDOW_HOURS,
    LiquidityDepthProvider,
)
from almanak.framework.backtesting.pnl.providers.subgraph_client import (
    SubgraphClient,
    SubgraphClientConfig,
)
from almanak.framework.backtesting.pnl.types import DataConfidence

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_subgraph_client():
    """Real SubgraphClient with the network-facing pieces mocked out.

    Provider calls flow through the real ``query_with_pagination`` cursor
    loop (VIB-5089) while tests stub ``.query`` per page; ``.close`` is
    mocked so ownership assertions keep working.
    """
    client = SubgraphClient(config=SubgraphClientConfig(api_key="test-key"))
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
    """Test the declaration-derived per-protocol facts (VIB-4851 Phase D)."""

    def test_data_sources_derived(self, mock_subgraph_client):
        """Provenance strings derive as <protocol>_subgraph per declared DEX."""
        provider = LiquidityDepthProvider(client=mock_subgraph_client)
        assert provider._data_source_for("uniswap_v3") == "uniswap_v3_subgraph"
        assert provider._data_source_for("sushiswap_v3") == "sushiswap_v3_subgraph"
        assert provider._data_source_for("pancakeswap_v3") == "pancakeswap_v3_subgraph"
        assert provider._data_source_for("aerodrome") == "aerodrome_subgraph"
        assert provider._data_source_for("traderjoe_v2") == "traderjoe_v2_subgraph"
        assert provider._data_source_for("curve") == "curve_subgraph"
        assert provider._data_source_for("balancer") == "balancer_subgraph"
        assert provider._data_source_for("unknown") == DATA_SOURCE_FALLBACK
        assert DATA_SOURCE_FALLBACK == "liquidity_fallback"

    def test_amm_families_declared(self):
        """AMM families derive from the connector declarations."""
        assert DexVolumeRegistry.entry_for("uniswap_v3").amm_family == "v3_concentrated"
        assert DexVolumeRegistry.entry_for("sushiswap_v3").amm_family == "v3_concentrated"
        assert DexVolumeRegistry.entry_for("pancakeswap_v3").amm_family == "v3_concentrated"
        assert DexVolumeRegistry.entry_for("aerodrome").amm_family == "solidly_v2"
        assert DexVolumeRegistry.entry_for("traderjoe_v2").amm_family == "liquidity_book"
        assert DexVolumeRegistry.entry_for("balancer").amm_family == "weighted"
        assert DexVolumeRegistry.entry_for("curve").amm_family == "stableswap"

    def test_supported_chains(self, mock_subgraph_client):
        """Supported chains derive from the declared DEX chain union."""
        provider = LiquidityDepthProvider(client=mock_subgraph_client)
        chains = provider.supported_chains
        assert "ethereum" in chains
        assert "arbitrum" in chains
        assert "base" in chains
        assert "optimism" in chains
        assert "polygon" in chains
        assert "avalanche" in chains
        assert "bsc" in chains

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
        with patch("almanak.framework.backtesting.pnl.providers.liquidity_depth.SubgraphClient") as mock_class:
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
        """Test supported_chains property derives from declared DEX chains."""
        chains = provider.supported_chains
        declared = DexVolumeRegistry.all_supported_chains()
        assert chains == [name for name in ChainRegistry.names() if name in declared]
        # Verify it's a copy
        chains.append("sonic")
        assert "sonic" not in provider.supported_chains


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
        with patch("almanak.framework.backtesting.pnl.providers.liquidity_depth.SubgraphClient") as mock_class:
            mock_instance = MagicMock()
            mock_instance.close = AsyncMock()
            mock_class.return_value = mock_instance

            provider = LiquidityDepthProvider()
            async with provider:
                pass

            mock_instance.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager_does_not_close_provided_client(self, provider, mock_subgraph_client):
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
        protocol = provider._detect_protocol_from_chain("base")
        assert protocol == "aerodrome"

    def test_detect_avalanche_defaults_to_traderjoe(self, provider):
        """Test Avalanche chain defaults to TraderJoe V2."""
        protocol = provider._detect_protocol_from_chain("avalanche")
        assert protocol == "traderjoe_v2"

    def test_detect_ethereum_defaults_to_uniswap_v3(self, provider):
        """Test Ethereum defaults to Uniswap V3."""
        protocol = provider._detect_protocol_from_chain("ethereum")
        assert protocol == "uniswap_v3"

    def test_detect_arbitrum_defaults_to_uniswap_v3(self, provider):
        """Test Arbitrum defaults to Uniswap V3."""
        protocol = provider._detect_protocol_from_chain("arbitrum")
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
            chain="arbitrum",
            timestamp=timestamp,
            protocol="uniswap_v3",
        )

        assert result.depth == Decimal("5000000")
        assert result.source_info.confidence == DataConfidence.HIGH
        assert result.source_info.source == "uniswap_v3_subgraph"

    @pytest.mark.asyncio
    async def test_query_sushiswap_v3_liquidity(self, provider, mock_subgraph_client):
        """Sushiswap's declared deployment is Messari-standard."""
        mock_subgraph_client.query.return_value = {
            "liquidityPoolDailySnapshots": [
                {"id": "0xsushi-1", "timestamp": 1704067200, "totalValueLockedUSD": "42000000", "dailyVolumeUSD": "1"},
            ]
        }

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await provider.get_liquidity_depth(
            pool_address="0xsushi",
            chain="ethereum",
            timestamp=timestamp,
            protocol="sushiswap_v3",
        )

        assert result.depth == Decimal("42000000")
        assert result.source_info.confidence == DataConfidence.HIGH


    @pytest.mark.asyncio
    async def test_query_pancakeswap_v3_liquidity(self, provider, mock_subgraph_client):
        """Pancake's bsc deployment is Messari-standard (per-chain override)."""
        mock_subgraph_client.query.return_value = {
            "liquidityPoolDailySnapshots": [
                {"id": "0x789-1", "timestamp": 1704067200, "totalValueLockedUSD": "10000000", "dailyVolumeUSD": "1"},
            ]
        }

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await provider.get_liquidity_depth(
            pool_address="0x789",
            chain="bsc",
            timestamp=timestamp,
            protocol="pancakeswap_v3",
        )

        assert result.depth == Decimal("10000000")
        assert result.source_info.source == "pancakeswap_v3_subgraph"

    @pytest.mark.asyncio
    async def test_query_pancakeswap_v3_liquidity_classic_chain(self, provider, mock_subgraph_client):
        """Pancake's base deployment keeps the classic v3 schema (no override)."""
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
            chain="base",
            timestamp=timestamp,
            protocol="pancakeswap_v3",
        )

        assert result.depth == Decimal("10000000")
        assert result.source_info.source == "pancakeswap_v3_subgraph"


# =============================================================================
# Test V2 Liquidity Query
# =============================================================================


class TestV2LiquidityQuery:
    """Test V2/Solidly-style subgraph queries."""

    @pytest.mark.asyncio
    async def test_query_aerodrome_liquidity(self, provider, mock_subgraph_client):
        """Test querying Aerodrome liquidity."""
        # The aerodrome subgraph exposes the uniswap-v3 fork schema
        # (poolDayDatas/tvlUSD), not solidly pairDayDatas (ALM-2930).
        mock_subgraph_client.query.return_value = {
            "poolDayDatas": [
                {
                    "id": "0xaero-19723",
                    "date": 1704067200,
                    "tvlUSD": "8000000",
                    "liquidity": "1000000",
                },
            ]
        }

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = await provider.get_liquidity_depth(
            pool_address="0xaero",
            chain="base",
            timestamp=timestamp,
            protocol="aerodrome",
        )

        assert result.depth == Decimal("8000000")
        assert result.source_info.source == "aerodrome_subgraph"
        assert result.source_info.confidence == DataConfidence.HIGH


# =============================================================================
# Test Liquidity Book Query
# =============================================================================


class TestLiquidityBookQuery:
    """Test TraderJoe V2 Liquidity Book queries."""

    @pytest.mark.asyncio
    async def test_query_traderjoe_liquidity(self, provider, mock_subgraph_client):
        """Test querying TraderJoe V2 Liquidity Book liquidity."""
        # Query-type field is lowercase-p lbpairDayDatas on the declared
        # deployment (see tests/audit/test_subgraph_schema_parity.py).
        mock_subgraph_client.query.return_value = {
            "lbpairDayDatas": [
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
            chain="avalanche",
            timestamp=timestamp,
            protocol="traderjoe_v2",
        )

        assert result.depth == Decimal("3000000")
        assert result.source_info.source == "traderjoe_v2_subgraph"
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
            chain="ethereum",
            timestamp=timestamp,
            protocol="balancer",
        )

        assert result.depth == Decimal("15000000")
        assert result.source_info.source == "balancer_subgraph"
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
            chain="ethereum",
            timestamp=timestamp,
            protocol="curve",
        )

        assert result.depth == Decimal("100000000")
        assert result.source_info.source == "curve_subgraph"
        assert result.source_info.confidence == DataConfidence.HIGH


# =============================================================================
# Test TWAP Calculation
# =============================================================================


class TestTWAPCalculation:
    """Test time-weighted average depth calculation."""

    @pytest.mark.asyncio
    async def test_twap_with_multiple_data_points(self, provider_with_twap, mock_subgraph_client):
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
            chain="arbitrum",
            timestamp=timestamp,
            protocol="uniswap_v3",
        )

        # TWAP should be between the two values (time-weighted average)
        assert result.depth >= Decimal("4000000")
        assert result.depth <= Decimal("6000000")
        assert result.source_info.confidence == DataConfidence.HIGH

    @pytest.mark.asyncio
    async def test_twap_with_single_data_point(self, provider_with_twap, mock_subgraph_client):
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
            chain="arbitrum",
            timestamp=timestamp,
            protocol="uniswap_v3",
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
            chain="arbitrum",
            timestamp=timestamp,
            protocol="uniswap_v3",
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
            chain="sonic",  # Not supported
            timestamp=timestamp,
            protocol="uniswap_v3",
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
            chain="arbitrum",
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
            chain="arbitrum",
            timestamp=timestamp,
            protocol="uniswap_v3",
        )

        assert result.depth == Decimal("1000000")
        assert result.source_info.confidence == DataConfidence.LOW


# =============================================================================
# Test Route Dispatch
# =============================================================================


class TestLiquidityDepthRouting:
    """Test top-level route dispatch and fallback boundaries."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("protocol", "chain", "method_name"),
        [
            ("uniswap_v3", "arbitrum", "_query_v3_liquidity"),
            # aerodrome is a solidly AMM whose subgraph is a v3 fork; the
            # connector declares liquidity_query_family="v3_concentrated" (ALM-2930).
            ("aerodrome", "base", "_query_v3_liquidity"),
            ("traderjoe_v2", "avalanche", "_query_liquidity_book"),
            ("balancer", "ethereum", "_query_balancer_liquidity"),
            # curve + sushiswap declared deployments are Messari-standard
            # (see tests/audit/test_subgraph_schema_parity.py).
            ("curve", "ethereum", "_query_messari_liquidity"),
            ("sushiswap_v3", "ethereum", "_query_messari_liquidity"),
        ],
    )
    async def test_protocol_family_dispatches_to_expected_query_method(
        self,
        provider,
        protocol,
        chain,
        method_name,
    ):
        sentinel = object()
        query_method = AsyncMock(return_value=sentinel)
        setattr(provider, method_name, query_method)

        result = await provider.get_liquidity_depth(
            pool_address="0xpool",
            chain=chain,
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            protocol=protocol,
        )

        assert result is sentinel
        query_method.assert_awaited_once_with(
            "0xpool",
            chain,
            datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            protocol,
        )

    @pytest.mark.asyncio
    async def test_unknown_protocol_returns_fallback_without_querying(self, provider):
        provider._query_v3_liquidity = AsyncMock()  # type: ignore[method-assign]

        result = await provider.get_liquidity_depth(
            pool_address="0xpool",
            chain="arbitrum",
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            protocol="not_a_dex",
        )

        assert result.depth == Decimal("0")
        assert result.source_info.source == DATA_SOURCE_FALLBACK
        provider._query_v3_liquidity.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_pagination_overflow_stays_loud(self, provider):
        provider._query_v3_liquidity = AsyncMock(  # type: ignore[method-assign]
            side_effect=DataSourceUnavailableError(
                data_type="liquidity",
                identifier="0xpool",
                remediation="narrow the query window",
                message="window too large",
            )
        )

        with pytest.raises(DataSourceUnavailableError, match="window too large"):
            await provider.get_liquidity_depth(
                pool_address="0xpool",
                chain="arbitrum",
                timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
                protocol="uniswap_v3",
            )

    @pytest.mark.asyncio
    async def test_messari_pagination_overflow_stays_loud(self, provider, monkeypatch):
        # Regression (CodeRabbit #3271): the messari-standard handler must let
        # DataSourceUnavailableError (pagination overflow) propagate like every
        # sibling family — a broad `except Exception` previously swallowed it,
        # breaking the fail-loud guarantee for curve / sushiswap_v3.
        monkeypatch.setattr(provider, "_get_subgraph_id", lambda *_a, **_k: "deployment-id")
        provider._client.query_with_pagination = AsyncMock(  # type: ignore[method-assign]
            side_effect=DataSourceUnavailableError(
                data_type="liquidity",
                identifier="0xpool",
                remediation="narrow the query window",
                message="window too large",
            )
        )

        with pytest.raises(DataSourceUnavailableError, match="window too large"):
            await provider._query_messari_liquidity(
                pool_address="0xpool",
                chain="ethereum",
                timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
                protocol_id="sushiswap_v3",
            )


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
            chain="arbitrum",
            timestamp=timestamp,
            protocol="uniswap_v3",
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
            chain="arbitrum",
            timestamp=timestamp,
            protocol="uniswap_v3",
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
            chain="arbitrum",
            timestamp=timestamp,
            protocol="uniswap_v3",
        )

        assert result.depth == Decimal("0")
        assert result.source_info.confidence == DataConfidence.LOW


# =============================================================================
# Test Range Query
# =============================================================================


class TestRangeQuery:
    """Test get_liquidity_depth_range method."""

    @pytest.mark.asyncio
    async def test_range_query_returns_multiple_results(self, provider, mock_subgraph_client):
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
            chain="arbitrum",
            start_time=start_time,
            end_time=end_time,
            protocol="uniswap_v3",
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

    def test_legacy_uppercase_to_canonical(self, provider):
        """Legacy uppercase (enum-era) values convert to lowercase keys."""
        assert provider._get_protocol_id("UNISWAP_V3") == "uniswap_v3"
        assert provider._get_protocol_id("AERODROME") == "aerodrome"

    def test_string_to_lowercase(self, provider):
        """Test string protocols are lowercased."""
        assert provider._get_protocol_id("CURVE") == "curve"
        assert provider._get_protocol_id("Balancer") == "balancer"

    def test_none_returns_none(self, provider):
        """Test None protocol returns None."""
        assert provider._get_protocol_id(None) is None

    def test_declared_aliases_resolve_to_canonical(self, provider):
        """Decl aliases resolve so liquidity subgraph-ID lookups succeed."""
        assert provider._get_protocol_id("uni_v3") == "uniswap_v3"
        assert provider._get_protocol_id("crv") == "curve"
        assert provider._get_protocol_id("bal") == "balancer"

    def test_unknown_protocol_keeps_raw_string(self, provider):
        """Unknown identifiers stay raw — explicit unknowns must hit the
        warning + fallback path, never chain auto-detection (None)."""
        assert provider._get_protocol_id("not_a_dex") == "not_a_dex"

    @pytest.mark.asyncio
    async def test_alias_routes_to_family_query(self, provider, mock_subgraph_client):
        """An alias takes the same query path as its canonical protocol."""
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

        result = await provider.get_liquidity_depth(
            pool_address="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
            chain="arbitrum",
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            protocol="uni_v3",
        )

        assert result.depth == Decimal("5000000")
        assert result.source_info.confidence == DataConfidence.HIGH
        assert result.source_info.source == "uniswap_v3_subgraph"


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
            chain="arbitrum",
            timestamp=naive_timestamp,
            protocol="uniswap_v3",
        )

        # Result timestamp should have UTC
        assert result.source_info.timestamp.tzinfo is not None


# =============================================================================
# Cursor pagination through the provider (VIB-5089)
# =============================================================================


class TestCursorPaginationThroughProvider:
    """Provider-level proof that >1000-row windows are fetched fully (VIB-5089)."""

    @pytest.mark.asyncio
    async def test_long_twap_window_consumes_all_pages(self, mock_subgraph_client):
        """A 1200-day TWAP window (>1000 daily rows) uses every data point.

        TVL ramps linearly 0..1199, so the exact TWAP (599.5) is only
        reachable when all 1200 points arrive; truncation at the first 1000
        points would yield 499.5 instead.
        """
        n_days = 1200
        day0 = 1_600_000_000  # aligned base timestamp
        rows = [
            {
                "id": f"day-{i}",
                "date": day0 + i * 86_400,
                "tvlUSD": str(i),
                "liquidity": "1",
            }
            for i in range(n_days)
        ]

        async def fake_query(subgraph_id, query, variables):
            lo = int(variables["startDate"])
            hi = int(variables["endDate"])
            window = [r for r in rows if lo <= r["date"] <= hi]
            window.sort(key=lambda r: r["date"])
            return {"poolDayDatas": window[: variables["first"]]}

        mock_subgraph_client.query = AsyncMock(side_effect=fake_query)
        provider = LiquidityDepthProvider(
            client=mock_subgraph_client,
            use_twap=True,
            twap_window_hours=n_days * 24,
        )

        target = datetime.fromtimestamp(day0 + (n_days - 1) * 86_400, tz=UTC)
        result = await provider.get_liquidity_depth(
            pool_address="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
            chain="arbitrum",
            timestamp=target,
            protocol="uniswap_v3",
        )

        assert result.depth == Decimal("599.5")
        assert result.source_info.confidence == DataConfidence.HIGH
        # 2 pages: 1000 + 201 (boundary row re-fetched and deduplicated)
        assert mock_subgraph_client.query.call_count == 2


class TestBalancerPoolIdNormalization:
    """Full 32-byte Balancer pool IDs normalize to their leading address."""

    @pytest.mark.asyncio
    async def test_full_pool_id_queries_by_leading_address(self, provider, mock_subgraph_client):
        captured: dict = {}

        async def capture(**kwargs):
            captured.update(kwargs.get("variables") or {})
            return []

        mock_subgraph_client.query_with_pagination = capture
        full_id = "0x32296969ef14eb0c6d29669c550d4a0449130230000200000000000000000080"

        await provider.get_liquidity_depth(
            pool_address=full_id,
            chain="ethereum",
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            protocol="balancer",
        )

        assert captured.get("poolAddress") == full_id[:42]
