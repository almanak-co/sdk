"""Tests for MultiDexPriceService.

This module tests the multi-DEX price comparison service including:
- Quote fetching from individual DEXs
- Cross-DEX price comparison
- Best DEX selection
- Slippage estimation
- Caching behavior
- Error handling
"""

from decimal import Decimal

import pytest

from ..multi_dex import (
    DEX_CHAINS,
    SUPPORTED_DEXS,
    Dex,
    DexNotSupportedError,
    DexQuote,
    MultiDexPriceResult,
    MultiDexPriceService,
)

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def service():
    """Create a MultiDexPriceService for testing."""
    return MultiDexPriceService(chain="ethereum")


@pytest.fixture
def arbitrum_service():
    """Create a MultiDexPriceService for Arbitrum."""
    return MultiDexPriceService(chain="arbitrum")


@pytest.fixture
def mock_uniswap_quote():
    """Mock quote function for Uniswap V3."""

    def quote_fn(token_in: str, token_out: str, amount_in: Decimal) -> DexQuote:
        return DexQuote(
            dex="uniswap_v3",
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            amount_out=amount_in * Decimal("0.0004"),  # 1 USDC = 0.0004 WETH
            price=Decimal("0.0004"),
            price_impact_bps=10,
            slippage_estimate_bps=5,
            gas_estimate=150000,
            gas_cost_usd=Decimal("5.00"),
            fee_bps=30,
            route="Direct pool",
            chain="ethereum",
        )

    return quote_fn


@pytest.fixture
def mock_curve_quote():
    """Mock quote function for Curve."""

    def quote_fn(token_in: str, token_out: str, amount_in: Decimal) -> DexQuote:
        # Curve is best for stablecoin pairs
        if token_in in ["USDC", "USDT", "DAI"] and token_out in ["USDC", "USDT", "DAI"]:
            return DexQuote(
                dex="curve",
                token_in=token_in,
                token_out=token_out,
                amount_in=amount_in,
                amount_out=amount_in * Decimal("0.9999"),  # Very tight spread
                price=Decimal("0.9999"),
                price_impact_bps=1,
                slippage_estimate_bps=1,
                gas_estimate=200000,
                gas_cost_usd=Decimal("7.00"),
                fee_bps=4,
                route="3pool",
                chain="ethereum",
            )
        else:
            return DexQuote(
                dex="curve",
                token_in=token_in,
                token_out=token_out,
                amount_in=amount_in,
                amount_out=amount_in * Decimal("0.00039"),  # Worse than Uniswap
                price=Decimal("0.00039"),
                price_impact_bps=15,
                slippage_estimate_bps=8,
                gas_estimate=200000,
                gas_cost_usd=Decimal("7.00"),
                fee_bps=30,
                route="CryptoSwap",
                chain="ethereum",
            )

    return quote_fn


@pytest.fixture
def mock_enso_quote():
    """Mock quote function for Enso."""

    def quote_fn(token_in: str, token_out: str, amount_in: Decimal) -> DexQuote:
        return DexQuote(
            dex="enso",
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            amount_out=amount_in * Decimal("0.00041"),  # Best rate as aggregator
            price=Decimal("0.00041"),
            price_impact_bps=8,
            slippage_estimate_bps=4,
            gas_estimate=250000,
            gas_cost_usd=Decimal("10.00"),
            fee_bps=20,
            route="Multi-DEX aggregated",
            chain="ethereum",
        )

    return quote_fn


# =============================================================================
# Initialization Tests
# =============================================================================


class TestMultiDexPriceServiceInit:
    """Tests for MultiDexPriceService initialization."""

    def test_init_default_chain(self):
        """Test initialization with default chain."""
        service = MultiDexPriceService()
        assert service.chain == "ethereum"
        assert "uniswap_v3" in service.dexs
        assert "curve" in service.dexs
        assert "enso" in service.dexs

    def test_init_with_chain(self):
        """Test initialization with specific chain."""
        service = MultiDexPriceService(chain="arbitrum")
        assert service.chain == "arbitrum"
        assert "uniswap_v3" in service.dexs

    def test_init_unsupported_chain(self):
        """Test initialization with unsupported chain raises error."""
        with pytest.raises(ValueError, match="Unsupported chain"):
            MultiDexPriceService(chain="unsupported_chain")

    def test_init_with_specific_dexs(self):
        """Test initialization with specific DEXs."""
        service = MultiDexPriceService(
            chain="ethereum",
            dexs=["uniswap_v3", "curve"],
        )
        assert service.dexs == ["uniswap_v3", "curve"]
        assert "enso" not in service.dexs

    def test_init_with_unsupported_dex(self):
        """Test initialization with unsupported DEX raises error."""
        with pytest.raises(DexNotSupportedError):
            MultiDexPriceService(
                chain="ethereum",
                dexs=["uniswap_v3", "unsupported_dex"],
            )

    def test_init_custom_cache_ttl(self):
        """Test initialization with custom cache TTL."""
        service = MultiDexPriceService(
            chain="ethereum",
            cache_ttl_seconds=30.0,
        )
        assert service._cache_ttl_seconds == 30.0


# =============================================================================
# Quote Fetching Tests
# =============================================================================


class TestGetQuote:
    """Tests for individual DEX quote fetching."""

    @pytest.mark.asyncio
    async def test_get_uniswap_quote(self, service, mock_uniswap_quote):
        """Test getting quote from Uniswap V3."""
        service.set_mock_quote("uniswap_v3", mock_uniswap_quote)

        quote = await service.get_quote(
            dex="uniswap_v3",
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
        )

        assert quote.dex == "uniswap_v3"
        assert quote.token_in == "USDC"
        assert quote.token_out == "WETH"
        assert quote.amount_in == Decimal("10000")
        assert quote.amount_out == Decimal("4")  # 10000 * 0.0004
        assert quote.price == Decimal("0.0004")

    @pytest.mark.asyncio
    async def test_get_curve_quote(self, service, mock_curve_quote):
        """Test getting quote from Curve."""
        service.set_mock_quote("curve", mock_curve_quote)

        quote = await service.get_quote(
            dex="curve",
            token_in="USDC",
            token_out="DAI",
            amount_in=Decimal("10000"),
        )

        assert quote.dex == "curve"
        assert quote.amount_out == Decimal("9999")  # Very tight spread
        assert quote.price_impact_bps == 1
        assert quote.slippage_estimate_bps == 1

    @pytest.mark.asyncio
    async def test_get_enso_quote(self, service, mock_enso_quote):
        """Test getting quote from Enso."""
        service.set_mock_quote("enso", mock_enso_quote)

        quote = await service.get_quote(
            dex="enso",
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
        )

        assert quote.dex == "enso"
        assert quote.route == "Multi-DEX aggregated"

    @pytest.mark.asyncio
    async def test_get_quote_unsupported_dex(self, service):
        """Test getting quote from unsupported DEX raises error."""
        with pytest.raises(DexNotSupportedError):
            await service.get_quote(
                dex="unsupported_dex",
                token_in="USDC",
                token_out="WETH",
                amount_in=Decimal("10000"),
            )

    @pytest.mark.asyncio
    async def test_get_quote_caching(self, service, mock_uniswap_quote):
        """Test that quotes are cached."""
        call_count = 0

        def counting_quote_fn(token_in: str, token_out: str, amount_in: Decimal) -> DexQuote:
            nonlocal call_count
            call_count += 1
            return mock_uniswap_quote(token_in, token_out, amount_in)

        service.set_mock_quote("uniswap_v3", counting_quote_fn)

        # First call
        await service.get_quote("uniswap_v3", "USDC", "WETH", Decimal("10000"))
        assert call_count == 1

        # Second call should use cache
        await service.get_quote("uniswap_v3", "USDC", "WETH", Decimal("10000"))
        assert call_count == 1  # Still 1, cache was used

        # Different amount should not use cache
        await service.get_quote("uniswap_v3", "USDC", "WETH", Decimal("20000"))
        assert call_count == 2


# =============================================================================
# Cross-DEX Price Comparison Tests
# =============================================================================


class TestGetPricesAcrossDexs:
    """Tests for cross-DEX price comparison."""

    @pytest.mark.asyncio
    async def test_get_prices_all_dexs(self, service, mock_uniswap_quote, mock_curve_quote, mock_enso_quote):
        """Test getting prices from all DEXs."""
        service.set_mock_quote("uniswap_v3", mock_uniswap_quote)
        service.set_mock_quote("curve", mock_curve_quote)
        service.set_mock_quote("enso", mock_enso_quote)

        result = await service.get_prices_across_dexs(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
        )

        assert result.token_in == "USDC"
        assert result.token_out == "WETH"
        assert result.amount_in == Decimal("10000")
        assert len(result.quotes) == 3
        assert "uniswap_v3" in result.quotes
        assert "curve" in result.quotes
        assert "enso" in result.quotes

    @pytest.mark.asyncio
    async def test_get_prices_specific_dexs(self, service, mock_uniswap_quote, mock_curve_quote):
        """Test getting prices from specific DEXs."""
        service.set_mock_quote("uniswap_v3", mock_uniswap_quote)
        service.set_mock_quote("curve", mock_curve_quote)

        result = await service.get_prices_across_dexs(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
            dexs=["uniswap_v3", "curve"],
        )

        assert len(result.quotes) == 2
        assert "enso" not in result.quotes

    @pytest.mark.asyncio
    async def test_best_quote_property(self, service, mock_uniswap_quote, mock_curve_quote, mock_enso_quote):
        """Test that best_quote property returns highest output."""
        service.set_mock_quote("uniswap_v3", mock_uniswap_quote)
        service.set_mock_quote("curve", mock_curve_quote)
        service.set_mock_quote("enso", mock_enso_quote)

        result = await service.get_prices_across_dexs(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
        )

        # Enso has best rate (0.00041 vs 0.0004 vs 0.00039)
        assert result.best_quote is not None
        assert result.best_quote.dex == "enso"

    @pytest.mark.asyncio
    async def test_price_spread_calculation(self, service, mock_uniswap_quote, mock_curve_quote, mock_enso_quote):
        """Test price spread calculation between DEXs."""
        service.set_mock_quote("uniswap_v3", mock_uniswap_quote)
        service.set_mock_quote("curve", mock_curve_quote)
        service.set_mock_quote("enso", mock_enso_quote)

        result = await service.get_prices_across_dexs(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
        )

        # Spread should be positive (best > worst)
        assert result.price_spread_bps > 0

    @pytest.mark.asyncio
    async def test_partial_failure(self, service, mock_uniswap_quote):
        """Test that partial failures still return available quotes."""
        service.set_mock_quote("uniswap_v3", mock_uniswap_quote)
        # Don't set mocks for curve and enso - they will use defaults

        result = await service.get_prices_across_dexs(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
        )

        # Should still have quotes (defaults)
        assert len(result.quotes) >= 1


# =============================================================================
# Best DEX Selection Tests
# =============================================================================


class TestGetBestDexPrice:
    """Tests for best DEX selection."""

    @pytest.mark.asyncio
    async def test_get_best_dex(self, service, mock_uniswap_quote, mock_curve_quote, mock_enso_quote):
        """Test selecting the best DEX."""
        service.set_mock_quote("uniswap_v3", mock_uniswap_quote)
        service.set_mock_quote("curve", mock_curve_quote)
        service.set_mock_quote("enso", mock_enso_quote)

        result = await service.get_best_dex_price(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
        )

        assert result.best_dex == "enso"  # Best rate
        assert result.best_quote is not None
        assert result.best_quote.amount_out == Decimal("4.1")  # 10000 * 0.00041

    @pytest.mark.asyncio
    async def test_savings_calculation(self, service, mock_uniswap_quote, mock_curve_quote, mock_enso_quote):
        """Test savings vs worst venue calculation."""
        service.set_mock_quote("uniswap_v3", mock_uniswap_quote)
        service.set_mock_quote("curve", mock_curve_quote)
        service.set_mock_quote("enso", mock_enso_quote)

        result = await service.get_best_dex_price(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
        )

        # Savings should be positive
        assert result.savings_vs_worst_bps > 0

    @pytest.mark.asyncio
    async def test_all_quotes_included(self, service, mock_uniswap_quote, mock_curve_quote, mock_enso_quote):
        """Test that all quotes are included in result."""
        service.set_mock_quote("uniswap_v3", mock_uniswap_quote)
        service.set_mock_quote("curve", mock_curve_quote)
        service.set_mock_quote("enso", mock_enso_quote)

        result = await service.get_best_dex_price(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
        )

        assert len(result.all_quotes) == 3

    @pytest.mark.asyncio
    async def test_curve_best_for_stables(self, service, mock_curve_quote):
        """Test that Curve wins for stablecoin pairs."""
        service.set_mock_quote("curve", mock_curve_quote)

        # Use only curve for stablecoin test
        result = await service.get_best_dex_price(
            token_in="USDC",
            token_out="DAI",
            amount_in=Decimal("10000"),
            dexs=["curve"],
        )

        assert result.best_dex == "curve"
        assert result.best_quote is not None
        assert result.best_quote.price_impact_bps == 1  # Very low


# =============================================================================
# DexQuote Tests
# =============================================================================


class TestDexQuote:
    """Tests for DexQuote dataclass."""

    def test_net_output_calculation(self):
        """Test net output with slippage."""
        quote = DexQuote(
            dex="uniswap_v3",
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
            amount_out=Decimal("4"),
            price=Decimal("0.0004"),
            slippage_estimate_bps=50,  # 0.5% slippage
        )

        # Net output = 4 * (1 - 0.005) = 3.98
        assert quote.net_output == Decimal("3.98")

    def test_net_output_no_slippage(self):
        """Test net output with zero slippage."""
        quote = DexQuote(
            dex="uniswap_v3",
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
            amount_out=Decimal("4"),
            price=Decimal("0.0004"),
            slippage_estimate_bps=0,
        )

        assert quote.net_output == Decimal("4")

    def test_is_valid(self):
        """Test quote validity check."""
        valid_quote = DexQuote(
            dex="uniswap_v3",
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
            amount_out=Decimal("4"),
            price=Decimal("0.0004"),
        )
        assert valid_quote.is_valid

        invalid_quote = DexQuote(
            dex="uniswap_v3",
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("0"),
            amount_out=Decimal("0"),
            price=Decimal("0"),
        )
        assert not invalid_quote.is_valid

    def test_to_dict(self):
        """Test quote serialization."""
        quote = DexQuote(
            dex="uniswap_v3",
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
            amount_out=Decimal("4"),
            price=Decimal("0.0004"),
            price_impact_bps=10,
            slippage_estimate_bps=5,
            gas_estimate=150000,
            gas_cost_usd=Decimal("5.00"),
            fee_bps=30,
            route="Direct pool",
            chain="ethereum",
        )

        data = quote.to_dict()

        assert data["dex"] == "uniswap_v3"
        assert data["token_in"] == "USDC"
        assert data["amount_in"] == "10000"
        assert data["price_impact_bps"] == 10


# =============================================================================
# MultiDexPriceResult Tests
# =============================================================================


class TestMultiDexPriceResult:
    """Tests for MultiDexPriceResult dataclass."""

    def test_best_quote_empty(self):
        """Test best_quote with no quotes."""
        result = MultiDexPriceResult(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
            quotes={},
        )

        assert result.best_quote is None

    def test_price_spread_single_quote(self):
        """Test price spread with single quote."""
        quote = DexQuote(
            dex="uniswap_v3",
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
            amount_out=Decimal("4"),
            price=Decimal("0.0004"),
        )

        result = MultiDexPriceResult(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
            quotes={"uniswap_v3": quote},
        )

        assert result.price_spread_bps == 0  # No spread with single quote


# =============================================================================
# Slippage Estimation Tests
# =============================================================================


class TestSlippageEstimation:
    """Tests for slippage estimation."""

    @pytest.mark.asyncio
    async def test_slippage_increases_with_size(self, service):
        """Test that slippage increases with trade size."""
        # Small trade
        small_quote = await service.get_quote(
            dex="uniswap_v3",
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1000"),
        )

        # Large trade
        large_quote = await service.get_quote(
            dex="uniswap_v3",
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1000000"),
        )

        # Large trade should have higher slippage
        assert large_quote.slippage_estimate_bps >= small_quote.slippage_estimate_bps

    @pytest.mark.asyncio
    async def test_curve_lower_slippage_for_stables(self, service):
        """Test that Curve has lower slippage for stablecoins."""
        # Get quotes for stablecoin pair
        curve_quote = await service.get_quote(
            dex="curve",
            token_in="USDC",
            token_out="DAI",
            amount_in=Decimal("10000"),
        )

        uniswap_quote = await service.get_quote(
            dex="uniswap_v3",
            token_in="USDC",
            token_out="DAI",
            amount_in=Decimal("10000"),
        )

        # Curve should have lower or equal slippage for stables
        assert curve_quote.slippage_estimate_bps <= uniswap_quote.slippage_estimate_bps


# =============================================================================
# Cache Management Tests
# =============================================================================


class TestCacheManagement:
    """Tests for cache management."""

    def test_clear_cache(self, service):
        """Test clearing the quote cache."""
        # Add something to cache
        service._quote_cache["test_key"] = (
            DexQuote(
                dex="uniswap_v3",
                token_in="USDC",
                token_out="WETH",
                amount_in=Decimal("10000"),
                amount_out=Decimal("4"),
                price=Decimal("0.0004"),
            ),
            0,
        )

        assert len(service._quote_cache) > 0

        service.clear_cache()

        assert len(service._quote_cache) == 0

    def test_set_mock_quote(self, service):
        """Test setting mock quote function."""

        def mock_fn(token_in, token_out, amount_in):
            return DexQuote(
                dex="uniswap_v3",
                token_in=token_in,
                token_out=token_out,
                amount_in=amount_in,
                amount_out=amount_in * Decimal("0.001"),
                price=Decimal("0.001"),
            )

        service.set_mock_quote("uniswap_v3", mock_fn)

        assert "uniswap_v3" in service._mock_quotes

    def test_clear_mock_quotes(self, service):
        """Test clearing mock quote functions."""
        service.set_mock_quote("uniswap_v3", lambda *args: None)
        service.set_mock_quote("curve", lambda *args: None)

        service.clear_mock_quotes()

        assert len(service._mock_quotes) == 0


# =============================================================================
# Constants Tests
# =============================================================================


class TestConstants:
    """Tests for module constants."""

    def test_supported_dexs(self):
        """Test SUPPORTED_DEXS constant."""
        assert "uniswap_v3" in SUPPORTED_DEXS
        assert "curve" in SUPPORTED_DEXS
        assert "enso" in SUPPORTED_DEXS

    def test_dex_chains(self):
        """Test DEX_CHAINS constant."""
        assert "ethereum" in DEX_CHAINS
        assert "arbitrum" in DEX_CHAINS
        assert "uniswap_v3" in DEX_CHAINS["ethereum"]
        assert "curve" in DEX_CHAINS["ethereum"]

    def test_dex_enum(self):
        """Test Dex enum values."""
        assert Dex.UNISWAP_V3.value == "uniswap_v3"
        assert Dex.CURVE.value == "curve"
        assert Dex.ENSO.value == "enso"
