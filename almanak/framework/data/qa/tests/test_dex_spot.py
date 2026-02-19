"""Tests for DEX Spot Price Test Module.

This test suite covers:
- DEXSpotResult dataclass creation and serialization
- DEXSpotPriceTest with mocked MultiDexPriceService
- Pass/fail logic validation
- Error handling for QuoteUnavailableError
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.data.qa.config import QAConfig, QAThresholds
from almanak.framework.data.qa.tests.dex_spot import (
    DEFAULT_TRADE_SIZE_WETH,
    DEXSpotPriceTest,
    DEXSpotResult,
)
from almanak.gateway.data.price import (
    BestDexResult,
    DexQuote,
    QuoteUnavailableError,
)

# =============================================================================
# DEXSpotResult Tests
# =============================================================================


class TestDEXSpotResult:
    """Tests for DEXSpotResult dataclass."""

    def test_create_passing_result(self) -> None:
        """Test creating a passing result."""
        result = DEXSpotResult(
            token="USDC",
            best_dex="uniswap_v3",
            price_weth=Decimal("0.0004"),
            amount_out=Decimal("0.00016"),
            price_impact_bps=5,
            passed=True,
            error=None,
        )

        assert result.token == "USDC"
        assert result.best_dex == "uniswap_v3"
        assert result.price_weth == Decimal("0.0004")
        assert result.amount_out == Decimal("0.00016")
        assert result.price_impact_bps == 5
        assert result.passed is True
        assert result.error is None

    def test_create_failing_result(self) -> None:
        """Test creating a failing result."""
        result = DEXSpotResult(
            token="USDC",
            best_dex=None,
            price_weth=None,
            amount_out=None,
            price_impact_bps=None,
            passed=False,
            error="Quote unavailable: No liquidity",
        )

        assert result.token == "USDC"
        assert result.best_dex is None
        assert result.price_weth is None
        assert result.amount_out is None
        assert result.price_impact_bps is None
        assert result.passed is False
        assert result.error == "Quote unavailable: No liquidity"

    def test_to_dict_with_values(self) -> None:
        """Test serialization with all values present."""
        result = DEXSpotResult(
            token="LINK",
            best_dex="enso",
            price_weth=Decimal("0.005"),
            amount_out=Decimal("0.002"),
            price_impact_bps=12,
            passed=True,
            error=None,
        )

        d = result.to_dict()

        assert d["token"] == "LINK"
        assert d["best_dex"] == "enso"
        assert d["price_weth"] == "0.005"
        assert d["amount_out"] == "0.002"
        assert d["price_impact_bps"] == 12
        assert d["passed"] is True
        assert d["error"] is None

    def test_to_dict_with_none_values(self) -> None:
        """Test serialization with None values."""
        result = DEXSpotResult(
            token="USDC",
            best_dex=None,
            price_weth=None,
            amount_out=None,
            price_impact_bps=None,
            passed=False,
            error="Some error",
        )

        d = result.to_dict()

        assert d["best_dex"] is None
        assert d["price_weth"] is None
        assert d["amount_out"] is None
        assert d["price_impact_bps"] is None


# =============================================================================
# DEXSpotPriceTest Tests
# =============================================================================


@pytest.fixture
def mock_dex_service() -> MagicMock:
    """Create a mock MultiDexPriceService."""
    mock = MagicMock()
    mock.get_best_dex_price = AsyncMock()
    mock.clear_cache = MagicMock()
    return mock


@pytest.fixture
def qa_config() -> QAConfig:
    """Create a QAConfig for testing."""
    return QAConfig(
        chain="arbitrum",
        historical_days=30,
        timeframe="4h",
        rsi_period=14,
        thresholds=QAThresholds(
            min_confidence=0.8,
            max_price_impact_bps=100,
            max_gap_hours=8.0,
            max_stale_seconds=120,
        ),
        popular_tokens=["ETH", "WBTC"],
        additional_tokens=["UNI"],
        dex_tokens=["USDC", "LINK", "ARB"],
    )


def create_mock_quote(
    token: str,
    dex: str = "uniswap_v3",
    price: Decimal = Decimal("0.0004"),
    amount_out: Decimal = Decimal("0.00016"),
    price_impact_bps: int = 5,
) -> DexQuote:
    """Helper to create a DexQuote for testing."""
    return DexQuote(
        dex=dex,
        token_in=token,
        token_out="WETH",
        amount_in=DEFAULT_TRADE_SIZE_WETH,
        amount_out=amount_out,
        price=price,
        price_impact_bps=price_impact_bps,
        slippage_estimate_bps=3,
        gas_estimate=150000,
        gas_cost_usd=Decimal("5.00"),
        fee_bps=30,
        route="Direct pool",
        timestamp=datetime.now(UTC),
        chain="arbitrum",
    )


def create_mock_best_result(
    token: str,
    quote: DexQuote | None,
) -> BestDexResult:
    """Helper to create a BestDexResult for testing."""
    return BestDexResult(
        token_in=token,
        token_out="WETH",
        amount_in=DEFAULT_TRADE_SIZE_WETH,
        best_dex=quote.dex if quote else None,
        best_quote=quote,
        all_quotes=[quote] if quote else [],
        savings_vs_worst_bps=0,
        timestamp=datetime.now(UTC),
    )


class TestDEXSpotPriceTest:
    """Tests for DEXSpotPriceTest class."""

    @pytest.mark.asyncio
    async def test_run_all_passing(self, mock_dex_service: MagicMock, qa_config: QAConfig) -> None:
        """Test run() when all tokens pass validation."""

        # Configure mock to return good quotes for all tokens
        def mock_get_best_price(token_in: str, token_out: str, amount_in: Decimal):
            quote = create_mock_quote(token_in, price_impact_bps=10)
            return create_mock_best_result(token_in, quote)

        mock_dex_service.get_best_dex_price.side_effect = mock_get_best_price

        test = DEXSpotPriceTest(qa_config, mock_dex_service)
        results = await test.run()

        # Should have 3 results (dex_tokens: USDC, LINK, ARB)
        assert len(results) == 3

        # All should pass
        assert all(r.passed for r in results)
        assert all(r.error is None for r in results)

        # Verify get_best_dex_price was called for each token
        assert mock_dex_service.get_best_dex_price.call_count == 3

    @pytest.mark.asyncio
    async def test_run_high_price_impact_fails(self, mock_dex_service: MagicMock, qa_config: QAConfig) -> None:
        """Test that high price impact causes failure."""

        # Configure mock to return quote with high price impact (>100 bps)
        def mock_get_best_price(token_in: str, token_out: str, amount_in: Decimal):
            quote = create_mock_quote(token_in, price_impact_bps=150)
            return create_mock_best_result(token_in, quote)

        mock_dex_service.get_best_dex_price.side_effect = mock_get_best_price

        test = DEXSpotPriceTest(qa_config, mock_dex_service)
        results = await test.run()

        # All should fail due to high price impact
        assert all(not r.passed for r in results)
        assert all("High price impact" in (r.error or "") for r in results)

    @pytest.mark.asyncio
    async def test_run_zero_price_fails(self, mock_dex_service: MagicMock, qa_config: QAConfig) -> None:
        """Test that zero price causes failure."""

        def mock_get_best_price(token_in: str, token_out: str, amount_in: Decimal):
            quote = create_mock_quote(token_in, price=Decimal("0"), amount_out=Decimal("0"))
            return create_mock_best_result(token_in, quote)

        mock_dex_service.get_best_dex_price.side_effect = mock_get_best_price

        test = DEXSpotPriceTest(qa_config, mock_dex_service)
        results = await test.run()

        # All should fail due to zero price
        assert all(not r.passed for r in results)
        assert all("Invalid price" in (r.error or "") for r in results)

    @pytest.mark.asyncio
    async def test_run_no_quotes_available(self, mock_dex_service: MagicMock, qa_config: QAConfig) -> None:
        """Test handling when no DEX quotes are available."""

        def mock_get_best_price(token_in: str, token_out: str, amount_in: Decimal):
            return create_mock_best_result(token_in, None)

        mock_dex_service.get_best_dex_price.side_effect = mock_get_best_price

        test = DEXSpotPriceTest(qa_config, mock_dex_service)
        results = await test.run()

        # All should fail with "No DEX quotes available"
        assert all(not r.passed for r in results)
        assert all(r.best_dex is None for r in results)
        assert all("No DEX quotes available" in (r.error or "") for r in results)

    @pytest.mark.asyncio
    async def test_run_handles_quote_unavailable_error(self, mock_dex_service: MagicMock, qa_config: QAConfig) -> None:
        """Test graceful handling of QuoteUnavailableError."""
        mock_dex_service.get_best_dex_price.side_effect = QuoteUnavailableError(
            dex="uniswap_v3",
            token_in="USDC",
            token_out="WETH",
            reason="No liquidity in pool",
        )

        test = DEXSpotPriceTest(qa_config, mock_dex_service)
        results = await test.run()

        # All should fail with error message
        assert all(not r.passed for r in results)
        assert all(r.price_weth is None for r in results)
        assert all("Quote unavailable" in (r.error or "") for r in results)

    @pytest.mark.asyncio
    async def test_run_handles_unexpected_error(self, mock_dex_service: MagicMock, qa_config: QAConfig) -> None:
        """Test graceful handling of unexpected exceptions."""
        mock_dex_service.get_best_dex_price.side_effect = RuntimeError("Network error")

        test = DEXSpotPriceTest(qa_config, mock_dex_service)
        results = await test.run()

        # All should fail with error message
        assert all(not r.passed for r in results)
        assert all("Unexpected error" in (r.error or "") for r in results)

    @pytest.mark.asyncio
    async def test_run_mixed_results(self, mock_dex_service: MagicMock, qa_config: QAConfig) -> None:
        """Test handling of mixed pass/fail results."""

        def mock_get_best_price(token_in: str, token_out: str, amount_in: Decimal):
            if token_in == "USDC":
                # USDC passes
                quote = create_mock_quote(token_in, price_impact_bps=10)
                return create_mock_best_result(token_in, quote)
            elif token_in == "LINK":
                # LINK fails due to high price impact
                quote = create_mock_quote(token_in, price_impact_bps=200)
                return create_mock_best_result(token_in, quote)
            else:
                # ARB has no quotes
                return create_mock_best_result(token_in, None)

        mock_dex_service.get_best_dex_price.side_effect = mock_get_best_price

        test = DEXSpotPriceTest(qa_config, mock_dex_service)
        results = await test.run()

        # Find results by token
        usdc_result = next(r for r in results if r.token == "USDC")
        link_result = next(r for r in results if r.token == "LINK")
        arb_result = next(r for r in results if r.token == "ARB")

        # USDC should pass
        assert usdc_result.passed is True
        assert usdc_result.error is None

        # LINK should fail due to high price impact
        assert link_result.passed is False
        assert "High price impact" in (link_result.error or "")

        # ARB should fail due to no quotes
        assert arb_result.passed is False
        assert "No DEX quotes available" in (arb_result.error or "")

    @pytest.mark.asyncio
    async def test_context_manager(self, mock_dex_service: MagicMock, qa_config: QAConfig) -> None:
        """Test async context manager properly closes resources."""
        async with DEXSpotPriceTest(qa_config, mock_dex_service) as test:
            assert test is not None

        mock_dex_service.clear_cache.assert_called_once()

    @pytest.mark.asyncio
    async def test_empty_token_list(self, mock_dex_service: MagicMock) -> None:
        """Test handling of empty dex_tokens list."""
        config = QAConfig(
            popular_tokens=["ETH"],
            additional_tokens=[],
            dex_tokens=[],
        )

        test = DEXSpotPriceTest(config, mock_dex_service)
        results = await test.run()

        # Should return empty list
        assert results == []
        mock_dex_service.get_best_dex_price.assert_not_called()

    @pytest.mark.asyncio
    async def test_custom_trade_size(self, mock_dex_service: MagicMock, qa_config: QAConfig) -> None:
        """Test that custom trade size is used."""
        custom_size = Decimal("1.0")  # 1 WETH

        def mock_get_best_price(token_in: str, token_out: str, amount_in: Decimal):
            # Verify the trade size is correct
            assert amount_in == custom_size
            quote = create_mock_quote(token_in)
            return create_mock_best_result(token_in, quote)

        mock_dex_service.get_best_dex_price.side_effect = mock_get_best_price

        test = DEXSpotPriceTest(qa_config, mock_dex_service, trade_size=custom_size)
        await test.run()

        # The assertion in mock_get_best_price verifies the trade size
        assert mock_dex_service.get_best_dex_price.call_count == 3

    @pytest.mark.asyncio
    async def test_different_dex_winners(self, mock_dex_service: MagicMock, qa_config: QAConfig) -> None:
        """Test that different DEXs can be reported as best."""

        def mock_get_best_price(token_in: str, token_out: str, amount_in: Decimal):
            if token_in == "USDC":
                quote = create_mock_quote(token_in, dex="uniswap_v3")
            elif token_in == "LINK":
                quote = create_mock_quote(token_in, dex="enso")
            else:
                quote = create_mock_quote(token_in, dex="curve")
            return create_mock_best_result(token_in, quote)

        mock_dex_service.get_best_dex_price.side_effect = mock_get_best_price

        test = DEXSpotPriceTest(qa_config, mock_dex_service)
        results = await test.run()

        # Find results by token
        usdc_result = next(r for r in results if r.token == "USDC")
        link_result = next(r for r in results if r.token == "LINK")
        arb_result = next(r for r in results if r.token == "ARB")

        assert usdc_result.best_dex == "uniswap_v3"
        assert link_result.best_dex == "enso"
        assert arb_result.best_dex == "curve"
