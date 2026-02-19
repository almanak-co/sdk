"""Tests for CEX Spot Price Test Module.

This test suite covers:
- CEXSpotResult dataclass creation and serialization
- CEXSpotPriceTest with mocked CoinGeckoPriceSource
- Pass/fail logic validation
- Error handling for DataSourceUnavailable
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable, PriceResult
from almanak.framework.data.qa.config import QAConfig, QAThresholds
from almanak.framework.data.qa.tests.cex_spot import CEXSpotPriceTest, CEXSpotResult

# =============================================================================
# CEXSpotResult Tests
# =============================================================================


class TestCEXSpotResult:
    """Tests for CEXSpotResult dataclass."""

    def test_create_passing_result(self) -> None:
        """Test creating a passing result."""
        result = CEXSpotResult(
            token="ETH",
            price_usd=Decimal("2500.00"),
            confidence=1.0,
            timestamp=datetime.now(UTC),
            is_fresh=True,
            passed=True,
            error=None,
        )

        assert result.token == "ETH"
        assert result.price_usd == Decimal("2500.00")
        assert result.confidence == 1.0
        assert result.is_fresh is True
        assert result.passed is True
        assert result.error is None

    def test_create_failing_result(self) -> None:
        """Test creating a failing result."""
        result = CEXSpotResult(
            token="ETH",
            price_usd=None,
            confidence=None,
            timestamp=None,
            is_fresh=False,
            passed=False,
            error="Data source unavailable: Timeout",
        )

        assert result.token == "ETH"
        assert result.price_usd is None
        assert result.confidence is None
        assert result.timestamp is None
        assert result.is_fresh is False
        assert result.passed is False
        assert result.error == "Data source unavailable: Timeout"

    def test_to_dict_with_values(self) -> None:
        """Test serialization with all values present."""
        ts = datetime(2025, 1, 18, 12, 0, 0, tzinfo=UTC)
        result = CEXSpotResult(
            token="ETH",
            price_usd=Decimal("2500.50"),
            confidence=0.95,
            timestamp=ts,
            is_fresh=True,
            passed=True,
            error=None,
        )

        d = result.to_dict()

        assert d["token"] == "ETH"
        assert d["price_usd"] == "2500.50"
        assert d["confidence"] == 0.95
        assert d["timestamp"] == "2025-01-18T12:00:00+00:00"
        assert d["is_fresh"] is True
        assert d["passed"] is True
        assert d["error"] is None

    def test_to_dict_with_none_values(self) -> None:
        """Test serialization with None values."""
        result = CEXSpotResult(
            token="ETH",
            price_usd=None,
            confidence=None,
            timestamp=None,
            is_fresh=False,
            passed=False,
            error="Some error",
        )

        d = result.to_dict()

        assert d["price_usd"] is None
        assert d["confidence"] is None
        assert d["timestamp"] is None


# =============================================================================
# CEXSpotPriceTest Tests
# =============================================================================


@pytest.fixture
def mock_price_source() -> MagicMock:
    """Create a mock CoinGeckoPriceSource."""
    mock = MagicMock()
    mock.get_price = AsyncMock()
    mock.close = AsyncMock()
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
        dex_tokens=[],
    )


class TestCEXSpotPriceTest:
    """Tests for CEXSpotPriceTest class."""

    @pytest.mark.asyncio
    async def test_run_all_passing(self, mock_price_source: MagicMock, qa_config: QAConfig) -> None:
        """Test run() when all tokens pass validation."""
        # Configure mock to return good prices
        mock_price_source.get_price.return_value = PriceResult(
            price=Decimal("2500.00"),
            source="coingecko",
            timestamp=datetime.now(UTC),
            confidence=1.0,
            stale=False,
        )

        test = CEXSpotPriceTest(qa_config, mock_price_source)
        results = await test.run()

        # Should have 3 results (2 popular + 1 additional)
        assert len(results) == 3

        # All should pass
        assert all(r.passed for r in results)
        assert all(r.is_fresh for r in results)
        assert all(r.error is None for r in results)

        # Verify get_price was called for each token
        assert mock_price_source.get_price.call_count == 3

    @pytest.mark.asyncio
    async def test_run_low_confidence_fails(self, mock_price_source: MagicMock, qa_config: QAConfig) -> None:
        """Test that low confidence causes failure."""
        # Configure mock to return low confidence
        mock_price_source.get_price.return_value = PriceResult(
            price=Decimal("2500.00"),
            source="coingecko",
            timestamp=datetime.now(UTC),
            confidence=0.5,  # Below 0.8 threshold
            stale=False,
        )

        test = CEXSpotPriceTest(qa_config, mock_price_source)
        results = await test.run()

        # All should fail due to low confidence
        assert all(not r.passed for r in results)
        assert all("Low confidence" in (r.error or "") for r in results)

    @pytest.mark.asyncio
    async def test_run_stale_data_fails(self, mock_price_source: MagicMock, qa_config: QAConfig) -> None:
        """Test that stale data causes failure."""
        # Create a timestamp that is older than max_stale_seconds (120s)
        from datetime import timedelta

        old_ts = datetime.now(UTC) - timedelta(seconds=200)

        mock_price_source.get_price.return_value = PriceResult(
            price=Decimal("2500.00"),
            source="coingecko",
            timestamp=old_ts,
            confidence=1.0,
            stale=True,
        )

        test = CEXSpotPriceTest(qa_config, mock_price_source)
        results = await test.run()

        # All should fail due to stale data
        assert all(not r.passed for r in results)
        assert all(not r.is_fresh for r in results)
        assert all("Stale data" in (r.error or "") for r in results)

    @pytest.mark.asyncio
    async def test_run_zero_price_fails(self, mock_price_source: MagicMock, qa_config: QAConfig) -> None:
        """Test that zero price causes failure."""
        mock_price_source.get_price.return_value = PriceResult(
            price=Decimal("0"),
            source="coingecko",
            timestamp=datetime.now(UTC),
            confidence=1.0,
            stale=False,
        )

        test = CEXSpotPriceTest(qa_config, mock_price_source)
        results = await test.run()

        # All should fail due to zero price
        assert all(not r.passed for r in results)
        assert all("Invalid price" in (r.error or "") for r in results)

    @pytest.mark.asyncio
    async def test_run_handles_data_source_unavailable(self, mock_price_source: MagicMock, qa_config: QAConfig) -> None:
        """Test graceful handling of DataSourceUnavailable."""
        mock_price_source.get_price.side_effect = DataSourceUnavailable(
            source="coingecko",
            reason="Rate limited",
        )

        test = CEXSpotPriceTest(qa_config, mock_price_source)
        results = await test.run()

        # All should fail with error message
        assert all(not r.passed for r in results)
        assert all(r.price_usd is None for r in results)
        assert all("Data source unavailable" in (r.error or "") for r in results)

    @pytest.mark.asyncio
    async def test_run_handles_unexpected_error(self, mock_price_source: MagicMock, qa_config: QAConfig) -> None:
        """Test graceful handling of unexpected exceptions."""
        mock_price_source.get_price.side_effect = RuntimeError("Network error")

        test = CEXSpotPriceTest(qa_config, mock_price_source)
        results = await test.run()

        # All should fail with error message
        assert all(not r.passed for r in results)
        assert all("Unexpected error" in (r.error or "") for r in results)

    @pytest.mark.asyncio
    async def test_run_mixed_results(self, mock_price_source: MagicMock, qa_config: QAConfig) -> None:
        """Test handling of mixed pass/fail results."""

        # Define side effects for each token
        def get_price_side_effect(token: str, quote: str) -> PriceResult:
            if token == "ETH":
                return PriceResult(
                    price=Decimal("2500.00"),
                    source="coingecko",
                    timestamp=datetime.now(UTC),
                    confidence=1.0,
                    stale=False,
                )
            elif token == "WBTC":
                return PriceResult(
                    price=Decimal("45000.00"),
                    source="coingecko",
                    timestamp=datetime.now(UTC),
                    confidence=0.5,  # Low confidence
                    stale=False,
                )
            else:
                raise DataSourceUnavailable(
                    source="coingecko",
                    reason="Token not found",
                )

        mock_price_source.get_price.side_effect = get_price_side_effect

        test = CEXSpotPriceTest(qa_config, mock_price_source)
        results = await test.run()

        # Find results by token
        eth_result = next(r for r in results if r.token == "ETH")
        wbtc_result = next(r for r in results if r.token == "WBTC")
        uni_result = next(r for r in results if r.token == "UNI")

        # ETH should pass
        assert eth_result.passed is True
        assert eth_result.error is None

        # WBTC should fail due to low confidence
        assert wbtc_result.passed is False
        assert "Low confidence" in (wbtc_result.error or "")

        # UNI should fail due to unavailable
        assert uni_result.passed is False
        assert "Data source unavailable" in (uni_result.error or "")

    @pytest.mark.asyncio
    async def test_context_manager(self, mock_price_source: MagicMock, qa_config: QAConfig) -> None:
        """Test async context manager properly closes resources."""
        async with CEXSpotPriceTest(qa_config, mock_price_source) as test:
            assert test is not None

        mock_price_source.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_empty_token_list(self, mock_price_source: MagicMock) -> None:
        """Test handling of empty token list."""
        config = QAConfig(
            popular_tokens=[],
            additional_tokens=[],
        )

        test = CEXSpotPriceTest(config, mock_price_source)
        results = await test.run()

        # Should return empty list
        assert results == []
        mock_price_source.get_price.assert_not_called()
