"""Unit tests for Hyperliquid Funding Rate Provider.

This module tests the HyperliquidFundingProvider class in
providers/perp/hyperliquid_funding.py. The provider is a thin
``RateHistoryService`` client since VIB-4851 Phase D — tests mock
``fetch_funding_points`` (the gateway seam), never HTTP:
- Provider initialization and configuration
- Per-measured-entry results from gateway history
- Fallback behavior (never raises; LOW-confidence fill on failure)
- Market symbol normalization (legacy input formats preserved)
- Current-rate convenience method
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.framework.backtesting.pnl.providers.perp._gateway_history import (
    FundingHistoryPoint,
)
from almanak.framework.backtesting.pnl.providers.perp.hyperliquid_funding import (
    DATA_SOURCE,
    DEFAULT_REQUESTS_PER_MINUTE,
    HyperliquidClientConfig,
    HyperliquidFundingProvider,
)
from almanak.framework.backtesting.pnl.providers.rate_limiter import TokenBucketRateLimiter
from almanak.framework.backtesting.pnl.types import DataConfidence
from almanak.framework.data.interfaces import DataSourceUnavailable

_GATEWAY_SEAM = "almanak.framework.backtesting.pnl.providers.perp.hyperliquid_funding.fetch_funding_points"


def _points(start: datetime, rates: list[str]) -> list[FundingHistoryPoint]:
    """Hourly points starting at ``start`` with the given rates."""
    t0 = int(start.timestamp())
    return [FundingHistoryPoint(timestamp=t0 + 3600 * i, rate_hourly=Decimal(rate)) for i, rate in enumerate(rates)]


class TestHyperliquidFundingProviderInitialization:
    """Tests for HyperliquidFundingProvider initialization."""

    def test_init_default(self):
        """Test provider initializes with default settings."""
        provider = HyperliquidFundingProvider()
        assert provider.config.requests_per_minute == DEFAULT_REQUESTS_PER_MINUTE
        assert provider.config.fallback_rate == Decimal("0.0001")
        assert provider._owns_rate_limiter is True

    def test_init_with_custom_config(self):
        """Test provider initializes with custom config."""
        config = HyperliquidClientConfig(
            requests_per_minute=50,
            fallback_rate=Decimal("0.0002"),
        )
        provider = HyperliquidFundingProvider(config=config)
        assert provider.config is config
        assert provider.config.fallback_rate == Decimal("0.0002")

    def test_init_with_provided_rate_limiter(self):
        """An injected rate limiter is used and not owned."""
        limiter = TokenBucketRateLimiter(requests_per_minute=5)
        provider = HyperliquidFundingProvider(rate_limiter=limiter)
        assert provider.rate_limiter is limiter
        assert provider._owns_rate_limiter is False


class TestMarketSymbolNormalization:
    """The legacy accepted input formats keep resolving to coin symbols."""

    @pytest.mark.parametrize(
        ("market", "expected"),
        [
            ("ETH-USD", "ETH"),
            ("ETH/USD", "ETH"),
            ("ETH-PERP", "ETH"),
            ("eth_usdt", "ETH"),
            ("ETH", "ETH"),
            ("BTC-USDC", "BTC"),
        ],
    )
    def test_normalize_market_symbol(self, market: str, expected: str):
        """Various market formats normalize to the bare coin symbol."""
        provider = HyperliquidFundingProvider()
        assert provider._normalize_market_symbol(market) == expected

    @pytest.mark.asyncio
    async def test_canonical_market_sent_to_gateway(self):
        """The RPC carries the canonical <COIN>-USD market identifier."""
        provider = HyperliquidFundingProvider()
        start = datetime(2024, 1, 1, tzinfo=UTC)

        with patch(_GATEWAY_SEAM, return_value=_points(start, ["0.0001"])) as seam:
            await provider.get_funding_rates("eth/usd", start, datetime(2024, 1, 1, 1, tzinfo=UTC))

        kwargs = seam.call_args.kwargs
        assert kwargs["venue"] == "hyperliquid"
        assert kwargs["market"] == "ETH-USD"
        assert kwargs["chain"] == ""


class TestGetFundingRates:
    """Tests for historical funding rates (one result per measured entry)."""

    @pytest.mark.asyncio
    async def test_get_funding_rates_success(self):
        """Measured entries map 1:1 to HIGH-confidence results."""
        provider = HyperliquidFundingProvider()
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 1, 5, tzinfo=UTC)

        with patch(_GATEWAY_SEAM, return_value=_points(start, ["0.0001", "0.0002", "0.0003"])):
            rates = await provider.get_funding_rates("ETH-USD", start, end)

        assert len(rates) == 3
        assert [r.rate for r in rates] == [Decimal("0.0001"), Decimal("0.0002"), Decimal("0.0003")]
        assert all(r.source_info.source == DATA_SOURCE for r in rates)
        assert all(r.source_info.confidence == DataConfidence.HIGH for r in rates)
        # Result timestamps are the measured entry timestamps.
        assert rates[1].source_info.timestamp == datetime(2024, 1, 1, 1, tzinfo=UTC)

    @pytest.mark.asyncio
    async def test_no_data_returns_fallback_fill(self):
        """An empty window degrades to an hourly LOW-confidence fill."""
        provider = HyperliquidFundingProvider()
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 1, 3, tzinfo=UTC)

        with patch(_GATEWAY_SEAM, return_value=[]):
            rates = await provider.get_funding_rates("ETH-USD", start, end)

        assert len(rates) == 4
        assert all(r.source_info.source == "fallback" for r in rates)
        assert all(r.rate == provider.config.fallback_rate for r in rates)

    @pytest.mark.asyncio
    async def test_naive_datetimes_treated_as_utc(self):
        """Naive datetimes are treated as UTC."""
        provider = HyperliquidFundingProvider()

        with patch(_GATEWAY_SEAM, return_value=[]):
            rates = await provider.get_funding_rates(
                "ETH-USD",
                datetime(2024, 1, 1),  # noqa: DTZ001 - deliberate naive input
                datetime(2024, 1, 1, 1),  # noqa: DTZ001
            )

        assert len(rates) == 2
        assert all(r.source_info.timestamp.tzinfo is not None for r in rates)


class TestErrorHandling:
    """get_funding_rates never raises — every failure degrades to fallback."""

    @pytest.mark.asyncio
    async def test_gateway_unavailable_returns_fallback(self):
        """A failed gateway round-trip yields a LOW-confidence fill."""
        provider = HyperliquidFundingProvider()
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 1, 2, tzinfo=UTC)

        with patch(_GATEWAY_SEAM, side_effect=DataSourceUnavailable(source="gateway", reason="down")):
            rates = await provider.get_funding_rates("ETH-USD", start, end)

        assert len(rates) == 3
        assert all(r.source_info.source == "fallback" for r in rates)
        assert all(r.source_info.confidence == DataConfidence.LOW for r in rates)

    @pytest.mark.asyncio
    async def test_unexpected_error_returns_fallback(self):
        """Even unexpected errors degrade to the fallback fill."""
        provider = HyperliquidFundingProvider()
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 1, 1, tzinfo=UTC)

        with patch(_GATEWAY_SEAM, side_effect=RuntimeError("boom")):
            rates = await provider.get_funding_rates("ETH-USD", start, end)

        assert len(rates) == 2
        assert all(r.source_info.source == "fallback" for r in rates)


class TestGetCurrentFundingRate:
    """Tests for the current-rate convenience method."""

    @pytest.mark.asyncio
    async def test_get_current_funding_rate_success(self):
        """The most recent measured entry wins."""
        provider = HyperliquidFundingProvider()
        recent = _points(datetime(2024, 1, 1, tzinfo=UTC), ["0.0001", "0.0008"])

        with patch(_GATEWAY_SEAM, return_value=recent):
            result = await provider.get_current_funding_rate("ETH-USD")

        assert result.rate == Decimal("0.0008")
        assert result.source_info.source == DATA_SOURCE

    @pytest.mark.asyncio
    async def test_get_current_funding_rate_no_data(self):
        """No measured entry degrades to a single fallback result."""
        provider = HyperliquidFundingProvider()

        with patch(_GATEWAY_SEAM, return_value=[]):
            result = await provider.get_current_funding_rate("ETH-USD")

        # The empty window produces a fallback fill; the latest fill is returned.
        assert result.source_info.source == "fallback"
        assert result.rate == provider.config.fallback_rate


class TestContextManager:
    """Tests for async context manager support."""

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """The provider supports async-with (close is a compat no-op)."""
        async with HyperliquidFundingProvider() as provider:
            assert provider.config.fallback_rate == Decimal("0.0001")
        await provider.close()
