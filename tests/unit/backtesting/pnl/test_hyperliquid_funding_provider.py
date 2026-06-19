"""Unit tests for the Hyperliquid funding provider."""

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from almanak.framework.backtesting.pnl.providers.perp._gateway_history import (
    FundingHistoryPoint,
)
from almanak.framework.backtesting.pnl.providers.perp.hyperliquid_funding import (
    HyperliquidAPIError,
    HyperliquidFundingProvider,
)
from almanak.framework.backtesting.pnl.types import DataConfidence


class TestHyperliquidFundingProvider:
    """Characterization tests for historical Hyperliquid funding rates."""

    @pytest.mark.asyncio
    async def test_gateway_points_are_high_confidence(self) -> None:
        provider = HyperliquidFundingProvider()
        timestamp = int(datetime(2024, 1, 1, 1, tzinfo=UTC).timestamp())

        with patch.object(
            provider,
            "_fetch_points",
            AsyncMock(return_value=[FundingHistoryPoint(timestamp, Decimal("0.00031"))]),
        ) as fetch_points:
            results = await provider.get_funding_rates(
                "eth-usd",
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2024, 1, 1, 2, tzinfo=UTC),
            )

        assert len(results) == 1
        assert results[0].rate == Decimal("0.00031")
        assert results[0].source_info.source == "gateway"
        assert results[0].source_info.confidence is DataConfidence.HIGH
        assert fetch_points.call_args.kwargs["market"] == "eth-usd"

    @pytest.mark.asyncio
    async def test_empty_gateway_response_returns_low_confidence_hourly_fallback(self) -> None:
        provider = HyperliquidFundingProvider()

        with patch.object(provider, "_fetch_points", AsyncMock(return_value=[])):
            results = await provider.get_funding_rates(
                "ETH-USD",
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2024, 1, 1, 2, tzinfo=UTC),
            )

        assert [result.source_info.timestamp.hour for result in results] == [0, 1, 2]
        assert {result.source_info.source for result in results} == {"fallback"}
        assert all(result.source_info.confidence is DataConfidence.LOW for result in results)

    @pytest.mark.asyncio
    async def test_gateway_failure_returns_low_confidence_fallback(self) -> None:
        provider = HyperliquidFundingProvider()

        with patch.object(
            provider,
            "_fetch_points",
            AsyncMock(side_effect=HyperliquidAPIError("gateway unavailable")),
        ):
            results = await provider.get_funding_rates(
                "ETH-USD",
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2024, 1, 1, 1, tzinfo=UTC),
            )

        assert len(results) == 2
        assert {result.source_info.source for result in results} == {"fallback"}
        assert all(result.source_info.confidence is DataConfidence.LOW for result in results)

    @pytest.mark.asyncio
    async def test_non_utc_fallback_window_is_emitted_in_utc(self) -> None:
        provider = HyperliquidFundingProvider()
        eastern = timezone(timedelta(hours=-5))

        with patch.object(provider, "_fetch_points", AsyncMock(return_value=[])):
            results = await provider.get_funding_rates(
                "ETH-USD",
                datetime(2024, 1, 1, 20, tzinfo=eastern),
                datetime(2024, 1, 1, 21, tzinfo=eastern),
            )

        assert [result.source_info.timestamp.tzinfo for result in results] == [UTC, UTC]
        assert [result.source_info.timestamp.hour for result in results] == [1, 2]
