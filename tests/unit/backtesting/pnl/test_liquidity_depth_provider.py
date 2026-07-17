"""Unit tests for the historical liquidity depth provider."""

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from almanak.framework.backtesting.exceptions import NoAcceptableDataSourceError
from almanak.framework.backtesting.pnl.providers.liquidity_depth import (
    DATA_SOURCE_FALLBACK,
    LiquidityDepthProvider,
)
from almanak.framework.backtesting.pnl.types import (
    DataConfidence,
    DataSourceInfo,
    LiquidityResult,
)


def _measured_result(timestamp: datetime) -> LiquidityResult:
    return LiquidityResult(
        depth=Decimal("4200000"),
        source_info=DataSourceInfo(
            source="uniswap_v3_subgraph",
            confidence=DataConfidence.HIGH,
            timestamp=timestamp,
        ),
    )


def _data_unavailable_error() -> NoAcceptableDataSourceError:
    return NoAcceptableDataSourceError(
        data_type="liquidity",
        identifier="pool",
        remediation="Narrow the liquidity query window.",
        message="Liquidity pagination window too large",
    )


class TestLiquidityDepthProviderRouting:
    """Characterization tests for public liquidity-depth routing."""

    @pytest.mark.asyncio
    async def test_explicit_protocol_success_returns_measured_result(self) -> None:
        provider = LiquidityDepthProvider(fallback_depth=Decimal("1"))
        timestamp = datetime(2024, 1, 1, 12, tzinfo=UTC)
        measured = _measured_result(timestamp)

        with patch.object(
            provider,
            "_query_liquidity_by_family",
            AsyncMock(return_value=measured),
        ) as query:
            result = await provider.get_liquidity_depth(
                "0x0000000000000000000000000000000000000001",
                "ethereum",
                timestamp,
                protocol="uniswap_v3",
            )

        assert result is measured
        assert query.call_args.kwargs["protocol_id"] == "uniswap_v3"

    @pytest.mark.asyncio
    async def test_auto_detected_protocol_routes_to_family_query(self) -> None:
        provider = LiquidityDepthProvider()
        timestamp = datetime(2024, 1, 1, 12, tzinfo=UTC)
        measured = _measured_result(timestamp)

        with (
            patch.object(provider, "_detect_protocol_from_chain", return_value="uniswap_v3"),
            patch.object(
                provider,
                "_query_liquidity_by_family",
                AsyncMock(return_value=measured),
            ) as query,
        ):
            result = await provider.get_liquidity_depth(
                "0x0000000000000000000000000000000000000001",
                "ethereum",
                timestamp,
                protocol=None,
            )

        assert result is measured
        assert query.call_args.kwargs["protocol_id"] == "uniswap_v3"

    @pytest.mark.asyncio
    async def test_unsupported_chain_returns_low_confidence_fallback(self) -> None:
        provider = LiquidityDepthProvider(fallback_depth=Decimal("123"))
        timestamp = datetime(2024, 1, 1, tzinfo=UTC)

        with patch.object(provider, "_query_liquidity_by_family", AsyncMock()) as query:
            result = await provider.get_liquidity_depth(
                "0x0000000000000000000000000000000000000001",
                "solana",
                timestamp,
                protocol="uniswap_v3",
            )

        assert result.depth == Decimal("123")
        assert result.source_info.source == DATA_SOURCE_FALLBACK
        assert result.source_info.confidence is DataConfidence.LOW
        query.assert_not_called()

    @pytest.mark.asyncio
    async def test_unknown_protocol_returns_low_confidence_fallback(self) -> None:
        provider = LiquidityDepthProvider(fallback_depth=Decimal("456"))
        timestamp = datetime(2024, 1, 1, tzinfo=UTC)

        with patch.object(provider, "_query_liquidity_by_family", AsyncMock()) as query:
            result = await provider.get_liquidity_depth(
                "0x0000000000000000000000000000000000000001",
                "ethereum",
                timestamp,
                protocol="unknown_dex",
            )

        assert result.depth == Decimal("456")
        assert result.source_info.source == DATA_SOURCE_FALLBACK
        assert result.source_info.confidence is DataConfidence.LOW
        query.assert_not_called()

    @pytest.mark.asyncio
    async def test_query_returning_none_returns_low_confidence_fallback(self) -> None:
        provider = LiquidityDepthProvider(fallback_depth=Decimal("789"))
        timestamp = datetime(2024, 1, 1, tzinfo=UTC)

        with patch.object(
            provider,
            "_query_liquidity_by_family",
            AsyncMock(return_value=None),
        ):
            result = await provider.get_liquidity_depth(
                "0x0000000000000000000000000000000000000001",
                "ethereum",
                timestamp,
                protocol="uniswap_v3",
            )

        assert result.depth == Decimal("789")
        assert result.source_info.confidence is DataConfidence.LOW

    @pytest.mark.asyncio
    async def test_unexpected_query_error_returns_low_confidence_fallback(self) -> None:
        provider = LiquidityDepthProvider(fallback_depth=Decimal("99"))
        timestamp = datetime(2024, 1, 1, tzinfo=UTC)

        with patch.object(
            provider,
            "_query_liquidity_by_family",
            AsyncMock(side_effect=RuntimeError("subgraph down")),
        ):
            result = await provider.get_liquidity_depth(
                "0x0000000000000000000000000000000000000001",
                "ethereum",
                timestamp,
                protocol="uniswap_v3",
            )

        assert result.depth == Decimal("99")
        assert result.source_info.confidence is DataConfidence.LOW

    @pytest.mark.asyncio
    async def test_pagination_overflow_stays_loud(self) -> None:
        provider = LiquidityDepthProvider()
        timestamp = datetime(2024, 1, 1, tzinfo=UTC)

        with patch.object(
            provider,
            "_query_liquidity_by_family",
            AsyncMock(side_effect=_data_unavailable_error()),
        ):
            with pytest.raises(NoAcceptableDataSourceError, match="pagination window"):
                await provider.get_liquidity_depth(
                    "0x0000000000000000000000000000000000000001",
                    "ethereum",
                    timestamp,
                    protocol="uniswap_v3",
                )

    @pytest.mark.asyncio
    async def test_fallback_timestamp_is_emitted_in_utc(self) -> None:
        provider = LiquidityDepthProvider(fallback_depth=Decimal("123"))
        eastern = timezone(timedelta(hours=-5))

        result = await provider.get_liquidity_depth(
            "0x0000000000000000000000000000000000000001",
            "solana",
            datetime(2024, 1, 1, 20, tzinfo=eastern),
            protocol="uniswap_v3",
        )

        assert result.source_info.timestamp.tzinfo is UTC
        assert result.source_info.timestamp.hour == 1
